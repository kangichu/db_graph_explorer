"""
Database Graph Explorer - FastAPI Backend
Supports PostgreSQL and MySQL schema introspection + safe schema editing
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
import re
import signal
import logging

logger = logging.getLogger("uvicorn.error")

@asynccontextmanager
async def lifespan(app):
    logger.info("DB Graph Explorer started")
    yield
    logger.info("Shutting down DB Graph Explorer cleanly...")

app = FastAPI(title="DB Graph Explorer", version="1.0.0", lifespan=lifespan)

# Handle Ctrl+C gracefully
signal.signal(signal.SIGINT, lambda sig, frame: exit(0))
signal.signal(signal.SIGTERM, lambda sig, frame: exit(0))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Request/Response Models ───────────────────────────────────────────────

class ConnectionConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str
    database: str
    db_type: str  # "postgresql" or "mysql"

class RelationshipCreate(BaseModel):
    connection: ConnectionConfig
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    constraint_name: Optional[str] = None

class RelationshipDelete(BaseModel):
    connection: ConnectionConfig
    table_name: str
    constraint_name: str

class SQLExecuteRequest(BaseModel):
    connection: ConnectionConfig
    sql: str

class ReportRequest(BaseModel):
    connection: ConnectionConfig
    table_name: str

class IndexExecuteRequest(BaseModel):
    connection: ConnectionConfig
    sql: str


# ─── Connection Helpers ─────────────────────────────────────────────────────

async def get_pg_connection(cfg: ConnectionConfig):
    try:
        import asyncpg
        conn = await asyncpg.connect(
            host=cfg.host, port=cfg.port, user=cfg.username,
            password=cfg.password, database=cfg.database, timeout=10,
        )
        return conn
    except ImportError:
        raise HTTPException(status_code=500, detail="asyncpg not installed. Run: pip install asyncpg")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"PostgreSQL connection failed: {str(e)}")

async def get_mysql_connection(cfg: ConnectionConfig):
    try:
        import aiomysql
        conn = await aiomysql.connect(
            host=cfg.host, port=cfg.port, user=cfg.username,
            password=cfg.password, db=cfg.database, connect_timeout=10, autocommit=False,
        )
        return conn
    except ImportError:
        raise HTTPException(status_code=500, detail="aiomysql not installed. Run: pip install aiomysql")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MySQL connection failed: {str(e)}")


# ─── Schema Introspection ───────────────────────────────────────────────────

async def introspect_postgresql(cfg: ConnectionConfig) -> Dict:
    import asyncpg
    conn = await get_pg_connection(cfg)
    try:
        tables_rows = await conn.fetch("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        columns_rows = await conn.fetch("""
            SELECT c.table_name, c.column_name, c.data_type,
                   c.is_nullable, c.column_default, c.ordinal_position,
                   CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.table_name, ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                  ON tc.constraint_name = ku.constraint_name AND tc.table_schema = ku.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
            ) pk ON pk.table_name = c.table_name AND pk.column_name = c.column_name
            WHERE c.table_schema = 'public'
            ORDER BY c.table_name, c.ordinal_position
        """)
        fk_rows = await conn.fetch("""
            SELECT tc.constraint_name, kcu.table_name AS from_table, kcu.column_name AS from_column,
                   ccu.table_name AS to_table, ccu.column_name AS to_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        """)
        # Approximate row counts from pg stats (fast, no full table scans)
        count_rows = await conn.fetch("""
            SELECT relname AS table_name, GREATEST(reltuples::bigint, 0) AS row_count
            FROM pg_class
            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
              AND relkind = 'r'
        """)
        return build_graph(
            [dict(r) for r in tables_rows],
            [dict(r) for r in columns_rows],
            [dict(r) for r in fk_rows],
            {r['table_name']: r['row_count'] for r in count_rows},
        )
    finally:
        await conn.close()


async def introspect_mysql(cfg: ConnectionConfig) -> Dict:
    import aiomysql
    conn = await get_mysql_connection(cfg)
    try:
        cursor = await conn.cursor(aiomysql.DictCursor)
        await cursor.execute("""
            SELECT table_name AS table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name
        """, (cfg.database,))
        tables_rows = await cursor.fetchall()

        await cursor.execute("""
            SELECT c.table_name AS table_name, c.column_name AS column_name,
                   c.data_type AS data_type, c.is_nullable AS is_nullable,
                   c.column_default AS column_default, c.ordinal_position AS ordinal_position,
                   CASE WHEN c.column_key = 'PRI' THEN 1 ELSE 0 END AS is_primary
            FROM information_schema.columns c
            WHERE c.table_schema = %s ORDER BY c.table_name, c.ordinal_position
        """, (cfg.database,))
        columns_rows = await cursor.fetchall()

        await cursor.execute("""
            SELECT kcu.constraint_name AS constraint_name,
                   kcu.table_name AS from_table,
                   kcu.column_name AS from_column,
                   kcu.referenced_table_name AS to_table,
                   kcu.referenced_column_name AS to_column
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc
              ON kcu.constraint_name = tc.constraint_name
             AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name
            WHERE kcu.table_schema = %s AND tc.constraint_type = 'FOREIGN KEY'
              AND kcu.referenced_table_name IS NOT NULL
        """, (cfg.database,))
        fk_rows = await cursor.fetchall()
        # Row counts from information_schema (fast, uses cached stats)
        await cursor.execute("""
            SELECT table_name AS table_name, table_rows AS row_count
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
        """, (cfg.database,))
        count_rows = await cursor.fetchall()
        await cursor.close()
        return build_graph(
            [dict(r) for r in tables_rows],
            [dict(r) for r in columns_rows],
            [dict(r) for r in fk_rows],
            {_lower_keys(r)['table_name']: _lower_keys(r).get('row_count') or 0 for r in count_rows},
        )
    finally:
        conn.close()


def _lower_keys(d: dict) -> dict:
    return {k.lower(): v for k, v in d.items()}

def build_graph(tables_rows, columns_rows, fk_rows, row_counts: Dict[str, int] = None) -> Dict:
    tables_rows = [_lower_keys(r) for r in tables_rows]
    columns_rows = [_lower_keys(r) for r in columns_rows]
    fk_rows = [_lower_keys(r) for r in fk_rows]
    row_counts = row_counts or {}
    columns_map: Dict[str, List] = {}
    for col in columns_rows:
        tname = col["table_name"]
        if tname not in columns_map:
            columns_map[tname] = []
        columns_map[tname].append({
            "name": col["column_name"],
            "type": col["data_type"],
            "nullable": col.get("is_nullable", "YES") in ("YES", 1, True),
            "default": col.get("column_default"),
            "is_primary": bool(col.get("is_primary", False)),
        })

    edges = []
    edge_set = set()
    fk_map: Dict[str, List] = {}
    degree_map: Dict[str, int] = {}
    # parents_map[child] = [parent, ...] — tables child depends on (child has FK → parent)
    parents_map: Dict[str, List[str]] = {}
    # children_map[parent] = [child, ...] — tables that depend on parent
    children_map: Dict[str, List[str]] = {}

    for fk in fk_rows:
        from_t = fk["from_table"]
        to_t = fk["to_table"]
        cname = fk["constraint_name"]
        edge_id = f"{from_t}__{to_t}__{cname}"
        if edge_id not in edge_set:
            edge_set.add(edge_id)
            edges.append({
                "id": edge_id,
                "source": from_t,
                "target": to_t,
                "from_column": fk["from_column"],
                "to_column": fk["to_column"],
                "constraint_name": cname,
            })
        if from_t not in fk_map:
            fk_map[from_t] = []
        fk_map[from_t].append({
            "constraint_name": cname,
            "from_column": fk["from_column"],
            "to_table": to_t,
            "to_column": fk["to_column"],
        })
        degree_map[from_t] = degree_map.get(from_t, 0) + 1
        degree_map[to_t] = degree_map.get(to_t, 0) + 1

        # track parent/child relationships
        if from_t not in parents_map:
            parents_map[from_t] = []
        if to_t not in parents_map[from_t]:
            parents_map[from_t].append(to_t)

        if to_t not in children_map:
            children_map[to_t] = []
        if from_t not in children_map[to_t]:
            children_map[to_t].append(from_t)

    # ── Kahn's topological sort → layer per table ────────────────────────────
    all_tables = [r["table_name"] for r in tables_rows]
    tables_set = set(all_tables)

    # in_degree = number of unique parents within the table set
    in_degree: Dict[str, int] = {t: 0 for t in all_tables}
    for child, parents in parents_map.items():
        if child in tables_set:
            in_degree[child] = len([p for p in parents if p in tables_set])

    from collections import deque
    queue = deque(t for t in all_tables if in_degree[t] == 0)
    layer_map: Dict[str, int] = {}
    current_layer = 0

    while queue:
        next_queue: deque = deque()
        while queue:
            node = queue.popleft()
            layer_map[node] = current_layer
            for child in children_map.get(node, []):
                if child not in layer_map and child in tables_set:
                    in_degree[child] -= 1
                    if in_degree[child] == 0:
                        next_queue.append(child)
        current_layer += 1
        queue = next_queue

    # Nodes in cycles or unresolved are placed at the end
    max_layer = current_layer
    for t in all_tables:
        if t not in layer_map:
            layer_map[t] = max_layer

    nodes = []
    for row in tables_rows:
        tname = row["table_name"]
        nodes.append({
            "id": tname,
            "label": tname,
            "columns": columns_map.get(tname, []),
            "degree": degree_map.get(tname, 0),
            "foreign_keys": fk_map.get(tname, []),
            "row_count": int(row_counts.get(tname, 0)),
            "layer": layer_map.get(tname, 0),
            "parents": sorted(parents_map.get(tname, [])),
            "children": sorted(children_map.get(tname, [])),
        })

    return {"nodes": nodes, "edges": edges}


# ─── SQL Generation ─────────────────────────────────────────────────────────

def validate_identifier(name: str):
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        raise HTTPException(status_code=400, detail=f"Invalid SQL identifier: {name}")

def generate_add_fk_sql(req: RelationshipCreate) -> str:
    cname = req.constraint_name or f"fk_{req.from_table}_{req.from_column}_{req.to_table}"
    for n in [cname, req.from_table, req.from_column, req.to_table, req.to_column]:
        validate_identifier(n)
    return (
        f"ALTER TABLE {req.from_table}\n"
        f"  ADD CONSTRAINT {cname}\n"
        f"  FOREIGN KEY ({req.from_column})\n"
        f"  REFERENCES {req.to_table} ({req.to_column});"
    )

def generate_drop_fk_sql(req: RelationshipDelete) -> str:
    for n in [req.table_name, req.constraint_name]:
        validate_identifier(n)
    if req.connection.db_type == "mysql":
        return f"ALTER TABLE {req.table_name}\n  DROP FOREIGN KEY {req.constraint_name};"
    return f"ALTER TABLE {req.table_name}\n  DROP CONSTRAINT {req.constraint_name};"


# ─── API Routes ─────────────────────────────────────────────────────────────

@app.post("/api/test-connection")
async def test_connection(cfg: ConnectionConfig):
    if cfg.db_type == "postgresql":
        conn = await get_pg_connection(cfg)
        await conn.close()
    elif cfg.db_type == "mysql":
        conn = await get_mysql_connection(cfg)
        conn.close()
    else:
        raise HTTPException(status_code=400, detail="Use 'postgresql' or 'mysql'")
    return {"status": "ok", "message": f"Connected to {cfg.db_type} '{cfg.database}'"}


@app.post("/api/schema")
async def get_schema(cfg: ConnectionConfig):
    if cfg.db_type == "postgresql":
        return await introspect_postgresql(cfg)
    elif cfg.db_type == "mysql":
        return await introspect_mysql(cfg)
    raise HTTPException(status_code=400, detail="Unsupported db_type.")


@app.post("/api/schema/partial")
async def get_partial_schema(
    cfg: ConnectionConfig, limit: int = 30, offset: int = 0,
    degree_filter: Optional[str] = None,
    neighbor_limit: int = 20, neighbor_offset: int = 0,
):
    full = await get_schema(cfg)
    all_nodes = full["nodes"]
    all_nodes_map = {n["id"]: n for n in all_nodes}

    if degree_filter:
        max_deg = max((n["degree"] for n in all_nodes), default=1) or 1
        high_thresh = int(max_deg * 0.2) + (1 if max_deg * 0.2 != int(max_deg * 0.2) else 0)
        low_thresh = int(max_deg * 0.08) + (1 if max_deg * 0.08 != int(max_deg * 0.08) else 0)

        def cat(d):
            if d == 0: return "isolated"
            if d >= high_thresh: return "high"
            if d >= low_thresh: return "normal"
            return "low"

        primary_nodes = [n for n in all_nodes if cat(n["degree"]) == degree_filter]
        primary_ids = {n["id"] for n in primary_nodes}

        total = len(primary_nodes)
        paged_primary = primary_nodes[offset:offset + limit]
        paged_primary_ids = {n["id"] for n in paged_primary}

        # Collect all neighbor IDs for paged primary nodes
        all_neighbor_ids = set()
        for e in full["edges"]:
            if e["source"] in paged_primary_ids and e["target"] not in primary_ids:
                all_neighbor_ids.add(e["target"])
            if e["target"] in paged_primary_ids and e["source"] not in primary_ids:
                all_neighbor_ids.add(e["source"])

        all_neighbor_list = sorted(all_neighbor_ids)
        total_neighbors = len(all_neighbor_list)
        paged_neighbor_ids = set(all_neighbor_list[neighbor_offset:neighbor_offset + neighbor_limit])

        neighbor_nodes = [all_nodes_map[nid] for nid in all_neighbor_list[neighbor_offset:neighbor_offset + neighbor_limit] if nid in all_nodes_map]

        for n in paged_primary:
            n["is_primary_match"] = True
        for n in neighbor_nodes:
            n["is_primary_match"] = False

        nodes = paged_primary + neighbor_nodes
        node_ids = {n["id"] for n in nodes}
        edges = [e for e in full["edges"] if e["source"] in node_ids and e["target"] in node_ids]
        return {
            "nodes": nodes, "edges": edges,
            "total_tables": total, "offset": offset, "limit": limit,
            "total_neighbors": total_neighbors,
            "neighbor_offset": neighbor_offset, "neighbor_limit": neighbor_limit,
        }

    total = len(all_nodes)
    nodes = all_nodes[offset:offset + limit]
    node_ids = {n["id"] for n in nodes}
    edges = [e for e in full["edges"] if e["source"] in node_ids and e["target"] in node_ids]
    return {"nodes": nodes, "edges": edges, "total_tables": total, "offset": offset, "limit": limit}


@app.post("/api/relationship/preview-add")
async def preview_add_relationship(req: RelationshipCreate):
    sql = generate_add_fk_sql(req)
    return {"sql": sql, "operation": "ADD_FOREIGN_KEY"}


@app.post("/api/relationship/preview-delete")
async def preview_delete_relationship(req: RelationshipDelete):
    sql = generate_drop_fk_sql(req)
    return {"sql": sql, "operation": "DROP_FOREIGN_KEY"}


@app.post("/api/relationship/execute")
async def execute_sql(req: SQLExecuteRequest):
    stripped = req.sql.strip().upper()
    if not stripped.startswith("ALTER TABLE"):
        raise HTTPException(status_code=400, detail="Only ALTER TABLE statements are permitted.")
    try:
        if req.connection.db_type == "postgresql":
            conn = await get_pg_connection(req.connection)
            try:
                await conn.execute(req.sql)
            finally:
                await conn.close()
        elif req.connection.db_type == "mysql":
            conn = await get_mysql_connection(req.connection)
            try:
                cursor = await conn.cursor()
                await cursor.execute(req.sql)
                await conn.commit()
                await cursor.close()
            finally:
                conn.close()
        else:
            raise HTTPException(status_code=400, detail="Unsupported db_type.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {str(e)}")
    return {"status": "ok", "message": "SQL executed successfully."}


# ─── Profiling / Report Engine ──────────────────────────────────────────────

def _safe_val(v):
    """Convert DB values to JSON-safe types."""
    if v is None:
        return None
    if isinstance(v, (int, float, bool, str)):
        return v
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


async def _get_indexes_pg(conn, table_name: str) -> list:
    """Fetch existing indexes for a PostgreSQL table."""
    validate_identifier(table_name)
    rows = await conn.fetch("""
        SELECT
            i.relname AS index_name,
            am.amname AS index_type,
            ix.indisunique AS is_unique,
            ix.indisprimary AS is_primary,
            array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_namespace ns ON ns.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE t.relname = $1 AND ns.nspname = 'public'
        GROUP BY i.relname, am.amname, ix.indisunique, ix.indisprimary
    """, table_name)
    return [{
        "name": r["index_name"],
        "type": r["index_type"],
        "is_unique": r["is_unique"],
        "is_primary": r["is_primary"],
        "columns": list(r["columns"]),
    } for r in rows]


async def _get_indexes_mysql(conn, table_name: str, db_name: str) -> list:
    """Fetch existing indexes for a MySQL table."""
    import aiomysql
    validate_identifier(table_name)
    validate_identifier(db_name)
    cursor = await conn.cursor(aiomysql.DictCursor)
    try:
        await cursor.execute("""
            SELECT INDEX_NAME, INDEX_TYPE, NON_UNIQUE, COLUMN_NAME, SEQ_IN_INDEX
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """, (db_name, table_name))
        rows = [_lower_keys(r) for r in await cursor.fetchall()]
    finally:
        await cursor.close()

    idx_map = {}
    for r in rows:
        name = r["index_name"]
        if name not in idx_map:
            idx_map[name] = {
                "name": name,
                "type": r["index_type"],
                "is_unique": r["non_unique"] == 0,
                "is_primary": name == "PRIMARY",
                "columns": [],
            }
        idx_map[name]["columns"].append(r["column_name"])
    return list(idx_map.values())


def _recommend_indexes(profile: dict, existing_indexes: list) -> list:
    """Generate index recommendations based on column stats and FK usage."""
    recs = []
    indexed_cols = set()
    for idx in existing_indexes:
        for c in idx["columns"]:
            indexed_cols.add(c.lower())

    # FK columns without indexes — these are the most impactful
    for intg in profile.get("integrity", []):
        fc = intg.get("from_column", "")
        if fc.lower() not in indexed_cols:
            sql = f"CREATE INDEX idx_{profile['table']}_{fc} ON {profile['table']} ({fc});"
            recs.append({
                "column": fc,
                "reason": f"Foreign key column '{fc}' has no index. JOINs and lookups on this FK will cause full table scans.",
                "index_type": "B-Tree",
                "impact": "high",
                "sql": sql,
            })

    for col in profile.get("columns", []):
        cname = col["column"]
        if cname.lower() in indexed_cols:
            continue
        row_count = profile.get("row_count", 0)
        if row_count < 100:
            continue

        # High uniqueness columns — great index candidates
        if col.get("uniqueness", 0) >= 95 and col["non_null"] > 100:
            sql = f"CREATE INDEX idx_{profile['table']}_{cname} ON {profile['table']} ({cname});"
            recs.append({
                "column": cname,
                "reason": f"'{cname}' has {col['uniqueness']}% uniqueness — highly selective, ideal for lookups and WHERE clauses.",
                "index_type": "B-Tree",
                "impact": "high",
                "sql": sql,
            })
        # Low-cardinality with lots of rows — consider for filtered scans
        elif col.get("distinct", 0) > 1 and col.get("distinct", 0) <= 20 and row_count > 1000:
            sql = f"CREATE INDEX idx_{profile['table']}_{cname} ON {profile['table']} ({cname});"
            recs.append({
                "column": cname,
                "reason": f"'{cname}' has only {col['distinct']} distinct values across {row_count} rows — index can speed up filtered queries with WHERE {cname} = '...'.",
                "index_type": "B-Tree",
                "impact": "medium",
                "sql": sql,
            })

    return recs


async def _profile_table_pg(conn, table_name: str, columns: list, fks: list) -> Dict:
    """Profile a single table using PostgreSQL."""
    validate_identifier(table_name)
    row = await conn.fetchrow(f'SELECT COUNT(*) AS cnt FROM "{table_name}"')
    row_count = row["cnt"]

    col_profiles = []
    for col in columns:
        cname = col["column_name"]
        validate_identifier(cname)
        stats = await conn.fetchrow(f"""
            SELECT
                COUNT(*) AS total,
                COUNT("{cname}") AS non_null,
                COUNT(DISTINCT "{cname}") AS distinct_count,
                MIN("{cname}"::text) AS min_val,
                MAX("{cname}"::text) AS max_val
            FROM "{table_name}"
        """)
        total = stats["total"] or 0
        non_null = stats["non_null"] or 0
        null_pct = round((1 - non_null / total) * 100, 1) if total > 0 else 0
        dtype = col["data_type"].lower()

        profile = {
            "column": cname,
            "type": col["data_type"],
            "nullable": col.get("is_nullable", "YES") in ("YES", True, 1),
            "total": total,
            "non_null": non_null,
            "null_pct": null_pct,
            "distinct": stats["distinct_count"],
            "min": _safe_val(stats["min_val"]),
            "max": _safe_val(stats["max_val"]),
        }

        # Numeric avg
        if any(k in dtype for k in ("int", "numeric", "decimal", "float", "double", "real", "serial", "money")):
            try:
                avg_row = await conn.fetchrow(f'SELECT AVG("{cname}"::numeric) AS avg_val FROM "{table_name}"')
                profile["avg"] = round(float(avg_row["avg_val"]), 2) if avg_row["avg_val"] is not None else None
            except Exception:
                profile["avg"] = None

        # Top 5 most common values
        if total > 0:
            try:
                top_rows = await conn.fetch(f"""
                    SELECT "{cname}"::text AS val, COUNT(*) AS freq
                    FROM "{table_name}"
                    WHERE "{cname}" IS NOT NULL
                    GROUP BY "{cname}"
                    ORDER BY freq DESC
                    LIMIT 5
                """)
                profile["top_values"] = [{"value": _safe_val(r["val"]), "count": r["freq"],
                                           "pct": round(r["freq"] / total * 100, 1)} for r in top_rows]
            except Exception:
                profile["top_values"] = []

        # Uniqueness ratio
        profile["uniqueness"] = round(stats["distinct_count"] / non_null * 100, 1) if non_null > 0 else 0

        col_profiles.append(profile)

    # FK referential integrity
    integrity = []
    for fk in fks:
        fc = fk["from_column"]
        tt = fk["to_table"]
        tc = fk["to_column"]
        validate_identifier(fc)
        validate_identifier(tt)
        validate_identifier(tc)
        try:
            orph = await conn.fetchrow(f"""
                SELECT COUNT(*) AS cnt FROM "{table_name}" t
                LEFT JOIN "{tt}" r ON t."{fc}" = r."{tc}"
                WHERE t."{fc}" IS NOT NULL AND r."{tc}" IS NULL
            """)
            integrity.append({
                "constraint": fk.get("constraint_name", ""),
                "from_column": fc, "to_table": tt, "to_column": tc,
                "orphaned_rows": orph["cnt"],
                "status": "clean" if orph["cnt"] == 0 else "orphaned",
            })
        except Exception as e:
            integrity.append({
                "constraint": fk.get("constraint_name", ""),
                "from_column": fc, "to_table": tt, "to_column": tc,
                "orphaned_rows": -1, "status": f"error: {str(e)[:80]}",
            })

    return {
        "table": table_name,
        "row_count": row_count,
        "columns": col_profiles,
        "integrity": integrity,
        "indexes": await _get_indexes_pg(conn, table_name),
    }


async def _profile_table_mysql(conn, table_name: str, db_name: str, columns: list, fks: list) -> Dict:
    """Profile a single table using MySQL."""
    import aiomysql
    validate_identifier(table_name)
    cursor = await conn.cursor(aiomysql.DictCursor)
    try:
        await cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
        row = await cursor.fetchone()
        row_count = row["cnt"]

        col_profiles = []
        for col in columns:
            cname = col["column_name"]
            validate_identifier(cname)
            await cursor.execute(f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(`{cname}`) AS non_null,
                    COUNT(DISTINCT `{cname}`) AS distinct_count,
                    MIN(`{cname}`) AS min_val,
                    MAX(`{cname}`) AS max_val
                FROM `{table_name}`
            """)
            stats = _lower_keys(await cursor.fetchone())
            total = stats["total"] or 0
            non_null = stats["non_null"] or 0
            null_pct = round((1 - non_null / total) * 100, 1) if total > 0 else 0
            dtype = col["data_type"].lower()

            profile = {
                "column": cname,
                "type": col["data_type"],
                "nullable": col.get("is_nullable", "YES") in ("YES", True, 1),
                "total": total,
                "non_null": non_null,
                "null_pct": null_pct,
                "distinct": stats["distinct_count"],
                "min": _safe_val(stats["min_val"]),
                "max": _safe_val(stats["max_val"]),
            }

            if any(k in dtype for k in ("int", "decimal", "float", "double", "numeric", "real")):
                try:
                    await cursor.execute(f"SELECT AVG(`{cname}`) AS avg_val FROM `{table_name}`")
                    avg_row = _lower_keys(await cursor.fetchone())
                    profile["avg"] = round(float(avg_row["avg_val"]), 2) if avg_row["avg_val"] is not None else None
                except Exception:
                    profile["avg"] = None

            if total > 0:
                try:
                    await cursor.execute(f"""
                        SELECT `{cname}` AS val, COUNT(*) AS freq
                        FROM `{table_name}`
                        WHERE `{cname}` IS NOT NULL
                        GROUP BY `{cname}`
                        ORDER BY freq DESC
                        LIMIT 5
                    """)
                    top_rows = [_lower_keys(r) for r in await cursor.fetchall()]
                    profile["top_values"] = [{"value": _safe_val(r["val"]), "count": r["freq"],
                                               "pct": round(r["freq"] / total * 100, 1)} for r in top_rows]
                except Exception:
                    profile["top_values"] = []

            profile["uniqueness"] = round(stats["distinct_count"] / non_null * 100, 1) if non_null > 0 else 0
            col_profiles.append(profile)

        integrity = []
        for fk in fks:
            fc = fk["from_column"]
            tt = fk["to_table"]
            tc = fk["to_column"]
            validate_identifier(fc)
            validate_identifier(tt)
            validate_identifier(tc)
            try:
                await cursor.execute(f"""
                    SELECT COUNT(*) AS cnt FROM `{table_name}` t
                    LEFT JOIN `{tt}` r ON t.`{fc}` = r.`{tc}`
                    WHERE t.`{fc}` IS NOT NULL AND r.`{tc}` IS NULL
                """)
                orph = _lower_keys(await cursor.fetchone())
                integrity.append({
                    "constraint": fk.get("constraint_name", ""),
                    "from_column": fc, "to_table": tt, "to_column": tc,
                    "orphaned_rows": orph["cnt"],
                    "status": "clean" if orph["cnt"] == 0 else "orphaned",
                })
            except Exception as e:
                integrity.append({
                    "constraint": fk.get("constraint_name", ""),
                    "from_column": fc, "to_table": tt, "to_column": tc,
                    "orphaned_rows": -1, "status": f"error: {str(e)[:80]}",
                })

        return {
            "table": table_name,
            "row_count": row_count,
            "columns": col_profiles,
            "integrity": integrity,
            "indexes": await _get_indexes_mysql(conn, table_name, db_name),
        }
    finally:
        await cursor.close()


def _compute_health(profile: Dict) -> Dict:
    """Compute a health score for a profiled table."""
    issues = []
    score = 100

    row_count = profile["row_count"]
    if row_count == 0:
        issues.append({"severity": "info", "msg": "Table is empty"})
        score -= 5

    for col in profile["columns"]:
        # High null rate
        if col["null_pct"] > 50:
            issues.append({"severity": "warning", "msg": f'{col["column"]}: {col["null_pct"]}% nulls'})
            score -= 5
        elif col["null_pct"] > 90:
            issues.append({"severity": "critical", "msg": f'{col["column"]}: {col["null_pct"]}% nulls — nearly empty column'})
            score -= 10

        # Low cardinality on non-boolean
        if col["distinct"] == 1 and col["non_null"] > 10 and col["type"].lower() not in ("boolean", "bool", "tinyint"):
            issues.append({"severity": "warning", "msg": f'{col["column"]}: single value across all rows'})
            score -= 5

        # Perfect uniqueness on large table = potential PK candidate
        if col["uniqueness"] == 100 and col["non_null"] > 100 and not col.get("is_primary"):
            issues.append({"severity": "info", "msg": f'{col["column"]}: 100% unique — potential PK/unique constraint candidate'})

    for intg in profile.get("integrity", []):
        if intg["status"] == "orphaned":
            issues.append({"severity": "critical", "msg": f'FK {intg["from_column"]}→{intg["to_table"]}.{intg["to_column"]}: {intg["orphaned_rows"]} orphaned rows'})
            score -= 15

    return {"score": max(0, min(100, score)), "issues": issues}


@app.post("/api/report")
async def generate_report(req: ReportRequest):
    validate_identifier(req.table_name)
    cfg = req.connection

    # Get full schema to find connected tables
    schema = await get_schema(cfg)
    node_map = {n["id"]: n for n in schema["nodes"]}

    if req.table_name not in node_map:
        raise HTTPException(status_code=404, detail=f"Table '{req.table_name}' not found in schema")

    root_node = node_map[req.table_name]

    # Find all directly connected tables
    connected_tables = set()
    for e in schema["edges"]:
        if e["source"] == req.table_name:
            connected_tables.add(e["target"])
        elif e["target"] == req.table_name:
            connected_tables.add(e["source"])

    tables_to_profile = [req.table_name] + sorted(connected_tables)

    # Build column info map from schema
    col_info = {}
    for n in schema["nodes"]:
        col_info[n["id"]] = [{"column_name": c["name"], "data_type": c["type"],
                               "is_nullable": c.get("nullable", True)} for c in n.get("columns", [])]

    fk_info = {}
    for n in schema["nodes"]:
        fk_info[n["id"]] = n.get("foreign_keys", [])

    profiles = []
    if cfg.db_type == "postgresql":
        import asyncpg
        conn = await get_pg_connection(cfg)
        try:
            for tname in tables_to_profile:
                try:
                    p = await _profile_table_pg(conn, tname, col_info.get(tname, []), fk_info.get(tname, []))
                    p["health"] = _compute_health(p)
                    p["index_recommendations"] = _recommend_indexes(p, p.get("indexes", []))
                    p["is_root"] = tname == req.table_name
                    profiles.append(p)
                except Exception as e:
                    profiles.append({"table": tname, "error": str(e)[:200], "is_root": tname == req.table_name})
        finally:
            await conn.close()
    elif cfg.db_type == "mysql":
        import aiomysql
        conn = await get_mysql_connection(cfg)
        try:
            for tname in tables_to_profile:
                try:
                    p = await _profile_table_mysql(conn, tname, cfg.database, col_info.get(tname, []), fk_info.get(tname, []))
                    p["health"] = _compute_health(p)
                    p["index_recommendations"] = _recommend_indexes(p, p.get("indexes", []))
                    p["is_root"] = tname == req.table_name
                    profiles.append(p)
                except Exception as e:
                    profiles.append({"table": tname, "error": str(e)[:200], "is_root": tname == req.table_name})
        finally:
            conn.close()
    else:
        raise HTTPException(status_code=400, detail="Unsupported db_type.")

    # Summary
    total_rows = sum(p.get("row_count", 0) for p in profiles)
    avg_health = round(sum(p.get("health", {}).get("score", 0) for p in profiles if "health" in p) /
                       max(1, len([p for p in profiles if "health" in p])), 1)
    total_issues = sum(len(p.get("health", {}).get("issues", [])) for p in profiles)
    critical_count = sum(1 for p in profiles for i in p.get("health", {}).get("issues", []) if i["severity"] == "critical")

    return {
        "root_table": req.table_name,
        "tables_profiled": len(profiles),
        "total_rows": total_rows,
        "avg_health_score": avg_health,
        "total_issues": total_issues,
        "critical_issues": critical_count,
        "profiles": profiles,
    }


@app.post("/api/execute-index")
async def execute_index(req: IndexExecuteRequest):
    stripped = req.sql.strip().upper()
    if not (stripped.startswith("CREATE INDEX") or stripped.startswith("CREATE UNIQUE INDEX")):
        raise HTTPException(status_code=400, detail="Only CREATE INDEX / CREATE UNIQUE INDEX statements are permitted.")
    try:
        if req.connection.db_type == "postgresql":
            conn = await get_pg_connection(req.connection)
            try:
                await conn.execute(req.sql)
            finally:
                await conn.close()
        elif req.connection.db_type == "mysql":
            conn = await get_mysql_connection(req.connection)
            try:
                cursor = await conn.cursor()
                await cursor.execute(req.sql)
                await conn.commit()
                await cursor.close()
            finally:
                conn.close()
        else:
            raise HTTPException(status_code=400, detail="Unsupported db_type.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Index creation failed: {str(e)}")
    return {"status": "ok", "message": "Index created successfully."}


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "db-graph-explorer"}
