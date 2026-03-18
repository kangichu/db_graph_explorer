# DB Graph Explorer "

A full-stack interactive database schema visualizer, profiler, and editor.
Supports **PostgreSQL** and **MySQL** with a live Cytoscape.js graph, data profiling reports, index analysis, and safe schema editing.

---

## Quick Start

### 1. Backend (FastAPI)

```bash
cd backend
python -m venv venv
source venv/bin/activate         # Windows: venv\Scripts\activate

pip install -r requirements.txt

uvicorn main:app --reload --port 8000
```

Backend runs at: http://localhost:8000
API docs at:    http://localhost:8000/docs

---

### 2. Frontend (React + Vite)

```bash
cd frontend
npm install
npm run dev
```

Frontend runs at: http://localhost:5173

---

## Features

### Connection
- Fill in host/port/user/password/database
- Toggle between PostgreSQL and MySQL
- "Test Connect" verifies credentials before loading
- "Load Schema" fetches full schema once, then all filtering/paging is client-side

### Graph Visualization
- Tables = draggable rounded-rectangle nodes sized by degree
- Foreign keys = directed edges
- Color coding by connectivity (configurable thresholds via ⚙ gear icon):
  - 🟡 **Amber** = High-degree hub tables (default ≥20% of max)
  - 🔵 **Cyan** = Normal connected tables (default ≥8% of max)
  - 🩵 **Teal** = Low connectivity (1+ connections below normal)
  - ⬛ **Gray** = Isolated (no foreign key relationships)
- Click legend items to filter the graph by connectivity category
- Neighbor context: filtered views show connected neighbor tables with separate pagination

### Table List (Left Panel)
- Displays **all tables** in the database, sorted alphabetically
- Content indicator dots: 🟢 green = has data, ○ hollow = empty table
- Tables visible on the current graph page shown as bright; off-graph tables shown dimmed
- Click any table to navigate — auto-pages to the correct graph page and focuses the node

### Search & Focus
- Real-time search filter across all tables
- Click → zooms & centers the graph, highlights neighborhood

### Neighborhood Exploration
- 1-hop: directly connected tables only
- 2-hop: extend to 2nd-degree connections
- All: show entire connected subgraph

### Data Profiling Reports
- Select any table → click **📊 Generate Report**
- Report opens in a **new browser tab** with full profiling data
- Profiles the selected table **+ all directly connected tables**
- Per-column statistics:
  - Null %, distinct count, uniqueness %, min/max values, average (numeric)
  - Top 5 most frequent values with bar charts and skew warnings
- **Health scoring** (0–100) with severity-coded issues (critical/warning/info)
- **FK integrity checks** — detects orphaned foreign key references
- **Actionable recommendations** per table (drop unused columns, add NOT NULL, normalize enums, fix orphans)
- Printable via built-in Print button

### Index Analysis
- Automatically introspects **existing indexes** for every profiled table
- Shows index name, type (B-Tree, Hash, etc.), columns, and PK/UNIQUE/INDEX badges
- **Recommends missing indexes** based on:
  - FK columns without indexes (high impact — causes full table scans on JOINs)
  - High-uniqueness columns (high impact — ideal for lookups)
  - Low-cardinality columns on large tables (medium impact — speeds up filtered queries)
- Each recommendation includes: impact level, explanation, and the exact `CREATE INDEX` SQL
- **One-click index creation** — execute recommended indexes directly from the report with success/failure feedback

### Schema Editing (SAFE — SQL Preview Required)
- Add FK: specify from_table.column → to_table.column
- Drop FK: specify table + constraint name
- Always generates SQL first → shows preview modal → requires confirmation
- Never auto-executes

### Pagination
- Large schemas: loads 40 tables per graph page
- Neighbor tables paginated separately (20 per page)
- Pagination controls appear at bottom of graph

### Graph Controls (bottom-right)
- `+` / `−` Zoom in/out
- `⊡` Fit all nodes
- `↺` Re-run cola layout

---

## API Endpoints

| Method | Path                             | Description                              |
|--------|----------------------------------|------------------------------------------|
| POST   | `/api/test-connection`           | Test DB connectivity                     |
| POST   | `/api/schema`                    | Full schema introspection                |
| POST   | `/api/schema/partial`            | Paginated schema slice (server-side)     |
| POST   | `/api/relationship/preview-add`  | Generate ADD FK SQL (no execute)         |
| POST   | `/api/relationship/preview-delete` | Generate DROP FK SQL (no execute)      |
| POST   | `/api/relationship/execute`      | Execute pre-approved ALTER TABLE         |
| POST   | `/api/report`                    | Generate data profiling report           |
| POST   | `/api/execute-index`             | Execute a CREATE INDEX statement         |
| GET    | `/api/health`                    | Health check                             |

---

## Architecture

```
db-graph-explorer/
├── backend/
│   ├── main.py            # FastAPI app — routes, introspection, profiling engine
│   └── requirements.txt
├── frontend/
│   ├── App.jsx            # Full React app + Cytoscape + report page
│   ├── index.css          # Industrial Blueprint design system
│   ├── main.jsx           # Entry point with ErrorBoundary
│   ├── index.html
│   ├── package.json
│   └── vite.config.js     # Proxies /api → localhost:8000
└── .gitignore
```

---

## Safety Model

- **ALTER TABLE** endpoint only accepts `ALTER TABLE` statements
- **CREATE INDEX** endpoint only accepts `CREATE INDEX` / `CREATE UNIQUE INDEX`
- All SQL identifiers validated against `^[a-zA-Z0-9_]+$` regex
- No DDL auto-runs — every schema change requires user confirmation via SQL preview
- Report profiling uses parameterized queries (no SQL injection)
- Index recommendations generate safe, validated SQL
