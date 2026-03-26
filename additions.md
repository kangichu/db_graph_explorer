You are working on an existing full-stack application called **DB Graph Explorer**.

### Current Stack

* Backend: FastAPI (Python)
* Frontend: React (Vite) + Cytoscape.js
* DB Support: PostgreSQL and MySQL
* Existing features include:

  * Schema introspection
  * Graph visualization (tables + foreign keys)
  * Data profiling reports
  * Index recommendations
  * Safe schema editing (SQL preview required)

Your task is to extend the system into a **Database Intelligence & Optimization Platform** by implementing the following features in a **modular, production-ready, and scalable way**.

---

# 🔥 1. Query Intelligence Layer

### Backend:

* Add support for ingesting query statistics:

  * PostgreSQL: pg_stat_statements
  * MySQL: slow query log or performance_schema
* Create endpoint:

  * POST /api/query-stats
* Normalize output:

  * query_text
  * execution_count
  * avg_time
  * tables_involved (parsed)

### Frontend:

* Overlay query intensity on graph:

  * Node heat (frequency of usage)
  * Edge thickness (join frequency)
* Add toggle: “Show Query Heatmap”

---

# 🧠 2. AI-Assisted Schema Insights

### Backend:

* Create endpoint:

  * POST /api/schema/analyze
* Input: schema JSON + profiling stats
* Output:

  * architectural observations
  * anti-patterns
  * normalization issues
  * missing constraints

### Frontend:

* Add “Analyze Schema” button
* Show results in a side panel with categorized insights

---

# 🧬 3. Schema Versioning & Diff Engine

### Backend:

* Create table: schema_snapshots
* Store:

  * timestamp
  * schema JSON
* Endpoints:

  * POST /api/schema/snapshot
  * GET /api/schema/snapshots
  * POST /api/schema/diff

### Diff Logic:

* Detect:

  * added/removed tables
  * added/removed columns
  * FK changes
  * index changes

### Frontend:

* Visual diff mode:

  * Green = added
  * Red = removed
  * Yellow = modified

---

# ⚠️ 4. Anomaly Detection Engine

### Backend:

* Track historical profiling stats
* Detect:

  * spikes in null %
  * duplicate increases
  * FK orphan growth

### Endpoint:

* GET /api/anomalies

### Frontend:

* Alert panel:

  * severity (critical/warning/info)
  * timestamped anomalies

---

# 🧱 5. Domain Modeling Layer

### Backend:

* Allow tagging tables with “domain”
* Store mapping:

  * table_name → domain

### Frontend:

* Group tables visually by domain
* Allow collapse/expand domain clusters

---

# 🔗 6. API-to-Database Mapping

### Backend:

* Allow registering endpoints and linked tables
* Endpoint:

  * POST /api/api-mapping

### Frontend:

* Click API endpoint → highlight related tables in graph

---

# ⚡ 7. Migration Simulator

### Backend:

* Extend schema editing endpoints:

  * simulate impact before execution
* Output:

  * affected tables
  * FK dependencies
  * index impact

### Frontend:

* Show “Impact Analysis” modal before confirming changes

---

# 📊 8. Workload-Aware Index Recommendations

### Backend:

* Combine:

  * query stats
  * column usage
* Recommend:

  * composite indexes
  * high-impact indexes

### Extend existing index recommendation engine

---

# 🧩 9. Data Lineage Tracking

### Backend:

* Infer data flow via FK chains
* Endpoint:

  * GET /api/lineage?table=xyz

### Frontend:

* Highlight directional flow across tables

---

# 🔐 10. Security & Risk Scanner

### Backend:

* Detect:

  * PII columns (email, phone, id)
  * missing indexes on auth fields
  * lack of audit trails

### Endpoint:

* GET /api/security-report

### Frontend:

* Security dashboard panel

---

# 🎯 11. Smart Optimization Engine

### Backend:

* Aggregate:

  * anomalies
  * index recommendations
  * schema insights

### Endpoint:

* GET /api/optimize

### Output:

* prioritized action list

### Frontend:

* “Optimize Database” button
* Show actionable checklist

---

# 🌍 12. Multi-Database Comparison

### Backend:

* Allow comparing two schemas
* Endpoint:

  * POST /api/schema/compare

### Frontend:

* Side-by-side or overlay comparison

---

# ⚙️ IMPLEMENTATION RULES

* All features must be modular and independent
* Do NOT break existing endpoints
* Follow current project structure
* Reuse existing schema introspection logic where possible
* Validate all SQL inputs strictly
* Maintain current safety model (no auto execution)
* Write clean, documented, production-level code

---

# 🎯 OUTPUT FORMAT

For each feature:

1. Backend implementation (FastAPI routes + logic)
2. Required database schema changes
3. Frontend implementation (React components/hooks)
4. Integration points with existing system

Do NOT skip steps. Do NOT give high-level summaries. Generate actual code where applicable.

Start with Feature 1 and proceed sequentially.
