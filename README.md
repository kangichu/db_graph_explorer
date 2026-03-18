# DB Graph Explorer — "Google Maps for Database Schema"

A full-stack interactive database schema visualizer and editor.
Supports **PostgreSQL** and **MySQL** with a live Cytoscape.js graph.

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
- "Load Schema" introspects the full schema

### Graph Visualization
- Tables = draggable rounded nodes
- Foreign keys = directed edges
- Color coding:
  - 🟡 AMBER  = High-degree hub tables (≥70% of max connections)
  - 🔵 CYAN   = Normal connected tables
  - 🩵 TEAL   = Low connectivity
  - ⬛ GRAY   = Isolated (no foreign key relationships)

### Search & Focus
- Search the table list in the left panel
- Click any table → zooms & centers the graph on it
- Fades all unrelated nodes

### Neighborhood Exploration
- 1-hop: show only directly connected tables
- 2-hop: extend to 2nd-degree connections
- All: show entire subgraph

### Schema Editing (SAFE — SQL Preview Required)
- Add FK: specify from_table.column → to_table.column
- Drop FK: specify table + constraint name
- Always generates SQL first → shows preview modal → requires confirmation
- Never auto-executes

### Pagination
- Large schemas: loads 40 tables at a time
- Pagination controls appear at bottom of graph for navigation

### Graph Controls (bottom-right)
- +/- Zoom
- ⊡ Fit all nodes
- ↺ Re-run layout

---

## API Endpoints

| Method | Path                           | Description                        |
|--------|--------------------------------|------------------------------------|
| POST   | /api/test-connection           | Test DB connectivity               |
| POST   | /api/schema                    | Full schema introspection          |
| POST   | /api/schema/partial?limit=&offset= | Paginated schema slice         |
| POST   | /api/relationship/preview-add  | Generate ADD FK SQL (no execute)   |
| POST   | /api/relationship/preview-delete | Generate DROP FK SQL (no execute)|
| POST   | /api/relationship/execute      | Execute pre-approved ALTER TABLE   |
| GET    | /api/health                    | Health check                       |

---

## Architecture

```
db-graph-explorer/
├── backend/
│   ├── main.py          # FastAPI app — all routes + introspection
│   └── requirements.txt
└── frontend/
    ├── src/
    │   ├── App.jsx      # Full React app + Cytoscape integration
    │   └── index.css    # Industrial Blueprint design system
    ├── index.html
    ├── package.json
    └── vite.config.js   # Proxies /api → localhost:8000
```

---

## Safety Model

- The execute endpoint **only accepts ALTER TABLE** statements
- All identifiers are validated against `^[a-zA-Z0-9_]+$` regex
- No DDL auto-runs — every change requires user confirmation
- SQL is shown in full before execution
