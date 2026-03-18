import { useState, useEffect, useRef, useCallback } from 'react'
import { createRoot } from 'react-dom/client'
import cytoscape from 'cytoscape'
import cola from 'cytoscape-cola'

cytoscape.use(cola)

const API = ''  // uses Vite proxy

// ─── Color helpers ────────────────────────────────────────────────────────

const NODE_COLORS = { high: '#f0a500', normal: '#00d4ff', low: '#1e6a7a', isolated: '#3a5a6a' }
const BORDER_COLORS = { high: '#c08000', normal: '#005870', low: '#005870', isolated: '#2a3a40' }

// ─── Cytoscape style ─────────────────────────────────────────────────────

const CY_STYLE = [
  {
    selector: 'node',
    style: {
      'background-color': 'data(color)',
      'label': 'data(label)',
      'color': '#e0eef5',
      'font-family': 'Share Tech Mono, monospace',
      'font-size': '11px',
      'text-valign': 'center',
      'text-halign': 'center',
      'width': 'data(size)',
      'height': 'data(size)',
      'border-width': 2,
      'border-color': 'data(borderColor)',
      'border-opacity': 0.85,
      'text-outline-width': 2,
      'text-outline-color': '#080c10',
      'transition-property': 'background-color, opacity, border-color, border-width',
      'transition-duration': '200ms',
      'shape': 'roundrectangle',
      'padding': '8px',
    }
  },
  {
    selector: 'node:selected',
    style: {
      'border-color': '#00d4ff',
      'border-width': 3,
      'background-color': '#0d3040',
      'box-shadow': '0 0 12px rgba(0,212,255,0.5)',
    }
  },
  {
    selector: 'node.highlighted',
    style: {
      'border-color': '#00d4ff',
      'border-width': 3,
      'opacity': 1,
    }
  },
  {
    selector: 'node.faded',
    style: { 'opacity': 0.12 }
  },
  {
    selector: 'node.neighbor',
    style: {
      'border-color': '#f0a500',
      'border-width': 2,
      'opacity': 1,
    }
  },
  {
    selector: 'edge',
    style: {
      'width': 1.5,
      'line-color': '#1e3a4a',
      'target-arrow-color': '#1e3a4a',
      'target-arrow-shape': 'triangle',
      'curve-style': 'bezier',
      'opacity': 0.7,
      'transition-property': 'opacity, line-color, width',
      'transition-duration': '200ms',
    }
  },
  {
    selector: 'edge.highlighted',
    style: {
      'line-color': '#00d4ff',
      'target-arrow-color': '#00d4ff',
      'width': 2.5,
      'opacity': 1,
    }
  },
  {
    selector: 'edge.faded',
    style: { 'opacity': 0.05 }
  },
  {
    selector: 'node.neighbor-ctx',
    style: {
      'opacity': 0.5,
      'border-style': 'dashed',
      'border-width': 1,
      'font-size': '9px',
    }
  },
]

// ─── Toast ────────────────────────────────────────────────────────────────

function Toast({ msg, type }) {
  if (!msg) return null
  return <div className={`toast ${type}`}>{msg}</div>
}

// ─── SQL Modal ────────────────────────────────────────────────────────────

function SQLModal({ sql, operation, onConfirm, onCancel, loading }) {
  return (
    <div className="modal-backdrop" onClick={e => e.target === e.currentTarget && onCancel()}>
      <div className="modal">
        <div className="modal-header">
          <span className="modal-title">⚡ SQL Preview — {operation}</span>
          <button className="modal-close" onClick={onCancel}>✕</button>
        </div>
        <div className="modal-body">
          <div className="modal-warning">
            <span>⚠</span>
            <span>Review this SQL carefully before executing. This operation will modify your database schema and cannot be undone automatically.</span>
          </div>
          <div className="sql-preview">{sql}</div>
        </div>
        <div className="modal-footer">
          <button className="btn-ghost" onClick={onCancel}>Cancel</button>
          <button className="btn-confirm" onClick={onConfirm} disabled={loading}>
            {loading ? 'Executing...' : 'Confirm & Execute'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ─── Report Page (opens in new window) ────────────────────────────────────

function ReportPage({ report, connPayload }) {
  const [creatingIndex, setCreatingIndex] = useState(null)
  const [indexResults, setIndexResults] = useState({})

  const healthColor = score => score >= 80 ? '#2ecc71' : score >= 50 ? '#f0a500' : '#e74c3c'
  const healthLabel = score => score >= 80 ? 'Good' : score >= 50 ? 'Needs Attention' : 'Poor'
  const sevColor = sev => sev === 'critical' ? '#e74c3c' : sev === 'warning' ? '#f0a500' : '#00d4ff'
  const impactColor = imp => imp === 'high' ? '#e74c3c' : imp === 'medium' ? '#f0a500' : '#00d4ff'

  async function handleCreateIndex(sql) {
    setCreatingIndex(sql)
    try {
      const res = await fetch('/api/execute-index', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ connection: connPayload, sql }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Failed')
      setIndexResults(prev => ({ ...prev, [sql]: 'ok' }))
    } catch (e) {
      setIndexResults(prev => ({ ...prev, [sql]: 'error:' + e.message }))
    } finally {
      setCreatingIndex(null)
    }
  }

  function getRecommendations(prof) {
    const recs = []
    if (!prof.columns) return recs
    for (const col of prof.columns) {
      if (col.null_pct >= 90)
        recs.push({ type: 'drop-or-fix', msg: `"${col.column}" is ${col.null_pct}% NULL — consider dropping the column if unused, or setting a DEFAULT value.` })
      else if (col.null_pct > 50)
        recs.push({ type: 'nulls', msg: `"${col.column}" is ${col.null_pct}% NULL — review if this data should be required (NOT NULL).` })
      if (col.uniqueness === 100 && col.non_null > 10)
        recs.push({ type: 'index', msg: `"${col.column}" has 100% unique values — add a UNIQUE index for lookup performance and integrity.` })
      if (col.distinct === 1 && col.non_null > 10 && !['boolean','bool','tinyint'].includes(col.type.toLowerCase()))
        recs.push({ type: 'constant', msg: `"${col.column}" has only 1 distinct value — stores no useful variation.` })
      if (col.distinct > 1 && col.distinct <= 10 && col.non_null > 50 && !['boolean','bool','tinyint'].includes(col.type.toLowerCase()))
        recs.push({ type: 'enum', msg: `"${col.column}" has only ${col.distinct} distinct values — consider ENUM or lookup table.` })
    }
    for (const intg of (prof.integrity || [])) {
      if (intg.status === 'orphaned')
        recs.push({ type: 'integrity', msg: `FK ${intg.from_column}→${intg.to_table}: ${intg.orphaned_rows} orphaned rows.` })
    }
    if (prof.row_count === 0)
      recs.push({ type: 'empty', msg: `Table is empty — verify if this table is in use.` })
    return recs
  }

  const Tip = ({ text }) => <span className="report-tip" title={text}>?</span>

  return (
    <div className="report-page">
      <header className="report-page-header">
        <div className="report-page-title">📊 Data Profiling Report — {report.root_table}</div>
        <button className="btn-ghost" onClick={() => window.print()}>🖨 Print</button>
      </header>

      <div className="report-page-content">
        {/* Guide */}
        <details className="report-guide">
          <summary>📖 How to read this report</summary>
          <div className="report-guide-body">
            <p><strong>Health Score</strong> (0–100) — Overall quality grade. 80+ = good, 50–79 = needs attention, below 50 = serious issues.</p>
            <p><strong>Nulls %</strong> — Percentage of rows where the column is empty.</p>
            <p><strong>Distinct</strong> — Count of unique values. 1 = no variation.</p>
            <p><strong>Unique %</strong> — Distinct / non-null × 100. 100% = candidate for unique index.</p>
            <p><strong>Index Analysis</strong> — Shows existing indexes and recommends new ones. You can create recommended indexes directly from this report.</p>
          </div>
        </details>

        {/* Summary */}
        <div className="report-summary">
          <div className="report-summary-item">
            <div className="report-summary-val">{report.root_table}</div>
            <div className="report-summary-label">Root Table</div>
          </div>
          <div className="report-summary-item">
            <div className="report-summary-val">{report.tables_profiled}</div>
            <div className="report-summary-label">Tables Profiled</div>
          </div>
          <div className="report-summary-item">
            <div className="report-summary-val">{report.total_rows.toLocaleString()}</div>
            <div className="report-summary-label">Total Rows</div>
          </div>
          <div className="report-summary-item">
            <div className="report-summary-val" style={{color: healthColor(report.avg_health_score)}}>{report.avg_health_score}</div>
            <div className="report-summary-label">Avg Health <Tip text="Average health score across profiled tables" /></div>
          </div>
          <div className="report-summary-item">
            <div className="report-summary-val" style={{color: report.critical_issues > 0 ? '#e74c3c' : 'var(--text-secondary)'}}>{report.total_issues}</div>
            <div className="report-summary-label">Issues ({report.critical_issues} critical)</div>
          </div>
        </div>

        {/* Per-table profiles */}
        {report.profiles.map(p => {
          const recs = getRecommendations(p)
          const indexes = p.indexes || []
          const indexRecs = p.index_recommendations || []
          return (
          <div className={`report-table-section ${p.is_root ? 'is-root' : ''}`} key={p.table}>
            <div className="report-table-header">
              <span className="report-table-name">{p.is_root ? '◉ ' : '→ '}{p.table}</span>
              {p.row_count !== undefined && <span className="report-row-count">{p.row_count.toLocaleString()} rows</span>}
              {p.health && (
                <span className="report-health" style={{color: healthColor(p.health.score)}}>
                  ● {p.health.score}/100 — {healthLabel(p.health.score)}
                </span>
              )}
            </div>

            {p.error && <div className="report-error">⚠ {p.error}</div>}

            {p.health?.issues?.length > 0 && (
              <div className="report-issues">
                {p.health.issues.map((iss, i) => (
                  <div className="report-issue" key={i}>
                    <span className="report-issue-sev" style={{color: sevColor(iss.severity)}}>
                      {iss.severity === 'critical' ? '✗' : iss.severity === 'warning' ? '⚠' : 'ℹ'}
                    </span>
                    <span>{iss.msg}</span>
                  </div>
                ))}
              </div>
            )}

            {p.columns && p.columns.length > 0 && (
              <div className="report-columns">
                <table className="report-col-table">
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Type</th>
                      <th>Nulls <Tip text="% of rows with no value" /></th>
                      <th>Distinct <Tip text="Count of unique values" /></th>
                      <th>Unique% <Tip text="Distinct / non-null × 100" /></th>
                      <th>Min</th>
                      <th>Max</th>
                      <th>Avg</th>
                    </tr>
                  </thead>
                  <tbody>
                    {p.columns.map(col => (
                      <tr key={col.column} className={col.null_pct >= 90 ? 'report-row-critical' : col.null_pct > 50 ? 'report-row-warn' : ''}>
                        <td className="report-col-name">{col.column}</td>
                        <td className="report-col-type">{col.type}</td>
                        <td style={{color: col.null_pct > 50 ? '#f0a500' : col.null_pct > 0 ? 'var(--text-secondary)' : '#2ecc71'}}>{col.null_pct}%</td>
                        <td>{col.distinct?.toLocaleString()}</td>
                        <td style={{color: col.uniqueness === 100 ? '#2ecc71' : col.uniqueness < 5 ? '#f0a500' : 'var(--text-muted)'}}>{col.uniqueness}%</td>
                        <td className="report-val" title={col.min}>{col.min != null ? String(col.min).slice(0, 20) : '—'}</td>
                        <td className="report-val" title={col.max}>{col.max != null ? String(col.max).slice(0, 20) : '—'}</td>
                        <td>{col.avg != null ? col.avg : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>

                {p.columns.filter(c => c.top_values?.length > 0).map(col => (
                  <details className="report-top-vals" key={col.column}>
                    <summary>Top values: {col.column}
                      {col.top_values?.[0]?.pct > 80 && <span className="report-skew-warn"> ⚠ Skewed: top value is {col.top_values[0].pct}%</span>}
                    </summary>
                    <div className="report-top-vals-grid">
                      {col.top_values.map((tv, i) => (
                        <div className="report-tv-row" key={i}>
                          <span className="report-tv-val">{tv.value != null ? String(tv.value).slice(0, 40) : 'NULL'}</span>
                          <span className="report-tv-bar">
                            <span style={{width: `${Math.min(100, tv.pct)}%`}} />
                          </span>
                          <span className="report-tv-pct">{tv.count.toLocaleString()} ({tv.pct}%)</span>
                        </div>
                      ))}
                    </div>
                  </details>
                ))}
              </div>
            )}

            {p.integrity?.length > 0 && (
              <div className="report-integrity">
                <div className="report-sub-title">Foreign Key Integrity <Tip text="Checks FK values reference existing parent rows" /></div>
                {p.integrity.map((intg, i) => (
                  <div className={`report-intg-row ${intg.status}`} key={i}>
                    <span>{intg.from_column} → {intg.to_table}.{intg.to_column}</span>
                    <span className={`report-intg-status ${intg.status}`}>
                      {intg.status === 'clean' ? '✓ Clean' : intg.status === 'orphaned' ? `✗ ${intg.orphaned_rows} orphaned` : intg.status}
                    </span>
                  </div>
                ))}
              </div>
            )}

            {/* Existing Indexes */}
            {indexes.length > 0 && (
              <div className="report-indexes">
                <div className="report-sub-title">🗂 Existing Indexes <Tip text="Indexes currently on this table" /></div>
                <table className="report-idx-table">
                  <thead>
                    <tr><th>Index Name</th><th>Type</th><th>Columns</th><th>Properties</th></tr>
                  </thead>
                  <tbody>
                    {indexes.map((idx, i) => (
                      <tr key={i}>
                        <td className="report-idx-name">{idx.name}</td>
                        <td className="report-idx-type">{idx.type}</td>
                        <td className="report-idx-cols">{idx.columns.join(', ')}</td>
                        <td>
                          {idx.is_primary && <span className="idx-badge pk">PK</span>}
                          {idx.is_unique && !idx.is_primary && <span className="idx-badge unique">UNIQUE</span>}
                          {!idx.is_unique && !idx.is_primary && <span className="idx-badge regular">INDEX</span>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Index Recommendations */}
            {indexRecs.length > 0 && (
              <div className="report-index-recs">
                <div className="report-sub-title">⚡ Index Recommendations <Tip text="Suggested indexes based on FK usage and cardinality" /></div>
                {indexRecs.map((rec, i) => {
                  const status = indexResults[rec.sql]
                  return (
                  <div className="report-idx-rec" key={i}>
                    <div className="report-idx-rec-header">
                      <span className="report-idx-impact" style={{color: impactColor(rec.impact)}}>{rec.impact.toUpperCase()}</span>
                      <span className="report-idx-rec-col">{rec.column}</span>
                      <span className="report-idx-rec-type">{rec.index_type}</span>
                    </div>
                    <div className="report-idx-rec-reason">{rec.reason}</div>
                    <div className="report-idx-rec-sql">
                      <code>{rec.sql}</code>
                      {status === 'ok' ? (
                        <span className="idx-created-badge">✓ Created</span>
                      ) : status?.startsWith('error:') ? (
                        <span className="idx-error-badge" title={status.slice(6)}>✗ Failed</span>
                      ) : (
                        <button
                          className="btn-create-index"
                          onClick={() => handleCreateIndex(rec.sql)}
                          disabled={creatingIndex === rec.sql}
                        >
                          {creatingIndex === rec.sql ? '⏳ Creating...' : '▶ Create Index'}
                        </button>
                      )}
                    </div>
                  </div>
                  )
                })}
              </div>
            )}

            {indexes.length === 0 && indexRecs.length === 0 && p.columns && (
              <div className="report-indexes-empty">
                <div className="report-sub-title">🗂 Index Analysis</div>
                <div className="report-idx-none">No indexes found and no recommendations — table may be too small to benefit.</div>
              </div>
            )}

            {recs.length > 0 && (
              <div className="report-recs">
                <div className="report-sub-title">💡 Recommendations</div>
                {recs.map((r, i) => (
                  <div className="report-rec" key={i}>
                    <span className="report-rec-type">{
                      r.type === 'integrity' ? '🔗' :
                      r.type === 'index' ? '⚡' :
                      r.type === 'nulls' || r.type === 'drop-or-fix' ? '🩹' :
                      r.type === 'constant' || r.type === 'enum' ? '🏷️' :
                      r.type === 'empty' ? '📭' : '📌'
                    }</span>
                    <span>{r.msg}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
          )
        })}
      </div>
    </div>
  )
}

function openReportWindow(report, connPayload) {
  const w = window.open('', '_blank')
  if (!w) return

  w.document.title = `Report — ${report.root_table}`

  // Copy stylesheets from parent
  document.querySelectorAll('link[rel="stylesheet"], style').forEach(s => {
    w.document.head.appendChild(s.cloneNode(true))
  })
  document.querySelectorAll('style[data-vite-dev-id]').forEach(s => {
    w.document.head.appendChild(s.cloneNode(true))
  })

  const meta = w.document.createElement('meta')
  meta.name = 'viewport'
  meta.content = 'width=device-width, initial-scale=1'
  w.document.head.appendChild(meta)

  w.document.body.style.overflow = 'auto'
  w.document.body.style.height = 'auto'
  w.document.documentElement.style.overflow = 'auto'
  w.document.documentElement.style.height = 'auto'

  const container = w.document.createElement('div')
  container.id = 'report-root'
  w.document.body.appendChild(container)

  const root = createRoot(container)
  root.render(<ReportPage report={report} connPayload={connPayload} />)
}

// ─── Main App ─────────────────────────────────────────────────────────────

export default function App() {
  // Connection state
  const [conn, setConn] = useState({ host: 'localhost', port: 5432, username: '', password: '', database: '', db_type: 'postgresql' })
  const [connStatus, setConnStatus] = useState(null) // null | 'loading' | 'ok' | 'error'
  const [connMsg, setConnMsg] = useState('')

  // Schema state
  const [schema, setSchema] = useState(null)       // current visible slice
  const fullSchemaRef = useRef(null)                // cached full schema
  const [loadingSchema, setLoadingSchema] = useState(false)
  const [schemaError, setSchemaError] = useState('')
  const [totalTables, setTotalTables] = useState(0)
  const [offset, setOffset] = useState(0)
  const PAGE_SIZE = 40
  const NEIGHBOR_PAGE_SIZE = 20
  const [neighborOffset, setNeighborOffset] = useState(0)
  const [totalNeighbors, setTotalNeighbors] = useState(0)

  // Graph state
  const cyRef = useRef(null)
  const [cyInstance, setCyInstance] = useState(null)
  const [selectedNode, setSelectedNode] = useState(null)
  const [hopCount, setHopCount] = useState(1)
  const pendingFocusRef = useRef(null)

  // Search & Filter
  const [search, setSearch] = useState('')
  const [degreeFilter, setDegreeFilter] = useState(null) // null | 'high' | 'normal' | 'low' | 'isolated'
  const [thresholds, setThresholds] = useState({ high: 20, normal: 8 }) // percentages of maxDegree
  const [showThresholdSettings, setShowThresholdSettings] = useState(false)

  // Edit
  const [editTab, setEditTab] = useState('add') // 'add' | 'remove'
  const [newRel, setNewRel] = useState({ from_table: '', from_column: '', to_table: '', to_column: '', constraint_name: '' })
  const [delRel, setDelRel] = useState({ table_name: '', constraint_name: '' })

  // SQL Modal
  const [sqlModal, setSqlModal] = useState(null) // { sql, operation }
  const [execLoading, setExecLoading] = useState(false)

  // Report
  const [reportLoading, setReportLoading] = useState(false)

  // Toast
  const [toast, setToast] = useState(null)
  const toastTimer = useRef(null)

  const showToast = useCallback((msg, type = 'info') => {
    clearTimeout(toastTimer.current)
    setToast({ msg, type })
    toastTimer.current = setTimeout(() => setToast(null), 3200)
  }, [])

  // ─── API helpers ─────────────────────────────────────────────────────

  const buildConnPayload = useCallback(() => ({
    host: conn.host, port: Number(conn.port),
    username: conn.username, password: conn.password,
    database: conn.database, db_type: conn.db_type,
  }), [conn])

  async function apiPost(path, body) {
    const res = await fetch(`${API}/api${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || 'Request failed')
    return data
  }

  // ─── Connection ───────────────────────────────────────────────────────

  async function handleTestConnect() {
    setConnStatus('loading')
    setConnMsg('')
    try {
      const d = await apiPost('/test-connection', buildConnPayload())
      setConnStatus('ok')
      setConnMsg(d.message)
    } catch (e) {
      setConnStatus('error')
      setConnMsg(e.message)
    }
  }

  // ─── Client-side filtering + pagination helper ─────────────────────

  function computeVisibleSlice(full, filter, pgOffset, nbrOffset) {
    if (!full) return null
    const allNodes = full.nodes
    const allEdges = full.edges
    const maxDeg = Math.max(...allNodes.map(n => n.degree), 1)
    const highT = Math.max(1, Math.ceil(maxDeg * thresholds.high / 100))
    const lowT = Math.max(1, Math.ceil(maxDeg * thresholds.normal / 100))

    function cat(d) {
      if (d === 0) return 'isolated'
      if (d >= highT) return 'high'
      if (d >= lowT) return 'normal'
      return 'low'
    }

    if (!filter) {
      const total = allNodes.length
      const paged = allNodes.slice(pgOffset, pgOffset + PAGE_SIZE)
      const ids = new Set(paged.map(n => n.id))
      const edges = allEdges.filter(e => ids.has(e.source) && ids.has(e.target))
      return { nodes: paged, edges, total, totalNeighbors: 0 }
    }

    // Filtered: primary nodes matching category
    const primary = allNodes.filter(n => cat(n.degree) === filter)
    const primaryIds = new Set(primary.map(n => n.id))
    const total = primary.length
    const pagedPrimary = primary.slice(pgOffset, pgOffset + PAGE_SIZE)
    const pagedPrimaryIds = new Set(pagedPrimary.map(n => n.id))

    // Neighbors of paged primary (not themselves primary)
    const neighborIds = new Set()
    for (const e of allEdges) {
      if (pagedPrimaryIds.has(e.source) && !primaryIds.has(e.target)) neighborIds.add(e.target)
      if (pagedPrimaryIds.has(e.target) && !primaryIds.has(e.source)) neighborIds.add(e.source)
    }
    const allNeighborList = [...neighborIds].sort()
    const totalNbrs = allNeighborList.length
    const pagedNeighborIds = new Set(allNeighborList.slice(nbrOffset, nbrOffset + NEIGHBOR_PAGE_SIZE))

    const nodeMap = Object.fromEntries(allNodes.map(n => [n.id, n]))
    const pagedNeighbors = allNeighborList
      .slice(nbrOffset, nbrOffset + NEIGHBOR_PAGE_SIZE)
      .filter(id => nodeMap[id])
      .map(id => ({ ...nodeMap[id], is_primary_match: false }))

    const markedPrimary = pagedPrimary.map(n => ({ ...n, is_primary_match: true }))
    const nodes = [...markedPrimary, ...pagedNeighbors]
    const nodeIds = new Set(nodes.map(n => n.id))
    const edges = allEdges.filter(e => nodeIds.has(e.source) && nodeIds.has(e.target))
    return { nodes, edges, total, totalNeighbors: totalNbrs }
  }

  function applySlice(filter, pgOffset, nbrOffset) {
    const slice = computeVisibleSlice(fullSchemaRef.current, filter, pgOffset, nbrOffset)
    if (slice) {
      setSchema({ nodes: slice.nodes, edges: slice.edges })
      setTotalTables(slice.total)
      setTotalNeighbors(slice.totalNeighbors)
    }
  }

  // ─── Schema loading (fetches once, then all client-side) ───────────

  async function handleLoadSchema() {
    setLoadingSchema(true)
    setSchemaError('')
    setSchema(null)
    setSelectedNode(null)
    setOffset(0)
    setNeighborOffset(0)
    try {
      const res = await fetch(`${API}/api/schema`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildConnPayload()),
      })
      let data
      try { data = await res.json() } catch { throw new Error('Backend returned invalid response') }
      if (!res.ok) throw new Error(data.detail || `Server error ${res.status}`)
      if (!data.nodes) throw new Error('Invalid schema response (no nodes)')
      fullSchemaRef.current = data
      const slice = computeVisibleSlice(data, degreeFilter, 0, 0)
      setSchema({ nodes: slice.nodes, edges: slice.edges })
      setTotalTables(slice.total)
      setTotalNeighbors(slice.totalNeighbors)
    } catch (e) {
      setSchemaError(e.message)
      showToast(e.message, 'error')
    } finally {
      setLoadingSchema(false)
    }
  }

  function loadMoreTables(newOffset) {
    setOffset(newOffset)
    setNeighborOffset(0)
    applySlice(degreeFilter, newOffset, 0)
  }

  function loadMoreNeighbors(newNeighborOffset) {
    setNeighborOffset(newNeighborOffset)
    applySlice(degreeFilter, offset, newNeighborOffset)
  }

  // ─── Navigate to any table (even off-page) ──────────────────────────

  function navigateToTable(nodeId) {
    const full = fullSchemaRef.current
    if (!full) return

    const allNodes = full.nodes
    const maxDeg = Math.max(...allNodes.map(n => n.degree), 1)
    const highT = Math.max(1, Math.ceil(maxDeg * thresholds.high / 100))
    const lowT = Math.max(1, Math.ceil(maxDeg * thresholds.normal / 100))

    function cat(d) {
      if (d === 0) return 'isolated'
      if (d >= highT) return 'high'
      if (d >= lowT) return 'normal'
      return 'low'
    }

    let targetFilter = degreeFilter
    let list

    if (!degreeFilter) {
      // No filter: find the node in the full list
      list = allNodes
    } else {
      // Check if node belongs to current filter category
      const node = allNodes.find(n => n.id === nodeId)
      if (node && cat(node.degree) === degreeFilter) {
        list = allNodes.filter(n => cat(n.degree) === degreeFilter)
      } else {
        // Node doesn't match current filter — clear it
        targetFilter = null
        list = allNodes
      }
    }

    const idx = list.findIndex(n => n.id === nodeId)
    if (idx < 0) return

    const newOffset = Math.floor(idx / PAGE_SIZE) * PAGE_SIZE
    pendingFocusRef.current = nodeId

    if (targetFilter !== degreeFilter) setDegreeFilter(targetFilter)
    setOffset(newOffset)
    setNeighborOffset(0)
    applySlice(targetFilter, newOffset, 0)
  }

  // ─── Build Cytoscape graph ────────────────────────────────────────────

  useEffect(() => {
    if (!schema || !schema.nodes.length) return

    const maxDegree = Math.max(...schema.nodes.map(n => n.degree), 1)
    const hasPrimaryFlag = schema.nodes.some(n => n.is_primary_match !== undefined)

    const elements = [
      ...schema.nodes.map(n => {
        const isPrimary = !hasPrimaryFlag || n.is_primary_match !== false
        const cat = getNodeCategory(n.degree)
        return {
          data: {
            id: n.id,
            label: n.label,
            degree: n.degree,
            color: isPrimary ? NODE_COLORS[cat] : '#1a2a35',
            borderColor: isPrimary ? BORDER_COLORS[cat] : '#2a3a40',
            size: isPrimary ? Math.max(60, Math.min(120, 60 + n.degree * 8)) : 50,
            nodeData: n,
            isPrimary,
          },
          classes: isPrimary ? '' : 'neighbor-ctx',
        }
      }),
      ...schema.edges.map(e => ({
        data: { id: e.id, source: e.source, target: e.target, edgeData: e }
      })),
    ]

    if (cyRef.current) {
      try {
        const cy = cytoscape({
          container: cyRef.current,
          elements,
          style: CY_STYLE,
          layout: {
            name: 'cola',
            animate: true,
            animationDuration: 800,
            randomize: true,
            nodeSpacing: 40,
            edgeLength: 150,
            maxSimulationTime: 3000,
          },
          wheelSensitivity: 0.3,
          minZoom: 0.05,
          maxZoom: 3,
        })

        cy.on('tap', 'node', evt => {
          const node = evt.target
          const nd = node.data('nodeData')
          setSelectedNode(nd)
          highlightNeighborhood(cy, node.id(), hopCount)
        })

        cy.on('tap', evt => {
          if (evt.target === cy) {
            clearHighlights(cy)
            setSelectedNode(null)
          }
        })

        setCyInstance(cy)

        // Auto-focus if a table was navigated to from the list
        if (pendingFocusRef.current) {
          const focusId = pendingFocusRef.current
          pendingFocusRef.current = null
          setTimeout(() => {
            const target = cy.$(`#${CSS.escape(focusId)}`)
            if (target.length) {
              const nd = target.data('nodeData')
              setSelectedNode(nd || null)
              cy.animate({ center: { eles: target }, zoom: 1.4 }, { duration: 500 })
              highlightNeighborhood(cy, focusId, hopCount)
            }
          }, 900) // wait for cola layout to settle
        }

        return () => { cy.destroy(); setCyInstance(null) }
      } catch (err) {
        console.error('Cytoscape init error:', err)
        showToast('Graph render failed: ' + err.message, 'error')
      }
    }
  }, [schema])

  // ─── Highlight helpers ────────────────────────────────────────────────

  function clearHighlights(cy) {
    cy.elements().removeClass('highlighted faded neighbor')
  }

  function highlightNeighborhood(cy, nodeId, hops) {
    clearHighlights(cy)
    const root = cy.$(`#${CSS.escape(nodeId)}`)
    if (!root.length) return

    let neighborhood = root
    for (let i = 0; i < hops; i++) {
      neighborhood = neighborhood.union(neighborhood.connectedEdges().connectedNodes())
    }
    const connEdges = neighborhood.connectedEdges().filter(e =>
      neighborhood.has(e.source()) && neighborhood.has(e.target())
    )

    cy.elements().addClass('faded')
    root.removeClass('faded').addClass('highlighted')
    neighborhood.filter(n => n.id() !== nodeId).removeClass('faded').addClass('neighbor')
    connEdges.removeClass('faded').addClass('highlighted')
  }

  useEffect(() => {
    if (cyInstance && selectedNode) {
      highlightNeighborhood(cyInstance, selectedNode.id, hopCount)
    }
  }, [hopCount])

  // ─── Search & focus ───────────────────────────────────────────────────

  function focusNode(nodeId) {
    if (!cyInstance) return
    const node = cyInstance.$(`#${CSS.escape(nodeId)}`)
    if (!node.length) return
    const nd = schema.nodes.find(n => n.id === nodeId)
    setSelectedNode(nd || null)
    cyInstance.animate({ center: { eles: node }, zoom: 1.4 }, { duration: 500 })
    highlightNeighborhood(cyInstance, nodeId, hopCount)
  }

  // ─── Graph controls ───────────────────────────────────────────────────

  function fitAll() { cyInstance?.fit(undefined, 40) }
  function zoomIn() { cyInstance?.zoom({ level: (cyInstance.zoom() || 1) * 1.3, renderedPosition: { x: cyInstance.width() / 2, y: cyInstance.height() / 2 } }) }
  function zoomOut() { cyInstance?.zoom({ level: (cyInstance.zoom() || 1) * 0.75, renderedPosition: { x: cyInstance.width() / 2, y: cyInstance.height() / 2 } }) }
  function relayout() {
    cyInstance?.layout({
      name: 'cola', animate: true, animationDuration: 600,
      randomize: false, nodeSpacing: 40, edgeLength: 150, maxSimulationTime: 2000,
    }).run()
  }

  // ─── Schema editing ───────────────────────────────────────────────────

  async function handlePreviewAdd() {
    try {
      const body = { connection: buildConnPayload(), ...newRel }
      const d = await apiPost('/relationship/preview-add', body)
      setSqlModal({ sql: d.sql, operation: 'ADD FOREIGN KEY', body })
    } catch (e) { showToast(e.message, 'error') }
  }

  async function handlePreviewDelete() {
    try {
      const body = { connection: buildConnPayload(), ...delRel }
      const d = await apiPost('/relationship/preview-delete', body)
      setSqlModal({ sql: d.sql, operation: 'DROP CONSTRAINT', body })
    } catch (e) { showToast(e.message, 'error') }
  }

  async function handleExecuteSQL() {
    if (!sqlModal) return
    setExecLoading(true)
    try {
      await apiPost('/relationship/execute', { connection: buildConnPayload(), sql: sqlModal.sql })
      showToast('SQL executed successfully!', 'success')
      setSqlModal(null)
      await handleLoadSchema()
    } catch (e) {
      showToast(e.message, 'error')
    } finally {
      setExecLoading(false)
    }
  }

  // ─── Report generation ────────────────────────────────────────────────

  async function handleGenerateReport(tableName) {
    setReportLoading(true)
    try {
      const data = await apiPost('/report', {
        connection: buildConnPayload(),
        table_name: tableName,
      })
      openReportWindow(data, buildConnPayload())
    } catch (e) {
      showToast('Report failed: ' + e.message, 'error')
    } finally {
      setReportLoading(false)
    }
  }

  // ─── Render ───────────────────────────────────────────────────────────

  const maxDeg = fullSchemaRef.current ? Math.max(...fullSchemaRef.current.nodes.map(n => n.degree), 1) : (schema ? Math.max(...schema.nodes.map(n => n.degree), 1) : 1)
  const highDegThreshold = Math.max(1, Math.ceil(maxDeg * thresholds.high / 100))
  const lowDegThreshold = Math.max(1, Math.ceil(maxDeg * thresholds.normal / 100))

  function getNodeCategory(degree) {
    if (degree === 0) return 'isolated'
    if (degree >= highDegThreshold) return 'high'
    if (degree >= lowDegThreshold) return 'normal'
    return 'low'
  }

  const visibleNodeIds = new Set((schema?.nodes ?? []).map(n => n.id))

  const allTableNodes = (fullSchemaRef.current?.nodes ?? schema?.nodes ?? [])
    .filter(n => !search || n.label.toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => a.label.localeCompare(b.label))

  const filteredNodes = (schema?.nodes ?? []).filter(n => {
    if (search && !n.label.toLowerCase().includes(search.toLowerCase())) return false
    return true
  })

  function toggleDegreeFilter(key) {
    const newFilter = degreeFilter === key ? null : key
    setDegreeFilter(newFilter)
    setOffset(0)
    setNeighborOffset(0)
    setSelectedNode(null)
    applySlice(newFilter, 0, 0)
  }

  return (
    <div className="app-shell no-select">
      {/* ── TopBar ── */}
      <header className="topbar">
        <div className="logo">
          <div className="logo-icon">⬡</div>
          DB Graph Explorer
        </div>
        <div className="topbar-stats">
          {schema && <>
            <div className="stat-pill"><span>Tables</span><span className="val">{totalTables}</span></div>
            <div className="stat-pill"><span>Loaded</span><span className="val">{schema.nodes.length}</span></div>
            <div className="stat-pill"><span>Edges</span><span className="val">{schema.edges.length}</span></div>
            {schema.nodes.filter(n => n.degree >= highDegThreshold).length > 0 &&
              <div className="stat-pill"><span>Hot Nodes</span><span className="val" style={{color:'var(--amber)'}}>{schema.nodes.filter(n => n.degree >= highDegThreshold).length}</span></div>
            }
            {schema.nodes.filter(n => n.degree === 0).length > 0 &&
              <div className="stat-pill"><span>Isolated</span><span className="val" style={{color:'var(--text-muted)'}}>{schema.nodes.filter(n => n.degree === 0).length}</span></div>
            }
          </>}
          {conn.database && <div className="stat-pill"><span>{conn.db_type}</span><span className="val">{conn.database}</span></div>}
        </div>
      </header>

      {/* ── Left Panel ── */}
      <aside className="left-panel">
        {/* Connection */}
        <div className="panel-section">
          <div className="panel-title">// database connection</div>
          <div className="conn-form">
            <div className="db-type-toggle">
              <button className={`db-type-btn ${conn.db_type === 'postgresql' ? 'active' : ''}`} onClick={() => setConn(c => ({ ...c, db_type: 'postgresql', port: 5432 }))}>PostgreSQL</button>
              <button className={`db-type-btn ${conn.db_type === 'mysql' ? 'active' : ''}`} onClick={() => setConn(c => ({ ...c, db_type: 'mysql', port: 3306 }))}>MySQL</button>
            </div>
            <div className="conn-row">
              <div className="field-group" style={{flex:3}}>
                <label>HOST</label>
                <input value={conn.host} onChange={e => setConn(c => ({ ...c, host: e.target.value }))} placeholder="localhost" />
              </div>
              <div className="field-group" style={{flex:1}}>
                <label>PORT</label>
                <input value={conn.port} onChange={e => setConn(c => ({ ...c, port: e.target.value }))} />
              </div>
            </div>
            <div className="field-group">
              <label>DATABASE</label>
              <input value={conn.database} onChange={e => setConn(c => ({ ...c, database: e.target.value }))} placeholder="mydb" />
            </div>
            <div className="conn-row">
              <div className="field-group">
                <label>USERNAME</label>
                <input value={conn.username} onChange={e => setConn(c => ({ ...c, username: e.target.value }))} placeholder="postgres" />
              </div>
              <div className="field-group">
                <label>PASSWORD</label>
                <input type="password" value={conn.password} onChange={e => setConn(c => ({ ...c, password: e.target.value }))} />
              </div>
            </div>
            <div style={{display:'flex',gap:6}}>
              <button className="btn-primary" style={{flex:1}} onClick={handleTestConnect} disabled={connStatus === 'loading'}>
                {connStatus === 'loading' ? 'Testing...' : 'Test Connect'}
              </button>
              <button className="btn-primary" style={{flex:1, borderColor:'var(--border-bright)', color:'var(--text-secondary)'}} onClick={() => { setDegreeFilter(null); handleLoadSchema() }} disabled={loadingSchema}>
                {loadingSchema ? 'Loading...' : 'Load Schema'}
              </button>
            </div>
            {connStatus === 'ok' && <div className="status-badge ok"><span className="status-dot" />  {connMsg}</div>}
            {connStatus === 'error' && <div className="status-badge err"><span className="status-dot" />  {connMsg}</div>}
          </div>
        </div>

        {/* Search */}
        {schema && (
          <div className="panel-section">
            <div className="panel-title">// search tables</div>
            <div className="search-wrap">
              <span className="search-icon">⌕</span>
              <input className="search-input" placeholder="table name..." value={search} onChange={e => setSearch(e.target.value)} />
            </div>
          </div>
        )}

        {/* Hop Controls */}
        {selectedNode && (
          <div className="panel-section">
            <div className="panel-title">// neighborhood hops</div>
            <div className="hop-controls">
              <button className={`hop-btn ${hopCount === 1 ? 'active' : ''}`} onClick={() => setHopCount(1)}>1-hop</button>
              <button className={`hop-btn ${hopCount === 2 ? 'active' : ''}`} onClick={() => setHopCount(2)}>2-hop</button>
              <button className={`hop-btn ${hopCount === 99 ? 'active' : ''}`} onClick={() => setHopCount(99)}>all</button>
              <button className="btn-ghost" style={{marginLeft:'auto'}} onClick={() => { clearHighlights(cyInstance); setSelectedNode(null) }}>clear</button>
            </div>
          </div>
        )}

        {/* Filter & Legend */}
        {schema && (
          <div className="panel-section">
            <div className="panel-title" style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
              <span>// filter by connectivity</span>
              <button
                className="threshold-settings-btn"
                title="Configure thresholds"
                onClick={() => setShowThresholdSettings(s => !s)}
              >⚙</button>
            </div>

            {showThresholdSettings && (
              <div className="threshold-popover">
                <div className="threshold-header">Degree Thresholds (% of max: {maxDeg})</div>
                <div className="threshold-row">
                  <div className="legend-dot" style={{background:'#f0a500'}} />
                  <label>High ≥</label>
                  <input
                    type="number" min="1" max="100"
                    value={thresholds.high}
                    onChange={e => {
                      const v = Math.max(1, Math.min(100, Number(e.target.value) || 1))
                      setThresholds(t => ({...t, high: v}))
                    }}
                  />
                  <span className="threshold-abs">= {highDegThreshold}+</span>
                </div>
                <div className="threshold-row">
                  <div className="legend-dot" style={{background:'#00d4ff'}} />
                  <label>Normal ≥</label>
                  <input
                    type="number" min="1" max="100"
                    value={thresholds.normal}
                    onChange={e => {
                      const v = Math.max(1, Math.min(100, Number(e.target.value) || 1))
                      setThresholds(t => ({...t, normal: v}))
                    }}
                  />
                  <span className="threshold-abs">= {lowDegThreshold}+</span>
                </div>
                <div className="threshold-note">Low = 1–{lowDegThreshold - 1} · Isolated = 0</div>
                <button className="btn-ghost" style={{width:'100%',marginTop:4,fontSize:10}} onClick={() => {
                  setShowThresholdSettings(false)
                  applySlice(degreeFilter, offset, neighborOffset)
                }}>Apply & Close</button>
              </div>
            )}

            <div className="legend">
              {[
                { key: 'high', color: '#f0a500', label: `High-degree (hub) ≥${highDegThreshold}` },
                { key: 'normal', color: '#00d4ff', label: `Normal ≥${lowDegThreshold}` },
                { key: 'low', color: '#1e6a7a', label: `Low 1–${Math.max(1, lowDegThreshold - 1)}` },
                { key: 'isolated', color: '#3a5a6a', label: 'Isolated (0)' },
              ].map(f => (
                <div
                  key={f.key}
                  className={`legend-item clickable ${degreeFilter === f.key ? 'active-filter' : ''}`}
                  onClick={() => toggleDegreeFilter(f.key)}
                >
                  <div className="legend-dot" style={{background: f.color}} />
                  <span>{f.label}</span>
                  {degreeFilter === f.key && <span className="filter-count">{totalTables}</span>}
                </div>
              ))}
              {degreeFilter && (
                <button className="btn-ghost" style={{width:'100%',marginTop:4,fontSize:11}} onClick={() => toggleDegreeFilter(degreeFilter)}>✕ Clear Filter</button>
              )}
            </div>
          </div>
        )}

        {/* Table List */}
        {schema && (
          <div className="table-list">
            {allTableNodes.map(n => {
              const isVisible = visibleNodeIds.has(n.id)
              return (
                <div
                  key={n.id}
                  className={`table-list-item ${selectedNode?.id === n.id ? 'selected' : ''} ${isVisible ? 'on-graph' : 'off-graph'}`}
                  onClick={() => {
                    if (isVisible) focusNode(n.id)
                    else navigateToTable(n.id)
                  }}
                  title={isVisible ? 'Visible on graph — click to focus' : 'Click to navigate to this table'}
                >
                  <span className={`content-dot ${n.row_count > 0 ? 'has-data' : 'empty'}`}
                    title={n.row_count > 0 ? `~${n.row_count.toLocaleString()} rows` : 'Empty table'}
                  />
                  <span style={{overflow:'hidden',textOverflow:'ellipsis'}}>{n.label}</span>
                  <span className={`degree-badge ${n.degree >= highDegThreshold ? 'high' : n.degree === 0 ? 'isolated' : ''}`}>{n.degree}</span>
                </div>
              )
            })}
          </div>
        )}
      </aside>

      {/* ── Graph Center ── */}
      <main className="graph-area">
        {!schema && !loadingSchema && (
          <div className="graph-empty">
            <div className="graph-empty-icon">⬡</div>
            <div className="graph-empty-text">No Schema Loaded</div>
            <div className="graph-empty-sub">Configure connection → Load Schema</div>
          </div>
        )}

        <div id="cy" ref={cyRef} style={{ opacity: schema ? 1 : 0 }} />

        {loadingSchema && (
          <div className="loading-overlay">
            <div className="spinner" />
            <div className="loading-text">INTROSPECTING SCHEMA...</div>
          </div>
        )}

        {/* Graph Controls */}
        {schema && (
          <div className="graph-controls">
            <button className="graph-ctrl-btn" title="Zoom In" onClick={zoomIn}>+</button>
            <button className="graph-ctrl-btn" title="Zoom Out" onClick={zoomOut}>−</button>
            <button className="graph-ctrl-btn" title="Fit All" onClick={fitAll} style={{fontSize:12}}>⊡</button>
            <button className="graph-ctrl-btn" title="Re-layout" onClick={relayout} style={{fontSize:12}}>↺</button>
          </div>
        )}

        {/* Pagination */}
        {schema && totalTables > PAGE_SIZE && (
          <div className="pagination-bar">
            <button className="btn-ghost" onClick={() => loadMoreTables(Math.max(0, offset - PAGE_SIZE))} disabled={offset === 0 || loadingSchema}>◀ Prev</button>
            <span>Page {Math.floor(offset / PAGE_SIZE) + 1} / {Math.ceil(totalTables / PAGE_SIZE)}</span>
            <button className="btn-ghost" onClick={() => loadMoreTables(offset + PAGE_SIZE)} disabled={offset + PAGE_SIZE >= totalTables || loadingSchema}>Next ▶</button>
          </div>
        )}
      </main>

      {/* ── Right Panel ── */}
      <aside className="right-panel">
        {selectedNode ? (
          <>
            <div className="detail-header">
              <div className="detail-table-name">{selectedNode.label}</div>
              <div className="detail-meta">
                <span className="meta-tag">{selectedNode.columns.length} cols</span>
                <span className={`meta-tag ${selectedNode.degree >= highDegThreshold ? 'primary' : selectedNode.degree === 0 ? 'isolated' : ''}`}>
                  {selectedNode.degree} links
                </span>
                <span className={`meta-tag ${selectedNode.row_count > 0 ? '' : 'isolated'}`}>
                  {selectedNode.row_count > 0 ? `~${selectedNode.row_count.toLocaleString()} rows` : 'empty'}
                </span>
                {selectedNode.degree >= highDegThreshold && <span className="meta-tag primary">HUB</span>}
                {selectedNode.degree === 0 && <span className="meta-tag isolated">ISOLATED</span>}
              </div>
            </div>
            <div className="detail-body">
              <button
                className="btn-report"
                onClick={() => handleGenerateReport(selectedNode.id)}
                disabled={reportLoading}
              >
                {reportLoading ? '⏳ Profiling...' : '📊 Generate Report'}
              </button>
              <div className="detail-section-title">⬡ Columns</div>
              {selectedNode.columns.map(col => (
                <div className="col-row" key={col.name}>
                  {col.is_primary && <span className="pk-badge">PK</span>}
                  <span className="col-name">{col.name}</span>
                  <span className="col-type">{col.type}</span>
                  {!col.nullable && <span style={{fontSize:9,color:'var(--text-muted)',marginLeft:'auto',flexShrink:0}}>NOT NULL</span>}
                </div>
              ))}

              {selectedNode.foreign_keys?.length > 0 && (
                <>
                  <div className="detail-section-title">→ Foreign Keys</div>
                  {selectedNode.foreign_keys.map((fk, i) => (
                    <div className="fk-row" key={i} onClick={() => focusNode(fk.to_table)}>
                      <span style={{color:'var(--text-secondary)'}}>{fk.from_column}</span>
                      <span className="fk-arrow">→</span>
                      <span className="fk-table">{fk.to_table}</span>
                      <span className="fk-arrow">.</span>
                      <span style={{color:'var(--text-secondary)'}}>{fk.to_column}</span>
                    </div>
                  ))}
                </>
              )}

              {selectedNode.degree > 0 && schema.edges.filter(e => e.target === selectedNode.id).length > 0 && (
                <>
                  <div className="detail-section-title">← Referenced By</div>
                  {schema.edges.filter(e => e.target === selectedNode.id).map((e, i) => (
                    <div className="fk-row" key={i} onClick={() => focusNode(e.source)}>
                      <span className="fk-table">{e.source}</span>
                      <span className="fk-arrow">→</span>
                      <span style={{color:'var(--text-secondary)'}}>{e.from_column}</span>
                    </div>
                  ))}
                </>
              )}
            </div>
          </>
        ) : (
          <div style={{ padding: '20px 16px', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', fontSize: 12, lineHeight: 1.7 }}>
            <div style={{marginBottom:12, color:'var(--text-secondary)', fontWeight:600, letterSpacing:'0.1em'}}>// INSPECTOR</div>
            <div>Click any table node to inspect its columns, foreign keys, and relationships.</div>
            <div style={{marginTop:12, opacity:0.6}}>• Drag nodes to rearrange<br/>• Scroll to zoom<br/>• Double-click to fit</div>
          </div>
        )}

        {/* Schema Edit Panel */}
        {schema && (
          <div className="edit-panel">
            <div className="panel-title">// schema editor</div>
            <div className="tabs">
              <button className={`tab ${editTab === 'add' ? 'active' : ''}`} onClick={() => setEditTab('add')}>Add FK</button>
              <button className={`tab ${editTab === 'remove' ? 'active' : ''}`} onClick={() => setEditTab('remove')}>Drop FK</button>
            </div>

            {editTab === 'add' ? (
              <div className="edit-form">
                {[
                  ['FROM TABLE', 'from_table', newRel, setNewRel],
                  ['FROM COLUMN', 'from_column', newRel, setNewRel],
                  ['TO TABLE', 'to_table', newRel, setNewRel],
                  ['TO COLUMN', 'to_column', newRel, setNewRel],
                  ['CONSTRAINT NAME (opt)', 'constraint_name', newRel, setNewRel],
                ].map(([label, key, state, setState]) => (
                  <div className="field-group" key={key}>
                    <label>{label}</label>
                    <input
                      value={state[key]}
                      onChange={e => setState(s => ({ ...s, [key]: e.target.value }))}
                      placeholder={key.replace('_', ' ')}
                    />
                  </div>
                ))}
                <button className="btn-primary"
                  onClick={handlePreviewAdd}
                  disabled={!newRel.from_table || !newRel.from_column || !newRel.to_table || !newRel.to_column}
                >
                  Preview SQL →
                </button>
              </div>
            ) : (
              <div className="edit-form">
                <div className="field-group">
                  <label>TABLE NAME</label>
                  <input value={delRel.table_name} onChange={e => setDelRel(s => ({ ...s, table_name: e.target.value }))} placeholder="orders" />
                </div>
                <div className="field-group">
                  <label>CONSTRAINT NAME</label>
                  <input value={delRel.constraint_name} onChange={e => setDelRel(s => ({ ...s, constraint_name: e.target.value }))} placeholder="fk_orders_users" />
                </div>
                <button className="btn-danger"
                  style={{width:'100%', padding:'8px'}}
                  onClick={handlePreviewDelete}
                  disabled={!delRel.table_name || !delRel.constraint_name}
                >
                  Preview DROP SQL →
                </button>
              </div>
            )}
          </div>
        )}
      </aside>

      {/* ── SQL Modal ── */}
      {sqlModal && (
        <SQLModal
          sql={sqlModal.sql}
          operation={sqlModal.operation}
          onConfirm={handleExecuteSQL}
          onCancel={() => setSqlModal(null)}
          loading={execLoading}
        />
      )}

      {/* ── Toast ── */}
      {toast && <Toast msg={toast.msg} type={toast.type} />}
    </div>
  )
}
