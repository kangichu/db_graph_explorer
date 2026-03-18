import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import './index.css'

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { hasError: false, error: null }
  }
  static getDerivedStateFromError(error) {
    return { hasError: true, error }
  }
  componentDidCatch(error, info) {
    console.error('App crashed:', error, info)
  }
  render() {
    if (this.state.hasError) {
      return (
        <div style={{ padding: 40, color: '#e0eef5', fontFamily: 'monospace', background: '#080c10', height: '100vh' }}>
          <h2 style={{ color: '#ff4060' }}>Something went wrong</h2>
          <pre style={{ color: '#7a9aaa', marginTop: 16, whiteSpace: 'pre-wrap' }}>{this.state.error?.message}</pre>
          <button
            onClick={() => { this.setState({ hasError: false, error: null }) }}
            style={{ marginTop: 20, padding: '8px 20px', background: '#00d4ff', color: '#080c10', border: 'none', cursor: 'pointer', fontWeight: 'bold' }}
          >
            Reload App
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </React.StrictMode>
)
