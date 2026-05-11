import React, { useCallback, useEffect, useState } from 'react'
import { formatBytes, formatUptime, getServerStatus } from '../api/statusApi'
import type { ServerStatus } from '../api/statusApi'

const POLL_INTERVAL_MS = 5000

export const AdminPanel: React.FC = () => {
  const [status, setStatus] = useState<ServerStatus | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [isH3, setIsH3] = useState(false)

  const fetchStatus = useCallback(async () => {
    try {
      const data = await getServerStatus()
      setStatus(data)
      setError(null)
      setLastUpdated(new Date())
      setIsH3(data.quic_enabled)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error')
    }
  }, [])

  useEffect(() => {
    fetchStatus()
    const id = setInterval(fetchStatus, POLL_INTERVAL_MS)
    return () => clearInterval(id)
  }, [fetchStatus])

  return (
    <div className="admin-panel">
      <div className="admin-header">
        <h2>Server Status</h2>
        <div className="admin-badges">
          {isH3 && <span className="badge badge-h3">HTTP/3 · QUIC</span>}
          <span className={`badge ${status ? 'badge-online' : 'badge-offline'}`}>
            {status ? 'Online' : 'Offline'}
          </span>
        </div>
      </div>

      {error && <div className="admin-error">{error}</div>}

      {lastUpdated && (
        <p className="admin-updated">
          Last updated: {lastUpdated.toLocaleTimeString()} · refreshes every {POLL_INTERVAL_MS / 1000}s
        </p>
      )}

      {status && (
        <>
          {/* ── Summary cards ─────────────────────────────────────────────── */}
          <div className="admin-cards">
            <StatCard label="Uptime" value={formatUptime(status.uptime_secs)} icon="⏱" />
            <StatCard label="Version" value={`v${status.version}`} icon="📦" />
            <StatCard label="Connected peers" value={String(status.peer_count)} icon="🔗" />
            <StatCard label="Files stored" value={String(status.file_count)} icon="📄" />
            <StatCard label="Total stored" value={formatBytes(status.total_bytes)} icon="💾" />
            <StatCard label="Total chunks" value={String(status.total_chunk_count)} icon="🧩" />
            <StatCard label="Active QUIC conns" value={String(status.active_quic_connections)} icon="⚡" />
            <StatCard label="Total QUIC conns" value={String(status.total_quic_connections)} icon="📊" />
          </div>

          {/* ── Peer table ────────────────────────────────────────────────── */}
          <section className="admin-section">
            <h3>Connected Peers ({status.peer_count})</h3>
            {status.peers.length === 0 ? (
              <p className="admin-empty">No peers connected</p>
            ) : (
              <table className="admin-table">
                <thead>
                  <tr>
                    <th>Peer ID</th>
                    <th>Files shared</th>
                    <th>Connected at</th>
                  </tr>
                </thead>
                <tbody>
                  {status.peers.map(p => (
                    <tr key={p.id}>
                      <td className="mono">{p.id}</td>
                      <td>{p.file_count}</td>
                      <td>{new Date(p.connected_at * 1000).toLocaleTimeString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>

          {/* ── File table ────────────────────────────────────────────────── */}
          <section className="admin-section">
            <h3>Stored Files ({status.file_count})</h3>
            {status.files.length === 0 ? (
              <p className="admin-empty">No files stored</p>
            ) : (
              <table className="admin-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Size</th>
                    <th>Chunks</th>
                    <th>Format</th>
                    <th>Uploaded</th>
                    <th>Hash</th>
                  </tr>
                </thead>
                <tbody>
                  {status.files.map(f => (
                    <tr key={f.hash}>
                      <td className="file-name-cell" title={f.name}>{f.name}</td>
                      <td>{formatBytes(f.size)}</td>
                      <td>{f.chunk_count}</td>
                      <td>
                        <span className={`badge ${f.compressed ? 'badge-legacy' : 'badge-cdc'}`}>
                          {f.compressed ? 'Brotli (legacy)' : 'CDC'}
                        </span>
                      </td>
                      <td>{new Date(f.uploaded_at * 1000).toLocaleString()}</td>
                      <td className="mono hash-cell" title={f.hash}>{f.hash.slice(0, 12)}…</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </>
      )}
    </div>
  )
}

// Small reusable stat card
const StatCard: React.FC<{ label: string; value: string; icon: string }> = ({ label, value, icon }) => (
  <div className="stat-card">
    <span className="stat-icon">{icon}</span>
    <div className="stat-body">
      <span className="stat-value">{value}</span>
      <span className="stat-label">{label}</span>
    </div>
  </div>
)
