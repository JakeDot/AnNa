// Status / management API — talks to GET /api/status

export interface FileStats {
  hash: string
  name: string
  size: number
  chunk_count: number
  uploaded_at: number
  compressed: boolean
}

export interface PeerStats {
  id: string
  connected_at: number
  file_count: number
}

export interface ServerStatus {
  version: string
  uptime_secs: number
  peer_count: number
  file_count: number
  total_bytes: number
  total_chunk_count: number
  active_quic_connections: number
  total_quic_connections: number
  /** True when the server has QUIC/HTTP3 listeners active. */
  quic_enabled: boolean
  files: FileStats[]
  peers: PeerStats[]
}

const API_BASE = import.meta.env.VITE_API_URL || ''

export async function getServerStatus(): Promise<ServerStatus> {
  const resp = await fetch(`${API_BASE}/api/status`)
  if (!resp.ok) throw new Error(`Status fetch failed: ${resp.status}`)
  return resp.json() as Promise<ServerStatus>
}

/** Format bytes into a human-readable string (KB / MB / GB). */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`
}

/** Format an uptime in seconds into "Xh Ym Zs". */
export function formatUptime(secs: number): string {
  const h = Math.floor(secs / 3600)
  const m = Math.floor((secs % 3600) / 60)
  const s = secs % 60
  if (h > 0) return `${h}h ${m}m ${s}s`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}
