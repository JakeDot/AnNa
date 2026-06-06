import axios from 'axios'
import { invoke, isTauri } from '@tauri-apps/api/core'
import { getServerUrlSync } from '../lib/serverConfig'

// ── Types ─────────────────────────────────────────────────────────────────────

export interface LocalFile {
  hash: string
  name: string
  size: number
  mime_type: string
  uploaded_at: number
  chunk_count: number
}

export interface ChunkBoundary {
  chunk_id: number
  offset: number
  length: number
  hash: string
}

export interface CheckResponse {
  exists: boolean
  chunks?: number[]
  bitfield?: number[]
}

// Legacy type alias kept for components that use FileMetadata
export type FileMetadata = LocalFile & { compressed?: boolean }

// ── Local file operations (Tauri native) ──────────────────────────────────────

export async function storeFileLocally(filePath: string): Promise<LocalFile> {
  return invoke<LocalFile>('store_file', { filePath })
}

export async function listLocalFiles(): Promise<LocalFile[]> {
  if (isTauri()) {
    return invoke<LocalFile[]>('list_local_files')
  }
  // Web fallback: fetch from server
  const resp = await axios.get<LocalFile[]>(`${getServerUrlSync()}/api/files`)
  return resp.data
}

export async function deleteLocalFile(hash: string): Promise<void> {
  return invoke('delete_local_file', { hash })
}

export async function getP2pAddress(): Promise<string> {
  return invoke<string>('get_p2p_address')
}

// ── Upload: store locally then announce (no auto-server upload) ───────────────

export async function uploadFile(file: File): Promise<LocalFile> {
  if (isTauri()) {
    // Write to temp path then hand off to Rust for CDC + local storage
    const tmpPath = await writeTempFile(file)
    return storeFileLocally(tmpPath)
  }
  // Web fallback: upload to server (unchanged behaviour in browser)
  const formData = new FormData()
  formData.append('file', file)
  const resp = await axios.post<LocalFile>(`${getServerUrlSync()}/api/upload?backup=true`, formData)
  return resp.data
}

async function writeTempFile(file: File): Promise<string> {
  // Use Tauri's temp directory via the fs plugin
  const { tempDir } = await import('@tauri-apps/api/path')
  const { writeFile } = await import('@tauri-apps/plugin-fs')
  const tmp = `${await tempDir()}anna_upload_${Date.now()}_${file.name}`
  const buf = await file.arrayBuffer()
  await writeFile(tmp, new Uint8Array(buf))
  return tmp
}

// ── Download: try P2P peers first, fall back to server ────────────────────────

export async function downloadFile(
  hash: string,
  peerAddresses: string[] = [],
): Promise<Blob> {
  // Try each peer's P2P HTTP server first
  for (const addr of peerAddresses) {
    try {
      // Get chunks list
      const chunksResp = await fetch(`http://${addr}/p2p/chunks/${hash}`, { signal: AbortSignal.timeout(3000) })
      if (!chunksResp.ok) continue

      const chunks = await chunksResp.json() as { chunk_id: number }[]

      // Download all chunks from peer
      const chunkBlobs: Blob[] = []
      let success = true
      for (const chunk of chunks) {
        try {
          const chunkResp = await fetch(`http://${addr}/p2p/chunk/${hash}/${chunk.chunk_id}`, {
            signal: AbortSignal.timeout(3000)
          })
          if (!chunkResp.ok) {
            success = false
            break
          }
          chunkBlobs.push(await chunkResp.blob())
        } catch {
          success = false
          break
        }
      }

      if (success && chunkBlobs.length > 0) {
        return new Blob(chunkBlobs)
      }
    } catch {
      // peer unreachable, try next
    }
  }

  // Fall back to server only if all P2P attempts fail
  const resp = await axios.get(`${getServerUrlSync()}/api/download/${hash}`, {
    responseType: 'blob',
  })
  return resp.data
}

export function getDownloadUrl(hash: string, peerAddr?: string): string {
  if (peerAddr) return `http://${peerAddr}/p2p/chunk/${hash}/0`
  return `${getServerUrlSync()}/api/download/${hash}`
}

// ── Explicit server backup / restore ─────────────────────────────────────────

export async function backupToServer(hash: string): Promise<void> {
  const serverUrl = getServerUrlSync()
  if (isTauri()) {
    return invoke('backup_to_server', { hash, serverUrl })
  }
  // Already on server (web mode), nothing to do
}

export async function restoreFromServer(hash: string): Promise<LocalFile> {
  const serverUrl = getServerUrlSync()
  if (isTauri()) {
    return invoke<LocalFile>('restore_from_server', { hash, serverUrl })
  }
  throw new Error('Restore only available in native app')
}

// ── Server file list (for restore UI) ────────────────────────────────────────

export async function listServerFiles(): Promise<FileMetadata[]> {
  const resp = await axios.get<FileMetadata[]>(`${getServerUrlSync()}/api/files`)
  return resp.data
}

export async function getChunks(hash: string): Promise<ChunkBoundary[]> {
  if (isTauri()) {
    return invoke<ChunkBoundary[]>('get_local_chunks', { hash })
  }
  const resp = await axios.get<ChunkBoundary[]>(`${getServerUrlSync()}/api/chunks/${hash}`)
  return resp.data
}

export async function fetchChunk(hash: string, chunkId: number, peerAddr?: string): Promise<ArrayBuffer> {
  const base = peerAddr ? `http://${peerAddr}` : getServerUrlSync()
  const endpoint = peerAddr ? `/p2p/chunk/${hash}/${chunkId}` : `/api/chunk/${hash}/${chunkId}`
  const resp = await axios.get(`${base}${endpoint}`, { responseType: 'arraybuffer' })
  return resp.data
}
