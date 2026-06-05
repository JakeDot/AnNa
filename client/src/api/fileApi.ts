import axios from 'axios'
import { getServerUrlSync } from '../lib/serverConfig'

function apiBase(): string {
  return getServerUrlSync()
}

export interface UploadResponse {
  status: string
  hash: string
  size: number
  chunk_count: number
}

export interface FileMetadata {
  hash: string
  name: string
  size: number
  mime_type: string
  uploaded_at: number
  chunk_count: number
  /** True only for files uploaded before CDC was introduced (legacy Brotli). */
  compressed: boolean
}

/** CDC chunk boundary returned by /api/chunks/:hash */
export interface ChunkBoundary {
  chunk_id: number
  offset: number
  length: number
  /** Hex-encoded BLAKE3 hash of the chunk bytes for integrity verification. */
  hash: string
}

export interface CheckResponse {
  exists: boolean
  chunks?: number[]
  /** Server bitfield: byte array where bit i of byte ⌊i/8⌋ (MSB-first) means chunk i is available. */
  bitfield?: number[]
}

export async function uploadFile(file: File): Promise<UploadResponse> {
  const formData = new FormData()
  formData.append('file', file)

  const response = await axios.post<UploadResponse>(`${apiBase()}/api/upload`, formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  })

  return response.data
}

export async function listFiles(): Promise<FileMetadata[]> {
  const response = await axios.get<FileMetadata[]>(`${apiBase()}/api/files`)
  return response.data
}

export async function checkFile(hash: string): Promise<CheckResponse> {
  const response = await axios.get<CheckResponse>(`${apiBase()}/api/files/check/${hash}`)
  return response.data
}

export async function downloadFile(hash: string): Promise<Blob> {
  const response = await axios.get(`${apiBase()}/api/download/${hash}`, {
    responseType: 'blob',
  })
  return response.data
}

export function getDownloadUrl(hash: string): string {
  return `${apiBase()}/api/download/${hash}`
}

/**
 * Fetch CDC chunk boundaries for a file.
 *
 * Use this to build a local bitfield of which chunks are needed, then send
 * `pipeline-request` over the WebSocket with a sliding window of chunk IDs
 * to get rarest-first peer assignments from the server.
 */
export async function getChunks(hash: string): Promise<ChunkBoundary[]> {
  const response = await axios.get<ChunkBoundary[]>(`${apiBase()}/api/chunks/${hash}`)
  return response.data
}

/**
 * Fetch a single chunk's raw bytes from the server fallback endpoint.
 *
 * In normal P2P operation peers exchange chunks directly over WebRTC data
 * channels.  This endpoint is the reliable fallback for chunks that no peer
 * can serve.
 *
 * The response includes an `x-chunk-hash` header with the expected BLAKE3
 * hash; callers should verify it before using the data.
 */
export async function fetchChunk(hash: string, chunkId: number): Promise<ArrayBuffer> {
  const response = await axios.get(`${apiBase()}/api/chunk/${hash}/${chunkId}`, {
    responseType: 'arraybuffer',
  })
  return response.data
}

