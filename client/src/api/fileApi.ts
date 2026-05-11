import axios from 'axios'

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:3000'

export interface UploadResponse {
  status: string
  hash: string
  size: number
}

export interface FileMetadata {
  hash: string
  name: string
  size: number
  mime_type: string
  uploaded_at: number
  chunk_count: number
  compressed: boolean
}

export async function uploadFile(file: File): Promise<UploadResponse> {
  const formData = new FormData()
  formData.append('file', file)

  const response = await axios.post<UploadResponse>(`${API_BASE}/api/upload`, formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  })

  return response.data
}

export async function listFiles(): Promise<FileMetadata[]> {
  const response = await axios.get<FileMetadata[]>(`${API_BASE}/api/files`)
  return response.data
}

export async function checkFile(hash: string): Promise<{ exists: boolean; chunks?: number[] }> {
  const response = await axios.get(`${API_BASE}/api/files/check/${hash}`)
  return response.data
}

export async function downloadFile(hash: string): Promise<Blob> {
  const response = await axios.get(`${API_BASE}/api/download/${hash}`, {
    responseType: 'blob',
  })
  return response.data
}

export function getDownloadUrl(hash: string): string {
  return `${API_BASE}/api/download/${hash}`
}
