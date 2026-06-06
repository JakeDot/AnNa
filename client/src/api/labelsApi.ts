import axios from 'axios'
import { getAuthHeaders } from './authApi'

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:3000'

export interface Label {
  id: string
  owner_id: string
  name: string
  color: string
  created_at: number
}

export async function listLabels(): Promise<Label[]> {
  const response = await axios.get<Label[]>(`${API_BASE}/api/labels`, {
    headers: getAuthHeaders(),
  })
  return response.data
}

export async function createLabel(name: string, color: string): Promise<Label> {
  const response = await axios.post<Label>(
    `${API_BASE}/api/labels`,
    { name, color },
    { headers: getAuthHeaders() }
  )
  return response.data
}

export async function deleteLabel(id: string): Promise<void> {
  await axios.delete(`${API_BASE}/api/labels/${id}`, {
    headers: getAuthHeaders(),
  })
}

export async function addFileLabel(hash: string, labelId: string): Promise<void> {
  await axios.post(
    `${API_BASE}/api/files/${hash}/labels`,
    { label_id: labelId },
    { headers: getAuthHeaders() }
  )
}

export async function removeFileLabel(hash: string, labelId: string): Promise<void> {
  await axios.delete(`${API_BASE}/api/files/${hash}/labels/${labelId}`, {
    headers: getAuthHeaders(),
  })
}

export async function getFileLabels(hash: string): Promise<Label[]> {
  const response = await axios.get<Label[]>(`${API_BASE}/api/files/${hash}/labels`)
  return response.data
}
