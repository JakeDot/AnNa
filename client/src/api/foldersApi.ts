import axios from 'axios'
import { getAuthHeaders } from './authApi'

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:3000'

export interface VirtualFolder {
  id: string
  owner_id: string | null
  name: string
  parent_id: string | null
  created_at: number
}

export interface FolderContents {
  folder: VirtualFolder
  subfolders: VirtualFolder[]
  file_hashes: string[]
}

export async function listFolders(parentId?: string): Promise<VirtualFolder[]> {
  const params: Record<string, string> = {}
  if (parentId) params.parent_id = parentId
  const response = await axios.get<VirtualFolder[]>(`${API_BASE}/api/folders`, {
    params,
    headers: getAuthHeaders(),
  })
  return response.data
}

export async function createFolder(name: string, parentId?: string): Promise<VirtualFolder> {
  const response = await axios.post<VirtualFolder>(
    `${API_BASE}/api/folders`,
    { name, parent_id: parentId ?? null },
    { headers: getAuthHeaders() }
  )
  return response.data
}

export async function getFolderContents(id: string): Promise<FolderContents> {
  const response = await axios.get<FolderContents>(`${API_BASE}/api/folders/${id}`, {
    headers: getAuthHeaders(),
  })
  return response.data
}

export async function addFileToFolder(folderId: string, fileHash: string): Promise<void> {
  await axios.post(
    `${API_BASE}/api/folders/${folderId}/files`,
    { file_hash: fileHash },
    { headers: getAuthHeaders() }
  )
}
