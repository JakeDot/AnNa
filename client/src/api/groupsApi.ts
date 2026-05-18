import axios from 'axios'
import { getAuthHeaders } from './authApi'

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:3000'

export interface Group {
  id: string
  owner_id: string
  name: string
  description: string | null
  created_at: number
}

export interface GroupMember {
  group_id: string
  user_id: string
  role: string
  joined_at: number
}

export interface GroupWithMembers extends Group {
  members: GroupMember[]
}

export async function listGroups(): Promise<Group[]> {
  const response = await axios.get<Group[]>(`${API_BASE}/api/groups`, {
    headers: getAuthHeaders(),
  })
  return response.data
}

export async function createGroup(name: string, description?: string): Promise<Group> {
  const response = await axios.post<Group>(
    `${API_BASE}/api/groups`,
    { name, description },
    { headers: getAuthHeaders() }
  )
  return response.data
}

export async function getGroup(id: string): Promise<GroupWithMembers> {
  const response = await axios.get<GroupWithMembers>(`${API_BASE}/api/groups/${id}`, {
    headers: getAuthHeaders(),
  })
  return response.data
}

export async function deleteGroup(id: string): Promise<void> {
  await axios.delete(`${API_BASE}/api/groups/${id}`, {
    headers: getAuthHeaders(),
  })
}

export async function addMember(groupId: string, userId: string, role?: string): Promise<void> {
  await axios.post(
    `${API_BASE}/api/groups/${groupId}/members`,
    { user_id: userId, role },
    { headers: getAuthHeaders() }
  )
}

export async function removeMember(groupId: string, userId: string): Promise<void> {
  await axios.delete(`${API_BASE}/api/groups/${groupId}/members/${userId}`, {
    headers: getAuthHeaders(),
  })
}
