import axios from 'axios'

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:3000'

const TOKEN_KEY = 'anna_auth_token'

export interface User {
  id: string
  github_id: string | null
  email: string | null
  name: string
  avatar_url: string | null
  created_at: number
}

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY)
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token)
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY)
}

export function getLoginUrl(): string {
  return `${API_BASE}/api/auth/github`
}

export function getAuthHeaders(): Record<string, string> {
  const token = getToken()
  return token ? { Authorization: `Bearer ${token}` } : {}
}

export async function getCurrentUser(): Promise<User | null> {
  const token = getToken()
  if (!token) return null
  try {
    const response = await axios.get<User>(`${API_BASE}/api/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
    })
    return response.data
  } catch {
    return null
  }
}

export function logout(): void {
  clearToken()
}
