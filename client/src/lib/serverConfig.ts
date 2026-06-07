import { invoke, isTauri } from '@tauri-apps/api/core'

const STORAGE_KEY = 'anna_server_url'
const DEFAULT_URL = 'http://localhost:3000'

export async function getServerUrl(): Promise<string> {
  if (isTauri()) {
    try {
      return await invoke<string>('get_server_url')
    } catch {
      return DEFAULT_URL
    }
  }
  return localStorage.getItem(STORAGE_KEY) || import.meta.env.VITE_API_URL || DEFAULT_URL
}

export async function setServerUrl(url: string): Promise<void> {
  if (isTauri()) {
    await invoke('set_server_url', { url })
  } else {
    localStorage.setItem(STORAGE_KEY, url)
  }
}

export function getServerUrlSync(): string {
  return localStorage.getItem(STORAGE_KEY) || import.meta.env.VITE_API_URL || DEFAULT_URL
}

export function setServerUrlSync(url: string): void {
  localStorage.setItem(STORAGE_KEY, url)
}

export function getWsUrl(serverUrl: string): string {
  return serverUrl.replace(/^http/, 'ws') + '/ws'
}
