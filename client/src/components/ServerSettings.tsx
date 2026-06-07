import { useState } from 'react'
import { getServerUrlSync, setServerUrl } from '../lib/serverConfig'

interface Props {
  onClose: () => void
  onSave: (url: string) => void
}

export function ServerSettings({ onClose, onSave }: Props) {
  const [url, setUrl] = useState(getServerUrlSync())

  const handleSave = async () => {
    const trimmed = url.trim().replace(/\/$/, '')
    await setServerUrl(trimmed)
    onSave(trimmed)
    onClose()
  }

  return (
    <div className="settings-overlay" onClick={onClose}>
      <div className="settings-modal" onClick={(e) => e.stopPropagation()}>
        <h2>Server Settings</h2>
        <label>
          Server URL
          <input
            type="url"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="http://localhost:3000"
            autoFocus
          />
        </label>
        <p className="settings-hint">
          Address of your AnNa Sync server (e.g. <code>http://192.168.1.10:3000</code>)
        </p>
        <div className="settings-actions">
          <button className="btn-secondary" onClick={onClose}>Cancel</button>
          <button className="btn-primary" onClick={handleSave}>Save</button>
        </div>
      </div>
    </div>
  )
}
