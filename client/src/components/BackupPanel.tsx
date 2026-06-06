import { useState } from 'react'
import { backupToServer, restoreFromServer, listServerFiles, LocalFile } from '../api/fileApi'
import { formatBytes } from '../api/statusApi'

interface Props {
  localFiles: LocalFile[]
  onRestored: (file: LocalFile) => void
}

export function BackupPanel({ localFiles, onRestored }: Props) {
  const [serverFiles, setServerFiles] = useState<LocalFile[]>([])
  const [loading, setLoading] = useState(false)
  const [status, setStatus] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'backup' | 'restore'>('backup')

  const localHashes = new Set(localFiles.map((f) => f.hash))

  const handleBackup = async (hash: string, name: string) => {
    setStatus(`Backing up ${name}…`)
    try {
      await backupToServer(hash)
      setStatus(`✓ ${name} backed up to server`)
    } catch (e) {
      setStatus(`✗ Backup failed: ${e}`)
    }
  }

  const handleLoadServer = async () => {
    setLoading(true)
    setStatus(null)
    try {
      const files = await listServerFiles()
      setServerFiles(files)
    } catch (e) {
      setStatus(`✗ Could not reach server: ${e}`)
    } finally {
      setLoading(false)
    }
  }

  const handleRestore = async (hash: string, name: string) => {
    setStatus(`Restoring ${name}…`)
    try {
      const file = await restoreFromServer(hash)
      onRestored(file)
      setStatus(`✓ ${name} restored to device`)
    } catch (e) {
      setStatus(`✗ Restore failed: ${e}`)
    }
  }

  return (
    <div className="backup-panel">
      <div className="backup-tabs">
        <button
          className={`backup-tab ${activeTab === 'backup' ? 'active' : ''}`}
          onClick={() => setActiveTab('backup')}
        >
          ☁ Backup to Server
        </button>
        <button
          className={`backup-tab ${activeTab === 'restore' ? 'active' : ''}`}
          onClick={() => { setActiveTab('restore'); handleLoadServer() }}
        >
          ⬇ Restore from Server
        </button>
      </div>

      {status && <p className="backup-status">{status}</p>}

      {activeTab === 'backup' && (
        <div>
          {localFiles.length === 0 ? (
            <p className="admin-empty">No local files to back up.</p>
          ) : (
            <table className="admin-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Size</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {localFiles.map((f) => (
                  <tr key={f.hash}>
                    <td className="file-name-cell">{f.name}</td>
                    <td>{formatBytes(f.size)}</td>
                    <td>
                      <button
                        className="btn-primary"
                        style={{ fontSize: '0.8rem', padding: '0.3rem 0.8rem' }}
                        onClick={() => handleBackup(f.hash, f.name)}
                      >
                        Back up
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      {activeTab === 'restore' && (
        <div>
          {loading && <p className="admin-empty">Loading server files…</p>}
          {!loading && serverFiles.length === 0 && (
            <p className="admin-empty">No files found on server.</p>
          )}
          {serverFiles.length > 0 && (
            <table className="admin-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Size</th>
                  <th>Status</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {serverFiles.map((f) => (
                  <tr key={f.hash}>
                    <td className="file-name-cell">{f.name}</td>
                    <td>{formatBytes(f.size)}</td>
                    <td>
                      <span className={`badge ${localHashes.has(f.hash) ? 'badge-online' : 'badge-legacy'}`}>
                        {localHashes.has(f.hash) ? 'On device' : 'Server only'}
                      </span>
                    </td>
                    <td>
                      {!localHashes.has(f.hash) && (
                        <button
                          className="btn-primary"
                          style={{ fontSize: '0.8rem', padding: '0.3rem 0.8rem' }}
                          onClick={() => handleRestore(f.hash, f.name)}
                        >
                          Restore
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}
