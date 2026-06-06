import { useState, useEffect, useCallback } from 'react'
import { isTauri } from '@tauri-apps/api/core'
import './App.css'
import { FileUploader } from './components/FileUploader'
import { FileList } from './components/FileList'
import { PeerStatus } from './components/PeerStatus'
import { AdminPanel } from './components/AdminPanel'
import { BackupPanel } from './components/BackupPanel'
import { ServerSettings } from './components/ServerSettings'
import { LoginButton } from './components/LoginButton'
import { GroupsPanel } from './components/GroupsPanel'
import { LabelManager } from './components/LabelManager'
import { useWebSocket } from './hooks/useWebSocket'
import { uploadFile, listLocalFiles, getP2pAddress } from './api/fileApi'
import type { LocalFile } from './api/fileApi'
import { getCurrentUser, setToken, getToken } from './api/authApi'
import type { User } from './api/authApi'

type Tab = 'files' | 'backup' | 'groups' | 'labels' | 'admin'

function App() {
  const [files, setFiles] = useState<LocalFile[]>([])
  const [uploading, setUploading] = useState(false)
  const [activeTab, setActiveTab] = useState<Tab>('files')
  const [showSettings, setShowSettings] = useState(false)
  const [p2pAddr, setP2pAddr] = useState<string>('')
  const [currentUser, setCurrentUser] = useState<User | null>(null)
  const { connected, peers, sendMessage } = useWebSocket()

  // Handle OAuth callback: extract token from ?token=... query parameter
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const token = params.get('token')
    if (token) {
      setToken(token)
      window.history.replaceState({}, '', window.location.pathname)
    }
  }, [])

  // Restore logged-in user from stored token
  useEffect(() => {
    if (getToken()) {
      getCurrentUser().then(user => setCurrentUser(user))
    }
  }, [])

  const fetchFiles = useCallback(async () => {
    try {
      const fileList = await listLocalFiles()
      setFiles(fileList)
    } catch (error) {
      console.error('Failed to fetch files:', error)
    }
  }, [])

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void fetchFiles()
    if (isTauri()) {
      getP2pAddress().then(setP2pAddr).catch(console.error)
    }
  }, [fetchFiles])

  const handleFileUpload = async (file: File) => {
    setUploading(true)
    try {
      const result = await uploadFile(file)
      await fetchFiles()
      // Announce to peers via WebSocket so they know this device has the file
      if (connected) {
        sendMessage({ type: 'announce', files: [result.hash] })
      }
    } catch (error) {
      console.error('Upload failed:', error)
      alert('Upload failed: ' + error)
    } finally {
      setUploading(false)
    }
  }

  const handleRestored = (file: LocalFile) => {
    setFiles((prev) => {
      if (prev.some((f) => f.hash === file.hash)) return prev
      return [file, ...prev]
    })
  }

  const handleLogout = () => {
    setCurrentUser(null)
  }

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-top">
          <div>
            <h1>ãnn@sync</h1>
            <p className="tagline">P2P File Sync Platform</p>
          </div>
          <div className="header-right">
            <LoginButton user={currentUser} onLogout={handleLogout} />
            <button
              className="settings-btn"
              onClick={() => setShowSettings(true)}
              title="Server settings"
            >
              ⚙
            </button>
          </div>
        </div>
        <PeerStatus connected={connected} peerCount={peers.length} />
      </header>

      <nav className="app-tabs">
        <button
          className={`tab-btn ${activeTab === 'files' ? 'active' : ''}`}
          onClick={() => setActiveTab('files')}
        >
          📁 Files
        </button>
        <button
          className={`tab-btn ${activeTab === 'backup' ? 'active' : ''}`}
          onClick={() => setActiveTab('backup')}
        >
          ☁ Backup
        </button>
        <button
          className={`tab-btn ${activeTab === 'groups' ? 'active' : ''}`}
          onClick={() => setActiveTab('groups')}
        >
          👥 Groups
        </button>
        <button
          className={`tab-btn ${activeTab === 'labels' ? 'active' : ''}`}
          onClick={() => setActiveTab('labels')}
        >
          🏷️ Labels
        </button>
        <button
          className={`tab-btn ${activeTab === 'admin' ? 'active' : ''}`}
          onClick={() => setActiveTab('admin')}
        >
          ⚡ Status
        </button>
      </nav>

      <main className="app-main">
        {activeTab === 'files' && (
          <>
            <section className="upload-section">
              <h2>Upload Files</h2>
              <p className="section-hint">
                Files are stored on this device. Use the Backup tab to push to the server.
                {p2pAddr && <> Sharing at <code>{p2pAddr}</code>.</>}
              </p>
              <FileUploader onUpload={handleFileUpload} uploading={uploading} />
            </section>

            <section className="files-section">
              <h2>Your Files ({files.length})</h2>
              <FileList files={files} />
            </section>
          </>
        )}

        {activeTab === 'backup' && (
          <section className="backup-section">
            <h2>Server Backup</h2>
            <p className="section-hint">
              Explicitly back up files to the server as a safety net, or restore
              files that only exist on the server to this device.
            </p>
            <BackupPanel localFiles={files} onRestored={handleRestored} />
          </section>
        )}

        {activeTab === 'groups' && <GroupsPanel currentUser={currentUser} />}

        {activeTab === 'labels' && <LabelManager currentUser={currentUser} />}

        {activeTab === 'admin' && <AdminPanel />}
      </main>

      <footer className="app-footer">
        <p>Built with Rust + Tauri + QUIC · P2P first</p>
      </footer>

      {showSettings && (
        <ServerSettings
          onClose={() => setShowSettings(false)}
          onSave={() => fetchFiles()}
        />
      )}
    </div>
  )
}

export default App
