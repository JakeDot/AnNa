import { useState, useEffect, useCallback } from 'react'
import './App.css'
import { FileUploader } from './components/FileUploader'
import { FileList } from './components/FileList'
import { PeerStatus } from './components/PeerStatus'
import { useWebSocket } from './hooks/useWebSocket'
import { uploadFile, listFiles } from './api/fileApi'

export interface FileMetadata {
  hash: string
  name: string
  size: number
  mime_type: string
  uploaded_at: number
  chunk_count: number
  compressed: boolean
}

function App() {
  const [files, setFiles] = useState<FileMetadata[]>([])
  const [uploading, setUploading] = useState(false)
  const { connected, peers, sendMessage } = useWebSocket()

  const fetchFiles = useCallback(async () => {
    try {
      const fileList = await listFiles()
      setFiles(fileList)
    } catch (error) {
      console.error('Failed to fetch files:', error)
    }
  }, [])

  useEffect(() => {
    fetchFiles()
  }, [fetchFiles])

  const handleFileUpload = async (file: File) => {
    setUploading(true)
    try {
      const result = await uploadFile(file)
      console.log('Upload result:', result)

      // Refresh file list
      await fetchFiles()

      // Announce new file to peers
      if (connected) {
        sendMessage({
          type: 'announce',
          files: [result.hash]
        })
      }
    } catch (error) {
      console.error('Upload failed:', error)
      alert('Upload failed: ' + error)
    } finally {
      setUploading(false)
    }
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>ãnn@sync</h1>
        <p className="tagline">P2P File Sync Platform</p>
        <PeerStatus connected={connected} peerCount={peers.length} />
      </header>

      <main className="app-main">
        <section className="upload-section">
          <h2>Upload Files</h2>
          <FileUploader onUpload={handleFileUpload} uploading={uploading} />
        </section>

        <section className="files-section">
          <h2>Your Files ({files.length})</h2>
          <FileList files={files} />
        </section>
      </main>

      <footer className="app-footer">
        <p>Built with Rust + React + WebRTC</p>
      </footer>
    </div>
  )
}

export default App
