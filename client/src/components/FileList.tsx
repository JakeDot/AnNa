import React from 'react'
import { FileMetadata } from '../App'
import { getDownloadUrl } from '../api/fileApi'

interface FileListProps {
  files: FileMetadata[]
}

export const FileList: React.FC<FileListProps> = ({ files }) => {
  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
  }

  const formatDate = (timestamp: number): string => {
    const date = new Date(timestamp * 1000)
    return date.toLocaleString()
  }

  if (files.length === 0) {
    return <div className="empty-state">No files uploaded yet</div>
  }

  return (
    <div className="file-list">
      {files.map((file) => (
        <div key={file.hash} className="file-item">
          <div className="file-icon">
            {file.mime_type.startsWith('image/') ? '🖼️' : '📄'}
          </div>
          <div className="file-info">
            <div className="file-name">{file.name}</div>
            <div className="file-meta">
              {formatBytes(file.size)} • {formatDate(file.uploaded_at)}
              {file.compressed && <span className="badge">Compressed</span>}
              <span className="badge">{file.chunk_count} chunks</span>
            </div>
          </div>
          <div className="file-actions">
            <a
              href={getDownloadUrl(file.hash)}
              download={file.name}
              className="button-small"
            >
              Download
            </a>
          </div>
        </div>
      ))}
    </div>
  )
}
