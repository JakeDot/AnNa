import React, { useCallback } from 'react'

interface FileUploaderProps {
  onUpload: (file: File) => Promise<void>
  uploading: boolean
}

export const FileUploader: React.FC<FileUploaderProps> = ({ onUpload, uploading }) => {
  const handleDrop = useCallback(
    (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault()
      if (uploading) return

      const files = Array.from(e.dataTransfer.files)
      if (files.length > 0) {
        onUpload(files[0])
      }
    },
    [onUpload, uploading]
  )

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault()
  }

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files
    if (files && files.length > 0) {
      onUpload(files[0])
    }
  }

  return (
    <div
      className={`file-uploader ${uploading ? 'uploading' : ''}`}
      onDrop={handleDrop}
      onDragOver={handleDragOver}
    >
      {uploading ? (
        <div className="upload-status">
          <div className="spinner"></div>
          <p>Uploading...</p>
        </div>
      ) : (
        <>
          <div className="upload-icon">📁</div>
          <p>Drag and drop files here</p>
          <p className="upload-or">or</p>
          <label className="file-input-label">
            <input
              type="file"
              onChange={handleFileInput}
              disabled={uploading}
              style={{ display: 'none' }}
            />
            <span className="button">Choose File</span>
          </label>
        </>
      )}
    </div>
  )
}
