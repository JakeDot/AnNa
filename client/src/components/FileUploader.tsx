import React, { useCallback } from 'react'

interface FileUploaderProps {
  onUpload: (file: File) => Promise<void>
  uploading: boolean
}

export const FileUploader: React.FC<FileUploaderProps> = ({ onUpload, uploading }) => {
  const uploadAll = useCallback(
    (files: File[]) => {
      if (files.length === 0) return
      // Upload files sequentially to avoid overloading the server
      files.reduce((chain, file) => chain.then(() => onUpload(file)), Promise.resolve())
    },
    [onUpload]
  )

  const handleDrop = useCallback(
    (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault()
      if (uploading) return
      uploadAll(Array.from(e.dataTransfer.files))
    },
    [uploading, uploadAll]
  )

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault()
  }

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      uploadAll(Array.from(e.target.files))
      // Reset input so the same file(s) can be re-uploaded if needed
      e.target.value = ''
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
          <div className="upload-buttons">
            <label className="file-input-label">
              <input
                type="file"
                multiple
                onChange={handleFileInput}
                disabled={uploading}
                style={{ display: 'none' }}
              />
              <span className="button">Choose Files</span>
            </label>
            <label className="file-input-label">
              {/* webkitdirectory lets the user pick an entire folder */}
              <input
                type="file"
                // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                // @ts-ignore — non-standard but universally supported attribute
                webkitdirectory=""
                multiple
                onChange={handleFileInput}
                disabled={uploading}
                style={{ display: 'none' }}
              />
              <span className="button">Upload Folder</span>
            </label>
          </div>
        </>
      )}
    </div>
  )
}

