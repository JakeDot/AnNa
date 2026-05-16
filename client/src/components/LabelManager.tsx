import React, { useCallback, useEffect, useState } from 'react'
import type { User } from '../api/authApi'
import type { Label } from '../api/labelsApi'
import { createLabel, deleteLabel, listLabels } from '../api/labelsApi'

interface LabelManagerProps {
  currentUser: User | null
}

export const LabelManager: React.FC<LabelManagerProps> = ({ currentUser }) => {
  const [labels, setLabels] = useState<Label[]>([])
  const [newName, setNewName] = useState('')
  const [newColor, setNewColor] = useState('#6366f1')
  const [creating, setCreating] = useState(false)

  const fetchLabels = useCallback(async () => {
    try {
      const data = await listLabels()
      setLabels(data)
    } catch (e) {
      console.error('Failed to fetch labels:', e)
    }
  }, [])

  useEffect(() => {
    fetchLabels()
  }, [fetchLabels])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!newName.trim()) return
    setCreating(true)
    try {
      await createLabel(newName.trim(), newColor)
      setNewName('')
      await fetchLabels()
    } catch (e) {
      alert('Failed to create label: ' + e)
    } finally {
      setCreating(false)
    }
  }

  const handleDelete = async (id: string) => {
    try {
      await deleteLabel(id)
      await fetchLabels()
    } catch (e) {
      alert('Failed to delete label: ' + e)
    }
  }

  return (
    <div className="label-manager">
      <h2>Labels</h2>

      {currentUser ? (
        <form className="create-form" onSubmit={handleCreate}>
          <h3>Create Label</h3>
          <input
            type="text"
            className="text-input"
            placeholder="Label name"
            value={newName}
            onChange={e => setNewName(e.target.value)}
            required
          />
          <label className="color-label">
            Color:&nbsp;
            <input
              type="color"
              value={newColor}
              onChange={e => setNewColor(e.target.value)}
              className="color-input"
            />
          </label>
          <button type="submit" disabled={creating} className="button">
            {creating ? 'Creating…' : 'Create Label'}
          </button>
        </form>
      ) : (
        <p className="admin-empty">Log in to create labels.</p>
      )}

      <div className="labels-list">
        {labels.length === 0 ? (
          <p className="admin-empty">No labels yet.</p>
        ) : (
          labels.map(l => (
            <div key={l.id} className="label-item">
              <span
                className="label-badge"
                style={{ backgroundColor: l.color }}
              >
                {l.name}
              </span>
              {currentUser?.id === l.owner_id && (
                <button
                  className="button-small"
                  onClick={() => handleDelete(l.id)}
                  title="Delete label"
                >
                  ✕
                </button>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  )
}
