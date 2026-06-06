import React, { useCallback, useEffect, useState } from 'react'
import type { User } from '../api/authApi'
import type { Group, GroupWithMembers } from '../api/groupsApi'
import { createGroup, deleteGroup, getGroup, listGroups } from '../api/groupsApi'

interface GroupsPanelProps {
  currentUser: User | null
}

export const GroupsPanel: React.FC<GroupsPanelProps> = ({ currentUser }) => {
  const [groups, setGroups] = useState<Group[]>([])
  const [selectedGroup, setSelectedGroup] = useState<GroupWithMembers | null>(null)
  const [newGroupName, setNewGroupName] = useState('')
  const [newGroupDesc, setNewGroupDesc] = useState('')
  const [creating, setCreating] = useState(false)

  const fetchGroups = useCallback(async () => {
    try {
      const data = await listGroups()
      setGroups(data)
    } catch (e) {
      console.error('Failed to fetch groups:', e)
    }
  }, [])

  useEffect(() => {
    fetchGroups()
  }, [fetchGroups])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!newGroupName.trim()) return
    setCreating(true)
    try {
      await createGroup(newGroupName.trim(), newGroupDesc.trim() || undefined)
      setNewGroupName('')
      setNewGroupDesc('')
      await fetchGroups()
    } catch (e) {
      alert('Failed to create group: ' + e)
    } finally {
      setCreating(false)
    }
  }

  const handleSelect = async (id: string) => {
    try {
      const g = await getGroup(id)
      setSelectedGroup(g)
    } catch (e) {
      console.error('Failed to fetch group details:', e)
    }
  }

  const handleDelete = async (id: string) => {
    if (!confirm('Delete this group? This cannot be undone.')) return
    try {
      await deleteGroup(id)
      if (selectedGroup?.id === id) setSelectedGroup(null)
      await fetchGroups()
    } catch (e) {
      alert('Failed to delete group: ' + e)
    }
  }

  return (
    <div className="groups-panel">
      <h2>Groups</h2>

      {currentUser ? (
        <form className="create-form" onSubmit={handleCreate}>
          <h3>Create Group</h3>
          <input
            type="text"
            className="text-input"
            placeholder="Group name"
            value={newGroupName}
            onChange={e => setNewGroupName(e.target.value)}
            required
          />
          <input
            type="text"
            className="text-input"
            placeholder="Description (optional)"
            value={newGroupDesc}
            onChange={e => setNewGroupDesc(e.target.value)}
          />
          <button type="submit" disabled={creating} className="button">
            {creating ? 'Creating…' : 'Create Group'}
          </button>
        </form>
      ) : (
        <p className="admin-empty">Log in to create and manage groups.</p>
      )}

      <div className="groups-list">
        {groups.length === 0 ? (
          <p className="admin-empty">No groups yet.</p>
        ) : (
          groups.map(g => (
            <div key={g.id} className="group-item">
              <div className="group-info" onClick={() => handleSelect(g.id)}>
                <strong>{g.name}</strong>
                {g.description && (
                  <span className="group-desc"> — {g.description}</span>
                )}
              </div>
              {currentUser?.id === g.owner_id && (
                <button
                  className="button-small button-danger"
                  onClick={() => handleDelete(g.id)}
                >
                  Delete
                </button>
              )}
            </div>
          ))
        )}
      </div>

      {selectedGroup && (
        <div className="group-detail">
          <h3>
            {selectedGroup.name} — Members ({selectedGroup.members.length})
          </h3>
          <table className="admin-table">
            <thead>
              <tr>
                <th>User ID</th>
                <th>Role</th>
                <th>Joined</th>
              </tr>
            </thead>
            <tbody>
              {selectedGroup.members.map(m => (
                <tr key={m.user_id}>
                  <td className="mono">{m.user_id.slice(0, 12)}…</td>
                  <td>
                    <span className="badge">{m.role}</span>
                  </td>
                  <td>{new Date(m.joined_at * 1000).toLocaleDateString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
