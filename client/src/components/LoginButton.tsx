import React from 'react'
import type { User } from '../api/authApi'
import { clearToken, getLoginUrl } from '../api/authApi'

interface LoginButtonProps {
  user: User | null
  onLogout: () => void
}

export const LoginButton: React.FC<LoginButtonProps> = ({ user, onLogout }) => {
  const handleLogin = () => {
    window.location.href = getLoginUrl()
  }

  const handleLogout = () => {
    clearToken()
    onLogout()
  }

  if (user) {
    return (
      <div className="user-info">
        {user.avatar_url && (
          <img src={user.avatar_url} alt={user.name} className="user-avatar" />
        )}
        <span className="user-name">{user.name}</span>
        <button className="button-small" onClick={handleLogout}>
          Logout
        </button>
      </div>
    )
  }

  return (
    <button className="button" onClick={handleLogin}>
      Login with GitHub
    </button>
  )
}
