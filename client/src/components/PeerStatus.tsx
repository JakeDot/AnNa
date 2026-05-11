import React from 'react'

interface PeerStatusProps {
  connected: boolean
  peerCount: number
}

export const PeerStatus: React.FC<PeerStatusProps> = ({ connected, peerCount }) => {
  return (
    <div className={`peer-status ${connected ? 'connected' : 'disconnected'}`}>
      <span className="status-indicator"></span>
      <span className="status-text">
        {connected ? `Connected • ${peerCount} peers` : 'Disconnected'}
      </span>
    </div>
  )
}
