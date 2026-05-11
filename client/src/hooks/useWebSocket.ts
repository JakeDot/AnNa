import { useState, useEffect, useCallback, useRef } from 'react'
import { io, Socket } from 'socket.io-client'

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:3000'

interface Peer {
  id: string
  connected_at: number
  files: string[]
}

export function useWebSocket() {
  const [connected, setConnected] = useState(false)
  const [peers, setPeers] = useState<Peer[]>([])
  const [peerId, setPeerId] = useState<string | null>(null)
  const socketRef = useRef<Socket | null>(null)

  useEffect(() => {
    // Connect to WebSocket
    const socket = io(WS_URL, {
      transports: ['websocket'],
    })

    socketRef.current = socket

    socket.on('connect', () => {
      console.log('WebSocket connected')
      setConnected(true)
    })

    socket.on('disconnect', () => {
      console.log('WebSocket disconnected')
      setConnected(false)
    })

    socket.on('welcome', (data: { peer_id: string }) => {
      console.log('Received peer ID:', data.peer_id)
      setPeerId(data.peer_id)
    })

    socket.on('peer-list', (data: { peers: string[] }) => {
      console.log('Peer list updated:', data.peers)
      // Convert peer IDs to Peer objects (simplified)
      const peerObjects: Peer[] = data.peers.map((id) => ({
        id,
        connected_at: Date.now(),
        files: [],
      }))
      setPeers(peerObjects)
    })

    socket.on('signal', (data: any) => {
      console.log('Received signal:', data)
      // Handle WebRTC signaling
    })

    socket.on('chunk-peers', (data: { file_hash: string; chunk_id: number; peers: string[] }) => {
      console.log('Chunk peers:', data)
      // Handle chunk peer information
    })

    return () => {
      socket.disconnect()
    }
  }, [])

  const sendMessage = useCallback((message: any) => {
    if (socketRef.current && connected) {
      socketRef.current.emit('message', message)
    }
  }, [connected])

  const joinRoom = useCallback((room: string) => {
    if (socketRef.current && connected) {
      socketRef.current.emit('join', { room })
    }
  }, [connected])

  return {
    connected,
    peers,
    peerId,
    sendMessage,
    joinRoom,
  }
}
