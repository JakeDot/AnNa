import { useState, useEffect, useCallback, useRef } from 'react'

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:3000/ws'

interface Peer {
  id: string
  connected_at: number
  files: string[]
}

interface WebSocketMessage {
  type: string
  [key: string]: any
}

export function useWebSocket() {
  const [connected, setConnected] = useState(false)
  const [peers, setPeers] = useState<Peer[]>([])
  const [peerId, setPeerId] = useState<string | null>(null)
  const socketRef = useRef<WebSocket | null>(null)
  const reconnectTimeoutRef = useRef<number | undefined>(undefined)
  const connectRef = useRef<(() => void) | null>(null)

  const connect = useCallback(() => {
    try {
      const socket = new WebSocket(WS_URL)
      socketRef.current = socket

      socket.onopen = () => {
        console.log('WebSocket connected')
        setConnected(true)
      }

      socket.onclose = () => {
        console.log('WebSocket disconnected')
        setConnected(false)
        socketRef.current = null

        // Attempt to reconnect after 3 seconds
        reconnectTimeoutRef.current = window.setTimeout(() => {
          console.log('Attempting to reconnect...')
          connectRef.current?.()
        }, 3000)
      }

      socket.onerror = (error) => {
        console.error('WebSocket error:', error)
      }

      socket.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data)
          console.log('Received message:', message)

          switch (message.type) {
            case 'welcome':
              setPeerId(message.peer_id)
              console.log('Received peer ID:', message.peer_id)
              break

            case 'peer-list':
              // Convert peer IDs to Peer objects
              const peerObjects: Peer[] = message.peers.map((id: string) => ({
                id,
                connected_at: Date.now(),
                files: [],
              }))
              setPeers(peerObjects)
              console.log('Peer list updated:', message.peers)
              break

            case 'signal':
              console.log('Received signal:', message)
              // Handle WebRTC signaling
              break

            case 'chunk-peers':
              console.log('Chunk peers:', message)
              // Handle chunk peer information
              break

            case 'error':
              console.error('Server error:', message.message)
              break

            default:
              console.warn('Unknown message type:', message.type)
          }
        } catch (error) {
          console.error('Failed to parse message:', error)
        }
      }
    } catch (error) {
      console.error('Failed to create WebSocket:', error)
    }
  }, [])

  connectRef.current = connect

  useEffect(() => {
    connect()

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current)
      }
      if (socketRef.current) {
        socketRef.current.close()
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const sendMessage = useCallback((message: WebSocketMessage) => {
    if (socketRef.current && connected) {
      socketRef.current.send(JSON.stringify(message))
    }
  }, [connected])

  const joinRoom = useCallback((room: string) => {
    sendMessage({ type: 'join', room })
  }, [sendMessage])

  const announceFiles = useCallback((files: string[]) => {
    sendMessage({ type: 'announce', files })
  }, [sendMessage])

  const requestChunk = useCallback((file_hash: string, chunk_id: number) => {
    sendMessage({ type: 'request-chunk', file_hash, chunk_id })
  }, [sendMessage])

  return {
    connected,
    peers,
    peerId,
    sendMessage,
    joinRoom,
    announceFiles,
    requestChunk,
  }
}
