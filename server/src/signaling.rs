//! WebSocket signaling server.
//!
//! Each peer connects and receives a `welcome` message with its assigned UUID.
//! From then on the server acts as a relay for WebRTC signaling (SDP offers /
//! answers and ICE candidates) and as a coordination layer for P2P chunk
//! scheduling.
//!
//! # New message types
//!
//! | Direction      | Type               | Purpose                                     |
//! |----------------|--------------------|---------------------------------------------|
//! | peer → server  | `join`             | Declare interest in a room                  |
//! | peer → server  | `signal`           | Forward an opaque WebRTC signal to a peer   |
//! | peer → server  | `announce`         | Tell the server which files this peer has   |
//! | peer → server  | `bitfield`         | Declare chunk availability for one file     |
//! | peer → server  | `pipeline-request` | Request rarest-first assignments for chunks |
//! | peer → server  | `request-chunk`    | Ask which peers have a specific chunk       |
//! | server → peer  | `welcome`          | Assign a peer_id                            |
//! | server → peer  | `peer-list`        | List of peers in the joined room            |
//! | server → peer  | `signal`           | Forwarded signal (adds `from` field)        |
//! | server → peer  | `chunk-peers`      | Single-chunk peer list                      |
//! | server → peer  | `chunk-assignments`| Rarest-first batch peer assignments         |
//! | server → peer  | `error`            | Protocol error                              |

use axum::extract::ws::{Message, WebSocket};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{AppState, PeerInfo};

// ══════════════════════════════════════════════════════════════════════════════
// Message types
// ══════════════════════════════════════════════════════════════════════════════

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum SignalMessage {
    Join {
        room: String,
    },
    Signal {
        to: String,
        signal: serde_json::Value,
    },
    Announce {
        files: Vec<String>,
    },
    /// Peer declares which chunks it has for a file.
    ///
    /// `bitfield` is an array of unsigned bytes where bit `i` of byte `i/8`
    /// (MSB-first within each byte) indicates that chunk `i` is available.
    Bitfield {
        file_hash: String,
        bitfield: Vec<u8>,
        chunk_count: u32,
    },
    /// Request rarest-first peer assignments for a batch of chunks.
    ///
    /// The client should maintain a sliding window of `chunk_ids` sized by
    /// its target pipeline depth (e.g. 8–16 outstanding requests).
    PipelineRequest {
        file_hash: String,
        chunk_ids: Vec<u32>,
    },
    RequestChunk {
        file_hash: String,
        chunk_id: u32,
    },
    Error {
        message: String,
    },
}

// ══════════════════════════════════════════════════════════════════════════════
// Connection handler
// ══════════════════════════════════════════════════════════════════════════════

pub async fn handle_websocket(socket: WebSocket, state: AppState) {
    let peer_id = Uuid::new_v4().to_string();
    info!("New WebSocket connection: {}", peer_id);

    let (mut ws_sink, mut ws_stream) = socket.split();

    // ── Outgoing channel ──────────────────────────────────────────────────────
    // All code that wants to send to this peer writes into `peer_tx`.
    // A dedicated task drains the channel and writes to the WebSocket sink,
    // decoupling message production from socket I/O and providing natural
    // backpressure: if the socket is slow, the channel fills up and senders
    // block (unbounded sender never blocks on send, but heap pressure builds).
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel::<String>();
    state.peer_channels.insert(peer_id.clone(), peer_tx.clone());

    // Register peer.
    state.peers.insert(
        peer_id.clone(),
        PeerInfo {
            id: peer_id.clone(),
            connected_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            files: Vec::new(),
        },
    );

    // Send welcome.
    let welcome = serde_json::json!({ "type": "welcome", "peer_id": &peer_id });
    if let Ok(s) = serde_json::to_string(&welcome) {
        let _ = peer_tx.send(s);
    }

    // Spawn the write pump.
    use futures_util::SinkExt;
    let write_pump = tokio::spawn(async move {
        while let Some(msg) = peer_rx.recv().await {
            if ws_sink.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // ── Receive loop ─────────────────────────────────────────────────────────
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Err(e) = handle_message(&text, &peer_id, &state, &peer_tx).await {
                    warn!("Error handling message from {}: {}", peer_id, e);
                    let err = serde_json::json!({
                        "type": "error",
                        "message": e.to_string(),
                    });
                    if let Ok(s) = serde_json::to_string(&err) {
                        let _ = peer_tx.send(s);
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!("WebSocket closed by peer: {}", peer_id);
                break;
            }
            Err(e) => {
                warn!("WebSocket error for {}: {}", peer_id, e);
                break;
            }
            _ => {}
        }
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────
    write_pump.abort();
    state.peer_channels.remove(&peer_id);
    state.peers.remove(&peer_id);
    state.chunk_tracker.remove_peer(&peer_id);
    info!("Peer disconnected: {}", peer_id);
}

// ══════════════════════════════════════════════════════════════════════════════
// Per-message dispatch
// ══════════════════════════════════════════════════════════════════════════════

async fn handle_message(
    text: &str,
    peer_id: &str,
    state: &AppState,
    peer_tx: &mpsc::UnboundedSender<String>,
) -> anyhow::Result<()> {
    let msg: SignalMessage = serde_json::from_str(text)?;

    match msg {
        // ── Join ──────────────────────────────────────────────────────────────
        SignalMessage::Join { room } => {
            info!("Peer {} joined room {}", peer_id, room);
            let peers: Vec<String> = state
                .peers
                .iter()
                .filter(|e| e.key() != peer_id)
                .map(|e| e.key().clone())
                .collect();
            let response = serde_json::json!({ "type": "peer-list", "peers": peers });
            peer_tx.send(serde_json::to_string(&response)?)?;
        }

        // ── Signal (WebRTC relay) ─────────────────────────────────────────────
        SignalMessage::Signal { to, signal } => {
            info!("Forwarding signal from {} to {}", peer_id, to);
            if let Some(target_tx) = state.peer_channels.get(&to) {
                let forward = serde_json::json!({
                    "type": "signal",
                    "from": peer_id,
                    "signal": signal,
                });
                // If the target's channel is full / disconnected, ignore rather
                // than returning an error that would kill the sender's loop.
                let _ = target_tx.send(serde_json::to_string(&forward)?);
            } else {
                let err = serde_json::json!({
                    "type": "error",
                    "message": format!("peer {to} not found"),
                });
                peer_tx.send(serde_json::to_string(&err)?)?;
            }
        }

        // ── Announce ─────────────────────────────────────────────────────────
        SignalMessage::Announce { files } => {
            info!("Peer {} announced {} file(s)", peer_id, files.len());
            if let Some(mut peer) = state.peers.get_mut(peer_id) {
                peer.files = files.clone();
            }
            for file_hash in files {
                if let Ok(metadata) = state.db.get_file(&file_hash).await {
                    for chunk_id in 0..metadata.chunk_count {
                        state
                            .chunk_tracker
                            .add_peer_chunk(&file_hash, chunk_id, peer_id.to_string());
                    }
                }
            }
        }

        // ── Bitfield ─────────────────────────────────────────────────────────
        // Peer sends a compact bitmap of which chunks it already has.
        // The server updates its rarity table so subsequent PipelineRequests
        // get accurate rarest-first ordering.
        SignalMessage::Bitfield {
            file_hash,
            bitfield,
            chunk_count,
        } => {
            info!(
                "Peer {} sent bitfield for {} ({} chunks)",
                peer_id, file_hash, chunk_count
            );
            state
                .chunk_tracker
                .set_peer_bitfield(&file_hash, peer_id, &bitfield, chunk_count);
        }

        // ── PipelineRequest ───────────────────────────────────────────────────
        // Client passes a window of chunk IDs it wants to fetch.  The server
        // returns them sorted rarest-first with the peers that can serve each.
        SignalMessage::PipelineRequest {
            file_hash,
            chunk_ids,
        } => {
            info!(
                "Peer {} pipeline-request: {} chunks of {}",
                peer_id,
                chunk_ids.len(),
                file_hash
            );
            let assignments = state
                .chunk_tracker
                .get_rarest_chunk_assignments(&file_hash, &chunk_ids);
            let response = serde_json::json!({
                "type": "chunk-assignments",
                "file_hash": file_hash,
                "assignments": assignments,
            });
            peer_tx.send(serde_json::to_string(&response)?)?;
        }

        // ── RequestChunk (single-chunk legacy compat) ─────────────────────────
        SignalMessage::RequestChunk {
            file_hash,
            chunk_id,
        } => {
            info!("Peer {} requested chunk {} of {}", peer_id, chunk_id, file_hash);
            let peers = state
                .chunk_tracker
                .get_peers_for_chunk(&file_hash, chunk_id);
            let response = serde_json::json!({
                "type": "chunk-peers",
                "file_hash": file_hash,
                "chunk_id": chunk_id,
                "peers": peers,
            });
            peer_tx.send(serde_json::to_string(&response)?)?;
        }

        SignalMessage::Error { message } => {
            warn!("Client-side error from {}: {}", peer_id, message);
        }
    }

    Ok(())
}

