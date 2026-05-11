use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{AppState, PeerInfo};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum SignalMessage {
    #[serde(rename = "join")]
    Join { room: String },

    #[serde(rename = "signal")]
    Signal { to: String, signal: serde_json::Value },

    #[serde(rename = "peer-list")]
    PeerList { peers: Vec<String> },

    #[serde(rename = "announce")]
    Announce { files: Vec<String> },

    #[serde(rename = "request-chunk")]
    RequestChunk {
        file_hash: String,
        chunk_id: u32,
    },

    #[serde(rename = "error")]
    Error { message: String },
}

pub async fn handle_websocket(socket: WebSocket, state: AppState) {
    let peer_id = Uuid::new_v4().to_string();
    info!("New WebSocket connection: {}", peer_id);

    // Register peer
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

    let (mut sender, mut receiver) = socket.split();

    // Send peer ID to client
    let welcome_msg = serde_json::json!({
        "type": "welcome",
        "peer_id": peer_id,
    });

    if let Ok(msg_str) = serde_json::to_string(&welcome_msg) {
        let _ = sender.send(Message::Text(msg_str)).await;
    }

    // Handle incoming messages
    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Err(e) = handle_message(&text, &peer_id, &state, &mut sender).await {
                    warn!("Error handling message: {}", e);
                }
            }
            Ok(Message::Close(_)) => {
                info!("WebSocket closed: {}", peer_id);
                break;
            }
            Err(e) => {
                warn!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    // Cleanup
    state.peers.remove(&peer_id);
    state.chunk_tracker.remove_peer(&peer_id);
    info!("Peer disconnected: {}", peer_id);
}

async fn handle_message(
    text: &str,
    peer_id: &str,
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> anyhow::Result<()> {
    let msg: SignalMessage = serde_json::from_str(text)?;

    match msg {
        SignalMessage::Join { room } => {
            info!("Peer {} joined room {}", peer_id, room);

            // Get list of peers in room (simplified - just all peers for now)
            let peers: Vec<String> = state
                .peers
                .iter()
                .filter(|entry| entry.key() != peer_id)
                .map(|entry| entry.key().clone())
                .collect();

            let response = SignalMessage::PeerList { peers };
            let response_str = serde_json::to_string(&response)?;
            sender.send(Message::Text(response_str)).await?;
        }

        SignalMessage::Signal { to, signal } => {
            // Forward signal to target peer
            info!("Forwarding signal from {} to {}", peer_id, to);

            // In a real implementation, we'd need to track WebSocket connections
            // and send the message to the target peer. For now, this is a simplified version.
            // TODO: Implement proper peer-to-peer signaling with connection tracking
        }

        SignalMessage::Announce { files } => {
            info!("Peer {} announced {} files", peer_id, files.len());

            // Update peer info
            if let Some(mut peer) = state.peers.get_mut(peer_id) {
                peer.files = files.clone();
            }

            // Register chunks for announced files
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

        SignalMessage::RequestChunk {
            file_hash,
            chunk_id,
        } => {
            info!(
                "Peer {} requested chunk {} of {}",
                peer_id, chunk_id, file_hash
            );

            // Get peers that have this chunk
            let peers = state
                .chunk_tracker
                .get_peers_for_chunk(&file_hash, chunk_id);

            let response = serde_json::json!({
                "type": "chunk-peers",
                "file_hash": file_hash,
                "chunk_id": chunk_id,
                "peers": peers,
            });

            let response_str = serde_json::to_string(&response)?;
            sender.send(Message::Text(response_str)).await?;
        }

        _ => {
            warn!("Unhandled message type from peer {}", peer_id);
        }
    }

    Ok(())
}
