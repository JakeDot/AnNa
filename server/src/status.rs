//! Server status endpoint — `GET /api/status`.
//!
//! Returns a JSON snapshot of the server's health and operational metrics.
//! This is the data source for the management UI served over the QUIC
//! endpoint.  The same handler is also mounted on the main TCP router so
//! the metrics are accessible without HTTP/3 support.
//!
//! # Fields
//!
//! | Field                | Description                                          |
//! |----------------------|------------------------------------------------------|
//! | `version`            | Crate version from `Cargo.toml`                      |
//! | `uptime_secs`        | Seconds since the process started                    |
//! | `peer_count`         | Number of currently connected WebSocket peers        |
//! | `file_count`         | Total files in the metadata DB                       |
//! | `total_bytes`        | Sum of stored file sizes                             |
//! | `total_chunk_count`  | Total CDC chunks across all files                    |
//! | `quic_connections`   | Live QUIC connections (updated by the QUIC acceptor) |
//! | `transport_hint`     | `"h3"` when the client used HTTP/3, `"tcp"` otherwise|

use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{extract::State, response::Json};
use serde::Serialize;

use crate::AppState;

// ══════════════════════════════════════════════════════════════════════════════
// Shared counters
// ══════════════════════════════════════════════════════════════════════════════

/// Atomic counters shared between the QUIC acceptor and the status handler.
///
/// These live in `AppState` as `Arc<ServerMetrics>`.  The QUIC acceptor
/// increments/decrements `active_quic_connections` on each connection
/// open/close; the status handler reads them without locking.
#[derive(Default)]
pub struct ServerMetrics {
    /// Unix timestamp (seconds) when the server process started.
    pub started_at: AtomicI64,
    /// Number of currently open QUIC connections.
    pub active_quic_connections: AtomicU64,
    /// Total QUIC connections accepted since startup (monotonic).
    pub total_quic_connections: AtomicU64,
}

impl ServerMetrics {
    pub fn new() -> Arc<Self> {
        let m = Arc::new(Self::default());
        m.started_at.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            Ordering::Relaxed,
        );
        m
    }

    pub fn quic_open(&self) {
        self.active_quic_connections.fetch_add(1, Ordering::Relaxed);
        self.total_quic_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn quic_close(&self) {
        self.active_quic_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Response type
// ══════════════════════════════════════════════════════════════════════════════

#[derive(Serialize)]
pub struct ServerStatus {
    pub version: &'static str,
    pub uptime_secs: u64,
    pub peer_count: usize,
    pub file_count: usize,
    pub total_bytes: u64,
    pub total_chunk_count: u64,
    pub active_quic_connections: u64,
    pub total_quic_connections: u64,
    /// Always `true` — indicates that the server has QUIC/HTTP3 listeners
    /// active.  Clients can use this to show a "QUIC available" badge
    /// regardless of which transport the current request arrived on.
    pub quic_enabled: bool,
    pub files: Vec<FileStats>,
    pub peers: Vec<PeerStats>,
}

#[derive(Serialize)]
pub struct FileStats {
    pub hash: String,
    pub name: String,
    pub size: u64,
    pub chunk_count: u32,
    pub uploaded_at: i64,
    pub compressed: bool,
}

#[derive(Serialize)]
pub struct PeerStats {
    pub id: String,
    pub connected_at: i64,
    pub file_count: usize,
}

// ══════════════════════════════════════════════════════════════════════════════
// Handler
// ══════════════════════════════════════════════════════════════════════════════

pub async fn status_handler(State(state): State<AppState>) -> Json<ServerStatus> {
    let started_at = state.metrics.started_at.load(Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uptime_secs = (now - started_at).max(0) as u64;

    let peer_count = state.peers.len();

    let peers: Vec<PeerStats> = state
        .peers
        .iter()
        .map(|e| PeerStats {
            id: e.key().clone(),
            connected_at: e.value().connected_at,
            file_count: e.value().files.len(),
        })
        .collect();

    // Pull file list from DB; on error return zeros rather than a 500.
    let (file_count, total_bytes, total_chunk_count, files) =
        match state.db.list_files().await {
            Ok(all) => {
                let total_bytes = all.iter().map(|f| f.size).sum();
                let total_chunks = all.iter().map(|f| f.chunk_count as u64).sum();
                let file_count = all.len();
                let stats: Vec<FileStats> = all
                    .into_iter()
                    .map(|f| FileStats {
                        hash: f.hash,
                        name: f.name,
                        size: f.size,
                        chunk_count: f.chunk_count,
                        uploaded_at: f.uploaded_at,
                        compressed: f.compressed,
                    })
                    .collect();
                (file_count, total_bytes, total_chunks, stats)
            }
            Err(_) => (0, 0, 0, Vec::new()),
        };

    Json(ServerStatus {
        version: env!("CARGO_PKG_VERSION"),
        uptime_secs,
        peer_count,
        file_count,
        total_bytes,
        total_chunk_count,
        active_quic_connections: state
            .metrics
            .active_quic_connections
            .load(Ordering::Relaxed),
        total_quic_connections: state
            .metrics
            .total_quic_connections
            .load(Ordering::Relaxed),
        quic_enabled: true, // QUIC listeners are always started alongside the server
        files,
        peers,
    })
}
