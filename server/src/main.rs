use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, State, WebSocketUpgrade},
    http::{header, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::SystemTime};
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::mpsc::UnboundedSender,
};
use tokio_util::io::ReaderStream;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, services::ServeDir, trace::TraceLayer};
use tracing::{info, warn};
use uuid::Uuid;

mod cdc;
mod database;
mod quic;
mod signaling;
mod status;
mod storage;

use cdc::compute_chunks;
use database::Database;
use signaling::handle_websocket;
use status::{status_handler, ServerMetrics};
use storage::{ChunkTracker, FileStorage};

/// Maximum single upload size (10 GB).
const MAX_UPLOAD_SIZE: usize = 10 * 1024 * 1024 * 1024;

/// How long (in seconds) HTTP/3-capable clients should cache the Alt-Svc hint.
const ALT_SVC_MAX_AGE_SECS: u32 = 86400;

#[derive(Clone)]
struct AppState {
    db: Database,
    storage: FileStorage,
    chunk_tracker: Arc<ChunkTracker>,
    peers: Arc<DashMap<String, PeerInfo>>,
    /// Outgoing message channel for each connected peer.
    /// Populated on WebSocket connect, removed on disconnect.
    peer_channels: Arc<DashMap<String, UnboundedSender<String>>>,
    /// Live server metrics (uptime, QUIC connection counts, etc.)
    metrics: Arc<ServerMetrics>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PeerInfo {
    id: String,
    connected_at: i64,
    files: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct FileMetadata {
    hash: String,
    name: String,
    size: u64,
    mime_type: String,
    uploaded_at: i64,
    chunk_count: u32,
    /// `true` for files uploaded before CDC was introduced (Brotli-compressed).
    /// New uploads always set this to `false`.
    compressed: bool,
}

#[derive(Serialize)]
struct CheckResponse {
    exists: bool,
    chunks: Option<Vec<u32>>,
    /// Server's chunk availability bitfield (MSB-first per byte).
    bitfield: Option<Vec<u8>>,
}

#[derive(Serialize)]
struct UploadResponse {
    status: String,
    hash: String,
    size: u64,
    chunk_count: u32,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, Json(self)).into_response()
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Entry point
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting ãnn@sync server…");

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3000);

    // Dedicated QUIC management port (UDP). Default: 4433.
    let mgmt_port = std::env::var("MGMT_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(4433);

    let public_dir = std::env::var("PUBLIC_DIR").unwrap_or_else(|_| "./public".to_string());

    let db = Database::new("./data/metadata.db").await?;
    db.init().await?;

    let storage = FileStorage::new("./data/uploads").await?;
    let chunk_tracker = Arc::new(ChunkTracker::new());
    let metrics = ServerMetrics::new();

    let state = AppState {
        db,
        storage,
        chunk_tracker,
        peers: Arc::new(DashMap::new()),
        peer_channels: Arc::new(DashMap::new()),
        metrics: metrics.clone(),
    };

    // ── Main application router (TCP + HTTP1.1/2) ─────────────────────────────
    // Build Alt-Svc header value once; it never changes at runtime.
    let alt_svc_value = format!(
        "h3=\":{port}\"; ma={ALT_SVC_MAX_AGE_SECS}, h3-29=\":{port}\"; ma={ALT_SVC_MAX_AGE_SECS}"
    );
    let alt_svc_header_value = alt_svc_value.parse::<header::HeaderValue>()
        .expect("Alt-Svc header value is always valid ASCII");

    let app = Router::new()
        .route("/api/files/check/{hash}", get(check_file))
        .route("/api/files", get(list_files))
        .route("/api/upload", post(upload_file))
        .route("/api/download/{hash}", get(download_file))
        .route("/api/chunk/{hash}/{chunk_id}", get(get_chunk))
        .route("/api/chunks/{hash}", get(list_chunks))
        .route("/api/peers", get(list_peers))
        .route("/api/status", get(status_handler))
        .route("/ws", get(websocket_handler))
        .nest_service("/", ServeDir::new(&public_dir))
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_SIZE))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        // Advertise HTTP/3 on every TCP response so browsers can upgrade.
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
            header::HeaderName::from_static("alt-svc"),
            alt_svc_header_value,
        ))
        .with_state(state.clone());

    // ── QUIC management router (UDP — status / admin only) ────────────────────
    let mgmt_router = Router::new()
        .route("/api/status", get(status_handler))
        .layer(CorsLayer::permissive())
        .with_state(state.clone());

    // ── Spawn QUIC listeners ──────────────────────────────────────────────────
    let quic_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let mgmt_addr = SocketAddr::from(([0, 0, 0, 0], mgmt_port));

    // Clone `app` before any moves so the TCP listener retains ownership.
    let app_for_quic = app.clone();
    tokio::spawn({
        let m = metrics.clone();
        async move {
            if let Err(e) = quic::serve_quic(quic_addr, app_for_quic, m).await {
                tracing::error!("QUIC listener error: {e}");
            }
        }
    });

    tokio::spawn({
        let m = metrics.clone();
        async move {
            if let Err(e) = quic::serve_quic_mgmt(mgmt_addr, mgmt_router, m).await {
                tracing::error!("QUIC management listener error: {e}");
            }
        }
    });

    // ── TCP listener (HTTP/1.1 + HTTP/2) ─────────────────────────────────────
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("HTTP/1.1+2 listener on tcp:{port}  |  QUIC/HTTP3 on udp:{port}  |  QUIC management on udp:{mgmt_port}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ══════════════════════════════════════════════════════════════════════════════
// Handlers
// ══════════════════════════════════════════════════════════════════════════════

async fn check_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<CheckResponse>, ErrorResponse> {
    let exists = state.storage.file_exists(&hash).await;
    if exists {
        let chunks = state.chunk_tracker.get_available_chunks(&hash);
        let bitfield = state.chunk_tracker.get_server_bitfield(&hash);
        Ok(Json(CheckResponse {
            exists: true,
            chunks: Some(chunks),
            bitfield: Some(bitfield),
        }))
    } else {
        Ok(Json(CheckResponse {
            exists: false,
            chunks: None,
            bitfield: None,
        }))
    }
}

async fn list_files(
    State(state): State<AppState>,
) -> Result<Json<Vec<FileMetadata>>, ErrorResponse> {
    state.db.list_files().await.map(Json).map_err(|e| {
        warn!("Failed to list files: {}", e);
        ErrorResponse {
            error: "Failed to list files".to_string(),
        }
    })
}

/// Upload a file.
///
/// # What changed vs the old implementation
///
/// | Old behaviour                         | New behaviour                          |
/// |---------------------------------------|----------------------------------------|
/// | SHA-256 hash (slow)                   | BLAKE3 hash (~3× faster)               |
/// | Read entire file into `Vec<u8>`       | File stays on disk; zero extra copy    |
/// | Brotli-compress before storing        | Store raw; HTTP layer compresses       |
/// | Fixed 256 KB chunks                   | FastCDC variable-size chunks (256K–4M) |
/// | Chunk boundaries guessed at serve time| Exact boundaries stored in `chunks` DB |
async fn upload_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, ErrorResponse> {
    let mut temp_file_path: Option<PathBuf> = None;
    let mut filename: Option<String> = None;
    let mut mime_type = "application/octet-stream".to_string();
    let mut hasher = blake3::Hasher::new();
    let mut file_size: u64 = 0;

    while let Some(field) = multipart.next_field().await.map_err(|e| ErrorResponse {
        error: format!("Multipart error: {e}"),
    })? {
        if field.name().unwrap_or("") != "file" {
            continue;
        }

        filename = field.file_name().map(str::to_string);
        mime_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_string();

        // Stream the upload to a temp file on disk.
        // We never load the entire body into memory.
        let temp_path =
            std::env::temp_dir().join(format!("anna-sync-upload-{}.tmp", Uuid::new_v4()));
        let mut temp_file = File::create(&temp_path).await.map_err(|e| ErrorResponse {
            error: format!("Failed to create temp file: {e}"),
        })?;

        let mut stream = field;
        while let Some(chunk) = stream.chunk().await.map_err(|e| ErrorResponse {
            error: format!("Failed to read upload chunk: {e}"),
        })? {
            hasher.update(&chunk);
            file_size += chunk.len() as u64;
            temp_file.write_all(&chunk).await.map_err(|e| ErrorResponse {
                error: format!("Failed to write temp file: {e}"),
            })?;
        }

        // Ensure data is on disk before we do anything else with the file.
        temp_file.sync_all().await.map_err(|e| ErrorResponse {
            error: format!("Failed to sync temp file: {e}"),
        })?;
        drop(temp_file);

        temp_file_path = Some(temp_path);
        break; // only process the first "file" field
    }

    let temp_path = temp_file_path.ok_or_else(|| ErrorResponse {
        error: "No file field in upload".to_string(),
    })?;

    let filename = filename.unwrap_or_else(|| "unnamed".to_string());
    let hash = hasher.finalize().to_hex().to_string();

    // ── Deduplication check ───────────────────────────────────────────────────
    if state.storage.file_exists(&hash).await {
        let _ = tokio::fs::remove_file(&temp_path).await;
        info!("Deduplicated upload: {}", hash);
        let chunk_count = state.chunk_tracker.get_available_chunks(&hash).len() as u32;
        return Ok(Json(UploadResponse {
            status: "exists".to_string(),
            hash,
            size: file_size,
            chunk_count,
        }));
    }

    // ── Content-defined chunking ──────────────────────────────────────────────
    // FastCDC scans the temp file and emits variable-size chunk boundaries.
    // This runs in spawn_blocking so it never stalls the async runtime.
    let boundaries = compute_chunks(&temp_path).await.map_err(|e| ErrorResponse {
        error: format!("CDC failed: {e}"),
    })?;
    let chunk_count = boundaries.len() as u32;

    // ── Persist file (atomic move) ────────────────────────────────────────────
    state
        .storage
        .save_file_from_path(&hash, &temp_path)
        .await
        .map_err(|e| ErrorResponse {
            error: format!("Failed to store file: {e}"),
        })?;

    // ── Persist metadata ──────────────────────────────────────────────────────
    let metadata = FileMetadata {
        hash: hash.clone(),
        name: filename,
        size: file_size,
        mime_type,
        uploaded_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        chunk_count,
        compressed: false, // new uploads are never compressed at rest
    };

    state.db.save_file(&metadata).await.map_err(|e| ErrorResponse {
        error: format!("Failed to save metadata: {e}"),
    })?;

    state.db.save_chunks(&hash, &boundaries).await.map_err(|e| ErrorResponse {
        error: format!("Failed to save chunk boundaries: {e}"),
    })?;

    // ── Register chunks in tracker ────────────────────────────────────────────
    for b in &boundaries {
        state.chunk_tracker.add_chunk(&hash, b.chunk_id);
    }

    info!(
        "Upload complete: {} ({} bytes, {} CDC chunks)",
        hash, file_size, chunk_count
    );

    Ok(Json(UploadResponse {
        status: "success".to_string(),
        hash,
        size: file_size,
        chunk_count,
    }))
}

/// Stream the full file to the client.
///
/// For new (uncompressed) files this is a true zero-copy stream — the OS
/// sends the file directly from the page cache.  Legacy Brotli-compressed
/// files are decompressed in memory first (they pre-date CDC and are rare).
async fn download_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Response, ErrorResponse> {
    let metadata = state.db.get_file(&hash).await.map_err(|_| ErrorResponse {
        error: "File not found".to_string(),
    })?;

    // Legacy path: Brotli-compressed files stored before CDC was introduced.
    if metadata.compressed {
        let data = state.storage.read_file(&hash).await.map_err(|e| ErrorResponse {
            error: format!("Failed to read file: {e}"),
        })?;
        let final_data = decompress_brotli(&data).await?;
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, metadata.mime_type)],
            final_data,
        )
            .into_response());
    }

    // Fast path: open the file and stream it without loading into memory.
    let file_path = state.storage.get_file_path(&hash);
    let file = File::open(&file_path).await.map_err(|e| ErrorResponse {
        error: format!("Failed to open file: {e}"),
    })?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, metadata.mime_type)],
        body,
    )
        .into_response())
}

/// Serve one CDC chunk.
///
/// # Complexity
/// Old: O(file_size) — the entire file had to be read and decompressed.
/// New: O(chunk_size) — we seek to the chunk's byte offset and read exactly
///      `length` bytes.  For a 10 GB file with 1 MB average chunks this is
///      a ~10 000× improvement per request.
///
/// Chunk integrity is verified against the stored BLAKE3 hash before the
/// response is returned, guaranteeing error-free delivery.
async fn get_chunk(
    State(state): State<AppState>,
    Path((hash, chunk_id)): Path<(String, u32)>,
) -> Result<Response, ErrorResponse> {
    let metadata = state.db.get_file(&hash).await.map_err(|_| ErrorResponse {
        error: "File not found".to_string(),
    })?;

    if chunk_id >= metadata.chunk_count {
        return Err(ErrorResponse {
            error: format!(
                "Chunk {} out of range (file has {} chunks)",
                chunk_id, metadata.chunk_count
            ),
        });
    }

    // Legacy path: no CDC boundaries — fall back to fixed-size slicing.
    if metadata.compressed {
        return get_chunk_legacy(&state, &hash, &metadata, chunk_id).await;
    }

    // Look up the exact byte range from the DB.
    let boundary = state
        .db
        .get_chunk_boundary(&hash, chunk_id)
        .await
        .map_err(|e| ErrorResponse {
            error: format!("Chunk boundary not found: {e}"),
        })?;

    // Read only those bytes.
    let data = state
        .storage
        .read_chunk(&hash, boundary.offset, boundary.length)
        .await
        .map_err(|e| ErrorResponse {
            error: format!("Failed to read chunk: {e}"),
        })?;

    // Integrity check: verify the BLAKE3 hash of the served bytes.
    let actual_hash = blake3::hash(&data).to_hex().to_string();
    if actual_hash != boundary.hash {
        return Err(ErrorResponse {
            error: format!("Chunk {} integrity check failed", chunk_id),
        });
    }

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());
    headers.insert(
        axum::http::HeaderName::from_static("x-chunk-hash"),
        boundary.hash.parse().unwrap(),
    );

    Ok((StatusCode::OK, headers, data).into_response())
}

/// Return CDC chunk boundaries for a file so peers can plan their requests.
///
/// Clients use this to:
/// 1. Build a local bitfield of missing chunks.
/// 2. Issue rarest-first `pipeline-request` messages over WebSocket.
/// 3. Verify received chunks with the stored BLAKE3 hashes.
async fn list_chunks(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<cdc::ChunkBoundary>>, ErrorResponse> {
    state.db.get_chunks(&hash).await.map(Json).map_err(|e| ErrorResponse {
        error: format!("Chunks not found: {e}"),
    })
}

async fn list_peers(State(state): State<AppState>) -> Json<Vec<PeerInfo>> {
    Json(state.peers.iter().map(|e| e.value().clone()).collect())
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

// ══════════════════════════════════════════════════════════════════════════════
// Legacy helpers (Brotli-compressed files uploaded before CDC)
// ══════════════════════════════════════════════════════════════════════════════

/// Serve a chunk from a legacy Brotli-compressed file using fixed-size slicing.
async fn get_chunk_legacy(
    state: &AppState,
    hash: &str,
    _metadata: &FileMetadata,
    chunk_id: u32,
) -> Result<Response, ErrorResponse> {
    const LEGACY_CHUNK_SIZE: usize = 256 * 1024;

    let data = state.storage.read_file(hash).await.map_err(|e| ErrorResponse {
        error: format!("Failed to read legacy file: {e}"),
    })?;
    let decompressed = decompress_brotli(&data).await?;

    let start = chunk_id as usize * LEGACY_CHUNK_SIZE;
    if start >= decompressed.len() {
        return Err(ErrorResponse {
            error: format!(
                "Chunk start offset {} exceeds file size {}",
                start,
                decompressed.len()
            ),
        });
    }
    let end = std::cmp::min(start + LEGACY_CHUNK_SIZE, decompressed.len());
    let chunk = decompressed[start..end].to_vec();

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        chunk,
    )
        .into_response())
}

async fn decompress_brotli(data: &[u8]) -> Result<Vec<u8>, ErrorResponse> {
    use async_compression::tokio::bufread::BrotliDecoder;
    use tokio::io::AsyncReadExt;

    let cursor = std::io::Cursor::new(data);
    let mut decoder = BrotliDecoder::new(tokio::io::BufReader::new(cursor));
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).await.map_err(|e| ErrorResponse {
        error: format!("Decompression failed: {e}"),
    })?;
    Ok(out)
}

