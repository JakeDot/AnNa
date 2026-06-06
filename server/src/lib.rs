pub mod cdc;
pub mod database;
pub mod quic;
pub mod signaling;
pub mod status;
pub mod storage;

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State, WebSocketUpgrade},
    http::{header, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::SystemTime};
use tokio::{fs::File, io::AsyncWriteExt, sync::mpsc::UnboundedSender};
use tokio_util::io::ReaderStream;
use tower_http::{
    compression::CompressionLayer, cors::CorsLayer, services::ServeDir, trace::TraceLayer,
};
use tracing::{info, warn};
use uuid::Uuid;

use cdc::compute_chunks;
use database::Database;
use signaling::handle_websocket;
use status::{status_handler, ServerMetrics};
use storage::{ChunkTracker, FileStorage};

pub const MAX_UPLOAD_SIZE: usize = 10 * 1024 * 1024 * 1024;
const ALT_SVC_MAX_AGE_SECS: u32 = 86400;

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub storage: FileStorage,
    pub chunk_tracker: Arc<ChunkTracker>,
    pub peers: Arc<DashMap<String, PeerInfo>>,
    pub peer_channels: Arc<DashMap<String, UnboundedSender<String>>>,
    pub metrics: Arc<ServerMetrics>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: String,
    pub connected_at: i64,
    pub files: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub hash: String,
    pub name: String,
    pub size: u64,
    pub mime_type: String,
    pub uploaded_at: i64,
    pub chunk_count: u32,
    pub compressed: bool,
}

#[derive(Serialize)]
struct CheckResponse {
    exists: bool,
    chunks: Option<Vec<u32>>,
    bitfield: Option<Vec<u8>>,
}

#[derive(Serialize)]
pub struct UploadResponse {
    pub status: String,
    pub hash: String,
    pub size: u64,
    pub chunk_count: u32,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, Json(self)).into_response()
    }
}

// ── Router builder ────────────────────────────────────────────────────────────

pub fn build_router(
    state: AppState,
    public_dir: &str,
    alt_svc: Option<header::HeaderValue>,
) -> Router {
    let mut router = Router::new()
        .route("/api/files/check/{hash}", get(check_file))
        .route("/api/files", get(list_files))
        .route("/api/upload", post(upload_file))
        .route("/api/download/{hash}", get(download_file))
        .route("/api/chunk/{hash}/{chunk_id}", get(get_chunk))
        .route("/api/chunks/{hash}", get(list_chunks))
        .route("/api/peers", get(list_peers))
        .route("/api/status", get(status_handler))
        .route("/ws", get(websocket_handler))
        .nest_service("/", ServeDir::new(public_dir))
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_SIZE))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    if let Some(v) = alt_svc {
        router = router.layer(
            tower_http::set_header::SetResponseHeaderLayer::overriding(
                header::HeaderName::from_static("alt-svc"),
                v,
            ),
        );
    }

    router
}

// ── Server startup (called from main.rs) ─────────────────────────────────────

pub async fn run() -> anyhow::Result<()> {
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

    let alt_svc_value = format!(
        "h3=\":{port}\"; ma={ALT_SVC_MAX_AGE_SECS}, h3-29=\":{port}\"; ma={ALT_SVC_MAX_AGE_SECS}"
    );
    let alt_svc_header_value = alt_svc_value
        .parse::<header::HeaderValue>()
        .expect("Alt-Svc header value is always valid ASCII");

    let app = build_router(state.clone(), &public_dir, Some(alt_svc_header_value));

    let mgmt_router = Router::new()
        .route("/api/status", get(status_handler))
        .layer(CorsLayer::permissive())
        .with_state(state.clone());

    let quic_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let mgmt_addr = SocketAddr::from(([0, 0, 0, 0], mgmt_port));

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

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("HTTP/1.1+2 on tcp:{port}  |  QUIC/HTTP3 on udp:{port}  |  QUIC mgmt on udp:{mgmt_port}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn check_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<CheckResponse>, ErrorResponse> {
    let exists = state.storage.file_exists(&hash).await;
    if exists {
        let chunks = state.chunk_tracker.get_available_chunks(&hash);
        let bitfield = state.chunk_tracker.get_server_bitfield(&hash);
        Ok(Json(CheckResponse { exists: true, chunks: Some(chunks), bitfield: Some(bitfield) }))
    } else {
        Ok(Json(CheckResponse { exists: false, chunks: None, bitfield: None }))
    }
}

async fn list_files(
    State(state): State<AppState>,
) -> Result<Json<Vec<FileMetadata>>, ErrorResponse> {
    state.db.list_files().await.map(Json).map_err(|e| {
        warn!("Failed to list files: {}", e);
        ErrorResponse { error: "Failed to list files".to_string() }
    })
}

#[derive(Deserialize, Default)]
struct UploadQuery {
    backup: Option<String>,
}

async fn upload_file(
    State(state): State<AppState>,
    Query(query): Query<UploadQuery>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, ErrorResponse> {
    if query.backup.as_deref() != Some("true") {
        return Err(ErrorResponse {
            error: "Direct uploads are disabled. Use ?backup=true to explicitly back up a file."
                .to_string(),
        });
    }

    let mut temp_file_path: Option<PathBuf> = None;
    let mut filename: Option<String> = None;
    let mut mime_type = "application/octet-stream".to_string();
    let mut hasher = blake3::Hasher::new();
    let mut file_size: u64 = 0;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ErrorResponse { error: format!("Multipart error: {e}") })?
    {
        if field.name().unwrap_or("") != "file" {
            continue;
        }
        filename = field.file_name().map(str::to_string);
        mime_type = field.content_type().unwrap_or("application/octet-stream").to_string();

        let temp_path =
            std::env::temp_dir().join(format!("anna-sync-upload-{}.tmp", Uuid::new_v4()));
        let mut temp_file = File::create(&temp_path)
            .await
            .map_err(|e| ErrorResponse { error: format!("Failed to create temp file: {e}") })?;

        let mut stream = field;
        while let Some(chunk) = stream
            .chunk()
            .await
            .map_err(|e| ErrorResponse { error: format!("Failed to read upload chunk: {e}") })?
        {
            hasher.update(&chunk);
            file_size += chunk.len() as u64;
            temp_file
                .write_all(&chunk)
                .await
                .map_err(|e| ErrorResponse { error: format!("Failed to write temp file: {e}") })?;
        }
        temp_file
            .sync_all()
            .await
            .map_err(|e| ErrorResponse { error: format!("Failed to sync temp file: {e}") })?;
        drop(temp_file);
        temp_file_path = Some(temp_path);
        break;
    }

    let temp_path =
        temp_file_path.ok_or_else(|| ErrorResponse { error: "No file field in upload".to_string() })?;
    let filename = filename.unwrap_or_else(|| "unnamed".to_string());
    let hash = hasher.finalize().to_hex().to_string();

    if state.storage.file_exists(&hash).await {
        let _ = tokio::fs::remove_file(&temp_path).await;
        info!("Deduplicated upload: {}", hash);
        let chunk_count = state.chunk_tracker.get_available_chunks(&hash).len() as u32;
        return Ok(Json(UploadResponse { status: "exists".to_string(), hash, size: file_size, chunk_count }));
    }

    let boundaries = compute_chunks(&temp_path)
        .await
        .map_err(|e| ErrorResponse { error: format!("CDC failed: {e}") })?;
    let chunk_count = boundaries.len() as u32;

    state
        .storage
        .save_file_from_path(&hash, &temp_path)
        .await
        .map_err(|e| ErrorResponse { error: format!("Failed to store file: {e}") })?;

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
        compressed: false,
    };

    state
        .db
        .save_file(&metadata)
        .await
        .map_err(|e| ErrorResponse { error: format!("Failed to save metadata: {e}") })?;
    state
        .db
        .save_chunks(&hash, &boundaries)
        .await
        .map_err(|e| ErrorResponse { error: format!("Failed to save chunk boundaries: {e}") })?;

    for b in &boundaries {
        state.chunk_tracker.add_chunk(&hash, b.chunk_id);
    }

    info!("Upload complete: {} ({} bytes, {} CDC chunks)", hash, file_size, chunk_count);

    Ok(Json(UploadResponse { status: "success".to_string(), hash, size: file_size, chunk_count }))
}

async fn download_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Response, ErrorResponse> {
    let metadata = state
        .db
        .get_file(&hash)
        .await
        .map_err(|_| ErrorResponse { error: "File not found".to_string() })?;

    if metadata.compressed {
        let data = state
            .storage
            .read_file(&hash)
            .await
            .map_err(|e| ErrorResponse { error: format!("Failed to read file: {e}") })?;
        let final_data = decompress_brotli(&data).await?;
        return Ok((StatusCode::OK, [(header::CONTENT_TYPE, metadata.mime_type)], final_data)
            .into_response());
    }

    let file_path = state.storage.get_file_path(&hash);
    let file = File::open(&file_path)
        .await
        .map_err(|e| ErrorResponse { error: format!("Failed to open file: {e}") })?;
    let body = Body::from_stream(ReaderStream::new(file));

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, metadata.mime_type)], body).into_response())
}

async fn get_chunk(
    State(state): State<AppState>,
    Path((hash, chunk_id)): Path<(String, u32)>,
) -> Result<Response, ErrorResponse> {
    let metadata = state
        .db
        .get_file(&hash)
        .await
        .map_err(|_| ErrorResponse { error: "File not found".to_string() })?;

    if chunk_id >= metadata.chunk_count {
        return Err(ErrorResponse {
            error: format!("Chunk {} out of range (file has {} chunks)", chunk_id, metadata.chunk_count),
        });
    }

    if metadata.compressed {
        return get_chunk_legacy(&state, &hash, chunk_id).await;
    }

    let boundary = state
        .db
        .get_chunk_boundary(&hash, chunk_id)
        .await
        .map_err(|e| ErrorResponse { error: format!("Chunk boundary not found: {e}") })?;

    let data = state
        .storage
        .read_chunk(&hash, boundary.offset, boundary.length)
        .await
        .map_err(|e| ErrorResponse { error: format!("Failed to read chunk: {e}") })?;

    let actual_hash = blake3::hash(&data).to_hex().to_string();
    if actual_hash != boundary.hash {
        return Err(ErrorResponse { error: format!("Chunk {} integrity check failed", chunk_id) });
    }

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());
    headers.insert(
        axum::http::HeaderName::from_static("x-chunk-hash"),
        boundary.hash.parse().unwrap(),
    );

    Ok((StatusCode::OK, headers, data).into_response())
}

async fn list_chunks(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<cdc::ChunkBoundary>>, ErrorResponse> {
    state
        .db
        .get_chunks(&hash)
        .await
        .map(Json)
        .map_err(|e| ErrorResponse { error: format!("Chunks not found: {e}") })
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

// ── Legacy helpers ────────────────────────────────────────────────────────────

async fn get_chunk_legacy(
    state: &AppState,
    hash: &str,
    chunk_id: u32,
) -> Result<Response, ErrorResponse> {
    const LEGACY_CHUNK_SIZE: usize = 256 * 1024;
    let data = state
        .storage
        .read_file(hash)
        .await
        .map_err(|e| ErrorResponse { error: format!("Failed to read legacy file: {e}") })?;
    let decompressed = decompress_brotli(&data).await?;
    let start = chunk_id as usize * LEGACY_CHUNK_SIZE;
    if start >= decompressed.len() {
        return Err(ErrorResponse {
            error: format!("Chunk start offset {} exceeds file size {}", start, decompressed.len()),
        });
    }
    let end = std::cmp::min(start + LEGACY_CHUNK_SIZE, decompressed.len());
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/octet-stream")], decompressed[start..end].to_vec())
        .into_response())
}

async fn decompress_brotli(data: &[u8]) -> Result<Vec<u8>, ErrorResponse> {
    use async_compression::tokio::bufread::BrotliDecoder;
    use tokio::io::AsyncReadExt;
    let cursor = std::io::Cursor::new(data);
    let mut decoder = BrotliDecoder::new(tokio::io::BufReader::new(cursor));
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .await
        .map_err(|e| ErrorResponse { error: format!("Decompression failed: {e}") })?;
    Ok(out)
}
