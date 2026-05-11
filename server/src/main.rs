use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, Query, State, WebSocketUpgrade},
    http::{header, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::SystemTime,
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
};
use tower_http::{
    cors::CorsLayer,
    compression::CompressionLayer,
    services::ServeDir,
    trace::TraceLayer,
};
use tracing::{info, warn};
use uuid::Uuid;

mod database;
mod signaling;
mod storage;

use database::Database;
use signaling::handle_websocket;
use storage::{ChunkTracker, FileStorage};

const CHUNK_SIZE: usize = 256 * 1024; // 256KB chunks
const MAX_UPLOAD_SIZE: usize = 10 * 1024 * 1024 * 1024; // 10GB

#[derive(Clone)]
struct AppState {
    db: Database,
    storage: FileStorage,
    chunk_tracker: Arc<ChunkTracker>,
    peers: Arc<DashMap<String, PeerInfo>>,
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
    compressed: bool,
}

#[derive(Deserialize)]
struct HashQuery {
    hash: String,
}

#[derive(Serialize)]
struct CheckResponse {
    exists: bool,
    chunks: Option<Vec<u32>>,
}

#[derive(Serialize)]
struct UploadResponse {
    status: String,
    hash: String,
    size: u64,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting ãnn@sync server...");

    // Read configuration from environment
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3000);

    let public_dir = std::env::var("PUBLIC_DIR").unwrap_or_else(|_| "./public".to_string());

    // Initialize database
    let db = Database::new("./data/metadata.db").await?;
    db.init().await?;

    // Initialize storage
    let storage = FileStorage::new("./data/uploads").await?;

    // Initialize chunk tracker
    let chunk_tracker = Arc::new(ChunkTracker::new());

    // Initialize app state
    let state = AppState {
        db,
        storage,
        chunk_tracker,
        peers: Arc::new(DashMap::new()),
    };

    // Build router
    let app = Router::new()
        // API routes
        .route("/api/files/check/:hash", get(check_file))
        .route("/api/files", get(list_files))
        .route("/api/upload", post(upload_file))
        .route("/api/download/:hash", get(download_file))
        .route("/api/chunk/:hash/:chunk_id", get(get_chunk))
        .route("/api/peers", get(list_peers))
        // WebSocket signaling
        .route("/ws", get(websocket_handler))
        // Static files
        .nest_service("/", ServeDir::new(&public_dir))
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_SIZE))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // Bind and serve
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn check_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<CheckResponse>, ErrorResponse> {
    let exists = state.storage.file_exists(&hash).await;

    if exists {
        let chunks = state.chunk_tracker.get_available_chunks(&hash);
        Ok(Json(CheckResponse {
            exists: true,
            chunks: Some(chunks),
        }))
    } else {
        Ok(Json(CheckResponse {
            exists: false,
            chunks: None,
        }))
    }
}

async fn list_files(State(state): State<AppState>) -> Result<Json<Vec<FileMetadata>>, ErrorResponse> {
    match state.db.list_files().await {
        Ok(files) => Ok(Json(files)),
        Err(e) => {
            warn!("Failed to list files: {}", e);
            Err(ErrorResponse {
                error: "Failed to list files".to_string(),
            })
        }
    }
}

async fn upload_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, ErrorResponse> {
    let mut temp_file_path: Option<PathBuf> = None;
    let mut filename: Option<String> = None;
    let mut mime_type = "application/octet-stream".to_string();
    let mut hasher = Sha256::new();
    let mut file_size: u64 = 0;

    while let Some(field) = multipart.next_field().await.map_err(|e| ErrorResponse {
        error: format!("Multipart error: {}", e),
    })? {
        let name = field.name().unwrap_or("").to_string();

        match name.as_str() {
            "file" => {
                filename = field.file_name().map(|s| s.to_string());
                mime_type = field
                    .content_type()
                    .unwrap_or("application/octet-stream")
                    .to_string();

                // Create a temporary file to stream the upload
                let temp_dir = std::env::temp_dir();
                let temp_name = format!("upload-{}.tmp", uuid::Uuid::new_v4());
                let temp_path = temp_dir.join(temp_name);

                let mut temp_file = File::create(&temp_path).await.map_err(|e| ErrorResponse {
                    error: format!("Failed to create temp file: {}", e),
                })?;

                // Stream the field data to disk while computing hash
                let mut stream = field;
                while let Some(chunk) = stream.chunk().await.map_err(|e| ErrorResponse {
                    error: format!("Failed to read chunk: {}", e),
                })? {
                    hasher.update(&chunk);
                    file_size += chunk.len() as u64;
                    temp_file.write_all(&chunk).await.map_err(|e| ErrorResponse {
                        error: format!("Failed to write to temp file: {}", e),
                    })?;
                }

                temp_file.sync_all().await.map_err(|e| ErrorResponse {
                    error: format!("Failed to sync temp file: {}", e),
                })?;
                drop(temp_file);

                temp_file_path = Some(temp_path);
            }
            _ => {}
        }
    }

    let temp_path = temp_file_path.ok_or_else(|| ErrorResponse {
        error: "No file provided".to_string(),
    })?;

    let filename = filename.unwrap_or_else(|| "unnamed".to_string());
    let hash = hex::encode(hasher.finalize());

    // Check if file already exists (deduplication)
    if state.storage.file_exists(&hash).await {
        // Clean up temp file
        let _ = fs::remove_file(&temp_path).await;

        info!("File {} already exists (deduplicated)", hash);
        return Ok(Json(UploadResponse {
            status: "exists".to_string(),
            hash: hash.clone(),
            size: file_size,
        }));
    }

    // Read the temp file back
    let mut temp_file = File::open(&temp_path).await.map_err(|e| ErrorResponse {
        error: format!("Failed to open temp file: {}", e),
    })?;

    let mut file_data = Vec::new();
    temp_file.read_to_end(&mut file_data).await.map_err(|e| ErrorResponse {
        error: format!("Failed to read temp file: {}", e),
    })?;

    // Clean up temp file
    let _ = fs::remove_file(&temp_path).await;

    // Determine if we should compress
    let should_compress = should_compress_file(&mime_type);
    let compressed_data = if should_compress {
        compress_data(&file_data).await?
    } else {
        file_data
    };

    // Save file
    state.storage.save_file(&hash, &compressed_data).await.map_err(|e| ErrorResponse {
        error: format!("Failed to save file: {}", e),
    })?;

    // Calculate chunks
    let chunk_count = ((file_size + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64) as u32;

    // Save metadata
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
        compressed: should_compress,
    };

    state.db.save_file(&metadata).await.map_err(|e| ErrorResponse {
        error: format!("Failed to save metadata: {}", e),
    })?;

    // Register chunks in tracker
    for i in 0..chunk_count {
        state.chunk_tracker.add_chunk(&hash, i);
    }

    info!("File uploaded successfully: {} ({} bytes)", hash, file_size);

    Ok(Json(UploadResponse {
        status: "success".to_string(),
        hash,
        size: file_size,
    }))
}

async fn download_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Response, ErrorResponse> {
    // Get metadata
    let metadata = state.db.get_file(&hash).await.map_err(|_| ErrorResponse {
        error: "File not found".to_string(),
    })?;

    // Read file
    let data = state.storage.read_file(&hash).await.map_err(|e| ErrorResponse {
        error: format!("Failed to read file: {}", e),
    })?;

    // Decompress if needed
    let final_data = if metadata.compressed {
        decompress_data(&data).await?
    } else {
        data
    };

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, metadata.mime_type)],
        final_data,
    )
        .into_response())
}

async fn get_chunk(
    State(state): State<AppState>,
    Path((hash, chunk_id)): Path<(String, u32)>,
) -> Result<Response, ErrorResponse> {
    let metadata = state.db.get_file(&hash).await.map_err(|_| ErrorResponse {
        error: "File not found".to_string(),
    })?;

    if chunk_id >= metadata.chunk_count {
        return Err(ErrorResponse {
            error: "Invalid chunk ID".to_string(),
        });
    }

    let data = state.storage.read_file(&hash).await.map_err(|e| ErrorResponse {
        error: format!("Failed to read file: {}", e),
    })?;

    // Decompress if needed
    let decompressed = if metadata.compressed {
        decompress_data(&data).await?
    } else {
        data
    };

    let start = (chunk_id as usize) * CHUNK_SIZE;

    // Guard against start exceeding data length
    if start >= decompressed.len() {
        return Err(ErrorResponse {
            error: format!(
                "Chunk start offset {} exceeds file size {}",
                start,
                decompressed.len()
            ),
        });
    }

    let end = std::cmp::min(start + CHUNK_SIZE, decompressed.len());
    let chunk = decompressed[start..end].to_vec();

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/octet-stream")], chunk).into_response())
}

async fn list_peers(State(state): State<AppState>) -> Json<Vec<PeerInfo>> {
    let peers: Vec<PeerInfo> = state
        .peers
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    Json(peers)
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

fn should_compress_file(mime_type: &str) -> bool {
    // Don't compress already-compressed formats
    let no_compress = [
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "video/",
        "audio/",
        "application/zip",
        "application/gzip",
        "application/x-bzip2",
        "application/x-7z-compressed",
    ];

    !no_compress.iter().any(|prefix| mime_type.starts_with(prefix))
}

async fn compress_data(data: &[u8]) -> Result<Vec<u8>, ErrorResponse> {
    use async_compression::tokio::bufread::BrotliEncoder;
    use tokio::io::AsyncReadExt;

    let cursor = std::io::Cursor::new(data);
    let mut encoder = BrotliEncoder::new(tokio::io::BufReader::new(cursor));
    let mut compressed = Vec::new();
    encoder.read_to_end(&mut compressed).await.map_err(|e| ErrorResponse {
        error: format!("Compression failed: {}", e),
    })?;

    Ok(compressed)
}

async fn decompress_data(data: &[u8]) -> Result<Vec<u8>, ErrorResponse> {
    use async_compression::tokio::bufread::BrotliDecoder;
    use tokio::io::AsyncReadExt;

    let cursor = std::io::Cursor::new(data);
    let mut decoder = BrotliDecoder::new(tokio::io::BufReader::new(cursor));
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).await.map_err(|e| ErrorResponse {
        error: format!("Decompression failed: {}", e),
    })?;

    Ok(decompressed)
}
