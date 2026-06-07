use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use anyhow::Result;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use blake3::Hasher;
use chrono::Utc;
use fastcdc::v2020::FastCDC;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::multipart;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager};
use tauri_plugin_store::StoreExt;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

// ── Data types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalFile {
    pub hash: String,
    pub name: String,
    pub size: u64,
    pub mime_type: String,
    pub uploaded_at: i64,
    pub chunk_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkBoundary {
    pub chunk_id: u32,
    pub offset: u64,
    pub length: u32,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2pAddress {
    pub addr: String, // "192.168.x.x:PORT"
}

// ── App state shared between Tauri commands and P2P HTTP server ───────────────

pub struct LocalStore {
    pub db: Pool<SqliteConnectionManager>,
    pub files_dir: PathBuf,
    pub p2p_addr: OnceLock<String>,
}

impl LocalStore {
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        let files_dir = data_dir.join("files");
        fs::create_dir_all(&files_dir)?;

        let db_path = data_dir.join("anna.db");
        let manager = SqliteConnectionManager::file(&db_path);
        let db = Pool::builder().max_size(8).build(manager)?;

        {
            let conn = db.get()?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE IF NOT EXISTS files (
                   hash TEXT PRIMARY KEY,
                   name TEXT NOT NULL,
                   size INTEGER NOT NULL,
                   mime_type TEXT NOT NULL,
                   uploaded_at INTEGER NOT NULL,
                   chunk_count INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS chunks (
                   file_hash TEXT NOT NULL,
                   chunk_id INTEGER NOT NULL,
                   offset INTEGER NOT NULL,
                   length INTEGER NOT NULL,
                   hash TEXT NOT NULL,
                   PRIMARY KEY (file_hash, chunk_id)
                 );",
            )?;
        }

        Ok(Self {
            db,
            files_dir,
            p2p_addr: OnceLock::new(),
        })
    }

    fn file_path(&self, hash: &str) -> PathBuf {
        self.files_dir.join(&hash[..2]).join(hash)
    }
}

// ── CDC + hashing ─────────────────────────────────────────────────────────────

fn process_file(path: &PathBuf) -> Result<(String, u64, Vec<ChunkBoundary>)> {
    let data = fs::read(path)?;
    let size = data.len() as u64;

    // BLAKE3 hash of full file
    let mut hasher = Hasher::new();
    hasher.update(&data);
    let hash = hex::encode(hasher.finalize().as_bytes());

    // FastCDC chunking
    let chunker = FastCDC::new(&data, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024);
    let mut boundaries = Vec::new();
    for (id, chunk) in chunker.enumerate() {
        let mut ch = Hasher::new();
        ch.update(&data[chunk.offset..chunk.offset + chunk.length]);
        boundaries.push(ChunkBoundary {
            chunk_id: id as u32,
            offset: chunk.offset as u64,
            length: chunk.length as u32,
            hash: hex::encode(ch.finalize().as_bytes()),
        });
    }

    Ok((hash, size, boundaries))
}

// ── Tauri commands ────────────────────────────────────────────────────────────

#[tauri::command]
async fn store_file(app: AppHandle, file_path: String) -> Result<LocalFile, String> {
    let path = PathBuf::from(&file_path);
    let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
    let mime = mime_guess::from_path(&path).first_or_octet_stream().to_string();

    let path_clone = path.clone();
    let (hash, size, chunks) = tokio::task::spawn_blocking(move || {
        process_file(&path_clone)
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;

    let store = app.state::<Arc<LocalStore>>();

    // Check duplicate
    let conn = store.db.get().map_err(|e| e.to_string())?;
    let exists: bool = conn
        .query_row("SELECT 1 FROM files WHERE hash = ?1", params![hash], |_| Ok(true))
        .unwrap_or(false);

    if !exists {
        // Copy file into content-addressed store
        let dest = store.file_path(&hash);
        fs::create_dir_all(dest.parent().unwrap()).map_err(|e| e.to_string())?;
        fs::copy(&path, &dest).map_err(|e| e.to_string())?;

        let now = Utc::now().timestamp();
        let chunk_count = chunks.len() as u32;

        conn.execute(
            "INSERT INTO files (hash, name, size, mime_type, uploaded_at, chunk_count)
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![hash, name, size as i64, mime, now, chunk_count],
        )
        .map_err(|e| e.to_string())?;

        for c in &chunks {
            conn.execute(
                "INSERT OR IGNORE INTO chunks (file_hash, chunk_id, offset, length, hash)
                 VALUES (?1,?2,?3,?4,?5)",
                params![hash, c.chunk_id, c.offset as i64, c.length, c.hash],
            )
            .map_err(|e| e.to_string())?;
        }
    }

    let file = conn
        .query_row(
            "SELECT hash,name,size,mime_type,uploaded_at,chunk_count FROM files WHERE hash=?1",
            params![hash],
            |r| {
                Ok(LocalFile {
                    hash: r.get(0)?,
                    name: r.get(1)?,
                    size: r.get::<_, i64>(2)? as u64,
                    mime_type: r.get(3)?,
                    uploaded_at: r.get(4)?,
                    chunk_count: r.get::<_, i64>(5)? as u32,
                })
            },
        )
        .map_err(|e| e.to_string())?;

    Ok(file)
}

#[tauri::command]
fn list_local_files(app: AppHandle) -> Result<Vec<LocalFile>, String> {
    let store = app.state::<Arc<LocalStore>>();
    let conn = store.db.get().map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare(
            "SELECT hash,name,size,mime_type,uploaded_at,chunk_count
             FROM files ORDER BY uploaded_at DESC",
        )
        .map_err(|e| e.to_string())?;
    let files = stmt
        .query_map([], |r| {
            Ok(LocalFile {
                hash: r.get(0)?,
                name: r.get(1)?,
                size: r.get::<_, i64>(2)? as u64,
                mime_type: r.get(3)?,
                uploaded_at: r.get(4)?,
                chunk_count: r.get::<_, i64>(5)? as u32,
            })
        })
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .collect();
    Ok(files)
}

#[tauri::command]
fn delete_local_file(app: AppHandle, hash: String) -> Result<(), String> {
    let store = app.state::<Arc<LocalStore>>();
    let conn = store.db.get().map_err(|e| e.to_string())?;
    conn.execute("DELETE FROM chunks WHERE file_hash=?1", params![hash])
        .map_err(|e| e.to_string())?;
    conn.execute("DELETE FROM files WHERE hash=?1", params![hash])
        .map_err(|e| e.to_string())?;
    let path = store.file_path(&hash);
    let _ = fs::remove_file(path);
    Ok(())
}

#[tauri::command]
fn get_p2p_address(app: AppHandle) -> String {
    app.state::<Arc<LocalStore>>()
        .p2p_addr
        .get()
        .cloned()
        .unwrap_or_default()
}

#[tauri::command]
fn get_local_chunks(app: AppHandle, hash: String) -> Result<Vec<ChunkBoundary>, String> {
    let store = app.state::<Arc<LocalStore>>();
    let conn = store.db.get().map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare(
            "SELECT chunk_id,offset,length,hash FROM chunks
             WHERE file_hash=?1 ORDER BY chunk_id",
        )
        .map_err(|e| e.to_string())?;
    let chunks = stmt
        .query_map(params![hash], |r| {
            Ok(ChunkBoundary {
                chunk_id: r.get::<_, i64>(0)? as u32,
                offset: r.get::<_, i64>(1)? as u64,
                length: r.get::<_, i64>(2)? as u32,
                hash: r.get(3)?,
            })
        })
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .collect();
    Ok(chunks)
}

// ── Server backup / restore ───────────────────────────────────────────────────

#[tauri::command]
async fn backup_to_server(
    app: AppHandle,
    hash: String,
    server_url: String,
) -> Result<(), String> {
    let store = app.state::<Arc<LocalStore>>();
    let conn = store.db.get().map_err(|e| e.to_string())?;

    let (name, mime): (String, String) = conn
        .query_row(
            "SELECT name, mime_type FROM files WHERE hash=?1",
            params![hash],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|e| e.to_string())?;

    let file_path = store.file_path(&hash);
    let file = tokio::fs::File::open(&file_path).await.map_err(|e| e.to_string())?;

    let part = multipart::Part::stream(file)
        .file_name(name)
        .mime_str(&mime)
        .map_err(|e| e.to_string())?;
    let form = multipart::Form::new().part("file", part);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/upload?backup=true", server_url.trim_end_matches('/')))
        .multipart(form)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !resp.status().is_success() {
        return Err(format!("Server returned {}", resp.status()));
    }
    Ok(())
}

#[tauri::command]
async fn restore_from_server(
    app: AppHandle,
    hash: String,
    server_url: String,
) -> Result<LocalFile, String> {
    let store = app.state::<Arc<LocalStore>>();

    // Fetch file list from server to get name/mime
    let client = reqwest::Client::new();
    let files: Vec<serde_json::Value> = client
        .get(format!("{}/api/files", server_url.trim_end_matches('/')))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;

    let meta = files
        .iter()
        .find(|f| f["hash"].as_str() == Some(&hash))
        .ok_or("File not found on server")?;

    let name = meta["name"].as_str().unwrap_or("unknown").to_string();
    let mime = meta["mime_type"].as_str().unwrap_or("application/octet-stream").to_string();

    // Download bytes streaming to disk
    let mut resp = client
        .get(format!("{}/api/download/{}", server_url.trim_end_matches('/'), hash))
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let tmp = std::env::temp_dir().join(format!("anna_restore_{}", hash));
    {
        let mut tmp_file = tokio::fs::File::create(&tmp).await.map_err(|e| e.to_string())?;
        use tokio::io::AsyncWriteExt;
        while let Some(chunk) = resp.chunk().await.map_err(|e| e.to_string())? {
            tmp_file.write_all(&chunk).await.map_err(|e| e.to_string())?;
        }
    }

    // Use store_file logic inline (can't call Tauri cmd from Rust easily)
    let tmp_clone = tmp.clone();
    let (fhash, size, chunks) = tokio::task::spawn_blocking(move || {
        process_file(&tmp_clone)
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;
    let _ = fs::remove_file(&tmp);

    let conn = store.db.get().map_err(|e| e.to_string())?;
    let dest = store.file_path(&fhash);
    fs::create_dir_all(dest.parent().unwrap()).map_err(|e| e.to_string())?;
    fs::copy(&tmp, &dest).map_err(|e| e.to_string())?;

    let now = Utc::now().timestamp();
    let chunk_count = chunks.len() as u32;

    conn.execute(
        "INSERT OR IGNORE INTO files (hash, name, size, mime_type, uploaded_at, chunk_count)
         VALUES (?1,?2,?3,?4,?5,?6)",
        params![fhash, name, size as i64, mime, now, chunk_count],
    )
    .map_err(|e| e.to_string())?;

    for c in &chunks {
        conn.execute(
            "INSERT OR IGNORE INTO chunks (file_hash, chunk_id, offset, length, hash)
             VALUES (?1,?2,?3,?4,?5)",
            params![fhash, c.chunk_id, c.offset as i64, c.length, c.hash],
        )
        .map_err(|e| e.to_string())?;
    }

    Ok(LocalFile {
        hash: fhash,
        name: meta["name"].as_str().unwrap_or("unknown").to_string(),
        size,
        mime_type: mime,
        uploaded_at: now,
        chunk_count,
    })
}

// ── P2P HTTP server (serves local chunks to peers directly) ──────────────────

#[derive(Clone)]
struct P2pState {
    files_dir: PathBuf,
    db: Pool<SqliteConnectionManager>,
}

async fn p2p_chunk(
    Path((hash, chunk_id)): Path<(String, u32)>,
    State(state): State<P2pState>,
) -> impl IntoResponse {
    let conn = match state.db.get() {
        Ok(c) => c,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new(), vec![]),
    };

    let row: Option<(i64, i64, String)> = conn
        .query_row(
            "SELECT offset, length, hash FROM chunks WHERE file_hash=?1 AND chunk_id=?2",
            params![hash, chunk_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .ok();

    let (offset, length, chunk_hash) = match row {
        Some(r) => r,
        None => return (StatusCode::NOT_FOUND, HeaderMap::new(), vec![]),
    };

    let file_path = {
        let dir = state.files_dir.join(&hash[..2]);
        dir.join(&hash)
    };

    let mut f = match fs::File::open(&file_path) {
        Ok(f) => f,
        Err(_) => return (StatusCode::NOT_FOUND, HeaderMap::new(), vec![]),
    };

    let _ = f.seek(SeekFrom::Start(offset as u64));
    let mut buf = vec![0u8; length as usize];
    if f.read_exact(&mut buf).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new(), vec![]);
    }

    let mut headers = HeaderMap::new();
    headers.insert("x-chunk-hash", chunk_hash.parse().unwrap());
    headers.insert("content-type", "application/octet-stream".parse().unwrap());
    (StatusCode::OK, headers, buf)
}

async fn p2p_file_list(State(state): State<P2pState>) -> impl IntoResponse {
    let conn = match state.db.get() {
        Ok(c) => c,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(serde_json::json!([]))),
    };
    let mut stmt = conn
        .prepare("SELECT hash, name, size, chunk_count FROM files ORDER BY uploaded_at DESC")
        .unwrap();
    let files: Vec<serde_json::Value> = stmt
        .query_map([], |r| {
            Ok(serde_json::json!({
                "hash": r.get::<_,String>(0)?,
                "name": r.get::<_,String>(1)?,
                "size": r.get::<_,i64>(2)?,
                "chunk_count": r.get::<_,i64>(3)?,
            }))
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    (StatusCode::OK, axum::Json(serde_json::json!(files)))
}

pub async fn start_p2p_server(store: Arc<LocalStore>) -> Result<String> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let port = listener.local_addr()?.port();

    let local_ip = local_ip_address::local_ip()
        .unwrap_or_else(|_| "127.0.0.1".parse().unwrap());
    let addr = format!("{}:{}", local_ip, port);

    let state = P2pState {
        files_dir: store.files_dir.clone(),
        db: store.db.clone(),
    };

    let app = Router::new()
        .route("/p2p/files", get(p2p_file_list))
        .route("/p2p/chunk/:hash/:chunk_id", get(p2p_chunk))
        .layer(CorsLayer::permissive())
        .with_state(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    Ok(addr)
}

// ── Server URL store ──────────────────────────────────────────────────────────

#[tauri::command]
fn get_server_url(app: AppHandle) -> String {
    app.store("settings.json")
        .ok()
        .and_then(|s| s.get("server_url"))
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "http://localhost:3000".to_string())
}

#[tauri::command]
fn set_server_url(app: AppHandle, url: String) -> Result<(), String> {
    let store = app.store("settings.json").map_err(|e| e.to_string())?;
    store.set("server_url".to_string(), serde_json::json!(url));
    store.save().map_err(|e| e.to_string())
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let data_dir = app.path().app_data_dir().expect("no app data dir");
            let store = LocalStore::new(data_dir)?;
            let store_arc = Arc::new(store);

            // Start the P2P HTTP server and record its LAN address.
            let addr = tauri::async_runtime::block_on(start_p2p_server(store_arc.clone()))?;
            let _ = store_arc.p2p_addr.set(addr);

            app.manage(store_arc);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            store_file,
            list_local_files,
            delete_local_file,
            get_p2p_address,
            get_local_chunks,
            backup_to_server,
            restore_from_server,
            get_server_url,
            set_server_url,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
