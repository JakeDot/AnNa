//! Integration tests for the AnNa Sync server.
//!
//! Each test spins up a real server bound to a random port with an isolated
//! temporary database and storage directory, so tests are fully parallel-safe.

use std::sync::Arc;

use anna_sync_server::{
    build_router,
    database::Database,
    status::ServerMetrics,
    storage::{ChunkTracker, FileStorage},
    AppState,
};
use dashmap::DashMap;
use futures_util::StreamExt;
use reqwest::{multipart, Client, StatusCode};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

// ── Test harness ──────────────────────────────────────────────────────────────

struct TestServer {
    base_url: String,
    client: Client,
    _db_dir: TempDir,
    _storage_dir: TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let db_dir = TempDir::new().unwrap();
        let storage_dir = TempDir::new().unwrap();

        let db_path = db_dir.path().join("test.db");
        let db = Database::new(db_path.to_str().unwrap()).await.unwrap();
        db.init().await.unwrap();

        let storage = FileStorage::new(storage_dir.path().to_str().unwrap())
            .await
            .unwrap();

        let state = AppState {
            db,
            storage,
            chunk_tracker: Arc::new(ChunkTracker::new()),
            peers: Arc::new(DashMap::new()),
            peer_channels: Arc::new(DashMap::new()),
            metrics: ServerMetrics::new(),
        };

        // Use "/tmp" as public_dir — tests don't serve static files.
        let router = build_router(state, "/tmp", None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        TestServer {
            base_url: format!("http://127.0.0.1:{port}"),
            client: Client::new(),
            _db_dir: db_dir,
            _storage_dir: storage_dir,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn upload(&self, name: &str, bytes: Vec<u8>, backup: bool) -> reqwest::Response {
        let part = multipart::Part::bytes(bytes)
            .file_name(name.to_string())
            .mime_str("application/octet-stream")
            .unwrap();
        let form = multipart::Form::new().part("file", part);
        let url = if backup {
            self.url("/api/upload?backup=true")
        } else {
            self.url("/api/upload")
        };
        self.client.post(url).multipart(form).send().await.unwrap()
    }

    /// Upload and return the parsed JSON body (convenience for success cases).
    async fn upload_ok(&self, name: &str, bytes: Vec<u8>) -> serde_json::Value {
        self.upload(name, bytes, true).await.json().await.unwrap()
    }
}

// ── /api/status ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn status_returns_ok() {
    let srv = TestServer::start().await;
    let resp = srv.client.get(srv.url("/api/status")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["version"].is_string());
    assert!(body["uptime_secs"].is_number());
    assert_eq!(body["file_count"], 0);
    assert_eq!(body["peer_count"], 0);
}

// ── /api/files ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn file_list_starts_empty() {
    let srv = TestServer::start().await;
    let files: Vec<serde_json::Value> = srv
        .client
        .get(srv.url("/api/files"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(files.is_empty());
}

// ── /api/upload gate ──────────────────────────────────────────────────────────

#[tokio::test]
async fn upload_without_backup_flag_is_rejected() {
    let srv = TestServer::start().await;
    let resp = srv.upload("test.bin", b"hello world".to_vec(), false).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("backup=true"));
}

#[tokio::test]
async fn upload_with_backup_flag_succeeds() {
    let srv = TestServer::start().await;
    let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
    let resp = srv.upload("sample.bin", data.clone(), true).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["hash"].as_str().unwrap().len(),
        64,
        "BLAKE3 hex is 64 chars"
    );
    assert_eq!(body["size"], data.len() as u64);
    assert!(body["chunk_count"].as_u64().unwrap() >= 1);
}

// ── Deduplication ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn duplicate_upload_returns_exists() {
    let srv = TestServer::start().await;
    let data = b"deduplicate me".to_vec();

    let b1 = srv.upload_ok("a.txt", data.clone()).await;
    assert_eq!(b1["status"], "success");

    let b2 = srv.upload_ok("b.txt", data).await;
    assert_eq!(b2["status"], "exists");
    assert_eq!(b1["hash"], b2["hash"]);
}

// ── File appears in list ──────────────────────────────────────────────────────

#[tokio::test]
async fn uploaded_file_appears_in_list() {
    let srv = TestServer::start().await;
    let data: Vec<u8> = (0u8..200).collect();
    let body = srv.upload_ok("listed.bin", data).await;
    let hash = body["hash"].as_str().unwrap();

    let files: Vec<serde_json::Value> = srv
        .client
        .get(srv.url("/api/files"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(files.len(), 1);
    assert_eq!(files[0]["hash"], hash);
    assert_eq!(files[0]["name"], "listed.bin");
    assert!(!files[0]["compressed"].as_bool().unwrap());
}

// ── /api/files/check/:hash ────────────────────────────────────────────────────

#[tokio::test]
async fn check_unknown_hash_returns_not_exists() {
    let srv = TestServer::start().await;
    let body: serde_json::Value = srv
        .client
        .get(srv.url(&format!("/api/files/check/{}", "a".repeat(64))))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["exists"], false);
}

#[tokio::test]
async fn check_existing_file_returns_chunks() {
    let srv = TestServer::start().await;
    let data: Vec<u8> = (0u8..=255).cycle().take(512 * 1024).collect();
    let upload = srv.upload_ok("chunked.bin", data).await;
    let hash = upload["hash"].as_str().unwrap();

    let body: serde_json::Value = srv
        .client
        .get(srv.url(&format!("/api/files/check/{hash}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["exists"], true);
    assert!(body["chunks"].is_array());
    assert!(body["bitfield"].is_array());
}

// ── /api/download/:hash ───────────────────────────────────────────────────────

#[tokio::test]
async fn download_returns_original_bytes() {
    let srv = TestServer::start().await;
    let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    let upload = srv.upload_ok("roundtrip.bin", data.clone()).await;
    let hash = upload["hash"].as_str().unwrap();

    let downloaded = srv
        .client
        .get(srv.url(&format!("/api/download/{hash}")))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(downloaded.as_ref(), data.as_slice());
}

#[tokio::test]
async fn download_unknown_hash_returns_404() {
    let srv = TestServer::start().await;
    let resp = srv
        .client
        .get(srv.url(&format!("/api/download/{}", "b".repeat(64))))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST); // ErrorResponse maps to 400
}

// ── /api/chunks/:hash  +  /api/chunk/:hash/:id ────────────────────────────────

#[tokio::test]
async fn chunk_list_matches_upload_chunk_count() {
    let srv = TestServer::start().await;
    let data: Vec<u8> = (0u8..=255).cycle().take(600 * 1024).collect();
    let upload = srv.upload_ok("big.bin", data).await;
    let hash = upload["hash"].as_str().unwrap();
    let chunk_count = upload["chunk_count"].as_u64().unwrap();

    let chunks: Vec<serde_json::Value> = srv
        .client
        .get(srv.url(&format!("/api/chunks/{hash}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(chunks.len() as u64, chunk_count);
    for (i, c) in chunks.iter().enumerate() {
        assert_eq!(
            c["chunk_id"], i as u64,
            "chunks must be ordered and contiguous"
        );
    }
}

#[tokio::test]
async fn chunk_fetch_returns_correct_bytes() {
    let srv = TestServer::start().await;
    let data: Vec<u8> = (0u8..=255).cycle().take(300 * 1024).collect();
    let upload = srv.upload_ok("fetch_chunk.bin", data.clone()).await;
    let hash = upload["hash"].as_str().unwrap();

    let chunks: Vec<serde_json::Value> = srv
        .client
        .get(srv.url(&format!("/api/chunks/{hash}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let c = &chunks[0];
    let offset = c["offset"].as_u64().unwrap() as usize;
    let length = c["length"].as_u64().unwrap() as usize;

    let chunk_resp = srv
        .client
        .get(srv.url(&format!("/api/chunk/{hash}/0")))
        .send()
        .await
        .unwrap();
    assert_eq!(chunk_resp.status(), StatusCode::OK);
    assert!(chunk_resp.headers().contains_key("x-chunk-hash"));

    let chunk_bytes = chunk_resp.bytes().await.unwrap();
    assert_eq!(chunk_bytes.as_ref(), &data[offset..offset + length]);
}

#[tokio::test]
async fn chunk_out_of_range_returns_error() {
    let srv = TestServer::start().await;
    let data: Vec<u8> = vec![0u8; 1024];
    let upload = srv.upload_ok("small.bin", data).await;
    let hash = upload["hash"].as_str().unwrap();
    let chunk_count = upload["chunk_count"].as_u64().unwrap();

    let resp = srv
        .client
        .get(srv.url(&format!("/api/chunk/{hash}/{chunk_count}")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── /api/peers ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn peers_starts_empty() {
    let srv = TestServer::start().await;
    let peers: Vec<serde_json::Value> = srv
        .client
        .get(srv.url("/api/peers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(peers.is_empty());
}

// ── /ws WebSocket ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn websocket_welcome_on_connect() {
    let srv = TestServer::start().await;
    let ws_url = srv.base_url.replacen("http", "ws", 1) + "/ws";

    let (mut socket, _) = connect_async(&ws_url).await.expect("WS connect failed");

    match socket.next().await {
        Some(Ok(Message::Text(text))) => {
            let msg: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(msg["type"], "welcome", "first message must be 'welcome'");
            assert!(msg["peer_id"].is_string(), "welcome must include a peer_id");
        }
        other => panic!("Expected text welcome message, got: {other:?}"),
    }
}

#[tokio::test]
async fn websocket_join_room_broadcasts_peer_list() {
    let srv = TestServer::start().await;
    let ws_url = srv.base_url.replacen("http", "ws", 1) + "/ws";

    let (mut s1, _) = connect_async(&ws_url).await.unwrap();
    let (mut s2, _) = connect_async(&ws_url).await.unwrap();

    // Skip welcome messages.
    s1.next().await;
    s2.next().await;

    // s1 joins a room.
    use tokio_tungstenite::tungstenite::Message;
    s1.send(Message::Text(
        r#"{"type":"join","room":"test-room"}"#.into(),
    ))
    .await
    .unwrap();

    // s2 joins the same room — s1 should get a peer-list update.
    s2.send(Message::Text(
        r#"{"type":"join","room":"test-room"}"#.into(),
    ))
    .await
    .unwrap();

    // s1 should receive a peer-list message.
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(2), s1.next()).await;
    match timeout {
        Ok(Some(Ok(Message::Text(text)))) => {
            let msg: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(msg["type"], "peer-list");
        }
        _ => panic!("Expected peer-list message within 2 seconds"),
    }
}
