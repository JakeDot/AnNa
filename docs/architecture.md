# AnNa Architecture

AnNa (ãnn@sync) is a P2P-first file sync platform for desktop and mobile. Files are stored locally on each device, chunked with content-defined chunking, and exchanged directly between peers over HTTP. A central server exists only as an explicit backup target and signaling relay — it is never the default storage destination.

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [Repository Layout](#2-repository-layout)
3. [Server Architecture (Rust/Axum)](#3-server-architecture-rustaxum)
   - 3.1 [Shared State](#31-shared-state)
   - 3.2 [HTTP Router](#32-http-router)
   - 3.3 [The Backup Gate](#33-the-backup-gate)
   - 3.4 [Upload Pipeline](#34-upload-pipeline)
   - 3.5 [Content-Defined Chunking (CDC)](#35-content-defined-chunking-cdc)
   - 3.6 [Storage Layer](#36-storage-layer)
   - 3.7 [Database](#37-database)
   - 3.8 [WebSocket Signaling](#38-websocket-signaling)
   - 3.9 [QUIC / HTTP3 Transport](#39-quic--http3-transport)
   - 3.10 [Authentication & Authorization](#310-authentication--authorization)
   - 3.11 [Groups, Labels, and Virtual Folders](#311-groups-labels-and-virtual-folders)
   - 3.12 [Monitoring](#312-monitoring)
   - 3.13 [lib.rs / main.rs Split](#313-librs--mainrs-split)
4. [Desktop Client Architecture (Tauri 2.0 + React)](#4-desktop-client-architecture-tauri-20--react)
   - 4.1 [Local Store](#41-local-store)
   - 4.2 [P2P HTTP Server (Embedded Axum)](#42-p2p-http-server-embedded-axum)
   - 4.3 [Tauri Commands](#43-tauri-commands)
   - 4.4 [Frontend (React / TypeScript)](#44-frontend-react--typescript)
   - 4.5 [Server Settings Persistence](#45-server-settings-persistence)
5. [Data Flow Walkthroughs](#5-data-flow-walkthroughs)
   - 5.1 [File Upload (local, P2P-first)](#51-file-upload-local-p2p-first)
   - 5.2 [Backup to Server (explicit)](#52-backup-to-server-explicit)
   - 5.3 [P2P Download with Server Fallback](#53-p2p-download-with-server-fallback)
6. [Key Invariants](#6-key-invariants)
7. [CI / CD](#7-ci--cd)
8. [Design Decision Reference](#8-design-decision-reference)

---

## 1. Design Philosophy

Three principles shape every design decision:

**1. Local-first, server-optional.**  
The device is the primary storage medium. Every file is stored on the local device before anything else happens. The central server is an explicit opt-in backup — not a write-through cache. This means the app works fully offline and is immune to server downtime for reading and writing local files.

**2. P2P transfer before server transfer.**  
When a peer on the same network has a chunk, that peer's embedded HTTP server is preferred over the central server. The central server acts as a seed of last resort, not a CDN. This reduces egress costs, improves throughput on LANs, and scales naturally as more peers join.

**3. Content-addressed, chunk-level deduplication.**  
Files are identified by BLAKE3 hash. Chunks within a file are also identified by their BLAKE3 hash. Identical content is never stored twice (at file or chunk granularity), and incremental updates only transfer changed chunks.

---

## 2. Repository Layout

```
AnNa/
├── server/                 Rust — central HTTP/QUIC server
│   ├── src/
│   │   ├── lib.rs          Router, handlers, AppState, upload gate
│   │   ├── main.rs         4-line entry point → anna_sync_server::run()
│   │   ├── cdc.rs          FastCDC chunking + BLAKE3 boundary hashing
│   │   ├── database.rs     SQLite schema, connection pool, query methods
│   │   ├── storage.rs      FileStorage (disk), ChunkTracker (in-memory)
│   │   ├── signaling.rs    WebSocket room-based peer signaling
│   │   ├── quic.rs         QUIC/HTTP3 transport, TLS cert management
│   │   ├── status.rs       Server metrics and /api/status handler
│   │   ├── auth.rs         GitHub OAuth + JWT issuance/validation
│   │   ├── groups.rs       Group CRUD and membership management
│   │   ├── labels.rs       File label CRUD
│   │   └── folders.rs      Virtual folder CRUD
│   └── tests/
│       └── integration.rs  16 integration tests (the API contract)
│
├── client/                 React + TypeScript + Tauri 2.0
│   ├── src/
│   │   ├── App.tsx         Top-level tab routing, auth state, upload handler
│   │   ├── api/
│   │   │   ├── fileApi.ts      Local file ops + P2P download + server backup
│   │   │   ├── authApi.ts      JWT storage, getCurrentUser
│   │   │   ├── groupsApi.ts    Group API calls
│   │   │   ├── labelsApi.ts    Label API calls
│   │   │   ├── foldersApi.ts   Folder API calls
│   │   │   └── statusApi.ts    Server status polling
│   │   ├── components/     FileList, FileUploader, BackupPanel, GroupsPanel,
│   │   │                   LabelManager, AdminPanel, PeerStatus,
│   │   │                   LoginButton, ServerSettings
│   │   ├── hooks/
│   │   │   └── useWebSocket.ts WebSocket lifecycle + auto-reconnect
│   │   └── lib/
│   │       └── serverConfig.ts URL persistence (localStorage / Tauri Store)
│   └── src-tauri/
│       ├── src/lib.rs      LocalStore, P2P server, Tauri commands, CDC
│       └── tauri.conf.json Capabilities, permissions, bundle config
│
└── .github/workflows/
    └── ci.yml              Rust (fmt + clippy + tests) · TS build · Tauri matrix
```

---

## 3. Server Architecture (Rust/Axum)

### 3.1 Shared State

All Axum handlers receive a clone of `AppState` via the extractor pattern:

```rust
#[derive(Clone)]
pub struct AppState {
    pub db: Database,                                      // SQLite pool
    pub storage: FileStorage,                              // Disk I/O
    pub chunk_tracker: Arc<ChunkTracker>,                  // In-memory peer/chunk map
    pub peers: Arc<DashMap<String, PeerInfo>>,             // Connected WebSocket peers
    pub peer_channels: Arc<DashMap<String, UnboundedSender<String>>>,  // WS write pumps
    pub metrics: Arc<ServerMetrics>,                       // Atomic counters
}
```

**Why `Arc<DashMap<…>>` instead of `Mutex<HashMap<…>>`**: DashMap is a sharded concurrent hashmap that avoids a single global lock. Under load with hundreds of peers connecting, a `Mutex<HashMap>` would serialize every peer join/leave/announce. DashMap shards the key space across multiple locks, making write contention O(1/shards) rather than O(1) of a global lock.

**Why `Arc` around DashMap and not just `DashMap`**: `Arc` allows `AppState` to implement `Clone` cheaply (clone increments the reference count, not the data). Axum clones `AppState` once per request, so this must be cheap.

**Why atomic `ServerMetrics`**: QUIC connection counters are incremented/decremented on every QUIC accept/close event. Using `AtomicU64` means zero lock contention for what could be thousands of events per second.

### 3.2 HTTP Router

```
GET  /api/files              List all server-stored files
GET  /api/files/check/{hash} Check existence + chunk availability
POST /api/upload             Upload a file (requires ?backup=true)
GET  /api/download/{hash}    Stream full file
GET  /api/chunk/{hash}/{id}  Serve single CDC chunk
GET  /api/chunks/{hash}      List chunk boundaries for a file
GET  /api/peers              List connected WebSocket peers
GET  /api/status             Server health + metrics
GET  /ws                     WebSocket upgrade (signaling)

GET  /api/auth/github         GitHub OAuth redirect
GET  /api/auth/github/callback OAuth callback → JWT
GET  /api/auth/me             Current user from JWT
POST /api/auth/logout

GET/POST  /api/groups
GET/DELETE /api/groups/{id}
POST /api/groups/{id}/members
DELETE /api/groups/{id}/members/{user_id}

GET/POST  /api/labels
DELETE    /api/labels/{id}
GET/POST  /api/files/{hash}/labels
DELETE    /api/files/{hash}/labels/{label_id}

GET/POST  /api/folders
GET       /api/folders/{id}
POST      /api/folders/{id}/files

POST /api/files/{hash}/share
```

The router is constructed by `build_router(state, public_dir, alt_svc)` in `lib.rs`. This function signature is also what the integration tests use — they call `build_router` directly, binding to a random port, without touching `main.rs`. This is the reason for the lib/main split.

**Why `ServeDir` nested under `/`**: The server also serves the compiled web frontend (if `PUBLIC_DIR` is set). This lets a single Axum instance handle both API requests and static file serving without a reverse proxy in development.

### 3.3 The Backup Gate

```rust
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
    // ...
}
```

This is the most important invariant in the entire system. Without this gate, any HTTP client that can reach the server could push files to it — turning the server into unintended cloud storage. The gate makes the P2P-first contract explicit: **the server does not store files unless you specifically tell it to**.

The integration test `upload_without_backup_flag_is_rejected` verifies this invariant survives refactors. Any change to the upload handler must keep this test passing.

### 3.4 Upload Pipeline

The upload handler avoids loading the entire file into memory using a temp-file-and-stream pattern:

1. Stream multipart body → temp file on disk (never in RAM).
2. Hash each chunk with `blake3::Hasher` as it arrives (single-pass).
3. Drop temp file handle, call `compute_chunks()` on disk path (off async runtime via `spawn_blocking`).
4. Atomically move temp file to content-addressed location via `save_file_from_path`.
5. Persist metadata and chunk boundaries to SQLite.

**TempFileGuard**: A RAII wrapper that deletes the temp file on `Drop` unless `disarm()`ed. If the handler returns early due to a deduplication hit or any error, the temp file is automatically cleaned up. Without this, a client that disconnects mid-upload leaves a multi-gigabyte file in `/tmp`.

```rust
struct TempFileGuard {
    path: PathBuf,
    armed: bool,  // true until save_file_from_path succeeds
}
impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
```

**Deduplication**: Before computing CDC boundaries (which reads the whole file), the server checks if the file hash already exists in storage. If so, it returns `status: "exists"` immediately and removes the temp file. This avoids running FastCDC on every duplicate upload of a large file.

### 3.5 Content-Defined Chunking (CDC)

```rust
pub const CDC_MIN_SIZE: u32 = 256 * 1024;      // 256 KB minimum chunk
pub const CDC_AVG_SIZE: u32 = 1_024 * 1_024;   // 1 MB average chunk
pub const CDC_MAX_SIZE: u32 = 4 * 1_024 * 1_024; // 4 MB maximum chunk
```

**Why FastCDC instead of fixed-size chunking:**

With fixed 256 KB chunks, inserting one byte at the start of a 1 GB file shifts every subsequent chunk boundary by one byte. The result: ~4096 "changed" chunks that are actually identical in content — just shifted. Every sync operation re-uploads the entire file.

FastCDC uses a rolling hash window. The window slides over the raw bytes and places a boundary whenever the hash output matches a bit pattern. Insertions near the start of a file only invalidate the chunk(s) that overlap the insertion. All downstream chunks retain their exact same content and hash — they don't need re-upload.

**Why these specific sizes:**
- **256 KB minimum**: Prevents boundary instability (the rolling hash occasionally fires very early). Below this floor, chunks are too small and metadata overhead grows.
- **1 MB average**: One network round-trip per megabyte. For a 100 MB file over a 100 Mbit/s LAN, this means ~100 requests. Manageable pipeline depth.
- **4 MB maximum**: Caps worst-case memory for in-flight chunk data. Without a cap, adversarial input could create a 10 GB "single chunk."

**BLAKE3 over SHA-256:**  
The server hashes both the full file (for deduplication identity) and each chunk (for integrity verification on serve). At 1 MB average chunk size, BLAKE3 is ~3× faster than SHA-256 on modern hardware. For a 10 GB upload with ~10,000 chunks, the hash time difference is meaningful. BLAKE3 is also designed for SIMD parallelism on multi-core systems.

**Blocking dispatch**: FastCDC's `StreamCDC` uses synchronous file I/O. Calling it directly inside an `async fn` would block the Tokio executor thread, starving other requests. `tokio::task::spawn_blocking` moves the work to Tokio's blocking thread pool — designed for exactly this kind of CPU-bound or sync-I/O work.

### 3.6 Storage Layer

**Directory sharding**:

```rust
fn get_file_path(&self, hash: &str) -> PathBuf {
    // e.g. "abc123..." → "./data/uploads/ab/abc123..."
    self.base_path.join(&hash[0..2]).join(hash)
}
```

With 64-character BLAKE3 hashes, the first two hex characters give 256 possible directory prefixes. A flat directory with 1 million files would have 1 million dentries, making `readdir` and path lookups slow on most filesystems. Two-level sharding caps any single directory at ~3900 files (1M ÷ 256) in a uniform distribution.

**Atomic save**: The server uses a rename-or-copy-and-rename strategy. `fs::rename` is O(1) and atomic on POSIX when source and destination share a filesystem. For cross-filesystem scenarios (e.g., `/tmp` on tmpfs, data on ext4), it falls back to: copy to a sibling temp file in the *destination* directory, sync to disk, then rename. This ensures the final path is never visible to readers in a partial state.

**ChunkTracker**:

```rust
pub struct ChunkTracker {
    // file_hash → chunk_id → set of peer IDs that have this chunk
    peer_chunks: DashMap<String, DashMap<u32, HashSet<String>>>,
    // Download state machine per peer (Missing→Requested→Downloading→Verified)
    chunk_states: DashMap<String, RwLock<HashMap<u32, ChunkState>>>,
}
```

This in-memory structure tracks which peers have which chunks without a database round-trip per chunk request. Peers announce their chunk availability over WebSocket on connect and after each download. The tracker enables the **rarest-first scheduler**: when a peer requests the next batch of chunks to fetch, the scheduler sorts by the number of peers that have each chunk (ascending) and assigns the rarest chunks first. This mirrors BitTorrent's rarest-first algorithm and maximises availability as the swarm grows.

**Bitfield encoding** follows the BitTorrent convention: bit `i` in byte `i/8`, MSB-first within each byte. Chunk 0 is bit 7 of byte 0; chunk 7 is bit 0 of byte 0; chunk 8 is bit 7 of byte 1. This allows future interoperability with BitTorrent clients and keeps peer exchange compact (8 chunks per byte).

### 3.7 Database

SQLite with WAL mode and a connection pool, running via `rusqlite` + `r2d2`.

**Schema summary:**

| Table | Purpose |
|---|---|
| `files` | One row per unique file: hash, name, size, MIME, upload timestamp, chunk count |
| `chunks` | One row per chunk boundary: file_hash, chunk_id, offset, length, chunk_hash |
| `users` | GitHub user ID, login, name, avatar |
| `groups` | Owner-created groups with name/description |
| `group_members` | M:N users ↔ groups with role (owner/member) |
| `labels` | User-owned named labels with hex colour |
| `file_labels` | M:N files ↔ labels |
| `file_shares` | Visibility (public/private/group) + group scope per file |
| `virtual_folders` | Hierarchical owner-scoped folder tree |
| `folder_files` | M:N files ↔ folders |

**Connection pool configuration:**

```sql
PRAGMA journal_mode=WAL;      -- readers don't block writers
PRAGMA synchronous=NORMAL;    -- durable at WAL checkpoint, not per-write
PRAGMA cache_size=10000;      -- 40 MB page cache per connection
PRAGMA busy_timeout=5000;     -- retry for up to 5s on SQLITE_BUSY
PRAGMA foreign_keys=ON;       -- referential integrity enforced
```

**WAL mode** is the most important of these. In the default journal mode, a `BEGIN EXCLUSIVE` write transaction blocks all readers. In WAL mode, readers read from the last committed checkpoint while writers append to the WAL file — they never block each other. For a server handling concurrent upload, download, and status requests, this means a slow upload (writing metadata) doesn't stall status page requests.

**`max_size=16` connections**: Each Axum worker thread can hold a connection from the pool. 16 connections supports up to 16 concurrent handler invocations that need the database simultaneously. SQLite handles this gracefully in WAL mode because most operations are very short-lived.

### 3.8 WebSocket Signaling

The signaling layer connects peers for file discovery and chunk exchange coordination. It does **not** transfer file data — only metadata.

**Message types (tagged union):**

```
welcome       → assigned peer ID on connect
join          → declare interest in a "room" (typically a file hash)
peer-list     → server broadcasts updated peers in a room
announce      → peer declares which file hashes it has
bitfield      → compact chunk availability update
pipeline-request → client requests rarest-first chunk assignments
signal        → opaque WebRTC signaling payload (forwarded)
chunk-peers   → server returns peers who have a specific chunk
```

**Write pump pattern:**

```rust
let (peer_tx, mut peer_rx) = mpsc::unbounded_channel::<String>();
state.peer_channels.insert(peer_id.clone(), peer_tx);

tokio::spawn(async move {
    while let Some(msg) = peer_rx.recv().await {
        ws_sink.send(Message::Text(msg)).await?;
    }
});
```

Each connected peer has a dedicated Tokio task that reads from an unbounded channel and writes to the WebSocket sink. Any handler that wants to push a message to a peer looks up the channel by peer ID and sends without holding any lock on the WebSocket. This decouples message production from I/O and provides natural backpressure (the channel fills if the client is slow to drain).

**Peer lifecycle:**
1. Connect → assign UUID, store in `peer_channels`, send `welcome`.
2. `join` → add to room map, broadcast updated `peer-list` to all room members.
3. `announce` → update `ChunkTracker` with the peer's file hashes.
4. `bitfield` → fine-grained update of chunk-level availability.
5. `pipeline-request` → respond with rarest-first chunk assignments.
6. Disconnect → abort write pump, remove from `peer_channels`, `peers`, `ChunkTracker`.

### 3.9 QUIC / HTTP3 Transport

The server runs three listeners simultaneously:

| Listener | Protocol | Port | Purpose |
|---|---|---|---|
| `axum::serve` | TCP HTTP/1.1 + HTTP/2 | `$PORT` (default 3000) | Primary API |
| `quic::serve_quic` | UDP QUIC/HTTP3 | Same `$PORT` | HTTP/3 upgrade |
| `quic::serve_quic_mgmt` | UDP QUIC/HTTP3 | `$MGMT_PORT` (default 4433) | Admin status only |

**Why QUIC for chunk transfers:**

HTTP/1.1 and HTTP/2 run over TCP. TCP's head-of-line blocking means a lost packet stalls all in-flight requests on that connection. For a client fetching 100 chunks from a server, one dropped packet can stall the 99 other in-flight requests.

QUIC runs over UDP with independent stream multiplexing. Each chunk fetch is an independent QUIC stream; a lost packet only stalls that one stream. This matters on high-latency or lossy connections (e.g., mobile, international).

**Alt-Svc advertisement:**

```
Alt-Svc: h3=":3000"; ma=86400, h3-29=":3000"; ma=86400
```

This header is injected into every TCP HTTP response, telling the browser/client that the same endpoint speaks HTTP/3 on the same port. Clients that support QUIC will upgrade on the next request. `ma=86400` tells clients to remember this hint for 24 hours.

**TLS:**

In development, `rcgen` generates a self-signed certificate at startup. For production, set `QUIC_CERT_PEM` and `QUIC_KEY_PEM` environment variables to point to real certificate files (e.g., from Let's Encrypt). The server uses TLS 1.3 exclusively (enforced by `rustls`).

**Transport tuning:**

```rust
transport
    .stream_receive_window(1_000_000u32.into())  // 1 MB per QUIC stream
    .receive_window(8_000_000u32.into())         // 8 MB total connection window
    .max_idle_timeout(Some(Duration::from_secs(30)));
```

Stream window = average chunk size. This means an entire average chunk can be in flight without waiting for a flow-control ACK. The connection window of 8 MB allows up to 8 simultaneous 1 MB chunks to be in flight concurrently, aligning with the pipeline-request batch size.

### 3.10 Authentication & Authorization

**GitHub OAuth + JWT:**

The server uses GitHub as the only identity provider. A user authenticates via OAuth and receives a short-lived (24h) JWT. All subsequent API calls carry this JWT as a Bearer token.

```
GET /api/auth/github
  → Redirect to github.com/login/oauth/authorize

GET /api/auth/github/callback?code=…
  → Exchange code for GitHub access token
  → Fetch github.com/user profile
  → Upsert user in database
  → Issue JWT (HS256, 24h expiry)
  → Redirect to frontend /?token=<jwt>

Frontend:
  → Extract token from URL, store in localStorage
  → Remove token from URL (replaceState)
  → Pass as Authorization: Bearer <jwt> on API calls
```

**Extractors:**

- `CurrentUser` — rejects with 401 if the JWT is missing or invalid. Used on routes that require authentication (group management, sharing).
- `OptionalUser` — never rejects; provides `Option<User>`. Used where both authenticated and anonymous access are supported (e.g., public file downloads).

**Why 24-hour JWT expiry:** Short enough to limit the blast radius of a stolen token; long enough that a user doesn't have to re-authenticate every work session.

### 3.11 Groups, Labels, and Virtual Folders

These three subsystems add collaborative and organisational metadata on top of the core P2P storage engine.

**Groups** allow a set of users to share files with access control. A user who creates a group is its owner. The owner can add and remove members. Files can be shared with a group via the `/api/files/{hash}/share` endpoint (visibility = "group", group_id = the group's UUID). Group members can then discover and download those files.

**Labels** are user-owned coloured tags applied to files. Labels are scoped to their creator — a user cannot apply another user's label. The M:N `file_labels` table allows a file to have multiple labels and a label to tag multiple files. Labels enable faceted filtering in the UI.

**Virtual Folders** are a hierarchy of named containers owned by a user. Files can be added to folders without moving the actual file on disk — the folder is purely a metadata relationship in `folder_files`. Folders support nesting via a `parent_id` self-reference. This allows users to organise their file collection without duplicating storage.

### 3.12 Monitoring

```rust
pub struct ServerMetrics {
    pub started_at: AtomicI64,            // Unix timestamp of server start
    pub active_quic_connections: AtomicU64,
    pub total_quic_connections: AtomicU64,
}
```

All metrics use `std::sync::atomic` types — no locks. QUIC accept/close events increment/decrement counters with `Relaxed` ordering (counts don't need sequential consistency across CPUs). The `/api/status` endpoint assembles a snapshot by reading atomics and querying the database for file/byte counts. A failed DB query returns zero rather than an error to keep the status page usable even under DB stress.

### 3.13 lib.rs / main.rs Split

```rust
// server/src/main.rs
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    anna_sync_server::run().await
}
```

`main.rs` is exactly four lines. All server logic lives in `lib.rs`, compiled as a library crate (`anna-sync-server`). This is the enabling condition for integration tests: `tests/integration.rs` can import `build_router`, `AppState`, and all public types directly, construct an isolated server bound to `127.0.0.1:0` (a random free port), and run the full HTTP stack in-process without spawning a subprocess or mocking any layers. The test harness is the same code path the production binary uses.

---

## 4. Desktop Client Architecture (Tauri 2.0 + React)

### 4.1 Local Store

```rust
pub struct LocalStore {
    pub db: Pool<SqliteConnectionManager>,  // r2d2 + rusqlite
    pub files_dir: PathBuf,                 // content-addressed file storage
    pub p2p_addr: OnceLock<String>,         // "192.168.1.5:54321", set once at startup
}
```

The desktop app has its own SQLite database (in the OS app data directory) with the same `files` and `chunks` tables as the server. This local DB is the source of truth for what files are on this device.

**`OnceLock<String>` for P2P address**: The embedded P2P HTTP server starts once during Tauri's `setup` hook, binds to a random port, and discovers the machine's LAN IP. This address is written into the `OnceLock` exactly once. All subsequent calls to `get_p2p_address` read from it without any locking overhead. A `Mutex<Option<String>>` would work but introduces unnecessary contention; `OnceLock` makes the write-once guarantee explicit in the type.

**Local SQLite schema:**

```sql
CREATE TABLE files (
    hash TEXT PRIMARY KEY,
    name TEXT, size INTEGER, mime_type TEXT,
    uploaded_at INTEGER, chunk_count INTEGER
);

CREATE TABLE chunks (
    file_hash TEXT REFERENCES files(hash),
    chunk_id INTEGER,
    offset INTEGER, length INTEGER, hash TEXT,
    PRIMARY KEY (file_hash, chunk_id)
);
```

Identical in structure to the server schema (minus `compressed` — the desktop app never stores legacy Brotli-compressed files), which means chunk boundary information is directly comparable between server and client, enabling P2P verification without format translation.

### 4.2 P2P HTTP Server (Embedded Axum)

Each desktop instance runs an embedded Axum HTTP server on a randomly-assigned port, accessible on the local network:

```
GET /p2p/files                  List locally-stored files
GET /p2p/chunk/{hash}/{chunk_id} Serve one chunk from local storage
GET /p2p/chunks/{hash}          List chunk boundaries for a file
```

**Why embedded HTTP instead of a custom protocol**: HTTP is universally supported, easy to debug with curl, and lets the same `fileApi.ts` code that talks to the central server also talk to peers. No custom protocol implementation needed on the client side.

**Startup sequence** (in `lib.rs` setup hook):

```rust
let store = LocalStore::new(data_dir)?;
let store_arc = Arc::new(store);
let addr = tauri::async_runtime::block_on(start_p2p_server(store_arc.clone()))?;
let _ = store_arc.p2p_addr.set(addr);  // "192.168.x.x:PORT"
app.manage(store_arc);
```

The `Arc` means the P2P server's Axum state and all Tauri commands share a single `LocalStore` instance and therefore a single SQLite connection pool. No two pools opening the same database file.

### 4.3 Tauri Commands

Tauri commands are Rust functions callable from TypeScript via `invoke(...)`. The command boundary is the security boundary — TypeScript cannot access the filesystem or database directly.

| Command | Description |
|---|---|
| `store_file(file_path)` | Read file, compute BLAKE3 + CDC, store locally, return `LocalFile` |
| `list_local_files()` | Query local DB for all files |
| `delete_local_file(hash)` | Remove file from local storage and DB |
| `get_p2p_address()` | Return the `OnceLock` value (LAN IP:port) |
| `get_local_chunks(hash)` | Return chunk boundaries from local DB |
| `backup_to_server(hash, server_url)` | Stream local file to server with `?backup=true` |
| `restore_from_server(hash, server_url)` | Download file from server, store locally |
| `get_server_url()` | Read persisted server URL from Tauri Store |
| `set_server_url(url)` | Persist server URL to Tauri Store |

**`store_file` pipeline:**

1. TypeScript picks a file via Tauri file dialog → passes the OS path to `store_file`.
2. Rust reads the file, runs BLAKE3 hash + FastCDC on a `spawn_blocking` thread.
3. Dedup check: if hash exists in local DB, return existing record immediately.
4. Copy file to content-addressed local storage.
5. Insert metadata + chunk boundaries into local DB.
6. Return `LocalFile` to TypeScript.
7. TypeScript announces the new file hash to peers via WebSocket.

**`backup_to_server` uses streaming**: The file is opened as a `tokio::fs::File` and wrapped in a `multipart::Part::stream(file)` — the file data is never loaded into memory. This handles the "Backup" use case for files up to 10 GB without OOM risk.

### 4.4 Frontend (React / TypeScript)

The React UI is structured around five tabs reflecting the main user workflows:

| Tab | Component | Purpose |
|---|---|---|
| Files | `FileList`, `FileUploader` | Browse and upload local files |
| Backup | `BackupPanel` | Push local files to server / restore from server |
| Groups | `GroupsPanel` | Create groups, manage membership |
| Labels | `LabelManager` | Create and apply labels |
| Status | `AdminPanel` | Server metrics and peer list |

**P2P download logic** (`fileApi.ts`):

```typescript
for (const addr of peerAddresses) {
    // 1. Fetch chunk boundaries from peer's embedded HTTP server
    const chunks = await fetch(`http://${addr}/p2p/chunks/${hash}`, { signal: AbortSignal.timeout(3000) })
    // 2. Download each chunk (3s timeout per chunk)
    // 3. If all chunks succeeded: return assembled Blob
    if (success && chunkBlobs.length > 0) return new Blob(chunkBlobs)
    // 4. Otherwise: try next peer
}
// 5. All peers failed → fall back to central server
return axios.get(`${serverUrl}/api/download/${hash}`, { responseType: 'blob' }).then(r => r.data)
```

The 3-second timeout per peer prevents slow peers from blocking the download indefinitely. If a peer is unreachable, the `AbortSignal.timeout` causes the fetch to reject, the catch block skips to the next peer. Server fallback is only reached if every known peer fails.

**WebSocket hook** (`useWebSocket.ts`): Manages the WebSocket connection lifecycle, including exponential reconnect (currently fixed at 3 seconds). The `connectRef` pattern (storing `connect` in a ref updated after every render) avoids the stale-closure problem where the reconnect callback captures an old version of `connect`.

**Upload → announce flow:**

```typescript
const result = await uploadFile(file)   // Tauri: store_file
await fetchFiles()                       // Refresh local file list
if (connected) {
    sendMessage({ type: 'announce', files: [result.hash] })  // Tell peers
}
```

This is the full P2P announce cycle: store locally, then tell the signaling server that this peer now has the file. Other peers listening on the same file hash room will receive the update and can start fetching chunks from this device.

### 4.5 Server Settings Persistence

The server URL must persist across app restarts. On web (dev mode), `localStorage` is used. In the native app, `localStorage` is cleared when the WebView is rebuilt or the app is reinstalled. Tauri provides a `Store` plugin that writes to a JSON file in the app data directory — durable across WebView rebuilds.

`serverConfig.ts` abstracts both:
- `getServerUrlSync()`: Returns from localStorage in web mode; returns the last-known cached value in Tauri mode (populated by `getServerUrl()` on startup).
- `setServerUrl(url)`: Writes to the Tauri Store in native mode; to localStorage in web mode.

The async `setServerUrl` is used from the settings modal's save handler to ensure the URL persists in the native app.

---

## 5. Data Flow Walkthroughs

### 5.1 File Upload (local, P2P-first)

```
User selects file
  → FileUploader.tsx calls uploadFile(file)
    → [Tauri mode] invoke('store_file', { file_path })
       → Rust: spawn_blocking { BLAKE3 hash + FastCDC }
       → Rust: copy to local files_dir/ab/abcdef...
       → Rust: INSERT into local files + chunks tables
       → Return { hash, name, size, chunk_count }
    → [Web mode] POST to /api/upload?backup=true
  → App.tsx refreshes file list
  → App.tsx: sendMessage({ type: 'announce', files: [hash] })
     → WebSocket → signaling server → broadcast to peers in hash's room
```

The server is not involved in Tauri mode upload except for the optional backup. The file exists only on the device until the user explicitly backs it up.

### 5.2 Backup to Server (explicit)

```
User clicks "Backup" on a file
  → BackupPanel.tsx calls backupToServer(hash)
    → invoke('backup_to_server', { hash, server_url })
       → Rust: open file as tokio::fs::File (streaming)
       → Rust: POST /api/upload?backup=true  (multipart stream)
         → Server: backup gate checks ?backup=true ✓
         → Server: stream to temp file, hash, CDC, atomic move
         → Server: INSERT into server files + chunks tables
         → Return { status: "success", hash, chunk_count }
```

### 5.3 P2P Download with Server Fallback

```
User requests a file download
  → fileApi.ts: downloadFile(hash, peerAddresses)
    For each peer address:
      1. GET http://{peer}/p2p/chunks/{hash}  (3s timeout)
         ← [ { chunk_id, offset, length, hash }, ... ]
      2. For each chunk:
         GET http://{peer}/p2p/chunk/{hash}/{id}  (3s timeout)
         ← raw bytes
      3. If all chunks received: return new Blob(chunkBlobs)
    If all peers failed:
      GET {server_url}/api/download/{hash}
      ← full file as blob
  → FileList triggers browser download
```

---

## 6. Key Invariants

These invariants must be preserved across all changes. The integration tests enforce them.

| Invariant | Location | Test |
|---|---|---|
| `POST /api/upload` requires `?backup=true` | `lib.rs:274` | `upload_without_backup_flag_is_rejected` |
| CDC parameters: min=256 KB, avg=1 MB, max=4 MB | `cdc.rs` + `client/src-tauri/src/lib.rs` | `chunk_list_matches_upload_chunk_count` |
| All hashes use BLAKE3 (not SHA-256) | `lib.rs`, `cdc.rs`, `client/src-tauri/src/lib.rs` | `upload_with_backup_flag_succeeds` (asserts 64-char hash) |
| Chunk integrity verified before serving | `lib.rs` `get_chunk` handler | `chunk_fetch_returns_correct_bytes` |
| Temp files cleaned up on upload error/abort | `TempFileGuard` in `lib.rs` | (implicit: no disk leak under repeated tests) |
| Server and client CDC parameters match | Both `cdc.rs` and Tauri `lib.rs` use identical constants | Manual cross-device verification |

---

## 7. CI / CD

```yaml
jobs:
  server:             # Gated: must pass before Tauri builds
    - cargo fmt --check          # No formatting drift
    - cargo clippy -- -D warnings # No lint warnings
    - cargo test --lib           # Unit tests
    - cargo test --test integration  # 16 integration tests

  client:             # Gated: must pass before Tauri builds
    - npm run lint               # ESLint (errors = fail)
    - npm run build              # tsc + vite build (type errors = fail)

  tauri:              # Runs only when server + client pass
    needs: [server, client]
    strategy:
      matrix:
        - Linux   (ubuntu-22.04)
        - Windows (windows-latest)
        - macOS   (macos-latest, --target universal-apple-darwin)
    - tauri-apps/tauri-action@v0
    - actions/upload-artifact    # .deb, .AppImage, .exe, .msi, .dmg
```

**Why `needs: [server, client]` on the Tauri job**: Tauri builds are expensive (10–20 minutes per platform, including a full Rust compile of the embedded backend). Gating them behind passing server and client checks prevents wasting runner minutes on a build that is known to be broken.

**Why the integration tests are the source of truth**: Unit tests can pass while the API contract breaks (e.g., a handler returns a different JSON field name). Integration tests spin up a real HTTP server against a real SQLite database and make real HTTP requests. They catch contract regressions that unit tests miss. Any change to a handler's observable behaviour must come with a matching change to `tests/integration.rs`.

---

## 8. Design Decision Reference

| Decision | Alternative Considered | Why This Way |
|---|---|---|
| **FastCDC variable-size chunking** | Fixed 256 KB chunks | Content-shift invariance: only changed chunks re-sync |
| **BLAKE3 for all hashes** | SHA-256 | ~3× faster; same output size; designed for SIMD parallelism |
| **SQLite + WAL mode** | PostgreSQL, RocksDB | Single-binary deployment; no separate DB process; WAL gives concurrent reads+writes |
| **`?backup=true` gate** | No gate (always store) | P2P-first contract: server storage is opt-in, not default |
| **DashMap** | `Mutex<HashMap>` | Sharded lock; no contention on peer join/leave under load |
| **Write pump per peer** | Direct `ws_sink.send` in handlers | Decouples message production from WebSocket I/O; natural backpressure |
| **Rarest-first chunk scheduling** | Sequential chunk ordering | Maximises swarm availability; prevents starvation of rare chunks |
| **MSB-first bitfield** | LSB-first | BitTorrent convention; future protocol interop |
| **`OnceLock` for P2P address** | `Mutex<Option<String>>` | Write-once guarantee in type; zero lock overhead after startup |
| **Single `Arc<LocalStore>`** | Separate store per command | One SQLite pool; no multi-writer contention on same DB file |
| **Embedded Axum P2P server** | Custom binary protocol | Reuses HTTP tooling; same `fileApi.ts` client code for server and peers |
| **lib.rs / main.rs split** | Monolithic main.rs | Enables `build_router` in integration tests without subprocess or mocking |
| **Tauri 2.0** | Electron, PWA | Native performance; smaller binary (~5 MB vs 200+ MB); sandboxed capabilities model |
| **2-level directory sharding** | Flat storage directory | Caps dentries per directory at ~4 K; fast `readdir` at scale |
| **RAII TempFileGuard** | Explicit cleanup in each code path | Cleanup is guaranteed even on early returns or panics |
| **Self-signed QUIC cert in dev** | Skip QUIC in dev | Tests the real QUIC code path in development; swap to real cert via env vars in production |
| **3-second P2P peer timeout** | No timeout | Prevents slow peers from stalling downloads; moves to next peer or server fallback quickly |
