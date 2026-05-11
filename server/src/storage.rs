use anyhow::Result;
use dashmap::DashMap;
use serde::Serialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::RwLock,
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt},
};
use uuid::Uuid;

// ══════════════════════════════════════════════════════════════════════════════
// FileStorage
// ══════════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
pub struct FileStorage {
    base_path: PathBuf,
}

impl FileStorage {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path).await?;
        Ok(Self { base_path })
    }

    // ---------------------------------------------------------------- writers

    /// Atomically move `src` (a temp file) to the content-addressed location
    /// for `hash`.  Two strategies:
    ///   1. `fs::rename` – O(1), works when src and dst are on the same filesystem.
    ///   2. Copy-then-rename – cross-filesystem fallback; writes to a sibling
    ///      temp in the destination directory, then renames into place so
    ///      readers never see a partial file.
    pub async fn save_file_from_path(&self, hash: &str, src: impl AsRef<Path>) -> Result<()> {
        let src = src.as_ref();
        let dst = self.get_file_path(hash);

        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Fast path: same filesystem.
        if fs::rename(src, &dst).await.is_ok() {
            return Ok(());
        }

        // Slow path: cross-filesystem.  Write to a temp file in the
        // destination directory, then rename so the final path is never seen
        // in a half-written state.
        let sibling_tmp = dst.with_extension(format!("tmp.{}", Uuid::new_v4()));
        fs::copy(src, &sibling_tmp).await?;
        {
            // Flush to disk before making it visible.
            let f = File::open(&sibling_tmp).await?;
            f.sync_all().await?;
        }
        if fs::rename(&sibling_tmp, &dst).await.is_err() {
            // Another concurrent upload of the same content beat us here.
            // Both files are identical (content-addressed), so just clean up.
            let _ = fs::remove_file(&sibling_tmp).await;
        }
        let _ = fs::remove_file(src).await;
        Ok(())
    }

    // ---------------------------------------------------------------- readers

    /// Read `length` bytes starting at `offset` from the stored file for
    /// `hash`.  This is O(chunk_size) — it never loads the entire file.
    pub async fn read_chunk(&self, hash: &str, offset: u64, length: u32) -> Result<Vec<u8>> {
        let file_path = self.get_file_path(hash);
        let mut file = File::open(&file_path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; length as usize];
        file.read_exact(&mut buf).await?;
        Ok(buf)
    }

    /// Read the entire stored file for `hash`.  Only used for full downloads
    /// and legacy compressed-file decompression.
    pub async fn read_file(&self, hash: &str) -> Result<Vec<u8>> {
        let file_path = self.get_file_path(hash);
        let mut file = File::open(&file_path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        Ok(data)
    }

    pub async fn file_exists(&self, hash: &str) -> bool {
        tokio::fs::try_exists(self.get_file_path(hash))
            .await
            .unwrap_or(false)
    }

    /// Remove the stored file for `hash` from disk.
    ///
    /// Called by the admin delete endpoint (not yet wired in routes).
    #[allow(dead_code)]
    pub async fn delete_file(&self, hash: &str) -> Result<()> {
        fs::remove_file(self.get_file_path(hash)).await?;
        Ok(())
    }

    /// Expose the resolved path so callers can open the file directly for
    /// streaming (e.g. `tokio_util::io::ReaderStream`).
    pub fn get_file_path(&self, hash: &str) -> PathBuf {
        if hash.len() >= 2 {
            self.base_path.join(&hash[0..2]).join(hash)
        } else {
            self.base_path.join(hash)
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// ChunkState
// ══════════════════════════════════════════════════════════════════════════════

/// Lifecycle of a single chunk as seen by a downloading peer.
///
/// Missing → Requested → Downloading → Verified
///                ↓ (timeout / error)
///             Missing
///
/// This state machine is maintained per-file on each downloading client and
/// used by the rarest-first scheduler to avoid duplicate requests.  The server
/// side stores it in `ChunkTracker::chunk_states` so the signaling handler can
/// update and query states without holding a lock on the entire peer map.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkState {
    /// Not yet requested.
    Missing,
    /// Request sent; waiting for data.
    Requested,
    /// Data is in flight.
    Downloading,
    /// Data received and BLAKE3 hash verified.
    Verified,
}

// ══════════════════════════════════════════════════════════════════════════════
// ChunkTracker
// ══════════════════════════════════════════════════════════════════════════════

/// Tracks which chunks each peer has, supports rarest-first scheduling, and
/// generates compact bitfield representations for fast peer exchange.
///
/// # Bitfield encoding
/// Bit `i` of byte `i / 8` (MSB = chunk 0 within each byte) is set when
/// chunk `i` is available.  This matches the BitTorrent bitfield convention
/// so future protocol interoperability is straightforward.
pub struct ChunkTracker {
    /// file_hash → chunk_id → set of peer IDs that have this chunk.
    peer_chunks: DashMap<String, DashMap<u32, std::collections::HashSet<String>>>,
    /// Per-peer download state machine: file_hash → (chunk_id → ChunkState).
    /// Used by the rarest-first scheduler to track in-flight requests and avoid
    /// requesting the same chunk from multiple peers simultaneously.
    #[allow(dead_code)]
    chunk_states: DashMap<String, RwLock<HashMap<u32, ChunkState>>>,
}

impl ChunkTracker {
    pub fn new() -> Self {
        Self {
            peer_chunks: DashMap::new(),
            chunk_states: DashMap::new(),
        }
    }

    // --------------------------------------------------------- peer registry

    /// Register a single chunk as available on the server (no peer owner).
    pub fn add_chunk(&self, file_hash: &str, chunk_id: u32) {
        self.peer_chunks
            .entry(file_hash.to_string())
            .or_default()
            .insert(chunk_id, std::collections::HashSet::new());
    }

    /// Register a chunk as available from a specific peer.
    pub fn add_peer_chunk(&self, file_hash: &str, chunk_id: u32, peer_id: String) {
        self.peer_chunks
            .entry(file_hash.to_string())
            .or_default()
            .entry(chunk_id)
            .or_default()
            .insert(peer_id);
    }

    /// Remove a peer from all chunk ownership records (called on disconnect).
    pub fn remove_peer(&self, peer_id: &str) {
        for file_entry in self.peer_chunks.iter() {
            for mut chunk_entry in file_entry.value().iter_mut() {
                chunk_entry.value_mut().remove(peer_id);
            }
        }
    }

    // ----------------------------------------------------------- bitfield I/O

    /// Apply a bitfield sent by `peer_id` for `file_hash`.
    ///
    /// `chunk_count` is the total number of chunks in the file; bits beyond
    /// `chunk_count` are ignored.
    pub fn set_peer_bitfield(
        &self,
        file_hash: &str,
        peer_id: &str,
        bitfield: &[u8],
        chunk_count: u32,
    ) {
        let file_chunks = self
            .peer_chunks
            .entry(file_hash.to_string())
            .or_default();

        let peer_str = peer_id.to_string();

        for (byte_idx, &byte) in bitfield.iter().enumerate() {
            for bit in 0u32..8 {
                let chunk_id = (byte_idx as u32) * 8 + bit;
                if chunk_id >= chunk_count {
                    return;
                }
                // MSB of each byte = lowest chunk index in that byte group.
                if byte & (1 << (7 - bit)) != 0 {
                    file_chunks
                        .entry(chunk_id)
                        .or_default()
                        .insert(peer_str.clone());
                }
            }
        }
    }

    /// Build a bitfield representing which chunk IDs the server has for
    /// `file_hash` (i.e. the chunks with at least one peer or server copy).
    pub fn get_server_bitfield(&self, file_hash: &str) -> Vec<u8> {
        let Some(file_chunks) = self.peer_chunks.get(file_hash) else {
            return Vec::new();
        };

        let max_chunk = match file_chunks.iter().map(|e| *e.key()).max() {
            Some(m) => m,
            None => return Vec::new(),
        };

        let byte_count = (max_chunk as usize / 8) + 1;
        let mut bitfield = vec![0u8; byte_count];

        for entry in file_chunks.iter() {
            let chunk_id = *entry.key() as usize;
            bitfield[chunk_id / 8] |= 1 << (7 - (chunk_id % 8));
        }

        bitfield
    }

    // ------------------------------------------------------- chunk scheduling

    /// Return all chunk IDs that are registered for `file_hash`.
    pub fn get_available_chunks(&self, file_hash: &str) -> Vec<u32> {
        self.peer_chunks
            .get(file_hash)
            .map(|m| m.iter().map(|e| *e.key()).collect())
            .unwrap_or_default()
    }

    /// Return the peers that have `chunk_id` for `file_hash`.
    pub fn get_peers_for_chunk(&self, file_hash: &str, chunk_id: u32) -> Vec<String> {
        if let Some(file_chunks) = self.peer_chunks.get(file_hash) {
            if let Some(peers) = file_chunks.get(&chunk_id) {
                return peers.value().iter().cloned().collect();
            }
        }
        Vec::new()
    }

    /// Rarest-first scheduling: given the `requested` chunk IDs, return them
    /// sorted so the least-available chunks come first.  Each element is
    /// `(chunk_id, peers_that_have_it)`.
    ///
    /// Chunks with zero known peers are placed last rather than first to avoid
    /// requesting chunks that no one can serve yet.
    pub fn get_rarest_chunk_assignments(
        &self,
        file_hash: &str,
        requested: &[u32],
    ) -> Vec<serde_json::Value> {
        let Some(file_chunks) = self.peer_chunks.get(file_hash) else {
            return Vec::new();
        };

        let mut scored: Vec<(u32, usize, Vec<String>)> = requested
            .iter()
            .filter_map(|&cid| {
                file_chunks.get(&cid).map(|peers| {
                    let list: Vec<String> = peers.iter().cloned().collect();
                    let count = list.len();
                    (cid, count, list)
                })
            })
            .collect();

        // Sort: chunks with the fewest peers first (rarest first).
        // Chunks with 0 peers go to the end so we do not request the
        // unobtainable before the rare.
        scored.sort_by(|a, b| {
            match (a.1, b.1) {
                (0, 0) => a.0.cmp(&b.0),
                (0, _) => std::cmp::Ordering::Greater,
                (_, 0) => std::cmp::Ordering::Less,
                _ => a.1.cmp(&b.1),
            }
        });

        scored
            .iter()
            .map(|(cid, _, peers)| {
                serde_json::json!({
                    "chunk_id": cid,
                    "peers": peers,
                })
            })
            .collect()
    }

    // ------------------------------------------------------- state machine

    /// Update the state of a chunk for a given file.
    ///
    /// Called by the rarest-first scheduler when a chunk request is dispatched
    /// to a peer, transitioning the state from `Missing` → `Requested`.
    #[allow(dead_code)]
    pub fn set_chunk_state(&self, file_hash: &str, chunk_id: u32, state: ChunkState) {
        self.chunk_states
            .entry(file_hash.to_string())
            .or_insert_with(|| RwLock::new(HashMap::new()))
            .write()
            .expect("ChunkTracker state RwLock should never be poisoned")
            .insert(chunk_id, state);
    }

    /// Read the current state of a chunk.
    #[allow(dead_code)]
    pub fn get_chunk_state(&self, file_hash: &str, chunk_id: u32) -> ChunkState {
        self.chunk_states
            .get(file_hash)
            .and_then(|lock| {
                lock.read()
                    .expect("ChunkTracker state RwLock should never be poisoned")
                    .get(&chunk_id)
                    .copied()
            })
            .unwrap_or(ChunkState::Missing)
    }
}

