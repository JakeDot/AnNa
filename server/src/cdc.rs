//! Content-Defined Chunking (CDC) using the FastCDC algorithm.
//!
//! Instead of fixed-size 256 KB slices, FastCDC slides a rolling-hash window
//! over the file data and places chunk boundaries wherever the hash matches a
//! threshold pattern.  This means that inserting a few bytes near the start of
//! a large file only invalidates the chunks that actually overlap the change;
//! all other chunks retain their hashes and can be skipped during sync.
//!
//! Throughput sizing rationale
//! ---------------------------
//! * MIN  256 KB – avoids tiny chunk overhead on noisy boundaries.
//! * AVG  1   MB – one round-trip per MB keeps pipeline depth manageable.
//! * MAX  4   MB – caps worst-case chunk memory without hurting dedup quality.
//!
//! Each chunk is hashed with BLAKE3 (3× faster than SHA-256) so that:
//!   1. Peers can verify chunk integrity without trusting the source.
//!   2. Identical chunks across different files are naturally deduplicated.

use anyhow::Result;
use fastcdc::v2020::StreamCDC;
use serde::Serialize;
use std::path::Path;

pub const CDC_MIN_SIZE: u32 = 256 * 1024; //   256 KB
pub const CDC_AVG_SIZE: u32 = 1024 * 1024; //    1 MB
pub const CDC_MAX_SIZE: u32 = 4 * 1024 * 1024; //    4 MB

/// Byte offset + length of one CDC chunk, plus its BLAKE3 content hash.
#[derive(Debug, Clone, Serialize)]
pub struct ChunkBoundary {
    pub chunk_id: u32,
    pub offset: u64,
    pub length: u32,
    /// Hex-encoded BLAKE3 hash of the raw chunk bytes.
    pub hash: String,
}

/// Run FastCDC over the file at `path` and return the ordered list of chunk
/// boundaries.  The file is opened with the blocking `std::fs::File` API and
/// the work is dispatched to Tokio's blocking thread pool so it never stalls
/// the async runtime.
pub async fn compute_chunks(path: impl AsRef<Path>) -> Result<Vec<ChunkBoundary>> {
    let path = path.as_ref().to_path_buf();

    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let chunker = StreamCDC::new(file, CDC_MIN_SIZE, CDC_AVG_SIZE, CDC_MAX_SIZE);

        let mut boundaries = Vec::new();
        let mut chunk_id: u32 = 0;

        for result in chunker {
            let chunk = result.map_err(|e| anyhow::anyhow!("CDC error: {e}"))?;

            // BLAKE3: same output length as SHA-256 (64 hex chars), ~3× faster.
            let hash = blake3::hash(&chunk.data).to_hex().to_string();

            boundaries.push(ChunkBoundary {
                chunk_id,
                offset: chunk.offset as u64,
                length: chunk.length as u32,
                hash,
            });
            chunk_id += 1;
        }

        Ok::<Vec<ChunkBoundary>, anyhow::Error>(boundaries)
    })
    .await?
}
