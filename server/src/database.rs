use anyhow::Result;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use std::path::Path;
use tokio::fs;

use crate::{cdc::ChunkBoundary, FileMetadata};

/// SQLite database handle backed by an r2d2 connection pool.
///
/// A pool of up to 16 connections is maintained so that concurrent async
/// handlers can each `spawn_blocking` without serialising on a single
/// `Connection::open` call.  WAL journal mode is enabled on every connection
/// so readers and the single writer never block each other.
#[derive(Clone)]
pub struct Database {
    pool: Pool<SqliteConnectionManager>,
}

impl Database {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path_ref = path.as_ref();

        if let Some(parent) = path_ref.parent() {
            fs::create_dir_all(parent).await?;
        }

        let manager = SqliteConnectionManager::file(path_ref).with_init(|conn| {
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA cache_size=10000;
                 PRAGMA busy_timeout=5000;
                 PRAGMA foreign_keys=ON;",
            )?;
            Ok(())
        });

        let pool = Pool::builder().max_size(16).build(manager)?;

        Ok(Self { pool })
    }

    pub async fn init(&self) -> Result<()> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS files (
                    hash        TEXT    PRIMARY KEY,
                    name        TEXT    NOT NULL,
                    size        INTEGER NOT NULL,
                    mime_type   TEXT    NOT NULL,
                    uploaded_at INTEGER NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    compressed  INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_uploaded_at
                    ON files(uploaded_at DESC);

                -- One row per CDC chunk: stores the byte range within the
                -- stored file so chunks can be served with a single seek.
                CREATE TABLE IF NOT EXISTS chunks (
                    file_hash  TEXT    NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
                    chunk_id   INTEGER NOT NULL,
                    offset     INTEGER NOT NULL,
                    length     INTEGER NOT NULL,
                    chunk_hash TEXT    NOT NULL,
                    PRIMARY KEY (file_hash, chunk_id)
                );
                CREATE INDEX IF NOT EXISTS idx_chunks_file
                    ON chunks(file_hash);
                CREATE INDEX IF NOT EXISTS idx_chunk_hash
                    ON chunks(chunk_hash);",
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    // ------------------------------------------------------------------ files

    pub async fn save_file(&self, metadata: &FileMetadata) -> Result<()> {
        let pool = self.pool.clone();
        let metadata = metadata.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT OR REPLACE INTO files
                 (hash, name, size, mime_type, uploaded_at, chunk_count, compressed)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    metadata.hash,
                    metadata.name,
                    metadata.size as i64,
                    metadata.mime_type,
                    metadata.uploaded_at,
                    metadata.chunk_count,
                    metadata.compressed as i32,
                ],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn get_file(&self, hash: &str) -> Result<FileMetadata> {
        let pool = self.pool.clone();
        let hash = hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT hash, name, size, mime_type, uploaded_at, chunk_count, compressed
                 FROM files WHERE hash = ?1",
            )?;
            let metadata = stmt.query_row(params![hash], |row| {
                Ok(FileMetadata {
                    hash: row.get(0)?,
                    name: row.get(1)?,
                    size: row.get::<_, i64>(2)? as u64,
                    mime_type: row.get(3)?,
                    uploaded_at: row.get(4)?,
                    chunk_count: row.get(5)?,
                    compressed: row.get::<_, i32>(6)? != 0,
                })
            })?;
            Ok::<FileMetadata, anyhow::Error>(metadata)
        })
        .await?
    }

    pub async fn list_files(&self) -> Result<Vec<FileMetadata>> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT hash, name, size, mime_type, uploaded_at, chunk_count, compressed
                 FROM files ORDER BY uploaded_at DESC",
            )?;
            let files = stmt
                .query_map([], |row| {
                    Ok(FileMetadata {
                        hash: row.get(0)?,
                        name: row.get(1)?,
                        size: row.get::<_, i64>(2)? as u64,
                        mime_type: row.get(3)?,
                        uploaded_at: row.get(4)?,
                        chunk_count: row.get(5)?,
                        compressed: row.get::<_, i32>(6)? != 0,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<Vec<FileMetadata>, anyhow::Error>(files)
        })
        .await?
    }

    pub async fn delete_file(&self, hash: &str) -> Result<()> {
        let pool = self.pool.clone();
        let hash = hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute("DELETE FROM files WHERE hash = ?1", params![hash])?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    // ----------------------------------------------------------------- chunks

    /// Persist CDC chunk boundaries for a file in a single transaction.
    pub async fn save_chunks(&self, file_hash: &str, chunks: &[ChunkBoundary]) -> Result<()> {
        let pool = self.pool.clone();
        let file_hash = file_hash.to_string();
        let chunks = chunks.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get()?;
            let tx = conn.transaction()?;
            {
                let mut stmt = tx.prepare(
                    "INSERT OR REPLACE INTO chunks
                     (file_hash, chunk_id, offset, length, chunk_hash)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )?;
                for c in &chunks {
                    stmt.execute(params![
                        file_hash,
                        c.chunk_id,
                        c.offset as i64,
                        c.length,
                        c.hash,
                    ])?;
                }
            }
            tx.commit()?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    /// Return all chunk boundaries for a file ordered by chunk_id.
    pub async fn get_chunks(&self, file_hash: &str) -> Result<Vec<ChunkBoundary>> {
        let pool = self.pool.clone();
        let file_hash = file_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT chunk_id, offset, length, chunk_hash
                 FROM chunks WHERE file_hash = ?1
                 ORDER BY chunk_id",
            )?;
            let chunks = stmt
                .query_map(params![file_hash], |row| {
                    Ok(ChunkBoundary {
                        chunk_id: row.get(0)?,
                        offset: row.get::<_, i64>(1)? as u64,
                        length: row.get(2)?,
                        hash: row.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<Vec<ChunkBoundary>, anyhow::Error>(chunks)
        })
        .await?
    }

    /// Return a single chunk boundary.
    pub async fn get_chunk_boundary(
        &self,
        file_hash: &str,
        chunk_id: u32,
    ) -> Result<ChunkBoundary> {
        let pool = self.pool.clone();
        let file_hash = file_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT chunk_id, offset, length, chunk_hash
                 FROM chunks WHERE file_hash = ?1 AND chunk_id = ?2",
            )?;
            let boundary = stmt.query_row(params![file_hash, chunk_id], |row| {
                Ok(ChunkBoundary {
                    chunk_id: row.get(0)?,
                    offset: row.get::<_, i64>(1)? as u64,
                    length: row.get(2)?,
                    hash: row.get(3)?,
                })
            })?;
            Ok::<ChunkBoundary, anyhow::Error>(boundary)
        })
        .await?
    }
}

