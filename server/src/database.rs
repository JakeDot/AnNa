use anyhow::Result;
use rusqlite::{params, Connection};
use std::path::Path;
use tokio::fs;

use crate::FileMetadata;

#[derive(Clone)]
pub struct Database {
    db_path: String,
}

impl Database {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        // Ensure directory exists
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).await?;
        }

        Ok(Self {
            db_path: path_str,
        })
    }

    pub async fn init(&self) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS files (
                hash TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                uploaded_at INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL,
                compressed INTEGER NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_uploaded_at ON files(uploaded_at DESC)",
            [],
        )?;

        Ok(())
    }

    pub async fn save_file(&self, metadata: &FileMetadata) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;

        conn.execute(
            "INSERT OR REPLACE INTO files (hash, name, size, mime_type, uploaded_at, chunk_count, compressed)
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

        Ok(())
    }

    pub async fn get_file(&self, hash: &str) -> Result<FileMetadata> {
        let conn = Connection::open(&self.db_path)?;

        let mut stmt = conn.prepare(
            "SELECT hash, name, size, mime_type, uploaded_at, chunk_count, compressed
             FROM files WHERE hash = ?1"
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

        Ok(metadata)
    }

    pub async fn list_files(&self) -> Result<Vec<FileMetadata>> {
        let conn = Connection::open(&self.db_path)?;

        let mut stmt = conn.prepare(
            "SELECT hash, name, size, mime_type, uploaded_at, chunk_count, compressed
             FROM files ORDER BY uploaded_at DESC"
        )?;

        let files = stmt.query_map([], |row| {
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

        Ok(files)
    }

    pub async fn delete_file(&self, hash: &str) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;
        conn.execute("DELETE FROM files WHERE hash = ?1", params![hash])?;
        Ok(())
    }
}
