use anyhow::Result;
use dashmap::DashMap;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

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

    pub async fn save_file(&self, hash: &str, data: &[u8]) -> Result<()> {
        let file_path = self.get_file_path(hash);

        // Create subdirectory based on first 2 chars of hash (sharding)
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Use atomic write pattern: write to temp file, then rename
        let temp_name = format!("{}.tmp.{}", hash, Uuid::new_v4());
        let temp_path = if let Some(parent) = file_path.parent() {
            parent.join(temp_name)
        } else {
            PathBuf::from(temp_name)
        };

        // Try to use create_new to detect if file already exists
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file_path)
            .await
        {
            Ok(mut file) => {
                // File didn't exist, write directly
                file.write_all(data).await?;
                file.sync_all().await?;
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // File already exists, this is OK (deduplication)
                return Ok(());
            }
            Err(_) => {
                // Other error, fall through to atomic write
            }
        }

        // Write to temp file first
        let mut temp_file = File::create(&temp_path).await?;
        temp_file.write_all(data).await?;
        temp_file.sync_all().await?;
        drop(temp_file);

        // Atomically rename temp file to final destination
        // If file already exists at this point, rename will fail on some systems
        // or overwrite on others, but the data will be the same due to content addressing
        match fs::rename(&temp_path, &file_path).await {
            Ok(_) => Ok(()),
            Err(_) => {
                // Clean up temp file if rename failed
                let _ = fs::remove_file(&temp_path).await;
                Ok(())
            }
        }
    }

    pub async fn read_file(&self, hash: &str) -> Result<Vec<u8>> {
        let file_path = self.get_file_path(hash);
        let mut file = File::open(&file_path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        Ok(data)
    }

    pub async fn file_exists(&self, hash: &str) -> bool {
        let file_path = self.get_file_path(hash);
        tokio::fs::try_exists(&file_path).await.unwrap_or(false)
    }

    pub async fn delete_file(&self, hash: &str) -> Result<()> {
        let file_path = self.get_file_path(hash);
        fs::remove_file(&file_path).await?;
        Ok(())
    }

    fn get_file_path(&self, hash: &str) -> PathBuf {
        // Shard files into subdirectories based on first 2 characters
        if hash.len() >= 2 {
            let subdir = &hash[0..2];
            self.base_path.join(subdir).join(hash)
        } else {
            self.base_path.join(hash)
        }
    }
}

/// Tracks which chunks are available from which peers
pub struct ChunkTracker {
    // hash -> chunk_id -> set of peer IDs
    chunks: DashMap<String, DashMap<u32, HashSet<String>>>,
}

impl ChunkTracker {
    pub fn new() -> Self {
        Self {
            chunks: DashMap::new(),
        }
    }

    pub fn add_chunk(&self, file_hash: &str, chunk_id: u32) {
        self.chunks
            .entry(file_hash.to_string())
            .or_insert_with(DashMap::new)
            .insert(chunk_id, HashSet::new());
    }

    pub fn add_peer_chunk(&self, file_hash: &str, chunk_id: u32, peer_id: String) {
        self.chunks
            .entry(file_hash.to_string())
            .or_insert_with(DashMap::new)
            .entry(chunk_id)
            .or_insert_with(HashSet::new)
            .insert(peer_id);
    }

    pub fn remove_peer(&self, peer_id: &str) {
        for file_entry in self.chunks.iter() {
            for mut chunk_entry in file_entry.value().iter_mut() {
                chunk_entry.value_mut().remove(peer_id);
            }
        }
    }

    pub fn get_available_chunks(&self, file_hash: &str) -> Vec<u32> {
        if let Some(file_chunks) = self.chunks.get(file_hash) {
            file_chunks
                .iter()
                .map(|entry| *entry.key())
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn get_peers_for_chunk(&self, file_hash: &str, chunk_id: u32) -> Vec<String> {
        if let Some(file_chunks) = self.chunks.get(file_hash) {
            if let Some(peers) = file_chunks.get(&chunk_id) {
                return peers.value().iter().cloned().collect();
            }
        }
        Vec::new()
    }
}
