use anyhow::Result;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

use crate::{cdc::ChunkBoundary, FileMetadata};

// ══════════════════════════════════════════════════════════════════════════════
// Model structs
// ══════════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub github_id: Option<String>,
    pub email: Option<String>,
    pub name: String,
    pub avatar_url: Option<String>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Group {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub description: Option<String>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GroupMember {
    pub group_id: String,
    pub user_id: String,
    pub role: String,
    pub joined_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Label {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub color: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileShare {
    pub file_hash: String,
    pub owner_id: Option<String>,
    /// "public" | "private" | "group"
    pub visibility: String,
    pub group_id: Option<String>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VirtualFolder {
    pub id: String,
    pub owner_id: Option<String>,
    pub name: String,
    pub parent_id: Option<String>,
    pub created_at: i64,
}

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
                // WAL journal mode: readers and the writer never block each other.
                // NORMAL synchronous: WAL checkpoints are still durable; we avoid
                // the overhead of full-fsync on every write.
                // cache_size: 10 000 pages ≈ 40 MB of page cache per connection.
                // busy_timeout 5 000 ms: SQLite retries on lock contention for up
                // to 5 s before returning SQLITE_BUSY, which avoids spurious
                // errors under short write bursts without blocking the OS thread
                // indefinitely (spawn_blocking releases the async thread).
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
                    ON chunks(chunk_hash);

                -- Users (created via OAuth)
                CREATE TABLE IF NOT EXISTS users (
                    id          TEXT    PRIMARY KEY,
                    github_id   TEXT    UNIQUE,
                    email       TEXT,
                    name        TEXT    NOT NULL,
                    avatar_url  TEXT,
                    created_at  INTEGER NOT NULL
                );

                -- Groups owned by users
                CREATE TABLE IF NOT EXISTS groups (
                    id          TEXT    PRIMARY KEY,
                    owner_id    TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name        TEXT    NOT NULL,
                    description TEXT,
                    created_at  INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_groups_owner ON groups(owner_id);

                -- Group membership
                CREATE TABLE IF NOT EXISTS group_members (
                    group_id    TEXT    NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
                    user_id     TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    role        TEXT    NOT NULL DEFAULT 'member',
                    joined_at   INTEGER NOT NULL,
                    PRIMARY KEY (group_id, user_id)
                );
                CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members(user_id);

                -- Labels owned by users
                CREATE TABLE IF NOT EXISTS labels (
                    id          TEXT    PRIMARY KEY,
                    owner_id    TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name        TEXT    NOT NULL,
                    color       TEXT    NOT NULL DEFAULT '#6366f1',
                    created_at  INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_labels_owner ON labels(owner_id);

                -- File ↔ label associations
                CREATE TABLE IF NOT EXISTS file_labels (
                    file_hash   TEXT    NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
                    label_id    TEXT    NOT NULL REFERENCES labels(id) ON DELETE CASCADE,
                    PRIMARY KEY (file_hash, label_id)
                );

                -- Per-file sharing settings
                CREATE TABLE IF NOT EXISTS file_shares (
                    file_hash   TEXT    PRIMARY KEY REFERENCES files(hash) ON DELETE CASCADE,
                    owner_id    TEXT    REFERENCES users(id) ON DELETE SET NULL,
                    visibility  TEXT    NOT NULL DEFAULT 'public',
                    group_id    TEXT    REFERENCES groups(id) ON DELETE SET NULL,
                    created_at  INTEGER NOT NULL
                );

                -- Virtual folders (tree structure)
                CREATE TABLE IF NOT EXISTS virtual_folders (
                    id          TEXT    PRIMARY KEY,
                    owner_id    TEXT    REFERENCES users(id) ON DELETE CASCADE,
                    name        TEXT    NOT NULL,
                    parent_id   TEXT    REFERENCES virtual_folders(id) ON DELETE CASCADE,
                    created_at  INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_folders_owner  ON virtual_folders(owner_id);
                CREATE INDEX IF NOT EXISTS idx_folders_parent ON virtual_folders(parent_id);

                -- File ↔ folder associations
                CREATE TABLE IF NOT EXISTS folder_files (
                    folder_id   TEXT    NOT NULL REFERENCES virtual_folders(id) ON DELETE CASCADE,
                    file_hash   TEXT    NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
                    PRIMARY KEY (folder_id, file_hash)
                );",
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

    /// Delete a file's metadata record from the database.
    ///
    /// Called by the admin delete endpoint (not yet wired in routes).
    #[allow(dead_code)]
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

    // ------------------------------------------------------------------ users

    pub async fn save_user(&self, user: &User) -> Result<()> {
        let pool = self.pool.clone();
        let user = user.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT OR REPLACE INTO users (id, github_id, email, name, avatar_url, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![user.id, user.github_id, user.email, user.name, user.avatar_url, user.created_at],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn get_user_by_id(&self, id: &str) -> Result<User> {
        let pool = self.pool.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT id, github_id, email, name, avatar_url, created_at FROM users WHERE id = ?1",
            )?;
            let user = stmt.query_row(params![id], |row| {
                Ok(User {
                    id: row.get(0)?,
                    github_id: row.get(1)?,
                    email: row.get(2)?,
                    name: row.get(3)?,
                    avatar_url: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })?;
            Ok::<User, anyhow::Error>(user)
        })
        .await?
    }

    pub async fn get_user_by_github_id(&self, github_id: &str) -> Result<Option<User>> {
        let pool = self.pool.clone();
        let github_id = github_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT id, github_id, email, name, avatar_url, created_at FROM users WHERE github_id = ?1",
            )?;
            match stmt.query_row(params![github_id], |row| {
                Ok(User {
                    id: row.get(0)?,
                    github_id: row.get(1)?,
                    email: row.get(2)?,
                    name: row.get(3)?,
                    avatar_url: row.get(4)?,
                    created_at: row.get(5)?,
                })
            }) {
                Ok(user) => Ok(Some(user)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(anyhow::Error::from(e)),
            }
        })
        .await?
    }

    // ----------------------------------------------------------------- groups

    pub async fn create_group(&self, group: &Group) -> Result<()> {
        let pool = self.pool.clone();
        let group = group.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT INTO groups (id, owner_id, name, description, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![group.id, group.owner_id, group.name, group.description, group.created_at],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn get_group(&self, id: &str) -> Result<Group> {
        let pool = self.pool.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT id, owner_id, name, description, created_at FROM groups WHERE id = ?1",
            )?;
            let group = stmt.query_row(params![id], |row| {
                Ok(Group {
                    id: row.get(0)?,
                    owner_id: row.get(1)?,
                    name: row.get(2)?,
                    description: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })?;
            Ok::<Group, anyhow::Error>(group)
        })
        .await?
    }

    /// Returns groups where the user is owner or member.
    pub async fn list_groups_for_user(&self, user_id: &str) -> Result<Vec<Group>> {
        let pool = self.pool.clone();
        let user_id = user_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT DISTINCT g.id, g.owner_id, g.name, g.description, g.created_at
                 FROM groups g
                 LEFT JOIN group_members gm ON g.id = gm.group_id
                 WHERE g.owner_id = ?1 OR gm.user_id = ?1
                 ORDER BY g.created_at DESC",
            )?;
            let groups = stmt
                .query_map(params![user_id], |row| {
                    Ok(Group {
                        id: row.get(0)?,
                        owner_id: row.get(1)?,
                        name: row.get(2)?,
                        description: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<Vec<Group>, anyhow::Error>(groups)
        })
        .await?
    }

    pub async fn delete_group(&self, id: &str) -> Result<()> {
        let pool = self.pool.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute("DELETE FROM groups WHERE id = ?1", params![id])?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn add_group_member(&self, group_id: &str, user_id: &str, role: &str) -> Result<()> {
        let pool = self.pool.clone();
        let (group_id, user_id, role) = (group_id.to_string(), user_id.to_string(), role.to_string());
        let joined_at = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT OR REPLACE INTO group_members (group_id, user_id, role, joined_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![group_id, user_id, role, joined_at],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn get_group_members(&self, group_id: &str) -> Result<Vec<GroupMember>> {
        let pool = self.pool.clone();
        let group_id = group_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT group_id, user_id, role, joined_at FROM group_members WHERE group_id = ?1",
            )?;
            let members = stmt
                .query_map(params![group_id], |row| {
                    Ok(GroupMember {
                        group_id: row.get(0)?,
                        user_id: row.get(1)?,
                        role: row.get(2)?,
                        joined_at: row.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<Vec<GroupMember>, anyhow::Error>(members)
        })
        .await?
    }

    pub async fn remove_group_member(&self, group_id: &str, user_id: &str) -> Result<()> {
        let pool = self.pool.clone();
        let (group_id, user_id) = (group_id.to_string(), user_id.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "DELETE FROM group_members WHERE group_id = ?1 AND user_id = ?2",
                params![group_id, user_id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn is_group_member(&self, group_id: &str, user_id: &str) -> Result<bool> {
        let pool = self.pool.clone();
        let (group_id, user_id) = (group_id.to_string(), user_id.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM group_members WHERE group_id = ?1 AND user_id = ?2",
                params![group_id, user_id],
                |row| row.get(0),
            )?;
            Ok::<bool, anyhow::Error>(count > 0)
        })
        .await?
    }

    // ----------------------------------------------------------------- labels

    pub async fn create_label(&self, label: &Label) -> Result<()> {
        let pool = self.pool.clone();
        let label = label.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT INTO labels (id, owner_id, name, color, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![label.id, label.owner_id, label.name, label.color, label.created_at],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn list_labels_for_user(&self, user_id: &str) -> Result<Vec<Label>> {
        let pool = self.pool.clone();
        let user_id = user_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT id, owner_id, name, color, created_at FROM labels WHERE owner_id = ?1 ORDER BY created_at DESC",
            )?;
            let labels = stmt
                .query_map(params![user_id], |row| {
                    Ok(Label {
                        id: row.get(0)?,
                        owner_id: row.get(1)?,
                        name: row.get(2)?,
                        color: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<Vec<Label>, anyhow::Error>(labels)
        })
        .await?
    }

    pub async fn delete_label(&self, id: &str, owner_id: &str) -> Result<()> {
        let pool = self.pool.clone();
        let (id, owner_id) = (id.to_string(), owner_id.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "DELETE FROM labels WHERE id = ?1 AND owner_id = ?2",
                params![id, owner_id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn add_file_label(&self, file_hash: &str, label_id: &str) -> Result<()> {
        let pool = self.pool.clone();
        let (file_hash, label_id) = (file_hash.to_string(), label_id.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT OR IGNORE INTO file_labels (file_hash, label_id) VALUES (?1, ?2)",
                params![file_hash, label_id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn remove_file_label(&self, file_hash: &str, label_id: &str) -> Result<()> {
        let pool = self.pool.clone();
        let (file_hash, label_id) = (file_hash.to_string(), label_id.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "DELETE FROM file_labels WHERE file_hash = ?1 AND label_id = ?2",
                params![file_hash, label_id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn get_file_labels(&self, file_hash: &str) -> Result<Vec<Label>> {
        let pool = self.pool.clone();
        let file_hash = file_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT l.id, l.owner_id, l.name, l.color, l.created_at
                 FROM labels l
                 JOIN file_labels fl ON l.id = fl.label_id
                 WHERE fl.file_hash = ?1",
            )?;
            let labels = stmt
                .query_map(params![file_hash], |row| {
                    Ok(Label {
                        id: row.get(0)?,
                        owner_id: row.get(1)?,
                        name: row.get(2)?,
                        color: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<Vec<Label>, anyhow::Error>(labels)
        })
        .await?
    }

    // --------------------------------------------------------------- file shares

    pub async fn set_file_share(&self, share: &FileShare) -> Result<()> {
        let pool = self.pool.clone();
        let share = share.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT OR REPLACE INTO file_shares (file_hash, owner_id, visibility, group_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![share.file_hash, share.owner_id, share.visibility, share.group_id, share.created_at],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn get_file_share(&self, file_hash: &str) -> Result<Option<FileShare>> {
        let pool = self.pool.clone();
        let file_hash = file_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT file_hash, owner_id, visibility, group_id, created_at
                 FROM file_shares WHERE file_hash = ?1",
            )?;
            match stmt.query_row(params![file_hash], |row| {
                Ok(FileShare {
                    file_hash: row.get(0)?,
                    owner_id: row.get(1)?,
                    visibility: row.get(2)?,
                    group_id: row.get(3)?,
                    created_at: row.get(4)?,
                })
            }) {
                Ok(share) => Ok(Some(share)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(anyhow::Error::from(e)),
            }
        })
        .await?
    }

    // ------------------------------------------------------------- virtual folders

    pub async fn create_folder(&self, folder: &VirtualFolder) -> Result<()> {
        let pool = self.pool.clone();
        let folder = folder.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT INTO virtual_folders (id, owner_id, name, parent_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![folder.id, folder.owner_id, folder.name, folder.parent_id, folder.created_at],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn get_folder(&self, id: &str) -> Result<VirtualFolder> {
        let pool = self.pool.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT id, owner_id, name, parent_id, created_at FROM virtual_folders WHERE id = ?1",
            )?;
            let folder = stmt.query_row(params![id], |row| {
                Ok(VirtualFolder {
                    id: row.get(0)?,
                    owner_id: row.get(1)?,
                    name: row.get(2)?,
                    parent_id: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })?;
            Ok::<VirtualFolder, anyhow::Error>(folder)
        })
        .await?
    }

    /// List folders, optionally filtered by owner and/or parent.
    /// - `parent_id = None`  → root folders (parent_id IS NULL)
    /// - `parent_id = Some`  → subfolders of that parent
    pub async fn list_folders(
        &self,
        owner_id: Option<&str>,
        parent_id: Option<&str>,
    ) -> Result<Vec<VirtualFolder>> {
        let pool = self.pool.clone();
        let owner_id = owner_id.map(str::to_string);
        let parent_id = parent_id.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let rows_to_folder = |row: &rusqlite::Row<'_>| -> rusqlite::Result<VirtualFolder> {
                Ok(VirtualFolder {
                    id: row.get(0)?,
                    owner_id: row.get(1)?,
                    name: row.get(2)?,
                    parent_id: row.get(3)?,
                    created_at: row.get(4)?,
                })
            };
            let folders: Vec<VirtualFolder> = match (owner_id.as_deref(), parent_id.as_deref()) {
                (Some(oid), Some(pid)) => {
                    let mut stmt = conn.prepare(
                        "SELECT id, owner_id, name, parent_id, created_at
                         FROM virtual_folders WHERE owner_id = ?1 AND parent_id = ?2
                         ORDER BY name",
                    )?;
                    let result = stmt.query_map(params![oid, pid], rows_to_folder)?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    result
                }
                (Some(oid), None) => {
                    let mut stmt = conn.prepare(
                        "SELECT id, owner_id, name, parent_id, created_at
                         FROM virtual_folders WHERE owner_id = ?1 AND parent_id IS NULL
                         ORDER BY name",
                    )?;
                    let result = stmt.query_map(params![oid], rows_to_folder)?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    result
                }
                (None, Some(pid)) => {
                    let mut stmt = conn.prepare(
                        "SELECT id, owner_id, name, parent_id, created_at
                         FROM virtual_folders WHERE parent_id = ?1
                         ORDER BY name",
                    )?;
                    let result = stmt.query_map(params![pid], rows_to_folder)?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    result
                }
                (None, None) => {
                    let mut stmt = conn.prepare(
                        "SELECT id, owner_id, name, parent_id, created_at
                         FROM virtual_folders WHERE parent_id IS NULL
                         ORDER BY name",
                    )?;
                    let result = stmt.query_map([], rows_to_folder)?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    result
                }
            };
            Ok::<Vec<VirtualFolder>, anyhow::Error>(folders)
        })
        .await?
    }

    pub async fn add_file_to_folder(&self, folder_id: &str, file_hash: &str) -> Result<()> {
        let pool = self.pool.clone();
        let (folder_id, file_hash) = (folder_id.to_string(), file_hash.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "INSERT OR IGNORE INTO folder_files (folder_id, file_hash) VALUES (?1, ?2)",
                params![folder_id, file_hash],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn list_files_in_folder(&self, folder_id: &str) -> Result<Vec<String>> {
        let pool = self.pool.clone();
        let folder_id = folder_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            let mut stmt = conn.prepare(
                "SELECT file_hash FROM folder_files WHERE folder_id = ?1",
            )?;
            let hashes = stmt
                .query_map(params![folder_id], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<String>>>()?;
            Ok::<Vec<String>, anyhow::Error>(hashes)
        })
        .await?
    }

    pub async fn remove_file_from_folder(&self, folder_id: &str, file_hash: &str) -> Result<()> {
        let pool = self.pool.clone();
        let (folder_id, file_hash) = (folder_id.to_string(), file_hash.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = pool.get()?;
            conn.execute(
                "DELETE FROM folder_files WHERE folder_id = ?1 AND file_hash = ?2",
                params![folder_id, file_hash],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}

