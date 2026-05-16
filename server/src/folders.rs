use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{
    auth::OptionalUser,
    database::VirtualFolder,
    AppState, ErrorResponse,
};

// ── Request / response types ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateFolderRequest {
    pub name: String,
    pub parent_id: Option<String>,
}

#[derive(Deserialize)]
pub struct AddFileRequest {
    pub file_hash: String,
}

#[derive(Deserialize)]
pub struct ListFoldersQuery {
    pub parent_id: Option<String>,
}

#[derive(Serialize)]
pub struct FolderContents {
    pub folder: VirtualFolder,
    pub subfolders: Vec<VirtualFolder>,
    pub file_hashes: Vec<String>,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// POST /api/folders
pub async fn create_folder(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
    Json(body): Json<CreateFolderRequest>,
) -> Result<(StatusCode, Json<VirtualFolder>), (StatusCode, Json<ErrorResponse>)> {
    let folder = VirtualFolder {
        id: Uuid::new_v4().to_string(),
        owner_id: user.map(|u| u.id),
        name: body.name,
        parent_id: body.parent_id,
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    };
    state.db.create_folder(&folder).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to create folder: {e}") }),
        )
    })?;
    Ok((StatusCode::CREATED, Json(folder)))
}

/// GET /api/folders[?parent_id=...]
pub async fn list_folders(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
    Query(query): Query<ListFoldersQuery>,
) -> Result<Json<Vec<VirtualFolder>>, (StatusCode, Json<ErrorResponse>)> {
    // Show folders owned by this user (or unowned folders if no auth)
    let owner_id_str;
    let owner_id: Option<&str> = match &user {
        Some(u) => {
            owner_id_str = u.id.clone();
            Some(&owner_id_str)
        }
        None => None,
    };
    let parent_id = query.parent_id.as_deref();
    state.db.list_folders(owner_id, parent_id).await.map(Json).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to list folders: {e}") }),
        )
    })
}

/// GET /api/folders/:id
pub async fn get_folder_contents(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<FolderContents>, (StatusCode, Json<ErrorResponse>)> {
    let folder = state.db.get_folder(&id).await.map_err(|_| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse { error: "Folder not found".to_string() }))
    })?;
    let subfolders = state.db.list_folders(None, Some(&id)).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to list subfolders: {e}") }),
        )
    })?;
    let file_hashes = state.db.list_files_in_folder(&id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to list files in folder: {e}") }),
        )
    })?;
    Ok(Json(FolderContents { folder, subfolders, file_hashes }))
}

/// POST /api/folders/:id/files
pub async fn add_file_to_folder(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<AddFileRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    state.db.add_file_to_folder(&id, &body.file_hash).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to add file to folder: {e}") }),
        )
    })?;
    Ok(StatusCode::CREATED)
}
