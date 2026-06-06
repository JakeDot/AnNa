use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{
    auth::{CurrentUser, OptionalUser},
    database::Label,
    AppState, ErrorResponse,
};

// ── Request types ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateLabelRequest {
    pub name: String,
    pub color: Option<String>,
}

#[derive(Deserialize)]
pub struct AddFileLabelRequest {
    pub label_id: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// POST /api/labels
pub async fn create_label(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Json(body): Json<CreateLabelRequest>,
) -> Result<(StatusCode, Json<Label>), (StatusCode, Json<ErrorResponse>)> {
    let label = Label {
        id: Uuid::new_v4().to_string(),
        owner_id: user.id.clone(),
        name: body.name,
        color: body.color.unwrap_or_else(|| "#6366f1".to_string()),
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    };
    state.db.create_label(&label).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to create label: {e}") }),
        )
    })?;
    Ok((StatusCode::CREATED, Json(label)))
}

/// GET /api/labels
pub async fn list_labels(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
) -> Result<Json<Vec<Label>>, (StatusCode, Json<ErrorResponse>)> {
    let user_id = user.map(|u| u.id).unwrap_or_default();
    state.db.list_labels_for_user(&user_id).await.map(Json).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to list labels: {e}") }),
        )
    })
}

/// DELETE /api/labels/:id
pub async fn delete_label(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    state.db.delete_label(&id, &user.id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to delete label: {e}") }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /api/files/:hash/labels
/// Applies one of the authenticated user's labels to a file.
pub async fn add_file_label(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Path(hash): Path<String>,
    Json(body): Json<AddFileLabelRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    // Verify the label belongs to the calling user before applying it
    let labels = state.db.list_labels_for_user(&user.id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to verify label ownership: {e}") }),
        )
    })?;
    let owned = labels.iter().any(|l| l.id == body.label_id);
    if !owned {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse { error: "Label not found or does not belong to you".to_string() }),
        ));
    }
    state.db.add_file_label(&hash, &body.label_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to add label to file: {e}") }),
        )
    })?;
    Ok(StatusCode::CREATED)
}

/// DELETE /api/files/:hash/labels/:label_id
/// Removes a label from a file; only the label owner may do so.
pub async fn remove_file_label(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Path((hash, label_id)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    // Verify ownership before removal
    let labels = state.db.list_labels_for_user(&user.id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to verify label ownership: {e}") }),
        )
    })?;
    let owned = labels.iter().any(|l| l.id == label_id);
    if !owned {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse { error: "Label not found or does not belong to you".to_string() }),
        ));
    }
    state.db.remove_file_label(&hash, &label_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to remove label from file: {e}") }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/files/:hash/labels
pub async fn get_file_labels(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<Label>>, (StatusCode, Json<ErrorResponse>)> {
    state.db.get_file_labels(&hash).await.map(Json).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to get file labels: {e}") }),
        )
    })
}
