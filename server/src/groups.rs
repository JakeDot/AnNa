use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{
    auth::{CurrentUser, OptionalUser},
    database::{Group, GroupMember},
    AppState, ErrorResponse,
};

// ── Request types ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Deserialize)]
pub struct AddMemberRequest {
    pub user_id: String,
    pub role: Option<String>,
}

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct GroupWithMembers {
    #[serde(flatten)]
    pub group: Group,
    pub members: Vec<GroupMember>,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// POST /api/groups
pub async fn create_group(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Json(body): Json<CreateGroupRequest>,
) -> Result<(StatusCode, Json<Group>), (StatusCode, Json<ErrorResponse>)> {
    let group = Group {
        id: Uuid::new_v4().to_string(),
        owner_id: user.id.clone(),
        name: body.name,
        description: body.description,
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    };

    state.db.create_group(&group).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to create group: {e}") }),
        )
    })?;

    // Owner is automatically added as an 'owner' member
    state.db.add_group_member(&group.id, &user.id, "owner").await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to add owner as member: {e}") }),
        )
    })?;

    Ok((StatusCode::CREATED, Json(group)))
}

/// GET /api/groups
pub async fn list_groups(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
) -> Result<Json<Vec<Group>>, (StatusCode, Json<ErrorResponse>)> {
    let user_id = user.map(|u| u.id).unwrap_or_default();
    state.db.list_groups_for_user(&user_id).await.map(Json).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to list groups: {e}") }),
        )
    })
}

/// GET /api/groups/:id
pub async fn get_group(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GroupWithMembers>, (StatusCode, Json<ErrorResponse>)> {
    let group = state.db.get_group(&id).await.map_err(|_| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse { error: "Group not found".to_string() }))
    })?;
    let members = state.db.get_group_members(&id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to get members: {e}") }),
        )
    })?;
    Ok(Json(GroupWithMembers { group, members }))
}

/// DELETE /api/groups/:id
pub async fn delete_group(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let group = state.db.get_group(&id).await.map_err(|_| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse { error: "Group not found".to_string() }))
    })?;
    if group.owner_id != user.id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse { error: "Only the group owner can delete this group".to_string() }),
        ));
    }
    state.db.delete_group(&id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to delete group: {e}") }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /api/groups/:id/members
pub async fn add_member(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
    Json(body): Json<AddMemberRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let group = state.db.get_group(&id).await.map_err(|_| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse { error: "Group not found".to_string() }))
    })?;
    if group.owner_id != user.id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse { error: "Only the group owner can add members".to_string() }),
        ));
    }
    let role = body.role.unwrap_or_else(|| "member".to_string());
    state.db.add_group_member(&id, &body.user_id, &role).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to add member: {e}") }),
        )
    })?;
    Ok(StatusCode::CREATED)
}

/// DELETE /api/groups/:id/members/:user_id
pub async fn remove_member(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Path((id, member_id)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let group = state.db.get_group(&id).await.map_err(|_| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse { error: "Group not found".to_string() }))
    })?;
    // Owner can remove anyone; a member can remove themselves
    if group.owner_id != user.id && user.id != member_id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse { error: "Insufficient permissions".to_string() }),
        ));
    }
    state.db.remove_group_member(&id, &member_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Failed to remove member: {e}") }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}
