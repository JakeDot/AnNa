use axum::{
    async_trait,
    extract::{FromRequestParts, Query, State},
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Json, Redirect},
};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{database::User, AppState, ErrorResponse};

// ── JWT ───────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// User ID
    pub sub: String,
    pub name: String,
    pub exp: usize,
}

fn jwt_secret() -> String {
    std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "default-dev-secret-change-in-production-please".to_string())
}

pub fn create_jwt(user_id: &str, name: &str) -> Result<String, jsonwebtoken::errors::Error> {
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
        + 30 * 24 * 3600; // 30 days
    let claims = Claims { sub: user_id.to_string(), name: name.to_string(), exp };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(jwt_secret().as_bytes()),
    )
}

pub fn decode_jwt(token: &str) -> Option<Claims> {
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(jwt_secret().as_bytes()),
        &Validation::default(),
    )
    .ok()
    .map(|d| d.claims)
}

// ── Extractors ────────────────────────────────────────────────────────────────

/// Axum extractor: authenticated user (returns 401 if token is missing/invalid).
pub struct CurrentUser(pub User);

/// Axum extractor: optionally authenticated user (never fails).
pub struct OptionalUser(pub Option<User>);

fn extract_bearer_token(parts: &Parts) -> Option<String> {
    // Authorization: Bearer <token>
    if let Some(auth) = parts.headers.get("authorization") {
        if let Ok(s) = auth.to_str() {
            if let Some(token) = s.strip_prefix("Bearer ") {
                return Some(token.to_string());
            }
        }
    }
    // X-Auth-Token fallback
    if let Some(tok) = parts.headers.get("x-auth-token") {
        if let Ok(s) = tok.to_str() {
            return Some(s.to_string());
        }
    }
    None
}

#[async_trait]
impl FromRequestParts<AppState> for CurrentUser {
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let token = extract_bearer_token(parts).ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse { error: "Missing authentication token".to_string() }),
            )
        })?;
        let claims = decode_jwt(&token).ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse { error: "Invalid or expired token".to_string() }),
            )
        })?;
        let user = state.db.get_user_by_id(&claims.sub).await.map_err(|_| {
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse { error: "User not found".to_string() }),
            )
        })?;
        Ok(CurrentUser(user))
    }
}

#[async_trait]
impl FromRequestParts<AppState> for OptionalUser {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        if let Some(token) = extract_bearer_token(parts) {
            if let Some(claims) = decode_jwt(&token) {
                if let Ok(user) = state.db.get_user_by_id(&claims.sub).await {
                    return Ok(OptionalUser(Some(user)));
                }
            }
        }
        Ok(OptionalUser(None))
    }
}

// ── OAuth structs ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct OAuthCallback {
    pub code: String,
}

#[derive(Deserialize)]
struct GitHubTokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct GitHubUser {
    id: i64,
    login: String,
    name: Option<String>,
    email: Option<String>,
    avatar_url: Option<String>,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// GET /api/auth/github → redirect to GitHub OAuth consent page.
pub async fn github_login() -> impl IntoResponse {
    let client_id = std::env::var("GITHUB_CLIENT_ID").unwrap_or_default();
    let redirect_url = format!(
        "https://github.com/login/oauth/authorize?client_id={}&scope=read:user,user:email",
        client_id
    );
    Redirect::temporary(&redirect_url)
}

/// GET /api/auth/github/callback?code=...
/// Exchange the code for an access token, resolve/create the user, issue JWT,
/// and redirect the browser back to the frontend with the token in the query
/// string so the React app can store it.
pub async fn github_callback(
    State(state): State<AppState>,
    Query(params): Query<OAuthCallback>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let client_id = std::env::var("GITHUB_CLIENT_ID").unwrap_or_default();
    let client_secret = std::env::var("GITHUB_CLIENT_SECRET").unwrap_or_default();
    let app_url =
        std::env::var("APP_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());

    let http = reqwest::Client::new();

    // Exchange code → access token
    let token_resp: GitHubTokenResponse = http
        .post("https://github.com/login/oauth/access_token")
        .header("Accept", "application/json")
        .json(&serde_json::json!({
            "client_id":     client_id,
            "client_secret": client_secret,
            "code":          params.code,
        }))
        .send()
        .await
        .map_err(|e| {
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: format!("GitHub token exchange failed: {e}") }))
        })?
        .json()
        .await
        .map_err(|e| {
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: format!("GitHub token response parse error: {e}") }))
        })?;

    // Fetch GitHub user profile
    let gh_user: GitHubUser = http
        .get("https://api.github.com/user")
        .header("Authorization", format!("Bearer {}", token_resp.access_token))
        .header("User-Agent", "anna-sync/1.0")
        .send()
        .await
        .map_err(|e| {
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: format!("GitHub user fetch failed: {e}") }))
        })?
        .json()
        .await
        .map_err(|e| {
            (StatusCode::BAD_GATEWAY, Json(ErrorResponse { error: format!("GitHub user parse error: {e}") }))
        })?;

    let github_id = gh_user.id.to_string();
    let name = gh_user.name.unwrap_or_else(|| gh_user.login.clone());

    // Find or create the user record
    let user = match state
        .db
        .get_user_by_github_id(&github_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB error: {e}") })))?
    {
        Some(existing) => existing,
        None => {
            let new_user = User {
                id: Uuid::new_v4().to_string(),
                github_id: Some(github_id.clone()),
                email: gh_user.email.clone(),
                name: name.clone(),
                avatar_url: gh_user.avatar_url.clone(),
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
            };
            state.db.save_user(&new_user).await.map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Failed to save user: {e}") }))
            })?;
            new_user
        }
    };

    let jwt = create_jwt(&user.id, &user.name).map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Failed to create JWT: {e}") }))
    })?;

    // Redirect the browser back to the frontend, carrying the token
    Ok(Redirect::temporary(&format!("{}/?token={}", app_url, jwt)))
}

/// GET /api/auth/me → returns the currently authenticated user.
pub async fn get_me(CurrentUser(user): CurrentUser) -> Json<User> {
    Json(user)
}

/// POST /api/auth/logout → client-side only; just acknowledge.
pub async fn logout() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}
