#[cfg(test)]
mod test;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::server::AppState;

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    docker: DockerStatus,
}

#[derive(Debug, Serialize)]
struct DockerStatus {
    connected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// GET /health — basic health check including Docker connectivity.
pub(crate) async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let docker_status = match state.docker.ping().await {
        Ok(_) => DockerStatus {
            connected: true,
            error: None,
        },
        Err(e) => DockerStatus {
            connected: false,
            error: Some(e.to_string()),
        },
    };

    let status = if docker_status.connected {
        "healthy"
    } else {
        "degraded"
    };

    let response = HealthResponse {
        status,
        docker: docker_status,
    };

    (StatusCode::OK, Json(response))
}

/// GET /metrics — return per-function invocation metrics.
pub(crate) async fn get_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let snapshot = state.metrics.snapshot();
    (StatusCode::OK, Json(serde_json::json!({ "functions": snapshot })))
}

/// Returns a router with health and metrics routes.
pub fn health_routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(get_metrics))
}
