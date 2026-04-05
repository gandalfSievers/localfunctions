use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use bollard::Docker;
use tokio::net::TcpListener;
use tracing::info;

use crate::api;
use crate::config::Config;
use crate::function::FunctionsConfig;

/// Shared application state accessible by all route handlers.
#[derive(Clone)]
#[allow(dead_code)]
pub struct AppState {
    pub config: Arc<Config>,
    pub docker: Docker,
    pub functions: Arc<FunctionsConfig>,
}

/// Create the external Invoke API router with a 6 MB request body limit.
pub fn invoke_router(state: AppState) -> Router {
    api::invoke_routes()
        .layer(axum::extract::DefaultBodyLimit::max(6 * 1024 * 1024))
        .with_state(state)
}

/// Create the Runtime API router used by Lambda containers.
pub fn runtime_router(state: AppState) -> Router {
    api::runtime_routes().with_state(state)
}

/// Start both the Invoke API and Runtime API servers.
///
/// Runs until a `SIGINT` or `SIGTERM` signal is received, then performs
/// graceful shutdown.
pub async fn start(state: AppState) -> anyhow::Result<()> {
    let invoke_addr = SocketAddr::new(state.config.host, state.config.port);
    let runtime_addr = SocketAddr::new(state.config.host, state.config.runtime_port);

    let invoke_app = invoke_router(state.clone());
    let runtime_app = runtime_router(state);

    let invoke_listener = TcpListener::bind(invoke_addr).await?;
    let runtime_listener = TcpListener::bind(runtime_addr).await?;

    info!(%invoke_addr, "Invoke API listening");
    info!(%runtime_addr, "Runtime API listening");

    let invoke_server =
        axum::serve(invoke_listener, invoke_app).with_graceful_shutdown(shutdown_signal());
    let runtime_server =
        axum::serve(runtime_listener, runtime_app).with_graceful_shutdown(shutdown_signal());

    tokio::try_join!(
        async { invoke_server.await.map_err(anyhow::Error::from) },
        async { runtime_server.await.map_err(anyhow::Error::from) },
    )?;

    Ok(())
}

/// Wait for SIGINT (Ctrl-C) or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
    info!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::Request;
    use std::collections::HashMap;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        let config = Config {
            host: "127.0.0.1".parse().unwrap(),
            port: 9600,
            runtime_port: 9601,
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            functions_file: "./functions.json".into(),
            log_level: "info".into(),
            shutdown_timeout: 30,
            container_idle_timeout: 300,
            max_containers: 20,
            docker_network: "localfunctions".into(),
        };
        let docker = Docker::connect_with_local_defaults().unwrap();
        let functions = FunctionsConfig {
            functions: HashMap::new(),
            runtime_images: HashMap::new(),
        };
        AppState {
            config: Arc::new(config),
            docker,
            functions: Arc::new(functions),
        }
    }

    #[tokio::test]
    async fn invoke_router_returns_404_for_unknown_route() {
        let app = invoke_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn runtime_router_returns_404_for_unknown_route() {
        let app = runtime_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn invoke_router_has_health_endpoint() {
        let app = invoke_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    #[tokio::test]
    async fn invoke_router_rejects_oversized_body() {
        let app = invoke_router(test_state());
        let big_body = vec![0u8; 7 * 1024 * 1024]; // 7 MB
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/2015-03-31/functions/test-fn/invocations")
                    .body(Body::from(big_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn runtime_router_has_health_endpoint() {
        let app = runtime_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    #[tokio::test]
    async fn app_state_is_clone() {
        let state = test_state();
        let _cloned = state.clone();
    }
}
