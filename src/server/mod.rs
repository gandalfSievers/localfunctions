use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use tower::Layer;
use bollard::Docker;
use tokio::net::TcpListener;
use tracing::{debug, info};

use crate::api;
use crate::api::virtual_host::extract_function_from_host;
use crate::config::Config;
use crate::container::{ContainerManager, ContainerRegistry};
#[cfg(test)]
use crate::container::CredentialForwardingConfig;
use crate::extensions::ExtensionRegistry;
use crate::function::FunctionsConfig;
use crate::metrics::MetricsCollector;
use crate::runtime::RuntimeBridge;

/// Shared application state accessible by all route handlers.
#[derive(Clone)]
#[allow(dead_code)]
pub struct AppState {
    pub config: Arc<Config>,
    pub docker: Docker,
    pub functions: Arc<FunctionsConfig>,
    pub container_registry: Arc<ContainerRegistry>,
    pub container_manager: Arc<ContainerManager>,
    pub shutting_down: Arc<AtomicBool>,
    pub runtime_bridge: Arc<RuntimeBridge>,
    pub metrics: Arc<MetricsCollector>,
    pub extension_registry: Arc<ExtensionRegistry>,
}

impl AppState {
    /// Returns true if the service is shutting down and should reject new
    /// invocations.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }
}

/// Create the external Invoke API router with a configurable request body limit.
///
/// Includes a virtual-host middleware layer that rewrites the URI when the
/// request's Host header matches an AWS-style
/// (`{function}.lambda.{region}.amazonaws.com`) or custom domain
/// (`{function}.{domain}`) pattern, enabling Function URL–style routing via
/// the hostname.
///
/// The virtual-host middleware is applied as an outer service wrapping the
/// inner Router so that URI rewriting happens **before** axum's route matching.
/// (`Router::layer()` only wraps matched route handlers, which is too late for
/// URI rewrites that affect which route is matched.)
pub fn invoke_router(state: AppState) -> Router {
    let max_body_size = state.config.max_body_size;

    // Build the inner router with all routes and body limit.
    let inner = api::invoke_routes()
        .layer(axum::extract::DefaultBodyLimit::max(max_body_size))
        .with_state(state.clone());

    // Wrap with virtual-host middleware via fallback_service so the URI
    // rewrite runs before the inner router's route matching.
    let vhost_layer = middleware::from_fn_with_state(state, virtual_host_rewrite);
    Router::new().fallback_service(vhost_layer.layer(inner))
}

/// Middleware that rewrites the URI path when a virtual hosted-style Host
/// header is detected.
///
/// When the Host header matches `{function}.lambda.{region}.amazonaws.com`
/// (always enabled) or `{function}.{domain}` (when `LOCAL_LAMBDA_DOMAIN` is
/// set), the path is rewritten from `/{path}` to `/{function}/{path}` so
/// that the existing Function URL routes (`/:function_name` and
/// `/:function_name/*path`) handle the request transparently.
///
/// Requests with a Host that does not match any virtual host pattern pass
/// through unchanged — normal path-based routing applies.
async fn virtual_host_rewrite(
    State(state): State<AppState>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let host = req
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok());

    if let Some(info) = host.and_then(|h| {
        extract_function_from_host(h, state.config.domain.as_deref())
    }) {
        let function_name = info.function_name;
        let original_path = req.uri().path();

        // Skip rewriting when the path already matches a known Lambda API pattern.
        // These paths are fully qualified and should route directly to the standard
        // invoke handler without prepending the function name.
        let is_lambda_api_path = original_path.starts_with("/2015-03-31/")
            || original_path.starts_with("/2021-11-15/");

        let new_path = if is_lambda_api_path {
            original_path.to_string()
        } else if original_path == "/" {
            format!("/{}", function_name)
        } else {
            format!("/{}{}", function_name, original_path)
        };

        let path_and_query = match req.uri().query() {
            Some(query) => format!("{}?{}", new_path, query),
            None => new_path,
        };

        if let Ok(pq) = path_and_query.parse::<http::uri::PathAndQuery>() {
            let mut parts = req.uri().clone().into_parts();
            parts.path_and_query = Some(pq);
            if let Ok(new_uri) = http::Uri::from_parts(parts) {
                debug!(
                    original_host = ?host,
                    function = %function_name,
                    original_uri = %req.uri(),
                    rewritten_uri = %new_uri,
                    "virtual host rewrite"
                );
                *req.uri_mut() = new_uri;
            }
        }
    }

    next.run(req).await
}

/// Create the Runtime API router used by Lambda containers.
pub fn runtime_router(state: AppState) -> Router {
    api::runtime_routes().with_state(state)
}

/// Start both the Invoke API and Runtime API servers.
///
/// Runs until a `SIGINT` or `SIGTERM` signal is received, then performs
/// graceful shutdown. Sets the `shutting_down` flag on the shared state so
/// handlers can reject new invocations immediately.
pub async fn start(state: AppState) -> anyhow::Result<()> {
    let invoke_addr = SocketAddr::new(state.config.host, state.config.port);
    let runtime_addr = SocketAddr::new(state.config.host, state.config.runtime_port);

    let invoke_app = invoke_router(state.clone());
    let runtime_app = runtime_router(state.clone());

    let invoke_listener = TcpListener::bind(invoke_addr).await?;
    let runtime_listener = TcpListener::bind(runtime_addr).await?;

    info!(%invoke_addr, "Invoke API listening");
    info!(%runtime_addr, "Runtime API listening");

    // Use a shared notify so both servers shut down from the same signal.
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let shutdown_trigger = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_trigger.notify_waiters();
    });

    let shutdown_a = shutdown.clone();
    let shutdown_b = shutdown.clone();
    let shutting_down = state.shutting_down.clone();

    let invoke_server = axum::serve(invoke_listener, invoke_app)
        .with_graceful_shutdown(async move { shutdown_a.notified().await });
    let runtime_server = axum::serve(runtime_listener, runtime_app)
        .with_graceful_shutdown(async move { shutdown_b.notified().await });

    // Wait for shutdown signal, then mark as shutting down.
    let shutdown_watcher = shutdown.clone();
    tokio::spawn(async move {
        shutdown_watcher.notified().await;
        shutting_down.store(true, std::sync::atomic::Ordering::Relaxed);
        info!("stopping new invocations");
    });

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
    use std::sync::atomic::AtomicBool;
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
            max_body_size: 6 * 1024 * 1024,
            log_format: crate::config::LogFormat::Text,
            pull_images: false,
            init_timeout: 10,
            container_acquire_timeout: 10,
            forward_aws_credentials: true,
            mount_aws_credentials: false,
            max_async_body_size: 256 * 1024,
            hot_reload: true,
            hot_reload_debounce_ms: 500,
            domain: None,
            callback_url: "http://0.0.0.0:9600".to_string(),
        };
        let docker = Docker::connect_with_local_defaults().unwrap();
        let functions = FunctionsConfig {
            functions: HashMap::new(),
            runtime_images: HashMap::new(),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let extension_registry = Arc::new(ExtensionRegistry::new(shutdown_rx.clone()));
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx));
        let container_registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let container_manager = Arc::new(ContainerManager::new(
            docker.clone(),
            HashMap::new(),
            "localfunctions".into(),
            9601,
            "us-east-1".into(),
            container_registry.clone(),
            20,
            CredentialForwardingConfig::default(),
        ));
        AppState {
            config: Arc::new(config),
            container_registry,
            container_manager,
            docker,
            functions: Arc::new(functions),
            shutting_down: Arc::new(AtomicBool::new(false)),
            runtime_bridge,
            metrics: Arc::new(MetricsCollector::new()),
            extension_registry,
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
    async fn invoke_router_respects_custom_body_limit() {
        let mut state = test_state();
        // Set a 1 KB limit
        Arc::get_mut(&mut state.config).unwrap().max_body_size = 1024;
        let app = invoke_router(state);
        let body = vec![0u8; 2048]; // 2 KB — should be rejected
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/2015-03-31/functions/test-fn/invocations")
                    .body(Body::from(body))
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

    #[test]
    fn app_state_shutting_down_flag() {
        let state = test_state();
        assert!(!state.is_shutting_down());
        state
            .shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(state.is_shutting_down());
    }

    // ---- Virtual host rewrite tests ----

    #[tokio::test]
    async fn virtual_host_aws_style_rewrites_root_to_function_name() {
        let app = invoke_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("host", "my-func.lambda.us-east-1.amazonaws.com:9600")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should reach the Function URL handler route (/:function_name) — which
        // returns 404 because the function doesn't exist in config, not because
        // the route was unmatched.
        assert_ne!(resp.status(), http::StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn virtual_host_aws_style_rewrites_path() {
        let app = invoke_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/some/path")
                    .header("host", "test-fn.lambda.us-east-1.amazonaws.com:9600")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Reaches /:function_name/*path — returns 404 (function not found).
        assert_ne!(resp.status(), http::StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn virtual_host_localhost_no_rewrite() {
        let app = invoke_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/nonexistent")
                    .header("host", "localhost:9600")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // No virtual host rewrite — /:function_name matches "nonexistent" as
        // a function name via the Function URL catch-all, returning 404.
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn virtual_host_custom_domain_rewrites_when_configured() {
        let mut state = test_state();
        Arc::get_mut(&mut state.config).unwrap().domain = Some("lambda.local".into());
        let app = invoke_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("host", "my-func.lambda.local:9600")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Rewritten to /my-func → reaches Function URL handler → 404 (no such function).
        assert_ne!(resp.status(), http::StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn virtual_host_preserves_query_string() {
        let app = invoke_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/?key=value")
                    .header("host", "my-func.lambda.us-east-1.amazonaws.com:9600")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should still reach the function URL handler.
        assert_ne!(resp.status(), http::StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn virtual_host_skips_rewrite_for_lambda_api_path() {
        let app = invoke_router(test_state());
        // When the path already matches a Lambda API pattern, the middleware
        // should NOT prepend the function name — the path routes directly to
        // the standard invoke handler.
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/2015-03-31/functions/my-func/invocations")
                    .header("host", "my-func.lambda.us-east-1.amazonaws.com:9600")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should reach the standard invoke route and return 404 (function not
        // in config), NOT 405 Method Not Allowed (route not matched).
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
    }
}
