use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use serde::Serialize;
use tracing::{debug, warn};

use crate::function::validate_function_name;
use crate::server::AppState;
use crate::types::ServiceError;

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

// ---------------------------------------------------------------------------
// Invoke API routes (external, port 9600 by default)
// ---------------------------------------------------------------------------

pub fn invoke_routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route(
            "/2015-03-31/functions/:function_name/invocations",
            post(invoke_function),
        )
}

/// Health check endpoint — pings Docker daemon and reports connectivity.
async fn health(State(state): State<AppState>) -> impl IntoResponse {
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

/// Invoke a Lambda function by name (stub — will be wired to ContainerManager).
async fn invoke_function(
    State(state): State<AppState>,
    Path(function_name): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    // Validate function name from the URL path.
    if let Err(e) = validate_function_name(&function_name) {
        let err = ServiceError::InvalidRequestContent(e.to_string());
        return (err.status_code(), Json(err.to_aws_response()));
    }

    // Log payload at DEBUG level only — never at INFO or above.
    debug!(
        function = %function_name,
        payload_size = body.len(),
        payload = %String::from_utf8_lossy(&body),
        "invoke request"
    );

    // Reject immediately if the service is shutting down.
    if state.is_shutting_down() {
        let err = ServiceError::ServiceException(
            "Service is shutting down, not accepting new invocations".into(),
        );
        return (err.status_code(), Json(err.to_aws_response()));
    }

    let err = ServiceError::ServiceException(format!(
        "Function invocation not yet implemented for '{}'",
        function_name
    ));
    (err.status_code(), Json(err.to_aws_response()))
}

// ---------------------------------------------------------------------------
// Runtime API routes (internal, port 9601 by default)
// ---------------------------------------------------------------------------

pub fn runtime_routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route(
            "/2018-06-01/runtime/invocation/next",
            get(next_invocation),
        )
        .route(
            "/2018-06-01/runtime/invocation/:request_id/response",
            post(invocation_response),
        )
        .route(
            "/2018-06-01/runtime/invocation/:request_id/error",
            post(invocation_error),
        )
        .route(
            "/2018-06-01/runtime/init/error",
            post(init_error),
        )
}

/// Header sent by containers to identify which function they serve.
const HEADER_FUNCTION_NAME: &str = "Lambda-Runtime-Function-Name";
/// Header sent by containers to identify themselves (optional).
const HEADER_CONTAINER_ID: &str = "Lambda-Runtime-Container-Id";

/// GET /2018-06-01/runtime/invocation/next
///
/// Called by the Lambda runtime inside a container to long-poll for the next
/// invocation. The container identifies itself via the
/// `Lambda-Runtime-Function-Name` header.
///
/// On first call, signals that the container is ready (cold start complete).
/// Keeps the connection open until an invocation arrives or the service shuts
/// down.
async fn next_invocation(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Identify the function this container serves.
    let function_name = match headers
        .get(HEADER_FUNCTION_NAME)
        .and_then(|v| v.to_str().ok())
    {
        Some(name) => name.to_string(),
        None => {
            warn!("runtime /next called without {} header", HEADER_FUNCTION_NAME);
            let err = ServiceError::InvalidRequestContent(format!(
                "Missing required header: {}",
                HEADER_FUNCTION_NAME
            ));
            return (err.status_code(), HeaderMap::new(), err.to_aws_response().to_json_bytes());
        }
    };

    // Verify the function exists.
    if !state.runtime_bridge.has_function(&function_name) {
        let err = ServiceError::ResourceNotFound(function_name);
        return (err.status_code(), HeaderMap::new(), err.to_aws_response().to_json_bytes());
    }

    // Mark container as ready on first /next call (cold start complete).
    let container_id = headers
        .get(HEADER_CONTAINER_ID)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(&function_name);
    state.runtime_bridge.mark_ready(container_id).await;

    debug!(
        function = %function_name,
        container_id = %container_id,
        "container long-polling for next invocation"
    );

    // Long-poll until an invocation is available or shutdown.
    match state.runtime_bridge.next_invocation(&function_name).await {
        Some(invocation) => {
            // Compute deadline in epoch milliseconds.
            let deadline_ms = {
                let remaining = invocation
                    .deadline
                    .saturating_duration_since(tokio::time::Instant::now());
                let epoch_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    + remaining.as_millis();
                epoch_ms.to_string()
            };

            // Build the invoked function ARN.
            let arn = format!(
                "arn:aws:lambda:{}:{}:function:{}",
                state.config.region, state.config.account_id, function_name
            );

            let mut response_headers = HeaderMap::new();
            response_headers.insert(
                "Lambda-Runtime-Aws-Request-Id",
                invocation.request_id.to_string().parse().unwrap(),
            );
            response_headers.insert(
                "Lambda-Runtime-Deadline-Ms",
                deadline_ms.parse().unwrap(),
            );
            response_headers.insert(
                "Lambda-Runtime-Invoked-Function-Arn",
                arn.parse().unwrap(),
            );

            if let Some(ref trace_id) = invocation.trace_id {
                response_headers.insert(
                    "Lambda-Runtime-Trace-Id",
                    trace_id.parse().unwrap(),
                );
            }

            debug!(
                function = %function_name,
                request_id = %invocation.request_id,
                "dispatching invocation to container"
            );

            (StatusCode::OK, response_headers, invocation.payload.to_vec())
        }
        None => {
            // Channel closed or shutdown — return a 500 to let the runtime
            // know it should exit.
            let err = ServiceError::ServiceException("Runtime shutting down".into());
            (
                err.status_code(),
                HeaderMap::new(),
                err.to_aws_response().to_json_bytes(),
            )
        }
    }
}

/// POST /2018-06-01/runtime/invocation/:request_id/response
async fn invocation_response(
    State(_state): State<AppState>,
    Path(_request_id): Path<String>,
    _body: Bytes,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// POST /2018-06-01/runtime/invocation/:request_id/error
async fn invocation_error(
    State(_state): State<AppState>,
    Path(_request_id): Path<String>,
    _body: Bytes,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// POST /2018-06-01/runtime/init/error
async fn init_error(State(_state): State<AppState>, _body: Bytes) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::Request;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::config::Config;
    use crate::container::ContainerRegistry;
    use crate::function::FunctionsConfig;
    use crate::runtime::RuntimeBridge;

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
        };
        let docker = bollard::Docker::connect_with_local_defaults().unwrap();
        let functions = FunctionsConfig {
            functions: HashMap::new(),
            runtime_images: HashMap::new(),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), shutdown_rx));
        AppState {
            config: Arc::new(config),
            container_registry: Arc::new(ContainerRegistry::new(docker.clone())),
            docker,
            functions: Arc::new(functions),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            runtime_bridge,
        }
    }

    #[tokio::test]
    async fn invoke_health_returns_ok_with_json() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("status").is_some());
        assert!(json.get("docker").is_some());
        assert!(json["docker"].get("connected").is_some());
    }

    #[tokio::test]
    async fn invoke_function_returns_service_exception() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn runtime_health_returns_ok_with_json() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("status").is_some());
        assert!(json.get("docker").is_some());
    }

    #[tokio::test]
    async fn runtime_next_invocation_rejects_missing_function_header() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::get("/2018-06-01/runtime/invocation/next")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "InvalidRequestContentException");
    }

    #[tokio::test]
    async fn runtime_next_invocation_rejects_unknown_function() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::get("/2018-06-01/runtime/invocation/next")
                    .header("Lambda-Runtime-Function-Name", "nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn runtime_next_invocation_returns_queued_invocation() {
        use crate::types::Invocation;
        use tokio::sync::{mpsc, oneshot};

        let (tx, rx) = mpsc::channel(10);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(receivers, shutdown_rx));

        let mut state = test_state();
        state.runtime_bridge = runtime_bridge;

        // Queue an invocation
        let (resp_tx, _resp_rx) = oneshot::channel();
        let request_id = uuid::Uuid::new_v4();
        let inv = Invocation {
            request_id,
            function_name: "my-func".into(),
            payload: bytes::Bytes::from(r#"{"hello":"world"}"#),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            trace_id: None,
            response_tx: resp_tx,
        };
        tx.send(inv).await.unwrap();

        let app = runtime_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::get("/2018-06-01/runtime/invocation/next")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);

        // Verify required headers
        assert_eq!(
            resp.headers()
                .get("Lambda-Runtime-Aws-Request-Id")
                .unwrap()
                .to_str()
                .unwrap(),
            request_id.to_string()
        );
        assert!(resp
            .headers()
            .get("Lambda-Runtime-Deadline-Ms")
            .is_some());
        assert!(resp
            .headers()
            .get("Lambda-Runtime-Invoked-Function-Arn")
            .is_some());

        // Trace-Id header should be absent when not set
        assert!(resp
            .headers()
            .get("Lambda-Runtime-Trace-Id")
            .is_none());

        // Verify payload
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), br#"{"hello":"world"}"#);
    }

    #[tokio::test]
    async fn runtime_next_invocation_includes_trace_id_header() {
        use crate::types::Invocation;
        use tokio::sync::{mpsc, oneshot};

        let (tx, rx) = mpsc::channel(10);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(receivers, shutdown_rx));

        let mut state = test_state();
        state.runtime_bridge = runtime_bridge;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let trace_id = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
        let inv = Invocation {
            request_id: uuid::Uuid::new_v4(),
            function_name: "my-func".into(),
            payload: bytes::Bytes::from("{}"),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            trace_id: Some(trace_id.to_string()),
            response_tx: resp_tx,
        };
        tx.send(inv).await.unwrap();

        let app = runtime_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::get("/2018-06-01/runtime/invocation/next")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("Lambda-Runtime-Trace-Id")
                .unwrap()
                .to_str()
                .unwrap(),
            trace_id
        );
    }

    #[tokio::test]
    async fn runtime_next_invocation_returns_error_on_shutdown() {
        use tokio::sync::mpsc;

        let (_tx, rx) = mpsc::channel::<crate::types::Invocation>(10);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(receivers, shutdown_rx));

        let mut state = test_state();
        state.runtime_bridge = runtime_bridge;

        // Trigger shutdown before the request
        shutdown_tx.send(true).unwrap();

        let app = runtime_routes().with_state(state);

        let resp = app
            .oneshot(
                Request::get("/2018-06-01/runtime/invocation/next")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn runtime_invocation_response_returns_not_implemented() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2018-06-01/runtime/invocation/abc-123/response")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn runtime_invocation_error_returns_not_implemented() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2018-06-01/runtime/invocation/abc-123/error")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn runtime_init_error_returns_not_implemented() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2018-06-01/runtime/init/error")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn invoke_rejects_invalid_function_name_with_spaces() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/bad%20name/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "InvalidRequestContentException");
    }

    #[tokio::test]
    async fn invoke_rejects_function_name_with_special_chars() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/func%40%23/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invoke_rejects_function_name_too_long() {
        let long_name = "a".repeat(65);
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post(format!(
                    "/2015-03-31/functions/{}/invocations",
                    long_name
                ))
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invoke_accepts_valid_function_name() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-valid_func123/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should not be a 400 — the stub returns 500 ServiceException for valid names
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn invoke_rejects_during_shutdown() {
        let state = test_state();
        state
            .shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["Message"]
            .as_str()
            .unwrap()
            .contains("shutting down"));
    }
}
