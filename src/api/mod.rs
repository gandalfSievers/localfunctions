use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, info_span, warn, Instrument};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// X-Ray Trace ID generation
// ---------------------------------------------------------------------------

/// Generate an AWS X-Ray trace header in the format:
/// `Root=1-{hex-timestamp}-{96-bit-hex-id};Parent={64-bit-hex-id};Sampled=1`
fn generate_xray_trace_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Use two UUIDs to get enough random bytes for the 96-bit and 64-bit IDs.
    let root_rand = Uuid::new_v4();
    let parent_rand = Uuid::new_v4();

    let root_bytes = root_rand.as_bytes();
    let parent_bytes = parent_rand.as_bytes();

    // 96-bit (12 bytes) hex ID for Root
    let root_id: String = root_bytes[..12]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();

    // 64-bit (8 bytes) hex ID for Parent
    let parent_id: String = parent_bytes[..8]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();

    format!(
        "Root=1-{:08x}-{};Parent={};Sampled=1",
        timestamp, root_id, parent_id
    )
}

use crate::function::validate_function_name;
use crate::server::AppState;
use crate::types::{ContainerState, ServiceError};

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
        .route("/metrics", get(get_metrics))
        .route("/2015-03-31/functions", get(list_functions))
        .route(
            "/2015-03-31/functions/:function_name",
            get(get_function),
        )
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

/// GET /metrics — return per-function invocation metrics.
async fn get_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let snapshot = state.metrics.snapshot();
    (StatusCode::OK, Json(serde_json::json!({ "functions": snapshot })))
}

// ---------------------------------------------------------------------------
// List / Get Function APIs
// ---------------------------------------------------------------------------

/// List all configured functions in AWS ListFunctions response format.
///
/// GET /2015-03-31/functions
async fn list_functions(State(state): State<AppState>) -> impl IntoResponse {
    let arn_prefix = format!(
        "arn:aws:lambda:{}:{}:function:",
        state.config.region, state.config.account_id
    );

    let mut functions: Vec<serde_json::Value> = state
        .functions
        .functions
        .values()
        .map(|f| function_configuration_json(f, &arn_prefix))
        .collect();

    // Sort by function name for deterministic output.
    functions.sort_by(|a, b| {
        a["FunctionName"]
            .as_str()
            .unwrap_or("")
            .cmp(b["FunctionName"].as_str().unwrap_or(""))
    });

    let body = serde_json::json!({
        "Functions": functions,
        "NextMarker": null,
    });

    (StatusCode::OK, Json(body))
}

/// Get a single function's configuration in AWS GetFunction response format.
///
/// GET /2015-03-31/functions/{name}
async fn get_function(
    State(state): State<AppState>,
    Path(function_name): Path<String>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    // Validate function name.
    if let Err(e) = validate_function_name(&function_name) {
        let err = ServiceError::InvalidRequestContent(e.to_string());
        return (
            err.status_code(),
            HeaderMap::new(),
            err.to_aws_response().to_json_bytes(),
        );
    }

    // Look up the function.
    let function_config = match state.functions.functions.get(&function_name) {
        Some(config) => config,
        None => {
            let err = ServiceError::ResourceNotFound(function_name);
            return (
                err.status_code(),
                HeaderMap::new(),
                err.to_aws_response().to_json_bytes(),
            );
        }
    };

    let arn_prefix = format!(
        "arn:aws:lambda:{}:{}:function:",
        state.config.region, state.config.account_id
    );

    let body = serde_json::json!({
        "Configuration": function_configuration_json(function_config, &arn_prefix),
        "Code": {
            "Location": "",
            "RepositoryType": "S3",
        },
        "Tags": {},
    });

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json".parse().unwrap());

    (
        StatusCode::OK,
        headers,
        serde_json::to_vec(&body).unwrap_or_default(),
    )
}

/// Build the AWS-format function configuration JSON for a single function.
fn function_configuration_json(
    f: &crate::types::FunctionConfig,
    arn_prefix: &str,
) -> serde_json::Value {
    let mut config = serde_json::json!({
        "FunctionName": f.name,
        "FunctionArn": format!("{}{}", arn_prefix, f.name),
        "Runtime": f.runtime,
        "Handler": f.handler,
        "CodeSize": 0,
        "Timeout": f.timeout,
        "MemorySize": f.memory_size,
        "LastModified": "1970-01-01T00:00:00.000+0000",
        "Version": "$LATEST",
        "RevisionId": "00000000-0000-0000-0000-000000000000",
        "PackageType": if f.image_uri.is_some() { "Image" } else { "Zip" },
        "EphemeralStorage": {
            "Size": f.ephemeral_storage_mb,
        },
    });

    if !f.environment.is_empty() {
        config["Environment"] = serde_json::json!({
            "Variables": f.environment,
        });
    }

    config
}

/// Build the standard headers for an invoke response.
fn invoke_base_headers(request_id: Uuid) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("X-Amz-Request-Id", request_id.to_string().parse().unwrap());
    h.insert("X-Amz-Executed-Version", "$LATEST".parse().unwrap());
    h
}

/// Invoke a Lambda function by name.
///
/// POST /2015-03-31/functions/{name}/invocations
///
/// Supports the following AWS headers:
/// - `X-Amz-Invocation-Type`: `RequestResponse` (default) or `Event` (async)
/// - `X-Amz-Log-Type`: passed through to the runtime (not acted on)
/// - `X-Amz-Client-Context`: passed through to the runtime
///
/// Returns the function response body on success, or an AWS-format error.
/// For `Event` invocation type, returns 202 Accepted immediately and executes
/// the function in the background.
async fn invoke_function(
    State(state): State<AppState>,
    Path(function_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let request_id = Uuid::new_v4();
    let span = info_span!("invocation", %request_id, function = %function_name);

    invoke_function_inner(state, function_name, headers, body, request_id)
        .instrument(span)
        .await
}

async fn invoke_function_inner(
    state: AppState,
    function_name: String,
    headers: HeaderMap,
    body: Bytes,
    request_id: Uuid,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let invoke_start = std::time::Instant::now();
    let mut is_cold_start = false;

    // Validate function name from the URL path.
    if let Err(e) = validate_function_name(&function_name) {
        let err = ServiceError::InvalidRequestContent(e.to_string());
        return (err.status_code(), invoke_base_headers(request_id), err.to_aws_response().to_json_bytes());
    }

    // Log payload at DEBUG level only — never at INFO or above.
    debug!(
        payload_size = body.len(),
        payload = %String::from_utf8_lossy(&body),
        "invoke request"
    );

    // Reject immediately if the service is shutting down.
    if state.is_shutting_down() {
        let err = ServiceError::ServiceException(
            "Service is shutting down, not accepting new invocations".into(),
        );
        return (err.status_code(), invoke_base_headers(request_id), err.to_aws_response().to_json_bytes());
    }

    // Check X-Amz-Invocation-Type — RequestResponse (default) and Event are supported.
    let is_event_invocation = match headers.get("X-Amz-Invocation-Type").and_then(|v| v.to_str().ok()) {
        Some("RequestResponse") | None => false,
        Some("Event") => true,
        Some(other) => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Unsupported invocation type '{}'. Supported types: RequestResponse, Event.",
                other
            ));
            return (err.status_code(), invoke_base_headers(request_id), err.to_aws_response().to_json_bytes());
        }
    };

    // For Event invocations, enforce the async payload size limit (default 256 KB).
    if is_event_invocation {
        let max_async = state.config.max_async_body_size;
        if body.len() > max_async {
            let err = ServiceError::RequestEntityTooLarge(format!(
                "Request payload size ({} bytes) exceeded maximum allowed payload size ({} bytes) for async invocations.",
                body.len(),
                max_async
            ));
            return (err.status_code(), invoke_base_headers(request_id), err.to_aws_response().to_json_bytes());
        }

        let span = info_span!("async_invocation", %request_id, function = %function_name);
        tokio::spawn(
            invoke_async_background(state, function_name, headers, body, request_id)
                .instrument(span),
        );
        return (StatusCode::ACCEPTED, invoke_base_headers(request_id), Vec::new());
    }

    // Look up the function in the configuration.
    let function_config = match state.functions.functions.get(&function_name) {
        Some(config) => config,
        None => {
            let err = ServiceError::ResourceNotFound(function_name);
            return (err.status_code(), invoke_base_headers(request_id), err.to_aws_response().to_json_bytes());
        }
    };

    // Check per-function concurrency limit before proceeding.
    if let Some(limit) = function_config.reserved_concurrent_executions {
        let active = state
            .container_manager
            .count_active_by_function(&function_name)
            .await;
        if active >= limit as usize {
            warn!(
                function = %function_name,
                active_concurrency = active,
                limit = limit,
                "throttled: concurrent invocation limit exceeded"
            );
            // AWS returns {"Type": "User", "Message": "Rate Exceeded."} for
            // throttling — NOT a function error, so X-Amz-Function-Error is
            // not set.
            let body = serde_json::json!({
                "Type": "User",
                "Message": "Rate Exceeded."
            });
            return (
                StatusCode::TOO_MANY_REQUESTS,
                invoke_base_headers(request_id),
                serde_json::to_vec(&body).unwrap(),
            );
        }
    }

    let timeout_secs = function_config.timeout;

    // Extract pass-through headers.
    let client_context = headers
        .get("X-Amz-Client-Context")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Propagate an incoming trace header, or generate a fresh one.
    let trace_id = headers
        .get("X-Amzn-Trace-Id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(generate_xray_trace_id);

    info!(trace_id = %trace_id, "trace context");

    // Compute the deadline from the function's configured timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    // Ensure a container is available for this function. Try to claim a warm
    // idle container first; if none are available, acquire a container slot
    // and cold-start a new one. If all container slots are taken, wait up to
    // container_acquire_timeout before rejecting with 429.
    let container_id = match state.container_manager.claim_idle_container(&function_name).await {
        Some(id) => {
            debug!(container_id = %id, "warm container claimed");
            id
        }
        None => {
            let acquire_timeout = Duration::from_secs(state.config.container_acquire_timeout);
            if !state.container_manager.acquire_container_slot(acquire_timeout).await {
                let err = ServiceError::TooManyRequests(
                    "Rate exceeded: max concurrent containers reached".into(),
                );
                return (err.status_code(), invoke_base_headers(request_id), err.to_aws_response().to_json_bytes());
            }

            is_cold_start = true;
            debug!("no warm container available, cold starting");
            let cold_id = match state.container_manager.create_and_start(function_config).await {
                Ok(id) => id,
                Err(e) => {
                    // Release the container slot since no container was created.
                    state.container_manager.release_container_slot();
                    let err = ServiceError::ServiceException(e.to_string());
                    return (
                        err.status_code(),
                        invoke_base_headers(request_id),
                        err.to_aws_response().to_json_bytes(),
                    );
                }
            };

            state
                .container_manager
                .set_state(&cold_id, ContainerState::Busy)
                .await;

            // Wait for the container's bootstrap process to complete (first
            // /next call). Detect bootstrap failures early rather than waiting
            // for the full function timeout.
            let ready_signal = state
                .runtime_bridge
                .register_ready_signal(&cold_id)
                .await;

            let init_timeout = Duration::from_secs(state.config.init_timeout);
            let mgr = state.container_manager.clone();
            let wait_id = cold_id.clone();

            enum BootstrapOutcome {
                Ready,
                Exited(Option<i64>),
                Timeout,
            }

            let outcome = tokio::select! {
                _ = ready_signal.notified() => BootstrapOutcome::Ready,
                exit_code = async { mgr.wait_for_exit(&wait_id).await } => {
                    BootstrapOutcome::Exited(exit_code)
                }
                _ = tokio::time::sleep(init_timeout) => {
                    // Check if it became ready in the meantime
                    if state.runtime_bridge.is_ready(&cold_id).await {
                        BootstrapOutcome::Ready
                    } else {
                        BootstrapOutcome::Timeout
                    }
                }
            };

            match outcome {
                BootstrapOutcome::Ready => cold_id,
                BootstrapOutcome::Exited(exit_code) => {
                    return bootstrap_failure_response(
                        &state, &cold_id, &function_name, false, exit_code, invoke_base_headers(request_id),
                    ).await;
                }
                BootstrapOutcome::Timeout => {
                    return bootstrap_failure_response(
                        &state, &cold_id, &function_name, true, None, invoke_base_headers(request_id),
                    ).await;
                }
            }
        }
    };

    info!(
        payload_size = body.len(),
        timeout_secs,
        container_id = %container_id,
        "invoking function"
    );

    // Start streaming container stdout/stderr in the background.
    let log_handle = state
        .container_manager
        .stream_container_logs(&container_id, &function_name, &request_id.to_string());

    // Submit the invocation to the runtime bridge.
    let response_rx = match state
        .runtime_bridge
        .submit_invocation(&function_name, request_id, body, deadline, Some(trace_id), client_context)
        .await
    {
        Ok(rx) => rx,
        Err(e) => {
            // If submit fails (e.g. channel closed), release the container.
            log_handle.abort();
            state.container_manager.release_container(&container_id).await;
            let err = ServiceError::ServiceException(e.to_string());
            return (
                StatusCode::BAD_GATEWAY,
                invoke_base_headers(request_id),
                err.to_aws_response().to_json_bytes(),
            );
        }
    };

    // Wait for the response with the configured timeout.
    let result = tokio::time::timeout(Duration::from_secs(timeout_secs), response_rx).await;

    let response = match result {
        Ok(Ok(invocation_result)) => match invocation_result {
            crate::types::InvocationResult::Success { body } => {
                // AWS enforces a 6,291,556 byte limit on synchronous invocation responses.
                const SYNC_RESPONSE_MAX_BYTES: usize = 6_291_556;
                let body_bytes = body.into_bytes();
                if body_bytes.len() > SYNC_RESPONSE_MAX_BYTES {
                    warn!(
                        actual = body_bytes.len(),
                        limit = SYNC_RESPONSE_MAX_BYTES,
                        "response payload size exceeded limit"
                    );
                    let mut resp_headers = invoke_base_headers(request_id);
                    resp_headers.insert("X-Amz-Function-Error", "Unhandled".parse().unwrap());
                    let error_body = serde_json::json!({
                        "errorType": "ResponseSizeTooLarge",
                        "errorMessage": format!(
                            "Response payload size ({} bytes) exceeded maximum allowed payload size ({} bytes).",
                            body_bytes.len(),
                            SYNC_RESPONSE_MAX_BYTES
                        ),
                    });
                    (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        resp_headers,
                        serde_json::to_vec(&error_body).unwrap(),
                    )
                } else {
                    info!("invocation succeeded");
                    (StatusCode::OK, invoke_base_headers(request_id), body_bytes)
                }
            }
            crate::types::InvocationResult::Error {
                error_type,
                error_message,
            } => {
                warn!(
                    error_type = %error_type,
                    error_message = %error_message,
                    "function returned error"
                );
                let mut resp_headers = invoke_base_headers(request_id);
                resp_headers.insert("X-Amz-Function-Error", "Handled".parse().unwrap());
                let error_body = serde_json::json!({
                    "errorType": error_type,
                    "errorMessage": error_message,
                });
                (StatusCode::OK, resp_headers, serde_json::to_vec(&error_body).unwrap())
            }
            crate::types::InvocationResult::Timeout => {
                // This branch is reached when the function runtime itself
                // reports a timeout via the runtime bridge (as opposed to the
                // tokio::time::timeout Err(_) branch below which fires when
                // the invoke handler's own deadline expires).
                warn!("function reported timeout via runtime bridge");
                let mut resp_headers = invoke_base_headers(request_id);
                resp_headers.insert("X-Amz-Function-Error", "Unhandled".parse().unwrap());
                let error_body = serde_json::json!({
                    "errorMessage": format!("Task timed out after {} seconds", timeout_secs),
                });
                (StatusCode::OK, resp_headers, serde_json::to_vec(&error_body).unwrap())
            }
        },
        Ok(Err(_)) => {
            // The response channel was dropped — container likely crashed.
            // Clean up the pending invocation and container.
            let crashed_container = state
                .runtime_bridge
                .timeout_invocation(request_id)
                .await;
            let cid = crashed_container.unwrap_or(container_id);
            let mgr = state.container_manager.clone();
            tokio::spawn(async move {
                let _ = mgr.stop_and_remove(&cid, Duration::from_secs(2)).await;
            });

            let err = ServiceError::ServiceException(
                "Container exited without responding".into(),
            );
            (
                StatusCode::BAD_GATEWAY,
                invoke_base_headers(request_id),
                err.to_aws_response().to_json_bytes(),
            )
        }
        Err(_) => {
            // Timeout waiting for response.
            warn!(timeout_secs, "invocation timed out");

            // Remove the pending invocation and find which container was
            // handling it so we can kill it.
            let timed_out_container = state
                .runtime_bridge
                .timeout_invocation(request_id)
                .await;

            // Kill the container in the background with a short grace period.
            let cid = timed_out_container.unwrap_or(container_id);
            let mgr = state.container_manager.clone();
            tokio::spawn(async move {
                let _ = mgr.stop_and_remove(&cid, Duration::from_secs(2)).await;
            });

            let mut resp_headers = invoke_base_headers(request_id);
            resp_headers.insert("X-Amz-Function-Error", "Unhandled".parse().unwrap());
            let error_body = serde_json::json!({
                "errorMessage": format!("Task timed out after {} seconds", timeout_secs),
            });
            (StatusCode::OK, resp_headers, serde_json::to_vec(&error_body).unwrap())
        }
    };

    // Stop streaming container logs now that the invocation is complete.
    log_handle.abort();

    // Record invocation metrics.
    let is_error = matches!(
        response.0,
        StatusCode::INTERNAL_SERVER_ERROR | StatusCode::BAD_GATEWAY
    ) || response.1.contains_key("X-Amz-Function-Error");
    state.metrics.record_invocation(
        &function_name,
        invoke_start.elapsed(),
        is_error,
        is_cold_start,
    );

    response
}

/// Execute an invocation in the background for Event-type invocations.
///
/// Runs the full synchronous invocation path; errors are logged but not
/// returned to any caller. Timeout enforcement and container cleanup still
/// apply through the normal invoke path.
fn invoke_async_background(
    state: AppState,
    function_name: String,
    mut headers: HeaderMap,
    body: Bytes,
    request_id: Uuid,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        // Swap the invocation type so the inner handler runs the synchronous path.
        headers.insert("X-Amz-Invocation-Type", "RequestResponse".parse().unwrap());
        let (status, _headers, _body) =
            invoke_function_inner(state, function_name, headers, body, request_id).await;
        match status {
            StatusCode::OK => {
                info!("async invocation completed successfully");
            }
            status => {
                warn!(%status, "async invocation completed with error");
            }
        }
    })
}

/// Build an error response for a bootstrap failure (container exited before
/// calling /next, or init timeout expired).
///
/// Collects stderr from the container, logs a WARN, cleans up the container
/// in the background, and returns a 502 with `InvalidRuntimeException`.
async fn bootstrap_failure_response(
    state: &AppState,
    container_id: &str,
    function_name: &str,
    is_timeout: bool,
    exit_code: Option<i64>,
    headers: HeaderMap,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let stderr = state
        .container_manager
        .get_container_stderr(container_id, "50")
        .await;

    warn!(
        function = %function_name,
        exit_code = ?exit_code,
        stderr = %stderr.trim(),
        "bootstrap failure: {}",
        if is_timeout { "init timeout" } else { "process exited before calling /next" }
    );

    // Clean up the container in the background
    let cleanup_mgr = state.container_manager.clone();
    let cleanup_id = container_id.to_string();
    tokio::spawn(async move {
        let _ = cleanup_mgr
            .stop_and_remove(&cleanup_id, Duration::from_secs(2))
            .await;
    });

    let error_detail = if is_timeout {
        format!(
            "Bootstrap did not complete within {} seconds. Container stderr: {}",
            state.config.init_timeout,
            stderr.trim()
        )
    } else {
        format!(
            "Bootstrap process exited (code: {}) before calling Runtime API. Container stderr: {}",
            exit_code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".into()),
            stderr.trim()
        )
    };

    let err = ServiceError::InvalidRuntime(error_detail);
    (StatusCode::BAD_GATEWAY, headers, err.to_aws_response().to_json_bytes())
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
            // Destructure to separate response_tx from the rest.
            let request_id = invocation.request_id;
            let payload = invocation.payload;
            let deadline = invocation.deadline;
            let trace_id = invocation.trace_id;
            let client_context = invocation.client_context;
            let response_tx = invocation.response_tx;

            // Store the response channel so /response and /error can forward
            // results back to the original invoke caller.
            let dispatched_container_id = headers
                .get(HEADER_CONTAINER_ID)
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            state
                .runtime_bridge
                .store_pending(request_id, function_name.clone(), dispatched_container_id, response_tx)
                .await;

            // Compute deadline in epoch milliseconds.
            let deadline_ms = {
                let remaining = deadline
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
                request_id.to_string().parse().unwrap(),
            );
            response_headers.insert(
                "Lambda-Runtime-Deadline-Ms",
                deadline_ms.parse().unwrap(),
            );
            response_headers.insert(
                "Lambda-Runtime-Invoked-Function-Arn",
                arn.parse().unwrap(),
            );

            if let Some(ref trace_id) = trace_id {
                response_headers.insert(
                    "Lambda-Runtime-Trace-Id",
                    trace_id.parse().unwrap(),
                );
            }

            if let Some(ref client_context) = client_context {
                response_headers.insert(
                    "Lambda-Runtime-Client-Context",
                    client_context.parse().unwrap(),
                );
            }

            debug!(
                function = %function_name,
                request_id = %request_id,
                "dispatching invocation to container"
            );

            (StatusCode::OK, response_headers, payload.to_vec())
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

/// Error body sent by Lambda runtimes when reporting invocation or init errors.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct RuntimeErrorRequest {
    #[serde(default)]
    errorMessage: String,
    #[serde(default)]
    errorType: String,
    #[serde(default)]
    stackTrace: Vec<String>,
}

/// Parse a runtime error from the request body and optional header.
///
/// Returns `(error_type, error_message)`. Falls back to `default_error_type`
/// when neither the body nor the header provides an error type.
fn parse_runtime_error(
    body: &Bytes,
    header_error_type: Option<String>,
    default_error_type: &str,
    default_error_message: &str,
) -> (String, String) {
    match serde_json::from_slice::<RuntimeErrorRequest>(body) {
        Ok(err_body) => {
            let etype = if err_body.errorType.is_empty() {
                header_error_type.unwrap_or_else(|| default_error_type.into())
            } else {
                err_body.errorType
            };
            let emsg = if err_body.errorMessage.is_empty() {
                default_error_message.into()
            } else {
                err_body.errorMessage
            };
            (etype, emsg)
        }
        Err(_) => {
            let etype = header_error_type.unwrap_or_else(|| default_error_type.into());
            let emsg = String::from_utf8_lossy(body).into_owned();
            let emsg = if emsg.is_empty() {
                default_error_message.into()
            } else {
                emsg
            };
            (etype, emsg)
        }
    }
}

/// POST /2018-06-01/runtime/invocation/:request_id/response
///
/// Called by the Lambda runtime after successfully processing an invocation.
/// Forwards the response body back to the original invoke caller.
async fn invocation_response(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let uuid = match Uuid::parse_str(&request_id) {
        Ok(id) => id,
        Err(_) => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Invalid request_id: {}",
                request_id
            ));
            return (err.status_code(), Json(serde_json::to_value(err.to_aws_response()).unwrap()));
        }
    };

    let body_str = String::from_utf8_lossy(&body).into_owned();

    debug!(
        request_id = %uuid,
        payload_size = body.len(),
        "invocation response received"
    );

    let (success, container_id) = state.runtime_bridge.complete_invocation(uuid, body_str).await;
    if success {
        // Release the container back to the idle pool for reuse.
        if let Some(ref cid) = container_id {
            debug!(container_id = %cid, "releasing container after successful invocation");
            state.container_manager.release_container(cid).await;
        }
        (StatusCode::ACCEPTED, Json(serde_json::json!({"status": "OK"})))
    } else {
        let err = ServiceError::InvalidRequestContent(format!(
            "Unknown request_id: {}",
            request_id
        ));
        (err.status_code(), Json(serde_json::to_value(err.to_aws_response()).unwrap()))
    }
}

/// POST /2018-06-01/runtime/invocation/:request_id/error
///
/// Called by the Lambda runtime when the function handler returns an error.
/// Formats the error in AWS error format and forwards it to the invoke caller.
async fn invocation_error(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let uuid = match Uuid::parse_str(&request_id) {
        Ok(id) => id,
        Err(_) => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Invalid request_id: {}",
                request_id
            ));
            return (err.status_code(), Json(serde_json::to_value(err.to_aws_response()).unwrap()));
        }
    };

    // The error type can come from the header or the body.
    let header_error_type = headers
        .get("Lambda-Runtime-Function-Error-Type")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let (error_type, error_message) = parse_runtime_error(
        &body,
        header_error_type,
        "Runtime.UnknownError",
        "Unknown error",
    );

    warn!(
        request_id = %uuid,
        error_type = %error_type,
        error_message = %error_message,
        "invocation error received"
    );

    let (success, container_id) = state
        .runtime_bridge
        .fail_invocation(uuid, error_type, error_message)
        .await;

    if success {
        // Release the container back to the idle pool for reuse.
        // AWS Lambda reuses containers after handled errors.
        if let Some(ref cid) = container_id {
            debug!(container_id = %cid, "releasing container after invocation error");
            state.container_manager.release_container(cid).await;
        }
        (StatusCode::ACCEPTED, Json(serde_json::json!({"status": "OK"})))
    } else {
        let err = ServiceError::InvalidRequestContent(format!(
            "Unknown request_id: {}",
            request_id
        ));
        (err.status_code(), Json(serde_json::to_value(err.to_aws_response()).unwrap()))
    }
}

/// POST /2018-06-01/runtime/init/error
///
/// Called by the Lambda runtime when it fails to initialize (e.g. handler not
/// found, syntax error). Fails all pending invocations for the function with
/// a 502 InvalidRuntimeException, then stops and removes the container.
///
/// NOTE: This endpoint requires a `Lambda-Runtime-Function-Name` header to
/// identify which function failed. This is a localfunctions extension — the
/// real AWS Runtime API identifies the function from the container context.
async fn init_error(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let function_name = match headers
        .get(HEADER_FUNCTION_NAME)
        .and_then(|v| v.to_str().ok())
    {
        Some(name) => name.to_string(),
        None => {
            warn!("runtime /init/error called without {} header", HEADER_FUNCTION_NAME);
            let err = ServiceError::InvalidRequestContent(format!(
                "Missing required header: {}",
                HEADER_FUNCTION_NAME
            ));
            return (err.status_code(), Json(serde_json::to_value(err.to_aws_response()).unwrap()));
        }
    };

    let (error_type, error_message) = parse_runtime_error(
        &body,
        None,
        "Runtime.InitError",
        "Runtime failed to initialize",
    );

    info!(
        function = %function_name,
        error_type = %error_type,
        error_message = %error_message,
        "runtime init error received"
    );

    // Fail all pending invocations (both queued and already dispatched).
    let failed_count = state.runtime_bridge.fail_init(&function_name).await;
    if failed_count > 0 {
        info!(
            function = %function_name,
            failed_count,
            "failed pending invocations due to init error"
        );
    }

    // Stop and remove the container(s) for this function.
    let timeout = Duration::from_secs(state.config.shutdown_timeout);
    let removed = state
        .container_registry
        .stop_and_remove_by_function(&function_name, timeout)
        .await;
    if !removed.is_empty() {
        info!(
            function = %function_name,
            removed_count = removed.len(),
            "stopped and removed containers due to init error"
        );
    }
    // Also clean up ContainerManager tracking for these containers.
    state
        .container_manager
        .deregister_by_function(&function_name)
        .await;

    // Return 502 InvalidRuntimeException to match AWS behavior.
    let err_response = serde_json::json!({
        "Type": "InvalidRuntimeException",
        "Message": format!("Runtime failed to initialize: {}", error_message),
    });
    (StatusCode::BAD_GATEWAY, Json(err_response))
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
    use crate::container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig};
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
            log_format: crate::config::LogFormat::Text,
            pull_images: false,
            init_timeout: 10,
            container_acquire_timeout: 10,
            forward_aws_credentials: true,
            mount_aws_credentials: false,
            max_async_body_size: 256 * 1024,
        };
        let docker = bollard::Docker::connect_with_local_defaults().unwrap();
        let functions = FunctionsConfig {
            functions: HashMap::new(),
            runtime_images: HashMap::new(),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
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
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            runtime_bridge,
            metrics: Arc::new(crate::metrics::MetricsCollector::new()),
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
    async fn metrics_endpoint_returns_empty_on_fresh_start() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let functions = json.get("functions").unwrap().as_object().unwrap();
        assert!(functions.is_empty());
    }

    #[tokio::test]
    async fn metrics_endpoint_reflects_recorded_invocations() {
        let state = test_state();
        // Simulate recording some invocations directly.
        state.metrics.record_invocation(
            "test-func",
            std::time::Duration::from_millis(50),
            false,
            true,
        );
        state.metrics.record_invocation(
            "test-func",
            std::time::Duration::from_millis(150),
            true,
            false,
        );

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let func_metrics = &json["functions"]["test-func"];
        assert_eq!(func_metrics["invocation_count"], 2);
        assert_eq!(func_metrics["error_count"], 1);
        assert_eq!(func_metrics["cold_start_count"], 1);
        assert!(func_metrics["avg_duration_ms"].as_f64().unwrap() > 0.0);
        assert!(func_metrics["p50_duration_ms"].as_f64().unwrap() > 0.0);
        assert!(func_metrics["p95_duration_ms"].as_f64().unwrap() > 0.0);
        assert!(func_metrics["p99_duration_ms"].as_f64().unwrap() > 0.0);
    }

    #[tokio::test]
    async fn invoke_function_not_found_returns_404() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // Invocation errors include request-id but NOT function-error header
        assert!(resp.headers().get("X-Amz-Request-Id").is_some());
        assert_eq!(
            resp.headers().get("X-Amz-Executed-Version").unwrap().to_str().unwrap(),
            "$LATEST"
        );
        assert!(resp.headers().get("X-Amz-Function-Error").is_none());
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "ResourceNotFoundException");
    }

    // -- List Functions / Get Function ----------------------------------------

    fn test_state_with_functions() -> AppState {
        let config = Config {
            host: "127.0.0.1".parse().unwrap(),
            port: 9600,
            runtime_port: 9601,
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
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
        };
        let docker = bollard::Docker::connect_with_local_defaults().unwrap();

        let mut functions_map = HashMap::new();
        functions_map.insert(
            "alpha-func".to_string(),
            crate::types::FunctionConfig {
                name: "alpha-func".into(),
                runtime: "python3.12".into(),
                handler: "main.handler".into(),
                code_path: std::path::PathBuf::from("/tmp/code"),
                timeout: 60,
                memory_size: 256,
                ephemeral_storage_mb: 1024,
                environment: HashMap::from([("ENV_KEY".into(), "env_val".into())]),
                image: None,
                image_uri: None,
                reserved_concurrent_executions: None,
                architecture: "x86_64".into(),
            },
        );
        functions_map.insert(
            "beta-func".to_string(),
            crate::types::FunctionConfig {
                name: "beta-func".into(),
                runtime: "nodejs20.x".into(),
                handler: "index.handler".into(),
                code_path: std::path::PathBuf::from("/tmp/code2"),
                timeout: 30,
                memory_size: 128,
                ephemeral_storage_mb: 512,
                environment: HashMap::new(),
                image: None,
                image_uri: Some("my-image:latest".into()),
                reserved_concurrent_executions: None,
                architecture: "x86_64".into(),
            },
        );
        let functions = FunctionsConfig {
            functions: functions_map,
            runtime_images: HashMap::new(),
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
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
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            runtime_bridge,
            metrics: Arc::new(crate::metrics::MetricsCollector::new()),
        }
    }

    #[tokio::test]
    async fn list_functions_returns_all_functions() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(Request::get("/2015-03-31/functions").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let functions = json["Functions"].as_array().unwrap();
        assert_eq!(functions.len(), 2);

        // Sorted alphabetically
        assert_eq!(functions[0]["FunctionName"], "alpha-func");
        assert_eq!(functions[1]["FunctionName"], "beta-func");

        // Verify ARN format
        assert_eq!(
            functions[0]["FunctionArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:alpha-func"
        );

        // Verify fields present
        assert_eq!(functions[0]["Runtime"], "python3.12");
        assert_eq!(functions[0]["Handler"], "main.handler");
        assert_eq!(functions[0]["Timeout"], 60);
        assert_eq!(functions[0]["MemorySize"], 256);

        // Environment included when non-empty
        assert_eq!(functions[0]["Environment"]["Variables"]["ENV_KEY"], "env_val");

        // Environment absent when empty
        assert!(functions[1]["Environment"].is_null());

        // PackageType for image-based
        assert_eq!(functions[1]["PackageType"], "Image");
        assert_eq!(functions[0]["PackageType"], "Zip");
    }

    #[tokio::test]
    async fn list_functions_empty() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(Request::get("/2015-03-31/functions").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let functions = json["Functions"].as_array().unwrap();
        assert_eq!(functions.len(), 0);
    }

    #[tokio::test]
    async fn get_function_returns_configuration() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(
                Request::get("/2015-03-31/functions/alpha-func")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let config = &json["Configuration"];
        assert_eq!(config["FunctionName"], "alpha-func");
        assert_eq!(
            config["FunctionArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:alpha-func"
        );
        assert_eq!(config["Runtime"], "python3.12");
        assert_eq!(config["Handler"], "main.handler");
        assert_eq!(config["Timeout"], 60);
        assert_eq!(config["MemorySize"], 256);
        assert_eq!(config["Environment"]["Variables"]["ENV_KEY"], "env_val");
        assert_eq!(config["EphemeralStorage"]["Size"], 1024);
        assert_eq!(config["Version"], "$LATEST");
        assert_eq!(config["PackageType"], "Zip");

        // Code section present
        assert!(json["Code"].is_object());
    }

    #[tokio::test]
    async fn get_function_not_found_returns_404() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::get("/2015-03-31/functions/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn get_function_invalid_name_returns_400() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::get("/2015-03-31/functions/inv@lid!")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "InvalidRequestContentException");
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
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

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
            client_context: None,
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
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

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
            client_context: None,
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
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

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
    async fn runtime_invocation_response_invalid_uuid_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2018-06-01/runtime/invocation/not-a-uuid/response")
                    .body(Body::from(r#"{"result": "ok"}"#))
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
    async fn runtime_invocation_response_unknown_request_id_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let unknown_id = uuid::Uuid::new_v4();
        let resp = app
            .oneshot(
                Request::post(format!(
                    "/2018-06-01/runtime/invocation/{}/response",
                    unknown_id
                ))
                .body(Body::from(r#"{"result": "ok"}"#))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn runtime_invocation_response_forwards_to_caller() {
        use crate::types::Invocation;
        use tokio::sync::{mpsc, oneshot};

        let (tx, rx) = mpsc::channel(10);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

        let mut state = test_state();
        state.runtime_bridge = runtime_bridge;

        // Queue an invocation
        let (resp_tx, resp_rx) = oneshot::channel();
        let request_id = uuid::Uuid::new_v4();
        let inv = Invocation {
            request_id,
            function_name: "my-func".into(),
            payload: bytes::Bytes::from(r#"{"input":"data"}"#),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            trace_id: None,
            client_context: None,
            response_tx: resp_tx,
        };
        tx.send(inv).await.unwrap();

        // First, dispatch the invocation via /next (this stores the pending response_tx)
        let app = runtime_routes().with_state(state.clone());
        let next_resp = app
            .oneshot(
                Request::get("/2018-06-01/runtime/invocation/next")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(next_resp.status(), StatusCode::OK);

        // Now post the response
        let app = runtime_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post(format!(
                    "/2018-06-01/runtime/invocation/{}/response",
                    request_id
                ))
                .body(Body::from(r#"{"result": "success"}"#))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // Verify the caller received the result
        let result = resp_rx.await.unwrap();
        assert_eq!(
            result,
            crate::types::InvocationResult::Success {
                body: r#"{"result": "success"}"#.into()
            }
        );
    }

    #[tokio::test]
    async fn runtime_invocation_error_invalid_uuid_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2018-06-01/runtime/invocation/not-a-uuid/error")
                    .body(Body::from(r#"{"errorMessage":"bad","errorType":"RuntimeError"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn runtime_invocation_error_forwards_to_caller() {
        use crate::types::Invocation;
        use tokio::sync::{mpsc, oneshot};

        let (tx, rx) = mpsc::channel(10);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

        let mut state = test_state();
        state.runtime_bridge = runtime_bridge;

        let (resp_tx, resp_rx) = oneshot::channel();
        let request_id = uuid::Uuid::new_v4();
        let inv = Invocation {
            request_id,
            function_name: "my-func".into(),
            payload: bytes::Bytes::from("{}"),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            trace_id: None,
            client_context: None,
            response_tx: resp_tx,
        };
        tx.send(inv).await.unwrap();

        // Dispatch via /next
        let app = runtime_routes().with_state(state.clone());
        let next_resp = app
            .oneshot(
                Request::get("/2018-06-01/runtime/invocation/next")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(next_resp.status(), StatusCode::OK);

        // Post error with Lambda-Runtime-Function-Error-Type header
        let app = runtime_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post(format!(
                    "/2018-06-01/runtime/invocation/{}/error",
                    request_id
                ))
                .header("Lambda-Runtime-Function-Error-Type", "Runtime.HandlerError")
                .body(Body::from(
                    r#"{"errorMessage":"something went wrong","errorType":"RuntimeError"}"#,
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // Verify error was forwarded
        let result = resp_rx.await.unwrap();
        assert_eq!(
            result,
            crate::types::InvocationResult::Error {
                error_type: "RuntimeError".into(),
                error_message: "something went wrong".into(),
            }
        );
    }

    #[tokio::test]
    async fn runtime_invocation_error_uses_header_when_body_has_no_type() {
        use crate::types::Invocation;
        use tokio::sync::{mpsc, oneshot};

        let (tx, rx) = mpsc::channel(10);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

        let mut state = test_state();
        state.runtime_bridge = runtime_bridge;

        let (resp_tx, resp_rx) = oneshot::channel();
        let request_id = uuid::Uuid::new_v4();
        let inv = Invocation {
            request_id,
            function_name: "my-func".into(),
            payload: bytes::Bytes::from("{}"),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            trace_id: None,
            client_context: None,
            response_tx: resp_tx,
        };
        tx.send(inv).await.unwrap();

        // Dispatch via /next
        let app = runtime_routes().with_state(state.clone());
        app.oneshot(
            Request::get("/2018-06-01/runtime/invocation/next")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        // Post error with type only in header, not body
        let app = runtime_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post(format!(
                    "/2018-06-01/runtime/invocation/{}/error",
                    request_id
                ))
                .header("Lambda-Runtime-Function-Error-Type", "Runtime.ImportError")
                .body(Body::from(r#"{"errorMessage":"module not found"}"#))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let result = resp_rx.await.unwrap();
        assert_eq!(
            result,
            crate::types::InvocationResult::Error {
                error_type: "Runtime.ImportError".into(),
                error_message: "module not found".into(),
            }
        );
    }

    #[tokio::test]
    async fn runtime_init_error_missing_header_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2018-06-01/runtime/init/error")
                    .body(Body::from(
                        r#"{"errorMessage":"handler not found","errorType":"Runtime.InitError"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn runtime_init_error_returns_502_invalid_runtime_exception() {
        use tokio::sync::mpsc;

        let (_tx, rx) = mpsc::channel::<crate::types::Invocation>(10);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

        let mut state = test_state();
        state.runtime_bridge = runtime_bridge;

        let app = runtime_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2018-06-01/runtime/init/error")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .body(Body::from(
                        r#"{"errorMessage":"handler not found","errorType":"Runtime.InitError"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "InvalidRuntimeException");
        assert!(json["Message"].as_str().unwrap().contains("handler not found"));
    }

    #[tokio::test]
    async fn runtime_init_error_fails_pending_invocations() {
        use crate::types::Invocation;
        use tokio::sync::{mpsc, oneshot};

        let (tx, rx) = mpsc::channel(10);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

        let mut state = test_state();
        state.runtime_bridge = runtime_bridge;

        // Queue an invocation (simulating a caller waiting)
        let (resp_tx, resp_rx) = oneshot::channel();
        let inv = Invocation {
            request_id: uuid::Uuid::new_v4(),
            function_name: "my-func".into(),
            payload: bytes::Bytes::from("{}"),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            trace_id: None,
            client_context: None,
            response_tx: resp_tx,
        };
        tx.send(inv).await.unwrap();

        // Send init error
        let app = runtime_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2018-06-01/runtime/init/error")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .body(Body::from(
                        r#"{"errorMessage":"cannot import module","errorType":"Runtime.ImportModuleError"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

        // The queued invocation should have received an error
        let result = resp_rx.await.unwrap();
        assert_eq!(
            result,
            crate::types::InvocationResult::Error {
                error_type: "InvalidRuntimeException".into(),
                error_message: "Runtime failed to initialize".into(),
            }
        );
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
        // Valid name but function doesn't exist → 404, not 400
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// Create a test state with a real function configured and invocation
    /// channels wired through the RuntimeBridge.
    async fn test_state_with_function(
        function_name: &str,
        timeout: u64,
    ) -> (AppState, tokio::sync::watch::Sender<bool>) {
        use crate::types::FunctionConfig;
        use std::path::PathBuf;

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
        };
        let docker = bollard::Docker::connect_with_local_defaults().unwrap();

        let mut functions_map = HashMap::new();
        functions_map.insert(
            function_name.to_string(),
            FunctionConfig {
                name: function_name.to_string(),
                runtime: "python3.12".into(),
                handler: "main.handler".into(),
                code_path: PathBuf::from("/tmp/code"),
                timeout,
                memory_size: 128,
                ephemeral_storage_mb: 512,
                environment: HashMap::new(),
                image: None,
                image_uri: None,
                reserved_concurrent_executions: None,
                architecture: "x86_64".into(),
            },
        );
        let functions = FunctionsConfig {
            functions: functions_map,
            runtime_images: HashMap::new(),
        };

        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let mut senders = HashMap::new();
        senders.insert(function_name.to_string(), tx);

        let mut receivers = HashMap::new();
        receivers.insert(function_name.to_string(), rx);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(senders, receivers, shutdown_rx));

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

        // Pre-populate an idle container so the invoke handler doesn't attempt
        // real Docker operations in unit tests.
        container_manager
            .insert_test_container(
                "test-container".into(),
                function_name.into(),
                crate::types::ContainerState::Idle,
            )
            .await;

        let state = AppState {
            config: Arc::new(config),
            container_registry,
            container_manager,
            docker,
            functions: Arc::new(functions),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            runtime_bridge,
            metrics: Arc::new(crate::metrics::MetricsCollector::new()),
        };

        (state, shutdown_tx)
    }

    #[tokio::test]
    async fn invoke_success_returns_200_with_body() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let app = invoke_routes().with_state(state);

        // Spawn a task that acts as the container runtime:
        // picks up the invocation from /next and posts a response.
        let bridge = runtime_bridge.clone();
        tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            bridge
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                .await;
            bridge
                .complete_invocation(inv.request_id, r#"{"result":"ok"}"#.into())
                .await;
        });

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from(r#"{"input":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get("X-Amz-Function-Error").is_none());
        // Verify AWS-compatible headers
        let req_id = resp.headers().get("X-Amz-Request-Id").unwrap().to_str().unwrap();
        uuid::Uuid::parse_str(req_id).expect("X-Amz-Request-Id should be a valid UUID");
        assert_eq!(
            resp.headers().get("X-Amz-Executed-Version").unwrap().to_str().unwrap(),
            "$LATEST"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), br#"{"result":"ok"}"#);
    }

    #[tokio::test]
    async fn invoke_function_error_returns_200_with_handled_error_header() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let app = invoke_routes().with_state(state);

        let bridge = runtime_bridge.clone();
        tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            bridge
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                .await;
            bridge
                .fail_invocation(
                    inv.request_id,
                    "RuntimeError".into(),
                    "something broke".into(),
                )
                .await;
        });

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("X-Amz-Function-Error").unwrap().to_str().unwrap(),
            "Handled"
        );
        // Verify AWS-compatible headers
        let req_id = resp.headers().get("X-Amz-Request-Id").unwrap().to_str().unwrap();
        uuid::Uuid::parse_str(req_id).expect("X-Amz-Request-Id should be a valid UUID");
        assert_eq!(
            resp.headers().get("X-Amz-Executed-Version").unwrap().to_str().unwrap(),
            "$LATEST"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["errorType"], "RuntimeError");
        assert_eq!(json["errorMessage"], "something broke");
    }

    #[tokio::test]
    async fn invoke_timeout_returns_200_with_unhandled_error_header() {
        // Use a very short timeout (1 second) and never respond.
        let (state, _shutdown_tx) = test_state_with_function("my-func", 1).await;

        let app = invoke_routes().with_state(state);

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("X-Amz-Function-Error").unwrap().to_str().unwrap(),
            "Unhandled"
        );
        // Verify AWS-compatible headers
        let req_id = resp.headers().get("X-Amz-Request-Id").unwrap().to_str().unwrap();
        uuid::Uuid::parse_str(req_id).expect("X-Amz-Request-Id should be a valid UUID");
        assert_eq!(
            resp.headers().get("X-Amz-Executed-Version").unwrap().to_str().unwrap(),
            "$LATEST"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["errorMessage"]
            .as_str()
            .unwrap()
            .contains("Task timed out after 1 seconds"));
    }

    #[tokio::test]
    async fn invoke_unsupported_invocation_type_returns_400() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let app = invoke_routes().with_state(state);

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Invocation-Type", "DryRun")
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
    async fn invoke_event_type_returns_202_immediately() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        // Spawn a background handler to process the async invocation so it
        // doesn't hang after the test completes.
        tokio::spawn(async move {
            if let Some(inv) = runtime_bridge.next_invocation("my-func").await {
                runtime_bridge
                    .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                    .await;
                let _ = runtime_bridge
                    .complete_invocation(inv.request_id, "ok".into())
                    .await;
            }
        });

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Invocation-Type", "Event")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn invoke_event_type_errors_are_logged_not_returned() {
        // Event invocation for a non-existent function should still return 202
        // immediately — the error is only logged in the background.
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let app = invoke_routes().with_state(state);

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/nonexistent/invocations")
                    .header("X-Amz-Invocation-Type", "Event")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should return 202 even though the function doesn't exist — the error
        // is handled asynchronously.
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn invoke_event_type_rejects_oversized_payload() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let app = invoke_routes().with_state(state);

        // 256 KB + 1 byte exceeds the async limit
        let oversized = vec![b'x'; 256 * 1024 + 1];

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Invocation-Type", "Event")
                    .body(Body::from(oversized))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "RequestEntityTooLargeException");
        assert!(json["Message"].as_str().unwrap().contains("262144"));
    }

    #[tokio::test]
    async fn invoke_event_type_accepts_payload_at_limit() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        tokio::spawn(async move {
            if let Some(inv) = runtime_bridge.next_invocation("my-func").await {
                runtime_bridge
                    .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                    .await;
                let _ = runtime_bridge
                    .complete_invocation(inv.request_id, "ok".into())
                    .await;
            }
        });

        let app = invoke_routes().with_state(state);

        // Exactly 256 KB should be accepted
        let at_limit = vec![b'x'; 256 * 1024];

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Invocation-Type", "Event")
                    .body(Body::from(at_limit))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn invoke_sync_allows_payload_over_async_limit() {
        // Synchronous invocations should not be restricted by the async limit.
        let (state, _shutdown_tx) = test_state_with_function("my-func", 1).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let bridge = runtime_bridge.clone();
        tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            bridge
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                .await;
            bridge
                .complete_invocation(inv.request_id, "ok".into())
                .await;
        });

        let app = invoke_routes().with_state(state);

        // 256 KB + 1 exceeds async limit but is fine for sync (< 6 MB)
        let payload = vec![b'x'; 256 * 1024 + 1];

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Invocation-Type", "RequestResponse")
                    .body(Body::from(payload))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn invoke_request_response_type_accepted() {
        // RequestResponse is the default and should work.
        let (state, _shutdown_tx) = test_state_with_function("my-func", 1).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let bridge = runtime_bridge.clone();
        tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            bridge
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                .await;
            bridge
                .complete_invocation(inv.request_id, "ok".into())
                .await;
        });

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Invocation-Type", "RequestResponse")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn invoke_passes_client_context_to_runtime() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let bridge = runtime_bridge.clone();
        let handle = tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            let ctx = inv.client_context.clone();
            bridge
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                .await;
            bridge
                .complete_invocation(inv.request_id, "done".into())
                .await;
            ctx
        });

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Client-Context", "eyJ0ZXN0IjogdHJ1ZX0=")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ctx = handle.await.unwrap();
        assert_eq!(ctx, Some("eyJ0ZXN0IjogdHJ1ZX0=".to_string()));
    }

    #[tokio::test]
    async fn invoke_oversized_response_returns_413() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let app = invoke_routes().with_state(state);

        // Spawn a fake runtime that responds with a payload exceeding 6,291,556 bytes.
        let bridge = runtime_bridge.clone();
        tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            bridge
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                .await;
            let oversized_body = "x".repeat(6_291_557);
            bridge
                .complete_invocation(inv.request_id, oversized_body)
                .await;
        });

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            resp.headers().get("X-Amz-Function-Error").unwrap().to_str().unwrap(),
            "Unhandled"
        );
        // Verify AWS-compatible headers are still present
        let req_id = resp.headers().get("X-Amz-Request-Id").unwrap().to_str().unwrap();
        uuid::Uuid::parse_str(req_id).expect("X-Amz-Request-Id should be a valid UUID");
        assert_eq!(
            resp.headers().get("X-Amz-Executed-Version").unwrap().to_str().unwrap(),
            "$LATEST"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["errorType"], "ResponseSizeTooLarge");
        assert!(json["errorMessage"]
            .as_str()
            .unwrap()
            .contains("6291557 bytes"));
        assert!(json["errorMessage"]
            .as_str()
            .unwrap()
            .contains("6291556 bytes"));
    }

    #[tokio::test]
    async fn invoke_response_at_limit_returns_200() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let app = invoke_routes().with_state(state);

        // Respond with exactly 6,291,556 bytes — should succeed.
        let bridge = runtime_bridge.clone();
        tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            bridge
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx)
                .await;
            let body = "x".repeat(6_291_556);
            bridge.complete_invocation(inv.request_id, body).await;
        });

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get("X-Amz-Function-Error").is_none());
    }

    #[tokio::test]
    async fn invoke_timeout_cleans_up_pending_invocation() {
        // Simulate a container that picks up the invocation via /next but
        // never responds — the timeout should clean up the pending state.
        let (state, _shutdown_tx) = test_state_with_function("my-func", 1).await;
        let runtime_bridge = state.runtime_bridge.clone();

        // Spawn a "container" that picks up the invocation and stores it as
        // pending (simulating what /next does), but never calls /response.
        let bridge = runtime_bridge.clone();
        tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            bridge
                .store_pending(
                    inv.request_id,
                    "my-func".into(),
                    Some("test-container-id".into()),
                    inv.response_tx,
                )
                .await;
            // Intentionally never respond — simulates a function that hangs.
        });

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Verify timeout response (200 with Unhandled, per AWS convention).
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("X-Amz-Function-Error")
                .unwrap()
                .to_str()
                .unwrap(),
            "Unhandled"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["errorMessage"]
            .as_str()
            .unwrap()
            .contains("Task timed out after 1 seconds"));
    }

    #[tokio::test]
    async fn invoke_timeout_with_dispatched_invocation_sends_timeout_result() {
        // Verify that when a dispatched (pending) invocation times out,
        // the InvocationResult::Timeout is sent on the response channel.
        let (state, _shutdown_tx) = test_state_with_function("my-func", 1).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        let bridge = runtime_bridge.clone();
        tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            let request_id = inv.request_id;

            // Store pending with a known container ID.
            bridge
                .store_pending(
                    request_id,
                    "my-func".into(),
                    Some("ctr-timeout-test".into()),
                    inv.response_tx,
                )
                .await;

            // Wait for the timeout to fire, then check the pending was removed.
            tokio::time::sleep(Duration::from_secs(2)).await;

            // After timeout, timeout_invocation should have already been called
            // by the invoke handler. Attempting to complete should fail (not found).
            let (was_found, _) = bridge.complete_invocation(request_id, "late".into()).await;
            let _ = result_tx.send(was_found);
        });

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("X-Amz-Function-Error")
                .unwrap()
                .to_str()
                .unwrap(),
            "Unhandled"
        );

        // The late complete_invocation should return false because the
        // pending invocation was already cleaned up by timeout_invocation.
        let was_found = result_rx.await.unwrap();
        assert!(!was_found, "pending invocation should have been removed by timeout");
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

    #[tokio::test]
    async fn invoke_returns_429_when_max_containers_reached() {
        use crate::types::FunctionConfig;
        use std::path::PathBuf;

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
            max_containers: 1, // only 1 container allowed
            docker_network: "localfunctions".into(),
            max_body_size: 6 * 1024 * 1024,
            log_format: crate::config::LogFormat::Text,
            pull_images: false,
            init_timeout: 10,
            container_acquire_timeout: 0, // no wait — reject immediately
            forward_aws_credentials: true,
            mount_aws_credentials: false,
            max_async_body_size: 256 * 1024,
        };
        let docker = bollard::Docker::connect_with_local_defaults().unwrap();

        let mut functions_map = HashMap::new();
        functions_map.insert(
            "my-func".to_string(),
            FunctionConfig {
                name: "my-func".into(),
                runtime: "python3.12".into(),
                handler: "main.handler".into(),
                code_path: PathBuf::from("/tmp/code"),
                timeout: 30,
                memory_size: 128,
                ephemeral_storage_mb: 512,
                environment: HashMap::new(),
                image: None,
                image_uri: None,
                reserved_concurrent_executions: None,
                architecture: "x86_64".into(),
            },
        );
        let functions = FunctionsConfig {
            functions: functions_map,
            runtime_images: HashMap::new(),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let mut senders = HashMap::new();
        senders.insert("my-func".to_string(), tx);
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let runtime_bridge = Arc::new(RuntimeBridge::new(senders, receivers, shutdown_rx));
        let container_registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let container_manager = Arc::new(ContainerManager::new(
            docker.clone(),
            HashMap::new(),
            "localfunctions".into(),
            9601,
            "us-east-1".into(),
            container_registry.clone(),
            1, // 1 slot
            CredentialForwardingConfig::default(),
        ));

        // Fill the only slot with a Busy container so no idle container is
        // available and the cold-start path triggers the semaphore check.
        container_manager
            .insert_test_container(
                "busy-container".into(),
                "my-func".into(),
                crate::types::ContainerState::Busy,
            )
            .await;

        let state = AppState {
            config: Arc::new(config),
            container_registry,
            container_manager,
            docker,
            functions: Arc::new(functions),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            runtime_bridge,
            metrics: Arc::new(crate::metrics::MetricsCollector::new()),
        };

        let app = crate::server::invoke_router(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"].as_str().unwrap(), "TooManyRequestsException");
    }

    #[test]
    fn generate_xray_trace_id_has_correct_format() {
        let trace_id = generate_xray_trace_id();

        // Must start with "Root=1-"
        assert!(trace_id.starts_with("Root=1-"), "trace_id: {}", trace_id);
        // Must contain ";Parent=" and ";Sampled=1"
        assert!(trace_id.contains(";Parent="), "trace_id: {}", trace_id);
        assert!(trace_id.ends_with(";Sampled=1"), "trace_id: {}", trace_id);

        // Parse out the components
        let root_part = trace_id.split(";Parent=").next().unwrap();
        let root_value = root_part.strip_prefix("Root=1-").unwrap();
        let parts: Vec<&str> = root_value.split('-').collect();
        assert_eq!(parts.len(), 2, "Root should have timestamp-id: {}", root_value);

        // Timestamp: 8 hex chars
        assert_eq!(parts[0].len(), 8, "timestamp hex: {}", parts[0]);
        assert!(u32::from_str_radix(parts[0], 16).is_ok());

        // Root ID: 24 hex chars (96 bits)
        assert_eq!(parts[1].len(), 24, "root id hex: {}", parts[1]);
        assert!(u128::from_str_radix(parts[1], 16).is_ok());

        // Parent ID: 16 hex chars (64 bits)
        let parent_part = trace_id
            .split(";Parent=")
            .nth(1)
            .unwrap()
            .split(";Sampled=")
            .next()
            .unwrap();
        assert_eq!(parent_part.len(), 16, "parent id hex: {}", parent_part);
        assert!(u64::from_str_radix(parent_part, 16).is_ok());
    }

    #[test]
    fn generate_xray_trace_id_is_unique() {
        let id1 = generate_xray_trace_id();
        let id2 = generate_xray_trace_id();
        assert_ne!(id1, id2);
    }
}
