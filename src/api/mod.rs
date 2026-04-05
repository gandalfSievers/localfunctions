mod eventstream;
pub(crate) mod function_url;
mod sample_events;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, info_span, warn, Instrument};
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

use crate::extensions::ExtensionEventType;
use crate::function::validate_function_name;
use crate::server::AppState;

// ---------------------------------------------------------------------------
// Qualifier parsing
// ---------------------------------------------------------------------------

/// Query parameters accepted by the Invoke and GetFunction APIs.
#[derive(Debug, Deserialize, Default)]
struct QualifierParams {
    #[serde(rename = "Qualifier")]
    qualifier: Option<String>,
}

/// Parse a function name that may contain a colon-separated qualifier
/// (e.g. `my-function:PROD`) and merge with an optional `?Qualifier=` query
/// parameter. The query parameter takes precedence if both are present.
///
/// Returns `(base_name, qualifier)` where qualifier is `None` for unqualified
/// requests and `Some(q)` when a qualifier was specified.
fn parse_qualifier(raw_name: &str, query_qualifier: Option<&str>) -> (String, Option<String>) {
    let (base_name, inline_qualifier) = match raw_name.split_once(':') {
        Some((name, qual)) => (name.to_string(), Some(qual.to_string())),
        None => (raw_name.to_string(), None),
    };
    let qualifier = query_qualifier
        .map(|q| q.to_string())
        .or(inline_qualifier);
    (base_name, qualifier)
}

/// Validate that a qualifier is supported. Only `$LATEST` (or absent) is valid
/// for this local emulator — all other qualifiers are treated as not found.
///
/// Returns `Ok(())` if the qualifier is acceptable, or `Err(ServiceError)` with
/// a `ResourceNotFoundException` for unrecognized qualifiers.
fn validate_qualifier(qualifier: &Option<String>, function_name: &str) -> Result<(), ServiceError> {
    match qualifier.as_deref() {
        None | Some("$LATEST") => Ok(()),
        Some(q) => Err(ServiceError::ResourceNotFound(format!(
            "Function not found: arn:aws:lambda:us-east-1:000000000000:function:{}:{}",
            function_name, q
        ))),
    }
}

/// Maximum size of the decoded client context in bytes (AWS limit).
const CLIENT_CONTEXT_MAX_BYTES: usize = 3_583;

/// Validate the `X-Amz-Client-Context` header value.
///
/// The value must be valid base64, decode to valid JSON, and the decoded
/// payload must not exceed 3,583 bytes (AWS limit).
fn validate_client_context(value: &str) -> Result<(), ServiceError> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|e| {
            ServiceError::InvalidRequestContent(format!(
                "Could not base64 decode client context: {e}"
            ))
        })?;

    if decoded.len() > CLIENT_CONTEXT_MAX_BYTES {
        return Err(ServiceError::InvalidRequestContent(format!(
            "Client context must be no more than {CLIENT_CONTEXT_MAX_BYTES} bytes when decoded, got {} bytes",
            decoded.len()
        )));
    }

    serde_json::from_slice::<serde_json::Value>(&decoded).map_err(|e| {
        ServiceError::InvalidRequestContent(format!(
            "Client context must be valid JSON when decoded: {e}"
        ))
    })?;

    Ok(())
}

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
        .route(
            "/2021-11-15/functions/:function_name/response-streaming-invocations",
            post(invoke_function_streaming),
        )
        // Sample event payload generation endpoints.
        .route("/admin/sample-events", get(sample_events::list_sample_events))
        .route("/admin/sample-events/:source", get(sample_events::get_sample_event))
        // Function URL endpoints — placed last so static routes take priority.
        .route(
            "/:function_name",
            axum::routing::any(function_url::function_url_handler),
        )
        .route(
            "/:function_name/*path",
            axum::routing::any(function_url::function_url_handler),
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
    Path(raw_function_name): Path<String>,
    Query(params): Query<QualifierParams>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    // Parse qualifier from function name (colon-separated) or query parameter.
    let (function_name, qualifier) =
        parse_qualifier(&raw_function_name, params.qualifier.as_deref());

    // Validate function name.
    if let Err(e) = validate_function_name(&function_name) {
        let err = ServiceError::InvalidRequestContent(e.to_string());
        return (
            err.status_code(),
            HeaderMap::new(),
            err.to_aws_response().to_json_bytes(),
        );
    }

    // Validate qualifier — only $LATEST is supported.
    if let Err(err) = validate_qualifier(&qualifier, &function_name) {
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

    if !f.layers.is_empty() {
        let layer_arns: Vec<String> = f
            .layers
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let layer_name = p.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| format!("layer-{}", i));
                format!("arn:aws:lambda:local:000000000000:layer:{}:{}", layer_name, i + 1)
            })
            .collect();
        config["Layers"] = serde_json::json!(
            layer_arns.iter().map(|arn| serde_json::json!({"Arn": arn})).collect::<Vec<_>>()
        );
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
/// - `X-Amz-Invocation-Type`: `RequestResponse` (default), `Event` (async), or `DryRun` (validation only)
/// - `X-Amz-Log-Type`: `None` (default) or `Tail` — returns last 4KB of logs base64-encoded in `X-Amz-Log-Result`
/// - `X-Amz-Client-Context`: passed through to the runtime
///
/// Returns the function response body on success, or an AWS-format error.
/// For `Event` invocation type, returns 202 Accepted immediately and executes
/// the function in the background.
/// For `DryRun` invocation type, validates the function exists and returns
/// 204 No Content without executing the function.
async fn invoke_function(
    State(state): State<AppState>,
    Path(raw_function_name): Path<String>,
    Query(params): Query<QualifierParams>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let request_id = Uuid::new_v4();

    // Parse qualifier from function name (colon-separated) or query parameter.
    let (function_name, qualifier) =
        parse_qualifier(&raw_function_name, params.qualifier.as_deref());

    // Validate qualifier — only $LATEST is supported.
    if let Err(err) = validate_qualifier(&qualifier, &function_name) {
        return (
            err.status_code(),
            invoke_base_headers(request_id),
            err.to_aws_response().to_json_bytes(),
        );
    }

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

    // Check X-Amz-Log-Type — Tail returns the last 4KB of logs base64-encoded.
    let log_type_tail = match headers.get("X-Amz-Log-Type").and_then(|v| v.to_str().ok()) {
        Some("Tail") => true,
        Some("None") | None => false,
        Some(other) => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Unsupported log type '{}'. Supported types: None, Tail.",
                other
            ));
            return (err.status_code(), invoke_base_headers(request_id), err.to_aws_response().to_json_bytes());
        }
    };

    // Check X-Amz-Invocation-Type — RequestResponse (default), Event, and DryRun are supported.
    let invocation_type = headers.get("X-Amz-Invocation-Type").and_then(|v| v.to_str().ok());

    // DryRun: validate function exists and return 204 without executing.
    if invocation_type == Some("DryRun") {
        if state.functions.functions.get(&function_name).is_none() {
            let err = ServiceError::ResourceNotFound(function_name);
            return (err.status_code(), invoke_base_headers(request_id), err.to_aws_response().to_json_bytes());
        }
        return (StatusCode::NO_CONTENT, invoke_base_headers(request_id), Vec::new());
    }

    let is_event_invocation = match invocation_type {
        Some("RequestResponse") | None => false,
        Some("Event") => true,
        Some(other) => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Unsupported invocation type '{}'. Supported types: RequestResponse, Event, DryRun.",
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

        // Strip client context — it is not forwarded for Event invocations.
        let mut headers = headers;
        headers.remove("X-Amz-Client-Context");

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

    // Extract and validate the client context header.
    // Client context is only forwarded for synchronous (RequestResponse) invocations.
    let client_context = if is_event_invocation {
        None
    } else if let Some(raw) = headers.get("X-Amz-Client-Context").and_then(|v| v.to_str().ok()) {
        if let Err(err) = validate_client_context(raw) {
            return (
                err.status_code(),
                invoke_base_headers(request_id),
                err.to_aws_response().to_json_bytes(),
            );
        }
        Some(raw.to_string())
    } else {
        None
    };

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

    let mut response = match result {
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
            let cid = crashed_container.unwrap_or_else(|| container_id.clone());
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
            let cid = timed_out_container.unwrap_or_else(|| container_id.clone());
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

    // If LogType is Tail, collect the last 4KB of logs and add as a base64-encoded
    // response header. Only applies to synchronous (RequestResponse) invocations.
    if log_type_tail {
        const LOG_TAIL_MAX_BYTES: usize = 4096;
        let logs = state
            .container_manager
            .get_container_logs(&container_id, LOG_TAIL_MAX_BYTES)
            .await;
        if !logs.is_empty() {
            let encoded = base64::engine::general_purpose::STANDARD.encode(logs.as_bytes());
            if let Ok(value) = encoded.parse() {
                response.1.insert("X-Amz-Log-Result", value);
            }
        }
    }

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
///
/// Failed invocations are retried up to `max_retry_attempts` times (default 2,
/// matching AWS Lambda behavior). Backoff between retries follows the AWS
/// pattern: 1 minute after the first failure, 2 minutes after the second.
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

        // Look up per-function config for retries and destinations.
        let (max_retries, on_success, on_failure) = state
            .functions
            .functions
            .get(&function_name)
            .map(|f| (f.max_retry_attempts, f.on_success.clone(), f.on_failure.clone()))
            .unwrap_or((2, None, None));

        // AWS backoff pattern: 1 minute after first failure, 2 minutes after second.
        let backoff_durations = [
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(120),
        ];

        let mut attempt = 0u32;
        loop {
            let rid = if attempt == 0 {
                request_id
            } else {
                Uuid::new_v4()
            };

            let (status, _headers, resp_body) = invoke_function_inner(
                state.clone(),
                function_name.clone(),
                headers.clone(),
                body.clone(),
                rid,
            )
            .await;

            if status == StatusCode::OK {
                info!(attempt, "async invocation completed successfully");

                // Route to on_success destination if configured.
                if let Some(ref dest) = on_success {
                    route_to_destination(DestinationRouteParams {
                        state: &state,
                        destination_function: dest,
                        source_function: &function_name,
                        request_id: &request_id,
                        condition: "Success",
                        approximate_invoke_count: attempt + 1,
                        request_payload: &body,
                        response_payload: Some(&resp_body),
                        error_status_code: None,
                    })
                    .await;
                }
                return;
            }

            attempt += 1;
            if attempt > max_retries {
                let body_str = String::from_utf8_lossy(&resp_body);
                error!(
                    %status,
                    attempt = attempt - 1,
                    max_retries,
                    function = %function_name,
                    response = %body_str,
                    "async invocation failed after all retries exhausted"
                );

                // Route to on_failure destination if configured.
                if let Some(ref dest) = on_failure {
                    route_to_destination(DestinationRouteParams {
                        state: &state,
                        destination_function: dest,
                        source_function: &function_name,
                        request_id: &request_id,
                        condition: "RetriesExhausted",
                        approximate_invoke_count: attempt,
                        request_payload: &body,
                        response_payload: Some(&resp_body),
                        error_status_code: Some(status.as_u16()),
                    })
                    .await;
                }
                return;
            }

            let backoff = backoff_durations[(attempt - 1) as usize];
            warn!(
                %status,
                attempt,
                max_retries,
                function = %function_name,
                backoff_secs = backoff.as_secs(),
                "async invocation failed, retrying after backoff"
            );
            tokio::time::sleep(backoff).await;
        }
    })
}

/// Parameters for routing an async invocation result to a destination function.
struct DestinationRouteParams<'a> {
    state: &'a AppState,
    destination_function: &'a str,
    source_function: &'a str,
    request_id: &'a Uuid,
    condition: &'a str,
    approximate_invoke_count: u32,
    request_payload: &'a Bytes,
    response_payload: Option<&'a [u8]>,
    error_status_code: Option<u16>,
}

/// Build the standard Lambda destination event wrapper and invoke the
/// destination function asynchronously (fire-and-forget via Event type).
///
/// The wrapper follows the AWS Lambda destination event format:
/// <https://docs.aws.amazon.com/lambda/latest/dg/invocation-async.html#invocation-async-destinations>
async fn route_to_destination(params: DestinationRouteParams<'_>) {
    let DestinationRouteParams {
        state,
        destination_function,
        source_function,
        request_id,
        condition,
        approximate_invoke_count,
        request_payload,
        response_payload,
        error_status_code,
    } = params;
    let function_arn = format!(
        "arn:aws:lambda:{}:{}:function:{}",
        state.config.region, state.config.account_id, source_function
    );

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    // Format as ISO 8601 / RFC 3339 with milliseconds (matching AWS format).
    // We avoid pulling in chrono by formatting manually from the unix timestamp.
    let timestamp = {
        // Days from epoch, accounting for leap years.
        let days = secs / 86400;
        let time_of_day = secs % 86400;
        let (year, month, day) = epoch_days_to_ymd(days);
        let hour = time_of_day / 3600;
        let minute = (time_of_day % 3600) / 60;
        let second = time_of_day % 60;
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            year, month, day, hour, minute, second, millis
        )
    };

    // Parse request payload as JSON value (fallback to raw string).
    let req_payload_value: serde_json::Value =
        serde_json::from_slice(request_payload).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(request_payload).into_owned())
        });

    // Parse response payload as JSON value (fallback to raw string).
    let resp_payload_value: serde_json::Value = response_payload
        .map(|b| {
            serde_json::from_slice(b).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
            })
        })
        .unwrap_or(serde_json::Value::Null);

    let mut dest_event = serde_json::json!({
        "version": "1.0",
        "timestamp": timestamp,
        "requestContext": {
            "requestId": request_id.to_string(),
            "functionArn": function_arn,
            "condition": condition,
            "approximateInvokeCount": approximate_invoke_count
        },
        "requestPayload": req_payload_value,
        "responsePayload": resp_payload_value
    });

    // For failure destinations, include responseContext with error details.
    if let Some(status_code) = error_status_code {
        dest_event["responseContext"] = serde_json::json!({
            "statusCode": status_code,
            "executedVersion": "$LATEST",
            "functionError": "Unhandled"
        });
    }

    let dest_body = match serde_json::to_vec(&dest_event) {
        Ok(b) => b,
        Err(e) => {
            error!(
                destination = %destination_function,
                source = %source_function,
                error = %e,
                "failed to serialize destination event"
            );
            return;
        }
    };

    let mut dest_headers = HeaderMap::new();
    dest_headers.insert("X-Amz-Invocation-Type", "Event".parse().unwrap());

    let dest_request_id = Uuid::new_v4();
    let span = info_span!(
        "destination_invocation",
        %dest_request_id,
        source = %source_function,
        destination = %destination_function,
        %condition
    );

    info!(
        destination = %destination_function,
        source = %source_function,
        %condition,
        "routing async invocation result to destination"
    );

    // Fire-and-forget: spawn the destination invocation so we don't block
    // the current task. The destination invocation itself goes through the
    // normal async path (Event type) so it gets its own retry semantics.
    let state_clone = state.clone();
    let dest_fn = destination_function.to_string();
    tokio::spawn(
        async move {
            let (_status, _headers, _body) = invoke_function_inner(
                state_clone,
                dest_fn,
                dest_headers,
                Bytes::from(dest_body),
                dest_request_id,
            )
            .await;
        }
        .instrument(span),
    );
}

/// Convert days since the Unix epoch to (year, month, day).
fn epoch_days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm based on the civil-from-days formula (Howard Hinnant).
    let days = days + 719_468; // shift epoch from 1970-01-01 to 0000-03-01
    let era = days / 146_097;
    let doe = days % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// InvokeWithResponseStream
// ---------------------------------------------------------------------------

/// POST /2021-11-15/functions/{FunctionName}/response-streaming-invocations
///
/// Invokes a Lambda function with response streaming. The response is returned
/// as chunked transfer encoding using the AWS event stream binary protocol.
///
/// Supports payloads larger than the 6MB synchronous limit. Errors mid-stream
/// are communicated via event stream error frames.
async fn invoke_function_streaming(
    State(state): State<AppState>,
    Path(raw_function_name): Path<String>,
    Query(params): Query<QualifierParams>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let request_id = Uuid::new_v4();

    let (function_name, qualifier) =
        parse_qualifier(&raw_function_name, params.qualifier.as_deref());

    if let Err(err) = validate_qualifier(&qualifier, &function_name) {
        return streaming_error_response(request_id, err.status_code(), &err.to_aws_response());
    }

    let span = info_span!("streaming_invocation", %request_id, function = %function_name);

    invoke_function_streaming_inner(state, function_name, headers, body, request_id)
        .instrument(span)
        .await
}

async fn invoke_function_streaming_inner(
    state: AppState,
    function_name: String,
    headers: HeaderMap,
    body: Bytes,
    request_id: Uuid,
) -> axum::response::Response {
    use axum::body::Body;
    use tokio_stream::wrappers::ReceiverStream;

    let invoke_start = std::time::Instant::now();
    let mut is_cold_start = false;

    // Validate function name.
    if let Err(e) = validate_function_name(&function_name) {
        let err = ServiceError::InvalidRequestContent(e.to_string());
        return streaming_error_response(request_id, err.status_code(), &err.to_aws_response());
    }

    debug!(
        payload_size = body.len(),
        payload = %String::from_utf8_lossy(&body),
        "streaming invoke request"
    );

    // Reject if shutting down.
    if state.is_shutting_down() {
        let err = ServiceError::ServiceException(
            "Service is shutting down, not accepting new invocations".into(),
        );
        return streaming_error_response(request_id, err.status_code(), &err.to_aws_response());
    }

    // Streaming only supports RequestResponse invocation type.
    if let Some(inv_type) = headers.get("X-Amz-Invocation-Type").and_then(|v| v.to_str().ok()) {
        if inv_type != "RequestResponse" {
            let err = ServiceError::InvalidRequestContent(format!(
                "InvokeWithResponseStream only supports RequestResponse invocation type, got '{}'",
                inv_type
            ));
            return streaming_error_response(request_id, err.status_code(), &err.to_aws_response());
        }
    }

    // Look up the function configuration.
    let function_config = match state.functions.functions.get(&function_name) {
        Some(config) => config,
        None => {
            let err = ServiceError::ResourceNotFound(function_name);
            return streaming_error_response(request_id, err.status_code(), &err.to_aws_response());
        }
    };

    // Check per-function concurrency limit.
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
            let body = crate::types::AwsErrorResponse {
                Type: "User".into(),
                Message: "Rate Exceeded.".into(),
            };
            return streaming_error_response(request_id, StatusCode::TOO_MANY_REQUESTS, &body);
        }
    }

    let timeout_secs = function_config.timeout;

    // Extract and validate the client context header.
    let client_context = if let Some(raw) = headers.get("X-Amz-Client-Context").and_then(|v| v.to_str().ok()) {
        if let Err(err) = validate_client_context(raw) {
            return streaming_error_response(request_id, err.status_code(), &err.to_aws_response());
        }
        Some(raw.to_string())
    } else {
        None
    };

    // Propagate or generate trace ID.
    let trace_id = headers
        .get("X-Amzn-Trace-Id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(generate_xray_trace_id);

    info!(trace_id = %trace_id, "trace context");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    // Acquire a container (same logic as synchronous invoke).
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
                return streaming_error_response(request_id, err.status_code(), &err.to_aws_response());
            }

            is_cold_start = true;
            debug!("no warm container available, cold starting");
            let cold_id = match state.container_manager.create_and_start(function_config).await {
                Ok(id) => id,
                Err(e) => {
                    state.container_manager.release_container_slot();
                    let err = ServiceError::ServiceException(e.to_string());
                    return streaming_error_response(request_id, err.status_code(), &err.to_aws_response());
                }
            };

            state
                .container_manager
                .set_state(&cold_id, ContainerState::Busy)
                .await;

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
                    let (status, _headers, body) = bootstrap_failure_response(
                        &state, &cold_id, &function_name, false, exit_code, invoke_base_headers(request_id),
                    ).await;
                    return axum::response::Response::builder()
                        .status(status)
                        .header("X-Amz-Request-Id", request_id.to_string())
                        .body(Body::from(body))
                        .unwrap();
                }
                BootstrapOutcome::Timeout => {
                    let (status, _headers, body) = bootstrap_failure_response(
                        &state, &cold_id, &function_name, true, None, invoke_base_headers(request_id),
                    ).await;
                    return axum::response::Response::builder()
                        .status(status)
                        .header("X-Amz-Request-Id", request_id.to_string())
                        .body(Body::from(body))
                        .unwrap();
                }
            }
        }
    };

    info!(
        payload_size = body.len(),
        timeout_secs,
        container_id = %container_id,
        "invoking function (streaming)"
    );

    // Start streaming container logs.
    let log_handle = state
        .container_manager
        .stream_container_logs(&container_id, &function_name, &request_id.to_string());

    // Submit streaming invocation.
    let mut stream_rx = match state
        .runtime_bridge
        .submit_streaming_invocation(&function_name, request_id, body, deadline, Some(trace_id), client_context)
        .await
    {
        Ok(rx) => rx,
        Err(e) => {
            log_handle.abort();
            state.container_manager.release_container(&container_id).await;
            let err = ServiceError::ServiceException(e.to_string());
            return streaming_error_response(request_id, StatusCode::BAD_GATEWAY, &err.to_aws_response());
        }
    };

    // Check X-Amz-Log-Type for tail logging.
    let log_type_tail = matches!(
        headers.get("X-Amz-Log-Type").and_then(|v| v.to_str().ok()),
        Some("Tail")
    );

    // Build a streaming response. We read from stream_rx with the function
    // timeout and encode each chunk as an AWS event stream PayloadChunk.
    let state_clone = state.clone();
    let container_id_clone = container_id.clone();
    let function_name_clone = function_name.clone();

    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);

    // Spawn a task to read chunks and forward as event stream frames.
    tokio::spawn(async move {
        let mut had_error = false;
        let mut error_code = String::new();
        let mut error_details = String::new();

        loop {
            match tokio::time::timeout_at(deadline, stream_rx.recv()).await {
                Ok(Some(chunk)) => match chunk {
                    crate::types::StreamChunk::Data(data) => {
                        let frame = eventstream::encode_payload_chunk(&data);
                        if body_tx.send(Ok(Bytes::from(frame))).await.is_err() {
                            break; // Client disconnected
                        }
                    }
                    crate::types::StreamChunk::Error { error_type, error_message } => {
                        had_error = true;
                        error_code = error_type.clone();
                        error_details = error_message.clone();
                        // Send error frame
                        let frame = eventstream::encode_error(&error_type, &error_message);
                        let _ = body_tx.send(Ok(Bytes::from(frame))).await;
                        break;
                    }
                    crate::types::StreamChunk::Complete => {
                        break;
                    }
                },
                Ok(None) => {
                    // Channel closed — container may have crashed.
                    had_error = true;
                    error_code = "ServiceException".into();
                    error_details = "Container exited without completing stream".into();
                    let frame = eventstream::encode_error(&error_code, &error_details);
                    let _ = body_tx.send(Ok(Bytes::from(frame))).await;
                    break;
                }
                Err(_) => {
                    // Timeout
                    had_error = true;
                    error_code = "TimeoutError".into();
                    error_details = format!("Task timed out after {} seconds", timeout_secs);
                    let frame = eventstream::encode_error(&error_code, &error_details);
                    let _ = body_tx.send(Ok(Bytes::from(frame))).await;

                    // Kill the container.
                    let cid = container_id_clone.clone();
                    let mgr = state_clone.container_manager.clone();
                    tokio::spawn(async move {
                        let _ = mgr.stop_and_remove(&cid, Duration::from_secs(2)).await;
                    });
                    break;
                }
            }
        }

        // Send InvokeComplete frame.
        let log_result = if log_type_tail {
            const LOG_TAIL_MAX_BYTES: usize = 4096;
            let logs = state_clone
                .container_manager
                .get_container_logs(&container_id_clone, LOG_TAIL_MAX_BYTES)
                .await;
            if !logs.is_empty() {
                base64::engine::general_purpose::STANDARD.encode(logs.as_bytes())
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        let complete_frame = eventstream::encode_invoke_complete(
            &error_code,
            &error_details,
            &log_result,
        );
        let _ = body_tx.send(Ok(Bytes::from(complete_frame))).await;

        // Stop log streaming.
        log_handle.abort();

        // Record metrics.
        state_clone.metrics.record_invocation(
            &function_name_clone,
            invoke_start.elapsed(),
            had_error,
            is_cold_start,
        );
    });

    // Build the streaming HTTP response.
    let stream = ReceiverStream::new(body_rx);
    let response_body = Body::from_stream(stream);

    let mut resp_headers = invoke_base_headers(request_id);
    resp_headers.insert(
        "Content-Type",
        "application/vnd.amazon.eventstream".parse().unwrap(),
    );

    let mut response = axum::response::Response::builder()
        .status(StatusCode::OK)
        .body(response_body)
        .unwrap();

    *response.headers_mut() = resp_headers;

    response
}

/// Build an error response for the streaming endpoint (pre-stream errors).
fn streaming_error_response(
    request_id: Uuid,
    status: StatusCode,
    body: &impl Serialize,
) -> axum::response::Response {
    let body_bytes = serde_json::to_vec(body).unwrap_or_default();
    axum::response::Response::builder()
        .status(status)
        .header("X-Amz-Request-Id", request_id.to_string())
        .header("X-Amz-Executed-Version", "$LATEST")
        .body(axum::body::Body::from(body_bytes))
        .unwrap()
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
// Shared container acquisition (used by Invoke API and Function URL)
// ---------------------------------------------------------------------------

/// Acquire a container for the given function, reusing a warm one or cold-
/// starting a new one.  Returns the container ID on success or an HTTP error
/// tuple that the caller can return directly.
pub(crate) async fn acquire_container(
    state: &AppState,
    function_config: &crate::types::FunctionConfig,
    request_id: &Uuid,
) -> Result<String, (StatusCode, HeaderMap, axum::body::Body)> {
    use axum::body::Body;

    let function_name = &function_config.name;

    if let Some(id) = state.container_manager.claim_idle_container(function_name).await {
        debug!(container_id = %id, "warm container claimed");
        return Ok(id);
    }

    let acquire_timeout = Duration::from_secs(state.config.container_acquire_timeout);
    if !state.container_manager.acquire_container_slot(acquire_timeout).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            HeaderMap::new(),
            Body::from("Rate exceeded: max concurrent containers reached"),
        ));
    }

    debug!("no warm container available, cold starting");
    let cold_id = match state.container_manager.create_and_start(function_config).await {
        Ok(id) => id,
        Err(e) => {
            state.container_manager.release_container_slot();
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Body::from(format!("Failed to start container: {e}")),
            ));
        }
    };

    state
        .container_manager
        .set_state(&cold_id, ContainerState::Busy)
        .await;

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
            if state.runtime_bridge.is_ready(&cold_id).await {
                BootstrapOutcome::Ready
            } else {
                BootstrapOutcome::Timeout
            }
        }
    };

    match outcome {
        BootstrapOutcome::Ready => Ok(cold_id),
        BootstrapOutcome::Exited(exit_code) => {
            let (status, _, body) = bootstrap_failure_response(
                state, &cold_id, function_name, false, exit_code, invoke_base_headers(*request_id),
            ).await;
            Err((status, HeaderMap::new(), Body::from(body)))
        }
        BootstrapOutcome::Timeout => {
            let (status, _, body) = bootstrap_failure_response(
                state, &cold_id, function_name, true, None, invoke_base_headers(*request_id),
            ).await;
            Err((status, HeaderMap::new(), Body::from(body)))
        }
    }
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
        // Extensions API
        .route(
            "/2020-01-01/extension/register",
            post(extension_register),
        )
        .route(
            "/2020-01-01/extension/event/next",
            get(extension_event_next),
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
            // Destructure to separate response channels from the rest.
            let request_id = invocation.request_id;
            let payload = invocation.payload;
            let deadline = invocation.deadline;
            let trace_id = invocation.trace_id;
            let client_context = invocation.client_context;
            let response_tx = invocation.response_tx;
            let stream_tx = invocation.stream_tx;

            // Store the response channel so /response and /error can forward
            // results back to the original invoke caller.
            let dispatched_container_id = headers
                .get(HEADER_CONTAINER_ID)
                .and_then(|v| v.to_str().ok())
                .map(String::from);

            if let Some(stx) = stream_tx {
                // Streaming invocation — store the mpsc sender.
                state
                    .runtime_bridge
                    .store_streaming_pending(
                        request_id,
                        function_name.clone(),
                        dispatched_container_id,
                        stx,
                    )
                    .await;
            } else if let Some(tx) = response_tx {
                state
                    .runtime_bridge
                    .store_pending(request_id, function_name.clone(), dispatched_container_id, tx)
                    .await;
            }

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

            // Notify registered extensions of the INVOKE event.
            let arn_for_ext = format!(
                "arn:aws:lambda:{}:{}:function:{}",
                state.config.region, state.config.account_id, function_name
            );
            let trace_for_ext = trace_id.as_deref().unwrap_or("");
            state.extension_registry.notify_invoke(
                &function_name,
                &request_id.to_string(),
                &arn_for_ext,
                deadline_ms.parse().unwrap_or(0),
                trace_for_ext,
            ).await;

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
///
/// For streaming invocations, reads the body as a stream and forwards chunks
/// via the mpsc channel. For synchronous invocations, reads the full body
/// and sends via the oneshot channel.
async fn invocation_response(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    request: axum::extract::Request,
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

    // Check if this is a streaming invocation before consuming the body.
    let is_streaming = state.runtime_bridge.is_streaming_invocation(uuid).await;

    if is_streaming {
        return handle_streaming_runtime_response(state, uuid, &request_id, request).await;
    }

    // Non-streaming path: read full body.
    let body = match axum::body::to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            let err = ServiceError::ServiceException(format!("Failed to read body: {}", e));
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

/// Handle the streaming variant of the runtime `/response` endpoint.
///
/// Reads the incoming body as a stream and forwards each chunk to the
/// invoke handler via the mpsc channel.
async fn handle_streaming_runtime_response(
    state: AppState,
    uuid: Uuid,
    request_id: &str,
    request: axum::extract::Request,
) -> (StatusCode, Json<serde_json::Value>) {
    use futures_util::StreamExt;

    debug!(request_id = %uuid, "streaming invocation response received");

    // Take the streaming sender from pending invocations.
    let (stream_tx, container_id) = match state.runtime_bridge.take_streaming_sender(uuid).await {
        Some(pair) => pair,
        None => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Unknown streaming request_id: {}",
                request_id
            ));
            return (err.status_code(), Json(serde_json::to_value(err.to_aws_response()).unwrap()));
        }
    };

    // Stream the body chunks to the invoke handler.
    let mut body_stream = http_body_util::BodyStream::new(request.into_body());
    while let Some(chunk_result) = body_stream.next().await {
        match chunk_result {
            Ok(frame) => {
                let Some(chunk) = frame.into_data().ok() else {
                    continue; // Skip non-data frames (trailers)
                };
                if stream_tx.send(crate::types::StreamChunk::Data(chunk)).await.is_err() {
                    warn!(request_id = %uuid, "streaming receiver dropped mid-stream");
                    break;
                }
            }
            Err(e) => {
                warn!(request_id = %uuid, %e, "error reading streaming body chunk");
                let _ = stream_tx
                    .send(crate::types::StreamChunk::Error {
                        error_type: "StreamError".into(),
                        error_message: e.to_string(),
                    })
                    .await;
                break;
            }
        }
    }

    // Signal completion.
    let _ = stream_tx.send(crate::types::StreamChunk::Complete).await;

    // Release the container back to the idle pool.
    if let Some(ref cid) = container_id {
        debug!(container_id = %cid, "releasing container after streaming invocation");
        state.container_manager.release_container(cid).await;
    }

    (StatusCode::ACCEPTED, Json(serde_json::json!({"status": "OK"})))
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

// ---------------------------------------------------------------------------
// Extensions API
// ---------------------------------------------------------------------------

/// Header used by extensions to identify themselves after registration.
const HEADER_EXTENSION_ID: &str = "Lambda-Extension-Identifier";
/// Header sent by extensions during registration to provide their name.
const HEADER_EXTENSION_NAME: &str = "Lambda-Extension-Name";

/// Request body for extension registration.
#[derive(Debug, Deserialize)]
struct ExtensionRegisterRequest {
    events: Vec<ExtensionEventType>,
}

/// POST /2020-01-01/extension/register
///
/// Called by Lambda extensions during the INIT phase to register with the
/// Extensions API. The extension specifies which lifecycle events it wants
/// to receive (INVOKE, SHUTDOWN).
///
/// Required headers:
/// - `Lambda-Extension-Name`: Human-readable extension name
/// - `Lambda-Runtime-Function-Name`: Function this extension belongs to
///
/// Returns the extension identifier in the `Lambda-Extension-Identifier`
/// response header.
async fn extension_register(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Extract extension name from header.
    let extension_name = match headers
        .get(HEADER_EXTENSION_NAME)
        .and_then(|v| v.to_str().ok())
    {
        Some(name) => name.to_string(),
        None => {
            warn!(
                "extension /register called without {} header",
                HEADER_EXTENSION_NAME
            );
            let err = ServiceError::InvalidRequestContent(format!(
                "Missing required header: {}",
                HEADER_EXTENSION_NAME
            ));
            return (
                err.status_code(),
                HeaderMap::new(),
                err.to_aws_response().to_json_bytes(),
            );
        }
    };

    // Extract function name from header.
    let function_name = match headers
        .get(HEADER_FUNCTION_NAME)
        .and_then(|v| v.to_str().ok())
    {
        Some(name) => name.to_string(),
        None => {
            warn!(
                "extension /register called without {} header",
                HEADER_FUNCTION_NAME
            );
            let err = ServiceError::InvalidRequestContent(format!(
                "Missing required header: {}",
                HEADER_FUNCTION_NAME
            ));
            return (
                err.status_code(),
                HeaderMap::new(),
                err.to_aws_response().to_json_bytes(),
            );
        }
    };

    // Parse the request body for event subscriptions.
    let register_req: ExtensionRegisterRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Invalid registration body: {}",
                e
            ));
            return (
                err.status_code(),
                HeaderMap::new(),
                err.to_aws_response().to_json_bytes(),
            );
        }
    };

    if register_req.events.is_empty() {
        let err =
            ServiceError::InvalidRequestContent("events array must not be empty".into());
        return (
            err.status_code(),
            HeaderMap::new(),
            err.to_aws_response().to_json_bytes(),
        );
    }

    // Register the extension.
    let extension_id: Uuid = state
        .extension_registry
        .register(&extension_name, &function_name, register_req.events)
        .await;

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HEADER_EXTENSION_ID,
        extension_id.to_string().parse().unwrap(),
    );

    let response_body = serde_json::json!({
        "functionName": function_name,
        "functionVersion": "$LATEST",
        "handler": "",
    });

    (
        StatusCode::OK,
        response_headers,
        serde_json::to_vec(&response_body).unwrap_or_default(),
    )
}

/// GET /2020-01-01/extension/event/next
///
/// Called by registered Lambda extensions to long-poll for the next lifecycle
/// event. The extension identifies itself via the `Lambda-Extension-Identifier`
/// header.
///
/// Returns INVOKE events before each function invocation and a SHUTDOWN event
/// when the execution environment is shutting down.
async fn extension_event_next(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Extract extension identifier from header.
    let extension_id_str = match headers
        .get(HEADER_EXTENSION_ID)
        .and_then(|v| v.to_str().ok())
    {
        Some(id) => id.to_string(),
        None => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Missing required header: {}",
                HEADER_EXTENSION_ID
            ));
            return (
                err.status_code(),
                HeaderMap::new(),
                err.to_aws_response().to_json_bytes(),
            );
        }
    };

    let extension_id = match Uuid::parse_str(&extension_id_str) {
        Ok(id) => id,
        Err(_) => {
            let err = ServiceError::InvalidRequestContent(format!(
                "Invalid extension identifier: {}",
                extension_id_str
            ));
            return (
                err.status_code(),
                HeaderMap::new(),
                err.to_aws_response().to_json_bytes(),
            );
        }
    };

    // Verify the extension is registered.
    if !state.extension_registry.is_registered(extension_id).await {
        let err = ServiceError::InvalidRequestContent(format!(
            "Extension not registered: {}",
            extension_id
        ));
        return (
            err.status_code(),
            HeaderMap::new(),
            err.to_aws_response().to_json_bytes(),
        );
    }

    debug!(
        extension_id = %extension_id,
        "extension long-polling for next event"
    );

    // Long-poll for the next event.
    match state.extension_registry.next_event(extension_id).await {
        Some(event) => {
            let mut response_headers = HeaderMap::new();
            response_headers.insert(
                HEADER_EXTENSION_ID,
                extension_id.to_string().parse().unwrap(),
            );

            let body = serde_json::to_vec(&event).unwrap_or_default();
            (StatusCode::OK, response_headers, body)
        }
        None => {
            // Extension was deregistered or channel closed.
            let err = ServiceError::ServiceException(
                "Extension event channel closed".into(),
            );
            (
                err.status_code(),
                HeaderMap::new(),
                err.to_aws_response().to_json_bytes(),
            )
        }
    }
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
            hot_reload: true,
            hot_reload_debounce_ms: 500,
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
            extension_registry: Arc::new(crate::extensions::ExtensionRegistry::new(
                tokio::sync::watch::channel(false).1,
            )),
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
            hot_reload: true,
            hot_reload_debounce_ms: 500,
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
                layers: vec![],
                function_url_enabled: false,
                max_retry_attempts: 2,
                on_success: None,
                on_failure: None,
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
                layers: vec![],
                function_url_enabled: false,
                max_retry_attempts: 2,
                on_success: None,
                on_failure: None,
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
            extension_registry: Arc::new(crate::extensions::ExtensionRegistry::new(
                tokio::sync::watch::channel(false).1,
            )),
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
            response_tx: Some(resp_tx),
            stream_tx: None,
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
            response_tx: Some(resp_tx),
            stream_tx: None,
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
            response_tx: Some(resp_tx),
            stream_tx: None,
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
            response_tx: Some(resp_tx),
            stream_tx: None,
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
            response_tx: Some(resp_tx),
            stream_tx: None,
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
            response_tx: Some(resp_tx),
            stream_tx: None,
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
            hot_reload: true,
            hot_reload_debounce_ms: 500,
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
                layers: vec![],
                function_url_enabled: false,
                max_retry_attempts: 2,
                on_success: None,
                on_failure: None,
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
            extension_registry: Arc::new(crate::extensions::ExtensionRegistry::new(
                tokio::sync::watch::channel(false).1,
            )),
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
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
                    .header("X-Amz-Invocation-Type", "Bogus")
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
    async fn invoke_dryrun_returns_204_no_body() {
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

        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn invoke_dryrun_nonexistent_function_returns_404() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let app = invoke_routes().with_state(state);

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/no-such-func/invocations")
                    .header("X-Amz-Invocation-Type", "DryRun")
                    .body(Body::from("{}"))
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
    async fn invoke_event_type_returns_202_immediately() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        // Spawn a background handler to process the async invocation so it
        // doesn't hang after the test completes.
        tokio::spawn(async move {
            if let Some(inv) = runtime_bridge.next_invocation("my-func").await {
                runtime_bridge
                    .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
                    .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
    async fn invoke_rejects_invalid_base64_client_context() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Client-Context", "not-valid-base64!!!")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap()).unwrap();
        assert!(body["Message"].as_str().unwrap().contains("base64"));
    }

    #[tokio::test]
    async fn invoke_rejects_non_json_client_context() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;

        // Base64-encode "not json" → valid base64 but invalid JSON
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"not json");

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Client-Context", &encoded)
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap()).unwrap();
        assert!(body["Message"].as_str().unwrap().contains("valid JSON"));
    }

    #[tokio::test]
    async fn invoke_rejects_oversized_client_context() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;

        // Create a JSON payload that exceeds 3,583 bytes when decoded.
        let big_json = format!("{{\"data\":\"{}\"}}", "x".repeat(3_584));
        let encoded = base64::engine::general_purpose::STANDARD.encode(big_json.as_bytes());

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Client-Context", &encoded)
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap()).unwrap();
        assert!(body["Message"].as_str().unwrap().contains("3583"));
    }

    #[tokio::test]
    async fn invoke_event_type_ignores_client_context() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let runtime_bridge = state.runtime_bridge.clone();

        let bridge = runtime_bridge.clone();
        let handle = tokio::spawn(async move {
            let inv = bridge.next_invocation("my-func").await.unwrap();
            let ctx = inv.client_context.clone();
            bridge
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
                .await;
            bridge
                .complete_invocation(inv.request_id, "ok".into())
                .await;
            ctx
        });

        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Invocation-Type", "Event")
                    .header("X-Amz-Client-Context", "eyJ0ZXN0IjogdHJ1ZX0=")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Event invocations return 202 immediately.
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // The client context should be None for event invocations.
        let ctx = handle.await.unwrap();
        assert_eq!(ctx, None);
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
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
                .store_pending(inv.request_id, "my-func".into(), None, inv.response_tx.unwrap())
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
                    inv.response_tx.unwrap(),
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
                    inv.response_tx.unwrap(),
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
            hot_reload: true,
            hot_reload_debounce_ms: 500,
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
                layers: vec![],
                function_url_enabled: false,
                max_retry_attempts: 2,
                on_success: None,
                on_failure: None,
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
            extension_registry: Arc::new(crate::extensions::ExtensionRegistry::new(
                tokio::sync::watch::channel(false).1,
            )),
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

    // -- Qualifier parsing unit tests -----------------------------------------

    #[test]
    fn parse_qualifier_no_qualifier() {
        let (name, qual) = parse_qualifier("my-func", None);
        assert_eq!(name, "my-func");
        assert_eq!(qual, None);
    }

    #[test]
    fn parse_qualifier_inline_colon() {
        let (name, qual) = parse_qualifier("my-func:PROD", None);
        assert_eq!(name, "my-func");
        assert_eq!(qual, Some("PROD".into()));
    }

    #[test]
    fn parse_qualifier_query_param() {
        let (name, qual) = parse_qualifier("my-func", Some("STAGING"));
        assert_eq!(name, "my-func");
        assert_eq!(qual, Some("STAGING".into()));
    }

    #[test]
    fn parse_qualifier_query_param_overrides_inline() {
        let (name, qual) = parse_qualifier("my-func:DEV", Some("PROD"));
        assert_eq!(name, "my-func");
        assert_eq!(qual, Some("PROD".into()));
    }

    #[test]
    fn parse_qualifier_dollar_latest() {
        let (name, qual) = parse_qualifier("my-func:$LATEST", None);
        assert_eq!(name, "my-func");
        assert_eq!(qual, Some("$LATEST".into()));
    }

    #[test]
    fn validate_qualifier_accepts_none() {
        assert!(validate_qualifier(&None, "my-func").is_ok());
    }

    #[test]
    fn validate_qualifier_accepts_latest() {
        assert!(validate_qualifier(&Some("$LATEST".into()), "my-func").is_ok());
    }

    #[test]
    fn validate_qualifier_rejects_unknown() {
        let result = validate_qualifier(&Some("PROD".into()), "my-func");
        assert!(result.is_err());
    }

    // -- Qualifier integration tests (Invoke API) -----------------------------

    #[tokio::test]
    async fn invoke_with_latest_qualifier_query_param() {
        let app = invoke_routes().with_state(test_state());
        // $LATEST qualifier should pass through to normal function lookup (404 because no function)
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations?Qualifier=$LATEST")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        // Not found because the function doesn't exist, but qualifier is accepted
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn invoke_with_unknown_qualifier_returns_404() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/alpha-func/invocations?Qualifier=PROD")
                    .body(Body::from("{}"))
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
    async fn invoke_with_colon_qualifier_latest() {
        let app = invoke_routes().with_state(test_state());
        // my-func:$LATEST — should parse and accept $LATEST
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func:$LATEST/invocations")
                    .body(Body::from("{}"))
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
    async fn invoke_with_colon_qualifier_unknown() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/alpha-func:PROD/invocations")
                    .body(Body::from("{}"))
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
    async fn invoke_with_version_number_qualifier() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/alpha-func/invocations?Qualifier=3")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "ResourceNotFoundException");
    }

    // -- Qualifier integration tests (GetFunction API) ------------------------

    #[tokio::test]
    async fn get_function_with_latest_qualifier() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(
                Request::get("/2015-03-31/functions/alpha-func?Qualifier=$LATEST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Configuration"]["FunctionName"], "alpha-func");
    }

    #[tokio::test]
    async fn get_function_with_unknown_qualifier_returns_404() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(
                Request::get("/2015-03-31/functions/alpha-func?Qualifier=PROD")
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
    async fn get_function_with_colon_qualifier_latest() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(
                Request::get("/2015-03-31/functions/alpha-func:$LATEST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Configuration"]["FunctionName"], "alpha-func");
    }

    #[tokio::test]
    async fn get_function_with_colon_qualifier_unknown() {
        let app = invoke_routes().with_state(test_state_with_functions());
        let resp = app
            .oneshot(
                Request::get("/2015-03-31/functions/alpha-func:STAGING")
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

    // -- X-Amz-Log-Type validation -----------------------------------------

    #[tokio::test]
    async fn invoke_unsupported_log_type_returns_400() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let app = invoke_routes().with_state(state);

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Log-Type", "Invalid")
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
        assert!(json["Message"].as_str().unwrap().contains("log type"));
    }

    #[tokio::test]
    async fn invoke_log_type_none_does_not_include_log_result_header() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let app = invoke_routes().with_state(state);

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .header("X-Amz-Log-Type", "None")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Function not found, but the header validation passed
        assert!(resp.headers().get("X-Amz-Log-Result").is_none());
    }

    #[tokio::test]
    async fn invoke_no_log_type_header_does_not_include_log_result() {
        let (state, _shutdown_tx) = test_state_with_function("my-func", 30).await;
        let app = invoke_routes().with_state(state);

        let resp = app
            .oneshot(
                Request::post("/2015-03-31/functions/my-func/invocations")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(resp.headers().get("X-Amz-Log-Result").is_none());
    }

    // -----------------------------------------------------------------------
    // Streaming invoke endpoint tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn streaming_invoke_not_found_returns_404() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post(
                    "/2021-11-15/functions/nonexistent/response-streaming-invocations",
                )
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn streaming_invoke_invalid_function_name_returns_400() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post(
                    "/2021-11-15/functions/invalid..name/response-streaming-invocations",
                )
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn streaming_invoke_rejects_during_shutdown() {
        let state = test_state();
        state
            .shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post(
                    "/2021-11-15/functions/test-fn/response-streaming-invocations",
                )
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn streaming_invoke_rejects_event_invocation_type() {
        let state = test_state_with_functions();
        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post(
                    "/2021-11-15/functions/alpha-func/response-streaming-invocations",
                )
                .header("X-Amz-Invocation-Type", "Event")
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
        assert!(json["Message"]
            .as_str()
            .unwrap()
            .contains("RequestResponse"));
    }

    #[tokio::test]
    async fn streaming_invoke_unknown_qualifier_returns_404() {
        let state = test_state_with_functions();
        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::post(
                    "/2021-11-15/functions/alpha-func:v2/response-streaming-invocations",
                )
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn streaming_invoke_route_is_matched() {
        // Verify the streaming endpoint route exists and returns a proper error
        // for a function that doesn't exist (not a 404 from the router, but
        // from the handler).
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post(
                    "/2021-11-15/functions/some-func/response-streaming-invocations",
                )
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();

        // Should be 404 ResourceNotFound from the handler, not 405 Method Not Allowed
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["Type"], "ResourceNotFoundException");
    }

    // -- Extensions API tests ------------------------------------------------

    #[tokio::test]
    async fn extension_register_returns_200_with_identifier() {
        let app = runtime_routes().with_state(test_state());
        let body = serde_json::json!({"events": ["INVOKE", "SHUTDOWN"]});
        let resp = app
            .oneshot(
                Request::post("/2020-01-01/extension/register")
                    .header("Lambda-Extension-Name", "my-ext")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .headers()
            .get("Lambda-Extension-Identifier")
            .is_some());

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["functionVersion"], "$LATEST");
    }

    #[tokio::test]
    async fn extension_register_missing_name_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let body = serde_json::json!({"events": ["INVOKE"]});
        let resp = app
            .oneshot(
                Request::post("/2020-01-01/extension/register")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extension_register_missing_function_name_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let body = serde_json::json!({"events": ["INVOKE"]});
        let resp = app
            .oneshot(
                Request::post("/2020-01-01/extension/register")
                    .header("Lambda-Extension-Name", "my-ext")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extension_register_empty_events_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let body = serde_json::json!({"events": []});
        let resp = app
            .oneshot(
                Request::post("/2020-01-01/extension/register")
                    .header("Lambda-Extension-Name", "my-ext")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extension_register_invalid_body_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::post("/2020-01-01/extension/register")
                    .header("Lambda-Extension-Name", "my-ext")
                    .header("Lambda-Runtime-Function-Name", "my-func")
                    .header("content-type", "application/json")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extension_event_next_missing_identifier_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::get("/2020-01-01/extension/event/next")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extension_event_next_invalid_uuid_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::get("/2020-01-01/extension/event/next")
                    .header("Lambda-Extension-Identifier", "not-a-uuid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extension_event_next_unregistered_returns_400() {
        let app = runtime_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::get("/2020-01-01/extension/event/next")
                    .header(
                        "Lambda-Extension-Identifier",
                        "550e8400-e29b-41d4-a716-446655440000",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extension_event_next_returns_shutdown_on_shutdown_signal() {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let ext_registry = Arc::new(crate::extensions::ExtensionRegistry::new(
            shutdown_rx.clone(),
        ));
        let runtime_bridge = Arc::new(RuntimeBridge::new(
            HashMap::new(),
            HashMap::new(),
            shutdown_rx,
        ));
        let docker = bollard::Docker::connect_with_local_defaults().unwrap();
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
        };
        let functions = FunctionsConfig {
            functions: HashMap::new(),
            runtime_images: HashMap::new(),
        };

        // Register an extension
        let ext_id = ext_registry
            .register("test-ext", "my-func", vec![
                crate::extensions::ExtensionEventType::Invoke,
                crate::extensions::ExtensionEventType::Shutdown,
            ])
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
            extension_registry: ext_registry.clone(),
        };

        // Send shutdown signal so the long-poll resolves immediately.
        _shutdown_tx.send(true).unwrap();

        let app = runtime_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::get("/2020-01-01/extension/event/next")
                    .header("Lambda-Extension-Identifier", ext_id.to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["eventType"], "SHUTDOWN");
        assert_eq!(json["shutdownReason"], "spindown");
        assert!(json["deadlineMs"].is_u64());
    }

    #[tokio::test]
    async fn extension_lifecycle_invoke_event_delivery() {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let ext_registry = Arc::new(crate::extensions::ExtensionRegistry::new(
            shutdown_rx.clone(),
        ));

        // Register extension for INVOKE events.
        let ext_id = ext_registry
            .register("test-ext", "my-func", vec![
                crate::extensions::ExtensionEventType::Invoke,
            ])
            .await;

        // Deliver an INVOKE event.
        ext_registry
            .notify_invoke(
                "my-func",
                "req-abc-123",
                "arn:aws:lambda:us-east-1:000:function:my-func",
                9999999999,
                "Root=1-test",
            )
            .await;

        // Poll for it via the registry directly.
        let event = ext_registry.next_event(ext_id).await;
        assert!(event.is_some());
        let event = event.unwrap();
        match event {
            crate::extensions::ExtensionEvent::Invoke {
                request_id,
                deadline_ms,
                invoked_function_arn,
                tracing,
            } => {
                assert_eq!(request_id, "req-abc-123");
                assert_eq!(deadline_ms, 9999999999);
                assert!(invoked_function_arn.contains("my-func"));
                assert_eq!(tracing.value, "Root=1-test");
            }
            _ => panic!("expected INVOKE event"),
        }
    }

    // -----------------------------------------------------------------------
    // Function URL routing tests
    // -----------------------------------------------------------------------

    fn test_state_with_url_function() -> AppState {
        let mut state = test_state();
        let mut functions_map = HashMap::new();
        functions_map.insert(
            "url-func".to_string(),
            crate::types::FunctionConfig {
                name: "url-func".into(),
                runtime: "python3.12".into(),
                handler: "main.handler".into(),
                code_path: std::path::PathBuf::from("/tmp/code"),
                timeout: 30,
                memory_size: 128,
                ephemeral_storage_mb: 512,
                environment: HashMap::new(),
                image: None,
                image_uri: None,
                reserved_concurrent_executions: None,
                architecture: "x86_64".into(),
                layers: vec![],
                function_url_enabled: true,
                max_retry_attempts: 2,
                on_success: None,
                on_failure: None,
            },
        );
        functions_map.insert(
            "no-url-func".to_string(),
            crate::types::FunctionConfig {
                name: "no-url-func".into(),
                runtime: "python3.12".into(),
                handler: "main.handler".into(),
                code_path: std::path::PathBuf::from("/tmp/code"),
                timeout: 30,
                memory_size: 128,
                ephemeral_storage_mb: 512,
                environment: HashMap::new(),
                image: None,
                image_uri: None,
                reserved_concurrent_executions: None,
                architecture: "x86_64".into(),
                layers: vec![],
                function_url_enabled: false,
                max_retry_attempts: 2,
                on_success: None,
                on_failure: None,
            },
        );
        let functions = FunctionsConfig {
            functions: functions_map,
            runtime_images: HashMap::new(),
        };
        Arc::get_mut(&mut state.functions).map(|f| *f = functions);
        state
    }

    #[tokio::test]
    async fn function_url_returns_404_for_unknown_function() {
        let app = invoke_routes().with_state(test_state());
        let resp = app
            .oneshot(
                Request::get("/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn function_url_returns_404_for_disabled_function() {
        let app = invoke_routes().with_state(test_state_with_url_function());
        let resp = app
            .oneshot(
                Request::get("/no-url-func")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn function_url_accepts_any_http_method() {
        // For an enabled function, the route should match any method.
        // We can't test full invocation without Docker, but we can verify the
        // route matches (it will fail at container acquisition, not 404/405).
        let state = test_state_with_url_function();
        for method in &["GET", "POST", "PUT", "DELETE", "PATCH"] {
            let app = invoke_routes().with_state(state.clone());
            let resp = app
                .oneshot(
                    Request::builder()
                        .method(*method)
                        .uri("/url-func")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            // Should not be 404 or 405 — the route matched.
            assert_ne!(resp.status(), StatusCode::NOT_FOUND, "method {} returned 404", method);
            assert_ne!(resp.status(), StatusCode::METHOD_NOT_ALLOWED, "method {} returned 405", method);
        }
    }

    #[tokio::test]
    async fn function_url_with_subpath_matches() {
        let state = test_state_with_url_function();
        let app = invoke_routes().with_state(state);
        let resp = app
            .oneshot(
                Request::get("/url-func/some/path")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should not be 404 — the route with wildcard path matched.
        assert_ne!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- epoch_days_to_ymd ---------------------------------------------------

    #[test]
    fn epoch_days_to_ymd_unix_epoch() {
        assert_eq!(epoch_days_to_ymd(0), (1970, 1, 1));
    }

    #[test]
    fn epoch_days_to_ymd_known_date() {
        // 2024-01-01 is day 19723 from epoch
        assert_eq!(epoch_days_to_ymd(19723), (2024, 1, 1));
    }

    #[test]
    fn epoch_days_to_ymd_leap_day() {
        // 2024-02-29 is day 19782 from epoch
        assert_eq!(epoch_days_to_ymd(19782), (2024, 2, 29));
    }
}
