#[cfg(test)]
mod test;

use super::common::*;
use super::eventstream;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use base64::Engine;
use bytes::Bytes;
use serde::Serialize;
use std::time::Duration;
use tracing::{debug, info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::function::validate_function_name;
use crate::server::AppState;
use crate::types::{ContainerState, ServiceError};

use super::invoke::bootstrap_failure_response;

// ---------------------------------------------------------------------------
// Streaming invoke routes
// ---------------------------------------------------------------------------

pub fn streaming_routes() -> Router<AppState> {
    Router::new().route(
        "/2021-11-15/functions/:function_name/response-streaming-invocations",
        post(invoke_function_streaming),
    )
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
