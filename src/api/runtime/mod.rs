#[cfg(test)]
mod test;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::server::AppState;
use crate::types::ServiceError;

use super::extensions;
use super::health;

// ---------------------------------------------------------------------------
// Runtime API routes (internal, port 9601 by default)
// ---------------------------------------------------------------------------

pub fn runtime_routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health::health))
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
        .merge(extensions::extension_routes())
}

/// Header sent by containers to identify which function they serve.
pub(crate) const HEADER_FUNCTION_NAME: &str = "Lambda-Runtime-Function-Name";
/// Header sent by containers to identify themselves (optional).
pub(crate) const HEADER_CONTAINER_ID: &str = "Lambda-Runtime-Container-Id";

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
