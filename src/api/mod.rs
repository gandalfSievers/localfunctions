pub(crate) mod common;
mod eventstream;
pub(crate) mod extensions;
pub(crate) mod function_url;
pub(crate) mod functions;
pub(crate) mod health;
pub(crate) mod runtime;
mod sample_events;

use common::*;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use base64::Engine;
use bytes::Bytes;
use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::function::validate_function_name;
use crate::server::AppState;
use crate::types::{ContainerState, ServiceError};

// ---------------------------------------------------------------------------
// Invoke API routes (external, port 9600 by default)
// ---------------------------------------------------------------------------

pub fn invoke_routes() -> Router<AppState> {
    Router::new()
        .merge(health::health_routes())
        .merge(functions::functions_routes())
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
// Runtime API routes are in the `runtime` submodule.
pub(crate) use runtime::runtime_routes;

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
