#[cfg(test)]
mod test;

use super::common::*;
use super::function_url;
use super::functions;
use super::health;
use super::sample_events;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use base64::Engine;
use bytes::Bytes;
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
        .merge(super::streaming::streaming_routes())
        // Sample event payload generation endpoints.
        .route("/admin/sample-events", get(sample_events::list_sample_events))
        .route("/admin/sample-events/:source", get(sample_events::get_sample_event))
        // SNS subscription lifecycle endpoints.
        .merge(super::sns::sns_routes())
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

pub(crate) async fn invoke_function_inner(
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

    // Ensure a container is available for this function. Check whether a warm
    // idle container exists; if not, acquire a container slot and cold-start a
    // new one. The actual Idle → Busy transition happens in the /next handler
    // when a container picks up the invocation from the shared channel.
    //
    // `container_id` is Some only for cold starts (where we know the exact
    // container). For warm reuse it is None — the /next handler will record
    // the real container in `pending_invocations`.
    let container_id: Option<String> = if state
        .container_manager
        .has_idle_container(&function_name)
        .await
    {
        debug!("warm container available, submitting to channel");
        None
    } else {
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
            .register_ready_signal(&cold_id, Some(&function_name))
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
            BootstrapOutcome::Ready => Some(cold_id),
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
    };

    if let Some(ref cid) = container_id {
        info!(
            payload_size = body.len(),
            timeout_secs,
            container_id = %cid,
            "invoking function (cold start)"
        );
    } else {
        info!(
            payload_size = body.len(),
            timeout_secs,
            "invoking function (warm)"
        );
    }

    // Start streaming container stdout/stderr in the background for cold
    // starts where we know the container ID. For warm reuse the /next handler
    // determines the actual container; logs are still available via Docker.
    let log_handle = container_id.as_ref().map(|cid| {
        state
            .container_manager
            .stream_container_logs(cid, &function_name, &request_id.to_string())
    });

    // Submit the invocation to the runtime bridge.
    let response_rx = match state
        .runtime_bridge
        .submit_invocation(&function_name, request_id, body, deadline, Some(trace_id), client_context)
        .await
    {
        Ok(rx) => rx,
        Err(e) => {
            // If submit fails (e.g. channel closed), release the container.
            if let Some(ref h) = log_handle {
                h.abort();
            }
            if let Some(ref cid) = container_id {
                state.container_manager.release_container(cid).await;
            }
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
            if let Some(cid) = crashed_container.or_else(|| container_id.clone()) {
                let mgr = state.container_manager.clone();
                tokio::spawn(async move {
                    let _ = mgr.stop_and_remove(&cid, Duration::from_secs(2)).await;
                });
            }

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
            if let Some(cid) = timed_out_container.or_else(|| container_id.clone()) {
                let mgr = state.container_manager.clone();
                tokio::spawn(async move {
                    let _ = mgr.stop_and_remove(&cid, Duration::from_secs(2)).await;
                });
            }

            let mut resp_headers = invoke_base_headers(request_id);
            resp_headers.insert("X-Amz-Function-Error", "Unhandled".parse().unwrap());
            let error_body = serde_json::json!({
                "errorMessage": format!("Task timed out after {} seconds", timeout_secs),
            });
            (StatusCode::OK, resp_headers, serde_json::to_vec(&error_body).unwrap())
        }
    };

    // Stop streaming container logs now that the invocation is complete.
    if let Some(ref h) = log_handle {
        h.abort();
    }

    // If LogType is Tail, collect the last 4KB of logs and add as a base64-encoded
    // response header. Only applies to synchronous (RequestResponse) invocations.
    if log_type_tail {
        if let Some(ref cid) = container_id {
            const LOG_TAIL_MAX_BYTES: usize = 4096;
            let logs = state
                .container_manager
                .get_container_logs(cid, LOG_TAIL_MAX_BYTES)
                .await;
            if !logs.is_empty() {
                let encoded = base64::engine::general_purpose::STANDARD.encode(logs.as_bytes());
                if let Ok(value) = encoded.parse() {
                    response.1.insert("X-Amz-Log-Result", value);
                }
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

/// Build an error response for a bootstrap failure (container exited before
/// calling /next, or init timeout expired).
///
/// Collects stderr from the container, logs a WARN, cleans up the container
/// in the background, and returns a 502 with `InvalidRuntimeException`.
pub(crate) async fn bootstrap_failure_response(
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
/// starting a new one.  Returns `Ok(Some(id))` for cold-starts, `Ok(None)`
/// when a warm container is available (the actual container is determined by
/// the `/next` handler), or an HTTP error tuple.
pub(crate) async fn acquire_container(
    state: &AppState,
    function_config: &crate::types::FunctionConfig,
    request_id: &Uuid,
) -> Result<Option<String>, (StatusCode, HeaderMap, axum::body::Body)> {
    use axum::body::Body;

    let function_name = &function_config.name;

    if state.container_manager.has_idle_container(function_name).await {
        debug!("warm container available, submitting to channel");
        return Ok(None);
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
        .register_ready_signal(&cold_id, Some(&function_config.name))
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
        BootstrapOutcome::Ready => Ok(Some(cold_id)),
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
