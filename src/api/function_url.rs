//! Lambda Function URL emulation.
//!
//! Transforms incoming HTTP requests into Lambda Function URL events and
//! translates function responses back to HTTP responses, matching the behavior
//! of real AWS Lambda Function URLs.
//!
//! Supports both payload format versions:
//! - **2.0** (default): Function URL v2 event with flat headers and comma-joined
//!   query parameters.
//! - **1.0**: API Gateway v1 REST API proxy integration event with
//!   `multiValueHeaders`, `multiValueQueryStringParameters`, and
//!   `requestContext.identity`.
//!
//! Supports both buffered and streaming response modes. When the function
//! responds via the streaming Runtime API with a metadata prelude (JSON +
//! null-byte separator), the body is streamed back to the HTTP client using
//! chunked transfer encoding.

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use base64::Engine;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::server::AppState;
use crate::types::StreamChunk;

// ---------------------------------------------------------------------------
// Function URL event format (payload version 2.0)
// ---------------------------------------------------------------------------

/// Lambda Function URL event payload (version 2.0).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FunctionUrlEvent {
    version: &'static str,
    raw_path: String,
    raw_query_string: String,
    headers: HashMap<String, String>,
    query_string_parameters: HashMap<String, String>,
    request_context: RequestContext,
    body: Option<String>,
    is_base64_encoded: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestContext {
    account_id: String,
    api_id: String,
    domain_name: String,
    domain_prefix: String,
    http: HttpContext,
    request_id: String,
    route_key: &'static str,
    stage: &'static str,
    time: String,
    time_epoch: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HttpContext {
    method: String,
    path: String,
    protocol: &'static str,
    source_ip: String,
    user_agent: String,
}

// ---------------------------------------------------------------------------
// REST API v1 event format (payload version 1.0)
// ---------------------------------------------------------------------------

/// Lambda API Gateway v1 REST API proxy integration event (version 1.0).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RestApiEvent {
    version: &'static str,
    resource: &'static str,
    path: String,
    http_method: String,
    headers: HashMap<String, String>,
    multi_value_headers: HashMap<String, Vec<String>>,
    query_string_parameters: Option<HashMap<String, String>>,
    multi_value_query_string_parameters: Option<HashMap<String, Vec<String>>>,
    path_parameters: Option<HashMap<String, String>>,
    stage_variables: Option<()>,
    request_context: RestApiRequestContext,
    body: Option<String>,
    is_base64_encoded: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RestApiRequestContext {
    account_id: String,
    api_id: String,
    domain_name: String,
    domain_prefix: String,
    http_method: String,
    identity: RestApiIdentity,
    path: String,
    protocol: &'static str,
    request_id: String,
    request_time: String,
    request_time_epoch: u64,
    resource_id: String,
    resource_path: &'static str,
    stage: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RestApiIdentity {
    source_ip: String,
    user_agent: String,
}

// ---------------------------------------------------------------------------
// Function URL response formats
// ---------------------------------------------------------------------------

/// Response shape for v1 REST API style responses (supports multiValueHeaders).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestApiResponse {
    #[serde(default = "default_status_code")]
    status_code: u16,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    multi_value_headers: HashMap<String, Vec<String>>,
    body: Option<String>,
    #[serde(default)]
    is_base64_encoded: bool,
}

/// The response shape a Lambda function returns for Function URL invocations.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FunctionUrlResponse {
    #[serde(default = "default_status_code")]
    status_code: u16,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    cookies: Vec<String>,
    body: Option<String>,
    #[serde(default)]
    is_base64_encoded: bool,
}

fn default_status_code() -> u16 {
    200
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Handle an incoming Function URL request for the given function.
///
/// Route: `/{function_name}` or `/{function_name}/*path` — any HTTP method.
///
/// Uses streaming invocation internally so that functions using the streaming
/// Runtime API can have their responses forwarded as chunked HTTP.
pub async fn function_url_handler(
    State(state): State<AppState>,
    Path(params): Path<HashMap<String, String>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let function_name = params.get("function_name").cloned().unwrap_or_default();
    let sub_path = params.get("path").cloned();
    let request_id = Uuid::new_v4();

    let span = info_span!("function_url", %request_id, function = %function_name);

    async {
        // Look up the function and verify function_url_enabled.
        let function_config = match state.functions.functions.get(&function_name) {
            Some(config) if config.function_url_enabled => config,
            Some(_) => {
                return (
                    StatusCode::NOT_FOUND,
                    HeaderMap::new(),
                    Body::from("Not Found"),
                )
                    .into_response();
            }
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    HeaderMap::new(),
                    Body::from("Not Found"),
                )
                    .into_response();
            }
        };

        let is_v1 = function_config.payload_format_version == "1.0";

        // Build the Function URL event in the configured payload format.
        let event_json = if is_v1 {
            let event = build_rest_api_event(
                &state,
                &function_name,
                &method,
                &uri,
                &headers,
                &body,
                &request_id,
                sub_path.as_deref(),
            );
            match serde_json::to_vec(&event) {
                Ok(j) => j,
                Err(e) => {
                    warn!(error = %e, "failed to serialize REST API event");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        HeaderMap::new(),
                        Body::from("Internal Server Error"),
                    )
                        .into_response();
                }
            }
        } else {
            let event = build_function_url_event(
                &state,
                &function_name,
                &method,
                &uri,
                &headers,
                &body,
                &request_id,
                sub_path.as_deref(),
            );
            match serde_json::to_vec(&event) {
                Ok(j) => j,
                Err(e) => {
                    warn!(error = %e, "failed to serialize Function URL event");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        HeaderMap::new(),
                        Body::from("Internal Server Error"),
                    )
                        .into_response();
                }
            }
        };

        debug!(payload_size = event_json.len(), "function URL event");

        // Invoke the function through the same path as the Invoke API.
        let payload = Bytes::from(event_json);
        let timeout_secs = function_config.timeout;
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
        let invoke_start = std::time::Instant::now();

        // Ensure a container is available.
        let container_id =
            match crate::api::acquire_container(&state, function_config, &request_id).await {
                Ok(id) => id,
                Err(resp) => return resp.into_response(),
            };

        if let Some(ref cid) = container_id {
            info!(
                payload_size = payload.len(),
                timeout_secs,
                container_id = %cid,
                "invoking function via Function URL (cold start)"
            );
        } else {
            info!(
                payload_size = payload.len(),
                timeout_secs,
                "invoking function via Function URL (warm)"
            );
        }

        // Stream container logs for cold starts.
        let log_handle = container_id.as_ref().map(|cid| {
            state.container_manager.stream_container_logs(
                cid,
                &function_name,
                &request_id.to_string(),
            )
        });

        // Submit the invocation as streaming so we can support chunked responses.
        let trace_id = headers
            .get("X-Amzn-Trace-Id")
            .and_then(|v| v.to_str().ok())
            .map(String::from)
            .unwrap_or_else(super::generate_xray_trace_id);

        let mut stream_rx = match state
            .runtime_bridge
            .submit_streaming_invocation(
                &function_name,
                request_id,
                payload,
                deadline,
                Some(trace_id),
                None,
            )
            .await
        {
            Ok(rx) => rx,
            Err(e) => {
                if let Some(ref h) = log_handle {
                    h.abort();
                }
                if let Some(ref cid) = container_id {
                    state.container_manager.release_container(cid).await;
                }
                warn!(error = %e, "failed to submit invocation");
                return (
                    StatusCode::BAD_GATEWAY,
                    HeaderMap::new(),
                    Body::from("Bad Gateway"),
                )
                    .into_response();
            }
        };

        // Read chunks from the stream. We buffer initial data to determine
        // whether the function used a streaming metadata prelude (JSON + \0 +
        // body) or a traditional buffered JSON response.
        let mut initial_buf = BytesMut::new();
        let mut had_error = false;
        let mut error_type = String::new();
        let mut error_message = String::new();
        let mut timed_out = false;
        let mut remaining_chunks: Vec<Bytes> = Vec::new();
        let mut found_prelude = false;

        loop {
            match tokio::time::timeout_at(deadline, stream_rx.recv()).await {
                Ok(Some(StreamChunk::Data(data))) => {
                    if !found_prelude {
                        initial_buf.extend_from_slice(&data);
                        // Check for null byte separator (streaming metadata prelude).
                        if let Some(pos) = initial_buf.iter().position(|&b| b == 0) {
                            found_prelude = true;
                            // Everything after the null byte is body data.
                            let after_null = initial_buf.split_off(pos + 1);
                            // Remove the null byte from initial_buf.
                            initial_buf.truncate(pos);
                            if !after_null.is_empty() {
                                remaining_chunks.push(after_null.freeze());
                            }
                        }
                    } else {
                        remaining_chunks.push(data);
                    }
                }
                Ok(Some(StreamChunk::Error {
                    error_type: et,
                    error_message: em,
                })) => {
                    had_error = true;
                    error_type = et;
                    error_message = em;
                    break;
                }
                Ok(Some(StreamChunk::Complete)) => break,
                Ok(None) => {
                    // Channel closed — container may have crashed.
                    had_error = true;
                    error_type = "ServiceException".into();
                    error_message = "Container exited without completing".into();
                    break;
                }
                Err(_) => {
                    // Timeout.
                    timed_out = true;
                    break;
                }
            }
        }

        if let Some(ref h) = log_handle {
            h.abort();
        }
        if let Some(ref cid) = container_id {
            state.container_manager.release_container(cid).await;
        }

        // Record metrics with actual duration and error/timeout status.
        state.metrics.record_invocation(
            &function_name,
            invoke_start.elapsed(),
            had_error,
            timed_out,
        );

        if timed_out {
            warn!("function timed out");
            return (
                StatusCode::GATEWAY_TIMEOUT,
                HeaderMap::new(),
                Body::from(format!(
                    "{{\"errorMessage\":\"Task timed out after {} seconds\"}}",
                    timeout_secs
                )),
            )
                .into_response();
        }

        if had_error {
            warn!(error_type = %error_type, error_message = %error_message, "function error");
            let error_body = serde_json::json!({
                "errorType": error_type,
                "errorMessage": error_message,
            });
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Body::from(serde_json::to_string(&error_body).unwrap_or_default()),
            )
                .into_response();
        }

        if found_prelude {
            // Streaming response with metadata prelude.
            // Parse the metadata JSON from the prelude.
            let metadata_str = String::from_utf8_lossy(&initial_buf);
            let metadata: FunctionUrlResponse =
                match serde_json::from_str(&metadata_str) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(error = %e, "failed to parse streaming metadata prelude");
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            HeaderMap::new(),
                            Body::from("Internal Server Error"),
                        )
                            .into_response();
                    }
                };

            let status =
                StatusCode::from_u16(metadata.status_code).unwrap_or(StatusCode::OK);

            let mut resp_headers = HeaderMap::new();
            for (key, value) in &metadata.headers {
                if let (Ok(k), Ok(v)) = (
                    key.parse::<http::header::HeaderName>(),
                    value.parse(),
                ) {
                    resp_headers.insert(k, v);
                }
            }
            for cookie in &metadata.cookies {
                if let Ok(v) = cookie.parse() {
                    resp_headers.append(http::header::SET_COOKIE, v);
                }
            }

            // If there are buffered body chunks but the stream is complete,
            // return them directly.
            if remaining_chunks.is_empty() {
                let mut response = axum::response::Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .unwrap();
                *response.headers_mut() = resp_headers;
                return response;
            }

            // Stream any remaining body chunks plus whatever comes from the
            // receiver (in case the loop broke on Complete before draining).
            let (body_tx, body_rx) =
                tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);

            // Send buffered chunks and then continue draining the receiver.
            tokio::spawn(async move {
                for chunk in remaining_chunks {
                    if body_tx.send(Ok(chunk)).await.is_err() {
                        return;
                    }
                }
                // Drain any remaining stream chunks (in case Complete hasn't
                // been received yet — though typically it has by this point).
                while let Some(chunk) = stream_rx.recv().await {
                    match chunk {
                        StreamChunk::Data(data) => {
                            if body_tx.send(Ok(data)).await.is_err() {
                                return;
                            }
                        }
                        StreamChunk::Complete | StreamChunk::Error { .. } => break,
                    }
                }
            });

            let stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);
            let response_body = Body::from_stream(stream);

            let mut response = axum::response::Response::builder()
                .status(status)
                .body(response_body)
                .unwrap();
            *response.headers_mut() = resp_headers;
            response
        } else {
            // Buffered response — treat initial_buf as the complete JSON
            // response body (same as before).
            let body_str = String::from_utf8_lossy(&initial_buf);
            let (status, resp_headers, resp_body) = if is_v1 {
                translate_rest_api_response(&body_str)
            } else {
                translate_function_response(&body_str)
            };
            (status, resp_headers, resp_body).into_response()
        }
    }
    .instrument(span)
    .await
}

// ---------------------------------------------------------------------------
// Event construction
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn build_function_url_event(
    state: &AppState,
    function_name: &str,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
    request_id: &Uuid,
    sub_path: Option<&str>,
) -> FunctionUrlEvent {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    // Flatten headers — Function URL uses lowercase single-value headers.
    let mut header_map: HashMap<String, String> = HashMap::new();
    for (key, value) in headers.iter() {
        let key_lower = key.as_str().to_lowercase();
        if let Ok(v) = value.to_str() {
            header_map
                .entry(key_lower)
                .and_modify(|existing| {
                    existing.push_str(", ");
                    existing.push_str(v);
                })
                .or_insert_with(|| v.to_string());
        }
    }

    // Parse query string parameters. AWS joins duplicate keys with commas.
    let raw_query = uri.query().unwrap_or("").to_string();
    let query_params = parse_query_params(&raw_query);

    // Determine the raw path relative to the function.
    let raw_path = if let Some(p) = sub_path {
        format!("/{}", p)
    } else {
        "/".to_string()
    };

    // Determine if body is binary.
    let content_type = header_map.get("content-type").cloned().unwrap_or_default();
    let is_binary = is_binary_content_type(&content_type);

    let (event_body, is_base64_encoded) = if body.is_empty() {
        (None, false)
    } else if is_binary {
        let encoded = base64::engine::general_purpose::STANDARD.encode(body.as_ref());
        (Some(encoded), true)
    } else {
        (
            Some(String::from_utf8_lossy(body.as_ref()).to_string()),
            false,
        )
    };

    let user_agent = header_map
        .get("user-agent")
        .cloned()
        .unwrap_or_default();

    // Generate a synthetic API ID from the function name.
    let api_id = format!("{:.12}", format!("{:x}", fnv_hash(function_name)));
    let domain_name = format!(
        "{}.lambda-url.{}.on.aws",
        api_id, state.config.region
    );

    FunctionUrlEvent {
        version: "2.0",
        raw_path: raw_path.clone(),
        raw_query_string: raw_query,
        headers: header_map,
        query_string_parameters: query_params,
        request_context: RequestContext {
            account_id: state.config.account_id.clone(),
            api_id: api_id.clone(),
            domain_name: domain_name.clone(),
            domain_prefix: api_id,
            http: HttpContext {
                method: method.to_string(),
                path: raw_path,
                protocol: "HTTP/1.1",
                source_ip: "127.0.0.1".to_string(),
                user_agent,
            },
            request_id: request_id.to_string(),
            route_key: "$default",
            stage: "$default",
            time: format_time(now.as_secs()),
            time_epoch: now.as_millis() as u64,
        },
        body: event_body,
        is_base64_encoded,
    }
}

/// Build an API Gateway v1 REST API proxy integration event (payload version 1.0).
#[allow(clippy::too_many_arguments)]
fn build_rest_api_event(
    state: &AppState,
    function_name: &str,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
    request_id: &Uuid,
    sub_path: Option<&str>,
) -> RestApiEvent {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    // v1 uses original-case headers (first value) and multi-value headers (all values).
    let mut single_headers: HashMap<String, String> = HashMap::new();
    let mut multi_headers: HashMap<String, Vec<String>> = HashMap::new();
    for (key, value) in headers.iter() {
        let key_str = key.as_str().to_string();
        if let Ok(v) = value.to_str() {
            single_headers.entry(key_str.clone()).or_insert_with(|| v.to_string());
            multi_headers
                .entry(key_str)
                .or_default()
                .push(v.to_string());
        }
    }

    // Parse query string parameters: single-value (last wins) and multi-value.
    let raw_query = uri.query().unwrap_or("").to_string();
    let (single_query, multi_query) = parse_query_params_v1(&raw_query);

    let raw_path = if let Some(p) = sub_path {
        format!("/{}", p)
    } else {
        "/".to_string()
    };

    // Determine if body is binary.
    let content_type = single_headers
        .get("content-type")
        .or_else(|| single_headers.get("Content-Type"))
        .cloned()
        .unwrap_or_default();
    let is_binary = is_binary_content_type(&content_type);

    let (event_body, is_base64_encoded) = if body.is_empty() {
        (None, false)
    } else if is_binary {
        let encoded = base64::engine::general_purpose::STANDARD.encode(body.as_ref());
        (Some(encoded), true)
    } else {
        (
            Some(String::from_utf8_lossy(body.as_ref()).to_string()),
            false,
        )
    };

    let user_agent = single_headers
        .get("user-agent")
        .or_else(|| single_headers.get("User-Agent"))
        .cloned()
        .unwrap_or_default();

    let api_id = format!("{:.12}", format!("{:x}", fnv_hash(function_name)));
    let domain_name = format!(
        "{}.lambda-url.{}.on.aws",
        api_id, state.config.region
    );

    // pathParameters with proxy key
    let proxy_value = raw_path.trim_start_matches('/').to_string();
    let path_parameters = if !proxy_value.is_empty() {
        let mut map = HashMap::new();
        map.insert("proxy".to_string(), proxy_value);
        Some(map)
    } else {
        None
    };

    RestApiEvent {
        version: "1.0",
        resource: "/{proxy+}",
        path: raw_path.clone(),
        http_method: method.to_string(),
        headers: single_headers,
        multi_value_headers: multi_headers,
        query_string_parameters: if single_query.is_empty() {
            None
        } else {
            Some(single_query)
        },
        multi_value_query_string_parameters: if multi_query.is_empty() {
            None
        } else {
            Some(multi_query)
        },
        path_parameters,
        stage_variables: None,
        request_context: RestApiRequestContext {
            account_id: state.config.account_id.clone(),
            api_id: api_id.clone(),
            domain_name: domain_name.clone(),
            domain_prefix: api_id,
            http_method: method.to_string(),
            identity: RestApiIdentity {
                source_ip: "127.0.0.1".to_string(),
                user_agent,
            },
            path: raw_path,
            protocol: "HTTP/1.1",
            request_id: request_id.to_string(),
            request_time: format_time(now.as_secs()),
            request_time_epoch: now.as_millis() as u64,
            resource_id: "local".to_string(),
            resource_path: "/{proxy+}",
            stage: "local".to_string(),
        },
        body: event_body,
        is_base64_encoded,
    }
}

/// Translate a function's JSON response into an HTTP response.
fn translate_function_response(
    body: &str,
) -> (StatusCode, HeaderMap, Body) {
    // If the response is not valid JSON or doesn't have the expected shape,
    // treat the raw body as a 200 text response (matching AWS behavior for
    // simple string returns).
    let parsed: FunctionUrlResponse = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(_) => {
            // AWS returns the raw string body with 200 if the function doesn't
            // return a proper response object.
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "application/json".parse().unwrap());
            return (StatusCode::OK, headers, Body::from(body.to_string()));
        }
    };

    let status = StatusCode::from_u16(parsed.status_code).unwrap_or(StatusCode::OK);

    let mut resp_headers = HeaderMap::new();
    for (key, value) in &parsed.headers {
        if let (Ok(k), Ok(v)) = (key.parse::<http::header::HeaderName>(), value.parse()) {
            resp_headers.insert(k, v);
        }
    }

    // Add Set-Cookie headers from the cookies array.
    for cookie in &parsed.cookies {
        if let Ok(v) = cookie.parse() {
            resp_headers.append(http::header::SET_COOKIE, v);
        }
    }

    let response_body = match parsed.body {
        Some(b) if parsed.is_base64_encoded => {
            match base64::engine::general_purpose::STANDARD.decode(&b) {
                Ok(decoded) => Body::from(decoded),
                Err(_) => Body::from(b),
            }
        }
        Some(b) => Body::from(b),
        None => Body::empty(),
    };

    (status, resp_headers, response_body)
}

/// Translate a v1 REST API function response into an HTTP response.
///
/// Handles `multiValueHeaders` with append semantics: each value in a
/// multi-value list becomes a separate header in the HTTP response.
fn translate_rest_api_response(
    body: &str,
) -> (StatusCode, HeaderMap, Body) {
    let parsed: RestApiResponse = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(_) => {
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "application/json".parse().unwrap());
            return (StatusCode::OK, headers, Body::from(body.to_string()));
        }
    };

    let status = StatusCode::from_u16(parsed.status_code).unwrap_or(StatusCode::OK);
    let mut resp_headers = HeaderMap::new();

    // First apply single-value headers.
    for (key, value) in &parsed.headers {
        if let (Ok(k), Ok(v)) = (key.parse::<http::header::HeaderName>(), value.parse()) {
            resp_headers.insert(k, v);
        }
    }

    // Then apply multiValueHeaders with append semantics (overwrites single-value
    // headers for the same key, matching AWS behavior).
    for (key, values) in &parsed.multi_value_headers {
        if let Ok(k) = key.parse::<http::header::HeaderName>() {
            let mut first = true;
            for value in values {
                if let Ok(v) = value.parse() {
                    if first {
                        resp_headers.insert(k.clone(), v);
                        first = false;
                    } else {
                        resp_headers.append(k.clone(), v);
                    }
                }
            }
        }
    }

    let response_body = match parsed.body {
        Some(b) if parsed.is_base64_encoded => {
            match base64::engine::general_purpose::STANDARD.decode(&b) {
                Ok(decoded) => Body::from(decoded),
                Err(_) => Body::from(b),
            }
        }
        Some(b) => Body::from(b),
        None => Body::empty(),
    };

    (status, resp_headers, response_body)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse query string parameters for v1 format: single-value (last value wins)
/// and multi-value (all values preserved per key).
fn parse_query_params_v1(
    query: &str,
) -> (HashMap<String, String>, HashMap<String, Vec<String>>) {
    if query.is_empty() {
        return (HashMap::new(), HashMap::new());
    }
    let mut single: HashMap<String, String> = HashMap::new();
    let mut multi: HashMap<String, Vec<String>> = HashMap::new();
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        let k = key.into_owned();
        let v = value.into_owned();
        single.insert(k.clone(), v.clone());
        multi.entry(k).or_default().push(v);
    }
    (single, multi)
}

/// Parse query string parameters, joining duplicate keys with commas per the
/// AWS Lambda Function URL v2.0 payload specification.
fn parse_query_params(query: &str) -> HashMap<String, String> {
    if query.is_empty() {
        return HashMap::new();
    }
    let mut params: HashMap<String, String> = HashMap::new();
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        params
            .entry(key.into_owned())
            .and_modify(|existing| {
                existing.push(',');
                existing.push_str(&value);
            })
            .or_insert_with(|| value.into_owned());
    }
    params
}

/// Check if the content type indicates binary data.
fn is_binary_content_type(content_type: &str) -> bool {
    let ct = content_type.to_lowercase();
    ct.starts_with("image/")
        || ct.starts_with("audio/")
        || ct.starts_with("video/")
        || ct.starts_with("application/octet-stream")
        || ct.starts_with("application/zip")
        || ct.starts_with("application/pdf")
        || ct.starts_with("multipart/form-data")
}

/// FNV-1a hash to generate a stable synthetic API ID from a function name.
fn fnv_hash(input: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Format a unix timestamp as an HTTP-date-like string for the event.
fn format_time(epoch_secs: u64) -> String {
    // Produce a simple ISO-like timestamp: "06/Apr/2026:12:00:00 +0000"
    // This matches the format AWS uses in requestContext.time.
    let secs = epoch_secs;
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simplified date calculation.
    let (year, month, day) = super::common::epoch_days_to_ymd(days);
    let month_name = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let m = if (1..=12).contains(&month) {
        month_name[(month - 1) as usize]
    } else {
        "Jan"
    };
    format!(
        "{:02}/{}/{:04}:{:02}:{:02}:{:02} +0000",
        day, m, year, hours, minutes, seconds
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_params_simple() {
        let result = parse_query_params("foo=bar&baz=qux");
        assert_eq!(result.get("foo").unwrap(), "bar");
        assert_eq!(result.get("baz").unwrap(), "qux");
    }

    #[test]
    fn test_parse_query_params_empty() {
        let result = parse_query_params("");
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_query_params_encoded() {
        let result = parse_query_params("key=hello%20world&other=a%2Bb");
        assert_eq!(result.get("key").unwrap(), "hello world");
        assert_eq!(result.get("other").unwrap(), "a+b");
    }

    #[test]
    fn test_parse_query_params_plus_as_space() {
        let result = parse_query_params("key=hello+world");
        assert_eq!(result.get("key").unwrap(), "hello world");
    }

    #[test]
    fn test_parse_query_params_duplicate_keys() {
        let result = parse_query_params("color=red&color=blue");
        assert_eq!(result.get("color").unwrap(), "red,blue");
    }

    #[test]
    fn test_parse_query_params_triple_duplicate() {
        let result = parse_query_params("x=1&x=2&x=3");
        assert_eq!(result.get("x").unwrap(), "1,2,3");
    }

    #[test]
    fn test_parse_query_params_multibyte_utf8() {
        // Percent-encoded multi-byte UTF-8: "日本" = %E6%97%A5%E6%9C%AC
        let result = parse_query_params("lang=%E6%97%A5%E6%9C%AC");
        assert_eq!(result.get("lang").unwrap(), "日本");
    }

    #[test]
    fn test_is_binary_content_type() {
        assert!(is_binary_content_type("image/png"));
        assert!(is_binary_content_type("application/octet-stream"));
        assert!(is_binary_content_type("application/pdf"));
        assert!(!is_binary_content_type("text/plain"));
        assert!(!is_binary_content_type("application/json"));
    }

    #[test]
    fn test_translate_function_response_structured() {
        let body = r#"{"statusCode":201,"headers":{"x-custom":"val"},"body":"created"}"#;
        let (status, headers, _body) = translate_function_response(body);
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(headers.get("x-custom").unwrap(), "val");
    }

    #[test]
    fn test_translate_function_response_plain_string() {
        let body = r#""just a string""#;
        let (status, headers, _body) = translate_function_response(body);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get("content-type").unwrap(), "application/json");
    }

    #[test]
    fn test_translate_function_response_base64_body() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"binary data");
        let body = format!(
            r#"{{"statusCode":200,"body":"{}","isBase64Encoded":true}}"#,
            encoded
        );
        let (status, _headers, resp_body) = translate_function_response(&body);
        assert_eq!(status, StatusCode::OK);
        // Body should be decoded binary.
        let collected = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                use http_body_util::BodyExt;
                resp_body.collect().await.unwrap().to_bytes()
            });
        assert_eq!(collected.as_ref(), b"binary data");
    }

    #[test]
    fn test_translate_function_response_with_cookies() {
        let body = r#"{"statusCode":200,"cookies":["session=abc; Path=/","theme=dark"],"body":"ok"}"#;
        let (status, headers, _body) = translate_function_response(body);
        assert_eq!(status, StatusCode::OK);
        let cookies: Vec<&str> = headers
            .get_all(http::header::SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(cookies.len(), 2);
        assert!(cookies.contains(&"session=abc; Path=/"));
        assert!(cookies.contains(&"theme=dark"));
    }

    #[test]
    fn test_translate_function_response_empty_body() {
        let body = r#"{"statusCode":204}"#;
        let (status, _headers, resp_body) = translate_function_response(body);
        assert_eq!(status, StatusCode::NO_CONTENT);
        let collected = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                use http_body_util::BodyExt;
                resp_body.collect().await.unwrap().to_bytes()
            });
        assert!(collected.is_empty());
    }

    #[test]
    fn test_fnv_hash_deterministic() {
        let a = fnv_hash("my-function");
        let b = fnv_hash("my-function");
        assert_eq!(a, b);
        assert_ne!(fnv_hash("func-a"), fnv_hash("func-b"));
    }

    #[test]
    fn test_format_time() {
        // 2024-01-01 00:00:00 UTC = 1704067200
        let result = format_time(1704067200);
        assert!(result.contains("2024"));
        assert!(result.contains("+0000"));
    }

    // -- v1 REST API event tests -----------------------------------------------

    #[test]
    fn test_parse_query_params_v1_simple() {
        let (single, multi) = parse_query_params_v1("foo=bar&baz=qux");
        assert_eq!(single.get("foo").unwrap(), "bar");
        assert_eq!(single.get("baz").unwrap(), "qux");
        assert_eq!(multi.get("foo").unwrap(), &vec!["bar".to_string()]);
        assert_eq!(multi.get("baz").unwrap(), &vec!["qux".to_string()]);
    }

    #[test]
    fn test_parse_query_params_v1_empty() {
        let (single, multi) = parse_query_params_v1("");
        assert!(single.is_empty());
        assert!(multi.is_empty());
    }

    #[test]
    fn test_parse_query_params_v1_duplicate_keys() {
        let (single, multi) = parse_query_params_v1("color=red&color=blue");
        // Last value wins for single-value map.
        assert_eq!(single.get("color").unwrap(), "blue");
        // Multi-value preserves all values in order.
        assert_eq!(
            multi.get("color").unwrap(),
            &vec!["red".to_string(), "blue".to_string()]
        );
    }

    #[test]
    fn test_build_rest_api_event_structure() {
        use crate::config::Config;
        use crate::container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig};
        use crate::function::FunctionsConfig;
        use crate::extensions::ExtensionRegistry;
        use crate::metrics::MetricsCollector;
        use crate::runtime::RuntimeBridge;
        use std::sync::Arc;

        let config = Config {
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            host: "127.0.0.1".parse().unwrap(),
            port: 9600,
            runtime_port: 9601,
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
            runtime_host: "host-gateway".to_string(),
        };
        let docker = bollard::Docker::connect_with_local_defaults().unwrap();
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
            "host-gateway".to_string(),
        ));
        let state = AppState {
            config: Arc::new(config),
            docker,
            container_registry,
            container_manager,
            functions: Arc::new(FunctionsConfig {
                functions: HashMap::new(),
                runtime_images: HashMap::new(),
                event_source_mappings: Vec::new(),
                sns_subscriptions: Vec::new(),
            }),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            runtime_bridge,
            metrics: Arc::new(MetricsCollector::new()),
            extension_registry: Arc::new(ExtensionRegistry::new(
                tokio::sync::watch::channel(false).1,
            )),
        };

        let method = Method::GET;
        let uri: Uri = "http://localhost/my-func/hello/world?name=alice&tag=a&tag=b"
            .parse()
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Host", "localhost".parse().unwrap());
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.append("Accept", "text/html".parse().unwrap());
        headers.append("Accept", "application/json".parse().unwrap());
        let body = Bytes::new();
        let request_id = Uuid::nil();

        let event = build_rest_api_event(
            &state,
            "my-func",
            &method,
            &uri,
            &headers,
            &body,
            &request_id,
            Some("hello/world"),
        );

        // Version and resource
        assert_eq!(event.version, "1.0");
        assert_eq!(event.resource, "/{proxy+}");
        assert_eq!(event.http_method, "GET");
        assert_eq!(event.path, "/hello/world");

        // Single-value headers (original case, first value)
        assert!(event.headers.contains_key("host"));
        assert!(event.headers.contains_key("content-type"));

        // Multi-value headers
        let accept_values = event.multi_value_headers.get("accept").unwrap();
        assert_eq!(accept_values.len(), 2);
        assert!(accept_values.contains(&"text/html".to_string()));
        assert!(accept_values.contains(&"application/json".to_string()));

        // Query parameters: single value (last wins)
        let qsp = event.query_string_parameters.as_ref().unwrap();
        assert_eq!(qsp.get("name").unwrap(), "alice");
        assert_eq!(qsp.get("tag").unwrap(), "b"); // last value wins

        // Multi-value query parameters
        let mqsp = event.multi_value_query_string_parameters.as_ref().unwrap();
        assert_eq!(
            mqsp.get("tag").unwrap(),
            &vec!["a".to_string(), "b".to_string()]
        );

        // pathParameters
        let pp = event.path_parameters.as_ref().unwrap();
        assert_eq!(pp.get("proxy").unwrap(), "hello/world");

        // stageVariables should be null
        assert!(event.stage_variables.is_none());

        // requestContext
        assert_eq!(event.request_context.account_id, "123456789012");
        assert_eq!(event.request_context.http_method, "GET");
        assert_eq!(event.request_context.protocol, "HTTP/1.1");
        assert_eq!(event.request_context.resource_path, "/{proxy+}");
        assert_eq!(event.request_context.stage, "local");
        assert_eq!(event.request_context.identity.source_ip, "127.0.0.1");

        // Body should be None for empty request.
        assert!(event.body.is_none());
        assert!(!event.is_base64_encoded);

        // Verify JSON serialization has correct v1 fields
        let json_str = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["version"], "1.0");
        assert_eq!(parsed["resource"], "/{proxy+}");
        assert!(parsed["httpMethod"].is_string());
        assert!(parsed["multiValueHeaders"].is_object());
        assert!(parsed["multiValueQueryStringParameters"].is_object());
        assert!(parsed["pathParameters"].is_object());
        assert!(parsed["stageVariables"].is_null());
        assert!(parsed["requestContext"]["identity"].is_object());
        assert!(parsed["requestContext"]["resourceId"].is_string());
        assert!(parsed["requestContext"]["resourcePath"].is_string());
    }

    #[test]
    fn test_translate_rest_api_response_simple() {
        let body = r#"{"statusCode":200,"headers":{"x-custom":"val"},"body":"ok"}"#;
        let (status, headers, _body) = translate_rest_api_response(body);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get("x-custom").unwrap(), "val");
    }

    #[test]
    fn test_translate_rest_api_response_multi_value_headers() {
        let body = r#"{
            "statusCode": 200,
            "multiValueHeaders": {
                "Set-Cookie": ["session=abc; Path=/", "theme=dark"],
                "X-Custom": ["value1", "value2"]
            },
            "body": "ok"
        }"#;
        let (status, headers, _body) = translate_rest_api_response(body);
        assert_eq!(status, StatusCode::OK);

        // Set-Cookie should have 2 values
        let cookies: Vec<&str> = headers
            .get_all(http::header::SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(cookies.len(), 2);
        assert!(cookies.contains(&"session=abc; Path=/"));
        assert!(cookies.contains(&"theme=dark"));

        // X-Custom should have 2 values
        let custom: Vec<&str> = headers
            .get_all("x-custom")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(custom.len(), 2);
    }

    #[test]
    fn test_translate_rest_api_response_multi_value_overrides_single() {
        let body = r#"{
            "statusCode": 200,
            "headers": {"x-custom": "single"},
            "multiValueHeaders": {"x-custom": ["multi1", "multi2"]},
            "body": "ok"
        }"#;
        let (_status, headers, _body) = translate_rest_api_response(body);
        // multiValueHeaders should override single-value headers
        let custom: Vec<&str> = headers
            .get_all("x-custom")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(custom, vec!["multi1", "multi2"]);
    }

    #[test]
    fn test_translate_rest_api_response_base64() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"binary data");
        let body = format!(
            r#"{{"statusCode":200,"body":"{}","isBase64Encoded":true}}"#,
            encoded
        );
        let (status, _headers, resp_body) = translate_rest_api_response(&body);
        assert_eq!(status, StatusCode::OK);
        let collected = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                use http_body_util::BodyExt;
                resp_body.collect().await.unwrap().to_bytes()
            });
        assert_eq!(collected.as_ref(), b"binary data");
    }

    #[test]
    fn test_translate_rest_api_response_plain_string() {
        let body = r#""just a string""#;
        let (status, headers, _body) = translate_rest_api_response(body);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get("content-type").unwrap(), "application/json");
    }
}
