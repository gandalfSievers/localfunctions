//! Lambda Function URL emulation.
//!
//! Transforms incoming HTTP requests into the Lambda Function URL event format
//! (payload version 2.0) and translates function responses back to HTTP
//! responses, matching the behavior of real AWS Lambda Function URLs.

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::server::AppState;

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
// Function URL response format
// ---------------------------------------------------------------------------

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
/// Route: `/url/{function_name}` or `/url/{function_name}/*path` — any HTTP method.
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
                );
            }
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    HeaderMap::new(),
                    Body::from("Not Found"),
                );
            }
        };

        // Build the Function URL event (v2.0 payload).
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

        let event_json = match serde_json::to_vec(&event) {
            Ok(j) => j,
            Err(e) => {
                warn!(error = %e, "failed to serialize Function URL event");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HeaderMap::new(),
                    Body::from("Internal Server Error"),
                );
            }
        };

        debug!(payload_size = event_json.len(), "function URL event");

        // Invoke the function through the same path as the Invoke API.
        let payload = Bytes::from(event_json);
        let timeout_secs = function_config.timeout;
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

        // Ensure a container is available.
        let container_id =
            match crate::api::acquire_container(&state, function_config, &request_id).await {
                Ok(id) => id,
                Err(resp) => return resp,
            };

        info!(
            payload_size = payload.len(),
            timeout_secs,
            container_id = %container_id,
            "invoking function via Function URL"
        );

        // Stream container logs.
        let log_handle = state.container_manager.stream_container_logs(
            &container_id,
            &function_name,
            &request_id.to_string(),
        );

        // Submit the invocation.
        let trace_id = headers
            .get("X-Amzn-Trace-Id")
            .and_then(|v| v.to_str().ok())
            .map(String::from)
            .unwrap_or_else(super::generate_xray_trace_id);

        let response_rx = match state
            .runtime_bridge
            .submit_invocation(&function_name, request_id, payload, deadline, Some(trace_id), None)
            .await
        {
            Ok(rx) => rx,
            Err(e) => {
                log_handle.abort();
                state.container_manager.release_container(&container_id).await;
                warn!(error = %e, "failed to submit invocation");
                return (
                    StatusCode::BAD_GATEWAY,
                    HeaderMap::new(),
                    Body::from("Bad Gateway"),
                );
            }
        };

        // Wait for the response.
        let result =
            tokio::time::timeout(Duration::from_secs(timeout_secs), response_rx).await;

        // Release container after invocation completes.
        log_handle.abort();
        state.container_manager.release_container(&container_id).await;

        // Record metrics (duration is approximate since we don't track the start).
        state.metrics.record_invocation(&function_name, Duration::from_millis(0), false, false);

        match result {
            Ok(Ok(crate::types::InvocationResult::Success { body })) => {
                translate_function_response(&body)
            }
            Ok(Ok(crate::types::InvocationResult::Error {
                error_type,
                error_message,
            })) => {
                warn!(error_type = %error_type, error_message = %error_message, "function error");
                let error_body = serde_json::json!({
                    "errorType": error_type,
                    "errorMessage": error_message,
                });
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HeaderMap::new(),
                    Body::from(serde_json::to_string(&error_body).unwrap_or_default()),
                )
            }
            Ok(Ok(crate::types::InvocationResult::Timeout)) | Err(_) => {
                warn!("function timed out");
                (
                    StatusCode::GATEWAY_TIMEOUT,
                    HeaderMap::new(),
                    Body::from(format!(
                        "{{\"errorMessage\":\"Task timed out after {} seconds\"}}",
                        timeout_secs
                    )),
                )
            }
            Ok(Err(_)) => {
                warn!("function container crashed");
                (
                    StatusCode::BAD_GATEWAY,
                    HeaderMap::new(),
                    Body::from("{\"errorMessage\":\"Container crashed\"}"),
                )
            }
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

    // Parse query string parameters.
    let raw_query = uri.query().unwrap_or("").to_string();
    let query_params: HashMap<String, String> = url_decode_query(&raw_query);

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
    let api_id = format!("{:.12}", format!("{:x}", md5_simple(function_name)));
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Simple query string parser.
fn url_decode_query(query: &str) -> HashMap<String, String> {
    if query.is_empty() {
        return HashMap::new();
    }
    query
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((
                percent_decode(key),
                percent_decode(value),
            ))
        })
        .collect()
}

/// Minimal percent decoding for query parameters.
fn percent_decode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next();
            let lo = chars.next();
            if let (Some(h), Some(l)) = (hi, lo) {
                let hex = [h, l];
                if let Ok(s) = std::str::from_utf8(&hex) {
                    if let Ok(byte) = u8::from_str_radix(s, 16) {
                        result.push(byte as char);
                        continue;
                    }
                }
            }
            result.push('%');
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
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

/// Simple non-cryptographic hash to generate a stable synthetic API ID.
fn md5_simple(input: &str) -> u64 {
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
    let (year, month, day) = epoch_days_to_ymd(days);
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

/// Convert epoch days to (year, month, day).
fn epoch_days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Civil calendar algorithm.
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_decode_query_simple() {
        let result = url_decode_query("foo=bar&baz=qux");
        assert_eq!(result.get("foo").unwrap(), "bar");
        assert_eq!(result.get("baz").unwrap(), "qux");
    }

    #[test]
    fn test_url_decode_query_empty() {
        let result = url_decode_query("");
        assert!(result.is_empty());
    }

    #[test]
    fn test_url_decode_query_encoded() {
        let result = url_decode_query("key=hello%20world&other=a%2Bb");
        assert_eq!(result.get("key").unwrap(), "hello world");
        assert_eq!(result.get("other").unwrap(), "a+b");
    }

    #[test]
    fn test_url_decode_query_plus_as_space() {
        let result = url_decode_query("key=hello+world");
        assert_eq!(result.get("key").unwrap(), "hello world");
    }

    #[test]
    fn test_percent_decode_basic() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("no+encoding"), "no encoding");
        assert_eq!(percent_decode("plain"), "plain");
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
    fn test_md5_simple_deterministic() {
        let a = md5_simple("my-function");
        let b = md5_simple("my-function");
        assert_eq!(a, b);
        assert_ne!(md5_simple("func-a"), md5_simple("func-b"));
    }

    #[test]
    fn test_format_time() {
        // 2024-01-01 00:00:00 UTC = 1704067200
        let result = format_time(1704067200);
        assert!(result.contains("2024"));
        assert!(result.contains("+0000"));
    }

    #[test]
    fn test_epoch_days_to_ymd() {
        // 2024-01-01 = day 19723 since epoch
        let (y, m, d) = epoch_days_to_ymd(19723);
        assert_eq!(y, 2024);
        assert_eq!(m, 1);
        assert_eq!(d, 1);
    }
}
