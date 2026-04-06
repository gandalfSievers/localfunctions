#[cfg(test)]
mod test;

use axum::http::HeaderMap;
use base64::Engine;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::types::ServiceError;

// ---------------------------------------------------------------------------
// X-Ray Trace ID generation
// ---------------------------------------------------------------------------

/// Generate an AWS X-Ray trace header in the format:
/// `Root=1-{hex-timestamp}-{96-bit-hex-id};Parent={64-bit-hex-id};Sampled=1`
pub(crate) fn generate_xray_trace_id() -> String {
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

// ---------------------------------------------------------------------------
// Qualifier parsing
// ---------------------------------------------------------------------------

/// Query parameters accepted by the Invoke and GetFunction APIs.
#[derive(Debug, Deserialize, Default)]
pub(crate) struct QualifierParams {
    #[serde(rename = "Qualifier")]
    pub qualifier: Option<String>,
}

/// Parse a function name that may contain a colon-separated qualifier
/// (e.g. `my-function:PROD`) and merge with an optional `?Qualifier=` query
/// parameter. The query parameter takes precedence if both are present.
///
/// Returns `(base_name, qualifier)` where qualifier is `None` for unqualified
/// requests and `Some(q)` when a qualifier was specified.
pub(crate) fn parse_qualifier(raw_name: &str, query_qualifier: Option<&str>) -> (String, Option<String>) {
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
pub(crate) fn validate_qualifier(qualifier: &Option<String>, function_name: &str) -> Result<(), ServiceError> {
    match qualifier.as_deref() {
        None | Some("$LATEST") => Ok(()),
        Some(q) => Err(ServiceError::ResourceNotFound(format!(
            "Function not found: arn:aws:lambda:us-east-1:000000000000:function:{}:{}",
            function_name, q
        ))),
    }
}

// ---------------------------------------------------------------------------
// Client context validation
// ---------------------------------------------------------------------------

/// Maximum size of the decoded client context in bytes (AWS limit).
pub(crate) const MAX_CLIENT_CONTEXT_BYTES: usize = 3_583;

/// Validate the `X-Amz-Client-Context` header value.
///
/// The value must be valid base64, decode to valid JSON, and the decoded
/// payload must not exceed 3,583 bytes (AWS limit).
pub(crate) fn validate_client_context(value: &str) -> Result<(), ServiceError> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|e| {
            ServiceError::InvalidRequestContent(format!(
                "Could not base64 decode client context: {e}"
            ))
        })?;

    if decoded.len() > MAX_CLIENT_CONTEXT_BYTES {
        return Err(ServiceError::InvalidRequestContent(format!(
            "Client context must be no more than {MAX_CLIENT_CONTEXT_BYTES} bytes when decoded, got {} bytes",
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

// ---------------------------------------------------------------------------
// Date utilities
// ---------------------------------------------------------------------------

/// Convert epoch days to (year, month, day).
pub(crate) fn epoch_days_to_ymd(days: u64) -> (u64, u64, u64) {
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
// Function configuration helpers
// ---------------------------------------------------------------------------

/// Build the AWS-format function configuration JSON for a single function.
pub(crate) fn function_configuration_json(
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
pub(crate) fn invoke_base_headers(request_id: Uuid) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("X-Amz-Request-Id", request_id.to_string().parse().unwrap());
    h.insert("X-Amz-Executed-Version", "$LATEST".parse().unwrap());
    h
}
