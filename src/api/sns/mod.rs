//! SNS subscription lifecycle endpoints.
//!
//! Provides HTTP endpoints that allow an SNS topic (typically localstack or a
//! real AWS SNS topic in development) to subscribe to a Lambda function and
//! deliver notifications.
//!
//! ## Endpoints
//!
//! - `POST /sns/:function_name` — Accepts SNS messages. Automatically confirms
//!   `SubscriptionConfirmation` requests by fetching the `SubscribeURL`, and
//!   forwards `Notification` messages to the named function as invocations.
//!
//! ## Callback URL
//!
//! When subscribing an SNS topic to a localfunctions endpoint, use the
//! `callback_url` from configuration to construct the full endpoint:
//!
//! ```text
//! {callback_url}/sns/{function_name}
//! ```
//!
//! This ensures SNS can reach localfunctions even when running inside Docker
//! Compose or behind a reverse proxy.

#[cfg(test)]
mod test;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use tracing::{debug, error, info, warn};

use crate::function::validate_function_name;
use crate::server::AppState;

/// Returns the SNS endpoint URL for a given function, constructed from the
/// configured `callback_url`.
///
/// This is the URL that should be passed to `sns:Subscribe` as the `Endpoint`
/// parameter when creating an HTTP/HTTPS subscription.
pub fn sns_endpoint_url(callback_url: &str, function_name: &str) -> String {
    format!("{}/sns/{}", callback_url, function_name)
}

/// Returns a router with SNS subscription lifecycle routes.
pub fn sns_routes() -> Router<AppState> {
    Router::new().route("/sns/:function_name", post(sns_handler))
}

/// Handle incoming SNS messages for a given function.
///
/// Inspects the `x-amz-sns-message-type` header to determine the message type:
///
/// - `SubscriptionConfirmation`: fetches the `SubscribeURL` to auto-confirm.
/// - `Notification`: wraps the SNS message in an SNS event record and invokes
///   the function.
/// - `UnsubscribeConfirmation`: logged and acknowledged.
async fn sns_handler(
    State(state): State<AppState>,
    Path(function_name): Path<String>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> (StatusCode, &'static str) {
    // Validate function name.
    if let Err(e) = validate_function_name(&function_name) {
        warn!(function = %function_name, error = %e, "SNS: invalid function name");
        return (StatusCode::BAD_REQUEST, "Invalid function name");
    }

    // Ensure the function exists.
    if !state.functions.functions.contains_key(&function_name) {
        warn!(function = %function_name, "SNS: function not found");
        return (StatusCode::NOT_FOUND, "Function not found");
    }

    let message_type = headers
        .get("x-amz-sns-message-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let endpoint = sns_endpoint_url(&state.config.callback_url, &function_name);

    match message_type {
        "SubscriptionConfirmation" => {
            handle_subscription_confirmation(&function_name, &endpoint, &body).await
        }
        "Notification" => {
            handle_notification(&state, &function_name, body).await
        }
        "UnsubscribeConfirmation" => {
            info!(function = %function_name, "SNS: unsubscribe confirmation received");
            (StatusCode::OK, "OK")
        }
        _ => {
            warn!(
                function = %function_name,
                message_type = %message_type,
                "SNS: unknown or missing message type"
            );
            (StatusCode::BAD_REQUEST, "Unknown SNS message type")
        }
    }
}

/// Auto-confirm an SNS subscription by fetching the `SubscribeURL`.
async fn handle_subscription_confirmation(
    function_name: &str,
    endpoint: &str,
    body: &Bytes,
) -> (StatusCode, &'static str) {
    let parsed: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            warn!(function = %function_name, error = %e, "SNS: failed to parse confirmation body");
            return (StatusCode::BAD_REQUEST, "Invalid JSON");
        }
    };

    let subscribe_url = match parsed.get("SubscribeURL").and_then(|v| v.as_str()) {
        Some(url) => url,
        None => {
            warn!(function = %function_name, "SNS: SubscriptionConfirmation missing SubscribeURL");
            return (StatusCode::BAD_REQUEST, "Missing SubscribeURL");
        }
    };

    info!(
        function = %function_name,
        subscribe_url = %subscribe_url,
        endpoint = %endpoint,
        "SNS: confirming subscription"
    );

    match reqwest::get(subscribe_url).await {
        Ok(resp) if resp.status().is_success() => {
            info!(function = %function_name, "SNS: subscription confirmed");
            (StatusCode::OK, "Subscription confirmed")
        }
        Ok(resp) => {
            error!(
                function = %function_name,
                status = %resp.status(),
                "SNS: subscription confirmation request failed"
            );
            (StatusCode::BAD_GATEWAY, "Subscription confirmation failed")
        }
        Err(e) => {
            error!(
                function = %function_name,
                error = %e,
                "SNS: failed to reach SubscribeURL"
            );
            (StatusCode::BAD_GATEWAY, "Failed to reach SubscribeURL")
        }
    }
}

/// Forward an SNS notification to the named function as a Lambda invocation.
///
/// Wraps the raw SNS message in the standard Lambda SNS event format
/// (`{"Records": [{"EventSource": "aws:sns", "Sns": {...}}]}`).
async fn handle_notification(
    state: &AppState,
    function_name: &str,
    body: Bytes,
) -> (StatusCode, &'static str) {
    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            warn!(function = %function_name, error = %e, "SNS: failed to parse notification body");
            return (StatusCode::BAD_REQUEST, "Invalid JSON");
        }
    };

    // Build the SNS event record matching the Lambda event format.
    let topic_arn = parsed
        .get("TopicArn")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let subscription_arn = format!(
        "arn:aws:sns:{}:{}:{}",
        state.config.region, state.config.account_id, function_name
    );

    let sns_event = serde_json::json!({
        "Records": [{
            "EventVersion": "1.0",
            "EventSubscriptionArn": subscription_arn,
            "EventSource": "aws:sns",
            "Sns": parsed
        }]
    });

    let event_bytes = match serde_json::to_vec(&sns_event) {
        Ok(b) => b,
        Err(e) => {
            error!(function = %function_name, error = %e, "SNS: failed to serialize event");
            return (StatusCode::INTERNAL_SERVER_ERROR, "Internal error");
        }
    };

    debug!(
        function = %function_name,
        topic_arn = %topic_arn,
        payload_size = event_bytes.len(),
        "SNS: forwarding notification to function"
    );

    // Use the standard invoke path via Event invocation type (async, fire-and-forget).
    let request_id = uuid::Uuid::new_v4();
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("X-Amz-Invocation-Type", "Event".parse().unwrap());

    let (status, _resp_headers, _resp_body) = super::invoke::invoke_function_inner(
        state.clone(),
        function_name.to_string(),
        headers,
        Bytes::from(event_bytes),
        request_id,
    )
    .await;

    if status == StatusCode::ACCEPTED {
        info!(
            function = %function_name,
            topic_arn = %topic_arn,
            %request_id,
            "SNS: notification forwarded"
        );
        (StatusCode::OK, "OK")
    } else {
        error!(
            function = %function_name,
            topic_arn = %topic_arn,
            %status,
            "SNS: failed to invoke function"
        );
        (StatusCode::INTERNAL_SERVER_ERROR, "Invocation failed")
    }
}
