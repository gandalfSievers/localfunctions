#[cfg(test)]
mod test;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use serde::Deserialize;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::extensions::ExtensionEventType;
use crate::server::AppState;
use crate::types::ServiceError;

use super::runtime::HEADER_FUNCTION_NAME;

/// Header used by extensions to identify themselves after registration.
pub(crate) const HEADER_EXTENSION_ID: &str = "Lambda-Extension-Identifier";
/// Header sent by extensions during registration to provide their name.
pub(crate) const HEADER_EXTENSION_NAME: &str = "Lambda-Extension-Name";

/// Request body for extension registration.
#[derive(Debug, Deserialize)]
pub(crate) struct ExtensionRegisterRequest {
    events: Vec<ExtensionEventType>,
}

/// Build the extension API routes.
pub fn extension_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/2020-01-01/extension/register",
            post(extension_register),
        )
        .route(
            "/2020-01-01/extension/event/next",
            get(extension_event_next),
        )
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
pub(crate) async fn extension_register(
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
pub(crate) async fn extension_event_next(
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
