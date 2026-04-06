#[cfg(test)]
mod test;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};

use crate::function::validate_function_name;
use crate::server::AppState;
use crate::types::ServiceError;

use super::common::{function_configuration_json, parse_qualifier, validate_qualifier, QualifierParams};

/// List all configured functions in AWS ListFunctions response format.
///
/// GET /2015-03-31/functions
pub(crate) async fn list_functions(State(state): State<AppState>) -> impl IntoResponse {
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
pub(crate) async fn get_function(
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

/// Returns a router with function listing/get routes.
pub fn functions_routes() -> Router<AppState> {
    Router::new()
        .route("/2015-03-31/functions", get(list_functions))
        .route("/2015-03-31/functions/:function_name", get(get_function))
}
