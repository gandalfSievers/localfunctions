use super::*;
use axum::body::Body;
use http::Request;
use std::collections::HashMap;
use std::sync::Arc;
use tower::ServiceExt;

use crate::api::invoke::invoke_routes;
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
        domain: None,
        callback_url: "http://0.0.0.0:9600".to_string(),
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

fn test_state_with_functions() -> AppState {
    let mut state = test_state();
    let mut functions_map = HashMap::new();
    functions_map.insert(
        "alpha-func".to_string(),
        crate::types::FunctionConfig {
            name: "alpha-func".into(),
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
            payload_format_version: "2.0".to_string(),
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
