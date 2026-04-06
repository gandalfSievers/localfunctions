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
        domain: None,
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
        domain: None,
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
        domain: None,
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
        domain: None,
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
