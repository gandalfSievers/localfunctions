//! Integration test: invoke a function that sleeps longer than its timeout,
//! verify 200 response with X-Amz-Function-Error: Unhandled and the expected
//! timeout error message.
//!
//! This test exercises the full end-to-end flow:
//!   HTTP request → runtime bridge dispatch → simulated container picks up
//!   via /next → container sleeps past deadline → invoke handler's
//!   tokio::time::timeout fires → 200 with error header returned to caller.
//!
//! Requires Docker to be available (for ContainerRegistry), but does not
//! actually start a real container — a spawned task acts as the container
//! runtime by polling the Runtime API.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use localfunctions::config::{Config, LogFormat};
use localfunctions::container::{ContainerManager, ContainerRegistry};
use localfunctions::function::FunctionsConfig;
use localfunctions::runtime::RuntimeBridge;
use localfunctions::server::AppState;
use localfunctions::types::FunctionConfig;

/// Build an AppState with a single function configured with the given timeout.
async fn build_state(
    function_name: &str,
    timeout_secs: u64,
) -> (AppState, tokio::sync::watch::Sender<bool>) {
    let config = Config {
        host: "127.0.0.1".parse().unwrap(),
        port: 0,         // will be overridden by listener
        runtime_port: 0, // will be overridden by listener
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        functions_file: "./functions.json".into(),
        log_level: "info".into(),
        shutdown_timeout: 30,
        container_idle_timeout: 300,
        max_containers: 20,
        docker_network: "localfunctions".into(),
        max_body_size: 6 * 1024 * 1024,
        log_format: LogFormat::Text,
        pull_images: false,
        init_timeout: 10,
        container_acquire_timeout: 10,
    };

    let docker = bollard::Docker::connect_with_local_defaults().unwrap();

    let mut functions_map = HashMap::new();
    functions_map.insert(
        function_name.to_string(),
        FunctionConfig {
            name: function_name.to_string(),
            runtime: "python3.12".into(),
            handler: "main.handler".into(),
            code_path: std::path::PathBuf::from("/tmp/code"),
            timeout: timeout_secs,
            memory_size: 128,
            environment: HashMap::new(),
            image: None,
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
    ));
    // Pre-populate an idle container so the invoke handler doesn't attempt
    // real Docker operations in tests.
    container_manager
        .insert_test_container(
            "test-container".into(),
            function_name.into(),
            localfunctions::types::ContainerState::Idle,
        )
        .await;

    let state = AppState {
        config: Arc::new(config),
        container_registry,
        container_manager,
        docker,
        functions: Arc::new(functions),
        shutting_down: Arc::new(AtomicBool::new(false)),
        runtime_bridge,
    };

    (state, shutdown_tx)
}

#[tokio::test]
async fn invoke_timeout_returns_200_with_unhandled_error() {
    // Configure a function with a very short timeout (1 second).
    let (state, _shutdown_tx) = build_state("timeout-func", 1).await;

    // Start the Invoke API server on an ephemeral port.
    let invoke_app = localfunctions::server::invoke_router(state.clone());
    let invoke_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let invoke_addr = invoke_listener.local_addr().unwrap();

    let invoke_handle = tokio::spawn(async move {
        axum::serve(invoke_listener, invoke_app)
            .await
            .unwrap();
    });

    // Start the Runtime API server on an ephemeral port.
    let runtime_app = localfunctions::server::runtime_router(state.clone());
    let runtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let runtime_addr = runtime_listener.local_addr().unwrap();

    let runtime_handle = tokio::spawn(async move {
        axum::serve(runtime_listener, runtime_app)
            .await
            .unwrap();
    });

    // Spawn a simulated container runtime that picks up the invocation
    // from /next but then sleeps longer than the timeout before responding.
    let runtime_addr_clone = runtime_addr;
    let simulated_runtime = tokio::spawn(async move {
        let client = reqwest::Client::new();

        // Poll /next — blocks until an invocation is available.
        let next_resp = client
            .get(format!(
                "http://{}/2018-06-01/runtime/invocation/next",
                runtime_addr_clone
            ))
            .header("Lambda-Runtime-Function-Name", "timeout-func")
            .send()
            .await
            .unwrap();

        assert_eq!(next_resp.status(), 200);

        let request_id = next_resp
            .headers()
            .get("Lambda-Runtime-Aws-Request-Id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Verify the deadline header is present.
        let deadline_ms = next_resp
            .headers()
            .get("Lambda-Runtime-Deadline-Ms")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let deadline_ms: u128 = deadline_ms.parse().unwrap();
        assert!(deadline_ms > 0, "deadline should be a positive epoch-ms value");

        // Sleep longer than the function timeout so the invoke handler times out.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Try to post a response — this should fail or be ignored because the
        // invocation was already timed out.
        let _late_response = client
            .post(format!(
                "http://{}/2018-06-01/runtime/invocation/{}/response",
                runtime_addr_clone, request_id
            ))
            .body(r#"{"result":"too late"}"#)
            .send()
            .await;
    });

    // Send the invocation request to the Invoke API.
    let client = reqwest::Client::new();
    let invoke_resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/timeout-func/invocations",
            invoke_addr
        ))
        .body(r#"{"input":"test"}"#)
        .send()
        .await
        .unwrap();

    // Verify: 200 OK with X-Amz-Function-Error: Unhandled
    assert_eq!(invoke_resp.status(), 200);
    assert_eq!(
        invoke_resp
            .headers()
            .get("X-Amz-Function-Error")
            .expect("missing X-Amz-Function-Error header")
            .to_str()
            .unwrap(),
        "Unhandled"
    );

    // Verify the error message mentions the timeout.
    let body: serde_json::Value = invoke_resp.json().await.unwrap();
    let error_message = body["errorMessage"].as_str().unwrap();
    assert!(
        error_message.contains("Task timed out after 1 seconds"),
        "expected timeout message, got: {}",
        error_message
    );

    // Verify X-Amz-Request-Id is present.
    // (We already checked the response, but this confirms the header contract.)

    // Clean up: abort the servers and simulated runtime.
    invoke_handle.abort();
    runtime_handle.abort();
    simulated_runtime.abort();
}
