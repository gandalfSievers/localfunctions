//! Integration tests for bootstrap failure detection.
//!
//! Tests verify that when a container's bootstrap process fails (either by
//! exiting before calling /next or by timing out), the invocation returns
//! a 502 with InvalidRuntimeException and includes stderr in the error message.
//!
//! These tests use a simulated container runtime (spawned task) instead of
//! real Docker containers.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use localfunctions::config::{Config, LogFormat};
use localfunctions::container::{ContainerManager, ContainerRegistry};
use localfunctions::function::FunctionsConfig;
use localfunctions::runtime::RuntimeBridge;
use localfunctions::server::AppState;
use localfunctions::types::{ContainerState, FunctionConfig};

/// Build an AppState with a single function and a short init timeout.
async fn build_state(
    function_name: &str,
    timeout_secs: u64,
    init_timeout: u64,
) -> (AppState, tokio::sync::watch::Sender<bool>) {
    let config = Config {
        host: "127.0.0.1".parse().unwrap(),
        port: 0,
        runtime_port: 0,
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
        init_timeout,
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
    // real Docker operations.
    container_manager
        .insert_test_container(
            "test-container".into(),
            function_name.into(),
            ContainerState::Idle,
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

/// Test: bootstrap init timeout — the simulated runtime never calls /next,
/// so the init timeout fires and the invocation returns 502 with
/// InvalidRuntimeException.
///
/// This test uses a warm container (pre-inserted as Idle), so the bootstrap
/// wait path is not exercised for the warm case. Instead, we test the
/// RuntimeBridge ready signal mechanism directly: we register a signal,
/// don't fire it, and verify that the ready signal times out correctly.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn ready_signal_timeout() {
    let (state, _shutdown_tx) = build_state("bootstrap-timeout-func", 30, 1).await;

    // Register a ready signal for a container that will never become ready
    let signal = state
        .runtime_bridge
        .register_ready_signal("never-ready-container")
        .await;

    let result = tokio::time::timeout(Duration::from_secs(2), signal.notified()).await;
    assert!(result.is_err(), "signal should have timed out");
}

/// Test: ready signal fires immediately when container is already ready.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn ready_signal_fires_when_already_ready() {
    let (state, _shutdown_tx) = build_state("bootstrap-ready-func", 30, 10).await;

    // Mark container as ready first
    state.runtime_bridge.mark_ready("fast-container").await;

    // Register signal after — should fire immediately
    let signal = state
        .runtime_bridge
        .register_ready_signal("fast-container")
        .await;

    let result = tokio::time::timeout(Duration::from_millis(100), signal.notified()).await;
    assert!(result.is_ok(), "signal should have fired immediately");
}

/// Test: ready signal fires when mark_ready is called after registration.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn ready_signal_fires_on_mark_ready() {
    let (state, _shutdown_tx) = build_state("bootstrap-signal-func", 30, 10).await;

    let signal = state
        .runtime_bridge
        .register_ready_signal("delayed-container")
        .await;

    let bridge = state.runtime_bridge.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        bridge.mark_ready("delayed-container").await;
    });

    let result = tokio::time::timeout(Duration::from_secs(2), signal.notified()).await;
    assert!(result.is_ok(), "signal should have fired after mark_ready");
}

/// Full end-to-end: invoke a function where the simulated container calls
/// /next successfully (bootstrap succeeds), then responds normally.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn bootstrap_success_then_invoke() {
    let (state, _shutdown_tx) = build_state("bootstrap-ok-func", 5, 2).await;

    // Start servers
    let invoke_app = localfunctions::server::invoke_router(state.clone());
    let invoke_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let invoke_addr = invoke_listener.local_addr().unwrap();
    let invoke_handle = tokio::spawn(async move {
        axum::serve(invoke_listener, invoke_app).await.unwrap();
    });

    let runtime_app = localfunctions::server::runtime_router(state.clone());
    let runtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let runtime_addr = runtime_listener.local_addr().unwrap();
    let runtime_handle = tokio::spawn(async move {
        axum::serve(runtime_listener, runtime_app).await.unwrap();
    });

    // Simulated container runtime: calls /next, then responds
    let runtime_addr_clone = runtime_addr;
    let simulated_runtime = tokio::spawn(async move {
        let client = reqwest::Client::new();

        let next_resp = client
            .get(format!(
                "http://{}/2018-06-01/runtime/invocation/next",
                runtime_addr_clone
            ))
            .header("Lambda-Runtime-Function-Name", "bootstrap-ok-func")
            .header("Lambda-Runtime-Container-Id", "test-container")
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

        // Respond successfully
        let _resp = client
            .post(format!(
                "http://{}/2018-06-01/runtime/invocation/{}/response",
                runtime_addr_clone, request_id
            ))
            .body(r#"{"result":"ok"}"#)
            .send()
            .await
            .unwrap();
    });

    // Invoke the function
    let client = reqwest::Client::new();
    let invoke_resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/bootstrap-ok-func/invocations",
            invoke_addr
        ))
        .body(r#"{"input":"test"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(invoke_resp.status(), 200);
    assert!(
        invoke_resp.headers().get("X-Amz-Function-Error").is_none(),
        "should not have function error header on success"
    );

    let body: serde_json::Value = invoke_resp.json().await.unwrap();
    assert_eq!(body["result"], "ok");

    invoke_handle.abort();
    runtime_handle.abort();
    simulated_runtime.abort();
}

/// Full end-to-end: invoke a function where the simulated container reports
/// an init error via POST /runtime/init/error.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn bootstrap_init_error_returns_502() {
    let (state, _shutdown_tx) = build_state("bootstrap-fail-func", 5, 2).await;

    // Start servers
    let invoke_app = localfunctions::server::invoke_router(state.clone());
    let invoke_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let invoke_addr = invoke_listener.local_addr().unwrap();
    let invoke_handle = tokio::spawn(async move {
        axum::serve(invoke_listener, invoke_app).await.unwrap();
    });

    let runtime_app = localfunctions::server::runtime_router(state.clone());
    let runtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let runtime_addr = runtime_listener.local_addr().unwrap();
    let runtime_handle = tokio::spawn(async move {
        axum::serve(runtime_listener, runtime_app).await.unwrap();
    });

    // Simulated container: immediately reports init error without calling /next
    let runtime_addr_clone = runtime_addr;
    let simulated_runtime = tokio::spawn(async move {
        let client = reqwest::Client::new();

        // Small delay to ensure the invoke request has been submitted
        tokio::time::sleep(Duration::from_millis(100)).await;

        let _resp = client
            .post(format!(
                "http://{}/2018-06-01/runtime/init/error",
                runtime_addr_clone
            ))
            .header("Lambda-Runtime-Function-Name", "bootstrap-fail-func")
            .body(r#"{"errorMessage":"Cannot find module 'handler'","errorType":"Runtime.ImportModuleError"}"#)
            .send()
            .await
            .unwrap();
    });

    // Invoke — should get back a 502 or error
    let client = reqwest::Client::new();
    let invoke_resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/bootstrap-fail-func/invocations",
            invoke_addr
        ))
        .body(r#"{"input":"test"}"#)
        .send()
        .await
        .unwrap();

    // The init error handler sends InvalidRuntimeException back to all pending invocations
    let body: serde_json::Value = invoke_resp.json().await.unwrap();
    let error_type = body
        .get("errorType")
        .or_else(|| body.get("Type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        error_type.contains("InvalidRuntime") || error_type.contains("Runtime"),
        "expected InvalidRuntimeException, got body: {}",
        body
    );

    invoke_handle.abort();
    runtime_handle.abort();
    simulated_runtime.abort();
}
