//! End-to-end integration tests that verify the full invocation flow:
//!   HTTP request → container start → Runtime API interaction → response
//!
//! These tests use simulated container runtimes (spawned Tokio tasks) that
//! behave like real Lambda containers — calling /next, processing the event,
//! and posting a response — without requiring actual Docker container images.
//!
//! All tests are gated with `#[ignore]` so that `cargo test` (unit tests)
//! runs without a Docker dependency. Run with: `cargo test -- --ignored`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use localfunctions::config::{Config, LogFormat};
use localfunctions::container::{ContainerManager, ContainerRegistry};
use localfunctions::function::FunctionsConfig;
use localfunctions::runtime::RuntimeBridge;
use localfunctions::server::AppState;
use localfunctions::types::{ContainerState, FunctionConfig};

/// Build an AppState with the given functions and pre-populated idle containers.
async fn build_e2e_state(
    functions: Vec<(&str, &str, &str, u64)>, // (name, runtime, handler, timeout)
) -> (AppState, tokio::sync::watch::Sender<bool>) {
    let config = Config {
        host: "127.0.0.1".parse().unwrap(),
        port: 0,
        runtime_port: 0,
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        functions_file: "./functions.json".into(),
        log_level: "info".into(),
        shutdown_timeout: 5,
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
    let mut senders = HashMap::new();
    let mut receivers = HashMap::new();

    for (name, runtime, handler, timeout) in &functions {
        functions_map.insert(
            name.to_string(),
            FunctionConfig {
                name: name.to_string(),
                runtime: runtime.to_string(),
                handler: handler.to_string(),
                code_path: std::path::PathBuf::from("/tmp/code"),
                timeout: *timeout,
                memory_size: 128,
                ephemeral_storage_mb: 512,
                environment: HashMap::new(),
                image: None,
            },
        );
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        senders.insert(name.to_string(), tx);
        receivers.insert(name.to_string(), rx);
    }

    let functions_config = FunctionsConfig {
        functions: functions_map,
        runtime_images: HashMap::new(),
    };

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

    // Pre-populate one idle container per function.
    for (name, _, _, _) in &functions {
        container_manager
            .insert_test_container(
                format!("e2e-container-{}", name),
                name.to_string(),
                ContainerState::Idle,
            )
            .await;
    }

    let state = AppState {
        config: Arc::new(config),
        container_registry,
        container_manager,
        docker,
        functions: Arc::new(functions_config),
        shutting_down: Arc::new(AtomicBool::new(false)),
        runtime_bridge,
    };

    (state, shutdown_tx)
}

/// Start both servers and return (invoke_addr, runtime_addr, handles).
async fn start_servers(
    state: AppState,
) -> (
    std::net::SocketAddr,
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
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

    (invoke_addr, runtime_addr, invoke_handle, runtime_handle)
}

/// Spawn a simulated container runtime that picks up /next and responds
/// with a successful result (echoing the event payload back).
fn spawn_echo_runtime(
    runtime_addr: std::net::SocketAddr,
    function_name: &str,
    container_id: &str,
) -> tokio::task::JoinHandle<()> {
    let function_name = function_name.to_string();
    let container_id = container_id.to_string();
    tokio::spawn(async move {
        let client = reqwest::Client::new();

        let next_resp = client
            .get(format!(
                "http://{}/2018-06-01/runtime/invocation/next",
                runtime_addr
            ))
            .header("Lambda-Runtime-Function-Name", &function_name)
            .header("Lambda-Runtime-Container-Id", &container_id)
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

        let payload = next_resp.text().await.unwrap();

        // Simulate Python/Node.js handler: echo input with a greeting.
        let response_body = format!(
            r#"{{"statusCode":200,"body":{{"message":"Hello from simulated runtime!","input":{}}}}}"#,
            payload
        );

        let resp = client
            .post(format!(
                "http://{}/2018-06-01/runtime/invocation/{}/response",
                runtime_addr, request_id
            ))
            .body(response_body)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 202);
    })
}

/// Spawn a simulated container runtime that picks up /next and reports
/// a function error via /invocation/{id}/error.
fn spawn_error_runtime(
    runtime_addr: std::net::SocketAddr,
    function_name: &str,
    container_id: &str,
    error_type: &str,
    error_message: &str,
) -> tokio::task::JoinHandle<()> {
    let function_name = function_name.to_string();
    let container_id = container_id.to_string();
    let error_type = error_type.to_string();
    let error_message = error_message.to_string();
    tokio::spawn(async move {
        let client = reqwest::Client::new();

        let next_resp = client
            .get(format!(
                "http://{}/2018-06-01/runtime/invocation/next",
                runtime_addr
            ))
            .header("Lambda-Runtime-Function-Name", &function_name)
            .header("Lambda-Runtime-Container-Id", &container_id)
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

        // Report a function error
        let error_body = format!(
            r#"{{"errorMessage":"{}","errorType":"{}"}}"#,
            error_message, error_type
        );

        let resp = client
            .post(format!(
                "http://{}/2018-06-01/runtime/invocation/{}/error",
                runtime_addr, request_id
            ))
            .body(error_body)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 202);
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Successful invocation of a Python function (simulated).
/// Verifies the full flow: HTTP invoke → /next → /response → caller gets 200.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn e2e_python_successful_invocation() {
    let (state, _shutdown_tx) =
        build_e2e_state(vec![("python-hello", "python3.12", "main.handler", 30)]).await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let runtime_task = spawn_echo_runtime(
        runtime_addr,
        "python-hello",
        "e2e-container-python-hello",
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/python-hello/invocations",
            invoke_addr
        ))
        .body(r#"{"key":"value"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("X-Amz-Function-Error").is_none(),
        "should not have function error header"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);
    assert_eq!(body["body"]["input"]["key"], "value");
    assert!(body["body"]["message"].as_str().unwrap().contains("Hello"));

    tokio::time::timeout(Duration::from_secs(5), runtime_task)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Successful invocation of a Node.js function (simulated).
/// Verifies the full flow: HTTP invoke → /next → /response → caller gets 200.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn e2e_nodejs_successful_invocation() {
    let (state, _shutdown_tx) =
        build_e2e_state(vec![("nodejs-hello", "nodejs20.x", "index.handler", 30)]).await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let runtime_task = spawn_echo_runtime(
        runtime_addr,
        "nodejs-hello",
        "e2e-container-nodejs-hello",
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/nodejs-hello/invocations",
            invoke_addr
        ))
        .body(r#"{"greeting":"world"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("X-Amz-Function-Error").is_none(),
        "should not have function error header"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);
    assert_eq!(body["body"]["input"]["greeting"], "world");

    tokio::time::timeout(Duration::from_secs(5), runtime_task)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Function error propagation: the container reports a runtime error,
/// and the caller receives a 200 with X-Amz-Function-Error header and
/// error details in the body.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn e2e_function_error_propagation() {
    let (state, _shutdown_tx) =
        build_e2e_state(vec![("error-func", "python3.12", "main.handler", 30)]).await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let runtime_task = spawn_error_runtime(
        runtime_addr,
        "error-func",
        "e2e-container-error-func",
        "ValueError",
        "Intentional test error",
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/error-func/invocations",
            invoke_addr
        ))
        .body(r#"{"trigger":"error"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let error_header = resp
        .headers()
        .get("X-Amz-Function-Error")
        .expect("should have X-Amz-Function-Error header")
        .to_str()
        .unwrap();
    // Function-reported errors use "Handled"; timeout errors use "Unhandled".
    assert_eq!(error_header, "Handled");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body.get("errorMessage")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("Intentional test error"),
        "error message should propagate, got: {}",
        body
    );

    tokio::time::timeout(Duration::from_secs(5), runtime_task)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Concurrent invocations across two different functions (Python and Node.js)
/// running simultaneously. Validates that the system correctly routes
/// invocations and responses for multiple functions in parallel.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn e2e_concurrent_multi_function_invocations() {
    let (state, _shutdown_tx) = build_e2e_state(vec![
        ("py-conc", "python3.12", "main.handler", 30),
        ("js-conc", "nodejs20.x", "index.handler", 30),
    ])
    .await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    // Spawn simulated runtimes for both functions.
    let py_runtime = spawn_echo_runtime(runtime_addr, "py-conc", "e2e-container-py-conc");
    let js_runtime = spawn_echo_runtime(runtime_addr, "js-conc", "e2e-container-js-conc");

    let client = reqwest::Client::new();

    // Send both invocations concurrently.
    let py_client = client.clone();
    let py_addr = invoke_addr;
    let py_invoke = tokio::spawn(async move {
        py_client
            .post(format!(
                "http://{}/2015-03-31/functions/py-conc/invocations",
                py_addr
            ))
            .body(r#"{"lang":"python"}"#)
            .send()
            .await
            .unwrap()
    });

    let js_client = client.clone();
    let js_addr = invoke_addr;
    let js_invoke = tokio::spawn(async move {
        js_client
            .post(format!(
                "http://{}/2015-03-31/functions/js-conc/invocations",
                js_addr
            ))
            .body(r#"{"lang":"nodejs"}"#)
            .send()
            .await
            .unwrap()
    });

    // Wait for both with explicit timeout.
    let py_resp = tokio::time::timeout(Duration::from_secs(10), py_invoke)
        .await
        .expect("python invoke should complete")
        .expect("python invoke should not panic");

    let js_resp = tokio::time::timeout(Duration::from_secs(10), js_invoke)
        .await
        .expect("nodejs invoke should complete")
        .expect("nodejs invoke should not panic");

    assert_eq!(py_resp.status(), 200);
    assert!(py_resp.headers().get("X-Amz-Function-Error").is_none());
    let py_body: serde_json::Value = py_resp.json().await.unwrap();
    assert_eq!(py_body["body"]["input"]["lang"], "python");

    assert_eq!(js_resp.status(), 200);
    assert!(js_resp.headers().get("X-Amz-Function-Error").is_none());
    let js_body: serde_json::Value = js_resp.json().await.unwrap();
    assert_eq!(js_body["body"]["input"]["lang"], "nodejs");

    // Clean up runtimes.
    for handle in [py_runtime, js_runtime] {
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("runtime should complete")
            .expect("runtime should not panic");
    }

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Container cleanup on shutdown: when the shutting_down flag is set,
/// the service should reject new invocations.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn e2e_shutdown_rejects_new_invocations() {
    let (state, _shutdown_tx) =
        build_e2e_state(vec![("shutdown-func", "python3.12", "main.handler", 30)]).await;

    let shutting_down = state.shutting_down.clone();
    let (invoke_addr, _, invoke_handle, runtime_handle) = start_servers(state).await;

    // Mark the service as shutting down.
    shutting_down.store(true, Ordering::Relaxed);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/shutdown-func/invocations",
            invoke_addr
        ))
        .body(r#"{"input":"test"}"#)
        .send()
        .await
        .unwrap();

    // The service should reject the request during shutdown.
    // Depending on implementation, this might be 503 Service Unavailable
    // or another error status.
    assert_ne!(
        resp.status(),
        200,
        "invocations during shutdown should not succeed with 200"
    );

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Verify that invoking a non-existent function returns 404.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn e2e_invoke_nonexistent_function_returns_404() {
    let (state, _shutdown_tx) =
        build_e2e_state(vec![("real-func", "python3.12", "main.handler", 30)]).await;

    let (invoke_addr, _, invoke_handle, runtime_handle) = start_servers(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/nonexistent-func/invocations",
            invoke_addr
        ))
        .body(r#"{"input":"test"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body.get("Type")
            .or(body.get("errorType"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("ResourceNotFound"),
        "expected ResourceNotFoundException, got: {}",
        body
    );

    invoke_handle.abort();
    runtime_handle.abort();
}
