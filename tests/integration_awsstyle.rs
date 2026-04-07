//! Integration tests that verify AWS-style virtual hosted Lambda routing.
//!
//! These tests invoke functions using the Host header pattern:
//!   `{function}.lambda.{region}.amazonaws.com`
//!
//! rather than path-based routing. The virtual host middleware rewrites the
//! request URI so that existing path-based handlers process the request
//! transparently.
//!
//! All tests are gated with `#[ignore]` so that `cargo test` (unit tests)
//! runs without a Docker dependency. Run with:
//!   `cargo test --test integration_awsstyle -- --ignored`
//!
//! For full DNS-based testing with dnsmasq, use docker-compose.test.awsstyle.yml.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use localfunctions::config::{Config, LogFormat};
use localfunctions::container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig};
use localfunctions::function::FunctionsConfig;
use localfunctions::runtime::RuntimeBridge;
use localfunctions::server::AppState;
use localfunctions::types::{ContainerState, FunctionConfig};

/// Build an AppState with the given functions and pre-populated idle containers.
async fn build_awsstyle_state(
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
        forward_aws_credentials: true,
        mount_aws_credentials: false,
        max_async_body_size: 256 * 1024,
        hot_reload: false,
        hot_reload_debounce_ms: 500,
        domain: None, // AWS-style routing is always enabled regardless
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
        CredentialForwardingConfig::default(),
    ));

    // Pre-populate one idle container per function.
    for (name, _, _, _) in &functions {
        container_manager
            .insert_test_container(
                format!("awsstyle-container-{}", name),
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
        metrics: Arc::new(localfunctions::metrics::MetricsCollector::new()),
        extension_registry: Arc::new(localfunctions::extensions::ExtensionRegistry::new(
            tokio::sync::watch::channel(false).1,
        )),
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

// ---------------------------------------------------------------------------
// Tests — AWS-style virtual host routing
// ---------------------------------------------------------------------------

/// Invoke a Python function with both an AWS-style Host header and the
/// standard Lambda API path. The middleware detects the Lambda API prefix
/// in the path and skips rewriting, so the request routes directly to the
/// standard invoke handler.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test --test integration_awsstyle -- --ignored`
async fn awsstyle_python_invocation_via_host_header() {
    let (state, _shutdown_tx) =
        build_awsstyle_state(vec![("python-hello", "python3.12", "main.handler", 30)]).await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let runtime_task = spawn_echo_runtime(
        runtime_addr,
        "python-hello",
        "awsstyle-container-python-hello",
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/python-hello/invocations",
            invoke_addr
        ))
        .header(
            "host",
            "python-hello.lambda.us-east-1.amazonaws.com:9600",
        )
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

    tokio::time::timeout(Duration::from_secs(5), runtime_task)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Invoke a Node.js function with both an AWS-style Host header and the
/// standard Lambda API path. The middleware skips rewriting for Lambda API
/// paths, routing directly to the standard invoke handler.
#[tokio::test]
#[ignore]
async fn awsstyle_nodejs_invocation_via_host_header() {
    let (state, _shutdown_tx) =
        build_awsstyle_state(vec![("nodejs-hello", "nodejs20.x", "index.handler", 30)]).await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let runtime_task = spawn_echo_runtime(
        runtime_addr,
        "nodejs-hello",
        "awsstyle-container-nodejs-hello",
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/nodejs-hello/invocations",
            invoke_addr
        ))
        .header(
            "host",
            "nodejs-hello.lambda.us-east-1.amazonaws.com:9600",
        )
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

/// Concurrent invocations of two functions with AWS-style Host headers and
/// standard Lambda API paths. Validates that the middleware correctly skips
/// rewriting for both when under concurrent load.
#[tokio::test]
#[ignore]
async fn awsstyle_concurrent_invocations_via_host_header() {
    let (state, _shutdown_tx) = build_awsstyle_state(vec![
        ("py-aws", "python3.12", "main.handler", 30),
        ("js-aws", "nodejs20.x", "index.handler", 30),
    ])
    .await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let py_runtime = spawn_echo_runtime(runtime_addr, "py-aws", "awsstyle-container-py-aws");
    let js_runtime = spawn_echo_runtime(runtime_addr, "js-aws", "awsstyle-container-js-aws");

    let client = reqwest::Client::new();

    let py_client = client.clone();
    let py_addr = invoke_addr;
    let py_invoke = tokio::spawn(async move {
        py_client
            .post(format!(
                "http://{}/2015-03-31/functions/py-aws/invocations",
                py_addr
            ))
            .header("host", "py-aws.lambda.us-east-1.amazonaws.com:9600")
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
                "http://{}/2015-03-31/functions/js-aws/invocations",
                js_addr
            ))
            .header("host", "js-aws.lambda.us-east-1.amazonaws.com:9600")
            .body(r#"{"lang":"nodejs"}"#)
            .send()
            .await
            .unwrap()
    });

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

    for handle in [py_runtime, js_runtime] {
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("runtime should complete")
            .expect("runtime should not panic");
    }

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Invoke a non-existent function via AWS-style Host header.
/// Should return 404 ResourceNotFoundException.
#[tokio::test]
#[ignore]
async fn awsstyle_nonexistent_function_returns_404() {
    let (state, _shutdown_tx) =
        build_awsstyle_state(vec![("real-func", "python3.12", "main.handler", 30)]).await;

    let (invoke_addr, _, invoke_handle, runtime_handle) = start_servers(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/nonexistent-func/invocations",
            invoke_addr
        ))
        .header(
            "host",
            "nonexistent-func.lambda.us-east-1.amazonaws.com:9600",
        )
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

/// Verify that requests with a different region in the Host header still
/// route correctly — the region is informational, not used for routing.
#[tokio::test]
#[ignore]
async fn awsstyle_different_region_routes_correctly() {
    let (state, _shutdown_tx) =
        build_awsstyle_state(vec![("region-func", "python3.12", "main.handler", 30)]).await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let runtime_task = spawn_echo_runtime(
        runtime_addr,
        "region-func",
        "awsstyle-container-region-func",
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/region-func/invocations",
            invoke_addr
        ))
        .header(
            "host",
            "region-func.lambda.eu-west-1.amazonaws.com:9600",
        )
        .body(r#"{"region":"eu-west-1"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);
    assert_eq!(body["body"]["input"]["region"], "eu-west-1");

    tokio::time::timeout(Duration::from_secs(5), runtime_task)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Invoke a Python function via AWS-style Host header with a simple root path.
///
/// This is the intended virtual host pattern: the Host header identifies the
/// function and the path is just `/` — no function name in the URI. The
/// middleware rewrites the path to `/{function_name}` so the Function URL
/// catch-all route handles it.
#[tokio::test]
#[ignore]
async fn awsstyle_host_only_root_path_python() {
    let (state, _shutdown_tx) =
        build_awsstyle_state(vec![("python-hello", "python3.12", "main.handler", 30)]).await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let runtime_task = spawn_echo_runtime(
        runtime_addr,
        "python-hello",
        "awsstyle-container-python-hello",
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/", invoke_addr))
        .header(
            "host",
            "python-hello.lambda.us-east-1.amazonaws.com:9600",
        )
        .body(r#"{"key":"host-only"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("X-Amz-Function-Error").is_none(),
        "should not have function error header"
    );

    // The Function URL handler wraps the request body in a v2.0 event, which
    // the echo runtime returns as its "input". Verify the original body is
    // present inside the Function URL event's `body` field.
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);
    let event_body = body["body"]["input"]["body"]
        .as_str()
        .expect("Function URL event should contain original body as string");
    let parsed: serde_json::Value = serde_json::from_str(event_body).unwrap();
    assert_eq!(parsed["key"], "host-only");

    tokio::time::timeout(Duration::from_secs(5), runtime_task)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Invoke a Node.js function via AWS-style Host header with a sub-path.
///
/// Sends Host: nodejs-hello.lambda.us-east-1.amazonaws.com with path /event
/// (no function name in the URI). The middleware rewrites to
/// /nodejs-hello/event which matches the Function URL catch-all route.
#[tokio::test]
#[ignore]
async fn awsstyle_host_only_subpath_nodejs() {
    let (state, _shutdown_tx) =
        build_awsstyle_state(vec![("nodejs-hello", "nodejs20.x", "index.handler", 30)]).await;

    let (invoke_addr, runtime_addr, invoke_handle, runtime_handle) =
        start_servers(state).await;

    let runtime_task = spawn_echo_runtime(
        runtime_addr,
        "nodejs-hello",
        "awsstyle-container-nodejs-hello",
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/event", invoke_addr))
        .header(
            "host",
            "nodejs-hello.lambda.us-east-1.amazonaws.com:9600",
        )
        .body(r#"{"greeting":"host-only"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("X-Amz-Function-Error").is_none(),
        "should not have function error header"
    );

    // The Function URL handler wraps the request body in a v2.0 event, which
    // the echo runtime returns as its "input". Verify the original body is
    // present inside the Function URL event's `body` field.
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);
    let event_body = body["body"]["input"]["body"]
        .as_str()
        .expect("Function URL event should contain original body as string");
    let parsed: serde_json::Value = serde_json::from_str(event_body).unwrap();
    assert_eq!(parsed["greeting"], "host-only");

    tokio::time::timeout(Duration::from_secs(5), runtime_task)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    invoke_handle.abort();
    runtime_handle.abort();
}

/// Shutdown rejects invocations via AWS-style Host header too.
#[tokio::test]
#[ignore]
async fn awsstyle_shutdown_rejects_invocations() {
    let (state, _shutdown_tx) =
        build_awsstyle_state(vec![("shutdown-func", "python3.12", "main.handler", 30)]).await;

    let shutting_down = state.shutting_down.clone();
    let (invoke_addr, _, invoke_handle, runtime_handle) = start_servers(state).await;

    shutting_down.store(true, Ordering::Relaxed);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{}/2015-03-31/functions/shutdown-func/invocations",
            invoke_addr
        ))
        .header(
            "host",
            "shutdown-func.lambda.us-east-1.amazonaws.com:9600",
        )
        .body(r#"{"input":"test"}"#)
        .send()
        .await
        .unwrap();

    assert_ne!(
        resp.status(),
        200,
        "invocations during shutdown should not succeed with 200"
    );

    invoke_handle.abort();
    runtime_handle.abort();
}
