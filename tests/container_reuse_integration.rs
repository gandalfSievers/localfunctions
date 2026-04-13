//! Integration tests for warm container reuse across sequential invocations.
//!
//! Validates that the Busy → Idle state transition works correctly so that a
//! single container can process multiple invocations sequentially (warm reuse)
//! without spawning new containers for each invocation.
//!
//! These tests verify the fix for a bug where containers stayed in the Busy
//! state after completing an invocation, causing:
//!   - SQS event source mapper to create new containers for each message
//!   - Exhaustion of the container limit, blocking all further invocations
//!
//! All tests are gated with `#[ignore]` so that `cargo test` runs without
//! external dependencies. Run with: `cargo test -- --ignored`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use localfunctions::config::{Config, LogFormat};
use localfunctions::container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig};
use localfunctions::function::FunctionsConfig;
use localfunctions::runtime::RuntimeBridge;
use localfunctions::server::AppState;
use localfunctions::types::{ContainerState, FunctionConfig};

/// Build an AppState with one function, one idle container, and the given
/// max_containers limit.
async fn build_reuse_state(
    function_name: &str,
    max_containers: usize,
) -> (
    AppState,
    tokio::sync::watch::Sender<bool>,
    std::net::SocketAddr,
    std::net::SocketAddr,
) {
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
        max_containers,
        docker_network: "localfunctions".into(),
        max_body_size: 6 * 1024 * 1024,
        log_format: LogFormat::Text,
        pull_images: false,
        init_timeout: 10,
        container_acquire_timeout: 5,
        forward_aws_credentials: true,
        mount_aws_credentials: false,
        max_async_body_size: 256 * 1024,
        hot_reload: false,
        hot_reload_debounce_ms: 500,
        domain: None,
        callback_url: "http://127.0.0.1:9600".to_string(),
        runtime_host: "host-gateway".to_string(),
    };

    let docker = bollard::Docker::connect_with_local_defaults().unwrap();

    let mut functions_map = HashMap::new();
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let mut senders = HashMap::new();
    let mut receivers = HashMap::new();
    senders.insert(function_name.to_string(), tx);
    receivers.insert(function_name.to_string(), rx);

    functions_map.insert(
        function_name.to_string(),
        FunctionConfig {
            name: function_name.to_string(),
            runtime: "python3.12".to_string(),
            handler: "main.handler".to_string(),
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

    let functions_config = FunctionsConfig {
        functions: functions_map,
        runtime_images: HashMap::new(),
        event_source_mappings: Vec::new(),
        sns_subscriptions: Vec::new(),
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(senders, receivers, shutdown_rx.clone()));

    let container_registry = Arc::new(ContainerRegistry::new(docker.clone()));
    let container_manager = Arc::new(ContainerManager::new(
        docker.clone(),
        HashMap::new(),
        "localfunctions".into(),
        9601,
        "us-east-1".into(),
        container_registry.clone(),
        max_containers,
        CredentialForwardingConfig::default(),
        "host-gateway".to_string(),
    ));

    // Pre-populate one idle container for the function.
    let container_id = format!("reuse-container-{}", function_name);
    container_manager
        .insert_test_container(
            container_id,
            function_name.to_string(),
            ContainerState::Idle,
        )
        .await;

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
            shutdown_rx,
        )),
    };

    // Start both servers on ephemeral ports.
    let invoke_app = localfunctions::server::invoke_router(state.clone());
    let invoke_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let invoke_addr = invoke_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(invoke_listener, invoke_app).await.unwrap();
    });

    let runtime_app = localfunctions::server::runtime_router(state.clone());
    let runtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let runtime_addr = runtime_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(runtime_listener, runtime_app).await.unwrap();
    });

    (state, shutdown_tx, invoke_addr, runtime_addr)
}

/// Spawn a simulated container runtime that processes multiple invocations
/// sequentially, echoing back the payload each time. This mimics the real
/// Lambda container lifecycle: call /next → process → post /response → repeat.
fn spawn_multi_invocation_runtime(
    runtime_addr: std::net::SocketAddr,
    function_name: &str,
    container_id: &str,
    invocation_count: Arc<AtomicUsize>,
    target_count: usize,
) -> tokio::task::JoinHandle<()> {
    let function_name = function_name.to_string();
    let container_id = container_id.to_string();
    tokio::spawn(async move {
        let client = reqwest::Client::new();

        loop {
            let current = invocation_count.load(Ordering::SeqCst);
            if current >= target_count {
                break;
            }

            // Long-poll for next invocation
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

            if next_resp.status() != 200 {
                // Shutdown or error — exit the loop
                break;
            }

            let request_id = next_resp
                .headers()
                .get("Lambda-Runtime-Aws-Request-Id")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            let payload = next_resp.text().await.unwrap();

            // Process: echo back with container info
            let body = format!(
                r#"{{"statusCode":200,"container":"{}","input":{}}}"#,
                container_id, payload
            );

            let resp = client
                .post(format!(
                    "http://{}/2018-06-01/runtime/invocation/{}/response",
                    runtime_addr, request_id
                ))
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 202);

            invocation_count.fetch_add(1, Ordering::SeqCst);
        }
    })
}

/// Test: A single container processes 10 sequential invocations via warm reuse.
///
/// With max_containers=5, if the container doesn't transition back to Idle
/// after each invocation, subsequent invocations would fail with "max
/// concurrent containers reached" since no warm container would be found
/// and no new slot could be acquired.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn single_container_processes_multiple_invocations_sequentially() {
    let function_name = "reuse-test";
    let max_containers = 5;
    let num_invocations = 10;

    let (state, _shutdown_tx, invoke_addr, runtime_addr) =
        build_reuse_state(function_name, max_containers).await;

    let container_id = format!("reuse-container-{}", function_name);
    let invocation_count = Arc::new(AtomicUsize::new(0));

    // Spawn a simulated runtime that processes multiple invocations
    let runtime_handle = spawn_multi_invocation_runtime(
        runtime_addr,
        function_name,
        &container_id,
        invocation_count.clone(),
        num_invocations,
    );

    // Send invocations one at a time, waiting for each to complete.
    // This verifies that the container transitions Busy → Idle after each
    // invocation, allowing it to be reused for the next one.
    let client = reqwest::Client::new();
    for i in 0..num_invocations {
        let payload = format!(r#"{{"message_index":{}}}"#, i);
        let resp = tokio::time::timeout(
            Duration::from_secs(10),
            client
                .post(format!(
                    "http://{}/2015-03-31/functions/{}/invocations",
                    invoke_addr, function_name
                ))
                .body(payload)
                .send(),
        )
        .await
        .unwrap_or_else(|_| panic!("invocation {} timed out — container likely stuck in Busy state", i))
        .unwrap_or_else(|e| panic!("invocation {} failed: {}", i, e));

        assert_eq!(
            resp.status(),
            200,
            "invocation {} should succeed (status={})",
            i,
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["input"]["message_index"].as_i64().unwrap(),
            i as i64,
            "invocation {} should echo back correct index",
            i
        );
    }

    // All invocations should have been processed
    assert_eq!(
        invocation_count.load(Ordering::SeqCst),
        num_invocations,
        "all {} invocations should have been processed by the single container",
        num_invocations
    );

    // Verify the container is back in Idle state
    assert!(
        state
            .container_manager
            .has_idle_container(function_name)
            .await,
        "container should be Idle after all invocations complete"
    );

    // Verify no extra containers were created (should still be just 1)
    assert_eq!(
        state.container_manager.count().await,
        1,
        "should have exactly 1 container — no extras spawned"
    );

    let _ = tokio::time::timeout(Duration::from_secs(5), runtime_handle).await;
}

/// Test: Self-invocation works — a Lambda function invokes itself via the
/// invoke API during its execution. This exercises the scenario where a
/// function in its "Process" step calls itself recursively.
///
/// Without the Busy → Idle fix, self-invocation would deadlock: the first
/// container stays Busy, no idle containers are found, and the self-invoke
/// tries to acquire a new container slot. With max_containers=1, it would
/// block forever.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn self_invocation_succeeds() {
    let function_name = "self-invoke-func";
    // Allow 2 containers so the self-invocation can get a second container
    let max_containers = 2;

    let (_state, _shutdown_tx, invoke_addr, runtime_addr) =
        build_reuse_state(function_name, max_containers).await;

    let container_id = format!("reuse-container-{}", function_name);

    // Spawn a simulated runtime that:
    // 1. Picks up the first invocation
    // 2. Self-invokes the same function with a "depth" marker
    // 3. The second invocation (depth=1) just returns immediately
    let invoke_url = format!(
        "http://{}/2015-03-31/functions/{}/invocations",
        invoke_addr, function_name
    );

    let runtime_fn = function_name.to_string();
    let runtime_cid = container_id.clone();
    let runtime_handle = tokio::spawn(async move {
        let client = reqwest::Client::new();

        // Process two invocations (the original + the self-invocation response)
        for _ in 0..2 {
            let next_resp = client
                .get(format!(
                    "http://{}/2018-06-01/runtime/invocation/next",
                    runtime_addr
                ))
                .header("Lambda-Runtime-Function-Name", &runtime_fn)
                .header("Lambda-Runtime-Container-Id", &runtime_cid)
                .send()
                .await
                .unwrap();

            if next_resp.status() != 200 {
                break;
            }

            let request_id = next_resp
                .headers()
                .get("Lambda-Runtime-Aws-Request-Id")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            let payload = next_resp.text().await.unwrap();
            let event: serde_json::Value = serde_json::from_str(&payload).unwrap_or_default();
            let depth = event["depth"].as_i64().unwrap_or(0);

            let response_body = if depth == 0 {
                // First invocation: self-invoke with depth=1
                let self_invoke_resp = client
                    .post(&invoke_url)
                    .body(r#"{"depth": 1}"#)
                    .send()
                    .await
                    .unwrap();

                let self_result = self_invoke_resp.text().await.unwrap();
                format!(r#"{{"depth":0,"self_invoke_result":{}}}"#, self_result)
            } else {
                // Recursive invocation: return immediately
                format!(r#"{{"depth":{},"status":"ok"}}"#, depth)
            };

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
        }
    });

    // Need a second container runtime to handle the self-invocation
    // since the first container is busy processing the original invocation.
    let runtime_fn2 = function_name.to_string();
    let runtime_cid2 = "self-invoke-container-2".to_string();
    // Insert a second idle container for the self-invocation
    _state
        .container_manager
        .insert_test_container(
            runtime_cid2.clone(),
            function_name.to_string(),
            ContainerState::Idle,
        )
        .await;

    let runtime_handle2 = tokio::spawn(async move {
        let client = reqwest::Client::new();

        let next_resp = client
            .get(format!(
                "http://{}/2018-06-01/runtime/invocation/next",
                runtime_addr
            ))
            .header("Lambda-Runtime-Function-Name", &runtime_fn2)
            .header("Lambda-Runtime-Container-Id", &runtime_cid2)
            .send()
            .await
            .unwrap();

        if next_resp.status() != 200 {
            return;
        }

        let request_id = next_resp
            .headers()
            .get("Lambda-Runtime-Aws-Request-Id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let payload = next_resp.text().await.unwrap();
        let event: serde_json::Value = serde_json::from_str(&payload).unwrap_or_default();
        let depth = event["depth"].as_i64().unwrap_or(0);

        let response_body = format!(r#"{{"depth":{},"status":"ok"}}"#, depth);

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
    });

    // Trigger the original invocation
    let client = reqwest::Client::new();
    let resp = tokio::time::timeout(
        Duration::from_secs(15),
        client
            .post(format!(
                "http://{}/2015-03-31/functions/{}/invocations",
                invoke_addr, function_name
            ))
            .body(r#"{"depth": 0}"#)
            .send(),
    )
    .await
    .expect("self-invocation should complete within 15 seconds")
    .expect("self-invocation request should not fail");

    assert_eq!(resp.status(), 200, "self-invocation should succeed");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["depth"], 0);
    assert!(body["self_invoke_result"].is_object());
    assert_eq!(body["self_invoke_result"]["status"], "ok");

    let _ = tokio::time::timeout(Duration::from_secs(5), runtime_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), runtime_handle2).await;
}

/// Test: SQS-like sequential processing — 10 messages processed by 1 container
/// with max_containers=5.
///
/// Simulates the SQS event source mapper pattern: messages arrive one at a
/// time (via `invoke_function_with_event`), and a single container handles
/// them all sequentially. Without the Busy → Idle fix, the container would
/// stay Busy after the first message, causing the mapper to create a new
/// container for each subsequent message until the limit is hit.
#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn event_source_sequential_processing_reuses_container() {
    let function_name = "sqs-reuse-test";
    let max_containers = 5;
    let num_messages = 10;

    let (state, _shutdown_tx, _invoke_addr, runtime_addr) =
        build_reuse_state(function_name, max_containers).await;

    let container_id = format!("reuse-container-{}", function_name);
    let invocation_count = Arc::new(AtomicUsize::new(0));

    // Spawn a simulated runtime that processes all messages
    let runtime_handle = spawn_multi_invocation_runtime(
        runtime_addr,
        function_name,
        &container_id,
        invocation_count.clone(),
        num_messages,
    );

    // Simulate event source mapper: invoke the function with events sequentially
    for i in 0..num_messages {
        let payload = bytes::Bytes::from(format!(
            r#"{{"Records":[{{"body":"message-{}"}}]}}"#,
            i
        ));

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            localfunctions::eventsource::invoke_function_with_event(
                &state,
                function_name,
                payload,
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "event invocation {} timed out — container likely stuck in Busy state (processed {} so far)",
                i,
                invocation_count.load(Ordering::SeqCst)
            )
        });

        assert!(
            result.is_ok(),
            "event invocation {} should succeed: {:?}",
            i,
            result.err()
        );
    }

    // Wait for the runtime to finish processing the last message (the count
    // is incremented after the response is posted, so there's a brief race).
    let _ = tokio::time::timeout(Duration::from_secs(5), runtime_handle).await;

    // Verify all messages were processed
    assert_eq!(
        invocation_count.load(Ordering::SeqCst),
        num_messages,
        "all {} messages should have been processed",
        num_messages
    );

    // Verify container count didn't grow — only 1 container should exist
    assert_eq!(
        state.container_manager.count().await,
        1,
        "should still have exactly 1 container — warm reuse should prevent new containers"
    );
}
