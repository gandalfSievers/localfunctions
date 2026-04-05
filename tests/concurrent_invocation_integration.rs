//! Integration test: 5 concurrent invocations to the same function all return
//! correct results.
//!
//! Exercises the full end-to-end concurrent invocation flow:
//!   5 HTTP invoke requests sent in parallel → each gets a separate container
//!   (simulated) → each container picks up its invocation via /next → each
//!   responds via /response → all 5 invoke callers receive correct results.
//!
//! Validates:
//!   - Multiple invocations of the same function run concurrently
//!   - All 5 requests return 200 with correct response bodies
//!   - Container pool is thread-safe under concurrent access

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use localfunctions::config::{Config, LogFormat};
use localfunctions::container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig};
use localfunctions::function::FunctionsConfig;
use localfunctions::runtime::RuntimeBridge;
use localfunctions::server::AppState;
use localfunctions::types::FunctionConfig;

/// Build an AppState with a single function and N pre-populated idle containers.
async fn build_concurrent_state(
    function_name: &str,
    num_containers: usize,
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
        init_timeout: 10,
        container_acquire_timeout: 10,
        forward_aws_credentials: true,
        mount_aws_credentials: false,
        max_async_body_size: 256 * 1024,
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
        },
    );

    let functions = FunctionsConfig {
        functions: functions_map,
        runtime_images: HashMap::new(),
    };

    let (tx, rx) = tokio::sync::mpsc::channel(100);
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

    // Pre-populate N idle containers so the invoke handler doesn't attempt
    // real Docker operations in tests.
    for i in 0..num_containers {
        container_manager
            .insert_test_container(
                format!("test-container-{}", i),
                function_name.into(),
                localfunctions::types::ContainerState::Idle,
            )
            .await;
    }

    let state = AppState {
        config: Arc::new(config),
        container_registry,
        container_manager,
        docker,
        functions: Arc::new(functions),
        shutting_down: Arc::new(AtomicBool::new(false)),
        runtime_bridge,
        metrics: Arc::new(localfunctions::metrics::MetricsCollector::new()),
        extension_registry: Arc::new(localfunctions::extensions::ExtensionRegistry::new(
            tokio::sync::watch::channel(false).1,
        )),
    };

    (state, shutdown_tx)
}

#[tokio::test]
#[ignore] // Requires Docker daemon — run with `cargo test -- --ignored`
async fn five_concurrent_invocations_all_return_correct_results() {
    let (state, _shutdown_tx) = build_concurrent_state("conc-func", 5).await;

    // Start the Invoke API server on an ephemeral port.
    let invoke_app = localfunctions::server::invoke_router(state.clone());
    let invoke_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let invoke_addr = invoke_listener.local_addr().unwrap();

    let invoke_handle = tokio::spawn(async move {
        axum::serve(invoke_listener, invoke_app).await.unwrap();
    });

    // Start the Runtime API server on an ephemeral port.
    let runtime_app = localfunctions::server::runtime_router(state.clone());
    let runtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let runtime_addr = runtime_listener.local_addr().unwrap();

    let runtime_handle = tokio::spawn(async move {
        axum::serve(runtime_listener, runtime_app).await.unwrap();
    });

    // Spawn 5 simulated container runtimes. Each polls /next for an
    // invocation and responds with a unique result based on the request_id.
    let mut runtime_handles = Vec::new();
    for i in 0..5 {
        let addr = runtime_addr;
        let container_id = format!("test-container-{}", i);
        runtime_handles.push(tokio::spawn(async move {
            let client = reqwest::Client::new();

            // Poll /next — blocks until an invocation is available.
            let next_resp = client
                .get(format!(
                    "http://{}/2018-06-01/runtime/invocation/next",
                    addr
                ))
                .header("Lambda-Runtime-Function-Name", "conc-func")
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

            // Respond with a result that includes the original payload and
            // the container_id, so we can verify correct routing.
            let response_body = format!(
                r#"{{"container":"{}","echo":{}}}"#,
                container_id, payload
            );

            let resp = client
                .post(format!(
                    "http://{}/2018-06-01/runtime/invocation/{}/response",
                    addr, request_id
                ))
                .body(response_body)
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status(), 202);
        }));
    }

    // Send 5 concurrent invocation requests, each with a unique payload.
    let client = reqwest::Client::new();
    let mut invoke_handles = Vec::new();
    for i in 0..5 {
        let client = client.clone();
        let addr = invoke_addr;
        invoke_handles.push(tokio::spawn(async move {
            let payload = format!(r#"{{"index":{}}}"#, i);
            let resp = client
                .post(format!(
                    "http://{}/2015-03-31/functions/conc-func/invocations",
                    addr
                ))
                .body(payload.clone())
                .send()
                .await
                .unwrap();

            assert_eq!(
                resp.status(),
                200,
                "invocation {} should succeed",
                i
            );

            // Verify no error header
            assert!(
                resp.headers().get("X-Amz-Function-Error").is_none(),
                "invocation {} should not have error header",
                i
            );

            let body: serde_json::Value = resp.json().await.unwrap();
            // The response should echo back our payload
            assert_eq!(
                body["echo"]["index"].as_i64().unwrap(),
                i as i64,
                "invocation {} should echo back the correct index",
                i
            );

            i
        }));
    }

    // Wait for all invocations to complete and verify all succeeded.
    let mut completed = Vec::new();
    for handle in invoke_handles {
        let result = tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("invocation should complete within 10 seconds")
            .expect("invocation task should not panic");
        completed.push(result);
    }

    // Verify we got all 5 results.
    completed.sort();
    assert_eq!(completed, vec![0, 1, 2, 3, 4]);

    // Wait for all simulated runtimes to complete.
    for handle in runtime_handles {
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("runtime should complete")
            .expect("runtime task should not panic");
    }

    // Clean up
    invoke_handle.abort();
    runtime_handle.abort();
}
