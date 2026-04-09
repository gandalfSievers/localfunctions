//! SQS event source integration tests.
//!
//! These tests verify the end-to-end flow: SQS message → poller receives →
//! invokes function via simulated runtime → messages deleted from queue.
//!
//! Requires a local SQS-compatible service (ElasticMQ or LocalStack) at
//! `SQS_ENDPOINT` (default: `http://localhost:9324`).
//!
//! All tests are gated with `#[ignore]` so that `cargo test` runs without
//! external dependencies. Run with: `cargo test -- --ignored`

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::Client as SqsClient;
use localfunctions::config::{Config, LogFormat};
use localfunctions::container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig};
use localfunctions::eventsource::sqs::{SqsEventSourceConfig, SqsPoller};
use localfunctions::eventsource::EventSourcePoller;
use localfunctions::extensions::ExtensionRegistry;
use localfunctions::function::FunctionsConfig;
use localfunctions::metrics::MetricsCollector;
use localfunctions::runtime::RuntimeBridge;
use localfunctions::server::AppState;
use localfunctions::types::{ContainerState, FunctionConfig};
use tokio::sync::watch;

/// Default SQS endpoint for local testing (ElasticMQ default port).
fn sqs_endpoint() -> String {
    std::env::var("SQS_ENDPOINT").unwrap_or_else(|_| "http://localhost:9324".into())
}

/// Build an SQS client pointing at the local endpoint.
async fn build_sqs_client() -> SqsClient {
    let region = aws_sdk_sqs::config::Region::new("us-east-1");
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .no_credentials()
        .endpoint_url(sqs_endpoint())
        .load()
        .await;
    SqsClient::new(&config)
}

/// Create a temporary SQS queue with short visibility timeout for fast test cycles.
async fn create_test_queue(client: &SqsClient, name: &str) -> String {
    let output = client
        .create_queue()
        .queue_name(name)
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout,
            "5",
        )
        .send()
        .await
        .expect("failed to create test queue");
    output.queue_url().unwrap().to_string()
}

/// Delete a test queue (best-effort cleanup).
async fn delete_test_queue(client: &SqsClient, queue_url: &str) {
    let _ = client.delete_queue().queue_url(queue_url).send().await;
}

/// Get approximate message count on a queue.
async fn get_queue_message_count(client: &SqsClient, queue_url: &str) -> i64 {
    let output = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(
            aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages,
        )
        .send()
        .await
        .expect("failed to get queue attributes");
    output
        .attributes()
        .and_then(|a| a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages))
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Build test AppState with a function configured and a simulated runtime
/// that echoes back the event. Returns (state, shutdown_tx, runtime_addr).
async fn build_sqs_test_state(
    function_name: &str,
) -> (
    AppState,
    watch::Sender<bool>,
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
        domain: None,
        callback_url: "http://127.0.0.1:9600".to_string(),
        runtime_host: "host-gateway".to_string(),
    };

    let docker = bollard::Docker::connect_with_local_defaults().unwrap();

    let mut functions_map = HashMap::new();
    let (tx, rx) = tokio::sync::mpsc::channel(10);
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

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(senders, receivers, shutdown_rx.clone()));

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
        "host-gateway".to_string(),
    ));

    // Pre-populate one idle container for the function.
    let container_id = format!("sqs-test-container-{}", function_name);
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
        metrics: Arc::new(MetricsCollector::new()),
        extension_registry: Arc::new(ExtensionRegistry::new(shutdown_rx)),
    };

    // Start the runtime API server so the simulated container can call /next.
    let runtime_app = localfunctions::server::runtime_router(state.clone());
    let runtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let runtime_addr = runtime_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(runtime_listener, runtime_app).await.unwrap();
    });

    (state, shutdown_tx, runtime_addr)
}

/// Spawn a simulated runtime that picks up /next and responds with a success body.
fn spawn_echo_runtime(
    runtime_addr: std::net::SocketAddr,
    function_name: &str,
    container_id: &str,
    response_body: Option<String>,
) -> tokio::task::JoinHandle<String> {
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

        let body = response_body.unwrap_or_else(|| {
            format!(r#"{{"statusCode":200,"input":{}}}"#, payload)
        });

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

        payload
    })
}

/// Spawn a simulated runtime that picks up /next and reports an error.
fn spawn_error_runtime(
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

        let resp = client
            .post(format!(
                "http://{}/2018-06-01/runtime/invocation/{}/error",
                runtime_addr, request_id
            ))
            .body(r#"{"errorMessage":"Test error","errorType":"RuntimeError"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
    })
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// End-to-end: send a message to SQS, poller receives it, invokes function,
/// message is deleted from queue after successful invocation.
#[tokio::test]
#[ignore] // Requires local SQS service — run with `cargo test -- --ignored`
async fn sqs_e2e_successful_invocation_deletes_message() {
    let sqs_client = build_sqs_client().await;
    let queue_name = format!("test-success-{}", uuid::Uuid::new_v4().simple());
    let queue_url = create_test_queue(&sqs_client, &queue_name).await;

    let function_name = "sqs-echo";
    let (state, shutdown_tx, runtime_addr) =
        build_sqs_test_state(function_name).await;

    // Send a message to the queue.
    sqs_client
        .send_message()
        .queue_url(&queue_url)
        .message_body(r#"{"order_id": "123"}"#)
        .send()
        .await
        .expect("failed to send message");

    // Spawn simulated runtime to handle the invocation.
    let runtime_handle = spawn_echo_runtime(
        runtime_addr,
        function_name,
        &format!("sqs-test-container-{}", function_name),
        None,
    );

    // Create and run the SQS poller.
    let sqs_config = SqsEventSourceConfig {
        queue_url: queue_url.clone(),
        function_name: function_name.into(),
        batch_size: 1,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: Some(sqs_endpoint()),
        event_source_arn: None,
        region: Some("us-east-1".into()),
        enabled: true,
    };

    let poller = SqsPoller::new(sqs_config, "us-east-1".into(), "000000000000".into());

    // Run poller in background, shut it down after the runtime responds.
    let poller_state = state.clone();
    let shutdown_rx = shutdown_tx.subscribe();
    let poller_handle = tokio::spawn(async move {
        poller.run(poller_state, shutdown_rx).await;
    });

    // Wait for the runtime to process the invocation.
    let payload = tokio::time::timeout(Duration::from_secs(30), runtime_handle)
        .await
        .expect("runtime should complete in time")
        .expect("runtime should not panic");

    // Verify the payload contains SQS Records.
    let event: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert!(event["Records"].is_array(), "payload should have Records array");
    let records = event["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["body"], r#"{"order_id": "123"}"#);
    assert_eq!(records[0]["eventSource"], "aws:sqs");
    assert!(!records[0]["messageId"].as_str().unwrap().is_empty());
    assert!(!records[0]["receiptHandle"].as_str().unwrap().is_empty());
    assert!(!records[0]["md5OfBody"].as_str().unwrap().is_empty());
    assert_eq!(records[0]["awsRegion"], "us-east-1");
    assert!(records[0]["eventSourceARN"].as_str().unwrap().contains(&queue_name));

    // Give the poller a moment to delete the message after invocation.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify message was deleted from queue.
    let count = get_queue_message_count(&sqs_client, &queue_url).await;
    assert_eq!(count, 0, "message should be deleted after successful invocation");

    // Cleanup.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), poller_handle).await;
    delete_test_queue(&sqs_client, &queue_url).await;
}

/// End-to-end: when function invocation fails, messages remain on the queue.
#[tokio::test]
#[ignore] // Requires local SQS service — run with `cargo test -- --ignored`
async fn sqs_e2e_failed_invocation_leaves_message_on_queue() {
    let sqs_client = build_sqs_client().await;
    let queue_name = format!("test-fail-{}", uuid::Uuid::new_v4().simple());
    let queue_url = create_test_queue(&sqs_client, &queue_name).await;

    let function_name = "sqs-fail";
    let (state, shutdown_tx, runtime_addr) =
        build_sqs_test_state(function_name).await;

    // Send a message.
    sqs_client
        .send_message()
        .queue_url(&queue_url)
        .message_body("fail-me")
        .send()
        .await
        .expect("failed to send message");

    // Spawn error runtime.
    let runtime_handle = spawn_error_runtime(
        runtime_addr,
        function_name,
        &format!("sqs-test-container-{}", function_name),
    );

    let sqs_config = SqsEventSourceConfig {
        queue_url: queue_url.clone(),
        function_name: function_name.into(),
        batch_size: 1,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: Some(sqs_endpoint()),
        event_source_arn: None,
        region: Some("us-east-1".into()),
        enabled: true,
    };

    let poller = SqsPoller::new(sqs_config, "us-east-1".into(), "000000000000".into());

    let poller_state = state.clone();
    let shutdown_rx = shutdown_tx.subscribe();
    let poller_handle = tokio::spawn(async move {
        poller.run(poller_state, shutdown_rx).await;
    });

    // Wait for the error runtime to complete.
    tokio::time::timeout(Duration::from_secs(30), runtime_handle)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    // Give poller time to process.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Stop poller.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), poller_handle).await;

    // Wait for visibility timeout to expire so the message becomes visible.
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Message should still be on the queue (not deleted).
    let count = get_queue_message_count(&sqs_client, &queue_url).await;
    assert!(
        count >= 1,
        "message should remain on queue after failed invocation, got count={}",
        count
    );

    delete_test_queue(&sqs_client, &queue_url).await;
}

/// End-to-end: with ReportBatchItemFailures, only successful messages are deleted.
/// Failed messages remain on the queue.
#[tokio::test]
#[ignore] // Requires local SQS service — run with `cargo test -- --ignored`
async fn sqs_e2e_partial_batch_failure_selective_delete() {
    let sqs_client = build_sqs_client().await;
    let queue_name = format!("test-partial-{}", uuid::Uuid::new_v4().simple());
    let queue_url = create_test_queue(&sqs_client, &queue_name).await;

    let function_name = "sqs-partial";
    let (state, shutdown_tx, runtime_addr) =
        build_sqs_test_state(function_name).await;

    // Send 3 messages.
    let mut sent_ids = Vec::new();
    for i in 0..3 {
        let output = sqs_client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("message-{}", i))
            .send()
            .await
            .expect("failed to send message");
        sent_ids.push(output.message_id().unwrap().to_string());
    }

    // The function will report the second message as a failure.
    let fail_id = sent_ids[1].clone();
    let response_body = format!(
        r#"{{"batchItemFailures": [{{"itemIdentifier": "{}"}}]}}"#,
        fail_id
    );

    let runtime_handle = spawn_echo_runtime(
        runtime_addr,
        function_name,
        &format!("sqs-test-container-{}", function_name),
        Some(response_body),
    );

    let sqs_config = SqsEventSourceConfig {
        queue_url: queue_url.clone(),
        function_name: function_name.into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec!["ReportBatchItemFailures".into()],
        endpoint_url: Some(sqs_endpoint()),
        event_source_arn: None,
        region: Some("us-east-1".into()),
        enabled: true,
    };

    let poller = SqsPoller::new(sqs_config, "us-east-1".into(), "000000000000".into());

    let poller_state = state.clone();
    let shutdown_rx = shutdown_tx.subscribe();
    let poller_handle = tokio::spawn(async move {
        poller.run(poller_state, shutdown_rx).await;
    });

    // Wait for runtime to process.
    tokio::time::timeout(Duration::from_secs(30), runtime_handle)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    // Give poller time to delete successful messages.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Stop poller.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), poller_handle).await;

    // Wait for visibility timeout to expire.
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Only the failed message should remain.
    let count = get_queue_message_count(&sqs_client, &queue_url).await;
    assert_eq!(
        count, 1,
        "only the failed message should remain on queue, got count={}",
        count
    );

    // Verify the remaining message is the one we marked as failed.
    let remaining = sqs_client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .wait_time_seconds(1)
        .send()
        .await
        .expect("failed to receive remaining messages");

    let msgs = remaining.messages().to_vec();
    assert_eq!(msgs.len(), 1, "should have exactly 1 remaining message");
    assert_eq!(msgs[0].body().unwrap(), "message-1", "the failed message should remain");

    delete_test_queue(&sqs_client, &queue_url).await;
}
