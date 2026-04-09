//! SNS push-subscription integration tests.
//!
//! These tests verify the end-to-end flow: SNS topic → localfunctions push
//! endpoint → function invocation via simulated runtime. The subscription
//! lifecycle (subscribe on startup, auto-confirm, unsubscribe on shutdown) is
//! also exercised.
//!
//! Requires a local SNS-compatible service (LocalStack) at `SNS_ENDPOINT`
//! (default: `http://localhost:4566`).
//!
//! All tests are gated with `#[ignore]` so that `cargo test` runs without
//! external dependencies. Run with: `cargo test -- --ignored`

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sns::Client as SnsClient;
use localfunctions::config::{Config, LogFormat};
use localfunctions::container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig};
use localfunctions::eventsource::sns::SnsSubscriptionConfig;
use localfunctions::eventsource::EventSourceManager;
use localfunctions::extensions::ExtensionRegistry;
use localfunctions::function::FunctionsConfig;
use localfunctions::metrics::MetricsCollector;
use localfunctions::runtime::RuntimeBridge;
use localfunctions::server::AppState;
use localfunctions::types::{ContainerState, FunctionConfig};
use tokio::sync::watch;

/// Default SNS endpoint for local testing (jameskbride/local-sns default port).
fn sns_endpoint() -> String {
    std::env::var("SNS_ENDPOINT").unwrap_or_else(|_| "http://localhost:9911".into())
}

/// Build an SNS client pointing at the local endpoint.
async fn build_sns_client() -> SnsClient {
    let region = aws_sdk_sns::config::Region::new("us-east-1");
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .no_credentials()
        .endpoint_url(sns_endpoint())
        .load()
        .await;
    SnsClient::new(&config)
}

/// Create a temporary SNS topic and return its ARN.
async fn create_test_topic(client: &SnsClient, name: &str) -> String {
    let output = client
        .create_topic()
        .name(name)
        .send()
        .await
        .expect("failed to create test topic");
    output.topic_arn().unwrap().to_string()
}

/// Delete a test topic (best-effort cleanup).
async fn delete_test_topic(client: &SnsClient, topic_arn: &str) {
    let _ = client.delete_topic().topic_arn(topic_arn).send().await;
}

/// Build test AppState returning (state, shutdown_tx, runtime_addr, invoke_addr).
/// Both the runtime and invoke API servers are started on random ports.
async fn build_sns_test_state(
    function_name: &str,
) -> (
    AppState,
    watch::Sender<bool>,
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
        callback_url: "http://127.0.0.1:0".to_string(),
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
    let container_id = format!("sns-test-container-{}", function_name);
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

    // Start runtime API server.
    let runtime_app = localfunctions::server::runtime_router(state.clone());
    let runtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let runtime_addr = runtime_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(runtime_listener, runtime_app).await.unwrap();
    });

    // Start invoke API server (includes /sns/:function_name).
    let invoke_app = localfunctions::server::invoke_router(state.clone());
    let invoke_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let invoke_addr = invoke_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(invoke_listener, invoke_app).await.unwrap();
    });

    // Update callback_url with actual invoke port.
    let updated_config = Config {
        callback_url: format!("http://127.0.0.1:{}", invoke_addr.port()),
        runtime_host: "host-gateway".to_string(),
        ..(*state.config).clone()
    };
    let state = AppState {
        config: Arc::new(updated_config),
        ..state
    };

    (state, shutdown_tx, runtime_addr, invoke_addr)
}

/// Spawn a simulated runtime that communicates over the runtime HTTP API.
/// Returns the invocation payload received by the "function".
fn spawn_echo_runtime(
    runtime_addr: std::net::SocketAddr,
    function_name: &str,
    container_id: &str,
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

        let body = format!(r#"{{"statusCode":200,"input":{}}}"#, payload);

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

/// Spawn a simulated runtime that communicates over the runtime HTTP API
/// and reports an error.
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

/// End-to-end: POST a notification to the SNS push endpoint, verify the
/// function receives a correctly-formatted SNS Lambda event.
#[tokio::test]
#[ignore] // Requires local SNS service — run with `cargo test -- --ignored`
async fn sns_e2e_notification_invokes_function() {
    let function_name = "sns-echo";
    let (_state, shutdown_tx, runtime_addr, invoke_addr) =
        build_sns_test_state(function_name).await;

    // Spawn simulated runtime to handle the invocation.
    let runtime_handle = spawn_echo_runtime(
        runtime_addr,
        function_name,
        &format!("sns-test-container-{}", function_name),
    );

    // Simulate an SNS Notification POST to the push endpoint.
    let sns_message = serde_json::json!({
        "Type": "Notification",
        "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
        "TopicArn": "arn:aws:sns:us-east-1:000000000000:test-topic",
        "Subject": "Test Subject",
        "Message": r#"{"order_id": 123}"#,
        "Timestamp": "2024-01-15T12:00:00.000Z",
        "SignatureVersion": "1",
        "Signature": "EXAMPLE",
        "SigningCertURL": "https://sns.us-east-1.amazonaws.com/example.pem",
        "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe",
        "MessageAttributes": {
            "event_type": {
                "Type": "String",
                "Value": "order.created"
            }
        }
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/sns/{}", invoke_addr, function_name))
        .header("x-amz-sns-message-type", "Notification")
        .header("content-type", "application/json")
        .body(sns_message.to_string())
        .send()
        .await
        .expect("failed to POST notification");

    assert_eq!(resp.status(), 200, "SNS notification handler should return 200");

    // Wait for the runtime to process the invocation.
    let payload = tokio::time::timeout(Duration::from_secs(10), runtime_handle)
        .await
        .expect("runtime should complete in time")
        .expect("runtime should not panic");

    // Verify the payload is a correctly-formatted SNS Lambda event.
    let event: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert!(event["Records"].is_array(), "payload should have Records array");
    let records = event["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);

    let record = &records[0];
    assert_eq!(record["EventVersion"], "1.0");
    assert_eq!(record["EventSource"], "aws:sns");
    assert!(
        record["EventSubscriptionArn"].as_str().unwrap().contains("sns-echo"),
        "subscription ARN should contain function name"
    );

    let sns = &record["Sns"];
    assert_eq!(sns["Type"], "Notification");
    assert_eq!(sns["MessageId"], "95df01b4-ee98-5cb9-9903-4c221d41eb5e");
    assert_eq!(sns["TopicArn"], "arn:aws:sns:us-east-1:000000000000:test-topic");
    assert_eq!(sns["Subject"], "Test Subject");
    assert_eq!(sns["Message"], r#"{"order_id": 123}"#);
    assert_eq!(
        sns["MessageAttributes"]["event_type"]["Type"], "String",
        "message attributes should be preserved"
    );
    assert_eq!(
        sns["MessageAttributes"]["event_type"]["Value"], "order.created",
        "message attribute value should be preserved"
    );

    let _ = shutdown_tx.send(true);
}

/// End-to-end: when function invocation fails, the SNS endpoint returns 500
/// so SNS knows to retry.
#[tokio::test]
#[ignore] // Requires local SNS service — run with `cargo test -- --ignored`
async fn sns_e2e_failed_invocation_returns_500() {
    let function_name = "sns-fail";
    let (_state, shutdown_tx, runtime_addr, invoke_addr) =
        build_sns_test_state(function_name).await;

    // Spawn error runtime.
    let runtime_handle = spawn_error_runtime(
        runtime_addr,
        function_name,
        &format!("sns-test-container-{}", function_name),
    );

    let sns_message = serde_json::json!({
        "Type": "Notification",
        "MessageId": "fail-msg-001",
        "TopicArn": "arn:aws:sns:us-east-1:000000000000:test-topic",
        "Message": "fail-me",
        "Timestamp": "2024-01-15T12:00:00.000Z",
        "SignatureVersion": "1",
        "Signature": "EXAMPLE",
        "SigningCertURL": "https://sns.us-east-1.amazonaws.com/example.pem",
        "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe"
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/sns/{}", invoke_addr, function_name))
        .header("x-amz-sns-message-type", "Notification")
        .header("content-type", "application/json")
        .body(sns_message.to_string())
        .send()
        .await
        .expect("failed to POST notification");

    // The handler should return 500 when the function errors, so SNS retries.
    assert_eq!(
        resp.status(),
        500,
        "SNS notification handler should return 500 on function error"
    );

    tokio::time::timeout(Duration::from_secs(10), runtime_handle)
        .await
        .expect("runtime should complete")
        .expect("runtime should not panic");

    let _ = shutdown_tx.send(true);
}

/// End-to-end: the SNS push endpoint auto-confirms subscription by fetching
/// the SubscribeURL.
#[tokio::test]
#[ignore] // Requires local SNS service — run with `cargo test -- --ignored`
async fn sns_e2e_subscription_confirmation_auto_confirms() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let function_name = "sns-echo";
    let (_state, shutdown_tx, _runtime_addr, invoke_addr) =
        build_sns_test_state(function_name).await;

    // Start a mock server to serve the SubscribeURL confirmation.
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/confirm"))
        .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let subscribe_url = format!("{}/confirm", mock_server.uri());

    let confirmation = serde_json::json!({
        "Type": "SubscriptionConfirmation",
        "MessageId": "confirm-001",
        "Token": "abc123",
        "TopicArn": "arn:aws:sns:us-east-1:000000000000:test-topic",
        "SubscribeURL": subscribe_url
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/sns/{}", invoke_addr, function_name))
        .header("x-amz-sns-message-type", "SubscriptionConfirmation")
        .header("content-type", "application/json")
        .body(confirmation.to_string())
        .send()
        .await
        .expect("failed to POST confirmation");

    assert_eq!(
        resp.status(),
        200,
        "subscription confirmation should succeed"
    );

    // Wiremock will verify the mock was called exactly once when dropped.
    let _ = shutdown_tx.send(true);
}

/// End-to-end: SNS subscription lifecycle — subscribe to a real local SNS
/// topic, publish a message, verify the function receives it.
#[tokio::test]
#[ignore] // Requires local SNS service — run with `cargo test -- --ignored`
async fn sns_e2e_subscribe_publish_invoke() {
    let sns_client = build_sns_client().await;
    let topic_name = format!("test-sns-{}", uuid::Uuid::new_v4().simple());
    let topic_arn = create_test_topic(&sns_client, &topic_name).await;

    let function_name = "sns-sub";
    let (_state, shutdown_tx, runtime_addr, invoke_addr) =
        build_sns_test_state(function_name).await;

    // Spawn simulated runtime.
    let runtime_handle = spawn_echo_runtime(
        runtime_addr,
        function_name,
        &format!("sns-test-container-{}", function_name),
    );

    // Subscribe localfunctions to the SNS topic.
    let callback_url = format!("http://127.0.0.1:{}", invoke_addr.port());
    let sns_config = SnsSubscriptionConfig {
        topic_arn: topic_arn.clone(),
        function_name: function_name.to_string(),
        filter_policy: None,
        filter_policy_scope: None,
        endpoint_url: Some(sns_endpoint()),
        region: Some("us-east-1".to_string()),
        enabled: true,
    };

    let handle = localfunctions::eventsource::sns::subscribe(
        &sns_config,
        &callback_url,
        "us-east-1",
    )
    .await;
    assert!(handle.is_some(), "subscription should succeed");
    let handle = handle.unwrap();

    // Give the subscription time to be confirmed (auto-confirm via push endpoint).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Publish a message to the topic.
    sns_client
        .publish()
        .topic_arn(&topic_arn)
        .message(r#"{"order_id": 456}"#)
        .subject("Order Event")
        .send()
        .await
        .expect("failed to publish message");

    // Wait for the runtime to receive the invocation.
    let payload = tokio::time::timeout(Duration::from_secs(15), runtime_handle)
        .await
        .expect("runtime should complete in time")
        .expect("runtime should not panic");

    // Verify the payload is a valid SNS Lambda event.
    let event: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert!(event["Records"].is_array(), "payload should have Records array");
    let records = event["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);

    let record = &records[0];
    assert_eq!(record["EventVersion"], "1.0");
    assert_eq!(record["EventSource"], "aws:sns");

    let sns = &record["Sns"];
    assert_eq!(sns["TopicArn"], topic_arn);
    // The message body should contain our published content.
    let message = sns["Message"].as_str().unwrap_or("");
    assert!(
        message.contains("order_id"),
        "SNS message should contain published content, got: {}",
        message
    );

    // Cleanup: unsubscribe and delete topic.
    localfunctions::eventsource::sns::unsubscribe(&handle).await;
    delete_test_topic(&sns_client, &topic_arn).await;
    let _ = shutdown_tx.send(true);
}

/// Verify the EventSourceManager tracks SNS handles and unsubscribes on shutdown.
#[tokio::test]
#[ignore] // Requires local SNS service — run with `cargo test -- --ignored`
async fn sns_e2e_manager_unsubscribes_on_shutdown() {
    let sns_client = build_sns_client().await;
    let topic_name = format!("test-mgr-{}", uuid::Uuid::new_v4().simple());
    let topic_arn = create_test_topic(&sns_client, &topic_name).await;

    let function_name = "sns-mgr";
    let (_state, shutdown_tx, _runtime_addr, invoke_addr) =
        build_sns_test_state(function_name).await;

    let callback_url = format!("http://127.0.0.1:{}", invoke_addr.port());
    let sns_config = SnsSubscriptionConfig {
        topic_arn: topic_arn.clone(),
        function_name: function_name.to_string(),
        filter_policy: None,
        filter_policy_scope: None,
        endpoint_url: Some(sns_endpoint()),
        region: Some("us-east-1".to_string()),
        enabled: true,
    };

    let handle = localfunctions::eventsource::sns::subscribe(
        &sns_config,
        &callback_url,
        "us-east-1",
    )
    .await;
    assert!(handle.is_some(), "subscription should succeed");
    let handle = handle.unwrap();

    // Give subscription time to confirm.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify the subscription exists on the topic.
    let subs_before = sns_client
        .list_subscriptions_by_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .expect("failed to list subscriptions");
    let sub_count_before = subs_before.subscriptions().len();
    assert!(
        sub_count_before >= 1,
        "should have at least 1 subscription, got {}",
        sub_count_before
    );

    // Create a manager, add the handle, and shut down.
    let mut manager = EventSourceManager::new();
    manager.add_sns_handle(handle);

    manager.shutdown().await;

    // Give LocalStack time to process the unsubscribe.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify the subscription was removed.
    let subs_after = sns_client
        .list_subscriptions_by_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .expect("failed to list subscriptions");

    let sub_count_after = subs_after.subscriptions().len();
    assert!(
        sub_count_after < sub_count_before,
        "subscription should have been removed: before={}, after={}",
        sub_count_before,
        sub_count_after
    );

    delete_test_topic(&sns_client, &topic_arn).await;
    let _ = shutdown_tx.send(true);
}

/// Verify that POST to an unknown function returns 404.
#[tokio::test]
#[ignore]
async fn sns_e2e_unknown_function_returns_404() {
    let function_name = "sns-echo";
    let (_state, shutdown_tx, _runtime_addr, invoke_addr) =
        build_sns_test_state(function_name).await;

    let sns_message = serde_json::json!({
        "Type": "Notification",
        "MessageId": "msg-001",
        "TopicArn": "arn:aws:sns:us-east-1:000000000000:test-topic",
        "Message": "hello"
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/sns/{}", invoke_addr, "nonexistent-function"))
        .header("x-amz-sns-message-type", "Notification")
        .header("content-type", "application/json")
        .body(sns_message.to_string())
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(resp.status(), 404, "unknown function should return 404");

    let _ = shutdown_tx.send(true);
}

/// Verify that a malformed SNS payload returns 400.
#[tokio::test]
#[ignore]
async fn sns_e2e_malformed_payload_returns_400() {
    let function_name = "sns-echo";
    let (_state, shutdown_tx, _runtime_addr, invoke_addr) =
        build_sns_test_state(function_name).await;

    let client = reqwest::Client::new();

    // Missing message type header.
    let resp = client
        .post(format!("http://{}/sns/{}", invoke_addr, function_name))
        .header("content-type", "application/json")
        .body(r#"{"Type": "Notification", "Message": "hello"}"#)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(
        resp.status(),
        400,
        "missing message type header should return 400"
    );

    // Invalid JSON with notification type.
    let resp = client
        .post(format!("http://{}/sns/{}", invoke_addr, function_name))
        .header("x-amz-sns-message-type", "Notification")
        .header("content-type", "application/json")
        .body("not json at all")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(
        resp.status(),
        400,
        "invalid JSON should return 400"
    );

    let _ = shutdown_tx.send(true);
}
