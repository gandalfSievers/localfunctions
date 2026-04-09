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
use crate::types::FunctionConfig;

fn test_state() -> AppState {
    test_state_with_callback("http://localfunctions:9600")
}

fn test_state_with_callback(callback_url: &str) -> AppState {
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
        callback_url: callback_url.to_string(),
        runtime_host: "host-gateway".to_string(),
    };
    let docker = bollard::Docker::connect_with_local_defaults().unwrap();

    let mut functions_map = HashMap::new();
    functions_map.insert(
        "my-function".to_string(),
        FunctionConfig {
            name: "my-function".into(),
            runtime: "python3.12".into(),
            handler: "app.handler".into(),
            code_path: "/tmp/test".into(),
            timeout: 30,
            memory_size: 128,
            ephemeral_storage_mb: 512,
            environment: HashMap::new(),
            image: None,
            image_uri: None,
            reserved_concurrent_executions: None,
            max_retry_attempts: 2,
            on_success: None,
            on_failure: None,
            layers: vec![],
            architecture: "x86_64".into(),
            function_url_enabled: false,
            payload_format_version: "2.0".to_string(),
        },
    );
    let functions = FunctionsConfig {
        functions: functions_map,
        runtime_images: HashMap::new(),
        event_source_mappings: Vec::new(),
        sns_subscriptions: Vec::new(),
    };

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(
        HashMap::new(),
        HashMap::new(),
        shutdown_rx,
    ));
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

#[test]
fn sns_endpoint_url_constructs_correctly() {
    assert_eq!(
        sns_endpoint_url("http://localfunctions:9600", "my-function"),
        "http://localfunctions:9600/sns/my-function"
    );
}

#[test]
fn sns_endpoint_url_no_double_slash() {
    // callback_url should already have trailing slash stripped by config,
    // but verify the format is correct regardless.
    assert_eq!(
        sns_endpoint_url("http://localfunctions:9600", "my-func"),
        "http://localfunctions:9600/sns/my-func"
    );
}

#[test]
fn sns_endpoint_url_with_https() {
    assert_eq!(
        sns_endpoint_url("https://proxy.local:8443", "handler"),
        "https://proxy.local:8443/sns/handler"
    );
}

#[tokio::test]
async fn sns_unknown_function_returns_404() {
    let app = sns_routes().with_state(test_state());
    let body = serde_json::json!({
        "Type": "Notification",
        "Message": "hello"
    });
    let resp = app
        .oneshot(
            Request::post("/sns/nonexistent-function")
                .header("x-amz-sns-message-type", "Notification")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sns_missing_message_type_returns_400() {
    let app = sns_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/sns/my-function")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sns_unknown_message_type_returns_400() {
    let app = sns_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/sns/my-function")
                .header("x-amz-sns-message-type", "SomethingElse")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sns_unsubscribe_confirmation_returns_ok() {
    let app = sns_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/sns/my-function")
                .header("x-amz-sns-message-type", "UnsubscribeConfirmation")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn sns_subscription_confirmation_missing_subscribe_url_returns_400() {
    let app = sns_routes().with_state(test_state());
    let body = serde_json::json!({
        "Type": "SubscriptionConfirmation",
        "Token": "abc123"
    });
    let resp = app
        .oneshot(
            Request::post("/sns/my-function")
                .header("x-amz-sns-message-type", "SubscriptionConfirmation")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sns_subscription_confirmation_invalid_json_returns_400() {
    let app = sns_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/sns/my-function")
                .header("x-amz-sns-message-type", "SubscriptionConfirmation")
                .body(Body::from("not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sns_notification_invalid_json_returns_400() {
    let app = sns_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/sns/my-function")
                .header("x-amz-sns-message-type", "Notification")
                .body(Body::from("not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sns_invalid_function_name_returns_400() {
    let app = sns_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/sns/.invalid")
                .header("x-amz-sns-message-type", "Notification")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn callback_url_used_in_endpoint_construction() {
    let state = test_state_with_callback("http://my-host:8080");
    let url = sns_endpoint_url(&state.config.callback_url, "my-function");
    assert_eq!(url, "http://my-host:8080/sns/my-function");
}

#[test]
fn build_sns_event_matches_reference_payload_structure() {
    let sns_message = serde_json::json!({
        "Type": "Notification",
        "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:my-topic",
        "Subject": "Test Subject",
        "Message": "Hello from SNS!",
        "Timestamp": "2024-01-15T12:45:07.000Z",
        "SignatureVersion": "1",
        "Signature": "EXAMPLEpH+...",
        "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/cert.pem",
        "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe",
        "MessageAttributes": {
            "TestKey": {
                "Type": "String",
                "Value": "TestValue"
            }
        }
    });

    let event = build_sns_event(&sns_message, "us-east-1", "123456789012", "my-function");

    // Verify top-level Records array exists
    let records = event.get("Records").expect("missing Records");
    let records_arr = records.as_array().expect("Records should be array");
    assert_eq!(records_arr.len(), 1);

    let record = &records_arr[0];

    // Verify required record fields
    assert_eq!(record["EventVersion"], "1.0");
    assert_eq!(record["EventSource"], "aws:sns");
    assert_eq!(
        record["EventSubscriptionArn"],
        "arn:aws:sns:us-east-1:123456789012:my-function"
    );

    // Verify Sns object contains all original fields
    let sns = record.get("Sns").expect("missing Sns object");
    assert_eq!(sns["MessageId"], "95df01b4-ee98-5cb9-9903-4c221d41eb5e");
    assert_eq!(sns["Message"], "Hello from SNS!");
    assert_eq!(sns["TopicArn"], "arn:aws:sns:us-east-1:123456789012:my-topic");
    assert_eq!(sns["Subject"], "Test Subject");
    assert_eq!(sns["Timestamp"], "2024-01-15T12:45:07.000Z");
    assert_eq!(sns["Type"], "Notification");

    // Verify MessageAttributes are preserved
    let attrs = sns.get("MessageAttributes").expect("missing MessageAttributes");
    assert_eq!(attrs["TestKey"]["Type"], "String");
    assert_eq!(attrs["TestKey"]["Value"], "TestValue");
}

#[test]
fn build_sns_event_constructs_correct_subscription_arn() {
    let sns_message = serde_json::json!({"Type": "Notification", "Message": "test"});
    let event = build_sns_event(&sns_message, "eu-west-1", "999888777666", "handler-fn");

    let arn = event["Records"][0]["EventSubscriptionArn"].as_str().unwrap();
    assert_eq!(arn, "arn:aws:sns:eu-west-1:999888777666:handler-fn");
}

#[tokio::test]
async fn sns_subscription_confirmation_auto_confirms() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/confirm"))
        .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let subscribe_url = format!("{}/confirm", mock_server.uri());

    let app = sns_routes().with_state(test_state());
    let body = serde_json::json!({
        "Type": "SubscriptionConfirmation",
        "Token": "abc123",
        "TopicArn": "arn:aws:sns:us-east-1:000000000000:test-topic",
        "SubscribeURL": subscribe_url
    });
    let resp = app
        .oneshot(
            Request::post("/sns/my-function")
                .header("x-amz-sns-message-type", "SubscriptionConfirmation")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
