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
async fn extension_register_returns_200_with_identifier() {
    let app = extension_routes().with_state(test_state());
    let body = serde_json::json!({"events": ["INVOKE", "SHUTDOWN"]});
    let resp = app
        .oneshot(
            Request::post("/2020-01-01/extension/register")
                .header("Lambda-Extension-Name", "my-ext")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("Lambda-Extension-Identifier")
        .is_some());

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["functionVersion"], "$LATEST");
}

#[tokio::test]
async fn extension_register_missing_name_returns_400() {
    let app = extension_routes().with_state(test_state());
    let body = serde_json::json!({"events": ["INVOKE"]});
    let resp = app
        .oneshot(
            Request::post("/2020-01-01/extension/register")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn extension_register_missing_function_name_returns_400() {
    let app = extension_routes().with_state(test_state());
    let body = serde_json::json!({"events": ["INVOKE"]});
    let resp = app
        .oneshot(
            Request::post("/2020-01-01/extension/register")
                .header("Lambda-Extension-Name", "my-ext")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn extension_register_empty_events_returns_400() {
    let app = extension_routes().with_state(test_state());
    let body = serde_json::json!({"events": []});
    let resp = app
        .oneshot(
            Request::post("/2020-01-01/extension/register")
                .header("Lambda-Extension-Name", "my-ext")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn extension_register_invalid_body_returns_400() {
    let app = extension_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/2020-01-01/extension/register")
                .header("Lambda-Extension-Name", "my-ext")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .header("content-type", "application/json")
                .body(Body::from("not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn extension_event_next_missing_identifier_returns_400() {
    let app = extension_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::get("/2020-01-01/extension/event/next")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn extension_event_next_invalid_uuid_returns_400() {
    let app = extension_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::get("/2020-01-01/extension/event/next")
                .header("Lambda-Extension-Identifier", "not-a-uuid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn extension_event_next_unregistered_returns_400() {
    let app = extension_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::get("/2020-01-01/extension/event/next")
                .header(
                    "Lambda-Extension-Identifier",
                    "550e8400-e29b-41d4-a716-446655440000",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn extension_event_next_returns_shutdown_on_shutdown_signal() {
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ext_registry = Arc::new(crate::extensions::ExtensionRegistry::new(
        shutdown_rx.clone(),
    ));
    let runtime_bridge = Arc::new(RuntimeBridge::new(
        HashMap::new(),
        HashMap::new(),
        shutdown_rx,
    ));
    let docker = bollard::Docker::connect_with_local_defaults().unwrap();
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
    };
    let functions = FunctionsConfig {
        functions: HashMap::new(),
        runtime_images: HashMap::new(),
    };

    // Register an extension
    let ext_id = ext_registry
        .register("test-ext", "my-func", vec![
            crate::extensions::ExtensionEventType::Invoke,
            crate::extensions::ExtensionEventType::Shutdown,
        ])
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
        extension_registry: ext_registry.clone(),
    };

    // Send shutdown signal so the long-poll resolves immediately.
    _shutdown_tx.send(true).unwrap();

    let app = extension_routes().with_state(state);
    let resp = app
        .oneshot(
            Request::get("/2020-01-01/extension/event/next")
                .header("Lambda-Extension-Identifier", ext_id.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["eventType"], "SHUTDOWN");
    assert_eq!(json["shutdownReason"], "spindown");
    assert!(json["deadlineMs"].is_u64());
}

#[tokio::test]
async fn extension_lifecycle_invoke_event_delivery() {
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ext_registry = Arc::new(crate::extensions::ExtensionRegistry::new(
        shutdown_rx.clone(),
    ));

    // Register extension for INVOKE events.
    let ext_id = ext_registry
        .register("test-ext", "my-func", vec![
            crate::extensions::ExtensionEventType::Invoke,
        ])
        .await;

    // Deliver an INVOKE event.
    ext_registry
        .notify_invoke(
            "my-func",
            "req-abc-123",
            "arn:aws:lambda:us-east-1:000:function:my-func",
            9999999999,
            "Root=1-test",
        )
        .await;

    // Poll for it via the registry directly.
    let event = ext_registry.next_event(ext_id).await;
    assert!(event.is_some());
    let event = event.unwrap();
    match event {
        crate::extensions::ExtensionEvent::Invoke {
            request_id,
            deadline_ms,
            invoked_function_arn,
            tracing,
        } => {
            assert_eq!(request_id, "req-abc-123");
            assert_eq!(deadline_ms, 9999999999);
            assert!(invoked_function_arn.contains("my-func"));
            assert_eq!(tracing.value, "Root=1-test");
        }
        _ => panic!("expected INVOKE event"),
    }
}
