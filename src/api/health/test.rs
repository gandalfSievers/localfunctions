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
        domain: None,
        callback_url: "http://0.0.0.0:9600".to_string(),
    };
    let docker = bollard::Docker::connect_with_local_defaults().unwrap();
    let functions = FunctionsConfig {
        functions: HashMap::new(),
        runtime_images: HashMap::new(),
        event_source_mappings: Vec::new(),
        sns_subscriptions: Vec::new(),
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
async fn health_returns_ok_with_json() {
    let app = health_routes().with_state(test_state());
    let resp = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("status").is_some());
    assert!(json.get("docker").is_some());
    assert!(json["docker"].get("connected").is_some());
}

#[tokio::test]
async fn metrics_endpoint_returns_empty_on_fresh_start() {
    let app = health_routes().with_state(test_state());
    let resp = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let functions = json.get("functions").unwrap().as_object().unwrap();
    assert!(functions.is_empty());
}

#[tokio::test]
async fn metrics_endpoint_reflects_recorded_invocations() {
    let state = test_state();
    // Simulate recording some invocations directly.
    state.metrics.record_invocation(
        "test-func",
        std::time::Duration::from_millis(50),
        false,
        true,
    );
    state.metrics.record_invocation(
        "test-func",
        std::time::Duration::from_millis(150),
        true,
        false,
    );

    let app = health_routes().with_state(state);
    let resp = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let func_metrics = &json["functions"]["test-func"];
    assert_eq!(func_metrics["invocation_count"], 2);
    assert_eq!(func_metrics["error_count"], 1);
    assert_eq!(func_metrics["cold_start_count"], 1);
    assert!(func_metrics["avg_duration_ms"].as_f64().unwrap() > 0.0);
    assert!(func_metrics["p50_duration_ms"].as_f64().unwrap() > 0.0);
    assert!(func_metrics["p95_duration_ms"].as_f64().unwrap() > 0.0);
    assert!(func_metrics["p99_duration_ms"].as_f64().unwrap() > 0.0);
}
