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
async fn runtime_health_returns_ok_with_json() {
    let app = runtime_routes().with_state(test_state());
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
}

#[tokio::test]
async fn runtime_next_invocation_rejects_missing_function_header() {
    let app = runtime_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::get("/2018-06-01/runtime/invocation/next")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Type"], "InvalidRequestContentException");
}

#[tokio::test]
async fn runtime_next_invocation_rejects_unknown_function() {
    let app = runtime_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::get("/2018-06-01/runtime/invocation/next")
                .header("Lambda-Runtime-Function-Name", "nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn runtime_next_invocation_returns_queued_invocation() {
    use crate::types::Invocation;
    use tokio::sync::{mpsc, oneshot};

    let (tx, rx) = mpsc::channel(10);
    let mut receivers = HashMap::new();
    receivers.insert("my-func".to_string(), rx);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

    let mut state = test_state();
    state.runtime_bridge = runtime_bridge;

    // Queue an invocation
    let (resp_tx, _resp_rx) = oneshot::channel();
    let request_id = uuid::Uuid::new_v4();
    let inv = Invocation {
        request_id,
        function_name: "my-func".into(),
        payload: bytes::Bytes::from(r#"{"hello":"world"}"#),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        trace_id: None,
        client_context: None,
        response_tx: Some(resp_tx),
        stream_tx: None,
    };
    tx.send(inv).await.unwrap();

    let app = runtime_routes().with_state(state);
    let resp = app
        .oneshot(
            Request::get("/2018-06-01/runtime/invocation/next")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Verify required headers
    assert_eq!(
        resp.headers()
            .get("Lambda-Runtime-Aws-Request-Id")
            .unwrap()
            .to_str()
            .unwrap(),
        request_id.to_string()
    );
    assert!(resp
        .headers()
        .get("Lambda-Runtime-Deadline-Ms")
        .is_some());
    assert!(resp
        .headers()
        .get("Lambda-Runtime-Invoked-Function-Arn")
        .is_some());

    // Trace-Id header should be absent when not set
    assert!(resp
        .headers()
        .get("Lambda-Runtime-Trace-Id")
        .is_none());

    // Verify payload
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), br#"{"hello":"world"}"#);
}

#[tokio::test]
async fn runtime_next_invocation_includes_trace_id_header() {
    use crate::types::Invocation;
    use tokio::sync::{mpsc, oneshot};

    let (tx, rx) = mpsc::channel(10);
    let mut receivers = HashMap::new();
    receivers.insert("my-func".to_string(), rx);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

    let mut state = test_state();
    state.runtime_bridge = runtime_bridge;

    let (resp_tx, _resp_rx) = oneshot::channel();
    let trace_id = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
    let inv = Invocation {
        request_id: uuid::Uuid::new_v4(),
        function_name: "my-func".into(),
        payload: bytes::Bytes::from("{}"),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        trace_id: Some(trace_id.to_string()),
        client_context: None,
        response_tx: Some(resp_tx),
        stream_tx: None,
    };
    tx.send(inv).await.unwrap();

    let app = runtime_routes().with_state(state);
    let resp = app
        .oneshot(
            Request::get("/2018-06-01/runtime/invocation/next")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("Lambda-Runtime-Trace-Id")
            .unwrap()
            .to_str()
            .unwrap(),
        trace_id
    );
}

#[tokio::test]
async fn runtime_next_invocation_returns_error_on_shutdown() {
    use tokio::sync::mpsc;

    let (_tx, rx) = mpsc::channel::<crate::types::Invocation>(10);
    let mut receivers = HashMap::new();
    receivers.insert("my-func".to_string(), rx);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

    let mut state = test_state();
    state.runtime_bridge = runtime_bridge;

    // Trigger shutdown before the request
    shutdown_tx.send(true).unwrap();

    let app = runtime_routes().with_state(state);

    let resp = app
        .oneshot(
            Request::get("/2018-06-01/runtime/invocation/next")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn runtime_invocation_response_invalid_uuid_returns_400() {
    let app = runtime_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/2018-06-01/runtime/invocation/not-a-uuid/response")
                .body(Body::from(r#"{"result": "ok"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Type"], "InvalidRequestContentException");
}

#[tokio::test]
async fn runtime_invocation_response_unknown_request_id_returns_400() {
    let app = runtime_routes().with_state(test_state());
    let unknown_id = uuid::Uuid::new_v4();
    let resp = app
        .oneshot(
            Request::post(format!(
                "/2018-06-01/runtime/invocation/{}/response",
                unknown_id
            ))
            .body(Body::from(r#"{"result": "ok"}"#))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn runtime_invocation_response_forwards_to_caller() {
    use crate::types::Invocation;
    use tokio::sync::{mpsc, oneshot};

    let (tx, rx) = mpsc::channel(10);
    let mut receivers = HashMap::new();
    receivers.insert("my-func".to_string(), rx);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

    let mut state = test_state();
    state.runtime_bridge = runtime_bridge;

    // Queue an invocation
    let (resp_tx, resp_rx) = oneshot::channel();
    let request_id = uuid::Uuid::new_v4();
    let inv = Invocation {
        request_id,
        function_name: "my-func".into(),
        payload: bytes::Bytes::from(r#"{"input":"data"}"#),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        trace_id: None,
        client_context: None,
        response_tx: Some(resp_tx),
        stream_tx: None,
    };
    tx.send(inv).await.unwrap();

    // First, dispatch the invocation via /next (this stores the pending response_tx)
    let app = runtime_routes().with_state(state.clone());
    let next_resp = app
        .oneshot(
            Request::get("/2018-06-01/runtime/invocation/next")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next_resp.status(), StatusCode::OK);

    // Now post the response
    let app = runtime_routes().with_state(state);
    let resp = app
        .oneshot(
            Request::post(format!(
                "/2018-06-01/runtime/invocation/{}/response",
                request_id
            ))
            .body(Body::from(r#"{"result": "success"}"#))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Verify the caller received the result
    let result = resp_rx.await.unwrap();
    assert_eq!(
        result,
        crate::types::InvocationResult::Success {
            body: r#"{"result": "success"}"#.into()
        }
    );
}

#[tokio::test]
async fn runtime_invocation_error_invalid_uuid_returns_400() {
    let app = runtime_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/2018-06-01/runtime/invocation/not-a-uuid/error")
                .body(Body::from(r#"{"errorMessage":"bad","errorType":"RuntimeError"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn runtime_invocation_error_forwards_to_caller() {
    use crate::types::Invocation;
    use tokio::sync::{mpsc, oneshot};

    let (tx, rx) = mpsc::channel(10);
    let mut receivers = HashMap::new();
    receivers.insert("my-func".to_string(), rx);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

    let mut state = test_state();
    state.runtime_bridge = runtime_bridge;

    let (resp_tx, resp_rx) = oneshot::channel();
    let request_id = uuid::Uuid::new_v4();
    let inv = Invocation {
        request_id,
        function_name: "my-func".into(),
        payload: bytes::Bytes::from("{}"),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        trace_id: None,
        client_context: None,
        response_tx: Some(resp_tx),
        stream_tx: None,
    };
    tx.send(inv).await.unwrap();

    // Dispatch via /next
    let app = runtime_routes().with_state(state.clone());
    let next_resp = app
        .oneshot(
            Request::get("/2018-06-01/runtime/invocation/next")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next_resp.status(), StatusCode::OK);

    // Post error with Lambda-Runtime-Function-Error-Type header
    let app = runtime_routes().with_state(state);
    let resp = app
        .oneshot(
            Request::post(format!(
                "/2018-06-01/runtime/invocation/{}/error",
                request_id
            ))
            .header("Lambda-Runtime-Function-Error-Type", "Runtime.HandlerError")
            .body(Body::from(
                r#"{"errorMessage":"something went wrong","errorType":"RuntimeError"}"#,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Verify error was forwarded
    let result = resp_rx.await.unwrap();
    assert_eq!(
        result,
        crate::types::InvocationResult::Error {
            error_type: "RuntimeError".into(),
            error_message: "something went wrong".into(),
        }
    );
}

#[tokio::test]
async fn runtime_invocation_error_uses_header_when_body_has_no_type() {
    use crate::types::Invocation;
    use tokio::sync::{mpsc, oneshot};

    let (tx, rx) = mpsc::channel(10);
    let mut receivers = HashMap::new();
    receivers.insert("my-func".to_string(), rx);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

    let mut state = test_state();
    state.runtime_bridge = runtime_bridge;

    let (resp_tx, resp_rx) = oneshot::channel();
    let request_id = uuid::Uuid::new_v4();
    let inv = Invocation {
        request_id,
        function_name: "my-func".into(),
        payload: bytes::Bytes::from("{}"),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        trace_id: None,
        client_context: None,
        response_tx: Some(resp_tx),
        stream_tx: None,
    };
    tx.send(inv).await.unwrap();

    // Dispatch via /next
    let app = runtime_routes().with_state(state.clone());
    app.oneshot(
        Request::get("/2018-06-01/runtime/invocation/next")
            .header("Lambda-Runtime-Function-Name", "my-func")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Post error with type only in header, not body
    let app = runtime_routes().with_state(state);
    let resp = app
        .oneshot(
            Request::post(format!(
                "/2018-06-01/runtime/invocation/{}/error",
                request_id
            ))
            .header("Lambda-Runtime-Function-Error-Type", "Runtime.ImportError")
            .body(Body::from(r#"{"errorMessage":"module not found"}"#))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let result = resp_rx.await.unwrap();
    assert_eq!(
        result,
        crate::types::InvocationResult::Error {
            error_type: "Runtime.ImportError".into(),
            error_message: "module not found".into(),
        }
    );
}

#[tokio::test]
async fn runtime_init_error_missing_header_returns_400() {
    let app = runtime_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::post("/2018-06-01/runtime/init/error")
                .body(Body::from(
                    r#"{"errorMessage":"handler not found","errorType":"Runtime.InitError"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn runtime_init_error_returns_502_invalid_runtime_exception() {
    use tokio::sync::mpsc;

    let (_tx, rx) = mpsc::channel::<crate::types::Invocation>(10);
    let mut receivers = HashMap::new();
    receivers.insert("my-func".to_string(), rx);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

    let mut state = test_state();
    state.runtime_bridge = runtime_bridge;

    let app = runtime_routes().with_state(state);
    let resp = app
        .oneshot(
            Request::post("/2018-06-01/runtime/init/error")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .body(Body::from(
                    r#"{"errorMessage":"handler not found","errorType":"Runtime.InitError"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Type"], "InvalidRuntimeException");
    assert!(json["Message"].as_str().unwrap().contains("handler not found"));
}

#[tokio::test]
async fn runtime_init_error_fails_pending_invocations() {
    use crate::types::Invocation;
    use tokio::sync::{mpsc, oneshot};

    let (tx, rx) = mpsc::channel(10);
    let mut receivers = HashMap::new();
    receivers.insert("my-func".to_string(), rx);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

    let mut state = test_state();
    state.runtime_bridge = runtime_bridge;

    // Queue an invocation (simulating a caller waiting)
    let (resp_tx, resp_rx) = oneshot::channel();
    let inv = Invocation {
        request_id: uuid::Uuid::new_v4(),
        function_name: "my-func".into(),
        payload: bytes::Bytes::from("{}"),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        trace_id: None,
        client_context: None,
        response_tx: Some(resp_tx),
        stream_tx: None,
    };
    tx.send(inv).await.unwrap();

    // Send init error
    let app = runtime_routes().with_state(state);
    let resp = app
        .oneshot(
            Request::post("/2018-06-01/runtime/init/error")
                .header("Lambda-Runtime-Function-Name", "my-func")
                .body(Body::from(
                    r#"{"errorMessage":"cannot import module","errorType":"Runtime.ImportModuleError"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

    // The queued invocation should have received an error
    let result = resp_rx.await.unwrap();
    assert_eq!(
        result,
        crate::types::InvocationResult::Error {
            error_type: "InvalidRuntimeException".into(),
            error_message: "Runtime failed to initialize".into(),
        }
    );
}
