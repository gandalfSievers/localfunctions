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

fn test_state_with_functions() -> AppState {
    let config = Config {
        host: "127.0.0.1".parse().unwrap(),
        port: 9600,
        runtime_port: 9601,
        region: "us-east-1".into(),
        account_id: "123456789012".into(),
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

    let mut functions_map = HashMap::new();
    functions_map.insert(
        "alpha-func".to_string(),
        crate::types::FunctionConfig {
            name: "alpha-func".into(),
            runtime: "python3.12".into(),
            handler: "main.handler".into(),
            code_path: std::path::PathBuf::from("/tmp/code"),
            timeout: 60,
            memory_size: 256,
            ephemeral_storage_mb: 1024,
            environment: HashMap::from([("ENV_KEY".into(), "env_val".into())]),
            image: None,
            image_uri: None,
            reserved_concurrent_executions: None,
            architecture: "x86_64".into(),
            layers: vec![],
            function_url_enabled: false,
            max_retry_attempts: 2,
            on_success: None,
            on_failure: None,
        },
    );
    functions_map.insert(
        "beta-func".to_string(),
        crate::types::FunctionConfig {
            name: "beta-func".into(),
            runtime: "nodejs20.x".into(),
            handler: "index.handler".into(),
            code_path: std::path::PathBuf::from("/tmp/code2"),
            timeout: 30,
            memory_size: 128,
            ephemeral_storage_mb: 512,
            environment: HashMap::new(),
            image: None,
            image_uri: Some("my-image:latest".into()),
            reserved_concurrent_executions: None,
            architecture: "x86_64".into(),
            layers: vec![],
            function_url_enabled: false,
            max_retry_attempts: 2,
            on_success: None,
            on_failure: None,
        },
    );
    let functions = FunctionsConfig {
        functions: functions_map,
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
async fn list_functions_returns_all_functions() {
    let app = functions_routes().with_state(test_state_with_functions());
    let resp = app
        .oneshot(Request::get("/2015-03-31/functions").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let functions = json["Functions"].as_array().unwrap();
    assert_eq!(functions.len(), 2);

    // Sorted alphabetically
    assert_eq!(functions[0]["FunctionName"], "alpha-func");
    assert_eq!(functions[1]["FunctionName"], "beta-func");

    // Verify ARN format
    assert_eq!(
        functions[0]["FunctionArn"],
        "arn:aws:lambda:us-east-1:123456789012:function:alpha-func"
    );

    // Verify fields present
    assert_eq!(functions[0]["Runtime"], "python3.12");
    assert_eq!(functions[0]["Handler"], "main.handler");
    assert_eq!(functions[0]["Timeout"], 60);
    assert_eq!(functions[0]["MemorySize"], 256);

    // Environment included when non-empty
    assert_eq!(functions[0]["Environment"]["Variables"]["ENV_KEY"], "env_val");

    // Environment absent when empty
    assert!(functions[1]["Environment"].is_null());

    // PackageType for image-based
    assert_eq!(functions[1]["PackageType"], "Image");
    assert_eq!(functions[0]["PackageType"], "Zip");
}

#[tokio::test]
async fn list_functions_empty() {
    let app = functions_routes().with_state(test_state());
    let resp = app
        .oneshot(Request::get("/2015-03-31/functions").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let functions = json["Functions"].as_array().unwrap();
    assert_eq!(functions.len(), 0);
}

#[tokio::test]
async fn get_function_returns_configuration() {
    let app = functions_routes().with_state(test_state_with_functions());
    let resp = app
        .oneshot(
            Request::get("/2015-03-31/functions/alpha-func")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let config = &json["Configuration"];
    assert_eq!(config["FunctionName"], "alpha-func");
    assert_eq!(
        config["FunctionArn"],
        "arn:aws:lambda:us-east-1:123456789012:function:alpha-func"
    );
    assert_eq!(config["Runtime"], "python3.12");
    assert_eq!(config["Handler"], "main.handler");
    assert_eq!(config["Timeout"], 60);
    assert_eq!(config["MemorySize"], 256);
    assert_eq!(config["Environment"]["Variables"]["ENV_KEY"], "env_val");
    assert_eq!(config["EphemeralStorage"]["Size"], 1024);
    assert_eq!(config["Version"], "$LATEST");
    assert_eq!(config["PackageType"], "Zip");

    // Code section present
    assert!(json["Code"].is_object());
}

#[tokio::test]
async fn get_function_not_found_returns_404() {
    let app = functions_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::get("/2015-03-31/functions/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn get_function_invalid_name_returns_400() {
    let app = functions_routes().with_state(test_state());
    let resp = app
        .oneshot(
            Request::get("/2015-03-31/functions/inv@lid!")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Type"], "InvalidRequestContentException");
}

// -- Qualifier integration tests (GetFunction API) ------------------------

#[tokio::test]
async fn get_function_with_latest_qualifier() {
    let app = functions_routes().with_state(test_state_with_functions());
    let resp = app
        .oneshot(
            Request::get("/2015-03-31/functions/alpha-func?Qualifier=$LATEST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Configuration"]["FunctionName"], "alpha-func");
}

#[tokio::test]
async fn get_function_with_unknown_qualifier_returns_404() {
    let app = functions_routes().with_state(test_state_with_functions());
    let resp = app
        .oneshot(
            Request::get("/2015-03-31/functions/alpha-func?Qualifier=PROD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn get_function_with_colon_qualifier_latest() {
    let app = functions_routes().with_state(test_state_with_functions());
    let resp = app
        .oneshot(
            Request::get("/2015-03-31/functions/alpha-func:$LATEST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Configuration"]["FunctionName"], "alpha-func");
}

#[tokio::test]
async fn get_function_with_colon_qualifier_unknown() {
    let app = functions_routes().with_state(test_state_with_functions());
    let resp = app
        .oneshot(
            Request::get("/2015-03-31/functions/alpha-func:STAGING")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["Type"], "ResourceNotFoundException");
}
