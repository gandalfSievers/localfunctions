//! Path-style integration tests against a real running localfunctions server
//! with real Docker Lambda containers.
//!
//! These tests require:
//!   - A running localfunctions server (with tests/fixtures/functions.json)
//!   - Pre-pulled Lambda runtime images:
//!       docker pull public.ecr.aws/lambda/python:3.12
//!       docker pull public.ecr.aws/lambda/nodejs:20
//!
//! Run via: `make test-integration-pathstyle`
//! Or manually:
//!   LOCAL_LAMBDA_FUNCTIONS_FILE=tests/fixtures/functions.json cargo run --release &
//!   cargo test --test docker_pathstyle_integration -- --ignored
//!
//! Set TEST_ENDPOINT_URL to override the default (http://localhost:9600).

use std::time::Duration;

fn endpoint_url() -> String {
    std::env::var("TEST_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:9600".into())
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Invoke API — path-style
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn pathstyle_invoke_python_hello() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/python-hello/invocations",
            endpoint_url()
        ))
        .body(r#"{"name":"Lambda"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);

    let inner: serde_json::Value = serde_json::from_str(body["body"].as_str().unwrap()).unwrap();
    assert_eq!(inner["message"], "Hello from Python!");
    assert_eq!(inner["input"]["name"], "Lambda");
}

#[tokio::test]
#[ignore]
async fn pathstyle_invoke_nodejs_hello() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/nodejs-hello/invocations",
            endpoint_url()
        ))
        .body(r#"{"greeting":"world"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);

    let inner: serde_json::Value = serde_json::from_str(body["body"].as_str().unwrap()).unwrap();
    assert_eq!(inner["message"], "Hello from Node.js!");
    assert_eq!(inner["input"]["greeting"], "world");
}

#[tokio::test]
#[ignore]
async fn pathstyle_invoke_python_error() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/python-error/invocations",
            endpoint_url()
        ))
        .body(r#"{}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("X-Amz-Function-Error").is_some(),
        "expected X-Amz-Function-Error header for error response"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    let error_message = body.to_string();
    assert!(
        error_message.contains("Intentional test error"),
        "expected error message in response, got: {}",
        error_message
    );
}

#[tokio::test]
#[ignore]
async fn pathstyle_invoke_nodejs_error() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/nodejs-error/invocations",
            endpoint_url()
        ))
        .body(r#"{}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("X-Amz-Function-Error").is_some(),
        "expected X-Amz-Function-Error header for error response"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    let error_message = body.to_string();
    assert!(
        error_message.contains("Intentional test error"),
        "expected error message in response, got: {}",
        error_message
    );
}

#[tokio::test]
#[ignore]
async fn pathstyle_invoke_nonexistent_function_returns_404() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/does-not-exist/invocations",
            endpoint_url()
        ))
        .body(r#"{}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);

    let body: serde_json::Value = resp.json().await.unwrap();
    let body_str = body.to_string();
    assert!(
        body_str.contains("ResourceNotFound"),
        "expected ResourceNotFoundException, got: {}",
        body_str
    );
}

#[tokio::test]
#[ignore]
async fn pathstyle_invoke_dry_run() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/python-hello/invocations",
            endpoint_url()
        ))
        .header("X-Amz-Invocation-Type", "DryRun")
        .body(r#"{}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);
}

#[tokio::test]
#[ignore]
async fn pathstyle_invoke_event_returns_202() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/python-hello/invocations",
            endpoint_url()
        ))
        .header("X-Amz-Invocation-Type", "Event")
        .body(r#"{"async":true}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 202);
}

#[tokio::test]
#[ignore]
async fn pathstyle_zz_concurrent_invocations() {
    let mut handles = Vec::new();
    for i in 0..3 {
        let url = format!(
            "{}/2015-03-31/functions/python-hello/invocations",
            endpoint_url()
        );
        let c = client();
        handles.push(tokio::spawn(async move {
            let resp = c
                .post(&url)
                .body(format!(r#"{{"index":{}}}"#, i))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            assert!(resp.headers().get("X-Amz-Function-Error").is_none());
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["statusCode"], 200);
        }));
    }

    for handle in handles {
        tokio::time::timeout(Duration::from_secs(90), handle)
            .await
            .expect("invocation should complete in time")
            .expect("invocation should not panic");
    }
}

// ---------------------------------------------------------------------------
// Management APIs
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn pathstyle_health_check() {
    let resp = client()
        .get(format!("{}/health", endpoint_url()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
}

#[tokio::test]
#[ignore]
async fn pathstyle_list_functions() {
    let resp = client()
        .get(format!("{}/2015-03-31/functions", endpoint_url()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let functions = body["Functions"].as_array().expect("Functions should be an array");
    assert!(
        functions.len() >= 5,
        "expected at least 5 functions, got {}",
        functions.len()
    );

    let names: Vec<&str> = functions
        .iter()
        .filter_map(|f| f["FunctionName"].as_str())
        .collect();
    assert!(names.contains(&"python-hello"));
    assert!(names.contains(&"nodejs-hello"));
}

#[tokio::test]
#[ignore]
async fn pathstyle_get_function() {
    let resp = client()
        .get(format!(
            "{}/2015-03-31/functions/python-hello",
            endpoint_url()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["Configuration"]["FunctionName"].as_str().unwrap(),
        "python-hello"
    );
    assert_eq!(
        body["Configuration"]["Runtime"].as_str().unwrap(),
        "python3.12"
    );
}

#[tokio::test]
#[ignore]
async fn pathstyle_get_nonexistent_function_returns_404() {
    let resp = client()
        .get(format!(
            "{}/2015-03-31/functions/does-not-exist",
            endpoint_url()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
#[ignore]
async fn pathstyle_metrics() {
    // Invoke once first to ensure metrics are populated
    client()
        .post(format!(
            "{}/2015-03-31/functions/python-hello/invocations",
            endpoint_url()
        ))
        .body(r#"{}"#)
        .send()
        .await
        .unwrap();

    let resp = client()
        .get(format!("{}/metrics", endpoint_url()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let functions = body["functions"]
        .as_object()
        .expect("metrics should have a 'functions' object");
    assert!(
        functions.contains_key("python-hello"),
        "expected python-hello in metrics.functions, got keys: {:?}",
        functions.keys().collect::<Vec<_>>()
    );
}
