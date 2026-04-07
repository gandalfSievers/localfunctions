//! AWS-style virtual host integration tests against a real running
//! localfunctions server with real Docker Lambda containers.
//!
//! These tests invoke functions using the Host header pattern:
//!   `{function}.lambda.{region}.amazonaws.com`
//!
//! They connect to the same running server as the path-style tests but
//! exercise the virtual host middleware by setting explicit Host headers.
//!
//! For full DNS-based testing (where the AWS SDK naturally routes via
//! subdomains), use docker-compose.test.awsstyle.yml with dnsmasq.
//!
//! Run via: `make test-integration-awsstyle`
//! Or manually:
//!   LOCAL_LAMBDA_FUNCTIONS_FILE=tests/fixtures/functions.json cargo run --release &
//!   cargo test --test docker_awsstyle_integration -- --ignored
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

/// Build a Host header for AWS-style virtual host routing.
fn aws_host(function_name: &str, region: &str) -> String {
    format!("{}.lambda.{}.amazonaws.com", function_name, region)
}

// ---------------------------------------------------------------------------
// Invoke API — AWS-style via Host header + Lambda API path
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn awsstyle_invoke_python_hello() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/python-hello/invocations",
            endpoint_url()
        ))
        .header("Host", aws_host("python-hello", "us-east-1"))
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
async fn awsstyle_invoke_nodejs_hello() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/nodejs-hello/invocations",
            endpoint_url()
        ))
        .header("Host", aws_host("nodejs-hello", "us-east-1"))
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
async fn awsstyle_invoke_error_function() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/python-error/invocations",
            endpoint_url()
        ))
        .header("Host", aws_host("python-error", "us-east-1"))
        .body(r#"{}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("X-Amz-Function-Error").is_some(),
        "expected X-Amz-Function-Error header"
    );
}

#[tokio::test]
#[ignore]
async fn awsstyle_nonexistent_function_returns_404() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/does-not-exist/invocations",
            endpoint_url()
        ))
        .header("Host", aws_host("does-not-exist", "us-east-1"))
        .body(r#"{}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
#[ignore]
async fn awsstyle_different_region_routes_correctly() {
    let resp = client()
        .post(format!(
            "{}/2015-03-31/functions/python-hello/invocations",
            endpoint_url()
        ))
        .header("Host", aws_host("python-hello", "eu-west-1"))
        .body(r#"{"region":"eu-west-1"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);
}

// ---------------------------------------------------------------------------
// Function URL style — Host header identifies the function, path is the route
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn awsstyle_root_path_invocation() {
    let resp = client()
        .post(format!("{}/", endpoint_url()))
        .header("Host", aws_host("python-hello", "us-east-1"))
        .body(r#"{"key":"host-only"}"#)
        .send()
        .await
        .unwrap();

    // Function URL handler translates the Lambda response into an HTTP
    // response: status from statusCode, body from body field.
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());

    let body: serde_json::Value = resp.json().await.unwrap();
    // The Function URL event wraps the original body as a string in the
    // "body" field. The handler echoes the whole event as "input".
    let event_body = body["input"]["body"]
        .as_str()
        .expect("Function URL event should contain original body as string");
    let parsed: serde_json::Value = serde_json::from_str(event_body).unwrap();
    assert_eq!(parsed["key"], "host-only");
}

#[tokio::test]
#[ignore]
async fn awsstyle_subpath_invocation() {
    let resp = client()
        .post(format!("{}/event", endpoint_url()))
        .header("Host", aws_host("nodejs-hello", "us-east-1"))
        .body(r#"{"path":"subpath"}"#)
        .send()
        .await
        .unwrap();

    // Function URL handler translates the Lambda response.
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());
}

// ---------------------------------------------------------------------------
// Concurrent AWS-style invocations
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn awsstyle_zz_concurrent_invocations() {
    let functions = vec![
        ("python-hello", "us-east-1"),
        ("nodejs-hello", "us-east-1"),
        ("python-hello", "eu-west-1"),
    ];

    let mut handles = Vec::new();
    for (func, region) in functions {
        let url = format!(
            "{}/2015-03-31/functions/{}/invocations",
            endpoint_url(),
            func
        );
        let host = aws_host(func, region);
        let c = client();
        handles.push(tokio::spawn(async move {
            let resp = c
                .post(&url)
                .header("Host", host)
                .body(r#"{"concurrent":true}"#)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            assert!(resp.headers().get("X-Amz-Function-Error").is_none());
        }));
    }

    for handle in handles {
        tokio::time::timeout(Duration::from_secs(90), handle)
            .await
            .expect("invocation should complete in time")
            .expect("invocation should not panic");
    }
}
