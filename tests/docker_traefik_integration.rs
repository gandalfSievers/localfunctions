//! Traefik TLS reverse proxy integration tests.
//!
//! These tests validate end-to-end HTTPS communication through a Traefik
//! reverse proxy using mkcert-generated certificates for *.amazonaws.com
//! domains. All services are accessed exclusively via their real AWS-style
//! hostnames — no short domains or explicit ports.
//!
//! Services under test:
//!   - Lambda:  https://{function}.lambda.us-east-1.amazonaws.com
//!   - SQS:     https://sqs.us-east-1.amazonaws.com
//!   - SNS:     https://sns.us-east-1.amazonaws.com
//!
//! localfunctions itself polls SQS through Traefik HTTPS using
//! webpki-roots-patcher (LD_PRELOAD) to trust the mkcert CA at runtime.
//!
//! Run via: `make test-integration-traefik`
//! Or manually:
//!   docker compose -f docker-compose.test.traefik.yml up -d --build
//!   docker compose -f docker-compose.test.traefik.yml run --rm test-runner
//!   docker compose -f docker-compose.test.traefik.yml down

use std::time::Duration;

const SQS_ENDPOINT: &str = "https://sqs.us-east-1.amazonaws.com";
const SNS_ENDPOINT: &str = "https://sns.us-east-1.amazonaws.com";

/// Build a reqwest client that trusts the mkcert CA certificate.
fn client() -> reqwest::Client {
    let ca_path =
        std::env::var("TRAEFIK_CA_CERT").unwrap_or_else(|_| "/certs/rootCA.pem".into());
    let ca_cert = std::fs::read(&ca_path).expect("failed to read CA cert");
    let cert = reqwest::Certificate::from_pem(&ca_cert).expect("failed to parse CA cert");
    reqwest::Client::builder()
        .add_root_certificate(cert)
        .timeout(Duration::from_secs(90))
        .build()
        .unwrap()
}

/// Lambda invoke URL via AWS-style virtual host.
fn lambda_invoke_url(function: &str) -> String {
    format!(
        "https://{}.lambda.us-east-1.amazonaws.com/2015-03-31/functions/{}/invocations",
        function, function
    )
}

/// Extract a value between XML tags. Minimal parser sufficient for test assertions.
fn extract_xml_value(xml: &str, tag: &str) -> String {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml
        .find(&open)
        .unwrap_or_else(|| panic!("tag <{}> not found in: {}", tag, xml))
        + open.len();
    let end = xml[start..]
        .find(&close)
        .unwrap_or_else(|| panic!("closing tag </{}> not found in: {}", tag, xml))
        + start;
    xml[start..end].to_string()
}

// ---------------------------------------------------------------------------
// Lambda invoke — through Traefik HTTPS
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn traefik_invoke_python_hello() {
    let resp = client()
        .post(lambda_invoke_url("python-hello"))
        .body(r#"{"name":"Traefik"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);

    let inner: serde_json::Value = serde_json::from_str(body["body"].as_str().unwrap()).unwrap();
    assert_eq!(inner["message"], "Hello from Python!");
    assert_eq!(inner["input"]["name"], "Traefik");
}

#[tokio::test]
#[ignore]
async fn traefik_invoke_nodejs_hello() {
    let resp = client()
        .post(lambda_invoke_url("nodejs-hello"))
        .body(r#"{"greeting":"traefik"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);

    let inner: serde_json::Value = serde_json::from_str(body["body"].as_str().unwrap()).unwrap();
    assert_eq!(inner["message"], "Hello from Node.js!");
    assert_eq!(inner["input"]["greeting"], "traefik");
}

#[tokio::test]
#[ignore]
async fn traefik_invoke_error_function() {
    let resp = client()
        .post(lambda_invoke_url("python-error"))
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

// ---------------------------------------------------------------------------
// Function URL style — through Traefik HTTPS
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn traefik_function_url_invocation() {
    let resp = client()
        .post("https://python-hello.lambda.us-east-1.amazonaws.com/")
        .body(r#"{"key":"function-url"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());

    let body: serde_json::Value = resp.json().await.unwrap();
    let event_body = body["input"]["body"]
        .as_str()
        .expect("Function URL event should contain original body");
    let parsed: serde_json::Value = serde_json::from_str(event_body).unwrap();
    assert_eq!(parsed["key"], "function-url");
}

// ---------------------------------------------------------------------------
// SQS — send and receive through Traefik HTTPS
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn traefik_sqs_send_and_receive_message() {
    let c = client();
    let queue_name = format!("traefik-sqs-{}", uuid::Uuid::new_v4().simple());

    // Create queue.
    let resp = c
        .post(format!("{}/", SQS_ENDPOINT))
        .form(&[("Action", "CreateQueue"), ("QueueName", &queue_name)])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "CreateQueue should succeed");
    let body = resp.text().await.unwrap();
    let queue_url = extract_xml_value(&body, "QueueUrl");
    assert!(
        queue_url.contains("amazonaws.com"),
        "queue URL should use amazonaws.com domain: {}",
        queue_url
    );

    // Send message.
    let msg_body = "hello from traefik TLS";
    let resp = c
        .post(&queue_url)
        .form(&[("Action", "SendMessage"), ("MessageBody", msg_body)])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "SendMessage should succeed");
    let body = resp.text().await.unwrap();
    let message_id = extract_xml_value(&body, "MessageId");
    assert!(!message_id.is_empty(), "should return a MessageId");

    // Receive message.
    let resp = c
        .post(&queue_url)
        .form(&[
            ("Action", "ReceiveMessage"),
            ("MaxNumberOfMessages", "1"),
            ("WaitTimeSeconds", "5"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "ReceiveMessage should succeed");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains(msg_body),
        "received message should contain sent body"
    );

    let receipt_handle = extract_xml_value(&body, "ReceiptHandle");

    // Delete message.
    let resp = c
        .post(&queue_url)
        .form(&[
            ("Action", "DeleteMessage"),
            ("ReceiptHandle", &receipt_handle),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "DeleteMessage should succeed");

    // Delete queue.
    let _ = c
        .post(&queue_url)
        .form(&[("Action", "DeleteQueue")])
        .send()
        .await;
}

// ---------------------------------------------------------------------------
// SNS — create topic and publish through Traefik HTTPS
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn traefik_sns_create_topic_and_publish() {
    let c = client();
    let topic_name = format!("traefik-sns-{}", uuid::Uuid::new_v4().simple());

    // Create topic.
    let resp = c
        .post(format!("{}/", SNS_ENDPOINT))
        .form(&[("Action", "CreateTopic"), ("Name", &topic_name)])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "CreateTopic should succeed");
    let body = resp.text().await.unwrap();
    let topic_arn = extract_xml_value(&body, "TopicArn");
    assert!(
        topic_arn.contains(&topic_name),
        "topic ARN should contain topic name: {}",
        topic_arn
    );

    // Publish message.
    let resp = c
        .post(format!("{}/", SNS_ENDPOINT))
        .form(&[
            ("Action", "Publish"),
            ("TopicArn", &topic_arn),
            ("Message", "hello from traefik TLS"),
            ("Subject", "Traefik Test"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "Publish should succeed");
    let body = resp.text().await.unwrap();
    let message_id = extract_xml_value(&body, "MessageId");
    assert!(!message_id.is_empty(), "should return a MessageId");

    // Delete topic.
    let _ = c
        .post(format!("{}/", SNS_ENDPOINT))
        .form(&[("Action", "DeleteTopic"), ("TopicArn", &topic_arn)])
        .send()
        .await;
}

// ---------------------------------------------------------------------------
// End-to-end: SQS message → Lambda consumer — all through Traefik HTTPS
// ---------------------------------------------------------------------------

/// Validates the full flow: send a message to SQS, retrieve it, invoke a
/// Lambda function with the message payload, and verify the function
/// processed the message correctly.
#[tokio::test]
#[ignore]
async fn traefik_e2e_sqs_to_lambda_consumer() {
    let c = client();
    let queue_name = format!("traefik-e2e-{}", uuid::Uuid::new_v4().simple());

    // 1. Create queue via Traefik HTTPS.
    let resp = c
        .post(format!("{}/", SQS_ENDPOINT))
        .form(&[("Action", "CreateQueue"), ("QueueName", &queue_name)])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let queue_url = extract_xml_value(&resp.text().await.unwrap(), "QueueUrl");

    // 2. Send message to SQS via Traefik HTTPS.
    let order = r#"{"order_id":"traefik-001","item":"widget"}"#;
    let resp = c
        .post(&queue_url)
        .form(&[("Action", "SendMessage"), ("MessageBody", order)])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "send message should succeed");

    // 3. Receive message from SQS via Traefik HTTPS.
    let resp = c
        .post(&queue_url)
        .form(&[
            ("Action", "ReceiveMessage"),
            ("MaxNumberOfMessages", "1"),
            ("WaitTimeSeconds", "5"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let recv_body = resp.text().await.unwrap();
    assert!(recv_body.contains("traefik-001"), "message should be received from SQS");
    let receipt_handle = extract_xml_value(&recv_body, "ReceiptHandle");

    // 4. Invoke Lambda with the original message payload via Traefik HTTPS.
    //    This simulates a consumer reading from SQS and invoking a Lambda.
    let resp = c
        .post(lambda_invoke_url("python-hello"))
        .body(order)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("X-Amz-Function-Error").is_none());

    let lambda_result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(lambda_result["statusCode"], 200);

    let inner: serde_json::Value =
        serde_json::from_str(lambda_result["body"].as_str().unwrap()).unwrap();
    assert_eq!(
        inner["input"]["order_id"], "traefik-001",
        "Lambda should have processed the SQS message"
    );

    // 5. Delete message from SQS via Traefik HTTPS (simulating successful consumer ack).
    let resp = c
        .post(&queue_url)
        .form(&[
            ("Action", "DeleteMessage"),
            ("ReceiptHandle", &receipt_handle),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "delete should succeed");

    // 6. Verify queue is empty.
    let resp = c
        .post(&queue_url)
        .form(&[
            ("Action", "ReceiveMessage"),
            ("MaxNumberOfMessages", "1"),
            ("WaitTimeSeconds", "1"),
        ])
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("<Message>") || !body.contains("<Body>"),
        "queue should be empty after consumer ack"
    );

    // Cleanup.
    let _ = c
        .post(&queue_url)
        .form(&[("Action", "DeleteQueue")])
        .send()
        .await;
}

// ---------------------------------------------------------------------------
// End-to-end: Lambda → SNS publish — all through Traefik HTTPS
// ---------------------------------------------------------------------------

/// Validates: invoke Lambda via Traefik, then publish the Lambda response
/// to SNS via Traefik.
#[tokio::test]
#[ignore]
async fn traefik_e2e_lambda_to_sns_publish() {
    let c = client();
    let topic_name = format!("traefik-e2e-sns-{}", uuid::Uuid::new_v4().simple());

    // 1. Create SNS topic via Traefik HTTPS.
    let resp = c
        .post(format!("{}/", SNS_ENDPOINT))
        .form(&[("Action", "CreateTopic"), ("Name", &topic_name)])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let topic_arn = extract_xml_value(&resp.text().await.unwrap(), "TopicArn");

    // 2. Invoke Lambda via Traefik HTTPS.
    let resp = c
        .post(lambda_invoke_url("python-hello"))
        .body(r#"{"event":"order.created","id":"sns-001"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let lambda_result = resp.text().await.unwrap();

    // 3. Publish Lambda result to SNS via Traefik HTTPS.
    let resp = c
        .post(format!("{}/", SNS_ENDPOINT))
        .form(&[
            ("Action", "Publish"),
            ("TopicArn", &topic_arn),
            ("Message", &lambda_result),
            ("Subject", "Lambda Result"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let message_id = extract_xml_value(&body, "MessageId");
    assert!(!message_id.is_empty(), "SNS publish should return MessageId");

    // Cleanup.
    let _ = c
        .post(format!("{}/", SNS_ENDPOINT))
        .form(&[("Action", "DeleteTopic"), ("TopicArn", &topic_arn)])
        .send()
        .await;
}

// ---------------------------------------------------------------------------
// Concurrent invocations — through Traefik HTTPS
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn traefik_zz_concurrent_invocations() {
    let functions = vec!["python-hello", "nodejs-hello", "python-hello"];

    let mut handles = Vec::new();
    for func in functions {
        let c = client();
        let url = lambda_invoke_url(func);
        handles.push(tokio::spawn(async move {
            let resp = c
                .post(&url)
                .body(r#"{"concurrent":true}"#)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            assert!(resp.headers().get("X-Amz-Function-Error").is_none());
        }));
    }

    for handle in handles {
        tokio::time::timeout(Duration::from_secs(120), handle)
            .await
            .expect("invocation should complete in time")
            .expect("invocation should not panic");
    }
}
