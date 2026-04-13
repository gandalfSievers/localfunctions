//! SQS container reuse integration tests against a real running localfunctions
//! server with real Docker Lambda containers and ElasticMQ.
//!
//! Validates the fix for the Busy → Idle state transition bug: a single Lambda
//! container processes multiple SQS messages sequentially via warm reuse,
//! without spawning new containers for each message.
//!
//! Infrastructure (managed by docker-compose.test.sqs-reuse.yml):
//!   - localfunctions server (max_containers=5, with SQS event source mapping)
//!   - ElasticMQ (SQS-compatible queue service)
//!   - python-sqs-consumer Lambda function
//!
//! Run via: `make test-integration-sqs-reuse`
//! Or manually:
//!   docker compose -f docker-compose.test.sqs-reuse.yml up -d
//!   SQS_ENDPOINT=http://localhost:9324 cargo test --test docker_sqs_reuse_integration -- --ignored --nocapture --test-threads=1

use std::time::Duration;

use aws_sdk_sqs::Client as SqsClient;

fn sqs_endpoint() -> String {
    std::env::var("SQS_ENDPOINT").unwrap_or_else(|_| "http://localhost:9324".into())
}

fn endpoint_url() -> String {
    std::env::var("TEST_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:9600".into())
}

async fn build_sqs_client() -> SqsClient {
    let region = aws_sdk_sqs::config::Region::new("us-east-1");
    let creds = aws_sdk_sqs::config::Credentials::new("test", "test", None, None, "test");
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .credentials_provider(creds)
        .endpoint_url(sqs_endpoint())
        .load()
        .await;
    SqsClient::new(&config)
}

/// Check if there are any visible messages on the queue by attempting to
/// receive them (non-destructive peek with short wait).
async fn get_visible_message_count(client: &SqsClient, queue_url: &str) -> usize {
    let output = client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(10)
        .wait_time_seconds(1)
        .visibility_timeout(0) // Don't hide messages — just peek
        .send()
        .await
        .unwrap_or_else(|e| panic!("failed to receive messages: {}", e));
    output.messages().len()
}

/// Wait until there are no visible messages on the queue, or timeout.
async fn wait_for_queue_drain(
    client: &SqsClient,
    queue_url: &str,
    timeout: Duration,
) -> usize {
    let start = tokio::time::Instant::now();
    loop {
        let count = get_visible_message_count(client, queue_url).await;
        if count == 0 {
            return 0;
        }
        if start.elapsed() > timeout {
            return count;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Wait for the localfunctions server to be healthy.
async fn wait_for_server(base_url: &str, timeout: Duration) {
    let client = reqwest::Client::new();
    let start = tokio::time::Instant::now();
    loop {
        if let Ok(resp) = client
            .get(format!("{}/health", base_url))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            if resp.status().is_success() {
                return;
            }
        }
        if start.elapsed() > timeout {
            panic!("localfunctions server did not become healthy within {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Send 10 SQS messages with a short delay between each. The SQS poller picks
/// them up and invokes the Lambda function. With max_containers=5, all 10
/// messages must be processed by reusing containers — if containers get stuck
/// in Busy state, the test will fail because the container limit is exhausted
/// and messages remain on the queue.
#[tokio::test]
#[ignore]
async fn sqs_polling_processes_all_messages_with_container_reuse() {
    let base_url = endpoint_url();
    let sqs_client = build_sqs_client().await;
    let queue_url = format!("{}/000000000000/reuse-test-queue", sqs_endpoint());

    // Wait for localfunctions to be ready (and its SQS poller to start)
    wait_for_server(&base_url, Duration::from_secs(30)).await;

    // Drain any leftover messages from a previous test run
    wait_for_queue_drain(&sqs_client, &queue_url, Duration::from_secs(5)).await;

    // Send 10 messages with a short delay between each
    let num_messages = 10;
    for i in 0..num_messages {
        sqs_client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!(r#"{{"index": {}, "data": "message-{}"}}"#, i, i))
            .send()
            .await
            .unwrap_or_else(|e| panic!("failed to send message {}: {}", i, e));

        // Small delay between messages to simulate realistic arrival pattern
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    eprintln!("Sent {} messages to queue, waiting for processing...", num_messages);

    // Wait for all messages to be consumed (deleted from queue).
    // With batch_size=1, the poller processes one message at a time.
    // Each cycle: poll → invoke → process → delete → poll next.
    // With container reuse working, this should complete well within the timeout.
    let final_count = wait_for_queue_drain(
        &sqs_client,
        &queue_url,
        Duration::from_secs(120),
    )
    .await;

    assert_eq!(
        final_count, 0,
        "all {} messages should be consumed from the queue, but {} remain. \
         This likely means containers got stuck in Busy state and the \
         container limit (max_containers=5) was exhausted.",
        num_messages, final_count
    );

    eprintln!("All {} messages processed successfully!", num_messages);
}

/// Verify the Lambda function can also be invoked directly via the invoke API
/// while the SQS poller is running, confirming that containers are properly
/// released and available for reuse.
#[tokio::test]
#[ignore]
async fn direct_invoke_works_alongside_sqs_polling() {
    let base_url = endpoint_url();
    wait_for_server(&base_url, Duration::from_secs(30)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();

    // Invoke python-hello directly (not the SQS consumer)
    let resp = client
        .post(format!(
            "{}/2015-03-31/functions/python-hello/invocations",
            base_url
        ))
        .body(r#"{"test": "direct-invoke"}"#)
        .send()
        .await
        .expect("direct invoke should succeed");

    assert_eq!(resp.status(), 200, "direct invoke should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["statusCode"], 200);
}

/// Send a burst of messages and then verify they all get processed, testing
/// that the container pool doesn't get exhausted under load. With
/// max_containers=5 and 10 messages sent in quick succession, the system
/// should handle them by reusing containers rather than creating new ones
/// for each message.
#[tokio::test]
#[ignore]
async fn sqs_burst_messages_all_processed_within_container_limit() {
    let base_url = endpoint_url();
    let sqs_client = build_sqs_client().await;
    let queue_url = format!("{}/000000000000/reuse-test-queue", sqs_endpoint());

    wait_for_server(&base_url, Duration::from_secs(30)).await;

    // Drain leftover messages
    wait_for_queue_drain(&sqs_client, &queue_url, Duration::from_secs(10)).await;

    // Send 10 messages as fast as possible (burst)
    let num_messages = 10;
    for i in 0..num_messages {
        sqs_client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!(r#"{{"burst_index": {}}}"#, i))
            .send()
            .await
            .unwrap_or_else(|e| panic!("failed to send burst message {}: {}", i, e));
    }

    eprintln!(
        "Sent {} burst messages, waiting for all to be processed...",
        num_messages
    );

    // Wait for all messages to drain. Give extra time since burst may cause
    // some retries due to visibility timeouts.
    let final_count = wait_for_queue_drain(
        &sqs_client,
        &queue_url,
        Duration::from_secs(180),
    )
    .await;

    assert_eq!(
        final_count, 0,
        "all {} burst messages should be consumed, but {} remain. \
         Container reuse may not be working correctly.",
        num_messages, final_count
    );

    eprintln!("All {} burst messages processed!", num_messages);
}
