use serde_json::json;

use super::*;

// ---------------------------------------------------------------------------
// Config parsing tests
// ---------------------------------------------------------------------------

#[test]
fn parse_sqs_mapping_minimal() {
    let raw = vec![json!({
        "queue_url": "http://localhost:4566/000000000000/my-queue",
        "function_name": "my-func"
    })];
    let configs = parse_sqs_mappings(&raw);
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].queue_url, "http://localhost:4566/000000000000/my-queue");
    assert_eq!(configs[0].function_name, "my-func");
    assert_eq!(configs[0].batch_size, 10); // default
    assert!(configs[0].enabled);
    assert!(configs[0].maximum_batching_window_in_seconds.is_none());
    assert!(configs[0].function_response_types.is_empty());
    assert!(configs[0].endpoint_url.is_none());
    assert!(configs[0].event_source_arn.is_none());
    assert!(configs[0].region.is_none());
}

#[test]
fn parse_sqs_mapping_full() {
    let raw = vec![json!({
        "queue_url": "http://localhost:4566/000000000000/orders",
        "function_name": "process-orders",
        "batch_size": 5,
        "maximum_batching_window_in_seconds": 30,
        "function_response_types": ["ReportBatchItemFailures"],
        "endpoint_url": "http://localhost:4566",
        "event_source_arn": "arn:aws:sqs:us-east-1:123456789012:orders",
        "region": "eu-west-1",
        "enabled": true
    })];
    let configs = parse_sqs_mappings(&raw);
    assert_eq!(configs.len(), 1);
    let c = &configs[0];
    assert_eq!(c.batch_size, 5);
    assert_eq!(c.maximum_batching_window_in_seconds, Some(30));
    assert!(c.reports_batch_item_failures());
    assert_eq!(c.endpoint_url.as_deref(), Some("http://localhost:4566"));
    assert_eq!(
        c.event_source_arn.as_deref(),
        Some("arn:aws:sqs:us-east-1:123456789012:orders")
    );
    assert_eq!(c.region.as_deref(), Some("eu-west-1"));
}

#[test]
fn parse_sqs_mapping_disabled() {
    let raw = vec![json!({
        "queue_url": "http://localhost:4566/000000000000/q",
        "function_name": "f",
        "enabled": false
    })];
    let configs = parse_sqs_mappings(&raw);
    assert_eq!(configs.len(), 1);
    assert!(!configs[0].enabled);
}

#[test]
fn parse_sqs_mapping_type_field() {
    let raw = vec![json!({
        "type": "sqs",
        "queue_url": "http://localhost:4566/000000000000/q",
        "function_name": "f"
    })];
    let configs = parse_sqs_mappings(&raw);
    assert_eq!(configs.len(), 1);
}

#[test]
fn parse_sqs_mapping_skips_non_sqs() {
    let raw = vec![
        json!({
            "type": "kinesis",
            "stream_arn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
            "function_name": "f"
        }),
        json!({
            "queue_url": "http://localhost:4566/000000000000/q",
            "function_name": "g"
        }),
    ];
    let configs = parse_sqs_mappings(&raw);
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].function_name, "g");
}

#[test]
fn parse_sqs_mapping_skips_invalid_and_warns() {
    let raw = vec![json!({
        "queue_url": "http://localhost:4566/000000000000/q"
        // missing required function_name
    })];
    let configs = parse_sqs_mappings(&raw);
    assert_eq!(configs.len(), 0);
}

#[test]
fn parse_empty_mappings() {
    let configs = parse_sqs_mappings(&[]);
    assert!(configs.is_empty());
}

// ---------------------------------------------------------------------------
// Config validation tests
// ---------------------------------------------------------------------------

#[test]
fn parse_sqs_mapping_batch_size_too_large_is_clamped() {
    let raw = vec![json!({
        "queue_url": "http://localhost:4566/000000000000/q",
        "function_name": "f",
        "batch_size": 50
    })];
    let configs = parse_sqs_mappings(&raw);
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].batch_size, 10, "batch_size >10 should be clamped to 10");
}

#[test]
fn parse_sqs_mapping_batch_size_zero_is_clamped() {
    let raw = vec![json!({
        "queue_url": "http://localhost:4566/000000000000/q",
        "function_name": "f",
        "batch_size": 0
    })];
    let configs = parse_sqs_mappings(&raw);
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].batch_size, 1, "batch_size 0 should be clamped to 1");
}

#[test]
fn parse_sqs_mapping_batch_size_valid_range_unchanged() {
    for size in [1, 5, 10] {
        let raw = vec![json!({
            "queue_url": "http://localhost:4566/000000000000/q",
            "function_name": "f",
            "batch_size": size
        })];
        let configs = parse_sqs_mappings(&raw);
        assert_eq!(configs[0].batch_size, size as u32);
    }
}

// ---------------------------------------------------------------------------
// Config helper tests
// ---------------------------------------------------------------------------

#[test]
fn reports_batch_item_failures_true() {
    let config = SqsEventSourceConfig {
        queue_url: "http://localhost/q".into(),
        function_name: "f".into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec!["ReportBatchItemFailures".into()],
        endpoint_url: None,
        event_source_arn: None,
        region: None,
        enabled: true,
    };
    assert!(config.reports_batch_item_failures());
}

#[test]
fn reports_batch_item_failures_false() {
    let config = SqsEventSourceConfig {
        queue_url: "http://localhost/q".into(),
        function_name: "f".into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: None,
        event_source_arn: None,
        region: None,
        enabled: true,
    };
    assert!(!config.reports_batch_item_failures());
}

// ---------------------------------------------------------------------------
// Event payload builder tests
// ---------------------------------------------------------------------------

#[test]
fn build_sqs_event_single_message() {
    let msg = aws_sdk_sqs::types::Message::builder()
        .message_id("msg-001")
        .receipt_handle("rh-001")
        .body(r#"{"key": "value"}"#)
        .md5_of_body("abc123")
        .build();

    let event =
        build_sqs_event(&[msg], "arn:aws:sqs:us-east-1:123456789012:q", "us-east-1");

    let records = event["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);

    let r = &records[0];
    assert_eq!(r["messageId"], "msg-001");
    assert_eq!(r["receiptHandle"], "rh-001");
    assert_eq!(r["body"], r#"{"key": "value"}"#);
    assert_eq!(r["md5OfBody"], "abc123");
    assert_eq!(r["eventSource"], "aws:sqs");
    assert_eq!(
        r["eventSourceARN"],
        "arn:aws:sqs:us-east-1:123456789012:q"
    );
    assert_eq!(r["awsRegion"], "us-east-1");
}

#[test]
fn build_sqs_event_multiple_messages() {
    let msg1 = aws_sdk_sqs::types::Message::builder()
        .message_id("msg-001")
        .receipt_handle("rh-001")
        .body("body1")
        .build();
    let msg2 = aws_sdk_sqs::types::Message::builder()
        .message_id("msg-002")
        .receipt_handle("rh-002")
        .body("body2")
        .build();

    let event = build_sqs_event(
        &[msg1, msg2],
        "arn:aws:sqs:us-east-1:000:q",
        "us-east-1",
    );
    let records = event["Records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["messageId"], "msg-001");
    assert_eq!(records[1]["messageId"], "msg-002");
}

#[test]
fn build_sqs_event_empty_messages() {
    let event =
        build_sqs_event(&[], "arn:aws:sqs:us-east-1:000:q", "us-east-1");
    let records = event["Records"].as_array().unwrap();
    assert!(records.is_empty());
}

#[test]
fn build_sqs_event_missing_fields_use_defaults() {
    // Message with no optional fields set
    let msg = aws_sdk_sqs::types::Message::builder().build();
    let event =
        build_sqs_event(&[msg], "arn:aws:sqs:us-east-1:000:q", "us-east-1");
    let r = &event["Records"][0];
    assert_eq!(r["messageId"], "");
    assert_eq!(r["receiptHandle"], "");
    assert_eq!(r["body"], "");
    assert_eq!(r["md5OfBody"], "");
    assert_eq!(r["eventSource"], "aws:sqs");
}

// ---------------------------------------------------------------------------
// SqsPoller unit tests
// ---------------------------------------------------------------------------

#[test]
fn poller_description() {
    let config = SqsEventSourceConfig {
        queue_url: "http://localhost:4566/000000000000/my-queue".into(),
        function_name: "my-func".into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: None,
        event_source_arn: None,
        region: None,
        enabled: true,
    };
    let poller =
        SqsPoller::new(config, "us-east-1".into(), "000000000000".into());
    assert_eq!(
        poller.description(),
        "SQS(http://localhost:4566/000000000000/my-queue -> my-func)"
    );
}

#[test]
fn poller_event_source_arn_from_config() {
    let config = SqsEventSourceConfig {
        queue_url: "http://localhost:4566/000000000000/q".into(),
        function_name: "f".into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: None,
        event_source_arn: Some(
            "arn:aws:sqs:eu-west-1:111111111111:custom".into(),
        ),
        region: None,
        enabled: true,
    };
    let poller =
        SqsPoller::new(config, "us-east-1".into(), "000000000000".into());
    assert_eq!(
        poller.event_source_arn(),
        "arn:aws:sqs:eu-west-1:111111111111:custom"
    );
}

#[test]
fn poller_event_source_arn_synthesized() {
    let config = SqsEventSourceConfig {
        queue_url: "http://localhost:4566/000000000000/my-queue".into(),
        function_name: "f".into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: None,
        event_source_arn: None,
        region: None,
        enabled: true,
    };
    let poller =
        SqsPoller::new(config, "us-east-1".into(), "123456789012".into());
    assert_eq!(
        poller.event_source_arn(),
        "arn:aws:sqs:us-east-1:123456789012:my-queue"
    );
}

#[test]
fn poller_effective_region_override() {
    let config = SqsEventSourceConfig {
        queue_url: "http://localhost/q".into(),
        function_name: "f".into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: None,
        event_source_arn: None,
        region: Some("ap-southeast-1".into()),
        enabled: true,
    };
    let poller =
        SqsPoller::new(config, "us-east-1".into(), "000".into());
    assert_eq!(poller.effective_region(), "ap-southeast-1");
}

#[test]
fn poller_effective_region_default() {
    let config = SqsEventSourceConfig {
        queue_url: "http://localhost/q".into(),
        function_name: "f".into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: None,
        event_source_arn: None,
        region: None,
        enabled: true,
    };
    let poller =
        SqsPoller::new(config, "us-east-1".into(), "000".into());
    assert_eq!(poller.effective_region(), "us-east-1");
}

// ---------------------------------------------------------------------------
// Batch item failure parsing tests
// ---------------------------------------------------------------------------

#[test]
fn batch_item_failures_response_parsing() {
    let body = r#"{"batchItemFailures": [{"itemIdentifier": "msg-002"}, {"itemIdentifier": "msg-004"}]}"#;
    let resp: BatchItemFailuresResponse =
        serde_json::from_str(body).unwrap();
    assert_eq!(resp.batch_item_failures.len(), 2);
    assert_eq!(resp.batch_item_failures[0].item_identifier, "msg-002");
    assert_eq!(resp.batch_item_failures[1].item_identifier, "msg-004");
}

#[test]
fn batch_item_failures_empty_response() {
    let body = r#"{"batchItemFailures": []}"#;
    let resp: BatchItemFailuresResponse =
        serde_json::from_str(body).unwrap();
    assert!(resp.batch_item_failures.is_empty());
}

#[test]
fn batch_item_failures_missing_field_defaults_empty() {
    let body = r#"{}"#;
    let resp: BatchItemFailuresResponse =
        serde_json::from_str(body).unwrap();
    assert!(resp.batch_item_failures.is_empty());
}

// ---------------------------------------------------------------------------
// Poller run: exits when function doesn't exist
// ---------------------------------------------------------------------------

#[tokio::test]
async fn poller_exits_when_function_not_found() {
    use crate::eventsource::test_helpers::test_state;

    let config = SqsEventSourceConfig {
        queue_url: "http://localhost:4566/000000000000/q".into(),
        function_name: "nonexistent-function".into(),
        batch_size: 10,
        maximum_batching_window_in_seconds: None,
        function_response_types: vec![],
        endpoint_url: None,
        event_source_arn: None,
        region: None,
        enabled: true,
    };
    let poller =
        SqsPoller::new(config, "us-east-1".into(), "000000000000".into());

    let state = test_state();
    let (_tx, rx) = watch::channel(false);

    // Should return immediately because the function doesn't exist
    poller.run(state, rx).await;
    // If we reach here, the poller correctly exited
}
