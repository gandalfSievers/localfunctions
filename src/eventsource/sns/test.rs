use serde_json::json;

use super::*;

// ---------------------------------------------------------------------------
// Config parsing tests
// ---------------------------------------------------------------------------

#[test]
fn parse_sns_subscription_minimal() {
    let raw = vec![json!({
        "topic_arn": "arn:aws:sns:us-east-1:000000000000:my-topic",
        "function_name": "my-func"
    })];
    let configs = parse_sns_subscriptions(&raw);
    assert_eq!(configs.len(), 1);
    assert_eq!(
        configs[0].topic_arn,
        "arn:aws:sns:us-east-1:000000000000:my-topic"
    );
    assert_eq!(configs[0].function_name, "my-func");
    assert!(configs[0].filter_policy.is_none());
    assert!(configs[0].filter_policy_scope.is_none());
    assert!(configs[0].endpoint_url.is_none());
    assert!(configs[0].region.is_none());
    assert!(configs[0].enabled);
}

#[test]
fn parse_sns_subscription_full() {
    let raw = vec![json!({
        "topic_arn": "arn:aws:sns:us-east-1:123456789012:orders",
        "function_name": "process-orders",
        "filter_policy": {
            "store": ["example_corp"],
            "event": [{"anything-but": "order_cancelled"}]
        },
        "filter_policy_scope": "MessageAttributes",
        "endpoint_url": "http://localhost:4566",
        "region": "eu-west-1",
        "enabled": true
    })];
    let configs = parse_sns_subscriptions(&raw);
    assert_eq!(configs.len(), 1);
    let c = &configs[0];
    assert_eq!(c.topic_arn, "arn:aws:sns:us-east-1:123456789012:orders");
    assert_eq!(c.function_name, "process-orders");
    assert!(c.filter_policy.is_some());
    let fp = c.filter_policy.as_ref().unwrap();
    assert!(fp["store"].is_array());
    assert_eq!(
        c.filter_policy_scope.as_deref(),
        Some("MessageAttributes")
    );
    assert_eq!(c.endpoint_url.as_deref(), Some("http://localhost:4566"));
    assert_eq!(c.region.as_deref(), Some("eu-west-1"));
    assert!(c.enabled);
}

#[test]
fn parse_sns_subscription_disabled() {
    let raw = vec![json!({
        "topic_arn": "arn:aws:sns:us-east-1:000:t",
        "function_name": "f",
        "enabled": false
    })];
    let configs = parse_sns_subscriptions(&raw);
    assert_eq!(configs.len(), 1);
    assert!(!configs[0].enabled);
}

#[test]
fn parse_sns_subscription_skips_invalid() {
    let raw = vec![json!({
        "topic_arn": "arn:aws:sns:us-east-1:000:t"
        // missing required function_name
    })];
    let configs = parse_sns_subscriptions(&raw);
    assert_eq!(configs.len(), 0);
}

#[test]
fn parse_sns_subscriptions_empty() {
    let configs = parse_sns_subscriptions(&[]);
    assert!(configs.is_empty());
}

#[test]
fn parse_sns_subscription_multiple() {
    let raw = vec![
        json!({
            "topic_arn": "arn:aws:sns:us-east-1:000:topic-a",
            "function_name": "func-a"
        }),
        json!({
            "topic_arn": "arn:aws:sns:us-east-1:000:topic-b",
            "function_name": "func-b",
            "filter_policy": {"color": ["red"]}
        }),
    ];
    let configs = parse_sns_subscriptions(&raw);
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].function_name, "func-a");
    assert!(configs[0].filter_policy.is_none());
    assert_eq!(configs[1].function_name, "func-b");
    assert!(configs[1].filter_policy.is_some());
}

#[test]
fn parse_sns_subscription_with_message_body_filter_scope() {
    let raw = vec![json!({
        "topic_arn": "arn:aws:sns:us-east-1:000:t",
        "function_name": "f",
        "filter_policy": {"price": [{"numeric": [">", 100]}]},
        "filter_policy_scope": "MessageBody"
    })];
    let configs = parse_sns_subscriptions(&raw);
    assert_eq!(configs.len(), 1);
    assert_eq!(
        configs[0].filter_policy_scope.as_deref(),
        Some("MessageBody")
    );
}

#[test]
fn parse_sns_subscription_filter_policy_serializes() {
    let policy = json!({
        "store": ["example_corp"],
        "event": [{"anything-but": "order_cancelled"}]
    });
    let raw = vec![json!({
        "topic_arn": "arn:aws:sns:us-east-1:000:t",
        "function_name": "f",
        "filter_policy": policy
    })];
    let configs = parse_sns_subscriptions(&raw);
    let serialized =
        serde_json::to_string(configs[0].filter_policy.as_ref().unwrap())
            .unwrap();
    // Verify it round-trips as valid JSON
    let _: serde_json::Value = serde_json::from_str(&serialized).unwrap();
}
