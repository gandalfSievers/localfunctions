use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;
use serde_json::{json, Value};

/// Available event source types with their descriptions.
#[derive(Debug, Serialize)]
pub struct EventSourceInfo {
    pub id: &'static str,
    pub name: &'static str,
    pub description: &'static str,
}

/// Response for GET /admin/sample-events
#[derive(Debug, Serialize)]
pub struct ListSampleEventsResponse {
    pub event_sources: Vec<EventSourceInfo>,
}

/// All available sample event source types.
const EVENT_SOURCES: &[EventSourceInfo] = &[
    EventSourceInfo {
        id: "s3-put",
        name: "S3 Put Object",
        description: "Triggered when an object is created in an S3 bucket",
    },
    EventSourceInfo {
        id: "sqs-message",
        name: "SQS Message",
        description: "Triggered when a message is received from an SQS queue",
    },
    EventSourceInfo {
        id: "sns-notification",
        name: "SNS Notification",
        description: "Triggered when a message is published to an SNS topic",
    },
    EventSourceInfo {
        id: "apigw-proxy-request",
        name: "API Gateway Proxy Request",
        description: "Triggered by an HTTP request through API Gateway (REST API, proxy integration)",
    },
    EventSourceInfo {
        id: "eventbridge-scheduled",
        name: "EventBridge Scheduled Event",
        description: "Triggered by an EventBridge rule on a schedule or event pattern",
    },
];

/// GET /admin/sample-events — list available event source types.
pub async fn list_sample_events() -> Json<ListSampleEventsResponse> {
    let event_sources = EVENT_SOURCES
        .iter()
        .map(|s| EventSourceInfo {
            id: s.id,
            name: s.name,
            description: s.description,
        })
        .collect();
    Json(ListSampleEventsResponse { event_sources })
}

/// GET /admin/sample-events/:source — return a sample payload for the given source.
pub async fn get_sample_event(Path(source): Path<String>) -> impl IntoResponse {
    match generate_sample_event(&source) {
        Some(payload) => Json(payload).into_response(),
        None => {
            let body = json!({
                "error": "Unknown event source",
                "message": format!("Event source '{}' is not recognized. Use GET /admin/sample-events to list available sources.", source),
                "available_sources": EVENT_SOURCES.iter().map(|s| s.id).collect::<Vec<_>>(),
            });
            (StatusCode::NOT_FOUND, Json(body)).into_response()
        }
    }
}

fn generate_sample_event(source: &str) -> Option<Value> {
    match source {
        "s3-put" => Some(s3_put_event()),
        "sqs-message" => Some(sqs_message_event()),
        "sns-notification" => Some(sns_notification_event()),
        "apigw-proxy-request" => Some(apigw_proxy_request_event()),
        "eventbridge-scheduled" => Some(eventbridge_scheduled_event()),
        _ => None,
    }
}

fn s3_put_event() -> Value {
    json!({
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2024-01-15T12:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "EXAMPLE"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "my-sample-bucket",
                        "ownerIdentity": {
                            "principalId": "EXAMPLE"
                        },
                        "arn": "arn:aws:s3:::my-sample-bucket"
                    },
                    "object": {
                        "key": "test/key.json",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901"
                    }
                }
            }
        ]
    })
}

fn sqs_message_event() -> Value {
    json!({
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "{\"key\": \"value\"}",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1705320000000",
                    "SenderId": "123456789012",
                    "ApproximateFirstReceiveTimestamp": "1705320000100"
                },
                "messageAttributes": {
                    "TestAttribute": {
                        "stringValue": "TestValue",
                        "binaryValue": null,
                        "dataType": "String"
                    }
                },
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-sample-queue",
                "awsRegion": "us-east-1"
            }
        ]
    })
}

fn sns_notification_event() -> Value {
    json!({
        "Records": [
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:us-east-1:123456789012:my-sample-topic:2bcfbf39-05c3-41de-beaa-fcfcc21c8f55",
                "EventSource": "aws:sns",
                "Sns": {
                    "SignatureVersion": "1",
                    "Timestamp": "2024-01-15T12:00:00.000Z",
                    "Signature": "EXAMPLE",
                    "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-0000000000000000000000.pem",
                    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                    "Message": "{\"key\": \"value\"}",
                    "MessageAttributes": {
                        "TestAttribute": {
                            "Type": "String",
                            "Value": "TestValue"
                        }
                    },
                    "Type": "Notification",
                    "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:123456789012:my-sample-topic:2bcfbf39-05c3-41de-beaa-fcfcc21c8f55",
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:my-sample-topic",
                    "Subject": "TestInvoke"
                }
            }
        ]
    })
}

fn apigw_proxy_request_event() -> Value {
    json!({
        "resource": "/my/path",
        "path": "/my/path",
        "httpMethod": "GET",
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
            "User-Agent": "Mozilla/5.0",
            "X-Forwarded-For": "127.0.0.1",
            "X-Forwarded-Port": "443",
            "X-Forwarded-Proto": "https"
        },
        "multiValueHeaders": {
            "Accept": ["text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"],
            "Accept-Encoding": ["gzip, deflate"],
            "Host": ["1234567890.execute-api.us-east-1.amazonaws.com"],
            "User-Agent": ["Mozilla/5.0"],
            "X-Forwarded-For": ["127.0.0.1"],
            "X-Forwarded-Port": ["443"],
            "X-Forwarded-Proto": ["https"]
        },
        "queryStringParameters": {
            "foo": "bar"
        },
        "multiValueQueryStringParameters": {
            "foo": ["bar"]
        },
        "pathParameters": null,
        "stageVariables": null,
        "requestContext": {
            "resourceId": "123456",
            "resourcePath": "/my/path",
            "httpMethod": "GET",
            "extendedRequestId": "request-id",
            "requestTime": "15/Jan/2024:12:00:00 +0000",
            "path": "/prod/my/path",
            "accountId": "123456789012",
            "protocol": "HTTP/1.1",
            "stage": "prod",
            "domainPrefix": "1234567890",
            "requestTimeEpoch": 1705320000000_i64,
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "identity": {
                "cognitoIdentityPoolId": null,
                "accountId": null,
                "cognitoIdentityId": null,
                "caller": null,
                "accessKey": null,
                "sourceIp": "127.0.0.1",
                "cognitoAuthenticationType": null,
                "cognitoAuthenticationProvider": null,
                "userArn": null,
                "userAgent": "Mozilla/5.0",
                "user": null
            },
            "domainName": "1234567890.execute-api.us-east-1.amazonaws.com",
            "apiId": "1234567890"
        },
        "body": null,
        "isBase64Encoded": false
    })
}

fn eventbridge_scheduled_event() -> Value {
    json!({
        "version": "0",
        "id": "12345678-1234-1234-1234-123456789012",
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "123456789012",
        "time": "2024-01-15T12:00:00Z",
        "region": "us-east-1",
        "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/my-scheduled-rule"
        ],
        "detail": {}
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use http::Request;
    use tower::ServiceExt;

    fn sample_events_router() -> Router {
        Router::new()
            .route("/admin/sample-events", get(list_sample_events))
            .route("/admin/sample-events/:source", get(get_sample_event))
    }

    async fn get_json(app: Router, uri: &str) -> (StatusCode, Value) {
        let resp = app
            .oneshot(
                Request::get(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        (status, parsed)
    }

    #[tokio::test]
    async fn list_returns_all_event_sources() {
        let (status, parsed) = get_json(sample_events_router(), "/admin/sample-events").await;
        assert_eq!(status, StatusCode::OK);

        let sources = parsed["event_sources"].as_array().unwrap();
        assert_eq!(sources.len(), 5);

        let ids: Vec<&str> = sources.iter().map(|s| s["id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"s3-put"));
        assert!(ids.contains(&"sqs-message"));
        assert!(ids.contains(&"sns-notification"));
        assert!(ids.contains(&"apigw-proxy-request"));
        assert!(ids.contains(&"eventbridge-scheduled"));
    }

    #[tokio::test]
    async fn get_s3_put_event() {
        let (status, parsed) = get_json(sample_events_router(), "/admin/sample-events/s3-put").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parsed["Records"][0]["eventSource"].as_str(), Some("aws:s3"));
        assert_eq!(parsed["Records"][0]["eventName"].as_str(), Some("ObjectCreated:Put"));
        assert_eq!(parsed["Records"][0]["s3"]["bucket"]["name"].as_str(), Some("my-sample-bucket"));
    }

    #[tokio::test]
    async fn get_sqs_message_event() {
        let (status, parsed) = get_json(sample_events_router(), "/admin/sample-events/sqs-message").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parsed["Records"][0]["eventSource"].as_str(), Some("aws:sqs"));
        assert!(parsed["Records"][0]["eventSourceARN"].as_str().unwrap().contains("my-sample-queue"));
    }

    #[tokio::test]
    async fn get_sns_notification_event() {
        let (status, parsed) = get_json(sample_events_router(), "/admin/sample-events/sns-notification").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parsed["Records"][0]["EventSource"].as_str(), Some("aws:sns"));
        assert!(parsed["Records"][0]["Sns"]["TopicArn"].as_str().unwrap().contains("my-sample-topic"));
    }

    #[tokio::test]
    async fn get_apigw_proxy_request_event() {
        let (status, parsed) = get_json(sample_events_router(), "/admin/sample-events/apigw-proxy-request").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parsed["httpMethod"].as_str(), Some("GET"));
        assert_eq!(parsed["path"].as_str(), Some("/my/path"));
        assert!(parsed["requestContext"]["requestId"].as_str().is_some());
    }

    #[tokio::test]
    async fn get_eventbridge_scheduled_event() {
        let (status, parsed) = get_json(sample_events_router(), "/admin/sample-events/eventbridge-scheduled").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parsed["source"].as_str(), Some("aws.events"));
        assert_eq!(parsed["detail-type"].as_str(), Some("Scheduled Event"));
        assert!(!parsed["resources"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_unknown_source_returns_404() {
        let (status, parsed) = get_json(sample_events_router(), "/admin/sample-events/unknown-source").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(parsed["error"].as_str(), Some("Unknown event source"));
        assert_eq!(parsed["available_sources"].as_array().unwrap().len(), 5);
    }

    #[tokio::test]
    async fn all_payloads_are_valid_json() {
        let sources = ["s3-put", "sqs-message", "sns-notification", "apigw-proxy-request", "eventbridge-scheduled"];
        for source in &sources {
            let payload = generate_sample_event(source);
            assert!(payload.is_some(), "Missing payload for {}", source);
            // Verify it serializes to valid JSON string and back
            let json_str = serde_json::to_string(&payload.unwrap()).unwrap();
            let _: Value = serde_json::from_str(&json_str).unwrap();
        }
    }
}
