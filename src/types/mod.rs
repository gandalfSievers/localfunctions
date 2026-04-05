use std::collections::HashMap;
use std::path::PathBuf;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// FunctionConfig
// ---------------------------------------------------------------------------

/// Configuration for a single Lambda function.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct FunctionConfig {
    pub name: String,
    pub runtime: String,
    pub handler: String,
    pub code_path: PathBuf,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default = "default_memory_size")]
    pub memory_size: u64,
    #[serde(default)]
    pub environment: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
}

#[allow(dead_code)]
fn default_timeout() -> u64 {
    3
}

#[allow(dead_code)]
fn default_memory_size() -> u64 {
    128
}

// ---------------------------------------------------------------------------
// Invocation
// ---------------------------------------------------------------------------

/// A single invocation request sent to a function container.
#[derive(Debug)]
#[allow(dead_code)]
pub struct Invocation {
    pub request_id: Uuid,
    pub function_name: String,
    pub payload: Bytes,
    pub deadline: Instant,
    pub response_tx: oneshot::Sender<InvocationResult>,
}

// ---------------------------------------------------------------------------
// InvocationResult
// ---------------------------------------------------------------------------

/// The outcome of a single function invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status")]
pub enum InvocationResult {
    #[serde(rename = "success")]
    Success { body: String },
    #[serde(rename = "error")]
    Error {
        error_type: String,
        error_message: String,
    },
    #[serde(rename = "timeout")]
    Timeout,
}

// ---------------------------------------------------------------------------
// ContainerState / ContainerInstance
// ---------------------------------------------------------------------------

/// Lifecycle state of a function container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContainerState {
    Starting,
    Idle,
    Busy,
    Stopping,
}

/// A running container that can serve invocations.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ContainerInstance {
    pub container_id: String,
    pub function_name: String,
    pub state: ContainerState,
    pub created_at: Instant,
    pub last_used: Instant,
    pub invocation_tx: mpsc::Sender<Invocation>,
}

// ---------------------------------------------------------------------------
// Service errors (AWS Lambda-compatible)
// ---------------------------------------------------------------------------

/// Errors returned by the invoke/management APIs, modelled after AWS Lambda
/// error codes.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum ServiceError {
    #[error("Function not found: {0}")]
    ResourceNotFound(String),

    #[error("Internal service error: {0}")]
    ServiceException(String),

    #[error("Invalid runtime: {0}")]
    InvalidRuntime(String),

    #[error("Too many requests: {0}")]
    TooManyRequests(String),

    #[error("Invalid request content: {0}")]
    InvalidRequestContent(String),
}

/// AWS-compatible error response body.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub struct AwsErrorResponse {
    pub Type: String,
    pub Message: String,
}

#[allow(dead_code)]
impl ServiceError {
    /// Return the AWS error type string for this variant.
    pub fn error_type(&self) -> &'static str {
        match self {
            ServiceError::ResourceNotFound(_) => "ResourceNotFoundException",
            ServiceError::ServiceException(_) => "ServiceException",
            ServiceError::InvalidRuntime(_) => "InvalidRuntimeException",
            ServiceError::TooManyRequests(_) => "TooManyRequestsException",
            ServiceError::InvalidRequestContent(_) => "InvalidRequestContentException",
        }
    }

    /// Return the HTTP status code for this error.
    pub fn status_code(&self) -> http::StatusCode {
        match self {
            ServiceError::ResourceNotFound(_) => http::StatusCode::NOT_FOUND,
            ServiceError::ServiceException(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
            ServiceError::InvalidRuntime(_) => http::StatusCode::BAD_REQUEST,
            ServiceError::TooManyRequests(_) => http::StatusCode::TOO_MANY_REQUESTS,
            ServiceError::InvalidRequestContent(_) => http::StatusCode::BAD_REQUEST,
        }
    }

    /// Serialize to the AWS `{Type, Message}` JSON format.
    pub fn to_aws_response(&self) -> AwsErrorResponse {
        AwsErrorResponse {
            Type: self.error_type().to_string(),
            Message: self.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- FunctionConfig ------------------------------------------------------

    #[test]
    fn function_config_serialization_roundtrip() {
        let config = FunctionConfig {
            name: "my-func".into(),
            runtime: "python3.12".into(),
            handler: "main.handler".into(),
            code_path: PathBuf::from("/tmp/code"),
            timeout: 30,
            memory_size: 256,
            environment: HashMap::from([("KEY".into(), "value".into())]),
            image: Some("custom:latest".into()),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: FunctionConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "my-func");
        assert_eq!(deserialized.runtime, "python3.12");
        assert_eq!(deserialized.handler, "main.handler");
        assert_eq!(deserialized.code_path, PathBuf::from("/tmp/code"));
        assert_eq!(deserialized.timeout, 30);
        assert_eq!(deserialized.memory_size, 256);
        assert_eq!(deserialized.environment.get("KEY").unwrap(), "value");
        assert_eq!(deserialized.image, Some("custom:latest".into()));
    }

    #[test]
    fn function_config_defaults() {
        let json = r#"{
            "name": "f",
            "runtime": "nodejs20.x",
            "handler": "index.handler",
            "code_path": "/code"
        }"#;

        let config: FunctionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.timeout, 3);
        assert_eq!(config.memory_size, 128);
        assert!(config.environment.is_empty());
        assert!(config.image.is_none());
    }

    #[test]
    fn function_config_with_optional_image() {
        let config = FunctionConfig {
            name: "f".into(),
            runtime: "python3.12".into(),
            handler: "h".into(),
            code_path: PathBuf::from("/c"),
            timeout: 3,
            memory_size: 128,
            environment: HashMap::new(),
            image: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        // image should be absent when None
        assert!(!json.contains("image"));
    }

    // -- InvocationResult ----------------------------------------------------

    #[test]
    fn invocation_result_success_serde() {
        let result = InvocationResult::Success {
            body: "ok".into(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: InvocationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, deserialized);
        assert!(json.contains(r#""status":"success"#));
    }

    #[test]
    fn invocation_result_error_serde() {
        let result = InvocationResult::Error {
            error_type: "RuntimeError".into(),
            error_message: "something went wrong".into(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: InvocationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, deserialized);
        assert!(json.contains(r#""status":"error"#));
    }

    #[test]
    fn invocation_result_timeout_serde() {
        let result = InvocationResult::Timeout;
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: InvocationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, deserialized);
        assert!(json.contains(r#""status":"timeout"#));
    }

    // -- ContainerState ------------------------------------------------------

    #[test]
    fn container_state_serde() {
        for (state, expected) in [
            (ContainerState::Starting, "\"starting\""),
            (ContainerState::Idle, "\"idle\""),
            (ContainerState::Busy, "\"busy\""),
            (ContainerState::Stopping, "\"stopping\""),
        ] {
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(json, expected);
            let deserialized: ContainerState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, deserialized);
        }
    }

    // -- ServiceError / AwsErrorResponse -------------------------------------

    #[test]
    fn service_error_to_aws_response_resource_not_found() {
        let err = ServiceError::ResourceNotFound("my-func".into());
        let resp = err.to_aws_response();
        assert_eq!(resp.Type, "ResourceNotFoundException");
        assert_eq!(resp.Message, "Function not found: my-func");
        assert_eq!(err.status_code(), http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn service_error_to_aws_response_service_exception() {
        let err = ServiceError::ServiceException("boom".into());
        let resp = err.to_aws_response();
        assert_eq!(resp.Type, "ServiceException");
        assert_eq!(resp.Message, "Internal service error: boom");
        assert_eq!(err.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn service_error_to_aws_response_invalid_runtime() {
        let err = ServiceError::InvalidRuntime("cobol".into());
        let resp = err.to_aws_response();
        assert_eq!(resp.Type, "InvalidRuntimeException");
        assert_eq!(resp.Message, "Invalid runtime: cobol");
        assert_eq!(err.status_code(), http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn service_error_to_aws_response_too_many_requests() {
        let err = ServiceError::TooManyRequests("slow down".into());
        let resp = err.to_aws_response();
        assert_eq!(resp.Type, "TooManyRequestsException");
        assert_eq!(resp.Message, "Too many requests: slow down");
        assert_eq!(err.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn service_error_to_aws_response_invalid_request_content() {
        let err = ServiceError::InvalidRequestContent("bad json".into());
        let resp = err.to_aws_response();
        assert_eq!(resp.Type, "InvalidRequestContentException");
        assert_eq!(resp.Message, "Invalid request content: bad json");
        assert_eq!(err.status_code(), http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn aws_error_response_serde() {
        let resp = AwsErrorResponse {
            Type: "ResourceNotFoundException".into(),
            Message: "Function not found: f".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""Type":"ResourceNotFoundException"#));
        assert!(json.contains(r#""Message":"Function not found: f"#));

        let deserialized: AwsErrorResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, deserialized);
    }

    // -- Invocation (non-serde, since it contains non-serializable fields) ---

    #[test]
    fn invocation_can_be_constructed() {
        let (tx, _rx) = oneshot::channel();
        let inv = Invocation {
            request_id: Uuid::new_v4(),
            function_name: "test-fn".into(),
            payload: Bytes::from("{}"),
            deadline: Instant::now() + std::time::Duration::from_secs(30),
            response_tx: tx,
        };
        assert_eq!(inv.function_name, "test-fn");
        assert_eq!(inv.payload, Bytes::from("{}"));
    }

    #[test]
    fn invocation_response_channel_works() {
        let (tx, mut rx) = oneshot::channel();
        let _inv = Invocation {
            request_id: Uuid::new_v4(),
            function_name: "f".into(),
            payload: Bytes::new(),
            deadline: Instant::now() + std::time::Duration::from_secs(5),
            response_tx: tx,
        };
        // Simulate sending a result back
        _inv.response_tx
            .send(InvocationResult::Success {
                body: "done".into(),
            })
            .unwrap();
        let result = rx.try_recv().unwrap();
        assert_eq!(
            result,
            InvocationResult::Success {
                body: "done".into()
            }
        );
    }

    // -- ContainerInstance ---------------------------------------------------

    #[test]
    fn container_instance_can_be_constructed() {
        let (tx, _rx) = mpsc::channel(1);
        let inst = ContainerInstance {
            container_id: "abc123".into(),
            function_name: "my-func".into(),
            state: ContainerState::Idle,
            created_at: Instant::now(),
            last_used: Instant::now(),
            invocation_tx: tx,
        };
        assert_eq!(inst.container_id, "abc123");
        assert_eq!(inst.state, ContainerState::Idle);
    }
}
