//! SQS event source mapping poller.
//!
//! Implements the [`EventSourcePoller`] trait to long-poll an SQS-compatible
//! queue (LocalStack, ElasticMQ) and invoke Lambda functions with correctly
//! formatted SQS event payloads.

#[cfg(test)]
mod test;

use std::time::{Duration, Instant};

use aws_sdk_sqs::Client as SqsClient;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use super::{invoke_function_with_event, EventSourcePoller};
use crate::server::AppState;
use crate::types::InvocationResult;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Typed configuration for an SQS event source mapping, parsed from the
/// `event_source_mappings` array in functions.json.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SqsEventSourceConfig {
    /// The SQS queue URL to poll.
    pub queue_url: String,
    /// The function to invoke when messages are received.
    pub function_name: String,
    /// Maximum number of messages per batch (1-10, default 10).
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    /// Maximum time in seconds to accumulate messages before invoking.
    /// When set, messages are collected across multiple ReceiveMessage calls
    /// until `batch_size` is reached or this window expires.
    #[serde(default)]
    pub maximum_batching_window_in_seconds: Option<u32>,
    /// Whether the function reports batch item failures. When enabled, only
    /// messages NOT listed in `batchItemFailures` are deleted.
    #[serde(default)]
    pub function_response_types: Vec<String>,
    /// Override the SQS endpoint URL (e.g. for LocalStack or ElasticMQ).
    #[serde(default)]
    pub endpoint_url: Option<String>,
    /// The event source ARN to include in the event payload.
    /// Defaults to a synthetic ARN based on region, account, and queue name.
    #[serde(default)]
    pub event_source_arn: Option<String>,
    /// AWS region override for the SQS client.
    #[serde(default)]
    pub region: Option<String>,
    /// Whether this mapping is enabled (default true).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_batch_size() -> u32 {
    10
}

fn default_enabled() -> bool {
    true
}

impl SqsEventSourceConfig {
    /// Returns true if function_response_types includes "ReportBatchItemFailures".
    pub fn reports_batch_item_failures(&self) -> bool {
        self.function_response_types
            .iter()
            .any(|t| t == "ReportBatchItemFailures")
    }
}

/// Parse SQS event source mappings from the raw JSON values in FunctionsConfig.
///
/// Returns all parsed configs including disabled ones (where `enabled` is false).
/// Callers are responsible for filtering out disabled mappings before starting
/// pollers — see `main.rs` for the filtering logic.
///
/// Invalid mappings (missing required fields, invalid batch_size) are skipped
/// with a warning log.
pub fn parse_sqs_mappings(
    raw_mappings: &[Value],
) -> Vec<SqsEventSourceConfig> {
    let mut configs = Vec::new();
    for value in raw_mappings {
        // Only parse mappings with queue_url field or explicit "type": "sqs".
        let is_sqs = value
            .get("type")
            .and_then(|v| v.as_str())
            .map(|t| t.eq_ignore_ascii_case("sqs"))
            .unwrap_or(false)
            || value.get("queue_url").is_some();

        if !is_sqs {
            continue;
        }

        match serde_json::from_value::<SqsEventSourceConfig>(value.clone()) {
            Ok(mut config) => {
                // Validate batch_size: SQS ReceiveMessage supports 1-10.
                if config.batch_size < 1 || config.batch_size > 10 {
                    warn!(
                        batch_size = config.batch_size,
                        function_name = %config.function_name,
                        "batch_size must be between 1 and 10, clamping to valid range"
                    );
                    config.batch_size = config.batch_size.clamp(1, 10);
                }
                configs.push(config);
            }
            Err(e) => {
                warn!(
                    error = %e,
                    mapping = %value,
                    "failed to parse SQS event source mapping, skipping"
                );
            }
        }
    }
    configs
}

// ---------------------------------------------------------------------------
// SQS event payload builder
// ---------------------------------------------------------------------------

/// Build an SQS Lambda event payload from received messages.
fn build_sqs_event(
    messages: &[aws_sdk_sqs::types::Message],
    event_source_arn: &str,
    region: &str,
) -> Value {
    let records: Vec<Value> = messages
        .iter()
        .map(|msg| {
            let body = msg.body().unwrap_or("");
            let md5 = msg.md5_of_body().unwrap_or("");

            // Build attributes map
            let attributes = if let Some(attrs) = msg.attributes() {
                let mut map = serde_json::Map::new();
                for (k, v) in attrs {
                    map.insert(k.as_str().to_string(), json!(v));
                }
                Value::Object(map)
            } else {
                json!({})
            };

            // Build message attributes map
            let message_attributes =
                if let Some(msg_attrs) = msg.message_attributes() {
                    let mut map = serde_json::Map::new();
                    for (k, v) in msg_attrs {
                        let mut attr = serde_json::Map::new();
                        if let Some(sv) = v.string_value() {
                            attr.insert(
                                "stringValue".to_string(),
                                json!(sv),
                            );
                        } else {
                            attr.insert(
                                "stringValue".to_string(),
                                Value::Null,
                            );
                        }
                        if let Some(bv) = v.binary_value() {
                            attr.insert(
                                "binaryValue".to_string(),
                                json!(base64::Engine::encode(
                                    &base64::engine::general_purpose::STANDARD,
                                    bv.as_ref()
                                )),
                            );
                        } else {
                            attr.insert(
                                "binaryValue".to_string(),
                                Value::Null,
                            );
                        }
                        attr.insert(
                            "dataType".to_string(),
                            json!(v.data_type()),
                        );
                        map.insert(k.clone(), Value::Object(attr));
                    }
                    Value::Object(map)
                } else {
                    json!({})
                };

            json!({
                "messageId": msg.message_id().unwrap_or(""),
                "receiptHandle": msg.receipt_handle().unwrap_or(""),
                "body": body,
                "attributes": attributes,
                "messageAttributes": message_attributes,
                "md5OfBody": md5,
                "eventSource": "aws:sqs",
                "eventSourceARN": event_source_arn,
                "awsRegion": region
            })
        })
        .collect();

    json!({ "Records": records })
}

// ---------------------------------------------------------------------------
// Batch item failure response parsing
// ---------------------------------------------------------------------------

/// Response format when function_response_types includes ReportBatchItemFailures.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchItemFailuresResponse {
    #[serde(default)]
    batch_item_failures: Vec<BatchItemFailure>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchItemFailure {
    item_identifier: String,
}

// ---------------------------------------------------------------------------
// SqsPoller
// ---------------------------------------------------------------------------

/// Polls an SQS queue and invokes a Lambda function with batches of messages.
pub struct SqsPoller {
    config: SqsEventSourceConfig,
    region: String,
    account_id: String,
}

impl SqsPoller {
    pub fn new(
        config: SqsEventSourceConfig,
        region: String,
        account_id: String,
    ) -> Self {
        Self {
            config,
            region,
            account_id,
        }
    }

    /// Derive the event source ARN from config or synthesize one.
    fn event_source_arn(&self) -> String {
        if let Some(ref arn) = self.config.event_source_arn {
            return arn.clone();
        }
        // Extract queue name from URL (last path segment)
        let queue_name = self
            .config
            .queue_url
            .rsplit('/')
            .next()
            .unwrap_or("unknown");
        format!(
            "arn:aws:sqs:{}:{}:{}",
            self.effective_region(),
            self.account_id,
            queue_name
        )
    }

    fn effective_region(&self) -> &str {
        self.config
            .region
            .as_deref()
            .unwrap_or(&self.region)
    }

    /// Build the SQS client with optional endpoint override.
    ///
    /// Uses the default AWS credential chain so that callers can supply
    /// dummy credentials (e.g. `AWS_ACCESS_KEY_ID=test`) for SigV4 signing.
    /// This allows the SDK to work with DNS-overridden AWS endpoints
    /// (e.g. `sqs.us-east-1.amazonaws.com` routed to ElasticMQ via Traefik).
    async fn build_client(&self) -> SqsClient {
        let region = aws_sdk_sqs::config::Region::new(
            self.effective_region().to_string(),
        );

        let mut config_builder = aws_config::defaults(
            aws_config::BehaviorVersion::latest(),
        )
        .region(region.clone());

        if let Some(ref endpoint) = self.config.endpoint_url {
            config_builder = config_builder.endpoint_url(endpoint);
        }

        let sdk_config = config_builder.load().await;
        SqsClient::new(&sdk_config)
    }

    /// Delete successfully processed messages from the queue using batch delete.
    ///
    /// Uses `DeleteMessageBatch` for efficiency (up to 10 messages per call).
    /// Accepts references to messages to avoid unnecessary cloning.
    async fn delete_messages(
        &self,
        client: &SqsClient,
        messages: &[&aws_sdk_sqs::types::Message],
    ) {
        // DeleteMessageBatch supports up to 10 entries per call.
        for chunk in messages.chunks(10) {
            let entries: Vec<_> = chunk
                .iter()
                .enumerate()
                .filter_map(|(i, msg)| {
                    msg.receipt_handle().map(|rh| {
                        aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                            .id(format!("{}", i))
                            .receipt_handle(rh)
                            .build()
                    })
                })
                .filter_map(|result| match result {
                    Ok(entry) => Some(entry),
                    Err(e) => {
                        warn!(error = %e, "failed to build DeleteMessageBatchRequestEntry, skipping");
                        None
                    }
                })
                .collect();

            if entries.is_empty() {
                continue;
            }

            match client
                .delete_message_batch()
                .queue_url(&self.config.queue_url)
                .set_entries(Some(entries))
                .send()
                .await
            {
                Ok(output) => {
                    for f in output.failed() {
                        warn!(
                            id = %f.id(),
                            code = %f.code(),
                            message = f.message().unwrap_or(""),
                            "failed to delete SQS message in batch"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        count = chunk.len(),
                        "DeleteMessageBatch call failed"
                    );
                }
            }
        }
    }

    /// Receive messages, optionally accumulating across multiple calls if
    /// a batching window is configured.
    async fn receive_batch(
        &self,
        client: &SqsClient,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Result<Vec<aws_sdk_sqs::types::Message>, aws_sdk_sqs::error::SdkError<aws_sdk_sqs::operation::receive_message::ReceiveMessageError>> {
        let batch_size = self.config.batch_size as i32;

        match self.config.maximum_batching_window_in_seconds {
            Some(window_secs) if window_secs > 0 => {
                // Accumulate messages across multiple receives until
                // batch_size or window expires.
                let mut accumulated = Vec::new();
                let deadline =
                    Instant::now() + Duration::from_secs(window_secs as u64);

                while (accumulated.len() as i32) < batch_size {
                    let remaining = batch_size - accumulated.len() as i32;
                    let remaining_time = deadline.saturating_duration_since(Instant::now());
                    if remaining_time < Duration::from_secs(1) {
                        break;
                    }

                    // Use shorter wait time within the window
                    let wait_secs = remaining_time
                        .as_secs()
                        .min(20) as i32;

                    let result = tokio::select! {
                        res = client
                            .receive_message()
                            .queue_url(&self.config.queue_url)
                            .max_number_of_messages(remaining)
                            .wait_time_seconds(wait_secs)
                            .message_system_attribute_names(aws_sdk_sqs::types::MessageSystemAttributeName::All)
                            .message_attribute_names("All")
                            .send() => res,
                        _ = shutdown_rx.changed() => {
                            break;
                        }
                    };

                    match result {
                        Ok(output) => {
                            if let Some(msgs) = output.messages {
                                accumulated.extend(msgs);
                            }
                        }
                        Err(e) => {
                            if accumulated.is_empty() {
                                return Err(e);
                            }
                            // Return what we have so far
                            warn!(error = %e, "SQS receive error during batching window, proceeding with accumulated messages");
                            break;
                        }
                    }
                }

                Ok(accumulated)
            }
            _ => {
                // Simple single receive with long polling
                let recv_future = client
                    .receive_message()
                    .queue_url(&self.config.queue_url)
                    .max_number_of_messages(batch_size)
                    .wait_time_seconds(20)
                    .message_system_attribute_names(aws_sdk_sqs::types::MessageSystemAttributeName::All)
                    .message_attribute_names("All")
                    .send();

                tokio::select! {
                    res = recv_future => {
                        res.map(|output| output.messages.unwrap_or_default())
                    }
                    _ = shutdown_rx.changed() => {
                        Ok(Vec::new())
                    }
                }
            }
        }
    }

    /// Process a batch of messages: invoke function and handle deletions.
    async fn process_batch(
        &self,
        client: &SqsClient,
        state: &AppState,
        messages: Vec<aws_sdk_sqs::types::Message>,
    ) {
        let event_source_arn = self.event_source_arn();
        let region = self.effective_region().to_string();
        let payload = build_sqs_event(&messages, &event_source_arn, &region);
        let payload_bytes = match serde_json::to_vec(&payload) {
            Ok(bytes) => Bytes::from(bytes),
            Err(e) => {
                warn!(
                    error = %e,
                    function = %self.config.function_name,
                    "failed to serialize SQS event payload, messages remain on queue"
                );
                return;
            }
        };

        debug!(
            function = %self.config.function_name,
            message_count = messages.len(),
            "invoking function with SQS batch"
        );

        match invoke_function_with_event(
            state,
            &self.config.function_name,
            payload_bytes,
        )
        .await
        {
            Ok(InvocationResult::Success { body }) => {
                if self.config.reports_batch_item_failures() {
                    // Parse response for partial batch failures
                    self.handle_batch_item_failures(
                        client, &messages, &body,
                    )
                    .await;
                } else {
                    // Delete all messages on success
                    let refs: Vec<&aws_sdk_sqs::types::Message> =
                        messages.iter().collect();
                    self.delete_messages(client, &refs).await;
                }
            }
            Ok(InvocationResult::Error {
                error_type,
                error_message,
            }) => {
                warn!(
                    function = %self.config.function_name,
                    error_type = %error_type,
                    error_message = %error_message,
                    message_count = messages.len(),
                    "function invocation failed, messages remain on queue"
                );
                // Messages remain on queue -- SQS visibility timeout
                // will make them available again.
            }
            Ok(InvocationResult::Timeout) => {
                warn!(
                    function = %self.config.function_name,
                    message_count = messages.len(),
                    "function invocation timed out, messages remain on queue"
                );
            }
            Err(e) => {
                warn!(
                    function = %self.config.function_name,
                    error = %e,
                    message_count = messages.len(),
                    "failed to invoke function, messages remain on queue"
                );
            }
        }
    }

    /// When ReportBatchItemFailures is enabled, parse the response and only
    /// delete messages that are NOT in the failure list.
    async fn handle_batch_item_failures(
        &self,
        client: &SqsClient,
        messages: &[aws_sdk_sqs::types::Message],
        response_body: &str,
    ) {
        let failed_ids: std::collections::HashSet<String> =
            match serde_json::from_str::<BatchItemFailuresResponse>(
                response_body,
            ) {
                Ok(resp) => resp
                    .batch_item_failures
                    .into_iter()
                    .map(|f| f.item_identifier)
                    .collect(),
                Err(e) => {
                    // Match AWS Lambda behavior: if the response can't be parsed,
                    // treat the entire batch as failed. Messages remain on the
                    // queue and will be retried after the visibility timeout.
                    warn!(
                        error = %e,
                        message_count = messages.len(),
                        "failed to parse batchItemFailures response, treating entire batch as failed"
                    );
                    return;
                }
            };

        if failed_ids.is_empty() {
            // No failures reported -- delete all
            let refs: Vec<&aws_sdk_sqs::types::Message> =
                messages.iter().collect();
            self.delete_messages(client, &refs).await;
            return;
        }

        // Delete only messages NOT in the failure set
        let successful: Vec<&aws_sdk_sqs::types::Message> = messages
            .iter()
            .filter(|msg| {
                msg.message_id()
                    .map(|id| !failed_ids.contains(id))
                    .unwrap_or(false)
            })
            .collect();

        debug!(
            total = messages.len(),
            failed = failed_ids.len(),
            deleting = successful.len(),
            "partial batch failure: deleting successful messages only"
        );

        self.delete_messages(client, &successful).await;
    }
}

#[async_trait::async_trait]
impl EventSourcePoller for SqsPoller {
    fn description(&self) -> String {
        format!(
            "SQS({} -> {})",
            self.config.queue_url, self.config.function_name
        )
    }

    async fn run(
        &self,
        state: AppState,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        // Validate that the target function exists
        if !state
            .functions
            .functions
            .contains_key(&self.config.function_name)
        {
            error!(
                function = %self.config.function_name,
                queue_url = %self.config.queue_url,
                "SQS poller disabled: target function does not exist in config"
            );
            return;
        }

        let client = self.build_client().await;
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);

        info!(
            queue_url = %self.config.queue_url,
            function = %self.config.function_name,
            batch_size = self.config.batch_size,
            "SQS poller started"
        );

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            match self
                .receive_batch(&client, &mut shutdown_rx)
                .await
            {
                Ok(messages) if messages.is_empty() => {
                    // Reset backoff on successful (empty) poll
                    backoff = Duration::from_secs(1);
                }
                Ok(messages) => {
                    backoff = Duration::from_secs(1);
                    self.process_batch(&client, &state, messages).await;
                }
                Err(e) => {
                    warn!(
                        queue_url = %self.config.queue_url,
                        error = %e,
                        backoff_secs = backoff.as_secs(),
                        "SQS receive error, backing off"
                    );

                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown_rx.changed() => {
                            break;
                        }
                    }

                    // Exponential backoff capped at max_backoff
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }

        info!(
            queue_url = %self.config.queue_url,
            function = %self.config.function_name,
            "SQS poller stopped"
        );
    }
}
