//! SNS subscription lifecycle management.
//!
//! Parses `sns_subscriptions` from the functions config, creates HTTP
//! subscriptions on startup via the AWS SNS SDK, and unsubscribes on shutdown.

#[cfg(test)]
mod test;

use aws_sdk_sns::Client as SnsClient;
use serde::Deserialize;
use serde_json::Value;
use tracing::{error, info, warn};

use super::SnsSubscriptionHandle;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Typed configuration for an SNS subscription, parsed from the
/// `sns_subscriptions` array in functions.json.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SnsSubscriptionConfig {
    /// The SNS topic ARN to subscribe to.
    pub topic_arn: String,
    /// The function to invoke when messages are received.
    pub function_name: String,
    /// Optional filter policy JSON (passed as subscription attribute).
    #[serde(default)]
    pub filter_policy: Option<Value>,
    /// Optional filter policy scope ("MessageAttributes" or "MessageBody").
    /// Defaults to "MessageAttributes" (matching AWS default).
    #[serde(default)]
    pub filter_policy_scope: Option<String>,
    /// Override the SNS endpoint URL (e.g. for LocalStack).
    #[serde(default)]
    pub endpoint_url: Option<String>,
    /// AWS region override for the SNS client.
    #[serde(default)]
    pub region: Option<String>,
    /// Whether this subscription is enabled (default true).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

/// Parse SNS subscription configs from the raw JSON values in FunctionsConfig.
///
/// Returns all parsed configs including disabled ones. Callers filter out
/// disabled entries before subscribing.
pub fn parse_sns_subscriptions(raw: &[Value]) -> Vec<SnsSubscriptionConfig> {
    let mut configs = Vec::new();
    for value in raw {
        match serde_json::from_value::<SnsSubscriptionConfig>(value.clone()) {
            Ok(config) => {
                configs.push(config);
            }
            Err(e) => {
                warn!(
                    error = %e,
                    subscription = %value,
                    "failed to parse SNS subscription config, skipping"
                );
            }
        }
    }
    configs
}

// ---------------------------------------------------------------------------
// Subscribe / Unsubscribe
// ---------------------------------------------------------------------------

/// Build an SNS client with optional endpoint override.
///
/// Uses the default AWS credential chain so that callers can supply
/// dummy credentials for SigV4 signing when using DNS-overridden
/// AWS endpoints (e.g. routed to LocalStack via Traefik).
async fn build_sns_client(
    region: &str,
    endpoint_url: Option<&str>,
) -> SnsClient {
    let region = aws_sdk_sns::config::Region::new(region.to_string());

    let mut config_builder =
        aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region.clone());

    if let Some(endpoint) = endpoint_url {
        config_builder = config_builder.endpoint_url(endpoint);
    }

    let sdk_config = config_builder.load().await;
    SnsClient::new(&sdk_config)
}

/// Subscribe to an SNS topic with HTTP protocol, pointing to the local
/// `/sns/{function_name}` endpoint.
///
/// Returns the subscription ARN on success, or `None` if the subscription
/// could not be created (logged and skipped — does not block startup).
pub async fn subscribe(
    config: &SnsSubscriptionConfig,
    callback_url: &str,
    default_region: &str,
) -> Option<SnsSubscriptionHandle> {
    let region = config
        .region
        .as_deref()
        .unwrap_or(default_region);

    let endpoint = crate::api::sns::sns_endpoint_url(
        callback_url,
        &config.function_name,
    );

    let client = build_sns_client(region, config.endpoint_url.as_deref()).await;

    let mut req = client
        .subscribe()
        .topic_arn(&config.topic_arn)
        .protocol("http")
        .endpoint(&endpoint);

    // Attach filter policy as a subscription attribute if present.
    if let Some(ref filter_policy) = config.filter_policy {
        let policy_json = match serde_json::to_string(filter_policy) {
            Ok(s) => s,
            Err(e) => {
                error!(
                    topic_arn = %config.topic_arn,
                    function = %config.function_name,
                    error = %e,
                    "SNS: failed to serialize filter policy, skipping subscription"
                );
                return None;
            }
        };

        req = req.attributes("FilterPolicy", policy_json);

        if let Some(ref scope) = config.filter_policy_scope {
            req = req.attributes("FilterPolicyScope", scope.clone());
        }
    }

    info!(
        topic_arn = %config.topic_arn,
        function = %config.function_name,
        endpoint = %endpoint,
        "SNS: creating subscription"
    );

    match req.send().await {
        Ok(output) => {
            let subscription_arn = output
                .subscription_arn()
                .unwrap_or("pending confirmation")
                .to_string();

            info!(
                topic_arn = %config.topic_arn,
                function = %config.function_name,
                subscription_arn = %subscription_arn,
                "SNS: subscription created"
            );

            Some(SnsSubscriptionHandle {
                function_name: config.function_name.clone(),
                topic_arn: config.topic_arn.clone(),
                subscription_arn: Some(subscription_arn),
                sns_endpoint_override: config.endpoint_url.clone(),
                region: region.to_string(),
            })
        }
        Err(e) => {
            error!(
                topic_arn = %config.topic_arn,
                function = %config.function_name,
                error = %e,
                "SNS: failed to subscribe — skipping (startup continues)"
            );
            None
        }
    }
}

/// Unsubscribe from an SNS topic (best-effort). Logs errors but does not fail.
pub async fn unsubscribe(handle: &SnsSubscriptionHandle) {
    let subscription_arn = match &handle.subscription_arn {
        Some(arn) if arn != "pending confirmation" && arn != "PendingConfirmation" => arn,
        _ => {
            info!(
                function = %handle.function_name,
                topic = %handle.topic_arn,
                "SNS: skipping unsubscribe (no confirmed subscription ARN)"
            );
            return;
        }
    };

    let client = build_sns_client(
        &handle.region,
        handle.sns_endpoint_override.as_deref(),
    )
    .await;

    info!(
        function = %handle.function_name,
        topic = %handle.topic_arn,
        subscription_arn = %subscription_arn,
        "SNS: unsubscribing"
    );

    match client
        .unsubscribe()
        .subscription_arn(subscription_arn)
        .send()
        .await
    {
        Ok(_) => {
            info!(
                function = %handle.function_name,
                subscription_arn = %subscription_arn,
                "SNS: unsubscribed successfully"
            );
        }
        Err(e) => {
            warn!(
                function = %handle.function_name,
                subscription_arn = %subscription_arn,
                error = %e,
                "SNS: best-effort unsubscribe failed"
            );
        }
    }
}
