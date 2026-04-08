//! Event source framework for poll-based and push-based event sources.
//!
//! Provides the [`EventSourcePoller`] trait for implementing poll-based event
//! sources (e.g. SQS) and the [`EventSourceManager`] lifecycle coordinator
//! that manages poller tasks and SNS subscription handles with consistent
//! startup and graceful shutdown.

pub mod sqs;

#[cfg(test)]
mod test;
#[cfg(test)]
pub(crate) mod test_helpers;

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use crate::server::AppState;
use crate::types::InvocationResult;

// ---------------------------------------------------------------------------
// EventSourcePoller trait
// ---------------------------------------------------------------------------

/// Trait for poll-based event sources (e.g. SQS, Kinesis).
///
/// Implementors define a `description()` for logging and a `run()` method
/// that polls for events until the shutdown signal fires.
#[async_trait::async_trait]
pub trait EventSourcePoller: Send + Sync {
    /// Human-readable description of this event source (for logging).
    fn description(&self) -> String;

    /// Run the poller loop. Must exit gracefully when `shutdown_rx` receives
    /// `true`. The `state` provides access to container management and the
    /// runtime bridge for invoking functions.
    async fn run(
        &self,
        state: AppState,
        shutdown_rx: watch::Receiver<bool>,
    );
}

// ---------------------------------------------------------------------------
// SnsSubscriptionHandle
// ---------------------------------------------------------------------------

/// Handle representing an active SNS subscription that was auto-created at
/// startup. Holds the information needed to unsubscribe on shutdown.
#[derive(Debug)]
pub struct SnsSubscriptionHandle {
    pub function_name: String,
    pub topic_arn: String,
    pub endpoint_url: String,
}

// ---------------------------------------------------------------------------
// invoke_function_with_event helper
// ---------------------------------------------------------------------------

/// Invoke a function programmatically with an event payload.
///
/// This is the shared helper used by event sources (SQS pollers, SNS handlers,
/// etc.) to submit invocations without going through the HTTP invoke path.
/// It reuses `acquire_container()` and `RuntimeBridge::submit_invocation()`
/// directly.
///
/// Returns `Ok(InvocationResult)` on successful invocation, or an error string.
pub async fn invoke_function_with_event(
    state: &AppState,
    function_name: &str,
    payload: Bytes,
) -> Result<InvocationResult, String> {
    // Verify function exists
    let function_config = state
        .functions
        .functions
        .get(function_name)
        .ok_or_else(|| format!("function '{}' not found", function_name))?;

    let timeout_secs = function_config.timeout;
    let request_id = Uuid::new_v4();
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

    // Acquire a container: try warm first, then cold-start.
    let _container_id = match state
        .container_manager
        .claim_idle_container(function_name)
        .await
    {
        Some(id) => id,
        None => {
            let acquire_timeout = std::time::Duration::from_secs(
                state.config.container_acquire_timeout,
            );
            if !state
                .container_manager
                .acquire_container_slot(acquire_timeout)
                .await
            {
                return Err("max concurrent containers reached".into());
            }

            let cold_id = match state
                .container_manager
                .create_and_start(function_config)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    state.container_manager.release_container_slot();
                    return Err(format!("failed to start container: {}", e));
                }
            };

            state
                .container_manager
                .set_state(
                    &cold_id,
                    crate::types::ContainerState::Busy,
                )
                .await;

            // Wait for bootstrap
            let ready_signal = state
                .runtime_bridge
                .register_ready_signal(&cold_id, Some(function_name))
                .await;

            let init_timeout =
                std::time::Duration::from_secs(state.config.init_timeout);

            tokio::select! {
                _ = ready_signal.notified() => cold_id,
                _ = tokio::time::sleep(init_timeout) => {
                    if state.runtime_bridge.is_ready(&cold_id).await {
                        cold_id
                    } else {
                        return Err(format!(
                            "container bootstrap timed out for '{}'",
                            function_name
                        ));
                    }
                }
            }
        }
    };

    // Submit the invocation via RuntimeBridge
    let response_rx = state
        .runtime_bridge
        .submit_invocation(function_name, request_id, payload, deadline, None, None)
        .await
        .map_err(|e| format!("failed to submit invocation: {}", e))?;

    // Wait for response with timeout
    match tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        response_rx,
    )
    .await
    {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(_)) => Err("invocation response channel closed".into()),
        Err(_) => Ok(InvocationResult::Timeout),
    }
}

// ---------------------------------------------------------------------------
// EventSourceManager
// ---------------------------------------------------------------------------

/// Lifecycle coordinator for event sources.
///
/// Manages poller [`JoinHandle`]s and [`SnsSubscriptionHandle`]s. Provides
/// `start()` to launch all pollers and `shutdown()` to signal graceful
/// termination and await completion.
pub struct EventSourceManager {
    pollers: Vec<Box<dyn EventSourcePoller>>,
    poller_handles: Vec<JoinHandle<()>>,
    sns_handles: Vec<SnsSubscriptionHandle>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl EventSourceManager {
    /// Create a new manager with its own shutdown channel.
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            pollers: Vec::new(),
            poller_handles: Vec::new(),
            sns_handles: Vec::new(),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Register a poller to be started when `start()` is called.
    pub fn add_poller(&mut self, poller: Box<dyn EventSourcePoller>) {
        self.pollers.push(poller);
    }

    /// Register an SNS subscription handle for tracking.
    pub fn add_sns_handle(&mut self, handle: SnsSubscriptionHandle) {
        self.sns_handles.push(handle);
    }

    /// Start all registered pollers as background tasks.
    ///
    /// Each poller runs in its own Tokio task and receives a clone of the
    /// shutdown receiver to coordinate graceful exit.
    pub fn start(&mut self, state: AppState) {
        let pollers = std::mem::take(&mut self.pollers);
        for poller in pollers {
            let poller: Arc<dyn EventSourcePoller> = Arc::from(poller);
            let state = state.clone();
            let shutdown_rx = self.shutdown_rx.clone();
            let desc = poller.description();
            info!(source = %desc, "starting event source poller");
            let handle = tokio::spawn(async move {
                poller.run(state, shutdown_rx).await;
            });
            self.poller_handles.push(handle);
        }

        if !self.sns_handles.is_empty() {
            info!(
                count = self.sns_handles.len(),
                "tracking SNS subscription handles"
            );
        }
    }

    /// Signal all pollers to shut down and wait for them to exit.
    pub async fn shutdown(&mut self) {
        info!("shutting down event source manager");

        // Signal shutdown
        let _ = self.shutdown_tx.send(true);

        // Wait for all pollers to exit
        let handles = std::mem::take(&mut self.poller_handles);
        for handle in handles {
            if let Err(e) = handle.await {
                warn!(error = %e, "event source poller task panicked");
            }
        }

        // Log SNS handle cleanup (actual unsubscribe would require SNS API)
        for handle in &self.sns_handles {
            info!(
                function = %handle.function_name,
                topic = %handle.topic_arn,
                "releasing SNS subscription handle"
            );
        }
        self.sns_handles.clear();

        info!("event source manager shutdown complete");
    }

    /// Returns the number of active poller handles.
    pub fn poller_count(&self) -> usize {
        self.poller_handles.len()
    }

    /// Returns the number of tracked SNS subscription handles.
    pub fn sns_handle_count(&self) -> usize {
        self.sns_handles.len()
    }
}
