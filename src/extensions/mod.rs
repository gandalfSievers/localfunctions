use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Mutex};
use tracing::{debug, info};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Extension lifecycle event types
// ---------------------------------------------------------------------------

/// Events that extensions can register to receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExtensionEventType {
    Invoke,
    Shutdown,
}

/// A lifecycle event delivered to an extension via the /event/next endpoint.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "eventType")]
pub enum ExtensionEvent {
    /// Delivered before each function invocation.
    #[serde(rename = "INVOKE")]
    Invoke {
        #[serde(rename = "deadlineMs")]
        deadline_ms: u64,
        #[serde(rename = "requestId")]
        request_id: String,
        #[serde(rename = "invokedFunctionArn")]
        invoked_function_arn: String,
        tracing: TracingContext,
    },
    /// Delivered when the execution environment is shutting down.
    #[serde(rename = "SHUTDOWN")]
    Shutdown {
        #[serde(rename = "shutdownReason")]
        shutdown_reason: String,
        #[serde(rename = "deadlineMs")]
        deadline_ms: u64,
    },
}

/// X-Ray tracing context included in INVOKE events.
#[derive(Debug, Clone, Serialize)]
pub struct TracingContext {
    #[serde(rename = "type")]
    pub tracing_type: String,
    pub value: String,
}

// ---------------------------------------------------------------------------
// Registered extension
// ---------------------------------------------------------------------------

/// A registered extension within a container's execution environment.
struct RegisteredExtension {
    /// Human-readable name of the extension.
    name: String,
    /// Event types the extension registered for.
    events: Vec<ExtensionEventType>,
    /// Channel to deliver events to the extension's /event/next long-poll.
    event_tx: watch::Sender<Option<ExtensionEvent>>,
}

// ---------------------------------------------------------------------------
// ExtensionRegistry
// ---------------------------------------------------------------------------

/// Tracks all registered extensions across all containers.
///
/// Extensions register via `POST /2020-01-01/extension/register` and then
/// long-poll for events via `GET /2020-01-01/extension/event/next`.
pub struct ExtensionRegistry {
    /// Maps extension identifier → registered extension.
    extensions: Mutex<HashMap<Uuid, RegisteredExtension>>,
    /// Maps function_name → list of extension identifiers registered for that function.
    function_extensions: Mutex<HashMap<String, Vec<Uuid>>>,
    /// Shutdown signal receiver.
    shutdown_rx: watch::Receiver<bool>,
}

impl ExtensionRegistry {
    /// Create a new, empty extension registry.
    pub fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            extensions: Mutex::new(HashMap::new()),
            function_extensions: Mutex::new(HashMap::new()),
            shutdown_rx,
        }
    }

    /// Register a new extension for a function.
    ///
    /// Returns the extension identifier (UUID) that the extension must use in
    /// subsequent `/event/next` calls via the `Lambda-Extension-Identifier`
    /// header.
    pub async fn register(
        &self,
        extension_name: &str,
        function_name: &str,
        events: Vec<ExtensionEventType>,
    ) -> Uuid {
        let identifier = Uuid::new_v4();
        let (event_tx, _event_rx) = watch::channel(None);

        let ext = RegisteredExtension {
            name: extension_name.to_string(),
            events,
            event_tx,
        };

        info!(
            extension_name = %extension_name,
            extension_id = %identifier,
            function = %function_name,
            "extension registered"
        );

        self.extensions.lock().await.insert(identifier, ext);
        self.function_extensions
            .lock()
            .await
            .entry(function_name.to_string())
            .or_default()
            .push(identifier);

        identifier
    }

    /// Wait for the next lifecycle event for an extension.
    ///
    /// This long-polls until an event is available or shutdown is triggered.
    /// Returns `None` on shutdown or if the extension is not registered.
    pub async fn next_event(&self, extension_id: Uuid) -> Option<ExtensionEvent> {
        // Get a receiver for this extension's event channel.
        let mut event_rx = {
            let exts = self.extensions.lock().await;
            let ext = exts.get(&extension_id)?;
            ext.event_tx.subscribe()
        };

        let mut shutdown_rx = self.shutdown_rx.clone();

        // If already shutting down, return a shutdown event immediately.
        if *shutdown_rx.borrow() {
            return Some(ExtensionEvent::Shutdown {
                shutdown_reason: "spindown".to_string(),
                deadline_ms: deadline_ms_from_now(2000),
            });
        }

        // Check if an event was already sent before we subscribed.
        // `subscribe()` marks the current value as "seen", so `changed()`
        // won't fire for it. We check the current value explicitly.
        {
            let current = event_rx.borrow_and_update().clone();
            if let Some(event) = current {
                return Some(event);
            }
        }

        // Long-poll: wait for either an event or shutdown.
        loop {
            tokio::select! {
                result = event_rx.changed() => {
                    match result {
                        Ok(()) => {
                            let event = event_rx.borrow_and_update().clone();
                            if let Some(event) = event {
                                return Some(event);
                            }
                            // None means no event yet, keep waiting.
                        }
                        Err(_) => {
                            // Sender dropped — extension was deregistered.
                            return None;
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    return Some(ExtensionEvent::Shutdown {
                        shutdown_reason: "spindown".to_string(),
                        deadline_ms: deadline_ms_from_now(2000),
                    });
                }
            }
        }
    }

    /// Deliver an INVOKE event to all extensions registered for a function
    /// that have subscribed to INVOKE events.
    pub async fn notify_invoke(
        &self,
        function_name: &str,
        request_id: &str,
        invoked_function_arn: &str,
        deadline_ms: u64,
        trace_id: &str,
    ) {
        let func_exts = self.function_extensions.lock().await;
        let ext_ids = match func_exts.get(function_name) {
            Some(ids) => ids.clone(),
            None => return,
        };
        drop(func_exts);

        let exts = self.extensions.lock().await;
        for ext_id in &ext_ids {
            if let Some(ext) = exts.get(ext_id) {
                if ext.events.contains(&ExtensionEventType::Invoke) {
                    let event = ExtensionEvent::Invoke {
                        deadline_ms,
                        request_id: request_id.to_string(),
                        invoked_function_arn: invoked_function_arn.to_string(),
                        tracing: TracingContext {
                            tracing_type: "X-Amzn-Trace-Id".to_string(),
                            value: trace_id.to_string(),
                        },
                    };
                    ext.event_tx.send_replace(Some(event));
                    debug!(
                        extension = %ext.name,
                        extension_id = %ext_id,
                        request_id = %request_id,
                        "delivered INVOKE event to extension"
                    );
                }
            }
        }
    }

    /// Deliver a SHUTDOWN event to all extensions registered for a function
    /// that have subscribed to SHUTDOWN events.
    #[allow(dead_code)]
    pub async fn notify_shutdown(&self, function_name: &str) {
        let func_exts = self.function_extensions.lock().await;
        let ext_ids = match func_exts.get(function_name) {
            Some(ids) => ids.clone(),
            None => return,
        };
        drop(func_exts);

        let exts = self.extensions.lock().await;
        let deadline_ms = deadline_ms_from_now(2000);
        for ext_id in &ext_ids {
            if let Some(ext) = exts.get(ext_id) {
                if ext.events.contains(&ExtensionEventType::Shutdown) {
                    let event = ExtensionEvent::Shutdown {
                        shutdown_reason: "spindown".to_string(),
                        deadline_ms,
                    };
                    ext.event_tx.send_replace(Some(event));
                    debug!(
                        extension = %ext.name,
                        extension_id = %ext_id,
                        "delivered SHUTDOWN event to extension"
                    );
                }
            }
        }
    }

    /// Deregister all extensions for a function.
    ///
    /// Called during container cleanup to free resources.
    #[allow(dead_code)]
    pub async fn deregister_function(&self, function_name: &str) {
        let mut func_exts = self.function_extensions.lock().await;
        if let Some(ext_ids) = func_exts.remove(function_name) {
            let mut exts = self.extensions.lock().await;
            for ext_id in ext_ids {
                if let Some(ext) = exts.remove(&ext_id) {
                    debug!(
                        extension = %ext.name,
                        extension_id = %ext_id,
                        "extension deregistered"
                    );
                }
            }
        }
    }

    /// Check whether an extension identifier is registered.
    pub async fn is_registered(&self, extension_id: Uuid) -> bool {
        self.extensions.lock().await.contains_key(&extension_id)
    }

    /// Get the function name associated with an extension identifier.
    #[allow(dead_code)]
    pub async fn get_function_name(&self, extension_id: Uuid) -> Option<String> {
        let func_exts = self.function_extensions.lock().await;
        for (function_name, ext_ids) in func_exts.iter() {
            if ext_ids.contains(&extension_id) {
                return Some(function_name.clone());
            }
        }
        None
    }

    /// Get the count of registered extensions for a function.
    #[allow(dead_code)]
    pub async fn extension_count(&self, function_name: &str) -> usize {
        self.function_extensions
            .lock()
            .await
            .get(function_name)
            .map(|ids| ids.len())
            .unwrap_or(0)
    }
}

/// Compute an epoch millisecond deadline from now + given milliseconds.
fn deadline_ms_from_now(ms: u64) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    now + ms
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::watch;

    fn shutdown_channel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
        watch::channel(false)
    }

    #[tokio::test]
    async fn register_returns_unique_identifiers() {
        let (_tx, rx) = shutdown_channel();
        let registry = ExtensionRegistry::new(rx);

        let id1 = registry
            .register("ext1", "my-func", vec![ExtensionEventType::Invoke])
            .await;
        let id2 = registry
            .register("ext2", "my-func", vec![ExtensionEventType::Invoke])
            .await;

        assert_ne!(id1, id2);
        assert!(registry.is_registered(id1).await);
        assert!(registry.is_registered(id2).await);
    }

    #[tokio::test]
    async fn unregistered_extension_is_not_found() {
        let (_tx, rx) = shutdown_channel();
        let registry = ExtensionRegistry::new(rx);

        assert!(!registry.is_registered(Uuid::new_v4()).await);
    }

    #[tokio::test]
    async fn extension_count_tracks_registrations() {
        let (_tx, rx) = shutdown_channel();
        let registry = ExtensionRegistry::new(rx);

        assert_eq!(registry.extension_count("my-func").await, 0);

        registry
            .register("ext1", "my-func", vec![ExtensionEventType::Invoke])
            .await;
        assert_eq!(registry.extension_count("my-func").await, 1);

        registry
            .register("ext2", "my-func", vec![ExtensionEventType::Shutdown])
            .await;
        assert_eq!(registry.extension_count("my-func").await, 2);

        // Different function
        assert_eq!(registry.extension_count("other-func").await, 0);
    }

    #[tokio::test]
    async fn deregister_function_removes_all_extensions() {
        let (_tx, rx) = shutdown_channel();
        let registry = ExtensionRegistry::new(rx);

        let id1 = registry
            .register("ext1", "my-func", vec![ExtensionEventType::Invoke])
            .await;
        let id2 = registry
            .register("ext2", "my-func", vec![ExtensionEventType::Shutdown])
            .await;
        let id3 = registry
            .register("ext3", "other-func", vec![ExtensionEventType::Invoke])
            .await;

        registry.deregister_function("my-func").await;

        assert!(!registry.is_registered(id1).await);
        assert!(!registry.is_registered(id2).await);
        // other-func extensions should be unaffected
        assert!(registry.is_registered(id3).await);
        assert_eq!(registry.extension_count("my-func").await, 0);
        assert_eq!(registry.extension_count("other-func").await, 1);
    }

    #[tokio::test]
    async fn get_function_name_returns_correct_function() {
        let (_tx, rx) = shutdown_channel();
        let registry = ExtensionRegistry::new(rx);

        let id = registry
            .register("ext1", "my-func", vec![ExtensionEventType::Invoke])
            .await;

        assert_eq!(
            registry.get_function_name(id).await,
            Some("my-func".to_string())
        );
        assert_eq!(registry.get_function_name(Uuid::new_v4()).await, None);
    }

    #[tokio::test]
    async fn notify_invoke_delivers_event() {
        let (_tx, rx) = shutdown_channel();
        let registry = Arc::new(ExtensionRegistry::new(rx));

        let ext_id = registry
            .register(
                "ext1",
                "my-func",
                vec![ExtensionEventType::Invoke],
            )
            .await;

        // Spawn a listener
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            registry_clone.next_event(ext_id).await
        });

        // Give the long-poll a moment to start
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Deliver INVOKE event
        registry
            .notify_invoke(
                "my-func",
                "req-123",
                "arn:aws:lambda:us-east-1:000:function:my-func",
                1234567890,
                "trace-id-abc",
            )
            .await;

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(event.is_some());
        match event.unwrap() {
            ExtensionEvent::Invoke {
                request_id,
                deadline_ms,
                ..
            } => {
                assert_eq!(request_id, "req-123");
                assert_eq!(deadline_ms, 1234567890);
            }
            _ => panic!("expected INVOKE event"),
        }
    }

    #[tokio::test]
    async fn notify_invoke_skips_extensions_not_subscribed() {
        let (shutdown_tx, rx) = shutdown_channel();
        let registry = Arc::new(ExtensionRegistry::new(rx));

        // Register extension only for SHUTDOWN events
        let ext_id = registry
            .register(
                "ext1",
                "my-func",
                vec![ExtensionEventType::Shutdown],
            )
            .await;

        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            registry_clone.next_event(ext_id).await
        });

        tokio::task::yield_now().await;

        // Deliver INVOKE event — should NOT reach this extension
        registry
            .notify_invoke("my-func", "req-123", "arn", 1234567890, "trace")
            .await;

        // The extension should not receive anything — trigger shutdown to unblock
        shutdown_tx.send(true).unwrap();

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle,
        )
        .await
        .unwrap()
        .unwrap();

        // Should get SHUTDOWN, not INVOKE
        assert!(matches!(event, Some(ExtensionEvent::Shutdown { .. })));
    }

    #[tokio::test]
    async fn next_event_returns_shutdown_on_signal() {
        let (shutdown_tx, rx) = shutdown_channel();
        let registry = Arc::new(ExtensionRegistry::new(rx));

        let ext_id = registry
            .register(
                "ext1",
                "my-func",
                vec![ExtensionEventType::Invoke, ExtensionEventType::Shutdown],
            )
            .await;

        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            registry_clone.next_event(ext_id).await
        });

        tokio::task::yield_now().await;
        shutdown_tx.send(true).unwrap();

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(matches!(event, Some(ExtensionEvent::Shutdown { .. })));
    }

    #[tokio::test]
    async fn next_event_returns_none_for_unknown_extension() {
        let (_tx, rx) = shutdown_channel();
        let registry = ExtensionRegistry::new(rx);

        let result = registry.next_event(Uuid::new_v4()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn next_event_returns_shutdown_when_already_shutting_down() {
        let (shutdown_tx, rx) = shutdown_channel();
        shutdown_tx.send(true).unwrap();

        let registry = ExtensionRegistry::new(rx);
        let ext_id = registry
            .register("ext1", "my-func", vec![ExtensionEventType::Invoke])
            .await;

        let event = registry.next_event(ext_id).await;
        assert!(matches!(event, Some(ExtensionEvent::Shutdown { .. })));
    }

    #[tokio::test]
    async fn notify_shutdown_delivers_event() {
        let (_tx, rx) = shutdown_channel();
        let registry = Arc::new(ExtensionRegistry::new(rx));

        let ext_id = registry
            .register(
                "ext1",
                "my-func",
                vec![ExtensionEventType::Shutdown],
            )
            .await;

        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            registry_clone.next_event(ext_id).await
        });

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        registry.notify_shutdown("my-func").await;

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(event.is_some());
        match event.unwrap() {
            ExtensionEvent::Shutdown {
                shutdown_reason, ..
            } => {
                assert_eq!(shutdown_reason, "spindown");
            }
            _ => panic!("expected SHUTDOWN event"),
        }
    }

    #[tokio::test]
    async fn extension_event_type_serde() {
        let invoke_json = serde_json::to_string(&ExtensionEventType::Invoke).unwrap();
        assert_eq!(invoke_json, "\"INVOKE\"");

        let shutdown_json = serde_json::to_string(&ExtensionEventType::Shutdown).unwrap();
        assert_eq!(shutdown_json, "\"SHUTDOWN\"");

        let deserialized: ExtensionEventType =
            serde_json::from_str("\"INVOKE\"").unwrap();
        assert_eq!(deserialized, ExtensionEventType::Invoke);

        let deserialized: ExtensionEventType =
            serde_json::from_str("\"SHUTDOWN\"").unwrap();
        assert_eq!(deserialized, ExtensionEventType::Shutdown);
    }

    #[tokio::test]
    async fn extension_event_invoke_serialization() {
        let event = ExtensionEvent::Invoke {
            deadline_ms: 1234567890,
            request_id: "req-123".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:000:function:my-func"
                .to_string(),
            tracing: TracingContext {
                tracing_type: "X-Amzn-Trace-Id".to_string(),
                value: "Root=1-abc".to_string(),
            },
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["eventType"], "INVOKE");
        assert_eq!(json["deadlineMs"], 1234567890);
        assert_eq!(json["requestId"], "req-123");
        assert!(json["invokedFunctionArn"]
            .as_str()
            .unwrap()
            .contains("my-func"));
        assert_eq!(json["tracing"]["type"], "X-Amzn-Trace-Id");
    }

    #[tokio::test]
    async fn extension_event_shutdown_serialization() {
        let event = ExtensionEvent::Shutdown {
            shutdown_reason: "spindown".to_string(),
            deadline_ms: 9999,
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["eventType"], "SHUTDOWN");
        assert_eq!(json["shutdownReason"], "spindown");
        assert_eq!(json["deadlineMs"], 9999);
    }

    #[tokio::test]
    async fn deadline_ms_from_now_is_in_future() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let deadline = deadline_ms_from_now(5000);
        assert!(deadline >= now_ms + 4000); // Allow small timing margin
        assert!(deadline <= now_ms + 6000);
    }
}
