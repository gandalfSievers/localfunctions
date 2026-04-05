use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch, Mutex};
use tracing::info;

use crate::types::Invocation;

/// Bridges the Invoke API (which sends invocations) and the Runtime API (where
/// containers long-poll for work). Holds per-function invocation receivers and
/// tracks which containers have signalled readiness.
pub struct RuntimeBridge {
    /// Per-function invocation receivers. Multiple containers for the same
    /// function share one receiver (behind a Mutex), which naturally
    /// load-balances work.
    queues: HashMap<String, Arc<Mutex<mpsc::Receiver<Invocation>>>>,

    /// Shutdown signal — when `true` is sent, all long-polling handlers exit.
    shutdown_rx: watch::Receiver<bool>,

    /// Track container IDs that have called /next at least once (readiness).
    ready_containers: Mutex<HashMap<String, bool>>,
}

impl RuntimeBridge {
    /// Create a new `RuntimeBridge` from the per-function receivers produced by
    /// [`FunctionManager::new`].
    ///
    /// The `shutdown_rx` watch channel should receive `true` when the service
    /// begins shutting down.
    pub fn new(
        receivers: HashMap<String, mpsc::Receiver<Invocation>>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        let queues = receivers
            .into_iter()
            .map(|(name, rx)| (name, Arc::new(Mutex::new(rx))))
            .collect();

        Self {
            queues,
            shutdown_rx,
            ready_containers: Mutex::new(HashMap::new()),
        }
    }

    /// Long-poll for the next invocation for the given function.
    ///
    /// Returns `None` if the channel is closed (all senders dropped) or
    /// shutdown is triggered.
    pub async fn next_invocation(&self, function_name: &str) -> Option<Invocation> {
        let queue = self.queues.get(function_name)?;

        // Check if already shutting down before acquiring the lock.
        if *self.shutdown_rx.borrow() {
            return None;
        }

        let mut rx = queue.lock().await;
        let mut shutdown_rx = self.shutdown_rx.clone();

        tokio::select! {
            invocation = rx.recv() => invocation,
            _ = shutdown_rx.changed() => None,
        }
    }

    /// Mark a container as ready (first call to /next signals cold start
    /// complete).
    pub async fn mark_ready(&self, container_id: &str) {
        let mut ready = self.ready_containers.lock().await;
        if !ready.contains_key(container_id) {
            info!(container_id = %container_id, "container signalled readiness");
            ready.insert(container_id.to_string(), true);
        }
    }

    /// Check whether a container has signalled readiness.
    #[allow(dead_code)]
    pub async fn is_ready(&self, container_id: &str) -> bool {
        self.ready_containers
            .lock()
            .await
            .contains_key(container_id)
    }

    /// Check whether a function has a registered queue.
    #[allow(dead_code)]
    pub fn has_function(&self, function_name: &str) -> bool {
        self.queues.contains_key(function_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::sync::oneshot;
    use tokio::time::Instant;
    use uuid::Uuid;

    fn make_invocation(function_name: &str) -> (Invocation, oneshot::Receiver<crate::types::InvocationResult>) {
        let (tx, rx) = oneshot::channel();
        let inv = Invocation {
            request_id: Uuid::new_v4(),
            function_name: function_name.to_string(),
            payload: Bytes::from(r#"{"key":"value"}"#),
            deadline: Instant::now() + std::time::Duration::from_secs(30),
            response_tx: tx,
        };
        (inv, rx)
    }

    fn shutdown_channel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
        watch::channel(false)
    }

    #[tokio::test]
    async fn next_invocation_returns_queued_item() {
        let (tx, rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = RuntimeBridge::new(receivers, shutdown_rx);

        let (inv, _rx) = make_invocation("test-func");
        let request_id = inv.request_id;
        tx.send(inv).await.unwrap();

        let result = bridge.next_invocation("test-func").await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().request_id, request_id);
    }

    #[tokio::test]
    async fn next_invocation_unknown_function_returns_none() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), shutdown_rx);

        let result = bridge.next_invocation("nonexistent").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn next_invocation_returns_none_on_shutdown() {
        let (_tx, rx) = mpsc::channel::<Invocation>(10);
        let (shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = Arc::new(RuntimeBridge::new(receivers, shutdown_rx));

        let bridge_clone = bridge.clone();
        let handle = tokio::spawn(async move {
            bridge_clone.next_invocation("test-func").await
        });

        // Give the long-poll a moment to start
        tokio::task::yield_now().await;
        shutdown_tx.send(true).unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn next_invocation_returns_none_when_already_shutdown() {
        let (_tx, rx) = mpsc::channel::<Invocation>(10);
        let (shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        // Trigger shutdown before creating bridge
        shutdown_tx.send(true).unwrap();

        let bridge = RuntimeBridge::new(receivers, shutdown_rx);
        let result = bridge.next_invocation("test-func").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn next_invocation_returns_none_when_senders_dropped() {
        let (tx, rx) = mpsc::channel::<Invocation>(10);
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = RuntimeBridge::new(receivers, shutdown_rx);

        drop(tx);

        let result = bridge.next_invocation("test-func").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn mark_ready_tracks_container() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), shutdown_rx);

        assert!(!bridge.is_ready("container-1").await);
        bridge.mark_ready("container-1").await;
        assert!(bridge.is_ready("container-1").await);
    }

    #[tokio::test]
    async fn mark_ready_idempotent() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), shutdown_rx);

        bridge.mark_ready("container-1").await;
        bridge.mark_ready("container-1").await;
        assert!(bridge.is_ready("container-1").await);
    }

    #[tokio::test]
    async fn has_function_returns_correct_value() {
        let (_tx, rx) = mpsc::channel::<Invocation>(10);
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("my-func".to_string(), rx);

        let bridge = RuntimeBridge::new(receivers, shutdown_rx);

        assert!(bridge.has_function("my-func"));
        assert!(!bridge.has_function("other-func"));
    }

    #[tokio::test]
    async fn long_poll_receives_after_delay() {
        let (tx, rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = Arc::new(RuntimeBridge::new(receivers, shutdown_rx));
        let bridge_clone = bridge.clone();

        let handle = tokio::spawn(async move {
            bridge_clone.next_invocation("test-func").await
        });

        // Send after a brief delay
        tokio::task::yield_now().await;
        let (inv, _rx) = make_invocation("test-func");
        let request_id = inv.request_id;
        tx.send(inv).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().request_id, request_id);
    }
}
