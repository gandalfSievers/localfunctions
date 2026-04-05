use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tracing::{info, warn};
use uuid::Uuid;

use bytes::Bytes;

use crate::types::{Invocation, InvocationResult, ServiceError};

/// A dispatched invocation waiting for a response from the container runtime.
struct PendingInvocation {
    function_name: String,
    container_id: Option<String>,
    response_tx: oneshot::Sender<InvocationResult>,
}

/// Bridges the Invoke API (which sends invocations) and the Runtime API (where
/// containers long-poll for work). Holds per-function invocation receivers and
/// tracks which containers have signalled readiness.
pub struct RuntimeBridge {
    /// Per-function invocation senders. Used by the Invoke API to submit new
    /// invocations.
    senders: HashMap<String, mpsc::Sender<Invocation>>,

    /// Per-function invocation receivers. Multiple containers for the same
    /// function share one receiver (behind a Mutex), which naturally
    /// load-balances work.
    queues: HashMap<String, Arc<Mutex<mpsc::Receiver<Invocation>>>>,

    /// Shutdown signal — when `true` is sent, all long-polling handlers exit.
    shutdown_rx: watch::Receiver<bool>,

    /// Track container IDs that have called /next at least once (readiness).
    ready_containers: Mutex<HashMap<String, bool>>,

    /// Pending invocations awaiting a response from the container runtime.
    /// Maps request_id → pending invocation (function name + oneshot sender).
    pending_invocations: Mutex<HashMap<Uuid, PendingInvocation>>,
}

impl RuntimeBridge {
    /// Create a new `RuntimeBridge` from the per-function receivers produced by
    /// [`FunctionManager::new`].
    ///
    /// The `shutdown_rx` watch channel should receive `true` when the service
    /// begins shutting down.
    pub fn new(
        senders: HashMap<String, mpsc::Sender<Invocation>>,
        receivers: HashMap<String, mpsc::Receiver<Invocation>>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        let queues = receivers
            .into_iter()
            .map(|(name, rx)| (name, Arc::new(Mutex::new(rx))))
            .collect();

        Self {
            senders,
            queues,
            shutdown_rx,
            ready_containers: Mutex::new(HashMap::new()),
            pending_invocations: Mutex::new(HashMap::new()),
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

    /// Submit an invocation for a function. Creates the invocation, sends it
    /// to the function's queue, and returns the request ID and a receiver for
    /// the result.
    ///
    /// Returns `ServiceError::ResourceNotFound` if the function has no sender.
    pub async fn submit_invocation(
        &self,
        function_name: &str,
        request_id: Uuid,
        payload: Bytes,
        deadline: tokio::time::Instant,
        trace_id: Option<String>,
        client_context: Option<String>,
    ) -> Result<oneshot::Receiver<InvocationResult>, ServiceError> {
        let sender = self
            .senders
            .get(function_name)
            .ok_or_else(|| ServiceError::ResourceNotFound(function_name.to_string()))?;

        let (response_tx, response_rx) = oneshot::channel();

        let invocation = Invocation {
            request_id,
            function_name: function_name.to_string(),
            payload,
            deadline,
            trace_id,
            client_context,
            response_tx,
        };

        sender.send(invocation).await.map_err(|_| {
            ServiceError::ServiceException(format!(
                "Failed to enqueue invocation for function '{}'",
                function_name
            ))
        })?;

        Ok(response_rx)
    }

    /// Store the response channel for a dispatched invocation so that the
    /// `/response` and `/error` endpoints can forward results back to the
    /// original caller.
    pub async fn store_pending(
        &self,
        request_id: Uuid,
        function_name: String,
        container_id: Option<String>,
        response_tx: oneshot::Sender<InvocationResult>,
    ) {
        self.pending_invocations
            .lock()
            .await
            .insert(request_id, PendingInvocation { function_name, container_id, response_tx });
    }

    /// Complete a pending invocation with a success result.
    ///
    /// Returns `(true, container_id)` if the invocation was found and the
    /// result was sent, `(false, None)` if the request_id was not found or the
    /// receiver was dropped.
    pub async fn complete_invocation(&self, request_id: Uuid, body: String) -> (bool, Option<String>) {
        let pending = self.pending_invocations.lock().await.remove(&request_id);
        match pending {
            Some(p) => {
                let container_id = p.container_id;
                if p.response_tx.send(InvocationResult::Success { body }).is_err() {
                    warn!(%request_id, "invocation caller already dropped");
                    (false, container_id)
                } else {
                    (true, container_id)
                }
            }
            None => (false, None),
        }
    }

    /// Complete a pending invocation with an error result.
    ///
    /// Returns `(true, container_id)` if the invocation was found and the
    /// result was sent, `(false, None)` if the request_id was not found or the
    /// receiver was dropped.
    pub async fn fail_invocation(
        &self,
        request_id: Uuid,
        error_type: String,
        error_message: String,
    ) -> (bool, Option<String>) {
        let pending = self.pending_invocations.lock().await.remove(&request_id);
        match pending {
            Some(p) => {
                let container_id = p.container_id;
                if p.response_tx
                    .send(InvocationResult::Error {
                        error_type,
                        error_message,
                    })
                    .is_err()
                {
                    warn!(%request_id, "invocation caller already dropped");
                    (false, container_id)
                } else {
                    (true, container_id)
                }
            }
            None => (false, None),
        }
    }

    /// Time out a pending invocation. Removes it from tracking and returns the
    /// container_id that was handling it (if known), so the caller can kill the
    /// container.
    pub async fn timeout_invocation(&self, request_id: Uuid) -> Option<String> {
        let pending = self.pending_invocations.lock().await.remove(&request_id);
        match pending {
            Some(p) => {
                let _ = p.response_tx.send(InvocationResult::Timeout);
                p.container_id
            }
            None => None,
        }
    }

    /// Fail all pending invocations for a given function with an init error.
    /// Used when the runtime fails to initialize.
    ///
    /// This drains both:
    /// - Queued invocations (not yet dispatched to a container via /next)
    /// - Dispatched invocations (already sent to a container via /next, sitting
    ///   in pending_invocations)
    ///
    /// Returns the number of invocations that were failed.
    pub async fn fail_init(
        &self,
        function_name: &str,
    ) -> usize {
        let init_error = InvocationResult::Error {
            error_type: "InvalidRuntimeException".into(),
            error_message: "Runtime failed to initialize".into(),
        };

        let mut count = 0;

        // 1. Drain queued invocations (not yet dispatched via /next).
        if let Some(queue) = self.queues.get(function_name) {
            let mut rx = queue.lock().await;
            while let Ok(inv) = rx.try_recv() {
                let _ = inv.response_tx.send(init_error.clone());
                count += 1;
            }
        }

        // 2. Drain dispatched invocations (already sent via /next, awaiting
        //    response in pending_invocations).
        let mut pending = self.pending_invocations.lock().await;
        let matching_ids: Vec<Uuid> = pending
            .iter()
            .filter(|(_, p)| p.function_name == function_name)
            .map(|(id, _)| *id)
            .collect();

        for id in matching_ids {
            if let Some(p) = pending.remove(&id) {
                let _ = p.response_tx.send(init_error.clone());
                count += 1;
            }
        }

        count
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
            trace_id: None,
            client_context: None,
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

        let bridge = RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx);

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
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let result = bridge.next_invocation("nonexistent").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn next_invocation_returns_none_on_shutdown() {
        let (_tx, rx) = mpsc::channel::<Invocation>(10);
        let (shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));

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

        let bridge = RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx);
        let result = bridge.next_invocation("test-func").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn next_invocation_returns_none_when_senders_dropped() {
        let (tx, rx) = mpsc::channel::<Invocation>(10);
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx);

        drop(tx);

        let result = bridge.next_invocation("test-func").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn mark_ready_tracks_container() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        assert!(!bridge.is_ready("container-1").await);
        bridge.mark_ready("container-1").await;
        assert!(bridge.is_ready("container-1").await);
    }

    #[tokio::test]
    async fn mark_ready_idempotent() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

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

        let bridge = RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx);

        assert!(bridge.has_function("my-func"));
        assert!(!bridge.has_function("other-func"));
    }

    #[tokio::test]
    async fn store_and_complete_invocation() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let (tx, rx) = oneshot::channel();
        let request_id = Uuid::new_v4();
        bridge.store_pending(request_id, "test-func".into(), None, tx).await;

        let (success, _) = bridge.complete_invocation(request_id, "done".into()).await;
        assert!(success);
        assert_eq!(
            rx.await.unwrap(),
            InvocationResult::Success {
                body: "done".into()
            }
        );
    }

    #[tokio::test]
    async fn complete_unknown_request_id_returns_false() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let (success, _) = bridge.complete_invocation(Uuid::new_v4(), "x".into()).await;
        assert!(!success);
    }

    #[tokio::test]
    async fn store_and_fail_invocation() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let (tx, rx) = oneshot::channel();
        let request_id = Uuid::new_v4();
        bridge.store_pending(request_id, "test-func".into(), None, tx).await;

        let (success, _) = bridge
            .fail_invocation(request_id, "RuntimeError".into(), "boom".into())
            .await;
        assert!(success);
        assert_eq!(
            rx.await.unwrap(),
            InvocationResult::Error {
                error_type: "RuntimeError".into(),
                error_message: "boom".into(),
            }
        );
    }

    #[tokio::test]
    async fn fail_unknown_request_id_returns_false() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let (success, _) = bridge
            .fail_invocation(Uuid::new_v4(), "X".into(), "Y".into())
            .await;
        assert!(!success);
    }

    #[tokio::test]
    async fn fail_init_drains_queued_invocations() {
        let (tx, rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx);

        // Queue two invocations
        let (inv1, rx1) = make_invocation("test-func");
        let (inv2, rx2) = make_invocation("test-func");
        tx.send(inv1).await.unwrap();
        tx.send(inv2).await.unwrap();

        let count = bridge.fail_init("test-func").await;
        assert_eq!(count, 2);

        // Both callers should receive init errors
        let r1 = rx1.await.unwrap();
        let r2 = rx2.await.unwrap();
        assert!(matches!(r1, InvocationResult::Error { ref error_type, .. } if error_type == "InvalidRuntimeException"));
        assert!(matches!(r2, InvocationResult::Error { ref error_type, .. } if error_type == "InvalidRuntimeException"));
    }

    #[tokio::test]
    async fn fail_init_drains_dispatched_pending_invocations() {
        let (_tx, rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx);

        // Simulate two invocations that were already dispatched via /next
        // and are sitting in pending_invocations.
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        bridge.store_pending(id1, "test-func".into(), None, tx1).await;
        bridge.store_pending(id2, "test-func".into(), None, tx2).await;

        // Also store one for a different function — should NOT be failed.
        let (tx3, mut rx3) = oneshot::channel();
        let id3 = Uuid::new_v4();
        bridge.store_pending(id3, "other-func".into(), None, tx3).await;

        let count = bridge.fail_init("test-func").await;
        assert_eq!(count, 2);

        // Both callers for test-func should receive init errors.
        let r1 = rx1.await.unwrap();
        let r2 = rx2.await.unwrap();
        assert!(matches!(r1, InvocationResult::Error { ref error_type, .. } if error_type == "InvalidRuntimeException"));
        assert!(matches!(r2, InvocationResult::Error { ref error_type, .. } if error_type == "InvalidRuntimeException"));

        // The other-func invocation should still be pending (not failed).
        assert!(rx3.try_recv().is_err());
    }

    #[tokio::test]
    async fn fail_init_unknown_function_returns_zero() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        assert_eq!(bridge.fail_init("nonexistent").await, 0);
    }

    #[tokio::test]
    async fn long_poll_receives_after_delay() {
        let (tx, rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let mut receivers = HashMap::new();
        receivers.insert("test-func".to_string(), rx);

        let bridge = Arc::new(RuntimeBridge::new(HashMap::new(), receivers, shutdown_rx));
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

    #[tokio::test]
    async fn timeout_invocation_returns_container_id() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let (tx, rx) = oneshot::channel();
        let request_id = Uuid::new_v4();
        bridge
            .store_pending(request_id, "test-func".into(), Some("container-abc".into()), tx)
            .await;

        let container_id = bridge.timeout_invocation(request_id).await;
        assert_eq!(container_id, Some("container-abc".to_string()));

        // Receiver should get Timeout result
        assert_eq!(rx.await.unwrap(), InvocationResult::Timeout);
    }

    #[tokio::test]
    async fn timeout_invocation_returns_none_for_unknown_request() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let result = bridge.timeout_invocation(Uuid::new_v4()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn timeout_invocation_without_container_id() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let (tx, rx) = oneshot::channel();
        let request_id = Uuid::new_v4();
        bridge
            .store_pending(request_id, "test-func".into(), None, tx)
            .await;

        let container_id = bridge.timeout_invocation(request_id).await;
        assert!(container_id.is_none());

        // Receiver should still get Timeout result
        assert_eq!(rx.await.unwrap(), InvocationResult::Timeout);
    }

    #[tokio::test]
    async fn store_pending_with_container_id_tracks_correctly() {
        let (_shutdown_tx, shutdown_rx) = shutdown_channel();
        let bridge = RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx);

        let (tx, rx) = oneshot::channel();
        let request_id = Uuid::new_v4();
        bridge
            .store_pending(request_id, "test-func".into(), Some("ctr-123".into()), tx)
            .await;

        // Complete normally — should still work
        let (success, cid) = bridge.complete_invocation(request_id, "ok".into()).await;
        assert!(success);
        assert_eq!(cid, Some("ctr-123".into()));
        assert_eq!(
            rx.await.unwrap(),
            InvocationResult::Success { body: "ok".into() }
        );
    }
}
