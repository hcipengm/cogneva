//! Task transfer & checkpoint-recovery protocol.
//! when an Agent shuts down (gracefully or abruptly) any task it had
//! `in_progress` is published to the `task_transfer` Redis Stream so a
//! replacement Agent can claim it.  The replacement loads the most recent
//! [`AgentCheckpoint`] and replays events from `event_offset` onward to resume
//! exactly where the original Agent left off.
//! This module also provides a polling [`StaleTaskDetector`] that walks the
//! [`AgentRegistry`] and re-publishes tasks owned by Agents whose TTL has
//! lapsed, satisfying the "Dead Agent → task transfer" branch of the
//! Supervisor protocol.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use cog_core::{
    AgentCheckpoint, AgentRegistry, CheckpointStore, Event, MessageBackend, SFError, SFResult,
    StateBackend,
};
use serde::{Deserialize, Serialize};

/// Default Redis stream name for task-transfer events.
pub const TASK_TRANSFER_STREAM: &str = "orchestrator:events:task_transfer";

/// Reason a task is being transferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferReason {
    /// Agent was shut down gracefully (SIGTERM); task remains recoverable.
    GracefulShutdown,
    /// Agent missed too many heartbeats and was declared dead.
    DeadAgent,
    /// Task ran past its hard timeout without progressing.
    StaleTask,
}

/// Payload XADD'd to the `task_transfer` stream when a task needs a new owner.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskTransferEvent {
    pub task_id: String,
    pub from_agent: String,
    pub reason: TransferReason,
    /// Snapshot id that should be loaded by the replacement Agent.
    /// `None` if no checkpoint was taken (replacement starts from scratch).
    pub checkpoint_id: Option<String>,
    /// Monotonically-increasing version (per-task) — replacement Agents reject
    /// any incoming transfer whose version is older than the local one.
    pub checkpoint_version: u64,
    pub timestamp: DateTime<Utc>,
}

/// Outcome of a `recover` call on a replacement Agent.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredTask {
    pub task_id: String,
    pub checkpoint: Option<AgentCheckpoint>,
    pub events: Vec<Event>,
}

/// Coordinator for task transfer: publishes transfer events and recovers
/// tasks from snapshots + replayed events.
pub struct TaskTransferCoordinator {
    backend: Arc<dyn MessageBackend>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    state_backend: Arc<dyn StateBackend>,
    stream_name: String,
}

impl TaskTransferCoordinator {
    pub fn new(
        backend: Arc<dyn MessageBackend>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        state_backend: Arc<dyn StateBackend>,
    ) -> Self {
        Self {
            backend,
            checkpoint_store,
            state_backend,
            stream_name: TASK_TRANSFER_STREAM.to_string(),
        }
    }

    pub fn with_stream_name(mut self, name: impl Into<String>) -> Self {
        self.stream_name = name.into();
        self
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Publish a [`TaskTransferEvent`] to the message backend.
    pub async fn publish_transfer(&self, event: &TaskTransferEvent) -> SFResult<()> {
        let payload = serde_json::to_vec(event).map_err(SFError::Serialization)?;
        self.backend.publish(&self.stream_name, &payload).await
    }

    /// Convenience: build + publish a transfer event for the given task.
    pub async fn transfer_task(
        &self,
        task_id: impl Into<String>,
        from_agent: impl Into<String>,
        reason: TransferReason,
        checkpoint_id: Option<String>,
        checkpoint_version: u64,
    ) -> SFResult<TaskTransferEvent> {
        let event = TaskTransferEvent {
            task_id: task_id.into(),
            from_agent: from_agent.into(),
            reason,
            checkpoint_id,
            checkpoint_version,
            timestamp: Utc::now(),
        };
        self.publish_transfer(&event).await?;
        Ok(event)
    }

    /// Recover a transferred task by loading its snapshot (if any) and any
    /// events that landed after the snapshot's `event_offset`.
    /// The replacement Agent should use `snapshot.context_window` as its
    /// initial context and apply each [`Event`] in order before resuming.
    pub async fn recover(
        &self,
        task_id: &str,
        checkpoint_id: Option<&str>,
    ) -> SFResult<RecoveredTask> {
        let checkpoint = match checkpoint_id {
            Some(id) => self.checkpoint_store.load(id).await?,
            None => None,
        };

        let event_offset = checkpoint.as_ref().map(|s| s.event_offset).unwrap_or(0);
        let events = self
            .state_backend
            .get_events(task_id, event_offset, 1024)
            .await?;

        Ok(RecoveredTask {
            task_id: task_id.to_string(),
            checkpoint,
            events,
        })
    }
}

/// Polls the [`AgentRegistry`] for tasks owned by Agents whose TTL has lapsed
/// and republishes them to the transfer stream.
pub struct StaleTaskDetector {
    registry: Arc<dyn AgentRegistry>,
    coordinator: Arc<TaskTransferCoordinator>,
    /// Tasks discovered by `task_owners()`: (task_id, owner_agent_id, last_known_offset).
    tracked: Arc<tokio::sync::Mutex<Vec<TrackedTask>>>,
    poll_interval: Duration,
}

#[derive(Debug, Clone)]
struct TrackedTask {
    task_id: String,
    owner: String,
    checkpoint_id: Option<String>,
    checkpoint_version: u64,
    /// Marks whether the task has already been transferred (so we don't double-publish).
    transferred: bool,
}

impl StaleTaskDetector {
    pub fn new(
        registry: Arc<dyn AgentRegistry>,
        coordinator: Arc<TaskTransferCoordinator>,
    ) -> Self {
        Self {
            registry,
            coordinator,
            tracked: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            poll_interval: Duration::from_secs(15),
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Register an in-flight task so the detector can transfer it on agent death.
    pub async fn track(
        &self,
        task_id: impl Into<String>,
        owner: impl Into<String>,
        checkpoint_id: Option<String>,
        checkpoint_version: u64,
    ) {
        let mut tracked = self.tracked.lock().await;
        tracked.push(TrackedTask {
            task_id: task_id.into(),
            owner: owner.into(),
            checkpoint_id,
            checkpoint_version,
            transferred: false,
        });
    }

    /// Stop tracking a task (e.g. after it completed normally).
    pub async fn untrack(&self, task_id: &str) {
        let mut tracked = self.tracked.lock().await;
        tracked.retain(|t| t.task_id != task_id);
    }

    /// Run a single sweep: any tracked task whose owner is no longer in the
    /// registry gets a [`TransferReason::DeadAgent`] event.
    /// Returns the list of tasks that were transferred this sweep.
    pub async fn sweep(&self) -> SFResult<Vec<TaskTransferEvent>> {
        let live = self.registry.list().await?;
        let live_ids: std::collections::HashSet<String> =
            live.into_iter().map(|r| r.agent_id).collect();

        let mut tracked = self.tracked.lock().await;
        let mut transferred = Vec::new();
        for task in tracked.iter_mut() {
            if task.transferred {
                continue;
            }
            if !live_ids.contains(&task.owner) {
                let event = self
                    .coordinator
                    .transfer_task(
                        task.task_id.clone(),
                        task.owner.clone(),
                        TransferReason::DeadAgent,
                        task.checkpoint_id.clone(),
                        task.checkpoint_version,
                    )
                    .await?;
                task.transferred = true;
                transferred.push(event);
            }
        }
        Ok(transferred)
    }

    /// Spawn a background task that calls [`Self::sweep`] on
    /// `poll_interval` until `cancel.wait()` resolves.
    pub fn spawn(self: Arc<Self>, cancel: cog_core::ShutdownSignal) -> tokio::task::JoinHandle<()> {
        let interval = self.poll_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = self.sweep().await {
                            tracing::warn!("stale-task sweep failed: {e}");
                        }
                    }
                    _ = cancel.wait() => break,
                }
            }
        })
    }
}
