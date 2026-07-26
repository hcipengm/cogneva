//! In-memory implementations of core backend traits.
//! All types here are intended for testing and local development.
//! They are re-exported from the crate root for convenience.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use cog_core::{
    AgentEvent, AgentState, ClusterOverview, ContextBoard, Event, EventFilter, LogEntry,
    MetricSample, MetricsBackend, ObservabilityGateway, RawLogIndex, RawLogIndexEntry,
    RawLogIndexStore, RawLogQuery, SFError, SFResult, SquadState, SquadStatus, StateBackend,
    TaskCheckpoint, TaskMetrics, VectorBackend, VectorSearchResult,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;

// ─── In-memory implementation ───

#[derive(Debug, Default)]
struct MemoryStore {
    agent_states: HashMap<String, AgentState>,
    checkpoints: HashMap<String, TaskCheckpoint>,
    events: HashMap<String, Vec<Event>>,
    boards: HashMap<String, ContextBoard>,
    dag_states: HashMap<String, serde_json::Value>,
    dag_tasks: HashMap<String, HashMap<String, cog_core::Task>>,
    dag_deps: HashMap<String, HashMap<String, Vec<String>>>,
    dag_dependents: HashMap<String, HashMap<String, Vec<String>>>,
}

/// In-memory state backend for testing.
pub struct MemoryStateBackend {
    store: RwLock<MemoryStore>,
}

impl MemoryStateBackend {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(MemoryStore::default()),
        }
    }
}

impl Default for MemoryStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateBackend for MemoryStateBackend {
    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.agent_states.get(agent_id).copied())
    }

    async fn set_agent_state(&self, agent_id: &str, state: &AgentState) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.agent_states.insert(agent_id.into(), *state);
        Ok(())
    }

    async fn cas_agent_state(
        &self,
        agent_id: &str,
        expected: &AgentState,
        new: &AgentState,
    ) -> SFResult<bool> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let current = store.agent_states.get(agent_id);
        if current == Some(expected) {
            store.agent_states.insert(agent_id.into(), *new);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn get_checkpoint(&self, task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.checkpoints.get(task_id).cloned())
    }

    async fn save_checkpoint(&self, checkpoint: &TaskCheckpoint) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store
            .checkpoints
            .insert(checkpoint.task_id.clone(), checkpoint.clone());
        Ok(())
    }

    async fn append_event(&self, task_id: &str, event: &Event) -> SFResult<u64> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let list = store.events.entry(task_id.into()).or_default();
        list.push(event.clone());
        Ok(list.len() as u64)
    }

    async fn get_events(&self, task_id: &str, offset: u64, limit: usize) -> SFResult<Vec<Event>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        match store.events.get(task_id) {
            Some(list) => {
                let start = offset as usize;
                let end = (start + limit).min(list.len());
                if start >= list.len() {
                    Ok(Vec::new())
                } else {
                    Ok(list[start..end].to_vec())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    async fn get_board(&self, task_id: &str) -> SFResult<Option<ContextBoard>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.boards.get(task_id).cloned())
    }

    async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let board = store
            .boards
            .entry(task_id.into())
            .or_insert_with(|| ContextBoard {
                task_id: task_id.into(),
                fields: HashMap::new(),
                updated_at: Utc::now(),
            });
        board.fields.insert(field.into(), value.into());
        board.updated_at = Utc::now();
        Ok(())
    }

    async fn delete_checkpoint(&self, task_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.checkpoints.remove(task_id);
        Ok(())
    }

    async fn delete_board(&self, task_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.boards.remove(task_id);
        Ok(())
    }

    async fn remove_board_field(&self, task_id: &str, field: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(board) = store.boards.get_mut(task_id) {
            board.fields.remove(field);
            board.updated_at = Utc::now();
        }
        Ok(())
    }

    async fn save_dag_state(&self, workspace_id: &str, state: &serde_json::Value) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.dag_states.insert(workspace_id.into(), state.clone());
        Ok(())
    }

    async fn load_dag_state(&self, workspace_id: &str) -> SFResult<Option<serde_json::Value>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.dag_states.get(workspace_id).cloned())
    }

    async fn dag_get_task(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Option<cog_core::Task>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_tasks
            .get(workspace_id)
            .and_then(|m| m.get(task_id).cloned()))
    }

    async fn dag_set_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        task: &cog_core::Task,
    ) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store
            .dag_tasks
            .entry(workspace_id.into())
            .or_default()
            .insert(task_id.into(), task.clone());
        Ok(())
    }

    async fn dag_remove_task(&self, workspace_id: &str, task_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(m) = store.dag_tasks.get_mut(workspace_id) {
            m.remove(task_id);
        }
        if let Some(m) = store.dag_deps.get_mut(workspace_id) {
            m.remove(task_id);
        }
        if let Some(m) = store.dag_dependents.get_mut(workspace_id) {
            m.remove(task_id);
        }
        Ok(())
    }

    async fn dag_list_tasks(&self, workspace_id: &str) -> SFResult<Vec<String>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_tasks
            .get(workspace_id)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn dag_get_dependencies(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Vec<String>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_deps
            .get(workspace_id)
            .and_then(|m| m.get(task_id).cloned())
            .unwrap_or_default())
    }

    async fn dag_set_dependencies(
        &self,
        workspace_id: &str,
        task_id: &str,
        deps: &[String],
    ) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store
            .dag_deps
            .entry(workspace_id.into())
            .or_default()
            .insert(task_id.into(), deps.to_vec());
        Ok(())
    }

    async fn dag_get_dependents(&self, workspace_id: &str, task_id: &str) -> SFResult<Vec<String>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .dag_dependents
            .get(workspace_id)
            .and_then(|m| m.get(task_id).cloned())
            .unwrap_or_default())
    }

    async fn dag_set_dependents(
        &self,
        workspace_id: &str,
        task_id: &str,
        dependents: &[String],
    ) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store
            .dag_dependents
            .entry(workspace_id.into())
            .or_default()
            .insert(task_id.into(), dependents.to_vec());
        Ok(())
    }

    async fn dag_complete_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;

        // Clone dependency data before taking mutable borrow of tasks.
        let dependents = store
            .dag_dependents
            .get(workspace_id)
            .cloned()
            .unwrap_or_default();
        let deps = store
            .dag_deps
            .get(workspace_id)
            .cloned()
            .unwrap_or_default();

        let tasks = store.dag_tasks.entry(workspace_id.into()).or_default();
        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;

        task.status = cog_core::TaskStatus::Completed;
        task.result = Some(result);
        task.updated_at = chrono::Utc::now();

        let mut ready = Vec::new();
        if let Some(dep_ids) = dependents.get(task_id) {
            for dep_id in dep_ids {
                let all_ready = deps
                    .get(dep_id)
                    .map(|blocked| {
                        blocked.iter().all(|b| {
                            tasks
                                .get(b)
                                .map(|t| matches!(t.status, cog_core::TaskStatus::Completed))
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(true);

                if all_ready {
                    if let Some(dep_task) = tasks.get_mut(dep_id) {
                        if matches!(dep_task.status, cog_core::TaskStatus::Pending) {
                            dep_task.status = cog_core::TaskStatus::Scheduled;
                            dep_task.updated_at = chrono::Utc::now();
                            ready.push(dep_id.clone());
                        }
                    }
                }
            }
        }

        Ok(ready)
    }

    async fn dag_fail_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        error: String,
        max_retries: u32,
    ) -> SFResult<(bool, Vec<String>)> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;

        // Clone dependency data before taking mutable borrow of tasks.
        let dependents = store
            .dag_dependents
            .get(workspace_id)
            .cloned()
            .unwrap_or_default();

        let tasks = store.dag_tasks.entry(workspace_id.into()).or_default();
        let task = tasks.get_mut(task_id).ok_or_else(|| SFError::TaskFailed {
            task_id: task_id.into(),
            reason: "Task not found".into(),
        })?;

        task.error = Some(error.clone());
        task.updated_at = chrono::Utc::now();

        let should_retry = task.retry_count < max_retries;
        let mut cancelled = Vec::new();

        if should_retry {
            task.status = cog_core::TaskStatus::Scheduled;
            task.retry_count += 1;
        } else {
            task.status = cog_core::TaskStatus::Failed;
            if let Some(dep_ids) = dependents.get(task_id) {
                for dep_id in dep_ids {
                    if let Some(dep_task) = tasks.get_mut(dep_id) {
                        if matches!(
                            dep_task.status,
                            cog_core::TaskStatus::Pending | cog_core::TaskStatus::Scheduled
                        ) {
                            dep_task.status = cog_core::TaskStatus::Cancelled;
                            dep_task.updated_at = chrono::Utc::now();
                            cancelled.push(dep_id.clone());
                        }
                    }
                }
            }
        }

        Ok((should_retry, cancelled))
    }

    async fn dag_clear_workspace(&self, workspace_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.dag_tasks.remove(workspace_id);
        store.dag_deps.remove(workspace_id);
        store.dag_dependents.remove(workspace_id);
        store.dag_states.remove(workspace_id);
        Ok(())
    }
}

/// In-memory snapshot store for testing.
#[derive(Debug, Default)]
pub struct MemorySnapshotStore {
    snapshots: std::sync::RwLock<HashMap<String, AgentCheckpoint>>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            snapshots: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

// ==========================================================================
// CheckpointStore implementation
// ==========================================================================

use cog_core::{AgentCheckpoint, CheckpointStore};

#[async_trait]
impl CheckpointStore for MemorySnapshotStore {
    async fn save(&self, checkpoint: &AgentCheckpoint) -> SFResult<String> {
        let mut store = self
            .snapshots
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        // Snapshot is a deprecated alias for AgentCheckpoint, so they share
        // the same fields.  We can store directly.
        store.insert(checkpoint.checkpoint_id.clone(), checkpoint.clone());
        Ok(checkpoint.checkpoint_id.clone())
    }

    async fn load(&self, checkpoint_id: &str) -> SFResult<Option<AgentCheckpoint>> {
        let store = self
            .snapshots
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.get(checkpoint_id).cloned())
    }

    async fn delete(&self, checkpoint_id: &str) -> SFResult<()> {
        let mut store = self
            .snapshots
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.remove(checkpoint_id);
        Ok(())
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentCheckpoint>> {
        let store = self
            .snapshots
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut cps: Vec<AgentCheckpoint> = store.values().cloned().collect();
        cps.sort_by_key(|a| std::cmp::Reverse(a.timestamp));
        cps.truncate(limit);
        Ok(cps)
    }
}

// ==========================================================================
// MemoryTraceStore — in-memory TraceStore for testing
// ==========================================================================

use cog_core::{AgentTrace, TraceStore};

/// In-memory trace store for testing.
#[derive(Debug, Default)]
pub struct MemoryTraceStore {
    traces: RwLock<HashMap<String, AgentTrace>>,
}

impl MemoryTraceStore {
    pub fn new() -> Self {
        Self {
            traces: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl TraceStore for MemoryTraceStore {
    async fn save(&self, trace: &AgentTrace) -> SFResult<String> {
        let mut store = self
            .traces
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.insert(trace.trace_id.clone(), trace.clone());
        Ok(trace.trace_id.clone())
    }

    async fn load(&self, trace_id: &str) -> SFResult<Option<AgentTrace>> {
        let store = self
            .traces
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.get(trace_id).cloned())
    }

    async fn delete(&self, trace_id: &str) -> SFResult<()> {
        let mut store = self
            .traces
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.remove(trace_id);
        Ok(())
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentTrace>> {
        let store = self
            .traces
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut result: Vec<_> = store.values().cloned().collect();
        result.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        result.truncate(limit);
        Ok(result)
    }

    async fn list_meta(&self, limit: usize) -> SFResult<Vec<cog_core::TraceMeta>> {
        let store = self
            .traces
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut result: Vec<_> = store
            .values()
            .map(cog_core::TraceMeta::from_trace)
            .collect();
        result.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        result.truncate(limit);
        Ok(result)
    }
}

pub use crate::mem::object_backends::MemoryObjectBackend;

// ─── In-memory implementation ───

#[derive(Debug, Default)]
pub struct MemoryMetricsBackend {
    gauges: RwLock<HashMap<String, Vec<MetricSample>>>,
    counters: RwLock<HashMap<String, Vec<MetricSample>>>,
    histograms: RwLock<HashMap<String, Vec<MetricSample>>>,
}

impl MemoryMetricsBackend {
    pub fn new() -> Self {
        Self {
            gauges: RwLock::new(HashMap::new()),
            counters: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    fn push_sample(
        store: &mut HashMap<String, Vec<MetricSample>>,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) {
        store.entry(name.into()).or_default().push(MetricSample {
            timestamp: Utc::now(),
            value,
            labels,
        });
    }

    fn query_range(
        store: &HashMap<String, Vec<MetricSample>>,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<MetricSample> {
        store
            .get(name)
            .map(|samples| {
                samples
                    .iter()
                    .filter(|s| s.timestamp >= start && s.timestamp <= end)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl MetricsBackend for MemoryMetricsBackend {
    async fn record_gauge(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let mut store = self
            .gauges
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Self::push_sample(&mut store, name, value, labels);
        Ok(())
    }

    async fn record_counter(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let mut store = self
            .counters
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Self::push_sample(&mut store, name, value, labels);
        Ok(())
    }

    async fn record_histogram(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let mut store = self
            .histograms
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Self::push_sample(&mut store, name, value, labels);
        Ok(())
    }

    async fn query_gauge_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let store = self
            .gauges
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(Self::query_range(&store, name, start, end))
    }

    async fn query_counter_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let store = self
            .counters
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(Self::query_range(&store, name, start, end))
    }

    async fn query_histogram_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let store = self
            .histograms
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(Self::query_range(&store, name, start, end))
    }

    async fn health_check(&self) -> SFResult<()> {
        let _guard = self
            .gauges
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(())
    }
}

// ─── In-memory implementation ───

#[derive(Debug, Clone)]
struct VectorEntry {
    vector: Vec<f32>,
    sparse: Option<cog_core::SparseEmbedding>,
    metadata: Value,
}

/// In-memory vector backend for testing and local development.
/// Uses brute-force cosine similarity — accurate but not scalable.
#[derive(Debug, Default)]
pub struct MemoryVectorBackend {
    collections: RwLock<HashMap<String, HashMap<String, VectorEntry>>>,
}

impl MemoryVectorBackend {
    pub fn new() -> Self {
        Self {
            collections: RwLock::new(HashMap::new()),
        }
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot / (norm_a * norm_b)
        }
    }
}

#[async_trait]
impl VectorBackend for MemoryVectorBackend {
    async fn create_collection(&self, collection: &str, _dimension: usize) -> SFResult<()> {
        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        cols.entry(collection.into()).or_default();
        Ok(())
    }

    async fn delete_collection(&self, collection: &str) -> SFResult<()> {
        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        cols.remove(collection);
        Ok(())
    }

    async fn insert(
        &self,
        collection: &str,
        vectors: Vec<Vec<f32>>,
        metadata: Vec<Value>,
    ) -> SFResult<Vec<String>> {
        if vectors.len() != metadata.len() {
            return Err(SFError::Agent(
                "vectors and metadata length mismatch".into(),
            ));
        }

        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let store = cols.entry(collection.into()).or_default();

        let mut ids = Vec::with_capacity(vectors.len());
        for (i, (vec, meta)) in vectors.into_iter().zip(metadata).enumerate() {
            let id = format!("vec-{}", store.len() + i);
            store.insert(
                id.clone(),
                VectorEntry {
                    vector: vec,
                    sparse: None,
                    metadata: meta,
                },
            );
            ids.push(id);
        }
        Ok(ids)
    }

    async fn search(
        &self,
        collection: &str,
        vector: &[f32],
        top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>> {
        let cols = self
            .collections
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let store = cols
            .get(collection)
            .ok_or_else(|| SFError::Agent(format!("collection {} not found", collection)))?;

        let mut results: Vec<VectorSearchResult> = store
            .iter()
            .map(|(id, entry)| VectorSearchResult {
                id: id.clone(),
                score: Self::cosine_similarity(vector, &entry.vector),
                metadata: entry.metadata.clone(),
            })
            .collect();

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        Ok(results)
    }

    async fn insert_sparse(
        &self,
        collection: &str,
        sparse: Vec<cog_core::SparseEmbedding>,
        metadata: Vec<Value>,
    ) -> SFResult<Vec<String>> {
        if sparse.len() != metadata.len() {
            return Err(SFError::Agent("sparse and metadata length mismatch".into()));
        }

        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let store = cols.entry(collection.into()).or_default();

        let mut ids = Vec::with_capacity(sparse.len());
        for (s, meta) in sparse.into_iter().zip(metadata) {
            let id = meta
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("vec-{}", store.len()));

            store
                .entry(id.clone())
                .and_modify(|e| e.sparse = Some(s.clone()))
                .or_insert(VectorEntry {
                    vector: Vec::new(),
                    sparse: Some(s),
                    metadata: meta,
                });
            ids.push(id);
        }
        Ok(ids)
    }

    async fn search_sparse(
        &self,
        collection: &str,
        sparse: &cog_core::SparseEmbedding,
        top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>> {
        let cols = self
            .collections
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let store = cols
            .get(collection)
            .ok_or_else(|| SFError::Agent(format!("collection {} not found", collection)))?;

        fn dot_product(a: &cog_core::SparseEmbedding, b: &cog_core::SparseEmbedding) -> f32 {
            let mut score = 0.0f32;
            let mut i = 0usize;
            let mut j = 0usize;
            while i < a.indices.len() && j < b.indices.len() {
                let ai = a.indices[i];
                let bj = b.indices[j];
                if ai == bj {
                    score += a.values[i] * b.values[j];
                    i += 1;
                    j += 1;
                } else if ai < bj {
                    i += 1;
                } else {
                    j += 1;
                }
            }
            score
        }

        let mut results: Vec<VectorSearchResult> = store
            .iter()
            .filter_map(|(id, entry)| {
                entry.sparse.as_ref().map(|s| VectorSearchResult {
                    id: id.clone(),
                    score: dot_product(sparse, s),
                    metadata: entry.metadata.clone(),
                })
            })
            .collect();

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        Ok(results)
    }

    async fn delete(&self, collection: &str, ids: &[String]) -> SFResult<()> {
        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(store) = cols.get_mut(collection) {
            for id in ids {
                store.remove(id);
            }
        }
        Ok(())
    }

    async fn collection_exists(&self, collection: &str) -> SFResult<bool> {
        let cols = self
            .collections
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(cols.contains_key(collection))
    }
}

// ─── In-memory implementation for testing / local dev ───

/// In-memory observability gateway for testing and local development.
/// Stores all data in RAM.  Events are broadcast via a bounded
/// [`tokio::sync::broadcast`] channel so that multiple consumers can
/// [`subscribe_events`] simultaneously.
pub struct MemoryObservabilityGateway {
    state_backend: Arc<dyn cog_core::StateBackend>,
    logs: RwLock<HashMap<String, Vec<LogEntry>>>,
    metrics: RwLock<HashMap<String, TaskMetrics>>,
    raw_index: RwLock<Vec<RawLogIndex>>,
    snapshots: RwLock<HashMap<String, String>>,
    squads: RwLock<HashMap<String, SquadState>>,
    event_tx: broadcast::Sender<AgentEvent>,
    event_channel_capacity: usize,
}

impl MemoryObservabilityGateway {
    pub fn new(state_backend: Arc<dyn cog_core::StateBackend>) -> Self {
        let (event_tx, _rx) = broadcast::channel(256);
        Self {
            state_backend,
            logs: RwLock::new(HashMap::new()),
            metrics: RwLock::new(HashMap::new()),
            raw_index: RwLock::new(Vec::new()),
            snapshots: RwLock::new(HashMap::new()),
            squads: RwLock::new(HashMap::new()),
            event_tx,
            event_channel_capacity: 256,
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    /// Publish an event to all active subscribers.
    pub fn publish_event(&self, event: AgentEvent) {
        let _ = self.event_tx.send(event);
    }

    /// Record a log entry for the given task.
    pub fn record_log(&self, task_id: &str, entry: LogEntry) {
        let mut logs = self.logs.write().unwrap();
        logs.entry(task_id.into()).or_default().push(entry);
    }

    /// Record metrics for a task.
    pub fn record_metrics(&self, metrics: TaskMetrics) {
        let mut m = self.metrics.write().unwrap();
        m.insert(metrics.task_id.clone(), metrics);
    }

    /// Register a raw-log index entry.
    pub fn register_raw_index(&self, entry: RawLogIndex) {
        let mut idx = self.raw_index.write().unwrap();
        idx.push(entry);
    }

    /// Register a snapshot URL.
    pub fn register_snapshot(&self, snapshot_id: &str, url: &str) {
        let mut snaps = self.snapshots.write().unwrap();
        snaps.insert(snapshot_id.into(), url.into());
    }

    /// Update squad state in memory.
    pub fn update_squad_state(&self, squad_id: &str, update: SquadState) {
        let mut squads = self.squads.write().unwrap();
        squads.insert(squad_id.into(), update);
    }
}

#[async_trait]
impl ObservabilityGateway for MemoryObservabilityGateway {
    async fn subscribe_events(
        &self,
        filter: EventFilter,
    ) -> SFResult<cog_core::observability::AgentEventStream> {
        let rx = self.event_tx.subscribe();
        let stream = futures::stream::unfold((rx, filter), |(mut rx, filter)| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if crate::event_filter::event_matches(&filter, &event) {
                            return Some((Ok(event), (rx, filter)));
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
        Ok(Box::pin(stream))
    }

    async fn get_agent_state(&self, agent_id: &str) -> SFResult<cog_core::AgentState> {
        match self.state_backend.get_agent_state(agent_id).await? {
            Some(s) => Ok(s),
            None => Err(SFError::Agent(format!("agent {} not found", agent_id))),
        }
    }

    async fn get_task_checkpoint(
        &self,
        task_id: &str,
    ) -> SFResult<Option<cog_core::TaskCheckpoint>> {
        self.state_backend.get_checkpoint(task_id).await
    }

    async fn get_task_metrics(&self, task_id: &str) -> SFResult<TaskMetrics> {
        let metrics = self.metrics.read().unwrap();
        metrics
            .get(task_id)
            .cloned()
            .ok_or_else(|| SFError::Agent(format!("metrics not found for task {}", task_id)))
    }

    async fn get_task_logs(&self, task_id: &str, limit: usize) -> SFResult<Vec<LogEntry>> {
        let logs = self.logs.read().unwrap();
        match logs.get(task_id) {
            Some(entries) => {
                let start = entries.len().saturating_sub(limit);
                Ok(entries[start..].to_vec())
            }
            None => Ok(Vec::new()),
        }
    }

    async fn get_snapshot_url(&self, snapshot_id: &str) -> SFResult<String> {
        let snaps = self.snapshots.read().unwrap();
        snaps
            .get(snapshot_id)
            .cloned()
            .ok_or_else(|| SFError::Agent(format!("snapshot {} not found", snapshot_id)))
    }

    async fn get_raw_log_index(&self, stream: &str, date: NaiveDate) -> SFResult<Vec<RawLogIndex>> {
        let idx = self.raw_index.read().unwrap();
        Ok(idx
            .iter()
            .filter(|i| i.stream == stream && i.date == date)
            .cloned()
            .collect())
    }

    async fn get_cluster_overview(&self) -> SFResult<ClusterOverview> {
        let metrics = self.metrics.read().unwrap();
        let total_tasks = metrics.len();
        let active_tasks = metrics.values().filter(|m| m.iterations > 0).count();
        let avg_duration = if total_tasks > 0 {
            metrics.values().map(|m| m.duration_ms).sum::<u64>() / total_tasks as u64
        } else {
            0
        };

        let squads = self.squads.read().unwrap();
        let total_squads = squads.len();
        let active_squads = squads
            .values()
            .filter(|s| matches!(s.status, SquadStatus::Running))
            .count();

        let total_agents = squads.values().map(|s| s.agents.len()).sum::<usize>();
        let active_agents = squads
            .values()
            .flat_map(|s| s.agents.iter())
            .filter(|a| matches!(a.state, cog_core::AgentState::Active))
            .count();

        Ok(ClusterOverview {
            total_agents,
            active_agents,
            total_tasks,
            active_tasks,
            queued_tasks: 0,
            failed_tasks: metrics
                .values()
                .filter(|m| m.tool_calls == 0 && m.iterations == 0)
                .count(),
            avg_task_duration_ms: avg_duration,
            cluster_health: "healthy".into(),
            timestamp: Utc::now(),
            total_squads,
            active_squads,
        })
    }

    async fn get_squad_state(&self, squad_id: &str) -> SFResult<SquadState> {
        let squads = self.squads.read().unwrap();
        squads
            .get(squad_id)
            .cloned()
            .ok_or_else(|| SFError::Agent(format!("squad {} not found", squad_id)))
    }

    fn publish_event(&self, event: AgentEvent) {
        let _ = self.event_tx.send(event);
    }
}

/// In-memory store used for tests and single-node deployments without
/// PostgreSQL.
#[derive(Debug, Default)]
pub struct MemoryRawLogIndexStore {
    entries: Mutex<Vec<RawLogIndexEntry>>,
}

impl MemoryRawLogIndexStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.lock().map(|v| v.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl RawLogIndexStore for MemoryRawLogIndexStore {
    async fn upsert(&self, entry: RawLogIndexEntry) -> SFResult<()> {
        let mut guard = self
            .entries
            .lock()
            .map_err(|_| SFError::Agent("raw_log_index lock poisoned".into()))?;
        // Treat (stream_name, log_date) as the primary key so re-runs are idempotent.
        if let Some(slot) = guard
            .iter_mut()
            .find(|e| e.stream_name == entry.stream_name && e.log_date == entry.log_date)
        {
            *slot = entry;
        } else {
            guard.push(entry);
        }
        Ok(())
    }

    async fn query(&self, q: &RawLogQuery) -> SFResult<Vec<RawLogIndexEntry>> {
        let guard = self
            .entries
            .lock()
            .map_err(|_| SFError::Agent("raw_log_index lock poisoned".into()))?;
        let mut out: Vec<RawLogIndexEntry> = guard
            .iter()
            .filter(|e| {
                q.stream
                    .as_deref()
                    .map(|s| s == e.stream_name)
                    .unwrap_or(true)
            })
            .filter(|e| q.tier.map(|t| t == e.tier).unwrap_or(true))
            .filter(|e| q.start.map(|s| e.end_time >= s).unwrap_or(true))
            .filter(|e| q.end.map(|t| e.start_time <= t).unwrap_or(true))
            .filter(|e| q.hour.map(|h| e.hour == h).unwrap_or(true))
            .cloned()
            .collect();

        out.sort_by_key(|e| e.start_time);
        if let Some(limit) = q.limit {
            out.truncate(limit);
        }
        Ok(out)
    }
}
