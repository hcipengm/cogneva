/// Snapshot framework for deterministic replay and debugging.
/// - Capture full event stream (Context + Tool results) to Protobuf
/// - Replay: reconstruct initial environment → replay events → verify output
/// - Hot/Warm/Cold tiered storage
///   **Agent/Developer layer**: deterministic replay, regression testing, bug reproduction.
///   **Machine layer**: event stream is the SSOT for rebuilding agent state.
use chrono::{DateTime, Utc};
use cog_core::AgentEvent;
use std::sync::Arc;

// ==========================================================================
// TraceSerializer — compression-aware serialization
// ==========================================================================

/// Trace serializer: handles encoding/decoding + compression.
use std::collections::HashMap;

pub struct TraceSerializer;

impl TraceSerializer {
    pub fn new() -> Self {
        Self
    }

    /// Serialize `AgentTrace` events to JSON Lines.
    pub fn to_jsonl(trace: &cog_core::AgentTrace) -> Result<String, serde_json::Error> {
        let mut lines = String::new();
        for event in &trace.events {
            lines.push_str(&serde_json::to_string(event)?);
            lines.push('\n');
        }
        Ok(lines)
    }

    /// Compute blake3 checksum of serialized `AgentTrace` events.
    pub fn compute_checksum(trace: &cog_core::AgentTrace) -> String {
        let jsonl = Self::to_jsonl(trace).unwrap_or_default();
        blake3::hash(jsonl.as_bytes()).to_string()
    }

    /// Serialize a trace to bytes with optional compression.
    pub fn serialize(
        &self,
        trace: &cog_core::AgentTrace,
        compression: i32,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let json = serde_json::to_string(trace)?;
        let bytes = json.into_bytes();
        if compression > 0 {
            Ok(zstd::encode_all(&bytes[..], compression)?)
        } else {
            Ok(bytes)
        }
    }

    /// Deserialize a trace from bytes.
    pub fn deserialize(
        &self,
        bytes: &[u8],
        compression: i32,
    ) -> Result<cog_core::AgentTrace, anyhow::Error> {
        let decompressed = if compression > 0 {
            zstd::decode_all(bytes)?
        } else {
            bytes.to_vec()
        };
        let json = String::from_utf8(decompressed)?;
        Ok(serde_json::from_str(&json)?)
    }
}

impl Default for TraceSerializer {
    fn default() -> Self {
        Self::new()
    }
}

// ==========================================================================
// TraceCollector — trait-based collector (design doc compliant)
// ==========================================================================

/// TraceCollector: collects Agent execution traces via [`cog_core::TraceStore`] trait.
/// Does **not** perform file I/O directly.  All persistence is delegated to
/// the injected [`cog_core::TraceStore`] implementation (e.g. `RedisTraceStore`,
/// `S3TraceStore`, `MemoryTraceStore`).
pub struct TraceCollector {
    trace_store: std::sync::Arc<dyn cog_core::TraceStore>,
}

impl TraceCollector {
    pub fn new(trace_store: std::sync::Arc<dyn cog_core::TraceStore>) -> Self {
        Self { trace_store }
    }

    /// Collect and persist an execution trace.
    pub async fn collect(
        &self,
        trace_id: impl Into<String>,
        session_id: Option<String>,
        task_id: Option<String>,
        agent_id: Option<String>,
        events: Vec<AgentEvent>,
    ) -> anyhow::Result<String> {
        let start = std::time::Instant::now();
        let id = trace_id.into();
        let event_count = events.len() as u64;
        let mut trace = cog_core::AgentTrace {
            trace_id: id.clone(),
            session_id,
            task_id: task_id.unwrap_or_default(),
            agent_id: agent_id.unwrap_or_default(),
            created_at: Utc::now(),
            event_count,
            byte_size: 0,
            version: env!("CARGO_PKG_VERSION").into(),
            tier: cog_core::StorageTier::Hot,
            compression: 0,
            checksum: String::new(),
            events,
            llm_requests: Vec::new(),
            llm_responses: Vec::new(),
            tool_calls: Vec::new(),
        };

        trace.checksum = TraceSerializer::compute_checksum(&trace);
        let json = serde_json::to_vec(&trace)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        trace.byte_size = json.len() as u64;

        self.trace_store
            .save(&trace)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let latency_ms = start.elapsed().as_millis() as u64;
        crate::observable::global_observable().record_snapshot_latency(latency_ms);
        for _ in 0..event_count {
            crate::observable::global_observable().record_event();
        }

        tracing::info!(
            trace_id = %id,
            event_count = trace.event_count,
            byte_size = trace.byte_size,
            "Trace collected and persisted via TraceStore"
        );

        Ok(id)
    }

    /// Load a trace by id.
    pub async fn load(&self, trace_id: &str) -> anyhow::Result<Option<cog_core::AgentTrace>> {
        self.trace_store
            .load(trace_id)
            .await
            .map_err(|e| anyhow::anyhow!("Trace load failed: {e}"))
    }

    /// List recent traces.
    pub async fn list(&self, limit: usize) -> anyhow::Result<Vec<cog_core::AgentTrace>> {
        self.trace_store
            .list(limit)
            .await
            .map_err(|e| anyhow::anyhow!("Trace list failed: {e}"))
    }

    /// List lightweight trace metadata.
    pub async fn list_meta(&self, limit: usize) -> anyhow::Result<Vec<cog_core::TraceMeta>> {
        self.trace_store
            .list_meta(limit)
            .await
            .map_err(|e| anyhow::anyhow!("Trace list_meta failed: {e}"))
    }

    /// Spawn a background task that subscribes to a broadcast [`AgentEvent`] stream
    /// and automatically collects per-agent execution traces.
    ///
    /// Events buffer in memory per `agent_id` and flush on [`AgentEvent::AgentEnd`].
    /// Squad agents can run for hours across dozens of iterations, so the buffer
    /// is bounded by `buffer_max_bytes`: on overflow the buffered events flush as
    /// a partial trace chunk (`{agent_id}-{run_id}-part{n}`) and buffering resumes.
    /// Without the bound a single long run accumulates an unbounded in-memory
    /// trace (observed 80 MiB for one planner) and OOM-kills the host process.
    /// `buffer_max_bytes == 0` disables chunking.
    /// The task stops when the broadcast channel closes or `shutdown` fires.
    pub fn spawn_collection_task(
        self: Arc<Self>,
        mut event_rx: tokio::sync::broadcast::Receiver<AgentEvent>,
        shutdown: cog_core::ShutdownSignal,
        buffer_max_bytes: usize,
    ) -> tokio::task::JoinHandle<()> {
        struct AgentBuffer {
            events: Vec<AgentEvent>,
            bytes: usize,
            /// Stable per run so all chunks of one run share the id prefix.
            run_id: String,
            /// How many partial chunks have been flushed for this run.
            flushed: u32,
        }

        fn event_size(event: &AgentEvent) -> usize {
            // Serialized length is the honest size measure: the same bytes are
            // what would sit in the buffer. Events arrive at LLM-call pace, so
            // the extra serialization is negligible against the flush-time
            // serialization that happens anyway.
            serde_json::to_vec(event).map(|v| v.len()).unwrap_or(0)
        }

        tokio::spawn(async move {
            let mut buffers: HashMap<String, AgentBuffer> = HashMap::new();
            loop {
                tokio::select! {
                    result = event_rx.recv() => {
                        match result {
                            Ok(event) => {
                                let agent_id = match &event {
                                    AgentEvent::AgentStart { agent_id, .. } => {
                                        let bytes = event_size(&event);
                                        buffers.insert(
                                            agent_id.clone(),
                                            AgentBuffer {
                                                events: vec![event.clone()],
                                                bytes,
                                                run_id: uuid::Uuid::new_v4().to_string(),
                                                flushed: 0,
                                            },
                                        );
                                        continue;
                                    }
                                    AgentEvent::AgentEnd { agent_id, .. } => {
                                        let entry = buffers.remove(agent_id);
                                        let (events, trace_id) = match entry {
                                            Some(mut entry) => {
                                                entry.events.push(event.clone());
                                                let trace_id = if entry.flushed == 0 {
                                                    format!("{}-{}", agent_id, entry.run_id)
                                                } else {
                                                    format!(
                                                        "{}-{}-part{}",
                                                        agent_id, entry.run_id, entry.flushed
                                                    )
                                                };
                                                (entry.events, trace_id)
                                            }
                                            // AgentEnd without a seen AgentStart (collector
                                            // joined mid-run): still persist the terminal
                                            // event, as before chunking existed.
                                            None => (
                                                vec![event.clone()],
                                                format!(
                                                    "{}-{}",
                                                    agent_id,
                                                    uuid::Uuid::new_v4()
                                                ),
                                            ),
                                        };
                                        if let Err(e) = self.collect(
                                            &trace_id,
                                            None,
                                            None,
                                            Some(agent_id.clone()),
                                            events,
                                        ).await {
                                            tracing::warn!("Trace collection failed: {}", e);
                                        }
                                        continue;
                                    }
                                    AgentEvent::CheckpointSaved { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::TurnStart { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::TurnEnd { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::MessageStart { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::MessageUpdate { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::MessageEnd { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::ToolExecutionStart { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::ToolExecutionUpdate { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::ToolExecutionEnd { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::ReActStepStart { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::ReActStepEnd { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::SelfReview { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::StateChange { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::TaskStatusChange { agent_id, .. } => {
                                        if let Some(id) = agent_id {
                                            id.clone()
                                        } else {
                                            continue;
                                        }
                                    }
                                    AgentEvent::AgentError { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::ResourceAlert { agent_id, .. } => agent_id.clone(),
                                    AgentEvent::Heartbeat { agent_id, .. } => agent_id.clone(),
                                };
                                if let Some(entry) = buffers.get_mut(&agent_id) {
                                    let size = event_size(&event);
                                    if buffer_max_bytes > 0
                                        && entry.bytes + size > buffer_max_bytes
                                        && !entry.events.is_empty()
                                    {
                                        let chunk = std::mem::take(&mut entry.events);
                                        let trace_id = format!(
                                            "{}-{}-part{}",
                                            agent_id, entry.run_id, entry.flushed
                                        );
                                        entry.flushed += 1;
                                        entry.bytes = 0;
                                        tracing::info!(
                                            agent_id = %agent_id,
                                            part = entry.flushed,
                                            buffer_max_bytes,
                                            "trace buffer budget reached; flushed partial trace chunk"
                                        );
                                        if let Err(e) = self.collect(
                                            &trace_id,
                                            None,
                                            None,
                                            Some(agent_id.clone()),
                                            chunk,
                                        ).await {
                                            tracing::warn!("Trace chunk collection failed: {}", e);
                                        }
                                    }
                                    entry.bytes += size;
                                    entry.events.push(event);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!("Trace collector lagged by {} events", n);
                            }
                        }
                    }
                    _ = shutdown.wait() => {
                        tracing::info!("Trace collection task shutting down");
                        break;
                    }
                }
            }
        })
    }
}

// ==========================================================================
// ReplayEngine — deterministic re-execution from a trace
// ==========================================================================

/// Replay engine: deterministic re-execution from a persisted trace.
/// Loads traces via [`cog_core::TraceStore`] (no direct file I/O) and replays events
/// step-by-step for regression testing and bug reproduction.
pub struct ReplayEngine {
    trace_store: Arc<dyn cog_core::TraceStore>,
}

impl ReplayEngine {
    pub fn new(trace_store: Arc<dyn cog_core::TraceStore>) -> Self {
        Self { trace_store }
    }

    /// Load a trace by id and replay its events.
    /// Returns the number of events replayed. Each event is passed to the
    /// callback for processing.
    pub async fn replay<F>(
        &self,
        trace_id: &str,
        mut event_handler: F,
    ) -> Result<u64, anyhow::Error>
    where
        F: FnMut(&AgentEvent) -> Result<(), anyhow::Error>,
    {
        let start = std::time::Instant::now();
        let trace = self
            .trace_store
            .load(trace_id)
            .await
            .map_err(|e| anyhow::anyhow!("Trace load failed: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("Trace not found: {trace_id}"))?;

        tracing::info!(
            trace_id = %trace_id,
            event_count = trace.event_count,
            "Replaying trace"
        );

        let mut replayed = 0u64;
        for event in &trace.events {
            event_handler(event)?;
            replayed += 1;
        }

        let latency_ms = start.elapsed().as_millis() as u64;
        crate::observable::global_observable().record_snapshot_latency(latency_ms);

        tracing::info!(
            trace_id = %trace_id,
            replayed,
            "Trace replay complete"
        );

        Ok(replayed)
    }

    /// List traces filtered by tier.
    pub async fn list_by_tier(
        &self,
        tier: cog_core::StorageTier,
        limit: usize,
    ) -> anyhow::Result<Vec<cog_core::AgentTrace>> {
        let all = self
            .trace_store
            .list(limit)
            .await
            .map_err(|e| anyhow::anyhow!("Trace list failed: {e}"))?;
        Ok(all.into_iter().filter(|t| t.tier == tier).collect())
    }

    /// List lightweight metadata filtered by tier.
    pub async fn list_meta_by_tier(
        &self,
        tier: cog_core::StorageTier,
        limit: usize,
    ) -> anyhow::Result<Vec<cog_core::TraceMeta>> {
        let all = self
            .trace_store
            .list_meta(limit)
            .await
            .map_err(|e| anyhow::anyhow!("Trace list_meta failed: {e}"))?;
        Ok(all.into_iter().filter(|m| m.tier == tier).collect())
    }
}

// ─── cog-core trait bridge ────────────────────────────────────────────────

#[async_trait::async_trait]
impl cog_core::ReplayEngine for ReplayEngine {
    async fn replay(
        &self,
        trace_id: &str,
        mut event_handler: Box<dyn FnMut(cog_core::AgentEvent) -> cog_core::SFResult<()> + Send>,
    ) -> cog_core::SFResult<u64> {
        let start = std::time::Instant::now();
        let trace = self
            .trace_store
            .load(trace_id)
            .await
            .map_err(|e| cog_core::SFError::IO(e.to_string()))?
            .ok_or_else(|| cog_core::SFError::IO(format!("Trace not found: {trace_id}")))?;

        tracing::info!(
            trace_id = %trace_id,
            event_count = trace.event_count,
            "Replaying trace"
        );

        let mut replayed = 0u64;
        for event in trace.events {
            event_handler(event).map_err(|e| cog_core::SFError::IO(e.to_string()))?;
            replayed += 1;
        }

        let latency_ms = start.elapsed().as_millis() as u64;
        crate::observable::global_observable().record_snapshot_latency(latency_ms);

        tracing::info!(
            trace_id = %trace_id,
            replayed,
            "Trace replay complete"
        );

        Ok(replayed)
    }
}

impl ReplayEngine {
    /// Verify that two traces are semantically equivalent.
    /// Used for regression testing: after framework iteration, replay
    /// the same trace and compare outputs.
    pub fn verify_equivalent(
        a: &cog_core::AgentTrace,
        b: &cog_core::AgentTrace,
    ) -> Result<(), String> {
        if a.events.len() != b.events.len() {
            return Err(format!(
                "Event count mismatch: {} vs {}",
                a.events.len(),
                b.events.len()
            ));
        }
        for (i, (ea, eb)) in a.events.iter().zip(b.events.iter()).enumerate() {
            let a_json = serde_json::to_string(ea).map_err(|e| e.to_string())?;
            let b_json = serde_json::to_string(eb).map_err(|e| e.to_string())?;
            if a_json != b_json {
                return Err(format!("Event {} differs", i));
            }
        }
        Ok(())
    }
}

// ==========================================================================
// Tier policy — storage-tier strategy mappings (lives in observability layer)
// ==========================================================================

/// Return the zstd compression level for a tier.
pub fn tier_compression_level(tier: cog_core::StorageTier) -> i32 {
    match tier {
        cog_core::StorageTier::Hot => 0,
        cog_core::StorageTier::Warm => 3,
        cog_core::StorageTier::Cold => 9,
    }
}

/// Return the retention duration in days for a tier.
pub fn tier_retention_days(tier: cog_core::StorageTier) -> u32 {
    match tier {
        cog_core::StorageTier::Hot => 7,
        cog_core::StorageTier::Warm => 90,
        cog_core::StorageTier::Cold => u32::MAX,
    }
}

/// Return the subdirectory name for a tier.
pub fn tier_subdir(tier: cog_core::StorageTier) -> &'static str {
    match tier {
        cog_core::StorageTier::Hot => "hot",
        cog_core::StorageTier::Warm => "warm",
        cog_core::StorageTier::Cold => "cold",
    }
}

/// Determine the tier for a given age in days.
pub fn tier_for_age(
    age_days: u32,
    hot_retention_days: u32,
    warm_retention_days: u32,
) -> cog_core::StorageTier {
    if age_days <= hot_retention_days {
        cog_core::StorageTier::Hot
    } else if age_days <= warm_retention_days {
        cog_core::StorageTier::Warm
    } else {
        cog_core::StorageTier::Cold
    }
}

/// Determine the tier from a creation timestamp.
pub fn tier_for_timestamp(
    created_at: DateTime<Utc>,
    hot_retention_days: u32,
    warm_retention_days: u32,
) -> cog_core::StorageTier {
    let age = Utc::now().signed_duration_since(created_at);
    let age_days = age.num_days().max(0) as u32;
    tier_for_age(age_days, hot_retention_days, warm_retention_days)
}

// ==========================================================================
// TraceTierMigrator — moves aged traces across tiers via TraceStore
// ==========================================================================

/// Migration statistics.
#[derive(Debug, Default)]
pub struct MigrationStats {
    pub hot_to_warm: u64,
    pub warm_to_cold: u64,
}

/// Trace tier migrator: scans persisted traces and updates their tier
/// when they have aged out of the current tier.
/// Operates entirely through [`cog_core::TraceStore`] — no direct file I/O.
pub struct TraceTierMigrator {
    trace_store: Arc<dyn cog_core::TraceStore>,
    hot_retention_days: u32,
    warm_retention_days: u32,
}

impl TraceTierMigrator {
    pub fn new(
        trace_store: Arc<dyn cog_core::TraceStore>,
        hot_retention_days: u32,
        warm_retention_days: u32,
    ) -> Self {
        Self {
            trace_store,
            hot_retention_days,
            warm_retention_days,
        }
    }

    /// Run a full migration scan.
    /// Lists all traces, checks each trace's age against the retention policy,
    /// and re-saves any trace whose tier has changed. The actual physical
    /// move (e.g. Redis -> S3) is handled by the [`cog_core::TraceStore`] backend
    /// or a downstream consumer that watches tier changes.
    pub async fn run_migration(&self) -> anyhow::Result<MigrationStats> {
        let mut stats = MigrationStats::default();

        tracing::info!(
            hot_days = self.hot_retention_days,
            warm_days = self.warm_retention_days,
            "Starting trace tier migration"
        );

        // Scan metadata only (avoids loading heavy event arrays for traces
        // that do not need migration).
        let metas = self
            .trace_store
            .list_meta(10_000)
            .await
            .map_err(|e| anyhow::anyhow!("List failed: {e}"))?;

        for meta in metas {
            let current_tier = tier_for_timestamp(
                meta.created_at,
                self.hot_retention_days,
                self.warm_retention_days,
            );
            if current_tier != meta.tier {
                let from = meta.tier;

                // Load the full trace only when migration is actually needed.
                let mut trace = self
                    .trace_store
                    .load(&meta.trace_id)
                    .await
                    .map_err(|e| anyhow::anyhow!("Load failed: {e}"))?
                    .ok_or_else(|| anyhow::anyhow!("Trace not found: {}", meta.trace_id))?;

                trace.tier = current_tier;
                trace.compression = tier_compression_level(current_tier);

                self.trace_store
                    .save(&trace)
                    .await
                    .map_err(|e| anyhow::anyhow!("Save failed: {e}"))?;

                match (from, current_tier) {
                    (cog_core::StorageTier::Hot, cog_core::StorageTier::Warm) => {
                        stats.hot_to_warm += 1
                    }
                    (cog_core::StorageTier::Warm, cog_core::StorageTier::Cold) => {
                        stats.warm_to_cold += 1
                    }
                    _ => {}
                }

                tracing::info!(
                    trace_id = %meta.trace_id,
                    from = ?from,
                    to = ?current_tier,
                    "Migrated trace tier"
                );
            }
        }

        tracing::info!(
            hot_to_warm = stats.hot_to_warm,
            warm_to_cold = stats.warm_to_cold,
            "Trace tier migration complete"
        );

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemTraceStore {
        saved: Mutex<Vec<cog_core::AgentTrace>>,
    }

    #[async_trait::async_trait]
    impl cog_core::TraceStore for MemTraceStore {
        async fn save(&self, trace: &cog_core::AgentTrace) -> cog_core::SFResult<String> {
            self.saved.lock().unwrap().push(trace.clone());
            Ok(trace.trace_id.clone())
        }
        async fn load(&self, trace_id: &str) -> cog_core::SFResult<Option<cog_core::AgentTrace>> {
            Ok(self
                .saved
                .lock()
                .unwrap()
                .iter()
                .find(|t| t.trace_id == trace_id)
                .cloned())
        }
        async fn delete(&self, trace_id: &str) -> cog_core::SFResult<()> {
            self.saved
                .lock()
                .unwrap()
                .retain(|t| t.trace_id != trace_id);
            Ok(())
        }
        async fn list(&self, limit: usize) -> cog_core::SFResult<Vec<cog_core::AgentTrace>> {
            Ok(self
                .saved
                .lock()
                .unwrap()
                .iter()
                .take(limit)
                .cloned()
                .collect())
        }
        async fn list_meta(&self, limit: usize) -> cog_core::SFResult<Vec<cog_core::TraceMeta>> {
            Ok(self
                .saved
                .lock()
                .unwrap()
                .iter()
                .take(limit)
                .map(cog_core::TraceMeta::from_trace)
                .collect())
        }
    }

    fn heartbeat(agent_id: &str) -> AgentEvent {
        AgentEvent::Heartbeat {
            agent_id: agent_id.to_string(),
            timestamp: Utc::now(),
        }
    }

    fn agent_start(agent_id: &str) -> AgentEvent {
        AgentEvent::AgentStart {
            agent_id: agent_id.to_string(),
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        }
    }

    fn agent_end(agent_id: &str) -> AgentEvent {
        AgentEvent::AgentEnd {
            agent_id: agent_id.to_string(),
            messages: Vec::new(),
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        }
    }

    /// A run whose events exceed the byte budget must flush bounded partial
    /// chunks instead of accumulating one unbounded in-memory trace, and the
    /// final chunk on AgentEnd carries the part suffix so all pieces share
    /// the run id prefix.
    #[tokio::test]
    async fn buffer_budget_overflow_flushes_partial_chunks() {
        let store = Arc::new(MemTraceStore::default());
        let collector = Arc::new(TraceCollector {
            trace_store: store.clone(),
        });
        let (tx, rx) = tokio::sync::broadcast::channel(64);
        let shutdown = cog_core::ShutdownSignal::new();
        let handle = collector
            .clone()
            .spawn_collection_task(rx, shutdown.clone(), 2048);

        let agent = "agent-x";
        tx.send(agent_start(agent)).unwrap();
        for _ in 0..60 {
            tx.send(heartbeat(agent)).unwrap();
        }
        tx.send(agent_end(agent)).unwrap();

        // Let the collector drain the channel before shutdown.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.trigger();
        let _ = handle.await;

        let saved = store.saved.lock().unwrap();
        assert!(
            saved.len() > 1,
            "expected multiple chunks, got {}",
            saved.len()
        );
        let prefix = format!("{agent}-");
        assert!(saved.iter().all(|t| t.trace_id.starts_with(&prefix)));
        assert!(saved.iter().any(|t| t.trace_id.contains("-part")));
        let total_events: u64 = saved.iter().map(|t| t.event_count).sum();
        assert_eq!(total_events, 62, "no event may be lost across chunks");
    }

    /// A run within budget keeps the legacy single-trace id shape (no part
    /// suffix), so existing trace consumers see no change.
    #[tokio::test]
    async fn run_within_budget_keeps_single_trace_id() {
        let store = Arc::new(MemTraceStore::default());
        let collector = Arc::new(TraceCollector {
            trace_store: store.clone(),
        });
        let (tx, rx) = tokio::sync::broadcast::channel(64);
        let shutdown = cog_core::ShutdownSignal::new();
        let handle = collector
            .clone()
            .spawn_collection_task(rx, shutdown.clone(), 1 << 20);

        let agent = "agent-y";
        tx.send(agent_start(agent)).unwrap();
        tx.send(heartbeat(agent)).unwrap();
        tx.send(agent_end(agent)).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.trigger();
        let _ = handle.await;

        let saved = store.saved.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert!(!saved[0].trace_id.contains("-part"));
        assert_eq!(saved[0].event_count, 3);
    }
}
