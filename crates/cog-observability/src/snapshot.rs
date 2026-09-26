/// Snapshot framework for deterministic replay and debugging.
/// - Capture full event stream (Context + Tool results) to Protobuf
/// - Replay: reconstruct initial environment → replay events → verify output
/// - Hot/Warm/Cold tiered storage
///   **Agent/Developer layer**: deterministic replay, regression testing, bug reproduction.
///   **Machine layer**: event stream is the SSOT for rebuilding agent state.
use chrono::Utc;
use cog_core::AgentEvent;

/// Loop name reported through the background-loop liveness family.
pub const TRACE_COLLECTOR_LOOP: &str = "observability_trace_collector";
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
            let beat = cog_core::loop_health::register(
                TRACE_COLLECTOR_LOOP,
                cog_core::loop_health::Cadence::EventDriven,
            );
            let _mortality = beat.watch_death(shutdown.clone());
            loop {
                beat.beat();
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
// TraceTierMigrator — moves aged traces across tiers via TraceStore
// ==========================================================================

/// Migration statistics.
#[derive(Debug, Default)]
pub struct MigrationStats {
    pub hot_to_warm: u64,
    pub warm_to_cold: u64,
    /// Entries whose tier no longer matches their age, i.e. the work a pass
    /// had. Equal to the moves plus `errors` (less any move that changes no
    /// tier name, which the coarse counters above cannot express).
    pub overdue: u64,
    /// Entries that could not be migrated. A pass continues past them, so a
    /// store with one unreadable entry still demotes all the others.
    pub errors: u64,
}

/// Trace tier migrator: scans persisted traces and updates their tier
/// when they have aged out of the current tier.
/// Operates entirely through [`cog_core::TraceStore`] — no direct file I/O.
pub struct TraceTierMigrator {
    trace_store: Arc<dyn cog_core::TraceStore>,
    config: cog_core::TierMigratorConfig,
    hot_overdue: AtomicU64,
    warm_overdue: AtomicU64,
    last_pass_seconds: AtomicU64,
    pass_failures: AtomicU64,
    measured: AtomicBool,
}

/// Gauge of entries per tier that are past their tier's age, whether or not
/// the pass could move them. Zero moves against a non-zero reading here is a
/// migration that is failing, not a store that needs nothing. Measured over
/// the pass' examined window, so a reading pinned at the configured scan batch
/// means the backlog is at least that large — i.e. the pass is behind, not
/// done.
pub const TRACE_TIER_OVERDUE_METRIC: &str = "cogneva_trace_tier_overdue";
/// Unix seconds of the last pass that ran, successful or not: the loop's
/// liveness, distinct from the store's health.
pub const TRACE_TIER_LAST_PASS_METRIC: &str = "cogneva_trace_tier_last_pass_seconds";
/// The configured cadence, so the staleness rule's threshold is a multiple of
/// whatever this deployment scans at rather than a number that has to be kept
/// in step with the migrator's config by hand.
pub const TRACE_TIER_SCAN_INTERVAL_METRIC: &str = "cogneva_trace_tier_scan_interval_seconds";
/// Passes that could not complete, i.e. the store could not be read. A pass
/// that completes keeps going past entries it cannot move, so those show up as
/// overdue rather than here.
pub const TRACE_TIER_PASS_FAILURES_METRIC: &str = "cogneva_trace_tier_pass_failures_total";
/// Names of the rules that consume these gauges, so the pairing can be
/// asserted rather than assumed.
pub const TRACE_TIER_OVERDUE_METRIC_RULE: &str = "trace_tier_demotion_stalled";
pub const TRACE_TIER_STALE_METRIC_RULE: &str = "trace_tier_migration_stale";
pub const TRACE_TIER_FAILURES_METRIC_RULE: &str = "trace_tier_migration_failing";

impl TraceTierMigrator {
    pub fn new(
        trace_store: Arc<dyn cog_core::TraceStore>,
        config: cog_core::TierMigratorConfig,
    ) -> Self {
        Self {
            trace_store,
            config,
            hot_overdue: AtomicU64::new(0),
            warm_overdue: AtomicU64::new(0),
            last_pass_seconds: AtomicU64::new(0),
            pass_failures: AtomicU64::new(0),
            measured: AtomicBool::new(false),
        }
    }

    /// How often a pass runs — the same config section that decides the tier
    /// boundaries, so one section governs both the decision and its cadence.
    pub fn scan_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.config.scan_interval_secs)
    }

    fn compression_for(&self, tier: cog_core::StorageTier) -> i32 {
        match tier {
            cog_core::StorageTier::Hot => 0,
            cog_core::StorageTier::Warm => self.config.warm_compression_level,
            cog_core::StorageTier::Cold => self.config.cold_compression_level,
        }
    }

    /// Run one migration pass.
    ///
    /// Each tier is scanned from its oldest entry, because an entry that has
    /// aged out of a tier is among that tier's oldest: reading the recent end
    /// would hide the backlog as soon as the tier outgrows the scan limit. A
    /// tier already at the right age for its contents is skipped, so the pass
    /// costs one listing per non-terminal tier.
    pub async fn run_migration(&self) -> anyhow::Result<MigrationStats> {
        self.last_pass_seconds
            .store(Utc::now().timestamp().max(0) as u64, Ordering::Relaxed);
        match self.run_pass().await {
            Ok(stats) => Ok(stats),
            Err(e) => {
                self.pass_failures.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    async fn run_pass(&self) -> anyhow::Result<MigrationStats> {
        let mut stats = MigrationStats::default();
        let hot = std::time::Duration::from_secs(self.config.hot_duration_secs);
        let warm = std::time::Duration::from_secs(self.config.warm_duration_secs);
        let batch = self.config.trace_scan_batch as usize;

        tracing::info!(
            hot_secs = hot.as_secs(),
            warm_secs = warm.as_secs(),
            scan_batch = batch,
            "Starting trace tier migration"
        );

        for (tier, slot) in [
            (cog_core::StorageTier::Hot, &self.hot_overdue),
            (cog_core::StorageTier::Warm, &self.warm_overdue),
        ] {
            // Metadata only: the heavy event arrays are loaded for the entries
            // that actually move, not for the ones examined and skipped.
            let metas = self
                .trace_store
                .list_meta_in_tier(tier, batch)
                .await
                .map_err(|e| anyhow::anyhow!("List {tier:?} failed: {e}"))?;

            let mut overdue = 0u64;
            for meta in metas {
                let target = cog_core::tier_for_age(Utc::now() - meta.created_at, hot, warm);
                if target == meta.tier {
                    continue;
                }
                overdue += 1;

                match self.move_trace(&meta.trace_id, target).await {
                    Ok(()) => {
                        match (meta.tier, target) {
                            (cog_core::StorageTier::Hot, cog_core::StorageTier::Warm) => {
                                stats.hot_to_warm += 1
                            }
                            (cog_core::StorageTier::Warm, cog_core::StorageTier::Cold) => {
                                stats.warm_to_cold += 1
                            }
                            _ => {}
                        }
                        tracing::debug!(
                            trace_id = %meta.trace_id,
                            from = ?meta.tier,
                            to = ?target,
                            "Migrated trace tier"
                        );
                    }
                    Err(e) => {
                        stats.errors += 1;
                        tracing::warn!(
                            trace_id = %meta.trace_id,
                            from = ?meta.tier,
                            to = ?target,
                            error = %e,
                            "Trace tier migration failed for this entry; continuing the pass"
                        );
                    }
                }
            }
            stats.overdue += overdue;
            slot.store(overdue, Ordering::Relaxed);
        }
        // Published only now: before a pass has completed, a zero would read as
        // "nothing overdue" when it means "nothing measured".
        self.measured.store(true, Ordering::Relaxed);

        tracing::info!(
            hot_to_warm = stats.hot_to_warm,
            warm_to_cold = stats.warm_to_cold,
            overdue = stats.overdue,
            errors = stats.errors,
            "Trace tier migration complete"
        );

        Ok(stats)
    }

    async fn move_trace(
        &self,
        trace_id: &str,
        target: cog_core::StorageTier,
    ) -> anyhow::Result<()> {
        let mut trace = self
            .trace_store
            .load(trace_id)
            .await
            .map_err(|e| anyhow::anyhow!("Load failed: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("Trace listed but not loadable: {trace_id}"))?;

        trace.tier = target;
        trace.compression = self.compression_for(target);

        self.trace_store
            .save(&trace)
            .await
            .map_err(|e| anyhow::anyhow!("Save failed: {e}"))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl cog_core::Observable for TraceTierMigrator {
    /// The dimension is ignored on purpose: none of this varies by dimension,
    /// so declaring none is what gets this observable pulled once. Answering
    /// only one dimension would instead have it absent from the scrape —
    /// invisible rather than unlabelled.
    async fn collect_metrics(
        &self,
        _dimension: &str,
    ) -> cog_core::SFResult<Vec<cog_core::observability::RawMetric>> {
        use cog_core::observability::RawMetric;

        let mut out = Vec::new();
        if self.measured.load(Ordering::Relaxed) {
            for (tier, slot) in [("hot", &self.hot_overdue), ("warm", &self.warm_overdue)] {
                out.push(
                    RawMetric::new(
                        TRACE_TIER_OVERDUE_METRIC,
                        slot.load(Ordering::Relaxed) as f64,
                    )
                    .with_label("tier", tier),
                );
            }
        }
        let last_pass = self.last_pass_seconds.load(Ordering::Relaxed);
        if last_pass > 0 {
            out.push(RawMetric::new(
                TRACE_TIER_LAST_PASS_METRIC,
                last_pass as f64,
            ));
            // Only alongside a pass timestamp: the interval means nothing to a
            // staleness comparison until there is a pass to be stale against.
            out.push(RawMetric::new(
                TRACE_TIER_SCAN_INTERVAL_METRIC,
                self.config.scan_interval_secs as f64,
            ));
        }
        let failures = self.pass_failures.load(Ordering::Relaxed);
        if failures > 0 {
            out.push(RawMetric::new(
                TRACE_TIER_PASS_FAILURES_METRIC,
                failures as f64,
            ));
        }
        Ok(out)
    }

    async fn collect_trace(
        &self,
        _task_id: &str,
    ) -> cog_core::SFResult<Vec<cog_core::observability::TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<cog_core::observability::DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Observable, TraceStore};
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemTraceStore {
        saved: Mutex<Vec<cog_core::AgentTrace>>,
    }

    /// A trace at a known age in a known tier, so a pass' decision can be
    /// stated as ages rather than as clock arithmetic inside the test.
    fn aged_trace(
        trace_id: &str,
        tier: cog_core::StorageTier,
        age_days: i64,
    ) -> cog_core::AgentTrace {
        cog_core::AgentTrace {
            trace_id: trace_id.into(),
            session_id: None,
            task_id: "task".into(),
            agent_id: "agent".into(),
            created_at: Utc::now() - chrono::Duration::days(age_days),
            event_count: 0,
            byte_size: 0,
            version: "test".into(),
            tier,
            compression: if tier == cog_core::StorageTier::Hot {
                0
            } else {
                3
            },
            checksum: String::new(),
            events: Vec::new(),
            llm_requests: Vec::new(),
            llm_responses: Vec::new(),
            tool_calls: Vec::new(),
        }
    }

    /// One day hot, one week warm, as shipped.
    fn migrator_config() -> cog_core::TierMigratorConfig {
        cog_core::TierMigratorConfig {
            enabled: true,
            hot_duration_secs: 86_400,
            warm_duration_secs: 604_800,
            scan_interval_secs: 3_600,
            trace_scan_batch: 100,
            ..Default::default()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::TraceStore for MemTraceStore {
        async fn save(&self, trace: &cog_core::AgentTrace) -> cog_core::SFResult<String> {
            // Upsert, like every real store: saving an existing id replaces it,
            // so a migration that re-saves a trace at a new tier must not leave
            // the old copy behind.
            let mut saved = self.saved.lock().unwrap();
            match saved.iter_mut().find(|t| t.trace_id == trace.trace_id) {
                Some(existing) => *existing = trace.clone(),
                None => saved.push(trace.clone()),
            }
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
        async fn list_meta_in_tier(
            &self,
            tier: cog_core::StorageTier,
            limit: usize,
        ) -> cog_core::SFResult<Vec<cog_core::TraceMeta>> {
            let mut metas: Vec<cog_core::TraceMeta> = self
                .saved
                .lock()
                .unwrap()
                .iter()
                .filter(|t| t.tier == tier)
                .map(cog_core::TraceMeta::from_trace)
                .collect();
            metas.sort_by_key(|m| m.created_at);
            metas.truncate(limit);
            Ok(metas)
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

    /// The tier an entry belongs in is a function of its age, so the pass has
    /// to be able to see entries that aged out of the tier they are sitting in
    /// — which is the oldest end of that tier, not the recent end.
    #[tokio::test]
    async fn aged_entries_are_demoted_and_fresh_ones_are_left_alone() {
        let store = Arc::new(MemTraceStore::default());
        for trace in [
            aged_trace("stale-hot", cog_core::StorageTier::Hot, 2),
            aged_trace("stale-warm", cog_core::StorageTier::Warm, 40),
            aged_trace("fresh", cog_core::StorageTier::Hot, 0),
        ] {
            store.save(&trace).await.unwrap();
        }
        let migrator = TraceTierMigrator::new(store.clone(), migrator_config());

        let stats = migrator.run_migration().await.unwrap();
        assert_eq!(stats.hot_to_warm, 1, "a 2-day-old hot trace is warm");
        assert_eq!(stats.warm_to_cold, 1, "a 40-day-old warm trace is cold");
        assert_eq!(stats.overdue, 2);
        assert_eq!(stats.errors, 0);

        {
            let saved = store.saved.lock().unwrap();
            let tier_of = |id: &str| {
                saved
                    .iter()
                    .find(|t| t.trace_id == id)
                    .map(|t| t.tier)
                    .unwrap_or_else(|| panic!("{id} missing"))
            };
            assert_eq!(tier_of("stale-hot"), cog_core::StorageTier::Warm);
            assert_eq!(tier_of("stale-warm"), cog_core::StorageTier::Cold);
            assert_eq!(tier_of("fresh"), cog_core::StorageTier::Hot);
        }

        // A second pass has nothing left to do: the demotion is a state, not a
        // repeated action, so a pass that keeps moving the same entry would
        // mean the move is not sticking.
        let again = migrator.run_migration().await.unwrap();
        assert_eq!(again.overdue, 0);
        assert_eq!(again.hot_to_warm + again.warm_to_cold, 0);
    }

    /// The gauge must not exist before a pass has measured anything: a zero
    /// would read as "nothing is overdue" when it means "nothing was looked at".
    #[tokio::test]
    async fn overdue_gauge_appears_only_after_a_pass() {
        let store = Arc::new(MemTraceStore::default());
        store
            .save(&aged_trace("stale", cog_core::StorageTier::Hot, 3))
            .await
            .unwrap();
        let migrator = TraceTierMigrator::new(store, migrator_config());

        let before = migrator.collect_metrics("D8").await.unwrap();
        assert!(
            before.is_empty(),
            "no pass has run, so there is nothing to report: {before:?}"
        );

        migrator.run_migration().await.unwrap();

        let after = migrator.collect_metrics("D8").await.unwrap();
        let overdue: Vec<_> = after
            .iter()
            .filter(|m| m.name == TRACE_TIER_OVERDUE_METRIC)
            .collect();
        assert_eq!(overdue.len(), 2, "one per non-terminal tier");
        let hot = overdue
            .iter()
            .find(|m| m.labels.get("tier").map(String::as_str) == Some("hot"))
            .expect("hot gauge");
        assert_eq!(
            hot.value, 1.0,
            "the pass found one hot entry that had aged out of hot"
        );
        let warm = overdue
            .iter()
            .find(|m| m.labels.get("tier").map(String::as_str) == Some("warm"))
            .expect("warm gauge");
        assert_eq!(
            warm.value, 0.0,
            "nothing in warm is past its age, so a steady state is a real zero"
        );
        assert!(
            after.iter().any(|m| m.name == TRACE_TIER_LAST_PASS_METRIC),
            "a staleness rule needs the last pass' timestamp"
        );
        assert_eq!(
            after
                .iter()
                .find(|m| m.name == TRACE_TIER_SCAN_INTERVAL_METRIC)
                .expect("scan interval gauge")
                .value,
            3_600.0,
            "the staleness threshold is a multiple of the deployment's own cadence"
        );
    }

    /// A rename on either side of the gauge/rule pair leaves both halves
    /// internally consistent and the signal silently absent, so the deployed
    /// rules are pinned against the metric names this module publishes.
    #[test]
    fn deployed_rules_query_the_metrics_this_module_publishes() {
        let chart = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = std::fs::read_to_string(&chart)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        let rules = root
            .pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .expect("infra_watch.rules present");

        for (rule_name, metric) in [
            (TRACE_TIER_OVERDUE_METRIC_RULE, TRACE_TIER_OVERDUE_METRIC),
            (TRACE_TIER_STALE_METRIC_RULE, TRACE_TIER_LAST_PASS_METRIC),
            (
                TRACE_TIER_FAILURES_METRIC_RULE,
                TRACE_TIER_PASS_FAILURES_METRIC,
            ),
        ] {
            let rule = rules
                .iter()
                .find(|r| r["name"] == rule_name)
                .unwrap_or_else(|| panic!("rule {rule_name} missing"));
            let promql = rule["promql"].as_str().expect("promql is a string");
            assert!(
                promql.contains(metric),
                "rule {rule_name} must query {metric}, got: {promql}"
            );
        }
    }
}
