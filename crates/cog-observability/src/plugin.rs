//! principle: feedback_a_metrics_name_can_be_the_opposite_of_the_verdict_it_carries
//!
//! Observability plugin — implements [`cog_core::SystemPlugin`].

use chrono::Utc;
use cog_core::alerts::{AlertChannel, AlertEvent, AlertInstance, AlertSeverity, AlertState};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use crate::alert_store::{AlertRowStore, NewAlert, PostgresAlertStore};
use crate::bell::InAppNotifier;
use crate::usage_store::LlmUsageStore;

/// 一次巡检的最小周期：30s。
///
/// 判据被配得极短时，这条界保证巡检不会变成对着表打的热循环；正常配置下巡检
/// 周期是判据的一半，远大于这个下限。
const MIN_SWEEP_PERIOD: std::time::Duration = std::time::Duration::from_secs(30);

/// Loop name reported through the background-loop liveness family.
pub const TRACE_TIER_MIGRATION_LOOP: &str = "observability_trace_tier_migration";

/// Observability plugin that self-assembles and publishes raw logger, metrics,
/// Loki, ClickHouse, Jaeger, and Elasticsearch services.
pub struct ObservabilityPlugin {
    initialized: bool,
    trace_collector: Option<Arc<crate::snapshot::TraceCollector>>,
    trace_tier_migrator: Option<Arc<crate::snapshot::TraceTierMigrator>>,
    /// Persistent alert state machine, created in `init` so the read-side
    /// service is published before any plugin `start` runs (init_all
    /// completes before start_all; publishing in start would race consumers).
    alert_store: Option<Arc<PostgresAlertStore>>,
    /// The LLM usage ledger, created in `init` from the same database the alert
    /// store uses; the rollup loop reads through it in `start`.
    usage_store: Option<Arc<LlmUsageStore>>,
    /// The saved-call ledger, created in `init` beside the usage ledger and
    /// published as the write face the skipping call sites hold. Absent when
    /// the database is absent, which is what makes a caller see "nothing to
    /// write to" rather than a saving that silently did not land.
    skip_store: Option<Arc<crate::skip_store::LlmSkipStore>>,
    /// The verdict projection over the two ledgers, created in `init` beside
    /// them and driven by its own loop in `start`. It exists only when the
    /// usage ledger does: a verdict about a spend that cannot be read is not a
    /// verdict, and the loop has nothing to resume from.
    verdict_store: Option<Arc<crate::spend_verdict::SpendVerdictStore>>,
    /// Per-agent trace buffer budget, read in `init` from config and applied
    /// to the collection task in `start`.
    trace_buffer_max_bytes: usize,
    /// Footprint gauge per claim-backed directory, created in `init` and
    /// scanned by one task each in `start`. Each observable carries the claim
    /// and the directory it belongs to, so a scan cannot be pointed at a
    /// directory other than the one its series is attributed to.
    data_volume: Vec<Arc<crate::data_volume::DataVolumeObservable>>,
}

impl ObservabilityPlugin {
    /// Create a plugin that will build all observability services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            trace_collector: None,
            trace_tier_migrator: None,
            alert_store: None,
            usage_store: None,
            skip_store: None,
            verdict_store: None,
            trace_buffer_max_bytes: crate::config::TraceCollectorConfig::default().buffer_max_bytes,
            data_volume: Vec::new(),
        }
    }
}

impl Default for ObservabilityPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for ObservabilityPlugin {
    fn name(&self) -> &'static str {
        "observability"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        // Snapshot config values to drop immutable borrow before publishing.
        let (app_name, app_version, log_level, observability, metrics, tier_migrator) = {
            let config = ctx.config();
            (
                config.app.name.clone(),
                config.app.version.clone(),
                config.app.log_level.clone(),
                // observability 是 cog-observability 自有配置段，自读 cogneva.json。
                crate::ObservabilityExportersConfig::load()?,
                config.metrics.clone(),
                config.tier_migrator.clone(),
            )
        };

        // Consume HTTP client (published by NetPlugin).
        let http_client: Arc<dyn cog_core::HttpClient> =
            ctx.require_service::<dyn cog_core::HttpClient>()?.clone();

        self.trace_buffer_max_bytes = observability.trace_collector.buffer_max_bytes;

        // ── Subscriber (global) ──
        let log_level = std::env::var("RUST_LOG").unwrap_or(log_level);
        let log_format = if log_level == "json" {
            crate::LogFormat::Json
        } else {
            crate::LogFormat::Pretty
        };

        // Loki mirroring is a tracing layer, and a global subscriber can be
        // installed only once, so the pusher that layer feeds has to exist
        // before the subscriber does. Building it after the install (as this
        // plugin used to) left the pusher published but never fed and its flush
        // loop never started: the app-side logs never left the process, while
        // the plugin logged that the pusher was published.
        let loki_service = observability.jaeger.service_name.clone();
        let loki_pusher = if !observability.loki.enabled {
            None
        } else if observability.jaeger.enabled {
            warn!(
                "loki and jaeger are both enabled; the jaeger initializer owns the \
                 subscriber, so the Loki log mirror is skipped"
            );
            None
        } else {
            let client = Arc::new(
                crate::logs::LokiPushClient::new(&observability.loki.endpoint)
                    .with_max_retries(observability.loki.max_retries.max(1))
                    .with_timeout(observability.loki.timeout_secs)
                    .with_label("service", &loki_service)
                    .with_client(http_client.clone()),
            );
            let pusher = Arc::new(crate::logs::LokiBackgroundPusher::new(
                client.clone(),
                std::time::Duration::from_secs(observability.loki.flush_interval_sec.max(1)),
                observability.loki.max_batch_size.max(1),
            ));
            // Same sink the ClickHouse buffer below takes: this plugin's init
            // runs after the storage plugin has published the backend, so the
            // handle is here and the pusher can report a batch it could not
            // deliver.
            pusher.set_metrics(ctx.consume_service::<dyn cog_core::MetricsBackend>());
            ctx.publish(client.clone());
            ctx.publish(pusher.clone());
            Some(pusher)
        };

        let (jaeger_exporter, log_filter_handle) = if observability.jaeger.enabled {
            let exporter = crate::jaeger::init_jaeger_subscriber(
                &observability.jaeger.endpoint,
                &observability.jaeger.service_name,
                &log_level,
                log_format,
                Some(http_client.clone()),
            );
            (Some(exporter), None)
        } else {
            let handle = crate::logs::init_subscriber_with_pusher(
                &log_level,
                log_format,
                loki_pusher.clone(),
                &loki_service,
            );
            (None, Some(handle))
        };
        // Started only after the sink above was set, so the loop never runs a
        // round it cannot record.
        if let Some(pusher) = loki_pusher.clone() {
            drop(pusher.run_loop());
            info!("ObservabilityPlugin Loki client + pusher published and flushing");
        }
        if let Some(handle) = log_filter_handle {
            ctx.publish(Arc::new(LogFilterHandleHolder(handle)));
            info!("ObservabilityPlugin LogFilterHandle published");
        }

        // ── Raw logger ──
        let raw_logger = ctx.require_service::<dyn cog_core::RawLogger>()?.clone();
        {
            let record = cog_core::RawRecord {
                meta: cog_core::RawMeta {
                    version: "1.0".into(),
                    stream: "system_raw".into(),
                    recorded_at: chrono::Utc::now(),
                    recorded_by: "cogneva".into(),
                    sequence: 0,
                    trace_id: uuid::Uuid::new_v4().to_string(),
                    span_id: None,
                },
                context: cog_core::RawContext::default(),
                payload: cog_core::RawPayload {
                    direction: "internal".into(),
                    transport: "system".into(),
                    format: Some("json".into()),
                    raw: serde_json::json!({
                        "event": "config_loaded",
                        "app_name": app_name,
                        "app_version": app_version,
                        "log_level": log_level,
                    }),
                },
            };
            if let Err(e) = raw_logger.write(record).await {
                warn!("RawLogger write failed (system_raw): {}", e);
            }
        }
        info!("ObservabilityPlugin raw logger consumed");

        // ── Metrics exporter ──
        if metrics.enabled {
            let ex: Arc<dyn cog_core::MetricsExporter> = Arc::new(crate::MetricsExporter::new());
            ctx.publish_service(ex);
            info!("ObservabilityPlugin metrics exporter published");
        } else {
            info!("Metrics exporter disabled by config");
        }

        // ── Loki ──
        // The client + pusher are built next to the subscriber, because the
        // tracing layer that feeds the pusher has to be in place before the
        // subscriber is installed. See the subscriber block above.
        if !observability.loki.enabled {
            info!("Loki push client disabled by config");
        }

        // ── ClickHouse ──
        if observability.clickhouse.enabled {
            let backend = crate::analytics::ClickHouseAnalyticsBackend::new(
                &observability.clickhouse.base_url,
                &observability.clickhouse.database,
            )
            .with_table(&observability.clickhouse.table)
            .with_auth(
                &observability.clickhouse.username,
                &observability.clickhouse.password,
            )
            .with_client(http_client.clone());
            let backend = Arc::new(backend);
            let buffer = Arc::new(crate::analytics::ClickHouseEventBuffer::new(
                backend.clone(),
                std::time::Duration::from_secs(observability.clickhouse.flush_interval_sec),
                observability.clickhouse.max_batch_size,
                ctx.consume_service::<dyn cog_core::MetricsBackend>(),
            ));
            ctx.publish(backend.clone());
            ctx.publish(buffer.clone());
            info!("ObservabilityPlugin ClickHouse backend + buffer published");
        } else {
            info!("ClickHouse analytics backend disabled by config");
        }

        // ── Jaeger exporter ──
        if let Some(exporter) = jaeger_exporter {
            ctx.publish(exporter);
            info!("ObservabilityPlugin Jaeger exporter published");
        }

        // ── Elasticsearch ──
        if observability.elasticsearch.enabled {
            let mut backend = crate::search_index::ElasticsearchBackend::new(
                &observability.elasticsearch.base_url,
            )
            .with_client(http_client.clone());
            if !observability.elasticsearch.api_key.is_empty() {
                backend = backend.with_api_key(&observability.elasticsearch.api_key);
            } else if !observability.elasticsearch.username.is_empty() {
                backend = backend.with_basic_auth(
                    &observability.elasticsearch.username,
                    &observability.elasticsearch.password,
                );
            }
            let backend: Arc<dyn cog_core::SearchBackend> = Arc::new(backend);
            ctx.publish_service(backend);
            info!("ObservabilityPlugin Elasticsearch backend published");
        } else {
            info!("Elasticsearch search backend disabled by config");
        }

        // ── Trace store ──
        let trace_store = ctx.require_service::<dyn cog_core::TraceStore>()?.clone();
        info!("ObservabilityPlugin trace store consumed");

        // ── Trace collector & replay engine ──
        let trace_collector = Arc::new(
            crate::snapshot::TraceCollector::new(trace_store.clone())
                .with_metrics(ctx.consume_service::<dyn cog_core::MetricsBackend>()),
        );
        let replay_engine: Arc<dyn cog_core::ReplayEngine> =
            Arc::new(crate::snapshot::ReplayEngine::new(trace_store.clone()));
        ctx.publish(trace_collector.clone());
        ctx.publish_service(replay_engine.clone());
        info!("ObservabilityPlugin trace collector + replay engine published");

        // ── Trace tier migrator ──
        let trace_tier_migrator = Arc::new(crate::snapshot::TraceTierMigrator::new(
            trace_store.clone(),
            tier_migrator.clone(),
        ));
        ctx.publish(trace_tier_migrator.clone());
        // Published as an observable as well: the infra alert rules read
        // Prometheus, and only observables are rendered on the /metrics
        // endpoint's raw-metric path. A migration whose overruns never reach
        // Prometheus has no observer outside its own log.
        ctx.publish_observable(trace_tier_migrator.clone());
        info!("ObservabilityPlugin trace tier migrator published");

        self.trace_collector = Some(trace_collector);
        self.trace_tier_migrator = Some(trace_tier_migrator);

        // Observable publish (pin-style)
        let observable = crate::observable::global_observable();
        ctx.publish_observable(observable.clone());
        info!("ObservabilityPlugin observable published");

        // Evolution metrics service for self-evolution pipeline.
        let evolution_metrics: Arc<dyn cog_core::EvolutionMetrics> = observable.clone();
        ctx.publish_service(evolution_metrics);
        info!("ObservabilityPlugin evolution metrics published");

        // ── Data directory footprint ──
        // The volume's declared size is compared against this gauge; nothing
        // else measures what a directory-backed volume actually holds, so
        // without it an overrun has no observation face at all. One gauge per
        // claim: a reading attributed to the wrong claim invents an overrun on
        // one volume and hides the one on another.
        let (targets, rejected) = crate::data_volume::resolve_targets(
            &observability.data_volume_watch,
            std::path::Path::new(&ctx.config().app.data_dir),
        );
        for reason in &rejected {
            warn!(reason = %reason, "watched volume not measured");
        }
        for target in targets {
            let volume = Arc::new(crate::data_volume::DataVolumeObservable::new(target));
            ctx.publish_observable(volume.clone());
            info!(
                claim = %volume.claim(),
                dir = %volume.dir().display(),
                "ObservabilityPlugin data volume footprint published"
            );
            self.data_volume.push(volume);
        }

        // ── Orphaned processes adopted as PID 1 ──
        // The container entrypoint is the only process that can collect an
        // orphan, and nothing else reports whether it is still doing so: a
        // reaper that stopped and a reaper with nothing to do read the same
        // from the outside. Publishing the count is what tells them apart.
        ctx.publish_observable(Arc::new(crate::process_zombies::ProcessZombieObservable));

        // ── Where the birth footprint went ──
        // Published even though most of the marks are taken before this plugin
        // initialises: the observable reads the registry at scrape time, so the
        // snapshots taken during startup are all there by the time anyone asks.
        ctx.publish_observable(Arc::new(crate::boot_footprint::BootFootprintObservable));

        // ── Host producers that feed the platform's accounts ──
        // The row is written by the producer on the host, once per successful
        // cycle whether or not that cycle had anything new to queue; the intake
        // is idempotent, so every other trace of a delivery is a trace of
        // content that arrived and says nothing about a producer that stopped.
        // Read here at scrape time rather than published by the account's own
        // loop, so that the reading goes quiet with the *producer* rather than
        // with the platform: a loop that reported its own feeder's silence would
        // be reporting it from inside the thing the feeder feeds.
        match std::env::var(cog_core::env::DATABASE_URL) {
            Ok(url) if !url.trim().is_empty() => {
                match crate::producer_heartbeat::ProducerHeartbeatStore::connect(&url).await {
                    Ok(store) => match store.init_schema().await {
                        Ok(()) => {
                            ctx.publish_observable(Arc::new(
                                crate::producer_heartbeat::ProducerHeartbeatObservable::new(store),
                            ));
                            info!("producer heartbeat readings published");
                        }
                        Err(e) => warn!(
                            error = %e,
                            "producer heartbeat table init failed; the producers' last \
                             deliveries are not readable and their rules read that as a \
                             producer that has delivered nothing"
                        ),
                    },
                    Err(e) => warn!(error = %e, "producer heartbeat store connect failed"),
                }
            }
            _ => info!("COGNEVA_DATABASE_URL unset; producer heartbeats not readable"),
        }

        // ── Persistent alert state machine ──
        // Created here (not in start) so the read-side ActiveAlertSource is
        // published before any plugin's start runs — init_all completes
        // before start_all, and self-discovery consumes it in start.
        self.alert_store = match std::env::var(cog_core::env::DATABASE_URL) {
            Ok(url) if !url.trim().is_empty() => match PostgresAlertStore::connect(&url).await {
                Ok(store) => match store.init_schema().await {
                    Ok(()) => {
                        info!("alert state machine persisted to PostgreSQL");
                        let store = Arc::new(store);
                        let source: Arc<dyn cog_core::ActiveAlertSource> = store.clone();
                        ctx.publish_service(source);
                        let sink: Arc<dyn cog_core::PersistentAlertSink> = store.clone();
                        ctx.publish_service(sink);
                        Some(store)
                    }
                    Err(e) => {
                        warn!(error = %e, "alert table init failed; alerts stay notification-only");
                        None
                    }
                },
                Err(e) => {
                    warn!(error = %e, "alert store connect failed; alerts stay notification-only");
                    None
                }
            },
            _ => {
                info!("COGNEVA_DATABASE_URL unset; alert state machine not persisted");
                None
            }
        };

        // ── LLM usage ledger + window rollup ──
        // The ledger table is created by the gateway, which is the only writer,
        // but this process creates it too: the rollup reads it from a different
        // pod, and a fold that only worked once the gateway had run would be a
        // fold that silently did nothing on a fresh database. `IF NOT EXISTS`
        // makes the two creators agree rather than race.
        self.usage_store = match std::env::var(cog_core::env::DATABASE_URL) {
            Ok(url) if !url.trim().is_empty() => match LlmUsageStore::connect(&url).await {
                Ok(store) => match store.init_schema().await {
                    Ok(()) => {
                        info!("LLM usage ledger reachable; window rollup armed");
                        Some(Arc::new(store))
                    }
                    Err(e) => {
                        warn!(error = %e, "LLM usage schema init failed; rollup disabled");
                        None
                    }
                },
                Err(e) => {
                    warn!(error = %e, "LLM usage store connect failed; rollup disabled");
                    None
                }
            },
            _ => {
                info!("COGNEVA_DATABASE_URL unset; LLM usage rollup disabled");
                None
            }
        };

        // Publish the rolled read face beside the store. The gateway answers
        // "where did the tokens go" from the fold through this, the same way
        // it reaches every other cross-crate reading, so it never has to name
        // this crate's storage internals. Absent when the ledger is absent,
        // which is what makes the route report "not configured" instead of an
        // empty spend.
        if let Some(store) = self.usage_store.as_ref() {
            let reader: Arc<dyn cog_core::LlmUsageReader> = Arc::new(
                crate::usage_store::RolledUsageReader::new((**store).clone()),
            );
            ctx.publish_service(reader);
            info!("LLM usage rolled read face published");
        }

        // ── Saved-call ledger ──
        // The other half of the token account. A call that is deliberately not
        // made writes no usage row, so the saving is stated where the decision
        // is taken, in this same database, and read back by the verdict fold
        // that joins it against the window's spend. Published as a write face
        // for the reason the rolled read face above is published: the call
        // sites hold a contract, never this crate's storage internals.
        self.skip_store = match std::env::var(cog_core::env::DATABASE_URL) {
            Ok(url) if !url.trim().is_empty() => {
                match crate::skip_store::LlmSkipStore::connect(&url).await {
                    Ok(store) => match store.init_schema().await {
                        Ok(()) => {
                            info!("saved-call ledger reachable");
                            Some(Arc::new(store))
                        }
                        Err(e) => {
                            warn!(error = %e, "saved-call schema init failed; savings stay in-process");
                            None
                        }
                    },
                    Err(e) => {
                        warn!(error = %e, "saved-call store connect failed; savings stay in-process");
                        None
                    }
                }
            }
            _ => None,
        };
        if let Some(store) = self.skip_store.as_ref() {
            let sink: Arc<dyn cog_core::LlmSkipSink> = Arc::new((**store).clone());
            ctx.publish_service(sink);
            info!("saved-call write face published");
        }

        // ── Spend verdict projection ──
        // The face that says what each call site's window *amounts to*: the
        // name of the direction on both sides of the account, which neither
        // ledger can produce on its own (a usage row only ever describes calls
        // that happened, and a saved call only ever describes a call that did
        // not). Built here rather than in `start` so the store exists before
        // the loop that drives it, and absent when there is no usage ledger to
        // judge.
        if let Some(ledger) = self.usage_store.as_ref() {
            if let Ok(url) = std::env::var(cog_core::env::DATABASE_URL) {
                if !url.trim().is_empty() {
                    let skips = self.skip_store.as_ref().map(|s| (**s).clone());
                    match crate::spend_verdict::SpendVerdictStore::connect(
                        &url,
                        (**ledger).clone(),
                        skips,
                    )
                    .await
                    {
                        Ok(store) => match store.init_schema().await {
                            Ok(()) => {
                                info!("spend verdict projection armed");
                                self.verdict_store = Some(Arc::new(store));
                            }
                            Err(e) => {
                                warn!(error = %e, "spend verdict schema init failed; the account keeps no verdicts")
                            }
                        },
                        Err(e) => warn!(error = %e, "spend verdict store connect failed"),
                    }
                }
            }
        }

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // ── Trace collector ──
        if let Some(ref trace_collector) = self.trace_collector {
            if let Some(event_tx) =
                ctx.consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            {
                let _handle = trace_collector.clone().spawn_collection_task(
                    (*event_tx).subscribe(),
                    ctx.consume::<cog_core::ShutdownSignal>()
                        .map(|s| (*s).clone())
                        .unwrap_or_default(),
                    self.trace_buffer_max_bytes,
                );
            }
        }

        // ── Trace tier migrator ──
        if let Some(ref trace_tier_migrator) = self.trace_tier_migrator {
            let trace_migrator = trace_tier_migrator.clone();
            let interval = trace_migrator.scan_interval();
            let shutdown = ctx
                .consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            // The loop supervises itself (a panic is run again in place) and
            // nobody holds its handle: shutdown closes it through the signal,
            // and the task goes away with the process either way.
            drop(cog_core::loop_health::spawn(
                TRACE_TIER_MIGRATION_LOOP,
                cog_core::loop_health::Cadence::Periodic(interval),
                shutdown.clone(),
                // Rebuilt per attempt, so everything the body consumes is cloned here.
                move |beat| {
                    let trace_migrator = trace_migrator.clone();
                    let shutdown = shutdown.clone();
                    async move {
                        let mut interval = tokio::time::interval(interval);
                        loop {
                            beat.beat();
                            tokio::select! {
                                _ = interval.tick() => {
                                    if let Err(e) = trace_migrator.run_migration().await {
                                        warn!("Trace tier migration failed: {}", e);
                                    }
                                }
                                _ = shutdown.wait() => break,
                            }
                        }
                    }
                },
            ));
        }

        // ── LLM usage window rollup ──
        // The fold runs here, in the main application, and not in the gateway:
        // the gateway is the process that restarts on every rollout, and a
        // reading whose producer dies on each rollout is the reading this loop
        // exists to replace. No handle is held; shutdown closes it through the
        // signal like the loop above.
        if let Some(store) = self.usage_store.clone() {
            let shutdown = ctx
                .consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            drop(crate::llm_usage_rollup::spawn(store, shutdown));
        }

        // ── Prompt-and-response archive retention ──
        // Here rather than in the gateway, which is where the archive is
        // written: the gateway restarts on every rollout, and a retention that
        // only runs at startup bounds a row's life only while rollouts are more
        // frequent than the retention is wide. Runs beside the rollup on the
        // same store, and publishes what it retired so the promise is
        // checkable rather than merely declared.
        if let Some(store) = self.usage_store.clone() {
            let shutdown = ctx
                .consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            let keep = std::time::Duration::from_secs(
                crate::exchange_retention::keep_days_from_env() as u64 * 86_400,
            );
            let mut sweep =
                crate::exchange_retention::ExchangeRetention::new(store.pool().clone(), keep)
                    .with_deployment(crate::exchange_retention::deployment_name());
            if let Some(mb) = ctx.consume_service::<dyn cog_core::MetricsBackend>() {
                sweep = sweep.with_metrics(mb);
            }
            drop(Arc::new(sweep).spawn(shutdown));
        }

        // ── Token ledger retention ──
        // The ledger's rows are what the fold above sums, so the sweep may only
        // retire what the fold already covered: a row no window summed is the
        // only copy of that spend, and deleting it would leave the aggregate
        // reporting the hour as quiet forever. The sweep reads the fold's own
        // older end each pass and publishes what it could not touch, so a
        // backfill that stops shows up as rows past the retention that stay.
        if let Some(store) = self.usage_store.clone() {
            let shutdown = ctx
                .consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            let keep = std::time::Duration::from_secs(
                crate::usage_retention::keep_days_from_env() as u64 * 86_400,
            );
            let mut sweep = crate::usage_retention::UsageRetention::new(store.pool().clone(), keep)
                .with_deployment(crate::usage_retention::deployment_name());
            if let Some(mb) = ctx.consume_service::<dyn cog_core::MetricsBackend>() {
                sweep = sweep.with_metrics(mb);
            }
            drop(Arc::new(sweep).spawn(shutdown));
        }

        // ── Spend verdict fold ──
        // Runs here for the same reason the fold above does, and after it in
        // the same process: a verdict is read against the ledger directly
        // rather than against the folded rows, so the two do not have to be
        // ordered — but a deployment that restarts its gateway on every
        // rollout must not be the process that decides what a spend meant.
        if let Some(store) = self.verdict_store.clone() {
            let shutdown = ctx
                .consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            drop(crate::spend_verdict::spawn(store, shutdown));
        }

        // ── Data directory footprint ──
        let obs_cfg = crate::ObservabilityExportersConfig::load()?;
        for volume in &self.data_volume {
            let shutdown = ctx
                .consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            tokio::spawn(crate::data_volume::run_data_volume_watch(
                volume.clone(),
                obs_cfg.data_volume_watch.interval_secs,
                shutdown,
            ));
        }

        // ── Alert outlets: every channel this deployment can address ──
        // One list drives the bridge, the two notifiers below and the
        // announcement; a channel that is configured but whose address this
        // process cannot read would otherwise be announced as if it were live.
        let alert_channels = obs_cfg.alertmanager.channels();
        let alert_timeout = obs_cfg.alertmanager.timeout_secs;
        let alert_outlets = obs_cfg.alertmanager.channel_names();
        if alert_outlets.is_empty() {
            warn!(
                "No alert outlet configured: firing alerts reach only the persisted alerts \
                 table. Set alertmanager.webhook_url (COGNEVA_ALERTMANAGER_WEBHOOK_URL), \
                 alertmanager.slack.webhook_url (COGNEVA_ALERTMANAGER_SLACK_WEBHOOK_URL) or \
                 alertmanager.email.recipients + smtp_host (COGNEVA_ALERTMANAGER_EMAIL_*) to \
                 give them a receiver"
            );
        } else {
            info!(outlets = ?alert_outlets, "Alert outlets configured");
        }
        // The log line above dies with this pod. Publish the same list as a
        // reading so that "there is nowhere for an alert to go" survives as a
        // value instead of as an absence, before anything else in this block
        // can decide not to run.
        publish_alert_channels(
            ctx.consume_service::<dyn cog_core::MetricsBackend>(),
            &alert_outlets,
            self.alert_store.is_some(),
        )
        .await;

        // ── Alert bridge: notification outlets + persistent state machine ──
        let alert_store = self.alert_store.clone();

        // 站内铃铛的出口。两端都由别的插件发布，所以缺存储的部署在这里拿到
        // `None`，而不是一个会虚构收件人或写进没人读的表的东西。它同时喂给两条
        // 告警来源：supervisor 事件桥与 PromQL 的 infra watcher——现网在响的
        // 告警绝大多数是后者，只接前者的话铃铛仍然长期是空的。
        let bell = match (
            ctx.consume_service::<dyn cog_core::NotificationStore>(),
            ctx.consume_service::<dyn cog_core::UserStore>(),
        ) {
            (Some(store), Some(users)) => Some(Arc::new(InAppNotifier::new(store, users))),
            _ => None,
        };
        let bell_wired = bell.is_some();

        // ── 站内铃铛：把 firing 却还没通知过人的告警写成通知 ──
        // 单点生产者。`alerts` 表有四个写入方（supervisor 事件桥、PromQL 的 infra
        // watcher、配置声明检查、安全网关），在各自写入点直发就要四处各接一次，
        // 而且接上通知之前就已在响的告警永远补不上。扫表让"还没通知"成为表上的
        // 一列（`notified_at IS NULL`）：存量在部署后第一轮补发，写失败下一轮重试。
        if let (Some(store), Some(notifier)) = (self.alert_store.clone(), bell.clone()) {
            let shutdown = ctx
                .consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            let metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
            // 同一张 alerts 表前可能站着不止一个进程：主应用与进化进程都装了
            // 这个插件，两者都在写告警。通知是 INSERT，不像 `set_alert` 那样按
            // dedup_key 幂等——两个扫描者会把同一条告警各自写一行通知给同一个人。
            // 租约把"扫表那半个"收成一个持有者；没有仲裁者的部署拿到的租约恒成立，
            // 回到每个进程自己扫的行为（那时只有一个进程共享这张表）。
            let role = ctx.consume_service::<dyn cog_core::OwnerLeaseBroker>();
            let interval = std::time::Duration::from_secs(ALERT_BELL_INTERVAL_SECS);
            drop(cog_core::loop_health::spawn(
                ALERT_BELL_LOOP,
                cog_core::loop_health::Cadence::Periodic(interval),
                shutdown.clone(),
                move |beat| {
                    let store = store.clone();
                    let notifier = notifier.clone();
                    let metrics = metrics.clone();
                    let shutdown = shutdown.clone();
                    let role = role.clone();
                    async move {
                        let hold = cog_core::RoleHold::start(
                            role,
                            ALERT_BELL_ROLE,
                            beat.clone(),
                            &shutdown,
                        );
                        let mut ticker = tokio::time::interval(interval);
                        loop {
                            beat.beat();
                            tokio::select! {
                                _ = ticker.tick() => {
                                    if !hold.may_act().await {
                                        continue;
                                    }
                                    match crate::bell::drain_pending(
                                        store.as_ref(),
                                        &notifier,
                                        metrics.as_ref(),
                                        ALERT_BELL_BATCH,
                                    )
                                    .await
                                    {
                                        Ok(0) => {}
                                        Ok(n) => info!(
                                            notified = n,
                                            "firing alerts announced in the notification centre"
                                        ),
                                        Err(e) => warn!(
                                            error = %e,
                                            "the bell could not read the alerts still waiting to be announced"
                                        ),
                                    }
                                }
                                _ = shutdown.wait() => return,
                            }
                        }
                    }
                },
            ));
        } else if !bell_wired {
            info!("notification centre disabled: no notification store or no user store published");
        }

        if alert_store.is_some() || !alert_channels.is_empty() {
            match ctx.consume_service::<dyn cog_core::Supervisor>() {
                Some(supervisor) => {
                    let http_client = ctx.consume_service::<dyn cog_core::HttpClient>();
                    if !alert_channels.is_empty() && http_client.is_none() {
                        info!("alert outlets configured but no HttpClient; alerts persist only");
                    }
                    let persistent_store = alert_store.is_some();
                    // 停机信号：拿得到就挂在进程的停机信号上（触发后退出的那一次
                    // 不算「循环死了」）；拿不到就是这套部署没有发布停机信号，
                    // 那它拿到的是一个永不触发的信号，语义等同于「没有人给过它
                    // 停的办法」。
                    let bridge_stop = ctx
                        .consume::<cog_core::ShutdownSignal>()
                        .map(|s| (*s).clone())
                        .unwrap_or_default();
                    let bridged = spawn_alert_bridge(
                        alert_channels.clone(),
                        alert_timeout,
                        http_client,
                        alert_store,
                        &supervisor,
                        ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                        // Ten health passes. The row's liveness clock moves on
                        // every pass for an agent the registry still tracks, so
                        // this many misses cannot be a busy pass -- only the id
                        // having left the registry, or a previous process life.
                        std::time::Duration::from_secs(
                            ctx.config().supervisor.health_interval_secs.max(1) * 10,
                        ),
                        bridge_stop,
                    );
                    // Report both outlets, not just the HTTP ones. With no
                    // outlet wired the bridge still runs and still persists, so
                    // logging only on a live webhook left "is the bridge
                    // running?" with no answer in the logs.
                    info!(
                        delivered = bridged.is_some(),
                        persistent_store,
                        "alert bridge started: supervisor events fan out to these outlets"
                    );
                }
                None => info!("alert bridge skipped: no Supervisor available"),
            }
        } else {
            info!("alert bridge disabled: no alert outlet and no PostgreSQL store");
        }

        // ── Delivered configuration document vs. this revision's ──
        // The alert rules travel inside that document, so no rule in it can
        // report the document's own absence or lag; this judgement runs in code
        // instead, on the document this process read at start, against the copy
        // compiled into this binary.
        let declaration_notifier = alert_manager(
            Vec::new(),
            &alert_channels,
            alert_timeout,
            ctx.consume_service::<dyn cog_core::HttpClient>(),
            ctx.consume_service::<dyn cog_core::MetricsBackend>(),
        );
        // 停机信号：拿到就挂在进程的停机信号上，拿不到就得到一个永不触发的信号
        // ——与告警桥同一条约定。这一支在判词落成 firing 之后会一直按节拍复述，
        // 没有停机信号它就只在这条进程结束时停。
        let declaration_stop = ctx
            .consume::<cog_core::ShutdownSignal>()
            .map(|s| (*s).clone())
            .unwrap_or_default();
        tokio::spawn(crate::config_delivery::run_config_declaration_check(
            std::time::Duration::from_secs(
                obs_cfg
                    .config_declaration
                    .settle_secs
                    .max(crate::config::ConfigDeclarationConfig::MIN_SETTLE_SECS),
            ),
            std::time::Duration::from_secs(
                obs_cfg
                    .config_declaration
                    .hold_secs
                    .max(crate::config::ConfigDeclarationConfig::MIN_HOLD_SECS),
            ),
            self.alert_store.clone(),
            declaration_notifier,
            ctx.consume_service::<dyn cog_core::MetricsBackend>(),
            declaration_stop,
        ));

        // ── Infra alert watcher: PromQL rules → persisted alerts ──
        // Infrastructure faults (node disk pressure, crash loops) happen
        // below the supervisor's view; without this they never become
        // persisted alerts and self-discovery stays blind to them.
        let infra = obs_cfg.infra_watch.clone();
        if infra.enabled && !infra.prometheus_url.is_empty() && !infra.rules.is_empty() {
            match ctx.consume_service::<dyn cog_core::HttpClient>() {
                Some(http) => {
                    // Convert infra rules into AlertRule entries so payloads
                    // resolve rule summaries.
                    let rules = infra
                        .rules
                        .iter()
                        .map(|r| {
                            cog_core::alerts::AlertRuleBuilder::new(
                                r.name.clone(),
                                r.promql.clone(),
                            )
                            .condition(r.condition)
                            .severity(r.severity)
                            .summary(r.summary.clone())
                            .build()
                        })
                        .collect();
                    let notifier = alert_manager(
                        rules,
                        &alert_channels,
                        alert_timeout,
                        Some(http.clone()),
                        ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                    );
                    let shutdown = ctx
                        .consume::<cog_core::ShutdownSignal>()
                        .map(|s| (*s).clone())
                        .unwrap_or_default();
                    let outlets = crate::infra_watch::InfraWatchOutlets {
                        store: self.alert_store.clone(),
                        notifier,
                        metrics: ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                    };
                    tokio::spawn(crate::infra_watch::run_infra_watch_loop(
                        infra, outlets, http, shutdown,
                    ));
                }
                None => warn!("infra watch configured but no HttpClient; disabled"),
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("ObservabilityPlugin shutdown");
        Ok(())
    }
}

/// Convert a [`SupervisorEvent`] into [`AlertEvent`]s for Alertmanager webhook dispatch.
fn supervisor_event_to_alert_events(event: &cog_core::SupervisorEvent) -> Vec<AlertEvent> {
    // LLM 上游池不可用是"派不动活"级别的故障：不补可联通的上游，整条 LLM
    // 依赖链停摆，所以路由到通知出口（Critical），恢复时自动 resolve。
    match event {
        cog_core::SupervisorEvent::LlmUpstreamPoolDown {
            evidenced_recovery_unix,
            next_attempt_unix,
            unavailable,
            pool_size,
            declared_min_healthy,
            quota_window_secs,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("alert_type".into(), "llm_upstream_pool_down".into());
            // 两个数各占一个标签，名字自带口径：`suspect_count` 是**当刻还在嫌疑
            // 窗内**的家数（随窗到期自己变小），`pool_size` 才是池有多大。只报前
            // 者、还把它叫 `unavailable_count`，读的人会以为池就这么大。
            labels.insert("suspect_count".into(), unavailable.len().to_string());
            labels.insert("pool_size".into(), pool_size.to_string());
            // 第三个数：这条判定是按哪条界做出来的。池判定收在**容量**上之后，
            // "3 of 4 在嫌疑窗内"与"4 of 4"可以是同一个判定（声明要求两家时，
            // 剩两家就是够的），而只报前两个数的读者会把它读成"还剩一家在供，
            // 为什么派活停了"。界与它的来源一起给，值不是别处抄的。
            labels.insert(
                "min_healthy".into(),
                cog_core::pool_floor(*pool_size, *declared_min_healthy).to_string(),
            );
            labels.insert(
                "min_healthy_source".into(),
                cog_core::min_healthy_source(*declared_min_healthy).to_string(),
            );
            labels.insert(
                "evidenced_recovery_unix".into(),
                evidenced_recovery_unix.to_string(),
            );
            labels.insert("next_attempt_unix".into(), next_attempt_unix.to_string());
            labels.insert("quota_window_secs".into(), quota_window_secs.to_string());
            labels.insert("unavailable".into(), unavailable.join(","));
            // 文案在契约层只写一次，共用它的消费者是**这两个**：这里的通知出口，
            // 和监督者那条进程内告警史（`cog-supervisor::alert_store`，容量
            // `ALERT_HISTORY_MAX`）。各写一遍就会漂移，而漂移是静默的——两句都读
            // 得通，没有一层会报错，读的人也无从判断该信哪句；时刻与窗口长度同样
            // 由那一句分开措辞。
            //
            // 「共用同一句」的范围到此为止：池不可用**落进 PG 告警表**那一行不在里
            // 面，它由 `cog-gateway::security_gateway::pool_down_message` 本地措辞
            // （并且点名在嫌疑窗内的上游，这一句不含名单）。两边落在不同载体上，
            // `/alerts` 那条读口会把同规则的两条合成一条，理由与挑哪一条见
            // `POOL_ALERT_RULE` 那段注释。
            labels.insert(
                "message".into(),
                cog_core::pool_down_verdict(
                    *pool_size,
                    unavailable,
                    *declared_min_healthy,
                    *evidenced_recovery_unix,
                    *next_attempt_unix,
                    *quota_window_secs,
                ),
            );
            let inst = AlertInstance {
                rule_name: "llm_upstream_pool_down".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Critical,
                value: unavailable.len() as f64,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: Utc::now(),
            };
            return vec![AlertEvent::Firing(inst)];
        }
        cog_core::SupervisorEvent::LlmUpstreamPoolRecovered { timestamp } => {
            let mut labels = HashMap::new();
            labels.insert(
                "message".into(),
                "LLM upstream pool recovered; LLM-dependent tasks resumed".into(),
            );
            let inst = AlertInstance {
                rule_name: "llm_upstream_pool_down".into(),
                labels,
                state: AlertState::Resolved,
                severity: AlertSeverity::Info,
                value: 0.0,
                starts_at: *timestamp,
                ends_at: Some(Utc::now()),
                updated_at: Utc::now(),
            };
            return vec![AlertEvent::Resolved(inst)];
        }
        _ => {}
    }

    let now = Utc::now();
    match event {
        cog_core::SupervisorEvent::AgentUnhealthy {
            agent_id,
            issue,
            timestamp,
        } => {
            let (severity, msg, value) = match issue {
                cog_core::HealthIssue::Suspect { missed_beats } => (
                    AlertSeverity::Warning,
                    format!("Agent suspect: missed {missed_beats} beats"),
                    *missed_beats as f64,
                ),
                cog_core::HealthIssue::Dead { .. } => (
                    AlertSeverity::Critical,
                    "Agent declared dead".to_string(),
                    1.0,
                ),
                cog_core::HealthIssue::Stuck { stuck_seconds } => (
                    AlertSeverity::Warning,
                    format!("Agent stuck for {stuck_seconds}s"),
                    *stuck_seconds as f64,
                ),
                cog_core::HealthIssue::StateBackendDead => (
                    AlertSeverity::Critical,
                    "Agent dead (state backend)".to_string(),
                    1.0,
                ),
            };
            let mut labels = HashMap::new();
            labels.insert("agent_id".into(), agent_id.clone());
            labels.insert("alert_type".into(), "agent_unhealthy".into());
            labels.insert("message".into(), msg);
            let inst = AlertInstance {
                rule_name: "agent_unhealthy".into(),
                labels,
                state: AlertState::Firing,
                severity,
                value,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::QuotaThresholdBreached {
            workspace_id,
            remaining,
            threshold,
            scheduler_paused,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("workspace_id".into(), workspace_id.clone());
            labels.insert("alert_type".into(), "quota_threshold_breached".into());
            labels.insert("scheduler_paused".into(), scheduler_paused.to_string());
            labels.insert(
                "message".into(),
                format!(
                    "Quota breached: remaining={remaining}, threshold={}, paused={scheduler_paused}",
                    threshold
                ),
            );
            let inst = AlertInstance {
                rule_name: "quota_threshold_breached".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Critical,
                value: *remaining as f64,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::AgentResourceAlert {
            agent_id,
            metric,
            threshold,
            current,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("agent_id".into(), agent_id.clone());
            labels.insert("metric".into(), metric.clone());
            labels.insert("threshold".into(), threshold.to_string());
            labels.insert(
                "message".into(),
                format!("Resource alert: {metric}={current:.2}, threshold={threshold:.2}"),
            );
            let inst = AlertInstance {
                rule_name: "agent_resource_alert".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Warning,
                value: *current,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::TaskDeadLetter {
            task_id,
            agent_id,
            crew_id,
            retry_count,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("task_id".into(), task_id.clone());
            if let Some(aid) = agent_id {
                labels.insert("agent_id".into(), aid.clone());
            }
            if let Some(cid) = crew_id {
                labels.insert("crew_id".into(), cid.clone());
            }
            labels.insert(
                "message".into(),
                format!("Task sent to DLQ after {retry_count} retries"),
            );
            let inst = AlertInstance {
                rule_name: "task_dead_letter".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Critical,
                value: *retry_count as f64,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::SquadRespawnRequested {
            crew_id,
            reason,
            timestamp,
            ..
        } => {
            let mut labels = HashMap::new();
            labels.insert("crew_id".into(), crew_id.clone());
            labels.insert(crate::LABEL_REASON.into(), reason.clone());
            labels.insert(
                "message".into(),
                format!("Squad respawn requested: {reason}"),
            );
            let inst = AlertInstance {
                rule_name: "squad_respawn_requested".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Warning,
                value: 1.0,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        // An agent that has left the platform cannot be unhealthy any more.
        // The supervisor reaps one whose heartbeat has been gone past the grace
        // period; without this the row it left behind stays `firing` with a
        // clock nothing moves, which reads exactly like an agent that is still
        // down. `AgentKilled` is also what an operator kill emits, and the same
        // reasoning holds there: the subject is gone, so its row is settled.
        cog_core::SupervisorEvent::AgentKilled {
            agent_id,
            reason,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("agent_id".into(), agent_id.clone());
            labels.insert(
                "message".into(),
                format!("Agent left the platform: {reason}"),
            );
            let inst = AlertInstance {
                rule_name: "agent_unhealthy".into(),
                labels,
                state: AlertState::Resolved,
                severity: AlertSeverity::Info,
                value: 0.0,
                starts_at: *timestamp,
                ends_at: Some(now),
                updated_at: now,
            };
            vec![AlertEvent::Resolved(inst)]
        }
        cog_core::SupervisorEvent::AgentRecovered {
            agent_id,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("agent_id".into(), agent_id.clone());
            labels.insert("message".into(), "Agent recovered".into());
            let inst = AlertInstance {
                rule_name: "agent_unhealthy".into(),
                labels,
                state: AlertState::Resolved,
                severity: AlertSeverity::Info,
                value: 0.0,
                starts_at: *timestamp,
                ends_at: Some(now),
                updated_at: now,
            };
            vec![AlertEvent::Resolved(inst)]
        }
        cog_core::SupervisorEvent::QuotaRecovered {
            workspace_id,
            remaining,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("workspace_id".into(), workspace_id.clone());
            labels.insert(
                "message".into(),
                format!("Quota recovered: remaining={remaining}"),
            );
            let inst = AlertInstance {
                rule_name: "quota_threshold_breached".into(),
                labels,
                state: AlertState::Resolved,
                severity: AlertSeverity::Info,
                value: *remaining as f64,
                starts_at: *timestamp,
                ends_at: Some(now),
                updated_at: now,
            };
            vec![AlertEvent::Resolved(inst)]
        }
        _ => vec![],
    }
}

/// The phrase the governance face table uses for "an alert fired here has
/// nowhere to go".
///
/// Published as a `readout` label on the family below, not as a metric name:
/// the face table records what a reading *says*, and the reader tries that
/// phrase both as a series name and as a `readout` value. A phrase that lives
/// only as prose in the table resolves to nothing, and "nothing" prints the
/// same as "not measured" — so the one state a reader most needs to see, an
/// alert with no outlet, was the one the face could not read.
pub const ALERT_CHANNEL_NONE_READOUT: &str = "alert_notifications_channel_none";

/// Publish which outlets this deployment can deliver through, one series per
/// configured kind at 1.
///
/// An empty list still writes one row, but it is the verdict row
/// (`state="verdict"`, no `channel` label) and it says 1, not a placeholder at
/// `channel="none"`. The family has to say something when there is no outlet:
/// the dispatch counter only moves when a batch is sent, so a quiet deployment
/// with nothing configured and a quiet deployment with a working webhook are the
/// same absence on every other series. The per-profile value is a constant per
/// process, not an accumulation, so a reader takes one series rather than adding
/// the copies.
async fn publish_alert_channels(
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    outlets: &[&str],
    in_app: bool,
) {
    let Some(metrics) = metrics else { return };
    // The verdict cell answers "is there anywhere to send an alert" as 1 = no
    // outlet, and it is written on both paths. The in-app surface counts as an
    // outlet: it has no address, so nothing about it can be misconfigured, and
    // the WebUI reads the same rows an external channel would carry. Without
    // that half, a deployment whose alerts are all visible in the product still
    // reported "nowhere to send an alert". Deriving it from the per-kind
    // rows would leave it unwritten in exactly the state it reports: the rows
    // exist only when there is an outlet. It carries no `channel` label and
    // the per-kind rows carry `state="configured"`, so the two kinds are told
    // apart by `state` alone -- which the family's help says to select on.
    // That matters because the two polarities are opposite: a bare-name sum
    // over the family answers "yes" whether or not an outlet exists, so the
    // distinction has to survive in the labels, not only in the prose.
    let verdict = HashMap::from([
        ("state".to_string(), "verdict".to_string()),
        (
            crate::alerts::LABEL_READOUT.to_string(),
            ALERT_CHANNEL_NONE_READOUT.to_string(),
        ),
    ]);
    if let Err(e) = metrics
        .record_gauge(
            cog_core::metric_names::ALERT_CHANNELS_CONFIGURED,
            if outlets.is_empty() && !in_app {
                1.0
            } else {
                0.0
            },
            verdict,
        )
        .await
    {
        warn!(error = %e, "could not record the alert outlet verdict");
    }
    if outlets.is_empty() {
        // Nothing to list. Writing a placeholder row here (`channel="none"` at
        // 0) would put a second meaning on the `channel` label: an outlet that
        // exists is 1, and the placeholder would mean "no outlets" at 0, so a
        // reader grouping by `channel` would answer "the none kind is not
        // configured" -- which is true and useless, and says nothing about the
        // state the row was meant to announce. The verdict row above is where
        // that state lives, and it is written on this path too.
        return;
    }
    for outlet in outlets {
        let labels = HashMap::from([
            ("state".to_string(), "configured".to_string()),
            ("channel".to_string(), (*outlet).to_string()),
        ]);
        if let Err(e) = metrics
            .record_gauge(
                cog_core::metric_names::ALERT_CHANNELS_CONFIGURED,
                1.0,
                labels,
            )
            .await
        {
            warn!(channel = %outlet, error = %e, "could not record the alert outlet reading");
        }
    }
}

/// The dispatcher for a set of channels, or `None` when there is nothing to
/// deliver to.
///
/// A configured outlet without an HTTP client cannot deliver anything, and
/// that is the caller's state to report rather than something to paper over:
/// returning a manager here would make the bridge believe it has an outlet.
fn alert_manager(
    rules: Vec<cog_core::alerts::AlertRule>,
    channels: &[AlertChannel],
    timeout_secs: u64,
    client: Option<Arc<dyn cog_core::HttpClient>>,
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
) -> Option<Arc<crate::alerts::AlertManager>> {
    if channels.is_empty() {
        return None;
    }
    let client = client?;
    let manager = crate::alerts::AlertManager::new(rules, channels.to_vec())
        .with_timeout(timeout_secs)
        .with_client(client);
    Some(Arc::new(match metrics {
        Some(m) => manager.with_metrics(m),
        None => manager,
    }))
}

/// Spawn the alert bridge: subscribe to `SupervisorEvent`s, map each one once,
/// and feed both outlets — every configured alert channel and the persistent
/// state machine (PostgreSQL `alerts` table). One mapping result drives both,
/// so what the notification says and what is stored agree.
/// `consumer` 标签值，写在 [`cog_core::metric_names::EVENT_BUS_LAGGED_TOTAL`] 上。
const LAG_CONSUMER_ALERT_BRIDGE: &str = "alert_bridge";

/// 循环名：告警桥那条消费环，报在 `cogneva_loop_*` 的 `loop` 标签上。
///
/// 与它落后时写的 `consumer` 标签值（[`LAG_CONSUMER_ALERT_BRIDGE`]）后半段同字面：
/// 两个面说的是同一个消费者，前缀标的是它在哪个组件里（与 `observability_*`
/// 那族循环名同约定）。
pub const ALERT_BRIDGE_LOOP: &str = "observability_alert_bridge";

/// 通知中心那条扫表循环的名字，报在 `cogneva_loop_*` 的 `loop` 标签上。
pub const ALERT_BELL_LOOP: &str = "observability_alert_bell";

/// 扫表这半个循环在仲裁者里的角色名：同一张库上只许一个进程把告警变成通知。
pub const ALERT_BELL_ROLE: &str = "alert_bell";

/// 多久扫一次"在 firing、还没通知过人"的告警。
///
/// 与前端 `useNotifications` 的轮询（60 秒）同一量级：更慢会让铃铛比轮询晚
/// 一整拍才亮，更快只是空转——浏览器不会比这更早看到。
const ALERT_BELL_INTERVAL_SECS: u64 = 60;

/// 一轮最多发多少条。存量补发时不要一次写几千行把这一轮的墙钟撑破。
const ALERT_BELL_BATCH: i64 = 200;

// 参数多是因为这一环的每个入参都来自不同的插件服务，把它们捆成一个结构体只是把
// 「谁给了什么」藏进构造点；这里逐个点名反而更好读。
#[allow(clippy::too_many_arguments)]
fn spawn_alert_bridge<S>(
    channels: Vec<AlertChannel>,
    timeout_secs: u64,
    http_client: Option<Arc<dyn cog_core::HttpClient>>,
    store: Option<Arc<S>>,
    supervisor: &Arc<dyn cog_core::Supervisor>,
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    // How long a firing row of a per-process rule may go unrefreshed before
    // the bridge closes it. Several health passes, so a live pass always
    // beats it to the row.
    row_stale_after: std::time::Duration,
    stop: cog_core::ShutdownSignal,
) -> Option<Arc<crate::alerts::AlertManager>>
where
    S: AlertRowStore + 'static,
{
    let manager = alert_manager(
        Vec::new(),
        &channels,
        timeout_secs,
        http_client,
        metrics.clone(),
    );
    // 订阅一次、跨重启共用：body 每次尝试都重建，重新订阅会落在通道当前的尾巴上，
    // 环死期间到达的事件就再也补不回来——而那正是它最该补上的那一段。
    let alert_rx = Arc::new(tokio::sync::Mutex::new(supervisor.subscribe()));
    let manager_for_task = manager.clone();
    // 桥只认这张表要的两件事，不认它是谁实现的：它到底按不按节拍去收，得能在没有
    // 数据库的地方量出来。
    let store: Option<Arc<dyn AlertRowStore>> = store.map(|s| s as Arc<dyn AlertRowStore>);
    let sweep_every = sweep_period(row_stale_after);
    // 这条桥以前是个裸 `tokio::spawn`：它退出之后读数面上什么都没有——**告警从此
    // 不再落库**，而数据库里那张表看起来与「这段时间没有告警」同形。登记之后，
    // 它的退出落进 `cogneva_loop_deaths_total`。
    let _bridge_loop = cog_core::loop_health::spawn(
        ALERT_BRIDGE_LOOP,
        cog_core::loop_health::Cadence::EventDriven,
        stop.clone(),
        move |beat| {
            run_alert_bridge(
                store.clone(),
                manager_for_task.clone(),
                metrics.clone(),
                Arc::clone(&alert_rx),
                row_stale_after,
                sweep_every,
                beat,
                stop.clone(),
            )
        },
    );
    manager
}

/// 这条桥负责收的、由**本进程自己的内存记账**维持的规则。
///
/// `agent_unhealthy` 点名的是「本进程的注册表多久没听见这个 agent」：注册表在内存里，
/// 只有同一个进程的下一拍会再提起它。进程重启后它从空开始，于是重启前那一批 id 再也
/// 不会被谁提起，它们的行就永远停在 firing。2026-10-24 实测 3992 行；2026-10-26 又是
/// 一个两天的窗口里两万行，当下 firing 的 754 行全部由早就离开舰队的短命 squad 成员
/// 撑着，而真正的舰队只有个位数。判据是**存活钟**而不是规则名：一条 `last_seen_at`
/// 好几个健康周期没动过的行，说的就是这个进程没在看它。收掉它只在一个方向上可能猜错
/// ——真的还不健康的 agent 会在下一拍用同一个 dedup 键重新点亮，一个周期内自愈。
const PER_PROCESS_RULES: &[&str] = &["agent_unhealthy"];

/// 多久收一次「没人再刷新」的行。
///
/// 取判据的一半：收得比判据慢，一条死掉的行在表上多挂的时长就是这段节拍；取一半让
/// 最坏滞后不超过判据本身的一倍半。下限只管住「判据被配得极短」时不要把它变成对着表
/// 打的热循环。
fn sweep_period(row_stale_after: std::time::Duration) -> std::time::Duration {
    (row_stale_after / 2).max(MIN_SWEEP_PERIOD)
}

/// 收掉 [`PER_PROCESS_RULES`] 里 `row_stale_after` 这么久没人刷新的行，回答收了几行。
async fn close_unrefreshed_rows(
    store: &dyn AlertRowStore,
    row_stale_after: std::time::Duration,
    phase: &'static str,
) -> u64 {
    let stale_before = chrono::Utc::now()
        - chrono::Duration::from_std(row_stale_after)
            .unwrap_or_else(|_| chrono::Duration::seconds(600));
    let mut closed = 0u64;
    for rule in PER_PROCESS_RULES {
        match store.resolve_unrefreshed(rule, stale_before).await {
            Ok(0) => {}
            Ok(n) => {
                closed += n;
                warn!(
                    rule = %rule,
                    phase,
                    resolved = n,
                    "closed rows that no live health pass has refreshed; they belong to \
                     a previous life of this process, or to an id the fleet has since \
                     dropped, and nothing would ever have moved them again"
                );
            }
            Err(e) => warn!(
                rule = %rule,
                phase,
                error = %e,
                "could not close the rows left unrefreshed; the alert surface keeps \
                 showing them as live"
            ),
        }
    }
    closed
}

/// 一次唤醒的原因。分成两支而不是把两支的代码并进 `select!`：收行要在**放下**订阅锁
/// 之后做（它是一次数据库往返），而事件那一支必须拿着锁读下一帧。
///
/// 事件带一个盒子：`SupervisorEvent` 是这一层最大的枚举，不装盒子时每轮循环都要按
/// 它的大小过一遍栈，而这条环在事件风暴里是最紧的那条。
enum BridgeWake {
    Sweep,
    Event(Box<Result<cog_core::SupervisorEvent, tokio::sync::broadcast::error::RecvError>>),
}

/// 告警桥的循环体。
///
/// 从 [`spawn_alert_bridge`] 里拆出来只为可测：它只认一个接收端和一张表，不需要
/// supervisor，所以「它到底按不按节拍去收」可以在没有集群、没有数据库的地方量出来。
#[allow(clippy::too_many_arguments)]
async fn run_alert_bridge(
    store: Option<Arc<dyn AlertRowStore>>,
    manager: Option<Arc<crate::alerts::AlertManager>>,
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    alert_rx: Arc<tokio::sync::Mutex<tokio::sync::broadcast::Receiver<cog_core::SupervisorEvent>>>,
    row_stale_after: std::time::Duration,
    sweep_every: std::time::Duration,
    beat: cog_core::loop_health::Beat,
    stop: cog_core::ShutdownSignal,
) {
    // 先收掉上一个进程留下的按进程告警行。
    if let Some(store) = store.as_deref() {
        close_unrefreshed_rows(store, row_stale_after, "start").await;
    }
    // 进程活着的这段时间也要收，而且这才是常态的一半：舰队的名字本来就在轮换（短命
    // squad 成员、每次进化都换名的 agent）。一个 id 离开注册表之后，它的行既不会被本
    // 进程再刷新、也不会被谁收掉——只留启动那一收，行数就只在重启时才掉一次，而读数
    // 面在两班重启之间读到的是一个只涨不落的数。
    let mut sweep =
        tokio::time::interval_at(tokio::time::Instant::now() + sweep_every, sweep_every);
    // 落后时不补打：补打只会在一次长时间停顿之后连着收几遍同一段。
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // 三种结局分开写：`Lagged` 也是一个 `Err`，`while let Ok(..)` 会把它读成
    // 「订阅结束」，于是这条桥静静退出——告警**从此不再落库**，而数据库里那张
    // 表看起来与「这段时间没有告警」一模一样。这条桥没有别的存活读数。
    loop {
        // 每一轮都盖一次章，无论队列里有没有东西：事件驱动的环大部分时间
        // 停在空队列上，只在有事件时才盖，报出来的是队列的安静。
        beat.beat();
        let woken = {
            let mut rx = alert_rx.lock().await;
            tokio::select! {
                biased;
                _ = stop.wait() => return,
                // 收排在事件之前：事件风暴里它永远轮不到，而风暴正是行最多的时候。
                // 它只是一句 UPDATE，插进来的延迟以毫秒计。
                _ = sweep.tick(), if store.is_some() => BridgeWake::Sweep,
                received = rx.recv() => BridgeWake::Event(Box::new(received)),
            }
        };
        let received = match woken {
            BridgeWake::Sweep => {
                if let Some(store) = store.as_deref() {
                    close_unrefreshed_rows(store, row_stale_after, "tick").await;
                }
                continue;
            }
            BridgeWake::Event(received) => *received,
        };
        let event = match received {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(skipped, "alert bridge fell behind; it keeps consuming");
                if let Some(backend) = metrics.as_ref() {
                    let mut labels = std::collections::HashMap::new();
                    labels.insert(
                        "consumer".to_string(),
                        LAG_CONSUMER_ALERT_BRIDGE.to_string(),
                    );
                    if let Err(e) = backend
                        .record_counter(
                            cog_core::metric_names::EVENT_BUS_LAGGED_TOTAL,
                            skipped as f64,
                            labels,
                        )
                        .await
                    {
                        tracing::warn!(error = %e, "alert bridge: could not record the events it missed");
                    }
                }
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        };
        let alerts = supervisor_event_to_alert_events(&event);
        if alerts.is_empty() {
            continue;
        }
        // Persist first: the row is what the in-app surface shows,
        // so whether it was written decides whether this batch
        // reached anybody when no external channel exists. The bell
        // reads the same table on its own cadence; writing here is
        // what makes an alert visible there at all.
        let persisted = match &store {
            Some(store) => persist_alerts(store.as_ref(), &alerts).await,
            None => false,
        };
        match &manager {
            Some(manager) => manager.notify(&alerts).await,
            None if persisted => {
                crate::alerts::record_delivered_in_app(metrics.as_ref(), alerts.len()).await
            }
            // Neither an external outlet nor a row: nobody can be
            // told, and without this cell that absence is on no
            // series at all.
            None => crate::alerts::record_undelivered(metrics.as_ref(), alerts.len()).await,
        }
    }
}

/// Stable dedup key: re-evaluating the same condition must hit the same row in
/// the `alerts` table. Only labels that identify the alert instance take part;
/// values that change between evaluations (thresholds, messages) must stay out
/// or every tick would insert a new row. A resolve event has to compute the
/// same key as the firing event it closes.
fn alert_dedup_key(inst: &AlertInstance) -> String {
    let label = |key: &str| inst.labels.get(key).map(String::as_str).unwrap_or("");
    let identity = match inst.rule_name.as_str() {
        "agent_unhealthy" => label("agent_id").to_string(),
        "quota_threshold_breached" => label("workspace_id").to_string(),
        "agent_resource_alert" => format!("{}:{}", label("agent_id"), label("metric")),
        "task_dead_letter" => label("task_id").to_string(),
        "squad_respawn_requested" => label("crew_id").to_string(),
        _ => String::new(),
    };
    if identity.is_empty() {
        inst.rule_name.clone()
    } else {
        format!("{}:{}", inst.rule_name, identity)
    }
}

/// Rules whose row in the `alerts` table belongs to a different writer.
///
/// `llm_upstream_pool_down` is written every tick by the gateway's pool sync,
/// keyed by the pool-down *event* — the unix second the pool crossed its lower
/// edge — so one row can answer "how many times did the pool die", and the
/// recovery closes that row by the same name. This bridge sees the same
/// condition as supervisor events, and its key is just the rule name: persisting
/// it opens a **second** row for one event, and the recovery closes only the one
/// it was keyed to.
///
/// Measured 2026-10-23: `llm_upstream_pool_down@1792687465` opened 16:44:25 and
/// never resolved, while its bare-key twin opened 17:41:26 and resolved
/// 22:55:29 — one condition, two rows, one of them doomed to stay firing and to
/// count against every reader of the table (the attention verdict had it as the
/// oldest unattended row at 3.4 days).
///
/// The webhook still receives these events; only the row is not ours to write.
const PERSISTED_ELSEWHERE: &[&str] = &["llm_upstream_pool_down"];

/// Whether this bridge is the writer of the row for `rule`.
fn owns_the_row(rule: &str) -> bool {
    !PERSISTED_ELSEWHERE.contains(&rule)
}

/// Human-readable alert text. The mapping layer writes the text into the
/// `message` label (single source of truth for wording); this only falls back
/// so a stored row never carries an empty message.
fn alert_message(inst: &AlertInstance) -> String {
    match inst.labels.get("message") {
        Some(text) if !text.is_empty() => text.clone(),
        _ => format!("{} {}", inst.rule_name, inst.state.as_str()),
    }
}

/// Drive mapped alerts into the PostgreSQL state machine: `Firing` guarantees an
/// open `firing` row, `Resolved` closes the row with the same dedup key.
/// Re-evaluating is idempotent, so a restarting producer cannot pile up rows.
///
/// Returns whether every row in the batch was written: the caller uses that to
/// decide between "the in-app surface has it" and "nobody was told".
/// 监督者事件落成的那一行归哪个面。
///
/// 这些行不是从规则库来的，配置里没有一处替它们申报面，映射只在这里：事件的主题
/// 与「谁去处理」在同一个地方一次定完，两边不会各改各的。写进来的规则名对不上任何
/// 一条时返回 `None`——那一行会以无主的样子出现，这正是它该有的样子（读数按
/// 「缺面」点名），而不是默认派给某个面。
fn supervisor_face(rule_name: &str) -> Option<&'static str> {
    match rule_name {
        "agent_unhealthy" | "task_dead_letter" | "squad_respawn_requested" => Some("任务管理"),
        "quota_threshold_breached" | "llm_upstream_pool_down" => Some("token 管理"),
        "agent_resource_alert" => Some("资源管理"),
        _ => None,
    }
}

/// 落库标签：映射层带来的标签原样保留，再补上这一行的面。
///
/// 已有面（PrometheusRule 抄进来的那些行自带一格）不覆盖：写者自己的申报比这里的
/// 默认更权威，而这里只是替那些没有规则库可申报的行补上。
fn row_labels(inst: &AlertInstance) -> HashMap<String, String> {
    let mut labels = inst.labels.clone();
    if !labels.contains_key(cog_core::alerts::ALERT_FACE_LABEL) {
        if let Some(face) = supervisor_face(&inst.rule_name) {
            labels.insert(cog_core::alerts::ALERT_FACE_LABEL.into(), face.into());
        }
    }
    labels
}

/// Write a batch into the alert state machine.
///
/// Returns whether every row was written. The notification edge is not
/// extracted here: the bell drains `notified_at IS NULL` rows on its own
/// cadence, so every writer of this table — including the ones added later —
/// reaches the same bell without knowing about it.
async fn persist_alerts(store: &dyn AlertRowStore, alerts: &[AlertEvent]) -> bool {
    let mut all_written = true;
    for event in alerts {
        let (condition, inst) = match event {
            AlertEvent::Firing(inst) => (true, inst),
            AlertEvent::Resolved(inst) => (false, inst),
        };
        // A row another writer owns is not "failed to write": nothing was owed
        // here, and reporting it as undelivered would put a false miss on the
        // delivery series.
        if !owns_the_row(&inst.rule_name) {
            continue;
        }
        let alert = NewAlert {
            rule: inst.rule_name.clone(),
            dedup_key: alert_dedup_key(inst),
            severity: inst.severity.as_str().to_string(),
            message: alert_message(inst),
            labels: serde_json::to_value(row_labels(inst))
                .unwrap_or_else(|_| serde_json::json!({})),
        };
        if let Err(e) = store.set_alert(condition, &alert).await {
            warn!(rule = %alert.rule, error = %e, "alert persistence failed");
            all_written = false;
        }
    }
    all_written
}

/// Wrapper so [`crate::LogFilterHandle`] can be stored in [`cog_core::PluginContext`].
pub struct LogFilterHandleHolder(pub crate::LogFilterHandle);

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "observability",
    requires: &["net", "storage"],
    optional_requires: &[],
    factory: || Box::new(ObservabilityPlugin::new()),
};

#[cfg(test)]
mod tests {
    use super::*;

    /// One gauge write: its name, its value, and the labels it carried.
    ///
    /// Spelled out, the tuple is a `type_complexity` lint at `-D warnings`, and
    /// this gate runs with that: a shape this size has to be named once rather
    /// than repeated at every field and signature.
    type GaugeWrite = (String, f64, HashMap<String, String>);

    /// Holds every gauge write so a cell can read the series a scrape would.
    #[derive(Default)]
    struct Gauges {
        calls: std::sync::Mutex<Vec<GaugeWrite>>,
    }

    impl Gauges {
        /// `(channel, value)` per recorded write that names an outlet kind.
        ///
        /// Writes without a `channel` label are the family's verdict cell, not a
        /// kind: folding them in would make "which outlets exist" and "is there
        /// any outlet at all" the same list, and the two are read by different
        /// questions.
        fn outlets(&self) -> Vec<(String, f64)> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(name, value, labels)| {
                    assert_eq!(name, "cogneva_alert_channels_configured");
                    labels
                        .get("channel")
                        .map(|channel| (channel.clone(), *value))
                })
                .collect()
        }

        /// `(readout, value)` per recorded write that names itself a reading.
        fn readouts(&self) -> Vec<(String, f64)> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(_, value, labels)| {
                    labels
                        .get(crate::alerts::LABEL_READOUT)
                        .map(|readout| (readout.clone(), *value))
                })
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for Gauges {
        async fn record_gauge(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.calls
                .lock()
                .unwrap()
                .push((name.as_str().to_string(), value, labels));
            Ok(())
        }

        async fn record_counter(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn record_histogram(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<Utc>,
            _end: chrono::DateTime<Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_gauge_latest(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<Utc>,
            _end: chrono::DateTime<Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_counter_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }

        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<Utc>,
            _end: chrono::DateTime<Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> cog_core::SFResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn health_check(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// A deployment with no outlet must still answer on the series: the whole
    /// point of the family is that "nobody can be told" is a value, so a
    /// backend that is handed nothing would leave the reader with the
    /// absence this reading exists to remove. The answer is the verdict row
    /// alone -- no configured row is written, because there is no kind to
    /// name, and a made-up one would read as a kind that is switched off.
    /// Every row that names an outlet kind is `state="configured"`; every row
    /// that names itself a reading is `state="verdict"`. A row carrying one
    /// without the other would let a `state` selector miss it -- and since the
    /// two kinds answer with opposite polarity, a row the selector misses gets
    /// added to the other kind's sum.
    fn assert_states_are_paired(calls: &[GaugeWrite]) {
        // A loop over an empty set passes without looking at anything, and
        // "nothing was written" is exactly the failure this check exists to
        // catch -- a publisher that stopped writing would keep it green.
        assert!(
            !calls.is_empty(),
            "no row was written, so there is nothing to pair"
        );
        for (_, _, labels) in calls {
            let expected = if labels.contains_key("channel") {
                "configured"
            } else {
                "verdict"
            };
            assert_eq!(
                labels.get("state").map(String::as_str),
                Some(expected),
                "row labels {labels:?} do not pair with their kind"
            );
        }
    }

    #[tokio::test]
    async fn no_outlet_is_a_verdict_row_rather_than_no_series() {
        let backend = Arc::new(Gauges::default());
        publish_alert_channels(Some(backend.clone()), &[], false).await;
        assert_eq!(
            backend.outlets(),
            Vec::new(),
            "a deployment with no outlet must name no kind at all: a placeholder \
             row would be read as a kind that exists and is switched off"
        );
        assert_eq!(
            backend.readouts(),
            vec![(ALERT_CHANNEL_NONE_READOUT.to_string(), 1.0)]
        );
        assert_states_are_paired(&backend.calls.lock().unwrap());
        // The verdict row carries no `channel`: its value means "no outlet at
        // all" (1) and the per-kind rows' value means "this outlet is
        // configured" (1). Labelling it with a channel would put both meanings
        // on one label, and a query grouping by `channel` would add them.
        let calls = backend.calls.lock().unwrap();
        let verdict_row = calls
            .iter()
            .find(|(_, _, labels)| labels.contains_key(crate::alerts::LABEL_READOUT))
            .expect("the verdict row was not written");
        assert!(
            !verdict_row.2.contains_key("channel"),
            "the verdict row must not carry a channel label: {:?}",
            verdict_row.2
        );
    }

    /// The in-app surface is an outlet. A deployment with no external channel
    /// but a readable `alerts` table can still tell somebody, and the row that
    /// reports "nowhere to send an alert" must not call that a dangling state.
    #[tokio::test]
    async fn the_in_app_surface_counts_as_an_outlet_in_the_verdict() {
        let backend = Arc::new(Gauges::default());
        publish_alert_channels(Some(backend.clone()), &[], true).await;
        assert_eq!(
            backend.readouts(),
            vec![(ALERT_CHANNEL_NONE_READOUT.to_string(), 0.0)],
            "a readable in-app alert surface is somewhere an alert can go"
        );
        assert_states_are_paired(&backend.calls.lock().unwrap());
    }

    /// The verdict cell is written in both directions, and it is the direction
    /// that matters that would otherwise be missing: the per-kind rows exist
    /// only when there is an outlet, so a cell derived from them could never
    /// say "there is none". A reader that only ever sees this one cell must
    /// still be able to tell a deployment with an outlet from one without.
    #[tokio::test]
    async fn the_verdict_cell_answers_with_an_outlet_configured_too() {
        let backend = Arc::new(Gauges::default());
        publish_alert_channels(Some(backend.clone()), &["webhook"], false).await;
        assert_eq!(
            backend.readouts(),
            vec![(ALERT_CHANNEL_NONE_READOUT.to_string(), 0.0)]
        );
        assert_states_are_paired(&backend.calls.lock().unwrap());
    }

    /// With outlets configured the family names them, one row per kind, so the
    /// reading answers *which* path an alert can take and not only whether one
    /// exists.
    #[tokio::test]
    async fn each_configured_outlet_is_its_own_row() {
        let backend = Arc::new(Gauges::default());
        publish_alert_channels(Some(backend.clone()), &["webhook", "email"], false).await;
        assert_eq!(
            backend.outlets(),
            vec![("webhook".to_string(), 1.0), ("email".to_string(), 1.0)]
        );
        assert_states_are_paired(&backend.calls.lock().unwrap());
    }

    /// No metrics service in the assembly is not an error to record: there is
    /// nowhere to write, and the caller must not fail the whole start over a
    /// reading.
    #[tokio::test]
    async fn a_deployment_without_a_metrics_backend_still_starts() {
        publish_alert_channels(None, &["webhook"], false).await;
    }

    fn inst(rule: &str, pairs: &[(&str, &str)], state: AlertState) -> AlertInstance {
        AlertInstance {
            rule_name: rule.into(),
            labels: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            state,
            severity: AlertSeverity::Critical,
            value: 1.0,
            starts_at: Utc::now(),
            ends_at: None,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn dedup_key_survives_resolve_and_ignores_volatile_labels() {
        let firing = cog_core::SupervisorEvent::AgentUnhealthy {
            agent_id: "a1".into(),
            issue: cog_core::HealthIssue::Dead {
                last_seen: Utc::now(),
            },
            timestamp: Utc::now(),
        };
        let recovered = cog_core::SupervisorEvent::AgentRecovered {
            agent_id: "a1".into(),
            timestamp: Utc::now(),
        };
        let key_of = |ev: &cog_core::SupervisorEvent| {
            let alerts = supervisor_event_to_alert_events(ev);
            let Some(AlertEvent::Firing(i) | AlertEvent::Resolved(i)) = alerts.first() else {
                panic!("事件必须映射出告警");
            };
            alert_dedup_key(i)
        };
        // 触发与恢复必须落同一行，否则 resolve 关不掉。
        assert_eq!(key_of(&firing), key_of(&recovered));
        assert_eq!(key_of(&firing), "agent_unhealthy:a1");
    }

    /// 一个离开平台的 agent 不可能还「不健康」：回收它之后，它留下的那一行必须
    /// 有人关。不关的话 `state='firing'` 会一直挂着、`last_seen_at` 再也不动，
    /// 而运营读「现在有什么在响」读的正是那一面——一条已经不存在的 agent 会永远
    /// 显示成正在出问题。
    #[test]
    fn an_agent_that_left_the_platform_closes_its_unhealthy_row() {
        let firing = cog_core::SupervisorEvent::AgentUnhealthy {
            agent_id: "a1".into(),
            issue: cog_core::HealthIssue::Dead {
                last_seen: Utc::now(),
            },
            timestamp: Utc::now(),
        };
        let killed = cog_core::SupervisorEvent::AgentKilled {
            agent_id: "a1".into(),
            reason: "reaped: no heartbeat for 7200s".into(),
            timestamp: Utc::now(),
        };
        let alerts = supervisor_event_to_alert_events(&killed);
        let Some(AlertEvent::Resolved(inst)) = alerts.first() else {
            panic!("回收事件必须映射成一条恢复，否则那一行没人关");
        };
        assert_eq!(inst.rule_name, "agent_unhealthy");
        assert_eq!(
            alert_dedup_key(inst),
            alert_dedup_key(match supervisor_event_to_alert_events(&firing).first() {
                Some(AlertEvent::Firing(i)) => i,
                _ => panic!("已知不健康必须是一条 firing"),
            }),
            "关的那一行必须是开的同一行"
        );
    }

    #[test]
    fn dedup_key_separates_resource_metric_and_pool_rule() {
        let resource = inst(
            "agent_resource_alert",
            &[("agent_id", "a1"), ("metric", "cpu")],
            AlertState::Firing,
        );
        assert_eq!(alert_dedup_key(&resource), "agent_resource_alert:a1:cpu");
        // 池全灭告警没有实例标签，去重键就是规则名——**不是**网关那一侧写的键：
        // 网关按「这一次事件」开行（规则名 + 下沿的 unix 秒），恢复也照那个名字关。
        // 两边同写一条条件，键却是两个，所以这一侧根本不该落库（见 `owns_the_row`）。
        let pool = inst("llm_upstream_pool_down", &[], AlertState::Firing);
        assert_eq!(alert_dedup_key(&pool), "llm_upstream_pool_down");
    }

    /// 这条桥落的每一行都要带面：读者在 `alerts` 表里按「标签缺失或空白」判无主，
    /// 而这些规则名不在规则库里，没有别处替它们申报。
    #[test]
    fn every_row_the_bridge_persists_names_the_face_that_acts_on_it() {
        for (rule, face) in [
            ("agent_unhealthy", "任务管理"),
            ("task_dead_letter", "任务管理"),
            ("squad_respawn_requested", "任务管理"),
            ("quota_threshold_breached", "token 管理"),
            ("llm_upstream_pool_down", "token 管理"),
            ("agent_resource_alert", "资源管理"),
        ] {
            assert_eq!(supervisor_face(rule), Some(face), "{rule} 的面");
            let row = row_labels(&inst(rule, &[], AlertState::Firing));
            assert_eq!(
                row.get(cog_core::alerts::ALERT_FACE_LABEL)
                    .map(String::as_str),
                Some(face),
                "{rule} 落库时没带面"
            );
        }

        // 规则库来的一行自带面（清单上申报的），映射这里不覆盖它。
        let declared = inst(
            "CognevaSomeRule",
            &[(cog_core::alerts::ALERT_FACE_LABEL, "通信")],
            AlertState::Firing,
        );
        assert_eq!(
            row_labels(&declared)
                .get(cog_core::alerts::ALERT_FACE_LABEL)
                .map(String::as_str),
            Some("通信")
        );
        // 认不出的规则名不臆造一个面：无主就该读成无主。
        assert_eq!(supervisor_face("CognevaSomeRule"), None);
    }

    #[test]
    fn the_bridge_does_not_write_the_row_the_gateway_owns() {
        // 池全灭那条行由网关按事件开、按同一个名字关。这条桥的键是裸规则名，
        // 同一次事件会在表里开出第二行，而恢复只关得掉其中一行——另一行就永远
        // firing，并计入每一个读表的人（2026-10-23 实测：旧那行 3.4 天没关）。
        assert!(!owns_the_row("llm_upstream_pool_down"));
        // 反向对照：没有第二个写者的规则仍然由这条桥落库。少了这一格，「跳过」
        // 写成「整批都不写」也会绿。
        assert!(owns_the_row("agent_unhealthy"));
        assert!(owns_the_row("task_dead_letter"));
        // 跳过的是那一行，不是这条告警：通知出口照旧收到它。
        let down = cog_core::SupervisorEvent::LlmUpstreamPoolDown {
            evidenced_recovery_unix: 0,
            next_attempt_unix: 0,
            unavailable: vec!["https://example.invalid/v1|m".into()],
            pool_size: 2,
            declared_min_healthy: Some(1),
            quota_window_secs: 3600,
            timestamp: Utc::now(),
        };
        assert!(matches!(
            supervisor_event_to_alert_events(&down).first(),
            Some(AlertEvent::Firing(_))
        ));
    }

    #[test]
    fn message_comes_from_label_with_rule_fallback() {
        let with_text = inst(
            "quota_threshold_breached",
            &[("message", "quota low")],
            AlertState::Firing,
        );
        assert_eq!(alert_message(&with_text), "quota low");
        let without = inst("agent_unhealthy", &[], AlertState::Resolved);
        assert_eq!(alert_message(&without), "agent_unhealthy resolved");
    }

    #[test]
    fn existing_alert_kinds_all_carry_message_and_identity() {
        // 存量告警必须与新告警一样具备落盘所需的文案与去重身份。
        let events = vec![
            cog_core::SupervisorEvent::AgentResourceAlert {
                agent_id: "a1".into(),
                metric: "cpu".into(),
                threshold: 0.9,
                current: 0.95,
                timestamp: Utc::now(),
            },
            cog_core::SupervisorEvent::TaskDeadLetter {
                task_id: "t1".into(),
                agent_id: None,
                crew_id: None,
                retry_count: 3,
                timestamp: Utc::now(),
            },
            cog_core::SupervisorEvent::SquadRespawnRequested {
                crew_id: "c1".into(),
                squad_id: None,
                reason: "unhealthy".into(),
                timestamp: Utc::now(),
            },
            cog_core::SupervisorEvent::QuotaThresholdBreached {
                workspace_id: "w1".into(),
                remaining: 1,
                threshold: 10,
                scheduler_paused: true,
                timestamp: Utc::now(),
            },
        ];
        for event in &events {
            let alerts = supervisor_event_to_alert_events(event);
            let Some(AlertEvent::Firing(i)) = alerts.first() else {
                panic!("事件必须映射出 firing 告警");
            };
            assert!(!alert_message(i).is_empty(), "文案不能为空");
            assert_ne!(
                alert_dedup_key(i),
                i.rule_name,
                "带实例标签的告警去重键要能区分实例"
            );
        }
    }
}

/// 告警桥的收行节拍。这条判据的坏法没有表面：桥停收之后，表里剩下的行与「舰队一直
/// 安静」同形，事件那一支照常工作，日志里也没有一行异常。
///
/// 所以这里量的是「它一直在收」，而不是「收一次对不对」——后者由 `alert_row_sweep`
/// 对着真库量（判据是存活钟、不是规则名）。
#[cfg(test)]
mod bridge_sweep {
    use super::*;
    use chrono::{DateTime, Utc};
    use std::sync::Mutex;
    use std::time::Duration;

    /// 记下每一次收行：哪条规则、判据截在哪个时刻。
    #[derive(Default)]
    struct RecordingStore {
        sweeps: Mutex<Vec<(String, DateTime<Utc>)>>,
    }

    #[async_trait::async_trait]
    impl AlertRowStore for RecordingStore {
        async fn set_alert(
            &self,
            _condition: bool,
            _alert: &NewAlert,
        ) -> anyhow::Result<crate::alert_store::AlertTransition> {
            panic!("这条测试不发 supervisor 事件，落库那一支不该被走到")
        }

        async fn resolve_unrefreshed(
            &self,
            rule: &str,
            stale_before: DateTime<Utc>,
        ) -> anyhow::Result<u64> {
            self.sweeps
                .lock()
                .unwrap()
                .push((rule.to_string(), stale_before));
            Ok(1)
        }

        async fn refresh_firing(&self, _dedup_key: &str) -> anyhow::Result<bool> {
            panic!("这条测试不发 supervisor 事件，落库那一支不该被走到")
        }
    }

    fn beat() -> cog_core::loop_health::Beat {
        cog_core::loop_health::register(
            "test_alert_bridge_sweep",
            cog_core::loop_health::Cadence::EventDriven,
        )
    }

    #[tokio::test]
    async fn the_bridge_keeps_closing_rows_while_it_runs() {
        let store = Arc::new(RecordingStore::default());
        // 发送端留在这里：接收端看到 `Closed` 时这条桥会退出，退出之后就不再收，
        // 那样这条测试会以「只收了一次」的方式绿不了也红不了。
        let (_tx, rx) = tokio::sync::broadcast::channel(8);
        let stop = cog_core::ShutdownSignal::new();
        let started = Utc::now();
        let task = tokio::spawn(run_alert_bridge(
            Some(store.clone() as Arc<dyn AlertRowStore>),
            None,
            None,
            Arc::new(tokio::sync::Mutex::new(rx)),
            Duration::from_secs(600),
            Duration::from_millis(20),
            beat(),
            stop.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let sweeps = store.sweeps.lock().unwrap().clone();
        stop.trigger();
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;

        assert!(
            sweeps.len() >= 3,
            "300ms 里只收了 {} 次：启动那一次之后就没人收了。舰队轮换掉的 id 会一直 \
             留在 firing 里，直到下一次进程重启",
            sweeps.len()
        );
        for (rule, cutoff) in &sweeps {
            assert_eq!(rule, "agent_unhealthy");
            // 每收一次都按「此刻往前推一个窗口」算判据，所以离本次启动刚好差这一个
            // 窗口，再减去这一收晚于启动的那点时间。
            let drift = chrono::Duration::seconds(600) - (started - *cutoff);
            assert!(
                drift.num_milliseconds().abs() <= 1000,
                "判据截在 {cutoff}，离启动 {:?}，不是 row_stale_after 之前的 600 秒",
                started - *cutoff
            );
        }
    }

    #[test]
    fn the_sweep_comes_round_faster_than_the_window_it_enforces() {
        for secs in [60u64, 120, 600, 3600] {
            let window = Duration::from_secs(secs);
            assert!(
                sweep_period(window) < window,
                "{secs}s 的判据配出了 {:?} 的节拍：一条死掉的行会比它自己的证据晚至少 \
                 一个整窗口才被收掉",
                sweep_period(window)
            );
        }
        assert_eq!(
            sweep_period(Duration::from_secs(600)),
            Duration::from_secs(300)
        );
        assert_eq!(
            sweep_period(Duration::from_secs(1)),
            Duration::from_secs(30),
            "判据被配得极短时节拍要有下限，否则就是对着表打热循环"
        );
    }
}
