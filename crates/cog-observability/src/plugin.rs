//! Observability plugin — implements [`cog_core::SystemPlugin`].

use chrono::Utc;
use cog_core::alerts::{AlertChannel, AlertEvent, AlertInstance, AlertSeverity, AlertState};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use crate::alert_store::{NewAlert, PostgresAlertStore};
use crate::usage_store::LlmUsageStore;

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
        let trace_collector = Arc::new(crate::snapshot::TraceCollector::new(trace_store.clone()));
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

        // ── Persistent alert state machine ──
        // Created here (not in start) so the read-side ActiveAlertSource is
        // published before any plugin's start runs — init_all completes
        // before start_all, and self-discovery consumes it in start.
        self.alert_store = match std::env::var("COGNEVA_DATABASE_URL") {
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
        self.usage_store = match std::env::var("COGNEVA_DATABASE_URL") {
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

        // ── Alert bridge: notification outlets + persistent state machine ──
        let alert_store = self.alert_store.clone();

        if alert_store.is_some() || !alert_channels.is_empty() {
            match ctx.consume_service::<dyn cog_core::Supervisor>() {
                Some(supervisor) => {
                    let http_client = ctx.consume_service::<dyn cog_core::HttpClient>();
                    if !alert_channels.is_empty() && http_client.is_none() {
                        info!("alert outlets configured but no HttpClient; alerts persist only");
                    }
                    let persistent_store = alert_store.is_some();
                    let bridged = spawn_alert_bridge(
                        alert_channels.clone(),
                        alert_timeout,
                        http_client,
                        alert_store,
                        &supervisor,
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
        );
        tokio::spawn(crate::config_delivery::run_config_declaration_check(
            std::time::Duration::from_secs(
                obs_cfg
                    .config_declaration
                    .settle_secs
                    .max(crate::config::ConfigDeclarationConfig::MIN_SETTLE_SECS),
            ),
            self.alert_store.clone(),
            declaration_notifier,
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
                    let notifier =
                        alert_manager(rules, &alert_channels, alert_timeout, Some(http.clone()));
                    let shutdown = ctx
                        .consume::<cog_core::ShutdownSignal>()
                        .map(|s| (*s).clone())
                        .unwrap_or_default();
                    let outlets = crate::infra_watch::InfraWatchOutlets {
                        store: self.alert_store.clone(),
                        notifier,
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
    // LLM 上游池全灭是"无人可用"级别的故障：不补可联通的上游，整条 LLM
    // 依赖链停摆，所以路由到通知出口（Critical），恢复时自动 resolve。
    match event {
        cog_core::SupervisorEvent::LlmUpstreamPoolDown {
            evidenced_recovery_unix,
            next_attempt_unix,
            unavailable,
            pool_size,
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
            labels.insert(
                "evidenced_recovery_unix".into(),
                evidenced_recovery_unix.to_string(),
            );
            labels.insert("next_attempt_unix".into(), next_attempt_unix.to_string());
            labels.insert("quota_window_secs".into(), quota_window_secs.to_string());
            labels.insert("unavailable".into(), unavailable.join(","));
            // 文案在契约层只写一次，所有消费者共用同一句：这里和告警历史各写一
            // 遍就会漂移，而漂移是静默的——两句都读得通，没有一层会报错，读的人
            // 也无从判断该信哪句。时刻与窗口长度同样由那一句分开措辞。
            labels.insert(
                "message".into(),
                cog_core::pool_down_verdict(
                    *pool_size,
                    unavailable,
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
            labels.insert("reason".into(), reason.clone());
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
) -> Option<Arc<crate::alerts::AlertManager>> {
    if channels.is_empty() {
        return None;
    }
    let client = client?;
    Some(Arc::new(
        crate::alerts::AlertManager::new(rules, channels.to_vec())
            .with_timeout(timeout_secs)
            .with_client(client),
    ))
}

/// Spawn the alert bridge: subscribe to `SupervisorEvent`s, map each one once,
/// and feed both outlets — every configured alert channel and the persistent
/// state machine (PostgreSQL `alerts` table). One mapping result drives both,
/// so what the notification says and what is stored agree.
fn spawn_alert_bridge(
    channels: Vec<AlertChannel>,
    timeout_secs: u64,
    http_client: Option<Arc<dyn cog_core::HttpClient>>,
    store: Option<Arc<PostgresAlertStore>>,
    supervisor: &Arc<dyn cog_core::Supervisor>,
) -> Option<Arc<crate::alerts::AlertManager>> {
    let manager = alert_manager(Vec::new(), &channels, timeout_secs, http_client);
    let mut alert_rx = supervisor.subscribe();
    let manager_for_task = manager.clone();
    tokio::spawn(async move {
        while let Ok(event) = alert_rx.recv().await {
            let alerts = supervisor_event_to_alert_events(&event);
            if alerts.is_empty() {
                continue;
            }
            if let Some(manager) = &manager_for_task {
                manager.notify(&alerts).await;
            }
            if let Some(store) = &store {
                persist_alerts(store, &alerts).await;
            }
        }
    });
    manager
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

/// Human-readable alert text. The mapping layer writes the text into the
/// `message` label (single source of truth for wording); this only falls back
/// so a stored row never carries an empty message.
fn alert_message(inst: &AlertInstance) -> String {
    match inst.labels.get("message") {
        Some(text) if !text.is_empty() => text.clone(),
        _ => format!(
            "{} {}",
            inst.rule_name,
            match inst.state {
                AlertState::Pending => "pending",
                AlertState::Firing => "firing",
                AlertState::Resolved => "resolved",
            }
        ),
    }
}

/// Drive mapped alerts into the PostgreSQL state machine: `Firing` guarantees an
/// open `firing` row, `Resolved` closes the row with the same dedup key.
/// Re-evaluating is idempotent, so a restarting producer cannot pile up rows.
async fn persist_alerts(store: &PostgresAlertStore, alerts: &[AlertEvent]) {
    for event in alerts {
        let (condition, inst) = match event {
            AlertEvent::Firing(inst) => (true, inst),
            AlertEvent::Resolved(inst) => (false, inst),
        };
        let alert = NewAlert {
            rule: inst.rule_name.clone(),
            dedup_key: alert_dedup_key(inst),
            severity: inst.severity.as_str().to_string(),
            message: alert_message(inst),
            labels: serde_json::to_value(&inst.labels).unwrap_or_else(|_| serde_json::json!({})),
        };
        if let Err(e) = store.set_alert(condition, &alert).await {
            warn!(rule = %alert.rule, error = %e, "alert persistence failed");
        }
    }
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

    #[test]
    fn dedup_key_separates_resource_metric_and_pool_rule() {
        let resource = inst(
            "agent_resource_alert",
            &[("agent_id", "a1"), ("metric", "cpu")],
            AlertState::Firing,
        );
        assert_eq!(alert_dedup_key(&resource), "agent_resource_alert:a1:cpu");
        // 池全灭告警没有实例标签，去重键就是规则名（与网关侧写入同键）。
        let pool = inst("llm_upstream_pool_down", &[], AlertState::Firing);
        assert_eq!(alert_dedup_key(&pool), "llm_upstream_pool_down");
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
