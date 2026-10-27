//! Supervisor plugin — implements [`cog_core::SystemPlugin`].

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tracing::{info, warn};

/// Holder so `watch::Sender<SupervisorConfig>` can be stored in [`cog_core::PluginContext`].
pub struct SupervisorConfigTxHolder(pub tokio::sync::watch::Sender<crate::SupervisorConfig>);

/// 循环名：告警存储那条消费环，报在 `cogneva_loop_*` 的 `loop` 标签上。
///
/// 与它落后时写的 `consumer` 标签值（
/// [`crate::consumer_lag::LAG_CONSUMER_SUPERVISOR_ALERT_STORE`]）同字面：两个面
/// 说的是同一个消费者，字面一致才能把「它丢了事件」和「它停了」并排读。
pub const ALERT_STORE_LOOP: &str = "supervisor_alert_store";

/// Supervisor plugin that self-assembles the supervision lifecycle.
pub struct SupervisorPlugin {
    initialized: bool,
    supervisor: Option<Arc<crate::Supervisor>>,
    scheduler_gate: Option<Arc<crate::SchedulerGate>>,
    /// 池状态来源在 `init` 建好并发布，`start` 只把它接进守护循环。判定本身
    /// 由网关独占，这个来源是消费方共用的那一个观测面。
    pool_source: Option<Arc<dyn cog_core::LlmPoolStatusSource>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    task_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl SupervisorPlugin {
    /// Create a plugin that will build supervisor services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            supervisor: None,
            scheduler_gate: None,
            pool_source: None,
            shutdown_tx: Mutex::new(None),
            task_handle: Mutex::new(None),
        }
    }
}

impl Default for SupervisorPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl SupervisorPlugin {
    /// 建好并发布池状态来源。发布放在 `init` 而不是 `start`：`start` 是所有
    /// 插件并行跑的，摄取器在 `start` 里取这个服务，只有 `init` 阶段发布才
    /// 保证它一定取得到。同一个实例既驱动调度器的 LLM 类暂停，也供摄取器
    /// 判断"该不该拉下一条"——池的判定只有网关一个观测面，消费方共用这一个
    /// 来源而不是各读一遍存储。
    ///
    /// 来源建在存储层发布的那张键值面上，而不是自己按地址开一条连接：本插件
    /// 已经在 `requires` 里声明了 storage，读到的一定是本进程共用的那一张面。
    /// 拿不到面就是这套部署没有共享存储（单进程），池判定本来就没有第二个
    /// 读者，守护循环随之不启动。
    async fn init_llm_pool_source(&mut self, ctx: &cog_core::PluginContext) {
        let Some(store) = ctx.consume_service::<dyn cog_core::KeyValueStore>() else {
            info!("LLM pool guard disabled: no shared key-value face");
            return;
        };
        let source: Arc<dyn cog_core::LlmPoolStatusSource> =
            Arc::new(crate::llm_pool_guard::KeyValuePoolStatusSource::new(store));
        ctx.publish_service(source.clone());
        self.pool_source = Some(source);
    }

    /// Start the LLM upstream pool awareness loop. Needs both the scheduler
    /// gate and a Redis URL (the gateway publishes the pool snapshot there);
    /// without Redis this is a single-process deployment, so the loop is not
    /// started and the gate stays under operator control only.
    fn spawn_llm_pool_guard(&self, ctx: &cog_core::PluginContext) {
        let (Some(gate), Some(source)) = (self.scheduler_gate.clone(), self.pool_source.clone())
        else {
            return;
        };
        let Some(event_tx) =
            ctx.consume::<tokio::sync::broadcast::Sender<cog_core::SupervisorEvent>>()
        else {
            warn!("LLM pool guard disabled: no SupervisorEvent sender published");
            return;
        };
        let interval_secs = std::env::var("COGNEVA_LLM_POOL_CHECK_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30)
            .max(5);
        let guard = Arc::new(crate::llm_pool_guard::LlmPoolGuard::new(source, gate));
        guard.spawn(
            (*event_tx).clone(),
            std::time::Duration::from_secs(interval_secs),
        );
        info!(interval_secs, "LLM pool guard started");
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for SupervisorPlugin {
    fn name(&self) -> &'static str {
        "supervisor"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        // Snapshot config values to drop immutable borrow before publishing.
        let (
            supervisor_config,
            _observability_event_channel_capacity,
            alert_history_max,
            dag_executor_workspace_id,
        ) = {
            let config = ctx.config();
            (
                config.supervisor.clone(),
                config.system.observability_event_channel_capacity,
                config.supervisor.alert_history_max,
                config.dag_executor.workspace_id.clone(),
            )
        };

        // ── Consume dependencies ──
        let quota_manager = ctx.require_service::<dyn cog_core::WorkspaceQuotaSource>()?;
        let state_backend = ctx.require_service::<dyn cog_core::StateBackend>()?;
        let supervisor_orchestrator = ctx.require_service::<dyn cog_core::OrchestratorControl>()?;
        let event_tx = ctx.require::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()?;
        let event_tx = (*event_tx).clone();
        let meta_learning = ctx.consume_service::<dyn cog_core::MetaLearning>();
        let fault_classifier = ctx.consume_service::<dyn cog_core::FaultClassifier>();
        // The gRPC command router, published by the protocol plugin. Required,
        // not optional: without it the lifecycle commands gateways expose to
        // the UI could only report that they did nothing.
        let lifecycle_server = ctx.require_service::<dyn cog_core::AgentLifecycleServer>()?;

        // ── Consume observability gateway ──
        let gateway = ctx
            .require_service::<dyn cog_core::ObservabilityGateway>()?
            .clone();

        // ── Build supervisor registry ──
        //
        // Attached to the shared agent registry when this deployment has one:
        // an agent's own pod is the only writer of presence, and the health
        // pass can only judge who it can see. Without the attachment the
        // supervisor's map stayed empty forever and every reading built on it
        // — the health pass, the status route, the process view — printed a
        // fleet of zero next to a roster of live workers.
        let mut supervisor_registry = crate::AgentRegistry::new()
            .with_heartbeat_history_max(supervisor_config.heartbeat_history_max);
        match ctx.consume_service::<dyn cog_core::AgentRegistry>() {
            Some(shared) => supervisor_registry = supervisor_registry.with_agent_registry(shared),
            None => info!("supervisor registry has no shared agent registry to read presence from"),
        }
        let supervisor_registry = Arc::new(supervisor_registry);
        ctx.publish(supervisor_registry.clone());
        info!("SupervisorPlugin registry published");

        // ── Build supervisor ──
        let (supervisor_config_tx, supervisor_config_rx) =
            tokio::sync::watch::channel(supervisor_config.clone().into());

        let scheduler_gate = Arc::new(crate::SchedulerGate::new());

        // Resolved once and cloned: the alert store's consumer loop below needs
        // the same handle and runs in a task of its own, and passing this call
        // straight into `with_metrics` moved it out of this scope -- which is
        // how that loop came to be the one production `Lagged` arm with nowhere
        // to say what it missed.
        let metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();

        let mut supervisor = crate::Supervisor::new(
            supervisor_config.into(),
            supervisor_registry.clone(),
            state_backend.clone(),
            supervisor_orchestrator,
            quota_manager,
            gateway,
            scheduler_gate.clone(),
            event_tx.clone(),
            meta_learning,
        )
        .with_config_watch(supervisor_config_rx)
        // Real dispatch path for the lifecycle commands the gateway exposes.
        .with_lifecycle_server(lifecycle_server)
        // The per-pass outcome reading needs the metrics backend; this plugin
        // declares `storage` in its descriptor, so that backend is published
        // before this `init` runs and the handle is available here.
        .with_metrics(metrics.clone());
        if let Some(classifier) = fault_classifier {
            supervisor = supervisor.with_fault_classifier(classifier);
        }
        let supervisor = Arc::new(supervisor);
        info!("Supervisor created");
        supervisor.track_workspace(dag_executor_workspace_id);

        // ── Build alert store ──
        let alert_store = Arc::new(crate::AlertStore::with_max_alerts(alert_history_max));
        // 订阅一次、跨重启共用：重启时重新订阅会落在通道当前的尾巴上，环死
        // 期间到达的告警就再也补不回来——而那正是它最该补上的那一段。
        let alert_event_rx = Arc::new(tokio::sync::Mutex::new(supervisor.subscribe()));
        let alert_store_for_task = alert_store.clone();
        let alert_lag_metrics = metrics.clone();
        // 这条环以前是个裸 `tokio::spawn`：它退出（订阅关闭）之后读数面上什么
        // 都没有——内存里那张告警表不再更新，而看起来与「这段时间没有值得告警
        // 的事」同形。登记之后它的退出落进 `cogneva_loop_deaths_total`。
        //
        // 停机信号取自插件上下文；取不到就是这套部署没有发布停机信号，那么它
        // 拿到的是一个永不触发的信号，语义等同于「没有人给过它停的办法」——
        // 每一次退出都算死。两种情况都比「退出不可见」好。
        // `consume` 交回的是 `Arc<ShutdownSignal>`（发布侧存的就是那个），这里取的
        // 是信号本身，所以多一层解引用克隆。
        let alert_stop = ctx
            .consume::<cog_core::ShutdownSignal>()
            .map(|s| (*s).clone())
            .unwrap_or_default();
        drop(cog_core::loop_health::spawn(
            ALERT_STORE_LOOP,
            cog_core::loop_health::Cadence::EventDriven,
            alert_stop.clone(),
            move |beat| {
                let store = alert_store_for_task.clone();
                let rx = Arc::clone(&alert_event_rx);
                let lag_metrics = alert_lag_metrics.clone();
                let stop = alert_stop.clone();
                async move { store.run(rx, lag_metrics, stop, beat).await }
            },
        ));

        // ── Publish everything ──
        ctx.publish_service(supervisor.clone() as Arc<dyn cog_core::Supervisor>);
        let alert_store_trait: Arc<dyn cog_core::AlertStore> = alert_store.clone();
        ctx.publish_service(alert_store_trait);
        ctx.publish_service(scheduler_gate.clone() as Arc<dyn cog_core::SchedulerGate>);
        ctx.publish_service(supervisor_registry.clone() as Arc<dyn cog_core::HeartbeatRegistry>);
        ctx.publish(Arc::new(SupervisorConfigTxHolder(supervisor_config_tx)));
        ctx.publish(Arc::new(supervisor.event_sender()));
        info!("SupervisorPlugin event sender published");

        // ── Publish binary switcher for self-evolution deployments ──
        let self_evolution = ctx.config().self_evolution.clone();
        if self_evolution.enabled {
            let switcher_config = crate::binary_switcher::BinarySwitcherConfig {
                binary_dir: PathBuf::from(&self_evolution.binary_dir),
                binary_name: cog_core::paths::APP_BINARY_NAME.into(),
                health_url: format!("http://127.0.0.1:{}/health", ctx.config().gateway.http_port),
                health_check_grace_period_secs: self_evolution.health_check_grace_period_secs,
                health_check_interval_secs: self_evolution.health_check_interval_secs,
                health_check_max_retries: self_evolution.health_check_max_retries,
                systemd_service_name: cog_core::paths::APP_BINARY_NAME.into(),
                sidecar_socket_path: crate::binary_switcher::sidecar_socket_path(),
            };
            let switcher: Arc<dyn cog_core::BinarySwitcher> =
                crate::binary_switcher::build_switcher(
                    &self_evolution.switch_mode,
                    switcher_config,
                );
            ctx.publish_service(switcher);
            info!(
                mode = %self_evolution.switch_mode,
                "SupervisorPlugin binary switcher published"
            );
        }

        // ── LLM upstream pool verdict source (published for consumers) ──
        self.init_llm_pool_source(ctx).await;

        self.supervisor = Some(supervisor);
        self.scheduler_gate = Some(scheduler_gate);
        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let supervisor = self.supervisor.clone().expect("supervisor not initialized");

        let handle = tokio::spawn(async move {
            supervisor
                .run(async move {
                    shutdown_rx.await.ok();
                })
                .await;
            info!("Supervisor task exited");
        });

        *self.shutdown_tx.lock().await = Some(shutdown_tx);
        *self.task_handle.lock().await = Some(handle);

        // ── LLM upstream pool awareness ──
        // Pool health is owned by the security gateway; this loop reads its
        // cross-process snapshot and pauses only the LLM-dependent class.
        self.spawn_llm_pool_guard(ctx);

        // ── Multi-backend consumer ──
        let mbc = ctx.config().multi_backend_consumer.clone();
        if mbc.enabled {
            // 回灌来源随事件面开关切换：events_on_bus 开启后 AgentEnd 只发
            // 事件面（JetStream），回灌必须从事件面读，否则 broadcast 上的
            // AgentEnd 彻底断流；关闭时维持全局后端（零回归）。
            let plane_backend = if mbc.events_on_bus {
                ctx.consume::<cog_core::EventPlaneBackend>()
                    .map(|h| h.0.clone())
            } else {
                None
            };
            let global_backend = ctx.consume_service::<dyn cog_core::MessageBackend>();
            let primary = plane_backend.or(global_backend);
            if let Some(backend) = primary {
                if let Some(event_tx) =
                    ctx.consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
                {
                    let consumer = crate::MultiBackendEventConsumer::new(
                        backend.clone(),
                        (*event_tx).clone(),
                        &mbc.channel,
                    )
                    .with_retry_interval(mbc.retry_interval_secs);
                    info!(
                        events_on_bus = mbc.events_on_bus,
                        "MultiBackendEventConsumer started"
                    );
                    consumer.spawn();
                }
            } else {
                info!("MultiBackendEventConsumer disabled: no message backend available");
            }
        } else {
            info!("MultiBackendEventConsumer disabled");
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.task_handle.lock().await.take() {
            if let Err(e) = handle.await {
                warn!("supervisor task shutdown error: {}", e);
            }
        }
        info!("supervisor plugin shutdown complete");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "supervisor",
    requires: &[
        "quota",
        "storage",
        "orchestrator",
        "stream",
        "observability",
        // Supplies the AgentLifecycleServer this plugin requires to dispatch
        // lifecycle commands. The protocol plugin requires nothing itself, so
        // this adds an ordering edge, not a cycle.
        "protocol",
    ],
    optional_requires: &["reflection"],
    factory: || Box::new(SupervisorPlugin::new()),
};
