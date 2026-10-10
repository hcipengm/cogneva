//! Reflection plugin — implements [`cog_core::SystemPlugin`].
//! A host that gave no build slot is consumed as `ExecutedChange::NoBuildSlot`, not charged to the change's failure account.

use std::sync::Arc;
use tracing::{error, info, warn};

/// Loop name reported through the background-loop liveness family.
pub const MICROVM_EVOLUTION_LOOP: &str = "reflection_microvm_evolution";
/// Loop name reported through the background-loop liveness family.
pub const CHANGE_VERIFICATION_LOOP: &str = "reflection_change_verification";

/// 延迟持有的 LLM 上游池暂停句柄。`SchedulerGate` 由 supervisor 插件在
/// init 阶段发布，而 reflection 的 init 早于 supervisor（supervisor 可选依赖
/// reflection）；自进化循环在 init 阶段就已 spawn，因此用这个句柄跨越
/// init→start 的时间窗：init 建句柄、start 填充、循环只读取。gate 未就绪时
/// 视为不暂停（保持既有行为，不因 supervisor 缺席而停摆）。
#[derive(Default)]
struct PoolGate(std::sync::Mutex<Option<Arc<dyn cog_core::SchedulerGate>>>);

impl PoolGate {
    fn set(&self, gate: Arc<dyn cog_core::SchedulerGate>) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = Some(gate);
        }
    }

    fn llm_paused(&self) -> bool {
        self.0
            .lock()
            .ok()
            .and_then(|slot| {
                slot.as_ref()
                    .map(|g| g.is_paused_kind(cog_core::TaskClass::LlmDependent))
            })
            .unwrap_or(false)
    }
}

/// Reflection plugin that self-assembles the reflection engine and spawns
/// evolution bridges.
pub struct ReflectionPlugin {
    initialized: bool,
    /// init 确认自进化管线真实启动且沙盒边界放行后为 true：start() 据此
    /// 挂基线移植触发循环（跨插件消费 OrchestratorControl 只能在 start，
    /// 见插件生命周期 init_all → start_all）。
    porter_armed: bool,
    /// 见 [`PoolGate`]；init 阶段 spawn 的循环据此跳过 LLM 依赖轮次。
    pool_gate: Arc<PoolGate>,
    /// 全插件唯一的分配器：部署器、各进化消费者都从这里取工作树。
    workspaces: Option<Arc<crate::workspace::WorkspaceManager>>,
    /// 产物级进化的读侧：既提供已记录的决策结果，也提供推荐路径此刻真正
    /// 在用的参数。参数搜索拿它当基线，不自己另推一份。
    meta_learning: Option<Arc<crate::MetaLearningEngine>>,
    /// 产物级进化的写侧：保存新版本并热替换 active 指针。
    artifact_evolution: Option<Arc<crate::ArtifactEvolution>>,
    /// The shared build cache's readings, published in init and measured from
    /// start(). Only the deployment that runs builds has a cache of its own, so
    /// every other one holds None here.
    build_cache: Option<Arc<crate::build_cache_readings::BuildCacheReadings>>,
    /// The in-cluster registry store's footprint, published in init and measured
    /// from start() by the process that pushes to it. A deployment with an
    /// external registry, or one that runs no deployer, holds None here.
    registry_footprint: Option<Arc<crate::registry_footprint::RegistryFootprint>>,
    /// The distance between the ceilings the repository declares and the ones
    /// the cluster enforces, published in init and recorded from start() by the
    /// mainline deployer. Holds None wherever that deployer is not started --
    /// no handle at all is the one state a reader can tell from "nothing to
    /// compare".
    governance_drift: Option<Arc<crate::governance_drift::GovernanceDrift>>,
    /// What the self-discovery watcher finds each round. Published in init and
    /// counted from start(), by the process that arms the watcher: the handle
    /// is created unconditionally so a deployment that runs no watcher can
    /// still say so, and the loop that arms it is what flips the role flag.
    signal_readings: Arc<crate::signal_readings::SignalWatcherReadings>,
    /// 被门禁拒绝的变更回流主流程那条支路的门与读数。init 建好并发布，
    /// `start()` 把编排器填进它的槽——跨插件消费只能在 start。
    rework_gate: Option<Arc<crate::change_rework::ChangeReworkGate>>,
}

impl ReflectionPlugin {
    /// Create a plugin that will build the reflection engine during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            porter_armed: false,
            pool_gate: Arc::new(PoolGate::default()),
            workspaces: None,
            meta_learning: None,
            artifact_evolution: None,
            build_cache: None,
            registry_footprint: None,
            governance_drift: None,
            signal_readings: Arc::new(crate::signal_readings::SignalWatcherReadings::new()),
            rework_gate: None,
        }
    }
}

impl Default for ReflectionPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for ReflectionPlugin {
    fn name(&self) -> &'static str {
        "reflection"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let strict_persistence = ctx.config().system.strict_persistence;

        let llm_provider = ctx.consume_service::<dyn cog_core::LlmClient>();
        let memory_backend = ctx.consume_service::<dyn cog_core::MemoryBackend>();
        let skill_registry = ctx
            .require::<tokio::sync::RwLock<cog_core::SkillRegistry>>()?
            .clone();
        let prompt_manager = ctx
            .require_service::<dyn cog_core::PromptProvider>()?
            .clone();

        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();
        let (tool_tx, mut tool_rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();

        let project_root = std::env::current_dir().ok();
        let change_dir = ctx.config().self_evolution.change_dir.clone();

        // 工作区分配器：全插件唯一实例，部署器与各进化消费者共用。裸仓库取自
        // 部署器配置（默认 /host-git，与 GitOps 推送端同源）。
        // 这份配置在 init 里读一次并留到下面：registry 占用读数的声明要取它的
        // 端点与命名空间——同一条部署器配置，不另开一份 env，免得两条通道各写
        // 一个端点。
        let ml_config = crate::MainlineDeployerConfig::load()?;
        let bare_repo = ml_config.bare_repo.clone();
        let ws_cfg = &ctx.config().self_evolution.workspaces;
        // 索引健康度的采样去处。在这里取一次而不是各消费者各取一次：分配器是
        // 单例，采样点在它内部，调用方不该为了上报再去问一遍服务表。
        let metrics_backend = ctx.consume_service::<dyn cog_core::MetricsBackend>();
        let mut workspaces =
            crate::workspace::WorkspaceManager::new(&bare_repo, &ws_cfg.root, &ws_cfg.target_dir)
                .with_ephemeral_ttl(std::time::Duration::from_secs(ws_cfg.ephemeral_ttl_secs));
        if let Some(metrics) = metrics_backend.clone() {
            workspaces = workspaces.with_metrics(metrics);
        }
        let workspaces = Arc::new(workspaces);
        info!(
            bare = %bare_repo,
            root = %ws_cfg.root,
            target_dir = %ws_cfg.target_dir,
            ttl_secs = ws_cfg.ephemeral_ttl_secs,
            "workspace allocator configured"
        );
        self.workspaces = Some(workspaces);

        // The governance drift handle is published **here** and not where the
        // deployer that records into it is built (`start()`): the metrics
        // endpoint and the store sampler both take their snapshot of the
        // published observables while the plugins initialise, so a handle
        // published from `start()` is held by nobody -- the reading would live
        // in this process and reach no scrape, and the alert rule that reads it
        // could never fire. The guard mirrors the block in `start()` that
        // starts the deployer: published exactly when something will record.
        if ml_config.enabled && self.workspaces.is_some() {
            let drift = Arc::new(crate::governance_drift::GovernanceDrift::new());
            ctx.publish_observable(drift.clone());
            self.governance_drift = Some(drift);
        }

        // 引擎只把 project_root 用于变更路径校验，给它一棵稳定只读的基线
        // 工作树即可；工作树随沙盒生命周期存在，不再是那棵被大家共用的树。
        let instance_id = resolve_port_instance_id().await;
        let version = current_version(ctx);
        let mut engine_root = project_root.clone();
        // 引擎基线树与启动期工作树清理都要写裸仓库；控制面进程的 /host-git
        // 是只读挂载（executor_enabled=false，不派生进化循环），建不出也清
        // 不动，硬试只会每轮启动刷一串降级告警。基线树跟着执行器职责走。
        let self_evolution_cfg = &ctx.config().self_evolution;
        if self_evolution_cfg.enabled && self_evolution_cfg.executor_enabled {
            if let Some(ws) = self.workspaces.as_ref() {
                let base = ws.resolve_base(&instance_id, &version).await;
                let spec = crate::workspace::WorkspaceSpec::persistent(
                    "engine-baseline",
                    crate::workspace::WorkspaceKind::EngineBaseline,
                    base,
                );
                match ws.ensure_persistent(spec).await {
                    Ok(w) => engine_root = Some(w.path),
                    Err(e) => warn!(
                        error = %e,
                        "engine baseline workspace unavailable; change path validation falls \
                         back to the process working directory"
                    ),
                }
                // 实例身份来自装入期指纹并落盘在持久数据目录，重启与换机都不变；
                // 只有运维显式轮换指纹、或新装机没带上原 Secret 时，按实例命名的
                // 常驻工作树才会被遗弃。启动时收一次：先清理登记残留，再回收泄漏
                // 的临时树，最后回收已轮换实例的常驻树。
                if let Err(e) = ws.prune().await {
                    warn!(error = %e, "workspace prune failed");
                }
                match ws.gc_stale().await {
                    Ok(reclaimed) if !reclaimed.is_empty() => {
                        info!(count = reclaimed.len(), "reclaimed leaked workspaces")
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "workspace gc failed"),
                }
                match ws
                    .gc_orphan_instances(&instance_id, crate::workspace::ORPHAN_MIN_AGE)
                    .await
                {
                    Ok(reclaimed) if !reclaimed.is_empty() => {
                        info!(
                            count = reclaimed.len(),
                            "reclaimed rotated-instance workspaces"
                        )
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "workspace orphan gc failed"),
                }
                // 裸仓库里的 `evol/<id>` 分支只有推入、没有删除，身份每轮换一次就
                // 永久多一条。启动时收一次：只动非本实例、且已并入 main 或超期的。
                match ws
                    .gc_orphan_evol_branches(
                        &instance_id,
                        std::time::Duration::from_secs(ws_cfg.orphan_branch_ttl_secs),
                    )
                    .await
                {
                    Ok(reclaimed) if !reclaimed.is_empty() => {
                        info!(
                            count = reclaimed.len(),
                            "reclaimed orphan evolution branches"
                        )
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "orphan evolution branch gc failed"),
                }
            }
        }

        // The metrics backend storage published. Consumed here rather than
        // passed down from wherever the reader sits: the readings that need it
        // are recorded by objects built in this function — the change pipeline
        // among them.
        // Build an evolution engine up-front for the in-memory fallback below,
        // which has no persistent memory backend to build one from. In
        // production the engine comes from `new_self_evolution`, which builds
        // its own — with the hook and tool sinks this one does not carry — so
        // this instance is not the one that runs the self-evolution pipeline.
        // It does not depend on a persistent memory backend, so it is created
        // whenever an LLM is available.
        let evolution_engine: Option<Arc<crate::EvolutionEngine>> =
            if let Some(ref llm) = llm_provider {
                let mut evolution = crate::EvolutionEngine::new(
                    llm.clone(),
                    skill_registry.clone(),
                    Some(prompt_manager.clone()),
                )
                .with_change_dir(change_dir.clone());
                if let Some(ref root) = engine_root {
                    evolution = evolution.with_project_root(root.clone());
                }
                Some(Arc::new(evolution))
            } else {
                None
            };

        // A missing memory backend means learned state would be dropped on
        // restart, which is what strict_persistence forbids. A missing LLM
        // only lowers reflection quality, so it is not gated.
        if strict_persistence && memory_backend.is_none() {
            return Err(cog_core::SFError::Config(
                "ReflectionEngine has no MemoryBackend; in-memory mode would drop persistent learning (strict_persistence=true)".into(),
            ));
        }

        // 反思条目与它的派生层是两次独立写，第二次失败或中途重启会留下「raw
        // 落了盘、schema 缺席」的孤儿，而检索走 schema，于是这条学习静默消失。
        // 补齐是确定性的（raw 里存的就是条目本身），不吃 LLM 配额，所以放在
        // init：它既不依赖自进化管线是否武装，也不受沙盒边界门禁影响——缺口
        // 在管线的哪一侧出现与门禁无关。
        {
            let repair_interval = ctx.config().self_evolution.schema_repair_interval_secs;
            if let Some(mb) = memory_backend.clone() {
                if repair_interval == 0 {
                    info!("memory schema repair disabled by config");
                } else {
                    let recorder =
                        crate::MemoryBackendRecorder::new(mb, crate::REFLECTION_NAMESPACE)
                            .with_metrics(metrics_backend.clone());
                    let shutdown = cog_core::ShutdownSignal::new();
                    if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                        let shutdown = shutdown.clone();
                        let mut rx = broadcast_tx.0.subscribe();
                        tokio::spawn(async move {
                            let _ = rx.recv().await;
                            shutdown.trigger();
                        });
                    }
                    info!(
                        interval_secs = repair_interval,
                        "memory schema repair loop started"
                    );
                    drop(crate::recorder::spawn_schema_repair_loop(
                        recorder,
                        std::time::Duration::from_secs(repair_interval),
                        shutdown,
                    ));
                }
            }
        }

        let engine = if let (Some(ref mb), Some(ref llm)) = (memory_backend, llm_provider) {
            info!("ReflectionEngine initialized in production mode (persistent learning)");
            crate::ReflectionEngine::new_self_evolution(
                skill_registry.clone(),
                llm.clone(),
                std::time::Duration::from_secs(3600),
                mb.clone(),
                Some(prompt_manager.clone()),
                Some(hook_tx),
                Some(tool_tx),
                engine_root.clone(),
                change_dir.clone(),
            )
        } else {
            warn!("ReflectionEngine falling back to in-memory mode (memory_backend or llm_provider unavailable)");
            info!("ReflectionEngine initialized in in-memory mode");
            let mut engine = crate::ReflectionEngine::new_in_memory(skill_registry.clone());
            if let Some(ref evo) = evolution_engine {
                engine.evolution = Some(evo.clone());
                info!("ReflectionEngine evolution engine attached in in-memory mode");
            }
            engine
        };

        // 学习数据飞轮（审计 4.4）：所有学习记录在本地持久化之外，
        // 同步导出 JSONL 数仓原始区 `{data_dir}/warehouse/`，供离线分析与策略训练。
        let mut engine = engine;
        let warehouse_dir = format!("{}/warehouse", ctx.config().app.data_dir);
        engine.recorder = Arc::new(crate::WarehouseRecorder::new(
            engine.recorder.clone(),
            Arc::new(crate::JsonlFileSink::new(&warehouse_dir)),
        ));
        info!(dir = %warehouse_dir, "learning warehouse flywheel enabled");

        // 产物级进化：策略产物版本化存储 +
        // 哈希链完整性 + 热替换。MetaLearningEngine 的推荐参数从策略产物
        // active 版本读取；ArtifactEvolution 供评估侧在统计显著时升级策略。
        let policy_dir = format!("{}/policies", ctx.config().app.data_dir);
        let policy_store = crate::PolicyStore::new(&policy_dir);
        // 决策聚合是进程内的，重启即空；而调参驱动与推荐路径都读它，所以它
        // 必须落在本进程自己的数据卷上——否则「攒够证据才调参」会被每次换版
        // 清零，参数搜索在频繁重建 Pod 的部署上永远到不了 min_trials。快照
        // 只被本进程读写，不碰共享库表，也不依赖 memory 后端。
        let stats_path = format!(
            "{}/meta_learning/decision_stats.json",
            ctx.config().app.data_dir
        );
        // 一个进程只造一个引擎，写侧和读侧共用同一个对象：squad 把决策结果
        // 记进它，推荐路径从它的 active 版本读参数，调参驱动也从它读已记录
        // 的结果。任何一侧拿到的是另一个实例，两边就都活着却谁也看不见谁
        // ——记录写进无人读的引擎，驱动对着空分组报证据不足。所以这里不按
        // 「原来有没有引擎」分叉：没有就造一个，造完一律挂上策略库。
        let meta_learning_engine = Arc::new(
            crate::MetaLearningEngine::with_durable_state(
                engine.recorder.clone(),
                crate::DecisionStatsSnapshot::new(&stats_path),
            )
            .await
            .with_policy_store(policy_store.clone(), "meta_learning.mode"),
        );
        engine.meta_learning = Some(meta_learning_engine.clone());
        let artifact_evolution = Arc::new(crate::ArtifactEvolution::new(policy_store));
        // 循环在 start() 挂载；这里先把两侧拿在手上。
        self.meta_learning = Some(meta_learning_engine.clone());
        self.artifact_evolution = Some(artifact_evolution.clone());
        let engine = Arc::new(engine);
        ctx.publish(artifact_evolution.clone());
        info!(dir = %policy_dir, "artifact-level evolution policy store enabled");

        ctx.publish(engine.clone());
        info!("ReflectionPlugin reflection engine published");

        // 被门禁拒绝的变更回流主流程那条支路的属主门与读数。读数在这里发布：
        // 采集侧的快照是插件初始化期间抓的，从 `start()` 发布的句柄没人读。提交
        // 权只给承担执行器职责的进程——插件表在两个进程里整表加载，都提交就是同一
        // 份需求提交两遍。编排器句柄要等 `start()` 才拿得到，这里只定属主。
        if ctx.config().self_evolution.executor_enabled {
            engine.rework.arm();
        }
        ctx.publish_observable(engine.rework.clone());
        self.rework_gate = Some(engine.rework.clone());

        // Publish ChangeSink when self-evolution is available.
        if let Some(ref evo) = engine.evolution {
            let sink: Arc<dyn cog_core::ChangeSink> = evo.clone();
            ctx.publish_service(sink);
            info!("ReflectionPlugin ChangeSink published");
        }

        // Publish reflection trait objects for downstream consumers.
        let squad_reflection: Arc<dyn cog_core::SquadReflection> =
            Arc::new(crate::DefaultSquadReflection::new(
                engine.recorder.clone(),
                engine.matcher.clone(),
                engine.promoter.clone(),
                None,
            ));
        ctx.publish_service(squad_reflection);

        // 下游拿到的必须就是上面那个挂了库的引擎。此前这里在引擎缺席时另造
        // 一个无库实例发布出去，于是执行面记录到的结果落进一个没有策略库、
        // 也没有驱动在读的引擎里。
        let meta_learning: Arc<dyn cog_core::MetaLearning> = meta_learning_engine.clone();
        ctx.publish_service(meta_learning);

        let fault_classifier: Arc<dyn cog_core::FaultClassifier> =
            Arc::new(crate::RuleBasedFaultClassifier::new());
        ctx.publish_service(fault_classifier);
        info!("ReflectionPlugin reflection trait objects published");

        // Spawn evolution bridges.
        let hook_engine = ctx.require_service::<dyn cog_core::HookEngine>()?;
        let tool_registry = ctx.require_service::<dyn cog_core::ToolRegistry>()?;

        // Hook bridge.
        {
            let hook_engine = hook_engine.clone();
            let evolution = engine.evolution.clone();
            tokio::spawn(async move {
                while let Some(hook_json) = hook_rx.recv().await {
                    match serde_json::from_value::<cog_core::HookDef>(hook_json) {
                        Ok(def) => {
                            let id = def.id.clone();
                            hook_engine.register(def).await;
                            info!("Evolution hook auto-registered: {}", id);
                            if let Some(ref evo) = evolution {
                                if !evo
                                    .update_status(&id, crate::EvolutionStatus::Registered)
                                    .await
                                {
                                    error!("Evolution hook registered but status update failed for artifact_id={}", id);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Evolution hook registration skipped: parse error: {}", e);
                        }
                    }
                }
            });
        }

        // Tool bridge.
        {
            let tool_registry = tool_registry.clone();
            let evolution = engine.evolution.clone();
            tokio::spawn(async move {
                while let Some(tool_json) = tool_rx.recv().await {
                    if let Some(name) = tool_json.get("name").and_then(|v| v.as_str()) {
                        let description = tool_json
                            .get("description")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let parameters = tool_json.get("parameters").cloned().unwrap_or_else(
                            || serde_json::json!({"type": "object", "properties": {}}),
                        );
                        let tool = cog_core::Tool {
                            name: name.to_string(),
                            description,
                            parameters,
                            implementation: cog_core::ToolImplementation::Native(Arc::new(
                                |_args| {
                                    Box::pin(async move {
                                        Ok(serde_json::json!({
                                            "error": "not implemented yet"
                                        }))
                                    })
                                },
                            )),
                        };
                        tool_registry.register(tool);
                        info!("Evolution tool variant registered: {}", name);
                        if let Some(ref evo) = evolution {
                            if !evo
                                .update_status(name, crate::EvolutionStatus::Registered)
                                .await
                            {
                                error!("Evolution tool registered but status update failed for artifact_id={}", name);
                            }
                        }
                    }
                }
            });
        }

        // Spawn self-evolution auto-deploy pipeline.
        {
            let self_evolution = ctx.config().self_evolution.clone();
            if self_evolution.enabled {
                // 晋级门配置是 cog-reflection 自有配置段（不进 core
                // config.rs）：本 crate 自己从 cogneva.json
                // self_evolution.promotion 段 + env 覆盖加载。
                let promotion = crate::PromotionGateConfig::load()?;
                // 按变更一次的 Job 那条路的上界与落点。读不到就是配置写坏了，
                // 不是"没配"：与上面那份门配置同一种读法，缺文件/缺段取默认。
                let change_job = crate::ChangeJobConfig::load()?;
                let evolution_metrics: Option<Arc<dyn cog_core::EvolutionMetrics>> =
                    ctx.consume_service::<dyn cog_core::EvolutionMetrics>();

                // Sandbox boundary check (audit roadmap Phase 3.1/3.2): real
                // auto apply/deploy is only allowed inside a detected
                // isolated environment, when the operator declares
                // sandbox_mode, or when force_autonomous bypasses the check.
                let (self_evolution, boundary) = crate::sandbox::enforce_sandbox_boundary(
                    &self_evolution,
                    &crate::sandbox::SandboxSignals::from_environment(),
                );
                match &boundary {
                    crate::sandbox::BoundaryDecision::Allowed(reason) => {
                        info!(reason = %reason, "Self-evolution sandbox boundary check passed");
                    }
                    crate::sandbox::BoundaryDecision::Downgraded(reason) => {
                        warn!(reason = %reason, "Self-evolution downgraded to dry-run");
                        if let Some(m) = evolution_metrics.as_ref() {
                            m.record_event(true).await;
                        }
                        // 通知模式（审计 Phase 3.1）：dry-run 降级必须让操作者
                        // 可见，不能只停留在日志里。
                        if self_evolution.notify_on_failure {
                            notify_sandbox_downgrade(ctx, reason).await;
                        }
                    }
                }

                // Firecracker 微虚拟机编排（审计 2.5.4）：microvm.enabled 时
                // host 不本地执行 change pipeline，而是每个 cycle 冷启动一个
                // MicroVM（挂载 PV → 执行进化 → 阅后即焚）。preflight 失败
                // 视为配置错误：显式报错并禁用 pipeline，绝不静默落到无沙盒
                // 的本地执行。
                // microVM 编排同样是执行器职责，且本分支会提前 return 跳过
                // 控制面（admin 服务 / GitOps 拉取端）。只有本进程承担变更执行
                // （executor_enabled）时才进入，否则主应用会被这条早退路径剥夺
                // 控制面。microvm 关闭时本就不进此分支，门禁只为防御「基础配置
                // 对全部 Pod 打开 microvm」的误配。
                if self_evolution.microvm.enabled && self_evolution.executor_enabled {
                    let microvm = crate::FirecrackerSandbox::new(self_evolution.microvm.clone());
                    if let Err(e) = microvm.preflight() {
                        error!(error = %e, "microvm preflight failed; self-evolution pipeline disabled");
                        return Err(e);
                    }
                    info!(
                        exec_timeout_secs = self_evolution.microvm.exec_timeout_secs,
                        "Firecracker microVM sandbox enabled; evolution runs inside cold-start VMs"
                    );
                    let metrics = evolution_metrics.clone();
                    let poll = std::time::Duration::from_secs(self_evolution.poll_interval_secs);
                    let pool_gate = self.pool_gate.clone();
                    let microvm = std::sync::Arc::new(microvm);
                    // Nothing hands this loop a stop signal, and it has no exit
                    // of its own: if it ends while the process lives, the
                    // evolution work it drives has stopped with it.
                    drop(cog_core::loop_health::spawn_unstoppable(
                        MICROVM_EVOLUTION_LOOP,
                        cog_core::loop_health::Cadence::Periodic(poll),
                        // Rebuilt per attempt, so everything the body consumes is
                        // cloned here.
                        move |beat| {
                            let microvm = std::sync::Arc::clone(&microvm);
                            let metrics = metrics.clone();
                            let pool_gate = pool_gate.clone();
                            async move {
                                let mut interval = tokio::time::interval(poll);
                                loop {
                                    beat.beat();
                                    interval.tick().await;
                                    if pool_gate.llm_paused() {
                                        info!("LLM upstream pool unavailable; skipping microvm evolution cycle");
                                        continue;
                                    }
                                    match microvm.run_evolution().await {
                                        Ok(outcome) => {
                                            if let Some(m) = metrics.as_ref() {
                                                m.record_event(!outcome.completed).await;
                                            }
                                            if outcome.completed {
                                                info!(
                                                    vm_id = %outcome.vm_id,
                                                    secs = outcome.duration_secs,
                                                    "microvm evolution cycle complete"
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            if let Some(m) = metrics.as_ref() {
                                                m.record_event(true).await;
                                            }
                                            warn!(error = %e, "microvm evolution cycle failed");
                                        }
                                    }
                                }
                            }
                        },
                    ));
                    self.initialized = true;
                    return Ok(());
                }

                let Some(project_root) = std::env::current_dir().ok() else {
                    warn!(
                        "Could not determine current directory; self-evolution pipeline disabled"
                    );
                    self.initialized = true;
                    return Ok(());
                };

                // 进程工作目录不再是源码树，环境校验改以基线工作树为准；
                // 基线不可用时退回工作目录（校验会显式报错，不静默降级）。
                let env_root = engine_root.clone().unwrap_or_else(|| project_root.clone());
                if let Err(e) = ensure_self_evolution_environment(&env_root, &self_evolution).await
                {
                    error!(error = %e, "Self-evolution environment validation failed");
                    return Err(e);
                }

                // 两个超时曾经只是配置面上的两个字段：写进去、没人读、也没有读数。
                // 现在它们是这一对运行的执行边界，所以既被 enforcement 读、也被观测面报，
                // 且**同一个对象**提供这两个数——分开传就会出现"读到 3600、实际按 1800 杀"
                // 的观测面，那比没有读数更坏。
                let budget =
                    std::sync::Arc::new(crate::verification_budget::VerificationBudget::new(
                        self_evolution.test_timeout_secs,
                        self_evolution.build_timeout_secs,
                    ));

                let mut pipeline = crate::ChangePipeline::new(
                    &project_root,
                    &self_evolution.change_dir,
                    // manual_approve holds test-passed changes at AwaitingReview
                    // (working tree rolled back) until an operator approves via
                    // the admin API, even when auto_apply is enabled.
                    self_evolution.auto_apply && !self_evolution.manual_approve,
                )
                .with_verification_budget(budget.clone())
                .with_promotion_policy(promotion.clone())
                // The command that judges a change comes from the deployment,
                // not from this crate: a project that is not a Rust workspace
                // declares its own suite here, and the value recorded in a
                // change's evidence is this one.
                .with_test_command(self_evolution.test_command.clone())
                .with_target_dir(&self_evolution.workspaces.target_dir);
                // 变更忠实度读数的去处：调用点在这里，句柄也从这里给，不再
                // 绕经生成引擎——那条路已经删了。
                if let Some(metrics) = metrics_backend.clone() {
                    pipeline = pipeline.with_metrics(metrics);
                }

                // What each change's build cost the host, split by which entry
                // point the change came from and by how it ended: a compile
                // failure, a budget kill and a refusal are three different
                // things to do about, and one "build failed" count cannot tell
                // which of them a run of failures was.
                let build_readings = std::sync::Arc::new(
                    crate::evolution_build_readings::EvolutionBuildReadings::new(),
                );

                let deployer = crate::EvolutionDeployer::new(
                    &project_root,
                    &self_evolution.binary_dir,
                    &self_evolution.backup_dir,
                )
                .with_verification_budget(budget.clone())
                .with_build_readings(build_readings.clone())
                .with_target_dir(&self_evolution.workspaces.target_dir);

                // 一条请求里不随变更变的那半：路径、预算、策略。**与流水线和部署器
                // 同源**——这里放进去的正是构造它们用的那几个值，不是把配置再解析
                // 一遍：再解析一次就可能让执行进程站在另一份世界里，而两份世界的
                // 差异要到很后面才以"文件不在这儿"的形式现形。
                let world = crate::change_execution::ChangeExecutionWorld {
                    project_root: project_root.clone(),
                    change_dir: self_evolution.change_dir.clone().into(),
                    workspace_root: self_evolution.workspaces.root.clone().into(),
                    bare_repo: bare_repo.clone().into(),
                    target_dir: self_evolution.workspaces.target_dir.clone().into(),
                    binary_dir: self_evolution.binary_dir.clone().into(),
                    backup_dir: self_evolution.backup_dir.clone().into(),
                    test_timeout_secs: self_evolution.test_timeout_secs,
                    test_command: self_evolution.test_command.clone(),
                    build_timeout_secs: self_evolution.build_timeout_secs,
                    auto_apply: self_evolution.auto_apply,
                    manual_approve: self_evolution.manual_approve,
                    promotion: promotion.clone(),
                    build_gate: self_evolution.build_gate.clone(),
                };

                // Published by clone: the flight readings below take the test
                // budget off this object rather than re-deriving it from the
                // configuration document, so the wall a flight is judged against
                // is the one it is actually killed by. Build readings likewise:
                // the Job route's parent has to record into the same object the
                // in-process route does.
                ctx.publish_observable(budget.clone());
                ctx.publish_observable(build_readings.clone());

                let binary_switcher = ctx.consume_service::<dyn cog_core::BinarySwitcher>();
                // 变更上游通道（平台集成侧实现）：沙盒验过的提交经它落到主分支。
                // 平台账号未连接时缺席——本轮照常构建部署，只是不回流上游。
                let landing = ctx.consume_service::<dyn cog_core::ChangeLanding>();
                if landing.is_none() {
                    info!(
                        "no ChangeLanding service; verified changes stay in the sandbox \
                         and are not landed upstream"
                    );
                }
                let audit_stream = ctx.consume_service::<dyn cog_core::AuditStream>();
                if audit_stream.is_none() {
                    warn!("AuditStream not published; change operations will not be audited");
                }
                let engine = engine.clone();

                // 本进程是否承担变更执行器职责。主应用（executor_enabled=false）
                // 只保留控制面（admin 服务 / GitOps 拉取端），不派生进化循环，
                // 也就不与共用同一实例指纹 / 裸仓库 / 工作树根目录的专用进化
                // worker 抢工作树。在这里读一次：下面的 spawn 会把 self_evolution
                // 整体 move 进闭包，事后再读它会触发 use-after-move，而队列读数
                // 与下面的循环都要这个值。
                let executor_enabled = self_evolution.executor_enabled;

                // The queue this process reads, published by every process and
                // measured only by the one that drains it: the role flag has to
                // exist on the control plane too, or "no process is set up to
                // drain this queue" would read the same as a series nobody
                // scraped. Same object the admin listing reports as its source,
                // so the directory a reader sees and the one a rule groups by
                // cannot drift apart.
                let queue_readings = Arc::new(
                    crate::evolution_queue_readings::EvolutionQueueReadings::new(
                        self_evolution.change_dir.clone(),
                        executor_enabled,
                        self_evolution.poll_interval_secs,
                        pipeline.clone(),
                        engine.evolution.clone(),
                    ),
                );
                ctx.publish_observable(queue_readings.clone());

                // The apply/test flight publishes itself while it runs, because
                // everything else about it is derived after it ends: a change
                // forty minutes into a healthy verification and a cycle stopped
                // at the verification had the same face, and the reading that
                // separates them is the age of the flight in progress.
                //
                // The wall it is judged against is the sum of the bounds of its
                // two slowest steps, taken from the objects that enforce them:
                // the verification budget the pipeline holds, and the build
                // gate's configured wait -- the same value `install_for` below
                // hands the gate this process runs under. A gate that turns out
                // not to be in force (disabled, or a slot directory it could not
                // create) makes this wall larger than the flight's true bound,
                // which delays a rule rather than firing one on a healthy
                // flight. Only the process that runs flights publishes any of
                // it: the role is the same `executor_enabled` the queue readings
                // publish, and a second flag here could drift out of step with
                // that one.
                let flight_wall_secs = budget.timeout_secs(crate::verification_budget::KIND_TEST)
                    + self_evolution.build_gate.wait_secs;
                let flight = Arc::new(if executor_enabled {
                    crate::evolution_flight_readings::EvolutionFlightReadings::new(
                        self_evolution.change_dir.clone(),
                        flight_wall_secs,
                    )
                } else {
                    crate::evolution_flight_readings::EvolutionFlightReadings::none(
                        self_evolution.change_dir.clone(),
                    )
                });
                ctx.publish_observable(flight.clone());

                // What the self-discovery watcher does with each round's
                // signals. Published by every process and counted only by the
                // one that arms the watcher: without the handle on the control
                // plane too, a deployment that runs no watcher would publish a
                // flat count that reads as a watcher finding nothing. The loop
                // itself is armed in start(), with the other loops.
                ctx.publish_observable(self.signal_readings.clone());

                // 自动晋级运行时一键暂停开关：admin API 与 AutoPromoter
                // 共享同一实例，暂停立即对排队晋级生效。
                let promotion_switch = Arc::new(crate::PromotionSwitch::new());
                let promotion_ledger: Option<Arc<dyn cog_core::PromotionLedger>> =
                    ctx.consume_service::<dyn cog_core::PromotionLedger>();

                // 接管台 SSE 推送通道：变更行状态即时广播。
                let (stream_tx, _) =
                    tokio::sync::broadcast::channel::<cog_core::EvolutionChangeInfo>(64);
                ctx.publish(Arc::new(stream_tx.clone()));

                // 晋级触发器：沙盒验证全过的 change 由它决定去向
                // （GitOps 自动晋级 / 审批台待办），配额/熔断/暂停全在
                // 其中判定。推送端只跟 Git 中央仓库说话，不持集群凭证。
                let promotion_channel: Option<Arc<dyn crate::PromotionChannel>> =
                    if promotion.gitops.enabled {
                        info!(
                            repo = %promotion.gitops.repo_url,
                            branch = %promotion.gitops.branch,
                            "GitOps promotion publisher enabled"
                        );
                        // 推送端只要求「能解析待发布提交的 git 目录」：指向裸仓库，
                        // 变更提交来自用完即弃的临时工作树也不影响推送。
                        Some(Arc::new(crate::GitOpsPublisher::new(
                            promotion.gitops.clone(),
                            &bare_repo,
                            &self_evolution.binary_dir,
                        )))
                    } else {
                        None
                    };
                let promoter: Option<Arc<crate::AutoPromoter>> = ctx
                    .consume_service::<dyn cog_core::PromotionLedger>()
                    .map(|ledger| {
                        Arc::new(
                            crate::AutoPromoter::new(
                                promotion.clone(),
                                ledger,
                                promotion_channel,
                                engine.clone(),
                            )
                            .with_switch(promotion_switch.clone()),
                        )
                    });
                if promoter.is_none() {
                    warn!("PromotionLedger not published; auto-promotion disabled");
                }

                // Publish admin-facing evolution control surface.
                let mut admin = crate::EvolutionAdminService::new(
                    engine.clone(),
                    pipeline.clone(),
                    deployer.clone(),
                    binary_switcher.clone(),
                    evolution_metrics.clone(),
                )
                .with_evolution_stream(stream_tx)
                .with_change_queue(queue_readings);
                if let Some(ref stream) = audit_stream {
                    admin = admin.with_audit_stream(stream.clone());
                }
                admin = admin.with_artifact_evolution(artifact_evolution.clone());
                if let Some(ws) = self.workspaces.clone() {
                    admin = admin.with_workspaces(ws);
                }
                if let Some(ref ledger) = promotion_ledger {
                    admin = admin.with_promotion_state(
                        promotion_switch.clone(),
                        ledger.clone(),
                        promotion.enabled,
                    );
                }
                // 审批台与自动通道共用同一个晋级器：批准一条「等人工审批」的
                // 变更要落成晋级台账的一次晋级，而不是另开一条路。
                if let Some(ref promoter) = promoter {
                    admin = admin.with_promoter(promoter.clone());
                }
                // 晋级周报（eval 长期趋势）：周期聚合台账写报告文件，趋势
                // 向下时写审计告警。latest 句柄同时交给 admin 端点。
                let mut trend_reporter = None;
                if promotion.trend_report_enabled {
                    if let Some(ref ledger) = promotion_ledger {
                        // 停摆（连续整周零晋级）没有告警就只是一份没人看的报告：
                        // 成功率为空的周会被趋势判定整周跳过，最响的失败读起来像
                        // 系统空闲。周报要有能力把它推进持久化告警面。
                        let alert_sink = ctx.consume_service::<dyn cog_core::PersistentAlertSink>();
                        if alert_sink.is_none() {
                            warn!("PersistentAlertSink not published; promotion stall alerts stay report-only");
                        }
                        let reporter = crate::PromotionTrendReporter::new(
                            ledger.clone(),
                            std::path::PathBuf::from(format!(
                                "{}/reports",
                                ctx.config().app.data_dir
                            )),
                            std::time::Duration::from_secs(promotion.trend_report_interval_secs),
                            audit_stream.clone(),
                            alert_sink,
                        );
                        admin = admin.with_trend_latest(reporter.latest());
                        trend_reporter = Some(reporter);
                    }
                }
                if self_evolution.image_rollout.enabled {
                    admin = admin.with_image_rollout(Arc::new(crate::ImageRollout::new(
                        self_evolution.image_rollout.clone(),
                    )));
                    info!(
                        deployment = %self_evolution.image_rollout.deployment,
                        namespace = %self_evolution.image_rollout.namespace,
                        "image-based rolling update enabled for evolution deploys"
                    );
                }
                let admin_service: Arc<dyn cog_core::EvolutionAdmin> = Arc::new(admin);
                ctx.publish_service(admin_service);
                info!("ReflectionPlugin evolution admin service published");

                // 晋级周报后台循环：立即生成一期，之后按间隔周期生成。
                if let Some(reporter) = trend_reporter {
                    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
                    if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                        let mut rx = broadcast_tx.0.subscribe();
                        tokio::spawn(async move {
                            let _ = rx.recv().await;
                            let _ = shutdown_tx.send(true);
                        });
                    }
                    info!("Promotion trend reporter enabled");
                    tokio::spawn(reporter.run(shutdown_rx));
                }

                // GitOps 拉取端：gitops.enabled 且 puller_enabled 时本进程
                // 所在集群各跑一个，poll 中央仓库 release 分支，各自金丝雀/
                // 回滚/熔断（台账 cluster 字段区分集群，单集群故障不影响
                // 其他集群）。沙盒推送端置 puller_enabled=false 只推不拉。
                if promotion.gitops.enabled && promotion.gitops.puller_enabled {
                    if let Some(ledger) = ctx.consume_service::<dyn cog_core::PromotionLedger>() {
                        let cluster = std::env::var("COGNEVA_CLUSTER_NAME")
                            .ok()
                            .filter(|s| !s.trim().is_empty())
                            .or_else(|| {
                                std::env::var("HOSTNAME")
                                    .ok()
                                    .filter(|s| !s.trim().is_empty())
                            })
                            .unwrap_or_else(|| "default".into());
                        let metrics_url = std::env::var("COGNEVA_GITOPS_METRICS_URL")
                            .ok()
                            .filter(|s| !s.trim().is_empty());
                        // A gate that could not read all watch needs its verdict
                        // to have a successor: a verdict that reaches only the
                        // ledger and the log reads like nobody being told. Without
                        // a sink, say so and stay report-only.
                        let alert_sink = ctx.consume_service::<dyn cog_core::PersistentAlertSink>();
                        if alert_sink.is_none() {
                            warn!(
                                "PersistentAlertSink not published; canary gate blindness \
                                 stays report-only"
                            );
                        }
                        // Each poll's outcome goes to the metric store: the loop's
                        // liveness is already read, but a puller that cycles and fails
                        // every round otherwise leaves only a log line, which dies with
                        // the pod and reads like a cluster with nothing to pull.
                        let gitops_metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
                        if gitops_metrics.is_none() {
                            warn!(
                                "MetricsBackend not published; GitOps poll outcomes \
                                 stay in the log only"
                            );
                        }
                        let puller = Arc::new(
                            crate::GitOpsPuller::new(
                                promotion.gitops.clone(),
                                ledger,
                                cluster.clone(),
                            )
                            .with_metrics_url(metrics_url)
                            .with_metrics(gitops_metrics)
                            .with_alert_sink(alert_sink),
                        );
                        let puller_shutdown = cog_core::ShutdownSignal::new();
                        if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                            let shutdown = puller_shutdown.clone();
                            let mut rx = broadcast_tx.0.subscribe();
                            tokio::spawn(async move {
                                let _ = rx.recv().await;
                                shutdown.trigger();
                            });
                        }
                        info!(
                            cluster = %cluster,
                            repo = %promotion.gitops.repo_url,
                            branch = %promotion.gitops.branch,
                            "GitOps promotion puller enabled for this cluster"
                        );
                        tokio::spawn(crate::run_puller_loop(puller, puller_shutdown));
                    } else {
                        warn!("PromotionLedger not published; GitOps puller disabled");
                    }
                }

                // The build gate is a property of a process that runs builds, so
                // it is installed by role: the executor process takes the gate
                // (it runs the cycles, the mainline deployer and the porter, all
                // of which build), and the control plane publishes zero readings
                // instead of locking a private copy of the directory. Here
                // because nothing above builds and the spawns below move the
                // config into their closures.
                let build_gate =
                    cog_core::build_gate::install_for(&self_evolution.build_gate, executor_enabled);
                ctx.publish_observable(build_gate);

                // What the shared build cache holds, layer by layer. Published
                // by every process so the series set does not depend on which
                // role this one has, and measured only by the one that builds:
                // a process with no cache of its own has nothing to report, and
                // reporting zero for it would read as an empty cache. The scan
                // loop is armed in start(), with the rest of the loops.
                let build_cache = std::sync::Arc::new(
                    crate::build_cache_readings::BuildCacheReadings::new(
                        self_evolution.workspaces.target_dir.clone(),
                    )
                    .with_cap(
                        self_evolution.workspaces.target_max_bytes,
                        self_evolution.workspaces.cache_scan_interval_secs,
                    ),
                );
                ctx.publish_observable(build_cache.clone());
                self.build_cache = executor_enabled.then_some(build_cache);

                // 集群内 registry 的占用。那个 store 的唯一写者就是本进程的
                // 部署器（buildah push），但它自己不会报尺寸，走查也不在它的
                // 挂载表里；kubelet 那条 per-volume 序列对目录型卷报的是节点
                // 盘（本集群实测 12 块卷全读 689GB，10Gi 的声明量在里面）。
                // 读数因此从 registry 自己的 API 取，按声明挂到那张卷上。
                let (registry_footprint, footprint_problems) = crate::registry_footprint::from_env(
                    &ml_config.registry,
                    &ml_config.namespace,
                    std::env::var(crate::registry_footprint::CLAIM_ENV)
                        .ok()
                        .as_deref(),
                );
                for problem in &footprint_problems {
                    warn!(problem = %problem, "registry footprint declaration is unusable");
                }
                if let Some(footprint) = registry_footprint {
                    ctx.publish_observable(footprint.clone());
                    self.registry_footprint = Some(footprint);
                }

                if executor_enabled {
                    let poll_interval =
                        std::time::Duration::from_secs(self_evolution.poll_interval_secs);

                    // 一轮里的活不受节拍约束：一轮要把队列里的待验变更全部消费完，而
                    // 一条变更要过三次宿主构建槽、跑三条各自的超时封顶的命令。心跳在
                    // 轮首打一次、之后每消费完一条变更再打一次，所以这个界要盖住的是
                    // **一条**变更——队列有多长这条循环才知道，声明的数不能是队列总和。
                    //
                    // 一条变更的活：三次构建槽排队（闸门 wait_secs，闸门关着时不排队）
                    // 之后各跑一条命令——clippy 与测试各吃一份测试预算、发布构建吃一份
                    // 构建预算——再加格式化那一段（每趟封在 fmt 预算上，趟数见
                    // `FMT_SEGMENT_WALL_SECS`）。三段都取自强制它们的那几个数，不是
                    // 挑一个整数把规则按住。
                    //
                    // Job 那条执行路径不用另外算：它的等待预算（deadline + 余量，默认
                    // 9900 秒）本来就短于这里算出来的进程内上界，盖得住。
                    let gate_wait_secs = if self_evolution.build_gate.enabled
                        && self_evolution.build_gate.max_concurrent > 0
                    {
                        self_evolution.build_gate.wait_secs
                    } else {
                        0
                    };
                    let one_change_work = std::time::Duration::from_secs(
                        crate::change_pipeline::one_change_wall_secs(
                            gate_wait_secs,
                            budget.timeout_secs(crate::verification_budget::KIND_TEST),
                            budget.timeout_secs(crate::verification_budget::KIND_BUILD),
                        ),
                    );

                    let Some(cycle_workspaces) = self.workspaces.clone() else {
                        warn!("workspace allocator unavailable; self-evolution cycle disabled");
                        self.initialized = true;
                        return Ok(());
                    };
                    let cycle_instance = instance_id.clone();
                    let cycle_version = version.clone();
                    // Carried into the cycle below so a service that was not
                    // published yet when this init read it can still be picked
                    // up.  Cloning shares the registry, not a snapshot of it.
                    let cycle_ctx = ctx.clone();

                    // No stop signal reaches this loop and it has no exit of its
                    // own, so any end means pending changes stop being verified.
                    drop(cog_core::loop_health::spawn_unstoppable(
                        CHANGE_VERIFICATION_LOOP,
                        cog_core::loop_health::Cadence::PeriodicWithWork {
                            period: poll_interval,
                            work: one_change_work,
                        },
                        // Rebuilt per attempt, so everything the body consumes is
                        // cloned here.
                        move |beat| {
                            let pipeline = pipeline.clone();
                            let deployer = deployer.clone();
                            let mut binary_switcher = binary_switcher.clone();
                            let cycle_ctx = cycle_ctx.clone();
                            let engine = engine.clone();
                            let self_evolution = self_evolution.clone();
                            let evolution_metrics = evolution_metrics.clone();
                            let promoter = promoter.clone();
                            let cycle_workspaces = cycle_workspaces.clone();
                            let landing = landing.clone();
                            let flight = flight.clone();
                            let change_job = change_job.clone();
                            let world = world.clone();
                            let cycle_budget = budget.clone();
                            let cycle_build_readings = build_readings.clone();
                            let cycle_instance = cycle_instance.clone();
                            let cycle_version = cycle_version.clone();
                            async move {
                                let mut interval = tokio::time::interval(poll_interval);
                                loop {
                                    beat.beat();
                                    interval.tick().await;
                                    // 这个句柄在 init 里读一次是不够的：发布它的 supervisor
                                    // 与本插件同层并行 init，先跑完的那个读到的 None 是
                                    // 「还没发布」，不是「本进程没有」。缺失时每轮重读一次，
                                    // 读到即停——停是为了让接线台账数的是「边」而不是
                                    // 「循环轮数」（PinAudit.reads 是计数）。不写
                                    // `.or(init 那份)`：init 那份为 None 时本就该让位给现读的。
                                    // 第一轮 tick 是立即返回的，所以这一次重读可能仍落在 init
                                    // 窗口里、被记成同一 pin 的第二条 init 期 demand——启动报告
                                    // 里那句话会多打一遍，是报告噪声，不是判据变化。
                                    if binary_switcher.is_none() {
                                        binary_switcher = cycle_ctx
                                            .consume_service::<dyn cog_core::BinarySwitcher>();
                                        if binary_switcher.is_some() {
                                            info!(
                                                "BinarySwitcher resolved after init; \
                                                 auto-deploy and the promotion hand-off \
                                                 no longer wait on the init-time read"
                                            );
                                        }
                                    }
                                    // 取走 soak 已满的晋级交接。判定必须在这里做，而不是
                                    // 由部署那一轮自己回调：self_exec 的切换是 execve，
                                    // 成功即不返回，回调永远不会执行（见 pending_promotions）。
                                    if let Some(p) = promoter.as_ref() {
                                        p.drain_handed_off().await;
                                        // 等人工审批的台账行里，有一部分等的是一件
                                        // 已经发生的事：落地通道先把它合进了主线。
                                        // 那件事没有对话者来销账（变更落地后已不在
                                        // 队列里），只有停摆告警一直读着它的出口，
                                        // 所以每轮顺手回收。
                                        p.reclaim_landed_approvals().await;
                                    }
                                    // 本轮是纯确定性消费：同步工作树、取出待验变更、apply/test/
                                    // build、落地、切二进制，全程不调 LLM。上游全灭时跳过它，只会让
                                    // 一条已经生成好的变更干等（生成侧的池门在 discovery 那边）。
                                    let deps = CycleDeps {
                                        pipeline: &pipeline,
                                        deployer: &deployer,
                                        binary_switcher: binary_switcher.as_ref(),
                                        engine: &engine,
                                        config: &self_evolution,
                                        evolution_metrics: evolution_metrics.as_ref(),
                                        promoter: promoter.as_ref(),
                                        workspaces: &cycle_workspaces,
                                        landing: landing.as_ref(),
                                        flight: &flight,
                                        change_job: &change_job,
                                        world: &world,
                                        budget: &cycle_budget,
                                        build_readings: &cycle_build_readings,
                                        beat: &beat,
                                    };
                                    if let Err(e) =
                                        run_evolution_cycle(deps, &cycle_instance, &cycle_version)
                                            .await
                                    {
                                        warn!(error = %e, "Self-evolution cycle failed");
                                    }
                                }
                            }
                        },
                    ));

                    info!("Self-evolution auto-deploy pipeline started");
                } else {
                    info!(
                        "self-evolution executor disabled here; running control plane only \
                         (admin API + GitOps puller), no change-execution cycle spawned"
                    );
                }

                // 基线移植触发循环（连同 start() 里的主线部署器）只在本进程承担
                // 执行器职责、且沙盒边界真实放行时挂载（dry-run 降级环境不做任何
                // git 写操作）。start() 里消费 OrchestratorControl。
                self.porter_armed = executor_enabled
                    && matches!(boundary, crate::sandbox::BoundaryDecision::Allowed(_));
            }
        }

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // 池暂停句柄在此填充（supervisor 已在 init 阶段发布 SchedulerGate）。
        let llm_gate = ctx.consume_service::<dyn cog_core::SchedulerGate>();
        if let Some(gate) = &llm_gate {
            self.pool_gate.set(gate.clone());
        } else {
            info!("no scheduler gate; LLM-dependent loops run unconditionally");
        }
        // 自发现信号 watcher 不依赖沙盒边界门禁：它只读 orchestrator 任务
        // 状态并提交内部意图，不碰 git 写操作。
        //
        // 但提交意图是**对外副作用**，而插件表在每个部署里都整表加载：主应用
        // 与进化 worker 都跑这个循环的话，同一个信号会被提交两遍，后到的那份
        // 只会撞上「任务已存在」，日志里看起来像提交失败。属主按执行器职责定
        // ——只有承担变更执行的那份部署产出自我发现意图。配置解析失败仍要在
        // 任何部署上响亮失败，所以先读配置再判属主。
        let sw_config = match crate::SignalWatcherConfig::load() {
            Ok(cfg) => cfg,
            Err(e) => return Err(e),
        };
        let owns_self_discovery = ctx.config().self_evolution.executor_enabled;
        let orchestrator = ctx.consume_service::<dyn cog_core::OrchestratorControl>();
        // 被拒变更的回流也要一个编排器句柄，而跨插件消费只能在 start 拿到。
        // 不承担提交职责的进程留空槽是设计内的一态；承担了却拿不到编排器，说明
        // 这条支路此刻没有出口，要响亮地说出来而不是静默丢弃。
        if let Some(gate) = self.rework_gate.as_ref() {
            match orchestrator.clone() {
                Some(orch) => gate.set_orchestrator(orch),
                None if gate.owns_submission() => warn!(
                    "no orchestrator; a change refused by a gate would have no way \
                     back into the main flow"
                ),
                None => {}
            }
        }
        if !sw_config.enabled {
            info!("signal watcher disabled by config");
        } else if !owns_self_discovery {
            info!(
                "signal watcher: this process is not the change executor; \
                 self-discovery intents disabled"
            );
        } else if let Some(orch) = orchestrator.clone() {
            let shutdown = cog_core::ShutdownSignal::new();
            if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                let shutdown = shutdown.clone();
                let mut rx = broadcast_tx.0.subscribe();
                tokio::spawn(async move {
                    let _ = rx.recv().await;
                    shutdown.trigger();
                });
            }
            // Persisted-alert channel: published by the observability
            // plugin in init, so it is guaranteed visible here.
            let alert_source = ctx.consume_service::<dyn cog_core::ActiveAlertSource>();
            if alert_source.is_none() {
                info!("signal watcher: no ActiveAlertSource; persisted-alert channel off");
            }
            // 外部提交的价值判定落盘在共用库，由持有库的插件发布；本进程只拿读面
            // （加上把读到的那一行的去向写回去，读与写必须落在同一处）。
            let taste_source = ctx.consume_service::<dyn cog_core::TasteIntentSource>();
            if taste_source.is_none() {
                info!("signal watcher: no TasteIntentSource; taste channel off");
            }
            drop(crate::spawn_signal_watcher_loop(
                orch,
                sw_config,
                shutdown,
                alert_source,
                taste_source,
                self.signal_readings.clone(),
                // 本循环产出的每个意图都会落到一个 squad，池全灭时跑一轮只买到一次 503。
                llm_gate.clone(),
            ));
        } else {
            info!("signal watcher: no orchestrator; self-discovery intents disabled");
        }

        // The cache size belongs to the role that builds: only the executor
        // deployment owns that directory, so on every other one the series does
        // not exist at all rather than existing as a zero nothing measured.
        match self.build_cache.as_ref() {
            Some(readings) => {
                let shutdown = cog_core::ShutdownSignal::new();
                if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                    let shutdown = shutdown.clone();
                    let mut rx = broadcast_tx.0.subscribe();
                    tokio::spawn(async move {
                        let _ = rx.recv().await;
                        shutdown.trigger();
                    });
                }
                drop(crate::build_cache_readings::spawn_build_cache_watch(
                    readings.clone(),
                    shutdown,
                ));
            }
            None => info!(
                dir = %ctx.config().self_evolution.workspaces.target_dir,
                "this process runs no builds; the build cache is measured where it is built"
            ),
        }

        // 产物级进化的自主触发者：拿本进程已记录的决策结果重放候选参数，
        // 显著更优才写新版本并热替换。与自发现信号 watcher 不同，它的副
        // 作用只落在本进程自己的策略目录里（`app.data_dir` 下的 PV），既
        // 不碰共享上游也不碰共享库表，所以不需要按部署指定属主——每个进程
        // 调的本来就是自己那套参数。配置解析失败要响亮失败，所以先读配置。
        let pe_config = crate::PolicyEvolutionConfig::load()?;
        if !pe_config.enabled {
            info!("artifact-level evolution driver disabled by config");
        } else if let (Some(engine), Some(evolution)) =
            (self.meta_learning.clone(), self.artifact_evolution.clone())
        {
            let shutdown = cog_core::ShutdownSignal::new();
            if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                let shutdown = shutdown.clone();
                let mut rx = broadcast_tx.0.subscribe();
                tokio::spawn(async move {
                    let _ = rx.recv().await;
                    shutdown.trigger();
                });
            }
            // 每轮结局读数的产出面。缺失时只在日志里交代一次，否则采纳与失败
            // 都不会离开那条随 Pod 消失的日志。
            let pe_metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
            if pe_metrics.is_none() {
                warn!(
                    "MetricsBackend not published; policy evolution round outcomes \
                     will not be reported"
                );
            }
            tokio::spawn(crate::run_policy_evolution_loop(
                Arc::new(
                    crate::PolicyEvolutionDriver::new(pe_config, engine, evolution)
                        .with_metrics(pe_metrics),
                ),
                shutdown,
            ));
        } else {
            info!("artifact-level evolution driver: no meta-learning engine; skipping");
        }

        if !self.porter_armed {
            return Ok(());
        }
        let Ok(project_root) = std::env::current_dir() else {
            warn!("no current dir; sandbox git loops disabled");
            return Ok(());
        };

        // 主线跟踪自动部署器：检测集群内 bare 仓库公版 main 前进 → 沙盒
        // 构建 → buildah 叠层推 registry → 派独立 Job 门禁滚动四部署。
        // 默认关闭，配置 enabled 才起循环（依赖 RBAC/registry 就位）。
        let ml_config = crate::MainlineDeployerConfig::load()?;
        if ml_config.enabled {
            let Some(workspaces) = self.workspaces.clone() else {
                warn!("mainline deployer enabled but workspace allocator missing; skipping");
                return Ok(());
            };
            let deployer = {
                // The distance between the declared ceilings and what the cluster
                // enforces. Attached to this loop rather than started on its own:
                // the declarations are only in hand at the moment a bundle is
                // assembled, and assembling one is this loop's own action, so the
                // process reading them shares its life. The handle itself was
                // published in init -- see the field's own note for why it cannot
                // be published from here.
                let deployer = crate::MainlineDeployer::new(ml_config.clone(), workspaces);
                let deployer = match self.governance_drift.clone() {
                    Some(drift) => deployer.with_governance_drift(drift),
                    // init publishes it under this same guard, so reaching here
                    // means the two conditions have drifted apart: the deployer
                    // runs and its reading nobody can see. Say so rather than
                    // failing the rollout over a metrics handle.
                    None => {
                        warn!(
                            "mainline deployer running with no governance drift handle; the \
                             ceiling comparison will reach no scrape"
                        );
                        deployer
                    }
                };
                // The version contract's readings are recorded apart from the
                // other judgements: they answer "which name is running, and how
                // far that name sits past a release", which is not the question
                // the build and rollout judgements ask.
                match ctx.consume_service::<dyn cog_core::MetricsBackend>() {
                    Some(metrics) => std::sync::Arc::new(deployer.with_metrics(metrics)),
                    None => std::sync::Arc::new(deployer),
                }
            };
            let shutdown = cog_core::ShutdownSignal::new();
            if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                let shutdown = shutdown.clone();
                let mut rx = broadcast_tx.0.subscribe();
                tokio::spawn(async move {
                    let _ = rx.recv().await;
                    shutdown.trigger();
                });
            }
            info!(
                bare = %ml_config.bare_repo,
                registry = %ml_config.registry,
                interval_secs = ml_config.poll_interval_secs,
                "mainline deployer enabled"
            );
            // 可观测性栈清单的收敛：那些清单只有安装脚本一条交付路径，装完
            // 一次之后仓库与现场各走各的。这一步与主线滚动分开跑——主线循环
            // 在"没有新 rev"时直接返回，挂上去会让这条收敛几乎不运行。
            if ml_config.observability_stack.enabled {
                crate::observability_stack::validate_config(&ml_config.observability_stack)?;
                // 没有持久化告警口，结论只进日志——一条只在日志里的事实和
                // 「没人报」长得一样。
                let alert_sink = ctx.consume_service::<dyn cog_core::PersistentAlertSink>();
                if alert_sink.is_none() {
                    warn!("PersistentAlertSink not published; observability stack verdicts stay report-only");
                }
                let stack = crate::observability_stack::StackConvergence::new(
                    deployer.clone(),
                    ml_config.observability_stack.clone(),
                    alert_sink,
                );
                info!(
                    namespace = %ml_config.observability_stack.namespace,
                    manifest_dir = %ml_config.observability_stack.manifest_dir,
                    interval_secs = ml_config.observability_stack.interval_secs,
                    "observability stack convergence enabled"
                );
                tokio::spawn(stack.run(shutdown.clone()));
            } else {
                info!("observability stack convergence disabled by config");
            }
            // 占用读数与"谁在推镜像"绑在同一个进程：registry 的唯一写者就是
            // 上面这条部署器循环，读它的进程因此与它同生共死。声明缺席时这里
            // 什么也不起——没有那张卷要量。
            if let Some(footprint) = self.registry_footprint.clone() {
                drop(crate::registry_footprint::spawn_watch(
                    footprint,
                    crate::registry_footprint::scan_interval_from_env(),
                    shutdown.clone(),
                ));
            }
            tokio::spawn(crate::run_mainline_loop(deployer, shutdown));
        } else {
            info!("mainline deployer disabled by config");
        }

        let bp_config = crate::BaselinePortConfig::load()?;
        if !bp_config.enabled {
            info!("baseline port trigger disabled by config");
            return Ok(());
        }

        // 无 orchestrator 也照常挂载：纯 cherry-pick 路径不依赖智能任务，
        // 只是冲突解决与语义吸收确认退化（porter 内部已按此降级）。
        if orchestrator.is_none() {
            info!("baseline port: no orchestrator; conflict resolution and semantic absorption checks degraded");
        }

        let instance_id = resolve_port_instance_id().await;
        let current_version = current_version(ctx);
        let state_path = std::path::PathBuf::from(format!(
            "{}/baseline-port-attempts.json",
            ctx.config().app.data_dir
        ));

        // 移植器按实例常驻一棵工作树：与部署器、引擎基线、各轮演进任务的工作
        // 树互不干涉，移植期间独占它做 checkout -B。
        let mut porter = if let Some(ws) = self.workspaces.clone() {
            let base = ws.resolve_base(&instance_id, &current_version).await;
            let spec = crate::workspace::WorkspaceSpec::persistent(
                format!("porter-{instance_id}"),
                crate::workspace::WorkspaceKind::Porter,
                base,
            );
            crate::BaselinePorter::new(ws.porter_workspace(&instance_id))
                .with_instance_id(instance_id.clone())
                .with_workspace(ws.clone(), spec)
                .with_target_dir(ws.target_dir().to_path_buf())
        } else {
            warn!(
                "no workspace allocator; baseline port falls back to the process working directory"
            );
            crate::BaselinePorter::new(&project_root).with_instance_id(instance_id.clone())
        };
        if let Some(orch) = orchestrator {
            porter = porter.with_orchestrator(orch);
        }
        // 池全灭时智能段（冲突解决/语义吸收/eval）退化，机械 cherry-pick 继续。
        if let Some(gate) = llm_gate {
            porter = porter.with_llm_gate(gate);
        }
        // 每轮结局读数的产出面：与 GitOps 拉取端同源。缺失时只在日志里交代一次，
        // 移植失败就不会只留在那条随 Pod 消失的日志里。
        let bp_metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
        if bp_metrics.is_none() {
            warn!("MetricsBackend not published; baseline port tick outcomes will not be reported");
        }
        porter = porter.with_metrics(bp_metrics);
        let porter = Arc::new(porter);

        let shutdown = cog_core::ShutdownSignal::new();
        if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
            let shutdown = shutdown.clone();
            let mut rx = broadcast_tx.0.subscribe();
            tokio::spawn(async move {
                let _ = rx.recv().await;
                shutdown.trigger();
            });
        }
        info!(
            instance = %instance_id,
            version = %current_version,
            repo = %project_root.display(),
            "baseline port trigger enabled"
        );
        tokio::spawn(crate::run_baseline_port_loop(
            porter,
            current_version,
            bp_config,
            state_path,
            shutdown,
        ));
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("ReflectionPlugin shutdown");
        Ok(())
    }
}

/// 当前运行版本号：部署注入的 COGNEVA_VERSION 优先（进化 Pod 种子即按它
/// 对齐基线），退化为应用配置版本；统一去掉 `v` 前缀。
fn current_version(ctx: &cog_core::PluginContext) -> String {
    std::env::var("COGNEVA_VERSION")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| ctx.config().app.version.clone())
        .trim_start_matches('v')
        .to_string()
}

/// 移植工作分支 `evol/<id>` 的实例 id 解析：显式 env 覆盖最优先，其次
/// 读 cog-github 身份状态文件里的 branch_id（同一机器两个 crate 推导出的
/// 分支名必须一致，否则 seed 对齐的 evol/* 与 porter 写出的 evol/<id>
/// 会错开），最后退化 "local"。cog-reflection 不依赖 cog-github，直接读
/// 其状态文件的 branch_id 字段。
async fn resolve_port_instance_id() -> String {
    if let Ok(v) = std::env::var("COGNEVA_INSTANCE_ID") {
        let v = v.trim();
        if !v.is_empty() {
            return v.to_string();
        }
    }
    let dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into());
    let path = std::path::PathBuf::from(dir).join("identity.json");
    if let Ok(text) = tokio::fs::read_to_string(&path).await {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(b) = v.get("branch_id").and_then(|x| x.as_str()) {
                if !b.is_empty() {
                    return b.to_string();
                }
            }
        }
    }
    "local".into()
}

/// Persist + dispatch a notification when the sandbox boundary check
/// downgrades the pipeline to dry-run. Both services are optional: when no
/// notification backend is configured, the warn log remains the only signal.
async fn notify_sandbox_downgrade(ctx: &cog_core::PluginContext, reason: &str) {
    let notification = cog_core::Notification {
        id: format!("sandbox-downgrade-{}", uuid::Uuid::new_v4()),
        title: "Self-evolution downgraded to dry-run".into(),
        body: reason.to_string(),
        is_read: false,
        created_at: chrono::Utc::now(),
        read_at: None,
    };

    let store = ctx.consume_service::<dyn cog_core::NotificationStore>();
    if let Some(store) = store {
        if let Err(e) = store.create(notification.clone()).await {
            warn!(error = %e, "Failed to persist sandbox-downgrade notification");
        }
    }

    let dispatcher = ctx.consume_service::<dyn cog_core::NotificationDispatcher>();
    if let Some(dispatcher) = dispatcher {
        if let Err(e) = dispatcher.dispatch(&notification).await {
            warn!(error = %e, "Failed to dispatch sandbox-downgrade notification");
        }
    }
}

/// Ensure the host environment is ready for self-evolution.
///
/// This function first checks, then attempts to repair the environment. In
/// `sandbox_mode` it is allowed to perform aggressive fixes (`chmod`,
/// `git init`, copying the current binary into place, installing missing
/// tools). Outside of a sandbox it stays conservative and only reports
/// actionable errors.
///
/// Checks / fixes:
/// - Required CLI tools (`cargo`, `git`, `rustc`) are on PATH.
/// - `project_root` exists and is a directory.
/// - `binary_dir` and `backup_dir` exist and are writable, and `change_dir`
///   does too — but only on the process that drains that queue.
/// - The project is inside a git repository.
/// - For `self_exec` switch mode, the current executable is installed at
///   the configured binary path.
async fn ensure_self_evolution_environment(
    project_root: &std::path::Path,
    config: &cog_core::SelfEvolutionConfig,
) -> cog_core::SFResult<()> {
    // Project root exists.
    if !project_root.is_dir() {
        return Err(cog_core::SFError::Config(format!(
            "project_root does not exist or is not a directory: {}",
            project_root.display()
        )));
    }

    // Required tools — install only when explicitly running in a sandbox.
    for tool in ["cargo", "git", "rustc"] {
        if !check_tool(tool).await && config.sandbox_mode {
            warn!(
                tool,
                "Missing build tool; attempting install because sandbox_mode=true"
            );
            if let Err(e) = install_tool(tool).await {
                warn!(tool, error = %e, "Automatic tool install failed");
            }
        }

        if !check_tool(tool).await {
            return Err(cog_core::SFError::Config(format!(
                "required build tool '{}' is not on PATH or not working. Please install it before enabling self-evolution.{}",
                tool,
                if config.sandbox_mode {
                    " Automatic install was attempted but failed."
                } else {
                    " Set self_evolution.sandbox_mode=true to allow automatic installation inside a sandbox."
                }
            )));
        }
    }

    // Resolve directories relative to project_root when they are relative paths.
    let resolve = |p: &str| -> std::path::PathBuf {
        let path = std::path::PathBuf::from(p);
        if path.is_absolute() {
            path
        } else {
            project_root.join(path)
        }
    };

    let change_dir = resolve(&config.change_dir);
    let binary_dir = resolve(&config.binary_dir);
    let backup_dir = resolve(&config.backup_dir);

    // Create and ensure writable directories.
    //
    // The queue directory belongs to the process that drains it. A process
    // without the executor role never receives a change to consume, so an
    // `evolution-changes` directory it created would sit empty on a filesystem
    // where nothing reads it, and "the queue is empty" is exactly the reading
    // that must not be manufactured: whoever inspects it cannot tell it from a
    // queue the reader is not allowed to see. The other two stay for every
    // process — staging and rolling back a binary is an action an admin API on
    // a non-executor can still take.
    let mut dirs: Vec<&std::path::PathBuf> = vec![&binary_dir, &backup_dir];
    if config.executor_enabled {
        dirs.push(&change_dir);
    }
    for dir in dirs {
        ensure_dir_writable(dir, config.sandbox_mode).await?;
    }

    // Inside a git repository — init if sandbox allows.
    ensure_git_repository(project_root, config.sandbox_mode).await?;

    // Self-exec path check — auto-deploy current binary if sandbox allows.
    if config.switch_mode == "self_exec" {
        ensure_binary_in_place(&binary_dir, config.sandbox_mode).await?;
    }

    Ok(())
}

async fn check_tool(tool: &str) -> bool {
    match tokio::process::Command::new(tool)
        .arg("--version")
        .output()
        .await
    {
        Ok(output) => output.status.success(),
        Err(_) => false,
    }
}

async fn install_tool(tool: &str) -> cog_core::SFResult<()> {
    match tool {
        "git" => install_git().await,
        "cargo" | "rustc" => install_rust().await,
        _ => Err(cog_core::SFError::Config(format!(
            "No automatic installer configured for tool {}",
            tool
        ))),
    }
}

async fn install_git() -> cog_core::SFResult<()> {
    info!("Attempting to install git via package manager");

    if check_tool("apt-get").await {
        run_command("apt-get", &["update"]).await?;
        run_command("apt-get", &["install", "-y", "git"]).await?;
    } else if check_tool("yum").await {
        run_command("yum", &["install", "-y", "git"]).await?;
    } else if check_tool("apk").await {
        run_command("apk", &["add", "git"]).await?;
    } else {
        return Err(cog_core::SFError::Config(
            "No supported package manager found for installing git".into(),
        ));
    }

    if !check_tool("git").await {
        return Err(cog_core::SFError::Config(
            "git installation reported success but git is still not on PATH".into(),
        ));
    }

    info!("git installed successfully");
    Ok(())
}

async fn install_rust() -> cog_core::SFResult<()> {
    info!("Attempting to install Rust via rustup");

    if check_tool("rustup").await {
        run_command("rustup", &["default", "stable"]).await?;
    } else {
        let rustup_init = std::env::temp_dir().join("rustup-init.sh");
        let output = tokio::process::Command::new("curl")
            .args([
                "--proto",
                "=https",
                "--tlsv1.2",
                "-sSf",
                "https://sh.rustup.rs",
            ])
            .output()
            .await
            .map_err(|e| cog_core::SFError::IO(format!("Failed to download rustup: {}", e)))?;

        if !output.status.success() {
            return Err(cog_core::SFError::IO(format!(
                "rustup download failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        tokio::fs::write(&rustup_init, &output.stdout).await?;
        run_command("sh", &[rustup_init.to_string_lossy().as_ref(), "-y"]).await?;
    }

    // Ensure cargo/rustc are on PATH for the current process by sourcing cargo env.
    let cargo_env = std::env::var_os("HOME")
        .map(|h| std::path::PathBuf::from(h).join(".cargo").join("env"))
        .filter(|p| p.exists());
    if let Some(env_path) = cargo_env {
        let _ = tokio::process::Command::new("sh")
            .args([
                "-c",
                &format!(
                    "source {} && cargo --version && rustc --version",
                    env_path.display()
                ),
            ])
            .output()
            .await;
    }

    if !check_tool("cargo").await || !check_tool("rustc").await {
        return Err(cog_core::SFError::Config(
            "Rust installation reported success but cargo/rustc are still not on PATH".into(),
        ));
    }

    info!("Rust installed successfully");
    Ok(())
}

async fn run_command(cmd: &str, args: &[&str]) -> cog_core::SFResult<()> {
    run_command_in_dir(cmd, args, None).await
}

async fn run_command_in_dir(
    cmd: &str,
    args: &[&str],
    dir: Option<&std::path::Path>,
) -> cog_core::SFResult<()> {
    let mut command = tokio::process::Command::new(cmd);
    command.args(args);
    if let Some(d) = dir {
        command.current_dir(d);
    }
    let output = command
        .output()
        .await
        .map_err(|e| cog_core::SFError::IO(format!("Failed to run {}: {}", cmd, e)))?;

    if !output.status.success() {
        return Err(cog_core::SFError::IO(format!(
            "{} {} failed: {}",
            cmd,
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

async fn ensure_dir_writable(dir: &std::path::Path, sandbox_mode: bool) -> cog_core::SFResult<()> {
    if let Err(e) = tokio::fs::create_dir_all(dir).await {
        return Err(cog_core::SFError::IO(format!(
            "Failed to create directory {}: {}. Please ensure the process has permission to create this directory or create it manually and grant write access.",
            dir.display(),
            e
        )));
    }

    // Test writability by creating a temp file.
    match tempfile::NamedTempFile::new_in(dir) {
        Ok(probe) => {
            drop(probe);
            Ok(())
        }
        Err(_) if sandbox_mode => {
            warn!(dir = %dir.display(), "Directory not writable; attempting chmod in sandbox mode");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = tokio::fs::metadata(dir).await?.permissions();
                perms.set_mode(perms.mode() | 0o777);
                tokio::fs::set_permissions(dir, perms).await?;
            }

            let probe = tempfile::NamedTempFile::new_in(dir).map_err(|e| {
                cog_core::SFError::IO(format!(
                    "Directory {} is still not writable after chmod: {}. Please grant the cogneva process write permission on this directory.",
                    dir.display(),
                    e
                ))
            })?;
            drop(probe);
            info!(dir = %dir.display(), "Directory made writable in sandbox mode");
            Ok(())
        }
        Err(e) => Err(cog_core::SFError::IO(format!(
            "Directory {} is not writable: {}. Please grant the cogneva process write permission on this directory. Set self_evolution.sandbox_mode=true to allow automatic chmod inside a sandbox.",
            dir.display(),
            e
        ))),
    }
}

async fn ensure_git_repository(
    project_root: &std::path::Path,
    sandbox_mode: bool,
) -> cog_core::SFResult<()> {
    let git_output = tokio::process::Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .current_dir(project_root)
        .output()
        .await
        .map_err(|e| {
            cog_core::SFError::IO(format!(
                "Failed to run git in {}: {}. Self-evolution requires a git repository.",
                project_root.display(),
                e
            ))
        })?;

    if git_output.status.success() {
        return Ok(());
    }

    if sandbox_mode {
        warn!(project_root = %project_root.display(), "Not a git repository; attempting git init in sandbox mode");
        run_command_in_dir("git", &["init"], Some(project_root))
            .await
            .map_err(|e| {
                cog_core::SFError::IO(format!(
                    "Failed to git init in {}: {}. Self-evolution requires a git repository.",
                    project_root.display(),
                    e
                ))
            })?;

        let git_output = tokio::process::Command::new("git")
            .args(["rev-parse", "--git-dir"])
            .current_dir(project_root)
            .output()
            .await
            .map_err(|e| {
                cog_core::SFError::IO(format!(
                    "git init succeeded but verification failed in {}: {}",
                    project_root.display(),
                    e
                ))
            })?;

        if git_output.status.success() {
            info!(project_root = %project_root.display(), "Initialized git repository in sandbox mode");
            return Ok(());
        }
    }

    let stderr = String::from_utf8_lossy(&git_output.stderr);
    Err(cog_core::SFError::Config(format!(
        "{} is not a git repository (git error: {}). Self-evolution requires git for rollback and deployment.{}",
        project_root.display(),
        stderr.trim(),
        if sandbox_mode {
            " Automatic git init was attempted but failed."
        } else {
            " Set self_evolution.sandbox_mode=true to allow automatic git init inside a sandbox."
        }
    )))
}

async fn ensure_binary_in_place(
    binary_dir: &std::path::Path,
    sandbox_mode: bool,
) -> cog_core::SFResult<()> {
    let expected_binary = binary_dir.join("cogneva");

    let current_exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Could not determine current executable path; skipping self_exec path check");
            return Ok(());
        }
    };

    let matches = match tokio::fs::canonicalize(&expected_binary).await {
        Ok(canon) => canon == current_exe,
        Err(_) => false,
    };

    if matches {
        return Ok(());
    }

    if sandbox_mode {
        warn!(
            current_exe = %current_exe.display(),
            expected = %expected_binary.display(),
            "Current executable path does not match configured binary_dir/cogneva; copying binary in sandbox mode"
        );
        // Stage then rename: another process may be executing the destination
        // (a previous instance or a dispatched Job), and truncating a binary
        // someone is running fails with ETXTBSY while exposing a half-written
        // executable to the exec that follows. The rename swaps whole files.
        let staged = binary_dir.join("cogneva.staging");
        tokio::fs::copy(&current_exe, &staged).await.map_err(|e| {
            cog_core::SFError::IO(format!(
                "Failed to copy current executable {} to {}: {}. Self-exec switch mode needs the binary at the configured binary_dir.",
                current_exe.display(),
                expected_binary.display(),
                e
            ))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755)).await?;
        }
        tokio::fs::rename(&staged, &expected_binary)
            .await
            .map_err(|e| {
                cog_core::SFError::IO(format!(
                    "Failed to install current executable at {}: {e}. Self-exec switch mode needs the binary at the configured binary_dir.",
                    expected_binary.display()
                ))
            })?;
        info!(path = %expected_binary.display(), "Copied current executable to configured binary path in sandbox mode");
        return Ok(());
    }

    warn!(
        current_exe = %current_exe.display(),
        expected = %expected_binary.display(),
        "Current executable path does not match configured binary_dir/cogneva. self_exec switch mode will replace {}, not the running process. Set self_evolution.sandbox_mode=true to allow automatic binary copy inside a sandbox.",
        expected_binary.display()
    );
    Ok(())
}

/// 一轮演进所需的共享依赖，逐轮不变。打包成一个结构体而不是摊成参数列表，
/// 免得每加一个消费者都要改所有调用点。
#[derive(Clone, Copy)]
struct CycleDeps<'a> {
    pipeline: &'a crate::ChangePipeline,
    deployer: &'a crate::EvolutionDeployer,
    binary_switcher: Option<&'a Arc<dyn cog_core::BinarySwitcher>>,
    engine: &'a Arc<crate::ReflectionEngine>,
    config: &'a cog_core::SelfEvolutionConfig,
    evolution_metrics: Option<&'a Arc<dyn cog_core::EvolutionMetrics>>,
    promoter: Option<&'a Arc<crate::AutoPromoter>>,
    workspaces: &'a Arc<crate::workspace::WorkspaceManager>,
    /// 变更上游通道：沙盒验收过的提交经它落到主分支。缺席时只做本地
    /// 构建部署，不做上游落地。
    landing: Option<&'a Arc<dyn cog_core::ChangeLanding>>,
    /// 本进程的 apply/test 飞行读数：起飞打戳、落地由守卫清除，年龄在抓取期
    /// 现算。变更在这段里不产出任何别的读数，因而"在飞"与"卡住"要靠它分开。
    flight: &'a Arc<crate::evolution_flight_readings::EvolutionFlightReadings>,
    /// 按变更一次的 Job 这条路的上界与落点。默认 `enabled=false`：执行留在常驻
    /// 进程里，与从前逐字相同。`enabled` 真的用来决定派不派 Job，`max_parallel`
    /// 真的用来限并发派发。
    change_job: &'a crate::config::ChangeJobConfig,
    /// 一条请求里不随变更变的那半（路径、预算、策略），与流水线、部署器同源。
    world: &'a crate::change_execution::ChangeExecutionWorld,
    /// 这一对的超时预算，也是两个超时读数（`cogneva_verification_*`）的落点。
    /// Job 里那两次运行搬回来的读数要并进**同一个**对象：读数跟着工作搬，不随着
    /// 进程留下，另开一份就成了同一个量的两个来源。
    budget: &'a Arc<crate::verification_budget::VerificationBudget>,
    /// 每次构建的代价按入口与结局分开记的那个读数族。Job 那条路的结局由执行侧
    /// 做出（只有它知道 cargo 是被预算杀的还是自己失败的），所以它的记账要在这里
    /// 补上——否则这个族在开关打开之后会安静地少掉那部分构建。
    build_readings: &'a Arc<crate::evolution_build_readings::EvolutionBuildReadings>,
    /// 本轮循环的心跳句柄。一轮消费多条变更，而每一条的活都可能长过声明的节拍；
    /// 逐条打点把"上一跳到现在"这一段收窄到**一条**变更，声明的工期界才盖得住。
    /// 只在轮首打点的话，界就得是整队之和，而那个数这条循环自己也不知道。
    beat: &'a cog_core::loop_health::Beat,
}

/// 把两条输入通道汇合成本轮要验证的变更集合。
///
/// 本地队列是"生成与验证必须同一个进程"留下的临时口；落地记录是这份交接的持久
/// 面——换一个部署生成、或者进程重启丢掉内存里的描述，记录仍然在。只认本地队列，
/// "某条主线后来验证它"这一支就不存在：记录会永远停在 unverified，既没人验也没人
/// 问。两条通道用同一个 id 指同一份变更，所以只按 id 去重、只验一次。
///
/// 返回的映射里是记录那份变更本身。落地要落它，而不是落一份从产物重建的副本——
/// 重建只剩描述与正文，记录里比产物多出来的字段（问题号、评审分）会被覆盖掉。
/// 队列里已有的变更也放进去：它同样有记录，只是这份记录还没被验过。
///
/// 汇合顺带判一次贡献面：判不过的变更**不进这一轮**，而是连同理由交回调用方收口
/// （收口要引擎与指标，这里没有）。判在这里而不是沙箱里，是因为沙箱的代价是那把
/// 唯一的构建槽——2026-09-27 一条被白名单拒掉的变更 6 小时里被 apply、测、冷编了
/// 12 次，每次都占着部署器推进也要的那把槽。判据归通道所有，这里只是问一句，不碰
/// 网络也不碰工作树，所以问到的答案与落地时给出的那个逐字相同。
async fn merge_verification_inputs(
    queued: Vec<crate::types::EvolutionResult>,
    landing: Option<&dyn cog_core::ChangeLanding>,
) -> (
    Vec<crate::types::EvolutionResult>,
    std::collections::HashMap<String, cog_core::GeneratedChange>,
    Vec<(String, String, Vec<std::path::PathBuf>)>,
) {
    let mut recorded: std::collections::HashMap<String, cog_core::GeneratedChange> =
        std::collections::HashMap::new();
    let Some(landing) = landing else {
        return (queued, recorded, Vec::new());
    };
    let list = match landing.unverified_changes().await {
        Ok(list) => list,
        Err(e) => {
            // 读不到记录不是"没有记录"：这一轮只验本地队列，但要把这件事说出来，
            // 否则"记录不可读"和"没有待验证记录"在日志里长得一模一样。
            warn!(
                error = %e,
                "Unverified landing records could not be read; verifying the local queue only"
            );
            return (queued, recorded, Vec::new());
        }
    };

    let mut changes = queued;
    for change in list {
        let already_queued = changes.iter().any(|c| c.artifact_id == change.change_id);
        if !already_queued {
            changes.push(crate::types::EvolutionResult {
                kind: crate::types::EvolutionKind::CodeChange,
                artifact_id: change.change_id.clone(),
                description: change.goal.clone(),
                content: change.content.clone(),
                status: crate::types::EvolutionStatus::CompileChecked,
                created_at: chrono::Utc::now(),
                eval_summary: None,
            });
        }
        recorded.insert(change.change_id.clone(), change);
    }

    // 每条变更只按"落地时会读的那份正文"判一次：记录在就是记录那份（落地落的也是
    // 它），否则队列那份。按 id 去重已经做过，所以同一份变更不会一个载体被拒、另一个
    // 载体照跑。
    let mut kept = Vec::with_capacity(changes.len());
    let mut refused = Vec::new();
    for change in changes {
        let diff = recorded
            .get(&change.artifact_id)
            .map(|c| c.content.as_str())
            .unwrap_or(change.content.as_str());
        match landing.check_contribution_allowed(diff) {
            Ok(()) => kept.push(change),
            Err(e) => {
                // 被拒的文件从**同一条规则**里取，而不是把这句英文再解析一遍：
                // 拒绝要按判据与文件归档，而重新读一遍同一条规则正是这两者与
                // 判词开始各说各话的方式。连路径都读不出来的 diff 保留拒绝本身，
                // 只丢掉名字。
                let files = landing.contribution_refusal_paths(diff).unwrap_or_default();
                // 记录不留给这一轮的落地：这条变更不会落地。真正的收口（两份载体
                // 一起退休）由调用方做，见 `run_evolution_cycle_in`。
                recorded.remove(&change.artifact_id);
                refused.push((change.artifact_id.clone(), e.to_string(), files));
            }
        }
    }
    (kept, recorded, refused)
}

/// 记录一次终局失败，并把变更移出待处理队列。
///
/// 两件事必须一起做：登记结论是审计线索，移出队列才让"拒绝"成为终局。队列本身
/// 就是变更目录，不带任何记忆；结论保存在引擎内存里，进程一重启就没了。记录一旦
/// 消失，目录里遗留的 .diff 和刚生成的变更完全无法区分（都读成 CompileChecked），
/// 于是一个永远打不上的变更会在部署存续期内每轮被重新 apply、每次都记一条学习，
/// 还把落地通道一直占住。
///
/// 只在"对变更本身的确定性判据"上退休（解析/校验/apply/测试），环境类失败
/// （校验管线报错、构建、落地、部署）不移除：那些是环境的问题，环境修好后同一个
/// 变更还该能落地。判据与环境的边界由 `apply_and_test_in` 的返回类型划开——判定走
/// `Ok(verdict: Refused(..))`，环境走 `Err`。
///
/// 同一个变更可能来自两条输入通道之一——本地队列（`.diff` 文件）或落地记录——
/// 所以两处都要收口，各自只认自己那一份，缺席的一方是空操作。只收口一边，另一边
/// 的输入会在下一轮原样回来：目录里的 `.diff` 被读成全新变更，记录被重新送进验证
/// 队列，于是同一个"打不上"的变更每轮重跑一次，永远不终局。
async fn fail_and_retire_change(
    engine: &crate::ReflectionEngine,
    pipeline: &crate::ChangePipeline,
    landing: Option<&dyn cog_core::ChangeLanding>,
    outcome: &crate::change_pipeline::ApplyResult,
) {
    let change_id = outcome.change_id.as_str();
    let reason = outcome.test_output.as_str();
    // 拒绝的结论要带上判据与它落在哪些文件上：学习里的重复计数就是按这两件事
    // 归并的，只留一句英文 dump 的话，两条不同的判据会被算成同一个反复出现的缺陷
    // ——而"反复出现"正是触发再生成的唯一依据。
    if let Some(cause) = outcome.verdict.cause() {
        let _ = engine
            .record_change_refusal(change_id, cause, &outcome.files_changed, reason)
            .await;
    } else {
        // 调用方只在判定类失败上走这条路，没有判据说明调用点错了；记一条不带
        // 判据的结论而不是凭空补一个，免得把"读不到判据"写成某个具体判据。
        warn!(
            change_id = %change_id,
            "A change was retired without a refusal cause; its record cannot be counted by criterion"
        );
        let _ = engine.record_change_outcome(change_id, false, reason).await;
    }
    retire_change_everywhere(
        pipeline,
        engine.evolution.as_ref(),
        landing,
        change_id,
        reason,
    )
    .await;
}

/// Take a change out of both places it can be offered from again.
///
/// The queue and the landing record are two independent input channels for the
/// same change, and each is read on its own: a `.diff` left in the directory
/// reads back as a change nobody has looked at yet, and an unverified record
/// is offered to whoever owns the verify loop. Retiring one side only lets the
/// other bring the change back on the next cycle, which is how a change that
/// can never land keeps being rebuilt.
async fn retire_change_everywhere(
    pipeline: &crate::ChangePipeline,
    evolution: Option<&Arc<crate::EvolutionEngine>>,
    landing: Option<&dyn cog_core::ChangeLanding>,
    change_id: &str,
    reason: &str,
) {
    if let Err(e) = pipeline.retire_change(change_id, reason).await {
        warn!(
            change_id = %change_id,
            error = %e,
            "Change could not be retired; it stays in the pending queue"
        );
    }
    drop_resident_record(pipeline, evolution, change_id).await;
    if let Some(landing) = landing {
        if let Err(e) = landing.retire_unverified(change_id, reason).await {
            warn!(
                change_id = %change_id,
                error = %e,
                "Change could not be settled on the landing channel; it stays unverified"
            );
        }
    }
}

/// Take a landed change out of the pending queue.
///
/// The retirement above is what makes "this change cannot be applied" terminal,
/// and it runs on the refusal path only. Landing is at least as terminal, and
/// had no such step: once the push succeeds the change is on the base branch,
/// and everything that follows it -- the release build, the promotion, the
/// deployer that ships revisions -- reads the base branch rather than this
/// directory. The entry is a leftover from that moment on.
///
/// A leftover is not inert. The next cycle finds the `.diff` in the queue,
/// cannot tell it from a change nobody has looked at yet, and verifies it from
/// scratch: apply, test, release build. That build takes the host's build slot,
/// and the slot is a single one shared with the deployer -- so the deployment
/// advancing past the revision the change just landed waits on the build of
/// the change it already has. On 2026-09-27 a change landed at 04:07, was still
/// the queue's only entry two hours later, and the deployer was refused the
/// build slot three times in twenty minutes by that change's own release build.
///
/// The entry is retired rather than deleted, so what was in the queue stays
/// readable after the fact.
async fn retire_landed_change(
    pipeline: &crate::ChangePipeline,
    evolution: Option<&Arc<crate::EvolutionEngine>>,
    change_id: &str,
) {
    if let Err(e) = pipeline
        .retire_change(change_id, "landed on the base branch")
        .await
    {
        warn!(
            change_id = %change_id,
            error = %e,
            "Change landed but could not be retired from the pending queue; it will be verified again next cycle"
        );
    }
    drop_resident_record(pipeline, evolution, change_id).await;
}

/// 产物退休之后，把常驻索引里那一格也丢掉。
///
/// 常驻索引只是队列的缓存，不是状态的事实来源——状态已经落在产物旁边的
/// 元数据记录里、随产物一起挪进 `retired/`。所以这一格的寿命就是"产物还在
/// 队列里"的寿命：产物退休了它就该跟着走，否则它只是把一份已经不活的状态
/// 一直占在内存里（每生成一条代码变更就多一格，直到进程重启）。
///
/// 丢之前由 `retire_result` 自己再断言一次产物确实已在 `retired/`——顺序在这里
/// 不靠调用方记得住：调用方多、每条路都可能失败，而这条断言只有一处。
async fn drop_resident_record(
    pipeline: &crate::ChangePipeline,
    evolution: Option<&Arc<crate::EvolutionEngine>>,
    change_id: &str,
) {
    if let Some(evolution) = evolution {
        evolution.retire_result(pipeline, change_id).await;
    }
}

/// Run one pass of the self-evolution auto-deploy pipeline.
///
/// 每轮演进独占一棵工作树（轮内多个变更串行复用），轮首刷新回基线：任何检出
/// 停在哪里都不影响部署器与别的轮次。工作树按实例常驻而非每轮新建，路径稳定才
/// 能让共享 target 目录命中增量缓存——换新路径会让全部本地 crate 重编一次。
async fn run_evolution_cycle(
    deps: CycleDeps<'_>,
    instance: &str,
    version: &str,
) -> cog_core::SFResult<()> {
    use crate::workspace::{WorkspaceKind, WorkspaceSpec};

    let base = deps.workspaces.resolve_base(instance, version).await;
    let workspace = deps
        .workspaces
        .ensure_persistent(WorkspaceSpec::persistent(
            format!("cycle-{instance}"),
            WorkspaceKind::Cycle,
            base.clone(),
        ))
        .await?;
    // 起点取本轮真正要工作的那棵树。版本 tag 落在主线历史上且落后于主线，而本轮
    // 马上会同步到主线；回退到 tag 再前进等于把两者之间每个文件重写两次，mtime
    // 一变，路径稳定换来的增量缓存就没了——正是上面那段注释要避免的事。
    let round_base = deps.workspaces.round_base(&workspace, base.clone()).await;
    deps.workspaces.refresh(&workspace, round_base).await?;
    info!(path = %workspace.path.display(), "evolution cycle workspace ready");

    // 引擎基线树在这里只保证存在；它停在哪由本轮工作树的实际 HEAD 决定，
    // 见 `align_engine_baseline`。这里若顺手 reset 一次，同步到主线后反而
    // 把基线留在解析出的旧基线上。
    if let Err(e) = deps
        .workspaces
        .ensure_persistent(WorkspaceSpec::persistent(
            "engine-baseline",
            WorkspaceKind::EngineBaseline,
            base,
        ))
        .await
    {
        warn!(error = %e, "engine baseline workspace unavailable");
    }

    run_evolution_cycle_in(deps, &workspace.path).await
}

/// 把引擎基线工作树挪到本轮工作树的 HEAD。
///
/// 引擎拿基线树跑 `git apply --check` 校验生成的变更，而变更真正被应用与测试
/// 的是本轮工作树。两棵树停在不同的提交上时，校验结论说的是另一棵树：主线新
/// 带进来的改动会让本来合法的变更被判成打不上，随后被丢弃却说不出原因。读不
/// 到本轮 HEAD 就不移动——没有证据时沿用上一轮基线，好过按猜的提交去 reset。
async fn align_engine_baseline(
    workspaces: &Arc<crate::workspace::WorkspaceManager>,
    workdir: &std::path::Path,
) {
    use crate::workspace::{BaseRef, WorkspaceKind, WorkspaceSpec};

    let Some(head) = workspaces.head_of(workdir).await else {
        warn!(
            path = %workdir.display(),
            "cycle workspace HEAD unreadable; engine baseline left where it was"
        );
        return;
    };
    let baseline = match workspaces
        .ensure_persistent(WorkspaceSpec::persistent(
            "engine-baseline",
            WorkspaceKind::EngineBaseline,
            BaseRef::Commit(head.clone()),
        ))
        .await
    {
        Ok(b) => b,
        Err(e) => {
            warn!(
                error = %e,
                "engine baseline workspace unavailable; change validation keeps the previous baseline"
            );
            return;
        }
    };
    if workspaces.head_of(&baseline.path).await.as_deref() == Some(head.as_str()) {
        return;
    }
    if let Err(e) = workspaces
        .refresh(&baseline, BaseRef::Commit(head.clone()))
        .await
    {
        warn!(
            error = %e,
            head = %head,
            "engine baseline refresh failed; change validation keeps the previous baseline"
        );
    } else {
        info!(head = %head, "engine baseline aligned to the round workspace");
    }
}

/// 一条变更执行完之后交到消费阶段的东西。
///
/// 执行（apply/fmt/test/commit/release/暂存）与消费（落地/切二进制/晋升）在这里
/// 分开：前者可以搬到别的进程里，后者不能——切二进制按定义只能由正在跑的那个进程
/// 做，落地与晋升要一个跨变更串行的账本。分开之后两条执行路径（本进程内 / 一个
/// Job）交出同一个形状，消费那一段因此只有一份，不会随执行方式分叉。
enum ExecutedChange {
    /// 没能做出判定，且坏在环境上。`stage` 是坏在哪一步，`None` 连结果都没读回来；
    /// 它只用来挑那句已有的措辞——判据不在文本里。
    Unjudged {
        change: crate::types::EvolutionResult,
        stage: Option<crate::change_execution::UnavailableStage>,
        error: String,
    },
    /// 主机整个等待预算里没给出构建槽位：什么都没被判定、什么也没失败。
    NoBuildSlot {
        change: crate::types::EvolutionResult,
        error: String,
    },
    /// 有判定。`held` 为真表示按配置停在人工审批上，此时按定义没有产物——它与
    /// "该有产物却缺了"是两回事，所以缺产物这件事必须由这个标志解释，不能靠推断。
    Judged {
        change: crate::types::EvolutionResult,
        result: crate::ApplyResult,
        artifact: Option<crate::BuildArtifact>,
        held: bool,
    },
}

/// 派发方的记账：这条变更派出去了，等着收回来。
enum DispatchedChange {
    /// 执行留在本进程里（默认）：没有可等的对象，收回来就是在这里把它跑完。
    InProcess {
        change: crate::types::EvolutionResult,
        intent: Option<cog_core::EvolutionIntent>,
    },
    /// 已经在集群里跑：等它结束，读回程。入口跟着走——构建的代价记在哪个入口的
    /// 账上是派发方知道的事，执行侧只报告结局。
    Job {
        change: crate::types::EvolutionResult,
        intent: Option<cog_core::EvolutionIntent>,
        plan: Box<crate::change_execution::ChangeJobPlan>,
    },
    /// 派发就没走成（基线读不到、清单造不出来、kubectl 起不来）。这条变更**没有被
    /// 执行过**，按"没能做出判定"收口；它是部署面的问题，不是变更的问题，所以理由
    /// 要带着走，不能假装没发生。
    Failed {
        change: crate::types::EvolutionResult,
        error: String,
    },
}

/// 本批的执行侧：每条变更由谁执行。
///
/// `job` 为 `None` 就是"执行留在常驻进程里"（默认）——那里没有别的东西在跑，执行
/// 就发生在调用它的这个进程里。
struct ChangeExecutor<'a> {
    job: Option<Box<crate::change_execution::ChangeJobContext>>,
    world: &'a crate::change_execution::ChangeExecutionWorld,
    /// 本轮的工作树：基线读它，进程内执行也在它里面跑。
    workdir: &'a std::path::Path,
    workspaces: &'a Arc<crate::workspace::WorkspaceManager>,
}

impl<'a> ChangeExecutor<'a> {
    /// 本批走哪条执行路径。
    ///
    /// 开关关着时连一次 kubectl 都不发生：否则"关"与"开着但派不出去"在集群侧读起来
    /// 一模一样。开关开着但现场读不回来（命名空间、Pod 名、镜像摘要、挂载面）时退回
    /// 进程内执行并留一句 warn——那条路把活干完，只是不并行，比丢掉这一轮强；而
    /// "这一轮没派 Job"必须说出来，不能退成沉默。
    async fn for_cycle(
        config: &crate::config::ChangeJobConfig,
        world: &'a crate::change_execution::ChangeExecutionWorld,
        workdir: &'a std::path::Path,
        workspaces: &'a Arc<crate::workspace::WorkspaceManager>,
    ) -> Self {
        let mut job = None;
        if config.enabled {
            match crate::change_execution::ChangeJobContext::read(config).await {
                Ok(ctx) => job = Some(Box::new(ctx)),
                Err(e) => warn!(
                    error = %e,
                    "change-execution jobs are enabled but the dispatch side cannot be read; \
                     executing inside this process for this cycle"
                ),
            }
        }
        Self {
            job,
            world,
            workdir,
            workspaces,
        }
    }

    /// 并发窗口。进程内那条路是 1：一次构建只有一份算力，"并行"在那里没有对应的
    /// 东西；窗口的意义只在于常驻进程这一侧最多同时有几个 Job 在跑。
    fn window(&self) -> usize {
        match &self.job {
            None => 1,
            Some(ctx) => ctx.config.max_parallel.max(1),
        }
    }

    /// 把这条变更派出去，不等它结束。
    async fn dispatch(
        &self,
        change: crate::types::EvolutionResult,
        intent: Option<cog_core::EvolutionIntent>,
    ) -> DispatchedChange {
        let Some(ctx) = &self.job else {
            return DispatchedChange::InProcess { change, intent };
        };
        // 基线是**派发这一刻**常驻进程那棵树上的 HEAD，不是本轮开始时的那个：
        // 执行侧要站在与它同一个提交上。本轮已经落地的变更都在这个 HEAD 里面，
        // 拿一个更旧的提交当基线，给出的判词就不是这棵树的判词。
        let Some(base) = self.workspaces.head_of(self.workdir).await else {
            let error = format!(
                "the base revision of the cycle workspace at {} could not be read",
                self.workdir.display()
            );
            warn!(change_id = %change.artifact_id, error = %error, "change not dispatched");
            return DispatchedChange::Failed { change, error };
        };
        let request = self.world.request(change.clone(), base, intent);
        match ctx.dispatch(&request).await {
            Ok(plan) => DispatchedChange::Job {
                change,
                intent,
                plan: Box::new(plan),
            },
            Err(e) => {
                warn!(change_id = %change.artifact_id, error = %e, "change not dispatched");
                DispatchedChange::Failed {
                    change,
                    error: e.to_string(),
                }
            }
        }
    }

    /// 等这条变更的执行结束，收成一个值。
    async fn collect(
        &self,
        dispatched: DispatchedChange,
        deps: CycleDeps<'_>,
        evo_engine: &crate::EvolutionEngine,
    ) -> ExecutedChange {
        match dispatched {
            DispatchedChange::Failed { change, error } => ExecutedChange::Unjudged {
                change,
                stage: None,
                error,
            },
            DispatchedChange::InProcess { change, intent } => {
                self.run_in_process(change, intent, deps, evo_engine).await
            }
            DispatchedChange::Job {
                change,
                intent,
                plan,
            } => match &self.job {
                Some(ctx) => collect_dispatched_job(ctx, *plan, change, intent, deps).await,
                // `dispatch` 只在 `job` 为真时给出 `Job`，这里不可能走到。
                None => ExecutedChange::Unjudged {
                    change,
                    stage: None,
                    error: "no change-execution job context for a dispatched change".into(),
                },
            },
        }
    }

    /// 进程内执行一条变更：与从前逐字相同的那一段，只是把结果收成一个值交出去。
    async fn run_in_process(
        &self,
        change: crate::types::EvolutionResult,
        intent: Option<cog_core::EvolutionIntent>,
        deps: CycleDeps<'_>,
        evo_engine: &crate::EvolutionEngine,
    ) -> ExecutedChange {
        let CycleDeps {
            pipeline,
            deployer,
            flight,
            evolution_metrics,
            config,
            ..
        } = deps;
        // The flight is the apply/test run and nothing after it: the commit, the
        // release build and the landing that follow each produce readings of
        // their own, and an age that covered them would report a state the
        // change is no longer in. The guard ends the reading on the way out of
        // every path -- a refusal, an error, a panic -- so the only way it can
        // read as still in flight is for the process to be inside it.
        let result = match flight
            .cover(pipeline.apply_and_test_in(&change, self.workdir))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                // `Err` 意味着管线没能对这个变更做出判定（工作树脏、git 起不来），
                // 判定类失败都以 `Ok(verdict: Refused(..))` 返回并在消费那一侧处理。
                return ExecutedChange::Unjudged {
                    change,
                    stage: Some(crate::change_execution::UnavailableStage::Verification),
                    error: e.to_string(),
                };
            }
        };

        evo_engine
            .update_status(&result.change_id, result.new_status)
            .await;

        if result.reformatted {
            // The change was conformed to this workspace's formatting and then
            // judged on what it does. Not a refusal and not a failure: the
            // count exists because the gate no longer returns such a change, so
            // without it a generator whose output never needs conforming and
            // one whose output always does read the same.
            info!(
                change_id = %result.change_id,
                "Change was conformed to the workspace's formatting before it was judged"
            );
            if let Some(m) = evolution_metrics {
                m.record_change_reformatted().await;
            }
        }

        if matches!(
            result.verdict,
            crate::change_pipeline::ChangeVerdict::Refused(_)
        ) {
            return ExecutedChange::Judged {
                change,
                result,
                artifact: None,
                held: false,
            };
        }

        // 等人工批准：管线按同一个判断把树回滚了，这条路上没有构建，也不该有。
        // 判词照给，"没有产物"作为一个事实由 `held` 解释。
        if !config.auto_apply || config.manual_approve {
            return ExecutedChange::Judged {
                change,
                result,
                artifact: None,
                held: true,
            };
        }

        match deployer
            .commit_and_build_in(&result.change_id, self.workdir, intent)
            .await
        {
            Ok(artifact) => ExecutedChange::Judged {
                change,
                result,
                artifact: Some(artifact),
                held: false,
            },
            // No slot for the whole wait budget: the build never ran, so nothing
            // about the change was judged and nothing failed. It is not a change
            // failure and does not go on that account -- the refusal is already
            // its own reading, and charging the host's load to the change is how
            // a busy machine reads as broken code.
            Err(e) if e.is_build_slot_refused() => ExecutedChange::NoBuildSlot {
                change,
                error: e.to_string(),
            },
            Err(e) => ExecutedChange::Unjudged {
                change,
                stage: Some(crate::change_execution::UnavailableStage::Build),
                error: e.to_string(),
            },
        }
    }
}

/// 等一个已经派出去的 Job 结束，把回程收成一次执行结果。
async fn collect_dispatched_job(
    ctx: &crate::change_execution::ChangeJobContext,
    plan: crate::change_execution::ChangeJobPlan,
    change: crate::types::EvolutionResult,
    intent: Option<cog_core::EvolutionIntent>,
    deps: CycleDeps<'_>,
) -> ExecutedChange {
    let returned = match ctx.collect(&plan).await {
        Ok(returned) => returned,
        Err(e) => {
            // 等待预算用尽、Pod 不在了、kubectl 起不来：这一类里没有判定，也没有
            // 哪一步失败的证据。变更留在队列里，下一轮接管它。
            return ExecutedChange::Unjudged {
                change,
                stage: None,
                error: e.to_string(),
            };
        }
    };

    let Some(outcome) = returned.outcome.as_ref() else {
        // 类别说不可用、又没有结果文件：连请求都没读进去，或者进程根本没起来，
        // 或者结果写不出去。这不是"构建失败"，是**没读回来**。
        return ExecutedChange::Unjudged {
            change,
            stage: None,
            error: format!(
                "the change job ended as {} without leaving an outcome",
                returned.category.as_str()
            ),
        };
    };

    // 读数跟着工作搬：Job 里那两次运行的耗时与超时次数并进常驻进程的同一个预算
    // 对象，于是 `cogneva_verification_*` 在执行搬走之后仍然覆盖这些运行。构建的
    // 结局只有执行侧知道（cargo 是被预算杀的还是自己失败的），所以它随回程一起
    // 搬回来，在这里记进同一个读数族。
    deps.budget.adopt_run(
        crate::verification_budget::KIND_TEST,
        outcome.test.last_secs,
        outcome.test.timeouts,
    );
    deps.budget.adopt_run(
        crate::verification_budget::KIND_BUILD,
        outcome.build.last_secs,
        outcome.build.timeouts,
    );
    if let Some(ending) = outcome.build_ending {
        deps.build_readings.record(intent, ending);
    }

    let result = match outcome.apply_result() {
        Ok(result) => result,
        Err(e) => {
            return ExecutedChange::Unjudged {
                change,
                stage: None,
                error: e.to_string(),
            }
        }
    };

    match returned.category {
        crate::change_execution::ExecutionCategory::Passed => {
            let artifact = outcome.build_artifact();
            let held = outcome.held_for_approval;
            ExecutedChange::Judged {
                change,
                result,
                artifact,
                held,
            }
        }
        crate::change_execution::ExecutionCategory::Refused => ExecutedChange::Judged {
            change,
            result,
            artifact: None,
            held: false,
        },
        crate::change_execution::ExecutionCategory::Unavailable => match &outcome.unavailable {
            // 主机没给槽位：这一轮这条变更没被判定过，也没有任何东西失败。
            Some(u) if u.cause == crate::change_execution::UnavailableCause::NoBuildSlot => {
                ExecutedChange::NoBuildSlot {
                    change,
                    error: u.reason.clone(),
                }
            }
            Some(u) => ExecutedChange::Unjudged {
                change,
                stage: Some(u.stage),
                error: u.reason.clone(),
            },
            None => ExecutedChange::Unjudged {
                change,
                stage: None,
                error: "the change job reports no verdict and no reason".into(),
            },
        },
    }
}

async fn run_evolution_cycle_in(
    deps: CycleDeps<'_>,
    workdir: &std::path::Path,
) -> cog_core::SFResult<()> {
    // 这一层只用得上通往执行与消费的那几项：部署器、切二进制、策略、晋级触发器
    // 都在消费那一段里按原样取用，在这里解出来只会多出四个没人读的名字。
    let CycleDeps {
        pipeline,
        engine,
        evolution_metrics,
        workspaces,
        landing,
        change_job,
        world,
        ..
    } = deps;
    let Some(evo_engine) = engine.evolution.as_ref() else {
        return Ok(());
    };

    // 先对齐上游主线再处理 change：沙盒树陈旧会让 GitOps 拉取端应用晋级
    // 产物时连带回退无关文件。同步失败不阻塞本轮（用当前树继续）。
    if let Err(e) = pipeline.sync_with_upstream_in(workdir).await {
        warn!(error = %e, "Sandbox source sync failed; continuing with current tree");
    }

    // 校验基线跟着本轮工作树走：同步后工作树已经在新主线上，基线还停在
    // 解析出的版本 tag 上就是两个提交。
    align_engine_baseline(workspaces, workdir).await;

    let queued = pipeline.pending_changes(Some(evo_engine)).await?;
    let (changes, recorded, refused) =
        merge_verification_inputs(queued, landing.map(|l| l.as_ref())).await;

    // 贡献面之外的变更在沙箱跑起来之前就收口：两份载体一起退休，并留下一条读数。
    // 判据是变更自身与这份部署的规则，重跑同一个变更只会得到同一个结论，所以两条
    // 路都只走一次——而"被拒"与"从没生成过"在记录里必须分得开，否则同一个缺口会
    // 一遍遍被重新发现。
    for (change_id, reason, files) in refused {
        warn!(
            change_id = %change_id,
            reason = %reason,
            "Change is outside the contributable surface; refused before the sandbox runs"
        );
        // 这一笔从前记成一条**没有判据**的 outcome，而 outcome 那条路有意不调
        // 触发器——两者合起来，一次贡献面拒绝既不进判据轴，也从不回到主流程，
        // 退休就成了一个没有出口的终态。记成 refusal 才带上判据、按判据计数，
        // 并交给回投那条路。
        let _ = engine
            .record_change_refusal(
                &change_id,
                cog_core::RejectionCause::ForbiddenPath,
                &files,
                &format!("Refused by the contribution rules: {reason}"),
            )
            .await;
        if let Some(m) = evolution_metrics {
            m.record_event(true).await;
            m.record_change_failed().await;
        }
        retire_change_everywhere(
            pipeline,
            engine.evolution.as_ref(),
            landing.map(|l| l.as_ref()),
            &change_id,
            &format!("the contribution rules refuse the change itself: {reason}"),
        )
        .await;
    }

    if changes.is_empty() {
        return Ok(());
    }

    info!(count = changes.len(), "Pending evolution changes found");
    // 执行侧：默认每条变更在本进程里跑完（与从前逐字相同）；`change_job.enabled`
    // 打开后每条交给一个一过性 Job。窗口只限**派发**：常驻进程这一侧仍然一次
    // 消费一条——落地、切二进制、晋升都是跨变更的单点，两处并行会把它们变成竞态，
    // 而这一项要搬的只是「执行」。
    let executor = ChangeExecutor::for_cycle(change_job, world, workdir, workspaces).await;
    let window = executor.window();
    let mut inflight: Vec<DispatchedChange> = Vec::new();
    let mut next = 0usize;
    // 派满窗口、收最老的那条、再补派。窗口为 1（默认，也是进程内那条路）时，
    // 这就是从前那个逐条循环：派一条、收一条、消费一条，顺序一字不差。
    while next < changes.len() || !inflight.is_empty() {
        // 逐条打点：上一跳到这一跳之间只有窗口里那些变更的活，声明的工期界盖的是
        // 一条。打完点再派，所以这一跳盖的是"接下来这一条"，而不是"刚过去那一条"。
        deps.beat.beat();
        while inflight.len() < window && next < changes.len() {
            let change = changes[next].clone();
            next += 1;
            let intent = recorded.get(&change.artifact_id).and_then(|c| c.intent);
            inflight.push(executor.dispatch(change, intent).await);
        }
        let dispatched = inflight.remove(0);
        let executed = executor.collect(dispatched, deps, evo_engine).await;
        consume_executed_change(executed, deps, &recorded).await?;
    }

    Ok(())
}

/// 消费一条执行完的变更：判定、落地、切二进制、记账。
///
/// 这一段与执行方式无关：它拿到的永远是同一个形状，无论判定来自本进程还是来自一个
/// 已经退出的 Job。「执行在哪儿发生」是这一项要改的事，而它不该改变任何一条消费
/// 规则——所以这里的分支与从前逐字相同，只是判定与产物从外面传进来。
async fn consume_executed_change(
    executed: ExecutedChange,
    deps: CycleDeps<'_>,
    recorded: &std::collections::HashMap<String, cog_core::GeneratedChange>,
) -> cog_core::SFResult<()> {
    let CycleDeps {
        pipeline,
        binary_switcher,
        engine,
        config,
        evolution_metrics,
        promoter,
        workspaces,
        landing,
        ..
    } = deps;

    let (change, result, artifact, held) = match executed {
        ExecutedChange::Unjudged {
            change,
            stage,
            error,
        } => {
            // 没能做出判定（工作树脏、git 起不来、结果没读回来）是**环境**问题：
            // 变更本身未必有毛病，重试有意义。所以只记结论、不移出队列——在这里
            // 退休会因一次环境抖动丢掉一个好变更。
            warn!(error = %error, "Change apply/test could not reach a verdict");
            let _ = engine
                .record_change_outcome(&change.artifact_id, false, &unjudged_message(stage, &error))
                .await;
            if let Some(m) = evolution_metrics {
                m.record_event(true).await;
                m.record_change_failed().await;
            }
            return Ok(());
        }
        ExecutedChange::NoBuildSlot { change, error } => {
            // 主机的负载不是变更的毛病：这一轮什么也没被判定，也什么都没失败。
            warn!(
                change_id = %change.artifact_id,
                error = %error,
                "Change not built this cycle: host had no build slot"
            );
            return Ok(());
        }
        ExecutedChange::Judged {
            change,
            result,
            artifact,
            held,
        } => (change, result, artifact, held),
    };

    if let Some(cause) = result.verdict.cause() {
        // The reason is already in the result; carrying it into the log is
        // what makes a rejection diagnosable without digging the artifact
        // out of the sandbox by hand.
        let reason: String = result.test_output.chars().take(500).collect();
        warn!(
            change_id = %result.change_id,
            status = ?result.new_status,
            cause = cause.as_str(),
            reason = %reason,
            "Change rejected; skipping deploy"
        );
        fail_and_retire_change(engine, pipeline, landing.map(|l| l.as_ref()), &result).await;
        // 被拒的这一笔与从前的尾部分支同源：一次否决同时进聚合与它自己的判据。
        if let Some(m) = evolution_metrics {
            m.record_event(true).await;
            m.record_change_failed().await;
            // Counted under its criterion as well as in the aggregate: the
            // aggregate says how much the loop is losing, the criterion
            // says whether to look at the generator, at what it reads from,
            // or at the verification run — three different repairs that one
            // number cannot tell apart.
            m.record_change_rejected(cause).await;
        }
        return Ok(());
    }

    if held {
        info!(change_id = %result.change_id, "Change awaiting manual approval");
        // 与从前逐字相同：判的是 auto_deploy（部署那道门），不是决定要不要构建的
        // auto_apply。两个开关不同向时这里会少记一笔，那是既有的形状，本项只把
        // 执行搬了家，不改它。
        if !config.auto_deploy || config.manual_approve {
            record_awaiting_approval(engine, evolution_metrics, &result.change_id).await;
        }
        return Ok(());
    }

    // 通过必然带产物：进程内那一侧按定义如此（没有产物就只可能是被拒或在等审批），
    // 跨进程那一侧由读回那一侧保证。走到这里说明那条保证被破坏了——绝不按通过
    // 放行：没有产物的「通过」会送一条没构建过的变更去落地。
    let Some(artifact) = artifact else {
        warn!(
            change_id = %result.change_id,
            "A passed change arrived with no artifact; treating it as an environment failure"
        );
        let _ = engine
            .record_change_outcome(
                &result.change_id,
                false,
                "Passed with no artifact: nothing was built to deploy",
            )
            .await;
        if let Some(m) = evolution_metrics {
            m.record_event(true).await;
            m.record_change_failed().await;
        }
        return Ok(());
    };

    info!(
        change_id = %artifact.change_id,
        commit = %artifact.commit_hash,
        "Change committed and built"
    );

    // 沙盒已经验过这个提交（apply → test → release build）：把它落到
    // 主分支上，而不是把这个变更留在沙盒里。落地必须先于二进制切换——
    // 切换会 exec 掉本进程，之后的代码在真实部署里永远不会执行。
    // 落地失败不放行部署：镜像里跑着主分支没有的代码，是最难排查的
    // 那种分叉。
    if let Some(landing) = landing {
        // 变更本来是记录里的那份就落它那份：从产物重建出来的副本只剩
        // 描述与正文，记录里比产物多出来的字段（问题号、评审分）会被
        // 落地的写回覆盖成默认值。
        let landed = recorded
            .get(&artifact.change_id)
            .cloned()
            .unwrap_or_else(|| cog_core::GeneratedChange {
                change_id: artifact.change_id.clone(),
                goal: change.description.clone(),
                content: change.content.clone(),
                affected_files: cog_core::parse_diff_affected_files(&change.content)
                    .unwrap_or_default(),
                ..Default::default()
            });
        let source = cog_core::LandedSource {
            repo: workspaces.bare_repo().to_path_buf(),
            rev: artifact.commit_hash.clone(),
        };
        match landing.land(&landed, Some(&source)).await {
            Ok(rev) => {
                info!(
                    change_id = %artifact.change_id,
                    rev = %rev,
                    "Change landed on the base branch"
                );
                retire_landed_change(pipeline, engine.evolution.as_ref(), &artifact.change_id)
                    .await;
            }
            Err(e) => {
                warn!(
                    change_id = %artifact.change_id,
                    error = %e,
                    "Landing on the base branch failed; change left undeployed"
                );
                let _ = engine
                    .record_change_outcome(
                        &artifact.change_id,
                        false,
                        &format!("Landing failed: {}", e),
                    )
                    .await;
                if let Some(m) = evolution_metrics {
                    m.record_event(true).await;
                    m.record_change_failed().await;
                }
                // The channel classified this failure as one the change's
                // own content caused -- a property of the change, not of
                // the world around it -- so running the change again
                // repeats the same refusal. Retire it rather than leaving
                // it in the queue: an entry left there is rebuilt from
                // scratch on the next cycle -- apply, the whole-workspace
                // test, the release build -- and that build takes the
                // single build slot the deployer needs to advance. On
                // 2026-09-27 one change the whitelist had already refused
                // was rebuilt and refused seven times in under two hours;
                // on 2026-10-04 a size refusal was re-driven the same way
                // at 22:17 and 22:30, off two rounds of test plus release
                // build.
                //
                // A category whose verdict says nothing about the change
                // keeps the behaviour it had and stays in the queue -- an
                // unreadable diff, which may yet be re-serialised into
                // one the gate can read, and a base that moved under the
                // change or a race it lost, which another attempt can
                // win. Those arrive as `Internal`, and a category this
                // side has never heard of keeps the same retryable
                // default rather than being retired unread. That default
                // is what keeps the unreadable case bounded: nothing else
                // reclaims it, so the queue re-offering it is the only
                // thing that brings it back for another look.
                if matches!(&e, cog_core::SFError::Validation(_)) {
                    // Retiring the entry must not drop the requirement. A
                    // refusal the channel carries as `Validation` is one the
                    // change's own content caused, so the defect is still
                    // there; recording the refusal is what hands it to the
                    // rework path, which regenerates it as a change that
                    // passes the gate. Both owner-side landing gates arrive
                    // here -- `forbidden_paths` and the size cap -- and that
                    // pair is what `PromotionGateRefused` names.
                    let refused_files: Vec<std::path::PathBuf> = landed
                        .affected_files
                        .iter()
                        .map(std::path::PathBuf::from)
                        .collect();
                    let _ = engine
                        .record_change_refusal(
                            &artifact.change_id,
                            cog_core::RejectionCause::PromotionGateRefused,
                            &refused_files,
                            &format!("Landing refused the change itself: {e}"),
                        )
                        .await;
                    retire_change_everywhere(
                        pipeline,
                        engine.evolution.as_ref(),
                        Some(landing.as_ref()),
                        &artifact.change_id,
                        &format!("landing refused the change itself: {e}"),
                    )
                    .await;
                }
                return Ok(());
            }
        }
    }

    if !config.auto_deploy {
        info!(change_id = %artifact.change_id, "Build artifact awaiting manual deploy");
    } else {
        let Some(switcher) = binary_switcher else {
            warn!("auto_deploy enabled but no BinarySwitcher service available");
            let _ = engine
                .record_change_outcome(
                    &artifact.change_id,
                    false,
                    "No BinarySwitcher service available",
                )
                .await;
            if let Some(m) = evolution_metrics {
                m.record_event(true).await;
                m.record_change_failed().await;
            }
            return Ok(());
        };

        if let Err(e) = switcher.stage_new_binary(&artifact.new_binary_path).await {
            warn!(change_id = %artifact.change_id, error = %e, "Staging failed");
            let _ = engine
                .record_change_outcome(
                    &artifact.change_id,
                    false,
                    &format!("Staging failed: {}", e),
                )
                .await;
            if let Some(m) = evolution_metrics {
                m.record_event(true).await;
                m.record_change_failed().await;
            }
            return Ok(());
        }
        info!(change_id = %artifact.change_id, "Staging new binary for switch");

        // 晋级判定在切换**之前**交出去（soak → 分级 → GitOps/审批台）。
        // self_exec 的切换就是本进程的 execve，成功即不返回——挂在它后面的回调
        // 永不执行，而这是晋级台账唯一可达的写者，所以判定不能长在本轮栈上。
        // 显式带上待发布提交：本轮工作树是临时的，判定要等 soak 期满才做，
        // 那时工作树可能已经归还，推送端只按裸仓库里的提交发布。
        let handed_off = if promoter.is_some() {
            let source = crate::PromotionSource {
                repo: workspaces.bare_repo().to_path_buf(),
                rev: artifact.commit_hash.clone(),
            };
            match crate::pending_promotions::hand_off(&change, &source).await {
                Ok(path) => Some(path),
                Err(e) => {
                    warn!(
                        change_id = %artifact.change_id,
                        error = %e,
                        "cannot hand off the promotion decision; this change will not be promoted"
                    );
                    None
                }
            }
        } else {
            None
        };

        // switch_and_restart may exec the current process and never return.
        if let Err(e) = switcher.switch_and_restart().await {
            warn!(error = %e, "Switch failed; attempting rollback");
            // 切换没成功就不存在「沙盒部署成功」这一件事：把交接撤掉，别让一条
            // 从没上过沙盒的变更凭一次没发生的切换去晋级。
            if let Some(path) = handed_off {
                crate::pending_promotions::withdraw(&path).await;
            }
            if let Err(rb_e) = switcher.rollback().await {
                warn!(error = %rb_e, "Rollback failed");
            }
            let _ = engine
                .record_change_outcome(&artifact.change_id, false, &format!("Switch failed: {}", e))
                .await;
            if let Some(m) = evolution_metrics {
                m.record_event(true).await;
                m.record_change_failed().await;
            }
            return Err(e);
        }

        // Reaching this point means the switch is the returning kind (systemd /
        // sidecar / image rollout) — `self_exec` replaced the process image at
        // the line above. So this bookkeeping only runs in those modes; that is
        // why `evolution_change_applied_total` is flat at zero in a `self_exec`
        // deployment, and it does not mean no change ever deployed. The exposed
        // name is exactly that, with no `cogneva_` prefix; the prefixed
        // `cogneva_change_fate_total` is a different series, the upstream
        // landing funnel. Promotion is not decided here: it was handed off
        // above and the next round's `drain_handed_off` picks it up, the same
        // way in both modes.
        let _ = engine
            .record_change_outcome(
                &artifact.change_id,
                true,
                "Change applied, tested, built, and deployed",
            )
            .await;
        if let Some(m) = evolution_metrics {
            m.record_change_applied().await;
            m.record_event(false).await;
        }
        return Ok(());
    }

    // 落了地但没有部署（auto_deploy 关着）：与从前逐字相同的那一笔。
    if !config.auto_deploy || config.manual_approve {
        record_awaiting_approval(engine, evolution_metrics, &result.change_id).await;
    }
    Ok(())
}

/// 「测试过了、在等人工批准」这一笔：既用在按配置停在审批上的那条路，也用在构建
/// 好了但没部署的那条路。措辞与从前一字不差。
async fn record_awaiting_approval(
    engine: &crate::ReflectionEngine,
    metrics: Option<&Arc<dyn cog_core::EvolutionMetrics>>,
    change_id: &str,
) {
    // Change succeeded tests but is waiting for approval; count as
    // a successful processing step without applying/deploying.
    let _ = engine
        .record_change_outcome(
            change_id,
            true,
            "Change passed tests; awaiting manual approval",
        )
        .await;
    if let Some(m) = metrics {
        m.record_event(false).await;
    }
}

/// 执行侧报回来的「没做出判定」该写成哪句话。
///
/// 措辞在这一侧定：跨进程搬回来的只有「坏在哪一步」这个事实，文本本身不是判据，
/// 也不该被拿去做判据。
fn unjudged_message(
    stage: Option<crate::change_execution::UnavailableStage>,
    reason: &str,
) -> String {
    use crate::change_execution::UnavailableStage;
    match stage {
        // 与进程内那条路上已有的两句话同源：构建失败与「管线没能判定」要落在
        // 不同的账上，合并成一句就是把归因丢掉。
        Some(UnavailableStage::Build) => format!("Build failed: {reason}"),
        Some(UnavailableStage::Verification) => format!("Pipeline error: {reason}"),
        // 连执行都没读回来（Job 没起来、结果没写出来、等待预算用尽）：它既不是
        // 「管线没能判定」，也不是「构建失败」，所以不借用那两句话。
        None => format!("Change execution could not be read back: {reason}"),
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "reflection",
    requires: &["skill", "prompt", "agent"],
    // supervisor 可选：其 SchedulerGate 用于池全灭时暂停 LLM 依赖型循环；
    // 不能写成 requires（supervisor 反向可选依赖 reflection，会成环）。
    optional_requires: &["llm", "memory", "supervisor"],
    factory: || Box::new(ReflectionPlugin::new()),
};

#[cfg(test)]
mod tests {
    use super::*;

    /// 执行侧的"没做出判定"按**坏在哪一步**分账，不合并成一句话。
    ///
    /// 两句话各自对上进程内那条路上原有的写法：构建失败与管线没能判定要落在不同的
    /// 账上。而"连执行都没读回来"（Job 没起来、结果没写出来、等待预算用尽）是第三
    /// 种，不许借用前两句话——那会把归因从"没读到"改成"某一步失败了"，判词里就有
    /// 了一个没发生过的失败。
    #[test]
    fn an_unreadable_execution_does_not_borrow_a_step_failure_wording() {
        use crate::change_execution::UnavailableStage;
        assert_eq!(
            unjudged_message(Some(UnavailableStage::Verification), "dirty working tree"),
            "Pipeline error: dirty working tree"
        );
        assert_eq!(
            unjudged_message(Some(UnavailableStage::Build), "no build slot"),
            "Build failed: no build slot"
        );
        let unread = unjudged_message(None, "the change job never started");
        assert!(
            !unread.starts_with("Pipeline error") && !unread.starts_with("Build failed"),
            "an unreadable execution borrowed a failure wording: {unread}"
        );
        // 理由必须带着走：没有理由的"不可用"在读的人那里等于没有读数。
        assert!(unread.contains("the change job never started"), "{unread}");
    }

    /// 落地通道替身：验证循环从它只读"待验证的记录"。其余方法一律炸掉——输入
    /// 汇合这一步不该落地、不该记录、也不该退休。
    #[derive(Debug)]
    struct FakeLanding {
        unverified: Vec<cog_core::GeneratedChange>,
        readable: bool,
        /// 通道对"这条变更能不能被贡献"的回答。汇合这一步会问它，所以这个替身不能
        /// 像别的方法那样炸掉：判在沙箱之前正是这一轮要钉住的行为。
        contributable: bool,
    }

    #[async_trait::async_trait]
    impl cog_core::ChangeLanding for FakeLanding {
        async fn land(
            &self,
            _change: &cog_core::GeneratedChange,
            _source: Option<&cog_core::LandedSource>,
        ) -> cog_core::SFResult<String> {
            unreachable!("input merging never lands a change")
        }

        async fn record_unverified(
            &self,
            _change: &cog_core::GeneratedChange,
        ) -> cog_core::SFResult<()> {
            unreachable!("input merging never records a change")
        }

        async fn unverified_changes(&self) -> cog_core::SFResult<Vec<cog_core::GeneratedChange>> {
            if !self.readable {
                return Err(cog_core::SFError::IO("landing dir unreadable".into()));
            }
            Ok(self.unverified.clone())
        }

        fn check_contribution_allowed(&self, _diff: &str) -> cog_core::SFResult<()> {
            if self.contributable {
                Ok(())
            } else {
                Err(cog_core::SFError::Validation(
                    "change touches non-contributable paths: crates/x/tests/y.rs".into(),
                ))
            }
        }

        /// 与 `check_contribution_allowed` 同源：放行时两边都空，拒绝时两边都
        /// 指名同一个文件。两份答案各写一份清单就会漂开，而这条替身存在的意义
        /// 正是"判据只有一份"。
        fn contribution_refusal_paths(
            &self,
            _diff: &str,
        ) -> cog_core::SFResult<Vec<std::path::PathBuf>> {
            if self.contributable {
                Ok(Vec::new())
            } else {
                Ok(vec![std::path::PathBuf::from("crates/x/tests/y.rs")])
            }
        }

        async fn retire_unverified(&self, _id: &str, _reason: &str) -> cog_core::SFResult<()> {
            unreachable!("input merging never retires a change")
        }
    }

    fn generated(id: &str, goal: &str) -> cog_core::GeneratedChange {
        cog_core::GeneratedChange {
            change_id: id.into(),
            goal: goal.into(),
            content: "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n".into(),
            affected_files: vec!["crates/x/src/lib.rs".into()],
            rationale: Some("because".into()),
            pge_mode: "squad".into(),
            self_review_score: Some(0.85),
            issue_number: Some(4),
            intent: Some(cog_core::EvolutionIntent::IssueFix),
        }
    }

    fn queued(id: &str, description: &str) -> crate::types::EvolutionResult {
        crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: id.into(),
            description: description.into(),
            content: "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n".into(),
            status: crate::types::EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        }
    }

    /// 契约承诺 unverified 的变更"等某条主线后来验证它"才落地。这条支线要真的
    /// 存在，验证循环就必须把记录当成一份输入——只有本地队列时，一条只留在记录里
    /// 的变更永远进不了验证，那句承诺就是空的。
    #[tokio::test]
    async fn a_record_without_a_local_diff_enters_verification() {
        let landing = FakeLanding {
            unverified: vec![generated("github-issue-4-abc", "Fix issue 4")],
            readable: true,
            contributable: true,
        };

        let (changes, recorded, refused) =
            merge_verification_inputs(Vec::new(), Some(&landing)).await;

        assert_eq!(changes.len(), 1, "记录必须成为本轮要验证的变更");
        assert_eq!(changes[0].artifact_id, "github-issue-4-abc");
        assert_eq!(
            changes[0].description, "Fix issue 4",
            "描述取记录里的目标，不是从临时文件路径猜出来的"
        );
        let kept = recorded
            .get("github-issue-4-abc")
            .expect("记录那份变更要留着");
        assert_eq!(
            kept.issue_number,
            Some(4),
            "落地要落记录那份：从产物重建的副本会把这个字段抹成默认值"
        );
        assert!(
            refused.is_empty(),
            "贡献面之内的变更一个都不许被提前拒掉：拒是终局，误拒等于把能落的活扔掉"
        );
    }

    /// 两条通道用同一个 id 指同一份变更：生成它的进程写本地队列，提交时又留下
    /// 一条记录。汇合后只能剩一条，否则同一份变更被 apply 两次。
    #[tokio::test]
    async fn a_change_present_in_both_channels_is_verified_once() {
        let landing = FakeLanding {
            unverified: vec![generated("chg-1", "goal from the record")],
            readable: true,
            contributable: true,
        };

        let (changes, recorded, refused) =
            merge_verification_inputs(vec![queued("chg-1", "goal from the queue")], Some(&landing))
                .await;

        assert_eq!(changes.len(), 1, "同一份变更只能被验一次");
        assert_eq!(
            changes[0].description, "goal from the queue",
            "队列里已有的那份保留原样，记录只补队列没有的"
        );
        assert!(
            recorded.contains_key("chg-1"),
            "变更就算来自队列，它的记录也要留给落地用"
        );
        assert!(
            refused.is_empty(),
            "两份载体都指同一份变更时也没有可拒的东西"
        );
    }

    /// 贡献面之外的变更**不进这一轮**——判在沙箱之前，因为沙箱的代价是那把唯一的
    /// 构建槽：2026-09-27 一条被白名单拒掉的变更 6 小时里被冷编了 12 次，每次都占着
    /// 部署器推进也要的那把槽。判据只有通道手里那一份，这里只是问一句。
    ///
    /// 三句都要：这条变更确实在供货（否则"没进这一轮"可能只是探针没伸到）；它没进
    /// 要验证的集合；它被交回调用方收口——静默丢掉与"从没生成过"在记录里长得一样。
    #[tokio::test]
    async fn a_change_outside_the_contributable_surface_never_enters_the_round() {
        let landing = FakeLanding {
            unverified: vec![generated("chg-1", "goal from the record")],
            readable: true,
            contributable: false,
        };

        assert_eq!(
            cog_core::ChangeLanding::unverified_changes(&landing)
                .await
                .unwrap()
                .len(),
            1,
            "记录必须在供货，否则下面的空集什么都证明不了"
        );

        let (changes, recorded, refused) =
            merge_verification_inputs(vec![queued("chg-1", "goal from the queue")], Some(&landing))
                .await;

        assert_eq!(changes.len(), 0, "贡献不出去的变更不许进这一轮");
        assert!(recorded.is_empty(), "它的记录也不留给落地");
        assert_eq!(refused.len(), 1, "被拒的要交回调用方收口，不能静默丢掉");
        assert_eq!(refused[0].0, "chg-1");
        assert!(
            refused[0].1.contains("non-contributable"),
            "理由要带通道给出的原因：{}",
            refused[0].1
        );
        // 被拒的路径要和拒绝一起交回，且来自通道自己的那条规则：调用方要按
        // 「判据 + 文件」归档这笔拒绝，而从句子里再解析一遍就是第二个判定者。
        assert_eq!(
            refused[0].2,
            vec![std::path::PathBuf::from("crates/x/tests/y.rs")],
            "被拒的路径必须随拒绝一起过来，否则归档时只能去拆那句英文"
        );
    }

    /// 读不到记录不是"没有记录"：读失败时本轮只验本地队列，但不能连本地队列也
    /// 丢掉，否则一次读抖动就让待验证的变更集体消失。
    #[tokio::test]
    async fn an_unreadable_record_set_leaves_the_local_queue_intact() {
        let landing = FakeLanding {
            unverified: Vec::new(),
            readable: false,
            contributable: true,
        };

        let (changes, recorded, refused) =
            merge_verification_inputs(vec![queued("chg-1", "goal")], Some(&landing)).await;

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].artifact_id, "chg-1");
        assert!(recorded.is_empty());
        assert!(
            refused.is_empty(),
            "记录读不到只是少一份输入，本地队列那份照样进这一轮，不许变成一次终局拒绝"
        );
    }

    /// 未连平台账号时没有落地通道，行为要与从前一致：只验本地队列。
    #[tokio::test]
    async fn without_a_landing_channel_the_local_queue_is_unchanged() {
        let (changes, recorded, refused) =
            merge_verification_inputs(vec![queued("chg-1", "goal")], None).await;

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].artifact_id, "chg-1");
        assert!(recorded.is_empty());
        assert!(
            refused.is_empty(),
            "没有落地通道就没有贡献面可判，本地队列不许被这条判据挡下"
        );
    }

    /// A change that lands leaves the pending queue.
    ///
    /// Retirement was written for the refusal path alone, so a change that
    /// reached the base branch stayed in the queue and was read as untouched
    /// work on the next cycle: applied, tested, and release-built again, on the
    /// build slot the deployer advancing past that very revision also needs.
    ///
    /// The judgement is in two halves and neither is enough on its own. The
    /// first says the queue really does offer this change, so the empty set
    /// afterwards cannot be a probe that never reached the file; the second
    /// says it stops offering it once the change has landed.
    #[tokio::test]
    async fn a_landed_change_leaves_the_pending_queue() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        tokio::fs::write(
            change_dir.join("chg-1.diff"),
            "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n",
        )
        .await
        .unwrap();
        let pipeline = crate::ChangePipeline::new(temp.path(), &change_dir, true);

        assert_eq!(
            pipeline.pending_changes(None).await.unwrap().len(),
            1,
            "the queue has to be offering this change, or the assertion below proves nothing"
        );

        retire_landed_change(&pipeline, None, "chg-1").await;

        assert!(
            pipeline.pending_changes(None).await.unwrap().is_empty(),
            "a landed change must not be offered for verification again"
        );
        // Retired rather than deleted: what was in the queue stays readable.
        assert!(change_dir.join("retired/chg-1.diff").exists());
    }

    /// 落地通道替身：记下退休调用。其余方法一律炸掉——退休一条变更不该落地、
    /// 也不该记录。
    #[derive(Debug)]
    struct RecordingLanding {
        unverified: std::sync::Mutex<Vec<cog_core::GeneratedChange>>,
        retired: std::sync::Mutex<Vec<(String, String)>>,
    }

    #[async_trait::async_trait]
    impl cog_core::ChangeLanding for RecordingLanding {
        async fn land(
            &self,
            _change: &cog_core::GeneratedChange,
            _source: Option<&cog_core::LandedSource>,
        ) -> cog_core::SFResult<String> {
            unreachable!("retiring a change never lands one")
        }

        async fn record_unverified(
            &self,
            _change: &cog_core::GeneratedChange,
        ) -> cog_core::SFResult<()> {
            unreachable!("retiring a change never records one")
        }

        async fn unverified_changes(&self) -> cog_core::SFResult<Vec<cog_core::GeneratedChange>> {
            Ok(self.unverified.lock().unwrap().clone())
        }

        /// 这些测试钉的是"一条变更要从两份载体里一起退休"，不是贡献面的判定本身
        /// （那一条在 `FakeLanding` 侧有自己的用例），所以这里一律放行——放行也
        /// 是让下面那些断言仍然只测它要测的那件事。
        fn check_contribution_allowed(&self, _diff: &str) -> cog_core::SFResult<()> {
            Ok(())
        }

        fn contribution_refusal_paths(
            &self,
            _diff: &str,
        ) -> cog_core::SFResult<Vec<std::path::PathBuf>> {
            Ok(Vec::new())
        }

        /// 与真实通道同语义，不只是记一笔：收口就是把记录**移出未验证集**，而
        /// 不是未验证的记录（已落地、已退休、或压根不在这一侧）原样不动。
        /// 只记日志的替身会继续把这条变更供出去，那正好是这条测试要证伪的。
        async fn retire_unverified(&self, id: &str, reason: &str) -> cog_core::SFResult<()> {
            let mut unverified = self.unverified.lock().unwrap();
            if let Some(pos) = unverified.iter().position(|c| c.change_id == id) {
                unverified.remove(pos);
                self.retired
                    .lock()
                    .unwrap()
                    .push((id.to_string(), reason.to_string()));
            }
            Ok(())
        }
    }

    /// 一份同时摆上两份载体的变更：队列里的 `.diff` 与记录里的那条。
    fn queued_and_recorded() -> (tempfile::TempDir, crate::ChangePipeline, RecordingLanding) {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        std::fs::create_dir_all(&change_dir).unwrap();
        std::fs::write(
            change_dir.join("chg-1.diff"),
            "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n",
        )
        .unwrap();
        let pipeline = crate::ChangePipeline::new(temp.path(), &change_dir, true);
        let landing = RecordingLanding {
            unverified: std::sync::Mutex::new(vec![generated("chg-1", "goal from the record")]),
            retired: std::sync::Mutex::new(Vec::new()),
        };
        (temp, pipeline, landing)
    }

    /// 一条被通道拒死的变更必须**同时**离开两份载体。
    ///
    /// 队列里的 `.diff` 与落地记录各自独立供货：只收口一边，另一边下一轮照样
    /// 把这条变更送进验证队列，代价是再 apply、再测、再发布构建，占的是部署器
    /// 推进也要的那把唯一的槽。2026-09-27 那条被白名单拒掉的变更，6 小时里被
    /// 构建了 12 次。
    ///
    /// 判据分两半：前半说两份载体**都在供货**，所以后面的空集不可能是"探针根本
    /// 没伸到那里"；后半说下一轮那次汇合——验证循环自己调的那个函数——在两份
    /// 载体里都找不到它。
    #[tokio::test]
    async fn a_refused_change_leaves_both_channels() {
        let (_temp, pipeline, landing) = queued_and_recorded();

        assert_eq!(
            pipeline.pending_changes(None).await.unwrap().len(),
            1,
            "队列必须在供货，否则下面的空集什么都证明不了"
        );
        assert_eq!(
            cog_core::ChangeLanding::unverified_changes(&landing)
                .await
                .unwrap()
                .len(),
            1,
            "记录也在供货：只清队列那一半，这条变更下一轮照样回来"
        );

        retire_change_everywhere(
            &pipeline,
            None,
            Some(&landing),
            "chg-1",
            "landing refused the change",
        )
        .await;

        assert_eq!(
            landing.retired.lock().unwrap().as_slice(),
            [(
                "chg-1".to_string(),
                "landing refused the change".to_string()
            )],
            "记录那份要一起收口，并带上关掉它的原因"
        );
        let (again, _, refused) = merge_verification_inputs(
            pipeline.pending_changes(None).await.unwrap(),
            Some(&landing),
        )
        .await;
        assert!(again.is_empty(), "下一轮不该在两份载体里再找到它");
        assert!(
            refused.is_empty(),
            "已经退休的变更不属于本轮被拒的那一类：它压根没进供货，再拒一次等于给它记第二笔"
        );
    }

    /// 只收口一边不算退休。这条是"成对"那一半的证伪件：队列清干净了，记录
    /// 仍把同一条变更交给下一轮。
    #[tokio::test]
    async fn one_channel_alone_brings_a_retired_change_back() {
        let (_temp, pipeline, landing) = queued_and_recorded();

        pipeline
            .retire_change("chg-1", "landing refused the change")
            .await
            .unwrap();
        assert!(
            pipeline.pending_changes(None).await.unwrap().is_empty(),
            "队列那一半已经关了"
        );

        let (again, _, refused) = merge_verification_inputs(
            pipeline.pending_changes(None).await.unwrap(),
            Some(&landing),
        )
        .await;
        assert_eq!(again.len(), 1, "记录还在供货，所以这条变更根本没有被退休");
        assert!(
            refused.is_empty(),
            "它回到这一轮的方式不能是提前拒掉：那等于把这条测试要证的缺陷换成另一种形态"
        );
    }

    #[tokio::test]
    async fn ensure_env_passes_for_current_repo() {
        let temp = tempfile::tempdir().unwrap();
        let project_root = std::env::current_dir().unwrap();
        let config = cog_core::SelfEvolutionConfig {
            change_dir: temp.path().join("changes").to_string_lossy().to_string(),
            binary_dir: temp.path().join("bin").to_string_lossy().to_string(),
            backup_dir: temp.path().join("backups").to_string_lossy().to_string(),
            switch_mode: "systemd".to_string(),
            ..Default::default()
        };

        let result = ensure_self_evolution_environment(&project_root, &config).await;
        assert!(result.is_ok(), "expected ensure to pass: {:?}", result);
    }

    #[tokio::test]
    async fn ensure_env_fails_for_missing_project_root() {
        let project_root = std::path::PathBuf::from("/nonexistent/path/that/should/not/exist");
        let config = cog_core::SelfEvolutionConfig::default();

        let result = ensure_self_evolution_environment(&project_root, &config).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("project_root does not exist"),
            "error should mention project root: {}",
            err
        );
    }

    #[tokio::test]
    async fn ensure_env_creates_and_checks_writable_dirs() {
        let temp = tempfile::tempdir().unwrap();
        let project_root = temp.path().join("repo");
        tokio::fs::create_dir_all(&project_root).await.unwrap();

        // Initialize a git repo so the git check passes.
        let init = tokio::process::Command::new("git")
            .args(["init"])
            .current_dir(&project_root)
            .output()
            .await
            .unwrap();
        assert!(init.status.success());

        let config = cog_core::SelfEvolutionConfig {
            change_dir: "changes".to_string(),
            binary_dir: "bin".to_string(),
            backup_dir: "backups".to_string(),
            switch_mode: "systemd".to_string(),
            ..Default::default()
        };

        let result = ensure_self_evolution_environment(&project_root, &config).await;
        assert!(result.is_ok(), "expected ensure to pass: {:?}", result);

        assert!(project_root.join("changes").is_dir());
        assert!(project_root.join("bin").is_dir());
        assert!(project_root.join("backups").is_dir());
    }

    /// A process without the executor role never consumes a change, so it must
    /// not create a queue directory either: an empty `evolution-changes` on a
    /// filesystem nobody reads from is a decoy that reads exactly like a queue
    /// that is empty. The sibling test above is the control here — it runs the
    /// same configuration with the default role, which is the one that drains
    /// the queue, and asserts the directory is created.
    #[tokio::test]
    async fn ensure_env_leaves_no_queue_dir_on_a_process_that_does_not_drain_it() {
        let temp = tempfile::tempdir().unwrap();
        let project_root = temp.path().join("repo");
        tokio::fs::create_dir_all(&project_root).await.unwrap();
        let init = tokio::process::Command::new("git")
            .args(["init"])
            .current_dir(&project_root)
            .output()
            .await
            .unwrap();
        assert!(init.status.success());

        let config = cog_core::SelfEvolutionConfig {
            change_dir: "changes".to_string(),
            binary_dir: "bin".to_string(),
            backup_dir: "backups".to_string(),
            switch_mode: "systemd".to_string(),
            executor_enabled: false,
            ..Default::default()
        };

        let result = ensure_self_evolution_environment(&project_root, &config).await;
        assert!(result.is_ok(), "expected ensure to pass: {:?}", result);

        assert!(
            !project_root.join("changes").exists(),
            "a non-owner must not manufacture a queue it never reads"
        );
        assert!(project_root.join("bin").is_dir());
        assert!(project_root.join("backups").is_dir());
    }

    #[tokio::test]
    async fn ensure_env_sandbox_mode_auto_git_inits() {
        let temp = tempfile::tempdir().unwrap();
        let project_root = temp.path().join("repo");
        tokio::fs::create_dir_all(&project_root).await.unwrap();

        let config = cog_core::SelfEvolutionConfig {
            sandbox_mode: true,
            change_dir: temp.path().join("changes").to_string_lossy().to_string(),
            binary_dir: temp.path().join("bin").to_string_lossy().to_string(),
            backup_dir: temp.path().join("backups").to_string_lossy().to_string(),
            switch_mode: "systemd".to_string(),
            ..Default::default()
        };

        let result = ensure_self_evolution_environment(&project_root, &config).await;
        assert!(
            result.is_ok(),
            "expected sandbox ensure to pass: {:?}",
            result
        );

        // Verify git repo was initialized.
        assert!(project_root.join(".git").is_dir());
    }

    #[tokio::test]
    async fn ensure_env_sandbox_mode_non_sandbox_rejects_missing_git() {
        let temp = tempfile::tempdir().unwrap();
        let project_root = temp.path().join("repo");
        tokio::fs::create_dir_all(&project_root).await.unwrap();

        let config = cog_core::SelfEvolutionConfig {
            sandbox_mode: false,
            change_dir: temp.path().join("changes").to_string_lossy().to_string(),
            binary_dir: temp.path().join("bin").to_string_lossy().to_string(),
            backup_dir: temp.path().join("backups").to_string_lossy().to_string(),
            switch_mode: "systemd".to_string(),
            ..Default::default()
        };

        let result = ensure_self_evolution_environment(&project_root, &config).await;
        assert!(
            result.is_err(),
            "non-sandbox mode should fail without git repo"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not a git repository"),
            "error should mention git repo: {}",
            err
        );
    }

    #[tokio::test]
    async fn ensure_env_sandbox_mode_copies_binary_for_self_exec() {
        let temp = tempfile::tempdir().unwrap();
        let project_root = std::env::current_dir().unwrap();
        let config = cog_core::SelfEvolutionConfig {
            sandbox_mode: true,
            change_dir: temp.path().join("changes").to_string_lossy().to_string(),
            binary_dir: temp.path().join("bin").to_string_lossy().to_string(),
            backup_dir: temp.path().join("backups").to_string_lossy().to_string(),
            switch_mode: "self_exec".to_string(),
            ..Default::default()
        };

        let result = ensure_self_evolution_environment(&project_root, &config).await;
        assert!(
            result.is_ok(),
            "expected sandbox self_exec ensure to pass: {:?}",
            result
        );

        let expected_binary = temp.path().join("bin").join("cogneva");
        assert!(
            expected_binary.exists(),
            "binary should be copied to binary_dir/cogneva in sandbox mode"
        );
    }

    fn git_in(dir: &std::path::Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// 裸仓库带两个提交：`old` 相当于版本 tag，`new` 相当于已前进的上游主线。
    fn seed_two_commits(root: &std::path::Path) -> (std::path::PathBuf, String, String) {
        let work = root.join("seed");
        std::fs::create_dir_all(&work).unwrap();
        git_in(&work, &["init", "-q", "-b", "main"]);
        std::fs::write(work.join("a.txt"), "a\n").unwrap();
        git_in(&work, &["add", "-A"]);
        git_in(&work, &["commit", "-qm", "a"]);
        let old = git_in(&work, &["rev-parse", "HEAD"]);
        std::fs::write(work.join("b.txt"), "b\n").unwrap();
        git_in(&work, &["add", "-A"]);
        git_in(&work, &["commit", "-qm", "b"]);
        let new = git_in(&work, &["rev-parse", "HEAD"]);
        let bare = root.join("bare.git");
        git_in(
            root,
            &[
                "clone",
                "-q",
                "--bare",
                work.to_str().unwrap(),
                bare.to_str().unwrap(),
            ],
        );
        (bare, old, new)
    }

    async fn seeded_workspaces(
        root: &std::path::Path,
        old: &str,
    ) -> Arc<crate::workspace::WorkspaceManager> {
        use crate::workspace::{BaseRef, WorkspaceKind, WorkspaceSpec};
        let bare = root.join("bare.git");
        let workspaces = Arc::new(crate::workspace::WorkspaceManager::new(
            &bare,
            root.join("workspaces"),
            root.join("target"),
        ));
        // 轮首：解析出的基线是版本 tag，工作树与基线都停在旧提交。
        workspaces
            .ensure_persistent(WorkspaceSpec::persistent(
                "cycle-t",
                WorkspaceKind::Cycle,
                BaseRef::Commit(old.to_string()),
            ))
            .await
            .unwrap();
        workspaces
            .ensure_persistent(WorkspaceSpec::persistent(
                "engine-baseline",
                WorkspaceKind::EngineBaseline,
                BaseRef::Commit(old.to_string()),
            ))
            .await
            .unwrap();
        workspaces
    }

    /// 引擎拿基线树跑 `git apply --check`，变更却在轮工作树里应用与测试：两棵树
    /// 必须停在同一个提交，否则校验结论说的是另一棵树。轮内工作树被同步到上游
    /// 主线之后，基线要跟着走。
    #[tokio::test]
    async fn engine_baseline_follows_the_round_workspace_head() {
        use crate::workspace::BaseRef;
        let tmp = tempfile::tempdir().unwrap();
        let (_bare, old, new) = seed_two_commits(tmp.path());
        let workspaces = seeded_workspaces(tmp.path(), &old).await;
        let cycle = workspaces.cycle_workspace("t");

        // 同步到上游主线：工作树前进了，基线还停在轮首的版本 tag 上。
        let ws = workspaces
            .ensure_persistent(crate::workspace::WorkspaceSpec::persistent(
                "cycle-t",
                crate::workspace::WorkspaceKind::Cycle,
                BaseRef::Commit(old.clone()),
            ))
            .await
            .unwrap();
        workspaces
            .refresh(&ws, BaseRef::Commit(new.clone()))
            .await
            .unwrap();
        let baseline = workspaces.engine_baseline_workspace();
        assert_eq!(
            workspaces.head_of(&baseline).await.as_deref(),
            Some(old.as_str()),
            "前提：基线此刻还停在旧提交"
        );

        align_engine_baseline(&workspaces, &cycle).await;

        assert_eq!(
            workspaces.head_of(&baseline).await.as_deref(),
            Some(new.as_str()),
            "基线与校验对象必须停在同一个提交"
        );
    }

    /// 读不到轮工作树的 HEAD 就不移动基线：没有证据时沿用上一轮的基线，好过按
    /// 猜的提交去 reset。
    #[tokio::test]
    async fn engine_baseline_stays_put_when_round_head_is_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        let (_bare, old, _new) = seed_two_commits(tmp.path());
        let workspaces = seeded_workspaces(tmp.path(), &old).await;
        let baseline = workspaces.engine_baseline_workspace();

        align_engine_baseline(&workspaces, &tmp.path().join("not-a-repo")).await;

        assert_eq!(
            workspaces.head_of(&baseline).await.as_deref(),
            Some(old.as_str()),
            "没有 HEAD 证据时基线不能被移动"
        );
    }
}
