//! Reflection plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{error, info, warn};

/// Reflection plugin that self-assembles the reflection engine and spawns
/// evolution bridges.
pub struct ReflectionPlugin {
    initialized: bool,
}

impl ReflectionPlugin {
    /// Create a plugin that will build the reflection engine during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
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
            .consume::<tokio::sync::RwLock<cog_core::SkillRegistry>>()
            .expect("skill registry")
            .clone();
        let prompt_manager = ctx
            .consume_service::<dyn cog_core::PromptProvider>()
            .expect("prompt manager")
            .clone();

        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();
        let (tool_tx, mut tool_rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();

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
                Some(std::env::current_dir().map_err(|e| {
                    error!("Failed to get current working directory: {}", e);
                    cog_core::SFError::IO(e.to_string())
                })?),
            )
        } else {
            if strict_persistence {
                warn!("ReflectionEngine falling back to in-memory mode (memory_backend or llm_provider unavailable)");
            }
            info!("ReflectionEngine initialized in in-memory mode");
            crate::ReflectionEngine::new_in_memory(skill_registry.clone())
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
        if engine.meta_learning.is_some() {
            engine.meta_learning = Some(Arc::new(
                crate::MetaLearningEngine::new(engine.recorder.clone())
                    .with_policy_store(policy_store.clone(), "meta_learning.mode"),
            ));
        }
        let artifact_evolution = Arc::new(crate::ArtifactEvolution::new(policy_store));
        let engine = Arc::new(engine);
        ctx.publish(artifact_evolution.clone());
        info!(dir = %policy_dir, "artifact-level evolution policy store enabled");

        ctx.publish(engine.clone());
        info!("ReflectionPlugin reflection engine published");

        // Publish PatchSink when self-evolution is available.
        if let Some(ref evo) = engine.evolution {
            let sink: Arc<dyn cog_core::PatchSink> = evo.clone();
            ctx.publish_service(sink);
            info!("ReflectionPlugin PatchSink published");
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

        let meta_learning: Arc<dyn cog_core::MetaLearning> = engine
            .meta_learning
            .clone()
            .unwrap_or_else(|| Arc::new(crate::MetaLearningEngine::new(engine.recorder.clone())));
        ctx.publish_service(meta_learning);
        info!("ReflectionPlugin reflection trait objects published");

        // Spawn evolution bridges.
        let hook_engine = ctx
            .consume_service::<dyn cog_core::HookEngine>()
            .expect("hook engine");
        let tool_registry = ctx
            .consume_service::<dyn cog_core::ToolRegistry>()
            .expect("tool registry");

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
                // host 不本地执行 patch pipeline，而是每个 cycle 冷启动一个
                // MicroVM（挂载 PV → 执行进化 → 阅后即焚）。preflight 失败
                // 视为配置错误：显式报错并禁用 pipeline，绝不静默落到无沙盒
                // 的本地执行。
                if self_evolution.microvm.enabled {
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
                    tokio::spawn(async move {
                        let mut interval = tokio::time::interval(poll);
                        loop {
                            interval.tick().await;
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
                    });
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

                if let Err(e) =
                    ensure_self_evolution_environment(&project_root, &self_evolution).await
                {
                    error!(error = %e, "Self-evolution environment validation failed");
                    return Err(e);
                }

                let pipeline = crate::PatchPipeline::new(
                    &project_root,
                    &self_evolution.patch_dir,
                    // manual_approve holds test-passed patches at AwaitingReview
                    // (working tree rolled back) until an operator approves via
                    // the admin API, even when auto_apply is enabled.
                    self_evolution.auto_apply && !self_evolution.manual_approve,
                )
                .with_test_timeout(self_evolution.test_timeout_secs)
                .with_promotion_policy(promotion.clone());

                let deployer = crate::EvolutionDeployer::new(
                    &project_root,
                    &self_evolution.binary_dir,
                    &self_evolution.backup_dir,
                )
                .with_build_timeout(self_evolution.build_timeout_secs);

                let binary_switcher = ctx.consume_service::<dyn cog_core::BinarySwitcher>();
                let audit_stream = ctx.consume_service::<dyn cog_core::AuditStream>();
                if audit_stream.is_none() {
                    warn!("AuditStream not published; patch operations will not be audited");
                }
                let engine = engine.clone();

                // 接管台 SSE 推送通道：补丁行变更即时广播。
                let (stream_tx, _) =
                    tokio::sync::broadcast::channel::<cog_core::EvolutionPatchInfo>(64);
                ctx.publish(Arc::new(stream_tx.clone()));

                // Publish admin-facing evolution control surface.
                let mut admin = crate::EvolutionAdminService::new(
                    engine.clone(),
                    pipeline.clone(),
                    deployer.clone(),
                    binary_switcher.clone(),
                    evolution_metrics.clone(),
                )
                .with_evolution_stream(stream_tx);
                if let Some(stream) = audit_stream {
                    admin = admin.with_audit_stream(stream);
                }
                admin = admin.with_artifact_evolution(artifact_evolution.clone());
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

                // 晋级触发器：沙盒验证全过的 patch 由它决定去向
                // （GitOps 自动晋级 / 审批台待办），配额/熔断/暂停全在
                // 其中判定。推送端只跟 Git 中央仓库说话，不持集群凭证。
                let promotion_channel: Option<Arc<dyn crate::PromotionChannel>> =
                    if promotion.gitops.enabled {
                        info!(
                            repo = %promotion.gitops.repo_url,
                            branch = %promotion.gitops.branch,
                            "GitOps promotion publisher enabled"
                        );
                        Some(Arc::new(crate::GitOpsPublisher::new(
                            promotion.gitops.clone(),
                            &project_root,
                            &self_evolution.binary_dir,
                        )))
                    } else {
                        None
                    };
                let promoter: Option<Arc<crate::AutoPromoter>> = ctx
                    .consume_service::<dyn cog_core::PromotionLedger>()
                    .map(|ledger| {
                        Arc::new(crate::AutoPromoter::new(
                            promotion.clone(),
                            ledger,
                            promotion_channel,
                            engine.clone(),
                        ))
                    });
                if promoter.is_none() {
                    warn!("PromotionLedger not published; auto-promotion disabled");
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
                        let puller = Arc::new(
                            crate::GitOpsPuller::new(
                                promotion.gitops.clone(),
                                ledger,
                                cluster.clone(),
                            )
                            .with_metrics_url(metrics_url),
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

                let poll_interval =
                    std::time::Duration::from_secs(self_evolution.poll_interval_secs);

                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(poll_interval);
                    loop {
                        interval.tick().await;
                        if let Err(e) = run_evolution_cycle(
                            &pipeline,
                            &deployer,
                            binary_switcher.as_ref(),
                            &engine,
                            &self_evolution,
                            evolution_metrics.as_ref(),
                            promoter.as_ref(),
                        )
                        .await
                        {
                            warn!(error = %e, "Self-evolution cycle failed");
                        }
                    }
                });

                info!("Self-evolution auto-deploy pipeline started");
            }
        }

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("ReflectionPlugin shutdown");
        Ok(())
    }
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
/// - `patch_dir`, `binary_dir`, `backup_dir` exist and are writable.
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

    let patch_dir = resolve(&config.patch_dir);
    let binary_dir = resolve(&config.binary_dir);
    let backup_dir = resolve(&config.backup_dir);

    // Create and ensure writable directories.
    for dir in [&patch_dir, &binary_dir, &backup_dir] {
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
        tokio::fs::copy(&current_exe, &expected_binary).await.map_err(|e| {
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
            tokio::fs::set_permissions(&expected_binary, std::fs::Permissions::from_mode(0o755))
                .await?;
        }
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

/// Run one pass of the self-evolution auto-deploy pipeline.
async fn run_evolution_cycle(
    pipeline: &crate::PatchPipeline,
    deployer: &crate::EvolutionDeployer,
    binary_switcher: Option<&Arc<dyn cog_core::BinarySwitcher>>,
    engine: &Arc<crate::ReflectionEngine>,
    config: &cog_core::SelfEvolutionConfig,
    evolution_metrics: Option<&Arc<dyn cog_core::EvolutionMetrics>>,
    promoter: Option<&Arc<crate::AutoPromoter>>,
) -> cog_core::SFResult<()> {
    let Some(evo_engine) = engine.evolution.as_ref() else {
        return Ok(());
    };

    // 先对齐上游主线再处理 patch：沙盒树陈旧会让 GitOps 拉取端应用晋级
    // 产物时连带回退无关文件。同步失败不阻塞本轮（用当前树继续）。
    if let Err(e) = pipeline.sync_with_upstream().await {
        warn!(error = %e, "Sandbox source sync failed; continuing with current tree");
    }

    let patches = pipeline.pending_patches(Some(evo_engine)).await?;
    if patches.is_empty() {
        return Ok(());
    }

    info!(count = patches.len(), "Pending evolution patches found");

    // Process patches serially. Each patch is applied, tested, committed,
    // built, and (when configured) deployed before moving to the next one.
    for patch in patches {
        let mut patch_failed = false;

        let result = match pipeline.apply_and_test(&patch).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Patch apply/test failed");
                let _ = engine
                    .record_patch_outcome(
                        &patch.artifact_id,
                        false,
                        &format!("Pipeline error: {}", e),
                    )
                    .await;
                if let Some(m) = evolution_metrics {
                    m.record_event(true).await;
                    m.record_patch_failed().await;
                }
                continue;
            }
        };

        evo_engine
            .update_status(&result.patch_id, result.new_status)
            .await;

        if !result.test_passed {
            warn!(patch_id = %result.patch_id, "Patch failed tests; skipping deploy");
            let _ = engine
                .record_patch_outcome(&result.patch_id, false, &result.test_output)
                .await;
            patch_failed = true;
        } else if !config.auto_apply || config.manual_approve {
            info!(patch_id = %result.patch_id, "Patch awaiting manual approval");
        } else {
            let artifact = match deployer.commit_and_build(&result.patch_id).await {
                Ok(a) => a,
                Err(e) => {
                    warn!(patch_id = %result.patch_id, error = %e, "Patch commit/build failed");
                    let _ = engine
                        .record_patch_outcome(
                            &result.patch_id,
                            false,
                            &format!("Build failed: {}", e),
                        )
                        .await;
                    if let Some(m) = evolution_metrics {
                        m.record_event(true).await;
                        m.record_patch_failed().await;
                    }
                    continue;
                }
            };

            info!(
                patch_id = %artifact.patch_id,
                commit = %artifact.commit_hash,
                "Patch committed and built"
            );

            if !config.auto_deploy {
                info!(patch_id = %artifact.patch_id, "Build artifact awaiting manual deploy");
            } else {
                let Some(switcher) = binary_switcher else {
                    warn!("auto_deploy enabled but no BinarySwitcher service available");
                    let _ = engine
                        .record_patch_outcome(
                            &artifact.patch_id,
                            false,
                            "No BinarySwitcher service available",
                        )
                        .await;
                    if let Some(m) = evolution_metrics {
                        m.record_event(true).await;
                        m.record_patch_failed().await;
                    }
                    continue;
                };

                if let Err(e) = switcher.stage_new_binary(&artifact.new_binary_path).await {
                    warn!(patch_id = %artifact.patch_id, error = %e, "Staging failed");
                    let _ = engine
                        .record_patch_outcome(
                            &artifact.patch_id,
                            false,
                            &format!("Staging failed: {}", e),
                        )
                        .await;
                    if let Some(m) = evolution_metrics {
                        m.record_event(true).await;
                        m.record_patch_failed().await;
                    }
                    continue;
                }
                info!(patch_id = %artifact.patch_id, "Staging new binary for switch");

                // switch_and_restart may exec the current process and never return.
                if let Err(e) = switcher.switch_and_restart().await {
                    warn!(error = %e, "Switch failed; attempting rollback");
                    if let Err(rb_e) = switcher.rollback().await {
                        warn!(error = %rb_e, "Rollback failed");
                    }
                    let _ = engine
                        .record_patch_outcome(
                            &artifact.patch_id,
                            false,
                            &format!("Switch failed: {}", e),
                        )
                        .await;
                    if let Some(m) = evolution_metrics {
                        m.record_event(true).await;
                        m.record_patch_failed().await;
                    }
                    return Err(e);
                }

                let _ = engine
                    .record_patch_outcome(
                        &artifact.patch_id,
                        true,
                        "Patch applied, tested, built, and deployed",
                    )
                    .await;
                if let Some(m) = evolution_metrics {
                    m.record_patch_applied().await;
                    m.record_event(false).await;
                }
                // 沙盒部署成功 → 交晋级触发器（soak → 分级 → GitOps/审批台）。
                if let Some(p) = promoter {
                    let p = p.clone();
                    let promoted_patch = patch.clone();
                    tokio::spawn(async move {
                        p.on_sandbox_deployed(promoted_patch).await;
                    });
                }
                continue;
            }
        }

        if patch_failed {
            if let Some(m) = evolution_metrics {
                m.record_event(true).await;
                m.record_patch_failed().await;
            }
        } else if !config.auto_deploy || config.manual_approve {
            // Patch succeeded tests but is waiting for approval; count as
            // a successful processing step without applying/deploying.
            let _ = engine
                .record_patch_outcome(
                    &result.patch_id,
                    true,
                    "Patch passed tests; awaiting manual approval",
                )
                .await;
            if let Some(m) = evolution_metrics {
                m.record_event(false).await;
            }
        }
    }

    Ok(())
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "reflection",
    requires: &["skill", "prompt", "agent"],
    optional_requires: &["llm", "memory"],
    provides: &[
        "ReflectionEngine",
        "SquadReflection",
        "MetaLearning",
        "EvolutionAdmin",
    ],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "LlmClient",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MemoryBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "SkillRegistry",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "PromptProvider",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "HookEngine",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "ToolRegistry",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "BinarySwitcher",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "EvolutionMetrics",
            required: false,
        },
    ],
    factory: || Box::new(ReflectionPlugin::new()),
};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ensure_env_passes_for_current_repo() {
        let temp = tempfile::tempdir().unwrap();
        let project_root = std::env::current_dir().unwrap();
        let config = cog_core::SelfEvolutionConfig {
            patch_dir: temp.path().join("patches").to_string_lossy().to_string(),
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
            patch_dir: "patches".to_string(),
            binary_dir: "bin".to_string(),
            backup_dir: "backups".to_string(),
            switch_mode: "systemd".to_string(),
            ..Default::default()
        };

        let result = ensure_self_evolution_environment(&project_root, &config).await;
        assert!(result.is_ok(), "expected ensure to pass: {:?}", result);

        assert!(project_root.join("patches").is_dir());
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
            patch_dir: temp.path().join("patches").to_string_lossy().to_string(),
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
            patch_dir: temp.path().join("patches").to_string_lossy().to_string(),
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
            patch_dir: temp.path().join("patches").to_string_lossy().to_string(),
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
}
