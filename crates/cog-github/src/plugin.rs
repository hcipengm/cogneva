//! GitHub plugin for the Cogneva plugin registry.
//!
//! When `github_integration.enabled` is set in the central config the plugin
//! builds a [`CodePlatformProvider`], publishes it as a service, and starts
//! the autonomous [`GitHubDiscoveryLoop`](crate::GitHubDiscoveryLoop)
//! (scan → triage → clarify → submit → record outcomes). Tokens are
//! resolved in this process only and never enter the sandbox.

use std::sync::{Arc, Mutex};

use tracing::{info, warn};

use crate::provider::CodePlatformProvider;

/// Background loop state, held behind a mutex because [`SystemPlugin::start`]
/// takes `&self`.
struct LoopState {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

/// GitHub integration plugin.
pub struct GitHubPlugin {
    config: Option<crate::config::GitHubIntegrationConfig>,
    provider: Option<Arc<dyn CodePlatformProvider>>,
    /// Gitee 侧：循环配置（策略继承 github_integration）+ 平台 provider。
    gitee: Option<(
        crate::config::GitHubIntegrationConfig,
        Arc<dyn CodePlatformProvider>,
    )>,
    loop_state: Mutex<Option<LoopState>>,
}

type SharedLoop = Arc<tokio::sync::Mutex<crate::discovery_loop::GitHubDiscoveryLoop>>;

/// 平台轮询任务：间隔触发 run_once，shutdown 信号退出。
fn spawn_polling_loop(
    platform: &'static str,
    shared: SharedLoop,
    interval_secs: u64,
    mut rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let interval = std::time::Duration::from_secs(interval_secs.max(30));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = rx.changed() => {
                    info!(platform, "discovery polling loop shutting down");
                    return;
                }
                _ = tokio::time::sleep(interval) => {}
            }
            if let Err(e) = shared.lock().await.run_once().await {
                warn!(platform, error = %e, "discovery round failed");
            }
        }
    })
}

/// 后台周期补发暂存变更。初始化时的 drain 只覆盖"启动时通道已就绪"；向导在
/// 进程运行期间补配 token 时只有网关滚动重启、本进程不重启，暂存变更会靠这个
/// 周期任务在下一轮自动提交。补发幂等：成功的变更由 drain 删除暂存文件，
/// 已推送的分支有 `already_published` 守卫，失败留到下一轮。
fn spawn_staged_drain(
    config: crate::config::GitHubIntegrationConfig,
    provider: Arc<dyn CodePlatformProvider>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        interval.tick().await; // 消费立即触发的首拍，让启动 drain 先跑
        loop {
            interval.tick().await;
            if crate::pending_changes::load_pending().await.is_empty() {
                continue;
            }
            let token = config
                .primary_account()
                .ok()
                .and_then(|a| a.resolve_token().ok());
            match crate::pr_publisher::ensure_workdir(&config, token.as_deref()).await {
                Ok(workdir) => {
                    let sink = crate::pr_publisher::GitHubChangeSink::new(
                        crate::pr_publisher::GitHubPrPublisher::new(workdir, config.clone()),
                        provider.clone(),
                    );
                    let n = crate::pending_changes::drain_into(&sink).await;
                    if n > 0 {
                        info!(count = n, "staged changes flushed by background drain");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "background staged-change drain skipped (channel not ready)");
                }
            }
        }
    });
}

impl GitHubPlugin {
    /// Create a new GitHub plugin instance.
    pub fn new() -> Self {
        Self {
            config: None,
            provider: None,
            gitee: None,
            loop_state: Mutex::new(None),
        }
    }
}

impl Default for GitHubPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for GitHubPlugin {
    fn name(&self) -> &'static str {
        "github"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // github/gitee_integration 是 cog-github 自有配置段，自读 cogneva.json。
        let mut config = crate::config::GitHubIntegrationConfig::load()?;
        let gitee_config = crate::config::GiteeIntegrationConfig::load()?;

        // 实例自治身份（Alice#a3f9d2c1 式）：首次进化时按机器指纹自动生成，
        // 后续提交作者 / evol 分支 / PR 元数据统一引用；纯确定性，可重入。
        let identity = crate::identity::resolve(&mut config.bot_identity).await;
        info!(
            handle = %identity.handle,
            branch = %identity.branch_id,
            "instance identity resolved"
        );

        if !config.enabled && !gitee_config.enabled {
            info!("GitHubPlugin disabled (github/gitee integration both disabled)");
            return Ok(());
        }

        if config.enabled {
            let mut pr_sink_published = false;
            match crate::default_provider(&config) {
                Ok(provider) => {
                    let provider: Arc<dyn CodePlatformProvider> = Arc::from(provider);
                    ctx.publish_service::<dyn CodePlatformProvider>(provider.clone());
                    info!(repo = %config.repo, "GitHubPlugin initialized");

                    // Change-to-PR publishing (ChangeSink) for autonomous fixes.
                    // Disabled when pr_workdir is not configured.
                    if !config.pr_workdir.is_empty() {
                        let token = config
                            .primary_account()
                            .ok()
                            .and_then(|a| a.resolve_token().ok());
                        match crate::pr_publisher::ensure_workdir(&config, token.as_deref()).await {
                            Ok(workdir) => {
                                let sink = Arc::new(crate::pr_publisher::GitHubChangeSink::new(
                                    crate::pr_publisher::GitHubPrPublisher::new(
                                        workdir.clone(),
                                        config.clone(),
                                    ),
                                    provider.clone(),
                                ));
                                // The channel is live: flush changes staged
                                // before it was connected (best effort;
                                // failures stay staged for the next start).
                                let flushed =
                                    crate::pending_changes::drain_into(sink.as_ref()).await;
                                if flushed > 0 {
                                    info!(count = flushed, "flushed staged changes to PRs");
                                }
                                ctx.publish_service::<dyn cog_core::ChangeSink>(sink);
                                pr_sink_published = true;
                                info!(workdir = %workdir.display(), "GitHub ChangeSink published");
                            }
                            Err(e) => {
                                warn!(error = %e, "GitHub PR workdir unavailable; ChangeSink not published");
                            }
                        }
                    }

                    // 向导在进程运行期间补配 token（写入网关 Secret 后滚动重启
                    // 网关，本进程并不重启）时，上面的启动 drain 不会重跑；
                    // 后台周期补发让暂存变更在通道接通后的下一个周期自动提交。
                    if !config.pr_workdir.is_empty() {
                        spawn_staged_drain(config.clone(), provider.clone());
                    }

                    self.provider = Some(provider);
                    self.config = Some(config.clone());
                }
                Err(e) => {
                    // A missing token must not take the whole system down; the
                    // integration simply stays inactive.
                    warn!(error = %e, "GitHubPlugin provider unavailable; integration inactive");
                }
            }
            // No PR path yet (channel unconfigured, no workdir, or no
            // provider): stage generated changes locally so they survive until
            // the contribution channel is connected and drained.
            if !pr_sink_published {
                ctx.publish_service::<dyn cog_core::ChangeSink>(Arc::new(
                    crate::pending_changes::PendingChangeSink,
                ));
                info!("contribution channel not ready; generated changes stage to pending dir");
            }
        }

        // Gitee 与 GitHub 地位平等：issue 即外部意图进化入口。策略（分诊
        // 标签/澄清对话/自动合并）继承 github_integration，平台字段由
        // gitee_integration 覆盖。Gitee 暂无开放 CI API 与 ChangeSink，
        // 发现循环承担 scan→triage→clarify→submit 全链。
        if gitee_config.enabled {
            match crate::gitee_provider(&gitee_config) {
                Ok(provider) => {
                    let mut loop_cfg = config.clone();
                    loop_cfg.enabled = true;
                    loop_cfg.repo = gitee_config.repo.clone();
                    loop_cfg.base_branch = gitee_config.base_branch.clone();
                    loop_cfg.poll_interval_secs = gitee_config.poll_interval_secs;
                    loop_cfg.max_issues_per_scan = gitee_config.max_issues_per_scan;
                    self.gitee = Some((loop_cfg, Arc::from(provider)));
                    info!(repo = %gitee_config.repo, "Gitee integration initialized");
                    // Gitee 无开放 CI API：CI 信号按 trait 默认降级为空，
                    // merge 决策的 require_ci_pass 在 Gitee PR 上恒不自动
                    // 合并（保守方向），issue/评论驱动不受影响。
                    info!("Gitee 侧无开放 CI API，CI 失败信号按 trait 默认降级（返回空），issue/评论驱动不受影响");
                }
                Err(e) => {
                    warn!(error = %e, "Gitee provider unavailable; integration inactive");
                }
            }
        }
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let orchestrator = ctx.consume_service::<dyn cog_core::OrchestratorControl>();
        let reflection = ctx.consume_service::<dyn cog_core::ReflectionEngine>();
        // 本 crate 是传感器/执行器，绝不直连 LLM。语义可行动性判定以
        // platform_intent_assess 任务经 orchestrator 派给 cog-collaboration 的
        // 单 agent 多模态分支；无 orchestrator 时 triage 退回本地规则启发式。
        if orchestrator.is_none() {
            info!("GitHubPlugin: no orchestrator; intent assessment falls back to local rules heuristic");
        }

        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // GitHub / Gitee 两个平台的 discovery loop 集中创建，轮询与事件
        // 入口共享同一实例（事件驱动与周期兜底互补）。
        let mk_loop = |config: &crate::config::GitHubIntegrationConfig,
                       provider: &Arc<dyn CodePlatformProvider>|
         -> SharedLoop {
            let triage = crate::triage::IssueTriage::rules_only();
            Arc::new(tokio::sync::Mutex::new(
                crate::discovery_loop::GitHubDiscoveryLoop::new(
                    provider.clone(),
                    triage,
                    config.clone(),
                    orchestrator.clone(),
                    reflection.clone(),
                ),
            ))
        };
        let github_shared = match (&self.config, &self.provider) {
            (Some(c), Some(p)) => Some(mk_loop(c, p)),
            _ => None,
        };
        let gitee_shared = self.gitee.as_ref().map(|(cfg, p)| mk_loop(cfg, p));
        if github_shared.is_none() && gitee_shared.is_none() {
            return Ok(());
        }

        // discovery_mode 由 github_integration 承载，Gitee 继承同一策略。
        let loop_cfg = self
            .config
            .clone()
            .or_else(|| self.gitee.as_ref().map(|(c, _)| c.clone()))
            .unwrap_or_default();
        let mode = loop_cfg.discovery_mode.as_str();
        let use_polling = mode == "polling" || mode == "both";
        let use_events = mode == "events" || mode == "both";
        if !use_polling && !use_events {
            warn!(
                mode,
                "discovery_mode 无法识别（polling/events/both），集成不启动"
            );
            return Ok(());
        }

        if use_polling {
            if let (Some(shared), Some(cfg)) = (github_shared.clone(), self.config.clone()) {
                handles.push(spawn_polling_loop(
                    "github",
                    shared,
                    cfg.poll_interval_secs,
                    rx.clone(),
                ));
                info!("GitHub discovery polling loop started");
            }
            if let Some(shared) = gitee_shared.clone() {
                let interval = self
                    .gitee
                    .as_ref()
                    .map(|(c, _)| c.poll_interval_secs)
                    .unwrap_or(300);
                handles.push(spawn_polling_loop("gitee", shared, interval, rx.clone()));
                info!("Gitee discovery polling loop started");
            }
        }

        if use_events {
            let webhook_cfg = loop_cfg.webhook.clone();
            if webhook_cfg.gateway_verified {
                // 网关验签模式：平台签名在安全网关完成，本进程只验内部
                // HMAC，GitHub 与 Gitee 事件共用同一入口。
                match crate::webhook::resolve_secret("COGNEVA_WEBHOOK_INTERNAL_SECRET") {
                    Some(secret) => {
                        let state = crate::webhook::VerifiedWebhookState {
                            github_loop: github_shared.clone(),
                            gitee_loop: gitee_shared.clone(),
                            internal_secret: secret.into(),
                        };
                        let port = webhook_cfg.port;
                        let github_path = webhook_cfg.path.clone();
                        let gitee_path = crate::webhook::GITEE_WEBHOOK_PATH.to_string();
                        let rx = rx.clone();
                        handles.push(tokio::spawn(async move {
                            if let Err(e) = crate::webhook::run_verified_webhook_server(
                                state,
                                port,
                                github_path,
                                gitee_path,
                                rx,
                            )
                            .await
                            {
                                warn!(error = %e, "verified webhook server exited");
                            }
                        }));
                        info!(
                            port,
                            "verified webhook event entry started (GitHub + Gitee)"
                        );
                    }
                    None => {
                        // 无内部 secret 启动事件入口等于接受伪造事件 —— 拒绝启动。
                        warn!(
                            "COGNEVA_WEBHOOK_INTERNAL_SECRET 未配置，网关验签事件入口不启动（fail-closed）"
                        );
                    }
                }
            } else if let Some(shared) = github_shared.clone() {
                // legacy 直连验签（仅 GitHub），供未迁网关的部署使用。
                match crate::webhook::resolve_secret(&webhook_cfg.secret_env) {
                    Some(secret) => {
                        let state = crate::webhook::WebhookState {
                            discovery_loop: shared,
                            secret: secret.into(),
                        };
                        let port = webhook_cfg.port;
                        let path = webhook_cfg.path.clone();
                        let rx = rx.clone();
                        handles.push(tokio::spawn(async move {
                            if let Err(e) =
                                crate::webhook::run_webhook_server(state, port, path, rx).await
                            {
                                warn!(error = %e, "GitHub webhook server exited");
                            }
                        }));
                        info!(port, "GitHub webhook event entry started (legacy direct)");
                    }
                    None => {
                        // 无 secret 启动 webhook 等于接受伪造事件 —— 拒绝启动。
                        warn!(
                            secret_env = %webhook_cfg.secret_env,
                            "GitHub webhook secret 未配置，事件入口不启动（fail-closed）"
                        );
                    }
                }
            }
        }

        if handles.is_empty() {
            return Ok(());
        }
        if let Ok(mut guard) = self.loop_state.lock() {
            *guard = Some(LoopState {
                shutdown_tx: tx,
                handles,
            });
        }
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        if let Ok(mut guard) = self.loop_state.lock() {
            if let Some(state) = guard.take() {
                let _ = state.shutdown_tx.send(true);
                for handle in state.handles {
                    handle.abort();
                }
            }
        }
        info!("GitHubPlugin shutdown");
        Ok(())
    }
}

/// Static plugin descriptor used by the generated plugin registry.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "github",
    requires: &[],
    optional_requires: &[],
    provides: &["CodePlatformProvider"],
    consumes: &[],
    factory: || Box::new(GitHubPlugin::new()),
};
