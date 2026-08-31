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

    /// GitHub 侧启动：轮询 + webhook（discovery_mode 决定），与 Gitee 侧
    /// 共享同一 shutdown channel。
    #[allow(clippy::too_many_arguments)]
    fn start_github_side(
        &self,
        config: crate::config::GitHubIntegrationConfig,
        provider: Arc<dyn CodePlatformProvider>,
        llm: &Option<Arc<dyn cog_core::LlmClient>>,
        orchestrator: &Option<Arc<dyn cog_core::OrchestratorControl>>,
        reflection: &Option<Arc<dyn cog_core::ReflectionEngine>>,
        rx: &tokio::sync::watch::Receiver<bool>,
        handles: &mut Vec<tokio::task::JoinHandle<()>>,
    ) {
        let mode = config.discovery_mode.as_str();
        let use_polling = mode == "polling" || mode == "both";
        let use_events = mode == "events" || mode == "both";
        if !use_polling && !use_events {
            warn!(
                mode,
                "GitHub discovery_mode 无法识别（polling/events/both），集成不启动"
            );
            return;
        }

        let triage = match llm {
            Some(l) => crate::triage::IssueTriage::with_llm(l.clone()),
            None => crate::triage::IssueTriage::rules_only(),
        };

        let discovery_loop = crate::discovery_loop::GitHubDiscoveryLoop::new(
            provider,
            triage,
            config.clone(),
            orchestrator.clone(),
            reflection.clone(),
        );
        // 轮询与 webhook 共享同一实例（事件驱动与周期兜底互补）。
        let shared = std::sync::Arc::new(tokio::sync::Mutex::new(discovery_loop));

        if use_polling {
            let shared = shared.clone();
            let mut rx = rx.clone();
            let interval = std::time::Duration::from_secs(config.poll_interval_secs.max(30));
            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx.changed() => {
                            info!("GitHub discovery polling loop shutting down");
                            return;
                        }
                        _ = tokio::time::sleep(interval) => {}
                    }
                    if let Err(e) = shared.lock().await.run_once().await {
                        warn!(error = %e, "GitHub discovery round failed");
                    }
                }
            }));
            info!("GitHub discovery polling loop started");
        }

        if use_events {
            match crate::webhook::resolve_secret(&config.webhook.secret_env) {
                Some(secret) => {
                    let state = crate::webhook::WebhookState {
                        discovery_loop: shared,
                        secret: secret.into(),
                    };
                    let port = config.webhook.port;
                    let path = config.webhook.path.clone();
                    let rx = rx.clone();
                    handles.push(tokio::spawn(async move {
                        if let Err(e) =
                            crate::webhook::run_webhook_server(state, port, path, rx).await
                        {
                            warn!(error = %e, "GitHub webhook server exited");
                        }
                    }));
                    info!(
                        port = config.webhook.port,
                        "GitHub webhook event entry started"
                    );
                }
                None => {
                    // 无 secret 启动 webhook 等于接受伪造事件 —— 拒绝启动。
                    warn!(
                        secret_env = %config.webhook.secret_env,
                        "GitHub webhook secret 未配置，事件入口不启动（fail-closed）"
                    );
                }
            }
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
        let config = crate::config::GitHubIntegrationConfig::load()?;
        let gitee_config = crate::config::GiteeIntegrationConfig::load()?;
        if !config.enabled && !gitee_config.enabled {
            info!("GitHubPlugin disabled (github/gitee integration both disabled)");
            return Ok(());
        }

        if config.enabled {
            match crate::default_provider(&config) {
                Ok(provider) => {
                    let provider: Arc<dyn CodePlatformProvider> = Arc::from(provider);
                    ctx.publish_service::<dyn CodePlatformProvider>(provider.clone());
                    info!(repo = %config.repo, "GitHubPlugin initialized");

                    // Patch-to-PR publishing (PatchSink) for autonomous fixes.
                    // Disabled when pr_workdir is not configured.
                    if !config.pr_workdir.is_empty() {
                        let token = config
                            .primary_account()
                            .ok()
                            .and_then(|a| a.resolve_token().ok());
                        match crate::pr_publisher::ensure_workdir(&config, token.as_deref()).await {
                            Ok(workdir) => {
                                let sink = Arc::new(crate::pr_publisher::GitHubPatchSink::new(
                                    crate::pr_publisher::GitHubPrPublisher::new(
                                        workdir.clone(),
                                        config.clone(),
                                    ),
                                    provider.clone(),
                                ));
                                ctx.publish_service::<dyn cog_core::PatchSink>(sink);
                                info!(workdir = %workdir.display(), "GitHub PatchSink published");
                            }
                            Err(e) => {
                                warn!(error = %e, "GitHub PR workdir unavailable; PatchSink not published");
                            }
                        }
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
        }

        // Gitee 与 GitHub 地位平等：issue 即外部意图进化入口。策略（分诊
        // 标签/澄清对话/自动合并）继承 github_integration，平台字段由
        // gitee_integration 覆盖。Gitee 暂无开放 CI API 与 PatchSink，
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
                }
                Err(e) => {
                    warn!(error = %e, "Gitee provider unavailable; integration inactive");
                }
            }
        }
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let llm = ctx.consume_service::<dyn cog_core::LlmClient>();
        let orchestrator = ctx.consume_service::<dyn cog_core::OrchestratorControl>();
        let reflection = ctx.consume_service::<dyn cog_core::ReflectionEngine>();
        if llm.is_none() {
            info!("GitHubPlugin: no LLM client; triage runs rules-only");
        }

        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        if let (Some(config), Some(provider)) = (self.config.clone(), self.provider.clone()) {
            self.start_github_side(
                config,
                provider,
                &llm,
                &orchestrator,
                &reflection,
                &rx,
                &mut handles,
            );
        }

        // Gitee 轮询入口：webhook 验签随下一轮整体迁网关，本轮仅轮询。
        if let Some((loop_cfg, provider)) = self.gitee.clone() {
            let triage = match &llm {
                Some(l) => crate::triage::IssueTriage::with_llm(l.clone()),
                None => crate::triage::IssueTriage::rules_only(),
            };
            let gitee_loop = crate::discovery_loop::GitHubDiscoveryLoop::new(
                provider,
                triage,
                loop_cfg.clone(),
                orchestrator.clone(),
                reflection.clone(),
            );
            let shared = std::sync::Arc::new(tokio::sync::Mutex::new(gitee_loop));
            let mut rx = rx.clone();
            let interval = std::time::Duration::from_secs(loop_cfg.poll_interval_secs.max(30));
            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx.changed() => {
                            info!("Gitee discovery polling loop shutting down");
                            return;
                        }
                        _ = tokio::time::sleep(interval) => {}
                    }
                    if let Err(e) = shared.lock().await.run_once().await {
                        warn!(error = %e, "Gitee discovery round failed");
                    }
                }
            }));
            info!(repo = %loop_cfg.repo, "Gitee discovery polling loop started");
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
