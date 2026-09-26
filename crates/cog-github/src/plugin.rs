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
    /// 落地通道：init 建好、start 用它拉起 CI 监视循环。
    channel: Option<Arc<crate::landing::MainChannel>>,
    /// Gitee 侧：循环配置（策略继承 github_integration）+ 平台 provider。
    gitee: Option<(
        crate::config::GitHubIntegrationConfig,
        Arc<dyn CodePlatformProvider>,
    )>,
    /// 本插件起的后台循环的停止信号（见 `new` 处的说明）。
    shutdown: cog_core::shutdown::ShutdownSignal,
    loop_state: Mutex<Option<LoopState>>,
}

type SharedLoop = Arc<tokio::sync::Mutex<crate::discovery_loop::GitHubDiscoveryLoop>>;

/// 循环名：由本插件启动的四个后台循环各一个。两个平台轮询是同一段代码的两个
/// 实例，名字必须分开——共用一个名字会让死掉的那个躲在活着的那个的心跳后面。
/// 取值集由此处的常量决定，不由流量决定。
pub const GITHUB_DISCOVERY_POLL_LOOP: &str = "github_discovery_poll";
/// 循环名，见 [`GITHUB_DISCOVERY_POLL_LOOP`] 的说明。
pub const GITEE_DISCOVERY_POLL_LOOP: &str = "gitee_discovery_poll";
/// 循环名，见 [`GITHUB_DISCOVERY_POLL_LOOP`] 的说明。
pub const STAGED_CHANGE_DRAIN_LOOP: &str = "github_staged_change_drain";
/// 循环名，见 [`GITHUB_DISCOVERY_POLL_LOOP`] 的说明。
pub const LANDING_CI_WATCH_LOOP: &str = "github_landing_ci_watch";

/// 平台轮询任务：间隔触发 run_once，shutdown 信号退出。
fn spawn_polling_loop(
    platform: &'static str,
    shared: SharedLoop,
    interval_secs: u64,
    shutdown: cog_core::shutdown::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let interval = std::time::Duration::from_secs(interval_secs.max(30));
    let name = if platform == "gitee" {
        GITEE_DISCOVERY_POLL_LOOP
    } else {
        GITHUB_DISCOVERY_POLL_LOOP
    };
    let stop = shutdown.clone();
    cog_core::loop_health::spawn(
        name,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown,
        // One set of handles per attempt: the body is rebuilt every time it is
        // restarted, so what it consumes has to be cloned inside the closure —
        // otherwise the second call has nothing to build itself from.
        move |beat| {
            let stop = stop.clone();
            let shared = Arc::clone(&shared);
            async move {
                loop {
                    // 每轮盖一次，抓到没有都盖：这一轮的轮询没发现新东西是常态，
                    // 不能读成循环停了。
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = stop.wait() => {
                            info!(platform, "discovery polling loop shutting down");
                            return;
                        }
                        _ = tokio::time::sleep(interval) => {}
                    }
                    if let Err(e) = shared.lock().await.run_once().await {
                        warn!(platform, error = %e, "discovery round failed");
                    }
                }
            }
        },
    )
}

/// 后台周期补发暂存变更。初始化时的 drain 只覆盖"启动时通道已就绪"；向导在
/// 进程运行期间补配 token 时只有网关滚动重启、本进程不重启，暂存变更会靠这个
/// 周期任务在下一轮自动提交。补发幂等：成功的变更由 drain 删除暂存文件，
/// 失败留到下一轮。
fn spawn_staged_drain(
    channel: Arc<crate::landing::MainChannel>,
    controller: Arc<crate::contribution::ContributionController>,
    shutdown: cog_core::shutdown::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let stop = shutdown.clone();
    cog_core::loop_health::spawn(
        STAGED_CHANGE_DRAIN_LOOP,
        cog_core::loop_health::Cadence::Periodic(std::time::Duration::from_secs(300)),
        shutdown,
        // The body is rebuilt per attempt, so handles are cloned in the closure;
        // see spawn_polling_loop.
        move |beat| {
            let stop = stop.clone();
            let channel = Arc::clone(&channel);
            let controller = Arc::clone(&controller);
            async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
                interval.tick().await; // 消费立即触发的首拍，让启动 drain 先跑
                loop {
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = stop.wait() => return,
                        _ = interval.tick() => {}
                    }
                    // 策略门禁：ask 档等属主逐条确认（走 ContributionControl::flush_pending），
                    // local 档永不自动回流；两者都跳过自动补发。
                    if controller.should_stage() {
                        continue;
                    }
                    if crate::pending_changes::load_pending().await.is_empty() {
                        continue;
                    }
                    let n = crate::pending_changes::drain_into(channel.as_ref()).await;
                    if n > 0 {
                        info!(count = n, "staged changes flushed by background drain");
                    }
                }
            }
        },
    )
}

/// 落地提交的 CI 监视循环：绿了收尾，红了撤销 + 重驱一次 + 记 reflection。
///
/// 循环读的是落盘记录而不是进程内状态：自进化主线在装上新二进制后会替换自身
/// 进程，落盘之前的推送与之后的监视必然分属两个进程，只有落盘的状态能跨过去。
fn spawn_landing_watch(
    channel: Arc<crate::landing::MainChannel>,
    reflection: Option<Arc<dyn cog_core::ReflectionEngine>>,
    orchestrator: Option<Arc<dyn cog_core::OrchestratorControl>>,
    interval_secs: u64,
    shutdown: cog_core::shutdown::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let interval = std::time::Duration::from_secs(interval_secs.max(30));
    let stop = shutdown.clone();
    cog_core::loop_health::spawn(
        LANDING_CI_WATCH_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown,
        // The body is rebuilt per attempt, so handles are cloned in the closure;
        // see spawn_polling_loop.
        move |beat| {
            let stop = stop.clone();
            let channel = Arc::clone(&channel);
            let reflection = reflection.clone();
            let orchestrator = orchestrator.clone();
            async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = stop.wait() => {
                            info!("landing CI watch shutting down");
                            return;
                        }
                        _ = ticker.tick() => {}
                    }
                    crate::landing::watch_landed(
                        channel.as_ref(),
                        reflection.as_deref(),
                        orchestrator.as_deref(),
                    )
                    .await;
                    // The census rides this tick because the funnel only moves when a
                    // landing or a verdict does, and this is the one loop that runs
                    // whether or not change generation is producing anything.
                    channel.publish_funnel().await;
                }
            }
        },
    )
}

impl GitHubPlugin {
    /// Create a new GitHub plugin instance.
    pub fn new() -> Self {
        Self {
            config: None,
            provider: None,
            channel: None,
            gitee: None,
            // Lives on the instance rather than in the loop state because one of
            // the loops is spawned during `init`, before there is any state to
            // store. The loops select on it and their death readings compare
            // against it: a loop that ended because this plugin is going down is
            // not a defect, and without the signal every clean shutdown would
            // read as one death per loop.
            shutdown: cog_core::shutdown::ShutdownSignal::new(),
            loop_state: Mutex::new(None),
        }
    }

    /// 登记已起的后台任务，shutdown 时统一停。句柄为空就不登记：留下一个没有
    /// 任何任务的 LoopState，只会让 shutdown 多做一次空转。
    fn store_loops(
        &self,
        shutdown_tx: tokio::sync::watch::Sender<bool>,
        handles: Vec<tokio::task::JoinHandle<()>>,
    ) {
        if handles.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.loop_state.lock() {
            *guard = Some(LoopState {
                shutdown_tx,
                handles,
            });
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
        // 贡献策略控制器：网关 admin API 经它读写属主档位、列暂存、按确认补发；
        // 无论通道是否已连接都发布，UI 才能在任何状态下读档/列暂存。
        let controller = crate::contribution::ContributionController::new_shared();
        ctx.publish_service::<dyn cog_core::ContributionControl>(controller.clone());

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
            let mut channel_published = false;
            match crate::default_provider(&config) {
                Ok(provider) => {
                    let provider: Arc<dyn CodePlatformProvider> = Arc::from(provider);
                    ctx.publish_service::<dyn CodePlatformProvider>(provider.clone());
                    info!(repo = %config.repo, "GitHubPlugin initialized");

                    // 变更落地通道：把沙盒验完的变更直接提交到主分支，并在之后
                    // 盯这次提交的 CI。工作目录未配置时从数据目录派生，通道默认
                    // 就是通的；真正的开关是贡献策略（auto/ask/local），不是路径
                    // 是否填了值。
                    let token = config
                        .primary_account()
                        .ok()
                        .and_then(|a| a.resolve_token().ok());
                    match crate::landing::ensure_workdir(&config, token.as_deref()).await {
                        Ok(workdir) => {
                            let channel = Arc::new(crate::landing::MainChannel::new(
                                workdir.clone(),
                                config.clone(),
                                provider.clone(),
                                controller.clone(),
                            ));
                            // 属主在 UI 点"提交"时经 ContributionControl::flush_pending
                            // 调用同一通道，跳过策略门禁（点击即批准）。
                            controller.set_sink(channel.clone());
                            // The channel is live: take over changes staged
                            // before it was connected (best effort; failures
                            // stay staged for the next attempt). ask/local 档
                            // 不自动回流：ask 等属主逐条确认，local 永不提交上游。
                            if !controller.should_stage() {
                                let flushed =
                                    crate::pending_changes::drain_into(channel.as_ref()).await;
                                if flushed > 0 {
                                    info!(count = flushed, "took over staged changes");
                                }
                            }
                            ctx.publish_service::<dyn cog_core::ChangeSink>(channel.clone());
                            ctx.publish_service::<dyn cog_core::ChangeLanding>(channel.clone());
                            self.channel = Some(channel.clone());
                            channel_published = true;
                            info!(workdir = %workdir.display(), "GitHub landing channel published");
                        }
                        Err(e) => {
                            warn!(error = %e, "GitHub landing workdir unavailable; channel not published");
                        }
                    }

                    // 向导在进程运行期间补配 token（写入网关 Secret 后滚动重启
                    // 网关，本进程并不重启）时，上面的启动 drain 不会重跑；
                    // 后台周期补发让暂存变更在通道接通后的下一个周期被接管。
                    if let Some(channel) = self.channel.clone() {
                        spawn_staged_drain(channel, controller.clone(), self.shutdown.clone());
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
            // No landing path yet (channel unconfigured, no workdir, or no
            // provider): stage generated changes locally so they survive until
            // the contribution channel is connected and drained.
            if !channel_published {
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
        // 池全灭时 supervisor 只暂停 LLM 依赖型任务；发现循环每轮入口据此跳过，
        // 不再发注定失败的 LLM 请求。gate 由 supervisor 插件发布，可选。
        let gate = ctx.consume_service::<dyn cog_core::SchedulerGate>();
        if gate.is_none() {
            info!("GitHubPlugin: no scheduler gate; discovery rounds run unconditionally");
        }
        // 本 crate 是传感器/执行器，绝不直连 LLM。语义可行动性判定以
        // platform_intent_assess 任务经 orchestrator 派给 cog-collaboration 的
        // 单 agent 多模态分支；无 orchestrator 时 triage 退回本地规则启发式。
        if orchestrator.is_none() {
            info!("GitHubPlugin: no orchestrator; intent assessment falls back to local rules heuristic");
        }
        // 落地失败的计数去处。在这里取而不是 init 时：storage 插件的层在 github
        // 之后（github 不 require 任何插件），init 阶段服务表里还没有它。取不到就
        // 只有日志，不改变落地行为。
        if let (Some(channel), Some(metrics)) = (
            self.channel.as_ref(),
            ctx.consume_service::<dyn cog_core::MetricsBackend>(),
        ) {
            channel.attach_metrics(metrics);
        }

        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // 落地提交的 CI 监视与发现循环彼此独立：发现循环可能因 discovery_mode
        // 不启动（纯事件入口且未配 secret 等），但已经落到主分支上的提交依然
        // 必须有人盯 CI，否则红了没人撤销。
        if let Some(channel) = self.channel.clone() {
            handles.push(spawn_landing_watch(
                channel.clone(),
                reflection.clone(),
                orchestrator.clone(),
                channel.ci_poll_interval_secs(),
                self.shutdown.clone(),
            ));
            info!("landing CI watch started");
        }

        // discovery_mode 由 github_integration 承载，Gitee 继承同一策略。
        let loop_cfg = self
            .config
            .clone()
            .or_else(|| self.gitee.as_ref().map(|(c, _)| c.clone()))
            .unwrap_or_default();

        // 非属主进程不建发现循环。落地监视（上面已起）不在此列：它盯的是本进程
        // 自己推上去的提交，两个进程各有各的工作树与落盘记录。句柄照常登记，
        // 否则这条早退会把已经起好的监视一并丢掉。
        if !loop_cfg.discovery_enabled {
            info!("discovery loops not started: this process does not own discovery");
            self.store_loops(tx, handles);
            return Ok(());
        }

        // GitHub / Gitee 两个平台的 discovery loop 集中创建，轮询与事件
        // 入口共享同一实例（事件驱动与周期兜底互补）。
        let mk_loop = |config: &crate::config::GitHubIntegrationConfig,
                       provider: &Arc<dyn CodePlatformProvider>|
         -> SharedLoop {
            let triage = crate::triage::IssueTriage::rules_only();
            let mut loop_ = crate::discovery_loop::GitHubDiscoveryLoop::new(
                provider.clone(),
                triage,
                config.clone(),
                orchestrator.clone(),
                reflection.clone(),
            );
            if let Some(gate) = gate.clone() {
                loop_ = loop_.with_gate(gate);
            }
            Arc::new(tokio::sync::Mutex::new(loop_))
        };
        let github_shared = match (&self.config, &self.provider) {
            (Some(c), Some(p)) => Some(mk_loop(c, p)),
            _ => None,
        };
        let gitee_shared = self.gitee.as_ref().map(|(cfg, p)| mk_loop(cfg, p));
        if github_shared.is_none() && gitee_shared.is_none() {
            self.store_loops(tx, handles);
            return Ok(());
        }

        let mode = loop_cfg.discovery_mode.as_str();
        let use_polling = mode == "polling" || mode == "both";
        let use_events = mode == "events" || mode == "both";
        if !use_polling && !use_events {
            warn!(
                mode,
                "discovery_mode 无法识别（polling/events/both），集成不启动"
            );
            self.store_loops(tx, handles);
            return Ok(());
        }

        if use_polling {
            if let (Some(shared), Some(cfg)) = (github_shared.clone(), self.config.clone()) {
                handles.push(spawn_polling_loop(
                    "github",
                    shared,
                    cfg.poll_interval_secs,
                    self.shutdown.clone(),
                ));
                info!("GitHub discovery polling loop started");
            }
            if let Some(shared) = gitee_shared.clone() {
                let interval = self
                    .gitee
                    .as_ref()
                    .map(|(c, _)| c.poll_interval_secs)
                    .unwrap_or(300);
                handles.push(spawn_polling_loop(
                    "gitee",
                    shared,
                    interval,
                    self.shutdown.clone(),
                ));
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

        self.store_loops(tx, handles);
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        // 先触发循环的停止信号，再停 HTTP 服务、再兜底 abort：顺序决定读数——
        // 信号在后的话，被 abort 的循环先一步跑到 Drop，那次结束没有任何东西
        // 能证明是"被要求停的"，于是每次干净关机都会给每个循环记一次死亡。
        self.shutdown.trigger();
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
    // 池全灭时按 SchedulerGate 跳过 LLM 依赖轮次；supervisor 缺席也能跑。
    optional_requires: &["supervisor"],
    factory: || Box::new(GitHubPlugin::new()),
};
