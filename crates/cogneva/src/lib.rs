//! Cogneva library — pure wiring and composition root.
//! This crate is both a library (for integration tests) and a binary.
//! All modules are declared here so tests can access them via `cogneva::`.

pub mod aof_repair;
pub mod assembly;
pub mod backup;
pub mod backup_crypto;
pub mod bootstrap;
pub mod build_info;
pub mod claim_store;
pub mod cli;
pub mod config_loader;
pub mod config_watcher;
pub mod conversation;
pub mod daemon;
pub mod eval_container;
pub mod eval_hle;
pub mod eval_mcp_in_pod;
pub mod eval_platform;
pub mod eval_subset;
pub mod eval_table;
pub mod eval_toolathlon;
pub mod eval_toolathlon_local;
pub mod gateway_observability;
pub mod governance;
pub mod governance_acts;
pub mod governance_readings;
pub mod health_check;
pub mod host_ledger;
pub mod hot_reload;
pub mod mcp_serve;
pub mod pidfile;
pub mod pin_census;
pub mod platform;
pub mod plugin_registry;
pub mod principles;
pub mod principles_cli;
pub mod process_census;
pub mod process_report;
pub mod schema_bundle;
pub mod scraper_endpoint;
pub mod self_change;
pub mod shutdown_coordinator;
pub mod startup_guard;
pub mod validate_config;
pub mod volume_walker;
pub mod work;
pub mod write_role;

#[cfg(test)]
mod upstream_request_shape_rejection_contract;

#[cfg(windows)]
pub mod windows_service;

/// Global shutdown coordinator reference for Windows Service STOP handling.
pub static SHUTDOWN: std::sync::OnceLock<shutdown_coordinator::ShutdownCoordinator> =
    std::sync::OnceLock::new();

use std::sync::Arc;
use tracing::warn;

/// Pure wiring logic: initialize components and connect them together.
pub async fn run_app() -> Result<(), Box<dyn std::error::Error>> {
    // 正式日志栈由 observability 插件安装，而它排在 net/storage 之后——在那
    // 之前的一切 tracing 事件（配置加载、版本行、身份播种、先初始化的插件）
    // 会落进 tracing 的 no-op 默认订阅者被静默丢弃，而那正是启动失败最常发生
    // 的窗口。这里先装一个最小 stdout 订阅者兜底；插件初始化时不二次安装
    // （全局订阅者只能装一次），而是把配置好的输出栈热替换进来并重载过滤器。
    // 只能放在 run_app()：子命令二进制（mainline-rollout、command-server、
    // security-gateway）各自用会 panic 的 .init() 装订阅者，不走这条路径。
    cog_observability::install_early_subscriber();
    // 出生占用主要在前半分钟里落地，而它占掉的是运行期可用的那份预算：这里每个
    // 点是那一刻整进程的常驻内存快照，读者按相邻两点的差读出每一段自己的代价。
    // 排在订阅者之后，否则第一条相位（`process-start`）同样落进 no-op 默认订阅者，
    // 日志里就只有六条——而按日志读启动的人正是拿「最后一条相位」当作停在哪里的。
    cog_observability::boot_footprint::mark("process-start");
    // 模型会话的相位由加载它的 crate 报，而那个 crate 不认识 observability：
    // 这里把读者交过去。必须在任何插件初始化之前——第一条属于模型的相位就发生
    // 在插件 init 之内，晚一行就是那一行之前的相位一次也报不出来。
    cog_memory::boot_mark::install(std::sync::Arc::new(
        cog_observability::boot_footprint::BootMarks,
    ));

    let mut config = assembly::infra::load_and_normalize_config();
    let app_version = if config.core.app.version.is_empty() {
        env!("CARGO_PKG_VERSION")
    } else {
        config.core.app.version.as_str()
    };
    // Both names, side by side, because they can disagree and that is the
    // interesting case: `app_version` is what the deployment claims (the
    // release its manifests were rendered from), the build label is what the
    // code actually is. A deployment that claims a release it has moved 92
    // commits past shows up here and nowhere else.
    tracing::info!(
        "Cogneva v{} (build {}, rev {}) starting",
        app_version,
        crate::build_info::VERSION_ID,
        crate::build_info::GIT_REVISION
    );
    // 解析 secret://env|file|vault 引用（审计 3.3）。
    config_loader::resolve_secret_refs(&mut config).await?;
    cog_observability::boot_footprint::mark("config-loaded");

    // 身份状态文件必须在任何插件初始化之前落地：分支名 `evol/<id>` 与实例
    // 工作树名由 cog-reflection 直接读状态文件得出，而身份解析发生在
    // cog-github 插件里，冷启动时谁先读谁就决定了名字。
    if let Some(identity) = cog_github::identity::seed_from_env().await {
        tracing::info!(
            handle = %identity.handle,
            branch = %identity.branch_id,
            "instance identity seeded from the provisioned fingerprint"
        );
    }

    let ctx = cog_core::PluginContext::new(config.core.clone());
    ctx.publish(Arc::new(config.core.clone()));

    // 镜像里的运行时资产（skills、migrations）由主线 overlay 从被部署 rev 的
    // 检出里重拷，而构建 overlay 的部署器永远是上一代二进制：它按上一代的资产
    // 表拷，本 rev 新增的资产就漏在镜像基底里那份旧副本上，且没有任何一面说得
    // 出现在跑的是哪一份。这一判在这里做——本进程是唯一跑最新代码的一侧——判据
    // 是 overlay 记在镜像里的那份拷贝记录，对本 rev 声明的每个落点逐个比。
    let declared_assets = cog_reflection::runtime_assets::embedded_asset_list();
    let asset_report = cog_reflection::runtime_assets::verify(
        std::path::Path::new(cog_reflection::runtime_assets::RUNTIME_ASSET_MANIFEST_DEST),
        &declared_assets,
    )
    .await;
    asset_report.log();
    let asset_observable = Arc::new(cog_reflection::runtime_assets::AssetIdentityObservable::new());
    asset_observable.record(asset_report).await;
    ctx.publish_observable(asset_observable);

    // A background loop has no parent task: when it panics or returns early it
    // takes the series it produced with it, and "nobody writes this series any
    // more" reads the same as "the subsystem behind it has nothing to report".
    // The census of loops therefore lives in a module-level registry and its age
    // is computed when the scrape arrives — there is no publisher task, because a
    // publisher is another thing that can die silently. Published from the
    // composition root so every process reports its own census, whatever plugins
    // it enabled.
    ctx.publish_observable(cog_core::loop_health::observable());

    // The census the runner takes at the end of startup, published as a reading.
    // It has to be published here rather than where it is filled: the gateway
    // takes its snapshot of the observables during `init`, and the census is not
    // taken until every plugin has started. A handle published from `start`
    // would be a series the exposition never renders.
    let pin_census = Arc::new(pin_census::PinCensusObservable::new());
    ctx.publish_observable(pin_census.clone());

    // The restart budget belongs to the process's loops rather than to any one of
    // them, so it is installed once here, from the same document every plugin
    // reads — a loop that panicked must not be judged differently depending on
    // which plugin happened to start it. Installed before any plugin initialises,
    // because a loop started by the first plugin has to be under the configured
    // budget and not the built-in one.
    cog_core::loop_health::install_restart_settings(cog_core::loop_health::RestartSettings {
        max_consecutive: config.core.system.loop_restart_max_consecutive,
        backoff_floor_secs: config.core.system.loop_restart_backoff_floor_secs,
    });

    let (daemon, _pid_file) = assembly::infra::init_daemon_and_pidfile();

    let task_event_tx = tokio::sync::broadcast::channel::<cog_core::TaskEvent>(
        config.system.task_event_channel_capacity,
    )
    .0;
    ctx.publish(Arc::new(task_event_tx.clone()));

    // Create shutdown coordinator early (before plugin init)
    let shutdown =
        shutdown_coordinator::ShutdownCoordinator::new(config.core.system.shutdown_timeout_ms);
    let _ = SHUTDOWN.set(shutdown.clone());
    let shutdown = Arc::new(shutdown);

    let mut plugin_runner = plugin_registry::register_all()?;

    // Config-driven plugin filtering
    if let Some(ref enabled) = config.system.enabled_plugins {
        plugin_runner.retain(|name| enabled.contains(&name.to_string()));
    } else if !config.system.disabled_plugins.is_empty() {
        plugin_runner.retain(|name| !config.system.disabled_plugins.contains(&name.to_string()));
    }

    // Validate that filtering did not break required dependencies
    plugin_runner.validate_after_filter()?;

    // The raw-record wire format lives in the protocol crate; the assembly is
    // what links the two crates together. Publishing it here is what lets the
    // storage plugin stay on the core contract alone — it reads the codec from
    // the context instead of naming the crate that carries it. Published before
    // `init_all` because the storage plugin builds its raw logger in `init`.
    ctx.publish_service::<dyn cog_core::RawRecordCodec>(Arc::new(
        cog_protocol::convert::ProtoCodec,
    ));

    // 界面上那一轮对话跑的那条循环。槽先发布成空的：填它要的编排器由某个插件在
    // 下面 `init_all` 里才发布，而网关在同一个 `init_all` 里就要拿到这个槽。空着
    // 的那段时间界面收到的是「还没接上」——这是有意的读数，不是要藏起来的错误。
    let conversation: cog_gateway::chat::ConversationHandle = Default::default();
    ctx.publish(conversation.clone());

    plugin_runner.init_all(&ctx).await?;
    cog_observability::boot_footprint::mark("plugins-initialized");

    // 账上此刻生效的规则交给内核：每轮开头随基础指令一起交给员工。与提示词
    // 本子同一类东西——部署的账，不是编译进二进制的一句；也没有「谁是属主」的
    // 分支，跑任务的进程与坐在界面里的进程读的是同一本。
    if let Some(principles) = ctx.consume_service::<dyn cog_core::RuntimePrincipleStore>() {
        cog_agent::install_principle_source(Arc::new(crate::principles::AccountPrinciples::new(
            principles,
        )));
    }

    // 申请改自己：装在**共用**的那张工具表上，不是某一段对话私有的那张。
    //
    // 出去干活的同事拿的就是这张表（派活的那个池子按它建每个 worker），所以装
    // 在这里它们才说得出「我想把这里改成那样」。只给界面装，等于只有坐在聊天框
    // 里的那一位有这条通道，而系统里真正在改自己的是别的同事。
    //
    // 不装进内核的 `register_builtins`：那只构造函数不认识判据载体那份名单，
    // 而申请要拒绝的正好是那份名单——名单在反射那一侧，装配根是唯一同时看得见
    // 两边的地方。申请人由调用时的派发方给出，见 `self_change::tool_for_caller`。
    if let Some(tools) = ctx.consume::<cog_agent::ToolRegistry>() {
        cog_core::ToolRegistry::register(
            &*tools,
            crate::self_change::tool_for_caller(
                ctx.consume_service::<dyn cog_core::ChangeLedgerStore>(),
            ),
        );
    }

    // 插件都起来了，编排器在手里了：把这一轮对话接到系统唯一的那条智能体循环上。
    // 填不上就明说——界面据此回答「还没接上」，而不是让网关自己另跑一条循环。
    match ctx.consume_service::<dyn cog_core::OrchestratorControl>() {
        Some(orchestrator) => {
            // 派活要过的那道准入：与 HTTP 提交面同一个管理器。没人配过上限时它
            // 本来就是 `Option::None`，那时提交面也放行，两处读的是同一个答案。
            let admission = ctx.consume_service::<dyn cog_core::HierarchyManager>();
            // 拒绝也要落一条，和提交面同一格。审计流同样是可选服务：没装它的
            // 进程本来也不写账，这里不替它编一条。
            let audit = ctx.consume_service::<dyn cog_core::AuditStream>();
            // 它手上有什么工具＝这个进程装配了什么。取内核那份具体注册表：
            // 契约 trait 只报得出名字，复制不了工具本身。
            let tools = ctx.consume::<cog_agent::ToolRegistry>();
            // 名册与收件箱是两半：有前者同事才发现得了它，有后者消息才有人读。
            // 缺哪一半都由进程如实缺席，这里不替它编一个内存实现。
            let roster = ctx.consume_service::<dyn cog_core::AgentRegistry>();
            let inbox = ctx.consume_service::<dyn cog_core::MessageBackend>();
            // 说过话的人另外记一份：注册表是心跳，人下线几十秒就被摘掉，而
            // 「我跟谁说过话」要活得比心跳久。同一份键值面，另一个键。
            let people = ctx.consume_service::<dyn cog_core::KeyValueStore>();
            // 三本只读账。它们由各自的插件发布，缺哪一本都由进程如实缺席——这里
            // 不拿一份内存账顶上：进程自己的账读起来像平台的账，而它只记得这个
            // 进程见过的那几条。
            let ledgers = crate::conversation::Ledgers {
                changes: ctx.consume_service::<dyn cog_core::ChangeLedgerStore>(),
                principles: ctx.consume_service::<dyn cog_core::PrincipleLedgerStore>(),
                alerts: ctx.consume_service::<dyn cog_core::AlertStore>(),
            };
            // 运行时原则账：与上面三本不同，它是这段对话里的人能往里写的一本——
            // 员工提案、属主拍板。上面装给内核的那一份是同一本（`consume_service`
            // 不拿走服务），所以批了之后下一轮开头读到的就是它。
            let runtime_principles = ctx.consume_service::<dyn cog_core::RuntimePrincipleStore>();
            // 进化控制面：属主在对话里放行一条等审批的变更、把正在跑的那一版退回去。
            // 由反射插件发布；没装的进程照实缺席，工具会把它说出来。
            let evolution_admin = ctx.consume_service::<dyn cog_core::EvolutionAdmin>();
            // 外部价值判定的落盘面：属主在对话里说一句「什么算好」，与网关那个提交面
            // 落到同一本账、署同一个人。没发布的进程照实缺席，工具会说「记不下来」。
            let taste_sink = ctx.consume_service::<dyn cog_core::TasteIntentSink>();
            // 平台上每个智能体开头读到的那段话是部署声明的，不是编译进二进制的
            // 一句。声明在 `prompts/` 里，随 ConfigMap 挂进进程；这里把本子交给
            // 内核，任务尝试与界面上的对话都从同一处取。没装提示词插件的进程
            // 就是没有这本子，那一轮按没有这段指示跑。
            if let Some(prompts) = ctx.consume_service::<dyn cog_core::PromptProvider>() {
                cog_agent::install_prompt_provider(prompts);
            }
            if conversation
                .set(Arc::new(crate::conversation::PlatformConversation::new(
                    orchestrator,
                    admission,
                    audit,
                    tools,
                    roster,
                    inbox,
                    people,
                    ledgers,
                    runtime_principles,
                    evolution_admin,
                    taste_sink,
                )))
                .is_err()
            {
                tracing::warn!("conversation slot was already filled; keeping the first wiring");
            }
        }
        None => {
            tracing::warn!(
                "no orchestrator control published — the Web UI chat will answer that it is not wired"
            );
        }
    }

    // Register shutdown hook now that RawLogger is available
    if let Some(raw_logger) = ctx.consume_service::<dyn cog_core::RawLogger>() {
        let logger = raw_logger.clone();
        shutdown.register_hook(move || {
            let l = logger.clone();
            async move {
                tracing::info!("Shutdown hook: flushing raw logger");
                if let Err(e) = l.shutdown().await {
                    tracing::warn!("Raw logger shutdown failed: {}", e);
                }
            }
        });
    }

    // Publish shutdown infrastructure so plugins can consume it
    let (shutdown_broadcast_tx, _shutdown_broadcast_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_signal = shutdown.signal();
    tokio::spawn({
        let sig = shutdown_signal.clone();
        let tx = shutdown_broadcast_tx.clone();
        async move {
            sig.wait().await;
            let _ = tx.send(());
        }
    });
    ctx.publish(shutdown.clone());
    ctx.publish(Arc::new(cog_core::ShutdownBroadcastTx(
        shutdown_broadcast_tx,
    )));
    ctx.publish(Arc::new(shutdown_signal));

    plugin_runner.start_all(&ctx).await?;
    cog_observability::boot_footprint::mark("serving");

    // Every plugin has started, so the graph is as observed as it will ever be:
    // take the same census the runner logs and hand it to the reading.
    pin_census.record(pin_census::census(&ctx)).await;

    // Wire the LLM client into the gateway's WebSocket chat handler. The llm
    // plugin is not topologically ordered before gateway, so the slot is
    // filled here after every plugin has initialised. The published client is
    // the hot-swappable wrapper, so later config-driven swaps stay effective.
    if let Some(gateway_state) = ctx.consume::<cog_gateway::GatewayState>() {
        let llm = ctx.consume_service::<dyn cog_core::LlmClient>();
        *gateway_state
            .llm_client
            .write()
            .unwrap_or_else(|e| e.into_inner()) = llm;
    }

    daemon.ready();

    // Spawn config hot-reload consumer (direct orchestration, not a plugin)
    let (config_watcher_opt, _notify_watcher) = assembly::infra::init_config_watcher();
    if let Some(watcher) = config_watcher_opt {
        let mut rx = watcher.subscribe();
        let log_handle = ctx
            .consume::<cog_observability::plugin::LogFilterHandleHolder>()
            .map(|h| h.0.clone());
        let supervisor_config_tx = ctx
            .require::<cog_supervisor::plugin::SupervisorConfigTxHolder>()?
            .0
            .clone();
        let gateway_state = ctx.require::<cog_gateway::GatewayState>()?;
        let llm_hot_swap = ctx.consume::<cog_llm::HotSwappableLlmClient>();
        let llm_http_client = ctx.consume_service::<dyn cog_core::HttpClient>();

        // llm_routing / tuning 已下沉 cog-llm（core Config 不再聚合）：
        // 初始与每轮热重载都经 cog-llm 自载器读 cogneva.json。
        let initial_llm_routing = cog_llm::LLMRoutingConfig::load()
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        // The reload path below re-applies a handful of sections; every other
        // section is read once while a plugin starts, so a change to it stays
        // inert until the pod restarts. State which those are once at startup:
        // the coverage of the reload path is otherwise something a reader has to
        // infer from what it happens to log.
        let startup_only: Vec<&str> = cog_core::config_sections::CONFIG_SECTIONS
            .iter()
            .filter(|s| s.effect == cog_core::config_sections::ConfigEffect::RestartRequired)
            .map(|s| s.name)
            .collect();
        tracing::info!(
            "config sections that only take effect at startup ({}): {}",
            startup_only.len(),
            startup_only.join(", ")
        );
        tokio::spawn(async move {
            let mut active_llm_routing = initial_llm_routing;
            while rx.changed().await.is_ok() {
                let new_config = rx.borrow_and_update().clone();
                let mut applied = Vec::new();
                let mut need_restart = Vec::new();

                // 1. log_level
                if let Some(ref handle) = log_handle {
                    if let Ok(new_filter) =
                        tracing_subscriber::EnvFilter::try_new(&new_config.app.log_level)
                    {
                        if let Err(e) = handle.reload(new_filter) {
                            tracing::warn!("Failed to reload log filter: {}", e);
                        } else {
                            applied.push(format!("log_level={}", new_config.app.log_level));
                        }
                    }
                }

                // 2. gateway config
                let (gateway_applied, gateway_restart) =
                    crate::hot_reload::apply_gateway_config_update(
                        &gateway_state.config,
                        &new_config.gateway,
                        &gateway_state.request_timeout_secs,
                    );
                applied.extend(gateway_applied);
                need_restart.extend(gateway_restart.clone());
                if !gateway_restart.is_empty() {
                    tracing::error!(
                        "PORT CHANGE DETECTED: {}. These CANNOT be hot-reloaded. The process MUST be restarted.",
                        gateway_restart.join("; ")
                    );
                }

                // 3. supervisor intervals
                let new_supervisor_cfg: cog_supervisor::SupervisorConfig =
                    new_config.supervisor.clone().into();
                if let Err(e) = supervisor_config_tx.send(new_supervisor_cfg) {
                    tracing::warn!("Failed to send supervisor config reload: {}", e);
                } else {
                    applied.push("supervisor_intervals_updated".to_string());
                }

                // 4. LLM provider hot-swap
                let new_llm_routing = match cog_llm::LLMRoutingConfig::load() {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("llm_routing reload skipped: {}", e);
                        continue;
                    }
                };
                if active_llm_routing != new_llm_routing {
                    let new_stream_capacity = match cog_llm::TuningConfig::load() {
                        Ok(t) => t.stream_capacity,
                        Err(e) => {
                            tracing::warn!("tuning reload skipped: {}", e);
                            continue;
                        }
                    };
                    if let Some(ref llm_hot_swap) = llm_hot_swap {
                        let new_provider = match cog_llm::plugin::build_llm_provider(
                            new_stream_capacity,
                            new_config.system.anthropic_default_max_tokens,
                            &new_llm_routing,
                            llm_http_client.clone(),
                        ) {
                            Ok(p) => Some(p),
                            Err(e) => {
                                tracing::warn!("Failed to reload LLM provider graph: {}", e);
                                None
                            }
                        };
                        if let Some(new_provider) = new_provider {
                            llm_hot_swap.swap(new_provider).await;
                            active_llm_routing = new_llm_routing.clone();
                            applied.push("llm_provider_swapped".to_string());
                        }
                    } else {
                        need_restart
                            .push("llm_config_changed_but_no_provider_initialized".to_string());
                    }
                }

                // 5. metrics
                need_restart.push(format!(
                    "metrics_enabled={}, sample_max_rows={} (requires restart)",
                    new_config.metrics.enabled, new_config.metrics.sample_max_rows
                ));

                if !applied.is_empty() {
                    tracing::info!("Config hot-reloaded (applied): {}", applied.join("; "));
                }
                if !need_restart.is_empty() {
                    tracing::error!(
                        "Config changed but requires restart: {}",
                        need_restart.join("; ")
                    );
                }
            }
        });
    }

    let _signal_handle = shutdown.spawn_signal_listener();
    shutdown.signal().wait().await;
    warn!("Shutdown signal received, stopping plugins...");

    plugin_runner.shutdown_all().await?;
    shutdown.wait_for_shutdown().await;
    warn!("Cogneva shutdown complete");
    Ok(())
}

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;

/// 单测共用的样例：真的走一遍 HTTP，读回状态码与正文。
#[cfg(test)]
pub(crate) mod test_support;
