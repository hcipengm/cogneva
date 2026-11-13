//! Agent plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{debug, info, warn};

/// Loop name reported through the background-loop liveness family.
pub const GRPC_CONTROL_HEARTBEAT_LOOP: &str = "agent_grpc_control_heartbeat";

/// Loop name of the reading that moves the pool's concurrency.
pub const POOL_SIZING_LOOP: &str = "agent_pool_sizing";

/// Loop name of the heartbeat that keeps the evaluator's roster row alive.
pub const EVAL_EMPLOYEE_HEARTBEAT_LOOP: &str = "agent_eval_employee_heartbeat";

/// How often the container's memory is re-read against the count in force.
///
/// Shorter than the pool's own task length on purpose: a task holds its turn
/// for minutes, so a reading that stepped faster than that could shrink the
/// pool and regrow it between two tasks without either reading being wrong.
// cap: bound — 负载以分钟为单位变，比那更快的读数会在两次任务之间把池缩了又长回来，而两次读数都没错
const POOL_SIZING_PERIOD: std::time::Duration = std::time::Duration::from_secs(30);

/// How often this agent says it is alive to the control plane. Short enough
/// that a restarted control plane re-learns its agents without a redeploy, long
/// enough that the heartbeat is not the busiest thing on the channel.
// cap: bound — 短到重启的控制面不用重新部署就能重新认出它的 agent，长到心跳不是这条通道上最忙的东西
const HEARTBEAT_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

/// Keep the pool's concurrency equal to what the container can carry.
///
/// The declared worker count is the ceiling this moves under, never above: a
/// `worker_count` of three means "three if the box allows it", and the box is
/// asked every period. The reading is logged on every move and on the first
/// pass, so a pool sitting at one slot can be told apart from one that never
/// read anything.
fn spawn_pool_sizing_loop(
    concurrency: Arc<crate::pool_sizing::PoolConcurrency>,
    spawner: PoolSpawner,
) {
    drop(cog_core::loop_health::spawn_unstoppable(
        POOL_SIZING_LOOP,
        cog_core::loop_health::Cadence::Periodic(POOL_SIZING_PERIOD),
        move |beat| {
            let concurrency = concurrency.clone();
            let spawner = spawner.clone();
            async move {
                let mut interval = tokio::time::interval(POOL_SIZING_PERIOD);
                let mut cost = crate::pool_sizing::SlotCost::default();
                let mut first = true;
                loop {
                    beat.beat();
                    interval.tick().await;
                    let reading = crate::pool_sizing::read_container_memory();
                    if let Some((working_set, _)) = reading {
                        cost.observe(working_set, concurrency.in_use());
                    }
                    let plan = crate::pool_sizing::next_slots(
                        concurrency.slots(),
                        concurrency.declared_cap(),
                        reading,
                        cost,
                    );
                    let changed = concurrency.set_slots(plan.slots).await;
                    // Workers are objects, the count is a measurement: the
                    // pool holds as many as the count in force, and until a
                    // task has been measured it adds one only while every
                    // worker it has is busy -- otherwise a quiet container
                    // would grow a consumer a minute for doing nothing.
                    let spawned = spawner.spawned();
                    let target = match plan.capacity {
                        Some(_) => plan.slots,
                        None if concurrency.in_use() >= spawned.max(1) => {
                            (spawned + 1).min(plan.slots.max(1))
                        }
                        None => spawned,
                    };
                    while spawner.spawned() < target {
                        spawner.spawn_one().await;
                    }
                    if changed || first || plan.movement != crate::pool_sizing::SlotMove::Hold {
                        info!(
                            slots = plan.slots,
                            workers = spawner.spawned(),
                            capacity = plan.capacity,
                            movement = ?plan.movement,
                            memory_fraction = plan.memory_fraction,
                            "agent pool concurrency"
                        );
                    }
                    crate::observable::global_observable().publish_pool_sizing(
                        plan.slots,
                        spawner.spawned(),
                        plan.capacity,
                        plan.memory_fraction,
                    );
                    first = false;
                }
            }
        },
    ));
}

/// Everything one pool worker needs to be created again later.
///
/// The count of workers is a measurement, so workers are created from the
/// sizing loop and not only from startup: this holds what the startup loop
/// used to capture directly.
#[derive(Clone)]
struct PoolSpawner {
    pool: Arc<crate::GlobalAgentManager>,
    llm: Arc<dyn cog_core::LlmClient>,
    runner: Arc<dyn cog_core::TaskExecutionCallback>,
    concurrency: Arc<crate::pool_sizing::PoolConcurrency>,
    role: String,
    instance: String,
    workspace: String,
    spawned: Arc<std::sync::atomic::AtomicUsize>,
}

/// The worker object a pool worker's own inbox handler answers with.
///
/// The handler has to be handed to `spawn_worker` before the worker exists, and
/// the worker is what the handler needs: this cell closes that loop. A message
/// that arrives before the cell is filled is kept pending for redelivery rather
/// than acknowledged with nothing done.
type PoolWorkerCell = Arc<tokio::sync::OnceCell<Arc<crate::Agent>>>;

/// Whether what landed in a worker's inbox is a piece of work or a sentence
/// addressed to the worker.
///
/// One judgement: a `goal` that reads as a task is work. The other half used to
/// be swallowed — the message was acknowledged, the worker did nothing, and the
/// person asking only ever saw a timeout. The workbench's dialogue box is
/// exactly that half.
fn inbox_task(msg: &cog_core::InboxMessage) -> Option<cog_core::Task> {
    let cog_core::InboxMessage::Prompt { goal, .. } = msg else {
        return None;
    };
    serde_json::from_value::<cog_core::Task>(goal.clone()).ok()
}

/// What a pool worker does with each command that reaches its inbox.
///
/// Work goes through the turn gate and the task runner: a worker that has no
/// turn yet waits for one rather than dropping the message, because the message
/// is already taken and a dropped one is work nobody will be handed again.
///
/// Everything else is a conversation. It goes to the worker's own command path,
/// which executes every other `InboxMessage` variant and publishes a prompt's
/// answer onto the stream the asker named. Handing a sentence to the task
/// runner would be reading it as work; acking it without running anything is
/// how this route stayed silent.
fn pool_worker_handler(
    runner: Arc<dyn cog_core::TaskExecutionCallback>,
    gate: Arc<crate::pool_sizing::PoolConcurrency>,
    worker: PoolWorkerCell,
) -> impl FnMut(cog_core::InboxMessage) -> futures::future::BoxFuture<'static, cog_core::SFResult<()>>
       + Send
       + 'static {
    move |msg| {
        let runner = runner.clone();
        let gate = gate.clone();
        let worker = worker.clone();
        Box::pin(async move {
            if let Some(task) = inbox_task(&msg) {
                let _turn = gate.acquire().await;
                runner.execute_task(task).await;
                return Ok(());
            }
            match worker.get() {
                Some(agent) => agent.handle_inbox(msg).await,
                None => Err(cog_core::SFError::Agent(
                    "a pool worker's inbox consumer started before the worker itself; the \
                     message is kept pending for redelivery"
                        .into(),
                )),
            }
        })
    }
}

impl PoolSpawner {
    fn spawned(&self) -> usize {
        self.spawned.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Create the next worker. The slot number is the worker's own index, so
    /// a name is never reused by two objects.
    async fn spawn_one(&self) {
        let slot = self
            .spawned
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = crate::identity::worker_name(slot);
        if !name.stable {
            warn!(
                slot, name = %name.handle,
                "no instance fingerprint available; worker names change with the next pod roll"
            );
        }
        let agent_id = name.id.clone();
        let registration = cog_core::AgentRegistration::new(
            agent_id.clone(),
            &self.instance,
            "127.0.0.1",
            &self.role,
            &self.workspace,
            vec!["plan".into(), "generate".into(), "evaluate".into()],
            cog_core::ResourceInfo::default(),
        );
        let worker = PoolWorkerCell::default();
        let handler = pool_worker_handler(
            self.runner.clone(),
            self.concurrency.clone(),
            worker.clone(),
        );
        match self
            .pool
            .spawn_worker(
                agent_id.clone(),
                self.role.clone(),
                self.llm.clone(),
                registration,
                handler,
            )
            .await
        {
            Ok(handle) => {
                // The loop closes here: from this point on, every sentence that
                // reaches this worker's inbox has an object to answer with.
                if worker.set(handle.agent.clone()).is_err() {
                    warn!(
                        agent_id = %agent_id,
                        "pool worker cell was already filled; the earlier object answers"
                    );
                }
                info!(
                    "Agent pool worker {} spawned (id={}, role={:?})",
                    name.handle, agent_id, self.role
                );
            }
            Err(e) => {
                warn!("Failed to spawn agent pool worker {}: {}", agent_id, e);
            }
        }
    }
}

/// Agent plugin that self-assembles and publishes hook engine, agent registry,
/// and tool registry.
use futures::StreamExt;

pub struct AgentPlugin {
    initialized: bool,
    agent_registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    pool: Option<Arc<crate::GlobalAgentManager>>,
    lifecycle_client: Option<Arc<dyn cog_core::AgentLifecycleClient>>,
    eval_event_rx: tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<cog_core::AgentEvent>>>,
    /// 这个进程里写判词的那一位的 id：登记成功才有。心跳环与关停注销都按这一位
    /// 走；没有它就是没登上，读面会看到一条告警而不是一个假人。
    eval_employee: Option<String>,
    /// 关停时等在飞的运行让路多久，来自 `core.system.shutdown_timeout_ms`。
    /// 零表示没人声明过，那就**不等**：一个没有依据的等待比不等更糟。
    drain_timeout_ms: std::sync::atomic::AtomicU64,
    /// 关停排空的读数写到哪里。与 `drain_timeout_ms` 相邻不是偶然：那个数是
    /// 这一格的界，这个读数说的是界够不够用，两个必须落在同一个进程里。
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
}

impl AgentPlugin {
    /// Create a plugin that will build agent services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            agent_registry: None,
            pool: None,
            lifecycle_client: None,
            eval_event_rx: tokio::sync::Mutex::new(None),
            eval_employee: None,
            drain_timeout_ms: std::sync::atomic::AtomicU64::new(0),
            metrics: None,
        }
    }

    /// 记一次排空：等了多久、等到了没有、还有几条没等到。
    ///
    /// `left` 只在超时时非零；等到零的那一次它也记一条 0，让「上一次超时留下了
    /// 三条、这一次一条没留」在同一条序列上读得出来——一个只在坏结局里才出现的
    /// 计数器，读的人分不清「归零了」与「这版还没装」。写失败只打一行日志：
    /// 进程正在退出，读数写不进去不该改变关停顺序。
    async fn record_drain(&self, millis: f64, outcome: &'static str, left: usize) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };
        let mut labels = std::collections::HashMap::new();
        labels.insert(crate::LABEL_OUTCOME.to_string(), outcome.to_string());
        if let Err(e) = metrics
            .record_histogram(
                cog_core::metric_names::SHUTDOWN_DRAIN_MILLISECONDS,
                millis,
                labels,
            )
            .await
        {
            warn!(error = %e, "could not record how long the shutdown drain took");
        }
        if let Err(e) = metrics
            .record_counter(
                cog_core::metric_names::SHUTDOWN_DRAIN_ABANDONED_TOTAL,
                left as f64,
                std::collections::HashMap::new(),
            )
            .await
        {
            warn!(error = %e, "could not record the runs the drain left behind");
        }
    }

    /// 记一次「没有声明预算、根本没有等」。
    async fn record_drain_undeclared(&self) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };
        if let Err(e) = metrics
            .record_counter(
                cog_core::metric_names::SHUTDOWN_DRAIN_UNDECLARED_TOTAL,
                1.0,
                std::collections::HashMap::new(),
            )
            .await
        {
            warn!(error = %e, "could not record a shutdown with no drain budget");
        }
    }
}

/// 排空这一格的结局取值。闭集，读者按它选行。
const DRAIN_DRAINED: &str = "drained";
const DRAIN_TIMEOUT: &str = "timeout";

impl Default for AgentPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for AgentPlugin {
    fn name(&self) -> &'static str {
        "agent"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        self.metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
        self.drain_timeout_ms.store(
            ctx.config().system.shutdown_timeout_ms,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Snapshot config values to drop immutable borrow before publishing.
        let (
            hook_change_dir,
            nats_config,
            hook_engine_config,
            agent_config,
            mbc,
            require_tool_identity,
        ) = {
            let config = ctx.config();
            (
                cog_core::config::self_evolution_hook_dir(&config.self_evolution.change_dir),
                config.dag_executor.nats.clone(),
                config.hook_engine.clone(),
                config.agent.clone(),
                config.multi_backend_consumer.clone(),
                config.system.require_tool_identity,
            )
        };

        // Consume dependencies published by earlier plugins.
        let shared_message_backend = ctx.consume_service::<dyn cog_core::MessageBackend>();
        let sandbox_backend = ctx.require_service::<dyn cog_core::SandboxBackend>()?;
        let guardrail = ctx.require_service::<dyn cog_core::Guardrail>()?;
        let plugin_registry = ctx.require_service::<dyn cog_core::PluginRegistry>()?;

        // Clone before moving into tool_registry so eval runtime can reuse them.
        let sandbox_backend_for_eval = sandbox_backend.clone();
        // And the pool: the registry's handle is what a worker's tools use, this
        // one is what the worker itself reads its checkout through.
        let sandbox_backend_for_pool = sandbox_backend.clone();
        let _guardrail_for_eval = guardrail.clone();
        let plugin_registry_for_eval = plugin_registry.clone();
        let hook_archive = ctx.consume_service::<dyn cog_core::HookArchive>();
        let agent_registry = ctx.require_service::<dyn cog_core::AgentRegistry>()?;
        // The agent's durable event log. Required, like the checkpoint store
        // below: a checkpoint taken without a log records `event_offset: 0`, so
        // a restore replays nothing and the events after the snapshot are gone
        // -- and that reads exactly like a clean resume.
        let wal_backend = ctx.require_service::<dyn cog_core::WalBackend>()?;

        // ── HookEngine ──
        let redis_backend: Arc<dyn cog_core::MessageBackend> = match shared_message_backend.clone()
        {
            Some(backend) => backend,
            None => {
                return Err(cog_core::SFError::Config(
                    "No message backend available for HookEngine".into(),
                ));
            }
        };

        let jetstream_backend: Option<Arc<dyn cog_core::MessageBackend>> =
            if !nats_config.urls.is_empty() {
                shared_message_backend.clone()
            } else {
                info!("HookEngine JetStream not configured. Tier 2 disabled.");
                None
            };

        let archive = hook_archive;

        let mut publisher = crate::TieredHookPublisher::new().with_redis(redis_backend);
        if let Some(js) = jetstream_backend {
            publisher = publisher.with_jetstream(js);
        }
        if let Some(arc) = archive {
            publisher = publisher.with_archive(arc);
        }
        if let Some(stream) = ctx.consume_service::<dyn cog_core::AuditStream>() {
            publisher = publisher.with_audit_stream(stream);
        }

        info!("HookEngine initialized with 3-tier publisher");
        let hook_engine = Arc::new(crate::HookEngine::with_config(
            Arc::new(publisher),
            hook_engine_config.into(),
        ));

        // Load persisted hooks from the change dir's hooks/ on startup. The path
        // comes from the same helper the synthesizer writes through: the two run
        // in different processes, and a locally-built path here silently loads
        // nothing.
        {
            match hook_engine.load_from_dir(&hook_change_dir).await {
                Ok(n) if n > 0 => info!(
                    "Loaded {} persisted hook(s) from {}",
                    n,
                    hook_change_dir.display()
                ),
                Ok(_) => debug!(
                    "No persisted hooks to load from {}",
                    hook_change_dir.display()
                ),
                Err(e) => warn!("Failed to load persisted hooks: {}", e),
            }
        }

        ctx.publish(hook_engine.clone());
        let hook_engine_dyn: Arc<dyn cog_core::HookEngine> = hook_engine;
        ctx.publish_service(hook_engine_dyn);
        info!("AgentPlugin hook engine published");

        // ── AgentRegistry ──
        self.agent_registry = Some(agent_registry.clone());
        info!("AgentPlugin agent registry consumed");

        // ── AgentLifecycleClient (gRPC control plane) ──
        let lifecycle_client = ctx.consume_service::<dyn cog_core::AgentLifecycleClient>();
        if lifecycle_client.is_some() {
            info!("AgentPlugin AgentLifecycleClient consumed");
        }
        self.lifecycle_client = lifecycle_client;

        // ── ToolRegistry ──
        let tool_registry = Arc::new(
            crate::ToolRegistry::new()
                .with_sandbox_backend(sandbox_backend.clone())
                .with_guardrail(guardrail)
                .with_plugin_registry(plugin_registry)
                .with_require_identity(require_tool_identity),
        );
        // Built-in execution tools. Without these the registry advertises zero
        // tool definitions and squads can plan but never act on the
        // environment.
        //
        // The list is registered through the one constructor for it rather
        // than laid out here, because this process is not the only caller: the
        // MCP server serves the same built-ins to an external client, and a
        // second list here would be a second answer to which tools exist.
        // `agent_loop` is a cog-agent-owned config section, read once here for
        // the memory API base the archiving tools borrow, for the fetched-body
        // threshold, and for the evaluator's budgets below; changing a value
        // needs no new image.
        let agent_loop_config = crate::AgentLoopConfig::load()?;
        let memory_api_base = agent_loop_config.memory_api_base.clone();
        // Memory tools read through the platform memory API over HTTP rather
        // than an in-process backend: the process that runs the loops holds no
        // memory dataset on purpose, so an in-process handle is absent exactly
        // where a squad runs, while the API's internal face is reachable with
        // no credentials. They stay registered even when the base or the client
        // is missing, so a squad can tell "this layer is absent" from "this
        // namespace is empty": registered-but-failing says the first, while a
        // skipped registration would look like the second.
        crate::tools::builtins::register_builtins(
            &*tool_registry,
            crate::tools::builtins::BuiltinInputs {
                http_client: ctx.consume_service::<dyn cog_core::HttpClient>(),
                metrics: ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                memory_api_base: memory_api_base.clone(),
                external_archive_threshold_chars: agent_loop_config
                    .external_archive_threshold_chars,
                // The pair that lets one agent address another. Both travel
                // into the same tool: a message without the roster check is a
                // key no consumer ever reads, and the roster without the queue
                // has nowhere to send it.
                message_backend: shared_message_backend.clone(),
                agent_registry: Some(agent_registry.clone()),
                web_search_base: agent_loop_config.web_search_base.clone(),
                web_search_fallback_base: agent_loop_config.web_search_fallback_base.clone(),
                web_fetch_base: agent_loop_config.web_fetch_base.clone(),
                // 搜索、glob、git、打补丁与 `run_command` 走同一个执行器：它们
                // 的答案取决于调用者自己那棵树，在本进程跑就等于读一份与调用者
                // 无关的源码。后端在交给注册表时克隆了一份，这里再拿一份。
                sandbox_backend: Some(sandbox_backend.clone()),
            },
        );
        info!(
            tools = ?tool_registry.names(),
            "AgentPlugin built-in tools registered"
        );
        ctx.publish(tool_registry.clone());
        let tool_registry_dyn: Arc<dyn cog_core::ToolRegistry> = tool_registry.clone();
        ctx.publish_service(tool_registry_dyn);
        info!("AgentPlugin tool registry published");

        // ── Eval AgentRuntime ──
        // The loop archives a tool output that will not fit the window by
        // POSTing it to the platform memory API — this process holds no memory
        // dataset (it is switched off where the loops run), so an in-process
        // handle would be absent exactly here. One archive handle serves the
        // eval runtime and the whole worker pool: the reading it carries is
        // keyed by tool, and a second copy would only split the count.
        let tool_output_archive: Option<Arc<dyn crate::ToolOutputArchive>> = ctx
            .consume_service::<dyn cog_core::HttpClient>()
            .map(|client| {
                let metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
                Arc::new(crate::HttpToolOutputArchive::new(
                    client,
                    memory_api_base.clone(),
                    metrics,
                )) as Arc<dyn crate::ToolOutputArchive>
            });
        if tool_output_archive.is_none() {
            warn!(
                "HttpClient unavailable; a tool output too large for the context window \
                 cannot be archived and its dropped tail is unrecoverable"
            );
        }
        let (eval_event_tx, eval_event_rx) =
            tokio::sync::mpsc::channel::<cog_core::AgentEvent>(128);
        // 评测这一侧的运行同样是这个进程里的一次模型调用，跑它的也是个人：
        // 名字与常驻池用同一个派生函数，只是分组不同，所以同一个进程里两批
        // 人不会撞名。从前这里写着 `eval-agent`——名册上没有、工号也没有的
        // 一个名字：账上读不出这次判词是谁写的，同事也没法给它发消息。
        let eval_employee = crate::identity::name_for(
            &format!(
                "{}:{}",
                crate::identity::agent_instance(),
                cog_core::roles::EVALUATOR
            ),
            cog_core::roles::EVALUATOR,
            0,
        );
        let eval_declaration =
            cog_core::DeliverableDeclaration::protocol(cog_core::Deliverable::Judgement);
        let eval_config = cog_core::RuntimeConfig {
            agent_id: eval_employee.id.clone(),
            role: cog_core::roles::EVALUATOR.into(),
            max_iterations: agent_loop_config.eval_max_iterations.unwrap_or(u32::MAX),
            context_window_size: 32000,
            skill_cache_ttl_secs: 30,
            think_stall_timeout_secs: 240,
            skill_config: None,
            crew_id: None,
            squad_id: None,
            owner_task_id: None,
            // 这一份活交的是判词：它不动工作树，判的是别人交的东西。这句是这段
            // 常驻评测的结构说的，不是行上某个字段说的，所以来源是 `protocol`。
            // 结论与来源从同一份声明里取出：两格各写一遍就会各漂一次。
            deliverable: eval_declaration.conclusion(),
            deliverable_state: eval_declaration.state(),
            // 常驻的评测员工没有一件具体的活，也就没有人就它说过方向。
            stated_directions: Vec::new(),
        };
        let mut eval_runtime = crate::AgentRuntime::new(eval_config, eval_event_tx)
            .with_tools(tool_registry.as_ref().clone())
            .with_roster(agent_registry.clone());
        eval_runtime = eval_runtime.with_sandbox_backend(sandbox_backend_for_eval);
        eval_runtime = eval_runtime.with_plugin_registry(plugin_registry_for_eval);
        if let Some(ref archive) = tool_output_archive {
            eval_runtime = eval_runtime.with_tool_output_archive(archive.clone());
        }
        if let Some(ref metrics) = self.metrics {
            eval_runtime = eval_runtime.with_metrics(metrics.clone());
        }
        // 登记走与常驻池同一条路：注册即建行、发工号。登记不上不拦启动——
        // 判词照样要写出来——但这条日志要说清读面会缺什么。
        if let Err(e) = agent_registry
            .register(&crate::runtime::manager::task_agent_registration(
                &eval_employee.id,
                cog_core::roles::EVALUATOR,
            ))
            .await
        {
            warn!(
                agent_id = %eval_employee.id,
                error = %e,
                "the evaluator could not be registered; its runs still happen, but the roster \
                 carries no row for the person writing the verdicts"
            );
        } else {
            self.eval_employee = Some(eval_employee.id.clone());
        }
        let eval_runtime_arc: Arc<tokio::sync::Mutex<dyn cog_core::AgentRuntime>> =
            Arc::new(tokio::sync::Mutex::new(eval_runtime));
        ctx.publish_service(eval_runtime_arc);
        info!(agent_id = %eval_employee.id, "AgentPlugin eval runtime published");
        *self.eval_event_rx.lock().await = Some(eval_event_rx);

        // ── GlobalAgentManager ──
        let supervisor_state_backend = ctx.require_service::<dyn cog_core::StateBackend>()?;
        let external_skill_registry = ctx.consume_service::<dyn cog_core::ExternalSkillRegistry>();
        // Required, not optional: every worker's per-task token census is
        // written through this gateway, and its absence is invisible at run
        // time — the tasks still complete, the per-task metrics table just
        // stays empty and its readers report a fleet that spends nothing.
        let observability = ctx.require_service::<dyn cog_core::ObservabilityGateway>()?;

        let pool_backend: Arc<dyn cog_core::MessageBackend> = match shared_message_backend.clone() {
            Some(backend) => backend,
            None => {
                return Err(cog_core::SFError::Config(
                    "No message backend available for GlobalAgentManager".into(),
                ));
            }
        };
        // agent_loop / agent_pool 是 cog-agent 自有配置段，自读 cogneva.json。
        let agent_loop_config: cog_core::RuntimeConfig = agent_loop_config.into();
        let mut pool_builder = crate::GlobalAgentManager::new(
            agent_registry.clone(),
            pool_backend,
            supervisor_state_backend.clone(),
        )
        .with_default_runtime_config(agent_loop_config)
        .with_heartbeat_interval_secs(agent_config.heartbeat_interval_secs)
        .with_tools(tool_registry)
        .with_sandbox_backend(sandbox_backend_for_pool)
        // The pool is the only place its own size is known. Without this the
        // roster exists solely as the binding nobody reads, and the only
        // reachable evidence of it is a connection count taken somewhere else.
        .with_roster_observer(crate::observable::global_observable());
        if let Some(ref archive) = tool_output_archive {
            pool_builder = pool_builder.with_tool_output_archive(archive.clone());
        }
        // Workers publish onto the cluster-wide bus (stream plugin) so live
        // observers see every turn/tool call in real time. Without the stream
        // plugin agents keep their private buses — tests and embedded use.
        if let Some(event_tx) =
            ctx.consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
        {
            pool_builder = pool_builder.with_event_bus((*event_tx).clone());
            info!("AgentPlugin workers attached to shared event bus");
        }
        // AgentEnd 持久总线投递：开关打开时 worker 的 AgentEnd 改走事件面
        // （JetStream），经有界内存缓冲+退避重试发布；broadcast 侧的 AgentEnd
        // 由回灌消费者再注入，恰好一次。开关关闭时维持纯 broadcast 行为。
        if mbc.enabled && mbc.events_on_bus {
            match ctx.consume::<cog_core::EventPlanePublisher>() {
                Some(plane_pub) => {
                    let sink = crate::EventBusSink::spawn(
                        plane_pub.0.clone(),
                        mbc.publish_buffer_capacity,
                        mbc.publish_retry_base_delay_ms,
                        ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                    );
                    pool_builder = pool_builder.with_event_bus_sink(sink);
                    info!("AgentPlugin AgentEnd event bus sink attached");
                }
                None => {
                    if ctx.config().system.strict_persistence {
                        return Err(cog_core::SFError::Config(
                            "events_on_bus=true but EventPlanePublisher unavailable (strict_persistence=true)"
                                .into(),
                        ));
                    }
                    warn!(
                        "events_on_bus=true but EventPlanePublisher unavailable; AgentEnd stays on broadcast only"
                    );
                }
            }
        }
        if let Some(ref esr) = external_skill_registry {
            pool_builder = pool_builder.with_external_skill_registry(esr.clone());
        }
        pool_builder = pool_builder.with_observability(observability);
        // 续跑读数走同一份后端：每个 worker 都要报「这次是从日志接着跑的，
        // 还是从头开始」，而它们在池子里建，池子只有这一个后端。
        if let Some(ref metrics) = self.metrics {
            pool_builder = pool_builder.with_metrics(metrics.clone());
        }
        pool_builder = pool_builder.with_wal_backend(wal_backend);
        // 检查点存储：把快照交给 agent 的落盘那一半。缺了它，快照照样返回、
        // 事件照样发、日志照样绿，只有 save 那一步不存在——于是「可续跑的
        // 进度」这个说法在生产里没有任何一份实物，而所有面看起来都装好了。
        // 存储插件一定发布它（Postgres 或文件），所以这里是硬要求，不是可选。
        match ctx.require_service::<dyn cog_core::CheckpointStore>() {
            Ok(store) => {
                pool_builder = pool_builder.with_checkpoint_store(store);
                info!("AgentPlugin agent pool attached to the checkpoint store");
            }
            Err(e) => {
                if ctx.config().system.strict_persistence {
                    return Err(e);
                }
                warn!(error = %e, "checkpoint store unavailable; snapshots are taken but not persisted");
            }
        }
        // 角色 → 技能：技能面里那个 max_iterations 在此之前是个死值——被装进
        // 注册表、被 extractor 校验、被 promoter 改写，运行时却没有任何读点。
        // 这里把它交到建 agent 的那一跳，技能预算才真的生效。
        match ctx.consume::<tokio::sync::RwLock<cog_core::SkillRegistry>>() {
            Some(registry) => {
                pool_builder = pool_builder.with_skill_registry(registry);
                info!("AgentPlugin agent pool attached to the shared skill registry");
            }
            None => {
                warn!(
                    "shared skill registry unavailable; agents keep their configured iteration budget"
                );
            }
        }
        let pool = Arc::new(pool_builder);
        self.pool = Some(pool.clone());
        let pool_dyn: Arc<dyn cog_core::AgentManager> = pool;
        ctx.publish_service(pool_dyn);
        info!("AgentPlugin agent pool published");

        // ── Observable publish (pin-style) ──
        ctx.publish_observable(crate::observable::global_observable());
        info!("AgentPlugin observable published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // 停机信号交给排空闸：内核在每个回合之间看它一眼，关停的那一侧在
        // `shutdown()` 里等运行走到边界。装在这里而不是二进制根，是因为
        // 「谁跑智能体」由这个插件决定，换一个宿主也不能少这一条。
        //
        // 在 `start` 而不是 `init`：停机信号是二进制根在 `init_all` 之后才发布
        // 的（它要在所有插件初始化完才有人等），`init` 里读只会读到没有。
        if let Some(signal) = ctx.consume::<cog_core::ShutdownSignal>() {
            crate::drain::install((*signal).clone());
        }
        // 写判词的那一位在注册表里也要有心跳：没有它，TTL 一过名册就把这个人
        // 读成「退休了」，而判词还在继续写。丢键（TTL 到期、Redis 被清、另一个
        // 副本关停）之后下一次心跳重新登记，而不是一直告警到重启。
        if let (Some(eval_id), Some(registry)) =
            (self.eval_employee.clone(), self.agent_registry.clone())
        {
            let registration = crate::runtime::manager::task_agent_registration(
                &eval_id,
                cog_core::roles::EVALUATOR,
            );
            let body = move |_beat: cog_core::loop_health::Beat| {
                let registry = registry.clone();
                let eval_id = eval_id.clone();
                let registration = registration.clone();
                async move {
                    if registry.heartbeat(&eval_id).await.is_err() {
                        if let Err(e) = registry.register(&registration).await {
                            warn!(
                                agent_id = %eval_id,
                                error = %e,
                                "the evaluator heartbeat could not put it back on the roster"
                            );
                        }
                    }
                }
            };
            let cadence = cog_core::loop_health::Cadence::Periodic(HEARTBEAT_PERIOD);
            match ctx.consume::<cog_core::ShutdownSignal>() {
                Some(signal) => drop(cog_core::loop_health::spawn(
                    EVAL_EMPLOYEE_HEARTBEAT_LOOP,
                    cadence,
                    (*signal).clone(),
                    body,
                )),
                None => drop(cog_core::loop_health::spawn_unstoppable(
                    EVAL_EMPLOYEE_HEARTBEAT_LOOP,
                    cadence,
                    body,
                )),
            }
        }
        if let Some(ref pool) = self.pool {
            if let Some(task_runner) = ctx.consume_service::<dyn cog_core::TaskExecutionCallback>()
            {
                if let Some(llm) = ctx.consume_service::<dyn cog_core::LlmClient>() {
                    let config = ctx.config();
                    let agent_pool = crate::AgentManagerConfig::load()?;
                    let role = agent_pool.worker_role.clone();
                    let instance = crate::identity::agent_instance();
                    // A written-down `worker_count` is the operator's own
                    // cap; zero means they wrote none, and then nothing but
                    // what this container measures bounds the pool. The count
                    // moves while tasks run, and the worker objects are
                    // created by the sizing loop, so they are built from one
                    // spawner rather than inline here.
                    let declared_cap =
                        (agent_pool.worker_count > 0).then_some(agent_pool.worker_count);
                    let concurrency =
                        Arc::new(crate::pool_sizing::PoolConcurrency::new(declared_cap));
                    let spawner = PoolSpawner {
                        pool: pool.clone(),
                        llm: llm.clone(),
                        runner: task_runner.clone(),
                        concurrency: concurrency.clone(),
                        role: role.clone(),
                        instance: instance.clone(),
                        workspace: config.dag_executor.workspace_id.clone(),
                        spawned: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                    };
                    spawner.spawn_one().await;
                    spawn_pool_sizing_loop(concurrency, spawner);
                }
            }
        }
        // ── gRPC control plane heartbeat + command subscription ──
        if let Some(client) = self.lifecycle_client.clone() {
            let agent_id = format!("cogneva-agent-{}", std::process::id());
            let heartbeat_period = HEARTBEAT_PERIOD;
            let hb_client = client.clone();
            let hb_agent_id = agent_id.clone();
            // No stop signal and no exit of its own: ending means this agent
            // stops reporting itself to the control plane.
            drop(cog_core::loop_health::spawn_unstoppable(
                GRPC_CONTROL_HEARTBEAT_LOOP,
                cog_core::loop_health::Cadence::Periodic(heartbeat_period),
                // Rebuilt per attempt, so everything the body consumes is cloned here.
                move |beat| {
                    let client = hb_client.clone();
                    let agent_id = hb_agent_id.clone();
                    async move {
                        let mut interval = tokio::time::interval(heartbeat_period);
                        loop {
                            beat.beat();
                            interval.tick().await;
                            if let Err(e) = client.heartbeat(&agent_id, "active").await {
                                tracing::warn!("gRPC heartbeat failed: {}", e);
                            }
                        }
                    }
                },
            ));
            tokio::spawn(async move {
                match client.subscribe_commands(&agent_id).await {
                    Ok(mut stream) => {
                        while let Some(cmd) = stream.next().await {
                            tracing::info!("Received command via gRPC: {:?}", cmd);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("gRPC subscribe_commands failed: {}", e);
                    }
                }
            });
        }

        // ── gRPC event reporting: low-frequency Unary, high-frequency Client Streaming ──
        if let Some(client) = self.lifecycle_client.clone() {
            let rx = self.eval_event_rx.lock().await.take();
            if let Some(rx) = rx {
                let agent_id = format!("cogneva-agent-{}", std::process::id());
                tokio::spawn(async move {
                    event_upload_task(agent_id, client, rx).await;
                });
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        // 先等手上的运行让路，再注销 worker：反过来的话，正在收尾的那一次运行
        // 会在注册表里找不到自己，而它其实还在跑。
        //
        // 等的是「走到回合边界」，不是「整件活做完」——内核在每轮模型调用之前
        // 看排空闸，走到那里就停，工具结果已经落盘，下一次派发原样接上。
        if crate::drain::requested() {
            let limit = std::time::Duration::from_millis(
                self.drain_timeout_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            );
            // 等多久是这一格的界，等出来的结果是它的读数。两件事在同一段里量：
            // 分开放会让「界够不够」只能靠日志里那句话去猜——而两种结局写下的
            // 是同一句「等着让路」。
            let started = std::time::Instant::now();
            if limit.is_zero() {
                // 没有声明过预算就没有排空可量：这一跑**没有等**，而不是等了零秒。
                // 把它记成一条 0 秒的观测量，会让「预算缺了」与「一次瞬间完成的排空」
                // 在读面上同形，而这两件事要求的动作相反。
                warn!(
                    in_flight = crate::drain::in_flight(),
                    "no shutdown timeout was declared; in-flight runs are left to their journals"
                );
                self.record_drain_undeclared().await;
            } else if crate::drain::wait_drained(limit).await {
                info!("every in-flight run reached a turn boundary before shutdown");
                self.record_drain(started.elapsed().as_secs_f64() * 1000.0, DRAIN_DRAINED, 0)
                    .await;
            } else {
                let left = crate::drain::in_flight();
                warn!(
                    in_flight = left,
                    "shutdown ran out of time with runs still in flight; they resume from their journals"
                );
                self.record_drain(
                    started.elapsed().as_secs_f64() * 1000.0,
                    DRAIN_TIMEOUT,
                    left,
                )
                .await;
            }
        }
        // 注册表是全集群共享的（多副本同名 worker、其他 pod 的 squad 智能体
        // 都在里面），只能注销本进程自己拉起的 worker；其余条目交给 TTL 自然
        // 过期，心跳侧遇到 key 丢失会自行重新注册。
        if let (Some(ref registry), Some(ref pool)) = (&self.agent_registry, &self.pool) {
            for agent_id in pool.worker_ids().await {
                if let Err(e) = registry.deregister(&agent_id).await {
                    warn!("registry deregister({}) failed: {}", agent_id, e);
                }
            }
        }
        // 写判词的那一位同样由这个进程拉起，就同样要在走之前从名册上退下来：
        // 留下一条没有进程服务的心跳，读面要等 TTL 过期才把它读成「不在岗」。
        if let (Some(ref registry), Some(ref eval_id)) = (&self.agent_registry, &self.eval_employee)
        {
            if let Err(e) = registry.deregister(eval_id).await {
                warn!("registry deregister({}) failed: {}", eval_id, e);
            }
        }
        info!("AgentPlugin shutdown");
        Ok(())
    }
}

// The process a pool runs in is now only the worker's *locator*: the name a
// worker registers under comes from `crate::identity`, which deliberately
// leaves the pod name out of it. A pod name changes on every roll, and every
// record keyed by the agent id -- who holds a task, who authored a change,
// which unit owns a face -- would change with it, reading as a roster that
// turned over when in fact the same slot came back in a new pod.

/// Classify an AgentEvent by frequency.
/// High-frequency events are batched via Client Streaming (`upload_events`).
/// Low-frequency critical events are sent immediately via Unary (`report_event`).
fn is_high_frequency_event(event: &cog_core::AgentEvent) -> bool {
    use cog_core::AgentEvent;
    matches!(
        event,
        AgentEvent::TurnStart { .. }
            | AgentEvent::TurnEnd { .. }
            | AgentEvent::MessageStart { .. }
            | AgentEvent::MessageUpdate { .. }
            | AgentEvent::MessageEnd { .. }
            | AgentEvent::ToolExecutionStart { .. }
            | AgentEvent::ToolExecutionUpdate { .. }
            | AgentEvent::ToolExecutionEnd { .. }
            | AgentEvent::ReActStepStart { .. }
            | AgentEvent::ReActStepEnd { .. }
            | AgentEvent::Heartbeat { .. }
            | AgentEvent::CheckpointSaved { .. }
    )
}

/// Background task that routes AgentEvents to the Supervisor.
/// - Low-frequency events → Unary `report_event` (immediate, reliable).
/// - High-frequency events → buffered and sent via Client Streaming `upload_events`.
async fn event_upload_task(
    agent_id: String,
    client: Arc<dyn cog_core::AgentLifecycleClient>,
    mut rx: tokio::sync::mpsc::Receiver<cog_core::AgentEvent>,
) {
    use tokio::time::{interval, Duration};

    // cap: bound — a batching choice; the flush deadline is what bounds the wait
    const BATCH_SIZE: usize = 16;
    // cap: bound — flush deadline for buffered events; the batch size of 16 is the other half of the batching
    const BATCH_TIMEOUT: Duration = Duration::from_millis(100);

    let mut buffer: Vec<cog_core::AgentEvent> = Vec::with_capacity(BATCH_SIZE);
    let mut deadline = interval(BATCH_TIMEOUT);
    deadline.tick().await; // skip immediate first tick

    loop {
        let timeout = deadline.tick();
        tokio::pin!(timeout);

        let event = tokio::select! {
            biased;
            _ = &mut timeout => {
                if !buffer.is_empty() {
                    let batch = std::mem::replace(&mut buffer, Vec::with_capacity(BATCH_SIZE));
                    if let Err(e) = client.upload_events(&agent_id, batch).await {
                        tracing::warn!("gRPC upload_events failed: {}", e);
                    }
                }
                continue;
            }
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(e) => e,
                    None => {
                        // Channel closed — flush remaining batch and exit.
                        if !buffer.is_empty() {
                            let batch = std::mem::replace(&mut buffer, Vec::with_capacity(BATCH_SIZE));
                            if let Err(e) = client.upload_events(&agent_id, batch).await {
                                tracing::warn!("gRPC upload_events on shutdown failed: {}", e);
                            }
                        }
                        break;
                    }
                }
            }
        };

        if is_high_frequency_event(&event) {
            buffer.push(event);
            if buffer.len() >= BATCH_SIZE {
                let batch = std::mem::replace(&mut buffer, Vec::with_capacity(BATCH_SIZE));
                if let Err(e) = client.upload_events(&agent_id, batch).await {
                    tracing::warn!("gRPC upload_events failed: {}", e);
                }
            }
        } else {
            // Low-frequency critical event — send immediately via Unary.
            if let Err(e) = client.report_event(&agent_id, &event).await {
                tracing::warn!("gRPC report_event failed: {}", e);
            }
        }
    }

    tracing::info!("Agent event upload task exited");
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "agent",
    // `skill` is a strong edge, not an optional one: the pool binds each role's
    // skill (its iteration budget above all) at construction time, inside this
    // plugin's `init`. Only `requires` orders `init` in this framework, so a
    // soft edge would leave the bind racing the skill plugin's publish — and
    // losing that race is silent, every role quietly reverting to its
    // configured seed. Declaring it also turns "skill disabled" into a startup
    // diagnostic instead of a budget regression nobody can see.
    requires: &["storage", "extension", "guardrail", "skill"],
    optional_requires: &["stream", "net"],
    factory: || Box::new(AgentPlugin::new()),
};

#[cfg(test)]
mod conversation_reaches_the_worker_tests {
    //! A sentence addressed to a pool worker is answered, not swallowed.
    //!
    //! The workbench's dialogue box reaches a standing worker through its inbox.
    //! The handler that ran there read every `Prompt` as a task, and when the
    //! goal did not parse as one it acknowledged the message and did nothing:
    //! the person asking saw a timeout, the transcript recorded a send that was
    //! never answered, and nothing anywhere said the sentence had been dropped.
    //! This judges the property directly — the answer arrives on the stream the
    //! asker named.

    use super::*;
    use cog_core::{
        AssistantMessageEvent, ChatOptions, ChatResponse, ContentBlock, EventStream, InboxMessage,
        LlmClient, Message, MessageBackend, SFResult, StopReason, Usage,
    };
    use std::time::Duration;

    /// Answers every request with one sentence: this test is about the reply
    /// reaching the asker, not about what a model would have said.
    struct Answering(&'static str);

    #[async_trait::async_trait]
    impl LlmClient for Answering {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, producer) = EventStream::with_capacity(4);
            let text = self.0.to_string();
            tokio::spawn(async move {
                let mut producer = producer;
                let _ = producer
                    .push(AssistantMessageEvent::TextDelta {
                        content_index: 0,
                        delta: text.clone(),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                producer.end(ChatResponse {
                    content: vec![ContentBlock::Text {
                        text,
                        text_signature: None,
                    }],
                    api: "mock".into(),
                    provider: "mock".into(),
                    model: "mock".into(),
                    response_id: None,
                    usage: Usage::default(),
                    stop_reason: StopReason::Stop,
                    error_message: None,
                    upstream_failure: None,
                    retry_after_secs: None,
                    timestamp: chrono::Utc::now(),
                });
            });
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!("this test only drives the chat loop")
        }

        async fn chat(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            Ok(ChatResponse {
                content: vec![ContentBlock::Text {
                    text: self.0.to_string(),
                    text_signature: None,
                }],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// A sentence is not work: reaching the runner with one is the defect this
    /// test exists for, so it fails loudly instead of running something.
    struct NoWork;

    #[async_trait::async_trait]
    impl cog_core::TaskExecutionCallback for NoWork {
        async fn execute_task(&self, _task: cog_core::Task) {
            panic!("a sentence addressed to a worker must not be run as a task");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sentence_reaches_the_worker_and_its_answer_lands_on_the_reply_stream() {
        let backend = Arc::new(cog_test_support::MemoryMessageBackend::new());
        let reply_stream = "worker-replies";
        backend
            .create_consumer_group(reply_stream, "asker")
            .await
            .expect("the asker's reader exists before the reply is published");
        let mut replies = backend
            .subscribe(reply_stream, "asker")
            .await
            .expect("the asker subscribes before asking");

        let config = cog_core::RuntimeConfig {
            agent_id: "worker-one".into(),
            ..Default::default()
        };
        let agent = Arc::new(
            crate::Agent::new(config, Arc::new(Answering("我是 Ada#1234")))
                .with_message_backend(backend.clone()),
        );
        let cell = PoolWorkerCell::default();
        assert!(cell.set(agent).is_ok(), "the cell starts empty");
        let mut handler = pool_worker_handler(
            Arc::new(NoWork),
            Arc::new(crate::pool_sizing::PoolConcurrency::new(None)),
            cell,
        );

        handler(InboxMessage::Prompt {
            goal: serde_json::json!("你叫什么名字？"),
            reply_stream: Some(reply_stream.into()),
            from_agent: None,
        })
        .await
        .expect("the worker answers a sentence without failing");

        let frame = tokio::time::timeout(Duration::from_secs(30), replies.next())
            .await
            .expect("the answer arrives within the window")
            .expect("a frame on the reply stream")
            .expect("the frame reads back");
        let answer: serde_json::Value =
            serde_json::from_slice(&frame.1).expect("the answer is JSON");
        assert!(
            answer.to_string().contains("Ada#1234"),
            "the answer on the reply stream is the worker's own: {answer}"
        );
    }
}

#[cfg(test)]
mod shutdown_drain_readings_tests {
    //! 关停排空的三条结局各自是读得出来的一格。
    //!
    //! 三种结局在日志里是同一句话——「等着让路」——差别只在结尾那一段。读数面
    //! 存在的理由就是把它们分开：等到零、超时还留着几条、以及根本没有预算所以
    //! 没等。第三种尤其不能记成一个 0 秒的观测量，那会让「没排空」与「瞬间排空」
    //! 同形，而两者要求的动作相反。
    //!
    //! 判的是序列名、标签与数值：读者按 `outcome` 选行，标签写错等于这一族永远
    //! 选不中。

    use super::*;
    use crate::recording_metrics::RecordingMetrics;
    use std::sync::Arc;

    fn plugin_with(spy: Arc<RecordingMetrics>) -> AgentPlugin {
        let mut plugin = AgentPlugin::new();
        plugin.metrics = Some(spy);
        plugin
    }

    #[tokio::test]
    async fn a_drained_shutdown_reports_its_duration_and_zero_abandoned() {
        let spy = Arc::new(RecordingMetrics::default());
        plugin_with(spy.clone())
            .record_drain(12_500.0, DRAIN_DRAINED, 0)
            .await;
        let seconds = spy.rows_for(cog_core::metric_names::SHUTDOWN_DRAIN_MILLISECONDS);
        assert_eq!(
            seconds.len(),
            1,
            "one observation per shutdown: {seconds:?}"
        );
        assert_eq!(
            seconds[0].2, 12_500.0,
            "the observed duration is the wait itself, in the unit the bucket ladder covers"
        );
        assert_eq!(
            seconds[0].1.get(crate::LABEL_OUTCOME).map(String::as_str),
            Some(DRAIN_DRAINED),
            "a drain that reached zero must be told apart by its outcome label"
        );
        let abandoned = spy.rows_for(cog_core::metric_names::SHUTDOWN_DRAIN_ABANDONED_TOTAL);
        assert_eq!(
            abandoned.len(),
            1,
            "a zero is a measurement, so it must be written too: {abandoned:?}"
        );
        assert_eq!(abandoned[0].2, 0.0);
    }

    #[tokio::test]
    async fn a_timed_out_drain_reports_the_runs_it_left_in_flight() {
        let spy = Arc::new(RecordingMetrics::default());
        plugin_with(spy.clone())
            .record_drain(30_000.0, DRAIN_TIMEOUT, 3)
            .await;
        let seconds = spy.rows_for(cog_core::metric_names::SHUTDOWN_DRAIN_MILLISECONDS);
        assert_eq!(
            seconds[0].1.get(crate::LABEL_OUTCOME).map(String::as_str),
            Some(DRAIN_TIMEOUT),
            "the two endings share a histogram only because the label separates them"
        );
        let abandoned = spy.rows_for(cog_core::metric_names::SHUTDOWN_DRAIN_ABANDONED_TOTAL);
        assert_eq!(
            abandoned[0].2, 3.0,
            "three runs were handed to their journals; the count is the damage"
        );
    }

    #[tokio::test]
    async fn a_shutdown_with_no_budget_is_not_an_instant_drain() {
        let spy = Arc::new(RecordingMetrics::default());
        plugin_with(spy.clone()).record_drain_undeclared().await;
        assert!(
            spy.rows_for(cog_core::metric_names::SHUTDOWN_DRAIN_MILLISECONDS)
                .is_empty(),
            "a process that never waited must not observe a duration"
        );
        let undeclared = spy.rows_for(cog_core::metric_names::SHUTDOWN_DRAIN_UNDECLARED_TOTAL);
        assert_eq!(
            undeclared.len(),
            1,
            "the missing budget has a reading of its own"
        );
        assert_eq!(undeclared[0].2, 1.0, "one increment per shutdown");
    }

    #[tokio::test]
    async fn no_metrics_backend_still_lets_the_process_exit() {
        // 没有后端时这些调用必须是空操作：关停路径上的一次 panic 会把「读数写不
        // 进去」升级成「进程退不掉」。
        let plugin = AgentPlugin::new();
        plugin.record_drain(1_000.0, DRAIN_DRAINED, 0).await;
        plugin.record_drain_undeclared().await;
    }
}
