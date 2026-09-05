//! Agent plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{debug, info, warn};

/// Agent plugin that self-assembles and publishes hook engine, agent registry,
/// and tool registry.
use futures::StreamExt;

pub struct AgentPlugin {
    initialized: bool,
    agent_registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    pool: Option<Arc<crate::GlobalAgentManager>>,
    lifecycle_client: Option<Arc<dyn cog_core::AgentLifecycleClient>>,
    eval_event_rx: tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<cog_core::AgentEvent>>>,
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
        }
    }
}

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

        // Snapshot config values to drop immutable borrow before publishing.
        let (data_dir, tool_timeout_secs, nats_config, hook_engine_config, _agent_config) = {
            let config = ctx.config();
            (
                config.app.data_dir.clone(),
                config.system.tool_timeout_secs,
                config.dag_executor.nats.clone(),
                config.hook_engine.clone(),
                config.agent.clone(),
            )
        };

        // Consume dependencies published by earlier plugins.
        let shared_message_backend = ctx.consume_service::<dyn cog_core::MessageBackend>();
        let sandbox_backend = ctx
            .consume_service::<dyn cog_core::SandboxBackend>()
            .expect("sandbox backend");
        let guardrail = ctx
            .consume_service::<dyn cog_core::Guardrail>()
            .expect("guardrail");
        let plugin_registry = ctx
            .consume_service::<dyn cog_core::PluginRegistry>()
            .expect("plugin registry");

        // Clone before moving into tool_registry so eval runtime can reuse them.
        let sandbox_backend_for_eval = sandbox_backend.clone();
        let _guardrail_for_eval = guardrail.clone();
        let plugin_registry_for_eval = plugin_registry.clone();
        let hook_archive = ctx.consume_service::<dyn cog_core::HookArchive>();
        let agent_registry = ctx
            .consume_service::<dyn cog_core::AgentRegistry>()
            .expect("agent registry");

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

        // Load persisted hooks from evolution-changes/hooks/ on startup.
        {
            let hook_change_dir =
                std::path::PathBuf::from(format!("{}/evolution-changes/hooks", data_dir));
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
                .with_wasm_timeout(tool_timeout_secs)
                .with_sandbox_backend(sandbox_backend)
                .with_guardrail(guardrail)
                .with_plugin_registry(plugin_registry),
        );
        // Built-in execution tools. Without these the registry advertises zero
        // tool definitions and squads can plan but never act on the
        // environment. search_code stays unregistered until it really
        // searches — an always-empty tool teaches agents to distrust tools.
        cog_core::ToolRegistry::register(&*tool_registry, crate::tools::builtins::read_file());
        cog_core::ToolRegistry::register(&*tool_registry, crate::tools::builtins::write_file());
        cog_core::ToolRegistry::register(&*tool_registry, crate::tools::builtins::run_command());
        if let Some(http_client) = ctx.consume_service::<dyn cog_core::HttpClient>() {
            cog_core::ToolRegistry::register(
                &*tool_registry,
                crate::tools::builtins::http_request(http_client),
            );
        } else {
            warn!("HttpClient unavailable; http_request tool not registered");
        }
        info!(
            tools = ?tool_registry.names(),
            "AgentPlugin built-in tools registered"
        );
        ctx.publish(tool_registry.clone());
        let tool_registry_dyn: Arc<dyn cog_core::ToolRegistry> = tool_registry.clone();
        ctx.publish_service(tool_registry_dyn);
        info!("AgentPlugin tool registry published");

        // ── Eval AgentRuntime ──
        let (eval_event_tx, eval_event_rx) =
            tokio::sync::mpsc::channel::<cog_core::AgentEvent>(128);
        let eval_config = cog_core::RuntimeConfig {
            agent_id: "eval-agent".into(),
            role: "evaluator".into(),
            max_iterations: 5,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        };
        let mut eval_runtime = crate::AgentRuntime::new(eval_config, eval_event_tx)
            .with_tools(tool_registry.as_ref().clone());
        eval_runtime = eval_runtime.with_sandbox_backend(sandbox_backend_for_eval);
        eval_runtime = eval_runtime.with_plugin_registry(plugin_registry_for_eval);
        let eval_runtime_arc: Arc<tokio::sync::Mutex<dyn cog_core::AgentRuntime>> =
            Arc::new(tokio::sync::Mutex::new(eval_runtime));
        ctx.publish_service(eval_runtime_arc);
        info!("AgentPlugin eval runtime published");
        *self.eval_event_rx.lock().await = Some(eval_event_rx);

        // ── GlobalAgentManager ──
        let supervisor_state_backend = ctx
            .consume_service::<dyn cog_core::StateBackend>()
            .expect("state backend");
        let external_skill_registry = ctx.consume_service::<dyn cog_core::ExternalSkillRegistry>();

        let pool_backend: Arc<dyn cog_core::MessageBackend> = match shared_message_backend.clone() {
            Some(backend) => backend,
            None => {
                return Err(cog_core::SFError::Config(
                    "No message backend available for GlobalAgentManager".into(),
                ));
            }
        };
        // agent_loop / agent_pool 是 cog-agent 自有配置段，自读 cogneva.json。
        let agent_loop_config: cog_core::RuntimeConfig = crate::AgentLoopConfig::load()?.into();
        let mut pool_builder = crate::GlobalAgentManager::new(
            agent_registry.clone(),
            pool_backend,
            supervisor_state_backend.clone(),
        )
        .with_default_runtime_config(agent_loop_config)
        .with_tools(tool_registry);
        // Workers publish onto the cluster-wide bus (stream plugin) so live
        // observers see every turn/tool call in real time. Without the stream
        // plugin agents keep their private buses — tests and embedded use.
        if let Some(event_tx) =
            ctx.consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
        {
            pool_builder = pool_builder.with_event_bus((*event_tx).clone());
            info!("AgentPlugin workers attached to shared event bus");
        }
        if let Some(ref esr) = external_skill_registry {
            pool_builder = pool_builder.with_external_skill_registry(esr.clone());
        }
        let pool = Arc::new(pool_builder);
        self.pool = Some(pool.clone());
        let pool_dyn: Arc<dyn cog_core::AgentManager> = pool;
        ctx.publish_service(pool_dyn);
        info!("AgentPlugin agent pool published");

        // ── Observable publish (pin-style) ──
        ctx.publish_service(crate::observable::global_observable());
        info!("AgentPlugin observable published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if let Some(ref pool) = self.pool {
            if let Some(task_runner) = ctx.consume_service::<dyn cog_core::TaskExecutionCallback>()
            {
                if let Some(llm) = ctx.consume_service::<dyn cog_core::LlmClient>() {
                    let config = ctx.config();
                    let agent_pool = crate::AgentManagerConfig::load()?;
                    let role = agent_pool.worker_role.clone();
                    for i in 0..agent_pool.worker_count {
                        let agent_id = format!("cogneva-worker-{}", i);
                        let registration = cog_core::AgentRegistration::new(
                            agent_id.clone(),
                            "localhost",
                            "127.0.0.1",
                            &agent_pool.worker_role,
                            &config.dag_executor.workspace_id,
                            vec!["plan".into(), "generate".into(), "evaluate".into()],
                            cog_core::ResourceInfo::default(),
                        );
                        let runner = task_runner.clone();
                        if let Err(e) = pool
                            .spawn_worker(
                                agent_id.clone(),
                                role.clone(),
                                llm.clone(),
                                registration,
                                move |msg| {
                                    let runner = runner.clone();
                                    async move {
                                        if let cog_core::InboxMessage::Prompt { goal, .. } = msg {
                                            if let Ok(task) =
                                                serde_json::from_value::<cog_core::Task>(goal)
                                            {
                                                runner.execute_task(task).await;
                                            }
                                        }
                                        Ok(())
                                    }
                                },
                            )
                            .await
                        {
                            warn!("Failed to spawn agent pool worker {}: {}", agent_id, e);
                        } else {
                            info!("Agent pool worker {} spawned (role={:?})", agent_id, role);
                        }
                    }
                }
            }
        }
        // ── gRPC control plane heartbeat + command subscription ──
        if let Some(client) = self.lifecycle_client.clone() {
            let agent_id = format!("cogneva-agent-{}", std::process::id());
            tokio::spawn({
                let client = client.clone();
                let agent_id = agent_id.clone();
                async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                    loop {
                        interval.tick().await;
                        if let Err(e) = client.heartbeat(&agent_id, "active").await {
                            tracing::warn!("gRPC heartbeat failed: {}", e);
                        }
                    }
                }
            });
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
        info!("AgentPlugin shutdown");
        Ok(())
    }
}

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

    const BATCH_SIZE: usize = 16;
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
    requires: &["storage", "extension", "guardrail"],
    optional_requires: &["stream", "skill", "net"],
    provides: &[
        "HookEngine",
        "ToolRegistry",
        "AgentManager",
        "AgentRuntime",
        "Observable",
    ],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "MessageBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "SandboxBackend",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "Guardrail",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "PluginRegistry",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "HookArchive",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "AgentRegistry",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "StateBackend",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "ExternalSkillRegistry",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "LlmClient",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "TaskExecutionCallback",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "HttpClient",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "Sender<AgentEvent>",
            required: false,
        },
    ],
    factory: || Box::new(AgentPlugin::new()),
};
