use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::Agent;
use cog_core::{
    AgentRegistration, AgentRegistry, InboxMessage, MessageBackend, SFResult, StateBackend,
};

/// Build the runtime config a new agent of `role` starts from.
///
/// Installing the role's skill here is the single point where the skill's
/// declared parameters reach the runtime: `AgentRuntime::new` applies the
/// skill's iteration budget on top of the config value, and the per-role
/// calibration in the observable moves it from there. Without this hop the
/// skill file is written, validated, and evolved while nothing reads it.
///
/// A role with no registered skill keeps the config value: the mapping is
/// absent for that role, which is not the same statement as "this role wants
/// the default" — it is simply no evidence, and inventing one would change
/// behaviour for roles nobody has described yet.
async fn runtime_config_for(
    default_config: &cog_core::RuntimeConfig,
    skill_registry: Option<&RwLock<cog_core::SkillRegistry>>,
    agent_id: &str,
    role: &str,
) -> cog_core::RuntimeConfig {
    let mut config = default_config.clone();
    config.agent_id = agent_id.into();
    config.role = role.into();
    if let Some(registry) = skill_registry {
        let registry = registry.read().await;
        if let Some(skill) = registry.skill_for_role(role) {
            config.skill_config = Some(skill.clone());
        }
    }
    config
}

/// Handle to a spawned worker agent.
#[derive(Clone)]
pub struct WorkerHandle {
    pub agent_id: String,
    pub role: String,
    pub capabilities: Vec<String>,
    pub agent: Arc<Agent>,
    /// When this object was put on the roster.
    ///
    /// Kept so the age of the oldest agent can be read out. The count alone
    /// cannot tell a roster that is working from one that never lets anything
    /// go: both grow, and the difference is only visible as age.
    created_at: std::time::Instant,
    /// The task this agent works for, when it has one.
    ///
    /// A field rather than something read back out of the name. Names are
    /// derived per role and per branch (`{task}-branch-{n}-{role}`), so parsing
    /// the task back out of them means the roster's idea of "which task owns
    /// this" changes shape with every call site that names an agent. The call
    /// sites already have the task in hand; this is what keeps them from having
    /// to spell it into a string and hope the reader agrees.
    ///
    /// `None` is the process-lifetime agent, which no task will ever retire.
    task_id: Option<String>,
}

/// Global agent manager for production multi-agent parallelism.
/// Manages a fleet of `Agent` workers that:
/// - Auto-register with the global `AgentRegistry` on startup
/// - Listen for inbox messages via `AgentConsumer`
/// - Share task state via `ContextBoard` (StateBackend)
/// - Are discoverable by the Supervisor through the registry
pub struct GlobalAgentManager {
    registry: Arc<dyn AgentRegistry>,
    message_backend: Arc<dyn MessageBackend>,
    state_backend: Arc<dyn StateBackend>,
    workers: RwLock<Vec<WorkerHandle>>,
    round_robin: Mutex<usize>,
    default_runtime_config: cog_core::RuntimeConfig,
    default_tools: Option<Arc<crate::ToolRegistry>>,
    /// The sandbox a worker's own reads of its checkout run in.
    ///
    /// A different handle from the one the tool registry carries. A worker's
    /// tools reach the sandbox through the registry, so a worker created
    /// without this one still runs commands and still edits its tree; what it
    /// loses is every read the agent itself makes of that tree, and the loss is
    /// silent — a backend that is absent and a checkout nobody changed both
    /// answer "no change here".
    sandbox_backend: Option<Arc<dyn cog_core::SandboxBackend>>,
    external_skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    /// The shared agent-skill registry (`skills/*.json`). Handing each new
    /// agent the skill its role declares is what makes the skill's runtime
    /// parameters — the iteration budget above all — actually take effect:
    /// otherwise the file is written, validated, and evolved by the reflection
    /// loop while nothing at runtime ever reads it.
    skill_registry: Option<Arc<RwLock<cog_core::SkillRegistry>>>,
    event_bus: Option<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>,
    event_bus_sink: Option<crate::EventBusSink>,
    /// Durable per-task token census sink handed to every spawned worker.
    observability: Option<Arc<dyn cog_core::ObservabilityGateway>>,
    /// Where a worker's checkpoint is persisted when something asks for a
    /// snapshot.
    ///
    /// An agent with no store still answers `snapshot`: it builds the
    /// checkpoint, emits the event, and returns it. Only the save is skipped,
    /// and it is skipped silently. Whoever takes that snapshot as evidence of a
    /// resumable task is then holding an id that resolves to nothing — the
    /// pointer is written, the read comes back empty, and the run starts over
    /// exactly as if nothing had ever been checkpointed.
    checkpoint_store: Option<Arc<dyn cog_core::CheckpointStore>>,
    /// The durable event log each agent writes its events to.
    ///
    /// The store above persists the checkpoint; this is what makes the events
    /// after it replayable. Without a backend every agent runs `wal: None`, its
    /// checkpoint records `event_offset: 0`, and a restore replays nothing --
    /// the snapshot loads and everything that happened after it is gone, while
    /// every surface reports the checkpoint was written.
    wal_backend: Option<Arc<dyn cog_core::WalBackend>>,
    /// Whether the pool has already said, out loud, that no backend was
    /// attached. One line per pool: the wiring that supplies the backend is a
    /// single call in the plugin, and a call that can be deleted without a word
    /// is the same failure one level up from the two `with_wal` call sites. A
    /// line per agent would bury it under its own repetition.
    wal_missing_reported: AtomicBool,
    /// Where a spawned worker's loop archives a tool output too large for the
    /// context window. Shared by every worker, like the observability gateway.
    tool_output_archive: Option<Arc<dyn crate::archive::ToolOutputArchive>>,
    /// How often every worker renews its registration.
    ///
    /// Read from the `agent` section at startup and handed to each spawned
    /// agent. `None` means nobody set it, and the agent keeps its own default
    /// rather than being handed a number that came from somewhere else: the
    /// configuration type's `Default` is derived, so "the config surface's
    /// default" is zero, and a zero interval makes the heartbeat task panic on
    /// its first tick -- an agent that never renews its registration, reported
    /// as a panic inside a task nobody joins.
    heartbeat_interval_secs: Option<u64>,
    /// Where the roster is published for the scrape to read.
    ///
    /// The roster is the only place the pool's own size is known, and until it
    /// is published the size exists only as a connection count elsewhere --
    /// which is a different quantity, since a connection can outlive the object
    /// that opened it and an object can exist with no connection at all. A
    /// reader comparing the two needs both, so this is not optional in
    /// production wiring even though the field is.
    roster_observer: Option<Arc<crate::observable::AgentObservable>>,
}

impl GlobalAgentManager {
    pub fn new(
        registry: Arc<dyn AgentRegistry>,
        message_backend: Arc<dyn MessageBackend>,
        state_backend: Arc<dyn StateBackend>,
    ) -> Self {
        Self {
            registry,
            message_backend,
            state_backend,
            workers: RwLock::new(Vec::new()),
            round_robin: Mutex::new(0),
            // The rest comes from the one default rather than a second copy of
            // the numbers: this fallback is replaced from the delivered section
            // at build time, and a literal here would take over silently the day
            // that replacement stops being wired.
            default_runtime_config: cog_core::RuntimeConfig {
                agent_id: String::new(),
                ..cog_core::RuntimeConfig::default()
            },
            default_tools: None,
            sandbox_backend: None,
            external_skill_registry: None,
            skill_registry: None,
            event_bus: None,
            event_bus_sink: None,
            observability: None,
            checkpoint_store: None,
            wal_backend: None,
            wal_missing_reported: AtomicBool::new(false),
            tool_output_archive: None,
            heartbeat_interval_secs: None,
            roster_observer: None,
        }
    }

    /// Publish the roster to `observer` whenever it changes.
    pub fn with_roster_observer(
        mut self,
        observer: Arc<crate::observable::AgentObservable>,
    ) -> Self {
        self.roster_observer = Some(observer);
        self
    }

    /// Hand the current roster to the observer, if one is attached.
    ///
    /// Called while the caller still holds the write lock, so the list it sees
    /// is the roster at that instant rather than a second read that could have
    /// moved on. The observer's lock is an ordinary `Mutex` and is never held
    /// across an await, so this cannot deadlock against the roster's own lock.
    fn publish_roster(&self, workers: &[WorkerHandle]) {
        if let Some(ref observer) = self.roster_observer {
            observer.publish_roster(
                workers
                    .iter()
                    .map(|w| (w.role.clone(), w.created_at, w.task_id.is_some()))
                    .collect::<Vec<_>>(),
            );
        }
    }

    /// Set how often every worker renews its registration, from the `agent`
    /// section of the delivered configuration.
    pub fn with_heartbeat_interval_secs(mut self, secs: u64) -> Self {
        self.heartbeat_interval_secs = Some(secs);
        self
    }

    /// The runtime config a newly spawned agent of `role` starts from: the
    /// manager's default, plus that role's own skill when one is registered.
    async fn runtime_config_for(&self, agent_id: &str, role: &str) -> cog_core::RuntimeConfig {
        runtime_config_for(
            &self.default_runtime_config,
            self.skill_registry.as_deref(),
            agent_id,
            role,
        )
        .await
    }

    /// Route AgentEnd events from every spawned worker onto the persistent
    /// event bus (JetStream event plane when enabled). Live events still go
    /// to the broadcast bus unless a sink is set, in which case AgentEnd
    /// reaches broadcast consumers via bus re-injection exactly once.
    pub fn with_event_bus_sink(mut self, sink: crate::EventBusSink) -> Self {
        self.event_bus_sink = Some(sink);
        self
    }

    /// Publish every spawned worker onto the shared cluster-wide event bus so
    /// live observers see turns, streaming output, and tool executions in real
    /// time. Without this each agent broadcasts on a private channel nobody
    /// outside the agent can reach.
    pub fn with_event_bus(
        mut self,
        tx: tokio::sync::broadcast::Sender<cog_core::AgentEvent>,
    ) -> Self {
        self.event_bus = Some(tx);
        self
    }

    /// Override the default runtime config used when spawning workers.
    pub fn with_default_runtime_config(mut self, config: cog_core::RuntimeConfig) -> Self {
        self.default_runtime_config = config;
        self
    }

    /// Set the default [`crate::ToolRegistry`] shared by all spawned workers.
    pub fn with_tools(mut self, tools: Arc<crate::ToolRegistry>) -> Self {
        self.default_tools = Some(tools);
        self
    }

    /// Give every spawned worker the sandbox its own checkout reads run in.
    ///
    /// The tool registry carries its own handle to the same backend, and that
    /// one is what a worker's tools use. This is the other handle: the one an
    /// agent reads its tree through when something asks what the run changed.
    pub fn with_sandbox_backend(mut self, backend: Arc<dyn cog_core::SandboxBackend>) -> Self {
        self.sandbox_backend = Some(backend);
        self
    }

    /// Set the external skill registry for injecting available_skills into worker system prompts.
    pub fn with_external_skill_registry(
        mut self,
        registry: Arc<dyn cog_core::ExternalSkillRegistry>,
    ) -> Self {
        self.external_skill_registry = Some(registry);
        self
    }

    /// Set the shared agent-skill registry newly spawned agents take their
    /// role's skill from.
    pub fn with_skill_registry(mut self, registry: Arc<RwLock<cog_core::SkillRegistry>>) -> Self {
        self.skill_registry = Some(registry);
        self
    }

    /// Give every spawned worker the gateway its per-task token census is
    /// written to.
    pub fn with_observability(mut self, gateway: Arc<dyn cog_core::ObservabilityGateway>) -> Self {
        self.observability = Some(gateway);
        self
    }

    /// Give every spawned worker the store its snapshots are persisted to.
    pub fn with_checkpoint_store(mut self, store: Arc<dyn cog_core::CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Attach the durable event log each agent writes to.
    ///
    /// One backend serves every worker; each agent gets its own log keyed by
    /// its id, so a restart replays that agent's events and not another's.
    pub fn with_wal_backend(mut self, backend: Arc<dyn cog_core::WalBackend>) -> Self {
        self.wal_backend = Some(backend);
        self
    }

    /// Open `agent_id`'s event log, or `None` when no backend is attached.
    ///
    /// A log that will not open is reported and the agent starts without one:
    /// the append path already treats a WAL failure as non-fatal, and refusing
    /// to spawn would turn a missing history into an outage. The log line names
    /// the consequence, because "no WAL" and "an empty WAL" look the same to
    /// every reader downstream.
    ///
    /// No backend at all reaches the same consequence from a different cause and
    /// is reported the same way, once per pool at the first agent that asks. The
    /// distinction is the whole point of the line: an empty WAL is this agent's
    /// history, a missing backend is this process's wiring, and only the second
    /// is fixed by editing a line of setup.
    async fn wal_for(&self, agent_id: impl AsRef<str>) -> Option<Arc<crate::wal::AgentWal>> {
        let agent_id = agent_id.as_ref();
        let Some(backend) = self.wal_backend.as_ref() else {
            if !self.wal_missing_reported.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "no WAL backend attached to this pool: every agent's checkpoint records event_offset 0, so a restore replays nothing after the snapshot"
                );
            }
            return None;
        };
        match crate::wal::AgentWal::new(backend.clone(), agent_id).await {
            Ok(wal) => Some(Arc::new(wal)),
            Err(e) => {
                tracing::warn!(
                    agent_id,
                    "WAL unavailable; this agent's checkpoints will replay no events: {e}"
                );
                None
            }
        }
    }

    /// Give every spawned worker the surface that archives an oversized tool
    /// output. One handle for the whole pool: the reading it carries is keyed by
    /// tool, not by worker, so a second copy would only split the count.
    pub fn with_tool_output_archive(
        mut self,
        archive: Arc<dyn crate::archive::ToolOutputArchive>,
    ) -> Self {
        self.tool_output_archive = Some(archive);
        self
    }

    /// Spawn a new worker agent, register it globally, and start its inbox consumer.
    /// # Arguments
    /// * `agent_id` — unique worker identifier
    /// * `role` — agent role (planner, generator, evaluator)
    /// * `llm` — shared LLM provider
    /// * `registration` — registry payload (capabilities, resources, etc.)
    /// * `handler` — closure invoked for each [`InboxMessage`]
    pub async fn spawn_worker<F, Fut>(
        &self,
        agent_id: impl Into<String>,
        role: String,
        llm: Arc<dyn cog_core::LlmClient>,
        registration: AgentRegistration,
        handler: F,
    ) -> SFResult<WorkerHandle>
    where
        F: FnMut(InboxMessage) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = SFResult<()>> + Send,
    {
        let agent_id = agent_id.into();
        let config = self.runtime_config_for(&agent_id, &role).await;

        let capabilities = registration.capabilities.clone();

        let agent = {
            let mut a = Agent::new(config, llm)
                .with_registry(self.registry.clone())
                .with_registration(registration)
                .with_message_backend(self.message_backend.clone())
                .with_state_backend(self.state_backend.clone());
            if let Some(secs) = self.heartbeat_interval_secs {
                a = a.with_heartbeat_interval(secs);
            }
            if let Some(wal) = self.wal_for(&agent_id).await {
                a = a.with_wal(wal);
            }
            if let Some(ref tools) = self.default_tools {
                a = a.with_tools(tools.as_ref().clone());
            }
            if let Some(ref sb) = self.sandbox_backend {
                a = a.with_sandbox_backend(sb.clone());
            }
            if let Some(ref esr) = self.external_skill_registry {
                a = a.with_external_skill_registry(esr.clone());
            }
            if let Some(ref bus) = self.event_bus {
                a = a.with_event_bus(bus.clone());
            }
            if let Some(ref sink) = self.event_bus_sink {
                a = a.with_event_bus_sink(sink.clone());
            }
            if let Some(ref ob) = self.observability {
                a = a.with_observability(ob.clone());
            }
            if let Some(ref cp) = self.checkpoint_store {
                a = a.with_checkpoint_store(cp.clone());
            }
            if let Some(ref ar) = self.tool_output_archive {
                a = a.with_tool_output_archive(ar.clone());
            }
            a
        };

        agent.start().await;
        agent.start_consumer(handler).await?;

        let handle = WorkerHandle {
            agent_id: agent_id.clone(),
            role: role.to_string(),
            capabilities,
            agent: Arc::new(agent),
            created_at: std::time::Instant::now(),
            // A worker spawned with a caller-supplied handler is not tied to a
            // task: nothing here knows one, and inventing an owner would make
            // an unrelated task's terminal state retire it.
            task_id: None,
        };

        let mut workers = self.workers.write().await;
        workers.push(handle.clone());
        self.publish_roster(&workers);
        Ok(handle)
    }

    /// Agent ids of workers spawned by this manager instance. Used on
    /// shutdown to deregister only our own entries — the registry is shared
    /// cluster-wide, so list-and-flush would wipe other replicas' agents.
    pub async fn worker_ids(&self) -> Vec<String> {
        self.workers
            .read()
            .await
            .iter()
            .map(|w| w.agent_id.clone())
            .collect()
    }

    /// Dispatch an [`InboxMessage`] to a worker using round-robin selection.
    pub async fn dispatch(&self, message: InboxMessage) -> SFResult<()> {
        let workers = self.workers.read().await;
        if workers.is_empty() {
            return Err(cog_core::SFError::Agent(
                "No workers available in pool".into(),
            ));
        }

        let idx = {
            let mut rr = self.round_robin.lock().await;
            let i = *rr % workers.len();
            *rr = (*rr + 1) % workers.len();
            i
        };

        let target = &workers[idx];
        Agent::send_message(&target.agent_id, message, self.message_backend.as_ref()).await
    }

    /// Return a snapshot of currently live workers.
    ///
    /// Retired objects are left out even while they are still in the roster:
    /// the roster is also the resume index, and a retired agent handed back to
    /// a resume pass is an id that resolves to nothing.
    pub async fn list_workers(&self) -> Vec<WorkerHandle> {
        self.workers
            .read()
            .await
            .iter()
            .filter(|w| !w.agent.is_retired())
            .cloned()
            .collect()
    }

    /// Retire one agent by name, returning whether it was there to retire.
    ///
    /// The retirement itself happens before the roster is touched: an object
    /// that is already stopped but still listed is one a caller can still reach
    /// and hand messages to, while an object that is gone from the list but
    /// still reading is one nobody can find.
    pub async fn retire_agent(&self, agent_id: &str) -> bool {
        let target = {
            let workers = self.workers.read().await;
            workers
                .iter()
                .find(|w| w.agent_id == agent_id)
                .map(|w| (w.agent.clone(), w.role.clone(), w.task_id.is_some()))
        };
        let Some((agent, role, task_scoped)) = target else {
            return false;
        };
        let report = agent.retire().await;
        self.finish_retirement(&agent, &role, task_scoped, report)
            .await;
        true
    }

    /// Retire every agent this manager created for `task_id`.
    pub async fn retire_task_agents(&self, task_id: &str) -> usize {
        let targets: Vec<(Arc<Agent>, String)> = {
            let workers = self.workers.read().await;
            workers
                .iter()
                .filter(|w| w.task_id.as_deref() == Some(task_id))
                .map(|w| (w.agent.clone(), w.role.clone()))
                .collect()
        };
        // Retired together, not one after another. Each retirement may wait out
        // the whole budget for a consumer that is inside a handler, so a task
        // with many branch agents would otherwise hold the terminal path for
        // the object count times that budget. They share nothing -- each stops
        // its own inbox and joins its own handle -- so the whole task costs one
        // budget rather than one per object.
        let reports =
            futures::future::join_all(targets.iter().map(|(agent, _)| agent.retire())).await;
        let mut retired = 0;
        for ((agent, role), report) in targets.into_iter().zip(reports) {
            if report.performed {
                retired += 1;
            }
            self.finish_retirement(&agent, &role, true, report).await;
        }
        retired
    }

    /// Take a retired object out of the roster and count what happened.
    ///
    /// Removed by identity rather than by name: a concurrent create may already
    /// have pushed a fresh object under the same name, and removing by name
    /// would take that one with it. The counting happens before the early
    /// return so that a repeat call is still recorded as `performed: false`
    /// rather than silently vanished.
    async fn finish_retirement(
        &self,
        agent: &Arc<Agent>,
        role: &str,
        task_scoped: bool,
        report: crate::agent::RetireReport,
    ) {
        if let Some(observer) = &self.roster_observer {
            observer.record_retirement(role, task_scoped, report);
        }
        if !report.performed {
            return;
        }
        let mut workers = self.workers.write().await;
        workers.retain(|w| !Arc::ptr_eq(&w.agent, agent));
        self.publish_roster(&workers);
    }

    /// Gracefully shutdown all workers: abort consumers and deregister from registry.
    pub async fn shutdown(&self) -> SFResult<()> {
        let workers = self.workers.read().await;
        for w in workers.iter() {
            let _ = w.agent.abort().await;
        }
        Ok(())
    }
}

/// How a task's role agent (planner / generator / evaluator) registers.
///
/// The host is the process this one runs in, not the literal `cog-agent`: a
/// roster whose every row claims to run on a host by that name cannot say
/// which pod holds which squad, so a stuck squad in one pod and a quiet pod
/// read the same. The pod name is available as `COGNEVA_POD_NAME` or
/// `HOSTNAME`, and a plain host run honestly reports that host.
fn task_agent_registration(agent_id: &str, role: &str) -> cog_core::AgentRegistration {
    cog_core::AgentRegistration::new(
        agent_id,
        crate::identity::agent_instance(),
        "127.0.0.1",
        role.to_string(),
        "default",
        vec![role.to_string()],
        cog_core::ResourceInfo::default(),
    )
}

#[async_trait::async_trait]
impl cog_core::AgentManager for GlobalAgentManager {
    async fn create_agent(
        &self,
        agent_id: &str,
        role: &str,
        llm: std::sync::Arc<dyn cog_core::LlmClient>,
        task_id: Option<&str>,
    ) -> cog_core::SFResult<std::sync::Arc<dyn cog_core::Agent>> {
        // 同一个名字只对应一个活的对象。
        //
        // 名字是按 (task, role) 推出来的（`agent_id_for`），所以同一件任务被
        // 重跑一次就会再来这里一次——而重跑在这套系统里是常态：清扫器把
        // `Scheduled` 到期的任务放回队列、任务失败后回投，走的都是同一条路。
        // 每次重跑都新建一个对象，就变成同一个名字下挂着若干个互不认识的
        // 消费者：它们订阅同一条 inbox，各自占一条到总线的连接，注册表里
        // 后一个覆盖前一个，而谁也不会被回收（管理器对它们持有强引用）。
        // 实测 2026-10-18：稳态 ~8800 条连接、10.8 GiB 常驻。
        //
        // 所以创建是幂等的：名字已经活着就把它交回去，重跑接上的是同一个对象。
        if let Some(existing) = self.get_agent(agent_id).await? {
            tracing::debug!(
                agent_id,
                "agent already exists under this name; handing back the live one"
            );
            return Ok(existing);
        }

        let mut config = self.runtime_config_for(agent_id, role).await;
        // 这个 agent 是在替哪件 DAG 任务干活，交给运行时记下来：账本与清点按它
        // 归属，运行自己的 id 只用于这一次执行的归档与沙箱命名。调用方传进来的
        // 就是这个任务的 id（`create_role_agent` 与 roundtable 的角色都是这么建
        // 的），所以这里不做推断。
        config.owner_task_id = task_id.map(str::to_string);

        let capabilities = vec![role.to_string()];
        let registration = task_agent_registration(agent_id, role);

        let agent = {
            let mut a = Agent::new(config, llm)
                .with_registry(self.registry.clone())
                .with_registration(registration)
                .with_message_backend(self.message_backend.clone())
                .with_state_backend(self.state_backend.clone());
            if let Some(secs) = self.heartbeat_interval_secs {
                a = a.with_heartbeat_interval(secs);
            }
            if let Some(wal) = self.wal_for(&agent_id).await {
                a = a.with_wal(wal);
            }
            if let Some(ref tools) = self.default_tools {
                a = a.with_tools(tools.as_ref().clone());
            }
            if let Some(ref sb) = self.sandbox_backend {
                a = a.with_sandbox_backend(sb.clone());
            }
            if let Some(ref esr) = self.external_skill_registry {
                a = a.with_external_skill_registry(esr.clone());
            }
            if let Some(ref bus) = self.event_bus {
                a = a.with_event_bus(bus.clone());
            }
            if let Some(ref sink) = self.event_bus_sink {
                a = a.with_event_bus_sink(sink.clone());
            }
            if let Some(ref ob) = self.observability {
                a = a.with_observability(ob.clone());
            }
            if let Some(ref cp) = self.checkpoint_store {
                a = a.with_checkpoint_store(cp.clone());
            }
            if let Some(ref ar) = self.tool_output_archive {
                a = a.with_tool_output_archive(ar.clone());
            }
            a
        };

        agent.start().await;
        // The consumer used to log each delivered message and ack it. That made
        // the inbox look healthy while every command an agent received was
        // thrown away. Hand the message to the agent's own command path
        // instead: a handler that fails leaves it pending for redelivery.
        let agent = std::sync::Arc::new(agent);
        let consumer_agent = agent.clone();
        agent
            .start_consumer(move |msg| {
                let agent = consumer_agent.clone();
                async move { agent.handle_inbox(msg).await }
            })
            .await?;

        let handle = WorkerHandle {
            agent_id: agent_id.into(),
            role: role.to_string(),
            capabilities,
            agent: agent.clone(),
            created_at: std::time::Instant::now(),
            task_id: task_id.map(str::to_string),
        };

        let arc_agent: std::sync::Arc<dyn cog_core::Agent> = handle.agent.clone();
        let mut workers = self.workers.write().await;
        // 上面的检查在锁外（建对象要 await），两个同名创建可能同时走到这里：
        // 后到的那个把自己刚建好的交出去，交出去的对象随即被丢弃，其消费者
        // 在 `Drop` 里中止。名字下仍然只有一个。
        if let Some(existing) = workers.iter().find(|w| w.agent_id == agent_id) {
            if !existing.agent.is_retired() {
                return Ok(existing.agent.clone());
            }
            // 名下那一个已经退役了。它不是「同一个名字还活着」，而是上一次
            // 生命留下的空壳：把它交出去，调用方拿到的消费者收得下消息、
            // 答不出一条。所以摘掉它，让这次新建的接上这个名字。
            workers.retain(|w| w.agent_id != agent_id);
        }
        workers.push(handle);
        self.publish_roster(&workers);
        Ok(arc_agent)
    }

    async fn retire_task_agents(&self, task_id: &str) -> cog_core::SFResult<usize> {
        Ok(self.retire_task_agents(task_id).await)
    }

    async fn dispatch(&self, msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        self.dispatch(msg).await
    }

    async fn list_workers(&self) -> cog_core::SFResult<Vec<cog_core::WorkerInfo>> {
        let workers = self.list_workers().await;
        Ok(workers
            .into_iter()
            .map(|w| cog_core::WorkerInfo {
                agent_id: w.agent_id,
                role: w.role,
                capabilities: w.capabilities,
            })
            .collect())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        self.shutdown().await
    }

    async fn get_agent(
        &self,
        agent_id: &str,
    ) -> cog_core::SFResult<Option<std::sync::Arc<dyn cog_core::Agent>>> {
        let workers = self.workers.read().await;
        for w in workers.iter() {
            // A retired object is not "the agent under this name" any more: it
            // accepts messages and answers none, so handing it back is worse
            // than finding nothing and creating a live one.
            if w.agent_id == agent_id && !w.agent.is_retired() {
                let agent: std::sync::Arc<dyn cog_core::Agent> = w.agent.clone();
                return Ok(Some(agent));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::SkillConfig;

    fn registry_with(skills: &str) -> Arc<RwLock<cog_core::SkillRegistry>> {
        let mut registry = cog_core::SkillRegistry::new();
        registry.load_skills_from_json(skills).expect("skills load");
        Arc::new(RwLock::new(registry))
    }

    fn skill_json(role: &str, max_iterations: u32) -> String {
        format!(
            r#"[{{"skill_id":"{role}","name":"{role}","tools":[],"max_iterations":{max_iterations},"role_type":"{role}"}}]"#
        )
    }

    /// 技能面的预算要真的走到运行时头上。这条链路曾经只差最后一跳——技能
    /// 被装上、被校验、被改写，运行时却没有任何读点，于是"自进化能调预算"
    /// 看起来是真的而实际是死的。这里量的是那一跳。
    #[tokio::test]
    async fn the_roles_skill_reaches_the_runtime_config() {
        let registry = registry_with(&skill_json("generator", 17));
        let config = runtime_config_for(
            &cog_core::RuntimeConfig::default(),
            Some(registry.as_ref()),
            "squad-a-generator",
            "generator",
        )
        .await;

        let skill: &SkillConfig = config.skill_config.as_ref().expect("skill installed");
        assert_eq!(skill.skill_id, "generator");
        assert_eq!(skill.max_iterations, 17);
        assert_eq!(config.role, "generator");
        assert_eq!(config.agent_id, "squad-a-generator");
    }

    /// 技能是**按角色**取的：别的角色的技能不能顺手落到这个角色头上。
    #[tokio::test]
    async fn another_roles_skill_is_not_installed() {
        let registry = registry_with(&skill_json("generator", 17));
        let config = runtime_config_for(
            &cog_core::RuntimeConfig::default(),
            Some(registry.as_ref()),
            "squad-a-evaluator",
            "evaluator",
        )
        .await;

        assert!(
            config.skill_config.is_none(),
            "an evaluator must not run under the generator's skill"
        );
    }

    /// 没有技能注册表时保持配置面现值：这条路径上的角色没有人描述过，
    /// 凭空给它安一个技能就是拿"没人写过"当成"要这个值"。
    #[tokio::test]
    async fn without_a_registry_the_configured_value_stands() {
        let default_config = cog_core::RuntimeConfig {
            max_iterations: 23,
            ..Default::default()
        };
        let config = runtime_config_for(&default_config, None, "worker-0", "planner").await;

        assert!(config.skill_config.is_none());
        assert_eq!(config.max_iterations, 23);
    }

    /// 那一跳的另一半：装上的技能预算真的成为运行时的预算。角色名只有本用例
    /// 用——预算还跟着该角色自己跑出来的读数走，借用别的用例写过的角色名会把
    /// 断言变成对执行顺序的断言。
    /// 名册上"这个 agent 跑在哪里"必须答得出真话。原来每个任务角色的 agent
    /// 都登记成 `cog-agent`——那不是任何一台机器的名字，于是「某个 pod 里的
    /// squad 卡住了」和「那个 pod 闲著」在名册上读成同一行。
    #[test]
    fn a_task_agent_registers_under_the_host_it_runs_on() {
        // 环境变量是进程级的：与 identity 的用例抢同一把锁，避免读到别人设的值。
        let _guard = crate::identity::tests::env_lock();
        let previous = std::env::var(cog_core::env::POD_NAME).ok();
        std::env::set_var("COGNEVA_POD_NAME", "cogneva-647bb49dcd-4cttc");

        let reg = task_agent_registration("squad:task-1-planner", "planner");

        assert_eq!(reg.hostname, "cogneva-647bb49dcd-4cttc");
        assert_eq!(reg.role, "planner");
        assert_eq!(reg.capabilities, vec!["planner".to_string()]);

        match previous {
            Some(v) => std::env::set_var("COGNEVA_POD_NAME", v),
            None => std::env::remove_var("COGNEVA_POD_NAME"),
        }
    }

    #[tokio::test]
    async fn the_installed_skill_sets_the_runtime_budget() {
        let role = "skill-chain-budget-test";
        let registry = registry_with(&skill_json(role, 17));
        let config = runtime_config_for(
            &cog_core::RuntimeConfig::default(),
            Some(registry.as_ref()),
            "skill-chain-budget-test-agent",
            role,
        )
        .await;

        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let runtime = crate::AgentRuntime::new(config, tx);
        assert_eq!(runtime.config().max_iterations, 17);
    }
}
