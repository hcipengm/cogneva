use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::Agent;
use cog_core::{
    AgentRegistration, AgentRegistry, InboxMessage, MessageBackend, SFResult, StateBackend,
};

/// Handle to a spawned worker agent.
#[derive(Clone)]
pub struct WorkerHandle {
    pub agent_id: String,
    pub role: String,
    pub capabilities: Vec<String>,
    pub agent: Arc<Agent>,
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
    external_skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    event_bus: Option<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>,
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
            default_runtime_config: cog_core::RuntimeConfig {
                agent_id: String::new(),
                role: "planner".into(),
                max_iterations: 10,
                context_window_size: 4000,
                skill_cache_ttl_secs: 30,
                skill_config: None,
                crew_id: None,
                squad_id: None,
            },
            default_tools: None,
            external_skill_registry: None,
            event_bus: None,
        }
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

    /// Set the default [`ToolRegistry`] shared by all spawned workers.
    pub fn with_tools(mut self, tools: Arc<crate::ToolRegistry>) -> Self {
        self.default_tools = Some(tools);
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
        let mut config = self.default_runtime_config.clone();
        config.agent_id = agent_id.clone();
        config.role = role.clone();

        let capabilities = registration.capabilities.clone();

        let agent = {
            let mut a = Agent::new(config, llm)
                .with_registry(self.registry.clone())
                .with_registration(registration)
                .with_message_backend(self.message_backend.clone())
                .with_state_backend(self.state_backend.clone());
            if let Some(ref tools) = self.default_tools {
                a = a.with_tools(tools.as_ref().clone());
            }
            if let Some(ref esr) = self.external_skill_registry {
                a = a.with_external_skill_registry(esr.clone());
            }
            if let Some(ref bus) = self.event_bus {
                a = a.with_event_bus(bus.clone());
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
        };

        let mut workers = self.workers.write().await;
        workers.push(handle.clone());
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
    pub async fn list_workers(&self) -> Vec<WorkerHandle> {
        self.workers.read().await.clone()
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

#[async_trait::async_trait]
impl cog_core::AgentManager for GlobalAgentManager {
    async fn create_agent(
        &self,
        agent_id: &str,
        role: &str,
        llm: std::sync::Arc<dyn cog_core::LlmClient>,
    ) -> cog_core::SFResult<std::sync::Arc<dyn cog_core::Agent>> {
        let mut config = self.default_runtime_config.clone();
        config.agent_id = agent_id.into();
        config.role = role.into();

        let capabilities = vec![role.to_string()];
        let registration = cog_core::AgentRegistration::new(
            agent_id,
            "cog-agent",
            "127.0.0.1",
            role.to_string(),
            "default",
            capabilities.clone(),
            cog_core::ResourceInfo::default(),
        );

        let agent = {
            let mut a = Agent::new(config, llm)
                .with_registry(self.registry.clone())
                .with_registration(registration)
                .with_message_backend(self.message_backend.clone())
                .with_state_backend(self.state_backend.clone());
            if let Some(ref tools) = self.default_tools {
                a = a.with_tools(tools.as_ref().clone());
            }
            if let Some(ref esr) = self.external_skill_registry {
                a = a.with_external_skill_registry(esr.clone());
            }
            if let Some(ref bus) = self.event_bus {
                a = a.with_event_bus(bus.clone());
            }
            a
        };

        agent.start().await;
        let backend_id = agent_id.to_string();
        agent
            .start_consumer(move |msg| {
                let bid = backend_id.clone();
                async move {
                    tracing::debug!(agent_id = %bid, "received inbox message: {:?}", msg);
                    Ok(())
                }
            })
            .await?;

        let handle = WorkerHandle {
            agent_id: agent_id.into(),
            role: role.to_string(),
            capabilities,
            agent: std::sync::Arc::new(agent),
        };

        let arc_agent: std::sync::Arc<dyn cog_core::Agent> = handle.agent.clone();
        let mut workers = self.workers.write().await;
        workers.push(handle);
        Ok(arc_agent)
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
            if w.agent_id == agent_id {
                let agent: std::sync::Arc<dyn cog_core::Agent> = w.agent.clone();
                return Ok(Some(agent));
            }
        }
        Ok(None)
    }
}
