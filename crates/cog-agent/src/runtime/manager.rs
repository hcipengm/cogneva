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
    /// The shared agent-skill registry (`skills/*.json`). Handing each new
    /// agent the skill its role declares is what makes the skill's runtime
    /// parameters — the iteration budget above all — actually take effect:
    /// otherwise the file is written, validated, and evolved by the reflection
    /// loop while nothing at runtime ever reads it.
    skill_registry: Option<Arc<RwLock<cog_core::SkillRegistry>>>,
    event_bus: Option<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>,
    event_bus_sink: Option<crate::EventBusSink>,
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
                context_window_size: 32000,
                skill_cache_ttl_secs: 30,
                think_stall_timeout_secs: 240,
                skill_config: None,
                crew_id: None,
                squad_id: None,
            },
            default_tools: None,
            external_skill_registry: None,
            skill_registry: None,
            event_bus: None,
            event_bus_sink: None,
        }
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
            if let Some(ref tools) = self.default_tools {
                a = a.with_tools(tools.as_ref().clone());
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
        let config = self.runtime_config_for(agent_id, role).await;

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
            if let Some(ref sink) = self.event_bus_sink {
                a = a.with_event_bus_sink(sink.clone());
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
