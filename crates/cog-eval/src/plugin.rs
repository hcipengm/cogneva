//! Eval plugin — implements [`cog_core::SystemPlugin`].
//! This plugin depends **only on `cog-core`**.  It self-assembles an `EvalRunner`
//! by consuming `AgentRuntime` + `LlmClient` from the plugin context, then
//! wraps it as [`dyn cog_core::EvalService`] during `start()`.

use std::sync::Arc;
use tracing::info;

use crate::service::EvalServiceImpl;

/// Eval plugin that self-assembles and publishes [`dyn cog_core::EvalService`].
pub struct EvalPlugin {
    service: std::sync::Mutex<Option<Arc<dyn cog_core::EvalService>>>,
}

impl EvalPlugin {
    /// Create a plugin that will assemble eval services during `init()`/`start()`.
    pub fn new() -> Self {
        Self {
            service: std::sync::Mutex::new(None),
        }
    }
}

impl Default for EvalPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for EvalPlugin {
    fn name(&self) -> &'static str {
        "eval"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let agent_runtime = ctx.consume_service::<tokio::sync::Mutex<dyn cog_core::AgentRuntime>>();
        let llm = ctx.consume_service::<dyn cog_core::LlmClient>();

        if let (Some(agent_runtime), Some(llm)) = (agent_runtime, llm) {
            let runner = crate::EvalRunner::new(
                agent_runtime,
                llm,
                crate::RunnerConfig {
                    max_concurrency: 4,
                    timeout_seconds: 120,
                    retry_count: 1,
                    judge_enabled: true,
                },
            );
            ctx.publish(Arc::new(tokio::sync::Mutex::new(runner)));
            info!("EvalRunner assembled and published");
        } else {
            info!("EvalPlugin: missing AgentRuntime or LlmClient, eval disabled");
        }
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if let Some(runner) = ctx.consume::<tokio::sync::Mutex<crate::EvalRunner>>() {
            // Collect all Observables from pin-style (consume_all_services)
            let observables = ctx.consume_all_services::<dyn cog_core::Observable>();
            info!("EvalPlugin collected {} Observable(s)", observables.len());

            let service = EvalServiceImpl::new(runner, observables);
            let svc = Arc::new(service) as Arc<dyn cog_core::EvalService>;
            ctx.publish_service(svc.clone());
            *self.service.lock().unwrap() = Some(svc);
            info!("EvalService published");
        } else {
            info!("EvalPlugin: no EvalRunner available (LLM likely disabled)");
        }
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("EvalPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "eval",
    requires: &["agent", "llm"],
    optional_requires: &[],
    provides: &["EvalService"],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "AgentRuntime",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "LlmClient",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "Observable",
            required: false,
        },
    ],
    factory: || Box::new(EvalPlugin::new()),
};
