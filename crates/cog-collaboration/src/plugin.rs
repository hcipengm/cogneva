//! Collaboration plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

/// Collaboration plugin that publishes [`crate::CollaborationExecutor`] as a
/// [`cog_core::TaskExecutor`] via pin-style.
pub struct CollaborationPlugin;

impl CollaborationPlugin {
    /// Create the collaboration plugin.
    pub fn new() -> Self {
        Self
    }
}

impl Default for CollaborationPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for CollaborationPlugin {
    fn name(&self) -> &'static str {
        "collaboration"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        info!("CollaborationPlugin initialized");

        // Observable publish (pin-style)
        let observable = crate::observable::global_observable();
        // The yield and diff-source counters are written through the metrics
        // backend rather than kept in the observable, so the sink has to be
        // taken here: this is the only place the plugin context is in hand, and
        // the writing methods have nowhere else to get it from.
        if let Some(backend) = ctx.consume_service::<dyn cog_core::MetricsBackend>() {
            observable.install_metrics_backend(backend);
        } else {
            warn!(
                "no metrics backend published; the self-evolution yield and \
                 change-diff-source counters will not be recorded"
            );
        }
        ctx.publish_observable(observable);
        info!("CollaborationPlugin observable published");

        let llm_provider = ctx.consume_service::<dyn cog_core::LlmClient>();

        // Read before the LLM gate: whether this process holds a knowledge
        // backend does not depend on whether it holds an LLM, and the absence
        // has to be reported even when no executor is published at all.
        let knowledge_backend = ctx.consume_service::<dyn cog_core::KnowledgeBackend>();

        if let Some(ref llm) = llm_provider {
            let hook_engine = ctx.consume_service::<dyn cog_core::HookEngine>();
            let squad_reflection = ctx.consume_service::<dyn cog_core::SquadReflection>();
            let meta_learning = ctx.consume_service::<dyn cog_core::MetaLearning>();
            let change_sinks = ctx.consume_all_services::<dyn cog_core::ChangeSink>();
            let reflection_engine = ctx.consume_service::<dyn cog_core::ReflectionEngine>();
            let agent_manager = ctx.consume_service::<dyn cog_core::AgentManager>();
            let skill_registry = ctx.consume_service::<dyn cog_core::ExternalSkillRegistry>();
            let state_backend = ctx.consume_service::<dyn cog_core::StateBackend>();

            // self_review / pge / boundary 是 cog-collaboration 自有配置段，
            // 自读 cogneva.json（core config.rs 不聚合单 crate 配置）。
            let mut collab = crate::CollaborationExecutor::new()
                .with_llm_provider(llm.clone())
                .with_boundary_config(crate::BoundaryConfig::load()?);

            if let Some(self_review) = crate::SelfReviewSettings::load()?.to_config() {
                collab = collab.with_self_review(self_review);
            }

            let pge = crate::PgeSettings::load()?;
            if !pge.schemas.is_empty() {
                collab = collab.with_pge_schemas(pge.schemas.clone());
            }
            collab = collab.with_local_repair_max(pge.local_repair_max);

            collab = collab.with_ralph_config(crate::RalphSettings::load()?.to_loop_config());

            if let Some(ref hook) = hook_engine {
                collab = collab.with_hook_engine(hook.clone());
            }
            if let Some(ref reflection) = squad_reflection {
                collab = collab.with_squad_reflection(reflection.clone());
            }
            if let Some(ref meta) = meta_learning {
                collab = collab.with_meta_learning(meta.clone());
            }
            for sink in change_sinks {
                collab = collab.with_change_sink(sink);
            }
            if let Some(ref engine) = reflection_engine {
                collab = collab.with_reflection_engine(engine.clone());
            }
            if let Some(ref manager) = agent_manager {
                collab = collab.with_agent_manager(manager.clone());
            }
            if let Some(ref kb) = knowledge_backend {
                collab = collab.with_knowledge_backend(kb.clone());
            }
            if let Some(ref registry) = skill_registry {
                collab = collab.with_skill_registry(registry.clone());
            }
            if let Some(ref backend) = state_backend {
                collab = collab.with_state_backend(backend.clone());
            }

            ctx.publish_service::<dyn cog_core::TaskExecutor>(Arc::new(collab));
            info!("CollaborationPlugin CollaborationExecutor published");
        } else {
            info!("CollaborationPlugin: no LLM provider available, skipping publish");
        }

        // A process that holds no knowledge backend consults nothing, and the
        // retrieval series shows that as the same empty face a build carrying
        // no such reading shows. Say it in the series' own cells instead: the
        // backend is built only when the wiki layer is reachable, so a process
        // without it holds no layer at all.
        if knowledge_backend.is_none() {
            if let Some(metrics) = ctx.consume_service::<dyn cog_core::MetricsBackend>() {
                cog_core::contract::knowledge::publish_no_knowledge_backend(&metrics).await;
            }
        }

        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("CollaborationPlugin shutdown");
        Ok(())
    }
}

/// Factory function for registration.
pub fn factory() -> Box<dyn cog_core::SystemPlugin> {
    Box::new(CollaborationPlugin::new())
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "collaboration",
    requires: &["llm", "reflection", "agent"],
    optional_requires: &[],
    factory,
};
