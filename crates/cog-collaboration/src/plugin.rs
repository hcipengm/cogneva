//! Collaboration plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Collaboration plugin that publishes [`CollaborationExecutor`] as a
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
        ctx.publish_service(crate::observable::global_observable());
        info!("CollaborationPlugin observable published");

        let llm_provider = ctx.consume_service::<dyn cog_core::LlmClient>();

        if let Some(ref llm) = llm_provider {
            let hook_engine = ctx.consume_service::<dyn cog_core::HookEngine>();
            let squad_reflection = ctx.consume_service::<dyn cog_core::SquadReflection>();
            let meta_learning = ctx.consume_service::<dyn cog_core::MetaLearning>();
            let patch_sinks = ctx.consume_all_services::<dyn cog_core::PatchSink>();
            let reflection_engine = ctx.consume_service::<dyn cog_core::ReflectionEngine>();
            let agent_manager = ctx.consume_service::<dyn cog_core::AgentManager>();
            let knowledge_backend = ctx.consume_service::<dyn cog_core::KnowledgeBackend>();
            let skill_registry = ctx.consume_service::<dyn cog_core::ExternalSkillRegistry>();

            let mut collab = crate::CollaborationExecutor::new()
                .with_llm_provider(llm.clone())
                .with_boundary_config(ctx.config().boundary.clone());

            if let Some(self_review) = ctx.config().self_review.to_config() {
                collab = collab.with_self_review(self_review);
            }

            if !ctx.config().pge.schemas.is_empty() {
                collab = collab.with_pge_schemas(ctx.config().pge.schemas.clone());
            }

            if let Some(ref hook) = hook_engine {
                collab = collab.with_hook_engine(hook.clone());
            }
            if let Some(ref reflection) = squad_reflection {
                collab = collab.with_squad_reflection(reflection.clone());
            }
            if let Some(ref meta) = meta_learning {
                collab = collab.with_meta_learning(meta.clone());
            }
            for sink in patch_sinks {
                collab = collab.with_patch_sink(sink);
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

            ctx.publish_service::<dyn cog_core::TaskExecutor>(Arc::new(collab));
            info!("CollaborationPlugin CollaborationExecutor published");
        } else {
            info!("CollaborationPlugin: no LLM provider available, skipping publish");
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
    provides: &["TaskExecutor", "Observable"],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "LlmClient",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "AgentManager",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "HookEngine",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "SquadReflection",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MetaLearning",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "PatchSink",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "ReflectionEngine",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "KnowledgeBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "ExternalSkillRegistry",
            required: false,
        },
    ],
    factory,
};
