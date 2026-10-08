//! Skill plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

/// Skill plugin that provides the skill registry and external skill registry.
pub struct SkillPlugin {
    registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>>,
    /// The filesystem-backed registry, held across `init` and `start`: the
    /// hot-reload watcher starts in `start`, where the metrics backend is
    /// already published, and it needs this handle to attach it.
    external: Option<Arc<crate::SkillRegistryImpl>>,
    initialized: bool,
}

impl SkillPlugin {
    /// Create a plugin that will build a fresh registry during `init`.
    pub fn new() -> Self {
        Self {
            registry: None,
            external: None,
            initialized: false,
        }
    }

    /// Create a plugin that wraps an existing registry.
    pub fn from_registry(registry: Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>) -> Self {
        Self {
            registry: Some(registry),
            external: None,
            initialized: false,
        }
    }
}

impl Default for SkillPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for SkillPlugin {
    fn name(&self) -> &'static str {
        "skill"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let registry = if let Some(ref reg) = self.registry {
            reg.clone()
        } else {
            Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()))
        };
        self.registry = Some(registry.clone());

        let directories = vec![
            std::path::PathBuf::from("/opt/cogneva/skills"),
            std::path::PathBuf::from("/var/lib/cogneva/skills"),
            dirs::home_dir()
                .map(|h| h.join(".cogneva/skills"))
                .unwrap_or_else(|| std::path::PathBuf::from("~/.cogneva/skills")),
        ];

        // 目标分解（action planner 的 decompose_goal）消费这份共享注册表；
        // 把扁平 JSON 技能定义（planner/generator/evaluator 等）装进来，
        // 否则注册表为空，自主执行流在调 LLM 前就失败。
        {
            let mut reg = registry.write().await;
            for dir in &directories {
                if !dir.is_dir() {
                    continue;
                }
                match reg.load_skills_from_dir(dir) {
                    Ok(()) => {
                        info!(dir = %dir.display(), count = reg.get_all().len(), "skills loaded")
                    }
                    Err(e) => warn!(dir = %dir.display(), error = %e, "failed to load skills"),
                }
            }
        }

        ctx.publish(registry);
        info!("SkillPlugin skill registry published");

        // Build external skill registry from configuration.
        let skill_config = crate::SkillConfig {
            directories,
            hot_reload_interval_secs: ctx.config().system.skill_hot_reload_interval_secs,
        };

        let impl_registry = crate::SkillRegistryImpl::new(skill_config);
        if let Err(e) = impl_registry.load_all().await {
            warn!(error = %e, "Failed to load some skills");
        }
        self.external = Some(impl_registry.clone());
        let external_registry: Arc<dyn cog_core::ExternalSkillRegistry> = impl_registry;
        ctx.publish_service(external_registry);
        info!("SkillPlugin external skill registry published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // The hot-reload watcher is started here rather than in `init` so it can
        // carry its per-round outcome to the metric store from its first round.
        // The metrics backend is published by the storage plugin, which
        // initialises after this one, so resolving it in `init` would find
        // nothing; every plugin's `init` has completed by the time `start` runs.
        let Some(registry) = self.external.clone() else {
            return Ok(());
        };
        let metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
        if metrics.is_none() {
            warn!(
                "MetricsBackend not published; skill hot-reload round outcomes \
                 will not be reported"
            );
        }
        registry.set_metrics(metrics);
        let _watcher = registry.spawn_watcher();
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("SkillPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "skill",
    requires: &[],
    optional_requires: &[],
    factory: || Box::new(SkillPlugin::new()),
};
