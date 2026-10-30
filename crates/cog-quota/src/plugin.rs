//! Quota plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Quota plugin that self-assembles and publishes quota and hierarchy managers.
pub struct QuotaPlugin {
    initialized: bool,
}

impl QuotaPlugin {
    /// Create a plugin that will build quota services during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for QuotaPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for QuotaPlugin {
    fn name(&self) -> &'static str {
        "quota"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let store = ctx.require_service::<dyn cog_core::KeyValueStore>()?;

        let quota_manager = Arc::new(crate::QuotaManager::new(store.clone(), 50_000));
        ctx.publish(quota_manager.clone());
        // The manager's decisions, published while initialising: the metrics
        // endpoint takes its snapshot of the observables during init, so a
        // handle published later is held by nobody who renders it.
        ctx.publish_observable(Arc::new(crate::QuotaObservable::new(
            quota_manager.readings(),
        )));
        let quota_dyn: Arc<dyn cog_core::QuotaManager> = quota_manager.clone();
        ctx.publish_service(quota_dyn);
        let workspace_quota_dyn: Arc<dyn cog_core::WorkspaceQuotaSource> = quota_manager;
        ctx.publish_service(workspace_quota_dyn);
        info!("QuotaPlugin quota manager published");

        let limits = crate::QuotaLimits::from_hard(50_000, 0.8);
        let hierarchy_manager = Arc::new(crate::HierarchyManager::new(store.clone(), limits));
        ctx.publish(hierarchy_manager.clone());
        // The reader takes `dyn cog_core::HierarchyManager` through the
        // service table, which is a different pin than the concrete value
        // published above -- the two are stored under different keys and
        // never meet, so the gateway's pre-create 5-level quota check was
        // silently skipped. Published both ways, exactly as the quota
        // manager is just above: the value-typed pin for a concrete
        // reader, the service pin for the trait-object one.
        let hm_service: Arc<dyn cog_core::HierarchyManager> = hierarchy_manager;
        ctx.publish_service(hm_service);
        info!("QuotaPlugin hierarchy manager published");

        // 按任务的预算：同一份存储。只有服务钉——读它的是 LLM 代理路径，那一侧
        // 只认契约（`dyn cog_core::TaskBudgetGate`），没有任何地方按具体类型取它。
        let task_budget: Arc<dyn cog_core::TaskBudgetGate> =
            Arc::new(crate::TaskBudgetManager::new(store));
        ctx.publish_service(task_budget);
        info!("QuotaPlugin task budget gate published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("QuotaPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "quota",
    requires: &["storage"],
    optional_requires: &[],
    factory: || Box::new(QuotaPlugin::new()),
};
