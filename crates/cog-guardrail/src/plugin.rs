//! Guardrail plugin — implements [`cog_core::SystemPlugin`].

use tracing::info;

/// Guardrail plugin that self-assembles and publishes the composite guardrail.
pub struct GuardrailPlugin {
    initialized: bool,
}

impl GuardrailPlugin {
    /// Create a plugin that will build the guardrail during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for GuardrailPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for GuardrailPlugin {
    fn name(&self) -> &'static str {
        "guardrail"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let audit: std::sync::Arc<dyn cog_core::GuardAuditRecorder> =
            if let Some(recorder) = ctx.consume_service::<dyn cog_core::GuardAuditRecorder>() {
                info!("GuardAuditRecorder consumed from plugin context");
                recorder.clone()
            } else {
                std::sync::Arc::new(crate::InMemoryAuditRecorder::new())
            };

        // 组装在 `build_default_guardrail` 里，这样「和主应用同一套护栏」不是一份
        // 需要各自维护的清单，而是一次调用。
        let guardrail = crate::build_default_guardrail(audit);
        info!("Guardrail initialized (Prompt + Content + PII + Tool + PrincipleShapes)");

        ctx.publish_service(guardrail);
        info!("GuardrailPlugin guardrail published");

        // Observable publish (pin-style)
        ctx.publish_observable(crate::observable::global_observable());
        info!("GuardrailPlugin observable published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("GuardrailPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "guardrail",
    requires: &[],
    optional_requires: &[],
    factory: || Box::new(GuardrailPlugin::new()),
};
