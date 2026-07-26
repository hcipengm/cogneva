//! Guardrail plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
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

        let mut composite = crate::CompositeGuardrail::new(audit);
        composite.add_guard(Box::new(crate::PromptGuard::new(
            crate::PromptGuardConfig::default(),
        )));
        composite.add_guard(Box::new(crate::ContentFilter::new(
            crate::ContentFilterConfig::default(),
        )));
        composite.add_guard(Box::new(crate::PiiDetector::new(
            crate::PiiDetectorConfig::default(),
        )));
        composite.add_guard(Box::new(crate::ToolGuard::new(
            crate::ToolGuardConfig::default(),
        )));
        info!("Guardrail initialized (Prompt + Content + PII + Tool)");

        let guardrail: Arc<dyn cog_core::Guardrail> = Arc::new(composite);
        ctx.publish_service(guardrail);
        info!("GuardrailPlugin guardrail published");

        // Observable publish (pin-style)
        ctx.publish_service(crate::observable::global_observable());
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
    provides: &["Guardrail", "Observable"],
    consumes: &[cog_core::ConsumeSpec {
        type_name: "GuardAuditRecorder",
        required: false,
    }],
    factory: || Box::new(GuardrailPlugin::new()),
};
