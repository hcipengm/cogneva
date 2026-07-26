use async_trait::async_trait;

/// Manages context-window budget for a single agent.
/// Corresponds to the runtime management of [`AgentWorkingMemory`].
#[async_trait]
pub trait ContextBudgetManager: Send + Sync {
    /// Current context utilization ratio (0.0 ~ 1.0).
    fn utilization(&self) -> f64;

    /// Add content to a named section. If the budget is exceeded, compact automatically.
    async fn add(&mut self, section: &str, content: &str, priority: u8);

    /// Explicitly compact a section with a directive (e.g. "keep architecture decisions, drop tool output").
    async fn compact(&mut self, section: &str, directive: &str);

    /// Render the final prompt text from all sections.
    fn render(&self) -> String;
}
