//!Tool definitions shared across the workspace.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Handler signature for native tool implementations.
pub type ToolHandler = Arc<
    dyn Fn(
            serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = crate::SFResult<serde_json::Value>> + Send>>
        + Send
        + Sync,
>;

/// Tool implementation variants.
#[derive(Clone)]
pub enum ToolImplementation {
    /// Native Rust closure.
    Native(ToolHandler),
    /// WASM plugin.
    Wasm {
        plugin_id: String,
        export_name: String,
    },
    /// Executed through the configured [`crate::SandboxBackend`] (remote
    /// executor pod in cluster deployments). The operation selects how
    /// arguments map to a [`crate::SandboxPayload`].
    Shell(ShellOp),
}

/// Environment operations routed through the sandbox backend.
/// File operations live here (not as in-process tools) so that an agent's
/// script written via `write_file` is visible to the same executor that
/// later runs it — and so credentials mounted in the orchestrator pod stay
/// out of the agent's reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellOp {
    /// Arguments: `command` (string).
    Command,
    /// Arguments: `path` (string).
    ReadFile,
    /// Arguments: `path`, `content` (strings).
    WriteFile,
}

/// Tool definition.
#[derive(Clone)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value, // JSON Schema
    pub implementation: ToolImplementation,
}

impl std::fmt::Debug for Tool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tool")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("parameters", &self.parameters)
            .finish_non_exhaustive()
    }
}

/// Trait for registries that can register and execute tools.
pub trait ToolRegistry: crate::ToolExecutor + Send + Sync {
    /// Register a single tool, replacing any existing tool with the same name.
    fn register(&self, tool: Tool);
}
