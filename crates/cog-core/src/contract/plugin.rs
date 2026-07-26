//!Plugin system contracts — `cog-core` Domain Kernel.
//!cog-plugin is the **external ecosystem** plugin system:
//!third-party developers inject new tools / skills / backends without
//!recompiling the main monorepo.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ==========================================================================
// Plugin manifest & capabilities
// ==========================================================================

/// Plugin manifest (similar to npm package.json or VS Code extension manifest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub author: String,
    /// Capabilities declared: which tools / skills / backends this plugin provides.
    pub capabilities: Vec<PluginCapability>,
    /// Entry file (WASM module path).
    pub entry: String,
    /// Public-key signature for integrity verification.
    pub signature: String,
    /// Sandbox policy override (inherits system policy by default).
    pub sandbox: Option<crate::sandbox::ResourceLimits>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginCapability {
    Tool {
        name: String,
        description: String,
        parameters: serde_json::Value,
    },
    Skill {
        skill_id: String,
    },
    Backend {
        backend_type: String,
    },
}

// ==========================================================================
// PluginRegistry trait
// ==========================================================================

#[async_trait]
pub trait PluginRegistry: Send + Sync {
    /// Discover available plugins from a local directory or remote registry.
    async fn discover(&self, source: &str) -> crate::SFResult<Vec<PluginManifest>>;
    /// Download and verify signature.
    async fn fetch(&self, manifest: &PluginManifest) -> crate::SFResult<Vec<u8>>;
    /// Load plugin into runtime (WASM instantiation).
    async fn load(&self, bytes: &[u8], manifest: &PluginManifest) -> crate::SFResult<PluginHandle>;
    /// Unload plugin.
    async fn unload(&self, handle: &PluginHandle) -> crate::SFResult<()>;
    /// Fetch a plugin's WASM bytes by its identifier.
    /// Implementations should cache discovered manifests for efficiency.
    async fn fetch_by_id(&self, plugin_id: &str) -> crate::SFResult<Vec<u8>>;
}

/// Handle to a loaded plugin.
#[derive(Debug, Clone)]
pub struct PluginHandle {
    pub plugin_id: String,
    pub manifest: PluginManifest,
    pub loaded_at: chrono::DateTime<chrono::Utc>,
}

// ==========================================================================
// ToolExecutor trait
// ==========================================================================

/// Abstract tool executor — implemented by `cog-agent::ToolRegistry` and
/// any other runtime that can actually invoke a tool by name.
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    /// Execute a tool with the given JSON arguments.
    async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> crate::SFResult<serde_json::Value>;
}
