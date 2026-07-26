//! WASM plugin loader — instantiate a WASM module via the sandbox backend.

use cog_core::{PluginHandle, PluginManifest, SFResult};
use std::sync::Arc;

/// Load a WASM plugin into the sandbox runtime.
pub async fn load_wasm(
    bytes: &[u8],
    manifest: &PluginManifest,
    sandbox: Arc<dyn cog_core::SandboxBackend>,
) -> SFResult<PluginHandle> {
    // Precompile via sandbox backend.
    let _module_id = sandbox.precompile(bytes).await?;

    Ok(PluginHandle {
        plugin_id: uuid::Uuid::new_v4().to_string(),
        manifest: manifest.clone(),
        loaded_at: chrono::Utc::now(),
    })
}

/// Unload a WASM plugin.
pub async fn unload_wasm(_handle: &PluginHandle) -> SFResult<()> {
    // MVP: no-op. In future this will evict from module cache.
    Ok(())
}
