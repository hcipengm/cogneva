//! Extension registry — plugin discovery, loading, signing, and registration.

pub mod discovery;
pub mod signature;

use cog_core::{PluginHandle, PluginManifest, PluginRegistry, SFResult};
use std::sync::Arc;

/// Concrete implementation of [`PluginRegistry`].
pub struct PluginRegistryImpl {
    local_dir: String,
    _remote_url: Option<String>,
    sandbox: Arc<dyn cog_core::SandboxBackend>,
}

impl PluginRegistryImpl {
    pub fn new(
        local_dir: String,
        remote_url: Option<String>,
        sandbox: Arc<dyn cog_core::SandboxBackend>,
    ) -> Self {
        Self {
            local_dir,
            _remote_url: remote_url,
            sandbox,
        }
    }
}

#[async_trait::async_trait]
impl PluginRegistry for PluginRegistryImpl {
    async fn discover(&self, source: &str) -> SFResult<Vec<PluginManifest>> {
        discovery::discover_local(source).await
    }

    async fn fetch(&self, manifest: &PluginManifest) -> SFResult<Vec<u8>> {
        let path = std::path::Path::new(&self.local_dir).join(&manifest.entry);
        tokio::fs::read(&path)
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("plugin fetch failed: {}", e)))
    }

    async fn load(&self, bytes: &[u8], manifest: &PluginManifest) -> SFResult<PluginHandle> {
        signature::verify(manifest, bytes)?;
        crate::runtime::loader::load_wasm(bytes, manifest, self.sandbox.clone()).await
    }

    async fn unload(&self, handle: &PluginHandle) -> SFResult<()> {
        crate::runtime::loader::unload_wasm(handle).await
    }

    async fn fetch_by_id(&self, plugin_id: &str) -> SFResult<Vec<u8>> {
        let manifests = self.discover(&self.local_dir).await?;
        let manifest = manifests
            .into_iter()
            .find(|m| m.name == plugin_id)
            .ok_or_else(|| cog_core::SFError::Agent(format!("plugin not found: {}", plugin_id)))?;
        self.fetch(&manifest).await
    }
}
