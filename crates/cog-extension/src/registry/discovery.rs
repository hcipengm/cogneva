//! Local plugin discovery — scan a directory for `plugin.yaml` manifests.

use cog_core::{PluginManifest, SFResult};
use std::path::Path;

/// Scan `dir` for `plugin.yaml` files and parse them into manifests.
pub async fn discover_local(dir: &str) -> SFResult<Vec<PluginManifest>> {
    let mut manifests = Vec::new();
    let dir_path = Path::new(dir);
    if !dir_path.exists() {
        tracing::warn!("Plugin directory does not exist: {}", dir);
        return Ok(manifests);
    }

    for entry in walkdir::WalkDir::new(dir_path)
        .max_depth(2)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_name() == "plugin.yaml" || entry.file_name() == "plugin.yml" {
            let content = tokio::fs::read_to_string(entry.path())
                .await
                .map_err(|e| cog_core::SFError::Agent(format!("read plugin.yaml failed: {}", e)))?;
            let manifest: PluginManifest = serde_yaml::from_str(&content).map_err(|e| {
                cog_core::SFError::Agent(format!("parse plugin.yaml failed: {}", e))
            })?;
            manifests.push(manifest);
        }
    }

    Ok(manifests)
}
