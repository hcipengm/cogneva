//! Local plugin discovery — scan a directory for `plugin.yaml` manifests.

use cog_core::{PluginManifest, SFResult};
use std::path::Path;

/// 走目录的结果：走不动的格子留成错误。
///
/// 这里原先写的是 `filter_map(|e| e.ok())`：哪一格走不动（子目录打不开），
/// 它底下装的插件就从答案里整片消失，而答案仍然说「这些就是装着的插件」，
/// `fetch_by_id` 再把那句折成 `plugin not found: X`。同一个函数对
/// `plugin.yaml` 的内容读不出来是报错的——目录进不去却当作没有，两种失败
/// 被折成了两种不同的答案。
fn walked<E: std::fmt::Display>(
    items: impl IntoIterator<Item = Result<walkdir::DirEntry, E>>,
) -> SFResult<Vec<walkdir::DirEntry>> {
    items
        .into_iter()
        .map(|item| {
            item.map_err(|e| {
                cog_core::SFError::Agent(format!("walk plugin directory failed: {}", e))
            })
        })
        .collect()
}

/// Scan `dir` for `plugin.yaml` files and parse them into manifests.
pub async fn discover_local(dir: &str) -> SFResult<Vec<PluginManifest>> {
    let mut manifests = Vec::new();
    let dir_path = Path::new(dir);
    if !dir_path.exists() {
        tracing::warn!("Plugin directory does not exist: {}", dir);
        return Ok(manifests);
    }

    for entry in walked(walkdir::WalkDir::new(dir_path).max_depth(2))? {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 一格走不动，答案就是错误，不是一张少了几个插件的清单。
    ///
    /// 判据：把 `walked` 换回 `filter_map(|e| e.ok())`，第一条断言会红——
    /// 那一格被跳过，调用方读到的是「没装插件」。
    #[test]
    fn a_directory_that_cannot_be_walked_is_not_an_absent_plugin() {
        let denied = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let error = walked(vec![Err::<walkdir::DirEntry, _>(denied)])
            .expect_err("走不动的一格不许折成「没有插件」");
        assert!(
            error.to_string().contains("walk plugin directory failed"),
            "错误要让读的人看出是走目录失败的：{error}"
        );
    }

    /// 走得动的格子原样交出来：报错只针对走不动的那一格。
    #[test]
    fn a_directory_that_can_be_walked_hands_its_entries_through() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("plugin.yaml"), "name: sample\n").unwrap();
        let entries = walked(walkdir::WalkDir::new(dir.path())).expect("读得到");
        assert!(
            entries.iter().any(|e| e.file_name() == "plugin.yaml"),
            "走得到的 plugin.yaml 不许在映射里丢掉"
        );
    }
}
