//! Skill registry implementation — in-memory cache with filesystem backing.

use async_trait::async_trait;
use cog_core::{DownloadOptions, ExternalSkillRegistry, SFResult, SkillDef, SkillMetadata};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::discovery::discover_all;
use crate::loader::load_skill;

/// Configuration for skill directories.
#[derive(Debug, Clone)]
pub struct SkillConfig {
    pub directories: Vec<PathBuf>,
    /// Hot-reload poll interval in seconds.
    pub hot_reload_interval_secs: u64,
}

impl Default for SkillConfig {
    fn default() -> Self {
        Self {
            directories: vec![
                PathBuf::from("/opt/cogneva/skills"),
                PathBuf::from("/var/lib/cogneva/skills"),
                PathBuf::from("~/.cogneva/skills"),
            ],
            hot_reload_interval_secs: 30,
        }
    }
}

/// In-memory skill registry backed by filesystem directories.
pub struct SkillRegistryImpl {
    config: SkillConfig,
    cache: RwLock<HashMap<String, CachedSkill>>,
}

struct CachedSkill {
    def: SkillDef,
    path: PathBuf,
    /// Last modified time of SKILL.md (for hot-reload detection).
    mtime: std::time::SystemTime,
}

impl SkillRegistryImpl {
    pub fn new(config: SkillConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            cache: RwLock::new(HashMap::new()),
        })
    }

    /// Scan all configured directories and load skills into cache.
    pub async fn load_all(&self) -> SFResult<()> {
        let discovered = discover_all(&self.config.directories).await?;
        let mut cache = self.cache.write().await;
        cache.clear();

        for (path, skill_id) in discovered {
            match load_skill(&path).await {
                Ok(def) => {
                    let mtime = Self::skill_mtime(&path)
                        .await
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                    tracing::info!(
                        skill_id = %skill_id,
                        path = %path.display(),
                        "Loaded skill"
                    );
                    cache.insert(skill_id, CachedSkill { def, path, mtime });
                }
                Err(e) => {
                    tracing::warn!(
                        skill_id = %skill_id,
                        path = %path.display(),
                        error = %e,
                        "Failed to load skill"
                    );
                }
            }
        }

        Ok(())
    }

    /// Get the modification time of a skill's SKILL.md file.
    async fn skill_mtime(path: &std::path::Path) -> SFResult<std::time::SystemTime> {
        let md = tokio::fs::metadata(path.join("SKILL.md"))
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("stat SKILL.md failed: {}", e)))?;
        md.modified()
            .map_err(|e| cog_core::SFError::Agent(format!("mtime failed: {}", e)))
    }

    /// Spawn a background task that watches skill directories for changes
    /// and hot-reloads skills every 30 seconds.
    pub fn spawn_watcher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let registry = self.clone();
        tokio::spawn(async move {
            let interval_secs = registry.config.hot_reload_interval_secs;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(e) = registry.check_and_reload().await {
                    tracing::warn!("Skill hot-reload check failed: {}", e);
                }
            }
        })
    }

    /// Compare disk state with in-memory cache and reload changed skills.
    async fn check_and_reload(&self) -> SFResult<()> {
        let discovered = discover_all(&self.config.directories).await?;
        let mut cache = self.cache.write().await;

        // Build a set of discovered skill IDs for eviction detection.
        let discovered_ids: std::collections::HashSet<String> =
            discovered.iter().map(|(_, id)| id.clone()).collect();

        // Evict deleted skills.
        let evict_ids: Vec<String> = cache
            .keys()
            .filter(|k| !discovered_ids.contains(*k))
            .cloned()
            .collect();
        for id in evict_ids {
            tracing::info!(skill_id = %id, "Evicted deleted skill");
            cache.remove(&id);
        }

        // Load new or modified skills.
        for (path, skill_id) in discovered {
            let need_reload = match cache.get(&skill_id) {
                Some(cached) => {
                    let current_mtime = Self::skill_mtime(&path)
                        .await
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                    current_mtime > cached.mtime
                }
                None => true,
            };

            if need_reload {
                match load_skill(&path).await {
                    Ok(def) => {
                        let mtime = Self::skill_mtime(&path)
                            .await
                            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                        tracing::info!(skill_id = %skill_id, path = %path.display(), "Hot-reloaded skill");
                        cache.insert(skill_id, CachedSkill { def, path, mtime });
                    }
                    Err(e) => {
                        tracing::warn!(skill_id = %skill_id, path = %path.display(), error = %e, "Failed to hot-reload skill");
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ExternalSkillRegistry for SkillRegistryImpl {
    async fn resolve_metadata(&self, skill_id: &str) -> SFResult<SkillMetadata> {
        let cache = self.cache.read().await;
        let cached = cache
            .get(skill_id)
            .ok_or_else(|| cog_core::SFError::Agent(format!("skill not found: {}", skill_id)))?;
        Ok(cached.def.metadata.clone())
    }

    async fn resolve(&self, skill_id: &str) -> SFResult<SkillDef> {
        let cache = self.cache.read().await;
        let cached = cache
            .get(skill_id)
            .ok_or_else(|| cog_core::SFError::Agent(format!("skill not found: {}", skill_id)))?;
        Ok(SkillDef {
            metadata: cached.def.metadata.clone(),
            skill_md: cached.def.skill_md.clone(),
            frontmatter: cached.def.frontmatter.clone(),
        })
    }

    async fn list(&self) -> SFResult<Vec<SkillMetadata>> {
        let cache = self.cache.read().await;
        Ok(cache.values().map(|c| c.def.metadata.clone()).collect())
    }

    async fn load_resource(&self, skill_id: &str, resource_path: &str) -> SFResult<String> {
        let cache = self.cache.read().await;
        let cached = cache
            .get(skill_id)
            .ok_or_else(|| cog_core::SFError::Agent(format!("skill not found: {}", skill_id)))?;

        let full_path = cached.path.join(resource_path);
        // Security: prevent directory traversal.
        let canonical_skill = std::fs::canonicalize(&cached.path)
            .map_err(|e| cog_core::SFError::Agent(format!("canonicalize skill dir: {}", e)))?;
        let canonical_resource = std::fs::canonicalize(&full_path)
            .map_err(|e| cog_core::SFError::Agent(format!("canonicalize resource: {}", e)))?;
        if !canonical_resource.starts_with(&canonical_skill) {
            return Err(cog_core::SFError::Agent(
                "resource path escapes skill directory".into(),
            ));
        }

        tokio::fs::read_to_string(&canonical_resource)
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("read resource failed: {}", e)))
    }

    async fn list_scripts(&self, skill_id: &str) -> SFResult<Vec<String>> {
        let cache = self.cache.read().await;
        let cached = cache
            .get(skill_id)
            .ok_or_else(|| cog_core::SFError::Agent(format!("skill not found: {}", skill_id)))?;

        let scripts_dir = cached.path.join("scripts");
        if !scripts_dir.exists() {
            return Ok(Vec::new());
        }

        let mut scripts = Vec::new();
        let mut entries = tokio::fs::read_dir(&scripts_dir)
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("read scripts dir: {}", e)))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("read dir entry: {}", e)))?
        {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Skip hidden files and Python cache.
            if name_str.starts_with('.') || name_str == "__pycache__" {
                continue;
            }
            if entry
                .file_type()
                .await
                .map_err(|e| cog_core::SFError::Agent(format!("file type: {}", e)))?
                .is_file()
            {
                scripts.push(name_str.to_string());
            }
        }

        Ok(scripts)
    }

    async fn script_path(&self, skill_id: &str, script_name: &str) -> SFResult<PathBuf> {
        let cache = self.cache.read().await;
        let cached = cache
            .get(skill_id)
            .ok_or_else(|| cog_core::SFError::Agent(format!("skill not found: {}", skill_id)))?;

        let script_path = cached.path.join("scripts").join(script_name);
        if !script_path.exists() {
            return Err(cog_core::SFError::Agent(format!(
                "script not found: {} in skill {}",
                script_name, skill_id
            )));
        }
        Ok(script_path)
    }

    async fn download(&self, source: &str, _opts: DownloadOptions) -> SFResult<()> {
        let dest_dir = self
            .config
            .directories
            .first()
            .cloned()
            .unwrap_or_else(|| std::env::temp_dir().join("cogneva-skills"));
        tokio::fs::create_dir_all(&dest_dir)
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("create skills dir failed: {}", e)))?;

        if source.ends_with(".git")
            || source.starts_with("git@")
            || source.contains("github.com/")
            || source.contains("gitlab.com/")
        {
            // Git clone path
            let repo_name = source
                .rsplit('/')
                .next()
                .and_then(|n| n.strip_suffix(".git").or(Some(n)))
                .unwrap_or("downloaded-skill");
            let target = dest_dir.join(repo_name);
            if target.exists() {
                return Err(cog_core::SFError::Agent(format!(
                    "skill directory already exists: {}",
                    target.display()
                )));
            }
            let output = tokio::process::Command::new("git")
                .args(["clone", source, &target.to_string_lossy()])
                .output()
                .await
                .map_err(|e| cog_core::SFError::Agent(format!("git clone failed: {}", e)))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(cog_core::SFError::Agent(format!(
                    "git clone failed: {}",
                    stderr
                )));
            }
            Self::validate_skill_dir(&target).await?;
            let def = load_skill(&target).await?;
            let mtime = Self::skill_mtime(&target)
                .await
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let skill_id = def.metadata.id.clone();
            let mut cache = self.cache.write().await;
            cache.insert(
                skill_id.clone(),
                CachedSkill {
                    def,
                    path: target,
                    mtime,
                },
            );
            tracing::info!(skill_id = %skill_id, "Downloaded and loaded skill from git");
            return Ok(());
        }

        // HTTP download path
        let response = reqwest::get(source)
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("HTTP download failed: {}", e)))?;
        if !response.status().is_success() {
            return Err(cog_core::SFError::Agent(format!(
                "HTTP download failed: {} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("read download body failed: {}", e)))?;

        let url_path = std::path::Path::new(source);
        let ext = url_path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let basename = url_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("downloaded-skill");
        let target = dest_dir.join(basename);

        match ext {
            "zip" => {
                let zip_path = dest_dir.join(format!("{}.zip", basename));
                tokio::fs::write(&zip_path, &bytes)
                    .await
                    .map_err(|e| cog_core::SFError::Agent(format!("write zip failed: {}", e)))?;
                let output = tokio::process::Command::new("unzip")
                    .args([
                        "-q",
                        "-o",
                        &zip_path.to_string_lossy(),
                        "-d",
                        &target.to_string_lossy(),
                    ])
                    .output()
                    .await
                    .map_err(|e| cog_core::SFError::Agent(format!("unzip failed: {}", e)))?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(cog_core::SFError::Agent(format!(
                        "unzip failed: {}",
                        stderr
                    )));
                }
                let _ = tokio::fs::remove_file(&zip_path).await;
            }
            "gz" | "tgz" => {
                let tar_path = dest_dir.join(format!("{}.tar.gz", basename));
                tokio::fs::write(&tar_path, &bytes)
                    .await
                    .map_err(|e| cog_core::SFError::Agent(format!("write tar.gz failed: {}", e)))?;
                tokio::fs::create_dir_all(&target).await.map_err(|e| {
                    cog_core::SFError::Agent(format!("create target dir failed: {}", e))
                })?;
                let output = tokio::process::Command::new("tar")
                    .args([
                        "-xzf",
                        &tar_path.to_string_lossy(),
                        "-C",
                        &target.to_string_lossy(),
                    ])
                    .output()
                    .await
                    .map_err(|e| cog_core::SFError::Agent(format!("tar failed: {}", e)))?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(cog_core::SFError::Agent(format!("tar failed: {}", stderr)));
                }
                let _ = tokio::fs::remove_file(&tar_path).await;
            }
            _ => {
                // Treat as raw SKILL.md
                tokio::fs::create_dir_all(&target).await.map_err(|e| {
                    cog_core::SFError::Agent(format!("create target dir failed: {}", e))
                })?;
                let skill_md_path = target.join("SKILL.md");
                tokio::fs::write(&skill_md_path, &bytes)
                    .await
                    .map_err(|e| {
                        cog_core::SFError::Agent(format!("write SKILL.md failed: {}", e))
                    })?;
            }
        }

        Self::validate_skill_dir(&target).await?;
        let def = load_skill(&target).await?;
        let mtime = Self::skill_mtime(&target)
            .await
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let skill_id = def.metadata.id.clone();
        let mut cache = self.cache.write().await;
        cache.insert(
            skill_id.clone(),
            CachedSkill {
                def,
                path: target,
                mtime,
            },
        );
        tracing::info!(skill_id = %skill_id, source = %source, "Downloaded and loaded skill");
        Ok(())
    }
}

impl SkillRegistryImpl {
    /// Validate that a directory contains a valid SKILL.md.
    async fn validate_skill_dir(path: &std::path::Path) -> SFResult<()> {
        let skill_md = path.join("SKILL.md");
        if !skill_md.exists() {
            return Err(cog_core::SFError::Agent(format!(
                "downloaded skill directory {} does not contain SKILL.md",
                path.display()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::ExternalSkillRegistry;

    /// 内置 PGE prompt skills（prompts/skills/pge_*）必须能被 registry
    /// 完整解析：SKILL.md 正文 + output_schema.json 资源。
    #[tokio::test]
    async fn builtin_pge_skills_resolve() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../prompts/skills");
        if !dir.is_dir() {
            // 仓库布局变化时静默跳过，避免误报。
            return;
        }
        let registry = SkillRegistryImpl::new(SkillConfig {
            directories: vec![dir],
            hot_reload_interval_secs: 60,
        });
        registry.load_all().await.unwrap();

        for (id, required_key) in [
            ("pge_planner", "sub_tasks"),
            ("pge_generator", "artifacts"),
            ("pge_evaluator", "verdict"),
        ] {
            let def = registry
                .resolve(id)
                .await
                .unwrap_or_else(|e| panic!("resolve {id}: {e}"));
            assert!(!def.skill_md.trim().is_empty(), "{id} SKILL.md empty");
            let schema_text = registry
                .load_resource(id, "output_schema.json")
                .await
                .unwrap_or_else(|e| panic!("load_resource {id}: {e}"));
            let schema: serde_json::Value = serde_json::from_str(&schema_text).unwrap();
            assert!(
                schema.to_string().contains(required_key),
                "{id} schema missing {required_key}"
            );
        }
    }
}
