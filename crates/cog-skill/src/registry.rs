//! Skill registry implementation — in-memory cache with filesystem backing.

use async_trait::async_trait;
use cog_core::{DownloadOptions, ExternalSkillRegistry, SFResult, SkillDef, SkillMetadata};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::discovery::discover_all;
use crate::loader::load_skill;

/// Loop name reported through the background-loop liveness family.
pub const SKILL_HOT_RELOAD_LOOP: &str = "skill_hot_reload";

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

/// Where a round of the hot-reload check ended.
///
/// The closed set of ends, plus `failed` for a round that could not scan the
/// directories: without it, a loop that is failing every round and a loop with
/// nothing to pick up are the same picture on the reading surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotReloadRound {
    /// At least one skill was loaded, replaced, or evicted.
    Applied,
    /// The scan and the cache agreed — nothing to do.
    Unchanged,
    /// A skill found on disk could not be loaded; the stale definition keeps
    /// serving and the entry is re-scanned on every round.
    ReloadFailed,
}

impl HotReloadRound {
    fn as_cell(&self) -> &'static str {
        match self {
            HotReloadRound::Applied => "applied",
            HotReloadRound::Unchanged => "unchanged",
            HotReloadRound::ReloadFailed => "reload_failed",
        }
    }
}

/// In-memory skill registry backed by filesystem directories.
pub struct SkillRegistryImpl {
    config: SkillConfig,
    cache: RwLock<HashMap<String, CachedSkill>>,
    /// Where each round's outcome goes. Unset when the metrics backend was not
    /// published: the round then leaves only the log line it always left.
    metrics: std::sync::OnceLock<Arc<dyn cog_core::MetricsBackend>>,
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
            metrics: std::sync::OnceLock::new(),
        })
    }

    /// Attach the sink for the per-round outcome, once.
    ///
    /// Called from the plugin's `start`, not its `init`: the metrics backend is
    /// published by the storage plugin, which initialises after this one, so
    /// resolving it inside this plugin's own `init` returns `None`. A `None`
    /// sink is not an error — the round keeps its log line — so it is simply not
    /// set.
    pub fn set_metrics(&self, metrics: Option<Arc<dyn cog_core::MetricsBackend>>) {
        if let Some(metrics) = metrics {
            let _ = self.metrics.set(metrics);
        }
    }

    /// Where this round's outcome goes. `None` when no backend was published.
    fn metrics(&self) -> Option<&Arc<dyn cog_core::MetricsBackend>> {
        self.metrics.get()
    }

    /// Record which cell of the closed outcome set this round landed in.
    ///
    /// `None` means the round ended in an error. It is written at the same point
    /// as the round itself so a round that could not scan the directories and a
    /// round that found nothing to do stay apart on the reading surface; without
    /// it both leave only a line that dies with the pod.
    async fn record_round(&self, outcome: Option<&HotReloadRound>) {
        let Some(metrics) = self.metrics() else {
            return;
        };
        let cell = outcome.map_or("failed", HotReloadRound::as_cell);
        let mut labels = HashMap::new();
        labels.insert("outcome".to_string(), cell.to_string());
        if let Err(e) = metrics
            .record_counter(cog_core::metric_names::SKILL_HOT_RELOAD_TOTAL, 1.0, labels)
            .await
        {
            tracing::warn!(error = %e, "skill hot-reload: could not record round outcome");
        }
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
        let interval_secs = registry.config.hot_reload_interval_secs;
        let period = std::time::Duration::from_secs(interval_secs);
        // No stop signal and no exit of its own: ending means a skill edited on
        // disk is never picked up again.
        cog_core::loop_health::spawn_unstoppable(
            SKILL_HOT_RELOAD_LOOP,
            cog_core::loop_health::Cadence::Periodic(period),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let registry = registry.clone();
                async move {
                    let mut interval = tokio::time::interval(period);
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        beat.beat();
                        interval.tick().await;
                        registry.run_round().await;
                    }
                }
            },
        )
    }

    /// Run one round and record where it landed.
    ///
    /// The recording lives here rather than in the caller so that the failure
    /// has one place to be dropped from, and that place is a round: a caller
    /// that unwrapped the result itself would lose the only cell that tells a
    /// loop failing every round from a loop with nothing to pick up.
    async fn run_round(&self) {
        match self.check_and_reload().await {
            Ok(outcome) => self.record_round(Some(&outcome)).await,
            Err(e) => {
                tracing::warn!("Skill hot-reload check failed: {}", e);
                self.record_round(None).await;
            }
        }
    }

    /// Compare disk state with in-memory cache and reload changed skills.
    ///
    /// Returns which cell of the closed outcome set this round landed in. A
    /// round that both moved the cache and failed to load another skill is
    /// `Applied`: the failed skill is re-scanned every round and re-surfaces,
    /// while what was applied is true only of this round.
    async fn check_and_reload(&self) -> SFResult<HotReloadRound> {
        let discovered = discover_all(&self.config.directories).await?;
        let mut cache = self.cache.write().await;

        let mut applied = false;
        let mut reload_failed = false;

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
            applied = true;
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
                        applied = true;
                    }
                    Err(e) => {
                        tracing::warn!(skill_id = %skill_id, path = %path.display(), error = %e, "Failed to hot-reload skill");
                        reload_failed = true;
                    }
                }
            }
        }

        Ok(if applied {
            HotReloadRound::Applied
        } else if reload_failed {
            HotReloadRound::ReloadFailed
        } else {
            HotReloadRound::Unchanged
        })
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

    /// The skill list in id order.
    ///
    /// The cache is a HashMap, so its iteration order is arbitrary — and a hot
    /// reload (`load_all` clearing the map and refilling it) is enough to change
    /// it. This list is rendered into the first system message of the agent
    /// prompt, where the model reads first, so a reshuffle invalidates the
    /// upstream prefix cache for the whole prompt. Order comes from the content
    /// (the ids), not from the container.
    async fn list(&self) -> SFResult<Vec<SkillMetadata>> {
        let cache = self.cache.read().await;
        let mut skills: Vec<SkillMetadata> =
            cache.values().map(|c| c.def.metadata.clone()).collect();
        skills.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(skills)
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
    use cog_core::{ExternalSkillRegistry, MetricsBackend};

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

    /// The rendered skill list must be in id order, and a hot reload must leave
    /// it that way.
    ///
    /// The cache is a HashMap and `load_all` clears and refills it — precisely
    /// the moment the iteration order can change. The list is rendered into the
    /// first system message of the agent prompt, so a reshuffle there invalidates
    /// the upstream prefix cache for the entire prompt. Enough skills that the
    /// cache order matching the id order by luck is negligible.
    #[tokio::test]
    async fn the_skill_list_is_in_id_order_and_a_hot_reload_does_not_reshuffle_it() {
        const COUNT: usize = 12;
        let dir = tempfile::tempdir().unwrap();
        let mut expected = Vec::new();
        for i in 0..COUNT {
            let id = format!("skill_{i:02}");
            let skill_dir = dir.path().join(&id);
            std::fs::create_dir_all(&skill_dir).unwrap();
            std::fs::write(
                skill_dir.join("SKILL.md"),
                format!(
                    "---\nname: Skill {i}\ndescription: synthetic skill {i}\n---\n\nbody {i}\n"
                ),
            )
            .unwrap();
            expected.push(id);
        }
        expected.sort();

        let registry = SkillRegistryImpl::new(SkillConfig {
            directories: vec![dir.path().to_path_buf()],
            hot_reload_interval_secs: 60,
        });

        let ids = |skills: Vec<SkillMetadata>| -> Vec<String> {
            skills.into_iter().map(|m| m.id).collect()
        };

        registry.load_all().await.unwrap();
        let first = ids(registry.list().await.unwrap());
        assert_eq!(first, expected, "the list must be in id order");

        // The hot reload is the writing side of this reading: it empties the map
        // and fills it again, which is where an unordered list would move.
        registry.load_all().await.unwrap();
        let second = ids(registry.list().await.unwrap());
        assert_eq!(
            second, first,
            "a hot reload must not reshuffle what the prompt renders"
        );
    }

    /// 闭集四格的名字两两不同：一格的名字就是它在读数面上的身份，
    /// 两格同名等于一格。
    #[test]
    fn hot_reload_cells_are_a_closed_set_of_distinct_names() {
        let cells: Vec<&str> = [
            HotReloadRound::Applied,
            HotReloadRound::Unchanged,
            HotReloadRound::ReloadFailed,
        ]
        .iter()
        .map(|r| r.as_cell())
        .collect();
        assert_eq!(cells, vec!["applied", "unchanged", "reload_failed"]);
        let unique: std::collections::HashSet<&str> = cells.iter().copied().collect();
        assert_eq!(unique.len(), cells.len(), "闭集里的名字必须两两不同");
    }

    /// 每一轮恰好落一格，且「同一轮里既搬动缓存又加载失败」记 `applied`。
    ///
    /// 最后一组断言是这条优先级的存在理由：让位给 `applied` 的
    /// `reload_failed` 下一轮必然再现（坏文件还在，缓存里也没有它），
    /// 所以它不会被这一轮吞掉；反过来则会永久压制自愈的那格。
    #[tokio::test]
    async fn every_hot_reload_round_lands_in_exactly_one_cell() {
        let dir = tempfile::tempdir().unwrap();
        let registry = SkillRegistryImpl::new(SkillConfig {
            directories: vec![dir.path().to_path_buf()],
            hot_reload_interval_secs: 60,
        });
        let metrics = Arc::new(cog_storage::MemoryMetricsBackend::new());
        registry.set_metrics(Some(metrics.clone()));
        // 空目录：扫描与缓存一致。
        registry.run_round().await;

        // 一个坏技能（frontmatter 少了收尾的 ---）：扫得到、加载不了。
        let broken = dir.path().join("broken_skill");
        std::fs::create_dir_all(&broken).unwrap();
        std::fs::write(broken.join("SKILL.md"), "---\nname: Broken\n").unwrap();
        registry.run_round().await;

        // 同一个坏技能还在，另加一个好技能：这一轮两格都点着了，记 `applied`。
        let good = dir.path().join("good_skill");
        std::fs::create_dir_all(&good).unwrap();
        std::fs::write(
            good.join("SKILL.md"),
            "---\nname: Good\ndescription: ok\n---\n\nbody\n",
        )
        .unwrap();
        registry.run_round().await;

        // 好技能已进缓存、mtime 没变；坏技能仍加载不了 ⇒ 它再现了。
        registry.run_round().await;

        // 折了的那一轮：目录指向一个普通文件，扫描本身就失败，也必须落格。
        let not_a_dir = dir.path().join("not_a_dir");
        std::fs::write(&not_a_dir, "not a directory").unwrap();
        let failing = SkillRegistryImpl::new(SkillConfig {
            directories: vec![not_a_dir],
            hot_reload_interval_secs: 60,
        });
        failing.set_metrics(Some(metrics.clone()));
        failing.run_round().await;

        let totals = metrics
            .query_counter_totals("cogneva_skill_hot_reload_total")
            .await
            .unwrap();
        let cell = |name: &str| {
            totals
                .iter()
                .find(|s| s.labels.get("outcome").map(String::as_str) == Some(name))
                .map(|s| s.value)
        };
        assert_eq!(
            cell("applied"),
            Some(1.0),
            "既搬动缓存又加载失败的那轮记 applied"
        );
        assert_eq!(cell("unchanged"), Some(1.0));
        assert_eq!(
            cell("reload_failed"),
            Some(2.0),
            "让位给 applied 的那格下一轮必须再现"
        );
        assert_eq!(cell("failed"), Some(1.0));
        assert_eq!(totals.len(), 4, "闭集恰好四格，多一格少一格都是形状变了");
    }
}
