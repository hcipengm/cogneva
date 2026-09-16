//! Per-task worktree routing for the standalone sandbox executor.
//!
//! Each DAG task id maps to a fixed-path git worktree cut lazily from the
//! executor's own bare repository: agents of the same task collaborate in one
//! tree while different tasks can never trample each other's files, and the
//! self-invented shared `/tmp/cogneva-repo` path disappears structurally.
//!
//! Lifecycle uses only evidence, no task-end signals: a sidecar next to each
//! tree records `last_used_unix`; a periodic GC reaps trees idle past the TTL
//! and enforces an LRU cap. Trees and the bare repo live on a PVC, so a
//! process restart reconciles state from `git worktree list` plus the sidecar
//! directory instead of trusting an in-memory index.
//!
//! The bare-repo + worktree + out-of-tree sidecar + external target dir +
//! stale-lock self-heal pattern is deliberately copied from the evolution
//! WorkspaceManager: cog-extension must not depend on upper crates, and only
//! about a hundred lines of that semantics apply here.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cog_core::{SFError, SFResult};
use prometheus::{Counter, CounterVec, Encoder, Gauge, Opts, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

const DEFAULT_WORKSPACES_ROOT: &str = "/opt/cogneva/sandbox/workspaces";
const DEFAULT_BARE_REPO: &str = "/opt/cogneva/sandbox/repo.git";
const DEFAULT_TARGET_DIR: &str = "/opt/cogneva/sandbox/src/target";
const DEFAULT_TTL_SECS: u64 = 21600;
const DEFAULT_GC_INTERVAL_SECS: u64 = 600;
const DEFAULT_FETCH_INTERVAL_SECS: u64 = 300;
const DEFAULT_MAX_WORKSPACES: usize = 8;

const GIT_TIMEOUT_SECS: u64 = 120;
const META_DIR_NAME: &str = ".meta";

/// Killed git processes (OOM, node restart) leave stale `.lock` files behind;
/// every git write in this process is bounded by [`GIT_TIMEOUT_SECS`] and
/// kill-on-drop, so a lock older than this can only be a corpse.
const STALE_GIT_LOCK_AGE: Duration = Duration::from_secs(600);

/// Maximum task id length, keeps the on-disk directory name clear of
/// `ENAMETOOLONG` while accommodating generated ids such as
/// `ralph-roundtable-<uuid>`.
const MAX_TASK_ID_LEN: usize = 128;

#[derive(Debug, Clone)]
pub struct WorkdirConfig {
    pub workspaces_root: PathBuf,
    pub bare_repo: PathBuf,
    pub target_dir: PathBuf,
    pub seed_url: Option<String>,
    pub ttl: Duration,
    pub gc_interval: Duration,
    pub fetch_interval: Duration,
    pub max_workspaces: usize,
}

impl WorkdirConfig {
    pub fn from_env() -> Self {
        let path = |key: &str, default: &str| {
            std::env::var(key)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| default.into())
        };
        let secs = |key: &str, default: u64, min: u64| {
            let v = std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(default)
                .max(min);
            Duration::from_secs(v)
        };
        let max = std::env::var("SANDBOX_MAX_TASK_WORKSPACES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_WORKSPACES)
            .max(1);
        Self {
            workspaces_root: path("SANDBOX_WORKSPACES_ROOT", DEFAULT_WORKSPACES_ROOT),
            bare_repo: path("SANDBOX_BARE_REPO", DEFAULT_BARE_REPO),
            target_dir: path("CARGO_TARGET_DIR", DEFAULT_TARGET_DIR),
            // A leftover `__PLACEHOLDER__` from an unrendered static manifest
            // must never be treated as a usable URL.
            seed_url: std::env::var("SANDBOX_REPO_SEED_URL")
                .ok()
                .filter(|u| is_usable_url(u)),

            ttl: secs("SANDBOX_WORKSPACE_TTL_SECS", DEFAULT_TTL_SECS, 1),
            gc_interval: secs(
                "SANDBOX_WORKSPACE_GC_INTERVAL_SECS",
                DEFAULT_GC_INTERVAL_SECS,
                10,
            ),
            fetch_interval: secs(
                "SANDBOX_REPO_FETCH_INTERVAL_SECS",
                DEFAULT_FETCH_INTERVAL_SECS,
                30,
            ),
            max_workspaces: max,
        }
    }
}

/// A URL is usable when it is non-empty and does not contain an unresolved
/// `__PLACEHOLDER__` marker.
fn is_usable_url(url: &str) -> bool {
    let u = url.trim();
    !u.is_empty() && !u.contains("__")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TaskMeta {
    task_id: String,
    created_at_unix: i64,
    last_used_unix: i64,
}

#[derive(Clone)]
pub struct WorkdirMetrics {
    registry: Registry,
    workspaces: Gauge,
    gc_reclaimed: Counter,
    fetch_failures: Counter,
    unscoped_requests: Counter,
    errors: CounterVec,
    disk_total_bytes: Gauge,
    disk_avail_bytes: Gauge,
}

impl WorkdirMetrics {
    fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let workspaces = Gauge::new(
            "sandbox_workspaces",
            "Current number of per-task git worktrees on the executor volume",
        )?;
        let gc_reclaimed = Counter::new(
            "sandbox_workspace_gc_reclaimed_total",
            "Task worktrees removed by TTL/LRU garbage collection",
        )?;
        let fetch_failures = Counter::new(
            "sandbox_workspace_fetch_failures_total",
            "Failures of the background bare-repo upstream fetch",
        )?;
        let unscoped_requests = Counter::new(
            "sandbox_workspace_unscoped_requests_total",
            "Executor requests that carried no task id and ran outside a task worktree",
        )?;
        let errors = CounterVec::new(
            Opts::new(
                "sandbox_workspace_errors_total",
                "Task worktree routing errors by kind",
            ),
            &["kind"],
        )?;
        let disk_total_bytes = Gauge::new(
            "sandbox_workspace_disk_total_bytes",
            "Total bytes on the filesystem backing the executor workspaces",
        )?;
        let disk_avail_bytes = Gauge::new(
            "sandbox_workspace_disk_avail_bytes",
            "Available bytes on the filesystem backing the executor workspaces",
        )?;
        registry.register(Box::new(workspaces.clone()))?;
        registry.register(Box::new(gc_reclaimed.clone()))?;
        registry.register(Box::new(fetch_failures.clone()))?;
        registry.register(Box::new(unscoped_requests.clone()))?;
        registry.register(Box::new(errors.clone()))?;
        registry.register(Box::new(disk_total_bytes.clone()))?;
        registry.register(Box::new(disk_avail_bytes.clone()))?;
        Ok(Self {
            registry,
            workspaces,
            gc_reclaimed,
            fetch_failures,
            unscoped_requests,
            errors,
            disk_total_bytes,
            disk_avail_bytes,
        })
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        if TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .is_ok()
        {
            String::from_utf8_lossy(&buf).into_owned()
        } else {
            String::new()
        }
    }

    pub(crate) fn inc_error(&self, kind: &str) {
        self.errors.with_label_values(&[kind]).inc();
    }

    pub fn inc_unscoped(&self) {
        self.unscoped_requests.inc();
    }
}

pub struct WorkdirRouter {
    cfg: WorkdirConfig,
    meta_dir: PathBuf,
    /// Serializes tree creation, cap eviction and GC so two first-seen tasks
    /// never run `worktree add` / `worktree remove` against the same state.
    /// The hot path for an existing tree takes no lock.
    create_lock: Mutex<()>,
    metrics: WorkdirMetrics,
}

impl WorkdirRouter {
    pub fn new(cfg: WorkdirConfig) -> SFResult<Arc<Self>> {
        let meta_dir = cfg.workspaces_root.join(META_DIR_NAME);
        let metrics =
            WorkdirMetrics::new().map_err(|e| SFError::IO(format!("workdir metrics init: {e}")))?;
        Ok(Arc::new(Self {
            cfg,
            meta_dir,
            create_lock: Mutex::new(()),
            metrics,
        }))
    }

    /// Build the router from `SANDBOX_*` env only when the seeded bare repo is
    /// present. An absent bare repo means the executor runs without a
    /// provisioned volume (local/embedded/test usage); callers then keep the
    /// legacy process-cwd behaviour instead of inventing a shared directory.
    pub async fn from_env() -> Option<Arc<Self>> {
        let cfg = WorkdirConfig::from_env();
        match tokio::fs::try_exists(cfg.bare_repo.join("HEAD")).await {
            Ok(true) => match Self::new(cfg) {
                Ok(router) => Some(router),
                Err(e) => {
                    warn!(error = %e, "per-task workdir router disabled");
                    None
                }
            },
            Ok(false) => {
                info!(bare = %cfg.bare_repo.display(),
                    "per-task workdir router disabled: bare repo not seeded");
                None
            }
            Err(e) => {
                warn!(error = %e, "per-task workdir router disabled");
                None
            }
        }
    }

    pub fn config(&self) -> &WorkdirConfig {
        &self.cfg
    }

    pub fn target_dir(&self) -> &Path {
        &self.cfg.target_dir
    }

    pub fn metrics(&self) -> &WorkdirMetrics {
        &self.metrics
    }

    /// Resolve the working directory for a task id, lazily creating the tree
    /// on first sight. Invalid ids and add failures surface as errors instead
    /// of silently collapsing onto a shared directory.
    pub async fn route(&self, task_id: &str) -> SFResult<PathBuf> {
        validate_task_id(task_id)?;
        let path = self.cfg.workspaces_root.join(task_id);
        if self.is_healthy(&path).await {
            self.touch(task_id).await;
            return Ok(path);
        }
        let _g = self.create_lock.lock().await;
        if self.is_healthy(&path).await {
            self.touch(task_id).await;
            return Ok(path);
        }
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            warn!(task_id = %task_id, path = %path.display(),
                "task path exists but is not a healthy worktree; rebuilding");
            self.force_remove(&path).await;
        }
        self.enforce_cap(1).await;
        if let Err(e) = self.add_worktree(task_id, &path).await {
            self.metrics.inc_error("worktree_add");
            return Err(e);
        }
        self.refresh_count_metric().await;
        Ok(path)
    }

    async fn add_worktree(&self, task_id: &str, path: &Path) -> SFResult<()> {
        tokio::fs::create_dir_all(&self.cfg.workspaces_root)
            .await
            .map_err(|e| {
                SFError::IO(format!(
                    "create workspaces root {}: {e}",
                    self.cfg.workspaces_root.display()
                ))
            })?;
        let now = unix_now();
        let meta = TaskMeta {
            task_id: task_id.to_string(),
            created_at_unix: now,
            last_used_unix: now,
        };
        // Sidecar lands before the tree: a crash between the two leaves a
        // reclaimable record; the reverse order leaves an untracked tree.
        self.write_meta(&meta).await?;
        if let Err(e) = self
            .git_bare(&[
                "worktree",
                "add",
                "--detach",
                path.to_str().unwrap_or(""),
                "main",
            ])
            .await
        {
            self.remove_meta(task_id);
            return Err(e);
        }
        info!(task_id = %task_id, path = %path.display(), "created per-task worktree");
        self.register_safe_directory(path).await;
        Ok(())
    }

    /// Healthy = worktree dir present with the `.git` link the bare repo
    /// manages. Authoritative registration reconciliation happens at boot and
    /// GC via `git worktree list`.
    async fn is_healthy(&self, path: &Path) -> bool {
        tokio::fs::try_exists(path.join(".git"))
            .await
            .unwrap_or(false)
            && self.read_meta_of(path).is_some()
    }

    /// Boot-time reconciliation: prune stale registrations, adopt registered
    /// trees that lost their sidecar, drop sidecars whose tree is gone, and
    /// make sure every tree path is a git safe.directory.
    pub async fn recover(&self) -> SFResult<()> {
        tokio::fs::create_dir_all(&self.meta_dir)
            .await
            .map_err(|e| {
                SFError::IO(format!("create meta dir {}: {e}", self.meta_dir.display()))
            })?;
        tokio::fs::create_dir_all(&self.cfg.target_dir).await.ok();
        let _ = self.git_bare(&["worktree", "prune"]).await;

        let trees = self.list_task_trees().await;
        for (id, path) in &trees {
            if self.read_meta(id).is_none() {
                warn!(task_id = %id, "adopting worktree with missing sidecar");
                let now = unix_now();
                self.write_meta(&TaskMeta {
                    task_id: id.clone(),
                    created_at_unix: now,
                    last_used_unix: now,
                })
                .await?;
            }
            self.register_safe_directory(path).await;
        }
        self.prune_orphan_metas(&trees).await;
        self.refresh_count_metric().await;
        Ok(())
    }

    /// Reap trees idle past the TTL, then enforce the LRU cap. Returns the
    /// number of reclaimed trees.
    pub async fn gc_once(&self) -> SFResult<u64> {
        let _g = self.create_lock.lock().await;
        let now = unix_now();
        let ttl = self.cfg.ttl.as_secs() as i64;
        let trees = self.list_task_trees().await;
        let mut reclaimed = 0u64;
        for (id, path) in trees.iter() {
            let reap = match self.read_meta(id) {
                Some(meta) => now.saturating_sub(meta.last_used_unix) >= ttl,
                None => {
                    // Registered tree without sidecar: adopt rather than reap,
                    // a restart may have raced its first write.
                    let now = unix_now();
                    let _ = self
                        .write_meta(&TaskMeta {
                            task_id: id.clone(),
                            created_at_unix: now,
                            last_used_unix: now,
                        })
                        .await;
                    false
                }
            };
            if reap {
                info!(task_id = %id, "reclaiming idle task worktree (TTL expired)");
                self.force_remove(path).await;
                self.remove_meta(id);
                self.metrics.gc_reclaimed.inc();
                reclaimed += 1;
            }
        }
        self.enforce_cap(0).await;
        let _ = self.git_bare(&["worktree", "prune"]).await;
        self.prune_orphan_metas(&self.list_task_trees().await).await;
        self.refresh_count_metric().await;
        Ok(reclaimed)
    }

    /// Remove least-recently-used trees until at most `max - incoming` trees
    /// remain. Caller must hold [`Self::create_lock`] semantics (route and
    /// gc_once take it; this fn assumes exclusion).
    async fn enforce_cap(&self, incoming: usize) {
        let max = self.cfg.max_workspaces;
        let mut trees = self.list_task_trees().await;
        if trees.len() + incoming <= max {
            return;
        }
        trees.sort_by_key(|(id, _)| {
            self.read_meta(id.as_str())
                .map(|m| m.last_used_unix)
                .unwrap_or(i64::MIN)
        });
        let mut remaining = trees.len() + incoming;
        for (id, path) in trees {
            if remaining <= max {
                break;
            }
            warn!(task_id = %id, cap = max, "reclaiming task worktree (LRU cap)");
            self.force_remove(&path).await;
            self.remove_meta(&id);
            self.metrics.gc_reclaimed.inc();
            remaining -= 1;
        }
    }

    /// Fetch upstream refs/tags into the bare repo. Never fatal: offline
    /// executors keep serving the refs present at seed.
    pub async fn fetch_once(&self) {
        if self
            .git_bare(&["remote", "get-url", "upstream"])
            .await
            .is_err()
        {
            let origin = self
                .git_bare(&["remote", "get-url", "origin"])
                .await
                .ok()
                .filter(|u| is_usable_url(u));
            let url = self.cfg.seed_url.clone().or(origin);
            let Some(url) = url else {
                warn!("no usable upstream remote for sandbox bare repo; skipping fetch");
                self.metrics.fetch_failures.inc();
                return;
            };
            if let Err(e) = self.git_bare(&["remote", "add", "upstream", &url]).await {
                warn!(error = %e, "could not add upstream remote");
                self.metrics.fetch_failures.inc();
                return;
            }
        }
        // A bare `fetch upstream` only writes refs/remotes/upstream/* and never
        // advances the local `main` that new worktrees are cut from; the
        // `+main:main` refspec fast-forces the local branch to upstream so the
        // 300s background fetch actually freshens the baseline. Existing
        // detached task trees keep pointing at their own commit.
        match self
            .git_bare(&["fetch", "upstream", "--tags", "--force", "+main:main"])
            .await
        {
            Ok(_) => info!("sandbox bare repo fetched from upstream"),
            Err(e) => {
                warn!(error = %e, "sandbox bare repo upstream fetch failed; serving seeded refs");
                self.metrics.fetch_failures.inc();
            }
        }
    }

    /// Spawn the GC and fetch background loops. Call once after [`Self::recover`].
    pub fn spawn_maintenance(self: &Arc<Self>) {
        let gc = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(gc.cfg.gc_interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(e) = gc.gc_once().await {
                    warn!(error = %e, "task worktree GC failed");
                }
            }
        });
        let fetch = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(fetch.cfg.fetch_interval).await;
                fetch.fetch_once().await;
            }
        });
    }

    // -- internals -------------------------------------------------------------

    fn meta_path(&self, task_id: &str) -> PathBuf {
        self.meta_dir.join(format!("{task_id}.json"))
    }

    async fn write_meta(&self, meta: &TaskMeta) -> SFResult<()> {
        tokio::fs::create_dir_all(&self.meta_dir)
            .await
            .map_err(|e| SFError::IO(format!("create meta dir: {e}")))?;
        let text = serde_json::to_string_pretty(meta)?;
        tokio::fs::write(self.meta_path(&meta.task_id), text)
            .await
            .map_err(|e| SFError::IO(format!("write task meta: {e}")))?;
        Ok(())
    }

    fn read_meta(&self, task_id: &str) -> Option<TaskMeta> {
        std::fs::read_to_string(self.meta_path(task_id))
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
    }

    fn read_meta_of(&self, path: &Path) -> Option<TaskMeta> {
        let id = path.file_name()?.to_str()?;
        self.read_meta(id)
    }

    fn remove_meta(&self, task_id: &str) {
        let _ = std::fs::remove_file(self.meta_path(task_id));
    }

    async fn touch(&self, task_id: &str) {
        if let Some(mut meta) = self.read_meta(task_id) {
            meta.last_used_unix = unix_now();
            if self.write_meta(&meta).await.is_err() {
                warn!(task_id = %task_id, "failed to refresh task worktree last_used");
            }
        }
    }

    async fn prune_orphan_metas(&self, trees: &[(String, PathBuf)]) {
        let live: std::collections::HashSet<String> =
            trees.iter().map(|(id, _)| id.clone()).collect();
        let Ok(mut entries) = tokio::fs::read_dir(&self.meta_dir).await else {
            return;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Some(id) = name.strip_suffix(".json") else {
                continue;
            };
            if !live.contains(id) {
                let _ = tokio::fs::remove_file(entry.path()).await;
            }
        }
    }

    /// Registered worktrees under the workspaces root, as `(task_id, path)`.
    async fn list_task_trees(&self) -> Vec<(String, PathBuf)> {
        let out = match self.git_bare(&["worktree", "list", "--porcelain"]).await {
            Ok(out) => out,
            Err(e) => {
                warn!(error = %e, "git worktree list failed");
                return Vec::new();
            }
        };
        parse_worktree_list(&out)
            .into_iter()
            .filter(|e| !e.bare && e.path.parent() == Some(self.cfg.workspaces_root.as_path()))
            .filter_map(|e| {
                let id = e.path.file_name()?.to_str()?.to_string();
                Some((id, e.path))
            })
            .collect()
    }

    async fn force_remove(&self, path: &Path) {
        let _ = self
            .git_bare(&["worktree", "remove", "--force", path.to_str().unwrap_or("")])
            .await;
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            let _ = tokio::fs::remove_dir_all(path).await;
        }
        let _ = self.git_bare(&["worktree", "prune"]).await;
    }

    async fn register_safe_directory(&self, path: &Path) {
        let _ = self
            .run_git(
                &[
                    "config",
                    "--global",
                    "--add",
                    "safe.directory",
                    path.to_str().unwrap_or(""),
                ],
                None,
            )
            .await;
    }

    async fn refresh_count_metric(&self) {
        self.metrics
            .workspaces
            .set(self.list_task_trees().await.len() as f64);
        self.refresh_disk_metric().await;
    }

    /// Filesystem capacity via `df` (the runtime image carries coreutils).
    async fn refresh_disk_metric(&self) {
        let out = tokio::process::Command::new("df")
            .arg("-PB1")
            .arg(&self.cfg.workspaces_root)
            .output()
            .await;
        if let Ok(out) = out {
            if out.status.success() {
                let text = String::from_utf8_lossy(&out.stdout);
                if let Some(line) = text.lines().nth(1) {
                    let cols: Vec<&str> = line.split_whitespace().collect();
                    if cols.len() >= 4 {
                        if let Ok(total) = cols[1].parse::<f64>() {
                            self.metrics.disk_total_bytes.set(total);
                        }
                        if let Ok(avail) = cols[3].parse::<f64>() {
                            self.metrics.disk_avail_bytes.set(avail);
                        }
                    }
                }
            }
        }
    }

    async fn run_git(&self, args: &[&str], workdir: Option<&Path>) -> SFResult<String> {
        match self.run_git_once(args, workdir).await {
            Ok(out) => Ok(out),
            Err(e) => {
                let cleared = match stale_lock_candidate(&e.to_string()) {
                    Some(lock) => {
                        let gone = clear_stale_git_lock(&lock, STALE_GIT_LOCK_AGE).await;
                        if gone {
                            warn!(lock = %lock.display(),
                                "removed stale git lock left by a killed process; retrying");
                        }
                        gone
                    }
                    None => false,
                };
                if cleared {
                    self.run_git_once(args, workdir).await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn run_git_once(&self, args: &[&str], workdir: Option<&Path>) -> SFResult<String> {
        let mut cmd = tokio::process::Command::new("git");
        cmd.args(args).kill_on_drop(true);
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
        let out = tokio::time::timeout(Duration::from_secs(GIT_TIMEOUT_SECS), cmd.output())
            .await
            .map_err(|_| SFError::IO(format!("git {} timed out", args.join(" "))))?
            .map_err(|e| SFError::IO(format!("failed to run git: {e}")))?;
        if !out.status.success() {
            return Err(SFError::IO(format!(
                "git {} failed: {}{}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    async fn git_bare(&self, args: &[&str]) -> SFResult<String> {
        let mut full: Vec<&str> = vec!["--git-dir", self.cfg.bare_repo.to_str().unwrap_or("")];
        full.extend_from_slice(args);
        self.run_git(&full, None).await
    }
}

/// Strict task id validation: the id is a client-supplied filesystem name, so
/// only whitelisted characters pass and any traversal or reserved form is an
/// error (never a silent rename into a shared bucket).
pub(crate) fn validate_task_id(id: &str) -> SFResult<()> {
    let valid = !id.is_empty()
        && id.len() <= MAX_TASK_ID_LEN
        && !id.starts_with('.')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(SFError::Agent(format!(
            "invalid task id for workspace routing: {id:?}"
        )))
    }
}

fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Extract the lock path from git's fixed wording:
/// `fatal: Unable to create '<path>/index.lock': File exists.`
/// Only `.lock` paths are accepted.
fn stale_lock_candidate(error_text: &str) -> Option<PathBuf> {
    const PREFIX: &str = "Unable to create '";
    const SUFFIX: &str = "': File exists";
    let start = error_text.find(PREFIX)? + PREFIX.len();
    let end = error_text[start..].find(SUFFIX)? + start;
    let path = PathBuf::from(&error_text[start..end]);
    if path.extension().and_then(|s| s.to_str()) == Some("lock") {
        Some(path)
    } else {
        None
    }
}

async fn clear_stale_git_lock(lock: &Path, max_age: Duration) -> bool {
    let Ok(meta) = tokio::fs::metadata(lock).await else {
        return false;
    };
    let Ok(mtime) = meta.modified() else {
        return false;
    };
    if mtime.elapsed().unwrap_or(Duration::ZERO) < max_age {
        return false;
    }
    tokio::fs::remove_file(lock).await.is_ok()
}

#[derive(Debug, Clone)]
struct WorktreeEntry {
    path: PathBuf,
    bare: bool,
}

fn parse_worktree_list(text: &str) -> Vec<WorktreeEntry> {
    let mut out = Vec::new();
    let mut cur: Option<WorktreeEntry> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("worktree ") {
            if let Some(entry) = cur.take() {
                out.push(entry);
            }
            cur = Some(WorktreeEntry {
                path: PathBuf::from(rest),
                bare: false,
            });
        } else if let Some(entry) = cur.as_mut() {
            if line == "bare" {
                entry.bare = true;
            }
        }
    }
    if let Some(entry) = cur {
        out.push(entry);
    }
    out
}

/// Anchor a payload path against a task worktree: relative paths resolve inside
/// the tree, absolute paths keep their meaning. No filesystem fence is added
/// here — the pod boundary remains the security boundary for now.
pub fn anchor_path(base: Option<&Path>, path: &str) -> PathBuf {
    let p = Path::new(path);
    match base {
        Some(dir) if p.is_relative() => dir.join(p),
        _ => p.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parses_env_with_safe_floors() {
        std::env::set_var("SANDBOX_WORKSPACE_TTL_SECS", "1");
        std::env::set_var("SANDBOX_WORKSPACE_GC_INTERVAL_SECS", "1");
        std::env::set_var("SANDBOX_REPO_FETCH_INTERVAL_SECS", "1");
        std::env::set_var("SANDBOX_MAX_TASK_WORKSPACES", "0");
        std::env::set_var("SANDBOX_REPO_SEED_URL", "__GIT_SEED_URL__");
        let cfg = WorkdirConfig::from_env();
        assert!(cfg.ttl >= Duration::from_secs(1));
        assert!(cfg.gc_interval >= Duration::from_secs(10));
        assert!(cfg.fetch_interval >= Duration::from_secs(30));
        assert_eq!(cfg.max_workspaces, 1);
        assert!(
            cfg.seed_url.is_none(),
            "unresolved placeholder is not a usable URL"
        );
        for key in [
            "SANDBOX_WORKSPACE_TTL_SECS",
            "SANDBOX_WORKSPACE_GC_INTERVAL_SECS",
            "SANDBOX_REPO_FETCH_INTERVAL_SECS",
            "SANDBOX_MAX_TASK_WORKSPACES",
            "SANDBOX_REPO_SEED_URL",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn rejects_invalid_task_ids() {
        assert!(validate_task_id("").is_err());
        assert!(validate_task_id("../escape").is_err());
        assert!(validate_task_id("..").is_err());
        assert!(validate_task_id(".").is_err());
        assert!(validate_task_id(".meta").is_err());
        assert!(validate_task_id("a/b").is_err());
        assert!(validate_task_id("a b").is_err());
        assert!(validate_task_id("task$x").is_err());
        assert!(validate_task_id(&"x".repeat(MAX_TASK_ID_LEN + 1)).is_err());
        validate_task_id("ralph-roundtable-7bb0d434-3a3e-4c9f-b1a0-deadbeefcafe").unwrap();
        validate_task_id("squad_task.1-x").unwrap();
    }

    #[test]
    fn placeholder_url_is_not_usable() {
        assert!(is_usable_url("https://github.com/o/r.git"));
        assert!(!is_usable_url("__GIT_SEED_URL__"));
        assert!(!is_usable_url("  "));
    }

    #[test]
    fn anchors_relative_paths_under_base_keeps_absolute() {
        let base = Path::new("/ws/task-1");
        assert_eq!(
            anchor_path(Some(base), "a/b.txt"),
            Path::new("/ws/task-1/a/b.txt")
        );
        assert_eq!(
            anchor_path(Some(base), "/etc/hostname"),
            Path::new("/etc/hostname")
        );
        assert_eq!(anchor_path(None, "a/b.txt"), Path::new("a/b.txt"));
    }

    #[test]
    fn parses_porcelain_worktree_list() {
        let text =
            "worktree /a/bare.git\nHEAD abc\nbare\n\nworktree /a/ws/task-1\nHEAD def\ndetached\n\n";
        let entries = parse_worktree_list(text);
        assert_eq!(entries.len(), 2);
        assert!(entries[0].bare);
        assert!(!entries[1].bare);
        assert_eq!(entries[1].path, PathBuf::from("/a/ws/task-1"));
    }

    #[test]
    fn stale_lock_candidate_parses_git_wording() {
        let err = "fatal: Unable to create '/repo/worktrees/x/index.lock': File exists.";
        assert_eq!(
            stale_lock_candidate(err),
            Some(PathBuf::from("/repo/worktrees/x/index.lock"))
        );
        assert!(stale_lock_candidate("Unable to create '/x/HEAD': File exists.").is_none());
        assert!(stale_lock_candidate("nothing here").is_none());
    }

    fn git(args: &[&str], dir: &Path) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }

    fn git_out(dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .expect("run git");
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn seed_bare(root: &Path) -> PathBuf {
        let work = root.join("seed");
        std::fs::create_dir_all(&work).unwrap();
        git(&["init", "-q", "-b", "main"], &work);
        std::fs::write(work.join("README"), "base\n").unwrap();
        git(&["add", "-A"], &work);
        git(&["commit", "-qm", "base"], &work);
        let bare = root.join("repo.git");
        let out = std::process::Command::new("git")
            .args(["clone", "-q", "--bare"])
            .arg(&work)
            .arg(&bare)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(out.status.success(), "bare clone failed");
        bare
    }

    fn router(root: &Path, bare: &Path, max: usize, ttl: Duration) -> Arc<WorkdirRouter> {
        let cfg = WorkdirConfig {
            workspaces_root: root.join("workspaces"),
            bare_repo: bare.to_path_buf(),
            target_dir: root.join("src").join("target"),
            seed_url: None,
            ttl,
            gc_interval: Duration::from_secs(600),
            fetch_interval: Duration::from_secs(300),
            max_workspaces: max,
        };
        WorkdirRouter::new(cfg).unwrap()
    }

    #[tokio::test]
    async fn route_creates_and_reuses_task_worktree() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);

        let path = r.route("task-1").await.unwrap();
        assert_eq!(path, tmp.path().join("workspaces").join("task-1"));
        assert!(path.join("README").exists());
        assert!(tmp.path().join("workspaces/.meta/task-1.json").exists());

        let again = r.route("task-1").await.unwrap();
        assert_eq!(again, path);
        assert_eq!(r.list_task_trees().await.len(), 1);
    }

    const DEFAULT_TTL_SECS_FALLBACK: Duration = Duration::from_secs(21600);

    #[tokio::test]
    async fn tasks_are_isolated_and_keep_their_own_files() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);

        let a = r.route("task-a").await.unwrap();
        let b = r.route("task-b").await.unwrap();
        std::fs::write(a.join("out.txt"), "from-a").unwrap();
        std::fs::write(b.join("out.txt"), "from-b").unwrap();

        assert_eq!(
            std::fs::read_to_string(a.join("out.txt")).unwrap(),
            "from-a"
        );
        assert_eq!(
            std::fs::read_to_string(b.join("out.txt")).unwrap(),
            "from-b"
        );
        assert_eq!(r.list_task_trees().await.len(), 2);
    }

    #[tokio::test]
    async fn invalid_id_is_error_not_shared_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        assert!(r.route("../evil").await.is_err());
        assert!(r.route(".meta").await.is_err());
        assert_eq!(r.list_task_trees().await.len(), 0);
    }

    #[tokio::test]
    async fn gc_reaps_idle_ttl_trees_and_sidecars() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, Duration::ZERO);
        let path = r.route("stale-task").await.unwrap();
        let meta = tmp.path().join("workspaces/.meta/stale-task.json");
        assert!(meta.exists());

        let n = r.gc_once().await.unwrap();
        assert_eq!(n, 1);
        assert!(!path.exists());
        assert!(!meta.exists());
        assert_eq!(r.list_task_trees().await.len(), 0);
    }

    #[tokio::test]
    async fn lru_cap_evicts_oldest_only() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 2, DEFAULT_TTL_SECS_FALLBACK);
        r.route("old").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("mid").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("new").await.unwrap();

        let trees = r.list_task_trees().await;
        let ids: Vec<String> = trees.into_iter().map(|(id, _)| id).collect();
        assert!(ids.contains(&"mid".to_string()), "{ids:?}");
        assert!(ids.contains(&"new".to_string()), "{ids:?}");
        assert!(!ids.contains(&"old".to_string()));
    }

    #[tokio::test]
    async fn restart_reconciles_trees_and_sidecars() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r1 = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let path = r1.route("task-survive").await.unwrap();

        // Simulate a lost sidecar (tree still registered): recover adopts it.
        std::fs::remove_file(tmp.path().join("workspaces/.meta/task-survive.json")).unwrap();
        r1.recover().await.unwrap();
        assert!(tmp
            .path()
            .join("workspaces/.meta/task-survive.json")
            .exists());

        // A brand-new router process over the same PVC routes to the same tree.
        let r2 = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        r2.recover().await.unwrap();
        assert_eq!(r2.route("task-survive").await.unwrap(), path);

        // Orphan sidecar without a tree is dropped during recovery.
        std::fs::write(tmp.path().join("workspaces/.meta/ghost.json"), "{}").unwrap();
        r2.recover().await.unwrap();
        assert!(!tmp.path().join("workspaces/.meta/ghost.json").exists());
    }

    #[tokio::test]
    async fn stale_git_lock_is_cleared_and_retried() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let path = r.route("lock-task").await.unwrap();
        let gitdir = PathBuf::from(git_out(&path, &["rev-parse", "--absolute-git-dir"]));
        let lock = gitdir.join("index.lock");
        std::fs::write(&lock, "").unwrap();
        // Backdate mtime past the 600s stale threshold.
        let old = std::time::SystemTime::now() - Duration::from_secs(900);
        let times = std::fs::FileTimes::new()
            .set_accessed(old)
            .set_modified(old);
        std::fs::File::open(&lock)
            .unwrap()
            .set_times(times)
            .unwrap();

        // run_git self-heals the stale lock and the retry succeeds.
        r.run_git(&["reset", "--hard", "HEAD"], Some(&path))
            .await
            .unwrap();
        assert!(!lock.exists());
    }
}
