//! 沙盒工作区动态分配。
//!
//! 以集群内裸仓库为唯一源，按用途分配 `git worktree` 检出：部署器、每轮演进、
//! 移植器各占一棵，谁把检出停在哪里都不改变别人的仓库状态，共用一棵可变树导致的
//! 互相卡死从结构上消失。
//!
//! 编译产物目录（cargo target）外置并共享：工作树里只有源码，因此工作树可以被
//! 清理或整棵重建而不丢增量缓存；cargo 自身对 target 目录加文件锁，多个构建天然
//! 串行，无需另造构建锁。
//!
//! 周期性构建路径必须**稳定复用**，不能每轮换新路径。cargo 对本地 path crate 的
//! 新鲜度按源码 mtime 判定（路径不进指纹）：`worktree add` 把整棵树写成当前时间，
//! 于是新路径的工作树会让全部本地 crate 重编一次，只有外部依赖命中缓存；同一路径
//! 上 `reset --hard` 只重写有差异的文件，未变 crate 才保持新鲜。所以部署器、每轮
//! 演进、移植器、引擎基线都取常驻工作树就地从基线刷新。只有一次性的临时任务
//! （如运维手工触发的 admin 部署）才用带 uuid 的 [`WorkspaceManager::acquire_ephemeral`]，
//! 以路径唯一换取并发安全，代价是那一次冷编。

use std::path::{Path, PathBuf};
use std::time::Duration;

use cog_core::{SFError, SFResult};
use serde::{Deserialize, Serialize};
use tracing::info;

/// 工作树默认根目录（沙盒 PVC 内，与 bin/backups/changes/mainline 同级）。
pub const DEFAULT_WORKSPACES_ROOT: &str = "/opt/cogneva/sandbox/workspaces";

/// git 子命令超时：worktree add 要检出全树，给足余量。
const GIT_TIMEOUT_SECS: u64 = 120;

/// 临时工作树默认存活上限。要容得下一整轮演进：属主 pid 是 Pod 进程号，
/// 同一进程内永远"活着"，存活时长才是唯一的兜底判据。
pub const DEFAULT_EPHEMERAL_TTL: Duration = Duration::from_secs(21600);

/// 回收"实例身份已轮换"遗留工作树的最小存活时长。滚动更新期间新旧 Pod 可能
/// 短暂共存、共享同一工作树根，旧 Pod 正在用的树必须活过这个窗口。
///
/// 取值依据：沙盒 PVC 是 ReadWriteOnce、工作负载策略是 Recreate，新 Pod 起来时
/// 旧 Pod 必须已经释放卷，实际重叠窗口接近零；这个值只防御将来换成 RWX 或
/// RollingUpdate 的情形，取默认终止宽限期的若干倍即可，不能取大——取值越大，
/// 重启越频繁时遗留树越可能永远躲过回收。
pub const ORPHAN_MIN_AGE: Duration = Duration::from_secs(900);

/// 工作树用途。除 `Ephemeral` 外都是常驻：不参与回收，跨轮次保留。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceKind {
    Deployer,
    Cycle,
    Porter,
    EngineBaseline,
    Ephemeral,
}

impl WorkspaceKind {
    pub fn is_persistent(self) -> bool {
        !matches!(self, WorkspaceKind::Ephemeral)
    }

    fn as_str(self) -> &'static str {
        match self {
            WorkspaceKind::Deployer => "deployer",
            WorkspaceKind::Cycle => "cycle",
            WorkspaceKind::Porter => "porter",
            WorkspaceKind::EngineBaseline => "engine-baseline",
            WorkspaceKind::Ephemeral => "ephemeral",
        }
    }
}

/// 工作树起点。
#[derive(Debug, Clone)]
pub enum BaseRef {
    Commit(String),
    Branch(String),
    Tag(String),
}

impl BaseRef {
    fn rev(&self) -> &str {
        match self {
            BaseRef::Commit(s) | BaseRef::Branch(s) | BaseRef::Tag(s) => s,
        }
    }
}

/// 一棵已分配的工作树。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub id: String,
    pub path: PathBuf,
    pub kind: WorkspaceKind,
    pub base: String,
}

impl Workspace {
    pub fn is_persistent(&self) -> bool {
        self.kind.is_persistent()
    }
}

/// 分配请求。
#[derive(Debug, Clone)]
pub struct WorkspaceSpec {
    pub id: String,
    pub kind: WorkspaceKind,
    pub base: BaseRef,
    pub task_id: Option<String>,
}

impl WorkspaceSpec {
    /// 常驻工作树请求（部署器 / 引擎基线 / 按实例的移植器）。
    pub fn persistent(id: impl Into<String>, kind: WorkspaceKind, base: BaseRef) -> Self {
        Self {
            id: id.into(),
            kind,
            base,
            task_id: None,
        }
    }
}

/// `git worktree list --porcelain` 的一条记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeEntry {
    pub path: PathBuf,
    pub head: String,
    pub branch: Option<String>,
    pub detached: bool,
    pub locked: bool,
    pub bare: bool,
}

/// 工作树属主边车。刻意放在工作树**之外**：树内任何多余文件都会让
/// `git status --porcelain` 非空，触发调用方的 clean 检查 fail-closed 拒绝。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceMeta {
    id: String,
    kind: WorkspaceKind,
    pid: u32,
    created_at_unix: i64,
    base: String,
    #[serde(default)]
    task_id: Option<String>,
}

/// 工作树分配器。裸仓库是唯一仓库源，所有工作树共享其对象库与 refs。
pub struct WorkspaceManager {
    bare_repo: PathBuf,
    root: PathBuf,
    target_dir: PathBuf,
    meta_dir: PathBuf,
    ephemeral_ttl: Duration,
}

impl WorkspaceManager {
    pub fn new(
        bare_repo: impl Into<PathBuf>,
        root: impl Into<PathBuf>,
        target_dir: impl Into<PathBuf>,
    ) -> Self {
        let root = root.into();
        let meta_dir = root.join(".meta");
        Self {
            bare_repo: bare_repo.into(),
            root,
            target_dir: target_dir.into(),
            meta_dir,
            ephemeral_ttl: DEFAULT_EPHEMERAL_TTL,
        }
    }

    /// 覆盖临时工作树存活上限（回收策略随分配器走，各子系统用同一个值）。
    pub fn with_ephemeral_ttl(mut self, ttl: Duration) -> Self {
        self.ephemeral_ttl = ttl;
        self
    }

    pub fn ephemeral_ttl(&self) -> Duration {
        self.ephemeral_ttl
    }

    pub fn bare_repo(&self) -> &Path {
        &self.bare_repo
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 构建产物目录：所有工作树共用，跨工作树重建保持增量缓存。
    pub fn target_dir(&self) -> &Path {
        &self.target_dir
    }

    pub fn path_for(&self, id: &str) -> PathBuf {
        self.root.join(sanitize_id(id))
    }

    /// 部署器独占的稳定路径工作树。
    pub fn deployer_workspace(&self) -> PathBuf {
        self.path_for("mainline")
    }

    /// 引擎只读基线工作树（稳定路径，就地刷新）。
    pub fn engine_baseline_workspace(&self) -> PathBuf {
        self.path_for("engine-baseline")
    }

    /// 某实例每轮演进独占的工作树（稳定路径，就地刷新）。同一实例的轮次串行，
    /// 故复用同一棵；跨实例各有一棵，互不干扰。
    pub fn cycle_workspace(&self, instance: &str) -> PathBuf {
        self.path_for(&format!("cycle-{instance}"))
    }

    /// 移植器按实例常驻的工作树路径。
    pub fn porter_workspace(&self, instance: &str) -> PathBuf {
        self.path_for(&format!("porter-{instance}"))
    }

    /// 解析工作树基线：优先基于当前版本的移植分支，其次该版本 tag，最后 main。
    ///
    /// `evol/<instance>` 只有在包含当前版本 tag 时才可用——否则它是上一版本的
    /// 残留分支，切过去等于把基线整体回退。裸仓库不可达时退化为 main，与
    /// 既有「无 tag 就用本地已有状态」的降级一致。
    pub async fn resolve_base(&self, instance: &str, version: &str) -> BaseRef {
        let tag = format!("v{}", version.trim_start_matches('v'));
        let branch = format!("evol/{instance}");
        let branch_ref = format!("refs/heads/{branch}");
        if self.rev_exists(&branch_ref).await
            && self.rev_exists(&format!("refs/tags/{tag}")).await
            && self
                .git_bare(&["merge-base", "--is-ancestor", &tag, &branch_ref])
                .await
                .is_ok()
        {
            return BaseRef::Branch(branch);
        }
        if self.rev_exists(&format!("refs/tags/{tag}")).await {
            return BaseRef::Tag(tag);
        }
        BaseRef::Branch("main".into())
    }

    async fn rev_exists(&self, rev: &str) -> bool {
        self.git_bare(&["rev-parse", "--verify", "--quiet", rev])
            .await
            .is_ok()
    }

    /// 常驻工作树：不存在 / 损坏 / 未登记则按 `spec.base` 重建；已存在则只补
    /// `local` 远程，**不动其 HEAD**——移植器等工作树带着自己的分支状态跨轮次，
    /// 需要移动时由调用方显式 [`WorkspaceManager::refresh`]。
    pub async fn ensure_persistent(&self, spec: WorkspaceSpec) -> SFResult<Workspace> {
        let path = self.path_for(&spec.id);
        if self.worktree_healthy(&path).await {
            self.ensure_local_remote(&path).await?;
            return Ok(self.workspace_of(&spec, &path));
        }
        info!(id = %spec.id, path = %path.display(), "workspace missing or corrupt; recreating");
        self.force_remove_path(&path).await;
        self.add_worktree(&spec, &path).await?;
        Ok(self.workspace_of(&spec, &path))
    }

    /// 临时工作树：id 带 uuid 后缀，同一 task_id 并发取也不会撞路径或 git 索引。
    pub async fn acquire_ephemeral(&self, task_id: &str, base: BaseRef) -> SFResult<Workspace> {
        let id = format!(
            "{}-{}",
            sanitize_id(task_id),
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );
        let spec = WorkspaceSpec {
            id,
            kind: WorkspaceKind::Ephemeral,
            base,
            task_id: Some(task_id.to_string()),
        };
        let path = self.path_for(&spec.id);
        self.add_worktree(&spec, &path).await?;
        Ok(self.workspace_of(&spec, &path))
    }

    /// 归还工作树。常驻类型是 no-op（生命周期与进程同长）。
    pub async fn release(&self, ws: &Workspace) -> SFResult<()> {
        if ws.is_persistent() {
            return Ok(());
        }
        self.force_remove_path(&ws.path).await;
        self.remove_meta(&ws.id);
        Ok(())
    }

    /// 就地移动到 `base`：`reset --hard` + `clean -ffdx`。target 目录外置，
    /// 故 `-x` 不会误删编译缓存，工作树可以放心回到干净基线。
    pub async fn refresh(&self, ws: &Workspace, base: BaseRef) -> SFResult<()> {
        self.git_in(&ws.path, &["reset", "--hard", base.rev()])
            .await?;
        self.git_in(&ws.path, &["clean", "-ffdx"]).await?;
        Ok(())
    }

    pub async fn list(&self) -> SFResult<Vec<WorktreeEntry>> {
        let out = self.git_bare(&["worktree", "list", "--porcelain"]).await?;
        // 裸仓库自身也会出现在 git 的列表里，但它不是分配出去的工作树。
        Ok(parse_worktree_list(&out)
            .into_iter()
            .filter(|e| !e.bare)
            .collect())
    }

    pub async fn prune(&self) -> SFResult<()> {
        self.git_bare(&["worktree", "prune"]).await.map(|_| ())
    }

    /// 回收泄漏的临时工作树：属主进程已死，或存活时间已达 TTL（进程仍在但任务
    /// 崩掉的唯一兜底）。常驻工作树永不回收。
    pub async fn gc_stale(&self) -> SFResult<Vec<PathBuf>> {
        let mut reclaimed = Vec::new();
        let ttl = self.ephemeral_ttl;
        let now = chrono::Utc::now().timestamp();
        for entry in self.list().await.unwrap_or_default() {
            if !entry.path.starts_with(&self.root) {
                continue;
            }
            let Some(id) = entry.path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some(meta) = self.read_meta(id) else {
                continue;
            };
            if meta.kind.is_persistent() {
                continue;
            }
            let age = (now - meta.created_at_unix).max(0) as u64;
            let alive = pid_alive(meta.pid);
            if !alive || age >= ttl.as_secs() {
                info!(
                    id = %id,
                    kind = meta.kind.as_str(),
                    age_secs = age,
                    pid_alive = alive,
                    "reclaiming leaked workspace"
                );
                self.force_remove_path(&entry.path).await;
                self.remove_meta(id);
                reclaimed.push(entry.path.clone());
            }
        }
        Ok(reclaimed)
    }

    /// 回收"实例身份已轮换"遗留的常驻工作树。
    ///
    /// 一个进程只有一个当前实例，按实例命名的 `cycle-*` / `porter-*` 树在实例
    /// 轮换后就是没人再用的残留；单例的 `mainline` / `engine-baseline` 不在此列。
    /// 加最小存活时长是因为滚动更新期间新旧 Pod 会短暂共存、共享同一个工作树根，
    /// 旧 Pod 正在使用的工作树必须活过这个窗口。
    pub async fn gc_orphan_instances(
        &self,
        instance: &str,
        min_age: Duration,
    ) -> SFResult<Vec<PathBuf>> {
        let keep = [
            sanitize_id(&format!("cycle-{instance}")),
            sanitize_id(&format!("porter-{instance}")),
        ];
        let mut reclaimed = Vec::new();
        let now = chrono::Utc::now().timestamp();
        for entry in self.list().await.unwrap_or_default() {
            if !entry.path.starts_with(&self.root) {
                continue;
            }
            let Some(id) = entry.path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if keep.iter().any(|k| k == id) {
                continue;
            }
            let Some(meta) = self.read_meta(id) else {
                continue;
            };
            if !matches!(meta.kind, WorkspaceKind::Cycle | WorkspaceKind::Porter) {
                continue;
            }
            let age = (now - meta.created_at_unix).max(0) as u64;
            if age < min_age.as_secs() {
                continue;
            }
            info!(
                id = %id,
                kind = meta.kind.as_str(),
                age_secs = age,
                "reclaiming workspace of a rotated instance identity"
            );
            self.force_remove_path(&entry.path).await;
            self.remove_meta(id);
            reclaimed.push(entry.path.clone());
        }
        Ok(reclaimed)
    }

    /// 回收"实例身份已轮换"遗留的 `evol/<旧id>` 分支。
    ///
    /// 移植工作分支以实例身份命名，身份一换名就再没有任何代码读它，但 ref
    /// 会连同其可达提交永久留在裸仓库里——每轮换一次身份就多一条。回收判据
    /// 二选一，缺一不删：
    ///
    /// - 已完全并入 main：活干完了，删掉不丢任何提交；
    /// - 末次提交已超过 ttl：所属实例长期不活动（存活实例的分支每次移植都会被
    ///   强推刷新），留着只占 ref 与可达对象。
    ///
    /// 当前实例自己的分支永不删；正被工作树占用的分支由 git 自身拒绝删除，
    /// 记录后跳过。`main` / `master` 之类不带 `evol/` 前缀的 ref 一律不在此列。
    pub async fn gc_orphan_evol_branches(
        &self,
        current_instance: &str,
        ttl: Duration,
    ) -> SFResult<Vec<String>> {
        let keep = format!("evol/{current_instance}");
        let out = self
            .git_bare(&[
                "for-each-ref",
                "--format=%(refname:short) %(objectname) %(committerdate:unix)",
                "refs/heads/evol/",
            ])
            .await?;
        let now = chrono::Utc::now().timestamp();
        let mut reclaimed = Vec::new();
        for line in out.lines() {
            let mut parts = line.split_whitespace();
            let (Some(name), Some(rev), Some(ts)) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            if name == keep {
                continue;
            }
            let merged = self
                .git_bare(&["merge-base", "--is-ancestor", rev, "main"])
                .await
                .is_ok();
            let age = (now - ts.parse::<i64>().unwrap_or(now)).max(0) as u64;
            if !merged && age < ttl.as_secs() {
                continue;
            }
            match self.git_bare(&["branch", "-D", name]).await {
                Ok(_) => {
                    info!(
                        branch = %name,
                        rev = %&rev[..rev.len().min(12)],
                        age_secs = age,
                        reason = if merged { "merged-into-main" } else { "stale" },
                        "reclaimed orphan evolution branch"
                    );
                    reclaimed.push(name.to_string());
                }
                Err(e) => {
                    tracing::warn!(
                        branch = %name,
                        error = %e,
                        "orphan evolution branch could not be deleted; leaving it in place"
                    );
                }
            }
        }
        Ok(reclaimed)
    }

    // -- 内部 ---------------------------------------------------------------

    fn workspace_of(&self, spec: &WorkspaceSpec, path: &Path) -> Workspace {
        Workspace {
            id: spec.id.clone(),
            path: path.to_path_buf(),
            kind: spec.kind,
            base: spec.base.rev().to_string(),
        }
    }

    async fn add_worktree(&self, spec: &WorkspaceSpec, path: &Path) -> SFResult<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                SFError::IO(format!("create workspaces root {}: {e}", parent.display()))
            })?;
        }
        let meta = WorkspaceMeta {
            id: spec.id.clone(),
            kind: spec.kind,
            pid: std::process::id(),
            created_at_unix: chrono::Utc::now().timestamp(),
            base: spec.base.rev().to_string(),
            task_id: spec.task_id.clone(),
        };
        // 边车先于工作树落地：崩溃后仍有登记可回收。反过来（先 add 后写）在
        // add 与写之间崩溃就会留下无主工作树，永远回收不掉。
        self.write_meta(&meta).await?;
        let path_s = path.to_str().unwrap_or("").to_string();
        if let Err(e) = self
            .git_bare(&["worktree", "add", "--detach", &path_s, spec.base.rev()])
            .await
        {
            self.remove_meta(&spec.id);
            return Err(e);
        }
        self.register_safe_directory(path).await;
        self.ensure_local_remote(path).await?;
        Ok(())
    }

    async fn worktree_healthy(&self, path: &Path) -> bool {
        if self
            .git_in(path, &["rev-parse", "--git-dir"])
            .await
            .is_err()
        {
            return false;
        }
        self.list()
            .await
            .map(|entries| entries.iter().any(|e| e.path == path))
            .unwrap_or(false)
    }

    /// 工作树里幂等补 `local` 远程（指向裸仓库本身）。各消费者沿用既有的
    /// `fetch local <branch>` / `reset --hard local/<branch>` / `push local ...`
    /// 语义，不必为工作树改写 git 流程。远程配置存在公共 config 里，同一
    /// 仓库的工作树共享，故只在首棵时真正写入。
    async fn ensure_local_remote(&self, path: &Path) -> SFResult<()> {
        if self
            .git_in(path, &["remote", "get-url", "local"])
            .await
            .is_ok()
        {
            return Ok(());
        }
        self.git_in(
            path,
            &[
                "remote",
                "add",
                "local",
                self.bare_repo.to_str().unwrap_or(""),
            ],
        )
        .await?;
        Ok(())
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

    /// 从登记中移除工作树并删目录；对已消失的路径同样安全（幂等）。
    async fn force_remove_path(&self, path: &Path) {
        let _ = self
            .git_bare(&["worktree", "remove", "--force", path.to_str().unwrap_or("")])
            .await;
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            let _ = tokio::fs::remove_dir_all(path).await;
        }
        let _ = self.prune().await;
    }

    fn meta_path(&self, id: &str) -> PathBuf {
        self.meta_dir.join(format!("{}.json", sanitize_id(id)))
    }

    async fn write_meta(&self, meta: &WorkspaceMeta) -> SFResult<()> {
        tokio::fs::create_dir_all(&self.meta_dir)
            .await
            .map_err(|e| SFError::IO(format!("create meta dir: {e}")))?;
        let text = serde_json::to_string_pretty(meta)?;
        tokio::fs::write(self.meta_path(&meta.id), text)
            .await
            .map_err(|e| SFError::IO(format!("write workspace meta: {e}")))?;
        Ok(())
    }

    fn read_meta(&self, id: &str) -> Option<WorkspaceMeta> {
        std::fs::read_to_string(self.meta_path(id))
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
    }

    fn remove_meta(&self, id: &str) {
        let _ = std::fs::remove_file(self.meta_path(id));
    }

    async fn run_git(&self, args: &[&str], workdir: Option<&Path>) -> SFResult<String> {
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

    /// 在裸仓库上执行（不依赖任何工作树）。
    async fn git_bare(&self, args: &[&str]) -> SFResult<String> {
        let mut full: Vec<&str> = vec!["--git-dir", self.bare_repo.to_str().unwrap_or("")];
        full.extend_from_slice(args);
        self.run_git(&full, None).await
    }

    /// 在一棵工作树内执行。
    async fn git_in(&self, path: &Path, args: &[&str]) -> SFResult<String> {
        self.run_git(args, Some(path)).await
    }
}

/// 工作树 id 只保留文件系统安全字符；空结果回退占位符。
fn sanitize_id(id: &str) -> String {
    let cleaned: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('-').trim_matches('.').to_string();
    if trimmed.is_empty() {
        "ws".to_string()
    } else {
        trimmed
    }
}

fn pid_alive(pid: u32) -> bool {
    pid != 0 && Path::new(&format!("/proc/{pid}")).exists()
}

/// 解析 `git worktree list --porcelain`：记录之间以空行分隔，每条以
/// `worktree <path>` 开头。
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
                head: String::new(),
                branch: None,
                detached: false,
                locked: false,
                bare: false,
            });
        } else if let Some(entry) = cur.as_mut() {
            if let Some(v) = line.strip_prefix("HEAD ") {
                entry.head = v.to_string();
            } else if let Some(v) = line.strip_prefix("branch ") {
                entry.branch = Some(v.trim_start_matches("refs/heads/").to_string());
            } else if line == "detached" {
                entry.detached = true;
            } else if line == "bare" {
                entry.bare = true;
            } else if line == "locked" || line.starts_with("locked ") {
                entry.locked = true;
            }
        }
    }
    if let Some(entry) = cur {
        out.push(entry);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(dir: &Path, args: &[&str]) {
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
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// 造一个带两个提交的裸仓库，返回 (裸仓库路径, rev_a, rev_b)。
    fn seed_bare(root: &Path) -> (PathBuf, String, String) {
        let work = root.join("seed");
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-q", "-b", "main"]);
        std::fs::write(work.join("a.txt"), "a\n").unwrap();
        git(&work, &["add", "-A"]);
        git(&work, &["commit", "-qm", "a"]);
        let rev_a = git_out(&work, &["rev-parse", "HEAD"]);
        std::fs::write(work.join("b.txt"), "b\n").unwrap();
        git(&work, &["add", "-A"]);
        git(&work, &["commit", "-qm", "b"]);
        let rev_b = git_out(&work, &["rev-parse", "HEAD"]);

        let bare = root.join("bare.git");
        let out = std::process::Command::new("git")
            .args([
                "clone",
                "-q",
                "--bare",
                work.to_str().unwrap(),
                bare.to_str().unwrap(),
            ])
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(out.status.success(), "bare clone failed");
        (bare, rev_a, rev_b)
    }

    fn manager(root: &Path, bare: &Path) -> WorkspaceManager {
        WorkspaceManager::new(bare, root.join("workspaces"), root.join("target"))
    }

    #[tokio::test]
    async fn persistent_workspace_created_and_reused() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);

        let spec = WorkspaceSpec::persistent(
            "mainline",
            WorkspaceKind::Deployer,
            BaseRef::Commit(rev_b.clone()),
        );
        let ws = mgr.ensure_persistent(spec.clone()).await.unwrap();
        assert_eq!(ws.path, mgr.deployer_workspace());
        assert!(ws.path.join("a.txt").exists());
        assert_eq!(git_out(&ws.path, &["rev-parse", "HEAD"]), rev_b);

        // 第二次调用复用同一棵树（不重建、不丢 HEAD 偏移）。
        let spec_a = WorkspaceSpec::persistent(
            "mainline",
            WorkspaceKind::Deployer,
            BaseRef::Commit(rev_b.clone()),
        );
        let again = mgr.ensure_persistent(spec_a).await.unwrap();
        assert_eq!(again.path, ws.path);
        assert_eq!(mgr.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn corrupt_workspace_is_recreated() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        let spec = WorkspaceSpec::persistent(
            "mainline",
            WorkspaceKind::Deployer,
            BaseRef::Commit(rev_b.clone()),
        );
        let ws = mgr.ensure_persistent(spec.clone()).await.unwrap();
        std::fs::remove_dir_all(&ws.path).unwrap();

        let rebuilt = mgr.ensure_persistent(spec).await.unwrap();
        assert!(rebuilt.path.join("a.txt").exists());
        assert_eq!(git_out(&rebuilt.path, &["rev-parse", "HEAD"]), rev_b);
    }

    #[tokio::test]
    async fn ephemeral_ids_are_unique_per_task() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);

        let w1 = mgr
            .acquire_ephemeral("cycle", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();
        let w2 = mgr
            .acquire_ephemeral("cycle", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();
        assert_ne!(w1.path, w2.path, "同 task_id 并发取必须得到不同路径");
        assert_eq!(mgr.list().await.unwrap().len(), 2);

        mgr.release(&w1).await.unwrap();
        assert!(!w1.path.exists());
        assert_eq!(mgr.list().await.unwrap().len(), 1, "登记也要一并移除");
    }

    #[tokio::test]
    async fn release_removes_worktree_registration_and_sidecar() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        let ws = mgr
            .acquire_ephemeral("cycle", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();
        let meta = mgr.meta_path(&ws.id);
        assert!(meta.exists(), "acquire 必须先写属主边车再建树");

        mgr.release(&ws).await.unwrap();
        assert!(!ws.path.exists(), "工作树目录要删掉");
        assert!(!meta.exists(), "属主边车要删掉");
        assert!(mgr.list().await.unwrap().is_empty(), "登记要清空");
        // 幂等：重复归还不报错。
        mgr.release(&ws).await.unwrap();
    }

    #[tokio::test]
    async fn gc_reclaims_leaked_ephemeral_but_spares_persistent() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare).with_ephemeral_ttl(Duration::ZERO);

        let kept = mgr
            .ensure_persistent(WorkspaceSpec::persistent(
                "mainline",
                WorkspaceKind::Deployer,
                BaseRef::Commit(rev_b.clone()),
            ))
            .await
            .unwrap();
        let leaked = mgr
            .acquire_ephemeral("cycle", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();

        let reclaimed = mgr.gc_stale().await.unwrap();
        assert_eq!(reclaimed, vec![leaked.path.clone()]);
        assert!(!leaked.path.exists(), "泄漏工作树目录要删掉");
        assert!(kept.path.exists(), "常驻工作树不能被动");
        assert_eq!(mgr.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn refresh_moves_worktree_and_keeps_target_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, rev_a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        let ws = mgr
            .acquire_ephemeral("cycle", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();
        // 模拟编译产物：target 外置，refresh 的 clean -ffdx 不能碰它。
        std::fs::create_dir_all(mgr.target_dir()).unwrap();
        std::fs::write(mgr.target_dir().join("marker"), "keep").unwrap();

        mgr.refresh(&ws, BaseRef::Commit(rev_a.clone()))
            .await
            .unwrap();
        assert_eq!(git_out(&ws.path, &["rev-parse", "HEAD"]), rev_a);
        assert!(
            !ws.path.join("b.txt").exists(),
            "回到 rev_a 后 b.txt 应消失"
        );
        assert!(mgr.target_dir().join("marker").exists(), "编译缓存要保住");
    }

    #[tokio::test]
    async fn cycle_workspace_is_reused_across_rounds() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, rev_a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        let spec =
            |base: BaseRef| WorkspaceSpec::persistent("cycle-quinn", WorkspaceKind::Cycle, base);

        let first = mgr
            .ensure_persistent(spec(BaseRef::Commit(rev_a.clone())))
            .await
            .unwrap();
        assert_eq!(first.path, mgr.cycle_workspace("quinn"));
        // 轮末把变更留在树里：下一轮必须回到基线，而不是新建一棵。
        std::fs::write(first.path.join("leftover.txt"), "dirty").unwrap();

        let second = mgr
            .ensure_persistent(spec(BaseRef::Commit(rev_b.clone())))
            .await
            .unwrap();
        assert_eq!(
            second.path, first.path,
            "同一实例必须复用同一路径，换路径会丢编译缓存"
        );
        assert_eq!(mgr.list().await.unwrap().len(), 1);
        mgr.refresh(&second, BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();
        assert!(!second.path.join("leftover.txt").exists());
        assert_eq!(git_out(&second.path, &["rev-parse", "HEAD"]), rev_b);
    }

    #[tokio::test]
    async fn cycle_workspaces_are_isolated_per_instance() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        let a = mgr
            .ensure_persistent(WorkspaceSpec::persistent(
                "cycle-quinn",
                WorkspaceKind::Cycle,
                BaseRef::Commit(rev_b.clone()),
            ))
            .await
            .unwrap();
        let b = mgr
            .ensure_persistent(WorkspaceSpec::persistent(
                "cycle-other",
                WorkspaceKind::Cycle,
                BaseRef::Commit(rev_b.clone()),
            ))
            .await
            .unwrap();
        assert_ne!(a.path, b.path);
        assert_eq!(mgr.list().await.unwrap().len(), 2);
        // 常驻类型不参与回收，GC 不许动它们。
        assert!(mgr.gc_stale().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn gc_reclaims_rotated_instance_trees_only() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        async fn persistent(
            mgr: &WorkspaceManager,
            id: &str,
            kind: WorkspaceKind,
            rev: &str,
        ) -> Workspace {
            mgr.ensure_persistent(WorkspaceSpec::persistent(
                id,
                kind,
                BaseRef::Commit(rev.to_string()),
            ))
            .await
            .unwrap()
        }

        // 单例：不随实例轮换，任何情况都不许回收。
        let mainline = persistent(&mgr, "mainline", WorkspaceKind::Deployer, &rev_b).await;
        let baseline = persistent(
            &mgr,
            "engine-baseline",
            WorkspaceKind::EngineBaseline,
            &rev_b,
        )
        .await;
        // 当前实例：正在用。
        let cycle_now = persistent(&mgr, "cycle-quinn", WorkspaceKind::Cycle, &rev_b).await;
        let porter_now = persistent(&mgr, "porter-quinn", WorkspaceKind::Porter, &rev_b).await;
        // 已轮换实例的残留：按实例命名，没人再用。
        let cycle_old = persistent(&mgr, "cycle-ada", WorkspaceKind::Cycle, &rev_b).await;
        let porter_old = persistent(&mgr, "porter-ada", WorkspaceKind::Porter, &rev_b).await;
        // 同样已轮换、但刚建出来：处在滚动更新共存窗口内，这一轮不回收。
        let cycle_fresh = persistent(&mgr, "cycle-max", WorkspaceKind::Cycle, &rev_b).await;

        // 把三棵"旧"树的边车时间戳推早，模拟身份轮换后的既存残留。
        let old = chrono::Utc::now().timestamp() - ORPHAN_MIN_AGE.as_secs() as i64 - 60;
        for ws in [&cycle_old, &porter_old] {
            let kind = mgr.read_meta(&ws.id).unwrap().kind;
            mgr.write_meta(&WorkspaceMeta {
                id: ws.id.clone(),
                kind,
                pid: std::process::id(),
                created_at_unix: old,
                base: rev_b.clone(),
                task_id: None,
            })
            .await
            .unwrap();
        }

        let reclaimed = mgr
            .gc_orphan_instances("quinn", ORPHAN_MIN_AGE)
            .await
            .unwrap();
        let mut got: Vec<String> = reclaimed
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        got.sort();
        assert_eq!(got, vec!["cycle-ada", "porter-ada"]);
        assert!(!cycle_old.path.exists(), "轮换实例的树要删掉");
        assert!(!porter_old.path.exists(), "轮换实例的树要删掉");
        for kept in [&mainline, &baseline, &cycle_now, &porter_now, &cycle_fresh] {
            assert!(kept.path.exists(), "{} 不能被动", kept.id);
        }
        assert_eq!(mgr.list().await.unwrap().len(), 5);
    }

    /// 在裸仓库里执行并断言成功，返回 stdout。
    fn bare(dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// 把 name 指向 parent 之上的一个新提交——不并入 main 的分支。
    fn branch_with_wip_commit(bare_dir: &Path, name: &str, parent: &str) {
        let tree = bare(bare_dir, &["rev-parse", &format!("{parent}^{{tree}}")]);
        let new = bare(bare_dir, &["commit-tree", &tree, "-p", parent, "-m", "wip"]);
        bare(bare_dir, &["branch", "-f", name, &new]);
    }

    #[tokio::test]
    async fn orphan_evol_branches_reclaimed_only_when_merged_or_stale() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare_repo, rev_a, _rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare_repo);

        // 当前实例：即使落后于 main 也不许动，下一轮移植还要强推它。
        bare(&bare_repo, &["branch", "evol/quinn", &rev_a]);
        // 旧实例，活干完了：已完全并入 main。
        bare(&bare_repo, &["branch", "evol/ada", &rev_a]);
        // 旧实例，未并入但刚提交：处在活跃窗口内，这一轮保留。
        branch_with_wip_commit(&bare_repo, "evol/max", &rev_a);

        let reclaimed = mgr
            .gc_orphan_evol_branches("quinn", Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(reclaimed, vec!["evol/ada"]);
        assert!(git_out(&bare_repo, &["branch", "--list", "evol/ada"]).is_empty());
        assert!(!git_out(&bare_repo, &["branch", "--list", "evol/quinn"]).is_empty());
        assert!(!git_out(&bare_repo, &["branch", "--list", "evol/max"]).is_empty());

        // ttl 归零：未并入的旧分支随即按超期回收。
        let reclaimed = mgr
            .gc_orphan_evol_branches("quinn", Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(reclaimed, vec!["evol/max"]);
        assert!(git_out(&bare_repo, &["branch", "--list", "evol/max"]).is_empty());
        assert!(!git_out(&bare_repo, &["branch", "--list", "evol/quinn"]).is_empty());
        // 非 `evol/` 前缀的 ref 不在扫描范围内。
        assert!(!git_out(&bare_repo, &["branch", "--list", "main"]).is_empty());
    }

    #[tokio::test]
    async fn local_remote_is_wired_for_consumers() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        let ws = mgr
            .acquire_ephemeral("cycle", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();

        // 消费者沿用 `fetch local main` → `reset --hard local/main` 的既有语义。
        let status = std::process::Command::new("git")
            .args(["fetch", "-q", "local", "main"])
            .current_dir(&ws.path)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .status()
            .unwrap();
        assert!(status.success(), "fetch local main 必须可用");
        assert!(!git_out(&ws.path, &["rev-parse", "local/main"]).is_empty());
    }

    /// 在裸仓库上直接执行 git（造 tag / 分支用）。
    fn in_bare(bare: &Path, args: &[&str]) {
        let mut full: Vec<&str> = vec!["--git-dir", bare.to_str().unwrap()];
        full.extend_from_slice(args);
        let status = std::process::Command::new("git")
            .args(&full)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }

    #[tokio::test]
    async fn resolve_base_prefers_versioned_evol_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, rev_a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);

        // 既无 tag 也无移植分支 → main。
        assert!(matches!(
            mgr.resolve_base("local", "0.5.7").await,
            BaseRef::Branch(b) if b == "main"
        ));

        // 只有 tag → 用 tag。
        in_bare(&bare, &["tag", "v0.5.7", &rev_b]);
        assert!(matches!(
            mgr.resolve_base("local", "0.5.7").await,
            BaseRef::Tag(t) if t == "v0.5.7"
        ));

        // 移植分支不含该 tag（停在 tag 之前的提交）→ 仍退回 tag。
        in_bare(&bare, &["branch", "evol/local", &rev_a]);
        assert!(matches!(
            mgr.resolve_base("local", "0.5.7").await,
            BaseRef::Tag(t) if t == "v0.5.7"
        ));

        // 移植分支包含该 tag（已在新基线上移植过）→ 用移植分支。
        in_bare(&bare, &["branch", "-f", "evol/local", &rev_b]);
        assert!(matches!(
            mgr.resolve_base("local", "0.5.7").await,
            BaseRef::Branch(b) if b == "evol/local"
        ));

        // 版本前进但没有对应 tag/分支 → main。
        assert!(matches!(
            mgr.resolve_base("local", "0.6.0").await,
            BaseRef::Branch(b) if b == "main"
        ));
    }

    #[test]
    fn parses_porcelain_worktree_list() {
        let text = "worktree /a/bare.git\nHEAD abc\nbare\n\nworktree /a/ws/mainline\nHEAD def\nbranch refs/heads/main\n\nworktree /a/ws/task-1\nHEAD 123\ndetached\nlocked\n";
        let entries = parse_worktree_list(text);
        assert_eq!(entries.len(), 3);
        assert!(entries[0].bare);
        assert_eq!(entries[1].branch.as_deref(), Some("main"));
        assert!(entries[2].detached);
        assert!(entries[2].locked);
    }

    #[test]
    fn sanitizes_ids_to_filesystem_safe() {
        assert_eq!(sanitize_id("evol/quinn-33b30bf1"), "evol-quinn-33b30bf1");
        assert_eq!(sanitize_id("porter::local"), "porter--local");
        assert_eq!(sanitize_id("///"), "ws");
    }
}
