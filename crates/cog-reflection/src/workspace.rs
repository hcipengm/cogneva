//! principle: feedback_a_registration_without_a_symmetric_removal_grows_a_shared_file_forever
//!
//! 沙盒工作区动态分配。
//!
//! 工作树层面的失败（取树、回收、git 起不来）是环境问题：变更本身没有被判定过，重试有意义。
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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cog_core::retention::{ttl_for_half_turnover, RetentionReadings, UNIT_SECONDS};
use cog_core::{SFError, SFResult};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// 一棵常驻工作树的下一次 `reset --hard` 会重写多少个已跟踪文件——索引没记住的
/// 都算在内，健康时为 0。
pub use cog_core::metric_names::WORKTREE_INDEX_MISSING_FILES as WORKTREE_INDEX_MISSING_FILES_METRIC;

/// 这棵工作树的索引文件是否在。0 与「在、但一个条目都没有」会让上一条都等于全树
/// 大小，这一条把两者分开：前者是文件被删或没挂上，后者是写到一半被截断。
pub use cog_core::metric_names::WORKTREE_INDEX_PRESENT as WORKTREE_INDEX_PRESENT_METRIC;

/// 这一棵工作树的**这次采样尝试**有没有取到数：取到记 0，任一条 git 读数失败记 1。
///
/// 上一条读数只在两笔 git 读数都成功时才盖章，所以"索引在、但读不出来"会让它停在
/// 上一笔（健康的）值上；读它的规则按伴生钟年龄判写者还在不在，而伴生钟冻住之后
/// 规则是**变静**而不是变红——「采样取不到」与「索引没事」于是在面上同形。这一格
/// 就是那件事自己的读数。
pub use cog_core::metric_names::WORKTREE_INDEX_SAMPLE_INCOMPLETE as WORKTREE_INDEX_SAMPLE_INCOMPLETE_METRIC;

/// 一棵常驻工作树的索引读数多久必须重盖一次章。
///
/// 这棵树只在**即将被 `reset --hard`** 的那一刻被采（见
/// [`WorkspaceManager::sample_index_health`]），于是两笔采样之间的空档是"两次 reset
/// 之间的空档"，不是写者自己的节奏：部署器没事可做时它一停几小时，而它报的那个索引
/// 状态在这几小时里一个字都没变。读这条读数的告警规则按伴生钟的年龄判"写者还在不在"，
/// 空档有多长规则就瞎多久——实测两棵长命树（部署器树、引擎基线树）在共享表留存的那
/// 两天里，采样间隔中位数 42 分钟、最长 5.7 小时，而规则给的界是三小时，也就是每棵树
/// 每天有一两次连着约两个半小时的静默，静默期间当值还在。心跳把这个空档封在写者自己
/// 的钟上：一轮没动也盖一次章，界于是可以对一条已知的节奏比较，而不是对"没事就不写"猜。
///
/// 值与普查、耐久池两处同拍：一个"没变但仍在被观测"的节奏全系统一个值——所以这个值
/// 只有一处声明，在三处之间搬一份数字就是在等它们漂开。
pub use cog_core::reading_cadence::UNCHANGED_READING_HEARTBEAT as INDEX_SAMPLE_HEARTBEAT;

/// 这棵常驻工作树的索引现在要不要重采一次。
///
/// 与普查那条同形状（`census_needs_write`），少的是"值变了"那一半：值由 git 现读，
/// 读一次就知道，缓存一份计数反而会拿旧数去比。`last` 是本进程上一笔真写下去的时刻，
/// `None` 表示本进程还没采过——那一定要采，缺席的一格要先被填上。
fn index_sample_needs_write(
    last: Option<std::time::Instant>,
    now: std::time::Instant,
    heartbeat: Duration,
) -> bool {
    match last {
        None => true,
        Some(at) => now.duration_since(at) >= heartbeat,
    }
}

/// 工作树仓库里上游主线的引用名（`local` 远程的 `main`）。与
/// `change_pipeline::sync_with_upstream_in` 对齐到的那个引用是同一个。
const MAINLINE_REF: &str = "local/main";

/// 从 git 失败输出里提取锁文件路径。git 的固定措辞：
/// `fatal: Unable to create '<path>/index.lock': File exists.`
/// 只认 `.lock` 结尾的路径，防止把别的引号串误当锁删除。
pub(crate) fn stale_lock_candidate(error_text: &str) -> Option<PathBuf> {
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

/// 锁文件年龄达到 `max_age` 才删除（防止误删并发活锁）。返回是否删掉。
pub(crate) async fn clear_stale_git_lock(lock: &Path, max_age: Duration) -> bool {
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

/// 临时工作树存活上限的**种子**。要容得下一整轮演进：属主 pid 是 Pod 进程号，
/// 同一进程内永远"活着"，存活时长才是唯一的兜底判据。
///
/// 实际生效的界每拍按实测重算（卷还装得下多少棵树 × 新树到达间隔 / 2），并把
/// 已归还工作树里最长的一趟当地板——量不到卷或到达率时才落回这个数。
// cap: bound — 种子：量不到卷与到达率时的退路，生效的界每拍按实测重算并以已归还的最长一趟当地板
pub const DEFAULT_EPHEMERAL_TTL: Duration = Duration::from_secs(21600);

/// 孤儿分支存活上限的**种子**：未并入 main、且所属实例已经轮换掉的 `evol/<id>`
/// 分支留多久。实际生效的界每拍按实测重算（裸仓库还能装下多少条分支的独有
/// 对象 × 新实例分支到达间隔 / 2）。
// cap: bound — 种子：量不到仓库容量或分支到达率时的退路，生效的界每拍按实测重算
pub const DEFAULT_ORPHAN_BRANCH_TTL: Duration = Duration::from_secs(2_592_000);

/// 临时工作树这一条保留回路在登记表里的 key，与 `evolution.workspaces.ephemeralTtlSecs`
/// 同名：读数按它点名，规则按它选取，两边写错的就不是同一个旋钮。
pub const EPHEMERAL_TTL_KNOB: &str = "evolution.workspaces.ephemeralTtlSecs";

/// 孤儿分支这一条保留回路在登记表里的 key。
pub const ORPHAN_BRANCH_TTL_KNOB: &str = "evolution.workspaces.orphanBranchTtlSecs";

/// 到达率按多长的窗口算。
///
/// 7 天：实例轮换是件稀疏的事，一个轮询周期里 0 或 1 次是常态；窗口短到一两次
/// 事件就能翻倍，长到与「这条卷多久会被重新填满」同一量级。窗口内的到达数除以
/// 窗口，就是这条回路的到达率。
// cap: fixed — 7 天：到达是稀疏事件，窗口短到一两次事件就能翻倍，长到与卷被重新填满同一量级
const ARRIVAL_WINDOW_SECS: u64 = 7 * 86_400;

/// 一件东西在这张卷上值得留多久：装得下多少件 × 新件到达间隔 / 2。
///
/// 半周转——到达的东西把这张卷重新填满所需时间的一半。一条律两处调用：临时
/// 工作树按棵树算，孤儿分支按独有对象算。问的是同一个问题：这件东西再留下去，
/// 腾出来的是别人要用的空间，还是删掉的是还会被用的东西。
///
/// `floor` 只由临时工作树用：已经归还的若干趟里最长的那一趟。正在用的一棵树
/// 不能短于任何一趟已经跑完的——属主 pid 在 Pod 内永远活着，年龄是唯一会把它
/// 判成泄漏的东西。回收时量到的年龄不进 `floor`：那个年龄被界自己截断，拿它
/// 当地板会让这条回路追着自己的输出涨。
///
/// 量不到卷、量不到单件字节、窗口内没有到达时落回 `seed`，`derived=false`。
/// `seed` 为零是「这个部署不要这类残留」的声明，实测不能把它重新打开。
fn keep_duration(
    capacity_bytes: Option<u64>,
    item_bytes: u64,
    arrivals: usize,
    span_secs: u64,
    floor: Duration,
    seed: Duration,
) -> (Duration, bool) {
    if seed.is_zero() {
        return (seed, false);
    }
    let Some(capacity) = capacity_bytes else {
        return (seed, false);
    };
    if item_bytes == 0 || arrivals == 0 || span_secs == 0 {
        return (seed, false);
    }
    let per_day = arrivals as f64 * 86_400.0 / span_secs as f64;
    let turnover = ttl_for_half_turnover(capacity, per_day, item_bytes as f64);
    if turnover.is_zero() {
        return (seed, false);
    }
    (turnover.max(floor), true)
}

fn unix_now() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

/// 窗口内的到达数。`now` 之前的 `span_secs` 里发生的才算——进程刚起来时窗口
/// 只有它活过的那一段，拿整窗去除会把刚发生的一次到达摊成七天的平均。
fn arrivals_within(at: &[u64], now: u64, span_secs: u64) -> usize {
    let since = now.saturating_sub(span_secs);
    at.iter().filter(|t| **t >= since).count()
}

/// 这些分支里不属于 main 的对象在盘上一共占多少字节。
///
/// 只算独有的那一段：与 main 共有的历史删不删都还在，占空间的是分支自己推上去
/// 的部分。git 读不出来（老版本没有 `--disk-usage`、refs 已不在）时返回 `None`，
/// 由调用方落回种子——一个量不出来的每分支成本不能反过来授权更早的回收。
fn unique_branch_bytes(bare: &Path, names: &[String]) -> Option<u64> {
    if names.is_empty() {
        return None;
    }
    let out = std::process::Command::new("git")
        .arg("--git-dir")
        .arg(bare)
        .args(["rev-list", "--disk-usage", "--objects"])
        .args(names)
        .args(["--not", "main"])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// 回收"实例身份已轮换"遗留工作树的最小存活时长。滚动更新期间新旧 Pod 可能
/// 短暂共存、共享同一工作树根，旧 Pod 正在用的树必须活过这个窗口。
///
/// 取值依据：沙盒 PVC 是 ReadWriteOnce、工作负载策略是 Recreate，新 Pod 起来时
/// 旧 Pod 必须已经释放卷，实际重叠窗口接近零；这个值只防御将来换成 RWX 或
/// RollingUpdate 的情形，取默认终止宽限期的若干倍即可，不能取大——取值越大，
/// 重启越频繁时遗留树越可能永远躲过回收。
// cap: bound — 取默认终止宽限期的若干倍；取值越大，重启越频繁时遗留树越可能躲过回收
pub const ORPHAN_MIN_AGE: Duration = Duration::from_secs(900);

/// 部署器独占那棵稳定工作树的 id。
///
/// 一个 id 就是一棵树：同一台机器上两次分配同一个 id 得到的是同一个目录。所以
/// 这个名字写错一处，读的人就去找一棵没人写的树——而「那棵树不存在」与「那棵树
/// 的读数为空」在调用方是同一种失败。
pub const MAINLINE_WORKSPACE_ID: &str = "mainline";

/// 引擎只读基线那棵稳定工作树的 id。它与 [`MAINLINE_WORKSPACE_ID`] 是两棵树，
/// 共用一个 id 会让基线与主线互相覆盖，且覆盖之后两边都还读得通。
pub const ENGINE_BASELINE_WORKSPACE_ID: &str = "engine-baseline";

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

/// 两次普查之间 `evol/` 分支集合的变化：新出现的名字与它们出现的时刻。
///
/// 只记本进程见过什么：一条分支是不是新实例推上来的，仓库里读不出来——ref
/// 只有最近一次强推的时间，第一次出现的时间没人写。两次普查的差集就是它，
/// 代价是一次名字集合的比较。
struct BranchSightings {
    /// 上一拍见到的名字；`None` 是还没普查过——第一拍是清点，不是到达。
    seen: Option<std::collections::HashSet<String>>,
    arrivals: Vec<u64>,
}

impl BranchSightings {
    fn new() -> Self {
        Self {
            seen: None,
            arrivals: Vec::new(),
        }
    }

    /// 用这一拍看到的分支名更新记录，返回窗口内的到达数。
    fn observe(&mut self, names: &[String], now: u64, span_secs: u64) -> usize {
        let current: std::collections::HashSet<String> = names.iter().cloned().collect();
        // 名字被删掉后又出现也算一次到达：那是两条分支的生命，不是同一条还活着。
        // 一条还活着的分支不会离开 `seen`，强推只更新它的时间，不换名字。
        let fresh = self
            .seen
            .as_ref()
            .map(|seen| current.difference(seen).count())
            .unwrap_or(0);
        self.arrivals.extend(std::iter::repeat_n(now, fresh));
        self.seen = Some(current);
        let since = now.saturating_sub(span_secs);
        self.arrivals.retain(|t| *t >= since);
        self.arrivals.len()
    }
}

/// 工作树分配器。裸仓库是唯一仓库源，所有工作树共享其对象库与 refs。
pub struct WorkspaceManager {
    bare_repo: PathBuf,
    root: PathBuf,
    target_dir: PathBuf,
    meta_dir: PathBuf,
    ephemeral_ttl: Duration,
    /// 孤儿分支存活上限的种子；生效值每拍按实测重算，见 [`Self::orphan_branch_ttl`]。
    orphan_branch_ttl: Duration,
    /// 本进程的实例身份：`evol/<id>` 里属于当前实例的那一条永不回收。
    /// 没装身份（分配器只被用来取临时树）时孤儿分支回收整条跳过。
    current_instance: Option<String>,
    /// 保留回路的读数面。缺席时两处界线照算，只是没人读得到——一条算给谁看
    /// 都不影响的回路，不因缺一个上报端点而停。
    retention: Option<Arc<RetentionReadings>>,
    /// 本进程见到的临时工作树到达时刻，按 [`ARRIVAL_WINDOW_SECS`] 裁剪。
    ephemeral_arrivals: std::sync::Mutex<Vec<u64>>,
    /// 已归还的临时工作树里最长的一趟，秒。回收时量到的年龄不算——见
    /// [`keep_duration`] 的 `floor`。
    longest_completed_round_secs: std::sync::Mutex<u64>,
    /// 分配器第一次见到世界的时刻。到达率的分母从这里起算，进程活了多久就
    /// 按多久算，不拿一个还没过完的整窗去除。
    started_unix: u64,
    /// 上一次普查见到的孤儿分支名。两次普查之间新出现的分支就是到达；据此
    /// 得到实例轮换的节奏，这个节奏不依赖 TTL 本身，所以这条回路不会追着自己。
    branch_sightings: std::sync::Mutex<BranchSightings>,
    /// `None` 表示跟着实测静默窗口走，见 [`cog_core::git_budget::stale_lock_age`]。
    stale_lock_age: Option<Duration>,
    /// 索引健康度采样的去处。缺席则只记日志、不上报——丢一个聚合读数不该让
    /// 刷新本身失败。
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    /// 每棵常驻树上一笔索引读数写下去的时刻，键是工作树 id。
    ///
    /// 只为本进程活着：这一格只用来回答"距上次盖章够久了没有"，进程重启后重采一遍
    /// 是对的——新进程还没有自己的读数。
    index_sampled_at: std::sync::Mutex<HashMap<String, std::time::Instant>>,
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
            orphan_branch_ttl: DEFAULT_ORPHAN_BRANCH_TTL,
            current_instance: None,
            retention: None,
            ephemeral_arrivals: std::sync::Mutex::new(Vec::new()),
            longest_completed_round_secs: std::sync::Mutex::new(0),
            started_unix: unix_now(),
            branch_sightings: std::sync::Mutex::new(BranchSightings::new()),
            stale_lock_age: None,
            metrics: None,
            index_sampled_at: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// 上报索引健康度采样的去处。
    pub fn with_metrics(mut self, metrics: Arc<dyn cog_core::MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 覆盖临时工作树存活上限的种子（回收策略随分配器走，各子系统用同一个值）。
    pub fn with_ephemeral_ttl(mut self, ttl: Duration) -> Self {
        self.ephemeral_ttl = ttl;
        self
    }

    /// 覆盖孤儿分支存活上限的种子。
    pub fn with_orphan_branch_ttl(mut self, ttl: Duration) -> Self {
        self.orphan_branch_ttl = ttl;
        self
    }

    /// 本进程的实例身份：`evol/<id>` 里属于当前实例的那一条永不回收。
    pub fn with_current_instance(mut self, instance: impl Into<String>) -> Self {
        self.current_instance = Some(instance.into());
        self
    }

    /// 两处保留界线的读数面。
    pub fn with_retention(mut self, readings: Arc<RetentionReadings>) -> Self {
        self.retention = Some(readings);
        self
    }

    /// 这块读数面本身：部署器的 registry 保留回路与这里的工作区 TTL 同用一份，
    /// `/metrics` 才只看到一条 `cogneva_retention_*` 序列族。
    pub(crate) fn retention_face(&self) -> Option<Arc<RetentionReadings>> {
        self.retention.clone()
    }

    /// 覆盖陈旧 git 锁的判定年龄（测试用短值触发自愈路径）。
    pub fn with_stale_lock_age(mut self, age: Duration) -> Self {
        self.stale_lock_age = Some(age);
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
        self.path_for(MAINLINE_WORKSPACE_ID)
    }

    /// 引擎只读基线工作树（稳定路径，就地刷新）。
    pub fn engine_baseline_workspace(&self) -> PathBuf {
        self.path_for(ENGINE_BASELINE_WORKSPACE_ID)
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

    /// 本轮演进的起点。解析出的基线是版本 tag 时，它必落在主线历史上且落后于
    /// 主线，而本轮紧接着会同步到上游主线：先回退到 tag 再前进到主线，两个提交
    /// 之间的每个文件都被改写两次。路径稳定换来的增量缓存按 mtime 判新鲜度，
    /// 于是这条路等于每轮把全部本地 crate 重新编一遍。
    ///
    /// 改用主线引用做起点只需一次 reset。实例自己的 evol 分支是显式基线，保持
    /// 不动；主线引用取不到时回落到原基线，不把整轮中断在一个缺失的引用上。
    pub async fn round_base(&self, ws: &Workspace, base: BaseRef) -> BaseRef {
        if matches!(base, BaseRef::Branch(_)) {
            return base;
        }
        match self
            .git_in(
                &ws.path,
                &["rev-parse", "--verify", "--quiet", MAINLINE_REF],
            )
            .await
        {
            Ok(_) => BaseRef::Branch(MAINLINE_REF.to_string()),
            Err(_) => base,
        }
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
        // 到达率是这条回路唯一的「新东西来得多快」读数：树还在时它的年龄会被
        // 保留界截断，树被还掉之后连元数据都没了，所以到达只能用在发生时。
        self.note_ephemeral_arrival();
        Ok(self.workspace_of(&spec, &path))
    }

    /// 归还工作树。常驻类型是 no-op（生命周期与进程同长）。
    pub async fn release(&self, ws: &Workspace) -> SFResult<()> {
        if ws.is_persistent() {
            return Ok(());
        }
        // 一趟跑完的长度由归还那一刻给出——这是唯一不被保留界截断的实测：
        // 被回收的树的年龄是「界到了」，不是「活完了」。它给临时工作树的界
        // 当地板，保证这个界不会短于任何一趟已经跑完的。
        if let Some(meta) = self.read_meta(&ws.id) {
            let round = unix_now().saturating_sub(meta.created_at_unix.max(0) as u64);
            let mut longest = self.longest_completed_round_secs.lock().unwrap();
            *longest = (*longest).max(round);
        }
        self.force_remove_path(&ws.path).await;
        self.remove_meta(&ws.id);
        Ok(())
    }

    fn note_ephemeral_arrival(&self) {
        let now = unix_now();
        let mut at = self.ephemeral_arrivals.lock().unwrap();
        at.push(now);
        let since = now.saturating_sub(ARRIVAL_WINDOW_SECS);
        at.retain(|t| *t >= since);
    }

    /// 进程活了多久与到达窗口里较短的那个，也就是到达率的分母。
    fn arrival_span_secs(&self, now: u64) -> u64 {
        now.saturating_sub(self.started_unix)
            .clamp(1, ARRIVAL_WINDOW_SECS)
    }

    /// 就地移动到 `base`：`reset --hard` + `clean -ffdx`。target 目录外置，
    /// 故 `-x` 不会误删编译缓存，工作树可以放心回到干净基线。
    pub async fn refresh(&self, ws: &Workspace, base: BaseRef) -> SFResult<()> {
        self.sample_index_health(ws).await;
        self.git_in(&ws.path, &["reset", "--hard", base.rev()])
            .await?;
        self.git_in(&ws.path, &["clean", "-ffdx"]).await?;
        Ok(())
    }

    /// 采样这棵工作树的索引健康度。调用方要在 `reset --hard` **之前**调：它量的是
    /// "这一次 reset 会重写多少文件"，而不是事后剩下什么。
    ///
    /// 索引和 HEAD 对不上时，`reset --hard` 会把整棵树按 HEAD 重写一遍——上千个
    /// 文件的 mtime 全刷新，共享 target 里本地 crate 的增量因此整轮作废（cargo 按
    /// 本地 path crate 的源码 mtime 判新鲜度）。索引为什么会丢至今没定因，也不是
    /// 每次重启都丢，所以先把它变成随时可读的数，等它自己再出现一次，而不是照着
    /// 一个未验证的假设再写一个守卫。
    ///
    /// 两个量都只读索引文件与 HEAD 的树对象，不 stat 工作树。用
    /// `git status --porcelain` 数 `D` 行能得到同一个数（索引空了时所有跟踪文件
    /// 都报 `D`），但那要扫一遍工作树，是每轮白付的钱。
    ///
    /// 只在常驻工作树上采。它们的 id 取自闭集——部署器、引擎基线、每实例的
    /// `cycle-*` 与 `porter-*`，旧实例的树由 [`Self::gc_orphan_instances`] 回收，
    /// 所以序列基数有界；临时树的 id 带 uuid，采了就是无界基数，而它本来就是一次性
    /// 检出、路径不参与增量，索引丢没丢不影响任何人。
    pub async fn sample_index_health(&self, ws: &Workspace) {
        self.sample_index_health_of(&ws.id, &ws.path, ws.is_persistent())
            .await;
    }

    /// 给两棵长命树补一次索引采样，把"这棵树多久没被重采"封在心跳上。
    ///
    /// 覆盖部署器树与引擎基线树：两棵都由本进程持有（同一份分配器，见
    /// `MainlineDeployer::poll_once` 的调用点），reset 之间可能闲着几小时，而它们的
    /// 索引读数正是给"下一次 reset 会不会重写整棵树"做证据的。按实例的 `cycle-*`/
    /// `porter-*` 树不在这里：它们每轮都在原地刷新（实测五分钟一次），不需要补；更
    /// 要紧的是**不能**补——那些树的寿命跟着实例走，实例没了树就被回收，它们的序列
    /// 本来就该随写者一起老掉，被心跳续期会把一棵已经不在的树的最后一笔读数永远说成
    /// 新鲜的，而那正是索引这条读数防的另一头。
    ///
    /// 两棵树的 id 取自闭集，所以序列基数有界：心跳只加行，不加维度。
    pub async fn heartbeat_index_health(&self) {
        for (id, path) in [
            (MAINLINE_WORKSPACE_ID, self.deployer_workspace()),
            (
                ENGINE_BASELINE_WORKSPACE_ID,
                self.engine_baseline_workspace(),
            ),
        ] {
            let last = {
                let sampled = self
                    .index_sampled_at
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                sampled.get(id).copied()
            };
            if !index_sample_needs_write(last, std::time::Instant::now(), INDEX_SAMPLE_HEARTBEAT) {
                continue;
            }
            self.sample_index_health_of(id, &path, true).await;
        }
    }

    /// [`Self::sample_index_health`] 的实现体：工作树 id 与路径就是它读的全部，
    /// 反过来"它属于哪一类树"决定采不采。心跳那条路只有 id 和路径（树可能在盘上还
    /// 没建起来，也可能已经被回收），所以判据留在这里而不是去凑一棵 [`Workspace`]。
    async fn sample_index_health_of(&self, id: &str, path: &Path, persistent: bool) {
        let Some(metrics) = self.metrics.clone() else {
            return;
        };
        if !persistent {
            return;
        }
        let Ok(gitdir) = self
            .git_in(path, &["rev-parse", "--absolute-git-dir"])
            .await
        else {
            // 解析不出 gitdir 就采不到，而那本身也是"这棵树的 git 元数据已经不对了"
            // 的一种；留一行日志，不编一个数。
            warn!(worktree = %id, "index sample: cannot resolve the git dir");
            return;
        };
        let labels = HashMap::from([(crate::LABEL_WORKTREE.to_string(), id.to_string())]);
        // 存在性先报：它只要一次 stat，后面两条 git 读数都失败时这一条仍然前进。
        // 但"这一条还在前进"只说得清文件在不在，说不清采样取没取到数——索引在、
        // 却读不出来时它照样报 1，而另一笔读数停在上一笔值上。采样停摆由下面单独
        // 那一格说（`WORKTREE_INDEX_SAMPLE_INCOMPLETE_METRIC`），这两笔读数谁都
        // 替不了它。
        let present = tokio::fs::metadata(PathBuf::from(gitdir).join("index"))
            .await
            .is_ok();
        report(
            metrics.as_ref(),
            WORKTREE_INDEX_PRESENT_METRIC,
            if present { 1.0 } else { 0.0 },
            &labels,
        )
        .await;

        let tracked = match self
            .count_git_lines(path, &["ls-tree", "-r", "--name-only", "HEAD"])
            .await
        {
            Ok(n) => n,
            Err(e) => {
                warn!(worktree = %id, "index sample: cannot list the files at HEAD: {e}");
                self.report_index_sample_outcome(metrics.as_ref(), id, true)
                    .await;
                return;
            }
        };
        let entries = match self.count_git_lines(path, &["ls-files"]).await {
            Ok(n) => n,
            Err(e) => {
                warn!(worktree = %id, "index sample: cannot list the index: {e}");
                self.report_index_sample_outcome(metrics.as_ref(), id, true)
                    .await;
                return;
            }
        };
        // 索引里比 HEAD 多的条目（暂存过又没提交的）不算"缺"，故只取正的差额。
        report(
            metrics.as_ref(),
            WORKTREE_INDEX_MISSING_FILES_METRIC,
            tracked.saturating_sub(entries) as f64,
            &labels,
        )
        .await;
        // 两笔读数都落下去了：这一次尝试的结局是"取到了数"。盖在失败的那两站上，
        // 也盖在这里，所以每一次尝试都只改这一格一次，值就是这一次尝试的结局。
        self.report_index_sample_outcome(metrics.as_ref(), id, false)
            .await;
        // 盖章只在两笔读数都落下去之后：上面任何一条早退都意味着这一棵这次没采到，
        // 而没采到就该在下一轮重试，不是把空档记成已经补过。落不下去的那次会每轮
        // 留一行 warn，那是对的——"这棵树的读数取不到"本来就该一直有人看得见。
        let mut sampled = self
            .index_sampled_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sampled.insert(id.to_string(), std::time::Instant::now());
    }

    /// 记下这一次采样尝试的结局：两笔读数都落下去记 0，任一条 git 读数没读成记 1。
    ///
    /// 与那两笔读数同一个产出点、按同一道心跳盖章，但**只盖在 git 读数这一头**：
    /// 解析不出 gitdir 是"这棵树还没建起来、或者已经被回收"，那是
    /// [`Self::heartbeat_index_health`] 认下的正常状态，在那条早退上记 1 会把一棵
    /// 还没建起来的树报成故障。而 git 读数失败是另一回事——树在，只是读不出来——它
    /// 让 missing-files 停在上一笔（健康的）值上，读那条读数的规则于是按"值没动"
    /// 判活，下面这一格是它唯一的失败面。
    async fn report_index_sample_outcome(
        &self,
        metrics: &dyn cog_core::MetricsBackend,
        id: &str,
        incomplete: bool,
    ) {
        report(
            metrics,
            WORKTREE_INDEX_SAMPLE_INCOMPLETE_METRIC,
            if incomplete { 1.0 } else { 0.0 },
            &HashMap::from([(crate::LABEL_WORKTREE.to_string(), id.to_string())]),
        )
        .await;
    }

    /// 数一条 git 命令输出的行数。空输出是 0 行，不是一个空行。
    async fn count_git_lines(&self, path: &Path, args: &[&str]) -> SFResult<u64> {
        let out = self.git_in(path, args).await?;
        Ok(out.lines().filter(|line| !line.is_empty()).count() as u64)
    }

    /// 某棵工作树当前的 HEAD。读不到返回 `None`——调用方据此保持原状，而不是
    /// 猜一个提交去移动别人。
    pub async fn head_of(&self, path: &Path) -> Option<String> {
        self.git_in(path, &["rev-parse", "HEAD"])
            .await
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
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
        // 先算界，再回收：读数记的是这一拍算出的那个数，即便这一拍没有树可回收。
        // 一个安静的卷与一条停摆的回路在这里分开——前者目标不变、计数仍在走。
        let ttl = self.effective_ephemeral_ttl().await;
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

    /// 一拍维护：按这一拍的实测重算两处保留界线，并用它回收。
    ///
    /// 两处界线的输入都在进程活着的时候变——卷被别的写入者占去、实例轮换、
    /// 工作树变大——「启动时量一次」的那个值迟早与卷上的东西对不上，而这两个
    /// 界删的东西都是回收不回来的（没提交的改动、没并入的提交）。
    pub async fn maintain(&self) {
        if let Err(e) = self.gc_stale().await {
            warn!(error = %e, "workspace gc failed");
        }
        if self.current_instance.is_none() {
            return;
        }
        if let Err(e) = self.gc_orphan_evol_branches().await {
            warn!(error = %e, "orphan evolution branch gc failed");
        }
    }

    /// 这一拍生效的临时工作树上限：卷还剩多少、一棵树多大、新树来得多快。
    async fn effective_ephemeral_ttl(&self) -> Duration {
        let now = unix_now();
        let span = self.arrival_span_secs(now);
        let arrivals = arrivals_within(&self.ephemeral_arrivals.lock().unwrap(), now, span);
        let trees = self.list().await.unwrap_or_default();
        let tree_bytes = self.sampled_tree_bytes(&trees).await;
        // 这张卷还能装新东西的字节数（`f_bavail`）：容量按它算，卷越满、界越短。
        let capacity = crate::build_gate::disk_bytes(&self.root);
        self.evaluate_ephemeral_ttl(capacity, tree_bytes, arrivals, span)
    }

    /// 把这一拍的输入折算成界并记进读数面，返回同一个界。
    fn evaluate_ephemeral_ttl(
        &self,
        capacity: Option<u64>,
        tree_bytes: u64,
        arrivals: usize,
        span_secs: u64,
    ) -> Duration {
        let floor = Duration::from_secs(*self.longest_completed_round_secs.lock().unwrap());
        let (ttl, derived) = keep_duration(
            capacity,
            tree_bytes,
            arrivals,
            span_secs,
            floor,
            self.ephemeral_ttl,
        );
        if let Some(readings) = &self.retention {
            readings.evaluate(EPHEMERAL_TTL_KNOB, ttl.as_secs_f64(), UNIT_SECONDS, derived);
        }
        ttl
    }

    /// 一棵工作树在这张卷上占多少字节。每棵都是同一个裸仓库的检出，量一棵代表
    /// 这一类；树有几十万条元数据，走查放进阻塞线程，不占住轮询。
    async fn sampled_tree_bytes(&self, entries: &[WorktreeEntry]) -> u64 {
        let deployer = self.deployer_workspace();
        let path = entries
            .iter()
            .map(|e| e.path.clone())
            .find(|p| *p == deployer)
            .or_else(|| {
                entries
                    .iter()
                    .map(|e| e.path.clone())
                    .find(|p| p.starts_with(&self.root))
            });
        let Some(path) = path else {
            return 0;
        };
        tokio::task::spawn_blocking(move || {
            cog_core::fs_size::dir_size_bytes(&path, &[]).unwrap_or(0)
        })
        .await
        .unwrap_or(0)
    }

    /// 把这一拍的分支输入折算成界并记进读数面，返回同一个界。
    fn evaluate_orphan_branch_ttl(
        &self,
        capacity: Option<u64>,
        branch_bytes: u64,
        arrivals: usize,
        span_secs: u64,
    ) -> Duration {
        let (ttl, derived) = keep_duration(
            capacity,
            branch_bytes,
            arrivals,
            span_secs,
            Duration::ZERO,
            self.orphan_branch_ttl,
        );
        if let Some(readings) = &self.retention {
            readings.evaluate(
                ORPHAN_BRANCH_TTL_KNOB,
                ttl.as_secs_f64(),
                UNIT_SECONDS,
                derived,
            );
        }
        ttl
    }

    /// 这一拍生效的孤儿分支上限：裸仓库还能装下多少条分支的独有对象、新实例
    /// 分支来得多快。`names` 是这一拍看到的孤儿分支名。
    fn effective_orphan_branch_ttl(&self, names: &[String]) -> Duration {
        let now = unix_now();
        let span = self.arrival_span_secs(now);
        let arrivals = self
            .branch_sightings
            .lock()
            .unwrap()
            .observe(names, now, span);
        let branch_bytes = if names.is_empty() {
            0
        } else {
            unique_branch_bytes(&self.bare_repo, names)
                .map(|bytes| bytes / names.len() as u64)
                .unwrap_or(0)
        };
        let capacity = crate::build_gate::disk_bytes(&self.bare_repo);
        self.evaluate_orphan_branch_ttl(capacity, branch_bytes, arrivals, span)
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
    /// - 末次提交已超过这一拍算出的界：所属实例长期不活动（存活实例的分支每次
    ///   移植都会被强推刷新），留着只占 ref 与可达对象。界不是写下的那个数，
    ///   而是裸仓库还能装下多少条分支的独有对象、新实例分支来得多快算出来的，
    ///   见 [`Self::effective_orphan_branch_ttl`]。
    ///
    /// 当前实例自己的分支永不删（没装实例身份的分配器整条跳过，免得把"不知道
    /// 谁是当前实例"读成"谁都不是"）；正被工作树占用的分支由 git 自身拒绝删除，
    /// 记录后跳过。`main` / `master` 之类不带 `evol/` 前缀的 ref 一律不在此列。
    pub async fn gc_orphan_evol_branches(&self) -> SFResult<Vec<String>> {
        let Some(current_instance) = self.current_instance.as_deref() else {
            return Ok(Vec::new());
        };
        let keep = format!("evol/{current_instance}");
        let out = self
            .git_bare(&[
                "for-each-ref",
                "--format=%(refname:short) %(objectname) %(committerdate:unix)",
                "refs/heads/evol/",
            ])
            .await?;
        // 这一拍看到的所有孤儿分支名先给界用：到达率与每条的独有对象都要它们，
        // 而回收只要其中过了界的那几条。
        let names: Vec<String> = out
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|name| *name != keep)
            .map(str::to_string)
            .collect();
        let ttl = self.effective_orphan_branch_ttl(&names);
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
        let Ok(entries) = self.list().await else {
            return false;
        };
        let Some(entry) = entries.iter().find(|e| e.path == path) else {
            return false;
        };
        // 被杀进程可能在 add/remove 中途留下管理锁：不清掉，`git worktree remove`
        // 永远拒绝动这棵树，分配器于是每次都在一棵拆不掉也修不了的树上失败。
        // 解锁受同一道年龄门约束；解锁失败按不健康处理，让调用方重建。
        if entry.locked && !self.unlock_stale_admin_lock(path).await {
            return false;
        }
        // 索引丢失或被截断时（被杀进程写了一半），git 登记与 `rev-parse` 依然
        // 正常，但 `reset --hard` 要么直接退出 128、要么按 HEAD 重写整棵树——
        // 共享 target 的增量缓存随之整轮作废，单构建槽被这一棵树的冷重建占满
        // 整个窗口，循环永远走不到队列的退休步。这正是 `sample_index_health`
        // 读的那个状态；在这里判不健康，让这棵树被重建而不是被"复用"。
        self.git_in(path, &["ls-files"]).await.is_ok()
    }

    /// 清除一棵已登记工作树上的管理锁，受与索引锁同一道年龄门约束：锁文件
    /// 必须老过 `stale_lock_age` 才动手（新锁可能是并发 add/remove 的活锁）。
    /// 返回这棵树之后是否处于未上锁状态。
    async fn unlock_stale_admin_lock(&self, path: &Path) -> bool {
        let Ok(gitdir) = self
            .git_in(path, &["rev-parse", "--absolute-git-dir"])
            .await
        else {
            return false;
        };
        let lock = PathBuf::from(gitdir).join("locked");
        if !clear_stale_git_lock(&lock, self.stale_lock_age()).await {
            return false;
        }
        warn!(
            path = %path.display(),
            "removed stale worktree admin lock left by a killed process; unlocking"
        );
        // 锁文件已删，`unlock` 只是清掉 git 的登记位；它失败不再构成阻塞。
        let _ = self
            .git_bare(&["worktree", "unlock", path.to_str().unwrap_or("")])
            .await;
        true
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

    /// Mark a tree safe for git, once.
    ///
    /// `--add` appends unconditionally, and the entry was never removed when
    /// the tree went away: every cycle left its path in the shared global
    /// config, which every git invocation on the host then parses. The
    /// dubious-ownership check reads global config only (`-c` and the
    /// `GIT_CONFIG_*` environment do not reach it), so the file is the one
    /// place this can live -- which makes "add only when absent" plus the
    /// removal in teardown the whole bound on its size.
    async fn register_safe_directory(&self, path: &Path) {
        let path = path.to_str().unwrap_or("");
        if path.is_empty() {
            return;
        }
        let listed = self
            .run_git(&["config", "--global", "--get-all", "safe.directory"], None)
            .await
            .unwrap_or_default();
        if !safe_directory_missing(&listed, path) {
            return;
        }
        let _ = self
            .run_git(
                &["config", "--global", "--add", "safe.directory", path],
                None,
            )
            .await;
    }

    /// Drop the tree's entry when the tree goes away, so the global config
    /// holds the live trees and nothing else. `--fixed-value` because git
    /// reads the value as a regex otherwise, and a path is full of
    /// regex metacharacters.
    async fn forget_safe_directory(&self, path: &Path) {
        let path = path.to_str().unwrap_or("");
        if path.is_empty() {
            return;
        }
        let _ = self
            .run_git(
                &[
                    "config",
                    "--global",
                    "--fixed-value",
                    "--unset-all",
                    "safe.directory",
                    path,
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
        self.forget_safe_directory(path).await;
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
        match self.run_git_once(args, workdir).await {
            Ok(out) => Ok(out),
            Err(e) => {
                // 被杀进程留下的锁会让该工作树的所有写操作永久失败；锁龄超过
                // 判定值即清除并重试一次（活锁受超时+kill_on_drop 保护，等不到
                // 这个年龄）。
                let cleared = match stale_lock_candidate(&e.to_string()) {
                    Some(lock) => {
                        let gone = clear_stale_git_lock(&lock, self.stale_lock_age()).await;
                        if gone {
                            warn!(
                                lock = %lock.display(),
                                "removed stale git lock left by a killed process; retrying"
                            );
                        }
                        gone
                    }
                    None => false,
                };
                if cleared {
                    return self.run_git_once(args, workdir).await;
                }
                Err(e)
            }
        }
    }

    /// 锁的判定年龄：默认跟着实测静默窗口走，见 [`cog_core::git_budget::stale_lock_age`]。
    fn stale_lock_age(&self) -> Duration {
        self.stale_lock_age
            .unwrap_or_else(cog_core::git_budget::stale_lock_age)
    }

    async fn run_git_once(&self, args: &[&str], workdir: Option<&Path>) -> SFResult<String> {
        let line = args.join(" ");
        let mut cmd = tokio::process::Command::new("git");
        cmd.args(args).kill_on_drop(true);
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
        let out = match cog_core::progress::run_child_watched(cmd, &format!("git {line}"))
            .await
            .map_err(|e| SFError::IO(format!("failed to run git: {e}")))?
        {
            cog_core::progress::ChildOutcome::Exited(out) => out,
            cog_core::progress::ChildOutcome::Stalled { silent, window } => {
                return Err(SFError::IO(format!(
                    "git {line} 停住了：{silent:?} 里没有输出、也没有 CPU 时间，\
                     比这套部署见过的任何一段仍在干活的步间隔（{window:?}）都长；已杀掉"
                )))
            }
        };
        if !out.status.success() {
            return Err(SFError::IO(format!(
                "git {line} failed: {}{}",
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

/// 上报一个采样值。采样不该因为上报失败而中断——工作树该刷新的还是要刷新——
/// 所以错误只留一行日志。
async fn report(
    metrics: &dyn cog_core::MetricsBackend,
    name: cog_core::MetricName,
    value: f64,
    labels: &HashMap<String, String>,
) {
    if let Err(e) = metrics.record_gauge(name, value, labels.clone()).await {
        warn!(metric = %name, "index sample: cannot report: {e}");
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

/// Whether `path` still has to be added to a safe.directory list holding
/// `listed` (the output of `git config --global --get-all safe.directory`).
///
/// Pure, so the decision is testable without writing anyone's global config:
/// the writer runs once per tree and once per process start, and a wrong
/// answer here is a file that grows forever.
fn safe_directory_missing(listed: &str, path: &str) -> bool {
    !path.is_empty() && !listed.lines().any(|line| line.trim() == path)
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
    use cog_core::MetricsBackend as _;

    /// 两个稳定 id 就是盘上已经存在的那两个目录名。
    ///
    /// 这两个名字是身份，不是措辞：机器上已经躺着叫这两个名字的工作树，改名
    /// 不会报错，只会让所有人去找一棵不存在的树——而「没有这棵树」与「这棵树
    /// 还没被采样」在读数上是同一个零。要改它们，得同时把盘上的树搬走。
    #[test]
    fn the_stable_workspace_ids_are_the_ones_already_on_disk() {
        assert_eq!(MAINLINE_WORKSPACE_ID, "mainline");
        assert_eq!(ENGINE_BASELINE_WORKSPACE_ID, "engine-baseline");
        assert_ne!(
            MAINLINE_WORKSPACE_ID, ENGINE_BASELINE_WORKSPACE_ID,
            "两个 id 相同就是同一棵树，基线与主线会互相覆盖且两边都还读得通"
        );
    }

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

    async fn gauge_of(
        mb: &cog_test_support::MemoryMetricsBackend,
        name: &str,
        worktree: &str,
    ) -> f64 {
        let samples = mb.query_gauge_latest(name).await.unwrap();
        samples
            .into_iter()
            .find(|s| s.labels.get(crate::LABEL_WORKTREE).map(String::as_str) == Some(worktree))
            .unwrap_or_else(|| panic!("no {name} sample for worktree {worktree}"))
            .value
    }

    #[tokio::test]
    async fn index_health_reads_clean_on_a_healthy_worktree() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mb = std::sync::Arc::new(cog_test_support::MemoryMetricsBackend::new());
        let mgr = manager(tmp.path(), &bare).with_metrics(mb.clone());

        let spec = WorkspaceSpec::persistent(
            "mainline",
            WorkspaceKind::Deployer,
            BaseRef::Commit(rev_b.clone()),
        );
        let ws = mgr.ensure_persistent(spec).await.unwrap();
        mgr.sample_index_health(&ws).await;

        assert_eq!(
            gauge_of(&mb, WORKTREE_INDEX_PRESENT_METRIC.as_str(), "mainline").await,
            1.0
        );
        assert_eq!(
            gauge_of(
                &mb,
                WORKTREE_INDEX_MISSING_FILES_METRIC.as_str(),
                "mainline"
            )
            .await,
            0.0
        );
    }

    /// 复现实验 ④ 的状态：索引文件被删。这正是「下一次 reset 会重写整棵树」的条件，
    /// 采样必须在 reset **之前**给出非零读数。
    #[tokio::test]
    async fn index_health_reports_every_tracked_file_when_the_index_is_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mb = std::sync::Arc::new(cog_test_support::MemoryMetricsBackend::new());
        let mgr = manager(tmp.path(), &bare).with_metrics(mb.clone());

        let spec = WorkspaceSpec::persistent(
            "mainline",
            WorkspaceKind::Deployer,
            BaseRef::Commit(rev_b.clone()),
        );
        let ws = mgr.ensure_persistent(spec).await.unwrap();
        let tracked = git_out(&ws.path, &["ls-tree", "-r", "--name-only", "HEAD"])
            .lines()
            .count() as f64;
        assert!(tracked > 0.0);

        let gitdir = git_out(&ws.path, &["rev-parse", "--absolute-git-dir"]);
        std::fs::remove_file(Path::new(&gitdir).join("index")).unwrap();
        mgr.sample_index_health(&ws).await;

        assert_eq!(
            gauge_of(&mb, WORKTREE_INDEX_PRESENT_METRIC.as_str(), "mainline").await,
            0.0
        );
        assert_eq!(
            gauge_of(
                &mb,
                WORKTREE_INDEX_MISSING_FILES_METRIC.as_str(),
                "mainline"
            )
            .await,
            tracked
        );
    }

    /// 索引文件在、却读不出来（写到一半被截断）时，这一对读数说"一切正常"：
    /// `present` 是一次 stat，当然报 1；missing-files 只在两笔 git 读数都成功时
    /// 才盖章，于是它停在上一笔健康值 0 上。实测这一态的后果：`git ls-files` 对
    /// 零长索引退出 128，`git reset --hard` 同样退出 128——这棵树再也刷新不了，
    /// 而采样器只留一行 warn。所以"这一次尝试没取到数"要单独成一格。
    #[tokio::test]
    async fn index_health_names_an_incomplete_sample_apart_from_a_healthy_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mb = std::sync::Arc::new(cog_test_support::MemoryMetricsBackend::new());
        let mgr = manager(tmp.path(), &bare).with_metrics(mb.clone());

        let spec = WorkspaceSpec::persistent(
            "mainline",
            WorkspaceKind::Deployer,
            BaseRef::Commit(rev_b.clone()),
        );
        let ws = mgr.ensure_persistent(spec).await.unwrap();
        mgr.sample_index_health(&ws).await;
        assert_eq!(
            gauge_of(
                &mb,
                WORKTREE_INDEX_SAMPLE_INCOMPLETE_METRIC.as_str(),
                "mainline"
            )
            .await,
            0.0,
            "取到数的那一次记 0"
        );

        // 零长索引：文件还在（stat 说得对），git 读不出来。
        let gitdir = git_out(&ws.path, &["rev-parse", "--absolute-git-dir"]);
        std::fs::write(Path::new(&gitdir).join("index"), b"").unwrap();
        mgr.sample_index_health(&ws).await;

        assert_eq!(
            gauge_of(&mb, WORKTREE_INDEX_PRESENT_METRIC.as_str(), "mainline").await,
            1.0,
            "文件在，存在性读数报的是对的"
        );
        assert_eq!(
            gauge_of(
                &mb,
                WORKTREE_INDEX_MISSING_FILES_METRIC.as_str(),
                "mainline"
            )
            .await,
            0.0,
            "这一笔没重盖，所以它停在上一笔的健康值上——正是缺陷的形状"
        );
        assert_eq!(
            gauge_of(
                &mb,
                WORKTREE_INDEX_SAMPLE_INCOMPLETE_METRIC.as_str(),
                "mainline"
            )
            .await,
            1.0,
            "取不到数这件事只有这一格说得出来"
        );
    }

    /// 临时树的 id 带 uuid，采了就是无界基数；它们本来就一次性检出。
    #[tokio::test]
    async fn index_health_skips_ephemeral_worktrees() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mb = std::sync::Arc::new(cog_test_support::MemoryMetricsBackend::new());
        let mgr = manager(tmp.path(), &bare).with_metrics(mb.clone());

        let ws = mgr
            .acquire_ephemeral("cycle", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();
        mgr.sample_index_health(&ws).await;

        assert!(
            mb.query_gauge_latest(WORKTREE_INDEX_PRESENT_METRIC.as_str())
                .await
                .unwrap()
                .is_empty(),
            "临时树不该被采样"
        );
    }

    /// 某棵树在某条读数上已经落了多少行。心跳的全部效果就是"落一行"或"不落"，
    /// 所以判据要数行，不能只看最新值——值在窗内重复写也是同一个数。
    async fn sample_rows(
        mb: &cog_test_support::MemoryMetricsBackend,
        name: &str,
        worktree: &str,
    ) -> usize {
        let now = chrono::Utc::now();
        mb.query_gauge_range(
            name,
            now - chrono::Duration::hours(1),
            now + chrono::Duration::minutes(1),
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|s| s.labels.get(crate::LABEL_WORKTREE).map(String::as_str) == Some(worktree))
        .count()
    }

    /// 心跳的判据只认本进程上一次真写完的时刻：没采过要采，窗内不采，到点再采。
    #[test]
    fn index_sample_heartbeat_writes_when_absent_then_only_at_the_bound() {
        let now = std::time::Instant::now();
        assert!(index_sample_needs_write(None, now, INDEX_SAMPLE_HEARTBEAT));
        assert!(!index_sample_needs_write(
            Some(now),
            now,
            INDEX_SAMPLE_HEARTBEAT
        ));
        assert!(!index_sample_needs_write(
            Some(now - (INDEX_SAMPLE_HEARTBEAT - Duration::from_secs(1))),
            now,
            INDEX_SAMPLE_HEARTBEAT
        ));
        assert!(index_sample_needs_write(
            Some(now - INDEX_SAMPLE_HEARTBEAT),
            now,
            INDEX_SAMPLE_HEARTBEAT
        ));
    }

    /// 两棵长命树只在"即将 reset"时被采，于是空闲的部署器能让读数静默几小时。
    /// 心跳是它们唯一的第二条写路，必须把它补上；补过之后窗内不许再补。
    #[tokio::test]
    async fn heartbeat_stamps_long_lived_worktrees_and_leaves_instance_trees_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mb = std::sync::Arc::new(cog_test_support::MemoryMetricsBackend::new());
        let mgr = manager(tmp.path(), &bare).with_metrics(mb.clone());

        // 三棵树都建出来，但一条读数都还没采（ensure_persistent 不采）。
        for (id, kind) in [
            ("mainline", WorkspaceKind::Deployer),
            ("engine-baseline", WorkspaceKind::EngineBaseline),
            ("cycle-7", WorkspaceKind::Cycle),
        ] {
            mgr.ensure_persistent(WorkspaceSpec::persistent(
                id,
                kind,
                BaseRef::Commit(rev_b.clone()),
            ))
            .await
            .unwrap();
        }
        assert_eq!(
            sample_rows(
                &mb,
                WORKTREE_INDEX_MISSING_FILES_METRIC.as_str(),
                "mainline"
            )
            .await,
            0
        );

        mgr.heartbeat_index_health().await;

        for id in ["mainline", "engine-baseline"] {
            assert_eq!(
                gauge_of(&mb, WORKTREE_INDEX_PRESENT_METRIC.as_str(), id).await,
                1.0,
                "{id} 是长命树，心跳必须给它盖章"
            );
            assert_eq!(
                sample_rows(&mb, WORKTREE_INDEX_MISSING_FILES_METRIC.as_str(), id).await,
                1
            );
        }
        assert_eq!(
            sample_rows(&mb, WORKTREE_INDEX_MISSING_FILES_METRIC.as_str(), "cycle-7").await,
            0,
            "实例树的寿命跟着实例走，被心跳续期会把已回收的树说成新鲜的"
        );

        // 紧接着的第二拍在窗内：行数不许涨。
        let before = sample_rows(
            &mb,
            WORKTREE_INDEX_MISSING_FILES_METRIC.as_str(),
            "mainline",
        )
        .await;
        mgr.heartbeat_index_health().await;
        assert_eq!(
            sample_rows(
                &mb,
                WORKTREE_INDEX_MISSING_FILES_METRIC.as_str(),
                "mainline"
            )
            .await,
            before,
            "窗内重复盖章等于把心跳退化成每拍刷一行"
        );
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

    const GIB: u64 = 1024 * 1024 * 1024;

    /// 临时工作树的界是卷、每棵树、到达率三个实测的函数：到达快一倍或卷小一半，
    /// 界各减半；量不到任何一项时落回种子。
    #[test]
    fn the_ephemeral_ttl_follows_the_measured_volume_and_arrivals() {
        let seed = Duration::from_secs(21_600);
        let days = |d: u64| d as f64 * 86_400.0;
        // 例如 100 GiB、每棵 1 GiB、每天到达一棵：半周转 = 100 × 1 天 / 2 = 50 天。
        let (ttl, derived) = keep_duration(Some(100 * GIB), GIB, 1, 86_400, Duration::ZERO, seed);
        assert!(derived);
        assert!((ttl.as_secs_f64() - days(50)).abs() < 1.0, "{ttl:?}");

        let (faster, _) = keep_duration(Some(100 * GIB), GIB, 2, 86_400, Duration::ZERO, seed);
        assert!(
            (faster.as_secs_f64() - days(25)).abs() < 1.0,
            "到达快一倍，界减半"
        );

        let (smaller, _) = keep_duration(Some(50 * GIB), GIB, 1, 86_400, Duration::ZERO, seed);
        assert!(
            (smaller.as_secs_f64() - days(25)).abs() < 1.0,
            "卷小一半，界减半"
        );

        let (bigger_tree, _) =
            keep_duration(Some(100 * GIB), 2 * GIB, 1, 86_400, Duration::ZERO, seed);
        assert!(
            (bigger_tree.as_secs_f64() - days(25)).abs() < 1.0,
            "树大一倍，界减半"
        );

        // 已归还的一趟是最少保留：半周转比它短时，它顶着。
        let (floored, derived) = keep_duration(
            Some(100 * GIB),
            GIB,
            1,
            86_400,
            Duration::from_secs(60 * 86_400),
            seed,
        );
        assert!(derived);
        assert_eq!(floored, Duration::from_secs(60 * 86_400));
    }

    /// 量不到的那一侧落回种子：没有卷、没有到达、没有单件成本都不是「零」。
    #[test]
    fn an_unmeasured_retention_falls_back_to_the_declaration() {
        let seed = Duration::from_secs(21_600);
        for (capacity, item, arrivals, span) in [
            (None, GIB, 1, 86_400),
            (Some(100 * GIB), 0, 1, 86_400),
            (Some(100 * GIB), GIB, 0, 86_400),
            (Some(100 * GIB), GIB, 1, 0),
        ] {
            let (ttl, derived) =
                keep_duration(capacity, item, arrivals, span, Duration::ZERO, seed);
            assert_eq!(ttl, seed);
            assert!(!derived, "没有实测就不是 derived");
        }
        // 种子为零是部署关掉这类残留的声明；实测不能把它重新打开。
        let (ttl, derived) = keep_duration(
            Some(100 * GIB),
            GIB,
            5,
            86_400,
            Duration::ZERO,
            Duration::ZERO,
        );
        assert_eq!(ttl, Duration::ZERO);
        assert!(!derived);
    }

    /// 孤儿分支用同一条律：新实例分支来得快或仓库空间小，界就短。
    #[test]
    fn the_orphan_branch_ttl_follows_the_measured_repo_and_rotations() {
        let seed = Duration::from_secs(2_592_000);
        let mib = 1024u64 * 1024;
        let (slow, derived) =
            keep_duration(Some(10 * GIB), 100 * mib, 1, 86_400, Duration::ZERO, seed);
        assert!(derived);
        let (fast, _) = keep_duration(Some(10 * GIB), 100 * mib, 2, 86_400, Duration::ZERO, seed);
        assert!(fast < slow, "轮换快，旧的留得短");
        let (packed, _) = keep_duration(Some(5 * GIB), 100 * mib, 1, 86_400, Duration::ZERO, seed);
        assert!(packed < slow, "仓库空间小，旧的留得短");
        let (unmeasured, derived) = keep_duration(None, 100 * mib, 1, 86_400, Duration::ZERO, seed);
        assert_eq!(unmeasured, seed);
        assert!(!derived);
    }

    /// 窗口内计数：窗口外的那次到达不再计入到达率。
    #[test]
    fn only_arrivals_inside_the_window_count() {
        let now = 1_000_000;
        let at = [now - 10, now - 400_000, now - 900_000];
        assert_eq!(arrivals_within(&at, now, 86_400), 1);
        assert_eq!(arrivals_within(&at, now, 500_000), 2);
        assert_eq!(arrivals_within(&at, now, 1_000_000), 3);
        assert_eq!(arrivals_within(&at, now, 5), 0);
    }

    /// 输入不变目标不动是对的，但「这一拍算过」必须读得出来：两拍同一个值，
    /// 计数仍然走到 2；改输入之后下一拍的目标跟着变。
    #[tokio::test]
    async fn a_beat_whose_target_does_not_move_still_counts() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, _b) = seed_bare(tmp.path());
        let readings = std::sync::Arc::new(RetentionReadings::new());
        let mgr = manager(tmp.path(), &bare)
            .with_retention(readings.clone())
            .with_orphan_branch_ttl(Duration::from_secs(2_592_000));

        let first = mgr.evaluate_ephemeral_ttl(Some(100 * GIB), GIB, 1, 86_400);
        let second = mgr.evaluate_ephemeral_ttl(Some(100 * GIB), GIB, 1, 86_400);
        assert_eq!(first, second);
        let (target, evaluations, derived) = readings.reading(EPHEMERAL_TTL_KNOB).unwrap();
        assert_eq!(target, first.as_secs_f64());
        assert_eq!(evaluations, 2, "目标不动，回路仍在算");
        assert!(derived);

        // 改输入、不重启：下一拍跟上。
        let changed = mgr.evaluate_ephemeral_ttl(Some(50 * GIB), GIB, 1, 86_400);
        assert!(changed < first);
        let (target, evaluations, _) = readings.reading(EPHEMERAL_TTL_KNOB).unwrap();
        assert_eq!(target, changed.as_secs_f64());
        assert_eq!(evaluations, 3);

        // 孤儿分支这一条同样每拍走一次，即便仓库里一条分支都没有。
        let seed_ttl = mgr.effective_orphan_branch_ttl(&[]);
        assert_eq!(seed_ttl, Duration::from_secs(2_592_000));
        mgr.effective_orphan_branch_ttl(&[]);
        let (target, evaluations, derived) = readings.reading(ORPHAN_BRANCH_TTL_KNOB).unwrap();
        assert_eq!(target, 2_592_000.0);
        assert_eq!(evaluations, 2);
        assert!(!derived);
    }

    /// 一轮跑完的长度是归还那一刻记下的；回收不记（它的年龄被界截断）。
    #[tokio::test]
    async fn a_completed_round_sets_the_floor_for_the_ephemeral_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        let ws = mgr
            .acquire_ephemeral("admin", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();
        // 把创建时间往回拨，模拟一趟跑了很久。
        let meta_path = mgr.meta_path(&ws.id);
        let mut meta: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta["created_at_unix"] = serde_json::json!(unix_now() as i64 - 7_200);
        std::fs::write(&meta_path, meta.to_string()).unwrap();

        mgr.release(&ws).await.unwrap();
        assert_eq!(*mgr.longest_completed_round_secs.lock().unwrap(), 7_200);
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
        let mgr = manager(tmp.path(), &bare_repo)
            .with_current_instance("quinn")
            .with_orphan_branch_ttl(Duration::from_secs(3600));

        // 当前实例：即使落后于 main 也不许动，下一轮移植还要强推它。
        bare(&bare_repo, &["branch", "evol/quinn", &rev_a]);
        // 旧实例，活干完了：已完全并入 main。
        bare(&bare_repo, &["branch", "evol/ada", &rev_a]);
        // 旧实例，未并入但刚提交：处在活跃窗口内，这一轮保留。
        branch_with_wip_commit(&bare_repo, "evol/max", &rev_a);

        let reclaimed = mgr.gc_orphan_evol_branches().await.unwrap();
        assert_eq!(reclaimed, vec!["evol/ada"]);
        assert!(git_out(&bare_repo, &["branch", "--list", "evol/ada"]).is_empty());
        assert!(!git_out(&bare_repo, &["branch", "--list", "evol/quinn"]).is_empty());
        assert!(!git_out(&bare_repo, &["branch", "--list", "evol/max"]).is_empty());

        // 种子归零：未并入的旧分支随即按超期回收。
        let zero = manager(tmp.path(), &bare_repo)
            .with_current_instance("quinn")
            .with_orphan_branch_ttl(Duration::ZERO);
        let reclaimed = zero.gc_orphan_evol_branches().await.unwrap();
        assert_eq!(reclaimed, vec!["evol/max"]);
        assert!(git_out(&bare_repo, &["branch", "--list", "evol/max"]).is_empty());
        assert!(!git_out(&bare_repo, &["branch", "--list", "evol/quinn"]).is_empty());
        // 非 `evol/` 前缀的 ref 不在扫描范围内。
        assert!(!git_out(&bare_repo, &["branch", "--list", "main"]).is_empty());

        // 不知道自己是哪个实例的分配器整条跳过：它连"谁是当前实例"都答不出，
        // 把留下的当成孤儿就会删掉正在被推的那一条。
        let anonymous = manager(tmp.path(), &bare_repo);
        bare(&bare_repo, &["branch", "evol/zed", &rev_a]);
        assert!(anonymous
            .gc_orphan_evol_branches()
            .await
            .unwrap()
            .is_empty());
        assert!(!git_out(&bare_repo, &["branch", "--list", "evol/quinn"]).is_empty());
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

    #[tokio::test]
    async fn round_base_aligns_to_the_mainline_instead_of_rewinding_to_the_version_tag() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, rev_a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        // 版本 tag 停在旧提交、主线在新提交——这正是「先回退到 tag 再前进到主线」
        // 会把两个提交之间的每个文件改写两次的形状。
        in_bare(&bare, &["tag", "v0.5.7", &rev_a]);
        let tag = BaseRef::Tag("v0.5.7".to_string());

        let ws = mgr
            .ensure_persistent(WorkspaceSpec::persistent(
                "cycle-local",
                WorkspaceKind::Cycle,
                tag.clone(),
            ))
            .await
            .unwrap();
        // 还没同步过：没有 local/main 可对齐，回落到原基线，而不是把整轮断在
        // 一个缺失的引用上。
        assert!(matches!(
            mgr.round_base(&ws, tag.clone()).await,
            BaseRef::Tag(t) if t == "v0.5.7"
        ));

        // 第一轮同步会建出 local/main；此后起点必须对齐主线。
        git(&ws.path, &["fetch", "-q", "local", "main"]);
        assert_eq!(git_out(&ws.path, &["rev-parse", "local/main"]), rev_b);
        assert!(matches!(
            mgr.round_base(&ws, tag).await,
            BaseRef::Branch(b) if b == "local/main"
        ));
    }

    #[tokio::test]
    async fn round_base_keeps_an_explicit_evol_branch_base() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _rev_a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        in_bare(&bare, &["branch", "evol/local", &rev_b]);
        let base = BaseRef::Branch("evol/local".to_string());

        let ws = mgr
            .ensure_persistent(WorkspaceSpec::persistent(
                "cycle-local",
                WorkspaceKind::Cycle,
                base.clone(),
            ))
            .await
            .unwrap();
        git(&ws.path, &["fetch", "-q", "local", "main"]);

        // 移植分支是显式基线：即使 local/main 可取也不改用它。
        assert!(matches!(
            mgr.round_base(&ws, base).await,
            BaseRef::Branch(b) if b == "evol/local"
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

    /// 已在列表里的路径不再追加；空路径永远不写（否则会往全局配置里塞一行空值）。
    /// 这一格盯的是「每次建树都追加一行」那个形状：它曾让宿主的全局配置长到
    /// 三万余行、1.5 MB，宿主上每条 git 命令都要解析。
    #[test]
    fn a_listed_path_is_not_added_again() {
        let listed = "/a/bare.git\n/tmp/x/workspaces/one\n";
        assert!(!safe_directory_missing(listed, "/tmp/x/workspaces/one"));
        assert!(safe_directory_missing(listed, "/tmp/x/workspaces/two"));
        assert!(safe_directory_missing("", "/tmp/x/workspaces/one"));
        assert!(!safe_directory_missing(listed, ""));
        // 前缀相同不等于同一条：`one` 与 `one-longer` 是两棵树。
        assert!(safe_directory_missing(
            listed,
            "/tmp/x/workspaces/one-longer"
        ));
    }

    #[test]
    fn sanitizes_ids_to_filesystem_safe() {
        assert_eq!(sanitize_id("evol/quinn-33b30bf1"), "evol-quinn-33b30bf1");
        assert_eq!(sanitize_id("porter::local"), "porter--local");
        assert_eq!(sanitize_id("///"), "ws");
    }

    /// 锁路径只从 git 的固定报错措辞里提取，且必须真是 .lock 文件——
    /// 别的引号串不能误当锁删掉。
    #[test]
    fn stale_lock_candidate_parses_git_wording() {
        let err =
            "git reset failed: fatal: Unable to create '/repo/.git/index.lock': File exists.\n";
        assert_eq!(
            stale_lock_candidate(err),
            Some(PathBuf::from("/repo/.git/index.lock"))
        );
        assert!(
            stale_lock_candidate("fatal: Unable to create '/repo/.git/HEAD': File exists.")
                .is_none(),
            "非 .lock 结尾的路径不许碰"
        );
        assert!(stale_lock_candidate("some other error").is_none());
    }

    /// 被杀进程留下的陈旧锁：清掉并重试后写操作恢复；锁龄不足的新锁
    /// 可能是并发活锁，不许动，错误原样抛出。
    #[tokio::test]
    async fn stale_git_lock_is_cleared_and_retried() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, rev_a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare).with_stale_lock_age(Duration::ZERO);
        let ws = mgr
            .acquire_ephemeral("cycle", BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap();

        // 工作树的 GIT_DIR 在裸仓库的 worktrees/ 下，锁要种在那里。
        let gitdir = PathBuf::from(git_out(&ws.path, &["rev-parse", "--absolute-git-dir"]));
        let lock = gitdir.join("index.lock");
        std::fs::write(&lock, "").unwrap();

        // 判定龄为零：任何锁都按尸体处理，清掉重试后 refresh 必须成功。
        mgr.refresh(&ws, BaseRef::Commit(rev_a.clone()))
            .await
            .unwrap();
        assert!(!lock.exists(), "陈旧锁要被清掉");
        assert_eq!(git_out(&ws.path, &["rev-parse", "HEAD"]), rev_a);

        // 默认 600s 判定龄下刚种的锁是新锁：不许删，操作必须失败。
        let mgr = manager(tmp.path(), &bare);
        std::fs::write(&lock, "").unwrap();
        let err = mgr
            .refresh(&ws, BaseRef::Commit(rev_b.clone()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("index.lock"));
        assert!(lock.exists(), "可能是活锁的新锁不能删");
    }

    /// 索引被截断（被杀进程写了一半）的树：git 登记与 `rev-parse` 都正常，
    /// 但它再也刷新不了——`reset --hard` 对这棵树退出 128。健康检查必须把它
    /// 判成不健康，让 ensure_persistent 重建，而不是"复用"进每轮一次的全树
    /// 重写（或一次永久的失败）。
    #[tokio::test]
    async fn a_persistent_tree_with_a_wedged_index_is_rebuilt() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare);
        let spec = WorkspaceSpec::persistent(
            "mainline",
            WorkspaceKind::Deployer,
            BaseRef::Commit(rev_b.clone()),
        );
        let ws = mgr.ensure_persistent(spec.clone()).await.unwrap();

        // 零长索引：文件还在，git 读不出来。
        let gitdir = git_out(&ws.path, &["rev-parse", "--absolute-git-dir"]);
        std::fs::write(Path::new(&gitdir).join("index"), b"").unwrap();

        let rebuilt = mgr.ensure_persistent(spec).await.unwrap();
        assert!(rebuilt.path.join("a.txt").exists());
        assert_eq!(git_out(&rebuilt.path, &["rev-parse", "HEAD"]), rev_b);
    }

    /// 被杀进程留下的管理锁（`git worktree lock` / 中途的 add-remove）：
    /// 锁龄超过判定值即按尸体处理——解锁后这棵树被复用，而不是每轮在一棵
    /// 拆不掉也修不了的树上失败。
    #[tokio::test]
    async fn a_stale_admin_lock_is_unlocked_and_the_tree_reused() {
        let tmp = tempfile::tempdir().unwrap();
        let (bare, _a, rev_b) = seed_bare(tmp.path());
        let mgr = manager(tmp.path(), &bare).with_stale_lock_age(Duration::ZERO);
        let spec = WorkspaceSpec::persistent(
            "mainline",
            WorkspaceKind::Deployer,
            BaseRef::Commit(rev_b.clone()),
        );
        let ws = mgr.ensure_persistent(spec.clone()).await.unwrap();
        git(&bare, &["worktree", "lock", ws.path.to_str().unwrap()]);
        assert!(mgr.list().await.unwrap()[0].locked);

        // 判定龄为零：锁按尸体处理，解锁后树被复用而不是重建。
        let again = mgr.ensure_persistent(spec).await.unwrap();
        assert_eq!(again.path, ws.path);
        assert!(!mgr.list().await.unwrap()[0].locked);
    }
}
