//! `GET /api/v1/delivery/build-gate` —— 宿主构建槽现在是什么样子。
//!
//! 这台机器同时跑集群和喂集群的构建。一次「同时几条构建」的界不是配置出来的，是宿主
//! 实测导出的，因此只有放在一个**每个构建方都能加入**的地方才管得住：一个目录、每个位子
//! 一个 `flock` 文件、界写在文件里。集群里的构建器、宿主上的包装器、每个 worktree 的全量
//! 门都是它的客户；客户越多，「现在谁占着、谁排着、这台机器有多堵」这个问题就越只能由
//! 一个地方回答。
//!
//! 这一面就是那个地方的读数。它不新建存储：字节已经在盘上了，这里只解析并说出三件事：
//!
//! - **谁占着**：每个 `slot-<n>.lock` 的持有者记录（`<pid>\t<epoch>\t<名字>`）。一个位子
//!   是不是被占着由内核回答（对这个文件做一次非阻塞独占 `flock`），不由记录文件在不在
//!   回答——一个被复制、被遗留的文件会让「空着」读成「占着」，而读的人会去找一条不存在
//!   的构建。
//! - **谁排着**：`builds.reserve` 里那个「下一个构建归我」的声明。它的寿命由新鲜窗决定，
//!   写它的人不再刷新就自己作废，所以读数要带**多久没刷新**，不能只给一个名字。
//! - **有多堵**：`/proc/pressure/{io,cpu,memory}` 与 `/proc/loadavg`，和槽位同一瞬间读。
//!   两件事分开读的读数答不了「是构建把这台机器压住了，还是别的东西」。
//!
//! 三种结局必须分得开，而且页面要把理由印出来：
//!
//! - `measured`：目录在、读得到，下面每一格都是这一拍的真读数；
//! - `unwired`：这个部署没有把槽目录挂进来（目录不在）——不是「没有构建」；
//! - `unread`：目录在、读不动——不是「空着」。
//!
//! 空目录与没有目录在页面上长得一样，处置却相反：一个是改挂载，一个是真没人构建。

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use serde::Serialize;

/// 槽目录。与集群里的闸门、宿主上的包装器同一个键，所以部署里只有一处设置。
///
/// 名字声明在 `cog_core::paths`，因为配置加载器也读它：这个名字只能拼一次，
/// 两处各写一遍时改名只改一处不会报错，另一处静默失效。
pub const GATE_DIR_ENV: &str = cog_core::paths::BUILD_GATE_LOCK_DIR_ENV;
/// 声明的新鲜窗。默认值与 `cog_core::BuildGateConfig` 那一格同源，不另抄一个数。
pub const RESERVE_FRESH_ENV: &str = cog_core::paths::BUILD_GATE_RESERVE_FRESH_SECS_ENV;

const BOUND_FILE: &str = "buildgate.bound";
const RESERVE_FILE: &str = "builds.reserve";
const SLOT_PREFIX: &str = "slot-";
const SLOT_SUFFIX: &str = ".lock";

/// 推之前那道门把每一格的秒数与结局写在这个子目录里，与槽位文件分开：
/// 位子那一面按 `slot-*.lock` 读，两边互不干扰。
const RUNS_SUBDIR: &str = "runs";
/// 收尾的记号是改名，不另写终止行：多一种行，读的时候就要多认一种。
const RUN_DONE_SUFFIX: &str = ".segments";
const RUN_LIVE_SUFFIX: &str = ".running";
/// 交付链上共用这份记录的三段。闭集：写的那一侧按这三个词落文件名，读的这一侧按后缀认。
const RUN_STAGES: [&str; 3] = ["precommit", "gate", "prepush"];
/// 文件名里没有阶段的那一种，就是整门——记录最早只有这一种。
const LEGACY_STAGE: &str = "gate";
// cap: bound — 每棵树每阶段各留一跑；三阶段 × 两棵在动的树，再留一点余量
const MAX_RUNS: usize = 8;
// cap: bound — 一跑最多带几格；一道门一百多格，上千格不是读数多，是有人在灌
const MAX_RUN_STEPS: usize = 512;

/// 一格读数。`state` 是闭集里的一个词，页面据此决定画读数还是画理由。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GateReport {
    pub state: String,
    /// `unwired` / `unread` 的原因；`measured` 时为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    pub dir: String,
    /// 这一拍认为同时可以跑几条。读不到就是 `None` 带原因，不是 1。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound: Option<u32>,
    /// 界那份文件多久没被写过。界是会动的读数，不是常量。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_written_secs_ago: Option<u64>,
    pub slots: Vec<SlotReading>,
    /// 这份界不覆盖谁，以及它受什么约束。
    ///
    /// 带内沙箱执行器在自己的 pod 里编译，受限的是那个 pod 的 cpu / memory quota，
    /// 不是这个目录里的位子——两层界不是同一件事。它不出现在 `slots` 里是对的；这一格
    /// 把「不在这个界里」和「没有界」分开，否则读数面会替一个真实存在的约束说没有。
    pub outside_the_bound: Vec<OutsideReading>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reservation: Option<ReservationReading>,
    pub pressure: Vec<PressureReading>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load: Option<LoadReading>,
    /// 推之前那道门最近跑过的那几跑：每格花了多少秒、结局是什么。
    ///
    /// 这一面回答的是「刚才那一推慢在哪」，而槽位那一面回答的是「现在谁在编译」——
    /// 两个问题不对等：一道两小时的门跑完之后，槽位读数恢复成「空着」，读者从这里
    /// 再也看不出时间花在哪一格。记录就落在同一个目录里，所以这里加的是读数，不是
    /// 通道。
    ///
    /// 空列表不是错误：这道门是后加的记录，槽目录里还没有它写过的东西；`runs_why`
    /// 只说「读不动」那一种。
    pub runs: Vec<RunReading>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runs_why: Option<String>,
    pub generated_at: String,
}

/// 推之前那道门的一跑：在哪棵树上、跑到哪儿了、每一格多少秒。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RunReading {
    /// 这一跑所在的树。目录名把路径里的 `/` 换成了 `_`，读者据此认出是哪棵工作树。
    pub tree: String,
    /// 这是交付链上的哪一段：`precommit` / `gate` / `prepush`。
    ///
    /// 三个钩子共用同一份记录实现，读者按阶段把一跑放回链路上对应的那一格；没有
    /// 这一格，一段 4 秒的提交前钩子会顶掉两小时的整门，成为页面上「最近一跑」。
    pub stage: String,
    /// 从开跑到这一刻过了多久。跑完的那一份是这一跑的时长。
    pub started_secs_ago: u64,
    /// 这一跑有没有收尾。没收尾的是「还在跑，或者被杀了」——记录本身不替它猜是哪一种。
    pub finished: bool,
    /// 最后一次落笔是多久以前。没收尾的那一跑，这一格是它「停了多久」。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_write_secs_ago: Option<u64>,
    /// 各格秒数之和。它不等于这一跑的墙钟时长：格子之间与构建槽排队的时间不在里面。
    pub total_secs: u64,
    pub red: usize,
    pub unjudged: usize,
    /// 记录在、内容读不出来（权限、是个目录、坏编码）时的原因。
    ///
    /// 没有这一格，读不动与「这一跑一格都没记」在页面上同形：两份都是空表配合计 0。
    /// 读者是另一个 uid（Pod 里 1001，宿主上是跑门的那个人），所以这条不是理论上的：
    /// 门在一个把 `umask` 收成 077 的 shell 里跑过，记录就是 0600，Pod 只读得到目录。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_error: Option<String>,
    pub steps: Vec<StepReading>,
}

/// 一跑里的一格：与这道门印成本表用的三列同一个来源。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StepReading {
    pub seconds: u64,
    /// `ok` / `RED` / `UNJUDGED`，由写记录的那一侧判定，这里原样转手。
    pub verdict: String,
    pub what: String,
}

/// 一份界之外的构建方：它是谁，以及它受什么约束。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OutsideReading {
    pub who: String,
    pub why: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SlotReading {
    /// `slot-0` 这样的位子名，与盘上的文件名一致，读者能拿它去对照。
    pub name: String,
    pub held: bool,
    /// 持有者记录：`<名字>`，加上它从什么时候开始占着。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder: Option<HolderReading>,
    /// 位子被占着但没人留下记录。这是「旧版本的构建器」与「记录没写成」两种情形共用的
    /// 读数，所以它自己是一句话，不与 `holder: None` 混为一谈（后者是空位子）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unrecorded: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HolderReading {
    pub what: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub held_secs: Option<u64>,
}

/// 排队的那个人，以及它多久没刷新过自己的声明——后者决定它还算不算数。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReservationReading {
    pub who: String,
    pub renewed_secs_ago: Option<u64>,
    pub fresh: bool,
    pub fresh_window_secs: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PressureReading {
    /// `io` / `cpu` / `memory`
    pub resource: String,
    pub avg10: String,
    pub avg60: String,
    pub avg300: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LoadReading {
    pub load1: String,
    pub load5: String,
    pub load15: String,
}

pub async fn build_gate_handler() -> Json<GateReport> {
    Json(read_gate(
        non_empty_env(GATE_DIR_ENV).as_deref(),
        reserve_fresh_secs(),
        &proc_root(),
    ))
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// 页面上那颗「多久没刷新」的钟与写声明的那颗必须是同一颗，所以取的是同一格配置。
fn reserve_fresh_secs() -> u64 {
    non_empty_env(RESERVE_FRESH_ENV)
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cog_core::BuildGateConfig::default().reserve_fresh_secs)
}

/// 读 `/proc` 的根。测试用它指向一份夹具，生产里就是 `/proc`。
fn proc_root() -> PathBuf {
    non_empty_env("COGNEVA_PROC_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/proc"))
}

pub fn default_gate_dir() -> PathBuf {
    // 没有配置时与集群里那一份同一条默认路径：部署里网关与演进进程读的是同一个目录，
    // 两处各自算一个默认值迟早会分叉。
    PathBuf::from(cog_core::paths::BUILD_GATE_LOCK_DIR)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 拼这一拍。`dir` 为 `None` 是「这个部署没给槽目录」——不是「没有构建」。
pub fn read_gate(dir: Option<&str>, fresh_window: u64, proc_root: &Path) -> GateReport {
    let generated_at = chrono::Utc::now().to_rfc3339();
    let pressure = read_pressure(proc_root);
    let load = read_load(proc_root);
    let path = dir.map(PathBuf::from).unwrap_or_else(default_gate_dir);
    let shown = path.display().to_string();

    let base = |state: &str, why: Option<String>| GateReport {
        state: state.to_string(),
        why,
        dir: shown.clone(),
        bound: None,
        bound_written_secs_ago: None,
        slots: Vec::new(),
        outside_the_bound: outside_the_bound(),
        reservation: None,
        pressure: pressure.clone(),
        load: load.clone(),
        runs: Vec::new(),
        runs_why: None,
        generated_at: generated_at.clone(),
    };

    if !path.exists() {
        return base(
            "unwired",
            Some(format!(
                "这个部署没有把构建槽目录挂进来（{GATE_DIR_ENV} = {shown}）：这里读不到谁在构建，不是没有构建"
            )),
        );
    }
    let entries = match std::fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(e) => {
            return base(
                "unread",
                Some(format!("槽目录 {shown} 读不动（{e}）：这里报不出谁在构建")),
            )
        }
    };

    let mut slots: Vec<SlotReading> = Vec::new();
    let mut bound_path: Option<PathBuf> = None;
    let mut reserve_reading = None;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name == BOUND_FILE {
            bound_path = Some(entry.path());
        } else if name == RESERVE_FILE {
            reserve_reading = Some(read_reservation(&entry.path(), fresh_window));
        } else if let Some(slot) = name
            .strip_prefix(SLOT_PREFIX)
            .and_then(|rest| rest.strip_suffix(SLOT_SUFFIX))
        {
            let _ = slot;
            slots.push(read_slot(&entry.path(), &slots_name(&name)));
        }
    }
    // 位子按编号排：盘上的顺序是目录实现的事，读者要按「第几个」读。
    slots.sort_by_key(|s| slot_index(&s.name));

    let (bound, bound_age) = match bound_path.as_deref() {
        Some(p) => read_bound(p),
        None => (None, None),
    };
    let (runs, runs_why) = read_runs(&path);

    GateReport {
        state: "measured".to_string(),
        why: None,
        dir: shown,
        bound,
        bound_written_secs_ago: bound_age,
        slots,
        outside_the_bound: outside_the_bound(),
        reservation: reserve_reading,
        pressure,
        load,
        runs,
        runs_why,
        generated_at,
    }
}

/// 读最近的那几跑门。返回的第二个值是「读不动」的理由——目录不在**不是**理由：
/// 那是「这道门还没往这里写过」，与「读不出来」处置相反，页面必须分得开。
fn read_runs(dir: &Path) -> (Vec<RunReading>, Option<String>) {
    let runs_dir = dir.join(RUNS_SUBDIR);
    if !runs_dir.exists() {
        return (Vec::new(), None);
    }
    let entries = match std::fs::read_dir(&runs_dir) {
        Ok(entries) => entries,
        Err(e) => {
            return (
                Vec::new(),
                Some(format!(
                    "{} 读不动（{e}）：这里报不出最近几跑花了多少",
                    runs_dir.display()
                )),
            )
        }
    };
    let mut found: Vec<FoundRun> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some((tree, stage, started, finished)) = parse_run_name(&name) {
            found.push(FoundRun {
                tree,
                stage,
                started,
                finished,
                path,
            });
        }
    }
    // 新的一跑在前：读者要的是「刚才那一推」，不是「最早那一推」。
    found.sort_by_key(|run| std::cmp::Reverse(run.started));
    // 每一格只留最近的一跑：提交前钩子、整门、推之前钩子各占一格，按阶段各取最新的
    // 那一份。不分组的话，一段几秒的钩子会把两小时的那一跑挤出上限，链路上就再也
    // 看不见最贵的那一段。
    let mut seen: Vec<(String, String)> = Vec::new();
    found.retain(|run| {
        let key = (run.tree.clone(), run.stage.clone());
        if seen.contains(&key) {
            return false;
        }
        seen.push(key);
        true
    });
    let now = now_secs();
    let runs = found
        .into_iter()
        .take(MAX_RUNS)
        .map(|run| read_run(&run, now))
        .collect();
    (runs, None)
}

struct FoundRun {
    tree: String,
    stage: String,
    started: u64,
    finished: bool,
    path: PathBuf,
}

/// 文件名就是这一跑的身份：`<树>-<阶段>-<开跑时刻>-<pid>[.running|.segments]`。
///
/// 树的路径里本来就有 `-` 和数字，所以从**右边**切两刀：最后一段是 pid，倒数第二段
/// 是开跑时刻，剩下的整段是「树 + 阶段」。阶段是闭集里的三个词之一，按后缀认；
/// 认不出就不切——那样整段都是树名，是这份记录最早的形状（那时只有整门一种）。
/// pid 不进读数：读者要的是「哪一跑」，不是「谁的」。
fn parse_run_name(name: &str) -> Option<(String, String, u64, bool)> {
    let (base, finished) = if let Some(base) = name.strip_suffix(RUN_DONE_SUFFIX) {
        (base, true)
    } else {
        (name.strip_suffix(RUN_LIVE_SUFFIX)?, false)
    };
    let mut parts = base.rsplitn(3, '-');
    let pid = parts.next()?;
    if pid.is_empty() || !pid.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let started = parts.next()?.parse::<u64>().ok()?;
    let rest = parts.next()?;
    let (tree, stage) = split_stage(rest);
    if tree.is_empty() {
        return None;
    }
    Some((tree.to_string(), stage.to_string(), started, finished))
}

/// 把「树 + 阶段」切成两段。阶段取闭集里的词，按 `-<词>` 后缀认。
fn split_stage(rest: &str) -> (&str, &str) {
    for stage in RUN_STAGES {
        if let Some(tree) = rest.strip_suffix(&format!("-{stage}")) {
            return (tree, stage);
        }
    }
    (rest, LEGACY_STAGE)
}

fn read_run(run: &FoundRun, now: u64) -> RunReading {
    let last_write_secs_ago = std::fs::metadata(&run.path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| now.saturating_sub(d.as_secs()));
    let (text, read_error) = match std::fs::read_to_string(&run.path) {
        Ok(text) => (text, None),
        Err(e) => (
            String::new(),
            Some(format!(
                "{} 读不动（{e}）",
                run.path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| run.path.display().to_string())
            )),
        ),
    };
    let mut steps: Vec<StepReading> = Vec::new();
    let mut total_secs = 0u64;
    let mut red = 0usize;
    let mut unjudged = 0usize;
    for line in text.lines() {
        if steps.len() >= MAX_RUN_STEPS {
            break;
        }
        let mut columns = line.split('\t');
        let Some(seconds) = columns.next().and_then(|s| s.trim().parse::<u64>().ok()) else {
            continue;
        };
        let verdict = columns.next().unwrap_or("").trim().to_string();
        let what = columns.next().unwrap_or("").trim().to_string();
        if verdict.is_empty() || what.is_empty() {
            continue;
        }
        total_secs += seconds;
        match verdict.as_str() {
            "RED" => red += 1,
            "UNJUDGED" => unjudged += 1,
            _ => {}
        }
        steps.push(StepReading {
            seconds,
            verdict,
            what,
        });
    }
    RunReading {
        tree: run.tree.clone(),
        stage: run.stage.clone(),
        started_secs_ago: now.saturating_sub(run.started),
        finished: run.finished,
        last_write_secs_ago,
        total_secs,
        red,
        unjudged,
        read_error,
        steps,
    }
}

/// 这一族构建方不受宿主界的约束，读数要说出来。
///
/// 它有自己的 pod cpu / memory quota，编译在 pod 里跑；宿主槽位那份 flock 与它无关。
/// 两者的界不是同一件事，所以它不出现在 `slots` 里——但「不在 slots 里」本身读不出
/// 这件事，于是列在这里，由页面原样印出。
fn outside_the_bound() -> Vec<OutsideReading> {
    vec![OutsideReading {
        who: cog_core::topology::SANDBOX_EXECUTOR.to_string(),
        why: "在 pod 内编译，受 pod 的 cpu / memory quota 约束，不占宿主的构建槽位".to_string(),
    }]
}

fn slots_name(file: &str) -> String {
    file.trim_end_matches(SLOT_SUFFIX).to_string()
}

fn slot_index(name: &str) -> u32 {
    name.strip_prefix(SLOT_PREFIX)
        .and_then(|n| n.parse().ok())
        .unwrap_or(u32::MAX)
}

fn read_bound(path: &Path) -> (Option<u32>, Option<u64>) {
    let age = std::fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| now_secs().saturating_sub(d.as_secs()));
    let bound = std::fs::read_to_string(path)
        .ok()
        .and_then(|t| t.trim().parse::<u32>().ok())
        .filter(|b| *b > 0);
    (bound, age)
}

/// 这个位子现在有没有人占着。
///
/// 判据是内核对这个文件的锁，不是记录文件在不在：位子文件会比占着它的构建活得久
/// （构建结束、文件还在），把「文件非空」当「占着」会让读者去找一条已经结束的构建。
fn read_slot(path: &Path, name: &str) -> SlotReading {
    let held = slot_is_held(path);
    let record = std::fs::read_to_string(path).ok();
    let mut slot = SlotReading {
        name: name.to_string(),
        held,
        holder: None,
        unrecorded: None,
    };
    if !held {
        return slot;
    }
    match record.as_deref().and_then(parse_holder) {
        Some(holder) => slot.holder = Some(holder),
        None => {
            slot.unrecorded =
                Some("位子被占着，但持有者没留下记录（旧版本的构建器，或记录没写成）".to_string())
        }
    }
    slot
}

fn parse_holder(text: &str) -> Option<HolderReading> {
    let line = text.lines().next()?.trim();
    let mut parts = line.split('\t');
    let pid = parts.next()?.trim().parse::<u32>().ok();
    let since = parts.next().and_then(|s| s.trim().parse::<u64>().ok());
    let what = parts.next().unwrap_or("").trim().to_string();
    if what.is_empty() {
        return None;
    }
    Some(HolderReading {
        what,
        pid,
        held_secs: since.map(|s| now_secs().saturating_sub(s)),
    })
}

fn slot_is_held(path: &Path) -> bool {
    use std::os::unix::io::AsRawFd;
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        // 打不开就当占着：读不到与空着是两件事，只有一件意味着不用等。
        Err(_) => return true,
    };
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        return false;
    }
    true
}

fn read_reservation(path: &Path, fresh_window: u64) -> ReservationReading {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let line = text.trim().to_string();
    let (who, renewed) = match line.split_once('\t') {
        Some((who, since)) => (
            who.trim().to_string(),
            since
                .trim()
                .parse::<u64>()
                .ok()
                .map(|s| now_secs().saturating_sub(s)),
        ),
        None => (line, None),
    };
    ReservationReading {
        who,
        renewed_secs_ago: renewed,
        fresh: renewed.map(|age| age <= fresh_window).unwrap_or(false),
        fresh_window_secs: fresh_window,
    }
}

fn read_pressure(proc_root: &Path) -> Vec<PressureReading> {
    let mut out = Vec::new();
    for resource in ["io", "cpu", "memory"] {
        let Ok(text) = std::fs::read_to_string(proc_root.join("pressure").join(resource)) else {
            continue;
        };
        let Some(line) = text.lines().find(|l| l.starts_with("some ")) else {
            continue;
        };
        let field = |key: &str| {
            line.split_whitespace()
                .find_map(|part| part.strip_prefix(key))
                .unwrap_or("")
                .to_string()
        };
        out.push(PressureReading {
            resource: resource.to_string(),
            avg10: field("avg10="),
            avg60: field("avg60="),
            avg300: field("avg300="),
        });
    }
    out
}

fn read_load(proc_root: &Path) -> Option<LoadReading> {
    let text = std::fs::read_to_string(proc_root.join("loadavg")).ok()?;
    let mut parts = text.split_whitespace();
    Some(LoadReading {
        load1: parts.next()?.to_string(),
        load5: parts.next()?.to_string(),
        load15: parts.next()?.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::io::AsRawFd;

    /// 界之外的构建方要在读数里点名。它不在 `slots` 里是对的，但「不在 slots 里」
    /// 读不出「它有自己的界」——没有这一格，一个在 pod 里跑编译的执行器，在这份
    /// 读数上与「根本没人在编译」同形。
    #[test]
    fn the_reading_names_the_builder_that_is_bound_by_its_pod_instead() {
        let gate_dir = fixture();
        let report = read_gate(
            Some(gate_dir.to_str().expect("夹具目录是 UTF-8")),
            900,
            &stand_in_proc_root(),
        );
        let who: Vec<&str> = report
            .outside_the_bound
            .iter()
            .map(|outside| outside.who.as_str())
            .collect();
        assert!(
            who.contains(&cog_core::topology::SANDBOX_EXECUTOR),
            "界之外的那一族要点名：{who:?}"
        );
        assert!(
            report
                .outside_the_bound
                .iter()
                .any(|outside| outside.why.contains("quota")),
            "要说清它受什么约束，否则「不在界里」还是读成「没有界」：{:?}",
            report.outside_the_bound
        );
    }

    /// 一份什么都读不到的 proc 根：这条用例只看 `outside_the_bound`，与槽位无关。
    fn stand_in_proc_root() -> PathBuf {
        std::env::temp_dir().join(format!(
            "cogneva-buildgate-proc-{}-{}",
            std::process::id(),
            now_secs()
        ))
    }

    fn fixture() -> PathBuf {
        // 一条进程里并行跑的那些用例共用同一个 pid 与同一秒，只靠这两个数取目录会让
        // 它们互相删对方的夹具——失败会印成「界读不到」，而原因在测试自己身上。
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "cogneva-buildgate-{}-{}-{}",
            std::process::id(),
            now_secs(),
            seq
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn proc_fixture(pressure_io: Option<&str>, load: Option<&str>) -> PathBuf {
        let dir = fixture();
        std::fs::create_dir_all(dir.join("pressure")).unwrap();
        if let Some(text) = pressure_io {
            std::fs::write(dir.join("pressure/io"), text).unwrap();
        }
        if let Some(text) = load {
            std::fs::write(dir.join("loadavg"), text).unwrap();
        }
        dir
    }

    #[test]
    fn a_directory_that_is_not_there_says_it_is_not_mounted_rather_than_nothing_running() {
        let missing = std::env::temp_dir().join("cogneva-buildgate-absent-does-not-exist");
        let report = read_gate(
            Some(missing.to_str().unwrap()),
            900,
            &proc_fixture(None, None),
        );
        assert_eq!(report.state, "unwired");
        assert!(
            report.why.as_deref().unwrap_or("").contains("不是没有构建"),
            "理由要说清「读不到」与「没有」的区别：{:?}",
            report.why
        );
        assert!(report.slots.is_empty());
    }

    #[test]
    fn a_measured_gate_reports_the_bound_every_slot_and_who_holds_it() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "2\n").unwrap();
        std::fs::write(dir.join("slot-0.lock"), "").unwrap();
        let mut held = std::fs::File::create(dir.join("slot-1.lock")).unwrap();
        // 持有者记录：这一格的名字与它占住的时刻。
        writeln!(
            held,
            "4242\t{}\tgate: cargo test -p cogneva",
            now_secs().saturating_sub(30)
        )
        .unwrap();
        // 真把它锁住，读数才有一格是「占着」。
        let rc = unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(rc, 0, "夹具自己要能把这一格锁上");

        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert_eq!(report.state, "measured");
        assert_eq!(report.bound, Some(2));
        assert_eq!(report.slots.len(), 2);
        assert_eq!(report.slots[0].name, "slot-0");
        assert!(!report.slots[0].held, "没被锁的位子是空位子");
        assert_eq!(report.slots[1].name, "slot-1");
        assert!(report.slots[1].held);
        let holder = report.slots[1].holder.as_ref().expect("有记录就该读出来");
        assert_eq!(holder.what, "gate: cargo test -p cogneva");
        assert_eq!(holder.pid, Some(4242));
        assert!(
            holder.held_secs.unwrap_or(0) >= 30,
            "占住的时长要真的算出来：{:?}",
            holder.held_secs
        );
    }

    #[test]
    fn a_held_slot_with_no_record_is_held_and_unnamed_rather_than_free() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "1\n").unwrap();
        // 锁上但一个字都不写：旧版本的构建器就是这个形状。
        let held = std::fs::File::create(dir.join("slot-0.lock")).unwrap();
        let rc = unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(rc, 0, "夹具自己要能把这一格锁上");
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert!(report.slots[0].held);
        assert!(report.slots[0].holder.is_none());
        assert!(
            report.slots[0].unrecorded.is_some(),
            "占着但没名字要说出来，不能读成空位子"
        );
    }

    #[test]
    fn a_reservation_carries_its_age_because_age_is_what_makes_it_count() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "1\n").unwrap();
        std::fs::write(
            dir.join(RESERVE_FILE),
            format!("gate: clippy\t{}", now_secs().saturating_sub(1200)),
        )
        .unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        let reservation = report.reservation.expect("声明要读出来");
        assert_eq!(reservation.who, "gate: clippy");
        assert!(
            reservation.renewed_secs_ago.unwrap_or(0) >= 1200,
            "声明要带出它多久没刷新：{:?}",
            reservation.renewed_secs_ago
        );
        assert!(
            !reservation.fresh,
            "过了新鲜窗的声明不再算数，页面不能只印名字"
        );
    }

    #[test]
    fn the_bound_is_a_reading_with_an_age_not_a_constant() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "3\n").unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert_eq!(report.bound, Some(3));
        assert!(
            report.bound_written_secs_ago.is_some(),
            "界是实测导出的，读者要知道它多久没重算过"
        );
    }

    #[test]
    fn an_unreadable_bound_is_null_not_one() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "not a number\n").unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert_eq!(report.bound, None, "量不到与量到 1 必须分得开");
    }

    #[test]
    fn pressure_and_load_are_read_or_left_out_together() {
        let proc = proc_fixture(
            Some("some avg10=31.83 avg60=31.83 avg300=31.25 total=701980500817\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"),
            Some("9.36 8.23 11.97 3/1234 5678\n"),
        );
        let report = read_gate(None, 900, &proc);
        let io = report
            .pressure
            .iter()
            .find(|p| p.resource == "io")
            .expect("io 那一行要读出来");
        assert_eq!(io.avg10, "31.83");
        let load = report.load.expect("loadavg 要读出来");
        assert_eq!(load.load1, "9.36");
    }

    #[test]
    fn a_host_without_pressure_files_says_nothing_rather_than_zero() {
        let proc = proc_fixture(None, None);
        let report = read_gate(None, 900, &proc);
        assert!(report.pressure.is_empty(), "没有那一行就不印 0");
        assert!(report.load.is_none());
    }

    /// 一跑写完的那一份：三列照读，红的步数单独数出来——读者先要看见「哪一格红」，
    /// 其次才是「一共花了多少」。
    #[test]
    fn a_finished_run_carries_every_step_and_counts_the_red_ones() {
        let dir = fixture();
        let runs = dir.join(RUNS_SUBDIR);
        std::fs::create_dir_all(&runs).unwrap();
        std::fs::write(
            runs.join("home_cogneva_work_wt-emp2-1700000000-4242.segments"),
            "900\tok\tTest（整棵工作区）\n61\tok\tClippy\n4\tRED\tFormat\n",
        )
        .unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert!(report.runs_why.is_none(), "{:?}", report.runs_why);
        let run = report.runs.first().expect("写完的那一跑要读出来");
        assert_eq!(run.tree, "home_cogneva_work_wt-emp2");
        assert!(run.finished);
        assert_eq!(run.steps.len(), 3);
        assert_eq!(run.total_secs, 965);
        assert_eq!(run.red, 1);
        assert_eq!(run.steps[2].verdict, "RED");
        assert_eq!(run.steps[2].what, "Format");
    }

    /// 没收尾的那一份：它还留在 `.running` 上。读数只说「没收尾」，不替它猜是还在跑
    /// 还是被杀了——两种处置不同，读数分不出来就不许编。
    #[test]
    fn an_unfinished_run_says_so_instead_of_inventing_a_verdict() {
        let dir = fixture();
        let runs = dir.join(RUNS_SUBDIR);
        std::fs::create_dir_all(&runs).unwrap();
        std::fs::write(
            runs.join("home_cogneva_work_wt-emp2-1700000000-4242.running"),
            "12\tok\tFormat\n",
        )
        .unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        let run = report.runs.first().expect("没收尾的那一跑也要读出来");
        assert!(!run.finished);
        assert_eq!(run.steps.len(), 1, "跑到哪儿就读到哪儿");
        assert!(
            run.last_write_secs_ago.is_some(),
            "「停了多久」是这一格唯一的证据：{:?}",
            run.last_write_secs_ago
        );
    }

    /// 阶段进文件名、进读数；没有阶段的那一种（记录最早只有整门）读成 gate。
    #[test]
    fn a_run_carries_the_stage_it_belongs_to() {
        let dir = fixture();
        let runs = dir.join(RUNS_SUBDIR);
        std::fs::create_dir_all(&runs).unwrap();
        std::fs::write(
            runs.join("tree-precommit-300-1.segments"),
            "4\tok\t10-registered-producer-paths\n",
        )
        .unwrap();
        std::fs::write(runs.join("tree-200-1.segments"), "900\tok\tTest\n").unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        let stages: Vec<&str> = report.runs.iter().map(|r| r.stage.as_str()).collect();
        assert_eq!(
            stages,
            vec!["precommit", "gate"],
            "新的在前；没有阶段后缀的那一种读成整门"
        );
    }

    /// 每一格只留最近的一跑：一段几秒的钩子不能把两小时的整门顶出上限。
    #[test]
    fn each_stage_keeps_its_newest_run() {
        let dir = fixture();
        let runs = dir.join(RUNS_SUBDIR);
        std::fs::create_dir_all(&runs).unwrap();
        std::fs::write(runs.join("tree-gate-100-1.segments"), "1\tok\told\n").unwrap();
        std::fs::write(runs.join("tree-gate-200-1.segments"), "2\tok\tnew\n").unwrap();
        std::fs::write(runs.join("tree-prepush-300-1.segments"), "3\tok\trefs\n").unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        let gate_runs: Vec<&RunReading> =
            report.runs.iter().filter(|r| r.stage == "gate").collect();
        assert_eq!(gate_runs.len(), 1, "同一阶段只留最新的一跑");
        assert_eq!(gate_runs[0].steps[0].what, "new");
        assert!(
            report.runs.iter().any(|r| r.stage == "prepush"),
            "另一阶段那一跑没被整门的记录顶掉：{:?}",
            report.runs
        );
    }

    /// 槽目录在、`runs/` 不在：这是「这道门还没往这里写过」，不是读不动——两者处置
    /// 相反，读数面必须分得开。
    #[test]
    fn a_missing_runs_directory_is_no_record_yet_not_a_read_failure() {
        let dir = fixture();
        std::fs::write(dir.join(BOUND_FILE), "1\n").unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert!(report.runs.is_empty());
        assert!(
            report.runs_why.is_none(),
            "没有记录与读不动是两句话：{:?}",
            report.runs_why
        );
        assert_eq!(report.state, "measured", "槽位那一面照常读");
    }

    /// 形状不对的文件名不进读数：名字里没有开跑时刻，就归不到哪一跑上。
    #[test]
    fn a_run_name_without_a_start_time_is_ignored() {
        let dir = fixture();
        let runs = dir.join(RUNS_SUBDIR);
        std::fs::create_dir_all(&runs).unwrap();
        std::fs::write(runs.join("notes.segments"), "1\tok\tx\n").unwrap();
        std::fs::write(runs.join("tree-notanumber-1.segments"), "1\tok\tx\n").unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert!(report.runs.is_empty(), "{:?}", report.runs);
    }

    /// 灌进来的行不读：一跑最多带那么多格，多了不是读数多。
    #[test]
    fn a_run_carrying_more_steps_than_the_bound_is_truncated() {
        let dir = fixture();
        let runs = dir.join(RUNS_SUBDIR);
        std::fs::create_dir_all(&runs).unwrap();
        let mut text = String::new();
        for index in 0..(MAX_RUN_STEPS + 10) {
            text.push_str(&format!("1\tok\tstep {index}\n"));
        }
        std::fs::write(runs.join("tree-100-1.segments"), text).unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        assert_eq!(report.runs[0].steps.len(), MAX_RUN_STEPS);
    }

    /// 记录在、内容读不出来：这一格必须说「读不动」，不能读成「这一跑一格都没记」。
    ///
    /// 两份在页面上原本同形：空表配合计 0。而真实的成因不是假设——读的人与写的人是
    /// 两个 uid，写的那一侧把 `umask` 收紧了，权限就是 0600。
    /// 夹具用「同名目录」而不是 chmod 000：`read_to_string` 对它报 EISDIR，与跑测试的
    /// 是不是 root 无关。
    #[test]
    fn an_unreadable_run_says_so_instead_of_reading_as_empty() {
        let dir = fixture();
        let runs = dir.join(RUNS_SUBDIR);
        std::fs::create_dir_all(runs.join("tree-100-1.segments")).unwrap();
        let report = read_gate(Some(dir.to_str().unwrap()), 900, &proc_fixture(None, None));
        let run = report.runs.first().expect("记录在，就该有一跑");
        assert!(run.steps.is_empty());
        assert!(
            run.read_error.is_some(),
            "读不动被读成「一格都没记」：{run:?}"
        );
    }
}
