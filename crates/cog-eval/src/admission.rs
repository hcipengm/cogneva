//! 评测驱动的准入闸门：同一台宿主上，这一刻放行几例。
//!
//! 与 [`cog_core::contract::build_gate`] 是两件事，故意不合并。构建闸门的语义是「槽位＝
//! 一次构建，持有一把 flock」；这里的语义是「一个控制器 ＋ 一个会动的界」。两者的持有者、
//! 读数与生命周期都不同：把「一次构建」与「一格的例」记进同一套会计，谁在占机器就读不出来
//! 了。共用的是**目录身份**——同一台宿主上的构建方与评测方必须看同一个目录，否则各卡
//! 各的，而「我们是不是在互相让」这件事没有任何读数能回答。
//!
//! 界由控制律算出来，不是一个配置值：
//!
//! - **红灯看 `avg10`（要快）**：PSI 三项里任一项的 avg10 越过红线、可用内存掉到地板
//!   以下、这一刻有构建槽被持有、或自身这一例比它的历史中位数慢一倍以上——四条路任一
//!   条成立就减半。
//! - **绿灯看 `avg60`（避抖）**：四项全都不红，且 PSI 三项的 avg60 都在绿灯线以下、
//!   可用内存还高于自己的绿灯线，才加一。
//! - 四条红灯里的两条 PSI 线、内存地板与绿灯内存线**都在这台宿主上量过**（7d，单节点
//!   24C／62.7 GiB）。数值和它的来源写在各常量的注释里；改动它们之前先重跑那份测量。
//!   地板与绿灯内存线是**两个量**，各有来源，不许再写成「地板的倍数」——那是把两条
//!   各自可测的约束拴成一个，抬一侧会静默关死另一侧。
//!
//! 只有一个进程能当控制器：两个控制器会互相把界推来推去，读数上还分不出是谁写的。
//! 选主是同一个目录里一个文件的 flock——持锁者即控制器，进程死了 fd 关闭自动释放，
//! 与槽位是同一套哲学，不用 pid 文件。界写在同一个目录的一个文件里，不参与决策的进程
//! 也读得到，于是「谁在放行」只有一个答案。
//!
//! **这里的只是「拿读数做什么」那一半**：控制律、界、选主、放行凭据。**采那读数**的那一半
//! 在 [`cog_core::admission`]——`Pressure` 与它的采样器、构建槽的 flock 判定。它在 core
//! 是因为这块宿主上还有第二个读它的人：构建闸门（`cog-reflection`）要采同一份压力、数
//! 同一批槽位，而两个实现者谁也不能依赖对方。阈值留在这里而不是跟着采样走：它们是这条律
//! 的判断，换一条律就该重新量一次，而不是被任何读压力的人继承。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cog_core::admission::{sample, Pressure};
use tokio::sync::Notify;

/// 红灯：PSI 任一项的 `avg10` 越过这个百分比就减半。
///
/// 实测（7d，10080 分钟）：抓取间隔 30s 而 `avg10` 是 10s 窗，历史上读不到同窗口，
/// 按 30–60s 代理 worst 值越过 20 的分钟占 26%（io 25%、cpu 1.6%、memory 1.4%）；
/// 1s 粒度活采里 17% 的样本越线、最大 49.8 —— 这条线够得着，不是摆设。它对行为也有
/// 实权：把两条 PSI 红灯整个去掉，界停在 1 的时间从 54.9% 掉到 44.1%。
pub const PSI_RED_AVG10: f64 = 20.0;
/// 绿灯：PSI 三项的 `avg60` 全在这条线以下才加一。
///
/// 实测：5.0 落在这台宿主自己 worst_avg60 分布的第 67 分位（p50 1.51、p75 21.7），
/// 不是圆整数。但它是这条律里最不灵敏的旋钮：5 → 20 或 5 → 2，界停在 1 的时间只在
/// 53.8%–56.7% 之间动（一到三个百分点）。别把它当主要调节手段。
pub const PSI_GREEN_AVG60: f64 = 5.0;
/// 可用内存的地板，低于它减半。
///
/// 实测：7d MemAvailable 谷底 36.46 GiB、p5 46.4 GiB。地板要「有牙齿」——逐档量
/// 「内存是唯一决定者的分钟」：≤42 GiB 一周内一次都不是它决定的（PSI 先红），
/// 44 GiB 才出现 2 分钟、48 GiB 22 分钟。取最小的有点的那一档。
pub const MEM_FLOOR_BYTES: u64 = 44 * 1024 * 1024 * 1024;
/// 绿灯的可用内存线：高于它才加一。
///
/// 它与地板是两个量，各有实测来源。取 7d 分布的 p5（46.4 GiB）向下取整：绿灯只排除
/// 最干的 5% 分钟，同时与 44 GiB 地板之间留下约 2.4 GiB 的回滞带，界不会在地板附近
/// 抖着加一减半。
pub const MEM_GREEN_MIN_BYTES: u64 = 46 * 1024 * 1024 * 1024;
/// 编译期不变式：绿灯线高于地板才留得出回滞带。把绿灯写回「地板的倍数」会先撞
/// 这里，而不是等到宿主上永远不加一才发现。
const _: () = assert!(MEM_GREEN_MIN_BYTES > MEM_FLOOR_BYTES);
/// 判定「这一例偏慢」的倍数：比自己的历史中位数慢这么多倍才算红灯。
///
/// 它是个比例而不是一个时间：一道题该跑多久取决于这道题，闸门不知道。所以这条灯只能
/// 相对自己——宿主整体变慢由 PSI 那三条路说，不走这条。
const LATENCY_RED_FACTOR: f64 = 2.0;
/// 判中位数之前至少要看过几例。少于这个数，中位数还不是「这个负载的历史」。
const LATENCY_MIN_SAMPLES: usize = 5;

/// 界为什么动。四种动法各是一个读数：少了「顶到上界」与「贴着下界」，「界在 2」与
/// 「界卡在 2 上不去」在表里同形。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Action {
    Increased,
    Decreased,
    AtCeiling,
    AtFloor,
    /// 既不红也不绿：保持，等下一窗。
    Held,
    /// 还没算过（控制器刚起来，或这一刻没有压力读数）。
    None,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Increased => "increased",
            Self::Decreased => "decreased",
            Self::AtCeiling => "at-ceiling",
            Self::AtFloor => "at-floor",
            Self::Held => "held",
            Self::None => "none",
        }
    }
}

impl std::fmt::Debug for Admission {
    /// 打的是这一刻的读数，不是内部状态：包住它的那些配置（`RunConfig` 等）打出来时，
    /// 读的人要知道的正是「当时界是多少、为什么」，而不是一把锁。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Admission{:?}", self.reading())
    }
}
/// 这一刻是哪一盏红灯亮着，以及**亮在哪**。
///
/// 判词要说的是因（哪一项压力、多少），不是命（「界降了」）。所以它返回的是一句话而不是
/// 一个 bool：`None` ＝四项全不红。四条路各是一个不同的追查方向——PSI 说机器忙、
/// MemAvailable 说内存快没了、构建槽说对方正在占机器、自身延迟说这一例比自己的历史慢
/// 得多——合成一句「压力大」就等于把三种因说成了一个。
pub fn red_light(p: &Pressure, latency_red: bool) -> Option<String> {
    let (which, value) = p.worst_avg10();
    if value > PSI_RED_AVG10 {
        return Some(format!(
            "{which} is at {value:.2}% stalled over the last 10s, past the {PSI_RED_AVG10:.0}% line"
        ));
    }
    if p.mem_available_bytes < MEM_FLOOR_BYTES {
        return Some(format!(
            "MemAvailable is {:.1}GiB, under the {:.1}GiB floor",
            p.mem_available_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            MEM_FLOOR_BYTES as f64 / (1024.0 * 1024.0 * 1024.0),
        ));
    }
    if p.builds_in_flight > 0 {
        return Some(format!(
            "{} build slot(s) are held on this host: the build and the benchmark are the same machine",
            p.builds_in_flight
        ));
    }
    if latency_red {
        return Some(
            "the last case ran at more than twice its own median: this column is no longer the \
             workload the earlier cases were"
                .to_string(),
        );
    }
    None
}

/// 一次控制律判决，纯函数，好单独试。
///
/// 输入是四盏红灯与绿灯条件，输出是新的界与「为什么」。界永远落在 `[floor, ceiling]`
/// 里：红灯减半也只减到地板，绿灯加一只加到天花板。
pub fn decide(
    bound: usize,
    floor: usize,
    ceiling: usize,
    pressure: Option<&Pressure>,
    latency_red: bool,
) -> (usize, Action) {
    let Some(p) = pressure else {
        // 读不出压力就不动界：把「读不出」当成「没压力」会一路加到底，而那是这张表
        // 唯一一个「因为我们没看见所以放大」的方向。
        return (bound, Action::None);
    };
    if red_light(p, latency_red).is_some() {
        let next = (bound / 2).max(floor);
        if next == bound {
            return (bound, Action::AtFloor);
        }
        return (next, Action::Decreased);
    }
    let green = p.worst_avg60() < PSI_GREEN_AVG60 && p.mem_available_bytes > MEM_GREEN_MIN_BYTES;
    if green {
        if bound >= ceiling {
            return (bound, Action::AtCeiling);
        }
        return ((bound + 1).min(ceiling), Action::Increased);
    }
    (bound, Action::Held)
}

/// 这一例是不是偏慢：比自己的历史中位数慢 `LATENCY_RED_FACTOR` 倍以上。
fn latency_is_red(latencies: &[u64]) -> bool {
    if latencies.len() < LATENCY_MIN_SAMPLES {
        return false;
    }
    let latest = *latencies.last().expect("non-empty");
    let mut sorted: Vec<u64> = latencies.to_vec();
    sorted.sort_unstable();
    let median = sorted[sorted.len() / 2];
    median > 0 && latest as f64 > median as f64 * LATENCY_RED_FACTOR
}

/// 闸门的配置。
///
/// 它**不是**下发文档里的一节：这个闸门服务的是宿主上跑的评测驱动，不是 Pod。它的键
/// 是那个子命令自己的参数（`--admission-*`），跟 `--seeds`、`--subset` 同一条路。
#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    /// 闸门目录。必须与构建方是同一个目录，否则一方卡一方不卡。
    pub dir: PathBuf,
    /// 下界。**未实测**：这一版的 1 是「不许零」的形式下界，不是量出来的并行度。
    pub floor_unmeasured: usize,
    /// 上界。**未实测**：这一版取的是驱动原先写死的那个并发数，不是量出来的。
    pub ceiling_unmeasured: usize,
    /// 重新算一次界的最小间隔。
    pub window: Duration,
}

/// 未实测的下界：1，只是「不许是零」的形式下界。
const UNMEASURED_FLOOR: usize = 1;

/// 未实测的上界：4，取的是驱动原先写死的并发数。
const UNMEASURED_CEILING: usize = 4;

/// 重新算一次界的最小间隔：60s。
///
/// 界要跟着实测走，而一次实测本身就是一份读数；比一分钟更密的窗口会让界在
/// 噪声里来回跳，反而压不住并行度。
const ADMISSION_RECOMPUTE_WINDOW: Duration = Duration::from_secs(60);

impl AdmissionConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            floor_unmeasured: UNMEASURED_FLOOR,
            ceiling_unmeasured: UNMEASURED_CEILING,
            window: ADMISSION_RECOMPUTE_WINDOW,
        }
    }
}

/// 开一道闸门。同一个目录上的第二个进程拿到的是**只读的那一个**：界的值由控制器写在
/// 目录里，它读回来。
///
/// `Err` 只在一个意义上出现——**这个目录用不了**（不存在、不是目录）。这一条是硬失败，
/// 不是「没有闸门就照旧跑」：请求过闸门的调用方，等到的是一台不受保护的机器，而这只会在
/// 它已经被压住的时候才发现。
pub fn open(cfg: &AdmissionConfig) -> Result<Arc<Admission>, String> {
    let dir = cfg.dir.clone();
    // 目录要**已经存在**，这里不替它建一个。建了的话，指错目录（或构建方还没起过）就会
    // 得到一个崭新空旷的目录：闸门照样放行、照样报数，而它谁也不协调——一个「生效了」
    // 的外观下面什么都没有。缺席就该是缺席。
    if !dir.is_dir() {
        return Err(format!(
            "the gate directory {} is not a directory: point it at the same directory the build \
             gate uses, otherwise the two of them coordinate nothing",
            dir.display()
        ));
    }
    let is_controller = take_controller_lock(&dir);
    let ceiling = cfg.ceiling_unmeasured.max(cfg.floor_unmeasured).max(1);
    let floor = cfg.floor_unmeasured.clamp(1, ceiling);
    let bound = read_bound(&dir).unwrap_or(floor).clamp(floor, ceiling);
    if is_controller {
        // 控制器一起来就把自己的界写出去。否则从零到第一次判决之间，非控制器会各自
        // 退回自己的 `floor` 去猜——同一个目录上就有两个不同的界，而读数上看不出来。
        write_bound(&dir, bound);
    }
    Ok(Arc::new(Admission {
        inner: Arc::new(Inner {
            dir,
            floor,
            ceiling,
            window: cfg.window,
            is_controller,
            notify: Notify::new(),
            state: Mutex::new(Gate {
                bound,
                total_in_flight: 0,
                per_column: HashMap::new(),
                blocked: 0,
                last_step: None,
                pressure: None,
                action: Action::None,
                red: None,
                windows_at_floor: 0,
                latencies: Vec::new(),
            }),
        }),
    }))
}

/// 一次放行。持有期间这一例占着一个名额，`drop` 时归还。
pub struct Admission {
    inner: Arc<Inner>,
}

struct Inner {
    dir: PathBuf,
    floor: usize,
    ceiling: usize,
    window: Duration,
    is_controller: bool,
    notify: Notify,
    state: Mutex<Gate>,
}

struct Gate {
    bound: usize,
    total_in_flight: usize,
    per_column: HashMap<String, usize>,
    blocked: u64,
    last_step: Option<Instant>,
    pressure: Option<Pressure>,
    action: Action,
    /// 上一刻是哪一盏红灯亮的，原话。`None` ＝四项都不红。
    red: Option<String>,
    windows_at_floor: u32,
    latencies: Vec<u64>,
}

/// 一次放行的凭据。它一落，这一例就不再占名额。
pub struct Permit {
    inner: Arc<Inner>,
    column: String,
    held: Instant,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let held = self.held.elapsed();
        {
            let mut gate = self.inner.state.lock().unwrap();
            if let Some(n) = gate.per_column.get_mut(&self.column) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    gate.per_column.remove(&self.column);
                }
            }
            gate.total_in_flight = gate.total_in_flight.saturating_sub(1);
            gate.latencies.push(held.as_millis() as u64);
            if gate.latencies.len() > LATENCY_MIN_SAMPLES * 2 {
                gate.latencies.remove(0);
            }
        }
        self.inner.notify.notify_waiters();
    }
}

impl Admission {
    /// 要一次放行。
    ///
    /// 有效并发是**两个界的较小者**：这一列自己的隔离上界（例如 toolathlon 恒 1，它所有
    /// 任务共享一个 postgres）与闸门此刻的界。放行的判据就在这一处，例内部不再设卡。
    pub async fn acquire(self: &Arc<Self>, column: &str, column_ceiling: Option<usize>) -> Permit {
        let mut counted = false;
        loop {
            let wait = self.inner.notify.notified();
            {
                let mut gate = self.inner.state.lock().unwrap();
                if self.inner.is_controller {
                    self.inner.maybe_step(&mut gate);
                } else {
                    // 不是控制器就只读：界的值由控制器写在目录里，读回来取小。
                    if let Some(bound) = read_bound(&self.inner.dir) {
                        gate.bound = bound.clamp(self.inner.floor, self.inner.ceiling);
                    }
                }
                let ceiling = column_ceiling.unwrap_or(usize::MAX).min(gate.bound).max(1);
                let in_column = gate.per_column.get(column).copied().unwrap_or(0);
                if gate.total_in_flight < gate.bound && in_column < ceiling {
                    *gate.per_column.entry(column.to_string()).or_insert(0) += 1;
                    gate.total_in_flight += 1;
                    return Permit {
                        inner: self.inner.clone(),
                        column: column.to_string(),
                        held: Instant::now(),
                    };
                }
                if !counted {
                    gate.blocked += 1;
                    counted = true;
                }
            }
            wait.await;
        }
    }

    /// 这一刻的读数，给报表用。
    pub fn reading(&self) -> GateReading {
        let gate = self.inner.state.lock().unwrap();
        GateReading {
            bound: gate.bound,
            floor: self.inner.floor,
            ceiling: self.inner.ceiling,
            action: gate.action.as_str().to_string(),
            pressure: gate.pressure,
            red: gate.red.clone(),
            blocked: gate.blocked,
            windows_at_floor: gate.windows_at_floor,
            in_flight: gate.total_in_flight,
            controller: self.inner.is_controller,
            dir: self.inner.dir.display().to_string(),
            identity: dir_identity(&self.inner.dir),
        }
    }
}

impl Inner {
    /// 到一个窗口就问一次压力、动一次界。只有控制器调它。
    fn maybe_step(&self, gate: &mut Gate) {
        let due = gate
            .last_step
            .map(|t| t.elapsed() >= self.window)
            .unwrap_or(true);
        if !due {
            return;
        }
        let pressure = sample(&self.dir);
        let latency_red = latency_is_red(&gate.latencies);
        let red = pressure.as_ref().and_then(|p| red_light(p, latency_red));
        let (next, action) = decide(
            gate.bound,
            self.floor,
            self.ceiling,
            pressure.as_ref(),
            latency_red,
        );
        gate.pressure = pressure;
        gate.red = red;
        gate.action = action;
        gate.last_step = Some(Instant::now());
        if next == self.floor && action != Action::None {
            gate.windows_at_floor = gate.windows_at_floor.saturating_add(1);
        } else {
            gate.windows_at_floor = 0;
        }
        if next != gate.bound {
            gate.bound = next;
            write_bound(&self.dir, next);
        }
    }
}

/// 控制器这一刻的读数与它为什么是这个数。界本身不是一个配置值，所以它必须带着自己的
/// 来历出来：上一刻的压力读数、以及界为什么动。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct GateReading {
    pub bound: usize,
    pub floor: usize,
    pub ceiling: usize,
    pub action: String,
    pub pressure: Option<Pressure>,
    /// 上一刻哪一盏红灯亮着、亮在哪。它是「界为什么在这个数上」的**因**；没有它，读的人
    /// 只能看到「界降了」这个**命**，然后去猜是机器忙还是构建在跑。
    #[serde(default)]
    pub red: Option<String>,
    /// 被闸门挡住的例数（进过等待的那些，各记一次）。
    pub blocked: u64,
    /// 界连着几个窗口贴着下界。它不是「界低」这件事本身，是「这件事持续了多久」。
    ///
    /// 窗口是**有人来要票时才走的**：闸门不在后台跑定时器，它只在有人要进来时看一眼压力。
    /// 所以这个数是「最近这么多次判断都贴着下界」，不是墙上时钟过了多久——没人要进来的时候
    /// 界停在哪儿都没有影响。
    pub windows_at_floor: u32,
    pub in_flight: usize,
    pub controller: bool,
    pub dir: String,
    /// 目录的 `dev:ino`。两个进程报同一个身份才说明它们在互相让。
    pub identity: String,
}

/// 目录身份：设备号 ＋ inode。读的是挂载表意义上的那个目录，不是某个文件系统。
///
/// 复用构建闸门那一份，不在这里再写一遍：这两个读数要能**并到同一个目录上**，各写一份
/// 就等于有两个「同一个目录」的判据，而它们完全可以不一致。
fn dir_identity(dir: &Path) -> String {
    cog_core::contract::build_gate::dir_identity(dir)
}

fn bound_path(dir: &Path) -> PathBuf {
    dir.join("admission.bound")
}

fn read_bound(dir: &Path) -> Option<usize> {
    let text = std::fs::read_to_string(bound_path(dir)).ok()?;
    text.trim().parse().ok()
}

/// 界的值写在目录里而不是只留在内存：非控制器进程也要读到同一个界，否则「谁在放行」
/// 有两个答案。写失败只说一句——界的值仍然是这一进程用的那个。
fn write_bound(dir: &Path, bound: usize) {
    if let Err(e) = std::fs::write(bound_path(dir), format!("{bound}\n")) {
        tracing::warn!(dir = %dir.display(), error = %e, "cannot publish the admission bound");
    }
}

/// 拿控制器的锁。拿不到就说明这一刻已经有控制器，本进程只读。
#[cfg(unix)]
fn take_controller_lock(dir: &Path) -> bool {
    let Ok(file) = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join("admission.controller"))
    else {
        return false;
    };
    use std::os::unix::io::AsRawFd;
    // SAFETY: flock 只碰本进程这张描述符表里、由本作用域持有的这一个描述符；
    // EWOULDBLOCK 是预期答案，没有内存上的义务。锁随进程消失而释放。
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        // 锁要跟着进程活：把它泄给内核，靠 fd 关闭释放。
        std::mem::forget(file);
        true
    } else {
        false
    }
}

#[cfg(not(unix))]
fn take_controller_lock(_dir: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pressure(cpu10: f64, cpu60: f64, avail: u64, builds: usize) -> Pressure {
        Pressure {
            cpu_avg10: cpu10,
            cpu_avg60: cpu60,
            io_avg10: 0.0,
            io_avg60: 0.0,
            mem_avg10: 0.0,
            mem_avg60: 0.0,
            mem_available_bytes: avail,
            builds_in_flight: builds,
        }
    }

    const GB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn a_red_light_halves_the_bound_and_a_green_one_raises_it_by_one() {
        let busy = pressure(30.0, 40.0, 8 * GB, 0);
        assert_eq!(decide(4, 1, 8, Some(&busy), false), (2, Action::Decreased));
        let calm = pressure(0.1, 0.2, 50 * GB, 0);
        assert_eq!(decide(4, 1, 8, Some(&calm), false), (5, Action::Increased));
    }

    #[test]
    fn a_build_holding_a_slot_is_pressure_even_when_the_kernel_is_calm() {
        // 构建的负载尖峰还没长到 PSI 上，但槽位已经被持有：这一刻加并行就是加在最坏的
        // 时刻上，所以它自己就是一条红灯。
        let with_build = pressure(0.0, 0.0, 50 * GB, 1);
        assert_eq!(
            decide(4, 1, 8, Some(&with_build), false),
            (2, Action::Decreased)
        );
    }

    #[test]
    fn the_bound_never_leaves_its_range() {
        let busy = pressure(90.0, 90.0, GB, 3);
        assert_eq!(decide(1, 1, 8, Some(&busy), true), (1, Action::AtFloor));
        let calm = pressure(0.0, 0.0, 50 * GB, 0);
        assert_eq!(decide(8, 1, 8, Some(&calm), false), (8, Action::AtCeiling));
    }

    #[test]
    fn no_pressure_reading_does_not_move_the_bound() {
        // 读不出压力就不动：把「读不出」当成「没压力」是这张表唯一一个越看不见越放大的
        // 方向。
        assert_eq!(decide(4, 1, 8, None, false), (4, Action::None));
    }

    #[test]
    fn a_case_that_is_twice_its_own_median_is_slow() {
        let mut history = vec![100, 100, 100, 100, 100];
        assert!(!latency_is_red(&history), "中位数上的一例不慢");
        history.push(250);
        assert!(latency_is_red(&history));
        // 看得太少就不判：中位数还不是这个负载的历史。
        assert!(!latency_is_red(&[100, 100, 900]));
    }

    #[tokio::test]
    async fn the_column_ceiling_is_taken_together_with_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = AdmissionConfig {
            dir: dir.path().to_path_buf(),
            floor_unmeasured: 4,
            ceiling_unmeasured: 4,
            window: Duration::from_secs(3600),
        };
        let gate = open(&cfg).unwrap();
        // 界是 4，但这列的隔离上界是 1：第二个要同一列的必须等。
        let first = gate.acquire("toolathlon", Some(1)).await;
        let waiting = tokio::time::timeout(
            Duration::from_millis(50),
            gate.acquire("toolathlon", Some(1)),
        )
        .await;
        assert!(waiting.is_err(), "同一列的上界没被算进有效并发");
        // 别的一列不受这一列的隔离上界影响（这一列只跟它自己共享环境）。
        let other =
            tokio::time::timeout(Duration::from_millis(50), gate.acquire("hle", None)).await;
        assert!(other.is_ok(), "别的一列被这一列的上界挡住了");
        drop(first);
        let again = tokio::time::timeout(
            Duration::from_millis(500),
            gate.acquire("toolathlon", Some(1)),
        )
        .await;
        assert!(again.is_ok(), "上一例落下来之后这一列该能再放行");
    }

    #[tokio::test]
    async fn the_bound_is_published_where_a_second_process_reads_it() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = AdmissionConfig {
            dir: dir.path().to_path_buf(),
            floor_unmeasured: 2,
            ceiling_unmeasured: 4,
            window: Duration::from_secs(3600),
        };
        let gate = open(&cfg).unwrap();
        assert!(gate.reading().controller);
        assert_eq!(
            read_bound(dir.path()),
            Some(2),
            "控制器一起来就要把自己的界写出去，别的进程不该各自退回 floor 去猜"
        );
        let _permit = gate.acquire("hle", None).await;
        assert_eq!(
            read_bound(dir.path()),
            Some(gate.reading().bound),
            "目录里的界与这一刻放行的界必须是同一个数"
        );
    }

    #[test]
    fn a_gate_directory_that_is_not_there_is_an_error_not_a_new_one() {
        // 替它建一个空目录，就等于把「指错了」变成「闸门生效了」：它照样放行、照样报数，
        // 只是谁也不协调。
        let missing = std::env::temp_dir().join("cogneva-admission-nothing-here-2f9c1a");
        let _ = std::fs::remove_dir_all(&missing);
        let err = open(&AdmissionConfig::new(&missing)).unwrap_err();
        assert!(err.contains("is not a directory"), "{err}");
        assert!(!missing.exists(), "不存在的目录不该被就地造出来");
    }

    #[test]
    fn each_red_light_states_which_one_it_is() {
        let busy = Pressure {
            io_avg10: 31.0,
            ..pressure(0.0, 0.0, 8 * GB, 0)
        };
        let why = red_light(&busy, false).unwrap();
        assert!(why.contains("io") && why.contains("31.00"), "{why}");
        let low_mem = pressure(0.0, 0.0, GB, 0);
        assert!(red_light(&low_mem, false).unwrap().contains("MemAvailable"));
        let building = pressure(0.0, 0.0, 50 * GB, 2);
        assert!(red_light(&building, false).unwrap().contains("build slot"));
        assert!(red_light(&pressure(0.0, 0.0, 50 * GB, 0), true)
            .unwrap()
            .contains("median"));
        assert!(red_light(&pressure(0.0, 0.0, 50 * GB, 0), false).is_none());
    }

    /// 内存的那两条线是**两个实测值**，不是一个数的两倍。
    ///
    /// 7d 实测：宿主 MemTotal 62.7 GiB、MemAvailable p5 46.4 GiB、谷底 36.46 GiB。
    /// 若把绿灯写回 `floor * 2`，地板抬到有牙齿的 44 GiB 就会要求 `mem > 88 GiB`，
    /// 超过 MemTotal ⇒ 绿灯永不成立，而改动看起来只是「把地板换成实测值」。
    #[test]
    fn the_green_memory_line_fits_under_the_host_that_was_measured() {
        let host_total_bytes = 62 * GB; // 62.7 GiB 向下取整，实测值
        assert!(
            MEM_GREEN_MIN_BYTES < host_total_bytes,
            "绿灯线高过宿主内存就永远不会亮"
        );
    }

    /// 落在回滞带里：既不红也不绿，保持不动。
    #[test]
    fn memory_between_the_floor_and_the_green_line_holds() {
        let band = pressure(0.0, 0.0, 45 * GB, 0);
        assert!(red_light(&band, false).is_none(), "45 GiB 高过 44 GiB 地板");
        assert_eq!(decide(4, 1, 8, Some(&band), false), (4, Action::Held));
    }

    #[test]
    fn a_second_gate_on_one_directory_is_a_reader_not_a_second_controller() {
        // 两个控制器会互相把界推来推去，读数上还分不出是谁写的。选主是同一个文件的
        // flock：同一个进程里两次 open 也是两个各自独立的上锁对象，所以这条在进程内
        // 也能试。
        let dir = tempfile::tempdir().unwrap();
        let first = open(&AdmissionConfig {
            dir: dir.path().to_path_buf(),
            floor_unmeasured: 3,
            ceiling_unmeasured: 4,
            window: Duration::from_secs(60),
        })
        .unwrap();
        let second = open(&AdmissionConfig {
            dir: dir.path().to_path_buf(),
            // 第二个进程自己的下界更低：它算自己的就会得到 1。它得读目录里那个 3。
            floor_unmeasured: 1,
            ceiling_unmeasured: 8,
            window: Duration::from_secs(60),
        })
        .unwrap();
        assert!(first.reading().controller, "先来的当控制器");
        assert!(!second.reading().controller, "后来的只读");
        assert_eq!(first.reading().bound, 3);
        assert_eq!(
            second.reading().bound,
            3,
            "只读的那一个要读目录里的界，不是自己算一个"
        );
    }

    #[tokio::test]
    async fn the_reading_says_why_the_bound_is_where_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = AdmissionConfig {
            dir: dir.path().to_path_buf(),
            floor_unmeasured: 1,
            ceiling_unmeasured: 3,
            window: Duration::from_secs(0),
        };
        let gate = open(&cfg).unwrap();
        let permit = gate.acquire("hle", None).await;
        let reading = gate.reading();
        assert!(reading.controller, "第一个开的闸门就是控制器");
        assert!(reading.blocked == 0);
        assert!(reading.in_flight == 1);
        assert!(!reading.dir.is_empty() && !reading.identity.is_empty());
        drop(permit);
        assert_eq!(gate.reading().in_flight, 0);
        assert!(
            !gate.reading().action.is_empty(),
            "界为什么动必须是个能读的判词"
        );
    }
}
