//! Whether a running attempt is *moving*, as a fact its supervisor can read.
//!
//! Before this module the only question a supervisor could ask about a running
//! task was how long it had been running, and the answer it acted on was a
//! declared budget: the task was killed the moment elapsed time passed it. That
//! predicate measures the clock, not the work — a run that was streaming tokens
//! the whole way and one that wedged in its first second are the same reading to
//! it. The cost was measurable: on 2026-11-08 every failed task in three days
//! carried the same sentence, `degenerate_loop: task ran its whole Ns budget
//! without producing a result`, and the budget was the p99 of *completed*
//! durations — so the runs it killed never entered the sample it was computed
//! from, and the loss was invisible from both ends.
//!
//! The replacement asks the question that separates those two runs: when did
//! this attempt last do something? A step that produces output records a beat;
//! silence past a window derived from measured gaps is what stops an attempt.
//! Elapsed time is not part of the decision, so a slow run that keeps working is
//! never cut, and a run that stops moving is cut without waiting for a budget to
//! expire.
//!
//! Process-local on purpose. A beat is evidence that *this* process is driving
//! the attempt; a supervisor in another process reads the lease, which is the
//! fact that survives a process going away. Persisting beats would turn a
//! liveness check into write traffic proportional to tokens.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The quantile of measured inter-beat gaps that the *reading* is taken at.
///
/// This is an alerting line, not a judgement line: an attempt past it has been
/// quiet longer than 999 steps in 1000, which is worth a log line and a
/// dashboard, and is also what a few live attempts a day look like. Nothing is
/// stopped here — [`crate::declared_wall::stall`] acts at the extreme instead, because a false stop
/// costs the whole attempt and a late stop costs only the time it was late.
///
/// It is also what the sample floor is derived from (see [`SAMPLE_FLOOR`]), so
/// moving this one number moves both, and neither can drift from the other.
// cap: bound — 误判预算是拍定的：窗口取在这里，随手停摆回收约千分之一次仍在干活的尝试，这就是选它的全部理由；没有一条测量能把它调到别处
pub const SILENCE_QUANTILE: f64 = 0.999;

/// How many gap samples a window needs before it is a reading at all.
///
/// One sample per quantile point: an estimate of the q-quantile carries no
/// information about the tail until there is at least one observation beyond it.
/// Derived from [`SILENCE_QUANTILE`] rather than picked, so the two cannot
/// disagree — a floor that stayed at 100 while the quantile moved to 0.9999
/// would report a window from a sample that cannot support it.
// cap: bound — 1 / (1 - SILENCE_QUANTILE)：估 0.999 分位要至少有一个落在它之外的点
pub const SAMPLE_FLOOR: usize = 1000;

fn beacons() -> &'static Mutex<HashMap<String, Instant>> {
    static B: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    B.get_or_init(|| Mutex::new(HashMap::new()))
}

fn gaps() -> &'static Mutex<GapSamples> {
    static G: OnceLock<Mutex<GapSamples>> = OnceLock::new();
    G.get_or_init(|| Mutex::new(GapSamples::default()))
}

/// A bounded reservoir of measured inter-beat gaps.
///
/// Bounded because the window it feeds is a quantile of recent behaviour, not of
/// all behaviour ever: an unbounded vector would answer with the slowest month
/// this deployment ever had, and a window that only widens is the same defect as
/// a constant — it stops being derived from what is happening.
#[derive(Default)]
struct GapSamples {
    seen: u64,
    ring: Vec<Duration>,
    next: usize,
    /// The window as of the last sample. Cached because the readers are hot and
    /// the writer is not: every watchdog poll, every reclaim sweep and every
    /// scrape asks for it, while a sample arrives once per step. Re-sorting the
    /// reservoir on each read would put a 10 000-element sort on the read path
    /// for a number that only changes when a step completes.
    window: Option<Duration>,
    /// The longest gap currently held — the line a *judgement* is drawn at.
    ///
    /// Cached for the same reason, and kept apart from `window` on purpose: two
    /// errors here are not the same size. Letting a stopped attempt run another
    /// minute costs a minute; stopping one that was working costs the whole
    /// attempt and its spend. So the reading that drives a kill is the extreme
    /// of the population, not its p99.9: silence longer than any gap a moving
    /// attempt has ever shown on this deployment. See [`crate::declared_wall::stall`].
    longest: Option<Duration>,
    /// 这一跑**自己**量到的最长一次间隔，只增不减，不受水库淘汰影响。
    ///
    /// 水库按条数淘汰，而高频率的进度拍（每搬一块数据一拍）能在几秒内把水库灌满，
    /// 把早先那些真正长的间隔挤出去——于是「这一跑量到的极值」会缩成几毫秒，写回
    /// 持久面的界跟着缩，下一趟开工就用一个不存在的窄界去判停。极值不能住在会被
    /// 高频样本冲掉的容器里，所以它单独记一份；它跟着这一跑的生死走，每跑重新量，
    /// 不会像常量那样永远停在一个数上。
    run_longest: Option<Duration>,
    /// 上一次运行留下的实测极值（[`adopt_longest_gap`]）。
    ///
    /// 一个刚起的进程没有任何间隔样本，若只看自己的水库，那么每一趟都只能回答
    /// 「量不到」——判据于是要么永远不成立，要么退回一个挑出来的数。带上它，这一跑
    /// 从开工起就有一个**别人量过的**界，而且它仍然是一个读数，不是常量。
    adopted: Option<Duration>,
}

impl GapSamples {
    /// Ten windows' worth is enough for a p99.9 without holding a day of samples:
    /// the reservoir is sized from the floor, which is itself derived from the
    /// quantile, so there is no second number to keep in step.
    fn capacity() -> usize {
        SAMPLE_FLOOR * 10
    }

    fn push(&mut self, gap: Duration) {
        self.seen = self.seen.saturating_add(1);
        self.run_longest = Some(match self.run_longest {
            Some(previous) => previous.max(gap),
            None => gap,
        });
        if self.ring.len() < Self::capacity() {
            self.ring.push(gap);
        } else {
            self.ring[self.next] = gap;
        }
        self.next = (self.next + 1) % Self::capacity();
        self.window = self.compute_quantile(SILENCE_QUANTILE);
        self.longest = self.compute_longest();
    }

    /// The extreme of the reservoir, or `None` under the same floor as the
    /// quantile: a maximum taken from three samples is not evidence about a
    /// tail, and acting on it would kill attempts on a number nobody measured.
    fn compute_longest(&self) -> Option<Duration> {
        (self.ring.len() >= SAMPLE_FLOOR).then(|| {
            self.ring
                .iter()
                .copied()
                .max()
                .expect("a non-empty reservoir has a maximum")
        })
    }

    /// The q-quantile of what is held, or `None` when the sample cannot support
    /// it. Reporting a window from too few samples would put a number on a
    /// population nobody has looked at yet — the same mistake the wall made,
    /// wearing the clothes of a measurement.
    fn compute_quantile(&self, q: f64) -> Option<Duration> {
        if self.ring.len() < SAMPLE_FLOOR {
            return None;
        }
        let mut sorted = self.ring.clone();
        sorted.sort_unstable();
        // Nearest-rank: the first observation at or beyond the quantile. No
        // interpolation, because the quantity being read is "how long did a
        // real gap last", and an average of two neighbours is a duration no run
        // ever had.
        let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
        Some(sorted[rank - 1])
    }
}

/// Record that this attempt just did something.
///
/// Called from the places where work actually advances — a streamed event, a
/// completed tool call, an iteration boundary — not from a timer, because a
/// timer would report the process is alive (which the lease already says) rather
/// than that the attempt is progressing.
pub fn beat(task_id: &str) {
    if task_id.is_empty() {
        return;
    }
    let mut map = beacons().lock().unwrap_or_else(|p| p.into_inner());
    let now = Instant::now();
    if let Some(previous) = map.insert(task_id.to_string(), now) {
        let gap = now.saturating_duration_since(previous);
        drop(map);
        gaps().lock().unwrap_or_else(|p| p.into_inner()).push(gap);
    }
    // 同一拍也是这一跑对容器外说的话：这一步刚刚过去。容器里的读者（看门狗）读内存，
    // 容器外的读者（kubelet 探针）只能读这个文件。
    publish_progress(false);
}

/// When this attempt last did something, or `None` when nothing ever recorded it.
///
/// `None` is a different fact from "a long time ago" and the caller must not
/// fold the two: an attempt whose executor emits no beats is unmeasurable, and
/// treating unmeasurable as silence would cut every task of that kind on its
/// first check.
pub fn last_beat(task_id: &str) -> Option<Instant> {
    beacons()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(task_id)
        .copied()
}

/// Drop an attempt's beacon. Called when the attempt ends, so a long-lived
/// process does not carry a map entry per task it has ever run.
pub fn forget(task_id: &str) {
    beacons()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(task_id);
}

/// The silence window, measured: the [`SILENCE_QUANTILE`] quantile of observed
/// inter-beat gaps.
///
/// `None` until [`SAMPLE_FLOOR`] gaps have been seen. A caller that gets `None`
/// has no window and must say so rather than substituting one; the reading is
/// "this deployment has not run enough steps to know", which is not the same as
/// "nothing is stuck" or "everything is stuck".
pub fn silence_window() -> Option<Duration> {
    gaps().lock().unwrap_or_else(|p| p.into_inner()).window
}

/// The longest inter-beat gap this deployment has recently seen, or `None`
/// under the same sample floor as [`silence_window`].
///
/// This is the line a *judgement* is drawn at. Every sample in it came from two
/// beats of an attempt that was still working, so it answers "how quiet does
/// this deployment's work get while it is still moving" — and silence past it
/// is a shape that has not once been observed on an attempt that was working.
pub fn longest_gap() -> Option<Duration> {
    let gaps = gaps().lock().unwrap_or_else(|p| p.into_inner());
    // 水库的极值还要跟这一跑自己的极值取大：水库按条数淘汰，密集的拍能在几小时里把
    // 早先那些真正长的间隔全挤出去，于是「这套部署干活时最多静默过多久」缩成一个窄数，
    // 而判停线一窄就是误杀。`run_longest` 不过期，但它也不在样本到量之前单独成立——
    // 三个样本的最大值不是关于尾巴的证据，这一条与 `compute_longest` 同一个门槛。
    let measured = match (gaps.longest, gaps.run_longest) {
        (Some(recent), Some(run)) => Some(recent.max(run)),
        (recent, _) => recent,
    };
    match (measured, gaps.adopted) {
        (Some(measured), Some(adopted)) => Some(measured.max(adopted)),
        (measured, adopted) => measured.or(adopted),
    }
}

/// 采纳上一次运行量到的极值，让这一跑从开工起就有一个别人量过的界。
///
/// 传进来的必须是一个**量出来的**数（`observed_longest_gap` 的返回），不是谁挑的。
/// 多次采纳取最大：两个来源都成立时，往宽松那一侧走是误杀成本更低的那一边。
pub fn adopt_longest_gap(gap: Duration) {
    let mut gaps = gaps().lock().unwrap_or_else(|p| p.into_inner());
    gaps.adopted = Some(match gaps.adopted {
        Some(previous) => previous.max(gap),
        None => gap,
    });
}

/// 这一次运行**自己**量到的最长间隔，不受 [`SAMPLE_FLOOR`] 限制，也不含采纳值。
///
/// 这是写回持久面的那个数：一个作业类每一趟只走几十步，凑不满量分位所需的样本，
/// 而「这个作业类成功跑里最长的一次间隔」是一个极值问题，不是分位问题——极值从两个
/// 样本起就是一条事实，只是它有多可靠要跟样本数一起说。
pub fn observed_longest_gap() -> Option<Duration> {
    // 读 `run_longest` 而不是对水库取最大：水库按条数淘汰，而一次高频率进度（每搬一块
    // 数据一拍）能把水库灌满，把早先那些真正长的间隔挤出窗口——`max()` 于是返回这一阵
    // 的毫秒级间隔，写回持久面就成了下一趟判停用的窄界。极值必须住在不会被冲掉的容器里。
    gaps().lock().unwrap_or_else(|p| p.into_inner()).run_longest
}

/// Has this attempt been quiet long enough to be *worth looking at*?
///
/// A reading, not a judgement: it fires at the p99.9 of step gaps, which on a
/// busy fleet is a few attempts a day that are still perfectly alive. Nothing
/// must stop a run on this — stopping one that was working costs the whole
/// attempt, while letting a stopped one run another minute costs a minute.
/// Alerting and log lines read it; [`crate::declared_wall::stall`] is what a
/// supervisor acts on.
pub fn quiet_longer_than_usual(task_id: &str) -> Option<(Duration, Duration)> {
    let last = last_beat(task_id)?;
    let window = silence_window()?;
    let silent = last.elapsed();
    (silent > window).then_some((silent, window))
}

/// 记一条「别人还在干活」的证据：更新这一拍，但**不产生一个间隔样本**。
///
/// 与 [`beat`] 的区别是这条证据不是我们这一步完成了一个单位，而是对面报告它还在
/// 执行。它同样要被容器外的探针看见，所以也发布进度文件；但它不该进入间隔水库——
/// 对端每秒报一次活，会把水库填满一秒的间隔，把实测极值压低，最后又把误杀拉回来。
pub fn mark(task_id: &str) {
    if task_id.is_empty() {
        return;
    }
    beacons()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(task_id.to_string(), Instant::now());
    publish_progress(false);
}

/// How many gap samples have been observed, for a reader that has to say whether
/// the window is a measurement yet.
pub fn gap_sample_count() -> usize {
    gaps().lock().unwrap_or_else(|p| p.into_inner()).ring.len()
}

/// 进程把「我刚推进了一步」发布到哪个文件，好让**容器之外**的人看得见。
///
/// 有些判停的读者不在本进程里：kubelet 的 exec 探针只读得到一个文件，读不到这里的
/// 内存。所以每推进一步，进程把那一刻写进这个路径；文件内容是「静默多少秒算停」，
/// 由写的一方决定——窗口是这个进程量出来的（[`longest_gap`]），探针只负责拿
/// `now - mtime` 跟它比。窗口写进文件而不是写进探针，是因为探针里的数一旦写死，
/// 下次窗口变了它不会跟着变，而它读起来还是「这条窗口」。
pub const PROGRESS_FILE_ENV: &str = "COGNEVA_PROGRESS_FILE";

/// 进度文件里「还没有实测窗口」时写的那个词。
///
/// 「量不到」与「量到就是这个值」必须是两个读数（见 [`longest_gap`]），而文件里只能写
/// 一个值。写一个数进去会把「我们还没量过」伪装成「界就是这个数」——一个没人量过、
/// 却会被容器外的人当判据用的数，正是这个仓里明令禁止的硬编码。所以没有实测时就写
/// 这个词，探针读到它只报读数、不判停：等它自己结束，比拿一个挑出来的数杀掉它便宜。
pub const PROGRESS_FILE_UNBOUNDED: &str = "unbounded";

/// 写进度文件的最小间隔：一拍输出就写一次盘，写盘量会跟着 token 走。
///
/// 它不改判据，只改这条读数自己的开销；一秒一次足够让探针读到「它还活着」，而
/// `mtime` 的精度也就是秒。
// cap: bound — 写盘频率是钉住的节奏，不是界；界是文件里那个窗口
const PROGRESS_FILE_WRITE_TICK: Duration = Duration::from_secs(1);

/// kubelet 的 exec 探针在容器里跑的那一条命令。
///
/// 读的正是 [`PROGRESS_FILE_ENV`] 指向的文件：没有文件＝这一跑还没发布过任何一步，
/// 静默超过文件里写的窗口＝它不再动。窗口与被测的静默都来自容器自己，所以探针里
/// 不含任何可调的界——它只把两个数读出来再比。
///
/// 文件里写的是 [`PROGRESS_FILE_UNBOUNDED`] 时（这个作业类还没有一条实测），探针只报
/// 读数、不判停。拿一个挑出来的数去杀一件没人量过的事，正是这个仓要拆掉的那种钟。
pub const PROGRESS_PROBE_COMMAND: &str = r#"f="$COGNEVA_PROGRESS_FILE"; if [ -z "$f" ] || [ ! -f "$f" ]; then echo "no progress file (COGNEVA_PROGRESS_FILE='$f')"; exit 1; fi; age=$(( $(date +%s) - $(date -r "$f" +%s) )); win=$(cat "$f"); echo "quiet ${age}s, window ${win}"; if [ "$win" = "unbounded" ]; then exit 0; fi; [ "$age" -le "$win" ]"#;

/// 探针多久读一次。
///
/// 这只是判词最晚多久出现，不是判据：判据是文件里那个窗口，探针的节奏只决定
/// 「停住」最多晚几分钟被说出来。
// cap: bound — 读取周期钉在这里，不是判据；判据是文件里的实测窗口
pub const PROBE_PERIOD_SECS: u64 = 30;

/// 连续几次读数超窗才判死。窗口本身已经是「这套部署从没在干活时静默过这么久」，
/// 再要几次连续读数，是为了不让一次读盘抖动变成一次重启。
// cap: bound — 连续读数次数钉在这里，不是界；界是实测窗口，它只防一次抖动
pub const PROBE_FAILURE_THRESHOLD: u32 = 3;

/// 容器起来之后先给多久再开始探。进程开工第一件事就是建这个文件
/// （[`start_progress_file`]），这段时间只是留给拉镜像与解压的。
// cap: bound — 开工宽限钉在这里，不是判据；它只留拉镜像与解压的时间
pub const PROBE_INITIAL_DELAY_SECS: u64 = 120;

fn last_progress_write() -> &'static Mutex<Option<Instant>> {
    static L: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(None))
}

fn publish_progress(force: bool) {
    let Ok(path) = std::env::var(PROGRESS_FILE_ENV) else {
        return;
    };
    let path = path.trim();
    if path.is_empty() {
        return;
    }
    {
        let mut last = last_progress_write()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !force && last.is_some_and(|t| t.elapsed() < PROGRESS_FILE_WRITE_TICK) {
            return;
        }
        *last = Some(Instant::now());
    }
    let window = crate::declared_wall::Bound::now().words();
    // 整份替换交给 `atomic_write`：`std::fs::write` 会先截断再写，探针正好落在
    // 中间那一瞬就会读到一个空文件，而空文件在探针里不是「没有界」，是一次解析
    // 失败——那是判死。改名在同一文件系统上是原子的，读者要么看到上一格，要么看
    // 到这一格。耐用性只要同内核：这个文件随时可以重写，断电之后重写一次就是。
    let body = format!("{window}\n");
    let written = crate::atomic_write::write_bytes(
        std::path::Path::new(path),
        body.as_bytes(),
        crate::atomic_write::Durability::SameKernel,
    );
    if let Err(e) = written {
        // 写不成就没有进度面，而「文件不动」与「根本没有文件」在集群面上同形——
        // 后者会让这一跑在自己的窗口上被杀掉。所以这一条不是噪音。临时文件由
        // `atomic_write` 自己收拾，这里只管报。
        tracing::warn!(path, error = %e, "progress file could not be written");
    }
}

/// 进程开工：先把进度文件建出来。
///
/// 不建的话，探针在第一步完成之前读到的都是「没有文件」，而它对这一读法的正确答案
/// 只能是「还没开始」——一直读不到就是一直被当成还没开始。开工那一拍是这一跑唯一
/// 一次不需要证据就能说「我在」的时刻。
pub fn start_progress_file() {
    publish_progress(true);
}

/// 容器 spec 里的 `livenessProbe` 那一格。
///
/// 一个生产者：执行 Job 的清单与 chart 里的两个 CronJob 读的是同一条命令、同一组
/// 读数节奏，谁也没有自己的一份。
pub fn liveness_probe() -> serde_json::Value {
    serde_json::json!({
        "exec": { "command": ["sh", "-c", PROGRESS_PROBE_COMMAND] },
        "periodSeconds": PROBE_PERIOD_SECS,
        "failureThreshold": PROBE_FAILURE_THRESHOLD,
        "initialDelaySeconds": PROBE_INITIAL_DELAY_SECS,
    })
}

/// 看门狗的读取节奏：多久去看一眼「它还在不在动」。
///
/// 这是**读取周期，不是判据**：判据是 [`crate::declared_wall::stall`] 那条实测窗口，
/// 这一拍只决定判词
/// 最晚多久出现，不会让它变宽或变窄。所以它不参与任何「多少秒算停」的决定。
// cap: bound — 读取周期钉在这里，不是界；界是实测的步间隔窗口
pub const WATCH_TICK: Duration = Duration::from_secs(1);

/// 判据与生产者共同的那一段：子进程被杀的原因是**它不再动**，不是它跑够了钟点。
///
/// 生产者在看门狗判停时用它写判词，判据（部署器把这类错误读成「本轮没有观测」
/// 而不是「这一版坏了」）按同一个常量认它。两边各写一句，改一处措辞的那天，判据里
/// 那一类错误就悄悄消失，消失的方向正好是把一次抖动读成缺陷。
pub const WENT_SILENT: &str = "went silent for";

/// `cmdline` 在 `silent` 里没有新产出、而这段静默已经超过实测窗口 `window` 时，
/// 写给它下游的那句话。
pub fn went_silent(cmdline: &str, silent: Duration, window: Duration) -> String {
    format!(
        "{cmdline} {WENT_SILENT} {silent:?}, longer than the {window:?} this deployment has measured"
    )
}

/// 一个被看门狗看着的子进程的结局。
#[derive(Debug)]
pub enum ChildOutcome {
    /// 进程自己退出了。`output` 是它的 stdout/stderr 与退出码。
    Exited(std::process::Output),
    /// 它在 `silent` 那么久里一个字节都没产出，而这套部署量出来的静默窗口是 `window`。
    Stalled { silent: Duration, window: Duration },
}

/// 跑一个子进程，按**它还在不在产出**判它停没停——不按一个声明的钟。
///
/// 进程每写出一段 stdout/stderr 就是一拍，所以「慢但一直在印」的不会被判，「一声
/// 不响」的会。判据与编排侧的回收、执行路由的守护读的是同一条（[`crate::declared_wall::stall`]），因此
/// 三处的判词不会漂移。**量不到窗口时它不判**（这一套部署还没跑够步数去量），
/// 那一刻没有证据，进程继续跑——「量不到」与「停住」在这里同样是两件事。
///
/// 这和 `tokio::time::timeout(cmd.wait_with_output())` 的差别不是宽严，是判据：
/// 后者看见的只有一个数——跑了多久——于是一个流式输出了一小时的进程与一个第一秒
/// 就卡死的进程在它眼里是同一个读数。
pub async fn run_child_watched(
    cmd: tokio::process::Command,
    id: &str,
) -> std::io::Result<ChildOutcome> {
    run_child_watched_with_pid(cmd, id, |_pid| {}).await
}

/// Same, handing the caller the child's pid as soon as it exists.
///
/// A caller that samples the process (memory, threads) needs the pid before the
/// process ends, and there is no way to learn it from a call that only returns
/// once the child is gone.
pub async fn run_child_watched_with_pid<F>(
    cmd: tokio::process::Command,
    id: &str,
    on_spawn: F,
) -> std::io::Result<ChildOutcome>
where
    F: FnOnce(u32),
{
    run_child_watched_observed(cmd, id, on_spawn, None).await
}

/// Same, and every beat this child produces is also handed to `also`.
///
/// A caller that is itself watched (the mainline deployer's loop) has a heartbeat
/// of its own, and a step of several minutes' work sits between two of its own
/// beats. Without this, that loop's declared work bound has to cover the whole
/// step — which is exactly the wall this module removes. Handing it each beat
/// instead makes the two readings describe the same movement: the step is
/// moving, so the loop is moving.
pub async fn run_child_watched_with_pid_and_observer<F, G>(
    cmd: tokio::process::Command,
    id: &str,
    on_spawn: F,
    also: G,
) -> std::io::Result<ChildOutcome>
where
    F: FnOnce(u32),
    G: Fn() + Send + Sync + 'static,
{
    run_child_watched_observed(cmd, id, on_spawn, Some(std::sync::Arc::new(also))).await
}

async fn run_child_watched_observed<F>(
    mut cmd: tokio::process::Command,
    id: &str,
    on_spawn: F,
    also: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
) -> std::io::Result<ChildOutcome>
where
    F: FnOnce(u32),
{
    use std::process::Stdio;
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    // 自己的进程组：`sh -c "cargo build"`、`docker exec …` 都不是一个进程。只杀
    // 领头的那一个，它启动的编译器还活着、还占着共享构建目录的锁——集群上实测过
    // 这一幕：外壳被砍了，cargo 继续攥着锁，后面每一轮都在自己的墙前编译了零个文件。
    cmd.process_group(0);
    let mut child = cmd.spawn()?;
    if let Some(pid) = child.id() {
        on_spawn(pid);
    }
    beat(id);
    watch_child(&mut child, id, also).await
}

/// 看着一个**已经起来**的子进程，判据与 [`run_child_watched`] 是同一条。
///
/// 存在的理由只有一个：有的调用方在它起来之后还要把东西写进 stdin（`kubectl
/// apply -f -` 的清单体），而 [`run_child_watched`] 从 `Command` 一路管到结束，
/// 中间没有交出句柄的缝。调用方自己起进程、把要读的那几条管子接成 `piped()`，
/// 再把句柄交过来——判停的线仍然只有这一条，不会因为多了一个入口就多出一条。
pub async fn watch_child(
    child: &mut tokio::process::Child,
    id: &str,
    also: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
) -> std::io::Result<ChildOutcome> {
    // 两条管子各一个读端：只读一条时，另一条写满管道缓冲就会把子进程堵在那里，
    // 而那副样子在读数上与「它停了」一模一样——一个自己造出来的停住。
    //
    // 管子可能根本没接（stdout 落进文件的那类调用方）：那一侧就只靠 CPU 时间与
    // 退出码，读端不起、也不许 `expect` 崩在这里。
    let mut readers = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        let id = id.to_string();
        let also = also.clone();
        let handle =
            tokio::spawn(async move { read_beating(&mut pipe, &id, also.as_deref()).await });
        readers.push((true, handle));
    }
    if let Some(mut pipe) = child.stderr.take() {
        let id = id.to_string();
        let also = also.clone();
        let handle =
            tokio::spawn(async move { read_beating(&mut pipe, &id, also.as_deref()).await });
        readers.push((false, handle));
    }

    let mut stalled = None;
    // 「什么算还在动」这条口径住在 `declared_wall`，与判据同处：读的是子进程的 CPU 与
    // 读写字节，动一次记一拍。写在这里而不是 `run_child_watched` 的调用方里，是因为
    // 只有这里握着那个 pid。
    let mut movement = crate::declared_wall::Movement::new();
    let status = loop {
        tokio::select! {
            status = child.wait() => break status?,
            _ = tokio::time::sleep(WATCH_TICK) => {
                // 干活不只是印东西：编译、链接、算矩阵这一段里它一个字节都不输出，
                // 但计数一直在涨。没有这一读，一份沉默的重活会在它自己最忙的时候被判成停住。
                if let Some(pid) = child.id() {
                    if movement.sample(pid, id) {
                        if let Some(also) = &also {
                            also();
                        }
                    }
                }
                if let Some((silent, window)) = crate::declared_wall::stall(id) {
                    stalled = Some((silent, window));
                    // 判停之后才动手：在这之前它的每一下产出与每一段 CPU 都被读到了。
                    if let Some(pid) = child.id() {
                        // SAFETY: one pid and one signal constant; no pointers.
                        //
                        // 只有它自己是组长时才按组收。调用方没给它自己的进程组时，
                        // `-pid` 落在一个更大的组上——很可能就是我们自己的组，整组
                        // SIGKILL 会把常驻进程连同兄弟一起杀掉。收一个停住的子进程
                        // 不该有这种失手，所以判据在这里，而不是靠每个调用方记得设组。
                        let own_group = unsafe { libc::getpgid(pid as i32) } == pid as i32;
                        unsafe {
                            let target = if own_group { -(pid as i32) } else { pid as i32 };
                            libc::kill(target, libc::SIGKILL);
                        }
                    }
                    let _ = child.start_kill();
                    break child.wait().await?;
                }
            }
        }
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    for (is_stdout, reader) in readers {
        let chunk = reader.await.unwrap_or_default();
        if is_stdout {
            stdout = chunk;
        } else {
            stderr = chunk;
        }
    }
    forget(id);

    if let Some((silent, window)) = stalled {
        return Ok(ChildOutcome::Stalled { silent, window });
    }
    Ok(ChildOutcome::Exited(std::process::Output {
        status,
        stdout,
        stderr,
    }))
}

/// 这个进程到现在为止烧掉了多少 CPU 时间（用户 + 内核，单位是内核的时钟滴答）。
///
/// 读 `/proc/<pid>/stat` 的第 14、15 个字段。读不到（进程已退出、这个平台没有 procfs）
/// 就是 `None`——那是「量不到」，不是「它没在干活」，调用方据此**不打拍**。
pub fn cpu_ticks(pid: u32) -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // 第 2 个字段是带括号的进程名，里面可能含空格，所以从最后一个 `)` 之后开始切。
    let rest = stat.rsplit(')').next()?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // `rest` 从第 3 个字段（state）起，于是 utime 是它里面的第 11 个、stime 是第 12 个。
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime, stime))
}

/// 一个进程「还在动」的**直接**证据：它烧掉的 CPU，和它读写的字节数。
///
/// 拍是间接证据——它只证明这个尝试主动报告过一次推进。可是编译、链接、算一个矩阵、
/// 走一次大查询，这几种工作在很长时间里一个字节都不输出、也不走任何一步，而它们一直
/// 在动；只按「没有拍」判停，就是把最忙的那一段读成停住。这三项是操作系统替它记的账，
/// 不依赖它自己开口，也不受它印不印东西影响。
///
/// 字段是 `Option`：这个平台没有 `/proc/<pid>/io`（或权限不够）时，I/O 那两项是
/// **量不到**，不是零。把量不到当零，下面 [`Activity::moved_since`] 会把「读不到」读成
/// 「没动」，于是又回到「静默判死」那条老路上。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Activity {
    cpu_ticks: u64,
    read_bytes: Option<u64>,
    written_bytes: Option<u64>,
}

impl Activity {
    pub fn new(cpu_ticks: u64, read_bytes: Option<u64>, written_bytes: Option<u64>) -> Self {
        Self {
            cpu_ticks,
            read_bytes,
            written_bytes,
        }
    }

    /// 与上一读相比有没有动。只比**两次都读到了**的那几项。
    pub fn moved_since(self, older: Self) -> bool {
        let moved = |now: Option<u64>, then: Option<u64>| match (now, then) {
            (Some(a), Some(b)) => a != b,
            _ => false,
        };
        self.cpu_ticks != older.cpu_ticks
            || moved(self.read_bytes, older.read_bytes)
            || moved(self.written_bytes, older.written_bytes)
    }
}

/// 读一个进程的活动计数。进程已退出、没有 procfs、读 `stat` 没权限，都是 `None`。
pub fn process_activity(pid: u32) -> Option<Activity> {
    let (utime, stime) = cpu_ticks(pid)?;
    let io = std::fs::read_to_string(format!("/proc/{pid}/io")).ok();
    let field = |name: &str| -> Option<u64> {
        io.as_deref()?
            .lines()
            .find_map(|line| line.strip_prefix(name))?
            .trim()
            .parse()
            .ok()
    };
    Some(Activity::new(
        utime.saturating_add(stime),
        field("rchar:"),
        field("wchar:"),
    ))
}

async fn read_beating<R>(
    reader: &mut R,
    id: &str,
    also: Option<&(dyn Fn() + Send + Sync)>,
) -> Vec<u8>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut collected = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                beat(id);
                if let Some(also) = also {
                    also();
                }
                collected.extend_from_slice(&buf[..n]);
            }
            Err(_) => break,
        }
    }
    collected
}

/// Test hook: plant `count` gaps of a known length, so a test can fix the
/// window instead of racing real timing. A window asserted from gaps the test
/// also produced is the only kind of window a test can compare against.
#[doc(hidden)]
pub fn seed_gaps_for_tests(gap: Duration, count: usize) {
    let mut g = gaps().lock().unwrap_or_else(|p| p.into_inner());
    for _ in 0..count {
        g.push(gap);
    }
}

/// The one lock the test hooks below hand out.
///
/// Tokio's rather than std's because some of the tests that need it are async:
/// a `std` guard held across an `await` is a deadlock waiting for the first
/// test that holds it while awaiting a task that wants it, and clippy refuses
/// the shape outright (`await_holding_lock`). One mutex with two doors, not two
/// mutexes — two would not exclude each other, and the races the lock exists to
/// prevent would come straight back.
fn serial() -> &'static tokio::sync::Mutex<()> {
    static L: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Test hook: hold the one lock that serialises tests sharing this state.
///
/// The beacons and the reservoir are one population per process, so two tests
/// that seed or clear them are two writers of the same measurement. Run
/// concurrently, one test's `reset` lands inside another's window and the
/// assertion then describes a population neither test set up — the failure
/// shows up as a flaky red that says nothing about the code under test.
///
/// Tests that only *beat* need not hold it: a handful of real gaps cannot reach
/// [`SAMPLE_FLOOR`], so they cannot move a window.
///
/// Blocks the thread. A test that is itself async takes it through
/// [`async_test_lock`] instead; calling this one from inside a runtime panics
/// by tokio's rule, which is the right failure — it names the wrong door rather
/// than hanging.
#[doc(hidden)]
pub fn test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    serial().blocking_lock()
}

/// Test hook: the same lock, for a test that is itself async.
#[doc(hidden)]
pub async fn async_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    serial().lock().await
}

/// Test hook: clear every beacon, every sample, and any adopted bound.
///
/// Exists because this state is process-global and the tests in different
/// modules would otherwise read each other's beacons — a specimen that shares
/// the population it measures is the failure mode this module's own tests must
/// not have.
///
/// The adopted bound is cleared with the samples because it is read the same
/// way: a test that adopts 90 s leaves [`longest_gap`] at 90 s for every test
/// after it in the same binary, and an assertion like "this run measured
/// nothing yet" then describes a previous test's measurement. Widening is
/// right for a real run and wrong between specimens.
#[doc(hidden)]
pub fn reset_for_tests() {
    beacons().lock().unwrap_or_else(|p| p.into_inner()).clear();
    *gaps().lock().unwrap_or_else(|p| p.into_inner()) = GapSamples::default();
}

#[cfg(test)]
mod tests {
    use super::*;
    // 判据与界住在 `declared_wall`：这里的用例仍然读它们，因为它们量的是同一批样本。
    use crate::declared_wall::{stall, stall_any, Bound};

    /// The sample floor is the number of quantile points, not a number anybody
    /// picked: `1 / (1 - q)`. A floor that stopped following the quantile would
    /// let the window be reported from a sample that cannot support it.
    #[test]
    fn the_sample_floor_follows_from_the_quantile() {
        let from_quantile = (1.0 / (1.0 - SILENCE_QUANTILE)).round() as usize;
        assert_eq!(
            SAMPLE_FLOOR, from_quantile,
            "the floor must be one sample per quantile point"
        );
    }

    #[test]
    fn a_window_is_not_reported_before_the_sample_supports_it() {
        let _serial = test_lock();
        reset_for_tests();
        for _ in 0..SAMPLE_FLOOR - 1 {
            gaps().lock().unwrap().push(Duration::from_millis(10));
        }
        assert_eq!(
            silence_window(),
            None,
            "a window from too few samples is a guess wearing a measurement's clothes"
        );
        gaps().lock().unwrap().push(Duration::from_millis(10));
        assert_eq!(silence_window(), Some(Duration::from_millis(10)));
    }

    /// The window moves with what the fleet actually does: faster steps shrink
    /// it, slower steps grow it. This is the property the wall never had — it
    /// was computed once from completed runs and then executed forever.
    #[test]
    fn the_window_follows_the_measured_gaps() {
        let _serial = test_lock();
        reset_for_tests();
        // Fill the reservoir so the ring is at capacity and the newest samples
        // replace the oldest, which is what makes it a reading of *now*.
        for _ in 0..GapSamples::capacity() {
            gaps().lock().unwrap().push(Duration::from_secs(10));
        }
        let slow = silence_window().expect("enough samples");
        for _ in 0..GapSamples::capacity() {
            gaps().lock().unwrap().push(Duration::from_millis(200));
        }
        let fast = silence_window().expect("enough samples");
        assert!(
            fast < slow,
            "the window must shrink when the fleet speeds up: slow={slow:?} fast={fast:?}"
        );
    }

    #[test]
    fn a_beat_is_readable_and_forgettable() {
        let _serial = test_lock();
        reset_for_tests();
        assert_eq!(last_beat("t1"), None, "no beat is not a very old beat");
        beat("t1");
        assert!(last_beat("t1").is_some());
        forget("t1");
        assert_eq!(last_beat("t1"), None);
    }

    /// Beats feed the window: two beats are one gap, and the gap between them is
    /// what the window is made of.
    #[test]
    fn beats_produce_gap_samples() {
        let _serial = test_lock();
        reset_for_tests();
        assert_eq!(gap_sample_count(), 0);
        beat("t1");
        assert_eq!(gap_sample_count(), 0, "the first beat has no predecessor");
        beat("t1");
        assert_eq!(gap_sample_count(), 1);
    }

    /// The two lines are not the same line, and the difference is the whole
    /// point: a reading at p99.9 is allowed to fire on an attempt that is still
    /// working, a judgement is not.
    ///
    /// The population here is the real shape — most steps are fast, and one step
    /// in a thousand is slow but still a step. Silence of 500 ms is past the
    /// p99.9 (200 ms) and nowhere near the longest gap a moving attempt has
    /// shown (2 s), so it must raise a reading and stop nothing.
    #[test]
    fn a_reading_may_fire_where_a_judgement_must_not() {
        let _serial = test_lock();
        reset_for_tests();
        for _ in 0..SAMPLE_FLOOR {
            gaps().lock().unwrap().push(Duration::from_millis(200));
        }
        gaps().lock().unwrap().push(Duration::from_secs(2));

        assert_eq!(silence_window(), Some(Duration::from_millis(200)));
        assert_eq!(
            longest_gap(),
            Some(Duration::from_secs(2)),
            "the judgement line is the extreme, not the quantile"
        );

        beat("slow-but-alive");
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            quiet_longer_than_usual("slow-but-alive").is_some(),
            "half a second is past p99.9: worth a reading"
        );
        assert_eq!(
            stall("slow-but-alive"),
            None,
            "and still short of anything a moving attempt has shown: stop nothing"
        );
    }

    /// The judgement is silence, not elapsed time: a child that keeps producing
    /// output runs many times past the measured window and is left alone.
    ///
    /// This is the property the time walls did not have, and the one that makes
    /// "stopped" mean stopped: a heavy step of ten minutes and a hung step of
    /// ten minutes read the same to a clock, and different to this.
    #[tokio::test]
    async fn a_child_that_keeps_printing_is_not_stopped_for_taking_long() {
        let _serial = async_test_lock().await;
        reset_for_tests();
        // 窗口取 2 秒而不是 500 毫秒：判据一拍一秒才看一次，而被测的是**这台机器**
        // 有没有把这一拍按时调度上。窗口窄到与拍子同量级时，一次正常的调度抖动就够
        // 越过它——那读的是我们自己的饥饿，不是子进程的沉默。窗口仍远小于下面这段
        // 产出撑起的时长，所以「有产出就不该被判停」这半句照样被证到。
        seed_gaps_for_tests(Duration::from_secs(2), SAMPLE_FLOOR);

        let mut cmd = tokio::process::Command::new("sh");
        cmd.args([
            "-c",
            "i=0; while [ $i -lt 120 ]; do echo step $i; i=$((i+1)); sleep 0.04; done",
        ]);
        // 120 steps × 40 ms runs well past the window, and no single gap between
        // two steps is anywhere near it: nothing but the beats keeps the child
        // alive, which is the point.
        match run_child_watched(cmd, "chatty").await.expect("spawn") {
            ChildOutcome::Exited(out) => assert!(out.status.success()),
            ChildOutcome::Stalled { silent, window } => panic!(
                "a child that printed a line every 40 ms was stopped after {silent:?} \
                 against a {window:?} window: elapsed time came back in as a criterion"
            ),
        }
    }

    /// 容器外的读者（kubelet 探针）只有这一份读数：进程开工写下的那个文件。
    ///
    /// 三件事一次说清——没有文件时探针不判「活着」；刚写过的一拍读作在动；同一个
    /// 陈旧文件在窗口内读作在动、出了窗口读作停了。最后两条是同一个 mtime 的两个
    /// 答案，所以窗口确实来自文件里的数，不是探针自己带的数。
    #[test]
    fn the_probe_stops_only_past_the_window_the_process_wrote() {
        let dir = std::env::temp_dir().join(format!("cog-progress-probe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let file = dir.join("progress");
        let probe = |file: &std::path::Path| {
            std::process::Command::new("sh")
                .arg("-c")
                .arg(PROGRESS_PROBE_COMMAND)
                .env(PROGRESS_FILE_ENV, file)
                .output()
                .expect("sh")
        };
        assert!(
            !probe(&file).status.success(),
            "没有文件＝这一跑还没说过它开工，不能读作在动"
        );
        std::fs::write(&file, "60\n").expect("write");
        assert!(probe(&file).status.success(), "刚写下的一拍就是在动");
        let stale = std::process::Command::new("touch")
            .args(["-d", "2 hours ago"])
            .arg(&file)
            .status()
            .expect("touch");
        assert!(stale.success(), "touch");
        assert!(
            !probe(&file).status.success(),
            "静默两小时、窗口只有 60 秒：这一读法是停了"
        );
        std::fs::write(&file, "7200\n").expect("write");
        assert!(
            probe(&file).status.success(),
            "同一个 mtime，窗口够了就是在动——窗口来自文件，不是探针自带的"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 开工那一拍必须把文件建出来，内容是窗口；之后每一拍都算「还在动」。
    #[test]
    fn starting_and_beating_publish_the_window_to_the_file() {
        let _serial = test_lock();
        reset_for_tests();
        let dir = std::env::temp_dir().join(format!("cog-progress-file-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let file = dir.join("progress");
        let previous = std::env::var(PROGRESS_FILE_ENV).ok();

        std::env::set_var(PROGRESS_FILE_ENV, &file);
        start_progress_file();
        let written = std::fs::read_to_string(&file).expect("开工要先把文件建出来");
        beat("published");
        beat("published");

        match previous {
            Some(value) => std::env::set_var(PROGRESS_FILE_ENV, value),
            None => std::env::remove_var(PROGRESS_FILE_ENV),
        }
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(
            written.trim(),
            PROGRESS_FILE_UNBOUNDED,
            "还没量到窗口时写的是「没有界」这个词，不是一个挑出来的数——写了数，容器外的人就会拿它当判据"
        );
    }

    /// A child that goes quiet is stopped at the measured window, and the verdict
    /// carries both numbers — how long it was silent, and the window it crossed.
    ///
    /// The child's own sleep is many times the window, so a verdict that arrives
    /// at all came from the watchdog rather than from the child ending on its own.
    #[tokio::test]
    async fn a_child_that_goes_quiet_is_stopped_at_the_measured_window() {
        let _serial = async_test_lock().await;
        reset_for_tests();
        seed_gaps_for_tests(Duration::from_millis(500), SAMPLE_FLOOR);

        let mut cmd = tokio::process::Command::new("sh");
        cmd.args(["-c", "sleep 30"]);
        let started = std::time::Instant::now();
        match run_child_watched(cmd, "silent").await.expect("spawn") {
            ChildOutcome::Stalled { silent, window } => {
                assert_eq!(window, Duration::from_millis(500));
                assert!(silent > window, "a stop is silence past the window");
            }
            ChildOutcome::Exited(out) => panic!(
                "a child silent for 30 s against a 500 ms window was not stopped: {:?}",
                out.status
            ),
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the verdict came from the child ending, not from the watchdog"
        );
    }

    /// 一个刚起的进程没有自己的样本，只有上一次运行留下的实测值；判据必须跟着它走。
    ///
    /// 这条量的是「换输入 ⇒ 值跟着变」：同一个进程，采纳的极值从 2 s 换成 9 s，
    /// 判停线就得跟着从 2 s 挪到 9 s。挪不动，就说明那个界又从测量退化成了常量。
    #[test]
    fn an_adopted_bound_moves_the_judgement_line() {
        let _serial = test_lock();
        reset_for_tests();
        // 水库凑不满样本，这一跑自己什么也没量到。
        seed_gaps_for_tests(Duration::from_millis(5), 3);
        assert_eq!(longest_gap(), None, "样本不足时不该有一条自己量出来的界");

        adopt_longest_gap(Duration::from_secs(2));
        assert_eq!(longest_gap(), Some(Duration::from_secs(2)));

        adopt_longest_gap(Duration::from_secs(9));
        assert_eq!(
            longest_gap(),
            Some(Duration::from_secs(9)),
            "采纳的界换了，判停线没跟着换：它就不再是读数了"
        );
        reset_for_tests();
    }

    /// 写回持久面的那个数不受样本下限约束，也不含采纳值——它问的是「这一跑自己看到了多长的间隔」。
    #[test]
    fn the_observed_extreme_is_this_runs_own_reading() {
        let _serial = test_lock();
        reset_for_tests();
        seed_gaps_for_tests(Duration::from_millis(100), 2);
        seed_gaps_for_tests(Duration::from_millis(700), 1);
        adopt_longest_gap(Duration::from_secs(60));

        assert_eq!(observed_longest_gap(), Some(Duration::from_millis(700)));
        assert_eq!(longest_gap(), Some(Duration::from_secs(60)));
        reset_for_tests();
    }

    /// 一条长间隔不能被后来那些密集的拍挤出水库。
    ///
    /// 这是这一族里最难靠读代码看出来的缺陷：水库按**条数**淘汰，一个例如每搬 4 MB 记一拍的
    /// 作业能在几秒内把它灌满。挤出去的是早先那些真正长的间隔，于是「这一跑量到的极值」
    /// 缩成几毫秒，写回持久面，下一趟开工就拿着一个根本不存在的窄界去判停——界越窄，
    /// 误杀越多，而每一步读数看起来都正常。
    #[test]
    fn a_long_gap_survives_the_reservoir_filling_up() {
        let _serial = test_lock();
        reset_for_tests();
        let long = Duration::from_secs(90);
        seed_gaps_for_tests(long, 1);
        // 把水库整整换过一遍还多一条：按条数淘汰的话，那一条长间隔此刻已经不在里面了。
        seed_gaps_for_tests(Duration::from_millis(1), GapSamples::capacity() + 1);

        assert_eq!(
            observed_longest_gap(),
            Some(long),
            "写回持久面的极值被高频率的拍冲掉了：下一趟会用一条不存在的窄界判停"
        );
        assert_eq!(
            longest_gap(),
            Some(long),
            "判停线同样不能因为水库换过一轮就缩窄"
        );
        reset_for_tests();
    }

    /// 持久面读不出来的那一格不是一道更小的界，是没有界。
    ///
    /// 三种读法各一格：没有那一行、读回来是 0、读回来不是有限数。谁把它们折成
    /// `Measured(0)`，下一趟就会在第一次静默时被杀——界从「没人量过」变成了「零秒」。
    #[test]
    fn a_stored_bound_that_cannot_be_read_is_unmeasured_not_zero() {
        assert_eq!(Bound::from_stored(None), Bound::Unmeasured);
        assert_eq!(Bound::from_stored(Some(0.0)), Bound::Unmeasured);
        assert_eq!(Bound::from_stored(Some(-5.0)), Bound::Unmeasured);
        assert_eq!(Bound::from_stored(Some(f64::NAN)), Bound::Unmeasured);
        assert_eq!(
            Bound::from_stored(Some(12.5)),
            Bound::Measured(Duration::from_secs_f64(12.5))
        );
    }

    /// 「量不到」在写给人看的那一面也只有一个词：进度文件、判词、写回持久面读的是
    /// 同一句。分叉的那天，容器外看到的界与进程判停用的界就是两个数。
    #[test]
    fn an_unmeasured_bound_says_so_in_the_same_word_everywhere() {
        let _serial = test_lock();
        reset_for_tests();
        assert_eq!(Bound::now(), Bound::Unmeasured);
        assert_eq!(Bound::now().words(), PROGRESS_FILE_UNBOUNDED);
        assert_eq!(Bound::observed(), Bound::Unmeasured);
        adopt_longest_gap(Duration::from_secs(42));
        assert_eq!(Bound::now(), Bound::Measured(Duration::from_secs(42)));
        assert_eq!(Bound::now().words(), "42");
        assert_eq!(
            Bound::observed(),
            Bound::Unmeasured,
            "采纳来的不是这一跑量到的：写回持久面的必须是自己看到的那个数"
        );
        reset_for_tests();
    }

    /// 秒数向上取整：一个不到一秒的界不能写成「0」。
    ///
    /// 这一格是容器外探针的判据。写成 0，探针的 `age <= 0` 几乎立刻不成立，于是一个
    /// 每一步都在推进的容器被自己的窗口杀掉——误杀就是从一次截断开始的。
    #[test]
    fn a_sub_second_bound_is_never_written_as_zero() {
        assert_eq!(Bound::Measured(Duration::from_millis(400)).words(), "1");
        assert_eq!(Bound::Measured(Duration::from_millis(1)).words(), "1");
        assert_eq!(Bound::Measured(Duration::from_secs(42)).words(), "42");
        assert_eq!(Bound::Measured(Duration::from_millis(42_500)).words(), "43");
        assert_eq!(Bound::Unmeasured.words(), PROGRESS_FILE_UNBOUNDED);
    }

    /// 一组通道里最年轻的一拍就是这一步的年龄：任何一条还在动，这一步就不算静默。
    #[test]
    fn the_youngest_channel_is_this_steps_age() {
        let _serial = test_lock();
        reset_for_tests();
        seed_gaps_for_tests(Duration::from_millis(200), SAMPLE_FLOOR);
        beat("step");

        assert!(stall_any(&["step"]).is_none(), "刚记过的一拍不该被判停");
        std::thread::sleep(Duration::from_millis(260));
        let (silent, bound) = stall_any(&["step"]).expect("静默超过实测界就该有判词");
        assert_eq!(bound, Duration::from_millis(200));
        assert!(silent > bound);

        // 对端报一次活，即使它不产生间隔样本，这一步也不再静默。
        mark("peer");
        assert!(
            stall_any(&["step", "peer"]).is_none(),
            "对端还在执行时这一步没有静默——把它算停就是把「等对面干活」读成卡死"
        );
        reset_for_tests();
    }

    /// 探针读到「没有界」只报读数，不判停。
    #[test]
    fn the_probe_reports_an_unbounded_window_without_killing() {
        let _serial = test_lock();
        let dir =
            std::env::temp_dir().join(format!("cog-progress-unbounded-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let file = dir.join("progress");
        std::fs::write(&file, PROGRESS_FILE_UNBOUNDED).expect("write");

        let out = std::process::Command::new("sh")
            .args(["-c", PROGRESS_PROBE_COMMAND])
            .env(PROGRESS_FILE_ENV, &file)
            .output()
            .expect("run probe");
        let _ = std::fs::remove_dir_all(&dir);

        assert!(
            out.status.success(),
            "关掉检测进程的「没有界」不该判停：{}",
            String::from_utf8_lossy(&out.stdout)
        );
        assert!(String::from_utf8_lossy(&out.stdout).contains(PROGRESS_FILE_UNBOUNDED));
    }
}
