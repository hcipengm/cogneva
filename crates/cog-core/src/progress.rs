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
/// stopped here — [`stall`] acts at the extreme instead, because a false stop
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
    /// attempt has ever shown on this deployment. See [`stall`].
    longest: Option<Duration>,
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
    gaps().lock().unwrap_or_else(|p| p.into_inner()).longest
}

/// Has this attempt been quiet long enough to be *worth looking at*?
///
/// A reading, not a judgement: it fires at the p99.9 of step gaps, which on a
/// busy fleet is a few attempts a day that are still perfectly alive. Nothing
/// must stop a run on this — stopping one that was working costs the whole
/// attempt, while letting a stopped one run another minute costs a minute.
/// Alerting and log lines read it; [`stall`] is what a supervisor acts on.
pub fn quiet_longer_than_usual(task_id: &str) -> Option<(Duration, Duration)> {
    let last = last_beat(task_id)?;
    let window = silence_window()?;
    let silent = last.elapsed();
    (silent > window).then_some((silent, window))
}

/// The stall verdict: how long this attempt has been quiet and the measured
/// window it exceeded, or `None` when there is no evidence that it stopped.
///
/// One definition, read by both things that can end a run — the orchestrator's
/// reclaim sweep and the executor router's watchdog. Two copies would be two
/// predicates, and the day they drift is the day one side stops a run while the
/// other keeps waiting for it, with no reading that says which is right.
///
/// The threshold is [`longest_gap`], not the p99.9 window, because the two
/// errors here cost different amounts: a stopped attempt left running for
/// another minute wastes a minute, and a working attempt stopped wastes the
/// whole attempt. A p99.9 line would stop roughly one live attempt in a
/// thousand on purpose; the extreme line stops an attempt only after a silence
/// this deployment has never once produced while working.
///
/// `None` covers three different situations on purpose, because all three mean
/// the same thing to a caller: nobody measured a beat for this attempt, or the
/// deployment has not run enough steps to know what a long gap is, or the
/// attempt is inside the window. In each case "do not stop it" is the only
/// answer supported by evidence.
pub fn stall(task_id: &str) -> Option<(Duration, Duration)> {
    let last = last_beat(task_id)?;
    let longest = longest_gap()?;
    let silent = last.elapsed();
    (silent > longest).then_some((silent, longest))
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

/// 进度文件里写的窗口下限：这个进程还没量够步数时用这个数。
///
/// 「量不到」与「量到就是这个值」必须是两个读数（见 [`longest_gap`]），而文件里只能
/// 写一个数，所以这里写的是**下限**：实测窗口只把它撑大，不把它压小——压小是往
/// 误杀那一侧走。默认值沿用被它替换掉的那几处 Job 期限（3600 s）：那些界量的是
/// **总时长**，这里量的是**静默**，同一个数在后一种判据下只会更宽松——一次跑两小时、
/// 每一步都在推进的活儿以前会被杀掉，现在不会。
// cap: override — 这个进程上没有一条测量给出窗口；沿用被替换的 Job 期限 3600 s，判据从「总时长」换成「静默」后只会更宽
pub const PROGRESS_FILE_WINDOW_FLOOR_SECS: u64 = 3600;

/// 写进度文件的最小间隔：一拍输出就写一次盘，写盘量会跟着 token 走。
///
/// 它不改判据，只改这条读数自己的开销；一秒一次足够让探针读到「它还活着」，而
/// `mtime` 的精度也就是秒。
// cap: fixed — 写盘频率，不是界；界是文件里那个窗口
const PROGRESS_FILE_WRITE_TICK: Duration = Duration::from_secs(1);

/// kubelet 的 exec 探针在容器里跑的那一条命令。
///
/// 读的正是 [`PROGRESS_FILE_ENV`] 指向的文件：没有文件＝这一跑还没发布过任何一步，
/// 静默超过文件里写的窗口＝它不再动。窗口与被测的静默都来自容器自己，所以探针里
/// 不含任何可调的界——它只把两个数读出来再比。
pub const PROGRESS_PROBE_COMMAND: &str = r#"f="$COGNEVA_PROGRESS_FILE"; if [ -z "$f" ] || [ ! -f "$f" ]; then echo "no progress file (COGNEVA_PROGRESS_FILE='$f')"; exit 1; fi; age=$(( $(date +%s) - $(date -r "$f" +%s) )); win=$(cat "$f"); echo "quiet ${age}s, window ${win}s"; [ "$age" -le "$win" ]"#;

/// 探针多久读一次。
///
/// 这只是判词最晚多久出现，不是判据：判据是文件里那个窗口，探针的节奏只决定
/// 「停住」最多晚几分钟被说出来。
// cap: fixed — 读取周期，不是判据
pub const PROBE_PERIOD_SECS: u64 = 30;

/// 连续几次读数超窗才判死。窗口本身已经是「这套部署从没在干活时静默过这么久」，
/// 再要几次连续读数，是为了不让一次读盘抖动变成一次重启。
// cap: fixed — 连续读数次数，不是界
pub const PROBE_FAILURE_THRESHOLD: u32 = 3;

/// 容器起来之后先给多久再开始探。进程开工第一件事就是建这个文件
/// （[`start_progress_file`]），这段时间只是留给拉镜像与解压的。
// cap: fixed — 开工宽限，不是判据
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
    let window = longest_gap()
        .map(|d| d.as_secs())
        .unwrap_or(0)
        .max(PROGRESS_FILE_WINDOW_FLOOR_SECS);
    if let Err(e) = std::fs::write(path, format!("{window}\n")) {
        // 写不成就没有进度面，而「文件不动」与「根本没有文件」在集群面上同形——
        // 后者会让这一跑在自己的窗口上被杀掉。所以这一条不是噪音。
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
/// 这是**读取周期，不是判据**：判据是 [`stall`] 那条实测窗口，这一拍只决定判词
/// 最晚多久出现，不会让它变宽或变窄。所以它不参与任何「多少秒算停」的决定。
// cap: fixed — 读取周期，不是界；界是实测的步间隔窗口
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
/// 不响」的会。判据与编排侧的回收、执行路由的守护读的是同一条（[`stall`]），因此
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
    let mut burned: Option<(u64, u64)> = None;
    let status = loop {
        tokio::select! {
            status = child.wait() => break status?,
            _ = tokio::time::sleep(WATCH_TICK) => {
                // 干活不只是印东西：编译、链接、算矩阵这一段里它一个字节都不输出，
                // 但它一直在吃 CPU。CPU 时间在涨就是「还在动」最硬的证据——没有
                // 这一拍，一份沉默的重活会在它自己最忙的时候被判成停住。
                if let Some(pid) = child.id() {
                    if let Some(now) = cpu_ticks(pid) {
                        if burned.is_some_and(|before| now != before) {
                            beat(id);
                            if let Some(also) = &also {
                                also();
                            }
                        }
                        burned = Some(now);
                    }
                }
                if let Some((silent, window)) = stall(id) {
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

/// Test hook: clear every beacon and every sample.
///
/// Exists because this state is process-global and the tests in different
/// modules would otherwise read each other's beacons — a specimen that shares
/// the population it measures is the failure mode this module's own tests must
/// not have.
#[doc(hidden)]
pub fn reset_for_tests() {
    beacons().lock().unwrap_or_else(|p| p.into_inner()).clear();
    *gaps().lock().unwrap_or_else(|p| p.into_inner()) = GapSamples::default();
}

#[cfg(test)]
mod tests {
    use super::*;

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
        seed_gaps_for_tests(Duration::from_millis(500), SAMPLE_FLOOR);

        let mut cmd = tokio::process::Command::new("sh");
        cmd.args([
            "-c",
            "i=0; while [ $i -lt 40 ]; do echo step $i; sleep 0.04; done",
        ]);
        // 40 steps × 40 ms runs three times past the window, and no single gap
        // between two steps is anywhere near it: nothing but the beats keeps the
        // child alive, which is the point.
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
            PROGRESS_FILE_WINDOW_FLOOR_SECS.to_string(),
            "还没量到窗口时写的是下限；量到之后只可能更大"
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
}
