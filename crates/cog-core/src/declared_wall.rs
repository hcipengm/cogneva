//! What to do when work outlives the wall someone declared for it.
//!
//! The old answer was to kill it. That answer is wrong whenever the thing that
//! overran is still doing the work: a `buildah push` cut at 1800 seconds throws
//! away a push that was about to finish, a `cargo build` cut at its budget
//! throws away every object it had already compiled, a `cargo test` cut at its
//! budget throws away the whole run and reports nothing. The host paid for all
//! of that work; killing it at a round number refunds none of it and costs the
//! next round the whole price again.
//!
//! So a declared wall is an **expectation**, not a knife: passing it publishes
//! one line saying the work is slower than expected, and the work is waited on
//! for as long as it lives. What actually ends it is the thing itself exiting,
//! or the machine losing the process — never a number we picked in advance.
//!
//! This lives in one place because the same control law copied into a dozen
//! files drifts: one copy keeps the warning, another silently drops it, and the
//! next reader cannot tell which behaviour this system has.
//!
//! The law has two clauses, and this module is their one home. It has two
//! entries because the clauses are asked at different times with different
//! evidence:
//!
//! * **Expectation** — [`wait_past_wall`] and [`wait_past_wall_beating`] — "this
//!   is taking longer than anyone expected": publish a line and keep waiting.
//!   It never ends anything.
//! * **Evidence** — [`wait_until_stalled`], over [`stall`]/[`stall_any`], with
//!   the line drawn at a [`Bound`] — "nothing has moved". It is the only thing
//!   in the system that may end a run on a time-shaped reading, and it refuses
//!   to answer while no run has measured how quiet this deployment gets while
//!   working.
//!
//! Silence is not the same fact as "not working", and the evidence clause must
//! not confuse them. A compile, a link, a big query and a `buildah push` all
//! spend long stretches producing no step and printing nothing while moving
//! perfectly well. Two things follow. First, "still moving" is read from as many
//! direct sources as exist: [`cog_core::progress::watch_child`] reads the
//! counters the kernel keeps for the child — CPU ticks, bytes read and written —
//! and counts a counter that moved as work that moved, so a silent compile is
//! never quiet; a backup pass names channels that are each a direct piece of
//! evidence (bytes copied, peer executing, step started) rather than a promise
//! to say something. Second, "the counter could not be read" is
//! [`Bound::Unmeasured`]'s sibling, never a zero. What the verdict then claims
//! is the narrowest true statement available: nothing this system can observe
//! has moved, for longer than anything it has ever observed while working.
//!
//! The evidence clause is a closed loop, not a constant: a run adopts the bound
//! the previous run measured ([`Bound::adopt`]), judges against it, and hands
//! back its own extreme when it ends ([`Bound::observed`]). "Not measured yet"
//! is a value of its own ([`Bound::Unmeasured`]) rather than a zero or a
//! default, and it is written with one word everywhere it is shown
//! ([`Bound::words`]) — so a caller with no measurement waits, which is the
//! cheap error.
//!
//! The samples the bound is made of live in [`cog_core::progress`], because a
//! beat is recorded from wherever work actually advances; what this module fixes
//! is the law those samples are read through. A third copy of either clause is
//! the defect this module exists to prevent: before it existed, the backup drill
//! carried its own "silent for N seconds" watchdog with a number written into
//! the deployment, which is an expectation wearing the clothes of a judgement.

use std::future::Future;
use std::time::Duration;

use crate::progress::{
    adopt_longest_gap, last_beat, longest_gap, observed_longest_gap, PROGRESS_FILE_UNBOUNDED,
};

/// 一道判据上的界：一次实测，或者「还没人量过」。
///
/// [`longest_gap`] 用 `Option` 说这件事，但它的读者有三种：判停的人、写进度文件的人、
/// 写回持久面的人。三处各自把 `None` 翻成一句话，翻法就会分叉——一处写 0、一处写默认
/// 值、一处留空，而「量不到」与「量到就是这个值」也就分不开了。做成一个值，词只有
/// 一份（[`Bound::words`]），判据（[`stall`]）也只从它取界。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bound {
    /// 这个作业类在它**还在干活**的时候，静默过这么久。
    Measured(Duration),
    /// 还没人量过：不是 0，也不是某个默认值。
    Unmeasured,
}

impl Bound {
    /// 这一跑现在能用的界：自己量到的，或者开工时采纳的上一次运行。
    pub fn now() -> Self {
        longest_gap().map_or(Self::Unmeasured, Self::Measured)
    }

    /// 这一跑**自己**量到的最长间隔——写回持久面的那个数，不含采纳值。
    pub fn observed() -> Self {
        observed_longest_gap().map_or(Self::Unmeasured, Self::Measured)
    }

    /// 持久面读回来的那一格。缺失、非正数、非有限，一律读作 [`Bound::Unmeasured`]：
    /// 一个读不出来的数不是一道更小的界，是没有界。
    pub fn from_stored(seconds: Option<f64>) -> Self {
        match seconds {
            Some(s) if s.is_finite() && s > 0.0 => Self::Measured(Duration::from_secs_f64(s)),
            _ => Self::Unmeasured,
        }
    }

    pub fn value(self) -> Option<Duration> {
        match self {
            Self::Measured(gap) => Some(gap),
            Self::Unmeasured => None,
        }
    }

    /// 把界装进这一跑的判据，让 [`stall`] 与 [`stall_any`] 从现在起用它。
    ///
    /// 只装实测值：`Unmeasured` 什么都不装，于是判据仍是「量不到 ⇒ 不判停」——
    /// 没有一条路能从这里装进一个挑出来的数。
    pub fn adopt(self) {
        if let Self::Measured(gap) = self {
            adopt_longest_gap(gap);
        }
    }

    /// 写给人看的那一格：秒数，或者「没有界」那个词。
    ///
    /// 秒数**向上取整、至少 1**。这一格是容器外那个探针的判据，截断是往窄里走，
    /// 而窄的那一侧正好是误杀：0.4 秒的界写成「0」，探针下一拍就把一个还在推进的
    /// 容器判死。宽出去的那不到一秒是这一侧唯一可接受的方向。
    pub fn words(self) -> String {
        match self {
            Self::Measured(gap) => gap.as_secs_f64().ceil().max(1.0).to_string(),
            Self::Unmeasured => PROGRESS_FILE_UNBOUNDED.to_string(),
        }
    }
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
    let longest = Bound::now().value()?;
    let silent = last.elapsed();
    (silent > longest).then_some((silent, longest))
}

/// 同一件事，按一组任务问：这一组里最近的一拍到现在多久，那道实证界是多少。
///
/// 备份与演练的一个「步骤」由好几条通道同时作证——这一步开工（步骤拍）、搬过一块
/// 数据（数据拍）、对面还在执行（对端拍）。把「哪一拍最年轻」算进去，判据就只看
/// 最新的那条证据；任何一条还在动，这一步就没有静默。
///
/// 这条判据的强度完全取决于**每条通道是不是一个直接证据**。所以记拍的规矩是：
/// 只有「确实搬过一个单位 / 对面确实在跑 / 内核确实在记它」才记，轮询本身不算——
/// 一条由轮询喂着的通道会把「它在动」永远写下去，让判据永远不响，而那种从来不响
/// 的判据读起来和「一切正常」一模一样。
pub fn stall_any(tasks: &[&str]) -> Option<(Duration, Duration)> {
    let last = tasks.iter().filter_map(|task| last_beat(task)).max()?;
    let longest = Bound::now().value()?;
    let silent = last.elapsed();
    (silent > longest).then_some((silent, longest))
}

/// 「它还在动」——由内核替它记的那笔账，读给判据听。
///
/// 判据的输入不只是「有没有打拍」。编译、链接、算一个矩阵、搬一个几 GB 的包，这些
/// 工作可以很久不报一步、一个字节都不印，而它们一直在动；只按「没有拍」收尾，就是把
/// 最忙的那一段读成停住。所以判停之前先读一次这个进程的计数（CPU 与读写字节），动了
/// 就是动了。
///
/// 这条口径写成一个小东西，而不是每个调用点各写一遍：口径一散，一处把「读不到」当
/// 零、一处当「没动」，判据就开始互相矛盾——那正是这个模块存在要防的事。
pub struct Movement {
    previous: Option<crate::progress::Activity>,
    /// 上一次读到的那个 pid。换了 pid 就不能拿旧读数比：两个进程的计数没有可比性。
    pid: Option<u32>,
}

impl Default for Movement {
    fn default() -> Self {
        Self::new()
    }
}

impl Movement {
    pub fn new() -> Self {
        Self {
            previous: None,
            pid: None,
        }
    }

    /// 读一次 `pid` 的计数；与上一次相比动过，就 `mark(evidence)` 并回答 `true`。
    ///
    /// 用 `mark` 而不是 `beat`：这是读数，不是一个完成的单位。进了间隔水库，一秒一次的
    /// 读数会把实测极值压成采样周期，下一趟就拿着一个不存在的窄界去判停。
    ///
    /// 读不到（进程已退出、平台没有 procfs）回答 `false`：量不到不是「没在动」，这里
    /// 只主张「看到了动」，不主张它的反面。
    pub fn sample(&mut self, pid: u32, evidence: &str) -> bool {
        // 换了 pid，上一次的计数说的就是另一个进程，不能拿来比。
        let previous = if self.pid == Some(pid) {
            self.previous
        } else {
            None
        };
        let Some(now) = crate::progress::process_activity(pid) else {
            self.previous = None;
            self.pid = Some(pid);
            return false;
        };
        let moved = previous.is_none_or(|before| now.moved_since(before));
        self.previous = Some(now);
        self.pid = Some(pid);
        if moved {
            crate::progress::mark(evidence);
        }
        moved
    }
}

/// 一次「问外部状态」的回答。
///
/// 三种，缺一不可。`Moving` 是这一次读到的状态（与上一次不同才算动过）；
/// `Done` 是这件事自己说了「成了」；`Failed` 是**直接证据**说了「不成」——
/// 崩溃、拉不到镜像、平台自己声明失败。只有 `Failed` 可以结束一件还在动的活；
/// 「读了多少秒」不在这个枚举里，所以它没有一条路能把钟变成判词。
pub enum Step<T> {
    Moving(T),
    Done(T),
    Failed(String),
}

/// 反复读一个外部状态，直到它完成或者读出失败的直接证据。
///
/// 这是 [`wait_until_stalled`] 的兄弟：那条等的是「通道不再打拍」，这条等的是
/// 「读回来的状态不再变」。部署器等的正是这种东西——Pod 重建、副本就绪、
/// 滚动推进，每一步都是一个能读到的状态；一个 `kubectl rollout status --timeout`
/// 把「等了二十秒」写成了判词，而二十秒里滚动可能一直在往前走。
///
/// 两条证据各自成立、互不顶替：
///
/// * 推进：读回来的状态变了就记一拍（[`crate::progress::mark`]，不产生间隔样本）。
///   第一次读数算一次，理由同 [`Movement::sample`]：基线本身是证据。
/// * 判停：[`stall_any`] 量到的界。界量不到就一直等——这是这条律最便宜的那一侧。
///
/// 读不出来不是「没动」：一次失败的 `kubectl get` 只说明这一眼看不清，调用方
/// 继续等，日志留一行。而过了申报的墙只说话（[`wait_past_wall`]），不收尾。
pub async fn wait_for_settled<T, F, Fut>(
    declared: Duration,
    what: &str,
    channel: &str,
    poll: Duration,
    read: F,
) -> Result<T, String>
where
    T: PartialEq,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Step<T>, String>>,
{
    wait_for_settled_with(declared, what, channel, poll, ClosureProbe(read)).await
}

/// 读一眼外部状态的闭包在 [`SettleProbe`] 里的样子。无状态的等待都走这一条。
struct ClosureProbe<F>(F);

impl<T, F, Fut> SettleProbe for ClosureProbe<F>
where
    T: PartialEq,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Step<T>, String>>,
{
    type State = T;

    async fn step(&mut self) -> Result<Step<T>, String> {
        (self.0)().await
    }
}

/// 一个自己持有状态的等待：每叫一次 [`SettleProbe::step`] 读一眼外部状态。
///
/// 有些等待读一眼回答不了：滚动等待的每一轮都要用到前面几轮攒下的东西（见过哪种
/// 证据、暂停握了多久、宿主负载采样了多少拍），而这些累积又改变下一眼怎么解释。
/// 把它们塞进 `FnMut` 闭包做不到——闭包交回的那枚 future 会借用它捕获的状态，这在
/// 借用检查里没有可写出来的形状。所以状态住在实现者身上，
/// [`wait_for_settled_with`] 只管那条控制律。
///
/// 实现者只该回答「这一眼读到了什么」，不许自己定判停的钟：两条证据
/// （[`Step`] 与 [`stall_any`]）都由下面那个函数统一执行。
pub trait SettleProbe {
    type State: PartialEq;

    fn step(&mut self) -> impl Future<Output = Result<Step<Self::State>, String>>;
}

/// 借用来的实现：
impl<P: SettleProbe + ?Sized> SettleProbe for &mut P {
    type State = P::State;

    async fn step(&mut self) -> Result<Step<P::State>, String> {
        (**self).step().await
    }
}

/// [`wait_for_settled`] 的本体，状态由调用方带着进来、带着出去。
///
/// 两个入口共用这一段循环，因为它们判的是同一件事：读到的状态变了才算动
/// （[`crate::progress::mark`]，不产生间隔样本），直接证据说了「不成」才收尾
/// （[`Step::Failed`]），而「读了多少秒」永远不构成收尾——申报的墙只说话
/// （[`wait_past_wall`]），判停只认实测界（[`stall_any`]）。
pub async fn wait_for_settled_with<P: SettleProbe>(
    declared: Duration,
    what: &str,
    channel: &str,
    poll: Duration,
    mut probe: P,
) -> Result<P::State, String> {
    wait_past_wall(declared, what, async move {
        let mut last: Option<P::State> = None;
        loop {
            let mut read_failed = false;
            match probe.step().await {
                Ok(Step::Done(done)) => return Ok(done),
                Ok(Step::Failed(why)) => return Err(why),
                Ok(Step::Moving(state)) => {
                    if last.as_ref() != Some(&state) {
                        last = Some(state);
                        crate::progress::mark(channel);
                    }
                }
                Err(e) => {
                    read_failed = true;
                    tracing::warn!(
                        channel,
                        error = %e,
                        "could not read the state this wait watches; a failed read is not silence"
                    );
                }
            }
            // 这一眼没读到的，这一拍就不判：静默的读数说的是「外部什么都没动」，而一次
            // 读失败只说「我们看不见」。把后者算进前者，集群短暂不可达就会变成一条
            // 判停判词——正是一次误杀；而它连一次都没读到，界再宽也证明不了什么。
            if !read_failed {
                if let Some((silent, bound)) = stall_any(&[channel]) {
                    return Err(stall_verdict(what, silent, bound));
                }
            }
            tokio::time::sleep(poll).await;
        }
    })
    .await
}

/// 判停那一条判词的开头。调用方要按它把「实测界判停」与「直接证据说不成」分开
/// ——两者在处置上是两回事（一个是我们量到的事不动了，一个是外部自己说的失败），
/// 而入口只交回一句话，所以这句话得有一个只有一种写法的开头。
pub const STALL_VERDICT_MARKER: &str = "showed no movement for";

fn stall_verdict(what: &str, silent: Duration, bound: Duration) -> String {
    format!(
        "{what} {STALL_VERDICT_MARKER} {}s, past the measured {}s window",
        silent.as_secs(),
        bound.as_secs()
    )
}

/// 这句话是不是 [`stall_verdict`] 写的。判据只认那一个开头词，不认整句措辞。
pub fn is_stall_verdict(why: &str) -> bool {
    why.contains(STALL_VERDICT_MARKER)
}

/// 平台自己写下的「进度死线用完了」判词。
///
/// Deployment 控制器在 `progressDeadlineSeconds`（清单不写时取平台缺省）之内看不到
/// 任何推进，才会把这一条写进 `Progressing` 条件。它是**直接证据**，不是我方挑的
/// 一个数：推进一次，平台的死线就重新起算，而一道固定的钟不会。等它的那一侧只
/// 负责把这一格读出来——判据就是平台自己那个词。
///
/// 词表放在这里而不是各调用点：装机器的引导器与部署器等的是同一件事，两处各写一个
/// 字面量时，平台改名的那天只有一处会失效，而失效的一侧读作「平台没说过话」。
pub const PROGRESS_DEADLINE_EXCEEDED: &str = "ProgressDeadlineExceeded";

/// 这份读数里平台有没有判过「进度死线用完了」。
///
/// 不看位置只看词：Deployment 与 DaemonSet 的读法把这一格放在不同下标上，而原因
/// 是一个闭集里的词，别的格里不可能出现同一个词。按下标读就要在每一份读法旁边记
/// 一个数，记错了不会报错，只会在谁也看不见的地方把判据换成另一格。读法自己写错
/// 时这一格读回来是空的——判据缺席，不是误判。
pub fn saw_progress_deadline(readout: &str) -> bool {
    readout
        .split('|')
        .any(|part| part.trim() == PROGRESS_DEADLINE_EXCEEDED)
}

/// 控制器处理过这一代模板了没有：`observedGeneration` 追上了 `metadata.generation`。
pub fn generation_reaches(generation: u64, observed: u64) -> bool {
    generation > 0 && observed >= generation
}

/// 一份 `metadata.generation|status.observedGeneration|…` 读数的最前面两格。
///
/// 这一族的读法（部署器、支撑面、装机引导器）都把这两格放在最前面。拆字符串这件
/// 事在每一处各写一遍时，写错的那一处只会静默地把上一代的判词安到这一代头上——
/// 而那份判词会直接结束一次刚下发的滚动。
pub fn readout_generation(readout: &str) -> (u64, u64) {
    let mut parts = readout.split('|');
    let generation = parts
        .next()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    let observed = parts
        .next()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    (generation, observed)
}

/// 平台写下的进度死线判词，**且它说的是这一代**。
///
/// `apply` 之后的第一眼往往早于控制器：那一段读数还是上一代的账，可能正带着上一代
/// 留下的 `ProgressDeadlineExceeded`。认了它就是拿上一代的事给刚下发的一代收尾——
/// 一次误杀，而误杀的那一侧要重来。平台这句话只有对控制器已经处理过的那一代作数。
pub fn platform_declares_stall(generation: u64, observed: u64, readout: &str) -> bool {
    generation_reaches(generation, observed) && saw_progress_deadline(readout)
}

/// 第二入口：装好界之后，等到**证据**说这一组通道不再动为止。
///
/// 返回（静默了多久，那道界）。界不是一个参数，而是 [`Bound::now`]：上一次运行量到的
/// 极值，或这一跑自己攒到的样本。**量不到时它不返回**——没有证据就继续等，把一个
/// 没人量过的数当界去收尾是这里最贵的错误。
///
/// 循环放在这里而不是每个作业类各写一遍：多久看一眼（[`cog_core::progress::WATCH_TICK`]）
/// 是这条控制律自己的节奏，不是调用方的参数；调用方只该回答「看哪几条通道」。
pub async fn wait_until_stalled(watched: &[&str]) -> (Duration, Duration) {
    loop {
        tokio::time::sleep(crate::progress::WATCH_TICK).await;
        // 本进程的活动**不**自动算进来：很多 waiter 自己在轮询（借连接读对面的状态、
        // 写日志），轮询本身就是 I/O，把它记成一拍会让这条通道永远新鲜，于是判停永远
        // 不成立——一条永远不响的判据比一个钟更坏，因为它读起来像「一切正常」。证据
        // 由调用方点名：哪个 pid 在动，谁有资格记这一拍，写在 `watch_child` 里。
        if let Some(verdict) = stall_any(watched) {
            return verdict;
        }
    }
}

/// How often the waiter wakes. It only exists so the completion of the inner
/// future can be raced against a clock; it is not a wall.
// cap: bound — 只排期、从不取消任何东西：进度由内层 future 决定，这个数只定回来看一眼的节拍
pub const POLL_EVERY: Duration = Duration::from_secs(15);

/// 轮询间隔的测试覆盖，0 表示用 [`POLL_EVERY`]。
///
/// 与节拍本身同一个理由：一个测试要观察「等待期间心跳有没有动」，就得让轮询快过
/// 测试自己的寿命，否则它只能在十秒级的节拍上干等。生产路径一字不改。
static POLL_OVERRIDE_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn poll_every() -> Duration {
    let ms = POLL_OVERRIDE_MS.load(std::sync::atomic::Ordering::Relaxed);
    if ms == 0 {
        POLL_EVERY
    } else {
        Duration::from_millis(ms)
    }
}

/// 测试钩子：把轮询间隔改短。传 [`Duration::ZERO`] 恢复生产值。
#[doc(hidden)]
pub fn set_poll_every_for_tests(poll: Duration) {
    POLL_OVERRIDE_MS.store(
        poll.as_millis() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// Once the declared wall is behind us, how often to say so again. A build that
/// has been running for three hours is a different fact from one that ran over
/// by a minute, and the first warning alone would read as stale — the same
/// reason any measured figure needs a freshness rule.
// cap: bound — 只决定超墙之后重复说一句的间隔，从不取消任何东西
pub const REPEAT_EVERY: Duration = Duration::from_secs(300);

/// Await `fut` to completion, warning when it outlives `declared`.
///
/// `declared` of zero means the caller declared nothing: the future is simply
/// awaited and no line is published.
pub async fn wait_past_wall<F>(declared: Duration, what: &str, fut: F) -> F::Output
where
    F: Future,
{
    wait_past_wall_beating(declared, what, || {}, fut).await
}

/// The same, with a beat callback run on every poll.
///
/// Long waits are observed by loops that treat silence as a stall (the build
/// gate's claim renewal, the deployer's round beat). A wait that stops calling
/// back reads as a dead process to every one of them, so the caller passes the
/// same beat it would have made had it not been waiting.
pub async fn wait_past_wall_beating<F, B>(
    declared: Duration,
    what: &str,
    beat: B,
    fut: F,
) -> F::Output
where
    F: Future,
    B: FnMut(),
{
    wait_with_poll(poll_every(), declared, what, beat, fut).await
}

/// The loop itself, with the poll period injected so tests do not have to sit
/// through a production cadence to observe it.
async fn wait_with_poll<F, B>(
    poll: Duration,
    declared: Duration,
    what: &str,
    mut beat: B,
    fut: F,
) -> F::Output
where
    F: Future,
    B: FnMut(),
{
    tokio::pin!(fut);
    // 用 tokio 的钟，不用 `std::time::Instant`：下面 `select!` 里的睡眠走的是
    // tokio 的钟，`start_paused` 只虚化它。两个钟混用时，暂停时钟的测试里睡眠「过
    // 了」45 秒而 `elapsed()` 仍是真实的 0，于是这条律一次都不说、测试拿到空日志；
    // 生产里两个钟都是真实时间，看不出差别——差别只在被测的那一档暴露，所以判据
    // 必须和它等的那个钟同源。
    let started = tokio::time::Instant::now();
    let mut said_so = false;
    // 重复的节拍从**申报点**算起，不是从开工算起：`declared + REPEAT_EVERY`
    // 才是「第二次该说」的时刻。
    let mut next_say = declared + REPEAT_EVERY;
    loop {
        tokio::select! {
            out = &mut fut => return out,
            _ = tokio::time::sleep(poll) => {
                beat();
                if declared.is_zero() {
                    continue;
                }
                let elapsed = started.elapsed();
                // 还没到申报的墙就什么都不说。轮询只是回来看一眼的节拍（默认
                // 15s），它比申报点更细；不判这一下的话，第一次轮询就会打出
                // 「已超过申报墙 elapsed=15s / declared=30s」——读数自相矛盾，
                // 而且把「还没到期望工期」读成了「超期」。
                if elapsed < declared {
                    continue;
                }
                if !said_so || elapsed >= next_say {
                    said_so = true;
                    while elapsed >= next_say {
                        next_say += REPEAT_EVERY;
                    }
                    tracing::warn!(
                        what,
                        declared_secs = declared.as_secs(),
                        elapsed_secs = elapsed.as_secs(),
                        "work has outlived the wall declared for it and is still running; waiting for it instead of killing work in progress"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 平台那句判词只属于它说出口的那一代。
    ///
    /// `apply` 之后的第一眼往往早于控制器，读数还是上一代的账，可能正带着上一代
    /// 留下的 `ProgressDeadlineExceeded`。把它认成对新下发的这一代的判词，就是一次
    /// 误杀：滚动在第一个动作上被记成「这一版坏了」，而这一版一个 Pod 都还没建。
    #[test]
    fn a_stale_deadline_does_not_condemn_the_generation_just_applied() {
        // 上一代判死、这一代刚下发（gen 7，控制器只处理到 6）。
        let stale = "7|6|1|0|0|0|ProgressDeadlineExceeded";
        let (generation, observed) = readout_generation(stale);
        assert_eq!((generation, observed), (7, 6));
        assert!(
            !platform_declares_stall(generation, observed, stale),
            "拿上一代的判词给刚下发的一代收了尾"
        );
        assert!(!generation_reaches(7, 6));
        assert!(!generation_reaches(0, 0), "没有代数时不能读成「处理过了」");

        // 控制器处理完这一代，判词说的就是这一代了。
        let current = "7|7|1|0|0|0|ProgressDeadlineExceeded";
        let (generation, observed) = readout_generation(current);
        assert!(platform_declares_stall(generation, observed, current));

        // 没有判词的读数当然不成立（判据缺席不是判词）。
        assert!(!platform_declares_stall(7, 7, "7|7|1|0|0|0|"));
    }

    /// The shape the whole module exists for: work that runs past its declared
    /// wall finishes, and its own output comes back. A killer would have
    /// returned neither.
    #[tokio::test]
    async fn work_that_outlives_its_wall_is_waited_on_not_killed() {
        let started = std::time::Instant::now();
        let got = wait_past_wall(Duration::from_millis(10), "test work", async {
            tokio::time::sleep(Duration::from_millis(120)).await;
            "finished on its own"
        })
        .await;
        assert_eq!(got, "finished on its own");
        assert!(
            started.elapsed() >= Duration::from_millis(120),
            "returned after {:?}: the work was not waited on",
            started.elapsed()
        );
    }

    /// A caller that declared nothing gets no warning and the same answer.
    #[tokio::test]
    async fn no_declared_wall_just_means_wait() {
        let got = wait_past_wall(Duration::ZERO, "no wall", async { 7 }).await;
        assert_eq!(got, 7);
    }

    /// 外部状态自己说「成了」就返回——等了多少秒不参与。
    #[tokio::test]
    async fn a_wait_for_state_ends_on_the_state_not_on_the_clock() {
        use crate::progress;
        let _serial = progress::async_test_lock().await;
        progress::reset_for_tests();
        let mut reads = 0;
        let got = wait_for_settled(
            Duration::ZERO,
            "state wait",
            "state-wait-channel",
            Duration::from_millis(5),
            || {
                reads += 1;
                let reads = reads;
                async move {
                    Ok(match reads {
                        1 => Step::Moving("replicas 1/3"),
                        2 => Step::Moving("replicas 2/3"),
                        _ => Step::Done("replicas 3/3"),
                    })
                }
            },
        )
        .await
        .expect("the state said it was done");
        assert_eq!(got, "replicas 3/3");
        assert_eq!(reads, 3, "returned before the state was read to done");
        progress::reset_for_tests();
    }

    /// 有状态的探针把跨轮攒下的东西带在身上，等待只借它、不拥有它。
    ///
    /// 滚动等待就是这个形状：前几眼学到的东西决定后几眼怎么读（见过哪种证据、
    /// 暂停握了多久、宿主负载采样了几拍）。这些状态塞不进 `FnMut` 闭包——闭包交回
    /// 的那枚 future 会借用它捕获的状态，借用检查里没有可写出来的形状——所以第二
    /// 入口收一个 [`SettleProbe`]，并且允许以 `&mut` 传进来：等待结束之后，调用方
    /// 还要读它攒下的东西来写判词。
    #[tokio::test]
    async fn a_stateful_probe_carries_what_it_learned_and_hands_it_back() {
        use crate::progress;
        let _serial = progress::async_test_lock().await;
        progress::reset_for_tests();

        struct Counting {
            reads: u32,
            done_at: u32,
        }
        impl SettleProbe for Counting {
            type State = u32;

            async fn step(&mut self) -> Result<Step<u32>, String> {
                self.reads += 1;
                if self.reads >= self.done_at {
                    Ok(Step::Done(self.reads))
                } else {
                    Ok(Step::Moving(self.reads))
                }
            }
        }

        let mut probe = Counting {
            reads: 0,
            done_at: 3,
        };
        let done = wait_for_settled_with(
            Duration::ZERO,
            "a state only the third reading can settle",
            "stateful-probe",
            Duration::from_millis(5),
            &mut probe,
        )
        .await
        .expect("the third reading said it was done");
        assert_eq!(done, 3);
        assert_eq!(
            probe.reads, 3,
            "the wait borrows the probe; what it learned must still be there afterwards"
        );
        progress::reset_for_tests();
    }

    /// 直接证据说「不成」就立刻结束，不等到任何墙。
    #[tokio::test]
    async fn direct_failure_evidence_ends_the_wait() {
        use crate::progress;
        let _serial = progress::async_test_lock().await;
        progress::reset_for_tests();
        let err = wait_for_settled(
            Duration::from_secs(3600),
            "state wait",
            "failure-channel",
            Duration::from_millis(5),
            || async { Ok::<_, String>(Step::<&str>::Failed("pod is in CrashLoopBackOff".into())) },
        )
        .await
        .expect_err("a direct failure must end the wait");
        assert!(err.contains("CrashLoopBackOff"), "{err}");
        progress::reset_for_tests();
    }

    /// 读不出来不是「没动」：一直读失败就一直等，不按一次读失败收尾。
    ///
    /// 没有一拍被记下来（读失败不记拍），所以判停也没有证据可用——这条同时锁住
    /// 「失败读数不许被当成静默」与「量不到就不判」。
    #[tokio::test]
    async fn a_failed_read_is_not_silence() {
        use crate::progress;
        let _serial = progress::async_test_lock().await;
        progress::reset_for_tests();
        let waited = tokio::time::timeout(
            Duration::from_millis(120),
            wait_for_settled(
                Duration::ZERO,
                "state wait",
                "unreadable-channel",
                Duration::from_millis(5),
                || async { Err::<Step<&str>, String>("kubectl: connection refused".into()) },
            ),
        )
        .await;
        assert!(
            waited.is_err(),
            "a failed read ended the wait: an unreadable state was read as no movement"
        );
        progress::reset_for_tests();
    }

    /// 第二入口：没有实测界时它不返回（不判停），装上一条实测界之后立刻按它判。
    ///
    /// 两半缺一不可。只有第一半，它和「永远挂着」分不开；只有第二半，「量不到」就又会
    /// 被某个默认值顶掉——那正是这条控制律要拆掉的钟。
    #[tokio::test]
    async fn the_evidence_entry_refuses_to_judge_without_a_measured_bound() {
        use crate::progress;
        let _serial = progress::async_test_lock().await;
        progress::reset_for_tests();
        progress::beat("watched-by-law");

        // 静默已经超过一秒：若界是任何一个挑出来的默认值，这一半就是红的。
        let silent_verdict = tokio::time::timeout(
            Duration::from_millis(1500),
            wait_until_stalled(&["watched-by-law"]),
        )
        .await;
        assert!(
            silent_verdict.is_err(),
            "没有任何实测界时第二入口给出了判词：界是被人替测量挑出来的"
        );

        Bound::Measured(Duration::from_millis(5)).adopt();
        let (silent, bound) = tokio::time::timeout(
            Duration::from_secs(3),
            wait_until_stalled(&["watched-by-law"]),
        )
        .await
        .expect("装上实测界之后就应当判停");
        assert_eq!(bound, Duration::from_millis(5));
        assert!(silent > bound);
        progress::reset_for_tests();
    }

    /// 读不到读数时不主张「没在动」。
    ///
    /// `sample` 只主张它**看到过**一次动；读不到（进程已退出、平台没有 procfs）回答
    /// `false`，并且不打拍。把「量不到」写成「没动」，判据就会在一个它根本看不见的
    /// 进程上按静默收尾——那是这条控制律最贵的那种错。
    ///
    /// pid 0 在 `/proc` 里永远不会有一个进程，所以这一条不靠时序。
    #[test]
    fn a_movement_reading_that_cannot_be_taken_claims_nothing() {
        let _serial = crate::progress::test_lock();
        crate::progress::reset_for_tests();
        let mut movement = Movement::new();
        assert!(!movement.sample(0, "never-read"));
        assert_eq!(
            crate::progress::last_beat("never-read"),
            None,
            "读不到读数却记了一拍：这一条通道会替一个看不见的进程说话"
        );
        crate::progress::reset_for_tests();
    }

    /// 第一次读是一次基线，因此算「动过」：一个刚起来的子进程在第一拍就该被记上，
    /// 而不是等到它的计数**变化**——那要等到它真正烧掉一个时钟滴答，而中间那一段
    /// 正好是最容易被读成静默的启动期。
    #[test]
    fn the_first_reading_of_a_live_process_counts_as_movement() {
        let _serial = crate::progress::test_lock();
        crate::progress::reset_for_tests();
        let mut movement = Movement::new();
        assert!(movement.sample(std::process::id(), "live-process"));
        assert!(
            crate::progress::last_beat("live-process").is_some(),
            "第一次读到活动却没有记拍"
        );
        crate::progress::reset_for_tests();
    }

    /// The beat runs while waiting, which is what keeps a long wait from
    /// reading as a stalled process.
    #[tokio::test]
    async fn the_beat_runs_while_we_wait() {
        let beats = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let b = beats.clone();
        let _ = wait_with_poll(
            Duration::from_millis(10),
            Duration::ZERO,
            "beating work",
            move || {
                b.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
            async {
                tokio::time::sleep(Duration::from_millis(45)).await;
            },
        )
        .await;
        assert!(
            beats.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "the beat never ran during a wait longer than two polls"
        );
    }

    /// 还没到申报的墙，一个字都不许说。
    ///
    /// 轮询（默认 15s）比申报点细，而真实的调用点就落在这一档上：supervisor 的
    /// 一轮期望是 30s、检查点每步的期望是 15s。不判这一下的话，第一次轮询就打
    /// 出一条「已超过申报墙 elapsed_secs=15 / declared_secs=30」——读数自相矛盾，
    /// 而且把「还没到期望工期」读成「超期」，告警面从此不可信。
    ///
    /// 用暂停的时钟跑：真等 45 秒不是这条判据的价格。
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn nothing_is_said_before_the_declared_wall() {
        use std::sync::{Arc, Mutex};
        use tracing::instrument::WithSubscriber;

        #[derive(Clone)]
        struct Sink(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = Sink(buf.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();

        // 申报 30s、干 45s：轮询会在 15s、30s 各醒一次。
        let got = wait_past_wall(Duration::from_secs(30), "slow work", async {
            tokio::time::sleep(Duration::from_secs(45)).await;
            7
        })
        .with_subscriber(subscriber)
        .await;
        assert_eq!(got, 7);

        let log = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            !log.contains("elapsed_secs=15"),
            "申报点（30s）之前就说了「已超过」：\n{log}"
        );
        assert_eq!(
            log.matches("outlived the wall").count(),
            1,
            "墙后只说一次（重复节拍是 300s，这次一共才 45s）：\n{log}"
        );
        assert!(
            log.contains("elapsed_secs=30"),
            "第一次说应当正好在申报点上：\n{log}"
        );
    }
}
