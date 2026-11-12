//! Observable implementation for cog-agent.
//! Exposes the agent's cumulative run counters.

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

static GLOBAL: OnceLock<Arc<AgentObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<AgentObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(AgentObservable::new()))
        .clone()
}

/// Agent-level observable state.
/// Tracks how the agent's runs ended, so the iteration ceiling can be derived
/// from what this role has actually delivered.
#[derive(Default)]
pub struct AgentObservable {
    run_count: AtomicU64,
    success_count: AtomicU64,
    budget_exhausted_count: AtomicU64,
    total_steps: AtomicU64,
    total_tool_calls: AtomicU64,
    /// Per-role counts of how a role's runs ended, keyed by role.
    ///
    /// Readings, not inputs: a label says which roster row keeps meeting a
    /// guardrail, and nothing here decides what a run does. The key is a role,
    /// which is a closed set the code names — not a task id or a path, which
    /// would grow without bound.
    role_calibrations: Arc<Mutex<HashMap<String, RoleCalibration>>>,
    /// Iterations consumed by the most recent runs that delivered on their own,
    /// one window for the process and one for each employee.
    ///
    /// The treatment rule is that a piece of work is treated by what it declares,
    /// by the measured delivery record of the person taking it, and by the
    /// operator's explicit override — so the person's own finished runs answer
    /// first. The process window stays as the fallback for an employee with no
    /// record of its own: a new hire has none, and reading that as `unmeasured`
    /// would hand every first run no ceiling at all.
    ///
    /// The two windows stay distinguishable in the reading (`measured_own` /
    /// `measured_process`): "measured from this person" and "measured from the
    /// process" are different claims, and a single `measured` label would make
    /// them look the same.
    ///
    /// Neither is keyed by role: a role is a free label on the roster, and the
    /// same work must not run under a different ceiling because a different name
    /// was written on the worker. The per-employee map is bounded by the live
    /// roster — [`Self::publish_roster`] drops the windows of employees that are
    /// no longer there — so employee churn cannot grow it without bound.
    delivered_iterations: Arc<Mutex<VecDeque<u32>>>,
    delivered_iterations_by_agent: Arc<Mutex<HashMap<String, VecDeque<u32>>>>,
    /// The agents the manager currently holds, one entry per live object.
    ///
    /// Published by the manager on every change and read at scrape time, so the
    /// count is the roster as it stands rather than the last event that touched
    /// it. The age is computed here for the same reason: an age written at the
    /// moment an agent was created stops growing the moment nothing else
    /// happens, and "nothing else happened" is exactly the state a leaked agent
    /// is in.
    ///
    /// The roster exists beside `role_calibrations` rather than inside it
    /// because they answer different questions about the same role: calibration
    /// is what a role's finished runs needed, this is how many of its objects
    /// are still alive. A leak shows up as the count and the age rising while
    /// the runs behind the calibration stop arriving.
    roster: Mutex<Vec<RosterEntry>>,
    /// Every (role, lifetime class) pair the roster has ever held.
    ///
    /// A gauge has no delete: a role whose last agent goes away simply stops
    /// being written, and the last nonzero value stays in the store reading as
    /// "still N alive". So the scrape writes an explicit zero for every role
    /// it has seen, and needs this set to know which those are.
    ///
    /// Paired with the lifetime class, not just the role: the two classes are
    /// two populations under one name, and the age reading only means anything
    /// within one of them. A role that has held both would otherwise be one
    /// key, so one of the two series would never be written zero and the other
    /// would be born from whatever count the role happened to have.
    roster_roles: Mutex<HashMap<(String, bool), ()>>,
    /// How many objects have been retired, keyed by role and lifetime class.
    ///
    /// The roster count answers "how many are alive"; this answers "is
    /// retirement running at all". A roster that stops growing is also what a
    /// system that stopped creating agents looks like, so the count alone can
    /// never say whether anything was ever let go.
    retired_total: Mutex<HashMap<(String, bool), u64>>,
    /// Retirements whose consumer outlived the budget, keyed by role, lifetime
    /// class and whether the agent was busy when it was asked to leave.
    ///
    /// The two states are kept apart because they call for different repairs: a
    /// busy agent that will not stop means the budget is smaller than a turn, an
    /// idle one means the read loop never looked at the signal.
    retire_timeout_total: Mutex<HashMap<(String, bool, bool), u64>>,
    /// Inbox commands this process finished and acknowledged, keyed by kind.
    ///
    /// The delivery path had no reading at all: a command that ran and a
    /// command that was written to a stream no consumer group reads left the
    /// same evidence behind, which was a log line from the sender and nothing
    /// else. This is the denominator that makes the two below readable as a
    /// rate rather than as isolated events.
    inbox_delivered_total: Mutex<HashMap<&'static str, u64>>,
    /// Inbox commands whose handler failed and that were left pending for
    /// retry, keyed by kind.
    ///
    /// A failed command is not redelivered until this consumer next starts, so
    /// without a counter it is invisible until a restart: the sender saw its
    /// publish succeed, the queue holds the message, and no run will happen.
    inbox_undelivered_total: Mutex<HashMap<&'static str, u64>>,
    /// Inbox payloads that were not a command at all and went to the
    /// dead-letter queue.
    ///
    /// Counted apart from the failures because the repair is different: these
    /// can never succeed on redelivery, which is why they are acked out.
    inbox_poisoned_total: AtomicU64,
    /// Sends the addressing tool refused before writing anything, keyed by the
    /// reason it refused.
    ///
    /// The refusal is the correct behaviour, but a caller that keeps naming
    /// agents nobody holds is a broken producer, and the sender's retry loop
    /// looks the same as a working one from the outside.
    inbox_refused_total: Mutex<HashMap<&'static str, u64>>,
    /// Sends that waited for an answer, keyed by how the wait ended.
    ///
    /// An ask that timed out is not a failed send: the command was published
    /// and the target may still be running it, so no failure counter moves and
    /// no queue depth changes. The caller learns it from the tool result and
    /// nobody else would ever learn it at all, which is what this is for.
    inbox_ask_total: Mutex<HashMap<&'static str, u64>>,
    /// 每一轮跑完时，这件活的声明来自哪一类，按来源计数。
    ///
    /// 与 [`RoleCalibration`] 分开：那个按角色名数结局，这个数的是「这轮待遇
    /// 由什么决定」里属于活自己的那一半。四态全部计数，包括 `Unreadable`——
    /// 「说了但读不出来」单列一格，才看得出有多少活带着一句没人读懂的话在跑。
    decisions_by_declaration: Mutex<HashMap<cog_core::DeclarationState, u64>>,
    /// 每一轮跑完时，轮次上限是哪一个来源给的，按来源计数。
    ///
    /// 与 `decisions_by_declaration` 并列而不是交叉成一张表：交叉之后每一格
    /// 都是一小撮运行，而这两个问题各自有各自的读者——一个问「活自己说了没」，
    /// 一个问「上限是操作者钉的还是实测出来的」。交叉表只会同时答不上来。
    decisions_by_ceiling: Mutex<HashMap<CeilingSource, u64>>,
    /// How many tasks the pool may run at once, how many workers it holds, and
    /// the measured capacity those two are converging on.
    ///
    /// Published apart from the roster on purpose: the roster is how many
    /// workers this process holds, which does not change when the container
    /// tightens, and a pool that kept every worker but let one hold a task read
    /// exactly like a busy one on the roster alone.
    pool_slots: AtomicU64,
    pool_workers: AtomicU64,
    /// How many tasks the container was measured to fit, once it has been.
    /// Absent until a task has run and finished: a zero here would read as a
    /// container nothing fits in.
    pool_capacity: Mutex<Option<usize>>,
    /// Working set over limit, when the container could be read. `None` is
    /// "not measured" and writes no series: a zero here would read as a
    /// container with nothing in it.
    pool_memory_fraction: Mutex<Option<f64>>,
}

/// Every kind of inbox command this build can execute.
///
/// The series are written for all of these on every scrape, not only for the
/// ones that have been seen, so that a zero is a measurement rather than an
/// absence. Kept as a list beside [`inbox_kind`] so a variant added to the wire
/// form without a label here shows up as a compile error at the match below
/// rather than as a command counted under nothing.
const INBOX_KINDS: [&str; 5] = ["prompt", "continue", "abort", "reset", "steering"];

/// The reasons the addressing tool refuses a send before it writes anything.
///
/// Closed for the same reason the kinds are: one of these means the caller
/// named an agent nobody holds, the other means the roster itself could not be
/// read, and they call for different repairs.
const INBOX_REFUSAL_REASONS: [&str; 2] = ["no_live_agent", "roster_unreadable"];

/// How a waited-for answer ended.
///
/// `timeout` says the wait gave up, not that the send failed — the distinction
/// is the reason these are counted apart from deliveries: a target that keeps
/// taking longer than its askers are willing to wait is a capacity question,
/// while a stream that closes under the reader is a broken channel.
///
/// `abandoned` is the fourth: the asker stopped before the wait could return,
/// either because the turn that was waiting was cancelled or because the send
/// failed after the reply stream already existed. It is counted apart from
/// `timeout` because the repair is different — nothing is slow here; the party
/// that was going to read the answer went away, so the answer will be written
/// to a stream nobody is subscribed to. Before this cell existed the reply
/// stream of a cancelled ask was simply left in the backend with no record that
/// it had ever been asked for.
const INBOX_ASK_OUTCOMES: [&str; 4] = ["replied", "timeout", "stream_error", "abandoned"];

/// The label an inbox command is counted under.
///
/// A closed set rather than the wire tag read back out of the payload: the set
/// of commands a build can execute is fixed by this enum, and a label taken
/// from bytes would let a producer choose the cardinality of the series.
pub(crate) fn inbox_kind(msg: &cog_core::InboxMessage) -> &'static str {
    match msg {
        cog_core::InboxMessage::Prompt { .. } => "prompt",
        cog_core::InboxMessage::Continue { .. } => "continue",
        cog_core::InboxMessage::Abort => "abort",
        cog_core::InboxMessage::Reset => "reset",
        cog_core::InboxMessage::Steering { .. } => "steering",
    }
}

/// The label a role's lifetime class is published under.
fn lifetime_label(task_scoped: bool) -> &'static str {
    if task_scoped {
        "task"
    } else {
        "process"
    }
}

/// One live agent as the manager holds it.
#[derive(Clone)]
struct RosterEntry {
    /// The employee this object is, not the label on it.
    ///
    /// Carried so the budget sample can be pruned to the employees that are
    /// still here. Without it the per-employee windows would outlive every
    /// employee that ever ran, and a long-lived process would accumulate one
    /// window per name it has ever seen.
    agent_id: String,
    role: String,
    created_at: std::time::Instant,
    /// Whether this object is scoped to a task or lives as long as the process.
    ///
    /// Carried into the exposition because the age reading is only judgeable
    /// within one of the two: one process-lifetime agent makes the age of the
    /// oldest object climb to the process uptime forever, and every forgotten
    /// task-scoped agent hides behind it.
    task_scoped: bool,
}

/// How many of the most recent completed runs feed the process's budget.
///
/// A window rather than a lifetime archive on purpose: the ceiling is meant to
/// track the workload as it is now, and a run from a month ago under a
/// different model says nothing about how much work a run needs today.
// cap: bound — 窗口跟的是现在的负载；一个月前另一个模型下的一轮跑，说不了今天的活要多少
const BUDGET_SAMPLE_WINDOW: usize = 64;

/// How a role's own finished runs ended.
///
/// The iteration budget used to be one literal for every role, which can only
/// be right by accident: set too low, a run that was still working is cut off
/// mid-exploration and returns nothing (measured: 6 of 8 generator runs stopped
/// at exactly the ceiling with tool calls still pending, against a 25% delivery
/// rate); set too high, every run pays for slack it does not use. How much work
/// a run needs is observable from the runs that complete, which is where the
/// ceiling comes from — but those runs are counted once for the process
/// ([`AgentObservable::delivered_iterations`]) and only their *endings* are
/// split by label here.
#[derive(Default)]
struct RoleCalibration {
    /// Runs of this role that were cut off at the ceiling. A run that stops
    /// this way is direct evidence the ceiling was binding, which is what an
    /// operator needs to see when the derived value is not moving.
    exhausted_runs: u64,
    /// Of those, the ones that ended with no answer at all — the run spent its
    /// whole budget and the forced final ask did not produce one either. Kept
    /// apart from `exhausted_runs` because that series also carries the runs the
    /// forced ask salvaged (`RunOutcome::DeliveredAfterExhaustion`), and the two
    /// endings cost the same and mean different things: one bought nothing, the
    /// other bought an answer out of a budget it had already spent. A reader
    /// told only the total cannot tell a role whose ceiling is too low from one
    /// whose work is simply not answering.
    exhausted_no_deliverable_runs: u64,
    /// Runs of this role that ended because a guardrail rule refused their
    /// calls. Kept per role because the answer an operator needs — which role
    /// keeps meeting a rule, and whether the rule is the one that role works
    /// under — is a question about that role, not about the process.
    refused_runs: u64,
}

/// How an agent run ended.
///
/// A run that spent its whole iteration budget still reading and testing has
/// bought nothing: it hands back a sentinel, not an answer. Counting it as a
/// success hides the ending that costs the most tokens, so the endings are
/// distinguished here and counted apart — including the one that delivers
/// anyway, because "it delivered" and "it delivered out of a budget it had
/// already spent" cost the same and mean different things.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The model stopped calling tools and produced an answer.
    Delivered,
    /// The iteration budget ran out, and the forced ask still produced an
    /// answer. The run handed something back, but how long it *needed* is
    /// unmeasured — it was cut off — so this ending may not feed the
    /// calibration that decides how long this role's runs may be.
    DeliveredAfterExhaustion,
    /// The iteration budget ran out before an answer was written.
    BudgetExhausted,
    /// 这一轮的窗口连自己上一步的结果都装不下：地板（system 与首条 user）
    /// 本身就超预算，于是每条结果进来都在同一拍被裁掉，模型每一轮收到的是
    /// 同一份请求。与 [`Self::BudgetExhausted`] 分开计：那一个说「再给几轮就行」，
    /// 这一个说「给多少轮都一样」——要改的是任务输入的大小或窗口预算。
    Unreadable,
    /// 这一轮把一段时间花在了只读调用上：被提醒过两次「工作树没动」，仍然没有
    /// 写下去。与 [`Self::BudgetExhausted`] 分开计：那一个说轮数不够，这一个说
    /// 轮数够了、动作没有——再给轮数只会换来同一批读。
    NoProgress,
    /// 进程被要求停下，运行在回合边界主动让路。
    ///
    /// 与 [`Self::BudgetExhausted`] 分开计：那一个说「再给几轮就行」，这一个
    /// 说这次运行是被外部叫停的，轮数还有。它不算交付（没有答案），也不该算
    /// 这个角色失败——真正读它的是重投之后那一次续跑。
    ///
    /// A run that stopped between turns because the process is shutting down.
    /// It delivered nothing, but nothing it did was wasted: the conversation is
    /// left open and the next dispatch resumes from it.
    Preempted,
    /// A guardrail rule refused a tool call and the run's refusal allowance was
    /// spent, so the run stopped without an answer.
    ///
    /// Apart from the two budget endings on purpose: those say the role needed
    /// more turns, and the cure is a larger ceiling. This one says a rule stood
    /// in the way, and buying more turns would only buy the same refusal again.
    /// Counted per role because the rule that refuses is usually the role's, and
    /// a total across roles cannot say which one keeps meeting it.
    Refused,
}

impl RunOutcome {
    /// Whether this ending handed an answer back.
    ///
    /// Two readers judge the same endings the same way — the per-role success
    /// counter and whatever scores the skill the role was bound to — so the
    /// rule sits here rather than being restated at each of them. A run cut off
    /// at the ceiling that still answered counts, because it did hand something
    /// back; one that spent the budget without an answer does not.
    pub fn delivered(self) -> bool {
        matches!(self, Self::Delivered | Self::DeliveredAfterExhaustion)
    }
}

/// 这一轮跑的轮次上限是谁给的。
///
/// 操作者在配置面上钉的数、接手这个人自己的交卷读数、这个人还没有自己的读数时
/// 这个进程的交卷读数，三样各成一格；外加一格表示三边都没有。分开不是为了好看：
/// 「量到的是这个人的」与「量到的是这个进程的，而这个人还没有」是两个不同的结论，
/// 而「量不到」是第三个。上限停在某个数上时，读者要能说出它是哪一样给的。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CeilingSource {
    /// 操作者钉在配置面上的数。实测读数只报它绑不绑，不改它。
    Operated,
    /// 没人钉数，接手这个人自己的交卷运行给出的值。
    MeasuredOwn,
    /// 没人钉数，这个人还没有自己的交卷样本；这个进程的交卷运行给出的值。
    MeasuredProcess,
    /// 没人钉数，这个进程也还没有一条交卷样本：量不到，所以这一轮无上限。
    Unmeasured,
}

impl CeilingSource {
    /// 读数标签上的那个词。
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Operated => "operated",
            Self::MeasuredOwn => "measured_own",
            Self::MeasuredProcess => "measured_process",
            Self::Unmeasured => "unmeasured",
        }
    }
}

/// 一轮运行跑完之后的代价，三项都是读数直接要用的量。
///
/// 打成一个值而不是三个位置参数：它们一起进 [`AgentObservable::record_run`]，
/// 一起决定「这一轮值多少」；逐项摆出来时，多一个读数项就多一个参数，而漏传
/// 一项在编译期看不见——结构体字段少一项就会在解构处响。
#[derive(Debug, Clone, Copy)]
pub struct RunCost {
    /// 上限与实测窗口使用的单位：轮数。
    pub iterations: u32,
    /// 助手回合数。
    pub steps: usize,
    /// 工具调用次数。
    pub tool_calls: usize,
}

/// 一轮运行跑完之后要记下的「待遇由谁决定」。
///
/// 与结局、花销一起在 [`AgentObservable::record_run`] 的同一处写，而不是由调用方
/// 顺手另记一笔：那一笔漏掉时，来源计数会安静地小于运行总数，而「小于」在读数面
/// 上看不出是漏记还是这部分运行本来就不存在。传一个值而不是两个可选参数，也让
/// 「这一轮没说」只能是一个明确的 `Unstated`，不能是忘了传。
#[derive(Debug, Clone, Copy)]
pub struct RunDecision {
    /// 这件活的声明来自哪一类。
    pub declaration: cog_core::DeclarationState,
    /// 轮次上限是哪个来源给的。
    pub ceiling: CeilingSource,
}

#[cfg(test)]
impl RunDecision {
    /// 单元测试里不关心待遇时的取值：没有声明，也没有实测样本。
    ///
    /// 只在本 crate 的测试里可用；其它 crate 的测试要自己写出那两个值，
    /// 因为那里的运行通常正是在验证其中一个。
    pub const fn no_declaration() -> Self {
        Self {
            declaration: cog_core::DeclarationState::Unstated,
            ceiling: CeilingSource::Unmeasured,
        }
    }
}

impl AgentObservable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish the pool's concurrency and the reading that decided it.
    ///
    /// `capacity` and `memory_fraction` are `None` when they have not been
    /// measured, and then no series is written at all: the pool holds where it
    /// is in that state, and a zero would say the opposite.
    pub fn publish_pool_sizing(
        &self,
        slots: usize,
        workers: usize,
        capacity: Option<usize>,
        memory_fraction: Option<f64>,
    ) {
        self.pool_slots.store(slots as u64, Ordering::Relaxed);
        self.pool_workers.store(workers as u64, Ordering::Relaxed);
        *self
            .pool_capacity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = capacity;
        *self
            .pool_memory_fraction
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = memory_fraction;
    }

    /// Replace the published roster with `entries`
    /// (agent id, role, creation instant, task scoped).
    ///
    /// Replaced wholesale rather than incremented on create and on retire: a
    /// reader comparing a count that was incremented and decremented on two
    /// separate paths can only ask whether the two balanced, and a path that
    /// failed to decrement looks exactly like a path that never ran. Publishing
    /// the list makes the manager the single owner of the truth and the scrape
    /// a plain view of it.
    ///
    /// The same publish is what bounds the per-employee budget windows: the
    /// roster is the set of employees that are still here, so a window for an id
    /// the roster no longer names is a window nothing can ever read again.
    pub fn publish_roster(
        &self,
        entries: impl IntoIterator<Item = (String, String, std::time::Instant, bool)>,
    ) {
        let next: Vec<RosterEntry> = entries
            .into_iter()
            .map(|(agent_id, role, created_at, task_scoped)| RosterEntry {
                agent_id,
                role,
                created_at,
                task_scoped,
            })
            .collect();
        {
            let mut roles = self
                .roster_roles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for entry in &next {
                roles.insert((entry.role.clone(), entry.task_scoped), ());
            }
        }
        {
            let live: std::collections::HashSet<&str> =
                next.iter().map(|entry| entry.agent_id.as_str()).collect();
            let mut samples = self
                .delivered_iterations_by_agent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            samples.retain(|agent, _| live.contains(agent.as_str()));
        }
        *self
            .roster
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
    }

    /// Record one retirement, and whether its consumer ended on time.
    ///
    /// A repeated call on an already-retired object reports `performed: false`
    /// and is not counted: a task that reaches its terminal state twice would
    /// otherwise double its own retirement in the series, and the number would
    /// stop being "how many objects were let go".
    pub fn record_retirement(
        &self,
        role: &str,
        task_scoped: bool,
        report: crate::agent::RetireReport,
    ) {
        if !report.performed {
            return;
        }
        {
            let mut retired = self
                .retired_total
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *retired.entry((role.to_string(), task_scoped)).or_insert(0) += 1;
        }
        if !report.consumer_ended {
            let mut timeouts = self
                .retire_timeout_total
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *timeouts
                .entry((role.to_string(), task_scoped, report.was_busy))
                .or_insert(0) += 1;
        }
    }

    /// Record one inbox command this process ran and acknowledged.
    pub fn record_inbox_delivered(&self, kind: &'static str) {
        let mut counts = self
            .inbox_delivered_total
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *counts.entry(kind).or_insert(0) += 1;
    }

    /// Record one inbox command whose handler failed, left pending for retry.
    pub fn record_inbox_undelivered(&self, kind: &'static str) {
        let mut counts = self
            .inbox_undelivered_total
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *counts.entry(kind).or_insert(0) += 1;
    }

    /// Record one inbox payload that was not a command and went to the DLQ.
    pub fn record_inbox_poisoned(&self) {
        self.inbox_poisoned_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a send the addressing tool refused, by the reason it gave.
    pub fn record_inbox_refused(&self, reason: &'static str) {
        let mut counts = self
            .inbox_refused_total
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *counts.entry(reason).or_insert(0) += 1;
    }

    /// Record one send that waited for an answer, by how the wait ended.
    pub fn record_inbox_ask(&self, outcome: &'static str) {
        let mut counts = self
            .inbox_ask_total
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *counts.entry(outcome).or_insert(0) += 1;
    }

    /// Record a high-level agent run (called by AgentRuntime::run).
    ///
    /// `agent` is the employee that ran; the window kept under its id is what the
    /// next run of the same employee starts its ceiling from. `role` labels the
    /// ending counts only: a role is a free label, and a run counted under a
    /// different name must not land in a different window from the same run
    /// counted under this one. `cost.iterations` is in the same unit the ceiling
    /// is expressed in — not `steps`, which counts assistant turns and would
    /// silently rescale every reading it fed.
    pub fn record_run(
        &self,
        agent: &str,
        role: &str,
        outcome: RunOutcome,
        cost: RunCost,
        decision: RunDecision,
    ) {
        self.run_count.fetch_add(1, Ordering::Relaxed);
        // 「这轮待遇由谁决定」在同一处记账：漏记时来源计数会小于运行总数，
        // 而分开记的两个调用点迟早会有一个被绕过。
        *self
            .decisions_by_declaration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(decision.declaration)
            .or_insert(0) += 1;
        *self
            .decisions_by_ceiling
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(decision.ceiling)
            .or_insert(0) += 1;
        if outcome.delivered() {
            self.success_count.fetch_add(1, Ordering::Relaxed);
        }
        self.total_steps
            .fetch_add(cost.steps as u64, Ordering::Relaxed);
        self.total_tool_calls
            .fetch_add(cost.tool_calls as u64, Ordering::Relaxed);

        if outcome == RunOutcome::Delivered {
            {
                let mut sample = self
                    .delivered_iterations
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                push_bounded(&mut sample, cost.iterations);
            }
            // The employee's own window is the one its next ceiling reads first.
            // Kept per id (not per role) so a different person taking the same
            // work starts from their own record, not from the previous holder's.
            let mut windows = self
                .delivered_iterations_by_agent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            push_bounded(
                windows.entry(agent.to_string()).or_default(),
                cost.iterations,
            );
        }

        let mut calibrations = self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let calibration = calibrations.entry(role.to_string()).or_default();
        // Counted here, at the run's end, and not at the moment the ceiling is
        // judged spent (`note_budget_exhausted`): only the ending says whether
        // the forced ask produced an answer. The judgement-time counter cannot
        // tell the two apart, which is exactly the gap this one fills.
        if outcome == RunOutcome::BudgetExhausted {
            calibration.exhausted_no_deliverable_runs += 1;
        }
        if outcome == RunOutcome::Refused {
            calibration.refused_runs += 1;
        }
    }

    /// Note that a run's iteration budget was spent, at the moment the budget is
    /// judged spent rather than at the end of the run.
    ///
    /// What a run cost and what it finally handed back are two facts about two
    /// moments. Recorded only at the end, a run that spent everything and was
    /// then salvaged by the forced ask would read exactly like one that never
    /// came near the ceiling: the ending that costs the most tokens, reported as
    /// the healthy one. Recorded here, it is counted whatever ending the run
    /// lands on — and counted even if it never lands on one.
    pub fn note_budget_exhausted(&self, role: &str) {
        self.budget_exhausted_count.fetch_add(1, Ordering::Relaxed);
        let mut calibrations = self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        calibrations
            .entry(role.to_string())
            .or_default()
            .exhausted_runs += 1;
    }

    /// The iteration ceiling to run this piece of work under, given the value a
    /// caller would use with no evidence behind it (`seed`: the operator's
    /// config).
    ///
    /// There are exactly two sources for a ceiling — the operator's number and
    /// this process's own measurements — and neither is keyed by a label. A role
    /// is a free tag on the roster; keying the ceiling by it meant the same work
    /// handed to a worker under another name ran under another ceiling, which is
    /// the label deciding behaviour.
    ///
    /// Only one of the two can answer at a time. A stated seed stands — it is the
    /// operator's own decision for this deployment, and moving a number without
    /// evidence would be guesswork with extra steps. Only where nobody stated
    /// one (`u32::MAX`) does the measurement answer, and the ceiling it gives is
    /// the shape the work usually takes plus one more typical run's worth of
    /// room:
    ///
    /// - the floor of experience is the ninetieth percentile of what has been
    ///   delivered, not the longest of it. A ceiling that will not admit the
    ///   shape this work is known to need is a defect, and ninety in a hundred
    ///   delivered runs is that shape. Taking the maximum makes a single outlier
    ///   the ceiling for every run until it ages out of the sample window, which
    ///   is the opposite of following the workload: measured on 2026-11-06 over
    ///   215 delivered generator runs in 24 hours, the median was 5 iterations,
    ///   the ninetieth percentile 32, the maximum 169. The 9.8% of runs above 32
    ///   carried 24.6% of every generator call, and the runs that spent a whole
    ///   3600s task budget without producing an artifact sat at 63 — above the
    ///   percentile, far below the maximum. A ceiling at the maximum was ~183 and
    ///   bound nothing; at the percentile it is ~46 and binds exactly on the tail
    ///   where the spend stops buying deliveries.
    /// - the headroom is what keeps the ceiling from being a claim about the
    ///   past. A ceiling set exactly to the percentile cuts off the first run
    ///   that is slightly harder than anything seen, and that cut is total: the
    ///   run returns nothing and has already spent everything. One typical work
    ///   unit (the mean) of slack absorbs that variance, and a run that is cut
    ///   off still gets the forced final ask rather than nothing at all.
    ///
    /// Both terms are observed, so the ceiling follows the workload instead of
    /// needing to be retuned by hand whenever the workload changes.
    ///
    /// An unstated ceiling (`u32::MAX`, "no ceiling stated here") is not a
    /// decision to be unbounded — it is the caller having no number. Leaving it
    /// as the answer silences the calibration in exactly the deployment that
    /// needs it most: the one that never states a ceiling. Measured on
    /// 2026-11-06, where nothing states one: a single generator run on one
    /// change reached 149 turns, and 98 runs of a million tokens or more were
    /// 60% of the agent bill, while every change that has ever completed did so
    /// on its first ralph iteration (60 of 60 over 14 days). The spend was
    /// going to retries that never turned into deliveries, and the only number
    /// anyone had measured was never consulted.
    pub fn iteration_budget_for(&self, seed: u32, agent: &str) -> u32 {
        self.iteration_budget_decision_for(seed, agent).0
    }

    /// 同上，并交出「这个数是谁给的」。
    ///
    /// 与 [`Self::iteration_budget_for`] 是同一次判断，不是两次：分两处各算一遍，
    /// 读数上标的来源与实际用的上限迟早会各自演化，而这一对读数存在的意义正是
    /// 让读者能指出「这一轮的上限是谁给的」。
    ///
    /// 三个来源按顺序问：操作者钉的数（`operated`），接手这个人自己的交卷运行
    /// （`measured_own`），这才进场的人还没有自己的样本时退回这个进程的
    /// （`measured_process`）。退到进程样本这一步不改变「待遇跟着人走」：自己的
    /// 样本一出现，下一轮就换成它。
    ///
    /// `Unmeasured` 不是第四个来源，是三个人都没有：没人钉数，这个进程也还没有
    /// 一条交卷样本。此时返回的上限就是那个「没人说」的记号，这一轮无上限——读数上
    /// 必须能与「实测给出 u32::MAX」分开，后者不可能发生，但前者必须看得见。
    pub fn iteration_budget_decision_for(&self, seed: u32, agent: &str) -> (u32, CeilingSource) {
        // Any value other than `u32::MAX` is the operator's own decision for this
        // deployment and stands as written. It used to be raised whenever the
        // measurement was larger, back when the seed could also arrive from a
        // role's skill row — a number written for a label, which the work was
        // allowed to outgrow. The only seed left is the process-wide config: a
        // statement about this deployment, which a measurement may report on (the
        // exhausted-runs reading says when it is binding) but not silently
        // overrule.
        if seed != u32::MAX {
            return (seed, CeilingSource::Operated);
        }
        let own = {
            let windows = self
                .delivered_iterations_by_agent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            windows.get(agent).cloned()
        };
        if let Some(own) = own.filter(|window| !window.is_empty()) {
            let sample: Vec<u32> = own.into_iter().collect();
            return (calibrated_ceiling(sample), CeilingSource::MeasuredOwn);
        }
        let sample = self
            .delivered_iterations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if sample.is_empty() {
            return (seed, CeilingSource::Unmeasured);
        }
        let sample: Vec<u32> = sample.iter().copied().collect();
        (calibrated_ceiling(sample), CeilingSource::MeasuredProcess)
    }
}

/// Push `value` into `window`, dropping the oldest entry once it is full.
fn push_bounded(window: &mut VecDeque<u32>, value: u32) {
    if window.len() == BUDGET_SAMPLE_WINDOW {
        window.pop_front();
    }
    window.push_back(value);
}

/// The ceiling a sample of delivered runs supports: the ninetieth percentile
/// (how long this work usually takes) plus one typical run (the mean, room for
/// work slightly harder than anything seen). Both terms move with the sample.
fn calibrated_ceiling(mut sample: Vec<u32>) -> u32 {
    let admitted = ninetieth_percentile(&mut sample);
    let sum: u32 = sample.iter().sum();
    let typical = sum.div_ceil(sample.len() as u32);
    admitted.saturating_add(typical)
}

/// The ninetieth percentile of a sample of delivered run lengths, nearest rank.
///
/// Nearest rank rather than an interpolated one: the answer is the length of a
/// run that actually happened, so it can be compared against an `iterations`
/// reading without inventing a run nobody ran. With ten samples or fewer this is
/// the maximum — that few runs cannot tell a tail from the shape of the work —
/// so it only departs from the maximum once the window holds enough runs for the
/// two to mean different things.
fn ninetieth_percentile(sample: &mut [u32]) -> u32 {
    sample.sort_unstable();
    let rank = (9 * sample.len()).div_ceil(10).max(1);
    sample[rank - 1]
}

#[async_trait]
impl Observable for AgentObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();

        // High-level counters (available in all dimensions)
        let runs = self.run_count.load(Ordering::Relaxed);
        metrics.push(RawMetric::new("agent_run_count", runs as f64));
        metrics.push(RawMetric::new(
            "agent_success_count",
            self.success_count.load(Ordering::Relaxed) as f64,
        ));
        metrics.push(RawMetric::new(
            "agent_budget_exhausted_count",
            self.budget_exhausted_count.load(Ordering::Relaxed) as f64,
        ));
        metrics.push(RawMetric::new(
            "agent_total_steps",
            self.total_steps.load(Ordering::Relaxed) as f64,
        ));
        metrics.push(RawMetric::new(
            "agent_total_tool_calls",
            self.total_tool_calls.load(Ordering::Relaxed) as f64,
        ));

        // 每一轮跑完都记下待遇的两半由谁决定：活自己声明的是哪一态，轮次上限由
        // 操作者钉的数还是这个进程的实测样本给出。运行总数是分母——这两族逐个
        // 来源加起来应当等于它，少了就是有运行没记下自己的决定。
        //
        // 写「见过的那些格」而不是每次都写全零：一个从没出现过的来源在这一刻
        // 是零，但 scrape 之间它可能刚发生又刚翻页，写零会把「发生过」抹成
        // 「没发生」。没写过的那一格读作「这进程里没发生过」，这正是对的。
        for (state, count) in self
            .decisions_by_declaration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            metrics.push(
                RawMetric::new("agent_runs_by_declaration", *count as f64)
                    .with_label(crate::LABEL_DECLARATION, state.as_str()),
            );
        }
        for (source, count) in self
            .decisions_by_ceiling
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            metrics.push(
                RawMetric::new("agent_runs_by_ceiling", *count as f64)
                    .with_label(crate::LABEL_CEILING_SOURCE, source.as_str()),
            );
        }

        // The iteration ceiling is decided per role, so the reading of it has
        // to be per role too: a single process-wide number here could not tell
        // an operator which role is being cut off, which is the one question
        // this pair of series exists to answer.
        for (role, calibration) in self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            metrics.push(
                RawMetric::new(
                    "agent_iteration_budget_exhausted",
                    calibration.exhausted_runs as f64,
                )
                .with_label(crate::LABEL_ROLE, role),
            );
            metrics.push(
                RawMetric::new(
                    "agent_iteration_budget_exhausted_no_deliverable",
                    calibration.exhausted_no_deliverable_runs as f64,
                )
                .with_label(crate::LABEL_ROLE, role),
            );
            // A sibling series, not a label on either of the two above: those
            // count runs that needed more turns, this one counts runs a rule
            // stopped, and raising a ceiling answers only the first. Reported
            // per role because that is the question an operator asks of it.
            metrics.push(
                RawMetric::new(
                    "agent_guardrail_refused_runs",
                    calibration.refused_runs as f64,
                )
                .with_label(crate::LABEL_ROLE, role),
            );
        }

        // The manager's roster, published on every change and read here. Two
        // series per role because either one alone can be read as healthy: a
        // count that stopped falling says nothing about how long the oldest
        // object has been there, and one agent created at boot looks exactly
        // like a busy role on the count alone.
        {
            let roster = self
                .roster
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Counted per (role, lifetime), not per role. A role can hold both
            // a process-lifetime object and task-scoped ones at the same time,
            // and a count that adds them together answers neither question: the
            // task-scoped series would be inflated by the one object that never
            // dies, so it could sit above its own ceiling forever without any
            // leak, and the process series would move with the workload.
            let mut by_lifetime: HashMap<(&str, bool), (u64, f64)> = HashMap::new();
            for entry in roster.iter() {
                let age = entry.created_at.elapsed().as_secs_f64();
                let slot = by_lifetime
                    .entry((entry.role.as_str(), entry.task_scoped))
                    .or_insert((0, 0.0));
                slot.0 += 1;
                if age > slot.1 {
                    slot.1 = age;
                }
            }
            let roles = self
                .roster_roles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for ((role, task_scoped), ()) in roles.iter() {
                let lifetime = lifetime_label(*task_scoped);
                let (count, oldest_age) = by_lifetime
                    .get(&(role.as_str(), *task_scoped))
                    .copied()
                    .unwrap_or((0, f64::NAN));
                metrics.push(
                    RawMetric::new("agent_roster", count as f64)
                        .with_label(crate::LABEL_ROLE, role)
                        .with_label(crate::LABEL_LIFETIME, lifetime),
                );
                // Only while the role still holds something. An age with no
                // object behind it would be a number left over from the last
                // scrape, and a reader who takes it as the age of the oldest
                // live agent reads a leak where there is none. The count is the
                // gate: zero there means the age of this role is not a reading.
                if count > 0 {
                    metrics.push(
                        RawMetric::new("agent_roster_oldest_age_seconds", oldest_age)
                            .with_label(crate::LABEL_ROLE, role)
                            .with_label(crate::LABEL_LIFETIME, lifetime),
                    );
                }
            }
        }

        // Retirements, by role and lifetime class. A counter rather than a
        // gauge: the roster above already says how many are alive, and a roster
        // that merely stops growing is also what a system that stopped creating
        // agents looks like. Only this counter tells the two apart.
        {
            let retired = self
                .retired_total
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for ((role, task_scoped), count) in retired.iter() {
                metrics.push(
                    RawMetric::new("agent_retired_total", *count as f64)
                        .with_label(crate::LABEL_ROLE, role)
                        .with_label(crate::LABEL_LIFETIME, lifetime_label(*task_scoped)),
                );
            }
        }
        {
            let timeouts = self
                .retire_timeout_total
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for ((role, task_scoped, was_busy), count) in timeouts.iter() {
                metrics.push(
                    RawMetric::new("agent_retire_timeout_total", *count as f64)
                        .with_label(crate::LABEL_ROLE, role)
                        .with_label(crate::LABEL_LIFETIME, lifetime_label(*task_scoped))
                        .with_label(crate::LABEL_STATE, if *was_busy { "busy" } else { "idle" }),
                );
            }
        }

        // Inter-agent delivery. Printed as a matching pair of counters over the
        // same label set, because either alone can be read as healthy: a
        // process that delivered nothing because nothing was sent and one that
        // delivered nothing because every command failed both leave no
        // `undelivered` series behind, and only the delivered counter beside it
        // says which one happened.
        {
            let delivered = self
                .inbox_delivered_total
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let undelivered = self
                .inbox_undelivered_total
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Every kind is written, including the ones this process has never
            // seen. A counter left out until its first event makes "no command
            // of this kind has failed" and "this build cannot name this kind"
            // the same absent series, and an alert written over the pair needs
            // the denominator to exist before the first command arrives.
            for kind in INBOX_KINDS {
                metrics.push(
                    RawMetric::new(
                        "agent_inbox_delivered_total",
                        delivered.get(kind).copied().unwrap_or(0) as f64,
                    )
                    .with_label(crate::LABEL_KIND, kind),
                );
                metrics.push(
                    RawMetric::new(
                        "agent_inbox_undelivered_total",
                        undelivered.get(kind).copied().unwrap_or(0) as f64,
                    )
                    .with_label(crate::LABEL_KIND, kind),
                );
            }
        }
        metrics.push(RawMetric::new(
            "agent_inbox_poisoned_total",
            self.inbox_poisoned_total.load(Ordering::Relaxed) as f64,
        ));
        {
            let refused = self
                .inbox_refused_total
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for reason in INBOX_REFUSAL_REASONS {
                metrics.push(
                    RawMetric::new(
                        "agent_inbox_refused_total",
                        refused.get(reason).copied().unwrap_or(0) as f64,
                    )
                    .with_label(crate::LABEL_REASON, reason),
                );
            }
        }
        {
            let asks = self
                .inbox_ask_total
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for outcome in INBOX_ASK_OUTCOMES {
                metrics.push(
                    RawMetric::new(
                        "agent_inbox_ask_total",
                        asks.get(outcome).copied().unwrap_or(0) as f64,
                    )
                    .with_label(crate::LABEL_OUTCOME, outcome),
                );
            }
        }

        // The pool's concurrency and the reading behind it. Written only when
        // a pool exists: a process with the pool switched off has no count to
        // report, and a zero would read as a pool that shrank to nothing.
        let slots = self.pool_slots.load(Ordering::Relaxed);
        if slots > 0 {
            metrics.push(RawMetric::new("agent_pool_slots", slots as f64));
            metrics.push(RawMetric::new(
                "agent_pool_workers",
                self.pool_workers.load(Ordering::Relaxed) as f64,
            ));
            if let Some(capacity) = *self
                .pool_capacity
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
            {
                metrics.push(RawMetric::new("agent_pool_capacity", capacity as f64));
            }
            if let Some(fraction) = *self
                .pool_memory_fraction
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
            {
                metrics.push(RawMetric::new("agent_pool_memory_fraction", fraction));
            }
        }

        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        // No per-step records are kept. The series this used to serve were keyed
        // by task id, so their cardinality grew with every task, and nothing read
        // them: the scrape never asks for them and no rule or panel named them.
        Ok(Vec::new())
    }

    /// 空集：这个 observable 的读数不随维度变。它只报累计计数器，采集侧因此
    /// "没有可采维度时采一次"，而不是按维度重复问同一份读数回来。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一律按「活自己声明了什么、上限谁给的」记账：三样决定者的第一样与第三样
    /// 每轮都要在读数里留下自己那一格。
    ///
    /// 漏记是这条读数的坏法：某一格变小，在读数面上与「这种运行本来就更少」
    /// 长得一样。所以两个方向都判——每个来源的格子数对得上，且加起来等于运行
    /// 总数，没有哪一轮漏报了自己的决定。
    #[tokio::test]
    async fn every_run_names_the_declaration_and_the_ceiling_that_decided_it() {
        let o = AgentObservable::new();
        let decided = |declaration, ceiling| RunDecision {
            declaration,
            ceiling,
        };
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::Delivered,
            RunCost {
                iterations: 4,
                steps: 9,
                tool_calls: 3,
            },
            decided(
                cog_core::DeclarationState::Stated,
                CeilingSource::MeasuredOwn,
            ),
        );
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::Delivered,
            RunCost {
                iterations: 4,
                steps: 9,
                tool_calls: 3,
            },
            decided(
                cog_core::DeclarationState::Stated,
                CeilingSource::MeasuredOwn,
            ),
        );
        o.record_run(
            "employee-1",
            "evaluator",
            RunOutcome::BudgetExhausted,
            RunCost {
                iterations: 10,
                steps: 12,
                tool_calls: 2,
            },
            decided(
                cog_core::DeclarationState::Unstated,
                CeilingSource::Unmeasured,
            ),
        );

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let by_label = |name: &str, key: &str| -> HashMap<String, f64> {
            metrics
                .iter()
                .filter(|m| m.name == name)
                .map(|m| (m.labels.get(key).cloned().unwrap_or_default(), m.value))
                .collect()
        };

        let declarations = by_label("agent_runs_by_declaration", crate::LABEL_DECLARATION);
        assert_eq!(declarations.get("stated"), Some(&2.0), "{declarations:?}");
        assert_eq!(declarations.get("unstated"), Some(&1.0), "{declarations:?}");

        let ceilings = by_label("agent_runs_by_ceiling", crate::LABEL_CEILING_SOURCE);
        assert_eq!(ceilings.get("measured_own"), Some(&2.0), "{ceilings:?}");
        assert_eq!(ceilings.get("unmeasured"), Some(&1.0), "{ceilings:?}");

        let runs = metrics
            .iter()
            .find(|m| m.name == "agent_run_count")
            .map(|m| m.value)
            .expect("agent_run_count");
        let declared: f64 = declarations.values().sum();
        let decided_by: f64 = ceilings.values().sum();
        assert_eq!(
            (declared, decided_by),
            (runs, runs),
            "有运行没记下自己的决定：运行 {runs}，按声明记下 {declared}，按上限来源记下 {decided_by}"
        );
    }

    /// 上限的来源与上限本身是同一次判定：没人钉数、也没有交卷样本时，上限是
    /// 「没有」，读数是 `unmeasured`，不是「实测给出一个很大的数」。
    #[test]
    fn an_empty_sample_with_no_stated_ceiling_reads_as_unmeasured() {
        let o = AgentObservable::new();
        assert_eq!(
            o.iteration_budget_decision_for(u32::MAX, "employee-1"),
            (u32::MAX, CeilingSource::Unmeasured)
        );
        assert_eq!(
            o.iteration_budget_decision_for(25, "employee-1"),
            (25, CeilingSource::Operated)
        );

        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::Delivered,
            RunCost {
                iterations: 6,
                steps: 9,
                tool_calls: 3,
            },
            RunDecision {
                declaration: cog_core::DeclarationState::Unstated,
                ceiling: CeilingSource::Unmeasured,
            },
        );
        let (value, source) = o.iteration_budget_decision_for(u32::MAX, "employee-1");
        assert_eq!(source, CeilingSource::MeasuredOwn);
        assert!(value > 6, "实测给出的上限要容得下交过的那一轮：{value}");

        // 同一个进程里另一个还没有自己样本的员工，读到的是进程样本，来源分得开。
        let (inherited, inherited_source) = o.iteration_budget_decision_for(u32::MAX, "employee-2");
        assert_eq!(inherited_source, CeilingSource::MeasuredProcess);
        assert_eq!(inherited, value, "自己的样本还没有时，先用这个进程的读数");

        // 钉了数就还是钉的数，有样本也一样。
        assert_eq!(
            o.iteration_budget_decision_for(25, "employee-1"),
            (25, CeilingSource::Operated)
        );
    }

    /// The collector asks an observable that declares no dimensions once, with
    /// an empty name; that is the only call this observable ever answers.
    async fn readings(o: &AgentObservable) -> (f64, f64, f64) {
        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let read = |name: &str| {
            metrics
                .iter()
                .find(|m| m.name == name)
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        (
            read("agent_run_count"),
            read("agent_success_count"),
            read("agent_budget_exhausted_count"),
        )
    }

    /// A role whose last agent was removed must still be written, as zero.
    ///
    /// A gauge has no delete. If the empty role is simply left out of the
    /// scrape, the store keeps the last nonzero value and every reader of it
    /// sees a role that still holds agents, forever.
    #[tokio::test]
    async fn an_emptied_role_is_written_as_zero_rather_than_left_out() {
        let o = AgentObservable::new();
        let now = std::time::Instant::now();
        o.publish_roster(vec![(
            "planner-1".to_string(),
            "planner".to_string(),
            now,
            true,
        )]);
        o.publish_roster(Vec::<(String, String, std::time::Instant, bool)>::new());

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let count = metrics
            .iter()
            .find(|m| {
                m.name == "agent_roster"
                    && m.labels.get(crate::LABEL_ROLE).map(String::as_str) == Some("planner")
            })
            .expect("the role is still reported after it emptied");
        assert_eq!(count.value, 0.0);
        assert_eq!(
            count.labels.get(crate::LABEL_LIFETIME).map(String::as_str),
            Some("task")
        );
        assert!(
            !metrics
                .iter()
                .any(|m| m.name == "agent_roster_oldest_age_seconds"),
            "an age with nothing behind it would be the previous scrape's number"
        );
    }

    /// The process-lifetime agent must be separable, or the age reading cannot
    /// be judged: it alone would hold the oldest age at the process uptime and
    /// hide every task agent that was never retired.
    #[tokio::test]
    async fn a_process_lifetime_agent_is_labelled_apart_from_task_agents() {
        let o = AgentObservable::new();
        let now = std::time::Instant::now();
        o.publish_roster(vec![
            ("planner-1".to_string(), "planner".to_string(), now, true),
            (
                "mode-selector".to_string(),
                "mode_selector".to_string(),
                now,
                false,
            ),
        ]);

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let lifetime_of = |role: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "agent_roster"
                        && m.labels.get(crate::LABEL_ROLE).map(String::as_str) == Some(role)
                })
                .and_then(|m| m.labels.get(crate::LABEL_LIFETIME).map(String::as_str))
        };
        assert_eq!(lifetime_of("planner"), Some("task"));
        assert_eq!(lifetime_of("mode_selector"), Some("process"));
    }

    /// One role can hold both lifetimes at once, and they are two populations.
    ///
    /// A series that adds them together answers neither question: the task
    /// series would be inflated by the one object that never dies, so it could
    /// sit above its own ceiling with no leak behind it, and the process series
    /// would move with the workload. The age has the same problem one step
    /// further — merging makes the process-lifetime object's age the age of
    /// every task agent under that role, which is the reading that lets a leak
    /// hide.
    ///
    /// Judged as two rows, each carrying its own population's count and oldest
    /// age: a role-only key produces one row with the sum, so both of these
    /// assertions are exactly what it cannot produce.
    #[tokio::test]
    async fn a_role_holding_both_lifetimes_reports_each_population_on_its_own() {
        let o = AgentObservable::new();
        let now = std::time::Instant::now();
        let long_lived = now - std::time::Duration::from_secs(30);
        o.publish_roster(vec![
            ("planner-1".to_string(), "planner".to_string(), now, true),
            ("planner-2".to_string(), "planner".to_string(), now, true),
            (
                "planner-3".to_string(),
                "planner".to_string(),
                long_lived,
                false,
            ),
        ]);

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let rows: Vec<_> = metrics
            .iter()
            .filter(|m| {
                m.name == "agent_roster"
                    && m.labels.get(crate::LABEL_ROLE).map(String::as_str) == Some("planner")
            })
            .collect();
        assert_eq!(
            rows.len(),
            2,
            "both lifetimes of one role need a row of their own, not one merged row"
        );
        let count = |lifetime: &str| {
            rows.iter()
                .find(|m| m.labels.get(crate::LABEL_LIFETIME).map(String::as_str) == Some(lifetime))
                .map(|m| m.value)
        };
        assert_eq!(
            count("task"),
            Some(2.0),
            "the two task agents, without the permanent one"
        );
        assert_eq!(
            count("process"),
            Some(1.0),
            "the permanent one, without the task agents"
        );

        let age = |lifetime: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "agent_roster_oldest_age_seconds"
                        && m.labels.get(crate::LABEL_ROLE).map(String::as_str) == Some("planner")
                        && m.labels.get(crate::LABEL_LIFETIME).map(String::as_str) == Some(lifetime)
                })
                .map(|m| m.value)
        };
        let process_age = age("process").expect("the permanent object has an age");
        let task_age = age("task").expect("the task agents have an age");
        assert!(
            process_age >= 29.0,
            "the permanent object is the old one, measured {process_age}"
        );
        assert!(
            task_age < 29.0,
            "the task agents must not inherit the permanent object's age, measured {task_age}"
        );
    }

    /// The counters carry the same two populations the roster does.
    ///
    /// "One object of this lifetime was let go" and "one of the other was" are
    /// different facts — one is the long-lived member being replaced, the other
    /// is a task's agent being released — and a counter keyed by role alone
    /// reports them as one number. The same goes for a consumer that overran
    /// its budget: which lifetime it belonged to decides whether the observer
    /// is looking at a turn that outlasted the budget or a permanent member
    /// that went down.
    #[tokio::test]
    async fn retirement_counters_keep_the_two_lifetimes_apart() {
        let o = AgentObservable::new();
        let retired = |consumer_ended, was_busy| crate::agent::RetireReport {
            performed: true,
            consumer_ended,
            was_busy,
        };
        o.record_retirement("planner", true, retired(true, false));
        o.record_retirement("planner", false, retired(false, true));

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let read = |name: &str, role: &str, lifetime: &str, state: Option<&str>| {
            metrics
                .iter()
                .find(|m| {
                    m.name == name
                        && m.labels.get(crate::LABEL_ROLE).map(String::as_str) == Some(role)
                        && m.labels.get(crate::LABEL_LIFETIME).map(String::as_str) == Some(lifetime)
                        && state.map(|s| {
                            m.labels.get(crate::LABEL_STATE).map(String::as_str) == Some(s)
                        }) != Some(false)
                })
                .map(|m| m.value)
        };
        assert_eq!(
            read("agent_retired_total", "planner", "task", None),
            Some(1.0)
        );
        assert_eq!(
            read("agent_retired_total", "planner", "process", None),
            Some(1.0)
        );
        assert_eq!(
            read(
                "agent_retire_timeout_total",
                "planner",
                "process",
                Some("busy")
            ),
            Some(1.0),
            "the permanent member that overran is one timeout, on the process row"
        );
        assert_eq!(
            read(
                "agent_retire_timeout_total",
                "planner",
                "task",
                Some("busy")
            ),
            None,
            "the task agent ended on time and must not appear as a timeout"
        );
    }

    #[tokio::test]
    async fn a_run_that_spent_its_budget_is_counted_as_an_exhaustion_not_a_success() {
        let o = AgentObservable::new();
        o.note_budget_exhausted("generator");
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::BudgetExhausted,
            RunCost {
                iterations: 10,
                steps: 21,
                tool_calls: 12,
            },
            RunDecision::no_declaration(),
        );

        let (runs, successes, exhausted) = readings(&o).await;
        assert_eq!(runs, 1.0);
        assert_eq!(
            successes, 0.0,
            "a run that wrote no answer must not be counted as a success"
        );
        assert_eq!(exhausted, 1.0);
    }

    /// 抢救回来的那一轮照样把预算花光了。读数必须两件都说：它是一次交付，
    /// 也是一次烧空。只在终局记一次的话，这种结尾会读成完全健康的一轮——
    /// 而它恰恰是最贵的那种。
    #[tokio::test]
    async fn a_run_salvaged_after_spending_its_budget_reads_as_both() {
        let o = AgentObservable::new();
        o.note_budget_exhausted("generator");
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            RunCost {
                iterations: 11,
                steps: 21,
                tool_calls: 12,
            },
            RunDecision::no_declaration(),
        );

        let (runs, successes, exhausted) = readings(&o).await;
        assert_eq!(runs, 1.0);
        assert_eq!(successes, 1.0, "它确实把东西交出去了");
        assert_eq!(exhausted, 1.0, "而它确实把预算花光了才交出去");
    }

    /// 被截断的运行不能反过来给上限当证据：它停在上限指定的地方，量到的是
    /// 上限本身，不是工作量。拿它抬墙就是「越抢救、墙越高、越容易再撞」。
    #[test]
    fn a_salvaged_run_does_not_raise_the_ceiling_it_stopped_at() {
        let o = AgentObservable::new();
        o.note_budget_exhausted("generator");
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            RunCost {
                iterations: 11,
                steps: 21,
                tool_calls: 12,
            },
            RunDecision::no_declaration(),
        );
        assert_eq!(
            o.iteration_budget_for(10, "employee-1"),
            10,
            "在上限处被截断的运行，不能是「上限太低」的证据"
        );
    }

    #[tokio::test]
    async fn a_run_that_delivered_an_answer_is_counted_as_a_success() {
        let o = AgentObservable::new();
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::Delivered,
            RunCost {
                iterations: 3,
                steps: 7,
                tool_calls: 2,
            },
            RunDecision::no_declaration(),
        );

        let (runs, successes, exhausted) = readings(&o).await;
        assert_eq!(runs, 1.0);
        assert_eq!(successes, 1.0);
        assert_eq!(exhausted, 0.0);
    }

    /// 没有任何观测就该原样用调用方给的种子：凭一个没看见过的事实改数字，
    /// 只是把猜数换了个地方猜。
    #[test]
    fn a_pool_with_no_completed_run_keeps_the_seed_budget() {
        let o = AgentObservable::new();
        assert_eq!(o.iteration_budget_for(10, "employee-1"), 10);
        // 只有被截断的运行也不是"跑完过"的证据：它没交卷，也就没说需要多少轮。
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::BudgetExhausted,
            RunCost {
                iterations: 10,
                steps: 21,
                tool_calls: 12,
            },
            RunDecision::no_declaration(),
        );
        assert_eq!(o.iteration_budget_for(10, "employee-1"), 10);
    }

    /// 调用方没写上限时（`u32::MAX` 就是"这里没写天花板"的拼法），标定不能被它
    /// 顶掉：那一刻唯一测到过的数就是这一份标定，拿一个"没人写过"的数去比大小，
    /// 等于把标定整个关掉——而那正好发生在一个从没写过上限的部署里。
    #[test]
    fn an_unstated_ceiling_takes_the_calibration_instead_of_discarding_it() {
        let o = AgentObservable::new();
        for _ in 0..4 {
            o.record_run(
                "employee-1",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 12,
                    steps: 21,
                    tool_calls: 12,
                },
                RunDecision::no_declaration(),
            );
        }
        // 最长 12 + 均值 12。
        assert_eq!(
            o.iteration_budget_for(u32::MAX, "employee-1"),
            24,
            "没写上限不等于不要上限：测到的那一份就是上限"
        );
        // 写下的数照原样用，写大了也一样。
        assert_eq!(o.iteration_budget_for(40, "employee-1"), 40);
        // 写小了也照原样用：这个数现在是操作者对本部署的决定，不是某一行的
        // 数。标定只在没有人写过上限的时候回答。上限绑得太紧由「撞墙」那条
        // 读数说，不靠标定悄悄改写一个人的决定。
        assert_eq!(o.iteration_budget_for(3, "employee-1"), 3);
    }

    /// 一次都没交付过的时候，没写上限就仍然是没有上限：这时没有测到任何数，
    /// 凭空给一个，只是把猜数换个地方猜。
    #[test]
    fn an_unstated_ceiling_stays_unstated_until_something_is_measured() {
        let o = AgentObservable::new();
        assert_eq!(o.iteration_budget_for(u32::MAX, "employee-1"), u32::MAX);
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::BudgetExhausted,
            RunCost {
                iterations: 10,
                steps: 21,
                tool_calls: 12,
            },
            RunDecision::no_declaration(),
        );
        assert_eq!(o.iteration_budget_for(u32::MAX, "employee-1"), u32::MAX);
    }

    /// 上限必须容得下这个进程**已经交付过**的轮数，否则它不是安全线而是缺陷：
    /// 一个已知形状的运行会被砍在半路，而且砍掉是全额的——什么都没产出，
    /// 花掉的却全花了。
    #[test]
    fn the_ceiling_admits_the_longest_run_this_process_has_delivered() {
        let o = AgentObservable::new();
        for iterations in [6, 8] {
            o.record_run(
                "employee-1",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations,
                    steps: 20,
                    tool_calls: 5,
                },
                RunDecision::no_declaration(),
            );
        }
        let budget = o.iteration_budget_for(u32::MAX, "employee-1");
        assert!(
            budget > 8,
            "the ceiling must clear the longest completed run: {budget}"
        );
        assert!(
            budget > 10,
            "two delivered runs of 6 and 8 say the shape is longer than that: {budget}"
        );
    }

    /// 一次离群的交付不该成为所有人的天花板。它落在样本里，但按分位取的是这一族
    /// 通常需要的形状：没有这一条，一次 169 轮的交付会给接下来整个窗口里的每一
    /// 道活都开 169 轮的口子，而那正是实测里「一轮肥跑养肥所有人」的形状。
    #[test]
    fn one_outlier_does_not_raise_every_runs_ceiling() {
        let o = AgentObservable::new();
        for _ in 0..18 {
            o.record_run(
                "employee-1",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 5,
                    steps: 20,
                    tool_calls: 4,
                },
                RunDecision::no_declaration(),
            );
        }
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::Delivered,
            RunCost {
                iterations: 169,
                steps: 20,
                tool_calls: 4,
            },
            RunDecision::no_declaration(),
        );
        let budget = o.iteration_budget_for(u32::MAX, "employee-1");
        assert!(
            budget < 60,
            "an outlier must not become every run's ceiling: {budget}"
        );
        assert!(
            budget > 5,
            "but the ceiling still has to admit the shape this role usually needs: {budget}"
        );
    }

    /// 同一批运行，换一个标签写进去，读出来的上限是同一个数。
    ///
    /// 这是「换一个标签接，行为不变」在预算这一面上的量法：两个 observable 收
    /// 到的是同样四次交付（20 轮），都由同一个员工交，只是记在 `generator` 与
    /// `evaluator` 两个名字下。以前上限按角色存，这两个数会分叉——一个名字接手
    /// 同一件活，天花板就换了；现在样本按员工存，标签只落在读数上。
    #[test]
    fn a_different_label_does_not_move_the_ceiling() {
        let labeled_generator = AgentObservable::new();
        let labeled_evaluator = AgentObservable::new();
        for _ in 0..4 {
            labeled_generator.record_run(
                "employee-1",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 20,
                    steps: 40,
                    tool_calls: 8,
                },
                RunDecision::no_declaration(),
            );
            labeled_evaluator.record_run(
                "employee-1",
                "evaluator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 20,
                    steps: 40,
                    tool_calls: 8,
                },
                RunDecision::no_declaration(),
            );
        }
        assert_eq!(
            labeled_generator.iteration_budget_for(u32::MAX, "employee-1"),
            labeled_evaluator.iteration_budget_for(u32::MAX, "employee-1"),
            "同一个进程同样的运行，标签不同，预算必须相同"
        );
        assert!(
            labeled_generator.iteration_budget_for(u32::MAX, "employee-1") > 20,
            "四次 20 轮的交付给出的是一个容得下它们的数，换个标签也一样"
        );
    }

    /// 一个员工名下的运行，不因标签不同就分进两个窗口：同一个 observable 里
    /// 同一个人换着标签交卷，轮数一起算。分窗口的那个键是员工，不是标签。
    #[test]
    fn runs_under_two_labels_share_one_sample() {
        let o = AgentObservable::new();
        for _ in 0..4 {
            o.record_run(
                "employee-1",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 6,
                    steps: 20,
                    tool_calls: 5,
                },
                RunDecision::no_declaration(),
            );
        }
        let one_label = o.iteration_budget_for(u32::MAX, "employee-1");
        for _ in 0..4 {
            o.record_run(
                "employee-1",
                "evaluator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 30,
                    steps: 20,
                    tool_calls: 5,
                },
                RunDecision::no_declaration(),
            );
        }
        let two_labels = o.iteration_budget_for(u32::MAX, "employee-1");
        assert!(
            two_labels > one_label,
            "第二次记录必须落进同一个窗口并把它抬起来：{one_label} -> {two_labels}"
        );
    }

    /// 待遇跟着人走：同一个进程里两个人交出不同形状的活，各自的上限就各自不同；
    /// 第三个还没有自己样本的人先读进程的，读数上标得出来，等他交出第一轮就换。
    ///
    /// 「同一个座位换个人坐，待遇跟着变」在这里是反过来的：同一个座位（同一种活、
    /// 同一个标签）换个人坐，他带的是自己那份交卷记录，不继承前任的。
    #[test]
    fn each_employee_brings_their_own_record_to_the_same_seat() {
        let o = AgentObservable::new();
        for _ in 0..6 {
            o.record_run(
                "quick-hand",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 4,
                    steps: 20,
                    tool_calls: 5,
                },
                RunDecision::no_declaration(),
            );
            o.record_run(
                "deep-hand",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 40,
                    steps: 200,
                    tool_calls: 40,
                },
                RunDecision::no_declaration(),
            );
        }

        let (quick, quick_source) = o.iteration_budget_decision_for(u32::MAX, "quick-hand");
        let (deep, deep_source) = o.iteration_budget_decision_for(u32::MAX, "deep-hand");
        assert_eq!(quick_source, CeilingSource::MeasuredOwn);
        assert_eq!(deep_source, CeilingSource::MeasuredOwn);
        assert_eq!(quick, 8, "六次 4 轮的交付：4 + 4");
        assert_eq!(deep, 80, "六次 40 轮的交付：40 + 40");

        // 新来的人坐同一个座位：先读进程的，来源是 measured_process。
        let (inherited, inherited_source) = o.iteration_budget_decision_for(u32::MAX, "newcomer");
        assert_eq!(inherited_source, CeilingSource::MeasuredProcess);
        assert_eq!(
            inherited,
            40 + 22,
            "进程窗口里两位都在：40 轮的第 90 百分位 + 22 的平均"
        );

        // 他交出第一轮之后，读的就是自己的了。
        o.record_run(
            "newcomer",
            "generator",
            RunOutcome::Delivered,
            RunCost {
                iterations: 6,
                steps: 20,
                tool_calls: 4,
            },
            RunDecision::no_declaration(),
        );
        let (own_now, own_source) = o.iteration_budget_decision_for(u32::MAX, "newcomer");
        assert_eq!(own_source, CeilingSource::MeasuredOwn);
        assert_eq!(own_now, 12, "自己那条 6 轮：6 + 6");
    }

    /// 员工走了，他的窗口跟着走：名册是唯一还认得这个 id 的地方，名册里没有他
    /// 了，这份窗口就再也没有读者——留着它只会让同一个 id 再出现时继承一段
    /// 已经不属于任何人的历史。
    #[test]
    fn an_employee_that_leaves_the_roster_takes_their_window_with_them() {
        let o = AgentObservable::new();
        let now = std::time::Instant::now();
        o.publish_roster(vec![
            ("departing".to_string(), "generator".to_string(), now, true),
            ("staying".to_string(), "generator".to_string(), now, true),
        ]);
        for _ in 0..4 {
            o.record_run(
                "departing",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 40,
                    steps: 20,
                    tool_calls: 4,
                },
                RunDecision::no_declaration(),
            );
            o.record_run(
                "staying",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 4,
                    steps: 20,
                    tool_calls: 4,
                },
                RunDecision::no_declaration(),
            );
        }
        assert_eq!(o.iteration_budget_for(u32::MAX, "departing"), 80);

        o.publish_roster(vec![(
            "staying".to_string(),
            "generator".to_string(),
            now,
            true,
        )]);
        assert_eq!(
            o.iteration_budget_decision_for(u32::MAX, "departing").1,
            CeilingSource::MeasuredProcess,
            "走了的人还剩进程样本可读，但不是他自己的那一份了"
        );
        assert_eq!(
            o.iteration_budget_decision_for(u32::MAX, "staying").1,
            CeilingSource::MeasuredOwn,
            "还在名册上的人，自己的窗口不受影响"
        );
    }

    /// 窗口是有界的：上限只跟着最近一轮的样本走，不会把整个历史累加进内存，
    /// 也不会被很久以前的运行永远按住。
    #[test]
    fn the_sample_window_stays_bounded_and_recent() {
        let o = AgentObservable::new();
        for _ in 0..(BUDGET_SAMPLE_WINDOW + 10) {
            o.record_run(
                "employee-1",
                "generator",
                RunOutcome::Delivered,
                RunCost {
                    iterations: 40,
                    steps: 80,
                    tool_calls: 8,
                },
                RunDecision::no_declaration(),
            );
        }
        // 40 * (window + 10) 如果全留下会把均值抬到 40，上限就成 80 了。
        assert_eq!(
            o.iteration_budget_for(u32::MAX, "employee-1"),
            40 + 40,
            "the ceiling must be derived from the retained window, not the whole history"
        );
    }

    /// 上限只能被**交卷**的运行推高：被截断的运行没交付，它证明不了需要多少轮，
    /// 拿它去加预算等于按一个从没看见过的数字结账。
    #[test]
    fn only_completed_runs_raise_the_ceiling() {
        let o = AgentObservable::new();
        for _ in 0..50 {
            o.record_run(
                "employee-1",
                "generator",
                RunOutcome::BudgetExhausted,
                RunCost {
                    iterations: 10,
                    steps: 21,
                    tool_calls: 12,
                },
                RunDecision::no_declaration(),
            );
        }
        assert_eq!(o.iteration_budget_for(10, "employee-1"), 10);
    }

    /// 每个角色的截断次数要能被读到：上限是不是在绑住某条路径，是这次改动
    /// 存在的原因本身，看不见就等于没改。
    #[tokio::test]
    async fn exhausted_runs_are_readable_per_role() {
        let o = AgentObservable::new();
        // 撞墙记在判定的那一刻，终局记在 run 结束时——按真实顺序两件都走一遍。
        for _ in 0..2 {
            o.note_budget_exhausted("generator");
            o.record_run(
                "employee-1",
                "generator",
                RunOutcome::BudgetExhausted,
                RunCost {
                    iterations: 10,
                    steps: 21,
                    tool_calls: 12,
                },
                RunDecision::no_declaration(),
            );
        }
        o.record_run(
            "employee-1",
            "evaluator",
            RunOutcome::Delivered,
            RunCost {
                iterations: 3,
                steps: 7,
                tool_calls: 2,
            },
            RunDecision::no_declaration(),
        );

        let metrics = o.collect_metrics("D1").await.expect("collect_metrics");
        let exhausted = |role: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "agent_iteration_budget_exhausted"
                        && m.labels.get(crate::LABEL_ROLE).map(String::as_str) == Some(role)
                })
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        assert_eq!(exhausted("generator"), 2.0);
        assert_eq!(
            exhausted("evaluator"),
            0.0,
            "a role that never exhausted must read zero, not be missing"
        );
    }

    /// 「跑到顶且没交卷」与「跑到顶、被强制一问救回来」是两种结局，在
    /// `exhausted_runs` 那条总线上读不出差别。这条钉住只数前者的那条序列：
    /// 总额把两者都算上，无交付那条只算真买了空的一个。
    #[tokio::test]
    async fn a_no_deliverable_ending_is_counted_apart_from_a_salvaged_one() {
        let o = AgentObservable::new();
        for _ in 0..2 {
            o.note_budget_exhausted("generator");
            o.record_run(
                "employee-1",
                "generator",
                RunOutcome::BudgetExhausted,
                RunCost {
                    iterations: 10,
                    steps: 21,
                    tool_calls: 12,
                },
                RunDecision::no_declaration(),
            );
        }
        o.note_budget_exhausted("generator");
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            RunCost {
                iterations: 10,
                steps: 21,
                tool_calls: 12,
            },
            RunDecision::no_declaration(),
        );

        let metrics = o.collect_metrics("D1").await.expect("collect_metrics");
        let read = |name: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == name
                        && m.labels.get(crate::LABEL_ROLE).map(String::as_str) == Some("generator")
                })
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        assert_eq!(read("agent_iteration_budget_exhausted"), 3.0);
        assert_eq!(
            read("agent_iteration_budget_exhausted_no_deliverable"),
            2.0,
            "the salvaged run belongs on the total but not on the no-deliverable series"
        );
    }

    /// A run a rule stopped is counted where the per-role reader looks, and
    /// under its own name: a role that keeps meeting a guardrail is a different
    /// finding from one whose ceiling is too low, and the cure is different
    /// too — raising the ceiling answers the second and only re-buys the first.
    #[tokio::test]
    async fn a_run_stopped_by_a_rule_is_counted_per_role_under_its_own_name() {
        let o = AgentObservable::new();
        o.record_run(
            "employee-1",
            "writer",
            RunOutcome::Refused,
            RunCost {
                iterations: 2,
                steps: 3,
                tool_calls: 2,
            },
            RunDecision::no_declaration(),
        );
        o.record_run(
            "employee-1",
            "writer",
            RunOutcome::Refused,
            RunCost {
                iterations: 2,
                steps: 3,
                tool_calls: 2,
            },
            RunDecision::no_declaration(),
        );
        o.record_run(
            "employee-1",
            "planner",
            RunOutcome::Delivered,
            RunCost {
                iterations: 4,
                steps: 9,
                tool_calls: 3,
            },
            RunDecision::no_declaration(),
        );
        // A budget ending must not land on this series: the two are different
        // findings and a reader that cannot separate them cannot act on either.
        o.record_run(
            "employee-1",
            "planner",
            RunOutcome::BudgetExhausted,
            RunCost {
                iterations: 10,
                steps: 21,
                tool_calls: 12,
            },
            RunDecision::no_declaration(),
        );

        let metrics = o.collect_metrics("D1").await.expect("collect_metrics");
        let read = |role: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "agent_guardrail_refused_runs"
                        && m.labels.get(crate::LABEL_ROLE).map(String::as_str) == Some(role)
                })
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        assert_eq!(read("writer"), 2.0);
        assert_eq!(
            read("planner"),
            0.0,
            "a role that never met a rule must read zero, not be missing"
        );
    }

    /// 这一组就是这个 observable 的全部产出。多一个名字就多一条没人读的序列，
    /// 少一个名字就是一条静默消失的读数，两种都要在这里响。
    #[tokio::test]
    async fn the_published_set_is_exactly_the_cumulative_counters() {
        let o = AgentObservable::new();
        o.record_run(
            "employee-1",
            "generator",
            RunOutcome::Delivered,
            RunCost {
                iterations: 3,
                steps: 7,
                tool_calls: 2,
            },
            RunDecision::no_declaration(),
        );
        o.record_run(
            "employee-1",
            "evaluator",
            RunOutcome::BudgetExhausted,
            RunCost {
                iterations: 10,
                steps: 21,
                tool_calls: 12,
            },
            RunDecision::no_declaration(),
        );

        let mut names: Vec<String> = o
            .collect_metrics("")
            .await
            .expect("collect_metrics")
            .into_iter()
            .map(|m| m.name)
            .collect();
        names.sort();
        names.dedup();
        assert_eq!(
            names,
            vec![
                "agent_budget_exhausted_count",
                "agent_guardrail_refused_runs",
                "agent_inbox_ask_total",
                "agent_inbox_delivered_total",
                "agent_inbox_poisoned_total",
                "agent_inbox_refused_total",
                "agent_inbox_undelivered_total",
                "agent_iteration_budget_exhausted",
                "agent_iteration_budget_exhausted_no_deliverable",
                "agent_run_count",
                "agent_runs_by_ceiling",
                "agent_runs_by_declaration",
                "agent_success_count",
                "agent_total_steps",
                "agent_total_tool_calls",
            ]
        );
    }

    /// 声明为空集不是遗漏：它让采集侧只问一次。一旦重新声明某个维度，采集侧
    /// 会按维度把同一份不随维度变的读数问回来，Prometheus 会把重复样本丢掉。
    #[test]
    fn no_dimension_is_declared_because_the_readings_do_not_vary_by_one() {
        assert!(AgentObservable::new().available_dimensions().is_empty());
    }

    /// A failed command stays pending until this consumer next starts, so the
    /// only evidence that outlives the log window is the counter. Delivered and
    /// undelivered are read together: either alone reads as healthy.
    #[tokio::test]
    async fn an_inbox_failure_is_counted_not_only_logged() {
        let o = AgentObservable::new();
        o.record_inbox_delivered("prompt");
        o.record_inbox_delivered("prompt");
        o.record_inbox_undelivered("steering");
        o.record_inbox_poisoned();
        o.record_inbox_refused("no_live_agent");

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let labelled = |name: &str, key: &str, value: &str| {
            metrics
                .iter()
                .find(|m| m.name == name && m.labels.get(key).map(String::as_str) == Some(value))
                .map(|m| m.value)
        };
        assert_eq!(
            labelled("agent_inbox_delivered_total", "kind", "prompt"),
            Some(2.0)
        );
        assert_eq!(
            labelled("agent_inbox_undelivered_total", "kind", "steering"),
            Some(1.0)
        );
        assert_eq!(
            labelled("agent_inbox_refused_total", "reason", "no_live_agent"),
            Some(1.0)
        );
        assert_eq!(
            metrics
                .iter()
                .find(|m| m.name == "agent_inbox_poisoned_total")
                .map(|m| m.value),
            Some(1.0)
        );
    }

    /// A kind with no traffic is written as zero rather than left out, so the
    /// pair exists before the first command and a rate can be computed without
    /// waiting for the denominator to be born.
    #[tokio::test]
    async fn every_kind_is_written_even_before_any_command_arrives() {
        let o = AgentObservable::new();
        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        for name in [
            "agent_inbox_delivered_total",
            "agent_inbox_undelivered_total",
        ] {
            for kind in INBOX_KINDS {
                let found = metrics.iter().find(|m| {
                    m.name == name
                        && m.labels.get(crate::LABEL_KIND).map(String::as_str) == Some(kind)
                });
                assert_eq!(found.map(|m| m.value), Some(0.0), "{name} kind={kind}");
            }
        }
        for reason in INBOX_REFUSAL_REASONS {
            let found = metrics.iter().find(|m| {
                m.name == "agent_inbox_refused_total"
                    && m.labels.get(crate::LABEL_REASON).map(String::as_str) == Some(reason)
            });
            assert_eq!(found.map(|m| m.value), Some(0.0), "reason={reason}");
        }
        for outcome in INBOX_ASK_OUTCOMES {
            let found = metrics.iter().find(|m| {
                m.name == "agent_inbox_ask_total"
                    && m.labels.get(crate::LABEL_OUTCOME).map(String::as_str) == Some(outcome)
            });
            assert_eq!(found.map(|m| m.value), Some(0.0), "outcome={outcome}");
        }
    }

    /// An ask that timed out moved no delivery counter: the command was
    /// published and the target may still be running it, so the only place the
    /// wait itself is visible is here. Counted by outcome because the three
    /// call for different repairs -- a peer that is slow, a caller that waits
    /// too briefly, and a channel that closed under the reader.
    #[tokio::test]
    async fn a_waited_for_answer_is_counted_by_how_the_wait_ended() {
        let o = AgentObservable::new();
        o.record_inbox_ask("replied");
        o.record_inbox_ask("timeout");
        o.record_inbox_ask("timeout");

        let metrics = o.collect_metrics("").await.expect("collect_metrics");
        let read = |outcome: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "agent_inbox_ask_total"
                        && m.labels.get(crate::LABEL_OUTCOME).map(String::as_str) == Some(outcome)
                })
                .map(|m| m.value)
        };
        assert_eq!(read("replied"), Some(1.0));
        assert_eq!(read("timeout"), Some(2.0));
        assert_eq!(
            read("stream_error"),
            Some(0.0),
            "an outcome no ask met reads zero, not missing"
        );
        assert_eq!(
            metrics
                .iter()
                .filter(|m| m.name.starts_with("agent_inbox_delivered")
                    || m.name.starts_with("agent_inbox_undelivered"))
                .map(|m| m.value)
                .sum::<f64>(),
            0.0,
            "a wait that gave up says nothing about delivery either way"
        );
    }

    /// The label list is a second spelling of the wire form, and a variant
    /// added to the enum without a label here would be counted under nothing:
    /// the scrape loop walks `INBOX_KINDS`, so the missing kind would simply
    /// never be printed and its commands would vanish from the reading.
    #[test]
    fn the_label_list_covers_every_command_the_wire_form_can_carry() {
        let every = [
            cog_core::InboxMessage::Prompt {
                goal: serde_json::Value::Null,
                reply_stream: None,
                from_agent: None,
            },
            cog_core::InboxMessage::Continue {
                instruction: String::new(),
                from_agent: None,
            },
            cog_core::InboxMessage::Abort,
            cog_core::InboxMessage::Reset,
            cog_core::InboxMessage::Steering {
                instruction: String::new(),
                from_agent: None,
            },
        ];
        let mut kinds: Vec<&'static str> = every.iter().map(inbox_kind).collect();
        kinds.sort_unstable();
        kinds.dedup();
        let mut listed = INBOX_KINDS.to_vec();
        listed.sort_unstable();
        assert_eq!(kinds, listed);
    }
}
