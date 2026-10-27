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
    /// Per-role reads of how many iterations a run actually took, keyed by role.
    ///
    /// The key is a role, which is a closed set the code names — not a task id
    /// or a path, which would grow without bound. The sample itself is a recent
    /// window (see [`BUDGET_SAMPLE_WINDOW`]), so the map stays bounded too.
    role_calibrations: Arc<Mutex<HashMap<String, RoleCalibration>>>,
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

/// How many of a role's most recent completed runs feed its budget.
///
/// A window rather than a lifetime archive on purpose: the ceiling is meant to
/// track the workload as it is now, and a run from a month ago under a
/// different model says nothing about how much work a run needs today.
const BUDGET_SAMPLE_WINDOW: usize = 64;

/// What a role's own finished runs say about how many iterations it needs.
///
/// The iteration budget used to be one literal for every role, which can only
/// be right by accident: set too low, a run that was still working is cut off
/// mid-exploration and returns nothing (measured: 6 of 8 generator runs stopped
/// at exactly the ceiling with tool calls still pending, against a 25% delivery
/// rate); set too high, every run pays for slack it does not use. How much work
/// a role needs is observable from the runs it completes, so the ceiling is
/// derived from that instead of being a number someone has to guess and keep
/// guessing.
#[derive(Default)]
struct RoleCalibration {
    /// Iterations consumed by runs of this role that finished with an answer it
    /// reached on its own. A run cut off at the ceiling is left out even when
    /// the forced ask salvaged an answer: it stopped where the ceiling said, so
    /// its length measures the ceiling, not the work. Feeding it back lets every
    /// rescue raise the next ceiling, which makes the next rescue more likely —
    /// a ceiling climbing on its own record of having been hit.
    delivered_iterations: VecDeque<u32>,
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

    /// Replace the published roster with `entries` (role, creation instant).
    ///
    /// Replaced wholesale rather than incremented on create and on retire: a
    /// reader comparing a count that was incremented and decremented on two
    /// separate paths can only ask whether the two balanced, and a path that
    /// failed to decrement looks exactly like a path that never ran. Publishing
    /// the list makes the manager the single owner of the truth and the scrape
    /// a plain view of it.
    pub fn publish_roster(
        &self,
        entries: impl IntoIterator<Item = (String, std::time::Instant, bool)>,
    ) {
        let next: Vec<RosterEntry> = entries
            .into_iter()
            .map(|(role, created_at, task_scoped)| RosterEntry {
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
    /// `role` is what makes the reading usable at all: the iteration ceiling is
    /// derived per role, and a run counted without its role cannot inform any
    /// decision. `iterations` is in the same unit the ceiling is expressed in —
    /// not `steps`, which counts assistant turns and would silently rescale
    /// every reading it fed.
    pub fn record_run(
        &self,
        role: &str,
        outcome: RunOutcome,
        iterations: u32,
        steps: usize,
        tool_calls: usize,
    ) {
        self.run_count.fetch_add(1, Ordering::Relaxed);
        if outcome.delivered() {
            self.success_count.fetch_add(1, Ordering::Relaxed);
        }
        self.total_steps.fetch_add(steps as u64, Ordering::Relaxed);
        self.total_tool_calls
            .fetch_add(tool_calls as u64, Ordering::Relaxed);

        let mut calibrations = self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let calibration = calibrations.entry(role.to_string()).or_default();
        if outcome == RunOutcome::Delivered {
            if calibration.delivered_iterations.len() == BUDGET_SAMPLE_WINDOW {
                calibration.delivered_iterations.pop_front();
            }
            calibration.delivered_iterations.push_back(iterations);
        }
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

    /// The iteration ceiling to run this role under, given the value a caller
    /// would use with no evidence behind it (`seed`: the operator's config, or
    /// the role's skill).
    ///
    /// With nothing observed about this role the seed stands — moving a number
    /// without evidence would be guesswork with extra steps. Once runs have
    /// completed, the ceiling is the longest run this role needed plus one more
    /// typical run's worth of room:
    ///
    /// - the longest completed run is the floor of experience. A ceiling that
    ///   will not admit a run of a shape this role is known to have needed is
    ///   not a safety limit, it is a defect;
    /// - the headroom is what keeps the ceiling from being a claim about the
    ///   past. A ceiling set exactly to the historical maximum cuts off the
    ///   first run that is slightly harder than anything seen, and that cut is
    ///   total: the run returns nothing and has already spent everything. One
    ///   typical work unit (the mean) of slack absorbs that variance.
    ///
    /// Both terms are observed, so the ceiling follows the workload instead of
    /// needing to be retuned by hand whenever the workload changes.
    pub fn iteration_budget_for(&self, role: &str, seed: u32) -> u32 {
        let calibrations = self
            .role_calibrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(calibration) = calibrations.get(role) else {
            return seed;
        };
        if calibration.delivered_iterations.is_empty() {
            return seed;
        }
        let longest = *calibration
            .delivered_iterations
            .iter()
            .max()
            .expect("sample is not empty");
        let sum: u32 = calibration.delivered_iterations.iter().sum();
        let typical = sum.div_ceil(calibration.delivered_iterations.len() as u32);
        seed.max(longest.saturating_add(typical))
    }
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
        o.publish_roster(vec![("planner".to_string(), now, true)]);
        o.publish_roster(Vec::<(String, std::time::Instant, bool)>::new());

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
            ("planner".to_string(), now, true),
            ("mode_selector".to_string(), now, false),
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
            ("planner".to_string(), now, true),
            ("planner".to_string(), now, true),
            ("planner".to_string(), long_lived, false),
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
        o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);

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
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            11,
            21,
            12,
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
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            11,
            21,
            12,
        );
        assert_eq!(
            o.iteration_budget_for("generator", 10),
            10,
            "在上限处被截断的运行，不能是「上限太低」的证据"
        );
    }

    #[tokio::test]
    async fn a_run_that_delivered_an_answer_is_counted_as_a_success() {
        let o = AgentObservable::new();
        o.record_run("generator", RunOutcome::Delivered, 3, 7, 2);

        let (runs, successes, exhausted) = readings(&o).await;
        assert_eq!(runs, 1.0);
        assert_eq!(successes, 1.0);
        assert_eq!(exhausted, 0.0);
    }

    /// 没有任何观测就该原样用调用方给的种子：凭一个没看见过的事实改数字，
    /// 只是把猜数换了个地方猜。
    #[test]
    fn a_role_with_no_completed_run_keeps_the_seed_budget() {
        let o = AgentObservable::new();
        assert_eq!(o.iteration_budget_for("generator", 10), 10);
        // 只有被截断的运行也不是"跑完过"的证据：它没交卷，也就没说需要多少轮。
        o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        assert_eq!(o.iteration_budget_for("generator", 10), 10);
    }

    /// 上限必须容得下这个角色**已经交付过**的轮数，否则它不是安全线而是缺陷：
    /// 一个已知形状的运行会被砍在半路，而且砍掉是全额的——什么都没产出，
    /// 花掉的却全花了。
    #[test]
    fn the_ceiling_admits_the_longest_run_this_role_has_delivered() {
        let o = AgentObservable::new();
        for iterations in [6, 8] {
            o.record_run("generator", RunOutcome::Delivered, iterations, 20, 5);
        }
        let budget = o.iteration_budget_for("generator", 10);
        assert!(
            budget > 8,
            "the ceiling must clear the longest completed run: {budget}"
        );
        assert!(
            budget > 10,
            "two delivered runs of 6 and 8 say the seed was binding, not that 10 is right: {budget}"
        );
    }

    /// 读数按角色分开：另外一条角色的运行说不了这个角色需要多少轮。
    #[test]
    fn one_roles_runs_do_not_set_another_roles_ceiling() {
        let o = AgentObservable::new();
        for _ in 0..4 {
            o.record_run("generator", RunOutcome::Delivered, 20, 40, 8);
        }
        assert_eq!(o.iteration_budget_for("evaluator", 5), 5);
        assert!(o.iteration_budget_for("generator", 10) > 10);
    }

    /// 窗口是有界的：上限只跟着最近一轮的样本走，不会把整个历史累加进内存，
    /// 也不会被很久以前的运行永远按住。
    #[test]
    fn the_sample_window_stays_bounded_and_recent() {
        let o = AgentObservable::new();
        for _ in 0..(BUDGET_SAMPLE_WINDOW + 10) {
            o.record_run("generator", RunOutcome::Delivered, 40, 80, 8);
        }
        // 40 * (window + 10) 如果全留下会把均值抬到 40，上限就成 80 了。
        assert_eq!(
            o.iteration_budget_for("generator", 1),
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
            o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        }
        assert_eq!(o.iteration_budget_for("generator", 10), 10);
    }

    /// 每个角色的截断次数要能被读到：上限是不是在绑住某条路径，是这次改动
    /// 存在的原因本身，看不见就等于没改。
    #[tokio::test]
    async fn exhausted_runs_are_readable_per_role() {
        let o = AgentObservable::new();
        // 撞墙记在判定的那一刻，终局记在 run 结束时——按真实顺序两件都走一遍。
        for _ in 0..2 {
            o.note_budget_exhausted("generator");
            o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        }
        o.record_run("evaluator", RunOutcome::Delivered, 3, 7, 2);

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
            o.record_run("generator", RunOutcome::BudgetExhausted, 10, 21, 12);
        }
        o.note_budget_exhausted("generator");
        o.record_run(
            "generator",
            RunOutcome::DeliveredAfterExhaustion,
            10,
            21,
            12,
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
        o.record_run("writer", RunOutcome::Refused, 2, 3, 2);
        o.record_run("writer", RunOutcome::Refused, 2, 3, 2);
        o.record_run("planner", RunOutcome::Delivered, 4, 9, 3);
        // A budget ending must not land on this series: the two are different
        // findings and a reader that cannot separate them cannot act on either.
        o.record_run("planner", RunOutcome::BudgetExhausted, 10, 21, 12);

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
        o.record_run("generator", RunOutcome::Delivered, 3, 7, 2);
        o.record_run("evaluator", RunOutcome::BudgetExhausted, 10, 21, 12);

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
            },
            cog_core::InboxMessage::Continue {
                instruction: String::new(),
            },
            cog_core::InboxMessage::Abort,
            cog_core::InboxMessage::Reset,
            cog_core::InboxMessage::Steering {
                instruction: String::new(),
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
