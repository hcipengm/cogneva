//! Self-discovery signal watcher — the producer side of system self-discovery.
//!
//! Evolution inputs are not only external intents (issues/PRs); the system
//! must also discover its own problems. This watcher polls orchestrator task
//! state and the durable submission store, and turns five classes of signals
//! into internal evolution intents submitted through the main flow
//! (`evolution_mode=generate_change`):
//!
//! 1. **Failure recurrence**: self-evolution tasks failing with the same
//!    error signature over and over mean a systematic defect, not bad luck.
//! 2. **Queue backlog**: pending/scheduled piling up means the pipeline is
//!    stalled somewhere — itself a defect worth an intent.
//! 3. **Periodic self-audit**: a cadence-gated intent asking a squad to audit
//!    the own repository (hardcoded secrets, weak defaults, injection
//!    surface) — runtime reflection cannot see static code properties.
//! 4. **Persisted alerts**: firing rows in the alert state machine (infra
//!    watcher, supervisor bridge) are faults something already judged
//!    alert-worthy; each becomes an intent keyed by its dedup key.
//! 5. **Taste intents**: judgements submitted from outside the pipeline
//!    (whether a claimed result holds up, which direction is worth more, what
//!    the standard of good should be). This is the one channel here whose
//!    input does not come from the system's own state, and it is the reason
//!    the other four cannot stand in for it: they all say where it hurts, and
//!    none of them says where to go. Its input is also the one that can be
//!    absent for a reason no reading here can see — nobody submitted anything
//!    — so a quiet channel means nobody spoke, not that nothing is valuable.
//!
//! Every intent carries a deterministic id, which lets the watcher ask the
//! task store whether the signal is already in hand before submitting: the
//! orchestrator raises on a duplicate id rather than skipping it, so the
//! idempotence has to live here. A task still running is left alone (and
//! spends no cooldown, so a later failure is noticed on the next tick, not a
//! whole cooldown later); a task that failed while its signal persists is
//! re-driven; a task that finished while its signal persists holds the id for
//! work that is over, so the row is cleared and the signal filed again — the
//! store keeps finished rows for two cooldowns, and without this the signal
//! would wait out that whole retention before anything looked at it again; a
//! task that never reached the orchestrator spends no cooldown either, so the
//! next tick retries instead of the signal going quiet.
//!
//! Only the process that owns the change-execution role runs this loop. The
//! plugin table is loaded whole by every deployment, and producing intents is
//! an outward side effect: a control plane doing it too submits every signal
//! twice, and the loser only learns of it as a duplicate-task error.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use cog_core::{OrchestratorControl, Task, TaskStatus, TaskType};

use crate::signal_readings::{SignalOutcome, SignalWatcherReadings};

/// Watcher configuration, loaded from `self_evolution.signal_watcher` in
/// cogneva.json with env overrides; missing section falls back to defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SignalWatcherConfig {
    /// Master switch.
    pub enabled: bool,
    /// Poll interval (seconds); floor 60.
    pub poll_interval_secs: u64,
    /// Same-signature failures within the window that trigger an intent.
    pub failure_recurrence_threshold: usize,
    /// Window (seconds) for counting recurring failures.
    pub failure_window_secs: i64,
    /// Pending+Scheduled count that counts as a backlog anomaly.
    pub backlog_threshold: usize,
    /// Minimum seconds before the same signal is re-reported.
    pub report_cooldown_secs: i64,
    /// Self-audit cadence (seconds); 0 disables the audit channel.
    pub self_audit_interval_secs: i64,
    /// Channel 4: turn persisted firing alerts into intents. Persisted
    /// alerts are how infrastructure and gateway faults surface to
    /// self-discovery — without this channel they page nobody.
    pub alert_channel_enabled: bool,
    /// Max alerts converted to intents per tick (flood guard).
    pub alert_channel_max_per_tick: usize,
    /// Longest an alert label value may be where this channel inlines it into
    /// a goal.
    ///
    /// The goal is submitted as a task input, and when that task stalls its
    /// input comes back as the next alert's labels — so an unbounded label
    /// value makes every generation cost more tokens than the one before it
    /// for the same non-progress. The cap is here, and not only on the
    /// producers, because the control plane that raised the alert can be an
    /// older revision than this watcher.
    pub alert_label_max_chars: usize,
    /// Channel 5: turn taste intents submitted from outside the pipeline into
    /// intents. This is the only channel here that carries a judgement of
    /// value rather than a defect.
    ///
    /// It carries only what someone stated, so a deployment where nobody
    /// submits has no signal on it — and nothing on this channel can tell that
    /// from having nothing to say. It therefore cannot be a system's only
    /// taste source: the judgements it cannot reach are the ones nobody has
    /// put into words yet, which are exactly the ones worth finding.
    pub taste_channel_enabled: bool,
    /// Max taste submissions acted on per tick (flood guard).
    pub taste_channel_max_per_tick: usize,
}

impl Default for SignalWatcherConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 300,
            failure_recurrence_threshold: 3,
            failure_window_secs: 3600,
            backlog_threshold: 100,
            report_cooldown_secs: 86_400,
            self_audit_interval_secs: 7 * 86_400,
            alert_channel_enabled: true,
            alert_channel_max_per_tick: 5,
            alert_label_max_chars: cog_core::ALERT_LABEL_VALUE_MAX_CHARS,
            taste_channel_enabled: true,
            taste_channel_max_per_tick: 5,
        }
    }
}

impl SignalWatcherConfig {
    /// 从 cogneva.json 的 `self_evolution.signal_watcher` 段加载，再叠加
    /// env 覆盖。文件或段缺失时返回 Default；段存在但解析失败、或 env
    /// 值非法时返回 Err——配置写错必须响亮失败。
    pub fn load() -> cog_core::SFResult<Self> {
        let path = cog_core::paths::config_file();
        let mut cfg = match std::fs::read_to_string(&path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| cog_core::SFError::Config(format!("{path}: {e}")))?;
                match root.pointer("/self_evolution/signal_watcher") {
                    Some(section) => serde_json::from_value(section.clone()).map_err(|e| {
                        cog_core::SFError::Config(format!(
                            "{path} self_evolution.signal_watcher: {e}"
                        ))
                    })?,
                    None => Self::default(),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => return Err(cog_core::SFError::Config(format!("{path}: {e}"))),
        };
        if let Ok(v) = std::env::var("COGNEVA_SIGNAL_WATCHER_ENABLED") {
            cfg.enabled = matches!(v.as_str(), "1" | "true" | "yes" | "on");
        }
        if let Ok(v) = std::env::var("COGNEVA_SIGNAL_WATCHER_POLL_SECS") {
            cfg.poll_interval_secs = v.parse().map_err(|_| {
                cog_core::SFError::Config(format!("COGNEVA_SIGNAL_WATCHER_POLL_SECS: {v}"))
            })?;
        }
        if let Ok(v) = std::env::var("COGNEVA_SELF_AUDIT_INTERVAL_SECS") {
            cfg.self_audit_interval_secs = v.parse().map_err(|_| {
                cog_core::SFError::Config(format!("COGNEVA_SELF_AUDIT_INTERVAL_SECS: {v}"))
            })?;
        }
        Ok(cfg)
    }
}

/// Persisted watcher guards (`$COGNEVA_DATA_DIR/self-signal-guards.json`):
/// when each signal key was last reported, and the last self-audit time.
/// Without persistence a pod restart would re-report every active signal.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SignalGuardState {
    /// signal key → last report time.
    #[serde(default)]
    reported: HashMap<String, DateTime<Utc>>,
    /// Last self-audit submission time.
    #[serde(default)]
    last_audit: Option<DateTime<Utc>>,
}

fn state_path() -> PathBuf {
    let dir = cog_core::paths::data_dir();
    PathBuf::from(dir).join("self-signal-guards.json")
}

async fn load_state() -> SignalGuardState {
    match tokio::fs::read_to_string(state_path()).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(_) => SignalGuardState::default(),
    }
}

async fn save_state(state: &SignalGuardState) {
    let path = state_path();
    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    if let Ok(text) = serde_json::to_string_pretty(state) {
        let _ = tokio::fs::write(&path, text).await;
    }
}

/// Drop the guard entries whose cooldown has already elapsed, and say how many
/// went.
///
/// An entry means one thing only: this key may not be reported again before
/// `last + cooldown`. A key that is absent and a key whose cooldown has elapsed
/// are answered identically by [`cooldown_elapsed`], so reclaiming them changes
/// no decision this store takes -- which is what makes it safe to do without
/// touching the check.
///
/// What it changes is the store's size, and that is the reason it exists. An
/// alert key carries the labels that identify its instance, and for most rules
/// those include the pod it was observed on. A rollout therefore leaves behind
/// entries naming a pod that will never be seen again, and this store had no
/// reclaimer: it only ever grew, so its size said nothing about how much was
/// actually being suppressed, and a key that can no longer match was
/// indistinguishable from one still holding a signal back.
fn prune_elapsed(state: &mut SignalGuardState, cooldown_secs: i64, now: DateTime<Utc>) -> usize {
    let before = state.reported.len();
    state
        .reported
        .retain(|_, last| (now - *last).num_seconds() < cooldown_secs);
    before - state.reported.len()
}

/// True when `key` was never reported or its cooldown elapsed. Pure check: the
/// report is recorded by [`report_outcome`] only once an intent actually
/// landed, so a submission that never reached the orchestrator is retried on
/// the next tick instead of being silenced for a whole cooldown.
fn cooldown_elapsed(
    state: &SignalGuardState,
    key: &str,
    cooldown_secs: i64,
    now: DateTime<Utc>,
) -> bool {
    match state.reported.get(key) {
        Some(last) => (now - *last).num_seconds() >= cooldown_secs,
        None => true,
    }
}

/// What this round owes the work standing behind a firing alert.
///
/// A firing alert is two facts at once, and the round used to answer both with
/// one predicate. The alert says a condition is true *now*; the work that
/// condition stands for is a task row, and that row is the only thing that
/// knows whether the work is done. Reading the report clock first meant a row
/// could say "the last attempt failed" or "the last attempt ended having
/// produced nothing" and the round would still do nothing, because the signal
/// had been announced too recently. On 2026-10-04 that is what happened: six
/// self-discovery attempts failed at 06:00Z and were not touched again for the
/// rest of the day, because the clock they were read against had been armed
/// when the signal was announced, hours earlier.
///
/// The clock is about repeats. Work that did not finish is not a repeat.
#[derive(Debug, PartialEq, Eq)]
enum AlertWork {
    /// A task for this signal is running. Nothing to do, and the clock is not
    /// consulted: an attempt in flight is work in hand, not a spent report.
    InHand,
    /// The condition is true and the work did not finish -- the attempt failed,
    /// or it ended without producing a change at all. Driven this round
    /// whatever the clock says.
    Unfinished,
    /// Nothing has been filed for this signal. A first announcement, and the
    /// clock for it starts at the last report.
    NeverFiled,
    /// The last attempt ended and did produce work. The signal outliving that
    /// attempt is a repeat, and the clock for a repeat runs from when the
    /// attempt ended -- not from when the signal was first announced, which may
    /// be weeks earlier and would hold every repeat behind a window that has
    /// nothing to do with it.
    Ended(DateTime<Utc>),
}

/// What became of the work filed for one alert, read from the row the
/// submission itself would read.
///
/// Through [`intent_action`], so the round's decision and the submission's
/// cannot disagree: a status this predicate calls finished is one
/// `submit_intent` will resubmit, and a status it calls in flight is one
/// `submit_intent` will refuse as a duplicate.
fn alert_work(existing: Option<&Task>, may_redrive: bool) -> AlertWork {
    let Some(task) = existing else {
        return AlertWork::NeverFiled;
    };
    match intent_action(Some(&task.status), may_redrive) {
        IntentAction::None => AlertWork::InHand,
        IntentAction::Redrive => AlertWork::Unfinished,
        IntentAction::Submit => AlertWork::NeverFiled,
        IntentAction::Resubmit => {
            if produced_a_change(task) {
                AlertWork::Ended(task.updated_at)
            } else {
                AlertWork::Unfinished
            }
        }
    }
}

/// Whether a finished attempt left anything behind.
///
/// The change ids are what the pipeline records on the task when it produces
/// artifacts. Their absence on an attempt that reached a terminal state means
/// the attempt ran and produced nothing to land: the job is unfinished, not
/// repeated, and re-driving it is the difference between a condition that is
/// being worked and one that only looks as if it is.
fn produced_a_change(task: &Task) -> bool {
    task.result
        .as_ref()
        .and_then(|r| r.get("change_ids"))
        .and_then(|v| v.as_array())
        .is_some_and(|ids| !ids.is_empty())
}

/// The firing alerts this tick should act on: the ones whose work is
/// unfinished first, then the ones whose report clock has elapsed, stopping
/// once `max` are left.
///
/// The cap bounds the work a single tick creates, so it has to count the alerts
/// actually selected rather than the first `max` rows the store returned. The
/// store hands them back newest-first, so truncating there lets a cluster of
/// newly fired alerts hide every older one behind them for as long as they keep
/// firing — which is exactly when a long-standing fault most needs to be seen.
fn select_alerts<'a>(
    alerts: &'a [cog_core::PersistedAlert],
    tasks: &[Task],
    state: &SignalGuardState,
    max: usize,
    cooldown_secs: i64,
    now: DateTime<Utc>,
    may_redrive: bool,
) -> SelectedAlerts<'a> {
    let mut unfinished = Vec::new();
    let mut announced = Vec::new();
    let mut in_hand = 0;
    let mut in_cooldown = 0;
    for alert in alerts {
        let row = tasks
            .iter()
            .find(|t| t.id == alert_task_id(&alert.dedup_key));
        match alert_work(row, may_redrive) {
            AlertWork::InHand => in_hand += 1,
            AlertWork::Unfinished => unfinished.push(alert),
            AlertWork::NeverFiled => {
                if cooldown_elapsed(
                    state,
                    &alert_signal_key(&alert.dedup_key),
                    cooldown_secs,
                    now,
                ) {
                    announced.push(alert);
                } else {
                    in_cooldown += 1;
                }
            }
            AlertWork::Ended(ended_at) => {
                if (now - ended_at).num_seconds() >= cooldown_secs {
                    announced.push(alert);
                } else {
                    in_cooldown += 1;
                }
            }
        }
    }
    // Unfinished work goes first: it is the part a cap must never be the reason
    // to leave undone.
    let selected: Vec<&cog_core::PersistedAlert> =
        unfinished.into_iter().chain(announced).take(max).collect();
    SelectedAlerts {
        selected,
        in_hand,
        in_cooldown,
    }
}

/// The alerts one tick acts on, and what it left alone.
///
/// These are returned together rather than counted apart: "an alert was in
/// cooldown", "an alert was already in hand" and "an alert was found and
/// submitted" are answers to the same question about the same input, and a
/// second pass over the same list to recover any of them would be a second copy
/// of the predicate that has to stay in step with this one. They stay separate
/// counts because "the clock held this signal back" and "this signal is already
/// being worked on" are not the same state, and a reader shown one number could
/// not tell which of them a quiet round was.
struct SelectedAlerts<'a> {
    selected: Vec<&'a cog_core::PersistedAlert>,
    /// Alerts whose task is running: nothing to drive.
    in_hand: usize,
    /// Alerts held back because this would have been a repeat report.
    in_cooldown: usize,
}

/// Turn one firing alert into the intent that will try to fix it.
///
/// The labels are clamped on the way in because this intent's task input is
/// what the *next* alert about this work will carry as its labels: an alert
/// that embedded its task's whole input, turned into a goal that inlines the
/// labels, became a task whose input the following alert embedded again.
/// Nothing in that chain makes progress until the upstream returns, so the
/// payload was pure growth — eight generations took one alert's labels from
/// 1.9 KB of JSON to 25 KB, all of it the same text re-nested.
fn alert_intent(
    alert: &cog_core::PersistedAlert,
    max_label_chars: usize,
) -> (String, serde_json::Value) {
    let labels = cog_core::bound_alert_labels(&alert.labels, max_label_chars);
    // The first line is the landing header: the rule name is the part that
    // tells two alert-driven changes apart, so it comes first, where a
    // shortened header still shows it.
    let goal = format!(
        "Fix alert \"{}\"\n\nInvestigate and fix the root cause of firing alert \"{}\" \
         (severity: {}): {}. Alert labels: {}. The alert fired at {} \
         and is still active. Identify the underlying defect or \
         resource condition, implement a durable fix, and explain \
         how recurrence is prevented.",
        alert.rule,
        alert.rule,
        alert.severity,
        alert.message,
        labels,
        alert.fired_at.to_rfc3339(),
    );
    let detail = serde_json::json!({
        "kind": "persisted_alert",
        "rule": alert.rule,
        "dedup_key": alert.dedup_key,
        "severity": alert.severity,
        "labels": labels,
    });
    (goal, detail)
}

/// Normalize a task error into a recurrence signature: same root cause must
/// map to the same signature regardless of run ids, numbers, timestamps.
fn error_signature(error: &str) -> String {
    let first_line = error.lines().next().unwrap_or("").trim();
    let mut sig = String::with_capacity(first_line.len());
    let mut last_space = true;
    for ch in first_line.chars() {
        if ch.is_ascii_digit() {
            if !last_space {
                sig.push(' ');
                last_space = true;
            }
            continue;
        }
        if ch.is_whitespace() {
            if !last_space {
                sig.push(' ');
            }
            last_space = true;
            continue;
        }
        sig.push(ch.to_ascii_lowercase());
        last_space = false;
    }
    let sig = sig.trim().to_string();
    let sig = if sig.is_empty() { "unknown" } else { &sig };
    sig.chars().take(80).collect()
}

fn short_hash(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// The report-cooldown key an alert's signal is filed under.
fn alert_signal_key(dedup_key: &str) -> String {
    format!("alert:{dedup_key}")
}

/// The task an alert's work is filed under.
///
/// Derived from the alert's identity rather than minted per submission, so the
/// row survives across rounds and can be read back. That read is what lets a
/// round ask whether the work behind a firing alert is done, which it cannot do
/// for a row it cannot name -- and the name is written here alone so the reader
/// and the writer cannot come to disagree about which row an alert owns.
fn alert_task_id(dedup_key: &str) -> String {
    format!(
        "self-signal-alert-{}",
        short_hash(&alert_signal_key(dedup_key))
    )
}

/// The report-cooldown key a taste submission's signal is filed under.
///
/// The kind and the subject together, both taken from the submission: two
/// judgements of the same kind about the same subject are one signal, and a
/// judgement about a subject differs from a preference over it. The subject is
/// the submitter's own naming, so this suppresses repeats rather than guarding
/// against a submitter — one thing named two ways is two subjects.
fn taste_signal_key(intent: &cog_core::TasteIntent) -> String {
    format!("taste:{}:{}", intent.payload.kind(), intent.subject)
}

/// The task a submission's work is filed under.
///
/// Derived from the submission's own id, which is minted per submission, so
/// each submission owns exactly one task row for its whole life: the row can be
/// read back to ask whether the work is still running, and re-submitting the
/// same judgement after its cooldown produces a new submission with a new id
/// rather than a second filing under a name that already holds a finished
/// attempt's history.
fn taste_task_id(intent: &cog_core::TasteIntent) -> String {
    format!("self-signal-taste-{}", intent.id)
}

/// Turn one taste submission into the intent that will act on it.
///
/// The judgement is quoted rather than paraphrased: the submitter's reason is
/// the whole input, and a squad acting on a summary of it would be acting on
/// the summary. The goal says what kind of thing this is — a stated judgement,
/// not an instruction — because a channel that turned one person's preference
/// into an order would be a very different system from one that weighs it.
fn taste_intent(intent: &cog_core::TasteIntent) -> (String, serde_json::Value) {
    // The first line is the landing header. The subject is what tells one
    // submission from another, so it leads rather than following the sentence
    // that explains where the submission came from.
    let goal = format!(
        "Act on judgement \"{}\"\n\nAct on a judgement submitted from outside the pipeline \
         about \"{}\": {}. Submitted by {} at {}. Decide whether this judgement calls for a \
         change — to behaviour, to a decision, or to the criterion work is judged by — \
         and implement the change it calls for; if it calls for none, say why in \
         the change description. This is a stated judgement rather than an \
         instruction: what is being asked for is the reason behind it, and the \
         usual gates still decide whether the result is good.",
        intent.subject,
        intent.subject,
        intent.payload.summary(),
        intent.submitted_by,
        intent.submitted_at.to_rfc3339(),
    );
    let detail = serde_json::json!({
        "kind": "taste_intent",
        "taste_kind": intent.payload.kind(),
        "subject": intent.subject,
        "submitted_by": intent.submitted_by,
        "submitted_at": intent.submitted_at.to_rfc3339(),
        "intent_id": intent.id,
        "payload": intent.payload,
    });
    (goal, detail)
}

/// What one round does about the taste submissions it read.
///
/// Work and endings are returned together for the same reason the alert
/// channel returns them together: "held back as a repeat" and "driven this
/// round" are answers about the same rows, and recovering either one from a
/// second pass would be a second copy of the predicate.
struct SelectedTaste<'a> {
    /// Submissions to hand to the task layer this round, unfinished work first.
    selected: Vec<&'a cog_core::StoredTasteIntent>,
    /// Submissions to close as superseded: a judgement of the same kind about
    /// the same subject was reported so recently that this one is a repeat.
    superseded: Vec<&'a cog_core::StoredTasteIntent>,
}

/// Choose this round's taste work.
///
/// The two reads answer different questions and are chained rather than merged.
/// A submission nothing was filed for is a first announcement, and the clock
/// for it is what holds a repeat back. A submission whose work was filed and
/// failed is unfinished work, and unfinished work is not a repeat: it is driven
/// whatever the clock says, which is why it goes first, ahead of the cap.
///
/// A submission whose work is still running, or whose work ended and produced
/// its result, has nothing to do and is not counted as anything — the row is
/// history from the moment its work finished. Only the repeats are returned to
/// be closed, because closing them is what keeps them from being re-read as new
/// announcements every round once the clock runs out.
fn select_taste<'a>(
    pending: &'a [cog_core::StoredTasteIntent],
    filed: &'a [cog_core::StoredTasteIntent],
    tasks: &HashMap<&str, &Task>,
    state: &SignalGuardState,
    cooldown_secs: i64,
    max: usize,
    now: DateTime<Utc>,
) -> SelectedTaste<'a> {
    let unfinished: Vec<&cog_core::StoredTasteIntent> = filed
        .iter()
        .filter(|stored| {
            stored
                .task_id
                .as_deref()
                .and_then(|id| tasks.get(id))
                .is_some_and(|task| task.status == TaskStatus::Failed)
        })
        .collect();

    let mut announcements = Vec::new();
    let mut superseded = Vec::new();
    for stored in pending {
        if cooldown_elapsed(state, &taste_signal_key(&stored.intent), cooldown_secs, now) {
            announcements.push(stored);
        } else {
            superseded.push(stored);
        }
    }

    SelectedTaste {
        selected: unfinished
            .into_iter()
            .chain(announcements)
            .take(max)
            .collect(),
        superseded,
    }
}

/// What this tick should do about a signal, given whether a task for it is
/// already in the store.
#[derive(Debug, PartialEq, Eq)]
enum IntentAction {
    /// No task yet — submit one.
    Submit,
    /// A previous attempt ended in failure while the signal is still present —
    /// reset it so it runs again.
    Redrive,
    /// A previous attempt already finished while the signal is still present —
    /// the terminal row has to be cleared before the work can be filed again.
    Resubmit,
    /// A task exists and has not finished — leave it alone.
    None,
}

/// A finished row and an in-flight row look the same to the orchestrator --
/// both hold the id, so submitting again is a duplicate -- but they mean
/// opposite things about the signal. In flight, the signal is being worked on
/// and nothing should be submitted. Finished, the attempt is over and the
/// signal is still firing, so the only thing standing between the signal and a
/// new attempt is the row itself: `retry_task` takes a failed task and nothing
/// else, so a finished one has to be cleared rather than re-driven.
///
/// `may_redrive` is false on a round the LLM pool gate holds off, and it turns
/// exactly the two driving answers into "leave the row alone". The distinction
/// it draws is between the two things a round can do with a signal: file it,
/// which spends no LLM call and is the only evidence that the signal was taken
/// up at all, and drive it again, which buys one refusing 503 per agent for
/// work the pool cannot run. A held-off round still does the first; it does not
/// do the second, and a row it leaves alone is picked up by the first round
/// after the pool is back rather than by every round in between.
fn intent_action(existing: Option<&TaskStatus>, may_redrive: bool) -> IntentAction {
    match existing {
        None => IntentAction::Submit,
        Some(TaskStatus::Failed) if may_redrive => IntentAction::Redrive,
        Some(TaskStatus::Completed | TaskStatus::Cancelled) if may_redrive => {
            IntentAction::Resubmit
        }
        Some(_) => IntentAction::None,
    }
}

/// The id a self-audit intent is filed under, stamped with the day it was due.
///
/// The stamp is what keeps the audit out of the re-drive premise the signal keys
/// live under: those re-drive a failed task on the next door, which is why the
/// store has to hold a failed row for at least one cooldown. An audit's own wait
/// is a week, and a stable id would put it behind a premise whose record dies
/// long before the next audit — so each audit is its own task, and an audit that
/// failed is scheduled again rather than re-driven.
fn self_audit_task_id(now: DateTime<Utc>) -> String {
    format!("self-audit-{}", now.format("%Y%m%d"))
}

/// What a signal-driven intent's project id starts with.
///
/// The project listing calls every non-empty id a goal, so this prefix is what
/// lets a reader tell a project the watcher opened from a goal somebody
/// submitted.
const SIGNAL_PROJECT_PREFIX: &str = "signal:";

/// The project a signal-driven intent belongs to: the condition that keeps
/// asking for it.
///
/// A project per task says nothing -- the board, the rollup and earned value all
/// read sums, and a sum over one row is that row. The signal's own vocabulary
/// already names what recurs: an alert is identified by its rule (the subject
/// varies, the rule is what fires again), a recurring failure by its signature,
/// and everything else by the kind of condition it is. Those are the three
/// groupings below, in the order the detail is read.
///
/// `None` when the detail names no kind at all, which leaves the task exactly as
/// it was submitted -- the one case where this file cannot say what the work is
/// about, and guessing would file it under a project nobody asked for.
fn signal_project_key(detail: &serde_json::Value) -> Option<String> {
    let kind = detail.get("kind")?.as_str()?.trim();
    if kind.is_empty() {
        return None;
    }
    let subject = ["rule", "signature"]
        .into_iter()
        .filter_map(|key| detail.get(key).and_then(|v| v.as_str()))
        .map(str::trim)
        .find(|v| !v.is_empty());
    Some(match subject {
        Some(subject) => format!("{SIGNAL_PROJECT_PREFIX}{kind}/{subject}"),
        None => format!("{SIGNAL_PROJECT_PREFIX}{kind}"),
    })
}

/// 一条信号行说的「做完」是什么意思，用这一侧自己读得到的量来写。
///
/// 没有验收标准的行是一个谁也关不掉的活：项目视图画得出它，但画不出「够了没有」，
/// 挣值那一格永远不合上。所以这一格不是给视图的装饰，它是这条活唯一的止损条件。
///
/// 判据一律取**造这条行的那次读数**：告警是这一条规则不再出现在 firing 集合里，
/// 复发失败是同一张窗里同一个签名不再出现，积压是同一个阈值读数降到线以下。
/// 这样清掉它的读者和造出它的读者是同一个，两边不可能各自漂。
///
/// 落不到具体种类时留下的那句「下一轮不再为它建行」不是循环论证：这一侧「没有新行」
/// 是「条件不再问」的定义，而这一侧还活着这件事由常驻循环自己的活性门看着，
/// 所以「观察器停了」不会被读成「条件结束了」。
fn signal_acceptance(detail: &serde_json::Value, project: Option<&str>) -> String {
    let kind = detail
        .get("kind")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim();
    let text = |key: &str| {
        detail
            .get(key)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_string()
    };
    let number = |key: &str| {
        detail
            .get(key)
            .and_then(|value| value.as_u64())
            .unwrap_or(0)
    };
    match kind {
        "persisted_alert" => format!(
            "告警「{}」不再处于 firing：下一轮读告警存储时它不在 firing 集合里",
            text("rule")
        ),
        "failure_recurrence" => format!(
            "签名「{}」在 {} 秒的失败窗里不再出现：同一张窗按签名分组，这一组为空",
            text("signature"),
            number("window_secs")
        ),
        "queue_backlog" => format!(
            "待办与在飞不再超过 {}：本文件每轮报的那个待办读数",
            number("threshold")
        ),
        // This one is not a condition that stops asking: the audit is filed
        // every period whether or not the last one found anything, so "it did
        // not ask again" can never be its completion. What closes it is the
        // change it was supposed to produce, read where changes land.
        SELF_AUDIT_SIGNAL_KIND => format!(
            "{} 这一期自审点名的缺陷在账上有一条落地的变更：账读得到这次自审的成果，\
             而不是任务跑完就算",
            text("period")
        ),
        _ => format!(
            "条件「{}」不再成立：下一轮不再为它建行",
            project.unwrap_or(kind)
        ),
    }
}

/// The task kind of the row that anchors one signal's project.
///
/// Not [`SIGNAL_PROJECT_PREFIX`]'s kind: a reader counting signal work must not
/// count the anchor, and the anchor is not work — nothing reads it as a signal
/// and the queue must never start it.
pub const SIGNAL_PROJECT_ROOT_KIND: &str = "self_signal_project";

/// What a signal project's anchor id starts with.
///
/// Derived from the project key, so the same project always names the same
/// anchor: a round that finds the row already there writes nothing, and a
/// project never opens a second root.
const SIGNAL_PROJECT_ROOT_PREFIX: &str = "signal-project:";

/// The row that anchors one signal's project.
///
/// A project is the group of rows under one `goal_id` whose root is the row with
/// no parent, and the root is where replanning hangs new work. The watcher files
/// one row per round for as long as a condition keeps firing, so without an
/// anchor the group grows a new root every round — and replanning refuses a
/// group with two roots outright rather than guessing which one to hang under.
/// Measured on 2026-10-30: 9 signal projects held two or more roots, 64 rows in
/// all, the largest with 41.
///
/// A stable anchor rather than "hang it under whichever row is the root today":
/// a finished row is deleted when the signal is resubmitted, so a group whose
/// root is one of its own rows loses the parent of everything under it at the
/// first resubmission. The anchor is submitted with every filing and is never a
/// signal, so it is never the row that gets cleared.
fn signal_project_root(project: &str) -> Task {
    let mut root = Task::new(
        signal_project_root_id(project),
        TaskType::Custom(SIGNAL_PROJECT_ROOT_KIND.into()),
        serde_json::json!({
            "goal": format!("signals filed under {project}"),
            (cog_core::INPUT_TASK_KIND): SIGNAL_PROJECT_ROOT_KIND,
        }),
    );
    root.goal_id = Some(project.to_string());
    root.parent_task_id = None;
    // The anchor is where work hangs, not work: nothing ever runs it.
    root.is_executable = false;
    root
}

/// The signal kind the periodic self-audit is filed under.
///
/// It shares the filing path with the alert and failure signals but names no
/// condition, so it has no project of its own -- the criterion back-fill finds
/// it by this kind rather than by a project prefix.
const SELF_AUDIT_SIGNAL_KIND: &str = "self_audit";

fn signal_project_root_id(project: &str) -> String {
    let scope = project
        .strip_prefix(SIGNAL_PROJECT_PREFIX)
        .unwrap_or(project);
    format!("{SIGNAL_PROJECT_ROOT_PREFIX}{scope}")
}

/// How many rows one settle is allowed to rewrite.
///
/// The first settle after a filing starts writing a criterion has a thousand
/// rows to reach, and this round also reads the whole graph every minute. A
/// batch bounds the burst, not the work: the round asks again on its next tick
/// until `remaining` is zero.
// cap: bound — 界的是这一轮的突发量，不是工作量；下一拍会接着问直到 remaining 归零
const SIGNAL_SETTLE_BATCH: usize = 250;

/// What one settle of the signal projects did, cell by cell.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SettledSignals {
    anchored: usize,
    reparented: usize,
    criteria: usize,
    /// Criterion back-filled onto a rework row this process filed.
    rework_criteria: usize,
    failed: usize,
    /// Rows still out of shape after this settle. Zero means the next tick has
    /// nothing left to do and the round can stop asking.
    remaining: usize,
}

impl SettledSignals {
    fn moved_anything(&self) -> bool {
        self.anchored > 0
            || self.reparented > 0
            || self.criteria > 0
            || self.rework_criteria > 0
            || self.failed > 0
    }

    fn line(&self) -> String {
        format!(
            "settled filings: anchored={} reparented={} criteria={} rework_criteria={} \
             failed={} remaining={}",
            self.anchored,
            self.reparented,
            self.criteria,
            self.rework_criteria,
            self.failed,
            self.remaining
        )
    }
}

/// Put the signal projects written before the anchor existed back into the shape
/// a filing now writes.
///
/// A filing only fixes the group it touches, and a condition that has stopped
/// firing is never touched again -- so the rows written before the anchor
/// existed would keep their extra roots for the life of the graph.
///
/// A project holding one root is in shape whichever row that root is: the single
/// filing it holds, or the anchor. Only a second root is a refusal, so only a
/// project with a second root is rewritten; a group that is already whole costs
/// no write.
///
/// Called until it stops finding anything to fill rather than on every tick
/// forever. Every row written after this is in shape by construction, and this
/// round already reads the whole graph every minute: reading it again would be
/// a cost paid forever for a defect that can no longer recur. The stop is the
/// settle's own `remaining` count, because how many ticks the back-fill takes
/// depends on how many rows the graph holds.
async fn settle_signal_parents(
    orch: &Arc<dyn OrchestratorControl>,
    tasks: &[Task],
) -> SettledSignals {
    let mut settled = SettledSignals::default();
    let mut projects: HashMap<&str, Vec<&Task>> = HashMap::new();
    for task in tasks {
        if let Some(project) = task
            .goal_id
            .as_deref()
            .filter(|project| project.starts_with(SIGNAL_PROJECT_PREFIX))
        {
            projects.entry(project).or_default().push(task);
        }
    }
    for (project, rows) in projects {
        let anchor_id = signal_project_root_id(project);
        let anchored = rows.iter().any(|row| row.id == anchor_id);
        let strays: Vec<&Task> = rows
            .iter()
            .copied()
            .filter(|row| row.parent_task_id.is_none() && row.id != anchor_id)
            .collect();
        // One root and no anchor is a project of exactly one filing, which is in
        // shape: writing an anchor there would add a row to every quiet signal
        // in the graph and change nothing about the tree.
        if strays.is_empty() || (!anchored && strays.len() < 2) {
            continue;
        }
        if !anchored {
            match orch.add_task(signal_project_root(project)).await {
                Ok(()) => settled.anchored += 1,
                Err(e) => {
                    settled.failed += 1;
                    warn!(
                        project,
                        error = %e,
                        "signal watcher: a signal project's anchor could not be written, so the \
                         group keeps the roots it was written with"
                    );
                    continue;
                }
            }
        }
        for row in strays {
            let mut hung = row.clone();
            hung.parent_task_id = Some(anchor_id.clone());
            match orch.replace_task(hung).await {
                Ok(()) => settled.reparented += 1,
                Err(e) => {
                    settled.failed += 1;
                    warn!(
                        project,
                        task = %row.id,
                        error = %e,
                        "signal watcher: a signal row could not be hung under its project's \
                         anchor, so the group still holds it as a root"
                    );
                }
            }
        }
    }
    fill_signal_criteria(orch, tasks, &mut settled).await;
    settled
}

/// Give every signal row written before the criterion existed the one it would
/// be filed with today.
///
/// The filing only fixes the row it submits, and a condition that has stopped
/// asking is never filed again -- so the rows written before the criterion
/// existed would keep an empty one for the life of the graph. Measured on
/// 2026-10-30: 1194 signal rows, none of them carrying a criterion.
///
/// The criterion is re-derived from the detail the row kept in its own payload,
/// not from the round's live reading: a row filed by an alert says what closes
/// that alert, and re-deriving it from today's reading would write a criterion
/// about a condition the row was not filed for. Rows that kept no detail are
/// left alone -- there is nothing to say about them, and a guess would be a
/// criterion that admits no answer.
async fn fill_signal_criteria(
    orch: &Arc<dyn OrchestratorControl>,
    tasks: &[Task],
    settled: &mut SettledSignals,
) {
    let candidates: Vec<&Task> = tasks
        .iter()
        .filter(|task| task.acceptance.is_empty())
        .filter(|task| task.input.get("signal").is_some())
        .filter(|task| {
            task.goal_id
                .as_deref()
                .is_some_and(|project| project.starts_with(SIGNAL_PROJECT_PREFIX))
                // The periodic audit is filed by this same path but names no
                // condition, so it gets no project: it is still one of this
                // process's filings and still owes a criterion.
                || task.input["signal"].get("kind").and_then(|k| k.as_str())
                    == Some(SELF_AUDIT_SIGNAL_KIND)
        })
        .collect();
    settled.remaining = candidates.len().saturating_sub(SIGNAL_SETTLE_BATCH);
    for row in candidates.into_iter().take(SIGNAL_SETTLE_BATCH) {
        // The graph was read once, before this loop, and a worker can move a
        // row between then and now. Written back from that snapshot the row
        // would lose the status it just reached -- and the parent the settle
        // above just gave it. So the row is read again here, and one that is
        // running this moment is left to the next tick rather than written over.
        // `replace_task` writes the whole row, so there is no narrower write to
        // fall back on.
        let Some(mut filled) = orch.get_task(&row.id).await else {
            continue;
        };
        if !filled.acceptance.is_empty() {
            continue;
        }
        if filled.status == TaskStatus::Running {
            settled.remaining += 1;
            continue;
        }
        let detail = filled.input["signal"].clone();
        filled
            .acceptance
            .push(signal_acceptance(&detail, filled.goal_id.as_deref()));
        match orch.replace_task(filled).await {
            Ok(()) => settled.criteria += 1,
            Err(e) => {
                settled.failed += 1;
                settled.remaining += 1;
                warn!(
                    task = %row.id,
                    error = %e,
                    "signal watcher: a signal row could not be given its criterion, so it stays \
                     a work package nobody can close"
                );
            }
        }
    }
}

/// Give the rework rows filed before their criterion existed the one they would
/// be filed with today.
///
/// Same defect as the signal rows, one kind over: the hand-off now writes the
/// criterion, but a requirement filed before it did keeps an empty one for the
/// life of the graph -- the learning that filed it is retired on the verdict,
/// so nothing files that requirement again.
///
/// Re-derived from the payload the row kept (the cause and the files the gate
/// pointed at), through the same function the hand-off writes with. A row that
/// kept neither is left alone: there is no honest criterion to write, and a
/// guess would close the row on a question nobody asked.
async fn fill_rework_criteria(
    orch: &Arc<dyn OrchestratorControl>,
    tasks: &[Task],
    settled: &mut SettledSignals,
) {
    let candidates: Vec<&Task> = tasks
        .iter()
        .filter(|task| task.acceptance.is_empty())
        .filter(|task| task.task_type.kind_key() == crate::change_rework::REWORK_TASK_KIND)
        .collect();
    settled.remaining += candidates.len().saturating_sub(SIGNAL_SETTLE_BATCH);
    for row in candidates.into_iter().take(SIGNAL_SETTLE_BATCH) {
        // Read again before writing, for the same reason as the signal rows: the
        // snapshot is a minute old and `replace_task` writes the whole row.
        let Some(mut filled) = orch.get_task(&row.id).await else {
            continue;
        };
        if !filled.acceptance.is_empty() {
            continue;
        }
        if filled.status == TaskStatus::Running {
            settled.remaining += 1;
            continue;
        }
        let cause = filled
            .input
            .get("rejection_cause")
            .and_then(|cause| cause.as_str())
            .map(str::to_string);
        let files: Vec<String> = filled
            .input
            .get("named_files")
            .and_then(|files| files.as_array())
            .map(|files| {
                files
                    .iter()
                    .filter_map(|file| file.as_str())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if cause.is_none() && files.is_empty() {
            continue;
        }
        let surface = if files.is_empty() {
            "这次被拒的那个面".to_string()
        } else {
            files.join("、")
        };
        filled
            .acceptance
            .push(crate::change_rework::rework_acceptance(
                cause.as_deref(),
                &surface,
            ));
        match orch.replace_task(filled).await {
            Ok(()) => settled.rework_criteria += 1,
            Err(e) => {
                settled.failed += 1;
                settled.remaining += 1;
                warn!(
                    task = %row.id,
                    error = %e,
                    "signal watcher: a rework row could not be given its criterion, so it stays \
                     a work package nobody can close"
                );
            }
        }
    }
}

/// Everything one round settles: the signal projects, and the rows this
/// process files that were written before their kind wrote a criterion.
///
/// One entry point because the caller has one flag to keep: a round that still
/// has rows left to settle asks again on its next tick, and it must not stop
/// asking just because one of the two halves finished.
async fn settle_filings(orch: &Arc<dyn OrchestratorControl>, tasks: &[Task]) -> SettledSignals {
    let mut settled = settle_signal_parents(orch, tasks).await;
    fill_rework_criteria(orch, tasks, &mut settled).await;
    settled
}

/// Result of one submission attempt. "Nothing needed doing" and "the intent
/// never reached the orchestrator" must stay distinct: conflating them makes an
/// idempotent hit look like a broken channel, and a broken channel look like
/// work already in hand.
#[derive(Debug)]
enum IntentOutcome {
    /// A new task was created.
    Registered,
    /// A task for this signal is already in hand and has not failed; no action
    /// taken and no cooldown spent, so the next tick still notices if it fails.
    Tracked,
    /// A failed attempt was reset and will run again.
    Redriven,
    /// The earlier attempt had already finished, so its row was cleared and the
    /// work filed as a new task.
    Resubmitted,
    /// Nothing was registered (orchestrator unavailable); retry next tick.
    Failed(String),
}

/// Submit one internal evolution intent, or drive the existing one forward.
/// The id is deterministic, so reading the task store first is what makes
/// repeats across ticks and pod restarts idempotent — the orchestrator's
/// self-evolution routing raises on a duplicate id instead of skipping it.
async fn submit_intent(
    orch: &Arc<dyn OrchestratorControl>,
    task_id: String,
    task_kind: &str,
    goal: String,
    detail: serde_json::Value,
    may_redrive: bool,
) -> IntentOutcome {
    let existing = orch.get_task(&task_id).await;
    let action = intent_action(existing.as_ref().map(|t| &t.status), may_redrive);
    let resubmitting = action == IntentAction::Resubmit;
    match action {
        IntentAction::None => IntentOutcome::Tracked,
        IntentAction::Redrive => match orch.retry_task(&task_id).await {
            Ok(()) => IntentOutcome::Redriven,
            Err(e) => IntentOutcome::Failed(format!("retry {task_id}: {e}")),
        },
        IntentAction::Submit | IntentAction::Resubmit => {
            // The finished row goes first, and the delete is checked rather
            // than assumed: if it fails, submitting under the same id raises on
            // the duplicate every tick after this one, so the attempt is
            // reported as failed and retried with the row still there.
            if resubmitting {
                if let Err(e) = orch.delete_task(&task_id).await {
                    return IntentOutcome::Failed(format!("delete {task_id}: {e}"));
                }
                info!(
                    task_id = %task_id,
                    "cleared a finished task for a signal that is still firing"
                );
            }
            // Read the project off the detail before the payload takes it:
            // `json!` moves the value in, so a key derived afterwards would be
            // derived from nothing.
            let project = signal_project_key(&detail);
            // What "done" means for this row, taken from the reading that filed
            // it: see `signal_acceptance`. A leaf carrying none is work nobody
            // can close.
            let acceptance = signal_acceptance(&detail, project.as_deref());
            let mut task = Task::new(
                task_id,
                TaskType::Custom(task_kind.into()),
                serde_json::json!({
                    "goal": goal,
                    (cog_core::INPUT_EVOLUTION_MODE): cog_core::EVOLUTION_MODE_GENERATE_CHANGE,
                    (cog_core::INPUT_TASK_KIND): task_kind,
                    "signal": detail,
                }),
            );
            // The same defect the change ledger's handover had: the goal path
            // mints one id per submission when the task carries none, and this
            // watcher submits one task at a time, so every signal became a
            // project of exactly one task. Measured on 2026-10-27: 1314
            // self-signal rows, 1314 distinct goal ids. Grouping them by the
            // condition that keeps asking is what turns "one thousand and
            // fourteen projects" into "the alert rules that are firing".
            task.goal_id = project.clone();
            task.acceptance.push(acceptance);
            // And the row hangs under the project's anchor rather than being a
            // root of its own -- a group whose every round is a root is a group
            // replanning refuses. See `signal_project_root`.
            let mut filing = Vec::with_capacity(2);
            if let Some(project) = &project {
                task.parent_task_id = Some(signal_project_root_id(project));
                filing.push(signal_project_root(project));
            }
            filing.push(task);
            match orch.submit_goal_auto(&goal, filing).await {
                Ok(_) if resubmitting => IntentOutcome::Resubmitted,
                Ok(_) => IntentOutcome::Registered,
                Err(e) => IntentOutcome::Failed(e.to_string()),
            }
        }
    }
}

/// Account one attempt against the signal's cooldown. Only attempts that
/// actually moved the signal forward spend the cooldown; a submission that
/// never landed leaves the key unrecorded so the next tick retries, and an
/// in-flight task stays unrecorded so a failure is noticed promptly rather
/// than after a full cooldown. A re-submission did move the signal forward --
/// a new attempt is running and the finished row is gone -- so it spends the
/// cooldown like any other submission. Returns true when the signal is in hand.
fn report_outcome(
    state: &mut SignalGuardState,
    key: &str,
    outcome: IntentOutcome,
    now: DateTime<Utc>,
    readings: &SignalWatcherReadings,
) -> bool {
    // Counted here rather than at the call sites: every path out of a
    // submission attempt comes through this function, so this is the one place
    // where an arm cannot be added without being counted.
    readings.record(match &outcome {
        IntentOutcome::Registered => SignalOutcome::Registered,
        IntentOutcome::Tracked => SignalOutcome::Tracked,
        IntentOutcome::Redriven => SignalOutcome::Redriven,
        IntentOutcome::Resubmitted => SignalOutcome::Resubmitted,
        IntentOutcome::Failed(_) => SignalOutcome::Failed,
    });
    match outcome {
        IntentOutcome::Registered => {
            state.reported.insert(key.to_string(), now);
            info!(signal = %key, "self-discovery intent submitted");
            true
        }
        IntentOutcome::Redriven => {
            state.reported.insert(key.to_string(), now);
            info!(signal = %key, "self-discovery intent re-driven; the previous attempt had failed");
            true
        }
        IntentOutcome::Resubmitted => {
            state.reported.insert(key.to_string(), now);
            info!(signal = %key, "self-discovery intent re-submitted; the previous attempt had finished");
            true
        }
        IntentOutcome::Tracked => {
            debug!(signal = %key, "self-discovery intent already tracked; nothing to submit");
            true
        }
        IntentOutcome::Failed(e) => {
            warn!(signal = %key, error = %e, "self-discovery intent not registered; retrying next tick");
            false
        }
    }
}

/// One watcher tick: scan task state, emit intents for active signals.
async fn tick(
    orch: &Arc<dyn OrchestratorControl>,
    config: &SignalWatcherConfig,
    alert_source: Option<&Arc<dyn cog_core::ActiveAlertSource>>,
    taste_source: Option<&Arc<dyn cog_core::TasteIntentSource>>,
    readings: &SignalWatcherReadings,
    held: bool,
    parents_settled: &mut bool,
) {
    // Stamped before anything is read, so a round that fails or finds nothing
    // still counts as a round: the tick count is what the signal counts are
    // read against, and a denominator that only moves on busy rounds would make
    // a broken watcher look like a quiet system.
    readings.tick();
    let now = Utc::now();
    let tasks = orch.get_all_tasks().await;
    // Until it stops finding anything to fill, on the graph this round already
    // read: see `settle_filings`. Before the held-round return, because shaping
    // the tree spends no LLM call and a pool outage is no reason to leave a
    // project replanning cannot hang work under.
    if !*parents_settled {
        let settled = settle_filings(orch, &tasks).await;
        if settled.moved_anything() {
            info!("signal watcher: {}", settled.line());
        }
        // A settle that left rows to fill asks again on the next tick; one that
        // reached everything is done for the life of the process.
        *parents_settled = settled.remaining == 0;
    }
    let mut state = load_state().await;
    let mut dirty = false;

    // Reclaimed before anything is read, so a key whose cooldown has run out is
    // gone from the round that would have been permitted to report it again --
    // the removal and the permission are the same event, and doing it later
    // would leave the store holding an entry whose only meaning had expired.
    let reclaimed = prune_elapsed(&mut state, config.report_cooldown_secs, now);
    if reclaimed > 0 {
        // Written even when the round found nothing, so the file shrinks as
        // entries expire instead of waiting for the next signal to be carried
        // along with it.
        dirty = true;
    }

    // A round the pool gate holds off is not an empty round: it takes the
    // firing alerts in and does nothing else.
    //
    // The intake is bookkeeping. It reads the alert store and writes one task
    // row per firing alert, and it spends no LLM call — the row is the only
    // thing that says an alert has been taken up, and the reader that asks
    // whether a firing alert is attended reads it. Standing the whole round
    // down is what left that reader with nothing to find for the whole of a
    // multi-day pool outage, measured on 2026-10-27: five rounds ran, five
    // stood down, every outcome counter read zero, and twenty rules were firing
    // with no row between them. The gate exists to stop buying refusing 503s,
    // and no 503 is bought by a row.
    //
    // Every other branch here asks for a fix, and a fix is exactly what the
    // pool cannot run: filed now, each one becomes a task that fails on the
    // same 503 the gate was added to stop, which is the pulse the gate came
    // from in the first place. So they wait for the pool rather than for the
    // clock, and `may_redrive` is false for the intake too — a row that already
    // failed stays failed until there is a pool to drive it again.
    if held {
        readings.held_round();
        if config.alert_channel_enabled {
            if let Some(source) = alert_source {
                take_alerts_in(
                    orch, config, source, &tasks, &mut state, &mut dirty, now, readings, false,
                )
                .await;
            }
        }
        publish_guard_store(&state, readings, reclaimed);
        if dirty {
            save_state(&state).await;
        }
        return;
    }

    // 1. Failure recurrence among self-evolution tasks.
    let window_start = now - chrono::Duration::seconds(config.failure_window_secs);
    let mut by_signature: HashMap<String, Vec<&Task>> = HashMap::new();
    for task in &tasks {
        if task.status != TaskStatus::Failed || task.updated_at < window_start {
            continue;
        }
        let is_evolution = task
            .input
            .get(cog_core::INPUT_EVOLUTION_MODE)
            .and_then(|v| v.as_str())
            == Some(cog_core::EVOLUTION_MODE_GENERATE_CHANGE);
        if !is_evolution {
            continue;
        }
        let sig = error_signature(task.error.as_deref().unwrap_or_default());
        by_signature.entry(sig).or_default().push(task);
    }
    for (sig, group) in &by_signature {
        if group.len() < config.failure_recurrence_threshold {
            continue;
        }
        let key = format!("failure:{sig}");
        if !cooldown_elapsed(&state, &key, config.report_cooldown_secs, now) {
            // A recurring failure that is being throttled is still a signal.
            // Leaving this uncounted would file it under "nothing was wrong",
            // which is the opposite of what the failure count says.
            readings.record(SignalOutcome::Cooldown);
            continue;
        }
        dirty = true;
        let hash = short_hash(&key);
        let sample_ids: Vec<&str> = group.iter().take(5).map(|t| t.id.as_str()).collect();
        // The signature leads: it is what tells one recurring failure from
        // another in a shortened header.
        let goal = format!(
            "Fix recurring failure \"{sig}\"\n\nInvestigate recurring self-evolution failure: \
             signature \"{sig}\" occurred {} times in the last {} seconds (sample tasks: {}). \
             Read the failed tasks' inputs and errors, find the shared root \
             cause, and implement a fix.",
            group.len(),
            config.failure_window_secs,
            sample_ids.join(", ")
        );
        let outcome = submit_intent(
            orch,
            format!("self-signal-failure-{hash}"),
            "self_signal",
            goal,
            serde_json::json!({
                "kind": "failure_recurrence",
                "signature": sig,
                "occurrences": group.len(),
                "window_secs": config.failure_window_secs,
                "sample_task_ids": sample_ids,
            }),
            true,
        )
        .await;
        report_outcome(&mut state, &key, outcome, now, readings);
    }

    // 2. Queue backlog anomaly.
    let backlog = tasks
        .iter()
        .filter(|t| matches!(t.status, TaskStatus::Pending | TaskStatus::Scheduled))
        .count();
    if backlog >= config.backlog_threshold {
        let key = "backlog".to_string();
        if !cooldown_elapsed(&state, &key, config.report_cooldown_secs, now) {
            readings.record(SignalOutcome::Cooldown);
        } else {
            dirty = true;
            let dlq = orch.dlq_len().await.unwrap_or(0);
            let goal = format!(
                "Drain the queue backlog\n\nInvestigate orchestrator queue backlog: {backlog} \
                 tasks pending/scheduled (DLQ: {dlq}). Determine why tasks are not \
                 draining and fix the bottleneck."
            );
            let outcome = submit_intent(
                orch,
                "self-signal-backlog".to_string(),
                "self_signal",
                goal,
                serde_json::json!({
                    "kind": "queue_backlog",
                    "backlog": backlog,
                    "dlq": dlq,
                    "threshold": config.backlog_threshold,
                }),
                true,
            )
            .await;
            report_outcome(&mut state, &key, outcome, now, readings);
        }
    }

    // 3. Periodic self-audit of the own repository.
    if config.self_audit_interval_secs > 0 {
        let due = match state.last_audit {
            Some(last) => (now - last).num_seconds() >= config.self_audit_interval_secs,
            None => true,
        };
        if due {
            dirty = true;
            let key = self_audit_task_id(now);
            let goal = "Audit the repository for security defects\n\nAudit this repository for \
                        security and reliability defects, going over: hardcoded secrets or weak \
                        default credentials, injection surfaces (command/SQL/XSS), unsafe \
                        dependency or configuration patterns, and error paths that silently \
                        swallow failures. Pick the single most critical confirmed finding and \
                        implement its fix; list the remaining findings in the change \
                        description."
                .to_string();
            let period = now.format("%Y%m%d").to_string();
            let outcome = submit_intent(
                orch,
                key.clone(),
                SELF_AUDIT_SIGNAL_KIND,
                goal,
                serde_json::json!({
                    "kind": SELF_AUDIT_SIGNAL_KIND,
                    "period": period,
                }),
                true,
            )
            .await;
            // 节拍只在审计任务确实在手时才推进：提交没落地就撤销本轮，
            // 下一轮重试，否则一次编排器抖动会让审计整整晚一个周期。
            if report_outcome(&mut state, &key, outcome, now, readings) {
                state.last_audit = Some(now);
            }
        }
    }

    // 4. Persisted firing alerts → intents. Alerts are how faults below the
    // task layer (node disk pressure, crash loops, pool outages) surface;
    // each firing alert becomes an evolution intent keyed by its dedup key,
    // so the fix work is tracked and cooled down like any other signal.
    if config.alert_channel_enabled {
        if let Some(source) = alert_source {
            take_alerts_in(
                orch, config, source, &tasks, &mut state, &mut dirty, now, readings, true,
            )
            .await;
        }
    }

    // 5. Taste intents submitted from outside the pipeline. This is the one
    // channel whose input is not the system's own state: a judgement about
    // whether a result holds up, which direction is worth more, or what the
    // standard should be. Every other channel here can only report that
    // something hurts.
    if config.taste_channel_enabled {
        if let Some(source) = taste_source {
            // A failed read on either side leaves this round with no taste work
            // and touches no row: "the store did not answer" must not arrive as
            // "nothing was submitted", because the rows it could not read are
            // still there and the next round is the one that can act on them.
            let pending = source
                .pending_intents(config.taste_channel_max_per_tick as i64)
                .await;
            // The window is how far back unfinished taste work is looked for,
            // and it is measured from when the work was handed over: a
            // submission that sat unclaimed through an outage is filed late,
            // and its failure has to be findable on the rounds after that
            // filing rather than on the rounds after the submission.
            let filed = source
                .filed_intents(
                    now - chrono::Duration::seconds(config.report_cooldown_secs),
                    config.taste_channel_max_per_tick as i64,
                )
                .await;
            match (pending, filed) {
                (Some(pending), Some(filed)) => {
                    let by_id: HashMap<&str, &Task> =
                        tasks.iter().map(|t| (t.id.as_str(), t)).collect();
                    let SelectedTaste {
                        selected,
                        superseded,
                    } = select_taste(
                        &pending,
                        &filed,
                        &by_id,
                        &state,
                        config.report_cooldown_secs,
                        config.taste_channel_max_per_tick,
                        now,
                    );
                    // Repeats are closed, not postponed. A submission held back
                    // because a judgement of the same kind about the same
                    // subject is already in hand is evidence that someone
                    // agrees with work that is already filed — kept, and never
                    // a second task. Left pending, the same row would be read
                    // again once the cooldown ran out and would then file a
                    // duplicate of work that had already been done.
                    for stored in superseded {
                        readings.record(SignalOutcome::Cooldown);
                        if let Err(e) = source
                            .dispose(
                                stored.intent.id,
                                cog_core::TasteDisposition::Superseded,
                                None,
                            )
                            .await
                        {
                            warn!(
                                id = %stored.intent.id,
                                error = %e,
                                "superseded taste submission not closed; it will be read again next round"
                            );
                        }
                    }
                    for stored in selected {
                        dirty = true;
                        let intent = &stored.intent;
                        let key = taste_signal_key(intent);
                        let task_id = taste_task_id(intent);
                        // 这条提交的活已经跑完过一次，缺的只是它那一行的去向
                        // （进程死在提交与写去向之间）。交给 submit_intent 会走
                        // 「清掉终态行再提一个新的」那条路，同一份一次性判定就
                        // 会被做第二遍。
                        if by_id.get(task_id.as_str()).is_some_and(|t| {
                            matches!(t.status, TaskStatus::Completed | TaskStatus::Cancelled)
                        }) {
                            state.reported.insert(key, now);
                            if let Err(e) = source
                                .dispose(
                                    intent.id,
                                    cog_core::TasteDisposition::Filed,
                                    Some(&task_id),
                                )
                                .await
                            {
                                warn!(
                                    id = %intent.id,
                                    error = %e,
                                    "finished taste submission not closed; it will be read again next round"
                                );
                            }
                            continue;
                        }
                        let (goal, detail) = taste_intent(intent);
                        let outcome =
                            submit_intent(orch, task_id.clone(), "self_signal", goal, detail, true)
                                .await;
                        // Only a submission that reached the task layer gets a
                        // fate written down: one that never got there stays
                        // pending and is retried, which is also what makes the
                        // retry idempotent — the second attempt reads the task
                        // store and finds the work already in hand.
                        if report_outcome(&mut state, &key, outcome, now, readings) {
                            if let Err(e) = source
                                .dispose(
                                    intent.id,
                                    cog_core::TasteDisposition::Filed,
                                    Some(&task_id),
                                )
                                .await
                            {
                                warn!(
                                    id = %intent.id,
                                    error = %e,
                                    "filed taste submission not marked; its task may be filed again"
                                );
                            }
                        }
                    }
                }
                _ => {
                    warn!("taste intent store unreadable; no taste work this round");
                }
            }
        }
    }

    // The store's own size is published every round, whatever this round did:
    // it is the only reading that says whether keys are accumulating, and a
    // store that only ever grows is how it went unnoticed for weeks. It is a
    // gauge of the live map rather than a count of insertions, so it comes back
    // down when the reclaimer runs.
    publish_guard_store(&state, readings, reclaimed);

    if dirty {
        save_state(&state).await;
    }
}

/// Take the firing alerts in: one task row per alert, keyed by its dedup key.
///
/// This is the half of a round that is bookkeeping, and it is a function of its
/// own because a round the pool gate holds off still runs it. `may_redrive` is
/// what it does with an alert whose row already failed: driven again on an open
/// pool, and left alone on a closed one.
#[allow(clippy::too_many_arguments)]
async fn take_alerts_in(
    orch: &Arc<dyn OrchestratorControl>,
    config: &SignalWatcherConfig,
    source: &Arc<dyn cog_core::ActiveAlertSource>,
    tasks: &[Task],
    state: &mut SignalGuardState,
    dirty: &mut bool,
    now: DateTime<Utc>,
    readings: &SignalWatcherReadings,
    may_redrive: bool,
) {
    // A failed read yields no intents for this tick, and the cooldown
    // bookkeeping is left untouched so the next tick retries with the
    // same slate. It also must not be read as "the alerts cleared":
    // nothing here concludes anything from absence.
    let Some(alerts) = source.list_active_alerts(100).await else {
        return;
    };
    let SelectedAlerts {
        selected,
        in_hand,
        in_cooldown,
    } = select_alerts(
        &alerts,
        tasks,
        state,
        config.alert_channel_max_per_tick,
        config.report_cooldown_secs,
        now,
        may_redrive,
    );
    // Firing alerts held back by their cooldown are signals too,
    // and the count is taken here where the predicate that held
    // them back is applied, rather than re-derived by a reader.
    for _ in 0..in_cooldown {
        readings.record(SignalOutcome::Cooldown);
    }
    // Same for the ones already in hand. Their submission is not
    // made, so the counting `submit_intent` does cannot happen on
    // this path -- and an alert the round found and chose not to
    // touch is not an alert the round did not find.
    for _ in 0..in_hand {
        readings.record(SignalOutcome::Tracked);
    }
    for alert in selected {
        let key = alert_signal_key(&alert.dedup_key);
        *dirty = true;
        let (goal, detail) = alert_intent(alert, config.alert_label_max_chars);
        let outcome = submit_intent(
            orch,
            alert_task_id(&alert.dedup_key),
            "self_signal",
            goal,
            detail,
            may_redrive,
        )
        .await;
        // The report clock belongs to this function's state, so the account is
        // taken here rather than by the round.
        report_outcome(state, &key, outcome, now, readings);
    }
}

/// Publish what the guard store holds after this round, and how much this round
/// reclaimed.
///
/// The size is read off the map itself rather than kept as a counter beside it:
/// a counter of insertions can only grow, and a store whose size was reported
/// that way would have looked healthy for every week it was accumulating dead
/// keys. Reading the map is what lets the series come back down.
fn publish_guard_store(
    state: &SignalGuardState,
    readings: &SignalWatcherReadings,
    reclaimed: usize,
) {
    readings.guard_store(state.reported.len(), reclaimed);
}

/// This loop's name in the liveness census.
pub const SIGNAL_WATCHER_LOOP: &str = "signal_watcher";

/// Whether the upstream pool gate currently holds LLM-dependent work off.
///
/// Every intent this watcher submits ends in a squad of planner/generator/
/// evaluator agents, so a round run while the pool is down cannot produce
/// anything: it buys one refusing 503 per agent and files the run as a failure.
/// The gate is the same one the other LLM-dependent loops read; a deployment
/// with no supervisor publishes no gate, and then the watcher runs as before
/// rather than standing down for a pause nobody declared.
fn llm_held_off(gate: Option<&Arc<dyn cog_core::SchedulerGate>>) -> bool {
    gate.is_some_and(|g| g.is_paused_kind(cog_core::TaskClass::LlmDependent))
}

/// The ticker this loop runs its rounds on: the first round one period out, and
/// every round after it a period apart.
///
/// A fresh interval's first tick completes at once, which is what the other
/// loops here want. This one does not: at process start the pool guard has not
/// published a verdict yet -- its own read of the pool is still in flight -- so
/// the gate is open for want of an answer rather than because the pool is well.
/// A round run then redrives exactly the alerts the gate was added to hold, and
/// it did so on every restart. Waiting a period costs the first round a period
/// and leaves the rest of the schedule alone; the guard's first pass is far
/// shorter than that, so the round is decided by a verdict instead of by its
/// absence.
fn round_ticker(interval: Duration) -> tokio::time::Interval {
    tokio::time::interval_at(tokio::time::Instant::now() + interval, interval)
}

/// Background loop; follows the same shutdown pattern as the baseline port
/// trigger loop.
pub fn spawn_signal_watcher_loop(
    orchestrator: Arc<dyn OrchestratorControl>,
    config: SignalWatcherConfig,
    shutdown: cog_core::ShutdownSignal,
    alert_source: Option<Arc<dyn cog_core::ActiveAlertSource>>,
    taste_source: Option<Arc<dyn cog_core::TasteIntentSource>>,
    readings: Arc<SignalWatcherReadings>,
    llm_gate: Option<Arc<dyn cog_core::SchedulerGate>>,
) -> tokio::task::JoinHandle<()> {
    let interval = Duration::from_secs(config.poll_interval_secs.max(60));
    info!(
        interval_secs = interval.as_secs(),
        failure_recurrence_threshold = config.failure_recurrence_threshold,
        backlog_threshold = config.backlog_threshold,
        self_audit_interval_secs = config.self_audit_interval_secs,
        alert_channel = alert_source.is_some() && config.alert_channel_enabled,
        taste_channel = taste_source.is_some() && config.taste_channel_enabled,
        "self-discovery signal watcher started"
    );
    // Self-discovery: if this loop stops, the system stops noticing its own
    // failures, and the signals it derives simply stop appearing -- which reads
    // the same as a system with nothing wrong.
    cog_core::loop_health::spawn(
        SIGNAL_WATCHER_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let orchestrator = orchestrator.clone();
            let config = config.clone();
            let shutdown = shutdown.clone();
            let alert_source = alert_source.clone();
            let taste_source = taste_source.clone();
            let readings = readings.clone();
            let llm_gate = llm_gate.clone();
            async move {
                // Set by the loop that is about to run rather than by the
                // caller that asked for it: the flag names a watcher that is
                // running, and a caller cannot know that a spawn it requested
                // was the one that took.
                readings.mark_running();
                // One period before the first round: see `round_ticker`.
                let mut ticker = round_ticker(interval);
                // Reported on the edge, not once per round: the round runs at
                // least once a minute and a down pool lasts days.
                let mut held = false;
                // Whether this process has already put the signal projects
                // written before the anchor existed back into shape. Once, not
                // per round: see `settle_signal_parents`.
                let mut parents_settled = false;
                loop {
                    // Every cycle is stamped, including the many that find nothing to
                    // report: a quiet system is the ordinary case here.
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => break,
                        _ = ticker.tick() => {
                            let standing_down = llm_held_off(llm_gate.as_ref());
                            if standing_down != held {
                                held = standing_down;
                                if standing_down {
                                    info!(
                                        "LLM upstream pool unavailable; the round takes the alerts in and files no other work until it recovers"
                                    );
                                }
                            }
                            tick(
                                &orchestrator,
                                &config,
                                alert_source.as_ref(),
                                taste_source.as_ref(),
                                &readings,
                                standing_down,
                                &mut parents_settled,
                            )
                            .await;
                        }
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkout::repo_root;

    #[test]
    fn error_signature_strips_digits_and_collapses_space() {
        let a = error_signature("CI run 34075549276 failed on job 12");
        let b = error_signature("CI run 999 failed on job 3");
        assert_eq!(a, b);
        assert_eq!(a, "ci run failed on job");
    }

    #[test]
    fn error_signature_handles_empty_and_truncates() {
        assert_eq!(error_signature(""), "unknown");
        let long = "x".repeat(200);
        assert_eq!(error_signature(&long).len(), 80);
    }

    #[test]
    fn cooldown_elapsed_only_after_the_window() {
        let mut state = SignalGuardState::default();
        let now = Utc::now();
        assert!(cooldown_elapsed(&state, "k", 3600, now));
        state.reported.insert("k".into(), now);
        assert!(!cooldown_elapsed(&state, "k", 3600, now));
        assert!(cooldown_elapsed(
            &state,
            "k",
            3600,
            now + chrono::Duration::seconds(3601)
        ));
    }

    /// The reclaimer drops the expired keys and keeps the live ones, so the
    /// store's size is the number of keys still holding something back.
    #[test]
    fn reclaiming_drops_only_the_entries_whose_cooldown_ran_out() {
        let now = Utc::now();
        let mut state = SignalGuardState::default();
        state
            .reported
            .insert("expired".into(), now - chrono::Duration::seconds(86_401));
        state
            .reported
            .insert("live".into(), now - chrono::Duration::seconds(86_399));

        assert_eq!(prune_elapsed(&mut state, 86_400, now), 1);
        assert_eq!(state.reported.len(), 1);
        assert!(state.reported.contains_key("live"));
    }

    /// Reclaiming is invisible to the decision the store is for. Every key
    /// answers `cooldown_elapsed` the same way before and after, including the
    /// keys that were dropped -- which is what makes it safe to do at all, and
    /// what a future change to either side has to keep true.
    #[test]
    fn reclaiming_changes_no_cooldown_verdict() {
        let now = Utc::now();
        let mut state = SignalGuardState::default();
        for (key, ago) in [
            ("long_expired", 200_000),
            ("just_expired", 86_400),
            ("live", 10),
            ("fresh", 0),
        ] {
            state
                .reported
                .insert(key.into(), now - chrono::Duration::seconds(ago));
        }
        // `absent` is the control: it was never in the store, and its verdict
        // has to match the one an expired key gets after reclaiming.
        let keys = ["long_expired", "just_expired", "live", "fresh", "absent"];
        let before: Vec<bool> = keys
            .iter()
            .map(|k| cooldown_elapsed(&state, k, 86_400, now))
            .collect();

        prune_elapsed(&mut state, 86_400, now);

        let after: Vec<bool> = keys
            .iter()
            .map(|k| cooldown_elapsed(&state, k, 86_400, now))
            .collect();
        assert_eq!(before, after, "reclaiming moved a cooldown verdict");
    }

    /// The reclaimer runs on the boundary the check uses: an entry exactly at
    /// its cooldown is permitted again and is reclaimed, not held for one more
    /// round. The two sides have to meet, or a store pruned on a stricter
    /// boundary would keep entries that can no longer act.
    #[test]
    fn the_reclaimer_and_the_check_agree_on_the_boundary() {
        let now = Utc::now();
        let mut state = SignalGuardState::default();
        state.reported.insert(
            "at_boundary".into(),
            now - chrono::Duration::seconds(86_400),
        );
        assert!(cooldown_elapsed(&state, "at_boundary", 86_400, now));
        assert_eq!(prune_elapsed(&mut state, 86_400, now), 1);
    }

    /// The published size is the map's own length, so a reclaimed key leaves
    /// the series as well as the file. A count kept beside the store could only
    /// rise, and the state it would have reported during the weeks this store
    /// was accumulating dead keys is indistinguishable from a healthy one.
    #[tokio::test]
    async fn the_published_size_is_the_store_it_was_read_from() {
        let now = Utc::now();
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        readings.tick();

        let mut state = SignalGuardState::default();
        for (key, ago) in [("live", 10), ("dead", 200_000)] {
            state
                .reported
                .insert(key.into(), now - chrono::Duration::seconds(ago));
        }
        let reclaimed = prune_elapsed(&mut state, 86_400, now);
        publish_guard_store(&state, &readings, reclaimed);

        use cog_core::Observable;
        let metrics = readings.collect_metrics("").await.unwrap();
        let value = |name: &str| {
            metrics
                .iter()
                .find(|m| m.name == name)
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        assert_eq!(
            value(crate::signal_readings::SIGNAL_GUARD_ENTRIES_METRIC),
            1.0
        );
        assert_eq!(
            value(crate::signal_readings::SIGNAL_GUARD_RECLAIMED_METRIC),
            1.0
        );
    }

    #[test]
    fn a_submission_that_never_landed_does_not_spend_the_cooldown() {
        let mut state = SignalGuardState::default();
        let now = Utc::now();
        let readings = SignalWatcherReadings::new();
        assert!(!report_outcome(
            &mut state,
            "alert:x",
            IntentOutcome::Failed("orchestrator unreachable".into()),
            now,
            &readings
        ));
        assert!(
            cooldown_elapsed(&state, "alert:x", 86400, now),
            "提交没落地就不能计冷却，否则一次编排器抖动会让信号静默一整个周期"
        );
    }

    #[test]
    fn a_landed_submission_spends_the_cooldown() {
        let mut state = SignalGuardState::default();
        let now = Utc::now();
        let readings = SignalWatcherReadings::new();
        assert!(report_outcome(
            &mut state,
            "alert:x",
            IntentOutcome::Registered,
            now,
            &readings
        ));
        assert!(!cooldown_elapsed(&state, "alert:x", 86400, now));
        assert!(report_outcome(
            &mut state,
            "alert:y",
            IntentOutcome::Redriven,
            now,
            &readings
        ));
        assert!(!cooldown_elapsed(&state, "alert:y", 86400, now));
        assert!(report_outcome(
            &mut state,
            "alert:z",
            IntentOutcome::Resubmitted,
            now,
            &readings
        ));
        assert!(
            !cooldown_elapsed(&state, "alert:z", 86400, now),
            "重新提交换掉的是一条已经结束的行，代价和一次新提交一样"
        );
    }

    #[test]
    fn a_tracked_signal_stays_unrecorded_so_a_later_failure_is_seen() {
        let mut state = SignalGuardState::default();
        let now = Utc::now();
        let readings = SignalWatcherReadings::new();
        assert!(report_outcome(
            &mut state,
            "alert:x",
            IntentOutcome::Tracked,
            now,
            &readings
        ));
        assert!(
            cooldown_elapsed(&state, "alert:x", 86400, now),
            "任务还在手上就不该记账：它一旦失败，下一轮要立刻能发现并重新驱动"
        );
    }

    /// Every way a submission can end is counted, under the name that says how
    /// it ended. An arm that returned without recording would put a whole class
    /// of round back into the one reading this family exists to break apart.
    #[tokio::test]
    async fn every_submission_outcome_is_counted_under_its_own_name() {
        use cog_core::observability::Observable;

        for (outcome, expected) in [
            (
                IntentOutcome::Registered,
                SignalOutcome::Registered.as_str(),
            ),
            (IntentOutcome::Tracked, SignalOutcome::Tracked.as_str()),
            (IntentOutcome::Redriven, SignalOutcome::Redriven.as_str()),
            (
                IntentOutcome::Resubmitted,
                SignalOutcome::Resubmitted.as_str(),
            ),
            (
                IntentOutcome::Failed("orchestrator unreachable".into()),
                SignalOutcome::Failed.as_str(),
            ),
        ] {
            let readings = SignalWatcherReadings::new();
            readings.mark_running();
            let mut state = SignalGuardState::default();
            report_outcome(&mut state, "k", outcome, Utc::now(), &readings);

            let counted: Vec<(String, f64)> = readings
                .collect_metrics("")
                .await
                .unwrap()
                .into_iter()
                .filter(|m| m.name == crate::signal_readings::SIGNAL_OUTCOMES_METRIC)
                .map(|m| {
                    (
                        m.labels
                            .get(crate::signal_readings::OUTCOME_LABEL)
                            .cloned()
                            .unwrap_or_default(),
                        m.value,
                    )
                })
                .collect();
            assert_eq!(
                counted
                    .iter()
                    .find(|(label, _)| label == expected)
                    .map(|(_, value)| *value),
                Some(1.0),
                "{expected} was not counted"
            );
            assert_eq!(
                counted.iter().map(|(_, value)| *value).sum::<f64>(),
                1.0,
                "one submission has to record exactly one outcome"
            );
        }
    }

    #[test]
    fn intent_action_maps_task_state_to_the_right_move() {
        assert_eq!(intent_action(None, true), IntentAction::Submit);
        assert_eq!(
            intent_action(Some(&TaskStatus::Failed), true),
            IntentAction::Redrive
        );
        for live in [
            TaskStatus::Pending,
            TaskStatus::Scheduled,
            TaskStatus::Running,
        ] {
            assert_eq!(
                intent_action(Some(&live), true),
                IntentAction::None,
                "{live:?} 不该再提交或重驱动"
            );
        }
    }

    /// A round the pool gate holds off files the signal and drives nothing, so
    /// the two answers that would spend the pool -- re-driving a failed row and
    /// resubmitting a finished one -- both come back as "leave it alone". The
    /// first filing still goes through: a signal with no row is a signal nothing
    /// knows about, and writing that row spends no LLM call.
    #[test]
    fn a_held_off_round_files_a_new_row_and_drives_no_old_one() {
        assert_eq!(intent_action(None, false), IntentAction::Submit);
        for existing in [
            TaskStatus::Failed,
            TaskStatus::Completed,
            TaskStatus::Cancelled,
            TaskStatus::Pending,
        ] {
            assert_eq!(
                intent_action(Some(&existing), false),
                IntentAction::None,
                "{existing:?} 在池关门时不该被驱动"
            );
        }
    }

    /// A row that has reached a terminal state holds the id but is not work in
    /// hand: the attempt is over and the signal is still firing, so it has to
    /// be cleared rather than left sitting there until the store ages it out.
    /// The distinction from `Failed` is which reset is legal -- `retry_task`
    /// takes a failed task only, so a finished one cannot be re-driven.
    #[test]
    fn a_finished_task_is_resubmitted_rather_than_redriven() {
        assert_eq!(
            intent_action(Some(&TaskStatus::Completed), true),
            IntentAction::Resubmit
        );
        assert_eq!(
            intent_action(Some(&TaskStatus::Cancelled), true),
            IntentAction::Resubmit
        );
    }

    /// What a round files and what it leaves alone, read off the store it wrote
    /// to rather than off the return value of any one call.
    struct FilingOrchestrator {
        tasks: std::sync::Mutex<Vec<Task>>,
    }

    impl FilingOrchestrator {
        fn holding(tasks: Vec<Task>) -> Self {
            Self {
                tasks: std::sync::Mutex::new(tasks),
            }
        }

        fn filed(&self) -> Vec<Task> {
            self.tasks.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl OrchestratorControl for FilingOrchestrator {
        async fn submit_goal_auto(
            &self,
            _goal: &str,
            tasks: Vec<Task>,
        ) -> cog_core::SFResult<Vec<String>> {
            let mut held = self.tasks.lock().unwrap();
            let mut ids = Vec::new();
            for task in tasks {
                // An id the store already holds is the idempotent no-op the real
                // graph makes of it, so a test cannot mistake a duplicate for a
                // filing.
                if held.iter().any(|t| t.id == task.id) {
                    continue;
                }
                ids.push(task.id.clone());
                held.push(task);
            }
            Ok(ids)
        }

        async fn get_all_tasks(&self) -> Vec<Task> {
            self.filed()
        }

        async fn get_task(&self, task_id: &str) -> Option<Task> {
            self.filed().into_iter().find(|t| t.id == task_id)
        }

        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn assign_task(&self, _t: &str, _a: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn add_task(&self, task: Task) -> cog_core::SFResult<()> {
            let mut held = self.tasks.lock().unwrap();
            if !held.iter().any(|t| t.id == task.id) {
                held.push(task);
            }
            Ok(())
        }
        async fn replace_task(&self, task: Task) -> cog_core::SFResult<()> {
            let mut held = self.tasks.lock().unwrap();
            let Some(slot) = held.iter_mut().find(|t| t.id == task.id) else {
                return Err(cog_core::SFError::Internal(format!(
                    "replace_task: no row {}",
                    task.id
                )));
            };
            *slot = task;
            Ok(())
        }
        async fn crew_can_retry(&self, _ids: &[String]) -> bool {
            false
        }
        async fn crew_retry_all(&self, _ids: &[String]) -> usize {
            0
        }
        async fn get_ready_tasks(&self) -> Vec<Task> {
            vec![]
        }
        async fn push_to_dlq(&self, _t: &str, _e: String) -> cog_core::SFResult<bool> {
            Ok(false)
        }
        async fn retry_task(&self, _t: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn dlq_len(&self) -> cog_core::SFResult<usize> {
            Ok(0)
        }
        async fn start_task(&self, _t: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn complete_task(
            &self,
            _t: &str,
            _r: serde_json::Value,
        ) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn fail_task(
            &self,
            _t: &str,
            _e: String,
            _c: Option<cog_core::UpstreamFailure>,
        ) -> cog_core::SFResult<(bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn fail_task_after(
            &self,
            _t: &str,
            _e: String,
            _c: Option<cog_core::UpstreamFailure>,
            _r: Option<u64>,
        ) -> cog_core::SFResult<(bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn cancel_task(&self, _t: &str) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn schedule_task(&self, _t: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            vec![]
        }
        async fn get_dependents(&self, _t: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_dependencies(&self, _t: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
            (vec![], vec![])
        }
        async fn delete_task(&self, _t: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn all_completed(&self) -> bool {
            false
        }
        async fn replay_dlq(&self, _t: &str) -> cog_core::SFResult<bool> {
            Ok(false)
        }
    }

    /// An alert store with a fixed set of firing rows in it.
    struct FiringAlerts(Vec<cog_core::PersistedAlert>);

    #[async_trait::async_trait]
    impl cog_core::ActiveAlertSource for FiringAlerts {
        async fn list_active_alerts(&self, _limit: i64) -> Option<Vec<cog_core::PersistedAlert>> {
            Some(self.0.clone())
        }

        async fn acknowledge_alert(
            &self,
            _dedup_key: &str,
            _by: &str,
        ) -> Result<Option<DateTime<Utc>>, String> {
            unimplemented!()
        }
    }

    /// A round the pool gate holds off still takes the alert in.
    ///
    /// This is the whole point of splitting the round: the row it writes costs
    /// no LLM call, and it is the only evidence a reader has that a firing alert
    /// has been taken up. Held back, the alert-to-work chain goes empty for as
    /// long as the pool is down, which is what happened for the whole of the
    /// 2026-10-27 outage: the watcher stamped five rounds, stood down five times,
    /// and registered nothing while twenty rules were firing.
    #[tokio::test]
    async fn a_held_off_round_still_files_the_alert_it_found() {
        let orch: Arc<dyn OrchestratorControl> = Arc::new(FilingOrchestrator::holding(vec![]));
        let source: Arc<dyn cog_core::ActiveAlertSource> =
            Arc::new(FiringAlerts(vec![firing("k")]));
        let config = SignalWatcherConfig::default();
        let readings = SignalWatcherReadings::new();
        let mut state = SignalGuardState::default();
        let mut dirty = false;

        take_alerts_in(
            &orch,
            &config,
            &source,
            &[],
            &mut state,
            &mut dirty,
            Utc::now(),
            &readings,
            false,
        )
        .await;

        let filed: Vec<String> = orch
            .get_all_tasks()
            .await
            .into_iter()
            .map(|t| t.id)
            .collect();
        // 记下这一格的行的确写下了；锚点也在这批里，那不是第二个「记录」——
        // 它不执行、不带信号，只是这一组的挂点。
        assert!(
            filed.contains(&alert_task_id("k")),
            "held-off rounds are the ones that have to file: the row is the only record"
        );
        assert!(
            filed
                .iter()
                .all(|id| id == &alert_task_id("k") || id.starts_with(SIGNAL_PROJECT_ROOT_PREFIX)),
            "这一轮只该写出这一格的记录行与项目锚点：{filed:?}"
        );
        assert!(
            dirty,
            "the report clock has to be written for the row to hold"
        );
        assert!(
            state.reported.contains_key(&alert_signal_key("k")),
            "a filed alert has to spend its report clock, or the next round files it again"
        );
    }

    /// 同一个条件响两轮：两行挂在**同一个**根下面，那个根是锚点，不是其中任何一行。
    ///
    /// 没有这一格，两个写法都照样绿：每轮各起一个根（这正是现网的形状，9 个项目
    /// 2 个以上根、共 64 行），或者把第二行挂到第一行下面（第一行在重投时会被删，
    /// 删掉它同一组下所有行的父指针就悬空了）。
    #[tokio::test]
    async fn two_filings_for_one_signal_hang_under_one_project_root() {
        let orch: Arc<dyn OrchestratorControl> = Arc::new(FilingOrchestrator::holding(vec![]));
        let detail = serde_json::json!({"kind": "persisted_alert", "rule": "pool_down"});
        let project = signal_project_key(&detail).expect("这份 detail 指名了项目");
        for round in 0..2 {
            let outcome = submit_intent(
                &orch,
                format!("self-signal-alert-pool-{round}"),
                "self_signal",
                format!("修掉 pool_down，第 {round} 轮"),
                detail.clone(),
                true,
            )
            .await;
            assert!(
                matches!(outcome, IntentOutcome::Registered),
                "第 {round} 轮没登记上：{outcome:?}"
            );
        }

        let filed = orch.get_all_tasks().await;
        let in_project: Vec<&Task> = filed
            .iter()
            .filter(|t| t.goal_id.as_deref() == Some(project.as_str()))
            .collect();
        assert_eq!(in_project.len(), 3, "两轮信号 + 一个锚点");

        let roots: Vec<&&Task> = in_project
            .iter()
            .filter(|t| t.parent_task_id.is_none())
            .collect();
        assert_eq!(roots.len(), 1, "这一组必须恰好一个根：{roots:?}");
        let anchor = roots[0];
        assert!(
            matches!(&anchor.task_type, TaskType::Custom(kind) if kind == SIGNAL_PROJECT_ROOT_KIND),
            "那个根得是锚点，不是某一轮的行：{:?}",
            anchor.task_type
        );
        assert!(!anchor.is_executable, "锚点不是活，队列不许起它");
        for row in in_project.iter().filter(|t| t.id != anchor.id) {
            assert_eq!(
                row.parent_task_id.as_deref(),
                Some(anchor.id.as_str()),
                "{} 没挂到锚点下面",
                row.id
            );
        }
    }

    /// 早先写下的、两个根的一组，一次结算后回到一个根——这是那 9 个项目的修法。
    ///
    /// 同时钉住射程：别的项目（这里是 UUID 项目）一行都不动，而不是「凡是
    /// 两行的组都去挂一个锚点」。
    #[tokio::test]
    async fn a_group_written_with_two_roots_is_settled_onto_one_anchor() {
        let project = "signal:persisted_alert/pool_down";
        let stray = |id: &str, project: &str| {
            let mut task = Task::new(
                id.to_string(),
                TaskType::Custom("self_signal".into()),
                serde_json::json!({"kind": "persisted_alert"}),
            );
            task.goal_id = Some(project.to_string());
            task
        };
        let orch: Arc<dyn OrchestratorControl> = Arc::new(FilingOrchestrator::holding(vec![
            stray("self-signal-a", project),
            stray("self-signal-b", project),
            // 单一文件的安静项目：一个根，已经在形状里。
            stray("self-signal-quiet", "signal:queue_backlog"),
            // 不是信号项目：不在射程内。
            stray("change-ledger-x", "3f0c1d2e-0000-4000-8000-000000000000"),
        ]));

        let tasks = orch.get_all_tasks().await;
        let settled = settle_signal_parents(&orch, &tasks).await;
        assert_eq!(settled.anchored, 1, "{settled:?}");
        assert_eq!(settled.reparented, 2, "{settled:?}");
        assert_eq!(settled.failed, 0, "{settled:?}");

        let after = orch.get_all_tasks().await;
        let anchor_id = signal_project_root_id(project);
        let mine: Vec<&Task> = after
            .iter()
            .filter(|t| t.goal_id.as_deref() == Some(project))
            .collect();
        assert_eq!(mine.len(), 3, "两行 + 一个锚点");
        assert_eq!(
            mine.iter().filter(|t| t.parent_task_id.is_none()).count(),
            1,
            "结算之后这一组还是不止一个根"
        );
        for row in mine.iter().filter(|t| t.id != anchor_id) {
            assert_eq!(row.parent_task_id.as_deref(), Some(anchor_id.as_str()));
        }
        assert!(
            after
                .iter()
                .find(|t| t.id == "self-signal-quiet")
                .is_some_and(|t| t.parent_task_id.is_none()),
            "单一文件的安静项目被动了：结算只该修有两个根以上的组"
        );
        assert!(
            after
                .iter()
                .find(|t| t.goal_id.as_deref() == Some("3f0c1d2e-0000-4000-8000-000000000000"))
                .is_some_and(|t| t.parent_task_id.is_none()),
            "非信号项目被动了"
        );
    }

    /// 早先写下的信号行没有验收标准：一次结算按它**自己存下的那次读数**补上。
    ///
    /// 这一格挡的是「改成提交时才写」留下的存量：提交只修它自己那一行，而一个已经
    /// 不再问的条件永远不会再被提交一次，所以那 1194 行会一直空着，项目视图里每一
    /// 行都是关不掉的活。
    ///
    /// 判据取自行自己的 payload 而不是结算那一轮的活读数：这一行是为哪条规则建的，
    /// 就该在哪条规则不响时合上；拿今天的读数去补，会让旧行写下一个它没被建来管的
    /// 条件。射程也钉住：别的项目、以及没有留下读数的行一行都不动。
    #[tokio::test]
    async fn a_signal_row_written_before_the_criterion_existed_is_given_one() {
        let project = "signal:persisted_alert/pool_down";
        let detail = serde_json::json!({"kind": "persisted_alert", "rule": "PoolDown"});
        let row = |id: &str, project: Option<&str>, payload: serde_json::Value| {
            let mut task = Task::new(
                id.to_string(),
                TaskType::Custom("self_signal".into()),
                payload,
            );
            task.goal_id = project.map(str::to_string);
            task
        };
        let orch: Arc<dyn OrchestratorControl> = Arc::new(FilingOrchestrator::holding(vec![
            // 早先的信号行：一个项目一个根，形状已经对，缺的只是验收标准。
            row(
                "self-signal-old",
                Some(project),
                serde_json::json!({"goal": "g", "signal": detail}),
            ),
            // 锚点：不是活，也不带读数。
            signal_project_root(project),
            // 没有留下读数的一行：没有东西可说，就不说。
            row(
                "self-signal-bare",
                Some(project),
                serde_json::json!({"goal": "g"}),
            ),
            // 不是信号项目的行：不在射程内。
            row(
                "change-ledger-x",
                Some("3f0c1d2e-0000-4000-8000-000000000000"),
                serde_json::json!({"goal": "g", "signal": detail}),
            ),
        ]));

        let tasks = orch.get_all_tasks().await;
        let settled = settle_signal_parents(&orch, &tasks).await;
        assert_eq!(settled.criteria, 1, "{settled:?}");
        assert_eq!(settled.failed, 0, "{settled:?}");
        assert_eq!(settled.remaining, 0, "{settled:?}");

        let after = orch.get_all_tasks().await;
        let filled = after.iter().find(|t| t.id == "self-signal-old").unwrap();
        assert_eq!(
            filled.acceptance,
            vec![signal_acceptance(&detail, Some(project))],
            "补上的判据必须是这一行自己那条规则，不是别的读数"
        );
        assert!(
            filled.acceptance[0].contains("PoolDown"),
            "判据要点名这条行是为哪条规则建的：{:?}",
            filled.acceptance
        );
        for untouched in ["self-signal-bare", "change-ledger-x"] {
            let task = after.iter().find(|t| t.id == untouched).unwrap();
            assert!(
                task.acceptance.is_empty(),
                "{} 不该被补：它没有留下读数，或者根本不在信号项目里",
                untouched
            );
        }
        assert!(
            after
                .iter()
                .find(|t| t.id == signal_project_root_id(project))
                .is_some_and(|t| t.acceptance.is_empty()),
            "锚点不是活，不该有验收标准"
        );
    }

    /// 一次结算只写一批，剩下的记在 `remaining` 上让下一轮接着写——存量有上千行，
    /// 一轮全写完会把这一侧的写压在别的 pass 上。
    #[tokio::test]
    async fn a_settle_writes_one_batch_and_says_how_much_is_left() {
        let total = SIGNAL_SETTLE_BATCH + 3;
        let tasks: Vec<Task> = (0..total)
            .map(|i| {
                let project = format!("signal:persisted_alert/rule-{i}");
                let detail =
                    serde_json::json!({"kind": "persisted_alert", "rule": format!("R{i}")});
                let mut task = Task::new(
                    format!("self-signal-{i}"),
                    TaskType::Custom("self_signal".into()),
                    serde_json::json!({"goal": "g", "signal": detail}),
                );
                task.goal_id = Some(project);
                task
            })
            .collect();
        let orch: Arc<dyn OrchestratorControl> = Arc::new(FilingOrchestrator::holding(tasks));

        let first = orch.get_all_tasks().await;
        let settled = settle_signal_parents(&orch, &first).await;
        assert_eq!(settled.criteria, SIGNAL_SETTLE_BATCH, "{settled:?}");
        assert_eq!(settled.remaining, 3, "{settled:?}");

        let second = orch.get_all_tasks().await;
        let settled = settle_signal_parents(&orch, &second).await;
        assert_eq!(settled.criteria, 3, "{settled:?}");
        assert_eq!(settled.remaining, 0, "{settled:?}");
        assert!(
            orch.get_all_tasks()
                .await
                .iter()
                .all(|t| !t.acceptance.is_empty()),
            "第二轮之后还有行没补上"
        );
    }

    /// 回流行同样是「早于判据写下的行」，同一趟结算要把它补上。
    ///
    /// 判据从这一行自己留的负载重算：成因和门点名的文件都在行里，所以补出来的
    /// 那句与今天提交时会写的完全一样——两处共用同一个函数。没有留下任何可用
    /// 字段的行不猜：写一句猜的判据，等于让这一行按一个没人问过的问题关闭。
    #[tokio::test]
    async fn a_rework_row_written_before_its_criterion_is_given_the_same_one() {
        let rework = |id: &str, payload: serde_json::Value| {
            Task::new(
                id.to_string(),
                TaskType::Custom(crate::change_rework::REWORK_TASK_KIND.into()),
                payload,
            )
        };
        let mut running = rework(
            "rework-running",
            serde_json::json!({
                "rejection_cause": "tests_failed",
                "named_files": ["crates/x/src/lib.rs"],
            }),
        );
        running.status = TaskStatus::Running;
        let orch: Arc<dyn OrchestratorControl> = Arc::new(FilingOrchestrator::holding(vec![
            rework(
                "rework-old",
                serde_json::json!({
                    "rejection_cause": "tests_failed",
                    "named_files": ["crates/x/src/lib.rs"],
                }),
            ),
            // 什么也没留下的行：没有诚实的判据可写。
            rework("rework-bare", serde_json::json!({"goal": "g"})),
            // 正在跑的这一行下一轮再说：写回快照会盖掉执行器刚设的状态。
            running,
        ]));

        let tasks = orch.get_all_tasks().await;
        let settled = settle_filings(&orch, &tasks).await;
        assert_eq!(settled.rework_criteria, 1, "{settled:?}");
        assert_eq!(settled.remaining, 1, "{settled:?}");

        let after = orch.get_all_tasks().await;
        let filled = after.iter().find(|t| t.id == "rework-old").unwrap();
        assert_eq!(
            filled.acceptance,
            vec![crate::change_rework::rework_acceptance(
                Some("tests_failed"),
                "crates/x/src/lib.rs"
            )],
            "补上的判据要与提交时写的那一句完全相同"
        );
        for untouched in ["rework-bare", "rework-running"] {
            let task = after.iter().find(|t| t.id == untouched).unwrap();
            assert!(
                task.acceptance.is_empty(),
                "{untouched} 不该被补：它没有可用的现场，或者正在跑"
            );
        }
    }

    /// 周期性自审不是「不再成立」那一族：它每一期都会再提交一次，所以
    /// 「下一轮不再为它建行」永远为真，等于给这一行写了一句恒真的判据——那和
    /// 空判据一样关不掉。关掉它的是它本该产出的那条变更。
    #[tokio::test]
    async fn the_periodic_audit_is_closed_by_the_change_it_produces() {
        let audit = serde_json::json!({"kind": SELF_AUDIT_SIGNAL_KIND, "period": "20261030"});
        let written = signal_acceptance(&audit, None);
        assert!(
            written.contains("20261030") && written.contains("落地"),
            "自审的判据要点名这一期，并指向一条落地的变更：{written}"
        );
        assert!(
            !written.contains("不再"),
            "下一期照样会建行，所以「不再成立」对自审是恒真的：{written}"
        );
    }

    /// 一组一个根、没有锚点：不写任何东西。这一格守的是成本——结算若给每个安静
    /// 信号都补一个锚点，图上会凭空多出上千条不执行的空行。
    #[tokio::test]
    async fn a_project_that_already_has_one_root_is_left_alone() {
        let mut only = Task::new(
            "self-signal-quiet".to_string(),
            TaskType::Custom("self_signal".into()),
            serde_json::json!({"kind": "queue_backlog"}),
        );
        only.goal_id = Some("signal:queue_backlog".to_string());
        let orch: Arc<dyn OrchestratorControl> = Arc::new(FilingOrchestrator::holding(vec![only]));

        let tasks = orch.get_all_tasks().await;
        let settled = settle_signal_parents(&orch, &tasks).await;
        assert!(!settled.moved_anything(), "{settled:?}");
        assert_eq!(orch.get_all_tasks().await.len(), 1, "多写了一行");
    }

    /// 每一种信号都写下「做完」是什么意思，而且写的是**它自己那一次读数**。
    ///
    /// 这一格挡住两种退化：整格空着（现网 1194 条信号行全是空的，项目视图里
    /// 每一条都是关不掉的活），或者三种信号共用一句套话（那就谁也清不掉谁）。
    #[tokio::test]
    async fn every_signal_row_states_what_done_means_in_its_own_terms() {
        let alert = serde_json::json!({"kind": "persisted_alert", "rule": "CognevaHostReadingProducerDown"});
        let failure = serde_json::json!({
            "kind": "failure_recurrence",
            "signature": "panic: index out of bounds",
            "window_secs": 3600,
        });
        let backlog = serde_json::json!({"kind": "queue_backlog", "threshold": 100});
        let unknown = serde_json::json!({"kind": "some_future_signal"});

        let written = [
            signal_acceptance(&alert, signal_project_key(&alert).as_deref()),
            signal_acceptance(&failure, signal_project_key(&failure).as_deref()),
            signal_acceptance(&backlog, signal_project_key(&backlog).as_deref()),
            signal_acceptance(&unknown, signal_project_key(&unknown).as_deref()),
        ];
        for (index, one) in written.iter().enumerate() {
            assert!(
                one.contains("不再"),
                "第 {index} 条没有说清什么算做完：{one}"
            );
        }
        assert!(
            written[0].contains("CognevaHostReadingProducerDown") && written[0].contains("firing"),
            "告警那条要点名规则、说清判据是 firing 集合：{}",
            written[0]
        );
        assert!(
            written[1].contains("panic: index out of bounds") && written[1].contains("3600"),
            "复发失败那条要点名签名与窗口：{}",
            written[1]
        );
        assert!(
            written[2].contains("100"),
            "积压那条要用它自己报的那个阈值：{}",
            written[2]
        );
        assert!(
            written[3].contains("some_future_signal"),
            "落不到具体种类时至少要点名那个项目：{}",
            written[3]
        );
        let distinct: std::collections::BTreeSet<&String> = written.iter().collect();
        assert_eq!(distinct.len(), written.len(), "四种信号写出了同一句话");

        // 端到端：写到图上的那一行确实带着它。
        let orch: Arc<dyn OrchestratorControl> = Arc::new(FilingOrchestrator::holding(vec![]));
        submit_intent(
            &orch,
            "self-signal-alert-producer-down".to_string(),
            "self_signal",
            "修掉 CognevaHostReadingProducerDown".to_string(),
            alert,
            true,
        )
        .await;
        let filed = orch.get_all_tasks().await;
        let row = filed
            .iter()
            .find(|t| t.id == "self-signal-alert-producer-down")
            .expect("这一轮该写出这一行");
        assert_eq!(row.acceptance.len(), 1, "{:?}", row.acceptance);
        assert!(row.acceptance[0].contains("CognevaHostReadingProducerDown"));
    }

    /// A row that already failed is left alone while the pool is closed, and
    /// driven again on the first round after it opens.
    ///
    /// Both halves matter. Without the hold, every round of an outage re-drives
    /// every failed alert into a refusing 503 -- the pulse the gate exists to
    /// stop. Without the second half the alert would be in hand forever: a round
    /// that never drives a failed row again is a work chain that reports progress
    /// it does not make.
    #[test]
    fn a_failed_alert_row_is_driven_only_when_there_is_a_pool_to_drive_it() {
        let alerts = vec![firing("k")];
        let tasks = vec![alert_row("k", TaskStatus::Failed, None)];
        let now = Utc::now();

        let closed = select_alerts(
            &alerts,
            &tasks,
            &SignalGuardState::default(),
            5,
            86400,
            now,
            false,
        );
        assert!(
            closed.selected.is_empty(),
            "a closed pool re-drove a failed row: {:?}",
            closed.selected
        );
        assert_eq!(closed.in_hand, 1);

        let open = select_alerts(
            &alerts,
            &tasks,
            &SignalGuardState::default(),
            5,
            86400,
            now,
            true,
        );
        assert_eq!(
            open.selected.len(),
            1,
            "an open pool has to drive the failure"
        );
    }

    fn firing(dedup_key: &str) -> cog_core::PersistedAlert {
        cog_core::PersistedAlert {
            rule: "test_rule".into(),
            dedup_key: dedup_key.into(),
            severity: "warning".into(),
            state: "firing".into(),
            message: "m".into(),
            labels: serde_json::json!({}),
            fired_at: Utc::now(),
            last_seen_at: Some(Utc::now()),
            acknowledged_at: None,
            acknowledged_by: None,
        }
    }

    #[test]
    fn alerts_in_cooldown_do_not_hide_the_ones_queued_behind_them() {
        let now = Utc::now();
        // Store order is newest-first, so "old" sits behind the two that just
        // fired and are now cooling down.
        let alerts = vec![firing("new1"), firing("new2"), firing("old")];
        let mut state = SignalGuardState::default();
        state.reported.insert("alert:new1".into(), now);
        state.reported.insert("alert:new2".into(), now);

        let picked = select_alerts(&alerts, &[], &state, 1, 86400, now, true);
        assert_eq!(picked.selected.len(), 1);
        assert_eq!(
            picked.selected[0].dedup_key, "old",
            "上限卡的是本轮产出多少意图，先把窗口截断会让冷却中的告警长期挡住排在后面的"
        );
        // The two held back are the reading that says this tick was not a quiet
        // one: they are firing alerts the watcher deliberately did not act on,
        // which is a different state from finding no alert at all.
        assert_eq!(picked.in_cooldown, 2);
        assert_eq!(picked.in_hand, 0);
    }

    #[test]
    fn the_cap_still_bounds_how_many_alerts_one_tick_acts_on() {
        let now = Utc::now();
        let alerts = vec![firing("a"), firing("b"), firing("c")];
        let picked = select_alerts(
            &alerts,
            &[],
            &SignalGuardState::default(),
            2,
            86400,
            now,
            true,
        );
        assert_eq!(
            picked
                .selected
                .iter()
                .map(|a| a.dedup_key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        // The cap and the cooldown are different reasons an alert is not acted
        // on, and a truncated alert is not in cooldown: it comes back next tick.
        assert_eq!(picked.in_cooldown, 0);
    }

    /// A task row for an alert, in the state a test needs to put it in.
    fn alert_row(dedup_key: &str, status: TaskStatus, result: Option<serde_json::Value>) -> Task {
        let mut task = Task::new(
            alert_task_id(dedup_key),
            TaskType::Custom("self_signal".into()),
            serde_json::json!({}),
        );
        task.status = status;
        task.result = result;
        task
    }

    /// One firing rule is one project, however many subjects it fires on.
    ///
    /// The watcher submits one task per signal, and the goal path mints an id
    /// per submission when the task carries none -- so without this grouping a
    /// task row and a project row are the same thing, and the listing cannot
    /// answer "what keeps breaking". The pairing is what is asserted: same rule
    /// shares, different rules do not, the subject inside the rule does not
    /// split it, and a detail that names no condition gets no project at all
    /// rather than an invented one.
    #[test]
    fn one_firing_rule_is_one_project() {
        let alert = |rule: &str, subject: &str| {
            serde_json::json!({
                "kind": "persisted_alert",
                "rule": rule,
                "dedup_key": format!("{rule}:{subject}"),
            })
        };
        let agent = signal_project_key(&alert("agent_unhealthy", "squad:a"));
        assert_eq!(
            agent,
            signal_project_key(&alert("agent_unhealthy", "squad:b")),
            "two subjects of one rule are one project"
        );
        assert_ne!(
            agent,
            signal_project_key(&alert("llm_upstream_pool_down", "squad:a")),
            "two rules are two projects"
        );
        let key = agent.expect("an alert-driven signal must name its project");
        assert!(key.starts_with(SIGNAL_PROJECT_PREFIX));
        assert!(key.ends_with("agent_unhealthy"));
        // A recurring failure recurs by signature, not by rule.
        assert_eq!(
            signal_project_key(&serde_json::json!({
                "kind": "failure_recurrence",
                "signature": "panic: index out of bounds",
            })),
            Some(format!(
                "{SIGNAL_PROJECT_PREFIX}failure_recurrence/panic: index out of bounds"
            ))
        );
        // A condition with no subject of its own still groups by what it is.
        assert_eq!(
            signal_project_key(&serde_json::json!({"kind": "queue_backlog"})),
            Some(format!("{SIGNAL_PROJECT_PREFIX}queue_backlog"))
        );
        // Nothing named, nothing invented.
        assert_eq!(signal_project_key(&serde_json::json!({})), None);
        assert_eq!(signal_project_key(&serde_json::json!({"kind": "  "})), None);
    }

    /// The defect this whole disposition exists for: the attempt failed while
    /// the alert is still firing, and a clock armed at the announcement is not
    /// a reason to leave it failed. Six attempts failed at 06:00Z on 2026-10-04
    /// and the round did not touch them again for the rest of the day.
    #[test]
    fn a_failed_attempt_is_driven_even_inside_the_report_cooldown() {
        let now = Utc::now();
        let alerts = vec![firing("k")];
        let tasks = vec![alert_row("k", TaskStatus::Failed, None)];
        let mut state = SignalGuardState::default();
        // Announced one second ago: every clock a repeat report is held by is
        // wide open against this round.
        state.reported.insert("alert:k".into(), now);

        let picked = select_alerts(&alerts, &tasks, &state, 5, 86400, now, true);
        assert_eq!(
            picked
                .selected
                .iter()
                .map(|a| a.dedup_key.as_str())
                .collect::<Vec<_>>(),
            vec!["k"],
            "一次失败的尝试不是一次重复的报告，冷却挡不住它"
        );
        assert_eq!(picked.in_cooldown, 0, "它没有在冷却里被拦下");
        assert_eq!(picked.in_hand, 0, "它也不在手上");
    }

    /// An attempt that ran to a terminal state and produced nothing to land is
    /// unfinished work too: the condition is true, and nothing was done about
    /// it. Read from the change ids the pipeline records on the task, because
    /// that is the only evidence on the row that anything was produced.
    #[test]
    fn an_attempt_that_ended_empty_is_driven_even_inside_the_report_cooldown() {
        let now = Utc::now();
        let alerts = vec![firing("k")];
        let empty = Some(serde_json::json!({"change_ids": []}));
        let tasks = vec![alert_row("k", TaskStatus::Completed, empty)];
        let mut state = SignalGuardState::default();
        state.reported.insert("alert:k".into(), now);

        let picked = select_alerts(&alerts, &tasks, &state, 5, 86400, now, true);
        assert_eq!(
            picked.selected.len(),
            1,
            "跑完了但什么都没产出的尝试不算做完"
        );
        assert_eq!(picked.in_cooldown, 0);
    }

    /// An attempt that is still running is work in hand: the round found the
    /// signal, found it already being worked, and drove nothing. That is a
    /// third state, and it is neither "in cooldown" nor "found nothing".
    #[test]
    fn an_attempt_in_flight_is_work_in_hand_not_a_spent_report() {
        let now = Utc::now();
        let alerts = vec![firing("k")];
        let tasks = vec![alert_row("k", TaskStatus::Running, None)];
        let mut state = SignalGuardState::default();
        state.reported.insert("alert:k".into(), now);

        let picked = select_alerts(&alerts, &tasks, &state, 5, 86400, now, true);
        assert!(picked.selected.is_empty(), "在跑的尝试不该被再投一次");
        assert_eq!(picked.in_hand, 1);
        assert_eq!(
            picked.in_cooldown, 0,
            "「已经在手上」和「被冷却挡住」是两种状态，不能合成一个读数"
        );
    }

    /// A repeat announcement is still a repeat. An attempt that ended with a
    /// change behind it leaves nothing for this round to redo, so the signal
    /// outliving it waits -- and the clock runs from when the attempt ended,
    /// not from when the signal was first announced.
    #[test]
    fn the_repeat_clock_runs_from_when_the_attempt_ended() {
        let now = Utc::now();
        let alerts = vec![firing("k")];
        let produced = Some(serde_json::json!({"change_ids": ["c1"]}));
        let mut task = alert_row("k", TaskStatus::Completed, produced);
        // The announcement is two days old, so a clock armed at it has long
        // elapsed and would resubmit this every tick; the attempt itself ended
        // a minute ago. The verdict has to follow the attempt.
        task.updated_at = now - chrono::Duration::seconds(60);
        let mut state = SignalGuardState::default();
        state.reported.insert(
            "alert:k".into(),
            now - chrono::Duration::seconds(2 * 86_400),
        );

        let picked = select_alerts(&alerts, &[task], &state, 5, 86400, now, true);
        assert!(
            picked.selected.is_empty(),
            "刚落地一笔产出的信号不是在重复报告，不该立刻再开一轮"
        );
        assert_eq!(picked.in_cooldown, 1);
    }

    /// And the same alert once the attempt's own clock has elapsed is driven
    /// again -- the repeat is permitted by when the work ended, which is the
    /// fact the clock is actually about.
    #[test]
    fn the_repeat_is_driven_once_the_attempt_s_own_clock_has_elapsed() {
        let now = Utc::now();
        let alerts = vec![firing("k")];
        let produced = Some(serde_json::json!({"change_ids": ["c1"]}));
        let mut task = alert_row("k", TaskStatus::Completed, produced);
        task.updated_at = now - chrono::Duration::seconds(86_401);

        let picked = select_alerts(
            &alerts,
            &[task],
            &SignalGuardState::default(),
            5,
            86400,
            now,
            true,
        );
        assert_eq!(picked.selected.len(), 1);
        assert_eq!(picked.in_cooldown, 0);
    }

    /// Unfinished work is taken before announcements, so the per-tick cap can
    /// never be the reason a failed attempt stays failed while a fresh
    /// announcement of something else spends the round's budget.
    #[test]
    fn the_cap_spends_the_round_on_unfinished_work_before_announcements() {
        let now = Utc::now();
        let alerts = vec![firing("announced"), firing("broken")];
        let tasks = vec![alert_row("broken", TaskStatus::Failed, None)];

        let picked = select_alerts(
            &alerts,
            &tasks,
            &SignalGuardState::default(),
            1,
            86400,
            now,
            true,
        );
        assert_eq!(
            picked
                .selected
                .iter()
                .map(|a| a.dedup_key.as_str())
                .collect::<Vec<_>>(),
            vec!["broken"],
            "上限花在没做完的活上，不是花在先冒出来的那一条上"
        );
    }

    #[test]
    fn an_alert_that_embeds_a_whole_input_yields_a_bounded_intent() {
        // The amplification this guards against: an alert carries its task's
        // whole input as a label, the watcher inlines the labels into a goal,
        // and that goal becomes the next task's input — which the next alert
        // then embeds, one nesting level deeper, for the same non-progress.
        let mut alert = firing("amp");
        alert.labels = serde_json::json!({
            "goal_id": "g1",
            "original_input": {
                "goal": "刷新接口未区分访问令牌".repeat(2000),
                "nested": { "original_input": { "goal": "g".repeat(50_000) } },
            },
        });

        let (goal, detail) = alert_intent(&alert, 256);

        assert!(
            goal.chars().count() < 4_000,
            "goal inlines the labels; unbounded labels make every re-drive bigger: {} chars",
            goal.chars().count()
        );
        let forwarded = detail["labels"]["original_input"]
            .as_str()
            .expect("a nested object must be flattened, not carried as nesting");
        assert!(
            forwarded.chars().count() <= 257,
            "the forwarded label is what the next alert embeds: {} chars",
            forwarded.chars().count()
        );
        assert!(forwarded.ends_with('…'));
    }

    fn shipped_document(path: &str) -> serde_json::Value {
        let full = repo_root().join(path);
        let text = std::fs::read_to_string(&full)
            .unwrap_or_else(|e| panic!("{} is unreadable: {e}", full.display()));
        serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("{} is not JSON: {e}", full.display()))
    }

    /// The pair as the deployment loads it: the document's own cooldown when the
    /// section is there, the code default when it is not — the resolution
    /// `SignalWatcherConfig::load` performs, reproduced rather than trusted.
    fn retention_and_wait(document: &serde_json::Value) -> (bool, u64, i64) {
        let enabled = document["dag_executor"]["archive_enabled"]
            .as_bool()
            .expect("dag_executor.archive_enabled");
        let retention = document["dag_executor"]["archive_after_secs"]
            .as_u64()
            .expect("dag_executor.archive_after_secs");
        let wait = document
            .pointer("/self_evolution/signal_watcher/report_cooldown_secs")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| SignalWatcherConfig::default().report_cooldown_secs);
        (enabled, retention, wait)
    }

    /// A failed row is the record the re-drive reads, and the store reaps it.
    ///
    /// `intent_action` re-drives a task whose previous attempt failed, and that
    /// branch is only reachable while the row is still in the store: a reaped
    /// row reads as "no such task", the signal goes out as new work, and the
    /// re-drive happens never — not late, never. The store reaps terminal rows
    /// after `dag_executor.archive_after_secs` and the wait between two attempts
    /// on one signal is `report_cooldown_secs`; neither component reads the
    /// other's number, so the comparison lives here, at the premise. It is read
    /// out of the documents the deployment ships, because a value that never
    /// reaches a cluster holds nothing together. Twice the wait, so a door
    /// missed to a restart still finds its record.
    #[test]
    fn the_store_outlives_the_re_drive_window_it_gates() {
        let shipped = [
            ("chart", "deploy/helm/cogneva/files/cogneva.json"),
            ("example", "cogneva.example.json"),
        ];
        let mut cases: Vec<(String, bool, u64, i64)> = shipped
            .iter()
            .map(|(name, path)| {
                let (enabled, retention, wait) = retention_and_wait(&shipped_document(path));
                ((*name).to_string(), enabled, retention, wait)
            })
            .collect();
        let defaults = cog_core::Config::default().dag_executor;
        // 归档开关默认关，可只要有人在配置里写上 `archive_enabled: true`
        // 而不写保留期，留下的这个默认值就是那把尺子——所以它也在这里量。
        cases.push((
            "code defaults, archival on".into(),
            true,
            defaults.archive_after_secs,
            SignalWatcherConfig::default().report_cooldown_secs,
        ));

        for (name, enabled, retention, wait) in cases {
            if !enabled {
                continue;
            }
            assert!(
                retention as i64 >= 2 * wait,
                "{name}: 归档窗口 {retention}s 不足重驱动窗口 {wait}s 的两倍——失败行会在门再开之前被回收，\
                 而 Redrive 只在行还在时可达，「重试」于是静默变成「重新提交」"
            );
        }
    }

    /// The audit's id carries the day it was due for, which is what keeps it out
    /// of the premise above: with a stable id an audit would be re-driven a week
    /// later against a record the store reaped days earlier.
    #[test]
    fn the_audit_id_carries_its_period() {
        let monday = DateTime::parse_from_rfc3339("2026-10-05T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let same_day = DateTime::parse_from_rfc3339("2026-10-05T23:59:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let next_day = DateTime::parse_from_rfc3339("2026-10-06T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(self_audit_task_id(monday), self_audit_task_id(same_day));
        assert_ne!(self_audit_task_id(monday), self_audit_task_id(next_day));
    }

    /// A gate that pauses exactly one class, so the answer says which class the
    /// caller asked about and not merely that something is paused.
    struct OneClassGate(cog_core::TaskClass);

    impl cog_core::SchedulerGate for OneClassGate {
        fn is_paused(&self) -> bool {
            false
        }

        fn pause(&self) -> bool {
            false
        }

        fn resume(&self) -> bool {
            false
        }

        fn is_paused_kind(&self, class: cog_core::TaskClass) -> bool {
            class == self.0
        }

        fn pause_kind(&self, _class: cog_core::TaskClass) -> bool {
            false
        }

        fn resume_kind(&self, _class: cog_core::TaskClass) -> bool {
            false
        }
    }

    /// The watcher stands down on the pool's verdict about its own class, and
    /// on nothing else: no gate is a deployment without a supervisor, and a
    /// paused mechanical class is not the pool's business.
    #[test]
    fn held_off_answers_the_llm_class_not_any_pause() {
        assert!(!llm_held_off(None));

        let mechanical: Arc<dyn cog_core::SchedulerGate> =
            Arc::new(OneClassGate(cog_core::TaskClass::Mechanical));
        assert!(!llm_held_off(Some(&mechanical)));

        let llm: Arc<dyn cog_core::SchedulerGate> =
            Arc::new(OneClassGate(cog_core::TaskClass::LlmDependent));
        assert!(llm_held_off(Some(&llm)));
    }

    /// The first round is one period out, not immediate: an immediate first
    /// round runs before the pool guard has published its verdict, which is how
    /// a restart redrove four alerts the gate was holding. The period after it
    /// is unchanged, so the round a restart costs is the round it waits for.
    #[tokio::test(start_paused = true)]
    async fn the_first_round_waits_one_period() {
        let interval = Duration::from_secs(300);
        let mut ticker = round_ticker(interval);

        // A second short of the period: the round has not run.
        tokio::time::advance(interval - Duration::from_secs(1)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), ticker.tick())
                .await
                .is_err(),
            "the first round ran before the period had elapsed"
        );

        // On the period: it runs, and the next one is a period behind it.
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_millis(1), ticker.tick())
            .await
            .expect("the first round never ran");

        tokio::time::advance(interval - Duration::from_secs(1)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), ticker.tick())
                .await
                .is_err(),
            "the rounds after the first are not a period apart"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_millis(1), ticker.tick())
            .await
            .expect("the second round never ran");
    }

    /// A held-off round stamps one tick and counts one stood-down round, and it
    /// counts neither twice.
    ///
    /// The round stamps itself, because a held-off round is not an empty one --
    /// it still takes the alerts in -- so the stand-down count must add only the
    /// second series. A pause has to be readable as itself: the tick alone would
    /// leave it looking like the quiet system the gate was added to tell it
    /// apart from, freezing the tick would leave it looking like a loop that
    /// died, and stamping it here as well as in the round would halve every rate
    /// computed against it.
    #[tokio::test]
    async fn a_held_off_round_is_stamped_once_and_counted_as_stood_down() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();

        for _ in 0..2 {
            readings.tick();
            readings.held_round();
        }

        use cog_core::Observable;
        let metrics = readings.collect_metrics("").await.unwrap();
        let value = |name: &str| {
            metrics
                .iter()
                .find(|m| m.name == name)
                .map(|m| m.value)
                .unwrap_or(-1.0)
        };
        assert_eq!(value(crate::signal_readings::SIGNAL_TICKS_METRIC), 2.0);
        assert_eq!(value(crate::signal_readings::SIGNAL_HELD_METRIC), 2.0);
    }

    fn judgement(subject: &str, preferred: &str) -> cog_core::TasteIntent {
        cog_core::TasteIntent {
            id: uuid::Uuid::new_v4(),
            subject: subject.into(),
            submitted_by: "reviewer".into(),
            submitted_at: Utc::now(),
            payload: cog_core::TasteIntentPayload::DirectionPreference(
                cog_core::DirectionPreferenceIntent {
                    preferred: preferred.into(),
                    alternative: "the other way".into(),
                    rationale: "it is the one the queue can absorb".into(),
                },
            ),
            evidence_refs: Vec::new(),
        }
    }

    fn filed_row(intent: cog_core::TasteIntent, task_id: &str) -> cog_core::StoredTasteIntent {
        cog_core::StoredTasteIntent {
            intent,
            task_id: Some(task_id.into()),
        }
    }

    fn pending_row(intent: cog_core::TasteIntent) -> cog_core::StoredTasteIntent {
        cog_core::StoredTasteIntent {
            intent,
            task_id: None,
        }
    }

    fn taste_row(intent: &cog_core::TasteIntent, status: TaskStatus) -> Task {
        let mut task = Task::new(
            taste_task_id(intent),
            TaskType::Custom("self_signal".into()),
            serde_json::json!({}),
        );
        task.status = status;
        task
    }

    /// The key is the pair, so two judgements of one kind about one subject are
    /// one signal however they are worded — and a judgement *about* a subject is
    /// not the same signal as a preference *over* it, which is what makes the
    /// kind part of the key rather than something the reader folds in later.
    #[test]
    fn the_taste_key_is_the_kind_and_the_subject_together() {
        let subject = "the retry policy";
        let mut verdict = judgement(subject, "the cheap one");
        verdict.payload =
            cog_core::TasteIntentPayload::EvaluatorVerdict(cog_core::EvaluatorVerdictIntent {
                verdict: cog_core::EvaluatorVerdict::Contradicted,
                rationale: "the failures come from the workload".into(),
            });

        assert_eq!(
            taste_signal_key(&judgement(subject, "the cheap one")),
            taste_signal_key(&judgement(subject, "a different wording")),
            "同一种判定针对同一个对象就是同一个信号，措辞不同不算两回事"
        );
        assert_ne!(
            taste_signal_key(&judgement(subject, "the cheap one")),
            taste_signal_key(&verdict),
            "对同一个对象「判定它对不对」和「偏好哪一个」是两种输入"
        );
        assert_ne!(
            taste_signal_key(&judgement(subject, "the cheap one")),
            taste_signal_key(&judgement("another subject", "the cheap one"))
        );
    }

    /// Every submission owns one task row, derived from its own id. Two
    /// submissions of the same judgement are two pieces of evidence and get two
    /// rows: filed under a shared name, the second would land on the first one's
    /// finished history, which `submit_intent` answers by clearing the row and
    /// running a one-shot judgement a second time.
    #[test]
    fn each_submission_owns_its_own_task_row() {
        let first = judgement("the retry policy", "the cheap one");
        let second = judgement("the retry policy", "the cheap one");

        assert_ne!(
            taste_task_id(&first),
            taste_task_id(&second),
            "同一个判定提交两次是两条证据，不能共用一个任务名"
        );
        assert_ne!(
            taste_task_id(&first),
            alert_task_id("k"),
            "两条通道的任务名不能撞在一起"
        );
    }

    /// The judgement is quoted, not paraphrased: the reason is the whole input,
    /// and a squad acting on a summary would be acting on the summary. What the
    /// goal says about it is what kind of thing it is — a stated judgement, not
    /// an instruction — because the reader of this goal is an agent that will
    /// otherwise read a preference as an order.
    #[test]
    fn the_goal_quotes_the_judgement_and_says_it_is_one() {
        let intent = judgement("the retry policy", "the cheap one");
        let rationale = "it is the one the queue can absorb".to_string();
        let (goal, detail) = taste_intent(&intent);

        assert!(goal.contains(&intent.subject));
        assert!(
            goal.contains(&rationale),
            "提交者给的理由要原样进 goal，转述一次就是丢一次输入"
        );
        assert!(goal.contains("reviewer"), "提交者要出现在 goal 里");
        assert!(
            goal.contains("stated judgement"),
            "goal 要说明这是一条陈述出来的判定，而不是一条指令"
        );
        assert_eq!(detail["kind"], "taste_intent");
        assert_eq!(detail["taste_kind"], intent.payload.kind());
        assert_eq!(detail["intent_id"], intent.id.to_string());
        assert_eq!(detail["submitted_by"], "reviewer");
        assert_eq!(
            detail["payload"].get("preferred").and_then(|v| v.as_str()),
            Some("the cheap one"),
            "判定本身要原样随活带下去，产出才能对着当初要求的东西被读"
        );
    }

    /// Unfinished work goes first, ahead of the cap, for the same reason it does
    /// on the alert channel: a failed attempt is not a repeat, and the cap must
    /// not be the reason it stays failed while a fresh announcement spends the
    /// round.
    #[test]
    fn unfinished_taste_work_comes_before_a_fresh_announcement() {
        let now = Utc::now();
        let broken = judgement("the retry policy", "the cheap one");
        let announced = judgement("the review rubric", "the stricter one");
        let filed = vec![filed_row(broken.clone(), &taste_task_id(&broken))];
        let pending = vec![pending_row(announced)];
        let tasks = [taste_row(&broken, TaskStatus::Failed)];
        let by_id: HashMap<&str, &Task> = tasks.iter().map(|t| (t.id.as_str(), t)).collect();

        let picked = select_taste(
            &pending,
            &filed,
            &by_id,
            &SignalGuardState::default(),
            86_400,
            1,
            now,
        );

        assert_eq!(
            picked
                .selected
                .iter()
                .map(|row| row.intent.subject.as_str())
                .collect::<Vec<_>>(),
            vec!["the retry policy"],
            "上限花在没做完的活上，不是花在先冒出来的那一条上"
        );
        assert!(picked.superseded.is_empty());
    }

    /// A submission whose work is still running has nothing to do and nothing to
    /// close: the row is not a repeat and not unfinished either. Selecting it
    /// would file a second task for one judgement.
    #[test]
    fn a_taste_submission_already_in_flight_is_left_alone() {
        let now = Utc::now();
        let intent = judgement("the retry policy", "the cheap one");
        let filed = vec![filed_row(intent.clone(), &taste_task_id(&intent))];
        let tasks = [taste_row(&intent, TaskStatus::Running)];
        let by_id: HashMap<&str, &Task> = tasks.iter().map(|t| (t.id.as_str(), t)).collect();

        let picked = select_taste(
            &[],
            &filed,
            &by_id,
            &SignalGuardState::default(),
            86_400,
            5,
            now,
        );

        assert!(picked.selected.is_empty(), "在跑的活不该被再投一次");
        assert!(picked.superseded.is_empty(), "它也不是一条重复的判定");
    }

    /// Work that ran to a terminal state is history. It is not selected here at
    /// all: handing it to the task layer would go through the path that clears a
    /// finished row and resubmits, which is a second run of a one-shot
    /// judgement. Closing the row is the caller's job, and it does it before it
    /// gets here.
    #[test]
    fn finished_taste_work_is_not_selected() {
        let now = Utc::now();
        let intent = judgement("the retry policy", "the cheap one");
        let filed = vec![filed_row(intent.clone(), &taste_task_id(&intent))];
        let tasks = [taste_row(&intent, TaskStatus::Completed)];
        let by_id: HashMap<&str, &Task> = tasks.iter().map(|t| (t.id.as_str(), t)).collect();

        let picked = select_taste(
            &[],
            &filed,
            &by_id,
            &SignalGuardState::default(),
            86_400,
            5,
            now,
        );

        assert!(picked.selected.is_empty());
        assert!(picked.superseded.is_empty());
    }

    /// A repeat is closed, not postponed. Left pending, the same row would be
    /// read as a fresh announcement the moment the cooldown ran out and would
    /// then file a duplicate of work that had already been done — the whole
    /// reason the closing exists is that the row will otherwise outlive the
    /// clock that held it back.
    #[test]
    fn a_repeat_judgement_is_closed_rather_than_postponed() {
        let now = Utc::now();
        let intent = judgement("the retry policy", "the cheap one");
        let pending = vec![pending_row(intent.clone())];
        let mut state = SignalGuardState::default();
        state.reported.insert(
            taste_signal_key(&intent),
            now - chrono::Duration::seconds(60),
        );

        let picked = select_taste(&pending, &[], &HashMap::new(), &state, 86_400, 5, now);

        assert!(picked.selected.is_empty(), "还在冷却里的判定不该被驱动");
        assert_eq!(picked.superseded.len(), 1);
        assert_eq!(picked.superseded[0].intent.id, intent.id);
    }

    /// And the same judgement once its own clock has run out is driven again —
    /// the cooldown holds a repeat back, it does not forbid one forever.
    #[test]
    fn a_repeat_is_driven_once_its_clock_has_run_out() {
        let now = Utc::now();
        let intent = judgement("the retry policy", "the cheap one");
        let pending = vec![pending_row(intent.clone())];
        let mut state = SignalGuardState::default();
        state.reported.insert(
            taste_signal_key(&intent),
            now - chrono::Duration::seconds(86_401),
        );

        let picked = select_taste(&pending, &[], &HashMap::new(), &state, 86_400, 5, now);

        assert_eq!(picked.selected.len(), 1);
        assert!(picked.superseded.is_empty());
    }

    /// The cap bounds one round, not the queue: submissions behind it are left
    /// pending rather than closed, because nothing has decided anything about
    /// them yet. Closing them would turn a busy round into a round that threw
    /// away the submissions it did not have budget for.
    #[test]
    fn the_taste_cap_leaves_the_rest_pending() {
        let now = Utc::now();
        let pending = vec![
            pending_row(judgement("subject a", "x")),
            pending_row(judgement("subject b", "x")),
            pending_row(judgement("subject c", "x")),
        ];

        let picked = select_taste(
            &pending,
            &[],
            &HashMap::new(),
            &SignalGuardState::default(),
            86_400,
            2,
            now,
        );

        assert_eq!(
            picked
                .selected
                .iter()
                .map(|row| row.intent.subject.as_str())
                .collect::<Vec<_>>(),
            vec!["subject a", "subject b"]
        );
        assert!(
            picked.superseded.is_empty(),
            "超上限的那一条不是重复，下一轮还要读它"
        );
    }
}
