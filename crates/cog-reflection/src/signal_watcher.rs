//! Self-discovery signal watcher — the producer side of system self-discovery.
//!
//! Evolution inputs are not only external intents (issues/PRs); the system
//! must also discover its own problems. This watcher polls orchestrator task
//! state and turns four classes of runtime signals into internal evolution
//! intents submitted through the main flow (`evolution_mode=generate_change`):
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
        }
    }
}

impl SignalWatcherConfig {
    /// 从 cogneva.json 的 `self_evolution.signal_watcher` 段加载，再叠加
    /// env 覆盖。文件或段缺失时返回 Default；段存在但解析失败、或 env
    /// 值非法时返回 Err——配置写错必须响亮失败。
    pub fn load() -> cog_core::SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
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
    let dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into());
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
fn alert_work(existing: Option<&Task>) -> AlertWork {
    let Some(task) = existing else {
        return AlertWork::NeverFiled;
    };
    match intent_action(Some(&task.status)) {
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
) -> SelectedAlerts<'a> {
    let mut unfinished = Vec::new();
    let mut announced = Vec::new();
    let mut in_hand = 0;
    let mut in_cooldown = 0;
    for alert in alerts {
        let row = tasks
            .iter()
            .find(|t| t.id == alert_task_id(&alert.dedup_key));
        match alert_work(row) {
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
    let goal = format!(
        "Investigate and fix the root cause of firing alert \"{}\" \
         (severity: {}): {}. Alert labels: {}. The alert fired at {} \
         and is still active. Identify the underlying defect or \
         resource condition, implement a durable fix, and explain \
         how recurrence is prevented.",
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
fn intent_action(existing: Option<&TaskStatus>) -> IntentAction {
    match existing {
        None => IntentAction::Submit,
        Some(TaskStatus::Failed) => IntentAction::Redrive,
        Some(TaskStatus::Completed | TaskStatus::Cancelled) => IntentAction::Resubmit,
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
) -> IntentOutcome {
    let existing = orch.get_task(&task_id).await;
    let action = intent_action(existing.as_ref().map(|t| &t.status));
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
            let task = Task::new(
                task_id,
                TaskType::Custom(task_kind.into()),
                serde_json::json!({
                    "goal": goal,
                    "evolution_mode": "generate_change",
                    "task_kind": task_kind,
                    "signal": detail,
                }),
            );
            match orch.submit_goal_auto(&goal, vec![task]).await {
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
    readings: &SignalWatcherReadings,
) {
    // Stamped before anything is read, so a round that fails or finds nothing
    // still counts as a round: the tick count is what the signal counts are
    // read against, and a denominator that only moves on busy rounds would make
    // a broken watcher look like a quiet system.
    readings.tick();
    let now = Utc::now();
    let tasks = orch.get_all_tasks().await;
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

    // 1. Failure recurrence among self-evolution tasks.
    let window_start = now - chrono::Duration::seconds(config.failure_window_secs);
    let mut by_signature: HashMap<String, Vec<&Task>> = HashMap::new();
    for task in &tasks {
        if task.status != TaskStatus::Failed || task.updated_at < window_start {
            continue;
        }
        let is_evolution =
            task.input.get("evolution_mode").and_then(|v| v.as_str()) == Some("generate_change");
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
        let goal = format!(
            "Investigate recurring self-evolution failure: signature \"{sig}\" \
             occurred {} times in the last {} seconds (sample tasks: {}). \
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
                "Investigate orchestrator queue backlog: {backlog} tasks \
                 pending/scheduled (DLQ: {dlq}). Determine why tasks are not \
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
            let goal = "Audit this repository for security and reliability \
                        defects, going over: hardcoded secrets or weak default \
                        credentials, injection surfaces (command/SQL/XSS), \
                        unsafe dependency or configuration patterns, and \
                        error paths that silently swallow failures. Pick the \
                        single most critical confirmed finding and implement \
                        its fix; list the remaining findings in the change \
                        description."
                .to_string();
            let period = now.format("%Y%m%d").to_string();
            let outcome = submit_intent(
                orch,
                key.clone(),
                "self_audit",
                goal,
                serde_json::json!({
                    "kind": "self_audit",
                    "period": period,
                }),
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
            // A failed read yields no intents for this tick, and the cooldown
            // bookkeeping is left untouched so the next tick retries with the
            // same slate. It also must not be read as "the alerts cleared":
            // nothing here concludes anything from absence.
            if let Some(alerts) = source.list_active_alerts(100).await {
                let SelectedAlerts {
                    selected,
                    in_hand,
                    in_cooldown,
                } = select_alerts(
                    &alerts,
                    &tasks,
                    &state,
                    config.alert_channel_max_per_tick,
                    config.report_cooldown_secs,
                    now,
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
                    dirty = true;
                    let (goal, detail) = alert_intent(alert, config.alert_label_max_chars);
                    let outcome = submit_intent(
                        orch,
                        alert_task_id(&alert.dedup_key),
                        "self_signal",
                        goal,
                        detail,
                    )
                    .await;
                    report_outcome(&mut state, &key, outcome, now, readings);
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

/// Background loop; follows the same shutdown pattern as the baseline port
/// trigger loop.
pub fn spawn_signal_watcher_loop(
    orchestrator: Arc<dyn OrchestratorControl>,
    config: SignalWatcherConfig,
    shutdown: cog_core::ShutdownSignal,
    alert_source: Option<Arc<dyn cog_core::ActiveAlertSource>>,
    readings: Arc<SignalWatcherReadings>,
) -> tokio::task::JoinHandle<()> {
    let interval = Duration::from_secs(config.poll_interval_secs.max(60));
    info!(
        interval_secs = interval.as_secs(),
        failure_recurrence_threshold = config.failure_recurrence_threshold,
        backlog_threshold = config.backlog_threshold,
        self_audit_interval_secs = config.self_audit_interval_secs,
        alert_channel = alert_source.is_some() && config.alert_channel_enabled,
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
            let readings = readings.clone();
            async move {
                // Set by the loop that is about to run rather than by the
                // caller that asked for it: the flag names a watcher that is
                // running, and a caller cannot know that a spawn it requested
                // was the one that took.
                readings.mark_running();
                let mut ticker = tokio::time::interval(interval);
                loop {
                    // Every cycle is stamped, including the many that find nothing to
                    // report: a quiet system is the ordinary case here.
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => break,
                        _ = ticker.tick() => {
                            tick(&orchestrator, &config, alert_source.as_ref(), &readings).await;
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
        assert_eq!(intent_action(None), IntentAction::Submit);
        assert_eq!(
            intent_action(Some(&TaskStatus::Failed)),
            IntentAction::Redrive
        );
        for live in [
            TaskStatus::Pending,
            TaskStatus::Scheduled,
            TaskStatus::Running,
        ] {
            assert_eq!(
                intent_action(Some(&live)),
                IntentAction::None,
                "{live:?} 不该再提交或重驱动"
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
            intent_action(Some(&TaskStatus::Completed)),
            IntentAction::Resubmit
        );
        assert_eq!(
            intent_action(Some(&TaskStatus::Cancelled)),
            IntentAction::Resubmit
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

        let picked = select_alerts(&alerts, &[], &state, 1, 86400, now);
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
        let picked = select_alerts(&alerts, &[], &SignalGuardState::default(), 2, 86400, now);
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

        let picked = select_alerts(&alerts, &tasks, &state, 5, 86400, now);
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

        let picked = select_alerts(&alerts, &tasks, &state, 5, 86400, now);
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

        let picked = select_alerts(&alerts, &tasks, &state, 5, 86400, now);
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

        let picked = select_alerts(&alerts, &[task], &state, 5, 86400, now);
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

        let picked = select_alerts(&alerts, &tasks, &SignalGuardState::default(), 1, 86400, now);
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
        let full = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path);
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
}
