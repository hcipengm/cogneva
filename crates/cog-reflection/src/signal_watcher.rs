//! Self-discovery signal watcher — the producer side of system self-discovery.
//!
//! Evolution inputs are not only external intents (issues/PRs); the system
//! must also discover its own problems. This watcher polls orchestrator task
//! state and turns three classes of runtime signals into internal evolution
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
//! idempotence has to live here. A task already in hand is left alone (and
//! spends no cooldown, so a later failure is noticed on the next tick, not a
//! whole cooldown later); a task that failed while its signal persists is
//! re-driven; a task that never reached the orchestrator spends no cooldown
//! either, so the next tick retries instead of the signal going quiet.
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

/// The firing alerts this tick should act on: drop the ones still inside their
/// cooldown, then stop once `max` are left.
///
/// The cap bounds the work a single tick creates, so it has to count the alerts
/// actually selected rather than the first `max` rows the store returned. The
/// store hands them back newest-first, so truncating there lets a cluster of
/// newly fired alerts hide every older one behind them for as long as they keep
/// firing — which is exactly when a long-standing fault most needs to be seen.
fn select_alerts<'a>(
    alerts: &'a [cog_core::PersistedAlert],
    state: &SignalGuardState,
    max: usize,
    cooldown_secs: i64,
    now: DateTime<Utc>,
) -> Vec<&'a cog_core::PersistedAlert> {
    alerts
        .iter()
        .filter(|a| cooldown_elapsed(state, &format!("alert:{}", a.dedup_key), cooldown_secs, now))
        .take(max)
        .collect()
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

/// What this tick should do about a signal, given whether a task for it is
/// already in the store.
#[derive(Debug, PartialEq, Eq)]
enum IntentAction {
    /// No task yet — submit one.
    Submit,
    /// A previous attempt ended in failure while the signal is still present —
    /// reset it so it runs again.
    Redrive,
    /// A task exists and has not failed — leave it alone.
    None,
}

fn intent_action(existing: Option<&TaskStatus>) -> IntentAction {
    match existing {
        None => IntentAction::Submit,
        Some(TaskStatus::Failed) => IntentAction::Redrive,
        Some(_) => IntentAction::None,
    }
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
    match intent_action(existing.as_ref().map(|t| &t.status)) {
        IntentAction::None => IntentOutcome::Tracked,
        IntentAction::Redrive => match orch.retry_task(&task_id).await {
            Ok(()) => IntentOutcome::Redriven,
            Err(e) => IntentOutcome::Failed(format!("retry {task_id}: {e}")),
        },
        IntentAction::Submit => {
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
/// than after a full cooldown. Returns true when the signal is in hand.
fn report_outcome(
    state: &mut SignalGuardState,
    key: &str,
    outcome: IntentOutcome,
    now: DateTime<Utc>,
) -> bool {
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
) {
    let now = Utc::now();
    let tasks = orch.get_all_tasks().await;
    let mut state = load_state().await;
    let mut dirty = false;

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
        report_outcome(&mut state, &key, outcome, now);
    }

    // 2. Queue backlog anomaly.
    let backlog = tasks
        .iter()
        .filter(|t| matches!(t.status, TaskStatus::Pending | TaskStatus::Scheduled))
        .count();
    if backlog >= config.backlog_threshold {
        let key = "backlog".to_string();
        if cooldown_elapsed(&state, &key, config.report_cooldown_secs, now) {
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
            report_outcome(&mut state, &key, outcome, now);
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
            let period = now.format("%Y%m%d").to_string();
            let goal = "Audit this repository for security and reliability \
                        defects, going over: hardcoded secrets or weak default \
                        credentials, injection surfaces (command/SQL/XSS), \
                        unsafe dependency or configuration patterns, and \
                        error paths that silently swallow failures. Pick the \
                        single most critical confirmed finding and implement \
                        its fix; list the remaining findings in the change \
                        description."
                .to_string();
            let key = format!("self-audit-{period}");
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
            if report_outcome(&mut state, &key, outcome, now) {
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
            let alerts = source.list_active_alerts(100).await;
            let selected = select_alerts(
                &alerts,
                &state,
                config.alert_channel_max_per_tick,
                config.report_cooldown_secs,
                now,
            );
            for alert in selected {
                let key = format!("alert:{}", alert.dedup_key);
                dirty = true;
                let hash = short_hash(&key);
                let goal = format!(
                    "Investigate and fix the root cause of firing alert \"{}\" \
                     (severity: {}): {}. Alert labels: {}. The alert fired at {} \
                     and is still active. Identify the underlying defect or \
                     resource condition, implement a durable fix, and explain \
                     how recurrence is prevented.",
                    alert.rule,
                    alert.severity,
                    alert.message,
                    alert.labels,
                    alert.fired_at.to_rfc3339(),
                );
                let outcome = submit_intent(
                    orch,
                    format!("self-signal-alert-{hash}"),
                    "self_signal",
                    goal,
                    serde_json::json!({
                        "kind": "persisted_alert",
                        "rule": alert.rule,
                        "dedup_key": alert.dedup_key,
                        "severity": alert.severity,
                        "labels": alert.labels,
                    }),
                )
                .await;
                report_outcome(&mut state, &key, outcome, now);
            }
        }
    }

    if dirty {
        save_state(&state).await;
    }
}

/// Background loop; follows the same shutdown pattern as the baseline port
/// trigger loop.
pub async fn run_signal_watcher_loop(
    orchestrator: Arc<dyn OrchestratorControl>,
    config: SignalWatcherConfig,
    shutdown: cog_core::ShutdownSignal,
    alert_source: Option<Arc<dyn cog_core::ActiveAlertSource>>,
) {
    let interval = Duration::from_secs(config.poll_interval_secs.max(60));
    info!(
        interval_secs = interval.as_secs(),
        failure_recurrence_threshold = config.failure_recurrence_threshold,
        backlog_threshold = config.backlog_threshold,
        self_audit_interval_secs = config.self_audit_interval_secs,
        alert_channel = alert_source.is_some() && config.alert_channel_enabled,
        "self-discovery signal watcher started"
    );
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                tick(&orchestrator, &config, alert_source.as_ref()).await;
            }
        }
    }
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

    #[test]
    fn a_submission_that_never_landed_does_not_spend_the_cooldown() {
        let mut state = SignalGuardState::default();
        let now = Utc::now();
        assert!(!report_outcome(
            &mut state,
            "alert:x",
            IntentOutcome::Failed("orchestrator unreachable".into()),
            now
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
        assert!(report_outcome(
            &mut state,
            "alert:x",
            IntentOutcome::Registered,
            now
        ));
        assert!(!cooldown_elapsed(&state, "alert:x", 86400, now));
        assert!(report_outcome(
            &mut state,
            "alert:y",
            IntentOutcome::Redriven,
            now
        ));
        assert!(!cooldown_elapsed(&state, "alert:y", 86400, now));
    }

    #[test]
    fn a_tracked_signal_stays_unrecorded_so_a_later_failure_is_seen() {
        let mut state = SignalGuardState::default();
        let now = Utc::now();
        assert!(report_outcome(
            &mut state,
            "alert:x",
            IntentOutcome::Tracked,
            now
        ));
        assert!(
            cooldown_elapsed(&state, "alert:x", 86400, now),
            "任务还在手上就不该记账：它一旦失败，下一轮要立刻能发现并重新驱动"
        );
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
            TaskStatus::Completed,
            TaskStatus::Cancelled,
        ] {
            assert_eq!(
                intent_action(Some(&live)),
                IntentAction::None,
                "{live:?} 不该再提交或重驱动"
            );
        }
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

        let picked = select_alerts(&alerts, &state, 1, 86400, now);
        assert_eq!(picked.len(), 1);
        assert_eq!(
            picked[0].dedup_key, "old",
            "上限卡的是本轮产出多少意图，先把窗口截断会让冷却中的告警长期挡住排在后面的"
        );
    }

    #[test]
    fn the_cap_still_bounds_how_many_alerts_one_tick_acts_on() {
        let now = Utc::now();
        let alerts = vec![firing("a"), firing("b"), firing("c")];
        let picked = select_alerts(&alerts, &SignalGuardState::default(), 2, 86400, now);
        assert_eq!(
            picked
                .iter()
                .map(|a| a.dedup_key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }
}
