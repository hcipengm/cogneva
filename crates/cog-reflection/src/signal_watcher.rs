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
//!
//! Every intent uses a deterministic id so the orchestrator's idempotent
//! skip dedupes across ticks and pod restarts; a local state file adds
//! cooldowns so a persisting signal re-reports instead of going silent.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

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

/// True when `key` was never reported or its cooldown elapsed; records the
/// report time when reporting is allowed.
fn may_report(
    state: &mut SignalGuardState,
    key: &str,
    cooldown_secs: i64,
    now: DateTime<Utc>,
) -> bool {
    match state.reported.get(key) {
        Some(last) if (now - *last).num_seconds() < cooldown_secs => false,
        _ => {
            state.reported.insert(key.to_string(), now);
            true
        }
    }
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

/// Submit one internal evolution intent. Deterministic id → orchestrator
/// idempotent skip dedupes repeats across ticks and restarts.
async fn submit_intent(
    orch: &Arc<dyn OrchestratorControl>,
    task_id: String,
    task_kind: &str,
    goal: String,
    detail: serde_json::Value,
) {
    let task = Task::new(
        task_id.clone(),
        TaskType::Custom(task_kind.into()),
        serde_json::json!({
            "goal": goal,
            "evolution_mode": "generate_change",
            "task_kind": task_kind,
            "signal": detail,
        }),
    );
    match orch.submit_goal_auto(&goal, vec![task]).await {
        Ok(ids) => info!(task = %task_id, tasks = ?ids, "self-discovery intent submitted"),
        Err(e) => warn!(task = %task_id, error = %e, "self-discovery intent submit failed"),
    }
}

/// One watcher tick: scan task state, emit intents for active signals.
async fn tick(orch: &Arc<dyn OrchestratorControl>, config: &SignalWatcherConfig) {
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
        if !may_report(&mut state, &key, config.report_cooldown_secs, now) {
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
        submit_intent(
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
    }

    // 2. Queue backlog anomaly.
    let backlog = tasks
        .iter()
        .filter(|t| matches!(t.status, TaskStatus::Pending | TaskStatus::Scheduled))
        .count();
    if backlog >= config.backlog_threshold {
        let key = "backlog".to_string();
        if may_report(&mut state, &key, config.report_cooldown_secs, now) {
            dirty = true;
            let dlq = orch.dlq_len().await.unwrap_or(0);
            let goal = format!(
                "Investigate orchestrator queue backlog: {backlog} tasks \
                 pending/scheduled (DLQ: {dlq}). Determine why tasks are not \
                 draining and fix the bottleneck."
            );
            submit_intent(
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
        }
    }

    // 3. Periodic self-audit of the own repository.
    if config.self_audit_interval_secs > 0 {
        let due = match state.last_audit {
            Some(last) => (now - last).num_seconds() >= config.self_audit_interval_secs,
            None => true,
        };
        if due {
            state.last_audit = Some(now);
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
            submit_intent(
                orch,
                format!("self-audit-{period}"),
                "self_audit",
                goal,
                serde_json::json!({
                    "kind": "self_audit",
                    "period": period,
                }),
            )
            .await;
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
) {
    let interval = Duration::from_secs(config.poll_interval_secs.max(60));
    info!(
        interval_secs = interval.as_secs(),
        failure_recurrence_threshold = config.failure_recurrence_threshold,
        backlog_threshold = config.backlog_threshold,
        self_audit_interval_secs = config.self_audit_interval_secs,
        "self-discovery signal watcher started"
    );
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                tick(&orchestrator, &config).await;
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
    fn may_report_enforces_cooldown() {
        let mut state = SignalGuardState::default();
        let now = Utc::now();
        assert!(may_report(&mut state, "k", 3600, now));
        assert!(!may_report(&mut state, "k", 3600, now));
        let later = now + chrono::Duration::seconds(3601);
        assert!(may_report(&mut state, "k", 3600, later));
    }
}
