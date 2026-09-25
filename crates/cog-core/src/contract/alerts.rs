//!Core alert types shared across crates.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Severity level for an alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

impl AlertSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertSeverity::Critical => "critical",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Info => "info",
        }
    }
}

/// Condition operator for a threshold rule.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertCondition {
    GreaterThan(f64),
    LessThan(f64),
    Equal(f64),
    NotEqual(f64),
    GreaterThanOrEqual(f64),
    LessThanOrEqual(f64),
}

impl AlertCondition {
    /// Evaluate the condition against a value.
    pub fn evaluate(&self, value: f64) -> bool {
        match self {
            AlertCondition::GreaterThan(th) => value > *th,
            AlertCondition::LessThan(th) => value < *th,
            AlertCondition::Equal(th) => (value - *th).abs() < f64::EPSILON,
            AlertCondition::NotEqual(th) => (value - *th).abs() >= f64::EPSILON,
            AlertCondition::GreaterThanOrEqual(th) => value >= *th,
            AlertCondition::LessThanOrEqual(th) => value <= *th,
        }
    }
}

/// A single alerting rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub name: String,
    pub metric_name: String,
    /// Labels that must match for the rule to apply.
    pub label_matchers: HashMap<String, String>,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    /// How long the condition must hold before firing (seconds).
    pub duration_sec: u64,
    /// Human-readable summary template.
    pub summary: String,
    /// Extra annotations (description, runbook_url, etc.).
    pub annotations: HashMap<String, String>,
}

/// Lifecycle state of an alert instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertState {
    /// Condition met but not yet for the full duration.
    Pending,
    /// Condition held for the required duration.
    Firing,
    /// Condition no longer met.
    Resolved,
}

/// An active alert instance (one firing occurrence of a rule).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertInstance {
    pub rule_name: String,
    pub labels: HashMap<String, String>,
    pub state: AlertState,
    pub severity: AlertSeverity,
    /// Last observed metric value.
    pub value: f64,
    /// When the condition was first observed.
    pub starts_at: DateTime<Utc>,
    /// When the alert was resolved (None if still active).
    pub ends_at: Option<DateTime<Utc>>,
    /// When the state last changed.
    pub updated_at: DateTime<Utc>,
}

/// SMTP configuration for Email alerts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    /// Optional SMTP username for authentication.
    pub username: Option<String>,
    /// Optional SMTP password for authentication.
    pub password: Option<String>,
    /// From address for all outgoing alert emails.
    pub from_address: String,
    /// Use TLS (default: true).
    #[serde(default = "default_true")]
    pub use_tls: bool,
}

fn default_true() -> bool {
    true
}

impl Default for SmtpConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 587,
            username: None,
            password: None,
            from_address: "alerts@cogneva.local".into(),
            use_tls: true,
        }
    }
}

/// Unified notification channel for alert dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AlertChannel {
    Webhook {
        url: String,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
    Email {
        smtp_config: SmtpConfig,
        to: Vec<String>,
        #[serde(default)]
        subject_template: String,
    },
    Slack {
        webhook_url: String,
        channel: String,
    },
}

/// Legacy alias for `AlertChannel`.
pub type NotificationChannel = AlertChannel;

/// Alert event produced by evaluation.
#[derive(Debug, Clone)]
pub enum AlertEvent {
    Firing(AlertInstance),
    Resolved(AlertInstance),
}

/// Templated alert message for human-readable rendering.
#[derive(Debug, Clone)]
pub struct AlertMessage {
    pub title: String,
    pub body: String,
    pub severity: AlertSeverity,
}

impl AlertMessage {
    /// Render a standard Markdown/plain-text alert message from an event.
    pub fn from_event(event: &AlertEvent, rule_summary: &str) -> Self {
        match event {
            AlertEvent::Firing(a) => Self {
                title: format!("[{}] {} is firing", a.severity.as_str().to_uppercase(), a.rule_name),
                body: format!(
                    "**Rule**: {}\n**Severity**: {}\n**Value**: {:.2}\n**Summary**: {}\n**Started at**: {}",
                    a.rule_name,
                    a.severity.as_str(),
                    a.value,
                    rule_summary,
                    a.starts_at.to_rfc3339()
                ),
                severity: a.severity,
            },
            AlertEvent::Resolved(a) => Self {
                title: format!("[RESOLVED] {}", a.rule_name),
                body: format!(
                    "**Rule**: {}\n**Severity**: {}\n**Resolved at**: {}\n**Duration**: ~{:.0}s",
                    a.rule_name,
                    a.severity.as_str(),
                    a.ends_at.map(|d| d.to_rfc3339()).unwrap_or_else(|| "unknown".into()),
                    if let Some(end) = a.ends_at {
                        (end - a.starts_at).num_seconds() as f64
                    } else {
                        0.0
                    }
                ),
                severity: AlertSeverity::Info,
            },
        }
    }
}

/// Convenience builder for alert rules.
pub struct AlertRuleBuilder {
    rule: AlertRule,
}

impl AlertRuleBuilder {
    pub fn new(name: impl Into<String>, metric_name: impl Into<String>) -> Self {
        Self {
            rule: AlertRule {
                name: name.into(),
                metric_name: metric_name.into(),
                label_matchers: HashMap::new(),
                condition: AlertCondition::GreaterThan(0.0),
                severity: AlertSeverity::Warning,
                duration_sec: 60,
                summary: String::new(),
                annotations: HashMap::new(),
            },
        }
    }

    pub fn condition(mut self, c: AlertCondition) -> Self {
        self.rule.condition = c;
        self
    }

    pub fn severity(mut self, s: AlertSeverity) -> Self {
        self.rule.severity = s;
        self
    }

    pub fn duration_sec(mut self, sec: u64) -> Self {
        self.rule.duration_sec = sec;
        self
    }

    pub fn match_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.rule.label_matchers.insert(key.into(), value.into());
        self
    }

    pub fn summary(mut self, s: impl Into<String>) -> Self {
        self.rule.summary = s.into();
        self
    }

    pub fn annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.rule.annotations.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> AlertRule {
        self.rule
    }
}

/// A single alert entry derived from a SupervisorEvent.
#[derive(Debug, Clone)]
pub struct Alert {
    pub id: String,
    pub severity: AlertSeverity,
    pub event_type: String,
    pub message: String,
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub crew_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub resolved: bool,
}

/// Alert store trait — abstracts in-memory or persistent alert storage.
pub trait AlertStore: Send + Sync {
    /// List active (unresolved) alerts, newest first, up to `limit`.
    fn list_active(&self, limit: usize) -> Vec<Alert>;
}

/// Storage-agnostic view of one persisted alert row. Produced by the
/// observability plugin's PostgreSQL-backed store and consumed by
/// self-discovery (cog-reflection), which must react to firing alerts
/// without depending on the storage crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedAlert {
    pub rule: String,
    /// Stable identity of the alert instance; re-raising the same condition
    /// reuses the same key, so consumers can dedup on it across restarts.
    pub dedup_key: String,
    pub severity: String,
    /// `firing` or `resolved`.
    pub state: String,
    pub message: String,
    pub labels: serde_json::Value,
    pub fired_at: DateTime<Utc>,
    /// When the condition was last evaluated and still found true. `fired_at`
    /// says since when, this says whether anyone is still looking: a consumer
    /// that only reads the edge cannot tell a condition being confirmed every
    /// tick from a row whose producer stopped. `None` when the row carries no
    /// sighting yet.
    pub last_seen_at: Option<DateTime<Utc>>,
}

/// Read-side handle over persisted alerts. Published by whichever plugin owns
/// alert persistence so other crates can turn firing alerts into work.
#[async_trait::async_trait]
pub trait ActiveAlertSource: Send + Sync {
    /// Alerts currently in the `firing` state, newest first.
    async fn list_active_alerts(&self, limit: i64) -> Vec<PersistedAlert>;
}

/// Rule name for alerts raised when goal decomposition ends without any
/// executable task after the bounded retries.
pub const ALERT_RULE_DECOMPOSITION_EMPTY: &str = "decomposition_empty";
/// Rule name for alerts raised by the stalled-orphan reconciler: a
/// non-executable parent placeholder with no children stuck pending.
pub const ALERT_RULE_DECOMPOSITION_ORPHANED: &str = "decomposition_orphaned";
/// Rule name for alerts raised when the promotion ledger has gone whole weeks
/// without a single promotion. Distinct from the trend rule: a flat zero has no
/// decided samples, so every success-rate comparison skips it and the loudest
/// possible failure reads as an idle system.
pub const ALERT_RULE_PROMOTION_STALL: &str = "promotion_stall";

/// A persistent alert condition a plugin wants driven into the alert state
/// machine. Mirrors the storage crate's `NewAlert` without coupling callers
/// to the concrete PostgreSQL store.
#[derive(Debug, Clone)]
pub struct PersistentAlertDraft {
    pub rule: String,
    /// Stable identity of this alert instance; re-evaluating the same
    /// condition must reuse the same key.
    pub dedup_key: String,
    pub severity: String,
    pub message: String,
    pub labels: serde_json::Value,
}

/// Longest an alert label value may be before it is truncated.
pub const ALERT_LABEL_VALUE_MAX_CHARS: usize = 1024;

/// Truncate on a character boundary and mark that it happened.
///
/// A silent cut reads as the whole value; the marker is what keeps a
/// truncated label from being mistaken for a complete one.
pub fn clamp_label_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max_chars).collect();
    out.push('…');
    out
}

/// Bounded, label-safe summary of a task input for an alert about that task.
///
/// The task id stays in the labels as the pointer to the full input; this is
/// only the excerpt a reader gets inline. It must be bounded because labels do
/// not stay in the alert: self-discovery inlines them into a new goal, that
/// goal becomes the next task's input, and when that task stalls its input
/// becomes labels again. Whatever a label carries therefore comes back one
/// generation deeper, so a label that embeds a whole input makes every
/// re-drive cost more than the last for the same non-progress.
pub fn bounded_task_goal(input: Option<&serde_json::Value>, max_chars: usize) -> String {
    let text = input
        .and_then(|v| v.get("goal"))
        .and_then(|g| g.as_str())
        .unwrap_or_default();
    clamp_label_text(text, max_chars)
}

/// Clamp every value of an alert label set to a bounded size.
///
/// The consumer applies this because the producer may predate the bound: an
/// evolution deployment is ahead of the control plane that raised the alert,
/// and only the consumer knows how much text it is about to embed in a prompt.
/// Nested objects and arrays are flattened to a clamped string rather than
/// descended into, because depth is exactly what the label chain grows by.
pub fn bound_alert_labels(labels: &serde_json::Value, max_chars: usize) -> serde_json::Value {
    match labels {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        match v {
                            serde_json::Value::String(s) => {
                                serde_json::Value::String(clamp_label_text(s, max_chars))
                            }
                            serde_json::Value::Number(_)
                            | serde_json::Value::Bool(_)
                            | serde_json::Value::Null => v.clone(),
                            nested => serde_json::Value::String(clamp_label_text(
                                &nested.to_string(),
                                max_chars,
                            )),
                        },
                    )
                })
                .collect(),
        ),
        other => serde_json::Value::String(clamp_label_text(&other.to_string(), max_chars)),
    }
}

/// Write-side handle over persisted alerts, symmetric to
/// [`ActiveAlertSource`]. Producers that detect durable fault conditions
/// (stalled DAG tasks, exhausted retries) raise alerts through this port so
/// self-discovery can turn them into work without depending on the storage
/// crate.
#[async_trait::async_trait]
pub trait PersistentAlertSink: Send + Sync {
    /// Drive one alert condition: `true` guarantees a firing row, `false`
    /// resolves the row for this dedup key. Errors are returned, never
    /// panicked: an alerting hiccup must not break the caller's main path.
    async fn set_persistent_alert(
        &self,
        condition: bool,
        draft: &PersistentAlertDraft,
    ) -> Result<(), String>;

    /// Firing alerts whose rule starts with `rule_prefix`, newest first.
    /// Used by supervisors to adopt rows raised before a restart.
    async fn list_active_persistent_alerts(
        &self,
        rule_prefix: &str,
        limit: i64,
    ) -> Vec<PersistedAlert>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_label_text_leaves_short_text_untouched() {
        assert_eq!(clamp_label_text("ok", 16), "ok");
        // Exactly at the cap is not a truncation.
        assert_eq!(clamp_label_text("abcd", 4), "abcd");
    }

    #[test]
    fn clamp_label_text_truncates_on_a_char_boundary_and_says_so() {
        let text = "刷新接口未区分访问令牌".repeat(10);
        let clamped = clamp_label_text(&text, 5);
        assert_eq!(clamped, "刷新接口未…");
        assert!(
            clamped.ends_with('…'),
            "a silent cut reads as a whole value"
        );
    }

    #[test]
    fn bounded_task_goal_reads_the_goal_and_caps_it() {
        let input = serde_json::json!({ "goal": "g".repeat(100), "other": "ignored" });
        assert_eq!(
            bounded_task_goal(Some(&input), 10),
            format!("{}…", "g".repeat(10))
        );
    }

    #[test]
    fn bounded_task_goal_of_a_missing_or_shapeless_input_is_empty() {
        assert_eq!(bounded_task_goal(None, 10), "");
        // A task whose input has no string goal yields nothing rather than a
        // serialized blob: the labels are not a place to smuggle the input in.
        assert_eq!(
            bounded_task_goal(Some(&serde_json::json!({"goal": 7})), 10),
            ""
        );
    }

    #[test]
    fn bound_alert_labels_clamps_values_and_flattens_nesting() {
        let nested = serde_json::json!({"original_input": {"goal": "x".repeat(500)}});
        let bounded = bound_alert_labels(
            &serde_json::json!({
                "goal_id": "g1",
                "attempts": 3,
                "source": "orphan_reconciler",
                "original_input": nested,
            }),
            64,
        );
        assert_eq!(bounded["goal_id"], "g1");
        assert_eq!(bounded["attempts"], 3);
        let flattened = bounded["original_input"]
            .as_str()
            .expect("a nested object must not survive as nesting");
        assert!(flattened.chars().count() <= 65, "over the cap: {flattened}");
        assert!(flattened.ends_with('…'));
    }

    #[test]
    fn bound_alert_labels_caps_a_bare_string() {
        let bounded = bound_alert_labels(&serde_json::json!("y".repeat(100)), 8);
        // A non-object label set is flattened by serializing it, so the clamped
        // text starts with the opening quote of that serialization.
        let text = bounded.as_str().expect("a bare value flattens to a string");
        assert!(text.ends_with('…'));
        assert!(text.chars().count() <= 9, "over the cap: {text}");
    }
}
