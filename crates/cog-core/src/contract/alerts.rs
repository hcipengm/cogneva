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
