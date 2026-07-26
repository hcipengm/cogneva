use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::broadcast;

use cog_core::{AlertSeverity, SupervisorEvent};

/// Maximum number of alerts retained in memory.
pub const ALERT_HISTORY_MAX: usize = 10_000;

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

/// In-memory alert store that subscribes to SupervisorEvent broadcast.
pub struct AlertStore {
    alerts: Mutex<VecDeque<Alert>>,
    max_alerts: usize,
}

impl Default for AlertStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AlertStore {
    pub fn new() -> Self {
        Self::with_max_alerts(ALERT_HISTORY_MAX)
    }

    pub fn with_max_alerts(max: usize) -> Self {
        Self {
            alerts: Mutex::new(VecDeque::with_capacity(max)),
            max_alerts: max,
        }
    }

    /// Subscribe to a SupervisorEvent broadcast channel and persist alert-worthy events.
    pub async fn run(&self, mut rx: broadcast::Receiver<SupervisorEvent>) {
        while let Ok(event) = rx.recv().await {
            if let Some(alert) = Self::event_to_alert(&event) {
                let mut alerts = self.alerts.lock().unwrap();
                alerts.push_back(alert);
                while alerts.len() > self.max_alerts {
                    alerts.pop_front();
                }
            }
        }
    }

    /// Convert a SupervisorEvent into an Alert if it is alert-worthy.
    fn event_to_alert(event: &SupervisorEvent) -> Option<Alert> {
        match event {
            SupervisorEvent::AgentUnhealthy { agent_id, issue, timestamp } => {
                let (severity, msg) = match issue {
                    cog_core::HealthIssue::Suspect { missed_beats } => (
                        AlertSeverity::Warning,
                        format!("Agent suspect: missed {missed_beats} beats"),
                    ),
                    cog_core::HealthIssue::Dead { .. } => (
                        AlertSeverity::Critical,
                        "Agent declared dead".to_string(),
                    ),
                    cog_core::HealthIssue::Stuck { stuck_seconds } => (
                        AlertSeverity::Warning,
                        format!("Agent stuck for {stuck_seconds}s"),
                    ),
                    cog_core::HealthIssue::StateBackendDead => (
                        AlertSeverity::Critical,
                        "Agent dead (state backend)".to_string(),
                    ),
                };
                Some(Alert {
                    id: uuid::Uuid::new_v4().to_string(),
                    severity,
                    event_type: "agent_unhealthy".to_string(),
                    message: msg,
                    agent_id: Some(agent_id.clone()),
                    task_id: None,
                    crew_id: None,
                    timestamp: *timestamp,
                    resolved: false,
                })
            }
            SupervisorEvent::QuotaThresholdBreached { workspace_id, remaining, threshold, scheduler_paused, timestamp } => {
                Some(Alert {
                    id: uuid::Uuid::new_v4().to_string(),
                    severity: AlertSeverity::Critical,
                    event_type: "quota_threshold_breached".to_string(),
                    message: format!("Quota breached: remaining={remaining}, threshold={threshold}, paused={scheduler_paused}"),
                    agent_id: None,
                    task_id: None,
                    crew_id: Some(workspace_id.clone()),
                    timestamp: *timestamp,
                    resolved: false,
                })
            }
            SupervisorEvent::AgentResourceAlert { agent_id, metric, threshold, current, timestamp } => {
                Some(Alert {
                    id: uuid::Uuid::new_v4().to_string(),
                    severity: AlertSeverity::Warning,
                    event_type: "agent_resource_alert".to_string(),
                    message: format!("Resource alert: {metric}={current:.2}, threshold={threshold:.2}"),
                    agent_id: Some(agent_id.clone()),
                    task_id: None,
                    crew_id: None,
                    timestamp: *timestamp,
                    resolved: false,
                })
            }
            SupervisorEvent::TaskDeadLetter { task_id, agent_id, crew_id, retry_count, timestamp } => {
                Some(Alert {
                    id: uuid::Uuid::new_v4().to_string(),
                    severity: AlertSeverity::Critical,
                    event_type: "task_dead_letter".to_string(),
                    message: format!("Task sent to DLQ after {retry_count} retries"),
                    agent_id: agent_id.clone(),
                    task_id: Some(task_id.clone()),
                    crew_id: crew_id.clone(),
                    timestamp: *timestamp,
                    resolved: false,
                })
            }
            SupervisorEvent::SquadRespawnRequested { crew_id, reason, timestamp, .. } => {
                Some(Alert {
                    id: uuid::Uuid::new_v4().to_string(),
                    severity: AlertSeverity::Warning,
                    event_type: "squad_respawn_requested".to_string(),
                    message: format!("Squad respawn requested: {reason}"),
                    agent_id: None,
                    task_id: None,
                    crew_id: Some(crew_id.clone()),
                    timestamp: *timestamp,
                    resolved: false,
                })
            }
            _ => None,
        }
    }

    /// List active (unresolved) alerts, newest first, up to `limit`.
    pub fn list_active(&self, limit: usize) -> Vec<Alert> {
        let alerts = self.alerts.lock().unwrap();
        alerts
            .iter()
            .rev()
            .filter(|a| !a.resolved)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Total number of alerts in the store.
    pub fn len(&self) -> usize {
        self.alerts.lock().unwrap().len()
    }

    /// Whether the store contains no alerts.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl cog_core::AlertStore for AlertStore {
    fn list_active(&self, limit: usize) -> Vec<cog_core::Alert> {
        self.list_active(limit)
            .into_iter()
            .map(|a| cog_core::Alert {
                id: a.id,
                severity: a.severity,
                event_type: a.event_type,
                message: a.message,
                agent_id: a.agent_id,
                task_id: a.task_id,
                crew_id: a.crew_id,
                timestamp: a.timestamp,
                resolved: a.resolved,
            })
            .collect()
    }
}
