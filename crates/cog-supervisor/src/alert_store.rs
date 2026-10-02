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
            SupervisorEvent::LlmUpstreamPoolDown {
                evidenced_recovery_unix,
                next_attempt_unix,
                unavailable,
                pool_size,
                quota_window_secs,
                timestamp,
            } => {
                // The sentence is built once and read by every consumer of this
                // event. Worded here separately it could disagree with the
                // notification egress, and two plausible sentences that
                // disagree are worse than either alone: nothing fails, so
                // nothing points at the one that is wrong.
                let message = cog_core::pool_down_verdict(
                    *pool_size,
                    unavailable,
                    *evidenced_recovery_unix,
                    *next_attempt_unix,
                    *quota_window_secs,
                );
                Some(Alert {
                    id: uuid::Uuid::new_v4().to_string(),
                    severity: AlertSeverity::Critical,
                    event_type: "llm_upstream_pool_down".to_string(),
                    message,
                    agent_id: None,
                    task_id: None,
                    crew_id: None,
                    timestamp: *timestamp,
                    resolved: false,
                })
            }
            SupervisorEvent::LlmUpstreamPoolRecovered { timestamp } => {
                Some(Alert {
                    id: uuid::Uuid::new_v4().to_string(),
                    severity: AlertSeverity::Info,
                    event_type: "llm_upstream_pool_down".to_string(),
                    message: "LLM upstream pool recovered; LLM-dependent tasks resumed".to_string(),
                    agent_id: None,
                    task_id: None,
                    crew_id: None,
                    timestamp: *timestamp,
                    resolved: true,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_down_maps_to_critical_unresolved_alert() {
        let event = SupervisorEvent::LlmUpstreamPoolDown {
            evidenced_recovery_unix: 1_789_315_200,
            next_attempt_unix: 1_789_314_600,
            unavailable: vec!["https://a.example|model-a".into()],
            pool_size: 4,
            quota_window_secs: 0,
            timestamp: Utc::now(),
        };
        let alert = AlertStore::event_to_alert(&event).expect("池全灭必须产生告警");
        assert_eq!(alert.event_type, "llm_upstream_pool_down");
        assert_eq!(alert.severity, AlertSeverity::Critical);
        assert!(!alert.resolved);
        assert!(
            alert.message.contains("2026-09-13"),
            "含上游报告的最早恢复时间: {}",
            alert.message
        );
        assert!(
            alert
                .message
                .contains("1 of 4 upstreams are suspect right now"),
            "嫌疑窗内那几家要报成池里的一部分，不能报成池的大小: {}",
            alert.message
        );
    }

    /// 上游一个恢复时刻都没报过时，告警必须直说"没有证据"，不能把退避窗到期
    /// 写成"最早恢复"——那等于替一个我们没有任何证据的外部系统下结论。
    #[test]
    fn pool_down_without_any_reported_reset_says_so() {
        let event = SupervisorEvent::LlmUpstreamPoolDown {
            evidenced_recovery_unix: 0,
            next_attempt_unix: 1_789_314_600,
            unavailable: vec!["https://a.example|model-a".into()],
            pool_size: 4,
            quota_window_secs: 0,
            timestamp: Utc::now(),
        };
        let alert = AlertStore::event_to_alert(&event).expect("没有恢复时刻也要告警");
        assert!(
            alert
                .message
                .contains("no upstream reported a recovery time"),
            "没有恢复证据就得直说没有: {}",
            alert.message
        );
        assert!(
            !alert
                .message
                .contains("earliest recovery reported by an upstream"),
            "没有任何上游报告过，就不能写成有: {}",
            alert.message
        );
        assert!(
            alert.message.contains("next attempt"),
            "退避节拍要作为独立的时刻报出来: {}",
            alert.message
        );
    }

    #[test]
    fn pool_recovered_maps_to_resolved_info_alert() {
        let event = SupervisorEvent::LlmUpstreamPoolRecovered {
            timestamp: Utc::now(),
        };
        let alert = AlertStore::event_to_alert(&event).expect("恢复必须产生告警");
        // 与 Down 同 event_type：满足告警状态机的同键 resolve 语义。
        assert_eq!(alert.event_type, "llm_upstream_pool_down");
        assert_eq!(alert.severity, AlertSeverity::Info);
        assert!(alert.resolved);
    }
}
