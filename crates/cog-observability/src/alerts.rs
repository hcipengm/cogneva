//! Alertmanager — alerting rules, state machine, and notification routing.
/// - Alert rules defined as ConfigMap-managed YAML (Phase 1: code config)
/// - Rule evaluation against Prometheus metrics
/// - Notification routing: webhook / email / Slack
/// - State machine: Pending → Firing → Resolved
///   **Phase 1**: in-memory evaluation + async webhook / SMTP / Slack dispatch.
///   **Phase 2**: persistent alert history, alert grouping, silences.
use chrono::Utc;
use cog_core::alerts::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Lightweight in-memory alert manager.
/// Evaluates rules synchronously (fast path) and dispatches
/// notifications asynchronously (background task).
use lettre::message::Message;
use lettre::transport::smtp::authentication::Credentials;
use lettre::AsyncTransport;
use lettre::{AsyncSmtpTransport, Tokio1Executor};

pub struct AlertManager {
    rules: Vec<AlertRule>,
    active: std::sync::Mutex<Vec<AlertInstance>>,
    channels: Vec<AlertChannel>,
    timeout_secs: u64,
    client: Option<std::sync::Arc<dyn cog_core::HttpClient>>,
}

impl AlertManager {
    pub fn new(rules: Vec<AlertRule>, channels: Vec<AlertChannel>) -> Self {
        Self {
            rules,
            active: std::sync::Mutex::new(Vec::new()),
            channels,
            timeout_secs: 10,
            client: None,
        }
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    pub fn with_client(mut self, client: std::sync::Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Evaluate a single metric sample against all rules.
    /// Callers (e.g. `PrometheusMetricsBackend` or task recorders) invoke
    /// this every time a metric is recorded so alerts react immediately.
    pub fn evaluate(
        &self,
        metric_name: &str,
        labels: &HashMap<String, String>,
        value: f64,
    ) -> Vec<AlertEvent> {
        let mut events = Vec::new();
        let now = Utc::now();

        for rule in &self.rules {
            if rule.metric_name != metric_name {
                continue;
            }
            if !Self::labels_match(&rule.label_matchers, labels) {
                continue;
            }

            let condition_met = rule.condition.evaluate(value);
            let mut active = self.active.lock().unwrap();

            let existing = active
                .iter_mut()
                .find(|a| a.rule_name == rule.name && Self::labels_match(&a.labels, labels));

            if condition_met {
                if let Some(inst) = existing {
                    inst.value = value;
                    inst.updated_at = now;
                    // Transition Pending → Firing if duration exceeded
                    if inst.state == AlertState::Pending {
                        let elapsed = (now - inst.starts_at).num_seconds() as u64;
                        if elapsed >= rule.duration_sec {
                            inst.state = AlertState::Firing;
                            events.push(AlertEvent::Firing(inst.clone()));
                        }
                    }
                } else {
                    let inst = AlertInstance {
                        rule_name: rule.name.clone(),
                        labels: labels.clone(),
                        state: AlertState::Pending,
                        severity: rule.severity,
                        value,
                        starts_at: now,
                        ends_at: None,
                        updated_at: now,
                    };
                    // If duration is 0, fire immediately
                    if rule.duration_sec == 0 {
                        let mut firing = inst.clone();
                        firing.state = AlertState::Firing;
                        active.push(firing.clone());
                        events.push(AlertEvent::Firing(firing));
                    } else {
                        active.push(inst);
                    }
                }
            } else if let Some(inst) = existing {
                // Condition no longer met → resolve
                inst.state = AlertState::Resolved;
                inst.ends_at = Some(now);
                inst.updated_at = now;
                let resolved = inst.clone();
                // Remove from active list
                active.retain(|a| {
                    !(a.rule_name == rule.name && Self::labels_match(&a.labels, labels))
                });
                events.push(AlertEvent::Resolved(resolved));
            }
        }

        events
    }

    /// Scan the active alert list and auto-resolve any alerts whose
    /// underlying metric has not been seen recently.
    pub fn resolve_stale(&self, max_age_sec: u64) -> Vec<AlertEvent> {
        let now = Utc::now();
        let mut active = self.active.lock().unwrap();
        let mut resolved = Vec::new();

        active.retain(|a| {
            let age = (now - a.updated_at).num_seconds() as u64;
            if age > max_age_sec && a.state != AlertState::Resolved {
                let mut r = a.clone();
                r.state = AlertState::Resolved;
                r.ends_at = Some(now);
                r.updated_at = now;
                resolved.push(AlertEvent::Resolved(r));
                false
            } else {
                true
            }
        });

        resolved
    }

    /// List currently active (Pending or Firing) alerts.
    pub fn active_alerts(&self) -> Vec<AlertInstance> {
        let active = self.active.lock().unwrap();
        active
            .iter()
            .filter(|a| a.state != AlertState::Resolved)
            .cloned()
            .collect()
    }

    /// List only firing alerts.
    pub fn firing_alerts(&self) -> Vec<AlertInstance> {
        let active = self.active.lock().unwrap();
        active
            .iter()
            .filter(|a| a.state == AlertState::Firing)
            .cloned()
            .collect()
    }

    /// Dispatch notifications for a batch of alert events.
    /// Sends via every configured channel in parallel (fire-and-forget).
    pub async fn notify(&self, events: &[AlertEvent]) {
        if events.is_empty() {
            return;
        }
        for channel in &self.channels {
            if let Err(e) = self.send_to_channel(channel, events).await {
                tracing::warn!(channel = ?channel, error = %e, "Alert notification failed");
            }
        }
    }

    async fn send_to_channel(
        &self,
        channel: &AlertChannel,
        events: &[AlertEvent],
    ) -> Result<(), anyhow::Error> {
        match channel {
            AlertChannel::Webhook { url, headers } => {
                let client = self
                    .client
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("AlertManager has no HttpClient configured"))?;
                let payload = serde_json::json!({
                    "alerts": events.iter().map(|e| match e {
                        AlertEvent::Firing(a) => serde_json::json!({
                            "status": "firing",
                            "labels": a.labels,
                            "annotations": {
                                "summary": self.rule_summary(&a.rule_name),
                            },
                            "startsAt": a.starts_at,
                            "severity": a.severity.as_str(),
                            "value": a.value,
                        }),
                        AlertEvent::Resolved(a) => serde_json::json!({
                            "status": "resolved",
                            "labels": a.labels,
                            "endsAt": a.ends_at,
                            "severity": a.severity.as_str(),
                        }),
                    }).collect::<Vec<_>>(),
                    "version": "1",
                });

                let mut req = cog_core::HttpRequest::post(url)
                    .json(&payload)
                    .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
                    .timeout(self.timeout_secs);
                for (k, v) in headers {
                    req = req.header(k, v);
                }
                let resp = client.execute(req).await?;
                if !resp.is_success() {
                    return Err(anyhow::anyhow!("Webhook returned {}", resp.status));
                }
                Ok(())
            }
            AlertChannel::Email {
                smtp_config,
                to,
                subject_template,
            } => {
                if to.is_empty() {
                    return Ok(());
                }
                let body = events
                    .iter()
                    .map(|e| {
                        let rule_name = match e {
                            AlertEvent::Firing(a) | AlertEvent::Resolved(a) => a.rule_name.clone(),
                        };
                        let msg = AlertMessage::from_event(e, &self.rule_summary(&rule_name));
                        format!("{}\n{}\n---\n", msg.title, msg.body)
                    })
                    .collect::<String>();

                let subject = if subject_template.is_empty() {
                    format!(
                        "Cogneva Alert — {} event{}",
                        events.len(),
                        if events.len() == 1 { "" } else { "s" }
                    )
                } else {
                    subject_template.clone()
                };

                // Build lettre email
                let from = smtp_config
                    .from_address
                    .parse::<lettre::message::Mailbox>()
                    .map_err(|e| anyhow::anyhow!("Invalid from address: {}", e))?;

                let mut builder = Message::builder().from(from).subject(subject);

                for addr in to {
                    let mbox = addr
                        .parse::<lettre::message::Mailbox>()
                        .map_err(|e| anyhow::anyhow!("Invalid to address '{}': {}", addr, e))?;
                    builder = builder.to(mbox);
                }

                let email = builder
                    .body(body)
                    .map_err(|e| anyhow::anyhow!("Email build failed: {}", e))?;

                let builder = if smtp_config.use_tls {
                    AsyncSmtpTransport::<Tokio1Executor>::relay(&smtp_config.host)
                        .map_err(|e| anyhow::anyhow!("SMTP relay build failed: {}", e))?
                        .port(smtp_config.port)
                } else {
                    AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&smtp_config.host)
                        .port(smtp_config.port)
                };

                let builder = if let Some(ref user) = smtp_config.username {
                    let creds = Credentials::new(
                        user.clone(),
                        smtp_config.password.clone().unwrap_or_default(),
                    );
                    builder.credentials(creds)
                } else {
                    builder
                };

                let transport = builder.build();
                transport
                    .send(email)
                    .await
                    .map_err(|e| anyhow::anyhow!("SMTP send failed: {}", e))?;

                Ok(())
            }
            AlertChannel::Slack {
                webhook_url,
                channel,
            } => {
                let text = format!(
                    "*Cogneva Alerts* ({} events)\n\n{}",
                    events.len(),
                    events
                        .iter()
                        .map(|e| match e {
                            AlertEvent::Firing(a) => format!(
                                "🔥 *{}* — `{}` = {:.2}",
                                a.severity.as_str().to_uppercase(),
                                a.rule_name,
                                a.value
                            ),
                            AlertEvent::Resolved(a) => format!("✅ *RESOLVED* — `{}`", a.rule_name),
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                let payload = serde_json::json!({
                    "channel": channel,
                    "text": text,
                });
                let client = self
                    .client
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("AlertManager has no HttpClient configured"))?;
                let req = cog_core::HttpRequest::post(webhook_url)
                    .header("Content-Type", "application/json")
                    .json(&payload)
                    .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
                    .timeout(self.timeout_secs);
                let resp = client.execute(req).await?;
                if !resp.is_success() {
                    return Err(anyhow::anyhow!("Slack webhook returned {}", resp.status));
                }
                Ok(())
            }
        }
    }

    fn labels_match(matchers: &HashMap<String, String>, labels: &HashMap<String, String>) -> bool {
        matchers
            .iter()
            .all(|(k, v)| labels.get(k).map(|s| s == v).unwrap_or(false))
    }

    fn rule_summary(&self, rule_name: &str) -> String {
        self.rules
            .iter()
            .find(|r| r.name == rule_name)
            .map(|r| r.summary.clone())
            .unwrap_or_default()
    }
}

/// Background evaluation loop for the alert manager.
/// When using `PrometheusMetricsBackend`, metrics are push-based into
/// the Registry.  The evaluation loop auto-resolves stale alerts.
pub async fn alert_manager_loop(
    manager: Arc<AlertManager>,
    stale_check_interval_sec: u64,
    stale_threshold_sec: u64,
) {
    let mut interval =
        tokio::time::interval(std::time::Duration::from_secs(stale_check_interval_sec));
    loop {
        interval.tick().await;
        let events = manager.resolve_stale(stale_threshold_sec);
        if !events.is_empty() {
            manager.notify(&events).await;
        }
    }
}
