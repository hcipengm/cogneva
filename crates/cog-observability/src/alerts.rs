//! Alert notification routing: webhook / email / Slack.
//!
//! Where alerts come from is not decided here. The infra watcher polls
//! Prometheus rules and drives each series into the persistent `alerts` table,
//! and the supervisor event bridge maps events; both hand the resulting
//! `AlertEvent`s to `notify`. The rules this type holds are carried only as the
//! text for an event that arrives without a rendered sentence — nothing
//! evaluates them here, and the sentence the producer rendered wins (see
//! [`AlertManager::summary_for`]).
use cog_core::alerts::*;

/// Formats alert events and dispatches them to the configured channels.
use lettre::message::Message;
use lettre::transport::smtp::authentication::Credentials;
use lettre::AsyncTransport;
use lettre::{AsyncSmtpTransport, Tokio1Executor};

pub struct AlertManager {
    rules: Vec<AlertRule>,
    channels: Vec<AlertChannel>,
    timeout_secs: u64,
    client: Option<std::sync::Arc<dyn cog_core::HttpClient>>,
}

impl AlertManager {
    pub fn new(rules: Vec<AlertRule>, channels: Vec<AlertChannel>) -> Self {
        Self {
            rules,
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
                let payload = self.webhook_payload(events);

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
                let body = self.email_body(events);

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

    fn rule_summary(&self, rule_name: &str) -> String {
        self.rules
            .iter()
            .find(|r| r.name == rule_name)
            .map(|r| r.summary.clone())
            .unwrap_or_default()
    }

    /// The sentence a human is meant to read for one event.
    ///
    /// The producers render the rule's template against the sample that fired it
    /// and carry the result as the event's `message` label — the infra watcher
    /// (`alert_labels`), the supervisor bridge and the config-declaration watcher
    /// all do, so that sentence is written exactly once, where the value and the
    /// labels are still in hand. This type holds the rules only to have *some*
    /// text for an event that arrives without one; it cannot render, because
    /// rendering needs that sample and a placeholder naming neither the value nor
    /// a label is left in the text as written. Re-reading the template here
    /// therefore puts `{value}` in front of a reader, which is why the label
    /// wins whenever it is there.
    fn summary_for(&self, a: &AlertInstance) -> String {
        a.labels
            .get("message")
            .filter(|m| !m.is_empty())
            .cloned()
            .unwrap_or_else(|| self.rule_summary(&a.rule_name))
    }

    /// The Alertmanager-shaped payload for one batch of events.
    fn webhook_payload(&self, events: &[AlertEvent]) -> serde_json::Value {
        serde_json::json!({
            "alerts": events.iter().map(|e| match e {
                AlertEvent::Firing(a) => serde_json::json!({
                    "status": "firing",
                    "labels": a.labels,
                    "annotations": {
                        "summary": self.summary_for(a),
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
        })
    }

    /// The plain-text body for one batch of events, as an email carries it.
    fn email_body(&self, events: &[AlertEvent]) -> String {
        events
            .iter()
            .map(|e| {
                let summary = match e {
                    AlertEvent::Firing(a) | AlertEvent::Resolved(a) => self.summary_for(a),
                };
                let msg = AlertMessage::from_event(e, &summary);
                format!("{}\n{}\n---\n", msg.title, msg.body)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use cog_core::alerts::AlertRuleBuilder;

    /// A rule whose template is unfinished on purpose: if the payload ever goes
    /// back to reading it, the assertion below sees the placeholder.
    fn manager() -> AlertManager {
        AlertManager::new(
            vec![AlertRuleBuilder::new("r", "m")
                .summary("queue {queue} is at {value}")
                .build()],
            Vec::new(),
        )
    }

    fn event(labels: &[(&str, &str)]) -> AlertEvent {
        let now = Utc::now();
        AlertEvent::Firing(AlertInstance {
            rule_name: "r".into(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            state: AlertState::Firing,
            severity: AlertSeverity::Warning,
            value: 7.0,
            starts_at: now,
            ends_at: None,
            updated_at: now,
        })
    }

    /// The rendered sentence the producer already wrote wins over the template.
    #[test]
    fn the_rendered_message_is_what_goes_out() {
        let m = manager();
        let e = event(&[("queue", "build"), ("message", "queue build is at 7.00")]);

        let payload = m.webhook_payload(std::slice::from_ref(&e));
        assert_eq!(
            payload["alerts"][0]["annotations"]["summary"],
            serde_json::json!("queue build is at 7.00")
        );
        let body = m.email_body(std::slice::from_ref(&e));
        assert!(
            body.contains("**Summary**: queue build is at 7.00"),
            "email body carries the rendered sentence: {body}"
        );
        for out in [payload.to_string(), body] {
            assert!(
                !out.contains("{value}") && !out.contains("{queue}"),
                "no template placeholder may reach a reader: {out}"
            );
        }
    }

    /// An event with no rendered sentence still gets the rule's text, so the
    /// fallback stays visible instead of an empty field.
    #[test]
    fn an_event_without_a_message_falls_back_to_the_rule_text() {
        let m = manager();
        let e = event(&[("queue", "build")]);
        let payload = m.webhook_payload(std::slice::from_ref(&e));
        assert_eq!(
            payload["alerts"][0]["annotations"]["summary"],
            serde_json::json!("queue {queue} is at {value}")
        );
    }
}
