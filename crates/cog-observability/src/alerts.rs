//! Alert notification routing: webhook / email / Slack.
//!
//! Where alerts come from is not decided here. The infra watcher polls
//! Prometheus rules and drives each series into the persistent `alerts` table,
//! and the supervisor event bridge maps events; both hand the resulting
//! `AlertEvent`s to `notify`. The rules this type holds are carried only so a
//! webhook payload can resolve a rule's summary text — nothing evaluates them
//! here.
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

    fn rule_summary(&self, rule_name: &str) -> String {
        self.rules
            .iter()
            .find(|r| r.name == rule_name)
            .map(|r| r.summary.clone())
            .unwrap_or_default()
    }
}
