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

/// The closed set of ways one dispatch to one channel can end, as recorded on
/// [`cog_core::metric_names::ALERT_NOTIFICATIONS_TOTAL`]'s `outcome` label.
pub const OUTCOME_DELIVERED: &str = "delivered";
/// The channel refused the batch; the events did not reach anyone.
pub const OUTCOME_FAILED: &str = "failed";
/// The channel is configured but has nowhere to send, so nothing went out.
/// Kept apart from `delivered` on purpose: an email with no recipients looks
/// like a success at the call site and is a silent loss to the reader.
pub const OUTCOME_SKIPPED: &str = "skipped";
/// No outlet was configured at all, so the batch reached no channel.
pub const OUTCOME_NO_CHANNEL: &str = "no_channel";

/// What one channel did with one batch.
enum Delivery {
    Sent,
    /// Configured, but there was nothing to send to.
    Skipped,
}

/// The `channel` label's value for one outlet: the kind, never the address.
///
/// A webhook URL carries its own credential (Slack's does), and an email
/// channel carries recipients, so neither may reach a label or a log field.
fn channel_kind(channel: &AlertChannel) -> &'static str {
    match channel {
        AlertChannel::Webhook { .. } => "webhook",
        AlertChannel::Email { .. } => "email",
        AlertChannel::Slack { .. } => "slack",
    }
}

pub struct AlertManager {
    rules: Vec<AlertRule>,
    channels: Vec<AlertChannel>,
    timeout_secs: u64,
    client: Option<std::sync::Arc<dyn cog_core::HttpClient>>,
    metrics: Option<std::sync::Arc<dyn cog_core::MetricsBackend>>,
}

impl AlertManager {
    pub fn new(rules: Vec<AlertRule>, channels: Vec<AlertChannel>) -> Self {
        Self {
            rules,
            channels,
            timeout_secs: 10,
            client: None,
            metrics: None,
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

    /// Where the delivery reading goes.
    ///
    /// Without it the manager still sends; it just leaves no trace, which is
    /// the state this reading exists to end, so every construction site that
    /// has a backend passes one.
    pub fn with_metrics(mut self, metrics: std::sync::Arc<dyn cog_core::MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Dispatch notifications for a batch of alert events.
    /// Sends via every configured channel in parallel (fire-and-forget).
    pub async fn notify(&self, events: &[AlertEvent]) {
        if events.is_empty() {
            return;
        }
        for channel in &self.channels {
            let kind = channel_kind(channel);
            match self.send_to_channel(channel, events).await {
                Ok(Delivery::Sent) => {
                    record_dispatch(self.metrics.as_ref(), kind, OUTCOME_DELIVERED, events.len())
                        .await
                }
                Ok(Delivery::Skipped) => {
                    record_dispatch(self.metrics.as_ref(), kind, OUTCOME_SKIPPED, events.len())
                        .await
                }
                Err(e) => {
                    // The kind, not the channel: its Debug form carries the
                    // webhook URL and the SMTP password.
                    tracing::warn!(channel = kind, error = %e, "Alert notification failed");
                    record_dispatch(self.metrics.as_ref(), kind, OUTCOME_FAILED, events.len()).await
                }
            }
        }
    }

    async fn send_to_channel(
        &self,
        channel: &AlertChannel,
        events: &[AlertEvent],
    ) -> Result<Delivery, anyhow::Error> {
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
                Ok(Delivery::Sent)
            }
            AlertChannel::Email {
                smtp_config,
                to,
                subject_template,
            } => {
                if to.is_empty() {
                    // Configured with nobody to tell. Returning success here
                    // was the reason a channel that could never deliver looked
                    // exactly like one that delivered.
                    return Ok(Delivery::Skipped);
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

                Ok(Delivery::Sent)
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
                Ok(Delivery::Sent)
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

/// Record one dispatch on the alert delivery counter.
///
/// A dispatch that is not recorded is the state this counter exists to end, so
/// the only silence here is a manager built without a backend -- which is a
/// construction site's omission, not a delivery outcome.
async fn record_dispatch(
    metrics: Option<&std::sync::Arc<dyn cog_core::MetricsBackend>>,
    channel: &'static str,
    outcome: &'static str,
    events: usize,
) {
    let Some(backend) = metrics else { return };
    let labels = std::collections::HashMap::from([
        ("channel".to_string(), channel.to_string()),
        ("outcome".to_string(), outcome.to_string()),
    ]);
    if let Err(e) = backend
        .record_counter(
            cog_core::metric_names::ALERT_NOTIFICATIONS_TOTAL,
            events as f64,
            labels,
        )
        .await
    {
        tracing::warn!(channel, outcome, error = %e, "could not record the alert delivery outcome");
    }
}

/// Record a batch that reached no outlet at all.
///
/// The bridge and the infra watcher hold `Option<AlertManager>`: when it is
/// `None` -- no channels configured, or an outlet without an HTTP client --
/// there is no `notify` to call, so the absence of a delivery has to be written
/// from here or it does not exist on any series while the alert row is still
/// there. **Only when the row was not written either**: `alerts` is one of the
/// places a person can read, so a batch that reached it was delivered (see
/// [`record_delivered_in_app`]).
pub async fn record_undelivered(
    metrics: Option<&std::sync::Arc<dyn cog_core::MetricsBackend>>,
    events: usize,
) {
    record_dispatch(metrics, "none", OUTCOME_NO_CHANNEL, events).await;
}

/// Record a batch that reached the in-app alert surface: the row is in the
/// `alerts` table and the WebUI reads it from `/api/v1/alerts/active`.
///
/// This is an outlet with no address and therefore no configuration to get
/// wrong, so the "no external channel" state is not the same as "nobody can be
/// told". Counting the two together made a deployment that is *quiet and wired
/// to the product itself* read exactly like one whose alerts vanish.
pub async fn record_delivered_in_app(
    metrics: Option<&std::sync::Arc<dyn cog_core::MetricsBackend>>,
    events: usize,
) {
    record_dispatch(metrics, "in_app", OUTCOME_DELIVERED, events).await;
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

    /// A backend that holds the counter writes it was handed, so the test can
    /// read the same cells a scrape would.
    /// One recorded counter write: the series, the value, and its labels.
    type Recorded = (String, f64, std::collections::HashMap<String, String>);

    #[derive(Default)]
    struct Captured {
        calls: std::sync::Mutex<Vec<Recorded>>,
    }

    impl Captured {
        /// `(channel, outcome, value)` per recorded write.
        fn dispatches(&self) -> Vec<(String, String, f64)> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(name, value, labels)| {
                    assert_eq!(name, "cogneva_alert_notifications_total");
                    (
                        labels.get("channel").cloned().unwrap_or_default(),
                        labels.get("outcome").cloned().unwrap_or_default(),
                        *value,
                    )
                })
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for Captured {
        async fn record_gauge(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn record_counter(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.calls
                .lock()
                .unwrap()
                .push((name.as_str().to_string(), value, labels));
            Ok(())
        }

        async fn record_histogram(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_gauge_latest(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_counter_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }

        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> cog_core::SFResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn health_check(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// An HTTP client that always answers 200, so a channel can reach its
    /// success path without a network.
    #[derive(Debug)]
    struct Accepted;

    #[async_trait::async_trait]
    impl cog_core::HttpClient for Accepted {
        async fn execute(
            &self,
            _req: cog_core::HttpRequest,
        ) -> cog_core::SFResult<cog_core::HttpResponse> {
            Ok(cog_core::HttpResponse {
                status: 200,
                headers: std::collections::HashMap::new(),
                body: Vec::new(),
            })
        }
    }

    /// A delivery that worked and one that did not may not share a cell: the
    /// whole point of the counter is that the manager's silent success stops
    /// being indistinguishable from its warning.
    #[tokio::test]
    async fn a_delivery_and_a_failure_are_counted_apart() {
        let captured = std::sync::Arc::new(Captured::default());
        let channel = AlertChannel::Slack {
            webhook_url: "https://hooks.example.invalid/services/T000/B000/XXXX".into(),
            channel: "#alerts".into(),
        };

        // No client: the Slack arm refuses, and that is the failed cell.
        let failing =
            AlertManager::new(Vec::new(), vec![channel.clone()]).with_metrics(captured.clone());
        failing.notify(&[event(&[("queue", "build")])]).await;
        assert_eq!(
            captured.dispatches(),
            vec![("slack".to_string(), OUTCOME_FAILED.to_string(), 1.0)]
        );

        // With one: the same call lands in the delivered cell.
        let delivering = AlertManager::new(Vec::new(), vec![channel])
            .with_client(std::sync::Arc::new(Accepted))
            .with_metrics(captured.clone());
        delivering
            .notify(&[event(&[("queue", "build")]), event(&[("queue", "eval")])])
            .await;
        assert_eq!(
            captured.dispatches()[1],
            ("slack".to_string(), OUTCOME_DELIVERED.to_string(), 2.0),
            "the cell value is how many events the dispatch carried"
        );
    }

    /// A channel with nobody to send to is not a delivery. Returning `Ok(())`
    /// here is exactly how an alert that reached no person used to look like
    /// one that did.
    #[tokio::test]
    async fn an_email_with_no_recipients_is_skipped_not_delivered() {
        let captured = std::sync::Arc::new(Captured::default());
        let manager = AlertManager::new(
            Vec::new(),
            vec![AlertChannel::Email {
                smtp_config: cog_core::alerts::SmtpConfig {
                    host: "smtp.example.invalid".into(),
                    port: 25,
                    username: None,
                    password: None,
                    from_address: "alerts@example.invalid".into(),
                    use_tls: false,
                },
                to: Vec::new(),
                subject_template: String::new(),
            }],
        )
        .with_metrics(captured.clone());

        manager.notify(&[event(&[("queue", "build")])]).await;
        assert_eq!(
            captured.dispatches(),
            vec![("email".to_string(), OUTCOME_SKIPPED.to_string(), 1.0)]
        );
    }

    /// The batch that had no outlet at all still has to land somewhere: the
    /// persisted row it left behind is otherwise the only trace, and that row
    /// reads as though somebody was told.
    #[tokio::test]
    async fn a_batch_with_no_outlet_is_counted_as_undelivered() {
        let captured = std::sync::Arc::new(Captured::default());
        let backend: std::sync::Arc<dyn cog_core::MetricsBackend> = captured.clone();
        record_undelivered(Some(&backend), 3).await;
        assert_eq!(
            captured.dispatches(),
            vec![("none".to_string(), OUTCOME_NO_CHANNEL.to_string(), 3.0)]
        );
    }
}
