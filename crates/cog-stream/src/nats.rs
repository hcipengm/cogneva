//! NATS JetStream-backed [`MessageBackend`] for production environments.
//! Replaces Redis Streams with durable, disk-backed message streams that
//! support horizontal scaling and native consumer groups.

use async_nats::jetstream;
use async_nats::jetstream::consumer::{pull, AckPolicy, DeliverPolicy};
use async_nats::jetstream::stream::RetentionPolicy;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use cog_core::{MessageBackend, MessageStream, NatsConfig, SFError, SFResult};

/// NATS JetStream-backed [`MessageBackend`].
/// Streams are auto-created from the subject name with `WorkQueue` retention.
/// Consumer groups are mapped to durable JetStream pull consumers.
/// Received messages are held in an internal map so that [`Self::ack`] can
/// reference them by sequence number.
pub struct NatsMessageBackend {
    jetstream: jetstream::Context,
    pending_acks: Arc<RwLock<HashMap<String, async_nats::jetstream::Message>>>,
}

impl NatsMessageBackend {
    /// Connect to NATS server(s) using the provided configuration.
    /// Supports single-node, clustered (multiple URLs), authenticated,
    /// and TLS-secured deployments.
    pub async fn new(config: &NatsConfig) -> SFResult<Self> {
        if config.urls.is_empty() {
            return Err(SFError::DagExecutor("NATS urls are empty".into()));
        }

        let urls = config.urls.join(",");

        let client = if config.auth.username.is_some() || config.tls.enabled {
            let mut opts = async_nats::ConnectOptions::new();

            if let (Some(u), Some(p)) = (&config.auth.username, &config.auth.password) {
                opts = opts.user_and_password(u.clone(), p.clone());
            } else if let Some(token) = &config.auth.token {
                opts = opts.token(token.clone());
            }

            if config.tls.enabled {
                opts = opts.require_tls(true);
                if config.tls.ca_cert_path.is_some() {
                    tracing::info!("NATS TLS CA cert path set but custom CA loading requires rustls-pemfile feature");
                }
                if config.tls.insecure_skip_verify {
                    tracing::warn!(
                        "NATS TLS insecure_skip_verify is enabled — do not use in production"
                    );
                }
            }

            async_nats::connect_with_options(&urls, opts)
                .await
                .map_err(|e| SFError::DagExecutor(format!("NATS connect failed: {e}")))?
        } else {
            async_nats::connect(&urls)
                .await
                .map_err(|e| SFError::DagExecutor(format!("NATS connect failed: {e}")))?
        };

        let jetstream = jetstream::new(client);
        Ok(Self {
            jetstream,
            pending_acks: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Ensure a JetStream stream exists for the given subject.
    async fn ensure_stream(&self, subject: &str) -> SFResult<jetstream::stream::Stream> {
        let stream_name = subject_to_stream_name(subject);
        self.jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.to_string()],
                retention: RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream stream create failed: {e}")))
    }
}

#[async_trait]
impl MessageBackend for NatsMessageBackend {
    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
        self.ensure_stream(subject).await?;
        let subject = subject.to_string();
        let payload = payload.to_vec();
        self.jetstream
            .publish(subject, payload.into())
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream publish failed: {e}")))?;
        Ok(())
    }

    async fn publish_batch(&self, subject: &str, payloads: &[Vec<u8>]) -> SFResult<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        self.ensure_stream(subject).await?;
        let subject = subject.to_string();
        let futures = payloads.iter().map(|payload| {
            let subject = subject.clone();
            let payload = payload.clone();
            let jetstream = self.jetstream.clone();
            async move {
                jetstream
                    .publish(subject, payload.into())
                    .await
                    .map_err(|e| SFError::DagExecutor(format!("JetStream publish failed: {e}")))?;
                Ok::<(), SFError>(())
            }
        });
        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    async fn subscribe(&self, subject: &str, group: &str) -> SFResult<MessageStream> {
        let stream = self.ensure_stream(subject).await?;
        let consumer: jetstream::consumer::Consumer<pull::Config> = stream
            .get_or_create_consumer(
                group,
                pull::Config {
                    durable_name: Some(group.to_string()),
                    deliver_policy: DeliverPolicy::New,
                    ack_policy: AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream consumer create failed: {e}")))?;

        let messages = consumer
            .messages()
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream messages failed: {e}")))?;

        let pending = Arc::clone(&self.pending_acks);
        let stream =
            futures::stream::try_unfold((messages, pending), |(mut msgs, pending)| async move {
                match msgs.next().await {
                    Some(Ok(msg)) => {
                        let seq = msg.info().map(|i| i.stream_sequence).unwrap_or(0);
                        let id = seq.to_string();
                        let payload = msg.payload.to_vec();
                        pending
                            .write()
                            .map_err(|_| SFError::Agent("ack lock poisoned".into()))?
                            .insert(id.clone(), msg);
                        Ok(Some(((id, payload), (msgs, pending))))
                    }
                    Some(Err(e)) => Err(SFError::DagExecutor(format!(
                        "JetStream message error: {e}"
                    ))),
                    None => Ok(None),
                }
            });

        Ok(Box::pin(stream))
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        start_id: &str,
    ) -> SFResult<MessageStream> {
        let stream = self.ensure_stream(subject).await?;

        let deliver_policy = if start_id == "0" || start_id.is_empty() {
            DeliverPolicy::All
        } else {
            let seq = start_id
                .parse::<u64>()
                .map_err(|_| SFError::Validation(format!("invalid NATS sequence: {start_id}")))?;
            DeliverPolicy::ByStartSequence {
                start_sequence: seq,
            }
        };

        let consumer: jetstream::consumer::Consumer<pull::Config> = stream
            .get_or_create_consumer(
                group,
                pull::Config {
                    durable_name: Some(group.to_string()),
                    deliver_policy,
                    ack_policy: AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream consumer create failed: {e}")))?;

        let messages = consumer
            .messages()
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream messages failed: {e}")))?;

        let pending = Arc::clone(&self.pending_acks);
        let stream =
            futures::stream::try_unfold((messages, pending), |(mut msgs, pending)| async move {
                match msgs.next().await {
                    Some(Ok(msg)) => {
                        let seq = msg.info().map(|i| i.stream_sequence).unwrap_or(0);
                        let id = seq.to_string();
                        let payload = msg.payload.to_vec();
                        pending
                            .write()
                            .map_err(|_| SFError::Agent("ack lock poisoned".into()))?
                            .insert(id.clone(), msg);
                        Ok(Some(((id, payload), (msgs, pending))))
                    }
                    Some(Err(e)) => Err(SFError::DagExecutor(format!(
                        "JetStream message error: {e}"
                    ))),
                    None => Ok(None),
                }
            });

        Ok(Box::pin(stream))
    }

    async fn create_consumer_group(&self, stream: &str, group: &str) -> SFResult<()> {
        let js_stream = self.ensure_stream(stream).await?;
        let _: jetstream::consumer::Consumer<pull::Config> = js_stream
            .get_or_create_consumer(
                group,
                pull::Config {
                    durable_name: Some(group.to_string()),
                    deliver_policy: DeliverPolicy::New,
                    ack_policy: AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream consumer create failed: {e}")))?;
        Ok(())
    }

    async fn ack(&self, _stream: &str, _group: &str, ids: &[String]) -> SFResult<()> {
        for id in ids {
            let msg = {
                let mut pending = self
                    .pending_acks
                    .write()
                    .map_err(|_| SFError::Agent("ack lock poisoned".into()))?;
                pending.remove(id)
            };
            if let Some(msg) = msg {
                msg.ack()
                    .await
                    .map_err(|e| SFError::DagExecutor(format!("JetStream ack failed: {e}")))?;
            }
        }
        Ok(())
    }

    async fn dlq(&self, stream: &str, msg_id: &str, reason: &str) -> SFResult<()> {
        let payload = serde_json::json!({
            "original_id": msg_id,
            "reason": reason,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });
        let bytes = serde_json::to_vec(&payload).map_err(SFError::Serialization)?;
        let dlq_subject = format!("{}:dlq", stream);
        self.publish(&dlq_subject, &bytes).await
    }

    async fn delay_publish(&self, subject: &str, payload: &[u8], delay_ms: u64) -> SFResult<()> {
        let subject = subject.to_string();
        let payload = payload.to_vec();
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            let _ = this.publish(&subject, &payload).await;
        });
        Ok(())
    }
}

impl Clone for NatsMessageBackend {
    fn clone(&self) -> Self {
        Self {
            jetstream: self.jetstream.clone(),
            pending_acks: Arc::clone(&self.pending_acks),
        }
    }
}

/// Convert a NATS subject into a valid JetStream stream name.
fn subject_to_stream_name(subject: &str) -> String {
    subject.replace(['.', '>'], "_")
}
