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
/// Streams are auto-created from the subject name with the retention policy
/// from [`NatsConfig::stream_retention`] (default `WorkQueue`).
/// Consumer groups are mapped to durable JetStream pull consumers.
/// Received messages are held in an internal map so that [`Self::ack`] can
/// reference them by sequence number.
pub struct NatsMessageBackend {
    jetstream: jetstream::Context,
    /// 连接本身，只用来读服务器声明的消息大小上限。JetStream 的发布路径
    /// 绕过了核心客户端那处按 `max_payload` 的前置检查，所以这里要自己拿
    /// 这个数：不拿的话，超大载荷会一路发到服务器，回来的是一个被抹平成
    /// 一句话的错，调用方分不出「这条太大」和「总线挂了」。
    client: async_nats::Client,
    pending_acks: Arc<RwLock<HashMap<String, async_nats::jetstream::Message>>>,
    consumer_tuning: ConsumerTuning,
    stream_retention: RetentionPolicy,
}

/// Parse [`NatsConfig::stream_retention`] into a JetStream retention policy.
fn parse_retention(raw: &str) -> SFResult<RetentionPolicy> {
    match raw {
        "workqueue" => Ok(RetentionPolicy::WorkQueue),
        "limits" => Ok(RetentionPolicy::Limits),
        "interest" => Ok(RetentionPolicy::Interest),
        other => Err(SFError::Validation(format!(
            "invalid NATS stream_retention: {other} (want workqueue|limits|interest)"
        ))),
    }
}

/// JetStream pull-consumer tuning carried from [`NatsConfig`].
#[derive(Clone, Copy)]
struct ConsumerTuning {
    ack_wait: std::time::Duration,
    max_deliver: i64,
    max_ack_pending: usize,
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

        let info_client = client.clone();
        let jetstream = jetstream::new(client);
        Ok(Self {
            jetstream,
            client: info_client,
            pending_acks: Arc::new(RwLock::new(HashMap::new())),
            consumer_tuning: ConsumerTuning {
                ack_wait: std::time::Duration::from_secs(config.consumer_ack_wait_secs),
                max_deliver: config.consumer_max_deliver,
                max_ack_pending: config.consumer_max_ack_pending,
            },
            stream_retention: parse_retention(&config.stream_retention)?,
        })
    }

    /// Build the pull consumer config with the deployment's tuning applied.
    /// `ack_wait` must exceed the worst-case per-message processing latency
    /// (memory ingest: archive + two LLM calls + retries, minutes), otherwise
    /// in-flight messages get redelivered and processed twice.
    fn consumer_config(&self, group: &str, deliver_policy: DeliverPolicy) -> pull::Config {
        pull::Config {
            durable_name: Some(group.to_string()),
            deliver_policy,
            ack_policy: AckPolicy::Explicit,
            ack_wait: self.consumer_tuning.ack_wait,
            max_deliver: self.consumer_tuning.max_deliver,
            max_ack_pending: self.consumer_tuning.max_ack_pending as i64,
            ..Default::default()
        }
    }

    /// 这条传输对**一条消息**能收多少字节：服务器声明的 `max_payload` 与流上
    /// 配的 `max_msg_size`（线上的键名，客户端结构里叫 `max_message_size`）取小，
    /// 后者非正数＝不额外设界，只有服务器那个数管。
    ///
    /// 两个数都来自服务器自己的声明，不看发布失败回来的文本：文本随上游改写，
    /// 拿它分类等于把判定权交给不受控的字符串。两个上限都存在是因为它们管
    /// 不同的东西——服务器那个是协议层的帧，流那个是这条流愿意存多大一条。
    fn effective_payload_limit(server_max_payload: usize, stream_max_message_size: i32) -> usize {
        if stream_max_message_size > 0 {
            server_max_payload.min(stream_max_message_size as usize)
        } else {
            server_max_payload
        }
    }

    /// 把 JetStream 的发布失败翻成我们的类型。
    ///
    /// 只有一条值得单独翻出来：服务器对「这条消息超过流允许的最大尺寸」有一条
    /// **带错误码**的答复（10054），库把它并进了泛化的 `Other`。照原样压成一句
    /// `JetStream publish failed: …`，调用方就再也分不出「这条太大」与「总线挂了」
    /// ——前者重试一万次也不会变小，后者重试会好，而它们拿到的是同一个错，于是
    /// 只能一起无限重试，队头就这么被一个不会变的东西占住。
    ///
    /// 判据取错误码，不取文本。其余原因保持原样：它们仍是「等一等可能就好」的
    /// 瞬时失败，重试的职责不变。
    ///
    /// 这一路拿得到被拒的字节数（就是调用方要发的那串），但拿不到服务器按哪条
    /// 界拒的：本地前置检查放它过去，正因为本地算出来的界比服务器实际那条松。
    /// 所以界报 `None`，而不是把本地那个数填上去充当服务器的界。
    fn classify_publish_error(
        e: async_nats::jetstream::context::PublishError,
        size: usize,
    ) -> SFError {
        if e.kind() == async_nats::jetstream::context::PublishErrorKind::Other {
            if let Some(source) = std::error::Error::source(&e) {
                if let Some(server) = source.downcast_ref::<jetstream::Error>() {
                    if server.error_code() == jetstream::ErrorCode::STREAM_MESSAGE_EXCEEDS_MAXIMUM {
                        return SFError::PayloadTooLarge { size, limit: None };
                    }
                }
            }
        }
        SFError::DagExecutor(format!("JetStream publish failed: {e}"))
    }

    /// Ensure a JetStream stream exists for the given subject.
    /// An existing stream whose retention differs from the configured policy
    /// is a hard error, not a silent reuse: NATS forbids changing retention
    /// to/from workqueue on a live stream, and running on the wrong policy
    /// (e.g. workqueue under a multi-consumer event plane) silently starves
    /// consumer groups. Recreate the stream deliberately instead.
    ///
    /// 确保 subject 对应的流存在，并返回它，外加这条流能接受的一条消息的最大
    /// 字节数。
    ///
    /// 上限随流一起返回而不是另开一次查询：`info()` 本来就要取一次（判保留
    /// 策略），两个数出自同一次读取，就不会出现「按上一拍的界判了这一拍的消息」。
    async fn ensure_stream(&self, subject: &str) -> SFResult<(jetstream::stream::Stream, usize)> {
        let stream_name = subject_to_stream_name(subject);
        let mut stream = self
            .jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.to_string()],
                retention: self.stream_retention,
                ..Default::default()
            })
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream stream create failed: {e}")))?;
        let info = stream
            .info()
            .await
            .map_err(|e| SFError::DagExecutor(format!("JetStream stream info failed: {e}")))?;
        let actual = info.config.retention;
        if actual != self.stream_retention {
            return Err(SFError::DagExecutor(format!(
                "JetStream stream {stream_name} retention mismatch: running {actual:?}, configured {:?}; delete and recreate the stream to change policy",
                self.stream_retention
            )));
        }
        let limit = Self::effective_payload_limit(
            self.client.server_info().max_payload,
            info.config.max_message_size,
        );
        Ok((stream, limit))
    }
}

#[async_trait]
impl MessageBackend for NatsMessageBackend {
    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
        let (_stream, limit) = self.ensure_stream(subject).await?;
        if payload.len() > limit {
            // 这条消息再发多少次也还是这么大：带上类型返回，调用方据此把它
            // 当次消费掉，而不是拿它反复撞同一面墙、把后面的消息一起堵住。
            return Err(SFError::PayloadTooLarge {
                size: payload.len(),
                limit: Some(limit),
            });
        }
        let subject = subject.to_string();
        let payload = payload.to_vec();
        let size = payload.len();
        self.jetstream
            .publish(subject, payload.into())
            .await
            .map_err(|e| Self::classify_publish_error(e, size))?;
        Ok(())
    }

    async fn publish_batch(&self, subject: &str, payloads: &[Vec<u8>]) -> SFResult<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        let (_stream, limit) = self.ensure_stream(subject).await?;
        if let Some(oversized) = payloads.iter().find(|p| p.len() > limit) {
            return Err(SFError::PayloadTooLarge {
                size: oversized.len(),
                limit: Some(limit),
            });
        }
        let subject = subject.to_string();
        let futures = payloads.iter().map(|payload| {
            let subject = subject.clone();
            let payload = payload.clone();
            let jetstream = self.jetstream.clone();
            async move {
                let size = payload.len();
                jetstream
                    .publish(subject, payload.into())
                    .await
                    .map_err(|e| NatsMessageBackend::classify_publish_error(e, size))?;
                Ok::<(), SFError>(())
            }
        });
        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    async fn subscribe(&self, subject: &str, group: &str) -> SFResult<MessageStream> {
        let (stream, _limit) = self.ensure_stream(subject).await?;
        let consumer: jetstream::consumer::Consumer<pull::Config> = stream
            .get_or_create_consumer(group, self.consumer_config(group, DeliverPolicy::New))
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
        let (stream, _limit) = self.ensure_stream(subject).await?;

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
            .get_or_create_consumer(group, self.consumer_config(group, deliver_policy))
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
        let (js_stream, _limit) = self.ensure_stream(stream).await?;
        let _: jetstream::consumer::Consumer<pull::Config> = js_stream
            .get_or_create_consumer(group, self.consumer_config(group, DeliverPolicy::New))
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

    /// Nothing to report. The messages this process has delivered and not
    /// acked live in a local map, so it can only ever describe its own
    /// in-flight work; a message abandoned by a consumer that died is exactly
    /// what it cannot see. JetStream tracks ack-pending broker-side, but this
    /// backend has no reclaim path, so a count of what is piling up would say
    /// nothing about whether anything can come back. Reporting the local map
    /// instead of saying "unobservable" would answer a healthy-looking number
    /// to the one question that matters.
    async fn pending_stats(
        &self,
        _stream: &str,
        _group: &str,
        _idle_threshold_ms: u64,
    ) -> SFResult<Option<cog_core::PendingStats>> {
        Ok(None)
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
            client: self.client.clone(),
            pending_acks: Arc::clone(&self.pending_acks),
            consumer_tuning: self.consumer_tuning,
            stream_retention: self.stream_retention,
        }
    }
}

/// Convert a NATS subject into a valid JetStream stream name.
fn subject_to_stream_name(subject: &str) -> String {
    subject.replace(['.', '>'], "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 两个上限取小，取到的是「这条消息真的能进去」的那个数：服务器帧是硬
    /// 天花板，流自己的 max_msg_size 只会在它之下再收紧。
    #[test]
    fn the_effective_limit_is_the_tighter_of_the_two_declared_ones() {
        // 流没另设界（NATS 用非正数表示）⇒ 服务器帧就是界。
        assert_eq!(
            NatsMessageBackend::effective_payload_limit(1_048_576, -1),
            1_048_576
        );
        assert_eq!(
            NatsMessageBackend::effective_payload_limit(1_048_576, 0),
            1_048_576
        );
        // 流收得更紧 ⇒ 用流那个，否则会在服务器已经接受之前先判定通过。
        assert_eq!(
            NatsMessageBackend::effective_payload_limit(1_048_576, 32_768),
            32_768
        );
        // 服务器收得更紧 ⇒ 用服务器那个，流放宽不能越过帧。
        assert_eq!(
            NatsMessageBackend::effective_payload_limit(4_096, 65_536),
            4_096
        );
    }
}
