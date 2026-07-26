use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{HookArchive, MessageBackend, SFResult};

use super::engine::HookPublisher;

fn same_instance(a: &Option<Arc<dyn MessageBackend>>, b: &Option<Arc<dyn MessageBackend>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::as_ptr(a) as *const () == Arc::as_ptr(b) as *const (),
        _ => false,
    }
}

/// 3-tier hook publisher: Redis (real-time) + NATS JetStream (persistent) + PostgreSQL (archive).
/// Every event is published to all configured tiers.  Failures are isolated
/// per-tier so one slow backend does not block the others.
pub struct TieredHookPublisher {
    redis: Option<Arc<dyn MessageBackend>>,
    jetstream: Option<Arc<dyn MessageBackend>>,
    archive: Option<Arc<dyn HookArchive>>,
    client: Option<Arc<dyn cog_core::HttpClient>>,
    audit_stream: Option<Arc<dyn cog_core::AuditStream>>,
}

impl Default for TieredHookPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl TieredHookPublisher {
    pub fn new() -> Self {
        Self {
            redis: None,
            jetstream: None,
            archive: None,
            client: None,
            audit_stream: None,
        }
    }

    pub fn with_redis(mut self, redis: Arc<dyn MessageBackend>) -> Self {
        self.redis = Some(redis);
        self
    }

    pub fn with_jetstream(mut self, jetstream: Arc<dyn MessageBackend>) -> Self {
        self.jetstream = Some(jetstream);
        self
    }

    pub fn with_archive(mut self, archive: Arc<dyn HookArchive>) -> Self {
        self.archive = Some(archive);
        self
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// 接入不可篡改审计流（审计 3.5）：Hook 触发写入哈希链。
    pub fn with_audit_stream(mut self, stream: Arc<dyn cog_core::AuditStream>) -> Self {
        self.audit_stream = Some(stream);
        self
    }

    async fn audit_internal(&self, payload: &serde_json::Value) {
        if let Some(ref stream) = self.audit_stream {
            let trigger = payload
                .get("trigger")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let actor = payload
                .get("agent_id")
                .and_then(|v| v.as_str())
                .unwrap_or("hook-engine");
            let target = payload
                .get("task_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");
            if let Err(e) = stream
                .append(
                    cog_core::AuditKind::HookTrigger,
                    actor,
                    target,
                    &format!("hook.{trigger}"),
                    payload.clone(),
                )
                .await
            {
                tracing::warn!("TieredHookPublisher audit append failed: {}", e);
            }
        }
    }

    async fn publish_redis_internal(
        &self,
        channel: &str,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        let backend = self.redis.as_ref().ok_or_else(|| {
            cog_core::SFError::Agent("TieredHookPublisher: redis not configured".into())
        })?;
        let bytes = serde_json::to_vec(payload).map_err(cog_core::SFError::Serialization)?;
        backend.publish(channel, &bytes).await
    }

    async fn publish_jetstream_internal(
        &self,
        channel: &str,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        let backend = self.jetstream.as_ref().ok_or_else(|| {
            cog_core::SFError::Agent("TieredHookPublisher: jetstream not configured".into())
        })?;
        let bytes = serde_json::to_vec(payload).map_err(cog_core::SFError::Serialization)?;
        backend.publish(channel, &bytes).await
    }

    async fn archive_internal(&self, payload: &serde_json::Value) -> SFResult<()> {
        let archive = self.archive.as_ref().ok_or_else(|| {
            cog_core::SFError::Agent("TieredHookPublisher: archive not configured".into())
        })?;
        // Extract trigger_type from payload if present, else default
        let trigger_type = payload
            .get("trigger")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let agent_id = payload.get("agent_id").and_then(|v| v.as_str());
        let task_id = payload.get("task_id").and_then(|v| v.as_str());
        let crew_id = payload.get("crew_id").and_then(|v| v.as_str());
        let squad_id = payload.get("squad_id").and_then(|v| v.as_str());
        archive
            .archive(
                trigger_type,
                None,
                agent_id,
                task_id,
                crew_id,
                squad_id,
                payload,
            )
            .await
    }
}

#[async_trait]
impl HookPublisher for TieredHookPublisher {
    async fn publish_webhook(
        &self,
        url: &str,
        headers: &HashMap<String, String>,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        let client = self.client.as_ref().ok_or_else(|| {
            cog_core::SFError::Agent("TieredHookPublisher: no HttpClient configured".into())
        })?;
        let mut req = cog_core::HttpRequest::post(url)
            .json(payload)
            .map_err(|e| {
                cog_core::SFError::Agent(format!("hook webhook serialize failed: {}", e))
            })?;
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let response = client
            .execute(req)
            .await
            .map_err(|e| cog_core::SFError::IO(format!("hook webhook failed: {}", e)))?;
        if !response.is_success() {
            return Err(cog_core::SFError::IO(format!(
                "hook webhook returned {}",
                response.status
            )));
        }
        Ok(())
    }

    async fn publish_redis_stream(
        &self,
        channel: &str,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        // 3-tier: publish to Redis (tier-1), JetStream (tier-2), and PG (tier-3)
        let same_backend = same_instance(&self.redis, &self.jetstream);
        let redis_fut = self.publish_redis_internal(channel, payload);
        let jetstream_fut: std::pin::Pin<
            Box<dyn std::future::Future<Output = SFResult<()>> + Send>,
        > = if same_backend {
            Box::pin(async { Ok(()) })
        } else {
            Box::pin(self.publish_jetstream_internal(channel, payload))
        };
        let archive_fut = self.archive_internal(payload);
        let audit_fut = self.audit_internal(payload);

        let (r1, r2, r3, _) = tokio::join!(redis_fut, jetstream_fut, archive_fut, audit_fut);
        // Redis is the critical path; others are best-effort
        r1?;
        if let Err(e) = r2 {
            tracing::warn!("TieredHookPublisher jetstream fallback: {}", e);
        }
        if let Err(e) = r3 {
            tracing::warn!("TieredHookPublisher archive fallback: {}", e);
        }
        Ok(())
    }

    async fn notify_user(&self, user_id: &str, payload: &serde_json::Value) -> SFResult<()> {
        let channel = format!("sf:notify:{}", user_id);
        self.publish_redis_stream(&channel, payload).await
    }
}
