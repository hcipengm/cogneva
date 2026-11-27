//! 密钥生命周期的写者与读法：`key` 这一类事实的 `issued` / `rotated` / `revoked` 三步。
//!
//! 私钥永远不进事件，也不进这一层：事件里只有公钥、指纹与动作。当前状态是这三步
//! 折出来的投影——删掉任何表，读法仍从流上折得回来。

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use std::sync::Arc;

use cog_core::{EventPayload, EventStore, FactEvent, KeyRow, SFError, SFResult, SecurityKeys};

/// 一把钥匙的三步写入口与读面。
pub struct KeyEventStore {
    pool: PgPool,
    store: Arc<dyn EventStore>,
}

impl KeyEventStore {
    pub fn new(pool: PgPool, store: Arc<dyn EventStore>) -> Self {
        Self { pool, store }
    }
}

#[async_trait::async_trait]
impl SecurityKeys for KeyEventStore {
    async fn list(&self) -> SFResult<Vec<KeyRow>> {
        keys(&self.pool).await
    }

    async fn issue(
        &self,
        key_id: &str,
        purpose: &str,
        public_key_b64: &str,
        fingerprint: &str,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()> {
        KeyEventStore::issue(
            self,
            key_id,
            purpose,
            public_key_b64,
            fingerprint,
            actor,
            at,
        )
        .await
    }

    async fn rotate(
        &self,
        key_id: &str,
        previous_key_id: &str,
        public_key_b64: &str,
        fingerprint: &str,
        overlap_until: Option<String>,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()> {
        KeyEventStore::rotate(
            self,
            key_id,
            previous_key_id,
            public_key_b64,
            fingerprint,
            overlap_until,
            actor,
            at,
        )
        .await
    }

    async fn revoke(
        &self,
        key_id: &str,
        reason: &str,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()> {
        KeyEventStore::revoke(self, key_id, reason, actor, at).await
    }
}

impl KeyEventStore {
    pub async fn issue(
        &self,
        key_id: &str,
        purpose: &str,
        public_key_b64: &str,
        fingerprint: &str,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()> {
        let mut event = FactEvent::new(EventPayload::KeyIssued {
            key_id: key_id.to_string(),
            purpose: purpose.to_string(),
            public_key_b64: public_key_b64.to_string(),
            fingerprint: fingerprint.to_string(),
            actor: actor.to_string(),
        });
        event.at = Some(at);
        self.store.append(event).await
    }

    pub async fn rotate(
        &self,
        key_id: &str,
        previous_key_id: &str,
        public_key_b64: &str,
        fingerprint: &str,
        overlap_until: Option<String>,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()> {
        let mut event = FactEvent::new(EventPayload::KeyRotated {
            key_id: key_id.to_string(),
            previous_key_id: previous_key_id.to_string(),
            public_key_b64: public_key_b64.to_string(),
            fingerprint: fingerprint.to_string(),
            overlap_until,
            actor: actor.to_string(),
        });
        event.at = Some(at);
        self.store.append(event).await
    }

    pub async fn revoke(
        &self,
        key_id: &str,
        reason: &str,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()> {
        let mut event = FactEvent::new(EventPayload::KeyRevoked {
            key_id: key_id.to_string(),
            reason: reason.to_string(),
            actor: actor.to_string(),
        });
        event.at = Some(at);
        self.store.append(event).await
    }
}

/// 从流上折出每一把钥匙的当前状态，按 key_id 排。
pub async fn keys(pool: &PgPool) -> SFResult<Vec<KeyRow>> {
    let rows = sqlx::query(
        "SELECT subject_id, step, attributes::jsonb AS attributes FROM cogneva_event \
          WHERE kind = 'key' ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read keys: {e}")))?;
    let mut out: Vec<KeyRow> = Vec::new();
    for row in rows {
        let key_id: String = row.get("subject_id");
        let step: String = row.get("step");
        let attributes: serde_json::Value = row.get("attributes");
        let text = |key: &str| {
            attributes
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };
        match step.as_str() {
            "issued" | "rotated" => {
                let entry = KeyRow {
                    key_id: key_id.clone(),
                    purpose: text("purpose").unwrap_or_default(),
                    public_key_b64: text("public_key").unwrap_or_default(),
                    fingerprint: text("fingerprint").unwrap_or_default(),
                    state: step,
                    overlap_until: text("overlap_until"),
                    revoked_reason: None,
                };
                match out.iter_mut().find(|row| row.key_id == key_id) {
                    Some(existing) => *existing = entry,
                    None => out.push(entry),
                }
            }
            "revoked" => {
                if let Some(existing) = out.iter_mut().find(|row| row.key_id == key_id) {
                    existing.state = "revoked".into();
                    existing.revoked_reason = text("reason");
                } else {
                    out.push(KeyRow {
                        key_id: key_id.clone(),
                        purpose: String::new(),
                        public_key_b64: String::new(),
                        fingerprint: String::new(),
                        state: "revoked".into(),
                        overlap_until: None,
                        revoked_reason: text("reason"),
                    });
                }
            }
            _ => {}
        }
    }
    out.sort_by(|a, b| a.key_id.cmp(&b.key_id));
    Ok(out)
}
