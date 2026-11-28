//! 签名钥匙的生命周期：签发、轮换、吊销，以及「此刻有哪几把」。
//!
//! 私钥不经过这一层：调用方（页面或命令）只交出公钥与指纹；产出私钥的那一步在
//! KMS/密钥库里，页面只看得到指纹。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::SFResult;

/// 一把钥匙此刻的状态。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyRow {
    pub key_id: String,
    pub purpose: String,
    pub public_key_b64: String,
    pub fingerprint: String,
    /// `issued` / `rotated` / `revoked`。
    pub state: String,
    pub overlap_until: Option<String>,
    pub revoked_reason: Option<String>,
}

/// 密钥生命周期的写面与读面。
#[async_trait]
pub trait SecurityKeys: Send + Sync {
    /// 此刻有哪几把钥匙，按 key_id 排。
    async fn list(&self) -> SFResult<Vec<KeyRow>>;

    async fn issue(
        &self,
        key_id: &str,
        purpose: &str,
        public_key_b64: &str,
        fingerprint: &str,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()>;

    async fn rotate(
        &self,
        key_id: &str,
        previous_key_id: &str,
        public_key_b64: &str,
        fingerprint: &str,
        overlap_until: Option<String>,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()>;

    async fn revoke(
        &self,
        key_id: &str,
        reason: &str,
        actor: &str,
        at: DateTime<Utc>,
    ) -> SFResult<()>;
}
