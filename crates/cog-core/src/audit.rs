//! 不可篡改审计流（审计 3.5）。
//!
//! 核心思想：追加式哈希链。每条 [`AuditEvent`] 携带前一条记录的哈希，
//! 任何对历史记录的篡改、删除或重排都会破坏链式校验（[`verify_chain`]）。
//!
//! - Agent 决策、Hook 触发、change 操作统一抽象为 [`AuditKind`]
//! - 存储后端实现 [`AuditStream`]；`cog-storage` 提供文件追加实现，
//!   其他后端（PG/远程 SIEM）可实现同一 trait

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 审计事件类别。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditKind {
    /// Agent 决策（plan/generate/evaluate/steer/abort 等）
    AgentDecision,
    /// Hook 触发
    HookTrigger,
    /// 自进化 change 操作（apply/test/commit/deploy/rollback/approve）
    #[serde(alias = "patch_operation")]
    ChangeOperation,
    /// 配额执法（拒绝/扣减）
    QuotaEnforcement,
    /// 认证/授权事件（登录、权限拒绝）
    Authz,
    /// 其他
    Custom(String),
}

/// 单条审计记录。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// 单调递增序号（由写入方分配）。
    pub seq: u64,
    pub timestamp: DateTime<Utc>,
    pub kind: AuditKind,
    /// 操作主体（agent id / user id / "system"）。
    pub actor: String,
    /// 关联对象（task id / change id / hook id）。
    pub target: String,
    /// 动作描述（如 "change.apply"、"hook.pre_prompt"）。
    pub action: String,
    /// 结构化详情（禁止包含密钥；写入前应经 `redact_secrets` 处理）。
    pub detail: serde_json::Value,
    /// 前一条记录的哈希；创世记录为 `"genesis"`。
    pub prev_hash: String,
    /// 本条记录哈希：blake3(seq‖timestamp‖kind‖actor‖target‖action‖detail‖prev_hash)。
    pub hash: String,
}

impl AuditEvent {
    /// 计算记录哈希（不含 `hash` 字段本身）。
    pub fn compute_hash(&self) -> String {
        let canonical = serde_json::json!({
            "seq": self.seq,
            "timestamp": self.timestamp,
            "kind": self.kind,
            "actor": self.actor,
            "target": self.target,
            "action": self.action,
            "detail": self.detail,
            "prev_hash": self.prev_hash,
        });
        blake3::hash(canonical.to_string().as_bytes())
            .to_hex()
            .to_string()
    }

    /// 构造下一条记录：自动填序号、时间戳、prev_hash 与 hash。
    pub fn next(
        prev: Option<&AuditEvent>,
        kind: AuditKind,
        actor: impl Into<String>,
        target: impl Into<String>,
        action: impl Into<String>,
        detail: serde_json::Value,
    ) -> Self {
        let (seq, prev_hash) = match prev {
            Some(p) => (p.seq + 1, p.hash.clone()),
            None => (1, "genesis".to_string()),
        };
        let mut event = Self {
            seq,
            timestamp: Utc::now(),
            kind,
            actor: actor.into(),
            target: target.into(),
            action: action.into(),
            detail,
            prev_hash,
            hash: String::new(),
        };
        event.hash = event.compute_hash();
        event
    }
}

/// 链校验结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainVerification {
    pub valid: bool,
    pub records_checked: usize,
    /// 第一个破坏链的序号（如有）。
    pub first_broken_seq: Option<u64>,
}

/// 校验整链：序号连续、prev_hash 衔接、hash 正确。
pub fn verify_chain(events: &[AuditEvent]) -> ChainVerification {
    let mut prev_hash = "genesis".to_string();
    for (idx, event) in events.iter().enumerate() {
        let expected_seq = idx as u64 + 1;
        if event.seq != expected_seq
            || event.prev_hash != prev_hash
            || event.hash != event.compute_hash()
        {
            return ChainVerification {
                valid: false,
                records_checked: (expected_seq - 1) as usize,
                first_broken_seq: Some(event.seq),
            };
        }
        prev_hash = event.hash.clone();
    }
    ChainVerification {
        valid: true,
        records_checked: events.len(),
        first_broken_seq: None,
    }
}

/// 审计流写入契约。
#[async_trait::async_trait]
pub trait AuditStream: Send + Sync {
    /// 追加一条记录。实现方负责序号分配与链式哈希（可用 [`AuditEvent::next`]）。
    async fn append(
        &self,
        kind: AuditKind,
        actor: &str,
        target: &str,
        action: &str,
        detail: serde_json::Value,
    ) -> crate::SFResult<AuditEvent>;

    /// 读取整链用于校验。
    async fn read_all(&self) -> crate::SFResult<Vec<AuditEvent>>;

    /// 校验链完整性。
    async fn verify(&self) -> crate::SFResult<ChainVerification> {
        Ok(verify_chain(&self.read_all().await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_chain(n: u64) -> Vec<AuditEvent> {
        let mut events = Vec::new();
        for i in 0..n {
            let prev = events.last();
            events.push(AuditEvent::next(
                prev,
                AuditKind::ChangeOperation,
                "system",
                format!("change-{i}"),
                "change.apply",
                serde_json::json!({"i": i}),
            ));
        }
        events
    }

    #[test]
    fn chain_verifies_clean() {
        let events = sample_chain(5);
        let result = verify_chain(&events);
        assert!(result.valid);
        assert_eq!(result.records_checked, 5);
    }

    #[test]
    fn tampered_detail_breaks_chain() {
        let mut events = sample_chain(5);
        events[2].detail = serde_json::json!({"forged": true});
        let result = verify_chain(&events);
        assert!(!result.valid);
        assert_eq!(result.first_broken_seq, Some(3));
    }

    #[test]
    fn deleted_record_breaks_chain() {
        let mut events = sample_chain(5);
        events.remove(2);
        let result = verify_chain(&events);
        assert!(!result.valid);
    }

    #[test]
    fn reordered_records_break_chain() {
        let mut events = sample_chain(5);
        events.swap(1, 2);
        let result = verify_chain(&events);
        assert!(!result.valid);
    }

    #[test]
    fn empty_chain_is_valid() {
        assert!(verify_chain(&[]).valid);
    }
}
