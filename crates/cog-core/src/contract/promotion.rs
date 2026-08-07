//! 晋级台账契约（docs/2026-08-06_真自治全进化无人值守方案.md）。
//!
//! 每次晋级（无论推送端还是各集群拉取端）全字段留档：
//! patch 级别、决策、结果、回滚原因。配额与熔断判定也基于台账：
//! - 配额：`count_since(24h 前)` 超 `quota_per_day` → 排队/转人工；
//! - 熔断：`recent_outcomes` 连续 rollback 或 failure 超阈值 → 转人工模式。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// 晋级记录生命周期。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionStatus {
    /// 已决策待执行（推送中 / 拉取端处理中）。
    Pending,
    /// 晋级完成（金丝雀全量通过 / 配置热更新生效）。
    Promoted,
    /// 晋级后回滚（金丝雀看护或健康检查判定回归）。
    RolledBack,
    /// 晋级执行失败（推送失败 / 构建失败 / apply 失败）。
    Failed,
    /// L2：机器全绿，等待人工审批。
    AwaitingApproval,
}

impl PromotionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            PromotionStatus::Pending => "pending",
            PromotionStatus::Promoted => "promoted",
            PromotionStatus::RolledBack => "rolled_back",
            PromotionStatus::Failed => "failed",
            PromotionStatus::AwaitingApproval => "awaiting_approval",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(PromotionStatus::Pending),
            "promoted" => Some(PromotionStatus::Promoted),
            "rolled_back" => Some(PromotionStatus::RolledBack),
            "failed" => Some(PromotionStatus::Failed),
            "awaiting_approval" => Some(PromotionStatus::AwaitingApproval),
            _ => None,
        }
    }
}

/// 一次晋级的完整档案。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionRecord {
    /// 记录 id（uuid）。
    pub id: String,
    /// 对应 patch 的 artifact_id。
    pub patch_id: String,
    /// 晋级级别：l0_config / l1_rollout / l2_approval。
    pub level: String,
    /// 决策理由（分级引擎输出 / 人工审批备注）。
    pub decision_reason: String,
    /// 记录来源：推送端写 "publisher"，拉取端写集群标识。
    pub cluster: String,
    pub status: PromotionStatus,
    /// 结果说明 / 回滚原因。
    pub outcome: String,
    /// 验证摘要（编译/测试/eval 结论）。
    pub eval_summary: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 晋级台账。配额、熔断、审计展示共用一份事实源。
#[async_trait]
pub trait PromotionLedger: Send + Sync {
    /// 追加一条晋级记录。
    async fn record(&self, rec: PromotionRecord) -> SFResult<()>;

    /// 更新记录状态与结果说明。
    async fn update_status(&self, id: &str, status: PromotionStatus, outcome: &str)
        -> SFResult<()>;

    /// 统计 since 之后状态为 Promoted 的自动晋级次数（配额判定）。
    async fn count_promoted_since(&self, since: DateTime<Utc>) -> SFResult<u64>;

    /// 取最近 limit 条记录（新在前），供熔断器判定连续失败/回滚。
    async fn recent(&self, limit: usize) -> SFResult<Vec<PromotionRecord>>;

    /// 审计展示用列表（新在前）。
    async fn list(&self, limit: usize) -> SFResult<Vec<PromotionRecord>> {
        self.recent(limit).await
    }
}
