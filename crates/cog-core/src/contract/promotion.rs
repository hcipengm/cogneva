//! 晋级台账契约。
//!
//! 每次晋级（无论推送端还是各集群拉取端）全字段留档：
//! change 级别、决策、结果、回滚原因。配额与熔断判定也基于台账：
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

/// 晋级门把变更分到哪条通道、按哪条规则分的。
///
/// 分级的全部输入是文件路径与 diff 行数，因此这条记录同时也是"阈值花了多少
/// 代价"的原始事实：`ApprovalDiffOverLimit` 多起来，说明 diff 行数上限在把
/// 变更往人工通道推，而不是这些变更本身有风险。只有把判定按类型留档，才知道
/// 该不该动那个上限；把判定压成一句 Debug 文本就再也量不出来了。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionGateKind {
    /// 未触及任何文件，影响面不可确认。
    RejectUnmeasured,
    /// 触及依赖清单 / 密钥材料，拒收且不进沙盒。
    RejectProtected,
    /// 全部落在配置路径：L0 热更新。
    AutoConfig,
    /// 全部落在白名单且 diff 未超限：L1 自动金丝雀。
    AutoRollout,
    /// diff 行数超上限，转人工。
    ApprovalDiffOverLimit,
    /// 触及核心路径，转人工。
    ApprovalCorePath,
    /// 路径未列入白名单，模糊从严转人工。
    ApprovalUnclassified,
}

impl PromotionGateKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            PromotionGateKind::RejectUnmeasured => "reject_unmeasured",
            PromotionGateKind::RejectProtected => "reject_protected",
            PromotionGateKind::AutoConfig => "auto_config",
            PromotionGateKind::AutoRollout => "auto_rollout",
            PromotionGateKind::ApprovalDiffOverLimit => "approval_diff_over_limit",
            PromotionGateKind::ApprovalCorePath => "approval_core_path",
            PromotionGateKind::ApprovalUnclassified => "approval_unclassified",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "reject_unmeasured" => Some(PromotionGateKind::RejectUnmeasured),
            "reject_protected" => Some(PromotionGateKind::RejectProtected),
            "auto_config" => Some(PromotionGateKind::AutoConfig),
            "auto_rollout" => Some(PromotionGateKind::AutoRollout),
            "approval_diff_over_limit" => Some(PromotionGateKind::ApprovalDiffOverLimit),
            "approval_core_path" => Some(PromotionGateKind::ApprovalCorePath),
            "approval_unclassified" => Some(PromotionGateKind::ApprovalUnclassified),
            _ => None,
        }
    }

    /// 这条判定是否由晋级门（而非运行时降级条件）把变更推向了人工审批。
    pub fn is_gate_diverted(&self) -> bool {
        matches!(
            self,
            PromotionGateKind::ApprovalDiffOverLimit
                | PromotionGateKind::ApprovalCorePath
                | PromotionGateKind::ApprovalUnclassified
        )
    }
}

/// 一次晋级的完整档案。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionRecord {
    /// 记录 id（uuid）。
    pub id: String,
    /// 对应 change 的 artifact_id。
    pub change_id: String,
    /// 晋级级别：l0_config / l1_rollout / l2_approval。
    pub level: String,
    /// 分级判定的类型化结果。`None` = 该记录不来自分级（旧记录或非分级路径）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_kind: Option<PromotionGateKind>,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 每个变体都要能原样穿过 `as_str` → 文本 → `parse` 这条窄通道，
    /// 否则落库的判定再也读不回来。
    #[test]
    fn gate_kind_round_trips_through_its_token() {
        for kind in [
            PromotionGateKind::RejectUnmeasured,
            PromotionGateKind::RejectProtected,
            PromotionGateKind::AutoConfig,
            PromotionGateKind::AutoRollout,
            PromotionGateKind::ApprovalDiffOverLimit,
            PromotionGateKind::ApprovalCorePath,
            PromotionGateKind::ApprovalUnclassified,
        ] {
            assert_eq!(PromotionGateKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(PromotionGateKind::parse("not_a_kind"), None);
    }

    /// 只有三条"转人工"判定算门槛代价，自动通道与拒收都不是。
    #[test]
    fn only_approval_kinds_count_as_gate_diverted() {
        for kind in [
            PromotionGateKind::RejectUnmeasured,
            PromotionGateKind::RejectProtected,
            PromotionGateKind::AutoConfig,
            PromotionGateKind::AutoRollout,
        ] {
            assert!(!kind.is_gate_diverted(), "{kind:?} must not count");
        }
        for kind in [
            PromotionGateKind::ApprovalDiffOverLimit,
            PromotionGateKind::ApprovalCorePath,
            PromotionGateKind::ApprovalUnclassified,
        ] {
            assert!(kind.is_gate_diverted(), "{kind:?} must count");
        }
    }
}
