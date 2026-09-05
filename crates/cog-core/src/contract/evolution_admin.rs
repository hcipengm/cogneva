//! Admin-facing contract for the self-evolution pipeline.
//!
//! This trait lives in `cog-core` so the Gateway can expose evolution controls
//! without taking a direct dependency on `cog-reflection`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Summarized view of a single evolution artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionChangeInfo {
    pub id: String,
    pub kind: String,
    pub description: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    /// One-line diff summary (e.g. "3 files, +42 -17"; policy updates show
    /// the version transition). Derived from the artifact content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_summary: Option<String>,
    /// Statistical evaluation verdict (two-proportion z-test) when the
    /// artifact passed through the eval gate, e.g. "Adopt z=2.31 uplift +18%".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eval_summary: Option<String>,
}

/// Request to evaluate an artifact-level policy candidate against a baseline
/// (产物级进化 §14.3). The verdict is gated by a two-proportion z-test;
/// an `Adopt` verdict does **not** activate the policy — it stages the
/// candidate at `AwaitingReview` until a human approves it, mirroring the
/// source-level `manual_approve` gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEvalRequest {
    /// Policy name (sanitized to `[A-Za-z0-9._-]`).
    pub name: String,
    /// Baseline outcomes under the current policy.
    pub baseline_outcomes: Vec<bool>,
    /// Candidate policy payload.
    pub candidate_payload: serde_json::Value,
    /// Candidate outcomes observed under the candidate payload.
    pub candidate_outcomes: Vec<bool>,
    /// Human-readable reason for the proposal (recorded in the version chain).
    pub reason: String,
}

/// Response from an explicit apply request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionApplyResponse {
    pub change_id: String,
    pub test_passed: bool,
    pub test_output: String,
    pub new_status: String,
    pub files_changed: Vec<String>,
}

/// Response from an explicit deploy request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionDeployResponse {
    pub change_id: String,
    pub commit_hash: String,
    pub staged_binary_path: String,
    pub switched: bool,
}

/// Snapshot of the self-evolution counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionMetricsSnapshot {
    pub events_total: u64,
    pub events_failed: u64,
    pub changes_applied: u64,
    pub changes_failed: u64,
}

/// A single entry in the evolution event stream (artifact lifecycle record).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionEventInfo {
    pub id: String,
    pub kind: String,
    pub description: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

/// Response from an explicit rollback request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionRollbackResponse {
    pub rolled_back: bool,
    pub message: String,
}

/// 自动晋级运行时开关快照。`effective_enabled = config_enabled && !paused`：
/// 配置文件给持久默认值，admin API 的运行时暂停立即生效、重启后回落
/// 到配置值（持久停用改 cogneva.json / env）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionSwitchInfo {
    /// 配置文件里的总开关（promotion.enabled）。
    pub config_enabled: bool,
    /// 运行时暂停标志（admin API 设置）。
    pub paused: bool,
    /// 实际生效状态：两者相与。
    pub effective_enabled: bool,
    /// 最近一次运行时切换时间；从未切换过为 None。
    pub updated_at: Option<DateTime<Utc>>,
    /// 切换备注（谁在干什么，进审计）。
    pub note: String,
}

/// 晋级周报快照（eval 长期趋势）：按 ISO 周聚合晋级台账。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionTrendReport {
    /// 报告生成时间。
    pub generated_at: DateTime<Utc>,
    /// 逐周桶（旧在前）。
    pub weeks: Vec<PromotionTrendWeek>,
    /// 趋势向下告警：连续多周成功率下降且样本足够时非空。
    pub alert: Option<String>,
}

/// 单周晋级聚合。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionTrendWeek {
    /// ISO 周标签，如 2026-W32。
    pub week: String,
    pub promoted: u64,
    pub rolled_back: u64,
    pub failed: u64,
    pub awaiting_review: u64,
    /// 成功率 = promoted / (promoted + rolled_back + failed)；无完结对
    /// 决样本（全在审批中）时为 None。
    pub success_rate: Option<f64>,
}

/// Admin operations for the self-evolution subsystem.
#[async_trait::async_trait]
pub trait EvolutionAdmin: Send + Sync {
    /// List all known evolution artifacts (newest first).
    async fn list_changes(&self) -> crate::SFResult<Vec<EvolutionChangeInfo>>;

    /// Apply a single change to the working tree and run the test suite.
    async fn apply_change(&self, change_id: &str) -> crate::SFResult<EvolutionApplyResponse>;

    /// Commit and build a change, then optionally stage and switch to the new binary.
    async fn deploy_change(&self, change_id: &str) -> crate::SFResult<EvolutionDeployResponse>;

    /// Human-in-the-loop gate release (Phase 3.1/4.3 `manual_approve`):
    /// approve a change that passed tests and is held at `AwaitingReview`,
    /// then commit/build/deploy it. Rejects changes not awaiting review.
    /// Default: not supported by this implementation.
    async fn approve_change(&self, _change_id: &str) -> crate::SFResult<EvolutionDeployResponse> {
        Err(crate::SFError::NotImplemented("evolution approve".into()))
    }

    /// Roll back to the previously deployed binary and restart.
    /// Default: not supported by this implementation.
    async fn rollback(&self) -> crate::SFResult<EvolutionRollbackResponse> {
        Err(crate::SFError::NotImplemented("evolution rollback".into()))
    }

    /// List recent evolution events (newest first), up to `limit` entries.
    /// Default: not supported by this implementation.
    async fn list_events(&self, _limit: usize) -> crate::SFResult<Vec<EvolutionEventInfo>> {
        Err(crate::SFError::NotImplemented("evolution events".into()))
    }

    /// Evaluate an artifact-level policy candidate (产物级进化).
    /// An `Adopt` verdict stages the candidate at `AwaitingReview`;
    /// `approve_change` on the returned artifact id hot-swaps the policy.
    /// Default: not supported by this implementation.
    async fn evaluate_policy(
        &self,
        _req: PolicyEvalRequest,
    ) -> crate::SFResult<EvolutionChangeInfo> {
        Err(crate::SFError::NotImplemented("policy evaluate".into()))
    }

    /// 读取自动晋级运行时开关快照（一键暂停）。
    /// Default: not supported by this implementation.
    async fn promotion_switch(&self) -> crate::SFResult<PromotionSwitchInfo> {
        Err(crate::SFError::NotImplemented("promotion switch".into()))
    }

    /// 设置运行时暂停标志：true = 立即停摆自动晋级（排队中的全部转
    /// 人工，已生效变更不受影响），false = 恢复。运行时生效，重启后
    /// 回落到配置文件值。
    /// Default: not supported by this implementation.
    async fn set_promotion_paused(
        &self,
        _paused: bool,
        _note: &str,
    ) -> crate::SFResult<PromotionSwitchInfo> {
        Err(crate::SFError::NotImplemented("promotion switch".into()))
    }

    /// 晋级台账历史（新在前），供接管台晋级历史页展示。
    /// Default: not supported by this implementation.
    async fn list_promotions(&self, _limit: usize) -> crate::SFResult<Vec<crate::PromotionRecord>> {
        Err(crate::SFError::NotImplemented("promotion list".into()))
    }

    /// 最新晋级周报（eval 长期趋势）；尚未生成过报告时返回空报告。
    /// Default: not supported by this implementation.
    async fn promotion_trend(&self) -> crate::SFResult<PromotionTrendReport> {
        Err(crate::SFError::NotImplemented("promotion trend".into()))
    }
}
