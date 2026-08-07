//! 晋级触发器（docs/2026-08-06_真自治全进化无人值守方案.md）。
//!
//! 沙盒内 patch 部署成功（apply → test → build → switch 全过）后，
//! 由本模块决定它的下一站：
//!
//! ```text
//! 沙盒 deploy_success
//!   → soak 试跑等待（期间进程崩了晋级自然作废，台账留 Pending）
//!   → 一键暂停检查（promotion.enabled=false 全部转人工）
//!   → 熔断检查（连续回滚/失败超阈值 → 转人工，人批准的成功晋级会
//!     把窗口推出去，熔断自然解除）
//!   → 配额检查（24h 内自动晋级次数超 quota_per_day → 转人工）
//!   → 分级引擎 classify：
//!       AutoConfig       → PromotionChannel::publish_config（GitOps L0）
//!       AutoRollout      → PromotionChannel::publish_rollout（GitOps L1）
//!       RequireApproval  → 状态 AwaitingReview + 台账 awaiting_approval（审批台待办）
//! ```
//!
//! 晋级结果（含回滚原因）同时调 `record_patch_outcome` 回流
//! Reflection，喂给下一轮补丁生成（全进化第六步的闭环回环）。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use cog_core::{PromotionLedger, PromotionRecord, PromotionStatus, SFResult};
use tracing::{info, warn};

use crate::promotion_gate::{classify, count_diff_lines, GateVerdict};
use crate::types::{EvolutionResult, EvolutionStatus};
use crate::ReflectionEngine;

/// 晋级出口（GitOps 推送端实现，见 `gitops_publisher`）。
/// 返回发布引用（commit hash / tag）。
#[async_trait]
pub trait PromotionChannel: Send + Sync {
    /// L0：仅配置/prompt 变化，发布到 release 分支供各集群拉取端热更新。
    async fn publish_config(&self, patch: &EvolutionResult) -> SFResult<String>;
    /// L1：代码变化，发布到 release 分支供各集群拉取端金丝雀。
    async fn publish_rollout(&self, patch: &EvolutionResult) -> SFResult<String>;
}

/// 晋级触发器。无状态时序：配额与熔断全部从台账推导，进程重启不丢。
pub struct AutoPromoter {
    policy: cog_core::PromotionGateConfig,
    ledger: Arc<dyn PromotionLedger>,
    channel: Option<Arc<dyn PromotionChannel>>,
    engine: Arc<ReflectionEngine>,
    /// 台账 cluster 字段标识；推送端固定为 "publisher"。
    cluster: String,
}

impl AutoPromoter {
    pub fn new(
        policy: cog_core::PromotionGateConfig,
        ledger: Arc<dyn PromotionLedger>,
        channel: Option<Arc<dyn PromotionChannel>>,
        engine: Arc<ReflectionEngine>,
    ) -> Self {
        Self {
            policy,
            ledger,
            channel,
            engine,
            cluster: "publisher".into(),
        }
    }

    pub fn with_cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = cluster.into();
        self
    }

    /// 沙盒部署成功回调：等待 soak 后走完整晋级判定。
    /// 设计为在后台任务里调用（调用方 tokio::spawn）。
    pub async fn on_sandbox_deployed(&self, patch: EvolutionResult) {
        let patch_id = patch.artifact_id.clone();
        if self.policy.soak_secs > 0 {
            info!(
                patch_id = %patch_id,
                soak_secs = self.policy.soak_secs,
                "Patch deployed in sandbox; soaking before promotion decision"
            );
            tokio::time::sleep(std::time::Duration::from_secs(self.policy.soak_secs)).await;
        }
        if let Err(e) = self.decide_and_promote(&patch).await {
            warn!(patch_id = %patch_id, error = %e, "Promotion decision failed");
        }
    }

    /// 完整晋级判定（测试可直接调用，跳过 soak）。
    pub async fn decide_and_promote(&self, patch: &EvolutionResult) -> SFResult<()> {
        let patch_id = patch.artifact_id.clone();

        // eval 门：评估明确否决的 patch 不晋级。
        if let Some(summary) = &patch.eval_summary {
            if summary.starts_with("Reject") || summary.starts_with("Inconclusive") {
                self.record(
                    &patch_id,
                    "unknown",
                    PromotionStatus::Failed,
                    &format!("eval gate rejected: {summary}"),
                    patch.eval_summary.as_deref(),
                )
                .await?;
                let _ = self
                    .engine
                    .record_patch_outcome(
                        &patch_id,
                        false,
                        &format!("eval gate rejected: {summary}"),
                    )
                    .await;
                return Ok(());
            }
        }

        let files: Vec<String> = crate::PatchPipeline::parse_patch(&patch.content)?
            .iter()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .collect();
        let diff_lines = count_diff_lines(&patch.content);
        let verdict = classify(&files, diff_lines, &self.policy);

        // 幂等：同一 patch 已有终态/进行中的晋级记录则跳过。
        if self.always_recorded(&patch_id).await? {
            info!(patch_id = %patch_id, "Patch already has a promotion record; skipping");
            return Ok(());
        }

        let (level, auto) = match &verdict {
            GateVerdict::Reject { reason } => {
                self.record(&patch_id, "unknown", PromotionStatus::Failed, reason, None)
                    .await?;
                return Ok(());
            }
            GateVerdict::AutoConfig => ("l0_config", true),
            GateVerdict::AutoRollout => ("l1_rollout", true),
            GateVerdict::RequireApproval { .. } => ("l2_approval", false),
        };

        // 自动通道的降级条件：暂停 / 熔断 / 配额 / 无出口。
        let decision_reason = format!("{verdict:?}");
        let downgrade = if !auto {
            Some("分级判定需人工审批".to_string())
        } else if !self.policy.enabled {
            Some("自动晋级总开关关闭（一键暂停）".to_string())
        } else if let Some(reason) = self.breaker_tripped().await? {
            Some(format!("熔断器触发：{reason}"))
        } else if self.quota_exceeded().await? {
            Some(format!(
                "24h 自动晋级配额（{}）已满",
                self.policy.quota_per_day
            ))
        } else if self.channel.is_none() {
            Some("晋级出口未配置（GitOps 推送端不可用）".to_string())
        } else {
            None
        };

        if let Some(reason) = downgrade {
            warn!(patch_id = %patch_id, reason = %reason, "Promotion downgraded to manual approval");
            self.record(
                &patch_id,
                level,
                PromotionStatus::AwaitingApproval,
                &reason,
                patch.eval_summary.as_deref(),
            )
            .await?;
            self.set_patch_status(&patch_id, EvolutionStatus::AwaitingReview)
                .await;
            return Ok(());
        }

        // 自动晋级。
        let record_id = self
            .record(
                &patch_id,
                level,
                PromotionStatus::Pending,
                &decision_reason,
                patch.eval_summary.as_deref(),
            )
            .await?;

        let channel = self.channel.as_ref().expect("checked above");
        let publish = if level == "l0_config" {
            channel.publish_config(patch).await
        } else {
            channel.publish_rollout(patch).await
        };

        match publish {
            Ok(reference) => {
                info!(patch_id = %patch_id, reference = %reference, "Patch promoted");
                self.ledger
                    .update_status(&record_id, PromotionStatus::Promoted, &reference)
                    .await?;
                let _ = self
                    .engine
                    .record_patch_outcome(&patch_id, true, &format!("promoted: {reference}"))
                    .await;
            }
            Err(e) => {
                warn!(patch_id = %patch_id, error = %e, "Promotion publish failed");
                self.ledger
                    .update_status(&record_id, PromotionStatus::Failed, &e.to_string())
                    .await?;
                let _ = self
                    .engine
                    .record_patch_outcome(&patch_id, false, &format!("promotion failed: {e}"))
                    .await;
            }
        }
        Ok(())
    }

    /// 人工审批通过后的晋级入口（审批台/admin API 调用）。
    /// 跳过配额（人本身就是配额），但仍走出口发布。
    pub async fn promote_approved(&self, patch: &EvolutionResult) -> SFResult<String> {
        let patch_id = patch.artifact_id.clone();
        let Some(channel) = self.channel.as_ref() else {
            return Err(cog_core::SFError::Config(
                "promotion channel not configured".into(),
            ));
        };
        let files: Vec<String> = crate::PatchPipeline::parse_patch(&patch.content)?
            .iter()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .collect();
        let level = if files.iter().all(|f| {
            self.policy
                .config_prefixes
                .iter()
                .any(|p| f.starts_with(p.as_str()))
        }) {
            "l0_config"
        } else {
            "l1_rollout"
        };
        let record_id = self
            .record(
                &patch_id,
                level,
                PromotionStatus::Pending,
                "人工审批通过",
                patch.eval_summary.as_deref(),
            )
            .await?;
        let publish = if level == "l0_config" {
            channel.publish_config(patch).await
        } else {
            channel.publish_rollout(patch).await
        };
        match publish {
            Ok(reference) => {
                self.ledger
                    .update_status(&record_id, PromotionStatus::Promoted, &reference)
                    .await?;
                let _ = self
                    .engine
                    .record_patch_outcome(
                        &patch_id,
                        true,
                        &format!("promoted (approved): {reference}"),
                    )
                    .await;
                Ok(reference)
            }
            Err(e) => {
                self.ledger
                    .update_status(&record_id, PromotionStatus::Failed, &e.to_string())
                    .await?;
                Err(e)
            }
        }
    }

    /// 熔断判定：台账最近记录中，连续 RolledBack ≥ 阈值或
    /// 连续 Failed ≥ 阈值。返回触发原因。
    async fn breaker_tripped(&self) -> SFResult<Option<String>> {
        let recent = self.ledger.recent(20).await?;
        let mut consecutive_rollback = 0u32;
        let mut consecutive_failed = 0u32;
        for rec in &recent {
            match rec.status {
                PromotionStatus::RolledBack => {
                    consecutive_rollback += 1;
                    consecutive_failed = 0;
                }
                PromotionStatus::Failed => {
                    consecutive_failed += 1;
                    consecutive_rollback = 0;
                }
                PromotionStatus::Promoted => break,
                _ => {}
            }
        }
        if consecutive_rollback >= self.policy.rollback_breaker_threshold {
            return Ok(Some(format!("连续 {consecutive_rollback} 次晋级后回滚")));
        }
        if consecutive_failed >= self.policy.failure_breaker_threshold {
            return Ok(Some(format!("连续 {consecutive_failed} 次晋级执行失败")));
        }
        Ok(None)
    }

    async fn quota_exceeded(&self) -> SFResult<bool> {
        let since: DateTime<Utc> = Utc::now() - Duration::hours(24);
        let count = self.ledger.count_promoted_since(since).await?;
        Ok(count >= self.policy.quota_per_day as u64)
    }

    async fn always_recorded(&self, patch_id: &str) -> SFResult<bool> {
        let recent = self.ledger.recent(50).await?;
        Ok(recent.iter().any(|r| {
            r.patch_id == patch_id
                && r.cluster == self.cluster
                && matches!(
                    r.status,
                    PromotionStatus::Pending
                        | PromotionStatus::Promoted
                        | PromotionStatus::AwaitingApproval
                )
        }))
    }

    /// 追加台账记录，返回记录 id。
    async fn record(
        &self,
        patch_id: &str,
        level: &str,
        status: PromotionStatus,
        reason: &str,
        eval_summary: Option<&str>,
    ) -> SFResult<String> {
        let now = Utc::now();
        let rec = PromotionRecord {
            id: uuid::Uuid::new_v4().to_string(),
            patch_id: patch_id.to_string(),
            level: level.to_string(),
            decision_reason: reason.to_string(),
            cluster: self.cluster.clone(),
            status,
            outcome: reason.to_string(),
            eval_summary: eval_summary.map(|s| s.to_string()),
            created_at: now,
            updated_at: now,
        };
        let id = rec.id.clone();
        self.ledger.record(rec).await?;
        Ok(id)
    }

    async fn set_patch_status(&self, patch_id: &str, status: EvolutionStatus) {
        if let Some(evo) = self.engine.evolution.as_ref() {
            evo.update_status(patch_id, status).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeChannel {
        published: Mutex<Vec<String>>,
        fail: bool,
    }

    #[async_trait]
    impl PromotionChannel for FakeChannel {
        async fn publish_config(&self, patch: &EvolutionResult) -> SFResult<String> {
            self.published
                .lock()
                .unwrap()
                .push(patch.artifact_id.clone());
            if self.fail {
                return Err(cog_core::SFError::IO("publish boom".into()));
            }
            Ok(format!("commit-{}", patch.artifact_id))
        }
        async fn publish_rollout(&self, patch: &EvolutionResult) -> SFResult<String> {
            self.publish_config(patch).await
        }
    }

    fn patch(id: &str, file: &str) -> EvolutionResult {
        EvolutionResult {
            kind: crate::types::EvolutionKind::CodePatch,
            artifact_id: id.into(),
            description: "test".into(),
            content: format!(
                "diff --git a/{file} b/{file}\nindex 1111111..2222222 100644\n--- a/{file}\n+++ b/{file}\n@@ -1 +1,2 @@\n x\n+y\n"
            ),
            status: EvolutionStatus::Active,
            created_at: Utc::now(),
            eval_summary: None,
        }
    }

    fn engine() -> Arc<ReflectionEngine> {
        Arc::new(ReflectionEngine::new_in_memory(Arc::new(
            tokio::sync::RwLock::new(cog_core::SkillRegistry::new()),
        )))
    }

    fn promoter(
        policy: cog_core::PromotionGateConfig,
        ledger: Arc<cog_storage::MemoryStateBackend>,
        channel: Option<Arc<dyn PromotionChannel>>,
    ) -> AutoPromoter {
        AutoPromoter::new(policy, ledger, channel, engine())
    }

    #[tokio::test]
    async fn l1_patch_auto_promotes_when_all_gates_pass() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        p.decide_and_promote(&patch("p1", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, PromotionStatus::Promoted);
        assert_eq!(records[0].level, "l1_rollout");
        assert_eq!(channel.published.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn l0_patch_goes_to_config_channel() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel));
        p.decide_and_promote(&patch("p2", "prompts/default.yaml"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].level, "l0_config");
        assert_eq!(records[0].status, PromotionStatus::Promoted);
    }

    #[tokio::test]
    async fn core_path_waits_for_approval() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), None);
        p.decide_and_promote(&patch(
            "p3",
            "crates/cog-storage/src/postgres/state_backend.rs",
        ))
        .await
        .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::AwaitingApproval);
        assert_eq!(records[0].level, "l2_approval");
    }

    #[tokio::test]
    async fn paused_switch_downgrades_to_approval() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        // enabled=false（一键暂停）
        let p = promoter(
            cog_core::PromotionGateConfig::default(),
            ledger.clone(),
            Some(channel.clone()),
        );
        p.decide_and_promote(&patch("p4", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::AwaitingApproval);
        assert!(records[0].outcome.contains("一键暂停"));
        assert!(channel.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn quota_exceeded_downgrades() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        // 预填 3 条 24h 内的 promoted 记录，打满默认配额。
        for i in 0..3 {
            ledger
                .record(PromotionRecord {
                    id: format!("old-{i}"),
                    patch_id: format!("old-{i}"),
                    level: "l1_rollout".into(),
                    decision_reason: "test".into(),
                    cluster: "publisher".into(),
                    status: PromotionStatus::Promoted,
                    outcome: "ok".into(),
                    eval_summary: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                })
                .await
                .unwrap();
        }
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        p.decide_and_promote(&patch("p5", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        let mine = records.iter().find(|r| r.patch_id == "p5").unwrap();
        assert_eq!(mine.status, PromotionStatus::AwaitingApproval);
        assert!(mine.outcome.contains("配额"));
        assert!(channel.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn breaker_trips_on_consecutive_rollbacks() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        for i in 0..2 {
            ledger
                .record(PromotionRecord {
                    id: format!("rb-{i}"),
                    patch_id: format!("rb-{i}"),
                    level: "l1_rollout".into(),
                    decision_reason: "test".into(),
                    cluster: "publisher".into(),
                    status: PromotionStatus::RolledBack,
                    outcome: "canary regression".into(),
                    eval_summary: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                })
                .await
                .unwrap();
        }
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), None);
        let reason = p.breaker_tripped().await.unwrap();
        assert!(reason.is_some());
        assert!(reason.unwrap().contains("回滚"));
    }

    #[tokio::test]
    async fn breaker_resets_after_success() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        for (i, status) in [
            PromotionStatus::RolledBack,
            PromotionStatus::RolledBack,
            PromotionStatus::Promoted,
        ]
        .iter()
        .enumerate()
        {
            ledger
                .record(PromotionRecord {
                    id: format!("s-{i}"),
                    patch_id: format!("s-{i}"),
                    level: "l1_rollout".into(),
                    decision_reason: "test".into(),
                    cluster: "publisher".into(),
                    status: *status,
                    outcome: "ok".into(),
                    eval_summary: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                })
                .await
                .unwrap();
        }
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), None);
        assert!(p.breaker_tripped().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn publish_failure_marks_failed_and_feeds_breaker() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: true,
        });
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel));
        p.decide_and_promote(&patch("p6", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::Failed);
    }

    #[tokio::test]
    async fn eval_rejected_patch_never_promotes() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        let mut pt = patch("p7", "crates/cog-agent/src/tools.rs");
        pt.eval_summary = Some("Reject z=-1.2 uplift -8%".into());
        p.decide_and_promote(&pt).await.unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::Failed);
        assert!(channel.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn duplicate_patch_not_promoted_twice() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        let pt = patch("p8", "crates/cog-agent/src/tools.rs");
        p.decide_and_promote(&pt).await.unwrap();
        p.decide_and_promote(&pt).await.unwrap();
        assert_eq!(channel.published.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_channel_downgrades_to_approval() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let policy = cog_core::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), None);
        p.decide_and_promote(&patch("p9", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::AwaitingApproval);
        assert!(records[0].outcome.contains("出口未配置"));
    }
}
