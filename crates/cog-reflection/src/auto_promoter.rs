//! 晋级触发器。
//!
//! 沙盒内 change 部署成功（apply → test → build → switch 全过）后，
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
//! 晋级结果（含回滚原因）同时调 `record_change_outcome` 回流
//! Reflection，喂给下一轮变更生成（全进化第六步的闭环回环）。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use cog_core::{PromotionGateKind, PromotionLedger, PromotionRecord, PromotionStatus, SFResult};
use tracing::{info, warn};

use crate::promotion_gate::{classify, count_diff_lines, GateVerdict};
use crate::types::{EvolutionResult, EvolutionStatus};
use crate::ReflectionEngine;

/// 幂等守卫与「等人工审批」查询看的窗口：两者必须看同一段历史，否则会出现
/// 「判定说这条已有记录、审批台却说没有那一行」。台账是追加表，一周一行时
/// 这个窗口按年计。
const LEDGER_LOOKBACK: usize = 50;

/// 待晋级提交的来源。沙盒变更提交在临时工作树里（detached HEAD，用完即弃），
/// 推送端不能假设自己的检出就是待发布内容，必须显式拿到提交。
#[derive(Debug, Clone)]
pub struct PromotionSource {
    /// 提交所在的 git 目录（工作树或裸仓库），只要对象库里有该提交即可。
    pub repo: std::path::PathBuf,
    /// 提交（完整或短 hash）。
    pub rev: String,
}

/// 晋级出口（GitOps 推送端实现，见 `gitops_publisher`）。
/// 返回发布引用（commit hash / tag）。
#[async_trait]
pub trait PromotionChannel: Send + Sync {
    /// L0：仅配置/prompt 变化，发布到 release 分支供各集群拉取端热更新。
    async fn publish_config(&self, change: &EvolutionResult) -> SFResult<String>;
    /// L1：代码变化，发布到 release 分支供各集群拉取端金丝雀。
    async fn publish_rollout(&self, change: &EvolutionResult) -> SFResult<String>;
    /// 从显式来源发布（L0）。缺省实现忽略来源，沿用推送端自己的检出——
    /// 老实现与测试据此零改动，只有真正支持指定提交的推送端才覆盖。
    async fn publish_config_from(
        &self,
        _source: &PromotionSource,
        change: &EvolutionResult,
    ) -> SFResult<String> {
        self.publish_config(change).await
    }
    /// 从显式来源发布（L1）。
    async fn publish_rollout_from(
        &self,
        _source: &PromotionSource,
        change: &EvolutionResult,
    ) -> SFResult<String> {
        self.publish_rollout(change).await
    }
    /// 这条变更是否已经在这条通道的目标分支上，返回承载它的提交。
    ///
    /// 落地通道与晋级通道各自把变更送上主线，两边互不知情：一条变更完全可以
    /// 已经由落地通道进了主线，而晋级台账那一行还在等人工审批。判据要从仓库
    /// 自己问出来——台账两边的记录互相看不见。
    ///
    /// 收的是 change id 而不是整条变更：回收一个没有对话者的台账行时手里只有
    /// 这个 id，而多一个只用得上其中一半的参数就是多一处能对不上的地方。
    ///
    /// 缺省实现答「看不出」，据此按原样发布，所以不查的推送端与测试零改动。
    async fn already_published(&self, _change_id: &str) -> Option<String> {
        None
    }
}

/// 晋级触发器。无状态时序：配额与熔断全部从台账推导，进程重启不丢。
pub struct AutoPromoter {
    policy: crate::PromotionGateConfig,
    ledger: Arc<dyn PromotionLedger>,
    channel: Option<Arc<dyn PromotionChannel>>,
    engine: Arc<ReflectionEngine>,
    /// 运行时一键暂停开关；未接线时退化为只看配置文件开关。
    switch: Option<Arc<crate::PromotionSwitch>>,
    /// 台账 cluster 字段标识；推送端固定为 "publisher"。
    cluster: String,
}

impl AutoPromoter {
    pub fn new(
        policy: crate::PromotionGateConfig,
        ledger: Arc<dyn PromotionLedger>,
        channel: Option<Arc<dyn PromotionChannel>>,
        engine: Arc<ReflectionEngine>,
    ) -> Self {
        Self {
            policy,
            ledger,
            channel,
            engine,
            switch: None,
            cluster: "publisher".into(),
        }
    }

    /// 接线运行时一键暂停开关（admin API 侧共享同一实例）。
    pub fn with_switch(mut self, switch: Arc<crate::PromotionSwitch>) -> Self {
        self.switch = Some(switch);
        self
    }

    pub fn with_cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = cluster.into();
        self
    }

    /// 取走部署进程交出的晋级判定（soak 期满的那些）并逐条判定。
    ///
    /// 判定不能由部署进程自己发一个「部署成功后回调」来完成：`self_exec` 切换
    /// 模式下部署就是本进程的 `execve`，成功即不返回，切换之后注册的回调永不
    /// 执行——那正是晋级台账在这次修复前从没有过一行的原因。部署进程只负责在
    /// 切换前把手交出去（`pending_promotions::hand_off`），由 soak 期满时活着的
    /// 进程在这里取走。判定本身幂等：台账里已有该 change 的记录就直接跳过，
    /// 所以同一份交接被重复取走不会晋级两次。
    pub async fn drain_handed_off(&self) {
        let due =
            crate::pending_promotions::load_due(self.policy.soak_secs, chrono::Utc::now()).await;
        for handed in due {
            let change_id = handed.change.artifact_id.clone();
            match self
                .decide_and_promote_with(&handed.change, Some(&handed.source))
                .await
            {
                Ok(()) => crate::pending_promotions::withdraw(&handed.path).await,
                Err(e) => warn!(
                    change_id = %change_id,
                    error = %e,
                    "handed-off promotion decision failed; keeping it for the next round"
                ),
            }
        }
    }

    /// 完整晋级判定（测试可直接调用，跳过 soak），按推送端自己的检出发布。
    pub async fn decide_and_promote(&self, change: &EvolutionResult) -> SFResult<()> {
        self.decide_and_promote_with(change, None).await
    }

    /// 完整晋级判定，可指定待发布提交来源。
    pub async fn decide_and_promote_with(
        &self,
        change: &EvolutionResult,
        source: Option<&PromotionSource>,
    ) -> SFResult<()> {
        let change_id = change.artifact_id.clone();

        // eval 门：评估明确否决的 change 不晋级。判定读**类型**（判词），不读散文。
        if let Some(report) = &change.eval_summary {
            if report.verdict.blocks_promotion() {
                let reason = format!("eval gate rejected: {}", report.summary);
                self.record(
                    &change_id,
                    "unknown",
                    PromotionStatus::Failed,
                    &reason,
                    None,
                    Some(report.summary.as_str()),
                )
                .await?;
                let _ = self
                    .engine
                    .record_change_outcome(&change_id, false, &reason)
                    .await;
                return Ok(());
            }
        }

        let files: Vec<String> = crate::ChangePipeline::parse_diff(&change.content)?
            .iter()
            .map(|t| t.path.replace('\\', "/"))
            .collect();
        let diff_lines = count_diff_lines(&change.content);
        let verdict = classify(&files, diff_lines, &self.policy);

        // 幂等：同一 change 已有终态/进行中的晋级记录则跳过。
        if self.always_recorded(&change_id).await? {
            info!(change_id = %change_id, "Change already has a promotion record; skipping");
            return Ok(());
        }

        let (level, auto) = match &verdict {
            GateVerdict::Reject { reason, .. } => {
                self.record(
                    &change_id,
                    "unknown",
                    PromotionStatus::Failed,
                    reason,
                    Some(verdict.kind()),
                    None,
                )
                .await?;
                return Ok(());
            }
            GateVerdict::AutoConfig => ("l0_config", true),
            GateVerdict::AutoRollout => ("l1_rollout", true),
            GateVerdict::RequireApproval { .. } => ("l2_approval", false),
        };

        // 自动通道的降级条件：暂停 / 熔断 / 配额 / 无出口。
        //
        // 判定理由按原文留下：分级在这里只输出一个 kind，具体是哪条规则
        // （超行数 / 核心路径 / 模糊地带）此前被这句通用文案顶掉，台账里就只剩
        // "需要人看"而说不出为什么，也再量不出阈值实际拦下了多少变更。
        let decision_reason = match verdict.reason() {
            Some(reason) => reason.to_string(),
            None => format!("分级判定 {}", verdict.kind().as_str()),
        };
        let downgrade = if !auto {
            Some("分级判定需人工审批".to_string())
        } else if self.switch.as_ref().is_some_and(|s| s.is_paused()) {
            Some("运行时一键暂停（admin API）".to_string())
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
            warn!(change_id = %change_id, reason = %reason, "Promotion downgraded to manual approval");
            // 降级理由替换掉分级理由，但 kind 仍然留下分级结果，人工审批与
            // 门槛代价的统计都不受降级文案影响。
            self.record(
                &change_id,
                level,
                PromotionStatus::AwaitingApproval,
                &reason,
                Some(verdict.kind()),
                change.eval_summary.as_ref().map(|r| r.summary.as_str()),
            )
            .await?;
            self.set_change_status(&change_id, EvolutionStatus::AwaitingReview)
                .await;
            return Ok(());
        }

        // 自动晋级。
        let record_id = self
            .record(
                &change_id,
                level,
                PromotionStatus::Pending,
                &decision_reason,
                Some(verdict.kind()),
                change.eval_summary.as_ref().map(|r| r.summary.as_str()),
            )
            .await?;

        let channel = self.channel.as_ref().expect("checked above");
        let publish = match (level, source) {
            ("l0_config", Some(src)) => channel.publish_config_from(src, change).await,
            ("l0_config", None) => channel.publish_config(change).await,
            (_, Some(src)) => channel.publish_rollout_from(src, change).await,
            (_, None) => channel.publish_rollout(change).await,
        };

        match publish {
            Ok(reference) => {
                info!(change_id = %change_id, reference = %reference, "Change promoted");
                self.ledger
                    .update_status(&record_id, PromotionStatus::Promoted, &reference)
                    .await?;
                let _ = self
                    .engine
                    .record_change_outcome(&change_id, true, &format!("promoted: {reference}"))
                    .await;
            }
            Err(e) => {
                warn!(change_id = %change_id, error = %e, "Promotion publish failed");
                self.ledger
                    .update_status(&record_id, PromotionStatus::Failed, &e.to_string())
                    .await?;
                let _ = self
                    .engine
                    .record_change_outcome(&change_id, false, &format!("promotion failed: {e}"))
                    .await;
            }
        }
        Ok(())
    }

    /// 人工审批通过后的晋级入口（审批台/admin API 调用）。
    /// 跳过配额（人本身就是配额），但仍走出口发布。
    pub async fn promote_approved(&self, change: &EvolutionResult) -> SFResult<String> {
        self.promote_approved_with(change, None).await
    }

    /// 人工审批通过后的晋级入口，可指定待发布提交来源。
    pub async fn promote_approved_with(
        &self,
        change: &EvolutionResult,
        source: Option<&PromotionSource>,
    ) -> SFResult<String> {
        let change_id = change.artifact_id.clone();
        let Some(channel) = self.channel.as_ref() else {
            return Err(cog_core::SFError::Config(
                "promotion channel not configured".into(),
            ));
        };
        let files: Vec<String> = crate::ChangePipeline::parse_diff(&change.content)?
            .iter()
            .map(|t| t.path.replace('\\', "/"))
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
        // 人工审批走的是台账里**那一行**：批准的动作就是把「等人工」这一格推
        // 出去，所以更新它而不是再追加一行。追加会让同一个 change 留下两行，
        // 旧行永远停在 awaiting_approval，停摆报表于是把一条已经晋级的变更
        // 永远算作待批。
        let record_id = match self.awaiting_approval(&change_id).await? {
            Some(id) => id,
            None => {
                self.record(
                    &change_id,
                    level,
                    PromotionStatus::Pending,
                    "人工审批通过",
                    // 人批的这条记录不带分级结论：它属于审批，不属于分级。
                    None,
                    change.eval_summary.as_ref().map(|r| r.summary.as_str()),
                )
                .await?
            }
        };

        // 落地通道与晋级通道各自把变更送上主线，两边互不知情：这条变更可能
        // 已经进了主线，而这一行还在等审批。再发一次只会对集群里已经在跑的
        // 那个版本开一场金丝雀——审批要的是把这一格推出去，不是重发。
        if let Some(reference) = channel.already_published(&change_id).await {
            info!(
                change_id = %change_id,
                reference = %reference,
                "Change is already on the mainline; recording the approval without republishing"
            );
            let outcome = format!("已在主线：{reference}（落地通道已合入，未重发）");
            self.ledger
                .update_status(&record_id, PromotionStatus::Promoted, &outcome)
                .await?;
            let _ = self
                .engine
                .record_change_outcome(&change_id, true, &format!("already on main: {reference}"))
                .await;
            return Ok(reference);
        }

        let publish = match (level, source) {
            ("l0_config", Some(src)) => channel.publish_config_from(src, change).await,
            ("l0_config", None) => channel.publish_config(change).await,
            (_, Some(src)) => channel.publish_rollout_from(src, change).await,
            (_, None) => channel.publish_rollout(change).await,
        };
        match publish {
            Ok(reference) => {
                self.ledger
                    .update_status(&record_id, PromotionStatus::Promoted, &reference)
                    .await?;
                let _ = self
                    .engine
                    .record_change_outcome(
                        &change_id,
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

    async fn always_recorded(&self, change_id: &str) -> SFResult<bool> {
        let recent = self.ledger.recent(LEDGER_LOOKBACK).await?;
        Ok(recent.iter().any(|r| {
            r.change_id == change_id
                && r.cluster == self.cluster
                && matches!(
                    r.status,
                    PromotionStatus::Pending
                        | PromotionStatus::Promoted
                        | PromotionStatus::AwaitingApproval
                )
        }))
    }

    /// 这条变更正卡在「等人工审批」那一格时，返回那一行的 id。
    ///
    /// 审批台据此选门：有这一行，人工批准走的就是晋级通道——那是这一格唯一
    /// 的出口；没有，就照旧走本地构建与切换。查询与幂等守卫看同一个窗口，
    /// 否则会出现「晋级器说这条已有记录、审批台说没有那一行」这种谁都对不上的
    /// 读数。
    pub async fn awaiting_approval(&self, change_id: &str) -> SFResult<Option<String>> {
        let recent = self.ledger.recent(LEDGER_LOOKBACK).await?;
        Ok(recent
            .into_iter()
            .find(|r| {
                r.change_id == change_id
                    && r.cluster == self.cluster
                    && matches!(r.status, PromotionStatus::AwaitingApproval)
            })
            .map(|r| r.id))
    }

    /// 把「等人工审批」里那些已经由落地通道合进主线的行收掉。
    ///
    /// 这一格没有别的回收者，而它的对话者会先走：写这一行的是晋级器，晋级器只
    /// 认手里那条变更；变更一旦落地就被移出待处理队列，于是这一行既等不到人批、
    /// 也等不到任何一轮判定再碰它，只有停摆告警一直读着它的出口。
    ///
    /// 判据只有一条，且是实证：这条变更已经在主线上了。那它等的那件事已经由
    /// 另一条路做完，把它记成晋级是**如实的记录**，不是替人做决定——没上主线的
    /// 行一格都不动，人批不动它、这条也绕不过它。
    pub async fn reclaim_landed_approvals(&self) {
        let Some(channel) = self.channel.as_ref() else {
            return;
        };
        let recent = match self.ledger.recent(LEDGER_LOOKBACK).await {
            Ok(recent) => recent,
            Err(e) => {
                warn!(
                    error = %e,
                    "promotion ledger unreadable; no awaiting-approval row reclaimed this round"
                );
                return;
            }
        };
        for rec in recent.iter().filter(|r| {
            r.cluster == self.cluster && matches!(r.status, PromotionStatus::AwaitingApproval)
        }) {
            let Some(reference) = channel.already_published(&rec.change_id).await else {
                continue;
            };
            let outcome = format!("已在主线：{reference}（落地通道已合入，未重发）");
            match self
                .ledger
                .update_status(&rec.id, PromotionStatus::Promoted, &outcome)
                .await
            {
                Ok(()) => info!(
                    change_id = %rec.change_id,
                    record_id = %rec.id,
                    reference = %reference,
                    "Change is already on the mainline; closed its awaiting-approval row"
                ),
                Err(e) => warn!(
                    record_id = %rec.id,
                    error = %e,
                    "could not close an awaiting-approval row whose change is already live"
                ),
            }
        }
    }

    /// 追加台账记录，返回记录 id。
    async fn record(
        &self,
        change_id: &str,
        level: &str,
        status: PromotionStatus,
        reason: &str,
        gate_kind: Option<PromotionGateKind>,
        eval_summary: Option<&str>,
    ) -> SFResult<String> {
        let now = Utc::now();
        let rec = PromotionRecord {
            id: uuid::Uuid::new_v4().to_string(),
            change_id: change_id.to_string(),
            level: level.to_string(),
            gate_kind,
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

    async fn set_change_status(&self, change_id: &str, status: EvolutionStatus) {
        if let Some(evo) = self.engine.evolution.as_ref() {
            evo.update_status(change_id, status).await;
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
        async fn publish_config(&self, change: &EvolutionResult) -> SFResult<String> {
            self.published
                .lock()
                .unwrap()
                .push(change.artifact_id.clone());
            if self.fail {
                return Err(cog_core::SFError::IO("publish boom".into()));
            }
            Ok(format!("commit-{}", change.artifact_id))
        }
        async fn publish_rollout(&self, change: &EvolutionResult) -> SFResult<String> {
            self.publish_config(change).await
        }
    }

    fn change(id: &str, file: &str) -> EvolutionResult {
        EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
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
        policy: crate::PromotionGateConfig,
        ledger: Arc<cog_storage::MemoryStateBackend>,
        channel: Option<Arc<dyn PromotionChannel>>,
    ) -> AutoPromoter {
        AutoPromoter::new(policy, ledger, channel, engine())
    }

    /// 部署进程在 `execve` 之前交出的那一条，必须由活着的进程取走并落进台账。
    /// 这一格是这次修复的判据：改之前台账里一行都没有，而原因不是「没有变更
    /// 部署成功」，是判定挂在了永不返回的调用之后。
    #[tokio::test]
    async fn a_handed_off_promotion_is_taken_up_and_recorded() {
        let _guard = crate::pending_promotions::DATA_DIR_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", tmp.path());

        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let policy = crate::PromotionGateConfig {
            enabled: true,
            // soak 已满由 `is_due` 的考试负责，这里考的是「取走并落账」。
            soak_secs: 0,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), None);

        let source = crate::PromotionSource {
            repo: std::path::PathBuf::from("/host-git"),
            rev: "abc123".into(),
        };
        crate::pending_promotions::hand_off(
            &change("handed-off", "crates/cog-agent/src/tools.rs"),
            &source,
        )
        .await
        .unwrap();

        p.drain_handed_off().await;

        let records = ledger.recent(10).await.unwrap();
        assert_eq!(
            records.len(),
            1,
            "交出去的判定必须落成台账里的记录，否则解除格仍然没有写者"
        );
        assert_eq!(records[0].change_id, "handed-off");
        assert!(
            crate::pending_promotions::load_due(0, chrono::Utc::now())
                .await
                .is_empty(),
            "判完要把交接撤回，否则每一轮都会再判一次"
        );

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 判定不到 soak 期不该取走：取早了等于 soak 从未发生。
    #[tokio::test]
    async fn a_handed_off_promotion_still_soaking_is_left_alone() {
        let _guard = crate::pending_promotions::DATA_DIR_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", tmp.path());

        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let policy = crate::PromotionGateConfig {
            enabled: true,
            soak_secs: 600,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), None);

        crate::pending_promotions::hand_off(
            &change("still-soaking", "crates/cog-agent/src/tools.rs"),
            &crate::PromotionSource {
                repo: std::path::PathBuf::from("/host-git"),
                rev: "abc123".into(),
            },
        )
        .await
        .unwrap();

        p.drain_handed_off().await;

        assert!(ledger.recent(10).await.unwrap().is_empty());
        assert_eq!(
            crate::pending_promotions::load_due(0, chrono::Utc::now())
                .await
                .len(),
            1,
            "没到期的交接要留在盘上等下一轮"
        );

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    #[tokio::test]
    async fn l1_change_auto_promotes_when_all_gates_pass() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        p.decide_and_promote(&change("p1", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, PromotionStatus::Promoted);
        assert_eq!(records[0].level, "l1_rollout");
        assert_eq!(channel.published.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn l0_change_goes_to_config_channel() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel));
        p.decide_and_promote(&change("p2", "prompts/default.yaml"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].level, "l0_config");
        assert_eq!(records[0].status, PromotionStatus::Promoted);
    }

    #[tokio::test]
    async fn core_path_waits_for_approval() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), None);
        p.decide_and_promote(&change(
            "p3",
            "crates/cog-storage/src/postgres/state_backend.rs",
        ))
        .await
        .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::AwaitingApproval);
        assert_eq!(records[0].level, "l2_approval");
    }

    /// 落地通道已经把这条变更合进主线，推送端要答得出承载它的提交。答不出来
    /// 就退回缺省，照原样发布——这一格是缺省的反面。按 id 配对：仓库答的是
    /// 「这一个 id 在主线上」，不认识的 id 要答「没查到」，否则回收者会把每一行
    /// 都当成已落地。
    struct LandedChannel {
        published: Mutex<Vec<String>>,
        landed: Vec<(String, String)>,
    }

    #[async_trait]
    impl PromotionChannel for LandedChannel {
        async fn publish_config(&self, change: &EvolutionResult) -> SFResult<String> {
            self.published
                .lock()
                .unwrap()
                .push(change.artifact_id.clone());
            Ok("should-not-have-published".into())
        }
        async fn publish_rollout(&self, change: &EvolutionResult) -> SFResult<String> {
            self.publish_config(change).await
        }
        async fn already_published(&self, change_id: &str) -> Option<String> {
            self.landed
                .iter()
                .find(|(id, _)| id == change_id)
                .map(|(_, rev)| rev.clone())
        }
    }

    /// 人工批准一条「等人工审批」的变更，要把**那一行**推出去，而不是再追加
    /// 一行：追加会让旧行永远停在 awaiting_approval，停摆报表于是把一条已经
    /// 晋级的变更永远算作待批。
    #[tokio::test]
    async fn approving_a_change_awaiting_approval_moves_that_row_out_of_awaiting() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        let c = change(
            "approve-me",
            "crates/cog-storage/src/postgres/state_backend.rs",
        );
        p.decide_and_promote(&c).await.unwrap();

        let before = ledger.recent(10).await.unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].status, PromotionStatus::AwaitingApproval);
        assert_eq!(
            p.awaiting_approval("approve-me").await.unwrap().as_deref(),
            Some(before[0].id.as_str()),
            "审批台要先认得出这条变更卡在等人工审批，才谈得上选门"
        );

        p.promote_approved(&c).await.unwrap();

        let after = ledger.recent(10).await.unwrap();
        assert_eq!(after.len(), 1, "批准更新那一行，不该再追加一行");
        assert_eq!(after[0].id, before[0].id);
        assert_eq!(after[0].status, PromotionStatus::Promoted);
        assert!(
            p.awaiting_approval("approve-me").await.unwrap().is_none(),
            "批准之后那一格必须没有出口之外的东西留下"
        );
        assert_eq!(channel.published.lock().unwrap().len(), 1);
    }

    /// 落地通道已经把变更合进主线，而台账那一行还在等审批：批准要能把它推
    /// 出去，但不能对集群里已经在跑的那个版本再开一场金丝雀。
    #[tokio::test]
    async fn approving_a_change_already_on_the_mainline_does_not_republish() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(LandedChannel {
            published: Mutex::new(Vec::new()),
            landed: vec![("already-landed".into(), "1fcf76a".into())],
        });
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        let c = change(
            "already-landed",
            "crates/cog-storage/src/postgres/state_backend.rs",
        );
        p.decide_and_promote(&c).await.unwrap();
        assert_eq!(
            ledger.recent(10).await.unwrap()[0].status,
            PromotionStatus::AwaitingApproval
        );

        let reference = p.promote_approved(&c).await.unwrap();

        assert_eq!(reference, "1fcf76a");
        assert!(
            channel.published.lock().unwrap().is_empty(),
            "已经在主线上就不该再发一次"
        );
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, PromotionStatus::Promoted);
        assert!(
            records[0].outcome.contains("已在主线"),
            "「没重发」这件事要落在证据里，否则事后读不出它为什么没发布：{}",
            records[0].outcome
        );
    }

    /// 等人工审批的行，等的可能是一件已经由落地通道做完了的事：变更既然已经
    /// 进了主线，落地那一刻就被移出待处理队列，于是这一行既等不到人批（审批台
    /// 手里已经找不到那条变更），也等不到任何一轮判定再碰它。只有这个回收者
    /// 能把它销账，而它的判据是实证：仓库说这个 id 已经在主线上。
    #[tokio::test]
    async fn a_landed_change_closes_its_awaiting_approval_row() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(LandedChannel {
            published: Mutex::new(Vec::new()),
            landed: vec![("landed-x".into(), "1fcf76a".into())],
        });
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));

        // 核心路径的变更分级判定为「需人工审批」，落成 awaiting_approval 那格。
        let core = "crates/cog-storage/src/postgres/state_backend.rs";
        p.decide_and_promote(&change("landed-x", core))
            .await
            .unwrap();
        p.decide_and_promote(&change("still-waiting", core))
            .await
            .unwrap();
        let before = ledger.recent(10).await.unwrap();
        assert_eq!(before.len(), 2);
        assert!(before
            .iter()
            .all(|r| r.status == PromotionStatus::AwaitingApproval));

        p.reclaim_landed_approvals().await;

        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records.len(), 2, "回收是销账，不是再记一笔");
        let landed = records.iter().find(|r| r.change_id == "landed-x").unwrap();
        assert_eq!(landed.status, PromotionStatus::Promoted);
        assert!(
            landed.outcome.contains("已在主线：1fcf76a"),
            "「为什么它能被销账」要落在证据里：{}",
            landed.outcome
        );
        let waiting = records
            .iter()
            .find(|r| r.change_id == "still-waiting")
            .unwrap();
        assert_eq!(
            waiting.status,
            PromotionStatus::AwaitingApproval,
            "没上主线的行一格都不能动——回收绕不过人批，只捡那些人已经不用批的"
        );
        assert!(
            channel.published.lock().unwrap().is_empty(),
            "回收不发布：做完这件事的是落地通道，不是晋级通道"
        );
    }

    /// 台账里没有这一行时，人工审批照旧落一条新记录并发布——接线不能把原有的
    /// 「批一条还没进过晋级器的变更」堵死。
    #[tokio::test]
    async fn approving_a_change_with_no_ledger_row_appends_a_record() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));

        let reference = p
            .promote_approved(&change("fresh", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();

        assert_eq!(reference, "commit-fresh");
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, PromotionStatus::Promoted);
        assert_eq!(records[0].decision_reason, "人工审批通过");
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
            crate::PromotionGateConfig::default(),
            ledger.clone(),
            Some(channel.clone()),
        );
        p.decide_and_promote(&change("p4", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::AwaitingApproval);
        assert!(records[0].outcome.contains("一键暂停"));
        assert!(channel.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn runtime_paused_switch_downgrades_to_approval() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        // 配置文件总开关开着，但运行时开关被 admin API 暂停：排队晋级转人工。
        let switch = Arc::new(crate::PromotionSwitch::new());
        switch.set_paused(true, "人工介入");
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone())).with_switch(switch.clone());
        p.decide_and_promote(&change("p4b", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::AwaitingApproval);
        assert!(records[0].outcome.contains("运行时一键暂停"));
        assert!(channel.published.lock().unwrap().is_empty());

        // 恢复后同一 change 可自动晋级（幂等跳过仅限已有记录，新 change 走全自动）。
        switch.set_paused(false, "恢复");
        p.decide_and_promote(&change("p4c", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::Promoted);
        assert_eq!(channel.published.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn quota_exceeded_downgrades() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        // 预填 3 条 24h 内的 promoted 记录，打满默认配额。
        for i in 0..3 {
            ledger
                .record(PromotionRecord {
                    id: format!("old-{i}"),
                    change_id: format!("old-{i}"),
                    level: "l1_rollout".into(),
                    gate_kind: None,
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
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        p.decide_and_promote(&change("p5", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        let mine = records.iter().find(|r| r.change_id == "p5").unwrap();
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
                    change_id: format!("rb-{i}"),
                    level: "l1_rollout".into(),
                    gate_kind: None,
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
        let policy = crate::PromotionGateConfig {
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
                    change_id: format!("s-{i}"),
                    level: "l1_rollout".into(),
                    gate_kind: None,
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
        let policy = crate::PromotionGateConfig {
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
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel));
        p.decide_and_promote(&change("p6", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::Failed);
    }

    #[tokio::test]
    async fn eval_rejected_change_never_promotes() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        let mut pt = change("p7", "crates/cog-agent/src/tools.rs");
        pt.eval_summary = Some(cog_core::EvalReport {
            verdict: cog_core::EvalVerdict::Reject,
            summary: "Reject z=-1.2 uplift -8%".into(),
        });
        p.decide_and_promote(&pt).await.unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::Failed);
        assert!(channel.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn duplicate_change_not_promoted_twice() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(FakeChannel {
            published: Mutex::new(Vec::new()),
            fail: false,
        });
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), Some(channel.clone()));
        let pt = change("p8", "crates/cog-agent/src/tools.rs");
        p.decide_and_promote(&pt).await.unwrap();
        p.decide_and_promote(&pt).await.unwrap();
        assert_eq!(channel.published.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_channel_downgrades_to_approval() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let policy = crate::PromotionGateConfig {
            enabled: true,
            ..Default::default()
        };
        let p = promoter(policy, ledger.clone(), None);
        p.decide_and_promote(&change("p9", "crates/cog-agent/src/tools.rs"))
            .await
            .unwrap();
        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records[0].status, PromotionStatus::AwaitingApproval);
        assert!(records[0].outcome.contains("出口未配置"));
    }
}
