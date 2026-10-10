//! Admin-facing evolution service — exposes manual control over the
//! self-evolution pipeline to the Gateway.

use std::sync::Arc;

use cog_core::{
    EvolutionAdmin, EvolutionApplyResponse, EvolutionChangeInfo, EvolutionDeployResponse, SFError,
    SFResult,
};
use tracing::{info, warn};

/// Service that implements [`EvolutionAdmin`] by delegating to the reflection
/// engine, change pipeline, and deployer.
pub struct EvolutionAdminService {
    engine: Arc<crate::ReflectionEngine>,
    pipeline: crate::ChangePipeline,
    deployer: crate::EvolutionDeployer,
    binary_switcher: Option<Arc<dyn cog_core::BinarySwitcher>>,
    evolution_metrics: Option<Arc<dyn cog_core::EvolutionMetrics>>,
    audit_stream: Option<Arc<dyn cog_core::AuditStream>>,
    image_rollout: Option<Arc<crate::ImageRollout>>,
    artifact_evolution: Option<Arc<crate::ArtifactEvolution>>,
    /// 策略提议结果存储：source-level EvolutionEngine 需要 LLM，未配置时
    /// （in-memory 模式）产物级进化链路用本地存储兜底，与 LLM 可用性解耦。
    policy_results:
        Arc<tokio::sync::RwLock<std::collections::HashMap<String, crate::types::EvolutionResult>>>,
    /// 变更行状态广播：接管台 SSE 订阅此通道，状态翻转即时推送而非轮询。
    stream: Option<tokio::sync::broadcast::Sender<EvolutionChangeInfo>>,
    /// 自动晋级运行时开关（与 AutoPromoter 共享同一实例）。
    switch: Option<Arc<crate::PromotionSwitch>>,
    /// 晋级器本体（与自动通道共享同一实例）。审批台只有在手里有这个句柄时
    /// 才认得「这条变更正卡在等人工审批那一格」——那一格唯一的出口就是它。
    promoter: Option<Arc<crate::AutoPromoter>>,
    /// 晋级台账（晋级历史页数据源）。
    promotion_ledger: Option<Arc<dyn cog_core::PromotionLedger>>,
    /// 配置文件里的自动晋级总开关快照（开关快照的 config_enabled 一列）。
    promotion_config_enabled: bool,
    /// 最新晋级周报（周期报表器写入，admin 端点读取）。
    trend_latest: Option<Arc<tokio::sync::RwLock<Option<cog_core::PromotionTrendReport>>>>,
    /// 工作区分配器：admin 触发的应用/构建各取一棵临时工作树，与自动流水线
    /// 和各轮演进互不干涉。未接线时退回进程工作目录（单测场景）。
    workspaces: Option<Arc<crate::workspace::WorkspaceManager>>,
    /// 本进程读的那个变更队列（目录 + 是否属主 + 周期）。与观测面共用同一个
    /// 对象，所以列举报的目录与指标挂的目录不可能是两个目录。
    queue: Option<Arc<crate::evolution_queue_readings::EvolutionQueueReadings>>,
}

/// 从 unified diff 文本提取一行摘要（"3 files, +42 -17"）；非 diff 内容返回 None。
fn summarize_diff(content: &str) -> Option<String> {
    let mut files = 0usize;
    let mut adds = 0usize;
    let mut dels = 0usize;
    for line in content.lines() {
        if line.starts_with("+++ ") {
            files += 1;
        } else if line.starts_with('+') {
            adds += 1;
        } else if line.starts_with('-') && !line.starts_with("---") {
            dels += 1;
        }
    }
    (files > 0).then(|| format!("{files} files, +{adds} -{dels}"))
}

/// EvolutionKind 的 API 表示（snake_case）。接管台按 `policy_update` 区分
/// 产物级行（只显示「审批」），不能用 Debug 小写（"policyupdate"）。
fn kind_name(kind: &crate::types::EvolutionKind) -> &'static str {
    match kind {
        crate::types::EvolutionKind::SkillRefinement => "skill_refinement",
        crate::types::EvolutionKind::HookSynthesis => "hook_synthesis",
        crate::types::EvolutionKind::ToolVariant => "tool_variant",
        crate::types::EvolutionKind::CodeChange => "code_change",
        crate::types::EvolutionKind::PolicyUpdate => "policy_update",
    }
}

impl EvolutionAdminService {
    pub fn new(
        engine: Arc<crate::ReflectionEngine>,
        pipeline: crate::ChangePipeline,
        deployer: crate::EvolutionDeployer,
        binary_switcher: Option<Arc<dyn cog_core::BinarySwitcher>>,
        evolution_metrics: Option<Arc<dyn cog_core::EvolutionMetrics>>,
    ) -> Self {
        Self {
            engine,
            pipeline: pipeline.with_auto_apply(true),
            deployer,
            binary_switcher,
            evolution_metrics,
            audit_stream: None,
            image_rollout: None,
            artifact_evolution: None,
            policy_results: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            stream: None,
            switch: None,
            promoter: None,
            promotion_ledger: None,
            promotion_config_enabled: false,
            trend_latest: None,
            workspaces: None,
            queue: None,
        }
    }

    /// 接入变更队列读数：本进程读的是哪个队列目录、是不是它的属主、按什么周期
    /// 消费。接入后列举会把这条出处一并交出去——空列表在两种情形下长得一样，
    /// 而它们要做的事相反。
    pub fn with_change_queue(
        mut self,
        queue: Arc<crate::evolution_queue_readings::EvolutionQueueReadings>,
    ) -> Self {
        self.queue = Some(queue);
        self
    }

    /// 接入工作区分配器：admin 操作在临时工作树里跑。
    pub fn with_workspaces(mut self, workspaces: Arc<crate::workspace::WorkspaceManager>) -> Self {
        self.workspaces = Some(workspaces);
        self
    }

    /// 接入变更行状态广播通道（接管台 SSE 推送）。
    pub fn with_evolution_stream(
        mut self,
        tx: tokio::sync::broadcast::Sender<EvolutionChangeInfo>,
    ) -> Self {
        self.stream = Some(tx);
        self
    }

    /// 接入产物级进化引擎（§14.3）：启用 `evaluate_policy` 契约方法，
    /// 策略升级提议走「z-test → AwaitingReview → 审批热替换」人工门。
    pub fn with_artifact_evolution(mut self, evo: Arc<crate::ArtifactEvolution>) -> Self {
        self.artifact_evolution = Some(evo);
        self
    }

    /// 审计 3.2：接入 image-based 滚动更新部署器。接入后 deploy 走
    /// 「构建镜像 → change Deployment → 滚动更新」，不再走二进制替换。
    pub fn with_image_rollout(mut self, rollout: Arc<crate::ImageRollout>) -> Self {
        self.image_rollout = Some(rollout);
        self
    }

    /// 接入不可篡改审计流（审计 3.5）：change 操作写入哈希链。
    pub fn with_audit_stream(mut self, stream: Arc<dyn cog_core::AuditStream>) -> Self {
        self.audit_stream = Some(stream);
        self
    }

    /// 接入自动晋级运行时状态：开关（与 AutoPromoter 共享）、台账与配置
    /// 文件总开关快照。接入后启用 promotion_switch / set_promotion_paused /
    /// list_promotions 契约方法。
    pub fn with_promotion_state(
        mut self,
        switch: Arc<crate::PromotionSwitch>,
        ledger: Arc<dyn cog_core::PromotionLedger>,
        config_enabled: bool,
    ) -> Self {
        self.switch = Some(switch);
        self.promotion_ledger = Some(ledger);
        self.promotion_config_enabled = config_enabled;
        self
    }

    /// 接入晋级器本体（与自动通道共享同一实例）。
    ///
    /// 接入后，人工批准的落点取决于台账：一条变更若是被晋级器判成「需人工
    /// 审批」，审批台上的批准就走晋级通道——那一格没有别的出口；否则照旧走
    /// 本地构建与切换。
    pub fn with_promoter(mut self, promoter: Arc<crate::AutoPromoter>) -> Self {
        self.promoter = Some(promoter);
        self
    }

    /// 接入最新晋级周报句柄（周期报表器写入侧）。接入后启用
    /// promotion_trend 契约方法。
    pub fn with_trend_latest(
        mut self,
        latest: Arc<tokio::sync::RwLock<Option<cog_core::PromotionTrendReport>>>,
    ) -> Self {
        self.trend_latest = Some(latest);
        self
    }

    async fn audit(&self, change_id: &str, action: &str, detail: serde_json::Value) {
        if let Some(ref stream) = self.audit_stream {
            if let Err(e) = stream
                .append(
                    cog_core::AuditKind::ChangeOperation,
                    "evolution-admin",
                    change_id,
                    action,
                    detail,
                )
                .await
            {
                warn!(error = %e, action = %action, "audit append failed");
            }
        }
    }

    /// 登记策略提议结果：优先写入 source-level EvolutionEngine（保持单一
    /// 结果源）；其未装配（无 LLM 的 in-memory 部署）时写本地存储。
    async fn store_policy_result(&self, result: crate::types::EvolutionResult) {
        if let Some(ref evo) = self.engine.evolution {
            evo.register_result(result).await;
        } else {
            self.policy_results
                .write()
                .await
                .insert(result.artifact_id.clone(), result);
        }
    }

    async fn get_policy_result(&self, change_id: &str) -> Option<crate::types::EvolutionResult> {
        if let Some(ref evo) = self.engine.evolution {
            evo.get_result(change_id).await
        } else {
            self.policy_results.read().await.get(change_id).cloned()
        }
    }

    async fn set_policy_status(&self, change_id: &str, status: crate::types::EvolutionStatus) {
        if let Some(ref evo) = self.engine.evolution {
            evo.update_status(change_id, status).await;
        } else if let Some(r) = self.policy_results.write().await.get_mut(change_id) {
            r.status = status;
        }
    }

    async fn all_change_results(&self) -> Vec<crate::types::EvolutionResult> {
        let mut results = if let Some(ref evo) = self.engine.evolution {
            evo.list_results().await
        } else {
            self.policy_results.read().await.values().cloned().collect()
        };

        // The engine's result map is process-private: it is empty in a process
        // that does not generate changes, and it starts empty after a restart
        // even though the change directory still holds the queue the pipeline
        // will work through. Listing only the map therefore reports a narrower
        // set than the pipeline acts on. Fold in the directory scan — the same
        // source `find_pending_change` consults — so both surfaces judge
        // "pending" by one predicate. Entries the engine already knows keep
        // their richer record; the scan only contributes what it alone has.
        //
        // The scan does not depend on an engine being attached: the two answer
        // different questions. The engine knows each change's status, the
        // directory knows which changes exist, and `pending_changes` takes the
        // engine as an optional refinement for exactly that reason. Gating the
        // scan on it would blind the listing in the one process where the
        // directory is the only source left.
        match self
            .pipeline
            .pending_changes(self.engine.evolution.as_deref())
            .await
        {
            Ok(pending) => {
                let known: std::collections::HashSet<String> =
                    results.iter().map(|r| r.artifact_id.clone()).collect();
                for change in pending {
                    if !known.contains(&change.artifact_id) {
                        results.push(change);
                    }
                }
            }
            Err(e) => warn!(
                error = %e,
                "could not read the change directory for the listing; \
                 showing only in-memory results"
            ),
        }

        // The durable records, which are what a change leaves behind when it is
        // retired. A retired change is out of the queue, so the scan above does
        // not return it, and its resident entry is dropped once its artifact is
        // retired -- this pass is what keeps it in the listing after that. The
        // two sources above win where they overlap: they carry the richer
        // record (the scan reads the diff text the listing summarizes, the
        // engine knows the live status), and this one states metadata only.
        let records = self.pipeline.known_change_records().await;
        let known: std::collections::HashSet<String> =
            results.iter().map(|r| r.artifact_id.clone()).collect();
        for record in records {
            if !known.contains(&record.artifact_id) {
                results.push(record);
            }
        }

        results
    }

    /// 将内部结果映射为 API 行（list_changes 与 SSE 推送共用同一映射）。
    async fn row_for(&self, r: crate::types::EvolutionResult) -> EvolutionChangeInfo {
        let diff_summary = if matches!(r.kind, crate::types::EvolutionKind::PolicyUpdate) {
            // 产物级：展示版本跃迁（active 版 → 候选版）
            let name = r.artifact_id.strip_prefix("policy:").unwrap_or("");
            match self.artifact_evolution.as_ref() {
                Some(evo) => match evo.store().load_active(name).await.ok().flatten() {
                    Some(active) => Some(format!("v{} → v{}", active.version, active.version + 1)),
                    None => Some("genesis → v1".to_string()),
                },
                None => None,
            }
        } else {
            // The resident index keeps no artifact text, so a change that has
            // already left the pending queue arrives here with `content` empty
            // and its diff is read back from where the pipeline retired it. A
            // pending change still carries its content from `pending_changes`,
            // and reading that back would be a second read of the same file.
            //
            // Only a code change has a diff on disk; the other kinds are read
            // here too (the index serves them all and clears each one's text)
            // but nothing wrote them to the change directory, so asking for a
            // file that cannot exist would be a stat per row per listing, and
            // it would build a path out of an id that never was a file name.
            let content = if !r.content.is_empty() {
                Some(r.content.clone())
            } else if matches!(r.kind, crate::types::EvolutionKind::CodeChange) {
                self.pipeline.read_change_content(&r.artifact_id).await
            } else {
                None
            };
            content.as_deref().and_then(summarize_diff)
        };
        EvolutionChangeInfo {
            id: r.artifact_id,
            kind: kind_name(&r.kind).to_string(),
            description: r.description,
            status: format!("{:?}", r.status).to_lowercase(),
            created_at: r.created_at,
            diff_summary,
            eval_summary: r.eval_summary.map(|report| report.summary),
        }
    }

    /// 向接管台广播行变更；无订阅者或无通道时静默丢弃。
    fn emit(&self, row: EvolutionChangeInfo) {
        if let Some(ref tx) = self.stream {
            let _ = tx.send(row);
        }
    }

    async fn find_pending_change(
        &self,
        change_id: &str,
    ) -> SFResult<crate::types::EvolutionResult> {
        let evo_engine =
            self.engine.evolution.as_ref().ok_or_else(|| {
                SFError::Validation("self-evolution engine not configured".into())
            })?;

        let changes = self.pipeline.pending_changes(Some(evo_engine)).await?;
        changes
            .into_iter()
            .find(|p| p.artifact_id == change_id)
            .ok_or_else(|| SFError::Validation(format!("change {} not found", change_id)))
    }

    async fn record_event(&self, failed: bool) {
        if let Some(ref m) = self.evolution_metrics {
            m.record_event(failed).await;
        }
    }

    async fn record_change_applied(&self) {
        if let Some(ref m) = self.evolution_metrics {
            m.record_change_applied().await;
        }
    }

    async fn record_change_failed(&self) {
        if let Some(ref m) = self.evolution_metrics {
            m.record_change_failed().await;
        }
    }

    async fn record_change_rejected(&self, cause: cog_core::RejectionCause) {
        if let Some(ref m) = self.evolution_metrics {
            m.record_change_rejected(cause).await;
        }
    }

    async fn record_change_reformatted(&self) {
        if let Some(ref m) = self.evolution_metrics {
            m.record_change_reformatted().await;
        }
    }

    /// 取一棵 admin 临时工作树；未接分配器时返回 None（调用方用进程工作目录）。
    /// 操作结束必须 [`EvolutionAdminService::release_admin_workspace`] 归还。
    async fn acquire_admin_workspace(&self) -> SFResult<Option<crate::workspace::Workspace>> {
        let Some(mgr) = self.workspaces.as_ref() else {
            return Ok(None);
        };
        let base = crate::workspace::BaseRef::Branch("main".into());
        Ok(Some(mgr.acquire_ephemeral("admin", base).await?))
    }

    fn admin_workdir(&self, ws: Option<&crate::workspace::Workspace>) -> std::path::PathBuf {
        match ws {
            Some(ws) => ws.path.clone(),
            None => self.pipeline.project_root().to_path_buf(),
        }
    }

    async fn release_admin_workspace(&self, ws: Option<&crate::workspace::Workspace>) {
        if let (Some(mgr), Some(ws)) = (self.workspaces.as_ref(), ws) {
            if let Err(e) = mgr.release(ws).await {
                warn!(error = %e, "admin workspace release failed");
            }
        }
    }

    /// Shared commit/build/switch flow used by both `deploy_change` and
    /// `approve_change`. Ensures the change is applied and tests pass first.
    async fn deploy_inner(&self, change_id: &str) -> SFResult<EvolutionDeployResponse> {
        // 应用与提交必须落在同一棵树：apply 把变更留在工作区里，deployer 紧接着
        // 提交它，中途换树就提交不到任何东西。
        let ws = self.acquire_admin_workspace().await?;
        let workdir = self.admin_workdir(ws.as_ref());
        let outcome = self.deploy_in_workdir(change_id, &workdir).await;
        self.release_admin_workspace(ws.as_ref()).await;
        outcome
    }

    async fn deploy_in_workdir(
        &self,
        change_id: &str,
        workdir: &std::path::Path,
    ) -> SFResult<EvolutionDeployResponse> {
        let apply_result = self.apply_in(change_id, workdir).await?;
        if !apply_result.test_passed {
            return Err(SFError::Validation(format!(
                "change {} did not pass tests; cannot deploy",
                change_id
            )));
        }

        // The admin path holds a change id and a workspace and nothing that
        // says which entry point the change came from, so its build is recorded
        // as unattributed rather than under a kind resolved from somewhere else.
        let artifact = self
            .deployer
            .commit_and_build_in(change_id, workdir, None)
            .await?;
        info!(
            change_id = %artifact.change_id,
            commit = %artifact.commit_hash,
            "Admin-deployed change committed and built"
        );

        let mut switched = false;
        let mut image_tag: Option<String> = None;
        if let Some(ref rollout) = self.image_rollout {
            // 审计 3.2：image-based 滚动更新路径（失败时 ImageRollout 内部已 undo）。
            match rollout.deploy(&artifact).await {
                Ok(tag) => {
                    info!(change_id = %artifact.change_id, tag = %tag, "Admin-deploy rolled out new image");
                    image_tag = Some(tag);
                    switched = true;
                }
                Err(e) => {
                    warn!(error = %e, "image rollout deploy failed");
                    self.record_event(true).await;
                    self.record_change_failed().await;
                    return Err(e);
                }
            }
        } else if let Some(ref switcher) = self.binary_switcher {
            switcher.stage_new_binary(&artifact.new_binary_path).await?;
            info!(change_id = %artifact.change_id, "Admin-deploy staged new binary");

            if let Err(e) = switcher.switch_and_restart().await {
                warn!(error = %e, "Admin switch failed; attempting rollback");
                if let Err(rb_e) = switcher.rollback().await {
                    warn!(error = %rb_e, "Admin rollback failed");
                }
                self.record_event(true).await;
                self.record_change_failed().await;
                return Err(e);
            }
            switched = true;
        }

        self.record_change_applied().await;
        self.record_event(false).await;
        self.audit(
            change_id,
            "change.deploy",
            serde_json::json!({
                "commit_hash": artifact.commit_hash,
                "switched": switched,
                "image_tag": image_tag,
            }),
        )
        .await;

        Ok(EvolutionDeployResponse {
            change_id: artifact.change_id,
            commit_hash: artifact.commit_hash,
            staged_binary_path: artifact.new_binary_path.to_string_lossy().to_string(),
            switched,
        })
    }

    /// 应用并测试变更的公共体（`apply_change` 与 `deploy_in_workdir` 共用）。
    async fn apply_in(
        &self,
        change_id: &str,
        workdir: &std::path::Path,
    ) -> SFResult<EvolutionApplyResponse> {
        let change = self.find_pending_change(change_id).await?;
        let result = self.pipeline.apply_and_test_in(&change, workdir).await?;

        if let Some(ref evo) = self.engine.evolution {
            evo.update_status(change_id, result.new_status).await;
        }

        if result.reformatted {
            // Conformed before it was judged, which is a fact about the
            // generator rather than about this change: the change passed, and
            // only this count keeps a generator that never writes the
            // formatter's spelling from reading like one that always does.
            self.record_change_reformatted().await;
        }

        if let Some(cause) = result.verdict.cause() {
            self.record_event(true).await;
            self.record_change_failed().await;
            self.record_change_rejected(cause).await;
        } else {
            self.record_event(false).await;
        }
        self.audit(
            change_id,
            "change.apply",
            serde_json::json!({
                "test_passed": result.verdict.passed(),
                "rejection_cause": result.verdict.cause().map(|c| c.as_str()),
                "new_status": format!("{:?}", result.new_status).to_lowercase(),
                "reformatted": result.reformatted,
            }),
        )
        .await;

        Ok(EvolutionApplyResponse {
            change_id: result.change_id,
            test_passed: result.verdict.passed(),
            rejection_cause: result.verdict.cause(),
            test_output: result.test_output,
            new_status: format!("{:?}", result.new_status).to_lowercase(),
            files_changed: result
                .files_changed
                .into_iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl EvolutionAdmin for EvolutionAdminService {
    async fn list_changes(&self) -> SFResult<Vec<EvolutionChangeInfo>> {
        let results = self.all_change_results().await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(self.row_for(r).await);
        }
        Ok(out)
    }

    /// 这次列举读的是哪个队列目录、本进程是不是它的属主。列举不带这条出处时，
    /// 一个结构上看不到那些变更的进程回出的空列表就是一句无从核对的声明。
    async fn change_queue_view(&self) -> SFResult<Option<cog_core::EvolutionQueueView>> {
        Ok(self.queue.as_ref().map(|q| q.view()))
    }

    async fn evaluate_policy(
        &self,
        req: cog_core::PolicyEvalRequest,
    ) -> SFResult<EvolutionChangeInfo> {
        let artifact_evo = self
            .artifact_evolution
            .as_ref()
            .ok_or_else(|| SFError::Validation("artifact evolution not configured".into()))?;

        let reason = req.reason.clone();
        let payload = req.candidate_payload.clone();
        let proposal = artifact_evo
            .evaluate(
                &req.name,
                &req.baseline_outcomes,
                crate::PolicyCandidate {
                    payload,
                    outcomes: req.candidate_outcomes,
                    reason: req.reason,
                },
            )
            .await?;

        let status = if matches!(proposal.verdict, crate::EvalVerdict::Adopt) {
            crate::types::EvolutionStatus::AwaitingReview
        } else {
            crate::types::EvolutionStatus::Rejected
        };
        let diff_summary = match proposal.current_version {
            Some(v) => format!("v{v} → v{}", v + 1),
            None => "genesis → v1".to_string(),
        };
        let artifact_id = format!("policy:{}", req.name);
        let description = format!("Policy {} update proposal: {}", req.name, reason);
        let created_at = chrono::Utc::now();

        self.store_policy_result(crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::PolicyUpdate,
            artifact_id: artifact_id.clone(),
            description: description.clone(),
            content: serde_json::to_string_pretty(&req.candidate_payload).unwrap_or_default(),
            status,
            created_at,
            eval_summary: Some(cog_core::EvalReport {
                verdict: proposal.verdict,
                summary: proposal.eval_summary.clone(),
            }),
        })
        .await;

        // 被评估门否决（Reject/Inconclusive）记为失败进化事件——防退化叙事
        // 在 D5 指标上可见；Adopt 记成功。
        self.record_event(!matches!(proposal.verdict, crate::EvalVerdict::Adopt))
            .await;
        self.audit(
            &artifact_id,
            "policy.evaluate",
            serde_json::json!({
                "verdict": format!("{:?}", proposal.verdict),
                "z": proposal.z,
                "eval_summary": proposal.eval_summary,
            }),
        )
        .await;

        let row = EvolutionChangeInfo {
            id: artifact_id,
            kind: "policy_update".into(),
            description,
            status: format!("{:?}", status).to_lowercase(),
            created_at,
            diff_summary: Some(diff_summary),
            eval_summary: Some(proposal.eval_summary),
        };
        self.emit(row.clone());
        Ok(row)
    }

    async fn apply_change(&self, change_id: &str) -> SFResult<EvolutionApplyResponse> {
        let ws = self.acquire_admin_workspace().await?;
        let workdir = self.admin_workdir(ws.as_ref());
        let outcome = self.apply_in(change_id, &workdir).await;
        self.release_admin_workspace(ws.as_ref()).await;
        outcome
    }

    async fn deploy_change(&self, change_id: &str) -> SFResult<EvolutionDeployResponse> {
        self.deploy_inner(change_id).await
    }

    async fn approve_change(&self, change_id: &str) -> SFResult<EvolutionDeployResponse> {
        // 产物级进化：审批 = 热替换策略版本，不走二进制部署。
        if let Some(name) = change_id.strip_prefix("policy:") {
            let artifact_evo = self
                .artifact_evolution
                .as_ref()
                .ok_or_else(|| SFError::Validation("artifact evolution not configured".into()))?;
            let row = self
                .get_policy_result(change_id)
                .await
                .ok_or_else(|| SFError::Validation(format!("change {change_id} not found")))?;
            if !matches!(row.status, crate::types::EvolutionStatus::AwaitingReview) {
                return Err(SFError::Validation(format!(
                    "policy proposal {} is not awaiting review (status: {:?})",
                    change_id, row.status
                )));
            }
            info!(policy = %name, "Operator approved policy update; hot-swapping");
            let artifact = artifact_evo.approve(name).await?;
            self.set_policy_status(change_id, crate::types::EvolutionStatus::Active)
                .await;
            if let Some(updated) = self.get_policy_result(change_id).await {
                self.emit(self.row_for(updated).await);
            }
            self.record_change_applied().await;
            self.record_event(false).await;
            self.audit(
                change_id,
                "policy.approve",
                serde_json::json!({
                    "version": artifact.version,
                    "hash": artifact.hash,
                }),
            )
            .await;
            return Ok(EvolutionDeployResponse {
                change_id: change_id.to_string(),
                commit_hash: artifact.hash,
                staged_binary_path: String::new(),
                switched: true,
            });
        }

        let change = self.find_pending_change(change_id).await?;
        if !matches!(change.status, crate::types::EvolutionStatus::AwaitingReview) {
            return Err(SFError::Validation(format!(
                "change {} is not awaiting review (status: {:?}); run apply first",
                change_id, change.status
            )));
        }
        info!(change_id = %change_id, "Operator approved change; proceeding to deploy");
        self.audit(change_id, "change.approve", serde_json::json!({}))
            .await;

        // 一条变更未必是等着本地部署，也可能是卡在晋级台账「等人工审批」那一格：
        // 晋级器判它要人批才发，而这一格唯一的写者就是晋级器自己、唯一把它推
        // 出去的动作就是人工批准。批准按钮要是只走本地构建与切换，那一格就没
        // 有出口，而停摆判据读的正是它的出口（本周 promoted > 0）。
        if let Some(ref promoter) = self.promoter {
            match promoter.awaiting_approval(change_id).await {
                Ok(Some(record_id)) => {
                    info!(
                        change_id = %change_id,
                        record_id = %record_id,
                        "Operator approval matches a promotion awaiting human approval; promoting"
                    );
                    let reference = promoter.promote_approved(&change).await?;
                    if let Some(ref evo) = self.engine.evolution {
                        evo.update_status(change_id, crate::types::EvolutionStatus::Active)
                            .await;
                    }
                    self.record_change_applied().await;
                    self.record_event(false).await;
                    self.audit(
                        change_id,
                        "change.promote",
                        serde_json::json!({ "reference": reference }),
                    )
                    .await;
                    return Ok(EvolutionDeployResponse {
                        change_id: change_id.to_string(),
                        commit_hash: reference,
                        staged_binary_path: String::new(),
                        switched: true,
                    });
                }
                Ok(None) => {}
                Err(e) => warn!(
                    change_id = %change_id,
                    error = %e,
                    "could not read the promotion ledger; falling back to the local deploy path"
                ),
            }
        }

        self.deploy_inner(change_id).await
    }

    async fn rollback(&self) -> SFResult<cog_core::EvolutionRollbackResponse> {
        let switcher = self
            .binary_switcher
            .as_ref()
            .ok_or_else(|| SFError::Validation("binary switcher not configured".into()))?;

        match switcher.rollback().await {
            Ok(()) => {
                info!("Admin-triggered rollback to previous binary succeeded");
                self.record_event(false).await;
                self.audit("binary", "change.rollback", serde_json::json!({"ok": true}))
                    .await;
                Ok(cog_core::EvolutionRollbackResponse {
                    rolled_back: true,
                    message: "rolled back to previous binary".into(),
                })
            }
            Err(e) => {
                warn!(error = %e, "Admin-triggered rollback failed");
                self.record_event(true).await;
                Err(e)
            }
        }
    }

    async fn list_events(&self, limit: usize) -> SFResult<Vec<cog_core::EvolutionEventInfo>> {
        let results = self.all_change_results().await;
        Ok(results
            .into_iter()
            .take(limit)
            .map(|r| cog_core::EvolutionEventInfo {
                id: r.artifact_id,
                kind: kind_name(&r.kind).to_string(),
                description: r.description,
                status: format!("{:?}", r.status).to_lowercase(),
                created_at: r.created_at,
            })
            .collect())
    }

    async fn promotion_switch(&self) -> SFResult<cog_core::PromotionSwitchInfo> {
        let Some(ref switch) = self.switch else {
            return Err(SFError::NotImplemented("promotion switch".into()));
        };
        Ok(switch.snapshot(self.promotion_config_enabled))
    }

    async fn set_promotion_paused(
        &self,
        paused: bool,
        note: &str,
    ) -> SFResult<cog_core::PromotionSwitchInfo> {
        let Some(ref switch) = self.switch else {
            return Err(SFError::NotImplemented("promotion switch".into()));
        };
        switch.set_paused(paused, note);
        let snapshot = switch.snapshot(self.promotion_config_enabled);
        // 谁在什么时间暂停/恢复了自动晋级，必须进审计（运行时状态进程
        // 重启即丢，审计是唯一的持久留痕）。
        self.audit(
            "promotion-switch",
            if paused { "pause" } else { "resume" },
            serde_json::json!({ "note": note, "effective_enabled": snapshot.effective_enabled }),
        )
        .await;
        info!(paused = paused, note = %note, "Promotion switch toggled via admin API");
        Ok(snapshot)
    }

    async fn list_promotions(&self, limit: usize) -> SFResult<Vec<cog_core::PromotionRecord>> {
        let Some(ref ledger) = self.promotion_ledger else {
            return Err(SFError::NotImplemented("promotion list".into()));
        };
        ledger.list(limit).await
    }

    async fn promotion_trend(&self) -> SFResult<cog_core::PromotionTrendReport> {
        let Some(ref latest) = self.trend_latest else {
            return Err(SFError::NotImplemented("promotion trend".into()));
        };
        // 报表器尚未产出第一期时返回空报告，admin 端点永远可用。
        Ok(latest
            .read()
            .await
            .clone()
            .unwrap_or(cog_core::PromotionTrendReport {
                generated_at: chrono::Utc::now(),
                weeks: Vec::new(),
                alert: None,
                stall_alert: None,
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::PromotionLedger;

    struct PlaceholderLlm;

    #[async_trait::async_trait]
    impl cog_core::LlmClient for PlaceholderLlm {
        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            Ok(cog_core::ChatResponse {
                content: vec![cog_core::ContentBlock::text("{}")],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!()
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    #[test]
    fn only_a_unified_diff_yields_a_summary() {
        // Pins the property that dropping `content` from the resident index
        // leans on. The summary was derived from the artifact text only when
        // that text was a unified diff; a hook, tool or skill result carries
        // the generator's JSON, and this returned None for it before the text
        // stopped being kept. So emptying those entries cannot take a summary
        // away from them -- and if this ever stops being true for the JSON
        // shape, the drop would become a display regression and this test says
        // so rather than the admin table quietly losing a column.
        assert_eq!(
            summarize_diff(r#"{"name":"tool","description":"a tool"}"#),
            None
        );
        assert_eq!(
            summarize_diff("--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-old\n+new\n"),
            Some("1 files, +1 -1".to_string())
        );
    }

    #[tokio::test]
    async fn admin_service_lists_changes_and_rejects_missing_apply() {
        let registry = Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()));
        let mut engine = crate::ReflectionEngine::new_in_memory(registry.clone());
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(PlaceholderLlm);
        engine.evolution = Some(Arc::new(crate::EvolutionEngine::new(
            llm,
            registry.clone(),
            None,
        )));

        let project_root = std::env::current_dir().unwrap();
        let change_dir =
            std::env::temp_dir().join(format!("cogneva-test-changes-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let binary_dir =
            std::env::temp_dir().join(format!("cogneva-test-bin-{}", uuid::Uuid::new_v4()));
        let backup_dir =
            std::env::temp_dir().join(format!("cogneva-test-backup-{}", uuid::Uuid::new_v4()));

        let pipeline = crate::ChangePipeline::new(&project_root, &change_dir, false)
            .with_auto_apply(true)
            .with_test_timeout(30);
        let deployer = crate::EvolutionDeployer::new(&project_root, &binary_dir, &backup_dir);

        let admin =
            crate::EvolutionAdminService::new(Arc::new(engine), pipeline, deployer, None, None);

        let changes = admin.list_changes().await.unwrap();
        assert!(changes.is_empty());

        let err = admin.apply_change("missing-change").await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    /// The engine's result map is process-private: a change generated by a
    /// previous process is gone from it but is still sitting in the change
    /// directory, where the pipeline will pick it up. The listing has to judge
    /// "pending" the same way the pipeline does, or it reports an empty queue
    /// while the pipeline still has work.
    #[tokio::test]
    async fn admin_service_lists_changes_that_only_the_directory_knows() {
        let registry = Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()));
        let mut engine = crate::ReflectionEngine::new_in_memory(registry.clone());
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(PlaceholderLlm);
        engine.evolution = Some(Arc::new(crate::EvolutionEngine::new(
            llm,
            registry.clone(),
            None,
        )));

        let project_root = std::env::current_dir().unwrap();
        let change_dir =
            std::env::temp_dir().join(format!("cogneva-test-changes-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        tokio::fs::write(
            change_dir.join("from-a-previous-process.diff"),
            "--- a/x.txt\n+++ b/x.txt\n@@ -0,0 +1 @@\n+one\n",
        )
        .await
        .unwrap();

        let binary_dir =
            std::env::temp_dir().join(format!("cogneva-test-bin-{}", uuid::Uuid::new_v4()));
        let backup_dir =
            std::env::temp_dir().join(format!("cogneva-test-backup-{}", uuid::Uuid::new_v4()));
        let pipeline = crate::ChangePipeline::new(&project_root, &change_dir, false);
        let deployer = crate::EvolutionDeployer::new(&project_root, &binary_dir, &backup_dir);
        let admin =
            crate::EvolutionAdminService::new(Arc::new(engine), pipeline, deployer, None, None);

        let changes = admin.list_changes().await.unwrap();
        let ids: Vec<&str> = changes.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["from-a-previous-process"]);

        let events = admin.list_events(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "from-a-previous-process");
    }

    /// 退休把变更挪出队列，也把常驻那一格丢掉；listing 若只读这两处，就会在变更
    /// 刚落地的下一刻回答"没有这个变更"。它读的第三处是产物旁边那份记录——那是
    /// 唯一在产物离开队列之后还留着的东西。
    #[tokio::test]
    async fn admin_service_lists_a_change_after_its_resident_entry_is_dropped() {
        let registry = Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()));
        let mut engine = crate::ReflectionEngine::new_in_memory(registry.clone());
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(PlaceholderLlm);

        let project_root = std::env::current_dir().unwrap();
        let unique = uuid::Uuid::new_v4();
        let change_dir = std::env::temp_dir().join(format!("cogneva-test-changes-{}", unique));
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let evo = Arc::new(
            crate::EvolutionEngine::new(llm, registry.clone(), None).with_change_dir(&change_dir),
        );
        engine.evolution = Some(evo.clone());

        let id = "landed-change";
        let record = crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: id.to_string(),
            description: "goal landed".to_string(),
            content: String::new(),
            status: crate::types::EvolutionStatus::AwaitingReview,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };
        tokio::fs::write(
            change_dir.join(format!("{id}.diff")),
            "--- a/x.txt\n+++ b/x.txt\n@@ -0,0 +1 @@\n+one\n",
        )
        .await
        .unwrap();
        let pipeline = crate::ChangePipeline::new(&project_root, &change_dir, false);
        pipeline.write_change_record(&record).await.unwrap();
        evo.register_result(record).await;

        pipeline.retire_change(id, "landed").await.unwrap();
        assert!(
            evo.retire_result(&pipeline, id).await,
            "the change has to be retired and recorded before the index may drop it"
        );
        assert!(evo.get_result(id).await.is_none(), "the index let it go");

        let deployer = crate::EvolutionDeployer::new(
            &project_root,
            std::env::temp_dir().join(format!("cogneva-test-bin-{}", unique)),
            std::env::temp_dir().join(format!("cogneva-test-backup-{}", unique)),
        );
        let admin =
            crate::EvolutionAdminService::new(Arc::new(engine), pipeline, deployer, None, None);

        let ids: Vec<String> = admin
            .list_changes()
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(
            ids,
            vec![id.to_string()],
            "a change that just landed is the one thing a listing must not lose"
        );
    }

    /// The directory is the queue, and the engine is only an enrichment of it: a
    /// process with no LLM has no engine attached, and it still has a queue to
    /// list. Gating the scan on the engine would make the listing go blind in
    /// exactly the process where the directory is the only source left.
    #[tokio::test]
    async fn admin_service_lists_the_queue_with_no_engine_attached() {
        let registry = Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()));
        let engine = crate::ReflectionEngine::new_in_memory(registry);
        assert!(engine.evolution.is_none(), "this test is about that case");

        let project_root = std::env::current_dir().unwrap();
        let change_dir =
            std::env::temp_dir().join(format!("cogneva-test-changes-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        tokio::fs::write(
            change_dir.join("nobody-is-running-me.diff"),
            "--- a/x.txt\n+++ b/x.txt\n@@ -0,0 +1 @@\n+one\n",
        )
        .await
        .unwrap();
        let binary_dir =
            std::env::temp_dir().join(format!("cogneva-test-bin-{}", uuid::Uuid::new_v4()));
        let backup_dir =
            std::env::temp_dir().join(format!("cogneva-test-backup-{}", uuid::Uuid::new_v4()));

        let pipeline = crate::ChangePipeline::new(&project_root, &change_dir, false);
        let deployer = crate::EvolutionDeployer::new(&project_root, &binary_dir, &backup_dir);
        let queue = Arc::new(
            crate::evolution_queue_readings::EvolutionQueueReadings::new(
                &change_dir,
                false,
                cog_core::config::SelfEvolutionConfig::default().poll_interval_secs,
                pipeline.clone(),
                None,
            ),
        );
        let admin =
            crate::EvolutionAdminService::new(Arc::new(engine), pipeline, deployer, None, None)
                .with_change_queue(queue);

        let ids: Vec<String> = admin
            .list_changes()
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(ids, vec!["nobody-is-running-me"]);

        // The listing says which queue it read and that this process is not the
        // one draining it: an empty list and a list from a process that cannot
        // see the queue have to be told apart by their reader.
        let view = admin.change_queue_view().await.unwrap().unwrap();
        assert!(!view.owner);
        assert_eq!(view.dir, change_dir.display().to_string());
    }

    /// Without the queue wired in, the listing reports no provenance rather than
    /// claiming the queue it read is the one that matters.
    #[tokio::test]
    async fn a_listing_with_no_queue_wired_reports_no_provenance() {
        let admin = build_admin(None);
        assert!(admin.change_queue_view().await.unwrap().is_none());
    }

    struct MockSwitcher {
        rolled_back: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait::async_trait]
    impl cog_core::BinarySwitcher for MockSwitcher {
        async fn stage_new_binary(&self, _new_binary_path: &std::path::Path) -> SFResult<()> {
            Ok(())
        }

        async fn switch_and_restart(&self) -> SFResult<()> {
            Ok(())
        }

        async fn rollback(&self) -> SFResult<()> {
            self.rolled_back
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    fn build_admin(
        switcher: Option<Arc<dyn cog_core::BinarySwitcher>>,
    ) -> crate::EvolutionAdminService {
        let registry = Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()));
        let mut engine = crate::ReflectionEngine::new_in_memory(registry.clone());
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(PlaceholderLlm);
        engine.evolution = Some(Arc::new(crate::EvolutionEngine::new(
            llm,
            registry.clone(),
            None,
        )));

        let project_root = std::env::current_dir().unwrap();
        let unique = uuid::Uuid::new_v4();
        let change_dir = std::env::temp_dir().join(format!("cogneva-test-changes-{}", unique));
        let binary_dir = std::env::temp_dir().join(format!("cogneva-test-bin-{}", unique));
        let backup_dir = std::env::temp_dir().join(format!("cogneva-test-backup-{}", unique));

        let pipeline = crate::ChangePipeline::new(&project_root, &change_dir, false);
        let deployer = crate::EvolutionDeployer::new(&project_root, &binary_dir, &backup_dir);

        crate::EvolutionAdminService::new(Arc::new(engine), pipeline, deployer, switcher, None)
    }

    #[tokio::test]
    async fn admin_rollback_requires_switcher_and_invokes_it() {
        // Without a switcher the rollback must fail with a clear error.
        let admin = build_admin(None);
        let err = admin.rollback().await.unwrap_err();
        assert!(err.to_string().contains("binary switcher not configured"));

        // With a switcher the rollback goes through.
        let rolled_back = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let admin = build_admin(Some(Arc::new(MockSwitcher {
            rolled_back: rolled_back.clone(),
        })));
        let resp = admin.rollback().await.unwrap();
        assert!(resp.rolled_back);
        assert!(rolled_back.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn admin_list_events_returns_empty_when_no_artifacts() {
        let admin = build_admin(None);
        let events = admin.list_events(10).await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn approve_rejects_missing_and_not_awaiting_review_changes() {
        let registry = Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()));
        let mut engine = crate::ReflectionEngine::new_in_memory(registry.clone());
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(PlaceholderLlm);
        engine.evolution = Some(Arc::new(crate::EvolutionEngine::new(
            llm,
            registry.clone(),
            None,
        )));

        let project_root = std::env::current_dir().unwrap();
        let unique = uuid::Uuid::new_v4();
        let change_dir = std::env::temp_dir().join(format!("cogneva-test-changes-{}", unique));
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        // Seed a pending change file; neither the engine nor the directory has a
        // status for it, so it surfaces as Unrecorded, not AwaitingReview.
        tokio::fs::write(
            change_dir.join("p1.diff"),
            "diff --git a/crates/x/src/lib.rs b/crates/x/src/lib.rs\n--- a/crates/x/src/lib.rs\n+++ b/crates/x/src/lib.rs\n@@ -1 +1 @@\n-a\n+b\n",
        )
        .await
        .unwrap();

        let pipeline = crate::ChangePipeline::new(&project_root, &change_dir, false);
        let deployer = crate::EvolutionDeployer::new(
            &project_root,
            std::env::temp_dir().join(format!("cogneva-test-bin-{}", unique)),
            std::env::temp_dir().join(format!("cogneva-test-backup-{}", unique)),
        );
        let admin =
            crate::EvolutionAdminService::new(Arc::new(engine), pipeline, deployer, None, None);

        let err = admin.approve_change("missing").await.unwrap_err();
        assert!(err.to_string().contains("not found"), "got {err}");

        let err = admin.approve_change("p1").await.unwrap_err();
        assert!(err.to_string().contains("not awaiting review"), "got {err}");
    }

    /// 记下发布过哪些变更的假推送端。
    #[derive(Default)]
    struct RecordingChannel {
        published: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crate::auto_promoter::PromotionChannel for RecordingChannel {
        async fn publish_config(&self, change: &crate::types::EvolutionResult) -> SFResult<String> {
            self.published
                .lock()
                .unwrap()
                .push(change.artifact_id.clone());
            Ok(format!("commit-{}", change.artifact_id))
        }
        async fn publish_rollout(
            &self,
            change: &crate::types::EvolutionResult,
        ) -> SFResult<String> {
            self.publish_config(change).await
        }
    }

    /// 批准一条被判成「需人工审批」的变更，落点必须是晋级台账那一行。
    ///
    /// 那一格只有晋级器一个写者，而把它推出去的动作只有人工批准——批准按钮
    /// 要是照旧走本地构建与切换，这一格就永远停在等审批，停摆判据读的正是它
    /// 的出口（本周 promoted > 0），于是那支 critical 告警按构造清不掉。
    #[tokio::test]
    async fn approving_a_change_that_awaits_approval_promotes_it_instead_of_deploying() {
        let registry = Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()));
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(PlaceholderLlm);
        let evo = Arc::new(crate::EvolutionEngine::new(llm, registry.clone(), None));
        let mut reflection = crate::ReflectionEngine::new_in_memory(registry.clone());
        reflection.evolution = Some(evo.clone());
        let reflection = Arc::new(reflection);

        // 核心路径：分级判它必须人工审批。
        let diff = "diff --git a/crates/cog-storage/src/postgres/state_backend.rs b/crates/cog-storage/src/postgres/state_backend.rs\n--- a/crates/cog-storage/src/postgres/state_backend.rs\n+++ b/crates/cog-storage/src/postgres/state_backend.rs\n@@ -1 +1 @@\n-a\n+b\n";
        let change = crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: "awaits".into(),
            description: "test".into(),
            content: diff.into(),
            status: crate::types::EvolutionStatus::AwaitingReview,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let unique = uuid::Uuid::new_v4();
        let change_dir = std::env::temp_dir().join(format!("cogneva-test-changes-{unique}"));
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        tokio::fs::write(change_dir.join("awaits.diff"), diff)
            .await
            .unwrap();
        evo.register_result(change.clone()).await;

        // 那一行由真实生产者写出来。手搭一行只能证明形状对，证明不了这条路
        // 真的会在跑起来时落到那一格。
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let channel = Arc::new(RecordingChannel::default());
        let promoter = Arc::new(crate::AutoPromoter::new(
            crate::PromotionGateConfig {
                enabled: true,
                ..Default::default()
            },
            ledger.clone(),
            Some(channel.clone()),
            reflection.clone(),
        ));
        promoter.decide_and_promote(&change).await.unwrap();
        let seeded = ledger.recent(10).await.unwrap();
        assert_eq!(seeded.len(), 1);
        assert_eq!(
            seeded[0].status,
            cog_core::PromotionStatus::AwaitingApproval
        );

        // 工作树指向临时目录：接线若断了，这条测试会走到本地构建那条路上，
        // 那也必须落在临时目录里而不是真的仓库里。
        let scratch = tempfile::tempdir().unwrap();
        let pipeline = crate::ChangePipeline::new(scratch.path(), &change_dir, false);
        let deployer = crate::EvolutionDeployer::new(
            scratch.path(),
            scratch.path().join("bin"),
            scratch.path().join("backup"),
        );
        let admin =
            crate::EvolutionAdminService::new(reflection.clone(), pipeline, deployer, None, None)
                .with_promoter(promoter.clone());

        let resp = admin.approve_change("awaits").await.unwrap();

        assert_eq!(resp.commit_hash, "commit-awaits");
        assert_eq!(
            resp.staged_binary_path, "",
            "走的是晋级通道，没有本地构建产物"
        );
        assert_eq!(channel.published.lock().unwrap().as_slice(), ["awaits"]);

        let records = ledger.recent(10).await.unwrap();
        assert_eq!(records.len(), 1, "批准更新那一行，不该再追加一行");
        assert_eq!(records[0].id, seeded[0].id);
        assert_eq!(records[0].status, cog_core::PromotionStatus::Promoted);
        assert_eq!(
            promoter.awaiting_approval("awaits").await.unwrap(),
            None,
            "批准之后那一格必须已经被推出去"
        );
    }
}
