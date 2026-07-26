//! Admin-facing evolution service — exposes manual control over the
//! self-evolution pipeline to the Gateway.

use std::sync::Arc;

use cog_core::{
    EvolutionAdmin, EvolutionApplyResponse, EvolutionDeployResponse, EvolutionPatchInfo, SFError,
    SFResult,
};
use tracing::{info, warn};

/// Service that implements [`EvolutionAdmin`] by delegating to the reflection
/// engine, patch pipeline, and deployer.
pub struct EvolutionAdminService {
    engine: Arc<crate::ReflectionEngine>,
    pipeline: crate::PatchPipeline,
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
    /// 补丁行变更广播：接管台 SSE 订阅此通道，状态翻转即时推送而非轮询。
    stream: Option<tokio::sync::broadcast::Sender<EvolutionPatchInfo>>,
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
        crate::types::EvolutionKind::CodePatch => "code_patch",
        crate::types::EvolutionKind::PolicyUpdate => "policy_update",
    }
}

impl EvolutionAdminService {
    pub fn new(
        engine: Arc<crate::ReflectionEngine>,
        pipeline: crate::PatchPipeline,
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
        }
    }

    /// 接入补丁行变更广播通道（接管台 SSE 推送）。
    pub fn with_evolution_stream(
        mut self,
        tx: tokio::sync::broadcast::Sender<EvolutionPatchInfo>,
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
    /// 「构建镜像 → patch Deployment → 滚动更新」，不再走二进制替换。
    pub fn with_image_rollout(mut self, rollout: Arc<crate::ImageRollout>) -> Self {
        self.image_rollout = Some(rollout);
        self
    }

    /// 接入不可篡改审计流（审计 3.5）：patch 操作写入哈希链。
    pub fn with_audit_stream(mut self, stream: Arc<dyn cog_core::AuditStream>) -> Self {
        self.audit_stream = Some(stream);
        self
    }

    async fn audit(&self, patch_id: &str, action: &str, detail: serde_json::Value) {
        if let Some(ref stream) = self.audit_stream {
            if let Err(e) = stream
                .append(
                    cog_core::AuditKind::PatchOperation,
                    "evolution-admin",
                    patch_id,
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

    async fn get_policy_result(&self, patch_id: &str) -> Option<crate::types::EvolutionResult> {
        if let Some(ref evo) = self.engine.evolution {
            evo.list_results()
                .await
                .into_iter()
                .find(|r| r.artifact_id == patch_id)
        } else {
            self.policy_results.read().await.get(patch_id).cloned()
        }
    }

    async fn set_policy_status(&self, patch_id: &str, status: crate::types::EvolutionStatus) {
        if let Some(ref evo) = self.engine.evolution {
            evo.update_status(patch_id, status).await;
        } else if let Some(r) = self.policy_results.write().await.get_mut(patch_id) {
            r.status = status;
        }
    }

    async fn all_patch_results(&self) -> Vec<crate::types::EvolutionResult> {
        if let Some(ref evo) = self.engine.evolution {
            evo.list_results().await
        } else {
            self.policy_results.read().await.values().cloned().collect()
        }
    }

    /// 将内部结果映射为 API 行（list_patches 与 SSE 推送共用同一映射）。
    async fn row_for(&self, r: crate::types::EvolutionResult) -> EvolutionPatchInfo {
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
            summarize_diff(&r.content)
        };
        EvolutionPatchInfo {
            id: r.artifact_id,
            kind: kind_name(&r.kind).to_string(),
            description: r.description,
            status: format!("{:?}", r.status).to_lowercase(),
            created_at: r.created_at,
            diff_summary,
            eval_summary: r.eval_summary,
        }
    }

    /// 向接管台广播行变更；无订阅者或无通道时静默丢弃。
    fn emit(&self, row: EvolutionPatchInfo) {
        if let Some(ref tx) = self.stream {
            let _ = tx.send(row);
        }
    }

    async fn find_pending_patch(&self, patch_id: &str) -> SFResult<crate::types::EvolutionResult> {
        let evo_engine =
            self.engine.evolution.as_ref().ok_or_else(|| {
                SFError::Validation("self-evolution engine not configured".into())
            })?;

        let patches = self.pipeline.pending_patches(Some(evo_engine)).await?;
        patches
            .into_iter()
            .find(|p| p.artifact_id == patch_id)
            .ok_or_else(|| SFError::Validation(format!("patch {} not found", patch_id)))
    }

    async fn record_event(&self, failed: bool) {
        if let Some(ref m) = self.evolution_metrics {
            m.record_event(failed).await;
        }
    }

    async fn record_patch_applied(&self) {
        if let Some(ref m) = self.evolution_metrics {
            m.record_patch_applied().await;
        }
    }

    async fn record_patch_failed(&self) {
        if let Some(ref m) = self.evolution_metrics {
            m.record_patch_failed().await;
        }
    }

    /// Shared commit/build/switch flow used by both `deploy_patch` and
    /// `approve_patch`. Ensures the patch is applied and tests pass first.
    async fn deploy_inner(&self, patch_id: &str) -> SFResult<EvolutionDeployResponse> {
        let apply_result = self.apply_patch(patch_id).await?;
        if !apply_result.test_passed {
            return Err(SFError::Validation(format!(
                "patch {} did not pass tests; cannot deploy",
                patch_id
            )));
        }

        let artifact = self.deployer.commit_and_build(patch_id).await?;
        info!(
            patch_id = %artifact.patch_id,
            commit = %artifact.commit_hash,
            "Admin-deployed patch committed and built"
        );

        let mut switched = false;
        let mut image_tag: Option<String> = None;
        if let Some(ref rollout) = self.image_rollout {
            // 审计 3.2：image-based 滚动更新路径（失败时 ImageRollout 内部已 undo）。
            match rollout.deploy(&artifact).await {
                Ok(tag) => {
                    info!(patch_id = %artifact.patch_id, tag = %tag, "Admin-deploy rolled out new image");
                    image_tag = Some(tag);
                    switched = true;
                }
                Err(e) => {
                    warn!(error = %e, "image rollout deploy failed");
                    self.record_event(true).await;
                    self.record_patch_failed().await;
                    return Err(e);
                }
            }
        } else if let Some(ref switcher) = self.binary_switcher {
            switcher.stage_new_binary(&artifact.new_binary_path).await?;
            info!(patch_id = %artifact.patch_id, "Admin-deploy staged new binary");

            if let Err(e) = switcher.switch_and_restart().await {
                warn!(error = %e, "Admin switch failed; attempting rollback");
                if let Err(rb_e) = switcher.rollback().await {
                    warn!(error = %rb_e, "Admin rollback failed");
                }
                self.record_event(true).await;
                self.record_patch_failed().await;
                return Err(e);
            }
            switched = true;
        }

        self.record_patch_applied().await;
        self.record_event(false).await;
        self.audit(
            patch_id,
            "patch.deploy",
            serde_json::json!({
                "commit_hash": artifact.commit_hash,
                "switched": switched,
                "image_tag": image_tag,
            }),
        )
        .await;

        Ok(EvolutionDeployResponse {
            patch_id: artifact.patch_id,
            commit_hash: artifact.commit_hash,
            staged_binary_path: artifact.new_binary_path.to_string_lossy().to_string(),
            switched,
        })
    }
}

#[async_trait::async_trait]
impl EvolutionAdmin for EvolutionAdminService {
    async fn list_patches(&self) -> SFResult<Vec<EvolutionPatchInfo>> {
        let results = self.all_patch_results().await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(self.row_for(r).await);
        }
        Ok(out)
    }

    async fn evaluate_policy(
        &self,
        req: cog_core::PolicyEvalRequest,
    ) -> SFResult<EvolutionPatchInfo> {
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
            eval_summary: Some(proposal.eval_summary.clone()),
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

        let row = EvolutionPatchInfo {
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

    async fn apply_patch(&self, patch_id: &str) -> SFResult<EvolutionApplyResponse> {
        let patch = self.find_pending_patch(patch_id).await?;
        let result = self.pipeline.apply_and_test(&patch).await?;

        if let Some(ref evo) = self.engine.evolution {
            evo.update_status(patch_id, result.new_status).await;
        }

        let failed = !result.test_passed;
        if failed {
            self.record_event(true).await;
            self.record_patch_failed().await;
        } else {
            self.record_event(false).await;
        }
        self.audit(
            patch_id,
            "patch.apply",
            serde_json::json!({
                "test_passed": result.test_passed,
                "new_status": format!("{:?}", result.new_status).to_lowercase(),
            }),
        )
        .await;

        Ok(EvolutionApplyResponse {
            patch_id: result.patch_id,
            test_passed: result.test_passed,
            test_output: result.test_output,
            new_status: format!("{:?}", result.new_status).to_lowercase(),
            files_changed: result
                .files_changed
                .into_iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect(),
        })
    }

    async fn deploy_patch(&self, patch_id: &str) -> SFResult<EvolutionDeployResponse> {
        self.deploy_inner(patch_id).await
    }

    async fn approve_patch(&self, patch_id: &str) -> SFResult<EvolutionDeployResponse> {
        // 产物级进化：审批 = 热替换策略版本，不走二进制部署。
        if let Some(name) = patch_id.strip_prefix("policy:") {
            let artifact_evo = self
                .artifact_evolution
                .as_ref()
                .ok_or_else(|| SFError::Validation("artifact evolution not configured".into()))?;
            let row = self
                .get_policy_result(patch_id)
                .await
                .ok_or_else(|| SFError::Validation(format!("patch {patch_id} not found")))?;
            if !matches!(row.status, crate::types::EvolutionStatus::AwaitingReview) {
                return Err(SFError::Validation(format!(
                    "policy proposal {} is not awaiting review (status: {:?})",
                    patch_id, row.status
                )));
            }
            info!(policy = %name, "Operator approved policy update; hot-swapping");
            let artifact = artifact_evo.approve(name).await?;
            self.set_policy_status(patch_id, crate::types::EvolutionStatus::Active)
                .await;
            if let Some(updated) = self.get_policy_result(patch_id).await {
                self.emit(self.row_for(updated).await);
            }
            self.record_patch_applied().await;
            self.record_event(false).await;
            self.audit(
                patch_id,
                "policy.approve",
                serde_json::json!({
                    "version": artifact.version,
                    "hash": artifact.hash,
                }),
            )
            .await;
            return Ok(EvolutionDeployResponse {
                patch_id: patch_id.to_string(),
                commit_hash: artifact.hash,
                staged_binary_path: String::new(),
                switched: true,
            });
        }

        let patch = self.find_pending_patch(patch_id).await?;
        if !matches!(patch.status, crate::types::EvolutionStatus::AwaitingReview) {
            return Err(SFError::Validation(format!(
                "patch {} is not awaiting review (status: {:?}); run apply first",
                patch_id, patch.status
            )));
        }
        info!(patch_id = %patch_id, "Operator approved patch; proceeding to deploy");
        self.audit(patch_id, "patch.approve", serde_json::json!({}))
            .await;
        self.deploy_inner(patch_id).await
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
                self.audit("binary", "patch.rollback", serde_json::json!({"ok": true}))
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
        let results = self.all_patch_results().await;
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn admin_service_lists_patches_and_rejects_missing_apply() {
        let registry = Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()));
        let mut engine = crate::ReflectionEngine::new_in_memory(registry.clone());
        let llm: Arc<dyn cog_core::LlmClient> = Arc::new(PlaceholderLlm);
        engine.evolution = Some(Arc::new(crate::EvolutionEngine::new(
            llm,
            registry.clone(),
            None,
        )));

        let project_root = std::env::current_dir().unwrap();
        let patch_dir =
            std::env::temp_dir().join(format!("cogneva-test-patches-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&patch_dir).await.unwrap();
        let binary_dir =
            std::env::temp_dir().join(format!("cogneva-test-bin-{}", uuid::Uuid::new_v4()));
        let backup_dir =
            std::env::temp_dir().join(format!("cogneva-test-backup-{}", uuid::Uuid::new_v4()));

        let pipeline = crate::PatchPipeline::new(&project_root, &patch_dir, false)
            .with_auto_apply(true)
            .with_test_timeout(30);
        let deployer = crate::EvolutionDeployer::new(&project_root, &binary_dir, &backup_dir);

        let admin =
            crate::EvolutionAdminService::new(Arc::new(engine), pipeline, deployer, None, None);

        let patches = admin.list_patches().await.unwrap();
        assert!(patches.is_empty());

        let err = admin.apply_patch("missing-patch").await.unwrap_err();
        assert!(err.to_string().contains("not found"));
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
        let patch_dir = std::env::temp_dir().join(format!("cogneva-test-patches-{}", unique));
        let binary_dir = std::env::temp_dir().join(format!("cogneva-test-bin-{}", unique));
        let backup_dir = std::env::temp_dir().join(format!("cogneva-test-backup-{}", unique));

        let pipeline = crate::PatchPipeline::new(&project_root, &patch_dir, false);
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
    async fn approve_rejects_missing_and_not_awaiting_review_patches() {
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
        let patch_dir = std::env::temp_dir().join(format!("cogneva-test-patches-{}", unique));
        tokio::fs::create_dir_all(&patch_dir).await.unwrap();
        // Seed a pending patch file; the engine has no status record for it,
        // so it surfaces as CompileChecked, not AwaitingReview.
        tokio::fs::write(
            patch_dir.join("p1.patch"),
            "diff --git a/crates/x/src/lib.rs b/crates/x/src/lib.rs\n--- a/crates/x/src/lib.rs\n+++ b/crates/x/src/lib.rs\n@@ -1 +1 @@\n-a\n+b\n",
        )
        .await
        .unwrap();

        let pipeline = crate::PatchPipeline::new(&project_root, &patch_dir, false);
        let deployer = crate::EvolutionDeployer::new(
            &project_root,
            std::env::temp_dir().join(format!("cogneva-test-bin-{}", unique)),
            std::env::temp_dir().join(format!("cogneva-test-backup-{}", unique)),
        );
        let admin =
            crate::EvolutionAdminService::new(Arc::new(engine), pipeline, deployer, None, None);

        let err = admin.approve_patch("missing").await.unwrap_err();
        assert!(err.to_string().contains("not found"), "got {err}");

        let err = admin.approve_patch("p1").await.unwrap_err();
        assert!(err.to_string().contains("not awaiting review"), "got {err}");
    }
}
