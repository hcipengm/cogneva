//! 基线移植控制器（规则3：旧变更跨基线自治移植）。
//!
//! 公版发布新 release tag（`v<x.y.z>`）后，实例历代晋级变更（`promote/*`
//! tag）不能随 `reset --hard` 丢成干净上游树——它们必须移植到新基线。
//!
//! 两阶段：
//! 1. 只读探测（[`BaselinePorter::plan`]）：盘点 `promote/*` tag，用
//!    `git cherry` patch-id 做吸收检测（别的实例回流或官方自行修复的变更
//!    直接跳过），产出 [`PortPlan`]。
//! 2. 移植执行（[`BaselinePorter::execute`]）：基于新 tag 切 `evol/<id>`
//!    分支，逐变更 cherry-pick；冲突时把[新基线上下文 + 原始 diff + 冲突
//!    证据 + 变更意图]打包成主流程任务，由 agent 在同一工作树重新实现
//!    （最多 3 轮）；每条变更过 cargo check + cargo test --workspace +
//!    适用时 eval A/B（z 检验，只拦统计显著回归）三重质量门；失败的变更
//!    回退并回流为重做任务，不阻塞其他变更；收口打 `gen-n` 代际 tag。
//!
//! 移植不是合并文本，是在新基线上重新达成旧变更的意图。本模块只做确定性
//! 编排与验收：一切智能步骤（冲突重写、修复、eval 执行）都是经
//! `OrchestratorControl` 提交的主流程任务，agent 只生成候选，验收权在
//! 编译器、测试和统计检验。

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cog_core::{
    ActionPlannerMeta, ActionPlannerSource, OrchestratorControl, SFError, SFResult, Task,
    TaskStatus, TaskType,
};
use tracing::{debug, info, warn};

use crate::eval_harness::{compare as eval_compare, EvalOutcome, EvalVerdict};
use crate::gitops_puller::parse_tag_message;

/// 一条历代晋级变更（从 `promote/<change_id>` annotated tag 解析）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotedChange {
    pub change_id: String,
    pub tag: String,
    /// tag 解引用到的 commit（annotated tag 的 *objectname）。
    pub commit: String,
    pub level: Option<String>,
    pub eval_summary: Option<String>,
}

/// 吸收检测结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsorptionStatus {
    /// 新基线已包含等价变更（patch-id 命中，或该提交已是新基线祖先）：
    /// 移植时跳过，不重复制造冲突。
    Absorbed,
    /// 新基线未包含：需要移植。
    Pending,
}

/// 移植计划中的单条：一条晋级变更 + 其吸收结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortPlanItem {
    pub change: PromotedChange,
    pub status: AbsorptionStatus,
}

/// 一次基线移植的完整计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortPlan {
    /// 新上游 release tag，如 `v0.5.8`。
    pub new_tag: String,
    /// 当前运行版本对应的 tag，如 `v0.5.7`。
    pub current_tag: String,
    pub items: Vec<PortPlanItem>,
}

impl PortPlan {
    pub fn pending(&self) -> impl Iterator<Item = &PortPlanItem> {
        self.items
            .iter()
            .filter(|i| i.status == AbsorptionStatus::Pending)
    }

    pub fn absorbed(&self) -> impl Iterator<Item = &PortPlanItem> {
        self.items
            .iter()
            .filter(|i| i.status == AbsorptionStatus::Absorbed)
    }
}

/// 单条变更落地新基线的路由方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortRoute {
    /// `git cherry-pick` 干净合入且质量门一次通过。
    CherryPicked,
    /// 经主流程 agent 解决（冲突重写或编译/测试修复），第 `n` 轮通过验收。
    AgentResolved { round: u32 },
}

/// 单条变更的移植结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortOutcome {
    /// 新基线已包含等价变更，跳过。
    Absorbed,
    /// 移植成功并通过质量门。
    Ported {
        /// 收口后该变更所在的 commit。
        commit: String,
        route: PortRoute,
    },
    /// 3 轮智能解决仍未通过质量门：已回退并回流为重做任务。
    NeedsRework { reason: String },
}

/// 一条变更的移植结果条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortItemResult {
    pub change_id: String,
    pub outcome: PortOutcome,
}

/// 一次基线移植执行的收口报告。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortReport {
    pub new_tag: String,
    /// 实例零晋级历史时为 true：消费方直接 reset 到新 tag 即可，
    /// 不建 evol 分支、不打 gen tag。
    pub clean_reset: bool,
    /// 移植工作分支 `evol/<instance_id>`（clean_reset 时为 None）。
    pub branch: Option<String>,
    /// 代际 tag `gen-n`（clean_reset 时为 None）。
    pub gen_tag: Option<String>,
    pub items: Vec<PortItemResult>,
}

impl PortReport {
    pub fn ported(&self) -> impl Iterator<Item = &PortItemResult> {
        self.items
            .iter()
            .filter(|i| matches!(i.outcome, PortOutcome::Ported { .. }))
    }

    pub fn needs_rework(&self) -> impl Iterator<Item = &PortItemResult> {
        self.items
            .iter()
            .filter(|i| matches!(i.outcome, PortOutcome::NeedsRework { .. }))
    }
}

pub struct BaselinePorter {
    /// 沙盒源码工作仓库（bare 的克隆，同时持有上游 v* tag 与 promote/* tag）。
    repo_dir: PathBuf,
    /// 编排主流程句柄。冲突解决、编译/测试修复等一切需要智能的步骤都打包成
    /// 任务提交给主流程 agent 执行；本模块绝不直接调用 LLM。缺失时只走纯
    /// git 路径（cherry-pick 干净可移植，冲突即回流）。
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
    /// 实例身份 id，决定工作分支名 `evol/<id>`；缺省 `local`。
    instance_id: Option<String>,
    /// 是否运行 cargo check/test 质量门（沙盒内开启；单元测试关闭）。
    quality_gate: bool,
    /// 单轮智能解决任务的等待上限（秒）。
    resolve_timeout_secs: u64,
    /// eval A/B 任务的等待上限（秒，含沙盒双构建）。
    eval_timeout_secs: u64,
    /// 语义吸收确认任务的等待上限（秒）。
    absorb_timeout_secs: u64,
    /// 轮询智能任务结果的间隔（秒）。
    poll_interval_secs: u64,
    /// 移植 commit 的提交者身份（agent 路径的新 commit 使用）。
    committer_name: String,
    committer_email: String,
}

impl BaselinePorter {
    pub fn new(repo_dir: impl Into<PathBuf>) -> Self {
        Self {
            repo_dir: repo_dir.into(),
            orchestrator: None,
            instance_id: None,
            quality_gate: true,
            resolve_timeout_secs: 1800,
            eval_timeout_secs: 3600,
            absorb_timeout_secs: 600,
            poll_interval_secs: 5,
            committer_name: "Cogneva Evolution".into(),
            committer_email: "evolution@cogneva.local".into(),
        }
    }

    /// 注入编排主流程句柄（智能解决步骤的唯一通道）。
    pub fn with_orchestrator(mut self, orch: Arc<dyn OrchestratorControl>) -> Self {
        self.orchestrator = Some(orch);
        self
    }

    /// 注入实例身份 id（决定 `evol/<id>` 分支名）。
    pub fn with_instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = Some(id.into());
        self
    }

    /// 关闭 cargo 质量门（仅单元测试用：临时仓库无 Rust workspace）。
    pub fn without_quality_gate(mut self) -> Self {
        self.quality_gate = false;
        self
    }

    /// 覆盖智能解决任务的等待/轮询节奏（测试用短超时）。
    pub fn with_resolve_timeout(mut self, timeout_secs: u64, poll_secs: u64) -> Self {
        self.resolve_timeout_secs = timeout_secs;
        self.poll_interval_secs = poll_secs.max(1);
        self
    }

    /// 覆盖 eval A/B 任务的等待上限（秒）。
    pub fn with_eval_timeout(mut self, timeout_secs: u64) -> Self {
        self.eval_timeout_secs = timeout_secs;
        self
    }

    /// 覆盖语义吸收确认任务的等待上限（秒）。
    pub fn with_absorb_timeout(mut self, timeout_secs: u64) -> Self {
        self.absorb_timeout_secs = timeout_secs;
        self
    }

    /// 覆盖移植 commit 的提交者身份。
    pub fn with_committer(mut self, name: impl Into<String>, email: impl Into<String>) -> Self {
        self.committer_name = name.into();
        self.committer_email = email.into();
        self
    }

    async fn git(&self, args: &[&str]) -> SFResult<String> {
        let cmdline = format!("git {}", args.join(" "));
        let fut = tokio::process::Command::new("git")
            .args(args)
            .current_dir(&self.repo_dir)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(Duration::from_secs(120), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after 120s")))?
            .map_err(|e| SFError::IO(format!("failed to run git: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!("{cmdline} failed: {stderr}")));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// 版本比较失败（ref 不存在等）按"无"处理，不刷成错误日志。
    async fn git_opt(&self, args: &[&str]) -> SFResult<Option<String>> {
        match self.git(args).await {
            Ok(s) if s.is_empty() => Ok(None),
            Ok(s) => Ok(Some(s)),
            Err(e) => {
                debug!(?args, error = %e, "git probe failed; treating as absent");
                Ok(None)
            }
        }
    }

    /// 仓库里最新的上游 release tag（`v<x.y.z>`，semver 取最大）；无 tag 返回 None。
    pub async fn latest_release_tag(&self) -> SFResult<Option<String>> {
        let out = self.git(&["tag", "--list", "v*"]).await?;
        let mut best: Option<((u64, u64, u64), String)> = None;
        for tag in out.lines().map(str::trim).filter(|t| !t.is_empty()) {
            if let Some(v) = parse_release_version(tag) {
                if best.as_ref().is_none_or(|(bv, _)| v > *bv) {
                    best = Some((v, tag.to_string()));
                }
            }
        }
        Ok(best.map(|(_, tag)| tag))
    }

    /// 盘点全部晋级变更：`promote/*` tag → commit + tag message 审计元数据。
    pub async fn list_promoted(&self) -> SFResult<Vec<PromotedChange>> {
        // 单行输出：`<tag> <tag-object> <peeled-commit>`。annotated tag 的
        // 解引用 commit 在 %(*objectname)；轻量 tag 该字段为空，回退 objectname。
        let out = self
            .git(&[
                "for-each-ref",
                "--format=%(refname:short) %(objectname) %(*objectname)",
                "refs/tags/promote/",
            ])
            .await?;
        let mut changes = Vec::new();
        for line in out.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }
            let tag = parts[0].to_string();
            let commit = match parts.get(2).copied() {
                Some(peeled) if !peeled.is_empty() => peeled.to_string(),
                _ => parts.get(1).copied().unwrap_or_default().to_string(),
            };
            if commit.is_empty() {
                warn!(%tag, "promote tag has no resolvable commit; skipping");
                continue;
            }
            let message = self
                .git(&["tag", "-l", "--format=%(contents)", &tag])
                .await?;
            let (change_id, level, eval_summary) = parse_tag_message(&message);
            let change_id =
                change_id.unwrap_or_else(|| tag.trim_start_matches("promote/").to_string());
            changes.push(PromotedChange {
                change_id,
                tag,
                commit,
                level,
                eval_summary,
            });
        }
        // for-each-ref 默认字典序，晋级历史线性，字典序即演进顺序。
        changes.sort_by(|a, b| a.tag.cmp(&b.tag));
        Ok(changes)
    }

    /// 含全部晋级提交的 tip：优先 evolution-release 分支远端引用，
    /// 退化为最后一个晋级 tag（晋级历史线性，新 tag 是旧 tag 的后代）。
    async fn evolution_tip(&self, changes: &[PromotedChange]) -> SFResult<Option<String>> {
        if let Some(tip) = self
            .git_opt(&[
                "rev-parse",
                "--verify",
                "-q",
                "refs/remotes/local/evolution-release",
            ])
            .await?
        {
            return Ok(Some(tip));
        }
        Ok(changes.last().map(|c| c.commit.clone()))
    }

    /// `git cherry <new_tag> <head>`：输出新基线..tip 范围每个提交一行，
    /// `- <sha>` = patch-id 命中（新基线已有等价 patch，已吸收），
    /// `+ <sha>` = 缺失（待移植）。返回 (absorbed, pending) 两个 sha 集合。
    async fn cherry_sets(
        &self,
        new_tag: &str,
        head: &str,
    ) -> SFResult<(HashSet<String>, HashSet<String>)> {
        let out = self.git(&["cherry", new_tag, head]).await?;
        let mut absorbed = HashSet::new();
        let mut pending = HashSet::new();
        for line in out.lines() {
            let mut it = line.split_whitespace();
            match (it.next(), it.next()) {
                (Some("-"), Some(sha)) => {
                    absorbed.insert(sha.to_string());
                }
                (Some("+"), Some(sha)) => {
                    pending.insert(sha.to_string());
                }
                _ => {}
            }
        }
        Ok((absorbed, pending))
    }

    /// 判定单条变更相对新基线的吸收状态。
    async fn classify(
        &self,
        change: &PromotedChange,
        new_tag: &str,
        absorbed: &HashSet<String>,
        pending: &HashSet<String>,
    ) -> SFResult<AbsorptionStatus> {
        if absorbed.contains(&change.commit) {
            return Ok(AbsorptionStatus::Absorbed);
        }
        if pending.contains(&change.commit) {
            return Ok(AbsorptionStatus::Pending);
        }
        // 不在 cherry 输出里：不在 new_tag..tip 范围内。正常情况是该提交
        // 已是新基线祖先（新基线直接包含了它）→ 已吸收。
        match self
            .git(&["merge-base", "--is-ancestor", &change.commit, new_tag])
            .await
        {
            Ok(_) => Ok(AbsorptionStatus::Absorbed),
            Err(_) => {
                // 既不在范围也不是祖先：tip 没盖住它（非线性历史）。
                // 以该提交自身为 head 单独跑一次 cherry 判定。
                let (abs, pen) = self.cherry_sets(new_tag, &change.commit).await?;
                if abs.contains(&change.commit) {
                    Ok(AbsorptionStatus::Absorbed)
                } else if pen.contains(&change.commit) {
                    Ok(AbsorptionStatus::Pending)
                } else {
                    // 仍无结论：保守按待移植处理，交给移植阶段的质量门兜底，
                    // 绝不静默丢弃一条晋级变更。
                    warn!(
                        change_id = %change.change_id,
                        commit = %change.commit,
                        "absorption detection inconclusive; treating as pending"
                    );
                    Ok(AbsorptionStatus::Pending)
                }
            }
        }
    }

    /// 语义吸收确认：patch-id 未命中时，把[变更意图 + 原始 diff + 新基线
    /// 相关现状]打包成 `baseline_port_absorb_check` 主流程任务，由 agent 读
    /// 新基线代码判定意图是否已被覆盖。返回 true = 已吸收（跳过移植）。
    ///
    /// fail-closed：无编排器、任务失败/超时、回复无法解析为
    /// `{"absorbed": bool}` 一律按未吸收处理——重复移植的成本远低于
    /// 静默丢一条晋级变更。
    async fn semantic_absorb_check(&self, change: &PromotedChange, new_tag: &str) -> bool {
        let Some(orch) = &self.orchestrator else {
            return false;
        };

        let patch = self.change_patch(&change.commit).await.unwrap_or_default();
        let intent = self.change_intent(change).await;
        let task_id = format!(
            "port-absorb-{}-{}",
            sanitize_ref(&change.change_id),
            sanitize_ref(new_tag)
        );
        let goal = format!(
            "Decide whether the new baseline {new_tag} already achieves the intent \
             of the promoted change described below, even though its exact patch \
             does not apply (different implementation, upstream fix, or refactor \
             covering the same behavior).\n\n\
             Change intent:\n{intent}\n\n\
             Original diff:\n{}\n\n\
             Inspect the current worktree (already at the new baseline) and answer \
             ONLY with a JSON object: {{\"absorbed\": true}} if the baseline fully \
             covers the intent, {{\"absorbed\": false}} if any part is still missing. \
             When in doubt, answer false.",
            tail(&patch, 12000)
        );
        let mut task = Task::new(
            task_id.clone(),
            TaskType::Custom("baseline_port_absorb_check".into()),
            serde_json::json!({
                "evolution_mode": "baseline_port",
                "task_kind": "baseline_port_absorb_check",
                "change_id": change.change_id,
                "baseline_tag": new_tag,
            }),
        );
        task.timeout_seconds = self.absorb_timeout_secs;
        task.action_planner_meta = Some(Self::verified_meta(
            "Baseline port semantic absorption check; read-only judgment",
        ));

        match self
            .submit_and_wait(orch, task, goal, self.absorb_timeout_secs)
            .await
        {
            Ok(Ok(completed)) => {
                let absorbed = completed
                    .result
                    .as_ref()
                    .and_then(|r| r.get("absorbed"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                info!(
                    change_id = %change.change_id,
                    absorbed,
                    "semantic absorption check completed"
                );
                absorbed
            }
            Ok(Err(reason)) => {
                warn!(
                    change_id = %change.change_id,
                    %reason,
                    "semantic absorption check failed; treating as pending"
                );
                false
            }
            Err(e) => {
                warn!(
                    change_id = %change.change_id,
                    error = %e,
                    "semantic absorption check error; treating as pending"
                );
                false
            }
        }
    }

    /// 该新基线是否已移植过：`evol/<id>` 分支存在且已包含 new_tag 即视为
    /// 完成（移植产物分支从新基线切出，成功推送后 new_tag 必是其祖先）。
    /// 远端探测失败按未移植处理——误判为重跑的代价是一次幂等移植，
    /// 误判为已完成的代价是晋级历史静默丢失，必须偏保守。
    pub async fn already_ported(&self, new_tag: &str) -> bool {
        let instance = self.instance_id.clone().unwrap_or_else(|| "local".into());
        let branch_ref = format!("refs/remotes/local/evol/{instance}");
        if self
            .git_opt(&["rev-parse", "--verify", "-q", &branch_ref])
            .await
            .ok()
            .flatten()
            .is_none()
        {
            return false;
        }
        self.git(&["merge-base", "--is-ancestor", new_tag, &branch_ref])
            .await
            .is_ok()
    }

    /// 刷新上游/宿主 tag。两个远程各自尽力而为：宿主 bare（local）可能
    /// 还没被 seed 播种，upstream 可能不可达——任一失败都不阻塞本轮判定
    ///（判定基于本地已有 tag，最多晚一个轮询周期看到新基线）。
    pub async fn fetch_tags(&self) {
        for remote in ["local", "upstream"] {
            if let Err(e) = self.git(&["fetch", "--tags", remote]).await {
                debug!(remote, error = %e, "tag refresh failed; using local tags as-is");
            }
        }
    }

    /// 产出移植计划。当前版本（如 `0.5.7`，不带 v 前缀）不低于最新 release
    /// tag、或仓库里没有 release tag 时返回 None（无新基线可移植）。
    pub async fn plan(&self, current_version: &str) -> SFResult<Option<PortPlan>> {
        let Some(new_tag) = self.latest_release_tag().await? else {
            return Ok(None);
        };
        let current = parse_release_version(&format!("v{current_version}"));
        let latest = parse_release_version(&new_tag).expect("tag came from latest_release_tag");
        let newer = match current {
            Some(c) => latest > c,
            None => true,
        };
        if !newer {
            return Ok(None);
        }

        let changes = self.list_promoted().await?;
        let current_tag = format!("v{current_version}");
        if changes.is_empty() {
            info!(%new_tag, "new upstream release found; instance has zero promoted changes");
            return Ok(Some(PortPlan {
                new_tag,
                current_tag,
                items: Vec::new(),
            }));
        }

        let Some(tip) = self.evolution_tip(&changes).await? else {
            return Ok(Some(PortPlan {
                new_tag,
                current_tag,
                items: Vec::new(),
            }));
        };
        let (absorbed, pending) = self.cherry_sets(&new_tag, &tip).await?;

        let mut items = Vec::with_capacity(changes.len());
        for change in changes {
            let mut status = self
                .classify(&change, &new_tag, &absorbed, &pending)
                .await?;
            // patch-id 判定为待移植时，再做一次语义吸收确认：官方可能以
            // 不同实现达成了同一意图（patch-id 不命中但语义已覆盖）。确认
            // 是智能判定，走主流程任务；任何失败一律 fail-closed 回 Pending，
            // 宁可重复移植也不静默丢弃晋级变更。
            if status == AbsorptionStatus::Pending
                && self.semantic_absorb_check(&change, &new_tag).await
            {
                status = AbsorptionStatus::Absorbed;
            }
            info!(
                change_id = %change.change_id,
                tag = %change.tag,
                ?status,
                "baseline port absorption check"
            );
            items.push(PortPlanItem { change, status });
        }

        let n_absorbed = items
            .iter()
            .filter(|i| i.status == AbsorptionStatus::Absorbed)
            .count();
        let n_pending = items.len() - n_absorbed;
        info!(
            %new_tag,
            total = items.len(),
            absorbed = n_absorbed,
            pending = n_pending,
            "baseline port plan ready"
        );
        Ok(Some(PortPlan {
            new_tag,
            current_tag,
            items,
        }))
    }

    // ========================================================================
    // 移植执行（任务2/3）
    //
    // 本模块只做确定性编排与验收：git 移植、cargo 质量门、tag、推送。
    // 一切需要智能的步骤（cherry-pick 冲突重写、编译/测试失败修复）都打包成
    // `baseline_port_resolve` 任务提交给 orchestration + collaboration 主流程，
    // 由沙盒 agent 在同一工作树完成修改；这里轮询任务后独立跑质量门复验。
    // 单条变更最多 3 轮，尽则回退并回流 `baseline_port_rework` 任务。
    // ========================================================================

    /// 执行移植计划：建 `evol/<id>` 分支 → 逐变更移植 → 失败回流 →
    /// 打 `gen-n` 代际 tag → 推送 `local` 远程。
    pub async fn execute(&self, plan: &PortPlan) -> SFResult<PortReport> {
        // 实例零晋级历史：新基线即目标，消费方纯 reset，不建分支不打 tag。
        if plan.items.is_empty() {
            info!(
                new_tag = %plan.new_tag,
                "no promoted changes; clean reset to new baseline"
            );
            return Ok(PortReport {
                new_tag: plan.new_tag.clone(),
                clean_reset: true,
                branch: None,
                gen_tag: None,
                items: Vec::new(),
            });
        }

        self.ensure_clean_tree().await?;

        let instance = self.instance_id.clone().unwrap_or_else(|| "local".into());
        let branch = format!("evol/{instance}");
        // 从新基线重建移植分支：移植是可重跑的确定性流程，-B 覆盖上一轮残留。
        self.git(&["checkout", "-B", &branch, &plan.new_tag])
            .await?;
        info!(%branch, new_tag = %plan.new_tag, "port branch created from new baseline");

        let mut items = Vec::with_capacity(plan.items.len());
        for item in &plan.items {
            let outcome = match item.status {
                AbsorptionStatus::Absorbed => {
                    info!(change_id = %item.change.change_id, "change absorbed upstream; skipped");
                    PortOutcome::Absorbed
                }
                AbsorptionStatus::Pending => self.port_one(&item.change, &plan.new_tag).await?,
            };
            if let PortOutcome::NeedsRework { ref reason } = outcome {
                self.submit_rework(&item.change, &plan.new_tag, reason)
                    .await;
            }
            items.push(PortItemResult {
                change_id: item.change.change_id.clone(),
                outcome,
            });
        }

        let n_ported = items
            .iter()
            .filter(|i| matches!(i.outcome, PortOutcome::Ported { .. }))
            .count();
        let n_rework = items
            .iter()
            .filter(|i| matches!(i.outcome, PortOutcome::NeedsRework { .. }))
            .count();

        let gen_tag = self
            .tag_generation(&plan.new_tag, n_ported, n_rework)
            .await?;
        self.push_results(&branch, &gen_tag).await?;

        info!(
            %branch, %gen_tag,
            ported = n_ported,
            rework = n_rework,
            "baseline port complete"
        );
        Ok(PortReport {
            new_tag: plan.new_tag.clone(),
            clean_reset: false,
            branch: Some(branch),
            gen_tag: Some(gen_tag),
            items,
        })
    }

    /// 移植单条变更。失败时工作树 reset 回本变更之前的 tip，不留痕迹。
    async fn port_one(&self, change: &PromotedChange, new_tag: &str) -> SFResult<PortOutcome> {
        let prev_head = self.git(&["rev-parse", "HEAD"]).await?;

        // 第一轮先试纯 git cherry-pick（保留原提交与作者）。
        let cherry = self.try_cherry_pick(&change.commit).await?;
        let cherry_ok = cherry.is_ok();
        let mut feedback = match cherry {
            Ok(()) => String::new(),
            Err(conflict) => conflict,
        };

        for round in 1u32..=3 {
            let need_agent = round > 1 || !cherry_ok;
            if need_agent {
                match self
                    .run_resolve_task(change, new_tag, round, &feedback)
                    .await?
                {
                    Ok(()) => {
                        if !self.worktree_dirty().await? {
                            feedback = format!(
                                "resolve task round {round} reported success but the \
                                 worktree has no changes"
                            );
                            warn!(change_id = %change.change_id, round, "{}", feedback);
                            continue;
                        }
                        self.git(&["add", "-A"]).await?;
                        self.commit_ported(change, new_tag, round).await?;
                    }
                    Err(task_error) => {
                        if self.orchestrator.is_none() {
                            // 无主流程可提交智能任务：纯 git 路径已走到头。
                            warn!(
                                change_id = %change.change_id,
                                "orchestrator unavailable; cannot resolve port failure"
                            );
                            feedback = task_error;
                            break;
                        }
                        feedback = task_error;
                        continue;
                    }
                }
            }

            // 独立验收：cargo check + cargo test --workspace + eval A/B。
            match self.run_quality_gate(change, new_tag, round).await? {
                Ok(()) => {
                    let commit = self.git(&["rev-parse", "HEAD"]).await?;
                    let route = if need_agent {
                        PortRoute::AgentResolved { round }
                    } else {
                        PortRoute::CherryPicked
                    };
                    info!(
                        change_id = %change.change_id,
                        ?route,
                        "change ported and passed quality gate"
                    );
                    return Ok(PortOutcome::Ported { commit, route });
                }
                Err(gate_output) => {
                    feedback = format!("quality gate failed:\n{gate_output}");
                    warn!(
                        change_id = %change.change_id,
                        round,
                        "quality gate failed; handing failure to main-loop agent"
                    );
                }
            }
        }

        warn!(
            change_id = %change.change_id,
            "port failed after 3 rounds; rewinding and reflowing as rework"
        );
        let _ = self.git(&["reset", "--hard", &prev_head]).await;
        Ok(PortOutcome::NeedsRework {
            reason: format!("port to {new_tag} failed after 3 rounds: {feedback}"),
        })
    }

    /// 尝试 cherry-pick。冲突时抓取冲突证据并 abort，返回 Err(证据文本)。
    async fn try_cherry_pick(&self, commit: &str) -> SFResult<Result<(), String>> {
        let output = tokio::process::Command::new("git")
            .args([
                "-c",
                &format!("user.name={}", self.committer_name),
                "-c",
                &format!("user.email={}", self.committer_email),
                "cherry-pick",
                commit,
            ])
            .current_dir(&self.repo_dir)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("failed to run git cherry-pick: {e}")))?;
        if output.status.success() {
            return Ok(Ok(()));
        }

        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let mut evidence = format!(
            "git cherry-pick did not apply cleanly:\n{}",
            tail(&stderr, 4000)
        );

        // 抓冲突文件列表与冲突标记规模（abort 后这些证据就没了）。
        if let Ok(names) = self.git(&["diff", "--name-only", "--diff-filter=U"]).await {
            if !names.trim().is_empty() {
                evidence.push_str("\nUnmerged files:\n");
                for f in names.lines().filter(|l| !l.trim().is_empty()).take(20) {
                    let mut hunks = 0usize;
                    if let Ok(content) = tokio::fs::read_to_string(self.repo_dir.join(f)).await {
                        hunks = content
                            .lines()
                            .filter(|l| l.starts_with("<<<<<<< "))
                            .count();
                    }
                    evidence.push_str(&format!("- {f} ({hunks} conflict hunks)\n"));
                }
            }
        }
        let _ = self.git(&["cherry-pick", "--abort"]).await;
        Ok(Err(evidence))
    }

    /// 提交智能移植任务给主流程并等待完成。agent 直接在工作树落地修改。
    /// 返回 Err(原因) 表示任务失败/超时/无产出（调用方据此进入下一轮）。
    async fn run_resolve_task(
        &self,
        change: &PromotedChange,
        new_tag: &str,
        round: u32,
        feedback: &str,
    ) -> SFResult<Result<(), String>> {
        let Some(orch) = &self.orchestrator else {
            return Ok(Err("orchestrator not configured".into()));
        };

        let patch = self.change_patch(&change.commit).await.unwrap_or_default();
        let intent = self.change_intent(change).await;
        let task_id = format!(
            "port-resolve-{}-{}-r{}",
            sanitize_ref(&change.change_id),
            sanitize_ref(new_tag),
            round
        );
        let failure = if feedback.trim().is_empty() {
            "The change did not apply cleanly to the new baseline.".to_string()
        } else {
            tail(feedback, 8000)
        };
        let goal = format!(
            "Port promoted change `{}` onto the new baseline {new_tag}.\n\n\
             Original change intent:\n{intent}\n\n\
             Work directly in the current git worktree, which is already checked \
             out at the new baseline. Re-implement the change's intent so it fits \
             the new baseline: edit source files, and make sure `cargo check \
             --workspace` and `cargo test --workspace` pass. Do NOT run git \
             commit; the porting controller handles commits. Modify only source \
             files under crates/**/src.\n\n\
             Round {round} status:\n{failure}",
            change.change_id
        );

        let mut task = Task::new(
            task_id.clone(),
            TaskType::Custom("baseline_port_resolve".into()),
            serde_json::json!({
                "evolution_mode": "baseline_port",
                "task_kind": "baseline_port_resolve",
                "change_id": change.change_id,
                "baseline_tag": new_tag,
                "round": round,
                "intent": intent,
                "original_diff": patch,
                "failure_feedback": failure,
            }),
        );
        task.timeout_seconds = self.resolve_timeout_secs;
        task.action_planner_meta = Some(Self::verified_meta(
            "Baseline port resolve; route to the sandbox evolution agent",
        ));

        match self
            .submit_and_wait(orch, task, goal, self.resolve_timeout_secs)
            .await?
        {
            Ok(_) => Ok(Ok(())),
            Err(reason) => Ok(Err(format!("resolve task {task_id} {reason}"))),
        }
    }

    /// 提交任务给主流程并轮询至终态。Completed 返回任务本身（带 result），
    /// Failed/Cancelled/超时返回原因文本。
    async fn submit_and_wait(
        &self,
        orch: &Arc<dyn OrchestratorControl>,
        task: Task,
        goal: String,
        timeout_secs: u64,
    ) -> SFResult<Result<Task, String>> {
        let task_id = task.id.clone();
        let ids = orch
            .submit_goal_auto(&goal, vec![task])
            .await
            .map_err(|e| SFError::Internal(format!("submit task {task_id} failed: {e}")))?;
        let id = ids.into_iter().next().unwrap_or(task_id);

        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            if let Some(t) = orch.get_task(&id).await {
                match t.status {
                    TaskStatus::Completed => return Ok(Ok(t)),
                    TaskStatus::Failed => {
                        return Ok(Err(format!("failed: {}", t.error.unwrap_or_default())))
                    }
                    TaskStatus::Cancelled => return Ok(Err("cancelled".into())),
                    _ => {}
                }
            }
            if Instant::now() >= deadline {
                return Ok(Err(format!("timed out after {timeout_secs}s")));
            }
            tokio::time::sleep(Duration::from_secs(self.poll_interval_secs)).await;
        }
    }

    fn verified_meta(note: &str) -> ActionPlannerMeta {
        ActionPlannerMeta {
            verified: true,
            version: Some("1.0.0".into()),
            note: Some(note.into()),
            source: Some(ActionPlannerSource::UserProvided),
            confidence: None,
            timestamp: Some(chrono::Utc::now()),
        }
    }

    /// 3 轮失败后回流：提交 `baseline_port_rework` 任务给全进化回路下一轮
    /// 重新实现；无编排器时落文件兜底，绝不静默丢弃。
    async fn submit_rework(&self, change: &PromotedChange, new_tag: &str, reason: &str) {
        let intent = self.change_intent(change).await;
        let goal = format!(
            "Re-implement the intent of promoted change `{}` from scratch on \
             baseline {new_tag}.\n\n\
             Original intent:\n{intent}\n\n\
             Automatic porting failed: {reason}\n\n\
             Generate a fresh change against the current baseline that achieves \
             the same goal.",
            change.change_id
        );

        let Some(orch) = &self.orchestrator else {
            self.fs_rework_dump(change, new_tag, reason, &goal).await;
            return;
        };
        let task = Task::new(
            format!(
                "port-rework-{}-{}",
                sanitize_ref(&change.change_id),
                sanitize_ref(new_tag)
            ),
            TaskType::Custom("baseline_port_rework".into()),
            serde_json::json!({
                "evolution_mode": "generate_change",
                "task_kind": "baseline_port_rework",
                "change_id": change.change_id,
                "baseline_tag": new_tag,
                "intent": intent,
                "reason": reason,
                "goal": goal,
            }),
        );
        match orch.submit_goal_auto(&goal, vec![task]).await {
            Ok(_) => info!(
                change_id = %change.change_id,
                "reflowed failed port as rework task to main evolution loop"
            ),
            Err(e) => {
                warn!(change_id = %change.change_id, error = %e, "rework task submit failed; dumping to file");
                self.fs_rework_dump(change, new_tag, reason, &goal).await;
            }
        }
    }

    async fn fs_rework_dump(
        &self,
        change: &PromotedChange,
        new_tag: &str,
        reason: &str,
        goal: &str,
    ) {
        let dir = self
            .repo_dir
            .parent()
            .unwrap_or(&self.repo_dir)
            .join("port-rework");
        if let Err(e) = tokio::fs::create_dir_all(&dir).await {
            warn!(error = %e, "cannot create port-rework dump dir");
            return;
        }
        let path = dir.join(format!(
            "{}-{}.json",
            sanitize_ref(&change.change_id),
            sanitize_ref(new_tag)
        ));
        let payload = serde_json::json!({
            "change_id": change.change_id,
            "baseline_tag": new_tag,
            "reason": reason,
            "goal": goal,
            "dumped_at": chrono::Utc::now().to_rfc3339(),
        });
        match tokio::fs::write(
            &path,
            serde_json::to_string_pretty(&payload).unwrap_or_default(),
        )
        .await
        {
            Ok(()) => warn!(path = %path.display(), "rework task dumped to file (no orchestrator)"),
            Err(e) => warn!(error = %e, "failed to dump rework task"),
        }
    }

    async fn commit_ported(
        &self,
        change: &PromotedChange,
        new_tag: &str,
        round: u32,
    ) -> SFResult<()> {
        let msg = format!(
            "port: {} onto {} (main-loop agent, round {})",
            change.change_id, new_tag, round
        );
        self.git(&[
            "-c",
            &format!("user.name={}", self.committer_name),
            "-c",
            &format!("user.email={}", self.committer_email),
            "commit",
            "-m",
            &msg,
        ])
        .await?;
        Ok(())
    }

    /// cargo check + cargo test --workspace + 适用时 eval A/B 回归验收。
    /// 失败返回尾部输出供下一轮反馈。
    async fn run_quality_gate(
        &self,
        change: &PromotedChange,
        new_tag: &str,
        round: u32,
    ) -> SFResult<Result<(), String>> {
        if !self.quality_gate {
            return Ok(Ok(()));
        }
        let (check_ok, check_out) = self.run_cargo(&["check", "--workspace"], 900).await?;
        if !check_ok {
            return Ok(Err(tail(&check_out, 8000)));
        }
        let (test_ok, test_out) = self.run_cargo(&["test", "--workspace"], 1800).await?;
        if !test_ok {
            return Ok(Err(tail(&test_out, 8000)));
        }
        self.run_eval_gate(change, new_tag, round).await
    }

    /// eval A/B 回归门：移植不得让固定评估任务集成功率统计显著下降。
    /// 评估执行（跑 agent 双构建）是智能步骤，打包成主流程任务；本控制器
    /// 只对返回的两组结果跑确定性 z 检验。无编排器的环境视为不适用跳过。
    async fn run_eval_gate(
        &self,
        change: &PromotedChange,
        new_tag: &str,
        round: u32,
    ) -> SFResult<Result<(), String>> {
        let Some(orch) = &self.orchestrator else {
            debug!(
                "no orchestrator; eval A/B gate skipped for {}",
                change.change_id
            );
            return Ok(Ok(()));
        };

        let task_id = format!(
            "port-eval-{}-{}-r{}",
            sanitize_ref(&change.change_id),
            sanitize_ref(new_tag),
            round
        );
        let goal = format!(
            "Run the fixed evaluation suite twice to check whether porting promoted \
             change `{}` onto baseline {new_tag} regressed behavior.\n\n\
             Procedure:\n\
             1. In the current worktree (already at the ported state), build and run \
             the project's standard fixed eval task set; record outcomes as `after`.\n\
             2. Create a temporary git worktree at tag {new_tag}, build there and run \
             the SAME eval task set; record outcomes as `before`. Remove the temporary \
             worktree afterwards. Do NOT commit anything in either worktree.\n\
             3. If no eval suite exists for this component, report applicable=false.\n\n\
             Return ONLY a JSON object:\n\
             {{\"applicable\": true, \"before\": [{{\"task_id\": \"...\", \"success\": \
             true, \"latency_ms\": 0, \"cost_tokens\": 0}}], \"after\": [...], \
             \"note\": \"\"}}",
            change.change_id
        );
        let mut task = Task::new(
            task_id.clone(),
            TaskType::Custom("baseline_port_eval".into()),
            serde_json::json!({
                "evolution_mode": "baseline_port",
                "task_kind": "baseline_port_eval",
                "change_id": change.change_id,
                "baseline_tag": new_tag,
                "round": round,
            }),
        );
        task.timeout_seconds = self.eval_timeout_secs;
        task.action_planner_meta = Some(Self::verified_meta(
            "Baseline port eval A/B; route to the sandbox evolution agent",
        ));

        let completed = match self
            .submit_and_wait(orch, task, goal, self.eval_timeout_secs)
            .await?
        {
            Ok(t) => t,
            Err(reason) => return Ok(Err(format!("eval task {task_id} {reason}"))),
        };
        let result = completed.result.unwrap_or_else(|| serde_json::json!({}));
        Ok(eval_regression_feedback(&result))
    }

    async fn run_cargo(&self, args: &[&str], timeout_secs: u64) -> SFResult<(bool, String)> {
        let cmdline = format!("cargo {}", args.join(" "));
        let fut = tokio::process::Command::new("cargo")
            .args(args)
            .current_dir(&self.repo_dir)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(Duration::from_secs(timeout_secs), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after {timeout_secs}s")))?
            .map_err(|e| SFError::IO(format!("failed to run cargo: {e}")))?;
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok((output.status.success(), combined))
    }

    async fn ensure_clean_tree(&self) -> SFResult<()> {
        let status = self.git(&["status", "--porcelain"]).await?;
        if !status.trim().is_empty() {
            return Err(SFError::Validation(format!(
                "worktree not clean before baseline port:\n{status}"
            )));
        }
        Ok(())
    }

    async fn worktree_dirty(&self) -> SFResult<bool> {
        Ok(!self
            .git(&["status", "--porcelain"])
            .await?
            .trim()
            .is_empty())
    }

    /// 变更原始 patch（相对其父提交的纯 diff）。
    async fn change_patch(&self, commit: &str) -> SFResult<String> {
        self.git(&["show", "--format=", "--no-ext-diff", commit])
            .await
    }

    /// 变更意图：原始提交信息 + 晋级 tag 审计元数据。
    async fn change_intent(&self, change: &PromotedChange) -> String {
        let msg = self
            .git(&["log", "-1", "--format=%s%n%n%b", &change.commit])
            .await
            .unwrap_or_default();
        format!(
            "{msg}\n[promotion] change_id={} level={} eval={}",
            change.change_id,
            change.level.as_deref().unwrap_or("unknown"),
            change.eval_summary.as_deref().unwrap_or("none")
        )
    }

    /// 打代际 tag：`gen-n`，n 在现有 gen-* 之上递增；message 带移植审计。
    async fn tag_generation(
        &self,
        new_tag: &str,
        n_ported: usize,
        n_rework: usize,
    ) -> SFResult<String> {
        let existing = self.git(&["tag", "--list", "gen-*"]).await?;
        let n = existing
            .lines()
            .filter_map(|l| l.strip_prefix("gen-")?.parse::<u32>().ok())
            .max()
            .unwrap_or(0)
            + 1;
        let tag = format!("gen-{n}");
        let msg = format!(
            "gen={n}\nbaseline={new_tag}\nported={n_ported}\nneeds-rework={n_rework}\ndate={}",
            chrono::Utc::now().to_rfc3339()
        );
        self.git(&["tag", "-a", "-m", &msg, &tag]).await?;
        Ok(tag)
    }

    /// 推送移植分支与代际 tag 到 `local` 远程（宿主 bare）。无 local 远程
    /// （非沙盒环境）时跳过。evol 分支为本流程独占，可 force 覆盖重建。
    async fn push_results(&self, branch: &str, gen_tag: &str) -> SFResult<()> {
        if self
            .git_opt(&["remote", "get-url", "local"])
            .await?
            .is_none()
        {
            debug!("no local remote; port branch/tag stay in working repo");
            return Ok(());
        }
        self.git(&[
            "push",
            "local",
            "--force",
            &format!("HEAD:refs/heads/{branch}"),
        ])
        .await?;
        self.git(&[
            "push",
            "local",
            &format!("refs/tags/{gen_tag}:refs/tags/{gen_tag}"),
        ])
        .await?;
        info!(%branch, %gen_tag, "port branch and generation tag pushed to local remote");
        Ok(())
    }
}

// ============================================================================
// 触发循环（规则3 接线）：轮询上游 release tag → 幂等判定 → plan → execute。
//
// 循环跑在沙盒进化 Pod 里，porter 的 repo_dir 即沙盒源码工作仓库。执行
// 期间 porter 独占该工作树（checkout -B evol/<id>）——新基线出现是低频
// 事件，移植完成后工作树即停在新基线移植产物上，与 seed 对齐逻辑同向。
// ============================================================================

/// 单个新基线的最近一次移植尝试记录。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct PortAttempt {
    /// "done" | "failed"。
    status: String,
    /// RFC3339 时间。
    at: String,
}

/// 尝试状态文件：tag → 最近一次尝试。done 永久跳过；failed 在冷却期
/// 内跳过（崩溃重启不立刻重跑同一个失败基线，进程重启也不丢"已移植"
/// 记忆）。文件损坏按空处理——最坏后果是一次幂等重跑，绝不反过来把
/// 未移植的基线记成已移植。
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct PortAttempts {
    attempts: std::collections::HashMap<String, PortAttempt>,
}

async fn load_attempts(path: &std::path::Path) -> PortAttempts {
    match tokio::fs::read_to_string(path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
            warn!(path = %path.display(), error = %e, "port attempts file corrupt; starting fresh");
            PortAttempts::default()
        }),
        Err(_) => PortAttempts::default(),
    }
}

async fn save_attempts(path: &std::path::Path, attempts: &PortAttempts) {
    if let Some(parent) = path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            warn!(path = %path.display(), error = %e, "cannot create port attempts dir");
            return;
        }
    }
    match serde_json::to_string_pretty(attempts) {
        Ok(json) => {
            if let Err(e) = tokio::fs::write(path, json).await {
                warn!(path = %path.display(), error = %e, "cannot persist port attempts");
            }
        }
        Err(e) => warn!(error = %e, "cannot serialize port attempts"),
    }
}

/// 该基线本轮是否应当尝试移植。bool 判定全部偏保守：任何不确定都
/// 倾向"再试一次"而不是"跳过"。
fn should_attempt(attempts: &PortAttempts, tag: &str, retry_cooldown_secs: u64) -> bool {
    match attempts.attempts.get(tag) {
        None => true,
        Some(a) if a.status == "done" => false,
        Some(a) => {
            let elapsed = chrono::DateTime::parse_from_rfc3339(&a.at)
                .map(|t| {
                    chrono::Utc::now()
                        .signed_duration_since(t.with_timezone(&chrono::Utc))
                        .num_seconds()
                        .max(0) as u64
                })
                // 时间戳不可解析：当作冷却已过，允许重试。
                .unwrap_or(u64::MAX);
            elapsed >= retry_cooldown_secs
        }
    }
}

/// 单轮触发：刷新 tag → 找新基线 → 幂等判定 → plan → execute → 记状态。
async fn port_tick(
    porter: &BaselinePorter,
    current_version: &str,
    config: &crate::BaselinePortConfig,
    state_path: &std::path::Path,
) -> SFResult<()> {
    porter.fetch_tags().await;
    let Some(latest) = porter.latest_release_tag().await? else {
        return Ok(());
    };
    let mut attempts = load_attempts(state_path).await;

    // 已成功移植过（含崩溃在 execute 之后、记状态之前的情形：远端分支
    // 已含新基线即视为完成，顺手补记状态）。
    if porter.already_ported(&latest).await {
        if attempts.attempts.get(&latest).map(|a| a.status.as_str()) != Some("done") {
            attempts.attempts.insert(
                latest.clone(),
                PortAttempt {
                    status: "done".into(),
                    at: chrono::Utc::now().to_rfc3339(),
                },
            );
            save_attempts(state_path, &attempts).await;
        }
        return Ok(());
    }
    if !should_attempt(&attempts, &latest, config.retry_cooldown_secs) {
        debug!(tag = %latest, "baseline port in retry cooldown; skipping");
        return Ok(());
    }

    let Some(plan) = porter.plan(current_version).await? else {
        return Ok(());
    };
    info!(
        new_tag = %plan.new_tag,
        pending = plan.pending().count(),
        absorbed = plan.absorbed().count(),
        "new upstream baseline detected; starting port"
    );
    let status = match porter.execute(&plan).await {
        Ok(report) => {
            info!(
                new_tag = %report.new_tag,
                clean_reset = report.clean_reset,
                ported = report.ported().count(),
                rework = report.needs_rework().count(),
                "baseline port finished"
            );
            "done"
        }
        Err(e) => {
            warn!(new_tag = %plan.new_tag, error = %e, "baseline port failed");
            "failed"
        }
    };
    attempts.attempts.insert(
        plan.new_tag.clone(),
        PortAttempt {
            status: status.into(),
            at: chrono::Utc::now().to_rfc3339(),
        },
    );
    save_attempts(state_path, &attempts).await;
    Ok(())
}

/// 基线移植触发循环。第一轮立即执行（启动即对齐新基线，不等一个
/// 轮询周期），之后按 `poll_interval_secs` 周期运行直到 shutdown。
pub async fn run_baseline_port_loop(
    porter: Arc<BaselinePorter>,
    current_version: String,
    config: crate::BaselinePortConfig,
    state_path: PathBuf,
    shutdown: cog_core::ShutdownSignal,
) {
    let interval = Duration::from_secs(config.poll_interval_secs.max(60));
    info!(
        interval_secs = interval.as_secs(),
        retry_cooldown_secs = config.retry_cooldown_secs,
        version = %current_version,
        "baseline port trigger loop started"
    );
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                if let Err(e) = port_tick(&porter, &current_version, &config, &state_path).await {
                    warn!(error = %e, "baseline port tick failed");
                }
            }
        }
    }
}

/// 输出尾部截断（编译/测试错误集中在尾部），按字符边界裁剪。
fn tail(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut start = s.len() - max;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    format!("[tail {max} of {} bytes]\n{}", s.len(), &s[start..])
}

/// eval A/B 任务结果的纯判定：移植只要求"不回归"——成功率统计显著下降
/// （z 检验 Reject）才判失败；显著提升或无统计差异都通过。数据缺失/无法
/// 解析按失败处理（验收权在统计检验，不能静默放行）。
fn eval_regression_feedback(result: &serde_json::Value) -> Result<(), String> {
    let applicable = result
        .get("applicable")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if !applicable {
        return Ok(());
    }
    let parse = |key: &str| -> Result<Vec<EvalOutcome>, String> {
        let raw = result
            .get(key)
            .ok_or_else(|| format!("eval result missing `{key}` outcomes"))?;
        serde_json::from_value(raw.clone())
            .map_err(|e| format!("eval result `{key}` outcomes unparseable: {e}"))
    };
    let before = parse("before")?;
    let after = parse("after")?;
    if before.is_empty() || after.is_empty() {
        return Err("eval result returned empty outcome sets".into());
    }
    let cmp = eval_compare(&before, &after);
    if cmp.verdict == EvalVerdict::Reject {
        let failing: Vec<&str> = after
            .iter()
            .filter(|o| !o.success)
            .map(|o| o.task_id.as_str())
            .collect();
        Err(format!(
            "eval A/B regression: z={:.2}, success rate {}/{} (baseline) -> {}/{} (ported); \
             failing tasks: {}",
            cmp.z_score,
            cmp.before.succeeded,
            cmp.before.total,
            cmp.after.succeeded,
            cmp.after.total,
            if failing.is_empty() {
                "(none reported)".to_string()
            } else {
                failing.join(", ")
            }
        ))
    } else {
        Ok(())
    }
}

/// 任务 id / 文件名安全化：只留 [A-Za-z0-9-_]。
fn sanitize_ref(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// 解析 `v<major>.<minor>.<patch>` 形式的 release tag；带预发布/构建后缀
/// （`-rc.1`、`+build`）时只比数值三元组。非 release tag 返回 None。
fn parse_release_version(tag: &str) -> Option<(u64, u64, u64)> {
    let core = tag.strip_prefix('v')?;
    let core = core.split('+').next().unwrap();
    let core = core.split('-').next().unwrap();
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    async fn git(dir: &Path, args: &[&str]) -> String {
        let output = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    /// 初始仓库：一个提交 + v0.5.7 tag，模拟私版 seed 基线。
    async fn setup_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init"]).await;
        git(dir.path(), &["config", "user.email", "test@test.com"]).await;
        git(dir.path(), &["config", "user.name", "Test"]).await;
        tokio::fs::write(dir.path().join("lib.rs"), "fn v1() {}\n")
            .await
            .unwrap();
        git(dir.path(), &["add", "."]).await;
        git(dir.path(), &["commit", "-m", "initial"]).await;
        git(dir.path(), &["tag", "v0.5.7"]).await;
        dir
    }

    /// 在 v0.5.7 之上做一条晋级变更并打 promote tag。
    async fn make_promoted(dir: &Path, change_id: &str, file: &str, body: &str) {
        tokio::fs::write(dir.join(file), body).await.unwrap();
        git(dir, &["add", "."]).await;
        git(dir, &["commit", "-m", &format!("change {change_id}")]).await;
        let msg = format!("change_id={change_id}\nlevel=l1_rollout\neval=Adopt z=2.3");
        git(
            dir,
            &["tag", "-a", "-m", &msg, &format!("promote/{change_id}")],
        )
        .await;
    }

    /// 从指定 tag 起 detached 建新提交并打新 release tag（模拟上游演进）。
    async fn upstream_release(dir: &Path, base: &str, tag: &str, file: &str, body: &str) {
        git(dir, &["checkout", "-q", base]).await;
        tokio::fs::write(dir.join(file), body).await.unwrap();
        git(dir, &["add", "."]).await;
        git(dir, &["commit", "-m", &format!("upstream {tag}")]).await;
        git(dir, &["tag", tag]).await;
    }

    #[tokio::test]
    async fn no_new_tag_returns_none() {
        let dir = setup_repo().await;
        let porter = BaselinePorter::new(dir.path());
        // 当前版本已是最新。
        let plan = porter.plan("0.5.7").await.unwrap();
        assert!(plan.is_none());
    }

    #[tokio::test]
    async fn new_tag_no_promoted_changes_yields_empty_plan() {
        let dir = setup_repo().await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "upstream.rs",
            "fn upstream() {}\n",
        )
        .await;
        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.new_tag, "v0.5.8");
        assert_eq!(plan.current_tag, "v0.5.7");
        assert!(plan.items.is_empty());
    }

    #[tokio::test]
    async fn promoted_change_absorbed_when_upstream_has_same_patch() {
        let dir = setup_repo().await;
        // 私版晋级一条变更。
        make_promoted(dir.path(), "chg-1", "feature.rs", "fn promoted() {}\n").await;
        // 上游独立合入了同一个 patch（cherry-pick 产生不同 commit、相同 patch-id）。
        git(dir.path(), &["checkout", "-q", "v0.5.7"]).await;
        git(dir.path(), &["cherry-pick", "promote/chg-1"]).await;
        git(dir.path(), &["tag", "v0.5.8"]).await;

        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].status, AbsorptionStatus::Absorbed);
        assert_eq!(plan.items[0].change.change_id, "chg-1");
        assert_eq!(plan.pending().count(), 0);
    }

    #[tokio::test]
    async fn promoted_change_pending_when_upstream_lacks_it() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-1", "feature.rs", "fn promoted() {}\n").await;
        // 上游只做了无关改动。
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "other.rs",
            "fn other() {}\n",
        )
        .await;

        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].status, AbsorptionStatus::Pending);
        assert_eq!(plan.pending().count(), 1);
    }

    #[tokio::test]
    async fn promoted_change_ancestor_of_new_tag_is_absorbed() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-1", "feature.rs", "fn promoted() {}\n").await;
        // 新 release tag 直接打在晋级提交之上（新基线已包含该历史）。
        git(dir.path(), &["tag", "v0.6.0", "promote/chg-1"]).await;

        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.items[0].status, AbsorptionStatus::Absorbed);
    }

    #[tokio::test]
    async fn list_promoted_parses_tag_metadata() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-7", "feature.rs", "fn promoted() {}\n").await;
        let porter = BaselinePorter::new(dir.path());
        let changes = porter.list_promoted().await.unwrap();
        assert_eq!(changes.len(), 1);
        let c = &changes[0];
        assert_eq!(c.change_id, "chg-7");
        assert_eq!(c.tag, "promote/chg-7");
        assert_eq!(c.level.as_deref(), Some("l1_rollout"));
        assert_eq!(c.eval_summary.as_deref(), Some("Adopt z=2.3"));
        assert!(!c.commit.is_empty());
    }

    #[tokio::test]
    async fn mixed_absorbed_and_pending_classified_independently() {
        let dir = setup_repo().await;
        // 两条私版晋级。
        make_promoted(dir.path(), "chg-a", "a.rs", "fn a() {}\n").await;
        make_promoted(dir.path(), "chg-b", "b.rs", "fn b() {}\n").await;
        // 上游只吸收了 chg-a（cherry-pick），chg-b 上游没有。
        git(dir.path(), &["checkout", "-q", "v0.5.7"]).await;
        git(dir.path(), &["cherry-pick", "promote/chg-a"]).await;
        tokio::fs::write(dir.path().join("upstream.rs"), "fn up() {}\n")
            .await
            .unwrap();
        git(dir.path(), &["add", "."]).await;
        git(dir.path(), &["commit", "-m", "upstream v0.5.8"]).await;
        git(dir.path(), &["tag", "v0.5.8"]).await;

        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.items.len(), 2);
        let by_id: std::collections::HashMap<&str, AbsorptionStatus> = plan
            .items
            .iter()
            .map(|i| (i.change.change_id.as_str(), i.status))
            .collect();
        assert_eq!(by_id["chg-a"], AbsorptionStatus::Absorbed);
        assert_eq!(by_id["chg-b"], AbsorptionStatus::Pending);
    }

    fn eval_outcomes(n_success: usize, n_total: usize) -> Vec<serde_json::Value> {
        (0..n_total)
            .map(|i| {
                serde_json::json!({
                    "task_id": format!("t{i}"),
                    "success": i < n_success,
                    "latency_ms": 100,
                    "cost_tokens": 50,
                })
            })
            .collect()
    }

    #[test]
    fn eval_gate_skips_when_not_applicable() {
        let result = serde_json::json!({ "applicable": false });
        assert!(eval_regression_feedback(&result).is_ok());
    }

    #[test]
    fn eval_gate_fails_closed_on_missing_outcomes() {
        assert!(eval_regression_feedback(&serde_json::json!({})).is_err());
        let only_before = serde_json::json!({
            "before": eval_outcomes(20, 20),
        });
        assert!(eval_regression_feedback(&only_before).is_err());
        let empty = serde_json::json!({
            "before": [],
            "after": eval_outcomes(1, 1),
        });
        assert!(eval_regression_feedback(&empty).is_err());
    }

    #[test]
    fn eval_gate_rejects_significant_regression() {
        // 基线 20/20 成功，移植后 14/20：z≈-2.66，统计显著下降。
        let result = serde_json::json!({
            "before": eval_outcomes(20, 20),
            "after": eval_outcomes(14, 20),
        });
        let err = eval_regression_feedback(&result).expect_err("regression must fail gate");
        assert!(err.contains("eval A/B regression"));
        assert!(err.contains("t14") || err.contains("failing tasks"));
    }

    #[test]
    fn eval_gate_passes_without_significant_difference() {
        // 18/20 -> 20/20：z≈1.45，不显著，移植不判回归。
        let result = serde_json::json!({
            "before": eval_outcomes(18, 20),
            "after": eval_outcomes(20, 20),
        });
        assert!(eval_regression_feedback(&result).is_ok());
        // 完全相同（无方差）同样放行。
        let same = serde_json::json!({
            "before": eval_outcomes(20, 20),
            "after": eval_outcomes(20, 20),
        });
        assert!(eval_regression_feedback(&same).is_ok());
    }

    #[test]
    fn parse_release_version_variants() {
        assert_eq!(parse_release_version("v0.5.7"), Some((0, 5, 7)));
        assert_eq!(parse_release_version("v1.2.3-rc.1"), Some((1, 2, 3)));
        assert_eq!(parse_release_version("v10.0.1+build.42"), Some((10, 0, 1)));
        assert_eq!(parse_release_version("v1.2"), None);
        assert_eq!(parse_release_version("promote/chg-1"), None);
        assert_eq!(parse_release_version("not-a-tag"), None);
    }

    #[test]
    fn should_attempt_gate_by_status_and_cooldown() {
        let mut attempts = PortAttempts::default();
        // 无记录 → 尝试。
        assert!(should_attempt(&attempts, "v0.5.8", 86_400));
        // done → 永久跳过。
        attempts.attempts.insert(
            "v0.5.8".into(),
            PortAttempt {
                status: "done".into(),
                at: chrono::Utc::now().to_rfc3339(),
            },
        );
        assert!(!should_attempt(&attempts, "v0.5.8", 86_400));
        // failed 且在冷却期内 → 跳过；冷却已过 → 重试。
        attempts.attempts.insert(
            "v0.5.9".into(),
            PortAttempt {
                status: "failed".into(),
                at: chrono::Utc::now().to_rfc3339(),
            },
        );
        assert!(!should_attempt(&attempts, "v0.5.9", 86_400));
        assert!(should_attempt(&attempts, "v0.5.9", 0));
        // 时间戳不可解析 → 按冷却已过处理（偏保守重试）。
        attempts.attempts.insert(
            "v0.6.0".into(),
            PortAttempt {
                status: "failed".into(),
                at: "not-a-time".into(),
            },
        );
        assert!(should_attempt(&attempts, "v0.6.0", 86_400));
    }

    #[tokio::test]
    async fn semantic_absorption_confirmed_marks_change_absorbed() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-sem", "feature.rs", "fn promoted() {}\n").await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "other.rs",
            "fn other() {}\n",
        )
        .await;

        let orch = Arc::new(
            MockOrchestrator::new(dir.path().to_path_buf())
                .with_absorb_answer(serde_json::json!({"absorbed": true})),
        );
        let porter = porter_for(dir.path()).with_orchestrator(orch.clone());
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].status, AbsorptionStatus::Absorbed);
        assert!(orch
            .task_types()
            .contains(&"baseline_port_absorb_check".to_string()));
    }

    #[tokio::test]
    async fn semantic_absorb_unparseable_result_fails_closed_to_pending() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-sem2", "feature.rs", "fn promoted() {}\n").await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "other.rs",
            "fn other() {}\n",
        )
        .await;

        // 回复不是 {"absorbed": bool}：宁可重复移植也不静默丢弃。
        let orch = Arc::new(
            MockOrchestrator::new(dir.path().to_path_buf())
                .with_absorb_answer(serde_json::json!({"answer": "yes"})),
        );
        let porter = porter_for(dir.path()).with_orchestrator(orch);
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        assert_eq!(plan.items[0].status, AbsorptionStatus::Pending);
    }

    #[tokio::test]
    async fn already_ported_detects_remote_branch_containing_tag() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-ap", "feature.rs", "fn promoted() {}\n").await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "other.rs",
            "fn other() {}\n",
        )
        .await;

        let porter = porter_for(dir.path()).with_instance_id("local");
        // 移植前：无 evol 远端引用。
        assert!(!porter.already_ported("v0.5.8").await);

        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        porter.execute(&plan).await.unwrap();
        // 模拟推送后的远端跟踪引用（execute 无 local remote 时不推）。
        let tip = git(dir.path(), &["rev-parse", "evol/local"]).await;
        git(
            dir.path(),
            &["update-ref", "refs/remotes/local/evol/local", &tip],
        )
        .await;
        assert!(porter.already_ported("v0.5.8").await);
        // 不存在的 tag：merge-base 失败 → 未移植（偏保守）。
        assert!(!porter.already_ported("v9.9.9").await);
    }

    // ========================================================================
    // 移植执行（execute）路径
    //
    // 智能步骤的替身是一个假的编排主流程：resolve 任务按脚本在同一工作树
    // 落地文件或报失败，rework 任务只登记。控制器自身只做 git/cargo 编排，
    // 这与生产架构一致——智能永远经任务进主流程，控制器不碰 LLM。
    // ========================================================================

    /// 模拟主流程 agent 对一轮 resolve 任务的行为。
    enum ResolveAction {
        /// agent 把（相对仓库根的）文件写入工作树后报 Completed。
        Write(Vec<(String, String)>),
        /// 报 Completed 但不改文件（控制器应判"无产出"并进下一轮）。
        CompleteNoChanges,
        /// 任务 Failed。
        Fail,
    }

    struct MockOrchestrator {
        repo_dir: PathBuf,
        tasks: std::sync::Mutex<std::collections::HashMap<String, Task>>,
        task_types: std::sync::Mutex<Vec<String>>,
        rework: std::sync::Mutex<Vec<Task>>,
        resolve_script: std::sync::Mutex<std::collections::VecDeque<ResolveAction>>,
        /// 语义吸收确认任务的固定回复（默认 {"absorbed": false}）。
        absorb_answer: serde_json::Value,
    }

    impl MockOrchestrator {
        fn new(repo_dir: PathBuf) -> Self {
            Self {
                repo_dir,
                tasks: std::sync::Mutex::new(std::collections::HashMap::new()),
                task_types: std::sync::Mutex::new(Vec::new()),
                rework: std::sync::Mutex::new(Vec::new()),
                resolve_script: std::sync::Mutex::new(std::collections::VecDeque::new()),
                absorb_answer: serde_json::json!({"absorbed": false}),
            }
        }

        fn script(self, actions: Vec<ResolveAction>) -> Self {
            *self.resolve_script.lock().unwrap() = actions.into();
            self
        }

        fn with_absorb_answer(mut self, answer: serde_json::Value) -> Self {
            self.absorb_answer = answer;
            self
        }

        fn task_types(&self) -> Vec<String> {
            self.task_types.lock().unwrap().clone()
        }

        fn rework_tasks(&self) -> Vec<Task> {
            self.rework.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl OrchestratorControl for MockOrchestrator {
        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> SFResult<()> {
            Ok(())
        }
        async fn submit_goal_auto(&self, _goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>> {
            let mut ids = Vec::with_capacity(tasks.len());
            for mut task in tasks {
                let type_name = match &task.task_type {
                    TaskType::Custom(name) => name.clone(),
                    other => format!("{other:?}"),
                };
                self.task_types.lock().unwrap().push(type_name.clone());
                if type_name == "baseline_port_resolve" {
                    let action = {
                        let mut script = self.resolve_script.lock().unwrap();
                        script.pop_front().unwrap_or(ResolveAction::Fail)
                    };
                    match action {
                        ResolveAction::Write(files) => {
                            for (rel, content) in files {
                                tokio::fs::write(self.repo_dir.join(&rel), content)
                                    .await
                                    .unwrap();
                            }
                            task.status = TaskStatus::Completed;
                        }
                        ResolveAction::CompleteNoChanges => {
                            task.status = TaskStatus::Completed;
                        }
                        ResolveAction::Fail => {
                            task.status = TaskStatus::Failed;
                            task.error = Some("mock resolve failure".into());
                        }
                    }
                } else if type_name == "baseline_port_rework" {
                    task.status = TaskStatus::Completed;
                    self.rework.lock().unwrap().push(task.clone());
                } else if type_name == "baseline_port_absorb_check" {
                    task.status = TaskStatus::Completed;
                    task.result = Some(self.absorb_answer.clone());
                }
                ids.push(task.id.clone());
                self.tasks.lock().unwrap().insert(task.id.clone(), task);
            }
            Ok(ids)
        }
        async fn assign_task(&self, _t: &str, _a: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn add_task(&self, _t: Task) -> SFResult<()> {
            unimplemented!()
        }
        async fn crew_can_retry(&self, _ids: &[String]) -> bool {
            false
        }
        async fn crew_retry_all(&self, _ids: &[String]) -> usize {
            0
        }
        async fn get_ready_tasks(&self) -> Vec<Task> {
            vec![]
        }
        async fn get_all_tasks(&self) -> Vec<Task> {
            vec![]
        }
        async fn push_to_dlq(&self, _t: &str, _e: String) -> SFResult<bool> {
            unimplemented!()
        }
        async fn retry_task(&self, _t: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn dlq_len(&self) -> SFResult<usize> {
            unimplemented!()
        }
        async fn start_task(&self, _t: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn complete_task(&self, _t: &str, _r: serde_json::Value) -> SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn fail_task(&self, _t: &str, _e: String) -> SFResult<(bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn cancel_task(&self, _t: &str) -> SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn get_task(&self, task_id: &str) -> Option<Task> {
            self.tasks.lock().unwrap().get(task_id).cloned()
        }
        async fn schedule_task(&self, _t: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            vec![]
        }
        async fn get_dependents(&self, _t: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_dependencies(&self, _t: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
            (vec![], vec![])
        }
        async fn delete_task(&self, _t: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn all_completed(&self) -> bool {
            true
        }
        async fn replay_dlq(&self, _t: &str) -> SFResult<bool> {
            unimplemented!()
        }
    }

    /// 测试用 porter：关 cargo 质量门（临时仓库无 Rust workspace）、短轮询。
    fn porter_for(dir: &Path) -> BaselinePorter {
        BaselinePorter::new(dir)
            .without_quality_gate()
            .with_resolve_timeout(30, 1)
    }

    #[tokio::test]
    async fn execute_empty_plan_is_clean_reset_without_branch_or_tag() {
        let dir = setup_repo().await;
        upstream_release(dir.path(), "v0.5.7", "v0.5.8", "up.rs", "fn up() {}\n").await;
        let porter = porter_for(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        assert!(plan.items.is_empty());

        let report = porter.execute(&plan).await.unwrap();
        assert!(report.clean_reset);
        assert!(report.branch.is_none());
        assert!(report.gen_tag.is_none());
        assert!(report.items.is_empty());
    }

    #[tokio::test]
    async fn execute_ports_clean_cherry_pick_and_tags_generation() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-clean", "feature.rs", "fn promoted() {}\n").await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "other.rs",
            "fn other() {}\n",
        )
        .await;

        let porter = porter_for(dir.path()).with_instance_id("local");
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        let report = porter.execute(&plan).await.unwrap();

        assert!(!report.clean_reset);
        assert_eq!(report.branch.as_deref(), Some("evol/local"));
        assert_eq!(report.gen_tag.as_deref(), Some("gen-1"));
        assert_eq!(report.items.len(), 1);
        match &report.items[0].outcome {
            PortOutcome::Ported {
                route: PortRoute::CherryPicked,
                ..
            } => {}
            other => panic!("expected CherryPicked, got {other:?}"),
        }
        // 分支与代际 tag 真实落地。
        assert!(git(dir.path(), &["branch", "--list", "evol/local"])
            .await
            .contains("evol/local"));
        assert!(git(dir.path(), &["tag", "--list", "gen-*"])
            .await
            .contains("gen-1"));
        // 移植内容在新分支工作树上。
        let f = tokio::fs::read_to_string(dir.path().join("feature.rs"))
            .await
            .unwrap();
        assert!(f.contains("fn promoted"));
    }

    #[tokio::test]
    async fn execute_conflict_resolved_by_main_loop_agent() {
        let dir = setup_repo().await;
        // 私版与上游都改 lib.rs 同一位置 → cherry-pick 必冲突。
        make_promoted(
            dir.path(),
            "chg-conflict",
            "lib.rs",
            "fn v1() {}\nfn promoted() {}\n",
        )
        .await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "lib.rs",
            "fn v1() {}\nfn upstream() {}\n",
        )
        .await;

        // 主流程 agent 一轮解决：写出双方意图合并后的文件。
        let orch = Arc::new(MockOrchestrator::new(dir.path().to_path_buf()).script(vec![
            ResolveAction::Write(vec![(
                "lib.rs".into(),
                "fn v1() {}\nfn upstream() {}\nfn promoted() {}\n".into(),
            )]),
        ]));
        let porter = porter_for(dir.path())
            .with_orchestrator(orch.clone())
            .with_instance_id("alice");
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        let report = porter.execute(&plan).await.unwrap();

        assert_eq!(report.branch.as_deref(), Some("evol/alice"));
        match &report.items[0].outcome {
            PortOutcome::Ported {
                route: PortRoute::AgentResolved { round },
                ..
            } => assert_eq!(*round, 1),
            other => panic!("expected AgentResolved round 1, got {other:?}"),
        }
        // agent 的解决结果真的落进了移植 commit。
        let f = tokio::fs::read_to_string(dir.path().join("lib.rs"))
            .await
            .unwrap();
        assert!(f.contains("fn upstream"));
        assert!(f.contains("fn promoted"));
        // 智能步骤走的是主流程任务通道。
        assert!(orch
            .task_types()
            .contains(&"baseline_port_resolve".to_string()));
    }

    #[tokio::test]
    async fn execute_exhausted_change_reflows_as_rework_but_gen_tag_still_cut() {
        let dir = setup_repo().await;
        make_promoted(
            dir.path(),
            "chg-hard",
            "lib.rs",
            "fn v1() {}\nfn promoted() {}\n",
        )
        .await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "lib.rs",
            "fn v1() {}\nfn upstream() {}\n",
        )
        .await;

        // 空脚本：三轮 resolve 全部 Failed。
        let orch = Arc::new(MockOrchestrator::new(dir.path().to_path_buf()));
        let porter = porter_for(dir.path()).with_orchestrator(orch.clone());
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        let report = porter.execute(&plan).await.unwrap();

        assert_eq!(report.needs_rework().count(), 1);
        assert_eq!(report.ported().count(), 0);
        // 代际 tag 照打：单条失败不阻塞收口。
        assert_eq!(report.gen_tag.as_deref(), Some("gen-1"));
        // 失败变更已回退到新基线，工作树无残留。
        assert_eq!(
            git(dir.path(), &["rev-parse", "HEAD"]).await,
            git(dir.path(), &["rev-parse", "v0.5.8"]).await
        );
        // 回流任务进主流程，且标记为全进化回路重新生成。
        let rework = orch.rework_tasks();
        assert_eq!(rework.len(), 1);
        assert!(matches!(
            &rework[0].task_type,
            TaskType::Custom(n) if n == "baseline_port_rework"
        ));
        assert_eq!(rework[0].input["evolution_mode"], "generate_change");
        assert_eq!(rework[0].input["change_id"], "chg-hard");
    }

    #[tokio::test]
    async fn execute_agent_reporting_done_without_changes_retries_next_round() {
        let dir = setup_repo().await;
        make_promoted(
            dir.path(),
            "chg-lazy",
            "lib.rs",
            "fn v1() {}\nfn promoted() {}\n",
        )
        .await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "lib.rs",
            "fn v1() {}\nfn upstream() {}\n",
        )
        .await;

        // 第一轮 agent 空报完成（无改动），第二轮才真正落地。
        let orch = Arc::new(MockOrchestrator::new(dir.path().to_path_buf()).script(vec![
            ResolveAction::CompleteNoChanges,
            ResolveAction::Write(vec![(
                "lib.rs".into(),
                "fn v1() {}\nfn upstream() {}\nfn promoted() {}\n".into(),
            )]),
        ]));
        let porter = porter_for(dir.path()).with_orchestrator(orch.clone());
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        let report = porter.execute(&plan).await.unwrap();

        match &report.items[0].outcome {
            PortOutcome::Ported {
                route: PortRoute::AgentResolved { round },
                ..
            } => assert_eq!(*round, 2),
            other => panic!("expected AgentResolved round 2, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_generation_tag_increments_over_existing_gens() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-gen", "feature.rs", "fn promoted() {}\n").await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "other.rs",
            "fn other() {}\n",
        )
        .await;
        // 预置一代。
        git(dir.path(), &["tag", "-a", "-m", "gen=1", "gen-1", "v0.5.8"]).await;

        let porter = porter_for(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        let report = porter.execute(&plan).await.unwrap();
        assert_eq!(report.gen_tag.as_deref(), Some("gen-2"));
    }

    #[tokio::test]
    async fn execute_skips_absorbed_and_ports_pending() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-a", "a.rs", "fn a() {}\n").await;
        make_promoted(dir.path(), "chg-b", "b.rs", "fn b() {}\n").await;
        // 上游吸收 chg-a（cherry-pick），chg-b 上游没有。
        git(dir.path(), &["checkout", "-q", "v0.5.7"]).await;
        git(dir.path(), &["cherry-pick", "promote/chg-a"]).await;
        tokio::fs::write(dir.path().join("up.rs"), "fn up() {}\n")
            .await
            .unwrap();
        git(dir.path(), &["add", "."]).await;
        git(dir.path(), &["commit", "-m", "upstream v0.5.8"]).await;
        git(dir.path(), &["tag", "v0.5.8"]).await;

        let porter = porter_for(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        let report = porter.execute(&plan).await.unwrap();

        let by_id: std::collections::HashMap<&str, &PortOutcome> = report
            .items
            .iter()
            .map(|i| (i.change_id.as_str(), &i.outcome))
            .collect();
        assert!(matches!(by_id["chg-a"], PortOutcome::Absorbed));
        assert!(matches!(by_id["chg-b"], PortOutcome::Ported { .. }));
        // 两条变更的内容都在新基线上（a 随上游带入，b 本次移植）。
        assert!(tokio::fs::try_exists(dir.path().join("a.rs"))
            .await
            .unwrap());
        assert!(tokio::fs::try_exists(dir.path().join("b.rs"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn execute_without_orchestrator_dumps_rework_to_file_on_conflict() {
        let td = tempfile::tempdir().unwrap();
        let repo = td.path().join("work");
        tokio::fs::create_dir(&repo).await.unwrap();
        git(&repo, &["init"]).await;
        git(&repo, &["config", "user.email", "test@test.com"]).await;
        git(&repo, &["config", "user.name", "Test"]).await;
        tokio::fs::write(repo.join("lib.rs"), "fn v1() {}\n")
            .await
            .unwrap();
        git(&repo, &["add", "."]).await;
        git(&repo, &["commit", "-m", "initial"]).await;
        git(&repo, &["tag", "v0.5.7"]).await;
        // 冲突型晋级变更。
        tokio::fs::write(repo.join("lib.rs"), "fn v1() {}\nfn promoted() {}\n")
            .await
            .unwrap();
        git(&repo, &["add", "."]).await;
        git(&repo, &["commit", "-m", "change chg-dump"]).await;
        git(
            &repo,
            &[
                "tag",
                "-a",
                "-m",
                "change_id=chg-dump\nlevel=l1_rollout\neval=Adopt z=2.3",
                "promote/chg-dump",
            ],
        )
        .await;
        // 上游同位置不同改动。
        git(&repo, &["checkout", "-q", "v0.5.7"]).await;
        tokio::fs::write(repo.join("lib.rs"), "fn v1() {}\nfn upstream() {}\n")
            .await
            .unwrap();
        git(&repo, &["add", "."]).await;
        git(&repo, &["commit", "-m", "upstream v0.5.8"]).await;
        git(&repo, &["tag", "v0.5.8"]).await;

        // 无编排器：冲突无法智能解决，回流必须落文件兜底。
        let porter = BaselinePorter::new(&repo).without_quality_gate();
        let plan = porter.plan("0.5.7").await.unwrap().unwrap();
        let report = porter.execute(&plan).await.unwrap();
        assert_eq!(report.needs_rework().count(), 1);

        // 文件名经 sanitize_ref：点号转横线。
        let dump = td.path().join("port-rework").join("chg-dump-v0-5-8.json");
        let body = tokio::fs::read_to_string(&dump).await.unwrap();
        assert!(body.contains("chg-dump"));
        assert!(body.contains("v0.5.8"));
    }
}
