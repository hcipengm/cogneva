//! GitOps 拉取端。
//!
//! 每个集群各跑一个（含主集群），周期 poll 中央仓库 release 分支：
//!
//! ```text
//! poll 中央仓库 → 发现新 HEAD
//!   → 找 HEAD 上的 promote/* tag，读 tag message（level / change_id）
//!   → 台账幂等：本集群已处理过该 change 则跳过
//!   → L0（l0_config）：提取变化的配置文件
//!       deploy/k3s/cogneva-json-configmap.yaml → kubectl apply（ConfigWatcher 热更新）
//!       prompts/** → 重建 prompts configmap → kubectl apply（hot_reload 热更新）
//!   → L1（l1_rollout）：拉取镜像（推送端已 push 到外部仓库或集群内
//!       registry，拉取端只产出引用，绝不本地构建）→ 金丝雀发布
//!       set image + rollout pause（新副本先起，旧副本不动）
//!       → 看护（readiness + restart count + 可选 metrics URL 阈值比对）
//!       → 通过：rollout resume 全量；异常：rollout undo 回滚 + 熔断计数
//!   → 全程写本集群台账（cluster 字段区分集群）
//! ```
//!
//! 每个集群的晋级节奏、看护、回滚、熔断都是本地决策——一个集群
//! 金丝雀失败只影响自己，不影响其他集群。

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::GitOpsConfig;
use cog_core::{
    PromotionLedger, PromotionRecord, PromotionStatus, SFError, SFResult,
    COUNTER_SEMANTICS_CUMULATIVE, COUNTER_SEMANTICS_MARKER,
};
use tracing::{debug, info, warn};

/// 一次待处理的晋级（从 release 分支 HEAD + promote tag 解析出来）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionCandidate {
    pub change_id: String,
    pub level: String,
    pub commit: String,
    pub eval_summary: Option<String>,
}

/// 解析 promote tag 的 message（`change_id=..\nlevel=..\neval=..`）。
pub fn parse_tag_message(message: &str) -> (Option<String>, Option<String>, Option<String>) {
    let mut change_id = None;
    let mut level = None;
    let mut eval_summary = None;
    for line in message.lines() {
        if let Some((k, v)) = line.split_once('=') {
            match k.trim() {
                "change_id" => change_id = Some(v.trim().to_string()),
                "level" => level = Some(v.trim().to_string()),
                "eval" => {
                    let v = v.trim();
                    eval_summary = (v != "none").then(|| v.to_string());
                }
                _ => {}
            }
        }
    }
    (change_id, level, eval_summary)
}

pub struct GitOpsPuller {
    config: GitOpsConfig,
    ledger: Arc<dyn PromotionLedger>,
    /// 本集群标识（台账 cluster 字段）。
    cluster: String,
    /// 可选 metrics 抓取地址（配置了才做指标阈值比对看护）。
    metrics_url: Option<String>,
}

impl GitOpsPuller {
    pub fn new(config: GitOpsConfig, ledger: Arc<dyn PromotionLedger>, cluster: String) -> Self {
        Self {
            config,
            ledger,
            cluster,
            metrics_url: None,
        }
    }

    pub fn with_metrics_url(mut self, url: Option<String>) -> Self {
        self.metrics_url = url;
        self
    }

    async fn run(
        &self,
        program: &str,
        args: &[&str],
        dir: Option<&Path>,
        timeout_secs: u64,
    ) -> SFResult<String> {
        let cmdline = format!("{} {}", program, args.join(" "));
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args).kill_on_drop(true);
        if let Some(d) = dir {
            cmd.current_dir(d);
        }
        let fut = cmd.output();
        let output = tokio::time::timeout(Duration::from_secs(timeout_secs), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after {timeout_secs}s")))?
            .map_err(|e| SFError::IO(format!("failed to run {program}: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!("{cmdline} failed: {stderr}")));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn work_dir(&self) -> PathBuf {
        PathBuf::from(&self.config.work_dir)
    }

    /// 确保本地有 release 分支的最新 checkout，返回远端 HEAD commit。
    /// release 分支尚不存在（首次晋级前）时返回空串——调用方按"无候选"
    /// 处理，不把正常空窗期刷成 warn 日志。
    async fn sync_repo(&self) -> SFResult<String> {
        let dir = self.work_dir();
        if !dir.join(".git").exists() {
            let probe = self
                .run(
                    "git",
                    &[
                        "ls-remote",
                        "--heads",
                        &self.config.repo_url,
                        &self.config.branch,
                    ],
                    None,
                    60,
                )
                .await?;
            if probe.is_empty() {
                return Ok(String::new());
            }
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| SFError::IO(format!("create work_dir: {e}")))?;
            self.run(
                "git",
                &[
                    "clone",
                    "--branch",
                    &self.config.branch,
                    "--single-branch",
                    &self.config.repo_url,
                    ".",
                ],
                Some(&dir),
                300,
            )
            .await?;
        } else {
            self.run(
                "git",
                &["fetch", "origin", &self.config.branch],
                Some(&dir),
                120,
            )
            .await?;
            self.run(
                "git",
                &["reset", "--hard", &format!("origin/{}", self.config.branch)],
                Some(&dir),
                60,
            )
            .await?;
        }
        // tag 也要拉（promote tag 带 level 元数据）。
        self.run("git", &["fetch", "--tags", "--force"], Some(&dir), 120)
            .await?;
        self.run("git", &["rev-parse", "HEAD"], Some(&dir), 30)
            .await
    }

    /// 从 HEAD 上的 promote tag 解析晋级候选；无 tag 返回 None。
    pub async fn candidate_at_head(&self) -> SFResult<Option<PromotionCandidate>> {
        let dir = self.work_dir();
        let head = self
            .run("git", &["rev-parse", "HEAD"], Some(&dir), 30)
            .await?;
        let tags = self
            .run("git", &["tag", "--points-at", &head], Some(&dir), 30)
            .await?;
        let Some(tag) = tags.lines().find(|t| t.starts_with("promote/")) else {
            return Ok(None);
        };
        let message = self
            .run(
                "git",
                &["tag", "-l", "--format=%(contents)", tag],
                Some(&dir),
                30,
            )
            .await?;
        let (change_id, level, eval_summary) = parse_tag_message(&message);
        match (change_id, level) {
            (Some(change_id), Some(level)) => Ok(Some(PromotionCandidate {
                change_id,
                level,
                commit: head,
                eval_summary,
            })),
            _ => {
                warn!(tag = %tag, "promote tag missing change_id/level metadata; skipping");
                Ok(None)
            }
        }
    }

    /// 幂等：本集群是否已处理过该 change。RolledBack 也算已处理——
    /// 已回滚的 change 绝不能被下个 poll 周期重复金丝雀（会反复打挂
    /// 集群）；Failed 不在列，瞬时失败允许下轮重试。
    async fn already_processed(&self, change_id: &str) -> SFResult<bool> {
        let recent = self.ledger.recent(50).await?;
        Ok(recent.iter().any(|r| {
            r.change_id == *change_id
                && r.cluster == self.cluster
                && matches!(
                    r.status,
                    PromotionStatus::Promoted
                        | PromotionStatus::Pending
                        | PromotionStatus::AwaitingApproval
                        | PromotionStatus::RolledBack
                )
        }))
    }

    /// Pending 尾巴自愈。上一个 puller 进程在金丝雀中途被自家滚动
    /// 替换时，台账停在 Pending：新 puller 的 already_processed 把它
    /// 当"已处理"跳过，候选永远不会终结。这里对超过看护+滚动窗口
    /// （另一个活着的 puller 一定已完结）的 Pending 做幂等收尾：
    /// L0 补做 apply（幂等）；L1 镜像已是目标则按 rollout 现状补记
    /// Promoted 或回滚，镜像还是旧版则清理并记 RolledBack。
    /// 返回 true 表示本候选已终结，本周期不再处理。
    async fn reconcile_pending(&self, candidate: &PromotionCandidate) -> SFResult<bool> {
        let recent = self.ledger.recent(50).await?;
        let Some(rec) = recent.iter().find(|r| {
            r.change_id == candidate.change_id
                && r.cluster == self.cluster
                && r.status == PromotionStatus::Pending
        }) else {
            return Ok(false);
        };
        // 并发护栏：另一副本里活着的 puller 可能正在跑这条金丝雀，
        // 看护+滚动窗口内绝不插手。
        let age = (chrono::Utc::now() - rec.updated_at).num_seconds().max(0) as u64;
        let guard = self.config.canary_watch_secs + 300;
        if age < guard {
            return Ok(false);
        }
        info!(
            change_id = %candidate.change_id,
            cluster = %self.cluster,
            age_secs = age,
            "Reconciling orphaned Pending promotion"
        );

        if candidate.level == "l0_config" {
            let note = self.apply_config(candidate).await?;
            self.ledger
                .update_status(
                    &rec.id,
                    PromotionStatus::Promoted,
                    &format!("finalized after puller restart: {note}"),
                )
                .await?;
            return Ok(true);
        }

        // L1 源码模式：镜像构建昂贵，标 Failed 让下轮 poll 完整重试。
        if self.config.registry.is_none() {
            self.ledger
                .update_status(
                    &rec.id,
                    PromotionStatus::Failed,
                    "puller restarted mid-canary; retry next poll",
                )
                .await?;
            return Ok(true);
        }

        let expected = self.obtain_image(candidate).await?;
        let current = self.current_deployment_image().await?;
        let d = self.config.deployment.clone();
        let n = self.config.namespace.clone();
        let k = self.config.kubectl_bin.clone();
        if current == expected {
            let status = self
                .run(
                    &k,
                    &[
                        "-n",
                        &n,
                        "rollout",
                        "status",
                        &format!("deployment/{d}"),
                        "--timeout",
                        "20s",
                    ],
                    None,
                    40,
                )
                .await;
            match status {
                Ok(_) => {
                    self.ledger
                        .update_status(
                            &rec.id,
                            PromotionStatus::Promoted,
                            "finalized after puller restart: rollout already complete",
                        )
                        .await?;
                }
                Err(e) => {
                    // 镜像已换但滚动卡死（拉不到镜像/副本不起）：回滚。
                    // resume 先于 undo（paused 部署 undo 会被 kubectl 拒绝）。
                    warn!(error = %e, "orphaned canary stuck; rolling back");
                    let _ = self
                        .run(
                            &k,
                            &["-n", &n, "rollout", "resume", &format!("deployment/{d}")],
                            None,
                            60,
                        )
                        .await;
                    let _ = self
                        .run(
                            &k,
                            &["-n", &n, "rollout", "undo", &format!("deployment/{d}")],
                            None,
                            120,
                        )
                        .await;
                    self.ledger
                        .update_status(
                            &rec.id,
                            PromotionStatus::RolledBack,
                            &format!("orphaned canary rolled back after puller restart: {e}"),
                        )
                        .await?;
                }
            }
        } else if self.deployment_paused().await.unwrap_or(false) {
            // 镜像不是目标且部署仍 paused：金丝雀未落地或已被 undo，
            // 是 puller 中途死亡留下的现场——resume 先于 undo（paused
            // 部署 undo 会被 kubectl 拒绝）清理。
            let _ = self
                .run(
                    &k,
                    &["-n", &n, "rollout", "resume", &format!("deployment/{d}")],
                    None,
                    60,
                )
                .await;
            let _ = self
                .run(
                    &k,
                    &["-n", &n, "rollout", "undo", &format!("deployment/{d}")],
                    None,
                    120,
                )
                .await;
            self.ledger
                .update_status(
                    &rec.id,
                    PromotionStatus::RolledBack,
                    "orphaned canary cleaned up after puller restart",
                )
                .await?;
        } else {
            // 镜像不是目标且部署未 paused：晋级之后又有新的滚动（正常
            // 换版/人工处置），世界已向前——绝不能 undo（会把合法的新
            // 部署回退掉），只把台账尾巴终结掉。
            self.ledger
                .update_status(
                    &rec.id,
                    PromotionStatus::RolledBack,
                    "superseded by a newer rollout after puller restart",
                )
                .await?;
        }
        Ok(true)
    }

    /// 部署是否处于 rollout pause 状态（金丝雀中途死亡的现场特征）。
    async fn deployment_paused(&self) -> SFResult<bool> {
        let out = self
            .run(
                &self.config.kubectl_bin.clone(),
                &[
                    "-n",
                    &self.config.namespace,
                    "get",
                    "deployment",
                    &self.config.deployment,
                    "-o",
                    "jsonpath={.spec.paused}",
                ],
                None,
                30,
            )
            .await?;
        Ok(out.trim() == "true")
    }

    /// 全量滚动是否完成：observedGeneration 追上 generation 且更新后
    /// 副本数与就绪副本数都达到期望。短查询（相对 rollout status 长等）
    /// 不怕 puller 所在旧副本被杀——每次调用都是独立命令。
    async fn rollout_complete(&self) -> SFResult<bool> {
        let out = self
            .run(
                &self.config.kubectl_bin.clone(),
                &[
                    "-n",
                    &self.config.namespace,
                    "get",
                    "deployment",
                    &self.config.deployment,
                    "-o",
                    "jsonpath={.metadata.generation} {.status.observedGeneration} {.spec.replicas} {.status.updatedReplicas} {.status.readyReplicas}",
                ],
                None,
                30,
            )
            .await?;
        let parts: Vec<&str> = out.split_whitespace().collect();
        if parts.len() < 5 {
            return Ok(false);
        }
        let gen: u64 = parts[0].parse().unwrap_or(0);
        let obs: u64 = parts[1].parse().unwrap_or(0);
        let spec: u32 = parts[2].parse().unwrap_or(0);
        let updated: u32 = parts[3].parse().unwrap_or(0);
        let ready: u32 = parts[4].parse().unwrap_or(0);
        Ok(obs >= gen && gen > 0 && updated == spec && ready == spec)
    }

    /// 当前部署在跑的容器镜像。
    async fn current_deployment_image(&self) -> SFResult<String> {
        self.run(
            &self.config.kubectl_bin.clone(),
            &[
                "-n",
                &self.config.namespace,
                "get",
                "deployment",
                &self.config.deployment,
                "-o",
                &format!(
                    "jsonpath={{.spec.template.spec.containers[?(@.name==\"{}\")].image}}",
                    self.config.container
                ),
            ],
            None,
            30,
        )
        .await
    }

    /// 一轮拉取。返回是否有新晋级被处理。
    pub async fn poll_once(&self) -> SFResult<bool> {
        let head = self.sync_repo().await?;
        if head.is_empty() {
            // release 分支尚未建立（首次晋级前的正常空窗期）。
            return Ok(false);
        }
        let Some(candidate) = self.candidate_at_head().await? else {
            return Ok(false);
        };
        if self.already_processed(&candidate.change_id).await? {
            // Pending 尾巴自愈：上一个 puller 进程可能在金丝雀中途被自家
            // 滚动替换，台账停在 Pending 永远不会终结（2026-08-07 双集群
            // 实测）。超过看护+滚动窗口的 Pending 记录做幂等 reconcile。
            if self.reconcile_pending(&candidate).await? {
                return Ok(true);
            }
            info!(change_id = %candidate.change_id, cluster = %self.cluster, "Promotion already processed by this cluster");
            return Ok(false);
        }

        info!(
            change_id = %candidate.change_id,
            level = %candidate.level,
            cluster = %self.cluster,
            "New promotion candidate pulled"
        );

        let record_id = self
            .record(
                &candidate,
                PromotionStatus::Pending,
                "pulled from release branch",
            )
            .await?;

        let result = if candidate.level == "l0_config" {
            self.apply_config(&candidate).await
        } else {
            self.canary_rollout(&candidate).await
        };

        match result {
            Ok(note) => {
                self.ledger
                    .update_status(&record_id, PromotionStatus::Promoted, &note)
                    .await?;
                info!(change_id = %candidate.change_id, cluster = %self.cluster, "Promotion applied: {note}");
            }
            Err(e) => {
                // canary_rollout 内部区分回滚与执行失败；这里统一记失败，
                // 回滚情形已在 canary 内部把状态改成 RolledBack。
                let recent = self.ledger.recent(1).await?;
                let already_marked = recent
                    .first()
                    .map(|r| r.id == record_id && r.status == PromotionStatus::RolledBack)
                    .unwrap_or(false);
                if !already_marked {
                    self.ledger
                        .update_status(&record_id, PromotionStatus::Failed, &e.to_string())
                        .await?;
                }
                warn!(change_id = %candidate.change_id, cluster = %self.cluster, error = %e, "Promotion failed");
                return Err(e);
            }
        }
        Ok(true)
    }

    /// L0：提取 commit 中变化的配置文件并热应用。
    async fn apply_config(&self, candidate: &PromotionCandidate) -> SFResult<String> {
        let dir = self.work_dir();
        let changed = self
            .run(
                "git",
                &["diff", "--name-only", "HEAD~1", "HEAD"],
                Some(&dir),
                30,
            )
            .await?;

        let mut applied = Vec::new();
        let mut prompts_touched = false;
        for file in changed.lines() {
            if file == "deploy/k3s/cogneva-json-configmap.yaml" {
                let content = self
                    .run("git", &["show", &format!("HEAD:{file}")], Some(&dir), 30)
                    .await?;
                self.kubectl_apply_stdin(&content).await?;
                applied.push(file.to_string());
            } else if file.starts_with("prompts/") {
                prompts_touched = true;
            }
        }

        if prompts_touched {
            applied.push(self.rebuild_prompts_configmap().await?);
        }

        if applied.is_empty() {
            return Err(SFError::Validation(format!(
                "L0 commit {} 未触及任何配置路径",
                candidate.commit
            )));
        }
        Ok(format!("config applied: {}", applied.join(", ")))
    }

    /// prompts/ 全量重建 cogneva-prompts configmap（挂载进主 Pod，
    /// hot_reload watcher 捕捉 configmap 交换热更新）。
    async fn rebuild_prompts_configmap(&self) -> SFResult<String> {
        let dir = self.work_dir();
        let staging = dir.join(".prompts-staging");
        let _ = tokio::fs::remove_dir_all(&staging).await;
        tokio::fs::create_dir_all(&staging)
            .await
            .map_err(|e| SFError::IO(format!("create prompts staging: {e}")))?;
        let listed = self
            .run(
                "git",
                &["ls-tree", "-r", "--name-only", "HEAD", "prompts/"],
                Some(&dir),
                30,
            )
            .await?;
        let mut count = 0usize;
        for rel in listed.lines() {
            let content = self
                .run("git", &["show", &format!("HEAD:{rel}")], Some(&dir), 30)
                .await?;
            let dest = staging.join(rel.trim_start_matches("prompts/"));
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| SFError::IO(format!("create prompts subdir: {e}")))?;
            }
            tokio::fs::write(&dest, content)
                .await
                .map_err(|e| SFError::IO(format!("write prompts staging: {e}")))?;
            count += 1;
        }
        let create = self
            .run(
                &self.config.kubectl_bin.clone(),
                &[
                    "-n",
                    &self.config.namespace,
                    "create",
                    "configmap",
                    "cogneva-prompts",
                    &format!("--from-file={}", staging.display()),
                    "--dry-run=client",
                    "-o",
                    "yaml",
                ],
                None,
                60,
            )
            .await?;
        let applied = self.kubectl_apply_stdin(&create).await;
        let _ = tokio::fs::remove_dir_all(&staging).await;
        applied?;
        Ok(format!("prompts/ configmap rebuilt ({count} files)"))
    }

    async fn kubectl_apply_stdin(&self, yaml: &str) -> SFResult<()> {
        let mut cmd = tokio::process::Command::new(&self.config.kubectl_bin);
        cmd.args(["apply", "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .map_err(|e| SFError::IO(format!("spawn kubectl apply: {e}")))?;
        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin
                .write_all(yaml.as_bytes())
                .await
                .map_err(|e| SFError::IO(format!("write kubectl stdin: {e}")))?;
        }
        let output = child
            .wait_with_output()
            .await
            .map_err(|e| SFError::IO(format!("kubectl apply: {e}")))?;
        if !output.status.success() {
            return Err(SFError::IO(format!(
                "kubectl apply failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }

    /// L1：金丝雀发布。pause → 看护 → resume / undo。
    async fn canary_rollout(&self, candidate: &PromotionCandidate) -> SFResult<String> {
        let image = self.obtain_image(candidate).await?;
        let d = &self.config.deployment;
        let c = &self.config.container;
        let n = self.config.namespace.clone();
        let k = self.config.kubectl_bin.clone();

        // 金丝雀节奏：maxSurge=1,maxUnavailable=0 → 新副本先起、旧副本不动。
        self.run(
            &k,
            &[
                "-n",
                &n,
                "change",
                "deployment",
                d,
                "--type",
                "merge",
                "-p",
                r#"{"spec":{"strategy":{"rollingUpdate":{"maxSurge":1,"maxUnavailable":0}}}}"#,
            ],
            None,
            60,
        )
        .await?;
        // 换镜像之前记下本部署已在跑的副本。此后新出现的副本才是候选，
        // 看护要观测的是它。拿不到这份名单就整轮关掉指标闸门（readiness 与
        // restart 仍在看），而不是把新旧混作一组自比自。
        let pre_rollout = match self.pre_rollout_pods().await {
            Some(set) => Some(set),
            None => {
                warn!("could not list pre-rollout pods; canary metrics gate disabled");
                None
            }
        };
        self.run(
            &k,
            &[
                "-n",
                &n,
                "set",
                "image",
                &format!("deployment/{d}"),
                &format!("{c}={image}"),
            ],
            None,
            60,
        )
        .await?;
        // 起第一个新副本后立即暂停，进入看护。"already paused" 容忍：
        // 上一个 puller 在金丝雀中途死亡会留下 paused 部署（孤儿现场），
        // 此时本次金丝雀就是恢复路径，不能因为 pause 报错卡死重试循环
        // （2026-08-07 cluster-2 实测）。
        if let Err(e) = self
            .run(
                &k,
                &["-n", &n, "rollout", "pause", &format!("deployment/{d}")],
                None,
                60,
            )
            .await
        {
            if !e.to_string().contains("already paused") {
                return Err(e);
            }
            info!("deployment already paused (orphaned canary scene); continuing");
        }

        let watch = self.watch_canary(pre_rollout.as_ref()).await;
        match watch {
            Ok(coverage) => {
                if let Err(e) = self
                    .run(
                        &k,
                        &["-n", &n, "rollout", "resume", &format!("deployment/{d}")],
                        None,
                        60,
                    )
                    .await
                {
                    if !e.to_string().contains("is not paused") {
                        return Err(e);
                    }
                }
                // 等全量滚动完成。不用 `rollout status --timeout`：puller
                // 跑在被滚动的部署里，长连接命令会随旧副本被杀而失败，
                // 把成功的滚动误判成失败进而 undo 掉好版本。改为短查询
                // 轮询完成条件；puller 中途被杀则台账留 Pending，由
                // reconcile_pending 收尾。
                let rollout_deadline = std::time::Instant::now()
                    + Duration::from_secs(self.config.canary_watch_secs.max(300));
                let mut rolled_out = false;
                while std::time::Instant::now() < rollout_deadline {
                    if self.rollout_complete().await.unwrap_or(false) {
                        rolled_out = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
                if !rolled_out {
                    // 看护通过但全量滚动失败（新副本后续拉不到镜像/不起）：
                    // 与看护失败同等处理——回滚 + RolledBack。否则候选记
                    // Failed 下轮 poll 会反复金丝雀（重试风暴），部署也
                    // 停在坏镜像上（2026-08-07 cluster-2 实测）。
                    // resume 先于 undo（paused 部署 undo 会被 kubectl 拒绝）。
                    let e = SFError::Agent("rollout did not complete in time".into());
                    warn!(error = %e, "rollout after canary failed; rolling back");
                    let _ = self
                        .run(
                            &k,
                            &["-n", &n, "rollout", "resume", &format!("deployment/{d}")],
                            None,
                            60,
                        )
                        .await;
                    let _ = self
                        .run(
                            &k,
                            &["-n", &n, "rollout", "undo", &format!("deployment/{d}")],
                            None,
                            120,
                        )
                        .await;
                    self.mark_rolled_back(candidate, &format!("rollout after canary: {e}"))
                        .await;
                    return Err(e);
                }
                // 镜像换了但 prompts configmap 不重建的话，挂载的旧
                // prompts 会遮蔽新镜像里的更新——本提交触及 prompts/
                // 时随金丝雀成功一并重建。
                // 放行的话里必须带上这轮判据的覆盖情况：只写「canary passed」，
                // 读台账的人会把它读成「延迟与错误率都验过了」，而取不到数时
                // 那条闸门整轮都在放行，结论长得一模一样。
                let mut note = format!("canary passed; {}; rolled out {image}", coverage.summary());
                if self.commit_touches("prompts/").await? {
                    note.push_str(&format!("; {}", self.rebuild_prompts_configmap().await?));
                }
                Ok(note)
            }
            Err(e) => {
                warn!(error = %e, "canary watch failed; rolling back");
                // resume 必须先于 undo：kubectl 拒绝对 paused 部署 undo
                // （"you cannot rollback a paused deployment"），先 undo 会
                // 静默失败、resume 后反而把坏镜像全量滚出（2026-08-07
                // cluster-2 实测）。未 pause 时 resume 报错忽略即可。
                let _ = self
                    .run(
                        &k,
                        &["-n", &n, "rollout", "resume", &format!("deployment/{d}")],
                        None,
                        60,
                    )
                    .await;
                let _ = self
                    .run(
                        &k,
                        &["-n", &n, "rollout", "undo", &format!("deployment/{d}")],
                        None,
                        120,
                    )
                    .await;
                self.mark_rolled_back(candidate, &format!("canary regression: {e}"))
                    .await;
                Err(e)
            }
        }
    }

    /// 回滚情形台账记 RolledBack（poll_once 会识别不再改 Failed，
    /// already_processed 也会挡住后续重试）。
    async fn mark_rolled_back(&self, candidate: &PromotionCandidate, reason: &str) {
        if let Ok(recent) = self.ledger.recent(1).await {
            if let Some(rec) = recent.first() {
                if rec.change_id == candidate.change_id && rec.cluster == self.cluster {
                    let _ = self
                        .ledger
                        .update_status(&rec.id, PromotionStatus::RolledBack, reason)
                        .await;
                }
            }
        }
    }

    /// 本 commit 是否触及指定前缀路径。
    async fn commit_touches(&self, prefix: &str) -> SFResult<bool> {
        let changed = self
            .run(
                "git",
                &["diff", "--name-only", "HEAD~1", "HEAD"],
                Some(&self.work_dir()),
                30,
            )
            .await?;
        Ok(changed.lines().any(|f| f.starts_with(prefix)))
    }

    /// 镜像获取：拉取端永远只产出引用、不构建镜像（主应用 Pod 非特权也无法
    /// 构建）。外部 registry 配置存在时引用外部仓库；缺省引用集群内
    /// registry——节点 kubelet 经 localhost NodePort pull（containerd 对
    /// localhost 默认 http 免 TLS；每个节点的 NodePort 都通，多节点天然
    /// 分发），镜像由推送端 buildah push 就位，pull 秒级。
    async fn obtain_image(&self, candidate: &PromotionCandidate) -> SFResult<String> {
        let tag = format!("cogneva:promote-{}", sanitize(&candidate.change_id));
        let endpoint = match &self.config.registry {
            Some(registry) => registry.trim_end_matches('/'),
            None => self.config.local_registry.trim_end_matches('/'),
        };
        Ok(format!("{endpoint}/{tag}"))
    }

    /// 金丝雀看护：watch 期内周期性检查新副本 readiness 与 restart
    /// count；配置了 metrics_url 时另做阈值比对。任何异常立即返回 Err。
    ///
    /// 返回值是这一轮**实际拿到了什么**。取不到数不是通过：整轮什么都没测到的
    /// 放行必须和「测了、没回归」在结论上分得开，否则一条长期失效的判据会无声
    /// 无息地一直放行。
    async fn watch_canary(&self, pre_rollout: Option<&HashSet<String>>) -> SFResult<GateCoverage> {
        let watch_secs = self.config.canary_watch_secs;
        let interval = std::cmp::max(watch_secs / 20, 5);
        let mut coverage = GateCoverage::default();
        if self.metrics_url.is_none() {
            coverage.metrics_off = Some("no metrics endpoint configured");
        } else if pre_rollout.is_none() {
            coverage.metrics_off = Some("pre-rollout pod list unavailable");
        }
        let started = std::time::Instant::now();
        let deadline = started + Duration::from_secs(watch_secs);
        // 宽限期：金丝雀刚 set image 时新副本还在 ContainerCreating，
        // not-ready 属正常；宽限过后仍不 ready 才算异常。
        let grace = Duration::from_secs(std::cmp::max(90, watch_secs / 4));
        // 参考点两侧同拍取，且装在同一个值里：只重取一侧就会让两侧的起算点分开，
        // 两个分位数描述的时间长度随即差一个数量级（旧副本已跑一整天、候选才几分
        // 钟），而这个差会静默地把闸门推宽或推窄。
        let mut window: Option<CanaryWindow> = None;

        while std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(interval)).await;
            self.check_pods_healthy(started.elapsed() < grace).await?;
            coverage.pod_checks += 1;
            let Some(pre) = pre_rollout else {
                continue;
            };
            let Ok((old_ips, new_ips)) = self.canary_pod_groups(pre).await else {
                continue;
            };
            // 两侧都要抓得到才是同一拍的两个参考点。新副本还在拉镜像时新组为空，
            // 旧副本被换掉时旧组为空——任一侧缺席这一拍就不起算，也绝不拿旧读数
            // 顶替，否则两侧的起算点会错开。
            let (Some(old_now), Some(new_now)) = (
                self.scrape_side(&old_ips).await,
                self.scrape_side(&new_ips).await,
            ) else {
                continue;
            };
            match &window {
                Some(w) if w.holds(&old_now, &new_now) => {
                    self.compare_metrics(w, &old_now, &new_now, &mut coverage)?;
                }
                Some(_) => {
                    // 观测对象或正文语义变过：两点不再是一段连续历史，整窗重取。
                    coverage.window_resets += 1;
                    window = Some(CanaryWindow {
                        old: old_now,
                        new: new_now,
                    });
                }
                None => {
                    window = Some(CanaryWindow {
                        old: old_now,
                        new: new_now,
                    });
                }
            }
        }
        coverage.no_window = coverage.metrics_off.is_none() && window.is_none();
        let summary = coverage.summary();
        // 有判据整轮没拿到读数时按 warn 记：这是需要有人看一眼的形态，不是常态。
        if coverage.measured("latency") && coverage.measured("error-rate") {
            info!(cluster = %self.cluster, "{summary}");
        } else {
            warn!(cluster = %self.cluster, "{summary}");
        }
        Ok(coverage)
    }

    /// 抓取一组副本，连同「刮的是谁、正文声明什么语义」一起记下。
    ///
    /// 单有一份读数不够：要判断两次读数是不是同一段连续历史，就必须知道各自
    /// 观测的是哪一组副本、正文声明的又是哪一套语义。
    async fn scrape_side(&self, ips: &[String]) -> Option<SideRead> {
        let (signals, semantics, hist) = self.scrape_group(ips).await?;
        Some(SideRead {
            pods: ips.join(","),
            semantics,
            signals,
            hist,
        })
    }

    /// k8s 信号：deployment 任一 pod 处于非 Ready/重启次数上升即异常。
    /// `allow_pending` 为宽限期标志：true 时容忍 not-ready（容器还在拉起），
    /// 但致命等待态（拉不到镜像、配置错误）无论是否在宽限期都立即异常。
    async fn check_pods_healthy(&self, allow_pending: bool) -> SFResult<()> {
        // Pod 标签是 app.kubernetes.io/name=<deployment>；此前误用 app=<deployment>
        // 匹配零个 Pod，看护空转通过（2026-08-07 双集群实测）。
        // kubectl 失败（API 抖动/凭证问题）必须向上传播走回滚，
        // 不能吞成空输出当"零 Pod"（2026-08-07 cluster-2 实测）。
        let out = self
            .run(
                &self.config.kubectl_bin.clone(),
                &[
                    "-n",
                    &self.config.namespace,
                    "get",
                    "pods",
                    "-l",
                    &format!("app.kubernetes.io/name={}", self.config.deployment),
                    "-o",
                    // 行分隔必须是 {"\n"} 双引号转义形式——单引号里放真实
                    // 换行符 kubectl jsonpath 直接解析失败（unterminated
                    // quoted string），旧代码靠吞错误空转掩盖了它。
                    "jsonpath={range .items[*]}{.status.containerStatuses[0].restartCount}{' '}{.status.containerStatuses[0].ready}{' '}{.status.containerStatuses[0].state.waiting.reason}{\"\\n\"}{end}",
                ],
                None,
                30,
            )
            .await?;
        if out.trim().is_empty() {
            // 零 Pod 不是"健康"——选择器失效或副本被删光都不能空转通过。
            return Err(SFError::Agent(
                "no pods found for canary watch (selector/deployment mismatch?)".into(),
            ));
        }
        for line in out.lines() {
            let mut parts = line.split_whitespace();
            let restarts: u32 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            let ready = parts.next().unwrap_or("false");
            let waiting_reason = parts.next().unwrap_or("");
            if matches!(
                waiting_reason,
                "ImagePullBackOff"
                    | "ErrImagePull"
                    | "InvalidImageName"
                    | "CreateContainerConfigError"
                    | "CrashLoopBackOff"
            ) {
                return Err(SFError::Agent(format!(
                    "pod in fatal waiting state {waiting_reason} during canary: {line}"
                )));
            }
            if ready != "true" && !allow_pending {
                return Err(SFError::Agent(format!(
                    "pod not ready during canary: {line}"
                )));
            }
            // 金丝雀新副本 restart > 0 说明新代码崩过。
            if restarts > 1 {
                return Err(SFError::Agent(format!(
                    "pod restarting during canary ({restarts} restarts)"
                )));
            }
        }
        Ok(())
    }

    /// 抓取一个 metrics 地址（可选）。返回正文里的原始计数、它声明的语义，
    /// 以及延迟直方图的累积桶。
    async fn scrape_metrics_at(
        &self,
        url: &str,
    ) -> SFResult<Option<(CanarySignals, CounterSemantics, HistogramSnapshot)>> {
        let body = self
            .run("curl", &["-sf", "--max-time", "10", url], None, 15)
            .await?;
        Ok(Some(parse_prometheus_signals(&body)))
    }

    /// 抓取一组副本并合成一个读数。
    ///
    /// 只在同一版本的副本之间合成：它们是同一个进程模型的多个实例，合成出的
    /// 是这一版的整体画像。跨版本（新旧副本）合成是另一回事，那正是判据要
    /// 分开比的东西，绝不在这里做。
    ///
    /// 组内任一副本抓不到就返回 `None`——半个组的读数不是这一版的读数，
    /// 拿它下结论等于拿不完整的证据判版本好坏。
    async fn scrape_group(
        &self,
        ips: &[String],
    ) -> Option<(CanarySignals, CounterSemantics, HistogramSnapshot)> {
        let base = self.metrics_url.as_deref()?;
        if ips.is_empty() {
            return None;
        }
        let mut total = CanarySignals {
            errors: 0.0,
            requests: 0.0,
        };
        let mut histograms = HistogramSnapshot::default();
        let mut semantics: Option<CounterSemantics> = None;
        for ip in ips {
            let url = metrics_url_for_pod(base, ip)?;
            let Ok(Some((signals, s, hist))) = self.scrape_metrics_at(&url).await else {
                warn!(pod_ip = %ip, "canary metrics scrape failed; skipping this group");
                return None;
            };
            match semantics {
                Some(prev) if prev != s => {
                    warn!(
                        pod_ip = %ip,
                        "replicas of one version disagree on counter semantics; \
                         refusing to combine them"
                    );
                    return None;
                }
                _ => semantics = Some(s),
            }
            total.errors += signals.errors;
            total.requests += signals.requests;
            histograms.merge(&hist);
        }
        semantics.map(|s| (total, s, histograms))
    }

    /// 看护范围内本部署的副本，返回 (名字, podIP)。还没有 IP 的副本
    /// （ContainerCreating）也列出来，它的 IP 为空串。
    async fn list_canary_pods(&self) -> SFResult<Vec<(String, String)>> {
        let out = self
            .run(
                &self.config.kubectl_bin.clone(),
                &[
                    "-n",
                    &self.config.namespace,
                    "get",
                    "pods",
                    "-l",
                    &format!("app.kubernetes.io/name={}", self.config.deployment),
                    "-o",
                    "jsonpath={range .items[*]}{.metadata.name}{\" \"}{.status.podIP}{\"\\n\"}{end}",
                ],
                None,
                30,
            )
            .await?;
        Ok(out
            .lines()
            .filter_map(|line| {
                let mut parts = line.split_whitespace();
                let name = parts.next()?;
                Some((name.to_string(), parts.next().unwrap_or("").to_string()))
            })
            .collect())
    }

    /// 看护范围内的副本，按「本次滚动之前就存在」分成旧组与新组（各返回 podIP）。
    ///
    /// 旧组跑上一版、新组跑候选，判据只在两组之间比才有意义。判据取"滚动前
    /// 已有的 Pod 名单"而不是镜像名或创建时间：浮动签重推时新旧镜像同名，
    /// 创建时间又要跟本进程记录的滚动起点对标，两者都不如名字集合直接。
    async fn canary_pod_groups(
        &self,
        pre_rollout: &HashSet<String>,
    ) -> SFResult<(Vec<String>, Vec<String>)> {
        let mut old = Vec::new();
        let mut new = Vec::new();
        for (name, ip) in self.list_canary_pods().await? {
            // 没有 IP 的副本抓不了，不进入任何一组：新副本还在拉镜像时
            // 新组为空，看护自然判"没有证据"。
            if ip.is_empty() {
                continue;
            }
            if pre_rollout.contains(&name) {
                old.push(ip);
            } else {
                new.push(ip);
            }
        }
        Ok((old, new))
    }

    /// 本次滚动开始前已在跑的 Pod 名单。拿不到时返回 `None`：宁可整轮跳过
    /// 指标闸门，也不能把新旧副本混作一组去比。
    async fn pre_rollout_pods(&self) -> Option<HashSet<String>> {
        let pods = self.list_canary_pods().await.ok()?;
        Some(pods.into_iter().map(|(name, _)| name).collect())
    }

    /// 金丝雀看护的阈值比对，一拍一次。
    ///
    /// `window` 是两侧同拍的参考点，`old`/`new` 是此刻的读数：两侧的速率与 p99
    /// 各自由**本侧的两个点**得出，不能拿一侧的参考点去减另一侧的计数——那是两个
    /// 进程各自的计数器，相减没有意义。
    ///
    /// 每拍把两条判据的结局写进 `coverage`：读数还是缺证据、缺在哪一侧、什么原因。
    /// 只判回归而不留这件事，会让「整轮没测到」在台账上长得和「测了没回归」一样。
    fn compare_metrics(
        &self,
        window: &CanaryWindow,
        old: &SideRead,
        new: &SideRead,
        coverage: &mut GateCoverage,
    ) -> SFResult<()> {
        // 分位数只在这段两点窗口内到达过请求时才存在。读不出来不是 0——用 0 当
        // 基线去比会把任何一次观测都判成回归，也会把「没测到」写成「延迟很好」。
        let baseline_p99 = p99_between(&window.old.hist, &old.hist);
        let candidate_p99 = p99_between(&window.new.hist, &new.hist);
        let outcome = latency_outcome(baseline_p99, candidate_p99);
        debug!("canary latency gate: {outcome}");
        coverage.record("latency", outcome);
        if let (LatencyRead::Measured(b), LatencyRead::Measured(c)) = (baseline_p99, candidate_p99)
        {
            if b > 0.0 && c > b * self.config.canary_p99_multiplier {
                return Err(SFError::Agent(format!(
                    "canary p99 regressed: {c:.0}ms > baseline {b:.0}ms x {}",
                    self.config.canary_p99_multiplier
                )));
            }
        }
        // 语义由 `CanaryWindow::holds` 保证两侧一致；变过就整窗重取了，不会走到这里。
        let read = error_rate_gate(window, old, new, self.config.canary_min_requests_for_rate);
        debug!(
            cluster = %self.cluster,
            "canary error-rate gate: {}", read.label()
        );
        coverage.record("error-rate", read.label());
        if let RateRead::Measured {
            candidate,
            baseline,
        } = read
        {
            if candidate > baseline * self.config.canary_error_rate_multiplier && candidate > 0.01 {
                return Err(SFError::Agent(format!(
                    "canary error rate regressed: {candidate:.4} > baseline {baseline:.4} x {}",
                    self.config.canary_error_rate_multiplier
                )));
            }
        }
        Ok(())
    }

    async fn record(
        &self,
        candidate: &PromotionCandidate,
        status: PromotionStatus,
        outcome: &str,
    ) -> SFResult<String> {
        let now = chrono::Utc::now();
        let rec = PromotionRecord {
            id: uuid::Uuid::new_v4().to_string(),
            change_id: candidate.change_id.clone(),
            level: candidate.level.clone(),
            // 拉取端执行的是推送端已经分好的级，自己不做分级。
            gate_kind: None,
            decision_reason: format!("gitops pull ({})", self.cluster),
            cluster: self.cluster.clone(),
            status,
            outcome: outcome.to_string(),
            eval_summary: candidate.eval_summary.clone(),
            created_at: now,
            updated_at: now,
        };
        let id = rec.id.clone();
        self.ledger.record(rec).await?;
        Ok(id)
    }
}

fn sanitize(change_id: &str) -> String {
    change_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// 拉取端后台循环入口（插件 spawn）。
pub async fn run_puller_loop(puller: Arc<GitOpsPuller>, shutdown: cog_core::ShutdownSignal) {
    // 本地路径仓库（如 Pod 内挂载的 /host-git，属主 root）会撞 git
    // dubious-ownership 检查。safe.directory 只有 global 配置被采信（-c
    // 与 GIT_CONFIG_* env 实测均无效，2026-08-06 主 Pod 内验证），启动时
    // 幂等写入；HOME 不可写时失败不致命（后续 poll 报错可见）。
    if !puller.config.repo_url.contains("://") && !puller.config.repo_url.contains('@') {
        let _ = tokio::process::Command::new("git")
            .args([
                "config",
                "--global",
                "--add",
                "safe.directory",
                &puller.config.repo_url,
            ])
            .output()
            .await;
    }
    let interval = Duration::from_secs(puller.config.poll_interval_secs.max(15));
    info!(
        repo = %puller.config.repo_url,
        branch = %puller.config.branch,
        cluster = %puller.cluster,
        interval_secs = interval.as_secs(),
        "GitOps puller loop started"
    );
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                if let Err(e) = puller.poll_once().await {
                    warn!(error = %e, "GitOps puller poll failed");
                }
            }
        }
    }
}

/// 把配置里的指标地址套到某个副本上：只换 host，scheme/端口/路径照旧。
///
/// 配置里那个 `localhost` 的含义是「每个 Pod 自己的那个端点」，而不是「拉取
/// 进程所在 Pod 的端点」。看护要读的是被观测的副本，所以把 host 换成它的
/// Pod IP。地址形态解析不出来时返回 `None`：宁可这一轮不抓，也不要拿一个
/// 猜出来的地址去下结论。
fn metrics_url_for_pod(base: &str, ip: &str) -> Option<String> {
    let (scheme, rest) = base.split_once("://")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    // 端口跟着配置走；裸 host 或端口非数字时按没有端口处理。
    let port = match authority.rsplit_once(':') {
        Some((_, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            format!(":{p}")
        }
        _ => String::new(),
    };
    // IPv6 字面量要带方括号，否则拼出的 authority 无法解析。
    let host = if ip.contains(':') {
        format!("[{ip}]")
    } else {
        ip.to_string()
    };
    Some(format!("{scheme}://{host}{port}{path}"))
}

/// 一次抓取到的原始计数。错误率与 p99 都不在这里算：怎么算取决于正文声明的
/// 计数器语义与两次抓取的对比，那只有拿到第二个观测点方才知道。
#[derive(Debug, Clone, Copy, PartialEq)]
struct CanarySignals {
    errors: f64,
    requests: f64,
}

/// 一次抓取到的延迟直方图累积桶，按序列（标签集去掉 `le`）分组。
///
/// 桶是累积量，单点只说明「进程启动以来」；要给出一段窗口内的分位数，必须留
/// 两个点做差。所以抓取的产物里要带上这一份原始桶，而不只是一个算好的数。
#[derive(Debug, Clone, Default, PartialEq)]
struct HistogramSnapshot {
    /// 标签集（去掉 `le`）→ 每个上界及其累积计数，按上界升序。
    series: std::collections::BTreeMap<String, Vec<(f64, u64)>>,
}

/// 正文声明的计数器取值含义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CounterSemantics {
    /// `_total` 是自进程启动以来的累积量：两次抓取相减就是这中间发生的事，
    /// 速率只能由增量得出。
    Cumulative,
    /// `_total` 是一段滑动窗口内样本的求和：它会随样本老化而下降，两点相减
    /// 只是窗口边界的抖动，唯一可用的速率是「值本身相除」。
    Windowed,
}

/// 一侧（旧版本或候选）在一次抓取里的观测。
///
/// 带上「刮的是哪一组副本」与「正文声明什么语义」：这两样一变，这一读与之前那读
/// 就不再是同一段连续历史，参考点必须重取。
#[derive(Debug, Clone)]
struct SideRead {
    /// 被观测的副本 IP 集合，拼成一个串。
    pods: String,
    semantics: CounterSemantics,
    signals: CanarySignals,
    hist: HistogramSnapshot,
}

/// 一段判据窗：旧版本与候选各自在这一拍上的参考读数。
///
/// 两侧必须**同拍**取参考点。一边从进程启动起算、另一边从被观测起算，两个分位数
/// 描述的时间长度就差着一个数量级——旧副本已经跑了一整天、候选才几分钟——比它们
/// 等于拿两个时间尺度在比，而这个差会静默地把闸门推宽或推窄。把两侧参考点放进
/// 同一个值里，就没法只重取其中一侧。
#[derive(Debug, Clone)]
struct CanaryWindow {
    old: SideRead,
    new: SideRead,
}

impl CanaryWindow {
    /// 两侧都还在观测同一组副本、同一套语义，这个窗才接着用。任一侧变了就整窗
    /// 重取——只重取一侧会让两侧的起算点分开，那正是这个值要杜绝的事。
    fn holds(&self, old: &SideRead, new: &SideRead) -> bool {
        self.old.same_observation(old) && self.new.same_observation(new)
    }
}

impl SideRead {
    /// 两次抓取观测的是同一组副本、同一套计数器语义。
    fn same_observation(&self, other: &SideRead) -> bool {
        self.pods == other.pods && self.semantics == other.semantics
    }
}

/// 一段观测窗里读延迟分位数的结果。
///
/// 四种「读不到」是四个不同的事实：正文里根本没有这条序列（这一版一生没服务过
/// 计时请求），序列在但窗口内没有新观测（没有流量到达被观测的副本），序列在两个
/// 观测点之间重启过（两个计数不是一段连续历史），序列读不出分位数（正文不是一份
/// 能读的直方图）。合成一句话会让它们互相冒充，而「候选根本不接流量」正是最该被
/// 看见的那一种。
#[derive(Debug, Clone, Copy, PartialEq)]
enum LatencyRead {
    /// 窗口内的 p99（毫秒）。
    Measured(f64),
    NoSeries,
    NoObservations,
    Restarted,
    Malformed,
}

impl LatencyRead {
    fn label(&self) -> &'static str {
        match self {
            Self::Measured(_) => "measured",
            Self::NoSeries => "no-series",
            Self::NoObservations => "no-observations",
            Self::Restarted => "restarted",
            Self::Malformed => "malformed",
        }
    }
}

/// 一拍错误率判据的读数。两种「读不到」是两个不同的事实，都不是「通过」。
///
/// 两侧语义不同不在这里：那是「这一读与之前那读不是一段连续历史」，整窗都要
/// 重取，落到 `CanaryWindow::holds` 上去判，而不是留着旧参考点逐轮跳过。
#[derive(Debug, Clone, Copy, PartialEq)]
enum RateRead {
    Measured {
        candidate: f64,
        baseline: f64,
    },
    /// 候选侧窗口内新增请求不到下限：一条 5xx 就能把小增量抬到任意高。
    NoDelta,
    /// 基线一侧没有读数：没有基线的相对量就没有相对判据。
    NoBaseline,
}

impl RateRead {
    fn label(&self) -> &'static str {
        match self {
            Self::Measured { .. } => "measured",
            Self::NoDelta => "no-delta",
            Self::NoBaseline => "no-baseline",
        }
    }
}

/// 一轮看护里每条判据实际拿到了什么。
///
/// 取不到数不是「通过」。整轮滚动的结论若不带这件事，读读数的人会把一次什么都没
/// 测到的放行读成「延迟与错误率都验过了」——而在这个部署上，那正是常态。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct GateCoverage {
    /// 整轮没做指标比对的确定性原因。没配 metrics 地址、或滚动前拿不到副本名单，
    /// 两者都不是缺陷，但也不能长得像「比过了没回归」。
    metrics_off: Option<&'static str>,
    /// 配了地址、名单也拿到了，却整轮没起算过参考点：候选或旧组始终抓不到。
    no_window: bool,
    /// 参考点被整窗重取的次数：副本名单换过、或正文声明的语义变过。
    window_resets: u32,
    /// 每一拍都跑了的 Pod 健康检查次数（跑过即通过，否则看护已中止）。
    pod_checks: u32,
    /// (判据, 结局) → 拍数。结局名带得出病因而非只说「跳过了」。
    outcomes: std::collections::BTreeMap<(String, String), u32>,
}

impl GateCoverage {
    fn record(&mut self, gate: &str, outcome: impl Into<String>) {
        *self
            .outcomes
            .entry((gate.to_string(), outcome.into()))
            .or_default() += 1;
    }

    fn outcomes_of(&self, gate: &str) -> Vec<String> {
        let mut counts: Vec<String> = self
            .outcomes
            .iter()
            .filter(|((g, _), _)| g == gate)
            .map(|((_, label), n)| format!("{label} x{n}"))
            .collect();
        counts.sort();
        counts
    }

    /// 这条判据整轮有没有拿到过可比读数。
    fn measured(&self, gate: &str) -> bool {
        self.outcomes
            .get(&(gate.to_string(), "measured".to_string()))
            .copied()
            .unwrap_or(0)
            > 0
    }

    fn one_gate(&self, gate: &str) -> String {
        let counts = self.outcomes_of(gate);
        // 一次都没跑也是一条事实，显式写出来：漏掉它，读者分不清「这条判据整轮
        // 没执行」和「执行了且在结论里没被提到」。
        let counts = if counts.is_empty() {
            "not-run".to_string()
        } else {
            counts.join(" ")
        };
        let tag = if self.measured(gate) {
            "measured"
        } else {
            "NO-EVIDENCE"
        };
        format!("{gate}={tag}[{counts}]")
    }

    /// 一条能进台账的话：哪几条判据真的给了读数、哪几条整轮没有证据、各是什么原因。
    fn summary(&self) -> String {
        if let Some(reason) = self.metrics_off {
            return format!(
                "gates: metrics off ({reason}); pods=ok x{}",
                self.pod_checks
            );
        }
        if self.no_window {
            return format!(
                "gates: NO-EVIDENCE (the candidate and the old group were never both \
                 scrapeable); pods=ok x{}",
                self.pod_checks
            );
        }
        let resets = if self.window_resets > 0 {
            format!(" window-resets={}", self.window_resets)
        } else {
            String::new()
        };
        format!(
            "gates: pods=ok x{}{resets} {} {}",
            self.pod_checks,
            self.one_gate("latency"),
            self.one_gate("error-rate"),
        )
    }
}

/// 两次抓取之间的错误率，按正文声明的计数器语义解释。
///
/// 累积语义下用「生命周期总量」相除，得到的是整段进程历史的平均错误率——
/// 金丝雀自己回归时它几乎不动，闸门形同虚设。两次抓取之间新增样本不足
/// `min_requests` 时返回 `None`：一条 5xx 就能把很小的增量抬到任意高的比值，
/// 据此下结论会把好版本判成回归。没有证据就是没有证据，返回 `None` 而不是 0。
fn error_rate(
    baseline: CanarySignals,
    current: CanarySignals,
    semantics: CounterSemantics,
    min_requests: f64,
) -> Option<f64> {
    match semantics {
        CounterSemantics::Windowed => {
            if current.requests > 0.0 {
                Some(current.errors / current.requests)
            } else {
                None
            }
        }
        CounterSemantics::Cumulative => {
            let requests = current.requests - baseline.requests;
            let errors = current.errors - baseline.errors;
            // 累积计数器只会上升；下降说明取值与自称的语义不符（或进程重启
            // 把计数器清零），此时的增量不能拿来算速率。
            if requests < min_requests || errors < 0.0 {
                None
            } else {
                Some(errors / requests)
            }
        }
    }
}

/// 延迟直方图的桶序列名。网关把请求延迟以毫秒记在 `http_request_duration_ms`
/// 下，正文里从不存在以秒记的同名序列。
const LATENCY_BUCKET_SERIES: &str = "http_request_duration_ms_bucket{";

/// 从 Prometheus 文本面读出原始计数、声明的计数器语义，以及延迟直方图的累积桶。
/// 纯函数：输入是抓取到的正文，输出只由正文决定，便于对着真实输出形态做断言。
fn parse_prometheus_signals(body: &str) -> (CanarySignals, CounterSemantics, HistogramSnapshot) {
    let mut error_total = 0.0f64;
    let mut request_total = 0.0f64;
    let mut semantics = CounterSemantics::Windowed;
    let mut histograms = HistogramSnapshot::default();
    for line in body.lines() {
        // 探针与抓取器的序列不进判据：它们不可能失败，留在分母里会让一个只影响
        // 业务端点的回归读不出来。过滤必须落在读侧——抓到的正文可能来自还在跑
        // 旧版本的副本，也可能带着生产者停下之前写下的累计量。
        if cog_core::series_endpoint(line).is_some_and(cog_core::is_infra_endpoint) {
            continue;
        }
        if let Some(rest) = line.strip_prefix(COUNTER_SEMANTICS_MARKER) {
            semantics = match rest.trim() {
                COUNTER_SEMANTICS_CUMULATIVE => CounterSemantics::Cumulative,
                _ => CounterSemantics::Windowed,
            };
        } else if line.starts_with("http_requests_total") {
            // 5xx 也是请求。把它排除在分母外会把错误率系统性放大，
            // 于是好版本被少报的成功数判成坏版本。
            let value = parse_series_value(line);
            request_total += value;
            if line.contains("status=\"5") {
                error_total += value;
            }
        } else if line.starts_with(LATENCY_BUCKET_SERIES) {
            histograms.push(line);
        }
    }
    (
        CanarySignals {
            errors: error_total,
            requests: request_total,
        },
        semantics,
        histograms,
    )
}

impl HistogramSnapshot {
    /// 记下一条 `_bucket` 行，按它所属序列的标签集分组。
    fn push(&mut self, line: &str) {
        let (Some(open), Some(close)) = (line.find('{'), line.rfind('}')) else {
            return;
        };
        let labels = &line[open + 1..close];
        let Some((bound, count)) = label_value(labels, "le")
            .and_then(parse_bound)
            .zip(Some(parse_series_value(line)))
        else {
            return;
        };
        let count = if count < 0.0 { 0 } else { count as u64 };
        let series = self.series.entry(without_le_label(labels)).or_default();
        // 正文不一定按上界升序；分位数依赖顺序，读取时先排好。
        series.push((bound, count));
        series.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }

    /// 并入同一版本另一个副本的桶。同一版本的副本是同一个进程模型，桶界相同，
    /// 逐序列逐上界相加即得这一版的整体画像；合成只发生在同版本副本之间，
    /// 新旧版本合成正是判据要分开比的东西。
    fn merge(&mut self, other: &HistogramSnapshot) {
        for (key, buckets) in &other.series {
            let series = self.series.entry(key.clone()).or_default();
            for (bound, count) in buckets {
                match series.iter_mut().find(|(b, _)| b == bound) {
                    Some(entry) => entry.1 += count,
                    None => series.push((*bound, *count)),
                }
            }
            series.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        }
    }
}

/// 一次抓取里某个标签值。只在标签边界上匹配，避免 `slot="1"` 被当成 `le`。
fn label_value<'a>(labels: &'a str, name: &str) -> Option<&'a str> {
    let needle = format!("{name}=\"");
    let mut search = 0usize;
    while let Some(found) = labels[search..].find(&needle) {
        let start = search + found;
        let value_start = start + needle.len();
        let value_end = value_start + labels[value_start..].find('"')?;
        if start == 0 || labels.as_bytes()[start - 1] == b',' {
            return Some(&labels[value_start..value_end]);
        }
        search = value_end + 1;
    }
    None
}

/// 标签集去掉 `le`：`le` 是被读的那一维，不属于标识一条序列的东西。
fn without_le_label(labels: &str) -> String {
    let Some(start) = labels.find("le=\"") else {
        return labels.to_string();
    };
    let Some(offset) = labels[start + 4..].find('"') else {
        return labels.to_string();
    };
    let end = start + 4 + offset + 1;
    let mut out = labels[..start].trim_end_matches(',').to_string();
    let tail = labels[end..].trim_start_matches(',');
    if !tail.is_empty() {
        if !out.is_empty() {
            out.push(',');
        }
        out.push_str(tail);
    }
    out
}

/// 桶上界。Prometheus 把无穷桶写成 `+Inf`，`f64` 认的是 `inf`，两者要搭上。
fn parse_bound(raw: &str) -> Option<f64> {
    match raw {
        "+Inf" | "Inf" | "inf" | "+inf" => Some(f64::INFINITY),
        other => other.parse().ok(),
    }
}

fn parse_series_value(line: &str) -> f64 {
    line.split_whitespace()
        .last()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0)
}

/// 一拍延迟闸门的结局。这条判据只有在两侧都有正读数时才给出结论。
///
/// 两侧各是一段连续历史里的两个计数点，任何一侧读不出数，这一拍就没有相对判据。
/// 两类病因都写进这一条标签里：候选那一侧读不出，恰恰是「候选根本没接流量」最该
/// 被看见的形态，不能被旧组的原因盖掉。
fn latency_outcome(baseline: LatencyRead, candidate: LatencyRead) -> String {
    match (baseline, candidate) {
        // 基线读作 0 时比值判据不存在：任何一次观测都大于 0，比下去等于把好版本
        // 判成回归。这与「基线读不出数」是同一类缺口，落同一个名字。
        (LatencyRead::Measured(b), LatencyRead::Measured(_)) if b <= 0.0 => {
            "no-baseline:non-positive".to_string()
        }
        (LatencyRead::Measured(_), LatencyRead::Measured(_)) => "measured".to_string(),
        (LatencyRead::Measured(_), c) => format!("no-candidate:{}", c.label()),
        (b, LatencyRead::Measured(_)) => format!("no-baseline:{}", b.label()),
        (b, c) => format!("no-baseline:{} no-candidate:{}", b.label(), c.label()),
    }
}

/// 一拍错误率闸门的读数：两侧各按本侧的两点算出速率，再相除比。
///
/// 读不出候选侧的速率，与读不出基线侧的速率，是两个不同的缺口；两者都返回
/// 「没有证据」而不是 0——拿 0 当基线等于「任何超过 1% 的错误率都判回归」，
/// 会无差别回滚好版本。
fn error_rate_gate(
    window: &CanaryWindow,
    old: &SideRead,
    new: &SideRead,
    min_requests: f64,
) -> RateRead {
    let semantics = new.semantics;
    let candidate = error_rate(window.new.signals, new.signals, semantics, min_requests);
    let baseline = error_rate(window.old.signals, old.signals, semantics, min_requests);
    match (candidate, baseline) {
        (Some(candidate), Some(baseline)) => RateRead::Measured {
            candidate,
            baseline,
        },
        (None, _) => RateRead::NoDelta,
        (Some(_), None) => RateRead::NoBaseline,
    }
}

/// 两次抓取之间到达的观测的 99 分位，取最差的一条序列。
///
/// 累积桶描述的是进程一生，而金丝雀两侧的「一生」长短差得很远：旧副本从上一次
/// 滚动起就一直活着，候选才起来几分钟。直接比两者的累积量，等于拿旧组一整周的
/// 长尾去比候选的几分钟，偏向「别动」。两点做差才能让两侧落在同一个窗口里。
///
/// 取最差的那条序列：闸门要盯的是最先越界的那一条，「最后一行赢」会让结论随
/// 正文行序变化。
fn p99_between(earlier: &HistogramSnapshot, later: &HistogramSnapshot) -> LatencyRead {
    // 正文里一条延迟序列都没有：这一版一生没服务过被计时的请求。
    if later.series.is_empty() {
        return LatencyRead::NoSeries;
    }
    let mut worst: Option<f64> = None;
    let mut restarted = false;
    let mut malformed = false;
    for (key, buckets) in &later.series {
        let delta = match earlier.series.get(key) {
            Some(previous) => match delta_buckets(previous, buckets) {
                Some(delta) => delta,
                // 序列在两点之间重启过，差值不是一个计数，这条不提供证据。
                None => {
                    restarted = true;
                    continue;
                }
            },
            // 这段窗口之前没有观测点，整份累积量都落在窗口内。
            None => buckets.clone(),
        };
        // 桶差里一个观测都没有：这条序列在窗口内是安静的。这与「读不出分位数」
        // 是两件事，先分开——分位数函数对二者都返回 `None`。
        if delta.last().map(|(_, count)| *count).unwrap_or(0) == 0 {
            continue;
        }
        match quantile_from_buckets(&delta, 0.99) {
            Some(q) => worst = Some(worst.map_or(q, |w: f64| w.max(q))),
            None => malformed = true,
        }
    }
    match worst {
        Some(q) => LatencyRead::Measured(q),
        // 一个数都没读出来时先报更具体的病因：正文本身读不出分位数，比「这段
        // 时间没观测」更能说明问题。
        None if malformed => LatencyRead::Malformed,
        None if restarted => LatencyRead::Restarted,
        None => LatencyRead::NoObservations,
    }
}

/// 同一条序列两个累积读数之间的增量。
///
/// 计数下降只可能是进程被换掉、序列从头开始，这时的差值不是观测数；返回 `None`
/// 让调用方跳过这条，而不是把负增量夹成 0 —— 夹零会破坏桶的单调性，分位数插值
/// 依赖的正是它。
fn delta_buckets(earlier: &[(f64, u64)], later: &[(f64, u64)]) -> Option<Vec<(f64, u64)>> {
    let mut out = Vec::with_capacity(later.len());
    for (bound, count) in later {
        let previous = earlier
            .iter()
            .find(|(b, _)| b == bound)
            .map(|(_, c)| *c)
            .unwrap_or(0);
        if *count < previous {
            return None;
        }
        out.push((*bound, count - previous));
    }
    Some(out)
}

/// 一条序列累积桶上的分位数，与 Prometheus 的 `histogram_quantile` 同法插值。
///
/// 与仪表盘用同一个算法，是为了让人在面板上看到的数与闸门据以判定的数是同一个
/// ——两个答案对同一个问题，迟早会有一个是错的。
fn quantile_from_buckets(buckets: &[(f64, u64)], q: f64) -> Option<f64> {
    let (top_bound, total) = *buckets.last()?;
    // 没有 `+Inf` 桶就不是一组能读的直方图；没有观测就没有可排的名次。
    if !top_bound.is_infinite() || total == 0 {
        return None;
    }
    let rank = q * total as f64;

    let mut lower_bound = 0.0f64;
    let mut lower_count = 0u64;
    for (bound, count) in buckets {
        if (*count as f64) >= rank {
            if !bound.is_finite() {
                // 名次落在 `+Inf` 桶里，只能说答案不低于最后一个有限上界。
                return Some(lower_bound);
            }
            let in_bucket = count - lower_count;
            if in_bucket == 0 {
                return Some(*bound);
            }
            let fraction = (rank - lower_count as f64) / in_bucket as f64;
            return Some(lower_bound + (*bound - lower_bound) * fraction);
        }
        lower_bound = *bound;
        lower_count = *count;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 窗口求和语义下的错误率，就是改造前的「值本身相除」。
    fn windowed_rate(signals: CanarySignals) -> Option<f64> {
        error_rate(signals, signals, CounterSemantics::Windowed, f64::INFINITY)
    }

    /// 正文取自网关 `/metrics` 的真实输出形态：延迟以毫秒记在
    /// `http_request_duration_ms` 下，且是累积桶而非预先算好的分位线。
    #[test]
    fn parses_error_rate_and_p99_from_gateway_body() {
        let first = "\
# TYPE http_requests_total counter
http_requests_total{endpoint=\"/api/v1/tasks\",method=\"POST\",status=\"201\"} 0
";
        let second = "\
# TYPE http_requests_total counter
http_requests_total{endpoint=\"/api/v1/tasks\",method=\"POST\",status=\"201\"} 90
http_requests_total{endpoint=\"/api/v1/tasks\",method=\"POST\",status=\"500\"} 10
# TYPE http_request_duration_ms histogram
http_request_duration_ms_bucket{endpoint=\"/api/v1/tasks\",method=\"POST\",le=\"100\"} 99
http_request_duration_ms_bucket{endpoint=\"/api/v1/tasks\",method=\"POST\",le=\"+Inf\"} 100
http_request_duration_ms_sum{endpoint=\"/api/v1/tasks\",method=\"POST\"} 12345.6
http_request_duration_ms_count{endpoint=\"/api/v1/tasks\",method=\"POST\"} 100
";
        let (_, _, earlier) = parse_prometheus_signals(first);
        let (signals, _, later) = parse_prometheus_signals(second);
        let rate = windowed_rate(signals).expect("有请求就有速率");
        assert!((rate - 0.1).abs() < 1e-9, "error_rate={rate}");
        // 99 分位落在 le=100 这条桶的上界上：这一段的观测都 ≤100ms。
        assert_eq!(p99_between(&earlier, &later), LatencyRead::Measured(100.0));
    }

    /// 分母是全部请求：5xx 既算错误也算请求。把它剔出分母会让错误率虚高，
    /// 拿虚高的数字去卡回归阈值，好版本会被判成坏版本。
    #[test]
    fn error_rate_counts_failures_in_the_denominator_too() {
        let body = "\
http_requests_total{endpoint=\"/a\",method=\"GET\",status=\"200\"} 100
http_requests_total{endpoint=\"/a\",method=\"GET\",status=\"503\"} 100
";
        let (signals, _, _) = parse_prometheus_signals(body);
        let rate = windowed_rate(signals).expect("有请求就有速率");
        assert!((rate - 0.5).abs() < 1e-9, "error_rate={rate}");
    }

    /// 探针与抓取器的序列不进分母，也不进 p99。它们的量级压过业务流量，留在
    /// 分母里会把错误率稀释到接近 0，闸门 `err > base_err * multiplier && err > 0.01`
    /// 就永远不成立——判据结构性失效，而不是判成通过。
    #[test]
    fn infra_series_stay_out_of_the_error_rate() {
        let body = "\
http_requests_total{endpoint=\"/health/live\",method=\"GET\",status=\"200\"} 100000
http_requests_total{endpoint=\"/health/ready\",method=\"GET\",status=\"200\"} 100000
http_requests_total{endpoint=\"/metrics\",method=\"GET\",status=\"200\"} 100000
http_requests_total{endpoint=\"/api/v1/tasks\",method=\"POST\",status=\"201\"} 90
http_requests_total{endpoint=\"/api/v1/tasks\",method=\"POST\",status=\"500\"} 10
http_request_duration_ms_bucket{endpoint=\"/health/live\",method=\"GET\",le=\"1\"} 100
http_request_duration_ms_bucket{endpoint=\"/health/live\",method=\"GET\",le=\"+Inf\"} 100
http_request_duration_ms_bucket{endpoint=\"/api/v1/tasks\",method=\"POST\",le=\"100\"} 99
http_request_duration_ms_bucket{endpoint=\"/api/v1/tasks\",method=\"POST\",le=\"+Inf\"} 100
";
        let (signals, _, hist) = parse_prometheus_signals(body);
        let rate = windowed_rate(signals).expect("有请求就有速率");
        assert!((rate - 0.1).abs() < 1e-9, "error_rate={rate}");
        // 探针的 1ms 系列被滤掉，剩下的最差一条是业务端点的 100ms。
        assert_eq!(
            p99_between(&HistogramSnapshot::default(), &hist),
            LatencyRead::Measured(100.0)
        );
    }

    /// 多序列各有一条 p99 时取最差的那条，结论不随正文行序变化。
    #[test]
    fn p99_is_the_worst_endpoint_regardless_of_line_order() {
        let body = "\
http_request_duration_ms_bucket{endpoint=\"/c\",le=\"+Inf\"} 100
http_request_duration_ms_bucket{endpoint=\"/b\",le=\"900\"} 99
http_request_duration_ms_bucket{endpoint=\"/a\",le=\"+Inf\"} 100
http_request_duration_ms_bucket{endpoint=\"/b\",le=\"+Inf\"} 100
http_request_duration_ms_bucket{endpoint=\"/a\",le=\"1\"} 99
http_request_duration_ms_bucket{endpoint=\"/c\",le=\"120\"} 99
";
        let (_, _, hist) = parse_prometheus_signals(body);
        assert_eq!(
            p99_between(&HistogramSnapshot::default(), &hist),
            LatencyRead::Measured(900.0)
        );
    }

    /// 两点之间没有新观测就没有 p99：累积量相同即增量为零，读作「窗口内没有观测」。
    /// 拿一份整段进程历史的累积量当窗口读数，会让金丝雀整轮看不到任何变化。
    #[test]
    fn unchanged_snapshots_carry_no_p99_evidence() {
        let body = "\
http_request_duration_ms_bucket{endpoint=\"/a\",le=\"100\"} 99
http_request_duration_ms_bucket{endpoint=\"/a\",le=\"+Inf\"} 100
";
        let (_, _, hist) = parse_prometheus_signals(body);
        assert_eq!(p99_between(&hist, &hist), LatencyRead::NoObservations);
    }

    /// p99 缺失时读作「没有证据」，而不是读作 0——把「读不到」当成「延迟很好」
    /// 写进结论，闸门就会对一次真正的回归放行。
    #[test]
    fn missing_p99_reads_no_evidence() {
        let body = "# TYPE http_requests_total counter\nhttp_requests_total{status=\"200\"} 5\n";
        let (signals, _, hist) = parse_prometheus_signals(body);
        assert_eq!(windowed_rate(signals), Some(0.0));
        assert_eq!(
            p99_between(&hist, &HistogramSnapshot::default()),
            LatencyRead::NoSeries
        );
    }

    /// 三种「读不到」是三个不同的事实，各自要有自己的词。
    ///
    /// 合成一句话会让它们互相冒充：正文里根本没有这条序列（这一版一生没服务过被
    /// 计时的请求）、序列在但这段窗口没有观测（没有流量到达被观测的副本）、正文
    /// 不是一份能读的直方图。「候选根本不接流量」正是最该被看见的那一种，不能被
    /// 另外两种盖成同一句话。
    #[test]
    fn the_three_ways_of_having_no_p99_read_differently() {
        // 这一版从来没有过这条序列。
        let no_series = p99_between(&HistogramSnapshot::default(), &HistogramSnapshot::default());
        // 序列在，但这段窗口里一个观测都没有。
        let mut quiet = HistogramSnapshot::default();
        quiet.series.insert(
            "endpoint=\"/a\"".to_string(),
            vec![(100.0, 99), (f64::INFINITY, 100)],
        );
        let no_observations = p99_between(&quiet, &quiet);
        // 正文里有桶，但收尾的不是 `+Inf` 桶，读不出分位数。
        let mut unfinishable = HistogramSnapshot::default();
        unfinishable
            .series
            .insert("endpoint=\"/a\"".to_string(), vec![(100.0, 99)]);
        let malformed = p99_between(&HistogramSnapshot::default(), &unfinishable);

        assert_eq!(no_series, LatencyRead::NoSeries);
        assert_eq!(no_observations, LatencyRead::NoObservations);
        assert_eq!(malformed, LatencyRead::Malformed);
        let labels = [
            no_series.label(),
            no_observations.label(),
            malformed.label(),
        ];
        for (i, a) in labels.iter().enumerate() {
            assert!(!matches!(a, &"measured"), "读不到不能读成通过：{a:?}");
            for b in labels.iter().skip(i + 1) {
                assert_ne!(a, b, "三种缺口不能用同一个词");
            }
        }
    }

    /// 序列在两个观测点之间重启过（计数下降）时，差值不是这一段窗口的观测数：
    /// 跳过这条序列。这与「窗口内没有观测」是两件事——前者说明这一读不是一段
    /// 连续历史，后者说明这段时间没有流量。
    #[test]
    fn a_restarted_series_reads_as_restarted_not_quiet() {
        let mut earlier = HistogramSnapshot::default();
        earlier.series.insert(
            "endpoint=\"/a\"".to_string(),
            vec![(100.0, 10), (f64::INFINITY, 10)],
        );
        let mut later = HistogramSnapshot::default();
        later.series.insert(
            "endpoint=\"/a\"".to_string(),
            vec![(100.0, 3), (f64::INFINITY, 3)],
        );
        assert_eq!(p99_between(&earlier, &later), LatencyRead::Restarted);
        assert_ne!(
            LatencyRead::Restarted.label(),
            LatencyRead::NoObservations.label()
        );
    }

    /// 序列在两个观测点之间重启过（计数下降）时，差值不是这一段窗口的观测数：
    /// 跳过这条序列，不把负增量夹成 0 —— 夹零会破坏桶的单调性，而分位数插值
    /// 依赖的正是它。
    #[test]
    fn a_restarted_series_yields_no_delta() {
        let earlier = vec![(100.0, 10u64), (f64::INFINITY, 10u64)];
        let later = vec![(100.0, 3u64), (f64::INFINITY, 3u64)];
        assert_eq!(delta_buckets(&earlier, &later), None);
    }

    /// 分位数在桶内插值，与 Prometheus 的 `histogram_quantile` 同法：名次落在
    /// 相邻两条桶界之间时，按落在桶内的比例线性取值。
    #[test]
    fn quantile_interpolates_within_the_bucket() {
        let buckets = vec![(256.0, 90u64), (512.0, 100u64), (f64::INFINITY, 100u64)];
        // 名次 99 落在 (256, 512] 这条桶内，桶里 10 个观测，第 9 个：0.9。
        let q = quantile_from_buckets(&buckets, 0.99).expect("有 +Inf 桶就能插值");
        assert!((q - (256.0 + (512.0 - 256.0) * 0.9)).abs() < 1e-9, "q={q}");
    }

    /// 名次落在 `+Inf` 桶里时只能说「不低于最后一个有限上界」，不能编出一个
    /// 具体的毫秒数。
    #[test]
    fn quantile_landing_in_the_overflow_bucket_reports_the_last_finite_bound() {
        let buckets = vec![(100.0, 50u64), (f64::INFINITY, 100u64)];
        assert_eq!(quantile_from_buckets(&buckets, 0.99), Some(100.0));
    }

    #[test]
    fn parse_tag_message_full() {
        let (p, l, e) = parse_tag_message("change_id=p-1\nlevel=l1_rollout\neval=Adopt z=2.0");
        assert_eq!(p.as_deref(), Some("p-1"));
        assert_eq!(l.as_deref(), Some("l1_rollout"));
        assert_eq!(e.as_deref(), Some("Adopt z=2.0"));
    }

    #[test]
    fn parse_tag_message_eval_none_becomes_none() {
        let (_p, _l, e) = parse_tag_message("change_id=p-1\nlevel=l0_config\neval=none");
        assert!(e.is_none());
    }

    #[test]
    fn parse_tag_message_missing_fields() {
        let (p, l, _e) = parse_tag_message("some random message");
        assert!(p.is_none());
        assert!(l.is_none());
    }

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

    /// 端到端（git 层）：publisher 推、puller 拉，候选解析正确。
    #[tokio::test]
    async fn puller_discovers_published_candidate() {
        let central = tempfile::tempdir().unwrap();
        git(central.path(), &["init", "--bare"]).await;

        let work = tempfile::tempdir().unwrap();
        git(work.path(), &["init"]).await;
        git(work.path(), &["config", "user.email", "t@t.com"]).await;
        git(work.path(), &["config", "user.name", "T"]).await;
        tokio::fs::write(work.path().join("lib.rs"), "fn v1() {}\n")
            .await
            .unwrap();
        git(work.path(), &["add", "."]).await;
        git(work.path(), &["commit", "-m", "initial"]).await;

        // L1 晋级会打 overlay 镜像：staged 二进制桩 + 假构建器（不真跑 buildah）。
        std::fs::write(work.path().join("cogneva"), b"staged-binary").unwrap();
        let fake_builder = crate::test_support::write_executable(
            work.path(),
            "fake-buildah",
            "#!/bin/sh\nexit 0\n",
        );

        // publisher 推。
        let publisher = crate::GitOpsPublisher::new(
            GitOpsConfig {
                repo_url: central.path().to_string_lossy().to_string(),
                builder_bin: fake_builder.to_string_lossy().into_owned(),
                ..Default::default()
            },
            work.path(),
            work.path(),
        );
        let change = crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: "p-42".into(),
            description: "test".into(),
            content: String::new(),
            status: crate::types::EvolutionStatus::Active,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };
        crate::PromotionChannel::publish_rollout(&publisher, &change)
            .await
            .unwrap();

        // puller 拉。
        let pull_dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let puller = GitOpsPuller::new(
            GitOpsConfig {
                repo_url: central.path().to_string_lossy().to_string(),
                work_dir: pull_dir.path().join("repo").to_string_lossy().to_string(),
                ..Default::default()
            },
            ledger,
            "cluster-b".into(),
        );

        puller.sync_repo().await.unwrap();
        let candidate = puller.candidate_at_head().await.unwrap().unwrap();
        assert_eq!(candidate.change_id, "p-42");
        assert_eq!(candidate.level, "l1_rollout");
    }

    #[tokio::test]
    async fn puller_without_promote_tag_returns_none() {
        let central = tempfile::tempdir().unwrap();
        git(central.path(), &["init", "--bare"]).await;
        let work = tempfile::tempdir().unwrap();
        git(work.path(), &["init"]).await;
        git(work.path(), &["config", "user.email", "t@t.com"]).await;
        git(work.path(), &["config", "user.name", "T"]).await;
        tokio::fs::write(work.path().join("lib.rs"), "fn v1() {}\n")
            .await
            .unwrap();
        git(work.path(), &["add", "."]).await;
        git(work.path(), &["commit", "-m", "initial"]).await;
        git(
            work.path(),
            &[
                "push",
                &central.path().to_string_lossy(),
                "HEAD:evolution-release",
            ],
        )
        .await;

        let pull_dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let puller = GitOpsPuller::new(
            GitOpsConfig {
                repo_url: central.path().to_string_lossy().to_string(),
                work_dir: pull_dir.path().join("repo").to_string_lossy().to_string(),
                ..Default::default()
            },
            ledger,
            "cluster-b".into(),
        );
        puller.sync_repo().await.unwrap();
        assert!(puller.candidate_at_head().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn already_processed_is_idempotent() {
        let ledger = Arc::new(cog_storage::MemoryStateBackend::new());
        let now = chrono::Utc::now();
        ledger
            .record(PromotionRecord {
                id: "r1".into(),
                change_id: "p-1".into(),
                level: "l1_rollout".into(),
                gate_kind: None,
                decision_reason: "test".into(),
                cluster: "cluster-b".into(),
                status: PromotionStatus::Promoted,
                outcome: "ok".into(),
                eval_summary: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let puller = GitOpsPuller::new(GitOpsConfig::default(), ledger, "cluster-b".into());
        assert!(puller.already_processed("p-1").await.unwrap());
        // 不同集群不算处理过（各集群各自晋级）。
        let puller_c = GitOpsPuller::new(
            GitOpsConfig::default(),
            Arc::new(cog_storage::MemoryStateBackend::new()),
            "cluster-c".into(),
        );
        assert!(!puller_c.already_processed("p-1").await.unwrap());
    }

    fn test_puller() -> GitOpsPuller {
        GitOpsPuller::new(
            GitOpsConfig::default(),
            Arc::new(cog_storage::MemoryStateBackend::new()),
            "c".into(),
        )
    }

    fn signals(errors: f64, requests: f64) -> CanarySignals {
        CanarySignals { errors, requests }
    }

    /// 一侧的一读。桶给空：本侧窗口内的分位数就由当前读数的桶单独决定，
    /// 便于把某一条判据单独拎出来看。
    fn side(semantics: CounterSemantics, signals: CanarySignals) -> SideRead {
        SideRead {
            pods: "10.0.0.1".to_string(),
            semantics,
            signals,
            hist: HistogramSnapshot::default(),
        }
    }

    /// 两侧同拍的一对参考点。
    fn window_of(
        semantics: CounterSemantics,
        old: CanarySignals,
        new: CanarySignals,
    ) -> CanaryWindow {
        CanaryWindow {
            old: side(semantics, old),
            new: side(semantics, new),
        }
    }

    /// 一段窗口里的桶差：一条序列，`buckets` 按上界升序给出。
    fn histogram(series: &str, buckets: &[(f64, u64)]) -> HistogramSnapshot {
        let mut hist = HistogramSnapshot::default();
        hist.series.insert(series.to_string(), buckets.to_vec());
        hist
    }

    /// 窗口内恰好 100 个观测、99 分位精确落在 `bound` 上的两个计数点。
    ///
    /// 参考点这一序列还没有观测，此刻 99 个 ≤ `bound`、共 100 个：桶差里名次 99
    /// 落在 `bound` 这条桶的最右端，插值系数正好是 1，读出来就是 `bound` 本身。
    fn p99_points(bound: f64) -> (HistogramSnapshot, HistogramSnapshot) {
        (
            histogram("endpoint=\"/a\"", &[(bound, 0), (f64::INFINITY, 0)]),
            histogram("endpoint=\"/a\"", &[(bound, 99), (f64::INFINITY, 100)]),
        )
    }

    /// 跑一拍阈值比对，返回结局与这一拍记下的判据覆盖。
    fn one_tick(
        puller: &GitOpsPuller,
        window: &CanaryWindow,
        old: &SideRead,
        new: &SideRead,
    ) -> (SFResult<()>, GateCoverage) {
        let mut coverage = GateCoverage::default();
        let result = puller.compare_metrics(window, old, new, &mut coverage);
        (result, coverage)
    }

    /// 配置里的 `localhost` 读作「每个 Pod 自己的端点」，套到被观测的副本上
    /// 时只换 host，scheme、端口、路径都照配置走。
    #[test]
    fn metrics_url_takes_the_observed_pods_host() {
        assert_eq!(
            metrics_url_for_pod("http://localhost:8080/metrics", "10.42.0.7").as_deref(),
            Some("http://10.42.0.7:8080/metrics")
        );
        // 非默认端口与带子路径的地址同样只换 host。
        assert_eq!(
            metrics_url_for_pod("https://localhost:9091/probe/metrics", "10.42.0.9").as_deref(),
            Some("https://10.42.0.9:9091/probe/metrics")
        );
        // 配置里没有端口时不留孤零零的冒号。
        assert_eq!(
            metrics_url_for_pod("http://localhost/metrics", "10.42.0.8").as_deref(),
            Some("http://10.42.0.8/metrics")
        );
        // 配置里没有路径时补根路径。
        assert_eq!(
            metrics_url_for_pod("http://localhost:8080", "10.42.0.8").as_deref(),
            Some("http://10.42.0.8:8080/")
        );
        // IPv6 字面量要带方括号，否则拼出来的 authority 解析不了。
        assert_eq!(
            metrics_url_for_pod("http://localhost:8080/metrics", "fd00::1").as_deref(),
            Some("http://[fd00::1]:8080/metrics")
        );
        // 形态不对就不猜地址，这一轮不抓。
        assert_eq!(
            metrics_url_for_pod("localhost:8080/metrics", "10.42.0.7"),
            None
        );
    }

    /// 副本按「滚动前是否已在跑」分成旧组与新组：新组才是候选，判据要打在
    /// 它身上。还没有 IP 的副本不进任何一组——它是还在拉镜像的候选，不是证据。
    #[tokio::test]
    async fn canary_splits_replicas_by_whether_they_preceded_the_rollout() {
        let dir = tempfile::tempdir().unwrap();
        let kubectl = crate::test_support::write_executable(
            dir.path(),
            "kubectl",
            "#!/bin/sh\nprintf 'cogneva-old 10.42.0.5\\ncogneva-new 10.42.0.6\\ncogneva-starting \\n'\n",
        );
        let puller = GitOpsPuller::new(
            GitOpsConfig {
                kubectl_bin: kubectl.to_string_lossy().into_owned(),
                namespace: "cogneva".into(),
                deployment: "cogneva".into(),
                ..Default::default()
            },
            Arc::new(cog_storage::MemoryStateBackend::new()),
            "c".into(),
        );

        // 滚动前的名单就是当下在跑的全部副本，含还没拿到 IP 的那个
        // （它的名字仍然出现，否则滚动后它会被误当成候选）。
        assert_eq!(
            puller.pre_rollout_pods().await.unwrap(),
            [
                "cogneva-old".to_string(),
                "cogneva-new".to_string(),
                "cogneva-starting".to_string()
            ]
            .into_iter()
            .collect()
        );

        let pre: HashSet<String> = ["cogneva-old".to_string()].into_iter().collect();
        let (old, new) = puller.canary_pod_groups(&pre).await.unwrap();
        assert_eq!(old, vec!["10.42.0.5"]);
        // 候选是滚动后才出现的那个；还没拿到 IP 的不算候选。
        assert_eq!(new, vec!["10.42.0.6"]);
    }

    /// 滚动前名单里的副本一个都没抓到时，不能退化成"什么都算候选"——
    /// 那会把旧副本当成候选去比，重演自比自。
    #[tokio::test]
    async fn canary_with_empty_pre_rollout_set_has_no_baseline_group() {
        let dir = tempfile::tempdir().unwrap();
        let kubectl = crate::test_support::write_executable(
            dir.path(),
            "kubectl",
            "#!/bin/sh\nprintf 'cogneva-old 10.42.0.5\\n'\n",
        );
        let puller = GitOpsPuller::new(
            GitOpsConfig {
                kubectl_bin: kubectl.to_string_lossy().into_owned(),
                namespace: "cogneva".into(),
                deployment: "cogneva".into(),
                ..Default::default()
            },
            Arc::new(cog_storage::MemoryStateBackend::new()),
            "c".into(),
        );
        // 空名单（列举失败时也走这条）：唯一在跑的副本被当成候选而不是旧组，
        // 于是基线取不到、指标闸门整轮跳过，而不是拿它跟自己比。
        let (old, new) = puller.canary_pod_groups(&HashSet::new()).await.unwrap();
        assert!(old.is_empty());
        assert_eq!(new, vec!["10.42.0.5"]);
    }

    /// 窗口求和语义下的阈值行为与拆分前一致：正文仍是旧版形态时，
    /// 看护判据不能因为这次改造而变宽或变严。
    #[test]
    fn windowed_semantics_keeps_the_existing_thresholds() {
        let puller = test_puller();
        let windowed = CounterSemantics::Windowed;
        let (base_before, base_now) = p99_points(100.0);
        let (_, at_120) = p99_points(120.0);
        let (_, at_125) = p99_points(125.0);
        let (_, at_140) = p99_points(140.0);
        // 旧版本这一侧：这段窗口内的 99 分位是 100ms。
        let mut w = window_of(windowed, signals(2.0, 100.0), signals(2.0, 100.0));
        w.old.hist = base_before;
        let mut old = side(windowed, signals(2.0, 100.0));
        old.hist = base_now;
        // 窗口语义下错误率只看当前读数，参考点不参与计算。
        // 基线错误率 2%，现 2.5%（1.25x，未超 1.5x）→ 通过。
        let mut new = side(windowed, signals(2.5, 100.0));
        new.hist = at_120.clone();
        assert!(one_tick(&puller, &w, &old, &new).0.is_ok());
        // 现 4%（2x）→ 回归。
        let mut new = side(windowed, signals(4.0, 100.0));
        new.hist = at_120;
        assert!(one_tick(&puller, &w, &old, &new).0.is_err());
        // 候选 p99 140ms（1.4x > 1.3x）→ 回归。
        let mut new = side(windowed, signals(2.0, 100.0));
        new.hist = at_140;
        assert!(one_tick(&puller, &w, &old, &new).0.is_err());
        // 候选 p99 125ms（1.25x）→ 通过。
        let mut new = side(windowed, signals(2.0, 100.0));
        new.hist = at_125;
        let (result, coverage) = one_tick(&puller, &w, &old, &new);
        assert!(result.is_ok());
        // 两侧都读了数，两条判据都在这一拍给出了结论——覆盖里必须留下这件事。
        assert!(coverage.measured("latency"));
        assert!(coverage.measured("error-rate"));
    }

    /// 累积语义下要比的是**增量**错误率。候选的生命周期比值在这里是 4%，
    /// 远超基线，但这一段窗口内新发生的 200 个请求一个都没错——判成回归就是在
    /// 回滚一个好版本，而闸门本来要盯的正是「这段时间有没有变坏」。
    #[test]
    fn cumulative_semantics_judges_the_delta_not_the_lifetime_ratio() {
        let puller = test_puller();
        let cumulative = CounterSemantics::Cumulative;
        // 旧版本在同一段窗口内的增量：1000 个请求错 10 个（1%）。
        let w = window_of(cumulative, signals(5.0, 1_000.0), signals(50.0, 1_000.0));
        let old = side(cumulative, signals(15.0, 2_000.0));
        // 候选这段窗口 200 个新请求零错误。生命周期比值 50/1200 = 4% 高于基线，
        // 但窗口内一个都没错。
        let good = side(cumulative, signals(50.0, 1_200.0));
        assert!(one_tick(&puller, &w, &old, &good).0.is_ok());
        // 增量里真的变坏了：200 个新请求错 20 个（10%）。
        let bad = side(cumulative, signals(70.0, 1_200.0));
        assert!(one_tick(&puller, &w, &old, &bad).0.is_err());
    }

    /// 候选侧速率必须由候选自己的两个点得出。新副本的累积计数器从零起步，
    /// 拿旧版本的累积量当参考点只会得到负增量，闸门整轮哑火。
    #[test]
    fn cumulative_semantics_differences_the_candidates_own_counters() {
        let puller = test_puller();
        let cumulative = CounterSemantics::Cumulative;
        // 旧版本这一段窗口 1000 个请求错 10 个（1%）。
        let w = window_of(
            cumulative,
            signals(5.0, 1_000.0),
            // 新副本刚起来：计数器归零。
            signals(0.0, 0.0),
        );
        let old = side(cumulative, signals(15.0, 2_000.0));
        // 500 个请求错 1 个（0.2%），好于基线 → 通过。
        assert!(
            one_tick(&puller, &w, &old, &side(cumulative, signals(1.0, 500.0)))
                .0
                .is_ok()
        );
        // 同一段窗口内错 20 个（4%）→ 回归。基线取的是旧版本的累积量，
        // 若误用它当参考点，这里会算出负增量而静默放过。
        assert!(
            one_tick(&puller, &w, &old, &side(cumulative, signals(20.0, 500.0)))
                .0
                .is_err()
        );
    }

    /// 增量太小时一条 5xx 就能把比值抬到任意高：20 个新请求错 1 个是 5%，
    /// 基线 1% 的 5 倍，据此回滚就是拿噪声当好版本缺陷。样本不足时不判，
    /// 而不是判通过或者判回归。
    #[test]
    fn cumulative_semantics_refuses_to_judge_on_too_few_new_requests() {
        let puller = test_puller();
        let cumulative = CounterSemantics::Cumulative;
        let w = window_of(cumulative, signals(10.0, 1_000.0), signals(10.0, 1_000.0));
        let old = side(cumulative, signals(11.0, 2_000.0));
        // 20 个新请求，不足下限（100）：不判，且覆盖里要写清是样本不够，
        // 而不是「测了没回归」。
        let (result, coverage) =
            one_tick(&puller, &w, &old, &side(cumulative, signals(11.0, 1_020.0)));
        assert!(result.is_ok());
        assert_eq!(coverage.outcomes_of("error-rate"), vec!["no-delta x1"]);
        // 增量够大且确实变坏，同一个基线就该判回归——上面那次通过的原因
        // 只能是样本不足，不能是判据根本不看错误率。
        let bad = side(cumulative, signals(60.0, 1_200.0));
        assert!(one_tick(&puller, &w, &old, &bad).0.is_err());
    }

    /// 基线速率取不到时不能拿 0 当基线：那等于「任何超过 1% 的错误率都判回归」，
    /// 好版本会被无差别回滚。没有基线的相对量就没有相对判据。
    #[test]
    fn missing_baseline_rate_does_not_roll_back_a_good_version() {
        let puller = test_puller();
        let cumulative = CounterSemantics::Cumulative;
        // 旧版本这一段窗口只多了 10 个请求，不足下限 → 基线速率读不出来。
        let w = window_of(cumulative, signals(10.0, 1_000.0), signals(0.0, 0.0));
        let old = side(cumulative, signals(10.0, 1_010.0));
        let new = side(cumulative, signals(50.0, 1_200.0));
        let (result, coverage) = one_tick(&puller, &w, &old, &new);
        assert!(result.is_ok());
        assert_eq!(coverage.outcomes_of("error-rate"), vec!["no-baseline x1"]);
    }

    /// 换版那一轮正文的语义会从窗口求和切成累积。跨语义相除得到的不是任何
    /// 一版的错误率，所以这次观测不能接着用旧的参考点：整窗作废重取。
    #[test]
    fn a_semantics_change_invalidates_the_whole_window() {
        let windowed = CounterSemantics::Windowed;
        let cumulative = CounterSemantics::Cumulative;
        let w = window_of(windowed, signals(2.0, 100.0), signals(0.0, 0.0));
        // 两侧都还观测着同一组副本、同一套语义 → 窗继续用。
        assert!(w.holds(
            &side(windowed, signals(2.0, 100.0)),
            &side(windowed, signals(9.0, 100.0))
        ));
        // 候选一侧的正文换了语义：这一读与参考点不再是同一段连续历史。
        assert!(!w.holds(
            &side(windowed, signals(2.0, 100.0)),
            &side(cumulative, signals(9.0, 100.0))
        ));
        // 旧版本一侧换了语义同样作废——只重取候选一侧会让两侧的起算点分开。
        assert!(!w.holds(
            &side(cumulative, signals(2.0, 100.0)),
            &side(windowed, signals(9.0, 100.0))
        ));
    }

    /// 窗的「继续用」要求**两侧都**还观测着原样的东西。只重取一侧会让两侧的
    /// 起算点分开，两个分位数随即描述不同长度的时间段。
    #[test]
    fn a_changed_pod_set_invalidates_the_whole_window() {
        let cumulative = CounterSemantics::Cumulative;
        let mut reference = window_of(cumulative, signals(1.0, 100.0), signals(1.0, 100.0));
        reference.old.pods = "10.0.0.1".to_string();
        reference.new.pods = "10.0.0.2".to_string();
        let mut old_same = side(cumulative, signals(1.0, 100.0));
        old_same.pods = "10.0.0.1".to_string();
        let mut new_same = side(cumulative, signals(1.0, 100.0));
        new_same.pods = "10.0.0.2".to_string();
        assert!(reference.holds(&old_same, &new_same));
        let mut moved = side(cumulative, signals(1.0, 100.0));
        moved.pods = "10.0.0.9".to_string();
        assert!(!reference.holds(&moved, &new_same));
        assert!(!reference.holds(&old_same, &moved));
    }

    /// 一拍延迟闸门的结局要说清缺在哪一侧：候选侧读不出数（候选根本没接流量）
    /// 与旧组侧读不出数，是两条不同的结论，都不能长得像通过。
    #[test]
    fn latency_outcome_names_the_side_that_is_missing() {
        let measured = LatencyRead::Measured(100.0);
        let quiet = LatencyRead::NoObservations;
        assert_eq!(latency_outcome(measured, measured), "measured");
        assert_eq!(
            latency_outcome(measured, quiet),
            "no-candidate:no-observations"
        );
        assert_eq!(
            latency_outcome(quiet, measured),
            "no-baseline:no-observations"
        );
        assert_eq!(
            latency_outcome(quiet, quiet),
            "no-baseline:no-observations no-candidate:no-observations"
        );
        // 基线读作 0 时比值判据不存在：任何一次观测都大于 0，比下去等于把
        // 好版本判成回归。这与「基线读不出数」落同一个名字。
        assert_eq!(
            latency_outcome(LatencyRead::Measured(0.0), measured),
            "no-baseline:non-positive"
        );
        for outcome in [
            latency_outcome(measured, quiet),
            latency_outcome(quiet, measured),
            latency_outcome(quiet, quiet),
        ] {
            assert_ne!(outcome, "measured");
        }
    }

    /// 整轮没做指标比对有两种确定性原因（没配地址、滚动前拿不到副本名单），
    /// 都不会产生指标读数。它们既不能长得像「比过了没回归」，也要各自说得出是哪种。
    #[test]
    fn a_roll_without_the_metrics_gates_says_so_in_its_conclusion() {
        let off = GateCoverage {
            pod_checks: 3,
            metrics_off: Some("no metrics endpoint configured"),
            ..Default::default()
        };
        assert!(off.summary().contains("metrics off"));
        assert!(off.summary().contains("no metrics endpoint configured"));
        assert!(!off.summary().contains("latency=measured"));

        let unlisted = GateCoverage {
            metrics_off: Some("pre-rollout pod list unavailable"),
            ..Default::default()
        };
        assert!(unlisted
            .summary()
            .contains("pre-rollout pod list unavailable"));
        assert_ne!(off.summary(), unlisted.summary());

        // 配了地址、名单也有，却整轮没起算过参考点：同样是没证据，但成因不同。
        let no_window = GateCoverage {
            pod_checks: 3,
            no_window: true,
            ..Default::default()
        };
        assert!(no_window.summary().contains("NO-EVIDENCE"));
        assert_ne!(no_window.summary(), off.summary());
    }

    /// 一轮看护的结论必须区分「判据给了读数」与「判据整轮没有证据」，
    /// 并且没有证据时要写清是什么缺口。少了这件事，一次什么都没测到的放行
    /// 会和「延迟与错误率都验过了」在台账上长得一模一样。
    #[test]
    fn gate_coverage_separates_measured_from_no_evidence() {
        let mut measured = GateCoverage {
            pod_checks: 20,
            ..Default::default()
        };
        measured.record("latency", "measured");
        measured.record("error-rate", "measured");
        assert!(measured.measured("latency"));
        assert!(measured.summary().contains("latency=measured"));
        assert!(measured.summary().contains("error-rate=measured"));

        let mut idle = GateCoverage {
            pod_checks: 20,
            ..Default::default()
        };
        idle.record("latency", "no-candidate:no-series");
        idle.record("error-rate", "no-delta");
        assert!(!idle.measured("latency"));
        assert!(idle.summary().contains("latency=NO-EVIDENCE"));
        assert!(idle.summary().contains("no-candidate:no-series x1"));
        assert!(idle.summary().contains("error-rate=NO-EVIDENCE"));
        assert_ne!(measured.summary(), idle.summary());

        // 判据一次都没跑与跑过但没读数，也不是同一件事。
        let not_run = GateCoverage {
            pod_checks: 20,
            ..Default::default()
        };
        assert!(not_run.summary().contains("latency=NO-EVIDENCE[not-run]"));
        assert_ne!(not_run.summary(), idle.summary());

        // 参考点被反复重取也要出现在结论里：那是「一直在换观测对象」。
        let mut flapping = GateCoverage {
            pod_checks: 20,
            window_resets: 7,
            ..Default::default()
        };
        flapping.record("latency", "measured");
        flapping.record("error-rate", "measured");
        assert!(flapping.summary().contains("window-resets=7"));
        assert_ne!(flapping.summary(), measured.summary());
    }

    /// 正文没有语义标记时按窗口求和处理：旧版正文的行为不能因为这次改造
    /// 而变成按增量解读（那会把窗口边界的抖动当成速率）。
    #[test]
    fn absent_marker_reads_the_body_as_windowed() {
        let body = "# TYPE http_requests_total counter\n\
                    http_requests_total{status=\"200\"} 40\n\
                    http_requests_total{status=\"500\"} 10\n\
                    http_request_duration_ms_bucket{le=\"88\"} 99\n\
                    http_request_duration_ms_bucket{le=\"+Inf\"} 100\n";
        let (signals, semantics, hist) = parse_prometheus_signals(body);
        assert_eq!(semantics, CounterSemantics::Windowed);
        assert_eq!(signals.requests, 50.0);
        assert_eq!(signals.errors, 10.0);
        assert_eq!(
            p99_between(&HistogramSnapshot::default(), &hist),
            LatencyRead::Measured(88.0)
        );
        // 5xx 也在分母里：排除它会系统性放大错误率。
        assert_eq!(windowed_rate(signals), Some(0.2));
    }

    /// 正文由生产侧的那份声明拼出来：两侧各写一份字面量就会各自漂移，
    /// 而漂移的表现是抓取端静默退回窗口语义，不报错。
    #[test]
    fn producer_declaration_reads_the_body_as_cumulative() {
        let body = format!(
            "{}\nhttp_requests_total{{status=\"200\"}} 40\nhttp_requests_total{{status=\"500\"}} 10\n",
            cog_core::cumulative_semantics_declaration()
        );
        let (_, semantics, _) = parse_prometheus_signals(&body);
        assert_eq!(semantics, CounterSemantics::Cumulative);
        // 增量下限用请求增量比对，不受错误数影响。
        assert_eq!(
            error_rate(
                signals(5.0, 100.0),
                signals(15.0, 300.0),
                CounterSemantics::Cumulative,
                100.0
            ),
            Some(0.05)
        );
        // 计数器下降说明自称的语义与取值不符，不能拿负增量算速率。
        assert_eq!(
            error_rate(
                signals(5.0, 300.0),
                signals(6.0, 100.0),
                CounterSemantics::Cumulative,
                10.0
            ),
            None
        );
    }
}
