//! GitOps 拉取端（docs/2026-08-06_真自治全进化无人值守方案.md 路线 B）。
//!
//! 每个集群各跑一个（含主集群），周期 poll 中央仓库 release 分支：
//!
//! ```text
//! poll 中央仓库 → 发现新 HEAD
//!   → 找 HEAD 上的 promote/* tag，读 tag message（level / patch_id）
//!   → 台账幂等：本集群已处理过该 patch 则跳过
//!   → L0（l0_config）：提取变化的配置文件
//!       deploy/k3s/cogneva-json-configmap.yaml → kubectl apply（ConfigWatcher 热更新）
//!       prompts/** → 重建 prompts configmap → kubectl apply（hot_reload 热更新）
//!   → L1（l1_rollout）：构建/拉取镜像 → 金丝雀发布
//!       set image + rollout pause（新副本先起，旧副本不动）
//!       → 看护（readiness + restart count + 可选 metrics URL 阈值比对）
//!       → 通过：rollout resume 全量；异常：rollout undo 回滚 + 熔断计数
//!   → 全程写本集群台账（cluster 字段区分集群）
//! ```
//!
//! 每个集群的晋级节奏、看护、回滚、熔断都是本地决策——一个集群
//! 金丝雀失败只影响自己，不影响其他集群。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::GitOpsConfig;
use cog_core::{PromotionLedger, PromotionRecord, PromotionStatus, SFError, SFResult};
use tracing::{info, warn};

/// 一次待处理的晋级（从 release 分支 HEAD + promote tag 解析出来）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionCandidate {
    pub patch_id: String,
    pub level: String,
    pub commit: String,
    pub eval_summary: Option<String>,
}

/// 解析 promote tag 的 message（`patch_id=..\nlevel=..\neval=..`）。
pub fn parse_tag_message(message: &str) -> (Option<String>, Option<String>, Option<String>) {
    let mut patch_id = None;
    let mut level = None;
    let mut eval_summary = None;
    for line in message.lines() {
        if let Some((k, v)) = line.split_once('=') {
            match k.trim() {
                "patch_id" => patch_id = Some(v.trim().to_string()),
                "level" => level = Some(v.trim().to_string()),
                "eval" => {
                    let v = v.trim();
                    eval_summary = (v != "none").then(|| v.to_string());
                }
                _ => {}
            }
        }
    }
    (patch_id, level, eval_summary)
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
        let (patch_id, level, eval_summary) = parse_tag_message(&message);
        match (patch_id, level) {
            (Some(patch_id), Some(level)) => Ok(Some(PromotionCandidate {
                patch_id,
                level,
                commit: head,
                eval_summary,
            })),
            _ => {
                warn!(tag = %tag, "promote tag missing patch_id/level metadata; skipping");
                Ok(None)
            }
        }
    }

    /// 幂等：本集群是否已处理过该 patch。RolledBack 也算已处理——
    /// 已回滚的 patch 绝不能被下个 poll 周期重复金丝雀（会反复打挂
    /// 集群）；Failed 不在列，瞬时失败允许下轮重试。
    async fn already_processed(&self, patch_id: &str) -> SFResult<bool> {
        let recent = self.ledger.recent(50).await?;
        Ok(recent.iter().any(|r| {
            r.patch_id == *patch_id
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
            r.patch_id == candidate.patch_id
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
            patch_id = %candidate.patch_id,
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
        if self.already_processed(&candidate.patch_id).await? {
            // Pending 尾巴自愈：上一个 puller 进程可能在金丝雀中途被自家
            // 滚动替换，台账停在 Pending 永远不会终结（2026-08-07 双集群
            // 实测）。超过看护+滚动窗口的 Pending 记录做幂等 reconcile。
            if self.reconcile_pending(&candidate).await? {
                return Ok(true);
            }
            info!(patch_id = %candidate.patch_id, cluster = %self.cluster, "Promotion already processed by this cluster");
            return Ok(false);
        }

        info!(
            patch_id = %candidate.patch_id,
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
                info!(patch_id = %candidate.patch_id, cluster = %self.cluster, "Promotion applied: {note}");
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
                warn!(patch_id = %candidate.patch_id, cluster = %self.cluster, error = %e, "Promotion failed");
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
                "patch",
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

        let watch = self.watch_canary().await;
        match watch {
            Ok(()) => {
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
                let mut note = format!("canary passed; rolled out {image}");
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
                if rec.patch_id == candidate.patch_id && rec.cluster == self.cluster {
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

    /// 镜像获取：registry 模式直接引用仓库镜像；源码模式本地 buildah 构建。
    async fn obtain_image(&self, candidate: &PromotionCandidate) -> SFResult<String> {
        if let Some(registry) = &self.config.registry {
            return Ok(format!(
                "{}/cogneva:promote-{}",
                registry,
                sanitize(&candidate.patch_id)
            ));
        }
        // 源码级：本地构建。复用仓库内 Containerfile.local（buildah 叠层流）。
        let dir = self.work_dir();
        let image = format!(
            "localhost/cogneva:promote-{}",
            sanitize(&candidate.patch_id)
        );
        self.run(
            "buildah",
            &["build", "-f", "Containerfile.local", "-t", &image, "."],
            Some(&dir),
            3600,
        )
        .await?;
        Ok(image)
    }

    /// 金丝雀看护：watch 期内周期性检查新副本 readiness 与 restart
    /// count；配置了 metrics_url 时另做阈值比对。任何异常立即返回 Err。
    async fn watch_canary(&self) -> SFResult<()> {
        let watch_secs = self.config.canary_watch_secs;
        let interval = std::cmp::max(watch_secs / 20, 5);
        let baseline = self.scrape_metrics().await.ok().flatten();
        let started = std::time::Instant::now();
        let deadline = started + Duration::from_secs(watch_secs);
        // 宽限期：金丝雀刚 set image 时新副本还在 ContainerCreating，
        // not-ready 属正常；宽限过后仍不 ready 才算异常。
        let grace = Duration::from_secs(std::cmp::max(90, watch_secs / 4));

        while std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(interval)).await;
            self.check_pods_healthy(started.elapsed() < grace).await?;
            if let (Some(base), Ok(Some(current))) = (&baseline, self.scrape_metrics().await) {
                self.compare_metrics(base, &current)?;
            }
        }
        Ok(())
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

    /// 抓取 metrics URL（可选）。返回 (error_rate, p99_ms)。
    async fn scrape_metrics(&self) -> SFResult<Option<(f64, f64)>> {
        let Some(url) = &self.metrics_url else {
            return Ok(None);
        };
        let body = self
            .run("curl", &["-sf", "--max-time", "10", url], None, 15)
            .await?;
        let mut error_total = 0.0f64;
        let mut request_total = 0.0f64;
        let mut p99 = 0.0f64;
        for line in body.lines() {
            if line.starts_with("http_requests_total") && line.contains("status=\"5") {
                error_total += line
                    .split_whitespace()
                    .last()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.0);
            } else if line.starts_with("http_requests_total") {
                request_total += line
                    .split_whitespace()
                    .last()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.0);
            } else if line.starts_with("http_request_duration_seconds")
                && line.contains("quantile=\"0.99\"")
            {
                p99 = line
                    .split_whitespace()
                    .last()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.0)
                    * 1000.0;
            }
        }
        let error_rate = if request_total > 0.0 {
            error_total / request_total
        } else {
            0.0
        };
        Ok(Some((error_rate, p99)))
    }

    fn compare_metrics(&self, baseline: &(f64, f64), current: &(f64, f64)) -> SFResult<()> {
        let (base_err, base_p99) = *baseline;
        let (err, p99) = *current;
        if err > base_err * self.config.canary_error_rate_multiplier && err > 0.01 {
            return Err(SFError::Agent(format!(
                "canary error rate regressed: {err:.4} > baseline {base_err:.4} x {}",
                self.config.canary_error_rate_multiplier
            )));
        }
        if base_p99 > 0.0 && p99 > base_p99 * self.config.canary_p99_multiplier {
            return Err(SFError::Agent(format!(
                "canary p99 regressed: {p99:.0}ms > baseline {base_p99:.0}ms x {}",
                self.config.canary_p99_multiplier
            )));
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
            patch_id: candidate.patch_id.clone(),
            level: candidate.level.clone(),
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

fn sanitize(patch_id: &str) -> String {
    patch_id
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tag_message_full() {
        let (p, l, e) = parse_tag_message("patch_id=p-1\nlevel=l1_rollout\neval=Adopt z=2.0");
        assert_eq!(p.as_deref(), Some("p-1"));
        assert_eq!(l.as_deref(), Some("l1_rollout"));
        assert_eq!(e.as_deref(), Some("Adopt z=2.0"));
    }

    #[test]
    fn parse_tag_message_eval_none_becomes_none() {
        let (_p, _l, e) = parse_tag_message("patch_id=p-1\nlevel=l0_config\neval=none");
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

        // publisher 推。
        let publisher = crate::GitOpsPublisher::new(
            GitOpsConfig {
                repo_url: central.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            work.path(),
            work.path(),
        );
        let patch = crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodePatch,
            artifact_id: "p-42".into(),
            description: "test".into(),
            content: String::new(),
            status: crate::types::EvolutionStatus::Active,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };
        crate::PromotionChannel::publish_rollout(&publisher, &patch)
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
        assert_eq!(candidate.patch_id, "p-42");
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
                patch_id: "p-1".into(),
                level: "l1_rollout".into(),
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

    #[test]
    fn metrics_comparison_thresholds() {
        let puller = GitOpsPuller::new(
            GitOpsConfig::default(),
            Arc::new(cog_storage::MemoryStateBackend::new()),
            "c".into(),
        );
        // 基线错误率 2%，现 2.5%（1.25x，未超 1.5x）→ 通过。
        assert!(puller
            .compare_metrics(&(0.02, 100.0), &(0.025, 120.0))
            .is_ok());
        // 现 4%（2x）→ 回归。
        assert!(puller
            .compare_metrics(&(0.02, 100.0), &(0.04, 120.0))
            .is_err());
        // p99 100ms → 140ms（1.4x > 1.3x）→ 回归。
        assert!(puller
            .compare_metrics(&(0.02, 100.0), &(0.02, 140.0))
            .is_err());
        // p99 125ms（1.25x）→ 通过。
        assert!(puller
            .compare_metrics(&(0.02, 100.0), &(0.02, 125.0))
            .is_ok());
    }
}
