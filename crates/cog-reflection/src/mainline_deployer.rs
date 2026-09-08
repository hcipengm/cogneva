//! 主线跟踪自动部署器：让集群自治跟踪公版 main。
//!
//! 构建侧（进化 Pod 内，[`MainlineDeployer`] + [`run_mainline_loop`]）：
//! 周期检测集群内 bare 仓库（/host-git）的 main 前进 → 沙盒源码树 reset 到
//! 新 rev → cargo build（PVC target 增量缓存）→ buildah 基于"当前在跑的
//! 不可变 tag"打最小 overlay → 推集群内 registry（`main-<rev12>` 不可变 tag
//! + `:local` 浮动签）→ 派独立 Job 跑滚动。
//!
//! 滚动侧（Job 内，[`RolloutExecutor`]，二进制子命令 `cogneva mainline-rollout`）：
//! 按固定顺序 set image 四个 deployment（网关代理面先行、进化宿主最后），
//! 每个部署过 rollout 完成 + Pod 健康双门禁，全部滚完后 soak 观察窗复查；
//! 任一失败把已滚部署反向 set image 回 prev tag。
//!
//! 滚动必须放 Job 而不是进化 Pod 自身：cogneva-evolution 是 Recreate 单副本，
//! 部署器就跑在里面，对自己 set image 会立刻杀掉门禁/回滚逻辑，新镜像
//! crashloop 时无人 undo，进化面永久宕。Job 用新镜像跑还顺带 smoke test：
//! 新二进制起不来则一次 set image 都不会发生。

use std::path::{Path, PathBuf};
use std::time::Duration;

use cog_core::{SFError, SFResult, ShutdownSignal};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::{MainlineDeployerConfig, RolloutTargetConfig};

/// buildah 存储库放 sandbox PVC：与金丝雀 publisher 共享基镜像层缓存，
/// Pod 重启不丢。
const BUILDAH_STORAGE: &str = "/opt/cogneva/sandbox/containers/storage";
const BUILDAH_RUNROOT: &str = "/opt/cogneva/sandbox/containers/run";

// ---------------------------------------------------------------------------
// 纯函数（无 IO，单测覆盖）
// ---------------------------------------------------------------------------

/// 完整 rev 取短 id（git 短 sha 惯例 12 字符）。
fn rev12(rev: &str) -> &str {
    if rev.len() >= 12 {
        &rev[..12]
    } else {
        rev
    }
}

/// 不可变主线镜像引用：`<registry>/cogneva:main-<rev12>`。
pub fn main_image(registry: &str, rev: &str) -> String {
    format!(
        "{}/cogneva:main-{}",
        registry.trim_end_matches('/'),
        rev12(rev)
    )
}

/// 浮动签镜像引用：`<registry>/cogneva:local`。
pub fn local_image(registry: &str) -> String {
    format!("{}/cogneva:local", registry.trim_end_matches('/'))
}

/// 从镜像引用解析 `main-<rev>` 的 rev 片段；非主线 tag（:local、promote-*、
/// 节点 localhost/cogneva:local 等）返回 None。
fn parse_main_rev(image: &str) -> Option<&str> {
    let tag = image.rsplit(':').next()?;
    tag.strip_prefix("main-")
}

/// 滚动 Job 名（含 rev，天然幂等键）。
pub fn job_name(rev: &str) -> String {
    format!("cogneva-mainline-{}", rev12(rev))
}

/// Pod 双标签选择器：主应用/网关/执行器的 name 标签都是 `cogneva`，
/// 单标签会跨部署误判（gitops puller 旧代码只用 name= 的同源缺陷）。
fn pod_selector(name: &str, component: &str) -> String {
    format!(
        "app.kubernetes.io/name={},app.kubernetes.io/component={}",
        name, component
    )
}

/// 四部署当前镜像的归类。
#[derive(Debug, PartialEq, Eq)]
enum DeployedState {
    /// 四部署统一跑在 `main-<rev>` 上。
    Main(String),
    /// 全都不是主线 tag（迁移前的 localhost/cogneva:local 时代）：允许首轮
    /// 以 registry :local 为基底前进。
    Legacy,
    /// 混合态（部分主线、部分旧 tag，或主线 rev 不一致）：上一轮滚动未
    /// 收敛，绝不触发新一轮。
    Mixed,
}

fn classify_deployed(images: &[String]) -> DeployedState {
    let mut revs: Vec<String> = Vec::new();
    let mut non_main = 0usize;
    for img in images {
        match parse_main_rev(img) {
            Some(r) => revs.push(r.to_string()),
            None => non_main += 1,
        }
    }
    if revs.is_empty() {
        return DeployedState::Legacy;
    }
    if non_main > 0 {
        return DeployedState::Mixed;
    }
    let first = revs[0].clone();
    if revs.iter().all(|r| r == &first) {
        DeployedState::Main(first)
    } else {
        DeployedState::Mixed
    }
}

/// 前进判定（纯函数）。`is_ancestor` = deployed rev 是 bare rev 的祖先
/// （git merge-base 判定结果，作为参数传入保持本函数无 IO）。
#[derive(Debug, PartialEq, Eq)]
enum AdvanceDecision {
    Advance,
    SameRev,
    NotAncestor,
    Mixed,
    InCooldown,
    MaxAttempts,
}

fn evaluate_advance(
    bare_rev: &str,
    deployed: &DeployedState,
    is_ancestor: bool,
    now_ts: i64,
    cooldown_until: i64,
    attempts: u32,
    max_attempts: u32,
) -> AdvanceDecision {
    match deployed {
        DeployedState::Legacy => AdvanceDecision::Advance,
        DeployedState::Mixed => AdvanceDecision::Mixed,
        DeployedState::Main(d) => {
            if rev12(d) == rev12(bare_rev) {
                return AdvanceDecision::SameRev;
            }
            if !is_ancestor {
                return AdvanceDecision::NotAncestor;
            }
            if now_ts < cooldown_until {
                return AdvanceDecision::InCooldown;
            }
            if attempts >= max_attempts {
                return AdvanceDecision::MaxAttempts;
            }
            AdvanceDecision::Advance
        }
    }
}

/// 构建锁陈旧判定：持锁进程已死，或锁龄超过构建超时（进程僵死/被杀）。
fn lock_is_stale(age_secs: u64, timeout_secs: u64, pid_alive: bool) -> bool {
    !pid_alive || age_secs > timeout_secs
}

// ---------------------------------------------------------------------------
// 持久状态（state.json，tmp+rename 原子写；权威事实是集群，PVC 丢了能重建）
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum Phase {
    SourceReady,
    Built,
    Pushed,
    Dispatched,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct InFlight {
    rev: String,
    phase: Phase,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct MainlineState {
    last_good_tag: Option<String>,
    last_good_rev: Option<String>,
    in_flight: Option<InFlight>,
    failed_rev: Option<String>,
    failed_cooldown_until: i64,
    failed_attempts: u32,
}

// ---------------------------------------------------------------------------
// 构建侧
// ---------------------------------------------------------------------------

pub struct MainlineDeployer {
    cfg: MainlineDeployerConfig,
    /// 沙盒源码树（进化 Pod workingDir，/opt/cogneva/sandbox/src）。
    src_dir: PathBuf,
}

impl MainlineDeployer {
    pub fn new(cfg: MainlineDeployerConfig, src_dir: impl Into<PathBuf>) -> Self {
        Self {
            cfg,
            src_dir: src_dir.into(),
        }
    }

    fn state_path(&self) -> PathBuf {
        Path::new(&self.cfg.state_dir).join("state.json")
    }

    fn lock_path(&self) -> PathBuf {
        Path::new(&self.cfg.state_dir).join("build.lock")
    }

    fn load_state(&self) -> MainlineState {
        match std::fs::read_to_string(self.state_path()) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
                warn!(error = %e, "mainline state.json corrupt; starting fresh");
                MainlineState::default()
            }),
            Err(_) => MainlineState::default(),
        }
    }

    fn save_state(&self, state: &MainlineState) -> SFResult<()> {
        std::fs::create_dir_all(&self.cfg.state_dir)
            .map_err(|e| SFError::IO(format!("create state dir {}: {e}", self.cfg.state_dir)))?;
        let path = self.state_path();
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(state)?;
        std::fs::write(&tmp, text).map_err(|e| SFError::IO(format!("write state tmp: {e}")))?;
        std::fs::rename(&tmp, &path).map_err(|e| SFError::IO(format!("rename state: {e}")))?;
        Ok(())
    }

    /// 构建串行锁：4C/7.5G 节点禁并发构建。锁陈旧（进程死/超构建超时）
    /// 可抢占；返回 None 表示有其他构建在跑。
    fn acquire_lock(&self) -> Option<BuildLock> {
        let path = self.lock_path();
        if let Ok(text) = std::fs::read_to_string(&path) {
            let pid: u32 = text
                .lines()
                .next()
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            let age = std::fs::metadata(&path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let pid_alive = pid != 0 && Path::new(&format!("/proc/{pid}")).exists();
            if !lock_is_stale(age, self.cfg.build_timeout_secs, pid_alive) {
                info!(
                    pid,
                    age_secs = age,
                    "mainline build lock held by another process"
                );
                return None;
            }
            warn!(pid, age_secs = age, "mainline build lock stale; preempting");
            let _ = std::fs::remove_file(&path);
        }
        let _ = std::fs::create_dir_all(&self.cfg.state_dir);
        std::fs::write(&path, format!("{}\n", std::process::id())).ok()?;
        Some(BuildLock { path })
    }

    async fn run_cmd(
        &self,
        program: &str,
        args: &[&str],
        workdir: Option<&Path>,
        timeout_secs: u64,
    ) -> SFResult<String> {
        let cmdline = format!("{} {}", program, args.join(" "));
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args).kill_on_drop(true);
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
        let fut = cmd.output();
        let output = tokio::time::timeout(Duration::from_secs(timeout_secs), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after {timeout_secs}s")))?
            .map_err(|e| SFError::IO(format!("failed to run {program}: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(SFError::IO(format!(
                "{cmdline} failed: {}{}",
                stderr, stdout
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// buildah 子命令统一加 PVC 存储全局参数（全局参数必须在子命令前）。
    async fn buildah(&self, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let mut full: Vec<&str> = vec!["--root", BUILDAH_STORAGE, "--runroot", BUILDAH_RUNROOT];
        full.extend_from_slice(args);
        self.run_cmd(&self.cfg.builder_bin, &full, None, timeout_secs)
            .await
    }

    async fn kubectl(&self, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let mut full: Vec<&str> = vec!["-n", &self.cfg.namespace];
        full.extend_from_slice(args);
        self.run_cmd(&self.cfg.kubectl_bin, &full, None, timeout_secs)
            .await
    }

    async fn git_src(&self, args: &[&str]) -> SFResult<String> {
        self.run_cmd("git", args, Some(&self.src_dir), 120).await
    }

    /// bare 仓库指定分支的完整 rev。
    async fn bare_main_rev(&self) -> SFResult<String> {
        let out = self
            .run_cmd(
                "git",
                &[
                    "--git-dir",
                    &self.cfg.bare_repo,
                    "rev-parse",
                    &self.cfg.branch,
                ],
                None,
                30,
            )
            .await?;
        Ok(out.trim().to_string())
    }

    /// older 是否 newer 的祖先（拒倒退/分叉）。
    async fn is_ancestor(&self, older: &str, newer: &str) -> bool {
        tokio::process::Command::new("git")
            .args([
                "--git-dir",
                &self.cfg.bare_repo,
                "merge-base",
                "--is-ancestor",
                older,
                newer,
            ])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// buildah 在 Pod 内 push/from 的端点（集群 DNS，http）。
    fn push_endpoint(&self) -> String {
        if self.cfg.registry.trim().is_empty() {
            format!(
                "cogneva-registry.{}.svc.cluster.local:5000",
                self.cfg.namespace
            )
        } else {
            self.cfg.registry.trim_end_matches('/').to_string()
        }
    }

    /// kubelet 在节点上 pull 的镜像引用端点（NodePort localhost；节点不解析
    /// 集群 DNS）。Job manifest 镜像与 set image 引用必须用这个端点。
    fn pull_endpoint(&self) -> String {
        if self.cfg.local_registry.trim().is_empty() {
            "localhost:30500".to_string()
        } else {
            self.cfg.local_registry.trim_end_matches('/').to_string()
        }
    }

    /// 四个 deployment 当前在跑的镜像（按 cfg.targets 顺序）。
    async fn deployed_images(&self) -> SFResult<Vec<String>> {
        let mut images = Vec::new();
        for t in &self.cfg.targets {
            let jsonpath = format!(
                "jsonpath={{.spec.template.spec.containers[?(@.name==\"{}\")].image}}",
                t.container
            );
            let img = self
                .kubectl(&["get", "deployment", &t.deployment, "-o", &jsonpath], 30)
                .await?;
            images.push(img.trim().to_string());
        }
        Ok(images)
    }

    /// 叠层基底（buildah from，Pod 内走 push 端点）：已在主线 tag 上则用
    /// 不可变 main-<prev>；迁移首轮（Legacy）用 registry :local
    /// （swap-image/bootstrap 已播种）。绝不基于节点 localhost/cogneva:local
    /// ——进化 Pod 没有节点 containerd socket，且浮动签脱节会自我放大旧镜像。
    fn resolve_base(&self, deployed: &DeployedState) -> String {
        let endpoint = self.push_endpoint();
        match deployed {
            DeployedState::Main(rev) => main_image(&endpoint, rev),
            _ => local_image(&endpoint),
        }
    }

    /// 一轮轮询。
    pub async fn poll_once(&self) -> SFResult<()> {
        let mut state = self.load_state();

        let bare = self.bare_main_rev().await?;
        let images = self.deployed_images().await?;
        let deployed = classify_deployed(&images);

        // 在飞任务收敛/终态处理。
        if let Some(inflight) = state.in_flight.clone() {
            match &deployed {
                DeployedState::Main(d) if rev12(d) == rev12(&inflight.rev) => {
                    info!(rev = %rev12(&inflight.rev), "mainline rollout converged");
                    state.last_good_rev = Some(inflight.rev.clone());
                    state.last_good_tag = Some(main_image(&self.pull_endpoint(), &inflight.rev));
                    state.in_flight = None;
                    state.failed_rev = None;
                    state.failed_attempts = 0;
                    state.failed_cooldown_until = 0;
                    self.save_state(&state)?;
                    return Ok(());
                }
                _ => {}
            }
            if inflight.phase == Phase::Dispatched {
                match self.job_status(&job_name(&inflight.rev)).await? {
                    JobStatus::Complete => {
                        // Job 完成但部署尚未收敛（API 缓存/滚动尾巴）：下轮再判。
                        info!(rev = %rev12(&inflight.rev), "rollout job complete; awaiting deployment convergence");
                        return Ok(());
                    }
                    JobStatus::Failed => {
                        warn!(rev = %rev12(&inflight.rev), "mainline rollout job failed; rollback handled by job itself");
                        let attempts = if state.failed_rev.as_deref() == Some(inflight.rev.as_str())
                        {
                            state.failed_attempts + 1
                        } else {
                            1
                        };
                        state.failed_rev = Some(inflight.rev.clone());
                        state.failed_attempts = attempts;
                        state.failed_cooldown_until =
                            chrono::Utc::now().timestamp() + self.cfg.failure_cooldown_secs as i64;
                        state.in_flight = None;
                        self.save_state(&state)?;
                        return Ok(());
                    }
                    JobStatus::Running | JobStatus::NotFound => return Ok(()),
                }
            }
            // phase < Dispatched：上轮在构建中途重启，落到下方构建流程
            // 幂等重跑（同 tag buildah/push 可重复）。
        }

        let attempts = if state.failed_rev.as_deref() == Some(bare.as_str()) {
            state.failed_attempts
        } else {
            0
        };
        let is_ancestor = match &deployed {
            DeployedState::Main(d) => self.is_ancestor(d, &bare).await,
            _ => true,
        };
        let now = chrono::Utc::now().timestamp();
        let decision = evaluate_advance(
            &bare,
            &deployed,
            is_ancestor,
            now,
            state.failed_cooldown_until,
            attempts,
            self.cfg.max_attempts_per_rev,
        );
        match decision {
            AdvanceDecision::Advance => {}
            AdvanceDecision::SameRev => return Ok(()),
            other => {
                info!(decision = ?other, "mainline advance skipped");
                return Ok(());
            }
        }

        let _lock = match self.acquire_lock() {
            Some(lock) => lock,
            None => return Ok(()),
        };

        // 同一镜像两个引用端点：buildah 在 Pod 内走集群 DNS push/from；
        // Job manifest 与 set image 走节点 NodePort（kubelet 不解析集群 DNS）。
        let push_tag = main_image(&self.push_endpoint(), &bare);
        let pull_tag = main_image(&self.pull_endpoint(), &bare);
        let base_tag = self.resolve_base(&deployed);
        info!(rev = %rev12(&bare), base = %base_tag, "mainline advance: building");

        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::SourceReady,
        });
        self.save_state(&state)?;

        // 1. 源码树对齐新 rev（脏树/有未发布 commit 时跳过本轮，绝不丢在途工作；
        // 具体原因由 ensure_source_at 内按场景记日志）。
        if !self.ensure_source_at(&bare).await? {
            info!("mainline source alignment skipped this round");
            state.in_flight = None;
            self.save_state(&state)?;
            return Ok(());
        }

        // 2. cargo build --release（target/ 在 source PVC 上增量缓存）。
        self.build_binary(&bare).await?;
        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::Built,
        });
        self.save_state(&state)?;

        // 3. buildah 叠层并推 registry（不可变 tag + :local 浮动签）。
        self.build_and_push(&bare, &base_tag, &push_tag).await?;
        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::Pushed,
        });
        self.save_state(&state)?;

        // 4. 派滚动 Job（explosion radius 外；新镜像 smoke test）。Job 启动后
        // 自己快照各部署当前镜像作为回滚目标，无需部署器推导 prev。
        self.dispatch_job(&bare, &pull_tag).await?;
        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::Dispatched,
        });
        self.save_state(&state)?;
        info!(rev = %rev12(&bare), tag = %pull_tag, "mainline rollout job dispatched");
        Ok(())
    }

    /// 源码树 reset 到目标 rev。返回 false = 沙盒忙（脏树/未发布 commit），
    /// 本轮跳过。安全规则与 change_pipeline::sync_with_upstream 同源。
    async fn ensure_source_at(&self, rev: &str) -> SFResult<bool> {
        if self
            .git_src(&["fetch", "local", &self.cfg.branch])
            .await
            .is_err()
        {
            warn!("mainline: fetch local main failed; keeping current tree");
            return Ok(false);
        }
        let dirty = self
            .git_src(&["status", "--porcelain"])
            .await
            .map(|s| !s.is_empty())
            .unwrap_or(true);
        if dirty {
            info!("mainline: sandbox tree dirty (in-flight change); skip this round");
            return Ok(false);
        }
        let head = self
            .git_src(&["rev-parse", "HEAD"])
            .await
            .unwrap_or_default();
        if rev12(head.trim()) == rev12(rev) {
            return Ok(true);
        }
        // HEAD 必须是目标 rev 的祖先（无未发布本地 commit）。
        let ancestor = tokio::process::Command::new("git")
            .args(["merge-base", "--is-ancestor", "HEAD", rev])
            .current_dir(&self.src_dir)
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !ancestor {
            // 与"脏树（在途变更，下轮自愈）"不同：分叉/无共同祖先是永久性
            // 卡死（典型：PVC 上残留 bare 重建前的旧克隆历史），每 10 分钟
            // 静默跳过，必须告警并给出处置方法。
            warn!(
                head = %head.trim(),
                target = %rev,
                "mainline: sandbox HEAD is not an ancestor of upstream main; refusing \
                 reset to protect unpublished commits. If the tree holds no in-flight \
                 change (e.g. stale pre-reseed history), an operator can realign with \
                 git -C <sandbox-src> reset --hard local/main"
            );
            return Ok(false);
        }
        self.git_src(&["reset", "--hard", rev]).await?;
        Ok(true)
    }

    async fn build_binary(&self, rev: &str) -> SFResult<()> {
        let jobs = self.cfg.cargo_build_jobs.to_string();
        let cmdline = format!("cargo build --release --bin cogneva (jobs={jobs})");
        let mut cmd = tokio::process::Command::new("cargo");
        cmd.args(["build", "--release", "--bin", "cogneva"])
            .current_dir(&self.src_dir)
            .env("CARGO_BUILD_JOBS", &jobs)
            // build.rs 回退只嵌 7 位短 sha，叠层后的 --version 校验匹配 12
            // 位前缀会必败；显式注入完整 rev（与 swap-image 双保险同源）。
            .env("COGNEVA_GIT_REVISION", rev)
            .kill_on_drop(true);
        let fut = cmd.output();
        let output = tokio::time::timeout(Duration::from_secs(self.cfg.build_timeout_secs), fut)
            .await
            .map_err(|_| {
                SFError::IO(format!(
                    "{cmdline} timed out after {}s",
                    self.cfg.build_timeout_secs
                ))
            })?
            .map_err(|e| SFError::IO(format!("failed to run cargo: {e}")))?;
        if !output.status.success() {
            return Err(SFError::Agent(format!(
                "mainline cargo build failed:\n{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        // strip 失败不致命（二进制可跑，只是体积大）。
        let bin = self.src_dir.join("target/release/cogneva");
        let _ = self
            .run_cmd("strip", &[bin.to_str().unwrap_or("")], None, 60)
            .await;
        Ok(())
    }

    /// buildah 叠层：FROM 当前在跑 tag → 换二进制 + migrations → --version
    /// 校验内嵌 rev → commit 不可变 tag → 同步 tag :local → 双推。
    async fn build_and_push(&self, rev: &str, base: &str, new_tag: &str) -> SFResult<()> {
        let ctr = self.buildah(&["from", base], 1800).await?;
        let ctr = ctr.trim().to_string();
        let result = self.buildah_steps(&ctr, rev, new_tag).await;
        if let Err(e) = self.buildah(&["rm", &ctr], 60).await {
            warn!(error = %e, "buildah rm failed after build");
        }
        result?;
        Ok(())
    }

    async fn buildah_steps(&self, ctr: &str, rev: &str, new_tag: &str) -> SFResult<()> {
        let bin = self.src_dir.join("target/release/cogneva");
        let migrations = self.src_dir.join("crates/cog-storage/migrations");
        self.buildah(
            &["copy", ctr, bin.to_str().unwrap(), "/opt/cogneva/cogneva"],
            300,
        )
        .await?;
        self.buildah(
            &[
                "copy",
                ctr,
                migrations.to_str().unwrap(),
                "/opt/cogneva/crates/cog-storage/migrations",
            ],
            300,
        )
        .await?;

        // 换版即验证：新二进制必须能自报版本，且内嵌 rev 是目标 rev。
        let version = self
            .buildah(
                &["run", ctr, "--", "/opt/cogneva/cogneva", "--version"],
                120,
            )
            .await?;
        if !version.contains(rev12(rev)) {
            return Err(SFError::Agent(format!(
                "built binary --version {version:?} does not contain target rev {}",
                rev12(rev)
            )));
        }

        self.buildah(
            &[
                "config",
                "--label",
                &format!("org.opencontainers.image.revision={rev}"),
                ctr,
            ],
            60,
        )
        .await?;
        self.buildah(&["commit", ctr, new_tag], 600).await?;

        // 浮动签权威同步落 registry：静态清单/GitOps apply pin 的是
        // registry :local，主线每推一个不可变 tag 都把 :local 前移。
        let local = local_image(&self.push_endpoint());
        self.buildah(&["tag", new_tag, &local], 60).await?;
        self.buildah(&["push", "--tls-verify=false", new_tag], 900)
            .await?;
        self.buildah(&["push", "--tls-verify=false", &local], 900)
            .await?;
        info!(image = %new_tag, "mainline overlay image pushed to registry");
        Ok(())
    }

    /// 派滚动 Job。`kubectl apply -f -` 幂等：同 rev 重跑 Job 已存在时
    /// 视为已派发（apply 是 upsert，不会重启已完成 Job）。new_tag 必须是
    /// 节点 pull 端点引用（kubelet 经 NodePort 拉取）。
    async fn dispatch_job(&self, rev: &str, new_tag: &str) -> SFResult<()> {
        let mut manifest = serde_json::json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name(rev),
                "namespace": self.cfg.namespace,
                "labels": {
                    "app.kubernetes.io/name": "cogneva",
                    "app.kubernetes.io/component": "mainline-rollout",
                },
            },
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": self.cfg.rollout_timeout_secs * 6 + self.cfg.soak_secs,
                "ttlSecondsAfterFinished": 86400,
                "template": {
                    "metadata": {
                        "labels": {
                            "app.kubernetes.io/name": "cogneva",
                            "app.kubernetes.io/component": "mainline-rollout",
                        },
                    },
                    "spec": {
                        "serviceAccountName": "cogneva-evolution",
                        "restartPolicy": "Never",
                        "containers": [{
                            "name": "mainline-rollout",
                            "image": new_tag,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/opt/cogneva/cogneva"],
                            "args": [
                                "mainline-rollout",
                                "--tag", new_tag,
                                "--ns", &self.cfg.namespace,
                                "--soak-secs", &self.cfg.soak_secs.to_string(),
                                "--restart-threshold", &self.cfg.restart_threshold.to_string(),
                                "--timeout", &self.cfg.rollout_timeout_secs.to_string(),
                            ],
                        }],
                    },
                },
            },
        });
        // Job Pod 也需要 kubectl：镜像不内置，挂载宿主 k3s 多调用二进制
        // （argv[0]=kubectl 即 kubectl）。配置为空（镜像自带/标准 K8s）时不挂。
        if !self.cfg.kubectl_host_path.is_empty() {
            manifest["spec"]["template"]["spec"]["volumes"] = serde_json::json!([
                {
                    "name": "kubectl-bin",
                    "hostPath": { "path": &self.cfg.kubectl_host_path, "type": "File" }
                }
            ]);
            manifest["spec"]["template"]["spec"]["containers"][0]["volumeMounts"] = serde_json::json!([
                {
                    "name": "kubectl-bin",
                    "mountPath": "/usr/local/bin/kubectl",
                    "readOnly": true
                }
            ]);
        }
        let body = serde_json::to_vec_pretty(&manifest)?;
        let mut child = tokio::process::Command::new(&self.cfg.kubectl_bin)
            .args(["-n", &self.cfg.namespace, "apply", "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| SFError::IO(format!("spawn kubectl apply: {e}")))?;
        {
            use tokio::io::AsyncWriteExt;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| SFError::IO("kubectl stdin unavailable".into()))?;
            stdin.write_all(&body).await?;
            stdin.shutdown().await?;
        }
        let output = tokio::time::timeout(Duration::from_secs(60), child.wait_with_output())
            .await
            .map_err(|_| SFError::IO("kubectl apply timed out".into()))?
            .map_err(|e| SFError::IO(format!("kubectl apply: {e}")))?;
        if !output.status.success() {
            return Err(SFError::IO(format!(
                "kubectl apply job failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }

    async fn job_status(&self, name: &str) -> SFResult<JobStatus> {
        let out = match self
            .kubectl(
                &[
                    "get",
                    "job",
                    name,
                    "-o",
                    "jsonpath={.status.succeeded} {.status.failed} {.status.active}",
                ],
                30,
            )
            .await
        {
            Ok(o) => o,
            Err(e) if e.to_string().contains("not found") || e.to_string().contains("NotFound") => {
                return Ok(JobStatus::NotFound)
            }
            Err(e) => return Err(e),
        };
        let parts: Vec<&str> = out.split_whitespace().collect();
        let succeeded = parts
            .first()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let failed = parts
            .get(1)
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        if succeeded >= 1 {
            Ok(JobStatus::Complete)
        } else if failed >= 1 {
            Ok(JobStatus::Failed)
        } else {
            Ok(JobStatus::Running)
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum JobStatus {
    Complete,
    Failed,
    Running,
    NotFound,
}

/// 构建锁守卫：Drop 时删锁文件。
struct BuildLock {
    path: PathBuf,
}

impl Drop for BuildLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// 构建侧后台循环入口（插件 spawn）。
pub async fn run_mainline_loop(
    deployer: std::sync::Arc<MainlineDeployer>,
    shutdown: ShutdownSignal,
) {
    // 宿主 bare 仓库（/host-git）与沙盒树属主/挂载场景会撞 git
    // dubious-ownership；safe.directory 只有 global 配置被采信（与
    // gitops puller 同源处理），启动时幂等写入。
    for dir in [deployer.cfg.bare_repo.as_str(), "/opt/cogneva/sandbox/src"] {
        let _ = tokio::process::Command::new("git")
            .args(["config", "--global", "--add", "safe.directory", dir])
            .output()
            .await;
    }
    let interval = Duration::from_secs(deployer.cfg.poll_interval_secs.max(30));
    info!(
        interval_secs = interval.as_secs(),
        push_endpoint = %deployer.push_endpoint(),
        pull_endpoint = %deployer.pull_endpoint(),
        "Mainline deployer loop started"
    );
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                if let Err(e) = deployer.poll_once().await {
                    warn!(error = %e, "mainline deployer poll failed");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 滚动侧（Job 内执行）
// ---------------------------------------------------------------------------

pub struct RolloutTarget {
    pub deployment: String,
    pub container: String,
    pub component: String,
    pub name: String,
}

pub struct RolloutPlan {
    /// 新镜像引用（节点 pull 端点，kubelet 经 NodePort 拉取）。
    pub tag: String,
    pub targets: Vec<RolloutTarget>,
}

impl RolloutPlan {
    pub fn from_config(cfg: &MainlineDeployerConfig, tag: String) -> Self {
        let targets = cfg
            .targets
            .iter()
            .map(|t: &RolloutTargetConfig| RolloutTarget {
                deployment: t.deployment.clone(),
                container: t.container.clone(),
                component: t.component.clone(),
                name: t.name.clone(),
            })
            .collect();
        Self { tag, targets }
    }
}

pub struct RolloutExecutor {
    kubectl: String,
    ns: String,
    soak_secs: u64,
    restart_threshold: u32,
    rollout_timeout_secs: u64,
}

impl RolloutExecutor {
    pub fn new(
        kubectl: impl Into<String>,
        ns: impl Into<String>,
        soak_secs: u64,
        restart_threshold: u32,
        rollout_timeout_secs: u64,
    ) -> Self {
        Self {
            kubectl: kubectl.into(),
            ns: ns.into(),
            soak_secs,
            restart_threshold,
            rollout_timeout_secs,
        }
    }

    async fn run_kubectl(&self, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let cmdline = format!("kubectl {}", args.join(" "));
        let mut full: Vec<&str> = vec!["-n", &self.ns];
        full.extend_from_slice(args);
        let fut = tokio::process::Command::new(&self.kubectl)
            .args(&full)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(Duration::from_secs(timeout_secs), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after {timeout_secs}s")))?
            .map_err(|e| SFError::IO(format!("failed to run kubectl: {e}")))?;
        if !output.status.success() {
            return Err(SFError::IO(format!(
                "{cmdline} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    async fn set_image(&self, t: &RolloutTarget, image: &str) -> SFResult<()> {
        let image_arg = format!("{}={}", t.container, image);
        self.run_kubectl(
            &[
                "set",
                "image",
                &format!("deployment/{}", t.deployment),
                &image_arg,
            ],
            60,
        )
        .await?;
        Ok(())
    }

    /// 快照单个部署当前在跑的镜像（回滚目标）。
    async fn current_image(&self, t: &RolloutTarget) -> SFResult<String> {
        let jsonpath = format!(
            "jsonpath={{.spec.template.spec.containers[?(@.name==\"{}\")].image}}",
            t.container
        );
        let img = self
            .run_kubectl(&["get", "deployment", &t.deployment, "-o", &jsonpath], 30)
            .await?;
        let img = img.trim().to_string();
        if img.is_empty() {
            return Err(SFError::Agent(format!(
                "deployment/{} has no image for container {}",
                t.deployment, t.container
            )));
        }
        Ok(img)
    }

    /// 轮询 deployment rollout 完成：observedGeneration 追上 generation 且
    /// updated/ready 副本数达期望（短查询，Job 在爆炸半径外不怕被杀）。
    async fn wait_rollout_complete(&self, t: &RolloutTarget) -> SFResult<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs(self.rollout_timeout_secs);
        loop {
            let out = self
                .run_kubectl(
                    &[
                        "get",
                        "deployment",
                        &t.deployment,
                        "-o",
                        "jsonpath={.metadata.generation} {.status.observedGeneration} {.spec.replicas} {.status.updatedReplicas} {.status.readyReplicas}",
                    ],
                    30,
                )
                .await?;
            let parts: Vec<&str> = out.split_whitespace().collect();
            if parts.len() >= 5 {
                let gen: u64 = parts[0].parse().unwrap_or(0);
                let obs: u64 = parts[1].parse().unwrap_or(0);
                let spec: u32 = parts[2].parse().unwrap_or(0);
                let updated: u32 = parts[3].parse().unwrap_or(0);
                let ready: u32 = parts[4].parse().unwrap_or(0);
                if obs >= gen && gen > 0 && updated == spec && ready == spec && spec > 0 {
                    return Ok(());
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(SFError::Agent(format!(
                    "rollout of deployment/{} did not complete within {}s (last: {out})",
                    t.deployment, self.rollout_timeout_secs
                )));
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Pod 健康信号：双标签选择器，查 restartCount/ready/waiting reason。
    /// `allow_pending` 宽限期内容忍 not-ready，但致命等待态（拉不到镜像、
    /// 配置错误、CrashLoop）无论宽限与否立即判病。空输出是选择器失效，
    /// 不能当健康。
    async fn pods_healthy(&self, t: &RolloutTarget, allow_pending: bool) -> SFResult<()> {
        let selector = pod_selector(&t.name, &t.component);
        let out = self
            .run_kubectl(
                &[
                    "get",
                    "pods",
                    "-l",
                    &selector,
                    "-o",
                    "jsonpath={range .items[*]}{.status.containerStatuses[0].restartCount}{' '}{.status.containerStatuses[0].ready}{' '}{.status.containerStatuses[0].state.waiting.reason}{\"\\n\"}{end}",
                ],
                30,
            )
            .await?;
        if out.trim().is_empty() {
            return Err(SFError::Agent(format!(
                "no pods found for selector {selector} (deployment/label mismatch?)"
            )));
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
                    "pod of deployment/{} in fatal waiting state {waiting_reason}: {line}",
                    t.deployment
                )));
            }
            if ready != "true" && !allow_pending {
                return Err(SFError::Agent(format!(
                    "pod of deployment/{} not ready: {line}",
                    t.deployment
                )));
            }
            if restarts > self.restart_threshold {
                return Err(SFError::Agent(format!(
                    "pod of deployment/{} restarting ({} restarts, threshold {}): {line}",
                    t.deployment, restarts, self.restart_threshold
                )));
            }
        }
        Ok(())
    }

    /// 按计划顺序滚动四部署，逐部署双门禁，全滚完后 soak 复查；任一失败
    /// 把已滚目标反向 set image 回 prev tag（不用 rollout undo——多目标
    /// 无事务性，undo 还会连带回退其他字段）。
    pub async fn run(&self, plan: &RolloutPlan) -> SFResult<()> {
        // 任何 set image 之前先快照各部署当前镜像作为回滚目标：per-target、
        // 端点零歧义，Legacy 首轮（节点 localhost/cogneva:local）也能精确回退。
        // 快照失败则一次 set image 都不发生（线上原样）。
        let mut prevs: Vec<(String, String)> = Vec::new();
        for t in &plan.targets {
            let img = self.current_image(t).await?;
            info!(deployment = %t.deployment, prev = %img, "mainline rollout: snapshot prev image");
            prevs.push((t.deployment.clone(), img));
        }
        let mut done: Vec<&RolloutTarget> = Vec::new();
        for target in &plan.targets {
            info!(deployment = %target.deployment, tag = %plan.tag, "mainline rollout: set image");
            if let Err(e) = self.set_image(target, &plan.tag).await {
                self.rollback(&done, &prevs).await;
                return Err(e);
            }
            if let Err(e) = self.wait_rollout_complete(target).await {
                done.push(target);
                self.rollback(&done, &prevs).await;
                return Err(e);
            }
            // 宽限期：新副本 ContainerCreating 时 not-ready 属正常。
            tokio::time::sleep(Duration::from_secs(15)).await;
            if let Err(e) = self.pods_healthy(target, false).await {
                done.push(target);
                self.rollback(&done, &prevs).await;
                return Err(e);
            }
            done.push(target);
        }

        info!(
            soak_secs = self.soak_secs,
            "mainline rollout: all targets updated, soaking"
        );
        tokio::time::sleep(Duration::from_secs(self.soak_secs)).await;
        for target in &plan.targets {
            if let Err(e) = self.pods_healthy(target, false).await {
                self.rollback(&done, &prevs).await;
                return Err(e);
            }
        }
        info!(tag = %plan.tag, "mainline rollout complete and healthy");
        Ok(())
    }

    /// 尽力回滚：已滚目标按快照的各自 prev 镜像反向 set image 并等收敛
    /// （不用 rollout undo——多目标无事务性，undo 还会连带回退其他字段）。
    /// 回滚本身失败只 warn（人工介入兜底），不掩盖原始错误。
    async fn rollback(&self, done: &[&RolloutTarget], prevs: &[(String, String)]) {
        warn!(count = done.len(), "mainline rollout failed; rolling back");
        for t in done.iter().rev() {
            let Some(prev) = prevs
                .iter()
                .find(|(d, _)| d == &t.deployment)
                .map(|(_, i)| i.as_str())
            else {
                warn!(deployment = %t.deployment, "no prev snapshot; skip rollback");
                continue;
            };
            if let Err(e) = self.set_image(t, prev).await {
                warn!(deployment = %t.deployment, error = %e, "rollback set image failed");
                continue;
            }
            if let Err(e) = self.wait_rollout_complete(t).await {
                warn!(deployment = %t.deployment, error = %e, "rollback wait failed");
            }
        }
    }
}

/// `cogneva mainline-rollout` 子命令入口（Job Pod 内执行）。targets 用
/// 内置默认四条（Job 不挂 configmap，与部署侧配置默认值同源）；回滚目标
/// 由 Job 启动时快照各部署当前镜像得到，不通过参数传入。
pub async fn run_rollout_cli() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let mut tag = String::new();
    let mut ns = "cogneva".to_string();
    let mut soak_secs = 120u64;
    let mut restart_threshold = 1u32;
    let mut timeout = 300u64;
    let mut kubectl = "kubectl".to_string();
    let mut i = 0;
    while i < args.len() {
        let value = |i: usize| -> Result<String, Box<dyn std::error::Error>> {
            args.get(i + 1)
                .cloned()
                .ok_or_else(|| format!("--{} requires a value", args[i]).into())
        };
        match args[i].as_str() {
            "--tag" => {
                tag = value(i)?;
                i += 2;
            }
            "--ns" => {
                ns = value(i)?;
                i += 2;
            }
            "--soak-secs" => {
                soak_secs = value(i)?.parse()?;
                i += 2;
            }
            "--restart-threshold" => {
                restart_threshold = value(i)?.parse()?;
                i += 2;
            }
            "--timeout" => {
                timeout = value(i)?.parse()?;
                i += 2;
            }
            "--kubectl" => {
                kubectl = value(i)?;
                i += 2;
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    if tag.is_empty() {
        return Err("--tag is required".into());
    }
    let cfg = MainlineDeployerConfig::default();
    let plan = RolloutPlan::from_config(&cfg, tag);
    let executor = RolloutExecutor::new(kubectl, ns, soak_secs, restart_threshold, timeout);
    executor.run(&plan).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rev12_truncates() {
        assert_eq!(rev12("abcdef0123456789"), "abcdef012345");
        assert_eq!(rev12("short"), "short");
    }

    #[test]
    fn image_refs_and_rev_parsing() {
        let img = main_image("cogneva-registry.cogneva.svc:5000", "abcdef0123456789");
        assert_eq!(
            img,
            "cogneva-registry.cogneva.svc:5000/cogneva:main-abcdef012345"
        );
        assert_eq!(parse_main_rev(&img), Some("abcdef012345"));
        assert_eq!(
            parse_main_rev("localhost/cogneva:local"),
            None,
            "node-local floating tag must not parse as mainline"
        );
        assert_eq!(parse_main_rev("cogneva-registry:5000/cogneva:local"), None);
        assert_eq!(
            parse_main_rev("cogneva-registry:5000/cogneva:promote-p-1"),
            None
        );
        assert_eq!(local_image("reg:5000/"), "reg:5000/cogneva:local");
        assert_eq!(
            job_name("abcdef0123456789"),
            "cogneva-mainline-abcdef012345"
        );
    }

    #[test]
    fn selector_uses_both_labels() {
        let s = pod_selector("cogneva", "gateway");
        assert_eq!(
            s,
            "app.kubernetes.io/name=cogneva,app.kubernetes.io/component=gateway"
        );
    }

    #[test]
    fn classify_deployed_states() {
        let reg = "r:5000";
        let main = |rev: &str| main_image(reg, rev);
        // 统一主线
        assert_eq!(
            classify_deployed(&[main("aa1111111111"), main("aa1111111111")]),
            DeployedState::Main("aa1111111111".into())
        );
        // 全非主线（迁移前）
        assert_eq!(
            classify_deployed(&[
                "localhost/cogneva:local".into(),
                "localhost/cogneva:local".into()
            ]),
            DeployedState::Legacy
        );
        // 混合
        assert_eq!(
            classify_deployed(&[main("aa1111111111"), "localhost/cogneva:local".into()]),
            DeployedState::Mixed
        );
        // 主线 rev 不一致（滚动未收敛）
        assert_eq!(
            classify_deployed(&[main("aa1111111111"), main("bb2222222222")]),
            DeployedState::Mixed
        );
    }

    #[test]
    fn advance_decisions() {
        let bare = "bb2222222222";
        let main = |r: &str| DeployedState::Main(r.into());
        let now = 1000i64;
        // 同 rev 不前进
        assert_eq!(
            evaluate_advance(bare, &main(bare), true, now, 0, 0, 2),
            AdvanceDecision::SameRev
        );
        // 非祖先（分叉/倒退）拒
        assert_eq!(
            evaluate_advance(bare, &main("aa1111111111"), false, now, 0, 0, 2),
            AdvanceDecision::NotAncestor
        );
        // 冷却中
        assert_eq!(
            evaluate_advance(bare, &main("aa1111111111"), true, now, 2000, 0, 2),
            AdvanceDecision::InCooldown
        );
        // 超次数
        assert_eq!(
            evaluate_advance(bare, &main("aa1111111111"), true, now, 0, 2, 2),
            AdvanceDecision::MaxAttempts
        );
        // 正常前进
        assert_eq!(
            evaluate_advance(bare, &main("aa1111111111"), true, now, 0, 1, 2),
            AdvanceDecision::Advance
        );
        // 迁移首轮（Legacy）直接前进
        assert_eq!(
            evaluate_advance(bare, &DeployedState::Legacy, false, now, 0, 0, 2),
            AdvanceDecision::Advance
        );
        // 混合态不前进
        assert_eq!(
            evaluate_advance(bare, &DeployedState::Mixed, true, now, 0, 0, 2),
            AdvanceDecision::Mixed
        );
    }

    #[test]
    fn lock_staleness() {
        assert!(!lock_is_stale(10, 3600, true));
        assert!(lock_is_stale(4000, 3600, true), "age over timeout is stale");
        assert!(lock_is_stale(10, 3600, false), "dead pid is stale");
    }

    #[test]
    fn state_serde_roundtrip() {
        let state = MainlineState {
            last_good_tag: Some("r:5000/cogneva:main-aa".into()),
            last_good_rev: Some("aa".into()),
            in_flight: Some(InFlight {
                rev: "bb".into(),
                phase: Phase::Pushed,
            }),
            failed_rev: None,
            failed_cooldown_until: 0,
            failed_attempts: 0,
        };
        let text = serde_json::to_string(&state).unwrap();
        let back: MainlineState = serde_json::from_str(&text).unwrap();
        assert_eq!(state, back);
    }

    #[test]
    fn rollout_plan_order_evolution_last() {
        let cfg = MainlineDeployerConfig::default();
        let plan = RolloutPlan::from_config(&cfg, "new".into());
        assert_eq!(plan.targets.len(), 4);
        assert_eq!(plan.targets[0].deployment, "cogneva-security-gateway");
        assert_eq!(plan.targets[1].deployment, "cogneva-sandbox-executor");
        assert_eq!(plan.targets[2].deployment, "cogneva");
        assert_eq!(plan.targets[3].deployment, "cogneva-evolution");
    }

    // --- 命令层测试：真实 git 仓库 + fake buildah/kubectl/cargo/strip ---

    use std::os::unix::fs::PermissionsExt;

    fn write_fake_bin(dir: &Path, name: &str, script: &str) {
        let path = dir.join(name);
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    async fn real_git(dir: &Path, args: &[&str]) {
        let out = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// 搭 bare + 工作树：工作树在 rev A，bare/main 在 rev B（A 是 B 祖先）。
    /// 返回 (bare_dir, work_dir, rev_a, rev_b)。
    async fn setup_repos(root: &Path) -> (PathBuf, PathBuf, String, String) {
        let bare = root.join("bare.git");
        let work = root.join("work");
        real_git(root, &["init", "--bare", bare.to_str().unwrap()]).await;
        real_git(
            root,
            &["clone", bare.to_str().unwrap(), work.to_str().unwrap()],
        )
        .await;
        real_git(&work, &["config", "user.email", "t@t.com"]).await;
        real_git(&work, &["config", "user.name", "T"]).await;
        std::fs::write(work.join("Cargo.toml"), "[workspace]\n").unwrap();
        std::fs::create_dir_all(work.join("crates/cog-storage/migrations")).unwrap();
        std::fs::write(work.join("crates/cog-storage/migrations/001.sql"), "").unwrap();
        real_git(&work, &["add", "."]).await;
        real_git(&work, &["commit", "-m", "a"]).await;
        // clone 后默认分支名随 git 配置（master/main），统一推到 bare 的 main。
        real_git(&work, &["push", "origin", "HEAD:main"]).await;
        real_git(&work, &["checkout", "-B", "main"]).await;
        real_git(&work, &["remote", "add", "local", bare.to_str().unwrap()]).await;
        let rev_a = real_git_stdout(&work, &["rev-parse", "HEAD"]).await;

        std::fs::write(work.join("lib.rs"), "fn b() {}\n").unwrap();
        real_git(&work, &["add", "."]).await;
        real_git(&work, &["commit", "-m", "b"]).await;
        real_git(&work, &["push", "origin", "main"]).await;
        let rev_b = real_git_stdout(
            &bare,
            &["--git-dir", bare.to_str().unwrap(), "rev-parse", "main"],
        )
        .await;
        // 工作树回到 A（模拟已部署 A，bare 前进到 B）。
        real_git(&work, &["reset", "--hard", &rev_a]).await;
        (bare, work, rev_a, rev_b)
    }

    async fn real_git_stdout(dir: &Path, args: &[&str]) -> String {
        let out = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// 命令层测试会改进程级 PATH（cargo/strip 靠 PATH 查找），用静态锁串行化，
    /// 避免并行测试互相串改环境。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// fake buildah：记录调用，from 输出容器名，run --version 输出带目标 rev
    /// 的版本串（rev 直接写进脚本，不走进程 env，避免并行竞态）。
    fn fake_buildah(dir: &Path, version_rev: &str) -> String {
        let log = dir.join("buildah.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "from" ]; then echo "ctr-test-123"; exit 0; fi
  if [ "$prev" = "run" ]; then echo "cogneva 0.5.7 (rev {version_rev})"; exit 0; fi
  prev="$a"
done
exit 0
"#,
            log = log.display(),
            version_rev = version_rev
        );
        write_fake_bin(dir, "fake-buildah", &script);
        dir.join("fake-buildah").to_string_lossy().to_string()
    }

    /// fake kubectl：deployment 镜像查询输出写死的 deployed_image（四部署同值），
    /// job 查询报 NotFound，apply 把 stdin 的 manifest 也落日志（job 名在
    /// manifest 里，不在 argv），其余成功。`-o` 参数必须带 `jsonpath=` 前缀
    /// （真 kubectl 对裸模板报 "unable to match a printer"，fake 同样拒绝，
    /// 否则这类漏前缀单测抓不到）。
    fn fake_kubectl(dir: &Path, deployed_image: &str) -> String {
        let log = dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer suitable for the output format \"$a\"" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"get deployment"*) echo "{deployed_image}" ;;
  *"get job"*) echo "Error: jobs.batch \"x\" not found" >&2; exit 1 ;;
  *"get pods"*) echo "0 true " ;;
  *"apply"*) cat >> '{log}'; echo "job.batch/x created" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            deployed_image = deployed_image
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    /// fake kubectl：pods 查询输出从文件读（测试逐例改写文件模拟不同 Pod 态）。
    /// `-o` 同样强制 jsonpath= 前缀（见 fake_kubectl 注释）。
    fn fake_kubectl_pods_from_file(dir: &Path, pods_file: &Path) -> String {
        let log = dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer suitable for the output format \"$a\"" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"get pods"*) cat '{pods_file}' ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            pods_file = pods_file.display()
        );
        write_fake_bin(dir, "fake-kubectl", &script);
        dir.join("fake-kubectl").to_string_lossy().to_string()
    }

    fn test_config(
        root: &Path,
        bare: &Path,
        buildah: &str,
        kubectl: &str,
    ) -> MainlineDeployerConfig {
        MainlineDeployerConfig {
            enabled: true,
            poll_interval_secs: 60,
            bare_repo: bare.to_string_lossy().into_owned(),
            branch: "main".into(),
            registry: "reg.local:5000".into(),
            local_registry: "localhost:30500".into(),
            namespace: "cogneva".into(),
            builder_bin: buildah.into(),
            kubectl_bin: kubectl.into(),
            kubectl_host_path: String::new(),
            state_dir: root.join("state").to_string_lossy().into_owned(),
            build_timeout_secs: 60,
            cargo_build_jobs: 2,
            soak_secs: 1,
            restart_threshold: 1,
            failure_cooldown_secs: 60,
            max_attempts_per_rev: 2,
            rollout_timeout_secs: 60,
            targets: MainlineDeployerConfig::default().targets,
        }
    }

    /// fake cargo：build_binary 靠 PATH 查找 "cargo"，假二进制必须叫这个名。
    /// 产出一个假二进制占位（migrations 由真实 git 仓库提供）；接收的
    /// COGNEVA_GIT_REVISION 落盘（构建侧必须显式注入完整 rev）。
    fn fake_cargo(dir: &Path, work: &Path) {
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
echo "$COGNEVA_GIT_REVISION" >> '{envlog}'
mkdir -p '{work}/target/release'
echo 'fake-binary' > '{work}/target/release/cogneva'
chmod +x '{work}/target/release/cogneva'
exit 0
"#,
            log = dir.join("cargo.log").display(),
            envlog = dir.join("cargo-env.log").display(),
            work = work.display()
        );
        write_fake_bin(dir, "cargo", &script);
    }

    fn fake_strip(dir: &Path) {
        write_fake_bin(dir, "strip", "#!/bin/sh\nexit 0\n");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    // ENV_LOCK 是进程级 PATH 串行锁：PATH 是进程全局状态，必须跨 await 持有
    // 直到被测命令跑完，用同步 Mutex 即可（测试内不跨任务死锁）。
    async fn poll_once_builds_pushes_and_dispatches_on_new_main() {
        let _env = ENV_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "reg.local:5000/cogneva:local");
        fake_cargo(&bin_dir, &work);
        fake_strip(&bin_dir);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, &work);

        // cargo/strip 靠 PATH 查找，bin_dir 前置；ENV_LOCK 保证无并行测试串改。
        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));

        deployer.poll_once().await.unwrap();

        std::env::set_var("PATH", old_path);

        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        assert!(
            buildah_calls.contains("from reg.local:5000/cogneva:local"),
            "base should be registry :local on first migration: {buildah_calls}"
        );
        // buildah commit/tag/push 走 Pod 内 push 端点（集群 DNS）。
        let push_tag = main_image("reg.local:5000", &rev_b);
        assert!(
            buildah_calls.contains(&format!("commit ctr-test-123 {push_tag}")),
            "{buildah_calls}"
        );
        assert!(
            buildah_calls.contains("push --tls-verify=false"),
            "{buildah_calls}"
        );
        assert!(buildah_calls.contains(&push_tag), "{buildah_calls}");
        assert!(
            buildah_calls.contains("reg.local:5000/cogneva:local"),
            "floating :local tagged at push endpoint: {buildah_calls}"
        );

        let kubectl_calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            kubectl_calls.contains("apply -f -"),
            "job should be dispatched: {kubectl_calls}"
        );
        // apply 的 stdin manifest 也落了日志，job 名（在 manifest 里）可验。
        assert!(
            kubectl_calls.contains(&job_name(&rev_b)),
            "job manifest missing: {kubectl_calls}"
        );
        // Job manifest 镜像与 --tag 必须是节点 pull 端点（kubelet 不解析集群 DNS），
        // 绝不能把 Pod 内 push 端点写进 image 引用。
        let pull_tag = main_image("localhost:30500", &rev_b);
        assert!(
            kubectl_calls.contains(&pull_tag),
            "job image must use node pull endpoint: {kubectl_calls}"
        );
        assert!(
            !kubectl_calls.contains(&format!("--tag {push_tag}")),
            "rollout --tag must not use push endpoint: {kubectl_calls}"
        );

        // 沙盒构建必须显式注入完整 rev：build.rs 回退只嵌 7 位短 sha，
        // 叠层 --version 的 12 位前缀校验会必败。
        let cargo_env = std::fs::read_to_string(bin_dir.join("cargo-env.log")).unwrap();
        assert!(
            cargo_env.trim().starts_with(&rev_b),
            "cargo must receive full COGNEVA_GIT_REVISION: {cargo_env:?}"
        );

        let state: MainlineState =
            serde_json::from_str(&std::fs::read_to_string(root.join("state/state.json")).unwrap())
                .unwrap();
        assert_eq!(state.in_flight.unwrap().phase, Phase::Dispatched);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    // 同上：ENV_LOCK 串行化进程级 PATH 修改，需跨 await 持有。
    async fn poll_once_noop_when_already_at_main() {
        let _env = ENV_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, "");
        let deployed = main_image("localhost:30500", &rev_b);
        let kubectl = fake_kubectl(&bin_dir, &deployed);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, &work);
        deployer.poll_once().await.unwrap();

        assert!(
            !bin_dir.join("buildah.log").exists(),
            "no buildah calls expected when already at main rev"
        );
        let kubectl_calls =
            std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap_or_default();
        assert!(
            !kubectl_calls.contains("apply"),
            "no job dispatch expected: {kubectl_calls}"
        );
    }

    #[tokio::test]
    async fn rollout_failure_rolls_back_updated_targets() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 快照（.image jsonpath）返回各部署当前镜像；rollout 查询（generation
        // jsonpath）：第一个目标 security-gateway 永远不完成（observedGeneration
        // 落后），其余正常。set image 都成功。
        let log = bin_dir.join("kubectl.log");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "error: unable to match a printer suitable for the output format \"$a\"" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *generation*)
    case "$*" in
      *cogneva-security-gateway*) echo "1 0 1 0 0" ;;
      *) echo "1 1 1 1 1" ;;
    esac ;;
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"get pods"*) echo "0 true " ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        // 轮询间隔 5s，超时 1s：首轮即败、醒后第二轮越过死线，快速失败。
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            1,
        );
        let cfg = MainlineDeployerConfig::default();
        let plan = RolloutPlan::from_config(&cfg, "localhost:30500/cogneva:main-new".into());
        let err = executor.run(&plan).await.unwrap_err();
        assert!(err.to_string().contains("did not complete"), "{err}");

        let calls = std::fs::read_to_string(&log).unwrap();
        // 回滚目标来自 Job 启动时快照（.image 查询），不是参数传入。
        assert!(
            calls.contains("set image deployment/cogneva-security-gateway security-gateway=localhost:30500/cogneva:main-old"),
            "rollback should set failed target back to its snapshotted prev: {calls}"
        );
        // 新镜像引用是节点 pull 端点。
        assert!(
            calls.contains("set image deployment/cogneva-security-gateway security-gateway=localhost:30500/cogneva:main-new"),
            "rollout should set failed target to new pull-endpoint tag: {calls}"
        );
    }

    #[tokio::test]
    async fn pods_healthy_detects_fatal_states() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let pods_file = bin_dir.join("pods.out");
        let kubectl = fake_kubectl_pods_from_file(&bin_dir, &pods_file);
        let executor = RolloutExecutor::new(kubectl, "cogneva", 1, 1, 60);
        let target = RolloutTarget {
            deployment: "cogneva".into(),
            container: "cogneva".into(),
            component: "gateway".into(),
            name: "cogneva".into(),
        };

        // ImagePullBackOff 立即判病（即使宽限期）。
        std::fs::write(&pods_file, "0 false ImagePullBackOff").unwrap();
        assert!(executor.pods_healthy(&target, true).await.is_err());

        // 重启超阈值。
        std::fs::write(&pods_file, "2 true ").unwrap();
        assert!(executor.pods_healthy(&target, false).await.is_err());

        // 空输出（选择器失效）判病。
        std::fs::write(&pods_file, "").unwrap();
        assert!(executor.pods_healthy(&target, false).await.is_err());

        // 健康。
        std::fs::write(&pods_file, "0 true ").unwrap();
        assert!(executor.pods_healthy(&target, false).await.is_ok());
    }
}
