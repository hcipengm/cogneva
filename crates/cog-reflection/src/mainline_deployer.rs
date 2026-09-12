//! 主线跟踪自动部署器：让集群自治跟踪公版 main。
//!
//! 构建侧（进化 Pod 内，[`MainlineDeployer`] + [`run_mainline_loop`]）：
//! 周期检测集群内 bare 仓库（/host-git）的 main 前进 → 沙盒源码树 reset 到
//! 新 rev → cargo build（PVC target 增量缓存）→ buildah 基于"当前在跑的
//! 不可变 tag"打最小 overlay → 推集群内 registry 的 `main-<rev12>` 不可变
//! tag → 派独立 Job 跑滚动 → 滚动收敛后才把 registry 浮动签 `:local` 前移
//! 到本 rev（`:local` 是静态清单/GitOps apply 的回退锚点，构建期就推会让
//! 失败回滚的坏镜像成为浮动签权威）。
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
/// cargo registry 缓存同样落 PVC：镜像里的 /usr/local/cargo 是容器可写层，
/// Pod 一重建（主线滚动最后一个目标就是 evolution 自己）索引与 crate 缓存
/// 全丢，每次构建都要在家庭网络上重拉整个 crates.io 索引。
const CARGO_HOME_PVC: &str = "/opt/cogneva/sandbox/cargo-home";

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

/// `host:port` 端点拆成 (host, port)。buildah 强制 registry 端点带端口，
/// 集群内 registry 因此永远可解析。
fn endpoint_host_port(endpoint: &str) -> Option<(&str, u16)> {
    let (host, port) = endpoint.trim_end_matches('/').rsplit_once(':')?;
    Some((host, port.parse().ok()?))
}

/// 单平台 manifest 的 config blob digest（多平台 index 没有这一层）。
fn config_digest_of(manifest: &serde_json::Value) -> Option<String> {
    manifest
        .get("config")
        .and_then(|c| c.get("digest"))
        .and_then(|d| d.as_str())
        .map(|d| d.to_string())
}

/// 多平台 index 里第一个子 manifest 的 digest（各平台镜像由同一次构建产出，
/// rev 标签一致，取哪个都行）。
fn first_manifest_digest(index: &serde_json::Value) -> Option<String> {
    index
        .get("manifests")
        .and_then(|m| m.as_array())
        .and_then(|a| a.first())
        .and_then(|m| m.get("digest"))
        .and_then(|d| d.as_str())
        .map(|d| d.to_string())
}

/// 从 image config blob 里取构建期写入的 rev 标签。
fn revision_of_config_blob(blob: &serde_json::Value) -> Option<String> {
    blob.get("config")?
        .get("Labels")?
        .get("org.opencontainers.image.revision")?
        .as_str()
        .map(|s| s.to_string())
}

/// 从裸 HTTP 响应里切出状态码与 body（按 `Content-Length` 截断；缺失则取
/// 剩余全部）。registry 的 JSON 响应永远带 Content-Length。
fn parse_http_response(raw: &[u8]) -> Option<(u16, Vec<u8>)> {
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&raw[..split]).ok()?;
    let mut lines = head.lines();
    let status = lines.next()?.split_whitespace().nth(1)?.parse().ok()?;
    let len = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok());
    let body = &raw[split + 4..];
    let body = match len {
        Some(n) if n <= body.len() => &body[..n],
        _ => body,
    };
    Some((status, body.to_vec()))
}

/// 这些 tag 的镜像内容与 rev 一一对应（不可变），其余（浮动签）不能由
/// tag 反推 rev——必须问 registry 当前内容是什么版本。
fn tag_is_immutable_for_rev(tag: &str) -> bool {
    tag.starts_with("main-")
}

/// 浮动签是否就是目标 rev：仅当 registry 上该 tag 的镜像当前确实构建自
/// `bare_rev` 时才成立。只看清单里的 tag 字符串会把"标签还在、内容已被
/// 重新播种成旧二进制"当成已收敛，浮动签随即前移，旧二进制被固化成
/// 静态清单 apply 的回退锚点。
fn floating_pin_is_converged(declared_rev: Option<&str>, bare_rev: &str) -> bool {
    matches!(declared_rev, Some(r) if rev12(r) == rev12(bare_rev))
}

/// 命中即无自救可能的 Pod 等待态：拉不到镜像、镜像引用非法、挂载/配置
/// 错误、容器反复崩溃退出。出现这些状态的新副本永远不会 ready，等再久
/// 也只会烧 rollout 超时，必须立即判败触发回滚。
const FATAL_WAITING_REASONS: &[&str] = &[
    "ImagePullBackOff",
    "ErrImagePull",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CrashLoopBackOff",
];

/// Pod 双标签选择器：主应用/网关/执行器的 name 标签都是 `cogneva`，
/// 单标签会跨部署误判（gitops puller 旧代码只用 name= 的同源缺陷）。
fn pod_selector(name: &str, component: &str) -> String {
    format!(
        "app.kubernetes.io/name={},app.kubernetes.io/component={}",
        name, component
    )
}

/// 四部署当前镜像的归类。
#[derive(Debug, Clone, PartialEq, Eq)]
enum DeployedState {
    /// 四部署统一跑在 `main-<rev>` 上。
    Main(String),
    /// 全都不是主线 tag（迁移前的 localhost/cogneva:local 时代）：允许首轮
    /// 以 registry :local 为基底前进。
    Legacy,
    /// 混合态（部分主线、部分旧 tag，或主线 rev 不一致）。有在飞滚动时表示
    /// 上一轮滚动未收敛，绝不触发新一轮；无在飞滚动时是外部写入造成的非一致，
    /// 由 [`normalize_deployed`] 归一后放行。
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

/// 归一混合态与"在飞滚动"的关系。
///
/// 混合态的原义是"上一轮的滚动还没收敛"——那只在自己真有滚动在飞时成立。
/// 没有在飞滚动却出现混合态，只可能是外部写入造成的（清单被重下发、部分 apply、
/// GitOps 金丝雀、手工 `set image`），此时按 Mixed 一直拒会让部署器**永久静默停摆**
/// （只有一行 INFO，没有任何自愈路径）。这种混合态按 Legacy 放行，本轮滚动把四部署
/// 重新 pin 回同一个 rev 即自愈。
///
/// 有在飞滚动时保持 Mixed 原义：绝不叠加新一轮。
fn normalize_deployed(deployed: &DeployedState, has_in_flight: bool) -> DeployedState {
    match deployed {
        DeployedState::Mixed if !has_in_flight => DeployedState::Legacy,
        other => other.clone(),
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
    /// 部署器独占一棵稳定路径的工作树。与进化任务的工作树互不干涉：这里是
    /// 唯一能自由 `reset --hard` 的检出，任何第三方检出停在哪里都不影响它。
    workspaces: std::sync::Arc<crate::workspace::WorkspaceManager>,
}

impl MainlineDeployer {
    pub fn new(
        cfg: MainlineDeployerConfig,
        workspaces: std::sync::Arc<crate::workspace::WorkspaceManager>,
    ) -> Self {
        Self { cfg, workspaces }
    }

    /// 部署器工作树路径（稳定）。
    pub fn workdir(&self) -> PathBuf {
        self.workspaces.deployer_workspace()
    }

    /// 外部共享的 CARGO_TARGET_DIR：工作树可整棵重建而不丢增量编译缓存。
    pub fn target_dir(&self) -> PathBuf {
        self.workspaces.target_dir().to_path_buf()
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

    /// 打一条 INFO 心跳：bare HEAD + 持久化状态摘要。bare 读取失败只降级
    /// 成占位文本，不影响主循环——心跳本身绝不能成为故障源。
    async fn log_heartbeat(&self) {
        let bare = self
            .bare_main_rev()
            .await
            .unwrap_or_else(|e| format!("unreadable({e})"));
        let summary = heartbeat_message(&self.load_state(), &bare, chrono::Utc::now().timestamp());
        info!(heartbeat = %summary, "mainline deployer heartbeat");
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
        let workdir = self.workdir();
        self.run_cmd("git", args, Some(&workdir), 120).await
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

    /// 集群内 registry 的最小只读客户端：明文 HTTP、同命名空间 DNS、
    /// 无凭证（insecure registry，buildah 走的就是这条通道）。
    async fn registry_get(&self, path: &str, accept: &[&str]) -> SFResult<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let endpoint = self.push_endpoint();
        let (host, port) = endpoint_host_port(&endpoint).ok_or_else(|| {
            SFError::Agent(format!("registry endpoint {endpoint:?} is not host:port"))
        })?;
        let accepted = if accept.is_empty() {
            String::new()
        } else {
            format!("Accept: {}\r\n", accept.join(", "))
        };
        let req =
            format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n{accepted}Connection: close\r\n\r\n");
        let mut stream = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::net::TcpStream::connect((host, port)),
        )
        .await
        .map_err(|_| SFError::IO(format!("registry {endpoint} connect timed out")))?
        .map_err(|e| SFError::IO(format!("registry {endpoint} connect failed: {e}")))?;
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("registry request write failed: {e}")))?;
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(60), stream.read_to_end(&mut raw))
            .await
            .map_err(|_| SFError::IO("registry read timed out".into()))?
            .map_err(|e| SFError::IO(format!("registry read failed: {e}")))?;
        let (status, body) = parse_http_response(&raw)
            .ok_or_else(|| SFError::IO("registry returned a malformed HTTP response".into()))?;
        if status != 200 {
            return Err(SFError::IO(format!("registry GET {path} -> {status}")));
        }
        Ok(body)
    }

    /// registry 上某 tag 当前内容构建自哪个 rev：manifest → config blob →
    /// `org.opencontainers.image.revision` 标签。多平台 index 多一跳，先下
    /// 第一个子 manifest 取它的 config digest（各平台同 rev，标签一致）。
    async fn registry_tag_revision(&self, tag: &str) -> SFResult<Option<String>> {
        const MANIFEST_ACCEPT: &[&str] = &[
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
        ];
        let manifest = self.registry_manifest(tag, MANIFEST_ACCEPT).await?;
        let config_digest = match config_digest_of(&manifest) {
            Some(d) => d,
            None => {
                let Some(child) = first_manifest_digest(&manifest) else {
                    return Ok(None);
                };
                let child_manifest = self.registry_manifest(&child, MANIFEST_ACCEPT).await?;
                let Some(d) = config_digest_of(&child_manifest) else {
                    return Ok(None);
                };
                d
            }
        };
        let blob: serde_json::Value = serde_json::from_slice(
            &self
                .registry_get(&format!("/v2/cogneva/blobs/{config_digest}"), &[])
                .await?,
        )
        .map_err(|e| {
            SFError::IO(format!(
                "registry config blob {config_digest} is not JSON: {e}"
            ))
        })?;
        Ok(revision_of_config_blob(&blob))
    }

    /// 取一个 manifest（tag 或 digest 引用皆可）。
    async fn registry_manifest(
        &self,
        reference: &str,
        accept: &[&str],
    ) -> SFResult<serde_json::Value> {
        serde_json::from_slice(
            &self
                .registry_get(&format!("/v2/cogneva/manifests/{reference}"), accept)
                .await?,
        )
        .map_err(|e| SFError::IO(format!("registry manifest {reference} is not JSON: {e}")))
    }

    /// registry 上某 tag 是否存在（manifest 可取）。用于"不重建也能修"的
    /// 快路：不可变 tag 内容与 rev 一一对应，存在即可直接滚动。
    async fn registry_tag_exists(&self, tag: &str) -> bool {
        self.registry_get(
            &format!("/v2/cogneva/manifests/{tag}"),
            &["application/vnd.docker.distribution.manifest.v2+json"],
        )
        .await
        .is_ok()
    }

    /// 四部署当前声明的镜像对应哪个 rev。不可变 `main-<rev>` 由 tag 直接
    /// 给出（tag 与内容一一对应）；浮动签必须问 registry 当前内容构建自
    /// 哪个 rev——tag 字符串本身不含 rev，而内容随时可能被重新播种。四部署
    /// 不一致或查不到一律 None（未知绝不当作已收敛）。
    async fn declared_image_rev(&self, images: &[String]) -> Option<String> {
        let first = images.first()?;
        if !images.iter().all(|i| i == first) {
            return None;
        }
        let tag = first.rsplit(':').next()?;
        if tag_is_immutable_for_rev(tag) {
            return parse_main_rev(first).map(|s| s.to_string());
        }
        self.registry_tag_revision(tag).await.ok().flatten()
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
                    // 浮动签只在收敛后前移，失败回滚的坏镜像绝不进 :local。
                    self.promote_local_tag(&inflight.rev).await?;
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
                        // Job 成功退出意味着滚动要么收敛、要么已回滚（回滚是非零
                        // 退出，记 Failed）。这里镜像仍不是目标 tag，只可能是
                        // Job 跑完后被外部 apply/GitOps 打回：kubectl apply 同名
                        // Job 是 no-op 不会重跑，必须删掉重新派发，否则永久卡
                        // "等待收敛"。镜像已是目标 tag 则只是收敛尾巴，下轮再判。
                        let target_tag = main_image(&self.pull_endpoint(), &inflight.rev);
                        let all_on_target = self
                            .deployed_images()
                            .await?
                            .iter()
                            .all(|i| i == &target_tag);
                        if all_on_target {
                            info!(rev = %rev12(&inflight.rev), "rollout job complete; awaiting deployment convergence");
                            return Ok(());
                        }
                        warn!(rev = %rev12(&inflight.rev), "rollout job complete but deployments not on target tag (reverted by an apply?); redispatching");
                        self.dispatch_job(&inflight.rev, &target_tag).await?;
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

        // 静态清单/GitOps apply 把四部署 pin 到 registry 浮动签 :local（见
        // chart/k3s 清单）；:local 只在收敛后前移，所以 pin 命中 last_good 且
        // bare 未再前进时就是"以浮动签形态收敛"，不重建重派。
        let local_pin = local_image(&self.pull_endpoint());
        let on_local_pin = !images.is_empty() && images.iter().all(|i| i == &local_pin);
        if state.last_good_rev.as_deref() == Some(bare.as_str()) && on_local_pin {
            // 声明态只是"清单里写的是 :local"。浮动签的内容可以被重新播种
            // （bootstrap/swap-image 从本机镜像重推），此时节点会随清单滚动
            // 落到旧二进制，而 tag 字符串一个字都没变——只看清单就会把"退回
            // 旧版"判成"无事可做"。必须问 registry 当前内容构建自哪个 rev。
            let running_rev = self.declared_image_rev(&images).await;
            if floating_pin_is_converged(running_rev.as_deref(), &bare) {
                info!(rev = %rev12(&bare), "deployments pinned to floating :local carrying the current mainline; nothing to do");
                return Ok(());
            }
            warn!(
                rev = %rev12(&bare),
                running_rev = ?running_rev,
                "floating :local no longer carries the current mainline (re-seeded?); re-pinning"
            );
            // 落到下方构建流程：reset 到 bare、复用已有不可变镜像或重建，
            // 再派 Job 把四部署 pin 回 `main-<rev>`。
        }

        let normalized = normalize_deployed(&deployed, state.in_flight.is_some());
        if normalized != deployed {
            warn!(
                images = ?images,
                "deployments sit on mixed images with no rollout in flight (an external partial apply?); converging them onto one revision"
            );
        }
        let deployed = normalized;

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

        // 不可变 tag 与 rev 一一对应：registry 上已有 `main-<rev>` 就说明
        // 该 rev 早已构建过（清单被重下发打回浮动签后，四部署只是需要重新
        // pin 回去），直接派 Job 滚动即可，4C 机器上省掉一次全程构建。
        // 该 rev 此前失败过就老老实实重建——半推成功留下的坏 tag 不能靠
        // 复用来"修复"，否则会在同一处反复失败。
        if state.failed_rev.as_deref() != Some(bare.as_str())
            && self.registry_tag_exists(&push_tag).await
        {
            info!(rev = %rev12(&bare), tag = %push_tag, "immutable image already in registry; re-pinning without a rebuild");
            state.in_flight = Some(InFlight {
                rev: bare.clone(),
                phase: Phase::Pushed,
            });
            self.save_state(&state)?;
            self.dispatch_job(&bare, &pull_tag).await?;
            state.in_flight = Some(InFlight {
                rev: bare.clone(),
                phase: Phase::Dispatched,
            });
            self.save_state(&state)?;
            return Ok(());
        }

        info!(rev = %rev12(&bare), base = %base_tag, "mainline advance: building");

        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::SourceReady,
        });
        self.save_state(&state)?;

        // 1. 把独占工作树对齐到新 rev。树是部署器自己的，可无条件 reset。
        self.ensure_source_at(&bare).await?;

        // 2. cargo build --release（target/ 在 source PVC 上增量缓存）。
        self.build_binary(&bare).await?;
        state.in_flight = Some(InFlight {
            rev: bare.clone(),
            phase: Phase::Built,
        });
        self.save_state(&state)?;

        // 3. buildah 叠层并推 registry（只推不可变 tag；:local 收敛后前移）。
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

    /// 把部署器独占的工作树对齐到目标 rev。这里没有守卫也没有"忙则跳过"：
    /// 树是自己的，不存在需要保护的在途工作，损坏就重建。
    async fn ensure_source_at(&self, rev: &str) -> SFResult<()> {
        let spec = crate::workspace::WorkspaceSpec::persistent(
            "mainline",
            crate::workspace::WorkspaceKind::Deployer,
            crate::workspace::BaseRef::Commit(rev.to_string()),
        );
        self.workspaces.ensure_persistent(spec).await?;
        self.git_src(&["reset", "--hard", rev]).await?;
        // target 目录在工作树之外，clean 只清源码，不丢增量编译缓存。
        self.git_src(&["clean", "-ffdx"]).await?;
        Ok(())
    }

    async fn build_binary(&self, rev: &str) -> SFResult<()> {
        let jobs = self.cfg.cargo_build_jobs.to_string();
        let cmdline = format!("cargo build --release --bin cogneva (jobs={jobs})");
        // CARGO_HOME 换 PVC 后，镜像 /usr/local/cargo/config.toml 里的 sparse
        // 镜像配置（受限网络构建注入）不会自动继承；缺失会直连 crates.io，
        // 家庭网络上索引拉取极慢。一次性把镜像内配置带到 PVC。
        let pvc_config = std::path::Path::new(CARGO_HOME_PVC).join("config.toml");
        if !tokio::fs::try_exists(&pvc_config).await.unwrap_or(false) {
            let img_config = std::path::Path::new("/usr/local/cargo/config.toml");
            if tokio::fs::try_exists(img_config).await.unwrap_or(false) {
                if let Ok(body) = tokio::fs::read(img_config).await {
                    let _ = tokio::fs::create_dir_all(CARGO_HOME_PVC).await;
                    let _ = tokio::fs::write(&pvc_config, body).await;
                }
            }
        }
        let mut cmd = tokio::process::Command::new("cargo");
        cmd.args(["build", "--release", "--bin", "cogneva"])
            .current_dir(self.workdir())
            .env("CARGO_BUILD_JOBS", &jobs)
            .env("CARGO_HOME", CARGO_HOME_PVC)
            // 工作树只放源码；产物落在共享 target，工作树重建也不用冷编译。
            .env("CARGO_TARGET_DIR", self.target_dir())
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
        let bin = self.target_dir().join("release/cogneva");
        let _ = self
            .run_cmd("strip", &[bin.to_str().unwrap_or("")], None, 60)
            .await;
        Ok(())
    }

    /// buildah 叠层：FROM 当前在跑 tag → 换二进制 + migrations → --version
    /// 校验内嵌 rev → commit 不可变 tag → 只推不可变 tag（:local 收敛后推）。
    async fn build_and_push(&self, rev: &str, base: &str, new_tag: &str) -> SFResult<()> {
        // 集群内 registry 是纯 HTTP，from 拉基镜像默认试 HTTPS 会报
        // "http: server gave HTTP response to HTTPS client"，与 push 一样
        // 必须显式关 TLS 校验。
        let ctr = self
            .buildah(&["from", "--tls-verify=false", base], 1800)
            .await?;
        let ctr = ctr.trim().to_string();
        let result = self.buildah_steps(&ctr, rev, new_tag).await;
        if let Err(e) = self.buildah(&["rm", &ctr], 60).await {
            warn!(error = %e, "buildah rm failed after build");
        }
        result?;
        Ok(())
    }

    async fn buildah_steps(&self, ctr: &str, rev: &str, new_tag: &str) -> SFResult<()> {
        let bin = self.target_dir().join("release/cogneva");
        let migrations = self.workdir().join("crates/cog-storage/migrations");
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

        // 只推不可变 tag；浮动签 :local 在滚动收敛后由 promote_local_tag 前移，
        // 防止构建失败/回滚的坏镜像成为静态清单 apply 的回退锚点。
        self.buildah(&["push", "--tls-verify=false", new_tag], 900)
            .await?;
        info!(image = %new_tag, "mainline overlay image pushed to registry");
        Ok(())
    }

    /// 滚动收敛后把 registry 浮动签 `:local` 前移到指定 rev。buildah 镜像库
    /// 在 sandbox PVC 上，正常情况刚构建的不可变 tag 还在本地；Pod 重建后
    /// 本地丢失则先从 registry 拉回（同 registry 秒回）再打签推送。
    async fn promote_local_tag(&self, rev: &str) -> SFResult<()> {
        let immutable = main_image(&self.push_endpoint(), rev);
        let local = local_image(&self.push_endpoint());
        let present = self
            .buildah(&["images", "-q", &immutable], 30)
            .await?
            .trim()
            .to_string();
        if present.is_empty() {
            self.buildah(&["pull", "--tls-verify=false", &immutable], 900)
                .await?;
        }
        self.buildah(&["tag", &immutable, &local], 60).await?;
        self.buildah(&["push", "--tls-verify=false", &local], 900)
            .await?;
        info!(rev = %rev12(rev), tag = %local, "floating :local advanced to converged revision");
        Ok(())
    }

    /// 派滚动 Job。同名 Job 已在跑时视为已派发（幂等）；同名 Job 已结束
    /// （成功后镜像被 apply 打回、或失败冷却后重试）必须先删除再 apply——
    /// `kubectl apply` 是 upsert，不会重新执行已完成的 Job。new_tag 必须
    /// 是节点 pull 端点引用（kubelet 经 NodePort 拉取）。
    async fn dispatch_job(&self, rev: &str, new_tag: &str) -> SFResult<()> {
        match self.job_status(&job_name(rev)).await? {
            JobStatus::Running | JobStatus::NotFound => {}
            JobStatus::Complete | JobStatus::Failed => {
                info!(rev = %rev12(rev), "replacing finished rollout job before dispatch");
                self.kubectl(&["delete", "job", &job_name(rev), "--ignore-not-found"], 60)
                    .await?;
            }
        }
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
                    // 分隔符必须显式占位：空格 + split_whitespace 会吞掉缺失
                    // 字段，失败任务 succeeded 缺省时 failed 值顶到第一位，
                    // 会被误判成 Complete。
                    "jsonpath={.status.succeeded}|{.status.failed}|{.status.active}",
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
        let parts: Vec<&str> = out.split('|').collect();
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

/// 心跳摘要（纯函数便于测试）：一行覆盖空闲态全部关键状态。SameRev 收敛
/// 路径静默返回，没有这条摘要时部署器存活无法从日志证明。
fn heartbeat_message(state: &MainlineState, bare_rev: &str, now_unix: i64) -> String {
    let in_flight = state
        .in_flight
        .as_ref()
        .map(|f| format!("{}@{:?}", rev12(&f.rev), f.phase))
        .unwrap_or_else(|| "none".into());
    format!(
        "bare={} last_good={} in_flight={} failed_rev={} failed_attempts={} cooldown_remaining_secs={}",
        rev12(bare_rev),
        state.last_good_rev.as_deref().map(rev12).unwrap_or("none"),
        in_flight,
        state.failed_rev.as_deref().map(rev12).unwrap_or("none"),
        state.failed_attempts,
        (state.failed_cooldown_until - now_unix).max(0),
    )
}

/// 构建侧后台循环入口（插件 spawn）。
pub async fn run_mainline_loop(
    deployer: std::sync::Arc<MainlineDeployer>,
    shutdown: ShutdownSignal,
) {
    // 宿主 bare 仓库（/host-git）与工作树属主/挂载场景会撞 git
    // dubious-ownership；safe.directory 只有 global 配置被采信（与
    // gitops puller 同源处理），启动时幂等写入。临时工作树路径启动时还
    // 不存在，由分配器在创建时按需注册。
    let workdir = deployer.workdir();
    for dir in [
        deployer.cfg.bare_repo.as_str(),
        deployer.workspaces.root().to_str().unwrap_or(""),
        workdir.to_str().unwrap_or(""),
    ] {
        let _ = tokio::process::Command::new("git")
            .args(["config", "--global", "--add", "safe.directory", dir])
            .output()
            .await;
    }
    // 崩溃残留的临时工作树与裸仓库里的孤儿登记在循环启动时清一次。
    let _ = deployer.workspaces.prune().await;
    match deployer.workspaces.gc_stale().await {
        Ok(reclaimed) if !reclaimed.is_empty() => {
            info!(
                count = reclaimed.len(),
                "reclaimed leaked mainline workspaces"
            )
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "workspace gc failed"),
    }
    let interval = Duration::from_secs(deployer.cfg.poll_interval_secs.max(30));
    info!(
        interval_secs = interval.as_secs(),
        push_endpoint = %deployer.push_endpoint(),
        pull_endpoint = %deployer.pull_endpoint(),
        "Mainline deployer loop started"
    );
    let mut ticker = tokio::time::interval(interval);
    // 空闲心跳：SameRev 路径静默返回，靠周期性 INFO 摘要证明部署器存活。
    let heartbeat_every = Duration::from_secs(deployer.cfg.heartbeat_log_secs);
    let mut last_heartbeat: Option<tokio::time::Instant> = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                if let Err(e) = deployer.poll_once().await {
                    warn!(error = %e, "mainline deployer poll failed");
                }
                let due = last_heartbeat
                    .map(|t| t.elapsed() >= heartbeat_every)
                    .unwrap_or(true);
                if due {
                    deployer.log_heartbeat().await;
                    last_heartbeat = Some(tokio::time::Instant::now());
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
    /// 轮询同时查 Pod 致命等待态：崩溃镜像永远不会 ready，干等 rollout 超时
    /// （默认 300s）既拖慢回滚又让故障窗口白白拉长，命中即早退触发回滚。
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
                        // 竖线显式占位：缺失字段（omitempty 的 updatedReplicas
                        // 等）不能顶掉后续字段的位置。
                        "jsonpath={.metadata.generation}|{.status.observedGeneration}|{.spec.replicas}|{.status.updatedReplicas}|{.status.readyReplicas}|{.status.unavailableReplicas}",
                    ],
                    30,
                )
                .await?;
            let parts: Vec<&str> = out.split('|').collect();
            if parts.len() >= 5 {
                let gen: u64 = parts[0].parse().unwrap_or(0);
                let obs: u64 = parts[1].parse().unwrap_or(0);
                let spec: u32 = parts[2].parse().unwrap_or(0);
                let updated: u32 = parts[3].parse().unwrap_or(0);
                let ready: u32 = parts[4].parse().unwrap_or(0);
                // unavailable 必须为 0：RollingUpdate 新旧副本并存时，旧副本
                // 仍 ready 会让 ready==spec 提前成立，但新崩溃副本计入
                // unavailable，不能判完成（等致命态/超时兜底）。
                let unavailable: u32 = parts.get(5).and_then(|v| v.parse().ok()).unwrap_or(0);
                if obs >= gen
                    && gen > 0
                    && updated == spec
                    && ready == spec
                    && unavailable == 0
                    && spec > 0
                {
                    return Ok(());
                }
            }
            // 滚动中新旧 Pod 交替、containerStatuses 可能暂时缺失，空输出/查询
            // 失败在这里不当致命（与 pods_healthy 不同），只认明确的致命等待态。
            self.fatal_pod_state(t).await?;
            if std::time::Instant::now() >= deadline {
                return Err(SFError::Agent(format!(
                    "rollout of deployment/{} did not complete within {}s (last: {out})",
                    t.deployment, self.rollout_timeout_secs
                )));
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// 只查致命等待态（拉不到镜像、配置错误、CrashLoop）。滚动交替期
    /// containerStatuses 缺失或查询临时失败均返回 Ok——由调用方的超时与
    /// 后续 pods_healthy 兜底，这里只负责让"必死"的滚动快速失败。
    async fn fatal_pod_state(&self, t: &RolloutTarget) -> SFResult<()> {
        let selector = pod_selector(&t.name, &t.component);
        let out = match self
            .run_kubectl(
                &[
                    "get",
                    "pods",
                    "-l",
                    &selector,
                    "-o",
                    "jsonpath={range .items[*]}{.status.containerStatuses[0].state.waiting.reason}{\"\\n\"}{end}",
                ],
                30,
            )
            .await
        {
            Ok(out) => out,
            Err(_) => return Ok(()),
        };
        for line in out.lines() {
            let reason = line.trim();
            if FATAL_WAITING_REASONS.contains(&reason) {
                return Err(SFError::Agent(format!(
                    "pod of deployment/{} in fatal waiting state {reason}",
                    t.deployment
                )));
            }
        }
        Ok(())
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
            if FATAL_WAITING_REASONS.contains(&waiting_reason) {
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
    // Job Pod 直接调这个子命令，不经 run_app；不初始化订阅者的话滚动/回滚
    // 日志全部不落，Job 失败时 kubectl logs 是空的，无法诊断。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
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
    fn endpoint_host_port_splits_registry_endpoint() {
        assert_eq!(
            endpoint_host_port("cogneva-registry.cogneva.svc.cluster.local:5000"),
            Some(("cogneva-registry.cogneva.svc.cluster.local", 5000))
        );
        assert_eq!(
            endpoint_host_port("reg.local:5000/"),
            Some(("reg.local", 5000))
        );
        assert_eq!(endpoint_host_port("no-port"), None);
        assert_eq!(endpoint_host_port("host:notaport"), None);
    }

    #[test]
    fn manifest_helpers_split_single_manifest_from_index() {
        let single: serde_json::Value = serde_json::json!({"config": {"digest": "sha256:cfg1"}});
        assert_eq!(config_digest_of(&single).as_deref(), Some("sha256:cfg1"));
        assert_eq!(first_manifest_digest(&single), None);

        let index: serde_json::Value = serde_json::json!({
            "manifests": [{"digest": "sha256:plat"}, {"digest": "sha256:plat2"}]
        });
        assert_eq!(config_digest_of(&index), None);
        assert_eq!(
            first_manifest_digest(&index).as_deref(),
            Some("sha256:plat")
        );

        assert_eq!(config_digest_of(&serde_json::json!({"layers": []})), None);
    }

    #[test]
    fn revision_label_read_from_config_blob() {
        let blob = serde_json::json!({
            "config": {"Labels": {"org.opencontainers.image.revision": "deadbeefcafe"}}
        });
        assert_eq!(
            revision_of_config_blob(&blob).as_deref(),
            Some("deadbeefcafe")
        );
        assert_eq!(
            revision_of_config_blob(&serde_json::json!({"config": {"Labels": {}}})),
            None
        );
        assert_eq!(revision_of_config_blob(&serde_json::json!({})), None);
    }

    #[test]
    fn http_response_body_cut_at_content_length() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\n{\"a\":1}trailing";
        let (status, body) = parse_http_response(raw).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"{\"a\":1}");

        // 无 Content-Length（或短于实际）时取剩余全部——registry 的响应
        // 一律带长度，这条只是不让解析在异常响应上 panic。
        let raw = b"HTTP/1.1 404 Not Found\r\n\r\nnope";
        let (status, body) = parse_http_response(raw).unwrap();
        assert_eq!(status, 404);
        assert_eq!(body, b"nope");
    }

    #[test]
    fn floating_pin_convergence_compares_content_not_the_tag_string() {
        let bare = "4dfd51ff1209abcdef";
        assert!(floating_pin_is_converged(Some("4dfd51ff1209abcdef"), bare));
        // registry 上的内容标签是短 id 也能对上。
        assert!(floating_pin_is_converged(Some("4dfd51ff1209"), bare));
        // 浮动签被重新播种成别的 rev：清单里 tag 一个字没变，但没收敛。
        assert!(!floating_pin_is_converged(Some("000000000000abcd"), bare));
        // 读不到内容版本一律不当作已收敛。
        assert!(!floating_pin_is_converged(None, bare));
    }

    #[test]
    fn only_main_prefixed_tags_imply_a_rev() {
        assert!(tag_is_immutable_for_rev("main-4dfd51ff1209"));
        assert!(!tag_is_immutable_for_rev("local"));
        assert!(!tag_is_immutable_for_rev("promote-p-1"));
    }

    #[test]
    fn heartbeat_message_summarizes_idle_state() {
        let state = MainlineState {
            last_good_tag: Some("cogneva:main-4dfd51ff1209".into()),
            last_good_rev: Some("4dfd51ff1209abcdef".into()),
            in_flight: None,
            failed_rev: None,
            failed_cooldown_until: 0,
            failed_attempts: 0,
        };
        let msg = heartbeat_message(&state, "4dfd51ff1209abcdef", 100);
        assert!(msg.contains("bare=4dfd51ff1209"), "{msg}");
        assert!(msg.contains("last_good=4dfd51ff1209"), "{msg}");
        assert!(msg.contains("in_flight=none"), "{msg}");
        assert!(msg.contains("failed_rev=none"), "{msg}");
        assert!(msg.contains("failed_attempts=0"), "{msg}");
        assert!(msg.contains("cooldown_remaining_secs=0"), "{msg}");
    }

    #[test]
    fn heartbeat_message_shows_inflight_and_cooldown() {
        let state = MainlineState {
            last_good_tag: None,
            last_good_rev: None,
            in_flight: Some(InFlight {
                rev: "aabbccddeeff0011".into(),
                phase: Phase::Pushed,
            }),
            failed_rev: Some("112233445566aabb".into()),
            failed_cooldown_until: 1500,
            failed_attempts: 2,
        };
        let msg = heartbeat_message(&state, "aabbccddeeff0011", 1000);
        assert!(msg.contains("in_flight=aabbccddeeff@Pushed"), "{msg}");
        assert!(msg.contains("failed_rev=112233445566"), "{msg}");
        assert!(msg.contains("failed_attempts=2"), "{msg}");
        assert!(msg.contains("cooldown_remaining_secs=500"), "{msg}");
    }

    #[test]
    fn heartbeat_message_survives_unreadable_bare_rev() {
        // 心跳本身绝不能成为故障源：bare 读取失败时降级为占位文本。
        let msg = heartbeat_message(&MainlineState::default(), "unreadable(git failed)", 0);
        assert!(msg.contains("bare=unreadable("), "{msg}");
        assert!(msg.contains("last_good=none"), "{msg}");
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

    /// 混合态只在真有滚动在飞时才算"上一轮未收敛"；无在飞滚动时是外部写入造成的
    /// 非一致，必须归一放行，否则部署器永久停摆。
    #[test]
    fn mixed_state_only_blocks_while_a_rollout_is_in_flight() {
        let mixed = DeployedState::Mixed;
        assert_eq!(
            normalize_deployed(&mixed, true),
            DeployedState::Mixed,
            "有在飞滚动：保持原义，绝不叠加新一轮"
        );
        assert_eq!(
            normalize_deployed(&mixed, false),
            DeployedState::Legacy,
            "无在飞滚动：外部写入的非一致，放行让本轮把它收敛回单一 rev"
        );
        // 归一不动其它态：正常主线不会被降级成 Legacy。
        assert_eq!(
            normalize_deployed(&DeployedState::Main("aa1111111111".into()), false),
            DeployedState::Main("aa1111111111".into())
        );
        assert_eq!(
            normalize_deployed(&DeployedState::Legacy, true),
            DeployedState::Legacy
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
    /// 避免并行测试互相串改环境。用异步锁是因为持有期必须覆盖被测命令的 await，
    /// 同步锁守卫跨 await 持锁会触发 clippy::await_holding_lock。
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

    /// fake kubectl：按 deployment 名字分别返回镜像，用来构造"四部署镜像不一致"
    /// 的现场（清单被部分重下发 / 手工 set image）。名字后的空格是必要边界：
    /// `cogneva ` 不会匹配上 `cogneva-evolution `。
    fn fake_kubectl_per_deployment(dir: &Path, first: &str, second: &str) -> String {
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
  *"get deployment cogneva-security-gateway "*) echo '{first}' ;;
  *"get deployment cogneva "*) echo '{first}' ;;
  *"get deployment "*" jsonpath="*) echo '{second}' ;;
  *"get job"*) echo "Error: jobs.batch \"x\" not found" >&2; exit 1 ;;
  *"get pods"*) echo "0 true " ;;
  *"apply"*) cat >> '{log}'; echo "job.batch/x created" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display(),
            first = first,
            second = second
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
            heartbeat_log_secs: 3600,
            targets: MainlineDeployerConfig::default().targets,
        }
    }

    /// 部署器工作树的分配器；工作树与 target 都落在测试临时目录内。
    fn test_workspaces(
        root: &Path,
        bare: &Path,
    ) -> std::sync::Arc<crate::workspace::WorkspaceManager> {
        std::sync::Arc::new(crate::workspace::WorkspaceManager::new(
            bare,
            root.join("workspaces"),
            root.join("target"),
        ))
    }

    /// fake cargo：build_binary 靠 PATH 查找 "cargo"，假二进制必须叫这个名。
    /// 产物写进外置的共享 target 目录（工作树里不再有 target/）；接收的
    /// COGNEVA_GIT_REVISION 落盘（构建侧必须显式注入完整 rev）。
    fn fake_cargo(dir: &Path, target_dir: &Path) {
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
echo "$COGNEVA_GIT_REVISION" >> '{envlog}'
mkdir -p '{target}/release'
echo 'fake-binary' > '{target}/release/cogneva'
chmod +x '{target}/release/cogneva'
exit 0
"#,
            log = dir.join("cargo.log").display(),
            envlog = dir.join("cargo-env.log").display(),
            target = target_dir.display()
        );
        write_fake_bin(dir, "cargo", &script);
    }

    fn fake_strip(dir: &Path) {
        write_fake_bin(dir, "strip", "#!/bin/sh\nexit 0\n");
    }

    #[tokio::test]
    // ENV_LOCK 是进程级 PATH 串行锁：PATH 是进程全局状态，必须跨 await 持有
    // 直到被测命令跑完。
    async fn poll_once_builds_pushes_and_dispatches_on_new_main() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "reg.local:5000/cogneva:local");
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, ws);

        // cargo/strip 靠 PATH 查找，bin_dir 前置；ENV_LOCK 保证无并行测试串改。
        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));

        deployer.poll_once().await.unwrap();

        std::env::set_var("PATH", old_path);

        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        assert!(
            buildah_calls.contains("from --tls-verify=false reg.local:5000/cogneva:local"),
            "base pull from in-cluster http registry must skip TLS verify: {buildah_calls}"
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

        // 浮动签 :local 只在滚动收敛后前移：构建阶段允许 FROM registry :local
        // （Legacy 基底），但绝不允许 tag/push 它，否则失败回滚的坏镜像会成为
        // 静态清单 apply 的回退锚点。
        let local_tag = local_image("reg.local:5000");
        let moves_local = buildah_calls.lines().any(|l| {
            (l.contains(" tag ") || l.contains(" push ")) && l.trim_end().ends_with(&local_tag)
        });
        assert!(
            !moves_local,
            "floating :local must not be tagged/pushed before rollout converges: {buildah_calls}"
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

    /// 本次死结的回归：第三方工作树停在无关提交（一条与 main 无共同祖先的
    /// 独立历史）时，部署器照常推进。旧设计里被占用的那棵树就是部署器唯一的
    /// 共享工作树，祖先守卫会把部署永久卡在"每轮静默跳过"。
    #[tokio::test]
    async fn deployer_not_wedged_by_foreign_worktree() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, work, _rev_a, rev_b) = setup_repos(root).await;
        let ws = test_workspaces(root, &bare);

        // 与 main 无共同祖先的旁支历史，推给裸仓库。
        real_git(&work, &["checkout", "--orphan", "side"]).await;
        std::fs::write(work.join("side.txt"), "side\n").unwrap();
        real_git(&work, &["add", "."]).await;
        real_git(&work, &["commit", "-m", "side"]).await;
        real_git(&work, &["push", "origin", "side"]).await;

        // 模拟智能体占树：另一棵工作树停在无关分支上。
        let foreign = ws
            .acquire_ephemeral(
                "agent-task",
                crate::workspace::BaseRef::Branch("side".into()),
            )
            .await
            .unwrap();
        assert!(foreign.path.exists());
        // 前提校验：这棵树确实停在 main 的非祖先上——正是旧守卫拒绝搬动的条件。
        let is_ancestor = tokio::process::Command::new("git")
            .args([
                "--git-dir",
                bare.to_str().unwrap(),
                "merge-base",
                "--is-ancestor",
                "side",
                "main",
            ])
            .output()
            .await
            .unwrap();
        assert!(
            !is_ancestor.status.success(),
            "setup must reproduce the wedge condition: side is not an ancestor of main"
        );

        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "reg.local:5000/cogneva:local");
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, ws.clone());

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        assert!(
            buildah_calls.contains(&main_image("reg.local:5000", &rev_b)),
            "deployer must keep advancing despite a foreign worktree: {buildah_calls}"
        );
        // 第三方工作树原样保留，部署器不碰它。
        assert!(foreign.path.exists(), "foreign worktree must be left alone");
        assert_eq!(
            real_git_stdout(&foreign.path, &["rev-parse", "HEAD"]).await,
            real_git_stdout(
                &bare,
                &["--git-dir", bare.to_str().unwrap(), "rev-parse", "side"]
            )
            .await
        );
    }

    /// 回归：四部署镜像被外部写入弄成不一致（清单部分重下发、手工 set image），
    /// 且本地没有在飞滚动。旧逻辑按"上一轮未收敛"永久跳过——只有一行 INFO，
    /// 没有任何自愈路径，部署器就此静默停摆。修正后应照常构建并派发滚动 Job，
    /// 由这一轮把四部署重新 pin 回同一个 rev。
    #[tokio::test]
    // 同上：ENV_LOCK 串行化进程级 PATH 修改，需跨 await 持有。
    async fn poll_once_converges_mixed_deployments_with_no_rollout_in_flight() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        // 网关与主应用停在浮动签，执行器与进化停在旧主线 tag。
        let kubectl = fake_kubectl_per_deployment(
            &bin_dir,
            "localhost:30500/cogneva:local",
            "localhost:30500/cogneva:main-000000000000",
        );
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, ws);

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        let kubectl_calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(
            kubectl_calls.contains("apply -f -"),
            "mixed deployments must not wedge the deployer; a rollout job should be dispatched: {kubectl_calls}"
        );
        assert!(
            kubectl_calls.contains(&job_name(&rev_b)),
            "rollout job must target the bare main rev: {kubectl_calls}"
        );
        // 基底退回浮动签：混合态里没有唯一可信的主线 rev 可作 from。
        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        assert!(
            buildah_calls.contains("from --tls-verify=false reg.local:5000/cogneva:local"),
            "{buildah_calls}"
        );
    }

    #[tokio::test]
    // 同上：ENV_LOCK 串行化进程级 PATH 修改，需跨 await 持有。
    async fn poll_once_noop_when_already_at_main() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, "");
        let deployed = main_image("localhost:30500", &rev_b);
        let kubectl = fake_kubectl(&bin_dir, &deployed);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
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

    /// 极简假 registry：按序应答预置响应，每连接一次。返回 (endpoint, 收到的请求)。
    async fn fake_registry(
        responses: Vec<String>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let mut reqs = Vec::new();
            for resp in responses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                reqs.push(String::from_utf8_lossy(&buf[..n]).to_string());
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
            reqs
        });
        (format!("127.0.0.1:{port}"), handle)
    }

    fn http_200(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    const HTTP_404: &str = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";

    #[tokio::test]
    async fn registry_tag_revision_reads_label_from_manifest_and_blob() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"config":{"digest":"sha256:cfg1"}}"#),
            http_200(
                r#"{"config":{"Labels":{"org.opencontainers.image.revision":"deadbeefcafe"}}}"#,
            ),
        ])
        .await;
        let mut cfg = test_config(root, Path::new("/nonexistent"), "noop", "noop");
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));

        let rev = deployer.registry_tag_revision("local").await.unwrap();
        assert_eq!(rev.as_deref(), Some("deadbeefcafe"));

        let reqs = handle.await.unwrap();
        assert!(
            reqs[0].contains("/v2/cogneva/manifests/local"),
            "{:?}",
            reqs[0]
        );
        assert!(
            reqs[1].contains("/v2/cogneva/blobs/sha256:cfg1"),
            "{:?}",
            reqs[1]
        );
        // 明文 HTTP：Pod 与集群内 registry 之间不做 TLS。
        assert!(reqs[0].starts_with("GET /v2/"), "{:?}", reqs[0]);
    }

    #[tokio::test]
    async fn registry_tag_revision_follows_a_multi_platform_index() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"manifests":[{"digest":"sha256:plat"}]}"#),
            http_200(r#"{"config":{"digest":"sha256:cfg2"}}"#),
            http_200(
                r#"{"config":{"Labels":{"org.opencontainers.image.revision":"cafebabe0011"}}}"#,
            ),
        ])
        .await;
        let mut cfg = test_config(root, Path::new("/nonexistent"), "noop", "noop");
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));

        let rev = deployer.registry_tag_revision("local").await.unwrap();
        assert_eq!(rev.as_deref(), Some("cafebabe0011"));
        let reqs = handle.await.unwrap();
        assert!(
            reqs[1].contains("/v2/cogneva/manifests/sha256:plat"),
            "{:?}",
            reqs[1]
        );
        assert!(
            reqs[2].contains("/v2/cogneva/blobs/sha256:cfg2"),
            "{:?}",
            reqs[2]
        );
    }

    #[tokio::test]
    async fn declared_image_rev_derives_immutable_tags_without_the_registry() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let mut cfg = test_config(root, Path::new("/nonexistent"), "noop", "noop");
        // 指向不可达端口：不可变 tag 必须不依赖 registry 就能读出 rev。
        cfg.registry = "127.0.0.1:1".into();
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, Path::new("/nonexistent")));

        let imgs = vec![main_image("localhost:30500", "abcdef0123456789"); 4];
        assert_eq!(
            deployer.declared_image_rev(&imgs).await.as_deref(),
            Some("abcdef012345")
        );
        // 四部署不一致：未知，绝不当成已收敛。
        let mixed = vec![
            main_image("localhost:30500", "abcdef0123456789"),
            local_image("localhost:30500"),
        ];
        assert_eq!(deployer.declared_image_rev(&mixed).await, None);
    }

    /// 浮动签被重新播种成别的 rev：清单里的 tag 字符串一个字没变，但节点
    /// 已经随清单滚到旧二进制。必须识别出没收敛并重新 pin 回 `main-<rev>`。
    #[tokio::test]
    async fn floating_pin_drift_is_detected_and_repaired() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "localhost:30500/cogneva:local");
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        // 三次 registry 命中：读 :local 的 manifest、读其 config blob（rev 是
        // 别的值）、查 main-<rev> 是否存在（不存在 → 必须真重建）。
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"config":{"digest":"sha256:cfg2"}}"#),
            http_200(
                r#"{"config":{"Labels":{"org.opencontainers.image.revision":"000000000000abcd"}}}"#,
            ),
            HTTP_404.to_string(),
        ])
        .await;
        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, ws);

        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            last_good_rev: Some(rev_b.clone()),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        handle.await.unwrap();
        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log"))
            .expect("drift must not take the no-op shortcut");
        assert!(
            buildah_calls.contains("push --tls-verify=false"),
            "{buildah_calls}"
        );
        let kubectl_calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(kubectl_calls.contains("apply -f -"), "{kubectl_calls}");
    }

    /// 不可变 tag 已在 registry 里：清单重下发把四部署打回浮动签后，只需
    /// 重新 pin 回去，4C 机器上不必再跑一次全程构建。
    #[tokio::test]
    async fn existing_immutable_image_is_repinned_without_a_rebuild() {
        let _env = ENV_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, rev12(&rev_b));
        let kubectl = fake_kubectl(&bin_dir, "localhost:30500/cogneva:local");
        let ws = test_workspaces(root, &bare);
        fake_cargo(&bin_dir, ws.target_dir());
        fake_strip(&bin_dir);

        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"config":{"digest":"sha256:cfg3"}}"#),
            http_200(
                r#"{"config":{"Labels":{"org.opencontainers.image.revision":"000000000000abcd"}}}"#,
            ),
            // main-<rev> 已存在：直接复用。
            http_200("{}"),
        ])
        .await;
        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, ws);

        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            last_good_rev: Some(rev_b.clone()),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_dir.display(), old_path));
        deployer.poll_once().await.unwrap();
        std::env::set_var("PATH", old_path);

        handle.await.unwrap();
        assert!(
            !bin_dir.join("buildah.log").exists(),
            "an image already in the registry must be reused, not rebuilt"
        );
        let kubectl_calls = std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap();
        assert!(kubectl_calls.contains("apply -f -"), "{kubectl_calls}");
        assert!(
            kubectl_calls.contains(&main_image("localhost:30500", &rev_b)),
            "job must pin the deployments back to the immutable tag: {kubectl_calls}"
        );
    }

    #[tokio::test]
    async fn convergence_promotes_floating_local_tag() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, "");
        // 四部署已经在目标 main tag 上：收敛分支优先于 Job 状态判定。
        let deployed = main_image("localhost:30500", &rev_b);
        let kubectl = fake_kubectl(&bin_dir, &deployed);

        let cfg = test_config(root, &bare, &buildah, &kubectl);
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            in_flight: Some(InFlight {
                rev: rev_b.clone(),
                phase: Phase::Dispatched,
            }),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        deployer.poll_once().await.unwrap();

        let buildah_calls = std::fs::read_to_string(bin_dir.join("buildah.log")).unwrap();
        let push_main = main_image("reg.local:5000", &rev_b);
        let push_local = local_image("reg.local:5000");
        assert!(
            buildah_calls.contains(&format!("tag {push_main} {push_local}")),
            "convergence must retag immutable tag to floating :local: {buildah_calls}"
        );
        assert!(
            buildah_calls.contains(&format!("push --tls-verify=false {push_local}")),
            "floating :local must be pushed on convergence: {buildah_calls}"
        );

        let state: MainlineState =
            serde_json::from_str(&std::fs::read_to_string(state_dir.join("state.json")).unwrap())
                .unwrap();
        assert!(state.in_flight.is_none());
        assert_eq!(state.last_good_rev.as_deref(), Some(rev_b.as_str()));
        assert_eq!(
            state.last_good_tag.as_deref(),
            Some(main_image("localhost:30500", &rev_b).as_str())
        );
    }

    #[tokio::test]
    async fn local_pin_at_last_good_is_noop_after_apply() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let buildah = fake_buildah(&bin_dir, "");
        // 外部 apply 把四部署打回静态清单 pin：registry 浮动签 :local。
        let kubectl = fake_kubectl(&bin_dir, "localhost:30500/cogneva:local");
        // registry 上 :local 的内容确实构建自当前 mainline rev：这才叫收敛。
        let (endpoint, handle) = fake_registry(vec![
            http_200(r#"{"schemaVersion":2,"config":{"digest":"sha256:cfg1"}}"#),
            http_200(&format!(
                r#"{{"config":{{"Labels":{{"org.opencontainers.image.revision":"{rev_b}"}}}}}}"#
            )),
        ])
        .await;

        let mut cfg = test_config(root, &bare, &buildah, &kubectl);
        cfg.registry = endpoint;
        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            last_good_rev: Some(rev_b.clone()),
            last_good_tag: Some(main_image("localhost:30500", &rev_b)),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        deployer.poll_once().await.unwrap();
        handle.await.unwrap();

        assert!(
            !bin_dir.join("buildah.log").exists(),
            "apply pin to current :local must not trigger rebuild"
        );
        let kubectl_calls =
            std::fs::read_to_string(bin_dir.join("kubectl.log")).unwrap_or_default();
        assert!(
            !kubectl_calls.contains("apply"),
            "apply pin to current :local must not redispatch: {kubectl_calls}"
        );
    }

    #[tokio::test]
    async fn completed_job_with_reverted_images_is_redispatched() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (bare, _work, _rev_a, rev_b) = setup_repos(root).await;
        let bin_dir = root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let log = bin_dir.join("kubectl.log");
        // 四部署已被外部 apply 打回 :local（Legacy），而同 rev 的 Job 已完成。
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "bad -o arg" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *"get deployment"*) echo "localhost:30500/cogneva:local" ;;
  *"get job"*) echo "1||" ;;
  *"apply"*) cat >> '{log}'; echo "job.batch/x created" ;;
  *"delete"*) echo "job.batch \"x\" deleted" ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        let kubectl = bin_dir.join("fake-kubectl");
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        let buildah = fake_buildah(&bin_dir, "");

        let cfg = test_config(root, &bare, &buildah, &kubectl.to_string_lossy());
        // 预置在飞状态：Job 已派发（Dispatched），避免触发构建流程。
        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state = MainlineState {
            in_flight: Some(InFlight {
                rev: rev_b.clone(),
                phase: Phase::Dispatched,
            }),
            ..Default::default()
        };
        std::fs::write(
            state_dir.join("state.json"),
            serde_json::to_string(&state).unwrap(),
        )
        .unwrap();

        let deployer = MainlineDeployer::new(cfg, test_workspaces(root, &bare));
        deployer.poll_once().await.unwrap();

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            calls.contains(&format!("delete job {}", job_name(&rev_b))),
            "completed job must be deleted before re-dispatch: {calls}"
        );
        assert!(
            calls.matches("apply -f -").count() >= 1,
            "rollout job must be re-dispatched: {calls}"
        );
        let state: MainlineState =
            serde_json::from_str(&std::fs::read_to_string(state_dir.join("state.json")).unwrap())
                .unwrap();
        assert_eq!(state.in_flight.unwrap().phase, Phase::Dispatched);
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
      *cogneva-security-gateway*) echo "1|0|1|0|0|" ;;
      *) echo "1|1|1|1|1|" ;;
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
    async fn crashloop_pod_fails_fast_without_waiting_for_rollout_timeout() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        let log = bin_dir.join("kubectl.log");
        // security-gateway rollout 永不完成且新 Pod CrashLoopBackOff：
        // wait_rollout_complete 必须在首轮轮询即致命态早退，而不是等满超时。
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in
      jsonpath=*) ;;
      *) echo "bad -o arg" >&2; exit 2 ;;
    esac
  fi
  prev="$a"
done
case "$*" in
  *generation*)
    case "$*" in
      *cogneva-security-gateway*) echo "1|0|1|0|0|" ;;
      *) echo "1|1|1|1|1|" ;;
    esac ;;
  *".image"*) echo "localhost:30500/cogneva:main-old" ;;
  *"get pods"*)
    case "$*" in
      *component=security-gateway*) echo "CrashLoopBackOff" ;;
      *) echo "0 true " ;;
    esac ;;
  *) echo ok ;;
esac
exit 0
"#,
            log = log.display()
        );
        write_fake_bin(&bin_dir, "fake-kubectl", &script);
        // 超时给 300s：若致命态早退失效，测试会真的等 300s（暴露问题）。
        let executor = RolloutExecutor::new(
            bin_dir.join("fake-kubectl").to_string_lossy().as_ref(),
            "cogneva",
            1,
            1,
            300,
        );
        let cfg = MainlineDeployerConfig::default();
        let plan = RolloutPlan::from_config(&cfg, "localhost:30500/cogneva:main-new".into());
        let err = executor.run(&plan).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("fatal waiting state CrashLoopBackOff"),
            "{err}"
        );

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            calls.contains("set image deployment/cogneva-security-gateway security-gateway=localhost:30500/cogneva:main-old"),
            "fatal state must trigger rollback to snapshotted prev: {calls}"
        );
    }

    #[tokio::test]
    async fn job_status_distinguishes_failed_when_succeeded_field_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = tmp.path().to_path_buf();
        // 真实 kubectl 对失败 Job 的输出形如 "|1|"：succeeded 缺省，
        // 空格分隔解析会把 failed 顶到第一位误判 Complete。
        let script = r#"#!/bin/sh
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then
    case "$a" in jsonpath=*) ;; *) echo "bad -o arg" >&2; exit 2 ;; esac
  fi
  prev="$a"
done
case "$*" in
  *job-failed*) echo "|1|" ;;
  *job-ok*) echo "1||" ;;
  *job-running*) echo "||1" ;;
  *) echo "not found" >&2; exit 1 ;;
esac
exit 0
"#;
        write_fake_bin(&bin_dir, "fake-kubectl", script);
        let cfg = MainlineDeployerConfig {
            kubectl_bin: bin_dir.join("fake-kubectl").to_string_lossy().to_string(),
            ..Default::default()
        };
        let deployer = MainlineDeployer::new(cfg, test_workspaces(&bin_dir, &bin_dir));
        assert_eq!(
            deployer.job_status("job-failed").await.unwrap(),
            JobStatus::Failed
        );
        assert_eq!(
            deployer.job_status("job-ok").await.unwrap(),
            JobStatus::Complete
        );
        assert_eq!(
            deployer.job_status("job-running").await.unwrap(),
            JobStatus::Running
        );
        assert_eq!(
            deployer.job_status("job-missing").await.unwrap(),
            JobStatus::NotFound
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
