//! Cogneva 元启动引导器（第二步：Rust 引导器全自动部署）。
//!
//! 职责：
//! 1. 静默探测 CPU/内存/架构/节点；
//! 2. 按环境变量与规模选集群供给：默认装 K3s（单节点 / 多节点 server+agents）；
//!    `COGNEVA_CLUSTER_DISTRO=kubespray` 且资源门禁通过时，用 kubespray 官方
//!    镜像新建标准 Kubernetes（即 K8s），门禁不过则告警并自动回落 K3s。用户
//!    既有集群（K3s 或标准 K8s）只复用、不重建；
//! 3. 生成 intent_config.yaml；
//! 4. 安装容器运行时 / buildah，并按供给装 K3s 或跑 kubespray（或复用现有集群）；
//! 5. 供给运行时镜像：优先下载预构建 release 包（sha256 校验后导入集群），
//!    不可用时回退从源码构建（K3s 单节点本地导入，K3s 多节点与标准 K8s 经
//!    DaemonSet 逐节点分发；清单引用 localhost/cogneva:local）；
//! 6. kubectl apply 部署清单并等待关键 Pod Ready；
//! 7. 打印 WebUI 地址并自动打开浏览器，退出（自毁）。
//!
//! 全程零问答，引导器完全不接触 LLM：接入由部署完成后的 WebUI 强制向导
//! （未配置不可关闭）完成，无人值守自动化直接调向导背后的
//! POST /api/v1/admin/llm-config（先登录拿 admin token）。

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

/// 在 /var/tmp 下创建 0700 随机工作目录。/var/tmp 全局可写，固定文件名会被
/// symlink 抢跑（root 写文件时被重定向到任意路径），故目录名带 urandom 熵
/// 且用 create_dir 独占创建（已存在即失败，不跟随符号链接）。
fn make_workdir(tag: &str) -> Result<PathBuf> {
    let mut entropy = [0u8; 8];
    std::fs::File::open("/dev/urandom")
        .context("无法读取 /dev/urandom")?
        .read_exact(&mut entropy)?;
    let dir = PathBuf::from("/var/tmp").join(format!(
        "cogneva-{tag}-{:016x}",
        u64::from_le_bytes(entropy)
    ));
    std::fs::create_dir(&dir).with_context(|| format!("无法创建工作目录 {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(dir)
}

use anyhow::{bail, Context, Result};
use cogneva_bootstrap::Distro;
use serde::Serialize;
use tokio::process::Command;
use tracing::{info, warn};

mod kubespray;

/// 集群供给决策：发行版 + 是否多节点 +（若有）从 kubespray 回落 K3s 的原因。
#[derive(Debug, Clone)]
struct ProvisionDecision {
    distro: Distro,
    multi: bool,
    fallback_reason: Option<String>,
}

/// 读取 `COGNEVA_CLUSTER_DISTRO=k3s|kubespray`（默认 k3s）。非法值告警回落 k3s。
fn requested_distro() -> Distro {
    match std::env::var("COGNEVA_CLUSTER_DISTRO")
        .ok()
        .as_deref()
        .map(str::trim)
    {
        Some("kubespray") => Distro::Kubespray,
        Some("k3s") | None => Distro::K3s,
        Some(other) => {
            warn!("未知 COGNEVA_CLUSTER_DISTRO={other}，按默认 k3s 处理");
            Distro::K3s
        }
    }
}

/// 决定集群供给：用户选 kubespray 时先过资源门禁（本机控制面内存 ≥2GB；多节点
/// 要求工作节点 SSH 免密可达），不过则告警并**自动回落 K3s**，元启动不中断。
/// K3s 路径维持现状（内存 <2GB 或单节点 → 单节点，否则多节点）。
async fn decide_provision(hw: &Hardware) -> ProvisionDecision {
    let workers = cluster_nodes_env();
    let k3s_multi = hw.mem_total_mb >= 2048 && hw.nodes > 1;
    let fallback = |reason: String| {
        warn!("{reason}：自动回落 K3s 供给");
        ProvisionDecision {
            distro: Distro::K3s,
            multi: k3s_multi,
            fallback_reason: Some(reason),
        }
    };

    match requested_distro() {
        Distro::K3s => ProvisionDecision {
            distro: Distro::K3s,
            multi: k3s_multi,
            fallback_reason: None,
        },
        Distro::Kubespray => {
            if hw.mem_total_mb < 2048 {
                return fallback(format!(
                    "kubespray 标准 K8s 控制面需要 ≥2GB 内存，当前 {}MB",
                    hw.mem_total_mb
                ));
            }
            for w in &workers {
                if !kubespray::node_ssh_reachable(w).await {
                    return fallback(format!("kubespray 工作节点 {w} SSH 免密不可达"));
                }
            }
            info!("资源门禁通过：使用 kubespray 新建标准 Kubernetes");
            ProvisionDecision {
                distro: Distro::Kubespray,
                multi: !workers.is_empty(),
                fallback_reason: None,
            }
        }
    }
}

/// 部署 profile：Helm chart 预渲染产物的环境形态。chart 是拓扑唯一权威源，
/// 环境差异（containerd socket、StorageClass、git-remote 供给方式）在 CI
/// 渲染时固化进各 profile，元启动探测环境后选定，用户不做选择。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Profile {
    /// K3s 单节点：/run/k3s/containerd、local-path + Retain SC、git-remote 走宿主 hostPath。
    K3sSingle,
    /// K3s 多节点：同上，但 git-remote 走集群卷（hostPath 跨节点不可达）。
    K3sMulti,
    /// 标准 K8s（kubeadm/EKS 等）：标准 containerd socket、PVC 跟随集群默认 SC。
    K8sStandard,
}

impl Profile {
    fn dir_name(self) -> &'static str {
        match self {
            Profile::K3sSingle => "k3s-single",
            Profile::K3sMulti => "k3s-multi",
            Profile::K8sStandard => "k8s-standard",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct Hardware {
    cpu_cores: usize,
    mem_total_mb: u64,
    arch: String,
    nodes: usize,
    /// /dev/kvm 可用：可启用 Firecracker 微虚拟机沙盒（审计 2.5.4）。
    kvm: bool,
}

#[derive(Debug, Serialize)]
struct IntentConfig {
    /// 实际供给的发行版（k3s / kubespray）；资源门禁回落时记的是回落结果。
    distro: Distro,
    /// 是否多节点形态（决定 K3s server+agents 与应用副本/事件总线）。
    multi: bool,
    /// 请求 kubespray 但门禁不过、自动回落 K3s 时的原因。
    fallback_reason: Option<String>,
    hardware: Hardware,
}

/// 节点数探测：env COGNEVA_NODES 显式覆盖优先；集群已存在时按预期最终数
/// （现有节点 + 声明但未入群的 agent）；无集群但已声明 COGNEVA_CLUSTER_NODES
/// 时按 server+agents 预期数；否则默认 1。
async fn probe_nodes() -> usize {
    if let Ok(v) = std::env::var("COGNEVA_NODES") {
        if let Ok(n) = v.parse::<usize>() {
            if n >= 1 {
                return n;
            }
        }
        warn!("COGNEVA_NODES={v} 无法解析为正整数，按实际探测");
    }
    let out = Command::new("kubectl")
        .args(["get", "nodes", "-o", "name"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await;
    if let Ok(o) = out {
        if o.status.success() {
            let n = String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter(|l| !l.trim().is_empty())
                .count();
            if n >= 1 {
                // 分支决策看的是"预期最终节点数"：现有节点 + 声明但尚未
                // 入群的 agent。只看现有数会把"已有多节点声明"错判成单节点
                // K3s 分支，agent 永远装不上（2026-08-04 实测抓到）
                let existing = cluster_internal_ips().await;
                let new_agents = cluster_nodes_env()
                    .iter()
                    .filter(|t| !existing.iter().any(|ip| ip == &target_host(t)))
                    .count();
                return n + new_agents;
            }
        }
    }
    let declared = cluster_nodes_env().len();
    if declared > 0 {
        1 + declared
    } else {
        1
    }
}

async fn probe_hardware() -> Hardware {
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mem_total_mb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
        })
        .map(|kb| kb / 1024)
        .unwrap_or(0);
    Hardware {
        cpu_cores,
        mem_total_mb,
        arch: std::env::consts::ARCH.to_string(),
        nodes: probe_nodes().await,
        kvm: Path::new("/dev/kvm").exists(),
    }
}

async fn run(cmd: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(cmd)
        .args(args)
        .stdin(Stdio::null())
        .status()
        .await
        .with_context(|| format!("无法执行 {cmd}"))?;
    if !status.success() {
        bail!("{cmd} {:?} 退出码 {:?}", args, status.code());
    }
    Ok(())
}

async fn command_exists(cmd: &str) -> bool {
    Command::new(cmd)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// 复用现有集群（kubectl 可用且能连通）则跳过安装。
async fn cluster_ready() -> bool {
    Command::new("kubectl")
        .args(["cluster-info"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn install_k3s() -> Result<()> {
    if cluster_ready().await {
        info!("检测到可用集群，跳过 K3s 安装");
        return Ok(());
    }
    info!("安装 K3s（官方脚本）...");
    if cn_mirror() {
        write_k3s_registries_cn()?;
    }
    // 受限网络：get.k3s.io 的 GitHub releases 下载必挂，走 rancher 国内镜像站
    let install = if cn_mirror() {
        "curl -fsSL https://rancher-mirror.rancher.cn/k3s/k3s-install.sh | INSTALL_K3S_MIRROR=cn sh -"
    } else {
        "curl -fsSL https://get.k3s.io | sh -"
    };
    let status = Command::new("sh").args(["-c", install]).status().await?;
    if !status.success() {
        bail!("K3s 安装失败");
    }
    // 等待 kubeconfig 就绪
    for _ in 0..30 {
        if cluster_ready().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    bail!("K3s 安装后集群未就绪");
}

/// CN 模式预置 K3s containerd 镜像站配置。K3s 系统镜像（coredns /
/// local-path-provisioner / pause 等）全走 docker.io，CN 空白机直连必爬
/// （2026-08-05 嵌套回归实测：coredns 23MB 镜像直连拉 10 分钟）。
/// 必须在 k3s 首次启动前写入；agent 侧由 install_k3s_agents 远程预置同一文件。
/// endpoint 多列几家，containerd 按序自动回退，单站故障不阻塞装机。
fn write_k3s_registries_cn() -> Result<()> {
    std::fs::create_dir_all("/etc/rancher/k3s")?;
    std::fs::write("/etc/rancher/k3s/registries.yaml", k3s_registries_yaml())?;
    info!("已预置 K3s registries.yaml（docker.io 多镜像站候选）");
    Ok(())
}

/// 多节点声明：COGNEVA_CLUSTER_NODES="user@ip[:port],user@ip2,..."。
/// 要求本机到各目标 SSH 免密可达（key 认证）。
fn cluster_nodes_env() -> Vec<String> {
    std::env::var("COGNEVA_CLUSTER_NODES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// 解析 SSH 目标："user@host:port" / "user@host" / "host:port" / "host"。
fn parse_ssh_target(target: &str) -> (String, Option<String>) {
    let (user_part, host_part) = match target.split_once('@') {
        Some((u, h)) => (format!("{u}@"), h.to_string()),
        None => (String::new(), target.to_string()),
    };
    match host_part.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => {
            (format!("{user_part}{h}"), Some(p.to_string()))
        }
        _ => (format!("{user_part}{host_part}"), None),
    }
}

fn target_host(target: &str) -> String {
    let (ssh, _) = parse_ssh_target(target);
    ssh.rsplit('@').next().unwrap_or(&ssh).to_string()
}

async fn first_ipv4() -> String {
    let out = Command::new("sh")
        .args(["-c", "hostname -I 2>/dev/null | awk '{print $1}'"])
        .stdin(Stdio::null())
        .output()
        .await;
    out.ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

async fn cluster_internal_ips() -> Vec<String> {
    let out = Command::new("kubectl")
        .args([
            "get",
            "nodes",
            "-o",
            "jsonpath={.items[*].status.addresses[?(@.type==\"InternalIP\")].address}",
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await;
    out.ok()
        .filter(|o| o.status.success())
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .split_whitespace()
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// K3s 多节点供给：本机作 server，再经 SSH 给声明的工作节点推装 agent。
///
/// 这是 **K3s 发行版**的多节点路径。K3s 能一键多节点，是因为它替用户做完了
/// 那一整套集群底座决策（内置 CNI、embedded etcd、local-path 存储、自管证书），
/// 代价是它是裁剪过的单二进制发行版、不是上游标准 Kubernetes。
///
/// 要新建**上游标准 Kubernetes（即 K8s）**不走这里——那是 `kubespray::run_kubespray`：
/// 跑 kubespray 官方容器镜像，由 Ansible 承载 etcd 拓扑 / CNI / PKI / kubeadm
/// token / CRI 版本对齐这整套决策（见 `kubespray` 模块）。用户既有集群（K3s
/// 或标准 K8s）则只复用、不重建。
///
/// 已有可用集群时仅补齐声明中缺失的 agent；无集群且无节点声明 → 失败前置。
async fn ensure_multi_node_cluster() -> Result<()> {
    let agents = cluster_nodes_env();
    if !cluster_ready().await {
        if agents.is_empty() {
            bail!(
                "K3s 多节点分支需要多节点集群：请用 COGNEVA_CLUSTER_NODES=user@ip[,user@ip2...] \
                 声明工作节点（本机将作为 server，需 SSH 免密可达），或预先搭建集群"
            );
        }
        install_k3s().await?;
    }
    if agents.is_empty() {
        info!("未声明 COGNEVA_CLUSTER_NODES，使用现有集群节点");
        return Ok(());
    }
    install_k3s_agents(&agents).await?;
    wait_all_nodes_ready(1 + agents.len()).await
}

async fn install_k3s_agents(agents: &[String]) -> Result<()> {
    let token = std::fs::read_to_string("/var/lib/rancher/k3s/server/node-token")
        .context("读取 K3s server token 失败（本机不是 K3s server？多节点要求本机先成为 server）")?
        .trim()
        .to_string();
    let server_url = match std::env::var("COGNEVA_K3S_URL") {
        Ok(u) => u,
        Err(_) => format!("https://{}:6443", first_ipv4().await),
    };
    /// 查询 server 端 k3s 版本（如 v1.35.5+k3s1），查不到返回 None。
    async fn server_k3s_version() -> Option<String> {
        let out = Command::new("kubectl")
            .args(["version", "-o", "json"])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
        v["serverVersion"]["gitVersion"]
            .as_str()
            .map(|s| s.to_string())
    }
    // agent 版本必须与 server 对齐：不钉版本安装脚本会拉最新 stable，
    // kubelet 比 apiserver 新违反 K8s 版本偏移策略（2026-08-04 实测 agent
    // 装上 v1.36.2 而 server 是 v1.35.5）
    let version_env = match server_k3s_version().await {
        Some(v) => format!(" INSTALL_K3S_VERSION={v}"),
        None => String::new(),
    };
    // 管道左侧的变量前缀只对 curl 生效，K3S_URL/K3S_TOKEN 必须写在
    // 管道右侧 sh 前面，否则安装脚本收不到会装成独立 server（脑裂），
    // 2026-08-04 嵌套实测抓到：目标机起了 k3s.service 而非 k3s-agent
    let install = if cn_mirror() {
        // agent 同样要在 k3s-agent 首启前预置 registries.yaml（pause 等系统镜像走 docker.io）
        let reg = k3s_registries_yaml()
            .replace('\n', "\\n")
            .replace('"', "\\\"");
        format!("mkdir -p /etc/rancher/k3s && printf '{reg}' > /etc/rancher/k3s/registries.yaml && curl -fsSL https://rancher-mirror.rancher.cn/k3s/k3s-install.sh | K3S_URL={server_url} K3S_TOKEN={token}{version_env} INSTALL_K3S_MIRROR=cn sh -")
    } else {
        format!("curl -fsSL https://get.k3s.io | K3S_URL={server_url} K3S_TOKEN={token}{version_env} sh -")
    };
    let existing_ips = cluster_internal_ips().await;
    for target in agents {
        let host = target_host(target);
        if existing_ips.iter().any(|ip| ip == &host) {
            info!("节点已在集群中，跳过: {target}");
            continue;
        }
        info!("安装 K3s agent: {target}（加入 {server_url}）...");
        let (ssh_target, port) = parse_ssh_target(target);
        let remote = &install;
        let mut args: Vec<String> = vec![
            "-o".into(),
            "BatchMode=yes".into(),
            "-o".into(),
            "ConnectTimeout=10".into(),
            "-o".into(),
            "StrictHostKeyChecking=accept-new".into(),
        ];
        if let Some(p) = port {
            args.push("-p".into());
            args.push(p);
        }
        args.push(ssh_target);
        args.push(remote.to_string());
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run("ssh", &arg_refs)
            .await
            .with_context(|| format!("agent 安装失败 {target}（需要本机到目标的 SSH 免密可达）"))?;
    }
    Ok(())
}

async fn wait_all_nodes_ready(expected: usize) -> Result<()> {
    info!("等待 {expected} 个节点全部 Ready...");
    for _ in 0..60 {
        let out = Command::new("kubectl")
            .args(["get", "nodes", "--no-headers"])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await;
        if let Ok(o) = out {
            if o.status.success() {
                let ready = String::from_utf8_lossy(&o.stdout)
                    .lines()
                    .filter(|l| l.split_whitespace().nth(1) == Some("Ready"))
                    .count();
                if ready >= expected {
                    info!("全部 {ready} 个节点 Ready");
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    bail!("agent 节点未在 5 分钟内全部 Ready，请检查各节点安装日志")
}

async fn ensure_buildah() -> Result<()> {
    if command_exists("buildah").await {
        info!("buildah 已安装");
        return Ok(());
    }
    info!("安装 buildah...");
    run("apt-get", &["update"]).await?;
    run("apt-get", &["install", "-y", "buildah"]).await?;
    Ok(())
}

/// 自进化 git 远程：evolution worker 的 hostPath bare 仓库（沙盒与宿主双向同步
/// 通道，清单里写死 /var/lib/cogneva-data/git-remote）。空白机上该目录不存在
/// 会导致 evolution Pod FailedMount，必须在部署清单前创建并 seed 源码。
///
/// bare 仓库直接从上游公开仓库**完整** clone（含全部历史与 tag），与多节点/PVC
/// 模式 initContainer 的 seed 行为一致。不能从节点源码 clone：bootstrap 取码是
/// `--depth 1` 浅克隆、tarball 路径连 .git 都没有，会把进化中央仓库也变成浅/
/// 零历史——进化需要完整历史做 log/blame/tag/基线对齐。节点源码（浅克隆）仍供
/// 编译引导器与 apply 清单，与进化 bare 解耦。
async fn ensure_git_remote() -> Result<()> {
    let remote = Path::new("/var/lib/cogneva-data/git-remote");
    if remote.join("HEAD").exists() {
        info!("git-remote bare 仓库已存在，跳过");
        return Ok(());
    }
    if let Some(parent) = remote.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // 地址与 chart evolution.gitRemote.seedUrl 同源；CN 走 Gitee，失败回落另一地址。
    let (primary, fallback) = if cn_mirror() {
        (
            "https://gitee.com/hcipengm/cogneva.git",
            "https://github.com/hcipengm/cogneva.git",
        )
    } else {
        (
            "https://github.com/hcipengm/cogneva.git",
            "https://gitee.com/hcipengm/cogneva.git",
        )
    };
    info!(
        "初始化自进化 git 远程仓库（从上游完整 clone）→ {}",
        remote.display()
    );
    for url in [primary, fallback] {
        // 失败重试前清掉半截 clone 产物，避免下次 clone 因目录非空报错。
        std::fs::remove_dir_all(remote).ok();
        match run("git", &["clone", "--bare", url, &remote.to_string_lossy()]).await {
            Ok(()) => {
                info!("git-remote bare 已从 {url} seed");
                return Ok(());
            }
            Err(e) => warn!("从 {url} clone git-remote bare 失败：{e}，尝试下一地址"),
        }
    }
    bail!("无法从任一上游地址 clone git-remote bare 仓库");
}

async fn ensure_firecracker() -> Result<()> {
    if !Path::new("/dev/kvm").exists() {
        warn!("KVM 不可用（/dev/kvm 缺失），跳过 Firecracker 安装；沙盒保持 K8s Pod 形态");
        return Ok(());
    }
    if command_exists("firecracker").await {
        info!("firecracker 已安装");
        return Ok(());
    }
    info!("安装 firecracker（官方 release）...");
    let version = "v1.7.0";
    let arch = std::env::consts::ARCH;
    let url = format!(
        "https://github.com/firecracker-microvm/firecracker/releases/download/{version}/firecracker-{version}-{arch}.tgz"
    );
    let script = format!(
        "set -e; d=$(mktemp -d); trap 'rm -rf \"$d\"' EXIT; cd \"$d\" && \
         curl -fsSL '{url}' -o firecracker.tgz && tar -xzf firecracker.tgz && \
         install -m 0755 release-{version}-{arch}/firecracker-{version}-{arch} /usr/local/bin/firecracker"
    );
    let status = Command::new("sh").args(["-c", &script]).status().await?;
    if !status.success() {
        warn!("firecracker 安装失败；沙盒保持 K8s Pod 形态（可稍后手动安装并启用 microvm）");
        return Ok(());
    }
    info!("firecracker 安装完成");
    Ok(())
}

/// 应用拓扑产物目录：chart 预渲染的 profile standalone YAML（元启动读目录
/// kubectl apply，引导链零 helm 依赖）。
fn rendered_manifest_dir(profile: Profile) -> PathBuf {
    repo_root().join("deploy/rendered").join(profile.dir_name())
}

fn repo_root() -> PathBuf {
    PathBuf::from(std::env::var("COGNEVA_REPO_ROOT").unwrap_or_else(|_| ".".to_string()))
}

/// 探测集群是否 K3s 发行版：节点 label node.kubernetes.io/instance-type=k3s
/// （K3s 安装时自动打）；取不到 label 时回看本机 /run/k3s（元启动自建
/// K3s 的 server/agent 节点必有）。
async fn probe_is_k3s() -> bool {
    let out = Command::new("kubectl")
        .args([
            "get",
            "nodes",
            "-o",
            "jsonpath={.items[*].metadata.labels.node\\.kubernetes\\.io/instance-type}",
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await;
    if let Ok(o) = out {
        if o.status.success()
            && String::from_utf8_lossy(&o.stdout)
                .split_whitespace()
                .any(|t| t == "k3s")
        {
            return true;
        }
    }
    Path::new("/run/k3s").exists()
}

/// 标准 K8s 路径的 PVC 全部跟随集群默认 StorageClass（不绑定 Longhorn 等
/// 厂商），部署前硬校验默认 SC 存在；缺失即报错提示先装存储并设为默认。
async fn ensure_default_storage_class() -> Result<()> {
    let out = Command::new("kubectl")
        .args([
            "get",
            "sc",
            "-o",
            "jsonpath={.items[?(@.metadata.annotations.storageclass\\.kubernetes\\.io/is-default-class==\"true\")].metadata.name}",
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await?;
    if String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .is_some()
    {
        return Ok(());
    }
    bail!(
        "集群没有默认 StorageClass。标准 K8s 路径的 PVC 全部跟随集群默认 SC，\
         请先安装 Longhorn 等存储供应并把它设为默认，例如：\n  \
         kubectl patch sc <存储类名> -p '{{\"metadata\":{{\"annotations\":{{\
         \"storageclass.kubernetes.io/is-default-class\":\"true\"}}}}}}'"
    );
}

/// 探测部署 profile：发行版（K3s / 标准 K8s）× 节点数（单 / 多）。
async fn detect_profile() -> Result<Profile> {
    let nodes = probe_nodes().await;
    let is_k3s = probe_is_k3s().await;
    let profile = if is_k3s {
        if nodes > 1 {
            Profile::K3sMulti
        } else {
            Profile::K3sSingle
        }
    } else {
        ensure_default_storage_class().await?;
        Profile::K8sStandard
    };
    info!(
        "环境探测: {} 发行版 / {} 节点 → {} profile",
        if is_k3s { "K3s" } else { "标准 K8s" },
        nodes,
        profile.dir_name()
    );
    Ok(profile)
}

/// 受限网络（CN）模式：由 bootstrap.sh 探测后通过 COGNEVA_CN_MIRROR 传入。
fn cn_mirror() -> bool {
    std::env::var("COGNEVA_CN_MIRROR").ok().as_deref() == Some("1")
}

// ---------- CN 镜像多候选自动选择 ----------
// 每个环节给多家候选，按顺序探活（5s 超时），第一家能用的胜出；
// 全部不可达时回退第一个候选（不低于过去写死单镜像的行为，
// 后续下载层的重试机制仍会兜底）。

/// docker.io 镜像站候选（CN 模式）。
const DOCKER_MIRROR_CANDIDATES: &[&str] = &[
    "docker.m.daocloud.io",
    "docker.1ms.run",
    "docker.1panel.live",
    "hub.rat.dev",
];

/// 探活：5s 内拿到任何 HTTP 响应即算存活（docker registry 未认证
/// 返回 401 也是健康），连接失败/超时才算不可达。
async fn probe_alive(url: &str) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    client.get(url).send().await.is_ok()
}

/// 候选表按顺序探活，返回第一个可用项的值；全挂回退第一项。
/// 表项为（选用值，探活 URL）。
async fn pick_alive(candidates: &[(&str, &str)]) -> String {
    for (value, probe) in candidates {
        if probe_alive(probe).await {
            return (*value).to_string();
        }
        warn!("镜像不可达，换下一个: {probe}");
    }
    candidates[0].0.to_string()
}

static DOCKER_MIRROR: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();

/// 选定 docker.io 镜像站（全进程一次探测，后续复用结果）。
async fn docker_mirror_host() -> &'static str {
    DOCKER_MIRROR
        .get_or_init(|| async {
            for host in DOCKER_MIRROR_CANDIDATES {
                if probe_alive(&format!("https://{host}/v2/")).await {
                    info!("docker.io 镜像站选定: {host}");
                    return host.to_string();
                }
                warn!("docker 镜像站不可达，换下一个: {host}");
            }
            DOCKER_MIRROR_CANDIDATES[0].to_string()
        })
        .await
}

/// K3s containerd registries.yaml：endpoint 全列，containerd 自己按序回退，
/// 无需探活；全部 endpoint 失败后 containerd 还会回源 docker.io 直连。
fn k3s_registries_yaml() -> String {
    let mut s = String::from("mirrors:\n  docker.io:\n    endpoint:\n");
    for h in DOCKER_MIRROR_CANDIDATES {
        s.push_str(&format!("      - \"https://{h}\"\n"));
    }
    s
}

/// 受限网络下为 buildah 配置 docker.io 镜像（Docker Hub 被墙，基础镜像拉取必挂）。
/// 多家候选全列进 [[registry.mirror]]，buildah 按序自动回退。
async fn ensure_buildah_mirror() -> Result<()> {
    if !cn_mirror() {
        return Ok(());
    }
    let dir = Path::new("/etc/containers/registries.conf.d");
    std::fs::create_dir_all(dir)?;
    let mut conf = String::from(
        "unqualified-search-registries = [\"docker.io\"]\n\
         [[registry]]\n\
         prefix = \"docker.io\"\n\
         location = \"docker.io\"\n",
    );
    for h in DOCKER_MIRROR_CANDIDATES {
        conf.push_str(&format!("\n[[registry.mirror]]\nlocation = \"{h}\"\n"));
    }
    std::fs::write(dir.join("cn-mirror.conf"), conf)?;
    info!("已配置 buildah docker.io 镜像站候选");
    Ok(())
}

/// 运行时镜像供给：清单引用 localhost/cogneva:local。
/// 优先从 GitHub/Gitee release 下载预构建镜像（sha256 校验），失败回退源码构建
/// （空白机全量 Rust release 构建需 1-3 小时，预构建下载仅需数分钟）。
/// 仅 K3s 单节点走本机 `k3s ctr import` 快路径；K3s 多节点与 kubespray 标准
/// K8s（单/多节点）都没有"本机即唯一节点"的前提，统一经镜像分发器 DaemonSet
/// 逐节点导入宿主 containerd（分发器自动探测 ctr 二进制与 containerd socket）。
async fn ensure_runtime_image(distro: Distro, multi: bool) -> Result<()> {
    const IMAGE: &str = "localhost/cogneva:local";
    let local_fast_path = matches!(distro, Distro::K3s) && !multi;
    if !local_fast_path {
        return distribute_image_to_nodes(IMAGE).await;
    }
    let present = Command::new("k3s")
        .args(["ctr", "-n", "k8s.io", "images", "ls", "-q"])
        .stdin(Stdio::null())
        .output()
        .await
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .any(|l| l == IMAGE)
        })
        .unwrap_or(false);
    if present {
        info!("运行时镜像已存在于集群: {IMAGE}");
        return Ok(());
    }
    match try_import_prebuilt(IMAGE).await {
        Ok(()) => {
            info!("预构建镜像已导入集群: {IMAGE}");
            return Ok(());
        }
        Err(e) => warn!("预构建镜像不可用（{e:#}），回退源码构建"),
    }
    build_runtime_image_from_source(IMAGE).await
}

/// 逐卷下载 {url}.part-aa/.part-ab... 并拼接为 tar。首卷不存在说明该
/// release 没有分卷，返回 Err 让上层走源码构建回退；中间卷缺失视为损坏。
async fn download_parts(url: &str, workdir: &Path, tar: &str) -> Result<()> {
    let mut out = std::fs::File::create(tar)?;
    for i in 0..usize::MAX {
        let suffix = format!(
            "{}{}",
            (b'a' + (i / 26) as u8) as char,
            (b'a' + (i % 26) as u8) as char
        );
        let part = workdir
            .join(format!("part-{suffix}"))
            .to_string_lossy()
            .into_owned();
        let got = run(
            "curl",
            &[
                "-fsSL",
                "--connect-timeout",
                "15",
                "--max-time",
                "1800",
                "--retry",
                "2",
                "-o",
                &part,
                &format!("{url}.part-{suffix}"),
            ],
        )
        .await;
        match got {
            Ok(()) => {
                info!("分卷 part-{suffix} 下载完成");
                let data = std::fs::read(&part)?;
                std::io::Write::write_all(&mut out, &data)?;
                let _ = std::fs::remove_file(&part);
            }
            Err(e) if i == 0 => bail!("首卷 part-aa 不存在，该 release 无分卷（{e}）"),
            Err(_) => break,
        }
    }
    use std::io::Seek;
    if out.stream_position()? == 0 {
        bail!("分卷下载结果为空");
    }
    Ok(())
}

/// 下载预构建镜像 tar.gz 并做 sha256 校验。返回（工作目录，tar 路径），
/// 工作目录由调用方负责清理。任何失败（无 release、网络、sha 不匹配）返回 Err。
async fn fetch_prebuilt_tar() -> Result<(PathBuf, String)> {
    let version = env!("CARGO_PKG_VERSION");
    let arch = std::env::consts::ARCH;
    let name = format!("cogneva-image-v{version}-linux-{arch}.tar.gz");
    let (url, expect_sha) = match std::env::var("COGNEVA_IMAGE_URL") {
        Ok(u) => (u, std::env::var("COGNEVA_IMAGE_SHA256").ok()),
        Err(_) => {
            let base = if cn_mirror() {
                format!("https://gitee.com/hcipengm/cogneva/releases/download/v{version}")
            } else {
                format!("https://github.com/hcipengm/cogneva/releases/download/v{version}")
            };
            let sha_body = download_string(&format!("{base}/{name}.sha256")).await?;
            let sha = sha_body
                .split_whitespace()
                .next()
                .context("sha256 文件格式异常")?
                .to_string();
            (format!("{base}/{name}"), Some(sha))
        }
    };
    let expect_sha =
        expect_sha.context("缺少预期 sha256（COGNEVA_IMAGE_SHA256 或 release .sha256 文件）")?;

    let workdir = make_workdir("prebuilt")?;
    let fetch = async {
        let tar = workdir.join(&name).to_string_lossy().into_owned();
        info!("下载预构建镜像 {url} ...");
        let direct = run(
            "curl",
            &[
                "-fsSL",
                "--connect-timeout",
                "15",
                "--max-time",
                "3600",
                "--retry",
                "2",
                "-o",
                &tar,
                &url,
            ],
        )
        .await;
        if let Err(e) = direct {
            // Gitee 附件单文件限 100MB，镜像包超限时按 .part-aa/.part-ab...
            // 分卷发布；整包 404 时回退逐卷下载再拼接
            info!("整包下载失败（{e}），尝试分卷下载...");
            download_parts(&url, &workdir, &tar).await?;
        }
        let actual_sha = sha256_file(&tar).await?;
        if !actual_sha.eq_ignore_ascii_case(&expect_sha) {
            bail!("sha256 不匹配（期望 {expect_sha}，实际 {actual_sha}）");
        }
        info!("sha256 校验通过");
        Ok(tar)
    }
    .await;
    match fetch {
        Ok(tar) => Ok((workdir, tar)),
        Err(e) => {
            let _ = std::fs::remove_dir_all(&workdir);
            Err(e)
        }
    }
}

/// K3s 路径：下载预构建镜像并导入本机 containerd。
async fn try_import_prebuilt(image: &str) -> Result<()> {
    let (workdir, tar) = fetch_prebuilt_tar().await?;
    let result = async {
        info!("导入 K3s containerd...");
        // containerd import 原生识别 gzip 压缩 tar
        run("k3s", &["ctr", "-n", "k8s.io", "images", "import", &tar]).await?;
        // 确认导入后清单引用的标签存在
        let present = Command::new("k3s")
            .args(["ctr", "-n", "k8s.io", "images", "ls", "-q"])
            .stdin(Stdio::null())
            .output()
            .await
            .map(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .lines()
                    .any(|l| l == image)
            })
            .unwrap_or(false);
        if !present {
            bail!("导入后集群中不存在标签 {image}（release 包内标签不符）");
        }
        // 补不可变版本 tag：归档内只有浮动签 :local，版本 tag 供追溯与
        // 按版本引用（ManagementPlan 同步等路径）。失败不致命，:local 已就绪。
        let versioned = format!("localhost/cogneva:{}", env!("CARGO_PKG_VERSION"));
        if let Err(e) = run(
            "k3s",
            &["ctr", "-n", "k8s.io", "images", "tag", image, &versioned],
        )
        .await
        {
            warn!("版本标签 {versioned} 打标失败（不影响 :local 部署）: {e:#}");
        }
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&workdir);
    result
}

/// K3s 多节点镜像供给：本机导入只覆盖单节点，多节点必须让每个节点的
/// containerd 都拥有镜像。分发器模式：集群内起临时 HTTP 服务承载 tar.gz，
/// DaemonSet 在每节点用宿主 ctr 二进制导入宿主 containerd，全部就绪后清理。
/// COGNEVA_IMAGE_REGISTRY 已配置 = 生产仓库供给，直接跳过（清单镜像引用
/// 由运维自行对齐，见 deploy/k8s/README.md）。
async fn distribute_image_to_nodes(image: &str) -> Result<()> {
    if std::env::var("COGNEVA_IMAGE_REGISTRY").is_ok() {
        info!("COGNEVA_IMAGE_REGISTRY 已配置，假定生产仓库供给，跳过逐节点分发");
        return Ok(());
    }
    let (workdir, tar) = match fetch_prebuilt_tar().await {
        Ok(v) => v,
        Err(e) => {
            warn!("预构建镜像不可用（{e:#}），回退本机源码构建后分发");
            build_image_locally(image).await?;
            export_image_tar(image).await?
        }
    };
    let result = distribute_via_daemonset(&tar).await;
    let _ = std::fs::remove_dir_all(&workdir);
    result?;
    info!("镜像已分发到全部节点: {image}");
    Ok(())
}

/// 起临时镜像服务 Pod → kubectl cp 注入 tar → DaemonSet 逐节点导入 → 清理。
async fn distribute_via_daemonset(tar: &str) -> Result<()> {
    let manifest = render_distributor_manifest().await?;
    let mdir = make_workdir("distributor")?;
    let mpath = mdir.join("image-distributor.yaml");
    std::fs::write(&mpath, &manifest)?;
    let mstr = mpath.to_string_lossy().into_owned();

    let run_result = async {
        // 命名空间可能尚不存在（K3s 多节点分支在 deploy_manifests 之前执行）
        run(
            "sh",
            &[
                "-c",
                "kubectl create namespace cogneva --dry-run=client -o yaml | kubectl apply -f -",
            ],
        )
        .await?;
        run("kubectl", &["apply", "-f", &mstr]).await?;
        info!("注入镜像包到分发服务 Pod...");
        // 等服务 Pod Ready 才能 cp。超时给足 10 分钟：空白机首拉 busybox
        // 镜像（经镜像站）可能远超 2 分钟，超时不等于失败
        run(
            "kubectl",
            &[
                "-n", "cogneva", "wait", "--for=condition=Ready", "pod/cogneva-image-server",
                "--timeout=600s",
            ],
        )
        .await?;
        run(
            "kubectl",
            &[
                "-n",
                "cogneva",
                "cp",
                tar,
                "cogneva-image-server:/share/image.tar.gz",
            ],
        )
        .await?;
        info!("触发/重发分发（rollout restart 保证重跑时重新导入）...");
        run(
            "kubectl",
            &[
                "-n",
                "cogneva",
                "rollout",
                "restart",
                "daemonset/cogneva-image-distributor",
            ],
        )
        .await?;
        info!("等待全部分发节点完成导入（DaemonSet rollout）...");
        run(
            "kubectl",
            &[
                "-n",
                "cogneva",
                "rollout",
                "status",
                "daemonset/cogneva-image-distributor",
                "--timeout=900s",
            ],
        )
        .await
        .context("镜像分发超时：请检查节点 containerd socket 路径与 ctr 二进制（详见 deploy/k8s/README.md）")?;
        Ok(())
    }
    .await;

    // 分发器常设保留（增量升级复用：注入新 tar + rollout restart 即可，
    // 见 deploy/scripts/distribute-image.sh）；失败时也保留现场便于排查
    let _ = std::fs::remove_dir_all(&mdir);
    run_result
}

/// 渲染镜像分发器清单（busybox 镜像名按网络模式替换后写出）。
async fn render_distributor_manifest() -> Result<String> {
    let template = include_str!("../../../deploy/k8s/image-distributor.yaml");
    let busybox = if cn_mirror() {
        format!("{}/library/busybox:latest", docker_mirror_host().await)
    } else {
        "docker.io/library/busybox:latest".to_string()
    };
    Ok(template
        .replace("__BUSYBOX_IMAGE__", &busybox)
        .replace("__IMAGE_TAG__", env!("CARGO_PKG_VERSION")))
}

/// 把基镜像 localhost/cogneva:local 播种进集群内 registry：自进化金丝雀
/// overlay 镜像 FROM 该基镜像，缺失则推送端构建必败。经宿主 containerd
/// 客户端直推 NodePort（localhost http 免 TLS，多节点每节点都通）。
/// 失败不致命——基座运行不依赖 registry，swap-image 换版时也会补播；
/// 但首次金丝雀晋级前必须播种成功，故给足重试并明确告警。
async fn seed_cluster_registry() -> Result<()> {
    let waited = run(
        "kubectl",
        &[
            "-n",
            "cogneva",
            "wait",
            "--for=condition=Available",
            "deployment/cogneva-registry",
            "--timeout=300s",
        ],
    )
    .await;
    if let Err(e) = waited {
        warn!(
            "集群内 registry 未就绪，跳过基镜像播种（金丝雀晋级前需补播，\
             见 swap-image.sh）: {e:#}"
        );
        return Ok(());
    }
    // k3s 是多调用二进制（argv0=ctr），标准 containerd 直接用 ctr。
    let (program, ctr_prefix): (&str, &[&str]) = if command_exists("k3s").await {
        ("k3s", &["ctr"][..])
    } else {
        ("ctr", &[][..])
    };
    let remote = "localhost:30500/cogneva:local";
    let object = "localhost/cogneva:local";
    for attempt in 1..=3 {
        let mut args: Vec<&str> = ctr_prefix.to_vec();
        args.extend([
            "-n",
            "k8s.io",
            "images",
            "push",
            "--plain-http",
            remote,
            object,
        ]);
        match run(program, &args).await {
            Ok(()) => {
                info!("集群内 registry 已播种基镜像 {remote}");
                return Ok(());
            }
            Err(e) if attempt < 3 => {
                warn!("registry 播种第 {attempt} 次失败（重试）: {e:#}");
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
            Err(e) => {
                warn!(
                    "registry 基镜像播种失败: {e:#}。金丝雀 overlay 推送会因此失败，\
                     可在节点上手动执行 ctr -n k8s.io images push --plain-http {remote} {object}"
                );
                return Ok(());
            }
        }
    }
    Ok(())
}

async fn download_string(url: &str) -> Result<String> {
    let body = reqwest::get(url)
        .await
        .with_context(|| format!("下载失败: {url}"))?
        .error_for_status()
        .with_context(|| format!("HTTP 错误: {url}"))?
        .text()
        .await?;
    Ok(body)
}

async fn sha256_file(path: &str) -> Result<String> {
    let out = Command::new("sha256sum")
        .arg(path)
        .stdin(Stdio::null())
        .output()
        .await
        .context("无法执行 sha256sum")?;
    if !out.status.success() {
        bail!("sha256sum 退出码 {:?}", out.status.code());
    }
    let text = String::from_utf8_lossy(&out.stdout);
    text.split_whitespace()
        .next()
        .map(|s| s.to_string())
        .context("sha256sum 输出为空")
}

/// 按物理内存给 cargo 并行度：≤6G 返回 Some(2)，更大内存返回 None（按核数自动）。
fn cargo_jobs_for_memory() -> Option<usize> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kb: u64 = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    (kb / 1024 / 1024 <= 6).then_some(2)
}

/// 从源码 buildah 构建镜像到本机存储（首次需 1-3 小时）。
async fn build_image_locally(image: &str) -> Result<()> {
    let root = repo_root();
    info!("从源码构建运行时镜像 {image}（首次需较长时间）...");
    // 同时打不可变版本 tag（:local 之外的追溯锚点，与 release 预构建流一致）
    let versioned = format!("localhost/cogneva:{}", env!("CARGO_PKG_VERSION"));
    // tarball 取码无 .git，revision 退化为 "source" 标识源码回退构建
    let revision = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(&root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "source".into());
    let mut build_args: Vec<String> = vec![
        "build".into(),
        "-t".into(),
        image.into(),
        "-t".into(),
        versioned,
        "-f".into(),
        root.join("Dockerfile").to_string_lossy().into_owned(),
        "--build-arg".into(),
        format!("VERSION={}", env!("CARGO_PKG_VERSION")),
        "--build-arg".into(),
        format!("GIT_REVISION={revision}"),
    ];
    if cn_mirror() {
        // 各环节多候选探活选择，单站故障自动换站
        let rustup_arch = match std::env::consts::ARCH {
            "aarch64" => "aarch64",
            _ => "x86_64",
        };
        let rustup = pick_alive(&[
            (
                "tuna",
                // TUNA 不托管 rustup-init.sh（404），探二进制路径
                &format!("https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup/dist/{rustup_arch}-unknown-linux-gnu/rustup-init"),
            ),
            (
                "ustc",
                &format!("https://mirrors.ustc.edu.cn/rust-static/rustup/dist/{rustup_arch}-unknown-linux-gnu/rustup-init"),
            ),
        ])
        .await;
        let (dist_server, update_root) = if rustup == "ustc" {
            (
                "https://mirrors.ustc.edu.cn/rust-static",
                "https://mirrors.ustc.edu.cn/rust-static/rustup",
            )
        } else {
            (
                "https://mirrors.tuna.tsinghua.edu.cn/rustup",
                "https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup",
            )
        };
        // crates 候选必须索引与文件都自托管：TUNA 稀疏索引的 dl 仍指向
        // static.crates.io，crate 文件直连国外会超时
        let crates_sparse = pick_alive(&[
            (
                "https://rsproxy.cn/index/",
                "https://rsproxy.cn/index/config.json",
            ),
            (
                "https://mirrors.ustc.edu.cn/crates.io-index/",
                "https://mirrors.ustc.edu.cn/crates.io-index/config.json",
            ),
        ])
        .await;
        let apt_host = pick_alive(&[
            (
                "mirrors.tuna.tsinghua.edu.cn",
                "https://mirrors.tuna.tsinghua.edu.cn/ubuntu/dists/noble/Release",
            ),
            (
                "mirrors.ustc.edu.cn",
                "https://mirrors.ustc.edu.cn/ubuntu/dists/noble/Release",
            ),
            (
                "mirrors.aliyun.com",
                "https://mirrors.aliyun.com/ubuntu/dists/noble/Release",
            ),
        ])
        .await;
        let npm_registry = pick_alive(&[
            (
                "https://registry.npmmirror.com",
                "https://registry.npmmirror.com/react",
            ),
            (
                "https://mirrors.cloud.tencent.com/npm",
                "https://mirrors.cloud.tencent.com/npm/react",
            ),
            (
                "https://repo.huaweicloud.com/repository/npm",
                "https://repo.huaweicloud.com/repository/npm/react",
            ),
        ])
        .await;
        build_args.extend([
            // TUNA/USTC 都不镜像按版本 channel，CN 模式工具链只能用 stable
            "--build-arg".into(),
            "RUST_TOOLCHAIN=stable".into(),
            "--build-arg".into(),
            format!("RUSTUP_DIST_SERVER={dist_server}"),
            "--build-arg".into(),
            format!("RUSTUP_UPDATE_ROOT={update_root}"),
            "--build-arg".into(),
            format!("CARGO_REGISTRY_SPARSE={crates_sparse}"),
            "--build-arg".into(),
            format!("APT_MIRROR_HOST={apt_host}"),
            "--build-arg".into(),
            format!("NPM_REGISTRY={npm_registry}"),
        ]);
    }
    // 低内存机器限制 cargo 并行度防 OOM（2-4G 空白机上 rustc 满核并行会爆内存）
    if let Some(jobs) = cargo_jobs_for_memory() {
        build_args.extend(["--build-arg".into(), format!("CARGO_BUILD_JOBS={jobs}")]);
    }
    build_args.push(root.to_string_lossy().into_owned());
    let status = Command::new("buildah")
        .args(&build_args)
        .stdin(Stdio::null())
        .status()
        .await
        .context("无法执行 buildah build")?;
    if !status.success() {
        bail!("运行时镜像构建失败（buildah 退出码 {:?}）", status.code());
    }
    Ok(())
}

/// 从本机 buildah 存储导出镜像为 tar.gz（供多节点分发）。返回（工作目录，tar 路径）。
async fn export_image_tar(image: &str) -> Result<(PathBuf, String)> {
    let workdir = make_workdir("image-export")?;
    let tar = workdir.join("image.tar.gz").to_string_lossy().into_owned();
    info!("导出镜像 {image} 为 tar.gz...");
    let result = run(
        "sh",
        &[
            "-c",
            &format!(
                "buildah push '{image}' 'docker-archive:/dev/stdout:{image}' | gzip -1 > '{tar}'"
            ),
        ],
    )
    .await;
    if let Err(e) = result {
        let _ = std::fs::remove_dir_all(&workdir);
        return Err(e);
    }
    Ok((workdir, tar))
}

/// K3s 兜底路径：源码构建 + 导出 + 导入本机 containerd。
async fn build_runtime_image_from_source(image: &str) -> Result<()> {
    build_image_locally(image).await?;
    let (workdir, tar) = export_image_tar(image).await?;
    let result = run("k3s", &["ctr", "-n", "k8s.io", "images", "import", &tar]).await;
    let _ = std::fs::remove_dir_all(&workdir);
    result?;
    info!("运行时镜像已导入集群: {image}");
    Ok(())
}

/// 内部密钥（PostgreSQL/Redis/内部签名）安装时随机生成：预渲染产物不带
/// Secret（secrets.create=false，避免每次 apply 轮换密码），这里幂等执行
/// init-secrets 脚本，已存在的密钥（含带外写入的平台凭证）绝不覆盖。
async fn ensure_internal_secrets() -> Result<()> {
    let script = repo_root().join("deploy/scripts/init-secrets.sh");
    if !script.is_file() {
        bail!("密钥初始化脚本缺失: {}", script.display());
    }
    let script = script.to_string_lossy().into_owned();
    info!("初始化内部密钥（幂等，已有值不覆盖）");
    run("bash", &[script.as_str()]).await
}

/// 投递方式：预渲染清单 kubectl apply（引导链零 helm 依赖，命门链路最稳），
/// 或 helm upgrade --install（release 可被 ArgoCD 等 GitOps 工具链接管）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    Apply,
    Helm,
}

/// 探测投递方式。规则按"既有管理状态优先、不中途换轨"设计：
/// - 已有同名 helm release → helm upgrade（release 生命周期不能被 apply 接管）；
/// - 工作负载已存在但无 release → 保持 apply（helm install 会撞已存在资源）；
/// - 绿地 + 复用的既有集群 → helm install（release 可被 GitOps 接管，获得升级
///   回滚管理）；本机没有 helm 就自动装（CN 走国内镜像，见 ensure_helm），
///   装不上才回落 apply；
/// - 元启动自建集群 → 预渲染清单 apply（命门链路零额外下载，最稳）。
async fn detect_delivery(cluster_existed: bool) -> Delivery {
    let helm = command_exists("helm").await;
    if helm && helm_release_exists().await {
        info!("投递探测: 检测到既有 helm release cogneva → helm upgrade（保持 release 管理）");
        return Delivery::Helm;
    }
    if cogneva_workload_exists().await {
        info!(
            "投递探测: 工作负载已由 apply 部署且无 helm release → 保持 apply（避免资源归属冲突）"
        );
        return Delivery::Apply;
    }
    if cluster_existed {
        if helm || ensure_helm().await {
            info!("投递探测: 复用既有集群 → helm install（release 可被 GitOps 接管）");
            return Delivery::Helm;
        }
        info!("投递探测: 复用既有集群但 helm 不可用 → 预渲染清单 apply（引导链零 helm 依赖）");
        return Delivery::Apply;
    }
    info!("投递探测: 元启动自建集群 → 预渲染清单 apply（命门链路零额外依赖）");
    Delivery::Apply
}

/// 确保 helm 客户端可用。仅在"复用既有集群、绿地部署、需要 helm 投递"时调用——
/// 元启动自建集群的命门链路永远走预渲染 apply，不下载 helm。
/// 下载候选：CN 首选华为云 helm 镜像（get.helm.sh 背后是 GitHub releases，
/// CN 直连不稳），海外首选 get.helm.sh；逐候选尝试，全失败返回 false 由调用方回落。
async fn ensure_helm() -> bool {
    if command_exists("helm").await {
        return true;
    }
    // 钉版本：与本机实测一致的 v4 稳定版；helm 3/4 包内布局相同（linux-<arch>/helm）。
    let version = "v4.2.4";
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => {
            warn!("helm 自动安装不支持架构 {other}，回落 apply");
            return false;
        }
    };
    let huawei = format!(
        "https://mirrors.huaweicloud.com/helm/{version}/helm-{version}-linux-{arch}.tar.gz"
    );
    let official = format!("https://get.helm.sh/helm-{version}-linux-{arch}.tar.gz");
    let candidates = if cn_mirror() {
        [huawei, official]
    } else {
        [official, huawei]
    };
    info!("未检测到 helm，自动安装（多候选，失败自动换下一个）...");
    for url in candidates {
        let script = format!(
            "set -e; d=$(mktemp -d); trap 'rm -rf \"$d\"' EXIT; cd \"$d\" && \
             curl -fsSL --connect-timeout 10 '{url}' -o helm.tgz && \
             tar -xzf helm.tgz && install -m 0755 linux-{arch}/helm /usr/local/bin/helm"
        );
        match Command::new("sh").args(["-c", &script]).status().await {
            Ok(s) if s.success() => {
                info!("helm 安装完成（来源 {url}）");
                return true;
            }
            _ => warn!("helm 下载/安装失败，换下一个候选: {url}"),
        }
    }
    warn!("helm 自动安装失败（所有候选不可达），回落预渲染清单 apply");
    false
}

/// 集群里是否已有同名 helm release（helm 3，release 存为集群内 Secret）。
async fn helm_release_exists() -> bool {
    let out = Command::new("helm")
        .args(["list", "-n", "cogneva", "-q"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await;
    matches!(out, Ok(o) if o.status.success()
        && String::from_utf8_lossy(&o.stdout).lines().any(|l| l.trim() == "cogneva"))
}

/// 工作负载是否已存在（用于判定"此前由 apply 部署"，helm release 检测先于此）。
async fn cogneva_workload_exists() -> bool {
    Command::new("kubectl")
        .args(["-n", "cogneva", "get", "deployment", "cogneva"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn deploy_manifests(cluster_existed: bool) -> Result<()> {
    let profile = detect_profile().await?;
    match detect_delivery(cluster_existed).await {
        Delivery::Apply => deploy_via_apply(profile).await,
        Delivery::Helm => deploy_via_helm(profile).await,
    }
}

async fn deploy_via_apply(profile: Profile) -> Result<()> {
    let dir = rendered_manifest_dir(profile);
    if !dir.is_dir() {
        bail!(
            "渲染产物目录不存在: {}（源码不完整？请重新获取仓库）",
            dir.display()
        );
    }
    ensure_internal_secrets().await?;
    let rendered = render_manifests_for_cluster(&dir).await?;
    info!(
        "apply {} profile 清单（已按网络环境适配）",
        profile.dir_name()
    );
    // kubectl apply -f <dir> 按文件名字典序逐个处理，namespace.yaml 排在
    // configmap/deployment 等之后，空白集群首轮会整批 namespace not found；
    // 先幂等建命名空间再整目录 apply（K3s 多节点分支的镜像分发器也做过，幂等无害）
    run(
        "sh",
        &[
            "-c",
            "kubectl create namespace cogneva --dry-run=client -o yaml | kubectl apply -f -",
        ],
    )
    .await?;
    run("kubectl", &["apply", "-f", &rendered.to_string_lossy()]).await
}

/// helm 投递：chart + 同一套 profile values。profile 为渲染 apply 固化了
/// secrets.create=false，这里改回 true——helm install 时 lookup+randAlphaNum
/// 安装时生成密钥（升级复用既有 Secret，不轮换），无需 init-secrets.sh。
/// CN 网络用 --post-renderer 复用与 apply 路径同一张镜像/seed 替换表。
async fn deploy_via_helm(profile: Profile) -> Result<()> {
    let chart = repo_root().join("deploy/helm/cogneva");
    let values = repo_root().join(format!(
        "deploy/helm/cogneva/profiles/{}.yaml",
        profile.dir_name()
    ));
    if !chart.is_dir() || !values.is_file() {
        bail!(
            "Helm chart 或 profile values 缺失（源码不完整？请重新获取仓库）: {} / {}",
            chart.display(),
            values.display()
        );
    }
    let mut args: Vec<String> = vec![
        "upgrade".into(),
        "--install".into(),
        "cogneva".into(),
        chart.to_string_lossy().into_owned(),
        "-n".into(),
        "cogneva".into(),
        "--create-namespace".into(),
        "-f".into(),
        values.to_string_lossy().into_owned(),
        "--set".into(),
        "secrets.create=true".into(),
    ];
    if cn_mirror() {
        // CN 适配走 values 覆盖（镜像站前缀 + seed 地址），不依赖 helm
        // 版本相关的 post-renderer 机制，release 元数据完整保留。
        for (key, val) in cn_helm_value_overrides(docker_mirror_host().await)? {
            args.push("--set".into());
            args.push(format!("{key}={val}"));
        }
    }
    info!(
        "helm 投递 {} profile（upgrade --install，幂等）",
        profile.dir_name()
    );
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    run("helm", &refs).await
}

/// 公开镜像引用加国内镜像站前缀，规则与 K3s containerd registries / apply 路径
/// 的镜像站前缀同源：docker hub 官方镜像（无 `/`）补 `library/`，docker hub
/// 用户镜像（首段无 `.`）直接加前缀，quay.io 走南大 quay 站（daocloud 系未收录）。
fn cn_mirror_image(image: &str, mirror: &str) -> String {
    if let Some(rest) = image.strip_prefix("quay.io/") {
        return format!("quay.nju.edu.cn/{rest}");
    }
    if image.contains('/') {
        format!("{mirror}/{image}")
    } else {
        format!("{mirror}/library/{image}")
    }
}

/// CN 网络下的 helm values 覆盖：镜像 tag 直接从 chart values.yaml 读取再改
/// 前缀（不硬编码 tag，chart 升版不漂移），seed 地址改 Gitee——与
/// render_manifests_for_cluster 的文本替换同义，两条投递路径网络适配一致。
fn cn_helm_value_overrides(mirror: &str) -> Result<Vec<(String, String)>> {
    let values_path = repo_root().join("deploy/helm/cogneva/values.yaml");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&std::fs::read_to_string(&values_path)?)?;
    let image_at = |path: &[&str]| -> Result<String> {
        let mut cur = &parsed;
        for k in path {
            cur = cur
                .get(k)
                .with_context(|| format!("values.yaml 缺字段 {}", path.join(".")))?;
        }
        cur.as_str()
            .map(String::from)
            .with_context(|| format!("values.yaml 字段 {} 非字符串", path.join(".")))
    };
    let mut out = Vec::new();
    for (key, path) in [
        (
            "backends.postgres.image",
            &["backends", "postgres", "image"][..],
        ),
        ("backends.redis.image", &["backends", "redis", "image"][..]),
        (
            "backends.qdrant.image",
            &["backends", "qdrant", "image"][..],
        ),
        ("backends.nats.image", &["backends", "nats", "image"][..]),
        ("buildah.image", &["buildah", "image"][..]),
        ("clusterRegistry.image", &["clusterRegistry", "image"][..]),
    ] {
        out.push((key.to_string(), cn_mirror_image(&image_at(path)?, mirror)));
    }
    out.push((
        "evolution.gitRemote.seedUrl".into(),
        "https://gitee.com/hcipengm/cogneva.git".into(),
    ));
    Ok(out)
}

/// 按运行网络环境处理预渲染 profile 产物副本。环境差异（containerd socket、
/// StorageClass、git-remote hostPath/PVC、ingress class）已在 CI 渲染时固化
/// 进各 profile，这里只做与网络可达性相关的替换：
/// - CN 模式 → 公开镜像加国内镜像站前缀（Docker Hub 被墙）；
/// - CN 模式 → git-remote seed 地址改用 Gitee（GitHub 拉取受限）。
///
/// 返回处理后的目录（kubectl apply 后即弃）。
async fn render_manifests_for_cluster(dir: &Path) -> Result<PathBuf> {
    let cn = cn_mirror();
    let out = make_workdir("manifests")?;
    let image_map = if cn {
        cn_image_map(docker_mirror_host().await)
    } else {
        Vec::new()
    };
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let mut text = std::fs::read_to_string(&path)?;
        for (from, to) in &image_map {
            text = text.replace(from.as_str(), to.as_str());
        }
        if cn {
            // profile 产物烘焙的是 GitHub seed 地址；CN 网络改用 Gitee。
            text = text.replace(
                "https://github.com/hcipengm/cogneva.git",
                "https://gitee.com/hcipengm/cogneva.git",
            );
        }
        std::fs::write(out.join(entry.file_name()), text)?;
    }
    Ok(out)
}

/// CN 模式公开镜像替换表：按探活选定的 docker 镜像站生成前缀，
/// 单站故障时下次安装自动换站（清单内嵌完整主机名，不走 containerd 回退）。
fn cn_image_map(mirror: &str) -> Vec<(String, String)> {
    [
        ("mysql:8.0", "library/mysql:8.0"),
        ("nats:2.10-alpine", "library/nats:2.10-alpine"),
        ("postgres:16-alpine", "library/postgres:16-alpine"),
        ("redis:7-alpine", "library/redis:7-alpine"),
        ("registry:2", "library/registry:2"),
    ]
    .into_iter()
    .map(|(from, to)| (format!("image: {from}"), format!("image: {mirror}/{to}")))
    .chain([
        (
            "image: qdrant/qdrant:".to_string(),
            format!("image: {mirror}/qdrant/qdrant:"),
        ),
        (
            // daocloud 系镜像站的 quay 镜像对 buildah/stable 返回 401/403
            //（未收录该仓库），改用南大 quay 镜像站
            "image: quay.io/buildah/stable:".to_string(),
            "image: quay.nju.edu.cn/buildah/stable:".to_string(),
        ),
    ])
    .collect()
}

async fn wait_ready() -> Result<()> {
    for deploy in ["cogneva", "cogneva-security-gateway"] {
        let status = Command::new("kubectl")
            .args([
                "-n",
                "cogneva",
                "rollout",
                "status",
                &format!("deployment/{deploy}"),
                "--timeout=180s",
            ])
            .status()
            .await?;
        if !status.success() {
            warn!("deployment/{deploy} 未在超时内 Ready，请人工检查");
        }
    }
    Ok(())
}

/// 三段语义化版本比较：a < b 返回 true；段数不齐补 0，非数字段按 0。
fn version_lt(a: &str, b: &str) -> bool {
    let parse = |s: &str| -> Vec<u64> {
        s.split('.')
            .map(|p| {
                p.chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .parse()
                    .unwrap_or(0)
            })
            .collect()
    };
    let (va, vb) = (parse(a), parse(b));
    for i in 0..va.len().max(vb.len()) {
        let x = va.get(i).copied().unwrap_or(0);
        let y = vb.get(i).copied().unwrap_or(0);
        if x != y {
            return x < y;
        }
    }
    false
}

/// 装机完成后最佳努力检测官方是否已发布更新版本。只提示、绝不自动升级；
/// 离线、超时、解析失败一律静默返回，不阻塞命门装机链路。
/// （标准 `curl .../main/bootstrap.sh` 用户装的就是 main 最新，通常不触发；
///  主要服务用了旧离线介质 / 旧脚本安装的场景。）
async fn maybe_warn_outdated() {
    let current = env!("CARGO_PKG_VERSION");
    let Ok(client) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    else {
        return;
    };
    // CN 首选 Gitee（国内可达），GitHub API 兜底；拿到任一有效响应即止。
    let endpoints = if cn_mirror() {
        [
            "https://gitee.com/api/v5/repos/hcipengm/cogneva/releases/latest",
            "https://api.github.com/repos/hcipengm/cogneva/releases/latest",
        ]
    } else {
        [
            "https://api.github.com/repos/hcipengm/cogneva/releases/latest",
            "https://gitee.com/api/v5/repos/hcipengm/cogneva/releases/latest",
        ]
    };
    for url in endpoints {
        let Ok(resp) = client.get(url).send().await else {
            continue;
        };
        let Ok(body) = resp.text().await else {
            continue;
        };
        let Some(tag) = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| {
                v.get("tag_name")
                    .and_then(|t| t.as_str())
                    .map(str::to_string)
            })
        else {
            continue;
        };
        let latest = tag.trim_start_matches('v').trim();
        if version_lt(current, latest) {
            let page = if cn_mirror() {
                "https://gitee.com/hcipengm/cogneva/releases"
            } else {
                "https://github.com/hcipengm/cogneva/releases"
            };
            info!(
                "检测到新版本 v{latest}（当前安装 v{current}）。更新说明与镜像包见发布页：{page}"
            );
        }
        return;
    }
}

/// 尝试用系统默认浏览器打开 WebUI（2.5.6）；失败仅告警，不影响自毁退出。
async fn open_browser(url: &str) {
    let opener = if cfg!(target_os = "macos") {
        Some("open")
    } else if cfg!(target_os = "windows") {
        Some("rundll32")
    } else {
        Some("xdg-open")
    };
    let Some(opener) = opener else { return };
    let args: Vec<&str> = if opener == "rundll32" {
        vec!["url.dll,FileProtocolHandler", url]
    } else {
        vec![url]
    };
    match Command::new(opener)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(_) => info!("已在默认浏览器打开 {url}"),
        Err(e) => warn!("自动打开浏览器失败（{e}），请手动访问 {url}"),
    }
}

/// 幂等后台 port-forward。Lima/WSL2 场景靠它把 svc 暴露到 VM 网络
/// （--address 0.0.0.0），再经 Lima portForwards / WSL localhostForwarding
/// 到达宿主浏览器；裸 Linux 上则直接对外提供 WebUI 入口。
/// 端口已在监听则跳过；失败仅告警并打印手动命令，不影响自毁退出。
async fn ensure_port_forward(webui: &str) {
    let port = webui
        .rsplit(':')
        .next()
        .and_then(|s| s.trim_end_matches('/').parse::<u16>().ok())
        .unwrap_or(8080);
    if tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_ok()
    {
        info!("端口 {port} 已在监听，跳过 port-forward");
        return;
    }
    let spec = format!("{port}:8080");
    match Command::new("kubectl")
        .args([
            "-n",
            "cogneva",
            "port-forward",
            "--address",
            "0.0.0.0",
            "svc/cogneva",
            &spec,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(_) => info!("已后台建立 port-forward: 0.0.0.0:{port} → svc/cogneva:8080"),
        Err(e) => warn!(
            "自动 port-forward 失败（{e}），请手动执行: \
             kubectl -n cogneva port-forward --address 0.0.0.0 svc/cogneva {spec}"
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let noninteractive = std::env::var("COGNEVA_BOOTSTRAP_NONINTERACTIVE")
        .ok()
        .as_deref()
        == Some("1");

    info!("== Cogneva 元启动引导器 ==");

    #[cfg(not(target_os = "linux"))]
    warn!(
        "引导器需在 Linux 运行层内执行，当前为 {}。\
         macOS 请改用 bootstrap.sh（自动经 Lima 虚拟机），Windows 请改用 bootstrap.ps1（自动经 WSL2）",
        std::env::consts::OS
    );

    info!("LLM 接入不在引导器做：部署完成后由 WebUI 强制向导完成（全自动零问答）");

    let hw = probe_hardware().await;
    info!(
        "硬件探测: {} 核 / {} MiB / {} / {} 节点",
        hw.cpu_cores, hw.mem_total_mb, hw.arch, hw.nodes
    );
    let decision = decide_provision(&hw).await;
    info!(
        "供给决策: distro={:?} 多节点={} 回落={:?}",
        decision.distro, decision.multi, decision.fallback_reason
    );

    let intent = IntentConfig {
        distro: decision.distro,
        multi: decision.multi,
        fallback_reason: decision.fallback_reason.clone(),
        hardware: hw.clone(),
    };
    let intent_path =
        std::env::var("COGNEVA_INTENT_CONFIG").unwrap_or_else(|_| "intent_config.yaml".into());
    std::fs::write(&intent_path, serde_yaml::to_string(&intent)?)?;
    info!("已生成 {intent_path}");

    // k8m 管理计划（审计 2.5.2/2.5.3）：统一声明式部署计划 + backend 自动选择。
    let plan = cogneva_bootstrap::ManagementPlan::for_environment(
        std::env::var("COGNEVA_ENV").unwrap_or_else(|_| "default".into()),
        &cogneva_bootstrap::HardwareProfile {
            memory_gb: (hw.mem_total_mb / 1024).max(1),
            cpu_cores: hw.cpu_cores as u32,
            nodes: hw.nodes as u32,
        },
        decision.distro,
    );
    let plan_path =
        std::env::var("COGNEVA_MANAGEMENT_PLAN").unwrap_or_else(|_| "management_plan.yaml".into());
    std::fs::write(&plan_path, plan.to_yaml()?)?;
    info!("已生成 {plan_path}（backend 自动选择已内嵌）");

    // 投递方式探测需要知道集群是"本次安装"还是"复用既有"：安装前取样。
    let cluster_existed = cluster_ready().await;
    match decision.distro {
        // K3s：单节点本机装 server；多节点 server + agents。
        Distro::K3s if !decision.multi => install_k3s().await?,
        Distro::K3s => ensure_multi_node_cluster().await?,
        // kubespray：跑官方镜像新建标准 Kubernetes（本机为控制面，声明节点作 worker）。
        Distro::Kubespray => kubespray::run_kubespray(&cluster_nodes_env()).await?,
    }
    ensure_buildah().await?;
    ensure_buildah_mirror().await?;
    ensure_firecracker().await?;
    if probe_nodes().await > 1 {
        // 多节点：git-remote 走集群卷（渲染时选 PVC 变体），宿主 bare 仓库不再使用
        info!("多节点集群：git-remote 走集群卷，跳过宿主 bare 仓库 seed");
    } else {
        ensure_git_remote().await?;
    }
    ensure_runtime_image(decision.distro, decision.multi).await?;
    deploy_manifests(cluster_existed).await?;
    // 清单 apply 后 registry 才存在；基镜像播种失败不阻断安装（best-effort）
    seed_cluster_registry().await?;
    wait_ready().await?;

    let webui =
        std::env::var("COGNEVA_WEBUI_URL").unwrap_or_else(|_| "http://localhost:8080".into());
    ensure_port_forward(&webui).await;
    info!("部署完成，WebUI 地址: {webui}");
    maybe_warn_outdated().await;
    if !noninteractive {
        open_browser(&webui).await;
    }

    info!("引导器使命完成，退出");
    Ok(())
}

#[cfg(test)]
mod profile_tests {
    use super::Profile;

    #[test]
    fn profile_dir_names_match_rendered_tree() {
        // 目录名必须与 deploy/scripts/render-deploy.sh 的 PROFILES 一一对应。
        assert_eq!(Profile::K3sSingle.dir_name(), "k3s-single");
        assert_eq!(Profile::K3sMulti.dir_name(), "k3s-multi");
        assert_eq!(Profile::K8sStandard.dir_name(), "k8s-standard");
    }

    #[test]
    fn version_lt_compares_semver_segments() {
        assert!(super::version_lt("0.5.7", "0.5.8"));
        assert!(super::version_lt("0.5.7", "0.6.0"));
        assert!(super::version_lt("0.5.7", "1.0.0"));
        assert!(!super::version_lt("0.5.7", "0.5.7"));
        assert!(!super::version_lt("0.5.8", "0.5.7"));
        // 段数不齐补 0、非数字后缀按 0 处理
        assert!(super::version_lt("0.5", "0.5.1"));
        assert!(super::version_lt("0.5.7", "0.6.0-rc1"));
    }

    #[test]
    fn cn_mirror_image_rewrite_rules() {
        let m = "docker.m.daocloud.io";
        // docker hub 官方镜像补 library/
        assert_eq!(
            super::cn_mirror_image("postgres:16-alpine", m),
            "docker.m.daocloud.io/library/postgres:16-alpine"
        );
        // docker hub 用户镜像（首段无点）直接加前缀，tag 原样保留
        assert_eq!(
            super::cn_mirror_image("qdrant/qdrant:v1.13.4", m),
            "docker.m.daocloud.io/qdrant/qdrant:v1.13.4"
        );
        // quay.io 固定走南大 quay 站（daocloud 系未收录 buildah）
        assert_eq!(
            super::cn_mirror_image("quay.io/buildah/stable:latest", m),
            "quay.nju.edu.cn/buildah/stable:latest"
        );
    }
}
