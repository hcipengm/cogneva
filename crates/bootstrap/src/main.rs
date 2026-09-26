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
//!    DaemonSet 逐节点分发），再把节点本地 localhost/cogneva:local 播种进集群
//!    内 registry——清单统一 pin localhost:30500/cogneva:local；
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

/// 把二进制内嵌的部署资产解包到一次性工作目录，并将 COGNEVA_REPO_ROOT 指向它。
/// 预编译引导器没有源码树，但 apply/helm 两条投递路径与密钥初始化都按
/// repo_root()/deploy/... 读盘——解包后这些路径全部可用，磁盘读取逻辑零改动。
fn materialize_assets() -> Result<PathBuf> {
    let dir = make_workdir("assets")?;
    for (rel, data) in embedded_assets::EMBEDDED_ASSETS {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, data)?;
        #[cfg(unix)]
        if rel.ends_with(".sh") {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
        }
    }
    Ok(dir)
}

use anyhow::{bail, Context, Result};
use cogneva_bootstrap::{cli, Distro};
use download::{curl_to_file, curl_to_string, pick_alive, probe as probe_alive, LARGE, MANDATORY};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::{info, warn};

mod apt;
mod download;
mod kubespray;
mod privileges;

/// 构建期内嵌的部署资产（预渲染清单 / init-secrets 脚本 / helm chart），
/// 由 build.rs 从 deploy/ 打包生成。
mod embedded_assets {
    include!(concat!(env!("OUT_DIR"), "/bootstrap_assets.rs"));
}

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

/// K3s 安装脚本地址：受限网络下 get.k3s.io 背后的 GitHub releases 必挂，
/// 走 rancher 国内镜像站。
const K3S_INSTALL_SH_CN: &str = "https://rancher-mirror.rancher.cn/k3s/k3s-install.sh";
const K3S_INSTALL_SH_INTL: &str = "https://get.k3s.io";

/// 下载 K3s 安装脚本到本地（带总超时）后执行。
///
/// 不用 `curl ... | sh -`：管道左侧的失败在 POSIX sh 里看不见（没有 pipefail 时
/// 退出码取的是右侧），curl 超时会被表现成 sh 的语法错误或"脚本里什么都没做"，
/// 报错指向完全无关的地方。先落盘再执行，失败点才落在真正失败的那一步。
async fn run_k3s_install_script(env: &str) -> Result<()> {
    let url = if cn_mirror() {
        K3S_INSTALL_SH_CN
    } else {
        K3S_INSTALL_SH_INTL
    };
    let dir = make_workdir("k3s-install")?;
    let script_path = dir.join("k3s-install.sh");
    curl_to_file(url, &script_path, MANDATORY)
        .await
        .with_context(|| format!("下载 K3s 安装脚本失败: {url}"))?;
    let script = script_path.to_string_lossy().into_owned();
    let cmd = if env.is_empty() {
        format!("sh '{script}'")
    } else {
        format!("{env} sh '{script}'")
    };
    let mut child = Command::new("sh");
    child.arg("-c").arg(&cmd);
    if !env.is_empty() {
        // 环境前缀已写进命令行，这里只是把同一份环境给 sh 本身，
        // 让安装脚本内的子进程继承（K3S_URL/K3S_TOKEN 走子进程读取）
        for kv in env.split_whitespace() {
            if let Some((k, v)) = kv.split_once('=') {
                child.env(k, v);
            }
        }
    }
    let status = child.status().await?;
    let _ = std::fs::remove_dir_all(&dir);
    if !status.success() {
        bail!("K3s 安装脚本退出码 {:?}", status.code());
    }
    Ok(())
}

/// K3s reads both the server's and an agent's settings from this file; it must
/// exist before the first start of the service.
const K3S_CONFIG_PATH: &str = "/etc/rancher/k3s/config.yaml";

/// Write the node's memory QoS settings where K3s will read them.
///
/// Never overwrites: a config file on the node is an operator's decision, and
/// this one file holds every kubelet argument -- taking it over would silently
/// discard whatever else is in it. The reservation and the eviction threshold
/// are derived from this node's own readings, so the same code produces the
/// right numbers on a small node and a large one.
fn write_k3s_qos_config(mem_total_mb: u64, cpu_cores: usize) -> Result<()> {
    let path = Path::new(K3S_CONFIG_PATH);
    if path.exists() {
        warn!(
            "{K3S_CONFIG_PATH} 已存在，保留不覆盖：请自行确认其中含节点内存预留与 \
             memory.available 驱逐阈值（本次未按 {mem_total_mb}MiB / {cpu_cores} 核推导的值写入）"
        );
        return Ok(());
    }
    std::fs::create_dir_all("/etc/rancher/k3s")?;
    let qos = cogneva_bootstrap::node_qos(mem_total_mb, cpu_cores as u32);
    std::fs::write(path, cogneva_bootstrap::k3s_qos_config_yaml(&qos))?;
    info!(
        "已预置节点内存 QoS（非 Pod 预留 cpu={}m / memory={}MiB，memory.available<{}MiB 触发驱逐）→ {K3S_CONFIG_PATH}",
        qos.reserved_cpu_milli, qos.reserved_memory_mb, qos.eviction_memory_mb
    );
    Ok(())
}

/// Report a cluster that is already running without the node QoS settings.
///
/// The settings are read once, at K3s' first start, so a cluster that was
/// provisioned before this file existed keeps running with `allocatable ==
/// capacity` and no memory eviction threshold -- nothing on the node stops a
/// pod from taking the memory the host itself needs. Naming the gap is all
/// this path can do: picking it up needs the service restarted.
fn warn_if_qos_missing() {
    if Path::new(K3S_CONFIG_PATH).exists() {
        return;
    }
    warn!(
        "现有集群从未写过 {K3S_CONFIG_PATH}：节点没有内存预留，也没有 memory.available \
         驱逐阈值（allocatable 等于 capacity，即容器可以吃掉整机内存，内核只能换页）。\
         写入后需重启 K3s 才生效，这一步不在元启动里自动做"
    );
}

async fn install_k3s(hw: &Hardware) -> Result<()> {
    if cluster_ready().await {
        info!("检测到可用集群，跳过 K3s 安装");
        warn_if_qos_missing();
        return Ok(());
    }
    info!("安装 K3s（官方脚本）...");
    if cn_mirror() {
        write_k3s_registries_cn()?;
    }
    write_k3s_qos_config(hw.mem_total_mb, hw.cpu_cores)?;
    let env = if cn_mirror() {
        "INSTALL_K3S_MIRROR=cn"
    } else {
        ""
    };
    run_k3s_install_script(env).await?;
    // 等待 kubeconfig 就绪
    for _ in 0..30 {
        if cluster_ready().await {
            write_kubeconfig()?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    bail!("K3s 安装后集群未就绪");
}

/// 把 K3s 生成的 kubeconfig 接到标准位置。
///
/// K3s 只写 /etc/rancher/k3s/k3s.yaml，而 helm / kubectl 默认读 $KUBECONFIG 或
/// ~/.kube/config。缺这一步时 helm 在 K3s 上根本连不上集群——错误信息是
/// "Kubernetes cluster unreachable"，看起来像集群没起来，实际只是没人告诉 helm
/// 去哪找 kubeconfig。这里落一份标准位置的副本（权限 0600，kubeconfig 等价于
/// 集群管理员凭证），k3s 的原始文件保持不动。
fn write_kubeconfig() -> Result<()> {
    let src = Path::new("/etc/rancher/k3s/k3s.yaml");
    if !src.is_file() {
        return Ok(());
    }
    if std::env::var("KUBECONFIG").is_ok() {
        info!("KUBECONFIG 已由环境显式指定，跳过 kubeconfig 落位");
        return Ok(());
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| "/root".into());
    let dir = PathBuf::from(home).join(".kube");
    let dst = dir.join("config");
    if dst.is_file() {
        return Ok(());
    }
    std::fs::create_dir_all(&dir)?;
    std::fs::copy(src, &dst).context("复制 kubeconfig 到 ~/.kube/config 失败")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o600))?;
    }
    std::env::set_var("KUBECONFIG", &dst);
    info!("kubeconfig 已落位: {}", dst.display());
    Ok(())
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
async fn ensure_multi_node_cluster(hw: &Hardware) -> Result<()> {
    let agents = cluster_nodes_env();
    if !cluster_ready().await {
        if agents.is_empty() {
            bail!(
                "K3s 多节点分支需要多节点集群：请用 COGNEVA_CLUSTER_NODES=user@ip[,user@ip2...] \
                 声明工作节点（本机将作为 server，需 SSH 免密可达），或预先搭建集群"
            );
        }
        install_k3s(hw).await?;
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
    // 远端也先落盘再执行：`curl | sh` 的管道左侧失败在目标机上不可见（无 pipefail
    // 时退出码取右侧），下载超时会被表现成"脚本什么都没做"。K3S_URL/K3S_TOKEN
    // 必须作为执行脚本时的环境变量传入——过去写成管道右侧的前缀，脚本收不到会装成
    // 独立 server（脑裂），2026-08-04 嵌套实测抓到：目标机起了 k3s.service 而非
    // k3s-agent。
    let script_url = if cn_mirror() {
        K3S_INSTALL_SH_CN
    } else {
        K3S_INSTALL_SH_INTL
    };
    let mirror_env = if cn_mirror() {
        "INSTALL_K3S_MIRROR=cn "
    } else {
        ""
    };
    let k3s_env = format!("K3S_URL={server_url} K3S_TOKEN={token}{version_env}");
    let download = format!(
        "curl -fsSL --connect-timeout 15 --max-time 900 --retry 2 -o /tmp/cogneva-k3s-install.sh {script_url}"
    );
    let run_remote = format!("{mirror_env}{k3s_env} sh /tmp/cogneva-k3s-install.sh");
    let prep = if cn_mirror() {
        // agent 同样要在 k3s-agent 首启前预置 registries.yaml（pause 等系统镜像走 docker.io）
        let reg = k3s_registries_yaml()
            .replace('\n', "\\n")
            .replace('"', "\\\"");
        format!(
            "mkdir -p /etc/rancher/k3s && printf '{reg}' > /etc/rancher/k3s/registries.yaml && "
        )
    } else {
        String::new()
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
        // 每个 agent 按**它自己的**读数推导，而不是照抄 server 的：内存与核数
        // 逐节点不同，同一份数字会让小节点留得过多、大节点留得过少。
        let remote = match remote_qos_config(&ssh_target, port.as_ref()).await {
            Some(qos) => format!("{prep}{}{download} && {run_remote}", qos_remote_write(&qos)),
            None => {
                warn!(
                    "读不到 {target} 的内存 / 核数，该节点将不带内存预留与驱逐阈值启动：\
                     装完后按它自己的读数写 {K3S_CONFIG_PATH} 并重启 K3s"
                );
                format!("{prep}{download} && {run_remote}")
            }
        };
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
        args.push(remote);
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run("ssh", &arg_refs)
            .await
            .with_context(|| format!("agent 安装失败 {target}（需要本机到目标的 SSH 免密可达）"))?;
    }
    Ok(())
}

/// SSH into a node and return its stdout, in the same invocation shape the
/// install path uses (same options, so a node reachable there is reachable
/// here). None covers every way the probe can fail: the reading is optional.
async fn ssh_capture(ssh_target: &str, port: Option<&String>, command: &str) -> Option<String> {
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
        args.push(p.clone());
    }
    args.push(ssh_target.to_string());
    args.push(command.to_string());
    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let out = Command::new("ssh")
        .args(&arg_refs)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The QoS config an agent should start with, rendered from that node's own
/// memory and CPU count.
async fn remote_qos_config(ssh_target: &str, port: Option<&String>) -> Option<String> {
    // MemTotal is in kB; nproc is the CPU count the kubelet will see.
    let out = ssh_capture(
        ssh_target,
        port,
        "awk '/^MemTotal:/{print int($2/1024)}' /proc/meminfo; nproc",
    )
    .await?;
    let mut lines = out.lines().filter(|l| !l.trim().is_empty());
    let mem_total_mb: u64 = lines.next()?.trim().parse().ok()?;
    let cpu_cores: u32 = lines.next()?.trim().parse().ok()?;
    Some(cogneva_bootstrap::k3s_qos_config_yaml(
        &cogneva_bootstrap::node_qos(mem_total_mb, cpu_cores),
    ))
}

/// Write the config on the node before K3s' first start, keeping whatever file
/// is already there. A heredoc rather than printf: the thresholds carry `%`,
/// which printf would read as a conversion and eat.
fn qos_remote_write(yaml: &str) -> String {
    format!(
        "if [ ! -f {K3S_CONFIG_PATH} ]; then mkdir -p /etc/rancher/k3s && \
         cat > {K3S_CONFIG_PATH} <<'COGNEVA_QOS'\n{yaml}COGNEVA_QOS\nfi && "
    )
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
    // 预编译引导器路径不经过 bootstrap.sh 的 ensure_cc，需要自己把 apt 源换到
    // 国内镜像；已在 shell 层换过时这里按标记文件跳过（幂等）。
    apt::ensure_cn_apt_mirror(cn_mirror()).await?;
    run("apt-get", &["update"]).await?;
    run("apt-get", &["install", "-y", "buildah"]).await?;
    Ok(())
}

/// 预编译引导器不经 shell 装基础工具；自进化 bare 仓库 seed 与镜像源码回退
/// 构建都要 git。源码构建回退路径由 bootstrap.sh 的 ensure_cc 兜底，这里覆盖
/// 预编译路径：按包管理器自动安装，装不上报错提示手动安装。
async fn ensure_git() -> Result<()> {
    if command_exists("git").await {
        return Ok(());
    }
    info!("未检测到 git，尝试自动安装...");
    apt::ensure_cn_apt_mirror(cn_mirror()).await?;
    let managers: &[(&str, &[&str])] = &[
        ("apt-get", &["apt-get", "install", "-y", "git"]),
        ("dnf", &["dnf", "install", "-y", "git"]),
        ("yum", &["yum", "install", "-y", "git"]),
        ("apk", &["apk", "add", "git"]),
    ];
    for (mgr, args) in managers {
        if !command_exists(mgr).await {
            continue;
        }
        if *mgr == "apt-get" {
            run("apt-get", &["update"]).await.ok();
        }
        if run(args[0], &args[1..]).await.is_ok() && command_exists("git").await {
            info!("git 已安装");
            return Ok(());
        }
    }
    bail!("git 不可用且自动安装失败，请手动安装 git 后重新运行引导器");
}

/// 上游公开仓库的两个镜像。可达性按主机不对称——实测本集群内 github 间歇黑洞
/// 而 gitee 秒级应答——所以凡按序尝试的地方都从这一份取地址，不各写一份字面量。
const GIT_MIRROR_GITHUB: &str = "https://github.com/hcipengm/cogneva.git";
const GIT_MIRROR_GITEE: &str = "https://gitee.com/hcipengm/cogneva.git";

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
    // 地址与 chart evolution.gitRemote.seedUrls 同源；CN 走 Gitee，失败回落另一地址。
    let (primary, fallback) = if cn_mirror() {
        (GIT_MIRROR_GITEE, GIT_MIRROR_GITHUB)
    } else {
        (GIT_MIRROR_GITHUB, GIT_MIRROR_GITEE)
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
    // 可选组件：用 OPTIONAL 档（短总超时），拿不到就降级——它挂着 MicroVM 沙盒的
    // 形态，不挂整台机器的安装。过去这里没有总超时，墙前会一直不返回，
    // 装机就停在"安装 firecracker"这一行上。
    let dir = make_workdir("firecracker")?;
    let tgz = dir.join("firecracker.tgz");
    let outcome = async {
        curl_to_file(&url, &tgz, download::OPTIONAL).await?;
        run(
            "sh",
            &[
                "-c",
                &format!(
                    "tar -xzf '{tgz}' -C '{dir}' && install -m 0755 '{dir}/release-{version}-{arch}/firecracker-{version}-{arch}' /usr/local/bin/firecracker",
                    tgz = tgz.display(),
                    dir = dir.display(),
                ),
            ],
        )
        .await
    }
    .await;
    let _ = std::fs::remove_dir_all(&dir);
    match outcome {
        Ok(()) => {
            info!("firecracker 安装完成");
            Ok(())
        }
        Err(e) => {
            warn!("firecracker 安装失败（{e:#}）；沙盒保持 K8s Pod 形态（可稍后手动安装并启用 microvm）");
            Ok(())
        }
    }
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
         kubectl change sc <存储类名> -p '{{\"metadata\":{{\"annotations\":{{\
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
// 后续下载层的重试机制仍会兜底）。探活与候选选用的实现都在 download 模块，
// 与其它出网动作共用同一份超时纪律。

/// docker.io 镜像站候选（CN 模式）。
const DOCKER_MIRROR_CANDIDATES: &[&str] = &[
    "docker.m.daocloud.io",
    "docker.1ms.run",
    "docker.1panel.live",
    "hub.rat.dev",
];

/// registry.k8s.io 镜像站候选。只镜像 docker.io 覆盖不到它，而
/// kube-prometheus-stack 的 kube-state-metrics 正在这个 registry 上：
/// 受限网络里 chart 会卡在它的 ImagePullBackOff 直到 --wait 超时。
const K8S_IO_MIRROR_CANDIDATES: &[&str] =
    &["k8s-gcr.m.daocloud.io", "m.daocloud.io/registry.k8s.io"];

/// quay.io 镜像站候选。prometheus-operator 等控制面镜像在这个 registry 上。
const QUAY_IO_MIRROR_CANDIDATES: &[&str] = &["quay.m.daocloud.io", "quay.nju.edu.cn"];

static DOCKER_MIRROR: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();

/// 选定 docker.io 镜像站（全进程一次探测，后续复用结果）。
async fn docker_mirror_host() -> &'static str {
    DOCKER_MIRROR
        .get_or_init(|| async {
            for host in DOCKER_MIRROR_CANDIDATES {
                if probe_alive(&format!("https://{host}/v2/"), download::PROBE).await {
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
/// 无需探活；全部 endpoint 失败后 containerd 还会回源原 registry 直连。
///
/// 三个 registry 都要覆盖，不是只镜像 docker.io：系统镜像与 chart 依赖分别落在
/// registry.k8s.io 和 quay.io 上，漏掉哪个都会在受限网络里卡住 pull。
fn k3s_registries_yaml() -> String {
    let mut s = String::from("mirrors:\n");
    for (registry, candidates) in [
        ("docker.io", DOCKER_MIRROR_CANDIDATES),
        ("registry.k8s.io", K8S_IO_MIRROR_CANDIDATES),
        ("quay.io", QUAY_IO_MIRROR_CANDIDATES),
    ] {
        s.push_str(&format!("  {registry}:\n    endpoint:\n"));
        for h in candidates {
            s.push_str(&format!("      - \"https://{h}\"\n"));
        }
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

/// 运行时镜像供给：镜像先进入节点 containerd（localhost/cogneva:local），随后由
/// seed_cluster_registry 播种进集群内 registry——工作负载清单统一 pin
/// localhost:30500/cogneva:local，不直接引用节点本地镜像名。
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
        let part = workdir.join(format!("part-{suffix}"));
        let got = curl_to_file(&format!("{url}.part-{suffix}"), &part, LARGE).await;
        let part = part.to_string_lossy().into_owned();
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
        let tar_path = workdir.join(&name);
        let tar = tar_path.to_string_lossy().into_owned();
        info!("下载预构建镜像 {url} ...");
        let direct = curl_to_file(&url, &tar_path, LARGE).await;
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

/// 起镜像服务 Deployment → kubectl cp 注入 tar → DaemonSet 逐节点导入 → 清理。
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
        // 镜像服务是 Deployment（裸 Pod 被驱逐后无控制器重建，2026-09-14 实证
        // 导致分发器永久 CrashLoop）；等 Available 后按 label 解析当前 Pod 名
        // 再 cp。超时给足 10 分钟：空白机首拉 busybox 镜像（经镜像站）可能
        // 远超 2 分钟，超时不等于失败
        run(
            "kubectl",
            &[
                "-n", "cogneva", "wait", "--for=condition=Available",
                "deployment/cogneva-image-server", "--timeout=600s",
            ],
        )
        .await?;
        let cp_cmd = format!(
            "POD=$(kubectl -n cogneva get pod -l app=cogneva-image-server \
             -o jsonpath='{{.items[0].metadata.name}}') && \
             kubectl -n cogneva cp '{tar}' \"$POD\":/share/image.tar.gz"
        );
        run("sh", &["-c", &cp_cmd]).await?;
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

/// 把基镜像 localhost/cogneva:local 播种进集群内 registry：四部署清单统一
/// pin localhost:30500/cogneva:local，自进化金丝雀/mainline overlay 也 FROM
/// 该基镜像，缺失则工作负载只能 ImagePullBackOff。经宿主 containerd 客户端
/// 直推 NodePort（localhost http 免 TLS，多节点每节点都通）。
/// 失败是硬失败：清单 pin 已在 registry，播种不成功四部署拉不到镜像；
/// 给足重试，仍失败则让引导器带着明确错误退出（可修复后重跑，幂等）。
async fn seed_cluster_registry() -> Result<()> {
    run(
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
    .await
    .context("等待集群内 registry 就绪超时")?;
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
                bail!(
                    "registry 基镜像播种三次失败: {e:#}。四部署 pin {remote}，\
                     可在节点上手动执行 ctr -n k8s.io images push --plain-http {remote} {object} \
                     后重跑引导器"
                );
            }
        }
    }
    Ok(())
}

/// registry 播种发生在清单 apply 之后：四部署首轮拉取 :local 时镜像可能还没进
/// registry，kubelet 已进入 ImagePullBackOff 退避（最长数分钟）。播种成功后
/// 删除卡在镜像拉取失败状态的 Pod，让 ReplicaSet 立即重建并同步拉取，不等退避。
async fn kick_image_pull_pending() -> Result<()> {
    let output = Command::new("kubectl")
        .args(["-n", "cogneva", "get", "pods", "-o", "json"])
        .output()
        .await?;
    if !output.status.success() {
        warn!("查询 Pod 列表失败，跳过拉取失败 Pod 清理");
        return Ok(());
    }
    let body: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let pending: Vec<String> = body
        .get("items")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|p| {
                    let name = p.pointer("/metadata/name")?.as_str()?;
                    let stuck = p
                        .pointer("/status/containerStatuses")
                        .and_then(|v| v.as_array())
                        .is_some_and(|statuses| {
                            statuses.iter().any(|cs| {
                                matches!(
                                    cs.pointer("/state/waiting/reason").and_then(|r| r.as_str()),
                                    Some("ImagePullBackOff" | "ErrImagePull" | "InvalidImageName")
                                )
                            })
                        })
                        || p.pointer("/status/initContainerStatuses")
                            .and_then(|v| v.as_array())
                            .is_some_and(|statuses| {
                                statuses.iter().any(|cs| {
                                    matches!(
                                        cs.pointer("/state/waiting/reason")
                                            .and_then(|r| r.as_str()),
                                        Some(
                                            "ImagePullBackOff"
                                                | "ErrImagePull"
                                                | "InvalidImageName"
                                        )
                                    )
                                })
                            });
                    stuck.then_some(name.to_string())
                })
                .collect()
        })
        .unwrap_or_default();
    if pending.is_empty() {
        return Ok(());
    }
    for pod in &pending {
        info!("registry 已播种，删除卡在镜像拉取的 Pod {pod} 触发立即重拉");
        let _ = Command::new("kubectl")
            .args(["-n", "cogneva", "delete", "pod", pod, "--ignore-not-found"])
            .status()
            .await;
    }
    Ok(())
}

/// 取小文本（校验文件等）。走 curl 而不是 reqwest 裸调：reqwest 的默认客户端
/// **既没有总超时也没有连接超时**，一个不响应的地址能让它无限期挂着，而这条路径
/// 是取 `.sha256`，正好在防火墙最可能拦的位置上。
async fn download_string(url: &str) -> Result<String> {
    curl_to_string(url, MANDATORY).await
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

/// 镜像源码构建回退需要完整源码树（Dockerfile + 构建上下文）。预编译引导器
/// 解包出的 repo_root 只有部署资产，按"本机进化 bare 仓库 → 上游克隆"顺序取
/// 源码：单节点的 bare 仓库已从上游完整 clone（含全部历史），本地克隆秒级
/// 完成且不耗外网；多节点（git-remote 走集群卷）或 bare 缺失时直接克隆上游。
async fn ensure_source_tree() -> Result<PathBuf> {
    let root = repo_root();
    if root.join("Dockerfile").is_file() {
        return Ok(root);
    }
    let dir = make_workdir("src")?;
    let bare = Path::new("/var/lib/cogneva-data/git-remote");
    let upstream = if cn_mirror() {
        [GIT_MIRROR_GITEE, GIT_MIRROR_GITHUB]
    } else {
        [GIT_MIRROR_GITHUB, GIT_MIRROR_GITEE]
    };
    let mut attempts: Vec<Vec<&str>> = Vec::new();
    if bare.join("HEAD").exists() {
        attempts.push(vec!["clone", bare.to_str().unwrap(), dir.to_str().unwrap()]);
    }
    for url in upstream {
        attempts.push(vec!["clone", "--depth", "1", url, dir.to_str().unwrap()]);
    }
    for args in &attempts {
        std::fs::remove_dir_all(&dir).ok();
        match run("git", args).await {
            Ok(()) => {
                info!("镜像源码回退树已就绪: {}", dir.display());
                return Ok(dir);
            }
            Err(e) => warn!("源码获取失败（{e:#}），尝试下一来源"),
        }
    }
    bail!("无法获取镜像构建源码树（本地 bare 仓库与上游克隆均失败）");
}

/// 从源码 buildah 构建镜像到本机存储（首次需 1-3 小时）。
async fn build_image_locally(image: &str) -> Result<()> {
    let root = ensure_source_tree().await?;
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
    // The derived label is read from this source tree the same way the rev is,
    // and a failure to read it says the distance is unknown rather than inventing
    // one: the declared version alone covers dozens of commits, so reporting a
    // bare version number merges two code states into one name.
    // --dirty only works when describing the working tree (git fails outright
    // when given a commit-ish too), and a locally bootstrapped tree may well
    // carry uncommitted changes, which the label has to say.
    let version_id = std::process::Command::new("git")
        .args([
            "describe", "--tags", "--long", "--dirty", "--match", "v[0-9]*",
        ])
        .current_dir(&root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| format!("v{}-unknown", env!("CARGO_PKG_VERSION")));
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
        "--build-arg".into(),
        format!("VERSION_ID={version_id}"),
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
/// - 已有**任何** cogneva 管理对象但无 release → 保持 apply（helm install 会撞
///   已存在资源）；
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
    if let Some(found) = existing_cogneva_objects().await {
        info!(
            "投递探测: 已存在 cogneva 对象（{found}）且无 helm release → 保持 apply（避免资源归属冲突）"
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

/// 命名空间里是否已有 cogneva 管理的对象，返回第一个命中的名字（没有则 None）。
///
/// 判据刻意**不**只查 `deployment/cogneva`：真正要防的是"把别人管理的资源拿 helm
/// 再管一遍"。上一轮安装可能只走到一半（namespace 建了、Secret 建了、Deployment
/// 还没建），这时只查 Deployment 会得出"绿地"的结论，于是切到 helm install ——
/// helm 撞上已存在的 Namespace/Secret 直接失败，或者更糟：把 npm 的对象接管成
/// 自己的。所以对象面要从"工作负载"放宽到"这个命名空间里有没有 cogneva 的痕迹"。
async fn existing_cogneva_objects() -> Option<String> {
    for (kind, name) in [
        ("deployment", "cogneva"),
        ("daemonset", "cogneva-image-distributor"),
        ("statefulset", "cogneva-postgres"),
        ("configmap", "cogneva-json"),
        ("secret", "cogneva-secrets"),
        ("serviceaccount", "cogneva"),
        ("persistentvolumeclaim", "cogneva-data-pvc"),
    ] {
        let ok = Command::new("kubectl")
            .args(["-n", "cogneva", "get", kind, name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            return Some(format!("{kind}/{name}"));
        }
    }
    None
}

/// helm 归属三件套的键。helm 只接管**带自己标记**的对象：切投递方式时，命名空间
/// 里已有的对象缺这三样，helm install 会直接拒绝渲染结果（报"已存在且无法并入
/// 当前 release"的归属校验错误，提示里的 `--force` 是陷阱——它会重建资源）。
const HELM_MANAGED_BY_KEY: &str = "app.kubernetes.io/managed-by";
const HELM_MANAGED_BY_VALUE: &str = "Helm";
const HELM_RELEASE_NAME_KEY: &str = "meta.helm.sh/release-name";
const HELM_RELEASE_NS_KEY: &str = "meta.helm.sh/release-namespace";
const HELM_RELEASE_NAME: &str = "cogneva";

/// 归属补标的 merge patch 体。构造与断言都用这几个常量，避免手写 JSON 与常量
/// 各自漂移（打错了键名 helm 照样拒，而报错只会说"归属校验失败"）。
fn helm_ownership_patch() -> String {
    format!(
        "{{\"metadata\":{{\"labels\":{{\"{HELM_MANAGED_BY_KEY}\":\"{HELM_MANAGED_BY_VALUE}\"}},\
         \"annotations\":{{\"{HELM_RELEASE_NAME_KEY}\":\"{HELM_RELEASE_NAME}\",\
         \"{HELM_RELEASE_NS_KEY}\":\"{HELM_RELEASE_NAME}\"}}}}}}"
    )
}

/// 从 apply 投递切到 helm 之前，给已存在的 cogneva 对象补归属标记。对象不存在
/// （绿地安装）是正常路径；其它失败要说出来，否则后续 helm 接管失败时无从知道
/// 是标记没打上。
async fn ensure_helm_ownership() -> Result<()> {
    let patch = helm_ownership_patch();
    let mut targeted = 0;
    for kind in [
        "namespace/cogneva",
        "serviceaccount/cogneva",
        "configmap/cogneva-json",
        "secret/cogneva-secrets",
        "persistentvolumeclaim/cogneva-data-pvc",
        "service/cogneva",
        "deployment/cogneva",
    ] {
        let out = Command::new("kubectl")
            .args(["patch", kind, "-n", "cogneva", "--type=merge", "-p", &patch])
            .stdin(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .await;
        match out {
            Ok(o) if o.status.success() => targeted += 1,
            Ok(o) => {
                let err = String::from_utf8_lossy(&o.stderr);
                if !err.contains("NotFound") && !err.contains("not found") {
                    warn!("补 helm 归属标记失败 {kind}: {}", err.trim());
                }
            }
            Err(e) => warn!("补 helm 归属标记无法执行 {kind}: {e}"),
        }
    }
    if targeted > 0 {
        info!("已为 {targeted} 个既有对象补 helm 归属标记（managed-by=Helm / release=cogneva）");
    }
    Ok(())
}

/// helm 客户端版本的唯一权威源。安装期按它下载 helm，CI 的部署 parity /
/// 渲染新鲜度门禁也按它装同一个版本——那两处门禁是**字节级**比对渲染产物的，
/// 而不同 helm 版本渲染块标量的尾随空白不同，放任 CI 自行取版本会让门禁在
/// 与代码无关的地方红。
const HELM_VERSION: &str = "v4.2.4";

/// 确保 helm 客户端可用。仅在"复用既有集群、绿地部署、需要 helm 投递"时调用——
/// 元启动自建集群的命门链路永远走预渲染 apply，不下载 helm。
/// 下载候选：CN 首选华为云 helm 镜像（get.helm.sh 背后是 GitHub releases，
/// CN 直连不稳），海外首选 get.helm.sh；逐候选尝试，全失败返回 false 由调用方回落。
async fn ensure_helm() -> bool {
    if command_exists("helm").await {
        return true;
    }
    // helm 3/4 包内布局相同（linux-<arch>/helm）。
    let version = HELM_VERSION;
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
        let dir = match make_workdir("helm") {
            Ok(d) => d,
            Err(e) => {
                warn!("创建 helm 安装工作目录失败（{e:#}），回落 apply");
                return false;
            }
        };
        let tgz = dir.join("helm.tgz");
        // 过去这里只有 --connect-timeout：连接建得起来但被限速到 KB/s 时永远不返回。
        let ok = match curl_to_file(&url, &tgz, MANDATORY).await {
            Ok(()) => run(
                "sh",
                &[
                    "-c",
                    &format!(
                        "tar -xzf '{tgz}' -C '{dir}' && install -m 0755 '{dir}/linux-{arch}/helm' /usr/local/bin/helm",
                        tgz = tgz.display(),
                        dir = dir.display(),
                    ),
                ],
            )
            .await
            .is_ok(),
            Err(_) => false,
        };
        let _ = std::fs::remove_dir_all(&dir);
        if ok {
            info!("helm 安装完成（来源 {url}）");
            return true;
        }
        warn!("helm 下载/安装失败，换下一个候选: {url}");
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
    // 卷声明在绑定后不可变（理由见 retain_existing_claims）：已存在的先摘出本次
    // apply 的输入集合、保留既有绑定并报出差异，否则一份改过的声明会被准入拒绝，
    // 而 `kubectl apply -f <目录>` 一处失败即中止整批——改过声明量的那几份卷就能
    // 把整套清单的安装一起打停。其余资源不受影响，仍整目录 apply。
    let kept = retain_existing_claims(&rendered).await?;
    // 同类问题、另一条判据：清单把某个 env 从字面量改成 valueFrom 后，对象上那份
    // 残留的 value 会让整批 apply 在这一处中止（理由见函数注释）。它和卷声明一样，
    // 必须在交付前处理，改清单本身解决不了。
    let cleared = clear_superseded_env_values_in_dir(&rendered).await?;
    if cleared > 0 {
        info!(
            count = cleared,
            "cleared env values the manifests now inject from the Secret"
        );
    }
    if kept > 0 {
        info!(
            count = kept,
            "existing volume claims kept as bound (their spec is immutable, so they are not re-applied)"
        );
    }
    run("kubectl", &["apply", "-f", &rendered.to_string_lossy()]).await
}

/// 卷声明是安装面对象，不是循环面对象：绑定后的 PVC spec 除 `resources.requests`
/// 外不可变（那个例外还要 StorageClass 支持扩容，local-path 不支持），
/// StorageClass 一旦绑定更是永远改不回来。对已存在的卷声明 apply 一份不同的声明
/// 必然被准入拒绝，而 `kubectl apply -f <目录>` 一处失败即中止整批。
///
/// 所以这里把已存在的卷声明从本次 apply 的输入里摘掉（临时产物目录是本进程独占
/// 的一次性拷贝，摘掉不动仓库里的清单），保留既有绑定，并把声明量与在用值的差异
/// 打出来。摘掉不等于"忽略"：声明够不够用由运行期持续的
/// `data_volume_over_declared_size` 规则对着在用 PVC 比对，比一次安装期的校核更
/// 贴近事实。
///
/// 返回摘掉（即已存在）的卷声明条数。
async fn retain_existing_claims(rendered: &Path) -> Result<usize> {
    let mut kept = 0usize;
    for entry in std::fs::read_dir(rendered)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let Some((name, declared)) = claim_declaration(&text) else {
            continue;
        };
        // 取不到在用值（不存在、查询失败、集群不可达）就当不存在，交给 apply 去
        // 创建：集群真的不可达时后续 apply 会报出真实错误，不在这里吞掉。
        let Some(live) = live_claim_storage(&name).await else {
            continue;
        };
        if quantity_bytes(declared.as_deref()) != quantity_bytes(Some(&live)) {
            warn!(
                claim = %name,
                declared = declared.as_deref().unwrap_or("<none>"),
                live = %live,
                "existing volume claim keeps its bound declaration; the manifest declares a different size and cannot be applied (a bound claim's spec is immutable, and this storage class may not support expansion)"
            );
        }
        std::fs::remove_file(&path)
            .with_context(|| format!("移出已存在的卷声明失败：{}", path.display()))?;
        kept += 1;
    }
    Ok(kept)
}

/// 交付前清掉"清单已改用 `valueFrom`、对象上却还留着字面量 `value`"的残留 env。
///
/// 这类残留**改清单改不掉**：`kubectl apply` 的三方合并按 `name` 合并 env 条目，
/// 清单去掉的字段在没有 last-applied 注解的对象上会被当成"别人写的"原样保留，
/// 于是该条目同时带着 `value` 与 `valueFrom` 被准入拒绝——**这个对象从此永远
/// apply 不进去**，而报错指向清单（清单其实是对的）。实测形态：安全网关的
/// `COGNEVA_GITEE_OAUTH_CLIENT_SECRET` 从明文改成从 Secret 注入后，整批
/// `kubectl apply -f <目录>` 就在这一处中止，元启动停在这一步。
///
/// 判据在 `cog_core::contract::env_supersede`（纯函数、带测试）：只有"清单声明
/// 了 valueFrom、对象上同名条目还带 value"才动手，其余一律不碰。
async fn clear_superseded_env_values_in_dir(rendered: &Path) -> Result<usize> {
    let mut cleared = 0usize;
    for entry in std::fs::read_dir(rendered)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        cleared += clear_superseded_env_values(&text).await?;
    }
    Ok(cleared)
}

/// 同上，输入是清单文本（helm template 的整份输出）。返回清掉的字段数。
async fn clear_superseded_env_values(text: &str) -> Result<usize> {
    let mut cleared = 0usize;
    for doc in serde_yaml::Deserializer::from_str(text) {
        let Ok(doc) = serde_yaml::Value::deserialize(doc) else {
            continue;
        };
        let Ok(desired) = serde_json::to_value(&doc) else {
            continue;
        };
        let Some(workload) = cog_core::contract::env_supersede::workload_identity(&desired) else {
            continue;
        };
        let (kind, name) = (workload.kind.clone(), workload.name.clone());
        // 清单一般不写命名空间（由 apply 时的 -n 决定），此时用交付面这一个。
        let namespace = workload
            .namespace
            .clone()
            .unwrap_or_else(|| "cogneva".to_string());
        // 读不到现状（对象还不存在、查询失败）就什么都不做：首次安装本来就没有残留，
        // 集群不可达时后续交付会报出真实错误，不在这里吞掉。
        let Some(live) = live_object(&kind, &name, &namespace).await else {
            continue;
        };
        let removals = cog_core::contract::env_supersede::superseded_env_values(&desired, &live);
        if removals.is_empty() {
            continue;
        }
        // 同一容器内按下标倒序删：正向删会让后面条目的下标整体前移，patch 打偏。
        let ops = cog_core::contract::env_supersede::removal_patch_ops(&removals);
        let names: Vec<&str> = removals.iter().map(|r| r.name.as_str()).collect();
        let out = Command::new("kubectl")
            .args([
                "-n",
                &namespace,
                "patch",
                &kind.to_lowercase(),
                &name,
                "--type=json",
                "-p",
                &serde_json::Value::Array(ops).to_string(),
            ])
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("清理 {kind}/{name} 的残留 env value 失败"))?;
        if !out.status.success() {
            bail!(
                "清理 {kind}/{name} 上被 valueFrom 取代的 env value 失败（{}）：{}",
                names.join(", "),
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        info!(
            workload = %format!("{kind}/{name}"),
            envs = %names.join(", "),
            "cleared superseded env values (the manifest injects them from the Secret)"
        );
        cleared += removals.len();
    }
    Ok(cleared)
}

/// 对象现状（`kubectl get -o json`）。不存在或查询失败返回 None。
async fn live_object(kind: &str, name: &str, namespace: &str) -> Option<serde_json::Value> {
    let out = Command::new("kubectl")
        .args([
            "-n",
            namespace,
            "get",
            &kind.to_lowercase(),
            name,
            "-o",
            "json",
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice(&out.stdout).ok()
}

/// 从一份清单文本认出卷声明：返回（名字，声明的 requests.storage）。不是
/// PersistentVolumeClaim 的文档、以及解析不了的文本一律返回 None（后者留在
/// apply 输入集合里，让 kubectl 报出真实语法错误）。
fn claim_declaration(text: &str) -> Option<(String, Option<String>)> {
    let doc: serde_yaml::Value = serde_yaml::from_str(text).ok()?;
    if doc.get("kind")?.as_str()? != "PersistentVolumeClaim" {
        return None;
    }
    let name = doc.get("metadata")?.get("name")?.as_str()?.to_string();
    let declared = doc
        .get("spec")
        .and_then(|s| s.get("resources"))
        .and_then(|r| r.get("requests"))
        .and_then(|r| r.get("storage"))
        .and_then(|s| s.as_str())
        .map(str::to_string);
    Some((name, declared))
}

/// 集群里这条卷声明在用的 requests.storage。查不到（不存在、kubectl 失败、
/// 集群不可达）返回 None——调用方按"不存在"处理，由后续 apply 报出真实错误。
async fn live_claim_storage(name: &str) -> Option<String> {
    let out = Command::new("kubectl")
        .args([
            "-n",
            "cogneva",
            "get",
            "pvc",
            name,
            "-o",
            "jsonpath={.spec.resources.requests.storage}",
        ])
        .stdin(Stdio::null())
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if v.is_empty() {
        None
    } else {
        Some(v)
    }
}

/// K8s 数量串 → 字节数，只认十进制与二进制后缀。两位声明量比"不同"时用它，
/// 免得 `5Gi` 与 API 规范化后的 `5368709120` 被读成差异。认不出返回 None。
fn quantity_bytes(text: Option<&str>) -> Option<f64> {
    let text = text?.trim();
    let digits_end = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(text.len());
    let (num, suffix) = text.split_at(digits_end);
    let value: f64 = num.parse().ok()?;
    let factor = match suffix {
        "" => 1.0,
        "m" => 1e-3,
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        "Ki" => 1024.0,
        "Mi" => 1024f64.powi(2),
        "Gi" => 1024f64.powi(3),
        "Ti" => 1024f64.powi(4),
        "Pi" => 1024f64.powi(5),
        "Ei" => 1024f64.powi(6),
        _ => return None,
    };
    Some(value * factor)
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
    let mut overrides: Vec<(String, String)> = Vec::new();
    if cn_mirror() {
        // CN 适配走 values 覆盖（镜像站前缀 + seed 地址），不依赖 helm
        // 版本相关的 post-renderer 机制，release 元数据完整保留。
        overrides = cn_helm_value_overrides(docker_mirror_host().await)?;
    }
    // 交付与预渲染必须拿到**逐字相同**的入参集合，否则清理所依据的清单不是交付的
    // 那一份（镜像站覆盖差异尤其隐蔽：渲染少一个 --set，对象与清单就对不上）。
    let common = helm_common_args(&chart, &values, &overrides);
    let mut args: Vec<String> = vec!["upgrade".into(), "--install".into()];
    args.extend(common.iter().cloned());
    args.push("--create-namespace".into());

    // chart 自己渲染 Namespace，所以复用一个已存在的命名空间（上一次 apply 建的、
    // 或使用者手建的）时，helm 会因为该对象"存在但无归属标记"而拒绝 install。
    // 先补标记再交付：这类对象是同一套清单建的，本来就该由本 release 接管。
    ensure_helm_ownership().await?;
    // 交付前先渲染一遍：清单与对象现状之间有类**交付本身清不掉**的差异——最典型
    // 的是 env 从字面量改成 valueFrom 后对象上残留的 value（详见
    // clear_superseded_env_values）。渲染只是拿来做这件事，不替代交付。
    // 渲染失败不当门禁：交付自己的报错更准确，这里只少一次清理。
    match helm_render(&common).await {
        Ok(text) => {
            let cleared = clear_superseded_env_values(&text).await?;
            if cleared > 0 {
                info!(
                    count = cleared,
                    "cleared env values the chart injects from the Secret"
                );
            }
        }
        Err(e) => warn!("helm template 预渲染失败，跳过残留 env 清理: {e}"),
    }
    info!(
        "helm 投递 {} profile（upgrade --install，幂等）",
        profile.dir_name()
    );
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    run("helm", &refs).await
}

/// `upgrade --install` 与 `template` 的公共入参（release 名、chart、命名空间、
/// values、镜像站覆盖）。两处共用一份，是为了让"清理依据的清单"与"交付的清单"
/// 不可能不同。
fn helm_common_args(chart: &Path, values: &Path, overrides: &[(String, String)]) -> Vec<String> {
    let mut out: Vec<String> = vec![
        "cogneva".into(),
        chart.to_string_lossy().into_owned(),
        "-n".into(),
        "cogneva".into(),
        "-f".into(),
        values.to_string_lossy().into_owned(),
        "--set".into(),
        "secrets.create=true".into(),
    ];
    for (key, val) in overrides {
        out.push("--set".into());
        out.push(format!("{key}={val}"));
    }
    out
}

/// 渲染 chart（不落地）：给交付前的一致性清理提供"将交付什么"的事实。
async fn helm_render(common: &[String]) -> Result<String> {
    let mut args: Vec<String> = vec!["template".into()];
    args.extend(common.iter().cloned());
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let out = Command::new("helm")
        .args(&refs)
        .stdin(Stdio::null())
        .output()
        .await
        .context("执行 helm template 失败")?;
    if !out.status.success() {
        bail!(
            "helm template 退出码非零: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
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
/// 前缀（不硬编码 tag，chart 升版不漂移），seed 镜像列表整体倒序成 Gitee 优先
/// ——与 render_manifests_for_cluster 的整表旋转同义，两条投递路径网络适配一致。
/// 只置首位会把另一个镜像挤掉，CN 下 Gitee 不可达时就没有回退项了。
fn cn_helm_value_overrides(mirror: &str) -> Result<Vec<(String, String)>> {
    let parsed = chart_values()?;
    let mut out = cn_helm_image_overrides(&parsed, mirror);
    for (key, url) in [
        ("evolution.gitRemote.seedUrls[0]", GIT_MIRROR_GITEE),
        ("evolution.gitRemote.seedUrls[1]", GIT_MIRROR_GITHUB),
        ("sandboxExecutor.gitSeedUrls[0]", GIT_MIRROR_GITEE),
        ("sandboxExecutor.gitSeedUrls[1]", GIT_MIRROR_GITHUB),
    ] {
        out.push((key.to_string(), url.to_string()));
    }
    Ok(out)
}

/// 镜像部分的覆盖表（seed 地址另算），由 values.yaml **遍历生成**。
///
/// 这里从前是手写清单：六条镜像逐条列出，漏了 `backends.meilisearch` 与
/// `backends.seaweedfs`（两个都会走 docker.io 直连），而手写清单与 values.yaml
/// 是两份事实，chart 里每加一个后端就漂一次。改成遍历后，"values.yaml 里有什么
/// 镜像"就是唯一事实源。
///
/// 跳过集群内 registry 引用：那些镜像由 bootstrap 自己导入节点，加公网前缀反而
/// 会让 kubelet 去公网找一个不存在的仓库。
fn cn_helm_image_overrides(values: &serde_yaml::Value, mirror: &str) -> Vec<(String, String)> {
    public_image_refs(values)
        .into_iter()
        .map(|(key, reference)| (key, cn_mirror_image(&reference, mirror)))
        .collect()
}

/// values.yaml 里的全部**公开**镜像引用（遍历收集 + 去掉集群内 registry）。
/// 两条网络适配路径（helm `--set` 与清单文本替换）都从这一个函数取数，避免各自
/// 走一遍遍历、各自漏各自的。
fn public_image_refs(values: &serde_yaml::Value) -> Vec<(String, String)> {
    let mut refs = Vec::new();
    collect_image_refs(values, "", &mut refs);
    refs.retain(|(_, reference)| !is_in_cluster_image(reference));
    refs
}

/// 递归收集 YAML 里所有镜像引用，返回 (dotted 路径, 引用)。
///
/// 两种形态都收：`image: postgres:16-alpine`（字符串）与
/// `image: {repository: …, tag: …}`（映射，取 repository）。只认字符串形态会
/// 静默漏掉映射形态，而漏掉的后果是那个镜像在 CN 下走直连。
fn collect_image_refs(value: &serde_yaml::Value, path: &str, out: &mut Vec<(String, String)>) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (k, v) in map {
                let Some(key) = k.as_str() else { continue };
                let child = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}.{key}")
                };
                match key {
                    "image" => match v {
                        serde_yaml::Value::String(s) => out.push((child, s.clone())),
                        serde_yaml::Value::Mapping(_) => {
                            if let Some(serde_yaml::Value::String(repo)) = v.get("repository") {
                                out.push((format!("{child}.repository"), repo.clone()));
                            }
                        }
                        _ => {}
                    },
                    _ => collect_image_refs(v, &child, out),
                }
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for (i, item) in seq.iter().enumerate() {
                collect_image_refs(item, &format!("{path}[{i}]"), out);
            }
        }
        _ => {}
    }
}

/// 镜像引用是否指向集群内 registry：host 段是 localhost，或带显式端口——集群内
/// registry 以 NodePort 暴露，引用里必然带端口，而公网仓库不带。这类引用的权威在
/// 集群内（镜像由 bootstrap 自己导入节点），加公网前缀会让 kubelet 去公网找一个
/// 不存在的仓库。
///
/// 没有 `/` 的引用（`postgres:16-alpine`、`registry:2`）是"name[:tag]"形态，没有
/// host 段，一律算公开镜像——把它们的 `:16-alpine` 读成端口就会把整个后端的镜像
/// 判成集群内，于是该加的镜像站前缀一条都不加。
fn is_in_cluster_image(reference: &str) -> bool {
    match reference.split_once('/') {
        None => false,
        Some((host, _)) => host == "localhost" || host.contains(':'),
    }
}

/// CN 网络下把 seed 镜像列表整体倒序（Gitee 置首）而不只留一个地址。清单里两个
/// 镜像都列着，只做「交换」才能保住回退项：逐个字符串替换会把两个都改成 Gitee。
/// 哨兵占位使交换对出现次数不敏感（两个 URL 出现几次就换几对）。
fn prefer_gitee_seed_mirrors(text: &str) -> String {
    const SENTINEL: &str = "__GIT_MIRROR_SWAP__";
    if !text.contains(GIT_MIRROR_GITHUB) {
        return text.to_string();
    }
    if !text.contains(GIT_MIRROR_GITEE) {
        // 只列了一个镜像（老清单/自定义 values）：直接换成 Gitee，与旧行为一致。
        return text.replace(GIT_MIRROR_GITHUB, GIT_MIRROR_GITEE);
    }
    text.replace(GIT_MIRROR_GITHUB, SENTINEL)
        .replace(GIT_MIRROR_GITEE, GIT_MIRROR_GITHUB)
        .replace(SENTINEL, GIT_MIRROR_GITEE)
}

/// 按运行网络环境处理预渲染 profile 产物副本。环境差异（containerd socket、
/// StorageClass、git-remote hostPath/PVC、ingress class）已在 CI 渲染时固化
/// 进各 profile，这里只做与网络可达性相关的替换：
/// - CN 模式 → 公开镜像加国内镜像站前缀（Docker Hub 被墙）；
/// - CN 模式 → seed 镜像列表倒序成 Gitee 优先（GitHub 拉取受限）。
///
/// 返回处理后的目录（kubectl apply 后即弃）。
async fn render_manifests_for_cluster(dir: &Path) -> Result<PathBuf> {
    let cn = cn_mirror();
    let out = make_workdir("manifests")?;
    // 替换表读不出来即失败：静默退回空表等于 CN 下公开镜像全走直连（必挂），
    // 而失败点会落在几十分钟后的镜像拉取超时上，看不见这里才是真因。
    let image_map = if cn {
        cn_image_map(&chart_values()?, docker_mirror_host().await)
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
            text = prefer_gitee_seed_mirrors(&text);
        }
        std::fs::write(out.join(entry.file_name()), text)?;
    }
    Ok(out)
}

/// CN 模式公开镜像替换表：按探活选定的 docker 镜像站生成前缀，
/// 单站故障时下次安装自动换站（清单内嵌完整主机名，不走 containerd 回退）。
///
/// 替换项由 chart values.yaml 遍历生成，与 helm 投递的 --set 覆盖同源——两条投递
/// 路径过去各自手写一份清单，于是各自漏各自的（本路径漏 meilisearch / seaweedfs，
/// 还留着一个早已不在任何清单里的 mysql 条目）。清单文本层面替换，不解析重写
/// YAML：渲染产物里的注释与缩进保持原样。
fn cn_image_map(values: &serde_yaml::Value, mirror: &str) -> Vec<(String, String)> {
    public_image_refs(values)
        .into_iter()
        .map(|(_, reference)| {
            let mirrored = cn_mirror_image(&reference, mirror);
            (format!("image: {reference}"), format!("image: {mirrored}"))
        })
        .collect()
}

/// chart 基础 values.yaml 解析结果（两条网络适配路径共用一份读入与解析）。
fn chart_values() -> Result<serde_yaml::Value> {
    let path = repo_root().join("deploy/helm/cogneva/values.yaml");
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("读取 {} 失败", path.display()))?;
    Ok(serde_yaml::from_str(&text)?)
}

/// 就绪门禁：两个 Deployment 都 rollout 成功才算装机完成。
///
/// 未就绪**不是**告警：调用方在这之后就会打印"部署完成"、建端口转发、打开浏览器
/// ——把一个没起来的系统当成功交出去，使用者看到的是一个打不开的页面，而唯一的
/// 线索被埋在告警里。所以未就绪即返回 Err（非零退出），并且把排查入口一并给出：
/// 只说"失败"等于把排查成本原样退回给使用者。
///
/// 对象不存在按"该组件未部署"处理：chart 支持 `securityGateway.enabled=false`，
/// 而这里拿不到对象时无法区分"被关掉"与"没建出来"。
async fn wait_ready() -> Result<()> {
    let mut not_ready = Vec::new();
    for deploy in ["cogneva", "cogneva-security-gateway"] {
        let out = Command::new("kubectl")
            .args([
                "-n",
                "cogneva",
                "rollout",
                "status",
                &format!("deployment/{deploy}"),
                "--timeout=180s",
            ])
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("无法执行 kubectl rollout status deployment/{deploy}"))?;
        if out.status.success() {
            continue;
        }
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        if stderr.contains("NotFound") || stderr.contains("not found") {
            warn!("deployment/{deploy} 不存在，视为该组件未部署（安全网关注销？）");
            continue;
        }
        let detail = if stderr.is_empty() {
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        } else {
            stderr
        };
        warn!("deployment/{deploy} 未在超时内 Ready: {detail}");
        not_ready.push(deploy);
    }
    if !not_ready.is_empty() {
        let hints: Vec<String> = not_ready
            .iter()
            .map(|d| format!("kubectl -n cogneva logs deploy/{d} --tail=100"))
            .collect();
        bail!(
            "部署未就绪（{}），装机未完成：kubectl -n cogneva get pods -o wide; {}",
            not_ready.join(", "),
            hints.join("; ")
        );
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
    // Arguments are settled first, before the tracing subscriber and before the
    // asset unpacking below — the latter writes a work directory, and a run
    // that only prints the usage must not leave one behind.
    match cli::Command::parse(std::env::args().skip(1)) {
        Ok(cli::Command::Run) => {}
        Ok(cli::Command::Help) => {
            println!("{}", cli::USAGE);
            return Ok(());
        }
        Ok(cli::Command::Version) => {
            println!("cogneva-bootstrap {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Err(unknown) => {
            eprintln!("cogneva-bootstrap: 无法识别的参数: {unknown}");
            eprintln!("{}", cli::USAGE);
            std::process::exit(2);
        }
    }

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

    // 预编译路径：引导器自带全部部署资产，解包后把 repo_root 指向解包目录。
    // 两种情况保持磁盘源码树优先：bootstrap.sh 源码构建会显式 export
    // COGNEVA_REPO_ROOT；开发者在仓库根直接运行时 cwd 下就是 deploy/。
    if std::env::var("COGNEVA_REPO_ROOT").is_err()
        && !Path::new("deploy/helm/cogneva/Chart.yaml").is_file()
    {
        let assets = materialize_assets()?;
        std::env::set_var("COGNEVA_REPO_ROOT", &assets);
        info!("部署资产已从二进制内嵌内容解包 → {}", assets.display());
    }

    #[cfg(not(target_os = "linux"))]
    warn!(
        "引导器需在 Linux 运行层内执行，当前为 {}。\
         macOS 请改用 bootstrap.sh（自动经 Lima 虚拟机），Windows 请改用 bootstrap.ps1（自动经 WSL2）",
        std::env::consts::OS
    );

    info!("LLM 接入不在引导器做：部署完成后由 WebUI 强制向导完成（全自动零问答）");

    // 身份在这一步结算：下面每一步都在写系统目录（/etc/rancher、/var/lib/cogneva-data、
    // apt），非 root 跑下去只会在某个深处的写操作上 EACCES，而那个位置的报错通常
    // 指向不相干的一步。`--help` / `--version` 已在上面的参数结算里返回，不经过这里。
    privileges::require_root("元启动（安装集群并部署）")?;

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
        Distro::K3s if !decision.multi => install_k3s(&hw).await?,
        Distro::K3s => ensure_multi_node_cluster(&hw).await?,
        // kubespray：跑官方镜像新建标准 Kubernetes（本机为控制面，声明节点作 worker）。
        Distro::Kubespray => kubespray::run_kubespray(&cluster_nodes_env()).await?,
    }
    ensure_buildah().await?;
    ensure_buildah_mirror().await?;
    ensure_firecracker().await?;
    ensure_git().await?;
    if probe_nodes().await > 1 {
        // 多节点：git-remote 走集群卷（渲染时选 PVC 变体），宿主 bare 仓库不再使用
        info!("多节点集群：git-remote 走集群卷，跳过宿主 bare 仓库 seed");
    } else {
        ensure_git_remote().await?;
    }
    ensure_runtime_image(decision.distro, decision.multi).await?;
    deploy_manifests(cluster_existed).await?;
    // 清单 apply 后 registry 才存在；四部署 pin registry :local，播种是硬依赖。
    seed_cluster_registry().await?;
    kick_image_pull_pending().await?;
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
    fn embedded_assets_cover_apply_and_helm_inputs() {
        let assets: Vec<&str> = super::embedded_assets::EMBEDDED_ASSETS
            .iter()
            .map(|(p, _)| *p)
            .collect();
        for required in [
            "deploy/scripts/init-secrets.sh",
            "deploy/helm/cogneva/Chart.yaml",
            "deploy/helm/cogneva/values.yaml",
            "deploy/helm/cogneva/profiles/k3s-single.yaml",
            "deploy/helm/cogneva/templates/secret.yaml",
            "deploy/rendered/k3s-single/00-namespace-cogneva.yaml",
            "deploy/rendered/k3s-single/41-deployment-cogneva.yaml",
            "deploy/rendered/k3s-multi/20-persistentvolumeclaim-cogneva-git-remote-pvc.yaml",
            "deploy/rendered/k8s-standard/60-ingress-cogneva.yaml",
        ] {
            assert!(assets.contains(&required), "内嵌资产缺失: {required}");
        }
        // 三个 profile 的渲染产物必须成套（38/39/37 量级，这里只断言下限防漏嵌）
        for profile in ["k3s-single", "k3s-multi", "k8s-standard"] {
            let n = assets
                .iter()
                .filter(|p| p.starts_with(&format!("deploy/rendered/{profile}/")))
                .count();
            assert!(n >= 30, "profile {profile} 内嵌清单数异常: {n}");
        }
    }

    #[test]
    fn materialized_assets_are_readable() {
        let dir = super::materialize_assets().expect("资产解包成功");
        assert!(dir.join("deploy/scripts/init-secrets.sh").is_file());
        assert!(dir.join("deploy/helm/cogneva/Chart.yaml").is_file());
        assert!(dir
            .join("deploy/rendered/k3s-single/00-namespace-cogneva.yaml")
            .is_file());
    }

    #[test]
    fn registries_yaml_covers_every_registry_the_charts_pull_from() {
        let y = super::k3s_registries_yaml();
        // 只镜像 docker.io 时，kube-prometheus-stack 的 kube-state-metrics
        // （registry.k8s.io）在受限网络里会一直 ImagePullBackOff 到 --wait 超时。
        for registry in ["docker.io", "registry.k8s.io", "quay.io"] {
            assert!(
                y.contains(&format!("  {registry}:\n")),
                "registries.yaml 缺 {registry} 段: {y}"
            );
        }
        assert!(y.contains("k8s-gcr.m.daocloud.io"), "缺 k8s.io 镜像站: {y}");
        assert!(y.contains("quay.m.daocloud.io"), "缺 quay 镜像站: {y}");
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

    #[test]
    fn cn_seed_mirror_list_is_reordered_not_collapsed() {
        let gh = super::GIT_MIRROR_GITHUB;
        let gt = super::GIT_MIRROR_GITEE;
        // 两个镜像都在：倒序，不能把 GitHub 那条吞掉
        assert_eq!(
            super::prefer_gitee_seed_mirrors(&format!("[{gh}, {gt}]")),
            format!("[{gt}, {gh}]")
        );
        // 同一 URL 出现多次也要成对交换，且结果都是合法地址
        assert_eq!(
            super::prefer_gitee_seed_mirrors(&format!("{gh}|{gh}|{gt}")),
            format!("{gt}|{gt}|{gh}")
        );
        // 只列了一个镜像：退化成直接替换，与旧行为一致
        assert_eq!(super::prefer_gitee_seed_mirrors(gh), gt);
        assert_eq!(super::prefer_gitee_seed_mirrors(gt), gt);
        // 与 seed 无关的文本原样保留
        assert_eq!(
            super::prefer_gitee_seed_mirrors("kind: Deployment"),
            "kind: Deployment"
        );
    }
}

#[cfg(test)]
mod install_claim_tests {
    use super::{claim_declaration, quantity_bytes};

    #[test]
    fn claim_declaration_only_matches_volume_claims() {
        let pvc = "kind: PersistentVolumeClaim\nmetadata:\n  name: cogneva-data-pvc\nspec:\n  resources:\n    requests:\n      storage: 24Gi\n";
        assert_eq!(
            claim_declaration(pvc),
            Some(("cogneva-data-pvc".to_string(), Some("24Gi".to_string())))
        );
        // 声明里没写 requests.storage（目录型卷）：仍算卷声明，量是 None
        let bare = "kind: PersistentVolumeClaim\nmetadata:\n  name: p\nspec: {}\n";
        assert_eq!(claim_declaration(bare), Some(("p".to_string(), None)));
        // 非卷声明与解析不了的文本都不认，留在 apply 输入集合里
        let deploy = "kind: Deployment\nmetadata:\n  name: cogneva\nspec: {}\n";
        assert_eq!(claim_declaration(deploy), None);
        assert_eq!(claim_declaration("kind: [unclosed"), None);
    }

    #[test]
    fn quantity_bytes_reads_decimal_and_binary_suffixes() {
        assert_eq!(quantity_bytes(Some("1")), Some(1.0));
        assert_eq!(quantity_bytes(Some("5Gi")), Some(5.0 * 1024f64.powi(3)));
        assert_eq!(quantity_bytes(Some("64Mi")), Some(64.0 * 1024f64.powi(2)));
        assert_eq!(quantity_bytes(Some("300G")), Some(300e9));
        assert_eq!(quantity_bytes(Some("500m")), Some(0.5));
        // 认不出的量返回 None（拿去比"不同"时只会多报不会漏报）
        assert_eq!(quantity_bytes(Some("abc")), None);
        assert_eq!(quantity_bytes(Some("5Zi")), None);
        assert_eq!(quantity_bytes(None), None);
    }

    /// 安装期报差异的判据：API 规范化后的等价值不算差异，声明量真的不同才算。
    #[test]
    fn claim_size_comparison_ignores_representation_and_keeps_real_diffs() {
        // 同一份量的两种写法（清单写 5Gi、API 规范化成字节数）不算差异
        assert_eq!(
            quantity_bytes(Some("5Gi")),
            quantity_bytes(Some("5368709120"))
        );
        assert_eq!(quantity_bytes(Some("24Gi")), quantity_bytes(Some("24Gi")));
        // 真实事故形态：声明比在用值小（源码卷 10Gi vs 在用 57Gi）与大（72Gi vs 10Gi）
        assert_ne!(quantity_bytes(Some("10Gi")), quantity_bytes(Some("57Gi")));
        assert_ne!(quantity_bytes(Some("72Gi")), quantity_bytes(Some("10Gi")));
        // 清单没声明量而在用有量 → 算差异，不能当成一致
        assert_ne!(quantity_bytes(None), quantity_bytes(Some("10Gi")));
    }
}

/// CN 网络适配表的门禁。三份输入必须覆盖同一组镜像：chart values.yaml（覆盖表的
/// 来源）、helm `--set` 表、清单文本替换表、以及**真正会被 apply 的渲染产物**。
///
/// 这一条的失效形态是**静默漏项**：漏掉的镜像在 CN 下走 docker.io 直连，失败点落在
/// 几十分钟后的镜像拉取超时上，与替换表毫无关联，排查时根本不会往这儿看。
#[cfg(test)]
mod cn_image_tests {
    use super::{
        cn_helm_image_overrides, cn_image_map, cn_mirror_image, collect_image_refs,
        is_in_cluster_image,
    };
    use std::path::{Path, PathBuf};

    fn repo_file(rel: &str) -> PathBuf {
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../..")).join(rel)
    }

    fn chart_values() -> serde_yaml::Value {
        let path = repo_file("deploy/helm/cogneva/values.yaml");
        serde_yaml::from_str(&std::fs::read_to_string(&path).unwrap())
            .unwrap_or_else(|e| panic!("解析 {} 失败: {e}", path.display()))
    }

    /// values.yaml 里的公开镜像一个都不能漏，集群内 registry 引用一个都不能动。
    #[test]
    fn every_public_image_is_overridden_and_in_cluster_refs_are_not() {
        let values = chart_values();
        let mut refs = Vec::new();
        collect_image_refs(&values, "", &mut refs);
        let public: Vec<&(String, String)> = refs
            .iter()
            .filter(|(_, r)| !is_in_cluster_image(r))
            .collect();
        // 遍历自身要有效：values 被重构成另一种形态、walk 静默退化成空表时这里红
        assert!(
            public.len() >= 7,
            "values.yaml 只遍历到 {} 个公开镜像（共 {} 条镜像字段）: {refs:?}",
            public.len(),
            refs.len()
        );
        let overrides = cn_helm_image_overrides(&values, "MIRROR");
        for (path, reference) in &public {
            assert!(
                overrides.iter().any(|(k, _)| k == path),
                "镜像 {reference}（{path}）没有进 CN 覆盖表"
            );
        }
        // 上一版手写清单漏掉的两个后端镜像（这两个都真会走 docker.io 直连）
        for path in ["backends.meilisearch.image", "backends.seaweedfs.image"] {
            assert!(
                overrides.iter().any(|(k, _)| k == path),
                "{path} 缺 CN 覆盖"
            );
        }
        // 集群内 registry（localhost:30500/cogneva）遍历得到但绝不覆盖
        assert!(refs.iter().any(|(k, _)| k == "image.repository"));
        assert!(overrides.iter().all(|(k, _)| k != "image.repository"));
        assert!(overrides.iter().all(|(_, v)| !v.contains("localhost")));
    }

    /// 文本替换表与 `--set` 表同源：同一批引用、同一个镜像站前缀。
    #[test]
    fn text_map_and_set_map_agree() {
        let values = chart_values();
        let set = cn_helm_image_overrides(&values, "MIRROR");
        let text = cn_image_map(&values, "MIRROR");
        let mut refs = Vec::new();
        collect_image_refs(&values, "", &mut refs);
        for (_, reference) in refs.iter().filter(|(_, r)| !is_in_cluster_image(r)) {
            let mirrored = cn_mirror_image(reference, "MIRROR");
            assert!(
                text.contains(&(format!("image: {reference}"), format!("image: {mirrored}"))),
                "文本替换表缺 {reference} → {mirrored}"
            );
            assert!(set.iter().any(|(_, v)| v == &mirrored));
        }
        assert_eq!(text.len(), set.len(), "两张表的镜像条数应一致");
    }

    /// 端到端：**真正会被 apply 的**渲染产物里，每个公开镜像都要能被替换表认出来。
    /// 这条读的是产出物而不是 values.yaml，所以它同时守着"清单里出现了 values.yaml
    /// 没登记的镜像"这种漏法。
    #[test]
    fn rendered_manifests_reference_no_uncovered_public_image() {
        let text = cn_image_map(&chart_values(), "MIRROR");
        let covered: Vec<String> = text.into_iter().map(|(from, _)| from).collect();
        let mut checked = 0;
        for file in deployed_manifests() {
            let content = std::fs::read_to_string(&file).unwrap();
            for line in content.lines() {
                let Some(rest) = line.trim().strip_prefix("image: ") else {
                    continue;
                };
                let reference = rest.trim().trim_matches('"');
                if is_in_cluster_image(reference) {
                    continue;
                }
                checked += 1;
                assert!(
                    covered.contains(&format!("image: {reference}")),
                    "{} 引用 {reference}，但它不在 CN 替换表里（CN 下这条会走 docker.io 直连）",
                    file.display()
                );
            }
        }
        assert!(
            checked >= 5,
            "只扫到 {checked} 条公开镜像引用，扫描路径疑似失效"
        );
    }

    /// cogneva 交付面（chart 渲染产物 + 集群静态清单）下的全部 YAML。
    /// 观测栈（`deploy/k3s/observability/`）不在其中：它有独立安装脚本与生命周期，
    /// 镜像地址由它自己处理。
    fn deployed_manifests() -> Vec<PathBuf> {
        let mut files = Vec::new();
        for dir in ["deploy/rendered", "deploy/k3s"] {
            let root = repo_file(dir);
            let Ok(entries) = std::fs::read_dir(&root) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if path.file_name().and_then(|n| n.to_str()) == Some("observability") {
                        continue;
                    }
                    collect_yaml(&path, &mut files);
                } else if path.extension().and_then(|e| e.to_str()) == Some("yaml") {
                    files.push(path);
                }
            }
        }
        assert!(!files.is_empty(), "未找到任何交付清单，路径解析有误");
        files
    }

    fn collect_yaml(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_yaml(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("yaml") {
                out.push(path);
            }
        }
    }

    /// 集群内 registry 判据本身：`registry:2` 的 `:2` 是 tag 不是端口。
    #[test]
    fn in_cluster_predicate_reads_a_port_not_a_tag() {
        assert!(is_in_cluster_image("localhost:30500/cogneva:local"));
        assert!(is_in_cluster_image("127.0.0.1:5000/x/y:1"));
        assert!(!is_in_cluster_image("registry:2"));
        assert!(!is_in_cluster_image("postgres:16-alpine"));
        assert!(!is_in_cluster_image("quay.io/buildah/stable:latest"));
        assert!(!is_in_cluster_image("qdrant/qdrant:v1.13.4"));
        assert!(!is_in_cluster_image("getmeili/meilisearch:v1.10.3"));
    }

    /// helm 归属补标的 JSON 必须带齐三件套：键名打错 helm 一样拒，而报错只会说
    /// "归属校验失败"，与真正的错因（键名拼错）隔着好几层。
    #[test]
    fn helm_ownership_patch_carries_the_three_marks() {
        let patch: serde_json::Value =
            serde_json::from_str(&super::helm_ownership_patch()).expect("补标 JSON 不可解析");
        let meta = &patch["metadata"];
        assert_eq!(meta["labels"]["app.kubernetes.io/managed-by"], "Helm");
        assert_eq!(meta["annotations"]["meta.helm.sh/release-name"], "cogneva");
        assert_eq!(
            meta["annotations"]["meta.helm.sh/release-namespace"],
            "cogneva"
        );
    }
}

#[cfg(test)]
mod node_qos_wiring_tests {
    /// K3s reads `/etc/rancher/k3s/config.yaml` once, when the service first
    /// starts, so writing it after the install script is the same as never
    /// writing it: the node would come up with no reservation and no memory
    /// eviction threshold, and nothing on it would say so.
    #[test]
    fn the_node_qos_config_is_written_before_k3s_starts() {
        let src = include_str!("main.rs");
        for (path, write, start) in [
            (
                "async fn install_k3s(",
                "write_k3s_qos_config",
                "run_k3s_install_script",
            ),
            (
                "async fn install_k3s_agents(",
                "qos_remote_write",
                "run(\"ssh\"",
            ),
        ] {
            let body = src
                .split(path)
                .nth(1)
                .unwrap_or_else(|| panic!("{path} 不在 main.rs 里"));
            let write_at = body
                .find(write)
                .unwrap_or_else(|| panic!("{path} 没有调用 {write}"));
            let start_at = body
                .find(start)
                .unwrap_or_else(|| panic!("{path} 没有调用 {start}"));
            assert!(
                write_at < start_at,
                "{path} 里 {write} 必须排在 {start} 之前（配置只在首启时被读一次）"
            );
        }
    }
}
