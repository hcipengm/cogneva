//! Cogneva 元启动引导器（第二步：Rust 引导器配 LLM + 探测环境）。
//!
//! 职责（docs/2026-06-27_14-37-48_元启动实施计划.md §2.2 / §5）：
//! 1. 交互式配置 LLM（API Key 或本地模型路径）并验证连通性；
//! 2. 静默探测 CPU/内存/架构/节点；
//! 3. 规则引擎选择 K3s（轻量）或 K8s（生产）分支；
//! 4. 生成 intent_config.yaml；
//! 5. 安装 containerd / buildah / K3s（或复用现有集群）；
//! 6. 供给运行时镜像：优先下载预构建 release 包（sha256 校验后导入集群），
//!    不可用时回退从源码构建（K3s 分支；清单引用 localhost/cogneva:local）；
//! 7. kubectl apply 部署清单并等待关键 Pod Ready；
//! 8. 打印 WebUI 地址，清零内存中的 API Key 后退出（自毁）。
//!
//! 非交互模式：设置 COGNEVA_BOOTSTRAP_NONINTERACTIVE=1，并通过
//! COGNEVA_LLM_PROVIDER / COGNEVA_LLM_API_KEY / COGNEVA_LLM_BASE_URL 传入配置。

use std::io::{self, BufRead, Read, Write};
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
use serde::Serialize;
use tokio::process::Command;
use tracing::{info, warn};
use zeroize::Zeroize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Branch {
    K3s,
    K8s,
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
    llm_provider: String,
    llm_base_url: String,
    branch: Branch,
    hardware: Hardware,
}

fn prompt(label: &str, default: &str) -> Result<String> {
    print!("{label} [{default}]: ");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    let line = line.trim();
    Ok(if line.is_empty() {
        default.to_string()
    } else {
        line.to_string()
    })
}

/// 节点数探测：env COGNEVA_NODES 显式覆盖优先；集群已存在时按实际节点数；
/// 已声明 COGNEVA_CLUSTER_NODES 时按 server+agents 预期数；否则默认 1。
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
                return n;
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

/// 规则引擎：内存 < 2GB 或单节点 → K3s 轻量分支；多节点高配 → K8s 生产分支。
fn decide_branch(hw: &Hardware) -> Branch {
    if hw.mem_total_mb < 2048 || hw.nodes <= 1 {
        Branch::K3s
    } else {
        Branch::K8s
    }
}

async fn verify_llm(provider: &str, base_url: &str, api_key: &str) -> Result<()> {
    if api_key.is_empty() {
        warn!("未提供 API Key（本地模型模式），跳过连通性验证");
        return Ok(());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let base = base_url.trim_end_matches('/');
    match provider {
        "anthropic" => {
            let resp = client
                .post(format!("{base}/v1/messages"))
                .header("x-api-key", api_key)
                .header("anthropic-version", "2023-06-01")
                .json(&serde_json::json!({
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 1,
                    "messages": [{"role": "user", "content": "ping"}]
                }))
                .send()
                .await?;
            if !resp.status().is_success() {
                bail!("Anthropic 连通性验证失败: HTTP {}", resp.status());
            }
        }
        _ => {
            let resp = client
                .get(format!("{base}/models"))
                .bearer_auth(api_key)
                .send()
                .await?;
            if !resp.status().is_success() {
                bail!("OpenAI 兼容端点连通性验证失败: HTTP {}", resp.status());
            }
        }
    }
    info!("LLM 连通性验证通过");
    Ok(())
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

/// K8s 分支集群供给：多节点 K8s = 本机 K3s server + SSH 推送安装 agent
/// （K3s 是 CNCF 认证 K8s，多节点 K3s 即多节点 K8s）。已有可用集群时
/// 仅补齐声明中缺失的 agent；无集群且无节点声明 → 失败前置。
async fn ensure_multi_node_cluster() -> Result<()> {
    let agents = cluster_nodes_env();
    if !cluster_ready().await {
        if agents.is_empty() {
            bail!(
                "K8s 分支需要多节点集群：请用 COGNEVA_CLUSTER_NODES=user@ip[,user@ip2...] \
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
    let install = if cn_mirror() {
        "curl -fsSL https://rancher-mirror.rancher.cn/k3s/k3s-install.sh | INSTALL_K3S_MIRROR=cn sh -"
    } else {
        "curl -fsSL https://get.k3s.io | sh -"
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
        let remote = format!("K3S_URL={server_url} K3S_TOKEN={token} {install}");
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
async fn ensure_git_remote() -> Result<()> {
    let remote = Path::new("/var/lib/cogneva-data/git-remote");
    if remote.join("HEAD").exists() {
        info!("git-remote bare 仓库已存在，跳过");
        return Ok(());
    }
    let src = repo_root()
        .canonicalize()
        .context("源码目录无法解析为绝对路径")?;
    if !src.join(".git").exists() {
        // tarball 方式取得的源码没有版本历史：就地初始化一个仓库作为同步起点，
        // 否则 git clone --bare 必失败（evolution worker 的 hostPath 依赖它）
        info!("源码目录无 .git（tarball 安装），就地初始化仓库: {}", src.display());
        run("git", &["-C", &src.to_string_lossy(), "init", "-b", "main"]).await?;
        run("git", &["-C", &src.to_string_lossy(), "config", "user.email", "evolution@cogneva.local"]).await?;
        run("git", &["-C", &src.to_string_lossy(), "config", "user.name", "Cogneva Evolution"]).await?;
        run("git", &["-C", &src.to_string_lossy(), "add", "-A"]).await?;
        run("git", &["-C", &src.to_string_lossy(), "commit", "-m", "cogneva bootstrap: initial source snapshot"]).await?;
    }
    if let Some(parent) = remote.parent() {
        std::fs::create_dir_all(parent)?;
    }
    info!("初始化自进化 git 远程仓库: {} -> {}", src.display(), remote.display());
    run(
        "git",
        &[
            "clone",
            "--bare",
            &src.to_string_lossy(),
            &remote.to_string_lossy(),
        ],
    )
    .await?;
    Ok(())
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

/// 应用清单位置：K8s 分支同样复用 deploy/k3s（应用清单是集群无关的；
/// deploy/k8s 仅存生产周边参考与分发器模板，见该目录 README）。
fn manifest_dir(_branch: Branch) -> PathBuf {
    let root = std::env::var("COGNEVA_REPO_ROOT").unwrap_or_else(|_| ".".to_string());
    Path::new(&root).join("deploy/k3s")
}

fn repo_root() -> PathBuf {
    PathBuf::from(std::env::var("COGNEVA_REPO_ROOT").unwrap_or_else(|_| ".".to_string()))
}

/// 受限网络（CN）模式：由 bootstrap.sh 探测后通过 COGNEVA_CN_MIRROR 传入。
fn cn_mirror() -> bool {
    std::env::var("COGNEVA_CN_MIRROR").ok().as_deref() == Some("1")
}

/// 受限网络下为 buildah 配置 docker.io 镜像（Docker Hub 被墙，基础镜像拉取必挂）。
async fn ensure_buildah_mirror() -> Result<()> {
    if !cn_mirror() {
        return Ok(());
    }
    let dir = Path::new("/etc/containers/registries.conf.d");
    std::fs::create_dir_all(dir)?;
    std::fs::write(
        dir.join("cn-mirror.conf"),
        "unqualified-search-registries = [\"docker.io\"]\n\
         [[registry]]\n\
         prefix = \"docker.io\"\n\
         location = \"docker.m.daocloud.io\"\n",
    )?;
    info!("已配置 buildah docker.io 镜像（daocloud）");
    Ok(())
}

/// 运行时镜像供给（设计文档 §2.2 步骤 3 的前置）：清单引用 localhost/cogneva:local。
/// 优先从 GitHub/Gitee release 下载预构建镜像（sha256 校验），失败回退源码构建
/// （空白机全量 Rust release 构建需 1-3 小时，预构建下载仅需数分钟）。
/// K3s 单节点导入本机 containerd；K8s 多节点经镜像分发器逐节点导入。
async fn ensure_runtime_image(branch: Branch) -> Result<()> {
    const IMAGE: &str = "localhost/cogneva:local";
    if branch == Branch::K8s {
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
        run(
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
        .await?;
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
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&workdir);
    result
}

/// K8s 多节点镜像供给：本机导入只覆盖单节点，多节点必须让每个节点的
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
    let manifest = render_distributor_manifest()?;
    let mdir = make_workdir("distributor")?;
    let mpath = mdir.join("image-distributor.yaml");
    std::fs::write(&mpath, &manifest)?;
    let mstr = mpath.to_string_lossy().into_owned();

    let run_result = async {
        // 命名空间可能尚不存在（K8s 分支在 deploy_manifests 之前执行）
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
        // 等服务 Pod Ready 才能 cp
        run(
            "kubectl",
            &[
                "-n", "cogneva", "wait", "--for=condition=Ready", "pod/cogneva-image-server",
                "--timeout=120s",
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
fn render_distributor_manifest() -> Result<String> {
    let template = include_str!("../../../deploy/k8s/image-distributor.yaml");
    let busybox = if cn_mirror() {
        "docker.m.daocloud.io/library/busybox:latest"
    } else {
        "docker.io/library/busybox:latest"
    };
    Ok(template.replace("__BUSYBOX_IMAGE__", busybox))
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
    let mut build_args: Vec<String> = vec![
        "build".into(),
        "-t".into(),
        image.into(),
        "-f".into(),
        root.join("Dockerfile").to_string_lossy().into_owned(),
    ];
    if cn_mirror() {
        build_args.extend([
            // TUNA 不镜像按版本 channel，CN 模式工具链只能用 stable
            "--build-arg".into(),
            "RUST_TOOLCHAIN=stable".into(),
            "--build-arg".into(),
            "RUSTUP_DIST_SERVER=https://mirrors.tuna.tsinghua.edu.cn/rustup".into(),
            "--build-arg".into(),
            "RUSTUP_UPDATE_ROOT=https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup".into(),
            "--build-arg".into(),
            // crates 走 rsproxy 而非 TUNA：TUNA 稀疏索引的 dl 仍指向
            // static.crates.io，crate 文件直连国外会超时，rsproxy 索引与文件都自托管
            "CARGO_REGISTRY_SPARSE=https://rsproxy.cn/index/".into(),
            "--build-arg".into(),
            "APT_MIRROR_HOST=mirrors.tuna.tsinghua.edu.cn".into(),
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

async fn deploy_manifests(branch: Branch) -> Result<()> {
    let dir = manifest_dir(branch);
    if !dir.is_dir() {
        bail!("清单目录不存在: {}", dir.display());
    }
    let rendered = render_manifests_for_cluster(&dir).await?;
    info!("apply 清单（已按集群环境适配）");
    run("kubectl", &["apply", "-f", &rendered.to_string_lossy()]).await
}

/// 按集群环境渲染清单副本：
/// - 集群无 local-path provisioner（通用 K8s）→ 去掉 cogneva-local-retain
///   引用，PVC 回落集群默认 StorageClass；
/// - CN 模式 → 公开镜像统一加 daocloud 前缀（Docker Hub 被墙）。
///
/// 返回渲染后的目录（调用方不删，kubectl apply 后即弃）。
async fn render_manifests_for_cluster(dir: &Path) -> Result<PathBuf> {
    let has_local_path = Command::new("kubectl")
        .args(["get", "sc", "local-path"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);
    let cn = cn_mirror();
    if has_local_path && !cn {
        return Ok(dir.to_path_buf());
    }
    if !has_local_path {
        info!("集群无 local-path provisioner，PVC 将使用集群默认 StorageClass");
    }
    let out = make_workdir("manifests")?;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let mut text = std::fs::read_to_string(&path)?;
        if !has_local_path {
            text = text
                .lines()
                .filter(|l| !l.contains("storageClassName: cogneva-local-retain"))
                .collect::<Vec<_>>()
                .join("\n");
        }
        if cn {
            for (from, to) in CN_IMAGE_MAP {
                text = text.replace(from, to);
            }
        }
        std::fs::write(out.join(entry.file_name()), text)?;
    }
    // observability 子目录同样处理
    let sub = dir.join("observability");
    if sub.is_dir() {
        let out_sub = out.join("observability");
        std::fs::create_dir_all(&out_sub)?;
        for entry in std::fs::read_dir(&sub)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            let mut text = std::fs::read_to_string(&path)?;
            if cn {
                for (from, to) in CN_IMAGE_MAP {
                    text = text.replace(from, to);
                }
            }
            std::fs::write(out_sub.join(entry.file_name()), text)?;
        }
    }
    Ok(out)
}

/// CN 模式公开镜像替换表（daocloud 镜像站）。
const CN_IMAGE_MAP: &[(&str, &str)] = &[
    (
        "image: mysql:8.0",
        "image: docker.m.daocloud.io/library/mysql:8.0",
    ),
    (
        "image: nats:2.10-alpine",
        "image: docker.m.daocloud.io/library/nats:2.10-alpine",
    ),
    (
        "image: postgres:16-alpine",
        "image: docker.m.daocloud.io/library/postgres:16-alpine",
    ),
    (
        "image: redis:7-alpine",
        "image: docker.m.daocloud.io/library/redis:7-alpine",
    ),
    (
        "image: qdrant/qdrant:",
        "image: docker.m.daocloud.io/qdrant/qdrant:",
    ),
    (
        "image: quay.io/buildah/stable:",
        "image: quay.m.daocloud.io/buildah/stable:",
    ),
];

/// 将 LLM API Key 注入 K8s Secret（仅安全网关挂载，沙盒零凭证）。
async fn inject_llm_secret(api_key: &str) -> Result<()> {
    info!("注入 LLM API Key 到 cogneva-secrets");
    let patch = format!(r#"{{"stringData":{{"llm-api-key":"{api_key}"}}}}"#);
    run(
        "kubectl",
        &[
            "-n",
            "cogneva",
            "patch",
            "secret",
            "cogneva-secrets",
            "--type=merge",
            "-p",
            &patch,
        ],
    )
    .await
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

async fn configure_llm(noninteractive: bool) -> Result<(String, String, String)> {
    if noninteractive {
        let provider = std::env::var("COGNEVA_LLM_PROVIDER").unwrap_or_else(|_| "openai".into());
        let base_url = std::env::var("COGNEVA_LLM_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com/v1".into());
        let api_key = std::env::var("COGNEVA_LLM_API_KEY").unwrap_or_default();
        return Ok((provider, base_url, api_key));
    }
    let provider = prompt("LLM provider (openai/anthropic/local)", "openai")?;
    let default_base = match provider.as_str() {
        "anthropic" => "https://api.anthropic.com",
        "local" => "http://localhost:11434/v1",
        _ => "https://api.openai.com/v1",
    };
    let base_url = prompt("LLM base URL", default_base)?;
    let api_key = prompt("LLM API Key（本地模型可留空）", "")?;
    Ok((provider, base_url, api_key))
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

    let (provider, base_url, mut api_key) = configure_llm(noninteractive).await?;
    verify_llm(&provider, &base_url, &api_key).await?;

    let hw = probe_hardware().await;
    info!(
        "硬件探测: {} 核 / {} MiB / {} / {} 节点",
        hw.cpu_cores, hw.mem_total_mb, hw.arch, hw.nodes
    );
    let branch = decide_branch(&hw);
    info!("规则引擎决策: {:?} 分支", branch);

    // API Key 只存在于内存，绝不落盘；稍后通过 kubectl 注入 K8s Secret
    let intent = IntentConfig {
        llm_provider: provider,
        llm_base_url: base_url,
        branch,
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
    );
    let plan_path =
        std::env::var("COGNEVA_MANAGEMENT_PLAN").unwrap_or_else(|_| "management_plan.yaml".into());
    std::fs::write(&plan_path, plan.to_yaml()?)?;
    info!("已生成 {plan_path}（backend 自动选择已内嵌）");

    match branch {
        Branch::K3s => install_k3s().await?,
        Branch::K8s => ensure_multi_node_cluster().await?,
    }
    ensure_buildah().await?;
    ensure_buildah_mirror().await?;
    ensure_firecracker().await?;
    ensure_git_remote().await?;
    ensure_runtime_image(branch).await?;
    deploy_manifests(branch).await?;
    if !api_key.is_empty() {
        inject_llm_secret(&api_key).await?;
    }
    wait_ready().await?;

    let webui =
        std::env::var("COGNEVA_WEBUI_URL").unwrap_or_else(|_| "http://localhost:8080".into());
    info!("部署完成，WebUI 地址: {webui}");
    if !noninteractive {
        open_browser(&webui).await;
    }

    // 自毁：清零内存中的 API Key
    api_key.zeroize();
    info!("引导器使命完成，退出");
    Ok(())
}
