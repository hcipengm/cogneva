//! Cogneva 元启动引导器（第二步：Rust 引导器配 LLM + 探测环境）。
//!
//! 职责（docs/2026-06-27_14-37-48_元启动实施计划.md §2.2 / §5）：
//! 1. 交互式配置 LLM（API Key 或本地模型路径）并验证连通性；
//! 2. 静默探测 CPU/内存/架构/节点；
//! 3. 规则引擎选择 K3s（轻量）或 K8s（生产）分支；
//! 4. 生成 intent_config.yaml；
//! 5. 安装 containerd / buildah / K3s（或复用现有集群）；
//! 6. kubectl apply 部署清单并等待关键 Pod Ready；
//! 7. 打印 WebUI 地址，清零内存中的 API Key 后退出（自毁）。
//!
//! 非交互模式：设置 COGNEVA_BOOTSTRAP_NONINTERACTIVE=1，并通过
//! COGNEVA_LLM_PROVIDER / COGNEVA_LLM_API_KEY / COGNEVA_LLM_BASE_URL 传入配置。

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

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

fn probe_hardware() -> Hardware {
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
        nodes: 1,
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
    let status = Command::new("sh")
        .args(["-c", "curl -fsSL https://get.k3s.io | sh -"])
        .status()
        .await?;
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

/// Firecracker/KVM 微虚拟机沙盒（审计 2.5.4）：KVM 可用且未安装时，从官方
/// release 安装 firecracker。best-effort：无 KVM（无嵌套虚拟化的云主机）
/// 或安装失败时告警并继续，沙盒保持 K8s Pod 形态，不阻塞主部署。
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
        "set -e; cd /tmp && curl -fsSL '{url}' -o firecracker.tgz && tar -xzf firecracker.tgz && \
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

fn manifest_dir(branch: Branch) -> PathBuf {
    let root = std::env::var("COGNEVA_REPO_ROOT").unwrap_or_else(|_| ".".to_string());
    match branch {
        Branch::K3s => Path::new(&root).join("deploy/k3s"),
        Branch::K8s => Path::new(&root).join("deploy/k8s"),
    }
}

async fn deploy_manifests(branch: Branch) -> Result<()> {
    let dir = manifest_dir(branch);
    if !dir.is_dir() {
        bail!("清单目录不存在: {}", dir.display());
    }
    info!("apply 清单目录 {}", dir.display());
    run("kubectl", &["apply", "-f", &dir.to_string_lossy()]).await
}

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

    let hw = probe_hardware();
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
        Branch::K8s => {
            if !cluster_ready().await {
                bail!("K8s 分支要求已存在可用集群（kubectl 可连通）");
            }
        }
    }
    ensure_buildah().await?;
    ensure_firecracker().await?;
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
