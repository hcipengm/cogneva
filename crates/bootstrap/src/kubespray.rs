//! kubespray 标准 Kubernetes 集群供给。
//!
//! 元启动新建集群时，除了默认的 K3s，还可以用 kubespray 官方容器镜像新建
//! 标准 Kubernetes（即 K8s）。本模块只负责"集群底座"：生成声明式 inventory /
//! group_vars、预检依赖、跑 kubespray 容器、接好 kubeconfig。cogneva 应用拓扑
//! 仍只来自 Helm chart——kubespray 产出的集群被 main.rs 的 `detect_profile()`
//! 识别为标准 K8s，走 `deploy/rendered/k8s-standard/`，本模块不碰应用清单。
//!
//! 决策默认值（零交互，可被 env 覆盖）：
//! - etcd：stacked（控制面节点同机，master 进 `[etcd]` 组），单 CP 的标准默认；
//! - CNI：Calico（`COGNEVA_K8S_CNI=flannel` 可换）；
//! - 容器运行时：containerd；
//! - 存储：local-path-provisioner 建默认 StorageClass；
//! - K8s 版本：跟 kubespray 钉的版本（保证 kubelet/apiserver/CRI 对齐）。
//!
//! 多控制面 HA（需稳定 LB endpoint）不在本次范围。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::process::Command;
use tracing::{info, warn};

use super::{cn_mirror, command_exists, probe_alive, run};

/// kubespray 容器镜像 tag（跟随官方稳定版，它钉住经测试的 K8s 版本矩阵）。
pub(crate) const KUBESPRAY_TAG: &str = "v2.31.0";

/// 工作目录：inventory 与 group_vars 落这里，挂载进 kubespray 容器。
fn work_dir() -> PathBuf {
    PathBuf::from("/var/lib/cogneva-data/kubespray")
}

/// 解析后的节点 SSH 目标。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeSpec {
    /// inventory 里的主机名（ansible inventory alias）。
    name: String,
    host: String,
    user: String,
    port: Option<u16>,
}

/// 解析 "user@ip[:port]" / "ip[:port]" / "ip" 为节点规格（纯函数，便于单测）。
pub(crate) fn parse_node(name: impl Into<String>, target: &str) -> NodeSpec {
    let name = name.into();
    let (user_part, host_part) = match target.split_once('@') {
        Some((u, h)) => (u.to_string(), h.to_string()),
        None => ("root".to_string(), target.to_string()),
    };
    let (host, port) = match host_part.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            (h.to_string(), p.parse::<u16>().ok())
        }
        _ => (host_part, None),
    };
    NodeSpec {
        name,
        host,
        user: user_part,
        port,
    }
}

/// 渲染 kubespray inventory。
///
/// 本机（master-0）始终是唯一 control-plane + stacked etcd；`workers` 里声明的
/// 节点作 kube_node。单节点（无 worker）时 master-0 同时进 kube_node 组
/// （all-in-one）。所有节点经 SSH 连接：master 走 127.0.0.1（容器 `--network=host`
/// 后即宿主），worker 走各自 IP；因此预检要保证 root 密钥对本机与各节点免密可达。
pub(crate) fn render_inventory(workers: &[String]) -> String {
    let mut out = String::new();
    out.push_str("[all]\n");
    out.push_str("master-0 ansible_host=127.0.0.1 ansible_user=root\n");
    let specs: Vec<NodeSpec> = workers
        .iter()
        .enumerate()
        .map(|(i, t)| parse_node(format!("worker-{i}"), t))
        .collect();
    for s in &specs {
        let mut line = format!("{} ansible_host={} ansible_user={}", s.name, s.host, s.user);
        if let Some(p) = s.port {
            line.push_str(&format!(" ansible_port={p}"));
        }
        out.push_str(&line);
        out.push('\n');
    }
    out.push_str("\n[kube_control_plane]\nmaster-0\n");
    out.push_str("\n[etcd]\nmaster-0\n");
    out.push_str("\n[kube_node]\n");
    if specs.is_empty() {
        // all-in-one：控制面同时承载工作负载。
        out.push_str("master-0\n");
    } else {
        for s in &specs {
            out.push_str(&s.name);
            out.push('\n');
        }
    }
    out.push_str("\n[k8s_cluster:children]\nkube_control_plane\nkube_node\n");
    out
}

/// 渲染 group_vars/k8s_cluster/cogneva.yml。
///
/// 基础项给零决策默认值（containerd / CNI / local-path 默认 SC）；CN 受限网络
/// 追加镜像仓库覆写与 containerd registry mirror，让系统镜像不走直连。
pub(crate) fn render_group_vars(cn: bool, cni: &str) -> String {
    let mut y = String::new();
    y.push_str("---\n");
    y.push_str("# 由 cogneva 元启动生成：标准 K8s 集群供给（kubespray）。\n");
    y.push_str("container_manager: containerd\n");
    y.push_str(&format!("kube_network_plugin: {cni}\n"));
    y.push_str("local_path_provisioner_enabled: true\n");
    if cn {
        y.push_str("\n# CN 受限网络：系统镜像走国内镜像站（daocloud 多 registry 代理）。\n");
        y.push_str("kube_image_repo: k8s-gcr.m.daocloud.io\n");
        y.push_str("gcr_image_repo: gcr.m.daocloud.io\n");
        y.push_str("docker_image_repo: docker.m.daocloud.io\n");
        y.push_str("quay_image_repo: quay.m.daocloud.io\n");
        y.push_str("github_image_repo: ghcr.m.daocloud.io\n");
        // local-path helper 镜像默认写死 docker.io/library/busybox，需显式改。
        y.push_str(
            "local_path_provisioner_helper_image_repo: docker.m.daocloud.io/library/busybox\n",
        );
        y.push_str("containerd_registries_mirrors:\n");
        y.push_str("  - prefix: docker.io\n    mirrors:\n");
        y.push_str("      - host: https://docker.m.daocloud.io\n        capabilities: [\"pull\", \"resolve\"]\n");
        y.push_str("  - prefix: registry.k8s.io\n    mirrors:\n");
        y.push_str("      - host: https://k8s-gcr.m.daocloud.io\n        capabilities: [\"pull\", \"resolve\"]\n");
        y.push_str("  - prefix: quay.io\n    mirrors:\n");
        y.push_str("      - host: https://quay.m.daocloud.io\n        capabilities: [\"pull\", \"resolve\"]\n");
    }
    y
}

/// 选定 kubespray 容器镜像引用。
///
/// 默认 `quay.io/kubespray/kubespray:<tag>`；CN 模式在 quay 镜像站候选里探活
/// （复用 main.rs 的 probe_alive，5s 超时）。`COGNEVA_KUBESPRAY_IMAGE` 可整体覆盖。
pub(crate) async fn kubespray_image(cn: bool) -> String {
    if let Ok(img) = std::env::var("COGNEVA_KUBESPRAY_IMAGE") {
        if !img.trim().is_empty() {
            return img.trim().to_string();
        }
    }
    let default = format!("quay.io/kubespray/kubespray:{KUBESPRAY_TAG}");
    if !cn {
        return default;
    }
    // (选用镜像前缀, 探活 URL)。
    let candidates = [
        (
            format!("quay.m.daocloud.io/kubespray/kubespray:{KUBESPRAY_TAG}"),
            "https://quay.m.daocloud.io/v2/",
        ),
        (
            format!("quay.1ms.run/kubespray/kubespray:{KUBESPRAY_TAG}"),
            "https://quay.1ms.run/v2/",
        ),
    ];
    for (img, probe) in candidates {
        if probe_alive(probe).await {
            return img;
        }
        warn!("kubespray 镜像站不可达，换下一个: {probe}");
    }
    warn!("所有 kubespray 镜像站候选不可达，回退默认 {default}（拉取可能失败）");
    default
}

/// 确保本机有能"运行容器"的运行时（buildah 只能 build 不能 run）。
/// 返回命令名（podman 或 docker）。
pub(crate) async fn ensure_container_runner() -> Result<String> {
    if command_exists("podman").await {
        return Ok("podman".into());
    }
    if command_exists("docker").await {
        return Ok("docker".into());
    }
    info!("安装 podman（用于运行 kubespray 容器）...");
    run("apt-get", &["update"]).await?;
    run("apt-get", &["install", "-y", "podman"]).await?;
    Ok("podman".into())
}

/// 在目标上执行一条 shell 命令（本机 local，远端经 SSH 免密）。
async fn sh_on(target: Option<&str>, remote_cmd: &str) -> Result<()> {
    match target {
        None => {
            let status = Command::new("sh")
                .args(["-c", remote_cmd])
                .stdin(Stdio::null())
                .status()
                .await?;
            if !status.success() {
                bail!("本机命令失败: {remote_cmd}");
            }
            Ok(())
        }
        Some(node) => {
            let spec = parse_node("target", node);
            let mut args: Vec<String> = vec![
                "-o".into(),
                "BatchMode=yes".into(),
                "-o".into(),
                "ConnectTimeout=10".into(),
                "-o".into(),
                "StrictHostKeyChecking=accept-new".into(),
            ];
            if let Some(p) = spec.port {
                args.push("-p".into());
                args.push(p.to_string());
            }
            args.push(format!("{}@{}", spec.user, spec.host));
            args.push(remote_cmd.to_string());
            let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            run("ssh", &arg_refs)
                .await
                .with_context(|| format!("远端命令失败 {node}（需本机到目标 SSH 免密可达）"))?;
            Ok(())
        }
    }
}

/// 资源门禁用：检查某节点是否 SSH 免密可达（10s 超时，BatchMode 绝不交互）。
pub(crate) async fn node_ssh_reachable(target: &str) -> bool {
    let spec = parse_node("target", target);
    let mut args: Vec<String> = vec![
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
    ];
    if let Some(p) = spec.port {
        args.push("-p".into());
        args.push(p.to_string());
    }
    args.push(format!("{}@{}", spec.user, spec.host));
    args.push("true".into());
    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    Command::new("ssh")
        .args(arg_refs)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// 预检：python3（kubespray 目标节点依赖）+ 本机 SSH 自可达（all-in-one ansible
/// 经 127.0.0.1 连本机）。target 为 None 表示本机。
async fn ensure_python(target: Option<&str>) -> Result<()> {
    let ensure =
        "python3 --version >/dev/null 2>&1 || (apt-get update && apt-get install -y python3)";
    sh_on(target, ensure).await
}

/// 保证 root 能以密钥免密 SSH 到本机 127.0.0.1（kubespray 容器经 host 网络连本机）。
async fn ensure_localhost_ssh() -> Result<()> {
    // 确保 sshd 在跑。
    sh_on(
        None,
        "command -v sshd >/dev/null 2>&1 || (apt-get update && apt-get install -y openssh-server)",
    )
    .await?;
    sh_on(
        None,
        "(systemctl start ssh 2>/dev/null || service ssh start 2>/dev/null || true)",
    )
    .await?;
    // 确保有 root 密钥并授权本机。
    sh_on(
        None,
        "test -f /root/.ssh/id_rsa || ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa",
    )
    .await?;
    sh_on(
        None,
        "mkdir -p /root/.ssh && grep -qf /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys 2>/dev/null || cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys; chmod 600 /root/.ssh/authorized_keys",
    )
    .await?;
    // 验证免密自连。
    let ok = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "root@127.0.0.1",
            "true",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);
    if !ok {
        bail!("root 无法免密 SSH 到本机 127.0.0.1；kubespray all-in-one/单机控制面需要本机 sshd 允许 root 密钥登录");
    }
    Ok(())
}

/// 拉取 kubespray 镜像。
async fn pull_image(runner: &str, image: &str) -> Result<()> {
    info!("拉取 kubespray 镜像: {image}");
    run(runner, &["pull", image]).await?;
    Ok(())
}

/// 跑完 kubespray 后把 kubeconfig 接到 root 默认位置（标准 K8s 不写 K3s 的
/// KUBECONFIG），保证后续 kubectl 可用。
async fn wire_kubeconfig() -> Result<()> {
    let admin = Path::new("/etc/kubernetes/admin.conf");
    if !admin.exists() {
        bail!("kubespray 跑完但 /etc/kubernetes/admin.conf 不存在");
    }
    sh_on(
        None,
        "mkdir -p /root/.kube && cp /etc/kubernetes/admin.conf /root/.kube/config",
    )
    .await?;
    Ok(())
}

/// 供给标准 Kubernetes 集群：预检 → 渲染 inventory/group_vars → 跑 kubespray 容器
/// → 接 kubeconfig。`workers` 为 `COGNEVA_CLUSTER_NODES` 声明的工作节点（空=单节点）。
pub(crate) async fn run_kubespray(workers: &[String]) -> Result<()> {
    let cn = cn_mirror();
    let cni = std::env::var("COGNEVA_K8S_CNI").unwrap_or_else(|_| "calico".into());
    let runner = ensure_container_runner().await?;

    info!(
        "kubespray 预检：本机与 {0} 个工作节点的 python3 / SSH ...",
        workers.len()
    );
    ensure_python(None).await?;
    ensure_localhost_ssh().await?;
    for w in workers {
        ensure_python(Some(w)).await?;
    }

    let image = kubespray_image(cn).await;
    pull_image(&runner, &image).await?;

    let work = work_dir();
    tokio::fs::create_dir_all(work.join("group_vars/k8s_cluster"))
        .await
        .context("创建 kubespray 工作目录")?;
    tokio::fs::write(work.join("inventory.ini"), render_inventory(workers))
        .await
        .context("写 inventory.ini")?;
    tokio::fs::write(
        work.join("group_vars/k8s_cluster/cogneva.yml"),
        render_group_vars(cn, &cni),
    )
    .await
    .context("写 group_vars")?;

    info!("运行 kubespray {KUBESPRAY_TAG}（CNI={cni}, CN={cn}）部署标准 Kubernetes ...");
    let work_str = work.to_string_lossy().to_string();
    let args: Vec<String> = vec![
        "run".into(),
        "--rm".into(),
        "--network=host".into(),
        "-v".into(),
        format!("{work_str}:/inventory:ro"),
        "-v".into(),
        "/root/.ssh:/root/.ssh:ro".into(),
        image,
        "ansible-playbook".into(),
        "-i".into(),
        "/inventory/inventory.ini".into(),
        "cluster.yml".into(),
        "-b".into(),
    ];
    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    if let Err(e) = run(&runner, &arg_refs).await {
        bail!(
            "kubespray 部署失败：{e:#}。inventory/group_vars 在 {work_str}/，\
             可据此重跑 ansible-playbook 排障"
        );
    }

    wire_kubeconfig().await?;
    info!("标准 Kubernetes 集群就绪（kubespray），kubeconfig 已接 /root/.kube/config");
    // 给节点一点时间注册，main.rs 随后会 wait_all_nodes_ready。
    tokio::time::sleep(Duration::from_secs(5)).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_node_variants() {
        let a = parse_node("n", "root@10.0.0.7:2222");
        assert_eq!(
            a,
            NodeSpec {
                name: "n".into(),
                host: "10.0.0.7".into(),
                user: "root".into(),
                port: Some(2222),
            }
        );
        let b = parse_node("n", "ubuntu@10.0.0.8");
        assert_eq!(b.user, "ubuntu");
        assert_eq!(b.host, "10.0.0.8");
        assert_eq!(b.port, None);
        let c = parse_node("n", "10.0.0.9");
        assert_eq!(c.user, "root");
        assert_eq!(c.host, "10.0.0.9");
    }

    #[test]
    fn single_node_inventory_is_all_in_one() {
        let inv = render_inventory(&[]);
        assert!(inv.contains("master-0 ansible_host=127.0.0.1 ansible_user=root"));
        assert!(inv.contains("[kube_control_plane]\nmaster-0"));
        assert!(inv.contains("[etcd]\nmaster-0"));
        // 单节点：master 同时是 worker。
        assert!(inv.contains("[kube_node]\nmaster-0"));
        // 没有 worker 别名。
        assert!(!inv.contains("worker-0"));
    }

    #[test]
    fn multi_node_inventory_roles() {
        let inv = render_inventory(&["root@10.0.0.7:2200".into(), "ubuntu@10.0.0.8".into()]);
        assert!(inv.contains("worker-0 ansible_host=10.0.0.7 ansible_user=root ansible_port=2200"));
        assert!(inv.contains("worker-1 ansible_host=10.0.0.8 ansible_user=ubuntu"));
        assert!(inv.contains("[kube_node]\nworker-0\nworker-1"));
        // 多节点：master 不进 kube_node。
        let node_group = inv.split("[kube_node]").nth(1).unwrap();
        assert!(!node_group.contains("master-0"));
        assert!(inv.contains("[k8s_cluster:children]"));
    }

    #[test]
    fn group_vars_defaults() {
        let y = render_group_vars(false, "calico");
        assert!(y.contains("container_manager: containerd"));
        assert!(y.contains("kube_network_plugin: calico"));
        assert!(y.contains("local_path_provisioner_enabled: true"));
        // 非 CN 不带镜像覆写。
        assert!(!y.contains("m.daocloud.io"));
        assert!(!y.contains("containerd_registries_mirrors"));
    }

    #[test]
    fn group_vars_cn_has_mirrors_and_flannel_override() {
        let y = render_group_vars(true, "flannel");
        assert!(y.contains("kube_network_plugin: flannel"));
        assert!(y.contains("kube_image_repo: k8s-gcr.m.daocloud.io"));
        assert!(y.contains("docker_image_repo: docker.m.daocloud.io"));
        assert!(y.contains(
            "local_path_provisioner_helper_image_repo: docker.m.daocloud.io/library/busybox"
        ));
        assert!(y.contains("containerd_registries_mirrors"));
        assert!(y.contains("prefix: registry.k8s.io"));
        assert!(y.contains("prefix: quay.io"));
    }

    #[test]
    fn default_image_ref_uses_quay() {
        // 非 CN：直接拼默认引用（不走 async 探活）。
        assert_eq!(
            format!("quay.io/kubespray/kubespray:{KUBESPRAY_TAG}"),
            "quay.io/kubespray/kubespray:v2.31.0"
        );
    }
}
