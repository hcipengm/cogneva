//! Firecracker/KVM 微虚拟机沙盒（审计 2.5.4）。
//!
//! 实现「冷启动 → 挂载 PV → 执行进化 → 销毁」的阅后即焚流程
//! （docs/2026-06-26_21-23-41_agent沙盒安全.md §4）：
//!
//! 1. **冷启动**：每次进化从原始 rootfs 复制 COW 副本，全新 boot，无历史残留；
//! 2. **挂载 PV**：持久化卷镜像以 RW 直接挂载（Retain 语义），进化产物写 PV；
//! 3. **执行进化**：guest init（boot_args 指定）从 PV 读取任务并执行；
//! 4. **阅后即焚**：guest 关机或超时后 kill firecracker、删除实例目录与
//!    rootfs 副本；PV 镜像保留，供下一次冷启动的新 VM 接管。
//!
//! 与 K8s Pod 沙盒互补：`microvm.enabled=true` 时进化执行面升级为硬件级
//! 隔离；凭证（LLM API Key 等）永远不进 VM，由安全网关在边界代理。

use std::path::{Path, PathBuf};
use std::time::Duration;

use cog_core::{MicroVmConfig, SFError, SFResult};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn};

/// Firecracker 实例句柄。
pub struct MicroVm {
    pub id: String,
    pub socket_path: PathBuf,
    pub instance_dir: PathBuf,
    child: tokio::process::Child,
}

/// 一次进化执行的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroVmOutcome {
    pub vm_id: String,
    /// true = guest 正常关机（init 完成）；false = 超时强制销毁。
    pub completed: bool,
    pub duration_secs: u64,
}

/// Firecracker/KVM 微虚拟机沙盒编排器。
pub struct FirecrackerSandbox {
    config: MicroVmConfig,
}

impl FirecrackerSandbox {
    pub fn new(config: MicroVmConfig) -> Self {
        Self { config }
    }

    /// 宿主机预检：/dev/kvm 与必需镜像是否存在。
    pub fn preflight(&self) -> SFResult<()> {
        if !Path::new("/dev/kvm").exists() {
            return Err(SFError::Validation(
                "microvm.enabled=true 但 /dev/kvm 不存在：KVM 不可用".into(),
            ));
        }
        for (label, p) in [
            ("kernel_image", &self.config.kernel_image),
            ("rootfs_image", &self.config.rootfs_image),
            ("pv_image", &self.config.pv_image),
        ] {
            if !Path::new(p).exists() {
                return Err(SFError::Validation(format!("microvm {label} 不存在: {p}")));
            }
        }
        Ok(())
    }

    /// 冷启动：建实例目录 → COW 复制 rootfs → spawn firecracker →
    /// 通过 API 配置 boot-source/drives/machine → InstanceStart。
    pub async fn cold_start(&self) -> SFResult<MicroVm> {
        let id = format!("evo-{}", uuid::Uuid::new_v4().simple());
        let instance_dir = Path::new(&self.config.instance_root).join(&id);
        tokio::fs::create_dir_all(&instance_dir)
            .await
            .map_err(|e| {
                SFError::IO(format!(
                    "create instance dir {}: {e}",
                    instance_dir.display()
                ))
            })?;

        // rootfs COW 副本：原始镜像只读复用，副本阅后即焚。
        let rootfs_copy = instance_dir.join("rootfs.ext4");
        copy_cow(&self.config.rootfs_image, &rootfs_copy).await?;

        let socket_path = instance_dir.join("firecracker.socket");
        let child = tokio::process::Command::new(&self.config.firecracker_bin)
            .arg("--api-sock")
            .arg(&socket_path)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| SFError::IO(format!("spawn {}: {e}", self.config.firecracker_bin)))?;

        // 等 API socket 出现（firecracker 启动即建）
        let socket_ready = wait_for_path(&socket_path, Duration::from_secs(5)).await;
        if !socket_ready {
            return Err(SFError::IO(format!(
                "firecracker API socket 未就绪: {}",
                socket_path.display()
            )));
        }

        let vm = MicroVm {
            id,
            socket_path,
            instance_dir,
            child,
        };
        self.configure_and_start(&vm, &rootfs_copy).await?;
        info!(vm_id = %vm.id, "microvm cold start complete");
        Ok(vm)
    }

    /// 配置并启动 guest：boot-source、rootfs（RW COW 副本）、PV（RW Retain）、
    /// machine-config，最后 InstanceStart。
    async fn configure_and_start(&self, vm: &MicroVm, rootfs_copy: &Path) -> SFResult<()> {
        let api = FcApiClient::new(&vm.socket_path);
        api.put(
            "/boot-source",
            serde_json::json!({
                "kernel_image_path": self.config.kernel_image,
                "boot_args": self.config.boot_args,
            }),
        )
        .await?;
        api.put(
            "/drives/rootfs",
            serde_json::json!({
                "drive_id": "rootfs",
                "path_on_host": rootfs_copy,
                "is_root_device": true,
                "is_read_only": false,
            }),
        )
        .await?;
        // PV 直接挂载（不复制）：Retain 语义，VM 销毁后进化产物保留。
        api.put(
            "/drives/evolution-pv",
            serde_json::json!({
                "drive_id": "evolution-pv",
                "path_on_host": self.config.pv_image,
                "is_root_device": false,
                "is_read_only": false,
            }),
        )
        .await?;
        api.put(
            "/machine-config",
            serde_json::json!({
                "vcpu_count": self.config.vcpu_count,
                "mem_size_mib": self.config.mem_size_mib,
            }),
        )
        .await?;
        api.put(
            "/actions",
            serde_json::json!({"action_type": "InstanceStart"}),
        )
        .await?;
        Ok(())
    }

    /// 完整编排：冷启动 → 等待 guest 完成（或超时）→ 阅后即焚。
    pub async fn run_evolution(&self) -> SFResult<MicroVmOutcome> {
        let begin = std::time::Instant::now();
        let mut vm = self.cold_start().await?;

        let timeout = Duration::from_secs(self.config.exec_timeout_secs);
        let completed = match tokio::time::timeout(timeout, vm.child.wait()).await {
            Ok(Ok(status)) => {
                if !status.success() {
                    warn!(vm_id = %vm.id, ?status, "firecracker exited abnormally");
                }
                true
            }
            Ok(Err(e)) => {
                self.destroy(vm).await.ok();
                return Err(SFError::IO(format!("wait firecracker: {e}")));
            }
            Err(_) => {
                warn!(vm_id = %vm.id, "microvm exec timeout; destroying");
                false
            }
        };

        let outcome = MicroVmOutcome {
            vm_id: vm.id.clone(),
            completed,
            duration_secs: begin.elapsed().as_secs(),
        };
        self.destroy(vm).await?;
        Ok(outcome)
    }

    /// 阅后即焚：kill firecracker、删除实例目录（含 rootfs 副本与 API
    /// socket）。PV 镜像不在实例目录内，自然保留。
    pub async fn destroy(&self, mut vm: MicroVm) -> SFResult<()> {
        if let Err(e) = vm.child.kill().await {
            warn!(vm_id = %vm.id, error = %e, "kill firecracker failed (may已退出)");
        }
        if let Err(e) = tokio::fs::remove_dir_all(&vm.instance_dir).await {
            warn!(vm_id = %vm.id, error = %e, "instance dir cleanup failed");
        }
        info!(vm_id = %vm.id, "microvm destroyed (PV retained)");
        Ok(())
    }
}

/// 复制文件，优先 reflink（btrfs/xfs COW），失败退回普通复制。
async fn copy_cow(src: &str, dst: &Path) -> SFResult<()> {
    let status = tokio::process::Command::new("cp")
        .arg("--reflink=auto")
        .arg(src)
        .arg(dst)
        .output()
        .await
        .map_err(|e| SFError::IO(format!("cp --reflink=auto {src}: {e}")))?;
    if !status.status.success() {
        return Err(SFError::IO(format!(
            "copy rootfs {src} -> {} failed: {}",
            dst.display(),
            String::from_utf8_lossy(&status.stderr)
        )));
    }
    Ok(())
}

async fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Firecracker REST API 的最小客户端：HTTP/1.1 over Unix socket。
/// 不引入新依赖；Firecracker API 无 keep-alive 需求，Connection: close。
struct FcApiClient {
    socket: PathBuf,
}

impl FcApiClient {
    fn new(socket: &Path) -> Self {
        Self {
            socket: socket.to_path_buf(),
        }
    }

    async fn put(&self, path: &str, body: serde_json::Value) -> SFResult<()> {
        let (status, resp) = self.request("PUT", path, Some(body)).await?;
        if !(200..300).contains(&status) {
            return Err(SFError::IO(format!(
                "firecracker PUT {path} -> {status}: {resp}"
            )));
        }
        Ok(())
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> SFResult<(u16, String)> {
        let payload = body
            .map(|b| serde_json::to_string(&b).unwrap_or_default())
            .unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        );

        let mut stream = tokio::net::UnixStream::connect(&self.socket)
            .await
            .map_err(|e| SFError::IO(format!("connect {}: {e}", self.socket.display())))?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("write firecracker api: {e}")))?;

        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .await
            .map_err(|e| SFError::IO(format!("read firecracker api: {e}")))?;
        let text = String::from_utf8_lossy(&raw);
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u16>().ok())
            .ok_or_else(|| SFError::IO(format!("malformed firecracker response: {text}")))?;
        let body_start = text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(text.len());
        Ok((status, text[body_start..].to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 假 firecracker API server：记录请求序列并按序返回 204。
    async fn fake_api_server(socket: &Path, log: PathBuf) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::UnixListener::bind(socket).unwrap();
        tokio::spawn(async move {
            // 5 个配置请求 + InstanceStart
            for _ in 0..5 {
                if let Ok((mut conn, _)) = listener.accept().await {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    // Connection: close → 客户端写完即半关闭
                    loop {
                        match conn.read(&mut chunk).await {
                            Ok(0) => break,
                            Ok(n) => {
                                buf.extend_from_slice(&chunk[..n]);
                                if String::from_utf8_lossy(&buf).contains("\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    let req = String::from_utf8_lossy(&buf);
                    let line = req.lines().next().unwrap_or("").to_string();
                    let mut f = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log)
                        .unwrap();
                    use std::io::Write;
                    writeln!(f, "{line}").unwrap();
                    conn.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await
                        .ok();
                }
            }
        })
    }

    fn test_config(dir: &Path) -> MicroVmConfig {
        for name in ["vmlinux", "rootfs.ext4", "pv.ext4"] {
            std::fs::write(dir.join(name), b"fake").unwrap();
        }
        MicroVmConfig {
            enabled: true,
            firecracker_bin: "true".into(), // 立即退出的假二进制
            kernel_image: dir.join("vmlinux").to_string_lossy().into(),
            rootfs_image: dir.join("rootfs.ext4").to_string_lossy().into(),
            pv_image: dir.join("pv.ext4").to_string_lossy().into(),
            vcpu_count: 2,
            mem_size_mib: 1024,
            boot_args: "init=/evolution/init".into(),
            instance_root: dir.join("instances").to_string_lossy().into(),
            exec_timeout_secs: 5,
        }
    }

    #[tokio::test]
    async fn configure_and_start_issues_api_sequence() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("fc.sock");
        let log = tmp.path().join("api.log");
        let server = fake_api_server(&socket, log.clone()).await;

        let cfg = test_config(tmp.path());
        let sandbox = FirecrackerSandbox::new(cfg);
        let vm = MicroVm {
            id: "test".into(),
            socket_path: socket.clone(),
            instance_dir: tmp.path().to_path_buf(),
            child: tokio::process::Command::new("true").spawn().unwrap(),
        };
        let rootfs = tmp.path().join("rootfs-copy.ext4");
        std::fs::write(&rootfs, b"copy").unwrap();

        sandbox.configure_and_start(&vm, &rootfs).await.unwrap();
        server.await.unwrap();

        let calls = std::fs::read_to_string(&log).unwrap();
        let lines: Vec<&str> = calls.lines().collect();
        assert_eq!(lines.len(), 5);
        assert!(lines[0].starts_with("PUT /boot-source"));
        assert!(lines[1].starts_with("PUT /drives/rootfs"));
        assert!(lines[2].starts_with("PUT /drives/evolution-pv"));
        assert!(lines[3].starts_with("PUT /machine-config"));
        assert!(lines[4].starts_with("PUT /actions"));
    }

    #[tokio::test]
    async fn api_client_surfaces_error_status() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("fc.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            if let Ok((mut conn, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = conn.read(&mut buf).await;
                conn.write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 11\r\nConnection: close\r\n\r\nbad request",
                )
                .await
                .ok();
            }
        });

        let api = FcApiClient::new(&socket);
        let err = api
            .put("/boot-source", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("400"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn destroy_removes_instance_dir_and_retains_pv() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        let pv_path = cfg.pv_image.clone();
        let sandbox = FirecrackerSandbox::new(cfg);
        let instance_dir = tmp.path().join("instances").join("vm-x");
        std::fs::create_dir_all(&instance_dir).unwrap();
        std::fs::write(instance_dir.join("rootfs.ext4"), b"copy").unwrap();

        let vm = MicroVm {
            id: "vm-x".into(),
            socket_path: instance_dir.join("fc.sock"),
            instance_dir: instance_dir.clone(),
            child: tokio::process::Command::new("sleep")
                .arg("30")
                .spawn()
                .unwrap(),
        };
        sandbox.destroy(vm).await.unwrap();
        assert!(!instance_dir.exists(), "实例目录应阅后即焚");
        assert!(Path::new(&pv_path).exists(), "PV 镜像必须保留");
    }

    #[test]
    fn preflight_reports_missing_kvm_and_images() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = test_config(tmp.path());
        cfg.kernel_image = tmp.path().join("missing-vmlinux").to_string_lossy().into();
        let sandbox = FirecrackerSandbox::new(cfg);
        let err = sandbox.preflight().unwrap_err().to_string();
        // /dev/kvm 与缺失镜像都可能先触发，二者皆可
        assert!(err.contains("/dev/kvm") || err.contains("kernel_image"));
    }
}
