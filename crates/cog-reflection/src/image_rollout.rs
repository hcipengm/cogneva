//! Image-based 滚动更新部署器（审计 3.2 剩余项）。
//!
//! 启用 `self_evolution.image_rollout` 后，change 部署路径从特权 Pod
//! `self_exec` 二进制替换升级为：
//!
//! 1. 基于已编译 staged 二进制生成最小 Containerfile 并构建镜像
//!    （tag = `{image_repo}:change-{change_id}`，change_id 经字符白名单消毒）；
//! 2. 可选 `<builder> push`（k3s 节点本地镜像可关）；
//! 3. `kubectl set image` change 目标 Deployment；
//! 4. `kubectl rollout status` 等待滚动完成；失败自动 `rollout undo` 回滚。
//!
//! 所有外部命令带超时；任何一步失败都会尽量回滚滚动更新。

use std::time::Duration;

use cog_core::{ImageRolloutConfig, SFError, SFResult};
use tracing::{info, warn};

pub struct ImageRollout {
    config: ImageRolloutConfig,
}

impl ImageRollout {
    pub fn new(config: ImageRolloutConfig) -> Self {
        Self { config }
    }

    /// 镜像 tag：change_id 只保留 [A-Za-z0-9-_]，其余替换为 '-'，
    /// 避免命令注入与非法 tag 字符。
    pub fn image_tag(&self, change_id: &str) -> String {
        let sanitized: String = change_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        format!("{}:change-{}", self.config.image_repo, sanitized)
    }

    async fn run(&self, program: &str, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let cmdline = format!("{} {}", program, args.join(" "));
        let fut = tokio::process::Command::new(program)
            .args(args)
            .kill_on_drop(true)
            .output();
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

    /// 在 staged 二进制同目录生成最小 Containerfile 并返回构建上下文目录。
    async fn write_containerfile(
        &self,
        binary_path: &std::path::Path,
    ) -> SFResult<std::path::PathBuf> {
        let context_dir = binary_path.parent().ok_or_else(|| {
            SFError::Validation(format!(
                "staged binary {} has no parent dir",
                binary_path.display()
            ))
        })?;
        let binary_name = binary_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                SFError::Validation(format!(
                    "staged binary {} has no file name",
                    binary_path.display()
                ))
            })?;

        let containerfile = format!(
            "FROM {}\nCOPY {} /opt/cogneva/cogneva\nENTRYPOINT [\"/opt/cogneva/cogneva\"]\n",
            self.config.base_image, binary_name
        );
        let path = context_dir.join("Containerfile.rollout");
        tokio::fs::write(&path, containerfile)
            .await
            .map_err(|e| SFError::IO(format!("failed to write {}: {}", path.display(), e)))?;
        Ok(context_dir.to_path_buf())
    }

    /// 执行完整滚动更新；成功返回镜像 tag。
    pub async fn deploy(
        &self,
        artifact: &crate::evolution_deployer::BuildArtifact,
    ) -> SFResult<String> {
        let tag = self.image_tag(&artifact.change_id);
        let context_dir = self.write_containerfile(&artifact.new_binary_path).await?;
        let context = context_dir.to_string_lossy().to_string();
        let containerfile_path = context_dir.join("Containerfile.rollout");
        let containerfile = containerfile_path.to_string_lossy().to_string();
        let timeout = self.config.rollout_timeout_secs;

        // 1. 构建镜像
        self.run(
            &self.config.builder_bin,
            &["build", "-t", &tag, "-f", &containerfile, &context],
            timeout,
        )
        .await?;
        info!(tag = %tag, "rollout image built");

        // 2. 可选推送
        if self.config.registry_push {
            self.run(&self.config.builder_bin, &["push", &tag], timeout)
                .await?;
            info!(tag = %tag, "rollout image pushed");
        }

        // 3. change Deployment 镜像
        let deploy_ref = format!("deployment/{}", self.config.deployment);
        let image_arg = format!("{}={}", self.config.container, tag);
        self.run(
            &self.config.kubectl_bin,
            &[
                "-n",
                &self.config.namespace,
                "set",
                "image",
                &deploy_ref,
                &image_arg,
            ],
            60,
        )
        .await?;
        info!(deployment = %deploy_ref, tag = %tag, "deployment image updated");

        // 4. 等待滚动完成；失败自动 undo
        let status_timeout = format!("{}s", timeout);
        if let Err(e) = self
            .run(
                &self.config.kubectl_bin,
                &[
                    "-n",
                    &self.config.namespace,
                    "rollout",
                    "status",
                    &deploy_ref,
                    &format!("--timeout={}", status_timeout),
                ],
                timeout + 30,
            )
            .await
        {
            warn!(error = %e, "rollout status failed; rolling back");
            if let Err(rb) = self
                .run(
                    &self.config.kubectl_bin,
                    &["-n", &self.config.namespace, "rollout", "undo", &deploy_ref],
                    120,
                )
                .await
            {
                warn!(error = %rb, "rollout undo failed");
            }
            return Err(e);
        }

        Ok(tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn write_fake_bin(dir: &std::path::Path, name: &str, script: &str) {
        let path = dir.join(name);
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn artifact(dir: &std::path::Path) -> crate::evolution_deployer::BuildArtifact {
        let binary = dir.join("cogneva");
        std::fs::write(&binary, b"fake-binary").unwrap();
        crate::evolution_deployer::BuildArtifact {
            change_id: "p-1".into(),
            commit_hash: "abc1234".into(),
            new_binary_path: binary,
            build_duration_secs: 1,
        }
    }

    fn rollout_config(dir: &std::path::Path, rollout_ok: bool) -> ImageRolloutConfig {
        // log 路径直接写进脚本：测试线程共享进程 env，用 $FAKE_LOG 会竞态。
        let log = dir.join("fake.log").to_string_lossy().to_string();
        // fake builder：记录参数并成功
        write_fake_bin(
            dir,
            "fake-builder",
            &format!("#!/bin/sh\necho \"$@\" >> '{log}'\nexit 0\n"),
        );
        // fake kubectl：rollout status 按 rollout_ok 决定成败，其余记录参数
        let kubectl_script = if rollout_ok {
            format!("#!/bin/sh\necho \"$@\" >> '{log}'\nexit 0\n")
        } else {
            format!(
                "#!/bin/sh\necho \"$@\" >> '{log}'\ncase \"$*\" in *\"rollout status\"*) exit 1;; esac\nexit 0\n"
            )
        };
        write_fake_bin(dir, "fake-kubectl", &kubectl_script);

        ImageRolloutConfig {
            enabled: true,
            image_repo: "localhost/cogneva".into(),
            base_image: "debian:bookworm-slim".into(),
            builder_bin: dir.join("fake-builder").to_string_lossy().to_string(),
            registry_push: true,
            kubectl_bin: dir.join("fake-kubectl").to_string_lossy().to_string(),
            namespace: "cogneva".into(),
            deployment: "cogneva".into(),
            container: "cogneva".into(),
            rollout_timeout_secs: 30,
        }
    }

    #[test]
    fn image_tag_sanitizes_change_id() {
        let cfg = ImageRolloutConfig {
            image_repo: "reg/cog".into(),
            ..Default::default()
        };
        let r = ImageRollout::new(cfg);
        assert_eq!(r.image_tag("abc-123_X"), "reg/cog:change-abc-123_X");
        assert_eq!(r.image_tag("a/b; rm -rf /"), "reg/cog:change-a-b--rm--rf--");
    }

    #[tokio::test]
    async fn deploy_happy_path_builds_pushes_and_rolls_out() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("fake.log");
        let cfg = rollout_config(tmp.path(), true);
        let r = ImageRollout::new(cfg);
        let art = artifact(tmp.path());

        let tag = r.deploy(&art).await.unwrap();
        assert_eq!(tag, "localhost/cogneva:change-p-1");

        // Containerfile 生成
        let cf = std::fs::read_to_string(tmp.path().join("Containerfile.rollout")).unwrap();
        assert!(cf.contains("FROM debian:bookworm-slim"));
        assert!(cf.contains("COPY cogneva /opt/cogneva/cogneva"));

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(calls.contains("build -t localhost/cogneva:change-p-1"));
        assert!(calls.contains("push localhost/cogneva:change-p-1"));
        assert!(calls.contains("set image deployment/cogneva cogneva=localhost/cogneva:change-p-1"));
        assert!(calls.contains("rollout status deployment/cogneva"));
    }

    #[tokio::test]
    async fn rollout_failure_triggers_undo() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("fake.log");
        let cfg = rollout_config(tmp.path(), false);
        let r = ImageRollout::new(cfg);
        let art = artifact(tmp.path());

        let err = r.deploy(&art).await.unwrap_err();
        assert!(err.to_string().contains("rollout status"));

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(calls.contains("rollout undo deployment/cogneva"));
    }
}
