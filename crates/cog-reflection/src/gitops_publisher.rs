//! GitOps 推送端。
//!
//! 晋级放行后把沙盒工作区当前 HEAD 推到中央仓库 release 分支，
//! 并打 `promote/<patch_id>` annotated tag（tag message 带审计元数据：
//! patch_id、级别、eval 摘要）。各集群拉取端 poll 该分支各自金丝雀。
//!
//! 安全边界：本模块只跟 Git 中央仓库说话（三仓库同步既有通道），
//! 全程不持有、不使用任何集群凭证（kubeconfig / API token）。
//!
//! 镜像分发双态：`registry` 配置存在时，同时用 buildah 把沙盒编译
//! 好的二进制打成镜像推到仓库（拉取端走镜像 pull）；缺省走纯源码
//! 级分发（拉取端本地构建）。

use std::path::PathBuf;
use std::time::Duration;

use crate::GitOpsConfig;
use async_trait::async_trait;
use cog_core::{SFError, SFResult};
use tracing::info;

use crate::auto_promoter::PromotionChannel;
use crate::types::EvolutionResult;

/// patch_id 只保留 [A-Za-z0-9-_]，其余替换为 '-'（git ref / 镜像 tag
/// 合法字符白名单，防注入）。
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

pub struct GitOpsPublisher {
    config: GitOpsConfig,
    project_root: PathBuf,
    /// 沙盒编译产物所在目录（registry 模式打镜像用）。
    binary_dir: PathBuf,
}

impl GitOpsPublisher {
    pub fn new(
        config: GitOpsConfig,
        project_root: impl Into<PathBuf>,
        binary_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            config,
            project_root: project_root.into(),
            binary_dir: binary_dir.into(),
        }
    }

    async fn run(&self, program: &str, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let cmdline = format!("{} {}", program, args.join(" "));
        let fut = tokio::process::Command::new(program)
            .args(args)
            .current_dir(&self.project_root)
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

    async fn git(&self, args: &[&str]) -> SFResult<String> {
        self.run("git", args, 120).await
    }

    /// 推送当前 HEAD 到 release 分支 + 打 promote tag。
    /// 返回 HEAD commit hash。
    async fn publish(&self, patch: &EvolutionResult, level: &str) -> SFResult<String> {
        if self.config.repo_url.is_empty() {
            return Err(SFError::Config(
                "gitops.repo_url 未配置，无法推送晋级产物".into(),
            ));
        }
        let head = self.git(&["rev-parse", "HEAD"]).await?;
        let tag = format!("promote/{}", sanitize(&patch.artifact_id));
        let msg = format!(
            "patch_id={}\nlevel={}\neval={}",
            patch.artifact_id,
            level,
            patch.eval_summary.as_deref().unwrap_or("none")
        );

        // promote tag 是指针性质，重推同 patch 允许 -f 覆盖。
        self.git(&["tag", "-a", "-f", "-m", &msg, &tag, &head])
            .await?;

        // 非 force 推分支：历史必须 fast-forward。远端出现非本管线
        // 提交时 push 失败 → 台账 Failed → 熔断兜底，绝不强推覆盖。
        self.git(&[
            "push",
            &self.config.repo_url,
            &format!("HEAD:{}", self.config.branch),
        ])
        .await?;
        self.git(&[
            "push",
            &self.config.repo_url,
            &format!("refs/tags/{tag}:refs/tags/{tag}"),
        ])
        .await?;

        info!(
            patch_id = %patch.artifact_id,
            level,
            commit = %head,
            branch = %self.config.branch,
            "Promoted patch published to GitOps release branch"
        );

        // 镜像双态：registry 配置存在时把沙盒编译产物打镜像推仓库。
        if let Some(registry) = &self.config.registry {
            self.push_image(registry, &patch.artifact_id).await?;
        }

        Ok(head)
    }

    /// buildah 打最小镜像（base + 沙盒编译二进制）并推仓库。
    async fn push_image(&self, registry: &str, patch_id: &str) -> SFResult<()> {
        let binary = self.binary_dir.join("cogneva");
        if !binary.exists() {
            return Err(SFError::IO(format!(
                "staged binary not found: {}",
                binary.display()
            )));
        }
        let image = format!("{}/cogneva:promote-{}", registry, sanitize(patch_id));
        let containerfile = self.binary_dir.join("Containerfile.promote");
        tokio::fs::write(
            &containerfile,
            format!(
                "FROM {}\nCOPY cogneva /opt/cogneva/cogneva\nENTRYPOINT [\"/opt/cogneva/cogneva\"]\n",
                "debian:bookworm-slim"
            ),
        )
        .await
        .map_err(|e| SFError::IO(format!("write Containerfile: {e}")))?;

        self.run(
            "buildah",
            &[
                "build",
                "-f",
                &containerfile.to_string_lossy(),
                "-t",
                &image,
                &self.binary_dir.to_string_lossy(),
            ],
            600,
        )
        .await?;
        self.run("buildah", &["push", &image], 600).await?;
        info!(image = %image, "Promotion image pushed to registry");
        Ok(())
    }
}

#[async_trait]
impl PromotionChannel for GitOpsPublisher {
    async fn publish_config(&self, patch: &EvolutionResult) -> SFResult<String> {
        self.publish(patch, "l0_config").await
    }

    async fn publish_rollout(&self, patch: &EvolutionResult) -> SFResult<String> {
        self.publish(patch, "l1_rollout").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn git(dir: &std::path::Path, args: &[&str]) -> String {
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

    async fn setup_repo() -> (tempfile::TempDir, tempfile::TempDir) {
        let central = tempfile::tempdir().unwrap();
        git(central.path(), &["init", "--bare"]).await;

        let work = tempfile::tempdir().unwrap();
        git(work.path(), &["init"]).await;
        git(work.path(), &["config", "user.email", "test@test.com"]).await;
        git(work.path(), &["config", "user.name", "Test"]).await;
        tokio::fs::write(work.path().join("lib.rs"), "fn v1() {}\n")
            .await
            .unwrap();
        git(work.path(), &["add", "."]).await;
        git(work.path(), &["commit", "-m", "initial"]).await;
        (central, work)
    }

    fn patch(id: &str) -> EvolutionResult {
        EvolutionResult {
            kind: crate::types::EvolutionKind::CodePatch,
            artifact_id: id.into(),
            description: "test".into(),
            content: String::new(),
            status: crate::types::EvolutionStatus::Active,
            created_at: chrono::Utc::now(),
            eval_summary: Some("Adopt z=2.0".into()),
        }
    }

    #[tokio::test]
    async fn publish_pushes_branch_and_annotated_tag() {
        let (central, work) = setup_repo().await;
        let publisher = GitOpsPublisher::new(
            GitOpsConfig {
                repo_url: central.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            work.path(),
            work.path(),
        );

        let head = publisher
            .publish(&patch("p-1"), "l1_rollout")
            .await
            .unwrap();

        // 分支已推送且指向 HEAD。
        let remote_head = git(central.path(), &["rev-parse", "evolution-release"]).await;
        assert_eq!(remote_head, head);

        // tag 存在且 message 带审计元数据。
        let tag_msg = git(
            central.path(),
            &["tag", "-l", "--format=%(contents)", "promote/p-1"],
        )
        .await;
        assert!(tag_msg.contains("patch_id=p-1"), "{tag_msg}");
        assert!(tag_msg.contains("level=l1_rollout"), "{tag_msg}");
        assert!(tag_msg.contains("eval=Adopt z=2.0"), "{tag_msg}");
    }

    #[tokio::test]
    async fn publish_sanitizes_patch_id_in_tag() {
        let (central, work) = setup_repo().await;
        let publisher = GitOpsPublisher::new(
            GitOpsConfig {
                repo_url: central.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            work.path(),
            work.path(),
        );
        publisher
            .publish(&patch("p/1; rm -rf"), "l0_config")
            .await
            .unwrap();
        let tags = git(central.path(), &["tag", "-l"]).await;
        assert!(tags.contains("promote/p-1--rm--rf"), "{tags}");
    }

    #[tokio::test]
    async fn second_publish_fast_forwards() {
        let (central, work) = setup_repo().await;
        let publisher = GitOpsPublisher::new(
            GitOpsConfig {
                repo_url: central.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            work.path(),
            work.path(),
        );
        publisher
            .publish(&patch("p-1"), "l1_rollout")
            .await
            .unwrap();

        tokio::fs::write(work.path().join("lib.rs"), "fn v2() {}\n")
            .await
            .unwrap();
        git(work.path(), &["add", "."]).await;
        git(work.path(), &["commit", "-m", "second"]).await;

        publisher
            .publish(&patch("p-2"), "l1_rollout")
            .await
            .unwrap();
        let log = git(central.path(), &["log", "--format=%s", "evolution-release"]).await;
        assert!(log.contains("second"), "{log}");
    }

    #[tokio::test]
    async fn empty_repo_url_rejected() {
        let (_central, work) = setup_repo().await;
        let publisher = GitOpsPublisher::new(GitOpsConfig::default(), work.path(), work.path());
        let err = publisher.publish(&patch("p-1"), "l1_rollout").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn channel_trait_maps_levels() {
        let (central, work) = setup_repo().await;
        let publisher = GitOpsPublisher::new(
            GitOpsConfig {
                repo_url: central.path().to_string_lossy().to_string(),
                ..Default::default()
            },
            work.path(),
            work.path(),
        );
        PromotionChannel::publish_config(&publisher, &patch("cfg-1"))
            .await
            .unwrap();
        let tag_msg = git(
            central.path(),
            &["tag", "-l", "--format=%(contents)", "promote/cfg-1"],
        )
        .await;
        assert!(tag_msg.contains("level=l0_config"), "{tag_msg}");
    }
}
