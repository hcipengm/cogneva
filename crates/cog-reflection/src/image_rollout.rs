//! Image-based 滚动更新部署器（审计 3.2 剩余项）。
//!
//! 启用 `self_evolution.image_rollout` 后，change 部署路径从特权 Pod
//! `self_exec` 二进制替换升级为：
//!
//! 1. 基于已编译 staged 二进制生成最小 Containerfile 并构建镜像
//!    （tag = `{image_repo}:change-{change_id}`，change_id 经字符白名单消毒）；
//! 2. 可选 `<builder> push`（k3s 节点本地镜像可关）；
//! 3. `kubectl set image` change 目标 Deployment；
//! 4. 短查询轮询滚动读数，**按动静判**：完成或平台声明失败才收尾，过了申报的
//!    墙只留一行字；失败（有直接证据）自动 `rollout undo` 回滚。
//!
//! 外部命令按动静判停（[`cog_core::progress::run_child_watched`]）；任何一步失败
//! 都会尽量回滚滚动更新。

use std::time::Duration;

use cog_core::declared_wall::Step;
use cog_core::{ImageRolloutConfig, SFError, SFResult};
use tracing::{info, warn};

/// 读部署状态的节奏。读取周期，不是界：界是
/// [`cog_core::declared_wall::wait_for_settled`] 按实测窗口判的那一条。
// cap: bound — 读取周期钉在这里，不参与判停
const ROLLOUT_POLL: Duration = Duration::from_secs(10);

/// 「滚动读数变了」这条证据通道的名字。等待方与判停方读同一个名字。
const ROLLOUT_CHANNEL: &str = "image-rollout";

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

    /// 镜像仓库由部署面声明，这里只负责「没声明就停」。
    ///
    /// 与基底同一条理由：`localhost/cogneva` 曾经是这个字段的默认值，它是某台
    /// 节点上的本地仓库，抄进二进制以后，换一个部署形态（远程仓库、带端口的
    /// 集群内 registry）就推不进任何地方——而失败的现场在镜像构建成功之后才出现，
    /// 读起来像新镜像坏了。空着就在写 Containerfile 之前拒绝，失败点落在配置上。
    fn image_repo(&self) -> SFResult<&str> {
        let repo = self.config.image_repo.trim();
        if repo.is_empty() {
            return Err(SFError::Validation(
                "没有声明镜像仓库（self_evolution.image_rollout.image_repo）：本进程不猜\
                 仓库地址，猜错要等镜像建完才发现"
                    .into(),
            ));
        }
        Ok(repo)
    }

    /// 镜像基底由部署面声明，这里只负责「没声明就停」。
    ///
    /// 空着不当作能用默认值补上：集群内的仓库地址写进代码就再也跟着部署走不动，
    /// 而错的内容要等镜像构建成功、容器启动时才暴露——失败现场落在滚动更新里，
    /// 读起来像「新镜像坏了」。在写 Containerfile 之前拒绝，失败点就在配置上。
    fn base_image(&self) -> SFResult<&str> {
        let base = self.config.base_image.trim();
        if base.is_empty() {
            return Err(SFError::Validation(
                "没有声明镜像基底（self_evolution.image_rollout.base_image）：本进程不猜\
                 集群内的仓库地址，猜错要等镜像建完、容器起不来才发现"
                    .into(),
            ));
        }
        Ok(base)
    }

    /// 判的是它还在不在产出，不是它跑了多久：一次冷构建要从仓库拉基镜像、解包
    /// 一点五 GB，安静地忙上很久；而一份卡死的构建与它在那条按秒的界眼里是同一个读数。
    ///
    /// 唯一的例外是「等别人的东西就位」（`kubectl rollout status`）：那是我方对集群
    /// 收敛的耐心，由它自己的 `--timeout` 说，见调用点。
    async fn run(&self, program: &str, args: &[&str]) -> SFResult<String> {
        let cmdline = format!("{} {}", program, args.join(" "));
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args).kill_on_drop(true);
        let output = match cog_core::progress::run_child_watched(cmd, &cmdline).await {
            Ok(cog_core::progress::ChildOutcome::Exited(output)) => output,
            Ok(cog_core::progress::ChildOutcome::Stalled { silent, window }) => {
                return Err(SFError::IO(format!(
                    "{cmdline} 停住了：{silent:?} 没有输出、也没有消耗 CPU，比这份部署见过的\
                     任何一次「还在干活」的两次推进之间的最长间隔都长（{window:?}）；已杀掉"
                )))
            }
            Err(e) => return Err(SFError::IO(format!("failed to run {program}: {e}"))),
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!("{cmdline} failed: {stderr}")));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// 一次短查询读滚动的现状：完成 / 平台声明失败 / 还在走。
    ///
    /// 失败那一支只认 `Progressing=False` 的 `ProgressDeadlineExceeded`——那是部署
    /// 自己申报的期望工期被越过且不再推进，是平台给出的判词。读不出来（查询本身
    /// 失败）不是「停住」：交给等待方继续问，不在这里把一眼看不清写成失败。
    async fn rollout_step(&self, deploy_ref: &str) -> Result<Step<String>, String> {
        let out = self
            .run(
                &self.config.kubectl_bin,
                &[
                    "-n",
                    &self.config.namespace,
                    "get",
                    "deployment",
                    &self.config.deployment,
                    "-o",
                    "jsonpath={.metadata.generation} {.status.observedGeneration} {.spec.replicas} {.status.updatedReplicas} {.status.readyReplicas} {.status.conditions[?(@.type==\"Progressing\")].reason}",
                ],
            )
            .await
            .map_err(|e| e.to_string())?;
        let parts: Vec<&str> = out.split_whitespace().collect();
        if parts.len() < 5 {
            return Ok(Step::Moving(out));
        }
        let gen: u64 = parts[0].parse().unwrap_or(0);
        let obs: u64 = parts[1].parse().unwrap_or(0);
        let spec: u32 = parts[2].parse().unwrap_or(0);
        let updated: u32 = parts[3].parse().unwrap_or(0);
        let ready: u32 = parts[4].parse().unwrap_or(0);
        if cog_core::declared_wall::generation_reaches(gen, obs) && updated == spec && ready == spec
        {
            return Ok(Step::Done(deploy_ref.to_string()));
        }
        // 判词只对控制器处理过的那一代作数：`apply` 之后的第一眼可能还带着上一代
        // 留下的 `ProgressDeadlineExceeded`。词表取自控制律那一侧，这一格按空格分列。
        if cog_core::declared_wall::generation_reaches(gen, obs)
            && parts.get(5) == Some(&cog_core::declared_wall::PROGRESS_DEADLINE_EXCEEDED)
        {
            return Ok(Step::Failed(format!(
                "{deploy_ref} reports Progressing=False (ProgressDeadlineExceeded): {out}"
            )));
        }
        Ok(Step::Moving(out))
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

        let binary = cog_core::paths::APP_BINARY;
        let containerfile = format!(
            "FROM {}\nCOPY {} {binary}\nENTRYPOINT [\"{binary}\"]\n",
            self.base_image()?,
            binary_name
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
        self.image_repo()?;
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
        )
        .await?;
        info!(tag = %tag, "rollout image built");

        // 2. 可选推送
        if self.config.registry_push {
            self.run(&self.config.builder_bin, &["push", &tag]).await?;
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
        )
        .await?;
        info!(deployment = %deploy_ref, tag = %tag, "deployment image updated");

        // 4. 等待滚动完成：只在完成或平台声明失败时走下一步，过了申报的墙只说话。
        //    `rollout status --timeout` 把「等了 N 秒」写成了判词；一次慢拉取就会让
        //    一个还在往前走的滚动被 undo 掉。
        let declared = Duration::from_secs(timeout);
        let settled = cog_core::declared_wall::wait_for_settled(
            declared,
            "the rollout after an image change",
            ROLLOUT_CHANNEL,
            ROLLOUT_POLL,
            || {
                let deploy_ref = deploy_ref.clone();
                async move { self.rollout_step(&deploy_ref).await }
            },
        )
        .await;
        if let Err(why) = settled {
            let e = SFError::Agent(why);
            warn!(error = %e, "rollout did not settle; rolling back");
            if let Err(rb) = self
                .run(
                    &self.config.kubectl_bin,
                    &["-n", &self.config.namespace, "rollout", "undo", &deploy_ref],
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
    use crate::test_support::write_executable as write_fake_bin;

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
            &format!("#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\nexit 0\n"),
        );
        // fake kubectl：部署读数按 rollout_ok 回答「滚完了」或「平台声明失败」，
        // 其余记录参数。
        let kubectl_script = if rollout_ok {
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\ncase \"$*\" in *observedGeneration*) echo \"3 3 2 2 2\" ;; esac\nexit 0\n"
            )
        } else {
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\ncase \"$*\" in *observedGeneration*) echo \"3 3 2 1 1 ProgressDeadlineExceeded\" ;; esac\nexit 0\n"
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
        assert!(calls.contains(".status.observedGeneration"));
    }

    #[tokio::test]
    async fn an_undeclared_base_image_stops_before_anything_is_built() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("fake.log");
        let mut cfg = rollout_config(tmp.path(), true);
        cfg.base_image = "   ".into();
        let r = ImageRollout::new(cfg);
        let art = artifact(tmp.path());

        let err = r.deploy(&art).await.unwrap_err();
        assert!(
            err.to_string().contains("base_image"),
            "拒绝要说清是哪个字段没声明：{err}"
        );
        assert!(
            !tmp.path().join("Containerfile.rollout").exists(),
            "拒绝之前不许先落一个 `FROM` 空地址的 Containerfile"
        );
        assert!(!log.exists(), "镜像构建器一次都不该被调起");
    }

    /// 仓库地址一条都没有时，与基底同一种拒绝：先停，再动手。代码里给过默认值
    /// （`localhost/cogneva`），那是某台节点上的本地仓库，换个部署形态就推不进
    /// 任何地方，而这件事要到镜像建完才看得见。
    #[tokio::test]
    async fn an_undeclared_image_repo_stops_before_anything_is_built() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("fake.log");
        let mut cfg = rollout_config(tmp.path(), true);
        cfg.image_repo = "  ".into();
        let r = ImageRollout::new(cfg);
        let art = artifact(tmp.path());

        let err = r.deploy(&art).await.unwrap_err();
        assert!(
            err.to_string().contains("image_repo"),
            "拒绝要说清是哪个字段没声明：{err}"
        );
        assert!(
            !tmp.path().join("Containerfile.rollout").exists(),
            "拒绝之前不许先落一个 `FROM` 空地址的 Containerfile"
        );
        assert!(!log.exists(), "镜像构建器一次都不该被调起");
    }

    #[tokio::test]
    async fn rollout_failure_triggers_undo() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("fake.log");
        let cfg = rollout_config(tmp.path(), false);
        let r = ImageRollout::new(cfg);
        let art = artifact(tmp.path());

        let err = r.deploy(&art).await.unwrap_err();
        assert!(
            err.to_string().contains("ProgressDeadlineExceeded"),
            "回滚的判词要说出是哪条直接证据：{err}"
        );

        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(calls.contains("rollout undo deployment/cogneva"));
    }
}
