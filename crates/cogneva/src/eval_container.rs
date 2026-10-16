//! 容器执行面：把评测台的执行端口接到本机已有的那台容器机器上。
//!
//! 为什么不是平台沙盒：沙盒的执行端口一次执行绑在一个 Pod 上，**没有选镜像这一项**，而
//! SWE-bench Pro 每一道题要的就是它自己那一份镜像（题号即 tag）。所以这一列的「环境」不是
//! 沙盒能表达的东西，它要的是一台能按名起任意镜像的机器。
//!
//! 本机现成的那台是 `buildah`。它在「当一台容器用」这件事上有三条实测过的语义，这套接线
//! 是按它们搭的：
//!
//! 1. 状态跨 `run` 保留——外壳在容器里改的文件，判分那一次 `run` 读得到，所以「同一棵树
//!    求解、同一棵树判分」这条契约成立。
//! 2. 默认有网络——`npm install` 这类步骤跑得动。
//! 3. 一次 `run` 里起的后台进程随这次 `run` 结束而消失——所以「起服务再跑测试」必须在同一次
//!    `run` 里完成；官方那份 `run_script.sh` 正好是这个形状。
//!
//! 这一层只做**接线**：起容器、按官方口径跑官方脚本、把「哪些测试过了」读回来。判分规则、
//! 工具面的名字与规格、数据格式都在 `cog-eval` 那一侧——在这一侧重写一份就是另起一份正本。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::{SFError, SFResult, ToolExecutor};
use cog_eval::{
    parse_test_list, swe_pro_tool_definitions, tools_bound_to, CaseEnv, EvalCase, SweProBackend,
    SweProTestReport, ToolFactory,
};

/// 容器里工作树的根。镜像是「起了就挂着等 exec」的服务形状，代码在 `/app`。
const WORK_DIR: &str = "/app";
/// 官方判分脚本在容器里的落点。官方流程也放这里，取回现场时对得上。
const WORKSPACE_DIR: &str = "/workspace";
/// 官方判分脚本在数据根里的位置：上游那个仓库的整棵树，脚本在它的 `run_scripts/` 下，
/// 一题一个子目录，目录名就是 `instance_id`。
///
/// 保留上游自己的层次（仓库 → `run_scripts/` → 题号），不把它摊平成一个自取的目录名：
/// 「官方的哪一件」与「盘上的哪一个文件」要能一对一找回去，而摊平之后那个对应关系只活在
/// 这里这一行字里。
pub const SWE_PRO_RUN_SCRIPTS_DIR: &str = "swe-bench-pro-os/run_scripts";
const RUN_SCRIPT: &str = "run_script.sh";
const PARSER_SCRIPT: &str = "parser.py";
/// 镜像名里 registry 之外的那一段。题号就是 tag。
const IMAGE_REPO: &str = "scaleapi/swe-bench_pro-v2";
const IMAGE_HOST: &str = "ghcr.io";

/// 把容器命令换成别的。整条命令（含空格分隔的参数）都从这里读，所以换 `podman`、或指到
/// 另一台机器上，都走这一个旋钮而不必改代码。
const ENV_BUILDAH: &str = "COGNEVA_EVAL_BUILDAH";
/// 拉镜像时把 registry 换成谁。
///
/// 这是一处**标了来源的覆盖位**，默认不动：远端是哪一个就拉哪一个。设它的理由有实测——直连
/// `ghcr.io` 实测 2 MB/min（一份 935 MB 的题镜像要 8 小时，等于用不了），同一份 blob 走镜像站
/// 实测 3.7 MB/s。走镜像站就要把「镜像站给的是不是同一件东西」当成一条判据，所以换过之后
/// 镜像 ref 会随读数一起出去，读的人看得见这份分是从哪一份镜像来的。
const ENV_IMAGE_MIRROR: &str = "COGNEVA_EVAL_IMAGE_MIRROR";
/// 单次容器命令的时间上界（秒）。
///
/// 实测一次完整的判分（`npm install` ＋ 跑测试）约 100 秒，一次镜像拉取约 60 秒。这里取 900
/// 是一个宽得多的**安全上界**，不是量出来的界，所以它可覆盖；它的意义是让一条卡住的命令
/// 变成一次带原因的失败，而不是一个永远挂在那里的格子。
const ENV_TIMEOUT_SECS: &str = "COGNEVA_EVAL_CONTAINER_TIMEOUT_SECS";
const DEFAULT_TIMEOUT_SECS: u64 = 900;
/// 运行时的开关：设成 `off` 就是这一次不接。
const ENV_RUNTIME: &str = "COGNEVA_EVAL_CONTAINER_RUNTIME";

/// 一次容器命令的结局。
#[derive(Debug, Clone)]
struct RunOutcome {
    stdout: String,
    stderr: String,
    /// `None` ＝ 被信号打断，没拿到退出码。
    code: Option<i32>,
}

/// 一台容器机器。今天只有 `buildah` 这一种实现，不为一个实现去造一个 trait。
pub struct BuildahRuntime {
    /// 命令前缀。rootful 那份 store 里才有别人拉好的镜像（rootless 另有一份 store），所以
    /// 默认先试 `sudo -n buildah`。构造时**探**出来，不写死。
    prefix: Vec<String>,
    mirror: Option<String>,
    timeout: Duration,
}

impl BuildahRuntime {
    /// 按环境接上一台容器机器。`Ok(None)` ＝ 这台宿主上没有可用的容器机器，或者这次显式关掉了
    /// 它——两种情况都不该拦住别的列跑，由调用方记进缺席声明。
    pub async fn detect() -> anyhow::Result<Option<Self>> {
        if std::env::var(ENV_RUNTIME)
            .map(|v| v.trim().eq_ignore_ascii_case("off"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let timeout = std::env::var(ENV_TIMEOUT_SECS)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_TIMEOUT_SECS));
        let mirror = std::env::var(ENV_IMAGE_MIRROR)
            .ok()
            .map(|m| m.trim().trim_end_matches('/').to_string())
            .filter(|m| !m.is_empty());

        let candidates: Vec<Vec<String>> = match std::env::var(ENV_BUILDAH) {
            Ok(raw) if !raw.trim().is_empty() => vec![raw
                .split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>()],
            // 顺序有实测依据：镜像落在 rootful 那份 store 里，而 rootless 另有一份 store。
            // 先试错的那一边，会把同一份 935 MB 的镜像再拉一遍。
            _ => vec![
                vec!["sudo".into(), "-n".into(), "buildah".into()],
                vec!["buildah".into()],
            ],
        };
        for prefix in candidates {
            if prefix.is_empty() {
                continue;
            }
            if probe(&prefix).await {
                return Ok(Some(Self {
                    prefix,
                    mirror,
                    timeout,
                }));
            }
        }
        Ok(None)
    }

    /// 一道题那一份镜像的 ref。`instance_id` 就是 tag。
    fn image_ref(&self, instance_id: &str) -> String {
        let host = self.mirror.as_deref().unwrap_or(IMAGE_HOST);
        format!("{host}/{IMAGE_REPO}:{instance_id}")
    }

    fn command(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(&self.prefix[0]);
        cmd.args(&self.prefix[1..]);
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd
    }

    /// 跑一条容器命令。超时是**失败**不是空结果：吞掉它，这一格的分就读成「模型没做对」，
    /// 而真相是「这台机器没跑完」。
    async fn exec(&self, args: &[String]) -> anyhow::Result<RunOutcome> {
        let mut cmd = self.command();
        cmd.args(args);
        let child = cmd
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| anyhow::anyhow!("cannot run `{}`: {e}", self.prefix.join(" ")))?;
        let out = match tokio::time::timeout(self.timeout, child.wait_with_output()).await {
            Ok(res) => res?,
            Err(_) => anyhow::bail!(
                "`{} {}` did not finish within {:?}",
                self.prefix.join(" "),
                args.first().map(String::as_str).unwrap_or(""),
                self.timeout
            ),
        };
        Ok(RunOutcome {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code(),
        })
    }
}

/// 这个前缀在这台宿主上能不能真起容器。
///
/// 判据是「读得出 store 根」，不是「二进制在不在」——`buildah --version` 在 rootless 下一样成功，
/// 但那份 store 里没有别人拉好的镜像。所以这里只认一个**像路径的**非空输出：字段名写错时
/// `--format` 并不报错，它把 `<no value>` 打到 stdout 且退出码为 0（实测：`{{.Store.GraphRoot}}`
/// 大小写不对就是这个样子），只看「非空」会把这个前缀读成可用。
async fn probe(prefix: &[String]) -> bool {
    let mut cmd = tokio::process::Command::new(&prefix[0]);
    cmd.args(&prefix[1..]);
    cmd.arg("info").arg("--format").arg("{{.store.GraphRoot}}");
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let Ok(child) = cmd.kill_on_drop(true).spawn() else {
        return false;
    };
    match tokio::time::timeout(Duration::from_secs(20), child.wait_with_output()).await {
        Ok(Ok(out)) => {
            let root = String::from_utf8_lossy(&out.stdout);
            out.status.success() && root.trim().starts_with('/')
        }
        _ => false,
    }
}

/// 这台机器上的一台工作容器。
struct Container {
    runtime: Arc<BuildahRuntime>,
    name: String,
    image: String,
}

impl Container {
    /// 按名起一台。同名先清掉：上一次跑留下的同名容器会让 `from` 直接失败，而它只是一份
    /// 半成品，不该拦住这一次。
    async fn create(runtime: Arc<BuildahRuntime>, image: &str, name: &str) -> anyhow::Result<Self> {
        let _ = runtime.exec(&["rm".into(), name.into()]).await;
        let out = runtime
            .exec(&["from".into(), "--name".into(), name.into(), image.into()])
            .await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot start a container from {image}: {}",
                tail(&out.stderr, 800)
            );
        }
        Ok(Self {
            runtime,
            name: name.to_string(),
            image: image.to_string(),
        })
    }

    /// 在容器里跑一段 shell。工作目录**显式**切到 `/app`：不靠 buildah 去读镜像的
    /// `WorkingDir`，那样这一层的行为就随 buildah 版本漂。
    ///
    /// 分组写成 `{ 换行 … 换行 }`：换行本身已经终结了那串命令，再补一个 `;` 就成了第二个分隔符，
    /// bash 会报 `syntax error near unexpected token ';'`（实测）。空脚本补一个 `:`——`{` 与 `}`
    /// 之间什么也没有同样是语法错误，而不带 `;` 的写法没法靠加分隔符绕开它。
    async fn shell(&self, script: &str) -> anyhow::Result<RunOutcome> {
        let body = if script.trim().is_empty() {
            ":"
        } else {
            script
        };
        let full = format!("cd {WORK_DIR} && {{\n{body}\n}}");
        self.runtime
            .exec(&[
                "run".into(),
                self.name.clone(),
                "--".into(),
                "bash".into(),
                "-c".into(),
                full,
            ])
            .await
    }

    /// 把宿主上的一个文件放进容器。`dest` 的父目录要先在。
    async fn copy_in(&self, host: &Path, dest: &str) -> anyhow::Result<()> {
        let out = self
            .runtime
            .exec(&[
                "copy".into(),
                self.name.clone(),
                host.display().to_string(),
                dest.to_string(),
            ])
            .await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot copy {} into the container: {}",
                host.display(),
                tail(&out.stderr, 400)
            );
        }
        Ok(())
    }

    /// 把容器里的一个文本文件读回宿主。
    async fn read_text(&self, path: &str) -> anyhow::Result<String> {
        let out = self
            .runtime
            .exec(&[
                "run".into(),
                self.name.clone(),
                "--".into(),
                "cat".into(),
                path.into(),
            ])
            .await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot read {path} in the container: {}",
                tail(&out.stderr, 400)
            );
        }
        Ok(out.stdout)
    }

    /// 写一个文本文件。走 shell 重定向而不是 `buildah copy`：这样写的是同一个 inode，
    /// 权限与属主原样留着，而 `copy` 会把宿主临时文件的权限带进去。
    ///
    /// 用引号包裹的 heredoc：定界符被引起来时里面一个字符都不展开，所以内容原样落盘。
    /// 定界符按内容挑，保证它自己不会出现在内容里——否则那一段会被截断。
    async fn write_text(&self, path: &str, content: &str) -> anyhow::Result<()> {
        let delimiter = pick_delimiter(content);
        let script = format!(
            "cat > {path} <<'{delimiter}'\n{content}\n{delimiter}",
            path = shq(path)
        );
        let out = self.shell(&script).await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot write {path} in the container: {}",
                tail(&out.stderr, 400)
            );
        }
        Ok(())
    }

    /// 容器里那份镜像的身份。判词里要说得出这份分是从哪一份镜像出来的。
    async fn image_identity(&self) -> Option<String> {
        let out = self
            .runtime
            .exec(&[
                "inspect".into(),
                "--type".into(),
                "container".into(),
                "--format".into(),
                "{{.FromImageID}}".into(),
                self.name.clone(),
            ])
            .await
            .ok()?;
        let id = out.stdout.trim().to_string();
        if out.code == Some(0) && !id.is_empty() {
            Some(id)
        } else {
            None
        }
    }

    async fn remove(&self) -> anyhow::Result<()> {
        let out = self.runtime.exec(&["rm".into(), self.name.clone()]).await?;
        // 已经不在了不是错误：回收会被重复调用。分辨的办法只有 buildah 自己的措辞——实测
        // `buildah rm <不存在的名字>` 退出码 125、stderr 是
        // `removing container "x": container not known`，与「真删失败」共用同一个退出码，
        // 所以这里按那句话分，而且那句话是 buildah 的，不是这里能自己定的。
        if out.code != Some(0) && !is_absent(&out.stderr) {
            anyhow::bail!(
                "cannot remove container {}: {}",
                self.name,
                tail(&out.stderr, 400)
            );
        }
        Ok(())
    }
}

/// 一道题的环境：一台容器。外壳在里面改代码，判分也在同一台里跑——换一台判就是在判另一道题。
pub struct SweProContainerEnv {
    container: Arc<Container>,
    /// 这次跑的是谁：镜像 ref、镜像 id、树停在哪个 rev。判词要能带走它。
    identity: String,
}

#[async_trait]
impl CaseEnv for SweProContainerEnv {
    fn id(&self) -> &str {
        &self.container.name
    }

    fn executor(&self) -> Option<Arc<dyn ToolExecutor>> {
        Some(Arc::new(ContainerExecutor {
            container: self.container.clone(),
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn teardown(&self) {
        if let Err(e) = self.container.remove().await {
            tracing::warn!("cannot reclaim container {}: {e}", self.container.name);
        }
    }
}

/// 这道题的执行面：模型喊的工具落在这道题自己的容器里。
///
/// 它只认 `cog-eval` 定下的那两个名字；喊第三个名字是**错误**，不是空结果——工具面之外的东西
/// 静默地返回空，会把「外壳不会用工具」记成「工具不好用」。
struct ContainerExecutor {
    container: Arc<Container>,
}

#[async_trait]
impl ToolExecutor for ContainerExecutor {
    async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
        match name {
            cog_eval::SWE_PRO_TOOL_BASH => {
                let command = arguments
                    .get("command")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        SFError::Validation("bash wants a `command` string".to_string())
                    })?;
                let out = self
                    .container
                    .shell(command)
                    .await
                    .map_err(|e| SFError::Internal(format!("bash failed to run: {e}")))?;
                // 退出码是**结果**不是错误：命令红了正是模型要看见的东西。
                Ok(serde_json::json!({
                    "stdout": out.stdout,
                    "stderr": out.stderr,
                    "exit_code": out.code,
                }))
            }
            cog_eval::SWE_PRO_TOOL_FILE_EDIT => {
                let field = |key: &str| -> SFResult<String> {
                    arguments
                        .get(key)
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                        .ok_or_else(|| {
                            SFError::Validation(format!("file_edit wants a `{key}` string"))
                        })
                };
                let path = field("path")?;
                let old = field("old_string")?;
                let new = field("new_string")?;
                if old.is_empty() {
                    return Err(SFError::Validation(
                        "file_edit wants a non-empty `old_string`".to_string(),
                    ));
                }
                let before =
                    self.container.read_text(&path).await.map_err(|e| {
                        SFError::Internal(format!("file_edit cannot read {path}: {e}"))
                    })?;
                let hits = before.matches(&old).count();
                if hits == 0 {
                    return Ok(serde_json::json!({
                        "error": format!("`old_string` does not occur in {path}"),
                    }));
                }
                if hits > 1 {
                    return Ok(serde_json::json!({
                        "error": format!(
                            "`old_string` occurs {hits} times in {path}; it has to be unique"
                        ),
                    }));
                }
                let after = before.replacen(&old, &new, 1);
                self.container
                    .write_text(&path, &after)
                    .await
                    .map_err(|e| {
                        SFError::Internal(format!("file_edit cannot write {path}: {e}"))
                    })?;
                Ok(serde_json::json!({ "ok": true, "path": path }))
            }
            other => Err(SFError::Validation(format!(
                "unknown tool `{other}`: this instance's face is exactly [{}, {}]",
                cog_eval::SWE_PRO_TOOL_BASH,
                cog_eval::SWE_PRO_TOOL_FILE_EDIT
            ))),
        }
    }
}

/// SWE-bench Pro 的容器后端：镜像来自题号，判分跑该题的官方脚本。
pub struct SweProContainerBackend {
    runtime: Arc<BuildahRuntime>,
    /// 官方那两件脚本的根。一题一个子目录。
    scripts_root: PathBuf,
}

/// 按环境接一份 swe-bench-pro 的后端。
///
/// `Err(why)` 就是那条缺席声明的因：「没接上」这一句要说得出缺的是哪一样，读的人才知道下一步
/// 是去装一台机器、去配镜像，还是去取官方脚本。三件事的处置互不相同，压成一句「没配」会把
/// 读的人送去找一个不存在的旋钮。
pub async fn swe_pro_container_backend(
    data_root: &Path,
) -> Result<Arc<SweProContainerBackend>, String> {
    let Some(runtime) = BuildahRuntime::detect().await.map_err(|e| e.to_string())? else {
        return Err(format!(
            "no container machine is usable on this host: this column runs each instance in its own \
             image, and no `buildah` (tried `sudo -n buildah` then `buildah`) answered; point \
             `{ENV_BUILDAH}` at one if it lives elsewhere"
        ));
    };
    let scripts_root = data_root.join(SWE_PRO_RUN_SCRIPTS_DIR);
    if !scripts_root.is_dir() {
        return Err(format!(
            "the official run scripts are not under the data root: expected {} to hold one \
             directory per instance with {RUN_SCRIPT} and {PARSER_SCRIPT}. How an instance's tests \
             are run and how their output is read is the upstream script's job, and this build will \
             not substitute its own guess for it; fetch them into that directory first",
            scripts_root.display()
        ));
    }
    Ok(Arc::new(SweProContainerBackend {
        runtime: Arc::new(runtime),
        scripts_root,
    }))
}

/// 这道题的工具面：规格由基准定，命令落在这道题自己的容器里。
///
/// 它不持有任何东西——容器要到 `env` 拿出来的时候才存在，所以清单必须按题现造。这也是
/// `cog-eval` 把工具面写成工厂而不是一张定死清单的原因。
pub fn swe_pro_container_tools() -> Arc<ToolFactory> {
    Arc::new(|_case, env| tools_bound_to(env, &swe_pro_tool_definitions()))
}

/// 把这道题的树恢复到「外壳动手之前」：base 那棵树，加上这道题的测试补丁。
///
/// 为什么打 `test_patch` 而不是跑这一行 `before_repo_set_cmd` 的最后一句：那一句是
/// `git checkout <修复提交> -- <几个测试文件>`，而实测这份镜像里**没有**那个提交（`git cat-file
/// -t` 取不到、`rev-list --all` 零命中），照着跑当场失败。两边等价是有实测的：731 行里 678 行
/// 的这一句是同一个形状，且它点名的文件集合与 `test_patch` 改的文件集合 **731/731 行完全
/// 相同**。所以打 `test_patch` 是这一句的忠实替代；判词里那行 `before-repo-set` 把这次替代
/// 说出来了，读的人看得见这一处的口径是怎么来的。
async fn prepare(container: &Container, base: &str, test_patch: &str) -> anyhow::Result<()> {
    let out = container
        .shell(&format!("git reset --hard {} && git clean -fd", shq(base)))
        .await?;
    if out.code != Some(0) {
        anyhow::bail!(
            "cannot reset the instance's tree to {base}: {}",
            tail(&out.stderr, 600)
        );
    }
    if test_patch.trim().is_empty() {
        return Ok(());
    }
    container
        .write_text("/tmp/test_patch.diff", test_patch)
        .await?;
    let out = container.shell("git apply -v /tmp/test_patch.diff").await?;
    if out.code != Some(0) {
        anyhow::bail!(
            "the instance's test_patch does not apply to the base tree: {}",
            tail(&out.stderr, 600)
        );
    }
    Ok(())
}

fn metadata<'a>(case: &'a EvalCase, key: &str) -> anyhow::Result<&'a str> {
    case.metadata
        .get(key)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "case {} carries no `{key}`: this column cannot run it in a container without it",
                case.id
            )
        })
}

#[async_trait]
impl SweProBackend for SweProContainerBackend {
    async fn acquire(&self, case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
        let instance = metadata(case, "instance_id")?;
        let base = metadata(case, "base_commit")?;
        let test_patch = case
            .metadata
            .get("test_patch")
            .map(String::as_str)
            .unwrap_or("");

        let image = self.runtime.image_ref(instance);
        let name = format!("cog-{}", case.id);
        let container = Arc::new(Container::create(self.runtime.clone(), &image, &name).await?);

        // 起完容器之后任一步失败都要回收：留着它既占盘（一份题镜像近 1 GB），又会让下一次
        // 同名起容器失败。
        if let Err(e) = prepare(&container, base, test_patch).await {
            let _ = container.remove().await;
            return Err(e);
        }

        let image_id = container
            .image_identity()
            .await
            .unwrap_or_else(|| "unknown".to_string());
        let head = container
            .shell("git rev-parse HEAD")
            .await
            .map(|o| o.stdout.trim().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        // 身份放在判词的最前面：判词里那段原始输出会被截断，而「这份分出自哪一份镜像」截掉了
        // 就补不回来。换过镜像站时这一行也是「镜像站给的是不是同一件东西」的落点。
        let identity = format!(
            "image\t{}\nimage-id\t{image_id}\nrev\t{head}\nbase-commit\t{base}\n\
             before-repo-set\t{}",
            container.image,
            if test_patch.is_empty() {
                "the row names no test_patch, so only the base tree was restored"
            } else {
                "the test_patch was applied in place of the row's last checkout line: that commit \
                 is not in this image"
            }
        );

        Ok(Arc::new(SweProContainerEnv {
            container,
            identity,
        }))
    }

    async fn run_tests(
        &self,
        env: &Arc<dyn CaseEnv>,
        case: &EvalCase,
    ) -> anyhow::Result<SweProTestReport> {
        // 判分只认这道题自己那台容器。拿到别的环境就报错，而不是对着一个不相干的执行面跑测试
        // ——那样读回来的测试结果不属这道题。
        let env = env
            .as_any()
            .downcast_ref::<SweProContainerEnv>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "case {} was judged against an environment this backend did not create",
                    case.id
                )
            })?;
        let container = &env.container;
        let instance = metadata(case, "instance_id")?;

        let dir = self.scripts_root.join(instance);
        let run_script = dir.join(RUN_SCRIPT);
        let parser_script = dir.join(PARSER_SCRIPT);
        for path in [&run_script, &parser_script] {
            if !path.is_file() {
                anyhow::bail!(
                    "{} is missing, so this instance cannot be judged: how this repository's tests \
                     are run and read is decided by the upstream scripts, not here",
                    path.display()
                );
            }
        }

        container
            .shell(&format!("mkdir -p {WORKSPACE_DIR}"))
            .await?;
        container
            .copy_in(&run_script, &format!("{WORKSPACE_DIR}/{RUN_SCRIPT}"))
            .await?;
        container
            .copy_in(&parser_script, &format!("{WORKSPACE_DIR}/{PARSER_SCRIPT}"))
            .await?;

        // 该跑哪几个测试文件由**这一行**点名。它是 Python 字面量的字符串，不是 JSON——
        // 用 JSON 读会静默得到空列表，于是「一个文件都没点名」被当成「全跑」。
        let files = case
            .metadata
            .get("selected_test_files_to_run")
            .map(|raw| parse_test_list(raw))
            .transpose()?
            .unwrap_or_default();
        let arg = if files.is_empty() {
            String::new()
        } else {
            format!(" {}", shq(&files.join(",")))
        };

        // 官方那一步就长这样：整份脚本一次 `run` 跑完（服务与测试必须在同一个 `run` 里，
        // 后起的进程活不过这一次 `run`），日志落盘，再拿解析器读日志而不是读退出码。
        let ran = container
            .shell(&format!(
                "bash {WORKSPACE_DIR}/{RUN_SCRIPT}{arg} > {WORKSPACE_DIR}/stdout.log \
                 2> {WORKSPACE_DIR}/stderr.log"
            ))
            .await?;
        // 脚本自己会重定向，所以它的退出码不算判据；但一条连日志都没产出的失败要说清楚。
        let parsed = container
            .shell(&format!(
                "python3 {WORKSPACE_DIR}/{PARSER_SCRIPT} {WORKSPACE_DIR}/stdout.log \
                 {WORKSPACE_DIR}/stderr.log {WORKSPACE_DIR}/output.json"
            ))
            .await?;
        if parsed.code != Some(0) {
            anyhow::bail!(
                "the instance's parser did not run to completion (run_script exit {:?}): {}",
                ran.code,
                tail(&parsed.stderr, 800)
            );
        }
        let raw_json = container
            .read_text(&format!("{WORKSPACE_DIR}/output.json"))
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "the parser produced no readable report, so this case has no verdict rather \
                     than a wrong one ({e}); stderr tail: {}",
                    tail(&ran.stderr, 400)
                )
            })?;
        let report: serde_json::Value = serde_json::from_str(&raw_json).map_err(|e| {
            anyhow::anyhow!(
                "the parser's report is not JSON ({e}); head: {}",
                tail(&raw_json, 400)
            )
        })?;

        let mut passed = std::collections::BTreeSet::new();
        let mut failed: Vec<String> = Vec::new();
        let mut other: Vec<String> = Vec::new();
        for test in report
            .get("tests")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let Some(name) = test.get("name").and_then(serde_json::Value::as_str) else {
                continue;
            };
            match test.get("status").and_then(serde_json::Value::as_str) {
                Some("PASSED") => {
                    passed.insert(name.to_string());
                }
                Some("FAILED") => failed.push(name.to_string()),
                // SKIPPED 之类既不算过也不算红：它进了判词，让「没跑」与「跑红了」分得开。
                Some(_) => other.push(name.to_string()),
                None => {}
            }
        }

        let noted = |names: &[String]| -> String {
            if names.is_empty() {
                "none".to_string()
            } else {
                let head: Vec<&str> = names.iter().take(5).map(String::as_str).collect();
                if names.len() > head.len() {
                    format!("{} (+{} more)", head.join(" | "), names.len() - head.len())
                } else {
                    head.join(" | ")
                }
            }
        };
        let raw = format!(
            "{}\nrun-script-exit\t{:?}\npassed\t{}\nfailed\t{}\nother\t{}\nfailed-names\t{}\n",
            env.identity,
            ran.code,
            passed.len(),
            failed.len(),
            other.len(),
            noted(&failed),
        );
        // 一条测试都没解析出来是**这次跑没得出判词**，不是「一条都没过」。前者要报错、由运行器
        // 记 `errored`，后者是一个可以照常打分的 0 分。两者的处置相反，不能压成同一个形状。
        if passed.is_empty() && failed.is_empty() && other.is_empty() {
            anyhow::bail!(
                "the instance's test run reported no tests at all, so this case has no verdict \
                 rather than a wrong one; run_script exit {:?}, stderr tail: {}",
                ran.code,
                tail(&ran.stderr, 800)
            );
        }
        Ok(SweProTestReport { passed, raw })
    }
}

/// 把一段文本安全地放进单引号里——它可能变成 shell 的一条参数，而里面的元字符会静默改写参数。
fn shq(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', r"'\''"))
}

/// 一行的尾巴，留给判词和读数。
fn tail(raw: &str, limit: usize) -> String {
    let trimmed = raw.trim();
    let chars: Vec<char> = trimmed.chars().collect();
    if chars.len() <= limit {
        return trimmed.to_string();
    }
    let start = chars.len() - limit;
    format!("…{}", chars[start..].iter().collect::<String>())
}

/// buildah 说「这个容器本来就不存在」的那句话。
///
/// 它和「真删失败」共用退出码 125，所以只能按措辞分。两句话都要认：`rm` 说 `container not known`，
/// 别处（`buildah run` 之类的子命令）用 `no such container`。这是 buildah 的措辞而不是这里定的，
/// 换一个 `COGNEVA_EVAL_BUILDAH` 前缀就得重新实测这两个短语。
fn is_absent(stderr: &str) -> bool {
    stderr.contains("not known") || stderr.contains("no such container")
}

/// 挑一个不出现在正文里的 heredoc 定界符。正文里带上了它，写进去的内容就会被提前截断。
fn pick_delimiter(content: &str) -> String {
    let mut delimiter = "COGNEVA_CONTAINER_SHELL_EOF".to_string();
    while content.contains(&delimiter) {
        delimiter.push('_');
    }
    delimiter
}
