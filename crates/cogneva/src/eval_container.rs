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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::{SFError, SFResult, ToolExecutor};
use cog_eval::{
    parse_test_list, swe_pro_tool_definitions, tools_bound_to, CaseEnv, EvalCase, ExecLimits,
    ExecOutcome, ExecRequest, ExecRunner, ExecSeam, FinishReason, SweProBackend, SweProTestReport,
    ToolFactory,
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

/// 跑**别人的程序**那一跑显式要的隔离档。
///
/// 要出来，不继承：`--isolation` 与 `--network` 的默认值来自宿主（`BUILDAH_ISOLATION`
/// 环境变量、`containers.conf` 的 `isolation` 与 `default_network`）。实测（本机 rootful
/// buildah）默认与 `--isolation oci --network private` 逐项相同——私有 net/pid 命名空间、
/// CNI 网段里的 eth0；而 `--isolation chroot` 下容器的 net 与 pid 命名空间**与宿主逐字相同**
/// （`/proc/net/dev` 里看得到宿主真实的网卡），也就是「第三方代码跑在哪儿」这件事会随宿主的
/// 一处配置静默改变。判分那一跑不动它（那一跑本来就跑上游自己的脚本，改它的读数是另一个
/// 改动），这一条只挂在外部流派那一跑上。
const FOREIGN_RUN_ISOLATION: [&str; 4] = ["--isolation", "oci", "--network", "private"];

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

/// 外部流派的声明：一行一支，制表符分三段——`名字 <TAB> 列名,列名 <TAB> 在这道题里怎么起它`。
///
/// 为什么是一块而不是每支流派一组旋钮：**这次跑的是哪几行**是这次实验的组成，声明在一处
/// 才读得出来；拆成每支几条，读的人要凑几条 `env` 才拼得出这一张表有几行。
///
/// 为什么是制表符：名字与列名里不会出现制表符，而命令里什么都会出现——命令是这三段里
/// 唯一一段自由文本，所以它放最后、占满余下全部。
pub const ENV_FOREIGN_FLOWS: &str = "COGNEVA_EVAL_FOREIGN_FLOWS";
/// 一支外部流派跑一次的三条上限：时间、核数、内存。**一起给，缺一条整条声明不成立**。
///
/// 不给一个猜来的默认值：外部流派的循环不在我们手里，上限就是这次跑唯一能管的那个旋钮，
/// 而「上限是多少」是被实验定义的东西，不是这里能替人拍的数。给个默认值，读表的人会以为
/// 那个数是量出来的——见 `ExecLimits`。
pub const ENV_FOREIGN_WALL_SECS: &str = "COGNEVA_EVAL_FOREIGN_WALL_SECS";
pub const ENV_FOREIGN_CPUS: &str = "COGNEVA_EVAL_FOREIGN_CPUS";
pub const ENV_FOREIGN_MEMORY_MB: &str = "COGNEVA_EVAL_FOREIGN_MEMORY_MB";

/// 容器里那一段题面的落点。外部流派怎么读题由它定，我们只承诺「这一段文字是这道题」。
const FOREIGN_PROBLEM_PATH: &str = "/tmp/foreign_problem.txt";
/// 给这棵树拍照用的那一份索引，与仓库自己的工作索引分开。
///
/// 分开是必须的：外部程序自己怎么动 `git`（`add`、`stash`、`commit`……）都在我们管不到
/// 的地方，用同一份索引取补丁，它动一下就变了——而这份补丁要拿去判分。
const FOREIGN_INDEX_PATH: &str = "/tmp/foreign.index";

/// 环境里声明的、可用的外部流派声明。
///
/// 读不出来就是 `Err(为什么)`，**不是**一个空列表：一块写坏的声明与「这次没有外部流派」
/// 读起来一样，而处置相反——前者要去改声明，后者什么都不用做。
pub fn foreign_flows() -> Result<Vec<ForeignFlowSpec>, String> {
    let raw = std::env::var(ENV_FOREIGN_FLOWS).unwrap_or_default();
    let lines: Vec<&str> = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    if lines.is_empty() {
        return Ok(Vec::new());
    }
    parse_flows(&raw, foreign_limits()?)
}

/// 读一块声明。纯函数：它读的是**文本**，环境那一步在外面——所以「哪种声明算写坏了」
/// 能被单测直接钉住，不必去动进程级的环境变量。
fn parse_flows(raw: &str, limits: ExecLimits) -> Result<Vec<ForeignFlowSpec>, String> {
    let lines: Vec<&str> = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    if lines.is_empty() {
        return Ok(Vec::new());
    }
    let mut flows: Vec<ForeignFlowSpec> = Vec::new();
    for line in lines {
        let mut parts = line.split('\t').map(str::trim);
        let (Some(name), Some(columns), Some(entrypoint), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(format!(
                "`{ENV_FOREIGN_FLOWS}` has a line that is not \
                 `name<TAB>column,column<TAB>how to start it`: {line}"
            ));
        };
        if name.is_empty() || entrypoint.is_empty() {
            return Err(format!(
                "`{ENV_FOREIGN_FLOWS}` has a line with no name or no entrypoint: {line}"
            ));
        }
        let columns: Vec<String> = columns
            .split(',')
            .map(str::trim)
            .filter(|column| !column.is_empty())
            .map(String::from)
            .collect();
        if columns.is_empty() {
            // 一支不落在任何列上的流派是一个永远不会跑的格子：它读起来像「这一行得了零分」，
            // 而真相是这一行没有定义域。
            return Err(format!(
                "`{ENV_FOREIGN_FLOWS}` declares `{name}` on no column: a row that lands on no \
                 column never runs, and reads like a row that scored zero"
            ));
        }
        if flows.iter().any(|flow| flow.name == name) {
            return Err(format!(
                "`{ENV_FOREIGN_FLOWS}` declares `{name}` twice: two rows with one name cannot be \
                 told apart in the table"
            ));
        }
        flows.push(ForeignFlowSpec {
            name: name.to_string(),
            columns,
            entrypoint: entrypoint.to_string(),
            limits,
        });
    }
    Ok(flows)
}

/// 三条上限。缺一条就整条声明不成立，理由要说得出缺的是哪一条。
fn foreign_limits() -> Result<ExecLimits, String> {
    let number = |key: &str| -> Result<f64, String> {
        std::env::var(key)
            .map_err(|_| {
                format!(
                    "external flows are declared but `{key}` is not set: how long a foreign flow \
                     may run and how much of this machine it may take is the one knob this run \
                     can hold on a loop that is not ours, and this build will not invent it"
                )
            })?
            .trim()
            .parse::<f64>()
            .map_err(|e| format!("`{key}` is not a number ({e})"))
            .and_then(|value| {
                if value.is_finite() && value > 0.0 {
                    Ok(value)
                } else {
                    Err(format!("`{key}` must be a positive number, got {value}"))
                }
            })
    };
    let wall = number(ENV_FOREIGN_WALL_SECS)?;
    let cpus = number(ENV_FOREIGN_CPUS)?;
    let memory_mb = number(ENV_FOREIGN_MEMORY_MB)?;
    Ok(ExecLimits {
        wall_clock: Duration::from_secs(wall as u64),
        cpus: cpus as f32,
        memory_bytes: (memory_mb * 1024.0 * 1024.0) as u64,
    })
}

/// 一支外部流派的声明：谁、落在哪几列上、怎么起它、上限多少。
///
/// 这里只描述**形状**，不认识任何一支具体的流派：名字与命令都从环境里读进来，所以
/// 接一支新的外部流派不用改这一侧的任何一行代码。
#[derive(Debug, Clone)]
pub struct ForeignFlowSpec {
    pub name: String,
    /// 它在哪些列上有定义（基准名）。列外的地方这一行**不建格子**，不是零分。
    pub columns: Vec<String>,
    pub entrypoint: String,
    pub limits: ExecLimits,
}

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
    /// 这台宿主的 store 里现在有哪些镜像 ref。**问一次，整跑记着**：这是一条普查读数，
    /// 每道题各问一遍等于把一次便宜的问摊成题数那么多次。`None` ＝ 问不出来（命令失败或
    /// 这份 buildah 不认 `images`），这时调用方不许把「问不出来」当成「本地没有」。
    images: tokio::sync::OnceCell<Option<HashSet<String>>>,
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
                    images: tokio::sync::OnceCell::new(),
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

    /// 本地 store 里现在有哪些镜像 ref（问一次，整跑缓存）。
    ///
    /// 存在的理由是一条**要分开的两件事**：`buildah from <ref>` 在镜像不在本地时会顺手去拉，
    /// 而「拉」与「跑」共用一个时间界，界到点报出来的是一句「命令没跑完」——读的人从这句里
    /// 分不出缺的是一份镜像、还是这台机器坏了。实测（2026-10-17，本机）：宿主默认路由换到了
    /// WiFi，实测吞吐 150–220 KB/s，一题 2.26 GB 的镜像在 900 秒的界里拉不完，整整一跑报回来
    /// 的却是 `sudo -n buildah from did not finish within 900s`。
    async fn image_refs(&self) -> Option<&HashSet<String>> {
        self.images
            .get_or_init(|| async {
                let out = self
                    .exec(&[
                        "images".into(),
                        "--format".into(),
                        "{{.Name}}:{{.Tag}}".into(),
                    ])
                    .await
                    .ok()?;
                if out.code != Some(0) {
                    return None;
                }
                Some(parse_image_refs(&out.stdout))
            })
            .await
            .as_ref()
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

/// `buildah images --format '{{.Name}}:{{.Tag}}'` 的正文 → 一份 ref 集合。
///
/// 单独成一个不吃运行时的函数，是因为这里有一处**会静默读错**的形状：buildah 对没有 tag 的
/// （以及中间层）镜像把 `Name` 或 `Tag` 打成字面量 `<none>`，于是输出里会有
/// `<none>:<none>` 这样的行。把它们当成 ref 收进来，集合里就多出几个永远匹配不上的名字，
/// 而「镜像在不在本地」这条判据只会在该说「不在」的时候说对——多出来的那些名字**永远不会**
/// 被问到，所以这份错在读数是看不出来的：它只会让集合大一点。
fn parse_image_refs(stdout: &str) -> HashSet<String> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        // `<none>` 不是 ref，是「这里没有名字」这个读数的打印形状。尾部那个 tag 也是。
        .filter(|line| *line != "<none>:<none>" && !line.ends_with(":<none>"))
        .map(str::to_string)
        .collect()
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
        self.shell_with(script, &[]).await
    }

    /// 同上，外加这一跑自己要的开关（`extra`）。隔离档走这条：它是**这一跑**的属性，
    /// 不是这台容器的属性——同一台容器，判分那一跑要默认，别人的程序那一跑要出来。
    async fn shell_with(&self, script: &str, extra: &[&str]) -> anyhow::Result<RunOutcome> {
        self.runtime
            .exec(&run_argv(&self.name, script, extra))
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

    /// 给这棵树拍一张快照（一个 git 树对象）。
    ///
    /// 补丁取的是**两次快照之差**，不是「树上现在有什么」：判分前打进去的测试补丁也在树上，
    /// 把它算成外部流派的功劳，读出来的补丁就不是它写的了。所以起跑前先拍一张。
    ///
    /// 快照走的一份**单独索引**：外部程序自己怎么动 `git` 都在我们管不到的地方（`add` 是
    /// 外部流派最常见的动作），共用仓库那份工作索引时它动一下，读出来的差就变了。
    async fn snapshot_tree(&self) -> anyhow::Result<String> {
        let out = self
            .shell(&format!(
                "export GIT_INDEX_FILE={FOREIGN_INDEX_PATH}\n\
                 [ -f {FOREIGN_INDEX_PATH} ] || git read-tree HEAD\n\
                 git add -A -- .\n\
                 git write-tree"
            ))
            .await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot snapshot the instance's tree: {}",
                tail(&out.stderr, 600)
            );
        }
        // 判据是**形状**：`git write-tree` 只打一个对象 id。别的行（warn、钩子的输出）
        // 混进 stdout 也不能被当成快照名——一个不是 id 的字符串要等到 `git diff` 那一步
        // 才炸，而那时补丁已经取不回来了。
        let tree = out.stdout.lines().last().unwrap_or("").trim().to_string();
        let hex = tree.len() == 40 || tree.len() == 64;
        if !hex || !tree.chars().all(|c| c.is_ascii_hexdigit()) {
            anyhow::bail!(
                "`git write-tree` did not report a tree id, so there is no snapshot to diff \
                 against; it said: {}",
                tail(&out.stdout, 300)
            );
        }
        Ok(tree)
    }

    /// 两次快照之间的补丁。`--binary` 让二进制文件的改动也是一份能落地的补丁，而不是
    /// 一行「Binary files differ」。
    async fn tree_patch(&self, before: &str, after: &str) -> anyhow::Result<String> {
        let out = self
            .shell(&format!("git diff --binary --no-color {before} {after}"))
            .await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot read back what the foreign flow left in the tree: {}",
                tail(&out.stderr, 600)
            );
        }
        Ok(out.stdout)
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

/// 一条「在容器里跑一段 shell」的 argv。
///
/// `extra` 放在子命令与容器名**之间**：buildah 的 flag 只在那一位上被认，放到容器名后面会被
/// 当成要执行的命令的一部分。工作目录**显式**切到 `/app`：不靠 buildah 去读镜像的
/// `WorkingDir`，那样这一层的行为就随 buildah 版本漂。
///
/// 分组写成 `{ 换行 … 换行 }`：换行本身已经终结了那串命令，再补一个 `;` 就成了第二个分隔符，
/// bash 会报 `syntax error near unexpected token ';'`（实测）。空脚本补一个 `:`——`{` 与 `}`
/// 之间什么也没有同样是语法错误，而不带 `;` 的写法没法靠加分隔符绕开它。
fn run_argv(name: &str, script: &str, extra: &[&str]) -> Vec<String> {
    let body = if script.trim().is_empty() {
        ":"
    } else {
        script
    };
    let full = format!("cd {WORK_DIR} && {{\n{body}\n}}");
    let mut argv: Vec<String> = vec!["run".to_string()];
    argv.extend(extra.iter().map(|arg| arg.to_string()));
    argv.push(name.to_string());
    argv.push("--".to_string());
    argv.push("bash".to_string());
    argv.push("-c".to_string());
    argv.push(full);
    argv
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

    fn exec_seam(&self) -> ExecSeam {
        // 外部程序跑的机器就是**这道题的那台容器**，所以缝开在这里而不是组合根：题面依赖的
        // 那棵树、判分要读的现场都在它里面，换一台机器跑出来的补丁不是这道题的补丁。
        ExecSeam::Ready(Arc::new(ContainerForeignRunner {
            container: self.container.clone(),
        }))
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

/// 外部流派的执行面：在**这一道题的那台容器**里起它，收回它在树上留下的那个补丁。
struct ContainerForeignRunner {
    container: Arc<Container>,
}

#[async_trait]
impl ExecRunner for ContainerForeignRunner {
    async fn run(&self, req: ExecRequest<'_>) -> anyhow::Result<ExecOutcome> {
        // 题面进容器：外部流派怎么读它由它定，我们只承诺「这一段文字是这道题」。
        self.container
            .write_text(FOREIGN_PROBLEM_PATH, req.problem)
            .await?;
        let before = self.container.snapshot_tree().await?;
        let started = std::time::Instant::now();
        let out = self
            .container
            .shell_with(
                &foreign_script(req.entrypoint, req.limits),
                &FOREIGN_RUN_ISOLATION,
            )
            .await?;
        let elapsed = started.elapsed();
        let after = self.container.snapshot_tree().await?;
        let patch = self.container.tree_patch(&before, &after).await?;

        let finish = foreign_finish(out.code);
        if finish != FinishReason::Answered {
            tracing::warn!(
                flow = req.flow,
                code = ?out.code,
                elapsed_secs = elapsed.as_secs(),
                stderr = %tail(&out.stderr, 400),
                "the foreign flow did not report success; its tree changes are still read back"
            );
        }
        // 撞上限也照样读补丁：它可能改了一半，而判分判的是那棵树。空补丁在这里是**真读数**
        // （它什么都没改），与「它根本没起起来」（那是 `Err`）不是一件事。
        Ok(ExecOutcome { patch, finish })
    }
}

/// 起一支外部流派的脚本：三条上限都落在这里。
///
/// 上限落在**这一次执行**上而不是容器上：这台容器判分那一步还要用，给容器上一条内存界会把
/// 判分自己也框住，那是另一个改动。所以用 shell 能设的这几条界，它们只作用在这段里。
fn foreign_script(entrypoint: &str, limits: ExecLimits) -> String {
    let wall = limits.wall_clock.as_secs().max(1);
    // `RLIMIT_CPU` 的单位是**核·秒**：给 n 个核、T 秒，最多就是 n·T 个 CPU 秒。这是从那
    // 一对值**导出**的一条界，不是另拍的一个数。
    let cpu_secs = ((limits.cpus.max(0.0) as f64) * wall as f64)
        .ceil()
        .max(1.0) as u64;
    let memory_kb = (limits.memory_bytes / 1024).max(1);
    format!(
        "if ! ulimit -v {memory_kb}; then \
           echo \"cannot apply the memory cap: ulimit -v {memory_kb}\" >&2; exit 125; \
         fi\n\
         if ! ulimit -t {cpu_secs}; then \
           echo \"cannot apply the cpu cap: ulimit -t {cpu_secs}\" >&2; exit 125; \
         fi\n\
         timeout -k 10 {wall} bash -lc {cmd}",
        cmd = shq(entrypoint)
    )
}

/// 这一跑算不算它自己收工的。
///
/// 撞上限与「它自己说不做了」必须分开：前者要问上限是不是给少了，后者要换方法。判据是退出码
/// ——`timeout(1)` 到点动手用的是 124，`ulimit -t`（CPU 界）到点用的是 SIGXCPU（152），
/// `ulimit -v` 撞上一般表现为进程被 OOM 掉（137）。这几个码只在这条脚本设的界上出现，而
/// 这几个界都是组合根给的。
fn foreign_finish(code: Option<i32>) -> FinishReason {
    match code {
        Some(0) => FinishReason::Answered,
        // 152 = 128 + 24（SIGXCPU，CPU 界）。
        Some(124) | Some(152) => FinishReason::ResourceLimit,
        // 137 = 128 + 9（SIGKILL）：要么是 `timeout -k` 那一枪，要么是内存界把它 OOM 掉。
        // 分不清是哪一件，但两件都是**我们设的界**打的，所以按界记。
        Some(137) => FinishReason::ResourceLimit,
        _ => FinishReason::Failed,
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
        // 起容器之前先读一次「这份镜像在不在本地」。**行为一个字不改**——`buildah from` 仍会
        // 在缺镜像时顺手去拉——改的只是它失败时那句话说得清缺的是哪一样：与「这台机器坏了」
        // 共用一句「命令没跑完」时，读的人会被送去查 buildah，而真正缺的是一份没取下来的镜像。
        let refs = self.runtime.image_refs().await;
        let present = refs.map(|set| set.contains(&image));
        if present == Some(false) {
            tracing::warn!(
                image = %image,
                local_refs = refs.map(|set| set.len()).unwrap_or(0),
                "this instance's image is not in the host's container store; starting a container \
                 from it means fetching it first"
            );
        }
        let container = match Container::create(self.runtime.clone(), &image, &name).await {
            Ok(container) => Arc::new(container),
            // 只有「查过、且不在」时才改判词。查不出来（`None`）时原样往上抛——把「问不出来」
            // 说成「本地没有」是拿一个没量到的读数当量到了。
            Err(e) if present == Some(false) => anyhow::bail!(
                "the instance image {image} is not in this host's container store ({} refs are), \
                 and starting a container from it did not finish: this is the fetch, not the case \
                 failing to run. Fetch it once (`buildah pull {image}`) and the case can run; the \
                 underlying failure was: {e}",
                refs.map(|set| set.len()).unwrap_or(0)
            ),
            Err(e) => return Err(e),
        };

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

#[cfg(test)]
mod tests {
    use super::*;

    /// 收进来的必须是**能匹配上的 ref**。实测里那份 `buildah images` 的输出长这样：
    /// 有 tag 的题镜像、有 tag 的普通镜像，加上没有名字的中间层（`<none>:<none>`）。
    /// 中间层被收进来时集合会大几个，而这条判据**照样绿**——所以这一格的见证要反过来：
    /// 断言那些 `<none>` **不在**结果里，而不是断言总数。
    #[test]
    fn the_image_census_keeps_refs_and_drops_the_unnamed() {
        let stdout = "\
docker.io/library/ubuntu:24.04
ghcr.nju.edu.cn/scaleapi/swe-bench_pro-v2:instance_NodeBB__NodeBB-04998908ba6721d64eba79ae3b65a351dcfbc5b5-vnan
<none>:<none>
docker.m.daocloud.io/library/busybox:1.36
<none>:<none>

";
        let refs = parse_image_refs(stdout);
        assert_eq!(refs.len(), 3, "got {refs:?}");
        assert!(refs.contains(
            "ghcr.nju.edu.cn/scaleapi/swe-bench_pro-v2:\
             instance_NodeBB__NodeBB-04998908ba6721d64eba79ae3b65a351dcfbc5b5-vnan"
        ));
        assert!(!refs.contains("<none>:<none>"));
        // 空输出是「这台机器上什么镜像都没有」，不是「问不出来」——两者由调用方分开表示
        // （`Some(empty)` 与 `None`），所以这里必须是空集合而不是别的。
        assert!(parse_image_refs("").is_empty());
    }

    /// 我们拿去查的那个 ref，必须与普查印出来的**逐字节同形**。
    ///
    /// 这是这一层唯一会**静默**读错的地方：形状一旦分家（换过镜像站、`Name` 打出来带上了
    /// 默认 registry 前缀、或者题号里的斜杠被规范化过），普查里就一条都匹配不上 ⇒ 每一道题
    /// 都被判成「镜像不在本地」，而那是一句**假读数**——镜像明明在。这条判据不看条数，
    /// 看的是「同一个名字在两个方向上相等」。
    #[test]
    fn the_ref_we_look_up_is_the_ref_the_census_prints() {
        let runtime = runtime_pointing_at(Some("ghcr.nju.edu.cn"));
        let upstream = runtime_pointing_at(None);
        let instance = "instance_NodeBB__NodeBB-04998908ba6721d64eba79ae3b65a351dcfbc5b5-vnan";

        let wanted = runtime.image_ref(instance);
        assert_eq!(
            wanted,
            format!("ghcr.nju.edu.cn/{IMAGE_REPO}:{instance}"),
            "the ref carries the instance id as its tag"
        );
        // 镜像站没设时落回上游那一个：两者是同一段代码的两个分支，都要能对上。
        assert_eq!(
            upstream.image_ref(instance),
            format!("{IMAGE_HOST}/{IMAGE_REPO}:{instance}")
        );

        let printed = format!("docker.io/library/ubuntu:24.04\n{wanted}\n<none>:<none>\n");
        let refs = parse_image_refs(&printed);
        assert!(
            refs.contains(&wanted),
            "the census prints the very name we look up; got {refs:?}"
        );
        // 反面：换过镜像站之后，同一个 store 里的那份镜像就**不该**再被认成在本地——它是
        // 另一个 ref 底下的东西。少了这一句，上面那条断言在「怎么都算命中」的实现下也绿。
        assert!(!refs.contains(&upstream.image_ref(instance)));
    }

    fn runtime_pointing_at(mirror: Option<&str>) -> BuildahRuntime {
        BuildahRuntime {
            prefix: vec!["sudo".into(), "-n".into(), "buildah".into()],
            mirror: mirror.map(str::to_string),
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            images: tokio::sync::OnceCell::new(),
        }
    }

    fn limits() -> ExecLimits {
        ExecLimits {
            wall_clock: Duration::from_secs(900),
            cpus: 2.0,
            memory_bytes: 4 * 1024 * 1024 * 1024,
        }
    }

    fn spec(name: &str, columns: &str, entrypoint: &str) -> String {
        format!("{name}\t{columns}\t{entrypoint}")
    }

    #[test]
    fn a_flow_declaration_carries_its_columns_and_its_command() {
        let raw = format!(
            "# 这次要接的两支\n{}\n{}\n",
            spec(
                "swe-agent",
                "swe-bench-pro",
                "sweagent run --problem /tmp/f.txt"
            ),
            spec(
                "live-swe-agent",
                "swe-bench-pro",
                "python -m live --in /tmp/f.txt"
            ),
        );
        let flows = parse_flows(&raw, limits()).unwrap();
        assert_eq!(flows.len(), 2);
        assert_eq!(flows[0].name, "swe-agent");
        assert_eq!(flows[0].columns, vec!["swe-bench-pro".to_string()]);
        // 命令里什么都会出现（引号、分号、管道），所以它必须原样读进来。
        assert_eq!(flows[0].entrypoint, "sweagent run --problem /tmp/f.txt");
        assert_eq!(flows[1].name, "live-swe-agent");
        assert_eq!(flows[0].limits, limits());
    }

    #[test]
    fn no_declaration_is_not_a_broken_declaration() {
        assert!(parse_flows("", limits()).unwrap().is_empty());
        assert!(parse_flows("\n\n  \n# 只有注释\n", limits())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_declaration_that_is_not_three_fields_is_refused_with_its_own_line() {
        // 少一段：命令被吃成了列名。
        let err = parse_flows("swe-agent\tswe-bench-pro", limits()).unwrap_err();
        assert!(err.contains("swe-agent\tswe-bench-pro"), "{err}");
        // 多一段：多半是命令里混进了制表符，读成两种意思都不对。
        let err = parse_flows("a\tb\tcmd\textra", limits()).unwrap_err();
        assert!(err.contains("TAB"), "{err}");
        // 名字或命令是空的。
        assert!(parse_flows("\tswe-bench-pro\tcmd", limits()).is_err());
        assert!(parse_flows("a\tswe-bench-pro\t", limits()).is_err());
    }

    #[test]
    fn a_row_that_lands_on_no_column_is_refused_rather_than_never_run() {
        let err = parse_flows("swe-agent\t\tcmd", limits()).unwrap_err();
        assert!(err.contains("no column"), "{err}");
        // 重复的名字在表里分不开。
        let raw = format!(
            "{}\n{}",
            spec("swe-agent", "swe-bench-pro", "a"),
            spec("swe-agent", "swe-bench-pro", "b")
        );
        let err = parse_flows(&raw, limits()).unwrap_err();
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn the_three_caps_all_land_in_the_script() {
        let script = foreign_script("sweagent solve", limits());
        // 内存界按 KB 给（ulimit -v 的单位是 KB）。
        assert!(script.contains("ulimit -v 4194304"), "{script}");
        // CPU 界是**导出**的：2 核 × 900 秒 = 1800 核·秒，不是另拍的数。
        assert!(script.contains("ulimit -t 1800"), "{script}");
        assert!(script.contains("timeout -k 10 900"), "{script}");
        // 命令原样进去，且被引起来：不引的话里面的元字符会静默改写参数。
        assert!(script.contains("'sweagent solve'"), "{script}");
    }

    #[test]
    fn the_command_is_quoted_so_its_metacharacters_do_not_rewrite_the_call() {
        let script = foreign_script("a; rm -rf /tmp/x", limits());
        assert!(script.contains("'a; rm -rf /tmp/x'"), "{script}");
        let script = foreign_script("echo 'x'", limits());
        assert!(script.contains(r"'echo '\''x'\'''"), "{script}");
    }

    #[test]
    fn hitting_a_cap_we_set_is_not_reported_as_the_flow_giving_up() {
        assert_eq!(foreign_finish(Some(0)), FinishReason::Answered);
        assert_eq!(foreign_finish(Some(124)), FinishReason::ResourceLimit);
        assert_eq!(foreign_finish(Some(152)), FinishReason::ResourceLimit);
        assert_eq!(foreign_finish(Some(137)), FinishReason::ResourceLimit);
        assert_eq!(foreign_finish(Some(1)), FinishReason::Failed);
        // 没有退出码＝被信号打断，不是它自己收工。
        assert_eq!(foreign_finish(None), FinishReason::Failed);
    }

    #[test]
    fn the_foreign_run_asks_for_its_isolation_and_the_grading_run_does_not() {
        let foreign = run_argv("c1", "echo hi", &FOREIGN_RUN_ISOLATION);
        // 四个开关都在，且都在容器名**之前**：buildah 的 flag 只在那一位上被认，放到容器名
        // 后面会被当成要执行的命令的一部分。
        let name_at = foreign.iter().position(|arg| arg == "c1").expect("名字在");
        for flag in FOREIGN_RUN_ISOLATION {
            let at = foreign
                .iter()
                .position(|arg| arg == flag)
                .unwrap_or_else(|| panic!("{flag} 要出现在这一跑里"));
            assert!(at < name_at, "{flag} 要在容器名之前");
        }
        // 判分那一跑不带它们：它跑的是上游自己的脚本，改那一跑的读数是另一个改动。
        let grading = run_argv("c1", "echo hi", &[]);
        assert!(!grading.iter().any(|arg| arg == "--isolation"));
        assert!(!grading.iter().any(|arg| arg == "--network"));
        assert_eq!(grading[0], "run");
        assert_eq!(grading[1], "c1");
    }

    #[test]
    fn the_isolation_flags_do_not_rewrite_the_script_the_run_gets() {
        let bare = run_argv("c1", "echo hi", &[]);
        let isolated = run_argv("c1", "echo hi", &FOREIGN_RUN_ISOLATION);
        // 脚本是 argv 的最后一段，两种跑法给出的是同一段：隔离是**这一跑**的属性，不是
        // 换一种脚本。
        assert_eq!(bare.last(), isolated.last());
        assert_eq!(
            bare.last().unwrap(),
            &format!("cd {WORK_DIR} && {{\necho hi\n}}")
        );
    }

    // 这一跑拿到的是**私有**的命名空间，且拿不到宿主进程的环境。这两条是「别人的程序跑在
    // 哪儿」的判据，不是 buildah 的默认值——默认值来自宿主配置（实测 `--isolation chroot`
    // 下容器的 net/pid 命名空间与宿主逐字相同），所以它要由这一侧的开关要来，且要能被量。
    //
    // 要一台能起容器的机器，以及 store 里已经有的一份镜像。手工跑法：
    //   cargo test -p cogneva --lib eval_container::tests::the_foreign_run_gets_its_own_namespaces -- --ignored --nocapture
    #[ignore = "needs a container machine with a buildah store and an image already in it"]
    #[tokio::test]
    async fn the_foreign_run_gets_its_own_namespaces() {
        let runtime = Arc::new(
            BuildahRuntime::detect()
                .await
                .expect("探测不该失败")
                .expect("这台宿主上要有可用的容器机器"),
        );
        let name = format!("cogneva-isolation-probe-{}", std::process::id());
        let container = Container::create(runtime.clone(), "docker.io/library/ubuntu:24.04", &name)
            .await
            .expect("从 store 里起一台探针容器");
        // 工作目录先建出来：`run_argv` 显式 `cd /app`，不靠镜像的 WorkingDir 恰好是什么。
        let setup: Vec<String> = ["run", name.as_str(), "--", "mkdir", "-p", WORK_DIR]
            .iter()
            .map(|arg| arg.to_string())
            .collect();
        runtime.exec(&setup).await.expect("建工作目录");
        // 探针要量的那条环境变量，先在**本进程**里设上：这样「容器里没有它」才是读出来的。
        std::env::set_var("COGNEVA_ISOLATION_PROBE", "1");

        let out = runtime
            .exec(&run_argv(
                &name,
                "readlink /proc/self/ns/net /proc/self/ns/pid; env",
                &FOREIGN_RUN_ISOLATION,
            ))
            .await
            .expect("跑探针");
        assert_eq!(out.code, Some(0), "探针要跑完：{}", tail(&out.stderr, 400));

        let host_net = std::fs::read_link("/proc/self/ns/net")
            .expect("读宿主自己的 net 命名空间")
            .display()
            .to_string();
        let first = out.stdout.lines().next().unwrap_or_default().to_string();
        assert_ne!(first, host_net, "容器要有自己的 net 命名空间");
        assert!(
            !out.stdout.contains("COGNEVA_ISOLATION_PROBE"),
            "宿主的进程环境不许漏进容器：{}",
            out.stdout
        );
        container.remove().await.expect("把探针容器收掉");
    }
}
