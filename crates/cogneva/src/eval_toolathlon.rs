//! toolathlon 的执行面：**一道题一个 Pod**。
//!
//! 为什么不和 swe-bench-pro 一样用本机那台 buildah：这一列要的不是「跑一条命令」，是
//! **一台活着的环境**——postgres 常驻、MCP server 常驻、外壳一步一步反复进入同一台家伙。
//! buildah 能表达的只有「在一台新容器里跑一条命令、跑完就结束」：一次 `run` 里起的后台进程
//! 随这次 `run` 一起没（实测 `(sleep 300 &)` 之后下一次 `run` 里 `pgrep -c sleep` 是 0），没有
//! `--detach` 这一类开关，`--network` 也只收 `private`/`none`/`ns:path`/`host` 四种、没有具名
//! 网络。**这是寿命对不上，不是缺一台机器**——换到集群原语，同样的三件事逐条反过来
//! （`kubectl exec` 之间状态与后台进程都活着、退出码读得回来）。
//!
//! 所以这一层做的是**接线**：把一道题写成一份 Pod 清单、按官方那两件脚本的顺序驱动它、
//! 把退出码与逐条检查读回来。判分规则在 `cog-eval` 那一侧（`ToolathlonJudge::grade`），
//! 在这里再写一份就是另起一份正本。
//!
//! 一道题的形状（每一处都对着官方那份 compose 的一行）：
//!
//! - 一个 `postgres` sidecar，库名/用户/口令与 compose 相同；`db/init.sql.gz` 走
//!   **官方那个机制**进库——initContainer 从 pack 镜像里把它复制进一个 `emptyDir`，那个
//!   emptyDir 挂成 postgres 的 `/docker-entrypoint-initdb.d`。不自己写导入脚本：compose 里
//!   那一行挂的就是这个目录，重新发明一遍就多一处会漂的口径。
//! - 一个 pack 容器，脚本跑在它里面。它与 postgres **共享 Pod 的网络命名空间**，所以
//!   `127.0.0.1:5432` 就是那台库——不要 Service、不要 DNS、不要给命名空间开洞。
//! - 三步都由 `kubectl exec` 驱动：等库就绪 → `preprocess/main.py` → `evaluation/main.py`。
//!   这正是 buildah 做不到的那三件事的形状。

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::{SFError, SFResult, ToolDefinition, ToolExecutor};
use cog_eval::{
    tools_bound_to, CaseEnv, EvalCase, ExecSeam, ToolFactory, ToolathlonBackend, ToolathlonCheck,
    ToolathlonReport,
};

use crate::eval_mcp_in_pod::{
    connect_declared, read_server_configs, sh_quote as shq, ServerPaths, ToolFace,
};
use crate::eval_toolathlon_local::LocalTools;

/// pack 镜像里的工作根。官方 compose 里 agent 容器就是 `working_dir: /workspace`。
const CONTAINER_WORK_DIR: &str = "/workspace";
/// 题树在**容器里**的根。镜像把整棵 gym 复制进 `/workspace`（`COPY . .`），所以它的
/// `tasks/finalpool/` 就是这里——与外壳读 case 时用的宿主路径 `TOOLATHLON_TASKS_DIR` 是
/// **同一棵树的两处名字**，所以两处都由常量给，不各写一遍字面量。
const CONTAINER_TASKS_DIR: &str = "/workspace/tasks/finalpool";
/// pack 镜像里那支 venv。Dockerfile 把 `uv venv /opt/venv` 建在 `/workspace` 外面
/// （免得被卷挂载盖住），并把它放在 `PATH` 最前。
const CONTAINER_PYTHON: &str = "/opt/venv/bin/python";
/// 库的就绪判据落点。走 `PATH` 不写绝对路径：它由 apt 的 `postgresql-client` 装进来，
/// 位置不是这份代码该记住的事——记错了症状是「等库等满 300 秒然后报连不上」。
const CONTAINER_PG_ISREADY: &str = "pg_isready";
/// 这道题的 agent 工作区（容器内）。评估脚本按 `--agent_workspace` 读它，
/// 要写的产物也落在这里——所以**两个方向用的是同一个常量**。
pub(crate) const CONTAINER_AGENT_WORKSPACE: &str = "/workspace/cogneva-eval/agent_workspace";
/// 上游那 25 份 MCP server 配置在镜像里的位置。`COPY . .` 把整棵 gym 放进
/// `/workspace`，所以它就是 `configs/mcp_servers/`——题面用的 server 名要在这一堆里找。
const CONTAINER_MCP_CONFIGS_DIR: &str = "/workspace/configs/mcp_servers";
/// 编译好的 local server 在镜像里的位置。
///
/// 它**不是** Python 那个缺省值 `./local_servers`：Dockerfile 把依赖装进
/// `/opt/local_servers`（`/workspace` 会被卷盖住），而 compose、`run_parallel.sh`、
/// `scripts/run_containerized.sh` 三处**都**显式给 `LOCAL_SERVERS_PATH=/opt/local_servers`。
/// 配置里的 `${local_servers_paths}` 是按这个值替换的，所以这一处同时是 Pod env 与
/// 占位符替换的来源——两处各写一遍就等于有第二份会漂的定义。
const CONTAINER_LOCAL_SERVERS: &str = "/opt/local_servers";
/// initContainer 复制 `db/init.sql.gz` 的中转目录（emptyDir）。
const INIT_VOLUME_DIR: &str = "/cogneva-initdb";
/// compose 里那一行挂的就是这个目录，postgres 官方入口脚本自己会处理 `.gz`。
const PG_INITDB_DIR: &str = "/docker-entrypoint-initdb.d";
const INIT_SQL_IN_IMAGE: &str = "/workspace/db/init.sql.gz";

/// 库名/用户/口令：与官方 compose 逐字相同。改这三个字面量就是把这道题换了一道题。
const PG_DATABASE: &str = "toolathlon_gym";
const PG_USER: &str = "eigent";
const PG_PASSWORD: &str = "camel";

/// pack 镜像的 ref。默认写成 registry 里那个名字——镜像进集群的正路是
/// `buildah push … localhost:30500/<repo>:<tag>`，集群里的 Pod 直接按这个名字拉。
const DEFAULT_PACK_IMAGE: &str = "localhost:30500/toolathlon-pack:latest";
/// postgres sidecar 的镜像。compose 写的是 `postgres:15`。
const DEFAULT_PG_IMAGE: &str = "docker.io/library/postgres:15";

/// 起一台 Pod、驱动它、回收它。整条命令从这一个旋钮读，所以指到另一台机器、换一个二进制
/// 都不用改代码。默认先试 `sudo -n kubectl`：这台宿主上普通用户读不到 kubeconfig
/// （与 `BuildahRuntime` 先试 `sudo -n buildah` 同一个理由）。
const ENV_KUBECTL: &str = "COGNEVA_EVAL_KUBECTL";
const ENV_NAMESPACE: &str = "COGNEVA_EVAL_TOOLATHLON_NAMESPACE";
const ENV_PACK_IMAGE: &str = "COGNEVA_EVAL_TOOLATHLON_IMAGE";
const ENV_PG_IMAGE: &str = "COGNEVA_EVAL_TOOLATHLON_PG_IMAGE";
const ENV_TIMEOUT_SECS: &str = "COGNEVA_EVAL_TOOLATHLON_TIMEOUT_SECS";
/// pack 容器的 limits 覆盖位。两个必须一起给：它们是一对，单独改一个只会在额度
/// 或内存的另一侧撞墙（额度按 limits 求和，内存不够则是 OOM）。
const ENV_PACK_CPU_LIMIT: &str = "COGNEVA_EVAL_TOOLATHLON_CPU_LIMIT";
const ENV_PACK_MEMORY_LIMIT: &str = "COGNEVA_EVAL_TOOLATHLON_MEMORY_LIMIT";
/// 运行时的开关：设成 `off` 就是这一次不接。
const ENV_RUNTIME: &str = "COGNEVA_EVAL_TOOLATHLON_RUNTIME";
const DEFAULT_NAMESPACE: &str = "cogneva";

/// 单次 Pod 命令的时间上界（秒）。默认比 swe-bench-pro 那一侧宽：一道题要起库、导入
/// `init.sql.gz`、跑前处理、再跑判分，其中导入那一步本身就按分钟计。这是一个**安全上界**
/// 不是量出来的界，所以它可覆盖；它的意义是让一条卡住的命令变成一次带原因的失败。
const DEFAULT_TIMEOUT_SECS: u64 = 1800;

/// 等库就绪的上界（秒）。超过就报「库没起来」，不是继续跑下去把每一格都判成错。
const PG_READY_DEADLINE_SECS: u64 = 300;

/// pack 容器（跑题的那一个）的 limits。**这两个数是被额度逼出来的，不是拍的**：
/// 命名空间的 `ResourceQuota` 按 limits 求和，而平台的常驻服务已经占掉其中七成半
/// （实测 18100m / 23 CPU、41.6 / 46 GiB），剩不到 5 CPU / 4.4 GiB。
/// 超过它就是 API server 直接 403，一题都跑不起来——比「跑得慢」更糟，所以这里取余量，
/// 不取「这道题最多能用多少」：pack 2 CPU ＋ sidecar 1 CPU ＝ 3 CPU，剩下的额度还容得下
/// 第二台 Pod 同时跑，而按 4 CPU 写就只容得下一台。两个值成对给：单独抬一个只会换一种
/// 方式撞墙。
/// 内存 3 GiB 有真跑支撑：3 GiB 的 Pod 里一道真题的 `preprocess/main.py` 6 秒、
/// `evaluation/main.py` 2 秒跑完。CPU 没有同等的读数——那次跑的是 3 CPU 的 Pod，
/// 2 CPU 是按「跑得动同一类脚本」外推的上界，不是量出来的下界。
const DEFAULT_PACK_CPU_LIMIT: &str = "2";
const DEFAULT_PACK_MEMORY_LIMIT: &str = "3Gi";
/// postgres sidecar 的 limits。它只装一道题要用的那点数据，1 CPU / 1 GiB 与探针同一档——
/// 探针那台在 1 CPU 下把一道真题的库初始化跑完了。
const DEFAULT_PG_CPU_LIMIT: &str = "1";
const DEFAULT_PG_MEMORY_LIMIT: &str = "1Gi";

/// 一次 `kubectl` 调用的结局。
#[derive(Debug, Clone)]
pub(crate) struct RunOutcome {
    pub(crate) stdout: String,
    pub(crate) stderr: String,
    /// `None` ＝ 被信号打断，没拿到退出码。
    pub(crate) code: Option<i32>,
}

/// 这台宿主上的一台 Pod。`kubectl` 前缀在构造时**探**出来，不写死。
pub struct PodRuntime {
    prefix: Vec<String>,
    namespace: String,
    timeout: Duration,
}

impl PodRuntime {
    /// 按环境接上一台「能起 Pod 的机器」。`Ok(None)` ＝ 这台宿主上没有可用的 kubectl、
    /// 或者这次显式关掉了它。两种情况都不该拦住别的列跑，由调用方记进缺席声明。
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
        let namespace = std::env::var(ENV_NAMESPACE)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());

        let candidates: Vec<Vec<String>> = match std::env::var(ENV_KUBECTL) {
            Ok(raw) if !raw.trim().is_empty() => {
                vec![raw.split_whitespace().map(str::to_string).collect()]
            }
            _ => vec![
                vec!["sudo".into(), "-n".into(), "kubectl".into()],
                vec!["kubectl".into()],
            ],
        };
        for prefix in candidates {
            if prefix.is_empty() {
                continue;
            }
            if probe(&prefix, &namespace).await {
                return Ok(Some(Self {
                    prefix,
                    namespace,
                    timeout,
                }));
            }
        }
        Ok(None)
    }

    fn command(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(&self.prefix[0]);
        cmd.args(&self.prefix[1..]);
        cmd.arg("-n").arg(&self.namespace);
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd
    }

    /// 拼一台不走环境探测的 runtime，只给单测用。
    ///
    /// [`Self::detect`] 要真跟集群说上话才返回 `Some`，所以它的结果**依赖这台机器当前
    /// 的状态**——拿它去钉 argv 的形状，测的就不是 argv 了。argv 是纯函数，给它一组
    /// 固定的前缀就能钉住。
    #[cfg(test)]
    pub(crate) fn for_test(prefix: Vec<String>, namespace: &str) -> Self {
        Self {
            prefix,
            namespace: namespace.to_string(),
            timeout: Duration::from_secs(60),
        }
    }

    /// 跑一条 `kubectl` 命令。超时是**失败**不是空结果：吞掉它，这一格的分就读成
    /// 「模型没做对」，而真相是「这台机器没跑完」。
    pub(crate) async fn exec(&self, args: &[String]) -> anyhow::Result<RunOutcome> {
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

    /// 一条**长命**的 `kubectl exec -i` 的程序与参数，交给 stdio 传输去起。
    ///
    /// 与 [`Self::exec`] 的差别只在寿命，不在命令：那条等一条命令跑完再把输出取回来，
    /// 这条要一直握着两根管子（本地写的是远端程序的 stdin，读的是它的 stdout），整场
    /// 对话共用一次 exec。所以它不能走 [`Self::exec`]——那个函数在命令结束时就把
    /// 管子的两端都收了。
    ///
    /// `-i` 是这条的全部要点：没有它，本地这侧写不进去，远端 server 读不到请求。
    pub fn stdio_exec_argv(
        &self,
        pod: &str,
        container: &str,
        remote_script: &str,
    ) -> (String, Vec<String>) {
        let mut args: Vec<String> = self.prefix[1..].to_vec();
        args.push("-n".into());
        args.push(self.namespace.clone());
        args.extend(["exec".into(), "-i".into(), pod.into()]);
        // 容器名显式给：一个 Pod 里不止一个容器（还有库），靠默认选中的那个是别人
        // 的规则，选中谁不该由集群决定。
        args.extend(["-c".into(), container.into()]);
        // `--` 之后按 argv **逐字**交给远端，不经任何 shell 解释——远端那条命令串自己
        // 要不要 shell 是它的事（本列要 `/bin/sh -c`，因为 cwd/env 只能写在串里）。
        args.extend([
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            remote_script.to_string(),
        ]);
        (self.prefix[0].clone(), args)
    }
}

/// 这个前缀能不能真把这一列的题跑起来。
///
/// 探的必须是**它接下来要做的那个动作的权限**。旧版本跑 `get namespace <n> -o name`：
/// 读的是命名空间这个资源，而这一列真正要做的是 `apply` 建 Pod、`exec` 进去。
/// RBAC 里 `get namespaces` 与 `create pods` 是两条独立授权，一个只读身份（观测、审计、
/// 任何 `view` ClusterRole）能过旧探针，然后在第一道题上拿 403——480 例于是从「这一列没有
/// 后端」变成「480 例跑错了」，而报表里这两格是要分开的。
/// `auth can-i create pods` 问的就是建 Pod 这个动作，答案只有 `yes`/`no`，不需要解析正文。
async fn probe(prefix: &[String], namespace: &str) -> bool {
    let mut cmd = tokio::process::Command::new(&prefix[0]);
    cmd.args(&prefix[1..])
        .args(["-n", namespace, "auth", "can-i", "create", "pods"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let Ok(child) = cmd.kill_on_drop(true).spawn() else {
        return false;
    };
    match tokio::time::timeout(Duration::from_secs(20), child.wait_with_output()).await {
        // 有的版本对 `no` 也退 0，所以判输出而不只判退出码；反过来 `yes` 却退非 0
        // 只可能是包装脚本出错，那时也不能算「能建」。
        Ok(Ok(out)) => {
            out.status.success()
                && String::from_utf8_lossy(&out.stdout)
                    .trim()
                    .eq_ignore_ascii_case("yes")
        }
        _ => false,
    }
}

/// 一道题的 Pod 名字。
///
/// 题名里的字符集实测在 503 道题里只有 `[a-z0-9-]`，但**判据不能建在「实测如此」上**——
/// 出一道上游新加的名字带下划线或点，`kubectl` 会拿一条与题无关的报错把这一格判成失败。
/// 所以这里把不合法字符一律换成 `-`，再截尾，再接 8 位十六进制：那 8 位来自题号，所以
/// 两道长题名前缀撞在一起时仍分得开，重跑同一题时又是同一个名字（同名先删再建，
/// 上一次跑剩下的半成品不该拦住这一次）。
fn pod_name(case_id: &str) -> String {
    let mut safe: String = case_id
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' {
                c
            } else if c.is_ascii_uppercase() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    while safe.starts_with('-') || safe.ends_with('-') {
        safe = safe.trim_matches('-').to_string();
    }
    if safe.len() > 40 {
        safe.truncate(40);
        while safe.ends_with('-') {
            safe.pop();
        }
    }
    let digest = short_hash(case_id);
    format!("ta-{safe}-{digest}")
}

/// 名册里那 8 位十六进制。用仓库里已有的 sha256，不引新依赖。
fn short_hash(seed: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(seed.as_bytes());
    let digest = hasher.finalize();
    hex::encode(&digest[..4])
}

/// 一道题在容器里的目录。
fn container_task_dir(case: &EvalCase) -> String {
    format!("{CONTAINER_TASKS_DIR}/{}", case.name)
}

/// 判分脚本要读的 groundtruth 目录。它在题目录里，跟着镜像走。
fn container_groundtruth_dir(case: &EvalCase) -> String {
    format!("{}/groundtruth_workspace", container_task_dir(case))
}

/// 一台 Pod 的资源上限。四个值都从构造时的那次探测/覆盖读进来，不在这里写第二遍。
#[derive(Debug, Clone, PartialEq, Eq)]
struct PodLimits {
    pack_cpu: String,
    pack_memory: String,
    pg_cpu: String,
    pg_memory: String,
}

impl Default for PodLimits {
    fn default() -> Self {
        Self {
            pack_cpu: DEFAULT_PACK_CPU_LIMIT.to_string(),
            pack_memory: DEFAULT_PACK_MEMORY_LIMIT.to_string(),
            pg_cpu: DEFAULT_PG_CPU_LIMIT.to_string(),
            pg_memory: DEFAULT_PG_MEMORY_LIMIT.to_string(),
        }
    }
}

/// 一份 Pod 清单。纯函数、吃两个镜像名、一个 Pod 名和一组资源上限，所以它的形状能被单测直接钉住。
///
/// 写 JSON 不写 YAML：`kubectl` 两条都收，而 JSON 是 serde_json 直接吐出来的，
/// 少引一个 YAML 依赖、也少一处手写缩进会错的地方。
fn manifest(pod: &str, pack_image: &str, pg_image: &str, limits: &PodLimits) -> serde_json::Value {
    let labels = serde_json::json!({
        // 一个新 component：现网那 5 条 NetworkPolicy 全部按这个键收窄，所以这台 Pod
        // **一条都不被选中**——同命名空间内的默认放行就是它要的网。这不是绕过策略，
        // 是策略本来就没打算管这一族。
        "app.kubernetes.io/component": "eval-toolathlon",
        "app.kubernetes.io/name": pod,
    });
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": pod, "labels": labels },
        "spec": {
            "restartPolicy": "Never",
            // 这台 Pod 里的程序不需要跟 API server 说话；不发令牌。
            "automountServiceAccountToken": false,
            "terminationGracePeriodSeconds": 1,
            "volumes": [
                { "name": "initdb", "emptyDir": {} }
            ],
            "initContainers": [{
                "name": "seed-initdb",
                "image": pack_image,
                // 复制官方那份 init 脚本，让 postgres 自己的入口脚本去加载它——
                // compose 里挂的就是同一个目录、同一个文件。
                "command": ["/bin/sh", "-c", format!("cp {INIT_SQL_IN_IMAGE} {INIT_VOLUME_DIR}/init.sql.gz")],
                "volumeMounts": [{ "name": "initdb", "mountPath": INIT_VOLUME_DIR }],
                "resources": {
                    "requests": { "cpu": "50m", "memory": "64Mi" },
                    "limits": { "cpu": "500m", "memory": "256Mi" }
                }
            }],
            "containers": [
                {
                    "name": "postgres",
                    "image": pg_image,
                    "env": [
                        { "name": "POSTGRES_DB", "value": PG_DATABASE },
                        { "name": "POSTGRES_USER", "value": PG_USER },
                        { "name": "POSTGRES_PASSWORD", "value": PG_PASSWORD },
                        { "name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata" }
                    ],
                    "ports": [{ "containerPort": 5432 }],
                    "volumeMounts": [{ "name": "initdb", "mountPath": PG_INITDB_DIR }],
                    "resources": {
                        "requests": { "cpu": "200m", "memory": "256Mi" },
                        "limits": { "cpu": limits.pg_cpu, "memory": limits.pg_memory }
                    }
                },
                {
                    "name": "app",
                    "image": pack_image,
                    "imagePullPolicy": "Always",
                    "workingDir": CONTAINER_WORK_DIR,
                    // 常驻：Pod 活着的时候这里要有个进程，好让 `kubectl exec` 进得来。
                    "command": ["/bin/sh", "-c", "while true; do sleep 30; done"],
                    "env": [
                        // 与 compose 的 `PGHOST: postgres` 等价：两者共享 Pod 的网络命名空间.
                        { "name": "PGHOST", "value": "127.0.0.1" },
                        { "name": "PGPORT", "value": "5432" },
                        { "name": "PGUSER", "value": PG_USER },
                        { "name": "PGPASSWORD", "value": PG_PASSWORD },
                        { "name": "PGDATABASE", "value": PG_DATABASE },
                        { "name": "LOCAL_SERVERS_PATH", "value": CONTAINER_LOCAL_SERVERS },
                        { "name": "PYTHON_BIN", "value": CONTAINER_PYTHON }
                    ],
                    "resources": {
                        "requests": { "cpu": "200m", "memory": "512Mi" },
                        "limits": { "cpu": limits.pack_cpu, "memory": limits.pack_memory }
                    }
                }
            ]
        }
    })
}

/// 判分脚本 stdout 里那两种行 → 逐条检查。
///
/// 认的是 `[PASS] <名字>` 与 `[FAIL] <名字>[: 细节]` 两种**形状**，不是「这行里有没有
/// `PASS`」——脚本自己的散文（比如 `Overall: 3/5 checks passed`）里也会出现这个词，
/// 按裸子串判会把统计行读成一条叫 `Overall` 的检查。
fn parse_checks(stdout: &str) -> Vec<ToolathlonCheck> {
    let mut checks = Vec::new();
    for line in stdout.lines() {
        let line = line.trim_start();
        let (passed, rest) = if let Some(rest) = line.strip_prefix("[PASS]") {
            (true, rest)
        } else if let Some(rest) = line.strip_prefix("[FAIL]") {
            (false, rest)
        } else {
            continue;
        };
        let name = rest.split(':').next().unwrap_or("").trim();
        if name.is_empty() {
            continue;
        }
        checks.push(ToolathlonCheck {
            name: name.to_string(),
            passed,
        });
    }
    checks
}

/// 一次评估跑完之后的回报。判词只看退出码（`ToolathlonJudge::grade` 那一侧），
/// 逐条检查只给归因用——但退出码与检查**必须都从同一次跑里读出来**。
fn report_from(out: &RunOutcome) -> ToolathlonReport {
    ToolathlonReport {
        exit_code: out.code.unwrap_or(-1),
        checks: parse_checks(&out.stdout),
        raw: format!("{}{}", out.stdout, out.stderr),
    }
}

/// toolathlon 的后端：一道题一个 Pod。
pub struct PodToolathlonBackend {
    runtime: Arc<PodRuntime>,
    pack_image: String,
    pg_image: String,
    limits: PodLimits,
}

impl PodToolathlonBackend {
    /// 按环境接上这台机器。镜像名也从环境读，默认是 registry 里那两个名字。
    pub async fn detect() -> anyhow::Result<Option<Self>> {
        let Some(runtime) = PodRuntime::detect().await? else {
            return Ok(None);
        };
        let image_or = |key: &str, fallback: &str| {
            std::env::var(key)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| fallback.to_string())
        };
        let defaults = PodLimits::default();
        Ok(Some(Self {
            runtime: Arc::new(runtime),
            pack_image: image_or(ENV_PACK_IMAGE, DEFAULT_PACK_IMAGE),
            pg_image: image_or(ENV_PG_IMAGE, DEFAULT_PG_IMAGE),
            limits: PodLimits {
                pack_cpu: image_or(ENV_PACK_CPU_LIMIT, &defaults.pack_cpu),
                pack_memory: image_or(ENV_PACK_MEMORY_LIMIT, &defaults.pack_memory),
                pg_cpu: defaults.pg_cpu,
                pg_memory: defaults.pg_memory,
            },
        }))
    }

    /// 起一台 Pod 并等它两个容器都就绪。
    async fn start(&self, case: &EvalCase) -> anyhow::Result<Arc<ToolathlonPod>> {
        let name = pod_name(&case.id);
        let pod = Arc::new(ToolathlonPod {
            runtime: self.runtime.clone(),
            name: name.clone(),
        });
        // 同名先清掉：上一次跑留下的半成品会让这一次的 apply 直接失败。
        pod.remove().await;

        let path = manifest_path(&name);
        std::fs::write(
            &path,
            manifest(&name, &self.pack_image, &self.pg_image, &self.limits).to_string(),
        )
        .map_err(|e| anyhow::anyhow!("cannot write {}: {e}", path.display()))?;
        let out = self
            .runtime
            .exec(&["apply".into(), "-f".into(), path.display().to_string()])
            .await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot create the pod for {}: {}",
                case.id,
                tail(&out.stderr, 800)
            );
        }

        self.wait_ready(&name).await?;
        Ok(pod)
    }

    /// 等到两个容器都 ready。**不等到就往下走**的后果是前处理连不上库，而报出来的错
    /// 读起来像这道题的代码坏了。
    async fn wait_ready(&self, name: &str) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(PG_READY_DEADLINE_SECS);
        loop {
            let out = self
                .runtime
                .exec(&[
                    "get".into(),
                    "pod".into(),
                    name.into(),
                    "-o".into(),
                    "jsonpath={.status.phase}|{range .status.containerStatuses[*]}{.name}={.ready};{end}"
                        .into(),
                ])
                .await?;
            let line = out.stdout.trim().to_string();
            if line.starts_with("Failed") {
                let why = self
                    .runtime
                    .exec(&[
                        "get".into(),
                        "pod".into(),
                        name.into(),
                        "-o".into(),
                        "jsonpath={.status.containerStatuses[*].state.waiting.reason}".into(),
                    ])
                    .await?;
                anyhow::bail!("pod {name} went to Failed: {}", why.stdout.trim());
            }
            let all_ready = line.contains("postgres=true") && line.contains("app=true");
            if all_ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                let why = self
                    .runtime
                    .exec(&["describe".into(), "pod".into(), name.into()])
                    .await?;
                anyhow::bail!(
                    "pod {name} was not ready within {PG_READY_DEADLINE_SECS}s: {}",
                    tail(&why.stdout, 1200)
                );
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

/// 清单落盘的位置。给 `kubectl apply -f` 一个真文件，而不是往 stdin 里灌——
/// 这条路径的每一次调用都用同一种交付方式，读的时候不必分辨「这一条是管道那条不是」。
fn manifest_path(pod: &str) -> PathBuf {
    std::env::temp_dir().join(format!("cogneva-toolathlon-{pod}.json"))
}

#[async_trait]
impl ToolathlonBackend for PodToolathlonBackend {
    async fn acquire(&self, case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
        let pod = self.start(case).await?;
        let task_dir = container_task_dir(case);

        // 库就绪：走 TCP 探，**不要**探 unix socket。postgres 官方入口脚本在跑
        // `/docker-entrypoint-initdb.d` 那一段时是 `--listen_addresses=''`，socket 已经可连、
        // TCP 还没开。探 socket 会在导入还没做完时就放行，而前处理正好要读那张还没灌完的表。
        let ready = pod
            .exec(&format!(
                "for i in $(seq 1 {PG_READY_DEADLINE_SECS}); do \
                   {CONTAINER_PG_ISREADY} -h 127.0.0.1 -p 5432 -U {PG_USER} -d {PG_DATABASE} >/dev/null 2>&1 \
                     && exit 0; sleep 1; done; echo 'postgres never accepted a TCP connection' >&2; exit 1"
            ))
            .await?;
        if ready.code != Some(0) {
            pod.remove().await;
            anyhow::bail!(
                "the postgres sidecar of {} never became reachable: {}",
                case.id,
                tail(&ready.stderr, 600)
            );
        }

        let pre = pod
            .exec(&format!(
                "cd {task_dir} && {CONTAINER_PYTHON} preprocess/main.py --agent_workspace {CONTAINER_AGENT_WORKSPACE}"
            ))
            .await?;
        if pre.code != Some(0) {
            pod.remove().await;
            anyhow::bail!(
                "the task's own preprocess/main.py failed for {} (exit {:?}): {}",
                case.id,
                pre.code,
                tail(&pre.stderr, 800)
            );
        }

        // 工具面在这里建，不在判分那一步：一端是 `task_config.json` 声明的名字，另一端是
        // 镜像里那 25 份配置，两者在这一刻都在手上。失败**不**把这份环境推翻——关工具的
        // 那一次跑照跑，要工具面的那一次到工厂里去读这条因。
        let face = match build_face(&pod, case).await {
            Ok(face) => PodToolFace::Ready(Arc::new(face)),
            Err(e) => PodToolFace::Absent(format!("{e:#}")),
        };

        Ok(Arc::new(ToolathlonPodEnv { pod, face }))
    }

    async fn evaluate(
        &self,
        env: &Arc<dyn CaseEnv>,
        case: &EvalCase,
    ) -> anyhow::Result<ToolathlonReport> {
        let pod = env
            .as_any()
            .downcast_ref::<ToolathlonPodEnv>()
            .map(|env| env.pod.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "this environment is not the pod backend's own: judging {} against it would \
                     be judging a different task",
                    case.id
                )
            })?;
        let out = pod
            .exec(&format!(
                "cd {} && {CONTAINER_PYTHON} evaluation/main.py \
                 --agent_workspace {CONTAINER_AGENT_WORKSPACE} \
                 --groundtruth_workspace {}",
                container_task_dir(case),
                container_groundtruth_dir(case)
            ))
            .await?;
        Ok(report_from(&out))
    }
}

/// 一道题的 Pod。判分在同一台里跑——换一台判就是在判另一道题。
pub(crate) struct ToolathlonPod {
    runtime: Arc<PodRuntime>,
    name: String,
}

impl ToolathlonPod {
    /// 在 `app` 容器里跑一段 shell。工作目录**显式**切到 `/workspace`，不靠镜像的
    /// `workingDir`——那样这一层的行为会随清单改一处而静默变。
    pub(crate) async fn exec(&self, script: &str) -> anyhow::Result<RunOutcome> {
        let full = format!("cd {CONTAINER_WORK_DIR} && {{\n{script}\n}}");
        self.runtime
            .exec(&[
                "exec".into(),
                self.name.clone(),
                "-c".into(),
                "app".into(),
                "--".into(),
                "/bin/sh".into(),
                "-c".into(),
                full,
            ])
            .await
    }

    /// 建一个目录（幂等）。本地工具把文件都落在这道题的 agent 工作区里，目录不预先存在。
    pub(crate) async fn mkdir(&self, dir: &str) -> anyhow::Result<()> {
        let out = self.exec(&format!("mkdir -p {}", shq(dir))).await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot create {dir} in pod {}: {}",
                self.name,
                out.stderr.trim()
            );
        }
        Ok(())
    }

    /// 把一段字节写进 Pod 的一个文件。
    ///
    /// 载荷走 base64：这些字节是**模型给的代码**，里面有引号、`$`、反引号，任何一层
    /// shell 读懂它们就会就地改写它们。
    pub(crate) async fn write_bytes(&self, path: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let script = format!(
            "mkdir -p $(dirname {path})\n{write}",
            path = shq(path),
            write = crate::eval_toolathlon_local::b64_write_script(path, bytes)
        );
        let out = self.exec(&script).await?;
        if out.code != Some(0) {
            anyhow::bail!(
                "cannot write {path} in pod {}: {}",
                self.name,
                out.stderr.trim()
            );
        }
        Ok(())
    }

    /// 读回一个文件的文本。`Ok(None)` ＝ 这个文件不存在（与「读失败」分开：前者是工具
    /// 的正常答复——「没有这件」——后者才是这一格出事了）。
    pub(crate) async fn read_text(&self, path: &str) -> anyhow::Result<Option<String>> {
        let out = self
            .exec(&format!(
                "if [ -f {p} ]; then cat {p}; else exit 44; fi",
                p = shq(path)
            ))
            .await?;
        match out.code {
            Some(0) => Ok(Some(out.stdout)),
            Some(44) => Ok(None),
            code => anyhow::bail!(
                "cannot read {path} in pod {} (exit {code:?}): {}",
                self.name,
                out.stderr.trim()
            ),
        }
    }

    /// 一台不起真 Pod 的实例，只给纯函数与 argv 的单测用。
    #[cfg(test)]
    pub(crate) fn for_test(name: &str) -> Self {
        Self::for_test_in(name, vec!["kubectl".into()], "cogneva")
    }

    /// 同上，但把 `kubectl` 前缀与命名空间**显式**给出来——对着活 Pod 跑的那几条
    /// `#[ignore]` 用例要的正是这一条：它们走的是真的 `kubectl`（宿主要 `sudo -n`），
    /// 而 `for_test` 的单测不碰集群。
    #[cfg(test)]
    pub(crate) fn for_test_in(name: &str, prefix: Vec<String>, namespace: &str) -> Self {
        Self {
            runtime: Arc::new(PodRuntime::for_test(prefix, namespace)),
            name: name.to_string(),
        }
    }

    /// 回收。重复调用是空操作——一条 case 失败也要回收。
    async fn remove(&self) {
        // `--wait=false` 之后就返回：这台机器同宿主上还跑着集群，等它真消失没有额外价值，
        // 留着被删的 Pod 反而会让下一次同名 apply 立刻成功（名字已经释放）。
        let _ = self
            .runtime
            .exec(&[
                "delete".into(),
                "pod".into(),
                self.name.clone(),
                "--wait=false".into(),
                "--ignore-not-found".into(),
            ])
            .await;
    }
}

/// 这道题的工具面**建起来了没有**。
///
/// 两件事分开：环境起来了（这道题能跑）与工具面建起来了（这道题的工具＝上游那一束）。
/// 关工具的那一次跑不需要后者，把它并进前者会让一条与工具无关的诊断因一台 server
/// 起不来而整题没跑成。所以失败留成一条**带原因的缺席**，到真要工具面的那一刻才报。
enum PodToolFace {
    Ready(Arc<TaskTools>),
    Absent(String),
}

/// 一道题的**整张**工具面：镜像里那几台 MCP server ＋ 题面声明的本地工具。
///
/// 两者共处一张面不并成一层：模型看到的是一个工具列表，名字**全局唯一**（上游那侧也是
/// 一个 `MCPToolkit` 加几个本地函数装进同一个函数表）。撞名时与 MCP 那一侧同一条规矩
/// ——先来的留下——并把撞掉的那件记下来。
struct TaskTools {
    mcp: Arc<ToolFace>,
    local: Arc<LocalTools>,
    definitions: Vec<ToolDefinition>,
}

impl TaskTools {
    fn build(mcp: ToolFace, local: LocalTools) -> Self {
        let mut definitions = mcp.definitions.clone();
        for def in local.definitions() {
            if let Some(kept) = mcp.server_of(&def.name) {
                tracing::warn!(
                    tool = %def.name,
                    kept_from = %kept,
                    dropped_from = "local tools",
                    "a local tool shares its name with an MCP tool; the MCP one is kept"
                );
                continue;
            }
            if definitions.iter().any(|d| d.name == def.name) {
                continue;
            }
            definitions.push(def);
        }
        Self {
            mcp: Arc::new(mcp),
            local: Arc::new(local),
            definitions,
        }
    }
}

#[async_trait]
impl ToolExecutor for TaskTools {
    async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
        if self.local.handles(name) {
            return self
                .local
                .call(name, &arguments)
                .await
                .map(serde_json::Value::String)
                .map_err(|e| SFError::Agent(format!("the local tool `{name}` failed: {e:#}")));
        }
        self.mcp.execute(name, arguments).await
    }
}

/// 这道题的环境：一台 Pod（库 ＋ app 两个容器）。
pub struct ToolathlonPodEnv {
    pod: Arc<ToolathlonPod>,
    face: PodToolFace,
}

#[async_trait]
impl CaseEnv for ToolathlonPodEnv {
    fn id(&self) -> &str {
        &self.pod.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// 这道题的工具面：连上的那几台 MCP server。
    ///
    /// 没连上的那种返回一个**会说话**的执行面（[`NoToolFace`]）而不是 `None`：
    /// `None` 是「这个基准本来就没有环境执行面」，而这里是「这道题的 server 没接上」，
    /// 两者的处置正相反——前者正常，后者该被看见。真正拦住工具面的地方是工厂
    /// （它拿不到 [`PodToolFace::Ready`] 就报那条因），执行面只是最后一道。
    fn executor(&self) -> Option<Arc<dyn ToolExecutor>> {
        let face: Arc<dyn ToolExecutor> = match &self.face {
            PodToolFace::Ready(face) => face.clone(),
            PodToolFace::Absent(_) => Arc::new(NoToolFace),
        };
        Some(face)
    }

    fn exec_seam(&self) -> ExecSeam {
        // 外部程序要跑的话，机器就是**这道题那台 Pod**；今天没接线，按缺席报。
        ExecSeam::Absent("the pod backend hosts no external agent program yet")
    }

    async fn teardown(&self) {
        self.pod.remove().await;
    }
}

/// 一个不提供任何工具的执行面。它存在是为了让「这一列的工具面还没实现」这件事有一个
/// **会说话的**出口，而不是一个空工具面。
struct NoToolFace;

#[async_trait]
impl ToolExecutor for NoToolFace {
    async fn execute(
        &self,
        name: &str,
        _arguments: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
        Err(SFError::Validation(format!(
            "`{name}` is not on this benchmark's tool face yet: toolathlon's tools are the MCP \
             servers a task declares, and nothing in this build turns them into tools."
        )))
    }
}

/// 按题面声明的名字，把镜像里那几台 server 起起来、各自列一次工具。
///
/// 一道题没有声明任何 server 时给一张**空**工具面，不是报错：这类题的工具面本来就只有
/// 本地的几件（`needed_local_tools`），把它读成「接不上」会让它永远跑不成。
async fn build_face(pod: &Arc<ToolathlonPod>, case: &EvalCase) -> anyhow::Result<TaskTools> {
    let declared: Vec<String> = serde_json::from_str(
        case.metadata
            .get("needed_mcp_servers")
            .map(String::as_str)
            .unwrap_or("[]"),
    )?;
    let mcp = if declared.is_empty() {
        ToolFace::build(&[])
    } else {
        let configs =
            read_server_configs(&pod.runtime, &pod.name, "app", CONTAINER_MCP_CONFIGS_DIR).await?;
        let servers = connect_declared(
            &pod.runtime,
            &pod.name,
            "app",
            &configs,
            &declared,
            &ServerPaths {
                local_servers: CONTAINER_LOCAL_SERVERS,
                agent_workspace: CONTAINER_AGENT_WORKSPACE,
                task_dir: &container_task_dir(case),
            },
        )
        .await?;
        ToolFace::build(&servers)
    };
    let local_declared: Vec<String> = serde_json::from_str(
        case.metadata
            .get("needed_local_tools")
            .map(String::as_str)
            .unwrap_or("[]"),
    )?;
    Ok(TaskTools::build(
        mcp,
        LocalTools::new(pod.clone(), &local_declared),
    ))
}

/// toolathlon 的工具面：一个 MCP **工具**一件，名字就是 server 自己报的那个。
///
/// 这个口径不是随手定的：上游 `MCPClient.get_tools()` 里 `func_name = mcp_tool.name`，
/// 不加 server 前缀；两台的 server 撞名时 `MCPToolkit.get_tools()` 丢掉后来的那件。
/// 换成「每台 server 一件、名字＝server 名」会让表里那一列**同名不同实**。
pub fn toolathlon_tools() -> Arc<ToolFactory> {
    Arc::new(|_case, env| {
        let own = env
            .as_any()
            .downcast_ref::<ToolathlonPodEnv>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "this environment is not the toolathlon pod backend's own, so its tool face \
                     would be bound to a machine that is not this task's"
                )
            })?;
        match &own.face {
            PodToolFace::Ready(face) => tools_bound_to(env, &face.definitions),

            PodToolFace::Absent(why) => anyhow::bail!(
                "this task's MCP servers did not come up, so its tool face would be smaller than \
                 the task's and a score would carry this benchmark's name without its meaning: {why}"
            ),
        }
    })
}

/// 截尾。读数的用处是让人看见现场，不是把整份日志搬进判词。
fn tail(raw: &str, n: usize) -> String {
    let s: String = raw.chars().rev().take(n).collect();
    s.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pod_name_is_dns_safe_whatever_the_task_is_called() {
        let name = pod_name("toolathlon-12306-beijing-shanghai-trip-notion-gcal-word");
        assert!(name.len() <= 63, "{name}");
        assert!(name.starts_with("ta-"));
        for c in name.chars() {
            assert!(
                c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-',
                "{name} has {c}"
            );
        }
        // 上上游哪天给一个带下划线与点的题名：仍然是合法 Pod 名。
        let odd = pod_name("toolathlon-A_b.c/d");
        for c in odd.chars() {
            assert!(
                c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-',
                "{odd} has {c}"
            );
        }
        // 同一题两次算出同一个名字（重跑要能覆盖上一台的半成品）……
        assert_eq!(pod_name("toolathlon-x"), pod_name("toolathlon-x"));
        // ……而名字不同、前缀又长到被截断的两题仍分得开。
        let a = pod_name(&format!("toolathlon-{}a", "z".repeat(60)));
        let b = pod_name(&format!("toolathlon-{}b", "z".repeat(60)));
        assert_ne!(a, b);
    }

    #[test]
    fn the_manifest_gives_the_sidecar_its_init_script_by_the_official_mechanism() {
        let m = manifest(
            "ta-x-0",
            "localhost:30500/pack:latest",
            "postgres:15",
            &PodLimits::default(),
        );
        let containers = m["spec"]["containers"].as_array().unwrap();
        assert_eq!(containers.len(), 2);
        // 库那一台：镜像、库名/用户/口令、以及与 compose 同一个挂载点。
        let pg = containers
            .iter()
            .find(|c| c["name"] == "postgres")
            .expect("a postgres container");
        assert_eq!(pg["image"], "postgres:15");
        let env: Vec<&str> = pg["env"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap())
            .collect();
        assert!(env.contains(&"POSTGRES_DB"));
        assert_eq!(
            pg["volumeMounts"][0]["mountPath"],
            serde_json::json!(PG_INITDB_DIR)
        );
        // init 脚本由 initContainer 从中转卷搬过去，不是我们自己跑导入。
        let init = &m["spec"]["initContainers"][0];
        assert_eq!(init["image"], "localhost:30500/pack:latest");
        assert!(init["command"][2]
            .as_str()
            .unwrap()
            .contains(INIT_SQL_IN_IMAGE));
        assert_eq!(m["spec"]["volumes"][0]["emptyDir"], serde_json::json!({}));
        // app 那一台：同一个镜像，PGHOST 指向共享网络命名空间里的回环。
        let app = containers.iter().find(|c| c["name"] == "app").unwrap();
        assert_eq!(app["image"], "localhost:30500/pack:latest");
        let host = app["env"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|e| (e["name"] == "PGHOST").then(|| e["value"].as_str().unwrap()));
        assert_eq!(host, Some("127.0.0.1"));
        // 资源上限从调用方那一组流下来：额度按 limits 求和，这两个数决定这台 Pod 进不进得去。
        assert_eq!(
            app["resources"]["limits"]["cpu"],
            serde_json::json!(DEFAULT_PACK_CPU_LIMIT)
        );
        assert_eq!(
            app["resources"]["limits"]["memory"],
            serde_json::json!(DEFAULT_PACK_MEMORY_LIMIT)
        );
        assert_eq!(
            pg["resources"]["limits"]["memory"],
            serde_json::json!(DEFAULT_PG_MEMORY_LIMIT)
        );
        // 这台 Pod 不跟 API server 说话。
        assert_eq!(
            m["spec"]["automountServiceAccountToken"],
            serde_json::json!(false)
        );
        // 标签落在 NetworkPolicy 收窄的那个键上，且是一个新值——所以不被任何一条选中。
        assert_eq!(
            m["metadata"]["labels"]["app.kubernetes.io/component"],
            "eval-toolathlon"
        );
    }

    #[test]
    fn the_limits_are_the_callers_not_a_literal_in_the_manifest() {
        // 覆盖位必须真的走到清单里：额度紧的时候这两个数要能被改，改不动就白搭。
        let limits = PodLimits {
            pack_cpu: "7".into(),
            pack_memory: "9Gi".into(),
            pg_cpu: "3".into(),
            pg_memory: "2Gi".into(),
        };
        let m = manifest("ta-x-0", "pack", "pg", &limits);
        let containers = m["spec"]["containers"].as_array().unwrap();
        let app = containers.iter().find(|c| c["name"] == "app").unwrap();
        let pg = containers.iter().find(|c| c["name"] == "postgres").unwrap();
        assert_eq!(app["resources"]["limits"]["cpu"], serde_json::json!("7"));
        assert_eq!(
            app["resources"]["limits"]["memory"],
            serde_json::json!("9Gi")
        );
        assert_eq!(pg["resources"]["limits"]["cpu"], serde_json::json!("3"));
        assert_eq!(
            pg["resources"]["limits"]["memory"],
            serde_json::json!("2Gi")
        );
    }

    #[test]
    fn checks_are_read_by_their_shape_not_by_the_word_pass() {
        let stdout = "=== Check 1 ===\n  [PASS] Travel_Plan.docx exists\n  \
                      [FAIL] Title 'X': got 'Y'\n\nOverall: 1/2 checks passed\nPASS\n";
        let checks = parse_checks(stdout);
        assert_eq!(checks.len(), 2);
        assert_eq!(checks[0].name, "Travel_Plan.docx exists");
        assert!(checks[0].passed);
        assert_eq!(checks[1].name, "Title 'X'");
        assert!(!checks[1].passed);
        // 散文里的 `Overall: 1/2 checks passed` 与末行的 `PASS` 都不是一条检查。
        assert!(!checks.iter().any(|c| c.name.contains("Overall")));
    }

    #[test]
    fn a_grader_that_did_not_run_to_a_verdict_keeps_its_exit_code() {
        // 没有退出码（被信号打断）时不许读成 0：0 在判词那一侧是「全过」。
        let out = RunOutcome {
            stdout: String::new(),
            stderr: "killed".into(),
            code: None,
        };
        let report = report_from(&out);
        assert_ne!(report.exit_code, 0);
        assert!(report.raw.contains("killed"));
    }

    /// 能读命名空间**不等于**能建 Pod。假 kubectl 对前者答得像模像样、对后者答 `no`，
    /// 探针必须说「不能」——它接下来要做的恰恰是建 Pod。旧探针在这里会返回真，
    /// 于是这一列不进缺席声明，480 例逐格撞 403，报表把它们记成「跑错了」。
    #[cfg(unix)]
    #[tokio::test]
    async fn a_namespace_read_does_not_stand_in_for_permission_to_create_pods() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let write = |name: &str, answer: &str| {
            let path = dir.path().join(name);
            std::fs::write(
                &path,
                format!(
                    "#!/bin/sh\n\
                     case \"$*\" in\n\
                     *\"auth can-i create pods\"*) echo {answer}; exit 0;;\n\
                     *\"get namespace\"*) echo namespace/probe; exit 0;;\n\
                     esac\n\
                     exit 1\n"
                ),
            )
            .expect("write fake kubectl");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake kubectl");
            vec![path.to_string_lossy().into_owned()]
        };

        let refused = write("kubectl-no", "no");
        assert!(
            !probe(&refused, "cogneva").await,
            "读得到命名空间不等于建得出 Pod"
        );

        // 对照：同一个假二进制答 `yes` 时探针必须过，否则上面那句只是「探针永远说不」。
        let granted = write("kubectl-yes", "yes");
        assert!(probe(&granted, "cogneva").await);
    }
}
