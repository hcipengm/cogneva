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
use cog_core::{SFError, SFResult, ToolExecutor};
use cog_eval::{CaseEnv, EvalCase, ExecSeam, ToolathlonBackend, ToolathlonCheck, ToolathlonReport};

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
const CONTAINER_AGENT_WORKSPACE: &str = "/workspace/cogneva-eval/agent_workspace";
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
/// 运行时的开关：设成 `off` 就是这一次不接。
const ENV_RUNTIME: &str = "COGNEVA_EVAL_TOOLATHLON_RUNTIME";
const DEFAULT_NAMESPACE: &str = "cogneva";

/// 单次 Pod 命令的时间上界（秒）。默认比 swe-bench-pro 那一侧宽：一道题要起库、导入
/// `init.sql.gz`、跑前处理、再跑判分，其中导入那一步本身就按分钟计。这是一个**安全上界**
/// 不是量出来的界，所以它可覆盖；它的意义是让一条卡住的命令变成一次带原因的失败。
const DEFAULT_TIMEOUT_SECS: u64 = 1800;

/// 等库就绪的上界（秒）。超过就报「库没起来」，不是继续跑下去把每一格都判成错。
const PG_READY_DEADLINE_SECS: u64 = 300;

/// 一次 `kubectl` 调用的结局。
#[derive(Debug, Clone)]
struct RunOutcome {
    stdout: String,
    stderr: String,
    /// `None` ＝ 被信号打断，没拿到退出码。
    code: Option<i32>,
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

    /// 跑一条 `kubectl` 命令。超时是**失败**不是空结果：吞掉它，这一格的分就读成
    /// 「模型没做对」，而真相是「这台机器没跑完」。
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

/// 这个前缀能不能真跟集群说上话。
///
/// 判据是「读得出一个像名字的东西」，不是「二进制在不在」——`kubectl version --client`
/// 在没有 kubeconfig 的机器上一样成功，那台机器却起不了任何 Pod。所以这里只认一次
/// `get namespace <n> -o name` 能退 0 且印出 `namespace/<n>`：字段名写错时 `--format`
/// 不报错、退出码还是 0（这条形状在本仓已经栽过一次），所以**判输出形状不判退出码**。
async fn probe(prefix: &[String], namespace: &str) -> bool {
    let mut cmd = tokio::process::Command::new(&prefix[0]);
    cmd.args(&prefix[1..])
        .args(["-n", namespace, "get", "namespace", namespace, "-o", "name"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let Ok(child) = cmd.kill_on_drop(true).spawn() else {
        return false;
    };
    match tokio::time::timeout(Duration::from_secs(20), child.wait_with_output()).await {
        Ok(Ok(out)) => {
            out.status.success()
                && String::from_utf8_lossy(&out.stdout)
                    .trim()
                    .starts_with("namespace/")
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

/// 一份 Pod 清单。纯函数、吃两个镜像名和一个 Pod 名，所以它的形状能被单测直接钉住。
///
/// 写 JSON 不写 YAML：`kubectl` 两条都收，而 JSON 是 serde_json 直接吐出来的，
/// 少引一个 YAML 依赖、也少一处手写缩进会错的地方。
fn manifest(pod: &str, pack_image: &str, pg_image: &str) -> serde_json::Value {
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
                        "limits": { "cpu": "2", "memory": "1Gi" }
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
                        { "name": "LOCAL_SERVERS_PATH", "value": "/opt/local_servers" },
                        { "name": "PYTHON_BIN", "value": CONTAINER_PYTHON }
                    ],
                    "resources": {
                        "requests": { "cpu": "200m", "memory": "512Mi" },
                        "limits": { "cpu": "4", "memory": "6Gi" }
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
        Ok(Some(Self {
            runtime: Arc::new(runtime),
            pack_image: image_or(ENV_PACK_IMAGE, DEFAULT_PACK_IMAGE),
            pg_image: image_or(ENV_PG_IMAGE, DEFAULT_PG_IMAGE),
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
            manifest(&name, &self.pack_image, &self.pg_image).to_string(),
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

        Ok(Arc::new(ToolathlonPodEnv { pod }))
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
struct ToolathlonPod {
    runtime: Arc<PodRuntime>,
    name: String,
}

impl ToolathlonPod {
    /// 在 `app` 容器里跑一段 shell。工作目录**显式**切到 `/workspace`，不靠镜像的
    /// `workingDir`——那样这一层的行为会随清单改一处而静默变。
    async fn exec(&self, script: &str) -> anyhow::Result<RunOutcome> {
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

/// 这道题的环境：一台 Pod（库 ＋ app 两个容器）。
pub struct ToolathlonPodEnv {
    pod: Arc<ToolathlonPod>,
}

#[async_trait]
impl CaseEnv for ToolathlonPodEnv {
    fn id(&self) -> &str {
        &self.pod.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// 这道题的工具面**今天还没有**。
    ///
    /// 返回一个「跑得了 shell」的执行面是**错的**：这一列的工具面是 `task_config.json`
    /// 点名的那几台 MCP server，今天没有任何东西把它们变成 `cog_core::Tool`。给一个能用
    /// 的执行面，读的人会以为「环境是好的、只是模型不会用工具」；照实报错才分得清
    /// 「这一列的工具面还没实现」与「模型做错了」——这两件事的处置正相反。
    fn executor(&self) -> Option<Arc<dyn ToolExecutor>> {
        Some(Arc::new(NoToolFace))
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
        let m = manifest("ta-x-0", "localhost:30500/pack:latest", "postgres:15");
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
}
