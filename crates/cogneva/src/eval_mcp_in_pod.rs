//! 跨进 Pod 去跟一个 **stdio** 程序谈话：`kubectl exec -i` 就是那条管道。
//!
//! 这一层解决的是拓扑问题，不是协议问题。toolathlon 每一道题点名的 MCP server 都是
//! **agent 进程的子进程**（25 份配置无例外 `type: stdio`），而它们的 stdio 面只能被同
//! 进程树里的一方驱动；外壳却在 Pod 外面。于是要的是「进去之后**待着**」，而
//! `kubectl exec -i` 给的正好是这条：本地写的是远端程序的 stdin，读的是它的 stdout，
//! 一次 exec 撑得起整场对话，远端退出码也传得回来（三条都实测过）。
//!
//! 因此 Pod 里**一个自写程序都不需要**：被 exec 的就是上游那台 server 本身，本地这侧
//! 要写的只是它那个协议的客户端——而那个客户端仓库里已经有了（`cog_protocol::McpClient`
//! ＋ `StdioTransport`）。本模块只是把「起哪个进程」这一件事接出来。
//!
//! 两个不该混在一起的东西在这里分开：
//!
//! - **环境（env）与工作目录（cwd）不能经本地 kubectl 进程传**。`exec` 的 argv 是发给
//!   远端进程的，而 kubectl 没有「顺带设远端环境」这一项，本地的环境也不会过界。
//!   所以它们只能写进远端那条命令串里，由 Pod 里的 `/bin/sh` 展开。
//! - **不是 stdio 的那种 server 要说清楚是哪种**。配置里 `type` 是别的值时，报的是
//!   「这台不是 stdio」而不是「起不来」——两者都会让这一格没有读数，但修法相反。

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use cog_core::ToolDefinition;
use cog_protocol::mcp::{McpTool, McpToolCall, McpToolResult};
use cog_protocol::{McpClient, StdioTransport};

use crate::eval_toolathlon::PodRuntime;

/// 配置没写 `client_session_timeout_seconds` 时用的那个数。
///
/// 60 秒取自上游（`cfg.get("client_session_timeout_seconds", 60)`），不由调用点另定：
/// 两个地方各定一个默认值，同一条配置就有两个超时，而慢的那一台会被读成坏的那一台。
// cap: bound
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(60);

/// 配置里那三个占位符。上游 runner 做的替换逐字就是这三个（`utils/mcp/tool_servers.py`
/// 的 `_resolve`），所以这里是照抄它，不是自己定点什么。
///
/// 只认这三个：别的 `${...}` 一律**原样留下**——上游也只替换这三个，注释掉的那些
/// `${config.proxy}` 若被这里多替换一次，两份配置的读数就分家了。
const PLACEHOLDER_LOCAL_SERVERS: &str = "${local_servers_paths}";
const PLACEHOLDER_AGENT_WORKSPACE: &str = "${agent_workspace}";
const PLACEHOLDER_TASK_DIR: &str = "${task_dir}";

/// 一道题里某一台 stdio MCP server 的起动参数。
///
/// 字段与上游那份 YAML 的 `params:` 一一对应，名字不改写：读配置的人对照着看时不该
/// 还要在脑子里做一次映射。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdioServerSpec {
    /// 配置自己声明的名字。题面用的就是它（不是文件名，两者实测有五处错位）。
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    /// 有序，因为要拼进命令串；同名重复的键在解析时就拒。
    pub env: Vec<(String, String)>,
    /// `None` ＝ 这份配置没钉工作目录（有的配置专门注释掉它，靠拉起方的 cwd）。
    pub cwd: Option<String>,
    /// 这份配置自己声明的会话超时。上游按台配（实测 20～100 秒），所以不能在这里
    /// 定一个全局值——那会把「这台答得慢」读成「这台坏了」。
    pub request_timeout: Option<Duration>,
}

impl StdioServerSpec {
    /// 从一份 `configs/mcp_servers/*.yaml` 的正文解析。
    ///
    /// 走 `serde_yaml::Value` 而不是 derive：这些配置里还有 `cache_tools_list` 这类本层
    /// 不关心的键，用结构体接会要求把上游的每一个键都登记一遍，上游加一个键这边就
    /// 解析失败——那是把「上游改了配置」读成「我们的代码坏了」。
    pub fn from_yaml(text: &str) -> anyhow::Result<Self> {
        let doc: serde_yaml::Value = serde_yaml::from_str(text)
            .map_err(|e| anyhow::anyhow!("the server config is not readable as YAML: {e}"))?;
        let map = doc
            .as_mapping()
            .ok_or_else(|| anyhow::anyhow!("the server config is not a mapping"))?;

        let declared = map.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
            anyhow::anyhow!(
                "the server config declares no `name`; the task refers to servers by it"
            )
        })?;

        // `type` 先判：不是 stdio 的那种要报成「这台是 X」而不是「起不来」。
        match map.get("type").and_then(|v| v.as_str()) {
            Some("stdio") => {}
            Some(other) => anyhow::bail!(
                "server `{declared}` is declared as `{other}`, which this route cannot drive; \
                 only stdio servers are started inside the pod"
            ),
            None => anyhow::bail!(
                "server `{declared}` declares no `type`, so how to reach it is unknown"
            ),
        }

        let params = map
            .get("params")
            .and_then(|v| v.as_mapping())
            .ok_or_else(|| anyhow::anyhow!("server `{declared}` declares no `params`"))?;

        let command = params
            .get("command")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .ok_or_else(|| anyhow::anyhow!("server `{declared}` declares no `command`"))?
            .to_string();

        let args = match params.get("args") {
            None => Vec::new(),
            Some(serde_yaml::Value::Sequence(items)) => items
                .iter()
                .map(|item| {
                    item.as_str().map(str::to_string).ok_or_else(|| {
                        anyhow::anyhow!(
                            "server `{declared}`: an argument is not a scalar, so it cannot be \
                             handed to a shell as one word"
                        )
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            Some(_) => anyhow::bail!("server `{declared}`: `args` is not a list"),
        };

        let env = match params.get("env") {
            None => Vec::new(),
            Some(serde_yaml::Value::Mapping(pairs)) => {
                let mut out: Vec<(String, String)> = Vec::with_capacity(pairs.len());
                for (key, value) in pairs {
                    let key = key.as_str().ok_or_else(|| {
                        anyhow::anyhow!("server `{declared}`: an env key is not a scalar")
                    })?;
                    // 名字不合法在这里就拒。拼进命令串之后它要么被 shell 换个意思解释，
                    // 要么整台起不来——那时报出来的错与这一个键毫无关系。
                    if !is_env_name(key) {
                        anyhow::bail!(
                            "server `{declared}`: `{key}` is not a usable environment variable \
                             name"
                        );
                    }
                    let value = value.as_str().ok_or_else(|| {
                        anyhow::anyhow!(
                            "server `{declared}`: the value of `{key}` is not a scalar, so it \
                             cannot be exported as one"
                        )
                    })?;
                    out.push((key.to_string(), value.to_string()));
                }
                out
            }
            Some(_) => anyhow::bail!("server `{declared}`: `env` is not a mapping"),
        };

        let cwd = match params.get("cwd") {
            None | Some(serde_yaml::Value::Null) => None,
            Some(v) => Some(
                v.as_str()
                    .ok_or_else(|| anyhow::anyhow!("server `{declared}`: `cwd` is not a scalar"))?
                    .to_string(),
            ),
        };

        let request_timeout = match map.get("client_session_timeout_seconds") {
            None | Some(serde_yaml::Value::Null) => None,
            Some(v) => {
                let secs = v.as_u64().ok_or_else(|| {
                    anyhow::anyhow!(
                        "server `{declared}`: `client_session_timeout_seconds` is not a number"
                    )
                })?;
                Some(Duration::from_secs(secs))
            }
        };

        Ok(Self {
            name: declared.to_string(),
            command,
            args,
            env,
            cwd,
            request_timeout,
        })
    }

    /// 把配置里的路径占位符换成这台机器上的实际路径。
    ///
    /// 三个占位符**逐字**照上游的替换做，包括「`task_dir` 为空时不替换成空串而是留下
    /// 占位符」这一点也说清楚：上游 `_resolve` 拿到的就是空串，照样替换，所以这里也
    /// 替换。两边对同一条配置得出不同的命令串，是同一条题在两个口径下跑。
    pub fn resolve_paths(
        &self,
        local_servers_path: &str,
        agent_workspace: &str,
        task_dir: &str,
    ) -> Self {
        let substitute = |s: &str| -> String {
            s.replace(PLACEHOLDER_LOCAL_SERVERS, local_servers_path)
                .replace(PLACEHOLDER_AGENT_WORKSPACE, agent_workspace)
                .replace(PLACEHOLDER_TASK_DIR, task_dir)
        };
        Self {
            name: self.name.clone(),
            command: substitute(&self.command),
            args: self.args.iter().map(|a| substitute(a)).collect(),
            env: self
                .env
                .iter()
                .map(|(k, v)| (k.clone(), substitute(v)))
                .collect(),
            // 没钉 cwd 的配置**不是**「不 cd」：上游把缺省值取成 agent_workspace 并
            // `os.makedirs(..., exist_ok=True)`。少了这一条，那些 server 会从镜像的
            // 工作目录（`/workspace`）起，读写的相对路径就换了一个根。
            cwd: Some(substitute(self.cwd.as_deref().unwrap_or(agent_workspace))),
            request_timeout: self.request_timeout,
        }
    }

    /// `/bin/sh` 在 Pod 里要执行的那条命令串。
    ///
    /// 三件事必须与上游一致，否则同一条配置在两个口径下跑出两棵树：
    ///
    /// 1. **cwd 缺省是 agent_workspace**，且缺目录要建出来（上游 `os.makedirs`）。
    /// 2. **YAML 里的 `env` 是缺省值，不是覆盖值**。上游的合并是 `{**env, **os.environ}`——
    ///    进程环境赢，正是为了让运行期注入的 `PGHOST=…` 不被配置里的 `PG_HOST: postgres`
    ///    压住。写成 `exec env K=V`（照抄配置）会把方向反过来，而这四个按库取数的 server
    ///    就连到另一只库上去。
    /// 3. **libpq 名到 `PG_*` 名的桥接在合并之后**（上游 `_pg_bridge`）：容器里有 `PGHOST`
    ///    就让它盖掉 `PG_HOST`，无论后者来自配置还是来自进程环境。
    pub fn remote_script(&self) -> String {
        let mut script = String::new();
        if let Some(cwd) = &self.cwd {
            let quoted = sh_quote(cwd);
            // 建不出来就当场停：继续下去 server 会在另一个目录里起，而那不是一个错误，
            // 是一份看起来正常的错数据。
            script.push_str(&format!("mkdir -p {quoted} || exit 1\n"));
            script.push_str(&format!("cd {quoted} || exit 1\n"));
        }
        for (key, value) in &self.env {
            // `${K+x}` 只在**未设**时展开为空——与「进程环境里有没有这个键」同义，
            // 而 `${K:-…}` 会把空串也算作没有，那是另一件事。
            script.push_str(&format!(
                "[ -n \"${{{key}+x}}\" ] || export {key}={}\n",
                sh_quote(value)
            ));
        }
        for (libpq, pg) in LIBPQ_TO_PG {
            script.push_str(&format!(
                "[ -n \"${{{libpq}+x}}\" ] && export {pg}=\"${{{libpq}}}\"\n"
            ));
        }
        script.push_str("exec ");
        script.push_str(&sh_quote(&self.command));
        for arg in &self.args {
            script.push(' ');
            script.push_str(&sh_quote(arg));
        }
        script
    }
}

/// 标准 libpq 变量名 → MCP server 那一侧用的 `PG_*` 名。
///
/// 上游 `utils/mcp/tool_servers.py` 里逐字是这五对，桥接发生在环境合并**之后**。
const LIBPQ_TO_PG: [(&str, &str); 5] = [
    ("PGHOST", "PG_HOST"),
    ("PGPORT", "PG_PORT"),
    ("PGDATABASE", "PG_DATABASE"),
    ("PGUSER", "PG_USER"),
    ("PGPASSWORD", "PG_PASSWORD"),
];

fn is_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// 把一段字面量包成 shell 里的**一个词**。
///
/// 一律用单引号包住、只把里面的单引号拆成 `'\''`：双引号里 `$`、反引号、反斜杠都还有
/// 意思，而这些值里有 JSON（`OPENAPI_MCP_HEADERS` 就是一段带引号的 JSON），过一遍双引号
/// 会被就地改写。单引号里除了单引号自己，一切都按字面走。
pub(crate) fn sh_quote(word: &str) -> String {
    let mut out = String::with_capacity(word.len() + 2);
    out.push('\'');
    for ch in word.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// 接上一台 Pod 里的 stdio server，拿一个能对它 `list_tools` / `call_tool` 的客户端。
///
/// 传出去的不是一个起好的进程，而是一个**惰性**客户端：`StdioTransport` 到第一次请求
/// 才起 `kubectl`、才握手，之后复用同一条会话。所以这个函数不会在这里把 Pod 连爆——
/// 一台 server 起没起得来，要等真问它一句才知道。
pub fn client_in_pod(
    runtime: &PodRuntime,
    pod: &str,
    container: &str,
    spec: &StdioServerSpec,
) -> anyhow::Result<McpClient> {
    let (program, args) = runtime.stdio_exec_argv(pod, container, &spec.remote_script());
    let timeout = spec.request_timeout.unwrap_or(DEFAULT_SESSION_TIMEOUT);
    let transport = StdioTransport::new(program, args, timeout).map_err(|e| {
        anyhow::anyhow!("cannot reach mcp server `{}` in pod {pod}: {e}", spec.name)
    })?;
    Ok(McpClient::new(Box::new(transport)))
}

/// 一台已经握上手、并且把它自己的工具列过一次的 server。
pub struct ConnectedServer {
    /// 题面声明的那个名字（＝ 配置里的 `name:`）。
    pub name: String,
    pub client: Arc<McpClient>,
    pub tools: Vec<McpTool>,
}

/// 一个目录里读出来的 server 配置。
#[derive(Debug, Default)]
pub struct ServerConfigs {
    /// 按**配置文件路径**排序。上游起 client 的顺序就是
    /// `for config_file in sorted(config_path.glob("*.yaml"))`，这个顺序不是随手排的：
    /// 两台 server 撞同一个工具名时，先起的那台赢（见 [`build_tool_face`]）。
    pub servers: Vec<(String, StdioServerSpec)>,
    /// 读不懂、或读得懂但这台起不来的那几份：路径、它自称的名字（读不出来时没有）、
    /// 原因。声明里点到它们时，这一条就是那个因——不能把「这份配置解析不了」报成
    /// 「这台 server 不在配置里」。
    pub unreadable: Vec<(String, Option<String>, String)>,
}

impl ServerConfigs {
    /// 按声明名找一份配置。名字取 YAML 里的 `name:`，不是文件名（两者实测有五处错位）。
    pub fn get(&self, name: &str) -> Option<&StdioServerSpec> {
        self.servers
            .iter()
            .find(|(_, spec)| spec.name == name)
            .map(|(_, spec)| spec)
    }

    /// 找不着时那句错要能分辨「没有这份配置」与「有但起不来」。
    ///
    /// 自称是这个名字的那一份排在最前：读的人要的是「gamma 那份说的是 `sse`」，
    /// 不是一份按路径排序的失败清单。
    fn why_missing(&self, name: &str) -> String {
        if self.unreadable.is_empty() {
            return format!("no server config in the pack declares `{name}`");
        }
        let mut why: Vec<&(String, Option<String>, String)> = self.unreadable.iter().collect();
        why.sort_by_key(|(_, claimed, _)| claimed.as_deref() != Some(name));
        let lines: Vec<String> = why
            .into_iter()
            .map(|(path, claimed, err)| match claimed {
                Some(claimed) => format!("{path} (declares `{claimed}`): {err}"),
                None => format!("{path}: {err}"),
            })
            .collect();
        format!(
            "no server config in the pack starts a server named `{name}`; {} config(s) could not \
             be read or driven: {}",
            self.unreadable.len(),
            lines.join(" | ")
        )
    }
}

/// 把「路径 / 正文」成对的记录解析成一份 [`ServerConfigs`]。
///
/// 读不懂的那一份留成一条**有路径的**因，不让整批失败：一台上游写坏了的配置不该让
/// 这道题所有别的 server 都起不来，而声明点到它时要说得清是哪一份。
pub fn parse_config_records(records: &[(String, String)]) -> ServerConfigs {
    let mut out = ServerConfigs::default();
    for (path, text) in records {
        match StdioServerSpec::from_yaml(text) {
            Ok(spec) => out.servers.push((path.clone(), spec)),
            Err(e) => {
                // 读不懂正文时仍试着把它自称的名字取出来：一份 `type: sse` 的配置是
                // **这台 server 起不来**，与「没有这台」是两条不同的处置。
                let claimed = serde_yaml::from_str::<serde_yaml::Value>(text)
                    .ok()
                    .and_then(|v| {
                        v.as_mapping()
                            .and_then(|m| m.get(serde_yaml::Value::String("name".into())))
                            .and_then(|n| n.as_str().map(str::to_string))
                    });
                out.unreadable.push((path.clone(), claimed, e.to_string()));
            }
        }
    }
    // 路径排序：`read_server_configs` 交回来的已经是排好的，但这一条判据要长在**纯函数**
    // 上——顺序是语义的一部分（撞名时谁赢），不能只靠调用方记得。
    out.servers.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// 把 Pod 里那个配置目录整个读回来。
///
/// 一次 exec 读完整个目录，用 NUL 分隔「路径 / 正文」：NUL 不可能出现在 YAML 正文里，
/// 而任何可打印的分隔行都可能——配置里带着 JSON（`OPENAPI_MCP_HEADERS` 就是），
/// 拿一行 `=== xxx` 去切会在某一天切错，且切错之后解析失败会被读成「这份配置坏了」。
pub async fn read_server_configs(
    runtime: &PodRuntime,
    pod: &str,
    container: &str,
    dir: &str,
) -> anyhow::Result<ServerConfigs> {
    let quoted = sh_quote(dir);
    let script = format!(
        "[ -d {quoted} ] || exit 3\n\
         for f in {quoted}/*.yaml; do [ -f \"$f\" ] || continue; \
           printf '%s\\0' \"$f\"; cat \"$f\"; printf '\\0'; done"
    );
    let out = runtime
        .exec(&[
            "exec".into(),
            pod.into(),
            "-c".into(),
            container.into(),
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            script,
        ])
        .await?;
    if out.code == Some(3) {
        anyhow::bail!("the pack image has no server config directory at {dir}");
    }
    if out.code != Some(0) {
        anyhow::bail!(
            "cannot read the server configs from {dir} in pod {pod} (exit {:?}): {}",
            out.code,
            out.stderr.trim()
        );
    }

    let mut fields = out.stdout.split('\0');
    let mut records = Vec::new();
    while let Some(path) = fields.next() {
        if path.is_empty() {
            continue;
        }
        let Some(text) = fields.next() else {
            anyhow::bail!(
                "the config listing from {dir} ended after the path `{path}` with no body: \
                 the read was truncated, so what came back is not the set of configs"
            );
        };
        records.push((path.to_string(), text.to_string()));
    }
    Ok(parse_config_records(&records))
}

/// 配置里那三个占位符在**这道题的 Pod 里**各自指哪儿。
///
/// 上游是从进程环境与参数里取的（`LOCAL_SERVERS_PATH`、`agent_workspace`、`task_dir`），
/// 所以它们不是常量：换一台机器、换一道题，值就变。放在这里显式传，是因为不替换的配置
/// 会把 `"${local_servers_paths}/filesystem/dist/index.js"` 原样交给 node——那是一条只有
/// 到 server 起不来时才暴露的失败。
#[derive(Debug, Clone, Copy)]
pub struct ServerPaths<'a> {
    pub local_servers: &'a str,
    pub agent_workspace: &'a str,
    pub task_dir: &'a str,
}

/// 把这道题声明的 server 全部连上，并各自列一次工具。
///
/// 顺序按配置路径（上游的形状），不是按声明顺序：撞名时谁的工具进工具面由它决定。
/// 没声明的不连——一台没人用的 server 起来就是白占 Pod 的内存与启动时间。
pub async fn connect_declared(
    runtime: &PodRuntime,
    pod: &str,
    container: &str,
    configs: &ServerConfigs,
    declared: &[String],
    paths: &ServerPaths<'_>,
) -> anyhow::Result<Vec<ConnectedServer>> {
    let wanted: BTreeSet<&str> = declared.iter().map(String::as_str).collect();
    let mut out = Vec::new();
    let mut started: BTreeSet<String> = BTreeSet::new();
    for (path, raw) in &configs.servers {
        let spec = &raw.resolve_paths(paths.local_servers, paths.agent_workspace, paths.task_dir);
        if !wanted.contains(spec.name.as_str()) || !started.insert(spec.name.clone()) {
            continue;
        }
        let client = Arc::new(client_in_pod(runtime, pod, container, spec)?);
        let tools = client.list_tools().await.map_err(|e| {
            anyhow::anyhow!(
                "server `{}` (from {path}) did not answer tools/list: {e}",
                spec.name
            )
        })?;
        out.push(ConnectedServer {
            name: spec.name.clone(),
            client,
            tools,
        });
    }
    let missing: Vec<&str> = declared
        .iter()
        .map(String::as_str)
        .filter(|name| !started.contains(*name))
        .collect();
    if !missing.is_empty() {
        let mut why = Vec::new();
        for name in &missing {
            why.push(format!("`{name}`: {}", configs.why_missing(name)));
        }
        anyhow::bail!(
            "this task declares MCP servers that the pack cannot start, so its tool face would be \
             smaller than the task's: {}",
            why.join(" | ")
        );
    }
    Ok(out)
}

/// 工具面：给外壳看的规格，加上「这个名字打到哪台 server」。
pub struct ToolFace {
    pub definitions: Vec<ToolDefinition>,
    /// 工具名 → (声明它的 server, 那台的客户端)。名字是**MCP 工具自己的名字**，不带
    /// server 前缀——上游给模型的正是这个名字，工具面换个拼法就换了动作空间。
    routes: HashMap<String, (String, Arc<McpClient>)>,
}

impl ToolFace {
    /// 把连好的那几台 server 拼成一张工具面。
    ///
    /// 撞名按上游的做法处理：**先起的那台赢，后来的那件丢掉**（上游 `MCPToolkit.get_tools`
    /// 里逐字是 `seen_names` ＋ 一条 warning）。这不是可以随手改成报错的地方——上游撞名
    /// 时照样跑完，报错会让某些题从「有分」变成「没跑成」，而这道题的分数并不因为撞名
    /// 而失去意义，只是少了一件工具。丢掉的那几件在这里留一条告警，别静默。
    pub fn build(servers: &[ConnectedServer]) -> Self {
        let mut definitions = Vec::new();
        let mut routes = HashMap::new();
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for server in servers {
            for tool in &server.tools {
                if let Some(first) = seen.get(tool.name.as_str()) {
                    tracing::warn!(
                        tool = %tool.name,
                        kept_from = %first,
                        dropped_from = %server.name,
                        "two mcp servers expose a tool with the same name; keeping the first, \
                         as the upstream toolkit does"
                    );
                    continue;
                }
                seen.insert(tool.name.as_str(), server.name.as_str());
                definitions.push(ToolDefinition {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    // 逐字用 server 自己声明的 schema：它才是这台 server 认的那份契约。
                    // 上游把它重写过一遍（strict OpenAI 那一套），那是给 OpenAI 的；
                    // 这里换个拼法就等于替 server 宣布了一个它没同意的参数面。
                    parameters: tool.input_schema.clone(),
                });
                routes.insert(
                    tool.name.clone(),
                    (server.name.clone(), server.client.clone()),
                );
            }
        }
        Self {
            definitions,
            routes,
        }
    }

    /// 工具名 → 声明它的 server。归因用（哪一台没答上来）。
    pub fn server_of(&self, tool: &str) -> Option<&str> {
        self.routes.get(tool).map(|(server, _)| server.as_str())
    }
}

#[async_trait::async_trait]
impl cog_core::ToolExecutor for ToolFace {
    async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> cog_core::SFResult<serde_json::Value> {
        let Some((server, client)) = self.routes.get(name) else {
            return Err(cog_core::SFError::Validation(format!(
                "`{name}` is not on this task's tool face (it exposes {} tools across its MCP \
                 servers); a call to a tool nobody bound would be run against the wrong thing",
                self.definitions.len()
            )));
        };
        // MCP 的 `arguments` 是一个对象；外壳那一侧给的若不是对象，就是这一层与外壳之间
        // 的口径不一致，报出来比悄悄当成空参数好——空参数会被 server 当成「没给」，
        // 然后走它自己的默认值，读起来像模型少给了一个参数。
        let arguments = arguments.as_object().cloned().ok_or_else(|| {
            cog_core::SFError::Validation(format!(
                "`{name}` takes an argument object; got {}",
                json_kind(&arguments)
            ))
        })?;
        let result = client
            .call_tool(McpToolCall {
                name: name.to_string(),
                arguments: arguments.into_iter().collect(),
            })
            .await
            .map_err(|e| {
                cog_core::SFError::Agent(format!("mcp tool `{name}` on server `{server}`: {e}"))
            })?;
        Ok(render_result(&result))
    }
}

/// 一次 `tools/call` 的答复 → 外壳看到的那个值。
///
/// 全是文本时给一段连起来的文本（模型与上游看到的就是这个），带图片或别的时给原始数组：
/// 把一张图压成一句「[image]」会让外壳以为 server 什么都没给。
fn render_result(result: &McpToolResult) -> serde_json::Value {
    let all_text = result
        .content
        .iter()
        .all(|c| matches!(c, cog_protocol::mcp::McpContent::Text { .. }));
    if all_text {
        let joined: Vec<&str> = result
            .content
            .iter()
            .filter_map(|c| match c {
                cog_protocol::mcp::McpContent::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        return serde_json::Value::String(joined.join("\n"));
    }
    serde_json::to_value(&result.content).unwrap_or(serde_json::Value::Null)
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval_toolathlon::PodRuntime;
    use cog_core::ToolExecutor;

    const NOTION: &str = r#"
type: stdio
name: notion
params:
  command: node
  args:
    - "${local_servers_paths}/notion-mcp-server/bin/cli.mjs"
    - "--page-id"
    - ""
  env:
    OPENAPI_MCP_HEADERS: "{\"Authorization\": \"Bearer ntn-placeholder\"}"
  cwd: "${agent_workspace}"
client_session_timeout_seconds: 100
cache_tools_list: true
"#;

    #[test]
    fn a_config_declares_the_name_the_task_uses_not_the_file_name() {
        let spec = StdioServerSpec::from_yaml(NOTION).unwrap();
        assert_eq!(spec.name, "notion");
        assert_eq!(spec.command, "node");
        assert_eq!(spec.request_timeout, Some(Duration::from_secs(100)));
        // 这份配置关心的键之外的键（`cache_tools_list`）不该让解析失败。
        let one_two_three_six = StdioServerSpec::from_yaml(
            "type: stdio\nname: rail_12306\nparams:\n  command: node\n  args: [a.js]\n",
        )
        .unwrap();
        assert_eq!(one_two_three_six.name, "rail_12306");
        // 没有 `cwd` 的配置是「没钉工作目录」，不是「工作目录为空」。
        assert_eq!(one_two_three_six.cwd, None);
        assert_eq!(one_two_three_six.request_timeout, None);
    }

    #[test]
    fn a_server_this_route_cannot_drive_is_named_not_written_off_as_a_failure() {
        // 不是 stdio ⇒ 报的是它是什么。写成「起不来」会让人去查网络。
        let err =
            StdioServerSpec::from_yaml("type: streamable-http\nname: x\nparams:\n  command: c\n")
                .unwrap_err()
                .to_string();
        assert!(err.contains("streamable-http"), "{err}");
        // 没写 `type` ⇒ 是「怎么进它不知道」，不是「它是别人那种」。
        let err = StdioServerSpec::from_yaml("name: x\nparams:\n  command: c\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no `type`"), "{err}");
        // 没有 `name` ⇒ 题面按名字点名，这个名字缺了就没法对上。
        let err = StdioServerSpec::from_yaml("type: stdio\nparams:\n  command: c\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("name"), "{err}");
    }

    #[test]
    fn every_key_that_rides_into_the_shell_is_checked_before_it_gets_there() {
        // 名字不合法：报它自己，不要等到 shell 换个意思解释。
        let err = StdioServerSpec::from_yaml(
            "type: stdio\nname: x\nparams:\n  command: c\n  env:\n    \"A B\": v\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("A B"), "{err}");
        // 值不是标量：导不出去。
        let err = StdioServerSpec::from_yaml(
            "type: stdio\nname: x\nparams:\n  command: c\n  env:\n    K:\n      a: b\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("K"), "{err}");
        // 参数不是标量：它得是一个词。
        let err = StdioServerSpec::from_yaml(
            "type: stdio\nname: x\nparams:\n  command: c\n  args:\n    - {a: b}\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("argument"), "{err}");
    }

    #[test]
    fn the_three_placeholders_are_the_upstream_ones_and_nothing_else() {
        let spec = StdioServerSpec::from_yaml(NOTION).unwrap().resolve_paths(
            "/opt/local_servers",
            "/workspace/agent_workspace",
            "",
        );
        assert_eq!(
            spec.args[0],
            "/opt/local_servers/notion-mcp-server/bin/cli.mjs"
        );
        assert_eq!(spec.cwd.as_deref(), Some("/workspace/agent_workspace"));
        // 别的 `${...}` 原样留下：上游也只换这三个，换多了两边读的就成了两条配置。
        let untouched = StdioServerSpec::from_yaml(
            "type: stdio\nname: x\nparams:\n  command: c\n  args: [\"${config.proxy}\"]\n",
        )
        .unwrap()
        .resolve_paths("/a", "/b", "/c");
        assert_eq!(untouched.args[0], "${config.proxy}");
    }

    #[test]
    fn the_remote_command_survives_values_that_shells_like_to_reinterpret() {
        // 带引号的 JSON：过一遍双引号就会被就地改写，所以判据取在「取回来逐字相等」上。
        let json =
            r#"{"Authorization": "Bearer ntn-placeholder", "Notion-Version": "2022-06-28" }"#;
        assert_eq!(
            sh_quote(json),
            r#"'{"Authorization": "Bearer ntn-placeholder", "Notion-Version": "2022-06-28" }'"#
        );
        // 单引号要能活下来：它既不能被吞掉，也不能提前把引号合上。
        assert_eq!(sh_quote("it's"), r#"'it'\''s'"#);
        // `$`、反引号、反斜杠在单引号里都是字面量——这三个是双引号会改写的那一批。
        assert_eq!(sh_quote("$HOME `id` \\n"), r#"'$HOME `id` \n'"#);
    }

    #[test]
    fn the_command_that_runs_in_the_pod_carries_cwd_and_env_because_kubectl_cannot() {
        let spec = StdioServerSpec::from_yaml(NOTION).unwrap().resolve_paths(
            "/opt/local_servers",
            "/workspace/agent_workspace",
            "",
        );
        // 逐字钉住：先建出 cwd 再进去（进不去就停），env 是**缺省值**不是覆盖值，最后 exec。
        // `exec` 是必须的——不经它，server 就是 sh 的子进程，本地关掉 stdin 时死的是 sh。
        assert_eq!(
            spec.remote_script(),
            concat!(
                "mkdir -p '/workspace/agent_workspace' || exit 1\n",
                "cd '/workspace/agent_workspace' || exit 1\n",
                "[ -n \"${OPENAPI_MCP_HEADERS+x}\" ] || export OPENAPI_MCP_HEADERS=",
                "'{\"Authorization\": \"Bearer ntn-placeholder\"}'\n",
                "[ -n \"${PGHOST+x}\" ] && export PG_HOST=\"${PGHOST}\"\n",
                "[ -n \"${PGPORT+x}\" ] && export PG_PORT=\"${PGPORT}\"\n",
                "[ -n \"${PGDATABASE+x}\" ] && export PG_DATABASE=\"${PGDATABASE}\"\n",
                "[ -n \"${PGUSER+x}\" ] && export PG_USER=\"${PGUSER}\"\n",
                "[ -n \"${PGPASSWORD+x}\" ] && export PG_PASSWORD=\"${PGPASSWORD}\"\n",
                "exec 'node' '/opt/local_servers/notion-mcp-server/bin/cli.mjs' '--page-id' ''"
            )
        );
        // 没钉 cwd 的那种**不是**不 cd：上游把缺省值取成 agent_workspace。
        let no_cwd = StdioServerSpec::from_yaml(
            "type: stdio\nname: x\nparams:\n  command: node\n  args: [a.js]\n",
        )
        .unwrap()
        .resolve_paths("/opt/local_servers", "/workspace/agent_workspace", "");
        assert_eq!(
            no_cwd.remote_script(),
            concat!(
                "mkdir -p '/workspace/agent_workspace' || exit 1\n",
                "cd '/workspace/agent_workspace' || exit 1\n",
                "[ -n \"${PGHOST+x}\" ] && export PG_HOST=\"${PGHOST}\"\n",
                "[ -n \"${PGPORT+x}\" ] && export PG_PORT=\"${PGPORT}\"\n",
                "[ -n \"${PGDATABASE+x}\" ] && export PG_DATABASE=\"${PGDATABASE}\"\n",
                "[ -n \"${PGUSER+x}\" ] && export PG_USER=\"${PGUSER}\"\n",
                "[ -n \"${PGPASSWORD+x}\" ] && export PG_PASSWORD=\"${PGPASSWORD}\"\n",
                "exec 'node' 'a.js'"
            )
        );
    }

    /// 逐字钉住跨进 Pod 的那条 argv——它是本模块唯一交给外部的形状。
    ///
    /// 钉在这里的理由不只是防回归：现场那条探针命令就是从这里印出来的，所以「量过的
    /// 那条命令」与「代码会发的那条命令」是同一个字符串，不靠人抄。
    #[test]
    fn the_argv_that_crosses_into_the_pod() {
        let runtime = PodRuntime::for_test(
            vec!["sudo".into(), "-n".into(), "kubectl".into()],
            "cogneva",
        );
        let spec = StdioServerSpec::from_yaml(
            "type: stdio\nname: probe\nparams:\n  command: /bin/sh\n  args: [/tmp/server.sh, --flag, \"\"]\n  env:\n    PROBE_MARK: \"x y'z\"\n    PG_HOST: postgres\n  cwd: /tmp\n",
        )
        .unwrap();
        let (program, args) = runtime.stdio_exec_argv("ta-mcp-probe", "srv", &spec.remote_script());
        assert_eq!(program, "sudo");
        assert_eq!(
            args,
            vec![
                "-n",
                "kubectl",
                "-n",
                "cogneva",
                "exec",
                "-i",
                "ta-mcp-probe",
                "-c",
                "srv",
                "--",
                "/bin/sh",
                "-c",
                "mkdir -p '/tmp' || exit 1\n\
                 cd '/tmp' || exit 1\n\
                 [ -n \"${PROBE_MARK+x}\" ] || export PROBE_MARK='x y'\\''z'\n\
                 [ -n \"${PG_HOST+x}\" ] || export PG_HOST='postgres'\n\
                 [ -n \"${PGHOST+x}\" ] && export PG_HOST=\"${PGHOST}\"\n\
                 [ -n \"${PGPORT+x}\" ] && export PG_PORT=\"${PGPORT}\"\n\
                 [ -n \"${PGDATABASE+x}\" ] && export PG_DATABASE=\"${PGDATABASE}\"\n\
                 [ -n \"${PGUSER+x}\" ] && export PG_USER=\"${PGUSER}\"\n\
                 [ -n \"${PGPASSWORD+x}\" ] && export PG_PASSWORD=\"${PGPASSWORD}\"\n\
                 exec '/bin/sh' '/tmp/server.sh' '--flag' ''",
            ]
        );
        // 印成 JSON：现场那条探针命令要由它**逐字**生成，多行脚本在 `join(" ")` 里读不回来。
        let mut argv = vec![program.clone()];
        argv.extend(args.iter().cloned());
        eprintln!("PROBE_ARGV_JSON: {}", serde_json::to_string(&argv).unwrap());
    }

    /// YAML 里的 `env` 是**缺省值**，进程环境赢；libpq 名在合并之后盖掉 `PG_*`。
    ///
    /// 这条不是照抄一段实现，是照抄上游那一份合并顺序——反过来写会让四个按库取数的
    /// server 拿到配置里的 `PG_HOST: postgres`，而 Pod 里没有叫这个名字的东西。
    #[test]
    fn the_server_env_is_a_default_and_the_process_env_wins() {
        let script = StdioServerSpec::from_yaml(
            "type: stdio\nname: x\nparams:\n  command: c\n  env:\n    PG_HOST: postgres\n    PG_DATABASE: toolathlon\n",
        )
        .unwrap()
        .resolve_paths("/a", "/w", "")
        .remote_script();
        // 缺省先落，且是「未设才落」——`${K+x}` 对空串也算「已设」，与 dict 合并同义。
        assert!(script.contains("[ -n \"${PG_HOST+x}\" ] || export PG_HOST='postgres'\n"));
        // 桥接在后面，所以库那侧给的 `PGHOST` 能盖掉它。
        let bridge = script.find("export PG_HOST=\"${PGHOST}\"").unwrap();
        let default = script.find("|| export PG_HOST='postgres'").unwrap();
        assert!(
            bridge > default,
            "the bridge has to run after the default:\n{script}"
        );
        // 五个 libpq 名一个不漏，顺序与上游那张表一致。
        for (libpq, pg) in LIBPQ_TO_PG {
            assert!(
                script.contains(&format!("[ -n \"${{{libpq}+x}}\" ] && export {pg}=")),
                "{libpq} -> {pg} missing from:\n{script}"
            );
        }
    }

    fn stub_server(name: &str, tool_names: &[&str]) -> ConnectedServer {
        let spec = StdioServerSpec::from_yaml(&format!(
            "type: stdio\nname: {name}\nparams:\n  command: c\n"
        ))
        .unwrap();
        // 客户端接不上任何远端也无所谓：这一层要看的是**面**怎么拼，不是真去问。
        let runtime = PodRuntime::for_test(vec!["true".into()], "ns");
        let client = Arc::new(client_in_pod(&runtime, "pod", "app", &spec).unwrap());
        ConnectedServer {
            name: name.to_string(),
            client,
            tools: tool_names
                .iter()
                .map(|n| McpTool {
                    name: (*n).to_string(),
                    description: format!("{n} on {name}"),
                    input_schema: serde_json::json!({"type": "object"}),
                })
                .collect(),
        }
    }

    /// 工具面的名字是 **MCP 工具自己的名字**，不是 server 名；撞名先起的赢。
    ///
    /// 这条判据对着的是上游实测（镜像里 camel-ai 0.2.90：两个 stub server 各带一件
    /// `echo`，`get_tools()` 出来 3 件、`echo` 只留第一台那件并打一条 warning）。
    #[test]
    fn the_face_is_named_by_the_tools_and_a_collision_keeps_the_first_server() {
        let face = ToolFace::build(&[
            stub_server("s_one", &["echo", "alpha_tool"]),
            stub_server("s_two", &["echo", "beta_tool"]),
        ]);
        let names: Vec<&str> = face.definitions.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["echo", "alpha_tool", "beta_tool"]);
        // 撞了名的那件归**第一台**，第二台那件被丢掉——丢的是名字，不是归因。
        assert_eq!(face.server_of("echo"), Some("s_one"));
        assert_eq!(face.server_of("beta_tool"), Some("s_two"));
        assert!(face.server_of("nobody").is_none());
        // server 名一个都不许出现在工具名里：出现就是换掉了动作空间。
        assert!(names
            .iter()
            .all(|n| !n.starts_with("s_one") && !n.starts_with("s_two")));
    }

    /// 一份读不懂的配置要留成**带路径的**因，而不是让整个目录读不出来。
    #[test]
    fn a_config_that_does_not_parse_is_kept_with_its_path_and_does_not_hide_the_others() {
        let configs = parse_config_records(&[
            (
                "/w/b.yaml".into(),
                "type: stdio\nname: beta\nparams:\n  command: b\n".into(),
            ),
            ("/w/a.yaml".into(), "this: [is not a server config\n".into()),
            (
                "/w/c.yaml".into(),
                "type: sse\nname: gamma\nparams:\n  command: c\n".into(),
            ),
        ]);
        // 排序按路径，不是按给的顺序：撞名时谁赢由它定。
        let names: Vec<&str> = configs
            .servers
            .iter()
            .map(|(_, s)| s.name.as_str())
            .collect();
        assert_eq!(names, vec!["beta"]);
        assert_eq!(configs.unreadable.len(), 2);
        // 读不懂正文的那一份没名字，`type: sse` 那一份读得出它自称什么。
        assert_eq!(configs.unreadable[0].1, None);
        assert_eq!(configs.unreadable[1].1.as_deref(), Some("gamma"));
        // 「声明里点了它、但那份起不来」要说得清是哪一份，别报成「没有这台」。
        let err = configs.why_missing("gamma");
        assert!(err.contains("cannot drive"), "{err}");
        // 点了名的那一份排在最前，别让读的人在一份失败清单里找它。
        let gamma_at = err.find("c.yaml").unwrap();
        let unrelated_at = err.find("a.yaml").unwrap();
        assert!(gamma_at < unrelated_at, "{err}");
    }

    /// 全是文本的答复给一段文本；带图片的给原始数组。
    #[test]
    fn a_text_only_reply_reaches_the_shell_as_text() {
        let text = McpToolResult {
            content: vec![
                cog_protocol::mcp::McpContent::text("first"),
                cog_protocol::mcp::McpContent::text("second"),
            ],
            is_error: false,
        };
        assert_eq!(render_result(&text), serde_json::json!("first\nsecond"));

        let mixed = McpToolResult {
            content: vec![cog_protocol::mcp::McpContent::Image {
                data: "AAAA".into(),
                mime_type: "image/png".into(),
            }],
            is_error: false,
        };
        assert!(render_result(&mixed).is_array());
    }

    /// 对着**一台真 Pod** 走完整条路：读配置 → 起 server → 列工具 → 真调一次。
    ///
    /// 它不随 `cargo test` 跑（要一台活着的 Pod，且要能 `kubectl exec`）。跑法是一个人、
    /// 一次手工：
    ///
    /// ```text
    /// kubectl -n cogneva run <name> --image=localhost:30500/toolathlon-pack:latest \
    ///   --overrides='{"spec":{"automountServiceAccountToken":false,"containers":[{"name":"app",
    ///   "image":"localhost:30500/toolathlon-pack:latest","command":["sleep","3600"]}]}}'
    /// COGNEVA_EVAL_TOOLATHLON_PROBE_POD=<name> cargo test -p cogneva --lib \
    ///   a_declared_server_really_comes_up -- --ignored --nocapture
    /// ```
    ///
    /// 它守的是**只有真 Pod 才验得了**的那几段：NUL 分帧读回整批配置、`kubectl exec -i`
    /// 撑住一根双向管、`${local_servers_paths}` 指到编译好的那棵树。这三处任何一处错了，
    /// 单元测试都照样全绿——因为它们测的是纯函数与拼出来的字符串。
    #[tokio::test]
    #[ignore = "needs a live pack pod: set COGNEVA_EVAL_TOOLATHLON_PROBE_POD (see the doc comment)"]
    async fn a_declared_server_really_comes_up() {
        let Ok(name) = std::env::var("COGNEVA_EVAL_TOOLATHLON_PROBE_POD") else {
            panic!("set COGNEVA_EVAL_TOOLATHLON_PROBE_POD to a running pack pod");
        };
        let namespace = std::env::var("COGNEVA_EVAL_TOOLATHLON_NAMESPACE")
            .unwrap_or_else(|_| "cogneva".to_string());
        let runtime = PodRuntime::for_test(
            vec!["sudo".into(), "-n".into(), "kubectl".into()],
            &namespace,
        );
        let configs = read_server_configs(&runtime, &name, "app", "/workspace/configs/mcp_servers")
            .await
            .expect("the pack image must carry the server configs");
        assert!(
            !configs.servers.is_empty(),
            "the config directory came back empty: the NUL framing or the glob is wrong"
        );
        let workspace = "/workspace/cogneva-eval/agent_workspace";
        let servers = connect_declared(
            &runtime,
            &name,
            "app",
            &configs,
            &["filesystem".to_string()],
            &ServerPaths {
                local_servers: "/opt/local_servers",
                agent_workspace: workspace,
                task_dir: "",
            },
        )
        .await
        .expect("filesystem must start and answer tools/list");
        let face = ToolFace::build(&servers);
        let names: Vec<&str> = face.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(
            names.contains(&"read_text_file"),
            "filesystem's own tool names must reach the face verbatim, got {names:?}"
        );
        // 真调一次：写一个文件再读回来，证明这条路不是只能列名字。
        let dir = "/workspace/cogneva-eval/agent_workspace";
        let write = face
            .execute(
                "write_file",
                serde_json::json!({"path": format!("{dir}/probe.txt"), "content": "hello"}),
            )
            .await
            .expect("write_file must reach the server");
        assert!(write.is_string(), "unexpected reply shape: {write}");
        let read = face
            .execute(
                "read_text_file",
                serde_json::json!({"path": format!("{dir}/probe.txt")}),
            )
            .await
            .expect("read_text_file must reach the server");
        assert_eq!(read, serde_json::json!("hello"));
    }
}
