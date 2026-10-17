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

use std::time::Duration;

use cog_protocol::{McpClient, StdioTransport};

use crate::eval_toolathlon::PodRuntime;

/// 配置没写 `client_session_timeout_seconds` 时用的那个数。
///
/// 60 秒取自上游（`cfg.get("client_session_timeout_seconds", 60)`），不由调用点另定：
/// 两个地方各定一个默认值，同一条配置就有两个超时，而慢的那一台会被读成坏的那一台。
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
fn sh_quote(word: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval_toolathlon::PodRuntime;

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
}
