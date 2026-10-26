//! `mcp-serve`：把本机的工具集按 MCP 协议交给一个外部 MCP 客户端。
//!
//! 传输是 stdio，不是 HTTP 端点，理由是信任边界而不是省事。stdio 那条传输的形状是
//! **客户端拉起服务端**：本进程与拉起它的进程同一个用户、同一台机器，它能做的，拉起
//! 它的人本来就能做，所以这条路上没有新多出来的授权面。一个监听端口则要先回答一个
//! 今天没有答案的问题——网络那头来的调用方是谁、凭什么让它跑 `run_command`；在没有
//! 那个答案之前摆一个监听端口，等于把任意命令执行挂在一个无人认领的地址上。（网关
//! 那条 MCP 路由是**反方向**的：它作为客户端去调外部服务端。）
//!
//! 工具集与主应用是同一批，不是另抄一份更弱的：同一批内建工具、同一个沙箱后端选择
//! （配了执行器地址就发给执行器 Pod，没配就在本进程内跑）、同一套护栏、同一个
//! `require_tool_identity` 读数。差别只在谁在调用它。

use std::sync::Arc;

use cog_core::ToolDefinition;

/// 起一个 MCP 服务端，在本进程的标准输入输出上服务，直到客户端关闭它的输入。
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    // 诊断必须走标准错误。标准输出在这条路上不是日志口而是**线**：往它上面写一行
    // 非 JSON-RPC 的东西，对客户端就是一条读不懂的消息，而它按行配对，不会知道那
    // 是给人看的。默认的 fmt 层写标准输出，所以这里必须点名 writer。
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = crate::config_loader::load();
    let system = &config.system;

    let backend = cog_extension::executor_backend(system.sandbox_executor_url.clone())?;
    let registry = cog_agent::ToolRegistry::new()
        .with_wasm_timeout(system.tool_timeout_secs)
        .with_shell_timeout(system.shell_timeout_secs)
        .with_sandbox_backend(backend)
        .with_guardrail(cog_guardrail::build_default_guardrail(Arc::new(
            cog_guardrail::InMemoryAuditRecorder::new(),
        )))
        .with_require_identity(system.require_tool_identity);

    // 内建工具**整批**取自与主应用同一个构造函数（`register_builtins`），不在这里
    // 另列一份。列一份更短的表曾经就写在下面这一行：它看起来完成度更高（少三个
    // 需要外部依赖的工具），实际上是把「这一层不存在」和「这个命名空间是空的」两
    // 件事合并成同一句「没有这个工具」，而模块头说的「同一批内建工具」也就不成立
    // 了。三个 shell 类实现都经过上面那个沙箱后端——所以这一层不是「有一张工具
    // 表」而是一张**跑得动**的工具表。HTTP 客户端在这里建（与主进程同一条工厂，
    // 超时/连接池/代理一致），内存 API 地址与归档阈值读同一个 `agent_loop` 配置节。
    // 客户端按「尽力而为」建：建不出来只少 `http_request` 一个工具，服务照起。
    // 让这一句 `?` 掉是拿一条与 MCP 无关的失败换掉整个服务端——本进程主要的活是
    // 交工具，不是发 HTTP。
    let http_client: Option<Arc<dyn cog_core::HttpClient>> = match cog_net::HttpClientConfig::load()
        .and_then(|config| cog_net::ReqwestHttpClient::from_config(&config))
    {
        Ok(client) => Some(Arc::new(client)),
        Err(e) => {
            tracing::warn!(error = %e, "mcp-serve: no HTTP client; http_request is not offered");
            None
        }
    };
    let agent_loop = cog_agent::AgentLoopConfig::load()?;
    cog_agent::tools::builtins::register_builtins(
        &registry,
        cog_agent::tools::builtins::BuiltinInputs {
            http_client,
            // 这个进程没有指标后端：`raw_fetch` 的预算耗尽读数随之为空，工具本身
            // 照常注册——缺读数不等于缺能力，二者在读面上不同形。
            metrics: None,
            memory_api_base: agent_loop.memory_api_base.clone(),
            external_archive_threshold_chars: agent_loop.external_archive_threshold_chars,
            // 这个进程由客户端拉起，不连 Redis、也没有 agent 台账，所以 `send_to_agent`
            // 不注册：没有队列可写，也判断不了对面还活不活着。工具缺席在这里是实情，
            // 而不是把「这一层不存在」说成「没有这个工具」——后者是那份更短的工具
            // 表犯过的错。
            message_backend: None,
            agent_registry: None,
            web_search_base: agent_loop.web_search_base.clone(),
            web_search_fallback_base: agent_loop.web_search_fallback_base.clone(),
            web_fetch_base: agent_loop.web_fetch_base.clone(),
        },
    );

    let definitions: Vec<ToolDefinition> = registry
        .list()
        .into_iter()
        .map(|tool| ToolDefinition {
            name: tool.name,
            description: tool.description,
            parameters: tool.parameters,
        })
        .collect();
    tracing::info!(
        tools = ?registry.names(),
        "mcp-serve: serving this machine's tools over stdio"
    );

    let executor: Arc<dyn cog_core::ToolExecutor> = Arc::new(registry);
    let server = cog_protocol::McpServer::new(definitions).with_executor(executor);
    cog_protocol::serve_stdio(&server).await?;
    Ok(())
}
