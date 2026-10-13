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

    // 内建环境工具三件套，与主应用注册的是同一批函数（read_file / write_file /
    // run_command）。它们都是 shell 类实现，即都要经过上面那个沙箱后端——所以这一
    // 层不是「有一张工具表」而是一张**跑得动**的工具表。
    cog_core::ToolRegistry::register(&registry, cog_agent::tools::builtins::read_file());
    cog_core::ToolRegistry::register(&registry, cog_agent::tools::builtins::write_file());
    cog_core::ToolRegistry::register(&registry, cog_agent::tools::builtins::run_command());

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
