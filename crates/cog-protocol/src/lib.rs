//! `cog-protocol` — MCP + A2A + Protobuf 协议支持。
//! 让 cogneva 能与外部 Agent/Tool 生态互通，不是孤岛。
//! - MCP Server：将 cogneva 的 Tool 以 MCP 协议暴露给外部
//! - MCP Client：调用外部 MCP tools
//! - A2A Agent Card：标准 Agent 描述 + Task 生命周期
//! - Protobuf 序列化：RawRecord envelope、WAL、AgentLifecycle gRPC
//!   Protobuf definitions live in this crate so that `cog-core` stays free of
//!   `prost`/`tonic` dependencies.

pub mod a2a;
pub mod convert;
pub mod grpc_agent_lifecycle;
pub mod mcp;
pub mod plugin;

/// Protobuf-generated types for agent lifecycle gRPC service.
/// tonic 生成的 service 方法以 `tonic::Status` 作 Err 变体（体积大），
/// 触发新版 clippy 的 result_large_err——生成代码不可改，模块级豁免。
#[allow(clippy::result_large_err)]
pub mod agent_lifecycle {
    include!(concat!(env!("OUT_DIR"), "/sf.network.agent_lifecycle.rs"));
}

/// Protobuf-generated types for raw record envelope.
pub mod raw {
    include!(concat!(env!("OUT_DIR"), "/sf.raw.v1.rs"));
}

/// Protobuf-generated types for WAL events.
pub mod wal {
    include!(concat!(env!("OUT_DIR"), "/sf.wal.v1.rs"));
}

pub use a2a::{
    A2aClient, A2aTask, AgentAuthentication, AgentCapabilities, AgentCard, AgentSkill, TaskStatus,
};
pub use mcp::{McpClient, McpServer, McpTransport, SseTransport};
