pub mod audit;
pub mod config;
pub mod contract;
pub mod error;
pub mod secrets;
pub mod types;

pub use audit::{verify_chain, AuditEvent, AuditKind, AuditStream, ChainVerification};
pub use config::*;
pub use contract::agent::*;
pub use contract::alerts::*;
pub use contract::auth::*;
pub use contract::codec::*;
pub use contract::context_builder::*;
pub use contract::dlq::*;
pub use contract::embedding::*;
pub use contract::eval::*;
pub use contract::event::*;
pub use contract::evolution_admin::*;
pub use contract::github::*;
pub use contract::guardrail::*;
pub use contract::hook::*;
pub use contract::knowledge::*;
pub use contract::llm::*;
pub use contract::media::*;
pub use contract::memory::*;
pub use contract::net::*;
pub use contract::notification::*;
pub use contract::observability::*;
pub use contract::orchestrator::*;
pub use contract::plugin::*;
pub use contract::promotion::*;
pub use contract::protocol::*;
pub use contract::quota::*;
pub use contract::reflection::*;
pub use contract::resilience::*;
pub use contract::sandbox::*;
pub use contract::shutdown::*;
pub use contract::skill::*;
pub use contract::snapshot::*;
pub use contract::storage::*;
pub use contract::stream::*;
pub use contract::supervisor::*;
pub use contract::system_plugin::*;
pub use contract::task::*;
pub use contract::tool::*;
pub use contract::wiki::*;
pub use error::{SFError, SFResult};
pub use secrets::{
    redact_secrets, ChainedSecretProvider, EnvSecretProvider, FileSecretProvider, SecretProvider,
};
pub use types::*;

// Backward-compatible module aliases for old module paths.
// Prefer `cog_core::ContractType` or `cog_core::contract::<domain>::Type` for new code.
pub mod agent {
    pub use crate::contract::agent::*;
}
pub mod alerts {
    pub use crate::contract::alerts::*;
}
pub mod auth {
    pub use crate::contract::auth::*;
}
pub mod codec {
    pub use crate::contract::codec::*;
}
pub mod dlq {
    pub use crate::contract::dlq::*;
}
pub mod embedding {
    pub use crate::contract::embedding::*;
}
pub mod eval {
    pub use crate::contract::eval::*;
}
pub mod guardrail {
    pub use crate::contract::guardrail::*;
}
pub mod hook {
    pub use crate::contract::hook::*;
}
pub mod knowledge {
    pub use crate::contract::knowledge::*;
}
pub mod llm {
    pub use crate::contract::llm::*;
}
pub mod media {
    pub use crate::contract::media::*;
}
pub mod memory {
    pub use crate::contract::memory::*;
}
pub mod net {
    pub use crate::contract::net::*;
}
pub mod notification {
    pub use crate::contract::notification::*;
}
pub mod observability {
    pub use crate::contract::observability::*;
}
pub mod orchestrator {
    pub use crate::contract::orchestrator::*;
}
pub mod plugin {
    pub use crate::contract::plugin::*;
}
pub mod protocol {
    pub use crate::contract::protocol::*;
}
pub mod quota {
    pub use crate::contract::quota::*;
}
pub mod reflection {
    pub use crate::contract::reflection::*;
}
pub mod resilience {
    pub use crate::contract::resilience::*;
}
pub mod sandbox {
    pub use crate::contract::sandbox::*;
}
pub mod shutdown {
    pub use crate::contract::shutdown::*;
}
pub mod skill {
    pub use crate::contract::skill::*;
}
pub mod snapshot {
    pub use crate::contract::snapshot::*;
}
pub mod storage {
    pub use crate::contract::storage::*;
}
pub mod stream {
    pub use crate::contract::stream::*;
}
pub mod supervisor {
    pub use crate::contract::supervisor::*;
}
pub mod system_plugin {
    pub use crate::contract::system_plugin::*;
}
pub mod tool {
    pub use crate::contract::tool::*;
}
pub mod wiki {
    pub use crate::contract::wiki::*;
}
pub mod agent_runtime {
    pub use crate::contract::agent::*;
}
pub mod agent_lifecycle {
    pub use crate::contract::agent::*;
}
pub mod agent_registration {
    pub use crate::contract::agent::*;
}
pub mod raw_logger {
    pub use crate::contract::storage::*;
}
pub mod wal {
    pub use crate::contract::storage::*;
}
pub mod tier_migrator {
    pub use crate::contract::storage::*;
}
pub mod memory_backend {
    pub use crate::contract::memory::*;
}
pub mod embedding_provider {
    pub use crate::contract::embedding::*;
}
pub mod knowledge_backend {
    pub use crate::contract::knowledge::*;
}
pub mod event_stream {
    pub use crate::contract::event::*;
}
pub mod event_publisher {
    pub use crate::contract::event::*;
}
pub mod hook_types {
    pub use crate::contract::hook::*;
}
pub mod hook_archive {
    pub use crate::contract::hook::*;
}
pub mod skill_registry {
    pub use crate::contract::skill::*;
}
pub mod task_executor {
    pub use crate::contract::task::*;
}
pub mod dag_executor {
    pub use crate::contract::orchestrator::*;
}
pub mod orchestrator_control {
    pub use crate::contract::orchestrator::*;
}

/// Re-export commonly used types
pub use chrono::{DateTime, Utc};
pub use serde_json;
pub use uuid::Uuid;
