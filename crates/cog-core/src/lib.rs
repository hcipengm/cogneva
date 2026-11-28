/// 一个 Result 出错时用缺省值继续，并把这个错误记下来。
///
/// `unwrap_or(默认值)` 在接收者不是 Result 时过不了编译——那正是这条宏与编译器的
/// 分工：能改成它的，是「有一个错误被换成了默认值」，于是必须记下来；改不成的，
/// 那个缺省值只是在补一个缺席的值，不是吞一个错误。
#[macro_export]
macro_rules! or_lost {
    ($e:expr, $d:expr) => {{
        match $e {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(error = ?err, "an error was replaced by a default here");
                $d
            }
        }
    }};
}

/// 丢掉一个结果时把错误记下来：`let _ = expr;` 的替代。
///
/// 「丢掉」在账面上与「没有出错」同形；这条宏把它变成一条带位置的 debug 记录，
/// 于是「这一步失败过」读得出来，而不是只剩静默。
/// 读取失败时按缺省值继续，但把错误**记下来**：`unwrap_or` / `unwrap_or_default`
/// 在错误上静默换成空值的替代。`source` 是那一格自己的表达式，日志里看得见是哪一次读。
#[macro_export]
macro_rules! logged_default {
    ($e:expr) => {{
        match $e {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = ?err, source = stringify!($e), "读取失败，按缺省值继续");
                Default::default()
            }
        }
    }};
    ($e:expr, $default:expr) => {{
        match $e {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = ?err, source = stringify!($e), "读取失败，按声明的缺省值继续");
                $default
            }
        }
    }};
}

/// 读取失败时按「没有」传递，并把错误记下来：`Result::ok()` 在错误上静默变成
/// `None` 的替代。调用点按 `None` 分支走，日志里留有这一次读为什么是空的。
#[macro_export]
macro_rules! logged_ok {
    ($e:expr) => {{
        match $e {
            Ok(value) => Some(value),
            Err(err) => {
                tracing::debug!(error = ?err, source = stringify!($e), "读取失败，按「没有」传递");
                None
            }
        }
    }};
}

#[macro_export]
macro_rules! ignore_lost {
    ($e:expr) => {{
        if let Err(err) = $e {
            tracing::debug!(error = ?err, "a result was dropped here; see this location");
        }
    }};
}

pub mod admission;
pub mod atomic_write;
pub mod attempt_journal;
pub mod audit;
pub mod bound;
pub mod build_cache;
pub mod build_cache_readings;
pub mod build_cache_reclaim;
pub mod claim_footprint;
pub mod config;
pub mod config_sections;
pub mod contract;
pub mod day;
pub mod day_key;
pub mod declared_wall;
pub mod env;
pub mod error;
pub mod excerpt;
pub mod fs_size;
pub mod git_budget;
pub mod host;
pub mod loop_health;
pub mod observability_text;
pub mod pacing;
pub mod paths;
pub mod progress;
pub mod reading_cadence;
pub mod retention;
pub mod roles;
pub mod secrets;
pub mod sql;
pub mod stagnation;
pub mod token_estimate;
pub mod topology;
pub mod types;
pub mod vocabulary;

pub use audit::{verify_chain, AuditEvent, AuditKind, AuditStream, ChainVerification};
pub use bound::{Bound, BoundSource};
pub use config::*;
pub use contract::agent::*;
pub use contract::alerts::*;
pub use contract::auth::*;
pub use contract::backup_run::*;
pub use contract::body_store::*;
pub use contract::change_ledger::*;
pub use contract::codec::*;
pub use contract::context_builder::*;
pub use contract::conversation::*;
pub use contract::dataset::*;
pub use contract::decision_stats::*;
pub use contract::dlq::*;
pub use contract::embedding::*;
pub use contract::employee_events;
pub use contract::employee_events::*;
pub use contract::eval::*;
pub use contract::event::*;
pub use contract::event_envelope::*;
pub use contract::event_fact::*;
pub use contract::event_id::*;
pub use contract::event_kinds::*;
pub use contract::event_log::*;
pub use contract::event_payload::*;
pub use contract::evolution_admin::*;
pub use contract::face_registry::*;
pub use contract::fault::*;
pub use contract::governance_acts::*;
pub use contract::guardrail::*;
pub use contract::hook::*;
pub use contract::host_documents;
pub use contract::intent_events::*;
pub use contract::knowledge::*;
pub use contract::kv::*;
pub use contract::live_revision::*;
/// Retry judgement for LLM refusals: the typed `UpstreamFailure` here, the
/// in-band deterministic-failure predicates under `contract::outcome`.
pub use contract::llm::*;
pub use contract::llm_call_event::*;
pub use contract::media::*;
pub use contract::memory::*;
pub use contract::message_events;
pub use contract::message_events::*;
pub use contract::metric_help::{documented_metric_names, metric_description, metric_help_text};
pub use contract::metric_names;
pub use contract::metric_names::{is_registered_metric, metric_name_of_ident, MetricName};
pub use contract::model_windows;
pub use contract::net::*;
pub use contract::notification::*;
pub use contract::observability::*;
pub use contract::orchestrator::*;
pub use contract::owner_lease;
pub use contract::owner_lease::{OwnerLease, OwnerLeaseBroker, Ownership, RoleClaim, RoleHold};
pub use contract::pge_stage::*;
pub use contract::plugin::*;
pub use contract::principle_bundle::*;
pub use contract::principle_bundle_sealed::*;
pub use contract::principle_ledger::*;
pub use contract::project_audit::*;
pub use contract::project_kind::*;
pub use contract::promotion::*;
pub use contract::protocol::*;
pub use contract::quota::*;
pub use contract::reflection::*;
pub use contract::reranker::*;
pub use contract::resilience::*;
pub use contract::resource_ledger::*;
pub use contract::resume_point::*;
pub use contract::round_batch::*;
pub use contract::runtime_principle::*;
pub use contract::sandbox::*;
pub use contract::security_keys::*;
pub use contract::shutdown::*;
pub use contract::skill::*;
pub use contract::snapshot::*;
pub use contract::storage::*;
pub use contract::stream::*;
pub use contract::supervisor::*;
pub use contract::system_plugin::*;
pub use contract::task::*;
pub use contract::taste::*;
pub use contract::tool::*;
pub use contract::tool_call_event::*;
pub use contract::usage_ledger::*;
pub use contract::wiki::*;
pub use day_key::{day_key, DAY_KEY_FORMAT};
pub use error::{SFError, SFResult};
pub use excerpt::Excerpt;
pub use secrets::{
    redact_json_secrets, redact_secrets, ChainedSecretProvider, EnvSecretProvider,
    FileSecretProvider, SecretProvider,
};
pub use token_estimate::estimate_tokens;
pub use types::*;
pub use vocabulary::fill_slots;

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
pub mod usage_ledger {
    pub use crate::contract::usage_ledger::*;
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

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;
pub use contract::rebuild_ports::*;
