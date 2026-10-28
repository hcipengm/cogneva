//! `cog-skill` — Markdown skill discovery, loading, and content serving.
//! This crate manages external skills: LLM-readable instruction documents
//! (SKILL.md + scripts/ + agents/ + references/) loaded dynamically from
//! the filesystem with hot-reload support.
//! **Zero LLM dependency.** This crate only does filesystem operations.
//! LLM triggering, script execution, and subagent spawning are handled by
//! the caller (`cog-agent` / `cog-orchestrator`).

pub mod discovery;
pub mod loader;
pub mod manifest;
pub mod plugin;
pub mod registry;

/// The label naming how a reading ended.
///
/// Every family here with more than one ending carries this label, and the
/// readers that count one ending select by it. It is declared once per crate
/// that writes it, under the same identifier, and a gate compares the
/// spellings: one edited copy publishes a family under a name every existing
/// selector reads as empty, which is what "this family never ran" looks like.
pub const LABEL_OUTCOME: &str = "outcome";

pub use registry::{SkillConfig, SkillRegistryImpl};

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;
