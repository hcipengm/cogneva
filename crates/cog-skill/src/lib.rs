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
