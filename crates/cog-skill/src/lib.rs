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

pub use registry::{SkillConfig, SkillRegistryImpl};
