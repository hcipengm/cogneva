//! Agent 自有配置段（agent_loop / agent_pool）——core config.rs 不聚合
//! 单 crate 配置。自读 cogneva.json 对应段并叠加
//! `COGNEVA_AGENT_LOOP_*` / `COGNEVA_AGENT_POOL_*` env 覆盖。

use serde::{Deserialize, Serialize};
use std::path::Path;

use cog_core::{SFError, SFResult};

fn load_section<T: serde::de::DeserializeOwned + Default>(
    pointer: &str,
    env_map: &[(&str, &str)],
) -> SFResult<T> {
    let path =
        std::env::var("COGNEVA_CONFIG_PATH").unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
    load_section_from(Path::new(&path), pointer, env_map)
}

fn load_section_from<T: serde::de::DeserializeOwned + Default>(
    path: &Path,
    pointer: &str,
    env_map: &[(&str, &str)],
) -> SFResult<T> {
    let mut section = match std::fs::read_to_string(path) {
        Ok(text) => {
            let root: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
            root.pointer(pointer)
                .cloned()
                .unwrap_or(serde_json::json!({}))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
        Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
    };
    cog_core::config::apply_env_paths(&mut section, env_map);
    serde_json::from_value(section)
        .map_err(|e| SFError::Config(format!("{} {pointer}: {e}", path.display())))
}

/// The env-var names this section answers to. Published so the deploy-config
/// gate can tell "honored, but not by the core schema" from "honored by
/// nobody".
pub const AGENT_LOOP_ENV: &[(&str, &str)] = &[
    ("COGNEVA_AGENT_LOOP_MAX_ITERATIONS", "max_iterations"),
    (
        "COGNEVA_AGENT_LOOP_EVAL_MAX_ITERATIONS",
        "eval_max_iterations",
    ),
    (
        "COGNEVA_AGENT_LOOP_CONTEXT_WINDOW_SIZE",
        "context_window_size",
    ),
    ("COGNEVA_AGENT_LOOP_ROLE", "role"),
    (
        "COGNEVA_AGENT_LOOP_SKILL_CACHE_TTL_SECS",
        "skill_cache_ttl_secs",
    ),
    (
        "COGNEVA_AGENT_LOOP_THINK_STALL_TIMEOUT_SECS",
        "think_stall_timeout_secs",
    ),
    (
        "COGNEVA_AGENT_LOOP_FINAL_DRAFT_TIMEOUT_SECS",
        "final_draft_timeout_secs",
    ),
    // The platform's own HTTP surface, borrowed the way the business pods
    // borrow the gateway (see `COGNEVA_GITHUB_API_BASE`): the loop archives an
    // oversized tool output by POSTing to it, carrying no token. Named without
    // the `AGENT_LOOP` prefix because it points at a shared endpoint rather
    // than a knob of this section.
    (cog_core::MEMORY_API_BASE_ENV, "memory_api_base"),
];

const AGENT_POOL_ENV: &[(&str, &str)] = &[
    ("COGNEVA_AGENT_POOL_ENABLED", "enabled"),
    ("COGNEVA_AGENT_POOL_WORKER_COUNT", "worker_count"),
    ("COGNEVA_AGENT_POOL_WORKER_ROLE", "worker_role"),
];

/// Agent registration and heartbeat configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentLoopConfig {
    pub agent_id: String,
    pub role: String,
    pub max_iterations: u32,
    /// Iteration budget of the dedicated evaluator runtime. It differs from
    /// [`Self::max_iterations`] on purpose: judging a delivered result needs
    /// fewer turns than producing one. Kept here rather than as a literal at
    /// the construction site so raising it does not require a new image.
    pub eval_max_iterations: u32,
    pub context_window_size: usize,
    /// TTL for the available_skills cache in AgentRuntime (seconds).
    pub skill_cache_ttl_secs: u64,
    /// Abort a streaming think turn only after no stream event arrives for
    /// this many consecutive seconds (stall detection, not a wall-clock cap).
    pub think_stall_timeout_secs: u64,
    /// Wall-clock cap for the forced final-draft ask (seconds). Unlike the
    /// stall timeout this bounds the whole call, because that ask is the one
    /// that runs after the run's budget is already spent.
    pub final_draft_timeout_secs: u64,
    /// Base URL of the platform memory API where an oversized tool output is
    /// archived. Absent when the deployment names none; the loop then still
    /// truncates, and the failure reading carries the cause.
    pub memory_api_base: Option<String>,
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            agent_id: "agent-001".into(),
            role: "planner".into(),
            max_iterations: 10,
            eval_max_iterations: 5,
            context_window_size: 32000,
            skill_cache_ttl_secs: 30,
            think_stall_timeout_secs: 240,
            final_draft_timeout_secs: 420,
            memory_api_base: None,
        }
    }
}

impl AgentLoopConfig {
    pub fn load() -> SFResult<Self> {
        load_section("/agent_loop", AGENT_LOOP_ENV)
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        load_section_from(path, "/agent_loop", AGENT_LOOP_ENV)
    }
}

impl From<AgentLoopConfig> for cog_core::RuntimeConfig {
    fn from(c: AgentLoopConfig) -> Self {
        Self {
            agent_id: c.agent_id,
            role: c.role,
            max_iterations: c.max_iterations,
            context_window_size: c.context_window_size,
            skill_cache_ttl_secs: c.skill_cache_ttl_secs,
            think_stall_timeout_secs: c.think_stall_timeout_secs,
            final_draft_timeout_secs: c.final_draft_timeout_secs,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        }
    }
}

/// Agent 池配置（worker 数量与角色）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentManagerConfig {
    pub enabled: bool,
    pub worker_count: usize,
    pub worker_role: String,
}

impl Default for AgentManagerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            worker_count: 3,
            worker_role: "planner".into(),
        }
    }
}

impl AgentManagerConfig {
    pub fn load() -> SFResult<Self> {
        load_section("/agent_pool", AGENT_POOL_ENV)
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        load_section_from(path, "/agent_pool", AGENT_POOL_ENV)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_legacy_core_values() {
        let l = AgentLoopConfig::default();
        assert_eq!(l.agent_id, "agent-001");
        assert_eq!(l.role, "planner");
        assert_eq!(l.max_iterations, 10);
        // The evaluator budget defaults to the value the runtime used to carry
        // as a literal; moving it here must not change it on its own.
        assert_eq!(l.eval_max_iterations, 5);
        let p = AgentManagerConfig::default();
        assert!(p.enabled);
        assert_eq!(p.worker_count, 3);
    }

    #[test]
    fn the_memory_api_base_env_is_honored_by_this_section() {
        // The archive call borrows the platform's own HTTP surface by env, the
        // way the business pods borrow the gateway. If the name drops out of
        // this table the deploy-config gate stops being able to tell "honored"
        // from "honored by nobody", and the loop silently falls back to
        // unconfigured.
        assert!(AGENT_LOOP_ENV.contains(&(cog_core::MEMORY_API_BASE_ENV, "memory_api_base")));
    }

    #[test]
    fn reads_section_and_missing_file_defaults() {
        let p = Path::new("/nonexistent/cogneva.json");
        assert_eq!(AgentLoopConfig::load_from(p).unwrap().role, "planner");
        assert_eq!(AgentManagerConfig::load_from(p).unwrap().worker_count, 3);

        let dir = std::env::temp_dir().join(format!("cog-agent-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"agent_loop": {"role": "evaluator", "max_iterations": 3,
                    "eval_max_iterations": 7, "memory_api_base": "http://cogneva:8080"},
                "agent_pool": {"worker_count": 8}}"#,
        )
        .unwrap();
        let l = AgentLoopConfig::load_from(&path).unwrap();
        assert_eq!(l.role, "evaluator");
        assert_eq!(l.max_iterations, 3);
        assert_eq!(l.eval_max_iterations, 7);
        assert_eq!(l.memory_api_base.as_deref(), Some("http://cogneva:8080"));
        assert_eq!(
            AgentManagerConfig::load_from(&path).unwrap().worker_count,
            8
        );
        let rt: cog_core::RuntimeConfig = l.into();
        assert_eq!(rt.role, "evaluator");
        std::fs::remove_dir_all(&dir).ok();
    }
}
