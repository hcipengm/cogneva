use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

use super::engine::HookEngine;
use cog_core::HookDef;

/// Top-level shape of the platform hooks config file (e.g. `/etc/cogneva/hooks.yaml` on Linux).
/// Example:
/// ```yaml
/// hooks:
///   - id: notify-completed
///     trigger: on_task_complete
///     action:
///       type: webhook
///       url: https://example.com/hook
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HookConfig {
    #[serde(default)]
    pub hooks: Vec<HookDef>,
}

impl HookConfig {
    /// Parse a YAML string into a [`HookConfig`].
    pub fn from_yaml(text: &str) -> SFResult<Self> {
        serde_yaml::from_str(text)
            .map_err(|e| SFError::Config(format!("invalid hooks YAML: {}", e)))
    }

    /// Read and parse a YAML file from disk.
    /// A missing file is treated as an empty config — this lets the engine
    /// boot in environments that don't ship a hooks config file.
    pub async fn load_from_path(path: impl AsRef<Path>) -> SFResult<Self> {
        let path = path.as_ref();
        match tokio::fs::read_to_string(path).await {
            Ok(text) => Self::from_yaml(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(SFError::IO(format!(
                "failed to read {}: {}",
                path.display(),
                e
            ))),
        }
    }

    /// Apply the parsed config to a running [`HookEngine`].
    pub async fn apply(&self, engine: &Arc<HookEngine>) {
        engine.replace_hooks(self.hooks.clone()).await;
    }
}

/// Convenience: load a YAML file and apply its contents to an engine.
pub async fn load_and_apply(path: impl AsRef<Path>, engine: &Arc<HookEngine>) -> SFResult<()> {
    let cfg = HookConfig::load_from_path(path).await?;
    cfg.apply(engine).await;
    Ok(())
}

/// Merge runtime hook definitions (e.g. fetched from a Redis Hash) on top of
/// the YAML-loaded set.  Runtime entries with the same `id` overwrite the
/// YAML defaults.
pub async fn apply_runtime_overrides(engine: &Arc<HookEngine>, runtime: Vec<HookDef>) {
    let mut current = engine.list_hooks().await;
    for def in runtime {
        if let Some(idx) = current.iter().position(|h| h.id == def.id) {
            current[idx] = def;
        } else {
            current.push(def);
        }
    }
    engine.replace_hooks(current).await;
}

#[cfg(test)]
mod tests {
    use super::super::engine::{HookEngine, HookPublisher};
    use super::*;
    use async_trait::async_trait;
    use cog_core::{HookAction, HookTrigger, LogLevel};
    use std::collections::HashMap;
    use tempfile::tempdir;

    struct NoopPublisher;
    #[async_trait]
    impl HookPublisher for NoopPublisher {
        async fn publish_webhook(
            &self,
            _: &str,
            _: &HashMap<String, String>,
            _: &serde_json::Value,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn publish_redis_stream(&self, _: &str, _: &serde_json::Value) -> SFResult<()> {
            Ok(())
        }
        async fn notify_user(&self, _: &str, _: &serde_json::Value) -> SFResult<()> {
            Ok(())
        }
    }

    #[test]
    fn parses_yaml_with_multiple_hooks() {
        let yaml = r#"
hooks:
  - id: log-start
    trigger: on_agent_start
    action:
      type: log
      level: info
  - id: ralph-pass
    trigger: on_ralph_pass
    action:
      type: redis_stream
      channel: orchestrator:events
"#;
        let cfg = HookConfig::from_yaml(yaml).unwrap();
        assert_eq!(cfg.hooks.len(), 2);
        assert_eq!(cfg.hooks[0].trigger, HookTrigger::OnAgentStart);
        assert!(matches!(cfg.hooks[0].action, HookAction::Log { .. }));
    }

    #[tokio::test]
    async fn missing_file_is_empty_config() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does_not_exist.yaml");
        let cfg = HookConfig::load_from_path(&path).await.unwrap();
        assert!(cfg.hooks.is_empty());
    }

    #[tokio::test]
    async fn load_and_apply_replaces_engine_hooks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("hooks.yaml");
        tokio::fs::write(
            &path,
            "hooks:\n  - id: a\n    trigger: on_agent_start\n    action:\n      type: log\n",
        )
        .await
        .unwrap();

        let engine = Arc::new(HookEngine::new(Arc::new(NoopPublisher)));
        load_and_apply(&path, &engine).await.unwrap();
        let hooks = engine.list_hooks().await;
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].id, "a");
    }

    #[tokio::test]
    async fn runtime_overrides_replace_by_id() {
        let engine = Arc::new(HookEngine::new(Arc::new(NoopPublisher)));
        engine
            .replace_hooks(vec![
                HookDef {
                    id: "shared".into(),
                    trigger: HookTrigger::OnAgentStart,
                    scope: cog_core::HookScope::Global,
                    crew_id_filter: None,
                    squad_id_filter: None,
                    action: HookAction::Log {
                        level: LogLevel::Info,
                    },
                    rate_limit: None,
                    timeout_ms: None,
                },
                HookDef {
                    id: "yaml-only".into(),
                    trigger: HookTrigger::OnAgentEnd,
                    scope: cog_core::HookScope::Global,
                    crew_id_filter: None,
                    squad_id_filter: None,
                    action: HookAction::Log {
                        level: LogLevel::Info,
                    },
                    rate_limit: None,
                    timeout_ms: None,
                },
            ])
            .await;

        apply_runtime_overrides(
            &engine,
            vec![
                HookDef {
                    id: "shared".into(),
                    trigger: HookTrigger::OnAgentStart,
                    scope: cog_core::HookScope::Global,
                    crew_id_filter: None,
                    squad_id_filter: None,
                    action: HookAction::Log {
                        level: LogLevel::Warn,
                    },
                    rate_limit: None,
                    timeout_ms: None,
                },
                HookDef {
                    id: "runtime-only".into(),
                    trigger: HookTrigger::OnTaskFail,
                    scope: cog_core::HookScope::Global,
                    crew_id_filter: None,
                    squad_id_filter: None,
                    action: HookAction::Log {
                        level: LogLevel::Error,
                    },
                    rate_limit: None,
                    timeout_ms: None,
                },
            ],
        )
        .await;

        let hooks = engine.list_hooks().await;
        assert_eq!(hooks.len(), 3);
        let shared = hooks.iter().find(|h| h.id == "shared").unwrap();
        match &shared.action {
            HookAction::Log { level } => assert_eq!(*level, LogLevel::Warn),
            _ => panic!("expected log action"),
        }
        assert!(hooks.iter().any(|h| h.id == "runtime-only"));
        assert!(hooks.iter().any(|h| h.id == "yaml-only"));
    }

    #[test]
    fn parses_yaml_with_scope_and_filters() {
        let yaml = r#"
hooks:
  - id: crew-specific
    trigger: on_task_complete
    scope: crew
    crew_id_filter: crew:task-1
    action:
      type: log
      level: info
  - id: squad-specific
    trigger: on_ralph_pass
    scope: squad
    squad_id_filter: crew:task-1:squad:0
    action:
      type: redis_stream
      channel: events
  - id: global-default
    trigger: on_agent_start
    action:
      type: log
      level: debug
"#;
        let cfg = HookConfig::from_yaml(yaml).unwrap();
        assert_eq!(cfg.hooks.len(), 3);

        let crew_hook = cfg.hooks.iter().find(|h| h.id == "crew-specific").unwrap();
        assert_eq!(crew_hook.scope, cog_core::HookScope::Crew);
        assert_eq!(crew_hook.crew_id_filter, Some("crew:task-1".into()));
        assert!(crew_hook.squad_id_filter.is_none());

        let squad_hook = cfg.hooks.iter().find(|h| h.id == "squad-specific").unwrap();
        assert_eq!(squad_hook.scope, cog_core::HookScope::Squad);
        assert_eq!(
            squad_hook.squad_id_filter,
            Some("crew:task-1:squad:0".into())
        );
        assert!(squad_hook.crew_id_filter.is_none());

        let global_hook = cfg.hooks.iter().find(|h| h.id == "global-default").unwrap();
        assert_eq!(global_hook.scope, cog_core::HookScope::Global);
        assert!(global_hook.crew_id_filter.is_none());
        assert!(global_hook.squad_id_filter.is_none());
    }
}
