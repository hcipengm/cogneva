use std::time::Duration;

/// Default per-hook execution timeout (30 seconds).
// cap: bound
pub const DEFAULT_HOOK_TIMEOUT: Duration = Duration::from_secs(30);

/// Default deduplication window (1 second).  Identical events fired inside the
/// window are coalesced — only the first one runs the action.
// cap: bound
pub const DEFAULT_DEDUP_WINDOW: Duration = Duration::from_secs(1);

/// The Redis stream a user's notifications are published to, before the user
/// id.
///
/// Public on its own because a subscriber matches on the prefix rather than on
/// a built name.
pub const NOTIFY_CHANNEL_PREFIX: &str = "sf:notify:";

/// The stream one user's notifications are published to.
///
/// Three publishers build this name — the plain one, the tiered one and the
/// one that buffers through the WAL — and each used to format the prefix
/// itself. Renaming it in one of them does not fail anywhere: that user's
/// notifications simply stop arriving by that path, and fewer notifications
/// reads the same as nothing having happened for them.
pub fn notify_channel(user_id: &str) -> String {
    format!("{NOTIFY_CHANNEL_PREFIX}{user_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{HookAction, HookDef, HookEvent, HookScope, HookTrigger, LogLevel};

    /// 这条名字是给订阅端看的：外面按 `sf:notify:*` 订阅，读的人不在这个
    /// crate 里。所以这里钉的是线上的形状本身，不是「函数等于自己」。
    #[test]
    fn a_user_stream_is_the_declared_prefix_plus_the_id() {
        assert_eq!(notify_channel("u-1"), "sf:notify:u-1");
    }

    #[test]
    fn dedup_key_uses_explicit_value() {
        let e = HookEvent::new(HookTrigger::OnAgentStart).with_dedup_key("custom");
        assert_eq!(e.effective_dedup_key(), "custom");
    }

    #[test]
    fn dedup_key_falls_back_to_agent_task_trigger() {
        let e = HookEvent::new(HookTrigger::OnTaskComplete)
            .with_agent_id("a-1")
            .with_task_id("t-1");
        assert_eq!(e.effective_dedup_key(), "a-1|t-1|OnTaskComplete");
    }

    #[test]
    fn hookdef_timeout_defaults_to_30s() {
        let def = HookDef {
            id: "x".into(),
            trigger: HookTrigger::OnAgentEnd,
            scope: HookScope::Global,
            crew_id_filter: None,
            squad_id_filter: None,
            action: HookAction::Log {
                level: LogLevel::Info,
            },
            rate_limit: None,
            timeout_ms: None,
        };
        assert_eq!(def.timeout(), DEFAULT_HOOK_TIMEOUT);
    }

    #[test]
    fn hookdef_serde_roundtrip_yaml() {
        let yaml = r#"
id: notify-completed
trigger: on_task_complete
action:
  type: webhook
  url: https://example.com/hook
  headers:
    X-Token: secret
rate_limit:
  burst: 10
  per_second: 5
timeout_ms: 5000
"#;
        let def: HookDef = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(def.id, "notify-completed");
        assert_eq!(def.trigger, HookTrigger::OnTaskComplete);
        assert!(matches!(def.action, HookAction::Webhook { .. }));
        assert_eq!(def.timeout(), Duration::from_millis(5000));
        assert_eq!(def.scope, HookScope::Global, "scope defaults to Global");
        assert!(def.crew_id_filter.is_none());
        assert!(def.squad_id_filter.is_none());
    }

    #[test]
    fn hookdef_serde_with_scope_and_filters() {
        let yaml = r#"
id: crew-specific
trigger: on_task_complete
scope: crew
crew_id_filter: crew:task-1
action:
  type: log
  level: info
"#;
        let def: HookDef = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(def.id, "crew-specific");
        assert_eq!(def.scope, HookScope::Crew);
        assert_eq!(def.crew_id_filter, Some("crew:task-1".into()));
        assert!(def.squad_id_filter.is_none());
    }

    #[test]
    fn hookdef_serde_squad_scope_with_filter() {
        let yaml = r#"
id: squad-specific
trigger: on_ralph_pass
scope: squad
squad_id_filter: crew:task-1:squad:0
action:
  type: redis_stream
  channel: events
"#;
        let def: HookDef = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(def.id, "squad-specific");
        assert_eq!(def.scope, HookScope::Squad);
        assert_eq!(def.squad_id_filter, Some("crew:task-1:squad:0".into()));
        assert!(def.crew_id_filter.is_none());
    }

    #[test]
    fn hook_event_builder_crew_and_squad() {
        let e = HookEvent::new(HookTrigger::OnCrewComplete)
            .with_crew_id("crew:task-1")
            .with_squad_id("crew:task-1:squad:0");
        assert_eq!(e.crew_id, Some("crew:task-1".into()));
        assert_eq!(e.squad_id, Some("crew:task-1:squad:0".into()));
    }
}
