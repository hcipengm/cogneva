//! Which parts of the configuration document reach a running process, and when.
//!
//! The process watches the config file and applies a hand-written list of
//! sections when it changes. Everything outside that list is read once while the
//! owning crate starts: the file on disk moves on, the process keeps the copy it
//! read, and nothing in the log says so. That is how a delivered alert rule set
//! can sit in a ConfigMap while the process keeps enforcing the previous one —
//! the delivery is reported as applied, and the rules in force are still the old
//! ones.
//!
//! The dispositions live here as data rather than as a habit, so two things can
//! exist: a gate that fails when the document grows a section nobody classified,
//! and a delivery path that rolls the workloads when a section that only takes
//! effect at startup has changed.
//!
//! Granularity is the whole section. A section is marked hot-reloadable only when
//! every field the running process reads from it is re-applied on reload; when
//! that is true of only some fields, the section is marked as needing a restart
//! (a restart too many is cheap, a silently inert field is the defect this
//! module prevents). The notes record the fields that do reload, so the coarse
//! answer is visible rather than implied.

use serde_json::Value;
use std::collections::BTreeSet;

/// How a section of the configuration document reaches the running process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigEffect {
    /// The reload path re-applies it to the running process: delivering the file
    /// is enough.
    HotReloaded,
    /// Read while the process starts. A change stays inert until a restart, so
    /// whoever delivers the file has to roll the workloads that mount it.
    RestartRequired,
}

/// One top-level key of the configuration document.
#[derive(Debug, Clone, Copy)]
pub struct ConfigSection {
    pub name: &'static str,
    pub effect: ConfigEffect,
    /// Who reads it, and what the reload path does or does not cover. Written for
    /// whoever has to classify the next new section; no code parses it.
    pub note: &'static str,
}

macro_rules! section {
    ($name:literal, $effect:ident, $note:literal) => {
        ConfigSection {
            name: $name,
            effect: ConfigEffect::$effect,
            note: $note,
        }
    };
}

/// Every top-level section of the delivered document, with its disposition.
///
/// A gate in the workspace compares this list against the shipped document in
/// both directions: a section with no entry fails, and an entry with no section
/// fails. Adding a section is therefore a deliberate act with a stated effect.
pub const CONFIG_SECTIONS: &[ConfigSection] = &[
    section!(
        "_comment",
        HotReloaded,
        "Free-form note. Changes nothing, so it must not cost a restart."
    ),
    section!(
        "env",
        HotReloaded,
        "Env-var to config-path mapping; the loader re-applies it on every load, including reloads."
    ),
    section!(
        "app",
        RestartRequired,
        "Reload covers app.log_level only. name/version/data_dir/config_dir/app_dir are read at startup."
    ),
    section!(
        "gateway",
        RestartRequired,
        "Reload covers the websocket and request timeouts and the notification limit. Ports cannot be \
         re-bound at all, and the remaining fields are read at startup."
    ),
    section!(
        "supervisor",
        RestartRequired,
        "Reload pushes the intervals to the supervisor, but control_plane_url and the health_checker \
         thresholds are read when it is constructed."
    ),
    section!(
        "metrics",
        RestartRequired,
        "The endpoint is bound at startup; the reload path can only report the change."
    ),
    section!(
        "llm_routing",
        HotReloaded,
        "cog-llm re-reads it on reload and swaps the provider graph when it differs."
    ),
    section!(
        "tuning",
        HotReloaded,
        "Re-read alongside llm_routing on reload to rebuild the stream capacity."
    ),
    section!(
        "prompts",
        RestartRequired,
        "dir/hot_reload are read when the prompt plugin starts. Prompt content lives in a separate \
         ConfigMap whose directory watcher does reload on its own."
    ),
    section!(
        "observability",
        RestartRequired,
        "Alert rules and the collector settings are read when the plugin starts, so a delivered rule \
         set is inert until the deployment rolls."
    ),
    section!(
        "memory",
        RestartRequired,
        "Ingest gates and model loading are read when the memory plugin starts."
    ),
    section!(
        "github_integration",
        RestartRequired,
        "Read when the github plugin starts, including the webhook port it binds."
    ),
    section!(
        "gitee_integration",
        RestartRequired,
        "Read when the github plugin starts."
    ),
    section!(
        "system",
        RestartRequired,
        "Channel capacities and timeouts are read at startup."
    ),
    section!(
        "providers",
        RestartRequired,
        "Connections are opened at startup."
    ),
    section!(
        "dag_executor",
        RestartRequired,
        "Consumer groups and batch settings are read at startup."
    ),
    section!(
        "agent_loop",
        RestartRequired,
        "Iteration and context limits are read at startup."
    ),
    section!(
        "agent_pool",
        RestartRequired,
        "Worker count and role are read at startup."
    ),
    section!(
        "boundary",
        RestartRequired,
        "Rule set is read at startup."
    ),
    section!(
        "hook_engine",
        RestartRequired,
        "Rate limits and dedup window are read at startup."
    ),
    section!(
        "lifecycle",
        RestartRequired,
        "Heartbeat and threshold settings are read at startup."
    ),
    section!(
        "multi_backend_consumer",
        RestartRequired,
        "Channel, group and retention are read at startup."
    ),
    section!(
        "pge",
        RestartRequired,
        "Schemas are read at startup."
    ),
    section!(
        "ralph",
        RestartRequired,
        "Iteration and stagnation settings are read at startup."
    ),
    section!(
        "raw_logger",
        RestartRequired,
        "Buffer and flush settings are read at startup."
    ),
    section!(
        "self_evolution",
        RestartRequired,
        "Fences that decide whether the system may change itself are read at startup."
    ),
    section!(
        "self_review",
        RestartRequired,
        "Iteration and quality thresholds are read at startup."
    ),
    section!(
        "tier_migrator",
        RestartRequired,
        "Scan intervals and compression settings are read at startup."
    ),
];

/// The disposition recorded for a section, or `None` when the table does not
/// mention it.
pub fn effect_of(name: &str) -> Option<ConfigEffect> {
    CONFIG_SECTIONS
        .iter()
        .find(|s| s.name == name)
        .map(|s| s.effect)
}

/// Top-level sections whose value differs between two documents, including
/// sections one side does not have at all.
pub fn changed_sections(old: &Value, new: &Value) -> BTreeSet<String> {
    let mut changed = BTreeSet::new();
    if let Some(new_map) = new.as_object() {
        for (name, new_value) in new_map {
            if old.get(name) != Some(new_value) {
                changed.insert(name.clone());
            }
        }
    }
    if let Some(old_map) = old.as_object() {
        for name in old_map.keys() {
            if new.get(name).is_none() {
                changed.insert(name.clone());
            }
        }
    }
    changed
}

/// Changed sections that no already-running copy can pick up without a restart,
/// in document order-independent alphabetical order.
///
/// A section the table does not mention counts as needing a restart: an
/// unclassified section is a gap in the table, and assuming it is inert is
/// exactly the failure this module exists to prevent. The gate catches that gap
/// before it ships; this is the runtime backstop for a document that arrived
/// ahead of the table.
pub fn sections_needing_restart_on_change(old: &Value, new: &Value) -> Vec<String> {
    changed_sections(old, new)
        .into_iter()
        .filter(|name| effect_of(name) != Some(ConfigEffect::HotReloaded))
        .collect()
}

/// Sections present in the document that the table does not classify.
pub fn unclassified_sections(doc: &Value) -> Vec<String> {
    let mut out: Vec<String> = doc
        .as_object()
        .map(|map| {
            map.keys()
                .filter(|name| effect_of(name).is_none())
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// Table entries with no matching section in the document. An entry that
/// outlives its section is a stale claim about a document that no longer has it,
/// and it hides the next real gap.
pub fn sections_missing_from_document(doc: &Value) -> Vec<&'static str> {
    let mut out: Vec<&'static str> = CONFIG_SECTIONS
        .iter()
        .map(|s| s.name)
        .filter(|name| doc.get(name).is_none())
        .collect();
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_hot_reloaded_section_does_not_force_a_restart() {
        let old = json!({"tuning": {"stream_capacity": 8}});
        let new = json!({"tuning": {"stream_capacity": 16}});
        assert_eq!(changed_sections(&old, &new).len(), 1);
        assert!(sections_needing_restart_on_change(&old, &new).is_empty());
    }

    #[test]
    fn a_section_read_at_startup_forces_a_restart_when_it_changes() {
        let old = json!({"observability": {"infra_watch": {"rules": []}}});
        let new = json!({"observability": {"infra_watch": {"rules": [{"name": "x"}]}}});
        assert_eq!(
            sections_needing_restart_on_change(&old, &new),
            vec!["observability".to_string()]
        );
    }

    #[test]
    fn an_unchanged_document_needs_nothing() {
        let doc = json!({"app": {"log_level": "info"}, "tuning": {"stream_capacity": 8}});
        assert!(changed_sections(&doc, &doc).is_empty());
        assert!(sections_needing_restart_on_change(&doc, &doc).is_empty());
    }

    #[test]
    fn a_section_the_table_does_not_know_counts_as_needing_a_restart() {
        let old = json!({});
        let new = json!({"a_section_added_ahead_of_the_table": 1});
        assert_eq!(
            sections_needing_restart_on_change(&old, &new),
            vec!["a_section_added_ahead_of_the_table".to_string()]
        );
        assert_eq!(
            unclassified_sections(&new),
            vec!["a_section_added_ahead_of_the_table".to_string()]
        );
    }

    #[test]
    fn a_removed_section_is_a_change_of_the_old_document() {
        let old = json!({"observability": {"infra_watch": {"enabled": true}}});
        let new = json!({});
        assert_eq!(
            sections_needing_restart_on_change(&old, &new),
            vec!["observability".to_string()]
        );
    }

    /// Both directions of the gate, on a document whose shape is fixed here: the
    /// real document is checked by the workspace gate that reads it.
    #[test]
    fn classification_gaps_are_reported_in_both_directions() {
        let doc = json!({"app": {}, "tuning": {}, "not_in_the_table": {}});
        assert_eq!(
            unclassified_sections(&doc),
            vec!["not_in_the_table".to_string()]
        );
        let missing = sections_missing_from_document(&doc);
        assert!(missing.contains(&"observability"));
        assert!(!missing.contains(&"app"));
    }

    /// Two sections that differ only in a nested field are still a change of the
    /// section: the comparison is per section, not per leaf, so a nested edit
    /// cannot slip past the restart decision.
    #[test]
    fn a_nested_edit_is_a_change_of_its_section() {
        let old = json!({"gateway": {"websocket_timeout_secs": 30, "http_port": 8080}});
        let new = json!({"gateway": {"websocket_timeout_secs": 60, "http_port": 8080}});
        assert_eq!(
            sections_needing_restart_on_change(&old, &new),
            vec!["gateway".to_string()]
        );
    }
}
