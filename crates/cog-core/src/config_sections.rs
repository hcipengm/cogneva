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

/// ConfigMap that carries the configuration document, and the key inside it.
///
/// A delivery path that re-applies this ConfigMap has to roll the workloads that
/// mount it whenever a section that only takes effect at startup has changed:
/// the object moves on, the process keeps the document it read at start, and the
/// delivery is otherwise reported as applied.
pub const CONFIG_CONFIGMAP: &str = "cogneva-json";
pub const CONFIG_DOCUMENT_KEY: &str = "cogneva.json";

/// Where the alert rules live inside the document.
///
/// Named once because more than one reader selects it from a document: the
/// comparison below, and the gate that checks the deployed rule set against
/// what this workspace publishes.
pub const ALERT_RULES_POINTER: &str = "/observability/infra_watch/rules";

/// How the document a process was handed differs from the one its revision
/// declares.
///
/// Compared by section and by rule name rather than by a digest of the whole
/// document: the reading a person has to act on is which rules are not in
/// force, and a digest answers only "different". Comment keys (`_comment*`, at
/// any depth) are left out of the comparison — they carry prose, so a document
/// that differs only in them is in force exactly as declared, and reporting it
/// would spend the credibility of the one alert that must always be believable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigDeclaration {
    /// Declared sections the delivered document does not have at all.
    pub absent: Vec<String>,
    /// Sections both documents have, holding different values.
    pub changed: Vec<String>,
    /// Sections the delivered document has that this revision does not declare.
    pub undeclared: Vec<String>,
    /// Alert rules this revision declares that the delivered document lacks.
    pub rules_absent: Vec<String>,
    /// Alert rules the delivered document carries that this revision does not
    /// declare.
    pub rules_undeclared: Vec<String>,
}

impl ConfigDeclaration {
    /// Whether the delivered document is the one this revision declares.
    pub fn matches(&self) -> bool {
        self.absent.is_empty()
            && self.changed.is_empty()
            && self.undeclared.is_empty()
            && self.rules_absent.is_empty()
            && self.rules_undeclared.is_empty()
    }
}

/// Compare a delivered document against the one this revision declares.
pub fn compare_declaration(delivered: &Value, declared: &Value) -> ConfigDeclaration {
    let delivered = without_comment_keys(delivered);
    let declared = without_comment_keys(declared);
    let mut out = ConfigDeclaration::default();
    for name in declared.as_object().map(|m| m.keys()).into_iter().flatten() {
        match delivered.get(name) {
            None => out.absent.push(name.clone()),
            Some(value) if value != &declared[name] => out.changed.push(name.clone()),
            Some(_) => {}
        }
    }
    for name in delivered
        .as_object()
        .map(|m| m.keys())
        .into_iter()
        .flatten()
    {
        if declared.get(name).is_none() {
            out.undeclared.push(name.clone());
        }
    }
    let delivered_rules = rule_names(&delivered);
    let declared_rules = rule_names(&declared);
    out.rules_absent = declared_rules
        .iter()
        .filter(|n| !delivered_rules.contains(n))
        .cloned()
        .collect();
    out.rules_undeclared = delivered_rules
        .iter()
        .filter(|n| !declared_rules.contains(n))
        .cloned()
        .collect();
    out.absent.sort();
    out.changed.sort();
    out.undeclared.sort();
    out.rules_absent.sort();
    out.rules_undeclared.sort();
    out
}

/// The document with every `_comment*` key removed, at any depth.
fn without_comment_keys(doc: &Value) -> Value {
    match doc {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(key, _)| !key.starts_with("_comment"))
                .map(|(key, value)| (key.clone(), without_comment_keys(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(without_comment_keys).collect()),
        other => other.clone(),
    }
}

/// The alert rule names a document declares, in document order.
fn rule_names(doc: &Value) -> Vec<String> {
    doc.pointer(ALERT_RULES_POINTER)
        .and_then(|rules| rules.as_array())
        .map(|rules| {
            rules
                .iter()
                .filter_map(|rule| rule.get("name").and_then(|n| n.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// What a process was handed, judged against the declaration compiled into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentDelivery {
    /// Nothing at the path the process reads. Not by itself a fault: a process
    /// can be configured entirely from the environment.
    Absent,
    /// Something is at the path and it is not a usable document.
    Unusable(String),
    /// Delivered, usable, and not this revision's document.
    Differs(ConfigDeclaration),
    /// Delivered and this revision's document.
    Matches,
}

/// Judge delivered text against the declared document.
///
/// Pure, and takes the delivered text rather than a path: the caller owns
/// reading the file, so every verdict — nothing there, unparseable, a
/// different revision — can be exercised without a filesystem.
pub fn judge_delivery(delivered: Option<&str>, declared: &str) -> DocumentDelivery {
    let Some(text) = delivered else {
        return DocumentDelivery::Absent;
    };
    let delivered: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(e) => return DocumentDelivery::Unusable(e.to_string()),
    };
    let declared: Value = serde_json::from_str(declared)
        .expect("the declaration compiled into this binary is valid JSON");
    if !delivered.is_object() || !declared.is_object() {
        // Two non-objects compare "equal" against nothing and would read as a
        // match: a document whose shape is wrong is judged unusable instead.
        return DocumentDelivery::Unusable("the document is not a JSON object".to_string());
    }
    let difference = compare_declaration(&delivered, &declared);
    if difference.matches() {
        DocumentDelivery::Matches
    } else {
        DocumentDelivery::Differs(difference)
    }
}

/// Longest message this module composes, in characters.
///
/// Bounded on characters rather than on the number of names: a list budget
/// multiplies by the length of the names, and section or rule names come from
/// the document rather than from this code.
const MAX_MESSAGE_CHARS: usize = 600;

/// Names per list before the rest are counted instead of spelled out.
const MAX_NAMES_PER_LIST: usize = 6;

/// Say what was delivered, for the alert row and the log.
pub fn describe_delivery(source: &str, delivery: &DocumentDelivery) -> String {
    let text = match delivery {
        DocumentDelivery::Matches => {
            format!("configuration document at {source} is the one this revision declares")
        }
        DocumentDelivery::Absent => format!(
            "no configuration document at {source}: this process runs on built-in defaults and \
             environment overrides"
        ),
        DocumentDelivery::Unusable(e) => {
            format!("the configuration document at {source} cannot be used: {e}")
        }
        DocumentDelivery::Differs(d) => {
            let mut parts: Vec<String> = Vec::new();
            push_difference(&mut parts, &d.rules_absent, "declared alert rule(s) absent");
            push_difference(
                &mut parts,
                &d.rules_undeclared,
                "rule(s) it does not declare",
            );
            push_difference(&mut parts, &d.absent, "section(s) absent");
            push_difference(&mut parts, &d.changed, "section(s) changed");
            push_difference(&mut parts, &d.undeclared, "section(s) it does not declare");
            format!(
                "the configuration document this process started with ({source}) is not the one \
                 this revision declares: {}",
                parts.join("; ")
            )
        }
    };
    let mut chars = text.chars();
    let bounded: String = chars.by_ref().take(MAX_MESSAGE_CHARS).collect();
    if chars.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}

/// Add one non-empty list to the message, named and counted.
fn push_difference(parts: &mut Vec<String>, names: &[String], what: &str) {
    if !names.is_empty() {
        parts.push(format!("{} {what} {}", names.len(), join_names(names)));
    }
}

/// Render one list of names, counting whatever does not fit instead of dropping
/// it: a list that silently stops at six reads as if it were complete.
fn join_names(names: &[String]) -> String {
    if names.len() <= MAX_NAMES_PER_LIST {
        return format!("[{}]", names.join(", "));
    }
    let shown = names[..MAX_NAMES_PER_LIST].join(", ");
    format!("[{shown}, +{} more]", names.len() - MAX_NAMES_PER_LIST)
}

/// Stamp that rolls a workload onto the configuration it mounts.
///
/// Wall-clock seconds: two rolls in the same second would write the same value
/// and the second one would not change the pod template at all, so the workload
/// the second change was for would keep running the older document.
pub fn restart_stamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_default()
}

/// Strategic-merge body that rolls one workload: an annotation on the pod
/// template, which is what the workload's own selector hashes.
pub fn restart_patch_body(stamp: &str) -> String {
    format!(
        r#"{{"spec":{{"template":{{"metadata":{{"annotations":{{"cogneva.io/restartedAt":"{stamp}"}}}}}}}}}}"#
    )
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

    /// The roll body has to land the annotation where the pod template is
    /// compared, and the stamp has to be a value the API server accepts.
    #[test]
    fn the_restart_body_annotates_the_pod_template() {
        let body: Value = serde_json::from_str(&restart_patch_body("1700000000")).unwrap();
        assert_eq!(
            body.pointer("/spec/template/metadata/annotations/cogneva.io~1restartedAt")
                .and_then(|v| v.as_str()),
            Some("1700000000")
        );
        assert!(!restart_stamp().is_empty());
    }

    /// A document of the shape the deployment delivers: two sections, one rule.
    fn declared_document() -> Value {
        json!({
            "_comment": "free-form note",
            "app": {"log_level": "info"},
            "self_evolution": {"build_gate": {"slots": 1}},
            "observability": {
                "infra_watch": {
                    "rules": [
                        {"name": "node_disk", "promql": "up == 0"},
                        {"name": "aof_repair_gave_up_bytes", "promql": "x > 0"}
                    ]
                }
            }
        })
    }

    fn delivery_of(delivered: &Value) -> DocumentDelivery {
        judge_delivery(
            Some(&delivered.to_string()),
            &declared_document().to_string(),
        )
    }

    /// The control: a document that is the declaration is not a finding.
    #[test]
    fn the_declared_document_itself_matches() {
        assert_eq!(delivery_of(&declared_document()), DocumentDelivery::Matches);
    }

    /// Prose is not configuration. A document that differs only in comment
    /// keys, at any depth, is in force exactly as declared.
    #[test]
    fn a_comment_only_difference_is_not_a_difference() {
        let mut delivered = declared_document();
        delivered["observability"]["infra_watch"]["_comment_poll"] =
            json!("the delivered copy explains the interval differently");
        delivered["_comment"] = json!("another revision's prose");
        assert_eq!(delivery_of(&delivered), DocumentDelivery::Matches);
    }

    /// The documented incident: the delivered document predates the revision,
    /// so the section the newer code reads is simply not there.
    #[test]
    fn a_section_the_delivered_document_lacks_is_named() {
        let mut delivered = declared_document();
        delivered.as_object_mut().unwrap().remove("self_evolution");
        match delivery_of(&delivered) {
            DocumentDelivery::Differs(d) => {
                assert_eq!(d.absent, vec!["self_evolution".to_string()]);
                assert!(d.changed.is_empty() && d.undeclared.is_empty());
            }
            other => panic!("expected a difference, got {other:?}"),
        }
    }

    /// A nested edit is a change of its section, and the comparison reports the
    /// section rather than staying silent because the top level looks the same.
    #[test]
    fn a_nested_edit_in_a_shared_section_is_reported() {
        let mut delivered = declared_document();
        delivered["app"]["log_level"] = json!("debug");
        match delivery_of(&delivered) {
            DocumentDelivery::Differs(d) => {
                assert_eq!(d.changed, vec!["app".to_string()]);
                assert!(d.absent.is_empty());
            }
            other => panic!("expected a difference, got {other:?}"),
        }
    }

    #[test]
    fn a_section_the_revision_does_not_declare_is_reported() {
        let mut delivered = declared_document();
        delivered["a_section_added_ahead_of_this_revision"] = json!({"x": 1});
        match delivery_of(&delivered) {
            DocumentDelivery::Differs(d) => {
                assert_eq!(
                    d.undeclared,
                    vec!["a_section_added_ahead_of_this_revision".to_string()]
                );
            }
            other => panic!("expected a difference, got {other:?}"),
        }
    }

    /// A rule the newer revision declares and the delivered document does not
    /// have: the rule set in force is smaller than the one this code believes
    /// it shipped, which is silent everywhere else.
    #[test]
    fn a_declared_rule_the_delivered_document_lacks_is_named() {
        let mut delivered = declared_document();
        delivered["observability"]["infra_watch"]["rules"] = json!([{"name": "node_disk"}]);
        match delivery_of(&delivered) {
            DocumentDelivery::Differs(d) => {
                assert_eq!(d.rules_absent, vec!["aof_repair_gave_up_bytes".to_string()]);
                assert!(d.changed.contains(&"observability".to_string()));
            }
            other => panic!("expected a difference, got {other:?}"),
        }
    }

    #[test]
    fn a_rule_the_revision_does_not_declare_is_named() {
        let mut delivered = declared_document();
        delivered["observability"]["infra_watch"]["rules"] = json!([
            {"name": "node_disk"},
            {"name": "aof_repair_gave_up_bytes"},
            {"name": "left_behind_by_a_rollback"}
        ]);
        match delivery_of(&delivered) {
            DocumentDelivery::Differs(d) => {
                assert_eq!(
                    d.rules_undeclared,
                    vec!["left_behind_by_a_rollback".to_string()]
                );
                assert!(d.rules_absent.is_empty());
            }
            other => panic!("expected a difference, got {other:?}"),
        }
    }

    /// Each of the three ways a document can fail to be usable has its own
    /// verdict: reading them as one would make "nothing there" and "something
    /// broken there" the same report.
    #[test]
    fn nothing_delivered_and_unusable_text_are_told_apart() {
        let declared = declared_document().to_string();
        assert_eq!(judge_delivery(None, &declared), DocumentDelivery::Absent);
        assert!(matches!(
            judge_delivery(Some("{not json"), &declared),
            DocumentDelivery::Unusable(_)
        ));
        // Valid JSON of the wrong shape compares equal to nothing, so it must
        // not be allowed to read as a match.
        assert!(matches!(
            judge_delivery(Some("[]"), &declared),
            DocumentDelivery::Unusable(_)
        ));
    }

    #[test]
    fn the_message_names_the_source_and_what_is_missing() {
        let source = "/etc/cogneva/cogneva.json";
        let mut delivered = declared_document();
        delivered["observability"]["infra_watch"]["rules"] = json!([{"name": "node_disk"}]);
        delivered.as_object_mut().unwrap().remove("self_evolution");
        let text = describe_delivery(source, &delivery_of(&delivered));
        assert!(text.contains(source), "{text}");
        assert!(text.contains("aof_repair_gave_up_bytes"), "{text}");
        assert!(text.contains("self_evolution"), "{text}");
    }

    #[test]
    fn the_message_matches_the_delivered_document_wording() {
        let source = "/etc/cogneva/cogneva.json";
        assert!(describe_delivery(source, &DocumentDelivery::Matches).contains(source));
        assert!(describe_delivery(source, &DocumentDelivery::Absent).contains("defaults"));
        assert!(
            describe_delivery(source, &DocumentDelivery::Unusable("bad".into())).contains("bad")
        );
    }

    /// A long list is truncated by characters and says how much it left out:
    /// a message that stops without a count reads as if it were complete.
    #[test]
    fn the_message_stays_bounded_when_the_lists_are_long() {
        let mut delivered = declared_document();
        delivered["observability"]["infra_watch"]["rules"] = json!([]);
        let mut declared = declared_document();
        let rules: Vec<Value> = (0..200)
            .map(|i| json!({"name": format!("rule_{i}_with_a_name_of_its_own")}))
            .collect();
        declared["observability"]["infra_watch"]["rules"] = json!(rules);
        let delivery = judge_delivery(Some(&delivered.to_string()), &declared.to_string());
        let text = describe_delivery("/etc/cogneva/cogneva.json", &delivery);
        assert!(text.chars().count() <= MAX_MESSAGE_CHARS + 1, "{text}");
        assert!(text.contains("more"), "{text}");
    }
}
