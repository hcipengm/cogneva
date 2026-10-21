//! principle: feedback_a_row_that_outlives_its_process_cannot_be_closed_only_by_the_one_that_opened_it
//! Is the document this process runs the one its revision declares?
//!
//! The alert rules live in the configuration document, so the delivery of that
//! document has no rule that can report its own failure: a rule saying "these
//! rules are not in force" would travel in the very document that did not
//! arrive. Everywhere else the watcher's self-alert (`infra_watch_eval_failure`)
//! covers "the rule is there and cannot be evaluated"; this module covers "the
//! rule is not there at all", which otherwise looks identical to a rule that is
//! quiet because nothing is wrong.
//!
//! The judgement is made here rather than from a rule for the same reason the
//! reference document is compiled into the binary: both sides have to be
//! readable when the configuration is not.
//!
//! What this reports is a fact about *this process*: the document it read at
//! start, against the one its own revision declares. A pod that starts while the
//! ConfigMap is being applied reads the previous document, and the second read
//! after the settle window tells that case apart from a document that is
//! genuinely behind — by then the file has moved on, and the deployment's own
//! reload path (or the roll a configuration change triggers) corrects the
//! process without anyone being woken.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::config_sections::{self, DocumentDelivery};
use cog_core::{AlertEvent, AlertInstance, AlertState};
use tracing::{info, warn};

use crate::alert_store::{NewAlert, PostgresAlertStore};
use crate::alerts::AlertManager;

/// Rule name for the delivery self-alert. Kept out of the configured rule set:
/// a config rule with this name would make the watcher adopt and resolve the
/// row, and the one row that reports a missing rule set would flap against the
/// rules that are missing.
pub const CONFIG_DECLARATION_RULE: &str = "config_declaration_mismatch";

/// What the check does to the alert row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryAction {
    /// Raise (or keep) the row with this message.
    Fire(String),
    /// Close the row: this process is on the document its revision declares.
    Resolve,
    /// Leave the row alone, and say in the log why nothing was concluded.
    Silent(String),
}

/// Decide from the two readings.
///
/// Pure, so every combination — including the rollout window, which needs a
/// clock to reproduce — can be exercised without sleeping.
pub fn decide(first: &DocumentDelivery, later: &DocumentDelivery, source: &str) -> DeliveryAction {
    match first {
        // The process is on its revision's document. Nothing to raise, and any
        // row left behind by a process that was not is closed here.
        DocumentDelivery::Matches => DeliveryAction::Resolve,
        // Nothing at the path. Deliberate for a process configured entirely
        // from the environment, so it is logged rather than alerted; the
        // loader's own fallback to defaults is loud in the same log.
        DocumentDelivery::Absent => {
            DeliveryAction::Silent(config_sections::describe_delivery(source, first))
        }
        // Something is there and cannot be used: the process is running on
        // defaults with a document mounted, which is never what the deployment
        // asked for.
        DocumentDelivery::Unusable(_) => {
            DeliveryAction::Fire(config_sections::describe_delivery(source, first))
        }
        DocumentDelivery::Differs(_) => match later {
            // The file has moved on since this process read it. The process is
            // one rollout behind rather than sitting on a delivery that never
            // arrived, so it is reported to the log and left to the reload path.
            DocumentDelivery::Matches => DeliveryAction::Silent(format!(
                "{}: the file has since been replaced, so this process is a rollout behind rather \
                 than stuck",
                config_sections::describe_delivery(source, first)
            )),
            // Still not this revision's document. This is the delivery that did
            // not happen, and it is silent everywhere else.
            _ => DeliveryAction::Fire(config_sections::describe_delivery(source, first)),
        },
    }
}

/// Read the document, wait out the settle window, read it again, and act.
pub async fn run_config_declaration_check(
    settle: Duration,
    store: Option<Arc<PostgresAlertStore>>,
    notifier: Option<Arc<AlertManager>>,
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
) {
    let path = crate::config::config_path();
    let source = path.display().to_string();
    let first = judge(&path);
    if settle > Duration::ZERO {
        tokio::time::sleep(settle).await;
    }
    let later = judge(&path);
    match decide(&first, &later, &source) {
        DeliveryAction::Fire(message) => {
            warn!(source = %source, "{}", message);
            set_row(
                &store,
                &notifier,
                metrics.as_ref(),
                CONFIG_DECLARATION_RULE,
                true,
                &source,
                &message,
            )
            .await;
        }
        DeliveryAction::Resolve => {
            info!(
                source = %source,
                "{}",
                config_sections::describe_delivery(&source, &first)
            );
            set_row(
                &store,
                &notifier,
                metrics.as_ref(),
                CONFIG_DECLARATION_RULE,
                false,
                &source,
                "",
            )
            .await;
        }
        DeliveryAction::Silent(reason) => {
            info!(source = %source, "config declaration check concluded nothing: {reason}");
        }
    }
}

/// Rule name for the loaded-rule-set self-alert. Like
/// [`CONFIG_DECLARATION_RULE`] it stays out of the configured set: the row says
/// the configured set in this process is not the one on disk, so a rule inside
/// that set cannot be the one raising or resolving it.
pub const RULES_NOT_LOADED_RULE: &str = "config_rules_not_loaded";

/// The phrase the governance face table uses for "the rules this process holds
/// are the ones the ConfigMap declares".
///
/// It is published as a `readout` label, not as a metric name, because that is
/// how the face reader looks an owed reading up: the face table records what a
/// reading *says*, and the reader tries the phrase both as a series name and as
/// a `readout` value. A phrase that only exists as prose in the table resolves
/// to nothing, and "nothing" prints the same as "not measured".
pub const RULE_SET_READOUT: &str = "rule_loaded_vs_configmap";

/// What the rule set on disk and the rule set in this process say to each other.
///
/// The two are different readings of the same configuration: the document is
/// re-read from the file on every check, the rules were read once when the
/// plugin started. The observability section is `RestartRequired`, so a
/// ConfigMap replaced under a running process changes one reading and not the
/// other, and nothing else in the process notices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadedRuleSet {
    /// Neither side could be read. Nothing is concluded and no row is touched.
    Unknown(String),
    /// The process holds exactly the names the document declares.
    Agree {
        /// How many rules the document declares. Carried on this arm too: the
        /// published count is a reading, and the agreeing state is the one a
        /// reader sees on an ordinary day -- an arm without the number would
        /// leave the metric unwritten exactly when someone asks "how many".
        declared: usize,
    },
    /// The document declares names this process did not load, or still
    /// declares names it no longer has.
    Differs {
        /// Declared on disk, absent from this process: a delivered rule that is
        /// inert until the deployment rolls.
        not_loaded: Vec<String>,
        /// Held by this process, no longer declared: a rule still evaluated
        /// after the document dropped it.
        still_loaded: Vec<String>,
        /// How many rules the document declares, for the message.
        declared: usize,
    },
}

/// Compare the names this process loaded with the ones the document declares.
pub fn judge_loaded_rule_set(loaded: &[String], document: Option<&str>) -> LoadedRuleSet {
    let Some(text) = document else {
        return LoadedRuleSet::Unknown("no document at the configured path".into());
    };
    let parsed: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(e) => return LoadedRuleSet::Unknown(format!("document did not parse: {e}")),
    };
    let Some(rules) = parsed
        .pointer("/observability/infra_watch/rules")
        .and_then(|rules| rules.as_array())
    else {
        return LoadedRuleSet::Unknown("document declares no rule array".into());
    };
    let declared: Vec<&str> = rules
        .iter()
        .filter_map(|rule| rule.get("name").and_then(|name| name.as_str()))
        .collect();
    let loaded_set: std::collections::HashSet<&str> =
        loaded.iter().map(std::string::String::as_str).collect();
    let declared_set: std::collections::HashSet<&str> = declared.iter().copied().collect();
    let mut not_loaded: Vec<String> = declared_set
        .difference(&loaded_set)
        .map(|name| (*name).to_string())
        .collect();
    let mut still_loaded: Vec<String> = loaded_set
        .difference(&declared_set)
        .map(|name| (*name).to_string())
        .collect();
    not_loaded.sort();
    still_loaded.sort();
    if not_loaded.is_empty() && still_loaded.is_empty() {
        LoadedRuleSet::Agree {
            declared: declared.len(),
        }
    } else {
        LoadedRuleSet::Differs {
            not_loaded,
            still_loaded,
            declared: declared.len(),
        }
    }
}

/// What one check does to the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleSetAction {
    Fire(String),
    Resolve,
    Nothing,
}

/// Streak carried between checks: one tick of disagreement is the document
/// landing under a rollout that is already on its way, which needs no row.
#[derive(Debug, Default)]
pub struct LoadedRuleSetWatch {
    disagreeing: u32,
    firing: bool,
}

/// Fold one verdict into the streak and decide what to do about the row.
pub fn rule_set_action(verdict: &LoadedRuleSet, watch: &mut LoadedRuleSetWatch) -> RuleSetAction {
    match verdict {
        // A read that failed is not a reading that says the sets agree: leave
        // the row where it is and say why nothing was concluded.
        LoadedRuleSet::Unknown(_) => RuleSetAction::Nothing,
        LoadedRuleSet::Agree { .. } => {
            watch.disagreeing = 0;
            watch.firing = false;
            // Asked for on every agreeing tick, not only after this process
            // raised the row. The row is keyed by the document, so it outlives
            // the process that opened it: a deployment that rolls in the fix
            // replaces the process, and the one that starts has no memory of
            // the row it inherited -- a resolve gated on this process's own
            // `firing` would leave that row open forever. Closing an already
            // closed row writes nothing, so the repeated ask costs one read.
            RuleSetAction::Resolve
        }
        LoadedRuleSet::Differs {
            not_loaded,
            still_loaded,
            declared,
        } => {
            watch.disagreeing = watch.disagreeing.saturating_add(1);
            if watch.firing || watch.disagreeing < 2 {
                RuleSetAction::Nothing
            } else {
                watch.firing = true;
                let mut message = format!(
                    "this process loaded a rule set the configuration document does not describe: \
                     the document declares {declared} rule(s)"
                );
                if !not_loaded.is_empty() {
                    message.push_str(&format!(
                        "; {} declared there {} not loaded ({}), so {} inert until the deployment rolls",
                        not_loaded.len(),
                        if not_loaded.len() == 1 { "is" } else { "are" },
                        not_loaded.join(", "),
                        if not_loaded.len() == 1 { "it is" } else { "they are" },
                    ));
                }
                if !still_loaded.is_empty() {
                    message.push_str(&format!(
                        "; {} loaded here {} no longer declared ({}), still being evaluated",
                        still_loaded.len(),
                        if still_loaded.len() == 1 { "is" } else { "are" },
                        still_loaded.join(", "),
                    ));
                }
                message.push_str(
                    ". The observability section is read once at startup, so replacing the \
                     document does not load what it declares: the deployment has to roll",
                );
                RuleSetAction::Fire(message)
            }
        }
    }
}

/// Re-read the document and compare its rule set with the one this process
/// holds. Called once per watcher tick.
pub async fn check_loaded_rule_set(
    watch: &mut LoadedRuleSetWatch,
    loaded: &[String],
    store: &Option<Arc<PostgresAlertStore>>,
    notifier: &Option<Arc<AlertManager>>,
    metrics: Option<&Arc<dyn cog_core::MetricsBackend>>,
) {
    let path = crate::config::config_path();
    let source = path.display().to_string();
    let document = match std::fs::read_to_string(&path) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            // An unreadable document is not an empty one; the row stays as it
            // was and the reason is logged. The counts are still published:
            // `loaded` is known without the document, and the cell saying the
            // document could not be read is the whole point of the reading on
            // exactly this tick.
            info!(source = %source, error = %e, "loaded rule set check read nothing");
            let verdict = LoadedRuleSet::Unknown(format!("document did not read: {e}"));
            record_rule_counts(metrics, &verdict, loaded.len()).await;
            return;
        }
    };
    let was_firing = watch.firing;
    let verdict = judge_loaded_rule_set(loaded, document.as_deref());
    record_rule_counts(metrics, &verdict, loaded.len()).await;
    match rule_set_action(&verdict, watch) {
        RuleSetAction::Fire(message) => {
            warn!(source = %source, "{}", message);
            set_row(
                store,
                notifier,
                metrics,
                RULES_NOT_LOADED_RULE,
                true,
                &source,
                &message,
            )
            .await;
        }
        RuleSetAction::Resolve => {
            if was_firing {
                info!(source = %source, "the loaded rule set matches the document again");
            }
            set_row(
                store,
                notifier,
                metrics,
                RULES_NOT_LOADED_RULE,
                false,
                &source,
                "",
            )
            .await;
        }
        RuleSetAction::Nothing => {}
    }
}

/// Publish the two sizes the check compares, whether it could read the document
/// at all, and whether the two sides agree.
///
/// One gauge rather than four names: the cells are the same question asked of
/// four subjects, and a reader subtracting them has to see them side by side.
/// `declared` is omitted when the document did not parse -- a count that was
/// never taken must not be written as zero, so the unreadable case says so in
/// `document_readable` instead of claiming the document declares no rules.
///
/// The `agrees` cell exists because the subtraction itself fails on the tick
/// that matters: with `declared` absent a reader cannot tell "the document says
/// nothing" from "the document says something else". It also carries
/// [`RULE_SET_READOUT`] as a `readout` label, which is the name the face owns
/// this reading under; the other cells keep `state` alone so a panel can still
/// group them without naming a phrase.
async fn record_rule_counts(
    metrics: Option<&Arc<dyn cog_core::MetricsBackend>>,
    verdict: &LoadedRuleSet,
    loaded: usize,
) {
    let Some(backend) = metrics else { return };
    let declared = match verdict {
        LoadedRuleSet::Unknown(_) => None,
        LoadedRuleSet::Agree { declared } | LoadedRuleSet::Differs { declared, .. } => {
            Some(*declared)
        }
    };
    let mut cells = vec![
        ("loaded", loaded as f64),
        (
            "document_readable",
            if declared.is_some() { 1.0 } else { 0.0 },
        ),
    ];
    if let Some(declared) = declared {
        cells.push(("declared", declared as f64));
    }
    for (state, value) in cells {
        let labels = std::collections::HashMap::from([("state".to_string(), state.to_string())]);
        if let Err(e) = backend
            .record_gauge(cog_core::metric_names::ALERT_RULE_COUNT, value, labels)
            .await
        {
            warn!(state, error = %e, "could not record the alert rule count");
        }
    }
    // A document that did not read is not an agreement: the verdict cell is 0
    // for `Unknown` and for `Differs` alike, so a reader that only holds this
    // one cell still never reads "fine" out of an unmeasured comparison.
    let agrees = matches!(verdict, LoadedRuleSet::Agree { .. });
    let labels = std::collections::HashMap::from([
        ("state".to_string(), "agrees".to_string()),
        ("readout".to_string(), RULE_SET_READOUT.to_string()),
    ]);
    if let Err(e) = backend
        .record_gauge(
            cog_core::metric_names::ALERT_RULE_COUNT,
            if agrees { 1.0 } else { 0.0 },
            labels,
        )
        .await
    {
        warn!(state = "agrees", error = %e, "could not record the loaded rule set verdict");
    }
}

/// Read the document this process would read at startup, and judge it.
fn judge(path: &std::path::Path) -> DocumentDelivery {
    let delivered = match std::fs::read_to_string(path) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            return DocumentDelivery::Unusable(format!("{}: {e}", path.display()));
        }
    };
    config_sections::judge_delivery(delivered.as_deref(), crate::config::DECLARED_CONFIG_JSON)
}

/// Raise or close the one row this check owns.
///
/// One row for the whole deployment, keyed by the document's path: every
/// workload that mounts it reads the same file, so a per-process key would
/// multiply one fact by the number of replicas and let them resolve each
/// other's rows.
async fn set_row(
    store: &Option<Arc<PostgresAlertStore>>,
    notifier: &Option<Arc<AlertManager>>,
    metrics: Option<&Arc<dyn cog_core::MetricsBackend>>,
    rule: &str,
    firing: bool,
    source: &str,
    message: &str,
) {
    let Some(store) = store else {
        return;
    };
    let alert = NewAlert {
        rule: rule.to_string(),
        dedup_key: format!("{rule}:{source}"),
        severity: if firing { "warning" } else { "info" }.to_string(),
        message: message.to_string(),
        labels: serde_json::json!({ "source": source }),
    };
    match store.set_alert(firing, &alert).await {
        Ok(_) => {
            // Only the firing edge interrupts anyone: a row closing is visible
            // on the surface people already read.
            if firing {
                let now = Utc::now();
                let labels = HashMap::from([
                    ("source".to_string(), source.to_string()),
                    ("message".to_string(), message.to_string()),
                ]);
                let events = [AlertEvent::Firing(AlertInstance {
                    rule_name: rule.to_string(),
                    labels,
                    state: AlertState::Firing,
                    severity: cog_core::AlertSeverity::Warning,
                    value: 1.0,
                    starts_at: now,
                    ends_at: None,
                    updated_at: now,
                })];
                match notifier.as_ref() {
                    Some(notifier) => notifier.notify(&events).await,
                    None => crate::alerts::record_undelivered(metrics, 1).await,
                }
            }
        }
        Err(e) => warn!(source = %source, error = %e, "config declaration alert persist failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::config_sections::ConfigDeclaration;

    const SOURCE: &str = "/etc/cogneva/cogneva.json";

    fn differs() -> DocumentDelivery {
        DocumentDelivery::Differs(ConfigDeclaration {
            rules_absent: vec!["aof_repair_verdict_absent".to_string()],
            changed: vec!["observability".to_string()],
            ..ConfigDeclaration::default()
        })
    }

    /// The deployment in transition: the process read the previous document and
    /// the file has since moved on. Nothing is raised, and the reason says which
    /// case this was rather than staying quiet.
    #[test]
    fn a_rollout_the_file_has_moved_on_from_is_not_an_alert() {
        match decide(&differs(), &DocumentDelivery::Matches, SOURCE) {
            DeliveryAction::Silent(reason) => {
                assert!(reason.contains("rollout"), "{reason}");
                assert!(reason.contains("aof_repair_verdict_absent"), "{reason}");
            }
            other => panic!("expected silence, got {other:?}"),
        }
    }

    /// The delivery that did not happen: both reads see the same lagging
    /// document.
    #[test]
    fn a_document_that_is_still_behind_raises_the_row() {
        match decide(&differs(), &differs(), SOURCE) {
            DeliveryAction::Fire(message) => {
                assert!(message.contains(SOURCE), "{message}");
                assert!(message.contains("aof_repair_verdict_absent"), "{message}");
            }
            other => panic!("expected an alert, got {other:?}"),
        }
    }

    /// A document that vanished or broke between the two reads is still a
    /// deployment in the wrong state, not a rollout to wait out.
    #[test]
    fn a_document_that_vanished_between_the_reads_still_raises() {
        assert!(matches!(
            decide(&differs(), &DocumentDelivery::Absent, SOURCE),
            DeliveryAction::Fire(_)
        ));
        assert!(matches!(
            decide(
                &differs(),
                &DocumentDelivery::Unusable("bad".into()),
                SOURCE
            ),
            DeliveryAction::Fire(_)
        ));
    }

    /// This process is on its revision's document: whatever row another process
    /// left behind is closed here.
    #[test]
    fn a_matching_document_resolves_the_row() {
        assert_eq!(
            decide(
                &DocumentDelivery::Matches,
                &DocumentDelivery::Matches,
                SOURCE
            ),
            DeliveryAction::Resolve
        );
    }

    /// No document at all is not alerted: a process configured from the
    /// environment alone is a supported way to run.
    #[test]
    fn no_document_is_logged_not_alerted() {
        match decide(&DocumentDelivery::Absent, &DocumentDelivery::Absent, SOURCE) {
            DeliveryAction::Silent(reason) => assert!(reason.contains(SOURCE), "{reason}"),
            other => panic!("expected silence, got {other:?}"),
        }
    }

    /// A document that is there and unusable is alerted: the process runs on
    /// defaults while a document is mounted.
    #[test]
    fn an_unusable_document_raises() {
        match decide(
            &DocumentDelivery::Unusable("expected value".into()),
            &DocumentDelivery::Unusable("expected value".into()),
            SOURCE,
        ) {
            DeliveryAction::Fire(message) => {
                assert!(message.contains("expected value"), "{message}")
            }
            other => panic!("expected an alert, got {other:?}"),
        }
    }

    /// The row this check owns must not be a name the configured rule set can
    /// contain, or the watcher would adopt and resolve it against the rules it
    /// is reporting as missing.
    #[test]
    fn the_rule_name_is_not_a_configured_rule_name() {
        let declared: serde_json::Value =
            serde_json::from_str(crate::config::DECLARED_CONFIG_JSON).unwrap();
        let names: Vec<String> = declared
            .pointer(cog_core::config_sections::ALERT_RULES_POINTER)
            .and_then(|rules| rules.as_array())
            .expect("the declared document carries a rule list")
            .iter()
            .filter_map(|rule| rule.get("name").and_then(|n| n.as_str()))
            .map(str::to_string)
            .collect();
        for self_alert in [CONFIG_DECLARATION_RULE, RULES_NOT_LOADED_RULE] {
            assert!(
                !names.iter().any(|n| n == self_alert),
                "{self_alert} is a configured rule: {}",
                names.join(", ")
            );
        }
    }

    /// The check judges the file it is pointed at, and tells a missing file
    /// apart from one it cannot use: the two lead to different actions, so a
    /// reading that collapsed them would alert on a process that is configured
    /// from the environment alone.
    #[test]
    fn the_check_reads_the_path_it_is_pointed_at() {
        let dir = std::env::temp_dir().join(format!("cog-config-delivery-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("not-delivered.json");
        let _ = std::fs::remove_file(&missing);
        assert_eq!(judge(&missing), DocumentDelivery::Absent);

        let broken = dir.join("broken.json");
        std::fs::write(&broken, "{\"observability\": ").unwrap();
        assert!(matches!(judge(&broken), DocumentDelivery::Unusable(_)));

        let other = dir.join("other-revision.json");
        std::fs::write(&other, "{\"app\": {\"log_level\": \"debug\"}}").unwrap();
        assert!(matches!(judge(&other), DocumentDelivery::Differs(_)));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The document this binary carries has to be one the running code can act
    /// on: it parses, and every section in it is classified by the table the
    /// delivery path uses. A declaration that fails either is a build defect,
    /// and it would make the check judge everything as a difference.
    #[test]
    fn the_declaration_this_binary_carries_is_usable() {
        let declared: serde_json::Value =
            serde_json::from_str(crate::config::DECLARED_CONFIG_JSON).unwrap();
        assert!(declared.is_object());
        assert_eq!(
            cog_core::config_sections::unclassified_sections(&declared),
            Vec::<String>::new()
        );
        assert_eq!(
            cog_core::config_sections::sections_missing_from_document(&declared),
            Vec::<&'static str>::new()
        );
        assert_eq!(
            config_sections::judge_delivery(
                Some(crate::config::DECLARED_CONFIG_JSON),
                crate::config::DECLARED_CONFIG_JSON
            ),
            DocumentDelivery::Matches
        );
    }

    /// The declaration has to declare the rules the deployed document
    /// carries: the mismatch that fired was a declaration lagging its own
    /// rule set, and a declaration without these names raises it again on
    /// every start.
    #[test]
    fn the_declaration_declares_the_mainline_ci_verdict_rules() {
        let declared: serde_json::Value =
            serde_json::from_str(crate::config::DECLARED_CONFIG_JSON).unwrap();
        let names: Vec<&str> = declared
            .pointer(cog_core::config_sections::ALERT_RULES_POINTER)
            .and_then(|rules| rules.as_array())
            .expect("the declared document carries a rule list")
            .iter()
            .filter_map(|rule| rule.get("name").and_then(|n| n.as_str()))
            .collect();
        for rule in [
            "mainline_ci_verdict_failed",
            "mainline_ci_verdict_unreadable",
        ] {
            assert!(names.contains(&rule), "declared rules lack {rule}");
        }
    }

    /// The observability section the declaration ships has to be one the
    /// running code loads: a section whose shape the schema no longer accepts
    /// reads as a permanent mismatch against the document every deployment
    /// renders from the same source.
    #[test]
    fn the_declared_observability_section_loads() {
        let declared: serde_json::Value =
            serde_json::from_str(crate::config::DECLARED_CONFIG_JSON).unwrap();
        let section = declared.get("observability").cloned().unwrap_or_default();
        serde_json::from_value::<crate::config::ObservabilityExportersConfig>(section)
            .expect("the declared observability section loads");
    }

    fn loaded(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn document(names: &[&str]) -> String {
        serde_json::json!({
            "observability": { "infra_watch": { "rules": names
                .iter()
                .map(|name| serde_json::json!({ "name": name }))
                .collect::<Vec<_>>() } }
        })
        .to_string()
    }

    /// The ordinary case: the process holds what the document declares, so it
    /// raises nothing — it asks for the row to be closed, which is a no-op when
    /// no row is open.
    #[test]
    fn a_process_on_the_documents_rule_set_raises_nothing() {
        let verdict = judge_loaded_rule_set(&loaded(&["a", "b"]), Some(&document(&["a", "b"])));
        assert_eq!(verdict, LoadedRuleSet::Agree { declared: 2 });
        let mut watch = LoadedRuleSetWatch::default();
        assert_eq!(
            rule_set_action(&verdict, &mut watch),
            RuleSetAction::Resolve
        );
    }

    /// A process that starts while the row from a previous process is open
    /// closes it on its first agreeing tick, even though it never raised it.
    ///
    /// The row is keyed by the document rather than by the process, so the
    /// deployment that rolls in the fix is not the deployment that reported the
    /// problem: gating the close on this process's own `firing` flag would leave
    /// the row open for as long as the document stays fixed, which is to say
    /// forever.
    #[test]
    fn a_restarted_process_closes_a_row_it_did_not_raise() {
        let agrees = judge_loaded_rule_set(&loaded(&["a", "b"]), Some(&document(&["a", "b"])));
        let mut watch = LoadedRuleSetWatch::default();
        assert!(!watch.firing, "a fresh process holds no row");
        assert_eq!(rule_set_action(&agrees, &mut watch), RuleSetAction::Resolve);
    }

    /// The document was replaced under a running process: the names it added
    /// are inert, and the row says which ones.
    #[test]
    fn a_rule_delivered_to_a_running_process_is_reported_by_name() {
        let verdict = judge_loaded_rule_set(&loaded(&["a"]), Some(&document(&["a", "b"])));
        let mut watch = LoadedRuleSetWatch::default();
        // One tick is the document landing under a roll that is already on its
        // way.
        assert_eq!(
            rule_set_action(&verdict, &mut watch),
            RuleSetAction::Nothing
        );
        match rule_set_action(&verdict, &mut watch) {
            RuleSetAction::Fire(message) => {
                assert!(message.contains("b"), "{message}");
                assert!(!message.contains("(a)"), "{message}");
            }
            other => panic!("expected a row, got {other:?}"),
        }
        // Stays firing across further disagreeing ticks without re-raising.
        assert_eq!(
            rule_set_action(&verdict, &mut watch),
            RuleSetAction::Nothing
        );
    }

    /// A rule the document dropped is still being evaluated here, which is the
    /// same divergence from the other side.
    #[test]
    fn a_rule_still_evaluated_after_the_document_dropped_it_is_reported() {
        let verdict = judge_loaded_rule_set(&loaded(&["a", "b"]), Some(&document(&["a"])));
        let mut watch = LoadedRuleSetWatch::default();
        assert_eq!(
            rule_set_action(&verdict, &mut watch),
            RuleSetAction::Nothing
        );
        match rule_set_action(&verdict, &mut watch) {
            RuleSetAction::Fire(message) => assert!(message.contains("b"), "{message}"),
            other => panic!("expected a row, got {other:?}"),
        }
    }

    /// The row closes when the two readings agree again (the deployment rolled).
    #[test]
    fn the_row_closes_when_the_process_catches_up() {
        let differs = judge_loaded_rule_set(&loaded(&["a"]), Some(&document(&["a", "b"])));
        let mut watch = LoadedRuleSetWatch::default();
        rule_set_action(&differs, &mut watch);
        rule_set_action(&differs, &mut watch);
        let agrees = judge_loaded_rule_set(&loaded(&["a", "b"]), Some(&document(&["a", "b"])));
        assert_eq!(rule_set_action(&agrees, &mut watch), RuleSetAction::Resolve);
        // Further agreeing ticks keep asking; the store turns each into a
        // no-op once the row is closed.
        assert_eq!(rule_set_action(&agrees, &mut watch), RuleSetAction::Resolve);
    }

    /// A document that cannot be read is not a document that agrees: an open
    /// row is left alone rather than closed by a failed read.
    #[test]
    fn an_unreadable_document_does_not_close_an_open_row() {
        let differs = judge_loaded_rule_set(&loaded(&["a"]), Some(&document(&["a", "b"])));
        let mut watch = LoadedRuleSetWatch::default();
        rule_set_action(&differs, &mut watch);
        rule_set_action(&differs, &mut watch);
        let unknown = judge_loaded_rule_set(&loaded(&["a"]), None);
        assert!(matches!(unknown, LoadedRuleSet::Unknown(_)));
        assert_eq!(
            rule_set_action(&unknown, &mut watch),
            RuleSetAction::Nothing
        );
        assert!(watch.firing);
    }

    /// One gauge write: the series' wire name, its value, and its labels.
    type Write = (String, f64, std::collections::HashMap<String, String>);

    /// Keeps the gauge writes, so a test reads what the check published rather
    /// than trusting that it called something.
    #[derive(Default)]
    struct GaugeWrites {
        calls: std::sync::Mutex<Vec<Write>>,
    }

    impl GaugeWrites {
        /// `(state, value)` per write, asserting each names the rule-count
        /// series: a test that accepted any name would pass on a typo.
        fn states(&self) -> Vec<(String, f64)> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(name, value, labels)| {
                    assert_eq!(name, cog_core::metric_names::ALERT_RULE_COUNT.as_str());
                    (labels.get("state").cloned().unwrap_or_default(), *value)
                })
                .collect()
        }

        /// Every `readout` value written, in order. Cells without the label are
        /// skipped: the other three are addressed by `state`, and only the
        /// verdict cell is also owed to a face by name.
        fn readouts(&self) -> Vec<String> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(_, _, labels)| labels.get("readout").cloned())
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for GaugeWrites {
        async fn record_gauge(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.calls
                .lock()
                .unwrap()
                .push((name.as_str().to_string(), value, labels));
            Ok(())
        }

        async fn record_counter(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn record_histogram(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_gauge_latest(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_counter_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }

        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }

        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> cog_core::SFResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn health_check(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// The agreeing state is the one a reader meets on an ordinary day, so it
    /// has to carry both numbers: a reading that only exists while something is
    /// wrong would leave "how many rules are loaded" unanswerable exactly when
    /// someone asks.
    #[tokio::test]
    async fn the_rule_counts_are_published_when_the_two_sides_agree() {
        let writes = std::sync::Arc::new(GaugeWrites::default());
        let backend: std::sync::Arc<dyn cog_core::MetricsBackend> = writes.clone();
        let verdict = judge_loaded_rule_set(&loaded(&["a", "b"]), Some(&document(&["a", "b"])));
        record_rule_counts(Some(&backend), &verdict, 2).await;
        assert_eq!(
            writes.states(),
            vec![
                ("loaded".to_string(), 2.0),
                ("document_readable".to_string(), 1.0),
                ("declared".to_string(), 2.0),
                ("agrees".to_string(), 1.0),
            ]
        );
    }

    /// Two sides of the same size are not the same set: the names can differ
    /// while both counts read N, so the verdict cell has to be published rather
    /// than derived from the counts by whoever reads them.
    #[tokio::test]
    async fn a_disagreeing_rule_set_publishes_zero() {
        let writes = std::sync::Arc::new(GaugeWrites::default());
        let backend: std::sync::Arc<dyn cog_core::MetricsBackend> = writes.clone();
        let verdict = judge_loaded_rule_set(&loaded(&["a"]), Some(&document(&["a", "b"])));
        record_rule_counts(Some(&backend), &verdict, 1).await;
        assert_eq!(
            writes.states(),
            vec![
                ("loaded".to_string(), 1.0),
                ("document_readable".to_string(), 1.0),
                ("declared".to_string(), 2.0),
                ("agrees".to_string(), 0.0),
            ]
        );
    }

    /// The face table names this reading by a phrase, and the reader looks a
    /// phrase up as a `readout` value. A cell that only carries `state` is
    /// published and unaddressable at the same time: the face prints `absent`
    /// beside a series that is sitting right there.
    #[tokio::test]
    async fn the_verdict_cell_carries_the_phrase_the_face_looks_up() {
        let writes = std::sync::Arc::new(GaugeWrites::default());
        let backend: std::sync::Arc<dyn cog_core::MetricsBackend> = writes.clone();
        let verdict = judge_loaded_rule_set(&loaded(&["a"]), Some(&document(&["a"])));
        record_rule_counts(Some(&backend), &verdict, 1).await;
        assert_eq!(writes.readouts(), vec![RULE_SET_READOUT.to_string()]);
    }

    /// A document that could not be read is not a document declaring zero
    /// rules. The count that was never taken stays unwritten, and the cell that
    /// says why is written on the same tick -- otherwise a reader subtracting
    /// the two sizes would read a missing measurement as an empty rule set.
    #[tokio::test]
    async fn an_unreadable_document_publishes_no_declared_count() {
        let writes = std::sync::Arc::new(GaugeWrites::default());
        let backend: std::sync::Arc<dyn cog_core::MetricsBackend> = writes.clone();
        let verdict = judge_loaded_rule_set(&loaded(&["a"]), None);
        record_rule_counts(Some(&backend), &verdict, 1).await;
        assert_eq!(
            writes.states(),
            vec![
                ("loaded".to_string(), 1.0),
                ("document_readable".to_string(), 0.0),
                ("agrees".to_string(), 0.0),
            ]
        );
    }
}
