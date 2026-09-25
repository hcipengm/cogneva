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
            set_row(&store, &notifier, true, &source, &message).await;
        }
        DeliveryAction::Resolve => {
            info!(
                source = %source,
                "{}",
                config_sections::describe_delivery(&source, &first)
            );
            set_row(&store, &notifier, false, &source, "").await;
        }
        DeliveryAction::Silent(reason) => {
            info!(source = %source, "config declaration check concluded nothing: {reason}");
        }
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
    firing: bool,
    source: &str,
    message: &str,
) {
    let Some(store) = store else {
        return;
    };
    let alert = NewAlert {
        rule: CONFIG_DECLARATION_RULE.to_string(),
        dedup_key: format!("{CONFIG_DECLARATION_RULE}:{source}"),
        severity: if firing { "warning" } else { "info" }.to_string(),
        message: message.to_string(),
        labels: serde_json::json!({ "source": source }),
    };
    match store.set_alert(firing, &alert).await {
        Ok(_) => {
            // Only the firing edge interrupts anyone: a row closing is visible
            // on the surface people already read.
            if let Some(notifier) = notifier.as_ref().filter(|_| firing) {
                let now = Utc::now();
                let labels = HashMap::from([
                    ("source".to_string(), source.to_string()),
                    ("message".to_string(), message.to_string()),
                ]);
                notifier
                    .notify(&[AlertEvent::Firing(AlertInstance {
                        rule_name: CONFIG_DECLARATION_RULE.to_string(),
                        labels,
                        state: AlertState::Firing,
                        severity: cog_core::AlertSeverity::Warning,
                        value: 1.0,
                        starts_at: now,
                        ends_at: None,
                        updated_at: now,
                    })])
                    .await;
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
        assert!(
            !names.iter().any(|n| n == CONFIG_DECLARATION_RULE),
            "{CONFIG_DECLARATION_RULE} is a configured rule: {}",
            names.join(", ")
        );
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
}
