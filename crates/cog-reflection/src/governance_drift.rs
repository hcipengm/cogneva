//! What the repository declares the resource ceilings to be, against what the
//! cluster is enforcing right now.
//!
//! Resource governance kinds are the operator's: the rollout loop never
//! delivers them, because replaying a committed default every revision would
//! overwrite a ceiling someone tuned (the deployer's `GOVERNANCE_KINDS` carries
//! that reasoning). Raising a limit in the repository therefore does not move
//! the object in the cluster until the next install-time apply runs, and
//! between those two moments the two sides hold different numbers with nothing
//! on the runtime side saying so: kube-state-metrics reports what the cluster
//! enforces, and what the repository declares is visible only to whoever goes
//! and reads the repository.
//!
//! A change that has landed and a change that has taken effect therefore look
//! the same from every existing reading -- until the difference surfaces as a
//! symptom, pods that cannot be created because the quota counting them is
//! still the old one, reported as an admission error rather than as a stale
//! ceiling.
//!
//! This module is that reading. Every time a bundle is assembled, the
//! governance documents of the revision being rolled out are compared field by
//! field against the objects the cluster is enforcing, and the number of fields
//! that differ is published per object.
//!
//! It reports a difference and does not judge it. A ceiling the operator
//! deliberately tuned in the cluster and a ceiling the repository raised that no
//! install has applied yet read identically here; telling them apart needs a
//! fact this side does not hold, so the reading states the difference and the
//! rule around it says the same. Zero is a reading of its own, and so is a
//! comparison that could not be made: "the two sides agree" and "nobody looked"
//! are different facts, and only one of them is reassuring.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use cog_core::claim_footprint::quantity_bytes;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFError, SFResult};

/// How many fields of one governance object disagree (gauge).
pub const GOVERNANCE_DRIFT_METRIC: &str = "cogneva_governance_drift_fields";

/// How many comparisons could not be made at all (counter). Deliberately a
/// series of its own rather than folded into the drift: when the cluster cannot
/// be read, "the two sides differ" and "nobody looked" are two facts, and one
/// cell for both leaves no way to tell "the two sides agree" from "nobody
/// looked".
pub const GOVERNANCE_CHECK_FAILURES_METRIC: &str = "cogneva_governance_check_failures_total";

/// The ceilings one governance document declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernanceDeclaration {
    /// The kind as the manifest spells it.
    pub kind: String,
    pub name: String,
    /// Field name -> the quantity as written (`limits.cpu` -> `18.5`).
    pub fields: BTreeMap<String, String>,
}

impl GovernanceDeclaration {
    /// The key this object carries in the reading.
    ///
    /// The lowercased kind is how `kubectl` spells a resource name, the same
    /// shape as the `resourcequota` label on the metrics side, so the two can
    /// be matched up.
    pub fn object(&self) -> String {
        format!("{}/{}", self.kind.to_lowercase(), self.name)
    }
}

/// The governance ceilings one manifest text declares.
///
/// Only `ResourceQuota` is covered, and the reason is scope rather than
/// convenience: its ceilings are a flat map under `spec.hard` whose field names
/// are identities. A `LimitRange` carries `spec.limits` as a sequence, and a
/// sequence's order is not an identity -- comparing by position would read a
/// reordered list as drift, and folding by `type` would need a further layer
/// expanding each type's fields, a layer no judgement has ever settled. The
/// only field evidence this module has sits on `ResourceQuota` (the cluster's
/// quota stopped at 17 while the repository already declares 18.5), so this is
/// the half that gets done, and the other is not filled in by symmetry.
pub fn governance_declarations(
    yaml_text: &str,
    origin: &str,
) -> SFResult<Vec<GovernanceDeclaration>> {
    let mut out = Vec::new();
    for doc in crate::mainline_deployer::split_docs(yaml_text, origin)? {
        let kind = doc.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if kind != "ResourceQuota" {
            continue;
        }
        let name = doc
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .ok_or_else(|| SFError::Config(format!("{origin}: a {kind} has no metadata.name")))?;
        let mut fields = BTreeMap::new();
        if let Some(hard) = doc.get("spec").and_then(|s| s.get("hard")) {
            let map = hard.as_mapping().ok_or_else(|| {
                SFError::Config(format!(
                    "{origin}: {kind}/{name} declares spec.hard as something other than a mapping"
                ))
            })?;
            for (k, v) in map {
                let Some(key) = k.as_str() else { continue };
                let Some(text) = quantity_text(v) else {
                    continue;
                };
                fields.insert(key.to_string(), text);
            }
        }
        out.push(GovernanceDeclaration {
            kind: kind.to_string(),
            name: name.to_string(),
            fields,
        });
    }
    Ok(out)
}

/// A YAML scalar as the quantity was written. Numbers count too: `pods: 40`
/// without quotes parses as a number, and it is the same declaration as
/// `pods: "40"`.
fn quantity_text(value: &serde_yaml::Value) -> Option<String> {
    match value {
        serde_yaml::Value::String(s) => Some(s.clone()),
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// The field names that disagree between a declaration and what the cluster
/// enforces, ordered by name.
///
/// The comparison lands on a common unit ([`quantity_bytes`]), because the two
/// sides may legally spell the same amount differently: a declaration writes
/// `10Gi` where the API answers `10737418240`, and `1500m` is the same quantity
/// as `1.5`. A different spelling is not drift, a different amount is --
/// comparing strings would read every normalization as one more disagreement.
///
/// A field only one side holds counts as differing. The declaration is the
/// authoritative face, but a field left behind by hand in the cluster is just
/// as much a disagreement; the reading reports it rather than deciding for the
/// reader which side is right.
pub fn drifted_fields(
    declared: &BTreeMap<String, String>,
    live: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut out = BTreeMap::new();
    for (key, d) in declared {
        match live.get(key) {
            Some(l) if same_quantity(d, l) => {}
            _ => {
                out.insert(key.clone(), ());
            }
        }
    }
    for key in live.keys() {
        if !declared.contains_key(key) {
            out.insert(key.clone(), ());
        }
    }
    out.into_keys().collect()
}

/// Whether two quantity texts denote the same amount.
///
/// When neither side parses, this falls back to comparing the text verbatim:
/// not parsing means the suffix is not one this repository knows, and then "is
/// it the same string" is the only question left to ask -- reading a
/// misspelled suffix as agreement would swallow a real difference.
fn same_quantity(a: &str, b: &str) -> bool {
    match (quantity_bytes(a), quantity_bytes(b)) {
        (Some(x), Some(y)) => x == y,
        _ => a.trim() == b.trim(),
    }
}

/// The output of `kubectl get resourcequota -o json` -> each object's enforced
/// `hard` field table.
///
/// Keys are `<lowercased kind>/<name>`, the same shape as
/// [`GovernanceDeclaration::object`], which is how the caller pairs the two
/// sides up. `spec.hard` is what admission reads.
///
/// Output that is not a listing is an error rather than half a reading: when
/// this text did not come from `kubectl get` (something else wrote to stdout,
/// say), comparing "whatever was left" yields a reading nobody can explain, and
/// it looks exactly like "the two sides agree".
pub fn live_quotas(text: &str) -> SFResult<BTreeMap<String, BTreeMap<String, String>>> {
    let v: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| SFError::Config(format!("resource quota listing is not JSON: {e}")))?;
    let items = v
        .get("items")
        .and_then(|i| i.as_array())
        .ok_or_else(|| SFError::Config("resource quota listing has no items array".into()))?;
    let mut out = BTreeMap::new();
    for item in items {
        // Identity comes from the object itself rather than leaving the caller
        // to guess by position; an entry without one is skipped, and it then
        // reads as "declared but not enforced" -- reported rather than
        // swallowed.
        let Some(kind) = item.get("kind").and_then(|k| k.as_str()) else {
            continue;
        };
        let Some(name) = item
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
        else {
            continue;
        };
        let mut fields = BTreeMap::new();
        if let Some(hard) = item
            .get("spec")
            .and_then(|s| s.get("hard"))
            .and_then(|h| h.as_object())
        {
            for (key, value) in hard {
                if let Some(text) = value.as_str() {
                    fields.insert(key.clone(), text.to_string());
                }
            }
        }
        out.insert(format!("{}/{name}", kind.to_lowercase()), fields);
    }
    Ok(out)
}

/// Which fields differ, per declared object, against what the cluster enforces.
///
/// "The cluster does not have this object at all" is judged here rather than in
/// the caller: on that side it is an empty table, and every declared field then
/// differs -- an absent object is not "nothing to compare", it is one shape of
/// the two sides disagreeing. Folding it together with "the cluster could not
/// be read" would produce an empty field that reports agreement.
pub fn drift_by_object(
    declared: &[GovernanceDeclaration],
    live: &BTreeMap<String, BTreeMap<String, String>>,
) -> Vec<(String, Vec<String>)> {
    let absent = BTreeMap::new();
    declared
        .iter()
        .map(|d| {
            let fields = live.get(&d.object()).unwrap_or(&absent);
            (d.object(), drifted_fields(&d.fields, fields))
        })
        .collect()
}

/// How many fields differ between each governance object and the declaration.
///
/// The drift count is a gauge rather than a counter: it answers "how far apart
/// are the two sides right now", and accumulating that answers nothing.
#[derive(Default)]
pub struct GovernanceDrift {
    entries: Mutex<BTreeMap<String, usize>>,
    check_failures: AtomicU64,
}

impl GovernanceDrift {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the drifted field count compared for one object (zero included).
    pub fn record(&self, object: &str, drifted: usize) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(object.to_string(), drifted);
    }

    /// Record one comparison that could not be made: the cluster was
    /// unreadable, the object absent, or the output unparseable.
    pub fn record_check_failure(&self) {
        self.check_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drift_fields(&self, object: &str) -> Option<usize> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(object)
            .copied()
    }

    pub fn objects(&self) -> Vec<String> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    pub fn check_failures(&self) -> u64 {
        self.check_failures.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Observable for GovernanceDrift {
    /// The failure count is published from the first scrape on: a process that
    /// can never complete a comparison has nothing else to publish, and that
    /// "nothing" looks the same on the scrape face as "there is no governance
    /// object to measure" -- only a process holding no handle at all is in a
    /// position to say the latter. Objects already compared publish their zero
    /// for the same reason.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = vec![RawMetric::new(
            GOVERNANCE_CHECK_FAILURES_METRIC,
            self.check_failures() as f64,
        )];
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        for (object, fields) in entries.iter() {
            out.push(
                RawMetric::new(GOVERNANCE_DRIFT_METRIC, *fields as f64)
                    // Spelled as a literal rather than through a constant: the
                    // shipped-summary gate reads label names out of the source
                    // text of this call, so a constant here would make the label
                    // invisible to it and the summaries naming {object} would
                    // fail it.
                    .with_label("object", object.clone()),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The reading does not vary by dimension: one cell per object, whichever
    /// dimension the question came from.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUOTA: &str = "kind: ResourceQuota\nmetadata:\n  name: cogneva-quota\nspec:\n  hard:\n    limits.cpu: \"18.5\"\n    limits.memory: 34Gi\n    pods: \"40\"\n";

    #[test]
    fn a_quota_yields_its_object_and_every_declared_field() {
        let decls = governance_declarations(QUOTA, "quota.yaml").unwrap();
        assert_eq!(decls.len(), 1);
        assert_eq!(decls[0].object(), "resourcequota/cogneva-quota");
        // A quoted string and a bare number are two spellings of one
        // declaration, and both count.
        assert_eq!(
            decls[0].fields.get("limits.cpu").map(String::as_str),
            Some("18.5")
        );
        assert_eq!(
            decls[0].fields.get("limits.memory").map(String::as_str),
            Some("34Gi")
        );
        assert_eq!(decls[0].fields.get("pods").map(String::as_str), Some("40"));
    }

    #[test]
    fn a_limit_range_is_left_to_the_half_that_has_evidence() {
        let text = "kind: LimitRange\nmetadata:\n  name: cogneva-limits\nspec:\n  limits:\n    - type: Container\n      max:\n        cpu: \"4\"\n";
        assert!(governance_declarations(text, "limits.yaml")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_quota_without_a_name_is_an_error_not_a_nameless_object() {
        let text = "kind: ResourceQuota\nspec:\n  hard:\n    pods: \"40\"\n";
        assert!(governance_declarations(text, "anon.yaml").is_err());
    }

    #[test]
    fn spelling_that_denotes_the_same_quantity_is_not_drift() {
        let declared: BTreeMap<String, String> = [
            ("limits.cpu".to_string(), "1500m".to_string()),
            ("limits.memory".to_string(), "10Gi".to_string()),
        ]
        .into_iter()
        .collect();
        // The API normalizes these into another spelling, and that is not a
        // disagreement.
        let live: BTreeMap<String, String> = [
            ("limits.cpu".to_string(), "1.5".to_string()),
            ("limits.memory".to_string(), "10737418240".to_string()),
        ]
        .into_iter()
        .collect();
        assert!(drifted_fields(&declared, &live).is_empty());
    }

    #[test]
    fn a_field_either_side_alone_holds_counts_as_drift() {
        let declared: BTreeMap<String, String> = [("limits.cpu".to_string(), "18.5".to_string())]
            .into_iter()
            .collect();
        let live: BTreeMap<String, String> = [
            ("limits.cpu".to_string(), "17".to_string()),
            ("pods".to_string(), "40".to_string()),
        ]
        .into_iter()
        .collect();
        // A different amount and a field only the cluster holds are both here:
        // the reading reports "the two sides differ", not just the differences
        // the declared side happens to recognize.
        assert_eq!(drifted_fields(&declared, &live), vec!["limits.cpu", "pods"]);
    }

    #[test]
    fn a_missing_field_on_the_cluster_side_is_drift() {
        let declared: BTreeMap<String, String> = [
            ("limits.cpu".to_string(), "18.5".to_string()),
            ("pods".to_string(), "40".to_string()),
        ]
        .into_iter()
        .collect();
        let live: BTreeMap<String, String> = [("limits.cpu".to_string(), "18.5".to_string())]
            .into_iter()
            .collect();
        assert_eq!(drifted_fields(&declared, &live), vec!["pods"]);
    }

    #[tokio::test]
    async fn zero_is_published_and_so_is_a_comparison_that_never_happened() {
        let drift = GovernanceDrift::new();
        // Nothing compared yet: only the failure count, no object at all.
        let metrics = drift.collect_metrics("default").await.unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, GOVERNANCE_CHECK_FAILURES_METRIC);
        assert!(!metrics.iter().any(|m| m.name == GOVERNANCE_DRIFT_METRIC));

        drift.record("resourcequota/cogneva-quota", 0);
        drift.record_check_failure();
        let metrics = drift.collect_metrics("default").await.unwrap();
        let found = metrics
            .iter()
            .find(|m| m.name == GOVERNANCE_DRIFT_METRIC)
            .expect("a checked object publishes its count even when the count is zero");
        assert_eq!(found.value, 0.0);
        assert_eq!(
            found.labels.get("object").map(String::as_str),
            Some("resourcequota/cogneva-quota")
        );
        let failures = metrics
            .iter()
            .find(|m| m.name == GOVERNANCE_CHECK_FAILURES_METRIC)
            .unwrap();
        assert_eq!(failures.value, 1.0);
    }

    const LIVE: &str = r#"{"apiVersion":"v1","kind":"List","items":[
        {"kind":"ResourceQuota","metadata":{"name":"cogneva-quota"},
         "spec":{"hard":{"limits.cpu":"17","limits.memory":"34Gi","pods":"40"}}}
    ]}"#;

    #[test]
    fn the_enforced_side_is_keyed_the_way_the_declared_side_is() {
        let live = live_quotas(LIVE).unwrap();
        let quota = live
            .get("resourcequota/cogneva-quota")
            .expect("the key has to match what a declaration builds for itself");
        assert_eq!(quota.get("limits.cpu").map(String::as_str), Some("17"));
    }

    #[test]
    fn an_empty_cluster_is_an_empty_listing_not_a_failure() {
        let live = live_quotas(r#"{"apiVersion":"v1","kind":"List","items":[]}"#).unwrap();
        assert!(live.is_empty());
    }

    #[test]
    fn output_that_is_not_a_listing_is_refused_rather_than_half_read() {
        assert!(live_quotas("Error from server (NotFound): resourcequotas not found").is_err());
        // A single object is not a listing: reading it as one would silently drop
        // every other quota in the namespace.
        assert!(live_quotas(r#"{"kind":"ResourceQuota","metadata":{"name":"q"}}"#).is_err());
    }

    #[test]
    fn a_quota_the_cluster_does_not_have_reads_as_every_field_drifting() {
        let decls = governance_declarations(QUOTA, "quota.yaml").unwrap();
        let compared = drift_by_object(&decls, &BTreeMap::new());
        assert_eq!(compared.len(), 1);
        assert_eq!(compared[0].0, "resourcequota/cogneva-quota");
        assert_eq!(
            compared[0].1,
            vec!["limits.cpu", "limits.memory", "pods"],
            "an object that is not there is not 'nothing to compare', it is every \
             declared field differing"
        );
    }

    /// The live case this batch was written for: the repository raised the
    /// ceiling to 18.5 and the cluster still admits against 17.
    #[test]
    fn the_landed_change_that_no_install_has_applied_shows_up_as_one_field() {
        let decls = governance_declarations(QUOTA, "quota.yaml").unwrap();
        let live = live_quotas(LIVE).unwrap();
        let compared = drift_by_object(&decls, &live);
        assert_eq!(compared[0].1, vec!["limits.cpu"]);
    }
}
