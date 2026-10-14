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

/// How many lines of one delivered observability manifest disagree (gauge).
///
/// The second object class on the same spine. A resource ceiling belongs to the
/// operator and is applied at install time; the observability stack's manifests
/// belong to the installer and are delivered only by the install script. Both
/// therefore sit in the repository at the revision being tracked while the
/// cluster holds whatever the last install left there, and in both cases a
/// change that has landed and a change that has taken effect look the same from
/// every reading that already exists.
///
/// The two are two series rather than two label values on one because neither
/// the objects nor the counts are the same kind. The ceilings come back from the
/// cluster as a flat map, so the governance side counts named fields and keys
/// them `resourcequota/cogneva-quota`. This side has a `kubectl diff` of a whole
/// manifest file and no per-object attribution that could be trusted -- the only
/// thing that would give one is the temp paths `kubectl diff` prints its headers
/// with, which are an internal convention and would silently degrade to
/// per-file on the day they move. So the count here is the differing lines of
/// that file's own diff and the key is the file. Two units, two label names, and
/// a query has to say which it is reading.
pub const OBSERVABILITY_DRIFT_METRIC: &str = "cogneva_observability_drift_lines";

/// The object classes this reading carries.
///
/// A closed set. A class picks the gauge its counts are published under, the
/// label its keys ride in -- the two key spaces hold different things, so one
/// label name for both would invite a reader to compare a ceiling with a
/// filename -- and, together with [`CheckFailure`], the reason label of a
/// comparison that could not be made: the two classes read different documents
/// through different commands, so where they fail is a different place to go and
/// look.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DriftClass {
    /// The resource ceilings the operator owns.
    Governance,
    /// The observability stack's own manifests, keyed by file.
    Observability,
}

impl DriftClass {
    /// Every class, in the order the label values are listed.
    pub const ALL: [DriftClass; 2] = [DriftClass::Governance, DriftClass::Observability];
}

/// How many comparisons could not be made at all (counter), by reason.
///
/// Deliberately a series of its own rather than folded into the drift: when the
/// cluster cannot be read, "the two sides differ" and "nobody looked" are two
/// facts, and one cell for both leaves no way to tell "the two sides agree"
/// from "nobody looked".
///
/// The count carries its reason for the same kind of reason. Each comparison
/// has three ways to fail, and across the two classes they point at six
/// different things to go and look at, so a bare total hands a reader a number
/// with no direction. The
/// `warn!` beside each failure used to carry that difference, and the process
/// that wrote it is gone exactly when the count is still being read -- the
/// count outlives the log by design, so the reason has to travel in the count.
pub const GOVERNANCE_CHECK_FAILURES_METRIC: &str = "cogneva_governance_check_failures_total";

/// The ways a comparison can fail to happen.
///
/// A closed set: the label value is one of these words and nothing else, so a
/// query can be written against the reason without a reader having to guess at
/// the spelling. The axis is the class crossed with the side that could not be
/// read, and both halves are in the label because both are a different place to
/// go and look: a ceiling the repository stopped declaring and a manifest
/// directory that could not be listed fail the same way and are fixed in
/// different files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckFailure {
    /// The declared ceilings could not be read from the tracked revision: the
    /// repository, the directory in it, or the declaration's own text.
    GovernanceDeclarationUnreadable,
    /// The enforced ceilings could not be read: `kubectl` did not run, or ran
    /// and could not reach the cluster.
    GovernanceLiveUnreadable,
    /// The cluster answered, and the answer is not text this side will parse.
    GovernanceLiveUnparseable,
    /// The observability stack's declared manifests could not be read from the
    /// tracked revision.
    ObservabilityDeclarationUnreadable,
    /// The stack's live objects could not be read.
    ObservabilityLiveUnreadable,
    /// The stack's live objects came back as text this side will not parse.
    ObservabilityLiveUnparseable,
}

impl CheckFailure {
    /// Every reason, in the order the label values are listed.
    ///
    /// All of them are published whether or not they have happened: an absent
    /// series and a series at zero are different readings, and "this failure
    /// mode has not occurred" is the second one.
    pub const ALL: [CheckFailure; 6] = [
        CheckFailure::GovernanceDeclarationUnreadable,
        CheckFailure::GovernanceLiveUnreadable,
        CheckFailure::GovernanceLiveUnparseable,
        CheckFailure::ObservabilityDeclarationUnreadable,
        CheckFailure::ObservabilityLiveUnreadable,
        CheckFailure::ObservabilityLiveUnparseable,
    ];

    /// The label value, which is also the word to search the source for.
    pub fn as_str(self) -> &'static str {
        match self {
            CheckFailure::GovernanceDeclarationUnreadable => "governance_declaration_unreadable",
            CheckFailure::GovernanceLiveUnreadable => "governance_live_unreadable",
            CheckFailure::GovernanceLiveUnparseable => "governance_live_unparseable",
            CheckFailure::ObservabilityDeclarationUnreadable => {
                "observability_declaration_unreadable"
            }
            CheckFailure::ObservabilityLiveUnreadable => "observability_live_unreadable",
            CheckFailure::ObservabilityLiveUnparseable => "observability_live_unparseable",
        }
    }

    /// Where this reason's cell lives. Derived from [`Self::ALL`] rather than
    /// from the declaration order, so the two cannot drift apart.
    fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|r| *r == self)
            .expect("every reason is in ALL")
    }
}

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
pub(crate) fn quantity_text(value: &serde_yaml::Value) -> Option<String> {
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

/// The enforced ceilings of one `ResourceQuota` and what the namespace is
/// currently counted as using.
///
/// `hard` is what admission reads; `used` is what the quota controller has
/// counted against it. They are two faces of one object and are read together
/// because the question the rollout asks is "does the new pod still fit", and
/// that needs both sides of the subtraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaUsage {
    pub hard: BTreeMap<String, String>,
    pub used: BTreeMap<String, String>,
}

/// The output of `kubectl get resourcequota -o json` -> each object's `hard`
/// and `used` field tables.
///
/// Keys are `<lowercased kind>/<name>`, the same shape as
/// [`GovernanceDeclaration::object`], which is how the caller pairs the two
/// sides up. `spec.hard` is what admission reads; `status.used` is what the
/// controller has counted so far and may be absent until it has run once, which
/// is why a missing `status` leaves an empty table rather than failing — "the
/// count is not there yet" is not "the listing is not a listing".
///
/// Output that is not a listing is an error rather than half a reading: when
/// this text did not come from `kubectl get` (something else wrote to stdout,
/// say), comparing "whatever was left" yields a reading nobody can explain, and
/// it looks exactly like "the two sides agree".
pub fn live_quota_usage(text: &str) -> SFResult<BTreeMap<String, QuotaUsage>> {
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
        let table = |v: Option<&serde_json::Value>| {
            let mut fields = BTreeMap::new();
            if let Some(map) = v.and_then(|h| h.as_object()) {
                for (key, value) in map {
                    if let Some(text) = value.as_str() {
                        fields.insert(key.clone(), text.to_string());
                    }
                }
            }
            fields
        };
        out.insert(
            format!("{}/{name}", kind.to_lowercase()),
            QuotaUsage {
                hard: table(item.get("spec").and_then(|s| s.get("hard"))),
                used: table(item.get("status").and_then(|s| s.get("used"))),
            },
        );
    }
    Ok(out)
}

/// The output of `kubectl get resourcequota -o json` -> each object's enforced
/// `hard` field table, the declaration-comparison face of
/// [`live_quota_usage`].
pub fn live_quotas(text: &str) -> SFResult<BTreeMap<String, BTreeMap<String, String>>> {
    Ok(live_quota_usage(text)?
        .into_iter()
        .map(|(object, usage)| (object, usage.hard))
        .collect())
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

/// How many fields differ between each object and the declaration, per class.
///
/// The drift count is a gauge rather than a counter: it answers "how far apart
/// are the two sides right now", and accumulating that answers nothing.
///
/// One table per class rather than one table keyed by object. A comparison
/// replaces its whole table (see [`Self::record_comparison`]), so a single table
/// would let the class that compared last erase the other's objects -- and the
/// erasure would look exactly like the ceiling it removed having been applied.
#[derive(Default)]
pub struct GovernanceDrift {
    governance: Mutex<BTreeMap<String, usize>>,
    observability: Mutex<BTreeMap<String, usize>>,
    /// One cell per [`CheckFailure`], in that order.
    check_failures: [AtomicU64; CheckFailure::ALL.len()],
}

impl GovernanceDrift {
    pub fn new() -> Self {
        Self::default()
    }

    /// The table one class's objects are counted in.
    fn table(&self, class: DriftClass) -> &Mutex<BTreeMap<String, usize>> {
        match class {
            DriftClass::Governance => &self.governance,
            DriftClass::Observability => &self.observability,
        }
    }

    /// Record the whole comparison of one class: one entry per declared object,
    /// zero counts included, and nothing else.
    ///
    /// Replacing the table rather than inserting into it is what keeps the
    /// reading clearable in both directions. A revision can stop declaring an
    /// object or rename it, and an entry left behind by the previous comparison
    /// would keep publishing the last count it saw -- a series sitting at a
    /// non-zero value with nothing left in the repository that could ever clear
    /// it, which is the same "fires once and can never be cleared" the
    /// comparison is arranged to avoid on the cluster side.
    ///
    /// A comparison that could not be made does not come through here at all: it
    /// records a failure instead and leaves the last table standing, because
    /// nobody looked is not a reading that the two sides agree.
    pub fn record_comparison(&self, class: DriftClass, drifted: &BTreeMap<String, usize>) {
        *self.table(class).lock().unwrap_or_else(|e| e.into_inner()) = drifted.clone();
    }

    /// Record one comparison that could not be made, and why.
    ///
    /// The reason is not optional: a caller that has a failure in hand knows
    /// which of the six it is, and a default would put the reason back to being
    /// unrecorded for whichever caller took it.
    pub fn record_check_failure(&self, reason: CheckFailure) {
        self.check_failures[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn drift_fields(&self, class: DriftClass, object: &str) -> Option<usize> {
        self.table(class)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(object)
            .copied()
    }

    pub fn objects(&self, class: DriftClass) -> Vec<String> {
        self.table(class)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    /// Every comparison that could not be made, whichever the reason.
    ///
    /// Computed from the per-reason cells rather than kept beside them: a
    /// separately maintained total is a second place for the same fact to
    /// live, and the two would be free to disagree.
    pub fn check_failures(&self) -> u64 {
        CheckFailure::ALL
            .iter()
            .map(|r| self.check_failures_for(*r))
            .sum()
    }

    pub fn check_failures_for(&self, reason: CheckFailure) -> u64 {
        self.check_failures[reason.index()].load(Ordering::Relaxed)
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
        let mut out: Vec<RawMetric> = CheckFailure::ALL
            .iter()
            .map(|reason| {
                RawMetric::new(
                    GOVERNANCE_CHECK_FAILURES_METRIC,
                    self.check_failures_for(*reason) as f64,
                )
                // Spelled as a literal for the same reason the object label
                // below is: the shipped-summary gate reads label names out of
                // the source text of this call.
                .with_label("reason", reason.as_str())
            })
            .collect();
        // One loop per class rather than one over [`DriftClass::ALL`]: the two
        // key spaces hold different things (a ceiling's name against a manifest
        // file), so the label they ride in is named after what it holds rather
        // than shared. Spelled as literals, not through a constant, because the
        // name is what a rule's summary selectors are written against.
        for (object, fields) in self
            .table(DriftClass::Governance)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            out.push(
                RawMetric::new(GOVERNANCE_DRIFT_METRIC, *fields as f64)
                    .with_label("object", object.clone()),
            );
        }
        for (manifest, lines) in self
            .table(DriftClass::Observability)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            out.push(
                RawMetric::new(OBSERVABILITY_DRIFT_METRIC, *lines as f64)
                    .with_label("manifest", manifest.clone()),
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
        // Nothing compared yet: every failure reason, no object at all.
        let metrics = drift.collect_metrics("default").await.unwrap();
        assert_eq!(metrics.len(), CheckFailure::ALL.len());
        assert!(metrics
            .iter()
            .all(|m| m.name == GOVERNANCE_CHECK_FAILURES_METRIC));
        assert!(!metrics.iter().any(|m| m.name == GOVERNANCE_DRIFT_METRIC));
        assert!(!metrics
            .iter()
            .any(|m| m.name == OBSERVABILITY_DRIFT_METRIC));

        drift.record_comparison(
            DriftClass::Governance,
            &BTreeMap::from([("resourcequota/cogneva-quota".to_string(), 0)]),
        );
        drift.record_check_failure(CheckFailure::GovernanceLiveUnreadable);
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
        let failures = failures_of(&metrics);
        assert_eq!(
            failures,
            vec![
                ("governance_declaration_unreadable", 0.0),
                ("governance_live_unreadable", 1.0),
                ("governance_live_unparseable", 0.0),
                ("observability_declaration_unreadable", 0.0),
                ("observability_live_unreadable", 0.0),
                ("observability_live_unparseable", 0.0),
            ],
            "每一种失败原因都是自己的一格，且没发生过的那种读成 0 而不是消失"
        );
    }

    /// The two classes publish under two gauges and keep two tables: a class
    /// that compares must not erase the other's objects, which is what one
    /// table would do and what would look like the other side agreeing.
    ///
    /// The keys are the ones the production paths really write -- a ceiling's
    /// name from the governance comparison, a manifest file from the stack
    /// convergence loop. Seeding this with a `kind/name` for the observability
    /// side would test the table and say nothing about the two key spaces, which
    /// is the disagreement a reader would actually meet.
    #[tokio::test]
    async fn a_class_that_compares_leaves_the_other_class_standing() {
        let drift = GovernanceDrift::new();
        drift.record_comparison(
            DriftClass::Governance,
            &BTreeMap::from([("resourcequota/cogneva-quota".to_string(), 2)]),
        );
        drift.record_comparison(
            DriftClass::Observability,
            &BTreeMap::from([("10-loki.yaml".to_string(), 3)]),
        );
        drift.record_comparison(
            DriftClass::Observability,
            &BTreeMap::from([("10-loki.yaml".to_string(), 0)]),
        );
        assert_eq!(
            drift.drift_fields(DriftClass::Governance, "resourcequota/cogneva-quota"),
            Some(2),
            "the second class's comparison erased the first class's object"
        );
        assert_eq!(
            drift.drift_fields(DriftClass::Observability, "10-loki.yaml"),
            Some(0)
        );
        let metrics = drift.collect_metrics("default").await.unwrap();
        let names: Vec<&str> = metrics.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&GOVERNANCE_DRIFT_METRIC));
        assert!(names.contains(&OBSERVABILITY_DRIFT_METRIC));
    }

    /// Each class's keys ride in the label named after what they hold, and the
    /// two names differ.
    ///
    /// Both rules aggregate the label away, so nothing on the alerting side
    /// notices if the two collapse into one name -- but a person reading the raw
    /// series does, and would be invited to compare a ceiling with a filename.
    /// One name for both is exactly the kind of drift this pair of readings
    /// exists to make visible, so it is pinned here.
    #[tokio::test]
    async fn each_class_publishes_its_keys_under_its_own_label() {
        let drift = GovernanceDrift::new();
        drift.record_comparison(
            DriftClass::Governance,
            &BTreeMap::from([("resourcequota/cogneva-quota".to_string(), 2)]),
        );
        drift.record_comparison(
            DriftClass::Observability,
            &BTreeMap::from([("10-loki.yaml".to_string(), 3)]),
        );
        let metrics = drift.collect_metrics("default").await.unwrap();

        let ceiling = metrics
            .iter()
            .find(|m| m.name == GOVERNANCE_DRIFT_METRIC)
            .expect("the ceiling was compared");
        assert_eq!(
            ceiling.labels.get("object").map(String::as_str),
            Some("resourcequota/cogneva-quota")
        );
        assert!(!ceiling.labels.contains_key("manifest"));

        let manifest = metrics
            .iter()
            .find(|m| m.name == OBSERVABILITY_DRIFT_METRIC)
            .expect("the manifest was compared");
        assert_eq!(
            manifest.labels.get("manifest").map(String::as_str),
            Some("10-loki.yaml")
        );
        assert!(
            !manifest.labels.contains_key("object"),
            "一份清单文件挂在名叫 object 的标签下，读的人会拿它去和一条 ceiling 比"
        );
    }

    /// Every published failure count as (reason, value), in the order published.
    fn failures_of(metrics: &[RawMetric]) -> Vec<(&str, f64)> {
        metrics
            .iter()
            .filter(|m| m.name == GOVERNANCE_CHECK_FAILURES_METRIC)
            .map(|m| {
                (
                    m.labels.get("reason").map(String::as_str).unwrap_or(""),
                    m.value,
                )
            })
            .collect()
    }

    #[test]
    fn a_reason_lands_in_its_own_cell_and_the_total_is_the_sum() {
        // Every reason is a different thing to go and look at, so they must not
        // share a cell -- and the total must not be a seventh place the same
        // fact is kept.
        let drift = GovernanceDrift::new();
        drift.record_check_failure(CheckFailure::GovernanceDeclarationUnreadable);
        drift.record_check_failure(CheckFailure::GovernanceLiveUnreadable);
        drift.record_check_failure(CheckFailure::GovernanceLiveUnreadable);
        drift.record_check_failure(CheckFailure::ObservabilityLiveUnreadable);
        assert_eq!(
            drift.check_failures_for(CheckFailure::GovernanceDeclarationUnreadable),
            1
        );
        assert_eq!(
            drift.check_failures_for(CheckFailure::GovernanceLiveUnreadable),
            2
        );
        assert_eq!(
            drift.check_failures_for(CheckFailure::GovernanceLiveUnparseable),
            0
        );
        assert_eq!(
            drift.check_failures_for(CheckFailure::ObservabilityDeclarationUnreadable),
            0
        );
        assert_eq!(
            drift.check_failures_for(CheckFailure::ObservabilityLiveUnreadable),
            1
        );
        assert_eq!(
            drift.check_failures_for(CheckFailure::ObservabilityLiveUnparseable),
            0
        );
        assert_eq!(drift.check_failures(), 4);
    }

    #[test]
    fn every_reason_has_a_distinct_label_value() {
        // The label is a closed set a query is written against, so two reasons
        // spelling the same word would be one cell wearing two names.
        let mut seen: Vec<&str> = CheckFailure::ALL.iter().map(|r| r.as_str()).collect();
        seen.sort_unstable();
        let before = seen.len();
        seen.dedup();
        assert_eq!(seen.len(), before, "{:?}", CheckFailure::ALL);
    }

    const LIVE: &str = r#"{"apiVersion":"v1","kind":"List","items":[
        {"kind":"ResourceQuota","metadata":{"name":"cogneva-quota"},
         "spec":{"hard":{"limits.cpu":"17","limits.memory":"34Gi","pods":"40"}}}
    ]}"#;

    /// A revision that stops declaring an object (or renames it) has to take the
    /// old series with it: an entry left behind reports a drift nothing in the
    /// repository could ever clear.
    #[tokio::test]
    async fn an_object_that_stops_being_declared_stops_being_published() {
        let drift = GovernanceDrift::new();
        drift.record_comparison(
            DriftClass::Governance,
            &BTreeMap::from([
                ("resourcequota/cogneva-quota".to_string(), 2),
                ("resourcequota/cogneva-quota-old".to_string(), 1),
            ]),
        );
        assert_eq!(drift.objects(DriftClass::Governance).len(), 2);

        // The next revision declares only the renamed object, and now matches.
        drift.record_comparison(
            DriftClass::Governance,
            &BTreeMap::from([("resourcequota/cogneva-quota-v2".to_string(), 0)]),
        );

        assert_eq!(
            drift.objects(DriftClass::Governance),
            vec!["resourcequota/cogneva-quota-v2"],
            "an object the revision no longer declares is still being published"
        );
        assert_eq!(
            drift.drift_fields(DriftClass::Governance, "resourcequota/cogneva-quota-old"),
            None
        );
    }

    #[test]
    fn the_enforced_side_is_keyed_the_way_the_declared_side_is() {
        let live = live_quotas(LIVE).unwrap();
        let quota = live
            .get("resourcequota/cogneva-quota")
            .expect("the key has to match what a declaration builds for itself");
        assert_eq!(quota.get("limits.cpu").map(String::as_str), Some("17"));
    }

    #[test]
    fn the_usage_face_reads_status_used_beside_hard() {
        let text = r#"{"apiVersion":"v1","kind":"List","items":[
            {"kind":"ResourceQuota","metadata":{"name":"cogneva-quota"},
             "spec":{"hard":{"limits.cpu":"23"}},
             "status":{"used":{"limits.cpu":"18500m"}}}
        ]}"#;
        let usage = live_quota_usage(text).unwrap();
        let q = usage.get("resourcequota/cogneva-quota").unwrap();
        assert_eq!(q.hard.get("limits.cpu").map(String::as_str), Some("23"));
        assert_eq!(q.used.get("limits.cpu").map(String::as_str), Some("18500m"));
    }

    #[test]
    fn a_quota_without_status_reads_an_empty_used_table_not_a_failure() {
        // `status.used` is written by the controller and may not be there yet;
        // that is not the same as the listing being unreadable.
        let usage = live_quota_usage(LIVE).unwrap();
        let q = usage.get("resourcequota/cogneva-quota").unwrap();
        assert_eq!(q.hard.get("limits.cpu").map(String::as_str), Some("17"));
        assert!(q.used.is_empty());
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
