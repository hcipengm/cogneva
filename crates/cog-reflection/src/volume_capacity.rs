//! Grows a volume's declaration to the size the volume turned out to need.
//!
//! A claim's declared size is arithmetic done at install time; what the volume
//! actually holds is measured at run time. Nothing joined the two: the walker
//! publishes `cogneva_data_volume_used_bytes`, one rule divides it by the
//! declared request, and that rule paging someone was the whole loop. The
//! rollout loop never replays a claim (`STORAGE_CLAIM_KINDS`), so a declaration
//! that turns out to be too small stays too small for the life of the install
//! while the only party that can measure the shortfall reports it and stops
//! there.
//!
//! This is the missing half: the same process that walks the volume decides
//! whether the declaration still covers what was measured and, when it does
//! not, raises the request. Three things make that a decision rather than a
//! reflex, and all three are stated here so the rule beside them can be read
//! against the code:
//!
//! - **Trigger.** The measured footprint is compared with the declaration once
//!   per round, and only the *peak* over the last [`PEAK_ROUNDS`] walks counts.
//!   A build target directory breathes: a sweep that lands just after a link
//!   step reads high and one after a `cargo clean` reads low, and a single
//!   sample would resize a volume on the way past.
//! - **Criterion.** The new request is the peak with the same headroom the
//!   chart's own declarations are sized by ([`HEADROOM_PERCENT`]), rounded up
//!   to a whole GiB. If that number is not larger than the request already in
//!   the cluster, nothing is written -- the declaration is already big enough
//!   and a patch would only churn the object.
//! - **Clamps.** A request is admission-controlled: the LimitRange's per-claim
//!   maximum and what the ResourceQuota has left are read from the cluster
//!   before the patch, and a target above either is reported as clamped rather
//!   than attempted and refused. The two numbers are what makes a correct
//!   decision unfixable by this loop alone, so they are named in the reading
//!   instead of being discovered as an admission error.
//!
//! Growth only. A volume that has been written to cannot be shrunk by the API,
//! and the peak it reached is a fact about what this deployment does rather than
//! about one noisy afternoon, so `declared` is monotone for as long as the
//! volume exists.
//!
//! The loop acts only on volumes this process both mounts and measures: the
//! `claim=path` list the deployment declares is also the list of claims whose
//! request it may raise, because a claim it cannot walk is a claim it cannot
//! have a criterion for. Everything else is another process's reading.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::claim_footprint::{self, quantity_bytes, ClaimFootprint};
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{ResourceEvent, ResourceEventKind, ResourceEventOutcome, SFResult, ShutdownSignal};
use tracing::{debug, info, warn};

/// What became of one claim in one round, counted per outcome.
pub const OUTCOME_METRIC: &str = "cogneva_volume_capacity_outcome_total";

/// The measured peak and the request in force, per claim.
pub const BYTES_METRIC: &str = "cogneva_volume_capacity_bytes";

/// Label naming the claim a row is about.
pub const CLAIM_LABEL: &str = "persistentvolumeclaim";

/// Label naming which quantity a row carries: `declared` or `peak`.
pub const STATE_LABEL: &str = "state";

/// The request the cluster holds for this claim.
pub const STATE_DECLARED: &str = "declared";

/// The highest footprint measured across the peak window.
pub const STATE_PEAK: &str = "peak";

/// What the claim's status says it was actually given.
///
/// Distinct from `declared` on purpose: the request is what this loop or the
/// chart asked for, and this is what the storage side handed back. On a class
/// that cannot carry an expansion the two never converge, and a volume whose
/// granted size is below what it already holds has no bound at all -- the pair
/// is what makes that readable without going to the claim object.
pub const STATE_GRANTED: &str = "granted";

/// The bound a claim was clamped by, and its size.
pub const CEILING_METRIC: &str = "cogneva_volume_capacity_ceiling_bytes";

/// Which admission object set the bound.
pub const BOUND_LABEL: &str = "bound";

/// The per-claim maximum a LimitRange declares.
pub const BOUND_LIMIT_RANGE: &str = "limit_range";

/// The room left under the namespace ResourceQuota.
pub const BOUND_QUOTA: &str = "quota";

/// The label naming what became of a claim this round.
pub const OUTCOME_LABEL: &str = "outcome";

/// The request already covers the peak with its headroom; nothing was written.
pub const OUTCOME_AT_TARGET: &str = "at_target";

/// The request was raised to the computed target.
pub const OUTCOME_GREW: &str = "grew";

/// The target is above what admission allows; the request was left alone.
///
/// Distinct from a refusal: the number this loop would have written is not
/// acceptable to the cluster at all, so the repair is in the LimitRange or the
/// quota, and no amount of retrying here changes it.
pub const OUTCOME_CLAMPED: &str = "clamped";

/// The patch was sent and the API server refused it.
pub const OUTCOME_REFUSED: &str = "refused";

/// The claim announces more than its status has been given back yet, so an
/// expansion is already in flight and a second one would be refused.
pub const OUTCOME_EXPANDING: &str = "expanding";

/// Nothing could be measured this round: the walk failed or the claim could not
/// be read. Distinct from `at_target`, which is a measurement that agreed.
pub const OUTCOME_UNMEASURED: &str = "unmeasured";

/// Every value the outcome label takes, in slot order.
pub const OUTCOMES: &[&str] = &[
    OUTCOME_AT_TARGET,
    OUTCOME_GREW,
    OUTCOME_CLAMPED,
    OUTCOME_REFUSED,
    OUTCOME_EXPANDING,
    OUTCOME_UNMEASURED,
];

/// One claim's fate in a round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityOutcome {
    AtTarget,
    Grew,
    Clamped,
    Refused,
    Expanding,
    Unmeasured,
}

impl CapacityOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AtTarget => OUTCOME_AT_TARGET,
            Self::Grew => OUTCOME_GREW,
            Self::Clamped => OUTCOME_CLAMPED,
            Self::Refused => OUTCOME_REFUSED,
            Self::Expanding => OUTCOME_EXPANDING,
            Self::Unmeasured => OUTCOME_UNMEASURED,
        }
    }

    fn index(self) -> usize {
        match self {
            Self::AtTarget => 0,
            Self::Grew => 1,
            Self::Clamped => 2,
            Self::Refused => 3,
            Self::Expanding => 4,
            Self::Unmeasured => 5,
        }
    }
}

/// Headroom the declaration is sized with, as a percentage of the peak.
///
/// The same factor the chart's volumes are declared by, and stated once here so
/// the loop that repairs a declaration and the numbers in `values.yaml` cannot
/// drift apart into two standards for "big enough".
pub const HEADROOM_PERCENT: u64 = 120;

/// Walks whose peak is kept. Six is about an hour at the default cadence, which
/// is longer than any one build and shorter than the day a mis-sized
/// declaration otherwise survives.
pub const PEAK_ROUNDS: usize = 6;

/// Cadence of the loop when the deployment states none.
pub const DEFAULT_INTERVAL_SECS: u64 = 600;

/// Floor on that cadence: a walk and an API read are cheap, but a loop that
/// re-decides faster than the volume changes is just a busier way to be wrong.
pub const MIN_INTERVAL_SECS: u64 = 300;

/// How long a claim that refused the patch is left alone before it is tried
/// again. An API refusal is usually a property of the StorageClass and will not
/// change by asking again next round.
pub const REFUSAL_COOLDOWN_SECS: u64 = 3600;

/// The size to request for a claim that peaked at `peak_bytes` and currently
/// asks for `declared_bytes`, or `None` when the declaration already covers it.
///
/// Rounded up to a whole GiB: a request spelled in bytes reads like a
/// measurement rather than a declaration, and every quantity the deployment
/// declares for storage is a whole number of GiB.
pub fn growth_target(peak_bytes: u64, declared_bytes: u64) -> Option<u64> {
    if peak_bytes == 0 {
        return None;
    }
    let wanted = peak_bytes.saturating_mul(HEADROOM_PERCENT) / 100;
    let gib = 1024u64 * 1024 * 1024;
    let rounded = wanted.div_ceil(gib).saturating_mul(gib);
    if rounded > declared_bytes {
        Some(rounded)
    } else {
        None
    }
}

/// The request in force for one claim, as the cluster holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveClaim {
    pub name: String,
    pub declared: u64,
    /// What the volume has actually been given. Behind `declared` while an
    /// expansion is being carried out by whoever owns the StorageClass.
    pub capacity: u64,
}

/// Read `kubectl get pvc <name> -o json` into the two sizes this loop needs.
///
/// An unreadable object is an error rather than a zero: a claim whose request
/// could not be parsed must not read as asking for nothing, because the answer
/// to "does the declaration cover the peak" would then always be yes after one
/// successful patch.
pub fn parse_claim(json: &str) -> Result<LiveClaim, String> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("claim is not JSON: {e}"))?;
    let name = v
        .pointer("/metadata/name")
        .and_then(|s| s.as_str())
        .ok_or("claim carries no name")?
        .to_string();
    let storage = |p: &str| -> Result<u64, String> {
        let text = v
            .pointer(p)
            .and_then(|s| s.as_str())
            .ok_or_else(|| format!("claim {name} carries no {p}"))?;
        let bytes =
            quantity_bytes(text).ok_or_else(|| format!("{p} = {text} is not a quantity"))?;
        Ok(bytes as u64)
    };
    let declared = storage("/spec/resources/requests/storage")?;
    // A claim the scheduler has not bound has no status yet; it announces more
    // than it has been given, and this loop leaves an unbound claim to the
    // party that is about to bind it.
    let capacity = match v
        .pointer("/status/capacity/storage")
        .and_then(|s| s.as_str())
    {
        Some(text) => quantity_bytes(text).unwrap_or(declared as f64) as u64,
        None => declared,
    };
    Ok(LiveClaim {
        name,
        declared,
        capacity,
    })
}

/// Which of the two admission objects set the bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bound {
    LimitRange,
    Quota,
}

impl Bound {
    pub fn as_str(self) -> &'static str {
        match self {
            Bound::LimitRange => BOUND_LIMIT_RANGE,
            Bound::Quota => BOUND_QUOTA,
        }
    }
}

/// The largest request admission would accept for one claim, and which object
/// set that number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ceiling {
    pub bytes: u64,
    pub bound: Bound,
}

/// The largest request admission would accept for one claim, or `None` when the
/// cluster states no bound.
///
/// Two objects bound it and both are read here: the LimitRange's per-claim
/// maximum, and what the namespace quota has left once this claim's own current
/// request is taken back out of the total it is already counted in.
///
/// The tighter one is reported together with its name rather than as a bare
/// number: "above what admission accepts" does not say which of the two objects
/// has to move, and the operator reading a clamp had to compare the peak with
/// the declaration to guess. The choice is already made here -- it is this
/// function's whole job -- so it travels rather than being recomputed.
pub fn admission_ceiling(limit_range_max: Option<u64>, quota_free: Option<u64>) -> Option<Ceiling> {
    let out = match (limit_range_max, quota_free) {
        (Some(a), Some(b)) if a <= b => Ceiling {
            bytes: a,
            bound: Bound::LimitRange,
        },
        (Some(_), Some(b)) => Ceiling {
            bytes: b,
            bound: Bound::Quota,
        },
        (Some(a), None) => Ceiling {
            bytes: a,
            bound: Bound::LimitRange,
        },
        (None, Some(b)) => Ceiling {
            bytes: b,
            bound: Bound::Quota,
        },
        (None, None) => return None,
    };
    Some(out)
}

/// The per-claim storage maximum a LimitRange declares, if it declares one.
pub fn limit_range_max_storage(json: &str) -> Result<Option<u64>, String> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("limit ranges are not JSON: {e}"))?;
    let mut best: Option<u64> = None;
    let items = v
        .get("items")
        .and_then(|i| i.as_array())
        .ok_or("limit range list carries no items")?;
    for item in items {
        let limits = item
            .pointer("/spec/limits")
            .and_then(|l| l.as_array())
            .into_iter()
            .flatten();
        for limit in limits {
            if limit.get("type").and_then(|t| t.as_str()) != Some("PersistentVolumeClaim") {
                continue;
            }
            let Some(text) = limit.pointer("/max/storage").and_then(|s| s.as_str()) else {
                continue;
            };
            let Some(bytes) = quantity_bytes(text) else {
                return Err(format!("max.storage = {text} is not a quantity"));
            };
            let bytes = bytes as u64;
            best = Some(best.map_or(bytes, |b| b.min(bytes)));
        }
    }
    Ok(best)
}

/// The storage left under a namespace quota, given how much of it this claim
/// already accounts for.
///
/// `used` is the sum of every claim's request, this one included, so the room
/// this claim can grow into is `hard - used + its own current request`.
pub fn quota_free_storage(json: &str, own_request: u64) -> Result<Option<u64>, String> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("quota is not JSON: {e}"))?;
    let items = v
        .get("items")
        .and_then(|i| i.as_array())
        .ok_or("quota list carries no items")?;
    let mut free: Option<u64> = None;
    for item in items {
        let Some(hard) = item
            .pointer("/status/hard")
            .and_then(|h| h.get("requests.storage"))
        else {
            continue;
        };
        let used = item
            .pointer("/status/used/requests.storage")
            .and_then(|s| s.as_str())
            .unwrap_or("0");
        let Some(hard) = hard.as_str().and_then(quantity_bytes) else {
            return Err("quota hard requests.storage is not a quantity".into());
        };
        let Some(used) = quantity_bytes(used) else {
            return Err("quota used requests.storage is not a quantity".into());
        };
        let room = (hard - used).max(0.0) as u64 + own_request;
        free = Some(free.map_or(room, |f: u64| f.min(room)));
    }
    Ok(free)
}

/// This process's view of one claim's decision history.
struct ClaimState {
    peak: Vec<u64>,
    declared: Option<u64>,
    /// What the storage side has given back, as the claim's status reports it.
    granted: Option<u64>,
    /// The bound that stopped the last round, kept only while it is the current
    /// conclusion: a claim that grew or reached its target has no bound to name.
    ceiling: Option<Ceiling>,
    refused_until: Option<std::time::Instant>,
    /// What the last round concluded, so the account gets one row per change of
    /// state rather than one per tick. A claim that has been above the ceiling
    /// for a week is one fact about the deployment, not a thousand events; the
    /// round counters already say how long it has been going on.
    last_outcome: Option<CapacityOutcome>,
}

impl ClaimState {
    fn new() -> Self {
        Self {
            peak: Vec::new(),
            declared: None,
            granted: None,
            ceiling: None,
            refused_until: None,
            last_outcome: None,
        }
    }

    /// Records this round's conclusion and answers whether it is a fact worth
    /// writing down: a bound that moved, or an ask that was turned away, and
    /// only when that differs from what the previous round concluded. A round
    /// that decided to do nothing is not an event -- the account records what
    /// happened, and a row per quiet round would bury the rows that matter.
    fn note(&mut self, outcome: CapacityOutcome) -> bool {
        let changed = self.last_outcome != Some(outcome);
        self.last_outcome = Some(outcome);
        changed
            && matches!(
                outcome,
                CapacityOutcome::Grew | CapacityOutcome::Clamped | CapacityOutcome::Refused
            )
    }

    /// The same, for a conclusion that is never an event, returning it so the
    /// round can end on one line.
    fn quiet(&mut self, outcome: CapacityOutcome) -> CapacityOutcome {
        let _ = self.note(outcome);
        outcome
    }

    fn observe(&mut self, used: u64) -> u64 {
        self.peak.push(used);
        if self.peak.len() > PEAK_ROUNDS {
            self.peak.remove(0);
        }
        *self.peak.iter().max().unwrap_or(&used)
    }
}

/// The loop's readings: closed-set counters per claim, plus the two sizes the
/// decision was made from.
#[derive(Default)]
pub struct VolumeCapacityReadings {
    claims: Mutex<HashMap<String, Vec<AtomicU64>>>,
    declared: Mutex<HashMap<String, AtomicU64>>,
    peak: Mutex<HashMap<String, AtomicU64>>,
    granted: Mutex<HashMap<String, AtomicU64>>,
    ceiling: Mutex<HashMap<String, Ceiling>>,
}

impl VolumeCapacityReadings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one round's outcome for a claim, seeding the whole closed set the
    /// first time the claim is seen.
    ///
    /// The seeding is the point: a rule asking whether this loop ever clamped
    /// has to be able to tell "never" from "this claim is not watched", and
    /// only a zero on a series that exists can do that.
    pub fn record(&self, claim: &str, outcome: CapacityOutcome) {
        let mut claims = self.claims.lock().unwrap();
        let slots = claims
            .entry(claim.to_string())
            .or_insert_with(|| (0..OUTCOMES.len()).map(|_| AtomicU64::new(0)).collect());
        slots[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Publish the sizes the decision was read from. Zero bytes is a measured
    /// volume that is empty, so it is published as such rather than withheld.
    pub fn observe_sizes(&self, claim: &str, declared: u64, peak: u64, granted: u64) {
        for (table, value) in [
            (&self.declared, declared),
            (&self.peak, peak),
            (&self.granted, granted),
        ] {
            table
                .lock()
                .unwrap()
                .insert(claim.to_string(), AtomicU64::new(value));
        }
    }

    /// Publish the bound that stopped this claim, or take it back down.
    ///
    /// Withheld rather than published as a zero when the claim is no longer
    /// clamped: a zero bound is "admission allows nothing at all", which is a
    /// different and much louder statement than "nothing is in the way".
    pub fn observe_ceiling(&self, claim: &str, ceiling: Option<Ceiling>) {
        let mut table = self.ceiling.lock().unwrap();
        match ceiling {
            Some(ceiling) => {
                table.insert(claim.to_string(), ceiling);
            }
            None => {
                table.remove(claim);
            }
        }
    }

    pub fn outcome_count(&self, claim: &str, outcome: CapacityOutcome) -> u64 {
        self.claims
            .lock()
            .unwrap()
            .get(claim)
            .map(|slots| slots[outcome.index()].load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn claims(&self) -> Vec<String> {
        let mut names: Vec<String> = self.claims.lock().unwrap().keys().cloned().collect();
        names.sort();
        names
    }
}

#[async_trait]
impl Observable for VolumeCapacityReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = Vec::new();
        let claims = self.claims();
        for claim in claims {
            let counters = self.claims.lock().unwrap();
            let Some(slots) = counters.get(&claim) else {
                continue;
            };
            for (i, name) in OUTCOMES.iter().enumerate() {
                out.push(
                    RawMetric::new(OUTCOME_METRIC, slots[i].load(Ordering::Relaxed) as f64)
                        .with_label(CLAIM_LABEL, claim.clone())
                        .with_label(OUTCOME_LABEL, *name),
                );
            }
            drop(counters);
            for (state, table) in [
                (STATE_DECLARED, &self.declared),
                (STATE_PEAK, &self.peak),
                (STATE_GRANTED, &self.granted),
            ] {
                let value = table
                    .lock()
                    .unwrap()
                    .get(&claim)
                    .map(|v| v.load(Ordering::Relaxed) as f64);
                if let Some(value) = value {
                    out.push(
                        RawMetric::new(BYTES_METRIC, value)
                            .with_label(CLAIM_LABEL, claim.clone())
                            .with_label(STATE_LABEL, state),
                    );
                }
            }
            if let Some(ceiling) = self.ceiling.lock().unwrap().get(&claim).copied() {
                out.push(
                    RawMetric::new(CEILING_METRIC, ceiling.bytes as f64)
                        .with_label(CLAIM_LABEL, claim.clone())
                        .with_label(BOUND_LABEL, ceiling.bound.as_str()),
                );
            }
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// What one round needs to read from the cluster.
#[derive(Debug, Clone)]
pub struct ClaimTarget {
    pub claim: String,
    pub dir: PathBuf,
    pub exclude: Vec<PathBuf>,
}

/// The `claim=path` list this process both mounts and measures.
///
/// The application directory is named separately from the mount list in the
/// deployment, so both are read: a process that declares only one of them is
/// still a process whose volume is measured, and the one it declared is the one
/// this loop may raise.
pub fn targets_from_env() -> Vec<ClaimTarget> {
    let mounts = match std::env::var(claim_footprint::MOUNTS_ENV) {
        Ok(raw) if !raw.trim().is_empty() => match claim_footprint::parse_claim_paths(&raw) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "volume capacity: mount declaration unreadable; no claim is acted on");
                Vec::new()
            }
        },
        _ => Vec::new(),
    };
    if let Some(claim) = std::env::var(claim_footprint::CLAIM_ENV)
        .ok()
        .filter(|c| !c.trim().is_empty())
    {
        // The application directory's claim is declared without its path: the
        // path is the configured data dir, which this process may not mount at
        // the same place as the deployment that names the claim.
        if !mounts.iter().any(|m| m.claim == claim) {
            warn!(
                claim = %claim,
                "volume capacity: the claim behind the data directory has no mounted path \
                 in this process; only claims declared with a path can be measured and raised"
            );
        }
    }
    let all = claim_footprint::current_mounts();
    mounts
        .into_iter()
        .map(|m| {
            let dir = PathBuf::from(&m.path);
            let exclude = claim_footprint::nested_mounts_under(&dir, &all);
            ClaimTarget {
                claim: m.claim,
                dir,
                exclude,
            }
        })
        .collect()
}

async fn kubectl(args: &[&str], timeout_secs: u64) -> Result<String, String> {
    let out = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        tokio::process::Command::new("kubectl").args(args).output(),
    )
    .await
    .map_err(|_| format!("kubectl {args:?} timed out after {timeout_secs}s"))?
    .map_err(|e| format!("kubectl {args:?} could not run: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "kubectl {args:?} exited {}: {}",
            out.status.code().unwrap_or(-1),
            stderr.trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn namespace() -> String {
    std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "cogneva".to_string())
}

/// This loop's name in the process liveness registry, and the value of
/// `cogneva_loop_tick_age_seconds{loop=...}`.
pub const LOOP: &str = "volume_capacity";

/// The name this loop writes its decisions under, both as the account's
/// `subject` prefix and in the log.
pub const SUBJECT_PREFIX: &str = "volume_capacity.";

/// The reading a decision in this loop was made on.
pub const READING: &str = BYTES_METRIC;

/// The account's row for one round, or `None` for a round that changed nothing.
///
/// Only three conclusions are facts about the deployment: a request that was
/// raised, a request the admission ceiling would not let through, and a request
/// the API refused. The other three are the absence of one -- `at_target`,
/// `expanding` and `unmeasured` are read from the round counters, which is where
/// a reader asking "how is this volume doing" looks anyway.
///
/// `to_value` is `None` on a refusal because nothing moved. Writing the size it
/// wanted there would let a reader of the table add up a bound that was never
/// asked for; the size it wanted and the wall it hit are in `detail`, where a
/// reader who wants the decision rather than the state goes.
pub fn capacity_event(
    target: &ClaimTarget,
    actor: &str,
    peak: u64,
    declared: u64,
    wanted: Option<u64>,
    ceiling: Option<u64>,
    outcome: CapacityOutcome,
) -> Option<ResourceEvent> {
    let (kind, ended, subject) = match outcome {
        CapacityOutcome::Grew => (
            ResourceEventKind::BoundChange,
            ResourceEventOutcome::Applied,
            format!("{SUBJECT_PREFIX}{}", target.claim),
        ),
        CapacityOutcome::Clamped => (
            ResourceEventKind::AdmissionRefusal,
            ResourceEventOutcome::Refused,
            format!("{SUBJECT_PREFIX}{}@admission_ceiling", target.claim),
        ),
        CapacityOutcome::Refused => (
            ResourceEventKind::AdmissionRefusal,
            ResourceEventOutcome::Refused,
            format!("{SUBJECT_PREFIX}{}@api", target.claim),
        ),
        CapacityOutcome::AtTarget | CapacityOutcome::Expanding | CapacityOutcome::Unmeasured => {
            return None
        }
    };
    let as_i64 = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
    Some(ResourceEvent {
        kind,
        occurred_at: chrono::Utc::now(),
        subject,
        actor: actor.to_string(),
        reading: READING.to_string(),
        reading_value: Some(peak as f64),
        from_value: Some(as_i64(declared)),
        to_value: wanted
            .map(as_i64)
            .filter(|_| outcome == CapacityOutcome::Grew),
        outcome: ended,
        detail: serde_json::json!({
            "claim": target.claim,
            "dir": target.dir.display().to_string(),
            "peak_bytes": peak,
            "declared_bytes": declared,
            "wanted_bytes": wanted,
            "ceiling_bytes": ceiling,
            "outcome": outcome.as_str(),
        }),
        count: 1,
    })
}

/// Hands one event to the resource account's carrier in this process.
///
/// The carrier is the build gate's ring, which only exists where a gate is in
/// force. That is the same process that spawns this loop, so an answer of
/// `false` is a defect rather than a configuration: the decision happened and
/// nowhere will hold it. It is said in the log rather than dropped quietly.
fn report(event: ResourceEvent) {
    if !cog_core::contract::build_gate::record_resource_event(event) {
        warn!(
            "volume capacity: this process keeps no resource-event ring, so a decision it just \
             made was not written to the account -- it is only in the log line beside this"
        );
    }
}

/// Records a round's conclusion and writes it to the account when it is one of
/// the three that are facts. Returns the conclusion so the caller can return it.
#[allow(clippy::too_many_arguments)]
fn settle(
    state: &mut ClaimState,
    target: &ClaimTarget,
    peak: u64,
    declared: u64,
    wanted: Option<u64>,
    ceiling: Option<u64>,
    outcome: CapacityOutcome,
) -> CapacityOutcome {
    if state.note(outcome) {
        if let Some(event) = capacity_event(
            target,
            &crate::build_gate::gate_actor(),
            peak,
            declared,
            wanted,
            ceiling,
            outcome,
        ) {
            report(event);
        }
    } else {
        debug!(
            claim = %target.claim,
            outcome = outcome.as_str(),
            "volume capacity: unchanged conclusion, not written to the account again"
        );
    }
    outcome
}

/// One full decision for one claim.
async fn reconcile(target: &ClaimTarget, state: &mut ClaimState) -> CapacityOutcome {
    let footprint = ClaimFootprint::new(
        target.claim.clone(),
        target.dir.clone(),
        target.exclude.clone(),
    );
    let walked =
        tokio::task::spawn_blocking(move || footprint.measure().map(|_| footprint.value())).await;
    let used = match walked {
        Ok(Ok(Some(bytes))) => bytes,
        Ok(Ok(None)) | Ok(Err(_)) | Err(_) => return state.quiet(CapacityOutcome::Unmeasured),
    };
    let peak = state.observe(used);

    let ns = namespace();
    let json = match kubectl(&["get", "pvc", &target.claim, "-n", &ns, "-o", "json"], 60).await {
        Ok(json) => json,
        Err(e) => {
            warn!(claim = %target.claim, error = %e, "volume capacity: claim unreadable");
            return state.quiet(CapacityOutcome::Unmeasured);
        }
    };
    let live = match parse_claim(&json) {
        Ok(live) => live,
        Err(e) => {
            warn!(claim = %target.claim, error = %e, "volume capacity: claim unparsable");
            return state.quiet(CapacityOutcome::Unmeasured);
        }
    };
    state.declared = Some(live.declared);
    state.granted = Some(live.capacity);
    // The bound is a conclusion about the round that just ended, so it is taken
    // back down here and set again only if this round is clamped. A claim that
    // stopped being clamped must not keep naming a wall it no longer stands at.
    state.ceiling = None;

    // An expansion that has been asked for and not yet carried out: the request
    // is already ahead of the capacity, and a second patch while the class is
    // still working would be refused by the API.
    if live.capacity < live.declared {
        return state.quiet(CapacityOutcome::Expanding);
    }

    let Some(wanted) = growth_target(peak, live.declared) else {
        return state.quiet(CapacityOutcome::AtTarget);
    };

    if let Some(until) = state.refused_until {
        if std::time::Instant::now() < until {
            return settle(
                state,
                target,
                peak,
                live.declared,
                Some(wanted),
                None,
                CapacityOutcome::Refused,
            );
        }
    }

    let ceiling = read_ceiling(&ns, live.declared).await;
    if let Some(ceiling) = ceiling {
        if wanted > ceiling.bytes {
            warn!(
                claim = %target.claim,
                wanted,
                ceiling = ceiling.bytes,
                bound = ceiling.bound.as_str(),
                "volume capacity: the size this volume needs is above what admission allows"
            );
            state.ceiling = Some(ceiling);
            return settle(
                state,
                target,
                peak,
                live.declared,
                Some(wanted),
                Some(ceiling.bytes),
                CapacityOutcome::Clamped,
            );
        }
    }

    let patch = format!(
        "{{\"spec\":{{\"resources\":{{\"requests\":{{\"storage\":\"{}\"}}}}}}}}",
        wanted
    );
    match kubectl(
        &[
            "patch",
            "pvc",
            &target.claim,
            "-n",
            &ns,
            "--type=merge",
            "-p",
            &patch,
        ],
        60,
    )
    .await
    {
        Ok(_) => {
            info!(
                claim = %target.claim,
                from = live.declared,
                to = wanted,
                peak,
                "volume capacity: request raised to cover the measured peak"
            );
            settle(
                state,
                target,
                peak,
                live.declared,
                Some(wanted),
                ceiling.map(|c| c.bytes),
                CapacityOutcome::Grew,
            )
        }
        Err(e) => {
            warn!(claim = %target.claim, wanted, error = %e, "volume capacity: patch refused");
            state.refused_until =
                Some(std::time::Instant::now() + Duration::from_secs(REFUSAL_COOLDOWN_SECS));
            settle(
                state,
                target,
                peak,
                live.declared,
                Some(wanted),
                ceiling.map(|c| c.bytes),
                CapacityOutcome::Refused,
            )
        }
    }
}

/// The bound admission would enforce, read fresh each time it is needed: the
/// quota moves as other claims are created, and a cached ceiling would let this
/// loop write a request the namespace can no longer accept.
async fn read_ceiling(ns: &str, own_request: u64) -> Option<Ceiling> {
    let limit = kubectl(&["get", "limitrange", "-n", ns, "-o", "json"], 60)
        .await
        .ok()
        .and_then(|json| match limit_range_max_storage(&json) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "volume capacity: limit range unreadable; ceiling unknown");
                None
            }
        });
    let quota = kubectl(&["get", "resourcequota", "-n", ns, "-o", "json"], 60)
        .await
        .ok()
        .and_then(|json| match quota_free_storage(&json, own_request) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "volume capacity: quota unreadable; ceiling unknown");
                None
            }
        });
    admission_ceiling(limit, quota)
}

/// Run the loop until shutdown.
///
/// Spawned only by the process that builds, because that is the process that
/// mounts and measures these volumes; a control plane raising requests on
/// claims it cannot walk would be deciding from a criterion it does not hold.
pub fn spawn_volume_capacity_loop(
    readings: std::sync::Arc<VolumeCapacityReadings>,
    shutdown: ShutdownSignal,
) -> Option<tokio::task::JoinHandle<()>> {
    let targets = targets_from_env();
    if targets.is_empty() {
        info!(
            "volume capacity: no mounted claim is declared for measurement; \
             no claim is acted on by this process"
        );
        return None;
    }
    let interval = std::env::var("COGNEVA_VOLUME_CAPACITY_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_INTERVAL_SECS)
        .max(MIN_INTERVAL_SECS);
    // Registered, not merely spawned: every number this loop publishes is a
    // measurement it took, so a task that died would leave a reading that says
    // the declaration is still fine rather than one that says nothing is being
    // measured. The liveness has to come from outside the loop for that reason.
    Some(cog_core::loop_health::spawn(
        LOOP,
        cog_core::loop_health::Cadence::Periodic(Duration::from_secs(interval)),
        shutdown.clone(),
        move |beat| {
            let shutdown = shutdown.clone();
            let readings = readings.clone();
            let targets = targets.clone();
            async move {
                let mut states: HashMap<String, ClaimState> = HashMap::new();
                let mut ticker = tokio::time::interval(Duration::from_secs(interval));
                loop {
                    // One stamp per pass, whatever the pass measured: a volume
                    // that did not grow is not a loop that stopped.
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    for target in &targets {
                        let state = states
                            .entry(target.claim.clone())
                            .or_insert_with(ClaimState::new);
                        let outcome = reconcile(target, state).await;
                        if let Some(declared) = state.declared {
                            let peak = state.peak.iter().copied().max().unwrap_or(0);
                            readings.observe_sizes(
                                &target.claim,
                                declared,
                                peak,
                                state.granted.unwrap_or(declared),
                            );
                        }
                        readings.observe_ceiling(&target.claim, state.ceiling);
                        readings.record(&target.claim, outcome);
                    }
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    fn claim_json(request: &str, capacity: &str) -> String {
        format!(
            r#"{{"metadata":{{"name":"cogneva-evolution-source-pvc"}},
                "spec":{{"resources":{{"requests":{{"storage":"{request}"}}}}}},
                "status":{{"capacity":{{"storage":"{capacity}"}}}}}}"#
        )
    }

    #[test]
    fn a_declaration_that_covers_the_peak_with_headroom_is_left_alone() {
        // 100Gi declared, peak 80Gi: 80 * 1.2 = 96 < 100.
        assert_eq!(growth_target(80 * GIB, 100 * GIB), None);
        // Exactly at the headroom the two are equal, and equality is not a
        // reason to write.
        assert_eq!(growth_target(100 * GIB, 120 * GIB), None);
    }

    #[test]
    fn a_declaration_below_the_peak_is_raised_to_the_headroom_rounded_up() {
        // 149GiB peak, 72Gi declared: 149 * 1.2 = 178.8 -> 179Gi.
        assert_eq!(growth_target(149 * GIB, 72 * GIB), Some(179 * GIB));
        // A peak that lands exactly on a GiB boundary is not rounded past it.
        assert_eq!(growth_target(10 * GIB, 5 * GIB), Some(12 * GIB));
    }

    #[test]
    fn an_empty_volume_never_asks_for_more() {
        assert_eq!(growth_target(0, 10 * GIB), None);
    }

    #[test]
    fn a_claim_reads_both_sizes_and_a_missing_status_is_not_a_shrink_claim() {
        let live = parse_claim(&claim_json("192Gi", "192Gi")).unwrap();
        assert_eq!(live.declared, 192 * GIB);
        assert_eq!(live.capacity, 192 * GIB);
        // Unbound: status has no capacity, so the claim is not read as having
        // been given less than it announced.
        let unbound =
            r#"{"metadata":{"name":"x"},"spec":{"resources":{"requests":{"storage":"10Gi"}}}}"#;
        let live = parse_claim(unbound).unwrap();
        assert_eq!(live.declared, 10 * GIB);
        assert_eq!(live.capacity, 10 * GIB);
    }

    #[test]
    fn an_unparsable_request_is_an_error_rather_than_zero() {
        assert!(parse_claim(&claim_json("lots", "1Gi")).is_err());
        assert!(parse_claim("not json").is_err());
    }

    #[test]
    fn the_ceiling_is_the_tighter_of_the_two_admission_bounds() {
        let tighter = |bytes, bound| Some(Ceiling { bytes, bound });
        assert_eq!(
            admission_ceiling(Some(200), Some(480)),
            tighter(200, Bound::LimitRange)
        );
        assert_eq!(
            admission_ceiling(Some(480), Some(200)),
            tighter(200, Bound::Quota)
        );
        assert_eq!(
            admission_ceiling(Some(200), None),
            tighter(200, Bound::LimitRange)
        );
        assert_eq!(
            admission_ceiling(None, Some(480)),
            tighter(480, Bound::Quota)
        );
        assert_eq!(admission_ceiling(None, None), None);
    }

    #[test]
    fn a_bound_named_apart_from_its_size_reaches_the_reading() {
        // The reading carries the name of the object that has to move, because
        // "above what admission accepts" does not say which one it is. The
        // series is withheld once the claim stops being clamped: a zero here
        // would mean admission allows nothing, not that nothing is in the way.
        let readings = VolumeCapacityReadings::new();
        readings.record("cogneva-evolution-source-pvc", CapacityOutcome::Clamped);
        readings.observe_sizes(
            "cogneva-evolution-source-pvc",
            192 * GIB,
            149 * GIB,
            72 * GIB,
        );
        readings.observe_ceiling(
            "cogneva-evolution-source-pvc",
            admission_ceiling(Some(200 * GIB), Some(90 * GIB)),
        );
        let metrics = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(readings.collect_metrics(""));
        let metrics = metrics.unwrap();
        let ceiling = metrics
            .iter()
            .find(|m| m.name == CEILING_METRIC)
            .expect("a clamped claim names its bound");
        assert_eq!(
            ceiling.labels.get(BOUND_LABEL).map(String::as_str),
            Some(BOUND_QUOTA)
        );
        assert_eq!(ceiling.value, (90 * GIB) as f64);

        let granted = metrics
            .iter()
            .find(|m| {
                m.name == BYTES_METRIC
                    && m.labels.get(STATE_LABEL).map(String::as_str) == Some(STATE_GRANTED)
            })
            .expect("the size the storage side gave back is published");
        assert_eq!(granted.value, (72 * GIB) as f64);

        readings.observe_ceiling("cogneva-evolution-source-pvc", None);
        let metrics = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(readings.collect_metrics(""))
            .unwrap();
        assert!(metrics.iter().all(|m| m.name != CEILING_METRIC));
    }

    #[test]
    fn a_limit_range_carries_its_pvc_maximum_and_ignores_other_types() {
        let json = r#"{"items":[{"spec":{"limits":[
            {"type":"Container","max":{"cpu":"2","memory":"4Gi"}},
            {"type":"PersistentVolumeClaim","max":{"storage":"200Gi"}}]}}]}"#;
        assert_eq!(limit_range_max_storage(json).unwrap(), Some(200 * GIB));
        let none = r#"{"items":[{"spec":{"limits":[{"type":"Container","max":{"cpu":"2"}}]}}]}"#;
        assert_eq!(limit_range_max_storage(none).unwrap(), None);
    }

    #[test]
    fn quota_room_counts_this_claims_own_request_back_in() {
        // 480Gi hard, 149Gi used of which this claim is 72Gi: it may ask for
        // up to 480 - 149 + 72 = 403Gi.
        let json = r#"{"items":[{"status":{
            "hard":{"requests.storage":"480Gi"},
            "used":{"requests.storage":"149Gi"}}}]}"#;
        assert_eq!(quota_free_storage(json, 72 * GIB).unwrap(), Some(403 * GIB));
    }

    #[test]
    fn every_outcome_has_its_own_slot_and_a_name() {
        for (i, outcome) in [
            CapacityOutcome::AtTarget,
            CapacityOutcome::Grew,
            CapacityOutcome::Clamped,
            CapacityOutcome::Refused,
            CapacityOutcome::Expanding,
            CapacityOutcome::Unmeasured,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(outcome.index(), i);
            assert_eq!(OUTCOMES[i], outcome.as_str());
        }
    }

    #[test]
    fn a_claim_seeds_the_whole_outcome_set_on_its_first_round() {
        let readings = VolumeCapacityReadings::new();
        readings.record("cogneva-evolution-source-pvc", CapacityOutcome::AtTarget);
        for outcome in OUTCOMES {
            let _ = outcome;
        }
        for o in [
            CapacityOutcome::AtTarget,
            CapacityOutcome::Grew,
            CapacityOutcome::Clamped,
            CapacityOutcome::Refused,
            CapacityOutcome::Expanding,
            CapacityOutcome::Unmeasured,
        ] {
            let expected = if o == CapacityOutcome::AtTarget { 1 } else { 0 };
            assert_eq!(
                readings.outcome_count("cogneva-evolution-source-pvc", o),
                expected
            );
        }
    }

    fn target() -> ClaimTarget {
        ClaimTarget {
            claim: "cogneva-evolution-source-pvc".to_string(),
            dir: PathBuf::from("/data"),
            exclude: Vec::new(),
        }
    }

    /// A raised request is a bound that moved: `from` is what the cluster held,
    /// `to` is what it holds now, and the subject names the claim rather than a
    /// wall, because no wall refused anything.
    #[test]
    fn a_raised_request_is_a_bound_change_from_the_old_declaration_to_the_new() {
        let event = capacity_event(
            &target(),
            "pod-a",
            GIB * 80,
            GIB * 64,
            Some(GIB * 96),
            Some(GIB * 200),
            CapacityOutcome::Grew,
        )
        .expect("a raised request is an event");
        assert_eq!(event.kind, ResourceEventKind::BoundChange);
        assert_eq!(event.outcome, ResourceEventOutcome::Applied);
        assert_eq!(
            event.subject,
            "volume_capacity.cogneva-evolution-source-pvc"
        );
        assert_eq!(event.reading, READING);
        assert_eq!(event.reading_value, Some((GIB * 80) as f64));
        assert_eq!(event.from_value, Some((GIB * 64) as i64));
        assert_eq!(event.to_value, Some((GIB * 96) as i64));
        assert_eq!(event.actor, "pod-a");
        assert_eq!(event.detail["peak_bytes"], (GIB * 80) as i64);
    }

    /// A refusal has no "after": writing the size it wanted into `to_value`
    /// would let a reader add up a bound that was never asked for.
    #[test]
    fn a_clamped_round_names_the_wall_and_claims_no_move() {
        let event = capacity_event(
            &target(),
            "pod-a",
            GIB * 180,
            GIB * 64,
            Some(GIB * 216),
            Some(GIB * 200),
            CapacityOutcome::Clamped,
        )
        .expect("a clamp is an event");
        assert_eq!(event.kind, ResourceEventKind::AdmissionRefusal);
        assert_eq!(event.outcome, ResourceEventOutcome::Refused);
        assert!(
            event.subject.ends_with("@admission_ceiling"),
            "the subject must name the wall that answered: {}",
            event.subject
        );
        assert_eq!(event.to_value, None, "a refusal claimed a move");
        assert_eq!(event.detail["wanted_bytes"], (GIB * 216) as i64);
        assert_eq!(event.detail["ceiling_bytes"], (GIB * 200) as i64);
    }

    /// The three quiet conclusions are the absence of an event, and each has to
    /// say so on its own: a test that only checked one would pass a builder
    /// that answered `None` for the wrong reason.
    #[test]
    fn a_round_that_changed_nothing_writes_no_row() {
        for outcome in [
            CapacityOutcome::AtTarget,
            CapacityOutcome::Expanding,
            CapacityOutcome::Unmeasured,
        ] {
            assert!(
                capacity_event(&target(), "pod-a", 1, 1, None, None, outcome).is_none(),
                "{outcome:?} was written to the account"
            );
        }
    }

    /// One fact, one row: a claim that stays above the ceiling for a week is a
    /// single event, and the round counters carry how long it has lasted.
    #[test]
    fn a_standing_clamp_is_written_once_and_a_new_one_after_it_clears() {
        let mut state = ClaimState::new();
        assert!(
            state.note(CapacityOutcome::Clamped),
            "the first clamp was not reported"
        );
        assert!(
            !state.note(CapacityOutcome::Clamped),
            "an unchanged clamp was reported again"
        );
        // It clears, then clamps again at a larger size: a second fact.
        assert!(!state.note(CapacityOutcome::AtTarget));
        assert!(
            state.note(CapacityOutcome::Clamped),
            "a clamp that cleared and came back was not reported"
        );
    }

    /// A patch the API refused is a fact, and a quiet round after it is not.
    #[test]
    fn a_refusal_is_written_on_entry_and_not_on_every_round_of_the_cooldown() {
        let mut state = ClaimState::new();
        assert!(state.note(CapacityOutcome::Refused));
        for _ in 0..10 {
            assert!(!state.note(CapacityOutcome::Refused));
        }
    }

    #[test]
    fn the_peak_window_forgets_an_older_spike() {
        let mut state = ClaimState::new();
        state.observe(10);
        for _ in 0..PEAK_ROUNDS {
            state.observe(1);
        }
        // The oldest sample has rolled out of the window, so the spike no
        // longer sizes a request.
        assert_eq!(*state.peak.iter().max().unwrap(), 1);
    }
}
