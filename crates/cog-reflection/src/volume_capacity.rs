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
use cog_core::{SFResult, ShutdownSignal};
use tracing::{info, warn};

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

/// The largest request admission would accept for one claim, or `None` when the
/// cluster states no bound.
///
/// Two objects bound it and both are read here: the LimitRange's per-claim
/// maximum, and what the namespace quota has left once this claim's own current
/// request is taken back out of the total it is already counted in.
pub fn admission_ceiling(limit_range_max: Option<u64>, quota_free: Option<u64>) -> Option<u64> {
    match (limit_range_max, quota_free) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
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
    refused_until: Option<std::time::Instant>,
}

impl ClaimState {
    fn new() -> Self {
        Self {
            peak: Vec::new(),
            declared: None,
            refused_until: None,
        }
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
    pub fn observe_sizes(&self, claim: &str, declared: u64, peak: u64) {
        self.declared
            .lock()
            .unwrap()
            .insert(claim.to_string(), AtomicU64::new(declared));
        self.peak
            .lock()
            .unwrap()
            .insert(claim.to_string(), AtomicU64::new(peak));
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
            for (state, table) in [(STATE_DECLARED, &self.declared), (STATE_PEAK, &self.peak)] {
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
    let mut mounts = match std::env::var(claim_footprint::MOUNTS_ENV) {
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
    let all: Vec<PathBuf> = mounts.iter().map(|m| PathBuf::from(&m.path)).collect();
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
        Ok(Ok(None)) | Ok(Err(_)) | Err(_) => return CapacityOutcome::Unmeasured,
    };
    let peak = state.observe(used);

    let ns = namespace();
    let json = match kubectl(&["get", "pvc", &target.claim, "-n", &ns, "-o", "json"], 60).await {
        Ok(json) => json,
        Err(e) => {
            warn!(claim = %target.claim, error = %e, "volume capacity: claim unreadable");
            return CapacityOutcome::Unmeasured;
        }
    };
    let live = match parse_claim(&json) {
        Ok(live) => live,
        Err(e) => {
            warn!(claim = %target.claim, error = %e, "volume capacity: claim unparsable");
            return CapacityOutcome::Unmeasured;
        }
    };
    state.declared = Some(live.declared);

    // An expansion that has been asked for and not yet carried out: the request
    // is already ahead of the capacity, and a second patch while the class is
    // still working would be refused by the API.
    if live.capacity < live.declared {
        return CapacityOutcome::Expanding;
    }

    let Some(wanted) = growth_target(peak, live.declared) else {
        return CapacityOutcome::AtTarget;
    };

    if let Some(until) = state.refused_until {
        if std::time::Instant::now() < until {
            return CapacityOutcome::Refused;
        }
    }

    let ceiling = read_ceiling(&ns, live.declared).await;
    if let Some(ceiling) = ceiling {
        if wanted > ceiling {
            warn!(
                claim = %target.claim,
                wanted,
                ceiling,
                "volume capacity: the size this volume needs is above what admission allows"
            );
            return CapacityOutcome::Clamped;
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
            CapacityOutcome::Grew
        }
        Err(e) => {
            warn!(claim = %target.claim, wanted, error = %e, "volume capacity: patch refused");
            state.refused_until =
                Some(std::time::Instant::now() + Duration::from_secs(REFUSAL_COOLDOWN_SECS));
            CapacityOutcome::Refused
        }
    }
}

/// The bound admission would enforce, read fresh each time it is needed: the
/// quota moves as other claims are created, and a cached ceiling would let this
/// loop write a request the namespace can no longer accept.
async fn read_ceiling(ns: &str, own_request: u64) -> Option<u64> {
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
    Some(tokio::spawn(async move {
        let mut states: HashMap<String, ClaimState> = HashMap::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(interval));
        loop {
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
                    readings.observe_sizes(&target.claim, declared, peak);
                }
                readings.record(&target.claim, outcome);
            }
        }
    }))
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
        assert_eq!(admission_ceiling(Some(200), Some(480)), Some(200));
        assert_eq!(admission_ceiling(Some(200), None), Some(200));
        assert_eq!(admission_ceiling(None, Some(480)), Some(480));
        assert_eq!(admission_ceiling(None, None), None);
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
