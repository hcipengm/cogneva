//! The reading a build cache's size travels under, and what its labels mean.
//!
//! Two processes publish this family: the one that runs a deployment's own
//! builds, and the sandbox executor, whose `CARGO_TARGET_DIR` lands on a volume
//! of its own. They cannot depend on each other, and one cache reported under
//! two spellings would let a panel and a rule each see half of it -- so the
//! names, the labels and the layer depth are stated once, here, and both
//! publishers read them from here rather than writing their own.
//!
//! The `dir` label carries a `dev:ino` identity rather than a path, which is
//! what makes two caches behind one path two series instead of one sum. See
//! [`crate::build_gate::dir_identity`].
//!
//! What each series says, and the rules that read them, are documented where
//! they are produced: the cap family's semantics are the ones a reclaim pass
//! reports on.

/// Apparent bytes the cache holds, per layer.
pub const BUILD_TARGET_BYTES_METRIC: &str = "cogneva_build_target_bytes";

/// Seconds since the cache was last measured.
pub const BUILD_TARGET_SCAN_AGE_METRIC: &str = "cogneva_build_target_bytes_scan_age_seconds";

/// The cap the cache is held to, in bytes. Published only when one is set.
pub const BUILD_TARGET_CAP_METRIC: &str = "cogneva_build_target_bytes_cap";

/// Bytes the cache is above its cap, as of the last walk.
pub const BUILD_TARGET_OVER_LIMIT_METRIC: &str = "cogneva_build_target_over_limit_bytes";

/// Bytes still above the cap after the last pass that ran.
pub const BUILD_TARGET_UNMET_METRIC: &str = "cogneva_build_target_unmet_bytes";

/// Walks that found the cache over its cap, by what happened next.
pub const BUILD_TARGET_OVER_CAP_METRIC: &str = "cogneva_build_target_over_cap_total";

/// The label naming what a pass did, or why it did not run.
pub const OUTCOME_LABEL: &str = "outcome";

/// A pass ran and brought the cache under its cap.
pub const OUTCOME_RECLAIMED: &str = "reclaimed";

/// A pass ran and the cache stayed above its cap.
pub const OUTCOME_UNMET: &str = "unmet";

/// A build held the only build slot, so no pass ran.
pub const OUTCOME_BUSY: &str = "busy";

/// No build gate is in force, so nothing may be removed.
pub const OUTCOME_UNGATED: &str = "ungated";

/// This loop's name in the liveness census.
pub const BUILD_CACHE_WATCH_LOOP: &str = "build_cache_watch";

/// Every value the outcome label takes, so a reader can see the whole domain
/// with zeros rather than inferring it from whichever values happened to occur.
pub const RECLAIM_OUTCOMES: &[&str] = &[
    OUTCOME_RECLAIMED,
    OUTCOME_UNMET,
    OUTCOME_BUSY,
    OUTCOME_UNGATED,
];

/// Bytes removed from the cache by this process so far.
pub const BUILD_TARGET_RECLAIMED_METRIC: &str = "cogneva_build_target_reclaimed_bytes_total";

/// When a reclamation pass last ran, in unix seconds.
pub const BUILD_TARGET_LAST_RECLAIM_METRIC: &str = "cogneva_build_target_last_reclaim_seconds";

/// The configured scan interval, so a rule can say how long is too long without
/// carrying a copy of the interval that goes stale when it is configured.
pub const BUILD_TARGET_SCAN_INTERVAL_METRIC: &str = "cogneva_build_target_scan_interval_seconds";

/// The layer label.
pub const LAYER_LABEL: &str = "layer";

/// The label naming the directory the reading belongs to.
///
/// `dev:ino` rather than the path, for the same reason the build gate publishes
/// it: the same path is mounted from different volumes in different workloads,
/// and two caches behind one path would otherwise be summed into one number with
/// nothing to say they are two.
pub const DIR_LABEL: &str = "dir";

/// The layer carrying everything past [`MAX_PUBLISHED_LAYERS`].
pub const OTHER_LAYER: &str = "other";

/// How deep into the cache a layer is taken from.
///
/// Two: one level is a cargo profile (`debug`, `release`), which says nothing
/// about what can be dropped, and cargo keeps its own division one level below
/// that (`deps`, `incremental`, `build`, `.fingerprint`, `examples`).
pub const CACHE_LAYER_DEPTH: usize = 2;

/// How many layers get a series of their own.
///
/// The layer name comes from a directory in the cache, and a directory name is
/// data that builds write, so the domain is not closed by construction. The cap
/// is on the published side and the fold is by size: the layers a reader would
/// act on keep their names, the rest sum into [`OTHER_LAYER`] so the total stays
/// exact, and a reader sees the fold as that series growing. Cargo's own layout
/// is well under this, so in practice nothing is folded.
pub const MAX_PUBLISHED_LAYERS: usize = 12;

/// Fastest the cache may be re-walked at. The walk is metadata-only, but it
/// walks a tree with hundreds of thousands of entries on a host that is also
/// building, so it holds no value being fresher than minutes.
pub const MIN_SCAN_INTERVAL_SECS: u64 = 60;

/// The layers to publish, largest first, with everything past
/// [`MAX_PUBLISHED_LAYERS`] folded into [`OTHER_LAYER`].
///
/// Largest first, then by name, so two layers of equal size do not swap series
/// between scrapes. The folded series is always published, zero included: a
/// reader has to be able to tell "no layer was folded" from "this series is not
/// wired up".
pub fn published_layers(layers: &std::collections::BTreeMap<String, u64>) -> Vec<(String, u64)> {
    let mut ordered: Vec<(&String, u64)> = layers.iter().map(|(k, v)| (k, *v)).collect();
    ordered.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

    let mut out = Vec::with_capacity(ordered.len().min(MAX_PUBLISHED_LAYERS) + 1);
    let mut folded = 0u64;
    for (index, (layer, bytes)) in ordered.into_iter().enumerate() {
        if index < MAX_PUBLISHED_LAYERS {
            out.push((layer.clone(), bytes));
        } else {
            folded = folded.saturating_add(bytes);
        }
    }
    out.push((OTHER_LAYER.to_string(), folded));
    out
}
