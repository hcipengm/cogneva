//! The build cache this executor's own commands write into, and what bounds it.
//!
//! Every command gets `CARGO_TARGET_DIR` pointed at one directory on the pod's
//! volume, so the tasks share a warm cache instead of compiling the same
//! dependencies once per task. That sharing is deliberate -- it is what makes a
//! task's first build affordable -- and its cost is that the directory only ever
//! grows. It shares a volume with the task worktrees, and it is the largest
//! thing this pod writes, so what a full disk turns into is a build that fails
//! for a reason that says nothing about the cache. Nothing here is new
//! pressure: the same pressure existed with nothing measuring it.
//!
//! The readings, the layer split and the order files are given up in are the
//! shared ones (`cogneva_build_target_*`): the process that builds a
//! deployment's changes keeps a cache in the same shape, and one cache reported
//! under two spellings -- or emptied by two sets of rules -- would leave a
//! panel and a rule each reading half of it.
//!
//! **What is not shared is the fact that says whether a build is running.** That
//! process holds the host-wide build gate; this one holds no slot in it and
//! cannot take one, because the slot directory lives on a host path this pod
//! does not mount. Here the fact is this router's own: a command holds a shared
//! claim on [`CommandSlot`] for as long as its child may live, and a reclaim
//! pass takes it exclusively, so "no build is running" is answered by the
//! commands this process is running rather than assumed. One consequence is
//! worth stating, because the outcome counts show it: `busy` here means this
//! executor was running a command, not that the host was busy.
//!
//! **The default holds the cache to a cap rather than only measuring it.** The
//! executor's volume is the deployment's own 5 GiB budget, shared with the
//! task worktrees, so a safe bound is derivable here instead of remade per
//! deployment: 5 GiB less the co-tenants leaves 3 GiB for the cache
//! ([`DEFAULT_MAX_BYTES`]), which keeps the cache below the volume's claim and
//! answers a full disk before it becomes a build that fails for a reason that
//! says nothing about the cache. While under the cap the pass only publishes
//! the size; an explicit `0` keeps the old measure-only behaviour; and the
//! environment overrides this number, so the reasoning for a different value
//! stays where that value is configured.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::build_cache_readings::{BuildCacheReadings, CacheSlot, CapSource};
use cog_core::claim_footprint::ClaimFootprint;
use cog_core::config::CacheCapSetting;
use tokio::sync::RwLock;
use tracing::warn;

/// This loop's name in the liveness census.
pub const SANDBOX_BUILD_CACHE_LOOP: &str = "extension_build_cache_watch";

/// Re-walk period when the deployment names none.
///
/// The walk is metadata-only, but the tree has hundreds of thousands of entries
/// and the pod is building into it, so a value below the shared floor buys
/// nothing; the floor itself is applied where the interval is read.
// cap: bound — 部署没给名字时的重扫周期
const DEFAULT_SCAN_INTERVAL_SECS: u64 = 300;

/// Bytes the cache may hold when the deployment names none.
///
/// The shared [`cog_core::build_cache_readings::DEFAULT_CACHE_MAX_BYTES`]:
/// one deployment-derived 3 GiB number (a 5 GiB claim less the measured
/// co-tenants -- `CARGO_HOME`, the bare repo, the per-task worktrees, the
/// host-docs journal), read from the same module that names the readings
/// rather than copied here, so two publishers cannot drift apart on it. A
/// deployment that needs another value sets `SANDBOX_BUILD_CACHE_MAX_BYTES`.
const DEFAULT_MAX_BYTES: u64 = cog_core::build_cache_readings::DEFAULT_CACHE_MAX_BYTES;

/// Deployment variable naming the claim this cache sits on and its declared
/// size, as `claim=size` -- 例如 `cogneva-sandbox-pvc=5Gi`, which is this
/// chart's own default rather than a capacity this code assumes.
///
/// The declared size is the one input no walk inside the pod can produce: a
/// directory-backed claim reports the node's filesystem, so the claim's own
/// number comes from the manifest that declared it. What the cache may take of
/// it is not stated here -- it is that size minus what the claim's co-tenants
/// hold, measured on every pass. Stating the size is not stating the cap.
const CLAIM_ENV: &str = "SANDBOX_VOLUME_CLAIM";

/// How the cache is bounded, and how often it is re-walked.
#[derive(Debug, Clone)]
pub struct BuildCacheConfig {
    /// Measure only (`Unbounded`), a number the deployment names (`Fixed`), or
    /// one the walks derive (`Auto`).
    pub cap: CacheCapSetting,
    /// How often the cache is re-walked and its cap re-enforced.
    pub scan_interval: Duration,
    /// The claim under this cache and the size its manifest declares.
    pub volume_claim: Option<(String, u64)>,
}

impl Default for BuildCacheConfig {
    fn default() -> Self {
        Self {
            cap: CacheCapSetting::Fixed(DEFAULT_MAX_BYTES),
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            volume_claim: None,
        }
    }
}

impl BuildCacheConfig {
    pub fn from_env() -> Self {
        let volume_claim = Self::claim_from_env();
        // With the claim stated, a derived cap is the default: the deployment
        // has said how much room there is, and which part of it this cache may
        // use is a measurement rather than a second number to keep in step.
        let unspecified = if volume_claim.is_some() {
            CacheCapSetting::Auto
        } else {
            CacheCapSetting::Fixed(DEFAULT_MAX_BYTES)
        };
        // Bytes, not a float: a cap read as a float would be rounded to the
        // nearest representable integer, which is a number nobody configured.
        let cap = Self::env_cap_or("SANDBOX_BUILD_CACHE_MAX_BYTES", unspecified);
        let scan_interval = Self::env_u64_or(
            "SANDBOX_BUILD_CACHE_SCAN_INTERVAL_SECS",
            DEFAULT_SCAN_INTERVAL_SECS,
        )
        // The floor is the shared one: a deployment that asks to re-walk
        // faster than a walk of this size warrants gets the floor, and the
        // interval that is published is the floor rather than the ask, so
        // the rule comparing a pass against its own interval does not
        // measure a cadence nothing runs at.
        .max(cog_core::build_cache::MIN_SCAN_INTERVAL_SECS);
        Self {
            cap,
            scan_interval: Duration::from_secs(scan_interval),
            volume_claim,
        }
    }

    /// Read `claim=size` from the deployment.
    ///
    /// Anything unreadable is `None` rather than a guess: an unmeasured claim
    /// leaves the cap where it was, which is visible as a cap that stopped
    /// moving rather than as a volume that quietly overran.
    fn claim_from_env() -> Option<(String, u64)> {
        let raw = std::env::var(CLAIM_ENV).ok()?;
        let (claim, size) = raw.trim().split_once('=')?;
        let bytes = cog_core::claim_footprint::quantity_bytes(size.trim())? as u64;
        if claim.trim().is_empty() || bytes == 0 {
            return None;
        }
        Some((claim.trim().to_string(), bytes))
    }

    /// What the claim behind this cache leaves for it, from two measurements
    /// this process already takes: the claim's footprint (the volume walker)
    /// and the cache's own bytes (this loop's last walk).
    ///
    /// `None` whenever either side is missing -- before the first walk, or on a
    /// volume the deployment did not declare. A cap derived from half a fact is
    /// a cap nobody can trace back to a reading.
    pub fn ceiling_bytes(&self, volume_used: Option<u64>, cache_used: Option<u64>) -> Option<u64> {
        let (_, declared) = self.volume_claim.as_ref()?;
        let (volume_used, cache_used) = (volume_used?, cache_used?);
        Some(declared.saturating_sub(volume_used.saturating_sub(cache_used)))
    }

    /// The same, read off the two live measurements rather than handed in.
    ///
    /// The footprint must be the *same claim* this configuration names: a
    /// walker measuring another volume would answer for room this cache cannot
    /// use, which reads on the dashboards as a cache that is bounded and on the
    /// disk as one that is not.
    pub fn measured_ceiling(
        &self,
        footprint: Option<&ClaimFootprint>,
        cache: &BuildCacheReadings,
    ) -> Option<u64> {
        let (claim, _) = self.volume_claim.as_ref()?;
        let footprint = footprint?;
        if footprint.claim() != claim.as_str() {
            return None;
        }
        let cache_used: u64 = cache.measured()?.values().sum();
        self.ceiling_bytes(footprint.value(), Some(cache_used))
    }

    /// Read a cap: a number bounds it, `0` measures without bounding, and
    /// `auto` derives it. Unset or unreadable takes `default`, which is not a
    /// configured value -- the same rule as the interval.
    fn env_cap_or(key: &str, default: CacheCapSetting) -> CacheCapSetting {
        let Ok(raw) = std::env::var(key) else {
            return default;
        };
        let raw = raw.trim();
        if raw.eq_ignore_ascii_case("auto") {
            return CacheCapSetting::Auto;
        }
        match raw.parse::<u64>() {
            Ok(0) => CacheCapSetting::Unbounded,
            Ok(bytes) => CacheCapSetting::Fixed(bytes),
            Err(_) => default,
        }
    }

    /// Read an integer configuration variable, falling back to `default` when
    /// it is unset or cannot be parsed: such a value is not a configured
    /// number -- the same rule for the cap and the interval.
    fn env_u64_or(key: &str, default: u64) -> u64 {
        std::env::var(key)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(default)
    }

    /// The readings this configuration asks for.
    pub fn readings(&self, dir: impl Into<std::path::PathBuf>) -> BuildCacheReadings {
        let readings = BuildCacheReadings::new(dir);
        match self.cap {
            CacheCapSetting::Unbounded => readings,
            CacheCapSetting::Fixed(bytes) => readings.with_cap(bytes, self.scan_interval.as_secs()),
            CacheCapSetting::Auto => readings
                .with_auto_cap(self.scan_interval.as_secs())
                .with_cap_source(CapSource::Derived),
        }
    }
}

/// The commands this executor is running, as the slot a reclaim pass takes.
///
/// One writer, many readers: a command holds a read guard for as long as its
/// child may live, and a pass takes the write guard, so it runs only while no
/// command is running -- and a command arriving during a pass waits for it
/// rather than building against a cache that is being emptied underneath it. The
/// lock is the whole of the fact; there is nothing else that could say a command
/// is running, which is why this process needs no build gate to answer it.
pub struct CommandSlot(Arc<RwLock<()>>);

impl CommandSlot {
    pub fn new(lock: Arc<RwLock<()>>) -> Self {
        Self(lock)
    }
}

#[async_trait]
impl CacheSlot for CommandSlot {
    /// Always: the question this answers is whether anything here says whether a
    /// build is running, and this process always has that answer. A `false` here
    /// would mean "nothing may be removed", not "nothing to remove".
    fn in_force(&self) -> bool {
        true
    }

    async fn take(&self) -> Option<Box<dyn Send + Sync>> {
        // Refusing is the answer to every way this can fail: a command holds the
        // slot, or one is queued for it (both are "a build may be running"), or
        // a task that held it panicked -- and nothing may be removed on the
        // strength of a lock in that state, where the panic is already reported.
        // The pass reports itself as deferred and tries again on the next walk
        // rather than waiting here: a pass that blocks is a pass that holds up
        // the loop that measures the cache.
        self.0
            .clone()
            .try_write_owned()
            .ok()
            .map(|guard| Box::new(guard) as Box<dyn Send + Sync>)
    }
}

/// The build cache family, in the Prometheus text this process serves.
///
/// The same renderer the other readings this process publishes itself go
/// through, so a series cannot arrive as a counter from one process and a gauge
/// from another -- the type comes from whichever scrape Prometheus saw.
pub async fn render(readings: &BuildCacheReadings) -> String {
    use cog_core::observability::Observable;
    match readings.collect_metrics("").await {
        Ok(metrics) => cog_core::observability_text::render_raw_metrics(&metrics),
        Err(e) => {
            warn!(error = %e, "build cache readings unavailable this scrape");
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::build_cache::OUTCOME_BUSY;
    use cog_core::fs_size;
    use std::sync::Mutex;

    /// Serializes the tests that mutate `SANDBOX_BUILD_CACHE_MAX_BYTES`: the
    /// variable is process-global while the harness runs tests on parallel
    /// threads.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// The ceiling is the claim's declared size minus what its co-tenants hold,
    /// both read at the moment the pass runs. Every input moving has to move the
    /// number, or it is a constant with a story attached.
    #[test]
    fn the_ceiling_is_the_claim_minus_what_its_cotenants_hold() {
        let config = BuildCacheConfig {
            volume_claim: Some(("cogneva-sandbox-pvc".into(), 5 * (1 << 30))),
            ..BuildCacheConfig::default()
        };
        let footprint = ClaimFootprint::new(
            "cogneva-sandbox-pvc",
            std::path::PathBuf::from("/opt/cogneva/sandbox"),
            vec![],
        );
        let cache = BuildCacheReadings::new("/opt/cogneva/sandbox/target");
        cache.set_layers(
            std::iter::once(("debug".to_string(), 2 * (1 << 30))).collect(),
            1_700_000_000,
        );
        // The volume holds the cache plus one GiB of co-tenants.
        footprint.set_used_bytes(3 * (1 << 30));
        assert_eq!(
            config.measured_ceiling(Some(&footprint), &cache),
            Some(4 * (1 << 30)),
            "5 GiB of claim, 1 GiB held by others, so 4 GiB is the cache's"
        );

        // The co-tenants grow by a gigabyte: the ceiling follows, without
        // anyone restating a number.
        footprint.set_used_bytes(4 * (1 << 30));
        assert_eq!(
            config.measured_ceiling(Some(&footprint), &cache),
            Some(3 * (1 << 30))
        );

        // A walker measuring another volume answers for room this cache cannot
        // use, which is worse than no answer.
        let other = ClaimFootprint::new(
            "cogneva-data-pvc",
            std::path::PathBuf::from("/opt/cogneva/data"),
            vec![],
        );
        other.set_used_bytes(1);
        assert_eq!(config.measured_ceiling(Some(&other), &cache), None);

        // Nothing measured yet: no ceiling rather than one computed from half
        // a fact.
        let unmeasured = ClaimFootprint::new(
            "cogneva-sandbox-pvc",
            std::path::PathBuf::from("/opt/cogneva/sandbox"),
            vec![],
        );
        assert_eq!(config.measured_ceiling(Some(&unmeasured), &cache), None);
        assert_eq!(config.measured_ceiling(None, &cache), None);
        let no_layers = BuildCacheReadings::new("/opt/cogneva/sandbox/target");
        assert_eq!(config.measured_ceiling(Some(&footprint), &no_layers), None);
    }

    #[test]
    fn the_default_cap_is_enforced_without_deployment_config() {
        let config = BuildCacheConfig::default();
        assert_eq!(
            config.readings("/nonexistent-cache").cap_bytes(),
            Some(DEFAULT_MAX_BYTES),
            "the default bounds the cache, not just measures it"
        );
    }

    #[test]
    fn a_missing_env_var_falls_back_to_the_default_cap() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("SANDBOX_BUILD_CACHE_MAX_BYTES");
        assert_eq!(
            BuildCacheConfig::from_env().cap,
            CacheCapSetting::Fixed(DEFAULT_MAX_BYTES)
        );
    }

    #[test]
    fn an_explicit_env_var_overrides_the_default_cap() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("SANDBOX_BUILD_CACHE_MAX_BYTES", "0");
        assert_eq!(
            BuildCacheConfig::from_env().cap,
            CacheCapSetting::Unbounded,
            "an explicit 0 still means measure-only"
        );
        std::env::set_var("SANDBOX_BUILD_CACHE_MAX_BYTES", "4096");
        assert_eq!(
            BuildCacheConfig::from_env().cap,
            CacheCapSetting::Fixed(4096)
        );
        std::env::set_var("SANDBOX_BUILD_CACHE_MAX_BYTES", "auto");
        assert_eq!(
            BuildCacheConfig::from_env().cap,
            CacheCapSetting::Auto,
            "the sentinel asks for a cap derived from the walks, not for a number"
        );
        std::env::set_var("SANDBOX_BUILD_CACHE_MAX_BYTES", "not-a-number");
        assert_eq!(
            BuildCacheConfig::from_env().cap,
            CacheCapSetting::Fixed(DEFAULT_MAX_BYTES),
            "an unparsable value is not a configured cap, so the default applies"
        );
        std::env::remove_var("SANDBOX_BUILD_CACHE_MAX_BYTES");
    }

    #[test]
    fn the_scan_interval_defaults_floors_and_overrides_from_the_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("SANDBOX_BUILD_CACHE_SCAN_INTERVAL_SECS");
        assert_eq!(
            BuildCacheConfig::from_env().scan_interval,
            Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS)
        );
        std::env::set_var("SANDBOX_BUILD_CACHE_SCAN_INTERVAL_SECS", "1");
        assert_eq!(
            BuildCacheConfig::from_env().scan_interval,
            Duration::from_secs(cog_core::build_cache::MIN_SCAN_INTERVAL_SECS),
            "a request faster than the shared floor is served at the floor"
        );
        std::env::set_var("SANDBOX_BUILD_CACHE_SCAN_INTERVAL_SECS", "1234");
        assert_eq!(
            BuildCacheConfig::from_env().scan_interval,
            Duration::from_secs(1234),
            "an explicit interval above the floor is the interval that runs"
        );
        std::env::set_var("SANDBOX_BUILD_CACHE_SCAN_INTERVAL_SECS", "soon");
        assert_eq!(
            BuildCacheConfig::from_env().scan_interval,
            Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            "an unparsable interval is not configured, so the default applies"
        );
        std::env::remove_var("SANDBOX_BUILD_CACHE_SCAN_INTERVAL_SECS");
    }

    #[test]
    fn a_cache_with_no_configured_cap_is_measured_and_left_alone() {
        let config = BuildCacheConfig {
            cap: CacheCapSetting::Unbounded,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            volume_claim: None,
        };
        assert!(
            config.readings("/nonexistent-cache").cap_bytes().is_none(),
            "0 means measured, not bounded"
        );
    }

    #[test]
    fn a_configured_cap_is_the_number_that_was_configured() {
        let config = BuildCacheConfig {
            cap: CacheCapSetting::Fixed(4 * 1024 * 1024 * 1024),
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            volume_claim: None,
        };
        assert_eq!(
            config.readings("/nonexistent-cache").cap_bytes(),
            Some(4 * 1024 * 1024 * 1024)
        );
    }

    /// The slot is free exactly while no command holds it, which is what makes
    /// "no build is running" a fact rather than a hope.
    #[tokio::test]
    async fn a_slot_held_by_a_command_defers_the_pass() {
        let lock = Arc::new(RwLock::new(()));
        let slot = CommandSlot::new(Arc::clone(&lock));
        assert!(slot.in_force());
        assert!(
            slot.take().await.is_some(),
            "an idle executor has nothing running"
        );

        let held = lock.clone().read_owned().await;
        assert!(
            slot.take().await.is_none(),
            "a command is running, so the pass may not delete"
        );
        drop(held);
        assert!(slot.take().await.is_some());
    }

    /// A pass holds the slot for as long as it deletes, so a command that
    /// arrives while one is running waits for it instead of building into a
    /// cache that is being emptied underneath it.
    #[tokio::test]
    async fn a_command_waits_for_a_pass_that_holds_the_slot() {
        let lock = Arc::new(RwLock::new(()));
        let slot = CommandSlot::new(Arc::clone(&lock));
        let pass = slot.take().await.expect("free to start with");

        let command = Arc::clone(&lock);
        let waited = tokio::spawn(async move {
            let _held = command.read_owned().await;
        });
        tokio::task::yield_now().await;
        assert!(
            !waited.is_finished(),
            "the command must not start while the pass is deleting"
        );
        drop(pass);
        waited.await.unwrap();
    }

    /// What the pass publishes is what the process serves: one walk, one total,
    /// and the cap family present as soon as a cap is configured -- before any
    /// walk has succeeded, so a cache whose walk keeps failing still reads as
    /// one that is supposed to be bounded.
    #[tokio::test]
    async fn the_published_family_carries_the_size_and_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("debug/deps")).unwrap();
        std::fs::write(dir.path().join("debug/deps/lib.rlib"), vec![b'x'; 128]).unwrap();
        let files =
            fs_size::dir_files(dir.path(), cog_core::build_cache::CACHE_LAYER_DEPTH, &[]).unwrap();
        let readings = BuildCacheConfig {
            cap: CacheCapSetting::Fixed(64),
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            volume_claim: None,
        }
        .readings(dir.path());
        readings.set_layers(fs_size::layer_totals(&files), 1);

        let text = render(&readings).await;
        assert!(
            text.contains("cogneva_build_target_bytes{"),
            "the size belongs in the scrape: {text}"
        );
        assert!(
            text.contains("cogneva_build_target_bytes_cap{"),
            "the cap belongs in the scrape: {text}"
        );
        assert!(
            text.contains(&format!("outcome=\"{OUTCOME_BUSY}\"")),
            "every outcome is served with its zero, so a reader sees the domain: {text}"
        );
    }
}
