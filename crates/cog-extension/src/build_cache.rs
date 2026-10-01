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
//! **The cap is measured and not bounded until the deployment sets one.** The
//! default here is 0, which publishes the size and removes nothing. What the
//! number should be follows from the volume behind the directory, and that
//! volume is shared with the worktrees, so a default that started deleting
//! would be answering "how much of this volume may the cache hold" on the
//! deployment's behalf -- and answering it wrong costs a cold rebuild of
//! everything. This deployment does set one, so the value a process is running
//! with is a deployment fact and not something this file states: it arrives
//! through the environment, and the reasoning for the number stays where the
//! number is configured.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::build_cache_readings::{BuildCacheReadings, CacheSlot};
use tokio::sync::RwLock;
use tracing::warn;

/// This loop's name in the liveness census.
pub const SANDBOX_BUILD_CACHE_LOOP: &str = "extension_build_cache_watch";

/// Re-walk period when the deployment names none.
///
/// The walk is metadata-only, but the tree has hundreds of thousands of entries
/// and the pod is building into it, so a value below the shared floor buys
/// nothing; the floor itself is applied where the interval is read.
const DEFAULT_SCAN_INTERVAL_SECS: u64 = 300;

/// Bytes the cache may hold, and how often it is re-walked.
#[derive(Debug, Clone)]
pub struct BuildCacheConfig {
    /// Bytes the cache may hold, or 0 to measure it without bounding it.
    pub max_bytes: u64,
    /// How often the cache is re-walked and its cap re-enforced.
    pub scan_interval: Duration,
}

impl Default for BuildCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 0,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
        }
    }
}

impl BuildCacheConfig {
    pub fn from_env() -> Self {
        // Bytes, not a float: a cap read as a float would be rounded to the
        // nearest representable integer, which is a number nobody configured.
        let max_bytes = std::env::var("SANDBOX_BUILD_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let scan_interval = std::env::var("SANDBOX_BUILD_CACHE_SCAN_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_SCAN_INTERVAL_SECS)
            // The floor is the shared one: a deployment that asks to re-walk
            // faster than a walk of this size is worth gets the floor, and the
            // interval that is published is the floor rather than the ask, so
            // the rule comparing a pass against its own interval does not
            // measure a cadence nothing runs at.
            .max(cog_core::build_cache::MIN_SCAN_INTERVAL_SECS);
        Self {
            max_bytes,
            scan_interval: Duration::from_secs(scan_interval),
        }
    }

    /// The readings this configuration asks for.
    pub fn readings(&self, dir: impl Into<std::path::PathBuf>) -> BuildCacheReadings {
        BuildCacheReadings::new(dir).with_cap(self.max_bytes, self.scan_interval.as_secs())
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

    #[test]
    fn a_cache_with_no_configured_cap_is_measured_and_left_alone() {
        let config = BuildCacheConfig {
            max_bytes: 0,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
        };
        assert!(
            config.readings("/nonexistent-cache").cap_bytes().is_none(),
            "0 means measured, not bounded"
        );
    }

    #[test]
    fn a_configured_cap_is_the_number_that_was_configured() {
        let config = BuildCacheConfig {
            max_bytes: 4 * 1024 * 1024 * 1024,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
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
            max_bytes: 64,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
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
