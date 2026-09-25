//! Orphan reaping for the process a container makes PID 1, and the gauge that
//! says whether reaping is still happening.
//!
//! Every container entrypoint is PID 1 in its own PID namespace, which makes it
//! the parent of last resort: the kernel reparents a process to it as soon as
//! that process's real parent exits, and keeps a process that has exited around
//! as a zombie until its parent collects it. An orphan therefore has no waiter
//! left -- the only process that would have called `wait()` is gone -- and if the
//! entrypoint never collects it the zombie lives for the life of the container.
//!
//! Nothing this program spawns leaves those orphans, so no spawn site of ours
//! can fix them. `git` serves a local remote by running
//! `sh -c "git-upload-pack <dir>"`, and that shell outlives the fetch it serves:
//! it is reparented to PID 1 and exits unwaited, so one zombie per fetch piles up
//! in every pod that runs git. The debt lands on whichever process is PID 1 and
//! stays there until that process collects it.
//!
//! Two guards keep the sweep from collecting a child that someone else is still
//! waiting for:
//!
//! - A pid this process holds a pidfd for has a waiter by construction -- tokio
//!   collects its own children through pidfds, not through `SIGCHLD` -- so it is
//!   never touched.
//! - A zombie is collected only after it has been seen in two consecutive
//!   sweeps. Owners that collect through a blocking `wait()` hold no pidfd, but
//!   they collect a child within microseconds of its exit, so a zombie that
//!   survives a whole interval has no owner coming for it.
//!
//! The gauge is the other half: reaping that silently stops working looks exactly
//! like reaping that has nothing to do.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Zombies of this process still waiting to be collected.
pub const PROCESS_ZOMBIES_METRIC: &str = "cogneva_process_zombies";

/// Orphans this process has collected since start.
pub const ORPHANS_REAPED_METRIC: &str = "cogneva_orphans_reaped_total";

/// Name of the rule that reads the gauge, so the pairing can be asserted.
pub const PROCESS_ZOMBIES_METRIC_RULE: &str = "orphans_unreaped";

/// Cadence of the sweep. Long enough that an owner collecting through a blocking
/// `wait()` always wins the race, short enough that a wedged sweep is visible
/// within a scrape interval.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(15);

static ORPHANS_REAPED: AtomicU64 = AtomicU64::new(0);

/// Starts the orphan sweep when this process is the container's init.
///
/// Outside a Linux PID namespace nothing gets reparented to us, so there is
/// nothing to collect and the call is a no-op.
#[cfg(target_os = "linux")]
pub fn start_orphan_reaper() {
    if std::process::id() != 1 {
        return;
    }
    let started = std::thread::Builder::new()
        .name("orphan-reaper".to_string())
        .spawn(|| {
            let mut tracked: HashSet<u32> = HashSet::new();
            loop {
                std::thread::sleep(SWEEP_INTERVAL);
                let reaped = sweep(&mut tracked);
                if reaped > 0 {
                    let total = ORPHANS_REAPED.fetch_add(reaped as u64, Ordering::Relaxed);
                    tracing::info!(
                        reaped,
                        total = total + reaped as u64,
                        "collected orphaned processes adopted as PID 1"
                    );
                }
            }
        });
    if let Err(e) = started {
        tracing::warn!(error = %e, "orphan reaper thread not started");
    }
}

#[cfg(not(target_os = "linux"))]
pub fn start_orphan_reaper() {}

/// One pass: collect the orphans nobody else can collect.
///
/// Returns how many were collected. A pid stays tracked while it is a zombie so
/// the next pass can tell a first sighting from one that outlived a whole
/// interval; anything collected -- or no longer a zombie -- drops out.
#[cfg(target_os = "linux")]
fn sweep(tracked: &mut HashSet<u32>) -> usize {
    let held = held_pidfds();
    let mut still_zombie = HashSet::new();
    let mut reaped = 0;
    for pid in zombie_children() {
        if may_reap(pid, tracked.contains(&pid), &held) && reap(pid) {
            reaped += 1;
            continue;
        }
        still_zombie.insert(pid);
    }
    *tracked = still_zombie;
    reaped
}

/// Whether a zombie observed in this pass may be collected: only if no pidfd
/// still points at it and it was already a zombie at the previous pass.
fn may_reap(pid: u32, seen_before: bool, held_pidfds: &HashSet<u32>) -> bool {
    seen_before && !held_pidfds.contains(&pid)
}

/// Zombie children of this process.
fn zombie_children() -> Vec<u32> {
    let me = std::process::id();
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return out;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        if let Some((state, ppid)) = parse_stat(&stat) {
            if state == 'Z' && ppid == me {
                out.push(pid);
            }
        }
    }
    out
}

/// `pid (comm) state ppid ...`. `comm` is the executable name and may itself
/// contain spaces and parentheses, so the fields after it begin after the *last*
/// ')' rather than after the first.
fn parse_stat(stat: &str) -> Option<(char, u32)> {
    let rest = stat.get(stat.rfind(')')? + 1..)?;
    let mut fields = rest.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let ppid = fields.next()?.parse().ok()?;
    Some((state, ppid))
}

/// `anon_inode:[pidfd]` is what a pidfd resolves to, and the only fd of ours that
/// names another process.
#[cfg(target_os = "linux")]
const PIDFD_LINK: &str = "anon_inode:[pidfd]";

/// PIDs this process holds a pidfd for -- each one has a collector already.
#[cfg(target_os = "linux")]
fn held_pidfds() -> HashSet<u32> {
    let mut out = HashSet::new();
    let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
        return out;
    };
    for entry in entries.flatten() {
        let Ok(target) = std::fs::read_link(entry.path()) else {
            continue;
        };
        if target != std::path::Path::new(PIDFD_LINK) {
            continue;
        }
        let fdinfo = format!("/proc/self/fdinfo/{}", entry.file_name().to_string_lossy());
        if let Ok(info) = std::fs::read_to_string(fdinfo) {
            if let Some(pid) = parse_pidfd_fdinfo(&info) {
                out.insert(pid);
            }
        }
    }
    out
}

/// `Pid:` is the line that names the target. `NSpid:` lists every namespace's
/// view of it and must not be mistaken for it.
fn parse_pidfd_fdinfo(info: &str) -> Option<u32> {
    info.lines()
        .find_map(|line| line.strip_prefix("Pid:")?.trim().parse().ok())
}

/// Collect one zombie. `WNOHANG` keeps this from blocking when the zombie was
/// already collected by its owner between the scan and this call.
#[cfg(target_os = "linux")]
fn reap(pid: u32) -> bool {
    // SAFETY: waitpid only reads the child table; the pid comes from /proc and a
    // null status pointer is allowed.
    unsafe {
        libc::waitpid(pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG) == pid as libc::pid_t
    }
}

/// Publishes the zombie gauge and the reaping counter.
pub struct ProcessZombieObservable;

#[async_trait]
impl Observable for ProcessZombieObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        Ok(vec![
            RawMetric::new(PROCESS_ZOMBIES_METRIC, zombie_children().len() as f64),
            RawMetric::new(
                ORPHANS_REAPED_METRIC,
                ORPHANS_REAPED.load(Ordering::Relaxed) as f64,
            ),
        ])
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The reading is the same for every dimension: it counts this process's
    /// children, and which task asked does not change that.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn stat_fields_start_after_the_last_paren() {
        // A comm containing a space and a paren of its own: the naive "split on
        // the first ')'" reads the parent pid out of the process name.
        let stat = "42 (my (odd) name) Z 1 42 42 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0";
        assert_eq!(parse_stat(stat), Some(('Z', 1)));
        assert_eq!(
            parse_stat("7 (redis-server) S 1234 7 7 0 -1"),
            Some(('S', 1234))
        );
        assert_eq!(parse_stat("garbage"), None);
    }

    #[test]
    fn pidfd_fdinfo_yields_the_pid_not_the_nspid() {
        let info = "pos:\t0\nflags:\t02000000\nmnt_id:\t10\nino:\t4242\nPid:\t913\nNSpid:\t913\n";
        assert_eq!(parse_pidfd_fdinfo(info), Some(913));
        assert_eq!(parse_pidfd_fdinfo("pos:\t0\n"), None);
    }

    #[test]
    fn a_zombie_is_collected_only_when_no_one_else_is_waiting_for_it() {
        let none = HashSet::new();
        // First sighting: a blocking waiter may still be on its way.
        assert!(!may_reap(1234, false, &none));
        // Survived a full interval with no pidfd: nobody is coming.
        assert!(may_reap(1234, true, &none));
        // tokio holds a pidfd: it collects this one itself.
        let held = HashSet::from([1234]);
        assert!(!may_reap(1234, true, &held));
    }

    /// The gauge only becomes an alert if a rule queries its exact name. A rename
    /// on either side leaves both halves internally consistent and the signal
    /// silently absent, so the two are pinned against each other here.
    #[test]
    fn deployed_rule_queries_the_metric_this_module_publishes() {
        let chart = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = fs::read_to_string(&chart)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        let rules = root
            .pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .expect("infra_watch.rules present");
        let rule = rules
            .iter()
            .find(|r| r["name"] == PROCESS_ZOMBIES_METRIC_RULE)
            .unwrap_or_else(|| panic!("rule {PROCESS_ZOMBIES_METRIC_RULE} missing"));
        let promql = rule["promql"].as_str().expect("promql is a string");
        assert!(
            promql.contains(PROCESS_ZOMBIES_METRIC),
            "rule {PROCESS_ZOMBIES_METRIC_RULE} must query {PROCESS_ZOMBIES_METRIC}, got: {promql}"
        );
        // A single sweep leaves a fresh orphan in place for one interval, so the
        // rule has to require the reading to *persist*; comparing it to itself an
        // interval or more back is what separates a stalled reaper from a sweep
        // that is simply between ticks.
        assert!(
            promql.contains("offset"),
            "rule {PROCESS_ZOMBIES_METRIC_RULE} must require a persistent reading, got: {promql}"
        );
    }
}
