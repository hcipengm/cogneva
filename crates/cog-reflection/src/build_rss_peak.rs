//! How much memory a build really took.
//!
//! The build gate decides how many builds fit at once by dividing the room it
//! can see by one constant: what a build of this workspace costs in memory.
//! That constant is a frozen measurement, taken once, and nothing measured the
//! same quantity again — so a bound computed from a stale divisor reads exactly
//! like a bound computed from a correct one, and the two failures it hides are
//! opposite. An understated cost admits a second build into memory that is not
//! there; an overstated one keeps the machine at one build while it has room
//! for three.
//!
//! The quantity measured here is the one the constant was measured with: the
//! sum of `VmRSS` over the build's process tree, sampled while it runs. Same
//! quantity on purpose. A cheaper proxy — the cgroup's charge, the child's own
//! high-water mark — answers a different question, and a difference between
//! two different questions is not a residual anybody can act on.
//!
//! Three properties, each of which has cost a deployment something:
//!
//! - The root's own `VmRSS` is not the build. `cargo` forks one `rustc` per
//!   crate it compiles and the memory is in those, so the tree is walked rather
//!   than the root read.
//! - A process that exits between the listing and the read is a hole in one
//!   sample, not a build that used no memory: it contributes nothing to that
//!   sample, and only a root that is gone altogether ends the sampling.
//! - The sampler never fails the build. Every error path here means "no
//!   measurement", and the build's outcome is decided where it was decided
//!   before — a reading that can break the thing it reads is worse than no
//!   reading.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How often the tree is sampled.
///
/// A cold build is tens of minutes and its memory curve has a shape — link
/// steps spike, codegen plateaus — so half a second is fine enough to see a
/// spike that lasts, and coarse enough that the sampler's own reads are a
/// rounding error against the compile it is watching.
// cap: bound — 密到抓得住真正持续的尖峰，又粗到采样自身开销在它监视的编译面前是舍入误差
pub const SAMPLE_PERIOD: Duration = Duration::from_millis(500);

/// How long a caller waits for the sampler to report after the process it was
/// watching has ended.
///
/// The loop stops within one [`SAMPLE_PERIOD`] of the tree going away, so this
/// is a wall no healthy host reaches: it exists for the process the kernel will
/// not reap, where waiting for the reading would cost the build. Named rather
/// than written at the call site because it is a wall with a reason, and a bare
/// number in a `Duration` constructor is one nothing can be asked about.
// cap: bound — 给有理由的墙时，而不是给构造函数里一个没处问的裸数
pub const DRAIN_WAIT: Duration = Duration::from_secs(5);

/// A bound on the tree walk, so a `/proc` that is misbehaving (a name cycle, a
/// pid reused mid-walk) costs a sample rather than the build.
// cap: bound — 走 /proc 的上界，让名字成环或 pid 在走的过程里被复用时损失的是一拍而不是整次构建
const MAX_TREE: usize = 4096;

/// What one build's memory looked like while it ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RssPeak {
    /// The largest sum of `VmRSS` over the tree that any sample saw.
    pub bytes: u64,
    /// How many samples the peak was taken over. Carried because a peak taken
    /// over one sample is one number at one moment, and a reader deciding
    /// whether to move a sizing constant should know which it has.
    pub samples: u32,
}

impl RssPeak {
    /// Fold one sample in. Kept separate from the loop so the high-water rule
    /// can be read without a clock in it.
    fn absorb(self_or_none: Option<Self>, bytes: u64) -> Self {
        match self_or_none {
            Some(peak) => Self {
                bytes: peak.bytes.max(bytes),
                samples: peak.samples.saturating_add(1),
            },
            None => Self { bytes, samples: 1 },
        }
    }
}

/// The sum of `VmRSS` over `root` and every process below it, in bytes.
///
/// `None` when the root itself cannot be read — that is the one case that means
/// the build is over. A descendant that cannot be read is simply not counted:
/// it exited, and counting it as zero would pull the sum down in exactly the
/// samples where the tree is churning.
pub fn tree_rss_bytes(proc_root: &Path, root: u32) -> Option<u64> {
    let mut total: u64 = 0;
    let mut root_seen = false;
    let mut visited: HashSet<u32> = HashSet::new();
    let mut queue = vec![root];
    while let Some(pid) = queue.pop() {
        if !visited.insert(pid) {
            continue;
        }
        if visited.len() > MAX_TREE {
            break;
        }
        if let Some(bytes) = rss_bytes_of(proc_root, pid) {
            root_seen |= pid == root;
            total = total.saturating_add(bytes);
        }
        queue.extend(children_of(proc_root, pid));
    }
    if root_seen {
        Some(total)
    } else {
        None
    }
}

/// One process's resident set, in bytes, or `None` when it is gone.
fn rss_bytes_of(proc_root: &Path, pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(proc_root.join(pid.to_string()).join("status")).ok()?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(kib.saturating_mul(1024))
}

/// The children of one process.
///
/// Read out of `task/<tid>/children` rather than by scanning every `/proc`
/// entry and pairing it by `PPid` in `stat`: the scan costs one read per
/// process on the host per sample, and the tree here is a handful of processes.
/// A kernel built without `/proc/<pid>/task/<tid>/children` yields no children,
/// which makes the measurement the root alone — a smaller number that says so
/// by carrying one sample's worth of build, not a wrong one that hides.
fn children_of(proc_root: &Path, pid: u32) -> Vec<u32> {
    let task_dir = proc_root.join(pid.to_string()).join("task");
    let Ok(tids) = std::fs::read_dir(&task_dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for tid in tids.flatten() {
        let Ok(text) = std::fs::read_to_string(tid.path().join("children")) else {
            continue;
        };
        out.extend(
            text.split_whitespace()
                .filter_map(|p| p.parse::<u32>().ok()),
        );
    }
    out
}

/// Sample the tree under `root` until it exits, and report the high-water mark.
///
/// `None` when the root was never readable at all: a process that was already
/// gone before the first sample leaves no measurement, and a zero would read as
/// a build that took no memory.
pub async fn watch(proc_root: PathBuf, root: u32, period: Duration) -> Option<RssPeak> {
    let mut peak: Option<RssPeak> = None;
    while let Some(bytes) = tree_rss_bytes(&proc_root, root) {
        peak = Some(RssPeak::absorb(peak, bytes));
        tokio::time::sleep(period).await;
    }
    peak
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A procfs-shaped fixture: `pid` -> (kv mib, children).
    fn fixture(root: &Path, pid: u32, kib: u64, children: &[u32]) {
        let dir = root.join(pid.to_string());
        std::fs::create_dir_all(dir.join("task").join(pid.to_string())).unwrap();
        std::fs::write(
            dir.join("status"),
            format!("Name:\tbuild\nVmRSS:\t {kib} kB\nRssAnon:\t 4 kB\n"),
        )
        .unwrap();
        let list = children
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        std::fs::write(
            dir.join("task").join(pid.to_string()).join("children"),
            list,
        )
        .unwrap();
    }

    fn temp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cogneva-rss-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The number this exists to produce is the tree's, not the root's: cargo's
    /// own footprint is the small part of what a build costs, and reading it
    /// alone would report a constant that never moves and says nothing.
    #[test]
    fn the_sum_is_over_the_tree_and_not_the_root_alone() {
        let root = temp_root("tree");
        fixture(&root, 100, 1024, &[101, 102]);
        fixture(&root, 101, 2048, &[103]);
        fixture(&root, 102, 512, &[]);
        fixture(&root, 103, 4096, &[]);
        assert_eq!(
            tree_rss_bytes(&root, 100),
            Some((1024 + 2048 + 512 + 4096) * 1024)
        );
    }

    /// A root that is gone is no measurement, not a zero. Zero would enter the
    /// high-water mark as a real reading and, on a build that exits before the
    /// first sample, become the only reading there is.
    #[test]
    fn a_gone_root_is_no_measurement_rather_than_zero() {
        let root = temp_root("gone");
        assert_eq!(tree_rss_bytes(&root, 4242), None);
    }

    /// A descendant that exits mid-walk contributes nothing to that sample. It
    /// must not be counted as zero and it must not end the sampling — the build
    /// is still running, and the processes that spawned it are still there.
    #[test]
    fn a_vanished_descendant_does_not_pull_the_sum_down() {
        let root = temp_root("vanish");
        fixture(&root, 200, 1024, &[201, 202]);
        fixture(&root, 201, 2048, &[]);
        // 202 is listed as a child and has no status file: it exited.
        std::fs::create_dir_all(root.join("202").join("task").join("202")).unwrap();
        assert_eq!(tree_rss_bytes(&root, 200), Some((1024 + 2048) * 1024));
    }

    /// A tree that names a cycle is walked once. Without the visited set this
    /// recurses until the walk is cut off, and the sample would be a sum over
    /// the same processes counted hundreds of times.
    #[test]
    fn a_cycle_is_walked_once() {
        let root = temp_root("cycle");
        fixture(&root, 300, 1000, &[301]);
        fixture(&root, 301, 1000, &[300]);
        assert_eq!(tree_rss_bytes(&root, 300), Some(2000 * 1024));
    }

    /// The high-water rule, without a clock in it: the peak is the largest
    /// sample and the count is how many were folded in.
    #[test]
    fn the_peak_keeps_the_largest_sample_and_counts_the_samples() {
        let peak = RssPeak::absorb(None, 100);
        assert_eq!(
            peak,
            RssPeak {
                bytes: 100,
                samples: 1
            }
        );
        let peak = RssPeak::absorb(Some(peak), 40);
        assert_eq!(
            peak,
            RssPeak {
                bytes: 100,
                samples: 2
            },
            "a smaller sample must not lower the peak"
        );
        let peak = RssPeak::absorb(Some(peak), 900);
        assert_eq!(
            peak,
            RssPeak {
                bytes: 900,
                samples: 3
            }
        );
    }
}
