//! The cgroup the sandbox commands run inside.
//!
//! The container's limit is where a `cargo build` actually dies, and the kernel
//! charges every kill it makes there to a cumulative counter in this cgroup.
//! Reading both is what separates "the container ran out of memory" from
//! "something killed the process": the signal number alone cannot tell those
//! apart, because an OOM-killed build prints nothing before dying and an
//! external kill looks the same.
//!
//! The CPU quota is the same question asked about time: a build that is
//! "slow" may simply be a build held back because the container had spent its
//! quota, and nothing in the build's own output says so. `cpu.stat` counts the
//! periods the kernel throttled and the microseconds lost to them, so the two
//! limits the chart writes can be checked against what the container actually
//! did rather than against the measurement they were chosen from.

use std::path::Path;
use std::sync::OnceLock;

use prometheus::{Counter, Encoder, Registry, TextEncoder};
use tracing::warn;

const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// The kernel file holding the high-water mark. Both the container reading and
/// one command's own cage read it, and a second spelling would go on reading a
/// path that no longer exists the day one of the two was renamed.
const MEMORY_PEAK: &str = "memory.peak";

/// What the container's cgroup reports about itself. Every field is
/// independent because every file is: a kernel that does not expose one leaves
/// it `None`, and an unmeasured quantity has to stay unmeasured rather than
/// default to a number nobody read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CgroupFacts {
    /// `memory.max`. `None` when the cgroup is unlimited.
    pub limit_bytes: Option<u64>,
    /// `memory.peak`, the high-water mark since the container started. This is
    /// the evidence for whether the limit is large enough, which is otherwise
    /// guesswork.
    pub peak_bytes: Option<u64>,
    /// `memory.current` as it stands. A sample rather than a high-water mark:
    /// what a build costs is measured by watching this while it runs, and a
    /// lifetime maximum cannot answer "what did that one build rise by".
    pub current_bytes: Option<u64>,
    /// `inactive_file` from `memory.stat`: page cache the kernel would drop
    /// under pressure. Counted out of the working set, because a container
    /// that read a file is not a container holding it.
    pub inactive_file_bytes: Option<u64>,
    /// The `oom_kill` count from `memory.events`, cumulative for this cgroup.
    pub oom_kills: Option<u64>,
    /// `throttled_usec` from `cpu.stat`: microseconds this container spent held
    /// back by its CPU quota, cumulative since it started. Non-zero is the one
    /// signal that the quota binds rather than merely existing.
    pub throttled_usec: Option<u64>,
    /// `nr_throttled` from `cpu.stat`: how many scheduling periods were cut
    /// short by that quota.
    pub nr_throttled: Option<u64>,
}

impl CgroupFacts {
    /// Whether the OOM killer claimed a process between two readings taken
    /// around the same command. Both readings have to have succeeded: a
    /// missing counter is no evidence in either direction, so it never reads
    /// as "not an OOM".
    /// `memory.current` less the page cache the kernel would drop anyway, or
    /// `None` when either reading could not be taken -- an unreadable
    /// container stays unreadable rather than reading as an empty one, and a
    /// missing `memory.stat` leaves the cache unmeasured rather than defaulting
    /// it to zero, which would count the whole page cache as resident.
    pub fn working_set_bytes(&self) -> Option<u64> {
        self.current_bytes
            .zip(self.inactive_file_bytes)
            .map(|(current, inactive)| current.saturating_sub(inactive))
    }

    pub fn oom_kill_between(before: &Self, after: &Self) -> bool {
        match (before.oom_kills, after.oom_kills) {
            (Some(before), Some(after)) => after > before,
            _ => false,
        }
    }
}

/// Read the container's own cgroup. The container runtime hands the process a
/// private cgroup namespace, so `/sys/fs/cgroup` is already the container root.
pub fn read() -> CgroupFacts {
    read_at(Path::new(CGROUP_ROOT))
}

/// Read from an arbitrary cgroup root, so the parsing can be tested against a
/// fixture directory instead of whatever kernel the test host happens to run.
pub fn read_at(root: &Path) -> CgroupFacts {
    let cpu = read_cpu_stat(root);
    CgroupFacts {
        limit_bytes: read_limit(root),
        peak_bytes: read_u64(root.join(MEMORY_PEAK)),
        current_bytes: read_u64(root.join("memory.current")),
        inactive_file_bytes: read_memory_stat(root, "inactive_file"),
        oom_kills: read_oom_kills(root),
        throttled_usec: cpu.map(|(throttled_usec, _)| throttled_usec),
        nr_throttled: cpu.map(|(_, nr_throttled)| nr_throttled),
    }
}

/// Read the two throttling counters from `cpu.stat`.
///
/// Both are required: a file that names one without the other is not half a
/// reading, it is a kernel this code does not know, and half of it would be a
/// number nobody can say the meaning of. They are returned together for that
/// reason.
fn read_cpu_stat(root: &Path) -> Option<(u64, u64)> {
    let text = std::fs::read_to_string(root.join("cpu.stat")).ok()?;
    let field = |name: &str| {
        text.lines().find_map(|line| {
            let mut parts = line.split_whitespace();
            (parts.next()? == name)
                .then(|| parts.next()?.parse::<u64>().ok())
                .flatten()
        })
    };
    Some((field("throttled_usec")?, field("nr_throttled")?))
}

fn read_limit(root: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(root.join("memory.max")).ok()?;
    match raw.trim() {
        // The kernel's spelling for "no limit": the file exists, but it names
        // no ceiling, and reporting a number here would invent one.
        "max" => None,
        text => text.parse().ok(),
    }
}

/// One field of `memory.stat`, which is a flat list of `name value` lines.
fn read_memory_stat(root: &Path, field: &str) -> Option<u64> {
    let text = std::fs::read_to_string(root.join("memory.stat")).ok()?;
    text.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        (parts.next()? == field)
            .then(|| parts.next()?.parse::<u64>().ok())
            .flatten()
    })
}

fn read_oom_kills(root: &Path) -> Option<u64> {
    let events = std::fs::read_to_string(root.join("memory.events")).ok()?;
    for line in events.lines() {
        if let Some((key, value)) = line.split_once(' ') {
            if key == "oom_kill" {
                return value.trim().parse().ok();
            }
        }
    }
    None
}

fn read_u64(path: std::path::PathBuf) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// One command's own cgroup, so the high-water mark it reports is that
/// command's and not the container's lifetime maximum.
///
/// The container-wide `memory.peak` never comes back down, so after the first
/// large build it answers "what has this container ever needed" and nothing
/// about the command in front of you -- a constant, except that a restart
/// measures it again. A child cgroup is made per command, the command is moved
/// into it, and `memory.peak` there is read once the command is gone.
///
/// A child that was made is not the same as accounting inside it: the memory
/// controller only reaches a cgroup whose parent delegates it, and where it
/// does not, the directory exists and holds no `memory.peak`. A container
/// cgroup that still holds the processes of its own subtree is the usual place
/// this happens -- a cgroup with processes in it cannot hand controllers down
/// to its children -- so on a plain container the expected reading is a cage
/// that exists and accounts nothing. That case is counted apart from the one
/// where no cage could be made at all, because the two are fixed by different
/// things (delegating `memory` to this process versus write access to the
/// tree), and a blank histogram cannot tell them apart.
pub struct CommandCage {
    dir: std::path::PathBuf,
}

/// What one command's cage could say about it. The three are separate because
/// a peak of zero, no accounting and no cage at all are three different
/// situations, and a reader that cannot tell them apart will size something
/// against a number nobody measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CageReading {
    /// The cage existed and reported the peak the kernel charged to it, plus
    /// how many of this cage's own processes the memory killer took.
    Peak { bytes: u64, oom_kills: Option<u64> },
    /// The cage existed but the memory controller does not reach it, so there
    /// is no peak to report.
    Unaccounted,
    /// No cage could be made here; the cgroup tree is not writable.
    Unavailable,
}

impl CageReading {
    /// Whether this command was killed for memory.
    ///
    /// `None` when the cage cannot say: an unreadable counter is no evidence in
    /// either direction, and the caller has to fall back to the container-wide
    /// count -- which only says something was killed around the same time. The
    /// cage's own count is the difference between "this command ran out of
    /// memory" and "another command did, while this one was killed".
    pub fn oom_killed(&self) -> Option<bool> {
        match self {
            CageReading::Peak { oom_kills, .. } => oom_kills.map(|n| n > 0),
            CageReading::Unaccounted | CageReading::Unavailable => None,
        }
    }

    /// The peak, when there is one.
    pub fn peak(&self) -> Option<u64> {
        match self {
            CageReading::Peak { bytes, .. } => Some(*bytes),
            CageReading::Unaccounted | CageReading::Unavailable => None,
        }
    }
}

impl CommandCage {
    /// Make a cage under `root` and put `pid` in it.
    ///
    /// `root` is a parameter so a test can cage against a plain directory and
    /// check what gets written, the same way `read_at` is read against one.
    ///
    /// The pid is attached after the spawn, which leaves a window: anything
    /// the command forks before the attach is charged to the parent cgroup
    /// instead. Closing that window would mean joining the cgroup from inside
    /// the child before `exec`, and that costs an allocation in a place where
    /// the language only allows async-signal-safe calls. The window can make a
    /// reading low and never high; a limit, once one is set here, will have to
    /// close it.
    pub fn for_pid_at(root: &Path, pid: u32) -> std::io::Result<Self> {
        let cage = Self {
            dir: root.join(cage_name(pid)),
        };
        std::fs::create_dir(&cage.dir)?;
        if let Err(e) = std::fs::write(cage.dir.join("cgroup.procs"), pid.to_string()) {
            cog_core::ignore_lost!(std::fs::remove_dir(&cage.dir));
            return Err(e);
        }
        Ok(cage)
    }

    /// The same, in the container's own cgroup tree.
    pub fn for_pid(pid: u32) -> std::io::Result<Self> {
        Self::for_pid_at(Path::new(CGROUP_ROOT), pid)
    }

    /// Where this cage sits, for logs and for tests that need to look inside it.
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// The peak charged to this command, or `None` when the memory controller
    /// does not reach the cage.
    pub fn peak_bytes(&self) -> Option<u64> {
        read_u64(self.dir.join(MEMORY_PEAK))
    }

    /// Everything this cage can say about the command it held.
    ///
    /// Separate from closing because the death of the command is decided before
    /// the cage is taken down, and that decision needs the cage's own OOM count.
    pub fn reading(&self) -> CageReading {
        match self.peak_bytes() {
            Some(bytes) => CageReading::Peak {
                bytes,
                oom_kills: read_oom_kills(&self.dir),
            },
            None => CageReading::Unaccounted,
        }
    }

    /// Take the cage down. A process still running in it keeps the directory
    /// alive -- the kernel refuses to drop a populated cgroup -- which is the
    /// honest outcome, and the only trace it leaves is this line.
    pub fn close(self) {
        if let Err(e) = std::fs::remove_dir(&self.dir) {
            tracing::debug!(dir = %self.dir.display(), error = %e, "cage left behind");
        }
    }

    /// Read and take down in one step, for callers with nothing left to decide.
    pub fn finish(self) -> CageReading {
        let reading = self.reading();
        self.close();
        reading
    }
}

/// A name nothing else in this container holds. The pid and the counter alone
/// would repeat after a restart once the kernel reuses a pid, so the clock
/// goes in as well; the counter keeps two commands of the same instant apart.
fn cage_name(pid: u32) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("cog-cmd-{pid}-{n}-{nanos}")
}

/// Publish what one command's cage saw.
///
/// The peak goes into a histogram rather than straight onto a gauge: a bound
/// for one command has to be read off the distribution of many, and a single
/// last value is the maximum of one sample. The outcome is counted separately
/// because an empty histogram and a deployment where no cage can be made look
/// the same from the histogram alone.
pub fn record_cage(reading: CageReading) {
    match reading {
        CageReading::Peak { bytes, oom_kills } => {
            metrics().command_peak.observe(bytes as f64);
            // Kept as a value beside the histogram: the price one command is
            // planned against has to be readable, and a histogram cannot be
            // asked what its samples were.
            record_peak(bytes);
            metrics().cages.with_label_values(&["measured"]).inc();
            // A kill charged to this command's own cage is this command's,
            // not a guess from the container-wide counter moving while it
            // ran, so it is counted on its own line.
            if oom_kills.is_some_and(|n| n > 0) {
                metrics().command_oom_kills.inc();
                warn!(bytes, oom_kills = ?oom_kills, "command's cage was OOM-killed");
            }
        }
        CageReading::Unaccounted => metrics().cages.with_label_values(&["unaccounted"]).inc(),
        CageReading::Unavailable => metrics().cages.with_label_values(&["unavailable"]).inc(),
    }
}

/// The peaks of the last commands that could report one, oldest first.
///
/// The histogram answers "what does a command cost on this deployment"; a
/// price to plan against needs the same reading as a value, and a lifetime
/// maximum would stop moving after the first heavy command. The window is the
/// same one the build lane prices builds from, so a reader comparing the two
/// is comparing like with like.
static RECENT_PEAKS: std::sync::Mutex<std::collections::VecDeque<u64>> =
    std::sync::Mutex::new(std::collections::VecDeque::new());

fn record_peak(bytes: u64) {
    let mut peaks = RECENT_PEAKS.lock().unwrap_or_else(|p| p.into_inner());
    if peaks.len() == crate::pressure::DEMAND_WINDOW {
        peaks.pop_front();
    }
    peaks.push_back(bytes);
}

/// The peaks of the last commands that could report one, oldest first. Empty
/// until a command's cage has been read, which is the reading `Demand` names as
/// missing rather than pricing the work at nothing.
pub fn recent_peaks() -> Vec<u64> {
    RECENT_PEAKS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .copied()
        .collect()
}

/// Counter for commands the cgroup OOM killer took. The limit and the peak are
/// readings of live state rather than events, so they are rendered straight
/// from the kernel on each scrape instead of being held here.
struct MemoryMetrics {
    registry: Registry,
    oom_kills: Counter,
    /// `sandbox_command_cages_total{state}`: how many commands got a cage, and
    /// for the ones that did, whether it could report a peak.
    cages: prometheus::CounterVec,
    /// `sandbox_command_memory_peak_bytes`: one observation per command whose
    /// own cage could report a peak. Buckets start at a quarter of a MiB and
    /// double, so a small tool call and a full build land in different ones;
    /// the top bucket is far past what one command of this deployment has been
    /// watched using, and anything above it counts as that bucket, which is
    /// what tells the shape apart from a mis-set top edge.
    command_peak: prometheus::Histogram,
    /// `sandbox_command_oom_kills_total`: kills the OOM killer charged to a
    /// command's own cage, so the count names the command instead of the
    /// container.
    command_oom_kills: Counter,
}

static METRICS: OnceLock<MemoryMetrics> = OnceLock::new();

fn metrics() -> &'static MemoryMetrics {
    METRICS.get_or_init(|| {
        let registry = Registry::new();
        let oom_kills = Counter::new(
            "sandbox_oom_kills_total",
            "Sandbox commands the container memory limit killed",
        )
        .expect("static counter");
        let cages = prometheus::CounterVec::new(
            prometheus::Opts::new(
                "sandbox_command_cages_total",
                "Sandbox commands by what their own cgroup could report: a measured peak, \
                 a cage the memory controller does not reach, or no cage at all",
            ),
            &["state"],
        )
        .expect("static counter");
        let command_peak = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sandbox_command_memory_peak_bytes",
                "Peak memory the kernel charged to each sandbox command's own cgroup",
            )
            .buckets(
                prometheus::exponential_buckets(256.0 * 1024.0, 2.0, 18).expect("static buckets"),
            ),
        )
        .expect("static histogram");
        let command_oom_kills = Counter::new(
            "sandbox_command_oom_kills_total",
            "Sandbox commands the memory killer took inside their own cgroup",
        )
        .expect("static counter");
        // Every state is on the scrape surface from the first one: a zero that
        // was never incremented says "this has not happened here", while a
        // missing series cannot be told apart from a build without the reading.
        for state in ["measured", "unaccounted", "unavailable"] {
            let _ = cages.with_label_values(&[state]);
        }
        for collector in [
            Box::new(oom_kills.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(cages.clone()),
            Box::new(command_peak.clone()),
            Box::new(command_oom_kills.clone()),
        ] {
            registry
                .register(collector)
                .expect("static counter registration");
        }
        MemoryMetrics {
            registry,
            oom_kills,
            cages,
            command_peak,
            command_oom_kills,
        }
    })
}

/// Count a command the OOM killer took and say so in the log, so the audit
/// trail and the scrape surface agree on the same event.
pub fn record_oom_kill(limit_bytes: Option<u64>, peak_bytes: Option<u64>) {
    metrics().oom_kills.inc();
    warn!(
        limit_bytes = ?limit_bytes,
        peak_bytes = ?peak_bytes,
        "sandbox command OOM-killed by the container memory limit"
    );
}

/// Prometheus text for the memory ceiling, refreshed from the kernel on every
/// scrape. A quantity that could not be read is left out of the output rather
/// than published as zero, which would be indistinguishable from a real limit
/// of zero bytes.
pub fn render() -> String {
    render_facts(read())
}

fn render_facts(facts: CgroupFacts) -> String {
    let mut out = String::new();
    if let Some(limit) = facts.limit_bytes {
        out.push_str(&gauge(
            "sandbox_memory_limit_bytes",
            "Memory ceiling the sandbox commands run under",
            limit,
        ));
    }
    if let Some(peak) = facts.peak_bytes {
        out.push_str(&gauge(
            "sandbox_memory_peak_bytes",
            "High-water mark of container memory use since it started",
            peak,
        ));
    }
    if let Some(throttled_usec) = facts.throttled_usec {
        out.push_str(&gauge_float(
            "sandbox_cpu_throttled_seconds",
            "Cumulative seconds sandbox commands were held back by the CPU quota",
            throttled_usec as f64 / 1_000_000.0,
        ));
    }
    if let Some(nr_throttled) = facts.nr_throttled {
        out.push_str(&gauge(
            "sandbox_cpu_throttle_events",
            "Cumulative scheduling periods cut short by the CPU quota",
            nr_throttled,
        ));
    }
    let mut buf = Vec::new();
    if TextEncoder::new()
        .encode(&metrics().registry.gather(), &mut buf)
        .is_ok()
    {
        out.push_str(&String::from_utf8_lossy(&buf));
    }
    out
}

fn gauge(name: &str, help: &str, value: u64) -> String {
    format!("# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n")
}

/// The kernel counts throttling in whole microseconds; a rule about "how much
/// time is being lost" reads seconds, so the divide happens here rather than in
/// every reader.
fn gauge_float(name: &str, help: &str, value: f64) -> String {
    format!("# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, body) in files {
            std::fs::write(dir.path().join(name), body).unwrap();
        }
        dir
    }

    const EVENTS: &str = "low 0\nhigh 0\nmax 0\noom 3\noom_kill 2\noom_group_kill 0\n";

    /// The working set is what a build is charged for, so the page cache the
    /// kernel would drop is counted out of it -- and a container whose current
    /// reading cannot be taken has no working set at all, rather than an empty
    /// one.
    #[test]
    fn the_working_set_leaves_out_the_cache_the_kernel_would_drop() {
        let dir = fixture(&[
            ("memory.current", "1000\n"),
            ("memory.stat", "anon 700\nfile 300\ninactive_file 250\n"),
        ]);
        assert_eq!(read_at(dir.path()).working_set_bytes(), Some(750));

        let dir = fixture(&[("memory.stat", "inactive_file 250\n")]);
        assert_eq!(read_at(dir.path()).working_set_bytes(), None);

        // 反向那一格同一条：`memory.current` 读得到、`memory.stat` 缺
        // `inactive_file`，报出来的工作集是整份 `memory.current`，也就是把
        // 页缓存算成了实占。量不到就是量不到，不是换个数顶上。
        let dir = fixture(&[("memory.current", "1000\n")]);
        assert_eq!(
            read_at(dir.path()).working_set_bytes(),
            None,
            "缺 inactive_file 时工作集是量不到，不是整个 memory.current"
        );
    }

    #[test]
    fn reads_a_finite_limit_peak_and_kill_count() {
        let dir = fixture(&[
            ("memory.max", "1073741824\n"),
            ("memory.peak", "53687091\n"),
            ("memory.events", EVENTS),
        ]);
        let facts = read_at(dir.path());
        assert_eq!(facts.limit_bytes, Some(1073741824));
        assert_eq!(facts.peak_bytes, Some(53687091));
        assert_eq!(facts.oom_kills, Some(2));
    }

    #[test]
    fn an_unlimited_cgroup_reports_no_limit_rather_than_a_number() {
        let dir = fixture(&[("memory.max", "max\n")]);
        assert_eq!(read_at(dir.path()).limit_bytes, None);
    }

    /// A cage is the command's own cgroup: the pid that is put in it is the one
    /// the kernel charges, and the peak that comes back out is what it charged
    /// -- not the container's lifetime maximum, which never comes down and so
    /// would answer the same for every command after the first big one.
    #[test]
    fn a_command_is_charged_to_the_cage_it_was_put_in() {
        let root = tempfile::tempdir().unwrap();
        let cage = CommandCage::for_pid_at(root.path(), 4242).unwrap();
        assert_eq!(
            std::fs::read_to_string(cage.path().join("cgroup.procs")).unwrap(),
            "4242"
        );
        std::fs::write(cage.path().join("memory.peak"), "53687091\n").unwrap();

        assert_eq!(
            cage.finish(),
            CageReading::Peak {
                bytes: 53687091,
                oom_kills: None,
            },
            "a cage with no memory.events has no kill count, which is not a count of zero"
        );
    }

    /// Taking the cage down is part of reading it, so the next command does not
    /// inherit this one's accounting. In a plain directory the file the attach
    /// wrote is a real file and keeps the directory alive, where a cgroup's
    /// kernel-provided files leave with it; removing it here is what the kernel
    /// does, and the directory is what the test is about.
    #[test]
    fn the_cage_is_taken_down_once_it_has_been_read() {
        let root = tempfile::tempdir().unwrap();
        let cage = CommandCage::for_pid_at(root.path(), 9).unwrap();
        let dir = cage.path().to_path_buf();
        std::fs::remove_file(dir.join("cgroup.procs")).unwrap();

        assert_eq!(cage.finish(), CageReading::Unaccounted);
        assert!(!dir.exists(), "the cage outlived the command it held");
    }

    /// The memory controller only reaches a cgroup whose parent delegates it.
    /// Where it does not, the cage is real and the reading is missing; that is
    /// not a peak of zero, and a reader that conflated them would size
    /// something against a number nobody measured.
    #[test]
    fn a_cage_the_controller_does_not_reach_reads_unaccounted_not_zero() {
        let root = tempfile::tempdir().unwrap();
        let unaccounted = CommandCage::for_pid_at(root.path(), 1).unwrap();
        assert_eq!(unaccounted.finish(), CageReading::Unaccounted);

        let measured = CommandCage::for_pid_at(root.path(), 2).unwrap();
        std::fs::write(measured.path().join("memory.peak"), "0\n").unwrap();
        assert_eq!(
            measured.finish(),
            CageReading::Peak {
                bytes: 0,
                oom_kills: None,
            }
        );
    }

    /// The container's kill counter only says something was killed while the
    /// command ran; the cage's own says this command was. A cage that cannot
    /// read the file has to stay silent rather than answer "not this one".
    #[test]
    fn only_the_cages_own_counter_can_name_the_command_it_killed() {
        let root = tempfile::tempdir().unwrap();

        let killed = CommandCage::for_pid_at(root.path(), 1).unwrap();
        std::fs::write(killed.path().join("memory.peak"), "900\n").unwrap();
        std::fs::write(
            killed.path().join("memory.events"),
            "low 0\nhigh 0\nmax 0\noom 1\noom_kill 1\n",
        )
        .unwrap();
        assert_eq!(killed.finish().oom_killed(), Some(true));

        let spared = CommandCage::for_pid_at(root.path(), 2).unwrap();
        std::fs::write(spared.path().join("memory.peak"), "900\n").unwrap();
        std::fs::write(
            spared.path().join("memory.events"),
            "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
        )
        .unwrap();
        assert_eq!(spared.finish().oom_killed(), Some(false));

        let unreachable = CommandCage::for_pid_at(root.path(), 3).unwrap();
        assert_eq!(unreachable.finish().oom_killed(), None);
    }

    #[test]
    fn a_cage_that_cannot_be_made_says_so_instead_of_pretending() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("not-there");
        assert!(CommandCage::for_pid_at(&missing, 3).is_err());
    }

    /// Two commands of one executor must not share a cage: the second one's
    /// pid would land in the first one's accounting and the peak would be the
    /// sum of both, which is a number about nobody.
    #[test]
    fn two_commands_get_two_cages_even_for_the_same_pid() {
        let root = tempfile::tempdir().unwrap();
        let first = CommandCage::for_pid_at(root.path(), 7).unwrap();
        let second = CommandCage::for_pid_at(root.path(), 7).unwrap();
        assert_ne!(first.path(), second.path());
        assert!(first.path().is_dir() && second.path().is_dir());
    }

    /// The point of separating unaccounted from unavailable is that an
    /// operator can see which of the two is happening; neither is visible if
    /// it never reaches the scrape surface.
    #[test]
    fn the_cage_outcomes_and_the_peak_are_on_the_scrape_surface() {
        let names: Vec<String> = metrics()
            .registry
            .gather()
            .into_iter()
            .map(|family| family.get_name().to_string())
            .collect();
        assert!(names.iter().any(|n| n == "sandbox_command_cages_total"));
        assert!(names
            .iter()
            .any(|n| n == "sandbox_command_memory_peak_bytes"));
    }

    /// What one command is planned to cost is read from a window of finished
    /// commands, not from a lifetime maximum: a heavy command ages out once
    /// enough others have finished behind it.
    #[test]
    fn the_price_window_follows_the_commands_that_finish() {
        let width = crate::pressure::DEMAND_WINDOW as u64;
        for peak in 0..width + 3 {
            record_cage(CageReading::Peak {
                bytes: peak + 1,
                oom_kills: None,
            });
        }
        let peaks = recent_peaks();
        assert_eq!(peaks.len(), crate::pressure::DEMAND_WINDOW);
        assert_eq!(peaks.last().copied(), Some(width + 3));
        assert!(
            !peaks.contains(&1),
            "the oldest sample outlived the window: {peaks:?}"
        );
    }

    #[test]
    fn a_missing_cgroup_leaves_every_field_unmeasured() {
        let dir = fixture(&[]);
        assert_eq!(read_at(dir.path()), CgroupFacts::default());
    }

    #[test]
    fn an_unparsable_field_stays_unmeasured() {
        let dir = fixture(&[
            ("memory.max", "not-a-number\n"),
            ("memory.peak", "\n"),
            ("memory.events", "low 0\nhigh 0\n"),
        ]);
        assert_eq!(read_at(dir.path()), CgroupFacts::default());
    }

    #[test]
    fn oom_attribution_needs_the_counter_to_have_risen() {
        let two = CgroupFacts {
            oom_kills: Some(2),
            ..Default::default()
        };
        let three = CgroupFacts {
            oom_kills: Some(3),
            ..Default::default()
        };
        assert!(CgroupFacts::oom_kill_between(&two, &three));
        assert!(!CgroupFacts::oom_kill_between(&two, &two));
        // A reading that failed is not evidence that nothing was killed.
        assert!(!CgroupFacts::oom_kill_between(
            &CgroupFacts::default(),
            &three
        ));
        assert!(!CgroupFacts::oom_kill_between(
            &two,
            &CgroupFacts::default()
        ));
    }

    /// The CPU half of the same question: a quota that binds shows up as
    /// throttled time, and the reading exists only when the kernel said so.
    #[test]
    fn the_cpu_quota_readings_come_from_cpu_stat_or_not_at_all() {
        let dir = fixture(&[(
            "cpu.stat",
            "usage_usec 1234\nnr_periods 900\nnr_throttled 12\nthrottled_usec 3400000\n",
        )]);
        let facts = read_at(dir.path());
        assert_eq!(facts.throttled_usec, Some(3_400_000));
        assert_eq!(facts.nr_throttled, Some(12));
        let text = render_facts(facts);
        assert!(text.contains("sandbox_cpu_throttled_seconds 3.4"));
        assert!(text.contains("sandbox_cpu_throttle_events 12"));

        // A quota that never bound is a real reading of zero and is published;
        // that is what makes the absence of the series mean something else.
        let unthrottled = read_at(
            fixture(&[(
                "cpu.stat",
                "nr_periods 900\nnr_throttled 0\nthrottled_usec 0\n",
            )])
            .path(),
        );
        assert_eq!(unthrottled.throttled_usec, Some(0));
        assert!(render_facts(unthrottled).contains("sandbox_cpu_throttle_events 0"));

        // Half the pair is no reading: the two counters only mean something
        // together, so a kernel that names one is not read at all.
        let half = read_at(fixture(&[("cpu.stat", "nr_throttled 12\n")]).path());
        assert_eq!(half.throttled_usec, None);
        assert_eq!(half.nr_throttled, None);

        // No file: neither series, rather than two zeros.
        let blind = render_facts(read_at(fixture(&[]).path()));
        assert!(!blind.contains("sandbox_cpu_throttled_seconds"));
        assert!(!blind.contains("sandbox_cpu_throttle_events"));
    }

    #[test]
    fn the_rendered_text_omits_what_could_not_be_read() {
        let blind = render_facts(CgroupFacts::default());
        assert!(!blind.contains("sandbox_memory_limit_bytes"));
        assert!(!blind.contains("sandbox_memory_peak_bytes"));
        assert!(blind.contains("sandbox_oom_kills_total"));
    }

    #[test]
    fn the_rendered_text_carries_the_readings() {
        let text = render_facts(CgroupFacts {
            limit_bytes: Some(1073741824),
            peak_bytes: Some(53687091),
            oom_kills: Some(0),
            ..Default::default()
        });
        assert!(text.contains("sandbox_memory_limit_bytes 1073741824"));
        assert!(text.contains("sandbox_memory_peak_bytes 53687091"));
    }
}
