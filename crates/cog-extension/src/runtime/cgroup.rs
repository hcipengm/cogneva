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
        peak_bytes: read_u64(root.join("memory.peak")),
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

/// Counter for commands the cgroup OOM killer took. The limit and the peak are
/// readings of live state rather than events, so they are rendered straight
/// from the kernel on each scrape instead of being held here.
struct MemoryMetrics {
    registry: Registry,
    oom_kills: Counter,
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
        registry
            .register(Box::new(oom_kills.clone()))
            .expect("static counter registration");
        MemoryMetrics {
            registry,
            oom_kills,
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
