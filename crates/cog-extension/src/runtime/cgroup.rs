//! The memory cgroup the sandbox commands run inside.
//!
//! The container's limit is where a `cargo build` actually dies, and the kernel
//! charges every kill it makes there to a cumulative counter in this cgroup.
//! Reading both is what separates "the container ran out of memory" from
//! "something killed the process": the signal number alone cannot tell those
//! apart, because an OOM-killed build prints nothing before dying and an
//! external kill looks the same.

use std::path::Path;
use std::sync::OnceLock;

use prometheus::{Counter, Encoder, Registry, TextEncoder};
use tracing::warn;

const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// What the container's memory cgroup reports about itself. Every field is
/// independent because every file is: a kernel that does not expose one leaves
/// it `None`, and an unmeasured quantity has to stay unmeasured rather than
/// default to a number nobody read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryFacts {
    /// `memory.max`. `None` when the cgroup is unlimited.
    pub limit_bytes: Option<u64>,
    /// `memory.peak`, the high-water mark since the container started. This is
    /// the evidence for whether the limit is large enough, which is otherwise
    /// guesswork.
    pub peak_bytes: Option<u64>,
    /// The `oom_kill` count from `memory.events`, cumulative for this cgroup.
    pub oom_kills: Option<u64>,
}

impl MemoryFacts {
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
pub fn read() -> MemoryFacts {
    read_at(Path::new(CGROUP_ROOT))
}

/// Read from an arbitrary cgroup root, so the parsing can be tested against a
/// fixture directory instead of whatever kernel the test host happens to run.
pub fn read_at(root: &Path) -> MemoryFacts {
    MemoryFacts {
        limit_bytes: read_limit(root),
        peak_bytes: read_u64(root.join("memory.peak")),
        oom_kills: read_oom_kills(root),
    }
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

fn render_facts(facts: MemoryFacts) -> String {
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
        assert_eq!(read_at(dir.path()), MemoryFacts::default());
    }

    #[test]
    fn an_unparsable_field_stays_unmeasured() {
        let dir = fixture(&[
            ("memory.max", "not-a-number\n"),
            ("memory.peak", "\n"),
            ("memory.events", "low 0\nhigh 0\n"),
        ]);
        assert_eq!(read_at(dir.path()), MemoryFacts::default());
    }

    #[test]
    fn oom_attribution_needs_the_counter_to_have_risen() {
        let two = MemoryFacts {
            oom_kills: Some(2),
            ..Default::default()
        };
        let three = MemoryFacts {
            oom_kills: Some(3),
            ..Default::default()
        };
        assert!(MemoryFacts::oom_kill_between(&two, &three));
        assert!(!MemoryFacts::oom_kill_between(&two, &two));
        // A reading that failed is not evidence that nothing was killed.
        assert!(!MemoryFacts::oom_kill_between(
            &MemoryFacts::default(),
            &three
        ));
        assert!(!MemoryFacts::oom_kill_between(
            &two,
            &MemoryFacts::default()
        ));
    }

    #[test]
    fn the_rendered_text_omits_what_could_not_be_read() {
        let blind = render_facts(MemoryFacts::default());
        assert!(!blind.contains("sandbox_memory_limit_bytes"));
        assert!(!blind.contains("sandbox_memory_peak_bytes"));
        assert!(blind.contains("sandbox_oom_kills_total"));
    }

    #[test]
    fn the_rendered_text_carries_the_readings() {
        let text = render_facts(MemoryFacts {
            limit_bytes: Some(1073741824),
            peak_bytes: Some(53687091),
            oom_kills: Some(0),
        });
        assert!(text.contains("sandbox_memory_limit_bytes 1073741824"));
        assert!(text.contains("sandbox_memory_peak_bytes 53687091"));
    }
}
