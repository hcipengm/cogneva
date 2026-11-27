//! How many builds this container may run at once, read off what it measures.
//!
//! The count used to be a default in code. What defended it was a shared target
//! directory: every lane pointed `CARGO_TARGET_DIR` at the same path, so a
//! second lane bought a second process parked on cargo's lock, not a second
//! compiler. Each lane now writes its own target directory, and a second lane
//! costs exactly two things this container can measure:
//!
//! - **memory**: the container's limit, less the idle working set, divided by
//!   the rise one build was seen to cause. One build's worth is kept back: the
//!   cost kept here is the largest seen, and a build that has not finished has
//!   not yet peaked, so planning up to the limit means the step that reaches
//!   the plan is the step the kernel kills.
//! - **disk**: each extra lane needs a whole target directory of its own, and
//!   the volume's free bytes say how many of those still fit.
//!
//! The count is the smaller of the two, and which one bound it is part of the
//! answer: a count of one because memory is tight and a count of one because
//! nothing has been measured yet are the same number and different facts.
//!
//! What a lane has *not* measured is whether more of them pay. Every finished
//! build records its wall clock and how many builds shared the container while
//! it ran, and those samples are published. They are not compared here: two
//! builds are two different questions -- a dependency build and an incremental
//! one have different walls -- so a rule that read "lane two was slower" off
//! two unrelated builds would be inventing the number it claims to read.
//!
//! An unreadable container is not a calm one: with no reading the count is one,
//! which is the count that ran before anything was measured, and the reason
//! says which reading was missing.

use std::collections::VecDeque;
use std::time::Duration;

use crate::pressure::{Demand, Pressure};

/// How many finished events the price of the next one is read from.
///
/// In events rather than in seconds: a build, or a command, is the thing being
/// priced, and both arrive with the work, not with the clock. A figure read
/// from a window this wide is current; one kept as a running maximum since the
/// process started stops moving after the first large event and is a constant
/// with a measurement's reputation.
///
/// One width for both streams: the build lane's cost window and the command
/// admission price answer the same shape of question -- "what was the worst of
/// the recent few" -- and two copies of that answer drift the day one of them
/// is widened. The stream being read is the caller's; the width is not.
// cap: fixed — 窗口宽度按事件次数定而不是按墙钟：一次编译、一条命令都是一个事件，
// not-a-bound: 窗口宽度按事件次数定而不是按墙钟：一次编译、一条命令都是一个事件，
// 都随任务到达；更老的样本由新样本挤出去，所以「第一次大事件」不会永久定住这个数
pub(crate) const WINDOW: usize = 32;

/// The name the deployment writes a lane count under.
pub const BUILD_CONCURRENCY_ENV: &str = "COGNEVA_SANDBOX_BUILD_CONCURRENCY";

/// What one finished build was measured to cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildSample {
    /// How far the container's working set rose above its idle reading while
    /// this build was the only one running. `None` when no reading was taken
    /// while it held its lane, or when another build shared the container and
    /// the rise cannot be attributed to this one.
    pub peak_bytes: Option<u64>,
    /// Apparent bytes in the target directory this build wrote, once it
    /// finished. It is what the next lane would need of its own.
    pub target_bytes: Option<u64>,
    /// Wall clock from taking the lane to releasing it.
    pub wall: Duration,
    /// How many builds, this one included, shared the container while it ran.
    pub beside: usize,
}

/// How the last build ended, when the container is the one that ended it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildEnd {
    Completed,
    /// The kernel killed the build for the container's memory limit. A build
    /// that did not finish measured nothing; what it says is that the lane it
    /// held was already one too many.
    OomKilled,
}

/// Which reading decided the count, so a reader can tell a count of one that
/// was measured from a count of one that nobody could measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneSource {
    /// The smaller of the measured bounds; the name is the reading that bound.
    Measured(&'static str),
    /// A reading this needs was missing. The name is the one that was missing.
    Unmeasured(&'static str),
    /// A build was OOM-killed since the last one finished; the count is one
    /// until a build completes without one.
    OomKilled,
    /// The deployment named the count itself. The measurements are published
    /// beside it, but a number an operator wrote down is theirs.
    Overridden(&'static str),
}

/// The count and the reading behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LanePlan {
    pub lanes: usize,
    pub source: LaneSource,
}

/// What this container has measured about one build.
#[derive(Debug, Clone, Default)]
pub struct BuildCost {
    /// The lowest working set seen with no build running. The rise a build
    /// causes is measured from it, so an unmeasured baseline leaves every
    /// later reading unattributable.
    baseline_bytes: Option<u64>,
    window: VecDeque<BuildSample>,
    last_end: Option<BuildEnd>,
}

impl BuildCost {
    /// Feed one reading of the container's working set while `builds` builds
    /// run. The baseline only ever lowers: a container that has been busier
    /// has already paid for what is resident now.
    pub fn observe(&mut self, working_set: u64, builds: usize) {
        if builds == 0 {
            self.baseline_bytes = Some(match self.baseline_bytes {
                Some(baseline) => baseline.min(working_set),
                None => working_set,
            });
        }
    }

    pub fn baseline_bytes(&self) -> Option<u64> {
        self.baseline_bytes
    }

    /// Record a finished build and drop the oldest one out of the window.
    pub fn record(&mut self, sample: BuildSample, end: BuildEnd) {
        self.last_end = Some(end);
        if end == BuildEnd::OomKilled {
            // A killed build's target directory is half-written, and its wall
            // is a wall that bought nothing: neither is a price.
            return;
        }
        if self.window.len() == WINDOW {
            self.window.pop_front();
        }
        self.window.push_back(sample);
    }

    /// The largest rise one build was seen to cause, or `None` when no build
    /// has been priced yet. A running maximum over the window, because an
    /// under-estimate of the cost is an over-estimate of how many fit.
    pub fn peak_bytes(&self) -> Option<u64> {
        self.window.iter().filter_map(|s| s.peak_bytes).max()
    }

    /// The largest target directory a build was seen to leave behind.
    pub fn target_bytes(&self) -> Option<u64> {
        self.window.iter().filter_map(|s| s.target_bytes).max()
    }

    /// Every wall clock recorded, with the number of builds that shared the
    /// container alongside it.
    pub fn walls(&self) -> impl Iterator<Item = (usize, Duration)> + '_ {
        self.window.iter().map(|s| (s.beside, s.wall))
    }

    fn oom_pending(&self) -> bool {
        self.last_end == Some(BuildEnd::OomKilled)
    }
}

/// The count for the next admission.
///
/// `declared` is a count somebody wrote down (`COGNEVA_SANDBOX_BUILD_CONCURRENCY`).
/// When it is there the measurements are still taken and published, but the
/// count is theirs; when it is not, the count comes from the two bounds above
/// and never below one -- a lane count of zero stops compiling, and nothing
/// would be left running to notice the pressure had cleared.
pub fn plan_lanes(
    pressure: &Pressure,
    free_bytes: Option<u64>,
    cost: &BuildCost,
    declared: Option<usize>,
) -> LanePlan {
    if let Some(declared) = declared.filter(|n| *n > 0) {
        return LanePlan {
            lanes: declared,
            source: LaneSource::Overridden(BUILD_CONCURRENCY_ENV),
        };
    }
    if cost.oom_pending() {
        return LanePlan {
            lanes: 1,
            source: LaneSource::OomKilled,
        };
    }
    // The memory bound is the shared answer, not a second version of it: the
    // room comes from [`Pressure`] and the price from [`Demand`], and one
    // build's worth is kept back because a build that has not finished has not
    // yet peaked -- planning up to the limit means the step that reaches the
    // plan is the step the kernel kills.
    let demand = Demand::from_peak(cost.peak_bytes());
    let memory = pressure
        .fits(&demand)
        .map(|fit| fit.saturating_sub(1).max(1));
    let disk = match (free_bytes, cost.target_bytes()) {
        (Some(free), Some(target)) if target > 0 => Some((1 + free / target) as usize),
        _ => None,
    };
    match (memory, disk) {
        (Some(memory), Some(disk)) if memory <= disk => LanePlan {
            lanes: memory,
            source: LaneSource::Measured("memory"),
        },
        (Some(_), Some(disk)) => LanePlan {
            lanes: disk,
            source: LaneSource::Measured("disk"),
        },
        (Some(memory), None) => LanePlan {
            lanes: memory,
            source: LaneSource::Measured("memory"),
        },
        (None, Some(disk)) => LanePlan {
            lanes: disk,
            source: LaneSource::Measured("disk"),
        },
        (None, None) => LanePlan {
            lanes: 1,
            source: LaneSource::Unmeasured(missing_reading(pressure, free_bytes, cost)),
        },
    }
}

/// Which reading a reader should go and look at first.
fn missing_reading(pressure: &Pressure, free: Option<u64>, cost: &BuildCost) -> &'static str {
    if pressure.limit_bytes().is_none() {
        "memory limit"
    } else if pressure.working_set_bytes().is_none() {
        "idle working set"
    } else if cost.peak_bytes().is_none() {
        "cost of one build"
    } else if cost.target_bytes().is_none() {
        "size of a target directory"
    } else if free.is_none() {
        "free bytes on the target volume"
    } else {
        // Every figure arrived and none of them bounds a second lane; the
        // count is one because the first lane already fills the room.
        "no room for a second lane"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn sample(peak: u64, target: u64) -> BuildSample {
        BuildSample {
            peak_bytes: Some(peak),
            target_bytes: Some(target),
            wall: Duration::from_secs(60),
            beside: 1,
        }
    }

    fn priced(builds: &[BuildSample]) -> BuildCost {
        let mut cost = BuildCost::default();
        cost.observe(512 * 1024 * 1024, 0);
        for sample in builds {
            cost.record(*sample, BuildEnd::Completed);
        }
        cost
    }

    /// The room as the build lane holds it: the container's limit, and what was
    /// resident with no build running as the working set to measure from.
    fn room(limit: Option<u64>, baseline: Option<u64>) -> Pressure {
        Pressure::new(limit, baseline, 0)
    }

    fn priced_room(limit: u64) -> Pressure {
        room(Some(limit), Some(512 * 1024 * 1024))
    }

    /// The distinction the whole module exists for: a count of one that was
    /// measured and a count of one nobody could measure are the same count.
    #[test]
    fn no_reading_is_not_a_calm_container() {
        let plan = plan_lanes(&room(None, None), None, &BuildCost::default(), None);
        assert_eq!(plan.lanes, 1);
        assert!(matches!(
            plan.source,
            LaneSource::Unmeasured("memory limit")
        ));

        let cost = BuildCost::default();
        let plan = plan_lanes(&room(Some(8 * GIB), None), Some(100 * GIB), &cost, None);
        assert_eq!(plan.lanes, 1);
        assert!(matches!(
            plan.source,
            LaneSource::Unmeasured("idle working set")
        ));
    }

    /// A measured one is labelled as measured, and names the bound.
    #[test]
    fn a_container_with_no_room_reports_a_measured_one() {
        // 例如：8 GiB 限额、0.5 GiB 空闲、一条编译峰 4 GiB——余地装得下一条半，
        // 最后一个位子留着。
        let cost = priced(&[sample(4 * GIB, 3 * GIB)]);
        let plan = plan_lanes(&priced_room(8 * GIB), Some(100 * GIB), &cost, None);
        assert_eq!(plan.lanes, 1);
        assert_eq!(plan.source, LaneSource::Measured("memory"));
    }

    #[test]
    fn the_memory_bound_and_the_disk_bound_are_read_against_each_other() {
        // 例如：余地 7.5 GiB、一条编译峰 2 GiB——装三条，留一条。
        let cost = priced(&[sample(2 * GIB, 20 * GIB)]);
        let plan = plan_lanes(&priced_room(8 * GIB), Some(100 * GIB), &cost, None);
        assert_eq!(plan.lanes, 2, "{plan:?}");
        assert_eq!(plan.source, LaneSource::Measured("memory"));

        // Same container, a volume with room for no second target directory.
        let plan = plan_lanes(&priced_room(8 * GIB), Some(10 * GIB), &cost, None);
        assert_eq!(plan.lanes, 1, "{plan:?}");
        assert_eq!(plan.source, LaneSource::Measured("disk"));
    }

    /// The cost is a window, not a lifetime maximum: a build that peaked once
    /// stops pricing the container once it has aged out.
    #[test]
    fn a_build_that_has_aged_out_of_the_window_stops_setting_the_price() {
        let mut cost = priced(&[sample(6 * GIB, GIB)]);
        assert_eq!(
            plan_lanes(&priced_room(8 * GIB), Some(100 * GIB), &cost, None).lanes,
            1
        );
        for _ in 0..WINDOW {
            cost.record(sample(GIB, GIB), BuildEnd::Completed);
        }
        assert_eq!(cost.peak_bytes(), Some(GIB));
        assert_eq!(
            plan_lanes(&priced_room(8 * GIB), Some(100 * GIB), &cost, None).lanes,
            6
        );
    }

    /// An OOM is the container saying the lane count was one too many, and it
    /// holds until a build completes: a cumulative counter read once would pin
    /// the count for the life of the process.
    #[test]
    fn an_oom_holds_the_count_until_a_build_finishes_again() {
        let mut cost = priced(&[sample(GIB, GIB)]);
        cost.record(sample(GIB, GIB), BuildEnd::OomKilled);
        let plan = plan_lanes(&priced_room(8 * GIB), Some(100 * GIB), &cost, None);
        assert_eq!(plan.lanes, 1);
        assert_eq!(plan.source, LaneSource::OomKilled);
        // The killed build was not priced: its target directory was half
        // written and what it measured is not a price.
        assert_eq!(cost.target_bytes(), Some(GIB));

        cost.record(sample(GIB, GIB), BuildEnd::Completed);
        assert_eq!(
            plan_lanes(&priced_room(8 * GIB), Some(100 * GIB), &cost, None).lanes,
            6
        );
    }

    /// A count written down by the deployment wins, and says so.
    #[test]
    fn a_declared_count_is_taken_as_written() {
        let plan = plan_lanes(&room(None, None), None, &BuildCost::default(), Some(4));
        assert_eq!(plan.lanes, 4);
        assert!(matches!(plan.source, LaneSource::Overridden(_)));
        // Zero is how a config spells "I did not write one".
        let plan = plan_lanes(
            &priced_room(8 * GIB),
            Some(100 * GIB),
            &priced(&[sample(GIB, GIB)]),
            Some(0),
        );
        assert_eq!(plan.lanes, 6);
        assert_eq!(plan.source, LaneSource::Measured("memory"));
    }
}
