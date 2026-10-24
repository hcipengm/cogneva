//! How many tasks the pool runs at once, read off the memory it runs in.
//!
//! `agent_pool.worker_count` is a ceiling somebody declared, not a reading: it
//! does not move when the container that carries it gets tighter, and until
//! this module nothing measured whether the box could carry it at all. The
//! number in force is the ceiling *or* what the container has room for,
//! whichever is smaller, re-read on a period.
//!
//! The scarce thing is not an idle worker -- an agent waiting on its inbox
//! costs kilobytes -- it is a task in flight: a squad of role agents, their
//! tool output and their context. So the pool keeps its declared number of
//! workers and bounds how many of them may hold a task at once.
//!
//! Only memory is read here. The token window also bounds how much work may
//! start, but that bound lives upstream in the orchestrator's admission, which
//! holds back tasks the window cannot pay for; a second copy of that
//! arithmetic in this crate would be a second answer to one question.
//!
//! An unreadable container is not a calm one: with no reading the count stays
//! at the ceiling, which is the same thing this process did before the module
//! existed, rather than treating "not measured" as "no pressure".

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

/// Shrink when the container's working set crosses this share of its limit.
///
/// The chart's `container_memory_near_limit` alert fires at 0.81 of the limit.
/// The pool backs off one step before that line, so the alert is the report of
/// a pool that could not back off rather than the first reader of a container
/// about to be killed.
pub const MEMORY_RED_FRACTION: f64 = 0.78;

/// Grow again only below this share, so one step cannot both shrink and grow.
///
/// It has to sit above the container's own baseline: measured 2026-10-24 on
/// the main pod, the working set was 6.01 GiB of an 8 GiB limit (0.70) with
/// the pool idle, and most of that is not the pool's. A growth line below the
/// baseline would trap a shrunk pool at one slot forever.
pub const MEMORY_GROW_FRACTION: f64 = 0.73;

/// Which way the last step moved, so a reader can tell a pool holding at its
/// ceiling from one holding because it could not read anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotMove {
    Grow,
    Hold,
    Shrink,
}

/// One step's arithmetic, with the reading that produced it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SlotPlan {
    pub slots: usize,
    pub movement: SlotMove,
    /// Working set over limit, when the container could be read.
    pub memory_fraction: Option<f64>,
}

/// The next count, from the count in force and the container's memory.
///
/// `ceiling` is the operator's declaration and is never exceeded; zero means
/// the pool is switched off and stays off. The floor is one worker: a pool
/// that shrank to zero stops consuming, and nothing left is listening to
/// notice when the pressure clears.
pub fn next_slots(current: usize, ceiling: usize, memory: Option<(u64, u64)>) -> SlotPlan {
    let fraction = memory.and_then(|(used, limit)| {
        if limit == 0 {
            None
        } else {
            Some(used as f64 / limit as f64)
        }
    });
    if ceiling == 0 {
        return SlotPlan {
            slots: 0,
            movement: SlotMove::Hold,
            memory_fraction: fraction,
        };
    }
    let current = current.clamp(1, ceiling);
    let plan = |slots: usize, movement: SlotMove| SlotPlan {
        slots,
        movement,
        memory_fraction: fraction,
    };
    match fraction {
        Some(fraction) if fraction >= MEMORY_RED_FRACTION => {
            plan((current / 2).max(1), SlotMove::Shrink)
        }
        Some(fraction) if fraction < MEMORY_GROW_FRACTION && current < ceiling => {
            plan(current + 1, SlotMove::Grow)
        }
        _ => plan(current, SlotMove::Hold),
    }
}

/// The container's working set and its limit, or `None` when either cannot be
/// read.
///
/// The working set is `memory.current` minus the page cache that the kernel
/// would drop under pressure (`inactive_file`). `memory.current` alone counts
/// cache this container is not holding onto, and a pool that backed off
/// because a file was read would be reading the wrong number.
pub fn read_container_memory() -> Option<(u64, u64)> {
    let used = read_number("/sys/fs/cgroup/memory.current")?;
    let limit = read_number("/sys/fs/cgroup/memory.max")?;
    let inactive_file =
        read_memory_stat_field("/sys/fs/cgroup/memory.stat", "inactive_file").unwrap_or(0);
    Some((used.saturating_sub(inactive_file), limit))
}

fn read_number(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn read_memory_stat_field(path: &str, field: &str) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines()
        .find_map(|line| line.strip_prefix(field)?.trim_start().parse().ok())
}

/// How many workers may hold a task at once, movable while they run.
///
/// The workers are spawned once and outlive the count: a worker that cannot
/// get a permit waits for one instead of being retired, because retiring a
/// consumer that is between messages means the message it was about to take
/// lands in the stream's pending list with nobody reading it. The count moves
/// by widening or narrowing the permits, and a task already running keeps its
/// permit until it finishes -- no count can take back work that has started.
pub struct PoolConcurrency {
    ceiling: usize,
    slots: AtomicUsize,
    permits: RwLock<Arc<Semaphore>>,
}

impl PoolConcurrency {
    pub fn new(ceiling: usize) -> Self {
        let slots = ceiling.max(1);
        Self {
            ceiling,
            slots: AtomicUsize::new(if ceiling == 0 { 0 } else { slots }),
            permits: RwLock::new(Arc::new(Semaphore::new(slots))),
        }
    }

    /// The count in force right now.
    pub fn slots(&self) -> usize {
        self.slots.load(Ordering::Relaxed)
    }

    pub fn ceiling(&self) -> usize {
        self.ceiling
    }

    /// Wait for a turn to run one task.
    ///
    /// `None` means the pool is switched off and this worker will never run
    /// anything -- distinct from a permit that arrives late.
    pub async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        if self.ceiling == 0 {
            return None;
        }
        loop {
            let permits = self.permits.read().await.clone();
            match permits.clone().acquire_owned().await {
                Ok(permit) => {
                    // The count may have moved while this waiter queued on the
                    // old gate. Holding a permit from a gate nobody shares any
                    // more would put one more task in flight than the current
                    // reading allows, so the permit is dropped and the waiter
                    // asks again on the gate in force.
                    if Arc::ptr_eq(&permits, &*self.permits.read().await) {
                        return Some(permit);
                    }
                }
                Err(_) => continue,
            }
        }
    }

    /// Move the count. Returns whether it changed.
    pub async fn set_slots(&self, slots: usize) -> bool {
        if self.ceiling == 0 {
            return false;
        }
        let slots = slots.clamp(1, self.ceiling);
        if slots == self.slots() {
            return false;
        }
        let mut permits = self.permits.write().await;
        let previous = std::mem::replace(&mut *permits, Arc::new(Semaphore::new(slots)));
        // Closing wakes everyone queued on the gate being replaced: an
        // `acquire` that resolves on the old gate with a stale count is the
        // over-admission this swap exists to prevent. Permits already handed
        // out are unaffected, so tasks in flight wind down on their own.
        previous.close();
        self.slots.store(slots, Ordering::Relaxed);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn a_container_with_room_keeps_the_declared_ceiling() {
        let plan = next_slots(3, 3, Some((3 * GIB, 8 * GIB)));
        assert_eq!(plan.slots, 3);
        assert_eq!(plan.movement, SlotMove::Hold);
    }

    #[test]
    fn a_container_near_its_limit_halves_the_pool() {
        // A container whose page cache has already been dropped: 7.4 GiB of
        // working set against an 8 GiB limit is 0.92, past the red line.
        let plan = next_slots(3, 3, Some((7_946_364_672, 8_589_934_592)));
        assert_eq!(plan.slots, 1);
        assert_eq!(plan.movement, SlotMove::Shrink);
    }

    #[test]
    fn the_measured_shape_of_the_main_pod_does_not_shrink_the_pool() {
        // Measured 2026-10-24: memory.current 6.80 GiB with 0.79 GiB of
        // inactive file cache, against an 8 GiB limit -- 0.70, not red. The
        // pod was carrying three slots' worth of work at that ratio, and the
        // count that was declared is what the reading supports.
        let plan = next_slots(3, 3, Some((6_804_500_480 - 790_851_584, 8_589_934_592)));
        assert_eq!(plan.slots, 3);
        assert_eq!(plan.movement, SlotMove::Hold);
        assert!((plan.memory_fraction.expect("read") - 0.70).abs() < 1e-3);
    }

    #[test]
    fn a_shrunk_pool_grows_back_once_there_is_room() {
        let plan = next_slots(1, 3, Some((2 * GIB, 8 * GIB)));
        assert_eq!(plan.slots, 2);
        assert_eq!(plan.movement, SlotMove::Grow);
    }

    #[test]
    fn the_band_between_shrinking_and_growing_is_wide_enough_to_hold() {
        // Between the two lines a pool neither grows nor shrinks: that is the
        // hysteresis that keeps a pool sitting at the edge of its limit from
        // stepping every pass.
        let plan = next_slots(2, 3, Some((6 * GIB, 8 * GIB)));
        assert_eq!(plan.movement, SlotMove::Hold);
        assert!((plan.memory_fraction.expect("read") - 0.75).abs() < 1e-9);
    }

    #[test]
    fn an_unreadable_container_holds_the_ceiling() {
        // "Not measured" must not read as "no pressure": with no reading the
        // pool does what it did before the count was measured at all.
        let plan = next_slots(3, 3, None);
        assert_eq!(plan.slots, 3);
        assert_eq!(plan.movement, SlotMove::Hold);
        assert_eq!(plan.memory_fraction, None);
    }

    #[test]
    fn the_pool_never_drops_below_one_worker() {
        let plan = next_slots(1, 3, Some((8 * GIB, 8 * GIB)));
        assert_eq!(plan.slots, 1);
        assert_eq!(plan.movement, SlotMove::Shrink);
    }

    #[test]
    fn a_ceiling_of_zero_switches_the_pool_off() {
        let plan = next_slots(3, 0, Some((GIB, 8 * GIB)));
        assert_eq!(plan.slots, 0);
    }

    #[test]
    fn the_growth_line_sits_below_the_red_line() {
        assert!(
            MEMORY_GROW_FRACTION < MEMORY_RED_FRACTION,
            "a growth line at or above the red line would let one pass both \
             shrink and grow the pool"
        );
    }

    #[tokio::test]
    async fn a_moved_count_is_reached_without_retiring_a_worker() {
        let gate = std::sync::Arc::new(PoolConcurrency::new(3));
        assert_eq!(gate.slots(), 3);
        let first = gate.acquire().await.expect("a permit");
        assert!(gate.set_slots(1).await);
        assert_eq!(gate.slots(), 1);
        // The task already running keeps its permit and finishes normally.
        drop(first);
        // And the narrowed gate hands out exactly one turn, not the three the
        // count used to allow.
        let _only = gate
            .acquire()
            .await
            .expect("the narrowed gate still has a turn");
        assert_eq!(gate.permits.read().await.available_permits(), 0);
    }

    #[tokio::test]
    async fn a_worker_spawned_under_a_count_of_zero_never_runs() {
        let gate = PoolConcurrency::new(0);
        assert_eq!(gate.slots(), 0);
        assert!(gate.acquire().await.is_none());
    }
}
