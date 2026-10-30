//! How many tasks the pool runs at once, read off the container it runs in.
//!
//! The count used to be `agent_pool.worker_count`: a number somebody typed into
//! a configmap, never asked again. Nothing measured whether the box could
//! carry it, and the box getting tighter did not move it.
//!
//! The count is now derived from two measurements, both taken here:
//!
//! - **What one task costs.** The working set with nothing running is the
//!   baseline; what the working set rises by while N tasks run, divided by N,
//!   is the marginal cost of one. Kept as a running maximum, because the
//!   number this feeds is a capacity and an under-estimate of the cost is an
//!   over-estimate of how many fit.
//! - **What the container has.** The cgroup's limit, less the measured cost of
//!   one more task: that difference is what says a slot can be added, and it is
//!   the same figure that says the pool has to shrink when it turns negative.
//!
//! `worker_count` remains as an operator's own cap when one is written down;
//! absent (zero), nothing but the measurement bounds the count. A declared cap
//! is the only number in this path that is not read off the machine.
//!
//! The scarce thing is not an idle worker -- an agent waiting on its inbox
//! costs kilobytes -- it is a task in flight: a squad of role agents, their
//! tool output and their context.
//!
//! Only memory is read here. The token window also bounds how much work may
//! start, but that bound lives upstream in the orchestrator's admission, which
//! holds back tasks the window cannot pay for; a second copy of that
//! arithmetic in this crate would be a second answer to one question.
//!
//! An unreadable container is not a calm one: with no reading the count stays
//! where it is, rather than treating "not measured" as "no pressure".

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

// Where the pool backs off is not a share of the limit. Two shares used to
// stand here -- 0.78 to shrink, 0.73 to grow, the gap picked by hand, the lower
// one picked to clear a baseline measured on one pod on one day -- and a share
// of a limit is a guess at a quantity this process already measures: what one
// task costs. The measured form is the limit less that cost, so the pool backs
// off exactly when one more launch is the one that would not fit, and adds a
// slot exactly while it would. A pod with a different limit or a different task
// mix needs no new number here, because there is no number here.

/// Which way the last step moved, so a reader can tell a pool holding at its
/// measured capacity from one holding because it could not read anything.
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
    /// How many tasks the container has room for, once that is measured.
    pub capacity: Option<usize>,
}

/// What this container spends to run one task, measured rather than declared.
///
/// The baseline is the lowest working set seen with nothing running. The cost
/// of a task is the rise above it divided by how many were running when the
/// reading was taken, kept as a maximum: a task that has not finished has not
/// yet peaked, so the largest figure seen is the one that has actually
/// happened to this process.
///
/// Both are measurements of *this* process in *this* container. Neither
/// travels: a different pod with a different mix of work has a different
/// baseline, and copying one into a config would be the same mistake as the
/// typed count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SlotCost {
    pub baseline_bytes: Option<u64>,
    pub per_task_bytes: Option<u64>,
}

impl SlotCost {
    /// Feed one reading: the working set and how many tasks were running.
    ///
    /// A reading with nothing running is a baseline, and only ever lowers it:
    /// a container that has been busier before has already paid for whatever
    /// is resident now. A reading with work running contributes a marginal
    /// cost only against a known baseline -- without one, the whole resident
    /// set would be divided by the task count and every other component in
    /// the pod would be charged to the pool.
    pub fn observe(&mut self, working_set: u64, tasks_running: usize) {
        if tasks_running == 0 {
            self.baseline_bytes = Some(match self.baseline_bytes {
                Some(baseline) => baseline.min(working_set),
                None => working_set,
            });
            return;
        }
        let Some(baseline) = self.baseline_bytes else {
            return;
        };
        if working_set <= baseline {
            return;
        }
        let marginal = (working_set - baseline) / tasks_running as u64;
        self.per_task_bytes = Some(match self.per_task_bytes {
            Some(cost) => cost.max(marginal),
            None => marginal,
        });
    }

    /// How many tasks fit in `limit` bytes, once both figures are measured.
    ///
    /// The room above the baseline, divided by what one task costs, less the
    /// one task kept back. The cost is a running maximum, and a task that has
    /// not finished has not yet peaked, so the last slot has to be one the next
    /// task's own peak still fits into; planning right up to the limit means
    /// the step that reaches the plan is the one that gets the container killed.
    pub fn capacity(&self, limit: u64) -> Option<usize> {
        let baseline = self.baseline_bytes?;
        let per_task = self.per_task_bytes?;
        if per_task == 0 {
            return None;
        }
        let room = limit.saturating_sub(baseline);
        Some((room / per_task).saturating_sub(1) as usize)
    }
}

/// The next count, from the count in force and what the machine says.
///
/// `declared_cap` is an operator's own ceiling when one is written down --
/// `None`, and `Some(0)` with it, means they did not write one and nothing
/// but the measurement bounds the count. `cost` is what this process has
/// measured about one task; without it neither the capacity nor the room for
/// one more is known, so the count holds at its floor until a task has been
/// measured. That is the first task's path: one slot is enough to run the task
/// that produces the measurement.
///
/// The floor is one worker: a pool that shrank to zero stops consuming, and
/// nothing left is listening to notice when the pressure clears.
pub fn next_slots(
    current: usize,
    declared_cap: Option<usize>,
    memory: Option<(u64, u64)>,
    cost: SlotCost,
) -> SlotPlan {
    let fraction = memory.and_then(|(used, limit)| {
        if limit == 0 {
            None
        } else {
            Some(used as f64 / limit as f64)
        }
    });
    let capacity = memory.and_then(|(_, limit)| cost.capacity(limit));
    let plan = |slots: usize, movement: SlotMove| SlotPlan {
        slots,
        movement,
        memory_fraction: fraction,
        capacity,
    };
    // A written zero is how a config says it wrote no cap -- every configmap
    // on both install paths documents it that way -- so it is not a ceiling of
    // nothing and it must not switch the pool off. Judged here as well as at
    // the config boundary, because this function is where the arithmetic
    // lives and a caller that hands it a zero should get the same answer as
    // the operator who typed it.
    let declared_cap = declared_cap.filter(|cap| *cap > 0);
    // Whichever bound is tighter; with neither written down there is no
    // bound, which is the whole point of the declared cap being optional.
    let bound = match (declared_cap, capacity) {
        (Some(declared), Some(measured)) => Some(declared.min(measured)),
        (Some(declared), None) => Some(declared),
        (None, Some(measured)) => Some(measured),
        (None, None) => None,
    };
    let mut next = current.max(1);
    if let Some(bound) = bound {
        next = next.min(bound.max(1));
    }
    // Both directions are the same comparison against the same measured number:
    // one more task, at the cost this process has observed, against the room the
    // container has left. Unmeasured is not calm -- with no cost there is no
    // evidence that another slot fits, and a pool that added one anyway would be
    // the typed count this module replaced.
    let one_more_fits = match (memory, cost.per_task_bytes) {
        (Some((used, limit)), Some(per_task)) => used.saturating_add(per_task) < limit,
        _ => false,
    };
    if matches!(memory, Some((_, limit)) if limit > 0) && !one_more_fits {
        next = (next / 2).max(1);
    } else if one_more_fits && bound.is_none_or(|bound| next < bound) {
        next += 1;
    }
    let movement = match next.cmp(&current) {
        std::cmp::Ordering::Greater => SlotMove::Grow,
        std::cmp::Ordering::Less => SlotMove::Shrink,
        std::cmp::Ordering::Equal => SlotMove::Hold,
    };
    plan(next, movement)
}

/// The container's working set and its limit, or `None` when either cannot be
/// read.
///
/// The working set is `memory.current` minus the page cache the kernel would
/// drop under pressure (`inactive_file`). `memory.current` alone counts cache
/// this container is not holding onto, and a pool that backed off because a
/// file was read would be reading the wrong number.
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

/// One task's turn at running.
///
/// A guard rather than a bare permit so the count of tasks in flight is known
/// when the permit is dropped, wherever it is dropped. That count is what the
/// cost measurement divides by, so it cannot be reconstructed from anywhere
/// else after the fact.
pub struct PoolTurn {
    _permit: OwnedSemaphorePermit,
    in_use: Arc<AtomicUsize>,
}

impl Drop for PoolTurn {
    fn drop(&mut self) {
        self.in_use.fetch_sub(1, Ordering::Relaxed);
    }
}

/// How many tasks may run at once, movable while they run.
///
/// The workers are spawned once and outlive the count: a worker that cannot
/// get a turn waits for one instead of being retired, because retiring a
/// consumer between messages risks the message it was about to take. The
/// count moves by widening or narrowing the turns, and a task already running
/// keeps its turn until it finishes -- no count can take back work that has
/// started.
pub struct PoolConcurrency {
    declared_cap: Option<usize>,
    slots: AtomicUsize,
    in_use: Arc<AtomicUsize>,
    turns: RwLock<Arc<Semaphore>>,
}

impl PoolConcurrency {
    /// A pool with the operator's ceiling, if they wrote one.
    ///
    /// A written zero is not a ceiling of nothing: it is the absence of a
    /// ceiling, and it is dropped here so that no method below has to ask
    /// the question twice.
    pub fn new(declared_cap: Option<usize>) -> Self {
        let declared_cap = declared_cap.filter(|cap| *cap > 0);
        let slots = 1;
        Self {
            declared_cap,
            slots: AtomicUsize::new(slots),
            in_use: Arc::new(AtomicUsize::new(0)),
            turns: RwLock::new(Arc::new(Semaphore::new(slots))),
        }
    }

    /// The count in force right now.
    pub fn slots(&self) -> usize {
        self.slots.load(Ordering::Relaxed)
    }

    pub fn declared_cap(&self) -> Option<usize> {
        self.declared_cap
    }

    /// How many tasks hold a turn at this instant.
    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Relaxed)
    }

    /// Wait for a turn to run one task.
    ///
    /// There is no way to be refused: the count can be as low as one, never
    /// zero, so a worker that asks always gets a turn eventually. A worker
    /// that had a way of being switched off would be work already taken from
    /// the inbox and then dropped.
    pub async fn acquire(&self) -> PoolTurn {
        loop {
            let turns = self.turns.read().await.clone();
            match turns.clone().acquire_owned().await {
                Ok(permit) => {
                    // The count may have moved while this waiter queued on the
                    // old gate. A turn from a gate nobody shares any more would
                    // put one more task in flight than the current reading
                    // allows, so it is dropped and the waiter asks again.
                    if Arc::ptr_eq(&turns, &*self.turns.read().await) {
                        self.in_use.fetch_add(1, Ordering::Relaxed);
                        return PoolTurn {
                            _permit: permit,
                            in_use: self.in_use.clone(),
                        };
                    }
                }
                Err(_) => continue,
            }
        }
    }

    /// Move the count. Returns whether it changed.
    pub async fn set_slots(&self, slots: usize) -> bool {
        let slots = match self.declared_cap {
            Some(cap) => slots.clamp(1, cap.max(1)),
            None => slots.max(1),
        };
        if slots == self.slots() {
            return false;
        }
        let mut turns = self.turns.write().await;
        let previous = std::mem::replace(&mut *turns, Arc::new(Semaphore::new(slots)));
        // Closing wakes everyone queued on the gate being replaced: a turn that
        // resolves on the old gate with a stale count is the over-admission
        // this swap exists to prevent. Turns already handed out are unaffected,
        // so tasks in flight wind down on their own.
        previous.close();
        self.slots.store(slots, Ordering::Relaxed);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    /// Two gigabytes resident with nothing running, one gigabyte per task, in
    /// an eight gigabyte limit: six tasks' worth of room, five after the one
    /// held back for a peak not yet seen.
    fn measured() -> SlotCost {
        SlotCost {
            baseline_bytes: Some(2 * GIB),
            per_task_bytes: Some(GIB),
        }
    }

    fn calm() -> Option<(u64, u64)> {
        Some((2 * GIB, 8 * GIB))
    }

    #[test]
    fn a_measured_capacity_bounds_the_count() {
        let plan = next_slots(5, None, calm(), measured());
        assert_eq!(plan.slots, 5);
        assert_eq!(plan.movement, SlotMove::Hold);
        assert_eq!(plan.capacity, Some(5));
    }

    #[test]
    fn a_capacity_below_the_count_takes_the_count_down_to_it() {
        // Two gigabytes per task: of the six gigabytes above the baseline
        // three fit, and the one held back leaves two.
        let cost = SlotCost {
            baseline_bytes: Some(2 * GIB),
            per_task_bytes: Some(2 * GIB),
        };
        let plan = next_slots(6, None, calm(), cost);
        assert_eq!(plan.slots, 2);
        assert_eq!(plan.movement, SlotMove::Shrink);
    }

    #[test]
    fn without_a_declared_cap_the_measurement_is_the_only_bound() {
        // Below a measured capacity of five: the count grows one step at a time.
        let plan = next_slots(2, None, calm(), measured());
        assert_eq!(plan.slots, 3);
        assert_eq!(plan.movement, SlotMove::Grow);
    }

    #[test]
    fn a_declared_cap_still_wins_when_it_is_the_tighter_one() {
        let plan = next_slots(2, Some(2), calm(), measured());
        assert_eq!(plan.slots, 2);
        assert_eq!(plan.movement, SlotMove::Hold);
    }

    #[test]
    fn an_unmeasured_cost_does_not_grow_the_pool() {
        // Nothing has run yet, so there is no cost of a task and no evidence
        // that another one fits. The floor of one slot is what runs the task
        // that produces the measurement; a count grown past it on no evidence
        // is the typed number this module replaced.
        let plan = next_slots(1, None, calm(), SlotCost::default());
        assert_eq!(plan.slots, 1);
        assert_eq!(plan.movement, SlotMove::Hold);
        assert_eq!(plan.capacity, None);
    }

    #[test]
    fn a_container_without_room_for_one_more_task_halves_the_pool() {
        // A container whose page cache has already been dropped: working set
        // at 92% of its limit, less than one task's cost away from it.
        let plan = next_slots(3, None, Some((7_946_364_672, 8_589_934_592)), measured());
        assert_eq!(plan.slots, 1);
        assert_eq!(plan.movement, SlotMove::Shrink);
    }

    #[test]
    fn the_measured_shape_of_the_main_pod_does_not_shrink_the_pool() {
        // Measured 2026-10-24: memory.current minus the inactive file cache
        // came to 70% of this container's limit, more than one task's cost
        // away from it -- room for the task being added.
        let plan = next_slots(
            3,
            Some(3),
            Some((6_804_500_480 - 790_851_584, 8_589_934_592)),
            measured(),
        );
        assert_eq!(plan.slots, 3);
        assert_eq!(plan.movement, SlotMove::Hold);
        assert!((plan.memory_fraction.expect("read") - 0.70).abs() < 1e-3);
    }

    #[test]
    fn an_unreadable_container_holds_the_count() {
        // "Not measured" must not read as "no pressure".
        let plan = next_slots(3, None, None, measured());
        assert_eq!(plan.slots, 3);
        assert_eq!(plan.movement, SlotMove::Hold);
        assert_eq!(plan.memory_fraction, None);
    }

    #[test]
    fn the_pool_never_drops_below_one_worker() {
        let plan = next_slots(1, None, Some((8 * GIB, 8 * GIB)), measured());
        assert_eq!(plan.slots, 1);
    }

    #[test]
    fn a_written_zero_is_no_cap_at_all() {
        // The configmap documents `worker_count: 0` as "not declared", on both
        // install paths, and that is the whole surface an operator has. Read
        // here as a ceiling of nothing, the same zero would have shut the pool
        // down -- so the two readings of one written value must not exist.
        let plan = next_slots(3, Some(0), calm(), measured());
        assert_eq!(
            plan,
            next_slots(3, None, calm(), measured()),
            "a written zero has to answer exactly as no cap"
        );
        assert_eq!(plan.slots, 4);
        assert_eq!(plan.movement, SlotMove::Grow);
    }

    #[test]
    fn a_baseline_is_learned_from_an_idle_reading_and_only_ever_lowers() {
        let mut cost = SlotCost::default();
        cost.observe(4 * GIB, 0);
        assert_eq!(cost.baseline_bytes, Some(4 * GIB));
        cost.observe(5 * GIB, 0);
        assert_eq!(
            cost.baseline_bytes,
            Some(4 * GIB),
            "a busier moment is not this container's floor"
        );
        assert_eq!(cost.per_task_bytes, None);
    }

    #[test]
    fn a_task_costs_what_the_working_set_rose_by_over_the_baseline() {
        let mut cost = SlotCost::default();
        cost.observe(4 * GIB, 0);
        cost.observe(6 * GIB, 2);
        assert_eq!(cost.per_task_bytes, Some(GIB));
        // A later reading with more in flight raises the figure only if it is
        // larger: an under-estimate of the cost is an over-estimate of how
        // many tasks fit.
        cost.observe(9 * GIB, 3);
        assert_eq!(cost.per_task_bytes, Some(5 * GIB / 3));
    }

    #[test]
    fn capacity_is_the_room_above_the_baseline_less_one_task() {
        let mut cost = SlotCost::default();
        cost.observe(4 * GIB, 0);
        cost.observe(5 * GIB, 1);
        // Observations of 4 and 5 put the per-task cost at one unit. Four
        // gigabytes of room fit four of them, and the one held back for the
        // next task's own peak leaves three.
        assert_eq!(cost.capacity(8 * GIB), Some(3));
    }

    #[test]
    fn capacity_is_unknown_until_both_figures_are_measured() {
        let mut cost = SlotCost::default();
        assert_eq!(cost.capacity(8 * GIB), None);
        cost.observe(4 * GIB, 0);
        assert_eq!(
            cost.capacity(8 * GIB),
            None,
            "a baseline alone does not say what a task costs"
        );
        cost.observe(5 * GIB, 1);
        assert!(cost.capacity(8 * GIB).is_some());
    }

    #[test]
    fn a_reading_with_work_in_flight_before_any_baseline_measures_nothing() {
        // Without a baseline the whole resident set would be divided by the
        // task count, charging every other component in the pod to the pool.
        let mut cost = SlotCost::default();
        cost.observe(6 * GIB, 2);
        assert_eq!(cost.baseline_bytes, None);
        assert_eq!(cost.per_task_bytes, None);
    }

    #[tokio::test]
    async fn a_moved_count_is_reached_without_retiring_a_worker() {
        let gate = Arc::new(PoolConcurrency::new(None));
        assert_eq!(gate.slots(), 1);
        let first = gate.acquire().await;
        assert_eq!(gate.in_use(), 1);
        assert!(gate.set_slots(3).await);
        assert_eq!(gate.slots(), 3);
        // The task already running keeps its turn and finishes normally.
        drop(first);
        assert_eq!(gate.in_use(), 0);
        // And the widened gate hands out three turns, not the one it started
        // with.
        let _a = gate.acquire().await;
        let _b = gate.acquire().await;
        let _c = gate.acquire().await;
        assert_eq!(gate.in_use(), 3);
    }

    #[tokio::test]
    async fn a_pool_created_with_a_written_zero_still_runs_a_task() {
        // A worker that could never get a turn would hold a message it can
        // never answer, so "zero" must reach this type as no cap rather than
        // as a shut gate.
        let gate = PoolConcurrency::new(Some(0));
        assert_eq!(gate.slots(), 1);
        assert_eq!(gate.declared_cap(), None);
        let turn = gate.acquire().await;
        assert_eq!(gate.in_use(), 1);
        drop(turn);
        assert_eq!(gate.in_use(), 0);
        assert!(gate.set_slots(4).await, "no cap means the count may move");
        assert_eq!(gate.slots(), 4);
    }
}
