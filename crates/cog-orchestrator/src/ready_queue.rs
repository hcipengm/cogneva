//! Which ready queue a task belongs on.
//!
//! Almost every task hands its result back over the results stream, so any
//! worker in any deployment can run it: the ready queue is a competing-consumer
//! queue and the DAG state that arbitration needs lives in the shared store.
//!
//! Self-evolution change generation is the exception. It does not put the
//! generated change on the wire — it hands it to the in-process `ChangeSink`
//! fan-out, which writes the `.diff` into *this* process's
//! `self_evolution.change_dir`, and the cycle that scans that directory
//! (`pending_changes`) runs in the same process. A generation task executed by
//! any other worker therefore yields an artifact no reader exists for: the
//! writer writes one directory, the scanner reads another, and the change is
//! lost without anything failing.
//!
//! So placement is decided by who can read the artifact, not by task type
//! alone. [`ready_stream`] carries the tasks whose output travels over the bus;
//! [`process_local_ready_stream`] carries the ones whose output only their own
//! producer can read, and is read only by the deployment that owns the
//! executor role (`self_evolution.executor_enabled`). A queue with no reader is
//! the intended failure mode there: the task waits for the executor to come
//! back instead of being executed where nothing can consume what it produced.

use cog_core::Task;

/// The ready queue every worker deployment reads.
pub fn ready_stream(workspace_id: &str) -> String {
    format!("orchestrator:ready:{workspace_id}")
}

/// The ready queue for tasks whose artifact is only readable by the process
/// that produced it.
pub fn process_local_ready_stream(workspace_id: &str) -> String {
    format!("orchestrator:ready:{workspace_id}:local")
}

/// Whether this task names the unit that must run it.
///
/// A task with an owner is not offered to the shared queue. The queue is a
/// competing-consumer stream: the first worker to read an entry runs it, so
/// publishing an owned task there hands it to whoever is idle rather than to
/// the unit that was named, and the owner only finds out afterwards. It stays
/// `Pending` until its owner asks for it, which is the same state a task
/// waiting on a dependency sits in -- the ready scan offers it again on every
/// pass, so nothing has to remember that it was held.
pub fn carries_an_owner(task: &Task) -> bool {
    task.owner_unit.is_some()
}

/// The queue a ready task is published to. One definition, because the
/// publisher and the consumer must answer the same question the same way.
pub fn ready_stream_for(task: &Task, workspace_id: &str) -> String {
    if task.is_self_evolution() {
        process_local_ready_stream(workspace_id)
    } else {
        ready_stream(workspace_id)
    }
}

/// The consumer group every deployment joins on a ready stream.
///
/// One per workspace, not per pod: the group is what survives a pod restart and
/// what lets a new pod resume from the last acknowledged entry instead of
/// re-reading the queue. Both the consumer that joins it and any reader that
/// asks the transport about its backlog have to name the same group, so the
/// name is derived here rather than written out at each call site.
pub fn ready_group(workspace_id: &str) -> String {
    format!("executor-loop-{workspace_id}")
}

/// How many more jobs the pool's window can pay for, before one is put on a
/// queue.
///
/// `None` is "no bound is derivable", and it is never zero: a missing
/// measurement read as a zero bound would stop dispatch for the whole length of
/// a rollout, which is a driver traded for a reading. Three cases return it --
/// nothing measured (`measured_upstreams == 0`), no job cost to divide by
/// (`reserve_tokens == 0`), and a measurement that covers fewer upstreams than
/// the pool has. The last one is the guard that keeps this from repeating the
/// refusal's old mistake: allowance and spend are summed over the upstreams
/// that reported a window, so a pool where only one member reports would be
/// sized by that member's ruler and the rest of the pool judged by it. A subset
/// says nothing about the pool, and `pool_size == 0` is "not reported" rather
/// than an empty pool.
///
/// `reserve_tokens` is one whole job, measured over finished jobs, so this is a
/// count of jobs and not an average call -- the gateway's own `blocked` test
/// compares the same two numbers, which makes one admitted job exactly one job
/// the gateway would not refuse on these same readings.
///
/// Spend past the allowance (a snapshot read after the window rolled) yields
/// zero rather than a wrapped-around huge number: saturated subtraction.
pub fn jobs_the_window_can_pay_for(
    pace: &cog_core::LlmWindowPace,
    pool_size: usize,
) -> Option<u64> {
    if pace.measured_upstreams == 0 || pace.reserve_tokens == 0 {
        return None;
    }
    if pool_size > 0 && pace.measured_upstreams < pool_size {
        return None;
    }
    Some(pace.allowance_tokens.saturating_sub(pace.spent_tokens) / pace.reserve_tokens)
}

/// How many pool members a pace measurement has to cover before it may bound
/// the queue.
///
/// Members whose last refusal was `entitlement` cannot come back by waiting:
/// the subscription, model, or account is not there, so the same refusal will
/// be there for as long as the configuration stands. Counting them makes the
/// measurement permanently partial -- two of the four upstreams in the pool
/// this was written against answer that way -- and a partial measurement is
/// deliberately `None`, which admits everything. That is the failure the bound
/// exists to prevent, reintroduced through the denominator.
///
/// A member that is merely out of quota still counts: its window rolls, so it
/// is part of what the pool will be able to pay for.
pub fn measurable_pool_size(status: &cog_core::LlmPoolStatus) -> usize {
    let permanently_out = status
        .upstream_evidence
        .iter()
        .filter(|entry| entry.refusal_kind == "entitlement")
        .count();
    status.pool_size.saturating_sub(permanently_out)
}

/// The pool's own bound, read off the whole status rather than the pace alone.
///
/// `unavailable` is a statement about the pool, not about one member: the
/// window's allowance keeps accruing while nothing serves, so a pool that is
/// down would otherwise admit work the moment the allowance crosses one job's
/// cost and hand the queue to a pool that cannot take it.
///
/// The degraded period has exactly these semantics, and they are what the
/// readings beside the count are for:
///
/// - **Zero admission, not a failure.** Every task that asks is held, whatever
///   the allowance pays for. Nothing is dropped, failed or retried into a
///   shorter deadline: a held task stays ready and asks again on the next pass,
///   so the cost of the outage is latency, not lost work.
/// - **The reason is the pool, not the budget.** [`hold_reason`] labels the
///   hold `pool_unavailable`; a reader that sees `window_exhausted` here would
///   be sent to tune an allowance that is not the constraint.
/// - **It has a clock.** `LlmPoolStatus::next_attempt_unix` is the earliest
///   instant the gateway will probe again, and it travels into the admission
///   reading as `retry_at_unix`. That is our retry cadence, not the upstream's
///   promise (`evidenced_recovery_unix` is that), so it is when the decision is
///   re-made, never when the window is claimed to reopen.
///
/// A status that was not read at all is `None` at the call site and admits
/// everything: an unanswered read happens during a rollout and is not an
/// outage.
pub fn jobs_the_window_can_pay_for_status(status: &cog_core::LlmPoolStatus) -> Option<u64> {
    if status.unavailable {
        return Some(0);
    }
    jobs_the_window_can_pay_for(&status.pace, measurable_pool_size(status))
}

/// Why a pass held work back, as the closed label the held counter carries.
///
/// The zeros arrive at the same place and mean different things: a spent window
/// is fixed by waiting, an unavailable pool is fixed by the upstreams coming
/// back, and a hold by the pass's own slice is fixed by nothing -- the next
/// pass takes the rest. Labelling any of them under another files an outage
/// under a budget and sends its reader to tune an allowance that is not the
/// constraint -- which is possible here precisely because the pool verdict
/// overrides an allowance that still pays for a job, and because a pass with no
/// reading at all has no window to blame.
pub fn hold_reason(pool_unavailable: bool, bound: PassBound) -> &'static str {
    match bound {
        PassBound::Ceiling => "pass_ceiling",
        PassBound::Window if pool_unavailable => "pool_unavailable",
        PassBound::Window => "window_exhausted",
    }
}

/// How many of `ready` may go on the wire, given what the queue already owes.
///
/// `window_jobs` is the pool's own bound, or `None` when nothing measured one
/// -- and `None` admits everything. The subtraction is what keeps a pass from
/// re-admitting the same allowance while the previous batch is still queued;
/// saturating, because spend recorded after the window rolled can leave `owed`
/// above the allowance, and a wrapped-around count would admit the whole queue.
pub fn admitted(ready: u64, owed: u64, window_jobs: Option<u64>) -> u64 {
    match window_jobs {
        Some(window_jobs) => ready.min(window_jobs.saturating_sub(owed)),
        None => ready,
    }
}

/// What one pass may put on a queue, and which gate produced that number.
///
/// `bound` is `window` when the pool's own allowance sized the batch and
/// `ceiling` when nothing measured the pool and the pass's slice did. The two
/// are different readings: a window that pays for nothing is repaired by
/// waiting, a ceiling is repaired by nothing at all -- the rest goes out on the
/// next pass -- and a reader told only "N held" would tune the first for the
/// second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassAdmission {
    pub jobs: u64,
    pub bound: PassBound,
}

/// Which gate sized one pass's batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassBound {
    /// The pool's window allowance.
    Window,
    /// The pass's own slice, because nothing measured the pool.
    Ceiling,
}

/// How many of `ready` one pass may put on the wire, when the pool may not
/// have measured anything.
///
/// [`admitted`] reads `None` as "admit everything", and for a single pass that
/// is what it means: the whole ready set goes out at once. That is affordable
/// only if the pool is there. The pass that reads no status is a rollout -- a
/// new process, before its first look at the pool -- which is also when the
/// transport is most likely to still be loading, and a batch that size is a
/// backlog handed to a stream whose writes may be refused one message at a
/// time, leaving the whole batch in `Scheduled` until the stall sweep re-arms
/// it.
///
/// So an unmeasured pass admits a slice instead. It does **not** admit nothing:
/// a publisher that stopped on a missing reading would be driving the platform
/// by the presence of a metric, and a pool source that is never attached would
/// then hold every task forever. The slice makes progress whenever the reading
/// is missing, at the ceiling's rate rather than the backlog's.
///
/// The owed count is deliberately not subtracted in this branch. `owed` is only
/// meaningful against a known allowance; subtracting it from a ceiling would
/// admit nothing as soon as the backlog exceeded one slice, which is the halt
/// this branch exists to avoid.
pub fn admission_for_pass(
    ready: u64,
    owed: u64,
    window_jobs: Option<u64>,
    ceiling: u64,
) -> PassAdmission {
    match window_jobs {
        Some(_) => PassAdmission {
            jobs: admitted(ready, owed, window_jobs),
            bound: PassBound::Window,
        },
        None => PassAdmission {
            jobs: ready.min(ceiling),
            bound: PassBound::Ceiling,
        },
    }
}

/// Which queue this is, as the label an admission reading carries.
///
/// A closed set of two, derived rather than passed: a label value taken from a
/// caller is a label value that will one day hold a task id, and a series named
/// by a value that changes with the tick is a series per tick.
pub fn stream_kind(stream: &str, workspace_id: &str) -> &'static str {
    if stream == process_local_ready_stream(workspace_id) {
        "local"
    } else {
        "shared"
    }
}

/// The queues a deployment consumes. Every worker reads the shared one; only
/// the executor role reads the process-local one.
pub fn consumed_ready_streams(workspace_id: &str, owns_executor_role: bool) -> Vec<String> {
    let mut streams = vec![ready_stream(workspace_id)];
    if owns_executor_role {
        streams.push(process_local_ready_stream(workspace_id));
    }
    streams
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Task, TaskType};

    fn task(id: &str, ty: TaskType, input: serde_json::Value) -> Task {
        Task::new(id, ty, input)
    }

    #[test]
    fn the_two_queues_are_distinct_and_derived_from_the_workspace() {
        assert_eq!(ready_stream("ws"), "orchestrator:ready:ws");
        assert_eq!(
            process_local_ready_stream("ws"),
            "orchestrator:ready:ws:local"
        );
        assert_ne!(ready_stream("ws"), process_local_ready_stream("ws"));
    }

    #[test]
    fn a_change_generation_task_goes_to_the_process_local_queue() {
        let generation = task(
            "t1",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"goal": "fix it", "evolution_mode": "generate_change"}),
        );
        assert_eq!(
            ready_stream_for(&generation, "ws"),
            process_local_ready_stream("ws")
        );

        let typed = task(
            "t2",
            TaskType::Custom("self_evolution".into()),
            serde_json::json!({}),
        );
        assert_eq!(
            ready_stream_for(&typed, "ws"),
            process_local_ready_stream("ws")
        );
    }

    #[test]
    fn ordinary_tasks_stay_on_the_shared_queue() {
        let plain = task(
            "t3",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"goal": "fix it"}),
        );
        assert_eq!(ready_stream_for(&plain, "ws"), ready_stream("ws"));

        let ported = task(
            "t4",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"evolution_mode": "baseline_port"}),
        );
        assert_eq!(ready_stream_for(&ported, "ws"), ready_stream("ws"));
    }

    #[test]
    fn a_task_naming_its_owner_is_not_offered_to_the_shared_queue() {
        let plain = task("t5", TaskType::DagNode, serde_json::json!({}));
        assert!(!carries_an_owner(&plain));

        let mut owned = task("t6", TaskType::DagNode, serde_json::json!({}));
        owned.owner_unit = Some("8b".into());
        assert!(
            carries_an_owner(&owned),
            "an owned task on the shared queue would be run by whoever read it \
             first, and its owner would only learn that afterwards"
        );
    }

    #[test]
    fn only_the_executor_role_reads_the_process_local_queue() {
        let worker = consumed_ready_streams("ws", false);
        assert_eq!(worker, vec![ready_stream("ws")]);
        assert!(
            !worker.contains(&process_local_ready_stream("ws")),
            "a deployment without the executor role must leave change-generation \
             tasks for the process that can read what they produce"
        );

        let executor = consumed_ready_streams("ws", true);
        assert_eq!(
            executor,
            vec![ready_stream("ws"), process_local_ready_stream("ws")]
        );
    }

    fn pace(measured: usize, allowance: u64, spent: u64, reserve: u64) -> cog_core::LlmWindowPace {
        cog_core::LlmWindowPace {
            measured_upstreams: measured,
            allowance_tokens: allowance,
            spent_tokens: spent,
            reserve_tokens: reserve,
            reserve_population: 5,
            window_secs: 604_800,
            window_start_unix: 0,
        }
    }

    #[test]
    fn a_measured_pool_yields_the_jobs_its_remaining_allowance_pays_for() {
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(2, 5_000_000, 3_000_000, 500_000), 2),
            Some(4)
        );
        // Spent past the allowance is zero, not a wrapped-around huge count.
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(2, 1_000_000, 2_000_000, 500_000), 2),
            Some(0)
        );
    }

    #[test]
    fn nothing_measured_is_no_bound_and_never_zero() {
        // A missing measurement read as a zero bound would stop dispatch for
        // as long as the reading is missing.
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(0, 0, 0, 500_000), 2),
            None
        );
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(2, 5_000_000, 0, 0), 2),
            None
        );
    }

    #[test]
    fn admission_is_measured_against_what_the_queue_already_owes() {
        // Nothing measured: this function says everything ready may go out.
        // The publisher does not use that answer as it stands -- see
        // `admission_for_pass`, which puts the pass's slice on this branch.
        assert_eq!(admitted(10, 5, None), 10);
        // Ten ready, five already owed, a window that pays for eight: three.
        // Compared with zero instead, this would be eight every pass -- the
        // same allowance re-admitted while the previous batch is still queued.
        assert_eq!(admitted(10, 5, Some(8)), 3);
        // A queue that owes more than the window pays for admits nothing, and
        // the count does not wrap around.
        assert_eq!(admitted(10, 20, Some(8)), 0);
        // Fewer ready than the window could pay for: the queue is the bound.
        assert_eq!(admitted(2, 1, Some(100)), 2);
    }

    #[test]
    fn a_pass_that_measured_nothing_takes_a_slice_and_not_the_backlog() {
        // The case that made this a defect: a pod minutes old, no status read
        // yet, two thousand ready tasks. Unbounded, the whole backlog went out
        // on one tick and the transport -- still loading -- refused the writes,
        // leaving all of them in `Scheduled` for a stall window.
        let slice = admission_for_pass(2_024, 0, None, 200);
        assert_eq!(slice.jobs, 200);
        assert_eq!(slice.bound, PassBound::Ceiling);
        // A measured pass is not trimmed to the slice: the pool's own answer is
        // the bound, and a window that pays for more than one slice is a window
        // that should be used.
        let measured = admission_for_pass(2_024, 0, Some(500), 200);
        assert_eq!(measured.jobs, 500);
        assert_eq!(measured.bound, PassBound::Window);
        // The owed count is not subtracted from the ceiling: with a backlog
        // already in flight above one slice, subtracting would admit nothing,
        // which is the halt the ceiling branch exists to avoid.
        assert_eq!(admission_for_pass(2_024, 1_729, None, 200).jobs, 200);
        // And a ready set smaller than a slice is still just the ready set.
        assert_eq!(admission_for_pass(7, 0, None, 200).jobs, 7);
    }

    fn bound_of(window_jobs: Option<u64>) -> PassBound {
        admission_for_pass(1, 0, window_jobs, 200).bound
    }

    #[test]
    fn a_hold_names_the_slice_when_no_reading_decided_it() {
        // Nothing measured the pool: neither the window nor availability can be
        // what held the rest, and naming either would send the reader to repair
        // something that is not the constraint.
        assert_eq!(hold_reason(false, bound_of(None)), "pass_ceiling");
        assert_eq!(hold_reason(true, bound_of(None)), "pass_ceiling");
        assert_eq!(hold_reason(false, bound_of(Some(3))), "window_exhausted");
        assert_eq!(hold_reason(true, bound_of(Some(0))), "pool_unavailable");
    }

    fn status(
        pool_size: usize,
        measured: usize,
        unavailable: bool,
        kinds: &[&str],
    ) -> cog_core::LlmPoolStatus {
        cog_core::LlmPoolStatus {
            unavailable,
            pool_size,
            pace: pace(measured, 5_000_000, 3_000_000, 500_000),
            upstream_evidence: kinds
                .iter()
                .map(|kind| cog_core::LlmUpstreamEvidence {
                    refusal_kind: (*kind).into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn upstreams_that_can_never_come_back_do_not_make_the_measurement_partial() {
        // Two of four members answer `entitlement`: no window rolls them back,
        // so the measurement can cover the whole pool this reading can ever
        // cover. Counting them made this `None`, which admits everything.
        assert_eq!(
            jobs_the_window_can_pay_for_status(&status(
                4,
                2,
                false,
                &["quota_cycle", "entitlement", "quota_cycle", "entitlement"]
            )),
            Some(4)
        );
    }

    #[test]
    fn a_hold_names_the_gate_that_produced_it_not_the_one_it_is_filed_under() {
        // Both zeros reach the same hold; only the label says which repair.
        assert_eq!(hold_reason(true, PassBound::Window), "pool_unavailable");
        assert_eq!(hold_reason(false, PassBound::Window), "window_exhausted");
        // The case that made this a defect: a pool that is down still has an
        // allowance that pays for jobs, so the window is not the constraint
        // while the bound is zero. Filing that under the window sends the
        // reader to tune a budget.
        let down = status(2, 2, true, &["quota_cycle", "quota_cycle"]);
        assert_eq!(jobs_the_window_can_pay_for_status(&down), Some(0));
        assert!(
            jobs_the_window_can_pay_for(&down.pace, measurable_pool_size(&down)).unwrap_or(0) > 0,
            "the fixture must keep paying for a job, or it cannot show the two reasons apart"
        );
        assert_eq!(
            hold_reason(down.unavailable, bound_of(Some(0))),
            "pool_unavailable"
        );
    }

    #[test]
    fn a_pool_that_is_down_pays_for_nothing_while_its_allowance_accrues() {
        // A window's allowance keeps growing whether or not anything serves.
        // Without the whole-pool statement, the moment it crossed one job's
        // cost this would start admitting work to a pool that cannot take it.
        assert_eq!(
            jobs_the_window_can_pay_for_status(&status(
                4,
                2,
                true,
                &["quota_cycle", "entitlement", "quota_cycle", "entitlement"]
            )),
            Some(0)
        );
    }

    #[test]
    fn a_measurement_that_covers_less_than_the_usable_pool_is_still_no_bound() {
        // One of the two members that can come back reported. That is a subset
        // of the usable pool, so it is still no bound -- the entitlement
        // exclusion must not turn a partial reading into a usable one.
        assert_eq!(
            jobs_the_window_can_pay_for_status(&status(
                4,
                1,
                false,
                &["quota_cycle", "entitlement", "quota_cycle", "entitlement"]
            )),
            None
        );
    }

    #[test]
    fn a_measurement_covering_part_of_the_pool_is_no_bound() {
        // One member's window summed as if it were the pool's repeats the
        // refusal's old category error on the enqueue side.
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(1, 5_000_000, 0, 500_000), 3),
            None
        );
        // `pool_size == 0` is "not reported", not a pool of no upstreams.
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(1, 5_000_000, 0, 500_000), 0),
            Some(10)
        );
    }
}
