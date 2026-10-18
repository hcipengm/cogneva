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
