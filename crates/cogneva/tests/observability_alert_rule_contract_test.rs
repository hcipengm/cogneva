//! Every PromQL expression in the deployed alert rules reads series by name.
//!
//! A rule whose expression names a series nothing produces never fires. It does
//! not error, it does not log, it does not show up anywhere except as an alert
//! that stays quiet through exactly the incident it was written for — the same
//! failure mode the dashboard contract covers, on the side that is supposed to
//! wake someone up. The two sides drift for the same reason: nothing about
//! editing one makes the other notice.
//!
//! The dashboard contract test covers panels. This one covers
//! `observability.infra_watch.rules` in the chart, which nothing checked before.
//!
//! The tables below are the contract. A series a rule reads must be either one
//! this workspace publishes (recorded with the file that publishes it, so a
//! deleted producer fails the test) or an explicitly owned foreign series.
//! Anything unclassified fails, because the failure mode here is silence, and
//! silence is what a forgotten classification looks like.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

#[path = "common/closed_set.rs"]
mod closed_set;
#[path = "common/dashboard.rs"]
mod dashboard;
#[path = "common/producer.rs"]
mod producer;
#[path = "common/promql.rs"]
mod promql;

use producer::carries_the_producer;
use promql::{
    lagged_equality_complaints, metric_names_in, shape_complaints, uncovered_window_complaints,
};

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";

/// Series this workspace publishes, with the source file that publishes them.
/// The file is read and searched for the name: an entry here whose producer was
/// renamed or removed fails the test rather than quietly licensing a rule that
/// can never fire.
const PRODUCED: &[(&str, &str)] = &[
    (
        "cogneva_aof_repair_dropped_bytes",
        "crates/cogneva/src/aof_repair.rs",
    ),
    (
        "cogneva_aof_repair_pass_timestamp_seconds",
        "crates/cogneva/src/aof_repair.rs",
    ),
    (
        "cogneva_aof_repair_suspected_interior_holes",
        "crates/cogneva/src/aof_repair.rs",
    ),
    (
        "cogneva_aof_repair_unhandled_layout",
        "crates/cogneva/src/aof_repair.rs",
    ),
    (
        "cogneva_aof_repair_untouched_torn_tail_bytes",
        "crates/cogneva/src/aof_repair.rs",
    ),
    (
        "cogneva_aof_repair_verdict_published",
        "crates/cogneva/src/aof_repair.rs",
    ),
    (
        "cogneva_build_gate_refused_total",
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_slots",
        "crates/cog-core/src/build_gate.rs",
    ),
    // The cap family, as far as the rules read it. `unmet` is what says a pass
    // ran and could not reach the cap; the other two are the pair a stalled cap
    // is judged by -- how long the cache has been over it against how long
    // between walks, so the threshold moves with a configured interval instead
    // of a constant that goes stale when the interval changes.
    // `last_reclaim_seconds` is deliberately not here. It is still published and
    // still has a panel, but no rule reads it: the age of the last pass is not
    // how long the cap has gone unmet -- a cache that spent that time under its
    // cap earns the same age by being fine -- and a rule that read one for the
    // other woke someone about a cache whose excess was one walk old.
    // The names are named in the contract file rather than in either publisher:
    // two processes measure a cache of this shape on two volumes that are not
    // each other's, so a second spelling of any of them would leave a rule
    // reading one process's cache and a panel the other's.
    (
        "cogneva_build_target_unmet_bytes",
        "crates/cog-core/src/build_cache.rs",
    ),
    (
        "cogneva_build_target_over_cap_seconds",
        "crates/cog-core/src/build_cache.rs",
    ),
    (
        "cogneva_build_target_scan_interval_seconds",
        "crates/cog-core/src/build_cache.rs",
    ),
    // The change queue. The role flag is read on its own, because it is the only
    // one of the four a process publishes without draining the queue: the rule
    // that says no executor exists reads the flag rather than the depth, since
    // with no executor there is no depth series to be silent about.
    (
        "cogneva_evolution_change_queue_owner",
        "crates/cog-reflection/src/evolution_queue_readings.rs",
    ),
    // The other three are the owner's: the depth and the age of what is waiting,
    // and the interval that turns "waiting a long time" into a comparison
    // against this deployment's own cycle rather than a constant.
    (
        "cogneva_evolution_change_queue_pending",
        "crates/cog-reflection/src/evolution_queue_readings.rs",
    ),
    (
        "cogneva_evolution_change_queue_oldest_seconds",
        "crates/cog-reflection/src/evolution_queue_readings.rs",
    ),
    (
        "cogneva_evolution_change_queue_poll_interval_seconds",
        "crates/cog-reflection/src/evolution_queue_readings.rs",
    ),
    // The apply/test flight. Neither of these is published by a task: the age is
    // derived from a stamp the flight leaves behind when the scrape arrives, and
    // the wall is a property of the process. The pair is what tells a
    // healthy-but-slow verification from a cycle that has stopped at the flight,
    // which nothing else could -- every other reading about that work is written
    // after it ends.
    (
        "cogneva_evolution_change_flight_seconds",
        "crates/cog-reflection/src/evolution_flight_readings.rs",
    ),
    (
        "cogneva_evolution_change_flight_budget_seconds",
        "crates/cog-reflection/src/evolution_flight_readings.rs",
    ),
    (
        "cogneva_evolution_build_outcomes_total",
        "crates/cog-reflection/src/evolution_build_readings.rs",
    ),
    (
        "cogneva_change_fate_total",
        "crates/cog-github/src/change_funnel.rs",
    ),
    (
        "cogneva_change_funnel",
        "crates/cog-github/src/change_funnel.rs",
    ),
    // The footprint of a claim-backed volume, published by whichever process
    // writes that volume: the main application for its data directory, the
    // sandbox executor for the volume its workspaces and build cache live on.
    // One volume therefore has one number, and the name it travels under is
    // stated once — the two publishers read it from there rather than each
    // spelling it, since a second spelling would leave the rule below reading
    // whichever half happened to match. Each publisher is pinned to that name by
    // its own test, which renders the series and compares it.
    (
        "cogneva_data_volume_used_bytes",
        "crates/cog-core/src/claim_footprint.rs",
    ),
    // The DAG's own repair. `Scheduled` is the one task state whose exit is a
    // message rather than a call the process makes on its own, so a task that
    // leaves `Pending` and never reaches `Running` is a state nothing else
    // revisits — the publisher scans `Pending`, the timeout checker reclaims
    // `Running`, and a consumed message leaves no pending entry for the stream
    // readings to report. This counter is the reclaim's own reading, and the
    // rule is the only face on which the state is visible at all.
    (
        "cogneva_dag_stalled_scheduled_reclaimed_total",
        "crates/cog-orchestrator/src/dag_executor/orchestrator.rs",
    ),
    // Keeping a running task resumable, read on both ends. The producer's four
    // outcomes are separate cells because the two failures they describe are
    // different incidents: a snapshot that never reached the store means no
    // resume point was recorded at all (the chain's write side is not there),
    // while one that could not be deleted means the store grows by one row per
    // agent per tick. The consumer's outcome is the only reading that tells a
    // resume point which could not be used from a task that never had one.
    (
        "cogneva_task_checkpoint_total",
        "crates/cog-orchestrator/src/dag_executor/orchestrator.rs",
    ),
    (
        "collab_task_resume_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    // The loop liveness family. The age is derived at scrape time from a stamp
    // the loop leaves behind — which is the point: a loop that died stops
    // stamping and its own series cannot report that, so the reading has to come
    // from a face that outlives it. The period is the loop's own declared
    // cadence, which is what the age is compared against here rather than a
    // constant written into the rule. The census series
    // (`cogneva_loop_registered`, and the loop-name label domain it makes
    // countable) is not read by any rule and is registered with the dashboard
    // contract instead.
    (
        "cogneva_loop_period_seconds",
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_tick_age_seconds",
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_deaths_total",
        "crates/cog-core/src/loop_health.rs",
    ),
    // The restarts are a rule of their own rather than a note on the death rule:
    // a loop that panicked and was run again publishes all of its series again,
    // so every other reading in the deployment shows a loop that never stopped.
    (
        "cogneva_loop_restarts_total",
        "crates/cog-core/src/loop_health.rs",
    ),
    // Who holds the single-writer roles. A loop that exists once per deployment
    // rather than once per process takes a lease on the role it guards, and
    // these three series are the whole of that state as a scrape sees it: the
    // declaration (this process runs the loop and the role is something to
    // contend for), the holder (1 for the process that holds it, 0 for one that
    // asked and was refused), and the count of asks that came back with nothing.
    // The holder series is deliberately absent rather than zero when the ask
    // could not be answered, which is why the unowned rule needs the
    // declaration as well: an absent series and a refusal mean different things,
    // and only the declaration distinguishes "nobody holds it" from "nobody is
    // asking any more". The acquisitions and losses counters sit on the same
    // scrape and no rule reads them: which replica holds a role is a question
    // about a state, and how many times it changed hands is a different
    // question, answered by the difference of two counter readings rather than
    // by a threshold written here.
    (
        "cogneva_loop_role_declared",
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_held",
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_probe_failures_total",
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_landing_failures_total",
        "crates/cog-github/src/landing.rs",
    ),
    (
        "cogneva_mirror_push_failures_total",
        "crates/cog-github/src/landing.rs",
    ),
    // The LLM pool's own readings. `llm_upstream_healthy` is the input the usage
    // rule needs on the other side: an upstream that is unmeasured *and* is
    // serving traffic is the condition, and the second half of it lives here.
    // `llm_usage_verdict_measured` is the measured/unmeasured decision itself,
    // and it is the one series that separates "the upstream reported nothing"
    // from "we never asked" -- a distinction no count can carry.
    (
        "llm_upstream_healthy",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_usage_verdict_measured",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The only record of a request-shape rejection. The health table never sees
    // that class of rejection by design, and an upstream that has answered
    // nothing successfully since this process started has no per-upstream health
    // series at all, so there is no healthy reading to keep: an upstream that
    // answers every request with 400 leaves the pool's available reading at 1 and
    // this counter as the only trace, and with no reader here nothing says so.
    (
        "llm_upstream_client_errors_total",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The pool's own verdict -- the companion the rejection rule joins on, and the
    // only one that survives the case that rule exists for: it is published on
    // every snapshot regardless of what the per-upstream table holds, so it is
    // still there when the rejecting upstream has no health series of its own.
    (
        "llm_pool_available",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // Request outcomes per upstream. This is where a pool that serves nothing
    // shows up at all, and its failure half is the only cell that cannot be
    // produced by idleness: the success half is published lazily, so a rule that
    // reads absence alone could not tell "nobody called" from "every call
    // failed", while a rule that requires the failure side to have moved can.
    (
        "llm_calls_total",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "cogneva_metric_held_without_producer",
        "crates/cog-gateway/src/lib.rs",
    ),
    (
        "cogneva_metric_held_unreadable",
        "crates/cog-gateway/src/lib.rs",
    ),
    (
        "cogneva_process_zombies",
        "crates/cog-observability/src/process_zombies.rs",
    ),
    (
        "cogneva_redrive_budget_losses_total",
        "crates/cog-github/src/redrive_budget.rs",
    ),
    (
        "cogneva_redrive_refusals_total",
        "crates/cog-github/src/redrive_budget.rs",
    ),
    (
        "cogneva_stream_pending_claim_idle_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_measure_failures_total",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_measure_interval_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_measure_last_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_unreclaimed_oldest_idle_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_read_block_seconds",
        "crates/cog-stream/src/observable.rs",
    ),
    (
        "cogneva_stream_read_silent_seconds",
        "crates/cog-stream/src/observable.rs",
    ),
    (
        "cogneva_runtime_asset_manifest_absent",
        "crates/cog-reflection/src/runtime_assets.rs",
    ),
    (
        "cogneva_runtime_asset_manifest_unusable",
        "crates/cog-reflection/src/runtime_assets.rs",
    ),
    (
        "cogneva_runtime_asset_state",
        "crates/cog-reflection/src/runtime_assets.rs",
    ),
    (
        "cogneva_trace_tier_last_pass_seconds",
        "crates/cog-observability/src/snapshot.rs",
    ),
    (
        "cogneva_trace_tier_overdue",
        "crates/cog-observability/src/snapshot.rs",
    ),
    (
        "cogneva_trace_tier_pass_failures_total",
        "crates/cog-observability/src/snapshot.rs",
    ),
    (
        "cogneva_trace_tier_scan_interval_seconds",
        "crates/cog-observability/src/snapshot.rs",
    ),
    (
        "collab_classification_unreachable",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "collab_declared_scale_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "collab_goal_class_source_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "collab_route_decisions_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "ralph_terminations_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "self_evolution_change_yield_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "sandbox_oom_kills_total",
        "crates/cog-extension/src/runtime/cgroup.rs",
    ),
    (
        "cogneva_verification_timeouts_total",
        "crates/cog-reflection/src/verification_budget.rs",
    ),
    // The notification outlets. A delivery that fails is invisible from
    // outside this process -- the caller cannot tell "a human was paged" from
    // "the message was dropped" -- so the per-outlet outcome counters are the
    // only surface the rule can read.
    (
        "cogneva_notification_delivery_total",
        "crates/cog-notification/src/delivery.rs",
    ),
    // The registry store's walks. What the store holds is published as three
    // readings that all keep their last value when a walk fails, so the failure
    // itself needs its own series -- and it is published from the first scrape,
    // which is what tells a walker that never succeeded apart from a process
    // that measures no registry (the two publish the same nothing otherwise).
    (
        "cogneva_registry_walk_failures_total",
        "crates/cog-reflection/src/registry_footprint.rs",
    ),
    // The reclamation round's own readings, and the pair is what gets read. A
    // round that ran and could not read the store counts as a run and
    // deliberately does not advance the completion stamp, which is what
    // separates it from a completed round with nothing to remove (a run too,
    // but the stamp moves) and from a store that was never due (neither moves).
    // The refusals are the third reading of the same round: deletions the store
    // would not carry out.
    (
        "cogneva_registry_maintenance_runs_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    (
        "cogneva_registry_maintenance_reading_unix",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    (
        "cogneva_registry_prune_failures_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // The governance face. A resource ceiling belongs to the operator and only
    // moves when the install face applies it, so a ceiling raised in the
    // repository stays unenforced until then -- and until it does, "the change
    // landed" and "the change is in effect" look the same from every reading
    // that already exists. The drift count is per object, because that is the
    // unit a ceiling is declared and enforced in. The failure count is the
    // comparison's own outcome, published from the first scrape: a process that
    // can never read the cluster publishes the same nothing as one with no
    // ceiling to compare, and only one of those is worth believing.
    (
        "cogneva_governance_drift_fields",
        "crates/cog-reflection/src/governance_drift.rs",
    ),
    (
        "cogneva_governance_check_failures_total",
        "crates/cog-reflection/src/governance_drift.rs",
    ),
    // Four series whose producers each carry a sentence saying what should read
    // them and which nothing read: the census below found them by walking the
    // closed set against both reader faces rather than by looking for rules to
    // write. Each one closes a failure that is otherwise silent.
    //
    // The signing face: the delivery readings count messages that went out, so
    // an outlet whose every request is refused and an outlet nobody calls are
    // the same zero there. The refusals split by whose they are -- an outlet no
    // signing rule covers is ours, a missing key is the operator's -- and only
    // this counter says which happened.
    (
        "cogneva_notification_sign_total",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The registry debt: a round deleted tags and the restart that reclaims the
    // space kept failing. The round publishes the debt every round precisely
    // because the moment it matters has no other reading; nothing read it.
    (
        "cogneva_registry_gc_owed",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // The pool signal: whether the gateway got the pool verdict into the key the
    // scheduler reads. Zero here means a pool that is down is not pausing
    // LLM-dependent work, and no other series says so.
    (
        "llm_pool_signal_connected",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The usage cells: an upstream asked for token usage that answered without
    // it. Every token total on that upstream is then blind, and the cell was
    // only mentioned in other series' summaries.
    (
        "llm_usage_readings_total",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // Five more found the same way. Each one's own help text says what to read
    // it with or what its non-zero means, and none of them was read.
    //
    // The sample log's capacity verdict: 1 when the log is over its row budget
    // and the oldest-first sweep cannot prune further without deleting a series'
    // current value. The sweep publishes a verdict; nothing consumed it.
    (
        "metrics_samples_over_capacity",
        "crates/cog-storage/src/metrics_sample_cap.rs",
    ),
    // The memory backlog past the re-drive window: raw sources the system will
    // not pick up again without a budgeted backfill. Its help names the action
    // it is waiting for, and it had no reader.
    (
        "memory_unextracted_raw_aged_out",
        "crates/cog-memory/src/ingestor.rs",
    ),
    // The audited document channel, whose help says "refused_by_audit climbing
    // is a security event" -- the security cell of a channel with no reader.
    (
        "audited_llm_requests_total",
        "crates/cog-gateway/src/document_egress.rs",
    ),
    // The version contract: a standing violation count per clause, reported for
    // every clause including the zeros precisely so an absent series means the
    // clause was never judged. Nothing read it.
    (
        "cogneva_version_contract_violations",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // The worktree index: tracked files a resident worktree's git index does not
    // remember, where the help states 0 is healthy and the size of the number is
    // the reading.
    (
        "cogneva_worktree_index_missing_files",
        "crates/cog-reflection/src/workspace.rs",
    ),
];

/// Series the rules read that this workspace does not publish, with the owner.
/// Listing them keeps the check honest: an unlisted foreign series fails the
/// test rather than being silently ignored.
const FOREIGN: &[(&str, &str)] = &[
    (
        "container_memory_working_set_bytes",
        "cadvisor（kubelet 内置）",
    ),
    ("kube_deployment_spec_replicas", "kube-state-metrics"),
    (
        "kube_deployment_status_replicas_available",
        "kube-state-metrics（Deployment 层面的可用副本数；读数取自各 Pod 的 ready，而 Pod 卡在 Terminating 时 API 仍把它记为 ready，所以这份计数在那种故障下不动）",
    ),
    ("kube_node_status_allocatable", "kube-state-metrics"),
    (
        "kube_node_status_capacity",
        "kube-state-metrics（与 allocatable 同源，相减即节点为非 Pod 工作留出的份额）",
    ),
    ("kube_node_status_condition", "kube-state-metrics"),
    (
        "kube_persistentvolumeclaim_resource_requests_storage_bytes",
        "kube-state-metrics",
    ),
    ("kube_pod_container_info", "kube-state-metrics"),
    (
        "kube_pod_init_container_info",
        "kube-state-metrics（init 容器单独一个序列，不在 container_info 里）",
    ),
    ("kube_pod_container_resource_limits", "kube-state-metrics"),
    (
        "kube_pod_container_status_last_terminated_reason",
        "kube-state-metrics",
    ),
    (
        "kube_pod_container_status_restarts_total",
        "kube-state-metrics",
    ),
    (
        "kube_pod_deletion_timestamp",
        "kube-state-metrics (present only while deletionTimestamp is set: a graceful deletion clears it in seconds, so a timestamp that persists is the wedge itself -- the API took the deletion and the runtime did not release the container, a state no other series reports)",
    ),
    ("kube_pod_owner", "kube-state-metrics"),
    ("kube_pod_status_phase", "kube-state-metrics"),
    ("node_filesystem_avail_bytes", "node-exporter"),
    ("node_filesystem_size_bytes", "node-exporter"),
    ("up", "Prometheus 采集目标存活序列"),
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// Every rule in the chart, name to promql, in file order.
fn chart_rules() -> Vec<(String, String)> {
    let path = repo_root().join(CHART_CONFIG);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("chart config unreadable at {}: {e}", path.display()));
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("chart config is not valid JSON");
    value["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("observability.infra_watch.rules is not an array")
        .iter()
        .map(|rule| {
            (
                rule["name"].as_str().unwrap_or_default().to_string(),
                rule["promql"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

#[test]
fn every_series_an_alert_rule_reads_is_one_something_produces() {
    let produced: BTreeSet<&str> = PRODUCED.iter().map(|(n, _)| *n).collect();
    let foreign: BTreeSet<&str> = FOREIGN.iter().map(|(n, _)| *n).collect();

    let mut unknown: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (rule, promql) in chart_rules() {
        for name in metric_names_in(&promql) {
            if produced.contains(name.as_str()) || foreign.contains(name.as_str()) {
                continue;
            }
            unknown.entry(name).or_default().push(rule.clone());
        }
    }

    assert!(
        unknown.is_empty(),
        "告警规则读了本仓库不产出、也没登记为外来序列的名字（这条规则永远不会触发）:\n{}",
        unknown
            .iter()
            .map(|(name, rules)| format!("  {name} <- {}", rules.join(", ")))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// A rule whose expression does not parse never fires, and the check above
/// passes it for the same reason a blank panel passes: every series it names is
/// really produced.
///
/// The shape this catches is the one a hand edit makes: an operator dropped
/// between two operands that a matching clause then sits in front of. Nothing
/// in this repository reads PromQL grammar, so before this test the only thing
/// between such an edit and an alert that stays quiet through its own incident
/// was a person noticing. A rule and a panel fail the same way here, which is
/// why both sides of this contract ask the same question about their text.
#[test]
fn every_expression_a_rule_writes_is_one_prometheus_can_parse() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for complaint in shape_complaints(&promql) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "告警规则的表达式 Prometheus 解析不了，这条规则永远不会触发:\n{}",
        complaints.join("\n")
    );
}

/// A rule that promises a duration has to read one. Comparing a series against
/// its own value `offset` a window back is a comparison of two samples, and two
/// samples that agree are not a duration -- least of all when the producer is
/// periodic, because then the two land on the same phase whenever the period
/// divides the offset.
///
/// The rule this was written for read a reaper's zombie count that way with a
/// 30m offset over a 15-second producer sampled every minute: it fired 45 times
/// in three days on a reaper that was working, each firing one poll interval
/// long, while its summary claimed PID 1 had held those children "for 30m". The
/// shipped text now reads `min_over_time(...[30m]) > 0`, which is the aggregate
/// that answers "non-zero throughout" -- and the same edit is owed by any rule
/// that reaches for the offset-equality shape to mean "persisted".
#[test]
fn a_rule_that_promises_a_duration_reads_a_window_not_two_samples() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for complaint in lagged_equality_complaints(&promql) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "告警规则用一个采样点和它 offset 之后的自己判「持续」，周期性的产出方会让它在健康系统上反复触发:\n{}",
        complaints.join("\n")
    );
}

/// The rule that says a role is unowned has to wait out the handover the lease
/// allows, or it reports the ordinary replacement of a pod as an unowned role.
///
/// That wait is the term plus the period the holder is allowed to take between
/// two asks, and both numbers live in the code that publishes the role series
/// rather than in the rule. Read here instead of argued in a summary, because a
/// window written into a rule drifts from the constants it was chosen against
/// the moment either side moves — and the drift is silent: the rule keeps
/// firing, just on healthy handovers.
#[test]
fn the_unowned_role_rule_waits_out_the_handover_the_lease_allows() {
    let (_, promql) = chart_rules()
        .into_iter()
        .find(|(name, _)| name == "background_loop_role_unowned")
        .expect(
            "the unowned-role rule is gone, and with it the only reading that says the \
             single-writer work a role guards is being done by nobody",
        );

    // The window is the bracketed term of the aggregation that asks whether the
    // role was held at any point in it.
    let window = promql
        .split_once("max_over_time(")
        .and_then(|(_, rest)| rest.split_once('['))
        .and_then(|(_, rest)| rest.split_once(']'))
        .and_then(|(text, _)| promql::duration_seconds(text))
        .unwrap_or_else(|| panic!("the rule's window is not readable as a duration: {promql}"));

    let term = cog_core::owner_lease::TERM.as_secs();
    let ask = cog_core::owner_lease::ASK_PERIOD.as_secs();
    assert!(
        window >= term + ask,
        "the unowned-role rule looks back {window}s, less than the {handover}s a handover may \
         take (a term of {term}s plus one ask period of {ask}s), so a killed holder's ordinary \
         replacement would be reported as a role nobody holds: {promql}",
        handover = term + ask
    );
}

/// The other half of the same promise: a rule that certifies a window has to
/// require the window to have been observed.
///
/// `min_over_time(x[30m]) > 0` reads the samples that exist in the window, so
/// for a series younger than the window it is a statement about whatever
/// samples are there -- the pod-start case, where one scrape of a fresh process
/// satisfies a half-hour claim. Replayed over six hours of the deployment's own
/// series, the shipped `orphans_unreaped` rule fires five times without its
/// coverage term, once per pod start, and not at all with it.
#[test]
fn a_rule_that_certifies_a_window_requires_the_window_to_be_covered() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for complaint in uncovered_window_complaints(&promql) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "告警规则用一个区间聚合判断整个窗口，却没有要求窗口被铺满——序列比窗口年轻时这句话是假的:\n{}",
        complaints.join("\n")
    );
}

#[test]
fn every_series_recorded_as_produced_is_still_published_there() {
    let root = repo_root();
    let mut missing: Vec<String> = Vec::new();
    for (name, source) in PRODUCED {
        let text = std::fs::read_to_string(root.join(source))
            .unwrap_or_else(|e| panic!("{source} unreadable: {e}"));
        if !carries_the_producer(&text, name) {
            missing.push(format!("{name} 记在 {source}，该文件里找不到这个名字"));
        }
    }

    assert!(
        missing.is_empty(),
        "PRODUCED 表登记的产出点已经不存在，登记本身成了空头许可:\n{}",
        missing.join("\n")
    );
}

/// FOREIGN is an allow-list, and the one thing an allow-list must never name is
/// something this workspace publishes.
///
/// The producer-existence check above only walks PRODUCED, so filing a produced
/// series under FOREIGN retires that check in silence: the name stops being
/// looked for in its source file, `every_series_an_alert_rule_reads_is_one_something_produces`
/// keeps accepting any rule that reads it, and if the producer is deleted later
/// nothing goes red.
///
/// The reading lives in `common/closed_set.rs` rather than here: the dashboard
/// contract sorts its series into the same two tables and has the same hole, so
/// the criterion is one function with two call sites instead of two copies that
/// drift apart.
#[test]
fn no_foreign_name_is_one_the_closed_set_publishes() {
    let filed_here = closed_set::published_by_this_build(FOREIGN);

    assert!(
        filed_here.is_empty(),
        "这些名字被登记成外来序列，却是本仓库闭集里的产出名——登记成外来它就不再被查产出点:\n{}",
        filed_here.join("\n")
    );
}

#[test]
fn every_label_value_a_rule_has_to_select_is_selected_by_one() {
    use cog_github::redrive_budget::{BudgetSide, RedriveRefusal};

    let rules = chart_rules();
    let domains: [(&str, Vec<&str>); 2] = [
        (
            cog_github::redrive_budget::REDRIVE_REFUSALS_METRIC.as_str(),
            RedriveRefusal::ALL.iter().map(|r| r.as_str()).collect(),
        ),
        (
            cog_github::redrive_budget::REDRIVE_BUDGET_LOSSES_METRIC.as_str(),
            BudgetSide::ALL.iter().map(|s| s.as_str()).collect(),
        ),
    ];

    let mut unread: Vec<String> = Vec::new();
    for (metric, values) in domains {
        for value in values {
            let quoted = format!("\"{value}\"");
            let read = rules
                .iter()
                .any(|(_, promql)| promql.contains(metric) && promql.contains(&quoted));
            if !read {
                unread.push(format!("{metric}{{…=\"{value}\"}}"));
            }
        }
    }

    assert!(
        unread.is_empty(),
        "这些计数器的取值没有任何规则在读，对应的事件会静默发生（名字被读了不算，取值没被读）: {unread:?}"
    );
}

/// Rule names this workspace raises about itself, which must stay outside the
/// configured rule set.
///
/// The watcher adopts the rows it finds for the rules it evaluates and resolves
/// the ones whose condition no longer holds. A configured rule carrying one of
/// these names would therefore let the watcher close the very row that reports
/// the rule set as incomplete — the one alert whose subject is the missing
/// judgement would be cancelled by the judgement that is missing.
///
/// The list is every rule this workspace raises about itself, whichever
/// component raises it: the puller's verdicts are read by the same watcher,
/// through the same rule set, as the watcher's own.
#[test]
fn the_watchers_own_rule_names_are_not_configured_rule_names() {
    let own = [
        cog_observability::infra_watch::EVAL_FAILURE_RULE,
        cog_observability::config_delivery::CONFIG_DECLARATION_RULE,
        cog_reflection::observability_stack::STACK_NOT_CONVERGED_RULE,
        cog_reflection::CANARY_GATE_BLIND_RULE,
    ];
    for name in own {
        assert!(!name.is_empty());
    }
    let mut distinct = own.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        own.len(),
        "two self-alerts share a rule name"
    );

    let configured: BTreeSet<String> = chart_rules().into_iter().map(|(name, _)| name).collect();
    let collision: Vec<&&str> = own
        .iter()
        .filter(|name| configured.contains(**name))
        .collect();
    assert!(
        collision.is_empty(),
        "这些自我告警的规则名同时存在于 configuration 的规则集里，watcher 会把它们当成自己的行来 adopt/resolve: {collision:?}"
    );
}

#[test]
fn the_tables_hold_no_series_that_no_rule_reads() {
    let read: BTreeSet<String> = chart_rules()
        .iter()
        .flat_map(|(_, promql)| metric_names_in(promql))
        .collect();

    let stale: Vec<&str> = PRODUCED
        .iter()
        .map(|(n, _)| *n)
        .chain(FOREIGN.iter().map(|(n, _)| *n))
        .filter(|name| !read.contains(*name))
        .collect();

    assert!(
        stale.is_empty(),
        "表里登记的名字没有任何规则在读，说明规则被删或改名后表没跟着收敛: {stale:?}"
    );
}

/// Every name the closed set publishes, against every face that reads one.
///
/// The test above asks the question in one direction only: a series registered
/// in `PRODUCED` must be read by a rule. It cannot see a name that was never
/// registered anywhere, which is exactly the shape of a series that is
/// published, described in `metric_help`, and read by nobody. This walks the
/// other way -- from `cog_core::metric_names::ALL`, which the recording macros
/// generate, so a name in it is published by this build by construction -- and
/// requires each one to be accounted for by an alert rule, by a dashboard
/// panel, or by an entry here.
///
/// There are two verdicts and no third. `Elsewhere` names the surface that
/// states the same fact, which makes leaving this name unread a decision
/// instead of an oversight. `Gap` records a fact that nothing states and
/// nothing reports: it is a debt, kept as a count by `GAPS_AT_CENSUS` so the
/// next one cannot arrive unnoticed, and never a statement that the reading is
/// not worth having.
enum Unread {
    /// The same fact is stated elsewhere; the surface is named.
    Elsewhere(&'static str),
    /// Nothing states it. A debt, counted by `GAPS_AT_CENSUS`.
    Gap(&'static str),
}

/// Names no face reads, with the verdict.
///
/// The census that filled this table landed a reader for nine names whose own
/// help text said what should read them or what their non-zero meant, and whose
/// failure was otherwise silent: the signing refusals, the registry reclaim
/// debt, the pool signal, the usage-silence cells, the sample log's capacity
/// verdict, the memory backlog past the re-drive window, the audited channel's
/// refusals, the version contract, and the worktree index. The entries below are
/// what remained, each with the reason it was not closed in that pass.
///
/// `Elsewhere` is a decision: the same fact is already stated by a surface that
/// is read, named here. `Gap` is a debt: nothing states the fact, and the reason
/// it could not be closed is what the entry carries -- a missing declared bound,
/// a cadence that no reading publishes, or a repair that is a policy decision
/// rather than a reader to write. `GAPS_AT_CENSUS` counts them so the next one
/// must be classified deliberately.
const UNREAD: &[(&str, Unread)] = &[
    // The denominator of the rule on the aged-out backlog: the sibling counts
    // every unextracted raw, this one counts the part the system will retry by
    // itself, and both are written by the same scan. A reader of the pair is
    // what the rule above is.
    (
        "memory_unextracted_raw",
        Unread::Elsewhere("the actionable subset is read by memory_raw_backlog_aged_out; this is its denominator"),
    ),
    (
        "memory_operations_total",
        Unread::Gap("memory backend operation volume; the instrumented facade's own comment says the counter is diluted by the archive housekeeping loop, so a rate threshold would track that loop rather than demand"),
    ),
    (
        "memory_operation_latency_ms",
        Unread::Gap("memory backend operation latency; no latency bound is declared anywhere in the repository to hang a threshold on"),
    ),
    (
        "memory_operation_errors_total",
        Unread::Gap("the memory backend's failure counter; a failing backend reaches the user as a failed task and no reading names memory as the cause -- the repair is a threshold policy for what error rate is a fault"),
    ),
    (
        "metrics_retired_rows_removed",
        Unread::Gap("rows of retired names the last release pass deleted; the help reads a table whose value stays non-zero as one not being drained, but 'stays' is a property of a sequence of passes and the pass cadence is not published by any series"),
    ),
    (
        "metrics_samples_rows",
        Unread::Elsewhere("one operand of the capacity verdict that metrics_log_at_floor_capacity reads"),
    ),
    (
        "metrics_samples_budget_rows",
        Unread::Elsewhere("the other operand of that same verdict"),
    ),
    (
        "metrics_samples_bytes",
        Unread::Elsewhere("the help says it lags the row count and is never the pruning criterion; the capacity verdict is the reading with a criterion"),
    ),
    (
        "tier_migration_total",
        Unread::Elsewhere("trace_tier_migration_failing, trace_tier_migration_stale and trace_tier_demotion_stalled read the same face's failures, staleness and backlog"),
    ),
    (
        "cogneva_worktree_index_present",
        Unread::Elsewhere("the sampling loop's liveness is stated by background_loop_stalled over cogneva_loop_tick_age_seconds"),
    ),
    (
        "cogneva_version_contract_checks_total",
        Unread::Elsewhere("the denominator of version_contract_violated: the help says it is what separates a contract that holds from a judgement that never ran"),
    ),
    (
        "cogneva_version_declared_info",
        Unread::Elsewhere("the help calls it an identity label rather than a measurement; the declared version is also on the image tag and in the release tag the artifact is built from"),
    ),
    (
        "cogneva_version_commits_since_release",
        Unread::Gap("how far the running code has moved past the release; no bound is declared, so a rule would have to invent the policy it is meant to enforce"),
    ),
    (
        "evolution_generated_change_files_total",
        Unread::Gap("generated-change fidelity: measured, logged, and read by nothing; the ratio needs a declared fidelity bound that does not exist"),
    ),
    (
        "evolution_generated_change_files_faithful",
        Unread::Gap("the faithful half of the same unread ratio"),
    ),
    (
        "evolution_generated_change_hunks_total",
        Unread::Gap("the same unread fidelity face at hunk granularity; the missing bound is the same one"),
    ),
    (
        "evolution_generated_change_hunks_faithful",
        Unread::Gap("same face, hunk granularity, faithful half"),
    ),
    (
        "cogneva_registry_pruned_tags_total",
        Unread::Elsewhere("registry_reclaim_debt_unpaid reads the deletions the tag server did not reclaim; the count of the ones that succeeded adds no other fact"),
    ),
    (
        "cogneva_registry_rebuild_hold_secs_total",
        Unread::Gap("seconds the tag server was held away per induced restart; the help's criterion is a ratio against the tags deleted, so the reader needs a pair and a bound on how much deferral is acceptable, neither declared"),
    ),
    (
        "cogneva_buildah_store_rounds_total",
        Unread::Gap("the help says read the rounds against the completion timestamp, but the bound is the attempt cadence and the store round is driven by the build slot rather than a declared period, so no window can be bounded from a reading that exists"),
    ),
    (
        "cogneva_buildah_store_reading_unix",
        Unread::Gap("the other half of that pair; the same missing cadence is what a staleness bound would need"),
    ),
    (
        "cogneva_buildah_store_pruned_images_total",
        Unread::Gap("written only when a round freed an image, so absence means no round ever has; same unattested cadence"),
    ),
    (
        "cogneva_buildah_store_pruned_layers_total",
        Unread::Gap("same face as the pruned image count"),
    ),
    (
        "cogneva_buildah_store_freed_bytes_total",
        Unread::Gap("same face; bytes freed, written only when both ends of the round were measurable"),
    ),
    (
        "cogneva_buildah_store_kept_images",
        Unread::Gap("the help calls it the reading that says the keep set is doing something; nothing reads it and no keep-set bound is declared"),
    ),
    (
        "cogneva_buildah_store_live_images",
        Unread::Gap("the help names the one way this pass could remove a base image that is in use -- a live reading of zero -- and nothing reads it; the highest-value entry in this family, still blocked on the same unattested cadence"),
    ),
    (
        "cogneva_rollout_job_cpu_throttled_ratio",
        Unread::Gap("share of the newest judgement run's CFS periods throttled by its own limit; no throttling bound is declared"),
    ),
    (
        "cogneva_rollout_job_memory_peak_ratio",
        Unread::Gap("peak against the declared limit; the help warns a healthy run reads near the top of a narrow band, so the ratio itself is not a usable threshold and the bound that would be is not declared"),
    ),
    (
        "cogneva_rollout_job_reading_unix",
        Unread::Gap("ties the two readings above to the run that produced them; a reader needs the run cadence, which is per-rollout and published by no series"),
    ),
    (
        "cogneva_mainline_superseded_rollout_total",
        Unread::Gap("1 or 0 per round on whether the carried revision had been overtaken; the help says the zeros keep the question visible and nothing reads the answer -- a reader needs the round cadence to bound the window"),
    ),
    (
        "cogneva_mainline_ci_verdict_total",
        Unread::Gap("CI verdicts read for the revision about to be promoted, pass/fail/no_evidence; a red verdict stops the rollout but no rule reads the count, so the reasons below have no surface"),
    ),
    (
        "cogneva_mainline_ci_no_verdict_reason_total",
        Unread::Gap("why the CI question came back without a verdict; the reasons include pending, which is normal, and the unreadable/config ones, so a rule has to enumerate the non-normal reasons or fire on every wait -- a fail-open list whose policy is not declared"),
    ),
    (
        "llm_upstream_failures_total",
        Unread::Elsewhere("llm_calls_all_failing reads llm_calls_total{result=error} and the health table states the current consecutive failures; the help says this cumulative total and the backoff window describe one history"),
    ),
    (
        "llm_request_param_clamped_total",
        Unread::Elsewhere("the producer's own comment states the verdict's readings are the pool entry's key presence and llm_usage_verdict_measured, which llm_usage_verdict_unmeasured reads"),
    ),
];

/// How many `Gap` entries the census left. A new series that no face reads and
/// that nothing else states must be classified, and calling it a gap raises
/// this number on purpose -- the point of the ratchet is that the increase is a
/// decision someone made, not a drift nobody saw.
const GAPS_AT_CENSUS: usize = 23;

#[test]
fn every_series_the_closed_set_publishes_has_a_decided_reader() {
    let rule_read: BTreeSet<String> = chart_rules()
        .iter()
        .flat_map(|(_, promql)| metric_names_in(promql))
        .collect();
    let panel_read = dashboard::series();

    // A histogram is published under its base name and read under one of the
    // recording suffixes, and the closed set holds the base name -- so a reader
    // naming the suffix has read the family. The suffixes are the recording
    // convention and nothing else: `memory_unextracted_raw` has a sibling whose
    // name merely starts the same way, and a prefix match would read that
    // sibling as this family's reader.
    let suffixes = ["_bucket", "_sum", "_count"];
    let read = |names: &BTreeSet<String>, name: &str| {
        names.contains(name)
            || suffixes
                .iter()
                .any(|s| names.contains(&format!("{name}{s}")))
    };

    let listed: BTreeSet<&str> = UNREAD.iter().map(|(n, _)| *n).collect();

    let unclassified: Vec<&str> = cog_core::metric_names::ALL
        .iter()
        .map(|n| n.as_str())
        .filter(|n| !read(&rule_read, n) && !read(&panel_read, n) && !listed.contains(n))
        .collect();
    assert!(
        unclassified.is_empty(),
        "闭集里的这些序列没有任何告警规则或面板在读，也没在 UNREAD 里给出判词——补读者，或写清那份事实在哪个面上已经可见，或登记成欠账:\n{}",
        unclassified.join("\n")
    );

    let published: BTreeSet<&str> = cog_core::metric_names::ALL
        .iter()
        .map(|n| n.as_str())
        .collect();

    let unknown: Vec<&str> = UNREAD
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| !published.contains(n))
        .collect();
    assert!(
        unknown.is_empty(),
        "UNREAD 里这些名字不在闭集里，登记已过期（改名或删除后没跟着收敛）: {unknown:?}"
    );

    let read_but_listed: Vec<&str> = UNREAD
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| read(&rule_read, n) || read(&panel_read, n))
        .collect();
    assert!(
        read_but_listed.is_empty(),
        "UNREAD 里这些名字其实已经有读者了，豁免是假的——读者已落地，登记该删: {read_but_listed:?}"
    );

    // The reason is the entry's whole value: an exemption without one is the
    // "not needed" this table exists to refuse, and a debt without one cannot be
    // acted on. Reading it here is also what keeps the payload from being an
    // unused field -- a verdict that states nothing is not a verdict.
    let unreasoned: Vec<&str> = UNREAD
        .iter()
        .filter(|(_, v)| {
            let reason = match v {
                Unread::Elsewhere(reason) | Unread::Gap(reason) => *reason,
            };
            reason.split_whitespace().count() < 5
        })
        .map(|(name, _)| *name)
        .collect();
    assert!(
        unreasoned.is_empty(),
        "UNREAD 里这些条目没写清理由（豁免要说出那份事实在哪个面上、欠账要说出为什么这轮补不了）: {unreasoned:?}"
    );

    let gaps = UNREAD
        .iter()
        .filter(|(_, v)| matches!(v, Unread::Gap(_)))
        .count();
    assert!(
        gaps <= GAPS_AT_CENSUS,
        "零读者序列的欠账数从 {GAPS_AT_CENSUS} 涨到 {gaps}：新增的零读者序列必须当轮补读者，或把它的成因写进 UNREAD 并同步改 GAPS_AT_CENSUS（改这个数就是承认多欠一笔）"
    );
}

/// The gate-blindness verdict must have a successor.
///
/// A canary gate that could not read used to leave its verdict in the rollout
/// note and a warn log, and nothing else: the promotion went through and the
/// only record of the judgement that was missing was a sentence nobody reads.
/// That is the same shape as a rule whose expression names a series nothing
/// produces — it stays quiet through exactly the incident it exists for. The
/// fix is that the verdict reaches the persistent alert surface, which means
/// the puller has to be handed that sink; without this wiring the alert surface
/// is empty while the code around it looks complete.
#[test]
fn the_puller_is_handed_the_persistent_alert_sink() {
    let plugin = read("crates/cog-reflection/src/plugin.rs");
    let construct = plugin
        .find("GitOpsPuller::new(")
        .expect("plugin.rs no longer constructs the GitOps puller here");
    // The whole block that constructs and starts this process's puller: from
    // the enabled guard to the spawn.
    let start = plugin[..construct]
        .rfind("promotion.gitops.puller_enabled")
        .expect("拉取端的构造不在 puller_enabled 守卫里了");
    let end = plugin[construct..]
        .find("run_puller_loop")
        .map(|i| construct + i)
        .expect("the puller is no longer started right after it is constructed");
    let block = &plugin[start..end];
    assert!(
        block.contains("PersistentAlertSink"),
        "拉取端没拿到持久化告警面：判据读不出数的结论又只剩台账与日志了"
    );
    assert!(
        block.contains("with_alert_sink"),
        "拉取端拿到了 sink 但没有交给自己（构造完之后必须 with_alert_sink）"
    );
    assert!(
        block.contains("report-only"),
        "sink 缺席时的降级路径没有留下痕迹：那时告警面是空的，而代码看起来什么都有"
    );
}

// ── 表达式的值能不能满足它自己的条件 ─────────────────────────────────────────

/// A range of values, open or closed at each end. Infinities stand for "no
/// bound"; an infinite end is never inclusive.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Range {
    low: f64,
    low_inclusive: bool,
    high: f64,
    high_inclusive: bool,
}

impl Range {
    fn everything() -> Self {
        Self {
            low: f64::NEG_INFINITY,
            low_inclusive: false,
            high: f64::INFINITY,
            high_inclusive: false,
        }
    }

    fn at(value: f64) -> Self {
        Self {
            low: value,
            low_inclusive: true,
            high: value,
            high_inclusive: true,
        }
    }

    fn above(value: f64, inclusive: bool) -> Self {
        Self {
            low: value,
            low_inclusive: inclusive,
            high: f64::INFINITY,
            high_inclusive: false,
        }
    }

    fn below(value: f64, inclusive: bool) -> Self {
        Self {
            low: f64::NEG_INFINITY,
            low_inclusive: false,
            high: value,
            high_inclusive: inclusive,
        }
    }

    /// Whether the two ranges share at least one value.
    fn intersects(&self, other: &Range) -> bool {
        if self.low > self.high || other.low > other.high {
            return false;
        }
        let (low, low_inclusive) = if self.low > other.low {
            (self.low, self.low_inclusive)
        } else if other.low > self.low {
            (other.low, other.low_inclusive)
        } else {
            (self.low, self.low_inclusive && other.low_inclusive)
        };
        let (high, high_inclusive) = if self.high < other.high {
            (self.high, self.high_inclusive)
        } else if other.high < self.high {
            (other.high, other.high_inclusive)
        } else {
            (self.high, self.high_inclusive && other.high_inclusive)
        };
        low < high || (low == high && low_inclusive && high_inclusive)
    }
}

fn is_word_byte(c: u8) -> bool {
    (c as char).is_ascii_alphanumeric() || c == b'_'
}

/// Split on the leftmost top-level `and` / `or` / `unless`, if there is one.
fn split_set_operator(expr: &str) -> Option<(&'static str, &str, &str)> {
    let bytes = expr.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] as char {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
        if depth == 0 && bytes[i].is_ascii_alphabetic() {
            let start = i;
            while i < bytes.len() && is_word_byte(bytes[i]) {
                i += 1;
            }
            let op = match &expr[start..i] {
                "and" => Some("and"),
                "or" => Some("or"),
                "unless" => Some("unless"),
                _ => None,
            };
            if let Some(op) = op {
                let mut right_start = i;
                while right_start < bytes.len()
                    && (bytes[right_start] as char).is_ascii_whitespace()
                {
                    right_start += 1;
                }
                // `and on(pod) (...)` carries a matching clause between the
                // operator and its right operand. The clause does not change
                // which side supplies the values, so it stays attached to the
                // right half.
                return Some((op, &expr[..start], &expr[right_start..]));
            }
            continue;
        }
        i += 1;
    }
    None
}

/// Strip one layer of balanced parentheses wrapping the whole expression.
fn strip_outer_parens(expr: &str) -> &str {
    let mut cur = expr.trim();
    loop {
        if !(cur.starts_with('(') && cur.ends_with(')')) {
            return cur;
        }
        let mut depth = 0i32;
        let bytes = cur.as_bytes();
        for (i, b) in bytes.iter().enumerate() {
            match *b as char {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    // The opening paren closed before the end: not a wrapper.
                    if depth == 0 && i + 1 != bytes.len() {
                        return cur;
                    }
                }
                _ => {}
            }
        }
        if depth != 0 {
            return cur;
        }
        cur = cur[1..cur.len() - 1].trim();
    }
}

/// The range a leaf expression reports, or `None` when this cannot tell.
fn leaf_range(expr: &str) -> Option<Range> {
    let expr = strip_outer_parens(expr);
    let bytes = expr.as_bytes();
    let mut depth = 0i32;
    let mut found: Option<(usize, &'static str)> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] as char {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            let rest = &expr[i..];
            let op = if rest.starts_with("==") {
                Some("==")
            } else if rest.starts_with("!=") {
                Some("!=")
            } else if rest.starts_with("<=") {
                Some("<=")
            } else if rest.starts_with(">=") {
                Some(">=")
            } else if rest.starts_with('=') || rest.starts_with("=~") || rest.starts_with("!~") {
                // A bare `=` or a regex matcher belongs inside braces; at depth
                // zero it is a typo this check does not own.
                None
            } else if rest.starts_with('<') {
                Some("<")
            } else if rest.starts_with('>') {
                Some(">")
            } else {
                None
            };
            if let Some(op) = op {
                found = Some((i, op));
                i += op.len();
                continue;
            }
        }
        i += 1;
    }
    let (at, op) = found?;
    let rhs = expr[at + op.len()..].trim();
    // `bool` turns the comparison into a 0/1 indicator rather than a filter, so
    // the operand's own value no longer arrives. What the indicator can hold is
    // bounded, but reading its exact domain is not this check's job.
    if rhs.starts_with("bool") {
        return None;
    }
    let value: f64 = rhs.parse().ok()?;
    Some(match op {
        "==" => Range::at(value),
        "<" => Range::below(value, false),
        "<=" => Range::below(value, true),
        ">" => Range::above(value, false),
        ">=" => Range::above(value, true),
        _ => return None,
    })
}

/// The ranges of values an expression can hand to its rule's condition.
///
/// Only the outermost operators decide. `and` and `unless` report the left
/// operand's values and `or` reports either side's, so this walks the top-level
/// set operators down to the leaves they leave. Anything unrecognised widens to
/// every real number.
fn reported_ranges(expr: &str) -> Vec<Range> {
    match split_set_operator(expr) {
        Some(("and" | "unless", left, _)) => reported_ranges(left),
        Some(("or", left, right)) => {
            let mut both = reported_ranges(left);
            both.extend(reported_ranges(right));
            both
        }
        _ => vec![leaf_range(expr).unwrap_or_else(Range::everything)],
    }
}

/// The values a condition accepts, or `None` when it accepts too much to pin
/// down (which reads as "no complaint").
fn accepted_range(condition: &cog_core::AlertCondition) -> Option<Range> {
    use cog_core::AlertCondition as C;
    Some(match condition {
        C::GreaterThan(t) => Range::above(*t, false),
        C::GreaterThanOrEqual(t) => Range::above(*t, true),
        C::LessThan(t) => Range::below(*t, false),
        C::LessThanOrEqual(t) => Range::below(*t, true),
        C::Equal(t) => Range::at(*t),
        // Every real number but one satisfies `!= t`, and an expression that
        // can only report a single point is not what a threshold rule is for.
        C::NotEqual(_) => return None,
    })
}

/// Complaints about a rule whose condition can never be satisfied by the value
/// its own expression reports.
///
/// A comparison without `bool` is a filter: Prometheus keeps the operand's own
/// value, so `x == 0` hands the condition 0 rather than 1, and a condition of
/// `> 0` then asks for a value the expression cannot produce. The rule is
/// silent for good, through exactly the incident it was written for — and the
/// name-level checks cannot see it, because every series in the expression is
/// really produced and the expression really parses.
///
/// This is not an evaluator. It reads the outermost operators, and any shape it
/// cannot classify counts as satisfiable: a shape it misses leaves the rule as
/// silent as it already is, while a wrong complaint would block a rule that
/// works.
fn unreachable_condition_complaints(
    expr: &str,
    condition: &cog_core::AlertCondition,
) -> Vec<String> {
    let Some(accepted) = accepted_range(condition) else {
        return Vec::new();
    };
    let reachable = reported_ranges(expr);
    if reachable.iter().any(|r| r.intersects(&accepted)) {
        return Vec::new();
    }
    vec![format!(
        "表达式报给条件的值落在 {reachable:?}，而条件的取值域是 {accepted:?}，两者不相交：\
         这条规则永远不会触发（比较运算符不带 `bool` 时是过滤，报的是操作数自己的值，\
         不是比较的结果）"
    )]
}

fn chart_rules_with_condition() -> Vec<(String, String, cog_core::AlertCondition)> {
    let path = repo_root().join(CHART_CONFIG);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("chart config unreadable at {}: {e}", path.display()));
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("chart config is not valid JSON");
    value["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("observability.infra_watch.rules is not an array")
        .iter()
        .map(|rule| {
            let condition = serde_json::from_value(rule["condition"].clone())
                .unwrap_or_else(|e| panic!("rule {} has no readable condition: {e}", rule["name"]));
            (
                rule["name"].as_str().unwrap_or_default().to_string(),
                rule["promql"].as_str().unwrap_or_default().to_string(),
                condition,
            )
        })
        .collect()
}

#[test]
fn every_condition_can_be_reached_by_the_value_its_rule_reports() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql, condition) in chart_rules_with_condition() {
        for complaint in unreachable_condition_complaints(&promql, &condition) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "这些规则的条件永远满足不了，规则不会触发:\n{}",
        complaints.join("\n")
    );
}

#[test]
fn a_condition_its_own_filter_contradicts_is_reported() {
    use cog_core::AlertCondition as C;
    // The shape this check exists for: the filter pins the value to 0 and the
    // condition then asks for something greater than 0.
    let complaints = unreachable_condition_complaints(
        "cogneva_aof_repair_verdict_published == 0",
        &C::GreaterThan(0.0),
    );
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("永远不会触发"), "{}", complaints[0]);

    // The same expression against a threshold it can reach is accepted, which
    // is what the rule was rewritten to.
    assert!(unreachable_condition_complaints(
        "cogneva_aof_repair_verdict_published < 1",
        &C::LessThan(1.0)
    )
    .is_empty());
    assert!(unreachable_condition_complaints(
        "cogneva_aof_repair_verdict_published == 0",
        &C::LessThan(1.0)
    )
    .is_empty());
}

#[test]
fn filters_the_written_rules_use_are_not_reported() {
    use cog_core::AlertCondition as C;
    let fine = [
        ("cogneva_aof_repair_dropped_bytes > 0", C::GreaterThan(0.0)),
        (
            "sum by (intent) (increase(cogneva_change_fate_total{fate=\"retired\"}[24h])) > 0 \
             and on(intent) (sum by (intent) (increase(cogneva_change_fate_total{fate=\"landed\"}[24h])) == 0)",
            C::GreaterThan(0.0),
        ),
        (
            "(min_over_time(cogneva_process_zombies[30m]) > 0) \
             and (count_over_time(cogneva_process_zombies[1h]) \
             > count_over_time(cogneva_process_zombies[30m]))",
            C::GreaterThan(0.0),
        ),
        (
            "count(kube_pod_init_container_info{namespace=\"cogneva\", container=\"aof-repair\"} \
             and on(pod) kube_pod_status_phase{namespace=\"cogneva\", phase=\"Running\"} == 1) \
             - (count(cogneva_aof_repair_pass_timestamp_seconds) or vector(0))",
            C::GreaterThan(0.0),
        ),
        ("cogneva_runtime_asset_manifest_absent", C::GreaterThan(0.0)),
        (
            "container_memory_working_set_bytes{container!=\"\"} \
             / on(namespace, pod, container) (kube_pod_container_resource_limits{resource=\"memory\"} > 0)",
            C::GreaterThan(0.75),
        ),
    ];
    for (expr, condition) in fine {
        let complaints = unreachable_condition_complaints(expr, &condition);
        assert!(complaints.is_empty(), "{expr}: {complaints:?}");
    }
}

#[test]
fn a_cap_below_the_threshold_is_reported_and_an_unreadable_operand_is_not() {
    use cog_core::AlertCondition as C;
    assert_eq!(
        unreachable_condition_complaints("a < 5", &C::GreaterThan(10.0)).len(),
        1
    );
    // Widening beats guessing: an operand this cannot read is not a complaint.
    assert!(unreachable_condition_complaints("a > 2 * b", &C::GreaterThan(0.0)).is_empty());
    assert!(unreachable_condition_complaints("a > b", &C::GreaterThan(0.0)).is_empty());
    assert!(unreachable_condition_complaints("a == bool 0", &C::GreaterThan(0.0)).is_empty());
    assert!(unreachable_condition_complaints("a != 0", &C::GreaterThan(0.0)).is_empty());
}

#[test]
fn set_operators_decide_which_side_supplies_the_value() {
    use cog_core::AlertCondition as C;
    assert!(
        unreachable_condition_complaints("(a == 0) or (b > 5)", &C::GreaterThan(3.0)).is_empty()
    );
    assert_eq!(
        unreachable_condition_complaints("(a == 0) and (b > 5)", &C::GreaterThan(3.0)).len(),
        1
    );
    assert_eq!(
        unreachable_condition_complaints("(a == 0) unless (b > 5)", &C::GreaterThan(3.0)).len(),
        1
    );
}

// ── 时长承诺读的是不是窗口里的每一个样本 ─────────────────────────────────────

/// Index of the `)` closing a group that is already open, or `None` when the
/// expression ends first. `open` points just past the opening parenthesis.
fn matching_paren_end(expr: &str, open: usize) -> Option<usize> {
    let bytes = expr.as_bytes();
    let mut depth = 1i32;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] as char {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The operand of every `min_over_time` / `max_over_time` call in an expression,
/// with the name of the aggregate it belongs to. The range selector is dropped:
/// `[30m:1m]` says which samples are read, not what they are read from.
fn range_aggregate_operands(expr: &str) -> Vec<(&'static str, &str)> {
    let mut found = Vec::new();
    for name in ["min_over_time", "max_over_time"] {
        let mut from = 0usize;
        while let Some(at) = expr[from..].find(name) {
            let start = from + at;
            from = start + name.len();
            let rest = &expr[from..];
            // A call, not a longer identifier or a bare mention: nothing but
            // whitespace between the name and its opening parenthesis.
            let Some(open) = rest.find('(') else { break };
            if !rest[..open].trim().is_empty() {
                continue;
            }
            let inner_start = from + open + 1;
            let Some(inner_end) = matching_paren_end(expr, inner_start) else {
                continue;
            };
            let arg = expr[inner_start..inner_end].trim();
            let operand = match arg.rfind('[') {
                Some(bracket) if arg.ends_with(']') => arg[..bracket].trim(),
                _ => arg,
            };
            found.push((name, operand));
        }
    }
    found
}

/// Complaints about a range aggregate that reads a filtered operand.
///
/// A comparison without `bool` is a filter: Prometheus drops the samples that
/// fail it before the aggregate sees them, and the aggregate reports a value
/// built only from the survivors. `min_over_time((x < 1)[30m])` therefore
/// answers "was at least one sample below 1" -- one survivor already puts the
/// minimum below 1 -- while the rule it sits in promises that *every* sample
/// over the window was. The reading such a rule wants is the aggregate over the
/// raw series with the threshold left to the condition, `max_over_time(x[30m])
/// < 1`, which is false the moment a single sample disagrees, and which is why
/// the aggregate has to be the direction that reads all of them: the smallest
/// value for a promise of "none above", the largest for a promise of "none
/// below".
///
/// Only `min_over_time` and `max_over_time` are read here. They are the two
/// whose value lands on one side of their own filter by construction, so the
/// answer collapses to an existence check exactly; the sum-style aggregates
/// carry no such claim.
///
/// This is not an evaluator. An operand whose comparison is against something
/// other than a literal is not classified, and an unclassified operand is not a
/// complaint -- same direction as the other checks here: a shape this misses
/// leaves the rule as unread as it already is, while a wrong complaint would
/// block a rule that works.
fn filtered_range_aggregate_complaints(expr: &str) -> Vec<String> {
    let mut complaints = Vec::new();
    for (name, operand) in range_aggregate_operands(expr) {
        let Some(range) = leaf_range(operand) else {
            continue;
        };
        complaints.push(format!(
            "`{name}` 的操作数带着过滤比较（{operand}，取值域 {range:?}）：不带 `bool` 的比较是先过滤，\
             聚合只看得见通过的样本，于是它答的是「窗口里至少有一个样本在这一侧」，而不是规则承诺的\
             「窗口里的每一个样本都在这一侧」。把比较挪到聚合外面、阈值交给条件（例：\
             `max_over_time(x[30m]) < 1`），聚合成败由窗口里有没有样本越界决定"
        ));
    }
    complaints
}

#[test]
fn a_range_aggregate_that_reads_a_filter_is_reported() {
    // The shape this check exists for. The five-minute form shipped once and
    // fired on healthy workloads: a single scrape at zero was enough, because
    // the filter had already thrown away every sample that said otherwise.
    let shipped = "((min_over_time((kube_deployment_status_replicas_available < 1)[5m:1m]) \
                    and on(namespace, deployment) (kube_deployment_spec_replicas > 0)) \
                    and on(namespace, deployment) \
                    (count_over_time(kube_deployment_status_replicas_available[10m]) \
                     > count_over_time(kube_deployment_status_replicas_available[5m])))";
    let complaints = filtered_range_aggregate_complaints(shipped);
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(
        complaints[0].contains("至少有一个样本"),
        "{}",
        complaints[0]
    );

    // The same promise read from the raw series is accepted, in both
    // directions ...
    assert!(filtered_range_aggregate_complaints(
        "((max_over_time(kube_deployment_status_replicas_available[30m:1m]) \
           and on(namespace, deployment) (kube_deployment_spec_replicas > 0)) \
           and on(namespace, deployment) \
           (count_over_time(kube_deployment_status_replicas_available[1h]) \
            > count_over_time(kube_deployment_status_replicas_available[30m])))"
    )
    .is_empty());
    // ... and so is a comparison that arrives after the aggregate has read the
    // window, and a filter whose samples are counted rather than aggregated.
    for fine in [
        "(min_over_time(cogneva_process_zombies[30m]) > 0) and (count_over_time(cogneva_process_zombies[1h]) > count_over_time(cogneva_process_zombies[30m]))",
        "(max_over_time(llm_usage_verdict_measured{upstream=~\".+\"}[1h]) == 0) and on (upstream) (max_over_time(llm_upstream_healthy{upstream=~\".+\"}[1h]) == 1)",
        "count_over_time((cogneva_change_fate_total > 0)[24h]) > 0",
    ] {
        assert!(
            filtered_range_aggregate_complaints(fine).is_empty(),
            "{fine}"
        );
    }

    // Widening beats guessing: an operand this cannot read is not a complaint.
    assert!(filtered_range_aggregate_complaints("min_over_time((x < y)[30m])").is_empty());
    assert!(filtered_range_aggregate_complaints("max_over_time((x == bool 0)[30m])").is_empty());
}

#[test]
fn no_written_rule_reads_a_filter_through_a_range_aggregate() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for complaint in filtered_range_aggregate_complaints(&promql) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "这些规则把时长读成了「至少一个样本」:\n{}",
        complaints.join("\n")
    );
}
