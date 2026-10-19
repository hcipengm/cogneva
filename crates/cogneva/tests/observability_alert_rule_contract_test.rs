//! principle: feedback_a_metric_vanishing_from_the_reader_census_means_the_clause_is_illegal
//! principle: feedback_a_pair_exclusion_shipped_before_its_label_matches_nothing
//! principle: feedback_a_registration_table_grows_only_where_its_consumers_are
//! principle: feedback_an_aggregate_over_a_series_family_can_starve_while_every_element_has_a_reader
//! principle: feedback_an_alarm_moved_off_a_published_series_orphans_it_and_costs_a_census_entry
//!
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

use cog_gateway::security_gateway::{RangeReadBaseline, RANGE_READ_BASELINES};

#[path = "common/closed_set.rs"]
mod closed_set;
#[path = "common/dashboard.rs"]
mod dashboard;
#[path = "common/derivation.rs"]
mod derivation;
#[path = "common/producer.rs"]
mod producer;
#[path = "common/promql.rs"]
mod promql;
#[path = "common/unread.rs"]
mod unread;

use producer::carries_the_producer;
use promql::{
    bare_observation_reads, companion_age_bound, difference_reads, kept_labels_by_clause,
    lagged_equality_complaints, metric_names_in, shape_complaints, uncovered_window_complaints,
    unguarded_store_reading_complaints, windows_over,
};
use unread::{unresolved_readers, Unread};

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
        "crates/cog-core/src/contract/build_gate.rs",
    ),
    (
        "cogneva_build_gate_slots",
        "crates/cog-core/src/contract/build_gate.rs",
    ),
    // How long each held slot has been held, read by the rule that fires on a
    // hold longer than anyone is willing to wait. The refusal counter beside it
    // only exists where someone asked, and the starved case is the ask that
    // stopped being made.
    (
        "cogneva_build_gate_holder_seconds",
        "crates/cog-core/src/contract/build_gate.rs",
    ),
    // The wall the hold rule is measured against. Published rather than
    // written into the rule as a constant so a deployment that changes the
    // budget moves the alert with it.
    (
        "cogneva_build_gate_wait_budget_ms",
        "crates/cog-core/src/contract/build_gate.rs",
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
    (
        "cogneva_build_target_over_cap_total",
        "crates/cog-core/src/build_cache.rs",
    ),
    // Which gates a change was routed to, labelled by the tier it landed in.
    // The pipeline publishes it; the cells are seeded at zero on the first
    // change it tiers, so from that change on the rule below can read
    // `real_gate` against the total on every scrape. Before that first change
    // the series is absent, and `increase` over an absent series is empty: the
    // rule then stays quiet rather than reading a fleet that never reaches the
    // real gate.
    (
        "cogneva_change_tier_total",
        "crates/cog-reflection/src/change_pipeline.rs",
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
    // The engine's in-memory index of code changes, against the number of
    // change files the directory holds. Neither number is remarkable alone; the
    // pair is the reading, and it is what states that the retirement path
    // really drops what it retired. Published by any process whose engine
    // exists rather than by the owner alone, because the index belongs to
    // whichever process generates changes.
    (
        "cogneva_evolution_resident_results",
        "crates/cog-reflection/src/evolution_queue_readings.rs",
    ),
    (
        "cogneva_evolution_change_queue_files",
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
    // The recurrence the thresholds were consulted on, by which threshold asked.
    // Read by `evolution_learning_recurrence_never_climbs`, which needs both the
    // count of consultations and the buckets they fell in -- a distribution
    // pinned below 3 over a week of consultations is a corpus that is not
    // accumulating, and no downstream decision can tell that from a threshold
    // that is merely waiting.
    (
        "cogneva_evolution_learning_recurrence_at_threshold_total",
        "crates/cog-reflection/src/learning_recurrence_readings.rs",
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
    // The read loop's execution permits. A ready loop issues no further read
    // while it waits for one, so its silence has two causes -- a bus that
    // stopped answering and a process with no room -- and
    // `stream_consumer_stalled` can only see the first. The wait is compared
    // against the longest timeout of the tasks the permits are running, which
    // is why the bound is a published series rather than a constant: the wall
    // is the task's own contract, and a deployment that changes it moves the
    // rule with it. The in-flight and slot gauges are read by the same rule as
    // the guards that the permits are held and the bound is in force.
    (
        "cogneva_task_router_claim_slots",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_task_router_claim_in_flight",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_task_router_claim_wait_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_task_router_claim_hold_bound_seconds",
        "crates/cog-orchestrator/src/observable.rs",
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
    // The work a cycle may run without a beat: the stall rule adds it to the
    // period, because a cadence bounds only the wait between cycles and a loop
    // whose cycle runs a build would age past a multiple of its own period by
    // construction.
    (
        "cogneva_loop_work_budget_seconds",
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
    // asking any more".
    //
    // The acquisitions and losses counters below are read by
    // `background_loop_role_books_unbalanced`, which asserts the arithmetic
    // between them and the holder: the terms a process took, minus the ones it
    // lost, is the state it publishes. That rule states an identity rather than
    // a number, so nothing here has to say how many handovers are ordinary --
    // and its blind spot is the absent holder above: while no process can answer,
    // the holder side of the comparison is empty and the rule goes quiet with the
    // family, which is what the probe-failure counter beside it is read for.
    (
        "cogneva_loop_role_declared",
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_held",
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_acquisitions_total",
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_losses_total",
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
    // The second open verdict beside it, and the same kind of series for the
    // same reason: whether a tool-carrying request is routed to an upstream on
    // evidence or on the assumption that it supports native tool calls. The
    // routing guard only moves tool traffic away from an upstream a probe
    // proved does not support them, so an unasked upstream reads as a yes --
    // and the failure that follows lands as a 200 whose tool calls were written
    // into the text, which the refusal-shaped readings beside it cannot see.
    (
        "llm_tool_calls_verdict_measured",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The third admission-probe verdict, and the one whose fallback is easiest to
    // mistake for safe: a profile that does not clamp fails in the open, so an
    // unmeasured verdict there looks like it needs nothing. The clamping profile
    // is the other direction -- it overwrites a temperature the caller chose and
    // returns 200, and the clamp counter, the status code and the body are all
    // identical whether that overwrite was evidence or a guess.
    (
        "llm_temperature_verdict_measured",
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
    // Why an upstream left the serving side, which `llm_upstream_healthy` cannot
    // say: a 0 there is the same reading for a quota window that has to run out
    // and for a subscription that expired, and those two call for opposite
    // repairs. Only the upstream's own words are classified, and a refusal that
    // cannot be read is published as `unclassified` rather than folded into
    // either. All three kinds are republished for every suspect upstream on each
    // pass, so a kind that stops holding is written back to zero instead of
    // staying at 1.
    (
        "llm_upstream_refusal_kind",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // Request outcomes per upstream. Its failure half is the only cell that
    // cannot be produced by idleness: the success half is published lazily, so a
    // rule that reads absence alone could not tell "nobody called" from "every
    // call failed", while a rule that requires the failure side to have moved
    // can. It is no longer what a pool that serves nothing is read *through* --
    // the rule that was built on this family asked for a difference it cannot
    // show, so it moved to the two failure families below -- but the per-caller
    // counts are read here still.
    (
        "llm_calls_total",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The pool's transport-failure counter, one cell per configured upstream per
    // `source`, published at zero for every one of them at startup. That is what
    // makes it readable by a difference function at all: the family beside it is
    // created on demand, so a series born from a burst carries the whole burst in
    // its first sample and `changes()` reads zero for it. Measured 2026-10-14:
    // 348 failed calls inside one 14-second window, a first sample already
    // holding 348, and the rule built on that family computed empty for the
    // fifteen hours that followed. The `source` split is read here too: the
    // request path and the health prober share this counter, so a pool that is
    // latched down -- requests never leaving the gateway -- would otherwise read
    // as callers failing, and `llm_calls_all_failing` selects `source="caller"`
    // for exactly that reason.
    (
        "llm_upstream_failures_total",
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // 耐久面丢掉的写。进程内那一份读数与它无关，所以写失败时这个网关与一个一切
    // 正常的网关是同一种画面：同样的值、同样的序列、同样的端点——差别只在这些读数
    // 能不能活得比 Pod 久，而那正是耐久面存在的全部理由。它按理由分格，两格都是
    // 抓取面读数、随进程重启归零；没有耐久面的进程在这个名字下一个字节都不发布。
    (
        "cogneva_gateway_durable_write_failures_total",
        "crates/cog-gateway/src/observable.rs",
    ),
    (
        "cogneva_metric_held_without_producer",
        "crates/cog-gateway/src/lib.rs",
    ),
    (
        "cogneva_metric_held_unreadable",
        "crates/cog-gateway/src/lib.rs",
    ),
    // The host's memory, split by the slices the kernel already keeps it in. One
    // series per slice, so the two names published together are one reading; the
    // rule reads only the slice that says "something outside this cluster is
    // holding memory", and the file slice is the denominator a reader compares it
    // against rather than a value any rule thresholds.
    (
        "cogneva_host_slice_anon_bytes",
        "crates/cog-observability/src/host_ledger.rs",
    ),
    // The two series that make the reading above usable on more than one
    // machine, and the reason each is a series rather than an assumption. A
    // ledger pod that fails to start on one node produces no sample at all, and
    // a node reporting nothing is the same picture as a node reporting zero --
    // so one series states the node is running its ledger, and the other states
    // how much of the host tree that ledger could read. The second is not a
    // convenience: a cgroup v1 host has the mount point and none of the slices,
    // which reads as three zeroes unless the coverage is published separately.
    (
        "cogneva_host_ledger_node_info",
        "crates/cog-observability/src/host_ledger.rs",
    ),
    (
        "cogneva_host_ledger_slices_readable",
        "crates/cog-observability/src/host_ledger.rs",
    ),
    // The host producer's heartbeat: when the process that feeds the principle
    // account last completed a delivery, and the period it declared on that
    // delivery. Both are read together and neither is a constant here -- the
    // moment is stamped by the database at receipt, the period is measured from
    // the timer that drives the producer, and the rule multiplies one by the
    // other. A producer that stopped and one that is running over an unchanged
    // corpus leave the same account, so these two series are the only reading
    // that tells them apart.
    (
        "cogneva_principle_producer_last_push_timestamp_seconds",
        "crates/cog-observability/src/producer_heartbeat.rs",
    ),
    (
        "cogneva_principle_producer_push_period_seconds",
        "crates/cog-observability/src/producer_heartbeat.rs",
    ),
    (
        "cogneva_process_zombies",
        "crates/cog-observability/src/process_zombies.rs",
    ),
    (
        "cogneva_redis_info_interval_seconds",
        "deploy/helm/cogneva/templates/redis.yaml",
    ),
    (
        "cogneva_redis_info_updated_timestamp_seconds",
        "deploy/helm/cogneva/templates/redis.yaml",
    ),
    (
        "cogneva_redis_memory_used_bytes",
        "deploy/helm/cogneva/templates/redis.yaml",
    ),
    (
        "cogneva_redis_wal_scan_interval_seconds",
        "deploy/helm/cogneva/templates/redis.yaml",
    ),
    (
        "cogneva_redis_wal_scan_timestamp_seconds",
        "deploy/helm/cogneva/templates/redis.yaml",
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
    // The check's own timestamp, which is the only series in the family that
    // says the check ran. It is read by absence rather than by value: it holds
    // an epoch second, not a duration, so there is no threshold to compare it
    // against -- what it answers is whether the report exists at all.
    (
        "cogneva_runtime_asset_check_seconds",
        "crates/cog-reflection/src/runtime_assets.rs",
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
    // Which end a generation round's diff came from. Its own rule reads the
    // tree cell against the total, so the family has to be registered for that
    // rule to be allowed to read it -- and the ratio the pair states is only
    // visible across rollouts, which is what the shared store is for.
    (
        "collab_change_diff_source_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "self_evolution_change_rework_total",
        "crates/cog-reflection/src/change_rework.rs",
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
    // The observability stack's half of the same pair. Its manifests have one
    // delivery path -- the install script -- so once installed, the repository
    // and the cluster each go their own way, and the convergence loop that
    // compares and re-applies them published its verdict only as a persistent
    // alert. That alert says a stack is not converged; it cannot say which
    // file, how long, or that a re-apply is not landing. This is the differing
    // lines of each delivered file's own diff -- the per-file reading the loop
    // already computes -- published before it applies: a repair that ran first
    // would clear the reading it was meant to be measured by.
    (
        "cogneva_observability_drift_lines",
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
    // The two series that verdict is computed from: the log's row count and the
    // budget it is held to. They were published and unread along with the
    // verdict itself -- nothing compared the log against its own budget, which
    // is why a sweep that stopped running was silent. The budget series is
    // absent, not zero, for a deployment that declares no cap.
    (
        "metrics_samples_rows",
        "crates/cog-storage/src/metrics_sample_cap.rs",
    ),
    (
        "metrics_samples_budget_rows",
        "crates/cog-storage/src/metrics_sample_cap.rs",
    ),
    // The memory backlog past the re-drive window: raw sources whose summary is
    // still owed and that no diagnosis says are impossible. Its help names what
    // repays them (the reconcile pass, a bounded batch per round) and what a
    // value that stays put means; it had no reader.
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
    // The build store's keep set, and the cluster read it is built from. The
    // help names a live reading of zero as the one way the reclaim pass removes
    // a base image something is running, and the producer shows why zero is
    // reachable: the read returns Ok with an empty list when the workload query
    // answers with no revision of ours, and the pass goes on to plan a prune
    // against that empty list. Nothing read the count.
    (
        "cogneva_buildah_store_live_images",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // The retired-row release. Its help states the criterion itself -- a table
    // whose reading stays non-zero is one the release is not draining -- and
    // the window that makes "stays" readable is bounded rather than guessed:
    // the pass rides the metric sweep, and the sweep's period is derived from
    // the fill rate but capped by a declared ceiling, so an hour is a large
    // multiple of a bound the code states.
    (
        "metrics_retired_rows_removed",
        "crates/cog-storage/src/metrics_retirement.rs",
    ),
    // The CI gate's own reading. A red verdict holds a rollout and leaves no
    // trace that outlives the pod, and the per-reason counter is the only place
    // that says which platform went silent instead of answering. Neither had a
    // reader, so a promotion that stopped because CI said no, and one that went
    // ahead because the answer could not be read, both arrived as a stall with
    // no cause on any surface.
    (
        "cogneva_mainline_ci_verdict_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    (
        "cogneva_mainline_ci_no_verdict_reason_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 这一对顶掉的是 `cogneva_mainline_superseded_rollout_total` 那条欠账。原来想让它
    // 自己当「守卫还在」的证据（它每一轮都写，跳过时写 1、没跳过写 0），但计数器上补
    // 一个 0 不会让已渲染的值动，所以「这一轮问过」在那条序列上读不出来；能读出来的是
    // 「最新一次观测在什么时候」，那是存储为每条序列渲染的 `_observed_timestamp_seconds`
    // 伴生，而这个族当时在门禁视界之外（现在认得，见 `closed_set::a_series_this_build_publishes`）。
    // 这一对作为所选读数另有其力：它答的是「走到决定这一共有几轮」与「守卫真被问了几次」，
    // 两者之差就是判据本身；伴生答不出「几次」，只答最后那次在何时。
    (
        "cogneva_mainline_rollout_attempts_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    (
        "cogneva_mainline_supersession_checks_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 「在试、什么都没落地」的那一面。attempts 在「走到决定要不要滚」时写，交付被
    // 集群拒也照涨，所以一个每轮空转 20 小时的部署器在所有别的计数上与健康的同形；
    // completed 只在真的滚上去（四部署到目标 rev 且判定跑完）时写。2026-10-12→13
    // 实测：evolution 的 Role 少了一条交付要的授权，每次派发都被拒，20 小时零部署，
    // 没有任何一条序列说出来——这一对是那件事分出来的那一面。
    (
        "cogneva_mainline_rollout_completed_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 被拒派发按哪个动词、哪个资源记一条。上面那一对答的是「在试、什么都没落地」，
    // 答不出**为什么**——被拒的 apply 死在那次滚动 Job 的 Pod 里，Job 一结束痕迹
    // 就没了。这条是部署器自己派发前拿集群问的 SelfSubjectAccessReview，标签里
    // 带着被扣住的动词和资源，2026-10-12→13 那 20 小时缺的就是它（Role 少
    // patch middlewares.traefik.io）。
    (
        "cogneva_mainline_preflight_refused_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 轮询循环每一轮的结果。循环自己的存活读数（`loop_tick_age_seconds`）在每拍开头
    // 盖章，量的是「它醒了」，量不到这一轮干了什么；而这一轮里别的读数（版本契约、
    // CI 判词、那对越权计数）全写在会失败的那一步**之前**。2026-10-08 实测：发布那
    // 一步连着两个多小时拒绝清单包，每一轮都折在那里，唯一的痕迹是一条会随下一个
    // 滚动消失的 warn——同一段时间里心跳照打、判词照涨，「每轮都失败」与「没事可做」
    // 在所有别的读数上同形。这一对是那件事分出来的那一面。
    (
        "cogneva_mainline_poll_cycles_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 连续多少轮没拿到构建槽。被拒的每一轮在日志里和上一轮逐字相同（同一个请求者、
    // 同一句 "host is building"），而推进是唯一零等待的调用者——别的调用方会等下去、
    // 到预算才失败，它这一轮直接回、下一轮再来。所以饿住的那一段留下的是一条没有任何
    // 人处理的拒因，以及一个只有进得了演化 Pod 才读得到的 `advance_deferrals`。
    // 这条读数把那一段搬进指标面，每轮写、含 0：共享表只增不改，饿住结束后必须有东西
    // 把它写回去，否则规则点亮之后没有任何东西能灭它。
    (
        "cogneva_mainline_advance_deferrals",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 版本契约的另一半：判定跑过了。它上面那条（`cogneva_version_contract_violations`）是
    // 按条款读的常驻值，而判定停摆时那条读数停在零——与契约成立同一个零——所以这个计数
    // 是分开两者的那一面。它每轮、每条款各写一次，且停在共享存储里：判定停了，最后那个
    // 总量仍然被每个 Pod 渲染出来而不是消失，规则读的就是这个冻结。
    (
        "cogneva_version_contract_checks_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 知识检索的回声只到一张空表为止，而「没这个层」「后端拒绝」
    // 「后端答了但没有」在返回值上是同一个空表——修法要在这三者之间选，而选择需要
    // 的信息调用方拿不到。这个计数把每次检索按 `layer` 与 `outcome`（hit/empty/error/
    // absent）各记一格，是这条区分唯一存在的地方。写入点集中在 cog-core 的
    // `record_retrieval_cell`：后端每次检索写一格，取不到后端的进程开机把整张网格
    // 播种为 0、并按 absent 报一次，两处都走这一个函数。
    (
        "cogneva_knowledge_retrieval_total",
        "crates/cog-core/src/contract/knowledge.rs",
    ),
    // 配额自己的决定。它此前在指标面上完全不可见：`QuotaMetrics` 把五个 family
    // 注册进自己建的私有 registry、唯一调用点在测试里，而中间件发出的 429 只把
    // 理由回给那一个调用方 ⇒ 「配额在拦谁」在单个响应里拼不出来。这条读数走的不是
    // 那条路：`cog-quota` 的 Observable 在 init 时发布，网关在 init 时取快照，
    // `/metrics` 按 `RawMetric` 渲染 ⇒ 它随产出者消失，没有行留在 store 里
    // （所以不进 `metric_names::ALL` 那个闭集），登记点就是这条规则。
    (
        "quota_pre_checks_total",
        "crates/cog-quota/src/observable.rs",
    ),
    // 闸门**此刻**由哪一路掌权，与上面那条同形：五格由产出进程按 `RawMetric` 渲染，
    // 随产出者消失，store 里没有行（所以同样不进 `metric_names::ALL` 那个闭集），
    // 登记点就是 `memory_raw_backlog_aged_out` 的抑制子句——它读
    // `pull_gate_local|pull_gate_shared|pull_gate_both` 这三格，好让「闸门被任何一路
    // 按住」不再被那条规则读成「扫不动／漏」。名字取自
    // `crates/cog-memory/src/observable.rs` 的 `PULL_GATE_HOLDER_METRIC`，五个标签
    // 值与换手计数共用 `PULL_GATE_OPERATIONS`，是同一套拼写而不是第二份。
    (
        "cogneva_memory_pull_gate_holder",
        "crates/cog-memory/src/observable.rs",
    ),
];

/// Series the rules read that this workspace does not publish, with the owner.
/// Listing them keeps the check honest: an unlisted foreign series fails the
/// test rather than being silently ignored.
const FOREIGN: &[(&str, &str)] = &[
    (
        "container_memory_rss",
        "cadvisor（kubelet 内置）。判内存余量读的是**不可回收**的那部分：工作集把可回收的页缓存算进分子，在这棵树上足以把每一个大容器都推到上限（app／沙箱／进化三族的 usage 峰值都在 0.9998–1.0000 of limit，而 rss 是 0.78／0.59／0.24–0.39），所以拿工作集当分子时「顶到上限」不携带信息。工作集名字本身仍在别的面板上被读，但**没有任何告警规则读它**，按 the_tables_hold_no_series_that_no_rule_reads 不能留在本表",
    ),
    ("kube_deployment_spec_replicas", "kube-state-metrics"),
    (
        "kube_deployment_status_replicas_available",
        "kube-state-metrics（Deployment 层面的可用副本数；读数取自各 Pod 的 ready，而 Pod 卡在 Terminating 时 API 仍把它记为 ready，所以这份计数在那种故障下不动）",
    ),
    (
        "kube_cronjob_created",
        "kube-state-metrics（CronJob 对象自己的创建时刻，**只要对象在就有**，与它排没排过班、成没成过无关）。它是「装了却一直什么都没跑」这一格唯一可用的界：判这条读的是对象已存在多久，而「对象在、账上四个计数全 0」这件事没有第二个时刻可扣——排班周期在 PromQL 里导不出来（schedule 是 cron 串），而能给出周期的那个字段（下一次排班时刻）是从「上一次排班时刻」算出来的，正是这一格按定义没有的字段。它同时兼着名册：对象不在、或 kube-state-metrics 不在时这条序列缺席，取 max 得空、积随之空，于是「源停了」留给看这个源的规则，不会被读成「备份一次都没记过」",
    ),
    (
        "kube_cronjob_status_last_successful_time",
        "kube-state-metrics（CronJob 自己答的完成时刻，只有当某次班次真的成功过才有这条序列）。它是备份账上「最后一次成功」唯一的对照物：账那一侧记的同样是「某个成功班次完成的时刻」，两者相减是同一台机器上两件本该同步的事；比一个写死进规则的周期，改班次那天就过期。而账空的时候根本不存在「最后一次」可比，所以读它的规则必须先有一支 absent 支来承载那个状态——两个面同时缺席正是它没法单独回答的情形",
    ),
    (
        "kube_cronjob_status_last_schedule_time",
        "kube-state-metrics（CronJob 最近一次被排上班的时刻，**只要排过班就在**，与那班成没成无关）。它回答的正是上面那条点名答不了的情形：完成时刻与账同时缺席时，读它对「排了一整天的班、一次也没成过」下判词——而这也是「源整个没了」与「备份一次没成」分开的地方，kube-state-metrics 不在时它同样缺席，规则于是静默，把这一格留给看这个源的规则。这两条序列是一对，一条只在成功过之后才有、另一条只要有班次就有，所以谁也不能替另一条说话",
    ),
    (
        "kube_cronjob_spec_suspend",
        "kube-state-metrics（CronJob 对象自己的 `suspend` 字段，**只要对象在就有**，与它排没排过班无关）。暂停一个 CronJob 是唯一一种「对象在、字段正常、却再也不会有班次」的停法：`created` 还在、`last_schedule_time` 停在上一次、账上一切照旧，而 kubelet 不会报错、Pod 不会重启。判「这个源被暂停了」只能读这个字段——别的序列在暂停期间全都不动，它们的静止与「没到点」逐字相同",
    ),
    (
        "kube_node_info",
        "kube-state-metrics（每个节点一条常量序列；它是「集群当前认为自己有哪些节点」的那份清单，所以节点被移除时它也消失——按它来判断账本缺席，才不会把「节点没了」和「节点在但不报」混成一件事）",
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
    (
        "kube_pod_start_time",
        "kube-state-metrics (the pod object's creation instant, in epoch seconds; read as `time() - kube_pod_start_time` it is the pod's age. `kube_pod_status_phase` reads Running from the instant the container starts, while a scrape target appears only after endpoint discovery has run, so the two are a discovery cycle apart -- an age bound is what tells a pod that has only just started from one that has been up with no scrape face at all)",
    ),
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

/// The deploy config the rules live in, parsed. Read through one function so a
/// gate that needs a value other than the rules reads the same document the
/// rules came from.
fn chart_json() -> serde_json::Value {
    serde_json::from_str(&read(CHART_CONFIG)).expect("chart config is not valid JSON")
}

/// Every rule in the chart, name to promql, in file order.
fn chart_rules() -> Vec<(String, String)> {
    let value = chart_json();
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

/// The alert-delivery counter is written by the same notifier every rule's
/// notification goes through. A rule that reads `delivered`, `failed` or
/// `skipped` can therefore be kept firing by its own delivery attempt: the
/// attempt fails, the counter moves, the rule fires again. Only
/// `outcome="no_channel"` is written before a channel is chosen, so it is the
/// one cell the rule's own notification cannot move.
const ALERT_DELIVERY_COUNTER: &str = "cogneva_alert_notifications_total";
const THE_ONLY_OUTCOME_A_RULE_MAY_READ: &str = r#"outcome="no_channel""#;

/// Every complaint about one expression watching the delivery counter. Reads
/// without a selector, selectors naming another outcome and selectors using a
/// negative or regex matcher are all reported: each of them counts the cells
/// the rule's own notification writes.
fn delivery_reader_complaints(promql: &str) -> Vec<String> {
    let mut complaints = Vec::new();
    for (pos, _) in promql.match_indices(ALERT_DELIVERY_COUNTER) {
        let after = &promql[pos + ALERT_DELIVERY_COUNTER.len()..];
        let Some(open) = after.find('{') else {
            complaints.push(format!(
                "reads `{ALERT_DELIVERY_COUNTER}` with no selector, so every \
                 outcome is counted, including the one its own notification writes"
            ));
            continue;
        };
        if !after[..open].trim().is_empty() {
            complaints.push(format!(
                "reads `{ALERT_DELIVERY_COUNTER}` without a selector on the \
                 series itself, so every outcome is counted"
            ));
            continue;
        }
        let Some(close) = after[open..].find('}') else {
            complaints.push(format!(
                "selector for `{ALERT_DELIVERY_COUNTER}` is unterminated"
            ));
            continue;
        };
        let selector = &after[open + 1..open + close];
        let outcomes: Vec<&str> = selector
            .split(',')
            .map(str::trim)
            .filter(|matcher| matcher.starts_with("outcome"))
            .collect();
        if outcomes.as_slice() != [THE_ONLY_OUTCOME_A_RULE_MAY_READ] {
            complaints.push(format!(
                "reads outcomes {outcomes:?}; only `{THE_ONLY_OUTCOME_A_RULE_MAY_READ}` \
                 cannot be moved by the rule's own notification"
            ));
        }
    }
    complaints
}

#[test]
fn a_rule_that_watches_alert_delivery_reads_only_the_cell_its_own_delivery_cannot_move() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for complaint in delivery_reader_complaints(&promql) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "这些规则在监视告警投递，却读了自己通知会移动的格子；规则自己的通知走同一个\
         通知器，一次投递失败会把它再钉回 firing:\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_delivery_reader_check_separates_the_cell_a_notification_cannot_move() {
    // The selector the check demands.
    assert!(delivery_reader_complaints(
        r#"sum(increase(cogneva_alert_notifications_total{outcome="no_channel"}[30m]))"#
    )
    .is_empty());

    // Each of these reads a cell the rule's own delivery writes.
    for bad in [
        "sum(increase(cogneva_alert_notifications_total[30m]))",
        r#"sum(increase(cogneva_alert_notifications_total{outcome="failed"}[30m]))"#,
        r#"sum(increase(cogneva_alert_notifications_total{outcome=~".*"}[30m]))"#,
        r#"sum(cogneva_alert_notifications_total{outcome!="no_channel"})"#,
        r#"sum(cogneva_alert_notifications_total{channel="webhook"})"#,
    ] {
        assert!(
            !delivery_reader_complaints(bad).is_empty(),
            "check let through a delivery reading it must reject: {bad}"
        );
    }
}

/// A rule may read a name this build records, a name it *derives* from one, or
/// a foreign series listed with its owner — nothing else, because a rule on any
/// other name fires never. The derived half is decided by
/// [`closed_set::a_series_this_build_publishes`], which owns it for both
/// readers: the store renders `_observed_timestamp_seconds` beside every series
/// it serves and a histogram's `_bucket`/`_sum`/`_count` come from the name its
/// producer records, so a rule naming either reads something that exists.
/// Spelling the judgement here instead is what made the two readers disagree.
#[test]
fn every_series_an_alert_rule_reads_is_one_something_produces() {
    let produced: BTreeSet<&str> = PRODUCED.iter().map(|(n, _)| *n).collect();
    let foreign: BTreeSet<&str> = FOREIGN.iter().map(|(n, _)| *n).collect();

    let mut unknown: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (rule, promql) in chart_rules() {
        for name in metric_names_in(&promql) {
            if produced.contains(name.as_str())
                || foreign.contains(name.as_str())
                || closed_set::a_series_this_build_publishes(&name)
            {
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

/// A difference read shows only what the cell looked like *before* the event, so
/// which of these families a rule may read that way is a decision with a
/// published baseline behind it -- declared next to the producers, in
/// `cog_gateway::security_gateway::RANGE_READ_BASELINES`, and reconciled here.
///
/// This is the reconciliation the rule table never had. Three directions, and
/// each of them is a way the pair drifts: a rule reading an undeclared family
/// (nobody decided how its first sample is produced), a declaration no rule
/// reads (a claim kept alive past its reader), and an exemption that does not
/// name every rule paying its cost (the cost is then paid by someone who never
/// wrote it down). Measured 2026-10-14 on the way to writing it: the old
/// `llm_calls_all_failing` read the exempt `llm_calls_total`, and 348 failures
/// inside one 14-second window created the series with the whole burst already
/// in its first sample, so `changes()` read zero and the rule computed to
/// nothing through the fifteen hours that followed a pool going fully down.
///
/// What it cannot judge is whether a baseline declared `SeededAtZero` really is
/// published before the first increment: the table is a claim about a publisher
/// in another crate, and the reader for that half is the publisher's own test,
/// which walks the same table.
#[test]
fn a_difference_read_of_a_metering_family_is_one_the_producer_declared() {
    let declared: BTreeSet<&str> = RANGE_READ_BASELINES.iter().map(|(n, _)| *n).collect();
    let mut undeclared: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut readers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (rule, promql) in chart_rules() {
        for family in difference_reads(&promql) {
            if declared.contains(family.as_str()) {
                readers.entry(family).or_default().insert(rule.clone());
            } else if family.starts_with("llm_") {
                undeclared.entry(family).or_default().push(rule.clone());
            }
        }
    }
    assert!(
        undeclared.is_empty(),
        "规则用差分函数（changes/increase/rate/…）读了 `llm_` 前缀的族，而产出侧没有为它\
         声明过基线（`RANGE_READ_BASELINES`）：按需创建的族在这种读法下首个事件读不出来\n{}",
        undeclared
            .iter()
            .map(|(name, rules)| format!("  {name} <- {}", rules.join(", ")))
            .collect::<Vec<_>>()
            .join("\n")
    );

    let mut missing_cost: Vec<String> = Vec::new();
    for (family, baseline) in RANGE_READ_BASELINES {
        let actual: BTreeSet<String> = readers.get(*family).cloned().unwrap_or_default();
        match baseline {
            RangeReadBaseline::SeededAtZero => {
                if actual.is_empty() {
                    missing_cost.push(format!(
                        "  {family} 声明为 SeededAtZero，但没有任何规则用差分函数读它：要么写一条规则，要么删掉这一条"
                    ));
                }
            }
            RangeReadBaseline::Exempt { readers: named, .. } => {
                let named: BTreeSet<String> = named.iter().map(|s| s.to_string()).collect();
                if named != actual {
                    missing_cost.push(format!(
                        "  {family} 的豁免名单与真正读它的规则对不上\n      名单里多出：{:?}\n      实际在读：{:?}",
                        named.difference(&actual).collect::<Vec<_>>(),
                        actual.difference(&named).collect::<Vec<_>>()
                    ));
                }
            }
        }
    }
    assert!(
        missing_cost.is_empty(),
        "基线表与规则表对不上（豁免的代价要由点得出名字的规则承担，声明也不能没有读者）:\n{}",
        missing_cost.join("\n")
    );
}

/// A rule whose expression does not parse never fires, and the check above
/// passes it for the same reason a blank panel passes: every series it names is
/// really produced.
///
/// This is not a parser, and the name says so rather than claiming more: it
/// recognises a finite set of shapes, each of them verified against a live
/// Prometheus to be rejected outright. What it knows is what a hand edit makes
/// -- an operator dropped between two operands that a matching clause then sits
/// in front of, and the same edit with the bracket on the wrong side of the
/// guard, a range selector handed to a set operator, which the clause scan
/// cannot see.
///
/// A shape outside that set gets past this gate. The backstop for whatever
/// ships anyway is the watcher's own eval-failure self-alert, which fires on a
/// query that keeps failing -- slower, and heard only if someone reads the
/// alerts. A rule and a panel fail the same way here, which is why both sides
/// of this contract ask the same question about their text.
#[test]
fn no_rule_expression_has_a_known_unparseable_shape() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for complaint in shape_complaints(&promql) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "告警规则的表达式含一种已知的、Prometheus 一定拒绝的形状，这条规则永远不会触发:\n{}",
        complaints.join("\n")
    );
}

/// Whether a device counter is named in seconds without measuring seconds.
///
/// On a block-multiqueue device the kernel writes the `*_ticks` fields as the
/// sum of each request's in-flight time, so they climb by however many requests
/// were overlapping rather than by the time the device spent busy. Measured on
/// this deployment's NVMe over one 20 second window, `write_ticks` reached
/// 11,322% of the wall clock and `time_in_queue` -- which is what the usual
/// `%util` query is built from -- 11,477%. A rate over one of these is not a
/// duration, and `sectors * 512 / write_ticks` is throughput divided by average
/// concurrency while reading exactly like a throughput.
///
/// `io_ticks` is the exception: it counts time at least one request was in
/// flight, which the clock does bound (84% of wall clock in the same window).
/// It is the only spelling of device time a rule may read.
fn sums_in_flight_time(metric: &str) -> bool {
    metric.starts_with("node_disk_")
        && metric.ends_with("_seconds_total")
        && metric != "node_disk_io_time_seconds_total"
}

/// No rule this repo ships reads a device counter as elapsed time.
///
/// A device reading that reads like a duration and is not one is worse than a
/// missing reading: the alert fires on a number nobody can interpret, and the
/// threshold picked for it was picked by reading the same wrong quantity.
///
/// "This repo ships" is the whole of the reach. The check reads the rules the
/// chart renders, and a rule carried by a third party in its own
/// `PrometheusRule` object is not in its view. Nothing ships such a rule today,
/// so there is no gap to close; but the reach is the chart's rules, not every
/// rule the cluster runs, and the name says so.
#[test]
fn no_rule_this_repo_ships_reads_a_device_counter_as_elapsed_time() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for metric in metric_names_in(&promql) {
            if sums_in_flight_time(&metric) {
                complaints.push(format!("{rule} reads {metric}\n    {promql}"));
            }
        }
    }

    assert!(
        complaints.is_empty(),
        "these device counters sum per-request in-flight time rather than time on \
         the clock, so a rate or ratio over one is not a duration:\n{}",
        complaints.join("\n")
    );
}

/// The gate above is aimed at the counters it means to catch.
///
/// Without this, a predicate that had drifted off the name it was written for
/// would report nothing and look like a clean fleet.
#[test]
fn the_device_counter_gate_reports_the_counter_it_is_aimed_at() {
    for caught in [
        "node_disk_read_time_seconds_total",
        "node_disk_write_time_seconds_total",
        "node_disk_discard_time_seconds_total",
        "node_disk_io_time_weighted_seconds_total",
    ] {
        assert!(sums_in_flight_time(caught), "{caught} should be reported");
    }
    for allowed in [
        "node_disk_io_time_seconds_total",
        "node_disk_written_bytes_total",
        "node_disk_read_bytes_total",
        "node_disk_writes_completed_total",
        "node_disk_info",
    ] {
        assert!(!sums_in_flight_time(allowed), "{allowed} should be allowed");
    }
}

/// A guard that reads the very series it guards never lets the rule through.
///
/// `L unless on () R` drops every `L` that has a matching `R`, and an empty `on ()`
/// list matches everything. When both halves are "this counter moved off zero" over
/// the same series, the guard is the fault, so the rule is silent in exactly the
/// situation it exists to report. `outcome!="delivered"` on a closed outcome set is
/// how this happens by accident.
#[test]
fn no_guard_suppresses_the_fault_it_watches() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for complaint in promql::self_suppressing_guard_complaints(&promql) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "告警规则的守卫那一半读的是它自己守的那个计数器的同一格，规则在它写明的故障里永远不会触发:\n{}",
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

/// The placeholder names a summary interpolates as `{name}`. A span whose inner
/// text is not a bare identifier is not a placeholder and is left alone.
fn summary_placeholders(summary: &str) -> Vec<String> {
    let bytes = summary.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            if let Some(rel) = summary[i + 1..].find('}') {
                let close = i + 1 + rel;
                let inner = summary[i + 1..close].trim();
                if !inner.is_empty() && inner.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    out.push(inner.to_string());
                }
                i = close + 1;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// A summary that names a label reads a rule whose expression keeps that label.
///
/// `{name}` in a summary is filled from the label's value on the firing series,
/// so a placeholder the expression does not keep renders empty on the alert it
/// was written to carry. The two halves drift the way the series names do:
/// editing an aggregation's `by (...)` says nothing to the summary that reads
/// it, and the alert keeps firing while the label it names goes blank. `{value}`
/// is reserved for the sample value and is always available.
///
/// Only rules with an explicit `by (...)` are judged. `without (...)` names the
/// labels an aggregation *drops*, so the set it keeps is everything its inputs
/// carry and is not readable from the text; a bare aggregation drops them all.
/// Those rules are unjudged, counted, and reported — counting them as passing
/// would be an all-green over a denominator that shrank.
#[test]
fn a_summary_names_only_labels_its_expression_keeps() {
    let rules = chart_json()["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("observability.infra_watch.rules is not an array")
        .clone();

    let mut complaints: Vec<String> = Vec::new();
    let mut judged = 0usize;
    let mut unjudged = 0usize;
    for rule in &rules {
        let name = rule["name"].as_str().unwrap_or_default();
        let expression = rule["promql"].as_str().unwrap_or_default();
        let summary = rule["summary"].as_str().unwrap_or_default();

        let clauses = kept_labels_by_clause(expression);
        if clauses.is_empty() {
            unjudged += 1;
            continue;
        }
        judged += 1;

        let kept: BTreeSet<&str> = clauses.iter().flatten().map(String::as_str).collect();
        for placeholder in summary_placeholders(summary) {
            if placeholder == "value" || kept.contains(placeholder.as_str()) {
                continue;
            }
            complaints.push(format!(
                "{name}: summary 点了 {{{placeholder}}}，而表达式保留的是 {kept:?}\n    {expression}"
            ));
        }
    }

    // 分母：只带 by(...) 的规则有保留集可静态读；without(...) 与裸聚合的规则记为未判。
    // 分母塌了（判过的规则太少）这条判据就形同虚设，所以先断言它还在。
    assert!(
        judged >= 55,
        "带 by(...) 的告警规则只剩 {judged} 条（未判 {unjudged} 条），读 summary 标签的判据已形同虚设：这条判据的分母是它"
    );
    assert!(
        complaints.is_empty(),
        "告警规则的 summary 点了一个它表达式不保留的标签，报警里那一格会是空的:\n{}",
        complaints.join("\n")
    );
    // 未判的规则数是分母的另一半：静态算不出保留集，只报数，不当通过。
    println!("summary 标签判据：判过 {judged} 条，未判 {unjudged} 条（无 by(...) 子句）");
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

/// Complaints about a rule whose window over a durable-gated series is no
/// longer than that series' writer heartbeat.
///
/// The two failures this separates are not symmetric. A window shorter than the
/// heartbeat makes the rule fire on a working gateway; a window at exactly the
/// heartbeat is the same thing, because the gap the gate may leave *is* the
/// heartbeat. Only strictly longer is safe.
fn durable_window_complaints(
    rule: &str,
    promql: &str,
    gated: &BTreeSet<String>,
    heartbeat: u64,
) -> Vec<String> {
    windows_over(promql, gated)
        .into_iter()
        .filter(|(_, window)| *window <= heartbeat)
        .map(|(series, window)| {
            format!(
                "{rule}: 窗口 {window}s 不长于写者心跳 {heartbeat}s——值不变时写者只在心跳时\
                 重写，这个窗口里的空档是一个健康网关的正常状态\n    {series}  {promql}"
            )
        })
        .collect()
}

/// A rule that windows a series the gateway writes through its durable gate has
/// to look back further than that gate's own heartbeat.
///
/// The gate writes a value when it changes and, for one that never changes,
/// once per `DURABLE_GAUGE_HEARTBEAT`. So a rule reading "no sample in the
/// window was above zero" over a window no longer than that gap is satisfied by
/// a healthy gateway sitting between two of its own writes: the window is
/// empty, the rule calls it a delivery failure, and nothing about the gateway
/// is wrong. Only a window strictly longer than the heartbeat can tell the two
/// apart.
///
/// Neither number is written here. Both are read from the gate that owns them,
/// because a window written into a rule drifts from the constant it was chosen
/// against the moment either side moves — and the drift is silent, in the
/// direction that makes the rule fire on a working system, which is how the
/// pool-signal rule shipped with a 30m window against a 1800s heartbeat and
/// fired on a healthy gateway. The direction is easy to write backwards, so it
/// is stated once, in the code that compares them.
#[test]
fn a_rule_windowing_a_durable_gauge_waits_longer_than_the_writers_heartbeat() {
    let heartbeat = cog_gateway::security_gateway::DURABLE_GAUGE_HEARTBEAT.as_secs();
    let gated: BTreeSet<String> = cog_gateway::security_gateway::DURABLE_POOL_GAUGES
        .iter()
        .map(|name| name.as_str().to_string())
        .collect();
    assert!(
        gated.len() >= 8,
        "耐久门控族只剩 {} 条序列，读它的窗口判据已经形同虚设：这条判据的分母是它",
        gated.len()
    );

    let mut complaints: Vec<String> = Vec::new();
    let mut windowed = 0usize;
    for (rule, promql) in chart_rules() {
        windowed += windows_over(&promql, &gated).len();
        complaints.extend(durable_window_complaints(&rule, &promql, &gated, heartbeat));
    }

    // 分母：一条规则都没窗口这些序列时，上面那句"没有投诉"是空的，不是绿的。
    // 池信号那条读数正是这样一条规则，它没了或它不再窗口就说明这条判据没有题目。
    assert!(
        windowed > 0,
        "没有任何告警规则对耐久门控族取窗口，这条判据没有分母（池信号送达与否的读数呢？）"
    );
    assert!(
        complaints.is_empty(),
        "告警规则对耐久门控序列取的窗口必须严格长于写者心跳，否则窗口里的空档是健康的:\n{}",
        complaints.join("\n")
    );
}

/// The gate above separates the two numbers it compares, and does so against a
/// fabricated rule as well as the shipped ones — a gate that only ever runs
/// over rules that pass has not been shown to reject anything.
#[test]
fn a_window_no_longer_than_the_heartbeat_is_reported_and_a_longer_one_is_not() {
    let gated: BTreeSet<String> = ["llm_pool_available"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let heartbeat = 1_800u64;

    // 短于心跳：健康的网关在两次写之间就能造出这个空窗口。
    assert_eq!(
        durable_window_complaints(
            "r",
            "max_over_time(llm_pool_available[30m]) < 1",
            &gated,
            heartbeat
        )
        .len(),
        1
    );
    // 等于心跳也不行：写者可以一直不写，直到心跳边界。
    assert_eq!(
        durable_window_complaints(
            "r",
            "max_over_time(llm_pool_available[1800s]) < 1",
            &gated,
            heartbeat
        )
        .len(),
        1
    );
    // 严格长于心跳：窗口里必有一条写。
    assert!(durable_window_complaints(
        "r",
        "max_over_time(llm_pool_available[1h]) < 1",
        &gated,
        heartbeat
    )
    .is_empty());
    // 不在耐久族里的序列不受这条判据管，多长的窗口都放行。
    assert!(durable_window_complaints(
        "r",
        "max_over_time(llm_calls_total[5m]) > 0",
        &gated,
        heartbeat
    )
    .is_empty());
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

/// One loop's outcome series paired with the cadence that loop actually runs on.
struct LoopCadence {
    /// The rule whose window is chosen against this cadence.
    rule: &'static str,
    /// The series the loop's outcome lands in.
    series: &'static str,
    /// The dotted path the deploy config sets this loop's interval at. Empty when
    /// the loop's period is not on the deploy surface, which leaves the owning
    /// crate's default as the effective value.
    config_path: &'static [&'static str],
    /// The owning crate's default for that interval.
    default_secs: u64,
    /// A floor the loop clamps its configured interval up to. A window or a
    /// freshness bound chosen against the configured number alone is short of the
    /// cadence the loop really keeps whenever this floor binds.
    floor_secs: u64,
}

impl LoopCadence {
    fn period(&self, chart: &serde_json::Value) -> u64 {
        effective_secs(chart, self.config_path, self.default_secs).max(self.floor_secs)
    }
}

/// The value a dotted config path resolves to: the deploy config's value when the
/// deployment sets one, else `default`.
///
/// Reading only the default guards a number the deployment is free to differ
/// from. Reading only the deploy value guards nothing when the deployment stays
/// silent -- which it does for every loop whose period has never needed tuning.
fn effective_secs(chart: &serde_json::Value, path: &[&str], default: u64) -> u64 {
    let mut node = chart;
    for key in path {
        match node.get(*key) {
            Some(next) => node = next,
            None => return default,
        }
    }
    // A path of no segments lands here: the whole document is not a number, so a
    // period that is not on the deploy surface resolves to the default.
    node.as_u64().unwrap_or(default)
}

/// The loops whose own outcome counter an alert rule reads, with the cadence each
/// one runs on.
///
/// Every period is read from the module that owns it rather than spelled here, so
/// the table cannot drift from the loop it describes: the deploy config's value
/// where one is set, else the owning crate's default.
fn loop_cadences() -> Vec<LoopCadence> {
    let system = || cog_core::config::SystemConfig::default();
    let supervisor = || cog_core::config::SupervisorConfig::default();
    vec![
        LoopCadence {
            rule: "baseline_port_tick_failing_every_round",
            series: "baseline_port_tick_failed",
            config_path: &["self_evolution", "baseline_port", "poll_interval_secs"],
            default_secs: cog_reflection::BaselinePortConfig::default().poll_interval_secs,
            floor_secs: cog_reflection::baseline_port::MIN_POLL_INTERVAL_SECS,
        },
        LoopCadence {
            rule: "memory_schema_repair_passes_all_failed",
            series: "cogneva_memory_schema_repair_total",
            config_path: &["self_evolution", "schema_repair_interval_secs"],
            default_secs: cog_core::config::SelfEvolutionConfig::default()
                .schema_repair_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "policy_evolution_rounds_all_failed",
            series: "cogneva_policy_evolution_rounds_total",
            config_path: &["self_evolution", "artifact_evolution", "interval_secs"],
            default_secs: cog_reflection::PolicyEvolutionConfig::default().interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "memory_decay_passes_all_failed",
            series: "cogneva_memory_decay_total",
            config_path: &["memory", "maintenance", "decay_interval_secs"],
            default_secs: cog_memory::config::MaintenanceConfig::default().decay_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "partition_maintenance_round_came_up_short",
            series: "cogneva_partition_maintenance_failures_total",
            config_path: &["system", "partition_maintenance_interval_secs"],
            default_secs: system().partition_maintenance_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "memory_bus_claim_rounds_all_failed",
            series: "memory_operations_total",
            config_path: &["memory", "ingest", "bus_claim_interval_secs"],
            default_secs: cog_memory::config::IngestConfig::default().bus_claim_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "skill_hot_reload_passes_all_failed",
            series: "cogneva_skill_hot_reload_total",
            config_path: &["system", "skill_hot_reload_interval_secs"],
            default_secs: system().skill_hot_reload_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "skill_reload_failing_every_scan",
            series: "cogneva_skill_hot_reload_total",
            config_path: &["system", "skill_hot_reload_interval_secs"],
            default_secs: system().skill_hot_reload_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "analytics_flush_batches_all_failed",
            series: "cogneva_analytics_flush_total",
            config_path: &["observability", "clickhouse", "flush_interval_sec"],
            default_secs: cog_observability::config::ClickHouseConfig::default().flush_interval_sec,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "loki_flush_batches_all_failed",
            series: "cogneva_loki_flush_total",
            config_path: &["observability", "loki", "flush_interval_sec"],
            default_secs: cog_observability::config::LokiConfig::default().flush_interval_sec,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "supervisor_health_pass_failing",
            series: "cogneva_supervisor_pass_total",
            config_path: &["supervisor", "health_interval_secs"],
            default_secs: supervisor().health_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "supervisor_quota_pass_failing",
            series: "cogneva_supervisor_pass_total",
            config_path: &["supervisor", "quota_interval_secs"],
            default_secs: supervisor().quota_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "supervisor_rebalance_pass_failing",
            series: "cogneva_supervisor_pass_total",
            config_path: &["supervisor", "rebalance_interval_secs"],
            default_secs: supervisor().rebalance_interval_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "supervisor_event_pass_failing",
            series: "cogneva_supervisor_pass_total",
            config_path: &["supervisor", "event_window_secs"],
            default_secs: supervisor().event_window_secs,
            floor_secs: 1,
        },
        LoopCadence {
            rule: "supervisor_control_plane_pass_failing",
            series: "cogneva_supervisor_pass_total",
            config_path: &["supervisor", "control_plane_interval_secs"],
            default_secs: supervisor().control_plane_interval_secs,
            floor_secs: 1,
        },
    ]
}

/// Complaints about a rule whose window over a loop's outcome series, or whose
/// freshness bound on that series' companion, is short of one round of the loop.
///
/// The two are one round in opposite directions. A window shorter than a round
/// can contain no increment at all, because the counter only moves once per
/// round. A freshness bound at or below a round expires inside every healthy
/// round, so the guard turns the rule off for the part of the round it is not
/// covering -- and the rule goes quiet without anything about the loop being
/// wrong. Both were live in the shipped `baseline_port` rule at once.
fn loop_cadence_complaints(
    row: &LoopCadence,
    promql: &str,
    chart: &serde_json::Value,
) -> Vec<String> {
    let period = row.period(chart);
    let one: BTreeSet<String> = std::iter::once(row.series.to_string()).collect();
    let mut out: Vec<String> = windows_over(promql, &one)
        .into_iter()
        .filter(|(_, window)| *window < period)
        .map(|(series, window)| {
            format!(
                "{}: 窗口 {window}s 短于 {series} 产出一轮 {period}s——计数器每轮才动一次，\
                 窗口里可以一条增量都没有，真出故障时这条规则仍是空的",
                row.rule
            )
        })
        .collect();
    if let Some(bound) = companion_age_bound(promql, row.series) {
        if bound <= period {
            out.push(format!(
                "{}: 新鲜度界 {bound}s 不长于 {} 产出一轮 {period}s——健康的一轮里，上一笔\
                 戳记会在下一次求值前就过期，守卫把这条规则关掉",
                row.rule, row.series
            ));
        }
    }
    out
}

/// A rule over a loop's own outcome has to look back at least one round of that
/// loop, and must not let its freshness guard expire inside a round.
///
/// Both numbers are the loop's cadence, and that cadence is owned by the code that
/// runs the loop: the deployed value where the deploy config sets one, else the
/// owning crate's default, never below the loop's own floor. Read here instead of
/// argued in a summary, because a summary's arithmetic drifts from the cadence
/// silently. The shipped `baseline_port` rule carried a six-minute window and a
/// ten-minute freshness bound against a loop whose configured interval is an
/// hour: one round in ten, and a guard that had already expired for five sixths of
/// every round, so the rule was blind to the failure it was written for while
/// reading as landed on every other surface.
#[test]
fn a_rule_over_a_loops_outcome_waits_at_least_one_round() {
    let chart = chart_json();
    let rows = loop_cadences();
    assert!(
        rows.len() >= 15,
        "循环结局规则表只剩 {} 条，这条判据的分母是它",
        rows.len()
    );

    let rules: BTreeMap<String, String> = chart_rules().into_iter().collect();
    let mut complaints: Vec<String> = Vec::new();
    let mut windowed = 0usize;
    for row in &rows {
        let promql = rules.get(row.rule).unwrap_or_else(|| {
            panic!(
                "规则 {} 不在配置里：这张表读的是循环的节拍，规则改名或删除时这一行就悄悄\
                 不再对着任何东西",
                row.rule
            )
        });
        let one: BTreeSet<String> = std::iter::once(row.series.to_string()).collect();
        let windows = windows_over(promql, &one);
        assert!(
            !windows.is_empty(),
            "{} 不再对 {} 取窗口，这一行已经不对着任何东西（序列改名了？）",
            row.rule,
            row.series
        );
        windowed += windows.len();
        complaints.extend(loop_cadence_complaints(row, promql, &chart));
    }

    // 分母：一条窗口都没读到，上面那句「没有投诉」是空的，不是绿的。
    assert!(
        windowed >= rows.len(),
        "读到的窗口数 {windowed} 少于表里的行数 {}，说明有行没有对着窗口",
        rows.len()
    );
    assert!(
        complaints.is_empty(),
        "告警规则对循环自己的结局读数取的窗口/新鲜度界必须容得下产出它的一轮:\n{}",
        complaints.join("\n")
    );
}

/// The gate above separates the two numbers it compares, and does so against a
/// fabricated rule as well as the shipped ones -- a gate that only ever runs over
/// rules that pass has not been shown to reject anything.
#[test]
fn a_window_shorter_than_the_loops_round_is_reported_and_a_longer_one_is_not() {
    let chart = serde_json::json!({});
    let row = LoopCadence {
        rule: "r",
        series: "a_loops_outcome_total",
        config_path: &[],
        default_secs: 3600,
        floor_secs: 1,
    };

    // 窗口短于一轮：计数器在这个窗口里可以一动不动。
    assert_eq!(
        loop_cadence_complaints(&row, "increase(a_loops_outcome_total[6m]) > 0", &chart).len(),
        1
    );
    // 窗口刚好一轮：窗口里必然落着一笔，放行。
    assert!(
        loop_cadence_complaints(&row, "increase(a_loops_outcome_total[1h]) > 0", &chart).is_empty()
    );
    // 新鲜度界不长于一轮：健康的一轮里戳记会先过期。
    assert_eq!(
        loop_cadence_complaints(
            &row,
            "(min_over_time(a_loops_outcome_total[2h]) == 1) and \
             ((time() - a_loops_outcome_total_observed_timestamp_seconds) < 600)",
            &chart
        )
        .len(),
        1
    );
    // 两条都短：各报一次。
    assert_eq!(
        loop_cadence_complaints(
            &row,
            "(min_over_time(a_loops_outcome_total[5m]) == 1) and \
             ((time() - a_loops_outcome_total_observed_timestamp_seconds) < 600)",
            &chart
        )
        .len(),
        2
    );
    // 地板抬高了实际节拍：按配置字面量选出来的窗口会被判短。
    let floored = LoopCadence {
        rule: "r",
        series: "a_loops_outcome_total",
        config_path: &[],
        default_secs: 30,
        floor_secs: 300,
    };
    assert_eq!(
        loop_cadence_complaints(&floored, "increase(a_loops_outcome_total[1m]) > 0", &chart).len(),
        1
    );
}

/// A rule that reports on a gauge the metric store serves has to say that the
/// writer behind it is still running.
///
/// The store hands back each series' newest row forever, so a gauge whose
/// writer stopped goes on being scraped at its last value. A rule reading that
/// value as an observation -- over a window, or against a constant with no
/// window at all -- reads a frozen sample as a fact about now, and a writer
/// that stopped on the firing side of its threshold leaves the rule fired for
/// good: no repair to the thing being measured can clear it, because the series
/// the rule reads has stopped being connected to it. The companion
/// `_observed_timestamp_seconds` is the one series that moves when the writer
/// does and its value is the instant the row was stored, so the reading is an
/// age bound on it: `(time() - companion) < <bound>`.
///
/// The bound is on the writer's own clock rather than on a change count, and the
/// difference is what the guard is: the companion is rendered per scrape, so a
/// `changes()` over it counts only what this serving pod has seen, and a pod
/// younger than the writer's cadence reads zero for a writer that is running
/// perfectly well. See [`no_shipped_rule_guards_a_store_gauge_with_a_change_count`].
///
/// The family comes from [`gated_store_gauges`], and four rules were hand-fixed
/// one at a time before this test existed -- the next rule to read one of these
/// gauges had no way to be told.
/// The gauges the metric store serves, minus the durable pool family.
///
/// The family is taken from the exposition's own description tables rather than
/// by searching the rule text for metric names. A name search over the rules
/// answers a different question and answers it wrong: it picks up series
/// exposed straight from a process's `/metrics`, which fall out of the scrape
/// with their producer and so never freeze, and it misses the family boundary
/// entirely.
///
/// The durable pool gauges are excluded mechanically rather than by a list of
/// names: their writer rewrites a value that has not moved once per
/// `DURABLE_GAUGE_HEARTBEAT`, which is why the criterion they carry is a window
/// longer than that heartbeat. Whether they also want this guard is a separate
/// question.
fn gated_store_gauges() -> BTreeSet<String> {
    let durable: BTreeSet<&str> = cog_gateway::security_gateway::DURABLE_POOL_GAUGES
        .iter()
        .map(|name| name.as_str())
        .collect();
    assert!(
        durable.len() >= 8,
        "耐久族只剩 {} 条，下面的排除已经等于没有排除",
        durable.len()
    );

    let gated: BTreeSet<String> = cog_core::documented_metric_names(cog_core::MetricType::Gauge)
        .filter(|name| !durable.contains(name))
        .map(|name| name.to_string())
        .collect();
    // 分母：这条判据的分母是「族里有几条非耐久 gauge」。族空了它就恒绿，
    // 而不是「库里没有共享表 gauge」。
    assert!(
        gated.len() >= 8,
        "闭集里非耐久 gauge 只剩 {} 条，这条判据已经形同虚设",
        gated.len()
    );
    gated
}

#[test]
fn a_rule_reading_a_store_gauge_requires_its_writer() {
    let gated = gated_store_gauges();

    let mut complaints: Vec<String> = Vec::new();
    let mut windowed = 0usize;
    let mut bare = 0usize;
    for (rule, promql) in chart_rules() {
        windowed += windows_over(&promql, &gated).len();
        bare += bare_observation_reads(&promql, &gated).len();
        for complaint in unguarded_store_reading_complaints(&promql, &gated) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    // 另一半分母：一条规则都没把这些 gauge 当观测读时，上面那句「没有投诉」是空的。
    // 今天读它们的正是本仓库那条样本日志与积压规则（取窗口）与普查那条（无窗口），
    // 它们没了就说明题目没了。
    assert!(
        windowed > 0,
        "没有任何告警规则对共享表 gauge 取窗口，这条判据没有分母"
    );
    assert!(
        bare > 0,
        "没有任何告警规则无窗口地读共享表 gauge 并把它当观测，这条判据的另一半没有分母"
    );
    assert!(
        complaints.is_empty(),
        "告警规则把共享表 gauge 的值当作观测（窗口或无窗口），却没有要求写者还在写——\
         一个死在点火那侧的写者会让这条规则永久点亮、谁也没法熄灭它:\n{}",
        complaints.join("\n")
    );
}

/// The positions a windowless read of a store-served gauge can be in, with the
/// two shipped rules that are not this defect as the negative cases: the row
/// budget a sample log is measured against is a bound and the maintenance
/// timestamp is a clock, and both are operands of arithmetic. What is left is a
/// rule comparing the stored value itself, and that one has to ask after its
/// writer — the shape the window-only version of this check could not see.
#[test]
fn a_windowless_read_is_the_subject_unless_arithmetic_holds_it() {
    let only =
        |names: &[&str]| -> BTreeSet<String> { names.iter().map(|s| s.to_string()).collect() };
    let funnel = only(&["cogneva_change_funnel"]);

    // The measurement itself, with its reduction around it: reported.
    assert_eq!(
        unguarded_store_reading_complaints(
            "sum(max by (intent, stage) (cogneva_change_funnel{intent=\"unattributed\"})) > 0",
            &funnel
        )
        .len(),
        1,
        "无窗口地把共享表 gauge 当观测读，没有被报出来"
    );
    // The same read with the writer asked about, in the shape the shipped rule
    // uses: silent.
    assert!(
        unguarded_store_reading_complaints(
            "sum(max by (intent, stage) (cogneva_change_funnel{intent=\"unattributed\"} \
             and ((time() - cogneva_change_funnel_observed_timestamp_seconds) < 3600))) > 0",
            &funnel
        )
        .is_empty(),
        "要求了写者还在写，却被报成没有"
    );
    // And the shape that only looks like one, refused here too: the count over a
    // per-scrape companion reads how long this pod has been serving the series,
    // not how long ago the writer wrote.
    assert_eq!(
        unguarded_store_reading_complaints(
            "sum(max by (intent, stage) (cogneva_change_funnel{intent=\"unattributed\"} \
             and (changes(cogneva_change_funnel_observed_timestamp_seconds[1h]) > 0))) > 0",
            &funnel
        )
        .len(),
        1,
        "把「我这个 Pod 起了多久」当成守卫的写法没有被报出来"
    );
    // A bound: an operand of `*`, so not the subject.
    assert!(
        unguarded_store_reading_complaints(
            "max(metrics_samples_rows) > \
             1.05 * max without (pod, container, instance) (metrics_samples_budget_rows)",
            &only(&["metrics_samples_budget_rows"])
        )
        .is_empty(),
        "作为阈值的读被当成了观测"
    );
    // A clock: an operand of `-`, so not the subject.
    assert!(
        unguarded_store_reading_complaints(
            "(time() - max without (pod, container, instance, job) \
             (cogneva_registry_maintenance_reading_unix)) > 3600",
            &only(&["cogneva_registry_maintenance_reading_unix"])
        )
        .is_empty(),
        "作为时钟的读被当成了观测"
    );
}

/// The guard over the census has to look back further than the publisher's own
/// heartbeat, the same way the durable pool gauges' bounds have to.
///
/// A cell is stamped when its count moves and, for one that is not moving, once
/// per `CENSUS_HEARTBEAT`. So an age bound no longer than that gap is failed by a
/// live census sitting between two of its own stamps. Neither number is written
/// into this test -- the bound comes out of the shipped expression and the
/// heartbeat out of the publisher -- because a bound chosen against a constant
/// drifts from it the moment either side moves, and the drift is silent and in
/// the direction that fires on a working system.
#[test]
fn a_rule_guarding_the_census_waits_longer_than_the_census_heartbeat() {
    let heartbeat = cog_github::change_funnel::CENSUS_HEARTBEAT.as_secs();
    assert!(heartbeat > 0, "普查心跳为 0，下面这条判据恒真");

    let mut guarded = 0usize;
    for (rule, promql) in chart_rules() {
        let Some(bound) =
            companion_age_bound(&promql, cog_core::metric_names::CHANGE_FUNNEL.as_str())
        else {
            continue;
        };
        guarded += 1;
        assert!(
            bound > heartbeat,
            "{rule}: 守卫的年龄界 {bound}s 不严格长于普查心跳 {heartbeat}s——\
             写者坐在两次盖章之间就能造出这个空窗，判据会在完好的普查上点亮"
        );
    }
    // 分母：没有一条规则读普查的伴生钟时，上面那句「没有投诉」是空的。
    assert!(
        guarded > 0,
        "没有任何规则对普查的伴生钟取年龄界，这条判据没有分母"
    );
}

/// The guard over the worktree index sample has to look back further than the
/// heartbeat that re-stamps it.
///
/// The sample is taken before a reset, so on a long-lived tree the gap between
/// two samples is the gap between two resets rather than a cadence the writer
/// chose -- an idle deployer leaves it unsampled for hours. The heartbeat
/// (`INDEX_SAMPLE_HEARTBEAT`) is that reading's second writer, so the guard's
/// bound has to clear it: a bound no longer than that gap is failed by a healthy
/// deployer sitting between two stamps, and the rule goes quiet on the one state
/// it exists for.
///
/// The family swept here is the three series one emission point publishes -- the
/// index-present stat, the missing-files count, and the outcome of the attempt
/// itself -- rather than the one series this test was written for. All three are
/// stamped by the same heartbeat, so a sweep that named one of them passes while
/// its siblings' guards go quiet on a working tree; the denominator is counted
/// per series for exactly that reason.
///
/// Neither number is written into this test -- the bound comes out of the
/// shipped expression and the heartbeat out of the sampler -- because a bound
/// chosen against a constant drifts from it the moment either side moves, and
/// the drift is silent and in the direction that fires on a working system.
#[test]
fn a_rule_guarding_the_index_sample_waits_longer_than_its_heartbeat() {
    let heartbeat = cog_reflection::workspace::INDEX_SAMPLE_HEARTBEAT.as_secs();
    assert!(heartbeat > 0, "索引采样心跳为 0，下面这条判据恒真");

    // 按产出点钉族：三格都由 `WorkspaceManager::sample_index_health_of` 发出、
    // 都由同一道心跳盖章。
    let family = [
        cog_core::metric_names::WORKTREE_INDEX_PRESENT.as_str(),
        cog_core::metric_names::WORKTREE_INDEX_MISSING_FILES.as_str(),
        cog_core::metric_names::WORKTREE_INDEX_SAMPLE_INCOMPLETE.as_str(),
    ];
    for metric in family {
        let mut guarded = 0usize;
        for (rule, promql) in chart_rules() {
            let Some(bound) = companion_age_bound(&promql, metric) else {
                continue;
            };
            guarded += 1;
            assert!(
                bound > heartbeat,
                "{rule}: 守卫的年龄界 {bound}s 不严格长于索引采样心跳 {heartbeat}s——\
                 部署器闲下来坐进两次盖章之间就能造出这个空窗，判据会在完好的树上点亮"
            );
        }
        // 分母按格计，不把两格合并：合并之后一族里少了一格仍然是绿的。
        assert!(
            guarded > 0,
            "{metric} 的伴生钟没有任何规则取年龄界，这条判据对它就等于没有题目"
        );
    }
}

/// A pass-outcome reading must be guarded on the same horizon its sibling is.
///
/// Three loops write their value readings only once a pass returns and publish
/// a separate reading for the pass itself, because a pass that fails leaves the
/// values frozen where the store keeps serving them. The point of that pairing
/// is that a reader of the sibling hears about the stall on the outcome series;
/// if the outcome rule bound a *different* horizon than its sibling, the window
/// between the two would be a stretch where neither speaks and the frozen value
/// reads as healthy -- the shape C161 fixed for the version contract by putting
/// both halves on one bound. Each pair is asserted by name so a rename or a
/// dropped rule shows up as an empty denominator rather than a green count.
#[test]
fn a_pass_outcome_rule_is_guarded_on_the_same_horizon_as_its_sibling() {
    // A series can only be named here once a rule reads it -- the assertion below
    // requires a bound on both halves. So a value reading that freezes on a pass
    // outcome stays out of this table until its rule lands, and that rule's author
    // is the one who must add it. `memory_dead_letter_raw` is such a series today:
    // its obligation is written into its UNREAD entry, which landing the rule
    // forces them to remove.
    let pairs = [
        (
            cog_core::metric_names::MEMORY_UNEXTRACTED_SCAN_FAILED.as_str(),
            cog_core::metric_names::MEMORY_UNEXTRACTED_RAW_AGED_OUT.as_str(),
        ),
        (
            cog_core::metric_names::METRICS_SAMPLE_SWEEP_FAILED.as_str(),
            cog_core::metric_names::METRICS_SAMPLES_ROWS.as_str(),
        ),
        (
            cog_core::metric_names::METRICS_RETIREMENT_RELEASE_FAILED.as_str(),
            cog_core::metric_names::METRICS_RETIRED_ROWS_REMOVED.as_str(),
        ),
    ];
    for (outcome, sibling) in pairs {
        let outcome_bound = guarded_bounds(outcome);
        let sibling_bound = guarded_bounds(sibling);
        assert!(
            !outcome_bound.is_empty(),
            "{outcome} 的伴生钟没有任何规则取年龄界——结局读数没有读者，判据对它等于没有题目"
        );
        assert!(
            !sibling_bound.is_empty(),
            "{sibling} 的伴生钟没有任何规则取年龄界——兄弟那一半没有读者"
        );
        for bound in &outcome_bound {
            assert!(
                sibling_bound.contains(bound),
                "{outcome} 的守卫界 {bound}s 与它的兄弟 {sibling} 的界 {sibling_bound:?} 不在一条线上——\
                 两界之间那段窗口里两半都不说话，冻住的值就被读成健康的"
            );
        }
    }
}

/// Every age bound any shipped rule places on this metric's companion clock.
fn guarded_bounds(metric: &str) -> Vec<u64> {
    let mut bounds = Vec::new();
    for (_rule, promql) in chart_rules() {
        if let Some(bound) = companion_age_bound(&promql, metric) {
            bounds.push(bound);
        }
    }
    bounds.sort_unstable();
    bounds.dedup();
    bounds
}

/// No shipped rule may guard a store-served gauge with a change count over its
/// companion.
///
/// The companion is rendered per scrape, so each serving pod carries its own
/// series and a range over it can only count changes since that series began. A
/// pod younger than the writer's cadence therefore reads zero for a writer that
/// is running normally, and the guard reports the reader's uptime as the
/// writer's absence -- the direction that hides a real fault. The gate reports
/// the shape, so what this test adds is the denominator: the sweep has to be
/// reading the shipped rule set at all, and the family has to be non-empty.
#[test]
fn no_shipped_rule_guards_a_store_gauge_with_a_change_count() {
    let gated = gated_store_gauges();
    let mut offenders: Vec<String> = Vec::new();
    let mut with_age_bound = 0usize;
    for (rule, promql) in chart_rules() {
        for name in &gated {
            if !promql.contains(name.as_str()) {
                continue;
            }
            let companion = cog_core::observability_text::observed_timestamp_name(name);
            if promql.contains(&format!("changes({companion}[")) {
                offenders.push(format!("{rule} 用 changes({companion}[…]) 守 {name}"));
            }
            if companion_age_bound(&promql, name).is_some() {
                with_age_bound += 1;
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "共享表 gauge 的守卫必须是对绝对时刻取年龄（`(time() - <伴生钟>) < <界>`）：\n{}",
        offenders.join("\n")
    );
    // 分母：一族守卫全被删掉时，空 offenders 什么也不证明。
    assert!(
        with_age_bound >= 9,
        "只有 {with_age_bound} 条规则对共享表 gauge 取了年龄界，守卫一族像是被删掉了"
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
    use cog_collaboration::ChangeYieldOutcome;
    use cog_github::redrive_budget::{BudgetSide, RedriveRefusal};

    let rules = chart_rules();
    let domains: [(&str, Vec<&str>); 3] = [
        (
            cog_github::redrive_budget::REDRIVE_REFUSALS_METRIC.as_str(),
            RedriveRefusal::ALL.iter().map(|r| r.as_str()).collect(),
        ),
        (
            cog_github::redrive_budget::REDRIVE_BUDGET_LOSSES_METRIC.as_str(),
            BudgetSide::ALL.iter().map(|s| s.as_str()).collect(),
        ),
        (
            cog_collaboration::observable::CHANGE_YIELD_METRIC,
            ChangeYieldOutcome::ALL.iter().map(|o| o.as_str()).collect(),
        ),
    ];

    let mut unread: Vec<String> = Vec::new();
    for (metric, values) in domains {
        for value in values {
            let read = rules
                .iter()
                .any(|(_, promql)| promql.contains(metric) && selects_value(promql, value));
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

/// Whether an expression selects this label value.
///
/// A value is selected by its own matcher (`outcome="no_artifacts"`) or as one
/// alternative of a regex matcher (`outcome=~"no_artifacts|submit_failed"`) --
/// a family whose cells have one repair each can also be one rule whose summary
/// says which cell it saw, and both spellings select the value.
///
/// Alternatives are compared exactly, and a pattern carrying regex operators is
/// not a name: `no_.*` would otherwise count as having selected whatever the
/// producer adds next, which is the drift this check exists to catch. `!~` is
/// not a selection either -- it is the set the rule excludes.
fn selects_value(promql: &str, value: &str) -> bool {
    if promql.contains(&format!("\"{value}\"")) {
        return true;
    }
    let mut rest = promql;
    while let Some(at) = rest.find("=~\"") {
        let start = at + 3;
        let Some(end) = rest[start..].find('"') else {
            return false;
        };
        let pattern = &rest[start..start + end];
        if pattern.split('|').any(|alt| alt.trim() == value) {
            return true;
        }
        rest = &rest[start + end..];
    }
    false
}

#[test]
fn a_label_value_is_read_through_either_matcher_spelling() {
    assert!(selects_value(r#"a{x="v"}"#, "v"));
    assert!(selects_value(r#"a{x=~"u|v|w"}"#, "v"));
    assert!(selects_value(r#"a{x=~"u|v|w"}"#, "u"));
    // A pattern is not a name.
    assert!(!selects_value(r#"a{x=~"v.*"}"#, "v"));
    assert!(!selects_value(r#"a{x=~".*"}"#, "v"));
    // An exclusion selects the other values, not this one.
    assert!(!selects_value(r#"a{x!~"u|v"}"#, "v"));
    assert!(!selects_value(r#"a{x=~"u|w"}"#, "v"));
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

// `enum Unread` and `unresolved_readers` live in `common/unread.rs`. The scrape
// census gives the same three verdicts to a different population -- the names a
// build can render, which the store never holds -- and one copy of the type is
// what keeps the two censuses from answering "what does `Elsewhere` claim"
// differently.

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
/// An entry's reason is a claim about the repository and it goes stale the way
/// any other claim does -- the build store's round pair was filed here as
/// needing "a declared deferral budget" and four days later the sibling reading
/// on the same completion cadence was already being read against exactly that
/// budget. The pair's entries are gone because the reader was written; a reason
/// that says a bound is missing is worth re-reading against the rules before it
/// is believed.
///
/// `Elsewhere` is a decision: the same fact is already stated by a surface that
/// is read, named here. `Gap` is a debt: nothing states the fact, and the reason
/// it could not be closed is what the entry carries -- a missing declared bound,
/// a cadence that no reading publishes, a signal the producer does not publish
/// yet, or a repair that is a policy decision rather than a reader to write.
/// `GAPS_AT_CENSUS` counts them so the next one must be classified deliberately.
const UNREAD: &[(&str, Unread)] = &[
    // An eval A/B gate ends in "did not measure" more often than not right now:
    // the repository ships no fixed eval task set, so the sandbox agent reports
    // `applicable=false` on every run. What would make a run of those skips a
    // fault is whether a suite is expected to exist, and until one does no
    // threshold can be declared -- so the reader is a debt, not an omission. The
    // closed outcome set, per site, is seeded at zero when the loop starts, so
    // the fact the rule needs is present on every scrape: two sites times six
    // zeroed cells say the gate has not run, and an absent series would only say
    // the binary never published it. That turns "the gate is idling" and "the
    // reading was never wired" into different pictures, which is what makes the
    // rule authorable the moment the suite lands.
    (
        "cogneva_eval_gate_outcomes_total",
        Unread::Gap("what would make a run of self-exempted or unavailable gate runs a fault is whether a fixed eval suite is expected to exist, and the repository ships none, so no threshold is declarable yet"),
    ),
    // The denominator of the rule on the aged-out backlog: the sibling counts
    // every raw whose summary is still owed, this one counts the part past the
    // re-drive window that the reconcile pass repays at a bounded rate, and both
    // are written by the same scan. A reader of the pair is what the rule above
    // is.
    (
        "memory_unextracted_raw",
        Unread::Elsewhere {
            readers: &["memory_unextracted_raw_aged_out"],
            reason: "the past-the-window subset is read by memory_raw_backlog_aged_out; this is its denominator",
        },
    ),
    // The stock the siblings above deliberately exclude: raw sources the ingest
    // path has permanently given up on. It pairs with the `dlq_written` cell of
    // `memory_operations_total`, which counts arrivals only -- a count that only
    // ever rises says nothing on a rate, so without this nobody can state how
    // much has accumulated. The number was always computed (the scan lists the
    // dead-letter namespace to keep diagnosed raw out of the backlog) and thrown
    // away; publishing it is free and needs no second pass. What keeps it a debt
    // is a bound rather than a reader: nothing prunes this namespace, so a rule
    // would have to declare how much stock is too much, and that is a reclaim
    // policy -- and reclaim is not "delete the record", because the backlog
    // exclusion is derived from these very records, so a deletion moves its raw
    // back into the debt on the next pass.
    (
        "memory_dead_letter_raw",
        Unread::Gap("the stock of raw the ingest path has given up on. Nothing prunes the dead-letter namespace, so a rule would have to declare how much stock is too much, and that is a reclaim policy nobody has stated -- and reclaim cannot be a deletion, since the backlog exclusion is derived from these records and removing one sends its raw back into the debt on the next pass. Removing this entry means a rule now reads the series, and that rule carries an obligation this table cannot state mechanically while the entry is here: the stock freezes on exactly the outcome the sibling backlog readings freeze on, so its age bound must be the same one memory_unextracted_scan_failed is guarded on -- and the pair must be added to a_pass_outcome_rule_is_guarded_on_the_same_horizon_as_its_sibling, which today asserts a non-empty bound for both halves and so cannot name a series with no rule yet"),
    ),
    (
        "memory_operation_latency_ms",
        Unread::Gap("memory backend operation latency; no latency bound is declared anywhere in the repository to hang a threshold on"),
    ),
    (
        "metrics_samples_bytes",
        Unread::NotAReading("the help says it lags the row count and is never the pruning criterion; the capacity verdict is the reading with a criterion"),
    ),
    (
        "tier_migration_total",
        Unread::Elsewhere {
            readers: &[
                "cogneva_trace_tier_pass_failures_total",
                "cogneva_trace_tier_last_pass_seconds",
                "cogneva_trace_tier_overdue",
            ],
            reason: "trace_tier_migration_failing, trace_tier_migration_stale and trace_tier_demotion_stalled read the same face's failures, staleness and backlog",
        },
    ),
    (
        "cogneva_version_declared_info",
        Unread::NotAReading("the help calls it an identity label rather than a measurement; the declared version is also on the image tag and in the release tag the artifact is built from"),
    ),
    (
        "cogneva_version_commits_since_release",
        Unread::Gap("how far the running code has moved past the release; no bound is declared, so a rule would have to invent the policy it is meant to enforce"),
    ),
    (
        "cogneva_registry_pruned_tags_total",
        Unread::Elsewhere {
            readers: &["cogneva_registry_gc_owed"],
            reason: "registry_reclaim_debt_unpaid reads the deletions the tag server did not reclaim; the count of the ones that succeeded adds no other fact",
        },
    ),
    (
        "cogneva_registry_rebuild_hold_secs_total",
        Unread::Gap("seconds the tag server was held away per induced restart. The pair the help's ratio needs exists -- cogneva_registry_pruned_tags_total, itself filed as read elsewhere -- so what is missing is a bound on how long a hold may last, and the mechanism declares none on purpose: the hold lifts when the server answers, which is readable on the spot rather than after a budget, and a hold that never lifts is the unpaid reclaim debt that registry_reclaim_debt_unpaid already reads on cogneva_registry_gc_owed. The ratio would say whether the trigger lets the volume grow too full, and that is a policy nobody has stated"),
    ),
    (
        "cogneva_buildah_store_pruned_images_total",
        Unread::Gap("written only when a round freed an image, so its absence is not a zero. It is a volume, and this store deliberately has no volume bound: the plan says in as many words that there is no cap and none is wanted, because the retention set is already the bound and it answers in the unit that matters -- revisions, not bytes -- which the same round publishes as cogneva_buildah_store_kept_images and cogneva_buildah_store_live_images. The cadence this entry used to call unpublished is published as well: cogneva_buildah_store_reading_unix is stamped by every completed round, cogneva_buildah_store_rounds_total counts the attempts beside it, and buildah_store_round_incomplete reads the pair. The family's fault-worthy states are read on those siblings -- a round attempted and never completed, and a keep set computed from an empty cluster read -- and neither of them needs this count"),
    ),
    (
        "cogneva_buildah_store_pruned_layers_total",
        Unread::Gap("the help's own point is that this is not a fixed multiple of the image count, so it is a second volume under a store that has no volume bound on purpose -- the retention set is the bound and it counts revisions; the completion cadence it shares is published by cogneva_buildah_store_reading_unix and read, so what stands in the way is the unit the bound is stated in rather than the cadence or the reading"),
    ),
    (
        "cogneva_buildah_store_freed_bytes_total",
        Unread::Gap("bytes freed, written only when both ends of the round were measurable; the help calls the number apparent rather than real, and the cadence is published and read as above, so what a rule would need is a volume bound this store deliberately does not have"),
    ),
    (
        "cogneva_buildah_store_kept_images",
        Unread::Gap("the help calls it the reading that says the keep set is doing something; what would count as the keep set doing something is a policy nobody declared, and the pass publishes the number every completed round, so the missing half is a bound rather than a reader"),
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
        Unread::Gap("ties the two readings above to the run that produced them; a reader needs the run cadence, which is per-rollout and published by no series -- every published name in this family was enumerated, and none counts judgement runs. The nearest is cogneva_mainline_rollout_attempts_total, whose help calls it the only published cadence of a promotion attempt, and it is not the run count: it is written where the round reaches the decision whether to roll, so a revision the upstream has already left reaches that decision and stops there, and reading it as the run cadence would pair a climbing counter with a timestamp that has no run to advance it on every superseded revision. The one reading that would not need the cadence is this timestamp's own age, and the store does render a companion that carries it -- cogneva_rollout_job_reading_unix_observed_timestamp_seconds, frozen while the writer is gone. That family is now inside the gate's view, so a rule may name it (see the derived half of every_series_an_alert_rule_reads_is_one_something_produces); what still keeps the reader unopened is the same missing bound, since an age is only a fault against a cadence, and the run happens per change rather than on a declared period. The bound the judgement itself runs under is declared -- the job's activeDeadlineSeconds is computed from the startup, rollout and soak budgets -- but it is a value in the job spec rather than a published series, so no rule can name it"),
    ),
    (
        "cogneva_mainline_superseded_rollout_total",
        Unread::Gap("the share of rounds the guard saved by leaving the carried revision alone. It counts answers, not questions: the producer writes a zero on an unskipped round and a zero sample does not move a counter's value, so this series cannot say whether a quiet stretch was a guard that kept answering no or a guard that stopped being called -- that half is now carried by a pair of its own, cogneva_mainline_rollout_attempts_total against cogneva_mainline_supersession_checks_total, which is what mainline_supersession_question_stopped reads. What is left here is the share itself, and what would make a run of skips a fault is how much upstream movement counts as normal on this repository: a bound nobody declared, so the missing half is a bound rather than a reader"),
    ),
    (
        "llm_request_param_clamped_total",
        Unread::Gap("fields the gateway rewrote before sending, by field and upstream. This entry used to be an Elsewhere citing llm_usage_verdict_measured, and that citation is what kills it: the producer's own comment on the usage verdict states that the verdict has two readings and that neither of them is this series, so the sentence offered as the reason is the sentence that excludes it. The two facts are different -- the gauge answers whether one input to one of the adaptations was settled from evidence, the counter answers what happened to a request that was already sent. No rule and no panel on either face names it, and it is not a store series either: the durable face carries only the declared gauge list and this is a counter, so it lives and dies with the gateway process. A rewrite is not a fault by construction -- the temperature clamp fires only where the pool entry or the compat profile says the upstream demands 1, and the max_tokens, store, reasoning_effort, strict and stream_options adaptations follow that same profile -- so a rule would have to declare which rewrites are fault-worthy. What that declaration turns on is evidence, and that picture has moved twice since this entry was written. It used to read that of the three verdicts the admission probe settles (tool calls, temperature, usage) only usage publishes a reading at all, so a clamp decided from a probe result and one decided from a profile guess were the same shape on every series. All three publish now, and the role rewrite is recorded here too -- that was the one rewrite that happened nowhere, since a response never echoes the messages it was sent, so nothing on any series could have shown it. That moves the question rather than answering it: pairing this counter with llm_temperature_verdict_measured does say which of the two inputs decided a temperature clamp, but the counter alone does not, and the other four profile decisions (the max_tokens spelling, store, reasoning_effort, strict) have no probe behind them at all and so can never be decided from measurement. Until somebody states the condition, a rule would report the gateway's own adaptation habits back at us"),
    ),
    (
        "llm_upstream_shape_errored",
        Unread::Gap("a rule cannot state it, and the premise is measured rather than assumed: the mark is cleared by the next probe outcome of either sign -- both the failure path and the success path remove it -- and a marked upstream is due for the very next probe tick, so its lifetime is one tick and its content is its two edges. A window over it would certify a transient, and a window long enough to outlast a restart would report the prober's own cadence back at us. It exists because a handoff that runs and one that was never wired used to look the same on every other series, and telling those apart is a question asked once per change, not a standing bound. What could keep it open is the prober not running, and that is a fact about the pool face's writer rather than about this gauge: the durable pool family is rewritten on a heartbeat, no rule reads that family's freshness, and a reader for it would be that family's entry to make. What is missing here is therefore neither a reader nor a bound on this series but a reading of the pool face's own liveness"),
    ),
    (
        "cogneva_landing_ci_failure_inherited_total",
        Unread::Gap("landings whose red verdict was traced to the commit they were replayed onto rather than to the change, so the change was kept instead of reverted. The underlying condition -- the base branch's tip is red -- is what mainline_ci_verdict_failed reads, but this series does not carry that fact: it carries the non-action taken because of it, and that warn line names the change and the checks it passed through. A rule would have to declare how many spared landings are a fault, which is a bound on how long the branch may stay red while landings keep landing: a policy nobody stated, so the missing half is a bound rather than a reader"),
    ),
    // Tool outputs cut to fit the context window, by tool. The count and its
    // character total were one debt until the fidelity panel landed: a cut is
    // expected traffic rather than a fault -- a tool a task asks to dump a large
    // file will cross the window share on purpose -- so what a rule would need
    // is a bound on how much truncation is normal, and the right amount follows
    // what the tasks read. The panel reads the three of them together instead,
    // which is the reading the pair actually supports: this counter is the
    // denominator of the landed-in-Raw share, and the character total is how
    // much was withheld. The fault in that family is the archive-failure
    // counter, which keeps its rule. So the entry is gone rather than moved --
    // the fact is read by a face, and leaving it here would be an exemption for
    // a series that has a reader.
    (
        "cogneva_tool_output_fetch_budget_exhausted_total",
        Unread::Gap("reads of an archived tool output that stopped short of the bytes because the payload outgrew the caller's byte budget, by tool. The budget is the caller's own, so a bounded read is expected traffic like the truncation counter beside it, not a fault: a caller reading back a large archived output sets the budget it can afford and is told when the source is bigger. What would make a run of them a fault is a bound nobody declared, since the right budget follows what the reads ask for, and the repair for one is local to the caller -- raise the budget or fetch the source directly -- rather than a standing condition a rule could hold open"),
    ),
    (
        "cogneva_project_kind_total",
        Unread::Gap("changes the pipeline dispatched, by the landing recipe it used and by whether the project declared that recipe or the built-in supplied it. A rule would have to say how much of a fleet running on the built-in is a fault, and that is a policy nobody has declared: at the moment this name is minted not one project carries a declaration, so every dispatch lands in the `builtin` cell and any bound set now would fire on the ordinary case. What makes the reading worth publishing before that bound exists is that it is the only place the fallback is visible at all -- a project landing as a built-in Rust workspace and a project that chose to land that way dispatch identically on every other series, and neither is a fault until somebody says which projects are expected to declare. The composition a rule would want is the one already published on the rejection side: a project whose declaration cannot be read is refused, and that refusal is counted under `cogneva_change_rejected_total{cause=project_kind_unusable}`, so the population that needs a threshold here is the one that silently took the built-in rather than the one that failed loudly"),
    ),
    // The three readings minted with the SOP dispatch loop. The loop's own
    // liveness is not a gap: it reports through the background-loop family like
    // every other resident loop, so a stopped dispatcher is already a reading.
    // What none of the three carries is a bound, and each is here for that
    // reason rather than for a missing reader.
    (
        "sop_dispatch_total",
        Unread::Gap("what the SOP dispatch loop did with each configured face on each tick, by `face` and a closed `result` set (compiled, skipped_unchanged, fetch_error, compile_error). The loop starts only where the environment names at least one face, so on a deployment that names none the series is absent rather than zero -- which is why no rule can treat absence as a fault, and why a rule would have to fire on the outcome cells instead. The cell that looks rule-worthy is the failure pair, and what is missing is how many failed ticks a face may have before it is a fault: a document being edited, a platform briefly refusing, and a face whose path is wrong all read as a run of `fetch_error`, and nothing declares how long a stretch of those is expected to be repaired by the next tick. The per-face cadence a reader would otherwise use to bound that stretch is the one number that never appears on the wire -- the intervals live in the configuration the loop reads and are published nowhere -- so a rule would have to derive them from this counter's own rate, which is circular"),
    ),
    (
        "sop_tasks_injected_total",
        Unread::Gap("how many tasks each face's plan put into the DAG, summed over ticks. It exists because a compile that injects nothing and one that injects ten are the same `compiled` cell on the tick counter beside it. What a rule would need is how much a face is expected to keep planned, and that is a property of the face's document rather than of this repository: a face whose plan is five tasks and one whose plan is fifty are both healthy, and the workload a deployment is willing to carry is a policy nobody has declared. It is a volume under no declared bound, which is the same shape as the change and build-store volumes filed above and not a reader anyone could write today"),
    ),
    (
        "sop_unmapped_role_total",
        Unread::Gap("tasks whose declared `role` named no row in the executor table, by `face`. The role is carried through verbatim and the dispatch path maps it to nothing, so a task can be injected with `is_executable` set and a role no hand answers to -- and that is a fact no other series states, since the injected count beside it is the same whether or not anything can pick the work up. The lookup itself is not missing: the executor table already answers it through its own role-to-skill mapping, and the count is taken against that answer rather than against a table spelled here. What a rule would have to say is which of these are faults, and that turns on a mapping nobody has declared: the roles a face's document uses and the role kinds the executor table registers are two vocabularies with no published correspondence between them, so at the moment this name is minted every role a face declares is unmapped until an operator adds a row for it, and a rule firing on any nonzero would flag the wiring of a new face rather than a fault. The bound is what the actor loop's own entry says is missing -- the population that needs a threshold is the one that took a fallback -- and here the fallback is simply that nobody picks the task up"),
    ),
    // The pair minted when a batch consumer's handoff turned out to be
    // unreadable. Neither can be read by a rule, and both are gaps for the same
    // reason: the thing they are compared against lives in a Postgres table,
    // which no query over the metric store can address.
    (
        "cogneva_event_bus_handoff_total",
        Unread::Gap("what one drain pass took off a broadcast channel and how much of that it handed on, by `consumer` and `stage`. It was minted because the loss counters either side of it cannot express the defect that prompted it: a pass that takes events and hands none on leaves `cogneva_event_bus_lagged_total` flat, exactly like a consumer with nothing to do, and leaves the store it writes to empty, exactly like a channel nothing was offered. A rule cannot close that, because the operand that decides it is the row count of the table the handoff writes -- a number that no query over the metric store can read -- so the comparison is a reading-script job rather than a threshold: `stage=\"taken\"` greater than `stage=\"attempted\"` over the same window is the fault, and it needs the second sample a rule would also have. Filed as a gap rather than given an invented threshold because any bound on its own would fire on an idle channel"),
    ),
    (
        "cogneva_observability_event_persist_failed_total",
        Unread::Gap("observability events whose insert failed, one per failed write, counted in the store where the outcome is known. A rule on any increase would look writable and is not, because the series it should be read against is the handoff attempt count above, and a failure there is already a loss by construction rather than a condition that turns into one at some level -- what a rule would add is a page for a defect the empty table already states, on a path whose repair is the write itself. The reason it stays a gap rather than becoming a rule is the same one the handoff entry gives: subtracting the two is what says how many events reached the table, and the row count it would be checked against is not on the metric surface. It is registered here so the census sees that this pair arrives together and that neither is dismissed for want of a reader nobody could write"),
    ),
    // The other half of the pair the sweep writes when it decides whether the
    // message it cannot see is lost. Its sibling has a reader -- the reclaim the
    // wait gave up on is a fault and `dag_task_reclaimed_despite_backlog` reads
    // it -- but a deferral is the wait doing its job: any amount of it is
    // correct, and a rule on a nonzero would page on a healthy backlog. What
    // would make a run of deferrals a fault is a backlog that never drains, and
    // no series states that: the sweep counts the rows it left alone, not the
    // messages still waiting, and the queue's undelivered count is read inside
    // the process that asks rather than published. So the missing operand is a
    // reader -- a published backlog depth -- and not a bound, which is why this
    // is filed rather than given a threshold.
    (
        "cogneva_dag_stalled_scheduled_deferred_for_backlog_total",
        Unread::Gap("rows the sweep left in Scheduled because their queue still held undelivered entries. Any nonzero is the wait working as designed, so a rule on it would page on a healthy backlog; what would make a run of deferrals a fault is a backlog that never drains, and the sweep cannot state that -- it counts the rows it left alone, not the messages still waiting, and the queue's undelivered count is read inside the process that asks rather than published as a series. Closing this entry means publishing that backlog depth and reading the two together, not inventing a threshold for this one"),
    ),
    // The pair minted when the publisher started holding ready tasks back on
    // what the window can pay for. Both arrive in the pass that made the hold
    // visible, and both are debts whose reader is a panel rather than a rule:
    // each of their cells is the gate working as designed at some level, so a
    // threshold on either would page on the mechanism doing its job. Filing
    // them here is the deliberate admission that the panel was not written in
    // this pass, not a claim that no reader can exist.
    (
        "cogneva_ready_admission_held_total",
        Unread::Gap("tasks the publisher refused to dispatch because the pool's window could not pay for them, by `stream` and `reason`. A rule cannot close it: any nonzero is the admission gate holding correctly while the window is spent, and what would make a hold a fault is one that outlasts the window that justified it -- a bound nobody has declared, since the only operand for it is the window's own reset time, which the readings series beside this one carries. Its reader is a panel drawing this count beside the remaining allowance and the reset, which is a panel because the healthy state is a nonzero count for as long as the window is spent. No such panel was written in this pass"),
    ),
    (
        "cogneva_ready_admission_readings",
        Unread::Gap("the admission pass's own arithmetic, by `stream` and `kind`: the bound it applied (`bounded`, written every pass), the jobs the window could pay for (`window_jobs`, written only when a bound exists), what the queue already owed, and what it admitted. This is a reading rather than a signal -- being bounded is the gate working -- so it has no threshold to fire on, and a rule over it would have to invent a level where bounded becomes wrong. Its reader is the panel above, drawn as the four cells of one pass so an idle pass cannot leave a refusal frozen; that panel is what this debt is owed to"),
    ),
];

/// How many `Gap` entries the census left. A new series that no face reads and
/// that nothing else states must be classified, and calling it a gap raises
/// this number on purpose -- the point of the ratchet is that the increase is a
/// decision someone made, not a drift nobody saw.
///
/// It comes down when a debt is paid, and that is what happened to the build
/// store's round pair: both entries said the missing piece was a declared
/// deferral budget, and the twelve-hour bound the sibling reading on the same
/// completion cadence is already read against *is* that budget. The two entries
/// left the table in the pass that wrote `buildah_store_round_incomplete`, so
/// 23 became 21.
///
/// It came down again when the memory decay maintenance loop's outcome counter
/// got its rule. That entry said the missing piece was a declared failure
/// frequency for a loop running on the decay interval, and the shape the
/// siblings in the loop-outcome family already use settles it: an all-failed
/// window is a maintenance path that is down, a single healed pass clears it,
/// and the window is a multiple of the loop's own declared period. The entry
/// left the table in the pass that wrote `memory_decay_passes_all_failed`, so
/// 30 became 29.
///
/// It came down to 21 when eight more entries of the same family left the table
/// in one pass. Every one of them said the missing piece was a rule, and every
/// one of them gave an alert-rule change as the reason it was deferred rather
/// than done -- which is a cost, not a judgement about the reading. The shape
/// the family already uses settles the same question each of them asked, and
/// where a counter carries no healthy cell to pair against (the partition
/// maintenance shortfall count, the baseline port's 0-or-1 gauge) the shape is
/// the one that counter can actually support rather than the one its siblings
/// use. Two of the eight carry a second fault cell beside the loop being down
/// -- `unrepairable` and `reload_failed` -- and those got their own rules,
/// because the all-failed right side would have counted a run of them as a
/// healthy loop. The supervisor's six arms became five rules, one per arm that
/// can report a failure; the autonomous arm is absent on purpose, since its
/// producer records every pass as ok by construction and a rule over it could
/// never fire. So 29 became 21.
///
/// It came down to 20 in the same pass, for the memory bus claim loop's
/// `memory_operations_total` entry. That entry was filed against the metric
/// name, and its reason was about the whole family: the instrumented facade
/// folds every backend operation into the same counter, so a threshold on the
/// name would track the archive housekeeping loop rather than the demand it was
/// meant to measure. What the claim loop added is not volume but a closed set of
/// per-round outcomes on three `operation` values the housekeeping loop never
/// writes -- exactly one of `bus_claim`, `bus_claim_held` and
/// `bus_claim_failed` per round -- and that subset is thresholdable without
/// touching the diluted whole. So the loop gets an all-failed rule and the
/// name-level entry leaves. So 21 became 20.
///
/// It came down to 15 in the pass that re-read every remaining reason against
/// the rules, which is the discipline this constant's own history asks for. Five
/// entries were filed as needing a bound that turned out to be already stated by
/// the mechanism they measure. `memory_operation_errors_total` was filed as
/// needing "a threshold policy for what error rate is a fault"; the counter is
/// incremented from the same call as the total beside it, so the pair states the
/// extreme -- one operation failing every call it got -- with no rate in it at
/// all. The four `evolution_generated_change_*` names were filed as needing "a
/// declared fidelity bound"; the gate accepts or rejects an artifact as a whole
/// and keeps the faithful half, so an artifact that fits has every file and hunk
/// faithful by construction and the pair's zero-remainder is its contract rather
/// than a level somebody has to pick. What that buys is narrower than the ratio
/// the entries asked for and is stated as what it is: nothing generated in the
/// window found its context at all, which is the state a generator reading a tree
/// it no longer has leaves behind. A share of artifacts coming back unfaithful is
/// the ordinary case -- a refusal is answered by design with a rework round -- and
/// is deliberately not read here. So 20 became 15.
///
/// The count stayed at 15 through the pass that re-read the remaining reasons
/// one more time. Four of them were filing a false obstacle: three buildah store
/// counters and the registry rebuild hold each said the cadence a reader needs is
/// published by no series, and in each case it is -- cogneva_buildah_store_reading_unix
/// is stamped by every completed round, cogneva_buildah_store_rounds_total counts
/// the attempts beside it, buildah_store_round_incomplete reads the pair, and the
/// hold's ratio has its partner in cogneva_registry_pruned_tags_total. What stands
/// in the way of those four is a bound the mechanisms deliberately do not state:
/// the build store says it has no cap because the retention set is the bound and it
/// counts revisions rather than bytes, and the tag server's hold lifts when the
/// server answers rather than after a budget. That is a decision made, not a
/// reading withheld. The fifth was the one that held
/// up: the rollout reading really does have no published run cadence, and its reason
/// now names the counter that looks like one and is not. So 15 stayed 15.
///
/// It stands at 16 for the pass that published `memory_dead_letter_raw`, the
/// stock of raw the dead-letter reconcile scan had been computing and throwing
/// away. That series arrives with no reader and its obligation written into its
/// own entry, so the count is one higher than the reasons above leave it. The
/// same arrival showed that this constant had been sitting one above the table
/// -- 16 against the 15 rows those reasons describe -- so the new entry took the
/// spare slot without moving the number, which is to say it went in already
/// classified by nobody. The assertion below therefore compares the two as an
/// equality. While it was a `<=`, that spare slot absorbed the next arrival
/// silently, and a row leaving the table went silently green as well; now either
/// direction is something the author has to say out loud, which is the only
/// reason to count them here at all.
///
/// It came down to 17 in the pass that gave the tool-output fidelity pair a
/// panel. The truncation count had been filed as a debt whose reader was a bound
/// nobody could declare -- how much truncation is normal follows what the tasks
/// read -- and that reason was true and still is. What changed is that a reader
/// arrived which does not need the bound: the panel draws the share of cuts whose
/// tail landed, and that share is the count minus the failure counter, so the
/// count is no longer read for its own level but as one operand of an identity.
/// The character total went in beside it in the same pass and needed no entry of
/// its own, which is the ordinary case for a new name added with its reader
/// rather than before one.
/// It went to 18 in the pass that published `cogneva_project_kind_total`, the
/// dispatch reading that says whether a change landed under the recipe its
/// project declared or under the built-in. Its reader is a bound rather than a
/// rule for the reason its own entry gives: nobody has declared how much of a
/// fleet running on the built-in is a fault, and every dispatch lands there
/// today. The count is what makes that a deliberate exemption instead of an
/// omission.
///
/// It went to 21 in the pass that published the SOP dispatch loop's three
/// readings. They arrived together and are filed together, because the loop
/// mints all three or none: a deployment that configures no face makes no
/// outbound call and writes no cell, so there is no state in which one of them
/// exists and its siblings do not. Each is a gap for the reason its own entry
/// gives -- a bound nobody declared, twice, and a role vocabulary with no
/// published correspondence to the executor table, once -- and the tick
/// counter's entry is the one worth reading, since it is the series a reader
/// would assume a rule already covers.
///
/// It went to 22 in the pass that gave a consumer's takings a series of their
/// own. That one is a debt for a reason no bound can close: what makes a
/// throughput reading actionable is the traffic it should have carried, and the
/// platform publishes no traffic counter on the channels these consumers read,
/// so "flat takings" cannot be turned into a rule by picking a threshold. The
/// entry says which consumer it is wired for and what the missing operand is.
///
/// It came back down to 21 in the pass that published that missing operand. The
/// entry's reason named the exact thing it needed -- a counter on the producer
/// side of the channel -- and `cogneva_event_bus_emitted_total` is it, so the
/// debt is paid rather than reclassified: `event_bus_consumer_stopped_taking`
/// reads the two together and the takings series has a reader. That is the
/// discipline this constant's own history asks for, and this is the first entry
/// whose reason named the series that would close it rather than a bound nobody
/// had declared.
///
/// It went to 23 in the pass that published the handoff pair. The defect that
/// prompted them was a table that had never been written to while every series
/// around it looked healthy, so both were minted to make the middle of that path
/// visible: what a drain took, what it handed on, and how many writes failed.
/// Neither has a reader, and neither can have one of the kind this census
/// counts -- their deciding operand is a row count in Postgres, which no rule or
/// panel can query -- so both are filed as gaps whose reason names the
/// comparison a reading script has to make instead. They arrive together
/// deliberately: one without the other still cannot say whether the fault is in
/// the consumer or in the write.
///
/// It went to 24 in the pass that gave the backlog guard its pair, and that
/// number is the one this constant holds. The pair was minted to tell a drain
/// from a loss: a task stuck in Scheduled past the transport's window is one
/// thing, and whether its message is still in line is another. Half of the pair
/// got a reader with it -- the reclaim that happened while the queue still
/// reported undelivered entries is a fault, and a rule now reads it. The other
/// half is the wait holding, which is what the sweep is supposed to do; a rule
/// on it would page on a healthy backlog, and the reading that would make it a
/// fault (how many messages are still waiting) is not published. So it is a
/// debt whose missing operand is a reader rather than a bound.
///
/// It went to 26 in the pass that stopped the publisher from dispatching work
/// the pool's window cannot pay for. The hold had been invisible in exactly the
/// shape that let the pacing defect run for days -- a gate doing its job and a
/// publisher with nothing to publish leave the same trace, no dispatch -- so the
/// pass minted two series, the refused count and the arithmetic behind it. Both
/// are debts with the same creditor and neither is a threshold: every cell says
/// the gate is working, and what would make a hold wrong is its duration, which
/// only the window's reset time can bound. The reader they need is a panel
/// drawing the four cells of one pass beside the remaining allowance, and that
/// panel is what this pass did not write. Recording both is what keeps the
/// increase a decision rather than a drift.
///
/// It went to 27 in the pass that published the PostgreSQL pool ceiling. That
/// number is not how many connections a pool holds, so a threshold on the value
/// would page on the fix working; the one thing worth waking somebody for is
/// `source="server_unreadable"`, the value that says the server was never read
/// and the configured ceiling stands unmeasured. The entry filed that reader as
/// a debt rather than a bound, because writing it touches the chart config that
/// three rendered profiles embed and the static k3s copy mirrors. The rule
/// `pg_pool_ceiling_unmeasured` is that reader, so the debt is paid and the
/// number comes back down to 26 -- the entry left the table in the pass that
/// wrote the rule, not reclassified out of it.
const GAPS_AT_CENSUS: usize = 26;

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
                Unread::Elsewhere { reason, .. }
                | Unread::NotAReading(reason)
                | Unread::Gap(reason) => *reason,
            };
            reason.split_whitespace().count() < 5
        })
        .map(|(name, _)| *name)
        .collect();
    assert!(
        unreasoned.is_empty(),
        "UNREAD 里这些条目没写清理由（豁免要说出那份事实在哪个面上、欠账要说出为什么这轮补不了）: {unreasoned:?}"
    );

    // The exemption's reader is looked up, not read. A reason is prose, so the
    // gate could only ever count its words -- and it did, once, for two entries
    // whose reason named a rule that reads neither series they exempted. A
    // series name is either one a face reads or it is not, so the same claim
    // written this way cannot be made without being true.
    let any_read: BTreeSet<String> = rule_read.union(&panel_read).cloned().collect();
    let unresolved: Vec<String> = UNREAD
        .iter()
        .flat_map(|(name, verdict)| match verdict {
            Unread::Elsewhere { readers, .. } => unresolved_readers(&any_read, name, readers),
            Unread::NotAReading(_) | Unread::Gap(_) => Vec::new(),
        })
        .collect();
    assert!(
        unresolved.is_empty(),
        "UNREAD 里这些豁免指向的序列没有任何规则在读，登记本身成了空头许可:\n{}",
        unresolved.join("\n")
    );

    let gaps = UNREAD
        .iter()
        .filter(|(_, v)| matches!(v, Unread::Gap(_)))
        .count();
    assert_eq!(
        gaps, GAPS_AT_CENSUS,
        "零读者序列的欠账数与 GAPS_AT_CENSUS 不再相等（表内 {gaps} 笔，常数 {GAPS_AT_CENSUS}）。多一笔：新增的零读者序列必须当轮补读者，或把它的成因写进 UNREAD 并同步改 GAPS_AT_CENSUS（改这个数就是承认多欠一笔）。少一笔：那是补上了读者，同样要把它改回来——这个数就是「还有几笔没读者」的账，不再相等意味着账没人核过"
    );
}

/// An exemption has to name a reader that is there.
///
/// The census shipped two `Elsewhere` entries for the sample log's row count and
/// budget whose reason said the floor rule read one of their operands. That rule
/// reads neither -- the two names occurred nowhere in it -- and the gate passed
/// both, because a reason is prose and all it can be asked is whether it is
/// long enough. Written as a series name instead, the same claim is a lookup:
/// the name is read by a shipped rule or it is not.
///
/// This is the unit side of the criterion, against a fabricated claim rather
/// than the shipped table, so the check that the table is clean cannot be the
/// only thing standing between a false exemption and a green run.
#[test]
fn an_elsewhere_entry_that_names_a_reader_no_rule_has_is_reported() {
    let any_read: BTreeSet<String> = chart_rules()
        .iter()
        .flat_map(|(_, promql)| metric_names_in(promql))
        .chain(dashboard::series())
        .collect();

    // A published name nothing reads: the claim the false entries made, in the
    // form the gate can check.
    let named = unresolved_readers(
        &any_read,
        "metrics_samples_rows",
        &["metrics_samples_bytes"],
    );
    assert!(
        !named.is_empty(),
        "豁免指向一条没有读者的序列时必须报出来，否则豁免又退回成一句判据核不了的话"
    );

    // The reader that really exists stays silent, so the criterion is not
    // refusing every entry it is given.
    let resolved = unresolved_readers(
        &any_read,
        "metrics_samples_rows",
        &["metrics_samples_over_capacity"],
    );
    assert!(resolved.is_empty(), "{resolved:?}");
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

/// Each GitOps poll's outcome must be published on a face the deployment reads.
///
/// The puller brings the cluster's desired state in, and its loop is on a
/// supervised cadence, so a puller that *stopped* was already visible through
/// the loop's own readings. What had no reader was the puller *doing its job and
/// failing*: the beat is stamped at the top of every cycle whether or not the
/// poll returned, and the puller published no series of its own, so a puller
/// that cannot reach the release branch poll after poll and a cluster with
/// nothing to pull were the same picture -- the cluster simply stopped taking in
/// updates and every reading stayed green. Three things make the fix real, and
/// each is asserted here: the reading exists, the construction hands the puller
/// the store it is written to, and the write sits in `poll_once`'s shell rather
/// than inside the attempt, because the attempt returns early on its failure
/// paths and a write behind those returns would be gated exactly when it matters.
#[test]
fn the_gitops_puller_publishes_each_polls_outcome() {
    let plugin = read("crates/cog-reflection/src/plugin.rs");
    let construct = plugin
        .find("GitOpsPuller::new(")
        .expect("plugin.rs no longer constructs the GitOps puller here");
    let start = plugin[..construct]
        .rfind("promotion.gitops.puller_enabled")
        .expect("拉取端的构造不在 puller_enabled 守卫里了");
    let end = plugin[construct..]
        .find("run_puller_loop")
        .map(|i| construct + i)
        .expect("the puller is no longer started right after it is constructed");
    let block = &plugin[start..end];
    assert!(
        block.contains("MetricsBackend"),
        "拉取端没拿到指标 store：poll 的结局又只剩那句随 Pod 消失的日志了"
    );
    assert!(
        block.contains("with_metrics"),
        "拉取端拿到了 store 但没有交给自己（构造完之后必须 with_metrics）"
    );

    let puller = read("crates/cog-reflection/src/gitops_puller.rs");
    let shell = puller
        .find("pub async fn poll_once(")
        .expect("GitOpsPuller::poll_once 没了");
    let body_end = puller[shell..]
        .find("\n    }\n")
        .map(|i| shell + i)
        .expect("poll_once 的函数体截不出来");
    let body = &puller[shell..body_end];
    assert!(
        body.contains("poll_attempt("),
        "poll_once 不再是包住一次尝试的薄壳：结局会随它的内部路径被门控"
    );
    assert!(
        body.contains("report_poll_outcome"),
        "poll_once 这条薄壳没有发布结局：每轮失败就没有读数"
    );
    assert_eq!(
        puller.matches("report_poll_outcome(").count(),
        2,
        "结局的写点只能有一处（定义 + poll_once 里那一次调用）——多出来的写点会各写一半，\
         少了的那一次就等于没写"
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

// ── 集合算子的操作数报的是不是过滤后的取值 ───────────────────────────────────

/// Drop the matching clause (`on(...)` / `ignoring(...)` / `group_left(...)` /
/// `group_right(...)`) the splitter leaves attached to the right operand, and
/// one layer of parentheses around what remains.
fn set_operand_expression(operand: &str) -> &str {
    let t = operand.trim();
    let rest = ["on", "ignoring", "group_left", "group_right"]
        .iter()
        .find_map(|kw| {
            let rest = t.strip_prefix(kw)?.trim_start();
            rest.starts_with('(').then_some(rest)
        });
    let expr = match rest {
        Some(rest) => match matching_paren_end(rest, 1) {
            Some(end) => rest[end + 1..].trim_start(),
            None => t,
        },
        None => t,
    };
    strip_outer_parens(expr)
}

/// Whether an operand reports a 0/1 indicator as its own value rather than the
/// value it measured.
///
/// `bool` is a value map: every series the selection matches survives carrying 1
/// or 0 instead of what it measured. That is what it is for when something
/// above consumes the indicator — `sum(x > bool 0)` counts the series that
/// satisfy the comparison, and `avg_over_time((y < bool 1)[15m:1m])` is the
/// fraction of the window it held. A set operator consumes no such thing:
/// `and` and `unless` pair on labels alone, so an indicator at the operand's own
/// top level is read by nobody. On their right side the damage is worse than
/// pointless — that side's value is discarded whatever it holds, so mapping it
/// leaves the operand selecting every series it matched, and a rule written as
/// "this happened and that never did" fires for as long as this keeps happening.
///
/// Only the top level counts. Deeper in, the 0/1 is inside a call or a range
/// window, which is a consumer.
fn reports_an_indicator(operand: &str) -> bool {
    let expr = set_operand_expression(operand);
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
            if &expr[start..i] == "bool" {
                return true;
            }
            continue;
        }
        i += 1;
    }
    false
}

/// Complaints about a set operator reading an operand that maps its values
/// instead of filtering them.
///
/// The two sides fail differently and are reported separately. On the right of
/// `and` / `unless` the mapping destroys a filter and the rule fires on
/// membership: `A and on() (B == bool 0)` holds whenever B has any sample,
/// because the set operator never looks at the 1/0 it was handed. On the left
/// the value does reach the condition, so the mapping there does not open the
/// gate — it blinds the verdict, which then judges a constant 1 instead of what
/// the operand measured.
///
/// `or` is left alone: it reports the value it takes from either side, so both
/// operands' values reach the condition.
///
/// This is not an evaluator, and it reads text rather than semantics: an
/// expression it cannot split is not a complaint, and neither is an indicator
/// buried in a call. A shape this misses leaves the rule as wrong as it already
/// is, while a wrong complaint would block a rule that works.
fn indicator_on_a_set_operand_complaints(expr: &str) -> Vec<String> {
    let mut complaints = Vec::new();
    collect_indicator_complaints(expr, &mut complaints);
    complaints
}

fn collect_indicator_complaints(expr: &str, out: &mut Vec<String>) {
    let Some((op, left, right)) = split_set_operator(expr) else {
        return;
    };
    if op != "or" {
        if reports_an_indicator(left) {
            out.push(format!(
                "`{op}` 左侧操作数 `{}` 用 `bool` 把取值换成了 0/1：`{op}` 报给条件的正是左侧的取值，\
                 带上 `bool` 之后判词看见的永远是 1，量出来的值到不了它。该去掉的是 `bool`，不是这个比较",
                set_operand_expression(left)
            ));
        }
        if reports_an_indicator(right) {
            out.push(format!(
                "`{op}` 右侧操作数 `{}` 用 `bool` 把过滤降级成了「这一侧有样本就成立」：`{op}` 只按标签配对、\
                 右侧报什么值都不看，映射成 0/1 之后它选中的是匹配到的每一组序列，而不是满足那个比较的那些。\
                 要「另一侧不成立」写成 `unless on() (… > 0)`",
                set_operand_expression(right)
            ));
        }
    }
    collect_indicator_complaints(left, out);
    collect_indicator_complaints(right, out);
}

#[test]
fn a_set_operand_that_maps_instead_of_filtering_is_reported() {
    // The shape this check exists for, and the rule that shipped with it: the
    // summary promises "finished in the last day and not one of them handed a
    // change to a sink", the expression fires for as long as any cell of the
    // counter has moved.
    let shipped = "(max without (pod, container, instance, job) \
                   (increase(self_evolution_change_yield_total[24h])) > bool 0) \
                   and on() (max without (pod, container, instance, job) \
                   (increase(self_evolution_change_yield_total{outcome=\"submitted\"}[24h])) == bool 0)";
    let complaints = indicator_on_a_set_operand_complaints(shipped);
    assert_eq!(complaints.len(), 2, "{complaints:?}");
    assert!(
        complaints.iter().any(|c| c.contains("过滤降级")),
        "{complaints:?}"
    );

    // The rewrite is accepted: the filter is back on the right operand, and it
    // is `unless` that says "and not".
    assert!(indicator_on_a_set_operand_complaints(
        "(max without (pod, container, instance, job) \
         (increase(self_evolution_change_yield_total[24h])) > 0) \
         unless on() (max without (pod, container, instance, job) \
         (increase(self_evolution_change_yield_total{outcome=\"submitted\"}[24h])) > 0)"
    )
    .is_empty());

    for fine in [
        // An indicator something above consumes: the count of the series that
        // satisfy the comparison.
        "sum(x > bool 0) and on() (y > 0)",
        // The same, through a subquery: the fraction of the window the compare
        // held for.
        "avg_over_time((sum(cogneva_evolution_change_queue_owner) < bool 1)[15m:1m]) \
         and on() (count_over_time(cogneva_evolution_change_queue_owner[30m]) \
         > count_over_time(cogneva_evolution_change_queue_owner[15m]))",
        // `or` reports the value it takes from either side.
        "(a == bool 0) or (b > 5)",
    ] {
        assert!(
            indicator_on_a_set_operand_complaints(fine).is_empty(),
            "{fine}"
        );
    }

    // The mapped operand on the left is the other half: the gate stays where it
    // was, and what breaks is the verdict, which now judges a constant.
    let left = indicator_on_a_set_operand_complaints("(a > bool 0) unless on() (b > 0)");
    assert_eq!(left.len(), 1, "{left:?}");
    assert!(left[0].contains("到不了它"), "{}", left[0]);

    // Widening beats guessing: an operand this cannot read is not a complaint.
    for fine in [
        "x and y",
        "a and on() (avg_over_time((b < bool 1)[5m:1m]) > 0)",
        "a unless on() (b > 0) unless on() (c > 0)",
    ] {
        assert!(
            indicator_on_a_set_operand_complaints(fine).is_empty(),
            "{fine}"
        );
    }
}

#[test]
fn no_written_rule_maps_a_set_operand_instead_of_filtering_it() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for complaint in indicator_on_a_set_operand_complaints(&promql) {
            complaints.push(format!("{rule}: {complaint}\n    {promql}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "这些规则的集合算子操作数没有被当成过滤读:\n{}",
        complaints.join("\n")
    );
}

/// An aggregation's answer depends on where the value comes from, so a rule
/// that aggregates a series has to say which answer it means.
///
/// `sum` over a process-local counter counts events, and is right. `sum` over a
/// series every process renders from the same store row multiplies one fact by
/// the number of processes that happened to be running — 2026-10-19 measured 53
/// such counters, two live processes each reporting byte-identical totals, and
/// the error grows further inside a window because dead pods' last samples stay
/// in it. The distinction is not in the name: `_total` suffixes both kinds.
///
/// So this test asks the registry, not the text. A name the registry knows and
/// has not classified fails here; a name classified as store-derived fails here
/// too, unless the span already reduced the process copies to one with
/// `max`/`min`/`topk`/`count`, after which summing facts is exactly what the
/// outer aggregation is for. Series from outside the registry (`kube_*`,
/// `ALERTS`) are out of scope for the same reason as everywhere else in this
/// file: their answer is not this repository's to declare.
#[test]
fn no_rule_aggregates_a_series_whose_derivation_makes_the_aggregate_wrong() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in chart_rules() {
        for span in promql::summed_spans(&promql) {
            for name in promql::metric_names_in(&span) {
                if !cog_core::is_registered_metric(&name) {
                    continue;
                }
                match derivation::metric_derivation(&name) {
                    Some(derivation::MetricDerivation::Store)
                        if !promql::deduplicated_over(&span, &name) =>
                    {
                        complaints.push(format!(
                            "{rule} aggregates {name}, which is rendered from the store: \
                             every process reports the same fact and the aggregate \
                             multiplies it. Put `max by (...)` (or any one-per-label-set \
                             operator) between the series and the sum.\n    {promql}"
                        ));
                    }
                    Some(_) => {}
                    None => complaints.push(format!(
                        "{rule} aggregates {name} without declaring its derivation. \
                         Add it to METRIC_DERIVATIONS: `sum` is only an answer once \
                         someone says whether the value is per-process or from the store.\n    {promql}"
                    )),
                }
            }
        }
    }

    assert!(
        complaints.is_empty(),
        "这些规则聚合了来历未声明或状态派生的序列:\n{}",
        complaints.join("\n")
    );
}

/// The reverse control for the gate above: the reader has to find the operand
/// of an aggregation and not the `by (...)` label list next to it, and it has to
/// tell a deduplicated aggregate from a plain one.
///
/// Without this, a reader that returns nothing for every expression would pass
/// the gate by never complaining — the file it judges currently has no
/// store-derived aggregate that is also correct, so the green corpus cannot
/// show the difference.
#[test]
fn the_derivation_gate_separates_a_label_list_from_an_operand() {
    let spans = promql::summed_spans(
        r#"sum by (tier) (increase(cogneva_change_tier_total[24h])) unless sum(increase(X_total[1h])) > 0"#,
    );
    assert_eq!(
        spans,
        vec![
            "increase(cogneva_change_tier_total[24h])".to_string(),
            "increase(X_total[1h])".to_string(),
        ]
    );

    // A plain aggregate over the series: the copies are still being added.
    assert!(!promql::deduplicated_over(
        &spans[0],
        "cogneva_change_tier_total"
    ));
    // Reduced to one value per label set first: the outer sum adds facts.
    assert!(promql::deduplicated_over(
        "max by (tier) (increase(cogneva_change_tier_total[24h]))",
        "cogneva_change_tier_total"
    ));
    assert!(promql::summed_metric_names("sum(max by (tier) (X_total))").contains("X_total"));
}
