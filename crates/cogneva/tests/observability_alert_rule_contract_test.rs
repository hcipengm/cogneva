//! principle: feedback_a_closed_state_set_must_route_an_unrecognized_verdict_to_a_non_pass_state
//! principle: feedback_a_row_is_an_event_so_the_agreeing_state_needs_its_own_series
//! principle: feedback_a_rule_and_the_name_it_reads_are_locked_by_two_censuses_that_guard_opposite_directions
//! principle: feedback_a_rule_that_certifies_a_window_must_require_the_window_to_be_covered
//! principle: feedback_a_metric_vanishing_from_the_reader_census_means_the_clause_is_illegal
//! principle: feedback_a_pair_exclusion_shipped_before_its_label_matches_nothing
//! principle: feedback_a_registration_table_grows_only_where_its_consumers_are
//! principle: feedback_an_aggregate_over_a_series_family_can_starve_while_every_element_has_a_reader
//! principle: feedback_an_alarm_moved_off_a_published_series_orphans_it_and_costs_a_census_entry
//! principle: feedback_an_absence_question_needs_a_window_not_an_instant
//! principle: feedback_a_range_selector_keeps_reading_a_subject_that_has_already_been_deleted
//! principle: feedback_a_companion_clock_only_moves_if_the_writer_advances_it
//! principle: feedback_a_transient_that_hits_every_subject_at_once_is_one_event_not_n
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

use crate::common;

use common::lex::Dialect;
use common::routes::{matching_paren, read_repo, repo_root};
use common::service_topology;

use std::collections::{BTreeMap, BTreeSet};

use cog_gateway::security_gateway::{RangeReadBaseline, RANGE_READ_BASELINES};

use crate::cells_written;
use crate::closed_set;
use crate::dashboard;
use crate::derivation;
use crate::producer;
use crate::promql;
use crate::unread;

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
        "cogneva_boot_rss_bytes",
        "crates/cog-observability/src/boot_footprint.rs",
    ),
    (
        "cogneva_claim_scan_age_seconds",
        "crates/cog-observability/src/claim_store.rs",
    ),
    (
        "cogneva_claim_scan_dirs",
        "crates/cog-observability/src/claim_store.rs",
    ),
    (
        "cogneva_claim_scan_readable",
        "crates/cog-observability/src/claim_store.rs",
    ),
    (
        "cogneva_image_store_age_seconds",
        "crates/cog-observability/src/image_store.rs",
    ),
    (
        "cogneva_image_store_bytes",
        "crates/cog-observability/src/image_store.rs",
    ),
    (
        "cogneva_image_store_parts",
        "crates/cog-observability/src/image_store.rs",
    ),
    (
        "cogneva_image_store_readable",
        "crates/cog-observability/src/image_store.rs",
    ),
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
    // The host, as the gate measured it for its last derivation. Every one of
    // these is a quantity the bound is derived from, published beside the
    // ceiling that took the smallest of them so a reader can see which wall is
    // binding and which is next. They live in the reflection crate rather than
    // the contract crate because they are one process's sample of the host it
    // runs on, not a name every crate shares -- and the rules below read them
    // to say why a bound is where it is, not to move it.
    (
        "cogneva_build_gate_ceiling",
        "crates/cog-core/src/contract/build_gate.rs",
    ),
    (
        "cogneva_host_cpu_busy_cores",
        "crates/cog-reflection/src/build_gate/mod.rs",
    ),
    (
        "cogneva_host_cpu_cores",
        "crates/cog-reflection/src/build_gate/mod.rs",
    ),
    (
        "cogneva_host_memory_available_bytes",
        "crates/cog-reflection/src/build_gate/mod.rs",
    ),
    (
        "cogneva_host_pressure_avg60",
        "crates/cog-reflection/src/build_gate/mod.rs",
    ),
    (
        "cogneva_host_disk_free_bytes",
        "crates/cog-reflection/src/build_gate/mod.rs",
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
    // The window no refusal covers: the disk stalled while no build was running.
    // A refusal is an event at the gate, and this is a state of the host -- the
    // gate admitted nothing and the machine was still stopped, which is the one
    // thing in this family that says the pressure keeping the bound down is not
    // the gate's own.
    (
        "cogneva_build_gate_io_outside_seconds_total",
        "crates/cog-core/src/contract/build_gate.rs",
    ),
    // The turn granted and never taken: a claim on the next build that a later
    // arrival read past its freshness budget. It is published by the reflection
    // process that runs the gate, not by the contract beside the other three,
    // and it is the one reading in this family that names the work that lost
    // the turn rather than the slot it lost -- which is why the rule that reads
    // it is where a starved asker becomes visible at all.
    (
        "cogneva_build_gate_reservation_lapsed_total",
        "crates/cog-reflection/src/build_gate/mod.rs",
    ),
    // The same work's other half: how long it waits between rewriting the claim
    // it already holds. Same publisher for the same reason, and the pair is the
    // point -- the lapse counter is silent while a claim is alive, so a work
    // that renews often enough to keep crowding everyone else out is invisible
    // until the interval between renewals is itself a reading.
    (
        "cogneva_build_gate_reservation_renew_gap_seconds",
        "crates/cog-reflection/src/build_gate/mod.rs",
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
    // The two task-graph readings a rule on "the graph stopped moving" is
    // built from, and they are a pair for the reason a stall is hard to read:
    // the oldest-untouched age grows while everything else holds still, and
    // the last-success stamp is what separates a graph that looks the same
    // because nothing changed from one that looks the same because the reader
    // died. Neither is a count, so neither belongs to the status vector.
    (
        "cogneva_task_oldest_untouched_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_task_graph_measure_last_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_task_graph_measure_failures_total",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_tasks_readings",
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
        "crates/cog-core/src/contract/loop_health.rs",
    ),
    // The work a cycle may run without a beat: the stall rule adds it to the
    // period, because a cadence bounds only the wait between cycles and a loop
    // whose cycle runs a build would age past a multiple of its own period by
    // construction.
    (
        "cogneva_loop_work_budget_seconds",
        "crates/cog-core/src/contract/loop_health.rs",
    ),
    (
        "cogneva_loop_tick_age_seconds",
        "crates/cog-core/src/contract/loop_health.rs",
    ),
    (
        "cogneva_loop_deaths_total",
        "crates/cog-core/src/contract/loop_health.rs",
    ),
    // The restarts are a rule of their own rather than a note on the death rule:
    // a loop that panicked and was run again publishes all of its series again,
    // so every other reading in the deployment shows a loop that never stopped.
    (
        "cogneva_loop_restarts_total",
        "crates/cog-core/src/contract/loop_health.rs",
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
        "crates/cog-core/src/contract/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_held",
        "crates/cog-core/src/contract/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_acquisitions_total",
        "crates/cog-core/src/contract/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_losses_total",
        "crates/cog-core/src/contract/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_probe_failures_total",
        "crates/cog-core/src/contract/loop_health.rs",
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
    // What a process loaded from the data root it runs on. Both halves of the
    // restore check are on this one series because they are one reading about
    // one moment: the package's answer and the process's, published together by
    // the start that follows a restore.
    (
        "cogneva_policy_store_readings",
        "crates/cog-reflection/src/store_readouts.rs",
    ),
    (
        "cogneva_process_zombies",
        "crates/cog-observability/src/process_zombies.rs",
    ),
    // The replan loop's own account, in two halves: what each pass did with the
    // breach it found, and why a pass could not measure one at all. Both are
    // collected by the gateway process as it runs rather than by a scrape
    // target, so the producer is the source file and not the chart.
    (
        "cogneva_project_control_outcomes_total",
        "crates/cog-gateway/src/project_control.rs",
    ),
    (
        "cogneva_project_control_unmeasured_total",
        "crates/cog-gateway/src/project_control.rs",
    ),
    // The project face's audit outlet, in three counters: how many rows were
    // asked for, how many of them did not land, and how many found no writer at
    // all. Split because the repairs differ -- the store, the database, and the
    // assembly -- and read together by the two rules above them, the ratio for
    // the first two and the presence of the third.
    (
        "cogneva_project_audit_writes_total",
        "crates/cog-gateway/src/project_audit.rs",
    ),
    (
        "cogneva_project_audit_write_failures_total",
        "crates/cog-gateway/src/project_audit.rs",
    ),
    (
        "cogneva_project_audit_unwired_total",
        "crates/cog-gateway/src/project_audit.rs",
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
        "cogneva_signal_watcher_running",
        "crates/cog-reflection/src/signal_readings.rs",
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
    // The claim-capacity loop: what became of each volume in each round, and the
    // two sizes the decision was made from. The outcome labels are a closed set
    // published at zero, so "this loop never had to clamp" is a readable fact and
    // not an absence.
    (
        "cogneva_volume_capacity_outcome_total",
        "crates/cog-reflection/src/volume_capacity.rs",
    ),
    // What was asked for, what the storage granted, and the peak measured off
    // the disk. Published as three `state` label values from the same loop, so
    // a rule can compare the request against the answer instead of trusting
    // that an accepted patch means the volume grew.
    (
        "cogneva_volume_capacity_bytes",
        "crates/cog-reflection/src/volume_capacity.rs",
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
    // The other drift loop, and the one whose verdict is about people rather
    // than about the cluster: one series per declared face, published with the
    // state as a label rather than as a value because the verdict is a closed
    // set and an enum encoded as a number needs its mapping published beside it
    // before anyone can read a 3. It is scrape-only rather than in the closed
    // set, so the producer check cannot find it by name and needs this row.
    (
        "cogneva_governance_face_state",
        "crates/cog-orchestrator/src/face_drift.rs",
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
    // 部署器自己的 kubectl 调用被授权拒绝的次数，按服务端说出的动词和资源分开。
    // 与上面那条互补：预检问的是「包里会 apply 的 kind」，这条接住部署器**读自己
    // 判断依据**的那些命令（ResourceQuota、LimitRange、在跑的配额），它们不在任何
    // 包里，也就进不了那道预检的视野。2026-10-19 那两小时半的 LimitRange 被拒正是
    // 这一格。
    (
        "cogneva_mainline_kubectl_refused_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 集群的准入面把一次滚动挡在 Pod 创建之外，按挡下它的那堵墙记一条。与上面那条
    // 互为两面：那条是部署器自己的读被拒，这条是交付出去的对象被拒——授权在、配额
    // 满了也是同一轮里的两回事。这条的唯一观测面是 Job 的 Failed 条件（Job 建得出、
    // Pod 建不出），读一次就随这一轮结束而消失；资源账那一行要有 gate 才写得下、
    // 且不被刮取，告警看不到它。2026-10-19 实测：LimitRange 那堵墙把 evolution 的
    // 滚动扣了两个半小时，`mainline_rollout_never_lands` 读到了停摆却说不清被什么扣住。
    (
        "cogneva_mainline_rollout_refused_total",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    // 部署器自己量到新 Pod 装不下、决定**先不交付**，按差在哪一维记一条。与上面那条
    // 不是一半的关系：那条是集群把已交付的 Pod 挡在创建之外（Job 建了、Pod 没建），
    // 这条是 Job 建了自己退出 75、旧 Pod 原封不动（集群谁也没拒）。两者都让主干没有新
    // rev，所以谁都不能从上一条推出来。维度取的是资源而不是配额对象名：CPU 与内存的
    // 修法不同（同一个对象的不同键），对象名又随工作负载变。2026-10-27 实测：主应用与
    // evolution 因 `limits.cpu` 的滚动求和短差被扣住一整轮，这件事当时只活在那一轮 Job
    // 的日志里，Job 一结束就没了——翻日志才知道差的是 CPU 还是内存。
    (
        "cogneva_mainline_admission_held_total",
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
    // 同一段等待的另一个量纲：起点。记的是「从哪一刻开始等」，没在等的轮写 0。
    // 用起点而不是「已经等了多久」，因为一轮才写一次的话年龄在两次采样之间是常数，
    // `for:` 短于一拍就永远凑不满；而写起点让陈旧落回 `_observed_timestamp_seconds`
    // 那条伴生腿，与这一族其它规则同一个形状。0 是一个会被读到的真实值而不是缺席：
    // 纪元那一刻的年龄不是等待，所以规则必须先问 `> 0` 再算年龄。
    (
        "cogneva_mainline_advance_wait_since_unix",
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
    // 闸门**此刻**由哪一路掌权：五格由产出进程按 `RawMetric` 渲染，随产出者消失，
    // store 里没有行（所以不进 `metric_names::ALL` 那个闭集），登记点就是
    // `memory_raw_backlog_aged_out` 的抑制子句——它读
    // `pull_gate_local|pull_gate_shared|pull_gate_both` 这三格，好让「闸门被任何一路
    // 按住」不再被那条规则读成「扫不动／漏」。名字取自
    // `crates/cog-memory/src/observable.rs` 的 `PULL_GATE_HOLDER_METRIC`，五个标签
    // 值与换手计数共用 `PULL_GATE_OPERATIONS`，是同一套拼写而不是第二份。
    (
        "cogneva_memory_pull_gate_holder",
        "crates/cog-memory/src/observable.rs",
    ),
    // 原则账的普查：四个状态格一起发，一次 pass 一写。规则读的是「这一族还在不在
    // 发」，所以登记的是产出者而不是任何一格的值——值合法地可以是 0 行。
    (
        "cogneva_principle_ledger_rows",
        "crates/cog-orchestrator/src/principle_ledger.rs",
    ),
    // The other half of the same account: how many times one principle's gate
    // stopped something, by slug. Cumulative and monotone, so no level of it is
    // a fault -- a slug refused five hundred times last year is not a fault
    // today -- and what the rule reads instead is the *shape*: the same slug
    // refused inside every 24-hour window of the last three days. The series is
    // written by the same pass that writes the census above, and named here
    // beside it for that reason: the census says whether anyone is still
    // counting, this one says what the counting found.
    (
        "cogneva_principle_refusals",
        "crates/cog-orchestrator/src/principle_ledger.rs",
    ),
    // 跨 agent 投递的两格。命令读完并 ack 与「handler 失败、留在 pending 等
    // 那次重启」在队列里都是同一条条目，而队列的 lag 读数看不见 pending，
    // 于是失败唯一留下的证据是那一行日志——它在一小时后早就滚掉了。两格都按
    // `kind`（prompt/continue/abort/reset/steering 这个闭集）分支，每次抓取
    // 把全部 kind 写一遍（含 0），好让规则的分母在第一条命令到达之前就存在。
    (
        "agent_inbox_undelivered_total",
        "crates/cog-agent/src/observable.rs",
    ),
    (
        "agent_inbox_poisoned_total",
        "crates/cog-agent/src/observable.rs",
    ),
    // 一次「问了另一 agent 并等回答」的结局。等超时不等于发送失败：命令已经
    // 发布出去了，目标可能还在跑，成功/失败两个投递计数器一个都不动——不单独
    // 记这一族，等待本身就没有任何读数。
    (
        "agent_inbox_ask_total",
        "crates/cog-agent/src/observable.rs",
    ),
    // 退役清扫只删「无主且空」的 inbox。另一半——属主已经不在注册表、条目却
    // 还在——永远不会被投递、也不会被删，而清扫的输出只报它删掉了多少：没人
    // 计过它留下的。两族都出声：只数流分不出「一条搁浅的命令」与「一万条」。
    (
        "agent_inbox_orphaned_streams",
        "crates/cog-storage/src/inbox_orphan_readings.rs",
    ),
    (
        "agent_inbox_orphaned_entries",
        "crates/cog-storage/src/inbox_orphan_readings.rs",
    ),
    // 一个已注册的告警频道为什么发不出去。读的是**配置事实**，不是投递结果：
    // `cogneva_alert_notifications_total` 只在真的投递时动，而 `no_recipients`
    // 描述的正是「永远投不出去」的那一档，它在那个计数器上根本不会出现。三格
    // （`ok`/`no_recipients`/`not_configured`）每拍全写一遍，匹配的那格为 1、
    // 其余为 0，好让「这格是 0」与「这格没有」分得开。
    (
        "cogneva_alert_channel_resolution_total",
        "crates/cog-observability/src/config.rs",
    ),
    // 一条判词写进去了没有。告警是两次写：`alerts` 那行读者看得见，判词那行说
    // 是哪个写者断言了什么。只有第二次能在第一次成功的前提下失败，而失败原先
    // 只有一行 warn——写它的 Pod 一滚就没了。两格（`written`/`failed`）每次尝试
    // 写一格为 1，好让「没人断言过」与「每个人都同意」不再同形。
    (
        "cogneva_alert_assertion_write_total",
        "crates/cog-observability/src/alert_store.rs",
    ),
];

/// Series the rules read that this workspace does not publish, with the owner.
/// Listing them keeps the check honest: an unlisted foreign series fails the
/// test rather than being silently ignored.
const FOREIGN: &[(&str, &str)] = &[
    // 这两条出自与 `build_containment_builds` 等四条同一个宿主读数
    // （`cogneva-host-build-containment-reading.sh`，在值守用的另一棵树里，以
    // cogneva 用户跑）。规则读的是**状态**而不是计数：闸外有几发、里头几发，与
    // 「此刻该不该出声」是两个数，前两个放在 description 里给人翻，规则必须有一个
    // 恒在的 0／1 才能不靠人比对两条读数。生产者不在本仓，所以本仓只能登记。
    (
        "build_containment_saturated_uncontained",
        "宿主读数 `cogneva-host-build-containment-reading.sh`（不在本仓，在值守用的另一棵树里，以 cogneva 用户跑）：宿主已经压着、而构建闸外仍有构建在跑时为 1。规则 `CognevaBuildOutsideTheGate` 读它，是为了让「压着的时候还有闸外的活在抢核」成为一条规则；几发与每一发的父链在同一个读数的 `build_containment_builds{containment=\"outside\"}` 与 `build_containment_outside_names` 里，规则侧不必再拆",
    ),
    (
        "build_containment_slice_unbounded",
        "同一生产者：构建所在的那个 slice 自称有界、`cpu.max` 却是 max 时为 1。它读那个 slice 自己的 `cpu.max`，不是包装脚本对自己账目的复述——量到「里头有构建、账上没总量」才置 1。与同族的 `build_containment_slice_builds` 分开印：后者只说里头有几发，装在真有界的 slice 里与装在无界 slice 里读数一样",
    ),
    (
        "container_memory_rss",
        "cadvisor（kubelet 内置）。判内存余量读的是**不可回收**的那部分：工作集把可回收的页缓存算进分子，在这棵树上足以把每一个大容器都推到上限（app／沙箱／进化三族的 usage 峰值都在 0.9998–1.0000 of limit，而 rss 是 0.78／0.59／0.24–0.39），所以拿工作集当分子时「顶到上限」不携带信息。工作集名字本身仍在别的面板上被读，但**没有任何告警规则读它**，按 the_tables_hold_no_series_that_no_rule_reads 不能留在本表",
    ),
    (
        "cogneva_alert_bound_band_birth_max_bytes",
        "宿主读数 `scripts/host/cogneva-alert-bound-band-reading.sh`（不在本仓，登记行 `alert_bound_band`）：每一代应用 Pod **稳定之后**（它这一代最早那 15 分钟之后，按时间跳、不按采样点个数）的 rss，取窗口里的最大值。它是「告警界脚下的那条带」的上沿——界是按「带上沿 ＋ 代内涨幅」定的，带往上漂而界不动，界就会开始把新常态报成压力。取稳定之后而不是第一次抓到的值：刚起的 Pod 第一次被采到时可能只有几 MiB，拿它当出生值会把代内涨幅读成整个进程那么大。跳过的长度按时间算而不是按点数算，是因为查询步长可调：按点数算时同一步在 300 秒步长下覆盖 15 分钟、在 60 秒步长下只有 3 分钟，同一代会被在启动途中取出生值",
    ),
    (
        "cogneva_alert_bound_band_move_max_bytes",
        "同一生产者。代内涨幅（该代最大值 − 稳定后的出生值）在窗口里的最大值，即「同一代自己会走多远」，是界必须多留的余量。出生与涨幅分开公布而不是直接公布比值：分母换一档时这两个字节不变，比值要靠重算，而重算是另一件事",
    ),
    (
        "cogneva_alert_bound_band_limit_bytes",
        "同一生产者（这一拍从 `kube_pod_container_resource_limits` 读回来的声明上限）。它跟着读数一起走而不是由规则侧另配一份：分母与分子必须来自同一次回放、同一个容器，否则滚动重叠那几分钟会把比值读错一半（同命名空间里进化 Pod 的容器同名、上限 16Gi，选择器不钉死就会取到它）",
    ),
    (
        "cogneva_debt_edge_verdicts",
        "宿主读数 `scripts/host/cogneva-debt-edge-reading.sh`（不在本仓，在值守用的另一棵树里）：依赖边账上每一类结局各有几条，`kind=\"unmeasured\"` 是钩子判不了、把提交放过去的那一路。本仓印不出它：账本与写它的提交门都住在另一棵树。规则 `CognevaDependencyEdgeHookCannotJudge` 读的就是这个格子——钩子判不了不拦（一条坏掉的钩子不该让所有人提交不了），代价是判不了的时候每一次提交仍然绿，唯一的痕迹是账上多这一行，没人去看它就不存在",
    ),
    (
        "cogneva_debt_edge_malformed",
        "同一生产者（`scripts/host/cogneva-debt-edge-reading.sh`）：账上读不出来的行数。坏行不会被丢掉，所以这个数只增不减，规则 `CognevaDependencyEdgeLedgerUnparsable` 因此拿 `> 0` 而不是窗口判它：它是持续状态，清走或教读者认那个形状才会消失，加地平线等于到点就不再提它",
    ),
    (
        "cogneva_code_debt_rows",
        "宿主读数 `scripts/host/cogneva-code-debt-reading.sh`（不在本仓，登记行 `code_debt`）：代码欠账登记表与它所判的那棵树之间，此刻有几行对不上。它在公开仓里只能是外来序列，因为**生产者与判决住在这个仓看不见的另一棵树**——`check-code-debt.py` 判的是本仓的源码布局，而它由私有仓的提交门驱动；规则读这一条，是为了让「登记表与树分家」在下一拍就被说出来，而不是等下一个在这边提交的人撞上那道门（此前三天里让三个不相干的人代付过）",
    ),
    (
        "eval_bench_cases",
        "宿主读数 `scripts/host/eval-evidence-reading.sh`（不在本仓，私有仓的登记行 `eval_evidence`）：这一臂此刻印得出来的用例数——检查点里读过几行，0 也是量出来的 0。它在本仓只能是外来序列：读的是宿主机上评测臂的检查点与运行日志，本仓没有任何进程能印出它。规则拿它当**在场证明**（`max(eval_bench_cases) > 0`），因为同一份文件按基准分的几条格是条件型的：一次都没跑出来时那些序列根本不存在，「缺席」与「这一臂干净」在读数上同形，只有这条恒在的计数能把两种情形分开。同族其余四条（`eval_bench_unattributed`、`eval_judged_cases`、`eval_judged_remaining`、`eval_pool_available`）出自同一个脚本、同一份登记行",
    ),
    (
        "eval_bench_unattributed",
        "同一生产者（`scripts/host/eval-evidence-reading.sh`，登记行 `eval_evidence`）：检查点里那些归不到任何基准的行数，不进任何基准的得分、也不进任何基准的失败。两个方向都看不见，所以它必须单独印出来与 `eval_bench_cases` 相除；归并落空通常来自基准名的写法不一致（改名、大小写、前缀），而那件事在库表里与「这一臂只跑了一个基准」同形",
    ),
    (
        "eval_judged_cases",
        "同一生产者（`scripts/host/eval-evidence-reading.sh`，登记行 `eval_evidence`）：已经判过的用例数，分母是它。面表里「商用交付」面欠的就是这条恒在的计数——它不为 0 才谈得上「判到第几例」",
    ),
    (
        "eval_judged_remaining",
        "同一生产者（`scripts/host/eval-evidence-reading.sh`，登记行 `eval_evidence`）：这一轮还欠多少例没判。与 `eval_judged_cases` 成对读：单看已判数看不出这一臂是跑完了还是刚起步",
    ),
    (
        "eval_pool_available",
        "同一生产者（`scripts/host/eval-evidence-reading.sh`，登记行 `eval_evidence`）：评测臂此刻能用的上游数，从宿主那一侧的池里读回来。它回答的是「这支臂为什么不动」里的一半——池是空的与臂是卡住的不需要同一个人去修，而两条在别的读数上都表现为「没有进展」",
    ),
    (
        "face_first_reading_consumers_unread",
        "宿主读数 `scripts/host/cogneva-face-first-reading-consumers-reading.sh`（不在本仓，私有仓的登记行 `face_first_reading_consumers`）：这一拍里「第一面（第一条读数）没有任何读者」的面数。它必须外来：三处读者面里有两处（宿主 `scripts/`、面表与登记表）住在本仓看不见的那棵树里，作者也在那边；本仓只能看见 `crates/` 与 `deploy/` 这一侧，自己印不出一个完整的计数",
    ),
    (
        "a2a_conformance_drift_cells",
        "宿主读数 `scripts/host/cogneva-a2a-conformance-reading.sh`（不在本仓，登记行 `a2a_conformance`）：这一拍里答得不是协议说的那样的格数，每拍真打十五个请求量出来。它与 `a2a_conformance_cells` 一起印，是因为没有分母的 0 证不了「比过」——生产者读不到时两条一起缺席，那种「没量」由宿主读数的生产者契约报，不在这里另立一条规则",
    ),
    (
        "a2a_tck_conformant",
        "宿主读数 `scripts/host/cogneva-a2a-tck-reading.sh`（不在本仓，登记行 `a2a_tck`）：上游官方套件（a2aproject/a2a-tck）按名片逐条跑出来的 0/1 判词，1 表示名片那一层全过。它是这一面唯一不由本仓库作者写的读数——其余 A2A 判据与实现出自同一拨人，错在同一个方向上时两边一起错，所以需要一个外面的尺子说了算。套件没跑起来时这条序列不出现，那是 `CognevaHostReadingProducerUnreadable{producer=\"a2a_tck\"}` 的事，不是「合规」",
    ),
    (
        "cogneva_alert_ownership_unowned",
        "宿主读数 `scripts/host/cogneva-alert-ownership-reading.sh`（不在本仓）：`alerts` 表里正 firing、却答不出「该谁动」的行数。它必须读表而不是读规则清单——规则的申报写在树里，而这一行只在运行时才知道响没响，两个写者（进程内求值、Prometheus 镜像）也只有表能把它们算在一起。逐条点名在 `cogneva_alert_ownership_unowned_rule{rule,reason}`，三种理由分开（`no_face`/`face_unknown`/`owner_vacant`）：补标签与派人不是同一个修法。分母 `cogneva_alert_ownership_firing`；读不到时一条序列都不印，与「表里没有无主」不同形",
    ),
    (
        "a2a_reach_defect",
        "宿主读数 `scripts/host/cogneva-a2a-discovery-reach-reading.sh`（不在本仓，登记行 `a2a_discovery_reach`）：0/1 判词，1 表示外部对端连发现这一步都走不通（名片要凭证、名片声明 jwt 而 RPC 收匿名调用方、服务不健康，或名片答了别的码）。三种坏法靠 key=value 档里的标签分开而不是各立一条序列：序列名一样、只差标签的话，「哪一条在响」会落到读的人自己拼。它盖住的是一个此前只有「想跑就手跑一下」的读数面——那种读数的判据再准，在读数面上也与「这一面没人看」同形，而网关路由或鉴权层一动，名片被挪回业务鉴权组这件事没有任何一个数会变色",
    ),
    (
        "container_memory_usage_bytes",
        "cadvisor（kubelet 内置）。cgroup 记的**整笔**占用：匿名页 + 页缓存 + slab。判「这个容器顶到上限了」读的是它，而判「顶到上限的到底是什么」要接着读 cache 那一半——两者相除才是那条规则的问题（可回收占多数时，rss 与工作集都不动）",
    ),
    (
        "container_memory_cache",
        "cadvisor（kubelet 内置）。同一笔占用里可回收的那部分。它在这里是**分子的一半而不是噪声**：cgroup 贴着上限而其中过半是页缓存时，一次 GFP_NOFS 分配回收不了同一文件系统的缓存，内核直接 OOM，而按 rss 或工作集读全都不动",
    ),
    ("kube_deployment_spec_replicas", "kube-state-metrics"),
    (
        "kube_daemonset_status_number_ready",
        "kube-state-metrics（DaemonSet 层面已就绪的副本数，取自各 Pod 的 ready 条件）。进程普查那条规则读它，是因为判「这台节点上的采集器起没起来」不能读平台自己的二进制：生成 `process_report_ingest_total` 的正是那个采集器，它没起来就没有序列，于是「没上报」和「没起来」在自己那一面上同形。DaemonSet 的就绪数是 apiserver 的账，与采集器在不在无关，两者相减才分得开",
    ),
    (
        "kube_daemonset_status_desired_number_scheduled",
        "kube-state-metrics（同一个 DaemonSet 期望铺到几台节点上）。有它就说明这个 DaemonSet 至少被调度过：`number_ready` 单独读会把「一台都没铺」与「铺了没就绪」读成同一个 0，而这两种现场一个是装错了、一个是采集中，修法相反",
    ),
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
        "container_cpu_cfs_throttled_periods_total",
        "cadvisor（kubelet 内置）。被 CFS 限流过多少个周期——容器「想要更多、被按住了」在内核里的原话。按 usage/limit 判会把「用满了给它的一份」读成「不够用」，而按限流周期判读的是真的没拿到。乘回周期长度就是墙钟时间里的份额；除它的分母（`container_cpu_cfs_periods_total`）不是墙钟而是「这个容器自己的可运行周期」，2026-10-22 实测同一批容器里 postgres 5.33 个/秒、redis 的 info-exporter 0.18 个/秒，比值跨容器不可比，所以那条分母已从本表撤下",
    ),
    (
        "kubelet_image_garbage_collected_total",
        "kubelet 自己的镜像管理器计数器。镜像库的回收动作本来就归运行时：它过了镜像 GC 高水位（默认 85%）就删没有被容器引用的镜像，降到低水位（默认 80%）为止。资源官不执行这个动作，只把它读进账——这一条是动作的回写面，`increase(...)` 就是「这一窗里真的回收了」",
    ),
    (
        "kubelet_container_log_filesystem_used_bytes",
        "kubelet 自己的 resource metrics（经 apiserver 代理取到，所以样本上的身份标签被采集端加了 `exported_` 前缀；判据读的是 `exported_namespace`，读裸 `namespace` 会指到代理目标自己的命名空间）",
    ),
    (
        "kube_pod_container_resource_requests",
        "kube-state-metrics（调度面的分子：容器声明要多少，不是它用了多少）",
    ),
    (
        "container_fs_writes_bytes_total",
        "cadvisor（kubelet 内置）。逐容器的落盘写字节数——宿主面的利用率只说盘忙，这一条说「谁在写」",
    ),
    (
        "kube_node_status_capacity",
        "kube-state-metrics（与 allocatable 同源，相减即节点为非 Pod 工作留出的份额）",
    ),
    ("kube_node_status_condition", "kube-state-metrics"),
    (
        "kube_persistentvolumeclaim_info",
        "kube-state-metrics（每个 bound 的 claim 一条常量序列，带 storageclass；它是「集群现在有哪些卷」的那份清单，所以 claim 被删时它也消失——按它当全集来判「这张卷有没有用量读数」，才不会把「卷没了」读成「量不到」）",
    ),
    (
        "kube_persistentvolumeclaim_resource_requests_storage_bytes",
        "kube-state-metrics",
    ),
    (
        "kube_persistentvolumeclaim_status_phase",
        "kube-state-metrics（claim 自己的 phase；只挑 Bound 的，因为 Pending 的 claim 还没有目录，把它算进「没量到」会让一条规则在正常扩容期间常亮）",
    ),
    (
        "kube_persistentvolume_claim_ref",
        "kube-state-metrics（每个 PV 一条，带它 claimRef 指向的 claim 的 namespace 与 name）。这是「盘上有个目录，集群还记不记得它」的另一半：claim 清单只答「那张 PVC 还在不在」，答不了「还有没有卷对象认领这堆字节」。PV 的 claimRef 不随 PVC 删除而清空，所以 claim 没了它还指着——「字节有主、主人已注销」这一格正是靠它从「连主人都没有」里分出来的，两者对磁盘的处置完全不同（一个删卷对象＋目录，一个连名字都只剩目录名）",
    ),
    (
        "kube_persistentvolume_status_phase",
        "kube-state-metrics（每个 PV 每个 phase 一条，当前 phase 为 1、其余为 0）。Released 是 Retain 策略留下的常态，Failed 是删过一次没删成：两者在 claimRef 上长得一模一样（都指着一个已经不存在的 claim），能把它们分开的只有这个字段，而两者要问的问题不同——一个是「这批字节留不留」，另一个是「为什么删不掉」",
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
    (
        "kube_pod_labels",
        "kube-state-metrics (one constant series per pod per allowlisted label key; with an empty allowlist the whole family does not exist, so this registration is only true while the key it is read through stays allowlisted -- the rule that counts running pods with no scrape target reads `cogneva.io/readings-delivery` from here to leave out workloads whose readings arrive by push, since a process census with no /metrics can only say so on its own pod object)",
    ),
    ("kube_pod_owner", "kube-state-metrics"),
    (
        "kube_pod_start_time",
        "kube-state-metrics (the pod object's creation instant, in epoch seconds; read as `time() - kube_pod_start_time` it is the pod's age. `kube_pod_status_phase` reads Running from the instant the container starts, while a scrape target appears only after endpoint discovery has run, so the two are a discovery cycle apart -- an age bound is what tells a pod that has only just started from one that has been up with no scrape face at all)",
    ),
    ("kube_pod_status_phase", "kube-state-metrics"),
    ("node_filesystem_avail_bytes", "node-exporter"),
    ("node_filesystem_size_bytes", "node-exporter"),
    (
        "node_filesystem_files_free",
        "node-exporter（inode 表的余量）。字节与 inode 是两条各自走到底的边界：这棵树上每一块会长的树都是小文件形态——镜像库单节点 809k 文件、构建缓存几十万个——所以 inode 先于字节耗尽，而上面两条磁盘规则只看字节，看不见这一格。取 free 而不是 avail：inode 表没有给 root 预留的份额，avail 在它上面与 free 逐值相同",
    ),
    (
        "node_filesystem_files",
        "node-exporter（同一序列的分母，inode 表的总数）。分子分母同源同标签，比值才是判据",
    ),
    (
        "node_memory_Dirty_bytes",
        "node-exporter（/proc/meminfo 的 Dirty：此刻还没有写回设备的页）。它与设备吞吐配对才成判据——单看它只是一个数，除以设备此刻的写速才是「这些脏页要多久才写得完」，而那个时长正是 bootstrap 的 node-io 那一步要给 vm.dirty_bytes 定的大小",
    ),
    (
        "node_disk_written_bytes_total",
        "node-exporter（/proc/diskstats 的写字节累计，按 device）。它是上面那条的分母，也是唯一能从集群里读出「这台机器的盘实际写得多快」的地方——bootstrap 要的 COGNEVA_NODE_IO_WRITE_BYTES_PER_SEC 今天没有生产者，只能由人照这条读数抄进去；过滤掉 loop/ram/sr/dm 是因为它们不是承载脏页回写的那张盘",
    ),
    (
        "kube_resourcequota",
        "kube-state-metrics（每个 ResourceQuota 一条 hard 序列、一条 used 序列，按 namespace 与 resource 成对）。它是唯一能回答「这个命名空间还装得下下一个对象吗」的读数：容器余量答的是运行中的容器离自己的上限多远，节点请求比答的是节点还排得下多少，配额到 1.0 时 API 服务器直接拒收下一个带该资源的对象，而拒收不产生事件、不产生日志，只让工作负载一直等",
    ),
    (
        "cogneva_clippy_verdict_present",
        "宿主 clippy 判词生产者 `scripts/host/cogneva-clippy-verdict-reading.sh`，由主机聚合器以 `job=\"cogneva-host-readings\"` 在宿主的 9464 端口暴露。读 0 说的是账本不在（没人跑过生产者），不是「全部通过」——这一格存在就是为了让那两件事不同形",
    ),
    (
        "cogneva_clippy_pending_revs",
        "同上（「最新一行不是 green 的 rev」有几笔）。它只有在一行合法判词存在时才发：账是空的、或每一行都写坏时这一格整格缺席，而不是 0——0 在这里是「不欠账」，正是另一件事",
    ),
    (
        "cogneva_clippy_verdict_unparsable_lines",
        "同上（账上读不出来的行数）。坏行只计数不清零，所以它与 `present` 同时可用：账在不在、以及账写坏了几行，是两个事实",
    ),
    (
        "cogneva_acceptance_present",
        "宿主验收面普查生产者，由主机聚合器以 `job=\"cogneva-host-readings\"` 在宿主的 9464 端口暴露。1 是有一份跑完的普查，0 是从来没有过；为 0 时这一族其余序列一条都不印，所以 0 与「缺席」要由两条规则分开读——缺席是生产者没了，0 是它跑成了却一份都没跑完",
    ),
    (
        "cogneva_acceptance_observed_timestamp_seconds",
        "同一生产者（最新一趟普查是什么时候量的）。它是「读数停在最后一次」唯一的落点：序列还在、值不动、退出码还是 0，只有拿它和 `time()` 相减才看得出刷新那一半死了",
    ),
    (
        "cogneva_acceptance_items",
        "同一生产者，一族按 `verdict` 标签分的计数（九种判词各一条）。只有 `stale` 意味着规格与代码不一致，其余八种是「还没轮到 / 跑不起来 / 机器判不了」——并成一条会让规格说谎与工具没跑同形",
    ),
    (
        "cogneva_acceptance_specs",
        "同一生产者（最新一趟普查覆盖了几份规格）。它与 `cogneva_acceptance_present` 分工：present 说有没有跑完的普查，它说跑完的那一份读了几份规格。0 份时判词那一族一条都不印，于是「没读到规格」与「全部通过」在面板上同形，只有这一格能把它们分开。一份都没跑完时它整格缺席而不是 0——这一格的 0 是「读完了、一份都没覆盖」，不是「没读过」，所以缺席由 present 那两条规则承担",
    ),
    (
        "host_reading_producer_up",
        "宿主读数聚合器 `scripts/host/cogneva-host-readings.sh`（不在本仓），由它按 `scripts/data/host-readings.tsv` 逐行拉起生产者，并把每拍跑成没跑成印成这条判活序列。它只有聚合器能发：单个生产者退出码为 127 时与崩溃同形，聚合器是唯一知道「这一行没脚本」的地方",
    ),
    (
        "host_reading_producer_unreadable",
        "同上（生产者跑成了，却一条序列都没发）。它与 `up` 必须分成两条：脚本崩了与输入读不到修法不同，合成一格就指不出该改哪边",
    ),
    (
        "host_reading_producer_capped",
        "同上（这一拍撞到聚合器自己的上限而没有跑完）。它与 `producer_up` 必须分成两条，而这一条**只有聚合器知道**：生产者自己不知道自己是被挤死的，它的退出码与一次普通崩溃相同。2026-10-30 实测，五十个生产者串行跑一拍要 134 秒，排在后半段的那几个起跑时机器已被同一批生产者压满，于是每拍都撞上限、落成 `up 0`——那四个读数看上去是「生产者坏了」，实际是「这一拍的预算不够」。修法相反：前者改生产者，后者改调度或上限",
    ),
    (
        "host_reading_beat_seconds",
        "同一聚合器自己量的上一拍墙钟耗时。它判的是这个通道**跟不跟得上自己声明的周期**：一拍比周期慢时，端点照常回一份完整、合法、只是更旧的表，抓取侧看不出任何异常，`up` 与每一个 `producer_up` 都是 1。没有这一格，「这条通道按三分半的节奏发数」这件事只能靠人拿秒表量",
    ),
    (
        "host_reading_beat_period_seconds",
        "同上（它这一拍声明的周期）。规则里的界是它的倍数，因为周期是配出来的：写死一个常数的话，宿主上改了调度、界不会跟着改，而失败方向是静默的——周期调大之后规则仍按旧数发火，调小之后它对真超时永远沉默。它与上一格同生同灭：端点读不到时两格一起缺席，那时由 `up` 那条规则报",
    ),
    (
        "host_reading_served_age_seconds",
        "同上（端点此刻回的那份表有多旧，按抓取时刻算，不是按拍算）。拍长那一格判不了「刷新线程卡死」：卡死之后没有任何一拍完成，`beat_seconds` 冻在最后一个健康值上，规则永远不会亮，而端点还在回一份每一格都写着正常的旧表。只有这一格会继续涨——它也是这份表里唯一一个不是由某一拍写出来的读数",
    ),
    (
        "reading_producer_absent",
        "宿主生产者 `scripts/host/cogneva-reading-producer-reading.sh`（不在本仓），由聚合器按 `scripts/data/host-readings.tsv` 逐拍拉起，经 `job=\"cogneva-host-readings\"` 印出。它判的是抓取面而不是树：`crates/cog-gateway/src/readings.rs` 里每条读数点名的指标，在 Prometheus 里至少有一条样本吗。这一格与上面两条分工相反——那两条问「宿主脚本跑没跑成」，它问「读数点名的指标有没有人发」。0 条缺席才是健康；缺席的那几条由同族的 `reading_producer_missing` 按读数 id 与指标名点名，所以这一格只给判据、不给成因",
    ),
    (
        "helm_release_revision_vs_workload_rev",
        "宿主生产者 `scripts/host/cogneva-upgrade-path-reading.sh`（不在本仓），由聚合器按 `scripts/data/host-readings.tsv` 逐拍拉起，经 `job=\"cogneva-host-readings\"` 在 :9464 印出。值说的是「helm release 与现网工作负载是不是同一版」：1 是同版且 release 为 deployed，0 是不是。0 里混着两种成因（修订号不同、或 release 本身不是 deployed），所以这一格只给判据，成因由 `release_revision`/`workload_rev`/`status` 三个标签分开读；`state=\"no-release\"` 是走 apply 投递、本来就没有 release 的集群，按构造不参与判定。读不到时它整条序列都不印而不是印 0——「这台上没有 release」「两边不同版」与「生产者死了」是三个不同的修法，合成一个空集会指错方向",
    ),
    (
        "image_wiring_yes",
        "宿主生产者 `scripts/host/cogneva-image-wiring-reading.sh`（不在本仓），由聚合器按 `scripts/data/host-readings.tsv` 逐拍拉起，经 `job=\"cogneva-host-readings\"` 印出。值说的是「现网工作负载的镜像 tag 与主线 main 头是不是同一个提交」：1 是，0 不是。这一格只给判据，成因由同族序列分开读——差几笔看 `image_wiring_behind_commits`，部署器自己以为上到哪版看 `deployer_rev` 标签。读不到时它整条序列都不印而不是印 0：「对齐了」与「没量到」处置相反，后者由 `image_wiring_reading_stopped` 那条缺席规则接住",
    ),
    (
        "image_wiring_live_commit_age_seconds",
        "同一个宿主生产者（`scripts/host/cogneva-image-wiring-reading.sh`，不在本仓），同一个 `job=\"cogneva-host-readings\"`。值说的是「现网在跑的那一版是多久以前提交的」，**起算点是提交时刻**，不是它在现网待了多久——里面同时装着流水线从提交走到上市的那段滞后。所以它判的是「流水线多久没把新东西送上线」，不是「现网多久没换」：要后者得用两次采样夹同一个 `live_rev`。拿它冒充「现网没换」会把每一条正常的构建滞后都读成故障；反过来，用「现网这一版不是主线尖端」当失败判也会永远在响（部署按小时滚、主线持续提交，两者不同步是常态）。读不到时整条序列都不印，那一半由 `image_wiring_yes` 的缺席规则接住",
    ),
    (
        "cogneva_host_credential_surface_age_seconds",
        "宿主凭证面生产者 `scripts/host/cogneva-credential-surface-state-reading.sh`（不在本仓），经聚合器以 `job=\"cogneva-host-readings\"` 暴露。它判的是归档最后一行的年龄：一个读「每拍追加一行」的归档的生产者，停更之后每一拍照样跑成、判词照样是 aligned，只有这一条会往后退",
    ),
    (
        "cogneva_host_credential_surface_measured",
        "同上（这一面从没跑成过时只发 0、不发年龄）。缺席不会告警，所以「从没量过」必须有一条自己的序列，否则它与「量过、只是旧」在规则里是同一个空集",
    ),
    (
        "cogneva_host_credential_surface_period_seconds",
        "同上（拉起这一拍的那个 timer 声明的周期，由 `systemd-analyze calendar` 读声明而不是读瞬时状态）。判「停了多久」的界是「几倍周期」：周期写死在规则里时，有人改了调度而界不会跟着改，失败方向是静默的；两个来源一个 30 分钟、一个 6 小时，一个常数套不住两个。量不出来时整条序列不印，由 absent() 那条规则报缺席",
    ),
    (
        "cogneva_host_credential_surface_aligned",
        "同上（归档最后一条判词是不是 aligned）。三态互不重叠：drift 的形状是 measured=1、aligned=0、unreadable=0，读的人不必反推",
    ),
    (
        "cogneva_host_credential_surface_unreadable",
        "同上（门没读成，什么也没判）。它与「判成不一致」修法相反——那一格去修适配器，这一格去修读取路径，所以是两条规则而不是一条",
    ),
    (
        "cogneva_mainline_behind_verdict_stalled",
        "宿主侧生产者 `scripts/host/cogneva-mainline-behind-verdict.sh`（不在本仓），由聚合器按 `scripts/data/host-readings.tsv` 逐拍拉起，经 `job=\"cogneva-host-readings\"` 在 :9464 印出，15 分钟一拍。它判的是「现网那一版还动不动」：连续两拍现网 rev 相同、且两拍都没有构建在飞才印 1；判不了时整条序列不印，由 `_judged` 那一格说清楚。这条序列是「部署器空转」唯一的判词：等槽被拒的轮数、Job 尝试数在健康的有界滞后里都会涨（tip 一直在动，每一轮都要重新排队），回答不了这个问题",
    ),
    (
        "cogneva_mainline_behind_verdict_judged",
        "同上（这一拍判没判出来：1 判了、0 判不了；判不了时 `_stalled` 与 `state` 都不印）。为什么不拿 `_stalled` 的缺席当「判不了」：缺席有两个原因，生产者根本没跑成（聚合器的 `host_reading_producer_up`／`_unreadable` 已经按 `producer` 标签在判），与生产者跑成了却给不出答案（只有一拍历史、上一拍判不了、比较的两拍之间一直有构建在飞）。前者修生产者，后者修判据的取样，修法相反，合成一条会让其中一半的消息指错地方，所以后者要一格自己的序列",
    ),
    (
        "cogneva_acceptance_age_seconds",
        "宿主侧生产者 `scripts/host/cogneva-acceptance-census-reading.sh`（不在本仓，登记行 `acceptance_census`），由聚合器按 `scripts/data/host-readings.tsv` 逐拍拉起，经 `job=\"cogneva-host-readings\"` 在 :9464 印出。年龄由生产者自己算：快照里的 `taken_at` 是同一台机上跑完那一趟普查时写的，减它的是同一台机的钟；把 `taken_at` 当序列交给 Prometheus 再减 `time()`，减的是另一台机的钟，两边时钟源不同时，「普查停了多久」会随漂移假响或假静。这一格是 `CognevaAcceptanceCensusOld` 唯一能问的问题：判词那一族只说得清这一趟判了什么，说不清这一趟是多久以前的——而一份停在三天前的 all-ok 与一份刚跑完的 all-ok 在判词上是同一句话",
    ),
    (
        "cogneva_acceptance_refresh_running_seconds",
        "同一生产者（此刻正在跑的那一趟已经跑了多少秒，没人跑时为 0）。年龄一格分不出「正在跑一趟慢的」与「刷新没跑成」：一趟合法的普查可以比年龄的界还长，那时年龄照旧在涨，而它涨的原因恰恰是普查在干正事。所以 `CognevaAcceptanceCensusOld` 有两个数：年龄超过界、且没有一趟在跑（或那一趟自己跑得太久，5400 秒＝90 分钟就不再豁免——一把卡死的锁会一直「正在跑」，豁免没有上界就等于把这条规则永久关掉）。生产这一格漏发时，`unless` 的左边不受影响，代价是多响一次，不是永久静音",
    ),
    (
        "cogneva_acceptance_landed_unjudgeable",
        "同一生产者（最新一趟普查里，状态写着「已落」、而判据跑不出判词的条数，只含 `unjudged`／`no-command`／`missing-script` 三种）。`cogneva_acceptance_items` 答不了这一问，因为它的标签只有 `verdict`、没有「这一行自己说没说已落」那一维：一条 check 写成散文却声明已落，与一条还没开工的条目在那一族里同样落在 `unjudged` 里，于是「规格在说谎」被读成「活还没干完」。反过来也不能拿 `unjudged` 整体当这一问的分母——多数 unjudged 是未落条目的正常形态，把它们算进去这条规则会天天响。三种成因（散文／没给命令／点名的脚本不存在）合成一格，因为修法都在那一条判据自己身上；`missing-binary` 与 `timeout` 不进这一问（前者归装置、后者是判据该拆）。旧快照没有这一格时序列不出现，而不是印 0——缺字段与「查过、一条都没有」不是同一个事实",
    ),
    (
        "cogneva_acceptance_stale_causes",
        "同一生产者（最新一趟普查里每一条 stale 红按「这条判据自己读的那片路径，此刻盘上干不干净」分成 own_scope_clean／own_scope_dirty／own_scope_unknown 三格，三格都发、干净时印 0）。为什么成因要单独成族：普查读的是工作树此刻的样子，三条会话同写一棵树，别人正在飞的改动会被读成「规格与代码对不上」——那不是规格在说谎，是有人还没提交。`cogneva_acceptance_items{verdict=\"stale\"}` 只有一个总数，答不出这一问，用它压这条规则就会把真矛盾一起压掉。修法在这里不指路（红在哪条判据上分别怎么改），所以这一族不标判词：它不是读数面的一条判据，是判词自己的成因拆解",
    ),
    (
        "cogneva_backup_package_owner_uid",
        "持久化存储面生产者 `scripts/host/cogneva-backup-package-owner-reading.sh`（不在本仓），经聚合器以 `job=\"cogneva-host-readings\"` 暴露。它读的是账本 `backup_runs` 里最新一行 `outcome='ok'` 点名的那个包在卷上的属主：包是恢复唯一能读的东西，属主决定「谁能读」，而备份面此前所有判据都不读它。裸 gauge，不拿包名当标签——包名一天一条，是标签最不该长的形状",
    ),
    (
        "cogneva_backup_package_age_seconds",
        "同上（从账本的 `finished_at` 算，不读文件 mtime）。写端直接写最终文件名，卷上从第一秒起就有那个 `.tar.zst`，所以 mtime 与「写完」无关；账本的完成时刻才是。界沿用持久化面既有的 26 小时，不另拍一个数",
    ),
    (
        "cogneva_backup_package_reading_state",
        "同上（`ok`/`unreadable`/`empty` 三态互不重叠）。读不到（账本或卷问不出来）与没有包（账本没有 ok 行、或账本点名的文件不在卷上）修法相反，所以各有一条规则；聚合器那条 `host_reading_producer_up` 另管「这一拍根本没跑成」",
    ),
    (
        "disk_trend_trigger_line_readable",
        "同一生产者（触发线落不落得下来：1 落下了、0 没落下）。缺 `node_filesystem_free_bytes` 时保留块量不出来，触发线就落不到刹车那把尺子上，此时 `disk_trend_days_to_trigger` 整条缺席——「读不出来」与「还有很久」在规则里是同一个空集，这一格存在的唯一目的就是把它们分开。整条生产者死掉不在射程内，那由 `host_reading_producer_up` 按 producer 名报",
    ),
    (
        "disk_trend_days_to_full_recent_margin",
        "同一生产者：只用**最近 24 小时**那一段拟合（`COGNEVA_DISK_TREND_RECENT_DAYS`）得到的「还有几天写满」，减去它这一拍公布的提前期（`COGNEVA_HOST_DISK_FULL_LEAD_DAYS`，默认 7 天，同一个值也印成 `disk_trend_full_lead_days`）。用最近那段是因为整窗斜率在加速段里滞后：2026-10-20 实测整窗把 0.7 天算成 1.7 天。提前期是策略输入，所以它不该写在规则里：规则比余量与 0，改提前期是一次公布、不是一次改规则。负数＝余量不够提前期，与磨损余量（`cogneva_host_nvme_wear_headroom_pct`）同极性。非在长的时候整条缺席而不是印 0——「没在长」与「没量出来」由 `host_reading_producer_unreadable{producer=\"host_disk_trend\"}` 分开",
    ),
    (
        "disk_trend_days_to_trigger_margin",
        "宿主生产者 `scripts/host/cogneva-host-disk-trend-reading.sh`（不在本仓）印出的「到构建缓存刹车触发线的天数」减去 `COGNEVA_HOST_DISK_TRIGGER_LEAD_DAYS`（默认 1 天，同一个值印成 `disk_trend_trigger_lead_days`）。这条线**只能在宿主上算**：刹车（`deploy/scripts/host-target-gc.sh`）判的是 `df` 的 Use%，分母是 used+avail、不含 root 保留块，而 node-exporter 的 `1 − avail/size` 分母含保留块；两把尺子差的份额随文件系统类型与 `tune2fs -m` 变，不是常数，在 PromQL 里重算就得先选一把尺子去逼近另一把。提前期是策略输入（腾地方要多久随时变），所以由生产者公布、规则只把余量与 0 比。触发线读不出来时底数整条缺席，余量也跟着缺席——「量不出」与「余量充足」仍由 `disk_trend_trigger_line_readable` 分开",
    ),
    (
        "cogneva_host_target_gc_measured",
        "宿主构建缓存回收器的读数 `scripts/host/cogneva-target-gc-reading.sh`（不在本仓），由聚合器按 `scripts/data/host-readings.tsv` 逐拍拉起，经 `job=\"cogneva-host-readings\"` 在 :9464 印出。它读的是回收器自己每拍写的状态文件（`~/.local/state/cogneva/target-gc.json`）。1 是这一拍有一份状态，0 是从没跑成过——「状态文件不在」与「读不到」由退出码 3（一条序列都不印）分开",
    ),
    (
        "cogneva_host_target_gc_age_seconds",
        "同一生产者（最后一次运行有多旧）。停产时判词会停在最后一次的值上，只有这一格继续长，所以「停了多久」的界只能按它判",
    ),
    (
        "cogneva_host_target_gc_period_seconds",
        "同一生产者（拉起这一拍的 timer 声明的周期，现网 1800 秒）。规则里的界是它的倍数，而周期由 timer 自己声明：写死一个常数的话，宿主上改了调度、界不会跟着改，失败方向是静默的。量不出时整格缺席，缺席由 `CadenceUnknown` 那条规则单独报",
    ),
    (
        "cogneva_host_target_gc_script_committed",
        "同一生产者（跑的那份脚本是不是 HEAD 里的那一份，比的是 blob 逐字节相等）。为 0 时回收器一个字节都不删——删除不可撤销，而磁盘等一次提交只要几分钟。这种运行写出的 outcome 可能是 `below_trigger`，只看结局的话与健康逐字同形，所以身份单独成一格",
    ),
    (
        "cogneva_host_target_gc_outcome",
        "同一生产者（闭集六种结局的 one-hot：below_trigger／released／floor_unreachable／script_unverified／probe_failed／planned）。三种红必须分开：够不到地板是「去看谁在吃盘」，probe_failed 是进程身份读不到（权限），script_unverified 是脚本身份立不住（提交或还原）。合成一个布尔就指不出该修哪边",
    ),
    (
        "cogneva_host_target_gc_left_alone",
        "同一生产者（这一趟因为构建占着树而没敢删的目标数，闭集六种结局之外的那个数）。它与 `outcome` 不重叠：outcome 说的是「这一趟判成了什么」，它说的是「有多少该做的没做」。大于零时高出地板的缺口不是缓存的证据——树被构建握着的时候删不了，缺口会一直摆在那儿，而面板上看不出是缓存真的没清还是这一趟被挡在树外；整格缺席＝它这次没看，不是它看见零棵树被挡（那一格在走不到删除那一步时才为 null）",
    ),
    (
        "cogneva_host_nvme_written_bytes_total",
        "宿主读数 `scripts/host/cogneva-nvme-wear-reading.sh`（不在本仓，登记行 `nvme_wear`）：控制器自己的写字节累计，按盘一条。加速那条规则拿它取六小时窗口速率，再折成 %/天。用它而不是 `percentage_used` 判加速，是因为磨损百分比只有 1% 的颗粒度——六小时里进一格就等于约 4 %/天，一次进位就能点着规则，那不是盘磨得快，是读数分辨率不够。node-exporter 的 nvme collector 不发这一格（要 ioctl 到控制器），所以别处没有第二份",
    ),
    (
        "cogneva_host_nvme_written_bytes_per_day",
        "同一生产者（这块盘一生平均的写字节／天，由累计写字节与通电小时数导出）。它是上面那条窗口速率的分母：绝对值单看没有意义，「比它自己一生平均快一倍」才是事件，而翻倍在多写一兆上也会成立——所以规则里还配一条「折成磨损要过 0.1 %/天」的地板。这一格缺席时那条规则整条不响，而不是拿 0 当分母",
    ),
    (
        "cogneva_host_nvme_media_errors_total",
        "同一生产者（SMART 记到的介质错误数，只会涨）。与磨损那一格分开：介质错误是盘已经在坏，处置是立刻换盘，与它磨到多少无关——一块用了 12% 的盘也可能已经在掉块",
    ),
    (
        "cogneva_host_nvme_critical_warning",
        "同一生产者（SMART 的 critical warning 位图）。与上一格同一条规则读，但它是控制器自己举手而不是累计计数：位图非零时健康状态已经被控制器判定为异常，不需要等介质错误累计出来",
    ),
    (
        "cogneva_host_nvme_devices",
        "同一生产者（这一拍从 `nvme list` 里发现的盘数）。覆盖格，与下一条一起构成「有没有盘没读到」：单看每一块盘的序列，一块盘读不出来与一块健康的盘在指标面上同形（都是没有序列）",
    ),
    (
        "cogneva_host_nvme_wear_headroom_pct",
        "同一生产者（磨损余量 = 界 − 已用，负数即越界；界是 `100 − 提前期天数 × 日磨损速率`，夹在 [1,99]）。界不是一个写死的百分比：0.05 %/天的盘在 80% 上还剩 400 天，0.8 %/天的盘在 80% 上只剩 25 天，同一个 80 对后者已经太晚。发余量而不是界本身，是因为余量一个比较就能判——界与已用做 `on(device)` 连接时标签集一旦对不上就是静默空结果，而清单里的连接形状没有门在读。`source` 标签说清界是从哪来的（derived 导出／default 刚上机的退路／override 人钉的），退路不许装作导出的",
    ),
    (
        "cogneva_host_nvme_wear_rate_pct_per_day",
        "同一生产者（这块盘自己的日磨损速率，由累计磨损与通电小时数导出）。它是「还剩多少天」的分母，也是「磨得突然变快」的基线：窗口速率压过它两倍才算加速，所以这一格必须有，只留累计值的话速率翻倍在读数上完全看不见",
    ),
    (
        "cogneva_host_nvme_replacements_total",
        "同一生产者（这块位次上累计换过几块盘，按序列号数，第一次见不计，基线存在宿主侧状态文件里）。换盘在磨损读数上是看不见的：换完 percentage_used 从 80 掉回 2，与「盘自己好了」同形。基线读不到或写不了时这一格**缺席**而不是印 0——「没读到基线」与「没换过」是两件事",
    ),
    (
        "cogneva_host_nvme_lead_days",
        "同一生产者（导出界那个公式里唯一的政策输入，默认 90 天）。界是多少可以从界本身读出来，但「凭什么这么早」只有它说得清；钉得太短等于等告警响了再采购，所以它自己有一条守卫规则",
    ),
    (
        "cogneva_host_nvme_readable",
        "同一生产者（其中 SMART 读成功的盘数）。`devices - readable > 0` 是「有盘没人看着它的磨损」；**全部**读不到时生产者退 3、一条序列都不印，那一档归 `CognevaHostReadingProducerUnreadable`（producer=\"nvme_wear\"），这一条不兼任",
    ),
    (
        "cogneva_file_size_census_unjudged",
        "宿主读数 `scripts/host/cogneva-file-size-census-reading.sh`（不在本仓）：公开仓 `crates/` 下 50 KiB 以上的 `.rs` 里，普查表没判过的有几个。它读的是 `scripts/gates/check-file-size-census.py --reading` 的同一次判决——普查表在私有仓、被判的树在公开仓，所以这道判据不能挂在任何一方的提交门上（挂在私有仓就会红在不持有那些字节的人身上，挂在公开仓则读不到表），它的位置是读数面。越线却没判要当天补一行，与反方向那格分开",
    ),
    (
        "cogneva_file_size_census_stale",
        "同一生产者（普查表里有行、那棵树里已经没有这个文件或它掉到线下）。与上一格是相反方向：那一格是树跑在表前面，这一格是表跑在树前面，修法一个补行一个撤行，合成一个数就读不出该做哪件",
    ),
    (
        "resource_six_column_state",
        "宿主读数 `scripts/host/cogneva-resource-six-column-reading.sh`（不在本仓）的逐列判词，one-hot 带 `column` 与 `state` 标签：rss／redis_mem／conn／oomkill／restarts／slot。六格里有四格的界归别的规则（`container_memory_near_limit` 的 0.81、`redis_memory_near_limit` 的 0.90、`pod_oom_killed`、`pod_restarting_*`），本仓库只有 conn（撞 maxclients）与 slot（占用越过窗口内提供过的最高界）两格此前没有任何读者，所以规则只读这两格。每一列都是**窗口极值**算出来的判词，窗口长度由同一生产者发的 `resource_six_column_window_seconds` 声明",
    ),
    (
        "host_reading_series_undeclared",
        "宿主聚合器 `scripts/host/cogneva-host-readings.sh`（不在本仓）自己发的一条：端点上有、而登记表那一行的第四列没登记多少条序列。账本只有一本（`scripts/data/host-readings.tsv`），这格就是它漏人的方向——印了却没登记。修法是把名字补进那一行（零成本），所以它没有「承认的欠账」：与下面那条方向相反，那一格是登记了却没人读",
    ),
    (
        "cogneva_verdict_series_unread",
        "宿主读数 `scripts/host/cogneva-verdict-readers-reading.sh`（不在本仓）：登记表里标了 `=verdict`、却在 `deploy/` 下任何一个规则文件里都找不到名字的条数。它与「这一面压根没写生产者」在读数面上同形（都是一条决定也不产生）。判红的是「涨过表头声明的 `unread_ceiling`」（下面那条），不是「大于 0」：今天就有十几条判词没有读者，写成 `> 0` 会让整条告警从第一天起常红",
    ),
    (
        "cogneva_verdict_series_ceiling",
        "同一生产者：登记表表头里写下的、已经承认的欠账条数。它是这条判据的**另一半**，不是一个可调的界——只许跟着实测值往下走（补上一条读者就减一）。做成第二条序列而不是写进规则表达式，是因为它属于账本，而账本不在本仓；规则里写死一个数，账本一改两边就各说各话",
    ),
    (
        "reading_witness_unresolved",
        "宿主读数 `scripts/host/cogneva-reading-witness-reading.sh`（不在本仓）：面表里声明要读、按它写的名字却在端点上取不到值的面数。它管的是「面表与生产者对不上」，不是「那一面坏了」——后者有那一面自己的判词。逐面定位读同一生产者发的 `reading_witness_face_resolved{face}=0`",
    ),
    (
        "tasks_publishers_agree",
        "宿主读数 `scripts/host/cogneva-tasks-face-reading.sh`（不在本仓）：同一批任务的两个发布者是否报出同一份状态，1 一致 / 0 不一致。判据不需要阈值——同一份正本的两个投影本来就该一致，不一致本身就是缺陷",
    ),
    (
        "cogneva_refusal_redispatch_state",
        "宿主读数 `scripts/host/cogneva-refusal-redispatch-reading.sh`（不在本仓）的逐拍判词，one-hot 带 `state` 标签。规则只读三个红态：`refused_no_redispatch`（有拒绝、免费重投为 0，等于白挡一次）、`below_bar`（重投通道在跑却交不回 1/3）、`turn_off`（按预登记判据该关掉这条路）。`not_wired` / `no_refusals` / `undecided` / `keep` 是绿态，不进规则",
    ),
    (
        "principle_carriers_slug_no_landing",
        "宿主读数 `scripts/host/cogneva-principle-census-reading.sh`（不在本仓）：载体在树上声明了「已落」，账本里却查不到对应落地行的 slug 条数。载体与账两处都不在公开仓，所以这一格只能作为外来读数登记",
    ),
    (
        "e2e_ten_rings_failed_settled",
        "宿主读数 `scripts/host/cogneva-e2e-rings-reading.sh`（不在本仓）：十环里有判据而不 ok、且不是构建或部署还在路上的环数（第 3、4、6 环不计）。规则不读同族的 `e2e_ten_rings_failed`，因为那个数在每次落地后的几分钟内必然大于 0——新版本还没构建出镜像、部署器还没滚到主线——给它立规则会从第一天常红，而「六小时什么都没落地」这条界另有一条规则持有。单环序列 `e2e_ten_rings{ring=…}` 同样刻意不进规则：按脚本 HELP，那一格的 0 同时表示「判了没过」与「这一拍读不到」，一条判据读一个二义值等于把两种修法混成一种；读不到与缺判据各有自己的计数序列",
    ),
    (
        "e2e_ten_rings_unread",
        "同一生产者：有判据但这一拍读不到输入的环数。与 `_failed` 分开立规则，因为修法不同——前者修链上那一段，这一条修那一环读的东西（构建还没出 tag、集群读不到、计数器文件没生成）",
    ),
    (
        "e2e_ten_rings_unchecked",
        "同一生产者：压根没有可执行判据的环数。它不是 0 也不是缺席，是这一环被跳过，而跳过在文本报告里长得像 ok——十环少一环有判据，整链的结论就少一段依据",
    ),
    (
        "cogneva_alert_mirror_unapplied",
        "宿主读数 `scripts/host/cogneva-alert-mirror-reading.sh`（不在本仓，登记行 `alert_mirror`）：树上给规则打了镜像标记、而集群里的 PrometheusRule 对象里少了其中几条。按**名字**比，不按条数比：条数相等而名字换了时条数比法恰好判绿，而集群里多留一条树已删掉的规则又会把条数比法误报成「没落地」。它读的是「清单 → 集群」这一段，与下一条的「配置 → 进程」是链路的两处断点，修法不同（重跑 apply / 重新部署），所以分成两格而不是一个「镜像坏了」",
    ),
    (
        "cogneva_alert_mirror_unconfigured",
        "同一生产者：集群里有带标记的规则、而跑着的配置里没有 `observability.infra_watch.mirror_alerts`。这是「落地≠生效」——清单换了、进程启动时读到的还是旧配置，那些标记只是注释。判它要读**活配置**（ConfigMap），不是读仓库里那份：读仓库只能证明有人写了，证明不了跑着的那份知道",
    ),
    (
        "cogneva_alert_mirror_missing",
        "同一生产者：Prometheus 里正 firing 的带标记规则，`alerts` 表里没有开着的行。按**规则名**比而不是按 `dedup_key`：后者是身份标签的序列化，在宿主上重算它就是把写面的那条规则抄第二遍，两份实现总有一份先漂；按名字比看不见「同一规则只抄了一部分实例」，那一段由同族的 `prometheus_firing` 与 `rows_open` 并排印出来、由人读",
    ),
    (
        "cogneva_alert_mirror_stale_rows",
        "同一生产者：表里开着、`last_seen_at` 超过十分钟没往前走的镜像行数。它与上一条是相反的两个方向——上一条是「从来没进去」，这一条是「曾经通过、后来停了」；读失败时镜像故意不 resolve，那一档由 `infra_watch_eval_failure` 报，所以这一条的点名序列要跟它一起读，否则会把设计读成故障",
    ),
    (
        "cogneva_alert_mirror_unmirrored_firing",
        "同一生产者：Prometheus 里正 firing、而镜像的选择器从来看不见的规则数（扣掉本进程自己的规则、`mirror_alerts.exclude` 点名的、以及已经有开着行的）。它与 `missing` 是两件事：`missing` 是「该抄的没抄到」，这里是「连位置都没有」——上游规则（kube-prometheus 的 `KubeJobFailed`、`CPUThrottlingHigh`）不带镜像标记，前四格全绿时账本照样不是全集。判据自己会变绿：有开着的行就不算，所以放宽选择器之后不需要有人回来改这一格",
    ),
    (
        "cogneva_alert_mirror_unmirrored_measured",
        "同一生产者：这一拍读不读得到「全部 firing」那一面。它自成一格而不是让整份读数退化成读不到，是因为另外四格仍然是有效读数，丢掉它们换一个未知更亏；但读不到的那一拍 `unmirrored_firing` 会印 0，而那个 0 是**没读到**、不是**没有**——不单独立一条判据，读不到就会被读成「账本完整」",
    ),
    (
        "cogneva_alert_mirror_views",
        "同一生产者：写者（进程里的镜像）与这一面读数对「谁在响」说的不是同一批告警的条数。镜像按 `/api/v1/alerts` 关行，读数按 PromQL `ALERTS{alertstate=\"firing\"}` 判缺口——两个视图来自同一台 Prometheus 却不是同一份数据（前者是规则管理器当前持有的活动告警，后者是求值写下的样本、带 5 分钟陈旧窗）。2026-10-26 实测：`CognevaAlertAccountNoisy` 在 `ALERTS` 里烧到 01:51，表里那一行 21:48 就关掉了，而 `missing` 整段读 0——判据落在写者不看的那个视图上，缺口就整段隐形。这一格让「缺口为 0」在分歧不为 0 时不再是证据",
    ),
    (
        "cogneva_alert_mirror_views_measured",
        "同一生产者：这一拍读不读得到写者自己读的那个视图（`/api/v1/alerts`）。与同族的 `unmirrored_measured` 同一处置：读不到时 `views` 印 0，而那个 0 是**没比过**、不是**一致**——不单独立一条判据，「写者视图读不到」会被读成「两个视图对得上」。镜像没配时它也是 0（没有写者就没有可比的那一面），那一档由 `cogneva_alert_mirror_configured==0` 与 `CognevaAlertMirrorUnconfigured` 说",
    ),
    (
        "cogneva_alert_mirror_configured",
        "同一生产者：跑着的配置里有没有 `observability.infra_watch.mirror_alerts`（1 有 / 0 没有）。它原本只作为读数印出来、没有任何规则读它；`CognevaAlertMirrorViewsUnreadable` 需要它把「读不到写者视图」与「压根没有写者」分开——没有写者时两个视图无从比较，那一档是 `CognevaAlertMirrorUnconfigured` 的判词，不是这一条",
    ),
    (
        "cogneva_alert_attention_unattended",
        "宿主读数 `scripts/host/cogneva-alert-attention-reading.sh`（不在本仓，登记行 `alert_attention`）：`alerts` 表里正 firing、响了超过窗口（默认 1800 秒）、而**人和机器都没接**的行。它是这条链的**读者**那一端——镜像那一族只判到「进了表」，进了之后有没有人接，此前没有任何序列在说；2026-10-22 主线停摆七十分钟，红与停都进了表、都被铃铛写过通知，而全程没有一条被确认。「接了」有两个机器可读的写者：人的 `acknowledged_at`，与机器的任务——`signal_watcher` 按告警的 `dedup_key` 开出 `self-signal-alert-*`，任务入参里带着这个键，所以 `cog_dag_tasks` 里查得到；只看人那一格，系统恢复自愈之后判词会在「每条告警都被机器接走」的健康态上永远亮着，而永远亮的判词与没有判词同价（2026-10-23 实测：现网 10 条 firing 里已有 1 条被机器接走）。两个写作点分开印成 `_acknowledged` 与 `_filed`，判词取并集。`notified_at` 不能当判据：它只说明站内铃铛写过通知（没人点开也算），按它判会把「告诉了」读成「看了」。按窗口而不是按比例判，是因为十件事接了九件时，剩下那一件仍然是没人接的那一件。判词**不把判它自己那条规则算进输入**（`CognevaAlertUnattended` 自己也是一条 firing、没人接、过了窗口的行）：算进去它就在为自己点火，一旦亮起再也回不到绿；它自己那几行另有 `cogneva_alert_attention_meta_unacknowledged` 单独数",
    ),
    (
        "cogneva_alert_sighting_unsighted",
        "宿主读数 `scripts/host/cogneva-alert-sighting-reading.sh`（不在本仓，登记行 `alert_sighting`）：`alerts` 表里正 firing、而**超过窗口（默认 900 秒）没有任何写者报告过条件仍然成立**的行。它判的是另外两条读数都看不见的那一档：`alert_mirror` 问「该进表的进没进」、`CognevaAlertUnattended` 问「进了表有没有人接」，两条都以「这一行说的是真的」为前提，而孤儿行在它们眼里是一个完全正常的、进行中的告警（计入 firing、计入未接、甚至成为最老的那一条）。唯一的机器可读特征是 `last_seen_at`：写者每次看见条件仍然成立都会推进它，所以「开着 + 很久没动 = 没有写者在看它」。2026-10-23 实测就是这一格找到的：`pod_restarting_repeatedly`（Pod 早被删）与 `dag_task_stalled_after_ready` 的 `last_seen_at` 一起冻在 22:59:48——同一次数据库慢语句窗口里两次关行写失败、键照删，于是再没有东西会来关它们，而这两行实际是靠人逐条对 Prometheus 才发现的。按窗口而不是「一冻就红」判：写者有自己的节拍，部署一次会让它们停几分钟；没有 `last_seen_at` 的行按最老算，它是从来没被看见过的那一档，恰恰最该被点名",
    ),
    (
        "cogneva_alert_traffic_noisy",
        "宿主读数 `scripts/host/cogneva-alert-traffic-reading.sh`（不在本仓，登记行 `alert_traffic`）：有没有哪条规则在一小时里写进 `alerts` 表的行数、或其中一分钟内就 resolve 的短命行数，越过了界。同族的 `alert_mirror` 判「该进表的进没进」、`alert_attention` 判「进了表有没有人看」，两条都是**逐行**的读数；这一格是**流量**——2026-10-22 同一天里两条规则各把一件事写成 82 行与 17 行，系统自己不报，是有人去数表才发现的。两个数分开数是因为它们指向两种修法：行数多是同一件事反复重开（该合并 dedup_key 或抬保持时长），短命多是一拍抖动被写成一行（该给 `for_seconds`）",
    ),
    (
        "commit_fast_lane_seconds",
        "宿主读数 `scripts/host/cogneva-commit-lane-reading.sh`（不在本仓，登记行 `commit_lane`）：走完提交门的那一笔从入队到落地的中位数，只数只动记忆、规格与文档的提交。它在宿主上量，因为门槛要的那一段——排队、判树、投影重算——一半在钩子里、一半在宿主进程里，本仓的构建产物里没有任何一刻同时看得见两头",
    ),
    (
        "product_commit_share",
        "宿主读数 `scripts/host/cogneva-product-commit-share-reading.sh`（不在本仓，登记行 `product_commit_share`）：近 7 天非 merge 提交里推进了 `crates/`（读数与观测那一格以外）的占比。分母与分子由同一个生产者同一拍发出来，规则侧不再另配一份——两份分母一旦不同步，「占比低」与「分母算错」就分不开了",
    ),
    (
        "brain_budget_remaining",
        "宿主读数 `scripts/host/cogneva-brain-budget-reading.sh`（不在本仓，登记行 `brain_budget`）：这一周还能新建几篇记忆，等于产品提交数减去新增记忆文件数。它是事前读数：余量为负时那一轮不新建记忆文件，所以规则要它响在**下一次**动笔之前，而不是等一周结束去数余量",
    ),
    (
        "face_symmetry_unknown",
        "宿主读数 `scripts/host/cogneva-face-symmetry-reading.sh`（不在本仓，登记行 `face_symmetry`）：本仓规则申报的面名里、正本不认的有几个。正本与载体分住两个仓，两侧的静态门都只看自己那棵树，于是改名只有等某条规则真的越界、读数面报出 `face_unknown` 才看得见。这一格把它提前到没有任何规则响的时候",
    ),
    (
        "face_symmetry_uncovered_excess",
        "同一生产者：正本里有、本仓却没有任何规则挂那个名字的面，超出已承认欠账（正本表头的 `# uncovered_ceiling=N`）的部分。欠账本身不是新闻——今天就有面还没有规则；涨过它才是。与上面那一格分开，是因为两个方向的修法不同：多出来的名字改载体，欠账涨了是去写第一条规则或者重新划这个面的责任",
    ),
    (
        "face_symmetry_invalid",
        "同一生产者：正本里读不出来的行数（列数不够、种类写成 face/cross 以外、同名声明两次）。有坏行的那一拍，上面两格不印——残缺的分母会让「少了一个面」和「少了一行」看起来是同一件事，而这两种情况的修法相反",
    ),
    ("up", "Prometheus 采集目标存活序列"),
    (
        "host_reading_series_vanished",
        "宿主读数聚合器 `scripts/host/cogneva-host-readings.sh` 自己发的四条之一（不在登记表任何一行里，表头写明不许替它加行）：登记表承诺每拍会印、生产者跑成了（退 0/1）却少印的序列数。标了 `=error` 的缺席是设计，不进这个数；退 3 的拍也不算——「读不到」已经有 `host_reading_producer_unreadable`，同一件事不该红两次。它红的意思只有一个：某一行账本与它的生产者分家了，修法在那一行或那个脚本里",
    ),
    (
        "must_green_flake_repeat_verdict",
        "宿主读数 `scripts/principles/must-green-flake-reading.sh`（不在本仓，登记行 `must_green_flake`）：0/1 判词，1 表示**同一个**驱动套件在窗口（默认 7 天）里留下的假红记录达到界（默认 3 次）。假红＝第一次红、同一份字节再跑转绿——真正复现的红落在 `red/`、不进这一堆，两群混起来数会把回归算成噪声。降到套件粒度是因为修法就在那一层（去查那一层里哪一份不稳），而代价落在所有排队提交者身上：每一条记录都让当时排队的人各付一整轮门。界与窗口只写在生产者里，这条规则读判词、不另立一套数；读不到目录时生产者一条序列都不印，那是 `CognevaHostReadingProducerUnreadable{producer=\"must_green_flake\"}` 的事，不是「这一周没有假红」",
    ),
];

/// A registered name that no rule reads yet, and what will judge it meanwhile.
///
/// The two censuses guard opposite directions -- a rule may not read a name the
/// tables do not carry, and a name the tables carry may not go unread -- so a
/// rule and its registration could only ever land in one commit, and only when
/// the shared worktree happened to be free. This band is that middle state made
/// countable: a name may sit here with no reader, and it must leave the moment
/// a rule reads it.
///
/// Each entry declares its own exit, and the checks hold the marker to what the
/// repository can see rather than to a list of who is allowed to enter. The
/// registry half (a `Witness` name really is in the host-readings table, a
/// non-`Witness` name really is not) lives where the table lives, because the
/// public repository's CI cannot see it.
enum PendingExit {
    /// The host-readings registry names it, so the witness reading that walks
    /// that registry -- producer on one side, consumer on the other -- is what
    /// says whether it is really published.
    Witness,
    /// Not in the registry, but this build publishes the name: the closed set
    /// is the witness, and the entry says why no rule reads it yet.
    Unregistered(&'static str),
    /// Neither: no registered reader and no producer in this build can judge it,
    /// so the entry has to say what it is waiting for.
    NoReading(&'static str),
}

/// Names registered with no rule reading them yet, each with the exit it
/// declares. Empty today: this commit adds the shape, not an entry.
const FOREIGN_PENDING: &[(&str, PendingExit)] = &[];

/// How many names are waiting. Raising this number is how a round admits it owes
/// one more reader; it can only fall by landing one.
const FOREIGN_PENDING_AT_CENSUS: usize = 0;

/// The ways the pending band can lie, as a pure predicate so an empty band does
/// not make its criteria vacuous.
///
/// `publishes` is passed in rather than looked up so the fixtures can exercise
/// both sides of `Unregistered` without a closed-set name that happens to be
/// unread.
fn pending_complaints(
    pending: &[(&str, PendingExit)],
    registered: &[&str],
    read: &BTreeSet<String>,
    publishes: &dyn Fn(&str) -> bool,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for (name, exit) in pending {
        if !seen.insert(*name) {
            out.push(format!("{name} 在 FOREIGN_PENDING 里出现了两次"));
        }
        if registered.contains(name) {
            out.push(format!(
                "{name} 同时在 PRODUCED/FOREIGN 里：一个名字只能有一个出口"
            ));
        }
        if read.contains(*name) {
            out.push(format!(
                "{name} 已经有规则在读：读到的名字必须升档进 PRODUCED/FOREIGN，不能停在 pending"
            ));
        }
        let reason = match exit {
            PendingExit::Witness => None,
            PendingExit::Unregistered(reason) | PendingExit::NoReading(reason) => Some(*reason),
        };
        if let Some(reason) = reason {
            // 数非空白字符，不数空白分词：这一档的理由与判据都写中文，而中文一句
            // 话按空白切就是 1 个词——用分词数会把每一条正当的中文理由都判成占位符，
            // 于是门只会被绕开（改写成英文短句）而不会让人把话说清。
            if reason.chars().filter(|c| !c.is_whitespace()).count() < 6 {
                out.push(format!(
                    "{name} 没写清它为什么停在这里、在等什么：这一档的判据就是那句理由"
                ));
            }
        }
        match exit {
            PendingExit::Unregistered(_) if !publishes(name) => out.push(format!(
                "{name} 标了 Unregistered，可本仓闭集里没有它——它不是本仓发布的，改标 NoReading 并写理由，或改标 Witness"
            )),
            PendingExit::NoReading(_) if publishes(name) => out.push(format!(
                "{name} 标了 NoReading，可本仓闭集在发布它——自报的出口与本仓的读法相反，改标 Unregistered"
            )),
            _ => {}
        }
    }
    out
}

/// The deploy config the rules live in, parsed. Read through one function so a
/// gate that needs a value other than the rules reads the same document the
/// rules came from.
fn chart_json() -> serde_json::Value {
    serde_json::from_str(&read_repo(CHART_CONFIG)).expect("chart config is not valid JSON")
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

/// Every rule carried by a `PrometheusRule` manifest here, name to expression,
/// in file order.
///
/// The chart is not the only place this repo ships rules: `deploy/k3s/` carries
/// its own `PrometheusRule` objects, and the cluster applies those directly. A
/// rule there that reads a series nothing publishes never fires, and a gate
/// that only reads the chart says nothing about it.
///
/// Recording rules are in reach too: "the series this reads does not exist" is
/// the same defect whether the rule alerts on it or records it.
fn manifest_rules() -> Vec<(String, String)> {
    service_topology::prometheus_rules(&repo_root())
}

/// Every rule this repo ships: the ones the chart renders plus the ones a
/// manifest applies directly. A check whose docstring says "this repo ships"
/// reads this, not the chart alone.
fn shipped_rules() -> Vec<(String, String)> {
    let mut rules = chart_rules();
    rules.extend(manifest_rules());
    rules
}

/// Every alert in a `PrometheusRule` manifest here, as (file, alert, labels).
fn manifest_alerts() -> Vec<(String, String, BTreeMap<String, String>)> {
    let mut alerts = Vec::new();
    for (file, doc) in service_topology::observability_documents(&repo_root()) {
        if doc["kind"].as_str() != Some("PrometheusRule") {
            continue;
        }
        for group in doc["spec"]["groups"].as_sequence().into_iter().flatten() {
            for rule in group["rules"].as_sequence().into_iter().flatten() {
                let Some(alert) = rule["alert"].as_str() else {
                    continue;
                };
                let labels = rule["labels"]
                    .as_mapping()
                    .into_iter()
                    .flatten()
                    .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str()?.to_string())))
                    .collect();
                alerts.push((file.clone(), alert.to_string(), labels));
            }
        }
    }
    alerts
}

/// A rule the platform does not book is a rule that fires into nobody.
///
/// Two libraries ship alerts here. The chart's rules are evaluated inside the
/// application, which writes them into the `alerts` table itself. The
/// `PrometheusRule` manifests are evaluated by Prometheus, and the small
/// profile runs no Alertmanager, so their firing exists only on Prometheus's
/// `ALERTS` face — nobody is told and no row appears. The watcher closes that
/// gap by copying the alerts that carry the mirror label out of Prometheus into
/// the same table, which makes two properties load-bearing and neither is
/// visible from either file alone:
///
///   * every manifest alert carries the label pair the config names — a rule
///     that loses it silently stops being delivered;
///   * no manifest alert shares a name with a rule the chart evaluates — that
///     alert would be booked twice, once by each writer, under keys that differ
///     because each writer reads a different response shape.
#[test]
fn every_manifest_alert_is_marked_for_the_mirror() {
    let value = chart_json();
    let mirror = &value["observability"]["infra_watch"]["mirror_alerts"];
    let label = mirror["label"]
        .as_str()
        .filter(|l| !l.is_empty())
        .expect("observability.infra_watch.mirror_alerts.label is missing");
    let wanted = mirror["value"]
        .as_str()
        .filter(|v| !v.is_empty())
        .expect("observability.infra_watch.mirror_alerts.value is missing");

    let chart_rule_names: BTreeSet<String> = chart_rules().into_iter().map(|(n, _)| n).collect();
    let alerts = manifest_alerts();
    assert!(
        !alerts.is_empty(),
        "no alert found in {}, so this gate checked nothing",
        service_topology::OBSERVABILITY_MANIFESTS
    );
    let complaints = mirror_complaints(&alerts, label, wanted, &chart_rule_names);

    assert!(
        complaints.is_empty(),
        "alerts that fire into nobody, or into two rows at once:\n{}",
        complaints.join("\n")
    );
}

fn mirror_complaints(
    alerts: &[(String, String, BTreeMap<String, String>)],
    label: &str,
    wanted: &str,
    chart_rule_names: &BTreeSet<String>,
) -> Vec<String> {
    let mut complaints = Vec::new();
    for (file, alert, labels) in alerts {
        if labels.get(label).map(String::as_str) != Some(wanted) {
            complaints.push(format!(
                "{file}: {alert} does not carry {label}=\"{wanted}\", so nothing books its firing"
            ));
        }
        if chart_rule_names.contains(alert) {
            complaints.push(format!(
                "{file}: {alert} is also evaluated by the chart, so both writers would book it"
            ));
        }
    }
    complaints
}

/// The gate above is aimed at both faults it means to catch: an unmarked alert
/// and a doubly-booked one. Without this, a predicate that had drifted off
/// either name would report nothing and look like a fully delivered fleet.
#[test]
fn the_mirror_gate_reports_both_the_unmarked_and_the_doubly_booked_alert() {
    let marked = BTreeMap::from([("cogneva_io_managed".to_string(), "true".to_string())]);
    let unmarked = BTreeMap::from([("severity".to_string(), "warning".to_string())]);
    let chart: BTreeSet<String> = ["node_disk_usage_high".to_string()].into_iter().collect();
    let alerts = vec![
        ("15-x.yaml".to_string(), "Missing".to_string(), unmarked),
        (
            "15-x.yaml".to_string(),
            "node_disk_usage_high".to_string(),
            marked.clone(),
        ),
        ("15-x.yaml".to_string(), "Fine".to_string(), marked),
    ];
    let complaints = mirror_complaints(&alerts, "cogneva_io_managed", "true", &chart);
    assert_eq!(complaints.len(), 2, "{complaints:#?}");
    assert!(complaints.iter().any(|c| c.contains("Missing")));
    assert!(complaints.iter().any(|c| c.contains("both writers")));
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
    for (rule, promql) in shipped_rules() {
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
    for (rule, promql) in shipped_rules() {
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

/// Registering `kube_pod_labels` as a foreign family says the family is
/// somebody else's to produce; it does not say the key a rule reads through it
/// is actually exported. With an empty allowlist the entire family is absent,
/// and the rule that reads `cogneva.io/readings-delivery` from it does not go
/// red -- it evaluates to nothing on that leg, so the exclusion silently
/// disappears and every push-only pod is counted as unscraped at each
/// evaluation again. Both profiles are checked because either one can be the
/// applied profile, and a key dropped from the profile that is not deployed
/// today is the same defect waiting for a smaller node.
///
/// Read off the flag itself, not off the file as text: the prose above the flag
/// names the key too, and a search for the bare string goes green after the key
/// is dropped from the list -- measured, by removing the key and watching a
/// `contains` assertion pass on the comment that explained it.
#[test]
fn the_pod_label_an_alert_rule_reads_through_is_allowlisted_in_every_profile() {
    const KEY: &str = "cogneva.io/readings-delivery";
    for (profile, values) in [
        (
            "default",
            read_repo("deploy/k3s/observability/helm/kube-prometheus-stack-values.yaml"),
        ),
        (
            "small",
            read_repo("deploy/k3s/observability/helm/kube-prometheus-stack-values-small.yaml"),
        ),
    ] {
        let allowed = values
            .lines()
            .filter_map(|line| line.trim().strip_prefix("- --metric-labels-allowlist="))
            .any(|flag| {
                flag.strip_prefix("pods=[")
                    .and_then(|rest| rest.strip_suffix(']'))
                    .is_some_and(|keys| {
                        keys.split(',').any(|key| {
                            let key = key.trim();
                            // `*` exports every key, so it answers the same
                            // question -- it is a worse answer for cardinality,
                            // not a wrong one for reachability.
                            key == KEY || key == "*"
                        })
                    })
            });
        assert!(
            allowed,
            "{profile}: kube-prometheus-stack 的 --metric-labels-allowlist 里没有 {KEY}，\
             读这个标签的那条规则就永远看不到它：`kube_pod_labels` 整族不产出，排除腿静默消失，\
             没有 /metrics 的工作负载会被一直算成没被抓"
        );
    }
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
    for (rule, promql) in shipped_rules() {
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

/// The series a rule reads through a range selector whose subject the cluster
/// can delete.
///
/// A range selector matches every sample inside its window, and the samples of
/// a series whose subject has been deleted stay in that window. A rule that
/// counts over a window therefore keeps reporting on a pod -- or, one level up,
/// on a node -- that no longer exists for as long as the window lasts, labelled
/// with the dead subject's name, so the row it opens outlives its subject.
fn range_subjects_where(promql: &str, wanted: impl Fn(&str) -> bool) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut index = 0usize;
    let mut in_string = false;
    while index < promql.len() {
        let ch = promql[index..].chars().next().expect("in range");
        if ch == '"' {
            in_string = !in_string;
            index += ch.len_utf8();
            continue;
        }
        if in_string || !(ch.is_ascii_alphabetic() || ch == '_') {
            index += ch.len_utf8();
            continue;
        }
        let mut end = index;
        for (offset, c) in promql[index..].char_indices() {
            if c.is_ascii_alphanumeric() || c == '_' {
                end = index + offset + c.len_utf8();
            } else {
                break;
            }
        }
        let name = &promql[index..end];
        if wanted(name) && followed_by_range_selector(&promql[end..]) {
            out.push(name.to_string());
        }
        index = end;
    }
    out
}

/// Pod-scoped series: the subject is a pod, and a rollout deletes pods daily.
fn pod_scoped_range_subjects(promql: &str) -> Vec<String> {
    range_subjects_where(promql, |name| {
        name.starts_with("kube_pod_") || name.starts_with("container_")
    })
}

/// Node-scoped series: the subject is a node. A node leaving the cluster is
/// rarer, which is exactly why a rule naming it for six hours goes unnoticed.
fn node_scoped_range_subjects(promql: &str) -> Vec<String> {
    range_subjects_where(promql, |name| name.starts_with("kube_node_"))
}

/// Whether what follows an identifier is `[<duration>]` or `{...}[<duration>]`.
fn followed_by_range_selector(tail: &str) -> bool {
    let tail = tail.trim_start();
    let tail = match tail.strip_prefix('{') {
        Some(rest) => match rest.find('}') {
            Some(close) => &rest[close + 1..],
            None => return false,
        },
        None => tail,
    };
    tail.trim_start().starts_with('[')
}

/// Rules that count a subject-scoped series over a window without asking
/// whether the subject is still there.
fn complaints_against(
    rules: &[(String, String)],
    subjects_of: fn(&str) -> Vec<String>,
    subject: &str,
) -> Vec<String> {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in rules {
        let subjects = subjects_of(promql);
        if subjects.is_empty() {
            continue;
        }
        // The conjunct reads the same series through an instant selector, which
        // stops returning it once the series is older than the lookback delta.
        if promql.contains("time() - timestamp(") {
            continue;
        }
        complaints.push(format!(
            "{rule} 在窗口里数 {}，却没有要求这条序列最近还在被采集（主体是{subject}）: {promql}",
            subjects.join(", ")
        ));
    }
    complaints
}

fn deleted_subject_complaints(rules: &[(String, String)]) -> Vec<String> {
    complaints_against(rules, pod_scoped_range_subjects, "Pod")
}

fn deleted_node_complaints(rules: &[(String, String)]) -> Vec<String> {
    complaints_against(rules, node_scoped_range_subjects, "节点")
}

/// A range selector keeps counting a subject that has already been deleted.
///
/// 2026-10-23: `pod_restarting_repeatedly` was still firing on
/// `cogneva-6b4c7cb489-5rb44` four minutes after the pod left
/// `kubectl get pods`, and would have gone on for six hours from its death --
/// `changes(kube_pod_container_status_restarts_total{uid="f65ae7ac-..."}[6h])`
/// read 3 while `kube_pod_info` for that uid was already empty. The rule's own
/// summary claimed the six-hour tail was fixed by counting with `changes()`
/// instead of `increase()`; both functions read the same window.
#[test]
fn a_range_selector_over_a_pod_scoped_series_requires_the_subject_to_still_exist() {
    let complaints = deleted_subject_complaints(&shipped_rules());
    assert!(
        complaints.is_empty(),
        "这些规则会在主体（Pod）被删除之后，其标签仍留在窗口里的样本继续让它们为真；\
         给每个区间聚合加一个合取项 `and on(namespace, pod, container) (time() - timestamp(<同一序列>) < 300)`:\n{}",
        complaints.join("\n")
    );
}

/// The gate's two directions, from fixtures rather than from the tree.
fn deleted_subject_case(name: &str, promql: &str) -> Vec<(String, String)> {
    vec![(name.to_string(), promql.to_string())]
}

#[test]
fn the_deleted_subject_gate_reports_the_rule_without_the_conjunct_and_allows_the_one_with_it() {
    let unguarded = deleted_subject_case(
        "restart",
        "changes(kube_pod_container_status_restarts_total[6h])",
    );
    assert_eq!(deleted_subject_complaints(&unguarded).len(), 1);

    let guarded = deleted_subject_case(
        "restart",
        "changes(kube_pod_container_status_restarts_total[6h]) and on(namespace, pod, container) (time() - timestamp(kube_pod_container_status_restarts_total) < 300)",
    );
    assert!(deleted_subject_complaints(&guarded).is_empty());

    // A gauge read at an instant carries no window, so the subject's absence
    // already silences it: this rule must not be reported.
    let instant = deleted_subject_case(
        "memory",
        "kube_pod_container_resource_limits{resource=\"memory\"}",
    );
    assert!(deleted_subject_complaints(&instant).is_empty());

    // The matcher spelling is part of the same shape: `{...}[5m]` is a range
    // too, and reading the identifier alone would let it through.
    let with_matcher = deleted_subject_case(
        "writes",
        "max by (namespace, pod, container) (rate(container_fs_writes_bytes_total{container!=\"\"}[5m]))",
    );
    assert_eq!(deleted_subject_complaints(&with_matcher).len(), 1);

    // A namespace aggregate whose subject is the namespace is not this shape.
    let namespaced = deleted_subject_case(
        "oom",
        "sum by (namespace) (increase(sandbox_oom_kills_total[30m]))",
    );
    assert!(deleted_subject_complaints(&namespaced).is_empty());
}

/// A node that leaves the cluster keeps its samples in the window too.
///
/// `node_capacity_changed` counts `changes()` over six hours and names the node
/// it read; on a node removed from the cluster the count stays true for the
/// rest of the window, so the alert outlives the machine it reports on. The
/// pod gate above cannot see it: its subject is a node.
#[test]
fn a_range_selector_over_a_node_scoped_series_requires_the_node_to_still_exist() {
    let complaints = deleted_node_complaints(&shipped_rules());
    assert!(
        complaints.is_empty(),
        "这些规则会在节点被移出集群之后，其标签仍留在窗口里的样本继续让它们为真；\
         给每个区间聚合加一个合取项 `and on(node) (time() - timestamp(<同一序列>) < 300)`:\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_node_subject_gate_reports_the_rule_without_the_conjunct_and_allows_the_one_with_it() {
    let unguarded = deleted_subject_case(
        "capacity",
        "max without (pod, container, instance) (changes(kube_node_status_allocatable{node=~\".+\", resource=~\"cpu|memory\"}[6h]))",
    );
    assert_eq!(deleted_node_complaints(&unguarded).len(), 1);

    let guarded = deleted_subject_case(
        "capacity",
        "max without (pod, container, instance) (changes(kube_node_status_allocatable{node=~\".+\", resource=~\"cpu|memory\"}[6h])) and on(node) (time() - timestamp(kube_node_status_allocatable{node=~\".+\", resource=~\"cpu|memory\"}) < 300)",
    );
    assert!(deleted_node_complaints(&guarded).is_empty());

    // A gauge read at an instant already goes silent with the node: not this shape.
    let instant = deleted_subject_case(
        "reserve",
        "sum by (node) (kube_node_status_capacity{resource=\"memory\"}) - sum by (node) (kube_node_status_allocatable{resource=\"memory\"})",
    );
    assert!(deleted_node_complaints(&instant).is_empty());

    // A pod-scoped rule is not the node gate's business either: the two gates
    // must not stand in for one another.
    let pod_scoped = deleted_subject_case(
        "restart",
        "changes(kube_pod_container_status_restarts_total[6h])",
    );
    assert!(deleted_node_complaints(&pod_scoped).is_empty());
    assert_eq!(deleted_subject_complaints(&pod_scoped).len(), 1);
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
    for (rule, promql) in shipped_rules() {
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
/// "This repo ships" is the whole of the reach: the rules the chart renders and
/// the ones a `PrometheusRule` manifest here applies. A rule a third party
/// carries in its own object is still not in view, so the name says the reach
/// is this repo's rules rather than every rule the cluster runs.
#[test]
fn no_rule_this_repo_ships_reads_a_device_counter_as_elapsed_time() {
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in shipped_rules() {
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

/// Which aggregation a rule applies to the declared ceilings it compares to the
/// node's capacity, and whether that aggregation takes one container or the fleet.
///
/// The subject is the aggregation in front of `kube_pod_container_resource_limits`:
/// `max by (...)` picks the largest single container, `sum by (...)` adds every
/// container up. Only the first can be a ceiling the node can never honour. The
/// second is above one in the steady state of any platform that sizes limits as
/// per-period burst allowances rather than as reservations -- on 2026-10-23 this
/// node read 1.20 on cpu and 1.01 on memory with nothing wrong -- so a rule
/// conditioned on it is lit every day, and it flaps across the boundary whenever
/// a pod is replaced: the two rows it raised resolved by themselves an hour later.
fn capacity_subject_complaints(rules: &[(String, String)]) -> Vec<String> {
    const LIMITS: &str = "kube_pod_container_resource_limits";
    const CAPACITY: &str = "kube_node_status_allocatable";
    let mut complaints = Vec::new();
    for (rule, promql) in rules {
        let Some(metric_at) = promql.find(LIMITS) else {
            continue;
        };
        if !promql.contains(CAPACITY) {
            continue;
        }
        let head = &promql[..metric_at];
        let nearest = ["max by", "sum by", "avg by", "min by"]
            .into_iter()
            .filter_map(|aggregation| head.rfind(aggregation).map(|at| (at, aggregation)))
            .max_by_key(|(at, _)| *at);
        match nearest {
            Some((_, "max by")) => {}
            Some((_, aggregation)) => complaints.push(format!(
                "{rule}: applies `{aggregation}` to the declared ceilings, so the reading is about \
                 the fleet rather than about one container's reachable ceiling\n    {promql}"
            )),
            None => complaints.push(format!(
                "{rule}: divides declared ceilings by node capacity without an aggregation that \
                 picks a single container\n    {promql}"
            )),
        }
    }
    complaints
}

/// A rule that compares declared ceilings to what a node holds takes one
/// container as its subject, not the sum of them all.
#[test]
fn a_capacity_rule_compares_one_container_to_the_node_not_the_sum_of_them() {
    let complaints = capacity_subject_complaints(&shipped_rules());
    assert!(
        complaints.is_empty(),
        "these capacity rules alarm on a ratio that is above one by design, so they are \
         lit whenever the fleet is healthy:\n{}",
        complaints.join("\n")
    );
}

/// The gate above is aimed at the shape it means to catch, and lets the other
/// one through.
#[test]
fn the_capacity_subject_gate_catches_a_summed_fleet_and_allows_a_single_container() {
    let envelope = |aggregation: &str| {
        (
            "capacity_under_test".to_string(),
            format!(
                r#"{aggregation} by (node, resource) (kube_pod_container_resource_limits{{resource=~"cpu|memory"}}) / on(node, resource) group_left sum by (node, resource) (kube_node_status_allocatable{{resource=~"cpu|memory"}})"#
            ),
        )
    };

    for summed in ["sum by", "avg by"] {
        assert_eq!(
            capacity_subject_complaints(&[envelope(summed)]).len(),
            1,
            "`{summed}` over the ceilings should be reported"
        );
    }
    assert!(
        capacity_subject_complaints(&[envelope("max by")]).is_empty(),
        "the largest single container is the subject this gate asks for"
    );
}

/// 拿申请量与节点可分配量相比的规则，只能数还占着位的 Pod。
///
/// `kube_pod_container_resource_requests` 对 API 里还在的**每个** Pod 都出样本，
/// 已经结束的也在：Pod 结束时 `nodeName` 与申请量都留在对象上，kube-state-metrics
/// 照读不误。于是「节点上被申请了多少」这句话，如果不按 Pod 的 phase 过滤，数的
/// 是「曾经来过多少」，而这一族容器每次发布留下的 Job Pod 都在往里加分。
///
/// 实测 2026-10-27：36 个结束的 `cogneva-mainline-*` Pod 占着 11.2 核里的 6.9 核，
/// 比值读到 0.470；只数还占着位的，是 0.181。三十天里不带这条腿的读数在 0.286 到
/// 0.470 之间随死 Pod 的多少摆动，带这条腿的从 0.146 走到 0.181——一条随发布次数
/// 上涨的读数，响的时候说的是部署器的历史，不是节点。
///
/// 门只拦「与可分配量相比」的那一类：单纯成篇记账（没有分母）不做这个联接也说得通，
/// 而这个库与这些清单里今天只有前一类（反例普查：全库只有这条规则读这个指标）。
fn unphased_request_ratio_complaints(rules: &[(String, String)]) -> Vec<String> {
    const REQUESTS: &str = "kube_pod_container_resource_requests";
    const CAPACITY: &str = "kube_node_status_allocatable";
    let mut complaints = Vec::new();
    for (rule, promql) in rules {
        if !promql.contains(REQUESTS) || !promql.contains(CAPACITY) {
            continue;
        }
        let live_phase = promql.contains("kube_pod_status_phase")
            && (promql.contains(r#"phase=~"Pending|Running""#)
                || promql.contains(r#"phase="Running""#));
        if !live_phase {
            complaints.push(format!(
                "{rule}: 把申请量除以节点可分配量，却没按 Pod 的 phase 过滤，于是数的是\
                 「曾经来过多少」而不是「现在占着多少」——结束的 Pod 仍带着 nodeName 与\
                 申请量，发布留下的 Job Pod 会一直加分\n    {promql}"
            ));
        }
    }
    complaints
}

#[test]
fn a_request_ratio_counts_only_the_pods_that_still_hold_their_request() {
    let complaints = unphased_request_ratio_complaints(&shipped_rules());
    assert!(
        complaints.is_empty(),
        "这些规则把申请量除以节点可分配量，却把已经结束的 Pod 也算进去了：\n{}",
        complaints.join("\n")
    );
}

/// 上面那条门抓的是它想抓的形状，且放行另一种。
#[test]
fn the_request_ratio_gate_catches_the_unfiltered_sum_and_allows_the_phase_join() {
    let with_phase = |phase: &str| {
        (
            "requests_under_test".to_string(),
            format!(
                r#"max by (node, resource) (sum by (node, resource) (kube_pod_container_resource_requests and on (namespace, pod) kube_pod_status_phase{{{phase}}} == 1) / on(node, resource) kube_node_status_allocatable)"#
            ),
        )
    };
    let unfiltered = (
        "requests_under_test".to_string(),
        "max by (node, resource) (sum by (node, resource) (kube_pod_container_resource_requests) / on(node, resource) kube_node_status_allocatable)".to_string(),
    );

    assert_eq!(
        unphased_request_ratio_complaints(&[unfiltered]).len(),
        1,
        "不按 phase 过滤应当被抓住"
    );
    for live in [r#"phase=~"Pending|Running""#, r#"phase="Running""#] {
        assert!(
            unphased_request_ratio_complaints(&[with_phase(live)]).is_empty(),
            "{live} 是这条门要的形状"
        );
    }
    assert_eq!(
        unphased_request_ratio_complaints(&[with_phase(r#"phase="Succeeded""#)]).len(),
        1,
        "按结束态过滤是把整条读数清零，不算过滤"
    );
    // 没有分母的那一类不在这条门的射程里：纯记账不按 phase 切也说得通。
    let accounting = (
        "accounting_under_test".to_string(),
        "sum by (namespace) (kube_pod_container_resource_requests)".to_string(),
    );
    assert!(unphased_request_ratio_complaints(&[accounting]).is_empty());
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
    for (rule, promql) in shipped_rules() {
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
    for (rule, promql) in shipped_rules() {
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
    for (rule, promql) in shipped_rules() {
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
    for (rule, promql) in shipped_rules() {
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

    let rules: BTreeMap<String, String> = shipped_rules().into_iter().collect();
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
    for (rule, promql) in shipped_rules() {
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

    // An existence question rather than a value read: a window over the series
    // with nothing asked of its writer, which is the shape four absent-shaped
    // rules ship. Silence here is the point -- a frozen value quiets this rule
    // instead of pinning it on, so the guard would be dead weight.
    let backups = only(&["cogneva_backup_run_readings"]);
    assert!(
        unguarded_store_reading_complaints(
            "max(absent_over_time(cogneva_backup_run_readings{readout=\"drill\"}[10m]))",
            &backups
        )
        .is_empty(),
        "问「这条序列出现过没有」被当成「把值当观测」报了出来"
    );
    // The same window read as a value still needs its writer: the exemption is
    // about existence, not about windows.
    assert_eq!(
        unguarded_store_reading_complaints(
            "max(cogneva_backup_run_readings{readout=\"drill\"}[10m]) > 0",
            &backups
        )
        .len(),
        1,
        "窗口内的取值没有被报出来"
    );
    // Both questions in one expression: the value read is still the subject, so
    // the name must not be exempted wholesale.
    assert_eq!(
        unguarded_store_reading_complaints(
            "max(absent_over_time(cogneva_backup_run_readings{readout=\"drill\"}[10m])) \
             + max(cogneva_backup_run_readings{readout=\"drill\"}[10m]) > 0",
            &backups
        )
        .len(),
        1,
        "同一个名字既问存在又读值时，读值那一处被一并豁免了"
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
    for (rule, promql) in shipped_rules() {
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
        for (rule, promql) in shipped_rules() {
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
    for (_rule, promql) in shipped_rules() {
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
    for (rule, promql) in shipped_rules() {
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

/// Complaints about a freshness bound on the policy store readouts that is no
/// longer than the loop which republishes them.
///
/// The two failures are not symmetric. A bound shorter than the loop's period
/// is failed by a healthy loop sitting between two of its own publishes; a
/// bound at exactly the period is the same thing, because the gap the loop may
/// leave *is* its period. Only strictly longer is safe.
fn readout_bound_complaints(rule: &str, promql: &str, series: &str, cadence: u64) -> Vec<String> {
    let Some(bound) = companion_age_bound(promql, series) else {
        return Vec::new();
    };
    if bound > cadence {
        return Vec::new();
    }
    vec![format!(
        "{rule}: 新鲜界 {bound}s 不长于读数循环周期 {cadence}s——循环在两次发布之间的正常\
         空档会被读成写者停了\n    {promql}"
    )]
}

/// A rule that judges the policy store readouts has to look back further than
/// the loop that republishes them.
///
/// The loop's period lives in the crate that owns the loop and the bound lives
/// in the chart; neither number is written into this test, because a bound
/// chosen against a constant drifts from it silently the moment either side
/// moves — and the drift here fires on a working process, since a bound
/// no longer than the period is satisfied by a healthy loop between two of its
/// own publishes. The restore hand-over rules are the subject: they are the
/// ones that must not be read as "the writer stopped" while it is merely
/// between rounds.
#[test]
fn a_rule_judging_the_policy_store_readouts_waits_longer_than_their_loop() {
    let series = cog_core::metric_names::POLICY_STORE_READINGS.as_str();
    let cadence = cog_reflection::store_readouts::STORE_READOUTS_INTERVAL.as_secs();
    assert!(cadence > 0, "读数循环周期为 0，下面的比较没有分母");

    let mut bounded = 0usize;
    let mut complaints: Vec<String> = Vec::new();
    for (rule, promql) in shipped_rules() {
        if !promql.contains(series) {
            continue;
        }
        if companion_age_bound(&promql, series).is_some() {
            bounded += 1;
        }
        complaints.extend(readout_bound_complaints(&rule, &promql, series, cadence));
    }

    // 分母：三条恢复交接规则都读这族读数。一条都不再带年龄界时，下面的
    // 「没有投诉」是空的；而且另一条门（读共享表 gauge 必须问写者）会先红。
    assert!(
        bounded >= 3,
        "只有 {bounded} 条规则对策略库读数取了年龄界，恢复交接的三条规则像是不见了"
    );
    assert!(
        complaints.is_empty(),
        "告警规则对策略库读数取的新鲜界必须严格长于发布循环的周期，否则空档是健康的:\n{}",
        complaints.join("\n")
    );
}

/// The gate above separates the two numbers it compares, against fabricated
/// rules as well as the shipped ones — a gate that only ever runs over rules
/// that pass has not been shown to reject anything.
#[test]
fn a_readout_bound_no_longer_than_the_loop_is_reported_and_a_longer_one_is_not() {
    let series = "cogneva_policy_store_readings";
    let cadence = 300u64;
    let rule = |bound: u64| {
        format!(
            "max(cogneva_policy_store_readings{{readout=\"active_version\"}} and \
             ((time() - cogneva_policy_store_readings_observed_timestamp_seconds\
             {{readout=\"active_version\"}}) < {bound}))"
        )
    };

    // 等于循环周期也不行：循环可以一直不写，直到周期边界。
    assert_eq!(
        readout_bound_complaints("r", &rule(300), series, cadence).len(),
        1,
        "等于循环周期的界没有被报出来"
    );
    assert!(
        readout_bound_complaints("r", &rule(1800), series, cadence).is_empty(),
        "严格长于循环周期的界被误报"
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

/// A rule selects a *cell*, and the name gate stops at the brace.
///
/// `every_series_an_alert_rule_reads_is_one_something_produces` compares series
/// names, so two rules over the same family are equally green whether or not
/// the cells they select exist. `max(cogneva_tasks_readings{readout="held_unstated_rows"}) > 0`
/// read `readout="held_unstated_row"` — one letter off a cell the producer
/// writes — selects a series that exists and a value that never arrives, so it
/// matches nothing and its silence is indistinguishable from health. Nothing in
/// the tree reds: the producer is right, the name is right, and the rule is
/// dead.
///
/// The cells checked are the ones selected from a series PRODUCED registers,
/// whatever the label. The producer side is every string literal the tree
/// writes outside the rule files themselves: this asks whether the cell is a
/// name this workspace writes, which is what a rename or a typo breaks. Reading
/// the rule files into that index would let every cell license itself.
///
/// A cell written only at run time (`format!`) is a false red here, and the
/// message says which rule and cell so it is one line to fix — either the
/// producer starts writing the literal or the rule stops selecting it.
#[test]
fn every_cell_a_rule_selects_is_a_name_this_tree_writes() {
    let rules = shipped_rules();
    let produced: BTreeSet<&str> = PRODUCED.iter().map(|(name, _)| *name).collect();
    let cells: Vec<(String, String, String)> = rules
        .iter()
        .flat_map(|(_, promql)| promql::selected_cells(promql))
        .filter(|(series, _, _)| produced.contains(series.as_str()))
        .collect();
    let labels: BTreeSet<&str> = cells.iter().map(|(_, label, _)| label.as_str()).collect();
    assert!(
        cells.len() >= 60 && labels.len() >= 15,
        "只从 {} 条规则里收到 {} 个单元格、{} 个标签：这份普查的解析面塌了，下面「没有未知单元格」是空的",
        rules.len(),
        cells.len(),
        labels.len()
    );

    let written = cells_written::literals_the_tree_writes(
        &repo_root(),
        PRODUCED.iter().map(|(_, source)| repo_root().join(source)),
    );
    let unknown = cells_written::cells_no_one_writes(&rules, &produced, &written);
    assert!(
        unknown.is_empty(),
        "这些规则选中的标签值在规则文件之外一次都没被写下过，它们读的是一个永远不会出现的取值:\n{}",
        unknown.join("\n")
    );
}

/// The gate separates the two answers against fabricated rules as well: a gate
/// that has only ever run over cells that exist has not been shown to report
/// one that does not.
#[test]
fn a_cell_no_producer_writes_is_reported_and_one_that_exists_is_not() {
    let produced: BTreeSet<&str> = ["cogneva_tasks_readings"].into_iter().collect();
    let written: BTreeSet<String> = ["held_unstated_rows"]
        .into_iter()
        .map(String::from)
        .collect();
    let rules = |promql: &str| vec![("rule".to_string(), promql.to_string())];

    assert!(cells_written::cells_no_one_writes(
        &rules(r#"max(cogneva_tasks_readings{readout="held_unstated_rows"}) > 0"#),
        &produced,
        &written
    )
    .is_empty());
    // One letter off, and the rule matches nothing for as long as it ships.
    let complaints = cells_written::cells_no_one_writes(
        &rules(r#"max(cogneva_tasks_readings{readout="held_unstated_row"}) > 0"#),
        &produced,
        &written,
    );
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(
        complaints[0].contains(r#"readout="held_unstated_row""#),
        "{}",
        complaints[0]
    );
    // An alternative of a regex is a cell too.
    assert_eq!(
        cells_written::cells_no_one_writes(
            &rules(r#"max(cogneva_tasks_readings{readout=~"held_unstated_rows|gone"}) > 0"#),
            &produced,
            &written
        )
        .len(),
        1
    );
    // A series PRODUCED does not register is another gate's question.
    assert!(cells_written::cells_no_one_writes(
        &rules(r#"max(not_ours_total{readout="gone"}) > 0"#),
        &produced,
        &written
    )
    .is_empty());
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
    closed_set::assert_no_foreign_name_is_ours(FOREIGN);
}

#[test]
fn every_label_value_a_rule_has_to_select_is_selected_by_one() {
    use cog_collaboration::ChangeYieldOutcome;
    use cog_github::redrive_budget::{BudgetSide, RedriveRefusal};

    let rules = shipped_rules();
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
        cog_observability::config_delivery::RULES_NOT_LOADED_RULE,
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

    let configured: BTreeSet<String> = shipped_rules().into_iter().map(|(name, _)| name).collect();
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
    let read: BTreeSet<String> = shipped_rules()
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

/// The pending band is a registration without a reader, so its own criteria are
/// the ones that can catch it lying: a name that two tables claim, a name that
/// is already read, an exit marker the repository contradicts, and a band whose
/// size nobody counted. The band is empty today, so these cases come from
/// fixtures rather than from the table itself.
/// One row of the pending-band table, in the order the assertion reads it:
/// the case name, what the band registers, the names already read by a real
/// table, the names another table claims, and the band size it should report.
///
/// Named rather than written inline because the tuple is five fields deep and
/// clippy refuses a type this complex at `-D warnings` -- which is the whole
/// workspace's bar, not this test's.
type PendingBandCase<'a> = (
    &'a str,
    &'a [(&'a str, PendingExit)],
    &'a [&'a str],
    &'a [&'a str],
    usize,
);

#[test]
fn foreign_pending_refuses_the_states_it_claims_to() {
    let publishes = |name: &str| name == "published_here";

    let cases: &[PendingBandCase] = &[
        (
            "干净的条目",
            &[("a_series", PendingExit::Witness)],
            &[],
            &[],
            0,
        ),
        (
            "同一个名字也进了真登记表",
            &[("a_series", PendingExit::Witness)],
            &["a_series"],
            &[],
            1,
        ),
        (
            "已经有规则在读",
            &[("a_series", PendingExit::Witness)],
            &[],
            &["a_series"],
            1,
        ),
        (
            "NoReading 没写理由",
            &[("a_series", PendingExit::NoReading("  "))],
            &[],
            &[],
            1,
        ),
        (
            "Unregistered 说本仓发布，闭集里却没有",
            &[("a_series", PendingExit::Unregistered("等那条规则落地"))],
            &[],
            &[],
            1,
        ),
        (
            "NoReading 说没人能判，本仓却在发布它",
            &[("published_here", PendingExit::NoReading("等那条规则落地"))],
            &[],
            &[],
            1,
        ),
        (
            "重名",
            &[
                ("a_series", PendingExit::Witness),
                ("a_series", PendingExit::Witness),
            ],
            &[],
            &[],
            1,
        ),
    ];

    for (what, pending, registered, read, expected) in cases {
        let read: BTreeSet<String> = read.iter().map(|n| (*n).to_string()).collect();
        assert_eq!(
            pending_complaints(pending, registered, &read, &publishes).len(),
            *expected,
            "{what}"
        );
    }
}

#[test]
fn the_pending_band_is_counted() {
    let registered: Vec<&str> = PRODUCED
        .iter()
        .map(|(n, _)| *n)
        .chain(FOREIGN.iter().map(|(n, _)| *n))
        .collect();
    let read: BTreeSet<String> = shipped_rules()
        .iter()
        .flat_map(|(_, promql)| metric_names_in(promql))
        .collect();

    let complaints = pending_complaints(
        FOREIGN_PENDING,
        &registered,
        &read,
        &closed_set::a_series_this_build_publishes,
    );
    assert!(
        complaints.is_empty(),
        "FOREIGN_PENDING 里有条目不成立:\n{}",
        complaints.join("\n")
    );
    assert_eq!(
        FOREIGN_PENDING.len(),
        FOREIGN_PENDING_AT_CENSUS,
        "pending 的条数与 FOREIGN_PENDING_AT_CENSUS 不再相等（表内 {n} 笔，常数 {c}）。\
         多一笔：那是多欠一个读者，改这个数就是承认它；少一笔：说明补上了读者，\
         同样要把它改回来——这个数就是「还有几个名字在等读者」的账",
        n = FOREIGN_PENDING.len(),
        c = FOREIGN_PENDING_AT_CENSUS
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
    // 被拒轮次的计数。它当初是为饿死那一面建的，而那个面现在按秒读：连续多少轮没拿到
    // 槽与「等了多久」是两个量纲，轮长十分钟时前者几乎不随后者变（同样十六段等待，
    // 三千多秒那次数到二），所以规则改读 `cogneva_mainline_advance_wait_since_unix`，
    // 这一格就失去了它唯一的读者。它仍在写，因为写它的那个函数是控制律取数的同一处：
    // 阈值比的是这个数（连续三轮被拒就去认领名额），分开写会让两个消费者读两份可能
    // 不同步的账。所以它不是一个可以撤掉的登记，而是一格只剩进程内消费者的读数。
    (
        "cogneva_mainline_advance_deferrals",
        Unread::Elsewhere {
            readers: &["cogneva_mainline_advance_wait_since_unix"],
            reason: "the starvation this counted is now read in seconds, and the rule reads that series; this row is still written only because the control law compares the same number inside the process, so nothing on the reading faces consumes it",
        },
    ),
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
        "cogneva_observability_events_bytes",
        Unread::NotAReading("on-disk size of the event log, and it lags the row count for the same reason the sample log's does -- PostgreSQL returns deleted space only when it vacuums -- so a bound here would fire on the volume doing its job. The capacity verdict is the reading with a criterion; the bytes are what the volume is paying for it"),
    ),
    (
        "cogneva_checkpoints_bytes",
        Unread::NotAReading("on-disk size of the checkpoint store, and it lags the row count for the same reason the other two do -- PostgreSQL returns the space a DELETE freed only when it vacuums -- so a bound here would fire on the volume doing its job. The capacity verdict is the reading with a criterion; the bytes are what the backup the drill restores is actually paying for"),
    ),
    (
        "gateway_llm_exchange_bytes",
        Unread::NotAReading("on-disk size of the prompt-and-response archive, and it lags what the retention deletes for the same reason the other three do -- PostgreSQL returns the space a DELETE freed only when it vacuums -- so a bound here would fire on the volume doing its job. The age rule is the reading with a criterion; the bytes say what the bodies are actually costing, which is why the archive is bounded by age rather than by a row count"),
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
    (
        "cogneva_mainline_ci_check_excluded_total",
        Unread::Gap("checks left out of the mainline's CI verdict because they belong to a workflow that does not judge this tree -- the Pages deployment, which answers whether a site can be published rather than whether this revision can roll. The per-revision fact is not this series: the state file records the excluded names and workflow paths under excluded_checks, and the release path writes an info line naming them, so what the counter adds is the count rather than the what. A rule would have to declare how many exclusions are a fault, and the bound is not a number on this series but a claim about the table: the list is meant to stay one entry long, so the fault shape is the list growing, which no rule can see from a count of exclusions alone. The assertion that would make the list's length a checked fact does not exist on this tree: nothing goes red when the list grows to a second entry, and no reading states how many exclusions are normal. That is a missing criterion, not a bound that has not arrived yet -- written as a bound it could be read as explained, and it is not"),
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
    // The pair minted when the publisher started holding ready tasks back --
    // one on what the window can pay for, one on the board column that is full
    // -- is not counted here any more: the dashboard's "Publisher Holds: The
    // Window and the Board" panel draws both, which is the reader the window
    // half was filed as owing. A rule still cannot take either: every nonzero
    // is the gate working as designed, and what would make a hold a fault is
    // one that outlasts what justified it. The window entry left with the
    // panel; the board entry never had to be filed, because it arrived with its
    // reader in the same pass -- which is why the count moves by one.
    //
    // The write-window record, published for the first time in this pass because
    // the face that owes the reading had nowhere to get it from. Four cells, and
    // a rule on any of them would have to say when an overlap or a self-approval
    // is a fault: `approved` is zero whenever nobody is writing, which is the
    // ordinary state, and one live window is not better or worse than two. The
    // reader owed here is a panel in the change-ledger group drawing the four
    // cells together -- deliberately not written in this pass, and filed rather
    // than given an invented threshold. What the cells are read by today is the
    // governance face reading on the host side, which takes `write_window_approved`
    // as that face's first reading; that reader lives outside this repository's
    // rules and panels, which is why it does not close this entry.
    (
        "cogneva_change_write_windows",
        Unread::Gap("the write windows the account holds, counted by what the record says about each: approved, overlapping scopes, rows that are both approved and revoked, and live windows their own writer approved. A rule cannot close it: `approved` is zero whenever nobody is writing, which is the ordinary state, and neither overlap nor self-approval has a declared bound -- how many accounts may write one path at once, and whether a self-approved window is a fault or merely a record to be corrected, are policies nobody has stated. Its reader is a panel in the change-ledger group drawing the four cells together; no such panel was written in this pass"),
    ),
    // The outlet list, published for the first time in this pass for the same
    // reason the write-window record was: the face that owes the reading had
    // nowhere to get it from. The dispatch counter only moves when a batch is
    // sent, so a deployment with no outlet and nothing firing had that state on
    // no series at all, and "nobody can be told" read exactly like "nothing
    // needed telling". A rule cannot close this one: a rule on `channel="none"`
    // at 0 would have to deliver its own page through the channel it reports as
    // absent, and a rule on a missing family would be the same false clear the
    // observability face already refused. The reader owed here is the
    // governance face reading on the host side, which takes this family as the
    // communication face's first reading; that reader lives outside this
    // repository's rules and panels, which is why it does not close this entry.
    (
        "cogneva_alert_channels_configured",
        Unread::Gap("which outlets this deployment can deliver an alert through. Two kinds of row with opposite polarity, told apart by `state`: `state=\"configured\"` rows name one outlet each in `channel` at 1, and no such row is written when there is no outlet -- a placeholder name would read as a kind that exists and is switched off -- and one `state=\"verdict\"` row carries no `channel` but the `readout` name the communication face owes, at 1 when there is no outlet at all and 0 when there is at least one -- written in both directions, since the configured rows exist only when there is an outlet and so could never say there is none. The in-app alert surface counts as an outlet: it has no address, so nothing about it can be misconfigured, and the WebUI reads the same rows the external channels would carry. A reader must select one kind by `state` before aggregating; summing the family answers yes either way. Every value is a constant per process, so a reader takes one series rather than adding the copies. It is filed rather than given a threshold because every cell is the configuration itself: 1 is the ordinary state of a deployment with an outlet and 0 the ordinary state of one without, so neither number is a condition that turns into a fault at some level. What makes a deployment without an outlet wrong is a decision about that deployment, not something this series can bound"),
    ),
    // The same account counted the other way, by the tier each carrier said its
    // principle would be checked at. No rule reads it, and the reason is not
    // that the reading is unimportant: the cell worth watching is `undeclared`,
    // the rows the state census counts as debt while nobody has said what a gate
    // for them would even look like, and any bound on how many of those a corpus
    // may hold is a policy about how much unfinished thinking is normal. Growth
    // is the one non-arbitrary signal, and it is not a fault by itself -- a
    // corpus that gains principles gains undeclared rows with them until someone
    // writes the sentence. Its reader is the panel beside the state census,
    // which shows the composition without inventing a threshold.
    // The refusal count left this table when `principle_refusals_sustained`
    // landed: one refusal is a gate doing its job, and the same slug refused
    // inside every 24-hour window of three days is a rule the corpus is not
    // internalizing. The bound that made it a debt is now a shape rather than a
    // level, which is what the entry above asks for in the same words.
    // The size readings of an archive whose retention bounds age. A stream is
    // retired when it goes quiet past the retention, so the row count and the
    // stream count settle at whatever the deployment writes in that window --
    // they are numbers about traffic, not verdicts about the sweep. The fault
    // these cells would catch, a sweep that stopped, is already stated by the
    // oldest-row rule each archive ships with, and that rule needs no declared
    // budget. They are published regardless: both tables had grown into the
    // hundreds of megabytes with no reading at all (measured on this deployment
    // on 2026-10-27), and the backup drill is sized from exactly this pair.
    (
        "cogneva_task_events_rows",
        Unread::Gap("how many events the per-task archive holds. The retention that bounds this table works on age rather than size: a stream is retired when it has been quiet past the retention, so at any fixed write rate the row count settles on its own and no row budget for it is declared anywhere in the tree. A threshold invented here would page on traffic instead of on a sweep that stopped, and that fault already has a reader in task_event_archive_behind, which compares the oldest surviving row against the retention the loop publishes. It is published anyway because the table reached 211 MB with nothing reading it, and the nightly backup's budget is sized from it"),
    ),
    (
        "cogneva_task_events_tasks",
        Unread::Gap("how many distinct task streams the per-task archive holds, published beside cogneva_task_events_rows because a pass that retires many events out of one stream and one that retires the same events out of a thousand are different pictures of the same sweep. That is what makes it worth storing; it does not make it judgeable, and the bound it would be read against is the same undeclared row budget its sibling is missing"),
    ),
    (
        "gateway_llm_exchange_rows",
        Unread::Gap("how many prompt-and-response bodies the archive holds. Its two sibling readings are read -- llm_exchange_retention_behind compares the oldest body's age against the retention the loop publishes -- and this one has nothing to be compared with: the archive is bounded by age, so the body count follows the call volume and how large a body is, and no budget for either is declared. It is published because the table reached 307 MB before any retention ran, and the backup drill is sized from it"),
    ),
    (
        "cogneva_llm_usage_rows",
        Unread::Gap("how many per-call token rows the ledger holds. Everything that can be judged about this table is judged from age instead: llm_usage_retention_behind compares the oldest surviving row against the retention the loop publishes, and llm_usage_fold_behind compares the rows the hourly fold never reached against what the sweep is allowed to delete. Neither is a statement about how many rows is too many -- one call writes one row whatever the task was doing, so the count tracks call volume rather than the sweep, and no row budget for it is declared anywhere in the tree. It is published because the table reached 312,803 rows and 32 days' worth of history with no reading at all until 2026-10-27, and the nightly backup's budget is sized from it"),
    ),
    // The re-measurement intake publishes two numbers from the same pass and only
    // the age got a rule. The reason is not that the depth is uninteresting: it
    // is the size of the population the age rule is about, and the help text
    // beside it already names a shape -- above zero across two passes is the
    // intake failing -- so this is not the usual "no bound is declared" case.
    // The shape cannot be sampled, though. The pass cadence is a number in the
    // writer's configuration and no series publishes it, so "two passes" is not
    // a window a rule can be written over; the nearest thing a rule could say is
    // "still above zero after N minutes", and the queue's ordinary life is
    // exactly one pass, which makes every N a decision about how long an
    // ordinary queue may be caught mid-turn rather than a reader someone forgot
    // to write. Meanwhile the fault the depth would catch, a fold that is not
    // reaching the request, already fires from the age at six passes. What the
    // pair settles -- a depth that turns over while the age climbs is one row
    // the tree cannot settle, a depth that does not move at all is a writer --
    // is a comparison of two curves, so the reader owed is a panel, not a rule.
    (
        "cogneva_change_ledger_remeasure_pending",
        Unread::Gap("how many re-measurement requests are waiting to be folded, written on every change-ledger pass beside the oldest pending age. Its help says a depth above zero across two passes is the intake failing, but the pass cadence is not published as a series, so the rule that sentence describes has no window to be sampled over: the queue's ordinary life is one pass, and every minute-based rendering of 'two passes' would fire on a queue caught mid-turn. The fault the depth would catch is already read from the age by change_ledger_remeasure_stuck, and telling one row the tree cannot settle from a writer that has stopped is a comparison of the two curves over time, which is a panel's job"),
    ),
    // The other half of the pool-wait pair, filed for the opposite reason to the
    // entry above: here the shape is known -- a stamp that outlives the pool's
    // recovery -- and the rule is written over the age. The depth is the size of
    // the population that age is about, and any rule over it would have to state
    // how many owed rows waiting on a pool is too many, which is a fraction of a
    // queue whose total nothing publishes and no policy bounds. A number that
    // rises with the backlog is not a fault while the backlog is legitimate.
    (
        "cogneva_change_dispatch_pool_blocked_rows",
        Unread::Gap("how many outstanding changes are stamped as waiting on the LLM pool, written on every dispatch pass beside the age of the oldest one. The rule reads the age: a stamp that outlives the pool's recovery is a wait nobody is going to release, and one pass is its whole ordinary life, so that shape has a bound the writer's own interval settles. The depth is the population that age is about, and a bound on it would have to say what fraction of an owed queue waiting on a pool is too many -- the queue's size is published per pass but no policy states a level, and the same depth is legitimate while the pool really is down. Read together with the age by a panel, which is the reader it owes. The intake's depth above was filed on the same reasoning from the other side: there the age had a bound and the depth had none. That sentence about the rule was false when it was first written and is true only from the pass that fixed the selection: the rule reached the verdict by a label key the producer never writes, so it matched the empty set and could not fire, while this census reads a reader off the rule's text and kept the seat filled. The key is now `pool_reading`, the one the emitter writes, and the fixture's seven cells run against the real chart"),
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
///
/// It came down to 25 when `llm_pool_open_but_idle` took the `ready` cell of
/// `cogneva_ready_admission_readings` as its workload conjunct. The entry said
/// the cells are the gate working and need a panel rather than a threshold,
/// which is true of each cell alone and false of the combination: work asking
/// to go out while the window pays and nothing is served is the fault this
/// series was minted to make readable, so one of its cells now has a rule. The
/// panel is still owed for the rest of the arithmetic -- this entry's debt is
/// only the part a rule can hold.
///
/// It went back to 26 in the pass that gave the write-window record a series.
/// That series exists because the face owing the reading had no producer at all
/// -- its first reading was a name in the roster and nothing behind it -- so the
/// pass published the four counts before it had a reader, deliberately. The
/// reader owed is a panel in the change-ledger group; a rule would have to
/// declare when an overlap or a self-approval is a fault, and both are policies
/// nobody has stated. The host-side governance face reading selects
/// `readout="write_window_approved"` today, and that reader is outside the rules
/// and panels this census counts, which is why the entry stands rather than
/// pointing at it.
///
/// It went to 29 in the pass that published the principle account's refusal
/// counts by slug. The account had stored the count per row since the gates
/// first reported refusals, and the only surface outside a query was a host
/// script reading the database directly -- outside this repository, so it
/// cannot be named as a reader here. The series closes that half. The half left
/// is a bound, and none exists: a principle refused once is a gate doing its
/// job, the same one refused for a week is a rule the corpus is not
/// internalizing, and the number between them is a policy nobody has stated.
/// Publishing first and filing the reader as a debt is deliberate -- the same
/// order the write-window record used, and for the same reason: a series that
/// waits for its threshold is a series nobody can watch while the threshold is
/// being argued about.
///
/// It went to 30 in the pass that published the change account's census by
/// face. Same shape as the entries above it: the producer counts a population
/// that was always in the rows but only ever in prose, and the bound that would
/// turn any of its cells into a fault is a policy about how much owed work is
/// normal, which nobody has stated. The panel is the reader it owes.
///
/// It went back to 29 in the pass that gave the publisher's holds their panel.
/// That pass also published a new series, `cogneva_board_column_held_total` --
/// the other way a ready task is held back, this one by a project's board
/// column -- and it arrived with the same panel already drawing it, so it added
/// no debt to file. A new series that a fancier publisher would have filed and
/// left is the shape this number punishes; here the two holds are one picture
/// because a reader who saw only the window's would think a starved column was
/// an idle one.
///
/// It went to 32 in the pass that gave the two archives a retention. Each
/// archive publishes its size beside its age, and only the age has a reader:
/// age is what a retention promise is about, while the size follows the write
/// rate and has no declared budget in the tree. Filing the three counts as
/// debts is the same order the write-window record and the principle account
/// used -- publish the reading first, argue about the threshold after -- and it
/// is what makes the growth visible while the policy is still undecided.
///
/// It stayed at 33 in the pass that published the principle account's tier
/// census. That census had been computed on every pass since the tiers were
/// introduced and only ever printed, so it arrived with everything a reading
/// needs except a reader -- and it arrived with one, because the account's
/// composition is one picture: the panel draws both partitions side by side,
/// and the state partition had no curve either, since the freshness rule over
/// it answers "is anyone still counting" and not "what does the count say". A
/// series published with its reader adds no debt, which is what this number is
/// for: it counts the ones that were left.
///
/// It came down to 32 when the principle account's refusal count got its rule.
/// That entry said the missing piece was a declared bound, and the sentence it
/// gave for one was a shape rather than a level: one refusal is a gate doing
/// its job, the same principle refused for a week is a rule the corpus is not
/// internalizing. A level was never going to close it, because the number is a
/// cumulative account value -- a slug refused five hundred times a year ago is
/// not a fault today -- so the rule reads change per day against a coverage
/// clause instead, and the entry left the table in the pass that wrote
/// `principle_refusals_sustained`.
///
/// It went back to 33 in the pass that gave the re-measurement intake its age
/// rule. The intake writes two numbers from the same pass -- the depth of the
/// queue and the age of its oldest row -- and the rule reads the age, because
/// six passes of waiting is a shape the writer's own interval settles. The
/// depth is the one entry here whose help text already names a shape rather
/// than asking for a level, and it is filed anyway: the shape is "two passes",
/// and the pass cadence is not a series, so a rule built from that sentence
/// would have to pick a number of minutes for a queue that legitimately lives
/// exactly one pass. That is a threshold argument, which is what the last
/// paragraph of this note says is the wrong reason to keep a debt and the right
/// reason to file one.
///
/// It went to 34 in the pass that made the pool stamps readable. That pass
/// publishes a pair from the dispatch loop and writes the rule over the age
/// only: the age is the wait, and a stamp that outlives the pool's recovery is
/// a wait nobody is going to release, which is a fault with a bound. The depth
/// is not uninteresting -- it is how much is stranded -- but a rule over it
/// would have to state what share of an owed queue may be waiting on a pool,
/// and that share is as large as the backlog whenever the pool really is down.
/// So the depth files the same way the intake's did one pass earlier, from the
/// other side: there the age had the rule and the depth had no bound the writer
/// could settle; here the depth is the number a level would have to be stated
/// for. Both are read together by the panel the change-ledger group owes.
///
/// Down to 33 in the pass that drew the change account by face. That entry said
/// the reader owed was a panel over the census by block and reading, and the
/// panels now exist -- two of them, one drawing the face's four counts (landed,
/// owed, retired, and `undecided`, which is a part of `owed` rather than a
/// fourth bucket) and one drawing the age of the longest-unmeasured owed row --
/// so the entry leaves the table the way the window entry did: the reader
/// landed, and a series with a reader may not stay filed as having none. What
/// does not change is why no rule takes it: the counts are a population and
/// nothing in the deployment declares how much owed work on one face is a
/// fault, and the age has a direction but not a bound -- a change owed for a
/// day is a queue, one owed for a month on a face that also retired nothing is
/// the face having stopped, and where between those a page belongs is the face
/// owners' call. That sentence is why the census has a reader and no rule, and
/// it is written here rather than in the entry because the entry's whole value
/// was the reader it named.
const GAPS_AT_CENSUS: usize = 33;

#[test]
fn every_series_the_closed_set_publishes_has_a_decided_reader() {
    let rule_read: BTreeSet<String> = shipped_rules()
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

    // A pending name is a declared debt of a different kind: it is waiting for a
    // reader, the band is counted, and it leaves the moment one lands. It is
    // classified, so it does not also owe an UNREAD verdict.
    let listed: BTreeSet<&str> = UNREAD
        .iter()
        .map(|(n, _)| *n)
        .chain(FOREIGN_PENDING.iter().map(|(n, _)| *n))
        .collect();

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
    let any_read: BTreeSet<String> = shipped_rules()
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
    let plugin = read_repo("crates/cog-reflection/src/plugin.rs");
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
    let plugin = read_repo("crates/cog-reflection/src/plugin.rs");
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

    let puller = read_repo("crates/cog-reflection/src/gitops_puller.rs");
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
    // 配平的括号只有一处读法：开头那个 `(` 一直配到末尾，才算一层包装。
    while cur.starts_with('(')
        && cur.ends_with(')')
        && matching_paren(cur, 0, Dialect::Rust) == Some(cur.len() - 1)
    {
        cur = cur[1..cur.len() - 1].trim();
    }
    cur
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
            let Some(inner_end) = matching_paren(expr, inner_start - 1, Dialect::Rust) else {
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
    for (rule, promql) in shipped_rules() {
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
        Some(rest) => match matching_paren(rest, 0, Dialect::Rust) {
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
    for (rule, promql) in shipped_rules() {
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
    for (rule, promql) in shipped_rules() {
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

/// The face drift rule has to fire on every state the enum does not call quiet.
///
/// The verdict travels as a label value, and a rule that names the states to
/// fire on reads a state added later as healthy: the new value matches no
/// branch, the expression returns nothing, and the alert stays silent through
/// exactly the drift that state was added to describe. That is the same
/// silence a stopped publisher produces, which is why this rule names what is
/// quiet instead -- everything that is not `on_target`, minus the yellow
/// states and minus the states another rule answers with a hold.
///
/// A state may leave this rule's range only by being answered somewhere else,
/// and "somewhere else" is checked rather than taken on trust: a rule whose
/// expression selects that one state and whose `for_seconds` is set. `unread`
/// is the state that needs this: it is what a scrape reads while the registry
/// cannot be asked, so a rollout makes every face say it for one beat, and a
/// rule without a hold turns that into one alert per face. A state that is
/// merely added to the exclusion list with no rule behind it fails here.
///
/// Both halves are read from the code that owns them rather than copied here:
/// the tiers come from `DriftState`, the exclusion comes out of the shipped
/// expression. A state added to the enum without a decision fails this test in
/// whichever direction is wrong -- fired on while it is a yellow, covered by a
/// hold that does not exist, or quietly folded into healthy.
#[test]
fn the_face_drift_rule_is_quiet_on_exactly_the_states_the_enum_calls_yellow() {
    let rules = chart_json()["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("observability.infra_watch.rules is not an array")
        .clone();
    let rule = rules
        .iter()
        .find(|r| r["name"] == "governance_face_not_on_target")
        .expect("面漂移规则不见了：没有任何东西会报一个面正在漂");
    let expression = rule["promql"].as_str().unwrap_or_default();

    assert!(
        expression.contains(r#"state!="on_target""#),
        "面漂移规则不再以「不是 on_target」为射程，新增状态会被默认读成健康:\n{expression}"
    );
    let quiet = quoted_label_after(expression, r#"state!~""#).unwrap_or_else(|| {
        panic!("面漂移规则没有点名安静的状态，三个黄档会跟着红档一起报警:\n{expression}")
    });
    let quiet: BTreeSet<&str> = quiet.split('|').collect();

    // A state this rule does not report may be reported by a rule of its own,
    // but only one that holds: `for_seconds` is the only thing that separates
    // "this face cannot be read right now" from "this process has not finished
    // starting".
    let held: BTreeSet<&str> = cog_core::DriftState::ALL
        .iter()
        .map(|state| state.as_str())
        .filter(|name| {
            let selector = format!(r#"state="{name}""#);
            rules.iter().any(|r| {
                r["promql"].as_str().unwrap_or_default().contains(&selector)
                    && r["for_seconds"].as_u64().unwrap_or(0) > 0
            })
        })
        .collect();

    let mut wrong: Vec<String> = Vec::new();
    for state in cog_core::DriftState::ALL {
        let name = state.as_str();
        if state.is_yellow() && !quiet.contains(name) {
            wrong.push(format!(
                "{name} 是黄档，但不在规则点名安静的状态里——它会按红档报警"
            ));
        }
        if !state.is_yellow() && quiet.contains(name) && !held.contains(name) {
            wrong.push(format!(
                "{name} 不是黄档，却被规则当成安静的，而没有任何规则带着保持时长替它报警——它永远不会响"
            ));
        }
        if !state.is_yellow() && !quiet.contains(name) && held.contains(name) {
            wrong.push(format!(
                "{name} 已经在一条带保持时长的规则里，却又被红档规则立刻报——同一件事会响两次"
            ));
        }
    }
    for listed in &quiet {
        let justified = match cog_core::DriftState::parse(listed) {
            Some(state) => state.is_yellow() || held.contains(listed),
            None => false,
        };
        if !justified {
            wrong.push(format!(
                "规则点名的安静状态 {listed} 既不是枚举里的黄档、也没有带保持时长的规则接手——这个名字没人认领"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The quoted value after a marker in a PromQL expression.
fn quoted_label_after<'a>(text: &'a str, marker: &str) -> Option<&'a str> {
    let start = text.find(marker)? + marker.len();
    let rest = &text[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

/// 一条规则从 `rules` 里删掉，它已经开着的行不会自己关：watcher 只为它**现在**持有的
/// 规则求值，被删的那条留下的行停在 `state='firing'`、`resolved_at` 空、`last_seen_at`
/// 冻在删除那一刻。运营问「现在有什么在响」读的正是 `state='firing'` 那一面，于是它跟
/// 一条正在持续的红读起来没有区别，只有把三个时钟并排看才分得出来。`retired_rules` 是
/// 唯一会让那些行被结算的地方（watcher 启动时按普通解析的同一路径关掉，带通知）。
///
/// 2026-10-20 实测：撤掉 `live_image_not_on_mainline` 的那一版滚上现网后，那一行从
/// 08:20:55Z 起持续 firing，`last_seen_at` 停在 08:47:04（旧 Pod 的最后一拍），新 Pod
/// 起来之后没有任何东西再看它——名字没写进名单，它就永久挂在告警面上。
#[test]
fn a_retired_rule_is_named_where_the_watcher_can_settle_it() {
    let path = repo_root().join(CHART_CONFIG);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("chart config unreadable at {}: {e}", path.display()));
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("chart config is not valid JSON");
    let watch = &value["observability"]["infra_watch"];

    let retired: BTreeSet<&str> = watch["retired_rules"]
        .as_array()
        .expect("observability.infra_watch.retired_rules is not an array")
        .iter()
        .filter_map(|name| name.as_str())
        .collect();

    // 「名单与 rules 不相交」那半由 `deploy/scripts/check-alert-rule-subjects.sh` 判，
    // 这里不抄第二份。这一条判的是它判不到的那半：一条**已经**从 rules 里删掉的规则，
    // 名字有没有被登记下来。删掉本身不留痕迹，所以只能逐条钉住这一族撤过的规则。
    assert!(
        retired.contains("live_image_not_on_mainline"),
        "live_image_not_on_mainline 已从 rules 里删掉，却不在 retired_rules 里：\
         它留下的 firing 行没有任何东西会去结算，会一直挂在「现在有什么在响」那一面上"
    );
}

/// Readings this workspace publishes **once per process, about a subject that
/// is not the process**.
///
/// The distinction decides what the alert ledger keys on. Every replica that
/// reads a stream publishes its own `read_silent_seconds` for its own loops,
/// and the labels it publishes carry the stream and the consumer group — not
/// the pod. So the pod on the scraped series names whoever answered that
/// scrape, which is exactly what `NON_IDENTITY_LABELS` keeps out of the dedup
/// key for the scrape's own `job`/`endpoint`/`service`: identity for this
/// reading is the name, and the reporter is noise.
///
/// Left in the key, it forks one incident into one row per replica and opens a
/// fresh fire/resolve pair on every rollout. Measured on 2026-10-23 over seven
/// days: `stream_consumer_stalled` wrote 379 rows, 379 distinct keys, 65 pods,
/// 78 stream/group names — 79 keys once the reporter is off the key, and half
/// the rows (`resolved_at - fired_at < 60s`) were that churn rather than an
/// incident that came and went.
///
/// The list is a design statement, not something a scan reads off the source,
/// which is why it is written here by hand. It is deliberately narrow: most
/// `cogneva_*` rules legitimately identify a pod (`orphans_unreaped` is one row
/// per pod that failed to reap its own orphans), so a rule like "no app metric
/// may be selected bare" would flag sixty correct rules and be turned off.
const PUBLISHED_PER_REPLICA: &[&str] = &[
    "cogneva_stream_read_silent_seconds",
    "cogneva_stream_read_failures_total",
    "cogneva_stream_read_loops",
];

/// The aggregation directly around the metric name at `pos`, as
/// `(function, modifier, its label list)`, or `None` when the name is selected
/// bare.
///
/// Only the shape PromQL allows here is recognised — `agg (name)`,
/// `agg by (…) (name)`, `agg without (…) (name)` — because that is the shape a
/// rule over these readings takes. Anything else is reported as bare, which is
/// the conservative answer: it keeps the check from passing a rule it could not
/// read.
fn aggregation_around(expr: &str, pos: usize) -> Option<(String, Option<String>, Option<String>)> {
    let bytes = expr.as_bytes();
    let mut i = pos;
    let backtrack = |mut i: usize| {
        while i > 0 && bytes[i - 1].is_ascii_whitespace() {
            i -= 1;
        }
        i
    };
    i = backtrack(i);
    if i == 0 || bytes[i - 1] != b'(' {
        return None;
    }
    i = backtrack(i - 1);
    let mut list = None;
    if i > 0 && bytes[i - 1] == b')' {
        let mut depth = 1usize;
        let mut j = i - 1;
        while j > 0 {
            j -= 1;
            match bytes[j] {
                b')' => depth += 1,
                b'(' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }
        if depth != 0 {
            return None;
        }
        list = Some(expr[j + 1..i - 1].to_string());
        i = backtrack(j);
    }
    let word_at = |mut i: usize| {
        let end = i;
        while i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
            i -= 1;
        }
        (i, expr[i..end].to_string())
    };
    let (start, word) = word_at(i);
    if word == "by" || word == "without" {
        let (_, function) = word_at(backtrack(start));
        if function.is_empty() {
            return None;
        }
        return Some((function, Some(word), list));
    }
    if word.is_empty() {
        return None;
    }
    Some((word, None, list))
}

/// Whether the metric selected at `pos` has the reporter aggregated away.
///
/// `agg(name)` drops every label, so it is clean; `agg by (…)` is clean as long
/// as `pod`, `instance` and `container` are not in the list; `agg without (…)`
/// is clean only when `pod` is. What is not clean is a bare selector — `None`
/// from [`aggregation_around`], or an aggregation whose label list keeps the
/// reporter.
fn reporter_aggregated_away(expr: &str, pos: usize) -> bool {
    let Some((_function, modifier, list)) = aggregation_around(expr, pos) else {
        return false;
    };
    let Some(modifier) = modifier else {
        // `max(NAME)`: every label is dropped.
        return true;
    };
    let list = list.unwrap_or_default();
    let keeps = |label: &str| list.split(',').map(str::trim).any(|l| l == label);
    match modifier.as_str() {
        "without" => keeps("pod"),
        _ => !(keeps("pod") || keeps("instance") || keeps("container")),
    }
}

/// Every occurrence of `name` in `expr` that is not aggregated away.
fn bare_occurrences(expr: &str, name: &str) -> Vec<usize> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(found) = expr[from..].find(name) {
        let pos = from + found;
        // A prefix of a longer identifier is not a selector of this name.
        let after_is_ident = expr[pos + name.len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
        if !after_is_ident && !reporter_aggregated_away(expr, pos) {
            out.push(pos);
        }
        from = pos + name.len();
    }
    out
}

/// A rule that reads a per-replica reading keeps the reporter out of the alert
/// instance.
///
/// The failure it exists for is not the reading being wrong — it is the ledger
/// filling with rows that name a pod instead of the thing that broke. A face
/// whose rows are one per replica cannot answer "what is broken" without the
/// reader first collapsing them by hand, and a rollout closes and reopens every
/// row it touches.
#[test]
fn a_reading_published_by_every_replica_is_not_keyed_on_the_reporter() {
    // The detector has to tell the two shapes apart, or the check below passes
    // on a rewritten rule that put the reporter back.
    let keyed_on_the_reporter =
        "cogneva_stream_read_silent_seconds > scalar(12 * max(cogneva_stream_read_block_seconds))";
    assert!(
        !bare_occurrences(keyed_on_the_reporter, "cogneva_stream_read_silent_seconds").is_empty()
    );
    let aggregated = "max without (pod, container, instance) \
                      (cogneva_stream_read_silent_seconds) > 0";
    assert!(bare_occurrences(aggregated, "cogneva_stream_read_silent_seconds").is_empty());
    let by_the_name = "max by (stream, consumer_group) (cogneva_stream_read_loops)";
    assert!(bare_occurrences(by_the_name, "cogneva_stream_read_loops").is_empty());
    // `by` that still carries the reporter is the same defect spelled the other
    // way round, and must not read as clean.
    let by_the_pod = "max by (stream, pod) (cogneva_stream_read_loops)";
    assert!(!bare_occurrences(by_the_pod, "cogneva_stream_read_loops").is_empty());

    let mut checked = 0usize;
    let mut wrong: Vec<String> = Vec::new();
    for (rule, expr) in shipped_rules() {
        for name in PUBLISHED_PER_REPLICA {
            let positions = bare_occurrences(&expr, name);
            if expr.contains(name) {
                checked += 1;
            }
            for pos in positions {
                let start = pos.saturating_sub(40);
                wrong.push(format!(
                    "{rule}: 选择 {name} 时没有把上报者聚合掉 —— {}…",
                    &expr[start..pos + name.len()]
                ));
            }
        }
    }
    assert!(
        checked > 0,
        "没有一条规则在读 {PUBLISHED_PER_REPLICA:?}，这条判据什么也没判 —— 名单该跟着读数走"
    );
    assert!(
        wrong.is_empty(),
        "这些规则按上报它的 Pod 给告警实例分键，一件事会被拆成每个副本一行、每次滚动再开一对:\n{}",
        wrong.join("\n")
    );
}

/// 界与带共用一个常数：`alert_bound_band_reaches_bound` 用 0.81 判「占用带已经爬到界上」，
/// `container_memory_near_limit` 用同一个 0.81 判「容器 rss 超过界」。它们是同一个数在两个
/// 问题上的两个身份——分开改，前一个就不再回答「界该不该动」，而后者会在带已经压上来之后
/// 继续把新常态报成压力（2026-10-23 的实测）。
///
/// 两向：任何一边单独改，这条测试红。这是这一对常数唯一的锁；带的生产者不在本仓，
/// 它读的是哪三条序列由 `FOREIGN` 那几行点名，形状由本测试第二段兜——分子少了涨幅，
/// 或者分母换成了别的量，这条规则读的就不是那条带了。
#[test]
fn the_bound_under_the_band_is_the_same_number_as_the_bound_above_it() {
    let chart = chart_json();
    let rules = chart["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("observability.infra_watch.rules is not an array");
    let find = |name: &str| -> &serde_json::Value {
        rules
            .iter()
            .find(|rule| rule["name"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("{name} 不在规则表里"))
    };
    let band = find("alert_bound_band_reaches_bound");
    let container = find("container_memory_near_limit");
    let band_bound = band["condition"]["greater_than"]
        .as_f64()
        .expect("alert_bound_band_reaches_bound 的界不是数");
    let container_bound = container["condition"]["greater_than"]
        .as_f64()
        .expect("container_memory_near_limit 的界不是数");
    assert_eq!(
        band_bound, container_bound,
        "带与界必须是同一个常数：band={band_bound} container={container_bound}"
    );

    let expr = band["promql"].as_str().expect("表达式不是字符串");
    for series in [
        "cogneva_alert_bound_band_birth_max_bytes",
        "cogneva_alert_bound_band_move_max_bytes",
        "cogneva_alert_bound_band_limit_bytes",
    ] {
        assert!(expr.contains(series), "表达式里缺 {series}：{expr}");
    }
    assert!(expr.contains(") / "), "必须除以声明上限：{expr}");
}
