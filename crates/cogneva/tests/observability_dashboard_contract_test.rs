//! The Grafana dashboard reads series and labels by name. Nothing else does.
//!
//! A panel whose series nothing produces, or whose legend names a label the
//! series does not carry, renders as an empty panel or a legend of blanks —
//! and an empty panel reads exactly like "the system is idle". Nothing about
//! editing either side makes the other notice, so the two only drift, in one
//! direction: the dashboard keeps promising readings the producer stopped
//! supplying. This test is the thing that notices.
//!
//! The table below states the contract for the series this workspace produces.
//! Series from outside it (cAdvisor's `container_*`) are not covered: their
//! label sets belong to the kubelet, not to this repository.
//!
//! principle: feedback_a_gate_keyed_on_the_artifacts_purpose_needs_a_fail_closed_type_table
//! principle: feedback_a_judge_reading_only_its_own_green_corpus_cannot_be_shown_to_red

use std::collections::BTreeSet;

#[path = "common/closed_set.rs"]
mod closed_set;
#[path = "common/producer.rs"]
mod producer;
#[path = "common/promql.rs"]
mod promql;

#[path = "common/dashboard.rs"]
mod dashboard;

use dashboard::{
    is_log_query, json_field, metric_exprs, repo_root, text as dashboard_text, PanelExpr,
};
use producer::carries_the_producer;
use promql::{metric_names_in, shape_complaints, uses_an_aggregation, AGGREGATIONS};

/// Series this workspace produces: the labels each one carries, and the file
/// that publishes it.
///
/// The labels are recorded at the call sites: `http_requests_total` /
/// `http_request_duration_ms` share one label map, as do `llm_calls_total` /
/// `llm_call_latency_ms`. The file is read and searched for the name, so a
/// producer that was renamed or removed fails the test instead of licensing a
/// panel that can never draw anything.
///
/// The third column is what makes the first two a contract rather than a
/// preference: it is the series' own producer, not another reader of it.
const PRODUCED: &[(&str, &[&str], &str)] = &[
    (
        "http_requests_total",
        &["method", "endpoint", "status"],
        "crates/cog-gateway/src/lib.rs",
    ),
    (
        "http_request_duration_ms",
        &["method", "endpoint", "status"],
        "crates/cog-gateway/src/lib.rs",
    ),
    (
        "llm_calls_total",
        &["upstream", "model", "result", "actor"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_call_latency_ms",
        &["upstream", "model", "result", "actor"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The per-call consumption record, split by the actor that spent it and by
    // the kind of token. The `kind` axis carries `input`, `output` and `cached`;
    // `cached / input` is the hit rate of the upstream's own prefix cache, which
    // is the only reading a prefix-stability change can be accepted on -- "the
    // stable half is byte-identical and first" is a claim about bytes the
    // upstream served, not about bytes we sent. It had no reader until this
    // panel: per-actor latency was drawn, per-actor spend was not, so the
    // reading that says where the money goes existed in the producer alone. The
    // ratio is read per upstream and protocol (on an OpenAI-compatible upstream
    // `cached` is a subset of `input`, on Anthropic it is disjoint), which is
    // why `kind` stays a separate series rather than being folded into a net
    // number at the producer.
    (
        "llm_tokens_total",
        &["upstream", "kind", "actor"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // Agent run counters carry no labels: they are totals for the process, not
    // per-task readings, so nothing distinguishes one sample from the next.
    (
        "agent_success_count",
        &[],
        "crates/cog-agent/src/observable.rs",
    ),
    (
        "agent_budget_exhausted_count",
        &[],
        "crates/cog-agent/src/observable.rs",
    ),
    // The iteration ceiling is derived per role, so which role is being cut off
    // is the question; a single unlabelled total could not answer it.
    (
        "agent_iteration_budget_exhausted",
        &["role"],
        "crates/cog-agent/src/observable.rs",
    ),
    // Of those cut-offs, the ones that ended with no answer at all. A sibling
    // series rather than a label on the one above, because it answers a
    // different question: the total counts the runs the forced final ask
    // salvaged too, so on it a role whose ceiling is too low and a role whose
    // work simply is not answering read the same. This is the one that says
    // which of a role's cut-offs bought nothing.
    (
        "agent_iteration_budget_exhausted_no_deliverable",
        &["role"],
        "crates/cog-agent/src/observable.rs",
    ),
    // Pool recovery readings. Three separate series rather than one "recovers
    // at" number: which upstream said it, when we will probe again, and how long
    // the window an upstream stated is are answers of different strength, and
    // collapsing them is how a probe cadence gets read as a promise.
    (
        "llm_pool_evidenced_recovery_unix",
        &[],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_pool_next_attempt_unix",
        &[],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_pool_quota_window_secs",
        &[],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "cogneva_verification_budget_seconds",
        &["kind"],
        "crates/cog-reflection/src/verification_budget.rs",
    ),
    (
        "cogneva_verification_last_run_seconds",
        &["kind"],
        "crates/cog-reflection/src/verification_budget.rs",
    ),
    (
        "cogneva_verification_timeouts_total",
        &["kind"],
        "crates/cog-reflection/src/verification_budget.rs",
    ),
    (
        "llm_upstream_quota_window_secs",
        &["upstream"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_upstream_quota_reset_unix",
        &["upstream"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_upstream_consecutive_failures",
        &["upstream"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The order a call walks the pool in, which is not the declared one: a
    // stable sort on suspicion sinks a failed upstream behind everything that
    // has not failed, so the pool converges on the declared order only once the
    // last healthy one is gone. The declared order arrives in a Secret at
    // process start, so without this series a reorder is confirmed by reading
    // the Secret and a pod-start time and calling it done -- a claim about the
    // manifest, not about the process the requests are actually walking.
    (
        "llm_upstream_position",
        &["upstream"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The host-wide build bound. Every reading carries the slot directory it is
    // about, because two processes that point at different directories bound
    // only themselves and nothing in the numbers would say so, and the role that
    // published it, because a process that runs no builds publishes the same
    // series at zero on purpose.
    (
        "cogneva_build_gate_slots",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    // What the machine could take, against what is being admitted. Its own row
    // because the difference between the two is the whole reading: the ceiling
    // carries the dimension that is setting it, so a bound held below a busy
    // disk and a bound that is simply the size of the machine do not look alike.
    (
        "cogneva_build_gate_ceiling",
        &["dir", "role", "dimension"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_in_flight",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_waiting",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    // The two gauges drawn beside the bound. Neither states how much of the
    // bound this process is using -- occupied counts the slot files locked in
    // the shared directory, so it is every builder on the host, and excluded is
    // 1 while a holder that is not a build is keeping builds out. They are on a
    // panel rather than behind a rule because exclusion is the gate working,
    // and how long a cache sweep may hold the field has no derivable answer.
    (
        "cogneva_build_gate_occupied",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_excluded",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_acquired_total",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_refused_total",
        &["dir", "role", "reason", "asker"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_wait_ms_total",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_wait_ms_max",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_held_ms_total",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_held_ms_max",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_wait_budget_ms",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    // What each change's release build cost the host. Every reading carries the
    // change kind, because the decision these panels feed is which kind of
    // change to spend the next build on, and the outcome, because a deploy
    // budget kill and a build that ended on its own are not the same purchase.
    (
        "cogneva_evolution_build_seconds_total",
        &["intent", "outcome"],
        "crates/cog-reflection/src/evolution_build_readings.rs",
    ),
    (
        "cogneva_evolution_build_last_seconds",
        &["intent"],
        "crates/cog-reflection/src/evolution_build_readings.rs",
    ),
    (
        "cogneva_evolution_build_outcomes_total",
        &["intent", "outcome"],
        "crates/cog-reflection/src/evolution_build_readings.rs",
    ),
    // The corpus-accumulation reading: every consultation of a recurrence
    // threshold, bucketed by the count it was consulted on. A distribution
    // pinned in the `1` bucket is a corpus that is not accumulating -- the same
    // defect that let the store hold `1` for three weeks with every reading
    // green. The `gate` axis is what keeps the two thresholds from being
    // averaged: the hook gate sees every detected learning, the change gate only
    // the code-defect half, and they act at different counts.
    (
        "cogneva_evolution_learning_recurrence_at_threshold_total",
        &["gate", "recurrence"],
        "crates/cog-reflection/src/learning_recurrence_readings.rs",
    ),
    // What the shared build cache holds. The layer label is what makes the
    // reading actionable -- dropping the compiled layers is a cold rebuild and
    // dropping the scratch ones is not -- and the dir label is what keeps two
    // caches behind one path from being summed into one number.
    (
        "cogneva_build_target_bytes",
        &["dir", "layer"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // The size says how much a layer holds; this says how much of it a build
    // rewrote inside the window. A build reusing its cache moves the count
    // without moving the size, so the two together are what tells a warm build
    // from one that had to rebuild -- and the window is read beside the count,
    // because a bare number says how many files moved and not over how long.
    (
        "cogneva_build_target_rewritten_files",
        &["dir", "layer"],
        "crates/cog-core/src/build_cache.rs",
    ),
    (
        "cogneva_build_target_rewrite_window_seconds",
        &["dir"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // The age of the reading above. Published separately because a failed walk
    // keeps the last size, so this is the only series that can tell a cache
    // nobody writes to from a scan loop that stopped.
    (
        "cogneva_build_target_bytes_scan_age_seconds",
        &["dir"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // What the cap is, so a panel can draw the cache against it rather than
    // beside it. The source label says where the number came from -- a cap the
    // deployment pinned reads `override`, one a measurement loop derived reads
    // `derived` -- so a reader can tell a cap that tracks the volume from one
    // that has stopped without reading the deployment's config beside it. The
    // line is published only while a cap is in force; a cache with no cap
    // publishes no cap line at all.
    (
        "cogneva_build_target_bytes_cap",
        &["dir", "source"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // The part of what is held that the cap says should not be there.
    (
        "cogneva_build_target_over_limit_bytes",
        &["dir"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // The part of that a pass could not reach. A cap that cannot be met reads
    // here and nowhere else -- the plan reaching no further is
    // indistinguishable from a cap nothing crossed.
    (
        "cogneva_build_target_unmet_bytes",
        &["dir"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // What passes actually freed, and when one last did. The count is what
    // separates a cap holding because it is enforced from one holding because
    // nothing grew; the timestamp is what tells a pass that stopped apart from
    // a cache that needs no pass.
    (
        "cogneva_build_target_reclaimed_bytes_total",
        &["dir"],
        "crates/cog-core/src/build_cache.rs",
    ),
    (
        "cogneva_build_target_last_reclaim_seconds",
        &["dir"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // How long the cache has been over its cap, as a run of walks. A different
    // question from the timestamp above, which ages whether or not the cache is
    // over anything: a cache brought under its cap and left there for an hour
    // earns a long age by being fine, where this one stays at zero.
    (
        "cogneva_build_target_over_cap_seconds",
        &["dir"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // Every over-cap pass, split by how it ended. The outcome label is the
    // whole reading: reclaimed, unmet, busy and ungated are four different
    // reasons no bytes moved, and folded into one count they would read as one.
    (
        "cogneva_build_target_over_cap_total",
        &["dir", "outcome"],
        "crates/cog-core/src/build_cache.rs",
    ),
    // The background loop census and its liveness. The loop label is the only
    // label these carry: the same loop runs in every process that enables its
    // plugin, and Prometheus's own instance label is what separates those lines,
    // so no legend here names it.
    (
        "cogneva_loop_tick_age_seconds",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_period_seconds",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_registered",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_deaths_total",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    // Restarts are on the same panel as deaths because the whole point of the
    // pair is reading them together: a loop that panicked and came back shows
    // restarts alone, one that used up its budget shows both, and a loop that
    // returned on its own shows deaths alone.
    (
        "cogneva_loop_restarts_total",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    // The single-writer roles, drawn as one panel because none of the four
    // answers its own question: the holder cell cannot say the work moved, and
    // the two counters cannot say who is holding it now. The books panel draws
    // the arithmetic signed, which the rule cannot: a comparison hands back the
    // left operand's value, so the rule reads the absolute value and the
    // direction of a mismatch is only readable here.
    (
        "cogneva_loop_owner_held",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_acquisitions_total",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_losses_total",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    (
        "cogneva_loop_owner_probe_failures_total",
        &["loop"],
        "crates/cog-core/src/loop_health.rs",
    ),
    // Orphans collected, against the zombies still waiting. The gauge is what
    // the rule `orphans_unreaped` watches and it can only say a reaper stopped:
    // a process that reaps everything and one that reaps nothing both show a
    // zero. The counter is the half no threshold carries -- that reaping is
    // happening at all -- and it has no bound to alert on, because what a host
    // leaves behind follows the work it does.
    (
        "cogneva_orphans_reaped_total",
        &[],
        "crates/cog-observability/src/process_zombies.rs",
    ),
    (
        "cogneva_process_zombies",
        &[],
        "crates/cog-observability/src/process_zombies.rs",
    ),
    // A consumed stream's backlog, split into what a reclaim pass would already
    // have taken back and the rest. Drawn together because one number cannot
    // tell a busy queue from a stuck one, and it is a panel rather than a rule
    // because the size of a backlog is the workload's own shape -- the one
    // reading here a threshold can be built on is the age of the oldest
    // unreclaimed entry, and that rule exists.
    (
        "cogneva_stream_pending_count",
        &["stream"],
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_unreclaimed_count",
        &["stream"],
        "crates/cog-orchestrator/src/observable.rs",
    ),
    // The two cells of one host slice's memory, drawn beside each other because
    // the rule in this family reads the anonymous one only: anonymous memory
    // does not fall on its own while page cache does, so a floor on the anon
    // cell means something a floor on their sum would not -- and a slice under
    // pressure from cache and one under pressure from a process read alike on
    // any total.
    (
        "cogneva_host_slice_file_bytes",
        &["slice", "node"],
        "crates/cog-observability/src/host_ledger.rs",
    ),
    (
        "cogneva_host_slice_anon_bytes",
        &["slice", "node"],
        "crates/cog-observability/src/host_ledger.rs",
    ),
    // What the self-discovery watcher does with each round's signals. No panel
    // draws these yet -- they are published so that "the watcher has gone quiet"
    // has a reading at all, and they are recorded here so their names and their
    // producer stay pinned together until something does draw them.
    //
    // The outcome label is the reading: a signal refused as a duplicate, one
    // held back by its own cooldown, and one whose submission never reached the
    // orchestrator all submit nothing, and folded into one count they would read
    // exactly like a system with nothing wrong. The tick count is the
    // denominator they are read against -- without it, a watcher that stopped
    // ticking and a watcher finding nothing are the same zero.
    (
        "cogneva_signal_watcher_running",
        &[],
        "crates/cog-reflection/src/signal_readings.rs",
    ),
    (
        "cogneva_signal_watcher_ticks_total",
        &[],
        "crates/cog-reflection/src/signal_readings.rs",
    ),
    // The other half of that pair: while the pool gate has the watcher standing
    // down, the tick count keeps moving, because a denominator that froze would
    // read like a loop that died. This is the series that counts the rounds it
    // stood down for, at the tick count's own cadence, so `ticks - held` is the
    // rounds that actually looked and a panel can tell a pause from a quiet
    // system without inferring it from a counter's silence.
    (
        "cogneva_signal_watcher_held_total",
        &[],
        "crates/cog-reflection/src/signal_readings.rs",
    ),
    (
        "cogneva_signal_watcher_signals_total",
        &["outcome"],
        "crates/cog-reflection/src/signal_readings.rs",
    ),
    // The report-cooldown store's size, and what the reclaimer took out of it.
    // No panel draws these yet either. They are here for the same reason as the
    // three above, and one more that is theirs alone: the store only ever grew
    // before the reclaimer existed, and its size is the only reading that would
    // have shown that -- the cooldown count says how many keys are holding a
    // signal back, not how many keys there are.
    (
        "cogneva_signal_watcher_guard_entries",
        &[],
        "crates/cog-reflection/src/signal_readings.rs",
    ),
    (
        "cogneva_signal_watcher_guard_reclaimed_total",
        &[],
        "crates/cog-reflection/src/signal_readings.rs",
    ),
    // How each round of the discovery loop's probes ended, one series per exit.
    // No panel draws these yet, and they are here for the reason the watcher
    // readings above are: the loop has always counted these and the counts
    // reached a reader exactly once, through one `info!` line, which cannot be
    // looked at over time. The re-measure recipe standing next to the probe
    // deadline is written in terms of how these move -- whether `still_open`
    // keeps standing above zero, whether `row_missing` ever has a value -- so
    // until a face draws them that recipe has nothing to read.
    //
    // One series per exit and never a total, because folded together "every
    // probe was answered" and "every probe came back without an answer" print
    // the same number, and the second one is the finding: it means the loop has
    // no judge of its own and is reading whatever the generic task path
    // returned. The last two are the pair the recipe leans on -- `still_open` is
    // the probes still on the books, and it is read against the round's width
    // rather than on its own, so the two are filed together; a probe that has
    // never been counted anywhere is what `not_recorded` is for, and a verdict
    // that was read but could not be written down is the one failure that would
    // otherwise leave the other eight looking clean.
    (
        "cogneva_discovery_probe_validated",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    (
        "cogneva_discovery_probe_rejected",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    (
        "cogneva_discovery_probe_inconclusive",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    (
        "cogneva_discovery_probe_unanswered",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    (
        "cogneva_discovery_probe_run_ended_without_result",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    (
        "cogneva_discovery_probe_row_missing",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    (
        "cogneva_discovery_probe_expired",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    (
        "cogneva_discovery_probe_still_open",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    (
        "cogneva_discovery_probe_not_recorded",
        &[],
        "crates/cog-reflection/src/discovery_outcome_readings.rs",
    ),
    // How much of a volume is in use: the claim label is what the alert rule
    // divides by the declared size. Every producer of it walks the volume, so the
    // panel is one wall per claim rather than one wall per kind of reader. A
    // process that can only ask a store's API cannot answer this -- it sees what
    // the store references, not what occupies the disk -- and publishing that
    // narrower number here is how the claim read 37% while it was 74% full.
    (
        "cogneva_data_volume_used_bytes",
        &["persistentvolumeclaim"],
        "crates/cog-core/src/claim_footprint.rs",
    ),
    // The other half of that comparison: what the registry's tags reference. No
    // panel draws it yet -- it is published so the gap to the series above has a
    // reading at all, which is what a retention policy has to drive down, and it
    // is recorded here so its name and its producer stay pinned together until
    // something does draw it.
    (
        "cogneva_registry_referenced_bytes",
        &["persistentvolumeclaim"],
        "crates/cog-reflection/src/registry_footprint.rs",
    ),
    // The registry store's own pair: what it holds and how long ago it was
    // measured. The age carries no labels and neither does the count -- there
    // is one store per cluster, so which store is Prometheus's own instance
    // label rather than a legend this panel could name.
    (
        "cogneva_registry_tag_count",
        &[],
        "crates/cog-reflection/src/registry_footprint.rs",
    ),
    (
        "cogneva_registry_scan_age_seconds",
        &[],
        "crates/cog-reflection/src/registry_footprint.rs",
    ),
    // The tool-output fidelity pair. All three are recorded from one site, the
    // archive handle that decides what became of a cut, which is why the count
    // and its character total cannot disagree about their labels and why the
    // failure family is what the landed-share line subtracts.
    (
        "cogneva_tool_output_truncated_total",
        &["tool"],
        "crates/cog-agent/src/archive.rs",
    ),
    (
        "cogneva_tool_output_truncated_chars_total",
        &["tool"],
        "crates/cog-agent/src/archive.rs",
    ),
    (
        "cogneva_tool_output_archive_failed_total",
        &["tool", "cause"],
        "crates/cog-agent/src/archive.rs",
    ),
    (
        "cogneva_memory_dropped_entries_total",
        &["namespace"],
        "crates/cog-memory/src/metrics_instrumented.rs",
    ),
    (
        "cogneva_memory_extraction_payloads_total",
        &["outcome"],
        "crates/cog-memory/src/observable.rs",
    ),
    // The three readings of a stream consumer's read health, published per
    // stream and consumer group. They are drawn together because none of them
    // decides anything alone: the silence says whether the bus is moving, the
    // refusals say what keeping up cost with the cause the bus named, and the
    // population says how many loops are behind the name -- the two loops that
    // a name covers are invisible in the other two, which is why neither of
    // them can be read as a statement about one consumer.
    (
        "cogneva_stream_read_loops",
        &["stream", "consumer_group"],
        "crates/cog-stream/src/observable.rs",
    ),
    (
        "cogneva_stream_read_silent_seconds",
        &["stream", "consumer_group"],
        "crates/cog-stream/src/observable.rs",
    ),
    (
        "cogneva_stream_read_failures_total",
        &["stream", "consumer_group", "reason"],
        "crates/cog-stream/src/observable.rs",
    ),
    // The promotion channel's weekly tally: how many ledger entries of one ISO
    // week ended each way, so `promoted` alone can be read against the four
    // cells beside it. It is drawn rather than alarmed on -- the in-process
    // stall condition already judges the same window, and a rule here would be
    // a strictly later, blunter statement of it, written into the same alerts
    // table from an uncoupled carrier.
    (
        "cogneva_promotion_week_changes",
        &["week", "outcome"],
        "crates/cog-reflection/src/promotion_trend.rs",
    ),
];

/// Series the dashboard may read that this workspace does not produce, with the
/// owner. Listing them keeps the coverage check honest: an unlisted foreign
/// series fails the test rather than being silently ignored.
const FOREIGN: &[(&str, &str)] = &[
    ("container_cpu_usage_seconds_total", "kubelet/cAdvisor 采集"),
    (
        "container_memory_working_set_bytes",
        "kubelet/cAdvisor 采集",
    ),
    (
        "container_memory_rss",
        "kubelet/cAdvisor 采集（21 号面板与告警规则 container_memory_near_limit 用同一个分子，面板与规则不许漂开：分子是不可回收的那部分，工作集里可回收的页缓存不算）",
    ),
    ("kube_pod_container_resource_limits", "kube-state-metrics"),
    (
        "kube_pod_container_status_restarts_total",
        "kube-state-metrics",
    ),
    (
        "kube_pod_container_status_last_terminated_reason",
        "kube-state-metrics",
    ),
    ("ALERTS", "Prometheus 由告警规则合成的序列"),
    // The declared size of a claim: no process of ours publishes it, because
    // the number lives in the PVC object, and kube-state-metrics is the only
    // thing that reads it. Drawn beside the measured bytes rather than summed
    // into them, since a volume over its declaration is a judgement about the
    // two together.
    (
        "kube_persistentvolumeclaim_resource_requests_storage_bytes",
        "kube-state-metrics",
    ),
];

/// Label names a legend template asks for: every `{{...}}` in the format.
fn legend_labels_in(legend: &str) -> BTreeSet<String> {
    legend
        .match_indices("{{")
        .filter_map(|(start, _)| {
            let rest = &legend[start + 2..];
            rest.find("}}").map(|end| rest[..end].trim().to_string())
        })
        .collect()
}

#[test]
fn every_series_the_dashboard_reads_is_one_something_produces() {
    let text = dashboard_text();
    let produced: BTreeSet<&str> = PRODUCED.iter().map(|(n, _, _)| *n).collect();
    let foreign: BTreeSet<&str> = FOREIGN.iter().map(|(n, _)| *n).collect();

    let mut unknown: BTreeSet<String> = BTreeSet::new();
    for panel in metric_exprs(&text) {
        for name in metric_names_in(&panel.expr) {
            if produced.contains(name.as_str())
                || foreign.contains(name.as_str())
                // A classic histogram is exposed as three series derived from the
                // one name the producer records: `_bucket`, `_sum`, `_count`, and
                // the store renders an `_observed_timestamp_seconds` companion
                // beside every series it serves. Which derivations count is
                // decided once, in the module both readers share, rather than
                // here -- spelling it in each reader is what let a `_bucket` be
                // accepted in a panel and rejected in a rule.
                || closed_set::a_series_this_build_publishes(&name)
            {
                continue;
            }
            unknown.insert(name);
        }
    }

    assert!(
        unknown.is_empty(),
        "面板读了本仓库不产出、也没登记为外来序列的名字: {unknown:?}\n\
         要么把它接上产出面，要么把它记进 FOREIGN 并写明属主"
    );
}

/// A panel whose expression does not parse draws nothing, exactly like a panel
/// whose series nothing produces — and the check above passes it, because every
/// series in it is really produced.
///
/// This is not a parser, and the name says so rather than claiming more: it
/// recognises a finite set of shapes, each of them verified against a live
/// Prometheus to be rejected outright -- an operator dropped between two
/// operands that a matching clause then sits in front of, and the bracket on
/// the wrong side of the guard, a range selector handed to a set operator.
///
/// A shape outside that set gets past this gate, and the panel side has no
/// backstop behind it: nothing evaluates a panel, so a shape this does not know
/// stays a blank panel until a person notices. The rule side of the same
/// question has the watcher's eval-failure self-alert behind it instead.
#[test]
fn no_panel_expression_has_a_known_unparseable_shape() {
    let mut complaints: Vec<String> = Vec::new();
    for panel in metric_exprs(&dashboard_text()) {
        for complaint in shape_complaints(&panel.expr) {
            complaints.push(format!(
                "第 {} 行: {complaint}\n    {}",
                panel.line, panel.expr
            ));
        }
    }

    assert!(
        complaints.is_empty(),
        "面板表达式含一种已知的、Prometheus 一定拒绝的形状，面板会一直是空的:\n{}",
        complaints.join("\n")
    );
}

/// Panels whose whole use is to show one number, and panels whose use is to
/// show one line per series. Named, both of them, because which list a panel
/// kind belongs in is the judgement the check below rests on, and a table that
/// silently covers a kind it was never told about would be a check that reads
/// a new panel as though it had been decided.
const SINGLE_VALUE_PANELS: &[&str] = &["stat", "singlestat", "gauge", "bargauge", "piechart"];
const PER_SERIES_PANELS: &[&str] = &["timeseries", "logs", "table", "heatmap"];

/// A single-value panel handed an expression over a metric with more than one
/// series shows one series and hides the rest: `sum(ALERTS{...})` and
/// `ALERTS{...}` render the same 44-pixel box, and only the first says
/// "how many alerts are firing". The second says "18" because 18 happens to be
/// one of the pods, and the panel has no way to look wrong.
///
/// The check is over the panel's kind, not over an expression: whether
/// several series are meant to be compared is a question only the panel answers,
/// and 50 of the 84 expressions here are right to have no aggregation.
#[test]
fn every_single_value_panel_collapses_the_series_it_reads() {
    let panels = metric_exprs(&dashboard_text());
    assert!(
        !panels.is_empty(),
        "一条表达式都没读到：门禁会空转，先查抽取器对这个文件还看不看得懂"
    );

    let complaints = single_value_complaints(&panels);
    assert!(
        complaints.is_empty(),
        "单值面板与被比较的序列对不上（聚合算子：{}）:\n{}",
        AGGREGATIONS.join("`/`"),
        complaints.join("\n")
    );
}

/// The complaints the check above raises, over panels handed in rather than
/// read.
///
/// Split out because the file this repository has cannot show the check
/// working: every one of its panels is right, so a version of the check that
/// reds on nothing -- or a predicate that answers `true` to everything -- reads
/// exactly the same. The test below feeds it the shapes it exists to catch.
fn single_value_complaints(panels: &[PanelExpr]) -> Vec<String> {
    let mut complaints: Vec<String> = Vec::new();
    for panel in panels {
        let kind = panel.panel_type.as_str();
        if SINGLE_VALUE_PANELS.contains(&kind) {
            if !uses_an_aggregation(&panel.expr) {
                complaints.push(format!(
                    "第 {} 行: 面板类型 `{kind}` 只显示一个值，而这条表达式没有聚合算子 —— \
                     一个序列一个值，面板会显示其中一条、其余看不见\n    {}",
                    panel.line, panel.expr
                ));
            }
        } else if !PER_SERIES_PANELS.contains(&kind) {
            complaints.push(format!(
                "第 {} 行: 面板类型 `{kind:?}` 不在任何一栏里 —— 要么它整块是一个值\
                 （加进 SINGLE_VALUE_PANELS 并给出聚合），要么它一条线一个序列\
                 （加进 PER_SERIES_PANELS 具名豁免）\n    {}",
                panel.line, panel.expr
            ));
        }
    }
    complaints
}

/// The three ways this check can be satisfied wrongly, and the one way it can
/// be refused wrongly.
#[test]
fn a_single_value_panel_that_does_not_collapse_its_series_is_reported() {
    let panel = |kind: &str, expr: &str| PanelExpr {
        line: 7,
        panel_type: kind.to_string(),
        expr: expr.to_string(),
    };
    let reported = |kind: &str, expr: &str| single_value_complaints(&[panel(kind, expr)]).len();

    // 该红的三种：单值面板没有聚合算子；面板类型没归栏（含抽取器没读到 `type`）；
    // 名字像单值但不在表里的新面板类型。
    assert_eq!(reported("stat", "ALERTS{alertstate=\"firing\"}"), 1);
    assert_eq!(reported("", "sum(x)"), 1);
    assert_eq!(reported("piechart2", "sum(x)"), 1);
    // 不该红的两种：带从句的聚合是聚合；多序列面板本来就不欠聚合。
    assert_eq!(reported("stat", "sum by (pod) (rate(x_total[5m]))"), 0);
    assert_eq!(reported("timeseries", "rate(x_total[5m])"), 0);
}

/// The other half of the same claim, and the half no other check can make: the
/// table says something records these series, and that is the file it names.
///
/// The check above only compares the panel against the table, and the panel and
/// the table are both edited by whoever changes the dashboard. A producer
/// renamed in its own crate moves neither, so every check here stays green while
/// the panel draws nothing at all — the empty panel this file exists to catch,
/// arrived at from the side nothing was watching. The name in the table is the
/// only place that claim lives, so it is the only place that can be held to it.
#[test]
fn every_series_the_table_claims_is_produced_is_named_by_its_producer() {
    let root = repo_root();
    let mut missing: Vec<String> = Vec::new();
    for (name, _, source) in PRODUCED {
        let text = std::fs::read_to_string(root.join(source))
            .unwrap_or_else(|e| panic!("{source} unreadable: {e}"));
        if !carries_the_producer(&text, name) {
            missing.push(format!("{name} 记在 {source}，该文件里找不到这个名字"));
        }
    }

    assert!(
        missing.is_empty(),
        "PRODUCED 表登记的产出点已经不存在，面板上的这个序列会永远画不出东西:\n{}",
        missing.join("\n")
    );
}

/// The same hole the alert-rule contract has, on the same two tables: this
/// file's producer check walks PRODUCED only, so a series filed as foreign
/// stops being looked for in its producer and no other check asks either. See
/// `common/closed_set.rs` for why the closed set is the reading that decides
/// it, and why the reverse direction cannot be decided by text.
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
fn every_label_a_legend_names_is_on_the_series_it_describes() {
    let text = dashboard_text();
    let labels_of = |name: &str| -> Option<&'static [&'static str]> {
        PRODUCED
            .iter()
            .find(|(n, _, _)| *n == name)
            .map(|(_, labels, _)| *labels)
    };

    // Panels pair one expr with one legend within the same object; walking the
    // file in order and keeping the most recent expr is enough for a manifest
    // that holds one target per panel.
    let mut current: Option<String> = None;
    let mut mismatches: Vec<String> = Vec::new();
    for line in text.lines() {
        if let Some(expr) = json_field(line, "expr") {
            current = if is_log_query(&expr) {
                None
            } else {
                metric_names_in(&expr).into_iter().next()
            };
        } else if let Some(legend) = json_field(line, "legendFormat") {
            let (Some(metric), Some(expected)) = (
                current.as_ref(),
                labels_of(current.as_ref().unwrap().as_str()),
            ) else {
                continue;
            };
            for label in legend_labels_in(&legend) {
                if !expected.contains(&label.as_str()) {
                    mismatches.push(format!(
                        "{metric} 的 legend 引用了它没有的标签 {{{{{label}}}}}; 实际标签: {expected:?}"
                    ));
                }
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "面板图例引用了序列上不存在的标签，图例会渲染成空白:\n{}",
        mismatches.join("\n")
    );
}

/// The `data:` entries of the dashboard ConfigMap as `(key, value)`.
///
/// Read line by line rather than through a YAML parser: the structure this
/// contract needs is one block scalar per key, and a parser would be a
/// dependency carried by every test in this crate for it.
fn configmap_data_blocks(text: &str) -> Vec<(String, String)> {
    let mut blocks = Vec::new();
    let mut lines = text.lines().peekable();
    let mut in_data = false;
    while let Some(line) = lines.next() {
        if line == "data:" {
            in_data = true;
            continue;
        }
        if !in_data {
            continue;
        }
        if !line.starts_with(' ') {
            in_data = false;
            continue;
        }
        let Some((key, _)) = line.trim().split_once(": |") else {
            continue;
        };
        let indent = line.len() - line.trim_start().len();
        let mut value = String::new();
        while let Some(next) = lines.peek() {
            if !next.trim().is_empty() && next.len() - next.trim_start().len() <= indent {
                break;
            }
            let body = lines.next().unwrap_or_default();
            // The block scalar holds whatever is indented past the key; cutting
            // the same amount off every line is what makes it parse as JSON.
            value.push_str(body.get(indent + 2..).unwrap_or(""));
            value.push('\n');
        }
        blocks.push((key.trim().to_string(), value));
    }
    blocks
}

/// A dashboard a ConfigMap ships has to be one Grafana will load.
///
/// Grafana's file provider reads each file as the panel model itself. The API
/// payload shape that wraps the model in `{"dashboard": ..., "overwrite": true}`
/// parses as JSON and reads as a model with an empty title, so Grafana drops
/// the dashboard and logs one line — every panel in it silently stops existing
/// while the manifest still looks like a dashboard to everyone reading the
/// repository. That is the failure this test exists for: the manifest is the
/// only artifact a reviewer sees, and it was wrong in exactly this way.
#[test]
fn every_dashboard_a_configmap_ships_is_one_grafana_can_load() {
    let text = dashboard_text();
    let blocks = configmap_data_blocks(&text);
    assert!(
        !blocks.is_empty(),
        "机读不到 data 里的面板 JSON，这份门禁会变成空转"
    );

    for (key, value) in blocks {
        let model: serde_json::Value = serde_json::from_str(&value)
            .unwrap_or_else(|e| panic!("{key} 不是合法 JSON，Grafana 也读不了: {e}"));
        assert!(
            model.get("dashboard").is_none() && model.get("overwrite").is_none(),
            "{key} 是 API 的 dashboard/overwrite 包装；\
             文件供给器要的是面板模型本身，包装会让它读到空标题并整张丢弃"
        );
        let title = model.get("title").and_then(|t| t.as_str()).unwrap_or("");
        assert!(!title.is_empty(), "{key} 没有 title，Grafana 会拒绝加载");
        let panels = model
            .get("panels")
            .and_then(|p| p.as_array())
            .unwrap_or_else(|| panic!("{key} 没有 panels 数组"));
        // Grafana 按 id 定位面板，两块面板共用一个 id 只会渲染出一块。
        let mut ids = BTreeSet::new();
        for panel in panels {
            let id = panel
                .get("id")
                .and_then(|i| i.as_i64())
                .unwrap_or_else(|| panic!("{key}: 面板没有 id: {panel}"));
            assert!(ids.insert(id), "{key}: 面板 id {id} 重复");
            let panel_title = panel.get("title").and_then(|t| t.as_str()).unwrap_or("");
            assert!(!panel_title.is_empty(), "{key}: 面板 {id} 没有标题");
            let queries = panel
                .get("targets")
                .and_then(|t| t.as_array())
                .unwrap_or_else(|| panic!("{key}: 面板 {id} 没有 targets，是块装饰"));
            assert!(!queries.is_empty(), "{key}: 面板 {id} 一条查询都没有");
        }
    }
}

const PANELS_JUDGING_A_SERIES_THIS_BUILD_PUBLISHES: usize = 0;

/// 带阈值、且阈值画在这棵树自己产出的序列上的面板，有几块。
///
/// 这个数**不是判词，是读数**，钉成常数是为了让「这个总体第一次非空」变成一次红灯，
/// 而不是一件要有人记得去看的事。
///
/// 它守的边界是：一块面板可以把它自己的判据（`fieldConfig.defaults.thresholds.steps`
/// 的档）画在某根序列上，而一条规则在同一根序列上按 promql 里的比较判——两边给出
/// **不同的界**，今天没有任何东西会红。这条边界的第一版谓词（「面板读的序列不许同时
/// 出现在规则的 promql 里」）被反例普查打死：同读一根是**正当布局**（规则侧 122 个名字、
/// 面板侧 58 个、交集 20，20/20 都正当），照它落门会恒红。订正后的谓词改为判**判断**
/// 而不是判**对象**（面板的档 vs 规则的比较），于是可判的前提是**面板侧的判据要被读出来**
/// ——而今天没有读者：`dashboard::metric_exprs` 只走 `expr`，不读 `thresholds`。
///
/// 所以这条测试先把那个读者建出来：它替「届时要写的那条禁令」产出**输入**。今天这个数
/// 是 0，因为唯一带阈值的那块面板读的是 `sum(ALERTS{alertstate="firing"})`，不是本进程
/// 能写出的任何一根序列 ⇒ 这个危害今天没有主体（在没有主体的地方写禁令就是写一条恒绿的
/// 门，那是记账不是判据）。第一次非空的那天，这条边界才第一次可判，要写的是禁令本身。
#[test]
fn a_panel_that_judges_a_series_this_build_publishes_is_counted() {
    let text = dashboard_text();
    let blocks = configmap_data_blocks(&text);
    let mut judging: Vec<String> = Vec::new();
    let mut panels_seen = 0usize;
    for (key, value) in blocks {
        let model: serde_json::Value = serde_json::from_str(&value)
            .unwrap_or_else(|e| panic!("{key} 不是合法 JSON，面板读不出来: {e}"));
        let Some(panels) = model.get("panels").and_then(|p| p.as_array()) else {
            continue;
        };
        for panel in panels {
            panels_seen += 1;
            let judged: BTreeSet<String> = panel
                .get("targets")
                .and_then(|t| t.as_array())
                .map(|targets| {
                    targets
                        .iter()
                        .filter_map(|t| t.get("expr").and_then(|e| e.as_str()))
                        // 日志查询按 label 选、不点序列名，抽取器会把 `$var` 读成名字。
                        .filter(|expr| !is_log_query(expr))
                        .flat_map(metric_names_in)
                        .filter(|n| closed_set::a_series_this_build_publishes(n))
                        .collect()
                })
                .unwrap_or_default();
            if judged.is_empty() {
                continue;
            }
            // `steps` 的第一档是 Grafana 的底色（`value: null`），不是判据的界；有 `steps`
            // 的块就算带判据，档数只用来报数。
            let steps = panel
                .pointer("/fieldConfig/defaults/thresholds/steps")
                .and_then(|s| s.as_array())
                .map(Vec::len)
                .unwrap_or(0);
            if steps == 0 {
                continue;
            }
            let title = panel
                .get("title")
                .and_then(|t| t.as_str())
                .unwrap_or("<无标题>");
            judging.push(format!(
                "{key} / {title}：{steps} 档，画在 {}",
                judged.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    assert!(
        panels_seen > 0,
        "一块面板都没读到，这条读数会变成空转（面板 JSON 的解析面换了形状？）"
    );
    assert_eq!(
        judging.len(),
        PANELS_JUDGING_A_SERIES_THIS_BUILD_PUBLISHES,
        "带阈值、且阈值画在这棵树自己产出的序列上的面板数变了（现在 {}，常数 {}）：\n{}\n\
         第一次非空 ⇒ 这条边界第一次可判，该写的是禁令（一块面板的档不许与同一根序列上某条\n\
         规则的比较给出不同的界），而不是继续钉着这个数；变少 ⇒ 那是把判据从面板上撤了，\n\
         同样要说明为什么。",
        judging.len(),
        PANELS_JUDGING_A_SERIES_THIS_BUILD_PUBLISHES,
        judging.join("\n")
    );
}
