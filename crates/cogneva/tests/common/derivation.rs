//! Which series are copies of a store row and which accumulate in their own
//! process, so that an aggregation can be judged by what it is adding.
//!
//! This lives beside the contract tests rather than in the metric registry:
//! the registry is a cross-crate contract that every crate rebuilds, and the
//! only readers of this table are the two contracts below and the live probe.
//! A metric that a production path ever needs to classify can move up then.

/// How a series' value comes to exist, which decides whether a reading that
/// adds several processes' copies is a total or a multiple.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MetricDerivation {
    /// The value is rendered from a row of the store. Every process that can
    /// reach the store reports the same number for the same reason a mirror
    /// shows the same face: it is one fact seen twice. Adding the copies
    /// multiplies the fact instead of counting events.
    Store,
    /// The value accumulated in this process from its own events. Two
    /// processes report different numbers because different events happened to
    /// them, so adding them is the total.
    Process,
}

/// Series whose derivation has been decided, and how.
///
/// An aggregation is the one operation whose answer turns on this distinction:
/// `sum` is right for [`MetricDerivation::Process`] and wrong for
/// [`MetricDerivation::Store`], where `max` — or any single-process read — is
/// the same fact once. So this is not a census of every series; it is the set
/// of names a shipped query aggregates, plus the names a reading has already
/// paid to classify. A query that aggregates a name absent from this list is
/// refused by the contract test, because nobody has said which answer is right
/// and an unclassified sum is an answer chosen by accident.
///
/// The `Store` entries were measured, not guessed: on 2026-10-19 both live
/// processes were scraped and a counter is listed here when the two reported
/// byte-identical non-zero totals, which is what two renderings of one store
/// row looks like (53 of the 95 shared counter families did). `Process` holds
/// the aggregated names the census found differing between processes, plus the
/// gateway-only counters whose producer increments them in its own process;
/// those are exposed by one process today, so a second one would report its own
/// events rather than a copy of the first. The list is not a substitute for the
/// measurement: a series can join the store-derived set later, and the reader
/// that notices is the live probe, which compares every declared series across
/// the processes and reds when a name declared `Process` is reported
/// byte-identically by all of them.
pub const METRIC_DERIVATIONS: &[(&str, MetricDerivation)] = &[
    ("cogneva_alert_notifications_total", MetricDerivation::Store),
    (
        "cogneva_buildah_store_freed_bytes_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_buildah_store_pruned_images_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_buildah_store_pruned_layers_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_buildah_store_rounds_total",
        MetricDerivation::Store,
    ),
    ("cogneva_change_fate_total", MetricDerivation::Store),
    // Rendered from the store's cumulative total like the other counters:
    // every process that can reach the store reports the same number, and the
    // rule that reads it takes `max` over the copies.
    (
        "cogneva_change_ledger_measure_unsettled_total",
        MetricDerivation::Store,
    ),
    ("cogneva_change_tier_total", MetricDerivation::Store),
    (
        "cogneva_dag_stalled_scheduled_deferred_for_backlog_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_dag_stalled_scheduled_reclaimed_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_dag_stalled_scheduled_reclaimed_with_backlog_total",
        MetricDerivation::Store,
    ),
    // Counted in the deployer's own process, from revisions it saw land and
    // never aimed at. There is one deployer per cluster today, and if a second
    // one ever runs, each would count the revisions its own rounds missed --
    // their own events, not a copy of a store row -- so adding them is the
    // total.
    (
        "cogneva_deployer_revs_skipped_total",
        MetricDerivation::Process,
    ),
    ("cogneva_event_bus_consumed_total", MetricDerivation::Store),
    ("cogneva_event_bus_dropped_total", MetricDerivation::Store),
    ("cogneva_event_bus_emitted_total", MetricDerivation::Store),
    ("cogneva_event_bus_handoff_total", MetricDerivation::Store),
    ("cogneva_event_bus_lagged_total", MetricDerivation::Store),
    ("cogneva_knowledge_retrieval_total", MetricDerivation::Store),
    (
        "cogneva_landing_ci_failure_inherited_total",
        MetricDerivation::Store,
    ),
    ("cogneva_landing_failures_total", MetricDerivation::Store),
    ("cogneva_loki_flush_total", MetricDerivation::Store),
    (
        "cogneva_mainline_ci_no_verdict_reason_total",
        MetricDerivation::Store,
    ),
    ("cogneva_mainline_ci_verdict_total", MetricDerivation::Store),
    (
        "cogneva_mainline_poll_cycles_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_mainline_rollout_attempts_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_mainline_rollout_completed_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_mainline_superseded_rollout_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_mainline_supersession_checks_total",
        MetricDerivation::Store,
    ),
    ("cogneva_memory_decay_total", MetricDerivation::Store),
    (
        "cogneva_memory_dropped_entries_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_memory_schema_repair_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_observability_event_persist_failed_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_partition_maintenance_failures_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_policy_evolution_rounds_total",
        MetricDerivation::Store,
    ),
    ("cogneva_project_kind_total", MetricDerivation::Store),
    (
        "cogneva_ready_admission_held_total",
        MetricDerivation::Store,
    ),
    ("cogneva_redrive_refusals_total", MetricDerivation::Store),
    (
        "cogneva_registry_maintenance_runs_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_registry_pruned_tags_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_registry_rebuild_hold_secs_total",
        MetricDerivation::Store,
    ),
    ("cogneva_skill_hot_reload_total", MetricDerivation::Store),
    ("cogneva_supervisor_pass_total", MetricDerivation::Store),
    ("cogneva_task_checkpoint_total", MetricDerivation::Store),
    (
        "cogneva_tool_output_archive_failed_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_tool_output_truncated_chars_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_tool_output_truncated_total",
        MetricDerivation::Store,
    ),
    (
        "cogneva_version_contract_checks_total",
        MetricDerivation::Store,
    ),
    ("collab_change_diff_source_total", MetricDerivation::Store),
    (
        "evolution_generated_change_files_faithful",
        MetricDerivation::Store,
    ),
    (
        "evolution_generated_change_files_total",
        MetricDerivation::Store,
    ),
    (
        "evolution_generated_change_hunks_faithful",
        MetricDerivation::Store,
    ),
    (
        "evolution_generated_change_hunks_total",
        MetricDerivation::Store,
    ),
    ("http_requests_total", MetricDerivation::Store),
    ("memory_operations_total", MetricDerivation::Store),
    ("self_evolution_change_yield_total", MetricDerivation::Store),
    ("tier_migration_total", MetricDerivation::Store),
    ("cogneva_change_funnel", MetricDerivation::Store),
    // Row counts read out of the shared task graph: every pod reports the same
    // number, so an aggregate over them multiplies one fact by the number of
    // reporters. `max` is the only aggregate that keeps the number honest.
    ("cogneva_task_unowned_rows", MetricDerivation::Store),
    // The principle account's two censuses: row counts the pass reads out of
    // one table and republishes whole, so whichever process holds the pass
    // renders the same numbers. A panel that adds the copies counts the corpus
    // once per publisher, which is why its expressions reduce to one.
    ("cogneva_principle_ledger_rows", MetricDerivation::Store),
    (
        "cogneva_principle_ledger_check_kind_rows",
        MetricDerivation::Store,
    ),
    ("llm_upstream_healthy", MetricDerivation::Store),
    // Today's spend and the age of that sum are written through the same durable
    // funnel as `llm_upstream_healthy`: every process renders the store's newest
    // row, so a `sum` over the copies would count the same fact once per
    // process. `max` is the aggregate that keeps the number.
    ("llm_budget_spent_tokens", MetricDerivation::Store),
    ("llm_budget_measured_at_unix", MetricDerivation::Store),
    (
        "cogneva_evolution_learning_recurrence_at_threshold_total",
        MetricDerivation::Process,
    ),
    (
        "cogneva_evolution_build_outcomes_total",
        MetricDerivation::Process,
    ),
    (
        "cogneva_mirror_push_failures_total",
        MetricDerivation::Process,
    ),
    ("cogneva_notification_sign_total", MetricDerivation::Process),
    (
        "cogneva_registry_prune_failures_total",
        MetricDerivation::Process,
    ),
    (
        "cogneva_resource_events_dropped_total",
        MetricDerivation::Process,
    ),
    ("collab_declared_scale_total", MetricDerivation::Process),
    ("collab_route_decisions_total", MetricDerivation::Process),
    ("audited_llm_requests_total", MetricDerivation::Process),
    ("llm_calls_total", MetricDerivation::Process),
    ("llm_tokens_total", MetricDerivation::Process),
    (
        "llm_upstream_client_errors_total",
        MetricDerivation::Process,
    ),
    ("llm_upstream_failures_total", MetricDerivation::Process),
    ("llm_usage_readings_total", MetricDerivation::Process),
];

/// How `name` is derived, when it has been decided. See [`METRIC_DERIVATIONS`].
pub fn metric_derivation(name: &str) -> Option<MetricDerivation> {
    METRIC_DERIVATIONS
        .iter()
        .find(|(declared, _)| *declared == name)
        .map(|(_, derivation)| *derivation)
}
