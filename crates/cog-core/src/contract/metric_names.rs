//! The closed set of series names the metric store may hold.
//!
//! A name reaches the store through [`crate::MetricsBackend::record_gauge`] and
//! its two siblings. Those take a [`MetricName`] rather than a `&str` so that
//! every name a build can write is one of the constants below: the compiler
//! refuses anything else, which is what makes [`ALL`] a complete answer to
//! "which series can this build still produce?".
//!
//! That question is what a stored series' liveness is judged by. The store is
//! the only durable witness of a producer that has been deleted: when a series
//! is dropped from the code and nobody adds its name to
//! [`crate::RETIRED_METRIC_NAMES`], the rows stay, nothing writes them, and a
//! frozen value is read downstream as "no traffic" rather than "this series is
//! gone". Nothing in the source tree can see that — the producer is gone, so
//! the name has nothing left to be searched for — so the judgement has to
//! compare the store's held names against this list at the one place both are
//! known, which is the exposition. A free-form name would defeat it in the
//! direction that hurts: a producer writing a name this list does not know
//! would be reported as having no producer.
//!
//! The list covers the store, not the scrape. Series rendered from an
//! `Observable`'s [`crate::RawMetric`] are published by the collecting process
//! and vanish with their producer, so they have no rows to leave behind.
//!
//! Scope, so that a future writer does not have to guess: a name belongs here
//! when a process can hand it to a metrics backend implementation, whatever
//! that implementation does with it (the in-process registry backend included).
//! Where a name is *produced* stays in the crate that produces it, and is
//! re-exported from here so the call site reads as before.

use std::fmt;

/// A series name the metric store may hold.
///
/// Constructed only by the constants in this module, so a name outside
/// [`ALL`] cannot be written by accident or by a string built at runtime.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct MetricName(&'static str);

impl MetricName {
    /// The wire form, for bindings, queries and label values.
    pub const fn as_str(self) -> &'static str {
        self.0
    }

    /// A name that is deliberately not in [`ALL`].
    ///
    /// Exists for tests that have to exercise what the exposition does with a
    /// series no build can produce — which is exactly the reading this module
    /// makes possible, and which cannot be set up through a registered name.
    /// Production code has no use for it: a series whose name lives only at a
    /// call site is the failure this module exists to prevent.
    #[doc(hidden)]
    pub const fn for_tests_only(name: &'static str) -> Self {
        Self(name)
    }
}

impl fmt::Display for MetricName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::ops::Deref for MetricName {
    type Target = str;

    fn deref(&self) -> &str {
        self.0
    }
}

/// Declares the registry: one constant per name, plus [`ALL`] over the same
/// list.
///
/// The three are generated together so they cannot drift — a list written by
/// hand next to the constants would be a second producer surface, and the
/// reading it feeds is only as complete as its worst half.
macro_rules! metric_names {
    ($($ident:ident => $name:literal,)+) => {
        $(
            #[doc = concat!("`", $name, "`")]
            pub const $ident: MetricName = MetricName($name);
        )+

        /// Every name a build can write to a metrics backend.
        pub const ALL: &[MetricName] = &[$($ident),+];

        /// The registry as `(identifier, series)` pairs.
        ///
        /// A file that publishes a series names the constant, not the wire
        /// form, so a check that reads such a file has to turn the binding back
        /// into the series it writes. That is what this view is for: it is
        /// generated from the same list as the constants, so it cannot report a
        /// binding the constants do not have.
        pub const IDENTIFIED: &[(&str, MetricName)] = &[$( (stringify!($ident), $ident) ),+];
    };
}

metric_names! {
    // cog-github — change funnel, landing and redrive budgets.
    CHANGE_FUNNEL => "cogneva_change_funnel",
    CHANGE_FATE_TOTAL => "cogneva_change_fate_total",
    LANDING_FAILURES_TOTAL => "cogneva_landing_failures_total",
    LANDING_CI_FAILURE_INHERITED_TOTAL => "cogneva_landing_ci_failure_inherited_total",
    MIRROR_PUSH_FAILURES_TOTAL => "cogneva_mirror_push_failures_total",
    REDRIVE_REFUSALS_TOTAL => "cogneva_redrive_refusals_total",
    REDRIVE_BUDGET_LOSSES_TOTAL => "cogneva_redrive_budget_losses_total",

    // cog-collaboration — where a self-evolution run's yield ended, and which
    // end the round's change artifact came from.
    //
    // Both are written by the collecting process, so they belong here rather
    // than only in an `Observable`'s rollup: a run that produced nothing and a
    // model that typed the diff instead of taking it from the checkout are
    // facts about the loop's output, and a reading of them that dies with the
    // pod cannot answer "has this been happening all along".
    //
    // The yield counter is labelled by a closed set rather than split per
    // cause, for the reason the build store's round counter is: a reader who
    // has to union six series to ask "did any run end" is reading one question
    // as six unrelated facts. The absence of a cell means that cause has not
    // happened yet, which the zero it replaces could not say -- a cell that was
    // never written and a cell written as zero are the same number and
    // different facts.
    SELF_EVOLUTION_CHANGE_YIELD_TOTAL => "self_evolution_change_yield_total",
    COLLAB_CHANGE_DIFF_SOURCE_TOTAL => "collab_change_diff_source_total",

    // cog-memory — operation counters, latency, and ingest reconciliation.
    MEMORY_OPERATIONS_TOTAL => "memory_operations_total",
    MEMORY_OPERATION_LATENCY_MS => "memory_operation_latency_ms",
    MEMORY_OPERATION_ERRORS_TOTAL => "memory_operation_errors_total",
    MEMORY_UNEXTRACTED_RAW => "memory_unextracted_raw",
    MEMORY_UNEXTRACTED_RAW_AGED_OUT => "memory_unextracted_raw_aged_out",
    MEMORY_UNEXTRACTED_SCAN_FAILED => "memory_unextracted_scan_failed",
    // The stock of sources the ingest path has permanently given up on: each
    // one left the backlog and will never be re-driven. It is the stock that
    // pairs with the `dlq_written` operation cell, which counts only arrivals
    // -- without it nothing states how much has accumulated, and a count that
    // only ever rises is invisible on the rate.
    MEMORY_DEAD_LETTER_RAW => "memory_dead_letter_raw",
    // cog-memory — how each pass of the decay maintenance loop ended, labelled
    // by `outcome`, one cell per namespace swept per pass. See the HELP for the
    // closed set and why the cell is a pass rather than an entry count.
    MEMORY_DECAY_TOTAL => "cogneva_memory_decay_total",
    // cog-memory — entries that left the searchable layer, by namespace. The
    // pass counter beside it answers whether the loop ran; this one answers how
    // much memory the loop actually gave up, which is the quantity a decay
    // policy is chosen for and the only one that says whether a quiet stretch
    // of passes was a loop finding nothing to drop or a drop rate too small to
    // matter.
    MEMORY_DROPPED_ENTRIES_TOTAL => "cogneva_memory_dropped_entries_total",

    // cog-storage — the metric store's own readings and tier migration.
    METRICS_RETIRED_ROWS_REMOVED => "metrics_retired_rows_removed",
    METRICS_RETIREMENT_RELEASE_FAILED => "metrics_retirement_release_failed",
    METRICS_SAMPLES_OVER_CAPACITY => "metrics_samples_over_capacity",
    METRICS_SAMPLES_BYTES => "metrics_samples_bytes",
    METRICS_SAMPLES_BUDGET_ROWS => "metrics_samples_budget_rows",
    METRICS_SAMPLES_ROWS => "metrics_samples_rows",
    METRICS_SAMPLE_SWEEP_FAILED => "metrics_sample_sweep_failed",
    TIER_MIGRATION_TOTAL => "tier_migration_total",

    // cog-storage — the observability event log's own readings.
    //
    // `cog_observability_events` is append-only and nothing ever removed a row
    // from it, so its size was the platform's uptime times its event rate --
    // 2.92 M rows / 1.6 GB on 2026-10-27, and every one of them in the nightly
    // backup whose restore the drill has to finish inside a budget. These four
    // cells are what makes the log's growth visible from the scrape instead of
    // only through a query against the database: what it holds, what it may
    // hold, whether the read window is what stops it, and what that costs on
    // disk.
    OBSERVABILITY_EVENTS_ROWS => "cogneva_observability_events_rows",
    OBSERVABILITY_EVENTS_BUDGET_ROWS => "cogneva_observability_events_budget_rows",
    OBSERVABILITY_EVENTS_OVER_CAPACITY => "cogneva_observability_events_over_capacity",
    OBSERVABILITY_EVENTS_BYTES => "cogneva_observability_events_bytes",
    OBSERVABILITY_EVENT_SWEEP_FAILED => "cogneva_observability_event_sweep_failed",

    // cog-storage — the checkpoint store's capacity: `cog_snapshots` held
    // 54,452 rows over 924 MB on 2026-10-27, of which 39,610 rows — 580 MB
    // measured the same day — no task board pointed at: the last checkpoint of
    // every finished task, kept for good because nothing deletes a board. Same
    // four cells and
    // the same pass-outcome cell as the two logs above, for the same reason:
    // what it holds, what it may hold, whether the floor is what stops it, what
    // that costs on disk, and whether the pass itself answered.
    CHECKPOINTS_ROWS => "cogneva_checkpoints_rows",
    CHECKPOINTS_BUDGET_ROWS => "cogneva_checkpoints_budget_rows",
    CHECKPOINTS_OVER_CAPACITY => "cogneva_checkpoints_over_capacity",
    CHECKPOINTS_BYTES => "cogneva_checkpoints_bytes",
    CHECKPOINT_SWEEP_FAILED => "cogneva_checkpoint_sweep_failed",

    // cog-storage — the per-task event archive's retention.
    //
    // `cog_events` files each iteration of a task's agent loop so a run that
    // did not converge can be read back afterwards, and nothing has ever
    // removed a row from it: 130,871 rows over 211 MB on 2026-10-27, growing by
    // about 3,800 rows a day, all of it in the nightly backup whose restore the
    // drill has to finish inside a budget. Retirement works on whole streams
    // rather than rows, because both readers replay a task's events from offset
    // zero and a deleted prefix would look like the start of the run.
    //
    // The age of the oldest row is the retention seen from the reader's side,
    // and the retention is published beside it so the rule compares two
    // readings instead of a constant written into the rules file. The stream
    // count is here because rows alone cannot tell one long run from a thousand
    // short ones, and a pass removes whole streams.
    TASK_EVENTS_ROWS => "cogneva_task_events_rows",
    TASK_EVENTS_TASKS => "cogneva_task_events_tasks",
    TASK_EVENTS_OLDEST_AGE_SECONDS => "cogneva_task_events_oldest_age_seconds",
    TASK_EVENTS_KEEP_SECONDS => "cogneva_task_events_keep_seconds",
    TASK_EVENTS_SWEEP_FAILED => "cogneva_task_events_sweep_failed",

    // cog-observability — the prompt-and-response archive's retention.
    //
    // `gateway_llm_exchange` keeps the bodies of what the platform sent
    // upstream and what came back, so the size of the archive is set by what
    // people paste into a prompt rather than by how often they call. Its
    // retention was enforced only at the gateway's startup — a bound on a row's
    // life while the gateway is rolled more often than the retention is wide,
    // which is an assumption about deployment habits and not something the
    // table can state. Measured on 2026-10-27, on this deployment: 307 MB at
    // 9,680 rows inside a thirty-day window.
    //
    // The age of the oldest row is the retention seen from the reader's side,
    // and the retention the process enforces is published beside it so the rule
    // compares two readings instead of a constant written into the rules file.
    // The bytes are what the nightly backup pays for; they are not a bound,
    // because PostgreSQL returns a DELETE's space only when it vacuums.
    LLM_EXCHANGE_ROWS => "gateway_llm_exchange_rows",
    LLM_EXCHANGE_OLDEST_AGE_SECONDS => "gateway_llm_exchange_oldest_age_seconds",
    LLM_EXCHANGE_KEEP_SECONDS => "gateway_llm_exchange_keep_seconds",
    LLM_EXCHANGE_BYTES => "gateway_llm_exchange_bytes",
    LLM_EXCHANGE_SWEEP_FAILED => "gateway_llm_exchange_sweep_failed",

    // cog-observability — the token ledger's retention, and what it may not
    // retire.
    //
    // `gateway_llm_usage` is one row per upstream call; nothing ever removed a
    // row from it, and the count follows call volume, which has no ceiling of
    // its own.
    // Measured on 2026-10-27 on this deployment: 312,803 rows over 32 days, 80 MB.
    //
    // Age alone cannot be the cutoff. The hourly fold is a *derivation* of this
    // ledger, so a row no window covered is the only copy of that spend:
    // deleting it would leave the fold reporting the hour as quiet forever,
    // with nothing to recompute it from. The sweep therefore reads both halves
    // — past the retention, and below the fold's own floor — and the pair
    // `uncovered_rows` / `uncovered_oldest_age_seconds` is what says the second
    // half has stopped holding: rows exist that the retention cannot touch, and
    // they are already past the age the deployment promised. A count without
    // the age cannot tell a fold catching up by an hour from a ledger that has
    // been unbounded for a month.
    LLM_USAGE_ROWS => "cogneva_llm_usage_rows",
    LLM_USAGE_OLDEST_AGE_SECONDS => "cogneva_llm_usage_oldest_age_seconds",
    LLM_USAGE_KEEP_SECONDS => "cogneva_llm_usage_keep_seconds",
    LLM_USAGE_UNCOVERED_ROWS => "cogneva_llm_usage_uncovered_rows",
    LLM_USAGE_UNCOVERED_OLDEST_AGE_SECONDS => "cogneva_llm_usage_uncovered_oldest_age_seconds",
    LLM_USAGE_SWEEP_FAILED => "cogneva_llm_usage_sweep_failed",

    // cog-storage — the monthly partition maintenance loop's per-round outcome.
    //
    // The loop keeps every time-series parent's window open ahead of the clock
    // and its DEFAULT partition empty; a table with no partition covering a
    // row's key rejects the insert and takes down whatever carried it. Its
    // *liveness* already has a reader -- it is on the supervised cadence,
    // `cogneva_loop_tick_age_seconds{loop="storage_partition_maintenance"}`
    // stops advancing when the task is gone, and background_loop_stalled says
    // so. What had no reader was the loop *doing its job* and coming up short:
    // a step that failed, and a table whose DEFAULT partition was found holding
    // rows (the window fell behind), both went to a `warn!` that dies with the
    // pod -- so a round that failed every hour and an instance with nothing to
    // open are the same picture on every reading. This is that missing face,
    // one cell per cause, labelled by `reason`.
    //
    // A cell is written only when its cause occurs. The closed set is declared
    // in the producing module, so a reader enumerates it and reads an absent
    // cell as zero; seeding four zeros into the store every round would spend
    // durable rows -- in the very sample log whose capacity is itself read and
    // bounded -- to restate a set the code already states.
    PARTITION_MAINTENANCE_FAILURES_TOTAL => "cogneva_partition_maintenance_failures_total",

    // cog-reflection — workspace index and generated-change fidelity.
    WORKTREE_INDEX_PRESENT => "cogneva_worktree_index_present",
    WORKTREE_INDEX_MISSING_FILES => "cogneva_worktree_index_missing_files",
    WORKTREE_INDEX_SAMPLE_INCOMPLETE => "cogneva_worktree_index_sample_incomplete",
    EVOLUTION_GENERATED_CHANGE_FILES_TOTAL => "evolution_generated_change_files_total",
    EVOLUTION_GENERATED_CHANGE_FILES_FAITHFUL => "evolution_generated_change_files_faithful",
    EVOLUTION_GENERATED_CHANGE_HUNKS_TOTAL => "evolution_generated_change_hunks_total",
    EVOLUTION_GENERATED_CHANGE_HUNKS_FAITHFUL => "evolution_generated_change_hunks_faithful",

    // cog-reflection — the version contract: what the tracked history says the
    // code at the tracked main is, and whether the release tags still describe it.
    VERSION_COMMITS_SINCE_RELEASE => "cogneva_version_commits_since_release",
    VERSION_DECLARED_INFO => "cogneva_version_declared_info",
    VERSION_CONTRACT_VIOLATIONS => "cogneva_version_contract_violations",
    VERSION_CONTRACT_CHECKS_TOTAL => "cogneva_version_contract_checks_total",
    VERSION_CONTRACT_UNJUDGED => "cogneva_version_contract_unjudged",

    // cog-reflection — the GitOps puller: the loop that brings the cluster's
    // desired state in, read by the cluster it pulls for.
    //
    // The loop is on a supervised cadence, so its *liveness* already has a
    // reader: `cogneva_loop_tick_age_seconds` stops moving when the task is gone
    // and background_loop_stalled says so. What had no reader at all was the
    // loop *doing its job* and failing: the beat is stamped at the top of every
    // cycle whether or not the poll returned, the failure was written to the log
    // alone, and the puller itself left no series behind -- so a puller that
    // could not reach the release branch poll after poll and a cluster with
    // nothing to pull were the same picture on every reading. This is that
    // missing face: 0 when the poll returned, 1 when it did not, written on
    // every attempt. Labelled by cluster, because every cluster runs its own
    // puller against its own checkout and a series without the name would read
    // two clusters' outcomes as one.
    GITOPS_PULL_FAILED => "gitops_pull_failed",

    // cog-reflection — the baseline port trigger: the same missing face as the
    // puller above, one step further along the lineage.
    //
    // The trigger's job is to carry this instance's promoted changes onto a new
    // upstream baseline. Its *liveness* already has a reader -- it is on the
    // same supervised cadence, `cogneva_loop_tick_age_seconds{loop="baseline_port"}`
    // stops advancing when the task is gone, and background_loop_stalled says so.
    // What had no reader is the loop *doing its job* and failing: the beat is
    // stamped at the top of every cycle whether or not `port_tick` returned, the
    // failure went to the log alone, and the tick left no series behind -- so a
    // trigger that cannot reach its repository, or whose port fails every round,
    // and an instance with nothing new to port are the same picture on every
    // reading. This is that missing face: 0 when the round returned, 1 when it
    // did not, written on every round. Unlabelled, because one evolution
    // deployment runs the trigger for its own instance.
    BASELINE_PORT_TICK_FAILED => "baseline_port_tick_failed",

    // cog-reflection — how each run of the baseline port's eval A/B gate ended,
    // labelled by `outcome`, one run per cell.
    //
    // The gate exists to refuse a port that regresses a fixed eval suite. Its
    // verdict comes from a sandbox agent that runs the suite twice and reports
    // the outcomes itself, and the gate only z-tests what came back — so the
    // verdict and the *having measured* are two different facts, and only the
    // first had any reading. Two paths passed the gate without measuring, both
    // returning success: the agent reporting `applicable=false` (about a
    // component with no suite), and there being no orchestrator at all. Neither
    // left a series behind, and the failure a tick carries — `baseline_port_tick_failed`
    // — is 0 for both, because a gate that skips is not a tick that failed. So a
    // port that was never measured and a port that was measured and did not
    // regress were the same picture on every reading, which is exactly the shape
    // this gate must not have.
    //
    // A counter, because a gate run is a cumulative event over the process's
    // life; the five cells are the closed set of ends, so a reader can separate
    // a gate that keeps passing from one that keeps skipping. `unavailable` is
    // the cell for "never reached a verdict" -- no orchestrator, or the eval
    // task did not complete; the two stay apart on `baseline_port_tick_failed`,
    // which the task failure sets to 1 and the missing orchestrator does not.
    EVAL_GATE_OUTCOMES_TOTAL => "cogneva_eval_gate_outcomes_total",

    // cog-reflection — the promotion trend report's per-week counts, labelled by
    // `week` (the ISO week, e.g. `2026-W42`) and `outcome`, written once per
    // cycle for every week in the report's window.
    //
    // The report answers "is this channel still landing anything", and until
    // this series it answered it on no face the monitoring stack can read: the
    // file under `reports/` is a document someone has to open, the decline
    // alert only writes the audit stream and only when the trend is falling,
    // and the stall alert carries the same five counts but only ever *while
    // stalled* -- it is written to the persisted-alert table, not to any scrape,
    // and it is resolved the moment a week lands. So "the channel is landing"
    // and "the channel has stopped" were the same picture on every panel: for
    // the weeks that do land, nothing anywhere published a number.
    //
    // A gauge, because each cell is that week's standing count, not an event:
    // the report re-aggregates the whole window every cycle, so a week's cell is
    // the same question asked again rather than an increment. A reader telling
    // "nothing generated" from "generated and stuck in the channel" reads the
    // other cells of the same week beside `promoted`.
    PROMOTION_WEEK_CHANGES => "cogneva_promotion_week_changes",

    // cog-reflection — which gates a change was routed to, labelled by `tier`,
    // written once per change the pipeline reaches.
    //
    // The routing reads what the change touches, so the cells answer two
    // questions at once: how much of the fleet's work is prose, configuration,
    // source, topology, and -- in `real_gate` -- how often a change moved
    // something a criterion reads. The last cell is the one with no previous
    // reading anywhere: before this, "no change has ever touched the criteria
    // face" and "the routing is inert and sends everything to the cheapest
    // tier" both looked like a quiet fleet.
    //
    // A counter, because a change reaching the pipeline is a cumulative event;
    // the cells are the closed set of tiers, seeded at zero by the first change
    // the pipeline tiers -- from then on an absent cell and a cell stuck at zero
    // stay different readings. Before that first change nothing is published at
    // all, and an absent series says only that nothing has been tiered yet.
    CHANGE_TIER_TOTAL => "cogneva_change_tier_total",

    // cog-reflection — which landing recipe each change was dispatched under,
    // and whether the project named it or the built-in supplied it.
    //
    // Without this the fallback is silent, and silence here is the shape the
    // whole declaration surface exists to remove: "this project never declared
    // a kind" and "this project declared the built-in" dispatch identically, so
    // a project landing as a Rust workspace on the strength of never having
    // said otherwise reads exactly like one that chose it. `kind` is a label
    // rather than a second series because the second recipe, when it exists,
    // is a value of the same question.
    //
    // A counter, because reaching a dispatch is a cumulative event per change;
    // the cells are the closed set of recipes crossed with the two sources, and
    // they are seeded at zero by the first change the pipeline dispatches, so
    // from then on a cell stuck at zero and an absent series stay different
    // readings.
    //
    // Not filed with the build-outcome cells: those are compile semantics
    // (`built`/`failed`/`timed_out`/`unstarted`) and a recipe that does not
    // compile anything has no way to land in one of them without lying about
    // what happened.
    PROJECT_KIND_TOTAL => "cogneva_project_kind_total",

    // cog-reflection — how each round of the artifact-level evolution search
    // ended, labelled by `outcome`.
    //
    // The loop's liveness is `cogneva_loop_tick_age_seconds{loop="policy_evolution"}`
    // and it moves every cycle whether or not the round produced anything, so it
    // cannot say which of the round's ends happened. Two of the three ends change
    // nothing external -- too little evidence to compare, and a comparison that
    // found no significantly better candidate -- and the third adopts a new policy
    // version. All three went to an `INFO` line, and a failed round to a `WARN`,
    // and those die with the pod: "adopted three versions this week" and "never
    // adopted one" and "ran out of evidence every round since deploy" and "failing
    // every round" are four different facts that no reading distinguished. The
    // module's own note -- not to read its per-round `INFO` as the chain running --
    // is the guarantee stated in prose with nothing behind it.
    //
    // A counter, because the round runs on every cycle and the fact is a
    // cumulative one; the four cells are the closed set of ends, so a reader can
    // separate a round that never adopted from one that never had evidence to.
    POLICY_EVOLUTION_ROUNDS_TOTAL => "cogneva_policy_evolution_rounds_total",

    // cog-observability — how each flush of the analytics event buffer ended,
    // labelled by `outcome` and counting the events the batch carried.
    //
    // The loop's only reading was its own beat, cogneva_loop_tick_age_seconds,
    // which is stamped on every flush interval whether or not a batch was
    // waiting, so it cannot say whether anything reached ClickHouse: delivering
    // a batch and failing to ship one read the same. The failure is a loss
    // rather than a retry -- the insert consumes the batch, so a failed flush
    // drops those events and only a WARN records it.
    //
    // A counter, because a flush is a cumulative event over the process's life;
    // the two cells are the closed set of ends, so a reader can separate a
    // buffer that has lost every batch from one that has had nothing to send.
    ANALYTICS_FLUSH_TOTAL => "cogneva_analytics_flush_total",

    // cog-reflection — how each round of the memory schema repair loop ended,
    // labelled by `outcome`, one round per cell.
    //
    // This loop exists to make a silent loss visible again: an archived entry
    // whose derived layer never landed is unreachable by search, and nothing
    // reports it, because the test for "was this written" is "is the derived
    // layer there" and an absence cannot testify to an absence. The loop's only
    // reading was its own beat, stamped every repair interval whether or not it
    // repaired anything, so a round that returned an error and a round that found
    // no orphan read the same -- and both were ordinary-looking. A failed round
    // was a WARN that dies with the pod.
    //
    // A counter, because the round runs on every cycle and the fact is a
    // cumulative one; the four cells are the closed set of ends, so a reader can
    // separate a repair path that is failing every round from one that has
    // nothing to fix, and can see the entries it found but could not type -- a
    // permanent loss that re-surfaces every round and reads on no other series.
    MEMORY_SCHEMA_REPAIR_TOTAL => "cogneva_memory_schema_repair_total",

    // cog-skill — how each round of the skill hot-reload loop ended, labelled by
    // `outcome`, one round per cell.
    //
    // This loop exists to keep the in-memory registry in step with the skill
    // files on disk. Its only reading was its own beat, stamped every poll
    // interval whether or not anything changed, so a round that could not scan
    // the directories at all and a round that scanned them and found nothing to
    // do read the same -- and both looked ordinary. A skill file that fails to
    // load left the previous definition in the cache and went to a WARN that
    // dies with the pod, so an edited skill that never took effect and a skill
    // nobody edited were the same picture.
    //
    // A counter, because the round runs on every poll interval and the fact is a
    // cumulative one; the four cells are the closed set of ends, so a reader can
    // separate a scan that is failing every round from one with nothing to pick
    // up, and can see the skills it found on disk but could not load.
    SKILL_HOT_RELOAD_TOTAL => "cogneva_skill_hot_reload_total",

    // cog-observability — how each dispatch of an alert batch to one channel
    // ended, labelled by `channel` and `outcome`, the cell's value being how
    // many events that dispatch carried.
    //
    // The manager's only trace was the WARN it logged when a channel refused a
    // batch: an alert delivered to a human and an alert that reached no outlet
    // at all read the same on every other series, because the alert row is
    // written whether or not anyone was told. `channel` is the kind of outlet
    // (`webhook|email|slack`, or `none` when the bridge has no manager), never
    // its URL or its recipients; `outcome` is the closed set of ends
    // (`delivered|failed|skipped|no_channel`), where `skipped` is a channel
    // that is configured but has nowhere to send -- an email with an empty
    // recipient list -- and deliberately does not count as delivered.
    ALERT_NOTIFICATIONS_TOTAL => "cogneva_alert_notifications_total",

    // cog-observability — which outlets this deployment can deliver an alert
    // through, one row per configured kind at 1.
    //
    // The dispatch counter above only gains a row when a batch is actually
    // dispatched, so on a deployment with no outlet and nothing firing, "no
    // channel is configured" is on no series at all and reads exactly like
    // "nothing needed delivering". That is the state a reader most needs to
    // see, so it is published at start rather than when a dispatch fails: the
    // family also carries a `state="verdict"` row, at 1 when no outlet exists
    // and 0 when one does. Read any `state="configured"` row at 1 as an outlet
    // that exists; an empty list writes no such row at all, so no reader has to
    // guess what a placeholder value would have meant. The value is a fixed 1
    // or 0 per process, so a reader takes one series instead of adding the
    // copies every process that loads this plugin reports.
    ALERT_CHANNELS_CONFIGURED => "cogneva_alert_channels_configured",

    // cog-observability — the two sizes the loaded-rule-set check compares,
    // labelled by `state` (`declared|loaded|document_readable`), the cell's
    // value being a count.
    //
    // The check already raised a row when the two sides disagreed, but a row is
    // an event: the ordinary state -- a document and a process agreeing on a
    // number -- had no series at all, so "the rule set is fine" and "nothing
    // ever compared the two" were the same reading, and the only way to answer
    // either was to read the startup log and count. `declared` is written only
    // when the document parsed: a document that could not be read is not a
    // document declaring zero rules, so `document_readable` carries that fact
    // as its own cell and a reader comparing the two sizes has to hold it at 1
    // rather than read a missing `declared` as the answer.
    ALERT_RULE_COUNT => "cogneva_alert_rule_count",

    // cog-observability — how each log-flush of the Loki pusher ended, labelled by
    // `outcome`, the cell's value being how many log entries that batch carried.
    //
    // This loop exists to deliver buffered logs to Loki. Its only reading was its
    // own beat, stamped every flush interval whether or not there was anything to
    // push, so a push that failed every round and a buffer that was simply empty
    // read the same -- and both looked ordinary. A failed push consumed the batch,
    // so the entries were lost, not retried, and their only trace was a WARN that
    // died with the pod: "delivered" and "the whole batch was dropped" were the
    // same picture.
    //
    // A counter whose cell value is the entry count, not the batch count: the fact
    // worth alerting on is how many entries a failed flush lost. Both triggers --
    // the batch-size one and the interval one -- come through the same push, so one
    // reading covers the whole path.
    LOKI_FLUSH_TOTAL => "cogneva_loki_flush_total",

    // cog-supervisor — how each pass of the supervisor loop ended, labelled by
    // `pass` (which arm ran) and `outcome`, one pass per cell.
    //
    // The loop drives six independent passes -- health, quota, rebalance, event
    // aggregation, autonomous decisions, and control-plane status reporting --
    // and each one, when it failed, left only a WARN that died with the pod. Its
    // only reading was its own beat, and that beat is driven by the shortest of
    // the six intervals: a pass that could not run at all and a pass with nothing
    // to do were the same picture, and a starved pass stayed invisible behind a
    // live beat the other five kept stamping.
    //
    // A counter, because every pass runs on each of its own ticks and the fact is
    // cumulative. `pass` names the arm and `outcome` is the closed set
    // {ok, failed}; every arm routes its round through one recording call, so the
    // reading covers all six the same way. The autonomous arm reports no error by
    // construction, so it only ever lands in `ok` -- a failure inside it surfaces
    // as the loop's own death, since a panic in the body is counted by the
    // liveness family.
    SUPERVISOR_PASS_TOTAL => "cogneva_supervisor_pass_total",

    // cog-reflection — the registry store's capacity reclamation, read by the
    // process that reclaims it.
    //
    // Four readings rather than one, because "the store is full" and "something
    // was removed" and "the removal was refused" are three different facts and a
    // single counter of bytes freed would answer none of them: a round that found
    // nothing to remove and a round that never ran look identical through it. The
    // times the round ran, how many tags it removed and how many deletions were
    // refused are separate counters so a growing store with a flat removal count
    // says which of the two is happening.
    //
    // The deletion is the one destructive thing this process does to a store
    // nobody else writes, so it also carries when it last happened: the counters
    // are monotonic, and a process that died leaves them frozen -- which reads
    // exactly like a store that needs nothing. The timestamp is what tells those
    // apart, the same way the rollout readings carry one.
    //
    // A round that ran and could not read the store counts as a run and leaves
    // the timestamp where it was, because that is what happened: it ran, and it
    // did not finish. Neither reading says that on its own -- a run with a
    // removal count of zero is also a completed round with nothing to remove,
    // and a frozen timestamp is also a store that was never due -- so the run
    // count rising against a timestamp that does not move is the reading for
    // that third shape.
    REGISTRY_MAINTENANCE_RUNS_TOTAL => "cogneva_registry_maintenance_runs_total",
    REGISTRY_PRUNED_TAGS_TOTAL => "cogneva_registry_pruned_tags_total",
    REGISTRY_PRUNE_FAILURES_TOTAL => "cogneva_registry_prune_failures_total",
    REGISTRY_MAINTENANCE_READING_UNIX => "cogneva_registry_maintenance_reading_unix",

    // cog-reflection — the local build store's reclaim, read as a family.
    //
    // Separate from the registry's family above because they are two stores on
    // two volumes with two triggers, and merging them would make one number
    // stand for whatever the other store happened to be doing. They share one
    // retention set, which is why they live in one process, and nothing else.
    //
    // The pass counter is labelled by `outcome` rather than split into series
    // per cause: the causes are a closed set the producing code declares, and a
    // reader that has to union six series to ask "did any pass run" is reading
    // the label as if it were six unrelated facts. A round that could not take
    // the host's build slot counts here too — it is a round that was attempted,
    // and a store that is never reclaimed because the host is always building
    // otherwise looks exactly like one nobody ever asked about.
    //
    // The pair to read across is this counter and the timestamp below: rounds
    // rising against a timestamp that does not move is "the reclaim keeps being
    // attempted and never completes", which is the shape a broken `buildah rmi`
    // takes. Both rising is a healthy round with or without anything to remove,
    // and neither moving is a process that is not doing this at all.
    BUILDAH_STORE_ROUNDS_TOTAL => "cogneva_buildah_store_rounds_total",
    // Images removed from the store, summed over rounds. Written only when a
    // round removed something, so its absence means no pass has ever freed an
    // image.
    BUILDAH_STORE_PRUNED_IMAGES_TOTAL => "cogneva_buildah_store_pruned_images_total",
    // Layer directories the store lost, summed over rounds. Read next to the
    // image count: buildah shares layers between images, so the two diverge, and
    // a round that removes many images for few layers is freeing less than the
    // image count suggests.
    BUILDAH_STORE_PRUNED_LAYERS_TOTAL => "cogneva_buildah_store_pruned_layers_total",
    // Apparent bytes the store's layers lost, summed over rounds. Apparent
    // rather than blocks, matching every other size this process reads, and
    // written only when both ends of the pass could be measured -- a delta with
    // one end missing would be a number with a provenance that does not exist.
    BUILDAH_STORE_FREED_BYTES_TOTAL => "cogneva_buildah_store_freed_bytes_total",
    // Images the retention set protected, as of the last completed round. The
    // reading that says the keep set is doing something: a pass whose only
    // number is "removed 0" cannot be told from one that protected nothing and
    // found nothing to remove.
    BUILDAH_STORE_KEPT_IMAGES => "cogneva_buildah_store_kept_images",
    // Revisions the running workloads referenced, as of the last completed
    // round. Published next to the count above because the two answer different
    // questions -- how many the window covers against how many something is
    // running right now -- and a live reading of zero is what a stalled cluster
    // read would look like, which is the one way this pass could delete a base
    // that is in use.
    BUILDAH_STORE_LIVE_IMAGES => "cogneva_buildah_store_live_images",
    // When a build-store reclaim last ran to completion, in unix seconds; 0
    // until one does. Seeded with the epoch rather than the process start, so a
    // process that has never completed a round reads as never having completed
    // one instead of as having just done it.
    BUILDAH_STORE_READING_UNIX => "cogneva_buildah_store_reading_unix",

    // cog-reflection — what one reclaim round's restart of the tag server cost.
    //
    // A counter of seconds, not a gauge of the last one: the quantity anyone
    // needs to price this is "how much rollout time has this cost in total",
    // and a gauge answers with the most recent occurrence instead. It sits next
    // to the removal counters because the two are read together: the sweep's
    // cost is proportional to how much waste accumulated since the last one, so
    // seconds per removed tag is the ratio that says whether the trigger is
    // letting the store grow too far between rounds.
    //
    // It measures the *deployer's* hold -- from issuing the restart to reading
    // the tag server's own answer -- which is not the fleet's outage: the
    // outage starts when the old pod is deleted, slightly earlier. The hold is
    // what this process can read exactly, and it is the cost this process pays.
    REGISTRY_REBUILD_HOLD_SECS_TOTAL => "cogneva_registry_rebuild_hold_secs_total",

    // cog-reflection — a reclaim round whose garbage collection did not go out.
    //
    // Deleting a manifest only drops a reference: the layers stay on disk until
    // the registry restarts and its init container sweeps them. A round whose
    // restart failed has therefore left a debt, and the condition that would pay
    // it is now behind it -- the tags are gone, so the next round removes
    // nothing and never restarts. What is left is a state rather than an event:
    // it outlives the round that raised it, and the reading has to as well, or a
    // debt sitting on the books for hours looks exactly like one never taken on.
    // Published at zero as well as at one: an absent series and a settled debt
    // are the same empty cell, and the question is worth asking every round.
    REGISTRY_GC_OWED => "cogneva_registry_gc_owed",

    // cog-reflection — the rollout judgement's own resource readings.
    //
    // The judgement runs in a short-lived Job pod, which is born, works for a
    // few minutes and disappears. Both generic resource rules sample over
    // windows that such a container leaves before it finished, and the pod is
    // gone from cadvisor's view by the time anyone could ask. The only place
    // "what the manifest declared" and "what this run used" are known at once
    // is the process itself, so it reads its own cgroup at the end of the run
    // and the deployer — which wrote the declared amounts into the manifest —
    // publishes the comparison.
    //
    // A ratio rather than the two sides separately: the levels differ per run
    // and mean nothing alone, and the two sides are already joined at the
    // source. No labels, because the reading is about the newest run only — a
    // label per revision would leave one row per revision forever for a value
    // that only ever describes one of them.
    ROLLOUT_JOB_CPU_THROTTLED_RATIO => "cogneva_rollout_job_cpu_throttled_ratio",
    ROLLOUT_JOB_MEMORY_PEAK_RATIO => "cogneva_rollout_job_memory_peak_ratio",
    // When the run took the reading, from the run's own clock. Carried so a
    // reader can tell a reading of the run it just watched from the one the
    // previous run left standing — the way these two go stale is by a run that
    // died before it could report.
    ROLLOUT_JOB_READING_UNIX => "cogneva_rollout_job_reading_unix",
    // Rounds the deployer stopped before rolling, because the upstream landed a
    // new tip while the revision it was about to roll was still being built —
    // or, on the re-dispatch path, between the revision's own rollout and the
    // re-dispatch an external apply forced.
    //
    // The zero the producer writes on an unskipped round does not, by itself,
    // make the question visible: the store accumulates the samples it is given,
    // so adding zero leaves the rendered value where it was and a round that
    // asked reads exactly like a round that never ran. What the zero does move
    // is the store's per-series last-write companion, which is outside this
    // list and which nothing reads. The count of questions is therefore carried
    // by a series of its own, on this side of the contract:
    // `..._supersession_checks_total` below.
    //
    // The saving it counts is one rollout Job and one set of workload restarts:
    // the compile and the image push for the superseded rev are already paid by
    // the time the question can be asked.
    MAINLINE_SUPERSEDED_ROLLOUT_TOTAL => "cogneva_mainline_superseded_rollout_total",
    // Rounds the deployer reached the decision point of rolling a revision out.
    //
    // It exists to be the second half of a pair. A reading that stops being
    // written and a round that never ran are the same absent cell, and the
    // series that would have told them apart is the one under suspicion — so
    // the question has to be asked of two counts at once. This one moves on
    // every round that got as far as deciding; `..._supersession_checks_total`
    // moves in the same call, on the path that also asks the guard. On a
    // healthy round they are equal, so the difference between them is the
    // number of rounds that decided to roll without asking the question, which
    // is what a guard whose call site was deleted looks like from outside. It
    // is also the cadence every other reading of the round is bounded by: how
    // often the deployer tries to promote is not the declared poll interval
    // (that bounds the loop, not the attempt) and no other series publishes it.
    MAINLINE_ROLLOUT_ATTEMPTS_TOTAL => "cogneva_mainline_rollout_attempts_total",
    // Rollouts the deployer watched land: the deployments actually on the
    // revision it dispatched, with the judgement that ran against them finished
    // and passed.
    //
    // The partner of the attempts counter above, and the reading whose absence
    // let a twenty-hour stall run silent. `..._rollout_attempts_total` answers
    // "how often did a round reach the decision to promote", and it moves
    // whether or not anything is ever deployed -- a round that decides to roll
    // writes it before it can fail, so through a deployer that decided every
    // cycle and never once got an image onto the cluster it climbed exactly as
    // it does on a healthy one, and nothing on the face told the two apart. That
    // is the shape a delivery refused by the cluster has: the round does its
    // work, the apply or the Job is rejected for a reason only the refusing side
    // knows, and the loop returns to the top with every counter still moving.
    // This one moves only where a rollout is observed whole, so attempts
    // climbing while completed stays flat is the deployer trying and nothing
    // landing -- a fault no single count can show, only the pair.
    //
    // A counter, not a "last deployed" timestamp: the store renders each
    // series' `_observed_timestamp_seconds` companion anyway, so the time of the
    // last completed rollout is readable beside this, while the count is the
    // half that answers "did any land in the window" without a cadence to bound
    // an age against -- which no series publishes for a per-change event.
    MAINLINE_ROLLOUT_COMPLETED_TOTAL => "cogneva_mainline_rollout_completed_total",
    // Rollouts the deployer refused to dispatch because the cluster says its
    // ServiceAccount may not apply one of the kinds the revision delivers, by
    // `verb` and `resource`.
    //
    // The delivery face and the authorization face are two declarations, and the
    // tree's own gate only proves them equal *in the tree*. What is installed on
    // a cluster is a separate delivery of the Role, and it can be behind the tree
    // -- a `helm upgrade` nobody ran, or a grant added to the tree that never
    // reached the release. While it is behind, every round reaches the same apply
    // and is refused the same way, and the refusal lives inside the rollout Job:
    // the poll counter shows a failed round, the attempts counter climbs, and
    // *which* verb on *which* resource the cluster withheld is written nowhere
    // that survives the pod. Measured over 2026-10-12 and 2026-10-13: about
    // twenty hours of that, one refused dispatch after another, until the Role
    // was patched by hand. This counter is the durable name of the cause the
    // outcome counter (`..._rollout_completed_total`) can only say was missing:
    // a rule fires on it, and the label says what to grant.
    //
    // Written before the dispatch, so a refused round is one that never reached
    // the Job at all -- the reading is the deployer's own judgement of the
    // cluster, not the cluster's rejection replayed.
    MAINLINE_PREFLIGHT_REFUSED_TOTAL => "cogneva_mainline_preflight_refused_total",
    // Times an API server refused one of the deployer's *own* `kubectl`
    // commands, by the verb and the resource it named in the sentence.
    //
    // The sibling above asks about the kinds a revision delivers, before a Job
    // is dispatched. The deployer also reads things it does not deliver -- the
    // namespace `ResourceQuota`, the enforced `LimitRange`s -- and a grant
    // missing on one of those is invisible to that preflight by construction:
    // adding them to it would be a second hand-written list of what the deployer
    // reads, which is the shape that drifts. Counting at the one wrapper every
    // own-command already goes through needs no list at all.
    //
    // Measured 2026-10-19: the LimitRange read was refused from 09:44 until the
    // installed Role was brought up at 12:14, and the only reading that ever
    // mentioned it was the comparison-failure meter behind a six-in-an-hour
    // threshold. An authorization denial does not heal by being retried, so the
    // rule over this counter fires on the first one.
    MAINLINE_KUBECTL_REFUSED_TOTAL => "cogneva_mainline_kubectl_refused_total",
    // Rollouts the cluster's admission face kept from ever creating a Pod, by
    // which wall did it: `quota`, `limitrange`, `podsecurity`, or `admission`
    // when the wording matched none of the three.
    //
    // The sibling above counts the deployer's own reads being refused; this one
    // counts the cluster refusing the delivered object. Neither is the other's
    // substitute: a grant can be in place and the quota still full, and the
    // refusal itself lands outside every reading the deployer publishes -- the
    // Job is created, no Pod is, and the only trace is the Job's Failed
    // condition, which is read here and then dies with the round. The resource
    // account keeps a row when the process holds a gate, but that account is
    // not scraped, so no rule can name it.
    //
    // The label is the wall rather than a boolean because the repair differs:
    // a full quota is raised in its ResourceQuota, an out-of-range request is
    // fixed in the LimitRange or in the manifest, and PodSecurity is a policy
    // change. "admission" is recorded when the wording matched none of the
    // known walls -- an unknown wall is still a refusal, and folding it into a
    // known one would name the wrong repair.
    //
    // Measured 2026-10-19: the LimitRange wall held the evolution rollout for
    // two and a half hours, and `mainline_rollout_never_lands` reported the
    // stall without being able to say what was holding it.
    MAINLINE_ROLLOUT_REFUSED_TOTAL => "cogneva_mainline_rollout_refused_total",
    // Rounds the deployer held a rollout back on its own judgement, by which
    // dimension of the ceiling the new pod would have crossed: `cpu`, `memory`,
    // `pods`, `storage`, or `other`.
    //
    // The sibling above counts the other half of the same stall and the two are
    // not interchangeable. That one is the cluster refusing the delivered
    // object: the Job exists, its Pod is never created, and the wall is named in
    // the Job's Failed condition. This one is the deployer reading the ceiling
    // itself before it touches anything -- the Job runs, decides the new pod
    // would not be admitted beside the old one, and exits 75 leaving the old pod
    // serving. The repair differs: a refusal is a Role, a LimitRange or a
    // PodSecurity policy that has to change; a hold is usually a ceiling that
    // has to drain, and it heals by itself on a later round.
    //
    // The label is the dimension rather than the quota object because that is
    // the part the failure signature cannot carry: `...:precheck:<target>:admission`
    // says where it broke, not whether `limits.cpu` or `limits.memory` ran out,
    // and raising one does nothing for the other. `other` is a real answer -- a
    // quota key this build has not seen is still a hold, and folding it into a
    // known dimension would name the wrong repair.
    //
    // Measured 2026-10-27: the main deployment and evolution were held while
    // three other workloads of the same round sat in their own surges, and the
    // only way to learn it was `limits.cpu` was to read the rollout Job's log
    // before it was collected. `mainline_rollout_never_lands` reported the stall
    // and could not name the wall.
    MAINLINE_ADMISSION_HELD_TOTAL => "cogneva_mainline_admission_held_total",
    // Times the deployer's guard put the overtaken question, counted whether
    // the answer was yes or no.
    //
    // Recorded as its own count rather than derived from the answers: the
    // answer counter only moves when the answer is yes, so counting questions
    // by counting skips would make a healthy quiet stretch read as a guard that
    // never ran. Read beside `..._rollout_attempts_total`: they climb together
    // while the guard is wired, and the gap that opens when they stop climbing
    // together is the guard no longer being asked.
    MAINLINE_SUPERSESSION_CHECKS_TOTAL => "cogneva_mainline_supersession_checks_total",
    // Revisions that landed between two deployer ticks and were superseded by
    // the next one, so that no tick ever aimed at them: never built, never held,
    // never named by any reading keyed on "which revision this round went for".
    //
    // The deployer aims each round at the head that exists at that moment. When
    // revisions land faster than a round completes, the ones in between are
    // invisible in exactly the way a quiet main is: the deployment keeps
    // climbing while a share of what was pushed was never tried. The label is a
    // closed set; `superseded_before_tick` is what it carries today, and
    // "tried and held" is a different cell that stays out of this one.
    MAINLINE_REVS_SKIPPED_TOTAL => "cogneva_deployer_revs_skipped_total",
    // Rounds the deployer asked the upstream platforms what CI concluded for the
    // revision it was about to roll, and what came back: `pass`, `fail` (that
    // round holds the rollout) or `no_evidence`.
    //
    // The reading exists because two of the three answers let the rollout go
    // ahead. The gate was built to fail open — an unreachable platform must not
    // stall the whole mainline — so "read a green" and "could not read at all"
    // both end in a rollout, and afterwards neither the state file (which only
    // ever records a hold) nor anything else distinguishes them. The one trace
    // that did was a log line in the deployer's pod, which is replaced on the
    // next rollout and takes its log with it. `no_evidence` is the label that
    // has to be readable for the fail-open to be a recorded answer rather than
    // an absence; a run of it says the gate is passing revisions for a reason
    // other than the upstream's verdict.
    //
    // The `ask` label says which of the round's questions heard it: `round_head`
    // (decides whether the round starts), `before_build` (decides whether to
    // spend a cold build on the host's only slot), `pre_dispatch` (decides
    // whether the built image enters the cluster) and `job_termination` (the
    // job re-asked and carried its answer back in the termination message).
    // Same criterion, different price: a `fail` heard at the round head costs
    // nothing, the same `fail` heard before the build saves a build, and one
    // heard only at dispatch has already paid for it. Without the label those
    // three are one row, so the counter cannot say whether adding the middle
    // question is saving anything.
    MAINLINE_CI_VERDICT_TOTAL => "cogneva_mainline_ci_verdict_total",
    // Why the round above got no verdict, by reason, one count per silent
    // upstream: `pending` (upstream still working on it), `no_runs` (asked and
    // there was nothing there), `status_unreadable`, `bad_api_base`,
    // `connect_failed`, `unusable_body`, and the http_* split by whose fault it
    // is (auth_rejected, not_found, rate_limited, upstream_error, other).
    //
    // The verdict counter above has three cells and one of them is a lie of
    // omission: `no_evidence` is the same reading whether the checks had not
    // finished, the token was refused, or the connection to the platform timed
    // out — three different owners (wait, fix the gateway, fix the path) and
    // three different actions. The gap was measured: 17 of 20 rounds in the
    // first 36 hours were `no_evidence` while the upstream had green checks
    // finished minutes earlier, and the same revision asked twice minutes apart
    // answered `pass` then `no_evidence` — so the silence was intermittent and
    // unactionable for lack of exactly this label. Recording it changes no
    // verdict: the gate still fails open by design.
    MAINLINE_CI_NO_VERDICT_REASON_TOTAL => "cogneva_mainline_ci_no_verdict_reason_total",
    // Check runs that reported a failure and were left out of the verdict above,
    // by the workflow that produced them.
    //
    // The verdict has one cell for "the upstreams reported green", and after an
    // exclusion that cell also covers "one check was red and its workflow does
    // not judge this tree". GitHub's dynamic Pages workflow is the case that
    // forced the distinction: it publishes a site rather than the revision's
    // artifacts, and on 2026-10-20 its failure (a missing `id-token: write`
    // permission) held a revision whose other fifteen checks were green. The
    // exclusion is deliberately narrow -- by workflow identity, never by check
    // name -- and this counter is what keeps it visible: without it an
    // exclusion that fires every round and one that no longer applies at all
    // read the same.
    MAINLINE_CI_CHECK_EXCLUDED_TOTAL => "cogneva_mainline_ci_check_excluded_total",
    // How long the rollout has been held by the CI gate, in seconds: 0 when it is
    // not held, the age of the current hold while it is. The state file and the
    // heartbeat line say *that* the gate is holding and which rev it holds; this
    // says for how long, and that is the quantity the bound is on -- a hold of
    // three minutes and a hold of three hours are the same row everywhere else,
    // while one of them means "wait" and the other means "go and read why the
    // upstream is still red".
    //
    // 0 is published while not held rather than the series being left absent:
    // the store only ever adds and overwrites, so a hold that ends has to be
    // written back as 0 by something, or its last value stays in the store
    // forever and a rule reading it stays lit with nothing able to clear it.
    // The gate is fail-open, so most rounds publish this as 0.
    //
    // No label per revision, for the reason MAINLINE_BUILD_SECONDS gives beside
    // it: a hold describes one revision, and a series per revision would leave
    // one permanent row per hold for a value that describes only itself. Which
    // revision is being held is answered where revisions are already the key --
    // the state file's `ci_hold.rev`, the heartbeat's `ci_hold=` cell and the
    // log -- and it is read together with this number rather than inside it.
    MAINLINE_CI_HOLD_SECONDS => "cogneva_mainline_ci_hold_seconds",
    // Consecutive rounds in which the advance could not take the host's build
    // slot, as a gauge; 0 on the round that takes one.
    //
    // The same fact as a value rather than a sentence in a state file. A round
    // that is refused logs the same line every time -- same asker, same "host
    // is building" -- and the state file's `advance_deferrals` is written where
    // only a reader already on the pod can see it; a run of refusals therefore
    // leaves the operator asking why the mainline stopped with every published
    // reading looking healthy. The count is the one quantity that separates
    // "happened to meet a host that was building" from "never got a turn":
    // measured under saturation, six consecutive refusals, one log line each.
    //
    // The advance is the only caller that waits zero seconds, so a starved
    // advance never shows up as a refusal anybody acts on -- the other callers
    // wait out the holder, this one returns and comes back next round. That is
    // why the value has to be published rather than counted at the refusal:
    // the refusal exists only where somebody asked, and the round after the
    // asker gives up is silent.
    //
    // One number, no labels: the run is the deployer's own, and the deployer is
    // the only writer. 0 is published on the round that takes the slot, so a
    // starvation that ends is written back rather than left as its last value,
    // for the reason MAINLINE_CI_HOLD_SECONDS gives above.
    MAINLINE_ADVANCE_DEFERRALS => "cogneva_mainline_advance_deferrals",
    // Rounds the deployer's poll loop finished, by how they finished: `ok` when
    // the round ran to the end, `failed` when it returned an error.
    //
    // The loop already publishes that it is still cycling, and that reading is
    // blind to what the cycles did. The tick age is stamped at the top of the
    // tick, before the body runs, and every reading the body publishes is
    // written ahead of the step that can fail. Measured on 2026-10-08: the
    // publish step refused the manifest bundle for two and a quarter hours (the
    // client-side apply annotation ceiling), every round failed there, and the
    // only trace was a warn the next rollout deletes with the pod that wrote
    // it. The tick age, the CI verdict counter and the version contract all
    // read healthy through it, so a deployer that had failed every round for
    // two hours looked exactly like one with nothing to do.
    //
    // A counter rather than the loop's own tick age, because the two answer
    // different questions: the age says whether the loop is cycling, which
    // stays true through any run of failures, and this says whether its rounds
    // arrive. The reader is the pair -- the failure cell moved while the ok
    // cell did not move over the window -- which separates a loop failing every
    // round from one that is idle and from one that is dead (a dead loop moves
    // neither cell, and the tick age is the reading that names it).
    MAINLINE_POLL_CYCLES_TOTAL => "cogneva_mainline_poll_cycles_total",
    // How long the round's `cargo build --release` took, in seconds, by
    // `outcome` (`ok` when it returned a binary, `failed` when it gave up). One
    // gauge per outcome holding the build that finished most recently.
    //
    // A gauge and not a histogram: the store derives a histogram's bucket
    // bounds from the series name, and the two schemes it has top out at 65.5s
    // (`_ms`) and 16.4s (everything else). A multi-minute compile lands wholly
    // in the `+Inf` bucket under either, so a histogram here would publish a
    // distribution with no information in it and a quantile that is always the
    // top bound -- a reading that looks alive and says nothing. The gauge holds
    // the value the store can always represent, and the sample log over it is
    // the history a panel draws; the alert reads the current value against the
    // declared bound.
    //
    // No label per revision: a revision is built once, so a series per revision
    // would leave one permanent row per build for a value that describes
    // exactly one of them. `outcome` is bounded and is kept because the two are
    // different readings -- a build that failed within seconds on a compile
    // error and one that ran the full compile must not be compared against the
    // same bound, or a run of early failures reads as the compiler getting
    // faster.
    MAINLINE_BUILD_SECONDS => "cogneva_mainline_build_seconds",

    // cog-gateway — request accounting.
    HTTP_REQUESTS_TOTAL => "http_requests_total",
    HTTP_REQUEST_DURATION_MS => "http_request_duration_ms",

    // cog-gateway — container process self-reports that reached the ingest
    // endpoint, by `outcome` (accepted / refused).
    //
    // The topology face answers "no process table for this container" for three
    // different facts at once: it is a third-party image that never reports, its
    // reporter has not run yet, or its reports are being dropped on the way in
    // (wrong URL, renamed Service port, a network policy that stopped admitting
    // this platform's own pods). The third is silent in a way the first two are
    // not -- the request leaves, nothing answers, and no error surfaces -- so
    // the cell exists to say whether anything is arriving at all. Both cells are
    // written at zero on startup: absence then means "this process does not
    // publish the series", while zero means "the channel is here and nothing
    // came in over this window", which is the reading that points at the path
    // rather than at the container.
    PROCESS_REPORT_INGEST_TOTAL => "cogneva_process_report_ingest_total",

    // cog-gateway — whether the cluster reads behind the topology, workload and
    // quota panels still work, by `resource` (the apiserver resource the face
    // reads) and `outcome` (ok / failed).
    //
    // The face answers a failed read by leaving a gap on screen, and a gap is
    // what "there is nothing there" looks like: on 2026-10-27 a ClusterRole that
    // granted `pods` but not the two cluster-wide list paths it had just started
    // reading left the whole topology panel empty for hours with no error
    // anywhere except the JSON body nobody was reading. The reads only happen
    // when someone opens the page, so nothing polled them and nothing could
    // notice them stopping.
    //
    // Both cells are published at zero on startup, for the reason the ingest
    // counter above states: absence means this process does not publish the
    // reading, zero means the probe ran and everything answered. The probe is
    // what makes the reading independent of a viewer -- a browser tab is not a
    // monitor.
    //
    // Carries the `deployment` the process runs as. Two deployments serve this
    // face from the same binary under different service accounts, so the two
    // halves of a grant (the ClusterRole's rule, the binding's subject) can be
    // wrong for one and right for the other; a reading that cannot name the
    // process leaves the alert pointing at a cluster-wide permission when the
    // fault is one account's binding.
    CLUSTER_READ_TOTAL => "cogneva_cluster_read_total",

    // cog-gateway — LLM pool and upstream health.
    LLM_CALLS_TOTAL => "llm_calls_total",
    LLM_CALL_LATENCY_MS => "llm_call_latency_ms",
    // Published at zero as well as at a value. An absent cell says the metering
    // path never ran for that upstream and actor, while a cell holding zero says
    // it ran and had nothing to add -- and an upstream that sends no usage frame
    // produces the second, not the first, so without the zero the two read the
    // same.
    LLM_TOKENS_TOTAL => "llm_tokens_total",
    // What the read of each finished response found, by `upstream` and
    // `outcome`. A series of its own rather than more labels on the counts
    // above, because it answers a different question: the counts say how much,
    // this says whether the upstream said anything at all. A zero on the counts
    // cannot tell those apart, which is how a meter that is blind looks exactly
    // like a workload that used nothing. The cell vocabulary is declared next to
    // the producer.
    LLM_USAGE_READINGS_TOTAL => "llm_usage_readings_total",
    // Whether the decision behind `stream_options` rests on something measured
    // about this upstream, by `upstream`. 1 = it does (the pool entry carries a
    // probed verdict, or the runtime asked again and got one), 0 = the gateway
    // is falling back to the vendor profile for it.
    //
    // A series of its own because the other face of this decision cannot carry
    // the distinction: a zero on the counts and an `absent` in the readings say
    // the upstream was silent, while this says whether silence was ours to
    // cause. The gateway that stopped asking and the upstream that reports
    // nothing look the same on every other series here, and only the second is
    // the upstream's doing.
    LLM_USAGE_VERDICT_MEASURED => "llm_usage_verdict_measured",
    // Whether the decision behind routing a tool-carrying request rests on
    // something measured about this upstream, by `upstream`. 1 = it does (the
    // pool entry carries a probed verdict, or the runtime asked again and got
    // one), 0 = nobody has an answer yet and the request is routed on the
    // assumption that the upstream supports them.
    //
    // A series of its own rather than a second label on the series above,
    // because the two verdicts are asked separately and one can be settled
    // while the other is not. What it separates is the same shape: the routing
    // guard moves tool traffic away only from an upstream a probe proved does
    // not support native tool calls, so an upstream nobody asked is a yes --
    // and the failure that follows is a success on every other series here.
    // The upstream answers 200 with the tool calls written as text, no tool
    // runs, and the shape-error reading and the client-error counter stay at
    // zero because nothing was refused.
    LLM_TOOL_CALLS_VERDICT_MEASURED => "llm_tool_calls_verdict_measured",
    // Whether the decision to rewrite the caller's `temperature` rests on
    // something measured about this upstream, by `upstream`. 1 = it does (the
    // pool entry carries a probed verdict, or the runtime asked again and got
    // one), 0 = the vendor profile decided instead.
    //
    // The third of the three questions the admission probe asks, and the one
    // whose fallback is easiest to mistake for harmless. A profile that does
    // not clamp fails in the open: the caller's value goes out unchanged, the
    // upstream refuses it, and the pool's own failure counter catches the
    // refusal -- which is why an unmeasured verdict there looks like it needs
    // no reading. The clamp direction is the opposite: `requires_temperature_one`
    // is a value the caller chose, the gateway overwrites it, and the request
    // comes back 200 with the sampling the caller asked for replaced by one
    // they cannot see or recover. Clamped-on-a-guess and clamped-on-evidence
    // are the same increment on the clamp counter, the same status code and the
    // same response body, so nothing else on this face can tell them apart.
    LLM_TEMPERATURE_VERDICT_MEASURED => "llm_temperature_verdict_measured",
    LLM_REQUEST_PARAM_CLAMPED_TOTAL => "llm_request_param_clamped_total",
    LLM_UPSTREAM_CLIENT_ERRORS_TOTAL => "llm_upstream_client_errors_total",
    LLM_UPSTREAM_FAILURES_TOTAL => "llm_upstream_failures_total",
    LLM_UPSTREAM_HEALTHY => "llm_upstream_healthy",
    // Upstreams holding a shape-class rejection (400/404/422) whose cause one
    // response cannot settle, by `upstream`, 1 or 0. Such a status code has two
    // opposite causes -- a request malformed enough that any compatible
    // upstream would refuse it too, or an upstream that is broken for
    // everybody -- and the response itself does not say which. So the request
    // path only records the question, and a minimal probe answers it.
    //
    // This series is that question's own reading: 1 from the moment a
    // shape-class response arrives until the prober rules on it, 0 otherwise.
    // Without it the handoff has no reading of its own, because the only other
    // series that moves is the prober's verdict -- and that one moves for its
    // own reasons too, since an upstream gets probed whether or not a request
    // ever named it. A handoff that runs and one that was never wired then look
    // the same, which is what makes a fix unverifiable after it lands.
    LLM_UPSTREAM_SHAPE_ERRORED => "llm_upstream_shape_errored",
    LLM_UPSTREAM_QUOTA_WINDOW_SECS => "llm_upstream_quota_window_secs",
    LLM_UPSTREAM_QUOTA_RESET_UNIX => "llm_upstream_quota_reset_unix",
    // Why an upstream is refusing, as a label on a constant 1, by `upstream`
    // and `kind`.
    //
    // Every refusal looks the same on the health series -- `llm_upstream_healthy`
    // 0 -- and the causes call for opposite repairs. A quota cycle comes back on
    // its own, so waiting is the plan; an invalid subscription, an unpurchased
    // model or a suspended account never comes back, so waiting is the mistake,
    // and how long the operator waits for a recovery that cannot arrive is set
    // by whatever they read. One series cannot carry that difference, which is
    // why this one exists rather than a second value on the health series.
    //
    // Only the upstream's own words are classified. A refusal whose cause is not
    // in its text is reported as `unclassified` rather than guessed: guessing a
    // never-heals refusal into the healing class is precisely the error that
    // makes waiting look like a plan. All three values are published for every
    // suspect upstream, the current one 1 and the others 0, so a kind that stops
    // holding is written back to zero instead of standing as the last thing ever
    // said.
    LLM_UPSTREAM_REFUSAL_KIND => "llm_upstream_refusal_kind",
    // Whether an upstream takes a call on a named protocol face, by `upstream`
    // and `face`: 1 it answered a minimal request, 0 the face does not exist at
    // this base, -1 it answered something that is not about the face (quota,
    // entitlement, transport).
    //
    // `api_style` is where an entry was *admitted*, and nothing re-asks it
    // afterwards, so a vendor that serves several faces contributes to one of
    // them and is invisible on the others: on 2026-10-25 the only face the
    // business and the eval arm call had no serving member for ten days while a
    // second configured member answered that same face. The pool could have
    // served it there and nothing said so.
    //
    // Three values rather than two because the repair differs: 0 is a wrong
    // base_url and no amount of waiting fixes it, -1 is an account or a network
    // and this entry may well be usable later. Collapsing -1 into 0 would send
    // an operator to edit an address that is correct.
    LLM_UPSTREAM_FACE_ANSWERS => "llm_upstream_face_answers",
    LLM_UPSTREAM_CONSECUTIVE_FAILURES => "llm_upstream_consecutive_failures",
    // Where each upstream sits in the order a call tries the pool, 0 first, by
    // `upstream`. The order is not the declared one: candidates are demoted by
    // a stable sort on suspicion, so a suspect sinks behind everything that has
    // not failed while config order is kept inside each group, and once the
    // last healthy one fails this series converges on the declared order.
    //
    // The declared order arrives in a Secret at process start, and nothing the
    // running process says otherwise distinguishes "loaded the order that is in
    // the Secret now" from "started after the edit and still holds the order it
    // read before it" -- the two are inferred apart from the Secret's content
    // plus a pod-start time, which is a claim about the manifest and not a
    // reading of the process. This series is that reading: values that move
    // between rollouts say the reorder reached the running pool.
    LLM_UPSTREAM_POSITION => "llm_upstream_position",
    LLM_POOL_AVAILABLE => "llm_pool_available",
    LLM_POOL_FACE_SERVING => "llm_pool_face_serving",
    LLM_POOL_FACE_UPSTREAMS => "llm_pool_face_upstreams",
    LLM_POOL_EVIDENCED_RECOVERY_UNIX => "llm_pool_evidenced_recovery_unix",
    LLM_POOL_NEXT_ATTEMPT_UNIX => "llm_pool_next_attempt_unix",
    LLM_POOL_QUOTA_WINDOW_SECS => "llm_pool_quota_window_secs",
    LLM_POOL_ALLOWANCE_TOKENS => "llm_pool_allowance_tokens",
    LLM_POOL_SPENT_TOKENS => "llm_pool_spent_tokens",
    LLM_POOL_RESERVE_TOKENS => "llm_pool_reserve_tokens",
    // How many finished jobs the reserve above was measured over. The reserve is
    // a percentile, and a percentile over three jobs returns a number of the same
    // shape as one over three hundred; the cost alone cannot say which, so every
    // reader that turns it into headroom is trusting a quantity whose solidity is
    // on no other series. The two travel together in the contract payload for the
    // same reason, and this publishes the population on the face the value is
    // already on.
    LLM_POOL_RESERVE_POPULATION => "llm_pool_reserve_population",
    // When the earliest of those jobs ran, unix seconds. The population says how
    // many tasks the percentile rests on and this says whether they reach back
    // across the window it is asked for: a deployment three hours old consults a
    // thirty-day quantile and gets an answer over three hours of work, with no
    // evidence of the difference anywhere in the value.
    LLM_POOL_RESERVE_SINCE_UNIX => "llm_pool_reserve_since_unix",
    LLM_POOL_PACED_UPSTREAMS => "llm_pool_paced_upstreams",
    // The pool gauges above sum only the members whose window was **measured**,
    // and the request path's `blocked` decides for the whole pool from that sum.
    // A member with no ruler therefore contributes zero, and on every one of
    // those gauges that is the same bytes as "this member has no allowance
    // left" -- while one of the two is repaired by waiting and the other by
    // getting a ruler onto the member. These two name which member, and why it
    // is not in the sum: one series per configured member (1 measured, 0 not,
    // `reason` naming the missing measurement), and the count of members that
    // are not. The count is published on its own rather than left to
    // `pool_size - paced` because the pool's size is on no metric face.
    LLM_POOL_UPSTREAM_WITH_RULER => "llm_pool_upstream_with_ruler",
    LLM_POOL_UPSTREAMS_WITHOUT_RULER => "llm_pool_upstreams_without_ruler",
    LLM_POOL_SIGNAL_CONNECTED => "llm_pool_signal_connected",
    // What the ledger says was spent since the start of the UTC day, and which
    // tick that sum came from. Two series because neither is a reading alone:
    // a spend of 0 is either "nothing spent yet today" or "the ledger could not
    // be read", and those call for opposite repairs — one is a quiet platform,
    // the other is a blind meter. Both are written every tick, because a series
    // that stops being written keeps its last value in the shared store and
    // would stand as the current reading forever. They are a reading only: the
    // platform does not put a ceiling on its own work, so nothing refuses a call
    // from these.
    LLM_BUDGET_SPENT_TOKENS => "llm_budget_spent_tokens",
    LLM_BUDGET_MEASURED_AT_UNIX => "llm_budget_measured_at_unix",

    // cog-gateway — the audited LLM channel.
    //
    // The one outbound mouth where a request body that may hold host document
    // text is looked at before it leaves. Labelled by `outcome` because the
    // ways a request can end here are different defects that call for opposite
    // fixes, and one counter would report them all as "the channel is not
    // working": a body that carried credential-shaped text is a security
    // event, a body over the auditable bound says the bound or the caller is
    // wrong, and a body rejected by the switch says the channel is closed by
    // configuration. That last one especially has to be readable on its own:
    // otherwise a channel nobody calls and a channel closed by configuration
    // look the same, which is how a switch that was never set (or set the
    // wrong way round) stays invisible. The cell vocabulary is declared next
    // to the producer, not here.
    AUDITED_LLM_REQUESTS_TOTAL => "audited_llm_requests_total",

    // cog-gateway — the notification signing face.
    //
    // The gateway is the only holder of the platform robots' signing keys, and
    // this counts what each request for a signature got. Labelled by `outlet`
    // and `outcome`, and every outcome is published at zero as well: a cell of
    // zero says the path ran and had nothing to do, an absent cell says it never
    // ran -- and a signing face nothing calls and one that refuses everything
    // render the same on the delivery readings alone (neither sends a message).
    // The outlet label holds a known outlet name or the literal `unknown`; the
    // asked-for name itself stays out of it, because the one request that
    // carries a name from outside this process is the one that is not an outlet.
    NOTIFICATION_SIGN_TOTAL => "cogneva_notification_sign_total",

    // cog-orchestrator — task delivery.
    DAG_STALLED_SCHEDULED_RECLAIMED => "cogneva_dag_stalled_scheduled_reclaimed_total",

    // The same repair, on the rows it took back while the queue still reported
    // undelivered entries. Kept apart from the total above because the two say
    // opposite things about the message: without a backlog the repair is
    // acting on a message that is gone, with one it is acting on a message
    // that may still be in line. A single total would make a backlog that
    // never drains -- the case the wait is supposed to cover -- read exactly
    // like the loss it is meant to distinguish.
    DAG_STALLED_SCHEDULED_RECLAIMED_WITH_BACKLOG => "cogneva_dag_stalled_scheduled_reclaimed_with_backlog_total",

    // Rows the repair left in `Scheduled` because their queue reports
    // undelivered entries. This is the other half of the pair above: it is the
    // wait doing its job, and it is a counter rather than a gauge for the same
    // reason -- the state it describes clears itself.
    DAG_STALLED_SCHEDULED_DEFERRED_FOR_BACKLOG => "cogneva_dag_stalled_scheduled_deferred_for_backlog_total",

    // cog-orchestrator — how much of the principle corpus has a gate, by the
    // four answers a row may hold.
    //
    // The census was already printed every pass by the loop that maintains the
    // account, and a line in a pod's log is a reading that dies with the pod:
    // nothing could alert on it, and "how much of the corpus is gated" -- the
    // one question the account exists to answer -- had no surface outside a
    // human running a query. The label is the closed set `enforced`,
    // `unenforced`, `retired` and `unmeasured`; every value is written on every
    // pass, zero included, so an absent series means the loop stopped
    // publishing rather than that this account holds none of that answer.
    PRINCIPLE_LEDGER_ROWS => "cogneva_principle_ledger_rows",

    // cog-orchestrator — the same account counted the other way: by the tier
    // each carrier said its principle would be checked at.
    //
    // A separate name rather than a second label on the census above, because
    // the two are different partitions of one population: labelled together,
    // `sum()` over the family would count the corpus twice, and the obvious
    // reading of a total that is wrong by exactly a factor of two is a corpus
    // that doubled. The tier census was computed on every pass and only ever
    // printed, and the question it answers is not the census's: a row reading
    // `unenforced` is in the same bucket whether its carrier said a walk of the
    // tree would settle it or said nothing at all, and those two debts are paid
    // in different coins -- one needs a gate written, the other first needs
    // somebody to say what the gate would look like. `undeclared` is the cell
    // that names the second.
    PRINCIPLE_LEDGER_CHECK_KIND_ROWS => "cogneva_principle_ledger_check_kind_rows",

    // cog-orchestrator — how many times a gate has refused work because of one
    // principle, by `slug`.
    //
    // The account already stored this per row (`refusals`, folded from what the
    // gates report), and nothing outside a SQL query could read it: the census
    // beside it answers "how much of the corpus has a gate", never "which rule
    // keeps stopping work". The value is the account's cumulative count, not a
    // process counter, so the name carries no `_total` suffix -- `rate()` or
    // `increase()` over a row that is only written when the fold runs would
    // invent a rate that the fold's cadence, not the refusals, decides.
    //
    // A slug with no series here has never been refused, and that is the whole
    // of what absence means: the producer writes every row the account holds
    // with `refusals > 0`, so a slug that drops out dropped out because the
    // account retired the row, not because the count fell. The label is a slug
    // and a slug is not always a principle: the gates also record their own
    // self-faults under names like `principle-gate.sh:self-fault:drill`, and
    // they are published as they are rather than filtered here.
    PRINCIPLE_REFUSALS => "cogneva_principle_refusals",

    // cog-orchestrator — ready tasks the pool's window could not pay for, and
    // the counts that decision was made on.
    //
    // The gate itself is only observable through this pair. A gate that is
    // holding work back and a publisher with nothing to publish leave exactly
    // the same trace: no dispatch. The counter says the gate refused, labelled
    // by `stream` and `reason` -- both closed sets, `shared`/`local` and
    // `window_exhausted` / `pool_unavailable` / `pass_ceiling` -- so a reader
    // can tell which queue is starved instead of seeing one anonymous refusal.
    //
    // Deliberately not labelled by a number or a task id: a label value that
    // changes with the tick is not part of a series' identity, and the gauge
    // store freezes the last value of every label combination it has seen.
    READY_ADMISSION_HELD_TOTAL => "cogneva_ready_admission_held_total",

    // cog-orchestrator — ready tasks held back because one of their project's
    // board columns was at its configured limit.
    //
    // Labelled by `project` and `column`, both read off work the publisher was
    // about to dispatch: `project` is the goal root the task belongs to (or the
    // named bucket for tasks outside every goal), `column` is one of the board's
    // status names. The limit itself is never a label -- it is configuration,
    // and a series that renames itself when a setting changes is a series whose
    // history cannot be read.
    //
    // A counter, because the state it describes clears itself as work finishes:
    // a gauge would read zero both before the column filled and after it
    // drained. Absent until a column first refuses, which is also what keeps an
    // unconfigured board from publishing a row of zeros.
    //
    // The `_total` suffix is not decoration: the exposition types a series from
    // its name alone, and the live probe selects counters by the same suffix,
    // so a counter named without it renders as a gauge holding a sum.
    BOARD_COLUMN_HELD_TOTAL => "cogneva_board_column_held_total",

    // The same pass's arithmetic, as one cell per `stream` × `kind` pair.
    //
    // `kind` is a closed set: `bounded` (1 when the window's allowance bounded
    // this pass, 0 when nothing measured one), `window_jobs` (how many jobs the
    // remaining allowance pays for), `owed` (tasks already scheduled or running
    // on that queue) and `admitted` (what this pass put on the wire). Every pass
    // writes `bounded`, `owed` and `admitted`; `window_jobs` is written only
    // when a bound exists, because a window that was never measured is not a
    // window that pays for zero jobs, and a zero there would read as the
    // strongest possible refusal. A reader tells the two apart by `bounded`.
    READY_ADMISSION_READINGS => "cogneva_ready_admission_readings",

    // cog-orchestrator — what one change-ledger dispatch pass did with the set
    // still owed, per tick.
    //
    // The ledger records a row per pass in its own table, and that row answers
    // the question only while someone is querying the database: a curve that
    // nothing serves to the metric store cannot be alerted on, and the shape
    // that matters -- a set that is owed and is not coming down -- was published
    // nowhere. Every count in the row is a cell here, named by `readout`, so the
    // pass's own arithmetic is readable as a series and the reader can ask
    // whether the debt fell without opening the table.
    //
    // The counts are published together because they only mean anything
    // together: `pending` alone says work is owed, and the per-row outcomes say
    // where this pass's share of it went. A pass that is holding work back has
    // `held_back` where a pass that dispatched has `submitted`/`failed`.
    //
    // The values are the ones the pass wrote to its table, not a second
    // computation: the reading face is a view of the account, and a view that
    // measured again would be a second account.
    //
    // `readout` is the whole label set, and that is a correction rather than a
    // simplification. This family once carried the pass's tick-varying answer to
    // "why does this tick look the way it does" -- the decision and the pool
    // reading -- in two more labels, so that the count and its reason were one
    // series. A label whose value changes with the tick is not part of a series'
    // identity, and on a gauge the store keeps the newest row of every label set
    // it has ever held and exempts it from its own sweep, so every combination
    // the pass ever reached left its last value frozen and served for ever: a
    // retired combination read as a live count, and the reader's `max` across
    // label sets preferred the frozen one whenever it was larger. The reason has
    // a family of its own now -- `cogneva_change_dispatch_verdict`, whose key
    // set is fixed and whose every cell is written on every pass -- and this
    // name is retired rather than re-scoped, because a name cannot be served
    // under two label sets at once and the rows already stored under the old one
    // would go on being served. Retirement is what stops the exposition serving
    // them and what lets the release pass delete them.
    CHANGE_DISPATCH_READOUTS => "cogneva_change_dispatch_readouts",

    // cog-orchestrator — the account counted by the face of the programme its
    // declaring documents named.
    //
    // The grouping was in the account's contents and not in its shape: the rows
    // a face's documents asked for were there, but the heading was only in prose,
    // so "how far along is this face" was a substring search whose answer changed
    // with who typed it. Two labels rather than one: `block` is the face (a
    // closed set, with `none` for rows no document filed and for rows whose
    // documents disagree) and `reading` is which count -- `landed`, `owed`,
    // `retired`, `undecided`, or `oldest_unmeasured_seconds`, the age of the owed
    // row that has gone longest without a measurement, taken from when it
    // entered if nothing has measured it at all. Every face is written on every
    // pass, including the ones that owe nothing: a face missing from the census
    // would read as a face that finished.
    //
    // `undecided` is the one cell that is not a bucket: it counts rows already
    // inside `owed`, the ones nothing has settled the question about. `landed` +
    // `owed` + `retired` is the face's population and stays that; adding
    // `undecided` to them counts those rows twice. It is written on every face
    // at zero when there are none, because the alternative -- skipping it like
    // the age -- would make "nobody has asked" the same reading as "nothing
    // unsettled".
    CHANGE_BLOCK_CENSUS => "cogneva_change_block_census",

    // cog-orchestrator — the verdict one change-ledger dispatch pass reached,
    // as one cell per combination of the two closed sets.
    //
    // Every cell is written on every pass, the current one as `1` and the rest
    // as `0`, and that is what makes the family readable rather than merely
    // present: a family that published only the combination its pass reached
    // would leave the previous combination's `1` standing, and a reader of the
    // debt would see a pass that stopped happening as a verdict that never
    // changes. The key set is fixed by the two enumerations, so the cells a
    // pass's future can mint are bounded by the sets, and a pass that a new
    // combination would otherwise have frozen has already been written as `0`.
    //
    // `0` here is a reading and not a placeholder: it says this pass did not
    // reach that verdict, which is the same answer a pass that never reaches it
    // gives, and the two are told apart by reading the cell that is `1`.
    CHANGE_DISPATCH_VERDICT => "cogneva_change_dispatch_verdict",

    // cog-orchestrator — the write windows the account holds, counted by what
    // the record says about each one.
    //
    // A write window is the record that one named account may write to a path,
    // under whose approval, until when. Until this family existed the record
    // lived in prose: the name of the approver in a paragraph, with no window
    // and nothing the running system could read, so the face that owes "how
    // many write windows are approved right now" had no producer and its reading
    // was absent -- and an absent reading there is the difference between "no
    // one is writing" and "no one is keeping track".
    //
    // `readout` carries the four cells, and each is a count over the same rows
    // under a different question: `approved` is the windows that stand now,
    // `overlap` is the path scopes with more than one live approved writer,
    // `inconsistent` is the rows that carry both an approval and a revocation,
    // and `self_approved` is the live windows whose approver is their writer.
    // The last two are not subsets of the first two by accident: a
    // self-approving row is never one of `approved` (the approval rule refuses
    // it), and a row that was approved and then withdrawn is not read as merely
    // revoked, because that would take the approval it once had out of the
    // reading. Written on every pass, so a window that lapses stops being
    // counted instead of standing at its last value; nothing published is not
    // zero -- a table that cannot be read publishes no cell at all.
    CHANGE_WRITE_WINDOWS => "cogneva_change_write_windows",

    // cog-orchestrator — what the last pass of the in-effect check found, one
    // cell per answer it can reach: `waiting` (landed rows the running revision
    // does not carry), `shipped` (it carries all of them), `unmeasured` (the
    // check could not be taken).
    //
    // Three cells rather than one number, because the account's own loop already
    // answered this question on every pass and had nowhere to put the answer:
    // the age went to a log line, and the two ways of having no age -- nothing
    // is waiting, and nobody could look -- were the same `None` in the code. A
    // rollout that is behind and a rollout nobody can see are different facts
    // with different repairs, and this is the family that keeps them apart. All
    // three cells are written on every pass, the reached one as `1` and the
    // rest as `0`: the store serves the newest row of every label set it has
    // ever held, so a cell written only when it occurs would go on standing at
    // its last value and the pass that stopped walking into it could not be
    // read.
    CHANGE_LEDGER_IN_EFFECT => "cogneva_change_ledger_in_effect",

    // cog-orchestrator — how long the oldest landed change has been waiting to
    // run, in seconds, published beside the family above.
    //
    // Two names rather than one, and the second is not redundant: the state is
    // a label because it is a closed set, while the age is a number a reader
    // compares against a bound. Folding the age into the state family would
    // make `0` mean "shipped", "unmeasured" and "a landing from this second" at
    // once -- the exact collision the state family exists to prevent. Written on
    // every pass (`0` when nothing is waiting), so an age does not stand after
    // the wait ends; a reader that wants "how long" reads it only where the
    // state family says `waiting`.
    CHANGE_LEDGER_OUT_OF_EFFECT_OLDEST_SECONDS => "cogneva_change_ledger_out_of_effect_oldest_seconds",

    // cog-orchestrator — what the re-measurement intake is holding: how many
    // requests nobody has folded yet, and how long the oldest has been waiting.
    //
    // The intake is the only way a row retired under a rule that lives outside
    // the measured tree gets looked at again, and until these two existed the
    // whole of it was a line in a pass log: a request that stopped folding left
    // no series, so the only reader who could see it was one who already
    // suspected it. Written on every pass, `0` included, so a queue that
    // drains cannot go on standing at its last depth.
    CHANGE_LEDGER_REMEASURE_PENDING => "cogneva_change_ledger_remeasure_pending",

    // cog-orchestrator — the age of the oldest unfolded re-measurement request,
    // in seconds, published beside the count above.
    //
    // A count cannot say how long: twenty-seven requests that arrived this
    // second and twenty-seven that have been stuck since yesterday are the same
    // number and different repairs. `0` means the queue is empty -- the count
    // is what tells a reader that, because the age of a request declared this
    // very second would also round to zero.
    CHANGE_LEDGER_REMEASURE_OLDEST_SECONDS => "cogneva_change_ledger_remeasure_oldest_seconds",

    // cog-orchestrator — how many outstanding rows are stamped as waiting on the
    // LLM pool, and how long the oldest of them has been stamped.
    //
    // The block is written by a pass on its own verdict and cleared by the first
    // pass that can hand work over, so it is a claim about the queue that
    // outlives the pass that made it. Nothing else reports it. The pool's own
    // rules read the pool, and they go green the moment it recovers -- which is
    // exactly the state in which a stamped row that no pass ever came back to
    // clear is still stamped, still counted as owed, and now indistinguishable
    // from a row that was never handed over. The pair fires while the pool is
    // healthy, which no other family can express.
    //
    // `rows` and the age travel together for the reason the re-measurement pair
    // does: a count cannot say how long, and an age alone cannot say how much.
    // Read from the stamps on the rows, not from the pass's verdict, because the
    // question is the rows': a pass that died before clearing left no verdict to
    // ask. Written on every pass, `0` included, so a queue that drains cannot go
    // on standing at its last depth.
    CHANGE_DISPATCH_POOL_BLOCKED_ROWS => "cogneva_change_dispatch_pool_blocked_rows",

    // cog-orchestrator — how long the oldest row stamped as waiting on the pool
    // has been waiting, in seconds, published beside the count above.
    //
    // `0` means no row carries the block, not a row stamped this second: an age
    // of zero and an empty set are the same number here, and the count is what
    // separates them. The age is the oldest row's rather than an average, so a
    // single row nobody releases stays visible while the rest of the queue turns
    // over underneath it.
    CHANGE_DISPATCH_POOL_BLOCKED_OLDEST_SECONDS => "cogneva_change_dispatch_pool_blocked_oldest_seconds",

    // cog-orchestrator — rows a change-ledger pass tried to measure and could
    // not, counted as they enter that set, by reason and by which half of the
    // pass could not settle them.
    //
    // A row whose check settles to no state keeps the state it had and is
    // written nowhere: the only trace was a line in the pass's log, so "how
    // many rows this pass could not measure, and why" had no series, no panel
    // and no rule. The reason is a label because re-running the same look
    // answers differently for some of them and never for others, and the two
    // ask for different repairs. `half` keeps the outstanding set and the
    // landed one apart: they are asked by different sweeps, and a landed row
    // whose check stopped being answerable is a claim the account already made.
    //
    // Counted on entry rather than as occupancy: every pass re-asks the rows it
    // already knows are unsettleable, so a count of the set would read one
    // permanently unreadable row as a rising rate. Seeded at zero for every
    // reason on each half at start-up and only added to when a row enters, so
    // `increase()` over a window is the flow of rows becoming unmeasurable.
    CHANGE_LEDGER_MEASURE_UNSETTLED_TOTAL => "cogneva_change_ledger_measure_unsettled_total",

    // cog-orchestrator — keeping a running task's progress resumable.
    //
    // The write side of the resume chain: a task that loses its process (a
    // version rollout, a killed pod) can only continue from progress that was
    // already on disk before it died. Labelled by `outcome` because the ways
    // this fails are not the same defect — a snapshot that never reached the
    // store is a deployment with no checkpoint store configured, while a failed
    // call is a run-time fault — and a single counter would report them as one
    // "checkpointing is broken".
    TASK_CHECKPOINT => "cogneva_task_checkpoint_total",

    // cog-orchestrator — what the SOP dispatch loop did with each configured face
    // on each tick, and what it found while compiling.
    //
    // The loop turns a face's declared plan into DAG tasks. Its failure modes do
    // not look alike: a document that cannot be fetched, one that does not
    // compile, and a tick that simply found nothing changed are three different
    // facts, and a loop that stops reaching a face entirely is a fourth that only
    // the per-face cadence shows. `result` is a closed set (compiled,
    // skipped_unchanged, fetch_error, compile_error) so a reader counts outcomes
    // rather than parsing messages.
    SOP_DISPATCH_TOTAL => "sop_dispatch_total",
    // How many tasks one face's plan put into the DAG, summed over ticks. It is
    // separate from the tick counter because a compile that succeeds and injects
    // nothing and one that injects ten are the same `compiled` cell.
    SOP_TASKS_INJECTED_TOTAL => "sop_tasks_injected_total",
    // Tasks whose declared `role` named no row in the executor table. The role is
    // carried through verbatim and nothing maps it yet, so such a task is
    // injected and then may never be picked up; this cell is what makes that
    // silence visible before a mapping exists to close it.
    SOP_UNMAPPED_ROLE_TOTAL => "sop_unmapped_role_total",

    // cog-wiki — what each knowledge layer gave back when it was consulted.
    //
    // One cell per layer per call, by `layer` (memory, wiki) and `outcome`
    // (hit, empty, error, absent). The four are four different facts, and the
    // retrieval calls answer three of them with the same empty list: a layer
    // this process does not hold, a layer whose backend refused, and a layer
    // that answered and had nothing are one `Vec` at every caller, so a caller
    // choosing between "repair the backend" and "nothing has been stored yet"
    // has nothing to choose from.
    //
    // A counter rather than a gauge because the question belongs to the call,
    // and the pairs worth reading are per layer: `error` moving says a store
    // refused, `empty` moving while `hit` stands still says the store answers
    // and holds nothing that matches, and `absent` on a deployment that means
    // to have that layer names the wiring rather than the data.
    KNOWLEDGE_RETRIEVAL_TOTAL => "cogneva_knowledge_retrieval_total",

    // cog-agent — the event bus mouth a finished run's history goes out through.
    //
    // With a delivery mouth attached, a run's AgentEnd goes only here: the agent
    // keeps no WAL and wires no raw log, and the broadcast copy downstream is
    // replayed *from* the bus rather than kept beside it, so the bus holds the
    // only durable copy of a run whose history is long enough to matter. A drop
    // here is therefore not a buffer refilling; it is a conversation whose whole
    // record exists nowhere. Labelled by `cause` because the two ways it happens
    // want opposite repairs and one number would report them as one event:
    // `buffer_full` is the bounded queue at the entrance backing up while the
    // publisher retries a fault that clears, so it heals on its own; and
    // `payload_rejected` is the transport refusing the bytes for exceeding its
    // own frame limit, which no amount of waiting makes smaller -- the mouth
    // consumes that event once and the run is gone. A reader must not sum the
    // label away for that reason: the sum cannot tell "it was busy for a moment"
    // from "this conversation was lost".
    EVENT_BUS_DROPPED_TOTAL => "cogneva_event_bus_dropped_total",

    // The consumer side of the same bus: how many events a subscriber missed
    // because it fell behind its broadcast channel.
    //
    // The publisher counts what it could not hand out (`..._dropped_total`); this
    // counts what a subscriber could not keep up with. Both are losses on the
    // same channel and they want different repairs, so they are two families and
    // not two cells of one: a drop is the bus refusing, a lag is a reader that
    // was too slow to take what the bus offered.
    //
    // Labelled by `consumer` -- the axis that separates these, the way `cause`
    // separates the drops. Two consumers falling behind are two incidents with
    // two owners, so a reader must not sum the label away; the sibling rule's
    // doctrine (`max by (cause)`, never a bare sum) transfers with only the axis
    // renamed.
    //
    // At the time this was introduced only the gateway's three consumers emitted
    // it (the collaboration listener, the supervisor event cache, and the
    // evolution stream) while the rest of the platform's `Lagged` arms computed
    // the count and handed it to a `warn!` with nowhere to read it. As of
    // 2026-10-15 the tree holds 17 real arms (counted line by line:
    // `grep -rn "RecvError::Lagged" --include=*.rs crates/` minus the prose
    // lines), of which 12 record here, over ten `consumer` values: the three
    // above, the hook forwarder, the memory ingestor, the trace collector, the
    // supervisor's event aggregator, its autonomous event loop, its alert store,
    // the alert bridge, and the observability event stream (two arms, the
    // Postgres and the in-memory gateway, deliberately one consumer value).
    // Five do not, and each for a reason that is not an omission: two are
    // `#[cfg(test)]` mocks, two are the development stream backend in
    // `cog-stream` (no metrics backend in that crate at all), and one is
    // `EventAggregator::wait_and_drain` -- the arm the aggregator's consumer
    // runs on since 2026-10-21, when consuming moved off the window pass. Its
    // count is not lost: it adds into the window's totals and
    // `Supervisor::run_event_pass` writes them once per window under this same
    // `consumer` value, which is the cadence the handoff beside it already
    // uses. The `next_event` arm it replaced had no callers and is gone, so the
    // count of arms is unchanged. Two of the arms that
    // had no counter were also `while let Ok(..)` loops that read a lag as "the
    // channel closed" and exited -- a consumer that swallowed its own death.
    // Write the count as a count with a date rather than as a rule over "every
    // production arm": a rule's denominator is chosen by the rule ("an arm that
    // has a backend" is "an arm already wired"), so it is true however many arms
    // are missed and reads like a census. A reader must treat an absent
    // `consumer` as "that consumer does not publish yet", not as "it never fell
    // behind".
    EVENT_BUS_LAGGED_TOTAL => "cogneva_event_bus_lagged_total",

    // The same channel's other direction: what a subscriber actually took. Sits
    // beside the two loss counters because the three are one question -- the bus
    // offered, the reader took, and these are the two ways the offer failed to
    // become work done.
    //
    // A loss counter cannot answer "is this consumer alive". Zero lag is what a
    // consumer that is keeping up publishes *and* what a consumer that never
    // wakes publishes, and the loop's own age gauge does not separate them
    // either: for an event-driven loop the age is the process's age, so a
    // consumer that stopped is covered by no reading at all. This is the reading
    // that a stopped consumer has to fail to write.
    //
    // Labelled by `consumer`, the same axis as the lag beside it, so the two
    // divide: a rate of losses over a rate of takings is what "it is behind"
    // means, and neither series says it alone.
    //
    // Deliberately not wired to every consumer that emits the lag counter. This
    // one is incremented per event taken, and a store write is two statements,
    // so a consumer whose channel carries heartbeats or hook events would pay a
    // database round trip per event -- a reading that slows down the loop it
    // measures. It is wired where the channel is a task or a decision, and
    // adding a consumer to it means arguing the rate first. Absent means "this
    // consumer does not publish its throughput", the same reading as an absent
    // `consumer` on the lag counter.
    EVENT_BUS_CONSUMED_TOTAL => "cogneva_event_bus_consumed_total",

    // The producer side of the same channel: what the pipeline handed to the
    // broadcast the two consumer-side counters above read.
    //
    // The three are one question and any two of them are not. `..._lagged_total`
    // is written only when a reader falls behind, so a consumer keeping up and
    // one that stopped both leave it flat; `..._consumed_total` is the reading a
    // stopped consumer has to fail to write, but a rate of takings on its own
    // cannot say whether the flat line is a reader that stopped or a channel
    // nothing was put on. This is the operand that decides it -- how much
    // traffic the channel carried -- and it is the one both of the others are
    // read against.
    //
    // Labelled by `kind`, the event variant. The label is what turns a firing
    // alert into a repair: "the consumer stopped taking" is the fault, and which
    // kinds were offered while nothing took them is the first thing the person
    // holding it asks. A single total would answer the first question only.
    //
    // Counted where the executor offers the event, not where a send returned, so
    // the series is the producer's own claim and does not depend on how many
    // subscribers happen to be attached. An event offered to a channel with no
    // reader is the one case the consumer side can never show, and counting only
    // the delivered ones would hide exactly it.
    //
    // The pair is the one `crate::loop_health` asks for in its own words: for a
    // loop marked event-driven it documents an idle-on-empty-queue consumer and
    // one wedged inside a handler as the same picture from the loop's age face,
    // and names the queue's reading as the one that separates them. This is that
    // reading from the offering side.
    EVENT_BUS_EMITTED_TOTAL => "cogneva_event_bus_emitted_total",

    // What one batch consumer did with the events it took: `stage="taken"` is
    // what its drain removed from the channel this pass, `stage="attempted"` is
    // how many of those it handed on for storage.
    //
    // Not a new `consumer` value on `..._consumed_total`. That counter is
    // incremented once per event, and its help says a consumer whose write
    // covers a whole pass has to mint its own name, or `rate()` means two
    // different things under one family and two label values stop being
    // comparable. This is that name: one write per pass, carrying that pass's
    // event count, so both sides of the comparison are measured in events.
    //
    // The pair answers what neither loss counter can ask. A pass that took
    // events and handed none on is a consumer that is running and a handoff that
    // is not -- from the lag counter alone that is identical to a consumer with
    // nothing to do, and from the table's row count it is identical to a channel
    // nothing was ever offered. `taken` against `attempted` says the first;
    // either against the store's rows says the second.
    //
    // Today's aggregator makes the two stages equal by construction: every
    // event a pass receives is handed on. The second stage is here for the
    // filter a future consumer will add, and the identity must not be read as
    // proof that the handoff worked -- it is one number until the stages
    // diverge. Events this consumer lagged past never reach either stage; they
    // are counted under the same `consumer` in `..._lagged_total`.
    EVENT_BUS_HANDOFF_TOTAL => "cogneva_event_bus_handoff_total",

    // The store's half of that handoff: a write whose persistence failed.
    //
    // Same unit as the handoff above -- events -- which is what makes
    // subtraction valid, but a different writer and a different cadence: the
    // handoff is written by the consumer once per pass with that pass's event
    // count, while this one is written where the outcome is known, once per
    // failed event. They stay two names rather than one family with a label
    // because one side is a batch written before the outcome exists and the
    // other is a count written after it; folding them would put two write
    // cadences and two sets of labels under one name. Subtracting this from
    // `..._handoff_total{stage="attempted"}` is what turns "the events reached
    // the handoff" into "the events reached the table".
    //
    // It exists because the alternative was a log line. An empty table with a
    // warning per failed write reads the same, at the metric surface, as a table
    // nothing was ever offered -- exactly the pair this family was added to
    // separate.
    OBSERVABILITY_EVENT_PERSIST_FAILED_TOTAL => "cogneva_observability_event_persist_failed_total",

    // How many connections each PostgreSQL pool may hold, by `pool`
    // (users/messages/config/explain) and by `source`: `server` when the
    // server's own max_connections divided among the sharing processes and
    // pools is what binds, `config_lower` when the configured ceiling is
    // lower, `tied` when the two are equal and neither decided anything, and
    // `server_unreadable` when the server was never read and the configured
    // value stands unmeasured. The last one is a blind spot, not a design
    // choice, and must not share a label value with it: one alert rule keyed
    // on `source` should be able to wake somebody for the blindness without
    // also firing on a ceiling somebody chose.
    //
    // The value has to be readable, not just obeyed. Four pools against one
    // database multiply the configured ceiling into a local allowance several
    // times the server's, and the failure that follows -- every writer waiting
    // out its acquire timeout while `pg_stat_activity` still shows idle
    // connections held by the sibling pools -- says nothing about which
    // ceiling was in force. A reader is told what the fix did only by this
    // gauge; without it the only witness is a startup log line that a later
    // reader of the store never sees. `source` is what keeps a lowered ceiling
    // from being read as the server's answer when the server was never asked.
    PG_POOL_MAX_CONNECTIONS => "cogneva_pg_pool_max_connections",

    // cog-agent — what happened to a tool output too large for the context.
    //
    // Two cells that must be read together. `truncated` says a tool's output did
    // not fit and its tail was dropped from the model's view; without it the
    // drop is visible only inside the text the model receives, so nothing
    // outside the conversation can count how much was withheld. `archive_failed`
    // says the tail was meant to stay reachable by reference and does not, so a
    // reference in a truncation marker points at a source that never landed and
    // a caller following it finds nothing. Kept as separate series by `tool`
    // because the first is expected traffic and the second is a defect: folding
    // them would let a failing archive read as ordinary truncation.
    //
    // The count of cuts says nothing about how much was withheld, and one cut
    // that dropped ten characters and one that dropped a hundred thousand are
    // the same event on it. `truncated_chars` is the other half of the same
    // event, in the unit the loss is actually in, and it is what turns the
    // count into a share of the conversation spent on oversized results. The
    // two are written from one place and one computation, so the count is
    // always the number of increments and the characters the sum of them.
    //
    // The third is the read side's own fate: a read found the source but its
    // payload outgrew the byte budget the caller set, so no bytes came back.
    // Without it, "the source is bigger than you asked for" is indistinguishable
    // from "the read failed" and, worse, from an empty answer -- a bounded read
    // and a source that is not there would look the same. Kept by `tool` for the
    // same reason the pair is: one tool's reads blowing past the budget names
    // that tool's payload shape rather than reads as a whole.
    //
    // Every cut ends in exactly one of two fates -- the tail was stored, or it
    // was not and `archive_failed` says why -- so a cut that reaches neither is
    // one the reading cannot see. The two fates have to add up to the count for
    // the landed share to be a share rather than a ceiling, which is why the
    // causes are a closed set and why a run that had no key to archive under
    // gets a cause of its own instead of being skipped.
    TOOL_OUTPUT_TRUNCATED_TOTAL => "cogneva_tool_output_truncated_total",
    TOOL_OUTPUT_TRUNCATED_CHARS_TOTAL => "cogneva_tool_output_truncated_chars_total",
    TOOL_OUTPUT_ARCHIVE_FAILED_TOTAL => "cogneva_tool_output_archive_failed_total",
    TOOL_OUTPUT_FETCH_BUDGET_EXHAUSTED_TOTAL => "cogneva_tool_output_fetch_budget_exhausted_total",

    // cog-orchestrator -- what the backup account holds, folded into one series
    // by `readout`.
    //
    // Every value is read from the account rather than computed a second time:
    // the reading face is a view of the row a run wrote, and a view that
    // measured the volume again would be a second account, disagreeing with the
    // first exactly when it mattered.
    //
    // The readouts are one family rather than one series each because they are
    // only meaningful together, and a reader who has to join families to ask
    // "is the backup working" reads one of the numbers without the other. The
    // two that decide that question are the ones that cannot be told apart from
    // a count: a week of successes and a week of failures leave the same number
    // of packages, so what separates them is the age of the newest success. That
    // age and the count of successes are one reading about one thing.
    //
    // Units are in the readout's own name -- `_seconds`, `_bytes` -- because one
    // series carries several, and a value whose unit has to be inferred from the
    // metric name is a value that will be read in the wrong one.
    //
    // Two readouts are absent rather than zero when they have nothing to say,
    // and the absence is the fact: `latest_success_age_seconds` is missing until
    // a run has ever succeeded (a zero would claim one just did), and
    // `oldest_running_age_seconds` is missing unless a run is still open (a zero
    // would claim one just started). A reader that needs "is anything open"
    // reads `runs_running`, which is always present.
    BACKUP_RUN_READINGS => "cogneva_backup_run_readings",

    // cog-reflection -- what this process loaded from the data root it runs on,
    // one family keyed by `readout` and labelled by `data_dir` and `store`.
    //
    // The labels are the reading. Two processes on one volume with different
    // `data_dir` are two stores, and a face that named only the workload would
    // merge them into one number that describes neither; the store is named
    // apart from its directory for the same reason, because the same directory
    // can hold several stores and the backup package keeps them apart too.
    //
    // The restore pair -- `restore_expected_*` against `restore_loaded_*` -- is
    // published once, at the start that follows a restore, and never refreshed:
    // the policy store moves on with evolution, and republishing one side of an
    // old comparison would turn it into two instants being compared. Both sides
    // are published rather than a single equal/not-equal: a mismatch is only
    // actionable if the reading says which side was what.
    POLICY_STORE_READINGS => "cogneva_policy_store_readings",

    // cog-orchestrator — the resource account's event half, as the loop that
    // folds it reads it back out of the table.
    //
    // The census is a gauge and not a counter, and the difference is the whole
    // reason these series exist: the count it carries is republished from the
    // table on every pass, so a process that restarts re-reads the same total
    // rather than starting at zero. A counter here would have the shape of the
    // in-process tally this account was built to replace -- the one that
    // published 244 series of which 243 began at zero, because every pod's
    // life was its own history.
    //
    // The labels are closed sets on both axes (`kind` and `outcome`), and every
    // pair is written on every pass, zero included: a cell that appears only
    // after its first event cannot be read as "none yet", which is exactly the
    // reading a refusal count is asked for.
    RESOURCE_EVENTS => "cogneva_resource_events",
    RESOURCE_EVENTS_PENDING => "cogneva_resource_events_pending",
    RESOURCE_EVENTS_OLDEST_UNFOLDED_AGE_SECONDS => "cogneva_resource_events_oldest_unfolded_age_seconds",
    RESOURCE_EVENTS_DROPPED_TOTAL => "cogneva_resource_events_dropped_total",
}

/// Whether `name` is a series a build can still write.
///
/// Retired names answer `false` here as well: they are names the store may
/// still hold while nothing writes them, which is the state the exposition
/// reports rather than an error to be papered over.
pub fn is_registered_metric(name: &str) -> bool {
    ALL.iter().any(|registered| registered.as_str() == name)
}

/// The series the registry constant named `ident` writes, if it has one.
pub fn metric_name_of_ident(ident: &str) -> Option<MetricName> {
    IDENTIFIED
        .iter()
        .find(|(candidate, _)| *candidate == ident)
        .map(|(_, metric)| *metric)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn the_registry_holds_each_name_once() {
        let unique: BTreeSet<&str> = ALL.iter().map(|n| n.as_str()).collect();
        assert_eq!(
            unique.len(),
            ALL.len(),
            "a duplicated name makes the list's length a lie about how many series exist"
        );
    }

    #[test]
    fn every_registered_name_is_lookupable_and_shaped_like_a_series() {
        for name in ALL {
            assert!(
                is_registered_metric(name.as_str()),
                "{name} cannot be found"
            );
            assert!(
                !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "{name} is not a series name: these reach label values and query expressions"
            );
        }
        assert!(!is_registered_metric("cogneva_no_such_series"));
    }

    #[test]
    fn no_name_is_both_published_and_retired() {
        for name in ALL {
            assert!(
                !crate::is_retired_metric(name.as_str()),
                "{name} is declared as published and as retired at once"
            );
        }
    }

    /// The identifier view is the constants' own list, seen from the other
    /// side: same length, every identifier unique, every entry resolving back
    /// to the series its constant declares. A check that reads a call site
    /// trusts this view to say which series a binding writes, so the view
    /// answering with a constant the registry does not have would make that
    /// check reject a live producer.
    #[test]
    fn every_identifier_resolves_to_the_series_its_constant_declares() {
        let mut idents: BTreeSet<&str> = BTreeSet::new();
        for (ident, metric) in IDENTIFIED {
            assert!(idents.insert(ident), "{ident} is declared twice");
            assert!(
                is_registered_metric(metric.as_str()),
                "{ident} resolves to a series that is not in ALL"
            );
            assert_eq!(
                metric_name_of_ident(ident).map(MetricName::as_str),
                Some(metric.as_str()),
                "{ident} does not resolve to the series it is paired with"
            );
        }
        assert_eq!(
            IDENTIFIED.len(),
            ALL.len(),
            "the two views of the registry disagree on how many series there are"
        );
        assert!(metric_name_of_ident("NO_SUCH_CONSTANT").is_none());
    }
}
