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

    // cog-memory — operation counters, latency, and ingest reconciliation.
    MEMORY_OPERATIONS_TOTAL => "memory_operations_total",
    MEMORY_OPERATION_LATENCY_MS => "memory_operation_latency_ms",
    MEMORY_OPERATION_ERRORS_TOTAL => "memory_operation_errors_total",
    MEMORY_UNEXTRACTED_RAW => "memory_unextracted_raw",
    MEMORY_UNEXTRACTED_RAW_AGED_OUT => "memory_unextracted_raw_aged_out",

    // cog-storage — the metric store's own readings and tier migration.
    METRICS_RETIRED_ROWS_REMOVED => "metrics_retired_rows_removed",
    METRICS_SAMPLES_OVER_CAPACITY => "metrics_samples_over_capacity",
    METRICS_SAMPLES_BYTES => "metrics_samples_bytes",
    METRICS_SAMPLES_BUDGET_ROWS => "metrics_samples_budget_rows",
    METRICS_SAMPLES_ROWS => "metrics_samples_rows",
    TIER_MIGRATION_TOTAL => "tier_migration_total",

    // cog-reflection — workspace index and generated-change fidelity.
    WORKTREE_INDEX_PRESENT => "cogneva_worktree_index_present",
    WORKTREE_INDEX_MISSING_FILES => "cogneva_worktree_index_missing_files",
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

    // cog-gateway — request accounting.
    HTTP_REQUESTS_TOTAL => "http_requests_total",
    HTTP_REQUEST_DURATION_MS => "http_request_duration_ms",

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
    LLM_REQUEST_PARAM_CLAMPED_TOTAL => "llm_request_param_clamped_total",
    LLM_UPSTREAM_CLIENT_ERRORS_TOTAL => "llm_upstream_client_errors_total",
    LLM_UPSTREAM_FAILURES_TOTAL => "llm_upstream_failures_total",
    LLM_UPSTREAM_HEALTHY => "llm_upstream_healthy",
    LLM_UPSTREAM_QUOTA_WINDOW_SECS => "llm_upstream_quota_window_secs",
    LLM_UPSTREAM_QUOTA_RESET_UNIX => "llm_upstream_quota_reset_unix",
    LLM_UPSTREAM_CONSECUTIVE_FAILURES => "llm_upstream_consecutive_failures",
    LLM_POOL_AVAILABLE => "llm_pool_available",
    LLM_POOL_EVIDENCED_RECOVERY_UNIX => "llm_pool_evidenced_recovery_unix",
    LLM_POOL_NEXT_ATTEMPT_UNIX => "llm_pool_next_attempt_unix",
    LLM_POOL_QUOTA_WINDOW_SECS => "llm_pool_quota_window_secs",
    LLM_POOL_SIGNAL_CONNECTED => "llm_pool_signal_connected",

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
