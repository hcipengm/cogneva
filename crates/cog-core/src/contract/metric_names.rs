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

    // cog-gateway — request accounting.
    HTTP_REQUESTS_TOTAL => "http_requests_total",
    HTTP_REQUEST_DURATION_MS => "http_request_duration_ms",

    // cog-gateway — LLM pool and upstream health.
    LLM_CALLS_TOTAL => "llm_calls_total",
    LLM_CALL_LATENCY_MS => "llm_call_latency_ms",
    LLM_TOKENS_TOTAL => "llm_tokens_total",
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
