//! What each recorded series means, by kind.
//!
//! A name says what is measured only when it is self-explanatory, and most of
//! these are not: `metrics_samples_over_capacity` is a 1-or-0 reading whose
//! point is *when* it is 1, and a series that is absent on purpose reads as a
//! dead producer to anyone who has not been told. The sentence that fixes that
//! lives here, next to the closed set of names, because the same series is
//! served on more than one exposition: an exposition holding its own copy of the
//! wording would describe one series two ways, and a reader comparing the two
//! would have to guess which of them is the measurement.
//!
//! The rule for membership: something in this codebase records the name. A
//! series nothing produces is not an undocumented measurement, it is an absent
//! one, and describing it here would assert a reading that never arrives.
//!
//! The tables are not a list of what to serve. What a scrape carries is decided
//! by what the storage backend holds; this module only says what a name cannot
//! say about itself.

use crate::MetricType;

/// Descriptions for the counter series.
const COUNTER_HELP: &[(&str, &str)] = &[
    (
        "evolution_generated_change_hunks_total",
        "Hunks carried by generated change artifacts, summed over rounds; \
         the ratio against the faithful count in the same window is generation \
         fidelity, and the unfaithful remainder is what the apply gate throws away",
    ),
    (
        "evolution_generated_change_hunks_faithful",
        "Subset of the above whose context was found in the target tree",
    ),
    (
        "evolution_generated_change_files_total",
        "Files touched by generated change artifacts, summed over rounds; a \
         low faithful ratio here with a high hunk ratio means artifacts aimed \
         at the wrong revision, not artifacts written wrong",
    ),
    (
        "evolution_generated_change_files_faithful",
        "Subset of the above whose whole file patch applied",
    ),
    (
        "memory_operations_total",
        "Total number of memory backend operations",
    ),
    (
        "memory_operation_errors_total",
        "Total number of failed memory backend operations",
    ),
    (
        crate::metric_names::HTTP_REQUESTS_TOTAL.as_str(),
        "Total number of HTTP requests",
    ),
    (
        "tier_migration_total",
        "Total number of storage tier migrations",
    ),
    (
        "llm_calls_total",
        "Total number of LLM upstream calls by result",
    ),
    (
        "llm_tokens_total",
        "Total LLM tokens consumed, split by `kind` into input, output and \
         cached. `cached` is the part of the input the upstream served from its \
         own cache; on an OpenAI-compatible upstream it is a subset of `input`, \
         on Anthropic it is disjoint from it, so read the ratio according to the \
         upstream's protocol. Published at zero too: a missing cell says the \
         metering path never ran for that upstream and actor, a cell holding \
         zero says it ran and found nothing",
    ),
    (
        crate::metric_names::LLM_USAGE_READINGS_TOTAL.as_str(),
        "What the read of each finished LLM response found, by upstream and \
         outcome. `absent` and `not_asked` both mean no number arrived, and \
         they are split by whose silence it is. `absent` is a call that did ask \
         and got no usage frame back: the token counts recorded beside that \
         call are zeros because the upstream said nothing, not because nothing \
         was used, and while it climbs every token total on that upstream is \
         blind. `not_asked` is a call that never asked -- the request went out \
         with no stream_options, so no usage frame was ever due and nothing was \
         ignored; the fix there is on our own request side, and it is the cell \
         to read before blaming an upstream. `read` is an upstream that spoke, \
         including one that reported zero. `interrupted` is a response that \
         never finished, kept apart so an upstream that dies mid-stream cannot \
         be read as one that answers quietly",
    ),
    (
        "llm_upstream_client_errors_total",
        "Total LLM upstream calls rejected as malformed requests",
    ),
    (
        "llm_upstream_failures_total",
        "Total LLM upstream failures, excluding rate limits",
    ),
    (
        crate::metric_names::NOTIFICATION_SIGN_TOTAL.as_str(),
        "Requests to the notification signing face, by outlet and outcome.          `signed` is a signature handed out, `unknown_outlet` a name that is not          one of the outlets the signing rules cover, and `not_configured` an          outlet this deployment holds no key for. The last two are refusals and          they are different people's to fix: a name nothing covers is ours, a          missing key is the operator's. Read it beside the delivery readings --          a signature that is never asked for and one that is always refused both          end with no message sent, and only this series says which happened",
    ),
    (
        crate::metric_names::AUDITED_LLM_REQUESTS_TOTAL.as_str(),
        "Requests offered to the audited document channel, by outcome. Every \
         cell is published at zero on purpose: without the zero, a channel the \
         switch closed and a channel nobody calls render the same. Read it \
         before looking for a missing document — `blocked_by_switch` says the \
         channel is shut by configuration, while `refused_by_audit` climbing is \
         a security event and not a caller mistake",
    ),
    (
        "cogneva_version_contract_checks_total",
        "Version contract judgements by clause and outcome. Read next to the \
         violations: without it, a contract that holds and a judgement that has \
         never run render the same",
    ),
    (
        crate::metric_names::MIRROR_PUSH_FAILURES_TOTAL.as_str(),
        "Pushes refused by a mirror of the same repository, by mirror. A \
         landing reports success only when every host took the commit, so a \
         mirror named here is the whole reason the round that refused it did \
         not land: the change is on the base branch and not on the mirror, the \
         mirror is behind by every commit since its first refusal, and the next \
         attempt fast-forwards them to it — unless its tip is not an ancestor \
         of the base branch, which is two hosts disagreeing about one branch \
         and stays here until something outside the loop resolves it",
    ),
];

/// Descriptions for the histogram series. See [`COUNTER_HELP`].
const HISTOGRAM_HELP: &[(&str, &str)] = &[
    (
        "memory_operation_latency_ms",
        "Memory backend operation latency in milliseconds",
    ),
    (
        crate::metric_names::HTTP_REQUEST_DURATION_MS.as_str(),
        "HTTP request duration in milliseconds",
    ),
];

/// Descriptions for the gauge series. See [`COUNTER_HELP`].
const GAUGE_HELP: &[(&str, &str)] = &[
    (
        "memory_unextracted_raw",
        "Archived raw sources still missing a summary, as last scanned",
    ),
    (
        "memory_unextracted_raw_aged_out",
        "Subset of the above that aged past the re-drive window; the system \
         will not pick these up again without a budgeted backfill",
    ),
    (
        "metrics_samples_rows",
        "Rows currently held in the metrics sample log",
    ),
    (
        "metrics_samples_budget_rows",
        "Rows the metrics sample log is allowed to hold; the sweep deletes \
         oldest-first past this, stopping at each gauge series' newest row",
    ),
    (
        "metrics_samples_over_capacity",
        "1 when the sample log is over its row budget and cannot be pruned \
         further without deleting a gauge series' current value, 0 otherwise",
    ),
    (
        "metrics_samples_bytes",
        "On-disk bytes the metrics sample log occupies, including indexes. \
         Lags the row count, since PostgreSQL frees deleted space only when it \
         vacuums, so it is a reading and never the pruning criterion",
    ),
    (
        "metrics_retired_rows_removed",
        "Rows of retired metric names the last release pass deleted, labelled \
         by the table they came from. Reported every pass, so a table whose \
         reading stays non-zero is one the release is not draining",
    ),
    (
        "llm_upstream_healthy",
        "Whether each LLM upstream has no outstanding failure, 1 or 0. \
         Absent for an upstream this process has never sent a call to: \
         'no record' is the shape a recovered upstream has too, so it is \
         reported as no reading rather than as health. A backoff window \
         expiring is not evidence of recovery either, so only a call that \
         actually succeeded clears it — the same rule the pool verdict uses",
    ),
    (
        "llm_pool_available",
        "Whether any LLM upstream is usable, 1 or 0",
    ),
    (
        "llm_pool_evidenced_recovery_unix",
        "Recovery instant an upstream itself reported, as a Unix timestamp; \
         0 when no upstream has given one",
    ),
    (
        "llm_pool_next_attempt_unix",
        "When the next pool probe is due, as a Unix timestamp",
    ),
    (
        "cogneva_version_commits_since_release",
        "First-parent commits between the tracked main and the newest release \
         tag reachable from it. The published artifact is built from the tag, \
         so this is how far the running code has moved past what was released. \
         Absent when no release tag is reachable, which is not zero",
    ),
    (
        "cogneva_version_declared_info",
        "The version the tracked main declares, as a label on a constant 1: a \
         label rather than a value because the declaration is an identity, not \
         a measurement. Bounded by the number of releases, unlike the build \
         label, which changes with every commit",
    ),
    (
        "cogneva_version_contract_violations",
        "Current violations of the version contract by clause. A standing count \
         rather than a total, so a violation nobody fixed does not look like a \
         rising rate of new ones. Reported for every clause, including the ones \
         at zero, so an absent series means the clause was never judged",
    ),
    (
        "cogneva_rollout_job_cpu_throttled_ratio",
        "Share of the CFS periods of the newest rollout judgement run in which \
         the run was throttled by its own CPU limit, read by the run from its \
         own cgroup. The judgement lives in a short-lived Job pod, so no \
         sampling rule over the pod ever sees a whole run: this is the reading \
         of the run that just finished. Absent when that run reported no \
         reading — a run that was killed before it could look at itself, or a \
         node with no cgroup this process can read — which is not a ratio of 0",
    ),
    (
        "cogneva_rollout_job_memory_peak_ratio",
        "The newest rollout judgement run's peak memory against the limit its \
         manifest declared, read by the run from its own cgroup. The cgroup \
         peak counts page cache, which the kernel reclaims instead of dying \
         for, so a healthy run reads near the top of a narrow band of its own \
         however much headroom it really had: this is a ceiling the run \
         touched, not the headroom it kept, and a run actually killed by this \
         limit shows up as its own failure locus rather than here. Read as a \
         peak rather than a sample because the run is short: the instant it \
         came closest is the one a scrape would have to be lucky to catch. \
         Absent when the run left no reading, or when the limit was declared \
         as unlimited, which is not a ratio of 0",
    ),
    (
        "cogneva_rollout_job_reading_unix",
        "When the newest rollout judgement run took its resource reading, from \
         the run's own clock, as a Unix timestamp. The other two series are \
         written only by a run that got far enough to report, so a run that \
         died on the way leaves the previous run's values standing: this is how \
         a reader ties them to the run it actually watched, and a timestamp \
         that does not advance across a completed run is that silence",
    ),
];

/// The description registered for one series, if it has one.
///
/// `None` means somebody recorded a name and never documented it. Callers
/// serving it should say so rather than drop the series.
pub fn metric_description(kind: MetricType, name: &str) -> Option<&'static str> {
    let table = match kind {
        MetricType::Counter => COUNTER_HELP,
        MetricType::Histogram => HISTOGRAM_HELP,
        MetricType::Gauge => GAUGE_HELP,
    };
    table
        .iter()
        .find(|(known, _)| *known == name)
        .map(|(_, text)| *text)
}

/// Every name described for one kind, in table order.
///
/// For readers that have to fall back to *something* when the storage backend
/// cannot enumerate its names: the described set is what such an exposition
/// served before enumeration existed, so the fallback degrades to the older
/// behaviour rather than to an empty body.
pub fn documented_metric_names(kind: MetricType) -> impl Iterator<Item = &'static str> {
    let table = match kind {
        MetricType::Counter => COUNTER_HELP,
        MetricType::Histogram => HISTOGRAM_HELP,
        MetricType::Gauge => GAUGE_HELP,
    };
    table.iter().map(|(name, _)| *name)
}

/// The description for one series, or a placeholder naming the omission.
///
/// Serving a series with a placeholder is better than the two alternatives:
/// dropping it loses the measurement, and refusing to serve it turns a missing
/// sentence into a missing measurement. The placeholder names the fix so the
/// omission is actionable rather than merely visible.
pub fn metric_help_text(kind: MetricType, name: &str) -> String {
    match metric_description(kind, name) {
        Some(text) => text.to_string(),
        None => format!("Undocumented metric {name}: no description is registered for it"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A name is described once, for one kind. Two entries for the same name
    /// would be two answers to one question, and the lookup returning the first
    /// would hide the second from every reader of the exposition.
    #[test]
    fn no_name_is_described_twice() {
        let mut seen: Vec<&str> = Vec::new();
        for (kind, table) in [
            (MetricType::Counter, COUNTER_HELP),
            (MetricType::Histogram, HISTOGRAM_HELP),
            (MetricType::Gauge, GAUGE_HELP),
        ] {
            for (name, text) in table {
                assert!(!text.trim().is_empty(), "{kind:?} {name} 描述为空");
                assert!(
                    !seen.contains(name),
                    "{name} 被描述了两次（第二次在 {kind:?}）"
                );
                seen.push(name);
            }
        }
    }

    /// The lookup goes through the kind, so a name described as a gauge does
    /// not answer for a counter of the same name.
    #[test]
    fn a_description_belongs_to_its_kind() {
        assert!(metric_description(MetricType::Gauge, "llm_upstream_healthy").is_some());
        assert!(metric_description(MetricType::Counter, "llm_upstream_healthy").is_none());
        assert!(
            metric_help_text(MetricType::Counter, "llm_upstream_healthy")
                .starts_with("Undocumented metric llm_upstream_healthy")
        );
    }
}
