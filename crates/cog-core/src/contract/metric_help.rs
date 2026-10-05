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
         including one that reported zero, and it counts a call whose caller \
         stopped reading after the usage frame arrived just the same, because \
         the frame is the whole reading. `interrupted` is a response that \
         never finished, kept apart so an upstream that dies mid-stream cannot \
         be read as one that answers quietly. `abandoned` is the caller's own \
         version of that: it stopped reading the body before the end and no \
         usage frame had arrived by then, so the silence belongs to neither \
         side -- it is not `absent`, because the upstream may have been about \
         to speak, and it is not `not_asked`, because the request did ask. \
         Read it beside `llm_calls_total{result=\"ok\"}`: an accepted call \
         always lands in exactly one of these cells, so a growing gap between \
         the two series is a reading that was never written at all",
    ),
    (
        "llm_upstream_client_errors_total",
        "Total LLM upstream calls rejected as malformed requests",
    ),
    (
        "llm_upstream_failures_total",
        "Total LLM upstream failures this process recorded against an upstream, \
         counting failures of serving requests and of the health probes that \
         retest a suspect window alike -- the backoff window beside it is driven \
         by the same events, so the two readings describe one history. Rejections \
         as malformed requests are counted separately. Absent until the first \
         failure: a missing series is neither a zero nor a broken exporter, and \
         only this counter's own reader can tell which",
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
    (
        "cogneva_change_fate_total",
        "Changes that have ended, by fate and by the entry point that produced \
         them. Cumulative, and separate from the funnel census for that reason: \
         a landed change leaves the record directory once its CI verdict comes \
         back green, so the census's landed cell drops back to zero and a rate \
         read off it errs toward looking healthy. fate is landed or retired — \
         the two ways a change finishes here; every other stage is a place to \
         wait",
    ),
    (
        "cogneva_landing_failures_total",
        "Landing calls that failed, one per attempt, by category. A landing \
         failure is otherwise a single log line inside a loop that then moves \
         on, so 'nothing needs landing' and 'landing is being refused' look the \
         same from outside. The category is the part that aggregates and the \
         part that says what happens next: only a path refusal is terminal -- \
         the paths were read and are not acceptable, and no later attempt \
         changes that -- so it appears once, at the moment the change leaves \
         the channel, while oversized, unreadable_diff, conflict, raced, \
         rejected and environment are retried and appear once per attempt. A \
         rising unreadable_diff is a change the loop cannot read its way out \
         of: the gate will not push a diff it cannot read, and unlike a path \
         refusal nothing retires it, so it comes back every round until someone \
         looks at why the diff is malformed. An oversized change reads the same \
         way for a different reason: the size cap is the one policy limit owner \
         approval lifts, so nothing in the loop ends it either and it too comes \
         back every round until the owner decides",
    ),
    (
        "cogneva_redrive_refusals_total",
        "Re-drives that were not submitted, by reason: no_evidence (the failure \
         arrived without a log, and a fix task built on its absence is a guess \
         that costs a full round) or cause_exhausted (this cause has already \
         spent its rounds for the window). Without it, 'nothing is being \
         generated' and 'generation was switched off for this cause' read the \
         same from outside",
    ),
    (
        "cogneva_redrive_budget_losses_total",
        "Round charges the re-drive ledger lost, by side: read (the ledger \
         could not be read, so its rounds were forgotten) or write (the round \
         was not charged, so the same cause can buy another one after a \
         restart). Both directions end in a budget that quietly stopped \
         applying; they are counted apart because the two repairs differ",
    ),
    (
        "cogneva_registry_maintenance_runs_total",
        "Registry maintenance rounds that ran, one per round, whether or not \
         anything was deleted. Read next to the pruned-tag count: a round that \
         ran and pruned nothing says the trigger fired on a volume that is not \
         full",
    ),
    (
        "cogneva_registry_pruned_tags_total",
        "Tags a registry maintenance round deleted, summed over rounds. Written \
         only when the round deleted something, so its absence means no round \
         has ever deleted a tag",
    ),
    (
        "cogneva_registry_prune_failures_total",
        "Tag deletions the registry refused, summed over rounds. Written only \
         when a round was refused something: a round that deleted most of what \
         it intended and was refused the rest reads as the two series together \
         rather than as a clean sweep",
    ),
    (
        "cogneva_registry_rebuild_hold_secs_total",
        "Seconds the tag server was held away from serving, summed over the \
         restarts the deployer itself induced. Written when the server answers \
         again, so it counts completed holds and never the one in progress. \
         Read with the pruned-tag count: how long a rebuild takes follows how \
         much waste had accumulated, so seconds per tag deleted is the ratio \
         that says whether the trigger lets the volume grow too full",
    ),
    (
        "cogneva_mainline_superseded_rollout_total",
        "Rounds the deployer's guard answered that the revision it was carrying \
         had been overtaken upstream: 1 when it had (nothing is rolled out, the \
         revision is left alone and the new tip is taken next round), 0 when it \
         had not. It counts answers, not questions — the producer writes a zero \
         on an unskipped round, and a zero sample does not move a counter's \
         value, so 'this round asked and was told no' is not readable here. The \
         questions are counted by cogneva_mainline_supersession_checks_total, \
         and the value held here is the share of rounds the guard saved"
    ),
    (
        "cogneva_mainline_rollout_attempts_total",
        "Rounds the deployer got as far as deciding whether to roll a revision \
         out. Kept as the partner of cogneva_mainline_supersession_checks_total, \
         which moves in the same call on the path that asks the guard: while the \
         guard is wired the two climb together, and rounds that decided to roll \
         without asking appear as the gap between them. It is also the only \
         published cadence of a promotion attempt — the declared poll interval \
         bounds the loop that looks for work, not the rounds that find any",
    ),
    (
        "cogneva_mainline_supersession_checks_total",
        "Times the deployer's guard put the overtaken question, counted whether \
         the answer was yes or no. Counted rather than inferred from the skips, \
         because the answer counter only moves on a yes and a quiet stretch would \
         then read as a guard that never ran. Read with \
         cogneva_mainline_rollout_attempts_total: equal while the guard is wired, \
         and the difference between them is the failure this pair was built to \
         show",
    ),
    (
        "cogneva_mainline_ci_no_verdict_reason_total",
        "Why the deployer's CI question came back without a verdict, one count \
         per silent upstream, by reason: pending (the upstream had not finished \
         the checks), no_runs (asked and there was nothing to judge), \
         status_unreadable, bad_api_base, connect_failed, unusable_body, and \
         http_auth_rejected / http_not_found / http_rate_limited / \
         http_upstream_error / http_other. The no_evidence cell of \
         cogneva_mainline_ci_verdict_total holds all of these at once, and they \
         do not have the same owner: one means wait, another means the gateway \
         or its credential, another means the path or the configuration",
    ),
    (
        "cogneva_mainline_ci_verdict_total",
        "CI verdicts the deployer read for the revision it was about to promote, \
         by verdict: pass, fail or no_evidence. no_evidence is its own value \
         rather than a kind of failure: a gate whose conclusion could not be \
         read is not a gate that said no, and counting the two together hides \
         exactly the case that lets an unverified revision through. Counted per \
         question rather than per revision: a round asks before the build and \
         again before the dispatch, so a revision that reads the same way twice \
         adds two",
    ),
    (
        "cogneva_dag_stalled_scheduled_reclaimed_total",
        "Tasks found stalled in Scheduled and put back in line without charging \
         an attempt, summed over repairs. A counter rather than a gauge, because \
         the repair empties the state it repairs: a gauge would read zero both \
         when nothing was ever stuck and when everything had just been unstuck. \
         It reads the symptom, not the cause: a task reaching the end of the \
         window without starting looks the same whether its message was queued \
         behind a full claim pool or lost, and the repair re-arms either way. \
         Absent until the first such repair",
    ),
    (
        "cogneva_task_checkpoint_total",
        "Task checkpoint outcomes, by outcome: saved, unpersisted, failed, \
         superseded, unsuperseded, no_agents. All six are published from the \
         first scrape, zeros included, so an outcome that never happens does not \
         read like one that was never wired up. no_agents counts tasks that were \
         running with none of their agents present in the chain",
    ),
    (
        "llm_request_param_clamped_total",
        "Request fields the gateway rewrote before sending, by field and \
         upstream: temperature forced to 1 for upstreams that require it, plus \
         each field the protocol adapter had to add or replace. A rewrite \
         changes what was asked for, so 'this upstream answers differently' has \
         this among its causes",
    ),
    (
        "cogneva_buildah_store_rounds_total",
        "Attempts to reclaim the local build store, by outcome: pruned, or \
         nothing to remove, or the host held the build slot, or the retention \
         set, the store listing, its parse failed. Read with \
         cogneva_buildah_store_reading_unix: rounds rising against a timestamp \
         that does not move is a reclaim that is attempted and never completes",
    ),
    (
        "cogneva_buildah_store_pruned_images_total",
        "Base images removed from the local build store, summed over rounds. \
         Written only when a round removed something, so its absence means no \
         round has ever freed one",
    ),
    (
        "cogneva_buildah_store_pruned_layers_total",
        "Layer directories the local build store lost, summed over rounds. \
         buildah shares layers between images, so this is not a fixed multiple \
         of the image count: a round removing many images for few layers frees \
         less than the image count suggests",
    ),
    (
        "cogneva_buildah_store_freed_bytes_total",
        "Apparent bytes the local build store's layers lost, summed over the \
         rounds whose store could be measured before and after. Written only \
         when both ends were measurable, since one end alone yields a delta with \
         no provenance; on a host whose store shares hardlinked layers this \
         counts each inode once",
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
    (
        "llm_call_latency_ms",
        "Latency of one LLM call attempt in milliseconds, by upstream, model, \
         result and actor. The same measurement is written to the ClickHouse \
         detail, which is why the per-model latency panel reads this instead: a \
         detail row is not something PromQL can query. upstream and model are \
         both carried because a pool fails over between endpoints serving the \
         same model, so 'which model is slow' and 'which endpoint is slow' are \
         different questions",
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
        "llm_pool_signal_connected",
        "Whether the last attempt to publish the pool verdict to Redis \
         succeeded, 1 or 0. The gateway is the only process holding upstream \
         credentials, so the scheduler reads its verdict from that key; the \
         channel is re-established on demand, so a 0 says the attempt failed \
         and the next one will try again rather than that the channel is gone. \
         Absent when no Redis is configured, which is a single-process \
         deployment and not a fault. A 0 beside llm_pool_available 1 is the \
         reading to look at: the pool verdict is then known to the gateway and \
         to nobody else, so the scheduler is not pausing LLM-dependent work, \
         and no other series says so",
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
    (
        "cogneva_change_funnel",
        "How many changes the platform is holding at each stage, by entry point: \
         staged (produced and withheld until the owner approves publication), \
         unverified (in the landing channel, not yet sandbox-verified), landed, \
         retired. Computed from the durable records on every scrape rather than \
         incremented as stages pass, so every stage has a value from the first \
         scrape — a counter would be absent until its first event, and 'never \
         happened' would look like 'never wired up'. landed falls back to zero \
         when a landed change's record is removed after its green verdict, \
         which is what the fate counter is for",
    ),
    (
        "cogneva_worktree_index_present",
        "Whether a resident worktree's git index file exists, 1 or 0, by \
         worktree. Read with the missing-files series: 'the index remembers no \
         files' and 'the index is not there' both make the next reset look free, \
         and this is what tells them apart",
    ),
    (
        "cogneva_worktree_index_missing_files",
        "Tracked files a resident worktree's git index does not remember, by \
         worktree; 0 is healthy. Every one of them is rewritten by that \
         worktree's next reset, so a value at the whole-tree size is a tree \
         about to be rebuilt rather than refreshed",
    ),
    (
        "cogneva_registry_maintenance_reading_unix",
        "When a registry maintenance round last finished, as a Unix timestamp. \
         Written only on completion, so a value that stops advancing is the age \
         of the last round that got through; absent means none ever has",
    ),
    (
        "cogneva_registry_gc_owed",
        "1 while a maintenance round has deleted tags and not yet got the tag \
         server to reclaim them, 0 otherwise. Published every round while a \
         registry claim is configured, because the moment it most needs to be \
         visible — the restart keeps failing — has no other reading. Absent when \
         no registry claim is configured, which is 'nobody asked'",
    ),
    (
        "llm_usage_verdict_measured",
        "1 when this upstream's usage-reporting capability has been settled from \
         evidence, 0 while it is still assumed, by upstream. Read next to the \
         token counters: an upstream carrying traffic while we still guess at \
         stripping stream_options cannot report non-zero tokens, and 'the \
         upstream does not report usage' and 'we never asked it to' look alike \
         in every other series",
    ),
    (
        "llm_upstream_quota_window_secs",
        "The quota window this upstream itself declared, in seconds; 0 when it \
         declared none, by upstream. Separate from the pool's next-attempt \
         instant: this is how long the upstream says it will be unusable, that \
         is when we will probe it again",
    ),
    (
        "llm_upstream_quota_reset_unix",
        "The instant this upstream itself said its quota returns, as a Unix \
         timestamp; 0 when it said nothing, by upstream. Kept per upstream \
         rather than only as a pool value, so a reader can tell which one is \
         holding the pool back",
    ),
    (
        "llm_upstream_consecutive_failures",
        "Consecutive failed probes against this upstream, by upstream; the input \
         the backoff window length is computed from. On the surface because a \
         reader cannot judge a backoff interval without seeing what produced it. \
         Absent for an upstream this process has never probed",
    ),
    (
        "llm_pool_quota_window_secs",
        "The longest quota window any upstream declared, in seconds; 0 when none \
         did. Answers 'how long', which is a different question from the pool's \
         two recovery instants and is not capped by our own probe cadence",
    ),
    (
        "cogneva_buildah_store_kept_images",
        "Images the retention set protected in the local build store, as of the \
         last completed reclaim round. The reading that says the keep set is \
         doing something: a round whose only number is 'removed 0' cannot be told \
         from one that protected nothing and had nothing to remove",
    ),
    (
        "cogneva_buildah_store_live_images",
        "Revisions the running workloads referenced, as of the last completed \
         build-store reclaim round. Published next to the protected count because \
         the two answer different questions -- how many the rollback window \
         covers against how many something is running now -- and a live reading \
         of zero is what a broken cluster read looks like, which is the one way \
         this pass could remove a base image that is in use",
    ),
    (
        "cogneva_buildah_store_reading_unix",
        "When a local build-store reclaim last ran to completion, as a Unix \
         timestamp; 0 until one does. Seeded with the epoch rather than the \
         process start, so a process that has never completed a round reads as \
         never having completed one instead of as having just done it",
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
