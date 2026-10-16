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
        crate::metric_names::MEMORY_DECAY_TOTAL.as_str(),
        "How each pass of the memory decay maintenance loop ended, labelled by \
         `outcome`, one increment per namespace swept per pass. Outcomes are a \
         closed set: `idle` (the pass ran and found nothing to decay), \
         `decayed` (it lowered the importance of at least one aged low-value \
         entry), `archived` (it removed at least one such entry from the \
         searchable summary layer), and `failed` (the pass could not complete). \
         A pass that both decayed and archived reports `archived`, because the \
         removal is the half worth seeing and the partial-demotion half is \
         reported by every other pass that decays without archiving. The cell \
         counts passes, not entries: an entry count mixes two units under one \
         name and cannot say whether a quiet stretch was a loop that found \
         nothing or a loop that stopped being called. This loop lives in every \
         memory-owning deployment, so a reader takes a single series rather \
         than summing across deployments.",
    ),
    (
        crate::metric_names::MEMORY_DROPPED_ENTRIES_TOTAL.as_str(),
        "Entries that left the searchable layer, labelled by `namespace`. \
         Entries, not passes: this is the quantity the pass counter beside it \
         deliberately does not carry, and the two are read together, since a \
         pass that dropped ten entries and one that dropped one are the same \
         increment there. What drops an entry is repeated decay -- its \
         importance falls below the floor and it stops being searchable -- and \
         that is the loop working rather than failing, so the reading has no \
         threshold: it says how much memory is being given up, and whether that \
         amount is right is a matter of the decay policy rather than of this \
         series. The other way an entry can go is an explicit delete, which has \
         no caller in this build and no cell of its own here: a delete call \
         reports success whatever it found, so a count taken at that call would \
         count requests rather than entries, and a series that says entries left \
         must not be built from a number that may be zero. The namespace set is \
         bounded by the loop's own configuration, which lists the namespaces it \
         sweeps, rather than by traffic. Like the pass counter, every \
         memory-owning deployment writes this series, so a reader takes a single \
         series rather than summing across deployments. It is written only when \
         something was dropped, so absence is a stretch with nothing to drop; a \
         frozen series is a namespace that has stopped dropping, which the store \
         reports as the held value rather than as absence.",
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
        "Total number of LLM upstream calls by result. The `actor` axis takes \
         only the values the security gateway declares: a caller that sends no \
         actor, or a word the platform does not carry, is counted under \
         `unknown` rather than opening a series of its own, so a rising \
         `unknown` cell is a call site that lost its tag and never a new actor \
         appearing",
    ),
    (
        "llm_tokens_total",
        "Total LLM tokens consumed, split by `kind` into input, output and \
         cached. `cached` is the part of the input the upstream served from its \
         own cache; on an OpenAI-compatible upstream it is a subset of `input`, \
         on Anthropic it is disjoint from it, so read the ratio according to the \
         upstream's protocol. Published at zero too: a missing cell says the \
         metering path never ran for that upstream and actor, a cell holding \
         zero says it ran and found nothing. The `actor` axis is the same \
         closed set as on `llm_calls_total`: spend under `unknown` is spend by \
         a caller that did not declare itself, so it is real money that no \
         per-actor view accounts for",
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
         split by which side observed them. `source=\"caller\"` is a call this \
         gateway was serving that came back with nothing usable, and \
         `source=\"probe\"` is a health probe retesting an upstream already in a \
         suspicion window. Both are real failures and both feed the backoff \
         window beside this counter, but they answer different questions, and \
         telling them apart is not a refinement: while the pool is latched down \
         requests never leave this gateway, so the caller cells sit still while \
         the probe cells climb every probe interval. A reader asking \"are the \
         callers failing\" has to select `source=\"caller\"` -- summing the \
         family over both sources reads the gateway's own heartbeat as traffic, \
         and it moves on every probe whether or not a single call was made. \
         Rejections as malformed requests are counted separately. Both cells are \
         published at zero for every configured upstream before the first call, \
         so the first failure of a process generation is a difference a reader \
         can see",
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
        "cogneva_project_kind_total",
        "Changes the pipeline dispatched, labelled by the landing recipe it \
         used (`kind`) and by where that recipe came from (`source`, either \
         `declared` or `builtin`). The declaration lives in the project's own \
         configuration document, so a project that says nothing lands by the \
         built-in without this reading being able to say so — which is what the \
         `source` label is for: a project that never declared a kind and a \
         project that declared the built-in dispatch identically and are two \
         different facts. A project whose declaration cannot be read is refused \
         instead, and appears under `cogneva_change_rejected_total` with cause \
         `project_kind_unusable`. The cells are seeded at zero by the first \
         change the pipeline dispatches: from then on, a recipe that never ran \
         reads as a zero while an absent series means no change has been \
         dispatched yet",
    ),
    (
        "cogneva_change_tier_total",
        "Which gates a change was routed to, labelled by `tier`, written once \
         per change the pipeline reaches. The routing reads what the change \
         touches -- its paths, the configuration keys it moves, the names it \
         takes out and does not put back -- and the cells are the closed set it \
         can land in: `minimal` for prose only, `tests` for source that is not \
         a criterion, `structural` for a configuration value no \
         criterion reads and for the deployment's own topology, and `real_gate` \
         for a change that can move a criterion. The four cells are seeded at \
         zero by the first change the pipeline tiers: from that change on, a \
         tier that never fired reads as a zero while an absent series means the \
         reading was never wired. Before that first change nothing is published \
         at all, so an absent series there says only that nothing has been \
         tiered yet -- no change has reached the pipeline, or execution has \
         moved into a per-change job, which publishes no reading of its own. \
         Read \
         `real_gate` against the total: a fleet that tiles every change but \
         never once reaches the real gate is a routing that has gone inert, and \
         that is the reading no other series states",
    ),
    (
        "cogneva_eval_gate_outcomes_total",
        "How each run of an eval A/B gate ended, labelled by `outcome` and \
         `site`, written once per run. The gate refuses a change that \
         regresses a fixed eval suite; its verdict comes from a sandbox agent \
         that runs the suite twice and reports the outcomes, and the gate \
         z-tests what came back. The same gate runs at two sites -- `port` \
         (before a baseline port lands) and `promotion` (before a change is \
         promoted) -- and the two block by different rules, so the site is its \
         own label: a gate gone inert at one site says nothing about the other. \
         Whether it measured at all is a separate fact from what it decided, \
         and only the second had a reading: the agent reporting \
         `applicable=false` and there being no orchestrator both returned \
         success and left no series, so a change that was never measured and \
         one that was measured and passed read the same. The cells are the \
         closed set of ends -- `passed`, `rejected` and `inconclusive` are the \
         gate deciding (a z-test that found no significant difference is \
         neither a pass nor a regression and keeps its own cell, so a run that \
         was blocked on the doubt is not stamped `passed`), `not_applicable` \
         and `unreadable` are a run that did not measure, and `unavailable` is \
         a run that never reached a verdict. A reader separates a gate that \
         keeps passing from one that keeps skipping; the skip's own \
         tick-failure reading cannot, because a gate that skips is not a tick \
         that failed",
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
        "cogneva_landing_ci_failure_inherited_total",
        "Landed changes whose red CI was traced to the commit they were replayed \
         onto rather than to the change itself, so the change was kept instead \
         of reverted. A commit's CI measures the whole tree it stands on, so a \
         check that was already failing on the base tip fails again on the \
         change built over it; reverting that change would restore the very \
         tree that failed. The change stays in the watch window, and if the same \
         check goes red once the tree is repaired it is no longer a subset of \
         what failed underneath and is charged to the change as before. This is \
         not a fault reading on its own -- each count is a revert that would not \
         have worked -- but it running while the base branch stays red is how \
         the branch's own redness is paid for in generation rounds, and only a \
         bound on how long landings may keep landing onto a red branch would \
         turn it into one",
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
        "self_evolution_change_yield_total",
        "Where self-evolution runs ended, one increment per finished run, by \
         outcome: submitted (a change was handed to a sink), no_sink (the run \
         produced one and the deployment has nothing configured to take it), \
         no_artifacts / non_change_artifacts (the answer carried no diff), \
         payload_missing / payload_unreadable (there was no result to read), \
         submit_failed (every sink refused the diff). The cells partition the \
         runs, so the causes that did not happen are absent rather than zero: \
         a reader who had to union them would be reading one question as \
         seven unrelated facts. Held in the metrics store, so it survives a \
         rollout -- a per-process count restarts at zero every hour and \
         re-asserts 'nothing has ever yielded'",
    ),
    (
        "collab_change_diff_source_total",
        "Where a generation round's change diff came from, by source: tree (a \
         diff read out of the checkout the run edited, so git derived its \
         headers from the files themselves and the apply gate can compare the \
         change against the tree) or model (the text the model wrote out, kept \
         when the harvest returned nothing usable). The tree cell standing \
         still while the total moves is what 'the harvest stopped firing' \
         looks like. Held in the metrics store: the ratio of the two is read \
         across rollouts, which a count that resets on every rollout cannot \
         show",
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
        "cogneva_partition_maintenance_failures_total",
        "Ways a monthly partition maintenance round came up short, summed over \
         rounds and labelled by the cause. The loop's liveness is read already \
         -- its beat stops when the task is gone -- but its outcome was not: a \
         step that failed and a table whose DEFAULT partition was found holding \
         rows were both written to a log line that dies with the pod, so a round \
         that failed every hour and an instance with nothing to open read the \
         same. The reason values are a closed set the producing module declares: \
         default_partition_missing and month_partition_missing when CREATE was \
         refused, default_backlog_unreadable when the parked-row count could \
         not be read, and window_behind when the count came back non-zero -- \
         rows in DEFAULT mean the window fell behind and they are outside every \
         open range. Written only when its cause occurs, because the closed set \
         is declared in the code a reader can enumerate; a reader takes an \
         absent cell as zero, and the rising-against-flat shape is what tells a \
         failing loop from one with nothing to do",
    ),
    (
        "cogneva_policy_evolution_rounds_total",
        "How each round of the artifact-level evolution search ended, summed \
         over rounds and labelled by the outcome. The loop's beat is stamped \
         every cycle whether or not the round produced anything, so it cannot \
         say which end was reached, and the round's end was only ever a log \
         line that dies with the pod: adopting a new policy version, finding no \
         better candidate, having too little evidence to compare, and failing \
         outright read the same. The values are a closed set the producing \
         module declares: insufficient_evidence and no_improvement (two ends \
         that change nothing external, and so leave no other trace), adopted \
         (a saved-and-hot-swapped version), and failed (the round returned an \
         error). Read as the rate of adopted against the rest: a flat adopted \
         cell with a rising insufficient_evidence cell says the search is \
         awake but has nothing to learn from, which is a different fact from a \
         failed cell rising, which is the loop not doing its job",
    ),
    (
        "cogneva_analytics_flush_total",
        "Events carried by each flush of the analytics event buffer, labelled \
         by outcome. The flush loop's only other reading is its own beat, which \
         is stamped every flush interval whether or not a batch was waiting, so \
         delivering a batch to ClickHouse and failing to were the same picture \
         -- and a failure loses the events: the insert consumes the batch, so \
         nothing retries it and the loss survived only as a log line that dies \
         with the pod. The values are a closed set the producing module \
         declares: delivered (the batch reached ClickHouse) and failed (the \
         insert returned an error and the batch was dropped). Read as the rate \
         of failed against delivered: a failed cell rising over a flat \
         delivered cell is the buffer losing events, which is a different fact \
         from an idle buffer that has had nothing to send",
    ),
    (
        "cogneva_memory_schema_repair_total",
        "How each round of the memory schema repair loop ended, summed over \
         rounds and labelled by the outcome. The loop exists to rebuild the \
         derived layer of archived entries that were written without one -- \
         without it such an entry is unreachable by search and nothing reports \
         it, since the test for whether it was written is whether the derived \
         layer is there. The loop's beat is stamped every repair interval \
         whether or not it repaired anything, so a round that failed and a \
         round that found no orphan were the same picture, and the failure \
         survived only as a log line that dies with the pod. The values are a \
         closed set the producing module declares: repaired (at least one \
         orphan was rebuilt), unrepairable (orphans were found but none could \
         be typed, so the loss is permanent and re-surfaces every round), clean \
         (nothing was missing), and failed (the pass returned an error). Read \
         the failed cell against the rest: a failed cell rising over a flat \
         repaired cell is the repair path failing every round, which is a \
         different fact from a store with nothing to repair",
    ),
    (
        "cogneva_skill_hot_reload_total",
        "How each round of the skill hot-reload loop ended, summed over rounds \
         and labelled by the outcome. The loop exists to keep the in-memory \
         skill registry in step with the files on disk; its only other reading \
         is its own beat, stamped every poll interval whether or not anything \
         changed, so a round that could not scan the directories at all and a \
         round that scanned them and found nothing to do were the same picture, \
         and a skill file that failed to load left the previous definition in \
         the cache and survived only as a log line that dies with the pod. The \
         values are a closed set the producing module declares: applied (at \
         least one skill was loaded, replaced, or evicted, so the cache moved \
         towards the disk), unchanged (the scan and the cache agreed), \
         reload_failed (a skill was found on disk but could not be loaded, so \
         the stale definition kept serving), and failed (the scan itself \
         returned an error and nothing was inspected). A round that both \
         applied a change and failed to load another skill is recorded as \
         applied: the failed skill is re-scanned every round and re-surfaces, \
         while the applied one is only true of this round. Read the failed and \
         reload_failed cells against the rest: a failed cell rising is the loop \
         not looking at the disk at all, which is a different fact from a \
         broken skill file that re-surfaces every round. The loop runs in every \
         deployment that enables the skill plugin and all of them write the one \
         store, so a reader takes a single series rather than summing across \
         deployments: the sum would report the same total once per writer",
    ),
    (
        "cogneva_loki_flush_total",
        "How each log-flush of the Loki background pusher ended, summed over \
         flushes and labelled by the outcome; the cell's value is how many log \
         entries that batch carried. The loop exists to deliver buffered logs to \
         Loki; its only other reading is its own beat, stamped every flush \
         interval whether or not there was anything to push, so a push that \
         failed every round and a buffer that was simply empty were the same \
         picture. A failed push consumed the batch, so the entries were lost, \
         not retried, and their only trace was a log line that dies with the \
         pod. The values are a closed set the producing module declares: \
         delivered (the batch reached Loki) and failed (it did not, and those \
         entries are gone). Read the failed cell against the delivered one: a \
         failed cell rising is the flush path dropping logs, which is a \
         different fact from a quiet gateway with nothing to ship. Because a \
         failed round is a loss rather than a retry, the cell value is the entry \
         count and not the batch count: the number that matters is how many \
         entries were dropped. Both triggers -- the batch-size one and the \
         interval one -- go through the same push, so one series covers the \
         whole path",
    ),
    (
        "cogneva_supervisor_pass_total",
        "How each pass of the supervisor loop ended, summed over rounds and \
         labelled by `pass` (which arm ran) and `outcome`. The loop drives six \
         independent passes -- health, quota, rebalance, event aggregation, \
         autonomous decisions, and control-plane status reporting -- and each one, \
         when it failed, left only a log line that dies with the pod. Its only \
         other reading is its own beat, and that beat is driven by the shortest of \
         the six intervals, so a pass that could not run at all and a pass with \
         nothing to do were the same picture, and an arm that stopped firing stayed \
         invisible behind a beat the others kept stamping. `pass` is the closed set \
         the producing module declares: health, quota, rebalance, event, \
         autonomous, control_plane. `outcome` is ok or failed: every arm routes its \
         round through one recording call, so the reading covers all six the same \
         way. A failed cell rising is a pass failing every time it runs, which is a \
         different fact from a pass with nothing to do. The autonomous arm reports \
         no error by construction, so it only ever lands in ok -- a failure inside \
         it surfaces as the loop's own death, since a panic in the body is counted \
         by the liveness family. The control-plane arm has no cell while no control \
         plane is configured, since the arm then has no work. The loop runs in every \
         deployment that enables the supervisor and all of them write the one store, \
         so a reader takes a single series rather than summing across deployments: \
         the sum would report the same total once per writer",
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
         written in the statement immediately before the one that asks the \
         guard: while the guard is wired the two climb together, and a round \
         that decided to roll without asking appears as the gap between them. \
         The two are two statements and not one call on purpose — that is what \
         lets a deleted question call site leave the attempt still climbing. \
         The placement matters as much as the pairing: counted any earlier, a \
         round that died on the way to the guard (there is a per-platform git \
         fetch between the decision and the question, and on this deployment \
         the workload carrying the deployer is rolled routinely) would land in \
         the same gap as a guard that is gone, permanently and one round at a \
         time. It is also the only published cadence of a promotion attempt — \
         the declared poll interval bounds the loop that looks for work, not \
         the rounds that find any",
    ),
    (
        "cogneva_mainline_rollout_completed_total",
        "Rollouts the deployer saw land: the deployments on the revision it \
         dispatched, with the judgement that ran against them finished and \
         passed. The partner of cogneva_mainline_rollout_attempts_total, and the \
         reading whose absence hid a stall: the attempts counter moves when a \
         round reaches the decision to promote, whether or not anything is ever \
         deployed, so a deployer refused every cycle climbed it exactly as a \
         healthy one did and no series said otherwise. Read them as a pair -- \
         attempts rising while completed stays flat is a deployer trying and \
         nothing landing, which is what a delivery the cluster refuses looks \
         like from here. The time of the last completed rollout is the \
         __observed_timestamp_seconds companion the store renders beside this",
    ),
    (
        "cogneva_mainline_preflight_refused_total",
        "Rollouts the deployer refused to dispatch because the cluster says its \
         ServiceAccount may not apply one of the kinds the revision delivers, by \
         verb and resource. What it names is the gap between the delivery face and \
         the authorization face -- two declarations that are equal in the tree \
         (a gate proves that) but separately delivered to a cluster, where the \
         installed Role can be behind: an upgrade nobody ran, a grant added to the \
         tree that never reached the release. The deployer's own tree-side gate \
         reads the tree; only the cluster can say what the cluster will accept, \
         so this is asked live, before the dispatch, and a refusal is one the \
         rollout Job never got to make. It exists because the refused dispatch \
         used to live and die inside that Job: the poll counter showed a failed \
         round and the attempts counter climbed, but which verb on which resource \
         was withheld was written nowhere that outlived the pod, and twenty hours \
         of that passed with the cause unreadable. Written before the dispatch, so \
         a refused round never reached the Job -- this is the deployer's judgement \
         of the cluster, not the cluster's rejection replayed after the fact",
    ),
    (
        "cogneva_mainline_supersession_checks_total",
        "Times the deployer's guard put the overtaken question, counted whether \
         the answer was yes or no. Counted rather than inferred from the skips, \
         because the answer counter only moves on a yes and a quiet stretch would \
         then read as a guard that never ran. Read with \
         cogneva_mainline_rollout_attempts_total: equal while the guard is wired, \
         and the difference between them is the failure this pair was built to \
         show. Read the difference within one process rather than between two \
         maxima: both halves are published by every process with the same label \
         set, so a per-process subtraction cancels the scrape arithmetic \
         exactly, while subtracting two separately taken maxima leaves a residue \
         whenever the two maxima come from different pods",
    ),
    (
        "cogneva_mainline_ci_no_verdict_reason_total",
        "Why the deployer's CI question came back without a verdict, one count \
         per silent upstream, labelled by platform (the upstream's \
         CodePlatform, so a mirror that structurally has no check-runs reads \
         apart from the same reason on the platform that does) and by reason: \
         pending (the upstream had not finished \
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
        "cogneva_mainline_poll_cycles_total",
        "Rounds the deployer's poll loop finished, by how they finished: ok when \
         the round ran to the end, failed when it returned an error. Written \
         once per round, and that is what it adds to the loop's own liveness \
         reading: cogneva_loop_tick_age_seconds is stamped before the body runs, \
         so it stays near the period through any run of failures, while this \
         pair says whether the rounds arrived anywhere. A round that fails \
         leaves its cause in the process log and nothing else, and the log is \
         replaced by the next rollout. Read the two cells together: failed \
         having moved while ok did not, over a window longer than a round, is \
         mainline_poll_cycles_all_failed. Both cells are written when they \
         happen and neither is seeded, so an ok cell that is absent means the \
         store holds no finished round yet -- which the pair's own rule treats \
         as the same answer as a zero. The totals are shared: the store holds \
         them and every process serves the same numbers, so a restart resets \
         neither cell and does not make one process's rounds look like \
         another's",
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
         each field the protocol adapter had to add or replace, plus the role \
         of a message the caller wrote as developer where the upstream takes \
         only system. A rewrite changes what was asked for, so 'this upstream \
         answers differently' has this among its causes",
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
    (
        crate::metric_names::KNOWLEDGE_RETRIEVAL_TOTAL.as_str(),
        "What each knowledge layer gave back when it was consulted, labelled by \
         `layer` (memory, wiki) and `outcome`, one increment per layer per \
         retrieval call. Outcomes are a closed set: `hit` (the layer answered \
         with at least one row), `empty` (it answered and had none), `error` \
         (its backend refused), and `absent` (this process does not hold that \
         layer at all). The last two are the cells this series exists for: the \
         retrieval calls return an empty list for all three of empty, error and \
         absent, so without them a layer that was never wired and one holding \
         nothing are the same answer at every caller. Read per layer — `error` \
         moving names a store that refused, `empty` rising against a still \
         `hit` names a store that answers and matches nothing, and `absent` on \
         a deployment that means to have the layer names a wiring fault. The \
         whole cross of layers and outcomes is seeded at zero when the process \
         starts, so a boot that runs no retrieval at all is a grid of readable \
         zeros rather than the same absence a build with no such reading shows. \
         `absent` is written on two schedules and a reader has to say which it \
         means: the backend writes it once per retrieval for a layer it does \
         not hold, while a process that reaches no knowledge backend at all — \
         the wiki layer is what the composite is built from — writes it once \
         per boot, on both layers. On the wiki cell an increase can only be the \
         boot case, since a live backend never writes `absent` for wiki, and \
         that is the cell that tells the two apart. Because one cell counts \
         boots where the rest count calls, no rule or panel may sum across \
         `outcome` and report the total as a number of retrievals. So a reader \
         concludes \"no knowledge backend\" from the wiki cell and never from \
         the memory cell: the memory cell carries ordinary traffic (a live \
         backend writes `absent` there whenever it answers for wiki and holds \
         no memory) and the two meanings are only separable while the wiki cell \
         is read with it.",
    ),
    (
        crate::metric_names::TOOL_OUTPUT_TRUNCATED_TOTAL.as_str(),
        "Tool outputs whose tail was dropped from the conversation because the \
         whole result did not fit the context, labelled by `tool`, one increment \
         per result cut. The model sees the cut in the text it is handed; this is \
         the same fact outside the conversation, so a run that spends its budget \
         on oversized tool results is countable rather than silent. Read per \
         `tool`: a series climbing for one tool names that tool's output shape, \
         not the context budget as a whole. It is the denominator of the landed \
         share: every cut ends either with its tail stored or with an increment \
         on `cogneva_tool_output_archive_failed_total`, so the share of cuts \
         whose tail stayed reachable is this counter minus that one over this \
         one, and the residue is what the reading cannot account for. Read it \
         with `cogneva_tool_output_truncated_chars_total` for the size of what \
         was withheld: this counter says how many results were cut and that one \
         says how much of them the model never saw.",
    ),
    (
        crate::metric_names::TOOL_OUTPUT_TRUNCATED_CHARS_TOTAL.as_str(),
        "Characters of tool output that the conversation never carried because \
         the whole result did not fit the context, labelled by `tool`, one \
         increment per cut carrying the number of characters left out. \
         `cogneva_tool_output_truncated_total` counts the cuts and says nothing \
         about their size -- one that dropped ten characters and one that \
         dropped a hundred thousand are one increment each on it -- so a reader \
         deciding whether oversized results are eating the conversation needs \
         these two together, and the ratio or the spend alone cannot be \
         recovered from either. On the cut that reports it the value is the \
         original length minus the kept prefix, the same two numbers the marker \
         in the text carries, so the count is the number of increments and this \
         is their sum.",
    ),
    (
        crate::metric_names::TOOL_OUTPUT_ARCHIVE_FAILED_TOTAL.as_str(),
        "Archives of a truncated tool output's full text that did not land, \
         labelled by `tool` and by `cause`, one increment per cut whose tail was \
         not stored. A truncation marker carries an `artifact://` reference only \
         when the full text was stored, so this counts the times a caller \
         following such a reference would find nothing. The causes are a closed \
         set and every cut ends in one of them: `unconfigured` (no base URL was \
         handed to this deployment, so nothing was attempted), `unreachable` \
         (the API did not answer), `refused` (it answered and rejected the \
         write), and `unkeyed` (the run carried no task id, so there was no \
         source to file the tail under and no attempt was made). That closure is \
         the point of the series: `cogneva_tool_output_truncated_total` minus \
         this one is the number of cuts whose tail landed, and the subtraction \
         only names a share while every unlanded cut is counted here. Read it \
         beside the truncation counter, where a share of the two moving together \
         names the archive endpoint rather than ordinary truncation.",
    ),
    (
        crate::metric_names::EVENT_BUS_DROPPED_TOTAL.as_str(),
        "Run histories the event bus mouth threw away instead of publishing, \
         labelled by `cause`, one increment per dropped event. It is on this \
         series and not on a log line because a dropped AgentEnd is the loss of \
         the run's only durable copy: with a mouth attached the event goes to \
         the bus and nowhere else, so nothing else holds it and nothing retries \
         it. The two causes are different defects and are why the series carries \
         a label rather than a single number. `buffer_full` is backpressure \
         working as designed -- the publisher is retrying a fault that clears, \
         the bounded queue at the entrance fills, and the run would have gone out \
         had it waited -- so a reader treats it as a capacity reading and the \
         next healthy tick stops incrementing it. `payload_rejected` is a \
         permanent refusal: the bytes are larger than the transport's own frame \
         limit and no retry makes them smaller, so the mouth consumes that event \
         once and the run is unreadable forever, which is the one a reader alerts \
         on. The two must not be summed, because the total cannot tell a bus that \
         was busy for a moment from a conversation that is gone; read them per \
         `cause`.",
    ),
    (
        crate::metric_names::EVENT_BUS_LAGGED_TOTAL.as_str(),
        "Events a broadcast subscriber missed because it fell behind its \
         channel, labelled by `consumer`, one increment per event the channel \
         dropped under it. It is the consumer-side twin of the dropped counter \
         and a different defect: a drop is the bus refusing to hand an event \
         out, a lag is a subscriber too slow to take one the bus offered, and \
         the repair for the second is on the reader, not on the bus. Written \
         because every `Lagged` arm in the platform already computed the count \
         -- `RecvError::Lagged(n)` hands it over -- and then had nowhere to read \
         it, so a subscriber that was silently shedding events looked exactly \
         like one that had nothing to do. Read it per `consumer`: two consumers \
         falling behind are two incidents with two owners, and the sum names \
         neither. A `consumer` label that is absent means that subscriber does \
         not publish this yet, not that it never fell behind. Pair it with \
         `cogneva_loop_tick_age_seconds` for an event-driven loop: the age resets \
         on every wake, so `min_over_time(..._loop_tick_age_seconds{loop=...}[W])` \
         low says the loop woke and a rising lag beside it says it woke and could \
         not keep up -- idle and stuck are then two different readings instead of \
         one. Zero here is not the same as healthy: it is also what a consumer \
         that never wakes publishes, which is why the throughput counter beside \
         it is the half that says whether anything was taken at all.",
    ),
    (
        crate::metric_names::EVENT_BUS_CONSUMED_TOTAL.as_str(),
        "Events a broadcast subscriber took off its channel, labelled by \
         `consumer`, one increment per event it handled. The positive side of \
         the same channel as the dropped and lagged counters, and the only one \
         of the three that a stopped consumer has to stop writing: zero lag is \
         what a subscriber that is keeping up publishes and also what a \
         subscriber that never wakes publishes, and the loop age gauge cannot \
         separate them for an event-driven loop, whose age is the process's age. \
         Read it per `consumer`; the throughput of one says nothing about \
         another. A reader comparing an interval with no takings against a \
         sibling the same interval needs to know what the channel carried, \
         because an event-driven consumer with nothing to take and one that is \
         gone write the same flat line -- so the two are told apart by their \
         producer, not by this series alone. That producer is \
         `cogneva_event_bus_emitted_total`, and the two are read together by \
         `event_bus_consumer_stopped_taking`, which fires when the channel was \
         offered events and this series took none of them. Two things a reader \
         has to carry. The unit is one event, so a \
         consumer that wants to count whole passes has to mint its own name \
         rather than publish here: mixing per-event and per-pass under one name \
         makes `rate()` mean different things per label value, and two series of \
         the same family stop being comparable. And the increment is written by \
         the consumer while it is consuming -- for the collaboration listener, \
         under the same lock that is held from the receive through the handler \
         -- so the store round trip sits on the consumption path. A rate read \
         here is therefore a floor on what the channel carried, not the channel's \
         throughput, and a store that slows down moves this series and the lag \
         beside it in opposite directions, which means the two are not two \
         independent readings of a failure but one event seen twice.",
    ),
    (
        crate::metric_names::EVENT_BUS_EMITTED_TOTAL.as_str(),
        "Task events the orchestrator handed to the broadcast its consumers \
         read, labelled by `kind`, one increment per event offered. It is the \
         producer side of the same channel the dropped, lagged and consumed \
         counters describe, and the operand those two consumer-side readings \
         cannot supply on their own: a consumer that is gone and one with an \
         empty channel both stop taking, so a flat `cogneva_event_bus_consumed_total` \
         is a fault only beside this series, which is what says the channel was \
         not empty. `event_bus_consumer_stopped_taking` is that comparison. The \
         `kind` label is the event variant and matches the `event` tag the same \
         events are serialized with, so a raw log line and a counter cell are \
         read with one word. Counted where the event is offered rather than \
         where a send returned, so the total is what the executor published and \
         does not shrink when a subscriber detaches: an event offered to a \
         channel with no reader is the case the consumer side can never show, \
         and counting only the delivered ones would hide it. It is a counter and \
         not a gauge for the same reason as its siblings: the interesting \
         quantity is how much traffic the channel carried over a window, which \
         is a difference between two samples, and a gauge would have to be reset \
         by whoever read it.",
    ),
    (
        crate::metric_names::TOOL_OUTPUT_FETCH_BUDGET_EXHAUSTED_TOTAL.as_str(),
        "Reads of an archived tool output that found the source but stopped \
         short of its bytes because the payload outgrew the byte budget the \
         caller set, labelled by `tool`, one increment per bounded read. A \
         bounded read is not a failure -- the budget is the caller's, and \
         raising it or fetching the source directly both answer it -- so this is \
         counted rather than alerted. It exists because the alternative is \
         silence: a read that returned no bytes for being too large and a read \
         of a source that is not there would otherwise be the same answer at \
         every caller. Read per `tool`: a series climbing for one tool names \
         that tool's payload shape, not the budget as a whole.",
    ),
    (
        crate::metric_names::SOP_DISPATCH_TOTAL.as_str(),
        "What the SOP dispatch loop did with one configured face on one tick, \
         labelled by `face` and `result`, one increment per face per tick that \
         came due. The results are a closed set and are four different facts: \
         `compiled` (the document was read, compiled and handed to the planner), \
         `skipped_unchanged` (read but byte-identical to the last compiled \
         revision, so nothing was re-injected), `fetch_error` (the platform could \
         not be read or refused the request) and `compile_error` (the document \
         was read but is not a usable plan). Read per `face`: a face whose \
         `compiled` stops while the loop's tick age stays fresh is a face the \
         document or the endpoint stopped answering for, which a single total \
         across faces would hide.",
    ),
    (
        crate::metric_names::SOP_TASKS_INJECTED_TOTAL.as_str(),
        "Tasks a face's plan put into the DAG, labelled by `face`, summed over \
         ticks. It is the numerator the dispatch counter cannot give: a compile \
         that succeeded and injected nothing and one that injected ten are the \
         same `compiled` cell, so a reader wanting to know the plan is doing work \
         reads this beside it. It counts what the injection answered with, not \
         what it was asked for, so a re-submission the graph already held adds \
         nothing -- the same document does not read as new work.",
    ),
    (
        crate::metric_names::SOP_UNMAPPED_ROLE_TOTAL.as_str(),
        "Tasks whose declared `role` named no row in the executor table, labelled \
         by `face`, one increment per such task at compile time. The role is \
         carried through verbatim and nothing maps it to a kind of hand yet, so \
         such a task is injected and may then never be picked up; this series is \
         what turns that into a reading instead of a hang that looks like work in \
         progress. Read per `face`: a face whose tasks all land here is a face \
         whose plan names roles this deployment does not staff, which is a \
         configuration gap rather than a defect in the document.",
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
        crate::metric_names::PROMOTION_WEEK_CHANGES.as_str(),
        "Promotion-ledger entries of one ISO week, by how they ended: \
         `promoted`, `rolled_back`, `failed`, `pending` (decided and still in \
         flight -- pushed, or not yet picked up by a cluster) and \
         `awaiting_review`. Labelled by `week` (e.g. `2026-W42`) and by \
         `outcome`; one cell per week inside the trend report's window, \
         re-written every report cycle rather than incremented, so a cell \
         standing at 0 and a cell nobody wrote stay different readings. The \
         cells are what separates a channel that generates nothing from one \
         that generates and cannot land: on `promoted` alone both read as a \
         quiet week, and that is the reading a six-week stall hid behind. \
         A week older than the window has no series at all rather than a zero \
         -- absent there says the report stopped covering it, not that it \
         landed nothing",
    ),
    (
        "memory_unextracted_raw",
        "Archived raw sources whose summary is still owed: missing a summary \
         and not already diagnosed as unextractable, as last scanned. A source \
         that failed extraction on its own content is not owed one -- it has a \
         dead-letter record instead, and re-driving it would reproduce the same \
         diagnosis -- so it leaves this reading and its discard is counted on \
         memory_operations_total{operation=\"dlq_written\"}",
    ),
    (
        "memory_unextracted_raw_aged_out",
        "Subset of the above that aged past the re-drive window. These are not \
         given up on: the reconcile pass picks them up on its own, at most \
         aged_out_redrive_batch per round, so the reading falls as the debt is \
         repaid. A debt deeper than that batch still moves from both ends -- \
         a quarter of the batch buys the oldest entries -- so a value that \
         stays put cannot be explained by the newest debt crowding out the rest. \
         What it cannot say on its own is whether it is falling: a \
         value that stays put is a debt larger than the batch, a pass that \
         stopped, or a batch of zero, and the count of sources each pass \
         re-drove separates the first from the other two -- it is on \
         memory_operations_total{operation=\"aged_out_redrive\"}",
    ),
    (
        "memory_unextracted_scan_failed",
        "Whether the last reconcile scan could not be completed, 1 or 0. Every \
         reading that scan publishes is stamped only once it returns a \
         result, so a scan that fails leaves them at their last, healthy value \
         while the rule over the aged-out one goes on trusting them until its \
         age bound closes it -- an unreadable backlog and a cleared one then \
         look the same. This is the reading that says the scan itself did not \
         answer; it is written on every attempt, so a scan that completes \
         keeps it at 0",
    ),
    (
        "memory_dead_letter_raw",
        "Archived raw sources the ingest path has permanently given up on: each \
         one was diagnosed unextractable, moved to the dead-letter namespace, \
         and will not be picked up again. It is the stock that pairs with the \
         dlq_written cell on memory_operations_total, which counts only \
         arrivals -- a count that never falls cannot be seen on a rate, and \
         this is the other half. A standing stock is not a fault and no \
         threshold is declared for it: content that cannot be read is an \
         outcome the extraction path is expected to meet, so a rule on this \
         value would fire every time the reading is fresh. What a growing stock \
         does say is how much has accumulated, which is what decides whether \
         the dead-letter namespace needs reclaiming at all. Reclaiming is not \
         simply deleting: the backlog excludes these sources because their \
         dead-letter record exists, so removing a record puts its raw back into \
         the debt on the next scan -- re-driven, re-diagnosed and re-written. \
         It is a gauge and not a counter, and like the sibling backlog readings it \
         is stamped only once a scan returns, so a scan that fails leaves it at \
         its last value; a zero on that scan is not the same fact as no scan at \
         all, and the pair that tells them apart is this series' \
         _observed_timestamp_seconds companion together with \
         memory_unextracted_scan_failed, which is written on every attempt",
    ),
    (
        "metrics_samples_rows",
        "Rows currently held in the metrics sample log",
    ),
    (
        "metrics_sample_sweep_failed",
        "Whether the last sample-log pass could not be completed, 1 or 0 \
         (a pass is the sweep when the log is over budget, otherwise the \
         measure). The row count, the budget, the floor verdict and the bytes \
         are all stamped only once a pass returns, so a pass that fails leaves \
         every one of them at its last value, and the two capacity rules then \
         trust a frozen log until their age bound closes them -- a sweep that \
         cannot run and a log that is not growing look the same. This reading \
         says the pass itself did not answer; it is written on every pass, so a \
         pass that completes keeps it at 0",
    ),
    (
        "metrics_samples_budget_rows",
        "Rows the metrics sample log is allowed to hold; the sweep deletes \
         oldest-first past this, stopping at each gauge series' newest row. \
         Every reading here names the deployment that published it, because \
         this is one deployment's own declaration and the log they land in is \
         shared, so without the name a deployment that declares no cap at all \
         is served the one the deployment beside it declared",
    ),
    (
        "metrics_samples_over_capacity",
        "1 when the sample log is over its row budget and cannot be pruned \
         further without deleting a gauge series' current value, 0 otherwise. \
         Carries the same deployment label as the budget it is read beside, \
         and is absent rather than 0 for a deployment that ran no pass to \
         report on",
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
         by the table they came from and by the deployment that ran the pass. \
         Reported every pass, so a table whose reading stays non-zero is one \
         the release is not draining. The deployment is part of the identity \
         because every deployment runs the release: a series keyed by the table \
         alone would carry two deployments' passes, and the reader's minimum \
         over that series is zero whenever the pass that ran second found the \
         rows already gone",
    ),
    (
        "metrics_retirement_release_failed",
        "Whether the last retired-rows release pass could not be completed, 1 \
         or 0. The removal reading above is stamped only once the release \
         returns, so a release that fails leaves it at its last value, and the \
         rule that watches it goes on reading a frozen count until its age \
         bound closes it -- a release that cannot run and a table that is \
         drained look the same. This reading says the pass itself did not \
         answer; it is written on every pass, so a pass that completes keeps it \
         at 0",
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
        "llm_upstream_shape_errored",
        "Whether each LLM upstream is holding a shape-class rejection \
         (400/404/422) that has not been settled yet, 1 or 0. Such a status \
         code has two opposite causes -- the request's shape, which any \
         compatible upstream would refuse as well, or the upstream being broken \
         for everyone -- and one response does not say which, so the request \
         path only records the question and a minimal probe answers it. This is \
         that question's own reading: 1 while it is open, 0 once the prober has \
         ruled either way. It is not a health verdict and does not feed one; \
         the upstream stays out of the suspect window until the probe fails",
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
        "cogneva_version_contract_unjudged",
        "1 when the round could not judge the version contract at all, 0 when it \
         did. The judgement needs the tracked main, read from the bare \
         repository, so a round that cannot read it publishes no verdict -- and \
         on a reading that only ever changes its value the store keeps serving \
         the last verdict with a companion clock that has stopped, which is the \
         same shape as a contract that holds. This is the reading that says \
         which of the two it is",
    ),
    (
        "gitops_pull_failed",
        "Whether the last GitOps poll could not be completed, 1 or 0, written \
         on every attempt and labelled by the cluster that pulled. The puller is \
         what brings this cluster's desired state in: it fetches the release \
         branch, picks up the promote tag at its head, applies the config change \
         or runs the canary. A poll that fails is written to the log and nothing \
         else -- the loop's beat is stamped at the top of the cycle whether or \
         not the poll returned, so the liveness readings keep saying the loop is \
         running while it does nothing. A puller that cannot reach the branch \
         and a cluster with nothing to pull are then the same picture, and the \
         cluster quietly stops receiving updates. This is the reading that tells \
         them apart; it is written on every attempt, so a poll that completes \
         keeps it at 0",
    ),
    (
        "baseline_port_tick_failed",
        "Whether the last baseline port tick could not be completed, 1 or 0, \
         written on every round. The trigger carries this instance's promoted \
         changes onto a new upstream release: it refreshes tags, finds the \
         newest baseline, plans the port and executes it, and a round that has \
         nothing to do is the ordinary state. A round that fails -- its \
         repository unreachable, or the port itself failing -- was written to \
         the log alone, and the loop's beat is stamped whether or not the tick \
         returned, so the liveness readings keep saying the loop runs while it \
         moves nothing. A trigger that fails every round and an instance with \
         nothing to port are then the same picture. This is the reading that \
         tells them apart; it is written on every round, so a round that \
         completes keeps it at 0",
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
        "cogneva_change_dispatch_readouts",
        "What one change-ledger dispatch pass did with the set still owed, by \
         readout, on the pass that recorded it: pending is the whole owed set, \
         and the rows that set broke into are submitted (handed over this pass), \
         already (a task the goal path already had), failed (this pass tried to \
         hand one over and the goal path refused), dead_environment (a task held \
         it, the environment refused every handover the budget allowed, and it \
         is given up on), held_back (owed, no task holds it, and this pass \
         handed nothing over) and unmeasurable (owed, and this pass could not \
         settle what is being asked of it). rearmed counts handovers that had to \
         be re-filed at a later generation and is an annotation over those \
         outcomes, not one of them, so submitted + already + failed + \
         dead_environment + held_back is what has to equal pending -- and the \
         equality is the reading: a pass whose counts do not add up to the set it \
         read is a pass that lost track of rows, which is how the environment- \
         killed ones went unrecorded once. Written once per pass, so a pass that \
         stops writing is visible as the companion timestamp going stale rather \
         than as a value that stops changing. Why this tick looks the way it does \
         is the pass's own verdict, read from cogneva_change_dispatch_verdict: \
         the two are separate families because the verdict changes with the tick \
         and a cell's identity must not, and a label that changes would leave one \
         frozen cell behind per combination the pass ever reached",
    ),
    (
        "cogneva_change_dispatch_verdict",
        "The verdict one change-ledger dispatch pass reached, as one cell for \
         each combination of the two closed sets -- decision (proceed, or \
         paused_llm_pool_unavailable when no configured upstream could serve a \
         request) and pool_reading (available, unavailable, or absent when no \
         snapshot said anything either way). The cell the pass reached is 1 and \
         the other five are 0, written on every pass: a cell that stopped being \
         written would go on being served at its last value, so a pass that \
         stops reaching a verdict would read as a verdict that never moved, and \
         a zero answers the question a reader actually asks -- whether this \
         pass's reason was this one -- without having to know which combination \
         is absent. The cells pair with the counts in \
         cogneva_change_dispatch_readouts by being published from the same pass \
         rather than by a label: a reader takes the two at one timestamp, which \
         is what the debt's own panel does, and neither family has to carry a \
         value that changes under its own label",
    ),
    (
        "cogneva_backup_run_readings",
        "What the backup account holds, by readout, read from the rows the backup \
         runs wrote rather than measured again from the volume. The run counts, \
         runs_ok / runs_failed / runs_running, cover the whole account and never \
         drop — every run leaves one row and no row is ever deleted. \
         runs_abandoned counts the runs the account had to conclude itself, \
         because they stopped reporting, and it is counted apart from \
         runs_failed: a run that reported a failure was there to say so, and a \
         run that went missing leaves a different repair behind. \
         latest_finish_age_seconds is how long ago a run last concluded anything, \
         and latest_success_age_seconds how long ago one last succeeded: the two \
         together separate a backup loop that is running and failing from one \
         that is not running at all, and a count cannot tell either from a week \
         of health, because a failing run prunes nothing and leaves the same \
         number of packages behind. oldest_running_age_seconds is the age of the \
         oldest run still open — the only reading a run that was killed mid-flight \
         ever leaves, since a process that dies writes nothing else — and it is \
         absent, not zero, when no run is open; \
         oldest_running_deadline_seconds is the deadline that same run was \
         opened under, published beside the age because the two are compared to \
         each other to decide a run is gone, and an age judged against a bound \
         copied from configuration would be one run's duration measured against \
         another's allowance. The retention pair is one run's \
         own answer, kept together: kept and keep_target are what the newest \
         successful run left against the bound it ran under, and \
         oldest_kept_age_seconds is the age of the oldest package that survived \
         its pass, which is the half a count cannot falsify. \
         newest_package_bytes is that run's package size in bytes — a backup \
         that suddenly shrinks is one that stopped capturing, and nothing else on \
         this series moves when that happens. Absent readouts are absent facts: \
         latest_success_age_seconds is missing until a run has ever succeeded",
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
        "cogneva_worktree_index_sample_incomplete",
        "Whether a resident worktree's index sample could not be completed by \
         its last attempt, 1 or 0, by worktree. The missing-files count is \
         written only once both git reads succeed, so an index that is there \
         and cannot be counted leaves that series at its last value -- the \
         healthy one -- where its own age guard goes on to close the rule over \
         it. This is the reading that says the sample itself failed rather \
         than the tree being fine; it is written on every attempt, so a tree \
         whose samples complete keeps it at 0",
    ),
    (
        "cogneva_registry_maintenance_reading_unix",
        "When a registry maintenance round last finished, as a Unix timestamp. \
         Written only on completion, so a value that stops advancing is the age \
         of the last round that got through; 0 until one does, and that 0 is \
         published rather than left absent, so a store no round has ever \
         finished reads as never having done one instead of as a series with \
         nothing to say. Read with the run count beside it, where a count rising \
         against this timestamp not moving is a round that started and did not \
         finish -- a reading an absent timestamp cannot carry: the comparison \
         against it is empty, so the state with nothing ever finished would be \
         the one the pair could not state",
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
        "1 when this openai-style upstream's usage-reporting capability has been settled from \
         evidence, 0 while it is still assumed, by upstream. Read next to the \
         token counters: an upstream carrying traffic while we still guess at \
         stripping stream_options cannot report non-zero tokens, and 'the \
         upstream does not report usage' and 'we never asked it to' look alike \
         in every other series",
    ),
    (
        "llm_tool_calls_verdict_measured",
        "1 when this openai-style upstream's support for native tool calls has \
         been settled from evidence, 0 while a request carrying tools would be \
         routed to it on the assumption that it does, by upstream. Read next to \
         the upstream health gauge: an upstream serving traffic with this at 0 \
         is one where a tool call may come back written as text, and neither a \
         refused request nor a shape-error reading can show that, because the \
         request succeeded",
    ),
    (
        "llm_temperature_verdict_measured",
        "1 when this openai-style upstream's demand for temperature=1 has been \
         settled from evidence, 0 while the vendor profile is what decides \
         whether the caller's temperature is rewritten, by upstream. Read next \
         to the upstream health gauge: at 0 the fallback may go either way, and \
         only one of them is visible. A profile that spares the value lets the \
         upstream refuse it, which the failure counter catches; a profile that \
         clamps overwrites a number the caller chose and returns 200, so the \
         request succeeds with sampling nobody asked for and neither the clamp \
         counter nor the response tells that the verdict behind it was a guess",
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
        "llm_upstream_position",
        "Where each upstream sits in the order a call tries the pool, 0 first, \
         by upstream. Reported for every upstream the process was configured \
         with, including ones it has never called: a position is defined for all \
         of them, and it is precisely the untouched ones that head the order. \
         This is the running process's effective order, not the declared one -- \
         suspects are demoted behind everything that has not failed, with config \
         order kept inside each group -- so when the whole pool is suspect these \
         values are the declared order, read from the process rather than \
         inferred from the Secret and a pod-start time",
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
         timestamp; 0 until one does, and that 0 is published rather than left \
         absent, so a store no round has ever finished reads as never having \
         done one instead of as a series with nothing to say. The seed is the \
         persisted reading, not the process start: a start time would read as a \
         reclaim that has just completed, which is exactly the window in which \
         the store should have been reclaimed",
    ),
    (
        crate::metric_names::MAINLINE_CI_HOLD_SECONDS.as_str(),
        "Seconds the rollout has been held by the upstream CI gate: 0 while it is \
         not held, and the age of the current hold while it is. It is the \
         observation the hold bound is on -- the state file and the heartbeat \
         line say that a hold exists and which revision it holds, and this says \
         how long it has lasted, which is what tells a round that is waiting out \
         a red upstream from a rollout that has stopped for the week. 0 is a \
         reading and not an absence: the store only adds and overwrites, so a \
         hold that ends is written back as 0 by the round that ends it -- and \
         since the gate fails open (only an explicit failure holds), most rounds \
         write that 0. No label per revision, for the reason the build-duration \
         gauge beside it gives: a hold describes one revision, and a series per \
         revision would leave a permanent row for a value that describes only \
         itself; the revision is carried by the state file's `ci_hold.rev` and \
         by the heartbeat's `ci_hold=` cell",
    ),
    (
        crate::metric_names::MAINLINE_BUILD_SECONDS.as_str(),
        "Duration in seconds of the most recent round's `cargo build --release`, \
         by `outcome`. The poll-cycles counter beside it answers whether the \
         loop is cycling; this answers how much of a round the host spent \
         compiling, which no other series publishes -- the rollout-job ratios \
         describe the dispatched Job, and the build happens on the host before \
         any Job exists. It is a gauge and not a histogram because the store's \
         bucket schemes top out at 65.5s and a compile runs for minutes, so a \
         histogram would hold every observation in its overflow bucket and \
         publish a quantile that is always the top bound. Read the `ok` cell: a \
         build whose duration climbs toward the host's disk ceiling is what a \
         rollout-stalling round shows before it stalls, and `failed` is a \
         reading of its own -- a compile that gave up in seconds is not a fast \
         build",
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

    /// A counter's description lives in the counter table.
    ///
    /// Pinned by hand for the entries whose kind was wrong, because nothing
    /// else in the tree judges it: a name's kind is written only at its
    /// `record_*` call site, so a description filed under the wrong kind and a
    /// description that was never written are the same thing on the exposition
    /// -- the placeholder. Both names below are written with `record_counter`.
    #[test]
    fn a_counters_description_is_found_under_counter() {
        for name in [
            "cogneva_change_tier_total",
            "cogneva_eval_gate_outcomes_total",
        ] {
            assert!(
                metric_description(MetricType::Counter, name).is_some(),
                "{name} 由 record_counter 写，描述却不在 counter 表里"
            );
            assert!(
                metric_description(MetricType::Gauge, name).is_none(),
                "{name} 不该在 gauge 表里——暴露面按实际 kind 查描述，登错 kind 只出占位符"
            );
        }
    }
}
