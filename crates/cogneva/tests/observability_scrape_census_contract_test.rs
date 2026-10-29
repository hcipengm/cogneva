//! principle: feedback_a_debt_count_comes_down_by_writing_the_reader_not_by_deleting_the_registration
//! principle: feedback_a_registration_row_is_evidence_only_if_its_producer_still_spells_the_name
//! principle: feedback_a_retired_name_is_a_third_face_the_census_must_read_from_the_list
//! principle: feedback_a_newly_published_series_lands_on_a_face_or_moves_the_gap_count
//! Every series this build can render is on a face that records it.
//!
//! A series reaches the scrape two ways. One is the metric store: a producer
//! hands a name from `cog_core::metric_names::ALL` to a backend, rows persist,
//! and the closed set is a complete answer to "what can this build still
//! write" -- which is what the reader-face census walks, and why a name
//! published that way cannot be missing from it.
//!
//! The other is an `Observable`'s rollup: the collecting process renders a
//! `RawMetric` into its own exposition, and the series vanishes with the
//! process, so there are no rows to leave behind and no registry to be in.
//! Those names are served on the scrape exactly like the stored ones, and
//! nothing about publishing one makes anyone notice it. A name a producer
//! renders and no face records is a reading on the wire that no rule can name,
//! no panel draws, and no table points back at its producer -- and every one of
//! those absences is silent, because the series itself is right there.
//!
//! So this walks that population. The denominator is every series name the
//! source tree publishes, read from the two shapes a producer uses:
//! `pub const <ident>: &str = "cogneva_..."` and `RawMetric::new("cogneva_...")`.
//! Each one must land on a face, or be given a verdict below.
//!
//! The faces, in the order they were added: the closed set, which is what the
//! store can hold; the chart's alert rules, which is what can wake someone; the
//! Grafana manifest's panels, which is what a person looking at the stack sees;
//! and the two contract tests' own produced tables, which name the file that
//! publishes each series and are what a rule or a panel is licensed against.
//! The tables are read out of those files rather than copied here, because a
//! copy would be a second answer to "which series does this workspace produce"
//! -- the drift those tables exist to prevent. Renaming one fails this test
//! loudly instead of emptying a face in silence.
//!
//! The last two are not the same kind of face as the first three. A rule and a
//! panel read a series; a table records who publishes it. A series carried by a
//! table and by nothing else is registered, not read, and the difference is its
//! own count below -- otherwise the union would read as coverage.
//!
//! The ceiling, so a reader does not have to guess it. A name built at run time
//! (`format!`, `concat!`) has no literal to parse and is outside the
//! denominator; today the tree has none. Everything else is covered, and the
//! literal scan below is what makes that a claim rather than a hope: every
//! well-formed `"cogneva_..."` literal under `crates/` must be on the
//! denominator, in the closed set, a recording-suffix derivative of a closed
//! set name, one of two declared non-series, named by a produced-table row
//! whose producer lies outside `crates/` and really spells it -- the textfile
//! collector in the chart is such a producer, and its names are born nowhere in
//! Rust -- or read by a face. The face leg is for the producers that run
//! outside this repository entirely: the host readings channel publishes its
//! series from a script, so there is no file here for a produced-table row to
//! name, and what this tree can still say about such a name is who reads it. A
//! producer that spells its name some other way and that nothing reads lands
//! there as an unexplained literal rather than as a series nothing checks.

use std::collections::BTreeSet;

mod common;

use common::routes::{read_repo, repo_root};
use common::rust_production::all_crate_files;
use common::service_topology;

#[path = "common/dashboard.rs"]
mod dashboard;
#[path = "common/promql.rs"]
mod promql;
#[path = "common/unread.rs"]
mod unread;

use promql::metric_names_in;
use unread::{unresolved_readers, Unread};

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";

/// The two produced-series tables, and the line that opens each. They are read
/// as text out of the test that owns them: the face is "the table in that
/// file", and taking a copy here would let the two disagree about which series
/// this workspace produces.
const ALERT_RULE_CONTRACT: &str = "crates/cogneva/tests/observability_alert_rule_contract_test.rs";
const ALERT_TABLE_OPENS: &str = "const PRODUCED: &[(&str, &str)] = &[";
const DASHBOARD_CONTRACT: &str = "crates/cogneva/tests/observability_dashboard_contract_test.rs";
const DASHBOARD_TABLE_OPENS: &str = "const PRODUCED: &[(&str, &[&str], &str)] = &[";

/// Names that look like series and are not, so the scan below does not read
/// them as a producer. Each is a different kind of exception: a table name, a
/// label key, a prefix on its own, and fixtures the tree publishes on purpose
/// without any build being able to write them.
///
/// An entry here is an exclusion from the population, so the directions that
/// can be checked are: the name has to be a literal the tree really carries,
/// it has to say why, and it must not be in the closed set -- a name the store
/// writes is a series whatever the sentence beside it says.
///
/// What that leaves is the honest limit of this table, and it is worth naming
/// rather than leaving for someone to find: a scrape-only series put here is
/// exempted from a census that would otherwise have demanded a verdict, and
/// nothing can tell such an entry apart from a real table name.
const NOT_A_SERIES: &[(&str, &str)] = &[
    (
        "cogneva_owner_leases",
        "the Postgres table the owner-lease rows live in, declared as LEASE_TABLE in cog-storage/src/owner_lease.rs; the live scrape carries no series of that name",
    ),
    (
        "cogneva_no_such_series",
        "a fixture: `MetricName::for_tests_only` exists so a test can publish a name no build can produce, which is the reading the closed set's own liveness check needs",
    ),
    (
        "cogneva_",
        "the prefix on its own, spelled by a test helper that asks whether a string looks like one of this build's series names; it is the start of a name and never a name, and nothing scrapes it",
    ),
    (
        "cogneva_io_managed",
        "a label key, not a series: the alert mirror selects the Prometheus alerts to copy by carrying this label, and the key travels in `observability.infra_watch.mirror_alerts.label` (cog-observability/src/config.rs) -- the same string is a value of `ALERTS`'s label set, not a name the exposition publishes",
    ),
    (
        "cogneva_io_face",
        "a label key, not a series: the rule that raised a row declares which face acts on it, and the declaration rides the stored row's labels while staying out of the dedup key (cog-observability/src/infra_watch.rs) -- it names who is called, never a measurement, so the exposition publishes nothing under it",
    ),
    (
        "cogneva_token",
        "a browser storage key, not a series: the WebUI keeps the access token under it (web/src/auth/session.ts) and the gateway page reads the same spelling back, so the string never leaves a localStorage call and no exposition publishes anything under it",
    ),
    (
        "cogneva_lang",
        "a browser storage key, not a series: the takeover page remembers the operator's language under it (crates/cog-gateway/src/takeover.html), which is a client-side preference and not a measurement any process reports",
    ),
];

/// The prefix every series name this build publishes is spelled with.
///
/// Assembled from two pieces rather than written whole, and that is not
/// decoration: the scan at the bottom of this file reads every `.rs` under
/// `crates/`, and this file is one of them. A needle spelling the prefix as a
/// single literal searches for itself, finds itself, and reports its own
/// machinery as a series nothing produces -- the first run of this census did
/// exactly that, twice. Two pieces leave this file free of the shape it looks
/// for, which is the only way the scan can cover the tree it is written in.
const NAME_PREFIX: &str = concat!("cog", "neva_");

/// The recording suffixes the exposition derives from a stored name: a
/// classic histogram's `_bucket`/`_sum`/`_count`, and the companion the store
/// renders beside every series it serves. A literal with one of these is a
/// derived name, not a third spelling of a producer.
const DERIVED_SUFFIXES: &[&str] = &["_bucket", "_sum", "_count", "_observed_timestamp_seconds"];

/// Names no face carries, with the verdict.
///
/// The first run of this census is where this table comes from: every entry was
/// judged once, by reading what its producer says the series is for, rather
/// than classified by shape. An entry's reason is a claim about the
/// repository and it goes stale the way any other claim does -- `Gap` here
/// means a face is owed and missing, so when one is written the entry leaves
/// the table.
///
/// The `Gap` count is pinned by `GAPS_AT_SCRAPE_CENSUS`, and the two `Elsewhere`
/// readers are looked up in the faces rather than counted in the words of the
/// reason. Both are the same discipline the reader-face census applies to the
/// closed set, on a population it cannot see: a name the store never holds has
/// no rows to compare against a retired list, so the recording surface for it
/// is exactly this table.
const UNREGISTERED: &[(&str, Unread)] = &[
    // A name that had ten `Gap` entries here until two panels were written to
    // read the pin census: the four pin families with their counts and the
    // plugin population with its count. Each entry said what was owed was a
    // reader of the population rather than a rule -- no threshold is derivable
    // on them, since a pin published for a reader in another process lands in
    // the unsatisfied family beside one with no reader anywhere -- and the
    // panels now carry both halves: the counts summed into a stat and the
    // members drawn with the pin, reader and publisher names in the legend.
    // The entries are gone and `GAPS_AT_SCRAPE_CENSUS` came down by ten. What
    // the panels do not settle is a bound, and one was never owed here: the
    // fourth count alone is three different facts (every edge declared, no init
    // reads at all, or the census never ran) and the last of those is read from
    // the absence of the family rather than from the number.
    // A series a build publishes with nothing anywhere reading it, found by a
    // scan rather than by a reader: the count of observables registered for
    // scraping. Its producer spelled the name inline in the exposition body,
    // which is a shape the declaration scan cannot read, so the series was live
    // on the wire and named by no gate until the literal scan was widened to
    // see it. What it is owed is the face its own comment intends.
    (
        "cogneva_observables_registered",
        Unread::Gap("how many observables are registered for scraping, rendered into the exposition body rather than recorded, which is why the registry does not hold it. The producer's own comment gives the reason it exists: registration is keyed on a static type id, so a service registered as a concrete type is invisible to this endpoint and its metrics leave the scrape silently -- the count is what lets a reader see the family go missing rather than wait for a person to notice. Two sibling series in that file are both read by a rule and this one is read by nothing, so the face is owed rather than decided"),
    ),
    // A name that had a `Gap` here until a panel was written to carry it:
    // `cogneva_stream_read_failures_total`, the count of reads a consumer's loop
    // had refused, split by the cause the bus itself named. The entry is gone
    // and `GAPS_AT_SCRAPE_CENSUS` came down by one because the face it was owed
    // now exists -- the stream-consumer panel draws it against the silence and
    // the loop population it is read with. What that does not settle is a bound
    // on the number, and that was never this table's to give: the value is a
    // pulse rather than a state (on the live series every consumer of a pod
    // steps 0 to 4 inside the first seconds of a new pod and then holds there
    // for the rest of that pod's life, so greater than zero is true until the
    // carrier is replaced and a rate reads that replacement as the noise
    // subsiding), and a rule that reads it has to take its bound from the split
    // rates rather than guess one. That debt sits on the alert side.

    // The task graph's five readings. The count is the one a person would
    // expect to be drawn and is not; the other four are what the count cannot
    // say -- how long the oldest piece has been waiting, how many pieces have
    // nobody behind them, whether the reader itself is still reading, and how
    // often it failed.
    //
    // A name that had a `Gap` here until a rule was written to read it:
    // `cogneva_task_oldest_untouched_seconds` and
    // `cogneva_task_graph_measure_last_seconds`. Both entries are gone and
    // `GAPS_AT_SCRAPE_CENSUS` came down by two, the same way the stream-read and
    // unowned-row families left this table. What the rule settles is having a
    // reader, and it settles the age's bound with it: the age is compared
    // against the graph's own read cadence rather than a number this table
    // invents, so the pair reads a reader that stopped apart from a graph that
    // is exactly as it last appeared. The rule does not draw the count by
    // status, which is why that entry stays.
    (
        "cogneva_tasks_current",
        Unread::Gap("tasks in the graph right now, one series per status. It is the shape a status panel is normally drawn from, and no panel or rule names it: a person reading the stack today sees no task count at all, and the three readings beside it are read with it"),
    ),
    (
        "cogneva_task_graph_measure_failures_total",
        Unread::Gap("reads of the task graph that failed. It is the counter beside the last-success second above, and like it has no reader: without one, a task face shows the last known shape indefinitely and reads as healthy while the reader is down"),
    ),
    // A name that had a `Gap` here until a panel was written to carry it:
    // `cogneva_task_unowned_rows`, the count of rows in a state that can still
    // move but with nobody assigned to them. The entry is gone and
    // `GAPS_AT_SCRAPE_CENSUS` came down by one, the same way the stream-read
    // family left this table. What the panel settles is having a reader; it
    // does not settle the count's bound -- a rule that watches it has to say
    // which status it is a budget for, and only the states where a claim is
    // possible belong on that line. That debt sits on the alert side, beside
    // the count and the failure counter above, neither of which is read yet.
    // The total restated from the status vector above, so that a reader can
    // name the reading instead of summing label cells it does not know the
    // label set of. It publishes no fact of its own: both series come from the
    // same snapshot in the same scrape, and the total is their sum. The debt,
    // if any, belongs to the vector it copies and is counted there.
    (
        "cogneva_tasks_readings",
        Unread::NotAReading("the whole-graph row count, restated from cogneva_tasks_current in the same scrape so a reader can name the reading rather than sum label cells; it carries no fact the status vector does not"),
    ),
    // A name whose fact a face already carries, looked up rather than asserted
    // in prose.
    (
        "cogneva_aof_repair_examined_increments",
        Unread::Elsewhere {
            readers: &[
                "cogneva_aof_repair_dropped_bytes",
                "cogneva_aof_repair_untouched_torn_tail_bytes",
                "cogneva_aof_repair_suspected_interior_holes",
                "cogneva_aof_repair_unhandled_layout",
            ],
            reason: "the rules read the finding classes; this is the denominator they are read against, which is why the module publishes one gauge per finding class and never one per verdict",
        },
    ),
    (
        "cogneva_owner_leases",
        Unread::NotAReading("the Postgres table the owner-lease rows live in, not a series: the same constant is also listed in NOT_A_SERIES so the literal scan does not read it as a producer"),
    ),
    // Published on 2026-10-28 by the change that started timing each startup
    // phase instead of only weighing it. Its sibling `cogneva_boot_rss_bytes`
    // has a face -- the rule that fires when a phase grows past its own 3-day
    // baseline -- and this entry used to say a threshold was derivable here and
    // the face owed was a rule of the same shape. It is not, and the rule that
    // was written against that shape came back off before it landed.
    //
    // Measured on 2026-10-29 over the whole retention, per process kind: the
    // app process reached `serving` in 24.1 s at its fastest and 236.2 s at its
    // slowest inside three days; the evolution process, which loads no dense
    // model and has four phases instead of seven, swings 2.1 s to 49.1 s over
    // the same window. The sibling's quantity does not swing like that at all:
    // resident set at a named moment is what the process allocated, and two
    // generations of one build hold it to within a few MiB.
    //
    // A bound wide enough to absorb a tenfold spread cannot name a phase that
    // doubled, and one tight enough to name it fires on every roll -- on the
    // day this was measured the window's fastest serving was 2.1 s while the
    // one then running read 74.8 s, so any five-second bound would have been
    // continuously true and pointed at nothing.
    //
    // Where the spread lives is not the whole startup, and reading it phase by
    // phase across eighteen rolls is what says so: `process-start` is 0.000 s
    // and `config-loaded` stays inside 0.006-0.803 s, while `embedding-dense`
    // runs 4.6-116.9 s, `embedding-sparse` 7.6-121.4 s and `reranker`
    // 21.7-188.1 s. The tenfold spread is inside the three phases that build
    // an ONNX session.
    //
    // Three explanations were checked and all three are wrong, which is worth
    // keeping. Host load: the same twenty hours hold a boot that reached
    // `serving` in 24.1 s at load 11.4 and one that took 74.8 s at load 10.3,
    // and the busiest host in the window (load 177.5) booted in 81.1 s. Disk:
    // the weights come from a read-only hostPath at /srv/cogneva/models, the
    // largest blob reads in under a second, so a 117-second phase is not
    // waiting on the disk, and the 0.33 correlation with
    // `rate(node_disk_io_time_seconds_total[5m])` is the disk busy with
    // something else. CPU: over each boot's own window the container averages
    // 0.1 to 0.43 cores and CFS throttling stays at 0.000-0.002, nowhere near
    // a quarter of its limit, and cores correlate *negatively* with duration
    // (-0.365) -- the slow boots are the ones that computed less.
    //
    // The fourth explanation was this entry's own and it was wrong too.
    // Twenty hours hold eighteen rolls and eighteen distinct images -- one
    // boot each -- so the spread looks like eighteen builds measured once
    // apiece. But the phases a build can change are the stable ones:
    // `config-loaded` moves by less than a second across all eighteen. If the
    // revision drove the spread, every phase would move with it. The revision
    // cannot be separated from the noise from this data in any case, because
    // no revision ran twice.
    //
    // So the debt is not a label to add, and it is not the sibling's shape
    // either: the sibling compares generations of one build, and startup here
    // has no two samples of anything. What is owed is startup settling enough
    // for a phase to be read against the same phase of another boot, and that
    // is not a metric's job. Until it does, a number that moved says the boot
    // landed somewhere else in a spread nothing measured yet explains.
    (
        "cogneva_boot_phase_elapsed_seconds",
        Unread::Gap("how long each startup phase took, one series per phase. Its sibling on the same label has a rule and the same shape on this one was written and withdrawn: the spread is tenfold and it sits inside the model-loading phases (`reranker` 21.7-188.1 s across eighteen rolls) while `config-loaded` never leaves a second, so it is not the build and it is not the host -- the slow boots use fewer cores than the fast ones and never throttle. A comparison needs two samples of one thing, and there is one boot per roll, so the face owed waits on startup settling; after that a five-second bound would name a phase that doubled"),
    ),
    // Two names that had a `Gap` here until panels were written to carry them,
    // and the reason they came off this table is the reason the debts were real:
    //   `cogneva_volume_capacity_bytes` -- the growth loop's `declared` and
    //   `peak` per claim. The loop decides a raise from the pair and publishes
    //   only its outcome, so a reader following the clamped rule's summary had
    //   to look the two numbers up by hand. The dashboard now draws them per
    //   claim, beside each other.
    //   `cogneva_build_gate_dimension_ceiling` -- what each of the four
    //   dimensions would allow on its own. The decision series names the winner
    //   and says nothing about the spread, and an overridden ceiling is the case
    //   where it says only that; the panel draws the four against the ceiling in
    //   force.
];

/// How many `Gap` entries this census left.
///
/// The first run is where this number comes from, and it is meant to be read as
/// it is: names the tree publishes into a scrape with no face carrying them.
/// The ratchet is the same one the reader-face census keeps -- a new series
/// nothing reads must be classified deliberately, and calling it a gap raises
/// this number on purpose. It comes down when a face is written, and that is a
/// change to this constant plus the removal of the entry: one name that was
/// here, the consumer read-failure counter, left when the stream-consumer panel
/// was written to carry it.
///
/// Nine, and the ninth is not a first-run reading: `cogneva_observables_registered`
/// was published by a shape neither the declaration scan nor the face census
/// could read, so it never became a row here. Widening the literal scan is what
/// made it visible, and the honest close was to file the debt that series had
/// been carrying -- the alternative, exempting it, would have taken a series
/// that answers on the live scrape out of the census' reach and left the count
/// saying eight while nine were owed.
///
/// Thirteen, and the four that took it there are the task graph's readings:
/// the count by status, the oldest untouched age, the last successful read,
/// and the read failures. The three arrived with the snapshot that survives a
/// failed read, whose point is precisely the distinction none of them is drawn
/// for; the count arrived earlier and was never drawn either. So they are
/// gaps, not exemptions -- on this census' own reading, a series a person
/// would expect to see is one nobody sees. One of the four has a derivable
/// threshold in principle (the age grows while the count holds steady); none
/// has a face. See the entries for what each is owed.
///
/// Fourteen, and the last one is the face drift loop's per-face verdict: the
/// loop publishes each face's state so that a face which drifted can be seen
/// without a person going to look, and nothing draws it -- no rule fires on a
/// drifted face and no panel lists the faces it is drifting from. The count
/// went up rather than the series being excused because the reader is exactly
/// what the loop was built to feed.
///
/// Thirteen, when that debt was paid: `governance_face_not_on_target` fires on
/// every state the enum does not call quiet, and `governance_face_reading_stopped`
/// on the family going quiet, so a drift now reaches a person without one going
/// to the exposition. The panel half of that entry is still owed, but a rule is
/// a face -- the census asks whether anything reads a name, not whether every
/// reader that was imagined got written. The entry left in the same pass as the
/// rules and the number came down with it, which is the half of the ratchet
/// that has to work or the count only ever grows.
///
/// Fourteen, and the one that took it back up was the task graph's unowned-row
/// count: the age beside it is the same family, and the age alone cannot say
/// how many rows have nobody behind them.
///
/// Back to thirteen when that one was paid: a panel draws the count per status,
/// so the name now has a reader. The debt it was raised for is not the same as
/// the one left behind -- the four other task-graph readings still have no
/// reader, and the count's own bound still has no rule -- but the census asks
/// whether anything reads a name, and now something does.
///
/// Thirteen. It stood at fifteen for two names that were published with no face
/// and, in one case, no name in any table: `cogneva_build_gate_dimension_ceiling`
/// was declared `const` rather than `pub const`, so the declaration scan never
/// saw it and only the literal scan's red brought it into view. Qualifying it
/// made it visible to the scan that asks what reads it; the two panels that came
/// after are what took both entries off this table -- one drawing the growth
/// loop's `declared` against its `peak` per claim, one drawing the four
/// dimension ceilings against the ceiling in force.
///
/// Fourteen, and the one that took it up is `cogneva_boot_phase_elapsed_seconds`:
/// timing each startup phase landed the evening this count was last held, the
/// sibling series on the same label already had a rule, and the new one was
/// rendered with nothing reading it. It was raised rather than exempted because
/// the shape of the debt was known and looked writable -- see the entry for
/// what the numbers said when someone went to write it, which is that the
/// sibling's shape does not transfer to a wall-clock quantity and the entry's
/// own claim that a threshold was derivable was wrong.
///
/// Down to four when the pin census got its panels: the four pin families and
/// the plugin population, each published as a count plus one series per member,
/// sat here as ten `Gap` entries whose reason was the same sentence in ten
/// spellings -- the face owed is a reader of the population, not a rule, and
/// there was none. Two panels pay all ten at once, because the reader those
/// entries asked for is one reader of the whole census: the counts summed into
/// a stat, the members drawn with their names in the legend. Ten came off in one
/// pass, which is the largest single step this ratchet has taken; it is also the
/// one most worth checking afterwards, since a handful of names can leave a
/// table in a single edit and nothing but the panels drawing them keeps them
/// read.
const GAPS_AT_SCRAPE_CENSUS: usize = 4;

/// How many scrape-only series are carried by a produced table and by nothing
/// that reads them.
///
/// A registration is not a reader. The two produced tables say which file
/// publishes a series, and a rule or a panel is licensed against them -- but a
/// name can sit in a table forever with nothing drawing it, which is the state
/// the dashboard contract's own comment describes for the signal watcher ("no
/// panel draws these yet ... recorded here so their names and their producer
/// stay pinned together until something does draw them"). That is a decision
/// someone made, and this number is what keeps it visible as a decision rather
/// than letting it read as a covered series.
///
/// So the three faces are not interchangeable and this is the count that says
/// so: the closed set is a face for the store, a rule or a panel is a face for a
/// reader, and a table is a face for the producer. Collapsing the three into one
/// "is it on anything" would have made these indistinguishable from a series a
/// rule watches.
///
/// Eight, and the eighth is the reason the reader set is built from query
/// expressions: `cogneva_build_target_rewrite_window_seconds` occurs in the
/// dashboard manifest exactly once, in the prose of another panel's
/// description -- "the window travels beside the count as ..." -- so a reading
/// that asks "does this name occur in the file" counts it as drawn, and a
/// reading that parses the queries does not. That is the difference between the
/// two, measured on the first run of this test.
///
/// Seventeen once, and the nine that took it there were the discovery loop's
/// per-exit counts. They arrived together because the family arrives together,
/// and they arrived registered rather than filed as gaps: a series newly on the
/// wire is owed a reader of the population, and naming its producer is what
/// keeps the nine from drifting apart while that reader is written. The reader
/// exists now -- the dashboard's "Discovery Probe Exits" panel draws the nine
/// against each other, which is the reading the re-measure recipe next to the
/// probe deadline asks for -- so they leave this count together, the way they
/// joined it. Their rows stay in the produced table, because a panel reading a
/// series is exactly what that table licenses. Eight again, and the eighth is
/// still the prose-versus-query distinction above.
///
/// Nine on 2026-10-19: `cogneva_build_gate_reservation_lapsed_total`, the
/// claims on the next build that expired unused. It was published because the
/// only trace of the mainline's 14:55 claim was a log line, and it was
/// registered rather than drawn because no threshold follows from a single
/// lapse -- the face owed was a reader of the population (how many lost turns
/// does a window hold, and for which work).
///
/// That reader exists now: the dashboard's "Build Gate: Reservations That
/// Expired Unused" panel draws the lapses over six hours, per asker and in
/// total, which is the population reading. The row stays in the produced table,
/// because a panel reading a series is exactly what that table licenses. Eight
/// again, and the eighth is still the prose-versus-query distinction above.
///
/// Seven on 2026-10-20: `cogneva_signal_watcher_running` acquired its first
/// reader. The outlet rule reads it to tell "nobody is listening to alerts"
/// from "someone in the cluster is", which is the question the family was
/// published for and the reason the dashboard comment above describes it as
/// registered ahead of a reader. The rest of the family is still counted here;
/// only the running flag left. The row also stays in the produced table.
const REGISTERED_BUT_UNREAD_AT_CENSUS: usize = 7;

#[test]
fn every_series_a_build_can_render_is_on_a_face_or_has_a_verdict() {
    let faces = faces();
    let on_a_face: BTreeSet<String> = faces
        .iter()
        .flat_map(|(_, names)| names.iter().cloned())
        .collect();
    let published = published_names();

    for (what, names) in &faces {
        assert!(
            !names.is_empty(),
            "{what} 一个名字都没读到——这张面的读法坏了（表被改名 / 文件搬走），它现在会「接受一切」或「都不接受」，两种都是假读数"
        );
    }
    assert!(
        !published.is_empty(),
        "分母是空的：两种产出形状一条都没解析出来，说明扫描坏了，这样每一步都会绿"
    );

    let excluded: BTreeSet<&str> = NOT_A_SERIES.iter().map(|(n, _)| *n).collect();
    let listed: BTreeSet<&str> = UNREGISTERED.iter().map(|(n, _)| *n).collect();

    // The gate. A name a build renders that no face carries and no verdict
    // covers is the reading this file exists to refuse: it is on the wire, and
    // everything else about it is absent.
    let unclassified: Vec<&str> = published
        .iter()
        .map(String::as_str)
        .filter(|n| !on_a_face.contains(*n) && !excluded.contains(n) && !listed.contains(n))
        .collect();
    assert!(
        unclassified.is_empty(),
        "这些序列能被渲染到抓取面，却不在任何一张面上，也没有判词——补一张面（规则 / 面板 / 登记表），或写清它为什么不需要：\n{}",
        unclassified.join("\n")
    );

    let unknown: Vec<&str> = UNREGISTERED
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| !published.contains(*n) && !excluded.contains(n))
        .collect();
    assert!(
        unknown.is_empty(),
        "UNREGISTERED 里这些名字已经不在产出面上了（改名、删除或改成别的形状后没收敛）: {unknown:?}"
    );

    let registered_but_listed: Vec<&str> = UNREGISTERED
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| on_a_face.contains(*n))
        .collect();
    assert!(
        registered_but_listed.is_empty(),
        "UNREGISTERED 里这些名字其实已经在某张面上了，登记是假的——面已落地，条目该删: {registered_but_listed:?}"
    );

    // The reason is the entry's whole value: a verdict without one is the "not
    // needed" this table exists to refuse, and a debt without one cannot be
    // acted on.
    let unreasoned: Vec<&str> = UNREGISTERED
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
        "UNREGISTERED 里这些条目没写清理由（豁免要说出那份事实被哪张面带走了，欠账要说出缺的是哪张面）: {unreasoned:?}"
    );

    // The exemption's reader is looked up, not read: a series name is either on
    // a face or it is not, so the claim cannot be made without being true.
    let unresolved: Vec<String> = UNREGISTERED
        .iter()
        .flat_map(|(name, verdict)| match verdict {
            Unread::Elsewhere { readers, .. } => unresolved_readers(&on_a_face, name, readers),
            Unread::NotAReading(_) | Unread::Gap(_) => Vec::new(),
        })
        .collect();
    assert!(
        unresolved.is_empty(),
        "UNREGISTERED 里这些豁免指向的序列不在任何一张面上，登记本身成了空头许可:\n{}",
        unresolved.join("\n")
    );

    let gaps = UNREGISTERED
        .iter()
        .filter(|(_, v)| matches!(v, Unread::Gap(_)))
        .count();
    assert_eq!(
        gaps, GAPS_AT_SCRAPE_CENSUS,
        "没有面的序列的欠账数与 GAPS_AT_SCRAPE_CENSUS 不再相等（表内 {gaps} 笔，常数 {GAPS_AT_SCRAPE_CENSUS}）。多一笔：新序列要当轮补面，或把成因写进 UNREGISTERED 并同步改这个数（改它就是承认多欠一笔）。少一笔：那是补上了面，同样要改回来——这个数就是「还有几笔没面」的账"
    );
}

/// A registration is not a reader, and the difference is counted.
///
/// The census above accepts a produced table as a face, and that is right: the
/// table is where a series' producer is pinned, and a rule or a panel is
/// licensed against it. But it does not read the series, so a name can be
/// recorded there and read by nothing -- deliberately, as the dashboard
/// contract says of the signal watcher, whose whole family exists so that "the
/// watcher has gone quiet" has a reading at all. Deliberate or not, it is the
/// shape that reads as covered while nothing covers it, so it gets its own
/// number rather than disappearing into the union of the faces.
#[test]
fn a_registration_no_rule_and_no_panel_reads_is_a_counted_row() {
    let registry: BTreeSet<String> = cog_core::metric_names::ALL
        .iter()
        .map(|n| n.as_str().to_string())
        .collect();
    let mut read: BTreeSet<String> = rule_expressions()
        .iter()
        .flat_map(|expr| metric_names_in(expr))
        .collect();
    read.extend(dashboard::series());
    let mut registered = table_names(ALERT_RULE_CONTRACT, ALERT_TABLE_OPENS);
    registered.extend(table_names(DASHBOARD_CONTRACT, DASHBOARD_TABLE_OPENS));
    let excluded: BTreeSet<&str> = NOT_A_SERIES.iter().map(|(n, _)| *n).collect();

    assert!(
        !read.is_empty(),
        "规则与面板一条都没读到，这一行数的是「有登记没读者」，读数会全靠坏掉的那一面"
    );
    assert!(!registered.is_empty(), "两张产出表一条都没读到，同上");

    // A name in the closed set is store-backed and the reader-face census
    // already judges it there; this is about the ones only a build can render.
    let published = published_names();
    let registered_unread: Vec<&str> = published
        .iter()
        .map(String::as_str)
        .filter(|n| {
            registered.contains(*n)
                && !read.contains(*n)
                && !registry.contains(*n)
                && !excluded.contains(n)
        })
        .collect();

    assert_eq!(
        registered_unread.len(),
        REGISTERED_BUT_UNREAD_AT_CENSUS,
        "有登记、没有读者的序列数与 REGISTERED_BUT_UNREAD_AT_CENSUS 不再相等（现在 {} 条，常数 {REGISTERED_BUT_UNREAD_AT_CENSUS}）:\n{}\n多一条：新登记的序列要当轮给它一个读者（规则或面板），否则它不是「已覆盖」而是「记下了却没人看」；少一条：补齐了读者，这个数要跟着减",
        registered_unread.len(),
        registered_unread.join("\n")
    );
}

/// Every series-name literal under `crates/` is accounted for.
///
/// The denominator above is read from two shapes, so a producer that spelled
/// its name a third way would be published and never checked -- a hole in the
/// gate that is invisible precisely because nothing in it is missing a verdict.
/// This closes it by walking the other way: the well-formed literals are
/// enumerated from the text with no shape assumed, and each has to be one of
/// the two shapes, a closed set name, a derived name, a name the retirement
/// list holds, a name a face reads, or a declared exception.
///
/// The face leg is what a series produced on the host needs. A bash producer
/// that runs outside this repository -- the host readings channel is the whole
/// family -- has no file here for a produced-table row to name, so the row that
/// pins a textfile producer cannot pin this one. The literal that survives is
/// in the contract table that files it as foreign, and the question the census
/// can still ask about such a name is the one it asks of every series: does
/// anything read it. That is the same read set `a_registration_no_rule_and_no_panel_reads_is_a_counted_row`
/// builds, and it is not a free pass -- a name no rule and no panel reads stays
/// unexplained, and a name filed as foreign is checked against the closed set
/// by the contract that files it.
#[test]
fn every_series_name_literal_in_the_tree_is_accounted_for() {
    let registry: BTreeSet<String> = cog_core::metric_names::ALL
        .iter()
        .map(|n| n.as_str().to_string())
        .collect();

    let mut literals = BTreeSet::new();
    for path in all_crate_files(&repo_root()) {
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
        literals.extend(series_name_literals(&text));
    }

    let published = published_names();
    let excluded: BTreeSet<&str> = NOT_A_SERIES.iter().map(|(n, _)| *n).collect();

    for (name, reason) in NOT_A_SERIES {
        assert!(
            literals.contains(*name),
            "NOT_A_SERIES 里的 {name} 在树里根本不是字面量了，豁免已过期（{reason}）"
        );
        assert!(
            reason.split_whitespace().count() >= 5,
            "NOT_A_SERIES 里的 {name} 没写清它为什么不是序列"
        );
        // One half of the exemption is checkable and this is it: a name the
        // closed set holds is a series the store writes, whatever a sentence
        // here says, so the store is the answer to "is this a series" for
        // every name it carries. What this cannot decide is a name the store
        // does not hold -- a scrape-only series put in this table would be a
        // series exempted from the census, and nothing can tell that apart
        // from a table name. That residue is written up as a memory with the
        // gate it does not have.
        assert!(
            !registry.contains(*name),
            "NOT_A_SERIES 里的 {name} 在闭集里——闭集里的名字是这个 build 会写进 store 的序列，不是标签也不是表名，把它豁免掉等于让这条序列整个在门的射程之外（{reason}）"
        );
    }

    assert!(
        !literals.is_empty(),
        "一个 cogneva_ 字面量都没扫到，扫描坏了"
    );

    let derived = |name: &str| {
        DERIVED_SUFFIXES.iter().any(|suffix| {
            name.strip_suffix(suffix)
                .is_some_and(|base| registry.contains(base))
        })
    };

    // A retired name is the third face a `cogneva_` literal can come from: the
    // retirement list the exporter and the deletion pass both read. It is read
    // from the constant rather than copied into a table here, because retiring
    // a name is how a served series' identity is changed, so that list grows --
    // and a copy would go stale in the direction that hurts, a retirement
    // landing as a red census whose fix looks like "add one line to the test".
    let retired: BTreeSet<&str> = cog_core::RETIRED_METRIC_NAMES.iter().copied().collect();
    for name in &retired {
        // The scan sees only prefixed literals, so only a prefixed retirement
        // can be asserted present: the unprefixed one is a legacy name and
        // appears here for the retirement pass, not for a producer.
        if name.contains(NAME_PREFIX) {
            assert!(
                literals.contains(*name),
                "退场的 {name} 在树里读不到字面量了——退场名单里的这条要么拼错了，要么连声明一起被删了，那样退场这件事本身就没有载体"
            );
        }
        // The half that stays dangerous: a rule or panel reading a retired name
        // reads a series this build no longer serves, and the retirement is
        // what makes it stop -- so the reading fails silently rather than
        // loudly, which is exactly the shape nothing else here would catch.
        assert!(
            !published.contains(*name),
            "退场的 {name} 还被声明为产出（仍是 `pub const ...: &str` 或 `RawMetric::new`）——退场和产出同时写着，读它的人会拿到一条不再被服务的序列"
        );
    }

    let outside = names_produced_outside_crates();
    // The names a face reads. Same two sources as the registered-but-unread
    // count, read once from the same extractors rather than twice from two
    // spellings of "what does a face read".
    let mut read_by_a_face: BTreeSet<String> = rule_expressions()
        .iter()
        .flat_map(|expr| metric_names_in(expr))
        .collect();
    read_by_a_face.extend(dashboard::series());
    let unexplained: Vec<&str> = literals
        .iter()
        .map(String::as_str)
        .filter(|n| {
            !published.contains(*n)
                && !registry.contains(*n)
                && !derived(n)
                && !excluded.contains(n)
                && !retired.contains(n)
                && !outside.contains(*n)
                && !read_by_a_face.contains(*n)
        })
        .collect();
    assert!(
        unexplained.is_empty(),
        "树里有这些 cogneva_ 字面量，既不是两种产出形状解析出的名字、也不在闭集里、也不是闭集名字的派生名、也不是产出表点名了一个真写着它的非 Rust 产出方、也没有任何规则或面板读它——产出面可能换了第三种写法，那样它就整个在门的射程之外：\n{}",
        unexplained.join("\n")
    );
}

/// Every face a name can be recorded on, as it names itself and the names it
/// carries.
fn faces() -> Vec<(&'static str, BTreeSet<String>)> {
    let registry: BTreeSet<String> = cog_core::metric_names::ALL
        .iter()
        .map(|n| n.as_str().to_string())
        .collect();
    let rules: BTreeSet<String> = rule_expressions()
        .iter()
        .flat_map(|expr| metric_names_in(expr))
        .collect();
    vec![
        ("cog_core::metric_names::ALL（闭集）", registry),
        ("chart 里的告警规则 promql", rules),
        ("Grafana 面板", dashboard::series()),
        (
            "告警规则契约测试的 PRODUCED 表",
            table_names(ALERT_RULE_CONTRACT, ALERT_TABLE_OPENS),
        ),
        (
            "面板契约测试的 PRODUCED 表",
            table_names(DASHBOARD_CONTRACT, DASHBOARD_TABLE_OPENS),
        ),
    ]
}

/// Every expression this repo ships as an alert or recording rule: the chart's,
/// then the manifests'.
///
/// The chart is not the only place a rule comes from. `deploy/k3s/` carries
/// `PrometheusRule` objects the cluster applies directly, and a rule there
/// reads a series exactly like a chart rule does. When this census read only
/// the chart, the host-credential-surface family landed in a manifest and the
/// census reported its five names as literals nothing reads -- a series that
/// is read, reported as read by nobody, which is the one direction this test
/// cannot afford to be wrong in. The alert-rule contract already scans this
/// directory; both sides have to see the same rules or each is a gate the
/// other does not know about.
///
/// Both sources are read through the one shared walk, so a document that will
/// not parse stops the census rather than leaving this face quietly smaller.
fn rule_expressions() -> Vec<String> {
    let text = read_repo(CHART_CONFIG);
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("chart config is not valid JSON");
    let mut expressions: Vec<String> = value["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("observability.infra_watch.rules is not an array")
        .iter()
        .map(|rule| rule["promql"].as_str().unwrap_or_default().to_string())
        .collect();
    expressions.extend(manifest_rule_expressions());
    assert!(
        !expressions.is_empty(),
        "一条规则表达式都没读到，规则面会是空的，而空面与「没人读」在这里同形"
    );
    expressions
}

/// Every `expr` the `PrometheusRule` manifests under the observability tree carry.
///
/// Recording rules count: "a series nothing publishes is read here" is the same
/// defect whether the rule alerts on it or records it.
fn manifest_rule_expressions() -> Vec<String> {
    service_topology::prometheus_rule_expressions(&repo_root())
}

/// The series names one produced table lists.
///
/// Read from the file as text, which is what makes the face a face: the table
/// is the record. A name is taken only where it stands alone as an entry --
/// a line that is exactly one quoted series name followed by a comma -- so a
/// comment beside an entry that quotes a name cannot add one. The table's opening line has to be there:
/// renamed or moved, this panics rather than reading an empty face.
fn table_names(rel: &str, opens: &str) -> BTreeSet<String> {
    table_entries(rel, opens)
        .into_iter()
        .filter(|(name, _)| name.starts_with(NAME_PREFIX))
        .map(|(name, _)| name)
        .collect()
}

/// The entries of one produced table, in the order the table writes them.
///
/// The alert-rule table writes `(series, producer)` pairs across two lines, so
/// the quoted entries pair up by position; anything else is a shape this
/// reader does not know how to pair and it panics rather than guessing.
fn table_entries(rel: &str, opens: &str) -> Vec<(String, String)> {
    let text = read_repo(rel);
    let start = text
        .find(opens)
        .unwrap_or_else(|| panic!("{rel} 里找不到产出表 `{opens}`，这张面读不到了"));
    let table = text[start..]
        .split_once("\n];")
        .map(|(block, _)| block)
        .unwrap_or_else(|| panic!("{rel} 里那张表没有以 `];` 收尾，读不完整"));
    let quoted: Vec<String> = table
        .lines()
        .filter_map(|line| line.trim().strip_prefix('"'))
        .filter_map(|rest| rest.strip_suffix("\","))
        .map(str::to_string)
        .collect();
    assert!(
        quoted.len() % 2 == 0,
        "{rel} 里那张表的条目数是奇数（{}），没法按「序列, 产出方」成对读",
        quoted.len()
    );
    quoted
        .chunks(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect()
}

/// The names whose producer is not Rust.
///
/// A textfile collector is a third place a series is born: the chart's sidecar
/// formats a `name value` line into a file that the node exporter serves, so
/// nothing under `crates/` declares the name and neither producer shape can see
/// it. The only place such a name appears in Rust is the produced-series table
/// row that records it -- and that row is evidence of a producer only if the
/// file it names really spells the series, which is checked here rather than
/// taken on the table's word. A row naming a file outside `crates/` that no
/// longer carries its name is a registration whose producer was renamed or
/// deleted, and it has to red here because no Rust shape will ever miss it.
///
/// Only the alert-rule table is read: its rows are all pairs, while the
/// dashboard table carries a reader list between the name and its producer.
fn names_produced_outside_crates() -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for (name, source) in table_entries(ALERT_RULE_CONTRACT, ALERT_TABLE_OPENS) {
        if source.starts_with("crates/") {
            continue;
        }
        let text = read_repo(&source);
        assert!(
            spells_series(&text, &name),
            "{name} 的产出表写着它由 `{source}` 产出，可那个文件里读不到这条名字——注册还在，产出方却改了名或被删了"
        );
        found.insert(name);
    }
    found
}

/// Whether `text` writes `name` as a series literal, in any language.
///
/// The same reader the Rust scan uses, applied to a producer that is not Rust:
/// what makes a spelling a spelling is the quote-delimited name, not the syntax
/// around it.
fn spells_series(text: &str, name: &str) -> bool {
    series_name_literals(text).iter().any(|n| n == name)
}

/// Every series name the tree publishes, from the two shapes a producer uses.
fn published_names() -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for path in all_crate_files(&repo_root()) {
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
        found.extend(declared_names(&text));
    }
    found
}

/// The names in `text` declared as `pub const ...: &str = "cogneva_..."` or
/// passed to `RawMetric::new("cogneva_...")`.
///
/// The name has to be closed on both sides by a quote and contain nothing but
/// the characters a series name uses, so the exposition fixtures a test writes
/// as text -- `"cogneva_no_such_series{node=\"a\"} 1"` -- are not read as
/// declarations. (The example names a series already listed in `NOT_A_SERIES`:
/// the literal scan below reads quoted names whatever follows them, so an
/// example naming a real series would red on its own prose.)
fn declared_names(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let needle = format!("\"{NAME_PREFIX}");
    let mut from = 0;
    while let Some(offset) = text[from..].find(&needle) {
        let quote = from + offset;
        let name_start = quote + 1;
        let name_end = text[name_start..]
            .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .map_or(text.len(), |i| name_start + i);
        let name = &text[name_start..name_end];
        if !name.is_empty() && text[name_end..].starts_with('"') && is_a_declaration(&text[..quote])
        {
            found.push(name.to_string());
        }
        from = name_end.max(quote + 1);
    }
    found
}

/// Whether the text immediately before a literal is one of the two shapes a
/// producer spells a series name with.
///
/// Read backwards from the literal: `RawMetric::new(` is the construction, and
/// `pub const <ident>: &str =` is the declaration. Nothing else counts, so a
/// name in a `let` or a test fixture is not read as a published series -- which
/// is why the literal scan above has to exist: a third shape is a hole here,
/// and there it is a red.
fn is_a_declaration(before: &str) -> bool {
    let before = before.trim_end();
    if before.ends_with("RawMetric::new(") {
        return true;
    }
    let Some(head) = before.strip_suffix(": &str =") else {
        return false;
    };
    let head = head.trim_end();
    let ident_starts = head
        .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .map_or(0, |i| i + 1);
    if ident_starts == head.len() {
        return false;
    }
    let before = head[..ident_starts].trim_end();
    before.ends_with("const")
        && before[..before.len() - "const".len()]
            .trim_end()
            .ends_with("pub")
}

/// Every well-formed series-name literal in `text`, with no shape assumed.
///
/// What the name may be followed by is not assumed either, and that is the
/// whole point of this scan: the declaration scan above reads a name closed on
/// both sides by a quote, so a producer that splices its name into a format
/// string -- `format!("cogneva_no_such_series {}\n", n)` -- spells a series
/// into the scrape with no shape any declaration reader can see. The
/// right-hand delimiter is therefore "whatever ends a name": a quote (the
/// declaration shape), a `{` (a selector, the shape an exposition fixture
/// writes), whitespace or the end of the text (a name starting a line being
/// built). The opening quote on the left is still required, so a name in prose
/// is not read as a literal unless it is quoted -- which is why the example
/// above is written with quotes and has to name a series that is already exempt.
///
/// The reach of this scan is therefore "every literal that begins with the
/// prefix", and that covers every shape a producer in this tree uses. The one
/// remaining way for a name to reach the scrape while never appearing as itself
/// -- spliced together at run time -- leaves the bare prefix in the text with
/// nothing after it, and the scan reads that as a name the population does not
/// hold. Stated as a reading rather than a proof: no file under `crates/`
/// carries `cogneva_{` today, and the unit test below pins the shape it would
/// arrive in.
fn series_name_literals(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let needle = format!("\"{NAME_PREFIX}");
    let mut from = 0;
    while let Some(offset) = text[from..].find(&needle) {
        let name_start = from + offset + 1;
        let name_end = text[name_start..]
            .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .map_or(text.len(), |i| name_start + i);
        let delimited = match text[name_end..].chars().next() {
            None => true,
            Some(c) => c == '"' || c == '{' || c.is_whitespace(),
        };
        if delimited {
            found.push(text[name_start..name_end].to_string());
        }
        from = name_end.max(name_start);
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both shapes a producer spells a name with are read, and this file cannot
    /// carry a specimen of either.
    ///
    /// The name is assembled at run time for the same reason the needle is:
    /// this file sits inside the tree the literal scan walks, so a fixture
    /// written as a literal would be found by that scan and reported as a
    /// series nothing produces.
    ///
    /// The construction shape needs this most. No producer in the tree writes
    /// `RawMetric::new` with a literal today, so without a specimen here that
    /// branch could be wrong and nothing would say so -- and the literal scan
    /// is only half a backstop for it, because a name the closed set also holds
    /// is explained by the set and never reaches the population. What the
    /// census would then miss is a series rendered by a producer whose name is
    /// also a store name: exactly the kind that appears on the scrape and in
    /// nobody's census.
    #[test]
    fn both_shapes_a_producer_spells_a_name_with_are_read() {
        let name = format!("{NAME_PREFIX}a_series_a_test_builds");

        let declared = format!("pub const SOMETHING_METRIC: &str = \"{name}\";");
        assert_eq!(
            declared_names(&declared),
            vec![name.clone()],
            "the declaration shape is read"
        );

        let constructed = format!("RawMetric::new(\"{name}\")");
        assert_eq!(
            declared_names(&constructed),
            vec![name.clone()],
            "and so is the construction shape"
        );

        // Not a third shape, and the census reads neither of these -- which is
        // what makes the literal scan necessary rather than a belt and braces.
        let rebound = format!("let name: &str = \"{name}\";");
        assert!(
            declared_names(&rebound).is_empty(),
            "a binding is not a producer, so it is not in the population"
        );

        let fixture = format!("\"{name}{{node=\\\"a\\\"}} 1\"");
        assert!(
            declared_names(&fixture).is_empty(),
            "an exposition fixture is a value being written as text, not a producer"
        );
    }

    /// A name spliced at run time leaves the bare prefix in the text, and the
    /// literal scan reads that as a name nothing produces.
    ///
    /// This is the boundary the scan is left with once the right-hand delimiter
    /// is read as "whatever ends a name": every other shape writes the whole
    /// name into the text somewhere, and a spliced one writes in as much of it
    /// as is written. That `cogneva_{` occurs in no file under `crates/` today
    /// is a reading rather than a property -- a name assembled in a way that
    /// leaves no trace would be nameless to the scan -- so the shape a splice
    /// does leave is pinned here instead of being asserted in prose. The
    /// specimen is assembled at run time for the same reason the other one is:
    /// this file sits inside the tree the scan walks.
    #[test]
    fn a_name_spliced_at_run_time_leaves_the_bare_prefix_in_the_text() {
        let spliced = format!("\"{NAME_PREFIX}{{}} 1\"");
        assert_eq!(
            series_name_literals(&spliced),
            vec![NAME_PREFIX.to_string()],
            "a spliced name is read as the bare prefix, which no population holds"
        );
    }

    /// A produced-table row licenses a literal only when the file it names
    /// really writes that name.
    ///
    /// The row is the census's only evidence that a name born outside `crates/`
    /// exists, so trusting it would turn the table into a place a typo -- or a
    /// producer deleted while its row stayed -- reads as coverage. Both halves
    /// are pinned here: the shell line that produces the series is read, and a
    /// file where the name was renamed is not.
    #[test]
    fn a_non_rust_producer_licenses_only_the_name_it_actually_writes() {
        let name = format!("{NAME_PREFIX}a_series_a_sidecar_writes");
        let produced = format!("printf '%s %s\\n' \"{name}\" \"$(value used_memory)\"");
        assert!(
            spells_series(&produced, &name),
            "a sidecar that writes the name into a textfile is a producer"
        );

        let renamed = produced.replace(&name, &format!("{name}_v2"));
        assert!(
            !spells_series(&renamed, &name),
            "a row naming a file that no longer spells the series is a registration without a producer"
        );
    }
}
