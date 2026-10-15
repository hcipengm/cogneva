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
//! set name, or one of two declared non-series. A producer that spells its name
//! a third way lands there as an unexplained literal rather than as a series
//! nothing checks.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

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
/// them as a producer. Two, and each is a different kind of exception: one is
/// not a series at all, the other is a series the tree publishes on purpose
/// without any build being able to write it.
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
    // The pin census's four families, published as the reading surface for a
    // family whose only other surface was an `info!` line. Each family is a
    // count that always answers plus one series per member. The module says in
    // as many words that no threshold belongs on them -- a pin published for a
    // reader in another process lands in the same family as one with no reader
    // anywhere, and an optional read is expected to go unsatisfied -- so what
    // is owed is a reader of the population, not a rule. What makes it a debt
    // rather than a decision is the module's own history: the family grew to
    // thirty-one pins with no reading ever disagreeing, which is a size nobody
    // was watching.
    (
        "cogneva_plugin_pin_unsatisfied",
        Unread::Gap("one series per plugin pin whose read nothing satisfies, carrying the pin, its publisher and its reader. No threshold is derivable on it -- the module says a member is not by itself a fault -- so the face owed is a reader of the population rather than a rule, and there is none: this family exists because it grew to thirty-one pins with nobody noticing"),
    ),
    (
        "cogneva_plugin_pin_unsatisfied_count",
        Unread::Gap("how many pins are in that state, published so that a family of zero and an absent family read differently -- a count derivable with `count()` is still published on its own because only its presence separates an empty family from an unwired one. It is the denominator of the family above and carries no verdict of its own, so it is owed the reader that family is owed"),
    ),
    (
        "cogneva_plugin_pin_unconsumed",
        Unread::Gap("one series per plugin pin published with nothing consuming it. The parties of a member are a label rather than a series each, so a pin with two publishers stays one pin -- which means this series is the only place the population is visible, and no face carries it"),
    ),
    (
        "cogneva_plugin_pin_unconsumed_count",
        Unread::Gap("how many pins nothing consumes, as the always-answering half of the family above. The count alone stays the same when the population changes underneath it and the members alone make an empty family and an unwired reading look alike, which is why both halves are published; the pair is carried by no face"),
    ),
    (
        "cogneva_plugin_pin_multi_published",
        Unread::Gap("one series per plugin pin published by more than one party. The count of parties is not derivable from the other families -- a pin with two publishers is one member here and one member of the unconsumed family -- so nothing else states how wide the duplication is"),
    ),
    (
        "cogneva_plugin_pin_multi_published_count",
        Unread::Gap("how many pins are published more than once, as the always-answering half of the family above. The module's point is that this is the reading that says the wiring is doing something a single publisher would not; no rule, panel or table names it"),
    ),
    (
        "cogneva_plugin_pin_unordered_init_read",
        Unread::Gap("one series per read that happens at init with no declared order between it and the publish it depends on, so the publisher's layer alone decides whether the read is in time. Nothing carries the population, and a member here is a finding the startup log prints one line per at warn -- a reader is owed"),
    ),
    (
        "cogneva_plugin_pin_unordered_init_read_count",
        Unread::Gap("how many such reads a missing `requires` edge left unordered, as the always-answering half of the family above. The first reading of this family is also what found its own defect: the number was one larger than the series any query could count, because a read registered twice at init is one series on the exposition. That half is closed now -- the members are collapsed where the series are built, so the size and the series agree -- and what remains owed is the reader nothing has given it"),
    ),
    // Two gauges on the build gate, each published because the series beside it
    // cannot state its fact. Both are read by nobody.
    (
        "cogneva_build_gate_occupied",
        Unread::Gap("slot files locked right now in the shared directory, so every builder on the host counted rather than this process alone. The doc says the two gauges exist for the difference between them -- one process admitting up to its bound while another builds beside it reads exactly like one process admitting up to its bound -- and no face reads this one, so whether the host is carrying one build or two is still unreadable everywhere"),
    ),
    (
        "cogneva_build_gate_excluded",
        Unread::Gap("1 while a holder that is not a build holds every slot to keep builds out. It is its own series because it answers the question a refusal raises -- did a build take the slot or something else -- and every other reading in the family answers that wrongly, counting a cache sweep as a build. No face reads it, so a refusal it caused still points at a build that does not exist"),
    ),
    // The ownership pair: the gauge and the probe-failure counter beside them
    // are read, and neither states what either of these counts.
    (
        "cogneva_loop_owner_acquisitions_total",
        Unread::Gap("times this process took the loop's role, including every term won back after losing one. The holder gauge is read and cannot state it: a first acquisition and a takeover leave the same pair of gauge values, and only this counter says the work moved to someone"),
    ),
    (
        "cogneva_loop_owner_losses_total",
        Unread::Gap("times this process held the role and then stopped. A loss is a takeover seen from the losing side and is not the winner's acquisition -- the loser has stopped doing work it was doing, and everything it had published is last-known from that moment. It is counted where it happened because the loser may be the only process that can say so, and no face reads it"),
    ),
    (
        "cogneva_orphans_reaped_total",
        Unread::Gap("orphans this process has collected since it started, published beside the gauge of zombies still waiting. The gauge's rule fires on a zombie that stays, which is a reaper that has stopped; this counter is the other half -- that reaping is happening at all -- and no face reads it. What a rule would need is a bound on how many collections are ordinary, and collections follow git fetches, so the missing half is a rate nobody has declared"),
    ),
    (
        "cogneva_host_slice_file_bytes",
        Unread::Gap("page cache and other file-backed memory held under one host slice. The rule on the neighbouring anon cell reads a floor and a flatness over fifteen minutes, and this cell is published separately on purpose -- a slice's memory.current is anon plus file plus a kernel remainder, so folding them would hide which of the two moved. No face reads it, so a slice whose pressure is page cache and one whose pressure is a process read the same on the rule that exists"),
    ),
    (
        "cogneva_stream_pending_count",
        Unread::Gap("entries pending right now on one consumed stream, measured on its own cadence because a sweep that blocks would otherwise freeze the reading. The rules on this family read the claim idle time, the measure cadence and the age of the oldest unreclaimed entry, and none of them states the size of the population: a stream holding one message and one holding a thousand read alike"),
    ),
    (
        "cogneva_stream_read_failures_total",
        Unread::Gap("reads this consumer's loop has had refused since it started, published as the deliberate pair to the silence reading -- silence says whether the bus is moving, this says what it cost to keep up. The stalled-consumer rule's own summary already tells a human to look at this series for the count of refused reads, so the reading is addressed to a reader and the face that would carry it does not exist"),
    ),
    (
        "cogneva_runtime_asset_check_seconds",
        Unread::Gap("when the startup check last ran, published as a separate series for the reason the module gives: only the problem states are exported, so a check that found nothing and a check that never ran leave the same reading, and this timestamp is what separates them. The four rules read the state gauges and none of them reads it, so the staleness it exists to make visible is visible to nobody"),
    ),
    // Two names whose fact a face already carries, each looked up rather than
    // asserted in prose.
    (
        "cogneva_stream_pending_unreclaimed_count",
        Unread::Elsewhere {
            readers: &["cogneva_stream_pending_unreclaimed_oldest_idle_seconds"],
            reason: "the rule reads the age of the longest-outstanding entry past the reclaim threshold, which is the maximum of exactly the population this counts",
        },
    ),
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
];

/// How many `Gap` entries this census left.
///
/// The first run is where this number comes from, and it is meant to be read as
/// it is: seventeen names the tree publishes into a scrape with no face
/// carrying them. The ratchet is the same one the reader-face census keeps --
/// a new series nothing reads must be classified deliberately, and calling it a
/// gap raises this number on purpose. It comes down when a face is written, and
/// that is a change to this constant plus the removal of the entry.
const GAPS_AT_SCRAPE_CENSUS: usize = 17;

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
const REGISTERED_BUT_UNREAD_AT_CENSUS: usize = 8;

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
/// the two shapes, a closed set name, a derived name, or a declared exception.
#[test]
fn every_series_name_literal_in_the_tree_is_accounted_for() {
    let registry: BTreeSet<String> = cog_core::metric_names::ALL
        .iter()
        .map(|n| n.as_str().to_string())
        .collect();

    let mut literals = BTreeSet::new();
    for path in rust_sources_under_crates() {
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

    let unexplained: Vec<&str> = literals
        .iter()
        .map(String::as_str)
        .filter(|n| {
            !published.contains(*n)
                && !registry.contains(*n)
                && !derived(n)
                && !excluded.contains(n)
        })
        .collect();
    assert!(
        unexplained.is_empty(),
        "树里有这些 cogneva_ 字面量，既不是两种产出形状解析出的名字、也不在闭集里、也不是闭集名字的派生名——产出面可能换了第三种写法，那样它就整个在门的射程之外：\n{}",
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

/// Every `promql` the chart's alert rules carry, in file order.
///
/// Read through the same path the alert-rule contract reads, so a rule one side
/// sees is a rule both sides see. Every `promql` field in the document lives
/// under this path -- checked when this census was written, 156 of 156 -- so
/// there is no second set of expressions to miss.
fn rule_expressions() -> Vec<String> {
    let text = read(CHART_CONFIG);
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("chart config is not valid JSON");
    value["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("observability.infra_watch.rules is not an array")
        .iter()
        .map(|rule| rule["promql"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// The series names one produced table lists.
///
/// Read from the file as text, which is what makes the face a face: the table
/// is the record. A name is taken only where it stands alone as an entry --
/// a line that is exactly one quoted series name followed by a comma -- so a
/// comment beside an entry that quotes a name cannot add one. The table's opening line has to be there:
/// renamed or moved, this panics rather than reading an empty face.
fn table_names(rel: &str, opens: &str) -> BTreeSet<String> {
    let text = read(rel);
    let start = text
        .find(opens)
        .unwrap_or_else(|| panic!("{rel} 里找不到产出表 `{opens}`，这张面读不到了"));
    let table = text[start..]
        .split_once("\n];")
        .map(|(block, _)| block)
        .unwrap_or_else(|| panic!("{rel} 里那张表没有以 `];` 收尾，读不完整"));
    table
        .lines()
        .filter_map(|line| line.trim().strip_prefix('"'))
        .filter_map(|rest| rest.strip_suffix("\","))
        .filter(|name| name.starts_with(NAME_PREFIX))
        .map(str::to_string)
        .collect()
}

/// Every series name the tree publishes, from the two shapes a producer uses.
fn published_names() -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for path in rust_sources_under_crates() {
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
/// as text -- `"cogneva_x{node=\"a\"} 1"` -- are not read as declarations.
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
fn series_name_literals(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let needle = format!("\"{NAME_PREFIX}");
    let mut from = 0;
    while let Some(offset) = text[from..].find(&needle) {
        let name_start = from + offset + 1;
        let name_end = text[name_start..]
            .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .map_or(text.len(), |i| name_start + i);
        if text[name_end..].starts_with('"') {
            found.push(text[name_start..name_end].to_string());
        }
        from = name_end.max(name_start);
    }
    found
}

/// Every `.rs` under `crates/`, as a path.
fn rust_sources_under_crates() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => walk(&path, out),
                Ok(t) if t.is_file() && path.extension().is_some_and(|e| e == "rs") => {
                    out.push(path)
                }
                _ => {}
            }
        }
    }
    let mut found = Vec::new();
    walk(&repo_root().join("crates"), &mut found);
    found.sort();
    found
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
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
}
