//! Gate: the works that ask the host-wide build gate for a slot are a closed set.
//!
//! `cogneva_build_gate_refused_total` carries an `asker` label and its values are
//! fixed (`cog_reflection::build_gate::BUILD_GATE_ASKERS`), because the caller's own
//! words are free text — the same words a holder writes into its slot file and a
//! refusal prints in its warn line — and a free text as a label value would grow
//! the series set with every call site. The set is what makes the label
//! admissible at all.
//!
//! The mapping is a lookup, so it cannot fail loudly on its own: a call site
//! asking under words the table does not list is counted under `unnamed` rather
//! than dropped, which is right for the total and wrong for the reading. A work
//! that quietly loses its name reads as a refusal by *some* work, and an
//! operator can no more act on it than on the wall alone — the starved work is
//! the one thing the label was added to find. Nothing else in the tree notices,
//! because both sides compile and the counter stays total.
//!
//! So this test reads the tree: every `.rs` under every `crates/*/src`, with the
//! part after the first `#[cfg(test)]` cut off — a unit test asking as `"a build
//! in the first slot"` is asking as itself and is meant to land unnamed. It also
//! reads the table the other way: an entry no call site asks under is a label
//! that promises a work nobody can be.
//!
//! What it does not decide: the words are matched, not the meaning. A call site
//! that asks with a table entry's exact words while being a different work passes
//! here, and so would two works sharing one entry. The table is one work to one
//! name by convention, and this gate can only keep the two halves — the names in
//! the code and the names in the reading — from drifting apart.
//!
//! principle: feedback_a_refusal_counter_without_labels_cannot_name_the_asker_it_starved

use std::fs;

use cog_reflection::build_gate::{BUILD_GATE_ASKERS, BUILD_GATE_ASKER_UNNAMED};

/// 人口与「哪一半是生产代码」都取自同一份普查，本文件不写第二遍。
#[path = "../../cogneva/tests/common/rust_production.rs"]
mod rust_production;

use rust_production::{production_sources, production_text};

/// The openings a call site asks through, as the text each one starts with.
///
/// The three `try_*` names are matched bare as well as under the module: the
/// cache sweep asks through the receiver (`self.try_acquire_exclusive(…)`, in
/// the trait impl that hands the gate to the sweep) rather than through the free
/// function, and both shapes are the same ask. `acquire` is matched only under
/// `build_gate::` because the name is not the gate's alone -- `AdmissionGate`
/// has an `acquire` of its own with a request name as its first argument, and a
/// bare pattern would read every model name in the eval rig as an ask.
///
/// What that leaves uncovered: a production ask written `gate.acquire("…")`
/// against the build gate's own `acquire` would be matched by nothing here. No
/// such call site exists today -- every production ask is either the qualified
/// free function or the sweep's receiver -- and the shape to add is this list.
///
/// The opening stops at the parenthesis, and the argument is read from what
/// follows, because a site may spell its word either way: as a literal, or as
/// the constant two works share. The sweep that excludes itself from every build
/// asks through the second, and a scan that only knew the first read its table
/// row as a label nobody asks under.
const ASKING_CALLS: [&str; 4] = [
    "build_gate::acquire(",
    "try_acquire(",
    "try_acquire_exclusive(",
    "reserve_build_field(",
];

/// The words the works outside this tree ask under.
///
/// The gate has a second client on the host -- the operator repo's
/// `scripts/host/cogneva-build.sh` -- which takes a slot through the same files
/// rather than through this crate. Its ask is as real as any call site here and
/// its refusals land in the same counter, so its words belong on the table; but
/// no `.rs` file in this workspace spells them, and the orphan check below would
/// read its cell as a label promising a work that cannot exist.
///
/// What this does not do: it cannot see the script, so the words here are a copy
/// of what the script claims under. The script's own repository pins the copy --
/// a check there reads `build_what` out of the script and fails when it is not a
/// word this table lists -- and that check is what makes the copy stay true.
const OUTSIDE_TREE_ASKS: [&str; 1] = ["host build"];

/// The workspace root, from the one algorithm the whole tree shares.
#[path = "../../cogneva/tests/common/root.rs"]
mod root;

use root::repo_root;

/// One ask site: the argument it spells, and the word that argument means.
struct Ask {
    /// What stands at the call site: a literal's contents, or a constant's name.
    argument: String,
    /// The word the ask is counted under. `None` when the argument is a name the
    /// scan could not resolve to one word -- which is a site it cannot read, and
    /// reading nothing as "named" is the failure this whole check exists to stop.
    words: Option<String>,
}

/// The `const NAME: &str = "…";` declarations in `text`, as the two halves.
///
/// A word two works must agree on is a constant in this tree, so an ask may be
/// spelled by naming that constant. Following the declaration is what keeps the
/// gate's other half -- no row on the table without a call site asking under it
/// -- from calling such a row orphaned: the call site asks, it just does not
/// spell the word twice.
///
/// Read as text, like everything else here: a `const … = "…";` inside a string
/// literal counts. A name declared twice with the same word is one word; twice
/// with different words is no answer at all, and the caller records it that way.
fn constant_words(text: &str) -> Vec<(&str, &str)> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("const ") {
        let after = &rest[at + "const ".len()..];
        let name_len = name_prefix(after);
        if name_len == 0 {
            rest = after;
            continue;
        }
        let (name, tail) = after.split_at(name_len);
        let Some(decl) = tail.trim_start().strip_prefix(": &str = \"") else {
            rest = tail;
            continue;
        };
        let Some(end) = decl.find('"') else {
            break;
        };
        out.push((name, &decl[..end]));
        rest = &decl[end..];
    }
    out
}

/// The bytes of an identifier at the front of `text`, up to the first one that
/// cannot be in it.
fn name_prefix(text: &str) -> usize {
    text.bytes()
        .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
        .count()
}

/// The word a constant name means, from every declaration the tree holds.
///
/// `None` covers both "no such constant" and "two declarations disagree". They
/// are one answer here because they are one failure at the call site: the scan
/// cannot say which word the ask carries, so it must not say "some row".
fn resolved(
    name: &str,
    spellings: &std::collections::BTreeMap<String, Option<String>>,
) -> Option<String> {
    spellings.get(name).and_then(|words| words.clone())
}

/// Whether an argument name is the shape a constant has.
///
/// Rust spells constants in SCREAMING_SNAKE_CASE, and that convention is what
/// lets this scan tell the two kinds of name apart without a type checker: a
/// lower-case name in argument position is a parameter being handed on -- the
/// words were decided where it was supplied, and counting the forwarder as an
/// ask would report every gate method that takes a `what`.
fn constant_shaped(name: &str) -> bool {
    name.bytes().any(|b| b.is_ascii_uppercase())
        && name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// The ask sites in `text`, each with the word it asks under when that is
/// readable.
///
/// `text` is the production half only: the asks that count are the ones a
/// running process makes, and the test modules ask as themselves. Which bytes
/// those are is decided by [`production_text`], not here.
///
/// `spellings` is the tree's constant declarations, gathered by the caller from
/// the same production halves -- so a constant declared in the file that asks
/// and one imported from elsewhere are read the same way.
///
/// Three shapes are asks: a literal, and a constant-shaped name that either
/// resolves to a word or is reported as unread so a reader sees it. Two are not,
/// and are skipped rather than reported: an argument written `name:` is a
/// function's own parameter list (the openings match declarations too), and a
/// lower-case name or a longer expression is plumbing that hands on words
/// decided elsewhere. What that leaves uncovered: a word carried in a plain
/// variable from the outer call site in, since neither end of that chain spells
/// it. No such site exists today; the shape to add is this paragraph.
fn asking_words(
    text: &str,
    spellings: &std::collections::BTreeMap<String, Option<String>>,
) -> Vec<Ask> {
    let mut out = Vec::new();
    for call in ASKING_CALLS {
        let mut rest = text;
        while let Some(at) = rest.find(call) {
            let after = rest[at + call.len()..].trim_start();
            if let Some(literal) = after.strip_prefix('"') {
                let Some(end) = literal.find('"') else {
                    break;
                };
                out.push(Ask {
                    argument: literal[..end].to_string(),
                    words: Some(literal[..end].to_string()),
                });
                rest = &literal[end..];
                continue;
            }
            let name_len = name_prefix(after);
            if name_len == 0 {
                rest = after;
                continue;
            }
            let (name, tail) = after.split_at(name_len);
            // `try_acquire(self: &Arc<Self>, what: &str)` is the declaration of
            // the method this scan is looking for calls to, and its first
            // argument is not an ask.
            if tail.trim_start().starts_with(':') {
                rest = tail;
                continue;
            }
            let just_the_name = tail.trim_start().starts_with([',', ')']);
            if just_the_name && constant_shaped(name) {
                out.push(Ask {
                    argument: name.to_string(),
                    words: resolved(name, spellings),
                });
            }
            rest = tail;
        }
    }
    out
}

#[test]
fn every_ask_the_build_gate_refuses_lands_in_a_named_cell() {
    // The denominator is the census's own: production_sources refuses a tree it
    // read almost none of, so a walk that silently found nothing cannot reach
    // the assertions below.
    let files = production_sources(&repo_root());

    let named: Vec<&str> = BUILD_GATE_ASKERS.iter().map(|(words, _)| *words).collect();
    assert!(
        !named.contains(&BUILD_GATE_ASKER_UNNAMED),
        "unnamed 是「不在表上」那一格的标签值，不能同时是某一行的提问词"
    );

    // The constants the tree declares, before any file is read for asks: a call
    // site may ask through one declared in another file, and the answer is the
    // same either way. Two declarations of one name that disagree are recorded
    // as no answer rather than as the last one read.
    let mut spellings: std::collections::BTreeMap<String, Option<String>> =
        std::collections::BTreeMap::new();
    for path in &files {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        for (name, words) in constant_words(&production_text(&text)) {
            match spellings.entry(name.to_string()) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(Some(words.to_string()));
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    if slot.get().as_deref() != Some(words) {
                        slot.insert(None);
                    }
                }
            }
        }
    }

    let mut unnamed_sites: Vec<String> = Vec::new();
    let mut asked: Vec<String> = Vec::new();
    for path in &files {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        for ask in asking_words(&production_text(&text), &spellings) {
            match &ask.words {
                Some(words) if named.contains(&words.as_str()) => asked.push(words.clone()),
                Some(words) => unnamed_sites.push(format!("{}: {words:?}", path.display())),
                None => unnamed_sites.push(format!(
                    "{}: {} 这个提问词读不出来（既不是字面量，也不是一个两边都同意的常量）",
                    path.display(),
                    ask.argument
                )),
            }
        }
    }

    // An ask site exists, or this test is reading a tree it does not describe.
    assert!(
        !asked.is_empty(),
        "一个生产提问点都没找到：调用点的写法变了，这条判据已经不覆盖任何东西"
    );

    assert!(
        unnamed_sites.is_empty(),
        "这些生产提问点用的词不在 BUILD_GATE_ASKERS 表上，它们的拒绝会落进 unnamed 那一格，\
         读数里看不出是谁被饿死:\n{}",
        unnamed_sites.join("\n")
    );

    // The declaration of outside-tree asks is only worth anything if the words
    // are on the table: a typo here would leave the real row looking orphaned
    // (caught below) or, worse, quietly excuse nothing.
    let misdeclared: Vec<&str> = OUTSIDE_TREE_ASKS
        .iter()
        .copied()
        .filter(|words| !named.contains(words))
        .collect();
    assert!(
        misdeclared.is_empty(),
        "这些声明为树外提问的词不在 BUILD_GATE_ASKERS 表上，声明等于没有:\n{}",
        misdeclared.join("\n")
    );

    // The other direction: a row no call site asks under is a label promising a
    // work that cannot exist, and it would sit at zero forever reading as a work
    // that simply was never refused.
    let orphans: Vec<&str> = BUILD_GATE_ASKERS
        .iter()
        .map(|(words, _)| *words)
        .filter(|words| !asked.iter().any(|asked| asked == words))
        .filter(|words| !OUTSIDE_TREE_ASKS.contains(words))
        .collect();
    assert!(
        orphans.is_empty(),
        "BUILD_GATE_ASKERS 里这些行的提问词没有任何生产调用点在用，那一格会永远读 0:\n{}",
        orphans.join("\n")
    );
}

/// The second spelling an ask may use: a constant the tree declares once and two
/// works name.
///
/// The tree's own sweep is the specimen: it asks through
/// `BUILD_CACHE_RECLAIM_ASKER` because the gate's rule about it and the slot it
/// takes have to agree on one word. A scan that only knew literals read the
/// sweep's row as a label no call site asks under -- an orphan -- while the ask
/// was there all along.
#[test]
fn an_ask_spelled_by_a_constant_reads_as_the_word_that_constant_means() {
    let text = r#"
pub const SWEEP_ASKER: &str = "build-cache-reclaim";
fn take(&self) {
    let _ = self.0.try_acquire_exclusive(SWEEP_ASKER).await;
}
"#;
    let mut spellings = std::collections::BTreeMap::new();
    for (name, words) in constant_words(text) {
        spellings.insert(name.to_string(), Some(words.to_string()));
    }
    let asks = asking_words(text, &spellings);
    assert_eq!(asks.len(), 1, "one call site, one ask");
    assert_eq!(asks[0].argument, "SWEEP_ASKER");
    assert_eq!(asks[0].words.as_deref(), Some("build-cache-reclaim"));
}

/// And the half that keeps the reading honest: a name the tree cannot resolve --
/// never declared, or declared twice with different words -- is not "a site that
/// asks something". It is a site the scan cannot read, and the check has to say
/// so rather than let it count as named.
#[test]
fn an_ask_through_a_name_with_no_single_word_reads_as_no_word() {
    let text = r#"
fn take(&self) {
    let _ = self.0.try_acquire_exclusive(UNDECLARED_ASKER).await;
    let _ = self.0.try_acquire("a literal still reads").await;
}
"#;
    let spellings = std::collections::BTreeMap::new();
    let asks = asking_words(text, &spellings);
    assert_eq!(asks.len(), 2, "both spellings are ask sites");
    let by_name = |argument: &str| {
        asks.iter()
            .find(|ask| ask.argument == argument)
            .unwrap_or_else(|| panic!("{argument} is a site the scan did not read"))
    };
    assert_eq!(
        by_name("UNDECLARED_ASKER").words,
        None,
        "a name nobody spells is not a word"
    );
    assert_eq!(
        by_name("a literal still reads").words.as_deref(),
        Some("a literal still reads")
    );
}

/// The two shapes that are not asks, so a reader can see they were decided
/// rather than missed.
///
/// The openings match declarations as well as calls -- `try_acquire(self: &Arc<Self>, …)`
/// is the method being asked -- and a lower-case argument is a parameter the
/// words were handed to (`gate.try_acquire(what)`). Reporting either as an ask
/// under no name would turn the gate red on plumbing, which is the same reading
/// as red on a real site: a check that fires on everything is read as nothing.
#[test]
fn a_declaration_and_a_handed_on_word_are_not_ask_sites() {
    let text = r#"
pub async fn try_acquire(self: &Arc<Self>, what: &str) -> SFResult<BuildPermit> {
    gate.try_acquire(what).await
}
fn forward(&self, what: &str) -> bool {
    self.reserve_build_field(what)
}
"#;
    let found: Vec<String> = asking_words(text, &std::collections::BTreeMap::new())
        .into_iter()
        .map(|ask| ask.argument)
        .collect();
    assert!(
        found.is_empty(),
        "neither a parameter list nor a pass-through asks under words: {found:?}"
    );
}
