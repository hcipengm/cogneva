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
use std::path::{Path, PathBuf};

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
const ASKING_CALLS: [&str; 4] = [
    "build_gate::acquire(\"",
    "try_acquire(\"",
    "try_acquire_exclusive(\"",
    "reserve_build_field(\"",
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

/// The workspace root: the crates directory's parent.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cog-reflection")
        .to_path_buf()
}

/// The words each production call site in `text` asks under.
///
/// `text` is the production half only: the asks that count are the ones a
/// running process makes, and the test modules ask as themselves. Which bytes
/// those are is decided by [`production_text`], not here.
fn asking_words(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    for call in ASKING_CALLS {
        let mut rest = text;
        while let Some(at) = rest.find(call) {
            let after = &rest[at + call.len()..];
            let Some(end) = after.find('"') else {
                break;
            };
            out.push(&after[..end]);
            rest = &after[end..];
        }
    }
    out
}

#[test]
fn every_ask_the_build_gate_refuses_lands_in_a_named_cell() {
    // The denominator is the census's own: production_sources refuses a tree it
    // read almost none of, so a walk that silently found nothing cannot reach
    // the assertions below.
    let files = production_sources(&workspace_root());

    let named: Vec<&str> = BUILD_GATE_ASKERS.iter().map(|(words, _)| *words).collect();
    assert!(
        !named.contains(&BUILD_GATE_ASKER_UNNAMED),
        "unnamed 是「不在表上」那一格的标签值，不能同时是某一行的提问词"
    );

    let mut unnamed_sites: Vec<String> = Vec::new();
    let mut asked: Vec<String> = Vec::new();
    for path in &files {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        for words in asking_words(&production_text(&text)) {
            if !named.contains(&words) {
                unnamed_sites.push(format!("{}: {words:?}", path.display()));
            }
            asked.push(words.to_string());
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
