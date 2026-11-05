//! principle: feedback_read_the_authors_stated_reason_before_calling_it_a_defect
//!
//! Gate: a shape-based defect claim is only a claim once the reason written at the
//! site it names has been read, and a claim that never read it is refused for the
//! read it skipped rather than for the defect it alleges.
//!
//! On 2026-10-13 `dispatch_pass` was called a defect from its shape. From the
//! outside the three readings it takes -- `deliveries`, `commits`, `alerts` -- read
//! as state filtered out and thrown away. The reason was written directly above
//! each parameter, one `//` block per reading saying what `None` means there; it
//! went unread. The shape was right about the code and wrong about the reason, and
//! the reason was at the site, where the next reader would look for it.
//!
//! Nothing in the compiler stops that: the finding compiles, the patch it asks for
//! compiles, and the only trace is a change spent on behaviour that was deliberate
//! and already explained. So this gate reads the production source, not a model of
//! it: the definition is located by name, the three readings are parsed out of its
//! parameter block, and each has to still carry the comment that says what its
//! `None` means. A reading whose comment is gone is not a defect in the reading --
//! it is a claim nobody can check any more, and that is what the assertion says.
//!
//! Rounds 1 and 2 of this gate had the defect it exists for, and they are named
//! here so the rule is seen applied to itself: a path the tree does not carry
//! (`crates/cog-orchestrator/src/dag_executor/orchestrator.rs`), and a window above
//! call sites whose population on this tree is empty -- both judged a shape without
//! reading it. The needles below are the ones the file actually writes: the docs
//! are `//` lines saying "`None` when", not `///` lines saying "`None` means".

// The repository root is published once for the whole tree, and the two
// projections of production source with it; this test mounts those rather than a
// copy of each.
#[path = "../../cogneva/tests/common/root.rs"]
mod root;
#[path = "../../cogneva/tests/common/rust_production.rs"]
mod rust_production;

use root::repo_root;
use rust_production::production_lines_keeping_comments;

/// The file the 2026-10-13 finding named.
const SITE: &str = "crates/cog-orchestrator/src/change_ledger.rs";

/// The definition, so the block below it is the one the reason is written in.
const DEFINITION: &str = "async fn dispatch_pass(";

/// The three readings the finding misread, by the name they are declared under. A
/// rename is a can't-see failure, not a pass: the gate would then judge two
/// parameters and call the tree clean.
const MISREAD: [&str; 3] = ["deliveries:", "commits:", "alerts:"];

/// The stated reason for one parameter: the run of `//` lines directly above the
/// line declaring it, if that run says what `None` means.
///
/// Contiguous by construction, not by a window: a reason written over a different
/// parameter is not this parameter's reason, and a distance chosen here would be a
/// guess about formatting rather than a reading of the site.
fn stated_reason<'a>(block: &[&'a str], parameter: &str) -> Option<&'a str> {
    let at = block
        .iter()
        .position(|line| line.trim_start().starts_with(parameter))?;
    block[..at]
        .iter()
        .rev()
        .take_while(|line| line.trim_start().starts_with("//"))
        .find(|line| line.contains("`None`"))
        .copied()
}

/// The parameter block of the definition: the lines between `async fn dispatch_pass(`
/// and the line that closes it.
fn parameter_block<'a>(lines: &[&'a str]) -> Option<Vec<&'a str>> {
    let start = lines.iter().position(|line| line.contains(DEFINITION))?;
    let end = lines[start..]
        .iter()
        .position(|line| line.trim_start().starts_with(") ->"))?;
    Some(lines[start + 1..start + end].to_vec())
}

/// The gate over the tree: each of the three readings the finding misread still
/// carries the reason that says what its `None` means.
#[test]
fn every_misread_reading_still_carries_the_reason_that_explains_it() {
    let text =
        std::fs::read_to_string(repo_root().join(SITE)).unwrap_or_else(|e| panic!("{SITE}: {e}"));
    let lines = production_lines_keeping_comments(&text);
    let Some(block) = parameter_block(&lines) else {
        panic!("{SITE}: `{DEFINITION}` has no parameter block this scan can read");
    };
    let unexplained: Vec<&str> = MISREAD
        .into_iter()
        .filter(|parameter| stated_reason(&block, parameter).is_none())
        .collect();
    assert!(
        unexplained.is_empty(),
        "{SITE}: {unexplained:?} carry no `//` line above them saying what `None` means. A \
         finding raised against one of them would be a claim about its shape with no evidence \
         that the reason written there was read -- read it first, or it is not a finding"
    );
}

/// A scan that stops recognising the site would report the tree as clean. The gate
/// has to say when it cannot see what it judges, so the definition and all three
/// parameter lines are asserted found rather than passed over.
#[test]
fn the_scan_finds_the_definition_and_all_three_readings() {
    let text =
        std::fs::read_to_string(repo_root().join(SITE)).unwrap_or_else(|e| panic!("{SITE}: {e}"));
    let lines = production_lines_keeping_comments(&text);
    assert!(
        lines.iter().any(|line| line.contains(DEFINITION)),
        "{SITE}: `{DEFINITION}` was not found, so this gate judged nothing -- a name that was \
         renamed or moved reads exactly like a tree with no offender"
    );
    let Some(block) = parameter_block(&lines) else {
        panic!("{SITE}: the parameter block of `{DEFINITION}` is not readable");
    };
    for parameter in MISREAD {
        assert!(
            block
                .iter()
                .any(|line| line.trim_start().starts_with(parameter)),
            "{SITE}: the parameter {parameter} was not found in the block, so the reason this \
             gate looks for is not the reason at this site"
        );
    }
}

/// The reader measures the reason at the parameter, not the presence of the word in
/// the file: a predicate that matched any occurrence would stay green through the
/// defect it exists for. Both directions, plus a hoisted comment, on text shaped
/// the way the tree writes it.
#[test]
fn the_reader_reports_the_reason_at_the_parameter_and_not_elsewhere() {
    let documented = [
        "    // The commits in that same window: `None` when the tree could not say.",
        "    commits: Option<CommitWindow>,",
        "    // The firing alerts this pass could read.",
        "    alerts: Option<&[PersistedAlert]>,",
    ];
    assert_eq!(stated_reason(&documented, "commits:"), Some(documented[0]));
    assert!(stated_reason(&documented, "alerts:").is_none());

    // The comment moved over a different parameter: it is then that parameter's
    // reason, and the one it was written for is unexplained.
    let hoisted = [
        "    // `None` when the tree could not say.",
        "    commits: Option<CommitWindow>,",
        "    deliveries: Option<&HashMap<String, String>>,",
    ];
    assert!(stated_reason(&hoisted, "deliveries:").is_none());

    // A parameter no line declares is not silently explained.
    assert!(stated_reason(&documented, "carried:").is_none());
}
