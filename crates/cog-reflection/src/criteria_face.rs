//! Which gates a change has to clear, decided from what the change touches.
//!
//! The expensive gate here is the evaluation run: it is the only one that costs
//! model calls, and the thing it alone can catch is a change that moves the
//! criteria the evaluation is scored against — a change that rewrites the eval
//! itself, or the thresholds and gate points the rest of the system judges by.
//! Nothing else needs it. So the question this module answers is narrow and
//! mechanical: *does this change touch the criteria face*.
//!
//! # What the criteria face is
//!
//! Three things, each mechanically enumerable:
//!
//! 1. **The criteria carriers.** Paths whose contents *are* a judgment: the
//!    gate scripts, the CI workflows, the integration tests, the benchmarks.
//!    Enumerated by pattern, never by a hand-written list — a list would go
//!    stale the moment someone adds a carrier, and the failure would be silent
//!    and in the unsafe direction.
//! 2. **The keys those carriers read.** A configuration key a gate reads is
//!    part of the face even though the gate's file did not change: moving the
//!    threshold a gate compares against moves the gate. Read out of the
//!    carriers' own text, so a new carrier brings its keys with it.
//! 3. **The gate points a change removes.** A gate can be taken out of the
//!    production path without touching any carrier — deleting the call that
//!    ran it. Only removals are read; weakening (`>=` to `>`, negating a
//!    condition) has no line-level signature and is left to the real gate to
//!    judge on content.
//!
//! # Why the face is walked, not baked in
//!
//! The carriers' *paths* are not compiled into the binary. A change that adds
//! `deploy/scripts/check-something.sh` adds a carrier, and a list built at
//! compile time would not contain it — so the change that most needs the gate
//! would be the one routed past it. So a path is tested against the patterns
//! directly, which catches a carrier the change adds before it exists on disk;
//! and the tokens are read out of the checkout the change is being applied to,
//! which is also the only revision that stays correct when the running binary
//! and the checkout are not the same commit.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cog_core::{DiffShape, DiffTarget};

/// Paths whose contents are a judgment rather than a product.
///
/// A pattern list, not a file list: it says what *kind* of thing is a carrier,
/// so a carrier added tomorrow is one without anyone editing this. The cost is
/// that a path matching no pattern is not a carrier, which is why the patterns
/// are deliberately wider than today's tree — a false carrier costs an extra
/// gate run, a missed one costs the gate.
pub const CARRIER_PATTERNS: &[&str] = &[
    "deploy/scripts/check-*.sh",
    "deploy/scripts/tests/**",
    ".github/workflows/*.yml",
    ".github/workflows/*.yaml",
    "crates/*/tests/**",
    "crates/*/benches/**",
    "crates/*/src/**/*_test.rs",
];

/// Directories that never hold a carrier, skipped so the walk stays cheap.
const IGNORED_DIRS: &[&str] = &["target", ".git", "node_modules", ".venv"];

/// Topology: the deployment's own shape, judged by the parity gate rather than
/// by compilation or by the evaluation run.
pub const TOPOLOGY_PREFIXES: &[&str] = &[
    "deploy/helm/",
    "deploy/k3s/",
    "deploy/rendered/",
    "deploy/scripts/render-deploy.sh",
];

/// The face a change is measured against.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CriteriaFace {
    /// Repo-relative paths on disk that are criteria carriers, sorted.
    ///
    /// Read to tell "a line was removed from inside a carrier" from "a line was
    /// removed elsewhere": a removal's file is always a file that already
    /// existed, so it is one the walk saw. Whether a path the *diff* names is a
    /// carrier is answered by pattern instead, since a carrier the change adds
    /// is not on disk yet.
    pub carriers: BTreeSet<String>,
    /// Identifiers the carriers' own text names. Used to recognize a
    /// configuration key a gate reads.
    pub criterion_tokens: BTreeSet<String>,
}

/// Which gates a change has to clear, weakest to strongest.
///
/// Ordered so that [`Ord::max`] is the merge rule: a change touching several
/// rows gets the strongest one, never the cheapest. A reader that takes the
/// minimum "to avoid slowing the build down" is the failure this ordering
/// exists to make impossible to express by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    /// Nothing runs: the change touches only prose.
    Minimal,
    /// The change has to compile.
    Compile,
    /// The existing tests have to pass.
    Tests,
    /// A structural gate applies (topology parity; the security allow-list).
    Structural,
    /// The evaluation gate has to run: this change can move a criterion.
    RealGate,
}

impl Tier {
    /// The label this tier is counted under.
    ///
    /// A closed set rather than a per-change string: the reading's point is to
    /// compare cells against each other, and a free-form label would let the
    /// cells drift into one per change.
    pub fn as_cell(self) -> &'static str {
        match self {
            Tier::Minimal => "minimal",
            Tier::Compile => "compile",
            Tier::Tests => "tests",
            Tier::Structural => "structural",
            Tier::RealGate => "real_gate",
        }
    }

    /// Every tier this build can return, weakest first.
    ///
    /// Published as a full cross product so that "this tier never fired" and
    /// "this reading was never wired up" stay different readings: both leave a
    /// series that is absent otherwise, and only the second is a defect.
    pub const ALL: [Tier; 5] = [
        Tier::Minimal,
        Tier::Compile,
        Tier::Tests,
        Tier::Structural,
        Tier::RealGate,
    ];
}

/// Why a tier was reached. Carried so the real gate does not have to re-derive
/// the trigger, and so a reading can say which rows fire in production.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierReason {
    /// Every path the change touches is prose.
    ProseOnly,
    /// A configuration value no criterion reads.
    ConfigValue,
    /// Source that is not a criterion and not a gate point.
    Code,
    /// A path that is itself a criteria carrier.
    CriteriaCarrier,
    /// A configuration key some carrier reads.
    CriteriaKey,
    /// A line a gate point used was removed.
    GatePointRemoved,
    /// A path the deployment's topology is made of.
    Topology,
}

/// The decision, with the rows that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tiering {
    pub tier: Tier,
    pub reasons: Vec<TierReason>,
    /// Whether the change moves the criteria *code* itself, as opposed to a key
    /// or a gate point it reads. This is the self-verification case: the gate
    /// that would judge the change is the one the change rewrote, so the
    /// verdict has to come from somewhere else.
    pub touches_criteria_code: bool,
}

/// Decide which gates a change has to clear.
///
/// Reads the change's path set, the configuration keys it moves, and the lines
/// it removes — and the criteria face those are measured against. Nothing here
/// consults the model, the network, or whether the change is *good*; a change
/// that touches the criteria face gets the real gate whether or not it seems
/// harmless, because "seems harmless" is the one reading an eval that was just
/// loosened is guaranteed to give.
pub fn tier(face: &CriteriaFace, targets: &[DiffTarget], shape: &DiffShape) -> Tiering {
    let paths: Vec<&str> = targets.iter().map(|t| t.path.as_str()).collect();
    let mut reasons = Vec::new();
    let mut tier = Tier::Minimal;
    let mut touches_criteria_code = false;

    if paths.is_empty() {
        return Tiering {
            tier: Tier::Minimal,
            reasons,
            touches_criteria_code,
        };
    }

    let mut hit = |reason: TierReason, at: Tier, criteria_code: bool| {
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
        if criteria_code {
            touches_criteria_code = true;
        }
        if at > tier {
            tier = at;
        }
    };

    if paths.iter().all(|p| cog_core::is_prose_path(p)) {
        hit(TierReason::ProseOnly, Tier::Minimal, false);
    }

    if shape.touches_config() {
        let moves_a_read_key = shape
            .changed_config_keys
            .iter()
            .any(|k| face.criterion_tokens.contains(k));
        if moves_a_read_key {
            // A configuration change no criterion reads is a structural dot on
            // a map; one a criterion reads is a new rule, and the eval is the
            // only thing that can say whether the rule is the one that was
            // meant.
            hit(TierReason::CriteriaKey, Tier::RealGate, false);
        } else {
            hit(TierReason::ConfigValue, Tier::Structural, false);
        }
    }

    for path in &paths {
        let normalized = path.replace('\\', "/");
        if is_carrier_path(&normalized) {
            hit(TierReason::CriteriaCarrier, Tier::RealGate, true);
        } else if !cog_core::is_prose_path(&normalized)
            && !cog_core::is_config_path(&normalized)
            && !is_topology(&normalized)
        {
            // Configuration is excluded: a configuration value is a structural
            // dot on the map, not source, and routing it to the test tier would
            // make every knob change look like a code change.
            hit(TierReason::Code, Tier::Tests, false);
        }
        if is_topology(&normalized) {
            hit(TierReason::Topology, Tier::Structural, false);
        }
    }

    // A gate point taken out of a file that is not itself a carrier: the
    // removal has to name something a criterion reads, or every deleted comment
    // would trip this. Weakening a comparison has no line-level signature at
    // all and is not read here — it is the real gate's job to judge on content.
    let removed_inside_a_carrier = shape
        .removed_lines
        .keys()
        .any(|path| face.carriers.contains(path));
    if !removed_inside_a_carrier {
        // Only names that did not come back: a rewritten line is an edit, and
        // counting its old copy would make every change a gate point removal.
        'files: for tokens in shape.net_removed_tokens().values() {
            for token in tokens {
                if face.criterion_tokens.contains(token) {
                    hit(TierReason::GatePointRemoved, Tier::RealGate, false);
                    break 'files;
                }
            }
        }
    }

    Tiering {
        tier,
        reasons,
        touches_criteria_code,
    }
}

/// Whether a path is a criteria carrier, decided by its kind.
///
/// Answered from the pattern list rather than from the walked face, because the
/// tier is decided before the change is applied: a change that *adds* a carrier
/// has a path in its diff that is not on disk yet, and a face lookup would miss
/// exactly the change that most needs the gate. The pattern list is the same one
/// the walk uses, so the two never disagree about what kind of thing a path is.
pub fn is_carrier_path(path: &str) -> bool {
    CARRIER_PATTERNS.iter().any(|p| glob_match(path, p))
}

/// Whether a path is part of the deployment's own topology.
pub fn is_topology(path: &str) -> bool {
    TOPOLOGY_PREFIXES.iter().any(|p| path.starts_with(p))
}

/// Walk `root` and read the criteria face out of it.
///
/// Best-effort by design: a path that cannot be read is left out of the face
/// rather than failing the walk, because the caller is already inside a change
/// it cannot abandon — but the faces it does read are read from the checkout
/// being changed, which is the only revision that can be trusted here.
pub fn criteria_face(root: &Path) -> CriteriaFace {
    let mut face = CriteriaFace::default();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if IGNORED_DIRS.contains(&name.as_ref()) {
                    continue;
                }
                stack.push(path);
                continue;
            }
            let Ok(relative) = path.strip_prefix(root) else {
                continue;
            };
            let relative = relative.to_string_lossy().replace('\\', "/");
            if !CARRIER_PATTERNS.iter().any(|p| glob_match(&relative, p)) {
                continue;
            }
            face.carriers.insert(relative.clone());
            if let Ok(text) = std::fs::read_to_string(root.join(&relative)) {
                face.criterion_tokens.extend(tokens_in(&text));
            }
        }
    }
    face
}

/// Identifiers a text names that could be a configuration key.
///
/// A key read by a gate is spelled the same way in the gate and in the document
/// it is set in, and both spellings are `snake_case`. Reading only identifiers
/// that contain an underscore keeps this from matching every English word in a
/// comment, and the cost of the ones it still matches is an extra gate run —
/// never a missed one.
fn tokens_in(text: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            current.push(ch);
            continue;
        }
        if current.len() >= 4 && current.contains('_') && !current.starts_with('_') {
            tokens.insert(std::mem::take(&mut current));
        }
        current.clear();
    }
    if current.len() >= 4 && current.contains('_') && !current.starts_with('_') {
        tokens.insert(current);
    }
    tokens
}

/// Whether a repo-relative path matches a glob pattern.
///
/// Supports `*` within a segment and `**` across segments, which is the whole
/// of what [`CARRIER_PATTERNS`] needs. A pattern this does not understand
/// matches nothing, so a typo in the list costs carriers rather than inventing
/// them — the direction that shows up as a change routed *past* a gate is the
/// one to avoid, and this is the other one.
fn glob_match(path: &str, pattern: &str) -> bool {
    let path: Vec<&str> = path.split('/').collect();
    let pattern: Vec<&str> = pattern.split('/').collect();
    match_segments(&path, &pattern)
}

fn match_segments(path: &[&str], pattern: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((&"**", rest)) => {
            // `**` matches zero or more segments.
            if match_segments(path, rest) {
                return true;
            }
            match path.split_first() {
                Some((_, tail)) => match_segments(tail, pattern),
                None => false,
            }
        }
        Some((segment, rest)) => match path.split_first() {
            Some((head, tail)) => segment_match(segment, head) && match_segments(tail, rest),
            None => false,
        },
    }
}

fn segment_match(pattern: &str, segment: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == segment,
        Some((prefix, suffix)) => {
            segment.len() >= prefix.len() + suffix.len()
                && segment.starts_with(prefix)
                && segment.ends_with(suffix)
        }
    }
}

/// A path relative to `root`, for callers holding an absolute one.
pub fn relative(root: &Path, path: &Path) -> Option<PathBuf> {
    path.strip_prefix(root).ok().map(Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{diff_shape, DiffTargetKind};
    use std::fs;

    fn target(path: &str) -> DiffTarget {
        DiffTarget {
            path: path.to_string(),
            kind: DiffTargetKind::Modify,
        }
    }

    fn face(carriers: &[&str], tokens: &[&str]) -> CriteriaFace {
        CriteriaFace {
            carriers: carriers.iter().map(|s| s.to_string()).collect(),
            criterion_tokens: tokens.iter().map(|s| s.to_string()).collect(),
        }
    }

    const CONFIG_DIFF: &str = "\
--- a/config/cogneva.json
+++ b/config/cogneva.json
@@ -1,2 +1,2 @@
-        \"extraction_input_budget_tokens\": 4096,
+        \"extraction_input_budget_tokens\": 8192,
";

    #[test]
    fn a_prose_only_change_runs_nothing() {
        let t = tier(
            &face(&[], &[]),
            &[target("docs/README.md")],
            &DiffShape::default(),
        );
        assert_eq!(t.tier, Tier::Minimal);
        assert_eq!(t.reasons, vec![TierReason::ProseOnly]);
    }

    #[test]
    fn source_that_is_not_a_criterion_has_to_compile_and_pass_tests() {
        let t = tier(
            &face(&["deploy/scripts/check-x.sh"], &[]),
            &[target("crates/cog-core/src/lib.rs")],
            &DiffShape::default(),
        );
        assert_eq!(t.tier, Tier::Tests);
        assert!(!t.touches_criteria_code);
    }

    #[test]
    fn touching_a_carrier_takes_the_real_gate() {
        let t = tier(
            &face(&["deploy/scripts/check-x.sh"], &[]),
            &[target("deploy/scripts/check-x.sh")],
            &DiffShape::default(),
        );
        assert_eq!(t.tier, Tier::RealGate);
        assert!(t.touches_criteria_code);
    }

    #[test]
    fn a_carrier_the_change_itself_adds_takes_the_real_gate() {
        // The tier is decided before the change is applied, so a newly added
        // carrier is not in the walked face — and a face lookup is exactly the
        // miss that would route the change which introduces a gate *past* it.
        let t = tier(
            &face(&[], &[]),
            &[target("deploy/scripts/check-brand-new.sh")],
            &DiffShape::default(),
        );
        assert_eq!(t.tier, Tier::RealGate);
        assert_eq!(t.reasons, vec![TierReason::CriteriaCarrier]);
        assert!(t.touches_criteria_code);
    }

    #[test]
    fn a_non_source_carrier_path_still_takes_the_real_gate() {
        // The correction this guards: a `check-*.sh` has no `.rs` extension, so
        // a rule that read only source extensions would route the change that
        // edits a gate *past* the gate.
        let t = tier(
            &face(&[".github/workflows/ci.yml"], &[]),
            &[target(".github/workflows/ci.yml")],
            &DiffShape::default(),
        );
        assert_eq!(t.tier, Tier::RealGate);
        assert!(t.touches_criteria_code);
    }

    #[test]
    fn merging_rows_takes_the_strongest_not_the_cheapest() {
        let t = tier(
            &face(&["crates/cog-core/tests/gate.rs"], &[]),
            &[
                target("docs/guide.md"),
                target("crates/cog-core/src/lib.rs"),
                target("crates/cog-core/tests/gate.rs"),
            ],
            &DiffShape::default(),
        );
        assert_eq!(t.tier, Tier::RealGate);
        assert_eq!(
            t.reasons,
            vec![TierReason::Code, TierReason::CriteriaCarrier]
        );
    }

    #[test]
    fn a_config_key_a_criterion_reads_takes_the_real_gate() {
        let shape = diff_shape(CONFIG_DIFF);
        assert!(shape
            .changed_config_keys
            .contains("extraction_input_budget_tokens"));
        let t = tier(
            &face(
                &["deploy/scripts/check-x.sh"],
                &["extraction_input_budget_tokens"],
            ),
            &[target("config/cogneva.json")],
            &shape,
        );
        assert_eq!(t.tier, Tier::RealGate);
        assert_eq!(t.reasons, vec![TierReason::CriteriaKey]);
        // The eval was not rewritten, only a value it reads was moved, so this
        // is not the self-verification case.
        assert!(!t.touches_criteria_code);
    }

    #[test]
    fn a_config_key_no_criterion_reads_is_only_structural() {
        let shape = diff_shape(CONFIG_DIFF);
        let t = tier(
            &face(&["deploy/scripts/check-x.sh"], &["some_other_key"]),
            &[target("config/cogneva.json")],
            &shape,
        );
        assert_eq!(t.tier, Tier::Structural);
        assert_eq!(t.reasons, vec![TierReason::ConfigValue]);
    }

    #[test]
    fn deleting_the_only_use_of_a_key_a_criterion_reads_takes_the_real_gate() {
        let diff = "\
--- a/crates/cog-reflection/src/promotion_gate.rs
+++ b/crates/cog-reflection/src/promotion_gate.rs
@@ -1,2 +1,1 @@
-    let cap = policy.max_diff_lines;
     let mode = policy.mode;
";
        let shape = diff_shape(diff);
        assert!(shape.removes_lines());
        let t = tier(
            &face(&["deploy/scripts/check-x.sh"], &["max_diff_lines"]),
            &[target("crates/cog-reflection/src/promotion_gate.rs")],
            &shape,
        );
        assert_eq!(t.tier, Tier::RealGate);
        assert_eq!(
            t.reasons,
            vec![TierReason::Code, TierReason::GatePointRemoved]
        );
    }

    #[test]
    fn rewriting_a_value_in_place_is_not_a_removed_gate_point() {
        // The commonest change there is: one line out, a different one in. The
        // name survived, so nothing was taken out of the gate.
        let shape = diff_shape(CONFIG_DIFF);
        assert!(shape.removes_lines());
        let t = tier(
            &face(
                &["deploy/scripts/check-x.sh"],
                &["extraction_input_budget_tokens"],
            ),
            &[target("config/cogneva.json")],
            &shape,
        );
        assert!(!t.reasons.contains(&TierReason::GatePointRemoved));
    }

    #[test]
    fn a_removed_name_the_criteria_and_the_change_do_not_share_is_not_a_gate_point() {
        let diff = "\
--- a/crates/cog-reflection/src/promotion_gate.rs
+++ b/crates/cog-reflection/src/promotion_gate.rs
@@ -1,2 +1,1 @@
-    let elapsed = some_unrelated_clock();
     let mode = policy.mode;
";
        let t = tier(
            &face(&["deploy/scripts/check-x.sh"], &["max_diff_lines"]),
            &[target("crates/cog-reflection/src/promotion_gate.rs")],
            &diff_shape(diff),
        );
        assert!(!t.reasons.contains(&TierReason::GatePointRemoved));
    }

    #[test]
    fn topology_is_structural() {
        let t = tier(
            &face(&[], &[]),
            &[target("deploy/k3s/cogneva-json-configmap.yaml")],
            &DiffShape::default(),
        );
        assert_eq!(t.tier, Tier::Structural);
        assert_eq!(t.reasons, vec![TierReason::Topology]);
    }

    #[test]
    fn globs_match_the_patterns_the_face_is_built_from() {
        assert!(glob_match(
            "deploy/scripts/check-deploy-parity.sh",
            "deploy/scripts/check-*.sh"
        ));
        assert!(glob_match(
            ".github/workflows/ci.yml",
            ".github/workflows/*.yml"
        ));
        assert!(glob_match(
            "crates/cog-core/tests/gate.rs",
            "crates/*/tests/**"
        ));
        assert!(glob_match(
            "crates/cog-core/src/a/b_test.rs",
            "crates/*/src/**/*_test.rs"
        ));
        assert!(!glob_match(
            "crates/cog-core/src/lib.rs",
            "crates/*/tests/**"
        ));
        assert!(!glob_match(
            "docs/workflows/ci.yml",
            ".github/workflows/*.yml"
        ));
    }

    #[test]
    fn the_walk_finds_carriers_by_kind_and_not_by_a_list() {
        let root = tempfile::tempdir().unwrap();
        let write = |rel: &str, body: &str| {
            let path = root.path().join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, body).unwrap();
        };
        write("deploy/scripts/check-new.sh", "grep -q max_diff_lines\n");
        write("crates/cog-core/tests/gate.rs", "fn g() {}\n");
        write("crates/cog-core/src/lib.rs", "pub fn nope() {}\n");
        write("target/junk/check-should-not-be-found.sh", "x\n");

        let face = criteria_face(root.path());
        assert!(face.carriers.contains("deploy/scripts/check-new.sh"));
        assert!(face.carriers.contains("crates/cog-core/tests/gate.rs"));
        assert!(!face.carriers.contains("crates/cog-core/src/lib.rs"));
        assert!(!face
            .carriers
            .contains("target/junk/check-should-not-be-found.sh"));
        assert!(face.criterion_tokens.contains("max_diff_lines"));
    }

    #[test]
    fn config_keys_are_read_only_from_config_files() {
        let shape = diff_shape(CONFIG_DIFF);
        assert!(shape.touches_config());
        assert!(shape.config_files.contains("config/cogneva.json"));
        // The key token is the leaf, not the dotted path: a diff carries no
        // indentation context to rebuild the path from.
        assert!(shape
            .changed_config_keys
            .contains("extraction_input_budget_tokens"));
        let prose = diff_shape(
            "--- a/docs/notes.md\n+++ b/docs/notes.md\n@@ -1 +1 @@\n-old\n+new_key_here: 1\n",
        );
        assert!(!prose.touches_config());
        assert!(prose.changed_config_keys.is_empty());
    }

    #[test]
    fn removal_lines_are_recorded_per_file_with_the_marker_stripped() {
        let diff = "\
--- a/a.rs
+++ b/a.rs
@@ -1,2 +1,1 @@
-let x = 1;
 let y = 2;
";
        let shape = diff_shape(diff);
        assert_eq!(
            shape.removed_lines.get("a.rs").map(Vec::as_slice),
            Some(["let x = 1;".to_string()].as_slice())
        );
        assert_eq!(shape.removed_line_count(), 1);
    }
}
