//! How the executor arms the host-wide build gate (`cog_core::build_gate`)
//! from the configuration it was handed.
//!
//! The gate itself — slot files, flocks, counters — lives in
//! `cog_core::build_gate`. What lives here is the decision about the *numbers*,
//! because the one mis-set number that keeps hurting is the wait budget: with a
//! bound of a single slot, a queued one-shot build (a change verification has no
//! next cycle to come back to) waits behind a holder whose own work is bounded
//! by `build_timeout_secs`. A wait budget shorter than that ceiling refuses the
//! queued build *before the holder is even out of bounds*: the build did not
//! fail, it never ran, and the refusal counter climbs on a host that was never
//! saturated. That is exactly what the `build_gate_refused` alert was reporting
//! — the budget was set below the longest legal hold.
//!
//! [`crate::build_gate::effective_config`] therefore floors a nonzero wait
//! budget at the build timeout plus a release margin. Deriving the floor from
//! `build_timeout_secs` (rather than raising a constant) keeps the two knobs
//! consistent forever: raising the build timeout automatically raises the wait
//! floor, so the misconfiguration cannot silently come back. An explicit
//! `wait_secs = 0` keeps its documented meaning — try once, no queue — and is
//! left alone.
//!
//! [`effective_gate_for`] is where a process applies it, and it is applied
//! once per process: every reader of the budget — the gate that refuses, the
//! wall a flight in progress is judged against, the wall a cycle's queue is
//! planned against — reads the same derivation. Two numbers here would mean a
//! build refused at a wall the readings do not show, which is the shape of the
//! defect the floor exists to remove.

use cog_core::config::BuildGateConfig;
use cog_core::SelfEvolutionConfig;

/// Extra seconds past the build timeout that a waiter is given, so a holder
/// that runs right up to its bound still has time to release before the waiter
/// is refused.
///
/// The margin is the part of the handover the build timeout cannot cover: a
/// holder killed at its own bound still has to drop the slot, and a waiter that
/// arrived before the holder took it has to survive that. It cannot be derived
/// from the timeout -- how long a killed holder takes to let go is a different
/// quantity -- but it is not measured here either. Nothing on this deployment
/// publishes the interval between a holder reaching its own deadline and its
/// slot becoming free, so this number is a name for the delay rather than a
/// reading of it, and it is the last frozen number on this path. What is known
/// about the delay on this deployment, measured 2026-10-12: the longest single
/// hold is 1424s (`cogneva_build_gate_held_ms_max`) against a 3600s build
/// timeout, and 3 builds had been refused at the then-armed 1800s.
///
/// What the floor this margin produces does and does not cover, because the
/// difference is a whole class of refusals: `build_timeout_secs + margin`
/// covers a waiter queued behind *one* holder running to the full of its right.
/// It does not cover a queue of several such holders -- with a bound of one, N
/// holders that each stay inside their right can still refuse the waiter behind
/// them, and every one of those holders was legal. That is not hypothetical:
/// `cogneva_build_gate_wait_ms_max` reached 1693s against a longest hold of
/// 1424s, so waits on this host already outlive any single hold. A refusal of
/// that kind is a capacity reading, not a misconfiguration, and telling the two
/// apart requires the refusal to say which wall it hit -- which the counter
/// does not carry yet.
///
/// The floor also tracks one field and the slot is held under two: a build is
/// killed at `build_timeout_secs` and a change's verification run is killed at
/// `test_timeout_secs`, and both take a slot in this same gate (the
/// verification's own hold is named `change verification`). The two are equal on
/// this deployment -- 3600s each -- so the floor covers both today, and an
/// operator who raised the test timeout alone would move the longest legal hold
/// past the floor without touching the number that answers for it.
pub const WAIT_RELEASE_MARGIN_SECS: u64 = 600;

/// The gate configuration to arm for a self-evolution block, and the one number
/// every reader of the budget must take.
///
/// Takes the whole block rather than the two fields because the two fields are
/// what must not drift apart: a caller that fetched `wait_secs` and
/// `build_timeout_secs` separately could floor against one and arm the other,
/// and the pair is only meaningful together.
pub fn effective_gate_for(self_evolution: &SelfEvolutionConfig) -> BuildGateConfig {
    effective_config(
        &self_evolution.build_gate,
        self_evolution.build_timeout_secs,
    )
}

/// The gate configuration this process should actually arm, given the build
/// timeout its builds are killed at.
///
/// A nonzero wait budget shorter than `build_timeout_secs + margin` is raised
/// to that floor; everything else is passed through unchanged, so a deliberately
/// generous operator setting is never lowered and a deliberate zero stays zero.
pub fn effective_config(cfg: &BuildGateConfig, build_timeout_secs: u64) -> BuildGateConfig {
    let mut out = cfg.clone();
    let floor = build_timeout_secs.saturating_add(WAIT_RELEASE_MARGIN_SECS);
    if out.wait_secs > 0 && out.wait_secs < floor {
        out.wait_secs = floor;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A whole self-evolution block around one gate config, so the pair the
    /// derivation reads comes from one place here too.
    fn self_evolution(
        build_gate: BuildGateConfig,
        build_timeout_secs: u64,
    ) -> cog_core::SelfEvolutionConfig {
        cog_core::SelfEvolutionConfig {
            build_timeout_secs,
            build_gate,
            ..Default::default()
        }
    }

    fn cfg(wait_secs: u64) -> BuildGateConfig {
        BuildGateConfig {
            enabled: true,
            max_concurrent: 1,
            wait_secs,
            lock_dir: "/tmp/build-gate-test".into(),
        }
    }

    /// The regression the alert reported: a default 1800s budget against a
    /// 3600s build timeout refuses every build queued behind a legal
    /// full-length holder. The armed gate must wait at least as long as the
    /// longest legal hold, plus the release margin.
    #[test]
    fn a_budget_below_the_longest_legal_hold_is_raised_to_cover_it() {
        let out = effective_config(&cfg(1800), 3600);
        assert_eq!(out.wait_secs, 3600 + WAIT_RELEASE_MARGIN_SECS);
        // The concurrency bound and the slot directory are the host's own
        // properties; flooring the budget must not touch them.
        assert_eq!(out.max_concurrent, 1);
        assert_eq!(out.lock_dir, "/tmp/build-gate-test");
    }

    /// A budget already past the floor is the operator's own answer and is
    /// kept as-is: the floor exists to stop refusals of legal waits, not to
    /// cap patience.
    #[test]
    fn a_budget_past_the_floor_is_kept() {
        let out = effective_config(&cfg(7200), 3600);
        assert_eq!(out.wait_secs, 7200);
    }

    /// Zero means "try once, never queue"; flooring it into a queue would
    /// silently change what the operator asked for.
    #[test]
    fn an_explicit_no_queue_budget_is_kept() {
        let out = effective_config(&cfg(0), 3600);
        assert_eq!(out.wait_secs, 0);
    }

    /// The floor follows the build timeout: raising the timeout raises the
    /// floor, so the two knobs cannot drift back into budget < longest hold.
    #[test]
    fn the_floor_tracks_the_build_timeout() {
        assert_eq!(
            effective_config(&cfg(100), 7200).wait_secs,
            7200 + WAIT_RELEASE_MARGIN_SECS
        );
        assert_eq!(
            effective_config(&cfg(100), 60).wait_secs,
            60 + WAIT_RELEASE_MARGIN_SECS
        );
    }

    /// The two numbers come out of one block, so a caller cannot floor against
    /// one of them and arm the other.
    #[test]
    fn the_gate_is_derived_from_the_block_that_holds_both_numbers() {
        let configured = self_evolution(cfg(1800), 3600);
        assert_eq!(
            effective_gate_for(&configured).wait_secs,
            3600 + WAIT_RELEASE_MARGIN_SECS
        );
        // Everything that is not the budget is the operator's own setting and is
        // passed through: the floor is about one number, not about the gate.
        let widened = self_evolution(
            BuildGateConfig {
                max_concurrent: 3,
                ..cfg(1800)
            },
            3600,
        );
        assert_eq!(effective_gate_for(&widened).max_concurrent, 3);
    }

    /// The executor arms the derivation, and no production reader in this crate
    /// takes the document's own budget.
    ///
    /// Read off the sources rather than asserted about the arithmetic, because
    /// the defect this replaces was not a wrong number: the derivation was
    /// correct and nothing called it, so the gate waited the document's 1800s
    /// while a legal hold was 3600s and builds were refused for it. A test of
    /// the function's own arithmetic passes while the wiring is dead, and that
    /// is exactly what had been happening.
    ///
    /// The crate, and not the one file an earlier version of this test looked
    /// at: the readers of the budget are the executor's wiring, the wall a
    /// flight in progress is judged against, and the wall a cycle's queue is
    /// planned against -- a check that names one file stops covering the
    /// readers added to the others, and saying nothing about them is not the
    /// same as saying they are fine.
    #[test]
    fn no_reader_in_this_crate_takes_the_documents_own_wait_budget() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect_sources(&src, &mut files);
        // A walk that read nothing must fail rather than pass: "no reader found"
        // and "no file read" are the same words for two different facts, and
        // only one of them is an answer.
        assert!(
            files.len() > 40,
            "the scan read {} source files under {}; that is not this crate",
            files.len(),
            src.display()
        );
        let mut arms = 0;
        for path in &files {
            let text = std::fs::read_to_string(path).expect("a source file to be readable");
            let production = production_part(&text);
            for at in occurrences(production, ".wait_secs") {
                // The receiver, read as an identifier: the field of the
                // configuration block is the defect, and `build_gate_cfg` --
                // the name the derived value is bound to -- is the answer.
                let before = &production[..at];
                let start = before
                    .char_indices()
                    .rev()
                    .take_while(|(_, ch)| ch.is_alphanumeric() || *ch == '_')
                    .last()
                    .map(|(index, _)| index)
                    .unwrap_or(before.len());
                assert_ne!(
                    &before[start..],
                    "build_gate",
                    "{} takes the configuration block's own wait budget; every reader has to \
                     take the derived one, or a build is refused at a wall the readings do \
                     not show",
                    path.display()
                );
            }
            if production.contains("effective_gate_for(") {
                arms += 1;
            }
        }
        assert!(
            arms > 0,
            "nothing in this crate derives the gate config it arms -- the floor is dead code \
             again"
        );
    }

    /// Every `.rs` file under `dir`, subdirectories included.
    fn collect_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// A file's text with its test module removed.
    ///
    /// Test code is not what this test is about, and it is where the document's
    /// own number is written down in a fixture -- left in, the check would
    /// refuse the very regression test that proves it works. Cut at the last
    /// top-level `#[cfg(test)]` rather than the first: the attribute is also
    /// used on single items (a `cfg(test)`-only import, a setter), and cutting
    /// at one of those would quietly drop production code from the scan.
    fn production_part(text: &str) -> &str {
        match text.rfind("\n#[cfg(test)]") {
            Some(at) => &text[..at],
            None => text,
        }
    }

    /// Where `needle` occurs in `text`.
    fn occurrences(text: &str, needle: &str) -> Vec<usize> {
        let mut out = Vec::new();
        let mut from = 0;
        while let Some(at) = text[from..].find(needle) {
            let at = from + at;
            out.push(at);
            from = at + needle.len();
        }
        out
    }
}
