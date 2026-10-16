//! Deciding whether anything enforces a principle, from the registries alone.
//!
//! Two registries answer, and neither of them is prose. The host side's column
//! arrives with the reading: the host generates it from its own table of checks,
//! so a principle with a gate there is one a hook actually runs. The platform
//! side's column is read out of *this* tree -- a contract test's header states
//! which principle it enforces -- so a principle with a gate here is one a test
//! that runs in CI covers. A sentence saying "we check this" is not a gate and
//! never counted as one; that is the whole reason this module reads registries
//! and not documents.
//!
//! The look has three answers, and the third is why it exists. A principle can
//! have a gate, have none (a debt, and the debt is the point), or be one the look
//! cannot settle. The ways it cannot be settled are a closed set -- the platform's
//! registry could not be read, no revision could be resolved, the host's column
//! never arrived, the host's own judgement names a carrier its tree no longer
//! holds, the registry holds a declaration whose slug cannot be read -- and none
//! of them is written down as `unenforced`. A look that could not tell, reported
//! as "no gate", is the same failure that made the host's own projection treat a
//! whole unread column as a clean roster.
//!
//! Nothing here writes. Every answer goes back to the loop that owns the account,
//! which is the only writer and has exactly one entry point to move a state.

use std::collections::{BTreeMap, HashSet};

use cog_core::{PrincipleDrift, PrincipleOrphan, PrincipleRow};

/// What the host's generator writes in the host column when its registry was
/// read and holds nothing for this principle.
///
/// A value rather than an empty column, and that is the whole reason the third
/// answer is sayable at all: `None` has to keep meaning "no reading has ever
/// arrived", so "a reading arrived and there is no gate" needs a spelling of its
/// own. The host's own projection writes this one, and the two agree by
/// construction rather than by both having been written by hand.
pub const HOST_REGISTRY_READ_NO_GATE: &str = "-";

/// What the host's generator prefixes a judgement with when the memory it names
/// is no longer in its tree.
///
/// A judgement whose rule left the tree is not a gate any more -- nothing runs it
/// and it covers nothing -- so this column value must not be read as one. It is
/// also not "no gate": whether some other check enforces the principle is now
/// unknown to the host side, which is a different fact and a different answer.
pub const HOST_JUDGEMENT_STALE_PREFIX: &str = "stale:";

/// The shape of a platform-side declaration, as a line of the platform's own
/// source.
///
/// Re-exported from the contract because a second reader reads the same shape
/// from the other direction -- a run that failed, rather than the registry of
/// what enforces what -- and one value keeps the two from drifting apart. Every
/// reference below this line is to that one definition.
pub use cog_core::PLATFORM_DECLARATION_PREFIX;

/// The expression the platform's registry is read with: the prefix at the start
/// of a line, which is where a module header sits and where prose about the
/// convention does not.
pub const PLATFORM_DECLARATION_PATTERN: &str = "^//! principle:";

/// Which files may declare. Rust sources, because a declaration is a header of
/// the test module that enforces it, and the tree's prose about the convention
/// lives outside them.
pub const PLATFORM_REGISTRY_FILES: &str = "*.rs";

/// Why a principle's gate could not be settled, as a closed set.
///
/// Closed because every one of them is reported as a count on every pass, and a
/// count keyed by free text makes each distinct wording its own series. The five
/// have five different owners: the platform's registry, the tree the pass reads,
/// the host's generator, the host's own tree, and whoever wrote a declaration
/// with a slug nothing can key on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnsettledReason {
    /// The platform's own registry could not be read out of the tree.
    RegistryUnreadable,
    /// No revision to judge against could be resolved.
    RevisionUnreadable,
    /// No reading has ever carried this principle, so the host's column says
    /// nothing about it -- which is not the same as the host having no gate.
    HostColumnAbsent,
    /// The host's own judgement names a memory its tree no longer holds.
    CarrierNotInTree,
    /// The registry holds a declaration whose slug cannot be read, so a
    /// principle with no gate found may still be one this declaration names.
    DeclarationUnparsable,
}

impl UnsettledReason {
    /// Every reason, for a reader that has to enumerate them.
    pub const ALL: [UnsettledReason; 5] = [
        UnsettledReason::RegistryUnreadable,
        UnsettledReason::RevisionUnreadable,
        UnsettledReason::HostColumnAbsent,
        UnsettledReason::CarrierNotInTree,
        UnsettledReason::DeclarationUnparsable,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            UnsettledReason::RegistryUnreadable => "registry_unreadable",
            UnsettledReason::RevisionUnreadable => "revision_unreadable",
            UnsettledReason::HostColumnAbsent => "host_column_absent",
            UnsettledReason::CarrierNotInTree => "carrier_not_in_tree",
            UnsettledReason::DeclarationUnparsable => "declaration_unparsable",
        }
    }
}

/// Which registry a gate was found in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateSide {
    /// The host's own table of checks.
    Host,
    /// A contract test in the platform's own tree.
    Platform,
}

/// What one look at the two registries concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// A gate exists, in this registry.
    Enforced(GateSide),
    /// Both registries were read and neither names a gate. A debt.
    Unenforced,
    /// The principle is one a person holds: no machine gate is meant to exist, so
    /// neither registry's silence about it is a finding. Distinct from a debt --
    /// a debt is work that a gate would clear, and this row's "no gate" cannot be
    /// cleared by any amount of work, so counting it as one makes the ungated
    /// number a count that never falls.
    HumanOnly,
    /// The look could not settle it, and says why.
    Unsettled {
        reason: UnsettledReason,
        why: String,
    },
}

impl Verdict {
    fn unsettled(reason: UnsettledReason, why: impl Into<String>) -> Self {
        Verdict::Unsettled {
            reason,
            why: why.into(),
        }
    }

    /// The state this verdict moves the row to, or `None` when it moves nothing.
    ///
    /// `None` does not mean "no gate". It means this look does not speak to the
    /// row, and writing `unenforced` anyway would turn a registry nobody could
    /// read into a debt against the principle.
    pub fn state(&self) -> Option<cog_core::PrincipleState> {
        match self {
            Verdict::Enforced(_) => Some(cog_core::PrincipleState::Enforced),
            Verdict::Unenforced => Some(cog_core::PrincipleState::Unenforced),
            Verdict::HumanOnly => None,
            Verdict::Unsettled { .. } => None,
        }
    }

    pub fn reason(&self) -> Option<UnsettledReason> {
        match self {
            Verdict::Unsettled { reason, .. } => Some(*reason),
            _ => None,
        }
    }

    pub fn why(&self) -> Option<&str> {
        match self {
            Verdict::Unsettled { why, .. } => Some(why),
            _ => None,
        }
    }
}

/// The platform's own registry, as read out of one revision of the tree.
///
/// Two things come back from one read, and both are needed: the declarations
/// that name a slug, and the ones that do not. Keeping the second as a count of
/// lines would answer the wrong question -- a declaration whose slug cannot be
/// read is a specific file's specific line, and "there is one of them" does not
/// tell anyone where to look.
#[derive(Debug, Clone, Default)]
pub struct Declarations {
    /// Slug to the file that declares it, sorted so a reading is a function of
    /// the tree and not of the order git happened to walk it in.
    gates: BTreeMap<String, String>,
    /// Declarations found that name a slug nothing can key on, as the file and
    /// the line as it was written.
    unreadable: Vec<(String, String)>,
}

impl Declarations {
    /// The declarations in a set of matched lines, as `(path, text)` pairs.
    ///
    /// A declaration is a header line whose remainder is exactly one slug-shaped
    /// word. Anything else is kept as unreadable rather than dropped, and the
    /// difference matters twice over: a slug that was mistyped is a gate the
    /// account would otherwise report as missing, and a line that is prose about
    /// the convention rather than a declaration is a file that has no business
    /// saying it enforces anything.
    pub fn parse(lines: &[(String, String)]) -> Self {
        let mut gates = BTreeMap::new();
        let mut unreadable = Vec::new();
        for (path, text) in lines {
            let Some(rest) = text.trim_start().strip_prefix(PLATFORM_DECLARATION_PREFIX) else {
                // The pattern anchors the prefix at the start of the line, so a
                // match that does not carry it was matched for some other reason.
                unreadable.push((path.clone(), text.trim().to_string()));
                continue;
            };
            let slug = rest.trim();
            if is_slug(slug) {
                gates.insert(slug.to_string(), path.clone());
            } else {
                unreadable.push((path.clone(), text.trim().to_string()));
            }
        }
        Self { gates, unreadable }
    }

    /// The file declaring a gate for this principle, if one does.
    pub fn gate_for(&self, slug: &str) -> Option<&str> {
        self.gates.get(slug).map(String::as_str)
    }

    /// Every declared slug.
    pub fn slugs(&self) -> impl Iterator<Item = &str> {
        self.gates.keys().map(String::as_str)
    }

    /// The declarations that name a slug nothing can key on, as the file and the
    /// line as it was written.
    pub fn unreadable(&self) -> &[(String, String)] {
        &self.unreadable
    }

    /// How many such declarations the registry held.
    pub fn unreadable_count(&self) -> usize {
        self.unreadable.len()
    }

    /// Whether the registry held anything it could not read.
    ///
    /// A single unreadable line is enough to make every "no gate was found"
    /// conclusion provisional: the declaration that cannot be keyed might have
    /// been the gate for any of them, and the whole point of this column is not
    /// to report a gate as missing.
    pub fn has_unreadable(&self) -> bool {
        !self.unreadable.is_empty()
    }

    /// The declarations whose slug the account has no row for, by name.
    ///
    /// The reverse direction, and the reason it is by name rather than by count
    /// is that two opposite acts produce it: a mistyped slug is a defect in the
    /// declaration that has to be fixed, while a declaration for a principle the
    /// account never heard of is a gap in the reading. Their total is one number
    /// and their fixes are not the same fix.
    pub fn orphans(&self, known: &HashSet<String>) -> Vec<PrincipleOrphan> {
        self.gates
            .iter()
            .filter(|(slug, _)| !known.contains(slug.as_str()))
            .map(|(slug, source)| PrincipleOrphan {
                slug: slug.clone(),
                source: source.clone(),
            })
            .collect()
    }
}

/// Whether a string is the shape a principle's identity has: the memory's file
/// name without its extension.
///
/// Narrow on purpose. Everything downstream keys on this string -- the account's
/// primary key, the two registries, the projection's columns -- and a slug with a
/// space or an upper-case letter in it is one that will key differently in two of
/// those places, which is the collision the whole identity is protecting against.
fn is_slug(slug: &str) -> bool {
    !slug.is_empty()
        && slug
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// One look at both registries for one row.
///
/// The two sides are asked in a fixed order so the answer does not depend on
/// which one was checked first: the host's column is read first because it is the
/// side that produces the readings, and a principle the host side holds a gate
/// for is settled no matter what the platform's registry says.
///
/// Every way out that is not a state is a reason from the closed set, and there
/// is no fallback: a host column that is empty or absent is *not* a gate, and it
/// is not the absence of one either.
pub fn judge(row: &PrincipleRow, declarations: &Declarations) -> Verdict {
    // A principle whose tier a person holds settles here, before either registry
    // is asked. Asking them would report it as a debt the moment neither named a
    // gate -- and no gate is meant to, so the debt would be permanent, and every
    // count of ungated principles would carry it forever. The tier is the record
    // that this is a person's to decide; nothing the registries say can change
    // that, so the look stops here rather than reading their silence as an
    // answer.
    if row.check_kind == Some(cog_core::PrincipleCheckKind::Judgement) {
        return Verdict::HumanOnly;
    }
    let platform = declarations.gate_for(&row.slug);
    match row.host_gate.as_deref().map(str::trim) {
        // No value and an empty value are the same reading: the host's column
        // says nothing about this principle. Reported as its own reason rather
        // than as a gate or as a missing one, because both of those are claims
        // about a registry, and this is a claim about the column.
        None | Some("") => Verdict::unsettled(
            UnsettledReason::HostColumnAbsent,
            "no reading has arrived carrying a host column for this principle, so whether \
             the host's own registry holds a gate for it is not known",
        ),
        Some(host) if host.starts_with(HOST_JUDGEMENT_STALE_PREFIX) => Verdict::unsettled(
            UnsettledReason::CarrierNotInTree,
            format!(
                "the host's judgement is `{host}`: the memory it executes is not in the host's \
                 tree, so it covers nothing"
            ),
        ),
        // A reading arrived and the host's registry holds nothing for this
        // principle, which is what makes the platform's registry the only one
        // left that could still name a gate.
        Some(HOST_REGISTRY_READ_NO_GATE) => match platform {
            Some(_) => Verdict::Enforced(GateSide::Platform),
            None if declarations.has_unreadable() => Verdict::unsettled(
                UnsettledReason::DeclarationUnparsable,
                "no gate was found for this principle, and the registry holds a declaration \
                 whose slug cannot be read: the unreadable one may be its gate",
            ),
            None => Verdict::Unenforced,
        },
        Some(_) => Verdict::Enforced(GateSide::Host),
    }
}

/// The rows a pass may measure: the outstanding set less the principles this
/// pass just found drifted.
///
/// A drifted row is one whose carrier's bytes are no longer the bytes it was
/// measured against, so a verdict written for it now would be a verdict about
/// the previous text -- the gate may well answer for the old wording and say
/// nothing about the new one. The drift is reported by name on the pass that
/// found it, and the row is measured on a later pass, against the text the
/// account has since been told about.
pub fn measurable<'a>(
    rows: &'a [PrincipleRow],
    drifted: &HashSet<String>,
) -> Vec<&'a PrincipleRow> {
    rows.iter()
        .filter(|row| !drifted.contains(&row.slug))
        .collect()
}

/// The annotation one drifted principle is reported with.
///
/// By name with both hashes, because the question the reading raises is "did the
/// gate follow the text", and a count of drifted rows cannot be answered without
/// looking up what moved. The hashes compared are the row's stored `carrier_sha256`
/// and the one the reading carried.
pub fn drift_annotation(drift: &PrincipleDrift) -> String {
    format!(
        "{}: carrier_sha256 {} -> {}",
        drift.slug, drift.was, drift.now
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn row(slug: &str, host: Option<&str>) -> PrincipleRow {
        PrincipleRow {
            slug: slug.to_string(),
            lead: None,
            carrier_path: None,
            carrier_sha256: None,
            host_gate: host.map(str::to_string),
            platform_gate: None,
            check_kind: None,
            state: None,
            refusals: 0,
            last_refused_at: None,
            first_seen_at: Utc.timestamp_opt(1_760_000_000, 0).unwrap(),
            measured_at: None,
            measured_rev: None,
        }
    }

    fn declarations(lines: &[(&str, &str)]) -> Declarations {
        Declarations::parse(
            &lines
                .iter()
                .map(|(path, text)| (path.to_string(), text.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    /// The two registries are asked, not the prose: a `-` in the host column
    /// with nothing in the platform's declares a debt, and that is the reading
    /// the whole ledger exists to produce.
    #[test]
    fn both_registries_read_and_neither_naming_it_is_a_debt() {
        let verdict = judge(&row("feedback_x", Some("-")), &Declarations::default());
        assert_eq!(verdict, Verdict::Unenforced);
        assert_eq!(verdict.state(), Some(cog_core::PrincipleState::Unenforced));
    }

    /// A `judgement` row settles on its tier, and the registries are never asked.
    /// The row here carries exactly the reading that would otherwise be a debt --
    /// a `-` in the host column with nothing in the platform's -- and the tier is
    /// what keeps it from being written down as one. A person holds this
    /// principle; no gate is meant to, so neither registry's silence is a finding
    /// and the ungated count must not carry it forever.
    #[test]
    fn a_judgement_row_settles_on_its_tier_before_the_registries_are_asked() {
        let mut held = row("feedback_x", Some("-"));
        held.check_kind = Some(cog_core::PrincipleCheckKind::Judgement);
        let verdict = judge(&held, &Declarations::default());
        assert_eq!(verdict, Verdict::HumanOnly);
        assert_eq!(verdict.state(), None);
        assert_eq!(verdict.reason(), None);
    }

    /// The tier is the whole reason the answer is HumanOnly: the same reading on
    /// a row of any other tier is still a debt. The short-circuit keys on the
    /// tier and not on the registries, so a row that names a machine-checkable
    /// tier is judged exactly as before.
    #[test]
    fn a_machine_checkable_row_is_still_a_debt() {
        for kind in [
            cog_core::PrincipleCheckKind::Shape,
            cog_core::PrincipleCheckKind::Tree,
            cog_core::PrincipleCheckKind::Live,
        ] {
            let mut row = row("feedback_x", Some("-"));
            row.check_kind = Some(kind);
            assert_eq!(
                judge(&row, &Declarations::default()),
                Verdict::Unenforced,
                "{kind:?}"
            );
        }
    }

    /// A gate in either registry is a gate. Neither side is a tiebreak for the
    /// other: both are registries that run.
    #[test]
    fn a_gate_in_either_registry_is_enforced() {
        let hosted = judge(
            &row("feedback_x", Some("SOME_RE")),
            &Declarations::default(),
        );
        assert_eq!(hosted, Verdict::Enforced(GateSide::Host));

        let declared = declarations(&[("crates/cogneva/tests/x.rs", "//! principle: feedback_x")]);
        let platformed = judge(&row("feedback_x", Some("-")), &declared);
        assert_eq!(platformed, Verdict::Enforced(GateSide::Platform));
        assert_eq!(platformed.state(), Some(cog_core::PrincipleState::Enforced));
    }

    /// No host column is not "no gate": it is a column that never arrived, and
    /// writing the row down as a debt would put the host's silence on the
    /// principle's account.
    #[test]
    fn a_host_column_that_never_arrived_settles_nothing() {
        for host in [None, Some("")] {
            let verdict = judge(&row("feedback_x", host), &Declarations::default());
            assert_eq!(verdict.state(), None, "{host:?}");
            assert_eq!(verdict.reason(), Some(UnsettledReason::HostColumnAbsent));
        }
    }

    /// A judgement whose carrier left the tree covers nothing, and it is not a
    /// finding that nothing else covers the principle either. Reading it as a
    /// debt would report a rule nobody runs as a rule nobody gated.
    #[test]
    fn a_stale_judgement_is_not_a_gate_and_not_a_debt() {
        let verdict = judge(
            &row("feedback_x", Some("stale:SOME_RE")),
            &Declarations::default(),
        );
        assert_eq!(verdict.state(), None);
        assert_eq!(verdict.reason(), Some(UnsettledReason::CarrierNotInTree));
        assert!(verdict.why().unwrap().contains("stale:SOME_RE"));
    }

    /// One unreadable declaration makes every "no gate" provisional, because the
    /// declaration that cannot be keyed may be the gate for any of them. The
    /// gates that *were* read keep answering: this is a narrowing of the
    /// negative answer, not a fault that stops the pass.
    #[test]
    fn an_unreadable_declaration_holds_the_negative_answer_back() {
        let registry = declarations(&[
            ("crates/x/tests/a.rs", "//! principle: feedback_x"),
            ("crates/x/tests/b.rs", "//! principle: Feedback-Y"),
        ]);
        assert!(registry.has_unreadable());
        // Read and named: still a gate.
        assert_eq!(
            judge(&row("feedback_x", Some("-")), &registry),
            Verdict::Enforced(GateSide::Platform)
        );
        // Not found, and the registry could not be read in full.
        let missing = judge(&row("feedback_z", Some("-")), &registry);
        assert_eq!(missing.state(), None);
        assert_eq!(
            missing.reason(),
            Some(UnsettledReason::DeclarationUnparsable)
        );
        // A host gate settles the row regardless: the platform side's silence is
        // not what the verdict rests on.
        assert_eq!(
            judge(&row("feedback_z", Some("SOME_RE")), &registry),
            Verdict::Enforced(GateSide::Host)
        );
    }

    /// The slug is parsed out of the line, so a declaration nothing can key on
    /// is found and named rather than filtered out of the reading.
    #[test]
    fn a_declaration_nothing_can_key_on_is_kept_and_named() {
        let registry = declarations(&[
            ("crates/x/tests/a.rs", "//! principle: Feedback-X"),
            ("crates/x/tests/b.rs", "//! principle: "),
            (
                "crates/x/tests/c.rs",
                "//! principle: feedback_x extra words",
            ),
            ("crates/x/tests/d.rs", "//! principle: feedback_x"),
        ]);
        assert_eq!(registry.slugs().collect::<Vec<_>>(), vec!["feedback_x"]);
        assert_eq!(registry.unreadable.len(), 3);
        assert!(registry
            .unreadable
            .iter()
            .all(|(path, _)| path.starts_with("crates/x/tests/")));
    }

    /// A line the expression matched but that does not carry the declaration
    /// prefix is prose about the convention, and it is kept rather than dropped:
    /// silence is what makes a wrong declaration unreadable as a wrong one.
    #[test]
    fn a_matched_line_that_is_not_a_declaration_is_not_a_gate() {
        let registry = declarations(&[("crates/x/src/lib.rs", "the //! principle: marker")]);
        assert_eq!(registry.slugs().count(), 0);
        assert_eq!(registry.unreadable.len(), 1);
    }

    /// The reverse direction, by name and with the file: a mistyped slug and a
    /// principle the account never heard of are one count and two different
    /// fixes.
    #[test]
    fn a_declaration_the_account_has_no_row_for_is_reported_by_name() {
        let registry = declarations(&[
            ("crates/x/tests/a.rs", "//! principle: feedback_x"),
            ("crates/x/tests/b.rs", "//! principle: feedback_typo"),
        ]);
        let known: HashSet<String> = ["feedback_x".to_string()].into_iter().collect();
        let orphans = registry.orphans(&known);
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].slug, "feedback_typo");
        assert_eq!(orphans[0].source, "crates/x/tests/b.rs");
    }

    /// A drifted row is measured on a later pass, not this one: a verdict written
    /// now would be about the text the gate was checked against, not the text the
    /// memory holds.
    #[test]
    fn a_drifted_row_is_not_measured_against_a_verdict_it_never_saw() {
        let rows = vec![
            row("feedback_x", Some("A_RE")),
            row("feedback_y", Some("B_RE")),
        ];
        let drifted: HashSet<String> = ["feedback_x".to_string()].into_iter().collect();
        let measurable = measurable(&rows, &drifted);
        assert_eq!(measurable.len(), 1);
        assert_eq!(measurable[0].slug, "feedback_y");
    }

    /// The annotation names the principle and both hashes: a count of drifted
    /// rows would leave every one of them to be looked up.
    #[test]
    fn the_drift_annotation_names_the_principle_and_both_hashes() {
        let note = drift_annotation(&PrincipleDrift {
            slug: "feedback_x".into(),
            was: "aaaa".into(),
            now: "bbbb".into(),
        });
        assert_eq!(note, "feedback_x: carrier_sha256 aaaa -> bbbb");
    }

    /// Every reason in the closed set has its own name: two sharing one would be
    /// a single count covering two faults with two different owners.
    #[test]
    fn every_reason_has_its_own_name() {
        let names = UnsettledReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(names.len(), UnsettledReason::ALL.len());
    }

    /// The identity is narrow, and it has to be: this string is the account's
    /// key, both registries' key and the projection's column, and a slug that is
    /// spelled differently in two of those is two rows for one principle.
    #[test]
    fn the_slug_is_the_shape_the_registries_key_on() {
        assert!(is_slug("feedback_x"));
        assert!(is_slug("feedback_1"));
        assert!(!is_slug(""));
        assert!(!is_slug("Feedback_x"));
        assert!(!is_slug("feedback-x"));
        assert!(!is_slug("feedback x"));
        assert!(!is_slug("feedback_x/y"));
    }
}
