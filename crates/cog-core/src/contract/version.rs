//! The release version contract.
//!
//! Three producers write to main — development sessions, the self-evolution
//! loop, and the contribution channel — and none of them is obliged to advance
//! the version constant. A number that each producer is trusted to bump is a
//! label rather than a fact, and it stops naming code: measured 2026-09-26,
//! `0.5.8` covered 93 consecutive first-parent commits while the only release
//! tag, `v0.5.8`, stayed on the first of them. Two different code states
//! answered to one name, and the release channel had produced nothing for 92
//! commits on a branch that said "nothing to do".
//!
//! The judgement lives here, once, because two consumers would otherwise
//! re-derive it and drift: the build stamp (which names the running code) and
//! the release job (which names the released code). They take the same
//! evidence — the commits that changed the declaration, the release tags, and
//! the tag sets each reporting point can see — and reach the same verdict.
//!
//! Every clause answers three ways, and the third is not a softer second:
//! `Satisfied`, `Violated`, or `Unreadable`. A clause whose evidence is
//! missing has not been checked, and a caller that folds it into `Satisfied`
//! reports "nothing wrong" for exactly the case where nothing could be seen.

use std::cmp::Ordering;

/// A `major.minor.patch` version as a release tag carries it.
///
/// Strict on purpose: a version that does not parse is evidence that could not
/// be read, and callers must treat it as such instead of as the number zero.
/// The pre-release and build suffixes (`-rc.1`, `+build`) are dropped, since
/// the contract orders releases by their numeric triple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl Version {
    /// Parse `v0.5.8` or `0.5.8` (with optional `-suffix` / `+suffix`).
    /// Returns `None` for anything that is not exactly three numeric fields,
    /// so that a malformed version cannot pass as a release.
    pub fn parse(text: &str) -> Option<Version> {
        let core = text.strip_prefix('v').unwrap_or(text);
        let core = core.split('+').next()?;
        let core = core.split('-').next()?;
        let mut parts = core.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Version {
            major,
            minor,
            patch,
        })
    }

    /// `0.5.8`, without the tag's leading `v`.
    pub fn as_str(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }

    /// Whether `text` is a release tag: a leading `v` and a parseable triple.
    pub fn is_release_tag(text: &str) -> bool {
        text.starts_with('v') && Version::parse(text).is_some()
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.major, self.minor, self.patch).cmp(&(other.major, other.minor, other.patch))
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// How far a commit sits from the release it descends from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distance {
    /// The commit is the tagged release itself.
    Release,
    /// This many commits past the nearest reachable release tag.
    Past(u64),
    /// The label carried no distance we could read.
    Unknown,
}

/// A name for one code state: the release it descends from, how far past that
/// release it is, and its own revision. The revision is what makes two code
/// states distinct when the release and the distance are equal — and the
/// distance is what makes them distinct when only the release is equal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionId {
    pub version: Version,
    pub distance: Distance,
    pub rev: String,
    /// Whether the tree the label names had uncommitted changes. A dirty tree
    /// is a code state no commit contains, so the label has to say so.
    pub dirty: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionIdError {
    /// No parseable version anywhere in the label.
    Malformed(String),
}

impl VersionId {
    /// Parse a label: `v0.5.8-92-g890e4c0`, `v0.5.8-0-g890e4c0`, the fallback
    /// shape `v0.5.8-unknown-g890e4c0` a build emits when it could not reach
    /// git, and any of those with a trailing `-dirty`. A bare `v0.5.8` is a
    /// *tag*, not a label: it does not say how far past the release the code
    /// is, so it parses to `Distance::Unknown` rather than passing as a
    /// release point.
    pub fn parse(label: &str) -> Result<VersionId, VersionIdError> {
        let trimmed = label.trim();
        let malformed = || VersionIdError::Malformed(trimmed.to_string());
        let (without_dirty, dirty) = match trimmed.strip_suffix("-dirty") {
            Some(rest) => (rest, true),
            None => (trimmed, false),
        };
        // Read from the right: the revision is the last field, the distance the
        // one before it, and the release is whatever is left. Reading leftward
        // instead would break on a release tag that itself carries a hyphen.
        let (material, rev) = match without_dirty.rsplit_once("-g") {
            Some((material, rev)) if !rev.is_empty() => (material, rev.to_string()),
            _ => (without_dirty, String::new()),
        };
        let (version_text, distance) = match material.rsplit_once('-') {
            Some((version_text, tail)) => {
                let distance = match tail {
                    "unknown" => Distance::Unknown,
                    number => match number.parse::<u64>() {
                        Ok(0) => Distance::Release,
                        Ok(n) => Distance::Past(n),
                        Err(_) => return Err(malformed()),
                    },
                };
                (version_text, distance)
            }
            None => (material, Distance::Unknown),
        };
        let version = Version::parse(version_text).ok_or_else(malformed)?;
        Ok(VersionId {
            version,
            distance,
            rev,
            dirty,
        })
    }

    /// How many commits past the release, when that is knowable.
    pub fn commits_since_release(&self) -> Option<u64> {
        match self.distance {
            Distance::Release => Some(0),
            Distance::Past(n) => Some(n),
            Distance::Unknown => None,
        }
    }
}

/// One commit that changed `workspace.package.version`, with the value before
/// and after. Only commits that touch the declaration can change it, so these
/// are the whole input needed to order the declarations along main.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclarationChange {
    pub rev: String,
    pub from: String,
    pub to: String,
}

/// A release tag, with the declaration its commit carries.
///
/// `declared_version` comes from the tagged commit's own `Cargo.toml`, so the
/// check compares the tag against the tree it points at rather than against
/// another tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseTag {
    pub tag: String,
    pub rev: String,
    pub declared_version: String,
}

/// The release tags one reporting point (a checkout, a cluster bare repo, the
/// upstream authority) can reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagSet {
    pub point: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clause {
    /// The declaration never decreases along main.
    DeclarationMonotone,
    /// A tag's commit declares the version the tag names.
    TagFidelity,
    /// One tag per version, on the first commit that declares it.
    ReleasePoint,
    /// Every reporting point sees the same release tags.
    TagSetAgreement,
}

impl Clause {
    pub fn as_str(&self) -> &'static str {
        match self {
            Clause::DeclarationMonotone => "declaration_monotone",
            Clause::TagFidelity => "tag_fidelity",
            Clause::ReleasePoint => "release_point",
            Clause::TagSetAgreement => "tag_set_agreement",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub clause: Clause,
    /// What the violation is about: a rev, or a tag name.
    pub subject: String,
    pub detail: String,
}

/// A clause's answer. `Unreadable` is a distinct outcome, never a pass: the
/// evidence was missing, so no claim about the clause can be made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Satisfied,
    Violated(Vec<Violation>),
    Unreadable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractReport {
    pub declaration_monotone: Verdict,
    pub tag_fidelity: Verdict,
    pub release_point: Verdict,
    pub tag_set_agreement: Verdict,
}

impl ContractReport {
    /// Every violation across all clauses, for logging and counting.
    pub fn violations(&self) -> Vec<Violation> {
        [
            &self.declaration_monotone,
            &self.tag_fidelity,
            &self.release_point,
            &self.tag_set_agreement,
        ]
        .into_iter()
        .filter_map(|v| match v {
            Verdict::Violated(vs) => Some(vs.clone()),
            _ => None,
        })
        .flatten()
        .collect()
    }

    /// Whether the contract holds. `Unreadable` is not a hold: a clause that
    /// could not be checked leaves the contract unproven, and the two are
    /// different outcomes for the caller to report.
    pub fn holds(&self) -> bool {
        self.verdicts()
            .iter()
            .all(|v| matches!(v, Verdict::Satisfied))
    }

    /// The clauses whose evidence was missing, by name.
    pub fn unreadable(&self) -> Vec<(&'static str, String)> {
        [
            (Clause::DeclarationMonotone, &self.declaration_monotone),
            (Clause::TagFidelity, &self.tag_fidelity),
            (Clause::ReleasePoint, &self.release_point),
            (Clause::TagSetAgreement, &self.tag_set_agreement),
        ]
        .into_iter()
        .filter_map(|(clause, verdict)| match verdict {
            Verdict::Unreadable(reason) => Some((clause.as_str(), reason.clone())),
            _ => None,
        })
        .collect()
    }

    fn verdicts(&self) -> [&Verdict; 4] {
        [
            &self.declaration_monotone,
            &self.tag_fidelity,
            &self.release_point,
            &self.tag_set_agreement,
        ]
    }
}

/// The evidence a judgement runs on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Evidence {
    /// Commits that changed the declaration, oldest first.
    pub declarations: Vec<DeclarationChange>,
    /// Release tags, with the declaration at the commit each points to.
    pub releases: Vec<ReleaseTag>,
    /// The release tags each reporting point can reach.
    pub tag_sets: Vec<TagSet>,
}

/// Judge the contract over the collected evidence.
pub fn judge(evidence: &Evidence) -> ContractReport {
    ContractReport {
        declaration_monotone: judge_declaration_monotone(&evidence.declarations),
        tag_fidelity: judge_tag_fidelity(&evidence.releases),
        release_point: judge_release_point(&evidence.releases, &evidence.declarations),
        tag_set_agreement: judge_tag_set_agreement(&evidence.tag_sets),
    }
}

fn judge_declaration_monotone(declarations: &[DeclarationChange]) -> Verdict {
    if declarations.is_empty() {
        return Verdict::Unreadable("no commit that changes the version declaration".to_string());
    }
    let mut violations = Vec::new();
    for change in declarations {
        let from = Version::parse(&change.from);
        let to = Version::parse(&change.to);
        let (Some(from), Some(to)) = (from, to) else {
            return Verdict::Unreadable(format!(
                "the declaration at {} could not be read ({} -> {})",
                change.rev, change.from, change.to
            ));
        };
        if to < from {
            violations.push(Violation {
                clause: Clause::DeclarationMonotone,
                subject: change.rev.clone(),
                detail: format!("the declaration went from {from} to {to}"),
            });
        }
    }
    if violations.is_empty() {
        Verdict::Satisfied
    } else {
        Verdict::Violated(violations)
    }
}

fn judge_tag_fidelity(releases: &[ReleaseTag]) -> Verdict {
    if releases.is_empty() {
        return Verdict::Unreadable("no release tag to check".to_string());
    }
    let mut violations = Vec::new();
    for release in releases {
        let Some(tagged) = Version::parse(&release.tag) else {
            return Verdict::Unreadable(format!("{} is not a release tag", release.tag));
        };
        let Some(declared) = Version::parse(&release.declared_version) else {
            return Verdict::Unreadable(format!(
                "the declaration at {} ({}), which {} points to, could not be read",
                release.rev, release.declared_version, release.tag
            ));
        };
        if tagged != declared {
            violations.push(Violation {
                clause: Clause::TagFidelity,
                subject: release.tag.clone(),
                detail: format!(
                    "{} points at {}, which declares {declared}",
                    release.tag, release.rev
                ),
            });
        }
    }
    if violations.is_empty() {
        Verdict::Satisfied
    } else {
        Verdict::Violated(violations)
    }
}

fn judge_release_point(releases: &[ReleaseTag], declarations: &[DeclarationChange]) -> Verdict {
    if releases.is_empty() {
        return Verdict::Unreadable("no release tag to check".to_string());
    }
    let mut violations = Vec::new();
    let mut seen: Vec<(Version, String)> = Vec::new();
    for release in releases {
        let Some(tagged) = Version::parse(&release.tag) else {
            return Verdict::Unreadable(format!("{} is not a release tag", release.tag));
        };
        if let Some((_, first_tag)) = seen.iter().find(|(version, _)| *version == tagged) {
            violations.push(Violation {
                clause: Clause::ReleasePoint,
                subject: release.tag.clone(),
                detail: format!("{tagged} is already released by {first_tag}"),
            });
            continue;
        }
        seen.push((tagged, release.tag.clone()));
        // A release is the commit that first declares the version. Releasing a
        // version that main had already left means the tag names a point that
        // was not a release, and the version now names two code states: the
        // tagged commit and (further along) whatever main carries today.
        if !declarations.is_empty() {
            let first = declarations.iter().position(|change| {
                Version::parse(&change.to)
                    .map(|v| v == tagged)
                    .unwrap_or(false)
            });
            match first {
                None => {
                    return Verdict::Unreadable(format!(
                        "{} declares {}, which no declaration change on main introduces",
                        release.rev, release.declared_version
                    ))
                }
                Some(index) if declarations[index].rev != release.rev => {
                    violations.push(Violation {
                        clause: Clause::ReleasePoint,
                        subject: release.tag.clone(),
                        detail: format!(
                            "{} was declared first at {}, but the tag points at {}",
                            tagged, declarations[index].rev, release.rev
                        ),
                    });
                }
                Some(_) => {}
            }
        }
    }
    if violations.is_empty() {
        Verdict::Satisfied
    } else {
        Verdict::Violated(violations)
    }
}

fn judge_tag_set_agreement(tag_sets: &[TagSet]) -> Verdict {
    if tag_sets.len() < 2 {
        return Verdict::Unreadable(
            "fewer than two reporting points, so the tag sets cannot disagree".to_string(),
        );
    }
    // The authority is the point that carries the most release tags: the
    // upstream platform is where release tags are created, and a local point
    // can only ever be missing them, never have extras nobody published.
    let authority = tag_sets
        .iter()
        .max_by_key(|set| set.tags.len())
        .expect("checked non-empty");
    let mut violations = Vec::new();
    for set in tag_sets {
        if set.point == authority.point {
            continue;
        }
        for tag in &authority.tags {
            if !set.tags.contains(tag) {
                violations.push(Violation {
                    clause: Clause::TagSetAgreement,
                    subject: tag.clone(),
                    detail: format!(
                        "{} is reachable from {} but not from {}; a version label derived there would name the wrong release",
                        tag, authority.point, set.point
                    ),
                });
            }
        }
    }
    if violations.is_empty() {
        Verdict::Satisfied
    } else {
        Verdict::Violated(violations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_parses_with_and_without_the_tag_prefix() {
        assert_eq!(
            Version::parse("v0.5.8"),
            Some(Version {
                major: 0,
                minor: 5,
                patch: 8
            })
        );
        assert_eq!(Version::parse("0.5.8"), Version::parse("v0.5.8"));
        assert_eq!(Version::parse("v1.0.0").unwrap().as_str(), "1.0.0");
    }

    #[test]
    fn suffixes_do_not_change_the_numeric_triple() {
        assert_eq!(Version::parse("v0.5.8-rc.1"), Version::parse("v0.5.8"));
        assert_eq!(Version::parse("v0.5.8+build.7"), Version::parse("v0.5.8"));
    }

    #[test]
    fn a_malformed_version_is_unreadable_rather_than_zero() {
        for text in ["", "v", "v0.5", "v0.5.8.9", "v0.5.x", "abc"] {
            assert_eq!(Version::parse(text), None, "{text} parsed");
        }
    }

    #[test]
    fn versions_order_by_their_triple() {
        assert!(Version::parse("v0.5.8").unwrap() < Version::parse("v0.5.9").unwrap());
        assert!(Version::parse("v0.5.9").unwrap() < Version::parse("v0.6.0").unwrap());
        assert!(Version::parse("v2.0.0").unwrap() > Version::parse("v1.99.99").unwrap());
    }

    #[test]
    fn only_a_leading_v_with_a_triple_is_a_release_tag() {
        assert!(Version::is_release_tag("v0.5.8"));
        assert!(!Version::is_release_tag("0.5.8"));
        assert!(!Version::is_release_tag("promote/abc"));
        assert!(!Version::is_release_tag("gen-3"));
    }

    #[test]
    fn a_label_carries_its_distance_and_revision() {
        let id = VersionId::parse("v0.5.8-92-g890e4c0").unwrap();
        assert_eq!(id.version, Version::parse("v0.5.8").unwrap());
        assert_eq!(id.distance, Distance::Past(92));
        assert_eq!(id.rev, "890e4c0");
        assert_eq!(id.commits_since_release(), Some(92));
    }

    #[test]
    fn zero_distance_is_a_release_point() {
        let id = VersionId::parse("v0.5.8-0-g72c99e3").unwrap();
        assert_eq!(id.distance, Distance::Release);
        assert_eq!(id.commits_since_release(), Some(0));
    }

    #[test]
    fn a_bare_version_has_no_distance_and_is_not_a_release_point() {
        let id = VersionId::parse("v0.5.8").unwrap();
        assert_eq!(id.distance, Distance::Unknown);
        assert_eq!(id.commits_since_release(), None);
    }

    #[test]
    fn the_fallback_shape_reads_as_unknown_distance_not_as_zero() {
        let id = VersionId::parse("v0.5.8-unknown-g890e4c0").unwrap();
        assert_eq!(id.distance, Distance::Unknown);
        assert_eq!(id.rev, "890e4c0");
        let bare = VersionId::parse("v0.5.8-unknown").unwrap();
        assert_eq!(bare.distance, Distance::Unknown);
        assert_eq!(bare.rev, "");
    }

    #[test]
    fn a_dirty_suffix_is_carried_and_does_not_hide_the_distance() {
        let id = VersionId::parse("v0.5.8-92-g890e4c0-dirty").unwrap();
        assert!(id.dirty);
        assert_eq!(id.distance, Distance::Past(92));
        assert_eq!(id.rev, "890e4c0");
        assert!(!VersionId::parse("v0.5.8-92-g890e4c0").unwrap().dirty);
    }

    #[test]
    fn a_release_tag_that_carries_its_own_hyphen_still_reads() {
        // Reading from the right is what keeps a hyphenated tag from being
        // mistaken for a distance.
        let id = VersionId::parse("v0.5.8-rc.1-92-g890e4c0").unwrap();
        assert_eq!(id.version, Version::parse("v0.5.8").unwrap());
        assert_eq!(id.distance, Distance::Past(92));
        assert_eq!(id.rev, "890e4c0");
    }

    #[test]
    fn a_label_without_a_version_is_malformed() {
        assert!(matches!(
            VersionId::parse("main-890e4c0"),
            Err(VersionIdError::Malformed(_))
        ));
        assert!(matches!(
            VersionId::parse("v0.5.8-notanumber-g890e4c0"),
            Err(VersionIdError::Malformed(_))
        ));
    }

    fn change(rev: &str, from: &str, to: &str) -> DeclarationChange {
        DeclarationChange {
            rev: rev.to_string(),
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    fn release(tag: &str, rev: &str, declared: &str) -> ReleaseTag {
        ReleaseTag {
            tag: tag.to_string(),
            rev: rev.to_string(),
            declared_version: declared.to_string(),
        }
    }

    #[test]
    fn an_increasing_declaration_is_satisfied() {
        let evidence = Evidence {
            declarations: vec![change("a", "0.5.7", "0.5.8"), change("b", "0.5.8", "0.5.9")],
            ..Default::default()
        };
        assert_eq!(
            judge_declaration_monotone(&evidence.declarations),
            Verdict::Satisfied
        );
    }

    #[test]
    fn a_declaration_that_goes_backwards_is_a_violation() {
        // The measured shape of a reverted bump: the name `0.5.9` would come
        // back to name code that is newer than it.
        let verdict = judge_declaration_monotone(&[change("a", "0.5.9", "0.5.8")]);
        match verdict {
            Verdict::Violated(violations) => {
                assert_eq!(violations.len(), 1);
                assert_eq!(violations[0].clause, Clause::DeclarationMonotone);
                assert_eq!(violations[0].subject, "a");
            }
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn an_unreadable_declaration_is_not_a_pass() {
        let verdict = judge_declaration_monotone(&[change("a", "0.5.8", "not-a-version")]);
        assert!(matches!(verdict, Verdict::Unreadable(_)), "{verdict:?}");
    }

    #[test]
    fn no_declaration_change_at_all_is_unreadable() {
        assert!(matches!(
            judge_declaration_monotone(&[]),
            Verdict::Unreadable(_)
        ));
    }

    #[test]
    fn a_tag_pointing_at_a_commit_that_declares_another_version_is_a_violation() {
        let verdict = judge_tag_fidelity(&[release("v0.5.8", "72c99e3", "0.5.9")]);
        match verdict {
            Verdict::Violated(violations) => {
                assert_eq!(violations[0].clause, Clause::TagFidelity);
                assert!(violations[0].detail.contains("declares 0.5.9"));
            }
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_faithful_tag_is_satisfied() {
        assert_eq!(
            judge_tag_fidelity(&[release("v0.5.8", "72c99e3", "0.5.8")]),
            Verdict::Satisfied
        );
    }

    #[test]
    fn every_clause_without_release_tags_is_unreadable() {
        assert!(matches!(judge_tag_fidelity(&[]), Verdict::Unreadable(_)));
        assert!(matches!(
            judge_release_point(&[], &[change("a", "0.5.7", "0.5.8")]),
            Verdict::Unreadable(_)
        ));
    }

    #[test]
    fn one_version_released_twice_is_a_violation() {
        let verdict = judge_release_point(
            &[
                release("v0.5.8", "72c99e3", "0.5.8"),
                release("v0.5.8", "890e4c0", "0.5.8"),
            ],
            &[change("72c99e3", "0.5.7", "0.5.8")],
        );
        match verdict {
            Verdict::Violated(violations) => {
                assert!(violations
                    .iter()
                    .any(|v| v.detail.contains("already released by v0.5.8")));
            }
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn tagging_past_the_release_point_is_a_violation() {
        // main declared 0.5.8 at `first`, then moved on; tagging a later commit
        // releases a point that was not a release and leaves the version naming
        // both the tagged commit and today's main.
        let verdict = judge_release_point(
            &[release("v0.5.8", "later", "0.5.8")],
            &[
                change("first", "0.5.7", "0.5.8"),
                change("later", "0.5.8", "0.5.8"),
            ],
        );
        match verdict {
            Verdict::Violated(violations) => {
                assert!(violations[0].detail.contains("declared first at first"));
            }
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn the_release_point_of_a_version_is_satisfied() {
        assert_eq!(
            judge_release_point(
                &[release("v0.5.8", "first", "0.5.8")],
                &[change("first", "0.5.7", "0.5.8")],
            ),
            Verdict::Satisfied
        );
    }

    #[test]
    fn a_reporting_point_missing_a_release_tag_is_a_violation() {
        let verdict = judge_tag_set_agreement(&[
            TagSet {
                point: "github".to_string(),
                tags: vec!["v0.5.7".to_string(), "v0.5.8".to_string()],
            },
            TagSet {
                point: "cluster-bare".to_string(),
                tags: vec!["v0.5.8".to_string()],
            },
        ]);
        match verdict {
            Verdict::Violated(violations) => {
                assert_eq!(violations.len(), 1);
                assert_eq!(violations[0].subject, "v0.5.7");
                assert!(violations[0].detail.contains("not from cluster-bare"));
            }
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn the_measured_shape_of_the_repo_is_satisfied_on_the_clauses_it_can_check() {
        // Measured in the working tree on 2026-09-26: main's first-parent
        // history starts at f9f1d02, whose Cargo.toml carries version 0.5.7 for
        // the first time (there is no earlier declaration to compare against,
        // so it is recorded as a no-op), and 72c99e3 is the one commit that
        // bumped it to 0.5.8. The only release tag reachable here is v0.5.8, and
        // it names 72c99e3 -- the very commit that first declared it. The 93
        // commits that followed without a bump are not a violation: they are a
        // distance, and the distance is what the reading reports instead.
        let evidence = Evidence {
            declarations: vec![
                change("f9f1d02", "0.5.7", "0.5.7"),
                change("72c99e3", "0.5.7", "0.5.8"),
            ],
            releases: vec![release("v0.5.8", "72c99e3", "0.5.8")],
            tag_sets: vec![
                TagSet {
                    point: "github".to_string(),
                    tags: vec!["v0.5.8".to_string()],
                },
                TagSet {
                    point: "cluster-bare".to_string(),
                    tags: vec!["v0.5.8".to_string()],
                },
            ],
        };
        let report = judge(&evidence);
        assert!(report.holds(), "{report:?}");
        assert!(report.unreadable().is_empty());
        assert!(report.violations().is_empty());
        let id = VersionId::parse("v0.5.8-93-g9a10a72").unwrap();
        assert_eq!(id.commits_since_release(), Some(93));
    }

    #[test]
    fn an_unreadable_clause_keeps_the_contract_from_holding() {
        let report = judge(&Evidence {
            declarations: vec![change("a", "0.5.8", "0.5.9")],
            releases: vec![],
            tag_sets: vec![],
        });
        assert!(!report.holds());
        assert!(report.violations().is_empty());
        let unreadable = report.unreadable();
        assert!(unreadable
            .iter()
            .any(|(clause, _)| *clause == "tag_fidelity"));
        assert!(unreadable
            .iter()
            .any(|(clause, _)| *clause == "tag_set_agreement"));
    }

    #[test]
    fn a_single_reporting_point_is_unreadable_not_satisfied() {
        let verdict = judge_tag_set_agreement(&[TagSet {
            point: "github".to_string(),
            tags: vec!["v0.5.8".to_string()],
        }]);
        assert!(matches!(verdict, Verdict::Unreadable(_)), "{verdict:?}");
    }
}
