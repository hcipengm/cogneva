//! Reading the version contract's evidence out of a git directory.
//!
//! The clauses themselves are pure functions in `cog-core`; what this module adds
//! is the reading: which commits changed the declaration along the first-parent
//! line, which release tags exist and what each one points at, and whether the
//! history was read to its root so a missing declaration can be told apart from a
//! blind spot.
//!
//! It lives outside the deployer on purpose. Two consumers judge the same
//! contract -- the cluster's deployer, which holds the history every producer
//! pushes into, and the CI judgement over a checkout, which covers a change before
//! it lands -- and a second reader written for the second consumer is how the two
//! would drift into disagreeing about the same commit.

use cog_core::contract::version::{
    Clause, ContractReport, DeclarationChange, Evidence, ReleaseTag, TagSet, Verdict,
};
use cog_core::{SFError, SFResult};
use std::time::Duration;

/// The version contract's readings, kept apart from its verdicts.
///
/// "How far main sits past the nearest release" is a reading rather than a
/// judgement: a threshold on it would call normal progress a violation. What
/// keeps the distance from being ignorable is the derived label -- the distance
/// is part of the name, so one name cannot cover two code states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionReadings {
    /// The nearest release tag and how many first-parent commits past it main is.
    pub nearest_release: Option<(String, u64)>,
    /// The version the tracked main declares.
    pub declared: Option<String>,
}

/// Run git against a git directory and return its stdout.
///
/// `--git-dir` is the only way this module talks to a repository: a bare repo
/// (what the cluster holds) and a working tree (what CI checks out) are both
/// addressed the same way, and neither needs a working directory of its own.
async fn git_out(git_dir: &str, args: &[&str], timeout_secs: u64) -> SFResult<String> {
    let mut full: Vec<&str> = vec!["--git-dir", git_dir];
    full.extend_from_slice(args);
    let mut cmd = tokio::process::Command::new("git");
    cmd.args(&full).kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(timeout_secs), cmd.output())
        .await
        .map_err(|_| {
            SFError::IO(format!(
                "git {} timed out after {timeout_secs}s",
                args.join(" ")
            ))
        })?
        .map_err(|e| SFError::IO(format!("failed to run git: {e}")))?;
    if !output.status.success() {
        return Err(SFError::IO(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// The commit a tag dereferences to.
async fn tag_commit_at(git_dir: &str, tag: &str) -> Option<String> {
    git_out(git_dir, &["rev-list", "-n1", tag], 30)
        .await
        .ok()
        .filter(|rev| !rev.is_empty())
}

/// The version declared at `rev` (`workspace.package.version`).
async fn declared_version_at(git_dir: &str, rev: &str) -> Option<String> {
    let manifest = git_out(git_dir, &["show", &format!("{rev}:Cargo.toml")], 30)
        .await
        .ok()?;
    cog_core::contract::version::declared_version(&manifest).map(|v| v.to_string())
}

/// Whether `older` is an ancestor of `newer`.
async fn is_ancestor_at(git_dir: &str, older: &str, newer: &str) -> bool {
    git_out(git_dir, &["merge-base", "--is-ancestor", older, newer], 30)
        .await
        .is_ok()
}

/// The version contract's evidence and readings, as this git directory holds them.
///
/// `rev` is the line to judge along -- its first-parent history is the main the
/// declaration chain is read from. `points` names the reporting points whose
/// per-point tag refs are compared; a checkout that tracks only one point's tags
/// passes one name, and the clause that needs two answers `Unreadable`, which is
/// the honest answer for a history carrying one point's tags.
///
/// The completeness of the chain is gathered rather than assumed: only a walk
/// that reached the root can say a version was never declared, and a truncated
/// history (a shallow clone) has to answer "unreadable" instead of reporting a
/// healthy history as broken. That is judged by `--is-shallow-repository` rather
/// than by guessing from whether the oldest declaration commit looks like a root,
/// because a shallow boundary answers "no parent" exactly like a root does.
pub async fn evidence_at(
    git_dir: &str,
    rev: &str,
    points: &[String],
) -> (Evidence, VersionReadings) {
    // Only a commit that changed Cargo.toml can change the declaration, so this
    // path carries every value the file ever held.
    let touching = git_out(
        git_dir,
        &[
            "log",
            "--first-parent",
            "--format=%H",
            rev,
            "--",
            "Cargo.toml",
        ],
        120,
    )
    .await
    .unwrap_or_default();

    let mut revs: Vec<&str> = touching
        .split_whitespace()
        .filter(|r| !r.is_empty())
        .collect();
    // Git lists newest first; comparing oldest to newest is what makes from/to
    // come out the right way round.
    revs.reverse();
    let mut declarations = Vec::new();
    let mut previous: Option<String> = None;
    let mut current_version: Option<String> = None;
    let mut chain_seen = 0usize;
    for rev in revs {
        chain_seen += 1;
        let Some(version) = declared_version_at(git_dir, rev).await else {
            continue;
        };
        match &previous {
            Some(before) if before != &version => declarations.push(DeclarationChange {
                rev: rev.to_string(),
                from: before.clone(),
                to: version.clone(),
            }),
            // The first declaration of the file has nothing before it to differ
            // from; carrying the same value on both sides keeps it in the chain
            // without inventing a version the file never held.
            None => declarations.push(DeclarationChange {
                rev: rev.to_string(),
                from: version.clone(),
                to: version.clone(),
            }),
            _ => {}
        }
        previous = Some(version.clone());
        current_version = Some(version);
    }

    let shallow = git_out(git_dir, &["rev-parse", "--is-shallow-repository"], 30)
        .await
        .map(|out| out == "true")
        // A repository whose shape could not be read is not a repository known to
        // be complete.
        .unwrap_or(true);
    let chain_complete = !shallow && chain_seen > 0;

    // Release tags in this repository: what tag fidelity and release point read.
    let mut releases = Vec::new();
    let mut reachable: Vec<(String, u64)> = Vec::new();
    if let Ok(list) = git_out(
        git_dir,
        &["for-each-ref", "--format=%(refname:short)", "refs/tags/v*"],
        60,
    )
    .await
    {
        for tag in list.split_whitespace() {
            let Some(commit) = tag_commit_at(git_dir, tag).await else {
                continue;
            };
            let on_main = is_ancestor_at(git_dir, &commit, rev).await;
            let declared = if on_main {
                declared_version_at(git_dir, &commit)
                    .await
                    .unwrap_or_default()
            } else {
                String::new()
            };
            if on_main {
                if let Ok(count) = git_out(
                    git_dir,
                    &["rev-list", "--count", &format!("{}..{}", commit, rev)],
                    60,
                )
                .await
                {
                    if let Ok(n) = count.parse::<u64>() {
                        reachable.push((tag.to_string(), n));
                    }
                }
            }
            releases.push(ReleaseTag {
                tag: tag.to_string(),
                rev: commit,
                on_tracked_main: on_main,
                declared_version: declared,
            });
        }
    }

    // The release tags each reporting point can reach, for the clause that
    // compares them. A missing point is not reported as agreement: the caller
    // decides what one point's worth of tags can answer.
    let mut tag_sets = Vec::new();
    for point in points {
        let prefix = format!("refs/cogneva/tags/{point}/");
        let pattern = format!("{prefix}v*");
        // Full ref names, not `%(refname:short)`: `:short` strips only the
        // well-known hierarchies (refs/heads, refs/tags, refs/remotes) and leaves
        // a custom one as `cogneva/tags/<point>/v0.5.8`, which no full-prefix
        // strip matches -- every point's set would come back empty, and two empty
        // sets read as agreement. An empty reading must not look like a reading
        // of agreement.
        let Ok(list) = git_out(
            git_dir,
            &["for-each-ref", "--format=%(refname)", &pattern],
            60,
        )
        .await
        else {
            continue;
        };
        let mut tags: Vec<String> = list
            .split_whitespace()
            .filter_map(|name| name.strip_prefix(prefix.as_str()).map(|s| s.to_string()))
            .collect();
        tags.sort();
        tag_sets.push(TagSet {
            point: point.clone(),
            tags,
        });
    }

    // `describe` names the nearest tag, so the reading is aligned with it -- the
    // same code state must not say 93 in one place and something else in another.
    let nearest = reachable.into_iter().min_by_key(|(_, distance)| *distance);
    (
        Evidence {
            declarations,
            chain_complete,
            releases,
            tag_sets,
        },
        VersionReadings {
            nearest_release: nearest,
            declared: current_version,
        },
    )
}

/// Whether a judgement over a checkout should fail the caller.
///
/// This judgement lives here rather than in the caller because the two
/// consumers differ in what they can see, not in what they conclude: the
/// deployer holds every reporting point's tags, a CI checkout holds one point's
/// at most. A verdict of `Violated` is a finding wherever it is read, so it
/// fails either way; `Unreadable` is a finding only when the evidence was
/// expected to be there, and the tag-set clause over a one-point history is not
/// that case -- asking whether two points agree when only one was read has no
/// answer, and failing on it would make the check fire on every checkout.
///
/// `chain_complete` is a separate premise rather than a clause: the three
/// clauses that read history answer from whatever they were given, so a shallow
/// checkout comes back satisfied from a history that stops mid-way. Failing
/// there is what keeps "could not read the history" apart from "the history is
/// fine".
pub fn ci_failure(report: &ContractReport, chain_complete: bool) -> bool {
    if !chain_complete {
        return true;
    }
    Clause::ALL
        .iter()
        .any(|clause| match report.verdict(*clause) {
            Verdict::Satisfied => false,
            Verdict::Violated(_) => true,
            Verdict::Unreadable(_) => *clause != Clause::TagSetAgreement,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::contract::version::judge;
    /// A repository with no commits is not a repository whose shape is known, so
    /// the reading has to come back incomplete rather than complete-by-accident.
    #[tokio::test]
    async fn a_git_dir_that_cannot_be_read_yields_no_claim_of_completeness() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope.git");
        let (evidence, readings) = evidence_at(missing.to_str().unwrap(), "main", &[]).await;
        assert!(!evidence.chain_complete);
        assert!(evidence.declarations.is_empty());
        assert!(evidence.releases.is_empty());
        assert_eq!(readings.declared, None);
        assert_eq!(readings.nearest_release, None);
    }

    async fn real_git(root: &std::path::Path, args: &[&str]) {
        let out = tokio::process::Command::new("git")
            .args(args)
            .current_dir(root)
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Evidence that satisfies every clause: one declaration of `version`, the
    /// tag naming it on the tracked main, and both reporting points carrying
    /// that tag. The gate's tests start from a green judgement so that the one
    /// thing they change is the only thing that can turn it red.
    fn agreed(rev: &str, version: &str, chain_complete: bool) -> Evidence {
        Evidence {
            declarations: vec![DeclarationChange {
                rev: rev.to_string(),
                from: version.to_string(),
                to: version.to_string(),
            }],
            chain_complete,
            releases: vec![ReleaseTag {
                tag: format!("v{version}"),
                rev: rev.to_string(),
                on_tracked_main: true,
                declared_version: version.to_string(),
            }],
            tag_sets: ["github", "gitee"]
                .iter()
                .map(|point| TagSet {
                    point: point.to_string(),
                    tags: vec![format!("v{version}")],
                })
                .collect(),
        }
    }

    /// A history the check could not read to its root cannot be judged, so it
    /// has to fail even though every verdict over it came back satisfied --
    /// which is exactly what a shallow checkout produces.
    #[test]
    fn an_incomplete_history_fails_even_when_every_verdict_is_satisfied() {
        let report = judge(&agreed("a", "0.5.8", false));
        for clause in Clause::ALL {
            assert!(
                matches!(report.verdict(clause), Verdict::Satisfied),
                "{} is green",
                clause.as_str()
            );
        }
        assert!(ci_failure(&report, false), "the unread history gates");
        assert!(
            !ci_failure(&report, true),
            "the same judgement over a complete reading does not"
        );
    }

    /// A violation of the tag-set clause is a real finding -- two points that
    /// were both read disagree -- and the clause being unreadable over a
    /// one-point checkout is not, so the gate has to tell the two apart.
    #[test]
    fn a_tag_set_violation_fails_but_a_one_point_history_does_not() {
        let set = |point: &str, tags: &[&str]| TagSet {
            point: point.to_string(),
            tags: tags.iter().map(|t| t.to_string()).collect(),
        };
        let mut one_point = agreed("a", "0.5.8", true);
        one_point.tag_sets = vec![set("github", &["v0.5.8"])];
        let one_point = judge(&one_point);
        assert!(matches!(
            one_point.tag_set_agreement,
            Verdict::Unreadable(_)
        ));
        assert!(!ci_failure(&one_point, true), "a checkout tracks one point");

        let mut disagreeing = agreed("a", "0.5.8", true);
        disagreeing.tag_sets = vec![set("github", &["v0.5.8"]), set("gitee", &[])];
        assert!(ci_failure(&judge(&disagreeing), true));
    }

    /// A checkout that tracks one point's tags cannot answer the clause that
    /// compares two, so the reading has to name the point it read rather than
    /// come back with an empty set that reads like agreement.
    #[tokio::test]
    async fn one_reporting_point_reads_that_points_tags_only() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        real_git(root, &["init", "-q", "-b", "main", "w"]).await;
        real_git(
            root,
            &[
                "-c",
                "user.email=t@t.com",
                "-c",
                "user.name=T",
                "-C",
                "w",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "x",
            ],
        )
        .await;
        real_git(
            root,
            &[
                "-C",
                "w",
                "update-ref",
                "refs/cogneva/tags/github/v0.5.8",
                "main",
            ],
        )
        .await;

        let git_dir = root.join("w/.git");
        let (evidence, _) =
            evidence_at(git_dir.to_str().unwrap(), "main", &["github".to_string()]).await;
        assert_eq!(
            evidence.tag_sets,
            vec![TagSet {
                point: "github".to_string(),
                tags: vec!["v0.5.8".to_string()],
            }]
        );
    }
}
