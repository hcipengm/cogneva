//! Measuring a change's own check against the project's tree.
//!
//! A ledger row names an artifact as a kind, a scope and a literal, and the state
//! it carries is where a look at the tree found that artifact. This module is the
//! look. Nothing here decides what a change wants -- the identity arrived through
//! the intake and is data -- and nothing here writes a row: it answers one
//! question per identity and hands the answer back to the loop that owns the
//! account.
//!
//! The tree is read out of the project's git repository, not out of a working
//! copy. A running deployment has no checkout of its own source: the image it
//! runs carries the binary and the migrations, not `crates/`. What it does have
//! is the repository the platform builds from, mounted read-only, and git answers
//! both questions this needs against a commit in it -- whether a path is present,
//! and whether a literal occurs under a path -- without a worktree and without
//! writing anything.
//!
//! Three outcomes, and the third one is the point. A check can find the artifact,
//! find it missing, or not be able to speak at all. That last case is not
//! "missing". A check whose literal is written both as "this must be gone" and as
//! "I searched and found nothing" does not distinguish the two; a scope naming a
//! directory this project does not have at all is a check about some other tree;
//! and a check whose literal is empty names nothing to look for. Reporting any of
//! them as "not there" would be inventing a verdict, so they come back as
//! [`CheckOutcome::Unsettled`], carrying both a reason from a closed set and the
//! detail, and the caller leaves those rows' states alone.

use std::path::PathBuf;
use std::time::Duration;

use cog_core::{ChangeIdentity, ChangeState};

/// How long one question to git may take before it is given up on.
///
/// The questions are asked of a local repository and take tens of milliseconds.
/// The bound exists so that a repository on a stalled mount fails as a reading
/// instead of holding the pass: the pass runs under an owner lease that is
/// renewed on a cadence, and a pass that hangs is a pass that loses the seat.
const GIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Where the platform's own deployment mounts the repository it builds from.
///
/// A default rather than a constant: the mount point is a fact about a
/// deployment's filesystem, so a deployment that puts it elsewhere says so
/// through [`TREE_REPO_ENV`] instead of living with the wrong one.
pub const DEFAULT_TREE_REPO: &str = "/host-git";

/// The revision a check is measured against when the deployment names none.
///
/// The mainline, because a change is landed when the project's own line of
/// development carries it; whether that revision is also the one running is a
/// different question, with a different reading.
pub const DEFAULT_TREE_REF: &str = "main";

/// Environment key naming the repository the tree is read out of.
pub const TREE_REPO_ENV: &str = "COGNEVA_CHANGE_LEDGER_TREE_REPO";

/// Environment key naming the revision checks are measured against.
pub const TREE_REF_ENV: &str = "COGNEVA_CHANGE_LEDGER_TREE_REF";

/// The tree this deployment measures against.
///
/// Read from the environment rather than from the configuration document because
/// what it names is a mount -- which path of the host is visible inside this
/// process -- and that is how the other mount-shaped settings arrive: the
/// puller's repository and the branch it follows. The document is for the
/// platform's own behaviour; where the project's source is nailed into the
/// container is a property of the container. The defaults are the arrangement the
/// platform's own deployment already has, so a deployment that says nothing gets
/// the right tree rather than an unread one.
pub fn tree_from_environment() -> CheckTree {
    CheckTree::new(
        std::env::var(TREE_REPO_ENV).unwrap_or_else(|_| DEFAULT_TREE_REPO.to_string()),
        std::env::var(TREE_REF_ENV).unwrap_or_else(|_| DEFAULT_TREE_REF.to_string()),
    )
}

/// Why a check could not be settled, as a closed set.
///
/// Closed because these are read as counts, and a count keyed by free text makes
/// every distinct wording its own series. The five are five different owners and
/// five different actions -- the tree could not be opened, the check has no
/// predicate, the predicate is a finding written out rather than a fragment, the
/// check is about something this tree cannot hold, git itself failed -- and a
/// number covering all five would name none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnsettledReason {
    /// No revision to judge against could be resolved.
    RevisionUnreadable,
    /// There is no predicate for this kind of check.
    KindWithoutPredicate,
    /// The predicate is a whole finding written out, so it names no fragment of
    /// this tree and no search for it can match.
    PredicateIsARendering,
    /// The check names something this tree cannot hold, so it never ran.
    ScopeOutsideTree,
    /// git was asked and did not answer.
    GitFailed,
}

impl UnsettledReason {
    /// Every reason, for a reader that has to enumerate them.
    pub const ALL: [UnsettledReason; 5] = [
        UnsettledReason::RevisionUnreadable,
        UnsettledReason::KindWithoutPredicate,
        UnsettledReason::PredicateIsARendering,
        UnsettledReason::ScopeOutsideTree,
        UnsettledReason::GitFailed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            UnsettledReason::RevisionUnreadable => "revision_unreadable",
            UnsettledReason::KindWithoutPredicate => "kind_without_predicate",
            UnsettledReason::PredicateIsARendering => "predicate_is_a_rendering",
            UnsettledReason::ScopeOutsideTree => "scope_outside_tree",
            UnsettledReason::GitFailed => "git_failed",
        }
    }
}

/// What one look at the tree found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    /// The tree holds the artifact the check names.
    Landed,
    /// The check ran, and the tree does not hold it.
    Absent,
    /// The check could not be run, or running it cannot settle the question.
    Unsettled {
        reason: UnsettledReason,
        why: String,
    },
}

impl CheckOutcome {
    /// An unsettled outcome, built from its reason and its own words.
    fn unsettled(reason: UnsettledReason, why: impl Into<String>) -> Self {
        CheckOutcome::Unsettled {
            reason,
            why: why.into(),
        }
    }

    /// The state this outcome moves the row to, or `None` when it moves nothing.
    ///
    /// `None` does not mean the check passed. It means this look does not speak
    /// to the row: writing a state anyway would replace what a document said with
    /// a verdict no look ever reached, and the row would read as answered.
    pub fn state(&self) -> Option<ChangeState> {
        match self {
            CheckOutcome::Landed => Some(ChangeState::Landed),
            CheckOutcome::Absent => Some(ChangeState::Pending),
            CheckOutcome::Unsettled { .. } => None,
        }
    }

    /// Why the look could not settle the row, for the one outcome that has one.
    pub fn why(&self) -> Option<&str> {
        match self {
            CheckOutcome::Unsettled { why, .. } => Some(why),
            _ => None,
        }
    }

    /// The reason class, for the one outcome that has one.
    pub fn reason(&self) -> Option<UnsettledReason> {
        match self {
            CheckOutcome::Unsettled { reason, .. } => Some(*reason),
            _ => None,
        }
    }
}

/// One revision of the tree, and the shape of its root.
///
/// Resolved once per pass so every check in it answers for the same revision: a
/// ref resolved per check would let a push land in the middle and leave half the
/// pass describing one tree and half another, which reads as a burst of
/// disagreements rather than as a tree that moved. The root's own entries come
/// along because whether this tree could hold a check at all is the same answer
/// for every check in the pass, and asking it per row would be one more git call
/// per row to learn the same thing.
#[derive(Debug, Clone)]
pub struct Snapshot {
    rev: String,
    top_level: Vec<String>,
}

impl Snapshot {
    /// The commit every check in this pass is judged against.
    pub fn rev(&self) -> &str {
        &self.rev
    }

    /// Whether this tree could hold a path under `scope` at all.
    ///
    /// A scope naming a location the project does not have at its top level is
    /// not a check that came back empty; it is a check about some other tree -- a
    /// sibling repository, or a filesystem path on somebody's machine. From a
    /// single `ls-tree` the two are the same empty answer, and only one of them
    /// means "not landed yet", so they are separated here, before the check runs.
    ///
    /// The test is the top-level entry and not the whole path on purpose. A path
    /// that is missing deeper down is what a change which has not landed looks
    /// like, and it has to stay answerable; a top-level entry the project does
    /// not have says the check is not about this tree.
    fn could_hold(&self, scope: &str) -> bool {
        let scope = scope.strip_prefix("./").unwrap_or(scope);
        if scope.is_empty() || scope == "." {
            return true;
        }
        // An absolute path names a place on a filesystem. This tree is not a
        // filesystem, so nothing here can confirm or deny it, and the project's
        // own deployment has no business answering for somebody's home
        // directory.
        if scope.starts_with('/') {
            return false;
        }
        match scope.split('/').next() {
            Some(root) => self.top_level.iter().any(|entry| entry == root),
            None => true,
        }
    }
}

/// The project's source, read at one commit out of a git repository.
///
/// Cheap to clone (a path and a ref) so that every check in a pass reads the same
/// snapshot.
#[derive(Debug, Clone)]
pub struct CheckTree {
    repo: PathBuf,
    reference: String,
}

impl CheckTree {
    pub fn new(repo: impl Into<PathBuf>, reference: impl Into<String>) -> Self {
        Self {
            repo: repo.into(),
            reference: reference.into(),
        }
    }

    /// The commit this pass reads, or why the repository cannot answer for the
    /// reference at all.
    ///
    /// Asked once per pass, and its failure is the whole pass's: a check cannot
    /// answer for a tree nobody could open, and the caller names that once rather
    /// than once per row. Resolving the revision is a separate step from asking
    /// about a path on purpose: `ls-tree` and `grep` report "no such revision"
    /// and "no such path" the same way, so a revision that could not be resolved
    /// would otherwise be read as every change being unlanded.
    pub async fn snapshot(&self) -> Result<Snapshot, String> {
        let rev = format!("{}^{{commit}}", self.reference);
        let out = self
            .git(&["rev-parse", "--verify", rev.as_str()])
            .await
            .map_err(|e| {
                format!(
                    "the tree at {}:{} cannot be read, so no check can answer for it: {e}",
                    self.repo.display(),
                    self.reference
                )
            })?;
        let rev = out.trim().to_string();
        let listing = self
            .git(&["ls-tree", "--name-only", rev.as_str()])
            .await
            .map_err(|e| {
                format!(
                    "the root of {}:{} could not be listed, so no check can answer for it: {e}",
                    self.repo.display(),
                    self.reference
                )
            })?;
        let top_level = listing
            .lines()
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect();
        Ok(Snapshot { rev, top_level })
    }

    /// One check, against the snapshot [`Self::snapshot`] returned.
    pub async fn judge(&self, snapshot: &Snapshot, identity: &ChangeIdentity) -> CheckOutcome {
        let scope = scope_of(identity);
        match identity.kind.as_str() {
            "grep" => self.judge_grep(snapshot, scope, &identity.literal).await,
            "exists" => self.judge_exists(snapshot, scope).await,
            // Both of these name a literal's absence, and both are written by
            // hand to mean two opposite things: the demand that it be gone, and
            // the evidence that a search came back empty. The identity holds the
            // same three parts either way, so a look at the tree cannot tell
            // which one it is looking at.
            "absent" | "missing" => CheckOutcome::unsettled(
                UnsettledReason::KindWithoutPredicate,
                "a check written as an absence is written both for `this must be gone` \
                 and for `a search of this path found nothing`, and the check itself \
                 does not say which one it is",
            ),
            other => CheckOutcome::unsettled(
                UnsettledReason::KindWithoutPredicate,
                format!("there is no evaluator for a `{other}` check"),
            ),
        }
    }

    /// The whole predicate for an `exists` check, and the one place the tree
    /// cannot speak for itself.
    ///
    /// A path this project has never had and a path that is not there yet are one
    /// answer from `ls-tree`, which is why [`Snapshot::could_hold`] runs first: a
    /// check whose top level this project does not have is not a check that
    /// missed, it is a check about another tree.
    async fn judge_exists(&self, snapshot: &Snapshot, scope: &str) -> CheckOutcome {
        // The whole tree is the artifact, and it is there by construction.
        if scope.is_empty() || scope == "." {
            return CheckOutcome::Landed;
        }
        if !snapshot.could_hold(scope) {
            return CheckOutcome::unsettled(
                UnsettledReason::ScopeOutsideTree,
                format!("`{scope}` is not a path this tree can hold"),
            );
        }
        match self.has(snapshot.rev(), scope).await {
            Ok(true) => CheckOutcome::Landed,
            Ok(false) => CheckOutcome::Absent,
            Err(why) => CheckOutcome::unsettled(UnsettledReason::GitFailed, why),
        }
    }

    async fn judge_grep(&self, snapshot: &Snapshot, scope: &str, literal: &str) -> CheckOutcome {
        if literal.is_empty() {
            return CheckOutcome::unsettled(
                UnsettledReason::KindWithoutPredicate,
                "a `grep` check with no literal names nothing to look for",
            );
        }
        if predicate_is_a_rendering(literal) {
            return CheckOutcome::unsettled(
                UnsettledReason::PredicateIsARendering,
                format!(
                    "`{literal}` is a finding written out rather than a fragment of this \
                     tree: it carries the separator a finding is split on"
                ),
            );
        }
        let scoped = !scope.is_empty() && scope != ".";
        if scoped && !snapshot.could_hold(scope) {
            return CheckOutcome::unsettled(
                UnsettledReason::ScopeOutsideTree,
                format!("`{scope}` is not a path this tree can hold"),
            );
        }
        // A scope that is not in the tree is not a check that found nothing: the
        // search never happened, and its empty result says nothing about the
        // artifact. Distinguished from a real miss on purpose -- one is a drifted
        // path, the other is the answer.
        if scoped {
            match self.has(snapshot.rev(), scope).await {
                Ok(true) => {}
                Ok(false) => {
                    return CheckOutcome::unsettled(
                        UnsettledReason::ScopeOutsideTree,
                        format!("this revision has no `{scope}`, so the check cannot be run"),
                    )
                }
                Err(why) => return CheckOutcome::unsettled(UnsettledReason::GitFailed, why),
            }
        }
        match self.holds(snapshot.rev(), scope, literal).await {
            Ok(true) => CheckOutcome::Landed,
            Ok(false) => CheckOutcome::Absent,
            Err(why) => CheckOutcome::unsettled(UnsettledReason::GitFailed, why),
        }
    }

    /// Whether a path is in the tree at this commit.
    ///
    /// Asked with `ls-tree`, which answers by listing: an empty listing is the
    /// path not being there. `cat-file -e` exits non-zero for a missing path and
    /// for an unreadable repository alike, so telling those apart would mean
    /// reading git's error text, and the two are different answers.
    async fn has(&self, rev: &str, path: &str) -> Result<bool, String> {
        let out = self.git(&["ls-tree", rev, "--", path]).await?;
        Ok(!out.trim().is_empty())
    }

    /// Whether any file under `scope` holds `literal`, byte for byte.
    ///
    /// `-F` because the literal is the artifact's spelling and not a pattern: a
    /// literal containing a `.` or a `[` is not a wildcard, and matching it as
    /// one would land changes the tree never had.
    async fn holds(&self, rev: &str, scope: &str, literal: &str) -> Result<bool, String> {
        let mut args = vec!["grep", "-q", "-F", "-e", literal, rev];
        if !scope.is_empty() && scope != "." {
            args.push("--");
            args.push(scope);
        }
        match self.git(&args).await {
            Ok(_) => Ok(true),
            // `git grep` exits 1 for "no match", which is the answer and not a
            // failure. Everything else it reports as itself.
            Err(e) if e.starts_with(NO_MATCH) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Every line under `pathspec` at `rev` matching an extended `pattern`, as
    /// the file it is in and its text.
    ///
    /// A general read of the tree rather than a check about one change: what the
    /// platform's own source declares about itself is read the same way, out of
    /// the same commit, with the same timeout -- a second git wrapper would be a
    /// second place for the revision discipline and the timeout to drift from
    /// this one.
    ///
    /// The expression is extended because the shape a declaration is written in
    /// is what is being looked for; a caller wanting its argument treated as
    /// text has [`Self::holds`], which anchors it with `-F`.
    ///
    /// A repository with no match is an empty list, not a failure: `git grep`
    /// reports the two differently and only one of them is news.
    pub async fn matching_lines(
        &self,
        rev: &str,
        pathspec: &str,
        pattern: &str,
    ) -> Result<Vec<(String, String)>, String> {
        let mut args = vec!["grep", "-n", "-E", "-e", pattern, rev];
        if !pathspec.is_empty() {
            args.push("--");
            args.push(pathspec);
        }
        match self.git(&args).await {
            Ok(out) => Ok(out
                .lines()
                .filter_map(|line| parse_grep_line(line, rev))
                .collect()),
            Err(e) if e.starts_with(NO_MATCH) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    /// One git invocation against this repository. `Ok` is its stdout; `Err` is
    /// what went wrong, as a line fit to put in a log.
    async fn git(&self, args: &[&str]) -> Result<String, String> {
        let mut cmd = tokio::process::Command::new("git");
        cmd.arg("--git-dir")
            .arg(&self.repo)
            .args(args)
            // The repository's own directory, so a scope is read relative to the
            // tree and not to wherever this process happens to have been started.
            .current_dir(&self.repo)
            .kill_on_drop(true);
        let out = match tokio::time::timeout(GIT_TIMEOUT, cmd.output()).await {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                return Err(format!(
                    "git could not be run against {}: {e}",
                    self.repo.display()
                ))
            }
            Err(_) => return Err(format!("git did not answer within {GIT_TIMEOUT:?}")),
        };
        if out.status.success() {
            return Ok(String::from_utf8_lossy(&out.stdout).into_owned());
        }
        let stderr = String::from_utf8_lossy(&out.stderr);
        // A refusal with nothing on stderr is `git grep` saying "no match"; it
        // carries a sentinel so the one caller that reads it can tell that from a
        // failure without matching on exit codes.
        if stderr.trim().is_empty() {
            return Err(NO_MATCH.to_string());
        }
        Err(format!(
            "git {} answered {}: {}",
            args.join(" "),
            out.status,
            stderr.trim()
        ))
    }
}

/// Leading marker for a refusal that carries no message, so that "no match" and
/// "could not run" stay apart without reading exit codes at the call site.
const NO_MATCH: &str = "no match";

/// One line of `git grep -n` output, as the file it is in and its text.
///
/// Two formats reach here and the difference is not cosmetic. Asked to read a
/// tree, `git grep` writes the revision in front of every line --
/// `<rev>:<path>:<line>:<text>` -- and asked about the working directory it does
/// not; a parser that took the first colon-separated field for the path would
/// name the revision as the file, keep the path as a line number, and hand its
/// caller text with a line number glued to the front. That misreading is silent:
/// every line still parses, the count is even right, and only the answers are
/// wrong.
///
/// The revision is dropped by its own value rather than by field position. It is
/// the string this call asked for, so a path that happens to hold a colon cannot
/// be mistaken for it, and a revision that git spells differently from the way it
/// was asked for -- a shortened one, say -- shows up as a path that will not
/// resolve rather than as a silently wrong one. The rest is split from the left
/// so that the text stays whole: it is the last field and holds anything, colons
/// included.
fn parse_grep_line(line: &str, rev: &str) -> Option<(String, String)> {
    let line = match line.strip_prefix(rev) {
        Some(rest) => rest.strip_prefix(':').unwrap_or(line),
        None => line,
    };
    let mut fields = line.splitn(3, ':');
    let path = fields.next()?;
    let lineno = fields.next()?;
    let text = fields.next()?;
    // A refusal line rather than a match: `git grep -n` prints a trailing
    // summary for a match with no line number on it, which is not a line of the
    // tree.
    (!lineno.is_empty()).then(|| (path.to_string(), text.to_string()))
}

/// The scope as a path inside the tree.
///
/// A check written by hand carries whatever quoting the document around it used;
/// backticks and padding belong to the document, not to the path.
fn scope_of(identity: &ChangeIdentity) -> &str {
    identity.scope.trim().trim_matches('`').trim()
}

/// The separator a finding is written with, between its scope and its predicate.
///
/// A check is recorded as `grep <scope> :: <literal>`, and the intake splits the
/// line on this. So the separator belongs to the record, and finding it inside a
/// predicate means the split went somewhere the record did not intend -- two
/// findings joined into one line, say, in which case everything after the first
/// separator lands in the predicate field, second rendering included.
const SCOPE_PREDICATE_SEPARATOR: &str = " :: ";

/// Whether the predicate field holds a finding written out rather than a
/// fragment of the tree.
///
/// Deliberately the separator alone, and not the other things a mis-assembled
/// line carries. `||`, `&&` and backticks are ordinary contents of a literal --
/// `a || b` is what a line of this tree's own source looks like -- and a rule
/// that read those as evidence of a rendering would take real checks out of the
/// measurable set, which is the opposite of the point. A padded ` :: ` is not
/// something a fragment of source holds.
///
/// Without this the check is not unanswerable, it is *wrong*: `git grep -F` for
/// a whole finding cannot match a line of source, so the check answers "absent"
/// on every pass forever, the row keeps reading as work still owed, and a change
/// that has landed is indistinguishable from one that has not.
fn predicate_is_a_rendering(literal: &str) -> bool {
    literal.contains(SCOPE_PREDICATE_SEPARATOR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn identity(kind: &str, scope: &str, literal: &str) -> ChangeIdentity {
        ChangeIdentity::new(kind, scope, literal)
    }

    /// A tree with a known root, so the "could this tree hold it" rule is
    /// exercised against a shape rather than against a live repository.
    fn snapshot() -> Snapshot {
        Snapshot {
            rev: "0".repeat(40),
            top_level: ["crates", "deploy", "scripts"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }

    /// Only two outcomes move a row. `Unsettled` moves nothing: the row keeps
    /// whatever a document gave it rather than gaining a verdict no look reached.
    #[test]
    fn only_a_settled_look_carries_a_state() {
        assert_eq!(CheckOutcome::Landed.state(), Some(ChangeState::Landed));
        assert_eq!(CheckOutcome::Absent.state(), Some(ChangeState::Pending));
        assert_eq!(
            CheckOutcome::unsettled(UnsettledReason::GitFailed, "whatever").state(),
            None
        );
    }

    /// A row measured as not there is `pending` -- wanted, not there yet -- and
    /// not `undecided`: something did look, and it settled the question.
    #[test]
    fn a_check_that_ran_and_missed_moves_the_row_to_pending() {
        assert_eq!(CheckOutcome::Absent.state(), Some(ChangeState::Pending));
    }

    /// The reason travels with the outcome that has one, and only with it.
    #[test]
    fn the_reason_belongs_to_the_unsettled_outcome_alone() {
        let unsettled =
            CheckOutcome::unsettled(UnsettledReason::ScopeOutsideTree, "the tree has no `x`");
        assert_eq!(unsettled.why(), Some("the tree has no `x`"));
        assert_eq!(unsettled.reason(), Some(UnsettledReason::ScopeOutsideTree));
        assert_eq!(CheckOutcome::Landed.why(), None);
        assert_eq!(CheckOutcome::Absent.why(), None);
        assert_eq!(CheckOutcome::Landed.reason(), None);
    }

    /// The reasons are a closed set a reader can enumerate, and every one of them
    /// has its own name: two sharing a name would be one count covering two
    /// faults.
    #[test]
    fn every_reason_has_its_own_name() {
        let names = UnsettledReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(names.len(), UnsettledReason::ALL.len());
    }

    /// The scope is the tree's path; the quoting around it is the document's.
    /// Falling to strip it would look for a directory named `` `crates/x` `` and
    /// report every such check as unrunnable.
    #[test]
    fn the_scope_is_read_without_the_documents_quoting() {
        assert_eq!(
            scope_of(&identity("grep", "  `crates/cog-core/src`  ", "fn main")),
            "crates/cog-core/src"
        );
        assert_eq!(scope_of(&identity("grep", "crates/x", "y")), "crates/x");
    }

    /// A check about some other tree is not a check that missed. The project has
    /// no `specs/` at its top level, so a row pointing there belongs to a sibling
    /// repository: answering "not landed" would report a document repository's
    /// file as this project's outstanding work.
    #[test]
    fn a_scope_the_project_does_not_have_at_its_root_is_not_this_trees_check() {
        let snapshot = snapshot();
        assert!(snapshot.could_hold("crates/cog-core/src/lib.rs"));
        assert!(snapshot.could_hold("crates"));
        assert!(snapshot.could_hold("."));
        assert!(snapshot.could_hold(""));
        assert!(!snapshot.could_hold("specs/protocol-constants.md"));
        assert!(!snapshot.could_hold("projects/cogneva/scripts/tracker-census.py"));
        assert!(!snapshot.could_hold("/home/cogneva/.local/bin/claude-tmux-host.sh"));
        assert!(!snapshot.could_hold("./specs/x"));
    }

    /// A path missing deeper down must stay answerable: that is exactly what a
    /// change which has not landed looks like, and widening the rule to the whole
    /// path would make every unlanded change unmeasurable.
    #[test]
    fn a_path_missing_below_a_real_root_stays_answerable() {
        assert!(snapshot().could_hold("crates/cog-nothing/src/lib.rs"));
        assert!(snapshot().could_hold("deploy/helm/cogneva/gone.yaml"));
    }

    /// The two absence kinds never settle, and say so in their own words: the
    /// same three parts are written for both directions of the demand.
    #[tokio::test]
    async fn an_absence_check_never_settles_a_row() {
        let tree = CheckTree::new("/nonexistent", "main");
        for kind in ["absent", "missing"] {
            let outcome = tree
                .judge(&snapshot(), &identity(kind, "crates/x", "y"))
                .await;
            assert_eq!(outcome.state(), None, "{kind} must not move a row");
            assert_eq!(
                outcome.reason(),
                Some(UnsettledReason::KindWithoutPredicate)
            );
            assert!(
                outcome.why().is_some_and(|why| why.contains("absence")),
                "{kind} must say why it could not settle: {outcome:?}"
            );
        }
    }

    /// An unknown kind is refused by name rather than treated as a miss: a check
    /// this platform cannot evaluate is not a check that came back empty.
    #[tokio::test]
    async fn an_unknown_kind_is_named_rather_than_missed() {
        let tree = CheckTree::new("/nonexistent", "main");
        let outcome = tree
            .judge(&snapshot(), &identity("resembles", "crates/x", "y"))
            .await;
        assert_eq!(outcome.state(), None);
        assert_eq!(
            outcome.reason(),
            Some(UnsettledReason::KindWithoutPredicate)
        );
        assert_eq!(
            outcome.why(),
            Some("there is no evaluator for a `resembles` check")
        );
    }

    /// A `grep` with no literal names nothing, so it cannot be run. Reading it as
    /// a miss would land the row on an empty search.
    #[tokio::test]
    async fn a_grep_with_no_literal_does_not_run() {
        let tree = CheckTree::new("/nonexistent", "main");
        let outcome = tree
            .judge(&snapshot(), &identity("grep", "crates/x", ""))
            .await;
        assert_eq!(outcome.state(), None);
        assert_eq!(
            outcome.reason(),
            Some(UnsettledReason::KindWithoutPredicate)
        );
        assert!(
            outcome.why().is_some_and(|why| why.contains("no literal")),
            "{outcome:?}"
        );
    }

    /// A check about another tree is refused before any git call, and refused as
    /// its own reason rather than as a miss -- this is the whole difference
    /// between a row about a sibling repository and an outstanding one.
    #[tokio::test]
    async fn a_cross_repository_scope_is_unsettled_not_missed() {
        let tree = CheckTree::new("/nonexistent", "main");
        for kind in ["exists", "grep"] {
            let outcome = tree
                .judge(
                    &snapshot(),
                    &identity(kind, "specs/protocol-constants.md", "x"),
                )
                .await;
            assert_eq!(
                outcome.state(),
                None,
                "{kind} must not answer for another tree"
            );
            assert_eq!(outcome.reason(), Some(UnsettledReason::ScopeOutsideTree));
        }
    }

    /// A repository that is not there is a reading, not a panic and not a pass:
    /// every answer it gives is "could not run", with the path in the reason.
    #[tokio::test]
    async fn a_repository_that_cannot_be_read_settles_nothing() {
        let tree = CheckTree::new("/nonexistent/tree", "main");
        let why = tree
            .snapshot()
            .await
            .expect_err("a repository that is not there has no snapshot");
        assert!(why.contains("/nonexistent/tree"), "{why}");
    }

    /// A predicate that is a finding written out whole answers `absent` on every
    /// pass, because no line of the tree can contain it. Read as a miss, the row
    /// stays in the outstanding set for good and a change that landed reads
    /// exactly like one that did not; it has to read as unattemptable instead.
    ///
    /// The row below is the shape the intake produces when one line carried two
    /// findings: the line is split at the first separator, and everything after
    /// it -- the second finding's own rendering included -- lands in the
    /// predicate field.
    #[tokio::test]
    async fn a_predicate_that_is_a_written_out_finding_never_settles_a_row() {
        let tree = CheckTree::new("/nonexistent", "main");
        let outcome = tree
            .judge(
                &snapshot(),
                &identity(
                    "grep",
                    "crates/cog-agent/src/agent_kernel/runtime.rs",
                    "await_an_unscoped_read || grep \
                     crates/cog-agent/src/agent_kernel/runtime.rs \
                     :: an_unscoped_run_writes_no_census_row",
                ),
            )
            .await;
        assert_eq!(outcome.state(), None, "{outcome:?}");
        assert_eq!(
            outcome.reason(),
            Some(UnsettledReason::PredicateIsARendering)
        );
    }

    /// The separator and nothing else. `||`, `&&` and backticks are all ordinary
    /// contents of a literal -- `a || b` is a line of Rust -- so a rule that read
    /// them as evidence of a rendering would take real checks out of the
    /// measurable set, which is the opposite of what this is for.
    #[test]
    fn a_literal_is_a_rendering_only_when_it_carries_the_separator() {
        assert!(!predicate_is_a_rendering("a || b"));
        assert!(!predicate_is_a_rendering("a && b"));
        assert!(!predicate_is_a_rendering("`fn main`"));
        assert!(!predicate_is_a_rendering("cog_core::Task"));
        assert!(!predicate_is_a_rendering("crates/x::y"));
        assert!(predicate_is_a_rendering("crates/x :: fn thing"));
    }

    /// Reading a tree, `git grep` puts the revision in front of every line. Both
    /// shapes are parsed, because both reach this function and the wrong one
    /// parses cleanly into a path nobody has.
    #[test]
    fn a_grep_line_is_read_with_the_revision_off_and_the_text_whole() {
        let rev = "0".repeat(40);
        assert_eq!(
            parse_grep_line(
                &format!("{rev}:crates/x/tests/a.rs:41://! principle: feedback_x"),
                &rev
            ),
            Some((
                "crates/x/tests/a.rs".to_string(),
                "//! principle: feedback_x".to_string()
            ))
        );
        // The working directory's format, with no revision in front.
        assert_eq!(
            parse_grep_line("crates/x/tests/a.rs:41://! principle: feedback_x", &rev),
            Some((
                "crates/x/tests/a.rs".to_string(),
                "//! principle: feedback_x".to_string()
            ))
        );
    }

    /// The text is whatever follows the second colon, colons included: a line of
    /// source is the thing being read, and one holding `::` must not come back
    /// cut in half.
    #[test]
    fn a_grep_line_keeps_the_colons_in_the_text() {
        let rev = "0".repeat(40);
        assert_eq!(
            parse_grep_line(
                &format!("{rev}:crates/x/src/a.rs:7:let t = cog_core::Task;"),
                &rev
            ),
            Some((
                "crates/x/src/a.rs".to_string(),
                "let t = cog_core::Task;".to_string()
            ))
        );
    }

    /// A prefix that looks like the revision but is not one -- a path that merely
    /// starts with the same characters -- is left alone, and a line carrying no
    /// line number is not a line of the tree.
    #[test]
    fn a_path_that_merely_starts_like_the_revision_is_not_one() {
        let rev = "abc";
        assert_eq!(
            parse_grep_line("abcdef.rs:3:text", rev),
            Some(("abcdef.rs".to_string(), "text".to_string()))
        );
        assert_eq!(parse_grep_line("abcdef.rs::text", rev), None);
    }
}
