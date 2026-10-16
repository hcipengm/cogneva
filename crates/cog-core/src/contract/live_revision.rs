//! The revision the cluster is actually running, as a reading with three
//! outcomes rather than a string.
//!
//! A change is landed when the project's own line of development carries it, and
//! that is a question about a tree -- one repository, one commit, answerable with
//! git alone. Whether the same change is *in effect* is a question about a second
//! face entirely: the images the workloads are running. The two disagree for the
//! whole of every gap between a merge and the rollout that ships it, and the gap
//! is not short -- a build, a CI verdict, an admission decision and a rolling
//! update all sit inside it. A reader that can only see the tree reports a
//! landed change as done while nothing is running it, and reports it that way
//! for as long as the rollout takes, with no reading anywhere that says so.
//!
//! So this is a *separate* reading, arriving through its own door, and it is
//! three-valued for the same reason every other reading here is. "The workloads
//! are all on one main revision" and "they are not" and "the cluster could not
//! be read" are three different facts, and the last two have opposite remedies:
//! the first is a rollout in progress, the second is a permission or an API
//! server, and a caller that recorded them as one `None` would send a reader
//! after the wrong one. Neither of them is "no change is out of effect", which
//! is what a bare string plus a default would say.
//!
//! No revision is accepted from the environment, a configuration document or a
//! caller. A revision written down is a claim about the past that nothing
//! revises: the deployment moves, the note does not, and every reading built on
//! it is a reading of the note. The implementation reads it from the workloads,
//! which is the only place it is live.
//!
//! The unanimity rule lives in the implementation, not here, and it is not an
//! accident of it: several workloads run the same image, a rollout moves them
//! one at a time, and "the live revision" is not a question with an answer while
//! they are split. An implementation that picked one of them would answer with
//! whichever it happened to read first, which is a different revision on every
//! tick of a rollout and never a revision of this platform.

use async_trait::async_trait;

/// Why the live revision could not be read.
///
/// A closed set, because these are read as counts and two rows a reader has to
/// act on differently must not collapse into one number. Each one names a
/// different thing to go and look at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiveRevisionGap {
    /// The workloads do not all carry the same main revision: a rollout is
    /// between them, or one is still on a floating tag. There is a revision
    /// running, but not one this reading can name -- naming any of them would
    /// pick whichever was read first.
    NoUnanimousMainTag,
    /// The workloads could not be read at all: no client, a refused grant, a
    /// timeout, an empty answer. Distinct from the case above because nothing
    /// was learned about the cluster here, and re-running may well answer.
    ClusterUnreadable,
}

impl LiveRevisionGap {
    /// Every gap, for a reader that has to enumerate them.
    pub const ALL: [LiveRevisionGap; 2] = [
        LiveRevisionGap::NoUnanimousMainTag,
        LiveRevisionGap::ClusterUnreadable,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            LiveRevisionGap::NoUnanimousMainTag => "no_unanimous_main_tag",
            LiveRevisionGap::ClusterUnreadable => "cluster_unreadable",
        }
    }
}

/// What the workloads are running.
///
/// The revision is the project's own, not a version string: it is the same
/// identity a measurement carries, so the two compare directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveRevision {
    /// Every workload is on one and the same main revision.
    Known(String),
    /// This reading could not be taken, and which way it failed.
    Unmeasurable(LiveRevisionGap),
}

impl LiveRevision {
    /// The revision, when there is one to name.
    pub fn known(&self) -> Option<&str> {
        match self {
            LiveRevision::Known(rev) => Some(rev),
            LiveRevision::Unmeasurable(_) => None,
        }
    }
}

/// A reader of the revision the workloads are running.
///
/// Published as a service because the component that can read it is the one that
/// already knows which workloads carry this platform's own image -- that list is
/// a fact about this deployment, and a second copy of it in the consumer would
/// be a second place to change one thing and a second thing to get wrong.
#[async_trait]
pub trait LiveRevisionSource: Send + Sync {
    /// Read the revision the workloads are running, or why it could not be read.
    ///
    /// Not a cached value and not a stored one: an implementation that answered
    /// from a snapshot it took earlier would answer with a revision the cluster
    /// may have moved off, which is precisely the fact this reading exists to
    /// keep current.
    async fn live_revision(&self) -> LiveRevision;
}
