//! Entries that leave the searchable layer, named.
//!
//! The decay counter answers "how many" and stops there. By the time a
//! `DecayReport` leaves a backend it carries two numbers, and the entries that
//! pass removed are gone from the store as well, so nothing downstream can
//! recover their names. But the question a decay setting raises is *which*
//! entries it is eating, and a count cannot answer it. So the names are emitted
//! where they still exist: at the moment the entry is removed.
//!
//! One event per entry rather than one line carrying a pass's ids, because a
//! pass can clear an arbitrary backlog and a single line would grow with it. A
//! line the log backend rejects is not merely its own loss -- it takes the rest
//! of its batch down with it, since a push consumes the batch and a failure is
//! not retried (only `cogneva_loki_flush_total{outcome="failed"}` records it).
//! A line that stays small is the shape that cannot cause that.
//!
//! The events carry scalars only: the entry's identity, where its content still
//! lives, and the numbers that explain why it fell. Every field here is bounded,
//! so nothing in this file needs a size limit.

use cog_core::SummaryEntry;

/// Record one entry leaving the searchable layer.
///
/// Call this after the removal succeeded, and only then: an event for a drop
/// that did not happen reads exactly like one that did.
pub(crate) fn entry_dropped(entry: &SummaryEntry) {
    tracing::info!(
        namespace = %entry.namespace,
        entry_id = %entry.id,
        raw_uri = %entry.source_ref.raw_uri,
        importance = entry.importance,
        retrieval_count = entry.retrieval_count,
        generated_at = %entry.generated_at.to_rfc3339(),
        "memory: entry left the searchable layer"
    );
}
