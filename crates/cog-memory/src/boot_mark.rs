//! Reporting startup phases to whoever publishes a reader.
//!
//! Loading an embedding or reranker model takes long enough that where it
//! happens in startup decides how the birth footprint is attributed, so the
//! loader reports the instant. The reader that turns those instants into a
//! series lives in another crate, and a crate that links a whole exporter to
//! say one sentence cannot be built without it, cannot be tested without it,
//! and cannot be reused from a host that serves no metrics at all.
//!
//! So the loader holds a sink, not a dependency: the host publishes one
//! [`cog_core::BootPhaseMarker`] through [`install`], and a process that
//! publishes none records nothing. The install is idempotent and keeps the
//! first sink — the sink is a property of the host process, not of a plugin
//! initialisation, and a second one would mean two readers for one series.

use std::sync::{Arc, OnceLock};

use cog_core::BootPhaseMarker;

static SINK: OnceLock<Arc<dyn BootPhaseMarker>> = OnceLock::new();

/// Publish the reader this process' phases are reported to.
///
/// The first call wins. A second one is ignored rather than replacing the
/// first, because two sinks would mean the marks of one process split across
/// two readers: each would hold a subset of the phases and read the missing
/// ones as "this boot never got there".
pub fn install(sink: Arc<dyn BootPhaseMarker>) {
    let _ = SINK.set(sink);
}

/// Report that this process reached `phase`, if anyone is listening.
pub fn mark(phase: &'static str) {
    if let Some(sink) = SINK.get() {
        sink.mark(phase);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A process that published no reader still loads models: the mark has to
    /// be a no-op rather than a panic or a dependency the host must satisfy.
    /// (The other half — a published reader receives the phase — is measured
    /// where the host is assembled, because the sink is one per process and a
    /// second test installing one would race this.)
    #[test]
    fn a_mark_with_no_reader_is_not_a_failure() {
        mark("embedding-dense");
    }
}
