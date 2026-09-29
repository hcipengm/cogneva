//! The one reading that can answer "is this series really not ours".
//!
//! Two contracts in this crate — the alert rules and the dashboard — sort every
//! series they read into a table this workspace produces (each entry naming the
//! file that publishes it, which is what makes a deleted producer fail) and a
//! table of foreign series (each entry naming the owner). The two tables are
//! meant to be exclusive, and only the first one is cross-checked against
//! anything: the second is an allow-list whose readers accept a name on it
//! without asking whether a producer exists.
//!
//! So a produced name filed as foreign retires the producer-existence check for
//! it in silence — the name stops being looked for in its source file, the
//! coverage check keeps accepting whatever reads it, and nothing goes red if
//! that producer is later deleted. That is the state this module makes visible.
//!
//! The reading is the closed set: `cog_core::metric_names::ALL` is generated
//! from the recording macros, so a name in it is published by this build by
//! construction. The reverse question — proving a foreign name has no producer
//! here — is deliberately not asked by searching the tree. Foreign names are
//! short and generic (`up`, `ALERTS`, `kube_pod_owner`), so a text match counts
//! prose, and the helper the produced tables use states that ceiling already:
//! whether a value is recorded at the line naming the series is not readable
//! from the text. One direction is judged by identity, the other by text, each
//! by what it can actually decide.

use std::collections::BTreeSet;

/// Names on a foreign table that this build publishes.
///
/// An empty result is the invariant; a non-empty one names the entries whose
/// producer-existence check has been silently retired.
pub fn published_by_this_build(foreign: &[(&str, &str)]) -> Vec<&'static str> {
    let listed: BTreeSet<&str> = foreign.iter().map(|(name, _)| *name).collect();
    let mut found: Vec<&'static str> = cog_core::metric_names::ALL
        .iter()
        .map(|name| name.as_str())
        .filter(|name| listed.contains(*name))
        .collect();
    found.sort_unstable();
    found
}
