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
//!
//! The registry is the recorded names, but the exposition also publishes names
//! *derived* from them, and those carry no producer of their own to be searched
//! for. [`a_series_this_build_publishes`] is where that second half is decided,
//! once, so the two readers cannot answer it differently — which is what they
//! did: the dashboard derived a histogram's `_bucket`/`_sum`/`_count` from its
//! base and the rule reader did not, so the same `_bucket` was accepted in a
//! panel and rejected in a rule. The companion family was in neither, which
//! leaves a rule naming one — a reading the store really does render — reported
//! as having no producer.

/// Whether a series name is one this build publishes.
///
/// Two shapes count. A recorded name, straight from the registry. And a name the
/// exposition derives from one: the classic histogram's `_bucket`/`_sum`/`_count`
/// and the store's `_observed_timestamp_seconds` companion. A derived name is
/// published only when the base is — the suffix proves nothing on its own, and
/// accepting one over any base would let a rule name a series no surface
/// renders, which is the failure both readers exist to catch.
///
/// This over-accepts a `_sum` or `_count` whose base is a counter rather than a
/// histogram, since the registry is not typed here. No rule or panel does that.
/// The same ceiling applies to the base: the answer is "this build can write
/// it", not "this deployment's store holds it". A registry name no deployment
/// ever records has a companion no scrape renders, and this reading cannot tell
/// that apart from one that is being written — deciding it needs the store,
/// which is a different question asked elsewhere.
pub fn a_series_this_build_publishes(name: &str) -> bool {
    let base_published = |base: &str| {
        cog_core::metric_names::ALL
            .iter()
            .any(|m| m.as_str() == base)
    };

    if base_published(name) {
        return true;
    }
    if let Some(base) = cog_core::observability_text::observed_timestamp_base(name) {
        return base_published(base);
    }
    ["_bucket", "_sum", "_count"]
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix))
        .is_some_and(base_published)
}

/// Names on a foreign table that this build publishes.
///
/// An empty result is the invariant; a non-empty one names the entries whose
/// producer-existence check has been silently retired.
pub fn published_by_this_build(foreign: &[(&'static str, &str)]) -> Vec<&'static str> {
    let mut found: Vec<&'static str> = foreign
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| a_series_this_build_publishes(name))
        .collect();
    found.sort_unstable();
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The invariant the foreign tables are checked against: a name this build
    /// renders may not be filed as somebody else's.
    #[test]
    fn a_published_name_filed_as_foreign_is_caught() {
        assert_eq!(
            published_by_this_build(&[("cogneva_change_funnel", "kube-state-metrics")]),
            vec!["cogneva_change_funnel"]
        );
        assert!(published_by_this_build(&[("up", "Prometheus")]).is_empty());
    }

    /// A rule may read a series derived from one this build publishes: the store
    /// renders the companion for every series it serves, and a histogram's
    /// buckets are rendered from the name its producer records.
    #[test]
    fn a_series_derived_from_a_published_one_counts_as_published() {
        assert!(a_series_this_build_publishes(
            "cogneva_rollout_job_reading_unix_observed_timestamp_seconds"
        ));
        assert!(a_series_this_build_publishes(
            "cogneva_change_funnel_observed_timestamp_seconds"
        ));
        // The histogram triple, decided here as well as in the panel reader.
        let recorded = cog_core::metric_names::ALL
            .first()
            .expect("the registry is not empty")
            .as_str();
        for suffix in ["_bucket", "_sum", "_count"] {
            assert!(
                a_series_this_build_publishes(&format!("{recorded}{suffix}")),
                "{recorded}{suffix} is rendered from {recorded}"
            );
        }
    }

    /// The other direction, which is what keeps the derivation from becoming an
    /// allow-list: the suffix only means something over a base this build can
    /// write. A rule naming one of these fires never, and the check has to say
    /// so rather than accept it for spelling the suffix.
    #[test]
    fn a_derived_name_over_an_unpublished_base_is_still_unknown() {
        for name in [
            "not_a_registry_name_observed_timestamp_seconds",
            "kube_pod_owner_observed_timestamp_seconds",
            "up_observed_timestamp_seconds",
            "not_a_registry_name_bucket",
            "_observed_timestamp_seconds",
        ] {
            assert!(
                !a_series_this_build_publishes(name),
                "{name} has no producer in this build"
            );
        }
    }
}
