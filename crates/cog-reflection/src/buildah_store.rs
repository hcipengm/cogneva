//! What to drop from the buildah store that holds the promotion builder's base
//! images, and what a pass must never touch.
//!
//! The store is a cache. Every image in it was pulled from the cluster registry,
//! which stays the authoritative copy, so removing one costs the next builder
//! that wants it a re-pull and never correctness. The other direction is the
//! expensive one: a base image removed while a rollout is running is a base
//! nobody can rebuild from, and one removed just before a builder wants it turns
//! a fast promotion into a cold one.
//!
//! So the deletable names are a **closed set**, for the same reason the
//! registry's own reclaim has one: only `main-<rev>` is written by this
//! repository one-to-one with a revision, so only `main-<rev>` is in the
//! criterion. The floating `:local`, the `:seed` a deployment starts from, and
//! an image carrying no name at all are outside it and are left alone. A list of
//! names to remove that is missing an entry removes the wrong image in silence,
//! so anything unrecognised is kept.
//!
//! What separates the `main-<rev>` names that remain is the retention set, which
//! is *not* computed here. It comes from the live references -- what the cluster
//! is running, what a rollout has in flight, what the retention window keeps for
//! rollback -- and it is the registry pass's own list. A second list would be a
//! second answer to "what is still in use", and the half that deletes is the
//! half that must not disagree.
//!
//! There is no cap here, and none is wanted: the retention set is already the
//! bound. A cap would be a number for "how much of a rebuildable cache we
//! tolerate", which the retention window answers in the unit that matters --
//! revisions, not bytes.
//!
//! Nothing in this module removes anything. [`plan_prune`] says what a pass
//! would remove and what it left behind, so the decision can be read without
//! acting on it.

use std::collections::HashSet;

/// One image record in the store, as the inventory reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreImage {
    /// The store's own id, which is what a removal names.
    pub id: String,
    /// Every name the record carries, in the order the inventory gave them.
    ///
    /// Empty when it carries none. An image whose tag has moved on still holds
    /// its layers, so this is not the same thing as an image that is not there.
    pub names: Vec<String>,
}

/// The inventory as `buildah images --json` writes it.
///
/// Only the two fields that decide anything are read. The rest of the record --
/// digests, sizes, timestamps -- is left to the parser to ignore rather than
/// pinned here, because a struct that names every field is a second declaration
/// of buildah's output format and would break the read on any field buildah
/// adds.
#[derive(Debug, serde::Deserialize)]
struct RawImage {
    id: String,
    /// `null` when the record carries no names, which is how buildah writes it.
    #[serde(default)]
    names: Option<Vec<String>>,
}

/// Parse a store inventory.
///
/// `Err` on anything the parser does not recognise, **including an empty body**:
/// the next step after a successful read is to remove bytes, so a body that
/// could not be read has to arrive as an error rather than as a store with
/// nothing in it. That distinction is the whole check -- a command that printed
/// nothing and exited 0 would otherwise read as an empty store.
pub fn parse_inventory(json: &str) -> Result<Vec<StoreImage>, String> {
    let raw: Vec<RawImage> =
        serde_json::from_str(json).map_err(|e| format!("inventory is not a JSON array: {e}"))?;
    Ok(raw
        .into_iter()
        .map(|image| StoreImage {
            id: image.id,
            names: image.names.unwrap_or_default(),
        })
        .collect())
}

/// What a pass would remove, and what it left behind, by reason.
///
/// The three unchanged counts are separate rather than one total because they
/// call for different things: `kept` is the retention set working, `unnamed` is
/// a residual the rule cannot reach, and `foreign` is a name this repository
/// does not write at all. A reader that only saw "kept 12" would take the other
/// two for the same thing.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct StorePlan {
    /// Images to remove, by id.
    pub doomed: Vec<StoreImage>,
    /// `main-<rev>` images the retention set protects.
    pub kept: usize,
    /// Images named by nothing. Left alone: with no name there is no revision to
    /// compare against the retention set, so nothing could say whether a builder
    /// still wants it.
    pub unnamed: usize,
    /// Images carrying a name that is not `main-<rev>`.
    pub foreign: usize,
}

/// Choose what to remove, given the store's inventory and the retained revisions.
///
/// Pure: the same inventory and set give the same plan, so what a pass would do
/// can be read without doing it. `kept` holds revisions, not tag names, and an
/// image is kept when **any** of its names is a retained revision: offering to
/// free a base that is still in the retention set because a second name of the
/// same record had fallen out of it would be the one mistake here that cannot be
/// walked back.
///
/// The revisions in `kept` arrive at whatever length their source used -- a
/// state file holds full forty-character revisions, a workload's image holds the
/// twelve the tag carries -- so both sides are shortened before they are
/// compared. Comparing them as given is the failure mode this guards: nothing
/// matches, every image reads as unreferenced, and a pass deletes the base
/// images of the revisions that are running. `plan_prune` is not the only
/// consumer of that set, so it does its own shortening rather than relying on
/// one having been done upstream.
pub fn plan_prune(images: &[StoreImage], kept: &HashSet<String>) -> StorePlan {
    let mut plan = StorePlan::default();
    let retained: HashSet<&str> = kept
        .iter()
        .map(|rev| crate::mainline_deployer::rev12(rev))
        .collect();
    for image in images {
        if image.names.is_empty() {
            plan.unnamed += 1;
            continue;
        }
        // Every name has to be a revision tag; one that is not puts the record
        // outside the criterion entirely.
        let revs: Option<Vec<&str>> = image
            .names
            .iter()
            .map(|name| crate::mainline_deployer::parse_main_rev(name))
            .collect();
        let Some(revs) = revs else {
            plan.foreign += 1;
            continue;
        };
        if revs.iter().any(|rev| retained.contains(rev)) {
            plan.kept += 1;
            continue;
        }
        plan.doomed.push(image.clone());
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(id: &str, names: &[&str]) -> StoreImage {
        StoreImage {
            id: id.to_string(),
            names: names.iter().map(|n| n.to_string()).collect(),
        }
    }

    fn kept(revs: &[&str]) -> HashSet<String> {
        revs.iter().map(|r| r.to_string()).collect()
    }

    const REG: &str = "cogneva-registry.cogneva.svc.cluster.local:5000";

    #[test]
    fn an_inventory_is_read_for_its_ids_and_names() {
        let json = r#"[
            {"id": "a9806341c4df", "names": null, "size": "2.2 GB", "readonly": false},
            {"id": "ec32cdc15c3b", "names": ["reg:5000/cogneva:main-198083f500d1"]},
            {"id": "ffffffffffff", "names": []}
        ]"#;
        let images = parse_inventory(json).unwrap();
        assert_eq!(images.len(), 3);
        assert!(images[0].names.is_empty(), "null names are no names");
        assert_eq!(images[1].names, vec!["reg:5000/cogneva:main-198083f500d1"]);
        assert!(images[2].names.is_empty(), "an empty list is no names");
    }

    #[test]
    fn an_unreadable_inventory_is_an_error_and_not_an_empty_store() {
        assert!(parse_inventory("").is_err(), "an empty body is not a store");
        assert!(parse_inventory("buildah: command not found").is_err());
        // Truncated mid-array: what a killed command leaves behind.
        assert!(parse_inventory(r#"[{"id": "a1", "names": null"#).is_err());
        assert!(parse_inventory("[]").is_ok(), "an empty store is readable");
    }

    #[test]
    fn only_a_revision_outside_the_retention_set_is_doomed() {
        let images = vec![
            named("old", &[&format!("{REG}/cogneva:main-111111111111")]),
            named("live", &[&format!("{REG}/cogneva:main-222222222222")]),
        ];
        let plan = plan_prune(&images, &kept(&["222222222222"]));
        assert_eq!(
            plan.doomed
                .iter()
                .map(|i| i.id.as_str())
                .collect::<Vec<_>>(),
            vec!["old"]
        );
        assert_eq!(plan.kept, 1);
        assert_eq!(plan.unnamed, 0);
        assert_eq!(plan.foreign, 0);
    }

    #[test]
    fn a_floating_or_seed_name_is_never_doomed() {
        // The two names this repository writes that are not revisions. Both are
        // what a deployment starts from, so neither may be removed by a pass
        // whose list only knows revisions.
        let images = vec![
            named("local", &[&format!("{REG}/cogneva:local")]),
            named("seed", &[&format!("{REG}/cogneva:seed")]),
        ];
        let plan = plan_prune(&images, &kept(&[]));
        assert!(plan.doomed.is_empty(), "{:?}", plan.doomed);
        assert_eq!(plan.foreign, 2);
    }

    #[test]
    fn an_unnamed_image_is_left_alone() {
        // No name means no revision to compare, so there is no reading that
        // could say whether anything still wants it.
        let images = vec![named("dangling", &[])];
        let plan = plan_prune(&images, &kept(&[]));
        assert!(plan.doomed.is_empty());
        assert_eq!(plan.unnamed, 1);
    }

    #[test]
    fn a_full_length_retained_rev_still_protects_its_tag() {
        // The keep set is assembled from several sources and they do not agree
        // on a length: a state file holds the full revision, a workload's image
        // holds the twelve the tag carries. Compared as given, an image whose
        // revision is only in the set in long form reads as unreferenced, and
        // the pass removes base images of revisions that are running.
        let images = vec![named(
            "live",
            &[&format!("{REG}/cogneva:main-222222222222")],
        )];
        let plan = plan_prune(
            &images,
            &kept(&["2222222222222222222222222222222222222222"]),
        );
        assert!(plan.doomed.is_empty(), "{:?}", plan.doomed);
        assert_eq!(plan.kept, 1);
    }

    #[test]
    fn one_retained_name_keeps_the_whole_record() {
        // Two names on one image: pruning it would take the retained revision
        // with it, and the layers are one file set either way.
        let images = vec![named(
            "both",
            &[
                &format!("{REG}/cogneva:main-111111111111"),
                &format!("{REG}/cogneva:main-222222222222"),
            ],
        )];
        let plan = plan_prune(&images, &kept(&["222222222222"]));
        assert!(plan.doomed.is_empty());
        assert_eq!(plan.kept, 1);
    }

    #[test]
    fn one_foreign_name_keeps_the_whole_record() {
        // A record named by a revision and by something else is outside the
        // closed set: the pass knows what `main-<rev>` means and nothing else.
        let images = vec![named(
            "mixed",
            &[
                &format!("{REG}/cogneva:main-333333333333"),
                &format!("{REG}/cogneva:local"),
            ],
        )];
        let plan = plan_prune(&images, &kept(&[]));
        assert!(plan.doomed.is_empty());
        assert_eq!(plan.foreign, 1);
    }

    #[test]
    fn an_empty_retention_set_dooms_every_revision_tag() {
        // Not a claim that the store should be emptied: it is what the caller
        // sees when the retention set is empty, which is why the caller refuses
        // to act on a set it could not read.
        let images = vec![named("a", &[&format!("{REG}/cogneva:main-111111111111")])];
        let plan = plan_prune(&images, &kept(&[]));
        assert_eq!(plan.doomed.len(), 1);
    }

    #[test]
    fn a_plan_is_a_function_of_its_inputs() {
        let images = vec![
            named("a", &[&format!("{REG}/cogneva:main-111111111111")]),
            named("b", &[&format!("{REG}/cogneva:local")]),
            named("c", &[]),
        ];
        let set = kept(&["222222222222"]);
        assert_eq!(plan_prune(&images, &set), plan_prune(&images, &set));
    }
}
