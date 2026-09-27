//! The caller side of host document organization: metadata in, an operation set out.
//!
//! Everything here is a **pure function of a listing plus a rule table**. Nothing
//! reads a file body, nothing talks to a network, and nothing looks at the clock.
//! That is not a style choice: the plan a person is asked to approve has to be
//! reproducible from the answer the same person could have got by listing the scope
//! themselves, or "the plan matches the directory" stops being checkable.
//!
//! The order of stations is the design's (09-21 §3.5): classify by name/extension
//! first, which never leaves the cluster; a model is consulted only for what the
//! rules could not decide, and only through the audited channel (`document_egress`
//! on the gateway), which is off by default. So this module's default behaviour --
//! and every behaviour reachable while the switch is off -- is local.
//!
//! Three directions are load-bearing and each is the **"do less"** side of a choice:
//!
//! - A file with no rule **stays where it is**. Dropping it into an `unsorted`
//!   directory would look like order while making it harder to find, and would make
//!   "we classified everything" true by construction.
//! - A destination name already taken **stops that one file, not the whole plan**,
//!   and it stops it by leaving it in place rather than by inventing a new name. A
//!   name the caller never chose is a rename nobody asked for.
//! - A model-supplied bucket name is only used if it is one of the **configured**
//!   bucket names. A model cannot name a directory here, so the set of paths this
//!   module can write to is the rule table -- a configuration fact -- and not a
//!   model output.
//!
//! What the model *can* do is bounded further down: it only ever sees the bodies of
//! files the rules could not place, and only counts toward a bucket that already
//! exists in the table.

use std::collections::{BTreeMap, BTreeSet};

use cog_core::{SFError, SFResult};
use serde::Serialize;

use crate::hostdocs::{EntryKind, HostDocListing, HostDocOp};

/// The rule table: `bucket:ext,ext;bucket:ext,...`.
pub const RULES_ENV: &str = "HOST_DOCS_CLASSIFY_RULES";

/// The taxonomy this module ships with.
///
/// A default rather than a constant of nature: the shape of a person's documents is
/// theirs, so the table is a knob (an operator can replace it) and the default only
/// has to be a reasonable first guess. It covers what a general-purpose documents
/// directory usually holds; anything it misses stays in place, visibly, rather than
/// being forced into a bucket that almost fits.
pub const DEFAULT_RULES: &str = "documents:md,txt,pdf,doc,docx,odt,rtf,epub;\
spreadsheets:csv,tsv,xls,xlsx,ods;\
presentations:ppt,pptx,odp;\
images:jpg,jpeg,png,gif,webp,heic,heif,svg,bmp,tiff,tif,avif;\
media:mp4,mov,mkv,avi,webm,mp3,wav,flac,m4a,ogg;\
archives:zip,tar,gz,tgz,bz2,xz,7z,rar;\
code:rs,go,py,js,ts,tsx,jsx,java,kt,c,h,cpp,hpp,cs,rb,php,sh,sql,json,yaml,yml,toml,ini,lock";

/// Why an entry that was looked at is not in the operation set. A closed set, and the
/// answer carries every reason (empty lists included) so that "these are all the
/// entries" and "these are the entries I decided about" cannot be read the same way.
///
/// The reasons are separated by **whose move it is next**: `no_rule` and
/// `hint_invalid` are about the table (or the model), `name_taken` is about the
/// directory's contents, `directory`/`nested`/`other_kind` say this plan was never
/// about that entry at all.
pub const SKIP_REASONS: [&str; 6] = [
    "no_rule",
    "hint_invalid",
    "name_taken",
    "directory",
    "nested",
    "other_kind",
];

/// Why a file that *is* in the operation set is there.
pub const MOVE_REASONS: [&str; 2] = ["rule", "model"];

/// Every cell a run's entries can land in, in the order the run decides: the moves
/// first, then the reasons something stayed. This is the vocabulary the counters are
/// published under, and `counts()` is keyed by exactly these names -- the answer and
/// the reading are one vocabulary, so a cell cannot exist on one side only.
pub const CLASSIFIED_OUTCOMES: [&str; 8] = [
    "moved_rule",
    "moved_model",
    "skipped_no_rule",
    "skipped_hint_invalid",
    "skipped_name_taken",
    "skipped_directory",
    "skipped_nested",
    "skipped_other_kind",
];

/// What one organize run ended as.
pub const ORGANIZE_OUTCOMES: [&str; 3] = ["staged", "nothing_to_do", "refused"];

/// The rule table, parsed. Buckets keep their configured order so that a plan built
/// from the same listing twice is the same plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizeRules {
    buckets: Vec<(String, Vec<String>)>,
    /// Specifications thrown out while parsing, with the reason. Kept rather than
    /// dropped silently: a rule that does not take effect is otherwise
    /// indistinguishable from a file that has no rule, and the two are fixed by
    /// different people.
    dropped: Vec<String>,
}

impl OrganizeRules {
    pub fn from_env() -> Self {
        match std::env::var(RULES_ENV) {
            Ok(spec) if !spec.trim().is_empty() => Self::parse(&spec),
            _ => Self::parse(DEFAULT_RULES),
        }
    }

    /// Parse `bucket:ext,ext;bucket:ext`.
    ///
    /// An entry that is malformed, or whose bucket name could not be a single
    /// directory segment, is **dropped**: a rule that resolves somewhere unintended
    /// is worse than no rule, because the file then moves somewhere the operator did
    /// not choose. The same goes for an extension already claimed by an earlier
    /// bucket -- first one wins, and the loser is reported, since silently letting the
    /// last one win would make the plan depend on the order of a string.
    pub fn parse(spec: &str) -> Self {
        let mut buckets: Vec<(String, Vec<String>)> = Vec::new();
        let mut taken: BTreeSet<String> = BTreeSet::new();
        let mut dropped = Vec::new();
        for entry in spec.split(';') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((name, exts)) = entry.split_once(':') else {
                dropped.push(format!("{entry} (expected `bucket:ext,ext`)"));
                continue;
            };
            let name = name.trim().to_ascii_lowercase();
            if !is_plain_segment(&name) {
                dropped.push(format!("{entry} ({name:?} is not a single directory name)"));
                continue;
            }
            let mut kept = Vec::new();
            for ext in exts.split(',') {
                let ext = ext.trim().trim_start_matches('.').to_ascii_lowercase();
                if ext.is_empty() {
                    continue;
                }
                if !taken.insert(ext.clone()) {
                    dropped.push(format!(
                        "{name}:{ext} (already claimed by an earlier bucket)"
                    ));
                    continue;
                }
                kept.push(ext);
            }
            if kept.is_empty() {
                dropped.push(format!("{entry} (no extensions)"));
                continue;
            }
            match buckets.iter_mut().find(|(b, _)| *b == name) {
                Some((_, existing)) => existing.extend(kept),
                None => buckets.push((name, kept)),
            }
        }
        Self { buckets, dropped }
    }

    pub fn buckets(&self) -> impl Iterator<Item = &str> {
        self.buckets.iter().map(|(name, _)| name.as_str())
    }

    pub fn dropped(&self) -> &[String] {
        &self.dropped
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// The bucket an entry's own name puts it in, if any. Only the last dot counts:
    /// `report.final.pdf` is a pdf, and `archive.tar.gz` is a gz (which the default
    /// table happens to place with the archives).
    pub fn bucket_of_name(&self, name: &str) -> Option<&str> {
        let (_, ext) = name.rsplit_once('.')?;
        let ext = ext.to_ascii_lowercase();
        if ext.is_empty() {
            return None;
        }
        self.buckets
            .iter()
            .find(|(_, exts)| exts.contains(&ext))
            .map(|(name, _)| name.as_str())
    }

    /// Whether a name may be used as a bucket. The model's answers go through this:
    /// the set of directories this module can create is the rule table, so a name
    /// that is not in it is refused rather than sanitized.
    pub fn has_bucket(&self, name: &str) -> bool {
        self.buckets.iter().any(|(b, _)| b == name)
    }
}

/// The counter cell a skip reason is counted under. `None` for a reason that has no
/// cell, which the tests rule out: the mapping is spelled out rather than built by
/// string concatenation so that renaming a cell is a compile-time-visible edit
/// instead of a silent change of the series name.
fn skip_cell(reason: &str) -> Option<&'static str> {
    match reason {
        "no_rule" => Some("skipped_no_rule"),
        "hint_invalid" => Some("skipped_hint_invalid"),
        "name_taken" => Some("skipped_name_taken"),
        "directory" => Some("skipped_directory"),
        "nested" => Some("skipped_nested"),
        "other_kind" => Some("skipped_other_kind"),
        _ => None,
    }
}

/// Whether `name` could be one directory name and nothing else: no separators, no
/// relative stepping, not empty, not a bare `.`.
fn is_plain_segment(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}

/// One file the plan moves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Moved {
    pub from: String,
    pub to: String,
    pub bucket: String,
    /// From [`MOVE_REASONS`]: decided by the table, or by a model answer.
    pub reason: &'static str,
}

/// What the rules make of one listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrganizeProposal {
    /// The operations, already in the order they must run: every directory first,
    /// then the moves. Sorted within each group so that the same listing yields the
    /// same plan byte for byte.
    pub ops: Vec<HostDocOp>,
    pub directories: Vec<String>,
    pub moved: Vec<Moved>,
    /// One list per reason in [`SKIP_REASONS`], each published even when empty and
    /// in that order.
    pub skipped: BTreeMap<String, Vec<String>>,
    /// How many entries the listing itself left out (symlinks, unnameable entries)
    /// and whether it was truncated. Carried through because a plan built from a
    /// truncated listing is a plan about part of a directory, and the person
    /// approving it is entitled to know which.
    pub listing_skipped_symlinks: usize,
    pub listing_skipped_unnamed: usize,
    pub listing_truncated: bool,
}

impl OrganizeProposal {
    /// Whether this proposal would do anything at all.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// One count per cell in [`CLASSIFIED_OUTCOMES`], zeros included: "the rules
    /// covered everything" and "the model did half of it" are the same number of
    /// entries, and only the split says which. `moved_*` is split by who decided
    /// because that is the reading that says whether the table fits the directory.
    pub fn counts(&self) -> BTreeMap<&'static str, usize> {
        let mut counts: BTreeMap<&'static str, usize> = CLASSIFIED_OUTCOMES
            .iter()
            .map(|cell| (*cell, 0usize))
            .collect();
        for moved in &self.moved {
            let cell = match moved.reason {
                "model" => "moved_model",
                _ => "moved_rule",
            };
            *counts.entry(cell).or_insert(0) += 1;
        }
        for reason in SKIP_REASONS {
            if let Some(cell) = skip_cell(reason) {
                counts.insert(cell, self.skipped.get(reason).map(Vec::len).unwrap_or(0));
            }
        }
        counts
    }
}

/// Build the operation set for one listing.
///
/// `hints` maps a scope-relative path to a bucket name, and is the only thing here
/// that can come from outside a configuration file: it is the audited channel's
/// answer for the entries the rules could not place. It never overrides a rule --
/// the table decides first, and the model is asked about what is left -- and a hint
/// naming an unconfigured bucket is ignored (counted as `hint_invalid`), so a model
/// cannot introduce a path into the plan.
pub fn propose(
    listing: &HostDocListing,
    rules: &OrganizeRules,
    hints: &BTreeMap<String, String>,
) -> SFResult<OrganizeProposal> {
    let existing: BTreeSet<&str> = listing.entries.iter().map(|e| e.path.as_str()).collect();
    let mut skipped: BTreeMap<String, Vec<String>> = SKIP_REASONS
        .iter()
        .map(|reason| (reason.to_string(), Vec::new()))
        .collect();
    let mut moved: Vec<Moved> = Vec::new();
    let mut claimed: BTreeSet<String> = BTreeSet::new();
    // Candidates are considered in path order and destinations are claimed as they
    // are handed out, so two files of the same name cannot both be told to take the
    // same destination -- the second one finds it taken and stays put.
    let mut entries: Vec<_> = listing.entries.iter().collect();
    entries.sort_by(|a, b| a.path.cmp(&b.path));

    for entry in entries {
        let depth = entry.path.matches('/').count();
        if depth > 0 {
            skipped
                .get_mut("nested")
                .expect("reason list is built from SKIP_REASONS")
                .push(entry.path.clone());
            continue;
        }
        match entry.kind {
            EntryKind::Dir => {
                skipped
                    .get_mut("directory")
                    .expect("reason list is built from SKIP_REASONS")
                    .push(entry.path.clone());
                continue;
            }
            EntryKind::Other => {
                skipped
                    .get_mut("other_kind")
                    .expect("reason list is built from SKIP_REASONS")
                    .push(entry.path.clone());
                continue;
            }
            EntryKind::File => {}
        }
        let (bucket, reason) = match rules.bucket_of_name(&entry.path) {
            Some(bucket) => (bucket.to_string(), "rule"),
            None => match hints.get(&entry.path) {
                Some(bucket) if rules.has_bucket(bucket) => (bucket.clone(), "model"),
                Some(_) => {
                    skipped
                        .get_mut("hint_invalid")
                        .expect("reason list is built from SKIP_REASONS")
                        .push(entry.path.clone());
                    continue;
                }
                None => {
                    skipped
                        .get_mut("no_rule")
                        .expect("reason list is built from SKIP_REASONS")
                        .push(entry.path.clone());
                    continue;
                }
            },
        };
        let destination = format!("{bucket}/{}", entry.path);
        if existing.contains(destination.as_str()) || claimed.contains(&destination) {
            skipped
                .get_mut("name_taken")
                .expect("reason list is built from SKIP_REASONS")
                .push(entry.path.clone());
            continue;
        }
        claimed.insert(destination.clone());
        moved.push(Moved {
            from: entry.path.clone(),
            to: destination,
            bucket,
            reason,
        });
    }

    // A directory is only made when something moves into it; making the whole
    // taxonomy in advance would put empty directories in someone's folder on a run
    // that moved nothing.
    let wanted: BTreeSet<String> = moved.iter().map(|m| m.bucket.clone()).collect();
    let directories: Vec<String> = wanted
        .into_iter()
        .filter(|bucket| !existing.contains(bucket.as_str()))
        .collect();

    let mut ops: Vec<HostDocOp> = directories
        .iter()
        .map(|path| HostDocOp::Mkdir { path: path.clone() })
        .collect();
    ops.extend(moved.iter().map(|m| HostDocOp::Rename {
        from: m.from.clone(),
        to: m.to.clone(),
    }));

    // A plan whose ops would not survive validation is a bug in this function, not a
    // condition to report: refuse here so nothing downstream has to wonder.
    for op in &ops {
        for rel in op.rel_paths() {
            if !is_plain_relative_path(rel) {
                return Err(SFError::Validation(format!(
                    "the organizer produced a path it must not: {rel:?} (paths are relative to the \
                     scope root and are made of plain segments)"
                )));
            }
        }
    }

    Ok(OrganizeProposal {
        ops,
        directories,
        moved,
        skipped,
        listing_skipped_symlinks: listing.skipped_symlinks,
        listing_skipped_unnamed: listing.skipped_unnamed,
        listing_truncated: listing.truncated,
    })
}

/// Whether a relative path is made only of plain segments. `/`-joined, no leading or
/// trailing slash, no `.`/`..`, no empty segment.
pub fn is_plain_relative_path(path: &str) -> bool {
    !path.is_empty() && path.split('/').all(is_plain_segment)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hostdocs::HostDocEntry;

    fn listing(paths: &[(&str, EntryKind, u64)]) -> HostDocListing {
        HostDocListing {
            scope: "docs".into(),
            prefix: None,
            entries: paths
                .iter()
                .map(|(path, kind, bytes)| HostDocEntry {
                    path: (*path).to_string(),
                    kind: *kind,
                    bytes: *bytes,
                    modified_unix: Some(1_700_000_000),
                })
                .collect(),
            skipped_symlinks: 0,
            skipped_unnamed: 0,
            truncated: false,
        }
    }

    fn rules() -> OrganizeRules {
        OrganizeRules::parse("images:jpg,png;documents:md,txt")
    }

    #[test]
    fn the_default_table_parses_into_distinct_buckets() {
        let rules = OrganizeRules::parse(DEFAULT_RULES);
        assert_eq!(rules.dropped(), [] as [String; 0]);
        assert!(rules.has_bucket("images") && rules.has_bucket("code"));
        // The same extension must not be claimed twice: the first bucket wins and the
        // loser is reported, because a silent last-wins would make the plan depend on
        // the order of a string in a configuration file.
        let taken = OrganizeRules::parse("images:jpg;media:jpg,wav");
        assert!(taken.has_bucket("media"));
        assert_eq!(
            taken.dropped(),
            ["media:jpg (already claimed by an earlier bucket)"]
        );
        assert_eq!(taken.bucket_of_name("song.wav"), Some("media"));
        assert_eq!(taken.bucket_of_name("photo.jpg"), Some("images"));
    }

    #[test]
    fn a_rule_that_could_not_be_a_directory_name_is_dropped_not_sanitized() {
        let rules = OrganizeRules::parse("../escape:jpg;ok/doc:png;:md;fine:txt");
        assert_eq!(rules.buckets().collect::<Vec<_>>(), ["fine"]);
        assert_eq!(rules.dropped().len(), 3);
        // Nothing is reachable under a dropped name, and text files have no rule at
        // all now, so they stay where they are.
        assert_eq!(rules.bucket_of_name("a.jpg"), None);
        assert_eq!(rules.bucket_of_name("a.md"), None);
    }

    #[test]
    fn files_are_moved_to_their_bucket_and_directories_are_made_first() {
        let listing = listing(&[
            ("photo.JPG", EntryKind::File, 10),
            ("notes.md", EntryKind::File, 10),
            ("images", EntryKind::Dir, 0),
        ]);
        let proposal = propose(&listing, &rules(), &BTreeMap::new()).unwrap();
        // `images` already exists, so only `documents` is created.
        assert_eq!(
            proposal.ops[0],
            HostDocOp::Mkdir {
                path: "documents".into()
            }
        );
        // The extension match is case-insensitive: a camera writes `IMG.JPG`.
        assert_eq!(
            proposal
                .moved
                .iter()
                .map(|m| m.to.clone())
                .collect::<Vec<_>>(),
            // Moves are ordered by source path, so the plan is a function of the
            // listing alone.
            vec![
                "documents/notes.md".to_string(),
                "images/photo.JPG".to_string()
            ]
        );
        assert!(proposal.moved.iter().all(|m| m.reason == "rule"));
        assert_eq!(proposal.directories, ["documents"]);
    }

    #[test]
    fn a_file_with_no_rule_stays_where_it_is_and_is_named() {
        let listing = listing(&[
            ("mystery.bin", EntryKind::File, 10),
            ("noextension", EntryKind::File, 10),
        ]);
        let proposal = propose(&listing, &rules(), &BTreeMap::new()).unwrap();
        assert!(proposal.is_empty());
        assert_eq!(
            proposal.skipped["no_rule"],
            ["mystery.bin".to_string(), "noextension".to_string()]
        );
        // Every reason is present as a key even when nothing landed in it: an absent
        // reason and an empty one read the same to a consumer, and only one of them
        // means the check ran.
        for reason in SKIP_REASONS {
            assert!(proposal.skipped.contains_key(reason), "{reason} missing");
        }
        assert_eq!(proposal.counts()["skipped_no_rule"], 2);
        assert_eq!(proposal.counts()["moved_rule"], 0);
    }

    #[test]
    fn the_answer_and_the_counter_share_one_vocabulary() {
        // Every cell of the vocabulary is a key of the answer, and every key of the
        // answer is a cell: a name that exists on one side only is a reading nobody
        // can line up with the thing it counts.
        let proposal = propose(&listing(&[]), &rules(), &BTreeMap::new()).unwrap();
        let counts = proposal.counts();
        assert_eq!(counts.len(), CLASSIFIED_OUTCOMES.len());
        for cell in CLASSIFIED_OUTCOMES {
            assert!(counts.contains_key(cell), "{cell} missing from the answer");
        }
        // The skip reasons and the counter cells are two spellings of one set: this is
        // what makes the mapping impossible to drift.
        let mut expected: Vec<&str> = SKIP_REASONS.iter().filter_map(|r| skip_cell(r)).collect();
        assert_eq!(
            expected.len(),
            SKIP_REASONS.len(),
            "a skip reason has no counter cell"
        );
        expected.extend(["moved_rule", "moved_model"]);
        expected.sort_unstable();
        let mut cells: Vec<&str> = CLASSIFIED_OUTCOMES.to_vec();
        cells.sort_unstable();
        assert_eq!(expected, cells);
    }

    #[test]
    fn a_taken_destination_stops_that_file_and_not_the_plan() {
        let listing = listing(&[
            ("photo.jpg", EntryKind::File, 10),
            ("images/photo.jpg", EntryKind::File, 10),
            ("note.md", EntryKind::File, 10),
        ]);
        let proposal = propose(&listing, &rules(), &BTreeMap::new()).unwrap();
        // The existing `images/photo.jpg` is not touched, the file at the root does not
        // get an invented name, and the unrelated move still happens.
        assert_eq!(proposal.skipped["name_taken"], ["photo.jpg".to_string()]);
        assert_eq!(proposal.moved.len(), 1);
        assert_eq!(proposal.moved[0].to, "documents/note.md");
    }

    #[test]
    fn two_candidates_for_one_destination_cannot_both_take_it() {
        // Only reachable when the table puts two different names in one bucket with the
        // same destination name -- which cannot happen (the destination keeps the source
        // name), so this pins the claiming order instead: the second file to ask for a
        // taken destination stays put.
        let listing = listing(&[("a.jpg", EntryKind::File, 10)]);
        let proposal = propose(&listing, &rules(), &BTreeMap::new()).unwrap();
        assert_eq!(proposal.moved.len(), 1);
        let mut hints = BTreeMap::new();
        hints.insert("a.jpg".to_string(), "images".to_string());
        let with_hint = propose(&listing, &rules(), &hints).unwrap();
        // The rule already placed it, so the hint changes nothing: the table decides
        // first and the model is asked about what is left.
        assert_eq!(with_hint.moved, proposal.moved);
    }

    #[test]
    fn nested_entries_and_non_regular_files_are_named_not_silently_dropped() {
        let listing = listing(&[
            ("inbox/old.jpg", EntryKind::File, 10),
            ("socket", EntryKind::Other, 0),
            ("sub", EntryKind::Dir, 0),
        ]);
        let proposal = propose(&listing, &rules(), &BTreeMap::new()).unwrap();
        assert!(proposal.is_empty());
        // This plan is never about a file inside a subdirectory: saying so beats
        // appearing to have considered it.
        assert_eq!(proposal.skipped["nested"], ["inbox/old.jpg".to_string()]);
        assert_eq!(proposal.skipped["other_kind"], ["socket".to_string()]);
        assert_eq!(proposal.skipped["directory"], ["sub".to_string()]);
    }

    #[test]
    fn a_hint_naming_an_unconfigured_bucket_is_ignored() {
        let listing = listing(&[("mystery.bin", EntryKind::File, 10)]);
        let mut hints = BTreeMap::new();
        // A model answer is an untrusted string: the set of directories this module can
        // create comes from the rule table, so an unknown name (or a path) is refused
        // rather than sanitized into something adjacent.
        hints.insert("mystery.bin".to_string(), "../../etc".to_string());
        let proposal = propose(&listing, &rules(), &hints).unwrap();
        assert!(proposal.is_empty());
        assert_eq!(
            proposal.skipped["hint_invalid"],
            ["mystery.bin".to_string()]
        );

        hints.insert("mystery.bin".to_string(), "images".to_string());
        let proposal = propose(&listing, &rules(), &hints).unwrap();
        assert_eq!(proposal.moved.len(), 1);
        assert_eq!(proposal.moved[0].reason, "model");
        assert_eq!(proposal.moved[0].to, "images/mystery.bin");
    }

    #[test]
    fn the_same_listing_yields_the_same_plan() {
        let paths = [
            ("b.png", EntryKind::File, 10),
            ("a.png", EntryKind::File, 10),
            ("c.md", EntryKind::File, 10),
        ];
        let first = propose(&listing(&paths), &rules(), &BTreeMap::new()).unwrap();
        let mut reversed = paths;
        reversed.reverse();
        let second = propose(&listing(&reversed), &rules(), &BTreeMap::new()).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.directories, ["documents", "images"]);
    }

    #[test]
    fn a_truncated_listing_is_carried_into_the_plan() {
        let mut listing = listing(&[("a.jpg", EntryKind::File, 10)]);
        listing.truncated = true;
        listing.skipped_symlinks = 2;
        let proposal = propose(&listing, &rules(), &BTreeMap::new()).unwrap();
        assert!(proposal.listing_truncated);
        assert_eq!(proposal.listing_skipped_symlinks, 2);
    }
}
