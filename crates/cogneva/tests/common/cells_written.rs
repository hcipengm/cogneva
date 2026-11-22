//! The cells a PromQL expression selects, answered against the strings the tree
//! writes.
//!
//! A rule fails silently when the cell it selects names a value nothing writes:
//! nothing matches, so nothing fires, and the failure reads as "all quiet". The
//! name census stops at the brace, so the label value has never been part of
//! that contract. The dashboard contract asked the same question of panels while
//! it existed, which is why the answer lives here rather than inside the rule
//! contract: one function instead of a copy on each side that drifts apart.
//!
//! Included with `#[path]` rather than through `common/mod.rs`, like the other
//! helpers here: it depends on the PromQL extractor and on `fsio` /
//! `rust_production`, which each caller declares alongside it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::common::declared_once::literals_in;
use crate::common::{fsio, rust_production};
use crate::promql::selected_cells;

/// Every string literal the tree writes.
///
/// The production sources of each crate, plus the files `sources` names for a
/// series whose producer sits outside one -- minus anything under `deploy/`.
///
/// No file there may license a cell. Every rule is written under `deploy/`, and
/// the chart's rules are rendered into configmap copies beside them, so a
/// literal read from `deploy/` can be the reader answering its own question;
/// the check would then never once answer anything but yes. A series registered
/// with a deploy path is registered so its producer can be found, not so the
/// values in that file count as written.
pub fn strings_the_tree_writes(
    root: &Path,
    sources: impl IntoIterator<Item = PathBuf>,
) -> BTreeSet<String> {
    let mut files = rust_production::production_sources(root);
    files.extend(sources);

    let mut out = BTreeSet::new();
    for path in files {
        if path
            .strip_prefix(root)
            .map(|relative| relative.starts_with("deploy"))
            .unwrap_or(false)
        {
            continue;
        }
        let text = fsio::read(&path);
        for literal in literals_in(&text) {
            out.insert(literal.value.to_string());
        }
    }
    out
}

/// The cells selected from a registered series whose value no file in the tree
/// writes, one line per cell, prefixed by the rule that selects it.
///
/// `readers` is `(where it is written, the expression)`; series the caller did
/// not register are not judged, because their label values belong to whoever
/// exposes them.
pub fn cells_no_one_writes(
    readers: &[(String, String)],
    produced: &BTreeSet<&str>,
    written: &BTreeSet<String>,
) -> Vec<String> {
    let mut out = Vec::new();
    for (reader, promql) in readers {
        for (series, label, value) in selected_cells(promql) {
            if !produced.contains(series.as_str()) || written.contains(&value) {
                continue;
            }
            out.push(format!("{reader}: {series}{{{label}=\"{value}\"}}"));
        }
    }
    out
}
