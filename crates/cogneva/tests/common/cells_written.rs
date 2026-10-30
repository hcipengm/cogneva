//! The cells a PromQL expression selects, answered against the strings the tree
//! writes.
//!
//! A rule and a panel fail the same way when the cell they select names a value
//! nothing writes: nothing matches, so nothing fires and nothing draws, and
//! both read as "all quiet". The name census on each side stops at the brace,
//! so the label value has never been part of either contract, and the two
//! surfaces drift for the same reason. The question is answered once here with
//! two call sites rather than twice with two copies that drift apart -- which
//! is the failure this module exists to catch, in the thing that catches it.
//!
//! Included with `#[path]` rather than through `common/mod.rs`, like the other
//! helpers here: it depends on the PromQL extractor and on `fsio` /
//! `rust_production`, which each caller declares alongside it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::common::{fsio, rust_production};
use crate::promql::selected_cells;

/// Every string literal the tree writes.
///
/// The production sources of each crate, plus the files `sources` names for a
/// series whose producer sits outside one -- minus anything under `deploy/`.
///
/// No file there may license a cell. Every rule and every panel is written
/// under `deploy/`, and the chart's rules are rendered into configmap copies
/// beside them, so a literal read from `deploy/` can be the reader answering its
/// own question; the check would then never once answer anything but yes. A
/// series registered with a deploy path is registered so its producer can be
/// found, not so the values in that file count as written.
pub fn literals_the_tree_writes(
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
        let mut rest = text.as_str();
        while let Some(open) = rest.find('"') {
            let after = &rest[open + 1..];
            let Some(close) = after.find('"') else {
                break;
            };
            out.insert(after[..close].to_string());
            rest = &after[close + 1..];
        }
    }
    out
}

/// The cells selected from a registered series whose value no file in the tree
/// writes, one line per cell, prefixed by the rule or panel that selects it.
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
