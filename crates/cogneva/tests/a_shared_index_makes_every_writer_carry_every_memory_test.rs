//! The merged suite is one index, and a file that nobody registers ships a
//! test nothing runs, which reads exactly like a file that has no tests in it.
//!
//! `the_suite_and_the_directory_agree` owns the whole census; this file asks the
//! narrower question the shared index makes answerable: every path the registry
//! names has to resolve to a file that is really there, and no path may be named
//! twice -- a second row for one file is a second writer landing on an index key
//! it does not own.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// The registry the merged suite is assembled from.
fn suite_source() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/suite.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Every `#[path = "..."]` target the registry names, in the order it names them.
fn registered_paths(source: &str) -> Vec<&str> {
    let mut out = Vec::new();
    for line in source.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("#[path = \"") else {
            continue;
        };
        let Some(end) = rest.find('"') else {
            continue;
        };
        out.push(&rest[..end]);
    }
    out
}

/// Where `#[path]` resolves a registered target: relative to this directory.
fn registered_file(target: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(target)
}

#[test]
fn the_registry_names_no_file_twice() {
    let source = suite_source();
    let paths = registered_paths(&source);
    assert!(!paths.is_empty(), "no file is registered");

    let mut seen: HashSet<&str> = HashSet::new();
    for path in paths {
        assert!(seen.insert(path), "{path} is registered twice");
    }
}

#[test]
fn every_registered_file_is_there() {
    let source = suite_source();
    let paths = registered_paths(&source);

    let missing: Vec<&str> = paths
        .iter()
        .copied()
        .filter(|target| !registered_file(target).is_file())
        .collect();
    assert!(
        missing.is_empty(),
        "the suite registers files that are not on disk: {missing:?}"
    );
}
