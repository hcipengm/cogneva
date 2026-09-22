//! Guards the retired-name list against its own dangerous direction.
//!
//! The list names series that may be deleted from every store a metric lives in
//! and that must no longer be served. Its harmless mistake is forgetting to add a
//! name — the behaviour then is what it would have been without the list at all.
//! Its harmful mistake is naming something that is still produced: the release
//! deletes a live series' current value, and the exposition stops serving it,
//! both of which look like a metric that simply went quiet.
//!
//! What separates the two is whether anything else in the tree still spells the
//! name out. A name that comes back — in code, in a manifest, in a dashboard —
//! means the retirement was premature or the rename was undone, and either way
//! the release has no business running. So the literal is allowed in exactly one
//! place, the declaration, and this walks the tree to hold that.

use std::path::{Path, PathBuf};

/// The one file allowed to spell a retired name out.
const DECLARATION: &str = "crates/cog-core/src/contract/observability.rs";

/// Extensions worth reading. Source, manifests and the things that quote series
/// names — a dashboard or an alert rule naming a retired series is a claim about
/// a reading that no longer arrives, which is the same defect from the other
/// side. Anything else is data or a binary and cannot record a metric.
const SCANNED_EXTENSIONS: &[&str] = &[
    "rs", "toml", "json", "yaml", "yml", "sh", "sql", "md", "html", "js", "proto",
];

/// Directories that are neither source nor manifests: build output, version
/// control internals, orchestration state, and vendored frontend trees.
const SKIPPED_DIRECTORIES: &[&str] = &["target", ".git", ".omc", "node_modules", "dist", ".venv"];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("cog-core lives two levels under the workspace root")
        .to_path_buf()
}

/// Every readable text file under `root`, as paths relative to it.
///
/// Symlinks are skipped rather than followed: the ones in this tree point out of
/// it, and a file reachable only through a link is not part of what this
/// workspace builds or deploys.
fn scanned_files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        let entries = std::fs::read_dir(&directory)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", directory.display()));
        for entry in entries {
            let entry =
                entry.unwrap_or_else(|e| panic!("cannot read {}: {e}", directory.display()));
            let path = entry.path();
            let file_type = entry
                .file_type()
                .unwrap_or_else(|e| panic!("cannot stat {}: {e}", path.display()));
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                if !SKIPPED_DIRECTORIES.contains(&name.as_str()) {
                    stack.push(path);
                }
                continue;
            }
            let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if SCANNED_EXTENSIONS.contains(&extension) {
                found.push(
                    path.strip_prefix(root)
                        .expect("walked paths are built from the root")
                        .to_path_buf(),
                );
            }
        }
    }
    found
}

#[test]
fn a_retired_name_is_spelled_out_in_exactly_one_place() {
    let root = workspace_root();
    let declaration = std::fs::read_to_string(root.join(DECLARATION))
        .unwrap_or_else(|e| panic!("cannot read {DECLARATION}: {e}"));

    let scanned = scanned_files(&root);
    assert!(
        scanned.len() > 100,
        "only {} files were scanned, which is too few for this to be the workspace \
         — the walk is not looking where it thinks it is",
        scanned.len()
    );

    for name in cog_core::RETIRED_METRIC_NAMES {
        assert!(
            declaration.contains(name),
            "{name} is declared retired but its literal is not in {DECLARATION}"
        );

        let mut offenders = Vec::new();
        for relative in &scanned {
            if relative == Path::new(DECLARATION) {
                continue;
            }
            let text = std::fs::read_to_string(root.join(relative))
                .unwrap_or_else(|e| panic!("cannot read {} as text: {e}", relative.display()));
            if text.contains(name) {
                offenders.push(relative.display().to_string());
            }
        }

        assert!(
            offenders.is_empty(),
            "{name} is declared retired and still named in {offenders:?} — a name that comes \
             back would have its live rows deleted and its series hidden from the scrape, both \
             of which read as a metric that went quiet"
        );
    }
}
