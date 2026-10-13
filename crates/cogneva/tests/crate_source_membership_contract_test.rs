//! Gate: a `.rs` under `crates/` is code only if a workspace member compiles it.
//!
//! Every `.rs` that the tree carries under `crates/<dir>/` has to belong to a
//! crate root `cargo metadata` resolves as a workspace member. A directory that
//! looks like a crate — a `src/`, a `.rs` inside it — but is not a member is
//! the shape a change takes when the side that wrote it mistook something for a
//! crate path: the file reads as landed, and no compiler ever sees it. Nothing
//! else in this repository asks the question, because every other gate starts
//! from a manifest or from a symbol, and this shape has neither.
//!
//! The gate is deliberately about the *directory*, not about a file list: a
//! member is whatever `cargo metadata` says it is, so the assertion cannot go
//! stale against the manifest the way a hand-kept list of crates would.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The repository root, derived from this crate's own manifest so the test does
/// not depend on the working directory it is run from.
fn workspace_root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::canonicalize(&root).unwrap_or(root)
}

/// The directories `cargo metadata` resolves as workspace members.
///
/// Read through cargo rather than by parsing the manifest: membership is what
/// cargo *resolves* — `exclude`, a nested workspace and a default-members list
/// all live outside the `members` array a text scan would look at — and the
/// question here is about the build, not about a table.
fn member_roots() -> BTreeSet<PathBuf> {
    let manifest = workspace_root().join("Cargo.toml");
    let out = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
        ])
        .arg(&manifest)
        .output()
        .expect("cargo metadata must be runnable from a test binary");
    assert!(
        out.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let doc: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("cargo metadata must emit JSON");
    let packages = doc["packages"]
        .as_array()
        .expect("cargo metadata carries packages");

    let mut roots = BTreeSet::new();
    for package in packages {
        let path = PathBuf::from(
            package["manifest_path"]
                .as_str()
                .expect("a package has a manifest path"),
        );
        let dir = path
            .parent()
            .expect("a manifest path has a directory")
            .to_path_buf();
        // cargo reports absolute paths; the walk below builds its own from the
        // same root, and `..` left in one side would make equal directories
        // compare unequal.
        roots.insert(std::fs::canonicalize(&dir).unwrap_or(dir));
    }
    roots
}

/// Every `.rs` under `crates/`, as a repo-relative path.
fn rust_sources_under_crates() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // `DirEntry::file_type` does not follow symlinks: a link out of the
            // tree is not a file this repository carries.
            match entry.file_type() {
                Ok(t) if t.is_dir() => walk(&path, out),
                Ok(t) if t.is_file() && path.extension().is_some_and(|e| e == "rs") => {
                    out.push(path);
                }
                _ => {}
            }
        }
    }

    let mut found = Vec::new();
    walk(&workspace_root().join("crates"), &mut found);
    found.sort();
    found
}

/// The crate root a source file under `crates/<dir>/...` belongs to.
fn crate_root_of(path: &Path) -> Option<PathBuf> {
    let root = workspace_root();
    let rel = path.strip_prefix(&root).ok()?;
    let mut parts = rel.components();
    let crates_dir = parts.next()?;
    assert_eq!(
        crates_dir.as_os_str(),
        "crates",
        "only files under crates/ are walked"
    );
    let crate_dir = parts.next()?;
    Some(root.join("crates").join(crate_dir))
}

#[test]
fn every_rust_source_under_crates_is_compiled_by_a_workspace_member() {
    let roots = member_roots();
    assert!(
        roots.len() > 1,
        "cargo metadata resolved {} workspace members, which cannot be right — \
         an empty or one-member answer would make this gate pass by having \
         nothing to compare against",
        roots.len()
    );

    let sources = rust_sources_under_crates();
    assert!(
        sources.len() > 100,
        "the walk found {} Rust sources under crates/, far fewer than the tree \
         carries — a walk that finds nothing would satisfy this gate vacuously",
        sources.len()
    );

    let orphans: Vec<String> = sources
        .iter()
        .filter(|path| crate_root_of(path).is_none_or(|root| !roots.contains(&root)))
        .map(|path| path.to_string_lossy().into_owned())
        .collect();

    assert!(
        orphans.is_empty(),
        "Rust sources under crates/ that no workspace member compiles: {orphans:?} — a file \
         here reads as landed while no compiler ever sees it, so either the crate it was \
         meant for is missing from the workspace members, or the directory is not a crate at \
         all and the file reached `main` without passing a build"
    );
}
