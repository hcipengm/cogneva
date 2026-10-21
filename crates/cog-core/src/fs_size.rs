//! Byte totals of a directory tree, and the layers those bytes fall into.
//!
//! Two readings measure a directory this way: the footprint of a claim-backed
//! volume, and the size of the shared build cache. They have to agree on what
//! "the size of this directory" means. The cap the cache is held against comes
//! from the same number a human reads, and a second walker with its own idea of
//! symlinks or of what to exclude would report a different size for one tree
//! with nothing in either reading to explain the difference. Both read this
//! module.
//!
//! Apparent size (file length), not allocated blocks: it is the amount of data
//! that was written, which is what a declared size, a cap and a growth rate are
//! about. Block accounting adds per-file allocation slack that a directory of
//! many small files inflates without anything having grown.
//!
//! Symlinks are neither counted nor descended into: their targets may live
//! outside the measured tree, and following them can revisit a directory
//! forever. Directory entries are read without following links, so a symlink is
//! skipped by being neither a regular file nor a directory.
//!
//! Hardlinked files are counted once, under the first name of them in path
//! order, because the bytes are on the volume once: counting every name would
//! report more than the directory holds, and a cap held against that total
//! could be met by removing a name that frees nothing. The other names are
//! reported as aliases of the one that counts. cargo makes such a link whenever
//! it lifts an artifact out of `deps` up into the profile directory, so a build
//! cache read any other way reads high by the size of everything it built.
//!
//! Where the filesystem does not report a file's identity, every path stands
//! for a file of its own, since two names cannot be told apart without it.

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// The layer of files that sit directly in the measured directory.
///
/// A name rather than an empty string: the value travels as a label, and a
/// series labelled with nothing is one a reader cannot tell from a label that
/// was never set.
pub const ROOT_LAYER: &str = ".";

/// Apparent bytes of every regular file below `dir`, leaving out the paths in
/// `exclude` and everything below them.
///
/// Defined as the sum of [`dir_layers`] rather than as a second walk, so the
/// total and the breakdown of one directory cannot drift apart.
pub fn dir_size_bytes(dir: &Path, exclude: &[PathBuf]) -> io::Result<u64> {
    Ok(dir_layers(dir, 1, exclude)?.values().sum())
}

/// Identity of a file, as opposed to identity of a name: two entries with the
/// same id are two names of one file on disk, and its bytes are on the volume
/// once however many names it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId {
    pub dev: u64,
    pub ino: u64,
}

/// One regular file a walk found, with the layer it belongs to.
///
/// Materialised rather than summed because the decision this feeds is about the
/// set: how much a cache has to give up depends on what every layer holds and on
/// what has to stay, which a running total cannot answer. The size and the
/// timestamp are the ones the walk read, so a plan built from these entries is
/// about the tree state that was measured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Path as walked, below the directory the walk started at.
    pub path: PathBuf,
    /// The layer the file falls into, at the depth the walk was given.
    pub layer: String,
    /// Apparent size in bytes.
    pub len: u64,
    /// Last modification time, when the filesystem reports one.
    pub modified: Option<SystemTime>,
    /// The file this path names, where the filesystem reports it.
    pub id: Option<FileId>,
    /// How many names this file has, where the filesystem reports it. A file
    /// with more names than the walk saw has one outside the measured tree.
    pub links: Option<u64>,
    /// Set by [`dir_files`]: another name of this same file is the one its
    /// bytes are counted under, so these are reported as stored and must not be
    /// added to a total again. Removing this name alone frees nothing.
    pub alias: bool,
}

/// Every regular file below `dir`, leaving out the paths in `exclude` and
/// everything below them.
///
/// Same traversal as [`dir_layers`], and the same answers about symlinks,
/// exclusions and unreadable directories: a total and the files it was summed
/// from have to come from one walk, or a decision made against the files is
/// made against a tree state nobody measured.
///
/// One name of each file is marked as the one that counts, and it is the first
/// in path order rather than the first the filesystem happened to list: which
/// layer holds an artifact's bytes must not change between two walks of an
/// unchanged tree.
pub fn dir_files(dir: &Path, depth: usize, exclude: &[PathBuf]) -> io::Result<Vec<FileEntry>> {
    let mut files = Vec::new();
    walk(dir, depth, exclude, |entry| files.push(entry))?;
    mark_aliases(&mut files);
    Ok(files)
}

/// Name the entry per file whose bytes count, and mark the rest as aliases.
fn mark_aliases(files: &mut [FileEntry]) {
    let mut counted: BTreeMap<FileId, usize> = BTreeMap::new();
    for (index, file) in files.iter().enumerate() {
        let Some(id) = file.id else {
            continue;
        };
        match counted.get(&id) {
            Some(&held) if files[held].path <= file.path => {}
            _ => {
                counted.insert(id, index);
            }
        }
    }
    for (index, file) in files.iter_mut().enumerate() {
        if let Some(id) = file.id {
            file.alias = counted.get(&id) != Some(&index);
        }
    }
}

/// Apparent bytes below `dir`, totalled per layer, where a layer is the first
/// `depth` components of a file's directory below `dir`.
///
/// The caller picks the depth because only the caller knows which division
/// answers its question: 1 gives one entry per top-level child, and 2 separates
/// a profile directory's own files from each cache below it, which is the
/// granularity that says which cache to drop. A file shallower than `depth`
/// belongs to the directory that holds it — the layers are the ones the tree
/// really has, not padding towards the requested depth.
///
/// A path whose components are not valid UTF-8 is attributed to its deepest
/// valid prefix. The bytes are still counted; only the layer name is coarser,
/// which is the direction a reader can see.
///
/// Apparent bytes of every regular file below `dir`, the same total
/// [`dir_size_bytes`] gives, folded as the walk goes.
///
/// Defined on the same traversal and with the same answers about symlinks and
/// exclusions, because two sizes for one tree is the failure both walkers exist
/// to prevent. What differs is what the walk is allowed to keep: this one holds
/// only the identities of the files it has already counted, so its cost is a
/// set of `(dev, ino)` pairs rather than an entry per file. That is the reading
/// a tree of millions of files needs -- a container image store is one -- where
/// materialising every file costs more than the measurement is worth and gets
/// the process killed by its own memory limit before it answers.
///
/// Same number, not merely a similar one: [`dir_size_bytes`] counts one name
/// per file, and which name counts is fixed by path order only so that two
/// walks of an unchanged tree agree on the layer split. A total that keeps the
/// first identity it meets reaches the same sum, because the choice of name
/// moves bytes between layers and never in or out of the tree.
///
/// A file whose identity the filesystem does not report is counted under each
/// name, the same as there.
pub fn dir_bytes_counted_once(dir: &Path, exclude: &[PathBuf]) -> io::Result<CountedOnce> {
    let mut seen: HashSet<FileId> = HashSet::new();
    let mut bytes: u64 = 0;
    let vanished = walk(dir, 1, exclude, |entry| {
        if let Some(id) = entry.id {
            if !seen.insert(id) {
                return;
            }
        }
        bytes = bytes.saturating_add(entry.len);
    })?;
    Ok(CountedOnce { bytes, vanished })
}

/// A streamed total, with the count of entries that left the tree while it was
/// being walked.
///
/// The count is part of the answer rather than a diagnostic beside it: an entry
/// that vanished is not bytes the walk failed to see -- it is bytes the tree no
/// longer holds -- so the total is right for the tree as it stood and short of
/// the tree as it was when the walk began. A reader comparing this number with
/// an earlier one needs to know which of the two it got, and a walk that reports
/// only the total cannot tell them apart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CountedOnce {
    /// Apparent bytes of the files the walk counted.
    pub bytes: u64,
    /// Entries removed between being listed and being read. Their bytes are in
    /// neither this total nor on the disk.
    pub vanished: usize,
}

/// A layer whose files are all aliases of files counted elsewhere is reported
/// with the zero bytes it holds rather than left out: the names are there, and
/// a layer that vanishes from the reading is one a reader cannot tell from a
/// layer the walk could not see.
///
/// An unreadable subdirectory fails the whole walk. The caller is expected to
/// keep its last measurement: a partial total is smaller than the truth, and a
/// smaller cache reading can only silence a cap.
pub fn dir_layers(
    dir: &Path,
    depth: usize,
    exclude: &[PathBuf],
) -> io::Result<BTreeMap<String, u64>> {
    Ok(layer_totals(&dir_files(dir, depth, exclude)?))
}

/// The layer totals of one walk's entries.
///
/// The fold [`dir_layers`] is defined as, exposed for a caller that already has
/// the entries and has to act on them: it totals them by the same rule instead
/// of writing a second one that could drift from the first.
pub fn layer_totals(files: &[FileEntry]) -> BTreeMap<String, u64> {
    let mut totals: BTreeMap<String, u64> = BTreeMap::new();
    for entry in files {
        let total = totals.entry(entry.layer.clone()).or_default();
        if !entry.alias {
            *total = total.saturating_add(entry.len);
        }
    }
    totals
}

/// Apparent bytes of one walk's entries, counted once per file.
///
/// The same number [`dir_size_bytes`] reads off the disk, from entries a caller
/// is already holding: the cap and the plan that enforces it are about one
/// total, and neither of them needs a second walk to have it.
pub fn counted_bytes(files: &[FileEntry]) -> u64 {
    files
        .iter()
        .filter(|entry| !entry.alias)
        .fold(0u64, |acc, entry| acc.saturating_add(entry.len))
}

/// How many of one walk's entries were written within `window_secs` of
/// `now_unix_secs`, totalled by the same layer rule as [`layer_totals`].
///
/// Files, not bytes: a build that recompiled a crate rewrites the same artifacts
/// it wrote last time, so the bytes barely move while the count says how much of
/// the cache was rebuilt. That is the reading that separates a hot build from a
/// cold one, and it comes off the walk the size already needs rather than a
/// second pass over the tree.
///
/// An entry with no modification time is not counted: it is not evidence that
/// nothing was written there, only that nothing can be said about it. Aliases
/// are skipped for the reason [`counted_bytes`] skips them -- one file written
/// once is one write, however many names it has.
pub fn recently_written_totals(
    files: &[FileEntry],
    now_unix_secs: u64,
    window_secs: u64,
) -> BTreeMap<String, u64> {
    let cutoff = now_unix_secs.saturating_sub(window_secs);
    let mut totals: BTreeMap<String, u64> = BTreeMap::new();
    for entry in files {
        if entry.alias {
            continue;
        }
        let Some(modified) = entry.modified else {
            continue;
        };
        let written_at = modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|since| since.as_secs())
            .unwrap_or(0);
        if written_at < cutoff {
            continue;
        }
        let total = totals.entry(entry.layer.clone()).or_default();
        *total = total.saturating_add(1);
    }
    totals
}

/// An I/O error that keeps its kind and names the path it happened on.
///
/// The caller logs one line when a tree cannot be walked, and "permission
/// denied" without a path names neither the file nor the remedy: the reading's
/// own account of what it could not measure is the only thing a person has.
fn annotate(e: io::Error, path: &Path) -> io::Error {
    io::Error::new(e.kind(), format!("{}: {e}", path.display()))
}

/// The one traversal both readings are built from.
///
/// Returns how many entries were listed and then found gone: a caller that
/// publishes a total has to be able to say the tree moved while it was read.
fn walk(
    dir: &Path,
    depth: usize,
    exclude: &[PathBuf],
    mut on_file: impl FnMut(FileEntry),
) -> io::Result<usize> {
    let mut vanished = 0usize;
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        // Read whole and in name order: which name of a file comes first decides
        // which layer counts its bytes, and a directory listing is not ordered.
        let listing = match std::fs::read_dir(&current) {
            Ok(listing) => listing,
            // Gone between being listed and being read, rather than unreadable:
            // every directory below the root was seen in its parent's listing,
            // and a tree written while it is walked -- a container runtime's
            // snapshot workspace, a build cache -- removes directories under
            // the walk. Failing the whole tree over a directory that no longer
            // exists reports a reading of nothing, and its bytes are not hidden
            // by leaving them out: they are gone.
            //
            // The root is the exception, and it is why the caller can tell a
            // missing tree from an empty one: what was asked for has to be
            // there.
            Err(e) if e.kind() == io::ErrorKind::NotFound && current.as_path() != dir => {
                vanished += 1;
                continue;
            }
            Err(e) => return Err(annotate(e, &current)),
        };
        let mut children = Vec::new();
        for entry in listing {
            match entry {
                Ok(entry) => children.push(entry),
                Err(e) if e.kind() == io::ErrorKind::NotFound => vanished += 1,
                Err(e) => return Err(annotate(e, &current)),
            }
        }
        children.sort_by_key(|entry| entry.file_name());
        for entry in children {
            let path = entry.path();
            if exclude.iter().any(|excluded| excluded == &path) {
                continue;
            }
            // Without following links: a symlink to a directory would otherwise
            // be descended into, and one to a file counted at its target's size.
            let meta = match entry.metadata() {
                Ok(meta) => meta,
                // The same rule for one entry: it was removed after this
                // directory was listed.
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    vanished += 1;
                    continue;
                }
                Err(e) => return Err(annotate(e, &path)),
            };
            if meta.is_dir() {
                pending.push(path);
            } else if meta.is_file() {
                on_file(FileEntry {
                    layer: layer_of(dir, &path, depth),
                    path,
                    len: meta.len(),
                    // Absent when the filesystem does not report one. Kept as an
                    // absence rather than a substituted epoch: a reclamation
                    // rule that ranks by age has to be able to tell "built long
                    // ago" from "age unknown".
                    modified: meta.modified().ok(),
                    id: file_id(&meta),
                    links: link_count(&meta),
                    // Set by `dir_files`, which is the only thing that can see
                    // two names of one file at once.
                    alias: false,
                });
            }
        }
    }
    Ok(vanished)
}

/// The file a directory entry names, where the filesystem reports it.
#[cfg(unix)]
fn file_id(meta: &std::fs::Metadata) -> Option<FileId> {
    use std::os::unix::fs::MetadataExt;
    Some(FileId {
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

#[cfg(not(unix))]
fn file_id(_meta: &std::fs::Metadata) -> Option<FileId> {
    None
}

/// How many names the file has, where the filesystem reports it.
#[cfg(unix)]
fn link_count(meta: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(meta.nlink())
}

#[cfg(not(unix))]
fn link_count(_meta: &std::fs::Metadata) -> Option<u64> {
    None
}

/// The layer a file belongs to, as a path relative to `dir`.
fn layer_of(dir: &Path, file: &Path, depth: usize) -> String {
    let Some(relative) = file.strip_prefix(dir).ok() else {
        return ROOT_LAYER.to_string();
    };
    let mut parts: Vec<&str> = Vec::new();
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            let Some(name) = component.as_os_str().to_str() else {
                break;
            };
            parts.push(name);
        }
    }
    if parts.is_empty() || depth == 0 {
        return ROOT_LAYER.to_string();
    }
    parts.truncate(depth);
    parts.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// A scratch tree that removes itself when the test ends. The first version
    /// returned the path alone and never removed it: one `cargo test` run left
    /// eleven trees behind, and on a host that runs the suite all day that was
    /// thousands of directories nothing owned and nothing could name.
    fn scratch(name: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("cog-fssize-{name}-"))
            .tempdir()
            .unwrap()
    }

    fn write(path: &Path, bytes: usize) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    /// The file walk and the layer totals are one traversal seen two ways: a
    /// plan built from the entries has to be a plan against the total that was
    /// published beside them.
    #[test]
    fn the_file_walk_totals_to_the_same_bytes_as_the_layers() {
        let root = scratch("files");
        let root = root.path();
        write(&root.join("debug/deps/lib.rlib"), 100);
        write(&root.join("debug/incremental/obj"), 30);
        write(&root.join("tmp/scratch"), 4);

        let files = dir_files(root, 2, &[]).unwrap();
        assert_eq!(files.len(), 3);
        let per_layer: BTreeMap<String, u64> = files.iter().fold(BTreeMap::new(), |mut acc, f| {
            *acc.entry(f.layer.clone()).or_default() += f.len;
            acc
        });
        assert_eq!(per_layer, dir_layers(root, 2, &[]).unwrap());
        assert_eq!(
            files.iter().map(|f| f.len).sum::<u64>(),
            dir_size_bytes(root, &[]).unwrap()
        );
        for file in &files {
            assert!(file.path.starts_with(root), "{file:?}");
            assert!(file.modified.is_some(), "{file:?}");
        }
        // The same exclusions and the same symlink rules as the totals.
        let exclude = vec![root.join("tmp")];
        assert_eq!(dir_files(root, 2, &exclude).unwrap().len(), 2);
    }

    /// The total is the sum of the layers: one walk answers both readings, so a
    /// cap held against the total cannot be enforced against a breakdown that
    /// was taken from a different tree state.
    #[test]
    fn the_total_is_the_sum_of_the_layers() {
        let root = scratch("total");
        let root = root.path();
        write(&root.join("debug/deps/lib.rlib"), 100);
        write(&root.join("debug/incremental/obj"), 30);
        write(&root.join("release/deps/lib.rlib"), 7);
        write(&root.join("CACHEDIR.TAG"), 1);

        let layers = dir_layers(root, 1, &[]).unwrap();
        assert_eq!(layers.get("debug"), Some(&130));
        assert_eq!(layers.get("release"), Some(&7));
        assert_eq!(layers.get(ROOT_LAYER), Some(&1));
        assert_eq!(
            dir_size_bytes(root, &[]).unwrap(),
            layers.values().sum::<u64>()
        );
        assert_eq!(dir_size_bytes(root, &[]).unwrap(), 138);
    }

    /// The depth is what separates a profile directory's own files from the
    /// caches below it, which is the decision the build cache reading feeds.
    #[test]
    fn the_depth_decides_which_layers_are_reported() {
        let root = scratch("depth");
        let root = root.path();
        write(&root.join("debug/deps/lib.rlib"), 100);
        write(&root.join("debug/cogneva"), 40);
        write(&root.join("debug/incremental/obj"), 30);

        let shallow = dir_layers(root, 1, &[]).unwrap();
        assert_eq!(shallow.get("debug"), Some(&170));

        let deep = dir_layers(root, 2, &[]).unwrap();
        assert_eq!(deep.get("debug/deps"), Some(&100));
        assert_eq!(deep.get("debug/incremental"), Some(&30));
        assert_eq!(deep.get("debug"), Some(&40));
        // A deeper request that the tree cannot fill stops at what exists.
        assert_eq!(dir_layers(root, 9, &[]).unwrap(), deep);
    }

    /// A file directly in the measured directory belongs to the directory
    /// itself, not to a layer invented to hold it.
    #[test]
    fn a_file_in_the_root_belongs_to_the_root_layer() {
        let root = scratch("rootfile");
        let root = root.path();
        write(&root.join("CACHEDIR.TAG"), 5);
        let layers = dir_layers(root, 2, &[]).unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(layers.get(ROOT_LAYER), Some(&5));
    }

    /// An excluded directory is neither counted nor descended into: a mount
    /// inside the measured tree is another volume's bytes, and counting them
    /// here would report them against both.
    #[test]
    fn an_excluded_subtree_is_left_out() {
        let root = scratch("exclude");
        let root = root.path();
        write(&root.join("kept/a"), 10);
        write(&root.join("nested/b"), 900);
        let exclude = vec![root.join("nested")];

        let layers = dir_layers(root, 1, &exclude).unwrap();
        assert_eq!(layers.get("kept"), Some(&10));
        assert_eq!(layers.get("nested"), None);
        assert_eq!(dir_size_bytes(root, &exclude).unwrap(), 10);
    }

    /// A symlink is not a file to measure and not a directory to walk: its
    /// target is outside the tree, or behind it.
    #[cfg(unix)]
    #[test]
    fn a_symlink_is_neither_counted_nor_followed() {
        let root = scratch("symlink");
        let root = root.path();
        let outside = scratch("symlink-target");
        let outside = outside.path();
        write(&outside.join("big"), 4096);
        write(&root.join("real"), 3);
        std::os::unix::fs::symlink(outside.join("big"), root.join("link-to-file")).unwrap();
        std::os::unix::fs::symlink(outside, root.join("link-to-dir")).unwrap();
        std::os::unix::fs::symlink(root, root.join("link-to-self")).unwrap();

        let layers = dir_layers(root, 1, &[]).unwrap();
        assert_eq!(layers.get(ROOT_LAYER), Some(&3), "{layers:?}");
        assert_eq!(layers.len(), 1, "{layers:?}");
    }

    /// Two names of one file are one file's worth of bytes, held under the name
    /// that comes first in path order. A cache read any other way reports more
    /// than it holds, and a cap held against that total could be met by
    /// removing a name that frees nothing.
    #[cfg(unix)]
    #[test]
    fn a_hardlinked_file_is_counted_once_under_its_first_name() {
        let root = scratch("hardlink");
        let root = root.path();
        write(&root.join("debug/deps/lib.rlib"), 100);
        write(&root.join("debug/deps/other.rlib"), 5);
        fs::hard_link(
            root.join("debug/deps/lib.rlib"),
            root.join("debug/lib.rlib"),
        )
        .unwrap();

        let files = dir_files(root, 2, &[]).unwrap();
        assert_eq!(files.len(), 3, "every name is reported: {files:?}");
        let counted = files.iter().find(|f| !f.alias).unwrap();
        assert_eq!(
            counted.path,
            root.join("debug/deps/lib.rlib"),
            "the first name in path order owns the bytes, not the one the \
             filesystem happened to list first"
        );
        assert_eq!(counted.layer, "debug/deps");
        assert_eq!(counted.links, Some(2));
        let alias = files
            .iter()
            .find(|f| f.path == root.join("debug/lib.rlib"))
            .unwrap();
        assert!(alias.alias, "{alias:?}");
        assert_eq!(alias.links, Some(2));

        assert_eq!(counted_bytes(&files), 105);
        assert_eq!(dir_size_bytes(root, &[]).unwrap(), 105);
        let layers = dir_layers(root, 2, &[]).unwrap();
        assert_eq!(layers.get("debug/deps"), Some(&105));
        // The layer the other name is in still exists in the reading, holding
        // the zero bytes it has: a layer that vanishes cannot be told from one
        // the walk could not see.
        assert_eq!(layers.get("debug"), Some(&0));
    }

    /// The streamed total is the materialised one, hardlinks and symlinks
    /// included: the two walkers cannot report different sizes for one tree, or
    /// the reading a store is measured against and the reading a cache is
    /// capped against would answer for the same bytes differently.
    #[cfg(unix)]
    #[test]
    fn the_streamed_total_is_the_materialised_one() {
        let root = scratch("streamed");
        let root = root.path();
        write(&root.join("content/blob"), 4096);
        write(&root.join("snap/1/fs/blob"), 8192);
        fs::create_dir_all(root.join("snap/2")).unwrap();
        fs::hard_link(root.join("content/blob"), root.join("snap/2/blob")).unwrap();
        std::os::unix::fs::symlink(root.join("content/blob"), root.join("link")).unwrap();

        let materialised = dir_size_bytes(root, &[]).unwrap();
        assert_eq!(materialised, 4096 + 8192, "the hardlink is one file");
        let streamed = dir_bytes_counted_once(root, &[]).unwrap();
        assert_eq!(streamed.bytes, materialised);
        assert_eq!(
            streamed.vanished, 0,
            "nothing was removed, so nothing may be reported as removed"
        );

        // The same exclusions, so a nested mount left out of one walk is left
        // out of both.
        let exclude = vec![root.join("snap")];
        assert_eq!(
            dir_bytes_counted_once(root, &exclude).unwrap().bytes,
            dir_size_bytes(root, &exclude).unwrap()
        );
    }

    /// A tree that is pruned while it is walked is measured, not failed, and
    /// the entries that left are reported rather than swallowed: a container
    /// runtime removes snapshots under its store while the store is walked, and
    /// one removed directory must not turn a 60 GB reading into none.
    #[test]
    fn an_entry_removed_mid_walk_is_counted_as_gone_not_as_a_failure() {
        let root = scratch("vanish");
        let root = root.path();
        // Read second, because the pending stack takes the last name listed
        // first: the callback fires while the other directory is being walked.
        write(&root.join("aaa/file"), 20);
        write(&root.join("zzz/file"), 10);

        let mut files = 0usize;
        let vanished = walk(root, 1, &[], |_entry| {
            files += 1;
            let _ = fs::remove_dir_all(root.join("aaa"));
        })
        .unwrap();

        assert_eq!(files, 1, "the surviving directory's file is the one read");
        assert_eq!(vanished, 1, "the removed directory is reported as gone");
    }

    /// A failed walk names where it stopped: the caller reports one line per
    /// tree, and a line saying only "permission denied" says neither which
    /// directory to look at nor which of several reasons to expect.
    #[test]
    fn a_failed_walk_names_the_path_it_stopped_at() {
        let root = scratch("annotate");
        let root = root.path();
        write(&root.join("d/f"), 1);
        let err = annotate(
            io::Error::new(io::ErrorKind::PermissionDenied, "denied"),
            &root.join("d"),
        );
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        let text = err.to_string();
        assert!(text.contains("denied"), "{text}");
        assert!(
            text.contains(&root.join("d").display().to_string()),
            "{text}"
        );
    }

    /// A tree the walk cannot read fails rather than reporting what it managed
    /// to reach: a partial total is smaller than the truth, and a smaller cache
    /// reading can only silence a cap.
    #[test]
    fn a_walk_that_cannot_read_the_tree_fails() {
        let root = scratch("missing");
        let root = root.path();
        write(&root.join("open/a"), 1);
        assert!(dir_size_bytes(&root.join("nowhere"), &[]).is_err());

        // The same for a directory inside the tree. Whether the permissions
        // below actually deny the process depends on the user it runs as, so
        // this first checks that they do rather than asserting it.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let closed = root.join("closed");
            write(&closed.join("b"), 1);
            fs::set_permissions(&closed, fs::Permissions::from_mode(0o000)).unwrap();
            let denied = fs::read_dir(&closed).is_err();
            let result = dir_size_bytes(root, &[]);
            fs::set_permissions(&closed, fs::Permissions::from_mode(0o755)).unwrap();
            if denied {
                assert!(result.is_err(), "{result:?}");
            }
        }
    }

    fn set_modified(path: &Path, at: SystemTime) {
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(at)
            .unwrap();
    }

    /// The count is of writes inside the window, by layer, and a file written
    /// before the window is not counted -- otherwise a build that reused the
    /// cache would read the same as one that rebuilt it.
    #[test]
    fn only_writes_inside_the_window_are_counted() {
        let root = scratch("recent");
        let root = root.path();
        let now = SystemTime::now();
        let now_secs = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        write(&root.join("debug/deps/a.rlib"), 10);
        write(&root.join("debug/deps/b.rlib"), 10);
        write(&root.join("release/deps/c.rlib"), 10);
        let old = root.join("release/deps/old.rlib");
        write(&old, 10);
        set_modified(&old, now - std::time::Duration::from_secs(2 * 3600));

        let files = dir_files(root, 2, &[]).unwrap();
        let counts = recently_written_totals(&files, now_secs, 3600);
        assert_eq!(counts.get("debug/deps"), Some(&2));
        assert_eq!(
            counts.get("release/deps"),
            Some(&1),
            "the file written before the window is left out"
        );
    }

    /// A file with no modification time is not evidence of a write, so it is
    /// left out rather than counted as recent.
    #[test]
    fn a_file_without_a_timestamp_is_not_counted() {
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let entry = FileEntry {
            path: PathBuf::from("debug/deps/x"),
            layer: "debug/deps".to_string(),
            len: 1,
            modified: None,
            id: None,
            links: None,
            alias: false,
        };
        assert!(recently_written_totals(&[entry], now_secs, 3600).is_empty());
    }
}
