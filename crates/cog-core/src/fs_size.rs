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
//! Hardlinks are counted once per path rather than once per inode. Telling the
//! two apart means keeping the walked inodes in memory; what that buys is the
//! one link cargo makes (the linked binary is a hardlink into `deps`), so the
//! total reads a few hundred megabytes above what `du` reports. `du` answers
//! "how much disk does this hold"; this answers "how many bytes are stored
//! here", and the two differ by exactly those links.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

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
/// An unreadable subdirectory fails the whole walk. The caller is expected to
/// keep its last measurement: a partial total is smaller than the truth, and a
/// smaller cache reading can only silence a cap.
pub fn dir_layers(
    dir: &Path,
    depth: usize,
    exclude: &[PathBuf],
) -> io::Result<BTreeMap<String, u64>> {
    let mut totals: BTreeMap<String, u64> = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let path = entry.path();
            if exclude.iter().any(|excluded| excluded == &path) {
                continue;
            }
            // Without following links: a symlink to a directory would otherwise
            // be descended into, and one to a file counted at its target's size.
            let meta = entry.metadata()?;
            if meta.is_dir() {
                pending.push(path);
            } else if meta.is_file() {
                let total = totals.entry(layer_of(dir, &path, depth)).or_default();
                *total = total.saturating_add(meta.len());
            }
        }
    }
    Ok(totals)
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

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cog-fssize-{}-{}", name, std::process::id()));
        fs::remove_dir_all(&dir).ok();
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(path: &Path, bytes: usize) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    /// The total is the sum of the layers: one walk answers both readings, so a
    /// cap held against the total cannot be enforced against a breakdown that
    /// was taken from a different tree state.
    #[test]
    fn the_total_is_the_sum_of_the_layers() {
        let root = scratch("total");
        write(&root.join("debug/deps/lib.rlib"), 100);
        write(&root.join("debug/incremental/obj"), 30);
        write(&root.join("release/deps/lib.rlib"), 7);
        write(&root.join("CACHEDIR.TAG"), 1);

        let layers = dir_layers(&root, 1, &[]).unwrap();
        assert_eq!(layers.get("debug"), Some(&130));
        assert_eq!(layers.get("release"), Some(&7));
        assert_eq!(layers.get(ROOT_LAYER), Some(&1));
        assert_eq!(
            dir_size_bytes(&root, &[]).unwrap(),
            layers.values().sum::<u64>()
        );
        assert_eq!(dir_size_bytes(&root, &[]).unwrap(), 138);
    }

    /// The depth is what separates a profile directory's own files from the
    /// caches below it, which is the decision the build cache reading feeds.
    #[test]
    fn the_depth_decides_which_layers_are_reported() {
        let root = scratch("depth");
        write(&root.join("debug/deps/lib.rlib"), 100);
        write(&root.join("debug/cogneva"), 40);
        write(&root.join("debug/incremental/obj"), 30);

        let shallow = dir_layers(&root, 1, &[]).unwrap();
        assert_eq!(shallow.get("debug"), Some(&170));

        let deep = dir_layers(&root, 2, &[]).unwrap();
        assert_eq!(deep.get("debug/deps"), Some(&100));
        assert_eq!(deep.get("debug/incremental"), Some(&30));
        assert_eq!(deep.get("debug"), Some(&40));
        // A deeper request that the tree cannot fill stops at what exists.
        assert_eq!(dir_layers(&root, 9, &[]).unwrap(), deep);
    }

    /// A file directly in the measured directory belongs to the directory
    /// itself, not to a layer invented to hold it.
    #[test]
    fn a_file_in_the_root_belongs_to_the_root_layer() {
        let root = scratch("rootfile");
        write(&root.join("CACHEDIR.TAG"), 5);
        let layers = dir_layers(&root, 2, &[]).unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(layers.get(ROOT_LAYER), Some(&5));
    }

    /// An excluded directory is neither counted nor descended into: a mount
    /// inside the measured tree is another volume's bytes, and counting them
    /// here would report them against both.
    #[test]
    fn an_excluded_subtree_is_left_out() {
        let root = scratch("exclude");
        write(&root.join("kept/a"), 10);
        write(&root.join("nested/b"), 900);
        let exclude = vec![root.join("nested")];

        let layers = dir_layers(&root, 1, &exclude).unwrap();
        assert_eq!(layers.get("kept"), Some(&10));
        assert_eq!(layers.get("nested"), None);
        assert_eq!(dir_size_bytes(&root, &exclude).unwrap(), 10);
    }

    /// A symlink is not a file to measure and not a directory to walk: its
    /// target is outside the tree, or behind it.
    #[cfg(unix)]
    #[test]
    fn a_symlink_is_neither_counted_nor_followed() {
        let root = scratch("symlink");
        let outside = scratch("symlink-target");
        write(&outside.join("big"), 4096);
        write(&root.join("real"), 3);
        std::os::unix::fs::symlink(outside.join("big"), root.join("link-to-file")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link-to-dir")).unwrap();
        std::os::unix::fs::symlink(&root, root.join("link-to-self")).unwrap();

        let layers = dir_layers(&root, 1, &[]).unwrap();
        assert_eq!(layers.get(ROOT_LAYER), Some(&3), "{layers:?}");
        assert_eq!(layers.len(), 1, "{layers:?}");
    }

    /// A tree the walk cannot read fails rather than reporting what it managed
    /// to reach: a partial total is smaller than the truth, and a smaller cache
    /// reading can only silence a cap.
    #[test]
    fn a_walk_that_cannot_read_the_tree_fails() {
        let root = scratch("missing");
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
            let result = dir_size_bytes(&root, &[]);
            fs::set_permissions(&closed, fs::Permissions::from_mode(0o755)).unwrap();
            if denied {
                assert!(result.is_err(), "{result:?}");
            }
        }
    }
}
