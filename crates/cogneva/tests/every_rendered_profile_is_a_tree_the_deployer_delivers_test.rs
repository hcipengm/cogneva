//! principle: feedback_no_hardcoding_config_surface
//! The in-cluster deployer delivers a fixed set of manifest trees, written as a
//! list in the code. The trees themselves are produced by CI: one directory per
//! Helm profile under `deploy/rendered/`, plus the static k3s tree. The list is
//! the code's copy of what the renderer emitted, and nothing compared the two.
//!
//! The failure this closes is silent by construction: a profile that renders a
//! tree the list does not name is simply never deployed on that topology, and a
//! topology that ships half a deployment reads, from every counter the deployer
//! owns, exactly like a topology where nothing was pending. Conversely a tree
//! named but not rendered makes the deployer wait on a directory that will never
//! appear.
//!
//! So the set is read off the disk here, not transcribed: every directory under
//! `deploy/rendered/` is a deliverable tree, and every Helm profile is one of
//! those directories.

mod common;

use common::routes::repo_root;

use std::collections::BTreeSet;
use std::path::Path;

fn entries(dir: &Path, want_dir: bool) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.is_dir() != want_dir {
                return None;
            }
            let name = path.file_name()?.to_string_lossy().into_owned();
            Some(if want_dir {
                name
            } else {
                path.file_stem()?.to_string_lossy().into_owned()
            })
        })
        .collect()
}

#[test]
fn the_delivered_set_is_the_set_the_renderer_emitted() {
    let root = repo_root();
    let rendered = entries(&root.join("deploy/rendered"), true);
    assert!(
        !rendered.is_empty(),
        "deploy/rendered 下没有任何渲染目录：先跑 deploy/scripts/render-deploy.sh"
    );

    let mut on_disk: BTreeSet<String> = rendered
        .iter()
        .map(|name| format!("deploy/rendered/{name}"))
        .collect();
    on_disk.insert("deploy/k3s".to_string());

    let declared: BTreeSet<String> = cog_reflection::DELIVERABLE_MANIFEST_TREES
        .iter()
        .map(|tree| tree.to_string())
        .collect();

    let undeclared: Vec<&String> = on_disk.difference(&declared).collect();
    let missing: Vec<&String> = declared.difference(&on_disk).collect();
    assert!(
        undeclared.is_empty() && missing.is_empty(),
        "部署器的清单树集合与磁盘对不上：磁盘上有而代码里没有 {undeclared:?}，\
         代码里有而磁盘上没有 {missing:?}——前者那种树在那套拓扑上永远不会被投递，\
         而「没投递」与「没有待投递的东西」在读数上分不开"
    );

    for tree in cog_reflection::DELIVERABLE_MANIFEST_TREES {
        let path = root.join(tree);
        assert!(
            path.is_dir(),
            "{tree} 在代码里被当作交付树，磁盘上却没有这个目录"
        );
    }
}

#[test]
fn every_helm_profile_has_the_tree_it_renders_to() {
    let root = repo_root();
    let profiles = entries(&root.join("deploy/helm/cogneva/profiles"), false);
    let rendered = entries(&root.join("deploy/rendered"), true);
    assert_eq!(
        profiles, rendered,
        "chart 的 profile 与预渲染目录不是一一对应：缺哪一边，那套拓扑就走上了\
         另一套的清单，或者根本没有清单可用"
    );
}
