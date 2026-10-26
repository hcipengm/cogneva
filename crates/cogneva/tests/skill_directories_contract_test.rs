//! principle: feedback_no_hardcoding_config_surface
//! Every directory the skill loader reads has to be one the image ships or the
//! deployment mounts. The loader skips a directory that is not there, and a
//! directory that is not there looks exactly like one with nothing new in it —
//! so a path that no manifest provides costs nothing at boot and buys nothing
//! either: the skills installed into it are never read, and nothing says so.
//!
//! That is not hypothetical. The same list was written twice — once in the
//! plugin, once in the config default — and the two copies had already drifted:
//! one expanded the home directory, the other passed `~/.cogneva/skills`
//! through as a literal that never resolves, and both carried a writable entry
//! (`/var/lib/cogneva/skills`) that no chart, no rendered profile and no image
//! layer mounts.
//!
//! So the predicate is placement, not spelling: a directory is fine if the
//! image ships it, if the main app's pod mounts it, or if it is under the
//! process's home — the last for the out-of-container case, where the binary
//! runs from a checkout and nothing is mounted at all.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The main app's mount points, as all three rendered profiles declare them.
/// Read rather than listed: the chart is where a mount is decided, and a copy of
/// the list here would be the same kind of second declaration this gate exists
/// to catch.
fn main_app_mounts() -> Vec<String> {
    let mut out = Vec::new();
    for profile in ["k3s-single", "k3s-multi", "k8s-standard"] {
        let path = repo_root().join(format!(
            "deploy/rendered/{profile}/41-deployment-cogneva.yaml"
        ));
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} 读不到：{e}", path.display()));
        let mut found = 0;
        for line in text.lines() {
            let Some(rest) = line.trim().strip_prefix("mountPath:") else {
                continue;
            };
            found += 1;
            let mount = rest.trim().trim_matches('"').to_string();
            if !out.contains(&mount) {
                out.push(mount);
            }
        }
        assert!(
            found > 0,
            "{} 里一个 mountPath 都没读到：判据会静默放过每一条路径",
            path.display()
        );
    }
    out
}

/// The directories that are neither shipped, mounted, nor under the home
/// directory — that is, the ones the loader would scan and find nothing in.
fn unplaced(
    directories: &[PathBuf],
    shipped: &Path,
    mounted: &[String],
    home: Option<&Path>,
) -> Vec<String> {
    directories
        .iter()
        .filter(|dir| !dir.starts_with(shipped))
        .filter(|dir| !mounted.iter().any(|mount| dir.starts_with(mount)))
        .filter(|dir| !home.is_some_and(|home| dir.starts_with(home)))
        .map(|dir| dir.display().to_string())
        .collect()
}

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

#[test]
fn every_directory_the_loader_reads_is_shipped_mounted_or_under_home() {
    let directories = cog_skill::registry::skill_directories();
    let home = home();
    let orphaned = unplaced(
        &directories,
        Path::new(cog_core::paths::APP_DIR),
        &main_app_mounts(),
        home.as_deref(),
    );
    assert!(
        orphaned.is_empty(),
        "技能加载器会去读这些目录，而镜像里没有、清单里也没挂：{orphaned:?}。\
         缺一个目录与一个没有新内容的目录在扫描里长得一样，所以这条路径既不会在启动时报错，\
         也永远不会被读到。清单挂在：{:?}",
        main_app_mounts()
    );
}

/// The writable half has to be there. A list of read-only shipped directories
/// loads the image's own skills and silently ignores whatever an operator
/// installed, which is the same quiet failure with the opposite sign.
#[test]
fn one_directory_is_writable_by_the_process() {
    let directories = cog_skill::registry::skill_directories();
    let data = cog_core::paths::data_dir();
    assert!(
        directories
            .iter()
            .any(|dir| dir.starts_with(&data) && Path::new(dir).is_absolute()),
        "技能目录里没有一个是挂在数据卷上的可写目录（数据目录 {data}）：\
         装机时放进去的技能读得到，装好的那一刻写不进去、重启也没了。实得 {directories:?}"
    );
}

/// The list is one list. Two copies had already drifted apart before this gate
/// existed, so the shape that made them drift is what is checked: a second
/// literal list of directories, in the plugin, next to the config default.
#[test]
fn the_directory_list_is_not_written_a_second_time() {
    let path = repo_root().join("crates/cog-skill/src/plugin.rs");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    assert!(
        text.contains("skill_directories()"),
        "插件没有走 skill_directories()：两份目录列表就是这么分叉的"
    );
}

/// The predicate has to redden on the path that was actually there, or it would
/// have passed on the tree this file was written for. Fed the old list, it must
/// name exactly the entry nothing provides.
#[test]
fn the_stale_writable_entry_is_reported() {
    let old: Vec<PathBuf> = vec![
        PathBuf::from(cog_core::paths::APP_SKILLS_DIR),
        PathBuf::from("/var/lib/cogneva/skills"),
        PathBuf::from("/home/somebody/.cogneva/skills"),
    ];
    let orphaned = unplaced(
        &old,
        Path::new(cog_core::paths::APP_DIR),
        &main_app_mounts(),
        Some(Path::new("/home/somebody")),
    );
    assert_eq!(
        orphaned,
        vec!["/var/lib/cogneva/skills".to_string()],
        "判据必须点名那条没有任何清单提供的路径，且只点名它"
    );
}
