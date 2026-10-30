//! principle: feedback_no_hardcoding_config_surface
//! The image lays the process's files out under one root: the binary, the built
//! web assets, the shipped skills, the migrations. The declaration is the
//! Dockerfile's `SF_APP_DIR`; the code carries it once, in
//! `cog_core::paths::APP_DIR`, and every other path is derived from there.
//!
//! A crate that spells the root out again has written a second declaration. It
//! is correct on the day it is written and stays correct for exactly as long as
//! nobody moves the layout — and when the layout does move, the copy does not
//! fail to compile or fail a gate: it fails as a file-not-found from whichever
//! component happens to look first, which reads as a missing file rather than as
//! a stale path.
//!
//! The predicate is the root string, in the production half of every crate. The
//! one file allowed to contain it is the declaration itself.

use crate::common;

use common::routes::repo_root;

use common::rust_production::{production_sources, production_text, relative_name};

/// The declaration file: the only place the root may be written.
const DECLARATION: &str = "crates/cog-core/src/paths.rs";

/// The root as the image declares it.
fn declared_by_the_image() -> String {
    let path = repo_root().join("Dockerfile");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let line = text
        .lines()
        .find(|line| line.contains("SF_APP_DIR="))
        .unwrap_or_else(|| panic!("{} 没有声明 SF_APP_DIR", path.display()));
    let value = line
        .split("SF_APP_DIR=")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|v| v.trim_end_matches('\\').into())
        .unwrap_or_default()
        .to_string();
    assert!(
        value.starts_with('/') && value.len() > 1,
        "{} 的 SF_APP_DIR 不是一个绝对路径：{line}",
        path.display()
    );
    value
}

/// Every `*.rs` file under `crates/*/src`, relative to the repo root.
/// The roots this process's files live under: the image's installation root and
/// the three the deployment mounts.
fn roots() -> Vec<String> {
    vec![
        declared_by_the_image(),
        cog_core::paths::DATA_DIR.to_string(),
        cog_core::paths::CONFIG_DIR.to_string(),
        cog_core::paths::RUN_DIR.to_string(),
        cog_core::paths::TREE_REPO.to_string(),
        cog_core::paths::BUILD_GATE_LOCK_DIR.to_string(),
        cog_core::paths::GIT_MIRROR_ROOT.to_string(),
    ]
}

/// Is `root` written in `line` as a path -- at a boundary, not as the prefix of
/// a longer directory name (`/var/lib/cogneva-data` must not match
/// `/var/lib/cogneva-build-gate`).
fn names_root(line: &str, root: &str) -> bool {
    let mut from = 0;
    while let Some(rel) = line[from..].find(root) {
        let at = from + rel;
        let before_ok = !line[..at].ends_with(['-', '_']);
        let after_ok = !line[at + root.len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric() || c == '-' || c == '_');
        if before_ok && after_ok {
            return true;
        }
        from = at + root.len();
    }
    false
}

#[test]
fn only_the_declaration_names_the_roots() {
    let roots = roots();
    let mut offenders: Vec<(String, String, String)> = Vec::new();
    for path in production_sources(&repo_root()) {
        let rel = relative_name(&repo_root(), &path);
        if rel == DECLARATION {
            continue;
        }
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for line in production_text(&text).lines() {
            for root in &roots {
                if names_root(line, root) {
                    offenders.push((rel.clone(), root.clone(), line.trim().to_string()));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "这些根只该在 {DECLARATION} 里写一次（安装根另加 Dockerfile 的 SF_APP_DIR）。这些\
         地方又抄了一份：图片或 chart 一动，它们就指向一个没有东西的目录，而报错读起来像\
         「还没有数据」或「文件不存在」：\n{offenders:#?}"
    );
}

/// The mounted roots are the chart's to place. A constant naming a directory the
/// chart never mounts reads an empty directory -- which says "no data yet", not
/// "nothing is mounted here".
#[test]
fn the_mounted_roots_are_mounted_by_the_chart() {
    let templates = repo_root().join("deploy/helm/cogneva/templates");
    let mut text = String::new();
    for entry in std::fs::read_dir(&templates).expect("chart templates/ 不在仓库里") {
        let path = entry.expect("读 templates/ 失败").path();
        if path.extension().and_then(|e| e.to_str()) == Some("yaml") {
            text.push_str(&std::fs::read_to_string(&path).unwrap_or_default());
        }
    }
    for root in [
        cog_core::paths::DATA_DIR,
        cog_core::paths::CONFIG_DIR,
        cog_core::paths::TREE_REPO,
    ] {
        assert!(
            text.contains(&format!("mountPath: {root}")),
            "chart 里没有任何工作负载把 {root} 挂进来：这个常量指着一个装着空气的目录"
        );
    }
}

/// The two host directories are declared by the chart's values, not by a
/// `mountPath`: the build-gate lock is a hostPath the same loop reads, and the
/// mirror root is the gateway's own working directory. Each is checked against
/// the place the chart actually puts it -- a lock dir changed in values must
/// change here too, or the loop takes a lock nobody else takes.
#[test]
fn the_host_directories_are_the_ones_the_chart_declares() {
    let values_path = repo_root().join("deploy/helm/cogneva/values.yaml");
    let values = std::fs::read_to_string(&values_path)
        .unwrap_or_else(|e| panic!("{}: {e}", values_path.display()));
    assert!(
        values.contains(&format!(
            "lockDir: {}",
            cog_core::paths::BUILD_GATE_LOCK_DIR
        )),
        "values.yaml 的 lockDir 与 cog_core::paths::BUILD_GATE_LOCK_DIR 不一致：\
         进程锁的是另一个目录，两台构建就不会互相看见"
    );
    let gateway = std::fs::read_to_string(
        repo_root().join("deploy/helm/cogneva/templates/security-gateway.yaml"),
    )
    .unwrap_or_default();
    assert!(
        gateway.contains(cog_core::paths::GIT_MIRROR_ROOT),
        "security-gateway 模板里没有 {}, 镜像根常量指向一个 chart 从不使用的目录",
        cog_core::paths::GIT_MIRROR_ROOT
    );
}

/// The code's copy of the root is the image's. These are two declarations of one
/// fact — the Dockerfile and the constant — and nothing at build time compares
/// them, so they are compared here.
#[test]
fn the_code_and_the_image_agree_on_the_root() {
    assert_eq!(
        cog_core::paths::APP_DIR,
        declared_by_the_image(),
        "Dockerfile 的 SF_APP_DIR 与 cog_core::paths::APP_DIR 不一致：镜像把文件装在一个地方，\
         进程去另一个地方找"
    );
}

/// The named layout constants are paths the image actually bakes. A constant
/// naming a path nothing copies is a reading with nothing under it, and the
/// components that use it would look for a file the image never had.
#[test]
fn the_named_layout_paths_are_paths_the_image_bakes() {
    let path = repo_root().join("Dockerfile");
    let dockerfile =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    for named in [
        cog_core::paths::APP_BINARY,
        cog_core::paths::APP_WEB_DIR,
        cog_core::paths::APP_SKILLS_DIR,
        cog_core::paths::MIGRATIONS_DIR,
    ] {
        assert!(
            dockerfile.contains(named),
            "Dockerfile 里没有 {named}：布局常量指着一个镜像从不放置的路径"
        );
    }
}
