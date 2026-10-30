//! principle: feedback_no_hardcoding_config_surface
//! The roots the process keeps its files under — data, config, run, the build
//! gate's lock directory, the git mirror — are declared once in
//! `cog_core::paths`, and every one of them has an environment override next to
//! it (`COGNEVA_DATA_DIR`, `COGNEVA_CONFIG_PATH`, …). A literal somewhere else
//! is a copy that no override moves: point the deployment at another data
//! directory and that one caller still reads the old one, which on a fresh
//! install is empty — so it reports "nothing there yet" rather than "wrong
//! path", and the trend it feeds reads as a system that has never done
//! anything.
//!
//! The roots are read from the declaration rather than written here: a list in
//! the test would be the same kind of second copy, and it would be the copy
//! nobody updates when a root moves.

use crate::common;

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources, relative_name};

/// Every `*.rs` file under `crates/*/src`, relative to the repo root.
/// The one file allowed to spell the roots.
const DECLARATION: &str = "crates/cog-core/src/paths.rs";

/// The declared roots, as the declaration spells them.
fn roots() -> Vec<(&'static str, &'static str)> {
    [
        ("DATA_DIR", cog_core::paths::DATA_DIR),
        ("CONFIG_DIR", cog_core::paths::CONFIG_DIR),
        ("RUN_DIR", cog_core::paths::RUN_DIR),
        ("BUILD_GATE_LOCK_DIR", cog_core::paths::BUILD_GATE_LOCK_DIR),
        ("GIT_MIRROR_ROOT", cog_core::paths::GIT_MIRROR_ROOT),
    ]
    .into_iter()
    .collect()
}

/// The lines of one file that carry a declared root into a literal.
fn offences(text: &str) -> Vec<String> {
    let mut hits = Vec::new();
    for line in production_lines(text) {
        if !line.contains('"') {
            continue;
        }
        for (name, root) in roots() {
            if line.contains(root) {
                hits.push(format!("{name}: {}", line.trim()));
            }
        }
    }
    hits
}

#[test]
fn only_the_declaration_spells_a_runtime_root() {
    let mut hits = Vec::new();
    for path in production_sources(&repo_root()) {
        let rel = relative_name(&repo_root(), &path);
        if rel == DECLARATION {
            continue;
        }
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        hits.extend(
            offences(&text)
                .into_iter()
                .map(|hit| format!("{rel}: {hit}")),
        );
    }
    assert!(
        hits.is_empty(),
        "这些地方没读 {DECLARATION} 里的声明，而是自己写了一遍根路径。\
         声明旁边就是它们的 env 覆盖项，抄一份等于给那个覆盖项留了个不听话的读者——\
         换了目录之后它读到的是一棵空树，报出来的是「还没有数据」。\n{}",
        hits.join("\n")
    );
}

/// The census is only worth its green if it can redden, and only trustworthy if
/// it does not redden on the two kinds of text that may name a root without
/// acting on one. Both are pinned on text that is not the tree.
#[test]
fn the_census_reddens_on_code_and_ignores_comments_and_fixtures() {
    let root = cog_core::paths::DATA_DIR;
    let text = format!(
        "/// 数据目录默认在 {root} 下。\n\
         pub fn read() -> String {{ \"{root}\".to_string() }}\n\
         // {root} 也行，注释不算\n\
         #[cfg(test)]\n\
         mod tests {{ fn fixture() -> &'static str {{ \"{root}\" }} }}\n"
    );
    let hits = offences(&text);
    assert_eq!(
        hits.len(),
        1,
        "应当只留下那行代码（注释与夹具都不算），实得 {hits:?}"
    );
    assert!(
        hits[0].contains("pub fn read()"),
        "留下的应当是代码行，实得 {hits:?}"
    );
}
