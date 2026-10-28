//! principle: feedback_no_hardcoding_config_surface
//! The data root has an inside: `meta_learning/`, `policies/`, the files in
//! them, and the editor's `workspace/`. Two parties always meet on each of
//! these names — the writer that puts something there and the reader that goes
//! looking for it — and they never meet anywhere that would notice a
//! disagreement. A backup copies `policies/`; if the store moved to another
//! directory name, the backup's `is_dir()` is false and it skips the whole
//! tree. The package still builds, the manifest simply has no entry for it,
//! and "not in the package" reads exactly like "this deployment never had
//! one".
//!
//! So each name is declared once, next to the party that owns it, and every
//! join reads that declaration. The names come from the declarations rather
//! than being listed here: a list in this file would be the same kind of
//! second copy, and it would be the copy nobody updates when an entry moves.
//!
//! What counts is the `join("<name>")` form, not the bare word: `"policies"` is
//! also a word in prose and an ordinary field name, and a gate that reddens on
//! those teaches people to rename their prose.

mod common;

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources, relative_name};

/// Every `*.rs` file under `crates/*/src`, relative to the repo root.
/// The declared entries, read from the consts themselves.
fn entries() -> Vec<(&'static str, &'static str)> {
    [
        ("DATA_SUBDIR", cog_reflection::DATA_SUBDIR),
        ("DECISION_STATS_FILE", cog_reflection::DECISION_STATS_FILE),
        ("POLICIES_SUBDIR", cog_reflection::POLICIES_SUBDIR),
        (
            "RESTORE_EXPECTATION_FILE",
            cog_reflection::RESTORE_EXPECTATION_FILE,
        ),
        ("WORKSPACE_SUBDIR", cog_gateway::ide::WORKSPACE_SUBDIR),
    ]
    .into_iter()
    .collect()
}

/// The lines of `text` that spell a declared entry inside a `join(`.
fn offences(text: &str) -> Vec<String> {
    let mut hits = Vec::new();
    for line in production_lines(text) {
        if !line.contains("join(") {
            continue;
        }
        for (name, value) in entries() {
            if line.contains(&format!("\"{value}\"")) {
                hits.push(format!("{name}: {}", line.trim()));
            }
        }
    }
    hits
}

#[test]
fn only_the_declarations_spell_a_data_root_entry() {
    let mut hits = Vec::new();
    for path in production_sources(&repo_root()) {
        let rel = relative_name(&repo_root(), &path);
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
        "这些地方拼了一遍数据根里的名字，而不是读声明。写方与读方各拼一份时，\
         改名的分歧不会报错：读方那一侧 is_dir()/is_file() 为假就整段跳过，\
         包照样生成，清单上的「没有」与「本来就没有」同形。\n{}",
        hits.join("\n")
    );
}

/// The census is only worth its green if it can redden, and only if it does not
/// redden on the two kinds of text that name an entry without acting on one.
#[test]
fn the_census_reddens_on_code_and_ignores_comments_and_fixtures() {
    let (name, value) = entries()[0];
    let text = format!(
        "/// 落在 {value}/ 下面。\n\
         pub fn read(dir: &Path) -> PathBuf {{ dir.join(\"{value}\") }}\n\
         // dir.join(\"{value}\") 写在注释里不算\n\
         #[cfg(test)]\n\
         mod tests {{ fn fixture(dir: &Path) -> PathBuf {{ dir.join(\"{value}\") }} }}\n"
    );
    let hits = offences(&text);
    assert_eq!(
        hits.len(),
        1,
        "应当只留下那行代码（注释与夹具都不算），实得 {hits:?}"
    );
    assert!(
        hits[0].contains("pub fn read") && hits[0].contains(name),
        "留下的应当是代码行且点名 {name}，实得 {hits:?}"
    );
}

/// 普查有下限：读不到声明就是这条门自己没读到题，不能绿。
#[test]
fn the_census_reads_the_declarations_it_judges() {
    let declared: Vec<&str> = entries().iter().map(|(_, value)| *value).collect();
    assert!(
        declared.len() >= 5,
        "登记的条目少于 5 个，判据没有题目：{declared:?}"
    );
    for value in &declared {
        assert!(
            !value.is_empty() && !value.contains('/'),
            "登记项 {value:?} 不是单个名字（带路径分隔符的应当是拼接出来的）"
        );
    }
}
