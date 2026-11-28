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
//! those teaches people to rename their prose. The reading, subtraction and
//! staleness check are the one implementation in `common::declared_once`.

use crate::common;

use common::declared_once::{Declared, Wall};
use common::routes::repo_root;

const SUBJECT: &str = "数据根条目";

/// What a red means: a writer and a reader each spell the name, and a rename
/// that reaches one of them turns the other's lookup into a quiet absence.
const WHY: &str = "写方与读方各拼一份时，改名的分歧不会报错：读方那一侧 \
                   is_dir()/is_file() 为假就整段跳过，包照样生成，清单上的\
                   「没有」与「本来就没有」同形。";

/// The declared entries, read from the consts themselves.
fn entries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("DATA_SUBDIR", cog_reflection::DATA_SUBDIR),
        ("DECISION_STATS_FILE", cog_reflection::DECISION_STATS_FILE),
        ("POLICIES_SUBDIR", cog_reflection::POLICIES_SUBDIR),
        (
            "RESTORE_EXPECTATION_FILE",
            cog_reflection::RESTORE_EXPECTATION_FILE,
        ),
        ("WORKSPACE_SUBDIR", cog_gateway::ide::WORKSPACE_SUBDIR),
    ]
}

fn wall() -> Wall<'static> {
    Wall::new(SUBJECT, WHY)
        .context("join(")
        .declared(
            entries()
                .into_iter()
                .map(|(name, value)| Declared::new(name, value))
                .collect(),
        )
        .floor(5)
        .ceiling(200)
}

#[test]
fn only_the_declarations_spell_a_data_root_entry() {
    wall().assert_clean(&repo_root());
}

/// 普查有下限：读不到声明就是这条门自己没读到题，不能绿。
#[test]
fn the_census_reads_the_declarations_it_judges() {
    let declared = entries();
    assert!(
        declared.len() >= 5,
        "登记的条目少于 5 个，判据没有题目：{declared:?}"
    );
    for (_, value) in &declared {
        assert!(
            !value.is_empty() && !value.contains('/'),
            "登记项 {value:?} 不是单个名字（带路径分隔符的应当是拼接出来的）"
        );
    }
}
