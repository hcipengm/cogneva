//! principle: feedback_no_hardcoding_config_surface
//! A role name is a string the configuration, the code and the stored
//! identifiers all have to agree on. `squad:{task}-{role}` is a key in the
//! checkpoint board; `schema_for(actor)` is a lookup in the PGE schema map; the
//! actor list in the configuration names the same five words. Spelling the word
//! at each use site means a rename edits some of them.
//!
//! What makes this worth a gate is how the disagreement reads. An unknown actor
//! is not an error: `schema_for` answers `None`, and every caller is written
//! `if let Some(schema) = ...` and carries on. A checkpoint under the old role
//! name is simply never found, which reads as "that task has no checkpoint yet".
//! So a squad that has been renamed runs to completion with the wrong prompt
//! structure and no checkpoint resume, and both look like ordinary behaviour.
//!
//! The names live in [`cog_core::roles`]. The reading, the subtraction of
//! registered same-word-different-thing sites and the staleness check are the
//! one implementation in `common::declared_once`; this file only says what is
//! declared and who owns it.

use crate::common;

use common::declared_once::{Declared, Exemption, Report, Wall};
use common::routes::repo_root;

use common::rust_production::production_lines;

/// Where the names are declared, relative to the repository root.
const HOME: &str = "crates/cog-core/src/roles.rs";

const SUBJECT: &str = "角色名";

/// What a red means: the two sides of a rename do not meet anywhere that would
/// notice, so both halves report success.
const WHY: &str = "改名时改一处不会报错：按旧名字取 schema 的那一侧拿到 `None`，\
                   按旧名字找 checkpoint 的那一侧什么也找不到，两者都不算失败。";

/// A site that spells a role name for a reason of its own.
///
/// The same word can name two unrelated things. Registering one of those as an
/// exemption is a claim someone had to look at; without it the gate would
/// report a true statement about the wrong thing.
const SAME_WORD_DIFFERENT_THING: &[Exemption] = &[Exemption::new(
    "crates/cogneva/src/work.rs",
    "external-worker",
    "`HOSTNAME` 读不到时的单元显示名，不是注册用的角色字段——那条构造里角色读的是常量",
)];

/// The wall: every name `ALL` carries, spelled nowhere but its declaration.
///
/// `ALL` is what consumers use, so it is what this gate follows: a role added
/// there is counted here without anyone remembering to edit this file.
/// `Debug`'s field labels are not data and are skipped by the shared reader.
fn wall() -> Wall<'static> {
    Wall::new(SUBJECT, WHY)
        .declared(
            cog_core::roles::ALL
                .iter()
                .map(|name| Declared::new(*name, *name).owned_by(HOME))
                .collect(),
        )
        .skip_debug_labels()
        .floor(5)
        .ceiling(200)
}

#[test]
fn every_role_name_is_spelled_in_the_place_it_is_declared() {
    let sites = wall().sites(&repo_root());
    // A listed site that stops existing is reported here too: an exemption
    // outliving its site is a licence nobody reviewed.
    Report::new(SUBJECT, WHY, &sites, SAME_WORD_DIFFERENT_THING).assert_clean();
}

#[test]
fn the_declaration_list_covers_every_name_declared_in_the_module() {
    let path = repo_root().join(HOME);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let consts = production_lines(&text)
        .into_iter()
        .filter(|line| line.trim_start().starts_with("pub const "))
        .filter(|line| line.contains(": &str = "))
        .count();
    let declared = cog_core::roles::ALL.len();
    assert_eq!(
        consts, declared,
        "roles 模块声明了 {consts} 个角色名，ALL 里只有 {declared} 个：漏掉的那个没有\
         任何消费者读得到，也没人拦得住别人重新拼一遍",
    );
}
