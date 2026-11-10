//! 「读数没读到」那两条自报的规则名，只写在一个地方。
//!
//! 这两条规则说的不是系统里的一件事，而是观察本身缺了一格：一条规则求值不出来，
//! 或者这份进程跑的配置不是部署声明的那个。它们与别的故障走同一张 `alerts` 表、
//! 同一个读者，读者靠**规则名**把两者分开——分开之后判据、目标、项目分组才是
//! 两套。名字在两边各拼一遍，改一处不会红：写行的那一侧换了个字，读行的那一侧
//! 只是再也认不出来，于是「读数没读到」被当成普通故障派下去，接活的人去看系统，
//! 而那正是没人看得见的东西。整条链上没有任何一步会说「我认不出这个名字」。
//!
//! 所以名字声明在 `cog-core` 的告警契约里（写行的 `cog-observability` 与读行的
//! `cog-reflection` 不共用一个 crate），这里判它只写在那一个文件里。读法、登记
//! 相减与过期判据是 `common::declared_once` 那一份。

use crate::common;

use common::declared_once::{Declared, Report, Wall};
use common::routes::repo_root;

/// 名字声明处，相对仓库根。
const HOME: &str = "crates/cog-core/src/contract/alerts.rs";

const SUBJECT: &str = "自报规则名";

/// 红的时候这句话是为什么：名字对不上不会报错，只会让一个来源悄悄变成另一个。
const WHY: &str = "两个拼写都合法，写行的那一侧换了字之后，读行的那一侧只是不认识这条规则，\
                   于是「读数没读到」被当成系统里的故障派下去；没有一处会报错。";

/// 这堵墙：三个名字，只许写在声明它们的那个文件里。
///
/// 名单从 `cog-core` 里读，不在本文件抄一遍：漏一个就少判一个，而少判与合规在
/// 判词上同形。
fn wall() -> Wall<'static> {
    let declared = vec![
        Declared::new(
            cog_core::alerts::EVAL_FAILURE_RULE,
            cog_core::alerts::EVAL_FAILURE_RULE,
        )
        .owned_by(HOME),
        Declared::new(
            cog_core::alerts::CONFIG_DECLARATION_RULE,
            cog_core::alerts::CONFIG_DECLARATION_RULE,
        )
        .owned_by(HOME),
        Declared::new(
            cog_core::alerts::CONFIG_RULES_NOT_LOADED_RULE,
            cog_core::alerts::CONFIG_RULES_NOT_LOADED_RULE,
        )
        .owned_by(HOME),
    ];
    Wall::new(SUBJECT, WHY)
        .declared(declared)
        .floor(3)
        .ceiling(3)
}

#[test]
fn every_self_alert_rule_name_is_spelled_where_it_is_declared() {
    let sites = wall().sites(&repo_root());
    Report::new(SUBJECT, WHY, &sites, &[]).assert_clean();
}

/// 读者认的名字必须就是写行的那一侧写下的名字。
///
/// `cog-reflection` 不依赖 `cog-observability`，所以这条只能按树读：读者那一侧
/// 的分类函数里出现的每一个 `EVAL_FAILURE_RULE` / `CONFIG_*` 常量，都得在
/// `cog-core` 里真有同名的声明，并且没有被谁在本地重新赋过值。上面那道墙管的是
/// 「字面量只有一处」，这一条管的是「读者读的就是那一处」。
#[test]
fn the_reader_and_the_writers_meet_on_the_same_declaration() {
    let path = repo_root().join("crates/cog-reflection/src/signal_watcher.rs");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let code = common::rust_production::production_text(&text);
    for name in [
        "EVAL_FAILURE_RULE",
        "CONFIG_DECLARATION_RULE",
        "CONFIG_RULES_NOT_LOADED_RULE",
    ] {
        let needle = format!("cog_core::alerts::{name}");
        assert!(
            code.contains(&needle),
            "signal_watcher 没有读 {needle}：读者的来源分类要么删了，要么在本地另抄了一个名字——\
             两种都让「读数没读到」悄悄退回普通故障"
        );
    }
}
