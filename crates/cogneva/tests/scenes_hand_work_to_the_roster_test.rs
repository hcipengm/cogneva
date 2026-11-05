//! 场景不自己叫模型，也不自己占一个身份；它只交出一件活。
//!
//! 一个场景知道要做什么（这一份材料按什么判据看、答案写给谁），但「谁来干」
//! 是名册的事。给每个场景在平台里写一段专用提示词、一次模型调用、一个 actor
//! 名，每个场景就都成了一个写进平台的角色：账记在场景名下、接活的人不可见，
//! 轮转与限额也就看不到它。这四处都曾是这个形状，先后被改成投一件
//! [`PLATFORM_AGENT_TASK`]。
//!
//! 判据读的是生产代码文本，不读注释、不读测试：`.chat(` / `.chat_stream(` /
//! `.with_actor(` 任何一处出现都红，且必须看得见它投的是哪一类活。接活的那
//! 一侧同时被钉住——认这个种类，并且把「谁接的」记到按员工读的计数上；少了
//! 后半句，「活被轮转均分了」与「其余人只是名单上的一行」在读数上同形。

use std::path::Path;

use crate::common;
use common::routes::repo_root;
use common::rust_production::production_text;

/// 交出判断的那几处场景。
const SCENES: &[&str] = &[
    "crates/cog-memory/src/extractor.rs",
    "crates/cog-wiki/src/distiller.rs",
    "crates/cogneva/src/eval_platform.rs",
    "crates/cog-reflection/src/evolution.rs",
];

/// 接活的那一份执行器。
const ROSTER: &str = "crates/cog-collaboration/src/collaboration_executor.rs";

fn production_of(root: &Path, rel: &str) -> String {
    let text =
        std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("{rel} unreadable: {e}"));
    production_text(&text)
}

#[test]
fn a_scene_hands_its_judgement_to_the_roster_instead_of_calling_a_model() {
    let root = repo_root();
    for rel in SCENES {
        let production = production_of(&root, rel);
        assert!(
            production.contains("PLATFORM_AGENT_TASK"),
            "{rel} no longer hands anything to the roster: it carries no work item kind, so \
             whatever it asks for is either a model call of its own or nothing at all"
        );
        for needle in [".chat(", ".chat_stream(", ".with_actor("] {
            assert!(
                !production.contains(needle),
                "{rel} still calls the model directly (`{needle}`). A scene that calls the \
                 model itself spends under its own name and leaves the employee who did the \
                 work out of the count: hand over a `PLATFORM_AGENT_TASK` instead."
            );
        }
    }
}

/// 接活的一侧认得出这件活，并且记下是谁接的。
#[test]
fn the_roster_routes_the_work_item_and_counts_who_took_it() {
    let root = repo_root();
    let production = production_of(&root, ROSTER);
    assert!(
        production.contains("kind == PLATFORM_AGENT_TASK"),
        "{ROSTER} no longer recognises `PLATFORM_AGENT_TASK`: every scene that hands one over \
         would fall through to the branch that decomposes a plan nobody wrote"
    );
    assert!(
        production.contains("record_work_item_taken"),
        "{ROSTER} takes the work item without recording which employee took it. Every employee \
         then reads the same on the metrics face, and a rotation that truly spreads the work \
         cannot be told apart from a roster where only one name ever answers."
    );
}
