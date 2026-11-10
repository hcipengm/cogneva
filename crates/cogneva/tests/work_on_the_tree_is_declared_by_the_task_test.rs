//! principle: flat-agent-society
//!
//! 要不要动工作树，是这件活说的，不是接手的人说的。
//!
//! 旧判据问的是角色名：`writes_the_checkout(role) == (role == generator)`。它把
//! 一种流水线当成了世界的形状——同一个人接了评审就永远「不该动手」，接了一份
//! 变更就永远「该动手」，而事实上决定这件事的是他手上那一件活。
//!
//! 这条判据错了也不会报：
//!
//! - 一位工程师接了产出变更的活，却因为注册表上写的是别的标签而不被「半小时
//!   没动」收住：它绕一个小时的圈，账上读成「跑满预算、零产出」。
//! - 一位规划者接了只出结论的活，却因为标签是 generator 被收住：它被当成
//!   「迷路了」，而它做的正是本职工作——只是这份工作本来就不该动这棵树。
//!
//! 两种错都在读数上长得像正常工作，所以本门分两半：一半问任务（同一份声明换
//! 输入，答案跟着变），一半普查树上还有没有按角色名判的地方。

use crate::common;

use common::lex::Dialect;
use common::root::repo_root;
use common::routes::matching_paren;
use common::rust_production::{production_sources, relative_name};

/// 树上每个生产源文件里出现过 `needle` 的那些。
fn mentions(needle: &str) -> Vec<String> {
    let root = repo_root();
    let mut hits = Vec::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        if text.contains(needle) {
            hits.push(relative_name(&root, &path));
        }
    }
    hits
}

/// 一件活要不要动工作树，答案是任务给的，而且换一件活就换答案。
///
/// 这里读的是公开接口：同一份输入写不写声明、是不是自进化的生成变更，两处各
/// 答一次。角色不进这个判断——下面两件任务的角色字段都没有，它们不应该需要。
#[test]
fn a_task_says_whether_its_work_changes_the_checkout() {
    use cog_core::{Task, TaskType};

    let writes = Task::new(
        "t-1",
        TaskType::Custom("platform_task".into()),
        serde_json::json!({ "goal": "raise the read timeout", "declares_change": true }),
    );
    let reads = Task::new(
        "t-2",
        TaskType::Custom("platform_task".into()),
        serde_json::json!({ "goal": "read the timeout" }),
    );
    let evolution = Task::new(
        "t-3",
        TaskType::Custom("self_evolution".into()),
        serde_json::json!({}),
    );

    assert!(writes.declares_change(), "声明了要交改动的活得算数");
    assert!(
        !reads.declares_change(),
        "没声明要动树的活不该被读成要动：它会被「树没动」当成没进展收住"
    );
    assert!(
        evolution.declares_change(),
        "自进化生成变更本身就是那份声明"
    );

    // 运行时读的是这一位，缺省是没有声明——没有声明时不拿「树没动」当证据。
    assert!(!cog_core::RuntimeConfig::default().declares_change);
}

/// 树上不再有按角色名决定「谁可以写工作树」的地方。
#[test]
fn nobody_decides_who_may_write_the_tree_by_role() {
    let by_name = mentions("writes_the_checkout");
    assert!(
        by_name.is_empty(),
        "按角色名收口「能不能写工作树」的判据还在：{by_name:?}。它把一种流水线当成\
         世界的形状——同一个人换件活就判错，而两边的错都长得像正常工作"
    );

    // 声明从任务抄进运行时的那个点只有一个：管理器建 agent 时。
    let root = repo_root();
    let manager = std::fs::read_to_string(root.join("crates/cog-agent/src/runtime/manager.rs"))
        .expect("manager.rs readable");
    assert!(
        manager.contains("config.declares_change = declares_change"),
        "管理器不再把任务的声明抄进运行时：那一句没了，声明就只写在构造函数里，\
         运行时永远读到 false，而「半小时没动」从此再也收不住任何人"
    );

    // 小队里建角色 agent 的地方：交改动的那一份活要带上声明，别的活儿不带。
    // 这条普查挡的是「新加一个产变更的角色，忘了说」——它会安静地跑成没有墙的。
    let mut checked = 0;
    for path in production_sources(&root) {
        let name = relative_name(&root, &path);
        if !name.starts_with("crates/cog-collaboration/src/") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let mut from = 0usize;
        while let Some(offset) = text[from..].find("crate::resume::create_role_agent(") {
            let start = from + offset;
            let Some(end) = matching_paren(
                &text,
                start + "crate::resume::create_role_agent".len(),
                Dialect::Rust,
            ) else {
                break;
            };
            let call = &text[start..=end];
            let produces = call.contains("roles::GENERATOR");
            if produces {
                assert!(
                    call.contains("declares_change"),
                    "{name} 建了一个要交改动的角色，却没把任务的声明传进去：这一位的\
                     每一轮都读成「没有声明要动树」，绕圈时不会被收住\n{call}"
                );
            } else {
                assert!(
                    call.contains("false"),
                    "{name} 建了一个只出结论的角色，却带了声明：它会被「树没动」当成\
                     没进展收住，而它做的正是本职工作\n{call}"
                );
            }
            checked += 1;
            from = end + 1;
        }
    }
    assert!(
        checked > 0,
        "一处 create_role_agent 都没普查到：这条判据读的名字改了，它现在什么都没在守"
    );
}
