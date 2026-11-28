//! 平台对每个智能体说的同一段话。
//!
//! 智能体是通用的：同一个内核、同一套工具、同一个部署里的任务与对话。它这一轮
//! 做什么，由它拿到的那件活或那句话决定；它是什么，不由一段写在代码里的自我
//! 介绍决定，也不由按角色分岔的预设决定。所以这里只有一段，不按 `role` 分岔。
//!
//! 这段指示是**声明**而不是常量：进程起来后把发布出来的提示词本子交进来，
//! 每次运行开头现取一次。改了声明，下一次运行就读到新的那句，不用发版。
//!
//! 没接上本子的进程（库的单元测试、独立跑的运行时）就是没有这段——空着，
//! 不在这里编一份兜底：兜底会让「改声明」看起来生效、实际没有。

use std::sync::{Arc, OnceLock};

use cog_core::PromptProvider;

static PROVIDER: OnceLock<Arc<dyn PromptProvider>> = OnceLock::new();

/// 把进程的提示词本子交给运行时；装配根在插件起来之后调一次。
pub fn install(provider: Arc<dyn PromptProvider>) {
    let _ = PROVIDER.set(provider);
}

/// 这一轮该给智能体的那段话。本子没接上、或没有这一条时是 `None`。
pub fn text() -> Option<String> {
    let pm = PROVIDER.get()?;
    pm.get("agent:default")
        .filter(|text| !text.trim().is_empty())
}

/// 同一段话后面跟的那一行：这个人是谁（id 与名字），它这一轮在哪个工作树里。
///
/// 名字与工号都是名册上那一行的读数（`EmployeeRecord::label`），这里不派生
/// 名字，只把它放到模型读得到的地方——不然被问「你是谁」时，答案只能从工具
/// 结果里猜，或者编一个。
///
/// id 也在这里，因为它是同事找得到这个人的那个东西：`send_to_agent` 要的是
/// 名册上的 agent id，模型不知道自己叫哪个 id，就没法让同事回信。
///
/// 工作树说的是**车道的名字**，不是绝对路径：路径在执行器那台机器上，这一侧
/// 的进程不持有它。编一个出来比不说更坏——模型会拿它去 `cd`，而那条路径在这
/// 一侧根本不存在。名字是两边都认的那个：命令落在哪条车道，就是这个名字。
/// 绝对路径由执行器在每条回复的 `workdir` 里给出，所以这一段不猜它，只告诉
/// 模型去哪读。
pub fn identity_note(
    agent_id: &str,
    display_name: Option<&str>,
    workspace: Option<&str>,
) -> Option<String> {
    let id = agent_id.trim();
    if id.is_empty() {
        return None;
    }
    let name = display_name.map(str::trim).filter(|n| !n.is_empty());
    let tree = workspace.map(str::trim).filter(|w| !w.is_empty());
    let mut note = format!("你在平台上的 id 是 `{id}`（同事给你发消息用这个 id）");
    if let Some(name) = name {
        note.push_str(&format!("，名字是「{name}」"));
    }
    note.push('。');
    if let Some(tree) = tree {
        note.push_str(&format!(
            "你这一轮的命令与文件落在工作树「{tree}」里（绝对路径在执行器那条机器上，\
             工具回复的 `workdir` 就是它；不必自己 `pwd` 去问）。"
        ));
    }
    Some(note)
}

#[cfg(test)]
mod tests {
    use super::identity_note;

    /// 三条都在：一句话里同时读得到 id、名字与车道，模型不必分三次问。
    #[test]
    fn the_identity_note_names_the_employee_and_its_workspace() {
        let note = identity_note(
            "kai-a9ab11fc",
            Some("E0412 林清和"),
            Some(&cog_core::worktree_lane("kai-a9ab11fc")),
        )
        .expect("an id is always known, so there is a note");
        assert!(note.contains("kai-a9ab11fc"), "{note}");
        assert!(note.contains("E0412 林清和"), "{note}");
        assert!(
            note.contains(&cog_core::worktree_lane("kai-a9ab11fc")),
            "{note}"
        );
    }

    /// 不知道名字就只说道路：编一个名字会让读的人以为名册上有这一位。
    #[test]
    fn a_run_without_a_roster_row_gets_the_workspace_but_no_name() {
        let note = identity_note(
            "no-one-yet",
            None,
            Some(&cog_core::worktree_lane("no-one-yet")),
        )
        .expect("the workspace alone is worth saying");
        assert!(
            note.contains(&cog_core::worktree_lane("no-one-yet")),
            "{note}"
        );
        assert!(!note.contains("名字"), "no name was claimed: {note}");
    }

    /// 没有 id 就没有这一行：这串是同事回信要用的地址，空着等于没有地址，
    /// 印一行空的会让模型以为照着念就有人收得到。
    #[test]
    fn a_run_without_an_id_gets_no_note() {
        assert!(identity_note("", Some("E0412 林清和"), Some("task-1")).is_none());
        assert!(identity_note("   ", None, None).is_none());
    }
}
