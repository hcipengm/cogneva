//! 从一条尝试日志里读出「接着从哪儿跑」。
//!
//! 这一段是纯的：给它日志，产出要交还给模型的消息、以及**没有结果的工具调用**
//! 是哪几个。怎样处置那几个调用要动工具，那是运行时的活，不在这里。
//!
//! 判据只有一条：**助手那条先于工具落盘**，所以「日志里有助手那条、没有某个
//! 调用的结果」当且仅当那次调用执行到一半进程没了。少了这条顺序，未执行与
//! 已执行不可分辨，续跑就只能二选一地猜。

use cog_core::attempt_journal::{JournalEntry, JournalKind};
use cog_core::Message;

/// 一次没有结果的工具调用。
#[derive(Debug, Clone, PartialEq)]
pub struct Doubt {
    pub call_id: String,
    pub tool: String,
    pub arguments: serde_json::Value,
}

/// 读出来的续跑计划。
#[derive(Debug, Default)]
pub struct ResumePlan {
    /// 按发生顺序交还给上下文的消息（用户、助手、工具结果）。
    pub messages: Vec<Message>,
    /// 没有结果的调用，按它们在助手那条里的顺序。
    pub doubts: Vec<Doubt>,
    /// 已经花掉的回合数——这次尝试的迭代预算从它之后算。
    pub assistant_turns: u32,
}

/// 这件任务的日志要不要接着跑；要，就给出怎么接着跑。
///
/// 空日志与**以关闭结尾**的日志都返回 `None`：前者没跑过，后者已经交付过。
/// 交付过的不重放——重放它等于把已经交出去的东西再算一次。
///
/// 日志按 agent 记，而一个 agent 会在同一件任务上跑很多次（`agent_id_for`
/// 只由任务与角色算出来，交付过的任务再派一次还是这个名字）。所以切点不是
/// 「第一条」，是**最后一条 `closed` 之后的第一条**：从整份日志开头重放，等于
/// 把上一个已交付尝试的整段对话交还给模型，它会以为自己上次就停在这里，
/// 接着一条早就交付完的路继续走。整份都交付过（最后一条就是 `closed`）时
/// 没有可续的那一段，与空日志同答。
pub fn plan(entries: &[JournalEntry]) -> Option<ResumePlan> {
    let last = entries.last()?;
    if last.kind == JournalKind::Closed {
        return None;
    }

    // 最后一个 closed 之后才是这一次尝试。没有 closed 就是开头。
    let start = entries
        .iter()
        .rposition(|e| e.kind == JournalKind::Closed)
        .map_or(0, |i| i + 1);
    let entries = &entries[start..];

    let mut out = ResumePlan::default();
    for entry in entries {
        if let Some(message) = &entry.message {
            out.messages.push(message.clone());
        }
        if entry.kind == JournalKind::Assistant {
            out.assistant_turns += 1;
        }
    }

    // 最后一条助手消息里的调用，哪些已经有结果。
    let Some(last_assistant) = entries
        .iter()
        .rev()
        .find(|e| e.kind == JournalKind::Assistant)
    else {
        return Some(out);
    };
    let Some(message) = &last_assistant.message else {
        return Some(out);
    };
    let answered: std::collections::HashSet<&str> = entries
        .iter()
        .filter(|e| e.kind == JournalKind::ToolResult)
        .filter_map(|e| e.tool_call_id.as_deref())
        .collect();
    for call in message.tool_calls() {
        if !answered.contains(call.id.as_str()) {
            out.doubts.push(Doubt {
                call_id: call.id.clone(),
                tool: call.name.clone(),
                arguments: call.arguments.clone(),
            });
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::Message;
    use serde_json::json;

    fn user(text: &str) -> JournalEntry {
        JournalEntry::message(JournalKind::User, Message::user(text), None, None)
    }

    fn assistant_with(calls: &[(&str, &str)]) -> JournalEntry {
        let blocks: Vec<cog_core::ContentBlock> = calls
            .iter()
            .map(|(id, name)| cog_core::ContentBlock::ToolCall {
                id: (*id).to_string(),
                name: (*name).to_string(),
                arguments: json!({"path": "a.rs"}),
                thought_signature: None,
            })
            .collect();
        JournalEntry::message(
            JournalKind::Assistant,
            Message::assistant(blocks),
            None,
            None,
        )
    }

    fn result_of(call_id: &str, tool: &str) -> JournalEntry {
        JournalEntry::message(
            JournalKind::ToolResult,
            Message::tool_result_text(call_id, tool, "ok"),
            Some(call_id.to_string()),
            Some(tool.to_string()),
        )
    }

    /// 一次干净的崩溃点：助手说了三个调用，只回来一个结果。回来的那个不再
    /// 执行，另外两个都算不确定——它们要么没开始，要么开始了一半，处置一样。
    #[test]
    fn an_interrupted_batch_leaves_every_unanswered_call_in_doubt() {
        let entries = vec![
            user("do it"),
            assistant_with(&[
                ("c1", "read_file"),
                ("c2", "run_command"),
                ("c3", "write_file"),
            ]),
            result_of("c1", "read_file"),
        ];
        let plan = plan(&entries).expect("an open attempt resumes");
        assert_eq!(
            plan.doubts
                .iter()
                .map(|d| d.call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["c2", "c3"]
        );
        assert_eq!(plan.assistant_turns, 1);
        assert_eq!(plan.messages.len(), 3);
    }

    /// 交付过的尝试不再续：它已经交出去了。
    #[test]
    fn a_closed_attempt_does_not_resume() {
        let entries = vec![user("do it"), JournalEntry::closed()];
        assert!(plan(&entries).is_none());
    }

    #[test]
    fn no_journal_at_all_means_a_fresh_start() {
        assert!(plan(&[]).is_none());
    }

    /// 同一个 agent 在同一件任务上跑第二次。上一次已经交付完（closed），
    /// 这一次停在第二个调用上。续跑要交回的是**这一次**的一段，不是整份日志：
    /// 从开头重放会把上一次那条已经交付的 `call-old` 也交回去，模型会以为
    /// 自己正卡在那条不该再做的事上。
    #[test]
    fn a_second_attempt_on_the_same_agent_resumes_only_from_the_last_close() {
        let entries = vec![
            user("old ask"),
            assistant_with(&[("call-old", "write_file")]),
            result_of("call-old", "write_file"),
            JournalEntry::closed(),
            user("new ask"),
            assistant_with(&[("call-new", "read_file")]),
        ];
        let plan = plan(&entries).expect("the open attempt resumes");
        assert_eq!(plan.assistant_turns, 1);
        assert_eq!(plan.messages.len(), 2);
        let text = serde_json::to_string(&plan.messages).unwrap();
        assert!(
            !text.contains("old ask"),
            "the delivered attempt must not replay: {text}"
        );
        assert!(
            !text.contains("call-old"),
            "the delivered call must not replay: {text}"
        );
        assert!(text.contains("new ask"));
        assert_eq!(
            plan.doubts
                .iter()
                .map(|d| d.call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["call-new"]
        );
    }

    /// 一轮结束在模型的话上（没有工具调用）时，没有可疑调用；续跑只是把这段
    /// 历史交回去，再问一次。
    #[test]
    fn a_turn_that_ended_in_words_leaves_nothing_in_doubt() {
        let entries = vec![
            user("do it"),
            JournalEntry::message(
                JournalKind::Assistant,
                Message::assistant_text("done"),
                None,
                None,
            ),
        ];
        let plan = plan(&entries).expect("still open");
        assert!(plan.doubts.is_empty());
        assert_eq!(plan.assistant_turns, 1);
    }
}
