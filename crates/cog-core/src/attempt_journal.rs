//! 一次尝试的对话，按模型看到的样子逐条落盘——断点续跑的锚点。
//!
//! 为什么是「模型的对话」而不是「任务的进度」：一次尝试被杀掉之后，能接着干的
//! 唯一依据是它已经付费读过的那段上下文。进程里的一切（`ContextWindow`、步数、
//! 已交付结果）都随进程消失；盘上唯一忠实的副本，是每一条进入过上下文的
//! `Message`。所以这份日志记的就是那些消息，按顺序、原样。
//!
//! 与 `gateway_llm_exchange` 的分工：那张表记的是**发出去的字节**，脱敏、截断、
//! 只供人事后查一笔调用（它的 doc 就是这么写的）。续跑要的是**能原样交还给模型的
//! 消息**，脱敏过的字节把它交回去，就是把一段被改过的对话当成它自己的历史。
//! 所以两件事两张表，各自只存自己那一面。
//!
//! 落点选在进程级单例（与 `progress` 同一个套路）：写这条日志的是内核，而内核
//! 不认识数据库。装进来的是存储插件，它手里有池子。
//!
//! 三条纪律，缺一条这套东西就是新的故障源：
//!
//! - **写失败不停跑**。续跑是省钱的辅助，不是运行的前置条件：库慢、库断，
//!   这一轮照跑，只留一条 warn 与一个计数。把「记不下来」变成「跑不了」，
//!   等于用一条辅助设施换掉主线。
//! - **只记模型看到的那一份**。工具结果进上下文时会按窗口预算收缩
//!   （`ContextWindow::add_message`），日志记收缩之后的那条：续跑把另一份交
//!   回去，模型读到的历史就与它当时读到的不是同一个东西。
//! - **顺序就是发生顺序**。助手那条在**工具执行之前**记：记在之后，进程死在
//!   工具里就同时丢掉「模型说过什么」和「工具跑没跑」，而后者恰恰是续跑时
//!   唯一要判的那件事。

use async_trait::async_trait;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use crate::types::Message;

/// 一条日志是什么。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalKind {
    /// 一段用户输入（任务请求、被拒后的改写要求、绕圈提醒）。
    User,
    /// 模型自己说的话，连同它要调用的工具。**先于工具执行写盘。**
    Assistant,
    /// 一次工具调用的结果，就是模型读到的那一条。
    ToolResult,
    /// 这次尝试已经交付。写在这里之后，同一件任务的下一次运行从零开始——
    /// 交付过的对话不重放，重放它等于把已经交出去的东西再算一次。
    Closed,
}

impl JournalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            JournalKind::User => "user",
            JournalKind::Assistant => "assistant",
            JournalKind::ToolResult => "tool_result",
            JournalKind::Closed => "closed",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "user" => JournalKind::User,
            "assistant" => JournalKind::Assistant,
            "tool_result" => JournalKind::ToolResult,
            "closed" => JournalKind::Closed,
            _ => return None,
        })
    }
}

/// 一条日志。
#[derive(Debug, Clone)]
pub struct JournalEntry {
    /// 这次尝试内的序号，由存储分配，读回来时已经排好序。
    pub seq: u64,
    pub kind: JournalKind,
    /// 消息本体。`Closed` 没有消息。
    pub message: Option<Message>,
    /// 工具结果那一条对着的调用 id；其余为空。
    pub tool_call_id: Option<String>,
    /// 工具名，便于不解析消息就能读日志。
    pub tool: Option<String>,
    pub created_at: SystemTime,
}

impl JournalEntry {
    pub fn message(
        kind: JournalKind,
        message: Message,
        tool_call_id: Option<String>,
        tool: Option<String>,
    ) -> Self {
        Self {
            seq: 0,
            kind,
            message: Some(message),
            tool_call_id,
            tool,
            created_at: SystemTime::now(),
        }
    }

    pub fn closed() -> Self {
        Self {
            seq: 0,
            kind: JournalKind::Closed,
            message: None,
            tool_call_id: None,
            tool: None,
            created_at: SystemTime::now(),
        }
    }
}

/// 工具调用的副作用等级——续跑时**怎么处置一个没有结果的调用**，只有这三类。
///
/// 分类的判据是「重放它会不会在世界上多留一次效果」，不是「它读不读文件」：
/// 一次重复的读只花一次读的钱，一次重复的写、一次重复的提交会留下两个结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectClass {
    /// 只读：重放它除了再花一次调用，不改变任何东西。
    Read,
    /// 写文件：重放是幂等的，前提是先判「它是不是已经落过了」。
    Write,
    /// 有外部效果：绝不能盲目重放。不确定就交给模型去核对。
    Effect,
}

/// 工具名 → 副作用等级。
///
/// **默认是 `Effect`**：没点名的工具按最坏情况处置。名单漏一个只读工具，
/// 代价是续跑时多问模型一句；名单多写一个有副作用的工具，代价是同一笔提交
/// 落两次，而后者不可能事后修好。
pub fn effect_of(tool: &str) -> EffectClass {
    match tool {
        "read_file" | "raw_list" | "raw_fetch" | "read_task_ledger" | "list_agents"
        | "search_code" | "grep" | "glob" | "git_status" | "git_diff" | "command_output" => {
            EffectClass::Read
        }
        "write_file" | "edit_file" | "apply_patch" => EffectClass::Write,
        _ => EffectClass::Effect,
    }
}

/// 谁在存这份日志。
#[async_trait]
pub trait AttemptJournal: Send + Sync {
    /// 追加一条。存储分配 `seq`，同一个 agent 下的顺序就是追加顺序。
    async fn append(&self, agent_id: &str, entry: &JournalEntry) -> Result<(), String>;
    /// 按 `seq` 升序读回一个 agent 的全部日志（含已关闭的那些——是不是已关闭，
    /// 由最后一条是不是 [`JournalKind::Closed`] 决定，读者自己判）。
    ///
    /// 键是 agent 而不是任务：一件任务被重派时 `agent_id` 与上次相同，
    /// 日志于是接在上一段之后，而不是另起一份读不回来的东西。
    async fn load(&self, agent_id: &str) -> Result<Vec<JournalEntry>, String>;
}

fn slot() -> &'static Mutex<Option<Arc<dyn AttemptJournal>>> {
    static J: OnceLock<Mutex<Option<Arc<dyn AttemptJournal>>>> = OnceLock::new();
    J.get_or_init(|| Mutex::new(None))
}

/// 装进来。已经有就换掉：一个进程里装两次是「谁说了算」的问题，不是错误，
/// 后装的（部署时那一个）说了算。
pub fn install(journal: Arc<dyn AttemptJournal>) {
    *slot().lock().unwrap() = Some(journal);
}

/// 现在这一份，没装就是 `None`——内嵌运行、单元测试都没有。
pub fn current() -> Option<Arc<dyn AttemptJournal>> {
    slot().lock().unwrap().clone()
}

/// 写失败，累计。续跑是辅助设施，它坏了主线照跑，但坏到什么程度必须读得到：
/// 一个悄悄从不落盘的日志，读起来与「每一次尝试都顺利完成」一模一样。
pub fn record_write_failure() {
    WRITE_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub fn write_failures() -> u64 {
    WRITE_FAILURES.load(std::sync::atomic::Ordering::Relaxed)
}

static WRITE_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// 单测之间共享进程，装进来的那一份要能拿走。
pub fn reset_for_tests() {
    *slot().lock().unwrap() = None;
    WRITE_FAILURES.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_named_read_tool_never_pays_for_a_second_look() {
        assert_eq!(effect_of("read_file"), EffectClass::Read);
        assert_eq!(effect_of("read_task_ledger"), EffectClass::Read);
        // 树上的四种读法：搜索、找文件、看改了没有、看改了什么。它们没有一样
        // 会改树，续跑时重看去一遍是白付一次。
        assert_eq!(effect_of("search_code"), EffectClass::Read);
        assert_eq!(effect_of("grep"), EffectClass::Read);
        assert_eq!(effect_of("glob"), EffectClass::Read);
        assert_eq!(effect_of("git_status"), EffectClass::Read);
        assert_eq!(effect_of("git_diff"), EffectClass::Read);
    }

    #[test]
    fn file_writes_are_replayable_only_because_the_resume_checks_them_first() {
        assert_eq!(effect_of("write_file"), EffectClass::Write);
        assert_eq!(effect_of("edit_file"), EffectClass::Write);
        // 补丁是一次落一片的写：没点名的话它会掉进 Effect，那一格仍然安全，
        // 但「这条要重放」的判断从此只能靠猜。
        assert_eq!(effect_of("apply_patch"), EffectClass::Write);
    }

    /// 提交不在可重放的那一类：同一笔提交落两次，历史里就多一个不该有的
    /// 提交，而这件事事后修不掉。
    #[test]
    fn a_commit_is_an_effect_not_a_replayable_write() {
        assert_eq!(effect_of("git_commit"), EffectClass::Effect);
    }

    /// 名单上是 Effect 的，与名单上根本没有的，必须是同一类：一个不认识的工具
    /// 按「重放无害」处置，这一刻就决定了它可能落两次。
    #[test]
    fn an_unknown_tool_is_treated_as_an_effect_not_as_a_read() {
        assert_eq!(effect_of("git_push"), EffectClass::Effect);
        assert_eq!(effect_of("run_command"), EffectClass::Effect);
        assert_eq!(effect_of("a_tool_written_next_week"), EffectClass::Effect);
    }
}
