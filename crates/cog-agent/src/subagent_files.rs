//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//! 子任务「看过哪些文件」的读口：从它自己的 `tool_call` 事实里取，不由它自报。
//!
//! 让子任务在答案里列清单，等于把「模型记得报」当成事实来源：它可能漏、可能编，
//! 而读者分不清。工具调用已经进了事件流（S36），每一次文件类调用带一个 `path`
//! （S37），于是清单是**读出来的**：一次查询，按子任务这一趟的 `unit_of_work`。
//!
//! 读口是 `Option`：没有它的时候交回值里那一格是 `null`——「这个部署没接读口」
//! 与「这一趟没看过任何文件」是两件事，用 `[]` 写前者会让后者永远读成零。

use async_trait::async_trait;

use cog_core::SFResult;

/// 一趟工作碰过的文件，按第一次出现排序、去重。
#[async_trait]
pub trait FilesSeen: Send + Sync {
    async fn files_for(&self, unit_of_work: &str) -> SFResult<Vec<String>>;
}
