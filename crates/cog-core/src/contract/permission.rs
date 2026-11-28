//! 「要不要动手」那一问里，这次改动长什么样。
//!
//! 只放形状，不放判定：算出它的是认识工具入参的那一层（`cog_agent::permission`），
//! 把它搬到界面上的是一条连接（`cog_gateway`），画它的是页面。三层都要看见同一个
//! 名字，任一侧另写一份，弹窗里说「改的是哪几行」就会与工具真正要做的改动错开——
//! 而这一格正是人点头之前唯一在看的东西。
//!
//! 一件说得清的事：`overwrite` 里没有改动前的内容。它是工具的入参，这一问没有去
//! 读树；拿不到就说拿不到，不把整份新文件画成满屏的 `+` 行、让人以为这个文件原来
//! 不存在。改动有多全由 `truncated` 交代：被截断过的那一份读起来像整个改动。

use serde::{Deserialize, Serialize};

/// 这份改动该按哪种画法给人看。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionDiffKind {
    /// 一处替换：`before` 换成 `after`（`edit_file`）。
    Edit,
    /// 整份被写进去的文件：`after` 是全文，`before` 不知道（`write_file`）。
    Overwrite,
    /// 一份统一 diff：`after` 是补丁原文（`apply_patch`）。
    Patch,
}

/// 一次改动在问句里长什么样。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionDiff {
    /// 动的是哪个文件。补丁自己写着路径时以补丁为准，否则取入参里那一格。
    pub path: String,
    pub kind: PermissionDiffKind,
    /// 给人读的那一行：这份东西是什么。
    pub label: String,
    /// 改动前。`overwrite` 与 `patch` 没有这一格，是空串。
    pub before: String,
    /// 改动后。`patch` 放的是补丁原文。
    pub after: String,
    /// 有一边被截断了（见 [`crate::contract::permission::PermissionDiff`] 顶上的说明）。
    pub truncated: bool,
}
