//! 属主写下的那几句话，随每一轮开头的基础指令一起交给员工。
//!
//! 它和平台原则不是一回事：原则是一份对所有人同时生效的账，改一条是改账上的
//! 一行；这里的每一条属于**写它的那个人**，只在他自己的对话里出现。它相当于
//! 项目根下那份 `CLAUDE.md` 里的一段——人写一句「这个仓不用 tab」，之后每一段
//! 对话都读到它，不必每回重说。
//!
//! 读不到不是这一轮的失败：拿不到这段约定的对话仍然有基础指令、仍然能干这件活。
//! 读失败记一条 warn，这一轮照跑；「一条都没写」与「写了但读不到」在读数面上
//! 分得开——前者的段落根本不存在，后者会留下一行日志。

use async_trait::async_trait;

use cog_core::SFResult;

/// 属主写下的一条约定。
///
/// 身份是 `id`（由写入面按内容算出来），不是序号：人撤掉一条之后其余几条的
/// 序号会变，而界面上手里拿着的撤销号不该跟着变。`text` 是原文，不加工——
/// 把它改写成一句更顺的话，读它的模型读到的就不是人写的那句了。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEntry {
    pub id: String,
    pub text: String,
}

/// 这一轮该读谁写的约定。
///
/// 实现放在装配层：读的是这个平台里人写下的那些条目，而这里只认识「一集条目」。
#[async_trait]
pub trait OwnerMemory: Send + Sync {
    async fn entries(&self) -> SFResult<Vec<MemoryEntry>>;
}

/// 这一集条目进系统前缀的那一段；一条都没有时是 `None`。
///
/// 空集不产出一段空标题：「属主写了以下约定」后面一条都没有，读起来像一条被
/// 截断的规则，而事实上就是这个人在这一轮之前什么都没写。
pub fn section(entries: &[MemoryEntry]) -> Option<String> {
    if entries.is_empty() {
        return None;
    }
    let mut text = String::from(
        "属主在之前的对话里写下的约定（只在属主本人的对话里生效；照它们做，\
         不要把其中一条当成建议）：",
    );
    for entry in entries {
        text.push_str("\n- ");
        text.push_str(&entry.text);
    }
    Some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, text: &str) -> MemoryEntry {
        MemoryEntry {
            id: id.into(),
            text: text.into(),
        }
    }

    /// 一条都没写时没有这一段：空标题会被读成一段被截断的规则。
    #[test]
    fn nothing_written_adds_no_section_at_all() {
        assert_eq!(section(&[]), None);
    }

    /// 写下的原文一字不改地进那一段，一条一行。
    #[test]
    fn each_entry_appears_verbatim_on_its_own_line() {
        let text = section(&[
            entry("a", "这个仓不用 tab，缩进一律两个空格"),
            entry("b", "提交信息用英文"),
        ])
        .expect("two entries produce a section");
        assert!(
            text.contains("\n- 这个仓不用 tab，缩进一律两个空格"),
            "{text}"
        );
        assert!(text.contains("\n- 提交信息用英文"), "{text}");
        // 一段话里不能把两条并成一行：读的人与模型都要能一条一条对上。
        let lines: Vec<&str> = text.lines().filter(|line| line.starts_with("- ")).collect();
        assert_eq!(lines.len(), 2, "{text}");
    }
}
