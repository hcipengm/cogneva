//! 有损的唯一表示：`Excerpt`。
//!
//! 值跨过一步的接口时只有两种形态——全量，或带引用。**全量**没有包装类型，完整值
//! 用它本来的类型；**有损**就是 `Excerpt`：一段摘要，加上一个能取回全文的引用。
//! 看到 `Excerpt` 就知道少了信息，并且回去的路是值的一部分。
//!
//! 擦除不另立类型：事实由 `redact_body` 写出的 `redacted` 事件承载（谁、为什么、
//! 原哈希与长度），「擦除必须带事实」由它的签名保证。

use std::fmt;

/// 一段摘要，连同取回全文的引用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Excerpt {
    shown: String,
    full_ref: String,
    total_chars: usize,
    purpose: &'static str,
}

impl Excerpt {
    /// `full_ref` 是取回全文的引用（`artifact://…` 这类），它随值一起走，
    /// 不是旁注：没有它，这段摘要就是「丢了且拿不回来」。
    pub fn new(
        text: &str,
        limit: usize,
        full_ref: impl Into<String>,
        purpose: &'static str,
    ) -> Self {
        let shown: String = text.chars().take(limit).collect();
        Self {
            total_chars: text.chars().count(),
            shown,
            full_ref: full_ref.into(),
            purpose,
        }
    }

    pub fn shown(&self) -> &str {
        &self.shown
    }

    pub fn full_ref(&self) -> &str {
        &self.full_ref
    }

    pub fn total_chars(&self) -> usize {
        self.total_chars
    }

    pub fn elided_chars(&self) -> usize {
        self.total_chars.saturating_sub(self.shown.chars().count())
    }

    pub fn purpose(&self) -> &'static str {
        self.purpose
    }
}

impl fmt::Display for Excerpt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}…[excerpt: {} of {} characters; full text at {}; for {}]",
            self.shown,
            self.shown.chars().count(),
            self.total_chars,
            self.full_ref,
            self.purpose
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 摘要必须带着回去的路：引用是值的一部分。
    #[test]
    fn an_excerpt_carries_the_way_back() {
        let excerpt = Excerpt::new(
            "把这一批的归属改回来",
            4,
            "artifact://conv/1",
            "日志里认出是哪一句",
        );
        assert_eq!(excerpt.shown(), "把这一批");
        assert_eq!(excerpt.full_ref(), "artifact://conv/1");
        assert_eq!(excerpt.elided_chars(), 6);
        let shown = excerpt.to_string();
        assert!(shown.contains("4 of 10"), "{shown}");
        assert!(shown.contains("artifact://conv/1"), "{shown}");
    }

    /// 没有引用就没有这个类型——这一条由构造函数签名保证：`full_ref` 不是可选的。
    #[test]
    fn the_constructor_requires_a_reference() {
        let source = include_str!("excerpt.rs");
        // 同 bound.rs：针在构造处拼出来，免得它命中这条测试自己的源码。
        let needle = format!("pub fn {}(text: &str, limit: usize, purpose:", "new");
        assert!(
            !source.contains(&needle),
            "有一个不需要引用的构造函数：那正是被打掉的「仅展示」"
        );
    }
}
