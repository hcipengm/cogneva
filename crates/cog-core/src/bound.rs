//! 界只能由两种东西构造：一条读数，或一条具名约定。
//!
//! 没有第三个构造函数，也没有公开字段：任何 `Bound` 值在类型上就已经回答了
//! 「它凭什么取这个数」。这比在一行旁边写 `// cap:` 强一档——注释写错了不会有
//! 任何命令失败，类型写错了过不了编译；普查器于是只需数「裸数字」，而无从误判
//! 「这个标记算不算来源」。
//!
//! 两种合法形态：
//!
//! - [`Bound::derived`]：值由一条读数算出，并点名一条「换输入值就跟着变」的测试。
//! - [`Bound::fixed`]：值由某条约定给定（协议、地址、结构），并点名是哪一条。
//!
//! 裸数字仍然是合法的**常量**（`SECONDS_PER_DAY` 这种定义性的数），但它不能
//! 被当成界使用：接受界的函数签名收 [`Bound`]，调用方拿一个裸数字就过不了编译。

use std::fmt;

/// 一条界的来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundSource {
    Derived {
        /// 哪一条读数算出它。
        reading: &'static str,
        /// 换输入值它就跟着变的那条测试。
        proof: &'static str,
    },
    Fixed {
        /// 哪一条协议／地址／结构约定给定它。
        agreement: &'static str,
    },
}

impl BoundSource {
    pub fn is_derived(&self) -> bool {
        matches!(self, BoundSource::Derived { .. })
    }
}

/// 一个数，连同它凭什么取这个值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound<T> {
    value: T,
    source: BoundSource,
}

impl<T: Copy> Bound<T> {
    /// 由一条读数算出：读数变了，值跟着变（`proof` 是那条换输入就红的测试）。
    pub fn derived(value: T, reading: &'static str, proof: &'static str) -> Self {
        Self {
            value,
            source: BoundSource::Derived { reading, proof },
        }
    }

    /// 由某条具名约定给定。
    pub fn fixed(value: T, agreement: &'static str) -> Self {
        Self {
            value,
            source: BoundSource::Fixed { agreement },
        }
    }

    pub fn get(&self) -> T {
        self.value
    }

    pub fn source(&self) -> BoundSource {
        self.source
    }
}

impl<T: fmt::Display + Copy> fmt::Display for Bound<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bound_carries_where_its_number_came_from() {
        let derived = Bound::derived(16usize, "p95 认领耗时", "the_batch_follows_the_reading");
        assert!(derived.source().is_derived());
        assert_eq!(derived.get(), 16);

        let fixed = Bound::fixed(50u32, "A2A 规范：page_size 不写按 50");
        assert!(!fixed.source().is_derived());
        assert_eq!(fixed.get(), 50);
    }

    /// 没有第三个构造函数：`Bound { value, .. }` 在别的模块里写不出来，裸数字
    /// 进不了这一层。这条测试读的是源码本身——字段不可见是编译期的事，而在
    /// 本模块里能写出来会让「任何 Bound 都有来源」这句话失去依据。
    #[test]
    fn there_is_no_constructor_without_a_source() {
        let source = include_str!("bound.rs");
        // 针不能以整串出现在本文件里：这一条读的就是本文件，写成一整串的针会
        // 命中它自己，于是测试永远红、而源码里有没有那个构造函数根本读不出来。
        let needle = ["pub fn ", "new("].concat();
        assert!(
            !source.contains(&needle),
            "`Bound::new(value)` 会让一个数在没有来源的情况下成为界"
        );
        assert!(
            source.contains("value: T,\n    source: BoundSource,"),
            "字段是公开的：任何模块都能构造一个没有来源的 Bound"
        );
    }
}
