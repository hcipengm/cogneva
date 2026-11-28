//! 把一张表换成另一侧（流上的投影、另一个库的表）时的三段路，以及「能不能往下一段走」。
//!
//! 三段：先双写、两侧逐行相等；再把读切到新的一侧；最后让旧的一侧只读、再删掉。
//! 关键不在顺序，在**每段的判据是读数而不是日程**：往下一段走的唯一依据是刚才量的那次
//! 逐行对照——只在旧侧、只在新侧的条数都是零。
//!
//! 对不上时停在哪一段就是哪一段：不往回退（退回去会让已经切过去的读者读到一个正在被
//! 改写的一侧），也不往前推（推过去会让读者开始读一份缺行的账）。两条都各有一个读数
//! 能说清它为什么停。

/// 一段路。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapStage {
    /// 两侧都在写，读还在旧的一侧。
    DoubleWrite,
    /// 读已经切到新的一侧，旧的一侧仍在写。
    ReadNew,
    /// 旧的一侧不再写，只等删。
    OldReadOnly,
    /// 旧的一侧删掉了；没有下一段。
    Dropped,
}

impl SwapStage {
    pub fn as_str(self) -> &'static str {
        match self {
            SwapStage::DoubleWrite => "double-write",
            SwapStage::ReadNew => "read-new",
            SwapStage::OldReadOnly => "old-read-only",
            SwapStage::Dropped => "dropped",
        }
    }

    /// 从台账/命令上的词读回来；认不出的词是 `None`（读不到不是「第一段」）。
    pub fn parse(word: &str) -> Option<Self> {
        Some(match word {
            "double-write" => SwapStage::DoubleWrite,
            "read-new" => SwapStage::ReadNew,
            "old-read-only" => SwapStage::OldReadOnly,
            "dropped" => SwapStage::Dropped,
            _ => return None,
        })
    }

    /// 下一段。
    pub fn next(self) -> Option<Self> {
        match self {
            SwapStage::DoubleWrite => Some(SwapStage::ReadNew),
            SwapStage::ReadNew => Some(SwapStage::OldReadOnly),
            SwapStage::OldReadOnly => Some(SwapStage::Dropped),
            SwapStage::Dropped => None,
        }
    }
}

/// 刚才量的一次逐行对照：只在旧侧的行数、只在新侧的行数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Comparison {
    pub only_old: i64,
    pub only_new: i64,
}

impl Comparison {
    /// 两侧逐行相等。
    pub fn is_equal(&self) -> bool {
        self.only_old == 0 && self.only_new == 0
    }
}

/// 能不能从 `current` 走到下一段。
///
/// 两个方向都拒绝，而且拒绝的理由不同：相等不成立就停在这一段（读数说出了差多少）；
/// 已经到最后一段就没有下一段（不是「可以再走一次」）。
pub fn advance(current: SwapStage, comparison: &Comparison) -> Result<SwapStage, String> {
    let Some(next) = current.next() else {
        return Err(format!(
            "已经走到最后一段（{}）：没有下一段可走",
            current.as_str()
        ));
    };
    if !comparison.is_equal() {
        return Err(format!(
            "两侧不是逐行相等（只在旧侧 {} 行、只在新侧 {} 行）：停在 {}，不往回退也不往前推",
            comparison.only_old,
            comparison.only_new,
            current.as_str()
        ));
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn equal() -> Comparison {
        Comparison {
            only_old: 0,
            only_new: 0,
        }
    }

    #[test]
    fn the_three_stages_are_a_fixed_sequence() {
        assert_eq!(SwapStage::DoubleWrite.next(), Some(SwapStage::ReadNew));
        assert_eq!(SwapStage::ReadNew.next(), Some(SwapStage::OldReadOnly));
        assert_eq!(SwapStage::OldReadOnly.next(), Some(SwapStage::Dropped));
        assert_eq!(SwapStage::Dropped.next(), None);
        for stage in [
            SwapStage::DoubleWrite,
            SwapStage::ReadNew,
            SwapStage::OldReadOnly,
            SwapStage::Dropped,
        ] {
            assert_eq!(SwapStage::parse(stage.as_str()), Some(stage));
        }
        assert_eq!(SwapStage::parse("half-way"), None);
    }

    #[test]
    fn a_difference_stops_the_walk_where_it_is() {
        let diff = Comparison {
            only_old: 3,
            only_new: 0,
        };
        let error =
            advance(SwapStage::DoubleWrite, &diff).expect_err("a difference stops the walk");
        assert!(error.contains("只在旧侧 3 行"), "{error}");
        assert!(error.contains("double-write"), "{error}");
        // 相等就往下走一段，恰好一段。
        assert_eq!(
            advance(SwapStage::DoubleWrite, &equal()),
            Ok(SwapStage::ReadNew)
        );
    }

    #[test]
    fn the_last_stage_has_no_next_even_when_the_sides_agree() {
        let error = advance(SwapStage::Dropped, &equal()).expect_err("no next stage");
        assert!(error.contains("没有下一段"), "{error}");
    }
}
