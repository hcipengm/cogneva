//! principle: feedback_an_estimate_used_as_a_lower_bound_must_be_monotone
//! principle: feedback_fixing_a_blind_spot_recalls_the_bounds_written_against_it
//! What a piece of text costs a window, estimated the same way on both sides.
//!
//! Two callers put a number on text and then act on it: the agent's window
//! decides what to drop with it, and the memory extractor decides how much of a
//! transcript to send. Their bounds are read against each other in practice — a
//! payload one of them accepted is the payload the other pays for — so the unit
//! has one definition. A second copy would not be a second opinion but a second
//! unit, and the two would disagree exactly where the text stopped being English.
//!
//! The estimate counts words, which is what fits text a model will read and
//! unfits text it will not: a payload with no whitespace at all — a binary dump,
//! one long line of JSON — is a single word. A word is priced by the characters
//! in it, so such a payload is priced by its length rather than by the fact that
//! it happens to contain no spaces: the estimate does not go blind on the text
//! it cannot recognize.
//!
//! It is an estimate, not a tokenizer: no model's vocabulary is consulted, and
//! the answer is only meaningful against another estimate from this function.

/// 简化的 token 估算。
/// CJK 字符每个算 2 token（保守）；其余按字符数 /4 粗估（英文约 4 字符
/// 1 token）。CJK 必须按字符计而非字节：UTF-8 一个汉字 3 字节，按字节
/// 会把中文上下文高估 3 倍，窗口提前触发裁剪。
pub fn estimate_tokens(text: &str) -> usize {
    Parts::new(text).map(|(_, _, cost)| cost).sum()
}

/// 非 CJK 字符每多少个算一个 token（英文约 4 字符 1 token）。
///
/// 公开是因为别的 crate 里也有按这一档算字符预算的地方：估算口径一处写死、
/// 别处各写一份 4，剪裁与估算就会按两个比例走，同一段文本两处算出来的窗口占用
/// 对不上。
// cap: bound — 别处各写一份 4，剪裁与估算就会按两个比例走，同一段文本两处算出来的窗口占用对不上
pub const CHARS_PER_TOKEN: usize = 4;

/// 一个英文词的代价，也是任何一个词的下限。
///
/// 这是旧口径给每个非 CJK 词的定价，留着当**下限**是为了让短词的价一分不变：
/// 两个调用方的既有预算都是按它定过的，下限一动，同一段英文的窗口账就整体变了。
/// 它与 `CHARS_PER_TOKEN` 同源——一个词大约四个字符一个 token——所以一个词超过
/// 这一档的长度之后，涨价由字符数自己给出，不需要另立一个阈值。
// cap: bound — 与 CHARS_PER_TOKEN 同源（一个词约四个字符一个 token）；词长超过这一档之后涨价由字符数自己给出，不需要另立阈值
const ENGLISH_WORD_TOKENS: usize = 4;

/// 一个词在估计口径里的代价。
///
/// 含 CJK 的词按字符逐个算（CJK 每个 2），纯非 CJK 词取「按 `CHARS_PER_TOKEN`
/// 折算」与「一个英文词的代价」里大的那个：短词与旧口径逐字同价，长词随长度涨。
/// 一个不含空白的整块是一个词，于是它的价随它的长度走——长度就是这种文本
/// 唯一的成本，没有空格不该是它免费的理由。
///
/// 下限加在**词**上、不加在分支上：混排词里的非 CJK 部分**不比它单独成词时便宜**。
/// 否则给一个词添一个汉字会让它降价（`aa` 记 4、`a中` 记 3），估算就不再单调——
/// 而估算是预算的**下界**，一段内容多加一点反而算得更少，是按它裁剪的那一侧
/// 唯一不能有的方向。汉字本身仍是 2（一个纯汉字词不受这条影响）。
fn word_cost(cjk_chars: usize, other_chars: usize) -> usize {
    (cjk_chars * 2 + other_chars.div_ceil(CHARS_PER_TOKEN)).max(non_cjk_part_cost(other_chars))
}

/// 一个词里**纯非 CJK 的那一部分**单独成词时的代价。
///
/// 没有非 CJK 字符就是 0：一个词的全部字符都是汉字时，它的价只有汉字那一半，
/// 不该被一个并不存在的英文词抬起来。
fn non_cjk_part_cost(other_chars: usize) -> usize {
    if other_chars == 0 {
        0
    } else {
        other_chars
            .div_ceil(CHARS_PER_TOKEN)
            .max(ENGLISH_WORD_TOKENS)
    }
}

/// 估计口径下的分词：每个词的（起始字符下标、字符数、代价）。
///
/// 估计本身也走这一份切法，所以「值多少 token」与「装得下多少字符」不会
/// 各切各的——两处切法一分叉，预算就变成按一份文本切、按另一份文本算。
struct Parts<'a> {
    chars: std::str::Chars<'a>,
    index: usize,
}

impl<'a> Parts<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            chars: text.chars(),
            index: 0,
        }
    }
}

impl Iterator for Parts<'_> {
    ///（起始字符下标、字符数、代价）。
    type Item = (usize, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        // 词前的空白不属于任何词、也不计代价（按词算），但下标要跟着走。
        let (start, first) = loop {
            let ch = self.chars.next()?;
            let at = self.index;
            self.index += 1;
            if !ch.is_whitespace() {
                break (at, ch);
            }
        };
        let mut cjk = usize::from(is_cjk(first));
        let mut other = usize::from(!is_cjk(first));
        let mut len = 1usize;
        loop {
            match self.chars.next() {
                Some(ch) if ch.is_whitespace() => {
                    self.index += 1;
                    break;
                }
                Some(ch) => {
                    if is_cjk(ch) {
                        cjk += 1;
                    } else {
                        other += 1;
                    }
                    len += 1;
                    self.index += 1;
                }
                None => break,
            }
        }
        Some((start, len, word_cost(cjk, other)))
    }
}

/// CJK 的判定范围：汉字、CJK 标点、全角形式。
fn is_cjk(c: char) -> bool {
    ('\u{4e00}'..='\u{9fff}').contains(&c)
        || ('\u{3000}'..='\u{303f}').contains(&c)
        || ('\u{ff00}'..='\u{ffef}').contains(&c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cjk_character_costs_more_than_a_latin_one() {
        // 按字节计会把中文高估成英文的 3 倍，按字符计才落在保守而不是失真的
        // 那一侧：一个汉字约 1 token 上下，取 2 是留余量。
        assert!(estimate_tokens("你好世界") > estimate_tokens("Hello"));
    }

    #[test]
    fn text_with_no_words_costs_nothing() {
        assert_eq!(estimate_tokens(""), 0);
        assert_eq!(estimate_tokens("  \n\t "), 0);
    }

    /// 词是按空白切的，所以空白不计代价：一个词内部的字符才计价。
    #[test]
    fn whitespace_between_words_costs_nothing() {
        assert_eq!(estimate_tokens("alpha beta"), 8);
        assert_eq!(estimate_tokens("alpha  \n\t beta"), 8);
    }

    /// 连写的汉字是**一个**词，整块按字符数计价：五个字 10 token。CJK 分支
    /// 按字符算而不是按词算，所以「一个词」不会让一段中文降价成 4 token。
    #[test]
    fn a_run_of_cjk_without_spaces_is_priced_by_character() {
        assert_eq!(estimate_tokens("一二三四五"), 10);
    }

    /// 修复只动「不含空白的巨块恒记 4」这一格，短词的价必须逐字不变：两个
    /// 调用方的既有预算口径都按旧价定过，动了下限或换了取整，同一段英文的
    /// 窗口账就整体变了——那是退化，不是修复。
    ///
    /// 涨价起点由「`CHARS_PER_TOKEN` 字符 1 token」这一档自己给出（17 字符
    /// 才超过旧口径给每个词定的 4），所以下面量的是档位边界，不是某个被挑中
    /// 的阈值。
    #[test]
    fn a_short_word_costs_what_it_always_did() {
        assert_eq!(estimate_tokens("hello"), 4);
        assert_eq!(estimate_tokens("a b c"), 12);
        assert_eq!(estimate_tokens(&"z".repeat(16)), 4);
        assert_eq!(estimate_tokens(&"z".repeat(17)), 5);
    }

    /// 无空白、非 CJK 的整块按**长度**计价，不按「它是一个词」计价。
    ///
    /// 修复前 `word_cost` 的 else 分支不看字符数、恒返 4，于是二进制倾倒与单行
    /// 大 JSON 在窗口账面上永远只值 4 token，`trim_if_needed` 判它没超限——这一条
    /// 要红的正是那个形状，所以它量的是 MB 级的那一段，不是最大到几千字符的样本。
    ///
    /// 断言写成关系：同一个词长一倍价就长一倍，且落在「`CHARS_PER_TOKEN` 字符
    /// 1 token」这一档的量级上。钉死一个数会被换单位或换取整搞红，而这里要守的
    /// 从来不是那个数。
    #[test]
    fn a_run_without_whitespace_is_priced_by_its_length() {
        let one = estimate_tokens(&"A".repeat(1 << 20));
        let two = estimate_tokens(&"A".repeat(2 << 20));
        assert_eq!(two, one * 2, "同一个词长一倍，价就该长一倍");
        assert!(
            ((1 << 18) / 2..=(1 << 18) * 2).contains(&one),
            "1 MiB 的无空白块记了 {one} token，不在 4 字符 1 token 这一档上"
        );
        // 空白是唯一让整块降价的形状，而它一出现就分词：同样 1 MiB 的
        // 「单字符 + 空格」由每个短词的下限定价，比实心块贵。
        assert!(estimate_tokens(&"A ".repeat(1 << 19)) > one);
    }

    /// 中英混排的一个词：汉字按 2、其余按四分之一计（取整向上，宁可多算），
    /// 同一个词里两类字符各算各的：2×2 + ceil(4/4) = 5。
    #[test]
    fn a_mixed_word_prices_both_scripts() {
        assert_eq!(estimate_tokens("部署done"), 5);
    }

    /// 加一个字符不会让估算**变便宜**。
    ///
    /// 单调性是估算是**预算下界**的前提：按它裁剪的那一侧要能说「内容多了，账只会
    /// 更多」。一段文本多加一个字符反而算得更少，裁剪就没有方向了。这里量的是**性质**
    /// 不是几个数——逐字符把可加的都试一遍，改口径时它替我们守住这个方向。
    #[test]
    fn adding_a_character_never_lowers_the_estimate() {
        let bases = [
            "",
            "a",
            "aa",
            "aaaa",
            "aaaaaaaaa",
            "zzzzzzzzzzzzzzzzz",
            "alpha",
            "中",
            "中文",
            "部署done",
        ];
        for base in bases {
            let before = estimate_tokens(base);
            for ch in ['a', 'Z', '4', '.', '中', '文', ' '] {
                let after = estimate_tokens(&format!("{base}{ch}"));
                assert!(
                    after >= before,
                    "「{base}」记 {before} token，加一个 {ch:?} 之后记 {after}：内容多了账反而少了"
                );
            }
        }
    }

    /// 混排词里的非 CJK 部分**不比它单独成词时便宜**：`aa` 记 4，`a中` 就不该记 3。
    ///
    /// 这是上一条性质当初破的那个形状——旧口径把下限只加在非 CJK 分支上，于是给一个
    /// 词添一个汉字会让它降价。修法是把下限加在**词**上，不是把汉字抬价：
    #[test]
    fn a_cjk_character_never_discounts_the_word_it_joins() {
        assert_eq!(estimate_tokens("aa"), 4);
        assert_eq!(estimate_tokens("a中"), 4, "添了一个汉字，价不该低于添之前");
        // 反过来钉住修法没走错路：纯汉字词仍按 2/字，一个字记 2（没被那条下限抬高）。
        assert_eq!(estimate_tokens("中"), 2);
        // 非 CJK 部分本来就超过下限的词，价一分不变。
        assert_eq!(estimate_tokens("部署done"), 5);
        assert_eq!(estimate_tokens("K3s沙盒"), 5);
    }
}
