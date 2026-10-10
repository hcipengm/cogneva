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
//! one long line of JSON — is a single word however long it is. A caller that
//! bounds such a payload measures characters instead.
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

/// 一个词在估计口径里的代价。
///
/// 含 CJK 的词按字符逐个算，纯非 CJK 词按一个英文词算——后者与词长无关，
/// 正是「没有空白的巨块永远是 4 token」这条已知性质的来源。
fn word_cost(cjk_chars: usize, other_chars: usize) -> usize {
    if cjk_chars > 0 {
        cjk_chars * 2 + other_chars.div_ceil(4)
    } else {
        4 // 英文单词约 4 token
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
    /// 按字符算而不是按词算，所以「一个词」不会让一段中文变成 4 token——
    /// 无空白大块那条盲区只落在非 CJK 的整块上。
    #[test]
    fn a_run_of_cjk_without_spaces_is_priced_by_character() {
        assert_eq!(estimate_tokens("一二三四五"), 10);
    }

    /// 中英混排的一个词：汉字按 2、其余按四分之一计（取整向上，宁可多算），
    /// 同一个词里两类字符各算各的：2×2 + ceil(4/4) = 5。
    #[test]
    fn a_mixed_word_prices_both_scripts() {
        assert_eq!(estimate_tokens("部署done"), 5);
    }
}
