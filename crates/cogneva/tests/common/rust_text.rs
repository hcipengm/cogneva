//! 从 Rust 源码里读形状：每条只许有一份的那几个短规则。
//!
//! 它们短到每一份夹具都觉得自己顺手写一遍更省事——`snake_case` 写了三遍，
//! `raw_string_end` 两遍，`trailing_identifier` 两遍，还有一份叫 `is_word`、
//! 一份叫 `is_component`，是同一个判据的两个名字。这类抄件的毛病不在重复本身，
//! 在于抄歪了没有任何东西会响：两份对「什么算原始字符串的结尾」只要有一份记错，
//! 用它的那道普查就会把字符串里的字当成代码、或者把代码当成字符串，而判出来的
//! 结论仍然是一句「全都合规」。名字也不可靠——抄的人往往顺手改名，按名字找根本
//! 找不全，所以判据在 `the_fixture_reading_steps_are_written_once_test` 里按**函数体**
//! 比，不按名字比。
//!
//! 判据本身也有边界：这里收的是「读完一段 Rust 文本」的纯函数，读文件、走目录的
//! 那些在 `fsio`，普查产出的那些在 `rust_production`。

#![allow(dead_code)]

/// `CamelCase` → `camel_case`：一个类型名与它写出去／读进来的词之间的换算。
///
/// 三份夹具都靠它把变体名折成线上的字段名。折法只有一种，所以它只有一份：
/// 有一份把连续大写折成 `camel__case` 的那天，用它的门会用另一套词表去比，
/// 而两边都对不上时看起来像「树上真有一处写错了」。
pub fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (i, ch) in name.char_indices() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// 一个能被当成词来比的小写串：非空、不太长、只有小写字母数字与 `_ . - /`。
///
/// 两边问的是同一件事，只是叫法不同：`one_value_has_one_name_inside_its_crate`
/// 管它叫 `is_word`，`repeated_path_components_are_named_once` 叫它 `is_component`。
/// 这不是两个判据——放宽一处，另一处的普查就会多认一批「词」，而多认进去的
/// 那段文字仍然被当成合规的声明。
pub fn is_lower_word(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 40
        && value.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'-' | b'/')
        })
}

/// 从 `i` 起的原始字符串（`r"…"`、`r#"…"#`、带 `b`/`c` 前缀的）到哪儿结束。
///
/// `None` 表示这里不是一个原始字符串的开头。井号的个数必须两边相等才算收尾——
/// 少认一个井号，`r#"a"#` 就会在 `a` 之后那个引号上提前收尾，剩下的 `#` 与后面
/// 整段文字就被当成了代码。
pub fn raw_string_end(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i;
    if matches!(bytes.get(j), Some(b'b') | Some(b'c')) && bytes.get(j + 1) == Some(&b'r') {
        j += 1;
    }
    if bytes.get(j) != Some(&b'r') {
        return None;
    }
    j += 1;
    let mut hashes = 0;
    while bytes.get(j) == Some(&b'#') {
        hashes += 1;
        j += 1;
    }
    if bytes.get(j) != Some(&b'"') {
        return None;
    }
    j += 1;
    while j < bytes.len() {
        if bytes[j] == b'"' {
            let mut k = j + 1;
            let mut seen = 0;
            while seen < hashes && bytes.get(k) == Some(&b'#') {
                seen += 1;
                k += 1;
            }
            if seen == hashes {
                return Some(k);
            }
        }
        j += 1;
    }
    None
}

/// 一段文字结尾的那个标识符（`Self::X` 收 `X`，`foo(...)` 收空串）。
///
/// 两份夹具都拿它从 `=>` 左边、赋值右边取出被写的那个名字。取法只有一种：
/// 一份连 `::` 一起收进来、另一份不收，两边的词表就永远对不上，而红的是
/// 「树上写错了」，不是「两把尺子不一样长」。
pub fn trailing_identifier(text: &str) -> String {
    text.trim_end()
        .chars()
        .rev()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}
