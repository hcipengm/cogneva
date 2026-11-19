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

use std::collections::BTreeMap;

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

/// `fn <name>(…)` 的函数体：从声明里的 `(` 走到花括号收尾，取中间那一段。
///
/// 两份夹具各写过一遍，逐字相同——它们读的是同一棵树上同一种声明。抄件之间只差在
/// 括号配平那一句，而那正是最该共享的一句：配平问的是「哪里是代码」，答案在
/// [`super::lex`] 与 [`super::routes`] 里各有一半，谁都不该在夹具里再写第三遍。
pub fn rust_function_body(source: &str, name: &str) -> Option<String> {
    let at = source.find(&format!("fn {name}("))?;
    let brace = at + source[at..].find('{')?;
    let end = super::routes::matching_brace(source, brace, super::lex::Dialect::Rust)?;
    Some(source[brace + 1..end].to_string())
}

/// 一段文字结尾的那个标识符（`Self::X` 收 `X`，`foo(...)` 收空串）。
///
/// 两份夹具都拿它从 `=>` 左边、赋值右边取出被写的那个名字。取法只有一种：
/// 一份连 `::` 一起收进来、另一份不收，两边的词表就永远对不上。
///
/// 它原来和「按顶层逗号切分」住在一起：切枚举变体与切 match 臂看起来是两件事，抄的
/// 人各写一遍 `split_arms` / `split_items`，而抄歪的那一份不会红——它只是把一条变体
/// 切成两条、或者把两条并成一条，两条判据都会照着自己那份读下去。切法现在在
/// [`super::lex`]：认引号的那台状态机只该有一份，调用点说明方言是 Rust。
///
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

/// `cli.rs` 的参数表：`Command::X` 变体 → argv 里那个词。
///
/// 判决面与投递面分开读：一条判据知道哪些工作负载「跑的是哪个词」，另一条知道
/// 「哪些词起了订阅者」，两条要放在一起才判得出「起了订阅者的那个词，它的工作
/// 负载接上日志后端了没有」。所以这份表只有一份——抄歪的那一份会让某一侧认不出
/// 一行（比如 `(\"x\", Command::Y),` 写成两行），而认不出的一行在判决上就是
/// 「这棵树里没有这种工作负载」，与「全都接上了」同形。
pub fn cli_argv_tokens(source: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in source.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("(\"") else {
            continue;
        };
        let Some((token, tail)) = rest.split_once("\", Command::") else {
            continue;
        };
        let Some(variant) = tail.strip_suffix("),") else {
            continue;
        };
        out.insert(variant.trim().to_owned(), token.to_owned());
    }
    out
}

/// `main.rs` 里 `STANDALONE_LOG_SERVICES` 那张表：变体 → 它写出的流名。
///
/// 按 `Command::` 扫，不按行切：条目允许折行（`Command::SandboxExecutor` 的服务名
/// 就写在下一行），按行切的解析器会悄悄丢掉那一行，而丢掉的正是「沙盒执行器接没接
/// 日志后端」这一条。
pub fn standalone_log_services(source: &str) -> BTreeMap<String, String> {
    let start = source
        .find("const STANDALONE_LOG_SERVICES")
        .expect("STANDALONE_LOG_SERVICES is gone: the log gates read the wiring from it");
    let region = &source[start..];
    let region = &region[..region.find("\n];").unwrap_or(region.len())];
    // An entry starts either as a one-liner (`(Command::X, name),`) or as a bare
    // `(` with the variant on the next line — which is how the sandbox executor's
    // row is written, and the row a line-at-a-time parser silently loses.
    let entries = region
        .lines()
        .filter(|line| {
            let line = line.trim();
            line == "(" || line.starts_with("(Command::")
        })
        .count();
    let mut out = BTreeMap::new();
    let mut rest = region;
    while let Some(at) = rest.find("Command::") {
        rest = &rest[at + "Command::".len()..];
        let variant: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if variant.is_empty() {
            continue;
        }
        // The service name is either a string literal or a constant path; both are
        // only ever used in a failure message.
        let tail = &rest[variant.len()..];
        let entry = &tail[..tail.find("(Command::").unwrap_or(tail.len())];
        let service = match entry.find('"') {
            Some(quote) => entry[quote + 1..]
                .split('"')
                .next()
                .unwrap_or("")
                .to_owned(),
            None => entry
                .trim_end_matches([',', ')', '\n', ' '])
                .trim()
                .to_owned(),
        };
        out.insert(variant, service);
    }
    assert_eq!(
        out.len(),
        entries,
        "STANDALONE_LOG_SERVICES 有 {entries} 行以 `(Command::` 开头的条目，却只解析出 {} \
         个（{:?}）：解析器认不出的那些子命令，在用它判决的门里整条消失",
        out.len(),
        out
    );
    out
}

/// 体里所有 `字段: 值`，按字节偏移给出位置。
///
/// 不像按逗号切块那样读：默认值里的行是可能嵌套的
/// （`breaker: Some(CircuitBreakerConfig { failure_threshold: 5 })`），按逗号切
/// 会把里面那几个字段留在外层的值里，数就漏了。这里逐个词找「小写标识符 + 冒号」，
/// 值读到下一个逗号或右花括号为止。
pub fn struct_fields(body: &str) -> Vec<(usize, String, String)> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_lowercase() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let name = &body[start..i];
            let mut j = i;
            while j < bytes.len() && bytes[j] == b' ' {
                j += 1;
            }
            // `::` 是路径分隔符，不是字段赋值。
            if j < bytes.len() && bytes[j] == b':' && bytes.get(j + 1) != Some(&b':') {
                let mut k = j + 1;
                while k < bytes.len() && bytes[k] == b' ' {
                    k += 1;
                }
                let value_start = k;
                while k < bytes.len() && !matches!(bytes[k], b',' | b'}' | b'\n') {
                    k += 1;
                }
                out.push((
                    start,
                    name.to_string(),
                    body[value_start..k].trim().to_string(),
                ));
                // 停在冒号后面而不是值的末尾：值是嵌套结构时，里面的字段还在这个
                // 值里，跳过整个值就再也看不见它们了。
                i = j + 1;
                continue;
            }
            continue;
        }
        i += 1;
    }
    out
}

/// 哨兵：`0` 不是谁挑的数，是「没写」这件事本身。`0`、`0.0`、`0.0f64` 都算。
pub fn is_sentinel_zero(value: &str) -> bool {
    let trimmed = value.trim();
    let digits: String = trimmed
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '_' || *c == '.')
        .filter(|c| *c != '_')
        .collect();
    !digits.is_empty() && digits.parse::<f64>() == Ok(0.0)
}

/// 一个只由数字组成的值：`30`、`1_000`、`4.1`、`24 * 1024`、`3600u64`。
///
/// 只要有一个部分不是以数字开头（`MILLIS_PER_SECOND`、`per_second`），它就是一条由
/// 名字组成的表达式——那个名字的来源归它自己的声明管。
pub fn bare_number(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || is_sentinel_zero(value) {
        return None;
    }
    let mut any = false;
    for part in value.split(['*', '+', '/']) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let first = part.chars().next()?;
        if !first.is_ascii_digit() {
            return None;
        }
        if !part
            .chars()
            .all(|c| c.is_ascii_digit() || c == '_' || c == '.' || c.is_ascii_lowercase())
        {
            return None;
        }
        any = true;
    }
    any.then(|| value.to_string())
}
