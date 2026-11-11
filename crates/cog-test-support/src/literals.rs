//! 一行生产代码里的双引号字面量：这条读法的唯一实现。
//!
//! 「从一行源码里读出双引号字面量」曾经在夹具里抄了十一遍。每一遍都只参数化那一小段
//! 真正不同的东西，于是抄出来的每一份都能单独改歪：有的把 `\"` 当成字面量的结尾，
//! 有的按字节读、把非 ASCII 的半个字符也算进词里，有的只读到一行里的第一个字面量。
//! 两份对同一行给出不同答案时，没有任何东西会红——用其中一份判出来的「全都合规」，
//! 另一份根本不会去问。
//!
//! 它住在这里而不是 `cog-core`：core 是契约宪法，只定义跨 crate 的 trait、类型、配置、
//! 错误与插件契约，不实现业务逻辑，也不做词法扫描。住在这里而不是某个夹具的 `common`：
//! 每个测试二进制都是一棵独立的 crate，跨 crate 的夹具够不着别人的 `common`，而
//! `cog-test-support` 是它们共同够得着的那一份。
//!
//! 它是 dev-only 的：生产代码不用这份读法，`crate_dependency_contract_test` 把
//! 「正常依赖指向它」判成 `linked into the artifact`。生产侧读声明的那几条判据
//! （`cog-core::contract::reflection` 的 `literal_field_assignment`）各自问的是别的
//! 形状，不枚举一行里的每一个字面量。
//!
//! 它必须证明自己读得对：转义、整字面量、行内第二处三条岔路各有一个反例，抄件们
//! 正是抄歪在这三处上。判据在下面 `tests` 里。

/// 一行生产代码里的一个双引号字面量。
#[derive(Clone, Copy, Debug)]
pub struct Literal<'a> {
    /// 两个引号之间的原文；反转义不做，声明值里不会有转义。
    pub value: &'a str,
    /// 开引号在行内的字节偏移。
    pub start: usize,
    /// 收引号在行内的字节偏移（不含）。
    pub end: usize,
    /// 字面量左边的那段，用来认出 `debug_struct(..).field("moderator", ..)` 这类标签。
    pub before: &'a str,
}

/// 一行里的每一个完整的双引号字面量，按出现顺序。
///
/// 转义认一份：`\"` 不是字面量的结尾。不认这一份的读取器会把 `"a\"b"` 读成两个
/// 字面量，而第二个的起点落在字符串中间——那之后的括号、名字、偏移量全都错位，
/// 错位在判词上不是红，是「那里没有」。
pub fn literals_in(line: &str) -> Vec<Literal<'_>> {
    let bytes = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        let start = i + 1;
        let mut j = start;
        while j < bytes.len() {
            match bytes[j] {
                b'\\' => j += 2,
                b'"' => break,
                _ => j += 1,
            }
        }
        // 收引号没找到：这一行剩下的部分不是一个完整字面量，不猜。
        if j >= bytes.len() {
            break;
        }
        out.push(Literal {
            value: &line[start..j],
            start,
            end: j,
            before: &line[..i],
        });
        i = j + 1;
    }
    out
}

/// [`literals_in`] 的取值面：按出现顺序，只要引号之间的文本。
///
/// 调用方要的是「这一段里的字面量」而不是位置时读这一份，别再各抄一遍剥引号的循环。
pub fn literal_values(text: &str) -> Vec<&str> {
    literals_in(text)
        .into_iter()
        .map(|literal| literal.value)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 读数只留一份的代价是它必须自己证明读得对：转义、整字面量、行内第二处这三处
    /// 各是一道以前抄歪过的岔路。
    #[test]
    fn a_literal_is_read_whole_and_an_escape_does_not_end_it() {
        assert_eq!(literal_values(r#"let a = "planner";"#), vec!["planner"]);
        assert_eq!(
            literal_values(r#"let a = "a\"b"; let b = "c";"#),
            vec![r#"a\"b"#, "c"],
            "反斜杠转义的引号不是字面量的结尾"
        );
        assert_eq!(
            literal_values(r#"let a = "x" + "y";"#),
            vec!["x", "y"],
            "一行里的每一个字面量都要读到"
        );
        assert_eq!(
            literal_values(r#"let a = "unterminated;"#),
            Vec::<&str>::new()
        );
    }

    /// 位置也是调用方读的东西：`before` 用来认 Debug 标签，`start`/`end` 用来切原文。
    /// 只判取值面的话，偏移量整体错一位也不会红。
    #[test]
    fn the_offsets_point_at_the_bytes_between_the_quotes() {
        let line = r#"let a = "one"; let b = "two";"#;
        let literals = literals_in(line);
        assert_eq!(literals.len(), 2);
        for literal in &literals {
            assert_eq!(literal.value, &line[literal.start..literal.end]);
        }
        assert_eq!(literals[1].before, r#"let a = "one"; let b = "#);
    }
}
