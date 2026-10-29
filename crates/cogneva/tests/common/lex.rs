//! 一段源码里「哪里是字符串、哪里是注释、哪里是代码」：只许有一份方言表。
//!
//! 这段规则被写过两遍，而两遍不一样：
//!
//!   - `routes::strip_comments` 把 `'` 一律当引号，也不认 Rust 的原始字符串。
//!     于是 `&'static str` 里的那个 `'` 会开一段「字符串」，吃掉后面的代码，直到
//!     下一个 `'`——在那之间写着的名字、路径、括号都读不到，而「那里没有」与
//!     「那里有但没读到」在判词上同形。有一份夹具就是因为这个不敢用它，自己另写了
//!     一段散文（`word_constants_with_the_same_name_say_the_same_word_test`）。
//!   - `rust_production::brace_shapes` 认原始字符串、也把生命周期与字符字面量分开，
//!     但它只输出括号形状，不输出剥过注释的正文，于是「剥注释」这条路只能另写。
//!
//! 两条规则对同一个字节给出不同答案，而没有任何东西比较它们。这里只留一份：同一台
//! 状态机一次跑出**剥过注释的正文**和**逐行的括号形状**，方言差在三处——`'` 怎么
//! 认（Rust 看它后面几个字节里有没有收尾引号，JavaScript 一律当引号）、反引号认不认
//! （JavaScript 的模板字符串认，Rust 里反引号不是引号），以及**这一种文字有没有注释
//! 与引号**（从字符串字面量里取出来的那段文字什么都没有，每个字节都是它自己）。
//!
//! 每种方言各自是对的：JavaScript 的长单引号字符串（`'api/v1'`）在 Rust 的窗口判据
//! 下认不出来，而 Rust 的生命周期在 JavaScript 的规则下会吃掉整段代码。所以方言是
//! **调用方必须说的**，不是这里猜的——猜错的那一边不会红，只会少读。
//!
//! 这份不 import 仓里的任何别的东西，理由与 `fsio` 同：`rust_production` 会被按
//! `#[path]` 挂到别的 crate 根上，那里没有 `common`。
//!
//! 判据在 `the_fixture_reading_steps_are_written_once_test`。

#![allow(dead_code)]

/// 被读的那段源码是什么语言。只影响引号、原始字符串与注释的认法。
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// `"`、`'`（字符字面量或生命周期）、`r"…"`、`r#"…"#`、`b"…"`、`br"…"`、`c"…"`、`cr#"…"#`。
    Rust,
    /// `"`、`'`、`` ` ``，`\` 转义；没有原始字符串。
    JavaScript,
    /// 一段从字符串字面量里取出来的文字：**没有注释，也没有引号**，每个字节都是自己。
    ///
    /// 取的时候字面量的边界已经没了，剩下的是页面本身。再按引号读一遍有两处会塌：
    /// 一是 `https://…` 这样的地址在 JavaScript 的规则下是注释，整行之后全被空掉；
    /// 二是页面里成对的引号（HTML 属性、JS 字符串）跨过别的字面量就不配对，一次错位
    /// 之后整段都算字符串——两种塌法都是「一个括号都读不到」，而读不到在判词上不是红，
    /// 是「那里没有」。
    Fragment,
}

/// One line's brace shape: how the nesting changes, and whether a `{` opened.
#[derive(Clone, Copy, Default)]
pub struct BraceShape {
    pub delta: i32,
    pub opens: bool,
}

/// 一次扫描的两个产出：逐行的括号形状，与剥掉注释之后的正文。
pub struct Scan {
    /// 每一行的括号增减。末行之后的那一段也各占一行，与源文件的行一一对应。
    pub lines: Vec<BraceShape>,
    /// 正文，注释已去掉，字符串逐字保留，换行原样保留（块注释里的换行也留着：
    /// 按行读的消费者不该因为一段注释被删就看见两行合成一行）。
    pub code: String,
    /// 正文，注释、字符串与字符字面量的**内容**都换成空格，换行原样保留。
    ///
    /// 给「找一条声明」的读者用：散文里提到一个名字、字符串里拼出同一个名字，
    /// 都不算声明。与 [`Scan::code`] 只差在字面量要不要留——两条输出出自同一台
    /// 状态机，所以不存在「一边认原始字符串、另一边不认」。
    pub blanked: String,
}

/// 按行切开一段源码，同时拿去注释的正文。
///
/// 按行单独数括号不够：夹具字符串跨行，`"suggestions":[]}"#,` 收的是四行之前开的
/// 那个原始字符串，而那段文字里的 `}` 会提前结束一个被跳过的条目，此后半个测试模块
/// 都被读成产出代码。多读是吵人的，少读是安静的，而这两种里只有一种算门。
///
/// 三种看着像嵌套、其实不是的东西：字符串与字符字面量里的括号，以及 `&'a str` 里
/// 那个开头是生命周期而不是字符字面量的 `'`。
pub fn scan(text: &str, dialect: Dialect) -> Scan {
    let chars: Vec<char> = text.chars().collect();
    let mut out: Vec<BraceShape> = Vec::new();
    let mut code = String::with_capacity(text.len());
    let mut blanked = String::with_capacity(text.len());
    let mut shape = BraceShape::default();
    let mut i = 0;
    let mut block_comment = 0usize;
    // 普通字符串：记着收尾引号是哪一个。
    let mut quote: Option<char> = None;
    // 原始字符串：记着开头数了几个 `#`。
    let mut raw_hashes: Option<usize> = None;
    while i < chars.len() {
        let c = chars[i];
        if c == '\n' {
            out.push(shape);
            shape = BraceShape::default();
            code.push('\n');
            blanked.push('\n');
            i += 1;
            continue;
        }
        if block_comment > 0 {
            // Rust 的块注释**可以嵌套**：`/* /* */ */` 的结尾在第二个 `*/`。只找
            // 第一个 `*/` 的写法会把后半截注释当成代码读——而这一半在判词上与
            // 「那里真写了这个名字」同形。
            if c == '/' && chars.get(i + 1) == Some(&'*') {
                block_comment += 1;
                blanked.push_str("  ");
                i += 2;
                continue;
            }
            if c == '*' && chars.get(i + 1) == Some(&'/') {
                block_comment -= 1;
                blanked.push_str("  ");
                i += 2;
                continue;
            }
            blanked.push(' ');
            i += 1;
            continue;
        }
        if let Some(hashes) = raw_hashes {
            if c == '"' {
                let mut j = i + 1;
                let mut seen = 0;
                while seen < hashes && chars.get(j) == Some(&'#') {
                    seen += 1;
                    j += 1;
                }
                if seen == hashes {
                    while i < j {
                        code.push(chars[i]);
                        blanked.push(' ');
                        i += 1;
                    }
                    raw_hashes = None;
                    continue;
                }
            }
            code.push(c);
            blanked.push(' ');
            i += 1;
            continue;
        }
        if let Some(q) = quote {
            code.push(c);
            blanked.push(' ');
            if c == '\\' {
                // 反斜杠只转义紧跟着的那一个字符。往后多看一个字节会把行尾的转义引号
                // （`value: \"1\"`）里那个 `"` 当成字符串的收尾，此后每一行都读成代码。
                //
                // 换行留给自己那一支：它要发一行括号形状。在这里把它当转义字符吃掉，
                // 行号就从这里开始对不上，后面每一行都错一位。
                i += 1;
                if i < chars.len() && chars[i] != '\n' {
                    code.push(chars[i]);
                    blanked.push(' ');
                    i += 1;
                }
                continue;
            }
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            '/' if dialect != Dialect::Fragment && chars.get(i + 1) == Some(&'/') => {
                while i < chars.len() && chars[i] != '\n' {
                    blanked.push(' ');
                    i += 1;
                }
                continue;
            }
            '/' if dialect != Dialect::Fragment && chars.get(i + 1) == Some(&'*') => {
                block_comment = 1;
                blanked.push_str("  ");
                i += 2;
                continue;
            }
            '"' if dialect != Dialect::Fragment => {
                quote = Some('"');
                code.push(c);
                blanked.push(' ');
                i += 1;
                continue;
            }
            '`' if dialect == Dialect::JavaScript => {
                quote = Some('`');
                code.push(c);
                blanked.push(' ');
                i += 1;
                continue;
            }
            '\'' if dialect == Dialect::JavaScript => {
                quote = Some('\'');
                code.push(c);
                blanked.push(' ');
                i += 1;
                continue;
            }
            '\'' if dialect == Dialect::Rust => {
                // Past the backslash an escape covers **two** chars, the backslash
                // and the character it escapes: stopping after the backslash leaves
                // `j` sitting on the escaped quote, so a literal like `'\''` closes
                // on its own escape and every quote or brace after it on the same
                // line reads as code.
                let mut j = i + 1;
                if chars.get(j) == Some(&'\\') {
                    j += 2;
                }
                let mut close = None;
                while j < chars.len() && j <= i + 5 {
                    if chars[j] == '\'' {
                        close = Some(j);
                        break;
                    }
                    j += 1;
                }
                // 收不了尾的那个 `'` 是生命周期（`&'a str`）或标签，不是一个字符
                // 字面量的开头：它自己是代码，只跳过它。
                let (end, keep) = close.map_or((i + 1, true), |at| (at + 1, false));
                while i < end {
                    code.push(chars[i]);
                    blanked.push(if keep { chars[i] } else { ' ' });
                    i += 1;
                }
                continue;
            }
            'r' | 'b' | 'c' if dialect == Dialect::Rust => {
                let mut j = i + 1;
                if chars.get(j) == Some(&'"') {
                    quote = Some('"');
                    while i <= j {
                        code.push(chars[i]);
                        blanked.push(' ');
                        i += 1;
                    }
                    continue;
                }
                if c == 'b' || c == 'c' {
                    if chars.get(j) != Some(&'r') {
                        code.push(c);
                        blanked.push(c);
                        i += 1;
                        continue;
                    }
                    j += 1;
                }
                let mut hashes = 0;
                while chars.get(j) == Some(&'#') {
                    hashes += 1;
                    j += 1;
                }
                if chars.get(j) == Some(&'"') {
                    raw_hashes = Some(hashes);
                    while i <= j {
                        code.push(chars[i]);
                        blanked.push(' ');
                        i += 1;
                    }
                    continue;
                }
                code.push(c);
                blanked.push(c);
                i += 1;
                continue;
            }
            '{' => {
                shape.delta += 1;
                shape.opens = true;
            }
            '}' => shape.delta -= 1,
            _ => {}
        }
        code.push(c);
        blanked.push(c);
        i += 1;
    }
    out.push(shape);
    Scan {
        lines: out,
        code,
        blanked,
    }
}

/// 只要剥过注释的正文。
pub fn strip(text: &str, dialect: Dialect) -> String {
    scan(text, dialect).code
}

/// 只要「注释与字面量都空掉」的那份正文：给「找一条声明」的读者用。
pub fn blank(text: &str, dialect: Dialect) -> String {
    scan(text, dialect).blanked
}

/// 顶层逗号切分：括号里、字符串里、字符字面量与原始字符串里的逗号都不算。
///
/// 这条规则和上面那台状态机是同一件事的两半——「哪里是一段字符串」只有这一个答案，
/// 而要不要切在某处逗号上，问的正是它。所以切分不自己认引号：层数在 `blank` 过的
/// 那份正文上数，逗号在它上面找，字节却从原文上切，字面量因此逐字留在每一段里。
///
/// 以前这条规则有四份：`js_literal::split_top_level` 认 `"` `'` 反引号与转义，
/// `rust_text::split_top_level` 认原始字符串，`llm_spend_verdict_contract_test` 里
/// 还有一份只认 `"` 的。四份对同一个字节可以给出四个答案，而没有东西比较它们——
/// 认错了引号的那一份不会红，它只是把一条实参切成两条、或者把两条并成一条，各自的
/// 判据照着自己那份读下去，结论仍然是一句「全都合规」。
pub fn split_top_level(text: &str, dialect: Dialect) -> Vec<&str> {
    let blanked = blank(text, dialect);
    // `scan` 的每个分支都把每个源字符恰好落成 `blanked` 里的一个字符，所以这两份
    // 逐字符对齐；`starts` 给每个字符记下它在原文里的字节位置。
    let mut starts: Vec<usize> = text.char_indices().map(|(at, _)| at).collect();
    starts.push(text.len());
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (index, c) in blanked.chars().enumerate() {
        let Some(&at) = starts.get(index) else { break };
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&text[starts[start]..at]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&text[starts[start]..]);
    parts
}

/// 只要一行（或一段没有换行的文本）的括号增减，给扫描器自己的用例用。
pub fn brace_delta(line: &str, dialect: Dialect) -> i32 {
    scan(line, dialect)
        .lines
        .first()
        .copied()
        .unwrap_or_default()
        .delta
}
