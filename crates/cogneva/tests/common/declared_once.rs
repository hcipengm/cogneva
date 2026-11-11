//! 一条声明只写在一个地方，别处读它：这条控制律的唯一实现。
//!
//! 树上的「不许硬编码」门各自都有同一副骨架：从声明处读出值，走遍 `crates/*/src`
//! 的生产行，找出又写了一遍的地方，减去登记过的「同形不同物」，报出剩下的，再证明
//! 登记表没有过期、这次读到的题目足够多。骨架被抄了十几遍，每一遍只参数化那一小段
//! 真正不同的东西——`join("<name>")` 里的上下文、整字面量的比对、以标记开头的字面量。
//! 抄出来的每一份都能单独改歪：有的按字节读、有的按字符读，有的认转义、有的不认。
//! 两份对同一行给出不同答案时，没有任何东西会红——这正是这一族缺陷的样子：
//! **同一份规则被写多遍，而没有任何东西比较它们**。
//!
//! 所以这里只留一份：
//!
//! * [`Wall`] 声明一道墙：题目、同形不同物时为什么不会响、声明值、每个值怎么算
//!   「又写了一遍」（[`Spell`]）以及哪个文件/目录拥有它（声明本身）。
//! * [`Wall::sites`] 走全树的生产行，按墙的拼法找出每一处再拼写。
//! * [`Report`] 做减法（登记过的同形不同物）、判过期（登记了却不再指向现场）、
//!   把剩下的现场按一种格式报出来。
//! * [`literals_in`] 是「一行里的双引号字面量」那条读法的唯一实现：转义由它认，
//!   行边界由它认，调用方不再各自写一遍。
//!
//! 墙的自证也在这里，不在每个调用方：`sites` 先用一条合成文本问自己能不能认出
//! 自己声明的值，认不出就红；整字面量判据还要证明更长的字面量不算一处。一道只会
//! 在空集上通过的墙，与一道被删掉的墙同形。
//!
//! 判据在 `every_declared_once_gate_shares_the_one_reader_test`：任何夹具再写一份
//! 字面量读取器，要么改读这里，要么在它的表里写明这是另一种方言、另一种形状。

#![allow(dead_code)]

use std::fmt;

use crate::common::rust_production::{
    production_numbered_lines, production_sources, relative_name,
};

/// 一个声明值怎么才算被「又写了一遍」。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Spell {
    /// 整个字符串字面量与声明值相等：`"planner"` 算，`"the planner decides"` 不算。
    Literal,
    /// 字面量以声明值开头：带内标记 `"terminal_env_failure: 没有产物"` 算，
    /// 与它同形但更长的名字不算。
    Prefix,
    /// 这一行里任何位置出现声明值，字面量边界不看。只给不可能作为别的词的一部分
    /// 出现的值用（带头尾分隔符的名字），否则会把散文也算成拼写。
    Text,
}

/// 一个声明值，以及谁拥有它。
#[derive(Clone, Debug)]
pub struct Declared {
    /// 声明里的名字，只用于报错时点名。
    pub name: String,
    pub value: String,
    pub spell: Spell,
    /// 拥有这个值的文件或目录：那里的生产代码可以写它本身。`None` = 谁都不可以。
    ///
    /// 目录按前缀认（`crates/cog-core` 认它下面的每一份源码），文件按全等认。
    pub owner: Option<String>,
}

impl Declared {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            spell: Spell::Literal,
            owner: None,
        }
    }

    pub fn spelled_as_prefix(mut self) -> Self {
        self.spell = Spell::Prefix;
        self
    }

    pub fn spelled_as_text(mut self) -> Self {
        self.spell = Spell::Text;
        self
    }

    /// 声明本身写在这里；它下面的生产代码可以写这个词。
    pub fn owned_by(mut self, owner: impl Into<String>) -> Self {
        let mut owner = owner.into();
        while owner.ends_with('/') {
            owner.pop();
        }
        self.owner = Some(owner);
        self
    }

    fn owned_here(&self, file: &str) -> bool {
        self.owner.as_deref().is_some_and(|owner| {
            file == owner
                || file
                    .strip_prefix(owner)
                    .is_some_and(|rest| rest.starts_with('/'))
        })
    }
}

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

/// `debug_struct("..").field("moderator", &self.moderator.is_some())` 里的那个词
/// 是字段标签：它不落到配置里、不命名任何执行体、不变成 id。
fn is_debug_label(before: &str) -> bool {
    before.trim_end().ends_with(".field(")
}

/// 一道「声明只写一次」的墙。
pub struct Wall<'a> {
    subject: &'a str,
    why: &'a str,
    declared: Vec<Declared>,
    context: Option<&'a str>,
    skip_debug_labels: bool,
    floor: usize,
    ceiling: usize,
}

impl<'a> Wall<'a> {
    /// `subject` 是「又写了一遍」的那个东西（「角色名」），`why` 是同形不同物时
    /// 为什么不会响——报错时原样接在题目后面。
    pub fn new(subject: &'a str, why: &'a str) -> Self {
        Self {
            subject,
            why,
            declared: Vec::new(),
            context: None,
            skip_debug_labels: false,
            floor: 0,
            ceiling: 0,
        }
    }

    pub fn declared(mut self, declared: Vec<Declared>) -> Self {
        self.declared = declared;
        self
    }

    /// 只有含这个标记的行才读（`join(`、`.field(`……）。其余行上的同一个词是
    /// 另一种形状，不是这堵墙的题目。
    pub fn context(mut self, marker: &'a str) -> Self {
        self.context = Some(marker);
        self
    }

    /// `Debug` 的字段标签不是声明值的第二处拼写。
    pub fn skip_debug_labels(mut self) -> Self {
        self.skip_debug_labels = true;
        self
    }

    /// 至少读到这么多个声明。读不到就是这堵墙没有题目，不能绿。
    pub fn floor(mut self, minimum: usize) -> Self {
        self.floor = minimum;
        self
    }

    /// 现场数必须**少于**这个数。比这棵树里可能有的多得多，说明读错了段落——
    /// 多读与少读在这里都表现为一句「全都合规」。
    pub fn ceiling(mut self, maximum: usize) -> Self {
        self.ceiling = maximum;
        self
    }

    /// 全树里每一处又写了一遍的地方。
    ///
    /// 读的是 `production_sources`：`crates/*/src` 的全部源码与 crate 根的
    /// `build.rs`，测试半边与注释不算。新加的 crate 按构造就在题目里，不需要
    /// 谁来登记。
    pub fn sites(&self, root: &std::path::Path) -> Vec<Site> {
        assert!(
            !self.declared.is_empty(),
            "这道墙一个声明值都没有：空集上的「一处都没有」不是判据"
        );
        assert!(
            self.declared.len() >= self.floor,
            "{} 只读到 {} 个声明（下限 {}）：读声明的那一段变了，不是树变干净了",
            self.subject,
            self.declared.len(),
            self.floor
        );
        self.prove_it_can_see_its_own_values();

        let mut out = Vec::new();
        for path in production_sources(root) {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let file = relative_name(root, &path);
            out.extend(self.read_text(&file, &text));
        }
        assert!(
            self.ceiling == 0 || out.len() < self.ceiling,
            "数出 {} 处{}，比这棵树里可能有的多得多（上限 {}）：说明读错了段落。\
             多读进来的那些会把这堵墙的判词变成一句「全都合规」",
            out.len(),
            self.subject,
            self.ceiling
        );
        out
    }

    /// 按这道墙自己的判词跑完整条控制律：全树取现场、比对、报。
    ///
    /// 没有豁免的墙直接调这里——「取现场 + `Report::new` + `assert_clean`」那一串
    /// 就不再在每个夹具里各抄一遍。同形不同物的墙用 [`Wall::sites`] 自己带登记表。
    pub fn assert_clean(&self, root: &std::path::Path) {
        let sites = self.sites(root);
        Report::new(self.subject, self.why, &sites, &[]).assert_clean();
    }

    /// 一段生产文本里的现场。
    ///
    /// 与 [`Wall::sites`] 同一段读法；分开是为了让调用方拿一段合成文本问
    /// 「这道墙认不认得出第二处」（`read_text` 不计下限、上限与自证——那些是
    /// 对全树说的）。
    pub fn read_text(&self, file: &str, text: &str) -> Vec<Site> {
        let owned: Vec<bool> = self
            .declared
            .iter()
            .map(|declared| declared.owned_here(file))
            .collect();
        let mut out = Vec::new();
        for (number, line) in production_numbered_lines(text) {
            if let Some(marker) = self.context {
                if !line.contains(marker) {
                    continue;
                }
            }
            let literals = literals_in(line);
            for (index, declared) in self.declared.iter().enumerate() {
                if owned[index] {
                    continue;
                }
                if self.spells(line, &literals, declared) {
                    out.push(Site {
                        file: file.to_string(),
                        line: number,
                        value: declared.value.clone(),
                        text: line.trim().to_string(),
                    });
                }
            }
        }
        out
    }

    fn spells(&self, line: &str, literals: &[Literal<'_>], declared: &Declared) -> bool {
        let usable =
            |literal: &Literal<'_>| !(self.skip_debug_labels && is_debug_label(literal.before));
        match declared.spell {
            Spell::Literal => literals
                .iter()
                .any(|literal| usable(literal) && literal.value == declared.value),
            Spell::Prefix => literals
                .iter()
                .any(|literal| usable(literal) && literal.value.starts_with(&declared.value)),
            Spell::Text => line.contains(&declared.value),
        }
    }

    /// 这堵墙得先证明自己认得出自己声明的值。
    fn prove_it_can_see_its_own_values(&self) {
        let Some(first) = self.declared.first() else {
            return;
        };
        let value = &first.value;
        let line = match self.context {
            Some(marker) => format!("x{marker}\"{value}\");"),
            None => format!("let x = \"{value}\";"),
        };
        let literals = literals_in(&line);
        assert!(
            self.spells(&line, &literals, first),
            "这道墙读不出自己声明的值 {value:?}（拼法 {:?}，上下文 {:?}）：\
             连自己声明的这一个都认不出，全树上的绿只是没读到",
            first.spell,
            self.context
        );
        if first.spell == Spell::Literal {
            let longer = format!("let x = \"{value}_longer\";");
            assert!(
                !self.spells(&longer, &literals_in(&longer), first),
                "整字面量判据把更长的字面量 {value:?}_longer 也算成了一处拼写：\
                 词表里的值会被它顺带判红，判词从此不可信"
            );
        }
    }
}

/// 一处又写了一遍的地方。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Site {
    pub file: String,
    pub line: usize,
    pub value: String,
    pub text: String,
}

impl Site {
    fn render(&self) -> String {
        format!(
            "  {}:{}: {}  <- {}",
            self.file, self.line, self.value, self.text
        )
    }
}

/// 一处同形不同物的登记。
pub struct Exemption {
    pub file: &'static str,
    pub value: &'static str,
    pub why: &'static str,
}

impl Exemption {
    pub const fn new(file: &'static str, value: &'static str, why: &'static str) -> Self {
        Self { file, value, why }
    }
}

impl fmt::Display for Exemption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}（{}）", self.file, self.value, self.why)
    }
}

/// 一次普查的结果：现场、登记、以及报出来的那一种格式。
pub struct Report<'a> {
    subject: &'a str,
    why: &'a str,
    sites: &'a [Site],
    exemptions: &'a [Exemption],
    staleness: Option<&'a [Site]>,
}

impl<'a> Report<'a> {
    pub fn new(
        subject: &'a str,
        why: &'a str,
        sites: &'a [Site],
        exemptions: &'a [Exemption],
    ) -> Self {
        Self {
            subject,
            why,
            sites,
            exemptions,
            staleness: None,
        }
    }

    /// 一个调用跑了两条判据（整字面量与带内标记），豁免可以发给其中任一条：
    /// 过期要看两条现场的并集，只看一条会把另一条的登记立刻报成过期。
    pub fn staleness_against(mut self, all: &'a [Site]) -> Self {
        self.staleness = Some(all);
        self
    }

    pub fn assert_clean(&self) {
        let unexpected: Vec<&Site> = self
            .sites
            .iter()
            .filter(|site| {
                !self
                    .exemptions
                    .iter()
                    .any(|rule| rule.file == site.file && rule.value == site.value)
            })
            .collect();

        let all = self.staleness.unwrap_or(self.sites);
        let stale: Vec<&Exemption> = self
            .exemptions
            .iter()
            .filter(|rule| {
                !all.iter()
                    .any(|site| site.file == rule.file && site.value == rule.value)
            })
            .collect();

        if !unexpected.is_empty() {
            panic!(
                "这些地方把{}又写了一遍，而不是读声明。{}\n{}\n\
                 要么改成读声明，要么在登记表里写清为什么这里的同一个拼写指的是另一件事。",
                self.subject,
                self.why,
                unexpected
                    .iter()
                    .map(|site| site.render())
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
        if !stale.is_empty() {
            panic!(
                "这些豁免已经不指向任何一处重写了：{}\n\
                 豁免是给一处现场发的许可，现场没了许可就该收回——留着它，\
                 下一次有人在这条路径上重写同一个字符串时就没有判据拦了。",
                stale
                    .iter()
                    .map(|rule| format!("\n  {rule}"))
                    .collect::<Vec<_>>()
                    .join("")
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 读数只留一份的代价是它必须自己证明读得对：转义、整字面量、字段标签这三处
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

    #[test]
    fn a_wall_reads_whole_literals_and_not_longer_words() {
        let wall = Wall::new("角色名", "").declared(vec![Declared::new("planner", "planner")]);
        let hits = |text: &str| wall.read_text("crates/x/src/a.rs", text).len();
        assert_eq!(hits(r#"let a = "planner";"#), 1);
        assert_eq!(hits(r#"let a = "the planner decides";"#), 0);
        assert_eq!(hits(r#"let a = "planner_v2";"#), 0);
        assert_eq!(hits("// let a = \"planner\";\n"), 0, "注释不算");
        assert_eq!(
            hits("#[cfg(test)]\nmod tests { const R: &str = \"planner\"; }\n"),
            0,
            "测试半边不算"
        );
    }

    #[test]
    fn a_debug_label_is_not_a_second_spelling() {
        let wall = Wall::new("角色名", "")
            .skip_debug_labels()
            .declared(vec![Declared::new("moderator", "moderator")]);
        let text = r#".field("moderator", &self.moderator.is_some())"#;
        assert!(wall.read_text("crates/x/src/a.rs", text).is_empty());
        assert_eq!(
            wall.read_text("crates/x/src/a.rs", r#"let r = "moderator";"#)
                .len(),
            1
        );
    }

    #[test]
    fn a_prefix_wall_sees_the_marker_at_the_head() {
        let wall = Wall::new("带内标记", "").declared(vec![Declared::new(
            "FAILURE_PREFIX",
            "terminal_env_failure",
        )
        .spelled_as_prefix()]);
        let hits = |text: &str| wall.read_text("crates/x/src/a.rs", text).len();
        assert_eq!(hits(r#"let m = "terminal_env_failure: 没有产物";"#), 1);
        assert_eq!(hits(r#"let m = "terminal_env_failure";"#), 1);
        assert_eq!(hits(r#"let m = "is_terminal_env_failure";"#), 0);
    }

    #[test]
    fn an_owner_may_spell_its_own_value() {
        let wall = Wall::new("角色名", "").declared(vec![
            Declared::new("PLANNER", "planner").owned_by("crates/cog-core/src/roles.rs")
        ]);
        let declared_here = r#"pub const PLANNER: &str = "planner";"#;
        assert!(wall
            .read_text("crates/cog-core/src/roles.rs", declared_here)
            .is_empty());
        assert_eq!(
            wall.read_text("crates/cog-core/src/other.rs", declared_here)
                .len(),
            1,
            "拥有者是那个文件，不是整个 crate"
        );
    }

    #[test]
    fn a_context_limits_the_wall_to_the_lines_it_names() {
        // 自证要在 `sites` 里真的会红：这里给一个上下文标记，合成行里也放上它，
        // 于是这道墙是能认出自己的；换成没放进上下文的墙就得红。
        let wall = Wall::new("数据根条目", "")
            .context("join(")
            .declared(vec![Declared::new("POLICIES_SUBDIR", "policies")]);
        let found = wall.read_text("crates/x/src/a.rs", r#"let p = dir.join("policies");"#);
        assert_eq!(found.len(), 1);
        let outside = wall.read_text("crates/x/src/a.rs", r#"let p = "policies";"#);
        assert!(outside.is_empty(), "没有上下文标记的行不是这堵墙的题目");
    }
}
