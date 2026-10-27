//! principle: feedback_no_hardcoding_config_surface
//!
//! 同一件事写了两遍：一个指标属于哪个维度。
//!
//! `EvalMetric::dimension()` 按变体给出维度，编译器保证每个变体都有答案——
//! 漏一个变体编译不过。`report::dimension_for_metric()` 按**名字的前缀**给出
//! 维度，那是另一份手写的表，两份之间没有任何东西比较。
//!
//! 漂移的方向是安静的：改 `name()` 产出的一个词（`goal_fulfillment` 变
//! `goal_achievement`），枚举这一侧立刻跟着走，字符串那一侧的前缀永远匹配不上，
//! 那个指标掉进兜底的 `D9`——报表里 D1 少一项、D9 多一项，两次都是合法读数，
//! 没有任何东西会失败。「这个维度这轮没测到」与「这个指标被归错了维度」在表上
//! 同形。
//!
//! 所以这条门不比两份表，只比**答案**：对 `metric.rs` 里每一个声明了名字的变体，
//! 两份实现必须给出同一个维度。判据读的是源码里的两段 match（`name()` 与
//! `dimension()`），不复制任何一条映射——复制的话，本门就成了第三个拼法。
//!
//! 判不了什么：
//!
//! - 前缀表为「枚举里没有的名字」给出的答案（trace 里的自定义指标名）。那一半
//!   没有枚举侧可对，本门一个字也说不出来。
//! - 维度划分本身合不合理。本门只保证改名字的时候两头一起动。
//! - 兜底值。名字掉进兜底是这条门要拦的**结果**，但它拦的是「两份答案不一致」；
//!   若两份都改成同一个兜底，本门绿——那是一个答案，不是一个分歧。

use std::collections::BTreeMap;

/// `cog-eval` 的 `metric.rs`：两份映射的唯一声明处。
fn metric_source() -> String {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/metric.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// 从 `signature` 起按大括号配对截出函数体（签名里那个 `{` 计作第一层）。
fn function_body(source: &str, signature: &str) -> String {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("metric.rs 里找不到 {signature}"));
    let mut depth = 0i32;
    let mut body = String::new();
    for ch in source[start..].chars() {
        body.push(ch);
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
    }
    body
}

/// 一段文本里出现的全部 `EvalMetric::X`，按出现顺序。
fn variants_in(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(pos) = text[from..].find("EvalMetric::") {
        let start = from + pos + "EvalMetric::".len();
        let end = text[start..]
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .map(|off| start + off)
            .unwrap_or(text.len());
        out.push(text[start..end].to_string());
        from = end;
    }
    out
}

/// `match` 里的第一个双引号字面量（本函数的 arm 返回的都是它）。
fn first_literal(text: &str) -> Option<String> {
    let open = text.find('"')?;
    let close = text[open + 1..].find('"')? + open + 1;
    Some(text[open + 1..close].to_string())
}

/// 把一段 `match` 体读成「变体 → 那个 arm 返回的字符串」。
///
/// 按 `=>` 切：每一段里，`=>` 左边是这组变体，右边第一个字面量是它们的答案。
/// arm 可以跨行（有几条把返回值写在下一行），所以不能按行读。
fn answers_by_variant(body: &str) -> BTreeMap<String, String> {
    let parts: Vec<&str> = body.split("=>").collect();
    let mut out = BTreeMap::new();
    for pair in parts.windows(2) {
        let (left, right) = (pair[0], pair[1]);
        let Some(answer) = first_literal(right) else {
            continue;
        };
        for variant in variants_in(left) {
            out.insert(variant, answer.clone());
        }
    }
    out
}

/// 每个变体：名字与维度的两处声明。
fn both_tables(source: &str) -> (BTreeMap<String, String>, BTreeMap<String, String>) {
    let names = answers_by_variant(&function_body(source, "pub fn name(&self) -> String {"));
    let dims = answers_by_variant(&function_body(
        source,
        "pub fn dimension(&self) -> &'static str {",
    ));
    (names, dims)
}

/// 声明了名字的变体数下限。测到的那一天是 120 出头；读到的比这少，说明解析
/// 已经跟 `metric.rs` 的写法错开了，那时的「全都对得上」什么也不证明。
const MIN_VARIANTS: usize = 110;

#[test]
fn every_metric_lands_in_the_dimension_its_variant_declares() {
    let source = metric_source();
    let (names, dims) = both_tables(&source);

    assert!(
        names.len() >= MIN_VARIANTS,
        "只从 name() 里读出 {} 个变体（下限 {MIN_VARIANTS}），这条判据没有题目",
        names.len()
    );
    assert!(
        dims.len() >= MIN_VARIANTS,
        "只从 dimension() 里读出 {} 个变体（下限 {MIN_VARIANTS}），这条判据没有题目",
        dims.len()
    );

    let mut findings = Vec::new();
    for (variant, name) in &names {
        let Some(declared) = dims.get(variant) else {
            findings.push(format!("  {variant}：name() 有它，dimension() 没有"));
            continue;
        };
        let computed = cog_eval::report::dimension_for_metric(name);
        if &computed != declared {
            findings.push(format!(
                "  {variant}（{name:?}）：dimension() 说 {declared}，dimension_for_metric() 说 {computed}"
            ));
        }
    }

    assert!(
        findings.is_empty(),
        "一个指标属于哪个维度有两份实现，它们给出了不同的答案。枚举那一侧有编译器\
         兜着，字符串那一侧没有——掉出来的指标会落进兜底维度，报表上只是某个维度\
         少一项，读起来和「这轮没测这一项」一样：\n{}",
        findings.join("\n")
    );
}

#[test]
fn the_reader_finds_a_grouped_arm_and_the_answer_on_the_next_line() {
    // arm 可以跨行，返回值也可以写在下一行：两种写法都要读出同一个变体。
    let body = concat!(
        "match self {\n",
        "    EvalMetric::A | EvalMetric::B => \"D1\",\n",
        "    EvalMetric::C { .. } => {\n",
        "        \"summary_distortion\".into()\n",
        "    }\n",
        "}\n",
    );
    let answers = answers_by_variant(body);
    assert_eq!(answers.get("A").map(String::as_str), Some("D1"));
    assert_eq!(answers.get("B").map(String::as_str), Some("D1"));
    assert_eq!(
        answers.get("C").map(String::as_str),
        Some("summary_distortion")
    );
    // 读得出来才说什么也没读到：一个变体都没解析到时，上面的下限会红。
    assert_eq!(variants_in("EvalMetric::NoAnswer(_) => x").len(), 1);
}
