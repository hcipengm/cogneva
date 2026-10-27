//! principle: feedback_no_hardcoding_config_surface
//!
//! 运维页把两种「状态词」各拼了一遍：变更行上那一格按 `EvolutionStatus` 的取值染色
//! 并决定要不要给「批准」按钮，晋级台账那一格按 `PromotionStatus` 的取值染色。服务端
//! 的取值由 `#[serde(rename_all = "snake_case")]` 决定，页面这边是按字面量逐个比对，
//! 中间没有东西对照。
//!
//! 这类漂移是静的：写错的词永远不相等，于是那一行没有色调、那个按钮不出现——「没有
//! 色调」和「状态不需要色调」长得一样，「按钮不在」和「没什么可批的」长得一样。实测
//! （2026-10-27）就有一处：`statusPillClass` 与 `actionButtons` 都把待审状态写成
//! `awaitingreview`（没有下划线），而线上发的是 `awaiting_review`，于是运维页里
//! 唯一那个「批准」按钮从来没渲染过；同一函数里 `validation_failed` / `compile_error`
//! 两种真会出现的失败态没有一条分支，而不会出现的 `applied` / `deployed` / `failed`
//! / `rolledback` 各占了一行。
//!
//! 判两件事，都是拿枚举自己的词当唯一正本：
//!
//! - 页面对比的每一个词，必须是那张词表里的一个词。**说错的词是缺陷，不只是白写
//!   一行**：它让「这里本该有分支」读起来像「这里不需要分支」。
//! - 每个词表都得读得出来，每个函数都得找得到。找不到就红——读不到与全绿长得一样，
//!   这条判据不能有那种失败方式。
//!
//! 实测（2026-10-27）：`EvolutionStatus` 9 个词、`PromotionStatus` 5 个词，页面三个
//! 函数合计比对 10 处（`isAwaitingReview` 1、`statusPillClass` 5、`promoStatusPill` 4）。
//! 消融两条一次做：把 `isAwaitingReview` 的词改回 `awaitingreview`、把 `statusPillClass`
//! 的 `compile_error` 改回 `failed`，本门红并分别点名这两个函数（连词表一起印出来）；
//! 改回即绿。
//!
//! 判不到的地方，写在这里：`actionButtons` 里还有一处 `p.kind === 'policy_update'`，
//! 那是 `kind_name` 那张词表（`EvolutionKind` 不走 serde 的 `rename_all`，另有一个
//! 手写的 match），不是状态词表，所以不在本条里；运维页是变更列表唯一的读者
//! （`web/src` 只声明了接口、没有调用点），所以这条判据只覆盖它一个客户端。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

mod common;

use common::routes::{matching_brace, read, repo_root, strip_comments};
use common::rust_production::production_text;

/// The operator page the gateway binary embeds.
const PAGE: &str = "crates/cog-gateway/src/takeover.html";
/// The change row's status vocabulary.
const EVOLUTION: &str = "crates/cog-reflection/src/types.rs";
const EVOLUTION_ENUM: &str = "EvolutionStatus";
/// The promotion ledger's status vocabulary.
const PROMOTION: &str = "crates/cog-core/src/contract/promotion.rs";
const PROMOTION_ENUM: &str = "PromotionStatus";

/// The page functions whose bodies are nothing but a comparison of a status
/// word. Naming functions rather than reading the whole page is what keeps the
/// judgement exact: everywhere else the page compares paths, kinds and keys,
/// and those vocabularies belong to other readers.
const SUBJECTS: &[(&str, &str)] = &[
    ("isAwaitingReview", EVOLUTION_ENUM),
    ("statusPillClass", EVOLUTION_ENUM),
    ("promoStatusPill", PROMOTION_ENUM),
];

/// `CompileChecked` → `compile_checked`, the shape `rename_all = "snake_case"`
/// gives a variant on the wire.
fn snake_case(name: &str) -> String {
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

/// The words an enum serialises, or `None` when the declaration is not one this
/// reader can trust: no `snake_case` rename, a variant it cannot parse, or no
/// enum at all. `None` is a red result, never an empty word list — a vocabulary
/// read as empty would make every word in the page a defect, and a vocabulary
/// read as "whatever is there" would make none of them one.
fn enum_words(source: &str, name: &str) -> Option<BTreeSet<String>> {
    let at = source.find(&format!("enum {name}"))?;
    // The serde attribute sits above the declaration; a byte offset taken 240
    // bytes back can land inside a multi-byte character, so it is walked to the
    // nearest boundary first.
    let mut from = at.saturating_sub(240);
    while from > 0 && !source.is_char_boundary(from) {
        from -= 1;
    }
    if !source[from..at].contains("rename_all = \"snake_case\"") {
        return None;
    }
    let brace = at + source[at..].find('{')?;
    let end = matching_brace(source.as_bytes(), brace)?;
    let mut words = BTreeSet::new();
    for part in source[brace + 1..end].split(',') {
        let mut part = part.trim();
        if part.is_empty() {
            continue;
        }
        // A variant may carry its own attribute; the name follows the last one.
        if let Some(close) = part.rfind(']') {
            part = part[close + 1..].trim();
        }
        let variant: String = part
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !variant.chars().next().is_some_and(|c| c.is_uppercase()) {
            return None;
        }
        words.insert(snake_case(&variant));
    }
    if words.is_empty() {
        return None;
    }
    Some(words)
}

/// The body of `function <name>(…)` in the page.
fn function_body(page: &str, name: &str) -> Option<String> {
    let at = page.find(&format!("function {name}("))?;
    let brace = at + page[at..].find('{')?;
    let end = matching_brace(page.as_bytes(), brace)?;
    Some(page[brace + 1..end].to_string())
}

/// The quoted words a body compares against: the right-hand side of `==`,
/// `===`, `!=` and `!==`. Everything else the body quotes — class names, tone
/// names, message keys — is not a vocabulary word and is not collected.
fn compared_words(body: &str) -> Vec<String> {
    let chars: Vec<char> = body.chars().collect();
    let mut words = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '=' && chars[i] != '!' {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        if j >= chars.len() || chars[j] != '=' {
            i += 1;
            continue;
        }
        j += 1;
        if chars.get(j) == Some(&'=') {
            j += 1;
        }
        while j < chars.len() && chars[j].is_whitespace() {
            j += 1;
        }
        let quote = match chars.get(j) {
            Some(&q) if q == '\'' || q == '"' => q,
            _ => {
                i = j;
                continue;
            }
        };
        let start = j + 1;
        let mut end = start;
        while end < chars.len() && chars[end] != quote {
            end += 1;
        }
        let word: String = chars[start..end].iter().collect();
        if !word.is_empty() && word.chars().all(|c| c.is_alphanumeric() || c == '_') {
            words.push(word);
        }
        i = end.max(i + 1);
    }
    words
}

/// What each subject compared, keyed by function name.
fn compared(root: &Path) -> BTreeMap<String, Vec<String>> {
    let page = strip_comments(&read(&root.join(PAGE)));
    let mut out = BTreeMap::new();
    for (function, _) in SUBJECTS.iter().copied() {
        if let Some(body) = function_body(&page, function) {
            out.insert(function.to_string(), compared_words(&body));
        }
    }
    out
}

fn vocabulary(root: &Path) -> BTreeMap<&'static str, Option<BTreeSet<String>>> {
    let evolution = enum_words(
        &production_text(&read(&root.join(EVOLUTION))),
        EVOLUTION_ENUM,
    );
    let promotion = enum_words(
        &production_text(&read(&root.join(PROMOTION))),
        PROMOTION_ENUM,
    );
    BTreeMap::from([(EVOLUTION_ENUM, evolution), (PROMOTION_ENUM, promotion)])
}

fn contradictions(root: &Path) -> Vec<String> {
    let words = vocabulary(root);
    let compared = compared(root);
    let mut complaints = Vec::new();
    for (function, enum_name) in SUBJECTS.iter().copied() {
        let Some(seen) = compared.get(function) else {
            complaints.push(format!(
                "页面里找不到 `function {function}(`：这条判据读不到它，就读不到它比对过的词。\
                 函数改名了就同步这条登记，别让它静默地不再判任何东西"
            ));
            continue;
        };
        let Some(Some(vocabulary)) = words.get(enum_name) else {
            complaints.push(format!(
                "读不出 `{enum_name}` 的词表：要么枚举不在了，要么它不再用 \
                 `rename_all = \"snake_case\"`，要么有读不懂的变体。读不出词表时这条判据\
                 判不了任何东西，所以按红处理"
            ));
            continue;
        };
        for word in seen {
            if vocabulary.contains(word) {
                continue;
            }
            complaints.push(format!(
                "  `{function}` 拿状态和 `{word}` 比，而 `{enum_name}` 从不发这个词\
                 （它只有 {vocabulary:?}）：这一支永远不成立，那一格没有色调、那个按钮不出现——\
                 与「这个状态不需要处理」同一种读数"
            ));
        }
    }
    complaints
}

#[test]
fn every_status_word_the_page_compares_is_a_word_the_status_writes() {
    let complaints = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "运维页按状态词分支，而它拼的词和枚举发的词对不上。对不上的那一支不会报错，\
         只是永远不成立：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let words = vocabulary(&root);
    for (name, set) in &words {
        let set = set
            .as_ref()
            .unwrap_or_else(|| panic!("`{name}` 的词表读不出来，这条判据在空转"));
        assert!(
            set.len() >= 5,
            "`{name}` 只读出 {} 个词，太少：{set:?}",
            set.len()
        );
    }
    let compared = compared(&root);
    assert_eq!(
        compared.len(),
        SUBJECTS.len(),
        "只找到 {} 个被登记的函数（登记了 {} 个）：{:?}",
        compared.len(),
        SUBJECTS.len(),
        compared.keys().collect::<Vec<_>>()
    );
    for (function, words) in &compared {
        assert!(
            !words.is_empty(),
            "`{function}` 里一处状态词比对都没读到：函数还在，body 的形状变了，\
             这条判据对它已经空转"
        );
    }
    let total: usize = compared.values().map(Vec::len).sum();
    assert!(
        total >= 8,
        "三个函数合计只读到 {total} 处状态词比对，读取器看错地方了：{compared:?}"
    );
}

#[test]
fn a_word_no_status_writes_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    for relative in [PAGE, EVOLUTION, PROMOTION] {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
    }
    std::fs::write(
        root.join(PAGE),
        "\
function isAwaitingReview(s) {
  return (s || '').toLowerCase() === 'awaiting_review';
}
function statusPillClass(s) {
  const v = (s || '').toLowerCase();
  if (isAwaitingReview(v)) return 'warn';
  if (v === 'active') return 'ok';
  if (v === 'failed') return 'err';
  return '';
}
function promoStatusPill(s) {
  const v = (s || '').toLowerCase();
  if (v === 'promoted') return 'ok';
  if (v === 'rolled_back') return 'err';
  return '';
}
",
    )
    .expect("page");
    std::fs::write(
        root.join(EVOLUTION),
        "\
#[derive(Serialize)]
#[serde(rename_all = \"snake_case\")]
pub enum EvolutionStatus {
    CompileChecked,
    AwaitingReview,
    Active,
    Rejected,
    CompileError,
}
",
    )
    .expect("evolution");
    std::fs::write(
        root.join(PROMOTION),
        "\
#[derive(Serialize)]
#[serde(rename_all = \"snake_case\")]
pub enum PromotionStatus {
    Pending,
    Promoted,
    RolledBack,
    Failed,
    AwaitingApproval,
}
",
    )
    .expect("promotion");

    let complaints = contradictions(root);
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("statusPillClass"), "{complaints:?}");
    assert!(complaints[0].contains("failed"), "{complaints:?}");

    std::fs::write(
        root.join(PAGE),
        "\
function isAwaitingReview(s) {
  return (s || '').toLowerCase() === 'awaiting_review';
}
function statusPillClass(s) {
  const v = (s || '').toLowerCase();
  if (isAwaitingReview(v)) return 'warn';
  if (v === 'active') return 'ok';
  if (v === 'compile_error') return 'err';
  return '';
}
function promoStatusPill(s) {
  const v = (s || '').toLowerCase();
  if (v === 'promoted') return 'ok';
  if (v === 'rolled_back') return 'err';
  return '';
}
",
    )
    .expect("page");
    assert!(
        contradictions(root).is_empty(),
        "{:?}",
        contradictions(root)
    );
}

#[test]
fn a_page_function_that_cannot_be_found_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    for relative in [PAGE, EVOLUTION, PROMOTION] {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
    }
    for (relative, body) in [
        (EVOLUTION, "pub enum EvolutionStatus {\n    Active,\n}\n"),
        (PROMOTION, "pub enum PromotionStatus {\n    Pending,\n}\n"),
        (PAGE, "function unrelated() { return 'x' === 'y'; }\n"),
    ] {
        std::fs::write(root.join(relative), body).expect("write");
    }
    let complaints = contradictions(root);
    for (function, _) in SUBJECTS.iter().copied() {
        assert!(
            complaints.iter().any(|c| c.contains(function)),
            "找不到 `{function}` 时本门没有点名它：{complaints:?}"
        );
    }
}
