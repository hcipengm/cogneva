//! principle: feedback_no_hardcoding_config_surface
//!
//! 运维页把变更行上的两种词各拼了一遍：那一格的色调与「批准」按钮看 `status`，按钮
//! 集合看不看某个按钮看 `kind`；晋级台账那一格看晋级状态。服务端发的是
//! `EvolutionStatus` / `PromotionStatus` 的 serde 词，和 `kind_name` 那个手写 match
//! 里的词，页面这边按字面量逐个比对，中间没有东西对照。
//!
//! 这类漂移是静的：写错的词永远不相等，于是那一行没有色调、那个按钮不出现——「没有
//! 色调」和「状态不需要色调」长得一样，「按钮不在」和「没什么可批的」长得一样。实测
//! （2026-10-27）就有一处：`statusPillClass` 与 `actionButtons` 都把待审状态写成
//! `awaitingreview`（没有下划线），而线上发的是 `awaiting_review`，于是运维页里
//! 唯一那个「批准」按钮从来没渲染过；同一函数里 `validation_failed` / `compile_error`
//! 两种真会出现的失败态没有一条分支，而不会出现的 `applied` / `deployed` / `failed`
//! / `rolledback` 各占了一行。
//!
//! 判两件事，都是拿词表的正本当唯一依据：
//!
//! - 页面对比的每一个词，必须是那张词表里的一个词。**说错的词是缺陷，不只是白写
//!   一行**：它让「这里本该有分支」读起来像「这里不需要分支」。
//! - 每张词表都得读得出来，每个函数都得找得到。找不到就红——读不到与全绿长得一样，
//!   这条判据不能有那种失败方式。
//!
//! 词表有两种写法，各自照自己的正本读：serde 枚举读变体名（`EvolutionStatus` /
//! `PromotionStatus` 都带 `rename_all = "snake_case"`），`kind_name` 是手写 match，
//! 读它每条臂 return 的那个词。两者都不接受「读出来是空的」。
//!
//! 实测（2026-10-27）：`EvolutionStatus` 9 个词、`PromotionStatus` 5 个词、`kind_name`
//! 5 个词；页面四个函数合计比对 11 处（`isAwaitingReview` 1、`statusPillClass` 5、
//! `promoStatusPill` 4、`actionButtons` 1）。消融三条，各自红、改回即绿：把
//! `isAwaitingReview` 的词改回 `awaitingreview`、把 `statusPillClass` 的 `compile_error`
//! 改回 `failed`（这两条一起做，红并分别点名两个函数）、把 `actionButtons` 的
//! `policy_update` 改成 `policy_changed`（红并点名它，词表一起印出来）。
//!
//! 判不到的地方，写在这里：运维页是变更列表唯一的读者（`web/src` 只声明了接口、没有
//! 调用点），所以这条判据只覆盖它一个客户端。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

mod common;

use common::rust_text::snake_case;

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
/// The kind words, written by hand instead of by serde.
const KINDS: &str = "crates/cog-reflection/src/evolution_admin.rs";
const KINDS_FUNCTION: &str = "kind_name";

/// Where a vocabulary's words are written down.
#[derive(Clone, Copy)]
enum Spoken {
    /// A serde enum: its variants, through `rename_all = "snake_case"`.
    Variants(&'static str, &'static str),
    /// A hand-written match: the word each arm returns.
    Arms(&'static str, &'static str),
}

impl Spoken {
    fn describe(self) -> String {
        match self {
            Spoken::Variants(_, name) => name.to_string(),
            Spoken::Arms(_, function) => format!("{function} 的臂"),
        }
    }

    /// How many words the vocabulary has to hold before a comparison against it
    /// means anything.
    fn floor(self) -> usize {
        match self {
            Spoken::Variants(..) => 5,
            Spoken::Arms(..) => 4,
        }
    }
}

/// The page functions whose bodies are nothing but a comparison of a row word.
/// Naming functions rather than reading the whole page is what keeps the
/// judgement exact: everywhere else the page compares paths, tones and message
/// keys, and those vocabularies belong to other readers.
const SUBJECTS: &[(&str, Spoken)] = &[
    (
        "isAwaitingReview",
        Spoken::Variants(EVOLUTION, EVOLUTION_ENUM),
    ),
    (
        "statusPillClass",
        Spoken::Variants(EVOLUTION, EVOLUTION_ENUM),
    ),
    (
        "promoStatusPill",
        Spoken::Variants(PROMOTION, PROMOTION_ENUM),
    ),
    ("actionButtons", Spoken::Arms(KINDS, KINDS_FUNCTION)),
];

/// `CompileChecked` → `compile_checked`, the shape `rename_all = "snake_case"`
/// gives a variant on the wire.
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

/// The body of `fn <name>(…)` in a Rust source.
fn rust_function_body(source: &str, name: &str) -> Option<String> {
    let at = source.find(&format!("fn {name}("))?;
    let brace = at + source[at..].find('{')?;
    let end = matching_brace(source.as_bytes(), brace)?;
    Some(source[brace + 1..end].to_string())
}

/// The quoted word that starts at `from`, skipping whitespace, and the index
/// just past its closing quote. Only words made of identifier characters count:
/// a quoted path or sentence after an operator is not a vocabulary word.
fn quoted_from(chars: &[char], mut from: usize) -> Option<(String, usize)> {
    while from < chars.len() && chars[from].is_whitespace() {
        from += 1;
    }
    let quote = *chars.get(from)?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let start = from + 1;
    let mut end = start;
    while end < chars.len() && chars[end] != quote {
        end += 1;
    }
    let word: String = chars[start..end].iter().collect();
    if word.is_empty() || !word.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    Some((word, end))
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
        if chars.get(j) != Some(&'=') {
            i += 1;
            continue;
        }
        j += 1;
        if chars.get(j) == Some(&'=') {
            j += 1;
        }
        match quoted_from(&chars, j) {
            Some((word, end)) => {
                words.push(word);
                i = end.max(i + 1);
            }
            None => i = j,
        }
    }
    words
}

/// The words a hand-written match returns, one per arm (`=> "word"`).
fn arm_words(body: &str) -> Option<BTreeSet<String>> {
    let chars: Vec<char> = body.chars().collect();
    let mut words = BTreeSet::new();
    let mut i = 0;
    while i + 1 < chars.len() {
        if chars[i] == '=' && chars[i + 1] == '>' {
            if let Some((word, end)) = quoted_from(&chars, i + 2) {
                words.insert(word);
                i = end.max(i + 2);
                continue;
            }
        }
        i += 1;
    }
    if words.is_empty() {
        None
    } else {
        Some(words)
    }
}

/// The words a subject's vocabulary holds, read from its own source.
fn words_of(root: &Path, spoken: Spoken) -> Option<BTreeSet<String>> {
    match spoken {
        Spoken::Variants(file, name) => enum_words(&production_text(&read(&root.join(file))), name),
        Spoken::Arms(file, function) => {
            let source = production_text(&read(&root.join(file)));
            arm_words(&rust_function_body(&source, function)?)
        }
    }
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

fn contradictions(root: &Path) -> Vec<String> {
    let compared = compared(root);
    let mut complaints = Vec::new();
    for (function, spoken) in SUBJECTS.iter().copied() {
        let Some(seen) = compared.get(function) else {
            complaints.push(format!(
                "页面里找不到 `function {function}(`：这条判据读不到它，就读不到它比对过的词。\
                 函数改名了就同步这条登记，别让它静默地不再判任何东西"
            ));
            continue;
        };
        let Some(vocabulary) = words_of(root, spoken) else {
            complaints.push(format!(
                "读不出 `{}` 的词表：要么声明不在了，要么它的写法变了。读不出词表时这条判据\
                 判不了任何东西，所以按红处理",
                spoken.describe()
            ));
            continue;
        };
        for word in seen {
            if vocabulary.contains(word) {
                continue;
            }
            complaints.push(format!(
                "  `{function}` 拿行上的词和 `{word}` 比，而 `{}` 从不发这个词\
                 （它只有 {vocabulary:?}）：这一支永远不成立，那一格没有色调、那个按钮不出现——\
                 与「这个状态不需要处理」同一种读数",
                spoken.describe()
            ));
        }
    }
    complaints
}

#[test]
fn every_row_word_the_page_compares_is_a_word_its_vocabulary_writes() {
    let complaints = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "运维页按状态词与种类词分支，而它拼的词和正本对不上。对不上的那一支不会报错，\
         只是永远不成立：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    for (_, spoken) in SUBJECTS.iter().copied() {
        let set = words_of(&root, spoken)
            .unwrap_or_else(|| panic!("`{}` 的词表读不出来，这条判据在空转", spoken.describe()));
        assert!(
            set.len() >= spoken.floor(),
            "`{}` 只读出 {} 个词，太少：{set:?}",
            spoken.describe(),
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
            "`{function}` 里一处词比对都没读到：函数还在，body 的形状变了，\
             这条判据对它已经空转"
        );
    }
    let total: usize = compared.values().map(Vec::len).sum();
    assert!(
        total >= 11,
        "四个函数合计只读到 {total} 处词比对，读取器看错地方了：{compared:?}"
    );
}

/// 页面上写着四个函数的树，词表正本也是最小的一份：两份都成立，除非用例把某一处
/// 词改掉。
const FIXTURE_PAGE: &str = "\
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
function actionButtons(p) {
  const isPolicy = p.kind === 'policy_update';
  return isPolicy;
}
";

const FIXTURE_EVOLUTION: &str = "\
#[derive(Serialize)]
#[serde(rename_all = \"snake_case\")]
pub enum EvolutionStatus {
    CompileChecked,
    ValidationFailed,
    CompileError,
    AwaitingReview,
    Active,
    Rejected,
}
";

const FIXTURE_PROMOTION: &str = "\
#[derive(Serialize)]
#[serde(rename_all = \"snake_case\")]
pub enum PromotionStatus {
    Pending,
    Promoted,
    RolledBack,
    Failed,
    AwaitingApproval,
}
";

const FIXTURE_KINDS: &str = "\
fn kind_name(kind: &EvolutionKind) -> &'static str {
    match kind {
        EvolutionKind::SkillRefinement => \"skill_refinement\",
        EvolutionKind::HookSynthesis => \"hook_synthesis\",
        EvolutionKind::ToolVariant => \"tool_variant\",
        EvolutionKind::CodeChange => \"code_change\",
        EvolutionKind::PolicyUpdate => \"policy_update\",
    }
}
";

fn fixture(page: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    for (relative, body) in [
        (PAGE, page),
        (EVOLUTION, FIXTURE_EVOLUTION),
        (PROMOTION, FIXTURE_PROMOTION),
        (KINDS, FIXTURE_KINDS),
    ] {
        let path = dir.path().join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
        std::fs::write(path, body).expect("write");
    }
    dir
}

#[test]
fn the_fixture_is_green_before_any_word_is_changed() {
    let dir = fixture(FIXTURE_PAGE);
    let complaints = contradictions(dir.path());
    assert!(complaints.is_empty(), "{complaints:?}");
}

#[test]
fn a_word_no_vocabulary_writes_is_reported() {
    let status = FIXTURE_PAGE.replace("'compile_error'", "'failed'");
    assert_ne!(status, FIXTURE_PAGE);
    let dir = fixture(&status);
    let complaints = contradictions(dir.path());
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("statusPillClass"), "{complaints:?}");
    assert!(complaints[0].contains("failed"), "{complaints:?}");

    let awaiting = FIXTURE_PAGE.replace("'awaiting_review'", "'awaitingreview'");
    assert_ne!(awaiting, FIXTURE_PAGE);
    let dir = fixture(&awaiting);
    let complaints = contradictions(dir.path());
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("isAwaitingReview"), "{complaints:?}");

    let kind = FIXTURE_PAGE.replace("'policy_update'", "'policy_changed'");
    assert_ne!(kind, FIXTURE_PAGE);
    let dir = fixture(&kind);
    let complaints = contradictions(dir.path());
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("actionButtons"), "{complaints:?}");
    assert!(complaints[0].contains("policy_changed"), "{complaints:?}");
}

#[test]
fn a_page_function_that_cannot_be_found_is_reported() {
    let dir = fixture("function unrelated() { return 'x' === 'y'; }\n");
    let complaints = contradictions(dir.path());
    for (function, _) in SUBJECTS.iter().copied() {
        assert!(
            complaints.iter().any(|c| c.contains(function)),
            "找不到 `{function}` 时本门没有点名它：{complaints:?}"
        );
    }
}

#[test]
fn a_vocabulary_that_cannot_be_read_is_reported() {
    let dir = fixture(FIXTURE_PAGE);
    // Dropping the serde attribute leaves the enum readable as a list of
    // variants but without the rule that turns them into wire words; the kind
    // match loses the arm the page compares against.
    std::fs::write(
        dir.path().join(EVOLUTION),
        FIXTURE_EVOLUTION.replace("#[serde(rename_all = \"snake_case\")]\n", ""),
    )
    .expect("evolution");
    std::fs::write(
        dir.path().join(KINDS),
        "fn kind_name(kind: &EvolutionKind) -> &'static str {\n    todo!()\n}\n",
    )
    .expect("kinds");
    let complaints = contradictions(dir.path());
    assert!(
        complaints.iter().any(|c| c.contains(EVOLUTION_ENUM)),
        "{complaints:?}"
    );
    assert!(
        complaints.iter().any(|c| c.contains(KINDS_FUNCTION)),
        "{complaints:?}"
    );
}
