//! principle: feedback_a_census_gate_is_proven_with_a_probe_file_outside_the_module_tree
//! principle: feedback_a_handwritten_census_list_must_be_checked_against_the_whole_tree
//!
//! The actor label on the LLM series is a closed set, and this is its census.
//!
//! `x-cogneva-actor` is what the cost and quota views group by, so a value that
//! reaches the label without being declared is a series nobody decided to carry
//! — and on the reading side it looks exactly like a feature someone meant to
//! add. The gateway now admits only declared values
//! (`cog_gateway::actor_labels::declared`), which closes the vocabulary at the
//! edge; this test closes it at the source, over the call sites that write the
//! header.
//!
//! What it judges: every actor literal the tree writes is declared, every
//! declared value is one the tree actually writes (so a declaration cannot
//! outlive its caller), and the set of files that write the header is exactly
//! the inventory below — a new call site anywhere has to be listed, with the
//! actor it reports under, before it can land.
//!
//! What it cannot judge, in the open: a call site that forgets the actor
//! entirely writes no literal, so no text census can see it. That case is
//! readable only at run time, as traffic in the `actor="unknown"` cell, and the
//! reader for that cell is not this test. This is the same division the closed
//! sets elsewhere in the platform use: the census bounds the vocabulary, the
//! counter shows the omissions.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::common;

use common::routes::repo_root;

use common::rust_production::{all_crate_files, production_sources, production_text};

use cog_gateway::actor_labels::{declared, ACTOR_LABELS, ACTOR_LABEL_PREFIXES, UNKNOWN_ACTOR};

/// Files that write the actor header, and the actor each reports under. Kept as
/// an inventory rather than derived, so that adding a call site is a decision
/// someone makes here instead of something that just happens.
const PRODUCERS: &[(&str, &[&str])] = &[
    ("crates/cog-agent/src/agent.rs", &["self_review:{role}"]),
    (
        "crates/cog-agent/src/agent_kernel/runtime.rs",
        &["agent:{role}"],
    ),
    (
        "crates/cog-agent/src/self_review.rs",
        &["self_review", "self_review:{role}"],
    ),
    // The attribution test drives the loop with both shapes and reads the
    // header the loop put on the request, so it names an actor the same way
    // the loop does.
    (
        "crates/cog-agent/tests/self_review_task_attribution.rs",
        &["self_review", "self_review:{role}"],
    ),
    (
        "crates/cog-collaboration/src/squad/ralph/ralph_loop.rs",
        &["ralph"],
    ),
    ("crates/cog-eval/src/adapters/hle.rs", &["eval"]),
    ("crates/cog-eval/src/judge.rs", &["eval"]),
    // The scaffolds are the harness's own loops (plan / step / GEPA judge). They
    // went without an actor until 2026-11-03, which is where the 28.1M tokens
    // the ledger holds under `unknown` on 10-18/10-19 came from.
    ("crates/cog-eval/src/scaffolds/mod.rs", &["eval"]),
    (
        "crates/cog-extension/src/hostdocs_assist.rs",
        &["sandbox-document-organizer"],
    ),
    // 记忆抽取不再自己叫模型：它把「这段材料里什么值得记」当成一件活交给
    // 名册上的员工，账记在接活的那个人身上（`agent:<id>`），所以这里没有
    // 属于场景自己的名字可写。
    ("crates/cog-reflection/src/evolution.rs", &["evolution"]),
    ("crates/cogneva/src/eval_table.rs", &["eval"]),
    (
        "crates/cogneva/tests/collaboration_self_review_test.rs",
        &["self_review:{role}"],
    ),
];

/// Files whose actor is not a value written at the call site: the value is
/// threaded in from the caller that built the loop.
const THREADED: &[(&str, &str)] = &[
    (
        "crates/cog-agent/src/agent.rs",
        "reports under the self-review role the agent itself is configured with",
    ),
    (
        "crates/cog-agent/src/agent_kernel/runtime.rs",
        "the loop writes whatever face its builder declared; the role family is \
         only the default — the Web UI conversation gives it a face of its own",
    ),
    (
        "crates/cog-agent/src/self_review.rs",
        "each step takes the actor the loop was built with",
    ),
];

/// Files the census does not read, and why.
const EXCLUDED: &[(&str, &str)] = &[
    (
        "crates/cog-core/src/contract/llm.rs",
        "it defines the header and the setter; the value it writes is the caller's",
    ),
    (
        "crates/cogneva/tests/llm_actor_label_contract_test.rs",
        "this census, whose samples are not call sites",
    ),
];

/// Declared values that no call site writes directly, and what writes them.
///
/// `self_review` was the last entry before 2026-11-02, and it is written at a
/// call site now that the review loop stamps its own actor header. The table
/// stays because the next value a caller stops naming needs a place to say so
/// with a reason.
const NOT_WRITTEN_AT_A_CALL_SITE: &[(&str, &str)] = &[];

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Site {
    /// A literal written at the call site.
    Literal(String),
    /// A role family, written as a format string whose first literal is the
    /// declared prefix.
    Family(String),
    /// An expression the census cannot evaluate; its file has to be in
    /// `THREADED`.
    Unresolved(String),
}

/// Every site in one file's text that writes an actor value.
fn sites_in(text: &str) -> Vec<Site> {
    let consts = const_strings(text);
    let mut sites = Vec::new();
    // 两个入口都读：循环自己挂的那条（`.with_actor(...)` 在请求选项上），以及
    // 装配方给整条循环换一张脸的那条（`.with_actor_label(...)`）。只读前者会把
    // 「给某条产出线换了个脸名」这件事整个漏出名单，而那正是这张名单要管的。
    let mut rest = text;
    while let Some((at, span)) = earliest_actor_call(rest) {
        rest = &rest[at + span..];
        let Some(arg) = argument(rest) else { continue };
        sites.push(classify(arg, &consts));
    }
    // The header written directly rather than through `with_actor`.
    for line in text.lines() {
        if !line.contains("LLM_ACTOR_HEADER")
            || !(line.contains(".header(") || line.contains(".insert("))
        {
            continue;
        }
        let after = &line[line.rfind("LLM_ACTOR_HEADER").unwrap() + "LLM_ACTOR_HEADER".len()..];
        if let Some(arg) = after.split_once(',').map(|(_, arg)| arg.trim()) {
            let arg = arg.trim_end_matches(')').trim();
            sites.push(classify(arg, &consts));
        }
    }
    sites.sort();
    sites
}

/// Where the next actor-writing call begins, and how long its name is.
///
/// Both spellings count: `.with_actor` puts a face on one request, and
/// `.with_actor_label` puts one on a whole loop. Reading only the first would
/// let a producer declare its face somewhere the census never looks.
fn earliest_actor_call(text: &str) -> Option<(usize, usize)> {
    [".with_actor(", ".with_actor_label("]
        .iter()
        .filter_map(|needle| text.find(needle).map(|at| (at, needle.len())))
        .min_by_key(|(at, _)| *at)
}

/// The text of the first argument of a call whose `(` has just been consumed,
/// up to its matching `)`.
fn argument(rest: &str) -> Option<&str> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (i, b) in rest.bytes().enumerate() {
        if in_string {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'(' => depth += 1,
            b')' if depth == 0 => return Some(rest[..i].trim()),
            b')' => depth -= 1,
            _ => {}
        }
    }
    None
}

/// `const NAME: &str = "value";` in one file, so that a site naming a constant
/// is judged on the value the constant holds.
fn const_strings(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        let Some(rest) = line
            .strip_prefix("const ")
            .or_else(|| line.strip_prefix("pub const "))
        else {
            continue;
        };
        let Some((name, tail)) = rest.split_once(':') else {
            continue;
        };
        let Some(value) = tail.split('"').nth(1) else {
            continue;
        };
        out.insert(name.trim().to_string(), value.to_string());
    }
    out
}

fn classify(arg: &str, consts: &BTreeMap<String, String>) -> Site {
    if let Some(lit) = arg.strip_prefix('"').and_then(|a| a.split('"').next()) {
        return Site::Literal(lit.to_string());
    }
    // A role family, written as `format!("agent:{}", role)` or concatenated.
    if arg.contains("format!(") || arg.contains(" + ") {
        if let Some(prefix) = arg.split('"').nth(1).and_then(|l| l.split('{').next()) {
            return Site::Family(prefix.to_string());
        }
    }
    // The same family, built by the helper the roles share.
    if arg.contains("self_review_actor(") {
        return Site::Family("self_review:".to_string());
    }
    let bare = arg.rsplit("::").next().unwrap_or(arg).trim();
    if let Some(value) = consts.get(bare.trim_end_matches(".to_string()")) {
        return Site::Literal(value.clone());
    }
    Site::Unresolved(arg.to_string())
}

/// The census, over the working tree. Returns the sites by file.
fn census(root: &Path) -> BTreeMap<String, Vec<Site>> {
    let files = all_crate_files(root);
    let mut found = BTreeMap::new();
    for file in files {
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .display()
            .to_string();
        if EXCLUDED.iter().any(|(path, _)| *path == rel) {
            continue;
        }
        // 读不出来就 panic：一个读不到的文件与「这个文件里没有写 actor 的调用点」
        // 在这张名单上是同一件事，而这张名单的结论正是「谁写了 actor」。
        let text = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", file.display()));
        // 产出面（用例体与 `#[cfg(test)]` 项都去掉）由 `common::rust_production` 一处算：
        // 自己切「第一处 `#[cfg(test)]` 之前」会漏掉文件中间那个 test-only 项之后的全部产出，
        // 而「那里没有写 actor 的调用点」与「那块文件没读」在这张名单上是同一件事。
        let sites = sites_in(&production_text(&text));
        if !sites.is_empty() {
            found.insert(rel, sites);
        }
    }
    found
}

#[test]
fn the_header_is_written_by_exactly_the_files_that_declare_an_actor() {
    let root = repo_root();
    let found = census(&root);
    let expected: BTreeSet<&str> = PRODUCERS.iter().map(|(path, _)| *path).collect();
    let actual: BTreeSet<&str> = found.keys().map(String::as_str).collect();
    let added: Vec<_> = actual.difference(&expected).collect();
    let gone: Vec<_> = expected.difference(&actual).collect();
    assert!(
        added.is_empty(),
        "these files write the actor header but are not in the inventory: {added:?}\n\
         Add them with the actor they report under, or give the call site an actor."
    );
    assert!(
        gone.is_empty(),
        "these files are in the inventory but no longer write the actor header: {gone:?}\n\
         Drop them, so the inventory keeps naming the real writers."
    );
}

#[test]
fn every_actor_the_tree_writes_is_declared() {
    let found = census(&repo_root());
    let mut complaints = Vec::new();
    for (file, sites) in &found {
        for site in sites {
            match site {
                Site::Literal(value) if !declared(value) => {
                    complaints.push(format!("{file} writes the undeclared actor {value:?}"));
                }
                Site::Family(prefix) if !ACTOR_LABEL_PREFIXES.contains(&prefix.as_str()) => {
                    complaints.push(format!(
                        "{file} builds an actor from the undeclared family {prefix:?}"
                    ));
                }
                _ => {}
            }
        }
    }
    assert!(complaints.is_empty(), "{}", complaints.join("\n"));
}

#[test]
fn every_declared_actor_is_one_the_tree_writes() {
    let found = census(&repo_root());
    let mut written: BTreeSet<String> = BTreeSet::new();
    for sites in found.values() {
        for site in sites {
            match site {
                Site::Literal(value) => {
                    written.insert(value.clone());
                }
                Site::Family(prefix) => {
                    // A family is written by whoever formats it; the sample
                    // stands in for the roles configuration supplies.
                    written.insert(format!("{prefix}planner"));
                }
                Site::Unresolved(_) => {}
            }
        }
    }
    let exempt: BTreeSet<&str> = NOT_WRITTEN_AT_A_CALL_SITE
        .iter()
        .map(|(value, _)| *value)
        .collect();
    let orphans: Vec<&str> = ACTOR_LABELS
        .iter()
        .copied()
        .filter(|value| !written.contains(*value) && !exempt.contains(value))
        .collect();
    assert!(
        orphans.is_empty(),
        "these actors are declared but nothing writes them: {orphans:?}\n\
         A declaration that outlives its caller is a value the gateway will keep \
         admitting while no traffic can carry it."
    );
    for (value, reason) in NOT_WRITTEN_AT_A_CALL_SITE {
        assert!(
            ACTOR_LABELS.contains(value),
            "{value} is exempted from being written but is not declared ({reason})"
        );
        assert!(
            !written.contains(*value),
            "{value} is written now; drop the exemption ({reason})"
        );
    }
    assert!(declared(UNKNOWN_ACTOR));
}

#[test]
fn a_threaded_actor_is_named_as_threaded() {
    let found = census(&repo_root());
    let expected: BTreeSet<&str> = THREADED.iter().map(|(path, _)| *path).collect();
    let actual: BTreeSet<&str> = found
        .iter()
        .filter(|(_, sites)| sites.iter().any(|s| matches!(s, Site::Unresolved(_))))
        .map(|(file, _)| file.as_str())
        .collect();
    assert_eq!(
        actual, expected,
        "the files whose actor the census cannot evaluate are not the ones listed as threaded"
    );
}

#[test]
fn the_census_reports_an_undeclared_actor() {
    // The reverse control: a reader that cannot tell a declared actor from an
    // undeclared one would pass the census above by never complaining.
    let sample = r#"ChatOptions::default().with_actor("planner")"#;
    let sites = sites_in(sample);
    assert_eq!(sites, vec![Site::Literal("planner".to_string())]);
    assert!(!declared("planner"), "the sample has to be undeclared");

    // And a declared one is not reported, so the reader is not merely always red.
    let ok = sites_in(r#"ChatOptions::default().with_actor("ralph")"#);
    assert!(matches!(ok.as_slice(), [Site::Literal(v)] if v == "ralph"));
    assert!(declared("ralph"));
}

// ============================================================================
// 第二格：这条产出替哪件活花的钱
// ============================================================================

/// 一条产出线的花销记在哪件活上。
///
/// 账本按两格取行：`actor` 答「谁花的」，`task_id` 答「哪件活花的」。上面那张
/// 名单管的是第一格，这一张管第二格——同一份名单，一个产出既然说了「谁」，就得
/// 说清「哪件活」；不替任何任务花钱的那些必须在这里写明它替的是什么。
///
/// 写明的理由不只是文档：一个产出把「来源 id」塞进 `task_id` 看着像归了因，实际
/// 读成本的那一面按任务图 join，查不到就是归不到（`unattributed_between` 把它
/// 算作无归因），于是账上多出一串看起来像任务、永远对不上的 id。反过来，一个替
/// 任务花钱的产出没盖章时，那件活的成本里不含它——2026-11-01 实测 635 次自检调用
/// 就是这样从「这件变更花了多少」里消失的。
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskKeying {
    /// 任务图里的一件活：任务 id 随调用走。
    Task,
    /// 任务图不管的那样东西，名字写在这里。
    KeyedBy(&'static str),
}

/// 与 `PRODUCERS` 同一份文件清单，逐个说清这家的钱记在哪件活上。
const TASK_KEYING: &[(&str, TaskKeying)] = &[
    ("crates/cog-agent/src/agent.rs", TaskKeying::Task),
    (
        "crates/cog-agent/src/agent_kernel/runtime.rs",
        TaskKeying::Task,
    ),
    ("crates/cog-agent/src/self_review.rs", TaskKeying::Task),
    (
        "crates/cog-agent/tests/self_review_task_attribution.rs",
        TaskKeying::Task,
    ),
    (
        "crates/cog-collaboration/src/squad/ralph/ralph_loop.rs",
        TaskKeying::Task,
    ),
    (
        "crates/cog-eval/src/adapters/hle.rs",
        TaskKeying::KeyedBy("一次评测里的那条样本，任务图不建它的节点"),
    ),
    (
        "crates/cog-eval/src/judge.rs",
        TaskKeying::KeyedBy("一次评测里的那个判分，任务图不建它的节点"),
    ),
    (
        "crates/cog-eval/src/scaffolds/mod.rs",
        TaskKeying::KeyedBy("一次评测运行，任务图不建它的节点"),
    ),
    (
        "crates/cog-extension/src/hostdocs_assist.rs",
        TaskKeying::KeyedBy("沙盒里的那份文档"),
    ),
    (
        "crates/cog-reflection/src/evolution.rs",
        TaskKeying::KeyedBy("一个技能 / 钩子 / 工具变体"),
    ),
    (
        "crates/cogneva/src/eval_table.rs",
        TaskKeying::KeyedBy("评测表里的一行"),
    ),
    (
        "crates/cogneva/tests/collaboration_self_review_test.rs",
        TaskKeying::KeyedBy("一个替身：它不发出任何计费调用"),
    ),
];

/// 这一个文件在产出文本里盖不盖任务章。
///
/// 只认两种写法：直接挂 `.with_task(`，或走那个把任务挂上去的助手
/// （`stamp_task`）。直接写 `LLM_TASK_HEADER` 的不是产出端——读请求头的网关
/// 也在那一格，而它记的是调用方说了什么，不是自己声称哪件活。
fn stamps_a_task(text: &str) -> bool {
    text.contains(".with_task(") || text.contains("stamp_task(")
}

/// 全树里盖任务章的文件。
fn task_stampers(root: &Path) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for file in all_crate_files(root) {
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .display()
            .to_string();
        if EXCLUDED.iter().any(|(path, _)| *path == rel) {
            continue;
        }
        let text = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", file.display()));
        if stamps_a_task(&production_text(&text)) {
            found.insert(rel);
        }
    }
    found
}

#[test]
fn every_producer_says_what_its_spend_is_charged_to() {
    let declared: BTreeSet<&str> = TASK_KEYING.iter().map(|(path, _)| *path).collect();
    let producers: BTreeSet<&str> = PRODUCERS.iter().map(|(path, _)| *path).collect();
    let unclassified: Vec<_> = producers.difference(&declared).collect();
    let stale: Vec<_> = declared.difference(&producers).collect();
    assert!(
        unclassified.is_empty(),
        "these producers say who spends but not what the spend is for: {unclassified:?}\n\
         给它们补一条 `TASK_KEYING`：替任务花钱的写 `Task` 并让任务 id 随调用走，\
         不替任务花钱的写清它替的是什么。"
    );
    assert!(
        stale.is_empty(),
        "these files are classified here but no longer write an actor: {stale:?}"
    );
    let mut seen = BTreeSet::new();
    for (path, _) in TASK_KEYING {
        assert!(seen.insert(*path), "{path} is classified twice");
    }
}

/// 只盖任务章的那一侧：写了的必须是任务产出，没写的一个字都不许有。
#[test]
fn a_producer_keyed_by_something_else_stamps_no_task() {
    let root = repo_root();
    let stamped = task_stampers(&root);
    let mut complaints = Vec::new();
    for (path, keying) in TASK_KEYING {
        let TaskKeying::KeyedBy(key) = keying else {
            continue;
        };
        if stamped.contains(*path) {
            complaints.push(format!(
                "{path} 盖了任务章，但它这里声明的是「键在 {key} 上」：\
                 把 {key} 写进 task_id 看着像归了因，按任务图 join 时对不上，\
                 读成本的那一面照样算作无归因"
            ));
        }
    }
    assert!(complaints.is_empty(), "{}", complaints.join("\n"));
}

/// 另一侧：盖了任务章的每一个文件都得在这里声明成 `Task`。
///
/// 缺一个就是一个新产出线悄悄开始记任务了，而上面两张表都还是绿的。
#[test]
fn every_file_that_stamps_a_task_is_declared_task_keyed() {
    let root = repo_root();
    let stamped = task_stampers(&root);
    let mut expected: BTreeSet<String> = TASK_KEYING
        .iter()
        .filter(|(_, keying)| matches!(keying, TaskKeying::Task))
        .map(|(path, _)| path.to_string())
        .collect();
    // 定义 `.with_task` 的那份契约不是产出端：它写的是「怎么盖」，不是「替谁盖」。
    expected.remove("crates/cog-core/src/contract/llm.rs");

    let added: Vec<_> = stamped.difference(&expected).collect();
    let gone: Vec<_> = expected.difference(&stamped).collect();
    assert!(
        added.is_empty(),
        "these files stamp a task header without being declared task-keyed: {added:?}\n\
         替任务花钱的登记成 `Task`；不替任何任务花钱的，那个章是编出来的 id。"
    );
    assert!(
        gone.is_empty(),
        "these files are declared `Task` but write no task header: {gone:?}\n\
         任务是空的，这件活的成本里就不会有这一族的钱。"
    );
}

#[test]
fn the_task_scan_sees_a_stamp() {
    // 反向对照：一个认不出任务章的扫法在两边都是绿的。
    assert!(stamps_a_task(&production_text(
        r#"let o = ChatOptions::default().with_task("t-1");"#
    )));
    assert!(!stamps_a_task(&production_text(
        r#"let o = ChatOptions::default().with_actor("ralph");"#
    )));
}

// ============================================================================
// 第三格：这棵树里有哪些地方能把请求交给模型
// ============================================================================
//
// 上面两格读的是「写了身份的那些调用点」：它们从文本里认出 `with_actor(...)`
// 并判那一格的值。一个**完全没写身份**的调用点在那里是不可见的——它不带任何
// 字面量，认字面量的普查当然看不见它。那个案子只有到运行时才露出来：网关
// 把这次调用拒成 400，或者落进 `actor="unknown"` 那一格。
//
// 这一格补的是源头那一半：生产代码里**每一处**把请求交给模型的地方，都必须在这里
// 有一行，说清它的身份从哪来。新增一个调用点、而没在这里登记，就直接红——那时
// 要回答的问题不是「该填哪个值」，是「这一处该不该有身份」。
//
// 它判不到的东西也写在明处：`Forwards` 说的是「它传的是调用方的 `ChatOptions`」，
// 而这份普查读不出调用方传进来的那一份里有没有身份。那条链的尽头是 `Stamps`
// 那些文件，它们由上面第一格逐个判过字面量。

/// 一处调用的身份从哪来。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorSource {
    /// 它自己把身份写在这次请求上（第一格看得见这个字面量）。
    Stamps,
    /// 它把调用方造好的 `ChatOptions` 原样传下去——身份是调用方的，它既不能
    /// 凭空造一个，也不会把它弄丢。
    Forwards,
}

/// 生产代码里把请求交给模型的每一处，以及它的身份从哪来。
///
/// 判的是**文件集合**，不是行号：一处换位置不算新调用点，一个文件里多出第二个
/// 悄悄不盖身份的调用点算——而这一点也能被看见，因为同一个文件已经登记过一次。
const CALL_SITES: &[(&str, ActorSource)] = &[
    ("crates/cog-agent/src/agent.rs", ActorSource::Stamps),
    (
        "crates/cog-agent/src/agent_kernel/runtime.rs",
        ActorSource::Stamps,
    ),
    ("crates/cog-agent/src/self_review.rs", ActorSource::Stamps),
    (
        "crates/cog-collaboration/src/squad/ralph/ralph_loop.rs",
        ActorSource::Stamps,
    ),
    // `json_mode` 就在这份契约里：它把调用方给的 `ChatOptions` 原样递下去。
    // 头与 setter 也定义在这份文件里，但那次调用不带自己的身份——它是调用方的。
    ("crates/cog-core/src/contract/llm.rs", ActorSource::Forwards),
    ("crates/cog-eval/src/backoff.rs", ActorSource::Forwards),
    ("crates/cog-eval/src/rig.rs", ActorSource::Forwards),
    (
        "crates/cog-eval/src/scaffolds/codeact.rs",
        ActorSource::Forwards,
    ),
    ("crates/cog-eval/src/scaffolds/mod.rs", ActorSource::Stamps),
    // 界面的这一轮不再自己叫模型：它把对话交给内核那条循环，身份由内核按
    // `agent:<这位员工>` 盖下（`crates/cog-agent/src/agent_kernel/runtime.rs` 那一条）。
    // 这一格删掉而不是改成 `Forwards`——网关手里已经没有请求可递。
    ("crates/cog-llm/src/hot_swap.rs", ActorSource::Forwards),
    ("crates/cog-llm/src/observed.rs", ActorSource::Forwards),
    (
        "crates/cog-llm/src/provider/anthropic.rs",
        ActorSource::Forwards,
    ),
    (
        "crates/cog-llm/src/provider/google.rs",
        ActorSource::Forwards,
    ),
    (
        "crates/cog-llm/src/provider/ollama.rs",
        ActorSource::Forwards,
    ),
    (
        "crates/cog-llm/src/provider/openai.rs",
        ActorSource::Forwards,
    ),
    (
        "crates/cog-llm/src/provider/responses.rs",
        ActorSource::Forwards,
    ),
    ("crates/cog-llm/src/resilience.rs", ActorSource::Forwards),
    ("crates/cog-llm/src/routing.rs", ActorSource::Forwards),
    ("crates/cogneva/src/eval_table.rs", ActorSource::Stamps),
    (
        "crates/cog-reflection/src/evolution.rs",
        ActorSource::Stamps,
    ),
];

/// 生产代码里出现了对模型的调用（`chat` / `chat_stream`）的那些文件。
///
/// 读的是产出面：`#[cfg(test)] mod tests` 里的替身也在调 `chat`，而替身不
/// 替任何东西花真钱，把它们算进来会让这张名单一半是夹具、一半是产出。
fn call_site_files(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for file in production_sources(root) {
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .display()
            .to_string();
        let text = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", file.display()));
        let production = production_text(&text);
        if production.contains(".chat(") || production.contains(".chat_stream(") {
            out.insert(rel);
        }
    }
    out
}

#[test]
fn every_place_that_hands_a_request_to_a_model_says_where_its_actor_comes_from() {
    let root = repo_root();
    let actual = call_site_files(&root);
    let expected: BTreeSet<String> = CALL_SITES
        .iter()
        .map(|(path, _)| path.to_string())
        .collect();

    let added: Vec<_> = actual.difference(&expected).collect();
    assert!(
        added.is_empty(),
        "these production files call a model without being listed here: {added:?}\n\
         每一处都要回答「它的身份从哪来」：自己盖（`Stamps`，第一格看得见那个字面量）、\n\
         还是原样传下去（`Forwards`）。\n\
         答不出来的时候，那不是一格登记，是这一处不该有身份。"
    );

    let gone: Vec<_> = expected.difference(&actual).collect();
    assert!(
        gone.is_empty(),
        "these files are listed as calling a model but no longer do: {gone:?}\n\
         名单只增不减时会慢慢变成一张历史表，它就不再回答「现在谁能碰到模型」。"
    );
}

#[test]
fn a_call_site_that_stamps_an_actor_is_one_the_literal_census_sees() {
    let producers: BTreeSet<&str> = PRODUCERS.iter().map(|(path, _)| *path).collect();
    let liars: Vec<_> = CALL_SITES
        .iter()
        .filter(|(_, source)| *source == ActorSource::Stamps)
        .map(|(path, _)| *path)
        .filter(|path| !producers.contains(path))
        .collect();
    assert!(
        liars.is_empty(),
        "these files are declared to stamp an actor, but the literal census does not see \
         one in them: {liars:?}\n\
         两格读的是同一件事的两面：这里说「它自己盖」，第一格就该读得出那个字面量。\n\
         读不出只有两种可能——它其实在往下传（改成 `Forwards`），或者它把身份忘了。"
    );
}
