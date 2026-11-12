//! principle: flat-agent-society
//!
//! 决定待遇的地方不许读标签。
//!
//! 差分（`label_differential_test`）证明的是「同一件活换个标签接手，待遇逐字相同」。
//! 它有两个看不见的角落：
//!
//! - 只在某条没被走到的路上才读标签的判据——差分只走了两个标签、一条路。
//! - 一个字节都不落进那份记录的待遇（沙箱权限、重试次数、某段提示的措辞）。
//!
//! 所以再把「读点」按结构卡一遍。判据不是全树清点角色名——那有一百多处聊天消息、
//! RBAC、协议座位，报出来的绝大多数不是读点；一张天天报假阳性的表没人会信，
//! 判据也就跟着失效。这里判的
//! 是**共现**：一行生产代码里同时出现「待遇的名字」与「标签的名字」时，这一行就有
//! 本事让待遇跟着标签走，是或不是都得作者自己说清。今天这棵树上一处共现都没有，
//! 所以这条判据的底噪是零——它一旦红，就是新长出来的读点。
//!
//! 另外三条把读点的形状钉住：
//!
//! - 唯一收下标签的那个构造函数（`runtime_config_for`）只能写「谁在跑」，
//!   写不了待遇——它拿得到标签，却拿不到写待遇的理由。
//! - 三个判据函数（轮次上限、自审判据、席位 schema）的参数表里没有标签。
//! - 待遇的每一个写点都登记着它由三样里的哪一样给（活声明的、实测的、覆盖的）；
//!   新长出来的写点不登记就红。

use std::collections::BTreeSet;

use crate::common;

use common::lex::Dialect;
use common::root::repo_root;
use common::routes::{matching_brace, matching_paren};
use common::rust_production::{production_lines, production_sources, relative_name};

/// 待遇的名字：决定「这件活怎么被对待」的每一格。
///
/// 与差分那份记录里的待遇半同源。这里只收**名字本身足以认出是一次待遇决定**的，
/// 不收 `tools` 这类同时是普通词的名字——它们在差分里已经被逐字比过。
const TREATMENT: &[&str] = &[
    "max_iterations",
    "context_window_size",
    "skill_cache_ttl_secs",
    "think_stall_timeout_secs",
    "deliverable",
    "deliverable_state",
    "stated_directions",
    "rubric",
    "schema_for",
];

/// 标签的名字：员工的角色名、技能行名、归因用的 actor。
///
/// `seat` 不在这里——席位是寻址，同一个座位换个人坐必须拿到同一份待遇，那是
/// 另一个方向的门（差分里两半的边界）。
const LABELS: &[&str] = &["role", "role_type", "skill_id", "actor", "label"];

/// 待遇的写点：谁在写，写哪一格，这一格由三样里的哪一样给。
///
/// 写待遇的地方必须在这张表上。名字决定了它只能是这三样之一——活自己声明了什么
/// （`declared`）、接手人的实测交卷（`measured`）、操作者的显式覆盖（`override`）。
const WRITE_POINTS: &[(&str, &str, &str)] = &[
    // 任务上的声明，管理器建 agent 时抄进运行时。
    (
        "crates/cog-agent/src/runtime/manager.rs",
        "deliverable",
        "declared",
    ),
    (
        "crates/cog-agent/src/runtime/manager.rs",
        "deliverable_state",
        "declared",
    ),
    (
        "crates/cog-agent/src/runtime/manager.rs",
        "stated_directions",
        "declared",
    ),
    // 轮次上限：判定函数把值与来源一起交出来，写回的就是它返回的那个值。
    (
        "crates/cog-agent/src/agent_kernel/runtime.rs",
        "max_iterations",
        "measured+override",
    ),
    // 判据构造点：被判的物与有人说过的话。
    (
        "crates/cog-agent/src/self_review.rs",
        "deliverable",
        "declared",
    ),
    (
        "crates/cog-agent/src/self_review.rs",
        "stated_directions",
        "declared",
    ),
];

/// 一行里作为一个词出现的 `word`（前后不是标识符字符）。
fn has_word(line: &str, word: &str) -> bool {
    let bytes = line.as_bytes();
    let needle = word.as_bytes();
    let mut from = 0;
    while let Some(offset) = line[from..].find(word) {
        let start = from + offset;
        let end = start + word.len();
        let before_ok =
            start == 0 || !(bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_');
        let after_ok =
            end >= bytes.len() || !(bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_');
        if before_ok && after_ok {
            return true;
        }
        from = start + needle.len().max(1);
    }
    false
}

/// 找出 `needle` 那一处 `fn …` 的参数表，返回参数原文。
fn parameters(text: &str, needle: &str) -> String {
    let start = text.find(needle).unwrap_or_else(|| {
        panic!("树里找不到 `{needle}`：这条判据读的名字改了，它现在什么都没在守")
    });
    let open = start
        + text[start..]
            .find('(')
            .unwrap_or_else(|| panic!("`{needle}` 后面没有参数表"));
    let close = matching_paren(text, open, Dialect::Rust)
        .unwrap_or_else(|| panic!("`{needle}` 的参数表没有收口"));
    text[open..=close].to_string()
}

/// 生产代码里同时出现「待遇的名字」与「标签的名字」的行。
#[test]
fn no_production_line_names_a_treatment_and_a_label_together() {
    let root = repo_root();
    let mut hits = Vec::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in production_lines(&text) {
            let treatment: Vec<&str> = TREATMENT
                .iter()
                .copied()
                .filter(|name| has_word(line, name))
                .collect();
            let labels: Vec<&str> = LABELS
                .iter()
                .copied()
                .filter(|name| has_word(line, name))
                .collect();
            if !treatment.is_empty() && !labels.is_empty() {
                hits.push(format!(
                    "{}：{}（待遇 {:?}，标签 {:?}）",
                    relative_name(&root, &path),
                    line.trim(),
                    treatment,
                    labels
                ));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "这些生产代码行里，决定待遇的名字与标签的名字同处一行——这一行有本事让\
         换个人坐就换一种待遇：\n{}",
        hits.join("\n")
    );
}

/// 唯一收下标签的那个构造函数，写不进待遇。
#[test]
fn the_constructor_that_receives_the_label_can_only_write_who_is_running() {
    let root = repo_root();
    let text = std::fs::read_to_string(root.join("crates/cog-agent/src/runtime/manager.rs"))
        .expect("manager.rs readable");
    let start = text
        .find("pub async fn runtime_config_for")
        .expect("runtime_config_for 不在了：这条判据守的是那一处的形状，得跟着它搬");
    let open = start
        + text[start..]
            .find('{')
            .expect("runtime_config_for 没有函数体");
    let close = matching_brace(&text, open, Dialect::Rust).expect("函数体没有收口");
    let body = &text[open..=close];

    let mut written = BTreeSet::new();
    let mut from = 0usize;
    while let Some(offset) = body[from..].find("config.") {
        let at = from + offset;
        let rest = &body[at + "config.".len()..];
        let field: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let after = rest[field.len()..].trim_start();
        if let Some(after) = after.strip_prefix('=') {
            // `==` 是比较，不是写入。
            if !after.trim_start().starts_with('=') {
                written.insert(field);
            }
        }
        from = at + "config.".len();
    }

    let allowed: BTreeSet<String> = ["agent_id", "role", "skill_config"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let extra: Vec<&String> = written.difference(&allowed).collect();
    assert!(
        extra.is_empty(),
        "`runtime_config_for` 拿得到标签，现在它也能写这些待遇了：{extra:?}。\
         这个函数是唯一同时握着标签与运行时的那一处——它写待遇，换个人坐就换一种\
         待遇，而差分只走了两条路，未必走得到这一条"
    );
    assert!(
        written.contains("role"),
        "`runtime_config_for` 不再写身份了：它要么搬走了，要么标签已经不进运行时——\
         这条判据得跟着搬，不能在这里空转"
    );
}

/// 三个判据函数的参数表里没有标签。
#[test]
fn the_deciders_take_the_work_not_the_name() {
    let root = repo_root();
    let deciders = [
        (
            "crates/cog-agent/src/observable.rs",
            "pub fn iteration_budget_decision_for",
        ),
        ("crates/cog-agent/src/self_review.rs", "fn judging_rubric"),
        (
            "crates/cog-collaboration/src/config.rs",
            "pub fn schema_for",
        ),
    ];
    for (file, needle) in deciders {
        let path = root.join(file);
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}：{e}"));
        let params = parameters(&text, needle);
        for label in LABELS {
            assert!(
                !has_word(&params, label),
                "{file} 的 `{needle}` 参数表里出现了标签 `{label}`：{params}\n\
                 待遇由活声明的、实测的、覆盖的来决定；把标签递进判据函数的参数表，\
                 就是给它一条按名字分岔的路"
            );
        }
    }
}

/// 待遇的每个写点都登记着它由哪一样给。
#[test]
fn every_write_point_names_which_of_the_three_gives_the_value() {
    let root = repo_root();
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let name = relative_name(&root, &path);
        for line in production_lines(&text) {
            for field in TREATMENT {
                // `x.<field> = …`：写入，不是比较、不是结构体字面量。
                let pattern = format!(".{field}");
                let mut from = 0;
                while let Some(offset) = line[from..].find(&pattern) {
                    let at = from + offset;
                    let rest = &line[at + pattern.len()..];
                    let trimmed = rest.trim_start();
                    if trimmed.starts_with('=') && !trimmed.starts_with("==") {
                        seen.insert((name.clone(), (*field).to_string()));
                    }
                    from = at + pattern.len();
                }
            }
        }
    }

    let declared: BTreeSet<(String, String)> = WRITE_POINTS
        .iter()
        .map(|(file, field, _)| ((*file).to_string(), (*field).to_string()))
        .collect();
    let unregistered: Vec<&(String, String)> = seen.difference(&declared).collect();
    let stale: Vec<&(String, String)> = declared.difference(&seen).collect();
    assert!(
        unregistered.is_empty(),
        "这些地方在写待遇，却没有登记它由哪一样给：{unregistered:?}\n\
         决定待遇的只有三样——活自己声明了什么、接手人的实测交卷、操作者的显式覆盖。\
         新写点要先回答它属于哪一样"
    );
    assert!(
        stale.is_empty(),
        "登记的写点找不到了：{stale:?}。表过期了，或者那一处在没人注意的时候搬走了"
    );
    for (file, field, source) in WRITE_POINTS {
        for part in source.split('+') {
            assert!(
                matches!(part, "declared" | "measured" | "override"),
                "{file} 的 `{field}` 写成 `{part}`：决定待遇的只许是活自己的声明、\
                 接手人的实测交卷、操作者的显式覆盖"
            );
        }
    }
}

/// 差分登记过的每一格待遇，这道门都认识。
///
/// 两处各有一份「哪些格子算待遇」的名单：差分按它能比出来的值登记（`TREATMENT_SOURCES`），
/// 本门按它能扫到的名字定词表。差分先登记了新的一格、本门不认识时，那一格就有一条
/// 「按标签决定」的读法而两份判据都看不见——两个数都读作完整，缺的那一格没有读数。
/// 所以这里把差分那张表读出来，逐个问本门认不认识；不认识的必须在 `OUTSIDE` 里说清
/// 为什么它不是一行代码能扫到的名字。
#[test]
fn every_treatment_the_differential_registers_is_a_name_this_gate_can_scan() {
    /// 差分的名字与本门词表拼法不同，或者根本不是「写进某个配置的一格」时的去处。
    const OUTSIDE: &[(&str, &str)] = &[
        (
            "task_type",
            "由声明派生出来的值，没有一处把它写进配置；差分比的是它的结果",
        ),
        (
            "tools",
            "进程装的那份清单，操作者覆盖；没有按标签写它的地方",
        ),
        (
            "deliverable_source",
            "声明记录本身（读出来的那一份），不是一个被写进去的旋钮",
        ),
    ];
    /// 差分的名字 → 本门词表里的名字。
    const SAME_CELL: &[(&str, &str)] = &[("schema", "schema_for")];

    let root = repo_root();
    let path = root.join("crates/cogneva/tests/label_differential_test.rs");
    let text = std::fs::read_to_string(&path).expect("差分读得到");
    let mut registered: Vec<String> = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        if line.contains("const TREATMENT_SOURCES") {
            inside = true;
            continue;
        }
        if !inside {
            continue;
        }
        if line.trim_start().starts_with("];") {
            break;
        }
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("(\"") {
            if let Some(end) = rest.find('"') {
                registered.push(rest[..end].to_string());
            }
        }
    }
    assert!(
        registered.len() >= 8,
        "差分那张待遇表只读到 {registered:?}：读法过期了，这条判据现在什么都没在守"
    );

    let scanned: Vec<&str> = TREATMENT.to_vec();
    for name in &registered {
        if OUTSIDE.iter().any(|(out, _)| out == name) {
            continue;
        }
        let cell = SAME_CELL
            .iter()
            .find(|(from, _)| from == name)
            .map_or(name.as_str(), |(_, to)| *to);
        assert!(
            scanned.contains(&cell),
            "差分把 `{name}` 登记成待遇，本门却扫不到这个名字——它有一条按标签决定\
             的读法，两边都看不见"
        );
    }
    for (name, reason) in OUTSIDE {
        assert!(
            registered.iter().any(|r| r == name),
            "`OUTSIDE` 里的 `{name}` 已经不在差分的待遇表里了（{reason}）：这一行过期了"
        );
    }
}
