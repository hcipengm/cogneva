//! principle: feedback_no_hardcoding_config_surface
//! The sibling gate in this directory holds every label name the rules *group*
//! by. It cannot see the other half of the same vocabulary: the key inside a
//! selector. `cogneva_change_dispatch_verdict{pool_reading="available"}` names
//! one key in the rule and one key in the emitter, and until this file nothing
//! compared them.
//!
//! That miss is not theoretical. `change_pool_block_stranded` selected
//! `reading="available"` on a series the emitter writes with `pool_reading`;
//! the emitter's own comment says "state, not reading" precisely because the
//! sibling family already spends `reading` on a different question. The rule
//! was structurally unable to match anything, and PromQL does not fail on a
//! selector that names a label the series does not carry -- it matches nothing,
//! the rule evaluates over the empty set, and "the alert did not fire" is the
//! same reading as "nothing was stranded". Its promtool fixture had been
//! written to the same wrong key, so the fixture agreed with the rule and both
//! disagreed with the code.
//!
//! What it judges: for every selector key the alert rules put on one of our own
//! `cogneva_` series, some emission site of *that* metric writes that key --
//! as a `HashMap::from([("k", …)])` / `.insert(…)` key, as a `with_label(…)`
//! key, or through a `LABEL_*` / `*_LABEL` constant whose value it is.
//!
//! What it cannot judge, said plainly because the count is pinned below:
//!
//! * **Neighbouring readings.** The key set of a metric is read from the lines
//!   around its emission site. Two counters published a few lines apart share
//!   that window, so a key belonging to the neighbour satisfies this gate. It
//!   judges "somebody writes this key in this region", not "this series carries
//!   it". Tightening it to the metric's own call was tried and costs the
//!   closure-built label sets (`let cell = |kind: &str| { … LABEL_KIND … }`),
//!   which is the larger and more common shape here.
//! * **Series published outside this tree's Rust.** The `cogneva_redis_*`
//!   gauges are emitted by the redis container's own shell loop in the chart,
//!   and the `*_observed_timestamp_seconds` companions are attached by the
//!   collection path rather than by a site in `crates/*/src`. Those pairs are
//!   listed in `UNJUDGED` with the reason, and that table is checked in both
//!   directions: a new rule landing on an unjudged metric is red until someone
//!   either teaches the extractor or writes the reason down.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::lex::Dialect;
use common::routes::{matching_brace, repo_root};
use common::rules::promql_expressions;

use common::rust_production::{
    production_lines_keeping_comments, production_sources, relative_name,
};

/// Keys the scrape surface attaches, keyed to the reason this tree does not
/// write them. Same list and same reasons as the sibling gate: these belong to
/// somebody else's vocabulary, and holding them to our emitters would be a
/// census of `kube-state-metrics`.
const FOREIGN: &[(&str, &str)] = &[
    ("pod", "Pod 名由 kubelet 的服务发现打上，代码不写它"),
    ("container", "同上：容器名由采集面打"),
    (
        "instance",
        "Prometheus 按抓取目标打的地址，不是我们写进 series 的",
    ),
    (
        "job",
        "Prometheus 按抓取任务打的任务名；代码里那个 `job` 是日志标签，不是这行的来源",
    ),
    ("endpoint", "服务发现按端点打的标签"),
    (
        "persistentvolume",
        "kube-state-metrics 的 PV 名：这里的分组加在它的 series 上，\
         同一段表达式的另一支才是我们的读数",
    ),
];

/// 规则选了、但这个门判不了的键，逐条写明理由。
///
/// 两个方向都判：表里多一条没人再选的行会红，规则里多一条表里没有的键也会红。
/// 后者是这张表唯一的作用——**新增一条落在判不了的读数上的选择子，是一个要
/// 写下来的决定**，不是一次静默的放行。
///
/// 现在这 11 条都判不了，两个来由：redis 那五条 gauge 由 chart 里 redis 容器的
/// shell 循环直接打印，`*_observed_timestamp_seconds` 伴随序列由采集面按本体那条
/// 读数打戳，两者在 `crates/*/src` 里都没有发点。
const UNJUDGED: &[(&str, &str, &str)] = &[
    (
        "cogneva_acceptance_items",
        "verdict",
        "验收面由集群外的普查过程发布，crates/*/src 里没有它的发点",
    ),
    (
        "cogneva_backup_run_readings_observed_timestamp_seconds",
        "readout",
        "`*_observed_timestamp_seconds` 伴随序列由采集面按同一个读数打戳，树里没有它的发点",
    ),
    (
        "cogneva_change_ledger_in_effect_observed_timestamp_seconds",
        "state",
        "同上：伴随序列，键随本体那条读数走",
    ),
    (
        "cogneva_notification_sign_total",
        "outcome",
        "网关签名面这条读数在 security_gateway 的另一个构造里，这一带读不到它的键集合",
    ),
    (
        "cogneva_policy_store_readings_observed_timestamp_seconds",
        "readout",
        "同上：伴随序列，键随本体那条读数走",
    ),
    (
        "cogneva_ready_admission_readings_observed_timestamp_seconds",
        "kind",
        "同上：伴随序列，键随本体那条读数走",
    ),
    (
        "cogneva_redis_info_interval_seconds",
        "namespace",
        "redis 的这几条 gauge 由 chart 里 redis 容器的 shell 循环直接打印，不在 Rust 里",
    ),
    (
        "cogneva_redis_info_updated_timestamp_seconds",
        "namespace",
        "同上",
    ),
    ("cogneva_redis_memory_used_bytes", "namespace", "同上"),
    (
        "cogneva_redis_wal_scan_interval_seconds",
        "namespace",
        "同上",
    ),
    (
        "cogneva_redis_wal_scan_timestamp_seconds",
        "namespace",
        "同上",
    ),
];

/// 发点附近往前读多少行算「这一带」。往后读得少，因为标签集合写在调用之前。
const WINDOW_BEFORE: usize = 80;
const WINDOW_AFTER: usize = 15;

/// 一次普查至少该读到多少个选择子键。
///
/// 测到的那一天规则里有 69 个（其中 6 个是采集面的键）。读到 0 个与规则里没有
/// 选择子同形，所以解析面自己要有下限。
const MIN_SELECTOR_PAIRS: usize = 60;

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// 从 `at` 开始读一个标识符；`at` 不是一个标识符的开头就返回 `None`。
fn ident_at(text: &str, at: usize) -> Option<(&str, usize)> {
    let rest = text.get(at..)?;
    let mut chars = rest.char_indices();
    let (_, first) = chars.next()?;
    if !is_ident_start(first) {
        return None;
    }
    let mut end = rest.len();
    for (offset, c) in chars {
        if !is_ident_char(c) {
            end = offset;
            break;
        }
    }
    Some((&rest[..end], at + end))
}

/// `a::b::c` 这样的路径在 `at` 处读成最后一段 `c`。
fn path_tail_at(text: &str, at: usize) -> Option<(&str, usize)> {
    let (mut name, mut cursor) = ident_at(text, at)?;
    loop {
        let rest = text.get(cursor..)?;
        let after_ws = rest.trim_start();
        if !after_ws.starts_with("::") {
            return Some((name, cursor));
        }
        let sep = rest.len() - after_ws.len();
        let at_next = cursor + sep + 2;
        let skipped = text.get(at_next..)?.trim_start();
        let at_next = at_next + (text.len() - at_next - skipped.len());
        match ident_at(text, at_next) {
            Some((segment, end)) => {
                name = segment;
                cursor = end;
            }
            None => return Some((name, cursor)),
        }
    }
}

/// 一行里出现的所有标识符，连同它们的位置。
fn idents_in(line: &str) -> Vec<(&str, usize)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < line.len() {
        match ident_at(line, i) {
            Some((name, end)) => {
                out.push((name, i));
                i = end;
            }
            None => i += 1,
        }
    }
    out
}

/// 每个 `"…"` 字面量的内容与它在行内的位置。
fn string_literals(line: &str) -> Vec<(&str, usize)> {
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
            if bytes[j] == b'\\' {
                j += 2;
                continue;
            }
            if bytes[j] == b'"' {
                break;
            }
            j += 1;
        }
        if j <= bytes.len() {
            out.push((&line[start..j.min(line.len())], start));
        }
        i = j.max(i + 1);
    }
    out
}

/// `const <name>: &str = "<value>";` 与宏表里的 `<NAME> => "<value>",`，全树一张表。
///
/// 一个名字可以对应**多个**值，因为它们来自不同的 crate、彼此没有关系：
/// `BYTES_METRIC` 在 `cog-observability` 里是 `cogneva_image_store_bytes`，在
/// `cog-reflection` 里是 `cogneva_volume_capacity_bytes`。只留第一次见到的那个时，
/// 后一个读数在表里就没有发点，整条被判成「读不出来」——一张按裸名字索引的全树表
/// 迟早会撞上这种重名，所以这里按名字收全部值。
fn str_consts(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let production = production_lines_keeping_comments(&text).join("\n");
        let mut from = 0;
        while let Some(at) = production[from..].find("const ") {
            let at = from + at;
            from = at + "const ".len();
            let Some((name, cursor)) = ident_at(&production, from) else {
                continue;
            };
            let rest = &production[cursor..];
            let rest = rest.trim_start();
            if !rest.starts_with(':') {
                continue;
            }
            let Some(eq) = rest.find('=') else {
                continue;
            };
            let after_eq = rest[eq + 1..].trim_start();
            let Some(after_eq) = after_eq.strip_prefix('"') else {
                continue;
            };
            let Some(end) = after_eq.find('"') else {
                continue;
            };
            out.entry(name.to_string())
                .or_default()
                .insert(after_eq[..end].to_string());
        }
        // `NAME => "value",`：读数名登记表不是 `const`，是一张宏表。只认 `const`
        // 的版本在这里会读空一半的读数，于是它们全被判成「读不出发点」。
        let mut from = 0;
        while let Some(at) = production[from..].find("=> ") {
            let at = from + at;
            from = at + 3;
            let Some((name, _)) = last_ident_before(&production, at) else {
                continue;
            };
            let after = production[at + 3..].trim_start();
            let Some(after) = after.strip_prefix('"') else {
                continue;
            };
            let Some(end) = after.find('"') else {
                continue;
            };
            let value = &after[..end];
            if value.is_empty() {
                continue;
            }
            out.entry(name.to_string())
                .or_default()
                .insert(value.to_string());
        }
    }
    out
}

/// `at` 之前紧挨着的那个标识符（跳过空白）。
fn last_ident_before(text: &str, at: usize) -> Option<(&str, usize)> {
    let head = text.get(..at)?;
    let trimmed = head.trim_end();
    let mut start = trimmed.len();
    for (offset, c) in trimmed.char_indices().rev() {
        if is_ident_char(c) {
            start = offset;
            continue;
        }
        break;
    }
    if start >= trimmed.len() {
        return None;
    }
    ident_at(trimmed, start)
}

/// `use …::TARGET as ALIAS;`：发点常写别名（`REDRIVE_REFUSALS_METRIC`），
/// 而常量只声明在 `metric_names` 里。别名解析到不动点，因为别名还能再被别名。
fn resolve_aliases(root: &Path, metrics: &mut BTreeMap<String, BTreeSet<String>>) {
    let mut aliases: Vec<(String, String)> = Vec::new();
    for path in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in production_lines_keeping_comments(&text) {
            let Some(at) = line.find("use ") else {
                continue;
            };
            let Some(as_at) = line[at..].find(" as ") else {
                continue;
            };
            let target = line[..at + as_at].trim_end().to_string();
            let target = target.rsplit("::").next().unwrap_or("").to_string();
            let Some((alias, _)) = ident_at(line, at + as_at + " as ".len()) else {
                continue;
            };
            if !target.is_empty() {
                aliases.push((target, alias.to_string()));
            }
        }
    }
    let mut changed = true;
    while changed {
        changed = false;
        for (target, alias) in &aliases {
            let Some(values) = metrics.get(target).cloned() else {
                continue;
            };
            let slot = metrics.entry(alias.clone()).or_default();
            for value in values {
                if slot.insert(value) {
                    changed = true;
                }
            }
        }
    }
}

/// 一个 `cogneva_` 系列名 → 它被发出去时这一带写下的键。
fn written_keys(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let consts = str_consts(root);
    let mut metrics: BTreeMap<String, BTreeSet<String>> = consts
        .iter()
        .filter(|(_, values)| values.iter().any(|value| value.starts_with("cogneva_")))
        .map(|(name, values)| {
            let only_metrics = values
                .iter()
                .filter(|value| value.starts_with("cogneva_"))
                .cloned()
                .collect();
            (name.clone(), only_metrics)
        })
        .collect();
    resolve_aliases(root, &mut metrics);
    let labels: BTreeMap<String, String> = consts
        .iter()
        .filter(|(name, _)| name.starts_with("LABEL_") || name.ends_with("_LABEL"))
        .filter_map(|(name, values)| {
            values
                .iter()
                .min()
                .map(|value| (name.clone(), value.clone()))
        })
        .collect();
    emitter_keys(root, &metrics, &labels)
}

fn emitter_keys(
    root: &Path,
    metrics: &BTreeMap<String, BTreeSet<String>>,
    labels: &BTreeMap<String, String>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let lines = production_lines_keeping_comments(&text);
        for (index, line) in lines.iter().enumerate() {
            let mut named: BTreeSet<String> = BTreeSet::new();
            for (name, _) in idents_in(line) {
                if let Some(values) = metrics.get(name) {
                    named.extend(values.iter().cloned());
                }
            }
            for (literal, _) in string_literals(line) {
                if literal.starts_with("cogneva_") {
                    named.insert(literal.to_string());
                }
            }
            if named.is_empty() {
                continue;
            }
            let lo = index.saturating_sub(WINDOW_BEFORE);
            let hi = (index + WINDOW_AFTER + 1).min(lines.len());
            let window = lines[lo..hi].join("\n");
            if !window.contains(".record_") && !window.contains(".with_label(") {
                continue;
            }
            let keys = keys_in(&window, labels);
            if keys.is_empty() {
                continue;
            }
            for metric in named {
                out.entry(metric).or_default().extend(keys.iter().cloned());
            }
        }
    }
    out
}

/// 一段文字里写下的标签键：`insert(…)` 与 `with_label(…)` 的第一个参数、
/// `("k", …)` / `("k".to_string(), …)` 这样的字面量键、以及解析得到值的常量。
fn keys_in(text: &str, labels: &BTreeMap<String, String>) -> BTreeSet<String> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for marker in [".insert(", ".with_label("] {
        let mut from = 0;
        while let Some(at) = text[from..].find(marker) {
            let at = from + at + marker.len();
            from = at;
            let rest = &text[at..];
            let skipped = rest.trim_start();
            let start = at + (rest.len() - skipped.len());
            if let Some((name, _)) = path_tail_at(text, start) {
                keys.insert(name.to_string());
            }
        }
    }
    // `("k", …)` 与 `("k".to_string(), …)`：键都写成字面量，且是小写的标识符。
    // 排除 `cogneva_` 开头的东西——那是读数名，不是键。
    let mut from = 0;
    while let Some(at) = text[from..].find("(\"") {
        let value_at = from + at + 2;
        from = value_at;
        let rest = &text[value_at..];
        let Some(close) = rest.find('"') else {
            continue;
        };
        let value = &rest[..close];
        if value.is_empty()
            || !value.chars().all(is_ident_char)
            || !value.starts_with(is_ident_start)
        {
            continue;
        }
        if value.starts_with("cogneva_") {
            continue;
        }
        let tail = rest[close + 1..].trim_start();
        if tail.starts_with(',')
            || tail.starts_with(").to_string()")
            || tail.starts_with(".to_string()")
        {
            keys.insert(value.to_string());
        }
    }
    for (name, _) in idents_in(text) {
        if name.starts_with("LABEL_") || name.ends_with("_LABEL") {
            keys.insert(name.to_string());
        }
    }
    // 常量换成它的值；换不掉的名字原样留着，它在下面的判据里会自然成为「没人写过」。
    keys.iter()
        .map(|key| labels.get(key).cloned().unwrap_or_else(|| key.clone()))
        .collect()
}

/// 一段 promql 里落在我们自己 `cogneva_` 序列上的选择子键：`(读数名, 键)`。
fn selector_pairs(expression: &str) -> BTreeSet<(String, String)> {
    let expression = expression.replace("\\\"", "\"");
    let mut out = BTreeSet::new();
    let mut from = 0;
    while let Some(at) = expression[from..].find("cogneva_") {
        let at = from + at;
        from = at + "cogneva_".len();
        // 读数名从 `cogneva_` 起读；不是标识符开头（比如已经读过了）就继续。
        let Some((metric, after_metric)) = ident_at(&expression, at) else {
            continue;
        };
        let rest = expression[after_metric..].trim_start();
        if !rest.starts_with('{') {
            continue;
        }
        // `rest` 是 `expression` 的后缀切片，所以开括号的字节下标是两个长度之差：
        // 共享的配平件要的是绝对下标，按相对偏移喂进去会从别处开始数。
        let open = expression.len() - rest.len();
        let Some(close) = matching_brace(&expression, open, Dialect::Rust) else {
            continue;
        };
        for matcher in split_matchers(&expression[open + 1..close]) {
            if let Some(key) = matcher_key(matcher) {
                out.insert((metric.to_string(), key.to_string()));
            }
        }
    }
    out
}

/// 选择子里的每一段都形如 `键<op>"值"`；按引号外的逗号切成段。
fn split_matchers(body: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = body.as_bytes();
    let mut quoted = false;
    let mut start = 0;
    for (i, byte) in bytes.iter().enumerate() {
        match byte {
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                out.push(&body[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < body.len() {
        out.push(&body[start..]);
    }
    out
}

/// 一段 `键=~"值"` 里的 `键`。四种匹配运算符都要认：`=`、`!=`、`=~`、`!~`。
fn matcher_key(matcher: &str) -> Option<&str> {
    let trimmed = matcher.trim_start();
    let (key, cursor) = ident_at(trimmed, 0)?;
    let rest = trimmed[cursor..].trim_start();
    if rest.starts_with('=') || rest.starts_with('!') {
        Some(key)
    } else {
        None
    }
}

fn rule_document(root: &Path) -> serde_json::Value {
    let path: PathBuf = root.join("deploy/helm/cogneva/files/cogneva.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// 规则里选的每一个 (读数名, 键)，去掉采集面那几条。
fn rule_selector_pairs(root: &Path) -> BTreeSet<(String, String)> {
    let document = rule_document(root);
    let mut expressions = Vec::new();
    promql_expressions(&document, &mut expressions);
    let foreign: BTreeSet<&str> = FOREIGN.iter().map(|(name, _)| *name).collect();
    let mut out = BTreeSet::new();
    for expression in &expressions {
        for (metric, key) in selector_pairs(expression) {
            if !foreign.contains(key.as_str()) {
                out.insert((metric, key));
            }
        }
    }
    out
}

#[test]
fn every_selector_key_the_rules_use_is_one_the_emitter_writes() {
    let root = repo_root();
    let pairs = rule_selector_pairs(&root);
    // 抽取面自检：读空了的时候下面每一条判据都会在空集上假装绿。
    assert!(
        pairs.len() >= MIN_SELECTOR_PAIRS,
        "只从规则里读到 {} 个选择子键，抽取面本身坏了：{pairs:?}",
        pairs.len()
    );
    assert!(
        pairs.contains(&(
            "cogneva_change_dispatch_verdict".to_string(),
            "pool_reading".to_string()
        )),
        "读不到 `cogneva_change_dispatch_verdict{{pool_reading=…}}`：选择子解析坏了"
    );

    let written = written_keys(&root);

    let pinned: BTreeSet<(String, String)> = UNJUDGED
        .iter()
        .map(|(metric, key, _)| (metric.to_string(), key.to_string()))
        .collect();
    let unjudged: BTreeSet<(String, String)> = pairs
        .iter()
        .filter(|(metric, _)| !written.contains_key(metric))
        .cloned()
        .collect();
    let judged: Vec<&(String, String)> = pairs
        .iter()
        .filter(|(metric, _)| written.contains_key(metric))
        .collect();

    let unknown: Vec<&&(String, String)> = judged
        .iter()
        .filter(|(metric, key)| !written[metric].contains(key))
        .collect();
    assert!(
        unknown.is_empty(),
        "规则在这些读数上按一个发点从不写下的键选数（判词与规则里写的顺序是 读数/键）：\
         {unknown:?}。PromQL 不报错——选择子匹配空集，规则在空集上求值，\
         报出来的是「没有告警」，与「一切正常」同形。每个读数这一带实际写下的键：{:?}",
        judged
            .iter()
            .filter(|(metric, key)| !written[metric].contains(key))
            .map(|(metric, key)| format!("{metric}/{key} -> {:?}", written[metric]))
            .collect::<Vec<_>>()
    );

    let newly_unjudged: Vec<&(String, String)> = unjudged
        .iter()
        .filter(|pair| !pinned.contains(*pair))
        .collect();
    assert!(
        newly_unjudged.is_empty(),
        "规则在这些读数上按一个键选数，而这个门读不出它们的发点（{newly_unjudged:?}）。\
         要么把发点写成这门认得的形状，要么在 UNJUDGED 表里指名理由——\
         「判不了」是一条要写下来的决定，不是一次静默的放行"
    );
    let stale: Vec<&(String, String)> = pinned
        .iter()
        .filter(|pair| !unjudged.contains(*pair))
        .collect();
    assert!(
        stale.is_empty(),
        "UNJUDGED 表里这些 (读数, 键) 已经不是判不了的状态了（规则不再选它，或者发点现在读得出来了）：\
         {stale:?}。表里留着一条不成立的行，下一次真判不了时会从这里读出一条错的先例"
    );
}

/// 解析面自己也要有判据，否则「读到空集」与「规则里没有选择子」不可分。
#[test]
fn the_selector_parser_reads_keys_and_ignores_foreign_series() {
    let pairs = selector_pairs(
        "max without (pod, container) \
         (cogneva_change_dispatch_verdict{decision=\"proceed\", pool_reading=\"available\"})",
    );
    assert_eq!(
        pairs.into_iter().collect::<Vec<_>>(),
        vec![
            (
                "cogneva_change_dispatch_verdict".to_string(),
                "decision".to_string()
            ),
            (
                "cogneva_change_dispatch_verdict".to_string(),
                "pool_reading".to_string()
            ),
        ]
    );

    // 别人的 series 不统计：`cogneva_` 前缀才是这一棵树自己的词汇。
    assert!(
        selector_pairs("kube_pod_container_resource_limits{container=\"redis\"}").is_empty(),
        "别人的 series 的选择子不该算进来"
    );

    // 匹配运算符的三种写法都要认，`=~` 不能被读成「没有键」。
    //
    // 标本用的是闭集里真有的一个 series 名，不是编出来的 `cogneva_x`：观测面
    // 那份普查会把「引号后面跟着一个 `cogneva_` 名字」读成「这棵树发布了这个
    // 名字」，一个编造的标本要么让那条门红、要么得给门开一个豁免把这一整类
    // 挡在射程外。用一个真名字，门照常扫，标本也照常说实话——这条断言只看
    // 键集合，名字换成真的不改变它测的东西。
    let regex = selector_pairs(
        "cogneva_loop_tick_age_seconds{outcome=~\"a|b\", phase!~\"c\", zone!=\"d\"}",
    );
    assert_eq!(
        regex.into_iter().map(|(_, key)| key).collect::<Vec<_>>(),
        vec![
            "outcome".to_string(),
            "phase".to_string(),
            "zone".to_string()
        ]
    );
}

/// 判据层在真树上的落点：这套发点写的是 `pool_reading`，不是 `reading`。
///
/// 这条不住在「改坏文件看它红」那种一次性的红证里，因为它要守的恰恰是那个**已经
/// 发生过的**错：同一个 crate 里 `LABEL_READING` 与 `LABEL_POOL_READING` 并存，
/// 规则作者按前者写了后者该用的地方，两边都不报错。断言「这一带的键集合里没有
/// `reading`」把两件事钉住：抽取面确实按读数分开了键，以及谁哪天让这个发点也写
/// `reading`，得先来改掉这一行、说清为什么。
#[test]
fn the_change_verdict_emitter_writes_pool_reading_and_not_reading() {
    let root = repo_root();
    let written = written_keys(&root);
    let keys = written
        .get("cogneva_change_dispatch_verdict")
        .unwrap_or_else(|| panic!("读不到 `cogneva_change_dispatch_verdict` 的发点：{written:?}"));
    assert!(
        keys.contains("pool_reading"),
        "这个读数这一带没有 `pool_reading` 这个键：{keys:?}"
    );
    assert!(
        keys.contains("decision"),
        "这个读数这一带没有 `decision` 这个键：{keys:?}"
    );
    assert!(
        !keys.contains("reading"),
        "这个发点现在也写 `reading` 了——同名的 `LABEL_READING` 是另一族的键，\
         两条序列共用一个键是「按错族选数」这类错的入口：{keys:?}"
    );
}

/// 发点一侧的读法也要有判据：常量换值、别名解析、字面量键，三样都得认。
#[test]
fn the_emitter_reader_resolves_constants_aliases_and_literal_keys() {
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("LABEL_POOL_READING".to_string(), "pool_reading".to_string());
    labels.insert("OUTCOME_LABEL".to_string(), "outcome".to_string());

    let keys = keys_in(
        "labels.insert(crate::LABEL_POOL_READING.to_string(), v);\
         labels.insert(OUTCOME_LABEL.to_string(), o);\
         let map = HashMap::from([(\"side\".to_string(), s)]);\
         metric.with_label(LABEL_DECISION, d)",
        &labels,
    );
    assert!(keys.contains("pool_reading"), "常量没换成值：{keys:?}");
    assert!(keys.contains("outcome"), "另一种常量写法没认：{keys:?}");
    assert!(keys.contains("side"), "字面量键没认：{keys:?}");
    // 声明里没有的常量换成不了值，原样留着：它在判据那一侧匹配不上任何 promql
    // 键，于是不会把一条规则误判成绿。反过来「猜一个值出来」才会。
    assert!(
        keys.contains("LABEL_DECISION"),
        "没有声明的常量应该原样留着，而不是被换成一个猜出来的名字：{keys:?}"
    );
}

/// 关系到一个名字的两种常量写法都认：`LABEL_X` 前缀与 `X_LABEL` 后缀。
#[test]
fn both_constant_spellings_are_read_from_the_tree() {
    let root = repo_root();
    let consts = str_consts(&root);
    assert!(
        consts.len() >= 200,
        "只读到 {} 条字符串常量，常量解析面坏了",
        consts.len()
    );
    assert!(
        consts
            .get("LABEL_POOL_READING")
            .is_some_and(|values| values.contains("pool_reading")),
        "`LABEL_POOL_READING` 没读出来"
    );
    assert!(
        consts.values().any(|values| values.contains("outcome")),
        "`OUTCOME_LABEL` 这一类后缀写法没读出来"
    );
    // 一个名字两个值：`BYTES_METRIC` 在两个 crate 里指两条不同的读数，两个都要留下来。
    assert!(
        consts
            .get("BYTES_METRIC")
            .is_some_and(|values| values.len() >= 2),
        "重名的常量只留下了一个值，另一个 crate 那条读数会因此被判成「读不出出发点」"
    );
}

/// 相对路径只在这里用一次，为了让 `relative_name` 不成为一个未用导入。
#[test]
fn production_sources_are_read_from_this_tree() {
    let root = repo_root();
    let files = production_sources(&root);
    assert!(
        files
            .iter()
            .any(|path| relative_name(&root, path) == "crates/cog-orchestrator/src/lib.rs"),
        "生产源集合里没有 cog-orchestrator 的根文件：读的地方不对"
    );
}
