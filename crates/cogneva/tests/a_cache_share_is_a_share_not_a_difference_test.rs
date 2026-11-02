//! principle: feedback_a_reading_must_not_let_two_different_things_share_one_cell
//! 记账面把两个协议面的缓存口径归一了：`input` 只装**没被缓存服务**的那部分，
//! 整个 prompt = `input + cached`，命中率一律 `cached / (input + cached)`。
//! 归一之后 `cached` 在命中过半时**必然**大过 `input`——那是缓存干得好。
//!
//! 2026-11-01 之前没这条不变式：那时 OpenAI 兼容面上 `cached` 是 `input` 的一部分，
//! 所以「cached 大过 input」确属按构造不可能，一条这样立的规则
//! （`llm_cached_tokens_exceed_input`）是合理的。归一把那个前提拿掉了，却没回头看
//! 它的读者：这条规则从此在**健康**的流量上常亮——11-01 22:17 它就响过一次，命中
//! 越好的小时越容易响。
//!
//! 这份标本判的就是「谁在读这两格、怎么读」：
//!
//! * 读 `llm_tokens_total{kind="cached"}` 的规则，必须在同一段表达式里也读
//!   `kind="input"`——缓存这一格单独读不出任何有含义的量，它只有作为分子才有意义；
//! * 读这两格的规则必须在这张名册上，且写上它拿这两格算什么。名册两个方向都查：
//!   规则没了而这行还在，也是红——否则名册会慢慢变成一张过期的宣称。
//!
//! 它判不出来的：表达式里把两格相减、再从别处加的写法能绕过「有没有 input」这一问。
//! 所以名册里那行不是形式主义——加一条新读者时，写不清它算的是哪一段就会在下一行
//! 判红。这是这份标本能给的最强的一格。

use std::path::Path;

use crate::common;

use common::routes::repo_root;
use common::rules::rule_document;

/// 读 `kind="cached"` 的每条规则：它把缓存那一格算成什么。
///
/// 一行一条，`(规则名, 它算的量)`。加规则要在这里写清楚，不是走过场：这条名册
/// 是「这两格怎么用」的唯一一处白纸黑字，下一份读到它的人先看这里。
const CACHED_READERS: &[(&str, &str)] = &[(
    "llm_cache_hit_share_collapsed",
    "命中率 = cached / (input + cached)，只在分母过二十万 token 时判下界 0.15",
)];

/// 规则库里每一条 `(名字, 表达式)`。
fn named_expressions(value: &serde_json::Value, out: &mut Vec<(String, String)>) {
    match value {
        serde_json::Value::Object(map) => {
            if let (Some(name), Some(promql)) = (
                map.get("name").and_then(|v| v.as_str()),
                map.get("promql").and_then(|v| v.as_str()),
            ) {
                out.push((name.to_string(), promql.to_string()));
                return;
            }
            for child in map.values() {
                named_expressions(child, out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                named_expressions(item, out);
            }
        }
        _ => {}
    }
}

fn cached_readers(root: &Path) -> Vec<(String, String)> {
    let document = rule_document(root);
    let mut all = Vec::new();
    named_expressions(&document, &mut all);
    // 抽取面自检：读空了的库里下面每一条判据都会在空集上假装绿。
    assert!(
        all.len() >= 300,
        "只从规则库里读到 {} 条带 promql 的规则，抽取面本身坏了",
        all.len()
    );
    all.into_iter()
        .filter(|(_, promql)| promql.contains(r#"kind="cached""#))
        .collect()
}

#[test]
fn a_rule_that_reads_the_cached_cell_also_reads_the_input_cell() {
    let root = repo_root();
    for (name, promql) in cached_readers(&root) {
        assert!(
            promql.contains(r#"kind="input""#),
            "规则 {name} 只读了 cached 这一格。归一之后 cached 是「整个 prompt 里由缓存\
             服务的那部分」，单独拿出来既不是命中率也不是总量：要它就得和 input 一起进\
             同一个分母。表达式：{promql}"
        );
    }
}

#[test]
fn every_rule_that_reads_the_cached_cell_says_what_it_computes() {
    let root = repo_root();
    let readers: Vec<String> = cached_readers(&root)
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    let named: Vec<&str> = CACHED_READERS.iter().map(|(n, _)| *n).collect();
    for name in &readers {
        assert!(
            named.contains(&name.as_str()),
            "规则 {name} 读了缓存那一格而不在名册上。加一行 `(名字, 它算的量)`：\
             这一行是下一份读到这两格的人唯一能看到的口径说明。"
        );
    }
    for name in &named {
        assert!(
            readers.iter().any(|r| r == name),
            "名册上的 {name} 已经不再读缓存那一格了，这一行在宣称一件不存在的事。"
        );
    }
    for (name, what) in CACHED_READERS {
        assert!(
            !what.trim().is_empty(),
            "名册上 {name} 那行没写它算的是什么——空白和没写是同一件事。"
        );
    }
}

#[test]
fn the_nested_face_rule_stays_deleted() {
    // 这条规则的名字在归一口径之后只会误报，所以它没有回旋的余地：
    // 它出现的那个名字本身就是「把两个已经互斥的计数当成嵌套关系」。
    let root = repo_root();
    let readers = cached_readers(&root);
    for (name, _) in readers {
        assert_ne!(
            name, "llm_cached_tokens_exceed_input",
            "这条规则的原前提（cached 是 input 的一部分）已经被记账面的归一拿掉了，\
             它会在命中率过半的健康小时上响"
        );
    }
}
