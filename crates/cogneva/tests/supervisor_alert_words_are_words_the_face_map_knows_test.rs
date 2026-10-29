//! principle: feedback_no_hardcoding_config_surface
//!
//! 监督者自己的告警不走规则库：`event_to_alert` 把一条 `SupervisorEvent` 落成一行
//! 告警，主题（`event_type`）在这一侧拼一次；而这一行归哪个面，是另一侧的
//! `supervisor_face(rule_name)` 按同一个词再拼一次。两处各写各的字符串，谁也不读谁的。
//!
//! 漂移的后果一侧亮、一侧不亮：
//!
//! - 写方改了词、映射没跟上 → 这一行落库时拿不到面标签。读数上它只是「缺面」的一条，
//!   和「这个面本来就没有这一类告警」在计数上分不开。
//! - 映射改了词、写方没跟上 → 那条映射永远不会命中。那一面从此收不到这类告警，
//!   而告警本身照旧在响——只是没有归属。
//!
//! 所以两边都读，逐词对。写方是 `alert_store.rs` 里所有 `event_type: "…"` 字面量；
//! 读方是 `plugin.rs` 里 `supervisor_face` 的臂（`None` 兜底不算一条映射）。
//!
//! 实测（2026-10-27）：监督者一共写 6 个主题（`llm_upstream_pool_down` 有两处写点），
//! `supervisor_face` 正好映射这 6 个，两边逐词相等、没有一条落空。
//! 消融：把 `supervisor_face` 的 `"task_dead_letter"` 改成 `"task_deadletter"`，编译通过、
//! 本门两个方向同时红（告警 `task_dead_letter` 没有面 ＋ 映射 `task_deadletter` 永不命中）；
//! 改回绿。
//!
//! 判不了什么：不判这两个文件的**语义**——同名词分到错的面，本门看不出来（那要有
//! 一张「谁该管哪件事」的对照表，树里没有）。也不判别的写入方：网关与规则库的行自带
//! 面标签，不走这张映射。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::lex::Dialect;
use common::routes::{matching_brace, repo_root};
use common::rust_production::{production_text, relative_name};

const SUPERVISOR: &str = "crates/cog-supervisor/src/alert_store.rs";
const FACE_MAP: &str = "crates/cog-observability/src/plugin.rs";
const FACE_FN: &str = "fn supervisor_face";

/// 写方：`event_type: "词".to_string()` 里的那些词。
fn written_words(source: &str) -> Result<BTreeSet<String>, String> {
    let mut words = BTreeSet::new();
    let mut cursor = 0;
    let mut saw_field = false;
    while let Some(offset) = source[cursor..].find("event_type:") {
        saw_field = true;
        let at = cursor + offset + "event_type:".len();
        cursor = at;
        let rest = source[at..].trim_start();
        let Some(literal) = rest.strip_prefix('"') else {
            // 两种不是「写一个词」的形状，放过：字段声明（`event_type: String,`）与
            // 原样转抄（`event_type: a.event_type,`，词还是别处写的那一个）。其余一律
            // 按红处理——算出来的词读不出来，静默少算一个比误报更糟。
            let head: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
                .collect();
            if matches!(head.as_str(), "String" | "str" | "Option") || head.ends_with(".event_type")
            {
                continue;
            }
            return Err(format!(
                "  `{SUPERVISOR}` 里有一处 `event_type:` 后面是 `{head}`，不是字面量也不是转抄：\
                 读不出来按红处理"
            ));
        };
        let Some(end) = literal.find('"') else {
            return Err(format!(
                "  `{SUPERVISOR}` 里有一处字面量没有收尾：读不出来按红处理"
            ));
        };
        let word = &literal[..end];
        if word.is_empty() || word.chars().any(char::is_whitespace) {
            return Err(format!(
                "  `{SUPERVISOR}` 里写出去的告警主题 `{word}` 不像一个词：读不出来按红处理"
            ));
        }
        words.insert(word.to_string());
    }
    if !saw_field {
        return Err(format!(
            "  `{SUPERVISOR}` 里一处 `event_type:` 都没有：写方没读到，本门判不了这一棵树"
        ));
    }
    Ok(words)
}

/// 读方：`supervisor_face` 的臂，词 → 面。`None` 兜底臂不算映射。
fn mapped_words(source: &str) -> Result<BTreeMap<String, String>, String> {
    let Some(start) = source.find(FACE_FN) else {
        return Err(format!(
            "  `{FACE_MAP}` 里找不到 `{FACE_FN}`：读方没读到，本门判不了这一棵树"
        ));
    };
    let Some(open) = source[start..].find('{').map(|o| start + o) else {
        return Err(format!(
            "  `{FACE_MAP}` 的 `{FACE_FN}` 没有函数体：读不出来按红处理"
        ));
    };
    let close = matching_brace(source, open, Dialect::Rust)
        .ok_or_else(|| format!("  `{FACE_MAP}` 的 `{FACE_FN}` 括号不闭合：读不出来按红处理"))?;
    let body = &source[open + 1..close];
    let mut mapped = BTreeMap::new();
    for arm in body.split(',') {
        let Some(arrow) = arm.find("=>") else {
            continue;
        };
        let Some(face_start) = arm[arrow..].find("Some(\"") else {
            // `_ => None` 兜底：没有面，不是一条映射。
            continue;
        };
        let tail = &arm[arrow + face_start + "Some(\"".len()..];
        let Some(face_end) = tail.find('"') else {
            return Err(format!(
                "  `{FACE_MAP}` 的 `{FACE_FN}` 里有一处面名没有收尾"
            ));
        };
        let face = &tail[..face_end];
        for word in literals(&arm[..arrow]) {
            mapped.insert(word, face.to_string());
        }
    }
    if mapped.is_empty() {
        return Err(format!(
            "  `{FACE_MAP}` 的 `{FACE_FN}` 里一条 `词 => Some(面)` 都读不出来：读方没读到"
        ));
    }
    Ok(mapped)
}

/// 一段文字里的字符串字面量。
fn literals(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        let mut word = String::new();
        while j < bytes.len() && bytes[j] != b'"' {
            if bytes[j] == b'\\' && j + 1 < bytes.len() {
                j += 1;
            }
            word.push(bytes[j] as char);
            j += 1;
        }
        out.push(word);
        i = j + 1;
    }
    out
}

fn contradictions(root: &Path) -> Vec<String> {
    let mut complaints = Vec::new();
    let supervisor_path = root.join(SUPERVISOR);
    let face_path = root.join(FACE_MAP);
    let written = match std::fs::read_to_string(&supervisor_path) {
        Ok(raw) => match written_words(&production_text(&raw)) {
            Ok(words) => words,
            Err(complaint) => {
                complaints.push(complaint);
                BTreeSet::new()
            }
        },
        Err(_) => {
            complaints.push(format!(
                "  `{}` 读不出来：读不出来按红处理",
                relative_name(root, &supervisor_path)
            ));
            BTreeSet::new()
        }
    };
    let mapped = match std::fs::read_to_string(&face_path) {
        Ok(raw) => match mapped_words(&production_text(&raw)) {
            Ok(map) => map,
            Err(complaint) => {
                complaints.push(complaint);
                BTreeMap::new()
            }
        },
        Err(_) => {
            complaints.push(format!(
                "  `{}` 读不出来：读不出来按红处理",
                relative_name(root, &face_path)
            ));
            BTreeMap::new()
        }
    };

    for word in &written {
        if !mapped.contains_key(word) {
            complaints.push(format!(
                "  监督者把一条告警写成 `{word}`，而 `{FACE_MAP}` 的 `{FACE_FN}` 不认这个词：\
                 这一行落库时没有面，读数只能当「缺面」看见——与「这个面本来就没有这一类\
                 告警」同一个形状"
            ));
        }
    }
    for word in mapped.keys() {
        if !written.contains(word) {
            complaints.push(format!(
                "  `{FACE_MAP}` 的 `{FACE_FN}` 给 `{word}` 分了一个面，可监督者一处都不写这个词：\
                 这条映射永远不会命中，那个面从此收不到这一类告警，而告警本身照旧在响"
            ));
        }
    }

    if written.len() < 4 || mapped.len() < 4 {
        complaints.push(format!(
            "普查只读到 {} 个监督者写出去的告警主题、{} 条面映射：读得太少，本门判不了这一棵树，\
             按红处理",
            written.len(),
            mapped.len()
        ));
    }
    complaints
}

#[test]
fn every_supervisor_alert_word_is_a_word_the_face_map_knows() {
    let complaints = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "监督者写出去的告警主题必须能被 `supervisor_face` 分到一个面；两边各拼各的词，\
         编译器不比较，改一边不会有任何报错。\n{}",
        complaints.join("\n")
    );
}

// ── 夹具：临时树里放这两份文件，逐条验证红与绿 ──

const SUPERVISOR_FIXTURE: &str = "\
fn event_to_alert(event: &SupervisorEvent) -> Option<Alert> {
    match event {
        SupervisorEvent::AgentUnhealthy { .. } => Some(Alert {
            event_type: \"agent_unhealthy\".to_string(),
            message: \"dead\".to_string(),
        }),
        SupervisorEvent::QuotaThresholdBreached { .. } => Some(Alert {
            event_type: \"quota_threshold_breached\".to_string(),
            message: \"quota\".to_string(),
        }),
        _ => None,
    }
}
";

const FACE_FIXTURE: &str = "\
fn supervisor_face(rule_name: &str) -> Option<&'static str> {
    match rule_name {
        \"agent_unhealthy\" => Some(\"任务管理\"),
        \"quota_threshold_breached\" => Some(\"token 管理\"),
        _ => None,
    }
}
";

fn fixture_complaints(supervisor: &str, face: &str) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    for (path, body) in [(SUPERVISOR, supervisor), (FACE_MAP, face)] {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
        std::fs::write(full, body).expect("write");
    }
    contradictions(dir.path())
        .into_iter()
        .filter(|c| !c.contains("普查只读到"))
        .collect()
}

#[test]
fn the_fixture_is_green_before_anything_is_changed() {
    assert_eq!(
        fixture_complaints(SUPERVISOR_FIXTURE, FACE_FIXTURE),
        Vec::<String>::new()
    );
}

#[test]
fn a_word_the_face_map_does_not_know_is_reported() {
    let supervisor = SUPERVISOR_FIXTURE.replace("agent_unhealthy", "agent_unwell");
    let complaints = fixture_complaints(&supervisor, FACE_FIXTURE);
    assert!(
        complaints.iter().any(|c| c.contains("agent_unwell")),
        "{complaints:?}"
    );
}

#[test]
fn a_face_no_word_reaches_is_reported() {
    let face = FACE_FIXTURE.replace("quota_threshold_breached", "quota_breached");
    let complaints = fixture_complaints(SUPERVISOR_FIXTURE, &face);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("quota_breached") && c.contains("永远不会命中")),
        "{complaints:?}"
    );
}

#[test]
fn an_unreadable_face_map_is_reported() {
    let face = FACE_FIXTURE.replace("fn supervisor_face", "fn some_other_map");
    let complaints = fixture_complaints(SUPERVISOR_FIXTURE, &face);
    assert!(
        complaints.iter().any(|c| c.contains("supervisor_face")),
        "{complaints:?}"
    );
}

#[test]
fn a_write_side_that_is_not_a_literal_is_reported() {
    let supervisor = SUPERVISOR_FIXTURE.replace(
        "event_type: \"agent_unhealthy\".to_string()",
        "event_type: rule.to_string()",
    );
    let complaints = fixture_complaints(&supervisor, FACE_FIXTURE);
    assert!(
        complaints.iter().any(|c| c.contains("不是字面量")),
        "{complaints:?}"
    );
}

/// 普查本身也要有下限：写方与读方都读空时，「没有矛盾」是个空承诺。
#[test]
fn a_tree_that_says_nothing_is_not_green() {
    let dir = tempfile::tempdir().expect("temp dir");
    for (path, body) in [(SUPERVISOR, SUPERVISOR_FIXTURE), (FACE_MAP, FACE_FIXTURE)] {
        let full: PathBuf = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
        std::fs::write(full, body).expect("write");
    }
    let complaints = contradictions(dir.path());
    assert!(
        complaints.iter().any(|c| c.contains("普查只读到")),
        "{complaints:?}"
    );
}
