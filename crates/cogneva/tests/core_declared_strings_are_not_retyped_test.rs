//! principle: feedback_no_hardcoding_config_surface
//! `cog_core` is where a string that more than one crate has to agree on is
//! declared: a route, an env name, a config pointer, a metric, a header, a
//! component. Declaring it there is only half the job — the other half is that
//! nobody types it again. A second copy does not fail when the first one
//! changes: the reader of the old string keeps looking for something that is no
//! longer written, and every one of those readers answers an absence the same
//! way it answers "nothing happened".
//!
//! So the values are read out of the declaration crate at test time and every
//! other crate's production code is searched for them. A site that legitimately
//! names a *different* thing with the same spelling is listed below with the
//! reason; a listed site that stops existing is reported too, because an
//! exemption outliving its site is a licence nobody reviewed.
//!
//! 实测（2026-10-27）：声明侧 137 个带分隔符的字符串；加固前全树有 22 处
//! 重写，本轮把其中 6 处改回读声明，其余按「同形不同物」逐条登记在 EXEMPT。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::rust_production::production_lines;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Where the shared strings are declared. Not searched for retypes: this is the
/// one place they are supposed to be written.
const DECLARATION_DIR: &str = "crates/cog-core/src";

/// One production-code line that spells a declared value.
#[derive(Debug, PartialEq, Eq)]
struct Hit {
    file: String,
    value: String,
    line: String,
}

/// A site that spells a declared value for a reason of its own.
struct Exempt {
    file: &'static str,
    value: &'static str,
    why: &'static str,
}

/// Same spelling, different thing.
///
/// Each of these names something that is not the declared string: a field of a
/// structured payload, a container name inside a pod, a command a person types.
/// Reading the declaration here would be a coupling nobody asked for, which is
/// why they are written down rather than quietly skipped.
const EXEMPT: &[Exempt] = &[
    Exempt {
        file: "crates/cog-agent/src/agent_kernel/runtime.rs",
        value: "guardrail_blocked",
        why: "错误载荷里的布尔字段名与错误分类标签，不是拼进 reason 文本的那个前缀；\
              理由前缀由 cog_core::contract::outcome 自己读",
    },
    Exempt {
        file: "crates/cog-collaboration/src/observable.rs",
        value: "upstream_unavailable",
        why: "自审跳过原因这一族自己的取值，与失败原因前缀同形但由跳过面消费",
    },
    Exempt {
        file: "crates/cog-gateway/src/llm_admin.rs",
        value: "security-gateway",
        why: "写回给向导的出处字段，说明这份配置是谁落的盘",
    },
    Exempt {
        file: "crates/cog-reflection/src/config.rs",
        value: "security-gateway",
        why: "Pod 里那个容器的名字，不是工作负载的组件标签",
    },
    Exempt {
        file: "crates/cog-reflection/src/config.rs",
        value: "sandbox-executor",
        why: "同上：容器名",
    },
    Exempt {
        file: "crates/cogneva/src/cli.rs",
        value: "security-gateway",
        why: "命令行子命令名，是人敲的字符串，跟着标签改名会把脚本打断",
    },
    Exempt {
        file: "crates/cogneva/src/cli.rs",
        value: "sandbox-executor",
        why: "同上：子命令名",
    },
    Exempt {
        file: "crates/cogneva/src/backup.rs",
        value: "cluster-registry",
        why: "换机损失清单里的一行，写的是这一行的主题而不是选择器",
    },
];

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// A value worth holding to one spelling: long enough and shaped like a path,
/// an env name, a metric, a header or a label rather than a word two unrelated
/// call sites may both use.
fn structural(value: &str) -> bool {
    value.len() >= 8
        && value
            .chars()
            .any(|c| matches!(c, '/' | ':' | '.' | '_' | '-'))
}

/// The `pub const NAME: &str = "value"` declarations in the contract crate.
fn declared(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut files = Vec::new();
    walk(&root.join(DECLARATION_DIR), &mut files);
    files.sort();
    let mut values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for line in text.lines() {
            let Some(at) = line.find("pub const ") else {
                continue;
            };
            let rest = &line[at + "pub const ".len()..];
            let Some((name, tail)) = rest.split_once(':') else {
                continue;
            };
            if !tail.trim_start().starts_with("&str = \"") {
                continue;
            }
            let Some(start) = tail.find('"') else {
                continue;
            };
            let Some(end) = tail[start + 1..].find('"') else {
                continue;
            };
            let value = &tail[start + 1..start + 1 + end];
            if !structural(value) {
                continue;
            }
            values
                .entry(value.to_string())
                .or_default()
                .insert(name.trim().to_string());
        }
    }
    values
}

/// One line's retyped value, if it spells one as a whole string literal.
///
/// A whole literal rather than a substring: `"deploy/helm/cogneva/files/cogneva.json"`
/// is a path that contains the declared document name, and it is not a second
/// copy of it.
fn retyped_value<'a>(line: &str, quoted: &'a [(&'a String, String)]) -> Option<&'a String> {
    quoted
        .iter()
        .find(|(_, literal)| line.contains(literal.as_str()))
        .map(|(value, _)| *value)
}

/// Every production line in a crate outside the declaration one that spells one
/// of these values as a whole string literal.
fn retypes(root: &Path, values: &BTreeMap<String, BTreeSet<String>>) -> Vec<Hit> {
    let quoted: Vec<(&String, String)> = values
        .keys()
        .map(|value| (value, format!("\"{value}\"")))
        .collect();
    let mut crates: Vec<PathBuf> = std::fs::read_dir(root.join("crates"))
        .expect("crates/ 不在仓库里")
        .flatten()
        .map(|entry| entry.path())
        .collect();
    crates.sort();
    let mut hits = Vec::new();
    for krate in crates {
        let src = krate.join("src");
        if !src.is_dir() {
            continue;
        }
        let mut files = Vec::new();
        walk(&src, &mut files);
        files.sort();
        for path in files {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let file = relative(root, &path);
            if file.starts_with(DECLARATION_DIR) {
                continue;
            }
            for line in production_lines(&text) {
                if let Some(value) = retyped_value(line, &quoted) {
                    hits.push(Hit {
                        file: file.clone(),
                        value: value.clone(),
                        line: line.trim().to_string(),
                    });
                }
            }
        }
    }
    hits
}

fn exempted(hit: &Hit) -> Option<&'static Exempt> {
    EXEMPT
        .iter()
        .find(|rule| rule.file == hit.file && rule.value == hit.value)
}

#[test]
fn every_shared_string_is_spelled_in_the_place_it_is_declared() {
    let root = repo_root();
    let values = declared(&root);
    assert!(
        values.len() > 100,
        "只读出 {} 个声明过的字符串，这条判据没有题目",
        values.len()
    );

    let hits = retypes(&root, &values);
    let unexpected: Vec<&Hit> = hits.iter().filter(|hit| exempted(hit).is_none()).collect();
    assert!(
        unexpected.is_empty(),
        "这些地方重写了一遍 cog_core 里声明过的字符串。只改一处时不会报错：\
         读旧字符串的那一侧照旧去找一个再也不出现的东西，而「没找到」与\
         「本来就没有」在读数上同形。要么改成读声明，要么在 EXEMPT 里写清为什么\
         这里的同一个拼写指的是另一件事：\n{}",
        unexpected
            .iter()
            .map(|hit| format!("  {}: {}  <- {}", hit.file, hit.value, hit.line))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn an_exemption_that_no_longer_matches_a_site_is_stale() {
    let root = repo_root();
    let values = declared(&root);
    let hits = retypes(&root, &values);
    let stale: Vec<&Exempt> = EXEMPT
        .iter()
        .filter(|rule| {
            !hits
                .iter()
                .any(|hit| hit.file == rule.file && hit.value == rule.value)
        })
        .collect();
    assert!(
        stale.is_empty(),
        "这些豁免已经不指向任何一处重写了：{}。豁免是给一处现场发的许可，\
         现场没了许可就该收回——留着它，下一次有人在这条路径上重写同一个字符串时\
         就没有判据拦了。",
        stale
            .iter()
            .map(|rule| format!("\n  {}: {}（{}）", rule.file, rule.value, rule.why))
            .collect::<Vec<_>>()
            .join("")
    );
}

#[test]
fn the_census_reddens_on_a_retyped_literal_and_ignores_comments_and_fixtures() {
    let values: BTreeMap<String, BTreeSet<String>> = [(
        "/api/v1/things".to_string(),
        BTreeSet::from(["THINGS_PATH".to_string()]),
    )]
    .into_iter()
    .collect();
    let quoted: Vec<(&String, String)> = values
        .keys()
        .map(|value| (value, format!("\"{value}\"")))
        .collect();

    // 一行真代码用字面量重写、一行在注释里提到、一行在测试夹具里重写。
    let text = "\
pub fn route() -> &'static str { \"/api/v1/things\" }
// \"/api/v1/things\"
#[cfg(test)]
mod tests { fn t() -> &'static str { \"/api/v1/things\" } }
";
    let hits: Vec<&String> = production_lines(text)
        .into_iter()
        .filter_map(|line| retyped_value(line, &quoted))
        .collect();
    assert_eq!(
        hits,
        vec![&"/api/v1/things".to_string()],
        "只该留下那一行真代码"
    );

    // 更长路径里的同一个名字不是重写：那是另一个字符串。
    assert_eq!(
        retyped_value(
            "const P: &str = \"deploy/helm/cogneva/files/cogneva.json\";",
            &[(&"cogneva.json".to_string(), "\"cogneva.json\"".to_string())]
        ),
        None
    );

    // 带分隔符的长字符串才算共用值：一个词在两个无关调用点各写一遍不算漂移。
    assert!(structural("/api/v1/things"));
    assert!(!structural("agent"));
    assert!(!structural("/v1/x"));
}
