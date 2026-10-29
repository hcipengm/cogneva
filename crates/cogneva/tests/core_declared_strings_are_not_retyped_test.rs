//! principle: feedback_no_hardcoding_config_surface
//! `cog_core` is where a string that more than one crate has to agree on is
//! declared: a route, an env name, a config pointer, a metric, a header, a
//! component. Declaring it there is only half the job — the other half is that
//! nobody types it again. A second copy does not fail when the first one
//! changes: the reader of the old string keeps looking for something that is no
//! longer written, and every one of those readers answers an absence the same
//! way it answers "nothing happened".
//!
//! So the values are read out of the declaring crates at test time and every
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

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources, relative_name};

/// Where the shared strings are declared.
///
/// The contract crate is the usual home, but not the only one: a crate that
/// owns a format gives it one declaration and lets everyone else read it from
/// there — the day key a rotating log file is named with is declared in the
/// storage crate, beside the writer and the parser that had to agree on it.
/// Any crate that declares a value is skipped *for that value only*: it is the
/// owner, so it may write the word; the rest of the workspace may not.
const DECLARATION_DIRS: &[&str] = &["crates/cog-core/src", "crates/cog-storage/src"];

/// The crate a repo-relative path belongs to (`crates/cog-core/src/x.rs` ->
/// `crates/cog-core`).
fn owning_crate(file: &str) -> &str {
    match file.split_once('/') {
        Some((_, rest)) => match rest.split_once('/') {
            Some((krate, _)) => {
                let end = "crates/".len() + krate.len();
                &file[..end]
            }
            None => file,
        },
        None => file,
    }
}

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

/// Same spelling, mostly a different thing.
///
/// Most of these name something that is not the declared string: a field of a
/// structured payload, a container name inside a pod, a command a person types.
/// Reading the declaration there would be a coupling nobody asked for, which is
/// why they are written down rather than quietly skipped.
///
/// The build-time names are the exception: same thing, written by a program
/// that cannot read the declaration. A build script and the compile that reads
/// its `env!` output are two programs; linking the contract crate into either
/// would put its whole dependency tree in front of every build. The spelling is
/// still held — the env-name census pins each of those files by name — so a
/// rename turns both gates red rather than one.
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
    Exempt {
        file: "crates/cog-gateway/src/security_gateway.rs",
        value: "%Y-%m-%d",
        why: "解析调用方送进来的时间戳，是接受两种外部写法，不是我们自己写出去的名字",
    },
    Exempt {
        file: "crates/cog-wiki/src/llm_maintainer.rs",
        value: "%Y-%m-%d",
        why: "给人看的页面日期，读它的是人不是解析器：改格式不产生「找不到」",
    },
    Exempt {
        file: "crates/cog-gateway/src/backup_admin.rs",
        value: "cogneva-backup-",
        why: "导出 Job 的对象名（`cogneva-backup-export-<毫秒>`），不是包文件名；\
              它由 apiserver 收下，没有第二个读者去按这个名字找东西",
    },
    Exempt {
        file: "crates/cogneva/src/backup_crypto.rs",
        value: "cogneva-backup-",
        why: "HKDF 的 info 域分隔串，已经烤进盘上每一个包的密钥派生里；\
              它不是包名，跟着包名一起改会让已有的包再也解不开",
    },
    Exempt {
        file: "crates/cogneva/src/work.rs",
        value: "external-worker",
        why: "`HOSTNAME` 读不到时给单元起的显示名，不是注册用的角色字段；\
              那条构造里角色读的是常量，显示名改了只换一个标签",
    },
    Exempt {
        file: "crates/cog-llm/src/model.rs",
        value: "openai-responses",
        why: "`ApiType::OpenAIResponses` 的序列化词，写它的是 `#[serde(rename = ...)]` 属性——\
              那个位置只接受字面量。它与 `cog_core::contract::llm::RESPONSES_API_STYLE` 同物，\
              其余读写点（`plugin.rs` 的分派与 `provider/responses.rs` 的报文标签）已改回读\
              声明，只剩这一处拼写；两者分家由 `model.rs` 的 \
              `the_responses_serialisation_word_is_the_declared_style` 判红",
    },
    Exempt {
        file: "crates/cogneva/build.rs",
        value: "COGNEVA_GIT_REVISION",
        why: "构建期名字的写这一侧：构建脚本是宿主程序，链接 cog-core 会把整棵契约\
              依赖树编到每一次构建前面。名字在这里声明一次、经 `cargo:rustc-env`\
              注入，读回它的是 build_info.rs 那两处 `env!` 字面量——三处任一改名，\
              这两条豁免与构建期名字的普查同时过期",
    },
    Exempt {
        file: "crates/cogneva/build.rs",
        value: "COGNEVA_VERSION_ID",
        why: "同上：构建脚本注入、编译期读回，两个程序各自需要这个名字",
    },
    Exempt {
        file: "crates/cogneva/src/build_info.rs",
        value: "COGNEVA_GIT_REVISION",
        why: "构建期名字：`env!` 只接受字面量，读它的是编译器而不是运行期，\
              所以这一处必须自己拼一遍；写它的是 `build.rs`（另一个程序），\
              把名字换成读 `cog_core::env` 的常量在这里编译不过",
    },
    Exempt {
        file: "crates/cogneva/src/build_info.rs",
        value: "COGNEVA_VERSION_ID",
        why: "同上：`env!` 的字面量要求",
    },
];

/// A value worth holding to one spelling: long enough and shaped like a path,
/// an env name, a metric, a header or a label rather than a word two unrelated
/// call sites may both use.
fn structural(value: &str) -> bool {
    value.len() >= 8
        && value
            .chars()
            .any(|c| matches!(c, '/' | ':' | '.' | '_' | '-'))
}

/// One `pub const NAME: &str = "value"` in a declaring crate, with the file
/// that writes it.
struct Declaration {
    file: String,
    name: String,
    value: String,
}

/// The `pub const NAME: &str = "value"` declarations in the declaring crates.
fn declarations(root: &Path) -> Vec<Declaration> {
    let mut files: Vec<PathBuf> = production_sources(root)
        .into_iter()
        .filter(|path| {
            DECLARATION_DIRS
                .iter()
                .any(|dir| path.starts_with(root.join(dir)))
        })
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "声明目录 {DECLARATION_DIRS:?} 下一个源文件都没有：这些值没人声明了，本门没有题目"
    );
    let mut out = Vec::new();
    for path in files {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, &path);
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
            out.push(Declaration {
                file: file.clone(),
                name: name.trim().to_string(),
                value: tail[start + 1..start + 1 + end].to_string(),
            });
        }
    }
    out
}

/// The declared values, under every name they are declared as.
fn declared(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for declaration in declarations(root) {
        if !structural(&declaration.value) {
            continue;
        }
        values
            .entry(declaration.value)
            .or_default()
            .insert(declaration.name);
    }
    values
}

/// A name that declares an in-band marker: a wire word a producer *prepends* to
/// a reason rather than one it writes on its own.
///
/// These need their own census. The one above looks for the value as a whole
/// literal, which is the right question for a route or a label; a marker at its
/// producer is always followed by the explanation it labels, so a producer that
/// retyped it never shows up there.
fn marker_name(name: &str) -> bool {
    name.ends_with("_PREFIX") || name.ends_with("_MARKER")
}

/// The string literals on a line, without their quotes.
fn literals(line: &str) -> Vec<&str> {
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
        while j < bytes.len() && bytes[j] != b'"' {
            if bytes[j] == b'\\' {
                j += 1;
            }
            j += 1;
        }
        let end = j.min(bytes.len());
        out.push(&line[start..end]);
        i = end + 1;
    }
    out
}

/// Lines that write a declared marker as the head of a literal of their own.
///
/// The declaring file itself is skipped -- it is where the value is supposed to
/// be written -- but the rest of the declaring crate is not: a marker
/// declared in the contract module is still retyped when the error that carries
/// it spells the word again.
fn marker_hits(root: &Path) -> Vec<Hit> {
    let markers: Vec<(String, String)> = declarations(root)
        .into_iter()
        .filter(|declaration| marker_name(&declaration.name) && structural(&declaration.value))
        .map(|declaration| (declaration.file, declaration.value))
        .collect();
    let mut hits = Vec::new();
    for path in production_sources(root) {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, &path);
        for line in production_lines(&text) {
            let written = literals(line);
            for (declared_in, marker) in &markers {
                if &file == declared_in {
                    continue;
                }
                if written
                    .iter()
                    .any(|literal| literal.starts_with(marker.as_str()))
                {
                    hits.push(Hit {
                        file: file.clone(),
                        value: marker.clone(),
                        line: line.trim().to_string(),
                    });
                }
            }
        }
    }
    hits
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

/// Every production line outside a value's declaring crate that spells that
/// value as a whole string literal.
fn retypes(root: &Path, values: &BTreeMap<String, BTreeSet<String>>) -> Vec<Hit> {
    let quoted: Vec<(&String, String)> = values
        .keys()
        .map(|value| (value, format!("\"{value}\"")))
        .collect();
    // The crate that owns each value, so that it may spell its own word while
    // every other crate has to read it.
    let mut declaring: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for declaration in declarations(root) {
        if !values.contains_key(&declaration.value) {
            continue;
        }
        declaring
            .entry(declaration.value.clone())
            .or_default()
            .insert(owning_crate(&declaration.file).to_string());
    }

    let mut hits = Vec::new();
    for path in production_sources(root) {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, &path);
        let krate_dir = owning_crate(&file);
        for line in production_lines(&text) {
            if let Some(value) = retyped_value(line, &quoted) {
                let owned_here = declaring
                    .get(value)
                    .is_some_and(|crates| crates.contains(krate_dir));
                if owned_here {
                    continue;
                }
                hits.push(Hit {
                    file: file.clone(),
                    value: value.clone(),
                    line: line.trim().to_string(),
                });
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
        "这些地方重写了一遍声明过的字符串。只改一处时不会报错：\
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

/// 带内标记是**拼在解释前面**发出去的那个词：`"terminal_env_failure: 没有产物"`。
/// 上一条判据按整字面量比对，天然看不见这种形状——重写过标记的生产者在那条判据
/// 下与被改写之前完全一样。这一条按「字面量以声明开头」比对，并且把声明所在的
/// crate 也算进来，只跳过写出这条声明的那个文件：`SFError::Blocked` 的 Display
/// 就在同一个 crate 的另一个文件里，它写的正是 `is_deterministic_failure` 要读
/// 的那个前缀，两处各自写死时谁都不会报错。
#[test]
fn every_in_band_marker_is_written_from_its_declaration() {
    let root = repo_root();
    let known: Vec<String> = declarations(&root)
        .into_iter()
        .filter(|declaration| marker_name(&declaration.name) && structural(&declaration.value))
        .map(|declaration| declaration.value)
        .collect();
    assert!(
        known.len() >= 4,
        "只读出 {known:?} 这几个带内标记；这么少说明读声明的解析变了，不是树变干净了"
    );

    let hits = marker_hits(&root);
    let unexpected: Vec<&Hit> = hits.iter().filter(|hit| exempted(hit).is_none()).collect();
    assert!(
        unexpected.is_empty(),
        "这些地方把带内标记写成了一个字面量的开头。生产者与读取者各写一遍时，\
         改一处另一处照旧编译：拒绝被当成可重试的故障、终止性失败被排进重试，\
         两种都读作「什么都没有失败」。要么用声明拼出这条文本，要么在 EXEMPT 里\
         写清为什么这里的同一个拼写指的是另一件事：\n{}",
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
    // 两种判据都认：豁免可以发给一处整字面量的重写，也可以发给一处在自己拼出
    // 来的文本里以声明开头的写法。只看前一种，后一种的豁免会立刻被报成过期。
    let mut hits = retypes(&root, &values);
    hits.extend(marker_hits(&root));
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

/// 对照：字面量扫描器确实读得出引号之间的文本。少了这条，`literals` 哪天退化
/// 成「什么都读不出来」，带内标记那条判据仍然全绿。
#[test]
fn the_literal_scanner_reads_the_text_between_quotes() {
    assert_eq!(literals(r#"let s = "marker: why";"#), vec!["marker: why"]);
    assert_eq!(literals(r#"let s = "a\"b";"#), vec![r#"a\"b"#]);
    assert_eq!(literals(r#"let s = r"raw";"#), vec!["raw"]);
    assert!(literals("let s = 1;").is_empty());
    assert!(literals(r#"let s = "marker: why";"#)[0].starts_with("marker"));
}

/// 声明 crate 里同一个值被声明成两个名字时，这两个名字各自代表什么。
///
/// 只有两种可能：它们说的是同一件东西（那就该只留一处，另一处改成读它），或者
/// 只是碰巧同形（登记理由）。无论哪种，重复本身都得有人看过一眼——没人登记时，
/// 改其中一个名字不需要动另一个，而两次「成功」的改动加在一起，就是同一个文件
/// 在部署里叫两个名字。
const SAME_WORD_DIFFERENT_THING: &[(&str, &str)] = &[
    (
        "cogneva",
        "二进制名、默认命名空间、主容器名、主 Deployment 名与工作负载名是五个不同的\
         东西：改二进制名不该牵动命名空间，改容器名不该牵动 Deployment",
    ),
    (
        "face",
        "任务类型（这一行怎么被认出来）与载荷键（这一行把内容放哪）是两个通道，\
         读写两侧各读同一个声明即可；取值相同是设计使然，合并不带来约束",
    ),
    ("handover", "同上：任务类型与载荷键是两个通道，不是同一个词"),
    ("decision", "同上：任务类型与载荷键是两个通道，不是同一个词"),
    ("vote", "同上：任务类型与载荷键是两个通道，不是同一个词"),
    (
        "decision_event",
        "同上：任务类型与载荷键是两个通道，不是同一个词",
    ),
    (
        "empty",
        "两族读数各自的「空」：检索结果落在 empty 格，收件箱孤儿状态也落在 empty 格。\
         它们是两个指标家族里的标签值，谁也不读对方的",
    ),
];

/// 一个值在声明 crate 里只有一个名字。
///
/// 跨 crate 的判据只看得见「别的 crate 重写了这个名字」，看不见「声明 crate
/// 自己写了两遍」：`cogneva.json` 曾经同时是配置文件名、ConfigMap 的键与项目
/// 声明的文档名，三处各自写死的字面量谁也拦不住谁——改名可以只成功一处，而
/// 读那份文件的一侧照旧去找一个再也不出现的名字。
#[test]
fn a_value_has_one_name_inside_the_declaration_crate() {
    let root = repo_root();
    let mut names: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for declaration in declarations(&root) {
        names
            .entry(declaration.value)
            .or_default()
            .insert(declaration.name);
    }

    let unregistered: Vec<String> = names
        .iter()
        .filter(|(value, declared)| {
            declared.len() > 1
                && !SAME_WORD_DIFFERENT_THING
                    .iter()
                    .any(|(same, _)| *same == value.as_str())
        })
        .map(|(value, declared)| {
            format!(
                "  {value:?} 声明成了 {} 个名字：{}",
                declared.len(),
                declared.iter().cloned().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    assert!(
        unregistered.is_empty(),
        "这些值在声明 crate 里各写了不止一遍。留下重复，改一处不会让另一处红：\n{}\n\
         要么让其中一处读另一处，要么在 SAME_WORD_DIFFERENT_THING 里写清为什么\
         它们同形却不是同一件东西：",
        unregistered.join("\n")
    );

    let stale: Vec<&str> = SAME_WORD_DIFFERENT_THING
        .iter()
        .filter(|(value, _)| names.get(*value).is_none_or(|declared| declared.len() < 2))
        .map(|(value, _)| *value)
        .collect();
    assert!(
        stale.is_empty(),
        "这些登记已经不再对应任何重复：{stale:?}。登记是给一处现场发的许可，\
         现场没了许可就该收回——留着它，下一次有人真的写重了同一个值就没有判据拦了。"
    );
}
