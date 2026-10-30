//! principle: feedback_no_hardcoding_config_surface
//!
//! 运维页拿字面量和变量比，比的是某个别处写出来的词：网关发的健康词、组件状态词、
//! git 身份的 `IdentityState`、贡献策略、事件标签……每拼一个词，就是把这个词的
//! 拼法在页面里再写一遍。拼错了不报错，只落到 `else` 那一支：健康提示永远是黄的、
//! 拓扑节点永远是灰的、策略按钮永远不高亮、弹窗消息永远对不上——都读作「本来如此」。
//!
//! 这一棵树上已有几道门各自守着自己那一族（事件名、进化状态、种类、模型事件标签）。
//! 本门守的是剩下的那一大片：**页面里每一个与字面量比较的词（含 `switch` 的
//! `case` 标签）都必须登记，并说明谁写它**。三种登记：
//!
//! - `Writer(文件, 那一段字节)`：服务端（或仓库里另一个页面）写这个词的地方。
//!   那段字节必须还在——服务端改名而页面没跟上时，红在登记行上；
//! - `Gate(门文件)`：整族词由另一道门判，这里只登记「有人管」，那道门不在了也红；
//! - `PageOnly(为什么)`：页面自己写自己读的词（键盘、语言、DOM 样式），给理由。
//!
//! 三个方向都判：页面上出现一个没登记的词 → 红（新硬编码不许悄悄进来）；登记过的
//! 词页面不再比 → 红（登记烂在这里就没人发现）；登记的服务端那段字节不在了 → 红。
//!
//! 判不了什么：同一个拼法在不同上下文可能各有写者（`error` 既是服务端状态词、也是
//! 页面自己的连接状态），本门只要求这个词至少有一个写者、且登记过的写者都还在，
//! 不区分「这一处比较该归谁」；只判这一个页面，WebUI 有它自己的登记门；页面用
//! 字符串拼出来、从来不与字面量比较的词（`t('…')` 的键、模板串）不判。
//!
//! 实测（2026-10-27）：页面 48 处字面量比较（39 个不同的词）＋ 17 个 `case` 标签，
//! 共 56 个词，登记 29 组覆盖全部。消融：在真树页面的 `componentToStatus` 里加一处
//! `s === 'healthy_v2'`，本门红并点名那个词；删掉即绿。

use std::collections::BTreeSet;
use std::path::Path;

use crate::common;

use common::routes::{read, repo_root, strip_comments};

const PAGE: &str = "crates/cog-gateway/src/takeover.html";

/// 整族判这些词的别的门。
const EVENT_VOCAB: &str = "crates/cogneva/tests/web_event_vocabulary_contract_test.rs";
const STATUS_WORDS: &str =
    "crates/cogneva/tests/page_status_words_are_words_the_status_writes_test.rs";
const ASSISTANT_TAGS: &str =
    "crates/cogneva/tests/assistant_event_tags_compared_are_tags_the_enum_writes_test.rs";

const PROCESSES: &str = "crates/cog-gateway/src/processes.rs";
const CLUSTER: &str = "crates/cog-gateway/src/cluster.rs";
const GIT_IDENTITY: &str = "crates/cog-gateway/src/git_identity.rs";
const RUNTIME: &str = "crates/cog-agent/src/agent_kernel/runtime.rs";
const AGENTS: &str = "crates/cog-gateway/src/agents.rs";
const REFLECTION: &str = "crates/cog-core/src/contract/reflection.rs";
/// Where the gateway writes the two code-platform words down.
const PLATFORM_WORDS: &str = "crates/cog-gateway/src/lib.rs";
const LLM_ADMIN: &str = "crates/cog-gateway/src/llm_admin.rs";
const PLATFORM: &str = "crates/cog-gateway/src/auth/platform.rs";
const OAUTH_CALLBACK: &str = "docs/oauth-callback.html";

enum Source {
    /// 写这个词的地方：文件 ＋ 必须还在的一段字节。
    Writer(&'static str, &'static str),
    /// 整族词由另一道门判：那道门的文件。
    Gate(&'static str),
    /// 页面自己写自己读：为什么它不该有服务端写者。
    PageOnly(&'static str),
}

struct Owned {
    words: &'static [&'static str],
    source: Source,
}

const OWNED: &[Owned] = &[
    // ── 别的门整族判的 ──
    Owned {
        words: &[
            "state_change",
            "agent_start",
            "turn_start",
            "react_step_start",
            "message_update",
            "tool_execution_start",
            "tool_execution_end",
            "self_review",
            "agent_end",
            "agent_error",
            "resource_alert",
            "task_status_change",
        ],
        source: Source::Gate(EVENT_VOCAB),
    },
    Owned {
        words: &[
            "message.start",
            "message.text_delta",
            "message.end",
            "message.thinking_delta",
            "message.thinking_end",
        ],
        source: Source::Gate(EVENT_VOCAB),
    },
    Owned {
        words: &[
            "awaiting_review",
            "unrecorded",
            "active",
            "rejected",
            "validation_failed",
            "compile_error",
        ],
        source: Source::Gate(STATUS_WORDS),
    },
    Owned {
        words: &["promoted", "rolled_back", "failed", "awaiting_approval"],
        source: Source::Gate(STATUS_WORDS),
    },
    Owned {
        words: &["policy_update"],
        source: Source::Gate(STATUS_WORDS),
    },
    Owned {
        words: &["thinking_delta"],
        source: Source::Gate(ASSISTANT_TAGS),
    },
    // ── 有写者的词：健康 ──
    Owned {
        words: &["healthy"],
        source: Source::Writer(PROCESSES, "health_of.insert(id, \"healthy\");"),
    },
    Owned {
        words: &["dead"],
        source: Source::Writer(PROCESSES, "health_of.insert(id, \"dead\");"),
    },
    Owned {
        words: &["unknown"],
        source: Source::Writer(PROCESSES, ".unwrap_or_else(|| \"unknown\".into())"),
    },
    // ── 组件状态 ──
    Owned {
        words: &["up", "down"],
        source: Source::Writer(CLUSTER, "status: if ready > 0 { \"up\" } else { \"down\" }"),
    },
    // ── git 身份 ──
    Owned {
        words: &["ready"],
        source: Source::Writer(GIT_IDENTITY, "IdentityState::Ready => \"ready\","),
    },
    Owned {
        words: &["unknown"],
        source: Source::Writer(GIT_IDENTITY, "IdentityState::Unknown => \"unknown\","),
    },
    // ── 沙盒运行状态 ──
    Owned {
        words: &["idle"],
        source: Source::Writer(RUNTIME, "RuntimeState::Idle => \"idle\","),
    },
    Owned {
        words: &["complete"],
        source: Source::Writer(RUNTIME, "RuntimeState::Complete => \"complete\","),
    },
    Owned {
        words: &["error"],
        source: Source::Writer(RUNTIME, "RuntimeState::Error => \"error\","),
    },
    // ── 接口错误体 ──
    Owned {
        words: &["error"],
        source: Source::Writer(AGENTS, "\"status\": \"error\","),
    },
    // ── 贡献 ──
    Owned {
        words: &["ask"],
        source: Source::Writer(REFLECTION, "Self::Ask => \"ask\","),
    },
    // The word moved from the match arms to the declarations the arms now
    // dispatch on, and the registration moves with it. Pointed at the old arms
    // this entry failed the moment the arms stopped spelling the word -- which
    // is the check working, not a word that lost its writer: `"gitee" => {` no
    // longer exists anywhere, and the sentence the page compares lives in one
    // place now.
    Owned {
        words: &["gitee"],
        source: Source::Writer(PLATFORM_WORDS, "GITEE_PLATFORM: &str = \"gitee\";"),
    },
    Owned {
        words: &["github"],
        source: Source::Writer(PLATFORM_WORDS, "GITHUB_PLATFORM: &str = \"github\";"),
    },
    // ── 模型接入 ──
    Owned {
        words: &["llm_verify_failed"],
        source: Source::Writer(LLM_ADMIN, "\"error\": \"llm_verify_failed\""),
    },
    // ── 授权弹窗 ──
    Owned {
        words: &["cogneva-login"],
        source: Source::Writer(PLATFORM, "type: 'cogneva-login'"),
    },
    Owned {
        words: &["cogneva-gitee-oauth", "cogneva-app-manifest"],
        source: Source::Writer(OAUTH_CALLBACK, "'cogneva-app-manifest'"),
    },
    // ── 页面自己写自己读的 ──
    Owned {
        words: &["live", "connecting"],
        source: Source::PageOnly("SSE 连接状态：页面 setCs 写、页面自己读"),
    },
    Owned {
        words: &["running"],
        source: Source::PageOnly("活动流里 agent 行的状态：页面自己从 idle 抬到 running"),
    },
    Owned {
        words: &["zh"],
        source: Source::PageOnly("界面语言：存在 localStorage 里，页面自己写自己读"),
    },
    Owned {
        words: &["string"],
        source: Source::PageOnly("`typeof` 的类型名，不是这一棵树的词"),
    },
    Owned {
        words: &["none"],
        source: Source::PageOnly("`style.display` 的取值，浏览器写的"),
    },
    Owned {
        words: &["giAutoDone"],
        source: Source::PageOnly("页面自己的元素 id"),
    },
    Owned {
        words: &["k", "r", "Enter", "https:"],
        source: Source::PageOnly("键盘按键与 `location.protocol`，浏览器写的"),
    },
];

/// 紧跟着的一个带引号单词（引号可以是 ' / " / `），跳过前导空白。
fn quoted_at(chars: &[char], mut from: usize) -> Option<String> {
    while from < chars.len() && chars[from].is_whitespace() {
        from += 1;
    }
    let quote = *chars.get(from)?;
    if quote != '\'' && quote != '"' && quote != '`' {
        return None;
    }
    let mut word = String::new();
    let mut i = from + 1;
    while i < chars.len() {
        if chars[i] == quote {
            return Some(word);
        }
        word.push(chars[i]);
        i += 1;
    }
    None
}

/// 页面里与字面量比较的词，以及 `switch` 的 `case` 标签。
fn compared_words(page: &str) -> BTreeSet<String> {
    let page = strip_comments(page);
    let chars: Vec<char> = page.chars().collect();
    let mut words = BTreeSet::new();
    let mut i = 0;
    while i + 1 < chars.len() {
        let op = (chars[i] == '=' || chars[i] == '!') && chars[i + 1] == '=';
        if !op {
            i += 1;
            continue;
        }
        let mut j = i + 2;
        if j < chars.len() && chars[j] == '=' {
            j += 1;
        }
        if let Some(word) = quoted_at(&chars, j) {
            if !word.is_empty() {
                words.insert(word);
            }
        }
        i = j;
    }
    // `case '…':`。`lowercase ` 里也有 `case `，前面必须是分隔符。
    let mut cursor = 0;
    while let Some(offset) = page[cursor..].find("case ") {
        let at = cursor + offset;
        cursor = at + "case ".len();
        let before = page[..at].chars().next_back();
        if before.is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '$') {
            continue;
        }
        let from = page[..cursor].chars().count();
        if let Some(word) = quoted_at(&chars, from) {
            if !word.is_empty() {
                words.insert(word);
            }
        }
    }
    words
}

/// 登记的那一行自己怎么说。词烂掉时把它一起印出来，读的人才知道该去哪改。
fn describe(source: &Source) -> String {
    match source {
        Source::Writer(file, _) => format!("写者是 `{file}`"),
        Source::Gate(file) => format!("判它的门是 `{file}`"),
        Source::PageOnly(why) => format!("页面自己的词：{why}"),
    }
}

fn contradictions(root: &Path, owned: &[Owned]) -> Vec<String> {
    let mut complaints = Vec::new();
    let words = compared_words(&read(&root.join(PAGE)));
    if words.is_empty() {
        complaints.push(
            "读不出页面比较过的词：一个都没有。读不出来的判据判不了任何东西，所以按红处理"
                .to_string(),
        );
        return complaints;
    }
    let mut registered: BTreeSet<&str> = BTreeSet::new();
    for entry in owned {
        for word in entry.words {
            registered.insert(word);
            if !words.contains(*word) {
                complaints.push(format!(
                    "  登记的 `{word}`（{}）页面已经不比了：要么那一处被删了，要么它改拼了\
                     别的词。登记烂在这里就没人发现",
                    describe(&entry.source)
                ));
                continue;
            }
            match &entry.source {
                Source::Writer(file, needle) => match std::fs::read_to_string(root.join(file)) {
                    Ok(text) if text.contains(needle) => {}
                    Ok(_) => complaints.push(format!(
                        "  `{word}` 登记的写者 `{file}` 里找不到那段字节 `{needle}`：写者\
                         改名了而页面没跟上，这一支永远不成立"
                    )),
                    Err(e) => complaints.push(format!(
                        "  `{word}` 登记的写者 `{file}` 读不出来（{e}）：读不出来按红处理"
                    )),
                },
                Source::Gate(file) => {
                    if !root.join(file).exists() {
                        complaints.push(format!(
                            "  `{word}` 登记由 `{file}` 判，而那道门不在了：这一族词现在\
                             没人管"
                        ));
                    }
                }
                Source::PageOnly(_) => {}
            }
        }
    }
    for word in &words {
        if registered.contains(word.as_str()) {
            continue;
        }
        complaints.push(format!(
            "  页面拿 `{word}` 和变量比，而这个词没有登记：它要么是某个写出来的词（那就\
             登记写它的地方），要么是页面自己写自己读的（那就登记理由）。没登记的词，\
             拼错了只会落到 else 那一支"
        ));
    }
    complaints
}

#[test]
fn every_word_the_page_compares_is_owned() {
    let complaints = contradictions(&repo_root(), OWNED);
    assert!(
        complaints.is_empty(),
        "运维页与字面量比较的词必须有人写、有人管。没人管的词拼错了不报错，只会\
         永远走另一支：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let words = compared_words(&read(&repo_root().join(PAGE)));
    assert!(
        words.len() >= 50,
        "页面只读出 {} 个比较的词，太少：{words:?}",
        words.len()
    );
    let registered: BTreeSet<&str> = OWNED.iter().flat_map(|e| e.words).copied().collect();
    assert!(registered.len() >= 50, "登记只有 {} 个词", registered.len());
}

/// 一棵最小的树：页面比较两个词，一个由写者写、一个是页面自己的。
const FIXTURE_PAGE: &str = "\
function f(x, y) {
  if (x === 'from_server') return 1;
  if (y === 'page_only') return 2;
  switch (x) {
    case 'labelled':
      return 3;
  }
  return 0;
}
";

fn fixture(root: &Path, page: &str, server: &str) {
    for (relative, body) in [
        (PAGE, page),
        ("server.rs", server),
        ("gate_test.rs", "// a gate\n"),
    ] {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
        std::fs::write(path, body).expect("write");
    }
}

fn fixture_registry() -> Vec<Owned> {
    vec![
        Owned {
            words: &["from_server"],
            source: Source::Writer("server.rs", "status: \"from_server\","),
        },
        Owned {
            words: &["page_only"],
            source: Source::PageOnly("页面自己的词"),
        },
        Owned {
            words: &["labelled"],
            source: Source::Gate("gate_test.rs"),
        },
    ]
}

#[test]
fn the_fixture_is_green_before_anything_is_changed() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(
        dir.path(),
        FIXTURE_PAGE,
        "let x = Row { status: \"from_server\", };\n",
    );
    let complaints = contradictions(dir.path(), &fixture_registry());
    assert!(complaints.is_empty(), "{complaints:?}");
}

#[test]
fn a_word_nobody_owns_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let page = FIXTURE_PAGE.replace("'page_only'", "'page_only_v2'");
    assert_ne!(page, FIXTURE_PAGE);
    fixture(
        dir.path(),
        &page,
        "let x = Row { status: \"from_server\", };\n",
    );
    let complaints = contradictions(dir.path(), &fixture_registry());
    assert!(
        complaints.iter().any(|c| c.contains("page_only_v2")),
        "{complaints:?}"
    );
}

#[test]
fn a_registered_word_the_page_stopped_comparing_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let page = FIXTURE_PAGE.replace("if (y === 'page_only') return 2;", "");
    assert_ne!(page, FIXTURE_PAGE);
    fixture(
        dir.path(),
        &page,
        "let x = Row { status: \"from_server\", };\n",
    );
    let complaints = contradictions(dir.path(), &fixture_registry());
    assert!(
        complaints.iter().any(|c| c.contains("page_only")),
        "{complaints:?}"
    );
}

#[test]
fn a_writer_that_stopped_writing_the_word_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(
        dir.path(),
        FIXTURE_PAGE,
        "let x = Row { status: \"renamed\", };\n",
    );
    let complaints = contradictions(dir.path(), &fixture_registry());
    assert!(
        complaints.iter().any(|c| c.contains("from_server")),
        "{complaints:?}"
    );
}

#[test]
fn a_gate_that_no_longer_exists_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(
        dir.path(),
        FIXTURE_PAGE,
        "let x = Row { status: \"from_server\", };\n",
    );
    std::fs::remove_file(dir.path().join("gate_test.rs")).expect("remove");
    let complaints = contradictions(dir.path(), &fixture_registry());
    assert!(
        complaints.iter().any(|c| c.contains("labelled")),
        "{complaints:?}"
    );
}
