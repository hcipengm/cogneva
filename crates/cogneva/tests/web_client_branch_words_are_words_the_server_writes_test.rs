//! principle: feedback_no_hardcoding_config_surface
//!
//! The WebUI branches on the words the server answers with, and the branch is
//! where the word is *used*: `outcome === 'timed_out'` picks the colour of a
//! build row, `status === 'hook_event'` decides a frame is not a task changing
//! state, `parsed.error === 'llm_verify_failed'` decides whether the operator is
//! offered the "save anyway" button at all.
//!
//! Nothing compares the two spellings. The client ships compiled and the server
//! answers whatever it answers, so a word renamed on the server side lands in
//! the client's `else` branch — and the `else` branch of a colour table is never
//! wrong-looking. The build that exceeded its budget is painted the same red as
//! a failed one; the hook frame is counted as a real task transition; the setup
//! screen says "save failed" and the way forward it should have offered is
//! simply absent. A branch that can never match reads as a feature that is
//! quiet, which is the same reading as "it did not happen".
//!
//! The event vocabulary between the bus and the client is already pinned by its
//! own gate; this one holds the words the client branches on when it reads a
//! *value* out of a payload — `outcome` and `status` — plus the one error word
//! the setup screen switches on.
//!
//! What it judges:
//!
//! * every `outcome` / `status` word the client compares against is either a
//!   word the server writes at a named site, or a word the client writes itself
//!   (registered with the reason). A negated comparison (`!==`) names the same
//!   word and is read the same way — swapping the arms does not take the word
//!   off the wire;
//! * each registered pair still exists on both sides: the server site still
//!   writes the word, and the client still compares it.
//!
//! What it cannot judge:
//!
//! * which field a word belongs to. `status` is written by several endpoints
//!   (task state, device-code polling, hook forwarding, backend health), and the
//!   same word may be answered by more than one of them; a word moved from one
//!   endpoint to another passes here;
//! * a server word the client *should* branch on and does not. A new build
//!   outcome falls through to the client's fallback colour, which is visibly
//!   wrong rather than silent, so that direction is left to the eye;
//! * fields outside the two named above (`kind`, `phase`, `type`, `code`).
//!   `type` is the event gate's; `kind` and `phase` are mostly the client's own
//!   union types; `code` on a change row carries git's porcelain letters
//!   forwarded verbatim, which are not this tree's words to hold;
//! * arms inside a `switch`/`case`. The subject of the switch is not on the
//!   case line, so pairing it with a word would mean following a variable
//!   through the function — a parser this gate does not have.

mod common;

use common::routes::repo_root;

use std::path::{Path, PathBuf};

/// The payload fields this gate holds. A comparison on any other field is out
/// of scope, not an exemption — see the module note for why each is out.
const COMPARED_FIELDS: &[&str] = &["outcome", "status"];

/// One word the client branches on, and the site that writes it.
struct ServerWord {
    field: &'static str,
    word: &'static str,
    /// The client file that branches on the word, and the file the server
    /// writes it from. Both are checked: the pair has to exist on both sides.
    client_file: &'static str,
    server_file: &'static str,
    /// The exact text at the write site. A needle that is just the bare word
    /// would also match a test fixture or a comment in the same file, and a
    /// rename elsewhere would leave this gate green — so the needle carries
    /// enough of the write to name one place.
    server_needle: &'static str,
}

const SERVER_WORDS: &[ServerWord] = &[
    ServerWord {
        field: "outcome",
        word: "built",
        client_file: "web/src/views/DeliveryView.tsx",
        server_file: "crates/cog-reflection/src/evolution_build_readings.rs",
        server_needle: "BuildEnding::Built(_) => \"built\"",
    },
    ServerWord {
        field: "outcome",
        word: "timed_out",
        client_file: "web/src/views/DeliveryView.tsx",
        server_file: "crates/cog-reflection/src/evolution_build_readings.rs",
        server_needle: "BuildEnding::TimedOut(_) => \"timed_out\"",
    },
    ServerWord {
        field: "outcome",
        word: "unstarted",
        client_file: "web/src/views/DeliveryView.tsx",
        server_file: "crates/cog-reflection/src/evolution_build_readings.rs",
        server_needle: "pub const OUTCOME_UNSTARTED: &str = \"unstarted\";",
    },
    ServerWord {
        field: "status",
        word: "completed",
        client_file: "web/src/store/liveComms.ts",
        server_file: "crates/cog-core/src/types/task.rs",
        server_needle: "TaskStatus::Completed => \"completed\"",
    },
    ServerWord {
        field: "status",
        word: "failed",
        client_file: "web/src/store/streamStore.ts",
        server_file: "crates/cog-core/src/types/task.rs",
        server_needle: "TaskStatus::Failed => \"failed\"",
    },
    ServerWord {
        field: "status",
        word: "hook_event",
        client_file: "web/src/hooks/useEventStream.ts",
        server_file: "crates/cog-gateway/src/hook_forwarder.rs",
        server_needle: "status: \"hook_event\"",
    },
    ServerWord {
        field: "status",
        word: "slow_down",
        client_file: "web/src/overlay/ContributionOverlay.tsx",
        server_file: "crates/cog-gateway/src/contribution_admin.rs",
        server_needle: "\"status\": \"slow_down\"",
    },
    ServerWord {
        field: "status",
        word: "connected",
        client_file: "web/src/overlay/ContributionOverlay.tsx",
        server_file: "crates/cog-gateway/src/contribution_admin.rs",
        server_needle: "\"status\": \"connected\"",
    },
    ServerWord {
        field: "status",
        word: "error",
        client_file: "web/src/overlay/ContributionOverlay.tsx",
        server_file: "crates/cog-gateway/src/contribution_admin.rs",
        server_needle: "\"status\": \"error\"",
    },
    ServerWord {
        field: "status",
        word: "PASS",
        client_file: "web/src/store/streamStore.ts",
        server_file: "crates/cog-core/src/types/event.rs",
        server_needle: "pub const SELF_REVIEW_STATUS_PASS: &str = \"PASS\";",
    },
    ServerWord {
        field: "status",
        word: "up",
        client_file: "web/src/observability/ProbesBoard.tsx",
        server_file: "crates/cog-gateway/src/backend_health.rs",
        server_needle: "BackendState::Up => \"up\"",
    },
    // The setup screen does not read this word out of `status` — it parses an
    // error body and switches on the parsed word. Registered here because the
    // failure it hides is the silent kind: without this branch the operator is
    // never shown the "save anyway" path that the refusal was telling them
    // about.
    ServerWord {
        field: "error",
        word: "llm_verify_failed",
        client_file: "web/src/overlay/LlmSetupOverlay.tsx",
        server_file: "crates/cog-gateway/src/llm_admin.rs",
        server_needle: "\"error\": \"llm_verify_failed\"",
    },
];

/// Statuses the client writes into its own stores and then compares against.
/// Nothing on the server answers these, so holding them to the server would be
/// a census of the panel's own words.
const CLIENT_ONLY: &[(&str, &str, &str)] = &[
    ("status", "running", "工具调用的行内状态，面板自己写自己读"),
    ("status", "sending", "对话气泡的发送状态，客户端自己写"),
    ("status", "streaming", "对话气泡的流式状态，客户端自己写"),
    ("status", "done", "对话气泡收尾后的状态，客户端自己写自己读"),
];

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn client_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            client_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "ts" || e == "tsx") {
            out.push(path);
        }
    }
}

/// Every `field === 'word'` in the client, as `(field, word)`.
///
/// `typeof x === 'string'` is not a branch on a word — the right side names a
/// JavaScript type — so those are skipped by looking at what precedes the field.
fn comparisons(source: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") || trimmed.starts_with('*') {
            continue;
        }
        for field in COMPARED_FIELDS {
            let mut from = 0;
            while let Some(at) = line[from..].find(field) {
                let start = from + at;
                let end = start + field.len();
                from = end;
                let boundary_before = line[..start]
                    .chars()
                    .next_back()
                    .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
                let boundary_after = line[end..]
                    .chars()
                    .next()
                    .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
                if !boundary_before || !boundary_after {
                    continue;
                }
                if follows_typeof(&line[..start]) {
                    continue;
                }
                let rest = line[end..].trim_start();
                // `!==` is the same branch with the arms swapped — the word is
                // still on the wire and still has to exist. `!=` last of all so
                // it cannot shadow `!==`.
                let rest = ["===", "!==", "==", "!="]
                    .iter()
                    .find_map(|op| rest.strip_prefix(op));
                let Some(rest) = rest else {
                    continue;
                };
                let rest = rest.trim_start();
                let Some(quote) = rest.chars().next().filter(|c| *c == '\'' || *c == '"') else {
                    continue;
                };
                let Some(close) = rest[1..].find(quote) else {
                    continue;
                };
                let word = &rest[1..1 + close];
                if word.is_empty() {
                    continue;
                }
                out.push((field.to_string(), word.to_string()));
            }
        }
    }
    out
}

/// Whether the field is the operand of a `typeof` — `typeof p.status === 'string'`
/// names a JavaScript type on the right, not a word the server writes. Both
/// spellings occur (`typeof status`, `typeof p.status`), so the dotted form is
/// stepped back over before looking for the keyword.
fn follows_typeof(before: &str) -> bool {
    let mut head = before.trim_end();
    if let Some(stripped) = head.strip_suffix('.') {
        let stripped = stripped.trim_end();
        let start = stripped
            .char_indices()
            .rev()
            .find(|(_, c)| !(c.is_alphanumeric() || *c == '_'))
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        head = stripped[..start].trim_end();
    }
    head.ends_with("typeof")
}

fn registered(server_words: &[ServerWord], field: &str, word: &str) -> bool {
    server_words
        .iter()
        .any(|w| w.field == field && w.word == word)
}

fn client_only(client_only: &[(&str, &str, &str)], field: &str, word: &str) -> bool {
    client_only
        .iter()
        .any(|(f, w, _)| *f == field && *w == word)
}

/// Every way the two sides disagree. An empty answer means the client only
/// branches on words that are on the wire.
fn disagreements(
    root: &Path,
    server_words: &[ServerWord],
    own_words: &[(&str, &str, &str)],
) -> Vec<String> {
    let mut out = Vec::new();

    let mut files: Vec<PathBuf> = Vec::new();
    client_files(&root.join("web/src"), &mut files);
    files.sort();
    let mut seen: Vec<(String, String)> = Vec::new();
    for file in &files {
        for pair in comparisons(&read(file)) {
            if seen.contains(&pair) {
                continue;
            }
            seen.push(pair.clone());
            if !registered(server_words, &pair.0, &pair.1)
                && !client_only(own_words, &pair.0, &pair.1)
            {
                out.push(format!(
                    "{} 按 {}='{}' 分支，服务端没有人写这个词（也没登记成客户端自己的词）",
                    file.strip_prefix(root).unwrap_or(file).display(),
                    pair.0,
                    pair.1
                ));
            }
        }
    }

    for w in server_words {
        let server = read(&root.join(w.server_file));
        if !server.contains(w.server_needle) {
            out.push(format!(
                "登记说 `{}` 在 {} 写 `{}`，那里已经没有这段字了：客户端按 {}='{}' 分支，落在 else 上",
                w.word, w.server_file, w.server_needle, w.field, w.word
            ));
        }
        let client = read(&root.join(w.client_file));
        let compares = format!("== '{0}'", w.word);
        let compares_eq = format!("=== '{0}'", w.word);
        let compares_dq = format!("== \"{0}\"", w.word);
        let compares_eq_dq = format!("=== \"{0}\"", w.word);
        if !(client.contains(&compares)
            || client.contains(&compares_eq)
            || client.contains(&compares_dq)
            || client.contains(&compares_eq_dq))
        {
            out.push(format!(
                "{} 里已经没有按 {}='{}' 的分支了，这条登记过期",
                w.client_file, w.field, w.word
            ));
        }
    }

    out
}

#[test]
fn every_word_the_client_branches_on_is_a_word_the_server_writes() {
    let root = repo_root();
    let found = disagreements(&root, SERVER_WORDS, CLIENT_ONLY);
    assert!(
        found.is_empty(),
        "WebUI 分支用的词与服务端写出的词对不上：\n{}\n\
         服务端改了名，客户端的这一支就永远不成立——颜色落到兜底、帧被当成状态变更、\
         该给的出路不出现，读起来都像「没发生」",
        found.join("\n")
    );
}

/// 判据自己也要有判据：一棵小树里两侧对得上，判出来必须是空；把服务端那句
/// 改掉、把客户端的比较改掉，判出来必须各自点名。少了这条，「永远返回空」与
/// 「两侧真的一致」不可分。
#[test]
fn a_word_missing_on_either_side_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    std::fs::create_dir_all(root.join("web/src")).expect("client dir");
    std::fs::create_dir_all(root.join("crates/example/src")).expect("server dir");
    std::fs::write(
        root.join("web/src/panel.tsx"),
        "const tone = w.outcome === 'built' ? 'ok' : 'danger';\n\
         const local = state.status === 'running';\n",
    )
    .expect("client");
    std::fs::write(
        root.join("crates/example/src/lib.rs"),
        "fn outcome() -> &'static str { \"built\" }\n",
    )
    .expect("server");

    let words = [ServerWord {
        field: "outcome",
        word: "built",
        client_file: "web/src/panel.tsx",
        server_file: "crates/example/src/lib.rs",
        server_needle: "-> &'static str { \"built\" }",
    }];
    let local = [("status", "running", "面板自己的词")];

    assert_eq!(disagreements(root, &words, &local), Vec::<String>::new());

    // 服务端改了名：客户端那一支永远不成立。
    std::fs::write(
        root.join("crates/example/src/lib.rs"),
        "fn outcome() -> &'static str { \"succeeded\" }\n",
    )
    .expect("server");
    let broken = disagreements(root, &words, &local);
    assert_eq!(broken.len(), 1, "{broken:?}");
    assert!(broken[0].contains("built"), "{broken:?}");

    // 客户端不再比较这个词：登记自己过期了。
    std::fs::write(
        root.join("crates/example/src/lib.rs"),
        "fn outcome() -> &'static str { \"built\" }\n",
    )
    .expect("server");
    std::fs::write(
        root.join("web/src/panel.tsx"),
        "const tone = w.outcome ?? 'built';\n",
    )
    .expect("client");
    let stale = disagreements(root, &words, &local);
    assert_eq!(stale.len(), 1, "{stale:?}");
    assert!(stale[0].contains("过期"), "{stale:?}");

    // 一个既没登记、也没说清是客户端自己的词：这一支也要点出来。
    std::fs::write(
        root.join("web/src/panel.tsx"),
        "const tone = w.outcome === 'built' ? 'ok' : 'danger';\n\
         const other = w.status === 'hooked';\n",
    )
    .expect("client");
    let unregistered = disagreements(root, &words, &local);
    assert_eq!(unregistered.len(), 1, "{unregistered:?}");
    assert!(unregistered[0].contains("hooked"), "{unregistered:?}");
}

/// 解析面自己也要有判据，否则「读到空集」与「客户端没有分支」不可分。
#[test]
fn the_comparison_reader_sees_words_and_skips_type_checks() {
    let found = comparisons(
        "a.outcome === 'timed_out'\n\
         b.status == \"hook_event\"\n\
         failed: p.status !== 'PASS'\n\
         typeof p.status === 'string'\n\
         const s = typeof status === 'string' ? status : '';\n\
         // c.status === 'commented_out'\n\
         d.statusTag === 'notme'\n\
         e.phase === 'ignored'\n",
    );
    assert_eq!(
        found,
        vec![
            ("outcome".to_string(), "timed_out".to_string()),
            ("status".to_string(), "hook_event".to_string()),
            ("status".to_string(), "PASS".to_string()),
        ]
    );
}
