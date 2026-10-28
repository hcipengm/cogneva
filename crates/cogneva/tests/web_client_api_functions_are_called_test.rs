//! principle: feedback_a_change_lands_only_if_it_reaches_a_build_target
//! principle: feedback_no_hardcoding_config_surface
//!
//! A request function in the WebUI's API module is a claim about a response: it
//! names a type, and that type is the second spelling of a shape the gateway
//! writes. Nothing in this tree compares the two — the client ships compiled,
//! the gateway answers whatever it answers — so the only force that ever
//! corrects the claim is somebody *calling* the function and reading the field.
//!
//! A function nothing calls is a claim that is never tested. It survives because
//! it looks like capability, and the shape it declares drifts freely: the entry
//! for the session endpoints declared `{ id, title, created_at, updated_at }`
//! and posted to a handler that answers `{ code, data: { session_id, title,
//! workspace_id, agent_id, created_at } }` — neither the envelope nor the key
//! names matched, and both were wrong in the silent direction. Whoever picked
//! that function up next would have read `undefined` off a field that has a
//! perfectly good name on the other side. The same was true of the cluster row
//! types removed beside it: `ClusterAgent.status` declared `recovering`, a word
//! no writer in the tree produces.
//!
//! So the rule is the one already used for Rust modules that no `mod` reaches: a
//! declaration that nothing wires is not code. This gate asks it of the client's
//! API module -- every exported function is imported by name from a module whose
//! specifier points at `api/client`, in some other file under `web/src`.
//!
//! Judged by the import, not by the word. A bare name match is how dead
//! functions hid here: the module's own `createSession` had no caller, and the
//! word `createSession` appears throughout `WorkspaceView.tsx` and
//! `chatStore.ts` naming the *store's* method of the same name. Counting words
//! would have called it alive forever. The import list is the only place that
//! says which `createSession` is meant.
//!
//! What it cannot judge: whether the declared response shape matches the
//! handler. A called function can still name the wrong keys -- this gate removes
//! the case where nobody would ever find out, not the case where the reader is
//! looking at the wrong field.
//!
//! 实测（2026-10-27）：本门落地时树上是 98 个导出函数，全部被至少一处具名
//! import 引用，零条报告；同一棵树里 `listSessions` 与 `sendMessage` 没有任何
//! 调用者（`Session`/`Message` 两个接口只被它们使用），本条实测前已删掉。
//! 消融对照两条，都在临时树上：给客户端加一个 `ghostFetch` 导出而不加调用者，
//! 本门红并点名它；把调用方写成从别的模块 import 同名函数，本门仍旧红——证明
//! 判据认的是 import 的模块，不是出现过这个词。

mod common;

use common::routes::repo_root;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The client's API module: the exports this gate holds.
const CLIENT: &str = "web/src/api/client.ts";

/// The directory whose files count as callers.
const WEB: &str = "web/src";

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Drop `//` and `/* */` comments, keep string literals.
///
/// The module specifiers this gate reads are strings, so they have to survive;
/// a commented-out import must not count as a caller, so the comments have to
/// go. Same reader as the query-name gate, for the same reason — the tree is
/// full of Chinese and every offset here is a byte offset.
fn strip_comments(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[i];
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == quote {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&text[start..i.min(bytes.len())]);
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            _ => {
                let ch = text[i..].chars().next().expect("in bounds");
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    out
}

/// The names an `import` statement pulls in, before `from`.
///
/// `{ a, type B, c as d }` yields `a`, `B`, `c` — the local alias is not what
/// the exporting module calls the thing.
fn imported_tokens(before_from: &str) -> Vec<String> {
    let Some(open) = before_from.find('{') else {
        return Vec::new();
    };
    let Some(close) = before_from[open..].find('}') else {
        return Vec::new();
    };
    before_from[open + 1..open + close]
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let part = part.strip_prefix("type ").unwrap_or(part).trim();
            match part.split_once(" as ") {
                Some((name, _)) => name.trim().to_string(),
                None => part.to_string(),
            }
        })
        .filter(|name| !name.is_empty())
        .collect()
}

/// Every `import … from '<specifier>'` in a file, as `(specifier, names)`.
fn imports(source: &str) -> Vec<(String, Vec<String>)> {
    let clean = strip_comments(source);
    let bytes = clean.as_bytes();
    let mut found = Vec::new();
    let mut cursor = 0;
    while let Some(at) = clean[cursor..].find("import") {
        let start = cursor + at;
        let boundary_before =
            start == 0 || !(bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_');
        let Some(len) = clean[start..].find(';') else {
            break;
        };
        let statement = &clean[start..start + len];
        cursor = start + len + 1;
        if !boundary_before {
            continue;
        }
        let Some(from_at) = statement.find("from") else {
            continue;
        };
        let tail = &statement[from_at + "from".len()..];
        let Some(quote_at) = tail.find(['\'', '"']) else {
            continue;
        };
        let quote = tail.as_bytes()[quote_at];
        let Some(close) = tail[quote_at + 1..].find(quote as char) else {
            continue;
        };
        let specifier = &tail[quote_at + 1..quote_at + 1 + close];
        found.push((
            specifier.to_string(),
            imported_tokens(&statement[..from_at]),
        ));
    }
    found
}

/// The `export (async) function` names a module declares, in order.
fn exported_functions(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in strip_comments(source).lines() {
        let line = line.trim_start();
        let rest = line
            .strip_prefix("export async function ")
            .or_else(|| line.strip_prefix("export function "));
        let Some(rest) = rest else { continue };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
            .collect();
        if !name.is_empty() {
            names.push(name);
        }
    }
    names
}

/// Every `.ts` / `.tsx` under `dir`.
fn ts_files(dir: &Path, out: &mut Vec<PathBuf>) {
    // 读不出来就 panic：递归遍历里跳过一个子目录不会让任何断言变红，而剩下的那份
    // 普查往往仍高于下限——「少读了一块」与「那一块都合规」在判词上分不开。
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| {
        panic!(
            "{} 读不出来（{e}）：这棵树的普查面少了一块，而用剩下的那份判出来的\
             结论与「整片区都合规」同形",
            dir.display()
        )
    });
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            ts_files(&path, out);
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("ts") | Some("tsx")
        ) {
            out.push(path);
        }
    }
}

/// The exported functions of `web/src/api/client.ts` under `root` that no other
/// file imports from the client module.
fn uncalled(root: &Path) -> Vec<String> {
    let client_path = root.join(CLIENT);
    if !client_path.exists() {
        return Vec::new();
    }
    let source = read(&client_path);
    let client = std::fs::canonicalize(&client_path).unwrap_or(client_path);
    let mut files = Vec::new();
    ts_files(&root.join(WEB), &mut files);

    let mut called = BTreeSet::new();
    for path in files {
        if std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone()) == client {
            continue;
        }
        for (specifier, names) in imports(&read(&path)) {
            if specifier.contains("api/client") {
                called.extend(names);
            }
        }
    }
    exported_functions(&source)
        .into_iter()
        .filter(|name| !called.contains(name))
        .collect()
}

fn client_source(root: &Path) -> String {
    read(&root.join(CLIENT))
}

#[test]
fn every_client_api_function_is_imported_by_a_caller() {
    let root = repo_root();
    let found = uncalled(&root);
    assert!(
        found.is_empty(),
        "these functions are exported from {CLIENT} and imported nowhere under {WEB}: {found:?}\n\
         A request function nobody calls is a response shape nobody ever checks: it drifts from \
         the handler silently (a declared key the gateway spells differently reads as undefined), \
         and it reads as capability that is already wired. Delete it, or wire it and read the \
         field."
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let exported = exported_functions(&client_source(&root));
    assert!(
        exported.len() >= 90,
        "only {} functions found in {CLIENT} — the reader has lost the module's shape, and a \
         short list would pass this gate by finding nothing: {exported:?}",
        exported.len()
    );

    let mut files = Vec::new();
    ts_files(&root.join(WEB), &mut files);
    let mut imported = BTreeSet::new();
    for path in files {
        for (specifier, names) in imports(&read(&path)) {
            if specifier.contains("api/client") {
                imported.extend(names);
            }
        }
    }
    assert!(
        imported.len() >= 90,
        "only {} names imported from the client module under {WEB} — the import reader is not \
         seeing the statements it is supposed to hold",
        imported.len()
    );

    for known in ["getClusterOverview", "getSessionMessages", "login"] {
        assert!(
            exported.contains(&known.to_string()),
            "{known} is no longer an exported function of {CLIENT}; this gate reads the module \
             through that shape, so its absence means the reader is stale, not that the endpoint \
             is gone"
        );
        assert!(
            imported.contains(known),
            "{known} is exported but no file under {WEB} imports it — either the module lost its \
             only caller or the import reader stopped seeing it"
        );
    }
}

#[test]
fn a_function_without_a_caller_is_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-api-called-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("web/src/api")).expect("temp tree");
    std::fs::write(
        root.join(CLIENT),
        "export async function listed(): Promise<string[]> {\n  return [];\n}\n\
         export async function ghostFetch(): Promise<string> {\n  return '';\n}\n\
         export function alsoFine(): number {\n  return 1;\n}\n",
    )
    .expect("client");

    std::fs::write(
        root.join("web/src/view.ts"),
        "import { listed, alsoFine } from '@/api/client';\n\
         // ghostFetch is mentioned here and nowhere else.\n\
         export const rows = listed;\nexport const n = alsoFine;\n",
    )
    .expect("view");

    // A same-named function exported by a *different* module is not a caller of
    // this one. This is the shape that hid the session functions: the store's
    // own `createSession` reads as a use of the API module's `createSession`
    // until you look at which module the import names.
    std::fs::write(
        root.join("web/src/store.ts"),
        "import { ghostFetch } from '@/store/local';\nexport const g = ghostFetch;\n",
    )
    .expect("store");

    assert_eq!(uncalled(&root), vec!["ghostFetch".to_string()]);

    // Wiring it clears the report: the gate holds the caller, not the name.
    std::fs::write(
        root.join("web/src/view.ts"),
        "import {\n  listed,\n  alsoFine,\n  type ghostFetch as Ghost,\n} from '@/api/client';\n\
         export const rows = listed;\nexport const n = alsoFine;\nexport type G = Ghost;\n",
    )
    .expect("view");
    assert!(uncalled(&root).is_empty(), "{:?}", uncalled(&root));

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn the_import_reader_sees_names_and_their_module() {
    let source = "import { a, type B, c as d } from '@/api/client';\n\
                  import type { E } from '@/api/client';\n\
                  import { f } from '@/other/module';\n\
                  // import { ghost } from '@/api/client';\n\
                  /* import { ghost2 } from '@/api/client'; */\n";
    let found = imports(source);
    assert_eq!(
        found,
        vec![
            (
                "@/api/client".to_string(),
                vec!["a".to_string(), "B".to_string(), "c".to_string()]
            ),
            ("@/api/client".to_string(), vec!["E".to_string()]),
            ("@/other/module".to_string(), vec!["f".to_string()]),
        ]
    );
}
