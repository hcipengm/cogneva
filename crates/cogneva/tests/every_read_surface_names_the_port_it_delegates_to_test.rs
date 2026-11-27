//! 网关与反射侧的每一个读口，都点名它落到哪个存储/服务调用上；那一串必须**逐字**出现在
//! 那个函数体里——搬走、改名、删掉都会红，而不是让读者照着一份过时的清单去找。
//!
//! 这一层自己不读表（另一道门守着），所以「这个答案从哪条流算出来」在这一层就是
//! 「它落到哪个读口」。名下没有这一行的读口 = 普查里没有它。
//!
//! 没有这次改动这条会红：62 个读口没有任何一份普查。

use crate::common::routes::repo_root;
use crate::common::rust_production::production_texts;
use cogneva::read_surfaces::READ_SURFACES;

const READ_PREFIXES: &[&str] = &[
    "read",
    "list",
    "get",
    "load",
    "pending",
    "current",
    "dialogue",
    "calls",
    "keys",
    "rebuild",
    "redactions",
    "lease",
    "item_",
    "principle_",
];

/// 从 `fn` 位置往后找到函数体（大括号配对到结尾）。
fn body_after(text: &str, from: usize) -> Option<&str> {
    let open = from + text[from..].find('{')?;
    let mut depth = 0usize;
    for (offset, ch) in text[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[open..open + offset + 1]);
                }
            }
            _ => {}
        }
    }
    None
}

#[test]
fn every_read_surface_is_censused_and_still_calls_the_port_it_names() {
    let root = repo_root();
    let mut problems = Vec::new();
    let mut found: Vec<(String, String)> = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        if !(relative.starts_with("crates/cog-gateway/src/")
            || relative.starts_with("crates/cog-reflection/src/"))
        {
            continue;
        }
        for marker in ["pub async fn "] {
            let mut search = 0usize;
            while let Some(at) = text[search..].find(marker) {
                let start = search + at;
                search = start + marker.len();
                let rest = &text[start + marker.len()..];
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                if !READ_PREFIXES.iter().any(|prefix| name.starts_with(prefix)) {
                    continue;
                }
                found.push((name.clone(), relative.clone()));
                let Some(body) = body_after(&text, start) else {
                    problems.push(format!("{relative}: {name} 读不出函数体"));
                    continue;
                };
                let Some(row) = READ_SURFACES
                    .iter()
                    .find(|row| row.function == name && row.path == relative)
                else {
                    problems.push(format!("{relative}: {name} 没有普查行"));
                    continue;
                };
                if row.delegates != "none" && !body.contains(row.delegates) {
                    problems.push(format!(
                        "{relative}: {name} 的普查点名的 `{}` 不在函数体里了",
                        row.delegates
                    ));
                }
            }
        }
    }
    for row in READ_SURFACES {
        if !found
            .iter()
            .any(|(name, path)| name == row.function && path == &row.path)
        {
            problems.push(format!(
                "{}: {} 这一行点名了一个已经不在的函数",
                row.path, row.function
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "读口普查与树对不上：\n  {}",
        problems.join("\n  ")
    );
}
