//! principle: feedback_no_hardcoding_config_surface
//!
//! Every field of a response interface in `web/src/api/client.ts` is the
//! client's spelling of a key some Rust type serialises or some `json!({...})`
//! literal writes. Nothing compares the two lists. The client is compiled and
//! the gateway answers whatever it answers, so a field spelled differently is
//! not an error anywhere — it decodes to `undefined`, and a panel drawing from
//! it draws the same thing it draws when the value is genuinely absent.
//!
//! The census here is deliberately blunt: it asks only whether the field's name
//! is spelled *anywhere in this tree's production Rust*, not whether that
//! particular endpoint writes it. That direction is the sound one. A field name
//! that no Rust file contains cannot be written by a handler built from those
//! files, so a hit is a real fault, while a miss proves nothing about which
//! endpoint produced the value. Pairing field to handler would need the
//! response builders resolved through `#[serde(flatten)]`, generic wrappers and
//! loop-built `json!` values — a resolver that is wrong quietly, which is the
//! failure this gate exists to catch.
//!
//! What it cannot judge: whether the endpoint a call reaches is the one that
//! writes the field. A field moved from one response to another passes here;
//! only the spelling is held. Nor does it see a handler that builds its object
//! with a computed key (`json!({ key: value })` with `key` a variable). No
//! writer in the tree does that today; if one starts, the fix is to spell the
//! key literally at the write site or to teach this gate the shape — not to
//! soften the rule, because a rule that permits a name to appear nowhere is a
//! rule that permits every typo.
//!
//! 实测（2026-10-27）：客户端 146 个响应接口、826 个字段，其中恰好一个没有
//! 出处——`LlmConfigResult.failed_restarts`。网关那张表写的是 `unverified_upstreams`，
//! 从来没有 `failed_restarts` 这个键；而唯一的调用方 `LlmSetupOverlay` 把返回值
//! 整个丢掉了，所以这个字段从写下来那天起就没被读过一次。本条实测前已把整张表
//! 删掉，`saveLlmConfig` 改为只回答「抛没抛」。另有且只有一个字段是客户端自己
//! 改的名（`LoginResponse.refreshToken` ← `refresh_token`），逐条登记在 `RENAMED`。
//! 消融对照：在临时树上给一个接口加 `ghost_field`，本门红并点名 `接口名.字段`；
//! 把这个名字写进 Rust 侧，同一棵树转绿。

use std::path::PathBuf;

mod common;

use common::routes::read;

use common::routes::repo_root;

use common::rust_production::{production_sources, production_text, sources_of_tree};

/// The client module whose interfaces are the response shapes.
const CLIENT: &str = "web/src/api/client.ts";

/// Every `export interface X {` in a module, with the plain fields declared at
/// the top level of its body.
///
/// Only top-level fields are read: a nested object literal is indented further,
/// and its keys belong to a shape declared elsewhere. Under-reading costs a few
/// field checks; reading a nested key as if it were this interface's would
/// report a name the server may well never write for *this* type.
fn interfaces(source: &str) -> Vec<(String, Vec<String>)> {
    let mut found = Vec::new();
    let mut current: Option<(String, Vec<String>, i32)> = None;
    for line in source.lines() {
        let trimmed = line.trim_start();
        if current.is_none() {
            if let Some(rest) = trimmed.strip_prefix("export interface ") {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() && trimmed.ends_with('{') {
                    current = Some((name, Vec::new(), 1));
                }
            }
            continue;
        }
        let opens = line.matches('{').count() as i32;
        let closes = line.matches('}').count() as i32;
        current.as_mut().expect("open").2 += opens - closes;
        if current.as_ref().expect("open").2 <= 0 {
            let done = current.take().expect("open");
            found.push((done.0, done.1));
            continue;
        }
        // A top-level field is indented two spaces, optional `?`, then a colon.
        let Some(body) = line.strip_prefix("  ") else {
            continue;
        };
        if body.starts_with(' ') || body.starts_with('*') || body.starts_with('/') {
            continue;
        }
        let field: String = body
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        let after = &body[field.len()..];
        if field.is_empty() {
            continue;
        }
        let after = after.strip_prefix('?').unwrap_or(after);
        if after.trim_start().starts_with(':') {
            current.as_mut().expect("open").1.push(field);
        }
    }
    found
}

/// The production Rust of the whole tree, as one text.
///
/// The sources that ship, and nothing else. A gate's own fixtures spell the names
/// it is looking for — this file's registry names `refreshToken` in order to allow
/// it — so reading `tests/` would let the gate satisfy its own census and pass
/// over a field nobody writes.
fn server_text(sources: &[PathBuf]) -> String {
    let mut text = String::new();
    for path in sources {
        text.push_str(&production_text(&read(path)));
        text.push('\n');
    }
    text
}

/// The `Interface.field` pairs whose field name appears nowhere in `server`.
fn unwritten(client: &str, server: &str) -> Vec<String> {
    let mut missing = Vec::new();
    for (name, fields) in interfaces(client) {
        for field in fields {
            if !server.contains(&field) {
                missing.push(format!("{name}.{field}"));
            }
        }
    }
    missing
}

/// The one shape where a client field name is not a wire key: the login answer
/// is folded from the gateway's flat body into the client's own object, and the
/// renewal credential is renamed on the way through. Each entry carries the line
/// that does the renaming — a field only gets to be a client-side name if the
/// mapping is right there in the module, and the wire key it reads has to be
/// spelled in the tree.
const RENAMED: &[(&str, &str, &str)] = &[("LoginResponse", "refreshToken", "res.refresh_token")];

fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (i, ch) in name.char_indices() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[test]
fn every_field_the_client_declares_is_a_field_the_tree_spells() {
    let root = repo_root();
    let client = read(&root.join(CLIENT));
    let server = server_text(&production_sources(&root));
    let missing = unwritten(&client, &server);

    for (interface, field, mapping) in RENAMED {
        let pair = format!("{interface}.{field}");
        assert!(
            missing.contains(&pair),
            "{pair} 登记成「客户端改名的字段」，但现在服务端已经写了这个字段名——\
             这条登记没在做事了，撤掉它"
        );
        assert!(
            client.contains(mapping),
            "{pair} 登记的改名依据 {mapping:?} 在 {CLIENT} 里找不到了：字段还叫这个名字，\
             可没人再从线上那个键折过来"
        );
        let wire = mapping
            .rsplit('.')
            .next()
            .expect("a mapping names the wire key after the dot");
        assert_eq!(snake_case(field), wire, "{pair} 的字段名折不回 {wire:?}");
        assert!(
            server.contains(wire),
            "{pair} 的线上键 {wire:?} 在本树的 Rust 里没有出处：改名这一说就不成立了，\
             客户端读的是一个谁都不写的键"
        );
    }

    let unregistered: Vec<&String> = missing
        .iter()
        .filter(|pair| {
            !RENAMED
                .iter()
                .any(|(i, f, _)| pair.as_str() == format!("{i}.{f}"))
        })
        .collect();
    assert!(
        unregistered.is_empty(),
        "these response fields are declared in {CLIENT} and their names appear in no production \
         Rust in this tree: {unregistered:?}\n\
         A reader of such a field gets `undefined`, which the panel draws exactly like a value \
         that was never there. Either the endpoint writes a different key (spell the client to \
         match it), or nothing writes this field at all (delete it)."
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let parsed = interfaces(&read(&root.join(CLIENT)));
    assert!(
        parsed.len() >= 140,
        "only {} interfaces parsed out of {CLIENT} — the reader has lost the module, and an \
         empty interface list would pass this gate by checking nothing",
        parsed.len()
    );
    let fields: usize = parsed.iter().map(|(_, fields)| fields.len()).sum();
    assert!(
        fields >= 800,
        "only {fields} fields parsed out of {} interfaces — too few to be the module's real \
         surface",
        parsed.len()
    );
    assert!(
        !server_text(&production_sources(&root)).is_empty(),
        "no production Rust was read at all; every field would then be reported and the gate \
         would fail for the wrong reason"
    );
}

#[test]
fn a_field_no_writer_spells_is_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-field-writer-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("web/src/api")).expect("temp web tree");
    std::fs::create_dir_all(root.join("crates/demo/src")).expect("temp crate tree");
    std::fs::write(
        root.join(CLIENT),
        "export interface Row {\n  known_id: string;\n  ghost_field: number;\n  nested: {\n    \
         deep_name: string;\n  };\n}\n",
    )
    .expect("client");

    // `nested` is a top-level field and is checked; `deep_name` is inside an
    // object literal and is deliberately not.
    std::fs::write(
        root.join("crates/demo/src/lib.rs"),
        "pub struct Row {\n    pub known_id: String,\n    pub nested: Inner,\n}\n",
    )
    .expect("lib");

    assert_eq!(
        unwritten(
            &read(&root.join(CLIENT)),
            &server_text(&sources_of_tree(&root))
        ),
        vec!["Row.ghost_field".to_string()]
    );

    // Spelling the missing key on the server side clears it: the gate holds the
    // name, not the interface.
    std::fs::write(
        root.join("crates/demo/src/lib.rs"),
        "pub struct Row {\n    pub known_id: String,\n    pub ghost_field: f64,\n    pub nested: \
         Inner,\n}\n",
    )
    .expect("lib");
    assert!(unwritten(
        &read(&root.join(CLIENT)),
        &server_text(&sources_of_tree(&root))
    )
    .is_empty());

    let _ = std::fs::remove_dir_all(&root);
}
