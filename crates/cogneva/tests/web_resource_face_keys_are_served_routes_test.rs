//! principle: feedback_no_hardcoding_config_surface
//!
//! REST 前缀这份词表也写了两遍：服务端按路径注册路由，客户端在
//! `resourceKey('…')` 里再写一遍「哪个面上的东西被改了」。两边对不上时的
//! 后果和频道名一样安静——网关往 `resource:<前缀>` 发的那一帧没人认，这个面
//! 就永远等不到一次刷新，屏幕上的数只跟着它自己的轮询走。不是每次都能看出来：
//! 轮询还在，界面还在动，只是慢了一拍且没人知道为什么。
//!
//! 判一个方向：客户端订阅的每个面名，服务端必须有一条 `/api/v1/<面名>` 的路由。
//! 少了那条路由，`resource_frame::resource_domain()` 就永远算不出这个面——那个键
//! 是死的。反过来（有路由而没有界面订它）不判：很多前缀是别的系统在读写的，
//! 没有界面订它不是缺陷，判上等于要求每个路由都得配一个界面。
//!
//! 「网关服务什么」只有一份读法（`common::routes`），与「客户端请求的路径是否
//! 有服务端路由」那条判据共用：两边各读一遍，分叉就出在两处都读不到的地方。
//!
//! 实测（2026-10-27）：客户端 20 个面名，网关 227 条路由折出 39 个前缀
//! （去掉 `/api/v1/` 后的第一段），全部对得上，红的一栏为空。消融：把
//! `SettingsView` 的 `resourceKey('quota')` 改成 `resourceKey('quoter')`，报
//! 「服务端没有 /api/v1/quoter 这条路」，改回即绿。
//!
//! 这条判据的已知弱点：某个前缀下只有 GET（只读面）时，键同样永远不会被抬起，
//! 而这里判不出来。「有写路由」要不要判，取决于路由表怎么读——把方法与前缀连起来
//! 读会把嵌套路由读漏，读漏的方向是假红。宁可少判一条，不判错的。

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

mod common;
use common::routes::{read, repo_root, served_paths, strip_comments};

const WEB: &str = "web/src";

fn files_under(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
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
            files_under(&path, ext, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some(ext) {
            out.push(path);
        }
    }
}

/// 服务端认得的前缀：路由表里 `/api/v1/<段>` 的第一段。
fn served_prefixes(root: &Path) -> BTreeSet<String> {
    let mut prefixes = BTreeSet::new();
    for path in served_paths(root) {
        let Some(rest) = path.strip_prefix("/api/v1/") else {
            continue;
        };
        let segment = rest.split('/').next().unwrap_or("");
        if !segment.is_empty() && segment != "{}" {
            prefixes.insert(segment.to_string());
        }
    }
    prefixes
}

/// 客户端写下的面名：`resourceKey('…')` 里的那个字面量。
fn face_keys(text: &str) -> BTreeSet<String> {
    let clean = strip_comments(text);
    let mut keys = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = clean[cursor..].find("resourceKey('") {
        let start = cursor + at + "resourceKey('".len();
        let Some(len) = clean[start..].find('\'') else {
            break;
        };
        keys.insert(clean[start..start + len].to_string());
        cursor = start + len;
    }
    keys
}

fn all_face_keys(root: &Path) -> BTreeSet<String> {
    let mut files = Vec::new();
    files_under(&root.join(WEB), "ts", &mut files);
    files_under(&root.join(WEB), "tsx", &mut files);
    let mut keys = BTreeSet::new();
    for path in files {
        keys.extend(face_keys(&read(&path)));
    }
    keys
}

fn contradictions(root: &Path) -> (Vec<String>, usize) {
    let prefixes = served_prefixes(root);
    let keys = all_face_keys(root);
    let mut complaints = Vec::new();
    for key in &keys {
        if !prefixes.contains(key) {
            complaints.push(format!(
                "客户端订了 resourceKey('{key}') 这个面，而服务端没有 /api/v1/{key} 这条路：\
                 resource_domain() 永远算不出这个面，这一面等的那一帧永远不会来，界面只能靠轮询"
            ));
        }
    }
    let judged = keys.len();
    (complaints, judged)
}

#[test]
fn every_face_a_client_watches_is_a_prefix_the_gateway_serves() {
    let (complaints, judged) = contradictions(&repo_root());
    assert!(judged > 0, "一个面名都没判到，这条判据没有题目");
    assert!(
        complaints.is_empty(),
        "REST 前缀这份词表两边写的不是同一份：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let prefixes = served_prefixes(&root);
    assert!(
        prefixes.len() >= 20,
        "只读到 {} 个前缀，太少：路由的写法变了，这条判据会靠「找不到题目」通过",
        prefixes.len()
    );
    let keys = all_face_keys(&root);
    assert!(
        keys.len() >= 12,
        "只读到 {} 个面名，太少：`resourceKey(...)` 的写法变了",
        keys.len()
    );
}

#[test]
fn the_reader_sees_multiline_routes_and_ignores_strings_that_are_not_routes() {
    let root = std::env::temp_dir().join(format!("cogneva-face-reader-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("temp gateway");
    std::fs::write(
        root.join("crates/cog-gateway/src/lib.rs"),
        "fn router() -> Router {\n    Router::new()\n        .route(\"/api/v1/tasks\", post(create))\n\
         \x20       .route(\n            \"/api/v1/raw_logs/{id}/url\",\n            get(url),\n\
         \x20       )\n}\n\nfn other() {\n    // 不是路由，只是提到一条路径。\n\
         \x20   let _ = \"/api/v1/ghost\";\n    let _ = \"/health\";\n}\n",
    )
    .expect("lib");

    assert_eq!(
        served_prefixes(&root),
        BTreeSet::from(["raw_logs".to_string(), "tasks".to_string()])
    );

    let _ = std::fs::remove_dir_all(&root);

    let client = "\
useKindTickedRefresh([resourceKey('settings'), 'agents'], refresh);
// 注释里的 resourceKey('ghost') 不算。
";
    assert_eq!(face_keys(client), BTreeSet::from(["settings".to_string()]));
}

#[test]
fn a_face_key_the_router_does_not_serve_is_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-face-keys-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("temp gateway");
    std::fs::create_dir_all(root.join(WEB)).expect("temp web");

    std::fs::write(
        root.join("crates/cog-gateway/src/lib.rs"),
        "fn r() -> Router { Router::new().route(\"/api/v1/settings\", post(save)) }\n",
    )
    .expect("lib");
    std::fs::write(
        root.join(WEB).join("view.tsx"),
        "useKindTickedRefresh([resourceKey('settings'), resourceKey('setting')], refresh);\n",
    )
    .expect("view");

    let (complaints, judged) = contradictions(&root);
    assert_eq!(judged, 2, "{complaints:?}");
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(
        complaints[0].contains("resourceKey('setting')"),
        "{complaints:?}"
    );

    std::fs::write(
        root.join(WEB).join("view.tsx"),
        "useKindTickedRefresh([resourceKey('settings')], refresh);\n",
    )
    .expect("view");
    let (complaints, _) = contradictions(&root);
    assert!(complaints.is_empty(), "{complaints:?}");

    let _ = std::fs::remove_dir_all(&root);
}
