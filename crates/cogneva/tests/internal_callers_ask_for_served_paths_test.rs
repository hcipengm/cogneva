//! principle: feedback_no_hardcoding_config_surface
//!
//! 网关的 REST 路径还有第三类调用者：平台自己的进程。评估面从 `cogneva` 起任务、
//! 读轨迹，记忆面从 `cog-agent` 写归档，进化面从 `cog-reflection` 取指标——它们
//! 各自把 `/api/v1/…` 拼一遍，而中间没有任何东西编译两边。改了一边，另一边拿到
//! 的是 404，调用点上把它记成一次失败的重试，界面上是「那块数据今天没更新」。
//!
//! 判一个方向：除网关自己与 `cog-core` 之外，生产代码里出现的每一条
//! `/api/v1/…` 字面量，都必须是网关注册过的一条路由（形状比对：`{id}` 与
//! `format!` 里的占位都算一个通配段）。
//!
//! 为什么排除这两处：
//! - `cog-gateway` 既是服务端也是别的服务的客户端（K8s 的 `/api/v1/namespaces/…`、
//!   Prometheus 的 `/api/v1/query`）。那些路径不是网关服务的，判进来只会一片假红。
//! - `cog-core` 里是路由**声明**（`MEMORY_INGEST_PATH` 之类），注册处用的就是它，
//!   不是一次调用。
//!
//! 只读生产那一半：`cog-agent` 的文件里有一个测试替身按路径分派，评测面里起过
//! 一个假的服务端——它们写下的路径是夹具，不是谁会发出的请求。
//!
//! 实测（2026-10-27）：生产侧 5 条路径（任务、轨迹、任务指标、记忆原始列表与
//! 单条），全部落在网关注册过的路由上。消融：把评估面的 `/api/v1/traces` 改成
//! `/api/v1/trace`，本门红并点名；改回即绿。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;
use common::routes::{normalize, read, repo_root, served_paths};
use common::rust_production::production_text;
use common::rust_production::{crate_files_of, production_sources, relative_name};

const API_ROOT: &str = "/api/v1/";
const SERVER: &str = "crates/cog-gateway/";
const DECLARATIONS: &str = "crates/cog-core/";

/// 文件 → 文件里出现的 `/api/v1/…` 形状。
fn asked_paths(text: &str) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find(&format!("\"{API_ROOT}")) {
        let start = cursor + at + 1;
        let Some(len) = text[start..].find('"') else {
            break;
        };
        let literal = &text[start..start + len];
        cursor = start + len;
        paths.insert(normalize(literal));
    }
    paths
}

fn callers(root: &Path, files: &[PathBuf]) -> (BTreeMap<String, BTreeSet<String>>, usize) {
    let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut scanned = 0;
    for path in files {
        let label = relative_name(root, path);
        if label.starts_with(SERVER) || label.starts_with(DECLARATIONS) {
            continue;
        }
        scanned += 1;
        let paths = asked_paths(&production_text(&read(path)));
        if !paths.is_empty() {
            found.insert(label, paths);
        }
    }
    (found, scanned)
}

fn contradictions(root: &Path, files: &[PathBuf]) -> (Vec<String>, usize) {
    let served = served_paths(root);
    let (found, _) = callers(root, files);
    let mut complaints = Vec::new();
    let mut judged = 0;
    for (file, paths) in &found {
        for path in paths {
            judged += 1;
            if !served.contains(path) {
                complaints.push(format!(
                    "{file} 请求 {path}，而网关没有注册这条路（形状比对）：调用点上拿到的是 \
                     404，被记成一次失败的重试；两边各拼一遍路径，中间没有东西比对"
                ));
            }
        }
    }
    (complaints, judged)
}

#[test]
fn every_path_an_internal_caller_asks_for_is_a_route_the_gateway_serves() {
    let root = repo_root();
    let (complaints, judged) = contradictions(&root, &production_sources(&root));
    assert!(judged > 0, "一条内部调用都没判到，这条判据没有题目");
    assert!(
        complaints.is_empty(),
        "平台自己的进程请求了网关没有的路由：\n{}\n\
         要那条路由，就把它注册上；不要的话，把调用删掉——不要留一条永远 404 的请求。",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let (found, scanned) = callers(&root, &production_sources(&root));
    assert!(scanned >= 20, "只扫到 {scanned} 个源文件，读取器看错地方了");
    // 今天只有一处：评估面。记忆面的调用走 `cog_core` 里的路径常量，字面量不在
    // 调用方这一侧——那不是漏读，是那份词表本来就只有一处。
    assert!(
        !found.is_empty(),
        "一个内部调用方都没读到：调用写法变了，这条判据会靠「找不到题目」通过"
    );
    let total: usize = found.values().map(BTreeSet::len).sum();
    assert!(
        total >= 4,
        "只读到 {total} 条内部调用路径，太少：调用写法变了"
    );
}

#[test]
fn the_reader_sees_literals_shapes_and_ignores_fixtures() {
    let production = "\
async fn call(&self) -> Result<()> {
    self.call(\"POST\", \"/api/v1/tasks\", Some(body)).await?;
    let _ = format!(\"/api/v1/tasks/{task_id}\");
    let _ = format!(\"/api/v1/traces?limit={LIMIT}\");
    Ok(())
}
";
    assert_eq!(
        asked_paths(production),
        BTreeSet::from([
            "/api/v1/tasks".to_string(),
            "/api/v1/tasks/{}".to_string(),
            "/api/v1/traces".to_string(),
        ])
    );

    let fixture = "\
fn handle(path_only: &str) {
    let _ = path_only.strip_prefix(\"/api/v1/ghost/\");
}

#[cfg(test)]
mod tests {
    #[test]
    fn fixture() {
        let _ = \"/api/v1/ghost\";
    }
}
";
    // 测试那一半写下的路径是夹具；生产那一半的 `strip_prefix` 仍然是一条路径形状。
    assert_eq!(
        asked_paths(&production_text(fixture)),
        BTreeSet::from(["/api/v1/ghost".to_string()])
    );
}

#[test]
fn a_caller_path_the_gateway_does_not_serve_is_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-internal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("temp gateway");
    std::fs::create_dir_all(root.join("crates/cog-eval/src")).expect("temp caller");

    std::fs::write(
        root.join("crates/cog-gateway/src/lib.rs"),
        "fn r() -> Router { Router::new().route(\"/api/v1/tasks\", post(create)) }\n",
    )
    .expect("server");
    std::fs::write(
        root.join("crates/cog-eval/src/lib.rs"),
        "fn f() { let _ = \"/api/v1/tasks\"; let _ = \"/api/v1/trace\"; }\n",
    )
    .expect("caller");

    let (complaints, judged) = contradictions(&root, &crate_files_of(&root));
    assert_eq!(judged, 2, "{complaints:?}");
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("/api/v1/trace"), "{complaints:?}");

    std::fs::write(
        root.join("crates/cog-eval/src/lib.rs"),
        "fn f() { let _ = \"/api/v1/tasks\"; }\n",
    )
    .expect("caller");
    let (complaints, _) = contradictions(&root, &crate_files_of(&root));
    assert!(complaints.is_empty(), "{complaints:?}");

    let _ = std::fs::remove_dir_all(&root);
}
