//! 「按动静判停，不按时钟判死」在**调用点**这一侧的判据。
//!
//! 控制律只有一份，在 `crates/cog-core/src/declared_wall.rs`：过了申报的墙只说一句、
//! 继续等，收尾只认直接证据（平台自己写的进度死线、致命等待态、探针自己的预算）。
//! 但一条律挡不住调用点抄近路——`kubectl wait --timeout=600s`、`/usr/bin/timeout 240
//! git clone`、清单里的 `activeDeadlineSeconds` 都不经过那条律，它们照样在钟点上一刀
//! 切掉一件还在动的活。这条门扫的就是这些形状：出现就红，不看它挂在哪个函数、哪份
//! 清单里。
//!
//! 按**出现形式**判，不按裸词判：`--timeout=` 与 `/usr/bin/timeout` 是命令行的实参
//! 形状，不会在一句散文里顺带出现；而判定进程自己的 `--timeout` 旗标（用来接「申报
//! 的期望工期」，过了点只说话）没有等号，不在这条判据里。注释行（`#`、`//`、`*`）
//! 一律跳过——说明「这里曾经是一道钟」的那几行正是要留下的。
//!
//! 不在范围内的：运维 shell 脚本里人工在环的 `--timeout`（那是操作者的手，不是系统的
//! 判词；本门只扫 yaml/yml/tpl），以及 `kubectl wait --for=...` 这种**没有**时限的
//! 等待（它等的是状态，正是这条律要的形状）。

use crate::common;

use common::root::repo_root;
use common::routes::walk;
use common::rust_production::production_sources;
use std::path::Path;

/// 命令行上「到点就杀」的写法。两侧都按子串找：真实调用点不会把旗标跨行拼。
const KILLED_FLAGS: [&str; 2] = ["--timeout=", "/usr/bin/timeout"];

/// 清单里「到点就杀」的字段。
const KILLED_FIELDS: [&str; 1] = ["activeDeadlineSeconds:"];

/// 这一行是不是在说「到点就杀」。返回命中的那个写法，好让判词点名。
fn kills_on_a_clock(line: &str, needles: &[&'static str]) -> Option<&'static str> {
    let trimmed = line.trim_start();
    // 注释里的字不算：两处「这里曾经是 activeDeadlineSeconds」的说明正是要留下的。
    if trimmed.starts_with('#') || trimmed.starts_with("//") || trimmed.starts_with('*') {
        return None;
    }
    needles.iter().copied().find(|n| line.contains(n))
}

fn scan_file(path: &Path, needles: &[&'static str], hits: &mut Vec<String>) {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("读不到 {path:?}: {e}"));
    for (n, line) in text.lines().enumerate() {
        if let Some(needle) = kills_on_a_clock(line, needles) {
            hits.push(format!("{}:{}: {needle}", path.display(), n + 1));
        }
    }
}

/// 遍历 `deploy/` 下的清单与模板。渲染产物 `deploy/rendered/` 也在内：它由 chart 生
/// 成，同一个调用点会在四份 profile 里各出现一次，漏扫它等于放过四分之三的现场。
fn deploy_manifests(root: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    walk(&root.join("deploy"), &["yaml", "yml", "tpl"], &mut out);
    out
}

/// 判据本身先证明自己会响：一条永远不响的门读起来和「一切正常」一模一样。
#[test]
fn the_predicate_names_the_shapes_it_forbids() {
    assert_eq!(
        kills_on_a_clock(r#""--timeout=600s","#, &KILLED_FLAGS),
        Some("--timeout=")
    );
    assert_eq!(
        kills_on_a_clock("  /usr/bin/timeout 240 git clone x", &KILLED_FLAGS),
        Some("/usr/bin/timeout")
    );
    assert_eq!(
        kills_on_a_clock("      activeDeadlineSeconds: 3600", &KILLED_FIELDS),
        Some("activeDeadlineSeconds:")
    );
    // 没有等号的旗标是「申报的期望工期」那一侧，不是杀。
    assert_eq!(
        kills_on_a_clock(r#""--timeout".into(),"#, &KILLED_FLAGS),
        None
    );
    // 注释里的字不算。
    assert_eq!(
        kills_on_a_clock("# 这里曾经是 activeDeadlineSeconds", &KILLED_FIELDS),
        None
    );
    assert_eq!(
        kills_on_a_clock("// kubectl wait --timeout=600s", &KILLED_FLAGS),
        None
    );
}

/// 生产代码里没有任何一处按时钟收尾。
#[test]
fn no_production_call_site_kills_work_on_a_clock() {
    let root = repo_root();
    let mut hits = Vec::new();
    for path in production_sources(&root) {
        // 生产源里只会出现命令行旗标这一种形状；`/usr/bin/timeout` 也在内（代码里
        // 那种写法同样可以是 `Command::new("/usr/bin/timeout")`）。
        scan_file(&path, &KILLED_FLAGS, &mut hits);
    }
    assert!(
        hits.is_empty(),
        "生产代码里还有按钟收尾的调用点；换成 declared_wall 的那条律（过了申报点只说一句，\
         收尾只认直接证据）：\n{}",
        hits.join("\n")
    );
}

/// 清单里没有任何一处按时钟收尾。
#[test]
fn no_manifest_kills_work_on_a_clock() {
    let root = repo_root();
    let files = deploy_manifests(&root);
    assert!(
        files.len() > 10,
        "只扫到 {} 份清单/模板：这份普查没有覆盖 deploy/",
        files.len()
    );
    let mut hits = Vec::new();
    for path in &files {
        scan_file(path, &KILLED_FLAGS, &mut hits);
        scan_file(path, &KILLED_FIELDS, &mut hits);
    }
    assert!(
        hits.is_empty(),
        "清单里还有到点就杀的界；容器自己在动的活由它自己写完，或交给 controller 的\
         progressDeadline：\n{}",
        hits.join("\n")
    );
}
