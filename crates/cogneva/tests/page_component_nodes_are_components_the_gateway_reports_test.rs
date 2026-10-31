//! principle: feedback_no_hardcoding_config_surface
//!
//! 运维页的拓扑面板画一组固定节点：应用本体、安全网关、沙盒执行器、进化沙箱、
//! 镜像仓。节点的状态来自 `/api/v1/cluster/components`：网关把
//! `ComponentStatus { id, status }` 按 id 交给页面，页面按 id 覆盖自己那张表的初值，
//! 再按 id 把这几行画出来。两边各写一遍这组 id：写它们的是
//! `cluster_components_handler`（`id: "main"` 一个字面量，其余来自 `READ_COMPONENTS`
//! 引用的 `cog_core::topology::COMPONENT_*` 常量），读它们的是运维页。具体几个不写死在
//! 这段说明里——它就是会随组件增减而变的那件事，判据读的是两边实际写了什么。
//!
//! 对不上时整条链路都不报错，两个方向都是静的：
//!
//! - 页面读的 id 没人发：那一行永远停在初值 `unknown`，`componentToStatus` 把它画成
//!   灰色 idle——与「这个组件确实没在跑」同一种读数。
//! - 网关发的 id 页面不画：一个真实存在的组件在拓扑图上没有位置，图上少一行与
//!   「这一版就发了这些」同一种读数。
//!
//! 所以两个方向都判，且都以「读到几个」自证：读不出组件表、读不出页面那组 id、
//! 或者读出来是空的，都按红处理。
//!
//! 实测（2026-10-27，当时发 3 个 id）：网关发 `main`、`security-gateway`、`evolution`，
//! 页面写 3 个、读 3 个，两个方向都为空。消融：把 `cluster_components_handler` 里
//! `id: "main"` 改成 `id: "core"`，本门红并同时点名两个方向（页面画的 `main` 没人发、
//! 网关发的 `core` 没人画）；改回即绿。2026-10-31 起组件表加了沙盒执行器与镜像仓，
//! 判据不变，两个方向仍为空。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::common;

use common::js_literal::{top_level_keys, KeyWords};
use common::lex::{split_top_level, Dialect};
use common::routes::{matching_brace, matching_delimiter, read, repo_root, strip_comments};
use common::rust_production::production_text;
use common::rust_text::rust_function_body;

const PAGE: &str = "crates/cog-gateway/src/takeover.html";
const HANDLER_FILE: &str = "crates/cog-gateway/src/cluster.rs";
const HANDLER: &str = "cluster_components_handler";
const COMPONENT_TABLE: &str = "READ_COMPONENTS";
const TOPOLOGY: &str = "crates/cog-core/src/topology.rs";
/// The page's map from component id to its last reported status.
const NODES: &str = "liveComponents";

/// The value of every `pub const NAME: &str = "…";` in a source.
fn constants(source: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in source.lines() {
        let Some(rest) = line.trim().strip_prefix("pub const ") else {
            continue;
        };
        let Some((name, tail)) = rest.split_once(':') else {
            continue;
        };
        let tail = tail.trim();
        let Some(tail) = tail.strip_prefix("&str") else {
            continue;
        };
        let Some(value) = tail.trim().strip_prefix('=') else {
            continue;
        };
        let value = value.trim();
        let Some(value) = value.strip_prefix('"') else {
            continue;
        };
        if let Some(end) = value.find('"') {
            out.insert(name.trim().to_string(), value[..end].to_string());
        }
    }
    out
}

/// The ids the gateway reports, or `None` when either half cannot be read: the
/// handler's own literals, and the topology constants the component table names.
fn reported(root: &Path) -> Option<BTreeSet<String>> {
    let handler = rust_function_body(&production_text(&read(&root.join(HANDLER_FILE))), HANDLER)?;
    let table_text = production_text(&read(&root.join(HANDLER_FILE)));
    let table = &table_text[table_text.find(&format!("const {COMPONENT_TABLE}"))?..];
    let table = &table[..table.find("];")? + 2];
    let constants = constants(&production_text(&read(&root.join(TOPOLOGY))));

    let mut ids = BTreeSet::new();
    // `id: "main"` — the component the handler builds itself.
    let mut cursor = 0;
    while let Some(at) = handler[cursor..].find("id: \"") {
        let start = cursor + at + "id: \"".len();
        let end = handler[start..].find('"')?;
        ids.insert(handler[start..start + end].to_string());
        cursor = start + end;
    }
    // `cog_core::topology::COMPONENT_…` — the first element of each pair in the
    // table is the component id; the second is the workload it is read from, a
    // different vocabulary that happens to sit in the same tuple.
    let body = table.split("= &[").nth(1)?;
    let body = &body[..body.rfind(']')?];
    for item in split_top_level(body, Dialect::Rust) {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let item = item.strip_prefix('(')?.trim();
        let item = item.strip_suffix(')').unwrap_or(item).trim();
        let first = split_top_level(item, Dialect::Rust)
            .into_iter()
            .next()?
            .trim()
            .to_string();
        let name = first.rsplit("topology::").next()?;
        if name == first {
            return None;
        }
        let name = name.trim_end_matches(',').trim();
        // A name the topology file does not spell is a name this reader cannot
        // resolve; guessing it would let a renamed constant pass.
        ids.insert(constants.get(name)?.clone());
    }
    if ids.is_empty() {
        return None;
    }
    Some(ids)
}

/// The component ids the page writes into its map and reads back out of it.
/// `None` when the map cannot be enumerated — a spread or a computed key stands
/// for ids this reader never sees, and guessing them is how a ghost id passes.
fn drawn(root: &Path) -> Option<(BTreeSet<String>, BTreeSet<String>)> {
    let page = strip_comments(&read(&root.join(PAGE)));
    let at = page.find(&format!("let {NODES} = {{"))?;
    let brace = at + page[at..].find('{')?;
    let end = matching_brace(&page, brace, Dialect::JavaScript)?;
    // 这张表的键就是组件 id：`security-gateway` 这类带横杠的词不是标识符，而这里
    // 正是它们住的地方——所以方言由调用点说明，读取器只有一份。
    let written = top_level_keys(&page[brace + 1..end], KeyWords::Any)?;

    let mut read_back = BTreeSet::new();
    let mut cursor = end;
    while let Some(offset) = page[cursor..].find(NODES) {
        let start = cursor + offset;
        cursor = start + NODES.len();
        let tail = &page[cursor..];
        if let Some(rest) = tail.strip_prefix('.') {
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                read_back.insert(name);
            }
        } else if let Some(rest) = tail.strip_prefix("['") {
            let end = rest.find('\'')?;
            read_back.insert(rest[..end].to_string());
        } else if tail.starts_with('[') {
            // A computed key: `liveComponents[c.id] = c.status` is the page
            // learning an id, and the id it writes is not a name this gate has
            // to read. A computed key on the *reading* side is a name it never
            // sees, and guessing it is how a ghost id passes.
            let close = matching_delimiter(&page, cursor, b'[', b']', Dialect::JavaScript)?;
            let after = page[close + 1..].trim_start();
            let assignment = after.starts_with('=') && !after.starts_with("==");
            if !assignment {
                return None;
            }
            cursor = close + 1;
        }
    }
    if written.is_empty() || read_back.is_empty() {
        return None;
    }
    Some((written, read_back))
}

fn contradictions(root: &Path) -> Vec<String> {
    let mut complaints = Vec::new();
    let Some(reported) = reported(root) else {
        complaints.push(
            "读不出网关发的组件 id：要么处理函数或组件表改了名，要么常量的写法变了。\
             读不出来的判据判不了任何东西，所以按红处理"
                .to_string(),
        );
        return complaints;
    };
    let Some((written, read_back)) = drawn(root) else {
        complaints.push(
            "读不出页面那张组件表：要么 `let liveComponents = { … }` 的形状变了，要么里面\
             有了展开或计算键。读不出来同样按红处理"
                .to_string(),
        );
        return complaints;
    };
    for (what, ids) in [("写的", &written), ("读的", &read_back)] {
        for id in ids {
            if reported.contains(id) {
                continue;
            }
            complaints.push(format!(
                "  页面{what}组件 id `{id}`，而网关从不发这个 id（它发 {reported:?}）：\
                 这一行永远停在初值 unknown，画出来是灰色 idle——与「这个组件没在跑」\
                 同一种读数"
            ));
        }
    }
    for id in &reported {
        if written.contains(id) {
            continue;
        }
        complaints.push(format!(
            "  网关发组件 id `{id}`，而页面不画它（页面画 {written:?}）：一个真实存在的\
             组件在拓扑图上没有位置，图上少一行与「本来就没有这个组件」同一种读数"
        ));
    }
    complaints
}

#[test]
fn every_component_node_is_one_the_gateway_reports_and_every_reported_one_is_drawn() {
    let complaints = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "运维页的拓扑节点与网关发的组件 id 对不上。对不上的那条路不会报错，只会一直\
         画成灰色或者干脆不画：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let reported = reported(&root).expect("读不出网关发的组件 id");
    assert!(
        reported.len() >= 3,
        "网关只读出 {} 个组件 id，太少：{reported:?}",
        reported.len()
    );
    let (written, read_back) = drawn(&root).expect("读不出页面的组件表");
    assert!(
        written.len() >= 3 && read_back.len() >= 3,
        "页面写 {} 个、读 {} 个组件 id，太少",
        written.len(),
        read_back.len()
    );
}

/// 一棵最小的树：网关发两个组件，页面也画这两个。用例只改其中一处。
fn fixture(root: &Path, page: &str, handler_id: &str) {
    let files = [
        (
            HANDLER_FILE,
            format!(
                "\
const READ_COMPONENTS: &[(&str, &str)] = &[(
    cog_core::topology::COMPONENT_SECURITY_GATEWAY,
    cog_core::topology::SECURITY_GATEWAY,
)];

pub async fn cluster_components_handler() -> Json<ClusterComponents> {{
    let mut components = vec![ComponentStatus {{
        id: \"{handler_id}\".to_string(),
        status: \"up\".to_string(),
        ready_replicas: None,
        desired_replicas: None,
    }}];
    Json(ClusterComponents {{ components }})
}}
"
            ),
        ),
        (
            TOPOLOGY,
            "\
pub const SECURITY_GATEWAY: &str = \"cogneva-security-gateway\";
pub const COMPONENT_SECURITY_GATEWAY: &str = \"security-gateway\";
"
            .to_string(),
        ),
        (PAGE, page.to_string()),
    ];
    for (relative, body) in files {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
        std::fs::write(path, body).expect("write");
    }
}

const FIXTURE_PAGE: &str = "\
let liveComponents = { main: 'up', 'security-gateway': 'unknown' };
function draw() {
  row(liveComponents.main);
  row(liveComponents['security-gateway']);
}
";

#[test]
fn the_fixture_is_green_before_any_id_is_changed() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path(), FIXTURE_PAGE, "main");
    let complaints = contradictions(dir.path());
    assert!(complaints.is_empty(), "{complaints:?}");
}

#[test]
fn an_id_the_gateway_never_sends_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let page = FIXTURE_PAGE.replace("liveComponents.main", "liveComponents.core");
    assert_ne!(page, FIXTURE_PAGE);
    fixture(dir.path(), &page, "main");
    let complaints = contradictions(dir.path());
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("core"), "{complaints:?}");
}

#[test]
fn an_id_the_page_never_draws_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path(), FIXTURE_PAGE, "core");
    let complaints = contradictions(dir.path());
    assert!(!complaints.is_empty(), "网关发的 core 没人画，本门没说话");
    assert!(
        complaints.iter().any(|c| c.contains("core")),
        "{complaints:?}"
    );
}

#[test]
fn a_page_table_that_cannot_be_read_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(
        dir.path(),
        "let liveComponents = { ...lastKnown, main: 'up' };\nfunction draw() { row(liveComponents.main); }\n",
        "main",
    );
    let complaints = contradictions(dir.path());
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("按红处理"), "{complaints:?}");
}
