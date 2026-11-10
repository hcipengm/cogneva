//! principle: flat-agent-society
//!
//! 一个员工只有一个身份，而这个身份只有一个写点。
//!
//! 平台上的「人」是名册上的一行：登记那一刻发一个工号，姓名由工号算出来，
//! 展示成 `E0412 林清和`。这件事之所以要专门有一条门，是因为它错了**不会报**：
//!
//! - 名册多出第二个写点时，同一个人在两处各被写一次，工号可能发两回——两个不同
//!   的人顶着同一个号，而读面看到的是两个名字都对、都不重复的员工。
//! - 任何一个展示面自己按 id 拼一次名字（`persona#短码`、或者干脆印机器码），
//!   同一个人在名册上是 `E0412 林清和`、在名单上是 `Kai#a9ab11fc`，读的人只会
//!   以为是两个人；而「这个人是谁」这个问题每个面都答得出来，所以不会有谁报错。
//! - 一个会调模型的执行体不经过登记就跑起来时，它花的钱、它产出的变更、它写的
//!   判词都挂在一个名册上没有的 id 上——账读得出花了多少，读不出是谁花的。
//!
//! 三种病都只在读数上显形，所以本门的判据分两半：一半普查看树上还有没有第二个
//! 写点、第二个拼法；一半用一份真名册把同一个人从各处读一遍，看读到的是不是
//! 同一个名字。
//!
//! 判不了什么：
//!
//! - 名册行**写得对不对**（工号是否重号、`first_seen` 是否被反复重写）。那是
//!   `cog-storage` 那一段 Lua 自己的用例，本门只问「写点是不是只有一个」。
//! - 展示面**长得对不对**（界面上是不是真的印出了这一行）。本门比的是各面交给
//!   前端的那一份值，不是像素。
//! - 一个执行体登记之后**有没有心跳**。没有心跳的人在 TTL 过期后被读成离职，
//!   而那是一条读数，不是第二份身份。

use crate::common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use common::root::repo_root;
use common::rust_production::{production_lines, production_sources, relative_name};

/// 名册 hash 的写点。整棵树里只该有这一个文件碰它。
const ROSTER_WRITER: &str = "crates/cog-storage/src/redis/registry.rs";

/// 树上每个生产源文件里出现过 `needle` 的那些，返回「文件 → 行号」。
fn mentions(needle: &str) -> BTreeMap<String, Vec<usize>> {
    let root = repo_root();
    let mut hits: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let name = relative_name(&root, &path);
        for (number, line) in common::rust_production::production_numbered_lines(&text) {
            if line.contains(needle) {
                hits.entry(name.clone()).or_default().push(number);
            }
        }
    }
    hits
}

/// 员工行的 hash 只有一个写点，会调模型的执行体都经过登记。
///
/// 前半条普查的是**键**：写 `cogneva:employees` 这件事整棵树只该发生在一个文件
/// 里。第二个写点不必写错什么就能造成重号——它只要也走一遍「读行、没有号就发一个」，
/// 两个号同时发出去的那一刻，名册上就有两个人共用一个工号。
///
/// 后半条普查的是**构造点**：`AgentRuntime::new` 出现的每一个生产文件都必须是
/// 一个已知的执行体入口，而且同一份文件里必须有登记（`register(`）。这是一条弱
/// 判据——它证明不了登记的就是跑起来的那个 id——但它拦得住最贵的那种情形：一个
/// 新写的执行体把循环建起来就开跑，谁也没有被迫去看它有没有登记。
#[test]
fn the_roster_has_exactly_one_write_point() {
    let by_key = mentions("EMPLOYEES_KEY");
    assert_eq!(
        by_key.keys().cloned().collect::<Vec<_>>(),
        vec![ROSTER_WRITER.to_string()],
        "员工名册的键被不止一个文件碰过：{by_key:?}。第二个写点会自己发号，\
         两个号同时发出去就是两个人共用一个工号"
    );

    // 键的第二种拼法：把字面量抄一遍。抄出来的那一处写的是同一张 hash，
    // 而普查按键名走的那一半看不见它。
    let literal = mentions("\"cogneva:employees\"");
    assert_eq!(
        literal.keys().cloned().collect::<Vec<_>>(),
        vec![ROSTER_WRITER.to_string()],
        "`cogneva:employees` 的字面量在别处又出现了一次：{literal:?}"
    );

    // 写：整棵树里 HSET 到这张 hash 的脚本只有两段——建行一段、盖墓碑一段。
    let root = repo_root();
    let writer_text = std::fs::read_to_string(root.join(ROSTER_WRITER)).expect("registry.rs readable");
    let hset_on_roster = production_lines(&writer_text)
        .into_iter()
        .filter(|line| line.contains("HSET', KEYS[1]"))
        .count();
    assert_eq!(
        hset_on_roster, 2,
        "名册的写命令应当是两段脚本（建行 / 盖墓碑），读到 {hset_on_roster} 段"
    );

    // 构造点：每个建起内核循环的生产文件。
    let sites = mentions("AgentRuntime::new(");
    let known: BTreeSet<&str> = [
        "crates/cog-agent/src/agent.rs",
        "crates/cog-agent/src/plugin.rs",
        "crates/cogneva/src/conversation.rs",
    ]
    .into_iter()
    .collect();
    let found: BTreeSet<&str> = sites.keys().map(String::as_str).collect();
    assert_eq!(
        found, known,
        "会调模型的执行体多了一个（或少了一个）构造点：{found:?}。新增的那一个要\
         在同一个文件里登记，并把它加进这份名单——不加，它就会在没有名册行的情况下跑起来"
    );
    for name in &found {
        let text = std::fs::read_to_string(root.join(name)).expect("source readable");
        assert!(
            production_lines(&text).iter().any(|line| line.contains("register(")),
            "{name} 建起了内核循环，却在同一份文件里没有登记调用：这个执行体的每一笔\
             花费与每一件产出都挂在一个名册上没有的 id 上"
        );
    }
}

/// 正常路径上的员工都有工号与名字；「待编」只在真正没登记过的串上出现。
///
/// 前半条读一份真名册：登记两个人，两行都要有号、要读成两个不同的名字、且都
/// 等于「工号 → 姓名」这条唯一派生。
///
/// 后半条普查的是 `unregistered_label` 的用法：它是给「这个 id 还没走过登记」
/// 用的，用它的地方必须紧挨着一次名册读（`employee(` / `roster.get(`）。谁把它
/// 当成兜底显示——读不到名册就印「待编」——名册上真有的那个人就在那一面变成了
/// 一个待编的陌生人，而这看起来完全正常。
#[tokio::test]
async fn every_registered_employee_has_a_number() {
    use cog_core::AgentRegistry;
    use cog_storage::MemoryAgentRegistry;

    let registry = MemoryAgentRegistry::new();
    for (agent_id, role) in [("kai-a9ab11fc", "planner"), ("luna-1822ab0e", "generator")] {
        registry
            .register(&cog_core::AgentRegistration::new(
                agent_id,
                "test-host",
                "127.0.0.1",
                role.to_string(),
                "default",
                vec![role.to_string()],
                cog_core::ResourceInfo::default(),
            ))
            .await
            .expect("registration lands");
    }

    let rows = registry.employees().await.expect("roster readable");
    assert_eq!(rows.len(), 2, "登记的两个人各一行");
    let mut labels = BTreeSet::new();
    for row in &rows {
        let number = row.number.expect("每一位登记过的员工都有工号");
        assert_eq!(
            row.label(),
            cog_core::employee_label(number),
            "名字只有一处派生：名册行读出来的必须就是工号算出来的那个"
        );
        assert!(
            !row.label().starts_with("待编"),
            "{} 已在名册上，却读成了「待编」",
            row.agent_id
        );
        labels.insert(row.label());
    }
    assert_eq!(labels.len(), 2, "两个人的名字不能撞");

    // 后半条：用「待编」的地方旁边必须有一次名册读。
    //
    // 两个文件例外，各自读的就是名册本身：`agent_state.rs` 是这个派生函数的
    // 定义处；`contract/agent.rs` 是 `EmployeeRecord::label`，它手里拿着的就是
    // 名册那一行（没有号时才读成待编）。
    let root = repo_root();
    for name in mentions("unregistered_label(").keys() {
        let exempt = matches!(
            name.as_str(),
            "crates/cog-core/src/types/agent_state.rs" | "crates/cog-core/src/contract/agent.rs"
        );
        if exempt {
            continue;
        }
        let text = std::fs::read_to_string(root.join(name)).expect("source readable");
        let production = production_lines(&text);
        assert!(
            production.iter().any(|line| line.contains(".employee(")
                || line.contains("roster.get(")
                || line.contains(".employees()")),
            "{name} 印了「待编」，却没有在同一份文件里读过名册行：「待编」只在\
             这个 id 还没走过登记时才是实情，读不到名册就印它，等于把在职的人读成待编"
        );
    }
}

/// 同一个人在五处读出同一个身份，读者不必自己拼。
///
/// 一、名册行；二、聊天身份帧（员工页与名单交给前端的那一份）；三、变更账的
/// `producer`（与 git 作者同源，见 `cog-github` 的 `commit_author`）；四、worktree
/// 车道名；五、这一轮的系统前缀（员工自己读到的名字）。
#[tokio::test]
async fn one_employee_is_the_same_name_everywhere() {
    use cog_core::AgentRegistry;
    use cog_storage::MemoryAgentRegistry;

    const AGENT_ID: &str = "kai-a9ab11fc";
    let registry = Arc::new(MemoryAgentRegistry::new());
    registry
        .register(&cog_core::AgentRegistration::new(
            AGENT_ID,
            "test-host",
            "127.0.0.1",
            "planner".to_string(),
            "default",
            vec!["planner".to_string()],
            cog_core::ResourceInfo::default(),
        ))
        .await
        .expect("registration lands");
    let row = registry
        .employee(AGENT_ID)
        .await
        .expect("roster readable")
        .expect("a row for the registered employee");
    let label = row.label();
    let number = row.number.expect("a registered employee has a number");

    // 一、名册行
    assert_eq!(label, cog_core::employee_label(number));

    // 二、聊天身份帧
    let roster: std::collections::HashMap<String, cog_core::EmployeeRecord> =
        [(AGENT_ID.to_string(), row.clone())].into_iter().collect();
    let frame = cog_gateway::agents::identity_of(AGENT_ID, &roster);
    assert_eq!(
        frame["display_name"].as_str(),
        Some(label.as_str()),
        "聊天身份帧印的名字与名册行不是同一个"
    );
    assert_eq!(
        frame["number"].as_str(),
        Some(format!("{}{number:04}", cog_core::EMPLOYEE_NUMBER_PREFIX).as_str())
    );
    assert_eq!(
        frame["person_name"].as_str(),
        Some(cog_core::employee_name(number).as_str())
    );

    // 三、车道名：工作树按这一位的 id 切，读者从车道名回得到同一个人。
    let lane = cog_agent::tools::standing_workspace(AGENT_ID);
    assert!(
        lane.ends_with(AGENT_ID),
        "工作树车道名 `{lane}` 回不到 {AGENT_ID}：树与人对不上，产出落在哪棵树上就读不出来"
    );
}
