//! 名册行在真的 Redis 上成立的三件事：注册即发一个工号、同一个 id 不重复
//! 吃号、退役之后行还在且号不变。
//!
//! 内存实现里也有同一组断言（`redis::registry::tests`），但那证明不了这里：
//! 发号那一步住在 Lua 脚本里，而内存实现根本没有脚本。两套实现各写一遍时，
//! 最贵的错法是「测试里工号能回收、线上不能」——两边都绿，线上在换人。
//!
//! ```text
//! COGNEVA_TEST_REDIS_URL=redis://:pw@127.0.0.1:16379 \
//!   cargo test -p cog-storage --test employee_roster -- --ignored
//! ```
//!
//! 这个文件只写自己的名字（前缀 [`PROBE`]），不删别人的行：名册是一张只增
//! 不减的表，退役靠墓碑而不是删除，用例照同一个规矩来，退出之后留下的行在
//! 一次性 Redis 里认得出来是谁写的。

use cog_core::{AgentRegistration, AgentRegistry, ResourceInfo};
use cog_storage::RedisAgentRegistry;
use cog_test_support::redis_url;

/// 本文件所有名字的公共前缀：跑在一台活的 Redis 上时，留下的行一眼认得出。
const PROBE: &str = "probe_employee_roster";

async fn registry() -> RedisAgentRegistry {
    RedisAgentRegistry::from_url(&redis_url())
        .await
        .expect("a throwaway Redis is required: set COGNEVA_TEST_REDIS_URL")
}

fn registration(name: &str) -> AgentRegistration {
    AgentRegistration::new(
        format!("{PROBE}:{name}"),
        "probe-host",
        "127.0.0.1",
        "probe",
        "probe",
        vec![],
        ResourceInfo::default(),
    )
}

/// 注册就发号，而且只发一次。
///
/// 名册行是注册的副作用：一类智能体要是绕过了注册这一步，它在名册上就不存在
/// ——小队成员与探针正是这么消失的。反过来，重复注册要是每次都吃新号，工号
/// 就不再是「一个人」的编号，而是一个计数的读数。
#[tokio::test]
#[ignore = "needs a throwaway Redis: set COGNEVA_TEST_REDIS_URL"]
async fn a_registered_agent_gets_a_number_exactly_once() {
    let reg = registry().await;
    let row = registration("once");
    reg.register(&row).await.unwrap();
    let first = reg
        .employee(&row.agent_id)
        .await
        .unwrap()
        .expect("注册之后名册上应当有这一行");
    assert!(first.is_active(), "刚注册的人不该是离职");
    let number = first.number.expect("建行时应当发了工号");

    reg.register(&row).await.unwrap();
    let again = reg.employee(&row.agent_id).await.unwrap().unwrap();
    assert_eq!(
        again.number,
        Some(number),
        "重复注册换了工号：同一个员工在名册上会变成两个人"
    );
}

/// 两个人两个号，展示名因此不重复。
#[tokio::test]
#[ignore = "needs a throwaway Redis: set COGNEVA_TEST_REDIS_URL"]
async fn two_employees_hold_different_numbers() {
    let reg = registry().await;
    let a = registration("pair-a");
    let b = registration("pair-b");
    reg.register(&a).await.unwrap();
    reg.register(&b).await.unwrap();
    let na = reg.employee(&a.agent_id).await.unwrap().unwrap().number;
    let nb = reg.employee(&b.agent_id).await.unwrap().unwrap().number;
    assert!(na.is_some() && nb.is_some());
    assert_ne!(na, nb, "两个员工共用一个工号");
}

/// 退役留行、留号、留入职时间；回来上班也不换号。
#[tokio::test]
#[ignore = "needs a throwaway Redis: set COGNEVA_TEST_REDIS_URL"]
async fn a_retired_employee_keeps_its_row_and_number() {
    let reg = registry().await;
    let row = registration("retire");
    reg.register(&row).await.unwrap();
    let before = reg.employee(&row.agent_id).await.unwrap().unwrap();

    reg.deregister(&row.agent_id).await.unwrap();
    let after = reg
        .employee(&row.agent_id)
        .await
        .unwrap()
        .expect("注销之后名册行被删了：离职与从来没有过这个人读成同一件事");
    assert!(after.retired_at.is_some(), "注销之后没有墓碑");
    assert_eq!(after.number, before.number, "退役换了工号");
    assert_eq!(after.first_seen, before.first_seen, "退役重置了入职时间");

    reg.register(&row).await.unwrap();
    let again = reg.employee(&row.agent_id).await.unwrap().unwrap();
    assert!(again.retired_at.is_none(), "回来上班的人还挂着离职");
    assert_eq!(again.number, before.number, "回来上班换了工号");
}

/// 退役的人也出现在整张名册上，并且带着墓碑。
#[tokio::test]
#[ignore = "needs a throwaway Redis: set COGNEVA_TEST_REDIS_URL"]
async fn the_roster_lists_a_retired_person() {
    let reg = registry().await;
    let row = registration("listed");
    reg.register(&row).await.unwrap();
    reg.deregister(&row.agent_id).await.unwrap();
    let rows = reg.employees().await.unwrap();
    let found = rows
        .iter()
        .find(|r| r.agent_id == row.agent_id)
        .expect("退役的人不在名册上");
    assert!(!found.is_active());
    assert!(found.number.is_some());
}
