//! 等一件活的结果：走到头有完成事件，没走到头只说「没在时限内答」。
//!
//! 从前的形状是「再发一条消息问」：`expect_reply` 是唯一同步等待，等不到就只剩
//! 一句超时，问的人分不出「对方还在跑」与「对方没做完」——前者该再等一会，后者
//! 该去看原因，两件事的下一步相反。
//!
//! 这里钉四段：
//!
//! 1. **等的不是客户端轮询**：`GET /api/v1/tasks/{id}?wait_ms=` 在服务端订阅
//!    任务总线，完成/失败/取消那一条到了就回来——完成是事件，不是「再问一次」；
//! 2. **三种答案三句话**：做完了（带结果）、到时限了（`reason: timeout`，附一句
//!    「可能还在跑」）、到了头但失败了（不叫超时）；
//! 3. **失败的等待也要说清是哪一行**：回执带 `task_id`，问的人拿它去账上读原因；
//! 4. **上限是人的一个决定**：`MAX_TASK_WAIT_MS` 标着来源，工具的默认等待与
//!    上限是同一组数，不另写一遍。
//!
//! 行为那一半在 `cog_agent::tools::builtins::tests` 的
//! `an_ended_wait_and_a_timed_out_wait_are_different_answers`（三种状态词三种答案）；
//! 这里钉接线。

use crate::common::routes::read_repo;

const TOOLS: &str = "crates/cog-agent/src/tools.rs";
const TASKS: &str = "crates/cog-gateway/src/tasks.rs";

/// 工具这一侧：按 id 等，等到的是「走没走到头」，超时明说超时。
#[test]
fn the_agent_waits_on_a_task_by_id_and_reads_an_end_not_a_silence() {
    let tools = read_repo(TOOLS);
    for (needle, why) in [
        ("pub fn await_task(", "没有「等一件活」这只工具"),
        ("\"task_completed\"", "回执不区分「完成」与「没完成」"),
        (
            "receipt[\"reason\"] = serde_json::json!(\"timeout\")",
            "超时没有说出自己是超时：读的人会把「没在时限内答」读成「没做完」",
        ),
        (
            "receipt[\"hint\"]",
            "超时那一句没有解释：这一行可能还在跑，而那正是要不要再等的判据",
        ),
        (
            "\"task_id\": task_id",
            "回执不带 id：拿到了失败也找不到是哪一行",
        ),
    ] {
        assert!(tools.contains(needle), "{TOOLS} 缺 {needle:?}：{why}");
    }
    // 登记在 `read_task_ledger` 之后：先读账再决定等谁，这个顺序就是模型的动作顺序。
    let ledger = tools
        .find("read_task_ledger(inputs.http_client.clone(), inputs.memory_api_base.clone())")
        .expect("台账读口的登记改了形状");
    let await_at = tools
        .find("await_task(inputs.http_client.clone(), inputs.memory_api_base.clone())")
        .expect("定义了这只工具却没有登记：模型看不到它");
    assert!(
        ledger < await_at,
        "{TOOLS} 把等结果排在读账之前：模型会先等一件它还不知道 id 的活"
    );
}

/// 网关这一侧：等待是服务端的一次订阅，不是把轮询交给调用方。
#[test]
fn the_gateway_waits_on_its_own_task_bus_instead_of_handing_the_caller_a_loop() {
    let tasks = read_repo(TASKS);
    for (needle, why) in [
        (
            "pub const MAX_TASK_WAIT_MS",
            "等待的上限没有落成一个有名字的界",
        ),
        (
            "async fn wait_for_task_end(",
            "读口没有「等它走完」这一层：调用方只能自己轮询",
        ),
        (
            "subscribe_task_events()",
            "等的不是任务总线上的事件：那不是完成事件，只是一次更晚的读",
        ),
        (
            "wait.wait_ms",
            "读口不收等待窗口：`?wait_ms=` 会被静默忽略，调用方以为自己在等",
        ),
        (
            "wait_ms.min(MAX_TASK_WAIT_MS)",
            "调用方要多久就等多久：一条请求能把连接占住任意久",
        ),
    ] {
        assert!(tasks.contains(needle), "{TASKS} 缺 {needle:?}：{why}");
    }
    // 界要标出来源：一个裸数字在这里就是「没人知道它是量出来的还是钉死的」。
    let cap = tasks
        .split("pub const MAX_TASK_WAIT_MS")
        .next()
        .expect("上限那一行还在");
    let above = cap.rsplit('\n').take(3).collect::<Vec<_>>().join("\n");
    assert!(
        above.contains("// cap: "),
        "{TASKS} 的上限没有来源标签：它既不是量出来的、也没人看得出是谁钉的"
    );
    // 早就是终局的行不必再等那一拍：读口先看一眼，已经是完成/失败/取消就直接答。
    assert!(
        tasks.contains("fn task_reached_an_end("),
        "{TASKS} 没有「这一行还会不会再变」的判据：已经完的行也要等满窗口"
    );
}
