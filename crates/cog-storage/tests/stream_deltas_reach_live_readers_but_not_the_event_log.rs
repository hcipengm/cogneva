//! 流式增量要送到实时订阅方，但不进事件表。
//!
//! 一个增量是「这条消息又长了一小段」，不是一件发生过的事。它按 token 生成，是
//! 事件表里条数最多的一类（集群库实测 2026-11-04：全部事件里八成四是它），而它的
//! 读者只有一个——正在看这段流的人。落到表里没有第二个人读：历史查询按「最近一天
//! 的前一万条」取，被增量填满时这个窗口只剩最后几十秒，别的任何事都翻不上来。
//!
//! 所以这条通路有两条腿，缺一条这条判据就没意义：
//! - 表里**没有**它，否则条数与读窗口的挤占都还在；
//! - 广播里有它，否则「不落库」变成了「不发送」，实时视图会静默失去流式输出。
//!
//! 只能对着真库问：判据落在表的一行上，内存实现里没有这张表。
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test stream_deltas_reach_live_readers_but_not_the_event_log -- --ignored
//! ```

use chrono::Utc;
use cog_core::{AgentEvent, AssistantMessageEvent, EventFilter, Message, ObservabilityGateway};
use cog_storage::PostgresObservabilityGateway;
use futures::StreamExt;
use sqlx::PgPool;

use cog_test_support::database_url;

/// 一次等多久算「没送到」。广播是本进程内的通道，正常是微秒级；给足一秒只为
/// 让慢机器上的调度误差不算失败。
// cap: bound — 同进程广播的等待上限，只用来区分「没送到」与「慢一拍」
const LIVE_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

fn turn_end(agent_id: &str) -> AgentEvent {
    AgentEvent::MessageEnd {
        agent_id: agent_id.to_string(),
        message: Message::Assistant {
            content: Vec::new(),
            tool_calls: None,
            usage: None,
            timestamp: Utc::now(),
        },
        timestamp: Utc::now(),
    }
}

fn delta(agent_id: &str, text: &str) -> AgentEvent {
    AgentEvent::MessageUpdate {
        agent_id: agent_id.to_string(),
        assistant_event: AssistantMessageEvent::TextDelta {
            content_index: 0,
            delta: text.to_string(),
            timestamp: Utc::now(),
        },
        timestamp: Utc::now(),
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_stream_delta_is_broadcast_but_not_written_to_the_event_log() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let gateway = PostgresObservabilityGateway::new(pool.clone());
    gateway.init_schema().await.expect("init the schema");

    let marker = format!(
        "stream-delta-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );

    // 先订阅再发布：这条腿要证明的是「广播仍然带着它」，历史重放不在判据里。
    let filter = EventFilter {
        agent_id: Some(marker.clone()),
        ..Default::default()
    };
    let mut live = gateway
        .subscribe_events(filter)
        .await
        .expect("subscribe to the live stream");

    let publisher: &dyn ObservabilityGateway = &gateway;
    publisher.publish_event(delta(&marker, "一"));
    publisher.publish_event(delta(&marker, "二"));
    publisher.publish_event(turn_end(&marker));

    // 腿一：广播里有增量。两条都要到，缺一条就说明这段流断了。
    let mut seen_deltas = 0;
    while seen_deltas < 2 {
        let next = tokio::time::timeout(LIVE_WAIT, live.next())
            .await
            .expect("the live stream went quiet: a stream delta was never broadcast");
        match next {
            Some(Ok(AgentEvent::MessageUpdate { .. })) => seen_deltas += 1,
            Some(Ok(_)) => {}
            Some(Err(e)) => panic!("the live stream failed: {e}"),
            None => panic!("the live stream ended before the deltas arrived"),
        }
    }

    // 腿二：表里有收尾那一条（说明这条通路确实在写），但没有增量。
    let mut rows = (0i64, 0i64);
    for _ in 0..200 {
        rows = sqlx::query_as::<_, (i64, i64)>(
            "SELECT count(*) FILTER (WHERE event_type = 'message_end'), \
                    count(*) FILTER (WHERE event_type = 'message_update') \
             FROM cog_observability_events WHERE agent_id = $1",
        )
        .bind(&marker)
        .fetch_one(&pool)
        .await
        .expect("count the rows this run wrote");
        if rows.0 == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // 先清理，再断言：判据红了也要把库留在接手时的样子。
    sqlx::query("DELETE FROM cog_observability_events WHERE agent_id = $1")
        .bind(&marker)
        .execute(&pool)
        .await
        .expect("clean up the rows this run wrote");

    assert_eq!(
        rows.0, 1,
        "收尾事件没落表：这条通路不是「不写增量」，是「什么都不写」"
    );
    assert_eq!(
        rows.1, 0,
        "流式增量又被写进事件表了：它是按 token 生成的那一类，占满了表里八成四的\
         行，历史查询的「最近一天的前一万条」会只剩最后几十秒"
    );
}
