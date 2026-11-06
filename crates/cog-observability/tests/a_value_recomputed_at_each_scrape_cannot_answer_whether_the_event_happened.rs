//! principle: feedback_a_value_recomputed_at_each_scrape_cannot_answer_whether_the_event_happened
//!
//! 每一拍抓取都按 spec 重算的「投影」永远指着未来（kube-state-metrics 的
//! `next_schedule_time` 是它最出名的样子）：它读的不是事件，而是「按此刻的口径，
//! 下一次大概什么时候会轮到」。所以它答不了「那件事发生过没有」——而且它不会随着
//! 时间流逝变成发生过：下一拍它被重算到新的未来去，而那一格读数自己不会说出这次
//! 重算。
//!
//! 本 crate 声明的这一对读数就在 `src/lib.rs` 里：`LABEL_NEXT_ATTEMPT_UNIX`
//! （`next_attempt_unix`，我们自己的重试节拍，每一拍重算）与
//! `LABEL_EVIDENCED_RECOVERY_UNIX`（`evidenced_recovery_unix`，上游自述的恢复
//! 时刻，也就是事件侧）。三条判据都落在这一对上：
//!
//! * 投影键按形状选出来（`*_unix` 里说的是「下一次」的那些），不是一张手写名单；
//!   每个投影键都必须有事件侧同伴一起声明，单独声明的投影是红的；
//! * 告警出口那一面，带着投影的标签集合里必须同时带着事件侧读数——只带投影的那
//!   一张通知，会让读它的人把「下次什么时候重试」读成「恢复没有」；
//! * 连着两拍读同一个对象：投影向前走了，事件侧一格没动。把这一对喂给进程自己的
//!   消费者 `LlmPoolStatus::resume_wait_secs`，在投影刚过期的那一刻它答的是「没有
//!   落在未来的界，恢复时刻未知」，不是「等到了」——从投影单独推出来的那句「事情
//!   发生了」正是这条原则禁止的形状。
//!
//! 判不了什么：投影的重算本身（那是 `cog-gateway` 的发布循环，不在本 crate 的可
//! 调用面里），以及 kube-state-metrics 那一侧同形的值。这里判的是本 crate 的读数
//! 面：把这格投影读成事件的答案，是这条原则在**这一层**唯一可能发生的走样。

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use cog_core::{metric_names, LlmPoolStatus, MetricName, MetricsBackend};
use cog_test_support::MemoryMetricsBackend;

/// 一个声明的标签键：常量名，与它指向的那段字面量。
type LabelKey = (String, String);

/// 本 crate 声明的标签键，从 `src/lib.rs` 本身读出来。
///
/// 抄一份名单就等于拿抄件当题目：这份判据问的正是「crate 声明了哪些键」。
fn declared_label_keys() -> Vec<LabelKey> {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read the crate root, where the label keys are declared");
    source
        .lines()
        .filter_map(|line| {
            let rest = line.trim().strip_prefix("pub const LABEL_")?;
            let (name, value) = rest.split_once(": &str = \"")?;
            Some((
                format!("LABEL_{name}"),
                value.strip_suffix("\";")?.to_string(),
            ))
        })
        .collect()
}

/// 「每一拍重算的投影」在读数上的形状：一个 unix 时刻，名字说的是「下一次」。
fn is_projection(key: &str) -> bool {
    key.ends_with("_unix") && key.contains("next")
}

/// 把声明的那一列投影键与事件侧键分开，每个键带着它的常量名与取值。
///
/// 按形状选，不按名字点：新增一个投影键而不带事件侧读数，走的就是这条判据。两个都要
/// 带上：形状是**取值**里的（`next_attempt_unix`），而读数面上写的是**常量名**
/// （`crate::LABEL_NEXT_ATTEMPT_UNIX`）——拿取值去面上找，找到的只是零。
fn split_instants(keys: &[LabelKey]) -> (Vec<LabelKey>, Vec<LabelKey>) {
    let mut projections = Vec::new();
    let mut event_side = Vec::new();
    for (name, value) in keys {
        if !value.ends_with("_unix") {
            continue;
        }
        if is_projection(value) {
            projections.push((name.clone(), value.clone()));
        } else {
            event_side.push((name.clone(), value.clone()));
        }
    }
    (projections, event_side)
}

/// 一面读数上带着投影、却没有带着任何事件侧读数时，把那些投影键报出来。
///
/// 每个投影键的同伴是「同一面上的事件侧读数」而不是「某个一定存在的配对」：这棵树
/// 里没有哪处声明说清哪个投影配哪个事件侧，所以这条判据要求的是同一面上至少有一条
/// 事件侧读数——一条投影配一条无关的事件侧读数，仍然比一条孤零零的投影要诚实。
fn projections_sailing_alone(surface: &str, keys: &[LabelKey]) -> Vec<String> {
    let (projections, event_side) = split_instants(keys);
    let carries_event = event_side
        .iter()
        .any(|(name, _)| surface.contains(&format!("crate::{name}")));
    projections
        .into_iter()
        .filter(|(name, _)| surface.contains(&format!("crate::{name}")))
        .filter(|_| !carries_event)
        .map(|(name, _)| format!("crate::{name}"))
        .collect()
}

/// 一个函数体的文本，按括号配对取出来。
///
/// 判据判的是「这一面写下的标签集合」，读整个文件会把别处的出现也算进来。
fn fn_body<'a>(source: &'a str, signature: &str) -> &'a str {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("{signature} 已经不在这份源码里了"));
    let open = start
        + source[start..]
            .find('{')
            .unwrap_or_else(|| panic!("{signature} 没有了函数体"));
    let mut depth = 0usize;
    for (offset, character) in source[open..].char_indices() {
        match character {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[open + 1..open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("{signature} 的函数体括号不配对");
}

/// 一个 unix 时刻与 `cog_core` 文案里那一句同样的写法：两边说的必须是同一个瞬间，
/// 才能从给人读的那句话里认出它。
fn stamp(unix: i64) -> String {
    DateTime::<Utc>::from_timestamp(unix, 0)
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| format!("unix {unix}"))
}

/// 写一格读数，走的是部署里那面后端。
async fn record(backend: &MemoryMetricsBackend, name: MetricName, value: f64) {
    backend
        .record_gauge(name, value, HashMap::new())
        .await
        .expect("部署里的后端起得来，这一格读数就写得下");
}

/// 读回这一面最新的那一格：与读者的读法相同，按标签集合取最新。
async fn latest(backend: &MemoryMetricsBackend, name: &str) -> f64 {
    let mut samples = backend
        .query_gauge_latest(name)
        .await
        .expect("同一面后端的读数读得回来");
    let sample = samples
        .pop()
        .unwrap_or_else(|| panic!("{name} 不在这一拍上"));
    assert!(
        samples.is_empty(),
        "{name} 上带着 {} 个标签集合：这里读到的值会是另一个对象的",
        samples.len()
    );
    sample.value
}

#[test]
fn a_declared_projection_ships_with_an_event_side_reading() {
    let keys = declared_label_keys();
    let (projections, event_side) = split_instants(&keys);

    // 选择器先要有题目：这两个键就是它认出来的形状。
    assert!(
        projections
            .iter()
            .any(|(_, value)| value == "next_attempt_unix"),
        "按形状选出的投影里没有 next_attempt_unix（选到的是 {projections:?}）：\
         这条判据读的已经不是那一格读数了"
    );
    assert!(
        event_side
            .iter()
            .any(|(_, value)| value == "evidenced_recovery_unix"),
        "按形状选出的事件侧读数里没有 evidenced_recovery_unix（选到的是 {event_side:?}）"
    );

    for (projection, _) in &projections {
        assert!(
            !event_side.is_empty(),
            "{projection} 是每一拍按 spec 重算的投影，声明里却没有一条事件侧读数陪着它：\
             只有投影的那一面，读它的人只能拿「下次什么时候重试」回答「那件事发生过没有」"
        );
    }
}

#[test]
fn the_alert_that_carries_the_projection_carries_the_event_reading_with_it() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/plugin.rs"))
        .expect("read the file the alert egress is built in");
    let body = fn_body(&source, "fn supervisor_event_to_alert_events(");
    let keys = declared_label_keys();

    // 两半都要在这一面上：投影说「下次什么时候重试」，事件侧读数才答得出「恢复没有」。
    assert!(
        body.contains("crate::LABEL_NEXT_ATTEMPT_UNIX"),
        "告警出口不再带投影了：这一面说的是池不可用，读它的人得知道下次什么时候重试"
    );
    assert!(
        body.contains("crate::LABEL_EVIDENCED_RECOVERY_UNIX"),
        "告警出口带着投影、却不带事件侧读数：这一张通知里，读它的人只剩下「下次什么时候\
         重试」可看，而那一格是每一拍按我们自己的节拍重算出来的"
    );

    let alone = projections_sailing_alone(body, &keys);
    assert!(
        alone.is_empty(),
        "这些投影到了告警出口却单独出厂：{alone:?}"
    );
}

#[test]
fn the_pairing_rule_reports_a_projection_that_goes_out_alone() {
    let keys = declared_label_keys();

    // 这一条不是判据，是上一条的对照：只带投影的那一面必须被报出来，否则上一条的
    // 空结果只是「没看」。
    let projection_only =
        "labels.insert(crate::LABEL_NEXT_ATTEMPT_UNIX.into(), next_attempt_unix.to_string());";
    assert_eq!(
        projections_sailing_alone(projection_only, &keys),
        vec!["crate::LABEL_NEXT_ATTEMPT_UNIX".to_string()],
        "只带投影的读数面没有被报出来"
    );

    let with_companion = "labels.insert(crate::LABEL_EVIDENCED_RECOVERY_UNIX.into(), \
                          evidenced_recovery_unix.to_string());\n\
                          labels.insert(crate::LABEL_NEXT_ATTEMPT_UNIX.into(), \
                          next_attempt_unix.to_string());";
    assert!(
        projections_sailing_alone(with_companion, &keys).is_empty(),
        "带着事件侧同伴的读数面被报成了单独出厂"
    );
}

#[tokio::test]
async fn two_scrapes_move_the_projection_and_leave_the_event_side_reading_where_it_was() {
    let backend = MemoryMetricsBackend::new();
    let projection = metric_names::LLM_POOL_NEXT_ATTEMPT_UNIX;
    let event_side = metric_names::LLM_POOL_EVIDENCED_RECOVERY_UNIX;
    // 我们自己的重试节拍：投影就是「此刻 + 这一拍」重算出来的那个时刻。
    let cadence = 60i64;
    let first_scrape = 1_800_000_000i64;

    // 第一拍：投影指着下一拍，事件侧是 0——没有上游报过恢复时刻。
    record(&backend, projection, (first_scrape + cadence) as f64).await;
    record(&backend, event_side, 0.0).await;
    let projection_first = latest(&backend, projection.as_str()).await;
    let event_first = latest(&backend, event_side.as_str()).await;
    assert_eq!(projection_first, (first_scrape + cadence) as f64);
    assert_eq!(event_first, 0.0, "没有上游报过恢复时刻，这一格是 0");

    // 第二拍：一拍过去，投影按 spec 重算到新的未来；事件侧没有新事，读数不动。
    record(&backend, projection, (first_scrape + 2 * cadence) as f64).await;
    record(&backend, event_side, event_first).await;
    let projection_second = latest(&backend, projection.as_str()).await;
    let event_second = latest(&backend, event_side.as_str()).await;
    assert_eq!(
        projection_second,
        (first_scrape + 2 * cadence) as f64,
        "两拍之间投影必须向前走：这条判据的题目就是「每一拍按 spec 重算的值」"
    );
    assert_eq!(
        event_second, event_first,
        "投影动了、事件侧一格没动——这一对读数要判的正是这个时刻"
    );

    let status = LlmPoolStatus {
        unavailable: true,
        evidenced_recovery_unix: event_second as i64,
        next_attempt_unix: projection_second as i64,
        pool_size: 1,
        ..Default::default()
    };

    // 每一拍读到它时它都指着未来：一条永远在未来、又永远不落地的读数，答不了
    // 「那件事发生过没有」。
    assert_eq!(
        status.resume_wait_secs(first_scrape + cadence + 1, 0),
        Some((cadence - 1) as u64),
        "刚抓完的那一拍上，投影必须落在未来；否则它就不是「按 spec 重算」出来的"
    );

    // 投影刚过期的那一刻：这是「等到了」与「还不知道」唯一可能被混起来的时刻。
    let observed_at = status.next_attempt_unix + 1;
    assert_eq!(
        status.resume_wait_secs(observed_at, 0),
        None,
        "投影已经过期、事件侧一格没动，进程自己的答案必须是「没有落在未来的界，恢复\
         时刻未知」；把那一格过期的投影读成「等到了」，正是这条原则禁止的那一步"
    );

    let verdict = cog_core::pool_down_verdict(
        1,
        &["upstream-a".to_string()],
        None,
        status.evidenced_recovery_unix,
        status.next_attempt_unix,
        0,
    );
    let projection_stamp = stamp(status.next_attempt_unix);
    assert_eq!(
        verdict.matches(projection_stamp.as_str()).count(),
        1,
        "投影的时刻在给人读的那句话里只能作为「下一次重试」出现一次：{verdict}"
    );
    assert!(
        !verdict.contains(&format!("recovery {projection_stamp}")),
        "这句话把投影说成了恢复时刻：{verdict}"
    );

    // 有事件侧证据时，那句话点名的必须是事件侧那一刻：两种读数在文案里也是两句话。
    let evidenced_at = status.next_attempt_unix - 300;
    let with_evidence = cog_core::pool_down_verdict(
        1,
        &["upstream-a".to_string()],
        None,
        evidenced_at,
        status.next_attempt_unix,
        0,
    );
    assert!(
        with_evidence.contains(stamp(evidenced_at).as_str()),
        "上游自述的恢复时刻没有被点名，读的人分不出「没有证据」与「有证据」：{with_evidence}"
    );
}
