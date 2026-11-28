//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//!
//! 事件流每天进了多少行，**按 kind 分**。
//!
//! 「流在不在动」今天只有一个间接回答：某一块面板上的数变了没有。可流停了与流空了
//! 长得一模一样——而它们要做的活相反（一个是写入口坏了，一个是本来就没人写）。这条
//! 读数是 `/api/v1/governance/changes` 与告警之前的那一格：哪一个 kind 一天一行都
//! 没有，而且它本来该有。
//!
//! 三条口径：
//!
//! - **闭集里的每一条都要印**，没有行的那一条印 0。少印一条，与「这一条有 0 行」在
//!   读数面上同形，而读者对一个消失的序列不会下任何结论——它看起来像没人配。
//! - **库里出现过的也印**：闭集之外的老 kind（更早的构建写下的）照样有行，漏掉它们
//!   等于把「这条流还在被旧版本写」这个事实藏起来。
//! - 窗口就是名字：这是「每天」的进量，天数不是旋钮。

use std::collections::BTreeMap;
use std::sync::Arc;

use sqlx::PgPool;

use cog_core::KINDS;
use cog_core::{metric_names, MetricsBackend, SFResult, ShutdownSignal};

/// 这条 loop 在存活读数里的名字。
pub const LOOP: &str = "storage_event_stream_census";

/// 进量按哪个窗口数。
// cap: fixed —— 读数叫「每天」，窗口就是这个名字，不是可调的界
// not-a-bound: — 读数叫「每天」，窗口就是这个名字，不是可调的界
pub const WINDOW_SECONDS: i64 = 86_400;

/// 这个 loop 多久问一次。问得比窗口短得多：窗口是「最近一天」，而它回答的是
/// 「刚刚这一拍还是不是 0」。
// cap: fixed —— 一拍的长度，由「面板多久刷一次」给定（20 秒那一档），不是策略
// keep: 删掉后一拍与面板刷新脱钩，面板每次刷新看到的不是同一段；由「面板多久刷一次」兜住
pub const PASS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// 一拍的结果：每个 kind 在窗口里进了多少行。
///
/// 拆成纯函数，好让「闭集里没有行的那一条印 0」这件事有一条不要数据库的用例。
pub fn census_cells(counts: &BTreeMap<String, u64>) -> Vec<(String, f64)> {
    let mut cells: BTreeMap<String, f64> = KINDS
        .iter()
        .map(|kind| ((*kind).to_string(), 0.0))
        .collect();
    for (kind, rows) in counts {
        cells.insert(kind.clone(), *rows as f64);
    }
    cells.into_iter().collect()
}

/// 读一次窗口里的行数，按 kind。
pub async fn read_window(pool: &PgPool, window_seconds: i64) -> SFResult<BTreeMap<String, u64>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT kind, count(*) AS rows FROM cogneva_event \
         WHERE at > now() - make_interval(secs => $1) \
         GROUP BY kind",
    )
    .bind(window_seconds as f64)
    .fetch_all(pool)
    .await
    .map_err(|e| cog_core::SFError::Database(format!("count the event stream by kind: {e}")))?;
    Ok(rows
        .into_iter()
        .map(|(kind, rows)| (kind, rows.max(0) as u64))
        .collect())
}

/// 一拍：读一次，把每一格写进读数。
pub async fn pass(pool: &PgPool, metrics: &Arc<dyn MetricsBackend>) -> SFResult<usize> {
    let counts = read_window(pool, WINDOW_SECONDS).await?;
    let cells = census_cells(&counts);
    for (kind, rows) in &cells {
        let mut labels = std::collections::HashMap::new();
        labels.insert(crate::LABEL_KIND.to_string(), kind.clone());
        // 这一格回答的是哪一类问题：领域事实还是遥测。分类只在一处（`class_of_kind`），
        // 读面按标签取；闭集之外的老 kind 落进 `unknown`，不冒充领域。
        let class = cog_core::contract::event_fact::class_of_kind(kind)
            .map(|class| match class {
                cog_core::contract::event_fact::EventClass::Domain => "domain",
                cog_core::contract::event_fact::EventClass::Telemetry => "telemetry",
            })
            .unwrap_or("unknown");
        labels.insert(crate::LABEL_CLASS.to_string(), class.to_string());
        metrics
            .record_gauge(metric_names::EVENT_STREAM_ROWS_PER_DAY, *rows, labels)
            .await?;
    }
    Ok(cells.len())
}

/// 起这条 loop，**走存活上报**：一个没有心跳的循环停掉了，与「它一直没东西可报」在
/// 读数上同形，而这两件事要做的活相反。名字只写一次——`spawn` 用它登记，读数用它报。
pub fn spawn(
    pool: PgPool,
    metrics: Arc<dyn MetricsBackend>,
    shutdown: ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    cog_core::loop_health::spawn(
        LOOP,
        cog_core::loop_health::Cadence::Periodic(PASS_INTERVAL),
        shutdown.clone(),
        move |beat| {
            let pool = pool.clone();
            let metrics = Arc::clone(&metrics);
            let shutdown = shutdown.clone();
            async move {
                match pass(&pool, &metrics).await {
                    Ok(cells) => {
                        tracing::debug!(
                            cells,
                            "event stream census: this pass published every kind"
                        )
                    }
                    // 读不到就说读不到：印一串 0 会把「库读不出来」读成「这一天什么都没发生」。
                    Err(e) => {
                        tracing::warn!(error = %e, "event stream census: the window could not be read")
                    }
                }
                beat.beat();
                shutdown.wait().await;
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 闭集里的每一条都要出现在读数里，没有行的印 0。
    #[test]
    fn every_kind_in_the_closed_set_is_published_even_with_no_rows() {
        let cells = census_cells(&BTreeMap::new());
        assert_eq!(cells.len(), KINDS.len());
        assert!(cells.iter().all(|(_, rows)| *rows == 0.0));
    }

    /// 闭集之外的老 kind 也要印：漏掉它们等于把「旧版本的流还在写」藏起来。
    #[test]
    fn a_kind_outside_the_closed_set_still_reports_its_rows() {
        let mut counts = BTreeMap::new();
        counts.insert("legacy_kind".to_string(), 7u64);
        counts.insert("llm_call".to_string(), 3u64);
        let cells = census_cells(&counts);
        let by_kind: BTreeMap<_, _> = cells.into_iter().collect();
        assert_eq!(by_kind.get("legacy_kind"), Some(&7.0));
        assert_eq!(by_kind.get("llm_call"), Some(&3.0));
        assert_eq!(by_kind.get("tool_call"), Some(&0.0));
    }
}
