//! 库自己那几条界，要走到面板与规则上，而不只在终端里印一次。
//!
//! 终端读数只有在有人跑那条命令时才存在；规则与面板读的是指标面。这一条把三件事
//! 钉在一起：
//!
//! - 每一条新序列都在闭集里、有登记的 help；
//! - 发布点（`cog-storage/src/db_surface.rs`）点名它，并且带上规则选行用的标签；
//! - 规则就在交付的 chart 里，表达式点的正是这些序列，**线**来自库自己：
//!   autovacuum 的触发线（比值 1.0），与 `idle_in_transaction_session_timeout`
//!   （跟着设置走，设置为 0 时静默）。线若被换成一个手写的数，这条会红。
//!
//! 没有这次改动这条会红：三条读数只在 `cogneva db-pressure` 的输出里，规则够不着。

use crate::common::routes::repo_root;

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";

fn published(name: &str) -> bool {
    cog_core::metric_names::ALL
        .iter()
        .any(|known| known.as_str() == name)
}

fn help_is_registered(name: &str) -> bool {
    cog_core::contract::metric_help::metric_description(cog_core::MetricType::Gauge, name)
        .map(|text| text.trim().len() > 40)
        .unwrap_or(false)
}

#[test]
fn every_new_series_is_in_the_closed_set_and_documented() {
    for name in [
        cog_core::metric_names::DB_XACT_AGE_SECONDS.as_str(),
        cog_core::metric_names::DB_DEAD_TUPLES.as_str(),
        cog_core::metric_names::DB_VACUUM_THRESHOLD_TUPLES.as_str(),
    ] {
        assert!(published(name), "{name} 不在指标闭集里");
        assert!(
            help_is_registered(name),
            "{name} 没有登记的 help：面板上会是一条没人解释的序列"
        );
    }
}

#[test]
fn the_publisher_names_each_series_and_the_labels_the_rules_select_on() {
    let source = std::fs::read_to_string(repo_root().join("crates/cog-storage/src/db_surface.rs"))
        .expect("db_surface.rs 读不到");
    for needle in [
        "metric_names::DB_XACT_AGE_SECONDS",
        "metric_names::DB_DEAD_TUPLES",
        "metric_names::DB_VACUUM_THRESHOLD_TUPLES",
        "DB_STATE_LABEL",
        "DB_TABLE_LABEL",
    ] {
        assert!(
            source.contains(needle),
            "发布点里没有 `{needle}`：规则按这个标签选行会读到空"
        );
    }
    // 没有事务的那一态必须缺席而不是 0——0 秒是一次刚开的事务。
    assert!(
        source.contains("let Some(age) = age else { continue };"),
        "没有事务的状态被发了 0，或者干脆没写这条分支：缺席与 0 在读数上必须分开"
    );
}

#[test]
fn the_rules_compare_against_the_databases_own_lines() {
    let text = std::fs::read_to_string(repo_root().join(CHART_CONFIG)).expect("chart 读不到");
    let chart: serde_json::Value = serde_json::from_str(&text).expect("chart 不是 JSON");
    let rules = chart["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("infra_watch.rules 不是数组");
    let find = |name: &str| {
        rules
            .iter()
            .find(|rule| rule["name"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("chart 里没有规则 {name}"))
    };

    let vacuum = find("db_vacuum_lag_past_trigger");
    let promql = vacuum["promql"].as_str().expect("promql");
    assert!(
        promql.contains(cog_core::metric_names::DB_DEAD_TUPLES.as_str())
            && promql.contains(cog_core::metric_names::DB_VACUUM_THRESHOLD_TUPLES.as_str()),
        "真空规则没有读那两条已发布的序列：{promql}"
    );
    assert_eq!(
        vacuum["condition"]["greater_than"].as_f64(),
        Some(1.0),
        "这条线是 autovacuum 自己的触发线；换成别的数就是拍出来的"
    );

    let idle = find("db_idle_transaction_past_timeout");
    let promql = idle["promql"].as_str().expect("promql");
    assert!(
        promql.contains(cog_core::metric_names::DB_XACT_AGE_SECONDS.as_str()),
        "空闲事务规则没有读年龄序列：{promql}"
    );
    assert!(
        promql.contains("idle_in_transaction_session_timeout"),
        "空闲事务规则的线必须来自库的设置，不是规则里另写一个秒数：{promql}"
    );
    assert!(
        promql.contains(cog_core::metric_names::DB_SETTING_SECONDS.as_str()),
        "规则的比较对象必须是那条设置自身的读数：{promql}"
    );
    assert_eq!(
        idle["condition"]["greater_than"].as_f64(),
        Some(0.0),
        "表达式在越过设置时返回正年龄；条件不该再压一个手写的秒数"
    );
}

#[test]
fn the_two_readings_are_on_the_panel() {
    let ids: Vec<&str> = cog_gateway::readings::READINGS
        .iter()
        .map(|spec| spec.id)
        .collect();
    for id in ["db_idle_transaction_age_seconds", "db_vacuum_lag_max_ratio"] {
        assert!(ids.contains(&id), "面板上没有 {id}：规则有了，读数却没人画");
    }
    // 比值在读到的地方算：发布比值本身会在阈值为 0 时编出一个数。
    let ratio = cog_gateway::readings::DB_VACUUM_LAG_RATIO;
    assert!(
        ratio.query.contains('/'),
        "比值的读数必须在查询里除出来：{}",
        ratio.query
    );
}
