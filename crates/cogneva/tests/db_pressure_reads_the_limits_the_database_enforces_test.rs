//! 库自己会掐什么、落后多少，要在库自己的读数里看得到，而不是等它掐了才知道。
//!
//! 三格，缺一格就有一种「被掐」与「自己断」同形：
//!
//! - 在飞的事务多老：库按自己的界掐它，此刻的年龄就是「还差多久」；
//! - 空闲事务多老：它占着快照，vacuum 追不上，而它自己不动；
//! - autovacuum 落后多少：死行数 ÷ 它自己的阈值，阈值由库的两条设置与本表活行数
//!   算出——换一个库、换一张表，阈值跟着走，不是人拍的一个数。
//!
//! 没有这次改动前两条与第三条都不存在（`cogneva db-pressure` 只印体积、事务速率、
//! 临时字节、死锁、慢查询）。

#[test]
fn the_pressure_reading_names_what_the_database_will_cut_and_what_vacuum_owes() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let source = std::fs::read_to_string(root.join("crates/cogneva/src/db_pressure.rs"))
        .expect("db_pressure.rs");

    // 在飞的事务：从 pg_stat_activity 读，按状态分开——active 与 idle in transaction
    // 是两种不同的「等」，合成一格就看不出该杀哪一个。
    assert!(
        source.contains("pg_stat_activity"),
        "没有读在飞的事务：库掐掉一条连接时，事后只剩断开"
    );
    assert!(
        source.contains("xact_active_max_age_secs") && source.contains("xact_idle_max_age_secs"),
        "在飞事务没有按 active／idle in transaction 分开"
    );

    // autovacuum 的阈值由库的设置推出：两条设置名都必须出现在这一处，且不另写一个数。
    assert!(
        source.contains("autovacuum_vacuum_threshold")
            && source.contains("autovacuum_vacuum_scale_factor"),
        "vacuum 落后多少没有按库自己的阈值算：那会变成一个人拍的数"
    );
    assert!(
        source.contains("vacuum_lagging_tables") && source.contains("vacuum_lag_max_ratio"),
        "没有「落后多少」的读数：死行数单独印出来读不出该不该动手"
    );
    // 没有任何死行时说「没有」，不印 0 比值——0 是一次真实读数，与「没有」不同。
    assert!(
        source.contains("no dead tuples"),
        "没有死行的时候会被读成一个 0 比值"
    );
    // 每张事实表最老一行多老：撤/搬之前要知道手里这份历史有多长；空表与没有时间列
    // 的表各说各的，不印 0。
    assert!(
        source.contains("oldest_row_secs"),
        "没有「每张表最老一行多老」的读数"
    );
    assert!(
        source.contains("no time column") && source.contains("the table is empty"),
        "空表与没有时间列的表被读成同一个 0 岁"
    );
}
