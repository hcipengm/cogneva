//! 普查里归为「事件流本身／事实面」的表，与「事实面不许到点删」那张名单必须是同一批：
//! 一边说它是事实，另一边却不知道它，那一条删除纪律在它身上就不生效——而缺了纪律的
//! 表与一张没人动过的表在读数上同形。
//!
//! 没有这次改动这条会红：两张归档表在普查里是事实面，却不在 `FACT_TABLES` 里。

use cog_storage::table_shapes::{CensusClass, FACT_TABLES, TABLE_CENSUS};

#[test]
fn every_table_the_census_calls_a_stream_is_a_fact_with_the_delete_rule() {
    let streams: Vec<&str> = TABLE_CENSUS
        .iter()
        .filter(|row| matches!(row.class, CensusClass::Stream))
        .map(|row| row.table)
        .collect();
    assert!(
        !streams.is_empty(),
        "没有一张表被归为事实面：普查没读到东西"
    );
    let missing: Vec<&str> = streams
        .iter()
        .copied()
        .filter(|table| !FACT_TABLES.contains(table))
        .collect();
    assert!(
        missing.is_empty(),
        "这些表在普查里是事实面，却不在「事实面不许到点删」的名单里：{missing:?}"
    );
}
