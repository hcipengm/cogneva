//! `cogneva table-census`：表与流的分工，跑一次就有读数。
//!
//! 每一张表在普查里有一行：事件流本身、可重建的投影、纯传输配置，或明确记作旧主存
//! 欠账。读数回答两件事：现在各是多少张；下一步该迁的是哪些（欠账逐条点名）。

use std::collections::BTreeMap;

use cog_storage::table_shapes::{CensusClass, TABLE_CENSUS};

pub fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut legacy = Vec::new();
    for row in TABLE_CENSUS {
        let class = match &row.class {
            CensusClass::Stream => "stream",
            CensusClass::Projection { .. } => "projection",
            CensusClass::Config { .. } => "config",
            CensusClass::Legacy { .. } => {
                legacy.push(row.table);
                "legacy"
            }
        };
        *counts.entry(class).or_default() += 1;
    }
    for (class, count) in &counts {
        println!("{class}\t{count}");
    }
    println!("total\t{}", TABLE_CENSUS.len());
    if !legacy.is_empty() {
        println!("legacy-tables\t{}", legacy.join(","));
    }
    Ok(())
}
