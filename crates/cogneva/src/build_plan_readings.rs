//! `cogneva build-plans --hours <n>`：从 `build_planned` 事实折「复用与真跑」的条数。
//!
//! 这条事实是 S20.9 的账：每一次 `cell-plan` 决定跑了哪些格、省了哪些格。读面按
//! 包分组印出「重跑几次、复用多少次、改动路径累计多少」——没有它，那几个数只活在
//! 那一次运行的 stderr 里。
//!
//! 窗口必须由调用方给：给多久取决于要回答哪一段，「默认 24 小时」是拍出来的一个数。

use std::collections::BTreeMap;

/// 一个包在这一段里被重跑了几次。
///
/// 复用是**整次计划**省下来的格数，不按包分摊：一次计划复用了 3 格，说不出是哪三格
/// 各被省了一次。分摊出来的「每包复用次数」是编的，所以这里只记重跑。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PackageReuse {
    pub rerun: u64,
}

/// 把一组 `build_planned` 事件的 attributes 折成按包分组的条数。
///
pub fn summarise(
    attributes: impl IntoIterator<Item = serde_json::Value>,
) -> (BTreeMap<String, PackageReuse>, PackageTotals) {
    let mut per_package: BTreeMap<String, PackageReuse> = BTreeMap::new();
    let mut totals = PackageTotals::default();
    for value in attributes {
        let rerun: Vec<String> = value
            .get("rerun")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|i| i.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or(Default::default());
        let reused = value.get("reused").and_then(|v| v.as_u64()).unwrap_or(0);
        let changed = value.get("changed").and_then(|v| v.as_u64()).unwrap_or(0);
        totals.plans += 1;
        totals.reused += reused;
        totals.changed_paths += changed;
        for package in rerun {
            per_package.entry(package).or_default().rerun += 1;
        }
    }
    (per_package, totals)
}

/// 这一段的总账：计划次数、复用格总数、改动路径总数。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PackageTotals {
    pub plans: u64,
    pub reused: u64,
    pub changed_paths: u64,
}

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args()
        .skip_while(|arg| arg != "build-plans")
        .skip(1)
        .collect();
    let mut hours: Option<u64> = None;
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--hours" {
            hours = args.get(i + 1).and_then(|value| value.parse::<u64>().ok());
            i += 2;
        } else {
            return Err(format!("unknown argument: {}", args[i]).into());
        }
    }
    let hours = hours.ok_or("usage: cogneva build-plans --hours <n>")?;
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the stream")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT attributes FROM cogneva_event \
          WHERE kind = 'build_planned' AND at > now() - make_interval(hours => $1)",
    )
    .bind(hours as i32)
    .fetch_all(&pool)
    .await?;
    let attributes = rows
        .into_iter()
        .filter_map(|(text,)| serde_json::from_str(&text).ok());
    let (per_package, totals) = summarise(attributes);
    for (package, row) in per_package {
        println!("package\t{package}\trerun={}", row.rerun);
    }
    println!("plans\t{}", totals.plans);
    println!("reused_cells\t{}", totals.reused);
    println!("changed_paths\t{}", totals.changed_paths);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plan_counts_its_rerun_packages_and_its_reuse() {
        let events = vec![
            serde_json::json!({"rerun": ["cog-core", "cog-storage"], "reused": 3, "changed": 2, "source": "cargo-hook"}),
            serde_json::json!({"rerun": ["cog-storage"], "reused": 4, "changed": 1, "source": "cargo-hook"}),
        ];
        let (per, totals) = summarise(events);
        assert_eq!(per["cog-core"].rerun, 1);
        assert_eq!(per["cog-storage"].rerun, 2);
        assert_eq!(totals.plans, 2);
        assert_eq!(totals.reused, 7);
        assert_eq!(totals.changed_paths, 3);
    }
}
