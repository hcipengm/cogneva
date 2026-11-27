//! `cogneva cell-plan --changed-paths-from -`：这一批要跑哪些格、哪些可以复用。
//!
//! 格是「一个 crate 的测试目标」；判据只有一句：**只跑输入变了的格**，其余复用上一次的
//! 结论（`cog_reflection::cell_reuse::plan`）。这里只把库的清单和这一批改到的路径变成
//! 那个判断的输入，并把结果按行印出来，给宿主脚本决定怎么调 cargo——执行者不顺手套用
//! 「我挑的那几条」。
//!
//! 认不出归属的路径按「有包改了」处理：`crates/` 之外的一处改动（工作区清单、构建脚本、
//! 部署脚本）可能影响任何一个格，那时一个格都不复用。

use std::io::Read;

use cog_reflection::cell_reuse::plan;

pub fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args()
        .skip_while(|arg| arg != "cell-plan")
        .skip(1)
        .collect();
    let mut changed = String::new();
    if args.iter().any(|arg| arg == "--changed-paths-from") {
        std::io::stdin().read_to_string(&mut changed)?;
    } else {
        return Err("usage: cogneva cell-plan --changed-paths-from - < paths".into());
    }
    let paths: Vec<String> = changed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();

    let metadata = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1"])
        .output()?;
    if !metadata.status.success() {
        return Err(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&metadata.stderr)
        )
        .into());
    }
    let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout)?;
    let packages = metadata
        .get("packages")
        .and_then(|packages| packages.as_array())
        .ok_or("cargo metadata carried no packages")?;

    // 包目录是它清单所在的目录；一处改动落在哪个目录下，就是哪个包的输入变了。
    let mut cells = Vec::new();
    let mut changed_packages = Vec::new();
    for package in packages {
        let Some(name) = package.get("name").and_then(|name| name.as_str()) else {
            continue;
        };
        let Some(manifest) = package.get("manifest_path").and_then(|path| path.as_str()) else {
            continue;
        };
        let Some(dir) = std::path::Path::new(manifest).parent() else {
            continue;
        };
        cells.push(format!("{name}:suite"));
        let prefix = format!("{}/", dir.display());
        if paths.iter().any(|path| path.starts_with(&prefix)) {
            changed_packages.push(name.to_string());
        }
    }
    // 工作区根的改动（`Cargo.toml`、`build.rs` 之外的任何一处越界路径）按「全都变了」：
    // 认不出归属时复用是拿一次没验过的绿去顶这一次。
    let workspace_wide = paths.iter().any(|path| {
        !packages.iter().any(|package| {
            package
                .get("manifest_path")
                .and_then(|m| m.as_str())
                .and_then(|m| std::path::Path::new(m).parent())
                .map(|dir| path.starts_with(&format!("{}/", dir.display())))
                .unwrap_or(false)
        })
    });
    if workspace_wide {
        changed_packages = cells
            .iter()
            .map(|cell| cell.split(':').next().unwrap_or("").to_string())
            .collect();
    }

    // 依赖方向：改一个 crate，**依赖它的那些 crate 的格也要重跑**——它们的输入里包含
    // 这一份代码。只按「自己的目录变没变」判，改 cog-core 会跳过 cog-storage 的格，而
    // 那些格恰恰最可能被这次改动弄红。
    let changed_packages = dependents_closure(&metadata, &changed_packages);
    let plan = plan(&cells, &changed_packages);
    // 这一次怎么决定，进账本：哪些格重跑、多少复用、由多少条改动路径算来。
    // 没有数据库的连接就用不上这条事实——计划照样印，账本那一步跳过并说一声。
    let rerun_packages: Vec<String> = plan
        .rerun
        .iter()
        .map(|cell| cell.split(':').next().unwrap_or("").to_string())
        .collect();
    if let Ok(url) = std::env::var(cog_core::env::DATABASE_URL) {
        match record_plan_blocking(&url, &rerun_packages, plan.reused.len(), paths.len()) {
            Ok(()) => {}
            Err(error) => eprintln!("cell-plan: build_planned fact not recorded: {error}"),
        }
    }
    for cell in &plan.rerun {
        println!("rerun\t{}", cell.split(':').next().unwrap_or(""));
    }
    for cell in &plan.reused {
        println!("reused\t{}", cell.split(':').next().unwrap_or(""));
    }
    match plan.reused_ratio() {
        Some(ratio) => println!("reused_ratio\t{ratio:.4}"),
        None => println!("reused_ratio\tnone (no cells)"),
    }
    Ok(())
}

/// 同步入口：CLI 是同步的，这里只为一个写口建一次当前线程运行时。
fn record_plan_blocking(
    url: &str,
    rerun: &[String],
    reused: usize,
    changed: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(record_plan(url, rerun, reused, changed))
}

/// 把这一拍的计划写成 `build_planned` 事实。
///
/// 单独拿出来是因为它唯一会碰数据库；CLI 其余部分只读 `cargo metadata` 与 stdout。
async fn record_plan(
    url: &str,
    rerun: &[String],
    reused: usize,
    changed: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    use cog_core::EventStore as _;
    let pool = sqlx::PgPool::connect(url).await?;
    let store = cog_storage::postgres::event_log::EventLogStore::new(pool, "cell-plan");
    let mut event = cog_core::FactEvent::new(cog_core::EventPayload::BuildPlanned {
        rerun: rerun.to_vec(),
        reused,
        changed,
        source: "cargo-hook".to_string(),
    });
    event.event_id = Some(store.next_event_id());
    store.append(event).await?;
    Ok(())
}

/// 把这些包与**依赖它们的全部包**闭在一起：`cargo metadata` 的 `resolve` 里
/// `deps[*].pkg` 是「我依赖谁」，反过来就是「谁依赖我」。所以这里要的是带依赖的
/// metadata（`--no-deps` 的 `resolve` 是空的）。没有 `resolve` 时原样返回：认不出
/// 依赖方向就不替它减法，调用方那一边按工作区级改动兜底。
fn dependents_closure(metadata: &serde_json::Value, changed: &[String]) -> Vec<String> {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    let Some(nodes) = metadata
        .get("resolve")
        .and_then(|resolve| resolve.get("nodes"))
        .and_then(|nodes| nodes.as_array())
    else {
        return changed.to_vec();
    };
    let names: BTreeMap<&str, &str> = metadata
        .get("packages")
        .and_then(|packages| packages.as_array())
        .map(|packages| {
            packages
                .iter()
                .filter_map(|package| {
                    Some((package.get("id")?.as_str()?, package.get("name")?.as_str()?))
                })
                .collect()
        })
        .unwrap_or(Default::default());
    // 反向边：被依赖的包 → 依赖它的包。
    let mut dependents: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for node in nodes {
        let Some(dependent) = node
            .get("id")
            .and_then(|id| id.as_str())
            .and_then(|id| names.get(id).copied())
        else {
            continue;
        };
        if let Some(deps) = node.get("deps").and_then(|deps| deps.as_array()) {
            for dep in deps {
                if let Some(dependency) = dep
                    .get("pkg")
                    .and_then(|id| id.as_str())
                    .and_then(|id| names.get(id).copied())
                {
                    dependents.entry(dependency).or_default().push(dependent);
                }
            }
        }
    }
    let mut seen: BTreeSet<String> = changed.iter().cloned().collect();
    let mut queue: VecDeque<String> = changed.iter().cloned().collect();
    while let Some(package) = queue.pop_front() {
        if let Some(list) = dependents.get(package.as_str()) {
            for dependent in list {
                if seen.insert((*dependent).to_string()) {
                    queue.push_back((*dependent).to_string());
                }
            }
        }
    }
    seen.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> serde_json::Value {
        serde_json::json!({
            "packages": [
                {"id": "core 0.1.0", "name": "core"},
                {"id": "store 0.1.0", "name": "store"},
                {"id": "app 0.1.0", "name": "app"},
                {"id": "other 0.1.0", "name": "other"}
            ],
            "resolve": {
                "nodes": [
                    {"id": "core 0.1.0", "deps": []},
                    {"id": "store 0.1.0", "deps": [{"pkg": "core 0.1.0"}]},
                    {"id": "app 0.1.0", "deps": [{"pkg": "store 0.1.0"}]},
                    {"id": "other 0.1.0", "deps": []}
                ]
            }
        })
    }

    /// 改一个被依赖的包，依赖它的那些包也要重跑；不相干的包不动。
    #[test]
    fn a_changed_package_pulls_in_everything_that_depends_on_it() {
        let closure = dependents_closure(&metadata(), &["core".to_string()]);
        assert_eq!(closure, vec!["app", "core", "store"]);
        let closure = dependents_closure(&metadata(), &["store".to_string()]);
        assert_eq!(closure, vec!["app", "store"]);
        // 没人依赖的包只闭到自己。
        let closure = dependents_closure(&metadata(), &["other".to_string()]);
        assert_eq!(closure, vec!["other"]);
    }

    /// 没有 `resolve` 时原样返回：认不出依赖方向就不替它减法。
    #[test]
    fn without_a_resolve_graph_the_changed_set_is_left_alone() {
        let metadata = serde_json::json!({"packages": [], "resolve": null});
        assert_eq!(
            dependents_closure(&metadata, &["core".to_string()]),
            vec!["core"]
        );
    }
}
