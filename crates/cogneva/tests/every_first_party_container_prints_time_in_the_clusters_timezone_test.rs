//! 门：一线工作负载的容器必须带 `TZ`，而且库不能带。
//!
//! 容器默认是 UTC。于是 `kubectl logs` 每行印的是 UTC 时刻、结尾一个 `Z`，而读它
//! 的人墙上是 +08：同一件事两个时刻，谁也没说出这件事；按本机日切的文件名也跟着偏。
//! 代码侧已经按本机时区打印（`cog_observability::logs::LocalTime`），但「本机」是
//! 谁，只有清单能回答——代码里写死偏移量就是第二个权威。
//!
//! 这条判据的两半都必要：
//!
//! * 漏一个工作负载，那一处的日志仍是 UTC，而它与别的日志进同一个流，读的人不会
//!   怀疑其中几行；
//! * 给 Postgres 加上，分区维护与额度日界一起挪八小时——那不是显示变了，是判定
//!   变了（`FOR VALUES FROM` 的字面量按会话时区解释，新分区会与既有分区重叠、
//!   `CREATE TABLE ... PARTITION OF` 直接失败）。
//!
//! 判据按镜像认一线/三方：镜像仓库来自 values 的 `image.repository`，这里是读它，
//! 不抄它。

use crate::common;

use common::routes::read;
use common::routes::repo_root;
use common::routes::walk;

/// 文档里任何一支 `containers:` 下的容器对象。init 容器不在其中：它们跑的是
/// `sh -c` 的一两步，不产日志。
fn containers(value: &serde_yaml::Value, out: &mut Vec<serde_yaml::Mapping>) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (key, child) in map {
                if key.as_str() == Some("containers") {
                    if let serde_yaml::Value::Sequence(items) = child {
                        for item in items {
                            if let serde_yaml::Value::Mapping(container) = item {
                                out.push(container.clone());
                            }
                        }
                    }
                }
                containers(child, out);
            }
        }
        serde_yaml::Value::Sequence(items) => {
            for item in items {
                containers(item, out);
            }
        }
        _ => {}
    }
}

fn env_pairs(container: &serde_yaml::Mapping) -> Vec<(String, Option<String>)> {
    container
        .get(serde_yaml::Value::String("env".to_owned()))
        .and_then(|v| v.as_sequence())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let name = item.get("name")?.as_str()?.to_owned();
                    let value = item
                        .get("value")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned);
                    Some((name, value))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn field<'a>(value: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a str> {
    let mut cur = value;
    for key in path {
        cur = cur.get(*key)?;
    }
    cur.as_str()
}

#[test]
fn first_party_containers_carry_the_clusters_timezone_and_the_database_does_not() {
    let root = repo_root();
    let values: serde_yaml::Value =
        serde_yaml::from_str(&read(&root.join("deploy/helm/cogneva/values.yaml")))
            .expect("values.yaml parses");
    let timezone = field(&values, &["global", "timezone"])
        .expect("values.yaml 的 global.timezone 没有值：清单里那一处就是集群的民用时间");
    let app_repo = field(&values, &["image", "repository"]).expect("image.repository");
    let pg_image = field(&values, &["backends", "postgres", "image"]).expect("postgres image");

    let mut files = Vec::new();
    walk(&root.join("deploy/rendered"), &["yaml"], &mut files);
    assert!(
        files.len() > 10,
        "读到 {} 份渲染清单：这条判据几乎在空集上通过",
        files.len()
    );

    let mut checked = 0usize;
    let mut missing = Vec::new();
    let mut database_with_timezone = Vec::new();
    for file in &files {
        let text = read(file);
        let shown = file.strip_prefix(&root).unwrap_or(file).display();
        for document in text.split("\n---") {
            let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(document) else {
                continue;
            };
            let mut found = Vec::new();
            containers(&value, &mut found);
            for container in &found {
                let name = container
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("<unnamed>");
                let image = container
                    .get("image")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let env = env_pairs(container);

                if image.starts_with(app_repo) {
                    checked += 1;
                    match env.iter().find(|(n, _)| n == "TZ") {
                        Some((_, Some(value))) if value == timezone => {}
                        Some((_, Some(value))) => missing.push(format!(
                            "{shown} [{name}] 的 TZ 是 {value:?}，values 的 global.timezone 是 \
                             {timezone:?}：两处各说一套，读日志的人不知道信哪个"
                        )),
                        Some((_, None)) => {
                            missing.push(format!("{shown} [{name}] 的 TZ 取自一个引用而不是字面值"))
                        }
                        None => missing.push(format!(
                            "{shown} [{name}] 没有 TZ：这一处的日志按 UTC 印时刻，\
                             与别的日志进同一个流，读它的人不会怀疑其中几行"
                        )),
                    }
                }

                if image.starts_with(pg_image) && env.iter().any(|(n, _)| n == "TZ") {
                    database_with_timezone.push(format!("{shown} [{name}]"));
                }
            }
        }
    }

    assert!(
        missing.is_empty(),
        "这些一线容器的时区没有跟 values 走：\n  {}\n\
         在 chart 模板里补 `{{{{- include \"cogneva.timeEnv\" . | nindent N }}}}`，\
         然后重跑 `deploy/scripts/render-deploy.sh`（`deploy/k3s/` 与 \
         `deploy/rendered/` 都是它的产物）。",
        missing.join("\n  ")
    );

    assert!(
        database_with_timezone.is_empty(),
        "库容器带上了 TZ：{}\n\
         库不跟集群的民用时间走。分区边界是按会话时区解释的日期字面量\
         （`FOR VALUES FROM ('2026-07-01')`），换时区之后新算出来的边界会与既有分区\
         重叠，`CREATE TABLE ... PARTITION OF` 会直接失败；额度日界同样按 UTC 定义。\
         要改库的时区，先把这两处 SQL 钉成显式 UTC，再谈显示。",
        database_with_timezone.join(", ")
    );

    assert!(
        checked >= 8,
        "只认出 {checked} 个一线容器：判据按 `image.repository` 认镜像，\
         认得太少说明这个值或渲染树读错了"
    );
}
