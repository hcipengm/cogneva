//! 门：一线的每个容器都带 `TZ`，库还要把服务端时区钉到同一个值。
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
//! * 库只在容器上带 `TZ` 是不够的：`initdb` 会把 `timezone` 写进 postgresql.conf，
//!   已建好的数据目录不跟环境变量走，于是 `psql` 与日志按 +08 印、会话时区仍是 UTC，
//!   而分区边界的日期字面量正是按会话时区解释的——同一个库两副钟，差的这几行没有
//!   任何东西会指出来。
//!
//! 判据按镜像认一线/三方：镜像仓库来自 values 的 `image.repository`，这里是读它，
//! 不抄它。

use crate::common;

use common::routes::read;
use common::routes::repo_root;
use common::routes::walk;
use common::yaml;

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

fn args_of(container: &serde_yaml::Mapping) -> Vec<String> {
    container
        .get(serde_yaml::Value::String("args".to_owned()))
        .and_then(|v| v.as_sequence())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
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
fn every_container_carries_the_clusters_timezone_including_the_database() {
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
    let mut database_without_server_zone = Vec::new();
    for file in &files {
        let text = read(file);
        let shown = file.strip_prefix(&root).unwrap_or(file).display();
        for document in text.split("\n---") {
            let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(document) else {
                continue;
            };
            let mut found = Vec::new();
            yaml::containers(&value, &mut found);
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

                // The database server is the container that starts one: it is
                // the only one carrying `POSTGRES_DB`. A client container on
                // the same image (the role bootstrap job) needs the `TZ` but
                // has no session default of its own to set.
                if image.starts_with(pg_image) {
                    match env.iter().find(|(n, _)| n == "TZ") {
                        Some((_, Some(value))) if value == timezone => {}
                        other => missing.push(format!(
                            "{shown} [{name}] 的库容器 TZ 是 {other:?}，values 的 \
                             global.timezone 是 {timezone:?}"
                        )),
                    }
                }
                let runs_the_server = env.iter().any(|(n, _)| n == "POSTGRES_DB");
                if image.starts_with(pg_image) && runs_the_server {
                    let args = args_of(container);
                    for setting in ["timezone", "log_timezone"] {
                        let expected = format!("{setting}={timezone}");
                        if !args.iter().any(|a| a == &expected) {
                            database_without_server_zone
                                .push(format!("{shown} [{name}] 缺 `-c {expected}`"));
                        }
                    }
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
        database_without_server_zone.is_empty(),
        "库的服务端时区没有跟 values 走：{}\n\
         容器上的 `TZ` 只决定进程的本地时间，`initdb` 写进 postgresql.conf 的\
         `timezone` 决定每个会话的默认时区，而分区边界的日期字面量按后者解释。\
         两处必须来自同一个值：在模板里把 `-c timezone=`／`-c log_timezone=` 接到\
         `global.timezone`，然后重跑 `deploy/scripts/render-deploy.sh`。",
        database_without_server_zone.join(", ")
    );

    assert!(
        checked >= 8,
        "只认出 {checked} 个一线容器：判据按 `image.repository` 认镜像，\
         认得太少说明这个值或渲染树读错了"
    );
}
