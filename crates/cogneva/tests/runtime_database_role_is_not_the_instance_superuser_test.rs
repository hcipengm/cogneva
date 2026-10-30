//! principle: feedback_a_gate_must_name_the_defect_it_stops
//!
//! 业务 Pod 连库用的角色不是实例的超级用户。
//!
//! 判据不是「代码里别写 cog_user」，而是**这条连接里装的是什么权限**：`cog_user`
//! 是 `rolsuper=t`，一条 SQL 注入拿到的不是这个库的读写，是整台实例——`COPY …
//! FROM PROGRAM` 能起进程、任意库能读、`pg_read_file` 能读宿主文件。所以四个
//! 业务工作负载的 DSN 必须落在 `cog_app` 上，而 `cog_app` 由角色引导 Job 建出来：
//! 只有 cogneva 库的连接与建表权，没有超权、没有 CREATEDB、不是超级用户那个角色
//! 的成员（成员可以 `SET ROLE` 过去，超权原样递过去，那等于没分家）。
//!
//! 会红的是这几种情况，每一种都不报错：
//! - 改回 `cog_user`：业务 Pod 照样连得上，权限面整个退回去；
//! - 新增一个工作负载照抄了旧 DSN：新 Pod 一样跑得起来；
//! - 引导 Job 不在清单里：新装的实例没有 `cog_app`，业务 Pod 起不来，而**已装好
//!   的实例**看不出来——它的角色早就在了；
//! - 口令键漏了：Job 建出没有口令的角色，或者业务侧拿不到那份口令。
//!
//! 唯一允许的例外是备份演练：它要把包装回一个一次性库，恢复路径里的
//! `SET LOCAL session_replication_role` 只对超级用户开放，而建那个库本身要
//! CREATEDB。这条例外不是「漏改」，所以它也必须被钉住——把演练也顺手改过去，
//! 演练会在真出事的那天才红。

use crate::common;

use common::routes::{read, repo_root};

use std::fs;
use std::path::{Path, PathBuf};

/// 业务连库必须落在的那个角色。
const APP_ROLE: &str = "cog_app";

/// 实例超级用户，只许出现在两个地方：数据库自身与角色引导。
const SUPERUSER_ROLE: &str = "cog_user";

/// 一份清单里每一条 `postgres://<role>:` 的角色名，按出现顺序。
fn dsn_roles(text: &str) -> Vec<String> {
    let mut roles = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("postgres://") {
        rest = &rest[at + "postgres://".len()..];
        let end = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        if let Some(after) = rest[end..].strip_prefix(':') {
            assert!(
                after.starts_with('$') || after.starts_with("${"),
                "角色后面要跟口令占位，读到的是 {:?}",
                &rest[..(end + 1).min(rest.len())]
            );
            roles.push(rest[..end].to_string());
        }
    }
    roles
}

fn yaml_files(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && matches!(
                    p.extension().and_then(|s| s.to_str()),
                    Some("yaml") | Some("yml")
                )
        })
        .collect();
    found.sort();
    assert!(
        !found.is_empty(),
        "{} 下一份清单都没读到——「没读到」与「都合规」不许同形",
        dir.display()
    );
    found
}

/// 一份文件里按 `---` 切出来的每一份文档。判据按文档而不是按文件：一份文件里
/// 可以同时装着导出的 CronJob 与演练的 CronJob，前者必须是受限角色、后者必须是
/// 超级用户，按文件比会把两者压成一个数。
fn documents(text: &str) -> Vec<String> {
    let mut docs = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim_end() == "---" {
            if !current.trim().is_empty() {
                docs.push(std::mem::take(&mut current));
            }
            continue;
        }
        current.push_str(line);
        current.push('\n');
    }
    if !current.trim().is_empty() {
        docs.push(current);
    }
    docs
}

/// 部署面上每一份带着连接串的文档：(文件, 文档序号, 角色)。
fn deployed_dsn_roles(root: &Path) -> Vec<(String, usize, String)> {
    let mut found = Vec::new();

    let mut sources: Vec<PathBuf> = yaml_files(&root.join("deploy/helm/cogneva/templates"));
    sources.extend(yaml_files(&root.join("deploy/k3s")));
    sources.push(root.join("deploy/helm/cogneva/files/cogneva.json"));
    sources.push(root.join("deploy/k3s/cogneva-json-configmap.yaml"));

    for path in sources {
        let label = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .trim_start_matches("deploy/")
            .to_string();
        for (index, doc) in documents(&read(&path)).into_iter().enumerate() {
            for role in dsn_roles(&doc) {
                found.push((label.clone(), index, role));
            }
        }
    }

    found
}

#[test]
fn every_business_workload_connects_as_the_restricted_role() {
    let root = repo_root();
    let seen = deployed_dsn_roles(&root);
    assert!(!seen.is_empty(), "部署面上一处连接串都没读到");

    let mut offenders = Vec::new();
    let mut drill_documents = 0;
    for (file, index, role) in &seen {
        // 演练是唯一的例外，判据是**这份文档自己叫什么**：它把包装回一次性库，
        // 恢复路径的 `session_replication_role` 只对超级用户开放。按文档判而不是
        // 按文件判，是因为 chart 的 `backup.yaml` 同时装着导出与演练两个 CronJob：
        // 按文件比会把「导出已经换成受限角色」这件事一起抹掉。
        let is_drill = file.ends_with("backup.yaml") || file.ends_with("backup-drill-cronjob.yaml");
        if is_drill && drill_document(&root, file, *index) {
            drill_documents += 1;
            assert_eq!(
                role, SUPERUSER_ROLE,
                "{file} 第 {index} 份文档是演练，它连的是源库并要建一次性库，必须仍是超级用户"
            );
            continue;
        }
        if role != APP_ROLE {
            offenders.push(format!("{file} 第 {index} 份文档: postgres://{role}:"));
        }
    }

    assert!(
        offenders.is_empty(),
        "业务连接串必须落在 {APP_ROLE} 上（唯一例外是演练）：\n  {}",
        offenders.join("\n  ")
    );

    // 例外那一侧也要钉住：顺手把演练也改成受限角色，它会在真出事的那天才红——
    // 那时没有第二个包可选。
    assert!(
        drill_documents >= 2,
        "演练文档只钉到 {drill_documents} 份（chart 与静态各一份）：例外没有落到纸上，\
         下一次有人把它也改掉时这一条判不出来"
    );
}

/// 这份文档是不是演练那一份。判据取它自己的名字，不取文件名：chart 的
/// `backup.yaml` 里导出与演练两件事住同一份文件。
fn drill_document(root: &Path, file: &str, index: usize) -> bool {
    let path = root.join("deploy").join(file);
    documents(&read(&path))
        .get(index)
        .map(|doc| doc.contains("cogneva-backup-drill"))
        .unwrap_or(false)
}

#[test]
fn the_restricted_role_is_created_before_anything_uses_it() {
    let root = repo_root();

    let bootstrap = root.join("deploy/helm/cogneva/files/db-role-bootstrap.sh");
    let script = read(&bootstrap);

    // 角色本身：能登录、不是超级用户、不能建库也不能建角色。少写一条，
    // 角色还是建得出来，只是它比设计更宽——而宽出来的那部分不报错。
    for clause in [
        "CREATE ROLE cog_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE",
        "ALTER ROLE cog_app WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE",
    ] {
        assert!(
            script.contains(clause),
            "引导脚本里没有 `{clause}`：角色会以默认属性被建出来"
        );
    }

    // 成员关系不是分家：被授予者对超权角色可以 SET ROLE，超权原样递过去。
    assert!(
        !script.contains("GRANT cog_user TO cog_app"),
        "把 cog_app 授成超级用户角色的成员等于没做这一刀：成员可以 SET ROLE 过去"
    );

    // 所有权必须挪：分区维护的 DETACH/ATTACH 是表属主才有的权限，GRANT 给不出来。
    assert!(
        script.contains("OWNER TO cog_app"),
        "对象所有权没挪给 cog_app：分区维护要表属主，GRANT 给不出那一格"
    );

    // 清单侧：Job 与它挂的 ConfigMap 都要在，且两份清单（chart 与静态）都在。
    for path in [
        root.join("deploy/helm/cogneva/templates/db-role-bootstrap.yaml"),
        root.join("deploy/k3s/db-role-bootstrap.yaml"),
    ] {
        let text = read(&path);
        assert!(
            text.contains("kind: Job") && text.contains("kind: ConfigMap"),
            "{} 里 Job 与它挂的脚本 ConfigMap 要一起出现",
            path.display()
        );
    }

    let kustomization = read(&root.join("deploy/k3s/kustomization.yaml"));
    assert!(
        kustomization.contains("- db-role-bootstrap.yaml"),
        "静态清单里没有把引导 Job 挂进 kustomization：apply 那一侧永远不会建出这个角色"
    );

    // 口令键：chart 侧生成、init-secrets 侧生成，业务 Pod 只读它。
    for (label, path) in [
        (
            "chart secret",
            root.join("deploy/helm/cogneva/templates/secret.yaml"),
        ),
        ("init-secrets", root.join("deploy/scripts/init-secrets.sh")),
    ] {
        assert!(
            read(&path).contains("app-password"),
            "{label} 里没有 app-password：角色建出来没有口令，或者业务侧拿不到那一份"
        );
    }
}

/// 口令和权限是两件事：换掉 DSN 里的角色，却把超级用户的口令留在同一个 Pod 的
/// env 里，等于把那一刀又还回去一半——注入拿不到它，但任何一次代码执行都拿得到。
#[test]
fn a_workload_that_connects_as_the_restricted_role_does_not_hold_the_superuser_password() {
    let root = repo_root();
    let mut sources: Vec<PathBuf> = yaml_files(&root.join("deploy/helm/cogneva/templates"));
    sources.extend(yaml_files(&root.join("deploy/k3s")));

    let mut offenders = Vec::new();
    let mut checked = 0;
    for path in sources {
        let label = path.file_name().unwrap().to_string_lossy().to_string();
        for doc in documents(&read(&path)) {
            let roles = dsn_roles(&doc);
            if !roles.iter().any(|r| r == APP_ROLE) {
                continue;
            }
            checked += 1;
            if doc.contains("key: pg-password") {
                offenders.push(label.clone());
            }
        }
    }

    assert!(checked > 0, "一份按受限角色连库的文档都没读到");
    assert!(
        offenders.is_empty(),
        "这些文档按 {APP_ROLE} 连库，却仍挂着实例超级用户的口令：{offenders:?}"
    );
}

/// 上面那条全量判据依赖「字典里装着每一个该被扫到的工作负载」。少扫一个文件，
/// 它写的 DSN 就不在 offenders 里，而整套仍然是绿的——空集与全合规同形。
#[test]
fn the_scan_covers_every_workload_that_takes_a_database_url() {
    let root = repo_root();
    let seen = deployed_dsn_roles(&root);

    let files: Vec<&str> = seen.iter().map(|(f, _, _)| f.as_str()).collect();
    let chart = files
        .iter()
        .filter(|f| f.starts_with("helm/cogneva/"))
        .count();
    let static_ = files.iter().filter(|f| f.starts_with("k3s/")).count();
    assert!(
        chart >= 5,
        "chart 里只有 {chart} 份文档带着连接串（四个业务工作负载加应用配置那一份），少一份就是漏扫"
    );
    assert!(
        static_ >= 5,
        "静态清单里只有 {static_} 份文档带着连接串，少一份就是漏扫"
    );

    for expected in [
        "helm/cogneva/templates/gateway.yaml",
        "helm/cogneva/templates/evolution.yaml",
        "helm/cogneva/templates/security-gateway.yaml",
        "helm/cogneva/templates/backup.yaml",
        "helm/cogneva/files/cogneva.json",
        "k3s/deployment.yaml",
        "k3s/gateway-deployment.yaml",
        "k3s/evolution-deployment.yaml",
        "k3s/backup-cronjob.yaml",
        "k3s/backup-drill-cronjob.yaml",
        "k3s/cogneva-json-configmap.yaml",
    ] {
        assert!(
            files.contains(&expected),
            "{expected} 没有出现在扫描结果里：它带着连接串却没被读到"
        );
    }
}
