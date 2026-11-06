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
//! 备份演练是唯一一个权限与业务不同的工作负载：它要在同一台服务器上
//! `CREATE DATABASE` 一个一次性库，而业务那个受限角色没有 CREATEDB。它有**自己的**
//! 角色 `cog_backup`——恢复本身不要求超级用户位（有它时关掉外键检查一次装完，没有
//! 它时按依赖顺序装、外键照样在检查，`restore_postgres` 按连接的角色选这两条路），
//! 需要的是「建库 + 读全部表 + 写自己那一行结论」。所以它不是「漏改」的那个例外，
//! 而是另一个受限角色——把演练也改回超级用户角色，等于把这一刀又还回去一半。
//!
//! 超权位本身收不掉，这条也钉在树上：`cog_user` 是 initdb 建出来的那个角色
//! （`POSTGRES_USER`），PostgreSQL 16 起不许任何人把它改成 NOSUPERUSER。所以这里
//! 判的不是「它不再是超权」，而是**没有任何工作负载连它**：它的口令不再注入任何
//! Pod，只剩 `postgres --single` 那一条恢复路径。

use crate::common;

use common::routes::{read, repo_root};

use std::fs;
use std::path::{Path, PathBuf};

/// 业务连库必须落在的那个角色。
const APP_ROLE: &str = "cog_app";

/// 备份演练连源库用的受限角色：建一次性演练库、读全部表比对、写自己那一行结论。
const BACKUP_ROLE: &str = "cog_backup";

/// 测试库（`cogneva_test`）的属主，变更验证跑实库用例时连它。
///
/// 它不是业务角色，也不是超级用户：要 CREATEDB 与 CREATEROLE 是因为包里那几条
/// 用例自己建库、自己建一个受限角色再删掉，而它读不到业务库里任何一行。
const TEST_ROLE: &str = "cog_test";

/// 这条连接是不是那条测试库连接。
///
/// 判据是它挂在哪条声明下，不是 URL 里的库名：库名能被改成别的，而声明名就是
/// 「这条连接是给谁用的」那一句话。
fn is_test_database(declared: Option<&str>) -> bool {
    declared.is_some_and(|name| name.eq_ignore_ascii_case("COGNEVA_TEST_DATABASE_URL"))
}

/// 实例的 bootstrap 超级用户（initdb 按 `POSTGRES_USER` 建出来的那个）。
///
/// 它的超权位收不掉——PostgreSQL 16 起不许任何人把它改成 NOSUPERUSER，因为同一个人
/// 也是 `postgres --single` 唯一能进的角色。所以判据落在**谁在连它**上：任何一份
/// 工作负载的 DSN 都不许是它。
const SUPERUSER_ROLE: &str = "cog_user";

/// 一份清单里每一条 `postgres://<role>:` 的角色名，连同它落在那条声明的名下。
///
/// 名下是要紧的：`COGNEVA_TEST_DATABASE_URL` 是**另一条连接**，不是业务库那一条。
/// 变更验证在自己 Pod 里跑的那批实库用例连的是测试库，那个角色生来就是
/// [`TEST_ROLE`]（它要 CREATEDB/CREATEROLE，业务角色整条设计就是不给这两项）。按
/// 角色一刀切会把这条正当写成 `cog_test` 的行读成「业务 Pod 用了别的角色」，
/// 而被指的那条连接从来没连过业务库。
fn dsn_roles(text: &str) -> Vec<(Option<String>, String)> {
    let mut found = Vec::new();
    let mut declared: Option<String> = None;
    for line in text.lines() {
        // 声明的名字在 URL 的上一行：YAML 是 `- name: X`，JSON 是同一个键。
        if let Some(rest) = line.trim().strip_prefix("- name: ") {
            declared = Some(rest.trim().to_string());
        } else if let Some(rest) = line.trim().strip_prefix("name: ") {
            declared = Some(rest.trim().to_string());
        }
        let mut rest = line;
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
                found.push((declared.clone(), rest[..end].to_string()));
            }
        }
    }
    found
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

/// 部署面上每一份带着连接串的文档：(文件, 文档序号, 声明名, 角色)。
fn deployed_dsn_roles(root: &Path) -> Vec<(String, usize, Option<String>, String)> {
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
            for (declared, role) in dsn_roles(&doc) {
                found.push((label.clone(), index, declared, role));
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
    let mut superuser_documents = Vec::new();
    let mut test_documents = Vec::new();
    for (file, index, declared, role) in &seen {
        // 测试库那条连接不是业务连接：它的角色就是 `cog_test`，正当。反过来也要钉住
        // ——写成业务角色就成了「拿 cog_app 跑那些自己建库删库的用例」，而它没有
        // CREATEDB。
        if is_test_database(declared.as_deref()) {
            test_documents.push(format!("{file} 第 {index} 份"));
            assert_eq!(
                role, TEST_ROLE,
                "{file} 第 {index} 份文档的测试库连接串连的是 `{role}`：它不是业务库那条\
                 连接，连的必须是 `{TEST_ROLE}`（它要 CREATEDB/CREATEROLE，而业务角色\
                 整条设计就是不给这两项）"
            );
            continue;
        }
        // 演练走的是自己的角色，不是「又一个业务角色」，也不是超级用户。判据是
        // **这份文档自己叫什么**：chart 的 `backup.yaml` 同时装着导出与演练两个
        // CronJob，按文件比会把「导出在 cog_app 上」这件事一起抹掉。
        if role == SUPERUSER_ROLE {
            superuser_documents.push(format!("{file} 第 {index} 份"));
            continue;
        }
        let is_drill = file.ends_with("backup.yaml") || file.ends_with("backup-drill-cronjob.yaml");
        if is_drill && drill_document(&root, file, *index) {
            drill_documents += 1;
            assert_eq!(
                role, BACKUP_ROLE,
                "{file} 第 {index} 份文档是演练：它要建一次性库（CREATEDB）、读全部表、\
                 写自己那一行结论，这些 `{BACKUP_ROLE}` 都有，而超级用户角色一个工作负载\
                 都不该连"
            );
            continue;
        }
        if role != APP_ROLE {
            offenders.push(format!("{file} 第 {index} 份文档: postgres://{role}:"));
        }
    }

    assert!(
        offenders.is_empty(),
        "业务连接串必须落在 {APP_ROLE} 上（演练在 {BACKUP_ROLE} 上）：\n  {}",
        offenders.join("\n  ")
    );

    // 判据的落点在「谁在连它」：超权位收不掉，可一个工作负载都不连它的时候，
    // 一条注入拿不到它，这个角色就只剩 `postgres --single` 那一条路。
    assert!(
        superuser_documents.is_empty(),
        "这些文档仍在用 {SUPERUSER_ROLE} 连库：{}——它的超权位收不掉，所以这一刀只能\
         落在「没有任何工作负载连它」上",
        superuser_documents.join("、")
    );

    // 演练那一侧也要钉住：把它改回业务角色，它会在真出事的那天才红——那时没有
    // 第二个包可选。
    assert!(
        drill_documents >= 2,
        "演练文档只钉到 {drill_documents} 份（chart 与静态各一份）：例外没有落到纸上，\
         下一次有人把它也改掉时这一条判不出来"
    );

    // 测试库那条连接也一样要钉住。它若从清单里消失，本门对 `cog_test` 的判断就变成
    // 一句没有题目的话——而它恰恰是唯一允许不是 `cog_app` 的那条业务之外的连接。
    assert!(
        test_documents.len() >= 2,
        "测试库连接串只读到 {} 份（chart 与静态各一份）：变更验证那些实库用例靠它连\
         测试库，它不在清单里时那些用例要么连业务库、要么一条都不跑",
        test_documents.len()
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

    // 口令键：chart 侧生成、init-secrets 侧生成，业务 Pod 只读它。演练那个角色
    // 用自己的一份，所以它也要在两条生成路径上都有——只在一个上，装出来就是
    // 「角色在、口令空」，连不上库而部署是绿的。
    for key in ["app-password", "backup-password"] {
        for (label, path) in [
            (
                "chart secret",
                root.join("deploy/helm/cogneva/templates/secret.yaml"),
            ),
            ("init-secrets", root.join("deploy/scripts/init-secrets.sh")),
        ] {
            assert!(
                read(&path).contains(key),
                "{label} 里没有 {key}：角色建出来没有口令，或者那一侧拿不到这份口令"
            );
        }
    }
}

/// 备份演练那个角色只带它真的用得上的三样。
///
/// 一个角色的权限少写一条不报错，多写一条也不报错——两边的证据都只有一行 `CREATE
/// ROLE`。所以这里逐条钉住：CREATEDB 要有（建一次性演练库）、超权位与 CREATEROLE
/// 不能有（它们不是演练要的东西）、不能是 `cog_app` 的成员（成员可以 `SET ROLE`
/// 过去，等于把业务角色的权限整包递过去）。读全部表走预定义角色而不是逐表 GRANT：
/// 逐表授的是一份快照，而演练的比对要读**每一张**表，漏一张读起来像一致。
#[test]
fn the_drill_role_carries_only_what_the_drill_needs() {
    let root = repo_root();
    let script = read(&root.join("deploy/helm/cogneva/files/db-role-bootstrap.sh"));

    for clause in [
        "CREATE ROLE cog_backup LOGIN NOSUPERUSER CREATEDB NOCREATEROLE NOREPLICATION",
        "ALTER ROLE cog_backup WITH LOGIN NOSUPERUSER CREATEDB NOCREATEROLE NOREPLICATION",
    ] {
        assert!(
            script.contains(clause),
            "引导脚本里没有 `{clause}`：演练那个角色会以默认属性被建出来（默认能登录、\
             不能建库——建不出演练库，那一趟连恢复都开始不了）"
        );
    }
    assert!(
        script.contains("GRANT pg_read_all_data TO cog_backup"),
        "演练那个角色没有读全部表：逐表 GRANT 是一份快照，而比对要读每一张表"
    );
    assert!(
        script.contains("GRANT INSERT ON public.backup_drills TO cog_backup"),
        "演练那个角色写不进 backup_drills：它给出的结论就写在那儿，写不进去这一趟等于没跑\
         （2026-11-01 实测：恢复与比对全部通过，最后一行 `permission denied for table backup_drills`）"
    );
    assert!(
        !script.contains("GRANT cog_app TO cog_backup"),
        "把演练那个角色授成 cog_app 的成员等于没分家：它可以 SET ROLE 过去"
    );

    // 超权位收不掉这件事要留在树里，否则下一个人会以为是漏了一步，去试一次
    // `ALTER ROLE cog_user NOSUPERUSER`，然后在部署时才遇到那条错。
    assert!(
        script.contains("The bootstrap user must have the SUPERUSER"),
        "引导脚本没有写下「bootstrap 超级用户的超权位收不掉」这件事：下一个人会重复试"
    );
    assert!(
        !script.contains("ALTER ROLE cog_user NOSUPERUSER"),
        "脚本里又出现了降权那一句：PostgreSQL 16 不许改 bootstrap 超级用户的超权位，\
         这一句会让每一次安装与升级都停在这里"
    );
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
            if roles.is_empty() {
                continue;
            }
            checked += 1;
            if doc.contains("key: pg-password") {
                offenders.push(label.clone());
            }
        }
    }

    assert!(checked > 0, "一份带连接串的文档都没读到");
    assert!(
        offenders.is_empty(),
        "这些文档用受限角色连库，却仍挂着实例超级用户的口令（任何工作负载都不该连它）：\
         {offenders:?}"
    );
}

/// 上面那条全量判据依赖「字典里装着每一个该被扫到的工作负载」。少扫一个文件，
/// 它写的 DSN 就不在 offenders 里，而整套仍然是绿的——空集与全合规同形。
#[test]
fn the_scan_covers_every_workload_that_takes_a_database_url() {
    let root = repo_root();
    let seen = deployed_dsn_roles(&root);

    let files: Vec<&str> = seen.iter().map(|(f, _, _, _)| f.as_str()).collect();
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
