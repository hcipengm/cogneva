//! principle: feedback_no_hardcoding_config_surface
//!
//! A store that creates its table on first start leaves that table in the live
//! database and in no migration file. Restoring a backup package replays the
//! schema of the image doing the restore before any `COPY` runs, so a table the
//! image cannot rebuild fails the restore at the first row — and the drill
//! reports the *package* as unrestorable when it was the image that could not
//! describe its own shape.
//!
//! `schema_bundle` answers that with a list of the stores' own DDL, and a unit
//! test there compares the tables it carries against the tables those files
//! create. Both sides of that comparison are drawn from one list, the
//! `include_str!` block — which is why it cannot see the failure it is named
//! after: a *new* store, in a file nobody added to that block, creates a table
//! that reaches neither list, and the test stays green because it never looked
//! at that file. A census of the tree is the half the bundle cannot supply.
//!
//! 实测（2026-10-27）：生产半边有 20 个文件建持久表，18 个在 bundle 里，
//! 另外 2 个按理由登记在下面（迁移自己的账本、分区维护器）。临时表不进账：
//! 它随会话生灭，进不了包，恢复也不用重建。
//!
//! What it judges:
//!
//! * every production file that runs `CREATE TABLE` is either named by an
//!   `include_str!` in the bundle, or registered below with the reason its
//!   tables are rebuilt another way;
//! * every `include_str!` in the bundle still resolves to a file, and that file
//!   still creates a table — a bundle entry kept for a file that stopped
//!   creating one is a line pretending to cover something.
//!
//! What it cannot judge:
//!
//! * whether the *statements* are complete. A store may create three tables in
//!   one file and export a constant that only carries two; the diff would be
//!   inside a file the bundle already names, which is the unit test's half;
//! * a table created by a migration rather than by a store. Those are rebuilt
//!   by running migrations at start-up, and migrations are `.sql` files this
//!   scan does not read;
//! * a table created from a name that is assembled at run time from parts. The
//!   file would still be caught here (the literal carries `CREATE TABLE`), but
//!   the name would not.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::common;

use common::routes::read;

use common::routes::repo_root;

use common::rust_production::{
    production_sources, production_text, relative_name, sources_of_tree,
};

const BUNDLE: &str = "crates/cogneva/src/schema_bundle.rs";

/// Files that create a table and are deliberately absent from the bundle.
///
/// Each of these creates something the restore path rebuilds another way, so
/// adding it to the bundle would run the same DDL twice — which is louder than
/// the gap, but wrong all the same.
const NOT_IN_THE_BUNDLE: &[(&str, &str)] = &[
    (
        "crates/cog-storage/src/migrate/mod.rs",
        "迁移自己的账本表：它由迁移器在重放时建，进 bundle 就是让同一句 DDL 跑两遍",
    ),
    (
        "crates/cog-storage/src/partition_maintainer.rs",
        "分区：父表由迁移建，分区是装进表里的数据，不是空库接纳行所需的形状",
    ),
];

/// The files the bundle's `include_str!` block names, as repo-relative paths.
///
/// The macro resolves relative to the file holding it, so `../../cog-storage/…`
/// written in `crates/cogneva/src/` means `crates/cog-storage/…`.
fn bundled_files(bundle: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = bundle;
    while let Some(at) = rest.find("include_str!(") {
        rest = &rest[at + "include_str!(".len()..];
        let Some(open) = rest.find('"') else { break };
        let after = &rest[open + 1..];
        let Some(close) = after.find('"') else { break };
        let raw = &after[..close];
        rest = &after[close..];
        let rel = raw.trim_start_matches("../").trim_start_matches("../");
        out.push(format!("crates/{rel}"));
    }
    out
}

/// Every string literal in a stretch of production text.
///
/// SQL in this tree is written as multi-line raw strings (`r#"…"#`) as often as
/// as plain ones, and a scan that only knew `"` would miss most of the DDL.
fn literals(text: &str) -> Vec<String> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'/' && b.get(i + 1) == Some(&b'/') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            let mut depth = 1;
            while i < b.len() && depth > 0 {
                if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                    depth += 1;
                    i += 2;
                    continue;
                }
                if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                    depth -= 1;
                    i += 2;
                    continue;
                }
                i += 1;
            }
            continue;
        }
        // Raw string: `r"…"`, `r#"…"#`, `r##"…"##`.
        if b[i] == b'r' && b.get(i + 1).is_some_and(|c| *c == b'"' || *c == b'#') {
            let mut j = i + 1;
            let mut hashes = 0;
            while b.get(j) == Some(&b'#') {
                hashes += 1;
                j += 1;
            }
            if b.get(j) == Some(&b'"') {
                let start = j + 1;
                j = start;
                loop {
                    if j >= b.len() {
                        break;
                    }
                    if b[j] == b'"' {
                        let mut seen = 0;
                        while seen < hashes && b.get(j + 1 + seen) == Some(&b'#') {
                            seen += 1;
                        }
                        if seen == hashes {
                            break;
                        }
                    }
                    j += 1;
                }
                out.push(text[start..j.min(text.len())].to_string());
                i = j + 1 + hashes;
                continue;
            }
        }
        if b[i] == b'"' {
            let start = i + 1;
            let mut j = start;
            while j < b.len() {
                if b[j] == b'\\' {
                    j += 2;
                    continue;
                }
                if b[j] == b'"' {
                    break;
                }
                j += 1;
            }
            out.push(text[start..j.min(text.len())].to_string());
            i = j + 1;
            continue;
        }
        i += 1;
    }
    out
}

/// The table names one SQL literal creates, one entry per statement.
///
/// An entry is empty when the name is a placeholder — `{table}`, `{}.{}` — which
/// is exactly how the analytics store and the partition maintainer write theirs.
/// Those files still create a table; only the name is not spelled here.
fn statement_names(literal: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (at, _) in literal.match_indices("CREATE") {
        let rest = literal[at + "CREATE".len()..].trim_start();
        // `TEMPORARY` and `UNLOGGED` sit between the verb and the noun.
        //
        // A temporary table is not a store table: it lives and dies with the
        // session that made it, so no backup can carry it and no restore has to
        // rebuild it. `UNLOGGED` is a different matter — it survives the
        // session and a dump carries it — so only the temporary spellings are
        // stepped over.
        if ["TEMPORARY ", "TEMP "]
            .iter()
            .any(|prefix| rest.starts_with(prefix))
        {
            continue;
        }
        let rest = rest
            .strip_prefix("UNLOGGED ")
            .map(|rest| rest.trim_start())
            .unwrap_or(rest);
        let Some(rest) = rest.strip_prefix("TABLE") else {
            continue;
        };
        if rest
            .chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            continue;
        }
        let rest = rest.trim_start();
        let rest = rest
            .strip_prefix("IF NOT EXISTS")
            .unwrap_or(rest)
            .trim_start();
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
            .collect();
        out.push(name);
    }
    out
}

/// Every table a stretch of production text creates, by name where the name is
/// spelled out. An empty answer means no `CREATE TABLE` ran at all.
fn tables_created(production: &str) -> Vec<String> {
    literals(production)
        .iter()
        .flat_map(|literal| statement_names(literal))
        .collect()
}

/// Every production file that creates a table, as `(repo-relative path, names)`.
fn table_creators(root: &Path, sources: &[PathBuf]) -> Vec<(String, BTreeSet<String>)> {
    let mut out = Vec::new();
    for path in sources {
        let text = read(path);
        let statements = tables_created(&production_text(&text));
        if statements.is_empty() {
            continue;
        }
        let tables: BTreeSet<String> = statements.into_iter().filter(|n| !n.is_empty()).collect();
        out.push((relative_name(root, path), tables));
    }
    out
}

fn disagreements(root: &Path, registered: &[(&str, &str)], sources: &[PathBuf]) -> Vec<String> {
    let mut out = Vec::new();
    let bundle = read(&root.join(BUNDLE));
    let bundled = bundled_files(&bundle);
    let named: BTreeSet<&str> = bundled.iter().map(String::as_str).collect();
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for (file, tables) in table_creators(root, sources) {
        seen.insert(file.clone());
        if named.contains(file.as_str()) {
            continue;
        }
        if registered.iter().any(|(path, _)| *path == file) {
            continue;
        }
        let spelled = if tables.is_empty() {
            "（表名是拼出来的，见该文件的 DDL）".to_string()
        } else {
            format!("{tables:?}")
        };
        out.push(format!(
            "{file} 建了表 {spelled}，但 `schema_bundle` 的 `include_str!` 里没有这个文件：\
             备份把它从库里导了出来，恢复时镜像重放的 schema 不认识这张表，\
             第一条 `COPY` 就失败——而演练会把包判成不可恢复"
        ));
    }

    for file in &bundled {
        if !seen.contains(file) {
            out.push(format!(
                "`schema_bundle` 的 `include_str!` 里列着 {file}，但那里已经没有建表语句了：\
                 这条登记过期"
            ));
        }
    }
    for (file, _) in registered {
        if !seen.contains(*file) {
            out.push(format!(
                "`NOT_IN_THE_BUNDLE` 里登记的 {file} 已经不建表了，这条登记过期"
            ));
        }
    }
    out
}

#[test]
fn every_store_that_creates_a_table_is_in_the_restore_bundle() {
    let root = repo_root();
    // 抽取面自检：一个文件都没扫到、或 bundle 一个文件都没读到时，下面那条
    // 判据会在两张空表上相等而假装绿。
    let creators = table_creators(&root, &production_sources(&root));
    assert!(
        creators.len() >= 15,
        "生产半边只认出 {} 个建表的文件，扫描器本身坏了",
        creators.len()
    );
    let bundled = bundled_files(&read(&root.join(BUNDLE)));
    assert!(
        bundled.len() >= 15,
        "只从 {BUNDLE} 里读到 {} 个 include_str!，解析本身坏了",
        bundled.len()
    );
    for known in [
        "crates/cog-storage/src/postgres/user_store.rs",
        "crates/cog-observability/src/usage_store.rs",
    ] {
        assert!(
            bundled.iter().any(|f| f == known),
            "bundle 里读不到 {known}：解析坏了或它真的被移出了"
        );
    }

    let found = disagreements(&root, NOT_IN_THE_BUNDLE, &production_sources(&root));
    assert!(
        found.is_empty(),
        "有 store 在库里建表，而恢复用的 bundle 不知道：\n{}\n\
         这张表只存在于「已经在跑的那个库」里——新装的镜像重放 schema 时建不出它，\
         而备份是从活库的目录里发现表名的，所以包里会有它，恢复时才会发现接不住",
        found.join("\n")
    );
}

/// 判据自己也要有判据：一棵小树里 bundle 列了一个建表文件、漏了另一个，
/// 判出来的必须正是漏掉的那个；补进 bundle 之后必须转空。
#[test]
fn a_store_outside_the_bundle_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    let bundle_dir = root.join("crates/cogneva/src");
    std::fs::create_dir_all(&bundle_dir).expect("bundle dir");
    let store_dir = root.join("crates/cog-example/src");
    std::fs::create_dir_all(&store_dir).expect("store dir");
    std::fs::write(
        store_dir.join("kept.rs"),
        "pub const DDL: &str = r#\"\nCREATE TABLE IF NOT EXISTS kept (\n    id BIGINT\n);\n\"#;\n",
    )
    .expect("kept");
    std::fs::write(
        store_dir.join("forgotten.rs"),
        "pub fn ddl() -> String { \"CREATE TABLE IF NOT EXISTS forgotten (id BIGINT)\".to_string() }\n",
    )
    .expect("forgotten");

    let bundle = |lines: &str| {
        format!(
            "const SOURCES: &[&str] = &[\n{lines}];\n\
             fn needle() -> &'static str {{ \"CREATE TABLE IF NOT EXISTS\" }}\n"
        )
    };
    std::fs::write(
        bundle_dir.join("schema_bundle.rs"),
        bundle("    include_str!(\"../../cog-example/src/kept.rs\"),\n"),
    )
    .expect("bundle");

    // bundle 自己建「表」是因为拿它当针，登记掉。
    let registered = [("crates/cogneva/src/schema_bundle.rs", "针")];
    let found = disagreements(root, &registered, &sources_of_tree(root));
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("forgotten.rs"), "{found:?}");
    assert!(found[0].contains("forgotten"), "{found:?}");

    // 补进 bundle：必须转空。
    std::fs::write(
        bundle_dir.join("schema_bundle.rs"),
        bundle(
            "    include_str!(\"../../cog-example/src/kept.rs\"),\n\
             \x20   include_str!(\"../../cog-example/src/forgotten.rs\"),\n",
        ),
    )
    .expect("bundle");
    assert_eq!(
        disagreements(root, &registered, &sources_of_tree(root)),
        Vec::<String>::new()
    );

    // 被 bundle 列着的文件不再建表了：这条登记过期。
    std::fs::write(
        store_dir.join("kept.rs"),
        "pub const DDL: &str = \"SELECT 1\";\n",
    )
    .expect("kept");
    let stale = disagreements(root, &registered, &sources_of_tree(root));
    assert_eq!(stale.len(), 1, "{stale:?}");
    assert!(stale[0].contains("过期"), "{stale:?}");
}

/// 解析面自己也要有判据，否则「读到零张表」与「真的一句 DDL 都没有」不可分。
#[test]
fn the_table_reader_sees_raw_and_plain_literals_and_skips_comments() {
    let production = "const A: &str = r#\"\nCREATE TABLE IF NOT EXISTS alpha (id BIGINT);\n\"#;\n\
                      fn b() -> &'static str { \"CREATE TABLE beta (id BIGINT)\" }\n\
                      fn c() -> &'static str { \"CREATE TEMPORARY TABLE gamma (id BIGINT)\" }\n\
                      // CREATE TABLE commented (id BIGINT)\n\
                      /* CREATE TABLE blocked (id BIGINT) */\n\
                      fn d() -> &'static str { \"SELECT * FROM delta\" }\n";
    let tables: BTreeSet<String> = tables_created(production).into_iter().collect();
    // gamma 是 `CREATE TEMPORARY TABLE`：随会话生灭的表进不了包，恢复也不用重建它。
    assert_eq!(
        tables,
        ["alpha", "beta"]
            .iter()
            .map(|s| s.to_string())
            .collect::<BTreeSet<_>>()
    );
    // 名字是占位符时必须仍然算「建了表」，否则 bundle 里那两处拼表名的 store
    // 会被读成「不建表」，登记过期与漏登记两个方向都会判错。
    assert_eq!(
        tables_created("fn ddl(t: &str) -> String { format!(\"CREATE TABLE IF NOT EXISTS {t} (id BIGINT)\") }\n"),
        [String::new()]
    );
}
