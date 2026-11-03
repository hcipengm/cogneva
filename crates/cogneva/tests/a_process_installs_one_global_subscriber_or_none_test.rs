//! Gate: 一支二进制里，全局 tracing 订阅者只许装一次，装它的地方归装配层管。
//!
//! 2026-11-03 的现场：集群里的 `cogneva-mainline-rollout` Job 启动即退，exit 101，
//! 日志只有一行
//! `Unable to install global subscriber: SetGlobalDefaultError("a global default
//! trace dispatcher has already been set")`，`BackoffLimitExceeded` 之后部署器停在
//! 原 rev 上不再前进。写错任何一行，都不是。
//!
//! 是两个写者都以为自己才是那个唯一：装配层（`main.rs` 的 `standalone_log_service`）
//! 为十一个独立子命令装了一次，`cog-reflection` 与 `cog-extension` 里各自的
//! `run_*_cli` 又以「子命令不经 `run_app`，得自己装」为理由装了第二次。两份都编译得
//! 过、单测都跑不到——每一处分开看都成立，只有把它们链接成同一支二进制、走到那个
//! 子命令时才撞上，而那时它已经是个 Job 了。
//!
//! 所以判据落在树上，问两个问题：
//!
//! 1. `crates/*/src` 的生产正文里，凡是一条语句同时出现 `tracing_subscriber`（或
//!    `tracing::subscriber`）与 `init()`／`::init()`／`set_global_default(` 的，
//!    必须正好是下面声明的那几处，每处带一条理由。多一处是又有一个写者来了；
//!    少一处是声明的豁免已经过期——没人重新决定过的豁免不许活过它的理由。
//! 2. 装配层那张表不许收 `mcp-serve`：它自己装订阅者，装的同时再让装配层给同一个
//!    进程装一次，panic 一模一样地回来。表与它的处理器分居两个 crate，谁也看不见谁。
//!
//! 判不了什么：不判 `try_init()` 那条路（它返回 `Result`、失败只记一行，不算装上了）；
//! 不判「先建 builder 存在变量里、后面再 `.init()`」这种跨语句的形状——今天树上没有，
//! 而为了它去写一个半吊子的数据流，判据本身会比它防的病更不可靠。

use crate::common;

use common::lex::{strip, Dialect};
use common::routes::{read, repo_root};
use common::rust_production::{production_sources, production_text, relative_name};

use std::collections::BTreeMap;

/// 装全局订阅者的写法。三条都进同一个全局槽，第二条装上时第二条就 panic：
/// `SubscriberBuilder::init` 内部是 `try_init().expect("Unable to install global
/// subscriber")`，`tracing_subscriber::fmt::init()` 是它的简写，
/// `set_global_default` 由调用方 `unwrap` 时同样。`try_init()` 刻意不在表里——
/// 它失败时只返回一个 `Err`，两条并存的进程不会死。
const INSTALL_CALLS: [&str; 3] = [".init(", "::init(", "set_global_default("];

/// 生产正文里唯一允许装全局订阅者的三处，每处一条理由。
///
/// 读法：多一处，说明有一支新二进制或一条新路径自己装上了，装配层那边迟早会再装一次；
/// 少一处，说明这里的理由已经不成立——要么它被删了却没删这一行，要么装配层已经接管了它
/// 而这一行还在替它说话。两种都要重新决定。
const DECLARED: [(&str, &str); 3] = [
    (
        "crates/bootstrap/src/main.rs",
        "独立二进制（引导器）：没有装配层为它装订阅者，它自己就是那一层",
    ),
    (
        "crates/cog-storage/src/migrate/main.rs",
        "独立二进制的 migrate CLI：同上，进程里只有它一个",
    ),
    (
        "crates/cogneva/src/mcp_serve.rs",
        "同一支二进制里唯一不归装配层管的子命令：`STANDALONE_LOG_SERVICES` 表里没有它，\
         所以它装的这一次就是这一次",
    ),
];

/// 装配层的那张表：列进去的子命令由 `main.rs` 在派发前装订阅者。
const ASSEMBLY: &str = "crates/cogneva/src/main.rs";
const TABLE: &str = "STANDALONE_LOG_SERVICES";

/// 一条语句的起点：最近的 `;` `{` `}` 之后。
fn statement_start(code: &str, at: usize) -> usize {
    code[..at]
        .rmatch_indices([';', '{', '}'])
        .next()
        .map_or(0, |(i, _)| i + 1)
}

/// 一条语句的终点：最近的分号（含）。
fn statement_end(code: &str, at: usize) -> usize {
    code[at..].find(';').map_or(code.len(), |i| at + i + 1)
}

/// 这份正文里所有「装全局订阅者」的调用点，按字节位置给出。
///
/// 按**语句**判而不是按裸子串判：`tracing_subscriber` 这七个字出现在一行里，与它
/// 出现在同一行的是两件事——一份只在 `EnvFilter` 上用它，一份真的在装。按子串判会把
/// 前者也算进来，于是一份门靠一批假条目撑着自己的非空，而真装机的那一行改个换行就漏了。
fn install_calls(text: &str) -> Vec<usize> {
    let code = strip(text, Dialect::Rust);
    let mut out = Vec::new();
    for needle in INSTALL_CALLS {
        let mut from = 0;
        while let Some(rel) = code[from..].find(needle) {
            let at = from + rel;
            let statement = &code[statement_start(&code, at)..statement_end(&code, at)];
            let names_a_subscriber = statement.contains("tracing_subscriber")
                || statement.contains("tracing::subscriber");
            if names_a_subscriber {
                out.push(statement_start(&code, at));
            }
            from = at + needle.len();
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn line_of(text: &str, at: usize) -> usize {
    text[..at].bytes().filter(|b| *b == b'\n').count() + 1
}

/// 这一棵树里每一处装机的地方，键是仓库相对名。
fn installers_in_this_tree() -> BTreeMap<String, Vec<usize>> {
    let root = repo_root();
    let mut out = BTreeMap::new();
    for path in production_sources(&root) {
        let text = production_text(&read(&path));
        let calls = install_calls(&text);
        if !calls.is_empty() {
            out.insert(
                relative_name(&root, &path),
                calls.iter().map(|at| line_of(&text, *at)).collect(),
            );
        }
    }
    out
}

#[test]
fn the_only_global_subscribers_are_the_declared_ones() {
    let found = installers_in_this_tree();
    assert!(
        !found.is_empty(),
        "一个装全局订阅者的地方都没读到：装配层自己就在装，读到空集说明这份普查没有走到 \
         crates/*/src"
    );

    let declared: BTreeMap<&str, &str> = DECLARED.into_iter().collect();

    let mut extra = Vec::new();
    for (file, lines) in &found {
        if !declared.contains_key(file.as_str()) {
            extra.push(format!("{file}:{lines:?}"));
        }
    }
    assert!(
        extra.is_empty(),
        "这些地方自己装了全局订阅者，而它们没有在 DECLARED 里：{extra:?}。\
         装配层已经为 `main.rs` 里 `STANDALONE_LOG_SERVICES` 列出的每个子命令装过一次，\
         处理器再装一次就是 `a global default trace dispatcher has already been set`——\
         编译过、单测过，只在 Job 里 panic。要么删掉这里的安装，要么把理由写进 DECLARED。"
    );

    let mut stale = Vec::new();
    for (file, why) in &declared {
        if !found.contains_key(*file) {
            stale.push(format!("{file}（{why}）"));
        }
    }
    assert!(
        stale.is_empty(),
        "DECLARED 里这些条目的理由已经不成立：{stale:?}——它们的生产正文里再没有装全局订阅者。\
         没人重新决定过的豁免不许活过它的理由：删掉这一条，别让它替一处不存在的安装说话。"
    );
}

/// 装配层那张表与 `mcp-serve` 的自由身是一对：表收它，它自己再装，就 `mcp-serve` 也 panic。
///
/// 这条之所以单列：`main.rs` 那张表与 `mcp_serve.rs` 那一行分居两个文件，改任何一边
/// 另一边都看不见，而它们合起来的效果只有跑起来才知道。
#[test]
fn the_assembly_does_not_also_install_for_the_one_command_that_installs_itself() {
    let assembly = production_text(&read(&repo_root().join(ASSEMBLY)));
    let start = assembly.find(TABLE).unwrap_or_else(|| {
        panic!("{ASSEMBLY} 里找不到 {TABLE}：装配层的表改名了，这条判据得跟着走")
    });
    let table = &assembly[start..];
    let table = &table[..table.find("];").map_or(table.len(), |i| i + 2)];

    for name in ["McpServe", "mcp-serve"] {
        assert!(
            !table.contains(name),
            "{TABLE} 收了 {name}，而 `mcp_serve.rs` 自己调 `init()`：装配层先装一次、\
             它再装一次，`mcp-serve` 会和 2026-11-03 的 `mainline-rollout` 一样启动即 panic。\
             要么把它从表里拿掉，要么让它别再自己装。"
        );
    }
}

/// 判据自己的两格：一条会响、一条不许响。
///
/// 缺了它，「装全局订阅者」的识别一旦退化成永远读不到，上面两条断言会一起变成空断言
/// 而仍然是绿的——这正是这份门要防的那种「都合规」。
#[test]
fn the_detector_knows_an_install_from_a_filter() {
    let installed = r#"
        pub async fn run() {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .init();
        }
    "#;
    assert_eq!(
        install_calls(installed).len(),
        1,
        "一条链式 .init() 必须认出来"
    );

    let short = "fn f() { tracing_subscriber::fmt::init(); }";
    assert_eq!(install_calls(short).len(), 1, "`fmt::init()` 是同一种装法");

    let filter_only = r#"
        fn level() -> tracing_subscriber::EnvFilter {
            tracing_subscriber::EnvFilter::try_new("info").unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        }
    "#;
    assert!(
        install_calls(filter_only).is_empty(),
        "只在 EnvFilter 上提到 tracing_subscriber 不是装订阅者"
    );

    let idempotent = r#"
        fn try_install() {
            let _ = tracing_subscriber::registry().try_init();
        }
    "#;
    assert!(
        install_calls(idempotent).is_empty(),
        "`try_init()` 失败只返回 Err，两条并存不会退出进程，不算装机"
    );

    let commented = "// tracing_subscriber::fmt().init();\nfn f() {}";
    assert!(
        install_calls(commented).is_empty(),
        "注释里写着的装法不是装法"
    );
}
