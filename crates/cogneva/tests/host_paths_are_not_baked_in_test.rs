//! 「这棵树在哪」不许写成某台机器上的字面量。
//!
//! `deploy/` 下的救援脚本手上都得知道检出根在哪：`pull-upstream-main.sh` 要在
//! 检出里 `git fetch`，`sync-git-remote.sh` 要把检出的 main 推进 bare 仓库，配套的
//! systemd 单元要把前者拉起来。三处都曾把这个位置写成 `/root/omc_workspace/cogneva`
//! ——那是某一台机器上的布局。换机之后它们并不是「坏了」，而是**以另一句话坏**：
//! 脚本以「源仓库不存在: /root/…」退出，读起来像这台机器没装好，实际是脚本自己
//! 记着一个不再存在的地址；单元则连启动都失败在同一个不存在的路径上。而这两条
//! 命令只在救援时手跑，平时没有任何东西读它们——所以它们可以一直是错的。
//!
//! 判据是「检出位置必须能从脚本自身推出来」的机械可读的一半：`deploy/` 里除测试
//! 夹具之外的每一行可执行文本，都不许出现 home 目录下的绝对路径。检出在哪由
//! `BASH_SOURCE` 往上数两级得出（`deploy/k3s/` → 检出根），这个推算逐机成立。
//!
//! 三个边界，各自都有反例，所以写清楚：
//!
//!   - **测试夹具不算**。`deploy/scripts/tests/*.test.sh` 在临时目录里搭一棵假树，
//!     把被测的检查脚本拷进 `${work}/root/deploy/scripts/` 再跑——那里的 `root` 是
//!     夹具目录的名字，不是这台机器的根。二十余份测试都是这个形状。
//!   - **注释与文档不算**。`deploy/README.md` 写的是**目标主机**上的路径
//!     （`/root/offsite-secrets` 这类凭据落点），那是被部署那一侧的固定位置，不是
//!     检出位置；容器与宿主上以 root 跑是常态。`.md` 不在人口里。
//!   - **片段不算**。`snapshots/*/fs/home/build` 里的 `/home/build` 是快照内部的
//!     相对片段，前面紧邻的是字母。判据只看前面不是字母、数字、下划线、`$`、`}`
//!     或 `/` 的那种绝对路径。
//!
//! 判据看不见什么，也写清楚：它只认 `deploy/`（外加两份入口脚本）里**以 home
//! 目录开头的绝对路径**。检出若落在 `/srv/…` 或 `/opt/…` 这类地方，字面量今天不会
//! 被拦——`/opt/cogneva` 与 `/srv/cogneva/models` 在树里是**目标侧**的正当路径
//! （装好的应用根、宿主上的模型缓存），把它们一起拦掉会误伤。真正兜底的是下一条：
//! 救援脚本必须留下那句从自身位置出发的推算，这样「地址写死」与「什么都没配」
//! 都不会再是同一个沉默。
//!
//! 这份文件自己也在人口之外（它不在 `deploy/` 下），所以可以原样举出上面那些标本。

use std::path::{Path, PathBuf};

mod common;

use common::routes::repo_root;

/// `deploy/` 下按文本判的扩展名。`.md` 不在其中：那里的路径讲的是目标主机。
const TEXT_EXTENSIONS: &[&str] = &[
    "sh", "bash", "service", "timer", "tpl", "yaml", "yml", "json", "py", "ps1",
];

/// 夹具目录：假树里那个叫 `root` 的临时目录不是任何一台机器的根。
const FIXTURES: &str = "deploy/scripts/tests/";

/// 人口下限。扫不到东西的门与「全都干净」在输出上同形，所以少了这么多份就报错。
/// 当前是五百余份，取四百留出删文件的空间而不留出「目录名写错」的空间。
const MIN_FILES: usize = 400;

/// 必须从自身位置推出检出根的两条救援脚本，以及它们各自该有的那句推算。
const RESCUE_SCRIPTS: &[(&str, &str)] = &[
    ("deploy/k3s/pull-upstream-main.sh", "BASH_SOURCE"),
    ("deploy/k3s/sync-git-remote.sh", "BASH_SOURCE"),
];

/// 人口里 `deploy/` 之外的那两份：元启动的两个入口。它们在目标主机上被跑起来，
/// 也同样需要知道自己在哪儿，所以和部署脚本是同一个问题。
const ENTRY_SCRIPTS: &[&str] = &["bootstrap.sh", "bootstrap.ps1"];

fn text_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for rel in ENTRY_SCRIPTS {
        let path = root.join(rel);
        if path.is_file() {
            out.push(path);
        }
    }
    let mut stack = vec![root.join("deploy")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let keeps = path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| TEXT_EXTENSIONS.contains(&e));
            if !keeps {
                continue;
            }
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if rel.starts_with(FIXTURES) {
                continue;
            }
            out.push(path);
        }
    }
    out.sort();
    out
}

/// 这一行里的 home 目录绝对路径，若有。
///
/// 前面紧邻的字符决定它是不是一个真的绝对路径：`fs/home/build` 里的是片段，
/// `${1:-/root/x}` 里的是默认值（`-` 是边界），`--out=/home/u/x` 里的是参数值。
fn baked_in(line: &str) -> Option<String> {
    let bytes = line.as_bytes();
    let mut from = 0usize;
    while let Some(found) = line[from..].find('/') {
        let at = from + found;
        from = at + 1;
        let boundary = match at.checked_sub(1).map(|i| bytes[i]) {
            None => true,
            Some(prev) => {
                !(prev.is_ascii_alphanumeric()
                    || prev == b'_'
                    || prev == b'$'
                    || prev == b'}'
                    || prev == b'/')
            }
        };
        if !boundary {
            continue;
        }
        let rest = &line[at..];
        for home in ["/root/", "/Users/"] {
            if let Some(tail) = rest.strip_prefix(home) {
                if !tail.is_empty() {
                    return Some(rest.split_whitespace().next().unwrap_or(rest).to_string());
                }
            }
        }
        if let Some(tail) = rest.strip_prefix("/home/") {
            // `/home/` 之后必须真有一个用户目录名下的一段，`/home/build` 这种
            // 片段也算，因为它的前面已经被上面那道边界判过了。
            if !tail.is_empty() && !tail.starts_with('/') {
                return Some(rest.split_whitespace().next().unwrap_or(rest).to_string());
            }
        }
    }
    None
}

/// 一份文件里的违规行：`(行号, 那一行)`。
fn violations_in(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim_start().starts_with('#') {
            continue;
        }
        if baked_in(line).is_some() {
            out.push((index + 1, line.trim().to_string()));
        }
    }
    out
}

fn scan(root: &Path, floor: usize) -> Vec<String> {
    let files = text_files(root);
    assert!(
        files.len() >= floor,
        "只扫到 {} 份 deploy/ 下的文本文件（下限 {floor}）：人口塌了，这条门此刻什么都证明不了",
        files.len()
    );
    let mut problems = Vec::new();
    for file in &files {
        let rel = file
            .strip_prefix(root)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\\', "/");
        // 读不出来就报错：把读失败当成空文件，等于让这份文件悄悄退出人口，
        // 而「它干净」与「没读到它」在结果上同形。
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("{} 读不出来：{e}", file.display()));
        for (line_no, line) in violations_in(&text) {
            problems.push(format!("{rel}:{line_no}: {line}"));
        }
    }
    problems
}

/// 真树：一条都不许有。
#[test]
fn no_file_under_deploy_names_a_checkout_by_a_host_path() {
    let root = repo_root();
    let problems = scan(&root, MIN_FILES);
    assert!(
        problems.is_empty(),
        "deploy/ 下把检出位置写成了某台机器上的路径（检出位置逐机不同，从 BASH_SOURCE 往上数两级推）：\n{}",
        problems.join("\n")
    );
}

/// 两条救援脚本必须真的还在推算，而不是把字面量删成了空串。
#[test]
fn the_rescue_scripts_derive_the_checkout_from_their_own_location() {
    let root = repo_root();
    for (rel, needle) in RESCUE_SCRIPTS {
        let path = root.join(rel);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} 读不出来：{e}", path.display()));
        assert!(
            text.contains(needle),
            "{rel} 里没有从自身位置出发的推算（找 {needle}）：检出根要么被别人传进来，要么就无从得知"
        );
    }
}

/// 假树上的对照：判据要能认出那三种写法，也要放过三种正当写法。
#[test]
fn the_predicate_reads_the_form_not_the_substring() {
    // 正例：三种真的写死了某台机器上的检出位置。
    for line in [
        r#"SRC_REPO="${1:-/root/omc_workspace/cogneva}""#,
        "ExecStart=/root/omc_workspace/cogneva/deploy/k3s/pull-upstream-main.sh",
        "cp x /home/alice/work/cogneva/README.md",
        "cp x /Users/alice/work/cogneva/README.md",
    ] {
        assert!(
            baked_in(line).is_some(),
            "这条是写死的机器路径，判据却没认出来：{line}"
        );
    }
    // 反例：片段、注释外的目标主机路径由调用方传入、以及推算出来的结果。
    for line in [
        "# snapshots/*/fs/home/build 属 uid 1000",
        r#"ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)""",
        r#"BARE="${2:-/var/lib/cogneva-data/git-remote}""#,
        "https://github.com/home/somebody",
    ] {
        assert!(
            baked_in(line).is_none(),
            "这条不是写死的机器路径，判据却判成了：{line}"
        );
    }
}

/// 假树整体：夹具目录里的 `root` 不算，别处的算。
#[test]
fn fixture_directories_are_not_the_root_of_any_machine() {
    let scratch = std::env::temp_dir().join(format!(
        "cogneva-host-paths-{}-{}",
        std::process::id(),
        line!()
    ));
    let _ = std::fs::remove_dir_all(&scratch);
    let fixture_dir = scratch.join("deploy/scripts/tests");
    std::fs::create_dir_all(&fixture_dir).expect("建夹具目录");
    let clean = scratch.join("deploy/k3s");
    std::fs::create_dir_all(&clean).expect("建干净目录");
    std::fs::write(
        fixture_dir.join("some.test.sh"),
        "mkdir -p \"${work}/root/deploy/scripts\"\n",
    )
    .expect("写夹具");
    std::fs::write(clean.join("ok.sh"), "ROOT=\"$(pwd)\"\n").expect("写干净脚本");

    assert!(
        scan(&scratch, 1).is_empty(),
        "夹具里的 root 与推算出来的根都不该算违规"
    );

    std::fs::write(
        clean.join("bad.sh"),
        "SRC=\"${1:-/root/omc_workspace/cogneva}\"\n",
    )
    .expect("写坏脚本");
    let problems = scan(&scratch, 1);
    assert_eq!(problems.len(), 1, "只该有那一行：{problems:?}");
    assert!(
        problems[0].starts_with("deploy/k3s/bad.sh:1:"),
        "违规要指名到行：{}",
        problems[0]
    );

    // 人口塌了要红，而不是报「干净」。
    let empty = scratch.join("empty");
    std::fs::create_dir_all(&empty).expect("建空目录");
    let panicked = std::panic::catch_unwind(|| scan(&empty, 1)).is_err();
    assert!(panicked, "扫不到文件时必须报错，不能静默通过");

    let _ = std::fs::remove_dir_all(&scratch);
}
