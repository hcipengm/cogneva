//! principle: feedback_no_hardcoding_config_surface
//!
//! 一句话由十来处生产者各拼一遍，判据按其中一段 `contains`。
//!
//! 部署器把含「超时」的错误读成「我们没看到集群」，而不是「看清楚了集群有
//! 问题」。这个方向是刻意的：一次 apiserver 握手超时被读成版本缺陷，刚推上去
//! 的好版本会被回滚，而回滚只退镜像，修不好一次网络抖动。
//!
//! 判据按措辞判，于是措辞本身就成了契约。它原来由 `cog-reflection` 的十来处
//! 各写一遍（`run_cmd`、`kubectl apply`、`kubectl diff`、gitops 发布与拉取、
//! 镜像滚动、基线移植……），判据那边按其中一段裸字符串匹配。改一处措辞，编译
//! 通过、测试照跑，只是那一类错误从此不在判据里——而它消失的方向正好是「把抖
//! 动读成缺陷」，也就是这条判据存在的全部理由。
//!
//! 所以这句话现在只有一份，在 `cmd_timeout` 里：一个常量给判据认，两个构造函
//! 数给生产者拼。本门盯的就是「只有一份」——`cog-reflection` 的生产代码里，这
//! 句话作为字面量出现的地方必须恰好一处，且必须在那份声明里。
//!
//! 判不了什么：
//!
//! - 别的 crate 里同形的措辞（`cog-gateway` 的 `git timed out after {}s` 等）。
//!   它们不是这条判据的输入：没有哪条错误从那里流进 `is_cluster_unreachable`。
//!   若将来接上了，本门要跟着扩到那个 crate——扩的方式是加目录，不是放宽。
//! - 措辞的**语义**是否仍然合适。本门只保证改的时候两头一起动。

mod common;

use std::path::{Path, PathBuf};

use common::routes::repo_root;
use common::rust_production::production_lines;
use common::rust_production::{production_sources, relative_name};

/// 被判据认作「超时」的那一段。裸串：生产者写的是 `"{cmdline} timed out after
/// {secs}s"`，这段嵌在里面，带引号数就数不到它——那正是要拦的形状。
const SENTENCE: &str = "timed out after";

/// 允许写出这句话的唯一文件：常量与构造函数就住在这里。
const DECLARED_IN: &str = "crates/cog-reflection/src/cmd_timeout.rs";

/// 普查下限。测到的那一天 `cog-reflection/src` 有 70 余个源文件；读到的比这
/// 少说明本门没走到目录，而那时的「恰好一处」什么也不证明。
const MIN_FILES: usize = 40;

/// 生产行里这句话作为字面量出现的全部落点。
fn sites(root: &Path) -> (usize, Vec<(String, String)>) {
    let files: Vec<PathBuf> = production_sources(root)
        .into_iter()
        .filter(|path| relative_name(root, path).starts_with("crates/cog-reflection/src/"))
        .collect();
    let mut found = Vec::new();
    for path in &files {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, path);
        for line in production_lines(&text) {
            if line.contains(SENTENCE) {
                found.push((file.clone(), line.trim().to_string()));
            }
        }
    }
    (files.len(), found)
}

#[test]
fn the_timeout_sentence_is_spelt_where_the_verdict_reads_it() {
    let (files, found) = sites(&repo_root());
    assert!(
        files >= MIN_FILES,
        "只读到 {files} 个源文件（下限 {MIN_FILES}），这条判据没有题目"
    );

    let elsewhere: Vec<&(String, String)> = found
        .iter()
        .filter(|(file, _)| file != DECLARED_IN)
        .collect();

    assert!(
        found.len() == 1 && elsewhere.is_empty(),
        "「超时」这句话是判据的输入，只许有一份（`{DECLARED_IN}`），现在有 {} 处：\n{}\n\
         每一处自己拼的时候，改一处措辞另一处不会失败——判据那边安静地少认一类\
         错误，而这一类正是「apiserver 抖了一下」：把它读成版本缺陷就会回滚一个好\
         版本。生产者改用 `crate::cmd_timeout::timed_out(cmdline, secs)`，判据认\
         `crate::cmd_timeout::TIMED_OUT_AFTER`。",
        found.len(),
        found
            .iter()
            .map(|(file, line)| format!("    {file}: {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn the_census_counts_a_second_spelling_and_reads_nothing_out_of_prose() {
    // 生产行里的一处就是一处。
    let one = "pub(crate) const TIMED_OUT_AFTER: &str = \"timed out after\";";
    assert_eq!(one.matches(SENTENCE).count(), 1);
    // 第二处就是第二处，哪怕写得一模一样。
    let two = concat!(
        "let a = format!(\"{cmdline} timed out after {secs}s\");\n",
        "let b = format!(\"{cmdline} timed out after {secs}s\");\n",
    );
    assert_eq!(two.matches(SENTENCE).count(), 2);
    // 注释里提到这句话不是第二处拼写——带引号也不数，因为注释行不进生产行。
    let prose = "// 判据认的是 \"timed out after\" 这一段\nlet a = 1;\n";
    let counted: usize = production_lines(prose)
        .iter()
        .map(|line| line.matches(SENTENCE).count())
        .sum();
    assert_eq!(counted, 0);
}
