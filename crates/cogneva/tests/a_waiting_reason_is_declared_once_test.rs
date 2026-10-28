//! principle: feedback_no_hardcoding_config_surface
//!
//! 「哪些 Pod 等待态是判死的」是一份词表，滚动侧与金丝雀侧都要读它。它曾经被写了
//! 两遍：`mainline_deployer` 里三词的正本，和 `gitops_puller` 里另抄的五词清单。
//!
//! 两份清单都「能跑」，所以没有任何东西会失败——但它们给出的判断相反。滚动侧把
//! 镜像拉取失败单独收成一组并写明理由：拉取失败可能只是 registry 此刻在重启，
//! 判死就会 undo 掉一份完好的版本（线上实测过一次，一次 support apply 让 registry
//! 与四个后端一起重启，目标 Pod 四秒后被读成版本类失败并回滚）。金丝雀那份五词
//! 清单里带着 `ErrImagePull` 与 `ImagePullBackOff`：同一件事，一个说等等、一个说
//! 判死。谁对不取决于哪份写得好看，取决于谁读过那次实测——而抄写不携带这件事。
//!
//! 现在这两个词表各自只有一处：判死的三词在 `mainline_deployer`，拉取失败的两个
//! 在 `mainline_rollout`，两侧都引用它们。本门盯的是「词表不再被第三次抄」：这五个
//! 词在 `crates/*/src` 的生产行里各只许出现一次，且必须落在声明那两个词表的文件里。
//!
//! 判不了什么：
//!
//! - 一个新造出来的等待态词（`RunContainerError` 之类）。本门钉的是这五个已经分叉
//!   过的词；新词第一次出现时没有第二处可比，也就没有分叉可言。
//! - 两个词表各自该收哪些词。判死与「拉取失败」两个集合的边界是那次实测的结论，
//!   由 `mainline_deployer` 的单测与金丝雀那三条断言看着。

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use common::routes::repo_root;
use common::rust_production::{production_lines, production_sources, relative_name};

/// 已经分叉过一次的那五个词，以及它们各自该被声明在哪。
const VOCABULARY: &[(&str, &str)] = &[
    (
        "InvalidImageName",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    (
        "CreateContainerConfigError",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    (
        "CrashLoopBackOff",
        "crates/cog-reflection/src/mainline_deployer.rs",
    ),
    (
        "ErrImagePull",
        "crates/cog-reflection/src/mainline_rollout.rs",
    ),
    (
        "ImagePullBackOff",
        "crates/cog-reflection/src/mainline_rollout.rs",
    ),
];

/// 登记在案的「同形不同物」：同一批词，判的是另一件事。
///
/// 登记的判据不是「它在另一个 crate」——那是理由的反面。判据是它回答的**问题**
/// 不同：词表在这里是「哪个版本坏了」，在那里是「哪个 Pod 卡在拉镜像、可以清掉」。
/// 同一个词在两个问题下的处置相反（判死回滚 / 删掉重来），所以它们不能合成一张表；
/// 但它们仍然是同一批 k8s 词，所以它们也不能被忘掉。
const REGISTERED_SECOND_SITES: &[(&str, &str)] = &[(
    "crates/bootstrap/src/main.rs",
    "判的是「卡在拉镜像、可以清掉」而不是「这个版本坏了」；引导器不依赖 cog-reflection",
)];

/// 一个文件的生产行里提到这些词的次数，按词记。
fn mentions_in(text: &str) -> BTreeMap<&'static str, usize> {
    let mut found: BTreeMap<&'static str, usize> =
        VOCABULARY.iter().map(|(word, _)| (*word, 0)).collect();
    for line in production_lines(text) {
        for (word, count) in found.iter_mut() {
            *count += line.matches(word).count();
        }
    }
    found
}

/// 每个词在生产代码里的全部落点。
fn census(root: &Path) -> BTreeMap<&'static str, Vec<String>> {
    let files = production_sources(root);
    let mut sites: BTreeMap<&'static str, Vec<String>> = VOCABULARY
        .iter()
        .map(|(word, _)| (*word, Vec::new()))
        .collect();
    for path in &files {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, path);
        for (word, count) in mentions_in(&text) {
            for _ in 0..count {
                sites
                    .get_mut(word)
                    .expect("a registered word")
                    .push(file.clone());
            }
        }
    }
    sites
}

#[test]
fn a_waiting_reason_is_declared_once_and_in_the_set_it_belongs_to() {
    let sites = census(&repo_root());

    let allowed: Vec<&str> = REGISTERED_SECOND_SITES.iter().map(|(f, _)| *f).collect();
    let mut findings = Vec::new();
    for (word, declared_in) in VOCABULARY {
        let hits = &sites[*word];
        let declared = hits.iter().filter(|f| *f == declared_in).count();
        let strays: Vec<&String> = hits
            .iter()
            .filter(|f| *f != declared_in && !allowed.contains(&f.as_str()))
            .collect();
        if declared == 1 && strays.is_empty() {
            continue;
        }
        findings.push(format!(
            "  {word}：{declared_in} 里 {declared} 处，另有 {} 处没有登记\n{}",
            strays.len(),
            strays
                .iter()
                .map(|f| format!("      {f}"))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }

    assert!(
        findings.is_empty(),
        "这些等待态词被抄到了第二处。两份清单都能跑，所以没有任何东西会失败：\
         它们只是给出相反的两个判断（一个说再等等，一个说判死并回滚），而谁对\
         取决于当初读没读过那次 registry 重启的实测——抄写不携带这件事。\n{}",
        findings.join("\n")
    );
}

#[test]
fn the_census_counts_a_second_spelling_and_reads_nothing_out_of_prose() {
    // 第二处就是第二处，哪怕写在另一个词表里。
    let two = concat!(
        "const A: &[&str] = &[\"ErrImagePull\"];\n",
        "const B: &[&str] = &[\"ErrImagePull\", \"ImagePullBackOff\"];\n",
    );
    let found = mentions_in(two);
    assert_eq!(found["ErrImagePull"], 2);
    assert_eq!(found["ImagePullBackOff"], 1);
    assert_eq!(found["CrashLoopBackOff"], 0);

    // 注释里提到这个词不是第二处声明。
    let prose = "// 这里以前有一份写着 ErrImagePull 的清单，已经收进声明处\nlet a = 1;\n";
    assert_eq!(mentions_in(prose)["ErrImagePull"], 0);
}
