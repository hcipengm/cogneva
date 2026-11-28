//! 生成侧最常产出的形状是「给一处代码补用例」，落进落地那一侧就是一条条
//! `crates/<成员>/tests/*.rs`。这一族曾经落在晋级门最后那一格：拒收，理由写
//! 「触及没有准入条件的路径族」——同一条台账行于是每次再生成一遍、每次被同一
//! 格拒掉，一整轮 apply / 编译 / 全量测试的钱花在这里，什么都没换回来。
//!
//! 这一族的准入条件本来就跑着：沙盒里 `cargo test` 已经在被改过的那棵树上跑过，
//! 跑红的变更到不了晋级门。所以这一格要判的不是「用例写得好不好」，而是两件可
//! 读的事——这一族有没有可执行条件，以及判中的是不是**就这一族**。本文件钉住
//! 这两件事，文件清单从目录读出来而不是照抄一张表，所以以后加进来的文件自动被
//! 覆盖，而把 `cog_reflection::promotion_gate` 里成员 `tests/` 那一格删掉，下面
//! 每一条都会变红。
//!
//! 断言里这个部署只声明了「没有前端构建」：成员 `src`/`tests` 的一族不靠那份
//! 声明活着——它的条件永远是编译加测试——所以把 web 关掉，下面过的就不是
//! WebUI 那一格的光。

use cog_reflection::config::PromotionGateConfig;
use cog_reflection::promotion_gate::{classify, GateVerdict, VerifiableFamilies};

use std::path::Path;

/// 少于这么多文件，说明目录没读到：下面每条断言都会在空集上通过。
const FLOOR: usize = 150;

/// 这个目录里的每个 `.rs`，写成变更里 `+++ b/` 那一行的样子。
fn the_files_in_this_directory() -> Vec<String> {
    let member = Path::new(env!("CARGO_MANIFEST_DIR"))
        .file_name()
        .expect("the crate directory is named")
        .to_string_lossy()
        .to_string();
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut files: Vec<String> = std::fs::read_dir(&tests)
        .expect("the tests directory is readable")
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .map(|path| {
            let name = path
                .file_name()
                .expect("a file is named")
                .to_string_lossy()
                .to_string();
            format!("crates/{member}/tests/{name}")
        })
        .collect();
    files.sort();
    files
}

fn verdict_on(files: &[String]) -> GateVerdict {
    classify(
        files,
        10,
        &PromotionGateConfig::default(),
        &VerifiableFamilies::default(),
    )
}

/// 写在生成侧脚下的文件进的是「会跑」的那一档，不是「被拒」的那一档。
#[test]
fn the_generator_has_a_file_and_the_gate_reads_it() {
    let files = the_files_in_this_directory();
    assert!(
        files.len() >= FLOOR,
        "the scan read {} files, fewer than the {FLOOR} this directory is known to hold; \
         every assertion below would then pass over the empty set",
        files.len()
    );

    let verdict = verdict_on(&files);
    assert_eq!(
        verdict,
        GateVerdict::AutoRollout,
        "a change touching {} is still refused by the family it lands in: {verdict:?}",
        files.first().expect("the scan is not empty")
    );
}

/// 判中的是这一族，不是「路径里恰好有 `tests`」：这两个长得像的目录照样拒收，
/// 所以上面那条不会在族边界挪走之后继续绿着。
#[test]
fn a_directory_that_only_looks_like_this_one_is_still_refused() {
    for path in [
        "crates/cogneva/tests2/suite.rs",
        "crates/cogneva/fixtures/suite.rs",
    ] {
        let verdict = verdict_on(&[path.to_string()]);
        assert!(
            matches!(verdict, GateVerdict::Reject { .. }),
            "{path} was judged as if it were this directory: {verdict:?}"
        );
    }
}

/// 没有条件的族是拒收，不是排队等人：等人批的那条路没有下一个动作，而这里
/// 缺的是配置里的一条声明——那句话得说出来。
#[test]
fn a_family_without_a_condition_is_refused_rather_than_queued_for_a_person() {
    match verdict_on(&["crates/cog-example/scripts/run.py".to_string()]) {
        GateVerdict::Reject { reason, .. } => assert!(
            reason.contains("没有准入条件"),
            "the refusal has to name what the family is missing, not ask for a reviewer: {reason}"
        ),
        other => panic!("expected Reject, got {other:?}"),
    }
}
