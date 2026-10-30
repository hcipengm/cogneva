//! principle: feedback_no_hardcoding_config_surface
//!
//! 三张归档表各有一个「保留多少天」的旋钮，三个旋钮的读法一字不差，只有**读到 0**
//! 那一条分支不同：任务事件流把 0 读成「停掉清扫」，交换归档与用量账把 0 读成「写坏
//! 了，按默认三十天清扫」。两种处置都有书面理由，也都对，但它们是**相反的**。
//!
//! 抄写不携带这个区别。有人把某处 `Some(0) => None` 一并删掉、或者把三份解析收成一份
//! 时随手挑了一边，编译不失败、测试不失败（每份自己的测试跟着改或不跟都没人比），
//! 那个旋钮就从「停掉清扫」变成「清空这张表」，且没有任何读数会说这件事发生过——
//! 一张被清空的表与一张保留策略正常工作的表，在面板上同形。
//!
//! 所以读的规矩只有一份（`cog_core::retention`），0 的含义变成调用点上的一个词：
//! [`keep_days_or_default`] 还是 [`keep_days_or_disable`]。本门盯两件事：
//!
//! 1. 这两个词只许出现在声明处与登记在案的调用点上；
//! 2. **每个调用点用的是哪一个词，必须与登记表上的那一行一致**——只登记「这个文件有
//!    一个保留天数旋钮」不够，换掉它用的是哪一边不会有人拦。
//!
//! 判不了什么：
//!
//! - 哪一边是对的。两种处置今天都在树上，统一它们要改的是线上数据保留行为，那是
//!   属主的判断；本门只保证这个分歧是被写明的选择，不是抄写的残余。
//! - 日数额之外的旋钮（清扫周期、单趟删多少行）。它们没有「0 算什么意思」这个问题。

use crate::common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::routes::repo_root;
use common::rust_production::{production_lines, production_sources, relative_name};

/// 这条读法唯一允许被写下来的地方。
const DECLARED_IN: &str = "crates/cog-core/src/retention.rs";

const FALL_BACK: &str = "keep_days_or_default";
const DISABLE: &str = "keep_days_or_disable";

/// 登记在案的保留天数旋钮：文件、读到 0 时的处置、为什么是这一边。
///
/// 一个文件要是有两个只增不减的旋钮，就登记两行。
const KNOBS: &[(&str, &str, &str)] = &[
    (
        "crates/cog-storage/src/task_event_retention.rs",
        DISABLE,
        "读者从偏移零回放整条流，退休掉前缀就答成一个看起来像「这次运行刚开始」的缺口，\
         所以 0 必须能明说「不扫」，而不是被读成一个极小的天数",
    ),
    (
        "crates/cog-observability/src/exchange_retention.rs",
        FALL_BACK,
        "零天保留每一趟都把归档清空，而一张被清空的表与一张保留策略正常工作的表在读数上\
         同形——「保留期写成了零」没有任何正当解释",
    ),
    (
        "crates/cog-observability/src/usage_retention.rs",
        FALL_BACK,
        "零天清扫把用量账清空，读数于是从「这个部署停止了记账」变成「这个部署什么也没花」，\
         而两者在面板上不可区分",
    ),
];

/// 这个文件里用到了哪几种读法。
fn policies_in(text: &str) -> BTreeSet<&'static str> {
    let mut found = BTreeSet::new();
    for line in production_lines(text) {
        if line.contains(FALL_BACK) {
            found.insert(FALL_BACK);
        }
        if line.contains(DISABLE) {
            found.insert(DISABLE);
        }
    }
    found
}

/// 每个用到这条读法的文件，以及它用的是哪几种。
fn census(root: &Path) -> BTreeMap<String, BTreeSet<&'static str>> {
    let files = production_sources(root);
    let mut sites = BTreeMap::new();
    for path in &files {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let found = policies_in(&text);
        if !found.is_empty() {
            let file = relative_name(root, path);
            sites.insert(file, found);
        }
    }
    sites
}

#[test]
fn every_retention_knob_says_what_zero_means_and_says_it_the_same_way_in_the_table() {
    let sites = census(&repo_root());

    // 声明处必须真的还声明着两边：只剩一边，登记表里另一半就成了空话。
    let declared = sites.get(DECLARED_IN).unwrap_or_else(|| {
        panic!("{DECLARED_IN} 里两个读法一个都没有了：它们被搬走或改名，本门没有题目")
    });
    assert_eq!(
        declared,
        &BTreeSet::from([FALL_BACK, DISABLE]),
        "{DECLARED_IN} 少了一种读法：两种处置都是契约的一部分"
    );

    let mut registered: BTreeMap<&str, BTreeSet<&'static str>> = BTreeMap::new();
    for (file, policy, _) in KNOBS {
        registered.entry(file).or_default().insert(*policy);
    }

    let mut findings = Vec::new();
    for (file, found) in &sites {
        if file == DECLARED_IN {
            continue;
        }
        match registered.get(file.as_str()) {
            None => findings.push(format!(
                "  {file}: 用了 {found:?} 却没有登记——这个旋钮读到 0 算什么，没人知道"
            )),
            Some(want) if want != found => findings.push(format!(
                "  {file}: 用的读法与登记不符（登记 {want:?}，实际 {found:?}）——\
                 换掉的是「0 停掉清扫」还是「0 落回默认」，而这两件事一个是清空一张表、\
                 一个是保住它"
            )),
            Some(_) => {}
        }
    }

    // 登记表也要保鲜：被登记的文件若不再调用它，那一行就开始描述一个不存在的东西。
    for file in registered.keys() {
        if !sites.contains_key(*file) {
            findings.push(format!(
                "  {file}: 登记在案，但这个文件已经不用这条读法了——登记行该跟着改"
            ));
        }
    }

    assert!(
        findings.is_empty(),
        "保留天数旋钮的读法与登记表对不上。读法只有一份 \
         （`cog_core::retention::{FALL_BACK}` / `{DISABLE}`），0 的含义由调用点选：\n{}",
        findings.join("\n")
    );
}

#[test]
fn the_census_reads_a_call_site_through_comments_and_counts_both_readings() {
    // 调用可以写全路径，也可以先 `use` 进来：两种都算这个文件用了它。
    assert_eq!(
        policies_in("let days = cog_core::retention::keep_days_or_disable(r, 30, 1);\n"),
        BTreeSet::from([DISABLE])
    );
    assert_eq!(
        policies_in("use cog_core::retention::keep_days_or_default;\n"),
        BTreeSet::from([FALL_BACK])
    );
    // 一个文件里两边都用，本门要看得见两种。
    assert_eq!(
        policies_in(
            "let a = keep_days_or_default(r, 30, 1);\nlet b = keep_days_or_disable(r, 30, 1);\n"
        ),
        BTreeSet::from([FALL_BACK, DISABLE])
    );
    // 文档与注释里提到的名字不是调用点：解释这个旋钮的文件不该被当成第二个写处。
    assert_eq!(
        policies_in(
            "// keep_days_or_disable 以前在这里\n/// 见 keep_days_or_default\nlet a = 1;\n"
        ),
        BTreeSet::new()
    );
    // 别的项目里那种「保留天数」解析没有被认成这两边。
    assert_eq!(
        policies_in("let days = raw.trim().parse::<i64>().unwrap_or(30);\n"),
        BTreeSet::new()
    );
}
