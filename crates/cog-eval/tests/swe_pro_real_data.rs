//! 拿真实取数产物跑一遍 SWE-bench Pro 的数据适配器。
//!
//! 单元测试用的是夹具，跑得动但证明不了「取数脚本真写出来的那份读得进来」。这条测试
//! 读 [`DATA_ROOT_ENV`] 指的目录（取数脚本的 `--dest`），只在数据真在的时候跑；不在时
//! 打印一行 SKIP 而不是静默变绿——「没数据」与「读通了」是两件事，不能同形。
//!
//! 这里最要命的一格是 `fail_to_pass` / `pass_to_pass`：它们是 Python 字面量列表的字符串，
//! 用 JSON 解析器读会静默得到空列表，于是整列判成「没有测试要跑」。所以这条测试断言
//! **每一行都解析出一个非空的 fail_to_pass 清单**，把那个静默失败按死在真实数据上。

use cog_eval::adapters::swe_pro::parse_test_list;
use cog_eval::{CaseSource, SweProCaseSource, DATA_ROOT_ENV, SWE_PRO_JSONL};

#[test]
fn the_real_fetch_output_reads_and_is_shaped_like_swe_pro() {
    let Ok(root) = std::env::var(DATA_ROOT_ENV) else {
        eprintln!(
            "SKIP: {DATA_ROOT_ENV} is not set, the real SWE-bench Pro text form was not read"
        );
        return;
    };
    let root = std::path::PathBuf::from(root);
    if !root.join(SWE_PRO_JSONL).exists() {
        eprintln!(
            "SKIP: no {} under {root:?}; run the fetch script with --convert against this --dest",
            SWE_PRO_JSONL
        );
        return;
    }

    let cases = SweProCaseSource
        .cases(&root)
        .expect("the real SWE-bench Pro text form must read");
    // 行数是取数时就钉住的（parquet 的行数）。
    assert_eq!(cases.len(), 731, "SWE-bench Pro public split is 731 rows");

    // id 唯一。
    let mut ids: Vec<&str> = cases.iter().map(|c| c.id.as_str()).collect();
    ids.sort_unstable();
    let before = ids.len();
    ids.dedup();
    assert_eq!(before, ids.len(), "duplicate SWE-bench Pro ids");

    // 每一行都要有金标补丁与容器标签，并且判分清单每行都解析得出、非空。
    let mut total_f2p = 0usize;
    let mut empty_p2p = 0usize;
    for c in &cases {
        assert!(
            c.expected_output
                .as_ref()
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty()),
            "{} has no gold patch",
            c.id
        );
        assert!(
            c.metadata
                .get("dockerhub_tag")
                .is_some_and(|s| !s.is_empty()),
            "{} has no dockerhub_tag",
            c.id
        );
        let f2p = parse_test_list(
            c.metadata
                .get("fail_to_pass")
                .map(String::as_str)
                .unwrap_or(""),
        )
        .unwrap_or_else(|e| panic!("{} fail_to_pass: {e}", c.id));
        assert!(!f2p.is_empty(), "{} parsed to an empty fail_to_pass", c.id);
        total_f2p += f2p.len();
        let p2p = parse_test_list(
            c.metadata
                .get("pass_to_pass")
                .map(String::as_str)
                .unwrap_or(""),
        )
        .unwrap_or_else(|e| panic!("{} pass_to_pass: {e}", c.id));
        if p2p.is_empty() {
            empty_p2p += 1;
        }
    }
    assert_eq!(
        total_f2p, 10546,
        "fail_to_pass total across the public split"
    );
    // pass_to_pass 为空是合法的行，公开集里有几百条；不该是全部、也不该是零。
    assert!(
        empty_p2p > 0 && empty_p2p < cases.len(),
        "pass_to_pass empty rows: {empty_p2p}"
    );

    // 只出现这几种语言，且不止一种——子集过滤要按 tag 找得到东西。
    let langs: std::collections::BTreeSet<&str> = cases
        .iter()
        .filter_map(|c| c.metadata.get("repo_language").map(String::as_str))
        .collect();
    assert!(langs.len() > 1, "repo languages: {langs:?}");
}
