//! 拿真实取数产物跑一遍 HLE 的数据适配器。
//!
//! 单元测试用的是夹具，跑得动但证明不了「取数脚本真写出来的那份读得进来」。这条测试
//! 读 [`DATA_ROOT_ENV`] 指的目录（取数脚本的 `--dest`），只在数据真在的时候跑；不在时
//! 打印一行 SKIP 而不是静默变绿——「没数据」与「读通了」是两件事，不能同形。

use cog_eval::adapters::HleCaseSource;
use cog_eval::{CaseSource, DATA_ROOT_ENV};

#[test]
fn the_real_fetch_output_reads_and_is_shaped_like_hle() {
    let Ok(root) = std::env::var(DATA_ROOT_ENV) else {
        eprintln!("SKIP: {DATA_ROOT_ENV} is not set, the real HLE text form was not read");
        return;
    };
    let root = std::path::PathBuf::from(root);
    if !root.join(cog_eval::HLE_JSONL).exists() {
        eprintln!(
            "SKIP: no {} under {root:?}; run the fetch script with --convert against this --dest",
            cog_eval::HLE_JSONL
        );
        return;
    }

    let cases = HleCaseSource
        .cases(&root)
        .expect("the real HLE text form must read");
    // HLE 的题量是取数时就钉住的（parquet 的行数）。
    assert_eq!(cases.len(), 2500, "HLE is 2500 questions");

    let image = cases
        .iter()
        .filter(|c| c.metadata.get("has_image").map(String::as_str) == Some("true"))
        .count();
    assert!(
        image > 0 && image < cases.len(),
        "HLE is multimodal but not all-image: {image}"
    );

    // id 唯一：重合的 id 会让按 id 归因的失败面张冠李戴。
    let mut ids: Vec<&str> = cases.iter().map(|c| c.id.as_str()).collect();
    ids.sort_unstable();
    let before = ids.len();
    ids.dedup();
    assert_eq!(before, ids.len(), "duplicate HLE ids");

    // 两种 answer_type 都在真实数据里出现过——判分器的两条路都有题走到。
    let types: std::collections::BTreeSet<&str> = cases
        .iter()
        .filter_map(|c| c.metadata.get("answer_type").map(String::as_str))
        .collect();
    assert!(
        types.contains("exactMatch") && types.contains("multipleChoice"),
        "{types:?}"
    );
}
