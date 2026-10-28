//! 单测共用的样例：一片写满可压缩字节的文件，与 chart 里那条按名字取的规则。
//!
//! 两件都不是任何一处的题目，是每处单测都要先摆好的东西。原先 `claim_store`、
//! `image_store` 各写了一遍同一个 `write`，`claim_store`、`host_ledger`、`image_store`
//! 各写了一遍同一个 `chart_rule`——chart 换了字段（比如把 `greater_than` 挪走）时，
//! 改到哪一份、漏了哪一份，读起来全靠记性，而三份都还是绿的。

use std::path::Path;

/// 在 `path` 写 `bytes` 字节的可压缩内容，父目录一并建出来。
///
/// 内容是清一色的 `x`：占地方的扫描器判的是**字节数**，写的是什么不影响它判的东西。
pub(crate) fn write(path: &Path, bytes: usize) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, vec![b'x'; bytes]).unwrap();
}

/// chart 里那条按名字取的告警规则。
///
/// 读的是**渲染用的那份 chart 配置**，不是某份抄件：一个读数只有被一条按它名字查数
/// 的规则读到才会变成告警，两边任何一侧改名都让这条信号静默消失，而各自内部还是一致
/// 的。所以规则从 chart 里取、名字从被测模块里取，两边在这条判据上对质。
pub(crate) fn chart_rule(name: &str) -> serde_json::Value {
    let chart = crate::checkout::repo_root().join("deploy/helm/cogneva/files/cogneva.json");
    let text = std::fs::read_to_string(&chart)
        .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
    let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
    root.pointer(cog_core::config_sections::ALERT_RULES_POINTER)
        .and_then(|v| v.as_array())
        .expect("infra_watch.rules present")
        .iter()
        .find(|r| r["name"] == name)
        .unwrap_or_else(|| panic!("rule {name} missing"))
        .clone()
}
