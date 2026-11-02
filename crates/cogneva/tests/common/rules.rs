//! 规则文档里那一条条 promql。
//!
//! 两个以上的门要从 `deploy/helm/cogneva/files/cogneva.json` 里取出每一条 promql，
//! 而它们的取法必须相同：一份取法不同的副本不是「多写了一遍」，是两边判的不是同一批
//! 表达式，红与不红各自成立却说的是两件事。这份只有一条路——按任意深度找 `promql`
//! 键，取它的字符串值。
#![allow(dead_code)]

/// 告警规则文档本身：`deploy/helm/cogneva/files/cogneva.json` 解析出来的那一份。
///
/// 读法（哪个文件、读不出来怎么办）只该有一个答案。两份读法各自照样能跑绿，但它们判
/// 的不是同一批字节：一份读错路径就 panic、另一份悄悄换成空的规则集，而那份「空的规则
/// 集」在每一条「规则里没有 X」的判据上都是绿的。
pub fn rule_document(root: &std::path::Path) -> serde_json::Value {
    let path: std::path::PathBuf = root.join("deploy/helm/cogneva/files/cogneva.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// 一条 promql 表达式，从规则文档里任意深度的 `promql` 键取出。
///
/// `promql` 键下不往下走：那一格已经是表达式，再往下只会在表达式自己的 JSON 结构里
/// 找同名键。其余每个子值都要走，规则文档的层级是它自己的事，不是这里能假定的。
pub fn promql_expressions(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                if key == "promql" {
                    if let Some(text) = child.as_str() {
                        out.push(text.to_string());
                    }
                } else {
                    promql_expressions(child, out);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                promql_expressions(item, out);
            }
        }
        _ => {}
    }
}
