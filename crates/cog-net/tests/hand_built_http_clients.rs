//! 生产代码里手工构造的 `reqwest` 客户端，按文件计数——一张只许变小的读数。
//!
//! 为什么需要这条门：本 crate 的约定是所有出站 HTTP 客户端都由
//! [`cog_net::build_client`] 构造，超时、连接池、代理只有一处定义。约定写在文档里
//! 而没人读文档，于是同一个仓库里长出了第二、第三份设置：有的 `Client::new()`
//! 连超时都没有（上游不应答就永远挂着），有的自钉一个数（改一处不影响另一处）。
//! 这些位点不是一起出现的，是一个一个出现的，而每次出现都没人红——这条门就是那个
//! 「红」。
//!
//! 表里的每一行都是**读数，不是许可**。行在表上只说明「这个文件里还有几个没人选过
//! 设置的客户端」，不说明它可以留着；把这行弄小或弄没，才是本读数在数的进展。
//! 反过来说，**计数只许减不许增**：新增一个手工客户端（新文件、或旧文件里多一个）
//! 都会让这条门红，而它的失败信息直接给出「多了/少了哪一个」。
//!
//! 什么不算：`#[cfg(test)]` 里的构造（测试自己搭一个假服务端，与生产设置无关）与本
//! 文件顶部这种注释行。为此这里有一处刻意的形式判据：`#[cfg(test)]` 标注的**那一项**
//! 整块跳过（按花括号配平），而不是「从这一行跳过文件末尾」——后者会把文件后半段的
//! 生产代码一起盲掉，那种盲法读起来与「这里没有位点」一模一样。
//!
//! 计数口径（复现方式就是跑本测试，或跑同一段遍历）：`crates/*/src/**/*.rs` 里
//! `reqwest::Client::new()` 与 `reqwest::Client::builder()` 的出现次数，去掉
//! `#[cfg(test)]` 项与注释行。
//!
//! 那份人口与「哪一半是生产代码」两条规则不在本文件里写第二遍：它们来自
//! `crates/cogneva/tests/common/rust_production.rs`，本文件按路径把它编进来。
//! 抄一份的代价不是多几行——是这份抄件与正本分叉的那天，本门读到的位点数会变，
//! 而「读到的少了」与「本来就没有」在读数上同形。

use std::collections::BTreeMap;
use std::fs;

#[path = "../../cogneva/tests/common/rust_production.rs"]
mod rust_production;

use rust_production::{production_sources, production_text, relative_name};

/// 豁免：不进表，因为在这里手工构造正是对的。每一条都要给出理由，理由不成立的
/// 时候这一行就该消失。只有一条，因为「正当性」在这里是例外而不是常态：连安装器
/// 那种「配置还不存在」的场景也照样计入读数——它是不是该并回同一份，是一个还没人
/// 判过的问题，而「还没判过」不该长得像「已经批准」。
const EXEMPT: &[(&str, &str)] = &[(
    "crates/cog-net/src/factory.rs",
    "工厂自己：这里的 builder 就是那份唯一设置的产地，不是绕过它的第二处",
)];

/// 读数：文件 → 手工构造的客户端个数。口径与文件头一致。
///
/// 这几条为什么还在（每一条都是待办，不是结论）：
/// - `cogneva/src/backup.rs`：Qdrant 快照的创建与搬运。共享的那份超时是给短请求的
///   （默认 30s），而快照创建要阻塞到落盘、下载是批量传输，套上它会把大集合的备份
///   切死。要有来源地给它一条**按请求**的超时，才谈得上接进工厂。
/// - 其余各条多为按用途各带一套设置（流式响应、探测的短超时）；它们是不是该并回
///   同一份，需要逐条判，未经判定的不计入「已知可并」。
const COUNTED: &[(&str, usize)] = &[
    ("crates/bootstrap/src/apt.rs", 1),
    ("crates/bootstrap/src/download.rs", 1),
    ("crates/bootstrap/src/main.rs", 1),
    ("crates/cog-extension/src/hostdocs_assist.rs", 1),
    ("crates/cog-extension/src/runtime/remote.rs", 1),
    ("crates/cog-gateway/src/git_identity.rs", 1),
    ("crates/cog-gateway/src/git_routing.rs", 1),
    ("crates/cog-gateway/src/llm_admin.rs", 5),
    ("crates/cog-gateway/src/security_gateway.rs", 3),
    ("crates/cog-observability/src/probes.rs", 1),
    ("crates/cogneva/src/backup.rs", 1),
];

/// 仓库根：一份算法，全仓共用（本文件按路径把那一份编进来）。
#[path = "../../cogneva/tests/common/root.rs"]
mod root;

use root::repo_root;

fn counted_sites() -> BTreeMap<String, usize> {
    let root = repo_root();

    let mut counts = BTreeMap::new();
    for path in production_sources(&root) {
        let rel = relative_name(&root, &path);
        if EXEMPT.iter().any(|(p, _)| rel == *p || rel.starts_with(p)) {
            continue;
        }
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        // 生产那一半：注释行与 `#[cfg(test)]` 项都不在里面，两种排除都在共享的
        // 那一份里，本文件不重新判一次。
        let mut n = 0;
        for line in production_text(&text).lines() {
            n += line.matches("reqwest::Client::new()").count();
            n += line.matches("reqwest::Client::builder()").count();
        }
        if n > 0 {
            counts.insert(rel, n);
        }
    }
    counts
}

#[test]
fn hand_built_http_clients_do_not_grow() {
    let got = counted_sites();

    // 空遍历读起来与「没有位点」一模一样，所以遍历本身要有一条判据：真实树里
    // 一定找得到文件，且表里那个已知有位的文件必须在其中。少一条，下面那次比较
    // 就可能在「什么都没扫到」上变绿。
    assert!(
        got.contains_key("crates/cogneva/src/backup.rs"),
        "遍历没扫到已知有位点的文件，这条门的读数不可信：{got:?}"
    );

    let expected: BTreeMap<String, usize> =
        COUNTED.iter().map(|(f, n)| (f.to_string(), *n)).collect();

    let added: Vec<(String, usize)> = got
        .iter()
        .filter(|(f, n)| expected.get(*f) != Some(n))
        .map(|(f, n)| (f.clone(), *n))
        .collect();
    let gone: Vec<(String, usize)> = expected
        .iter()
        .filter(|(f, n)| got.get(*f) != Some(n))
        .filter(|(f, _)| !added.iter().any(|(a, _)| a == *f))
        .map(|(f, n)| (f.clone(), *n))
        .collect();

    assert!(
        added.is_empty() && gone.is_empty(),
        "手工构造的 HTTP 客户端对不上读数。\n\
         多了或变多（应为 0，改成走 cog_net::build_client 或 cog_net::ReqwestHttpClient::from_config；\
         确实必须自己建时，把理由写进本文件的文档再改这张表）：{added:?}\n\
         少了或变少（这是进展：把这一行从 COUNTED 里缩小或删掉，读数才会跟上）：{gone:?}\n\
         当前实际：{got:?}"
    );
}
