//! 一个部署面的数，从 chart 送进进程要经过三跳：values 叶子 → ConfigMap 里的环境
//! 变量 → 配置文档 `env` 表给出的点分路径。这三跳每一跳都有门，但没有一条门问过
//! **同一件事是不是被写了两遍**。
//!
//! 坏法是这样的：`values.yaml` 里写着 300，交付的 `cogneva.json` 里同一个字段写着
//! 60，进程实际跑 300（env 层覆盖文档），而文档里那个 60 看上去像这个部署的决定。
//! 改文档的人以为改了节奏，没有任何东西跟着变；看文档的人以为部署每 60 秒看一次
//! 新活，而它每 300 秒才看一次。两个数都在树里，都有出处，谁也没错——错的是同一件
//! 事被声明了两遍，而没有东西比较它们。
//!
//! 判据从部署面自己取，不另抄一份名单：
//!
//! 1. 走一遍 chart 的模板，取每一行 `NAME: {{ .Values.… }}`——这是 chart 送进进程
//!    的环境变量，以及它取自哪个 values 叶子。只认恰好引用一个叶子的行：求和
//!    （`gateway.replicas + evolution.replicas + …`）没有单一出处，不在这条判据里。
//! 2. 环境变量 → 配置路径的映射取配置文档自己的 `env` 表。那张表的另一份（代码里
//!    的表）由 `config_loader` 的单测对账，这里不重造。
//! 3. 于是拿到「这一跳通到的配置路径」。文档正文在那个路径上有值 = 同一个字段在
//!    文档里也声明了一遍。
//!
//! 两半判词，问的是同一件事的两面：
//!
//! - **删掉它，行为不变吗**：把文档里那个叶子去掉，按应用自己的配置类型解析一遍
//!   再序列化回来——值仍然是同一个。这说明这个叶子什么都没决定：不带 env 跑时，
//!   代码默认值给的就是它；带 env 跑时，chart 的值覆盖它。两种跑法都不经过它。
//!   判据用的是解析往返，不读结构体默认值：`#[serde(default = "…")]` 与
//!   `Default::default()` 可以是两个数，猜错哪一个都会把「这个叶子有用」说成
//!   「没用」。
//! - **文档说的和 chart 说的一样吗**：叶子删掉后默认值不等于文档值时，这个叶子
//!   要就与 chart 的值一致（它是给不带 env 的跑法留的兜底），要就谁的值都不是。
//!   第三种状态——文档写着一个既非默认、又非 chart 的值——是上面那个坏法本身：
//!   部署实际不会走到它，它只负责让人读错。
//!
//! 不判什么，说清楚：模板里写死的数字（不来自 `.Values`）归
//! `chart_template_literals_contract_test.rs`；`values.yaml` 的数值叶子各带一条
//! `# tunable:` 判词，归 `tunable_registry_contract_test.rs`；`values` 叶子与代码
//! 常量的同值关系（例如端口）是第三种载体，本轮不判——端口在文档里与 chart 同值、
//! 与代码默认值不同值，它确实是不带 env 跑法的兜底，两半判词都放它过。
//!
//! 文档正文还有一份副本：`deploy/k3s/cogneva-json-configmap.yaml` 里内嵌的那段
//! JSON。它与 chart 那份的逐字段相等由 `config_loader` 的单测
//! `the_two_shipped_config_documents_are_one_document` 判，所以这里的修法必须
//! 两份一起改，只改一份会在那条门上红。

use crate::common;

use common::routes::{read, read_repo, repo_root};

use std::collections::BTreeMap;

use cogneva::config_loader::AppConfig;

/// chart 的 values：部署能拨的那一层。
const VALUES: &str = "deploy/helm/cogneva/values.yaml";

/// chart 模板所在目录：values 叶子变成环境变量的地方。
const TEMPLATES: &str = "deploy/helm/cogneva/templates";

/// 交付的配置文档：env 表的落点，也是本判据判的那一层。
const CHART_DOC: &str = "deploy/helm/cogneva/files/cogneva.json";

/// 三跳走通的一对：chart 从哪个叶子送出一个环境变量，它落到文档的哪个路径。
#[derive(Debug, Clone)]
struct Handover {
    /// 环境变量的名字，报错时点它。
    env: String,
    /// chart 引用的 values 叶子（点分，不含 `.Values.`）。
    values_path: String,
    /// 文档 `env` 表里这个名字映射到的配置路径。
    config_path: String,
    /// 送出它的模板位置，报错时点它。
    site: String,
}

/// 取一行里的 values 引用：`.Values.a.b` 之后连续的 `[A-Za-z0-9_]` 与点。
fn values_refs(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(at) = rest.find(".Values.") {
        let tail = &rest[at + ".Values.".len()..];
        let end = tail
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
            .unwrap_or(tail.len());
        let path = tail[..end].trim_end_matches('.');
        if !path.is_empty() {
            out.push(path.to_string());
        }
        rest = &tail[end..];
    }
    out
}

/// 一行是不是 `NAME: …` 形状的环境变量赋值。
fn env_assignment(line: &str) -> Option<(&str, &str)> {
    let trimmed = line.trim_start();
    let (key, value) = trimmed.split_once(':')?;
    let is_env = !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    is_env.then_some((key, value))
}

/// 三跳的前两跳：chart 从哪些 values 叶子送出了哪些环境变量。
///
/// 只收恰好引用一个叶子的行。一个环境变量由几个叶子求和得出（`PG_SHARING_PROCESSES`
/// 就是），它没有单一出处，不属于「同一件事写了几遍」这条判据。
fn chart_fed_envs() -> BTreeMap<String, (String, String)> {
    let dir = repo_root().join(TEMPLATES);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.expect("a template directory entry").path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("yaml"))
        .collect();
    files.sort();
    let mut out = BTreeMap::new();
    for file in &files {
        let text = read(file);
        for (n, line) in text.lines().enumerate() {
            let Some((name, value)) = env_assignment(line) else {
                continue;
            };
            let refs = values_refs(value);
            if refs.len() != 1 {
                continue;
            }
            out.entry(name.to_string())
                .or_insert_with(|| (refs[0].clone(), format!("{}:{}", file.display(), n + 1)));
        }
    }
    out
}

/// 文档自己的 `env` 表：环境变量名 → 点分配置路径。
fn doc_env_map(doc: &serde_json::Value) -> BTreeMap<String, String> {
    doc["env"]
        .as_object()
        .unwrap_or_else(|| panic!("{CHART_DOC} 没有 `env` 表：三跳的第二跳没有名字映射"))
        .iter()
        .filter(|(name, _)| name.starts_with("COGNEVA_"))
        .map(|(name, target)| {
            (
                name.clone(),
                target
                    .as_str()
                    .unwrap_or_else(|| panic!("{name} 映射到非字符串目标：{target}"))
                    .to_string(),
            )
        })
        .collect()
}

/// 三跳本身：值从 chart 的哪个叶子出发，落到文档的哪条路径。
fn handovers() -> (Vec<Handover>, serde_json::Value) {
    let doc: serde_json::Value = serde_json::from_str(&read_repo(CHART_DOC))
        .unwrap_or_else(|e| panic!("{CHART_DOC} 不是合法 JSON：{e}"));
    let map = doc_env_map(&doc);
    let mut out = Vec::new();
    for (env, (values_path, site)) in chart_fed_envs() {
        let Some(config_path) = map.get(&env) else {
            continue;
        };
        out.push(Handover {
            env,
            values_path,
            config_path: config_path.clone(),
            site,
        });
    }
    (out, doc)
}

/// 点分路径上的值。
fn value_at<'a>(doc: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut cur = doc;
    for key in path.split('.') {
        cur = cur.as_object()?.get(key)?;
    }
    Some(cur)
}

/// 点分路径上的值，取走：返回原来的值，父对象里删掉这个键。
fn take_at(doc: &mut serde_json::Value, path: &str) -> Option<serde_json::Value> {
    let (parents, last) = path.rsplit_once('.')?;
    let mut cur = doc;
    for key in parents.split('.') {
        cur = cur.as_object_mut()?.get_mut(key)?;
    }
    cur.as_object_mut()?.remove(last)
}

/// 这个叶子删掉之后，进程拿到的还是同一个值吗。
///
/// 用解析往返而不是结构体默认值：`#[serde(default = "…")]` 与 `Default::default()`
/// 可以给出两个数，拿错一个就会把「这个叶子有用」判成「没用」。往返问的是应用
/// 自己：不读这个键、只看剩下的文档，它给这个字段的答案是什么。
fn what_the_default_states_without(doc: &serde_json::Value, path: &str) -> serde_json::Value {
    let mut without = doc.clone();
    take_at(&mut without, path)
        .unwrap_or_else(|| panic!("{path} 不在文档里，删不掉——路径与文档对不上"));
    let parsed: AppConfig = serde_json::from_value(without).unwrap_or_else(|e| {
        panic!("去掉 {path} 之后文档不再是一个配置：{e}——判据读不懂它要判的东西")
    });
    let round = serde_json::to_value(&parsed).expect("AppConfig 能序列化回 JSON");
    value_at(&round, path)
        .cloned()
        .unwrap_or_else(|| panic!("去掉 {path} 之后往返里没有这个字段——它不是配置字段"))
}

/// chart 在 values 里给这个叶子写的值。
fn chart_value(values: &serde_yaml::Value, path: &str) -> Option<serde_yaml::Value> {
    let mut cur = values;
    for key in path.split('.') {
        cur = cur
            .as_mapping()?
            .get(serde_yaml::Value::String(key.into()))?;
    }
    Some(cur.clone())
}

/// 文档里的值与 chart 里的值说的是不是同一个数/串/开关。
///
/// `None` 表示比不了：chart 那边不是标量，或者值落在模板表达式里，两种都不该
/// 被当成「一致」放过去，也不该被当成「不一致」报红。
fn agrees(doc_value: &serde_json::Value, chart: &serde_yaml::Value) -> Option<bool> {
    match (doc_value, chart) {
        (serde_json::Value::Null, serde_yaml::Value::Null) => Some(true),
        (serde_json::Value::Bool(a), serde_yaml::Value::Bool(b)) => Some(*a == *b),
        (serde_json::Value::Number(a), serde_yaml::Value::Number(b)) => {
            let a = a.as_f64()?;
            let b = b.as_f64()?;
            Some((a - b).abs() < f64::EPSILON)
        }
        (serde_json::Value::String(a), serde_yaml::Value::String(b)) => Some(a == b),
        // chart 里是字符串、文档里是数（`"300"` 与 300）：「同一个数」按数值比。
        (serde_json::Value::Number(a), serde_yaml::Value::String(b)) => {
            Some(b.trim() == a.to_string())
        }
        (serde_json::Value::String(a), serde_yaml::Value::Number(b)) => {
            Some(a.trim() == b.to_string())
        }
        _ => None,
    }
}

/// 走查本身要看得见东西：三跳读不到数时，下面两条判据会在空集上通过。
#[test]
fn the_join_reads_the_whole_handover() {
    let (pairs, doc) = handovers();
    let values: serde_yaml::Value = serde_yaml::from_str(&read_repo(VALUES))
        .unwrap_or_else(|e| panic!("{VALUES} 不是合法 YAML：{e}"));

    assert!(
        pairs.len() >= 8,
        "三跳只走通了 {} 对（下限 8）：模板读法或 env 表读法坏了，下面的判据会在空集上通过",
        pairs.len()
    );

    let declared = pairs
        .iter()
        .filter(|pair| value_at(&doc, &pair.config_path).is_some())
        .count();
    // 下限只能是 1，钉不出别的数：一对里哪些该在文档正文出现，取决于 chart 与代码
    // 默认值谁说了算——同一个字段如果两处都不该写，文档里就不该有它，而逼出这个结果
    // 正是判据的活。把下限钉在「修好之前有几个」就等于要求永远留一个副本。
    // 下限 1 只挡一种事故：三跳读法整个失效，一对都读不到，下面的判据在空集上报绿。
    assert!(
        declared >= 1,
        "{} 对里一对都没在文档正文里声明（下限 1）：三跳读法坏了，或文档换了形状——\\
         下面的判据会在空集上通过",
        pairs.len()
    );

    let mut missing = Vec::new();
    for pair in &pairs {
        if chart_value(&values, &pair.values_path).is_none() {
            missing.push(format!(
                "{}: chart 引用的 .Values.{} 在 {} 里不存在（引用一个不存在的键渲染成空串而不报错）",
                pair.site, pair.values_path, VALUES
            ));
        }
    }
    assert!(missing.is_empty(), "{}", missing.join("\n"));
}

/// 文档不许把一个代码默认值已经说过的值再说一遍：那个叶子什么都没决定。
///
/// 判词是「删掉它，进程拿到的还是同一个值」。真如此，它存在的唯一作用就是让人
/// 以为这个部署在这个字段上做了决定，而决定在别处（chart 或默认值）。
#[test]
fn the_document_does_not_restate_a_value_the_default_already_states() {
    let (pairs, doc) = handovers();
    let mut copies = Vec::new();
    let mut judged = 0;
    for pair in &pairs {
        let Some(shipped) = value_at(&doc, &pair.config_path) else {
            continue; // 文档没声明这个字段：env 或默认值负责，这正是要的形态
        };
        judged += 1;
        let default = what_the_default_states_without(&doc, &pair.config_path);
        if &default == shipped {
            copies.push(format!(
                "{} -> {}: 文档写着 {}，而去掉这个叶子后应用自己给的就是 {}（经 {} 从 .Values.{} 送进来）。\
                 删掉文档里的这个叶子：它没有决定任何事，只让人以为这个部署在它上面做过决定",
                pair.env, pair.config_path, shipped, default, pair.site, pair.values_path
            ));
        }
    }
    // 同一条理由：下限 1 只挡「三跳读坏了、一个字段都没判到」，不钉标本数——标本数
    // 就是文档里声明过的字段数，修好之后它本来就该少。
    assert!(
        judged >= 1,
        "一个声明过的字段都没判到：这条判据空了（三跳读法坏了，或文档不声明任何 chart 送进来的字段）"
    );
    assert!(
        copies.is_empty(),
        "同一件事在文档与代码默认值里各写了一遍：\n{}",
        copies.join("\n")
    );
}

/// 文档不许写一个既不是默认值、也不是 chart 值的数：部署永远走不到它，它只负责
/// 让人读错。
#[test]
fn the_document_does_not_state_a_value_neither_the_chart_nor_the_default_uses() {
    let (pairs, doc) = handovers();
    let values: serde_yaml::Value = serde_yaml::from_str(&read_repo(VALUES))
        .unwrap_or_else(|e| panic!("{VALUES} 不是合法 YAML：{e}"));
    let mut stray = Vec::new();
    let mut judged = 0;
    for pair in &pairs {
        let Some(shipped) = value_at(&doc, &pair.config_path) else {
            continue;
        };
        let default = what_the_default_states_without(&doc, &pair.config_path);
        if &default == shipped {
            continue; // 抄的是默认值，上一条判据点名，这里不重复报
        }
        let Some(chart) = chart_value(&values, &pair.values_path) else {
            continue;
        };
        let Some(same) = agrees(shipped, &chart) else {
            continue; // 不是标量、比不了：报出来只会是猜
        };
        judged += 1;
        if !same {
            stray.push(format!(
                "{} -> {}: 文档写 {}，chart 从 .Values.{} 送 {:?}，代码默认值 {}。\
                 三种值里部署只会用后两个——文档里这个数谁都走不到，读它的人却以为部署是这样跑的",
                pair.env, pair.config_path, shipped, pair.values_path, chart, default
            ));
        }
    }
    // 同一条理由：不钉「可比的字段有几个」。修好之后剩下的可比字段是端口那类兜底，
    // 它们本来就该在文档里；判据要判的是不能出现第三种值，不是标本够不够。
    assert!(
        judged >= 1,
        "一个可比的字段都没比到：这条判据空了（三跳或 values 读法坏了）"
    );
    assert!(
        stray.is_empty(),
        "文档与 chart 对同一个字段各说各的：\n{}",
        stray.join("\n")
    );
}
