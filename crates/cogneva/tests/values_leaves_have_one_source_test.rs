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
//! 第三个面：**chart 自己**也不许把一个代码默认值再说一遍。判据同上，方向换成
//! `values.yaml` 的叶子：这个叶子只由一条 env 转发消费、判词是 `fixed`（没有闭环
//! 读它）、而它写的值等于「文档里没有这个键时应用自己给的值」——这条 env 送进去的
//! 和不给就有的，是同一个数，它没有决定任何事。`derived` 的叶子不在其中：它的值
//! 是一个闭环的种子，种子本来就是部署可以自己定的那一格。
//!
//! 第四个面：**每一格都要有人读**。`values.yaml` 的叶子人口现读，读法从模板现读
//! ——绝对路径、变量别名（`$l.minCpu`）、`with`/`range` 作用域里的裸 `.字段`，以及
//! 整棵子树交出去（`toYaml`／`join`／`include`／`range`）。没有读者的一格，改它
//! 渲染出来的清单一个字都不变：那是一条没人执行的部署说明。
//!
//! 不判什么，说清楚：模板里写死的数字（不来自 `.Values`）归
//! `chart_template_literals_contract_test.rs`；`values.yaml` 的数值叶子各带一条
//! `# tunable:` 判词，归 `tunable_registry_contract_test.rs`；`values` 叶子与代码
//! 常量的同值关系（例如端口）是另一种载体——端口在文档里与 chart 同值、与代码默认值
//! 不同值，它确实是不带 env 跑法的兜底，三条判据都放它过。字符串叶子（工作树的
//! 根目录这类）不在这一块：本文件判的是数值，字符串路径的另一半归模板字面量那条门。
//!
//! 文档正文还有一份副本：`deploy/k3s/cogneva-json-configmap.yaml` 里内嵌的那段
//! JSON。它与 chart 那份的逐字段相等由 `config_loader` 的单测
//! `the_two_shipped_config_documents_are_one_document` 判，所以这里的修法必须
//! 两份一起改，只改一份会在那条门上红。

use crate::common;

use common::routes::{read, read_repo, repo_root};

use std::collections::{BTreeMap, BTreeSet};

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
    // 文档里本来就没有这个键，也是合法的一问：这时「去掉它之后」就是「照现在这样」。
    take_at(&mut without, path);
    let parsed: AppConfig = serde_json::from_value(without).unwrap_or_else(|e| {
        panic!("去掉 {path} 之后文档不再是一个配置：{e}——判据读不懂它要判的东西")
    });
    let round = serde_json::to_value(&parsed).expect("AppConfig 能序列化回 JSON");
    // 往返里没有这一格＝这一格的默认就是「没配」（`null`）：带
    // `skip_serializing_if = "Option::is_none"` 的字段就是这样。
    value_at(&round, path)
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

/// chart 的模板里引用这个 values 叶子的每一处：`(file:line, 是不是一条 env 转发)`。
///
/// 「转发」＝这一行是 `COGNEVA_*: …` 且只引用这一个叶子。别处的引用（容器的
/// `containerPort`、Service 的端口、拼进地址里的端口）说明这个叶子还要决定清单
/// 本身，删了它清单就变了——那种叶子不是「默认值的复述」。
fn template_uses(values_path: &str) -> Vec<(String, bool)> {
    let dir = repo_root().join(TEMPLATES);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.expect("a template directory entry").path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("yaml"))
        .collect();
    files.sort();
    let mut out = Vec::new();
    for file in &files {
        let text = read(file);
        for (n, line) in text.lines().enumerate() {
            let refs = values_refs(line);
            if !refs.iter().any(|r| r == values_path) {
                continue;
            }
            let forwards = env_assignment(line).is_some() && refs.len() == 1;
            out.push((format!("{}:{}", file.display(), n + 1), forwards));
        }
    }
    out
}

/// 一个叶子在 `values.yaml` 里带的判词：`derived`、`fixed`，或者没有块。
///
/// 判词块就是叶子上面那段连续的 `#` 注释里的一行 `# tunable: …`。这一份读法只
/// 回答「这个词是什么」——块本身写得够不够，归 `tunable_registry_contract_test`。
fn tunable_word(text: &str, values_path: &str) -> Option<String> {
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut word = None;
    let mut block = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            block.push(trimmed.trim_start_matches('#').trim().to_string());
            continue;
        }
        if trimmed.is_empty() || trimmed.starts_with("- ") || trimmed.starts_with("{{") {
            continue;
        }
        let indent = line.len() - trimmed.len();
        let (key, rest) = match trimmed.split_once(':') {
            Some(split) => split,
            None => continue,
        };
        if key.is_empty() || key.contains(' ') {
            continue;
        }
        while stack.last().is_some_and(|(depth, _)| *depth >= indent) {
            stack.pop();
        }
        stack.push((indent, key.to_string()));
        if !rest.trim().is_empty() {
            let path = stack
                .iter()
                .map(|(_, key)| key.as_str())
                .collect::<Vec<_>>()
                .join(".");
            if path == values_path {
                word = block
                    .iter()
                    .find_map(|line| line.strip_prefix("tunable:"))
                    .map(|line| line.trim().to_string());
            }
            block.clear();
        }
    }
    word
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

/// chart 里一个判成 `fixed` 的叶子，不许写一个代码默认值已经说过的值。
///
/// 与上一节同一条判据，方向换成 chart：这个叶子只由一条 env 转发消费（没有别处
/// 拿它去渲染清单），判词是 `fixed`（没有闭环读它），而它写的值与「文档里没有这个
/// 键时应用自己给的值」一样——那么这条 env 送给进程的和进程不给就有的，是同一个数。
/// 它没有决定任何事，只让人以为这个部署在这个字段上做过决定。
///
/// 为什么 `derived` 的叶子不在这条判据里：它的值是一个闭环的**种子**（首拍之前
/// 要有个值），种子本来就是部署可以自己定的那一格；代码默认值与它相同只说明种子
/// 取自同一个数，不说明这一格是复述。真正的复述是 `fixed`——那个词不承诺任何
/// 闭环，值又与默认值一样，于是它唯一的作用就是占一行。
///
/// 不判的：这个叶子还被模板别处引用（端口这类：删了清单就变了），它本来就在文档里
/// 声明过（那是上一条判据的题目），或者它不是数值叶子（字符串路径另有形状，不在
/// 这一块的账上）。
#[test]
fn the_chart_does_not_restate_a_fixed_value_the_default_already_states() {
    let (pairs, doc) = handovers();
    let text = read_repo(VALUES);
    let values: serde_yaml::Value =
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{VALUES} 不是合法 YAML：{e}"));
    let mut copies = Vec::new();
    let mut judged = 0;
    let mut seeds = 0;
    for pair in &pairs {
        if value_at(&doc, &pair.config_path).is_some() {
            continue; // 文档也声明了这个字段：归上一条判据
        }
        let Some(chart) = chart_value(&values, &pair.values_path) else {
            continue;
        };
        if !chart.is_number() {
            continue; // 这一块只判数值叶子
        }
        let word = tunable_word(&text, &pair.values_path);
        let default = what_the_default_states_without(&doc, &pair.config_path);
        let Some(same) = agrees(&default, &chart) else {
            continue; // 比不了：报出来只会是猜
        };
        judged += 1;
        if word.as_deref() == Some("derived") {
            seeds += 1; // 闭环的种子：部署可以自己定，见上面
            continue;
        }
        if !same {
            continue;
        }
        let elsewhere: Vec<String> = template_uses(&pair.values_path)
            .into_iter()
            .filter(|(_, forwards)| !*forwards)
            .map(|(site, _)| site)
            .collect();
        if !elsewhere.is_empty() {
            continue; // 这个叶子还要决定清单本身
        }
        copies.push(format!(
            "{} -> {}: .Values.{}（判词 {}）写 {:?}，而文档里没有这个键时应用自己给的就是 {}。\
             这条 env 送进去的和不给就有的，是同一个数——既没有闭环读它、又没决定任何事",
            pair.env,
            pair.config_path,
            pair.values_path,
            word.as_deref().unwrap_or("（没有判词块）"),
            chart,
            default
        ));
    }
    // 下限只挡「三跳或默认值往返整个失效」：判到 0 格时下面那条会在空集上报绿。
    // 不钉「该有几个 fixed 复述」——修好之后那就是 0，钉在「修好之前有几个」等于
    // 要求永远留一个。
    assert!(
        judged >= 1,
        "一个「叶子 -> env -> 默认值」的格都没判到：这条判据空了（三跳读法坏了）"
    );
    // 判词读法本身也要看得见东西：这些格子里必须有 `derived` 的种子，否则上面那个
    // 「不是 derived 就判」的分支会把每个叶子都当成 `fixed`——读法坏了与全树都是
    // 复述，在红字上一模一样。
    assert!(
        seeds >= 1,
        "判到的 {judged} 格里没有一个是 `derived` 的种子：判词块的读法坏了，\
         下面的判据会把种子也当成复述"
    );
    assert!(
        copies.is_empty(),
        "chart 里 `fixed` 的叶子把代码默认值又说了一遍：\n{}",
        copies.join("\n")
    );
}

/// 产品自己的文件系统布局：这些路径不是谁挑的数，是这套清单的结构。
const PRODUCT_LAYOUT_ROOTS: [&str; 2] = ["/opt/cogneva", "/var/lib/cogneva"];

/// values 里所有「产品布局路径」叶子：点分路径 → 值。
///
/// 只收严格落在 [`PRODUCT_LAYOUT_ROOTS`] 之下的字符串叶子：那一族是产品在自己
/// 卷里的位置，模板里写死一处、values 里声明一处，两边迟早各说各的（把工作树的根
/// 挪到别处，`workingDir` 与 `safe.directory` 还指着老路径）。地址、`/metrics`
/// 这类不是布局：它们是另一件事恰好长成同一个串，比了只会是猜。
fn layout_leaves(values: &serde_yaml::Value) -> BTreeMap<String, String> {
    fn walk(value: &serde_yaml::Value, path: &str, out: &mut BTreeMap<String, String>) {
        match value {
            serde_yaml::Value::Mapping(map) => {
                for (key, child) in map {
                    if let Some(key) = key.as_str() {
                        let next = if path.is_empty() {
                            key.to_string()
                        } else {
                            format!("{path}.{key}")
                        };
                        walk(child, &next, out);
                    }
                }
            }
            serde_yaml::Value::String(text) => {
                if PRODUCT_LAYOUT_ROOTS
                    .iter()
                    .any(|root| text.len() > root.len() && text.starts_with(&format!("{root}/")))
                {
                    out.insert(path.to_string(), text.clone());
                }
            }
            _ => {}
        }
    }
    let mut out = BTreeMap::new();
    walk(values, "", &mut out);
    out
}

/// chart 的模板不许把一个布局路径再写一遍：它已经在 `values.yaml` 里有一处声明。
///
/// 判据是同一个串：模板里某一行写着这个叶子的值，而那一行不是从 `.Values.<叶子>`
/// 取来的。两处今天相等，改一处另一处不动——这正是「叶子 → 模板字面量同义」那一格。
#[test]
fn the_chart_does_not_spell_a_layout_path_that_a_leaf_declares() {
    let values: serde_yaml::Value = serde_yaml::from_str(&read_repo(VALUES))
        .unwrap_or_else(|e| panic!("{VALUES} 不是合法 YAML：{e}"));
    let leaves = layout_leaves(&values);
    assert!(
        !leaves.is_empty(),
        "{VALUES} 里一个布局路径叶子都没读到：下面那条判据会在空集上通过"
    );

    let dir = repo_root().join(TEMPLATES);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.expect("a template directory entry").path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("yaml"))
        .collect();
    files.sort();

    let mut copies = Vec::new();
    for file in &files {
        let text = read(file);
        for (n, line) in text.lines().enumerate() {
            let code = line.split('#').next().unwrap_or("");
            if code.trim().is_empty() || code.trim_start().starts_with("{{- /*") {
                continue;
            }
            for (path, value) in &leaves {
                if !code.contains(value.as_str()) {
                    continue;
                }
                if code.contains(&format!(".Values.{path}")) {
                    continue;
                }
                copies.push(format!(
                    "{}:{} 把 {} 写成了字面量（values 里 .Values.{} 也声明着它）",
                    file.display(),
                    n + 1,
                    value,
                    path
                ));
            }
        }
    }
    assert!(
        copies.is_empty(),
        "同一个布局路径在模板与 values 里各声明了一次：\n{}",
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

/// `values.yaml` 的每个标量叶子：点分路径，列表按下标写（`a.b.0`）。
///
/// 树、块、叶子一视同仁——判的是「这一格有没有人读」，不是「它是不是数」。
fn scalar_leaves(value: &serde_yaml::Value, path: &mut Vec<String>, out: &mut Vec<String>) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (key, child) in map {
                if let Some(key) = key.as_str() {
                    path.push(key.to_string());
                    scalar_leaves(child, path, out);
                    path.pop();
                }
            }
        }
        serde_yaml::Value::Sequence(items) => {
            for (index, child) in items.iter().enumerate() {
                path.push(index.to_string());
                scalar_leaves(child, path, out);
                path.pop();
            }
        }
        serde_yaml::Value::Null if path.is_empty() => {}
        _ => {
            if !path.is_empty() {
                out.push(path.join("."));
            }
        }
    }
}

/// 模板把一个 values 路径读走的两种形态。
///
/// 一种是把路径写出来（`.Values.a.b`、`$l.c`、`with .Values.a` 之后的 `.b`），
/// 另一种更常见、也更容易漏：整棵子树交出去——`toYaml .Values.healthProbes.startup`
/// 读走了三个叶子，而它们的名字一个也不在那一行里。只认字面量会把这一类全判成
/// 没人读。
#[derive(Debug, Default)]
struct Reads {
    exact: BTreeSet<String>,
    subtrees: BTreeSet<String>,
}

impl Reads {
    fn covers(&self, leaf: &str) -> bool {
        self.exact.contains(leaf)
            || self.subtrees.iter().any(|prefix| {
                leaf.len() > prefix.len()
                    && leaf.starts_with(prefix.as_str())
                    && leaf.as_bytes()[prefix.len()] == b'.'
            })
    }
}

/// 一个模板文件里的所有动作 `{{ … }}`；模板注释 `{{/* … */}}` 不算动作。
fn actions(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < chars.len() {
        if chars[i] != '{' || chars[i + 1] != '{' {
            i += 1;
            continue;
        }
        // 模板注释：跳到 `*/}}`，里面的内容不是模板，注释里点名的叶子不算被读。
        if i + 3 < chars.len() && chars[i + 2] == '/' && chars[i + 3] == '*' {
            let mut j = i + 4;
            while j + 3 < chars.len()
                && !(chars[j] == '*' && chars[j + 1] == '/' && chars[j + 2] == '}' && chars[j + 3] == '}')
            {
                j += 1;
            }
            i = (j + 4).min(chars.len());
            continue;
        }
        let mut j = i + 2;
        while j + 1 < chars.len() && !(chars[j] == '}' && chars[j + 1] == '}') {
            j += 1;
        }
        out.push(chars[i + 2..j.min(chars.len())].iter().collect());
        i = j + 2;
    }
    out
}

/// 动作里写的 values 路径（`.Values.` 之后连着的标识符与点）。
fn absolute_refs(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(at) = rest.find(".Values.") {
        let tail = &rest[at + ".Values.".len()..];
        let end = tail
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
            .unwrap_or(tail.len());
        let path = tail[..end].trim_matches('.');
        if !path.is_empty() {
            out.push(path.to_string());
        }
        rest = &tail[end..];
    }
    out
}

/// 一个动作的正文里，当前作用域下的裸 `.字段`（`with`/`range` 把 `.` 换成了
/// 一棵子树，`{{ .replicas }}` 读的就是 `子树.replicas`）。
fn bare_refs(body: &str) -> Vec<String> {
    let chars: Vec<char> = body.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '.' {
            i += 1;
            continue;
        }
        let previous = if i == 0 { ' ' } else { chars[i - 1] };
        if previous.is_ascii_alphanumeric() || previous == '_' || previous == '.' || previous == '$' {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '_' || chars[j] == '.') {
            j += 1;
        }
        let ident: String = chars[i + 1..j].iter().collect();
        let ident = ident.trim_matches('.');
        if !ident.is_empty() && ident != "Values" {
            out.push(ident.to_string());
        }
        i = j;
    }
    out
}

/// 被整棵交出去的形态：`toYaml`／`join`／`include`／`range` 的实参。
fn materializes(body: &str) -> bool {
    ["toYaml", "join", "include", "range"].iter().any(|word| body.contains(word))
}

/// 一个 `with`/`range` 的实参解析成 values 路径：绝对路径、某个变量别名，
/// 或者是别的什么（`.`、函数调用），后两种什么都解析不出来。
fn operand_path(operand: &str, aliases: &BTreeMap<String, String>) -> Option<String> {
    let operand = operand.trim().trim_start_matches(|c| c == '(' || c == '-').trim();
    if operand.starts_with(".Values.") {
        return absolute_refs(operand).into_iter().next();
    }
    if let Some(rest) = operand.strip_prefix('$') {
        let (name, suffix) = match rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
            Some(at) => (&rest[..at], &rest[at..]),
            None => (rest, ""),
        };
        let base = aliases.get(&format!("${name}"))?;
        let suffix = suffix.trim_start_matches('.');
        return Some(if suffix.is_empty() {
            base.clone()
        } else {
            format!("{base}.{suffix}")
        });
    }
    None
}

/// 模板读到的东西：绝对引用、变量别名、`with`/`range` 作用域。
fn reads_of(text: &str) -> Reads {
    let mut reads = Reads::default();
    let mut aliases: BTreeMap<String, String> = BTreeMap::new();
    // 块栈：`with`/`range` 压作用域（实参不是 values 路径时压 `None`），
    // `if`/`define`/`block` 只占一个位置，好让 `end` 一一对上。
    let mut blocks: Vec<Option<Option<String>>> = Vec::new();
    let current = |blocks: &Vec<Option<Option<String>>>| -> Option<String> {
        for block in blocks.iter().rev() {
            match block {
                Some(Some(scope)) => return Some(scope.clone()),
                Some(None) => return None,
                None => continue,
            }
        }
        None
    };
    for action in actions(text) {
        let body = action.trim().trim_matches('-').trim();
        let head = body.trim_start_matches('-').trim();
        if head == "end" || head.starts_with("end ") {
            blocks.pop();
            continue;
        }
        if head.starts_with("with ") || head.starts_with("range ") {
            let rest = head.split_once(' ').map(|(_, rest)| rest).unwrap_or("");
            let operand = rest.split('|').next().unwrap_or("").trim();
            let scope = if operand.contains(":=") {
                // `range $k, $v := X`：`. ` 是元素，不是 X 本身。
                None
            } else {
                operand_path(operand, &aliases)
            };
            blocks.push(Some(scope));
        } else if head.starts_with("if ") || head.starts_with("define ") || head.starts_with("block ") {
            blocks.push(None);
        } else if head == "else" || head.starts_with("else ") {
            // 同一个块的另一支，不压栈。
        } else if let Some((lhs, rhs)) = body.split_once(":=") {
            let lhs = lhs.trim();
            if lhs.starts_with('$') {
                if let Some(path) = absolute_refs(rhs).into_iter().next() {
                    aliases.insert(lhs.to_string(), path);
                }
            }
        }
        let mut found = absolute_refs(body);
        // 变量别名：`$l.minCpu` → `resourceGovernance.limitRange.minCpu`。
        for (name, path) in &aliases {
            let mut rest = body;
            while let Some(at) = rest.find(name.as_str()) {
                let tail = &rest[at + name.len()..];
                let end = tail
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
                    .unwrap_or(tail.len());
                let suffix = tail[..end].trim_matches('.');
                if end > 0 && suffix.is_empty() {
                    found.push(path.clone());
                } else if !suffix.is_empty() {
                    found.push(format!("{path}.{suffix}"));
                }
                rest = &tail[end..];
            }
        }
        if let Some(scope) = current(&blocks) {
            for bare in bare_refs(body) {
                found.push(format!("{scope}.{bare}"));
            }
        }
        let whole = materializes(body);
        for path in found {
            reads.exact.insert(path.clone());
            if whole {
                reads.subtrees.insert(path);
            }
        }
    }
    reads
}

/// chart 的模板读过的一切 values 路径：所有 `.yaml` 与 `_helpers.tpl`。
///
/// `NOTES.txt` 不算：那是装完后给人看的说明，写一句「这个值现在是 30」不会让
/// 任何东西读到它。
fn consumed_values() -> Reads {
    let dir = repo_root().join(TEMPLATES);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.expect("a template directory entry").path())
        .filter(|path| {
            matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("yaml") | Some("tpl")
            )
        })
        .collect();
    files.sort();
    let mut reads = Reads::default();
    for file in &files {
        let one = reads_of(&read(file));
        reads.exact.extend(one.exact);
        reads.subtrees.extend(one.subtrees);
    }
    reads
}

/// `values.yaml` 的每一格都要有一个读者：一个没人读的叶子声明了一件谁都不为它
/// 改变的部署事实。
///
/// 判据只走 chart 与代码：叶子人口从 `values.yaml` 现读，读法从模板现读，没有
/// 名单。今天是空集，不代表它以后也是——一个过期的档位、一次改名留下的旧键，
/// 都会以「某个部署以为改它有作用」的样子留在 values 里，而渲染结果一个字都不变。
#[test]
fn every_leaf_of_the_values_is_read_by_a_template() {
    let values: serde_yaml::Value = serde_yaml::from_str(&read_repo(VALUES))
        .unwrap_or_else(|e| panic!("{VALUES} 不是合法 YAML：{e}"));
    let mut leaves = Vec::new();
    scalar_leaves(&values, &mut Vec::new(), &mut leaves);
    assert!(
        !leaves.is_empty(),
        "{VALUES} 里一个叶子都没读到：下面那条判据会在空集上通过"
    );

    let reads = consumed_values();
    assert!(
        !reads.exact.is_empty(),
        "模板里一个 values 路径都没读到：读法坏了，下面会把每个叶子都判成没人读"
    );

    let dead: Vec<&String> = leaves.iter().filter(|leaf| !reads.covers(leaf)).collect();
    assert!(
        dead.is_empty(),
        "这些 values 叶子没有任何模板读它：改它渲染出来的清单一个字都不变，\
         它只是一句没人执行的部署说明（删掉，或者让它真的决定点什么）：\n{dead:#?}"
    );
}
