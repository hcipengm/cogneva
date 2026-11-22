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
//! `values.yaml` 的叶子：这个叶子只由一条 env 转发消费、块里没有证明（没有闭环
//! 读它）、而它写的值等于「文档里没有这个键时应用自己给的值」——这条 env 送进去的
//! 和不给就有的，是同一个数，它没有决定任何事。块里点了证明的叶子不在其中：它的值
//! 是一个闭环的种子，种子本来就是部署可以自己定的那一格。
//!
//! 第四个面：**每一格都要有人读**。`values.yaml` 的叶子人口现读，读法从模板现读
//! ——绝对路径、变量别名（`$l.minCpu`）、`with`/`range` 作用域里的裸 `.字段`，以及
//! 整棵子树交出去（`toYaml`／`join`／`include`／`range`）。没有读者的一格，改它
//! 渲染出来的清单一个字都不变：那是一条没人执行的部署说明。
//!
//! 第五个面：**env 层不许复述交接文档**。裸词（`cogneva`、`main`、`local-fs`）
//! 按字面量相等判是不成立的——同一个词在别处可能是另一件事。判据换成文档自己的
//! `env` 表：变量名通向哪条路径，文档正文在那条路径上是不是写着同一个值。写着
//! 同一个值的，是同一件事的两处声明（文档不带 env 跑就是它，带 env 跑送进去的
//! 还是它）；值不同的是这份 Pod 与共享文档的差，那正是覆盖层的用处，不在这里判。
//!
//! 第六个面：**共享的 env 层不许让文档那一份变成没人跑的值**。第五面只判「值相同」
//! 那一半；对偶的坏法是值不同——跑起来的是 env 送进去的那个，文档在同一条路径上写
//! 的那一份没有任何进程会读到，可它读起来仍像这个部署的决定。两种 env 层要分开看：
//! `app-config.yaml` 是每份 Pod 的覆盖层，它盖掉文档正是它的用处（进化关 memory、
//! 关分层迁移，那是这份 Pod 与共享文档的差）；`configmap.yaml` 与 `deploy/k3s/` 的
//! 载体是共享层，它整份的身份就是「把文档投影进环境变量」，在那里写出一个不同的值，
//! 等于第二份决定。判据的人口就是共享层的字面量，与文档自己的 `env` 表对账。
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

/// 一个叶子在 `values.yaml` 里的块，逐行（`#` 去掉、去空白）。
///
/// 判词块就是叶子上面那段连续的注释，以一行 `# tunable:` 开头。这一份读法只把
/// 块交出来——块写得够不够、分类算得对不对，归 `tunable_registry_contract_test`。
fn tunable_block(text: &str, values_path: &str) -> Option<Vec<String>> {
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
            if path == values_path && block.iter().any(|line| line.starts_with("tunable:")) {
                word = Some(block.clone());
            }
            block.clear();
        }
    }
    word
}

/// 这个叶子有没有人在量它：块的证据是 `proof:` 那一行。
///
/// 分类不读词——`values.yaml` 里已经没有词了。一个叶子只要点了证明测试，它的值
/// 就是闭环的种子；没有点的，就是部署自己的一个决定。
fn tunable_is_measured(text: &str, values_path: &str) -> bool {
    tunable_block(text, values_path)
        .is_some_and(|block| block.iter().any(|line| line.starts_with("proof:")))
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

/// chart 里一个没人量着的叶子，不许写一个代码默认值已经说过的值。
///
/// 与上一节同一条判据，方向换成 chart：这个叶子只由一条 env 转发消费（没有别处
/// 拿它去渲染清单），块里没有证明（没有闭环读它），而它写的值与「文档里没有这个
/// 键时应用自己给的值」一样——那么这条 env 送给进程的和进程不给就有的，是同一个数。
/// 它没有决定任何事，只让人以为这个部署在这个字段上做过决定。
///
/// 为什么有人量着的叶子不在这条判据里：它的值是一个闭环的**种子**（首拍之前
/// 要有个值），种子本来就是部署可以自己定的那一格；代码默认值与它相同只说明种子
/// 取自同一个数，不说明这一格是复述。真正的复述是块里没有证明的叶子——它不承诺
/// 任何闭环，值又与默认值一样，于是它唯一的作用就是占一行。
///
/// 不判的：这个叶子还被模板别处引用（端口这类：删了清单就变了），它本来就在文档里
/// 声明过（那是上一条判据的题目），或者它不是数值叶子（字符串路径另有形状，不在
/// 这一块的账上）。
#[test]
fn the_chart_does_not_restate_a_value_the_default_already_states() {
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
        let measured = tunable_is_measured(&text, &pair.values_path);
        let default = what_the_default_states_without(&doc, &pair.config_path);
        let Some(same) = agrees(&default, &chart) else {
            continue; // 比不了：报出来只会是猜
        };
        judged += 1;
        if measured {
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
            "{} -> {}: .Values.{}（块里没有 `proof:`）写 {:?}，而文档里没有这个键时应用自己给的就是 {}。\
             这条 env 送进去的和不给就有的，是同一个数——既没有闭环读它、又没决定任何事",
            pair.env,
            pair.config_path,
            pair.values_path,
            chart,
            default
        ));
    }
    // 下限只挡「三跳或默认值往返整个失效」：判到 0 格时下面那条会在空集上报绿。
    // 不钉「该有几个复述」——修好之后那就是 0，钉在「修好之前有几个」等于
    // 要求永远留一个。
    assert!(
        judged >= 1,
        "一个「叶子 -> env -> 默认值」的格都没判到：这条判据空了（三跳读法坏了）"
    );
    // 读法本身也要看得见东西：这些格子里必须有带证明的种子，否则上面那个
    // 「没有证明就判」的分支会把每个叶子都当成复述——读法坏了与全树都是
    // 复述，在红字上一模一样。
    assert!(
        seeds >= 1,
        "判到的 {judged} 格里没有一个是带 `proof:` 的种子：块的读法坏了，\
         下面的判据会把种子也当成复述"
    );
    assert!(
        copies.is_empty(),
        "chart 里没人量着的叶子把代码默认值又说了一遍：\n{}",
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
            serde_yaml::Value::String(text)
                if PRODUCT_LAYOUT_ROOTS.iter().any(|root| {
                    text.len() > root.len() && text.starts_with(&format!("{root}/"))
                }) =>
            {
                out.insert(path.to_string(), text.clone());
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

/// 如果 `at` 处开始的是一个模板注释 `{{…/* … */…}}`，返回它之后的偏移。
///
/// `{{` 与 `/*` 之间可以有空白与破折号（`{{- /*`、`{{/*` 两种写法都在仓里），
/// `*/` 与 `}}` 之间同理。只认紧挨着的 `{{/*` 会把带破折号的那一大类漏掉，而那一类
/// 恰好就是本仓写得最多的写法。
fn template_comment_end(chars: &[char], at: usize) -> Option<usize> {
    let mut open = at + 2;
    while open < chars.len() && (chars[open] == '-' || chars[open].is_whitespace()) {
        open += 1;
    }
    if open + 1 >= chars.len() || chars[open] != '/' || chars[open + 1] != '*' {
        return None;
    }
    let mut close = open + 2;
    while close + 1 < chars.len() && !(chars[close] == '*' && chars[close + 1] == '/') {
        close += 1;
    }
    let mut after = (close + 2).min(chars.len());
    while after < chars.len() && (chars[after] == '-' || chars[after].is_whitespace()) {
        after += 1;
    }
    if after + 1 < chars.len() && chars[after] == '}' && chars[after + 1] == '}' {
        after += 2;
    }
    Some(after)
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
        // 写法有两种：`{{/* … */}}` 与带破折号的 `{{- /* … */ -}}`，后者在本仓
        // 模板里更常见——只认前一种，注释里的路径就会被当成读者。
        if let Some(after) = template_comment_end(&chars, i) {
            i = after;
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
        if previous.is_ascii_alphanumeric() || previous == '_' || previous == '.' || previous == '$'
        {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        while j < chars.len()
            && (chars[j].is_ascii_alphanumeric() || chars[j] == '_' || chars[j] == '.')
        {
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
    ["toYaml", "join", "include", "range"]
        .iter()
        .any(|word| body.contains(word))
}

/// 一个 `with`/`range` 的实参解析成 values 路径：绝对路径、某个变量别名，
/// 或者是别的什么（`.`、函数调用），后两种什么都解析不出来。
fn operand_path(operand: &str, aliases: &BTreeMap<String, String>) -> Option<String> {
    let operand = operand.trim().trim_start_matches(['(', '-']).trim();
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
        } else if head.starts_with("if ")
            || head.starts_with("define ")
            || head.starts_with("block ")
        {
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

/// 模板里「不成文的那部分」：去掉 Helm 注释与整行的 YAML 注释。
///
/// 注释里出现的字面量不是部署事实——`namespace="cogneva"` 那种写法在注释里解释了
/// 为什么必须渲染，它本身不该被当成本地声明了一次。
fn comment_free(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '{' && chars.get(i + 1) == Some(&'{') {
            if let Some(after) = template_comment_end(&chars, i) {
                i = after;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out.lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
}

/// 引用形的字符串叶子：值里带 `:` 或 `/` 的那些——地址、镜像、路径。
///
/// 为什么只比这一族：`main`、`info` 这种裸词在两个不相干的地方各写一遍是巧合，
/// 把它判成「同一件事写了两遍」只会逼人加一张豁免名单；而一个带冒号或斜杠的串
/// 是某个具体对象的名字，两处相等按构造就是同一个对象被声明了两次。
fn reference_leaves(
    value: &serde_yaml::Value,
    path: &mut Vec<String>,
    out: &mut Vec<(String, String)>,
) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (key, child) in map {
                if let Some(key) = key.as_str() {
                    path.push(key.to_string());
                    reference_leaves(child, path, out);
                    path.pop();
                }
            }
        }
        serde_yaml::Value::Sequence(items) => {
            for (index, child) in items.iter().enumerate() {
                path.push(index.to_string());
                reference_leaves(child, path, out);
                path.pop();
            }
        }
        serde_yaml::Value::String(text)
            if !text.is_empty() && text.contains([':', '/']) && !path.is_empty() =>
        {
            out.push((path.join("."), text.clone()));
        }
        _ => {}
    }
}

/// 一行里的所有引号字面量（`"…"` 与 `'…'`）。
fn quoted_literals(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '"' && ch != '\'' {
            continue;
        }
        let quote = ch;
        let mut literal = String::new();
        for next in chars.by_ref() {
            if next == quote {
                break;
            }
            literal.push(next);
        }
        out.push(literal);
    }
    out
}

/// 一行模板里，哪些引用形叶子被写成了字面量。
fn spelled_references(
    line: &str,
    leaves: &[(String, String)],
    reads: &Reads,
    site: &str,
) -> Vec<String> {
    let mut out = Vec::new();
    for literal in quoted_literals(line) {
        for (path, value) in leaves {
            if &literal != value || !reads.covers(path) {
                continue;
            }
            if line.contains(&format!(".Values.{path}")) {
                continue;
            }
            out.push(format!(
                "{site} 把 {value} 写成了字面量（.Values.{path} 已经在 values 里声明着它）"
            ));
        }
    }
    out
}

/// chart 不许把一个引用形的字符串叶子在模板里再写一遍。
///
/// 这一族与布局路径那一条是同一件事的两种写法：布局路径判的是产品自己卷里的位置
/// （可能不带引号地写在 `workingDir:` 后面），这里判的是带引号的地址、镜像与路径
/// ——`image: "registry:5000/…"` 这类。同一个引用写两处，改一处另一处不动，而两边
/// 各自看起来都像唯一的出处。
///
/// 只判有读者的叶子：没人读的叶子归「每一格都要有人读」那一条，这里不重复报。
#[test]
fn the_chart_does_not_spell_a_reference_a_leaf_declares() {
    let values: serde_yaml::Value = serde_yaml::from_str(&read_repo(VALUES))
        .unwrap_or_else(|e| panic!("{VALUES} 不是合法 YAML：{e}"));
    let mut leaves = Vec::new();
    reference_leaves(&values, &mut Vec::new(), &mut leaves);
    assert!(
        !leaves.is_empty(),
        "{VALUES} 里一个引用形的字符串叶子都没读到：下面那条判据会在空集上通过"
    );
    let reads = consumed_values();

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

    let mut copies = Vec::new();
    for file in &files {
        let text = comment_free(&read(file));
        for (n, line) in text.lines().enumerate() {
            copies.extend(spelled_references(
                line,
                &leaves,
                &reads,
                &format!("{}:{}", file.display(), n + 1),
            ));
        }
    }
    copies.sort();
    copies.dedup();
    assert!(
        copies.is_empty(),
        "同一个引用在 values 与模板里各声明了一次：\n{}",
        copies.join("\n")
    );
}

/// 这条判据不是空转：同一条引用写两遍会被点名，指着叶子取的那一行不会。
#[test]
fn the_reference_gate_names_a_spelled_copy_and_lets_the_reference_through() {
    let mut reads = Reads::default();
    reads.exact.insert("backends.redis.addr".to_string());
    let leaves = vec![(
        "backends.redis.addr".to_string(),
        "redis://redis:6379".to_string(),
    )];
    let copies = spelled_references(
        "              value: \"redis://redis:6379\"",
        &leaves,
        &reads,
        "probe.yaml:9",
    );
    assert_eq!(
        copies.len(),
        1,
        "同一条引用在模板里写成字面量没有被点名：{copies:#?}"
    );
    let taken = spelled_references(
        "              value: {{ .Values.backends.redis.addr | quote }}",
        &leaves,
        &reads,
        "probe.yaml:9",
    );
    assert!(
        taken.is_empty(),
        "指着叶子取的那一行被当成了复述：{taken:#?}"
    );
}

/// 文档 `env` 表里每个变量，在文档正文那条路径上的值——写成环境变量行会带的那种字符串。
///
/// 非标量（对象、数组、null）不进这张表：环境变量只能送一个标量，那种路径本来就
/// 不是 env 层能表达的东西，轮不到这道判据说话。
fn document_env_values(doc: &serde_json::Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (name, path) in doc_env_map(doc) {
        let Some(value) = value_at(doc, &path) else {
            continue;
        };
        let shown = match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => continue,
        };
        out.insert(name, shown);
    }
    out
}

/// 一行模板写死的 `COGNEVA_*` 值：变量名与引号里的字面量。
///
/// 只认完整引号包住的标量：`{{ … }}` 那一类是投影（谁投影的归各自的门），
/// `value: "a" # 说明` 这一类的尾巴由 `env_assignment` 之前的形态挡住——引号
/// 不在结尾就不算字面量，宁可漏判一条也不要拿半行去比。
fn env_literal(line: &str) -> Option<(&str, &str)> {
    let (name, value) = env_assignment(line)?;
    if !name.starts_with("COGNEVA_") {
        return None;
    }
    let value = value.trim();
    let inner = value.strip_prefix('"')?.strip_suffix('"')?;
    if inner.contains(['{', '}']) {
        return None;
    }
    Some((name, inner))
}

/// 把整行的 YAML 注释抹成空行，**不动行号**：点名要指回文件里那一行。
///
/// `comment_free` 会把注释行删掉，后面每一行都往前挪——报出来的行号对不上文件，
/// 读的人顺着它找不到那一行。
fn blank_comments(text: &str) -> String {
    text.lines()
        .map(|line| {
            if line.trim_start().starts_with('#') {
                ""
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// 一段模板里，哪些 `COGNEVA_*` 行把交接文档已经写着的那句话又写了一遍。
fn restatements(text: &str, declared: &BTreeMap<String, String>, site: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let Some((name, literal)) = env_literal(line) else {
            continue;
        };
        let Some(want) = declared.get(name) else {
            continue;
        };
        if want == literal {
            out.push(format!(
                "{site}:{}  {name}: \"{literal}\"——文档在 env 表指向的那条路径上已经写着它",
                n + 1
            ));
        }
    }
    out
}

/// env 层不许把交接文档已经写着的话再说一遍。
///
/// 这一族是裸词——`cogneva`、`main`、`local-fs` 这类既可能是应用名、也可能是命名
/// 空间、也可能是库名的字面量。按字面量相等判它是不成立的：同一个词在两处完全可能
/// 是两件事（`COGNEVA_AGENT_GROUP: "main"` 与 `gitops.clusterName: main` 就是）。
/// 但这道判据不比字面量，它比的是**变量名通向哪条路径**：文档自己的 `env` 表说
/// `COGNEVA_APP_NAME` 落在 `app.name`，而文档正文在 `app.name` 上写着同一个值——
/// 于是这两行是同一件事的两处声明。证据来自文档自己的映射表，不是人对词的判断。
///
/// 判词只落在「值相等」这一半。值不等的那些是另一个面：覆盖层本来就允许盖掉文档
/// （进化 Pod 关掉 memory、关掉分层迁移），那是这份 Pod 与共享文档的差，不是复述。
/// 值相等的那一行什么都决定不了：文档说不带 env 跑就是这个值，而带 env 跑的这一行
/// 送进去的还是它——两处一起改才生效，只改一处，被盖掉的那处沉默地读不到。
///
/// 修法两条，按这一行是谁的来定：共享的那份（`cogneva-config`）从文档取——送进
/// Pod 的值一字不差，只是不再由模板另写一遍；某一份 Pod 覆盖层里的，删掉——文档
/// 已经说着它，那份覆盖层没有作出任何差。
#[test]
fn the_env_layer_does_not_restate_the_handover_document() {
    let doc: serde_json::Value = serde_json::from_str(&read_repo(CHART_DOC))
        .unwrap_or_else(|e| panic!("{CHART_DOC} 不是合法 JSON：{e}"));
    let declared = document_env_values(&doc);
    assert!(
        !declared.is_empty(),
        "{CHART_DOC} 的 env 表一条标量路径都没解析出来：下面那条判据会在空集上通过"
    );

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

    let mut found = Vec::new();
    for file in &files {
        let text = blank_comments(&read(file));
        found.extend(restatements(&text, &declared, &file.display().to_string()));
    }
    assert!(
        found.is_empty(),
        "这些环境变量把交接文档已经写着的那句话又写了一遍：\n{}",
        found.join("\n")
    );
}

/// 这条判据不是空转：复述的那一行会被点名，覆盖与投影都不会。
#[test]
fn the_restatement_gate_names_a_copy_and_lets_an_override_through() {
    let mut declared = BTreeMap::new();
    declared.insert("COGNEVA_APP_NAME".to_string(), "cogneva".to_string());
    declared.insert("COGNEVA_MEMORY_ENABLED".to_string(), "true".to_string());

    let probe = "  COGNEVA_APP_NAME: \"cogneva\"\n  COGNEVA_LOG_LEVEL: \"info\"\n  COGNEVA_MEMORY_ENABLED: \"false\"\n  COGNEVA_PG_PROVIDER: {{ $doc.providers.pg.provider | quote }}\n";
    let found = restatements(probe, &declared, "probe.yaml");
    assert_eq!(
        found.len(),
        1,
        "只该点名复述的那一行（日志级别不在文档映射里、memory 是这份 Pod 的差、\
         provider 是投影）：{found:#?}"
    );
    assert!(
        found[0].contains("COGNEVA_APP_NAME") && found[0].contains("probe.yaml:1"),
        "点名要落在文件与行号上：{found:#?}"
    );
}

/// 共享的 env 层：整份的身份是把交接文档投影进环境变量，不是作出第二个决定。
///
/// `app-config.yaml` 不在这里——它是每份 Pod 的覆盖层，盖掉文档正是它的用处。
const SHARED_ENV_LAYERS: [&str; 2] = [
    "deploy/helm/cogneva/templates/configmap.yaml",
    "deploy/k3s/configmap.yaml",
];

/// 共享层写进去的字面量与文档在同一条路径上写的值不一样的地方。
///
/// 返回比过几行，以及 `(变量名, 文档的值, env 的值)` 与一处落点。投影（`{{ … }}`）
/// 不进来：它送进去的就是文档的值，这一类由第五面按「复述或投影」各自判。
/// 一处 `(变量名, 文档的值, env 的值)`，以及它落在哪一行。
type DeadDocumentValue = ((String, String, String), String);

fn dead_document_values(
    declared: &BTreeMap<String, String>,
    text: &str,
    site: &str,
) -> (usize, Vec<DeadDocumentValue>) {
    let mut compared = 0;
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let Some((name, literal)) = env_literal(line) else {
            continue;
        };
        let Some(want) = declared.get(name) else {
            continue;
        };
        compared += 1;
        if want != literal {
            out.push((
                (name.to_string(), want.clone(), literal.to_string()),
                format!("{site}:{}", n + 1),
            ));
        }
    }
    (compared, out)
}

/// 共享层与文档对同一条路径给了两个值：跑的是 env 那个，文档那个没人读。
///
/// 这不是「覆盖层允许的差」——覆盖层是 `app-config.yaml`，它给每份 Pod 写自己的差；
/// 共享层整份就是从文档投影出来的，在那里再写一个不同的值，同一件事就有了第二个
/// 决定，而读文档的人以为那就是部署的取值。
///
/// 今天全树只有一处，且是一笔**还没定**的部署决定：`COGNEVA_STORAGE_PROVIDER` 写
/// `local-fs`，交接文档 `providers.storage.provider` 写 `seaweedfs`（连同一整套
/// endpoint / bucket / 凭据，在这个部署上谁都走不到）。哪边是对的不从树里判——要看
/// 活着的集群上 seaweedfs 的卷里有没有对象，这份读数的现场不在本仓。判据把这一处
/// 钉住，是为了让它**不能悄悄变化**：这一对值任何一边动了，都说明部署换了存储后端，
/// 那件事要按实测重新定，不是一次渲染改动。
///
/// 那条部署决定收口之后这条断言要收紧成空集：两处声明同值（于是这一对消失），或者
/// 文档那一份删掉（路径只剩 env 一层声明）。
#[test]
fn the_shared_env_layer_does_not_leave_the_document_stating_a_dead_value() {
    let doc: serde_json::Value = serde_json::from_str(&read_repo(CHART_DOC))
        .unwrap_or_else(|e| panic!("{CHART_DOC} 不是合法 JSON：{e}"));
    let declared = document_env_values(&doc);
    assert!(
        !declared.is_empty(),
        "{CHART_DOC} 的 env 表一条路径都没解析出来：下面那条判据会在空集上通过"
    );

    let mut compared = 0;
    let mut found = Vec::new();
    for layer in SHARED_ENV_LAYERS {
        let text = blank_comments(&read(&repo_root().join(layer)));
        let (n, hits) = dead_document_values(&declared, &text, layer);
        compared += n;
        found.extend(hits);
    }
    // 下限只挡「走查整个失效」：两张共享层上落在文档映射里的字面量今天 14 条（7 + 7，
    // 两张是同一份的两次投影）。它只挡读法坏掉，不是要把这个数钉住。
    assert!(
        compared >= 10,
        "共享层只比了 {compared} 条落在文档映射里的字面量，floor 10——疑读法坏了，\
         下面的对账会在空集上通过"
    );

    let mut differing: BTreeSet<(String, String, String)> = BTreeSet::new();
    for (key, _) in &found {
        differing.insert(key.clone());
    }
    let known: BTreeSet<(String, String, String)> = BTreeSet::from([(
        "COGNEVA_STORAGE_PROVIDER".to_string(),
        "seaweedfs".to_string(),
        "local-fs".to_string(),
    )]);
    assert_eq!(
        differing, known,
        "共享层与交接文档对同一条路径给了两个值，或已知那一处变了。已知的那一处是\n\
         `providers.storage.provider`：文档写 seaweedfs、两张载体都写 local-fs，跑的是\n\
         后者，文档那段 S3 配置谁都走不到——这是 D7，要在活着的集群上量过才知道哪边对，\n\
         不能顺手改。落点：{:#?}。新出现的条目不是覆盖，是同一件事的第二个决定：要么\n\
         删掉文档那一份（它什么都没决定），要么让两边同值。",
        found
    );
}
