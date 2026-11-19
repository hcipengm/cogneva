// reads: deploy/helm/cogneva/files/cogneva.json subject
//! principle: feedback_a_whole_tree_census_reads_shapes_so_new_artifacts_must_declare_themselves
//!
//! 交付文档里的每一个数，都是「这个部署决定用这个数」的一句话。但这句话有一个完全
//! 同形的替代写法：把代码里 `Default` 已经说过的数在文档里再抄一遍。两种写法在纸上
//! 一模一样，在运行期也一样——默认值生效时读到的就是同一个数。区别只在有人去改的时候：
//! 改代码默认值的人以为改了一处，部署却还按文档里那份旧数跑；看文档的人以为这是本部署
//! 的决定，它却只是某个函数体里那个数的副本。
//!
//! 这个文件判的正是这一半：**文档里的数不许等于应用自己对它的答案**。
//!
//! 判据从应用自己取，不从名单取。把那个叶子从文档里删掉，按 `AppConfig` 解析一遍再
//! 序列化回来，读同一个路径——值不变，说明这个叶子什么都没决定。用解析往返而不是
//! `Default::default()`：`#[serde(default = "…")]` 与 `impl Default` 会给出两个不同的数，
//! 猜错哪一个都会把「这个叶子有用」判成「没用」。
//!
//! 不判什么，说清楚：
//! - **既非默认值、又没有约定**的数（「无主」）这一轮不判。它的判据要有「同处约定声明」
//!   这一面，而那一面今天只有 7 处有（`metrics._comment_sample_max_rows` 这类
//!   `_comment_<键>` 兄弟键）；先把抄件清零，无主那一半再单独立门，两半混在一个红里
//!   会让人以为删掉抄件就干净了。
//! - 删一个叶子会不会改变行为：不会。上面那个往返就是这个断言的机器版本；删掉的叶子
//!   全部是「删了读到同一个数」的那一类。
//!
//! 抄件删掉之后，「这个旋钮没有文档点名」那一半由
//! `every_config_knob_is_named_by_a_delivered_document_test` 接手：文档自己的 `env`
//! 表点到它的（27 个）算点名，其余 38 个在那边登记为什么只能由代码给值。两道门咬
//! 在一起才不互相拆台——只留这一道，删掉抄件就等于让旋钮从交付面消失；只留那一道，
//! 让文档变绿的唯一办法就是把默认值再抄一遍。
//!
//! 走查本身要看得见东西：把整份文档原样解析往返一遍，每个被点名的数值叶子都必须在
//! 往返结果里以同一个数出现。读不到数说明这个键应用根本不认，走查看到的「数」是一份
//! 应用看不见的文本；数对不上说明走查的读法与应用对同一个键的理解已经分叉。

use crate::common;

use common::routes::read_repo;

use serde_json::Value;

use std::collections::BTreeMap;

use cogneva::config_loader::AppConfig;

/// 交付的配置文档：本判据判的那一层。
const CHART_DOC: &str = "deploy/helm/cogneva/files/cogneva.json";

/// 路径上的一步：进对象的一个键，或者进数组的一个下标。
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Key(String),
    Index(usize),
}

impl Step {
    fn render(&self, out: &mut String) {
        match self {
            Step::Key(key) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(key);
            }
            Step::Index(index) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(&index.to_string());
            }
        }
    }
}

fn path_text(steps: &[Step]) -> String {
    let mut out = String::new();
    for step in steps {
        step.render(&mut out);
    }
    out
}

fn is_number(value: &Value) -> bool {
    value.is_number()
}

/// 文档里的数值叶子：跳过 `_comment*` 键（它们是散文，不是配置）。
fn numeric_leaves(doc: &Value) -> Vec<(String, Vec<Step>, Value)> {
    fn walk(value: &Value, at: &mut Vec<Step>, out: &mut Vec<(String, Vec<Step>, Value)>) {
        match value {
            Value::Object(map) => {
                for (key, child) in map {
                    if key.starts_with("_comment") {
                        continue;
                    }
                    at.push(Step::Key(key.clone()));
                    if is_number(child) {
                        out.push((path_text(at), at.clone(), child.clone()));
                    } else {
                        walk(child, at, out);
                    }
                    at.pop();
                }
            }
            Value::Array(items) => {
                for (index, child) in items.iter().enumerate() {
                    at.push(Step::Index(index));
                    if is_number(child) {
                        out.push((path_text(at), at.clone(), child.clone()));
                    } else {
                        walk(child, at, out);
                    }
                    at.pop();
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(doc, &mut Vec::new(), &mut out);
    out
}

fn value_at<'a>(doc: &'a Value, steps: &[Step]) -> Option<&'a Value> {
    let mut cur = doc;
    for step in steps {
        cur = match step {
            Step::Key(key) => cur.as_object()?.get(key)?,
            Step::Index(index) => cur.as_array()?.get(*index)?,
        };
    }
    Some(cur)
}

fn remove_at(doc: &mut Value, steps: &[Step]) -> bool {
    let Some((last, parents)) = steps.split_last() else {
        return false;
    };
    let mut cur = doc;
    for step in parents {
        cur = match step {
            Step::Key(key) => match cur.as_object_mut() {
                Some(map) => match map.get_mut(key) {
                    Some(child) => child,
                    None => return false,
                },
                None => return false,
            },
            Step::Index(index) => match cur.as_array_mut() {
                Some(items) => match items.get_mut(*index) {
                    Some(child) => child,
                    None => return false,
                },
                None => return false,
            },
        };
    }
    match last {
        Step::Key(key) => cur
            .as_object_mut()
            .and_then(|map| map.remove(key))
            .is_some(),
        // 数组里的一个数是某个值的组成部分（`ports: [3306]`），不是「这个部署决定用
        // 这个数」的一句话。删掉它只会让后面的元素前移、把另一个数读到这个位置上，
        // 所以它不在「删掉试一试」这条判据里。
        Step::Index(_) => false,
    }
}

/// 去掉这个叶子之后，应用自己给同一个字段的答案。
///
/// `None` 有四种来源，都读作「这个叶子不是抄件」，但原因要分得开：数组元素（不是一个
/// 可以删的声明）、删不掉（路径与文档对不上）、删掉之后文档解析不了（这个字段是必填的，
/// 文档必须写）、往返里没有这个字段（应用不认这个键）。
fn the_apps_answer_without(doc: &Value, steps: &[Step]) -> Option<Value> {
    let mut without = doc.clone();
    if !remove_at(&mut without, steps) {
        return None;
    }
    let parsed: AppConfig = serde_json::from_value(without).ok()?;
    let round = serde_json::to_value(&parsed).ok()?;
    value_at(&round, steps).cloned()
}

/// 两个 JSON 数说的是不是同一个数（`1` 与 `1.0` 在 JSON 里不同形，在配置里同值）。
fn same_number(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => (x - y).abs() <= f64::EPSILON * x.abs().max(y.abs()).max(1.0),
        _ => false,
    }
}

fn shipped_doc() -> Value {
    serde_json::from_str(&read_repo(CHART_DOC))
        .unwrap_or_else(|e| panic!("{CHART_DOC} 不是合法 JSON：{e}"))
}

/// 走查看到的那份文档，就是应用读到的那份。
///
/// `AppConfig` 只认十四个顶层节——`agent_loop`、`memory`、`github_integration`、
/// `boundary` 这些节由各自的插件用各自的配置类型读，`AppConfig` 往返之后整个节都
/// 不在。这些节里的数本门读不到它们的默认值，**不判**，但必须数出来、点名：不判
/// 的量与判过的量在读数面上分得开，才不会把「门没看这里」读成「这里看过、没问题」。
///
/// 「节不在往返里」与「节在、字段不在」要分开：前者是这个类型不管这一节，后者是
/// 字段被 `skip_serializing_if` 省掉或改了名——同一个键，走查与应用的理解已经分叉。
#[test]
fn the_walk_reads_the_document_the_app_reads() {
    let doc = shipped_doc();
    let leaves = numeric_leaves(&doc);
    assert!(
        leaves.len() >= 1,
        "数值叶子一个都没走读到：走查读法坏了，下面那条判据会在空集上通过"
    );

    let parsed: AppConfig = serde_json::from_value(doc.clone()).unwrap_or_else(|e| {
        panic!("{CHART_DOC} 按 AppConfig 解析不了：{e}——判据读不懂它要判的东西")
    });
    let round = serde_json::to_value(&parsed).expect("AppConfig 能序列化回 JSON");

    let mut broken = Vec::new();
    let mut unowned: BTreeMap<String, usize> = BTreeMap::new();
    let mut owned = 0;
    for (path, steps, shipped) in &leaves {
        match value_at(&round, steps) {
            Some(theirs) if same_number(theirs, shipped) => owned += 1,
            Some(theirs) => broken.push(format!(
                "{path}: 文档写 {shipped}，应用解析往返之后是 {theirs}——同一个键，两边理解已经分叉"
            )),
            None => {
                let section = match &steps[0] {
                    Step::Key(key) => key.clone(),
                    Step::Index(_) => "<顶层数组>".to_string(),
                };
                // 「这一块不归这个类型」与「这一块归、字段却被省掉」要分开：前者是
                // 另一个 crate 按 JSON 指针读的块（`self_evolution.artifact_evolution`
                // 就是），后者是同一个键两边理解分叉。判据是叶子**父亲**在不在往返里。
                let parents = &steps[..steps.len() - 1];
                if parents.is_empty() || value_at(&round, parents).is_some() {
                    broken.push(format!(
                        "{path}: 这一块在往返里有，这个字段却没有——被 `skip_serializing_if` \
                         省掉或改了名，走查看到的键应用不按这个名字读"
                    ));
                } else {
                    *unowned.entry(section).or_default() += 1;
                }
            }
        }
    }
    assert!(
        broken.is_empty(),
        "走查与应用对这份文档的理解不一致：\n{}",
        broken.join("\n")
    );
    assert!(
        owned >= 1,
        "一个叶子都没和应用的往返对上：这条判据在空集上通过了"
    );

    let unowned_total: usize = unowned.values().sum();
    let named: Vec<String> = unowned.iter().map(|(s, n)| format!("{s}({n})")).collect();
    println!(
        "{}: 数值叶子 {} 个（数组里 {} 个）；本类型认得并逐一比对 {} 个；\
         其余 {} 个在 {} 个不归本类型的节里，本门不判",
        CHART_DOC,
        leaves.len(),
        leaves
            .iter()
            .filter(|(_, steps, _)| steps.iter().any(|s| matches!(s, Step::Index(_))))
            .count(),
        owned,
        unowned_total,
        unowned.len()
    );
    println!("不归本类型、本门不判的节：{}", named.join("、"));
}

/// 文档里不许出现一个「删掉它、应用给出的还是同一个数」的叶子。
#[test]
fn no_number_restates_the_answer_the_app_already_gives() {
    let doc = shipped_doc();
    let leaves = numeric_leaves(&doc);

    let mut copies = Vec::new();
    let mut judged = 0;
    for (path, steps, shipped) in &leaves {
        let Some(default) = the_apps_answer_without(&doc, steps) else {
            continue; // 必填字段、或应用不认这个键：不是抄件，另外两条判据各有归属
        };
        judged += 1;
        if same_number(&default, shipped) {
            copies.push(format!(
                "{path}: 文档写 {shipped}，删掉这个叶子应用给的还是 {default}"
            ));
        }
    }

    assert!(
        judged >= 1,
        "一个叶子都没判到：「删掉它应用给什么」这条路整个没走通，判据在空集上通过了"
    );
    println!(
        "{CHART_DOC}: 判了 {judged} 个数值叶子，抄件 {} 个",
        copies.len()
    );
    assert!(
        copies.is_empty(),
        "文档把应用自己给的答案抄了一遍（{} 个）。删掉这些叶子：它们没有决定任何事，\
         只让人以为这个部署在它们上面做过决定；代码里那处才是唯一声明。\n{}",
        copies.len(),
        copies.join("\n")
    );
}
