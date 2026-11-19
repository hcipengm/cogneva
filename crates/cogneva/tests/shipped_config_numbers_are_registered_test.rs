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
//! 另一半是同一个病的反面：一个数既不是应用给的默认值、旁边又没人写过它凭什么取这个值。
//! 那种数就是属主口径里的 `override`——人拍的一个数，答不出该由哪条实测导出，也点不出
//! 是哪条协议约定。判据是「这个键在这一节的某个块里有没有一条 `_comment_<键>`」：判的是
//! **有没有人写过一句话**，不是散文写得好不好——后者机器判不了，硬判只会奖励贴标签。
//!
//! 判据从读这一节的那个类型取，不从名单取。把那个叶子从文档里删掉，按该节的读取器
//! 类型解析一遍再序列化回来，读同一个路径——值不变，说明这个叶子什么都没决定。用解析
//! 往返而不是 `Default::default()`：`#[serde(default = "…")]` 与 `impl Default` 会给出
//! 两个不同的数，猜错哪一个都会把「这个叶子有用」判成「没用」。读这一节的是哪个类型，
//! 只有 `common::config_surface::readers` 说了算：那道表与 `config_section_reader_contract_test`
//! 共用，抄第二份就会有两份对同一节给出不同答案、而两份都各自绿。
//!
//! 判到什么、不判什么，说清楚：「有没有人写过一句话」这一问，走查对每一个叶子都问；
//! 「是不是抄件」那一问只对其中一类叶子问得出来。
//! - **判到类型答案**的叶子（类型能把这个路径读回来、又有个默认值可比）才判抄件，也判
//!   无主。一个不是代码默认值、旁边又没人写过一句话的数，就是属主口径里的 `override`
//!   ——人拍的一个数，答不出该由哪条实测导出，也点不出是哪条协议约定。它的三种去向：
//!   删掉（本来就不决定任何事的，比如功能关着时那两个不起作用的批参数）、搬出文档由
//!   闭环算（`derived`）、或在值旁边写一句点名约定（`fixed`）。走查把这一类里的无主
//!   数点名并断言为零。
//! - **删了就读不进来**的（「块里必需」）与**是数组元素、不是声明**的两类判不了抄件：
//!   前者没有默认值可比——删掉它，整节读不进来，无从问「值变没变」；后者的问题不是
//!   值，是这一格在搜什么。这两类不属于「本门没看」，它们照样被走查点名并要求各自带
//!   一句来源说明（数组元素的说明挂在数组自己的键上），所以「无主」在这里同样为零。
//! - 删一个叶子会不会改变行为：不会。上面那个往返就是这个断言的机器版本；删掉的叶子
//!   全部是「删了读到同一个数」的那一类。
//! - **返回里没有这个叶子**的（动态键、`Value` 块、被 `skip_serializing_if` 省掉的）不判
//!   抄件：没有默认值可比，也没有一个键能让说明挂上去。它们各自有门——「文档写的键有
//!   字段接着」是 `config_section_reader_contract_test`，「类型能读的键有文档写出来」是
//!   `every_config_knob_is_named_by_a_delivered_document_test`。这里只把它们数出来、
//!   按节点名，免得「本门没看这里」被读成「这里看过、没问题」。
//! - 没有一个读取器的节不判，同样数出来、点名。
//!
//! 走查本身要看得见东西：每节的读取器必须能把这节的文档读进来（`read_back` 不报错），
//! 否则这一节里的叶子全部不可判而判据仍然全绿——那不是干净，是失明。

use crate::common;

use common::config_surface::{readers, Reader};
use common::routes::read_repo;

use serde_json::Value;

use std::collections::BTreeMap;

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
                    if child.is_number() {
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
                    if child.is_number() {
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

/// 读这个叶子所在那一节的读取器，以及这条路径上有多少步属于那一节的起点。
///
/// 一个节的读取器可能在节里再嵌一个（`self_evolution.artifact_evolution` 是另一
/// 个 crate 按 JSON 指针读的），所以先看第一段是不是某一节，再看第二段是不是它的
/// 嵌套读取器。
fn reader_for<'r>(readers: &'r [Reader], steps: &[Step]) -> Option<(&'r Reader, usize)> {
    let Step::Key(section) = steps.first()? else {
        return None;
    };
    let reader = readers.iter().find(|reader| reader.section == section)?;
    if let Some(Step::Key(child)) = steps.get(1) {
        if let Some(nested) = reader.nested.iter().find(|nested| nested.section == child) {
            return Some((nested, 2));
        }
    }
    Some((reader, 1))
}

/// 删掉这个叶子再问读这一节的类型：它给同一个路径什么答案。
enum Answer {
    /// 往返里有这个路径：值就是类型自己的默认值，可以和文档比。
    Gives(Value),
    /// 往返里没有这个路径：动态键、`Value` 块、被 `skip_serializing_if` 省掉的。
    /// 没有默认值可比。
    Absent,
    /// 不是一个可以删的声明：数组元素，或路径与文档对不上。
    NotADeclaration,
    /// 删掉之后这一节读不进类型了：这个叶子是那个块里必须有的——单键枚举的一支
    /// （`condition: {greater_than: 0.9}`）、或类型要求的一块。删它不是「让默认值
    /// 生效」，是把配置删坏，所以它不在本判据里。
    LoadBearing,
}

fn the_answer_without(doc: &Value, steps: &[Step], reader: &Reader, base: usize) -> Answer {
    let mut without = doc.clone();
    if !remove_at(&mut without, steps) {
        return Answer::NotADeclaration;
    }
    let Some(section) = value_at(&without, &steps[..base]) else {
        return Answer::NotADeclaration;
    };
    let Ok(round) = reader.read_back(section) else {
        return Answer::LoadBearing;
    };
    match value_at(&round, &steps[base..]) {
        Some(value) => Answer::Gives(value.clone()),
        None => Answer::Absent,
    }
}

/// 这个叶子有没有说明它的来源：从它所在的块一路向上，有没有哪个对象带
/// `_comment_<键>` 兄弟键。
///
/// 判的是**存在**，不是散文内容——一句话是不是真的点出了那条约定，机器判不了；
/// 机器能判的是「有人为这个键写过一句话」。往上找而不是只看紧邻的父对象，是因为
/// 文档自己的写法就是把一族同名的键在块级说明一次（`infra_watch._comment_for_seconds`
/// 解释每一条规则的 `for_seconds`）；再往上就出了这一节，那已经是别的键的地盘。
///
/// 这条判据只对**不是抄件**的叶子发问：一个既不是代码默认值、又没人写过一句话的
/// 数，就是属主口径里的 `override`——人拍的一个数，答不出该由哪条实测导出，也点
/// 不出是哪条协议约定。
fn same_place_declaration(doc: &Value, steps: &[Step]) -> bool {
    let Some((last, parents)) = steps.split_last() else {
        return false;
    };
    // 数组里的一个数说明的是它所在的那一格（`min_samples_grid: [2, 3, 5, 8]` 是
    // 「这一格搜哪些候选」），所以它的说明挂在数组自己的键上，不挂在元素下标上。
    let key = match last {
        Step::Key(key) => key.clone(),
        Step::Index(_) => match parents.last() {
            Some(Step::Key(key)) => key.clone(),
            _ => return false, // 顶层数组：连「哪一格」都没有
        },
    };
    let comment = format!("_comment_{key}");
    (1..=parents.len()).rev().any(|depth| {
        matches!(
            value_at(doc, &steps[..depth]),
            Some(Value::Object(block)) if block.contains_key(&comment)
        )
    })
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
/// 两件事：每节的读取器必须能把这一节的文档读进来；读不进来的节、没有读取器的节、
/// 返回里没有这个叶子的路径，都必须数出来、点名——不判的量与判过的量在读数面上分得
/// 开，才不会把「门没看这里」读成「这里看过、没问题」。
#[test]
fn the_walk_reads_the_document_the_app_reads() {
    let doc = shipped_doc();
    let leaves = numeric_leaves(&doc);
    assert!(
        !leaves.is_empty(),
        "数值叶子一个都没走读到：走查读法坏了，下面那条判据会在空集上通过"
    );

    let readers = readers();
    // 先问一件不对叶子下手的事：这份文档的这一节，读它的类型读得进来吗。读不进来
    // 时下面每个叶子都会落进「删了就读不进来」那一堆，而那一堆**看起来**像是叶子
    // 都是必需项——真正的缺陷（文档与读取器已经分叉）会藏在里面。
    let mut unreadable = Vec::new();
    for reader in &readers {
        if reader.section == "_comment" {
            continue; // 文档头部那句散文，不是一节
        }
        if let Some(section) = doc.get(reader.section) {
            if let Err(why) = reader.read_back(section) {
                unreadable.push(format!(
                    "{}: 读它的类型 {} 读不进来：{why}",
                    reader.section, reader.type_name
                ));
            }
        }
    }
    assert!(
        unreadable.is_empty(),
        "交付文档里有整节读不进它的读取器，走查没法判这些节里的数：\n{}",
        unreadable.join("\n")
    );

    let mut unowned: BTreeMap<String, usize> = BTreeMap::new();
    let mut typed = 0;
    let mut absent: BTreeMap<String, usize> = BTreeMap::new();
    let mut load_bearing: BTreeMap<String, usize> = BTreeMap::new();
    let mut elements = 0;
    // 不是抄件的那些叶子，旁边有没有一句话说它的来源。
    let mut declared = 0;
    let mut no_declaration: Vec<String> = Vec::new();
    // 「块里必需」与「数组元素」这两类不判抄件，但它们一样是数：有多少带着来源
    // 说明、多少个没有。这是本门不判的那部分人口的读数，不判不等于干净。
    let mut load_bearing_declared = 0;
    let mut elements_declared = 0;
    for (path, steps, _) in &leaves {
        let section_name = match &steps[0] {
            Step::Key(key) => key.clone(),
            Step::Index(_) => "<顶层数组>".to_string(),
        };
        let Some((reader, base)) = reader_for(&readers, steps) else {
            *unowned.entry(section_name).or_default() += 1;
            continue;
        };
        match the_answer_without(&doc, steps, reader, base) {
            Answer::Gives(default) => {
                typed += 1;
                let shipped = value_at(&doc, steps).unwrap_or(&Value::Null);
                if same_number(&default, shipped) {
                    // 抄件：下面那条判据要它清零，这里不重复数。
                } else if same_place_declaration(&doc, steps) {
                    declared += 1;
                } else {
                    no_declaration.push(path.clone());
                }
            }
            Answer::Absent => *absent.entry(section_name).or_default() += 1,
            Answer::LoadBearing => {
                *load_bearing.entry(section_name).or_default() += 1;
                if same_place_declaration(&doc, steps) {
                    load_bearing_declared += 1;
                }
            }
            Answer::NotADeclaration => {
                elements += 1;
                if same_place_declaration(&doc, steps) {
                    elements_declared += 1;
                }
            }
        }
    }
    assert!(
        typed >= 1,
        "一个叶子都没和读取器的往返对上：这条判据在空集上通过了"
    );

    let total = |m: &BTreeMap<String, usize>| -> usize { m.values().sum() };
    let named = |m: &BTreeMap<String, usize>| -> String {
        m.iter()
            .map(|(s, n)| format!("{s}({n})"))
            .collect::<Vec<_>>()
            .join("、")
    };
    println!(
        "{CHART_DOC}: 数值叶子 {} 个；判到类型答案 {} 个；\
         返回里没有 {} 个（动态键）；删了就读不进来 {} 个（块里必需）；\
         是数组元素、不是声明 {} 个；没有读取器 {} 个（{} 节）",
        leaves.len(),
        typed,
        total(&absent),
        total(&load_bearing),
        elements,
        total(&unowned),
        unowned.len()
    );
    println!("返回里没有的：{}", named(&absent));
    println!("删了就读不进来的：{}", named(&load_bearing));
    println!("没有读取器、本门不判的节：{}", named(&unowned));
    println!(
        "不是抄件的 {} 个里：旁边有来源说明 {} 个，没有说明（无主）{} 个",
        typed,
        declared,
        no_declaration.len()
    );
    println!(
        "本门不判的两类里带来源说明的：块里必需 {}/{}、数组元素 {}/{}",
        load_bearing_declared,
        total(&load_bearing),
        elements_declared,
        elements
    );
    // 这两类不判抄件（删了是把配置删坏），但它们一样是数：不判「是不是抄件」不等于
    // 不判「有没有来源」。缺一句话的，和「无主」是同一个病。
    assert_eq!(
        load_bearing_declared,
        total(&load_bearing),
        "有「块里必需」的数旁边没有一句话说它的来源（{} 个）：{}\n\
         它们的说明通常写在这群同名键的上一层块里（`_comment_<键>`），或者写在这条规则\
         自己的对象里。",
        total(&load_bearing) - load_bearing_declared,
        named(&load_bearing)
    );
    assert_eq!(
        elements_declared,
        elements,
        "有数组元素没有一句话说这一格在搜什么（{} 个）：数组的说明挂在数组自己的键上\
         （`_comment_<键>`）。",
        elements - elements_declared
    );
    assert!(
        declared >= 1,
        "一个叶子的来源说明都没认出来：上面那条「数出来、点名」的路整个没走通，\
         下面那条断言会在空集上通过"
    );
    assert!(
        no_declaration.is_empty(),
        "这些数既不是应用给的默认值，旁边也没有一句话说它是怎么来的（{} 个）。\
         它们不是这个部署的决定，是有人拍下的数：删掉它（让默认值生效）、搬出文档\
         由闭环算出、或者在值旁边写一句点名约定。\n{}",
        no_declaration.len(),
        no_declaration.join("\n")
    );
}

/// 文档里不许出现一个「删掉它、应用给出的还是同一个数」的叶子。
#[test]
fn no_number_restates_the_answer_the_app_already_gives() {
    let doc = shipped_doc();
    let leaves = numeric_leaves(&doc);
    let readers = readers();

    let mut copies = Vec::new();
    let mut judged = 0;
    for (path, steps, shipped) in &leaves {
        let Some((reader, base)) = reader_for(&readers, steps) else {
            continue;
        };
        let Answer::Gives(default) = the_answer_without(&doc, steps, reader, base) else {
            continue; // 数组元素、动态键、块里必需：不是抄件，上面那条判据已把它们数出来
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

/// 告警阈值不是豁免。它过不过形状门（`every_alert_condition_has_a_promql_shape_test`
/// 与那条夹具自证）问的是「会不会按预期响」，与「这条线该在哪、凭什么」是两件事：
/// 一条永远不响的规则照样过那些格子。把走查判来源的那一步单独喂一份最小的规则文档，
/// `rules` 那一族带一句 `_comment_greater_than` 时有主，拿走那一句就无主——缺的正是
/// 形状门看不见的那一半。
#[test]
fn an_alert_threshold_is_owned_by_its_family_note() {
    let steps = [
        Step::Key("observability".into()),
        Step::Key("infra_watch".into()),
        Step::Key("rules".into()),
        Step::Index(0),
        Step::Key("condition".into()),
        Step::Key("greater_than".into()),
    ];
    let doc = |note: bool| {
        let mut infra_watch = serde_json::json!({
            "poll_interval_secs": 15,
            "rules": [{
                "name": "disk_nearly_full",
                "promql": "node_filesystem_avail_bytes / node_filesystem_size_bytes < 0.25",
                "condition": { "greater_than": 0.75 },
            }],
        });
        if note {
            infra_watch["_comment_greater_than"] =
                Value::String("这条规则要抓的问题定这条线，不是这一族共用一条实测。".into());
        }
        serde_json::json!({ "observability": { "infra_watch": infra_watch } })
    };
    assert!(
        same_place_declaration(&doc(true), &steps),
        "规则的阈值旁边有一句 `_comment_greater_than`，就该算有主"
    );
    assert!(
        !same_place_declaration(&doc(false), &steps),
        "把那一句拿走，阈值就成了没人说过它从哪来的数；形状门判不到这一层，所以这里必须判"
    );
}
