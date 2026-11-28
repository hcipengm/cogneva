//! principle: feedback_no_hardcoding_config_surface
//!
//! 配置面的第三个半场：字段有、文档写了，生产代码还得真的读它。
//!
//! 前两道门各守一侧——文档写的键必须有字段接着（`config_section_reader_contract_test`），
//! 类型能读的键必须有一份交付文档写出来（`every_config_knob_is_named_by_a_delivered_document_test`）。
//! 两侧都绿的时候，还存在第三种状态：字段与文档都在，而没有任何生产代码读它。运维改了
//! 这个值，没有东西跟着变；它等价于写死在代码里，只是多了一层能改的假象。
//!
//! M6/M7 清掉的十一个字段就是这样找到的，但当时的探针是一次性的：按名字在树上搜，
//! 看不见解构读取，也会把另一个类型上恰好同名的字段当成读取。本门把那次的判据留在
//! 树上，并把读法收紧到「按类型名或节路径锚定」：
//!
//! * 节路径（`self_evolution.poll_interval_secs`）出现即算读到；
//! * 接收者类型在一个函数作用域里查得出来的 `.字段` 访问算读到；
//! * `T { f, .. }` 这样的解构算读到，构造字面量 `T { f: v }` 不算。
//!
//! 剥注释与字符串、切掉测试半场，都在 `common::config_consumption` 里，判据只此一份。
//! 没有永久豁免：字段没人读，就接上生产读取，或者连同环境映射与交付文档键一起删掉。
//! 一个只活在 `Default` 里的字段，与写死的常量没有区别。

use crate::common;

use common::config_consumption::census;
use common::config_surface::{readers, TypeGraph};
use common::routes::repo_root;

/// 至少该读到多少个配置字段。空集让任何「都读到了」在空集上成立。
const FIELD_FLOOR: usize = 250;

/// 至少该读到多少条访问链。读错树时链数会塌到个位数，而断言仍会「通过」。
const CHAIN_FLOOR: usize = 5_000;

#[test]
fn every_config_field_is_read_by_production_code() {
    let root = repo_root();
    let graph = TypeGraph::new(&root);
    let fields = graph.config_fields(&readers());
    assert!(
        fields.len() >= FIELD_FLOOR,
        "只从读取器类型里读到 {} 个配置字段（下限 {FIELD_FLOOR}）：普查没有题目，\
         任何「每个字段都有人读」的断言在这么小的集合上都成立",
        fields.len()
    );

    let evidence = census(&root, &graph, &fields);
    assert!(
        evidence.chains() >= CHAIN_FLOOR,
        "只读到 {} 条访问链（下限 {CHAIN_FLOOR}）：这份普查没有真读到 crates/*/src，\
         断言在空集上会自己通过",
        evidence.chains()
    );

    let missing: Vec<&_> = fields
        .iter()
        .filter(|field| !evidence.covers(field))
        .collect();
    let listing: Vec<String> = missing
        .iter()
        .map(|field| {
            format!(
                "  {} — {crate}::{}, field `{ident}`",
                field.section_path,
                field.owner_type,
                crate = field.owner_crate,
                ident = field.ident,
            )
        })
        .collect();
    assert!(
        missing.is_empty(),
        "{} of {} config fields are read by no production code:\n{}\n\n\
         一个没人读的字段就是写死的常量：运维改了它，没有东西跟着变。两条路——\n\
         在生产代码里接上读取（并有一条「改配置值 → 行为跟着变」的测试），\n\
         或者连同它的环境映射与交付文档键一起删掉。没有永久豁免。",
        missing.len(),
        fields.len(),
        listing.join("\n")
    );
}
