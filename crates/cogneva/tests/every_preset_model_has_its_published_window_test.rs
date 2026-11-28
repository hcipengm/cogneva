//! 设置页里能选的每一只模型，都带着它公布的上下文窗口。
//!
//! 窗口的起点是「谁在答话 × 它公布了多少」。这个数不在代码里，它跟着 LLM 配置
//! 走：操作者挑一只型号，那一项预设带着厂商公布的值，保存时随上游落进
//! `llm-upstreams`，网关的池状态把它报给主应用，主应用写进
//! `cog_core::model_windows`——运行时的「起点」就是从这里读的。
//!
//! 所以缺一格不是「少一条数据」，是这个部署上**没人说过窗口有多大**：配置里那个
//! 声明值是 null，实测要到第一次超窗才说话。判据钉在预设这一端：`PRESETS` 里除了
//! 「自定义」，每一项都得有一个正数。
//!
//! 数字对不对不归这条判据管——那是厂商规格，出处写在预设旁边的注释里；这里管的是
//! **有没有这一格**，以及这份清单还读得懂。

use crate::common::routes::read_repo;

/// WebUI 的 LLM 配置：操作者在这里挑模型，它是「现在用哪些型号」的唯一清单。
const PRESETS: &str = "web/src/overlay/LlmSetupOverlay.tsx";

/// `PRESETS` 数组的原文。
fn presets_block() -> String {
    let text = read_repo(PRESETS);
    let start = text
        .find("const PRESETS")
        .unwrap_or_else(|| panic!("{PRESETS} 里找不到 PRESETS：这条判据读的名字改了"));
    let end = start + text[start..].find("];").expect("PRESETS 数组没有收口");
    text[start..end].to_string()
}

/// 每一项预设的 `(model, context_window)`。
fn presets() -> Vec<(String, String)> {
    let block = presets_block();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(offset) = block[from..].find("model: '") {
        let at = from + offset + "model: '".len();
        let close = block[at..].find('\'').expect("model 字面量没有收口");
        let model = block[at..at + close].to_string();
        // 同一项里的 `context_window` 写在 model 之后；找下一个 `model:` 之前的那一个。
        let rest = &block[at + close..];
        let window = rest.find("context_window:").map(|at| {
            let after = rest[at + "context_window:".len()..].trim_start();
            after
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '_')
                .collect::<String>()
        });
        out.push((model, window.unwrap_or_default()));
        from = at + close;
    }
    out
}

#[test]
fn every_preset_model_carries_its_published_window() {
    let presets = presets();
    assert!(
        presets.len() >= 5,
        "只从 {PRESETS} 读到 {presets:?}：读法过期了，这条判据现在什么都没在守"
    );

    let mut missing = Vec::new();
    let mut checked = 0;
    for (model, window) in &presets {
        if model.trim().is_empty() {
            // 「自定义」那一项：没有型号可查，运行时走实测那条路。
            continue;
        }
        checked += 1;
        let parsed: u64 = window.replace('_', "").parse().unwrap_or(0);
        if parsed == 0 {
            missing.push(model.clone());
        }
    }
    assert!(checked >= 5, "只有 {checked} 只型号真的看过：清单读错了");
    assert!(
        missing.is_empty(),
        "这些预设没有 `context_window`（或写的是 0）：{missing:?}\n\
         保存之后运行时拿到的起点就是「没人说过」——第一次超窗之前，这件事在\n\
         读数面上是看不见的。去厂商的模型页查这个数，写进那一项预设，出处\n\
         （哪一页、哪一天）写在同一项的注释里"
    );
}

/// 每一项预设的窗口都要写明出处：它是 `fixed`，约定就是厂商的模型文档。
///
/// 这一格不是可调的量，没有「实测 → 调整」的闭环可走——能走闭环的是运行时那
/// 一侧（收下过多大的 prompt、拒收时自述的界）。所以它唯一的规矩是**来源可查**：
/// 写一个数却不说是哪一页给的，下一个人没法判断它是不是过期了。这条判据钉的
/// 就是那行注释在不在，不去核对数字——数字对不对只有厂商页面说了算。
#[test]
fn every_preset_window_names_the_document_it_came_from() {
    let block = presets_block();
    let mut uncited = Vec::new();
    let mut cited = 0;
    for entry in block.split("label:").skip(1) {
        let model = entry
            .find("model: '")
            .map(|at| {
                let rest = &entry[at + "model: '".len()..];
                rest[..rest.find('\'').unwrap_or(0)].to_string()
            })
            .unwrap_or(Default::default());
        if model.trim().is_empty() {
            // 「自定义」：没有型号，也就没有文档可引。
            continue;
        }
        let cites = entry.contains("// 窗口：");
        if cites {
            cited += 1;
        } else {
            uncited.push(model);
        }
    }
    assert!(cited >= 5, "只有 {cited} 项带出处：解析器读错了清单的形状");
    assert!(
        uncited.is_empty(),
        "这些预设的 `context_window` 没有出处注释（`// 窗口：厂商…模型文档——N`）：{uncited:?}\n\
         它是 fixed 的那一档：约定是厂商文档，规矩不是「数字好看」而是「查得到」。\n\
         把出处写在那一项预设里，换型号时连同这一行一起换"
    );
}
