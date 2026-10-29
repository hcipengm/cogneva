//! 声明的那句提示词和代码里的兜底，是同一句话的两份。
//!
//! 两条路会分岔而没有任何别的门看得见：代码这一路是测试跑的（每个用例都建不带提示词
//! 管理器的引擎，跑的是兜底），声明那一路是生产跑的（ConfigMap 是部署的进程加载的）。
//! 所以只改一边能过掉每一个行为测试，只改变模型在生产里被告知什么。
//!
//! 比较放在装配根：`prompts/` 由 `cog-prompt` 按进程的方式加载（从随仓发布的那一份
//! 读，不是用例自己写一份），兜底由 `cog-reflection` 声明。两侧都在这里看得见，而被测
//! 的两侧不必因此认识对方——那正是这条守卫放在这里的原因。

use cog_core::contract::prompt::PromptManager as PromptManagerContract;
use cog_prompt::{PromptManager, WatchMode};
use cog_reflection::evolution::fallback;
use std::collections::HashMap;
use std::sync::Arc;

/// Every key this guard compares. The list is checked against what
/// `prompts/` actually declares in both directions, so a declaration added
/// or removed without touching this guard fails here rather than dropping
/// out of coverage.
const COMPARED: [&str; 6] = [
    "agent:default",
    "reflection:evolution_refinement",
    "reflection:evolution_refinement_system",
    "reflection:evolution_tool",
    "reflection:evolution_tool_system",
    "reflection:skill_extractor",
];

/// Load `prompts/` the way a process does: from the directory as shipped,
/// not from a copy written by the test.
///
/// Built by name and read through the contract. A loader names the type it
/// builds, so this is the one line that reaches for the implementation; every
/// read below goes through [`cog_core::contract::prompt::PromptManager`],
/// which is what makes the key set below the declarations' own rather than a
/// second copy of them.
async fn manager() -> Arc<dyn PromptManagerContract> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../prompts");
    let manager = PromptManager::from_dir(dir, WatchMode::None)
        .await
        .unwrap_or_else(|e| panic!("prompts/ must load from {dir}: {e}"));
    Arc::new(manager)
}

/// A `|` block scalar keeps one trailing newline; the code's literal has
/// none. That byte is YAML's, not the instruction's, so it is not a
/// difference in what the model is told.
fn same_instruction(rendered: &str, text: &str) -> bool {
    rendered.trim_end_matches('\n') == text.trim_end_matches('\n')
}

fn refinement_vars() -> (HashMap<String, String>, &'static str, &'static str) {
    let json = "{\"skill_id\":\"sk-1\"}";
    let id = "sk-1";
    (
        HashMap::from([
            ("skill_json".to_string(), json.to_string()),
            ("skill_id".to_string(), id.to_string()),
        ]),
        json,
        id,
    )
}

fn tool_vars() -> (HashMap<String, String>, &'static str, &'static str) {
    let name = "read_file";
    let errors = "timeout\nbad path";
    (
        HashMap::from([
            ("tool_name".to_string(), name.to_string()),
            ("error_patterns".to_string(), errors.to_string()),
        ]),
        name,
        errors,
    )
}

#[tokio::test]
async fn every_declared_prompt_is_the_text_the_code_falls_back_to() {
    let pm = manager().await;
    let mut keys: Vec<String> = pm.keys();
    keys.sort();
    let compared: Vec<String> = COMPARED.iter().map(|k| k.to_string()).collect();
    assert_eq!(
        keys, compared,
        "prompts/ declares a different set of keys than this guard compares"
    );

    // Each arm reads its key as a literal at the call rather than passing the
    // loop's variable down: the workspace-wide sweep that reconciles every
    // prompt call site against `prompts/` reads the key off the call, and a
    // call it cannot read is a call it cannot check.
    for key in COMPARED {
        // Bootstrap entry owned by cog-prompt; this crate never asks for it
        // and so has no fallback to compare against.
        if key == "agent:default" {
            continue;
        }
        let pairs: Vec<(String, String)> = match key {
            "reflection:skill_extractor" => vec![(
                pm.get("reflection:skill_extractor").expect("declared"),
                cog_reflection::extractor::SKILL_EXTRACTOR_SYSTEM.to_string(),
            )],
            "reflection:evolution_refinement" => {
                let (vars, json, id) = refinement_vars();
                vec![(
                    pm.render("reflection:evolution_refinement", &vars)
                        .expect("renders"),
                    fallback::skill_refinement(json, id),
                )]
            }
            "reflection:evolution_refinement_system" => vec![(
                pm.get("reflection:evolution_refinement_system")
                    .expect("declared"),
                fallback::SKILL_REFINEMENT_SYSTEM.to_string(),
            )],
            "reflection:evolution_tool" => {
                let (vars, name, errors) = tool_vars();
                vec![(
                    pm.render("reflection:evolution_tool", &vars)
                        .expect("renders"),
                    fallback::tool_variant(name, errors),
                )]
            }
            "reflection:evolution_tool_system" => vec![(
                pm.get("reflection:evolution_tool_system")
                    .expect("declared"),
                fallback::TOOL_VARIANT_SYSTEM.to_string(),
            )],
            other => panic!(
                "{other} is declared in prompts/ and this guard has no case for \
                 it. If cog-reflection falls back for it, compare that text \
                 here; if another crate reads it, name that crate in this arm."
            ),
        };
        assert!(
            !pairs.is_empty(),
            "{key} produced no comparison, so its arm asserts nothing"
        );
        for (declared, expected) in pairs {
            assert!(
                same_instruction(&declared, &expected),
                "{key}: the declared instruction and the code's fallback differ"
            );
        }
    }
}
