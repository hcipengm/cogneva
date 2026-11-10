//! principle: flat-agent-society
//!
//! 一张技能行只说「这是哪种手，有几轮」。它不说这一位是谁，也不说这一位能用什么。
//!
//! 两样东西曾经都写在这一行上，而两样都不该在：
//!
//! - `system_prompt` 是人格。它把一种流水线的口吻安在接手的人身上——「你是一位
//!   生成器」——而这一轮做什么，由它拿到的那件活说。更糟的是它按角色发：同一件
//!   活换个人接手，收到的自我介绍就换了，而两句话都在说「你是谁」。
//! - `tools` 是白名单。它把「能不能改工作树」钉在角色名上：同一个进程里的两位对
//!   同一张注册表看到两套工具，裁成哪一套取决于一个自由标签。裁掉的那一项不会
//!   报错，它只是「不存在」，于是模型学会了不去用它——一个能力就这样安静地没了。
//!
//! 两样删掉之后都不会有谁报错：技能行少一个字段，加载照常成功，跑起来照常有
//! 输。所以判据长在文件和树上：文件里没有这两个键，树里没有第二个能收窄工具表
//! 的函数。

use crate::common;

use common::root::repo_root;
use common::rust_production::{production_sources, relative_name};

/// 随仓附带的技能行。
fn shipped_skill_files() -> Vec<std::path::PathBuf> {
    let dir = repo_root().join("skills");
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("skills/ readable")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "skills/ 里一份 JSON 都没有：这条判据读了个空");
    files
}

/// 技能行不再带人格，也不再声明工具边界。
#[test]
fn a_skill_row_carries_neither_a_persona_nor_a_tool_boundary() {
    for path in shipped_skill_files() {
        let text = std::fs::read_to_string(&path).expect("skill file readable");
        assert!(
            !text.contains("You are a"),
            "{} 里还有一句人格设定：这一行不回答「你是谁」，只回答「这是哪种手」",
            path.display()
        );

        let row: serde_json::Value =
            serde_json::from_str(&text).expect("技能行是一份 JSON 对象");
        for key in ["system_prompt", "tools"] {
            assert!(
                row.get(key).is_none(),
                "{} 里的 `{key}` 还在：技能行声明它，运行时就会按角色收窄或按角色灌人格，\
                 而两样都是把一种流水线当成世界的形状",
                path.display()
            );
        }
        assert!(
            row.get("role_type").is_some(),
            "{} 少了 `role_type`：这一行至少要说出它是哪种手",
            path.display()
        );
    }

    // 结构上也没有地方写了：字段删掉之后，反序列化出来的那一行只该有种类与预算。
    let row: cog_core::SkillConfig = serde_json::from_value(serde_json::json!({
        "skill_id": "planner",
        "name": "Planner",
        "role_type": "planner"
    }))
    .expect("技能行读得回来");
    let shaped = serde_json::to_value(&row).expect("技能行写得出去");
    for key in ["system_prompt", "tools"] {
        assert!(
            shaped.get(key).is_none(),
            "`SkillConfig` 又把 `{key}` 写出来了：字段回来，按角色的人格与边界也会跟着回来"
        );
    }
}

/// 树上没有第二个能收窄工具表的函数。
///
/// 白名单删掉之后，工具只有一套：手上有什么由**进程装了什么**决定。留下一个
/// 「按名字裁一张表」的公共函数，下一个人就会再按角色裁一次——而裁下去的那一刀
/// 仍然不会报错。
#[test]
fn nothing_narrows_a_tool_registry_by_name() {
    let root = repo_root();
    let mut hits: Vec<String> = Vec::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for needle in ["fn restricted_to(", "fn missing_tools("] {
            if text.contains(needle) {
                hits.push(format!("{}: {needle}", relative_name(&root, &path)));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "按名字裁工具表的函数还在：{hits:?}。工具由进程装了什么决定，不由角色名决定——\
         收窄一刀不报错，它只是让那一项「不存在」"
    );
}
