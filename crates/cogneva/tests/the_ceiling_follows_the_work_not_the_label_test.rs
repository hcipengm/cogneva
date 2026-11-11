//! principle: flat-agent-society
//!
//! 轮次上限只有两个来源：操作者在配置面上钉的数（覆盖），和这个进程自己交卷
//! 运行跑出来的轮数（实测）。第三个来源曾经存在——技能行上的 `max_iterations`，
//! 行按角色名取——于是同一件活换个标签接手就换一个天花板。
//!
//! 上限裁的是「这一轮还要不要接着干」。那是一个关于**工作量**的问题，而标签是
//! 注册表上一个自由词：它不预测这份活要跑几轮，也不该决定谁被砍在半路。删掉
//! 那一格之后没有任何一处报错——行少一个字段，加载照常成功，跑起来照常有输出，
//! 只是同一个进程里两条标签下的同一件活按两个上限收场。所以判据长在三条上：
//! 行里没有这一格、代码里没有从标签推上限的读点、同一批运行换标签读出的数相同。

use crate::common;

use common::root::repo_root;
use common::rust_production::{production_sources, relative_name};

use cog_agent::observable::{AgentObservable, RunOutcome};
use cog_core::roles;

/// 同一批运行，两个标签，两个上限必须相同。
///
/// 两个 observable 收到的是同样六次交付（9 轮），唯一差别是记在哪个角色名下。
/// 按角色存样本时这会读出两个不同的天花板——同一件活换个人接，上限就换了。
#[test]
fn the_same_runs_under_two_labels_yield_the_same_ceiling() {
    let under_planner = AgentObservable::new();
    let under_generator = AgentObservable::new();
    for _ in 0..6 {
        under_planner.record_run(roles::PLANNER, RunOutcome::Delivered, 9, 20, 4);
        under_generator.record_run(roles::GENERATOR, RunOutcome::Delivered, 9, 20, 4);
    }

    let planner = under_planner.iteration_budget_for(u32::MAX);
    let generator = under_generator.iteration_budget_for(u32::MAX);
    assert_eq!(
        planner, generator,
        "同一件活换一个标签接，上限必须一样：planner 读到 {planner}，generator 读到 {generator}"
    );
    assert!(
        planner > 9,
        "六次 9 轮的交付给出的上限要容得下它们：{planner}"
    );
}

/// 技能行只剩身份与种类：没有任何一格能被读成轮次。
#[test]
fn a_skill_row_has_no_turn_ceiling() {
    // 随仓附带的技能行里没有这个键。
    let dir = repo_root().join("skills");
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("skills/ readable")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "skills/ 里一份 JSON 都没有：这条判据读了个空"
    );
    for path in files {
        let text = std::fs::read_to_string(&path).expect("skill file readable");
        assert!(
            !text.contains("max_iterations"),
            "{} 里还写着轮次：技能行按角色名分发，写上它就是让标签决定谁被砍在半路",
            path.display()
        );
    }

    // 结构里也没有地方写了：写出去的行只有这三个键。
    let row = cog_core::SkillConfig {
        skill_id: "planner".into(),
        name: "Planner".into(),
        role_type: "planner".into(),
    };
    let shaped = serde_json::to_value(&row).expect("技能行写得出去");
    assert!(
        shaped.get("max_iterations").is_none(),
        "`SkillConfig` 又把轮次写出来了：这一格回来，按标签分发天花板也会跟着回来"
    );
}

/// `SkillConfig` 只有三格：身份、名字、种类。
///
/// 逐字段读出来而不是只说「没有 max_iterations」：这一行任何一格回来都可能重新
/// 成为按标签分发行为的入口（工具白名单与人格就是这样回来的两次）。要加一格，
/// 先在这里说清它为什么不是标签的函数。
#[test]
fn the_executor_row_is_identity_and_kind_only() {
    let path = repo_root().join("crates/cog-core/src/types/action_plan.rs");
    let text = std::fs::read_to_string(&path).expect("action_plan.rs readable");
    let start = text
        .find("pub struct SkillConfig {")
        .expect("SkillConfig is declared in action_plan.rs");
    let body = &text[start..];
    let end = body.find("\n}").expect("the struct has an end");
    let fields: Vec<String> = body[..end]
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let rest = line.strip_prefix("pub ")?;
            // 第一行是 `pub struct SkillConfig {` 本身，不是一格。
            if rest.contains('{') {
                return None;
            }
            Some(rest.split(':').next().unwrap_or_default().to_string())
        })
        .collect();
    assert_eq!(
        fields,
        vec![
            "skill_id".to_string(),
            "name".to_string(),
            "role_type".to_string()
        ],
        "执行器行多了或少了一格：\n{fields:?}\n一行只说它是哪种手，不说它是谁、能用什么、能跑几轮"
    );
}

/// 树上没有一处从标签推轮次：上限的推导不接受角色名，运行时不从技能行读预算。
#[test]
fn nothing_derives_a_ceiling_from_a_label() {
    let root = repo_root();
    let mut wrong_signature: Vec<String> = Vec::new();
    let mut read_off_the_row: Vec<String> = Vec::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for (n, line) in text.lines().enumerate() {
            if line.contains("fn iteration_budget_for(") && !line.contains("seed: u32") {
                wrong_signature.push(format!(
                    "{}:{}: {}",
                    relative_name(&root, &path),
                    n + 1,
                    line.trim()
                ));
            }
            // 同一行同时出现技能行与轮次，就是那一跳回来了：`skill_config` 上
            // 的 `max_iterations` 按角色名取到，再把这一轮裁掉。
            if line.contains("skill_config") && line.contains("max_iterations") {
                read_off_the_row.push(format!(
                    "{}:{}: {}",
                    relative_name(&root, &path),
                    n + 1,
                    line.trim()
                ));
            }
        }
    }
    assert!(
        wrong_signature.is_empty(),
        "轮次上限的推导又收下了角色名：{wrong_signature:?}。样本是这个进程的，\n\
         标签只落在读数上，不进这条推导"
    );
    assert!(
        read_off_the_row.is_empty(),
        "又有一处从技能行读轮次：{read_off_the_row:?}。行按角色名分发，读它\n\
         等于让标签决定谁被砍在半路"
    );
}
