//! principle: flat-agent-society
//!
//! 树是任务的，不是接活那个人的。
//!
//! 隔离的粒度等于并行的粒度：一件活有它自己的工作树，这件活上的规划、生成、
//! 评审看的是同一份文件；一个人先后接两件活，那是两棵树。以前树按执行者切，
//! 于是同一件活里的两位各自站在自己的树上——评审读到的是自己那棵树里并不存在
//! 的改动，而这件事不报错。
//!
//! 只按任务切也有代价，这笔账记在这里：一件活空了不代表它做完了，它可能在等
//! 人、等在审批。所以空闲回收不能只看时钟——树里还有没提交的东西就不删，判据
//! 在 `WorkdirRouter::holds_uncommitted_work`。
//!
//! 所以本门分三半：车道名两边算出来必须一模一样（提示词说的树就是命令落进去的
//! 树）；切出来的树必须站在自己的分支上；树上不许有第二套切树的路。

use crate::common;

use common::root::repo_root;
use common::rust_production::{production_sources, relative_name};
use common::rust_text::rust_function_body;

fn source(relative: &str) -> String {
    std::fs::read_to_string(repo_root().join(relative))
        .unwrap_or_else(|e| panic!("{relative} readable: {e}"))
}

/// 提示词里说的那棵树，就是命令真落进去的那棵树。
///
/// 两边不能互相 import：运行时在 cog-agent，路由在 cog-extension。所以名字由
/// `cog-core` 算一次，两边都去取。各拼一遍的话，模型会在前缀里读到一棵树、在
/// 文件里改到另一棵——这种错不报，它只是让之后的每一次读都对不上。
#[test]
fn the_prompt_and_the_router_name_the_same_lane() {
    // 两半都必须在：先按任务，任务不在才按人。任何一半丢了，这条门都该红。
    assert_eq!(
        cog_core::checkout_lane(Some("DSC-1A2B"), Some("squad:DSC-1A2B-generator")),
        Some(cog_core::worktree_task_lane("DSC-1A2B")),
        "点了任务的调用不落在这件活那棵树上"
    );
    assert_eq!(
        cog_core::checkout_lane(None, Some("kai-a9ab11fc")),
        Some(cog_core::worktree_lane("kai-a9ab11fc")),
        "没点名任务的调用没有落回执行者自己的树"
    );

    let runtime = source("crates/cog-agent/src/agent_kernel/runtime.rs");
    let lane_fn = rust_function_body(&runtime, "workspace_lane")
        .expect("workspace_lane 还在：它是系统前缀里那棵树的唯一来源");
    assert!(
        lane_fn.contains("cog_core::checkout_lane"),
        "系统前缀里的车道不再从那一处派生：\n{lane_fn}"
    );
    assert!(
        lane_fn.contains("run_task_id"),
        "车道又只由执行者决定了：同一件活里的两位会站在两棵树上，\
         评审读不到生成写的文件\n{lane_fn}"
    );
}

/// 请求落到哪棵树，只由 `cog_core::checkout_lane` 回答。
///
/// 执行器自己拼一遍「先人后任务」，就与系统前缀那一侧分成了两份规矩；而这份
/// 规矩一旦分叉，模型读到的是 A 树、命令改的是 B 树。
#[test]
fn the_executor_takes_the_lane_from_the_shared_derivation() {
    let server = source("crates/cog-extension/src/command_server.rs");
    let resolve = rust_function_body(&server, "resolve_workdir")
        .expect("resolve_workdir 还在：它是请求落到哪棵树的唯一入口");
    assert!(
        resolve.contains("cog_core::checkout_lane"),
        "执行器不再从那一处取车道：\n{resolve}"
    );
    assert!(
        !resolve.contains("worktree_for_agent"),
        "执行器又自己按执行者拼了一遍车道：两份规矩迟早分叉\n{resolve}"
    );

    // 客户端钉车道也用同一处：钉的是 A、跑的是 B，搬运与找树就都在说别的树。
    let client = source("crates/cog-extension/src/runtime/remote.rs");
    let lane_of = rust_function_body(&client, "lane_of")
        .expect("lane_of 还在：它是客户端钉住哪条车道的唯一来源");
    assert!(
        lane_of.contains("cog_core::checkout_lane"),
        "客户端钉车道的算法与执行器分叉了：\n{lane_of}"
    );
}

/// 切出来的树站在自己的分支上，而不是一个游离头。
#[test]
fn a_checkout_is_cut_on_a_branch_named_after_its_lane() {
    let workdir = source("crates/cog-extension/src/workdir.rs");
    let cut =
        rust_function_body(&workdir, "cut_worktree").expect("cut_worktree 还在：切树的唯一入口");
    assert!(
        cut.contains("lane_branch"),
        "切出来的树没有分支名：提交会落进没有名字的地方\n{cut}"
    );
    assert!(
        cut.contains("\"-B\""),
        "分支必须被建出来或挪回主线（`-B`）：`-b` 在名字被占时直接失败，\
         而车道的分支活得比树长\n{cut}"
    );
    let warm = rust_function_body(&workdir, "point_at_branch")
        .expect("暖池交出去的树也要挂分支，除非池子不再交出树");
    assert!(
        warm.contains("checkout") && warm.contains("\"-B\""),
        "暖池交出去的树没挂上车道分支：池子里的槽是游离切出来的\n{warm}"
    );
}

/// 树落在哪，必须由唯一知道答案的那一侧说出来，并一路回到模型手里。
///
/// 车道名在主应用里拼，目录在执行器上解：模型能算出的只有名字。执行器若不
/// 报路径，模型唯一的办法是跑 `pwd`——一次往返，去问一个已经知道的答案。两侧
/// 任何一侧断掉都不报错，只是模型从此不知道自己在哪棵树里。
#[test]
fn the_checkout_the_executor_names_reaches_the_model() {
    let server = source("crates/cog-extension/src/command_server.rs");
    let handler = rust_function_body(&server, "execute_handler")
        .expect("execute_handler 还在：它是执行器接一条命令的地方");
    assert!(
        handler.contains("CommandEvent::Workdir"),
        "执行器的回复不报工作目录：\n{handler}"
    );

    let tools = source("crates/cog-agent/src/tools.rs");
    assert!(
        tools.contains("reply[\"workdir\"]"),
        "执行器报的目录没有落进 run_command 的回复"
    );
    assert!(
        tools.contains("CommandEvent::Workdir"),
        "工具侧不再读执行器报的工作目录"
    );
}

/// 树上只有一套「怎么切树」。
///
/// 第二套不会红：它平时不跑，某天有人从那里切一棵，就成了「同一件事两种行为」。
#[test]
fn there_is_no_second_way_to_cut_a_tree() {
    let root = repo_root();
    let mut hits = Vec::new();
    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        if text.contains("WorktreeManager") {
            hits.push(relative_name(&root, &path));
        }
    }
    assert!(
        hits.is_empty(),
        "树上还有第二个切树的实现：{hits:?}。留着两套「怎么切树」是下一次「同一件\
         事两种行为」的来源"
    );
}
