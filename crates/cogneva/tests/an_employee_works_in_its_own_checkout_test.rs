//! principle: flat-agent-society
//!
//! 树是员工的，不是某件活的。
//!
//! 以前树按任务号切：同一个任务里的规划、生成、评审挤在一棵树里，而任务号做完
//! 这一轮就作废，树跟着作废——聊天那一路更糟，把它当成一个随调用变化的名字。三
//! 个后果都不报错：
//!
//! - 同一件活里的两位看到同一份工作区：一位读到的是另一位刚写下的半成品，而无
//!   论它读到的是谁写的，系统上都读成「同一个任务的工作树」。
//! - 任务结束，树跟着回收：这一位下一回合回来，文件已经在回收站里了。
//! - 树是游离头（`--detach`）：在上面提交的活不属于任何分支，读的人说不出它落在
//!   哪棵树上，而这一位自己也找不回刚提过的那个提交。
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
    for agent in [
        "squad:DSC-1A2B-generator",
        "kai-a9ab11fc",
        "kai.a9ab11fc",
        ".hidden",
        &"x".repeat(200),
    ] {
        let lane = cog_extension::workdir::worktree_for_agent(agent)
            .unwrap_or_else(|| panic!("{agent} 是一位报了名号的执行者，必须有一条车道"));
        assert_eq!(
            lane,
            cog_core::worktree_lane(agent),
            "路由与系统前缀算出了两条车道：一位员工会有两棵树"
        );
    }
    assert!(
        cog_extension::workdir::worktree_for_agent("  ").is_none(),
        "没报名号的请求没有车道，只能按它带来的任务号退回去"
    );

    // 运行时那一侧读的就是这一条派生：任务号不再参与选树。
    let runtime = source("crates/cog-agent/src/agent_kernel/runtime.rs");
    let lane_fn = rust_function_body(&runtime, "workspace_lane")
        .expect("workspace_lane 还在：它是系统前缀里那棵树的唯一来源");
    assert!(
        lane_fn.contains("cog_core::worktree_lane"),
        "系统前缀里的车道不再从那一处派生：\n{lane_fn}"
    );
    assert!(
        !lane_fn.contains("run_task_id"),
        "车道又由任务号决定了：同一件活换了个人接就换一棵树，而一件活做完那棵树\
         还在——树该跟着人走\n{lane_fn}"
    );
}

/// 请求里的执行者决定树，任务号只做归属。
#[test]
fn the_executor_routes_by_the_employee_before_the_task() {
    let server = source("crates/cog-extension/src/command_server.rs");
    let resolve = rust_function_body(&server, "resolve_workdir")
        .expect("resolve_workdir 还在：它是请求落到哪棵树的唯一入口");
    assert!(
        resolve.contains("worktree_for_agent"),
        "执行器不再按执行者选树：\n{resolve}"
    );
    let by_employee = resolve.find("worktree_for_agent").expect("checked above");
    let by_task = resolve.find("task_id").expect("任务号仍然要读，作为退路");
    assert!(
        by_employee < by_task,
        "执行者必须排在任务号前面：反过来，同一件活里的几位又挤回一棵树\n{resolve}"
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
