//! 界面上那段对话不是一次补全，而是系统唯一的那条智能体循环上的一轮。
//!
//! 属主 2026-11-04 定的能力口径有两层，缺一层都不是这个能力：
//!
//! - **循环只有一条**：工具守卫、预算、token 记账、总线事件、清点都只在智能体内核
//!   里发生一次。网关里没有第二条循环，只有把这一轮交给装配层填进来的句柄——空着
//!   的时候界面读到的是一句「还没接上」，不是一条自作主张的旁路。
//! - **员工是自由的**：它手上有什么工具由**这个进程装配了什么**决定（有没有消息
//!   队列、有没有沙盒、有没有出门的客户端），不在这里另列一张短表裁剪。会话就是
//!   它的生命周期，名字与 id 由会话纯函数推出，跨轮、跨重启不变——实例可换，
//!   员工不换，账上也不能出现 unknown。
//!
//! 所以这里判：网关里没有循环只有交接；控制面五只装在唯一那条循环上；工具表来自
//! 进程；每一轮的 agent_id 是这段会话的员工；句柄在插件起来前发布、起来后填上；
//! 系统自己那三本账（变更、原则、告警）它读得到，而且只读。

use crate::common::{fsio, root};

const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";
const BOOT: &str = "crates/cogneva/src/lib.rs";

fn source(path: &str) -> String {
    fsio::read(&root::repo_root().join(path))
}

#[test]
fn the_gateway_hands_the_turn_over_instead_of_running_one() {
    let text = source(CHAT);

    // 交接点在，而且只有这一个入口：跑到这里说明这一轮真的交给了那条循环。
    assert!(
        text.contains("state.conversation.get()"),
        "网关不再从装配层填的句柄里取循环：它要么没有循环可用，要么自己另起了一条"
    );
    assert!(
        text.contains(".turn("),
        "这一轮没有交给 `ConversationTurn`：界面上的对话不在系统那条循环上跑"
    );

    // 网关里不该再有循环的形状。两条路径并存时，屏幕上分不出这一句是哪条跑出来的，
    // 而只有一条会过守卫、记账、清点。
    for shape in [
        "TurnScope",
        "TOOL_ROUNDS",
        "round_options.tools",
        "Message::ToolResult",
        "for round in 0..",
    ] {
        assert!(
            !text.contains(shape),
            "网关里还有 `{shape}`：这就是第二条执行路径。循环只有一条，在智能体内核里"
        );
    }
}

#[test]
fn the_control_plane_tools_ride_on_the_system_s_own_loop() {
    let text = source(ASSEMBLY);

    assert!(
        text.contains("cog_agent::AgentRuntime::new"),
        "装配层没有用系统的那条循环：它得自己拼一条，而那正是旁路"
    );
    assert!(
        text.contains("run_conversation"),
        "装配层取到循环后没有跑对话那一轮：工具与历史都不会经过守卫与记账"
    );
    assert!(
        text.contains("with_tools(tools)"),
        "工具集没有装到循环上：定义了却从不传出去的工具集与没有工具是同一种能力"
    );

    for name in [
        "dispatch_goal",
        "list_tasks",
        "get_task",
        "retry_task",
        "cancel_task",
    ] {
        assert!(
            text.contains(&format!("\"{name}\"")),
            "缺 {name}：这段对话读得到系统或改得动系统的能力缺了一半"
        );
    }

    // 身份与归属：按人记账，而且这个人就是这位员工本人。
    assert!(
        text.contains("with_call_identity"),
        "这一轮没有声明调用身份：界面花的钱会落在没有人的那一格"
    );
    assert!(
        text.contains("role: employee.id.clone()"),
        "这一轮的角色不是这位员工自己：账上会读到一个共同的名字，\
         同一个人开两个会话的两笔钱在账上就分不开"
    );
    assert!(
        !text.contains("with_actor_label"),
        "循环上另外钉了一个 actor：它会盖掉这位员工的名字，账上、名册上、\
         日志里读到的就不是同一个名字了"
    );
}

#[test]
fn the_slot_is_published_empty_before_plugins_start_and_filled_after() {
    let text = source(BOOT);

    let publish = text
        .find("ctx.publish(conversation.clone())")
        .expect("装配层没有把会话句柄发布出去：网关取不到，界面永远报「还没接上」");
    let init = text
        .find("plugin_runner.init_all(&ctx)")
        .expect("找不到插件初始化点，这条判据失去锚");
    let fill = text
        .find("PlatformConversation::new(")
        .expect("装配层没有在插件起来后把循环填进句柄：界面永远报「还没接上」");

    assert!(
        publish < init,
        "句柄必须在插件起来前发布：网关在 `init_all` 里就要取它"
    );
    assert!(
        init < fill,
        "填句柄必须在插件起来后：它要的编排器由插件在 `init` 里发布"
    );
}

/// 界面上派活的那一道准入，与 HTTP 提交面是同一条。
///
/// 少这一道，同一个人今天在提交面上已经提交不了了，在聊天框里却还能派——两条
/// 路要的是同一份钱，账上看得出来，准入上却看不见。估值也必须读同一条规则：
/// 各写一条除法，两处对同一句话给出不同的数，被挡住的那一侧就说不清为什么。
#[test]
fn a_dispatch_from_the_chat_asks_the_same_quota_as_the_submit_face() {
    let text = source(ASSEMBLY);

    assert!(
        text.contains("HierarchyManager"),
        "装配层没有拿配额管理器：界面上派活是一道没人看的门"
    );
    let check = text
        .find("admission_refusal(")
        .expect("装配层没有在派活前过准入");
    let submit = text
        .find("submit_goal_auto(")
        .expect("装配层不再直接提交目标，这条判据失去锚");
    assert!(
        check < submit,
        "准入必须挡在提交之前：放在后面就只是先派出去再问一句能不能派"
    );
    assert!(
        text.contains("cog_gateway::tasks::estimated_goal_tokens"),
        "估值不是那条共享规则：两处各写一条除法，同一句话会得到两个数"
    );
    // 拒绝同样要落账：提交面写的那一格，界面这一侧也写。少这一条，同一条规则
    // 在两个入口上一个留痕一个不留。
    assert!(
        text.contains("AuditKind::QuotaEnforcement"),
        "界面上的拒绝没有写进审计链：账上只看得见「今天有人没派活」"
    );
    assert!(
        text.contains("quota.dispatch_rejected"),
        "拒绝的动作名不是与提交面同一族（`quota.*`），读账的人会把两处当成两件事"
    );

    let boot = source(BOOT);
    assert!(
        boot.contains("consume_service::<dyn cog_core::HierarchyManager>()"),
        "装配根没有把配额管理器交给这段对话：准入拿不到管理器，就只会放行"
    );
    assert!(
        boot.contains("consume_service::<dyn cog_core::AuditStream>()"),
        "装配根没有把审计流交给这段对话：拒绝会静默丢掉"
    );
}

#[test]
fn the_one_tool_that_changes_the_system_is_dispatch() {
    let text = source(ASSEMBLY);
    assert!(
        text.contains("\"dispatch_goal\""),
        "没有派活工具：这段对话看得见系统的账，却改不动它——用户说「去做」时它只能回答"
    );
    // 派活那一条必须点名目标号是唯一权威：模型引用的 id 只能来自工具返回。
    assert!(
        text.contains("照抄它"),
        "派活的返回里没有说「id 照抄」，模型就会在回复里自己编一个目标号"
    );
}

/// 工具表来自这个进程，不是这里另列的一张短表。
///
/// 裁剪成「只读台账 ＋ 派活」看起来更安全，实际是把一个数字员工做成一个按钮：
/// 同一个进程里 worker 池有 `send_to_agent`、有沙盒、有出门的客户端，界面上这位
/// 没有——它就没法像同事那样找人、查账、看告警，只能说自己看见的那五个动作。
/// 另写一张短表还有第二种坏法：在没有沙盒的进程里假装没有执行能力，在有沙盒的
/// 进程里又把已经装好的能力藏起来。
#[test]
fn the_turn_carries_the_process_tool_table_instead_of_a_short_one() {
    let assembly = source(ASSEMBLY);
    assert!(
        assembly.contains("shared.list()"),
        "工具表没有从进程那一份复制过来：界面上这位与同事拿的不是同一套工具"
    );
    assert!(
        !assembly.contains("restricted_to"),
        "这里在按名单裁剪工具：手上的能力由进程装配决定，不由这一段代码决定"
    );

    let boot = source(BOOT);
    assert!(
        boot.contains("consume::<cog_agent::ToolRegistry>()"),
        "装配根没有把进程的工具表交给这段对话：它只能拿到自己编的那几只"
    );
}

/// 每一轮都是「这段会话的员工」在跑，不是一个叫 unknown 的匿名进程。
///
/// 身份决定三件事同时成立：名册上认得出它、消息上找得到它、账上分得开它。
/// 每轮一个新 id（或干脆不设 id）会让这三件事一起失效，而屏幕上看不出差别。
#[test]
fn a_turn_runs_as_the_employee_of_its_session() {
    let text = source(ASSEMBLY);
    assert!(
        text.contains("employee_for(&request.session_id)"),
        "这一轮没有按会话取员工：同一个会话的两轮在账上会变成两个人"
    );
    assert!(
        text.contains("agent_id: employee.id.clone()"),
        "运行配置里的 agent_id 不是这个员工：名册、消息、账三处都会落到别处"
    );
    assert!(
        text.contains("pub fn employee_for(session_id: &str) -> cog_agent::identity::AgentName"),
        "员工名字不是从会话推出的纯函数：重启一次就换人，名册上的历史对不上"
    );
}

/// 被挡下的派活记在这个员工名下，不是记在「谁在用这个界面」名下。
///
/// 同一个人开两个会话，两笔拒绝是两个人做的；只记 owner 时它们合成一笔，
/// 而账上本来要回答的是「哪个员工在做这件事」。
#[test]
fn a_refused_dispatch_is_booked_under_the_employee() {
    let text = source(ASSEMBLY);
    assert!(
        text.contains("record_refusal(audit.as_ref(), &actor, &owner, &refusal)"),
        "拒绝的 actor 不是那个员工：账上只剩「有人被挡过」"
    );
}

/// 它是一段对话里的员工：说话的时候在名册上，说完下线；同事发来的话开跑前读到。
///
/// 两半缺一不可。只有名册没有收件箱，别人找得到地址、投进去没人读；只有收件箱
/// 没有名册，消息能收，可谁也不知道该往哪投。
#[test]
fn the_employee_is_on_the_roster_and_listens_before_it_speaks() {
    let boot = source(BOOT);
    assert!(
        boot.contains("consume_service::<dyn cog_core::AgentRegistry>()"),
        "装配根没有把名册交给这段对话：同事在名单上找不到它"
    );
    assert!(
        boot.contains("consume_service::<dyn cog_core::MessageBackend>()"),
        "装配根没有把收件箱交给这段对话：发给它的话没人读"
    );

    let text = source(ASSEMBLY);
    assert!(
        text.contains("EmployeeLease::take"),
        "说话的时候没有把自己登记进名册：这一轮里它不可达"
    );
    assert!(
        text.contains("lease.release()"),
        "说完没有注销：名册上留一条过期记录，同事会以为它还醒着"
    );
    let drain = text
        .find("drain_inbox(backend")
        .expect("开跑前没有读收件箱：同事的话要等到下一轮才看得见");
    let run = text
        .find("run_conversation(")
        .expect("找不到跑对话的那一行，这条判据失去锚");
    assert!(
        drain < run,
        "收件箱要在开跑之前读：跑完再读，这一轮的上下文里就没有同事的话"
    );
}

/// 它读得到系统自己的三本账：变更、原则、告警。
///
/// 「这件事落地了没有」的答案写在账上，不在谁的印象里。这三只工具读的是本进程
/// 里已经发布的服务——与网关上那些面板读的是同一批行，不是两次转发、也不是第二份
/// 拷贝。而账缺席时必须说出来：回一个空列表就是把「读不到」说成「一条都不欠」，
/// 这两个读数正好相反。
#[test]
fn the_employee_reads_the_three_accounts_the_system_keeps() {
    let text = source(ASSEMBLY);
    for name in [
        "list_changes",
        "get_change",
        "list_principles",
        "active_alerts",
    ] {
        assert!(
            text.contains(&format!("\"{name}\"")),
            "缺 {name}：它读不到系统自己的账，只能凭印象回答「那件事落地了没有」"
        );
    }
    assert!(
        text.contains("ledger_absent("),
        "账缺席时没有一句话把它说出来：空列表会被读成「一条都不欠」"
    );
    // 读账的工具不许写账：判一条需求做完了没有，只能由看树的那一侧写。工具层
    // 出现其中一个写入口，读账的人就成了给自己发完成证明的人。
    for writer in [
        "apply_measurement",
        "record_delivery",
        "report_refusals",
        "record_human_only",
        "record_dispatch_state",
    ] {
        assert!(
            !text.contains(writer),
            "读账的这一层出现了写入口 `{writer}`：完成由看树的那一侧判定，不由读账的人写"
        );
    }

    let boot = source(BOOT);
    for service in ["ChangeLedgerStore", "PrincipleLedgerStore", "AlertStore"] {
        assert!(
            boot.contains(&format!("consume_service::<dyn cog_core::{service}>()")),
            "装配根没有把 {service} 交给这段对话：这一面在界面上就是瞎的"
        );
    }
}

/// 界面上的员工没有轮数天花板：一轮什么时候结束，由模型自己不再要工具来定。
///
/// 「界面上那位只许想六轮」看起来只是省一点钱，实际是让它比任何同事都早被要求
/// 「别查了，先答」——而它答不出来「这件事落地了没有」，一半原因就在这里。会话
/// 是人一句、它一句，什么时候查够了该由它自己知道，不由一份常量代答。
#[test]
fn the_turn_carries_no_iteration_ceiling_of_its_own() {
    let text = source(ASSEMBLY);
    assert!(
        text.contains("max_iterations: u32::MAX"),
        "这一段对话又给轮数写了一个数：界面上这位会比同事更早被要求「别查了，先答」"
    );
    assert!(
        !text.contains("CHAT_ITERATIONS"),
        "又长回一个只属于这个界面的轮数常量"
    );

    let chat = source(CHAT);
    assert!(
        !chat.contains("工具只有两类"),
        "系统提示词又把工具写成一张闭集：手上有什么由进程装配决定，不由提示词裁剪"
    );
    assert!(
        !chat.contains("按此口径回答"),
        "系统提示词又硬编码了一套身份口径：员工是谁不该由一段预设写完"
    );
}

/// 员工申请改自己：进的是账的入口，不是账的状态；尺子的名单复用系统那一份。
///
/// 「自己改自己」与「自己宣布自己改好了」是两件事，这一条钉的正是那道缝：申请
/// 走的是 `declare`——账的读入队列，写进去的是一条没有状态的行；判它落没落地的
/// 只有看树的那一侧。这一层要是出现了 `apply_measurement` 之类的入口，提的人
/// 就成了给自己发完成证明的人，账上「落地了」这四个字也就不再是别人的读数。
///
/// 另一半是那把尺子：拒绝改判据载体用的必须是全系统一直在用的那份名单。另抄
/// 一份的话，两份迟早说的不是同一件事，而这份名单正是决定「什么算尺子」的那一处。
#[test]
fn the_employee_can_ask_to_change_the_system_but_not_to_mark_it_done() {
    let assembly = source(ASSEMBLY);
    assert!(
        assembly.contains("self_change::tool("),
        "界面上的员工没有申请改自己的入口：它连「我想把这里改成那样」都说不出来"
    );

    let module = source("crates/cogneva/src/self_change.rs");
    assert!(
        module.contains("store.declare("),
        "申请没有进账的读入队列：那它就哪儿也没去"
    );
    for writer in [
        "apply_measurement",
        "record_delivery",
        "record_dispatch_state",
        "record_human_only",
    ] {
        assert!(
            !module.contains(writer),
            "自改这一层出现了写完成状态的入口 `{writer}`：提的人能宣布自己改完了"
        );
    }
    assert!(
        module.contains("criteria_face::is_carrier_path"),
        "尺子的名单没有复用系统那一份：抄出来的第二份迟早与第一份说的不是同一件事"
    );
}
