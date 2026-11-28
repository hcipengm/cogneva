//! 链路里「编译与测」那一格要答「刚才那一推慢在哪」，不只要答「现在槽里有没有人」。
//!
//! 槽位那一面回答的是另一个问题：一跑两小时的门跑完之后，槽位读数恢复成「空着」，
//! 从那张表上再也看不出时间花在哪一格。记录由门自己写进同一个槽目录（`runs/`），
//! 网关在同一条读法里读出来，页面在已有的那一格里展开——不新开通道，也不新开一面。
//!
//! 这条判据钉五件事，任何一件改了而别处没跟，就会长成「页面在、服务端没这一格」或者
//! 「记录在写、没有一处读」：
//!
//! - 服务端声明了 `runs`，并且把「没收尾」与「跑完了」分开；
//! - 页面读的是服务端判好的那一份，不自己再判一次红绿；
//! - 写记录的那一侧（门的 lib 脚本）与读记录的那一侧（网关）用同一组后缀名——
//!   两边各写一个字符串，改名只改一处，记录就静默不进城；
//! - 记录对得上「读者是另一个 uid」：权限不信 umask，读不动与空记录分得开；
//! - 流程里的具体步骤从记录里取，页面不自己写一份清单：钩子增删、判据增减，
//!   页面下一次轮询就是新的，不必为流程改一次前端。
//!
//! Rust 与前端源码按去注释之后读（`strip_comments`）：改一句说明不该让它变红。脚本按
//! 原样读，断言钉在带实参的调用上，理由见 `script`。

use std::path::{Path, PathBuf};

use crate::common;
use crate::common::routes::region_between;

fn root() -> PathBuf {
    common::root::repo_root()
}

fn source(root: &Path, rel: &str) -> String {
    common::routes::strip_comments(&common::fsio::read(&root.join(rel)))
}

/// 脚本按原样读：那套注释剥离器认的是 JavaScript 的规则，`https://` 在它眼里是一段
/// 注释的起点，一段被吞掉的脚本会让判据读成「没有这一句」。脚本里的断言因此钉在调用
/// 的形状上（带实参的那一行），一句说明性注释不会长成那个样子。
fn script(root: &Path, rel: &str) -> String {
    common::fsio::read(&root.join(rel))
}

const VIEW: &str = "web/src/views/DeliveryView.tsx";
const CLIENT: &str = "web/src/api/client.ts";
const GATEWAY: &str = "crates/cog-gateway/src/buildgate.rs";
const WRITER_LIB: &str = "deploy/scripts/lib/gate-run-record.sh";
const GATE: &str = "deploy/scripts/check-before-push.sh";

/// 服务端那一格：「没收尾」是一个独立的读数，不是「没有跑」也不是「跑完了」。
#[test]
fn the_gate_report_carries_the_runs_and_keeps_unfinished_apart() {
    let report = source(&root(), GATEWAY);
    assert!(
        report.contains("pub runs: Vec<RunReading>"),
        "`GateReport` 不带最近几跑：页面拿不到「刚才那一推花了多少」，只能回去看终端"
    );
    assert!(
        report.contains("pub runs_why: Option<String>"),
        "读不动与没有记录要分得开：前者要去改挂载，后者只是还没人写过"
    );
    assert!(
        report.contains("pub finished: bool"),
        "一跑有没有收尾必须是一个读数：没收尾的读成「跑完了」，读者会去追一个不存在的判词"
    );
    assert!(
        report.contains("pub last_write_secs_ago: Option<u64>"),
        "没收尾的那一跑要说「停了多久」：没有这一格，跑着与被杀在读面上同形"
    );
    assert!(
        report.contains("pub red: usize") && report.contains("pub unjudged: usize"),
        "红与没判各自数出来：一格没进构建槽就判红，是把机器的忙碌栽给这一笔改动"
    );
}

/// 页面读服务端判好的那一份：自己再挑一次「最新一跑」就是第二个判据。
#[test]
fn the_chain_cell_reads_the_run_the_report_judged() {
    let text = source(&root(), VIEW);
    let body = region_between(&text, "function gateNode(", "function mainlineJob(");
    assert!(
        body.contains("lastRun(gate, 'gate')"),
        "`gateNode` 不按阶段取整门那一跑：一段几秒的提交前钩子比两小时的整门更新，\
         取 `runs[0]` 会让整门从页面上消失"
    );
    assert!(
        body.contains("run.finished"),
        "`gateNode` 不读 `finished`：没收尾的那一跑会被当成一次已经结束的判定"
    );
    assert!(
        body.contains("run.red > 0") && body.contains("step.verdict === 'RED'"),
        "`gateNode` 不点名红的那几格：读者要的是「哪一步红」，不是「有几步红」"
    );
    assert!(
        body.contains("run.total_secs"),
        "`gateNode` 不报合计秒数：这一格答不出「这一推花了多久」"
    );
}

/// 那一格展开的表：每一格都带着秒数与结局，长的在前——与终端那张成本表同一读法。
#[test]
fn the_run_panel_shows_each_step_with_its_seconds_and_verdict() {
    let text = source(&root(), VIEW);
    let body = region_between(&text, "function GateRunPanel(", "function formatAge(");
    assert!(
        body.contains("report.runs"),
        "`GateRunPanel` 不读 `report.runs`"
    );
    assert!(
        body.contains("run.steps"),
        "`GateRunPanel` 不列每一格：只剩一个合计数，砍起预算来只能猜"
    );
    assert!(
        body.contains("step.seconds") && body.contains("step.verdict"),
        "表里缺秒数或结局：红的与零成本的一格在表上同形"
    );
    assert!(
        body.contains("b.seconds - a.seconds"),
        "没按秒数从长到短排：读的人第一眼该看见最贵的那一格"
    );
}

/// 前端与服务端两个类型定义必须一起动：少一个字段，页面上那格是空白，而空白读起来
/// 像「本来就没有」。
#[test]
fn the_client_declares_the_fields_the_report_writes() {
    let client = source(&root(), CLIENT);
    for field in ["runs: BuildGateRun[]", "runs_why?: string"] {
        assert!(
            client.contains(field),
            "`BuildGateReport` 少声明了 `{field}`：服务端写了，页面读不到"
        );
    }
    for field in [
        "finished: boolean",
        "red: number",
        "unjudged: number",
        "stage: string",
        "steps: BuildGateStep[]",
    ] {
        assert!(client.contains(field), "`BuildGateRun` 少声明了 `{field}`");
    }
    assert!(
        client.contains("verdict: string") && client.contains("seconds: number"),
        "`BuildGateStep` 缺秒数或结局"
    );
}

/// 写记录的那一侧与读记录的那一侧，后缀名必须逐字相同。
///
/// 这两处各写一个字符串，改一处不会报错：记录照常写、读者照常读，只是读到的永远是
/// 「还没有这一跑」。门的脚本里没有任何地方会因为这个而不绿。
#[test]
fn the_gate_writer_and_the_reader_agree_on_the_suffixes() {
    let root = root();
    let writer = script(&root, WRITER_LIB);
    let reader = source(&root, GATEWAY);
    assert!(
        writer.contains("\".segments\"") || writer.contains(".segments"),
        "写的那一侧不再用 `.segments` 收尾"
    );
    assert!(
        writer.contains(".running"),
        "写的那一侧不再用 `.running` 标记没收尾"
    );
    // 阶段是文件名的一部分：写的人按闭集落名，读的人按同一组词切回来。两边各写
    // 一份清单，加一个阶段只改一边，那一阶段的记录就永远读不出来。
    assert!(
        writer.contains("precommit | gate | prepush"),
        "写的那一侧不再把阶段当闭集校验"
    );
    assert!(
        reader.contains(r#"["precommit", "gate", "prepush"]"#),
        "读的那一侧认的阶段与写的那一侧不是同一组"
    );
    let live = const_string_value(&reader, "RUN_LIVE_SUFFIX");
    let done = const_string_value(&reader, "RUN_DONE_SUFFIX");
    assert_eq!(live, ".running", "读的那一侧认的没收尾后缀变了：{live}");
    assert_eq!(done, ".segments", "读的那一侧认的收尾后缀变了：{done}");
    assert_eq!(live, ".running", "写与读的没收尾后缀不是同一个字符串");
}

/// 门脚本要把三件事都接上：开跑、每格落笔、收尾。少接一处，记录会一直是空的或者
/// 一直没有收尾——两种都长得像「门没跑」。
#[test]
fn the_gate_wires_the_record_into_both_the_table_and_the_run_file() {
    let gate = script(&root(), GATE);
    assert!(
        gate.contains("run_file=\"$(gate_run_begin"),
        "门不开始记录：槽目录的 runs/ 永远是空的"
    );
    assert!(
        gate.contains("gate_run_append \"${run_file}\""),
        "每格不落笔：记录里没有「花了多少秒」"
    );
    assert!(
        gate.contains("gate_run_finish \"${run_file}\""),
        "门不收尾：那一跑永远显示成「还没走完」"
    );
    assert!(
        gate.contains(". \"${here}/lib/gate-run-record.sh\""),
        "门没有把那组函数带进来：下面三处调用都会是 command not found"
    );
}

/// 记录的世界可读是硬要求：读的人（Pod 里 uid 1001）与写的人不是同一个。读不动
/// 必须是一个读数，不能读成「这一跑一格都没记」。
#[test]
fn the_record_is_readable_by_another_uid_and_unreadable_is_not_zero() {
    let root = root();
    let writer = script(&root, WRITER_LIB);
    assert!(
        writer.contains("chmod 0755"),
        "`runs/` 的权限跟着跑门的 shell 走了：一个 `umask 077` 的 shell 就把 Pod 挡在目录外"
    );
    assert!(
        writer.contains("chmod 0644"),
        "记录文件的权限跟着 umask 走了：读的人与写的人不是同一个 uid，0600 就是「文件在、读不动」"
    );
    let report = source(&root, GATEWAY);
    assert!(
        report.contains("pub read_error: Option<String>"),
        "读不动的记录被读成空表：与「这一跑一格都没记」在页面上同形"
    );
    let view = source(&root, VIEW);
    assert!(
        view.contains("run.read_error"),
        "页面上没有读不动的理由：那一格只剩空表"
    );
    assert!(
        view.contains("run?.read_error"),
        "`gateNode` 不把读不动判成 unknown：它会被当成一次已经结束的判定"
    );
    let client = source(&root, CLIENT);
    assert!(
        client.contains("read_error?: string"),
        "`BuildGateRun` 没声明 read_error：服务端写了，页面读不到"
    );
}

/// 链路上的一格对应一处真实读数：七个阶段按发生顺序排，每一格由它自己的记录回答。
///
/// 少一格就是从编辑到部署有一个环节在页面上不存在；多一格就是页面替某一步编了一个
/// 读数。镜像构建曾经排在「推到 main」前面——顺序错了，读者会把「还没推上去」当成
/// 「还没编完」。
#[test]
fn the_chain_lists_the_seven_steps_in_the_order_they_happen() {
    let text = source(&root(), VIEW);
    let body = region_between(&text, "function chainNodes(", "function lastRun(");
    let steps = [
        "editNode(line, lineError)",
        "commitNode(gate, gateError)",
        "gateNode(gate, gateError)",
        "prePushNode(gate, gateError)",
        "pushNode(line, lineError)",
        "buildNode(builds, workloads)",
        "deployNode(workloads, line, observability)",
    ];
    let mut previous = 0;
    for step in steps {
        let at = body
            .find(step)
            .unwrap_or_else(|| panic!("`chainNodes` 里没有 `{step}`：这一步在页面上不存在"));
        assert!(at > previous, "`{step}` 不在它该在的位置：链路按发生顺序排");
        previous = at;
    }
    assert!(
        text.contains("function StepLane("),
        "没有全局那一条：七步只能逐行读结论，看不出「卡在哪一步」"
    );
    let lane = region_between(&text, "function StepLane(", "function ChainPanel(");
    assert!(
        lane.contains("chainColor(node.state)") && lane.contains("node.stage"),
        "全局那一行不按结局上色或不带步骤名：七格长得一样"
    );
}

/// 流程里的具体步骤跟着记录走，不跟这一页走。
///
/// 一份钩子加进 `pre-commit.d/`、整门多一条判据，步骤清单就变了。页面若自己写一份
/// 清单，流程改了它不会跟着改，而它显示的仍是「上一版的流程」——看不出错，只看出
/// 少了几步。所以逐步记录只从 `run.steps` 取，页面只负责按记录的顺序铺开；标题里的
/// 步数也由节点数算出来，不写死。
#[test]
fn the_flow_steps_come_from_the_record_not_from_the_page() {
    let text = source(&root(), VIEW);
    let nodes = region_between(&text, "function chainNodes(", "function lastRun(");
    assert!(
        nodes.contains("steps: run.steps"),
        "`chainNodes` 不把记录里的那几格挂到节点上：页面上的步骤清单只能自己写死"
    );
    let flow = region_between(&text, "interface FlowStepRow", "function ChainPanel(");
    assert!(
        flow.contains("node.steps"),
        "`FlowSteps` 不读节点的逐步记录：流程增删了它不会变"
    );
    assert!(
        flow.contains("flowStepRows"),
        "`FlowSteps` 不按环节顺序铺平：逐步表与上面的链路对不上"
    );
    assert!(
        text.contains("{chain.length} 步"),
        "标题把步数写死：流程增减之后标题与链路对不上，而这也是「步骤可变」的读数之一"
    );
}

/// 一个 `const NAME: &str = "…"` 的字符串值。
fn const_string_value(text: &str, name: &str) -> String {
    let at = text
        .find(&format!("const {name}: &str = \""))
        .unwrap_or_else(|| panic!("源码里找不到 {name}"));
    let rest = &text[at + format!("const {name}: &str = \"").len()..];
    let close = rest
        .find('"')
        .unwrap_or_else(|| panic!("{name} 的字面量没有闭合"));
    rest[..close].to_string()
}
