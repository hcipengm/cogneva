//! E18：对话里问得到「这段对话的命令跑在什么样的边界里」，越界的那一步被拒，
//! 拒绝时说得可写范围是什么。
//!
//! 判的不是「有没有一个叫边界的接口」，是四件看得见的事：
//!
//! 1. **四行、四种状态词**：可写路径 / 网络 / 资源 / 超时各一行，`enforced`、
//!    `declared`、`none`、`unknown` 分得开——「部署声明了」不能读成「这一层会拒」，
//!    「量不到」也不能读成「量到就是不设界」；
//! 2. **走出这棵树的路径被拒，且说得出范围**：拒绝的消息里有那棵树的根，没有替
//!    这条路径悄悄改道；
//! 3. **进程内执行如实说自己不隔离**：没有容器的部署不该读起来和执行器 Pod 一样；
//! 4. **装配层不替执行器说话**：边界由跑命令的那一层自报，没有接执行器时明说
//!    说不清，而不是拼一份看起来安全的描述。

use std::sync::Arc;

use crate::common::routes::read_repo;
use cog_core::{
    BoundaryLine, SFError, SFResult, SandboxBackend, SandboxBoundary, SandboxRequest, SandboxResult,
};
use cog_gateway::chat::ConversationTurn;
use cogneva::conversation::{Ledgers, PlatformConversation};

fn line<'a>(reading: &'a SandboxBoundary, dimension: &str) -> &'a BoundaryLine {
    reading
        .lines
        .iter()
        .find(|line| line.dimension == dimension)
        .unwrap_or_else(|| {
            panic!(
                "边界里没有「{dimension}」这一行：{:?}",
                reading
                    .lines
                    .iter()
                    .map(|line| line.dimension.as_str())
                    .collect::<Vec<_>>()
            )
        })
}

/// 走出这棵树的路径被拒，拒绝里说得可写范围。
///
/// 这一条是「越界说明」的判据本身：把那一步改回原样交给文件系统（`anchor_path`），
/// 它当场红——那时 `../../etc/passwd` 会被安静地落到宿主上，而界面上四行仍写着
/// `enforced`。
#[test]
fn a_path_out_of_the_tree_is_refused_with_the_root_named() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("workspaces").join("task-1");
    std::fs::create_dir_all(&root).expect("worktree root");

    // 树里的相对路径挂在这棵树上，绝对路径保持它自己的意思。
    let inside = cog_extension::workdir::anchor_inside(&root, "src/main.rs").expect("inside");
    assert_eq!(inside, root.join("src").join("main.rs"));
    // 绕一圈仍落在树里的路径不算越界：`..` 只当路径段，不当作罪证。
    let wandering =
        cog_extension::workdir::anchor_inside(&root, "src/../Cargo.toml").expect("inside");
    assert_eq!(wandering, root.join("Cargo.toml"));

    for escape in ["../task-2/secret.txt", "../../etc/passwd", "/etc/passwd"] {
        let refusal = match cog_extension::workdir::anchor_inside(&root, escape) {
            Ok(anchored) => panic!("{escape} 被放行到了 {}", anchored.display()),
            Err(reason) => reason,
        };
        assert!(
            refusal.contains(&root.display().to_string()),
            "拒绝 {escape} 时没有说出可写范围：{refusal}"
        );
        assert!(
            refusal.contains(escape),
            "拒绝没有说清是哪条路径越界：{refusal}"
        );
    }
}

/// 执行器自报四行；「量不到」与「量到就是不设界」是两个状态词。
#[test]
fn the_executor_answers_four_lines_and_none_is_not_unknown() {
    let reading = cog_extension::command_server::boundary_reading(None);
    assert_eq!(reading.kind, "executor");
    assert!(reading.isolated, "执行器是隔着一条进程边界跑的");
    let dimensions: Vec<&str> = reading.lines.iter().map(|l| l.dimension.as_str()).collect();
    assert_eq!(dimensions, vec!["可写路径", "网络", "资源", "超时"]);

    // 没有车道就没有可写的界：这一层真的不设界。
    assert_eq!(line(&reading, "可写路径").state, "none");

    // 网络这一层读不到策略本体：声明了就是 `declared`，没声明是 `unknown`，
    // 两者都不是 `none`——把答不出来写成「不设界」，会让人以为出口是敞的。
    let network = line(&reading, "网络");
    assert_ne!(network.state, "none");
    let declared = std::env::var("SANDBOX_EGRESS_POLICY")
        .map(|name| !name.trim().is_empty())
        .unwrap_or(false);
    assert_eq!(
        network.state,
        if declared { "declared" } else { "unknown" },
        "网络这一行没有跟着部署声明走：{}",
        network.detail
    );

    // 资源与超时要么量到了，要么说量不到；执行器不会「不设界」。
    for dimension in ["资源", "超时"] {
        let state = &line(&reading, dimension).state;
        assert!(
            state == "enforced" || state == "unknown",
            "「{dimension}」读成了 {state}：执行器里没有这一格"
        );
    }
}

/// 进程内执行如实说自己不隔离。
#[tokio::test]
async fn a_local_backend_says_it_is_not_isolated() {
    let reading = cog_extension::runtime::LocalExecutor::new()
        .boundary()
        .await
        .expect("进程内执行答得出自己的边界");
    assert_eq!(reading.kind, "local");
    assert!(
        !reading.isolated,
        "进程内执行被写成了隔离的：这会让默认部署读起来和执行器 Pod 一样"
    );
    let dimensions: Vec<&str> = reading.lines.iter().map(|l| l.dimension.as_str()).collect();
    assert_eq!(dimensions, vec!["可写路径", "网络", "资源", "超时"]);
    for dimension in ["可写路径", "网络", "资源"] {
        assert_eq!(
            line(&reading, dimension).state,
            "none",
            "「{dimension}」在进程内执行里没有界，不能报成别的"
        );
    }
    // 沉默窗口不在进程边界上，但这一步是这条命令自己的：要么量到了，要么说没量到。
    let silence = &line(&reading, "超时").state;
    assert!(silence == "enforced" || silence == "unknown", "{silence}");
}

/// 一个只答边界的后端：回话由它给出，装配层只负责递过来。
struct Fixed(SandboxBoundary);

#[async_trait::async_trait]
impl SandboxBackend for Fixed {
    async fn execute(&self, _req: &SandboxRequest) -> SFResult<SandboxResult> {
        Err(SFError::Agent("这条测试只问边界，不跑命令".into()))
    }

    async fn boundary(&self) -> SFResult<SandboxBoundary> {
        Ok(self.0.clone())
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(SFError::Agent("不编译 wasm".into()))
    }
}

fn conversation(sandbox: Option<Arc<dyn SandboxBackend>>) -> PlatformConversation {
    use crate::common::orchestrator::control;

    PlatformConversation::new(
        control("test-workspace"),
        None,
        None,
        None,
        None,
        None,
        None,
        Ledgers::default(),
        None,
        None,
        None,
        None,
        None,
        sandbox,
        None,
    )
}

/// 装配层不替执行器说话：后端说什么就递什么，没有后端就明说。
#[tokio::test]
async fn the_conversation_hands_the_boundary_the_backend_reports() {
    let reported = SandboxBoundary {
        kind: "executor".into(),
        isolated: true,
        workdir: Some("/opt/cogneva/sandbox/workspaces/task-1".into()),
        lines: vec![
            BoundaryLine {
                dimension: "可写路径".into(),
                state: "enforced".into(),
                detail: "/opt/cogneva/sandbox/workspaces/task-1".into(),
            },
            BoundaryLine {
                dimension: "网络".into(),
                state: "declared".into(),
                detail: "cogneva-sandbox-executor-egress".into(),
            },
        ],
    };
    let handed = conversation(Some(Arc::new(Fixed(reported.clone()))))
        .sandbox_boundary("alice", "session-1")
        .await
        .expect("后端答得出边界");
    assert_eq!(
        handed, reported,
        "装配层改了后端的话：读的人看到的就不再是执行器量到的那份"
    );

    let error = conversation(None)
        .sandbox_boundary("alice", "session-1")
        .await
        .expect_err("没有执行器时说不清命令跑在哪里");
    assert!(
        error.contains("执行器"),
        "没有执行器时的回话没有说清缺的是什么：{error}"
    );
}

/// 页面上看得见这是哪一类沙箱，以及四行状态各是哪一句话。
///
/// 读数到了界面上被写成一句「已隔离」，就等于没有这一层：四种状态词要能在
/// 面板里各自成句，`declared` 不能被显示成 `enforced`。
#[test]
fn the_page_shows_which_kind_of_sandbox_it_is() {
    let pane = read_repo("web/src/conversation/ConversationPane.tsx");
    for wanted in [
        "chat_sandbox_boundary",
        "boundaryWord",
        "管住",
        "部署声明",
        "不设界",
        "答不出",
    ] {
        assert!(
            pane.contains(wanted),
            "对话页里没有「{wanted}」：边界面板只剩一句笼统的「沙箱」"
        );
    }

    let transport = read_repo("web/src/transport/websocket.ts");
    assert!(
        transport.contains("chat_sandbox_boundary"),
        "传输层没有这条要读数的消息"
    );

    let protocol = read_repo("crates/cog-gateway/src/websocket_protocol.rs");
    for wanted in ["chat_sandbox_boundary", "ChatSandboxBoundary"] {
        assert!(
            protocol.contains(wanted),
            "协议里没有「{wanted}」：界面问出去的话没有人接"
        );
    }

    let socket = read_repo("crates/cog-gateway/src/websocket.rs");
    assert!(
        socket.contains("sandbox_boundary"),
        "套接字上没有把这条请求接到对话上"
    );

    // 部署里那一条策略名要与执行器读的变量对上：面板上「部署声明」那一行说
    // 的就是这个名字；改了一处没改另一处，它说的就是另一条策略。
    const EGRESS_POLICY: &str = "cogneva-sandbox-executor-egress";
    let chart = read_repo("deploy/helm/cogneva/templates/sandbox.yaml");
    assert!(
        chart.contains("SANDBOX_EGRESS_POLICY"),
        "chart 没有声明执行器该读哪条网络策略：网络那一行会永远读成 unknown"
    );
    assert!(
        chart.contains(EGRESS_POLICY),
        "执行器读的策略名不是部署里那条：面板上的「部署声明」说的是另一条策略"
    );
    let policy = read_repo("deploy/helm/cogneva/templates/network-policy.yaml");
    assert!(
        policy.contains(EGRESS_POLICY),
        "chart 里没有叫 {EGRESS_POLICY} 的网络策略：执行器报的是一个不存在的名字"
    );
}
