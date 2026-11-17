//! E30：一个命令排在编译队里的时候，界面上看得见「在等执行槽」——排第几、等了多久，
//! 由执行器答，编排侧不自己排一条队再猜。
//!
//! 判三件事：
//!
//! 1. **队只在执行器那一侧**：命令的流还没开出来的时候，工具按节拍问一次执行器；
//!    问到了就把那句话交出去。不问的话，一次排队与一次正在编译在界面上同形——都只
//!    是一行「进行中」；
//! 2. **说不出来与队是空的不是一回事**：没有编译队的后端答「说不出」，而不是答一份
//!    空名单；工具读到「说不出」就不说话，不编一个位次出来；
//! 3. **界上看得见**：工具那一行收起时显示最近一句中途输出，于是排队与正在编译分得开。

use std::sync::Arc;
use std::time::Duration;

use cog_agent::tools::builtins::run_command;
use cog_agent::tools::{ToolScope, ToolUpdateSink};
use cog_agent::ToolRegistry;
use cog_core::{
    BuildLaneReading, CommandEvent, CommandEventStream, LaneWaiter, SFError, SFResult,
    SandboxBackend, SandboxRequest, SandboxResult, ToolRegistry as _,
};

/// 一个把「排队」做成真的的执行器：流要等一会儿才开出来，而它一直说得出自己排第几。
#[derive(Default)]
struct Queued {
    /// 流开出来那一刻这条命令归谁：测试用它证明问的是这一条。
    opened_for: Arc<std::sync::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl SandboxBackend for Queued {
    async fn execute(&self, _req: &SandboxRequest) -> SFResult<SandboxResult> {
        Err(SFError::Agent("this backend only streams".into()))
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(SFError::Agent(
            "this backend runs commands, not wasm".into(),
        ))
    }

    /// 命令还没开跑：这条命令正排在编译队的第 2 位。
    async fn lane(&self) -> SFResult<BuildLaneReading> {
        Ok(BuildLaneReading {
            capacity: Some(1),
            holding: Vec::new(),
            waiting: vec![LaneWaiter {
                place: 2,
                agent: "worker-3".into(),
                tool: "run_command".into(),
                waited_ms: 1_500,
            }],
            room: None,
        })
    }

    async fn execute_stream(&self, req: &SandboxRequest) -> SFResult<CommandEventStream> {
        self.opened_for.lock().unwrap().push(req.agent_id.clone());
        tokio::time::sleep(Duration::from_millis(900)).await;
        Ok(Box::pin(futures::stream::iter(vec![
            CommandEvent::Stdout {
                data: "built\n".into(),
            },
            CommandEvent::Exit { code: 0 },
        ])))
    }
}

fn scope() -> ToolScope {
    ToolScope {
        task_id: "task-1".into(),
        agent_id: "worker-3".into(),
    }
}

fn collecting() -> (ToolUpdateSink, Arc<std::sync::Mutex<Vec<String>>>) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink_seen = Arc::clone(&seen);
    let sink: ToolUpdateSink = Arc::new(move |chunk: &str| {
        sink_seen.lock().unwrap().push(chunk.to_string());
    });
    (sink, seen)
}

/// 命令还在队里没开跑的那段空档，工具问执行器一句，并把「在等执行槽、排第几」交出去。
///
/// 消融：把 `execute_stream` 前面的那个 `select!` 换回一次 `await`，这段空档就没有
/// 任何一句话——打开页面的那个人看到的是「进行中」，与一条正在编译的命令一样。
#[tokio::test]
async fn a_command_that_waits_for_a_build_slot_says_so_while_it_waits() {
    let backend: Arc<dyn SandboxBackend> = Arc::new(Queued::default());
    let registry = ToolRegistry::new().with_sandbox_backend(Arc::clone(&backend));
    registry.register(run_command());
    let (sink, seen) = collecting();

    registry
        .execute_scoped_streaming(
            "run_command",
            serde_json::json!({ "command": "cargo build" }),
            Some(&scope()),
            Some(&sink),
        )
        .await
        .expect("the command runs to its end");

    let said = seen.lock().unwrap().join("");
    assert!(
        said.contains("在等执行槽"),
        "排队的那段空档没有说给人听：{said:?}"
    );
    assert!(said.contains("排第 2 位"), "没说排第几：{said:?}");
}

/// 一个说不出自己有没有编译队的后端：默认答「说不出」——不是一份空名单。
struct Bare;

#[async_trait::async_trait]
impl SandboxBackend for Bare {
    async fn execute(&self, _req: &SandboxRequest) -> SFResult<SandboxResult> {
        Ok(SandboxResult {
            stdout: "ok\n".into(),
            exit_code: 0,
            ..Default::default()
        })
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(SFError::Agent(
            "this backend runs commands, not wasm".into(),
        ))
    }
}

/// 「说不出队」不等于「队是空的」：默认回答是一句说不出，工具读到它就不说话。
///
/// 消融：把默认实现改成回一份空名单，这条仍然通过——所以它同时判了下面那句
/// 「工具没有编一个位次出来」，两者合起来才是「量不到时不假装量到了」。
#[tokio::test]
async fn a_backend_without_a_lane_says_it_cannot_say_and_the_tool_stays_quiet() {
    let error = Bare
        .lane()
        .await
        .expect_err("a backend with no lane cannot answer");
    assert!(
        error.to_string().contains("说不出"),
        "说不出来就该说说不出来，而不是一份空名单：{error}"
    );

    let registry =
        ToolRegistry::new().with_sandbox_backend(Arc::new(Bare) as Arc<dyn SandboxBackend>);
    registry.register(run_command());
    let (sink, seen) = collecting();
    registry
        .execute_scoped_streaming(
            "run_command",
            serde_json::json!({ "command": "echo ok" }),
            Some(&scope()),
            Some(&sink),
        )
        .await
        .expect("the command runs to its end");
    let said = seen.lock().unwrap().join("");
    assert!(
        !said.contains("在等执行槽"),
        "读不到队的时候编了一个位次出来：{said:?}"
    );
}

/// 界上看得见：收起的那一行显示最近一句中途输出，于是「在等执行槽」不必展开就看得见。
///
/// 整句必须在行上那一段之外还读得到：行上只有一截（`latest.shown`），而读数不对劲
/// 时人要把整句抄走，所以注释面板里放的是原文（`latest.full`）。原生 `title=` 画在
/// 页面之外，选不中、复制不了——这条同时判「没有退回原生提示框」。
#[test]
fn the_collapsed_tool_row_shows_what_a_running_call_last_said() {
    let page = crate::common::routes::read_repo("web/src/conversation/ConversationPane.tsx");
    assert!(
        page.contains("function lastProgressLine(progress: string): LatestWords | null"),
        "页面没有从那一串中途输出里挑出最近一句"
    );
    assert!(
        page.contains("call.status === 'running' ? lastProgressLine(call.progress) : null"),
        "最近一句只在收敛起那一行的时候取，而且只在还在跑的时候取"
    );
    assert!(
        page.contains("{latest && ("),
        "取到了却没有画在收起的那一行上"
    );
    assert!(
        page.contains("{latest.shown}") && page.contains("最近一句："),
        "行上那一截有了，整句没处可抄：注释面板里要放 `latest.full`"
    );
    assert!(
        !page.contains("title={latest}"),
        "整句退回成了原生提示框：它画在页面之外，选不中、复制不了"
    );
}
