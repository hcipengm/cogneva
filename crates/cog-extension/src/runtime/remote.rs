//! Remote sandbox executor client: routes `Command` payloads to the executor
//! pod over HTTP and streams output back as NDJSON [`CommandEvent`] lines.

use async_trait::async_trait;

/// 连远端运行时（沙盒执行器）的墙：10s。
///
/// 连不上通常是被网络策略挡住或对端没起，这两种都不靠等解决；10s 让调用方
/// 快速失败，而不是卡在 SYN 重传里。
// cap: bound — 连沙盒执行器的墙
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// 执行器换一次版要多久没有后端：这副部署只有一副副本，且
/// `maxSurge: 0` / `maxUnavailable: 1`——单节点上第二副没有地方调度，所以换版是
/// 先删后建，中间有一段服务没有就绪后端（取舍写在
/// `deploy/k3s/sandbox-executor-deployment.yaml`）。
///
/// 实测 2026-11-06：新 Pod 的 init 在 06:09:37 起来，主容器 06:12:03 才开始跑，
/// 两分半里连 `echo alive` 都拿不到连接。而在飞的那一轮 Agent 正是靠这个执行器
/// 跑 `cargo clippy`、读文件、产出 diff：它重试十几次都落在窗口里，最后交了白卷，
/// 一条停线单连交三轮零改动、预算耗尽记 `failed`。窗口比一条命令长，客户端就得
/// 比窗口更长——这不是"执行器坏了"，是"执行器正在被换掉"。
// cap: bound — 跨一次执行器换版的等待上界
const REPLACEMENT_WINDOW: std::time::Duration = std::time::Duration::from_secs(200);

/// 换版窗口里第一次重试之前等多久。
///
/// 比它更短的等法没有意义：端口拒绝是「还没有就绪后端」，而下一次尝试要跨过的是
/// 一次容器启动，不是一次调度；等得比这更短只是把同一条失败的连接多打几遍。
// cap: bound — 重试退避的起点，短于一次容器启动就没有可等的东西
const RETRY_FIRST_PAUSE: std::time::Duration = std::time::Duration::from_millis(250);

/// 退避翻倍到这个数就不再涨。
///
/// 窗口有两百秒，而退避只负责「不要空转」：五秒已经很粗，再长就只是在窗口里少试
/// 几次，把本来已经就绪的后端多等一轮。
// cap: bound — 退避的粗粒度上界，再长就少试几次而已
const RETRY_MAX_PAUSE: std::time::Duration = std::time::Duration::from_secs(5);

/// 端口被明确拒绝与包被丢掉是两件事。前者说「服务现在没有就绪后端」，也就是
/// 一次换版的窗口，自己会好；后者多是网络策略挡住，等多久都不会好，等它只是把
/// 失败推后。所以只认前者。
///
/// 也只在**连接没建起来**时重试：这条错误链上出现 `ConnectionRefused` 意味着请求
/// 根本没送到对端，重发是安全的；一旦连通过，`run_command` 与 `write_file` 都不
/// 幂等，重发就可能执行两次。
fn the_port_refused(e: &(dyn std::error::Error + 'static)) -> bool {
    let refused = |candidate: &(dyn std::error::Error + 'static)| {
        candidate
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused)
    };
    // 链头也要看：调用方直接递进来的就是一个 `io::Error` 时，它自己就是答案，
    // 往它的 `source()` 里找只会找到 `None`。
    if refused(e) {
        return true;
    }
    let mut source = e.source();
    while let Some(current) = source {
        if refused(current) {
            return true;
        }
        source = current.source();
    }
    false
}
use cog_core::{
    CommandEvent, CommandEventStream, SFResult, SandboxBackend, SandboxPayload, SandboxRequest,
    SandboxResult,
};
use futures::StreamExt;

/// HTTP client for the cluster sandbox executor (`cogneva sandbox-executor`).
pub struct RemoteExecutor {
    base_url: String,
    http: reqwest::Client,
    /// 端口被拒之后还等多久。见 [`REPLACEMENT_WINDOW`]。
    replacement_window: std::time::Duration,
}

impl RemoteExecutor {
    pub fn new(base_url: impl Into<String>) -> SFResult<Self> {
        Self::with_replacement_window(base_url, REPLACEMENT_WINDOW)
    }

    /// 同一个客户端，换一段等待上界。生产的窗口是
    /// [`REPLACEMENT_WINDOW`]；这里开一个口子是为了让"窗口内会重试""窗口过了
    /// 要说出等了多久"这两条判据能在测试里按秒验证，而不是按两分半验证。
    pub fn with_replacement_window(
        base_url: impl Into<String>,
        replacement_window: std::time::Duration,
    ) -> SFResult<Self> {
        Ok(Self {
            base_url: base_url.into(),
            replacement_window,
            // No total timeout: a long-running command is stopped by the
            // executor when it stops moving, and the client must not cut the
            // stream early. The connect phase is bounded separately: if the
            // executor pod is
            // down or unreachable (e.g. blocked by network policy), fail fast
            // in 10s instead of leaving the caller hanging on SYN retries.
            http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .map_err(|e| {
                    cog_core::SFError::Agent(format!("sandbox executor client init failed: {}", e))
                })?,
        })
    }
}

#[async_trait]
impl SandboxBackend for RemoteExecutor {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        Ok(super::local::collect_stream(self.execute_stream(req).await?).await)
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(cog_core::SFError::Agent(
            "RemoteExecutor does not compile WASM modules".into(),
        ))
    }

    async fn execute_stream(&self, req: &SandboxRequest) -> SFResult<CommandEventStream> {
        if matches!(req.payload, SandboxPayload::Wasm { .. }) {
            return Err(cog_core::SFError::Agent(
                "RemoteExecutor only executes environment payloads".into(),
            ));
        }
        let url = format!("{}/execute", self.base_url.trim_end_matches('/'));
        let body = serde_json::json!({
            "payload": req.payload,
            "task_id": req.task_id,
            "agent_id": req.agent_id,
        });
        // 换版窗口里的调用要等，窗口外的失败要当场报。「等」与「报」的分界是
        // 端口有没有明确拒绝：拒绝＝服务暂时没有后端，会自己好；其余（连不通、
        // 被丢掉）等下去也不会好。见 [`the_port_refused`]。
        let started = std::time::Instant::now();
        let mut pause = RETRY_FIRST_PAUSE;
        let response = loop {
            match self.http.post(&url).json(&body).send().await {
                Ok(response) => break response,
                Err(e) if the_port_refused(&e) && started.elapsed() < self.replacement_window => {
                    tracing::warn!(
                        waited_secs = started.elapsed().as_secs(),
                        retry_in_ms = pause.as_millis() as u64,
                        "the sandbox executor has no ready backend; a rollout replaces this \
                         pod, so the call waits inside the replacement window"
                    );
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(RETRY_MAX_PAUSE);
                }
                Err(e) => {
                    let note = if the_port_refused(&e) {
                        format!(
                            " (the port refused for {}s, past the {}-second replacement window: \
                             the executor's rollout is either still going or stuck, so waiting \
                             longer from here would just postpone the failure)",
                            started.elapsed().as_secs(),
                            self.replacement_window.as_secs()
                        )
                    } else {
                        String::new()
                    };
                    return Err(cog_core::SFError::Agent(format!(
                        "sandbox executor unreachable: {e}{note}"
                    )));
                }
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(cog_core::SFError::Agent(format!(
                "sandbox executor returned {}: {}",
                status, body
            )));
        }

        // Decode the NDJSON stream: buffer bytes, split on newlines, parse
        // each complete line as a CommandEvent.
        let state = (response.bytes_stream(), String::new());
        let stream = futures::stream::unfold(state, |(mut bytes, mut buf)| async move {
            loop {
                if let Some(pos) = buf.find('\n') {
                    let line = buf[..pos].to_string();
                    buf.drain(..=pos);
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<CommandEvent>(&line) {
                        Ok(event) => return Some((event, (bytes, buf))),
                        Err(e) => {
                            tracing::warn!("sandbox executor sent malformed event: {}", e);
                            continue;
                        }
                    }
                }
                match bytes.next().await {
                    Some(Ok(chunk)) => buf.push_str(&String::from_utf8_lossy(&chunk)),
                    Some(Err(e)) => {
                        tracing::warn!("sandbox executor stream error: {}", e);
                        return None;
                    }
                    None => return None,
                }
            }
        });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal NDJSON server on an ephemeral port speaking the executor
    /// wire protocol; no HTTP framework needed for a canned response.
    async fn serve_once(lines: Vec<String>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            // Drain request headers.
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let body = lines.join("\n") + "\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://{}", addr)
    }

    fn cmd_req(command: &str) -> SandboxRequest {
        SandboxRequest {
            task_id: "t".into(),
            agent_id: "test".into(),
            payload: SandboxPayload::Command {
                command: command.into(),
            },
            input: serde_json::json!({}),
            limits: Default::default(),
        }
    }

    #[tokio::test]
    async fn remote_executor_decodes_ndjson_stream() {
        let url = serve_once(vec![
            serde_json::to_string(&CommandEvent::Stdout {
                data: "hello".into(),
            })
            .unwrap(),
            serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap(),
        ])
        .await;
        let executor = RemoteExecutor::new(url).unwrap();
        let result = executor.execute(&cmd_req("echo hello")).await.unwrap();
        assert_eq!(result.stdout, "hello");
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn remote_executor_surfaces_http_errors() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            socket
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 3\r\n\r\nbad")
                .await
                .unwrap();
        });
        let executor = RemoteExecutor::new(format!("http://{}", addr)).unwrap();
        let err = executor.execute(&cmd_req("echo hi")).await.unwrap_err();
        assert!(err.to_string().contains("500"));
    }

    /// 一个刚被释放的端口：连上去操作系统会明确拒绝，这正是执行器换版时
    /// 「服务没有就绪后端」的样子。
    async fn a_port_nothing_listens_on() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    /// 等待期间起来的执行器必须被等到：换版窗口是两分半，一条命令的失败不该
    /// 记在 Agent 头上。断言的是"重试跨过了窗口里的那一段"，不是"重试存在"。
    #[tokio::test]
    async fn a_replaced_executor_is_waited_for_instead_of_reported_unreachable() {
        let addr = a_port_nothing_listens_on().await;
        let executor = RemoteExecutor::with_replacement_window(
            format!("http://{addr}"),
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let (mut socket, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let body = format!(
                "{}\n",
                serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap()
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let result = executor.execute(&cmd_req("echo alive")).await;
        assert!(
            result.is_ok(),
            "the executor came back inside the window, so the call must succeed: {:?}",
            result.err().map(|e| e.to_string())
        );
    }

    /// 窗口过了还连不上，报错要说清等过多久、为什么不再等——否则下一轮 Agent
    /// 会把一段换版读成"平台坏了"，再交一次白卷。
    #[tokio::test]
    async fn a_port_that_never_comes_back_names_the_window_it_waited() {
        let addr = a_port_nothing_listens_on().await;
        let executor = RemoteExecutor::with_replacement_window(
            format!("http://{addr}"),
            std::time::Duration::from_millis(600),
        )
        .unwrap();
        let err = executor.execute(&cmd_req("echo hi")).await.unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("replacement window"),
            "the message must say the wait was bounded by a replacement window: {text}"
        );
        assert!(
            text.contains("sandbox executor unreachable"),
            "the message must still name what was unreachable: {text}"
        );
    }

    /// 拒绝与丢包分属两条路：前者会自己好，后者等多久都不会。
    #[test]
    fn a_refused_port_is_told_apart_from_a_packet_the_network_dropped() {
        let refused = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let dropped = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
        assert!(the_port_refused(&refused));
        assert!(!the_port_refused(&dropped));
    }

    #[tokio::test]
    async fn remote_executor_rejects_wasm_payload() {
        let executor = RemoteExecutor::new("http://127.0.0.1:1").unwrap();
        let mut req = cmd_req("echo hi");
        req.payload = SandboxPayload::Wasm {
            bytes: vec![],
            entry: "main".into(),
        };
        assert!(executor.execute(&req).await.is_err());
    }
}
