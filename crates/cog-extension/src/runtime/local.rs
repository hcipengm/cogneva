//! In-process environment backend: executes `Command` payloads via `sh -c`
//! and file payloads via `tokio::fs`.
//!
//! Used when no remote sandbox executor is configured (embedded and
//! development mode). Production cluster deployments route these payloads to
//! the executor pod via [`super::remote::RemoteExecutor`] instead.

use async_trait::async_trait;
use cog_core::{
    CommandEvent, CommandEventStream, SFResult, SandboxBackend, SandboxPayload, SandboxRequest,
    SandboxResult,
};

/// Runs shell commands and file operations in the current process.
pub struct LocalExecutor;

impl LocalExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn `sh -c <command>` and drive stdout/stderr/exit into an event channel.
/// Shared by the local backend and the executor server; kills the child on
/// timeout.
pub(crate) fn spawn_command(
    command: &str,
    timeout: std::time::Duration,
) -> SFResult<tokio::sync::mpsc::Receiver<CommandEvent>> {
    let mut child = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| cog_core::SFError::IO(format!("spawn sh: {}", e)))?;

    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let (tx, rx) = tokio::sync::mpsc::channel::<CommandEvent>(64);

    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let tx_out = tx.clone();
        let tx_err = tx.clone();
        let read_out = async move {
            let mut buf = [0u8; 8192];
            loop {
                match stdout.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let data = String::from_utf8_lossy(&buf[..n]).into_owned();
                        if tx_out.send(CommandEvent::Stdout { data }).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        };
        let read_err = async move {
            let mut buf = [0u8; 8192];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let data = String::from_utf8_lossy(&buf[..n]).into_owned();
                        if tx_err.send(CommandEvent::Stderr { data }).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        };
        let readers = async {
            tokio::join!(read_out, read_err);
        };
        let exited = tokio::time::timeout(timeout, async {
            readers.await;
            child.wait().await
        })
        .await;
        let code = match exited {
            Ok(Ok(status)) => status.code().unwrap_or(-1),
            Ok(Err(_)) => -1,
            Err(_) => {
                let _ = child.kill().await;
                let _ = tx
                    .send(CommandEvent::Stderr {
                        data: format!("command timed out after {}s", timeout.as_secs()),
                    })
                    .await;
                -1
            }
        };
        let _ = tx.send(CommandEvent::Exit { code }).await;
    });

    Ok(rx)
}

/// A file operation as a two-event stream: content (or error) then exit.
fn file_stream(result: std::io::Result<String>) -> CommandEventStream {
    let events = match result {
        Ok(content) => vec![
            CommandEvent::Stdout { data: content },
            CommandEvent::Exit { code: 0 },
        ],
        Err(e) => vec![
            CommandEvent::Stderr {
                data: e.to_string(),
            },
            CommandEvent::Exit { code: 1 },
        ],
    };
    Box::pin(futures::stream::iter(events))
}

/// Collect a command event stream into a buffered [`SandboxResult`].
pub(crate) async fn collect_stream(mut stream: CommandEventStream) -> SandboxResult {
    use futures::StreamExt;
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut exit_code = 0;
    while let Some(event) = stream.next().await {
        match event {
            CommandEvent::Stdout { data } => stdout.push_str(&data),
            CommandEvent::Stderr { data } => stderr.push_str(&data),
            CommandEvent::Exit { code } => exit_code = code,
        }
    }
    SandboxResult {
        stdout,
        stderr,
        exit_code,
        output: None,
        duration_ms: 0,
        resource_usage: Default::default(),
    }
}

#[async_trait]
impl SandboxBackend for LocalExecutor {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        Ok(collect_stream(self.execute_stream(req).await?).await)
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(cog_core::SFError::Agent(
            "LocalExecutor does not compile WASM modules".into(),
        ))
    }

    async fn execute_stream(&self, req: &SandboxRequest) -> SFResult<CommandEventStream> {
        match &req.payload {
            SandboxPayload::Command { command } => {
                tracing::warn!(command = %command, "local command execution (no remote executor configured)");
                let rx = spawn_command(command, req.timeout)?;
                Ok(Box::pin(futures::stream::unfold(rx, |mut rx| async move {
                    rx.recv().await.map(|event| (event, rx))
                })))
            }
            SandboxPayload::ReadFile { path } => {
                Ok(file_stream(tokio::fs::read_to_string(path).await))
            }
            SandboxPayload::WriteFile { path, content } => Ok(file_stream(
                tokio::fs::write(path, content).await.map(|_| String::new()),
            )),
            SandboxPayload::Wasm { .. } => Err(cog_core::SFError::Agent(
                "LocalExecutor only executes environment payloads".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(payload: SandboxPayload) -> SandboxRequest {
        SandboxRequest {
            task_id: "t".into(),
            agent_id: "test".into(),
            payload,
            input: serde_json::json!({}),
            timeout: std::time::Duration::from_secs(10),
            limits: Default::default(),
        }
    }

    fn cmd(command: &str) -> SandboxRequest {
        req(SandboxPayload::Command {
            command: command.into(),
        })
    }

    #[tokio::test]
    async fn local_backend_runs_pipes() {
        let backend = LocalExecutor::new();
        let result = backend
            .execute(&cmd("echo a b c | tr ' ' '\\n' | head -2"))
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, "a\nb\n");
    }

    #[tokio::test]
    async fn local_backend_reports_exit_code() {
        let backend = LocalExecutor::new();
        let result = backend.execute(&cmd("exit 3")).await.unwrap();
        assert_eq!(result.exit_code, 3);
    }

    #[tokio::test]
    async fn local_backend_kills_on_timeout() {
        let backend = LocalExecutor::new();
        let mut req = cmd("sleep 30");
        req.timeout = std::time::Duration::from_secs(1);
        let result = backend.execute(&req).await.unwrap();
        assert_eq!(result.exit_code, -1);
        assert!(result.stderr.contains("timed out"));
    }

    #[tokio::test]
    async fn local_backend_file_roundtrip() {
        let backend = LocalExecutor::new();
        let dir = std::env::temp_dir().join(format!("cog-local-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.txt").to_string_lossy().into_owned();

        let write = backend
            .execute(&req(SandboxPayload::WriteFile {
                path: path.clone(),
                content: "payload".into(),
            }))
            .await
            .unwrap();
        assert_eq!(write.exit_code, 0);

        let read = backend
            .execute(&req(SandboxPayload::ReadFile { path: path.clone() }))
            .await
            .unwrap();
        assert_eq!(read.exit_code, 0);
        assert_eq!(read.stdout, "payload");

        let missing = backend
            .execute(&req(SandboxPayload::ReadFile {
                path: dir.join("nope").to_string_lossy().into_owned(),
            }))
            .await
            .unwrap();
        assert_eq!(missing.exit_code, 1);
        assert!(!missing.stderr.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn local_backend_rejects_wasm_payload() {
        let backend = LocalExecutor::new();
        let r = req(SandboxPayload::Wasm {
            bytes: vec![],
            entry: "main".into(),
        });
        assert!(backend.execute(&r).await.is_err());
    }
}
