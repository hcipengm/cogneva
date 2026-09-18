//! In-process environment backend: executes `Command` payloads via `sh -c`
//! and file payloads via `tokio::fs`.
//!
//! Used when no remote sandbox executor is configured (embedded and
//! development mode). Production cluster deployments route these payloads to
//! the executor pod via [`super::remote::RemoteExecutor`] instead.

use std::os::unix::process::ExitStatusExt;
use std::path::Path;

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

/// Name the common signals so a record reads without a lookup: 137 and 139
/// differ only in which signal arrived, and SIGKILL (usually the cgroup memory
/// limit) calls for different handling than SIGSEGV (code actually crashed).
/// Unknown signals return an empty string so the caller reports the number alone.
fn signal_name(sig: i32) -> &'static str {
    match sig {
        1 => "SIGHUP",
        2 => "SIGINT",
        3 => "SIGQUIT",
        4 => "SIGILL",
        6 => "SIGABRT",
        8 => "SIGFPE",
        9 => "SIGKILL",
        11 => "SIGSEGV",
        13 => "SIGPIPE",
        15 => "SIGTERM",
        _ => "",
    }
}

/// Spawn `sh -c <command>` and drive stdout/stderr/exit into an event channel.
/// Shared by the local backend and the executor server; kills the child on
/// timeout.
///
/// `workdir` overrides the child cwd (the executor anchors it to the caller's
/// task worktree); `cargo_target` is injected as `CARGO_TARGET_DIR` so trees
/// share one externalized build cache. Both are `None` for embedded usage,
/// preserving process-default behaviour.
pub(crate) fn spawn_command(
    command: &str,
    timeout: std::time::Duration,
    workdir: Option<&Path>,
    cargo_target: Option<&Path>,
) -> SFResult<tokio::sync::mpsc::Receiver<CommandEvent>> {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c")
        .arg(command)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(dir) = workdir {
        cmd.current_dir(dir);
    }
    if let Some(target) = cargo_target {
        cmd.env("CARGO_TARGET_DIR", target);
    }
    let mut child = cmd
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
            // `code()` is only set on a normal exit: a signal-terminated child
            // (the cgroup OOM killer, a crash, an external kill) reports None,
            // and this branch used to collapse into the same -1 as a timeout --
            // the two deaths looked identical downstream and the signal number
            // was dropped. Report it with the shell's 128+signal convention
            // (SIGKILL -> 137, SIGTERM -> 143, matching what k8s reports for an
            // OOMKilled container) and name the signal in stderr, since 137 by
            // itself cannot be told apart from a process calling exit(137).
            Ok(Ok(status)) => match (status.code(), status.signal()) {
                (Some(c), _) => c,
                (None, Some(sig)) => {
                    let named = signal_name(sig);
                    let data = if named.is_empty() {
                        format!("command killed by signal {sig}")
                    } else {
                        format!("command killed by signal {sig} ({named})")
                    };
                    let _ = tx.send(CommandEvent::Stderr { data }).await;
                    128 + sig
                }
                (None, None) => -1,
            },
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
                let rx = spawn_command(command, req.timeout, None, None)?;
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

    /// A signal death must not look like a timeout or like any other failure:
    /// the code carries which signal arrived (128+signal, the convention k8s
    /// uses for OOMKilled) and stderr names it. A cgroup OOM kill and a code
    /// crash have to be told apart from a plain non-zero exit by whoever reads
    /// the result.
    #[tokio::test]
    async fn local_backend_names_the_signal_that_killed_the_command() {
        let backend = LocalExecutor::new();

        let oom = backend.execute(&cmd("kill -9 $$")).await.unwrap();
        assert_eq!(oom.exit_code, 137, "SIGKILL must not collapse into -1");
        assert!(
            oom.stderr.contains("killed by signal 9 (SIGKILL)"),
            "the record must name the signal: {}",
            oom.stderr
        );

        // A second signal proves the number is carried, not hard-coded.
        let term = backend.execute(&cmd("kill -15 $$")).await.unwrap();
        assert_eq!(term.exit_code, 143);
        assert!(
            term.stderr.contains("signal 15 (SIGTERM)"),
            "{}",
            term.stderr
        );
    }

    /// A command that exits 137 on its own is a normal exit and must not be
    /// dressed up as a signal death.
    #[tokio::test]
    async fn a_self_reported_exit_code_is_not_read_as_a_signal() {
        let backend = LocalExecutor::new();
        let result = backend.execute(&cmd("exit 137")).await.unwrap();
        assert_eq!(result.exit_code, 137);
        assert!(result.stderr.is_empty(), "{}", result.stderr);
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
