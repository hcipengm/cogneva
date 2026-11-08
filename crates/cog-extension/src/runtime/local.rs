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

/// The stderr note for a death by signal. A `SIGKILL` reaches here from three
/// places that need different responses -- the cgroup memory limit, an outside
/// kill, a crash -- and only the counter in the command's cgroup can say which,
/// so the memory cause is named only when that counter rose across the command.
fn signal_death_note(sig: i32, facts: &super::cgroup::CgroupFacts, oom: bool) -> String {
    let named = signal_name(sig);
    let signal = if named.is_empty() {
        format!("signal {sig}")
    } else {
        format!("signal {sig} ({named})")
    };
    if !oom {
        return format!("command killed by {signal}");
    }
    match (facts.limit_bytes, facts.peak_bytes) {
        (Some(limit), Some(peak)) => format!(
            "command killed by {signal}: the container memory limit OOM-killed it \
             (limit {limit} bytes, peak reached {peak} bytes)"
        ),
        (Some(limit), None) => format!(
            "command killed by {signal}: the container memory limit OOM-killed it \
             (limit {limit} bytes)"
        ),
        (None, _) => {
            format!("command killed by {signal}: the container memory limit OOM-killed it")
        }
    }
}

/// How long the wall's kill waits for its shell to be reaped.
///
/// The signal is not negotiable and the shell was already the one process this
/// side started, so what is left here is collecting it (a zombie for the life of
/// the executor otherwise). A wait longer than this buys nothing: a leader that
/// has not died to SIGKILL after it is not going to.
// cap: bound — SIGKILL 之后只剩回收：等更久换不到更早的回，只会吊住执行器
const REAP_AFTER_KILL_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Programs whose invocation compiles the tree they run in.
const BUILD_PROGRAMS: &[&str] = &[
    "cargo",
    "cargo-clippy",
    "cargo-nextest",
    "rustc",
    "clippy-driver",
    "rustdoc",
];

/// Whether this shell command compiles the tree it runs in.
///
/// Every command the executor runs is pointed at the same `CARGO_TARGET_DIR`,
/// and cargo serialises the writers to one target directory on a lock that no
/// caller's wall covers. Commands started together therefore each spend their
/// whole budget waiting for a lock, time out having compiled nothing, and leave
/// the host reading as saturated -- which narrows the build gate the same host
/// publishes to a single slot, so the verification pipeline queues behind work
/// that produced no artifact. The bound this decides is what keeps them from
/// being started together in the first place.
///
/// Read at the position a program can occupy, not anywhere in the string: a
/// command that searches for `cargo` is not a build, and charging it a build
/// slot would hold that slot for every search. A segment is cut at the shell's
/// separators, and its first word that is not a wrapper or an argument to one
/// is the program that will run.
///
/// Running the program is not the same as compiling with it. The slot is not a
/// licence for everything cargo can be asked to do: it exists because two
/// writers to one target directory serialise *inside* cargo's own lock, after
/// the caller's wall has already started. Asking cargo its version, formatting
/// the tree, or reading a manifest writes nothing there, so it is read the same
/// way a search is -- as no build at all (see
/// [`NON_BUILDING_CARGO_SUBCOMMANDS`]).
pub(crate) fn builds_the_tree(command: &str) -> bool {
    command
        .split([';', '&', '|', '(', ')', '\n'])
        .any(|segment| match program_of(segment) {
            Some((program, args)) => BUILD_PROGRAMS.contains(&program) && compiles_the_tree(&args),
            None => false,
        })
}

/// Subcommands of a build program that write nothing into the shared target
/// directory.
///
/// Measured on the host this runs on, 2026-11-08: the one build slot every
/// compile on a pod queues on was held, for 190 to 1145 seconds at a stretch,
/// by `cargo fmt --all -- --check`, `cargo --version` and `command -v cargo`
/// while builds waited behind them and then timed out inside their own wall.
///
/// `clean` is deliberately absent from this list: it compiles nothing and it
/// deletes exactly the directory the other builds write, so it needs the slot
/// for the same reason they do.
const NON_BUILDING_CARGO_SUBCOMMANDS: &[&str] = &[
    "fmt",
    "metadata",
    "tree",
    "locate-project",
    "pkgid",
    "verify-project",
    "read-manifest",
    "add",
    "remove",
    "update",
    "fetch",
    "search",
    "login",
    "logout",
    "new",
    "init",
    "help",
    "generate-lockfile",
    "vendor",
];

/// Whether what follows a build program still compiles the tree.
///
/// A program with no word left after its flags prints its own help; a word that
/// is a flag or a toolchain selector is not a subcommand; and what is left is a
/// subcommand, which compiles unless it is one of the above. Reading the
/// selector rather than requiring the subcommand first is what keeps
/// `cargo +nightly test` counted as the build it is.
fn compiles_the_tree(args: &[&str]) -> bool {
    // Flags that ask cargo about itself rather than about the tree. Read only
    // before `--`: everything after it belongs to the program the subcommand
    // runs, and `cargo test -- --version` is still a test run.
    let head = args.split(|word| *word == "--").next().unwrap_or(&[]);
    if head.iter().any(|word| ASKS_CARGO_ITSELF.contains(word)) {
        return false;
    }
    let subcommand = args.iter().copied().find(|word| {
        !word.starts_with('-') && !word.starts_with('+') && !is_a_file_descriptor(word)
    });
    match subcommand {
        Some(word) => !NON_BUILDING_CARGO_SUBCOMMANDS.contains(&word),
        None => false,
    }
}

/// Flags that make an invocation a question about cargo itself.
const ASKS_CARGO_ITSELF: &[&str] = &["--version", "-V", "--help", "-h"];

/// A bare number in command position is the descriptor a redirect named.
///
/// `cargo --version 2>&1 | head -2` is cut at `&` before this runs, so the `2`
/// of `2>&1` arrives as a word of its own and sits exactly where a subcommand
/// would. Read as one, it makes every redirect look like a build: the probe
/// then asks the build lane for a turn it never needed, and that turn is the
/// one the changes that do compile are queued for.
fn is_a_file_descriptor(word: &str) -> bool {
    !word.is_empty() && word.bytes().all(|b| b.is_ascii_digit())
}

/// The program a shell segment runs, once its wrappers are stepped over, and
/// the words that follow it.
///
/// `timeout 580 cargo test` runs cargo, `env A=B cargo build` runs cargo, and
/// `grep cargo` runs grep. The wrappers are recognised by name and their
/// arguments by shape, so what is left is the word the shell would resolve.
/// The words after it are what tells `cargo test` from `cargo --version`: both
/// run cargo, only one compiles.
///
/// `command -v cargo` is the exception among the wrappers: it asks the shell
/// where a program is and runs nothing, so there is no program to charge.
fn program_of(segment: &str) -> Option<(&str, Vec<&str>)> {
    let words: Vec<&str> = segment.split_whitespace().collect();
    for (i, &word) in words.iter().enumerate() {
        let name = Path::new(word)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(word);
        if name == "command" && matches!(words.get(i + 1), Some(&"-v") | Some(&"-V")) {
            return None;
        }
        if matches!(
            name,
            "timeout" | "nice" | "sudo" | "nohup" | "exec" | "command" | "env" | "stdbuf"
        ) {
            continue;
        }
        // A wrapper's own arguments: an option, a duration, a signal name, or
        // an assignment. None of them is the program the segment runs.
        if word.starts_with('-') || word.contains('=') || looks_like_duration(word) {
            continue;
        }
        return Some((name, words[i + 1..].to_vec()));
    }
    None
}

/// A wrapper argument that is a bare duration, such as `timeout`'s `580` or
/// `timeout`'s `10s`.
fn looks_like_duration(word: &str) -> bool {
    let digits = word.trim_end_matches(['s', 'm', 'h', 'd']);
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())
}

/// Spawn `sh -c <command>` and drive stdout/stderr/exit into an event channel.
/// Shared by the local backend and the executor server; kills the child on
/// timeout.
///
/// `workdir` overrides the child cwd (the executor anchors it to the caller's
/// task worktree); `cargo_target` is injected as `CARGO_TARGET_DIR` so trees
/// share one externalized build cache. Both are `None` for embedded usage,
/// preserving process-default behaviour.
///
/// `hold` is kept until the child has exited, which is not the same moment the
/// caller stops reading: a client that disconnects mid-command leaves it
/// running, and both facts it carries -- the executor's claim on the worktree
/// and its place in the command slot the cache pass reads -- have to last that
/// long, or the tree could be reclaimed underneath the command and the cache
/// emptied while it builds.
///
/// `build_slot` is the same kind of fact one level out: the place this command
/// holds among the builds allowed to compile into the shared target directory.
/// It is released when the child is gone, not when the request is answered, so
/// the next build starts against a directory nobody is writing.
pub(crate) fn spawn_command(
    command: &str,
    timeout: std::time::Duration,
    workdir: Option<&Path>,
    cargo_target: Option<&Path>,
    hold: Option<crate::workdir::CommandHold>,
    build_slot: Option<tokio::sync::OwnedSemaphorePermit>,
) -> SFResult<tokio::sync::mpsc::Receiver<CommandEvent>> {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c")
        .arg(command)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    // Own process group, so the timeout below can reach what the command
    // actually started. `sh -c "cargo build"` is two processes deep: killing
    // the shell leaves cargo running, and a cargo that outlives its caller
    // keeps the shared build directory locked -- measured on the cluster, a
    // clippy whose `sh` had been killed at its wall went on holding the lock
    // for the next rounds, each of which then died at its own wall having
    // compiled nothing.
    cmd.process_group(0);
    if let Some(dir) = workdir {
        cmd.current_dir(dir);
    }
    if let Some(target) = cargo_target {
        cmd.env("CARGO_TARGET_DIR", target);
    }
    // Read before the child exists: the kill count is cumulative, so the only
    // way to charge a kill to this command is to know where it started.
    let memory_before = super::cgroup::read();
    let mut child = cmd
        .spawn()
        .map_err(|e| cog_core::SFError::IO(format!("spawn sh: {}", e)))?;
    // The group id is the leader's pid once the child has run `process_group(0)`.
    let group = child.id();

    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let (tx, rx) = tokio::sync::mpsc::channel::<CommandEvent>(64);

    tokio::spawn(async move {
        // Dropped when this task ends, i.e. after the child was waited for.
        let _hold = hold;
        let _build_slot = build_slot;
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
                    let facts = super::cgroup::read();
                    let oom = super::cgroup::CgroupFacts::oom_kill_between(&memory_before, &facts);
                    if oom {
                        super::cgroup::record_oom_kill(facts.limit_bytes, facts.peak_bytes);
                    }
                    let data = signal_death_note(sig, &facts, oom);
                    let _ = tx.send(CommandEvent::Stderr { data }).await;
                    128 + sig
                }
                (None, None) => -1,
            },
            Ok(Err(_)) => -1,
            Err(_) => {
                // Not `child.kill()`: that reaches the shell and leaves every
                // process the command started -- cargo, rustc, the grandchild
                // that holds the shared target directory -- alive and holding
                // it. The whole group goes, leader included.
                if let Some(pid) = group {
                    kill_group(pid);
                }
                // Reap the leader so the pid is not left as a zombie for the
                // life of the executor. Bounded: a group that ignores SIGKILL
                // is not something this path should wait on.
                let _ = tokio::time::timeout(REAP_AFTER_KILL_WAIT, child.wait()).await;
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

/// SIGKILL a whole process group. `pid` is the leader, which is the group id
/// because the command was started with `process_group(0)`.
///
/// A missing group is the normal race between "the wall came up" and "the
/// command had already exited", and is not an error worth reporting.
fn kill_group(pid: u32) {
    // SAFETY: one pid and one signal constant; no pointers, no ownership.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
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
                // Embedded use has no shared target directory to serialise:
                // the command runs in whatever tree the caller's cwd names, so
                // there is no slot to take here.
                let rx = spawn_command(command, req.timeout, None, None, None, None)?;
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
            SandboxPayload::EditFile {
                path,
                old_string,
                new_string,
                replace_all,
            } => {
                let result = crate::edit::apply(
                    std::path::Path::new(path),
                    old_string,
                    new_string,
                    *replace_all,
                )
                .await
                .map(|report| report.describe(path))
                .map_err(std::io::Error::other);
                Ok(file_stream(result))
            }
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

    /// The commands this bound exists for are read at the position a program
    /// occupies: what matters is the program the shell will run, not whether
    /// the word appears somewhere in the string.
    #[test]
    fn a_build_is_read_where_a_program_runs_and_nowhere_else() {
        assert!(builds_the_tree("cargo test -p cog-agent --lib"));
        assert!(builds_the_tree(
            "cd /tmp/w && cargo clippy --all-targets -- -D warnings"
        ));
        assert!(builds_the_tree("timeout 580 cargo test -p cogneva --lib"));
        assert!(builds_the_tree("env CARGO_TARGET_DIR=/tmp/t cargo build"));
        assert!(builds_the_tree("cargo test --workspace 2>&1 | tail -30"));
        assert!(builds_the_tree("/usr/local/rustup/toolchains/x/bin/rustc -o /tmp/x /tmp/x.rs"));
        assert!(builds_the_tree("cargo +nightly test -p cog-core"));
        assert!(builds_the_tree("cargo nextest run --profile ci"));
        // `clean` compiles nothing and deletes the directory the others write.
        assert!(builds_the_tree("cargo clean -p cog-core"));

        // A search, a read, and a listing mention the word without compiling.
        assert!(!builds_the_tree("grep -rn cargo crates/ | head"));
        assert!(!builds_the_tree("cat Cargo.toml"));
        assert!(!builds_the_tree("ls -la target/debug"));
        assert!(!builds_the_tree("echo 'cargo test' >> notes.txt"));
        assert!(!builds_the_tree(""));

        // Running one of those programs is not compiling with it. Each of
        // these held the pod's only build slot for minutes while real builds
        // queued behind it and timed out inside their own wall.
        assert!(!builds_the_tree("command -v cargo"));
        assert!(!builds_the_tree("command -V cargo && cargo --version"));
        assert!(!builds_the_tree("cargo --version 2>&1 | head -2"));
        assert!(!builds_the_tree("cargo -V"));
        assert!(!builds_the_tree("cargo fmt --all -- --check"));
        assert!(!builds_the_tree(
            "cd . && git apply x.patch && cargo fmt --all -- --check && echo FMT_STILL_OK"
        ));
        assert!(!builds_the_tree(
            "cargo metadata --format-version 1 --no-deps"
        ));
        assert!(!builds_the_tree("cargo tree -p cog-core --depth 1"));
        assert!(!builds_the_tree("cargo locate-project --message-format plain"));
        assert!(!builds_the_tree("cargo fetch --locked"));
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

    /// Whether the pid is still doing work: present in `/proc` and not already
    /// a zombie. A killed process that its own parent has not reaped yet is
    /// dead, and counting it as alive would make this reading depend on the
    /// reaping habits of whatever pid 1 is.
    fn still_running(pid: u32) -> bool {
        let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(s) => s,
            Err(_) => return false,
        };
        // The state field follows the parenthesised command name, which may
        // itself contain spaces and parentheses.
        let after_name = match stat.rfind(')') {
            Some(i) => &stat[i + 1..],
            None => return true,
        };
        after_name.split_whitespace().next() != Some("Z")
    }

    /// The wall has to end what the command started, not only the shell this
    /// executor spawns. `sh -c "cargo build"` is two processes deep, and a
    /// cargo left behind by a timed-out command keeps the shared build
    /// directory locked: measured on the cluster, a build whose `sh` had been
    /// killed at its wall went on holding the lock, and every round queued
    /// behind it died at its own wall having compiled nothing.
    #[tokio::test]
    async fn a_timeout_kills_what_the_command_started_not_only_the_shell() {
        let backend = LocalExecutor::new();
        let mut req = cmd("sleep 300 & echo $!; wait");
        req.timeout = std::time::Duration::from_secs(1);
        let result = backend.execute(&req).await.unwrap();
        assert_eq!(result.exit_code, -1);
        let pid: u32 = result
            .stdout
            .trim()
            .parse()
            .expect("the command echoes the pid it started");

        for _ in 0..40 {
            if !still_running(pid) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("pid {pid} outlived the timeout: the wall reached the shell only");
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

    /// The memory cause is named only when the cgroup counter moved across the
    /// command, and then it carries the ceiling that was hit. Guessing it from
    /// the signal number alone would blame the memory limit for an outside
    /// kill, and a build that dies of an actual OOM with no numbers attached
    /// leaves the reader unable to tell a too-small limit from a bad change.
    #[test]
    fn a_memory_limit_kill_names_the_ceiling_and_nothing_else_claims_one() {
        let facts = super::super::cgroup::CgroupFacts {
            limit_bytes: Some(1073741824),
            peak_bytes: Some(1073741824),
            oom_kills: Some(7),
            ..Default::default()
        };
        let oom = signal_death_note(9, &facts, true);
        assert!(oom.contains("signal 9 (SIGKILL)"), "{oom}");
        assert!(oom.contains("memory limit OOM-killed"), "{oom}");
        assert!(oom.contains("limit 1073741824 bytes"), "{oom}");
        assert!(oom.contains("peak reached 1073741824 bytes"), "{oom}");

        // The counter read fine but did not move: the kill came from elsewhere
        // and the note must not mention memory at all.
        let outside = signal_death_note(9, &facts, false);
        assert_eq!(outside, "command killed by signal 9 (SIGKILL)");

        // Counter rose but the cgroup published no ceiling: still an OOM kill,
        // just without a number to quote.
        let unmeasured = signal_death_note(
            9,
            &super::super::cgroup::CgroupFacts {
                oom_kills: Some(1),
                ..Default::default()
            },
            true,
        );
        assert!(
            unmeasured.contains("memory limit OOM-killed"),
            "{unmeasured}"
        );
        assert!(!unmeasured.contains("bytes"), "{unmeasured}");
    }

    #[test]
    fn an_unknown_signal_number_is_still_reported() {
        assert_eq!(
            signal_death_note(64, &Default::default(), false),
            "command killed by signal 64"
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
