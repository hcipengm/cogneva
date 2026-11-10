//! Commands that outlive the tool call that started them.
//!
//! A build or a test run is minutes of wall time, and the agent that started it
//! has nothing to add until it ends. Held in the foreground, the entire
//! conversation waits on it: the turn stays silent for as long as the compile
//! takes, and the person on the other end cannot tell a long build from a stuck
//! one. So the call returns a handle at once, the command runs on in the task's
//! working directory, and `command_output` reads what it has written since.
//!
//! The book of handles is process-wide rather than per registry. A registry is
//! forked per conversation and per task, and a command must stay readable from
//! whichever fork asks next -- including the fork that only has the output tool.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use cog_core::CommandEvent;

/// How many finished commands the book keeps, most recent last.
///
/// The output of a finished command is held until it is dropped, so an
/// unbounded book is an unbounded leak in a process that never restarts. Only
/// finished commands are dropped, and only the oldest; a handle that has aged
/// out is refused by name rather than answered with someone else's output.
// cap: bound — 这本书只服务「刚发出去、还在被轮询」的那几条；几十条之外的读者要的已经不是「刚才那条」而是审计，而审计不在这本书里
const RETAINED: usize = 64;

/// How a command ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    /// The shell gave an exit status.
    Exited(i32),
    /// The executor could not run it, or its stream ended without a status.
    Failed(String),
}

/// One command, running or finished, named by a handle.
pub struct Command {
    handle: String,
    started: Instant,
    stdout: Mutex<String>,
    stderr: Mutex<String>,
    /// The checkout the command runs in, as the executor announced it. A
    /// background run answers with a handle before the executor has said
    /// anything, so the path arrives with a later poll, not with the handle.
    workdir: Mutex<Option<String>>,
    ending: Mutex<Option<Ending>>,
    done: tokio::sync::watch::Sender<bool>,
    done_rx: tokio::sync::watch::Receiver<bool>,
}

impl Command {
    fn new(handle: String) -> Arc<Self> {
        let (done, done_rx) = tokio::sync::watch::channel(false);
        Arc::new(Self {
            handle,
            started: Instant::now(),
            stdout: Mutex::new(String::new()),
            stderr: Mutex::new(String::new()),
            workdir: Mutex::new(None),
            ending: Mutex::new(None),
            done,
            done_rx,
        })
    }

    pub fn handle(&self) -> &str {
        &self.handle
    }

    /// Fold one streamed event in. The exit status is returned rather than
    /// stored here: the caller decides whether the stream is over.
    pub fn push(&self, event: CommandEvent) -> Option<i32> {
        match event {
            CommandEvent::Stdout { data } => {
                self.stdout.lock().unwrap().push_str(&data);
                None
            }
            CommandEvent::Stderr { data } => {
                self.stderr.lock().unwrap().push_str(&data);
                None
            }
            CommandEvent::Workdir { path } => {
                *self.workdir.lock().unwrap() = Some(path);
                None
            }
            CommandEvent::Exit { code } => Some(code),
        }
    }

    pub fn finish(&self, ending: Ending) {
        *self.ending.lock().unwrap() = Some(ending);
        let _ = self.done.send(true);
    }

    pub fn ending(&self) -> Option<Ending> {
        self.ending.lock().unwrap().clone()
    }

    /// Wait up to `secs` for the command to finish. Zero means "answer now".
    pub async fn wait(&self, secs: u64) {
        if secs == 0 || self.ending().is_some() {
            return;
        }
        let mut finished = self.done_rx.clone();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(secs),
            finished.wait_for(|done| *done),
        )
        .await;
    }

    /// What the reader gets: what this command has written since `offset`, and
    /// how it stands.
    ///
    /// `stdout` is paged from `offset` so a poll of a long build carries only
    /// what is new; `next_offset` is the cursor to pass back. `stderr` is
    /// returned whole -- it is for reading why something failed, and a failure
    /// is normally the last thing read.
    pub fn snapshot(&self, offset: usize) -> serde_json::Value {
        let stdout = self.stdout.lock().unwrap().clone();
        // A caller can only ever pass back an offset this reader produced, but
        // it is an argument, and slicing a string on a byte that is not a
        // character boundary panics. Walk back to one instead of trusting it.
        let mut start = offset.min(stdout.len());
        while start > 0 && !stdout.is_char_boundary(start) {
            start -= 1;
        }
        let slice = &stdout[start..];
        let mut out = serde_json::json!({
            "handle": self.handle,
            "stdout": slice,
            "stderr": self.stderr.lock().unwrap().clone(),
            "next_offset": stdout.len(),
            "elapsed_ms": self.started.elapsed().as_millis() as u64,
        });
        let (state, code, error) = match self.ending() {
            None => ("running", serde_json::Value::Null, serde_json::Value::Null),
            Some(Ending::Exited(code)) => {
                ("exited", serde_json::json!(code), serde_json::Value::Null)
            }
            Some(Ending::Failed(why)) => {
                ("failed", serde_json::Value::Null, serde_json::json!(why))
            }
        };
        if let Some(workdir) = self.workdir.lock().unwrap().clone() {
            out["workdir"] = serde_json::json!(workdir);
        }
        out["state"] = serde_json::json!(state);
        out["exit_code"] = code;
        out["error"] = error;
        out
    }
}

#[derive(Default)]
struct Book {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    commands: HashMap<String, Arc<Command>>,
    order: VecDeque<String>,
}

impl Book {
    fn insert(&self, command: Arc<Command>) {
        let mut inner = self.inner.lock().unwrap();
        let handle = command.handle().to_string();
        inner.order.push_back(handle.clone());
        inner.commands.insert(handle, command);
        // Trim from the oldest end. A running command is never dropped -- its
        // reader would lose the only way to find out what it printed -- and
        // the scan steps over it rather than stopping there: one long build at
        // the head would otherwise hold every command started behind it, and
        // the book would grow for as long as that build ran instead of staying
        // at its bound.
        let mut kept = VecDeque::with_capacity(inner.order.len());
        let mut len = inner.commands.len();
        while let Some(handle) = inner.order.pop_front() {
            let finished = len > RETAINED
                && inner
                    .commands
                    .get(&handle)
                    .map(|c| c.ending().is_some())
                    .unwrap_or(true);
            if finished {
                inner.commands.remove(&handle);
                len -= 1;
            } else {
                kept.push_back(handle);
            }
        }
        inner.order = kept;
    }

    fn get(&self, handle: &str) -> Option<Arc<Command>> {
        self.inner.lock().unwrap().commands.get(handle).cloned()
    }
}

fn book() -> &'static Book {
    static BOOK: OnceLock<Book> = OnceLock::new();
    BOOK.get_or_init(Book::default)
}

/// Hand out a handle and file it. The caller runs the command and folds its
/// events in; the handle is readable from that moment, which is why the book
/// entry exists before the command has produced anything.
pub fn open() -> Arc<Command> {
    let handle = format!("cmd-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let command = Command::new(handle);
    book().insert(command.clone());
    command
}

/// The command under `handle`, or `None` if it was never opened or has aged out.
pub fn get(handle: &str) -> Option<Arc<Command>> {
    book().get(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_handle_reads_back_its_own_output_and_state() {
        let command = open();
        let handle = command.handle().to_string();
        assert_eq!(
            get(&handle)
                .expect("a handle is readable the moment it exists")
                .handle(),
            handle
        );
        command.push(CommandEvent::Stdout {
            data: "compiling\n".into(),
        });
        command.push(CommandEvent::Stderr {
            data: "warning\n".into(),
        });
        let running = get(&handle).unwrap().snapshot(0);
        assert_eq!(running["state"], "running");
        assert_eq!(running["stdout"], "compiling\n");
        assert_eq!(running["stderr"], "warning\n");

        command.finish(Ending::Exited(0));
        let done = get(&handle).unwrap().snapshot(0);
        assert_eq!(done["state"], "exited");
        assert_eq!(done["exit_code"], 0);
    }

    /// 分页读：第一页之后的读只带新写的那一截，游标由上一页给出。
    #[tokio::test]
    async fn a_second_read_carries_only_what_is_new() {
        let command = open();
        command.push(CommandEvent::Stdout {
            data: "one\n".into(),
        });
        let first = command.snapshot(0);
        let cursor = first["next_offset"].as_u64().unwrap() as usize;
        command.push(CommandEvent::Stdout {
            data: "two\n".into(),
        });
        let second = command.snapshot(cursor);
        assert_eq!(second["stdout"], "two\n");
        assert_eq!(second["next_offset"].as_u64().unwrap() as usize, 8);
    }

    /// 游标是参数，按字节走在多字节字符中间时退到字符边界，而不是 panic。
    #[tokio::test]
    async fn an_offset_inside_a_character_does_not_panic() {
        let command = open();
        command.push(CommandEvent::Stdout {
            data: "改动完成\n".into(),
        });
        let snapshot = command.snapshot(1);
        assert_eq!(snapshot["stdout"], "改动完成\n");
    }

    /// 等待：没到时立刻答，到时答，完成了不等。
    #[tokio::test]
    async fn waiting_returns_when_the_command_ends_or_the_wait_runs_out() {
        let command = open();
        let started = std::time::Instant::now();
        command.wait(0).await;
        assert!(started.elapsed() < std::time::Duration::from_millis(200));

        let waiting = {
            let command = command.clone();
            tokio::spawn(async move { command.wait(30).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        command.finish(Ending::Exited(0));
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("finishing the command releases the waiter")
            .unwrap();

        // Already finished: a waiter does not wait again.
        let started = std::time::Instant::now();
        command.wait(30).await;
        assert!(started.elapsed() < std::time::Duration::from_millis(200));
    }

    /// 书是有界的：只淘汰已经结束的那些，运行中的一条都不丢。
    #[test]
    fn the_book_drops_old_finished_commands_and_never_a_running_one() {
        let book = Book::default();
        let mut kept = Vec::new();
        for _ in 0..RETAINED + 5 {
            let command = Command::new(format!("cmd-{}", kept.len()));
            command.finish(Ending::Exited(0));
            kept.push(command.handle().to_string());
            book.insert(command);
        }
        assert!(
            book.get(&kept[0]).is_none(),
            "超过上限后最老的已完成命令应当被丢掉"
        );
        assert!(book.get(&kept[kept.len() - 1]).is_some());

        let running = Command::new("cmd-running".into());
        book.insert(running.clone());
        assert!(
            book.get("cmd-running").is_some(),
            "运行中的命令即便落在队头也不能被丢掉"
        );
    }

    /// 队头那条一直在跑时，它后面的已完成命令仍然要按上限淘汰——否则书的
    /// 「有界」只在那条命令结束之前成立。
    #[test]
    fn a_running_command_at_the_head_does_not_keep_the_book_growing() {
        let book = Book::default();
        book.insert(Command::new("cmd-head".into()));

        for i in 0..RETAINED + 5 {
            let command = Command::new(format!("cmd-{i}"));
            command.finish(Ending::Exited(0));
            book.insert(command);
        }

        assert!(
            book.get("cmd-head").is_some(),
            "运行中的那条从头到尾都在账上"
        );
        assert_eq!(
            book.inner.lock().unwrap().commands.len(),
            RETAINED,
            "书的条数不因队头那条一直运行而超过上限"
        );
        assert!(
            book.get("cmd-0").is_none() && book.get(&format!("cmd-{}", RETAINED + 4)).is_some(),
            "淘汰的仍然是已经结束的那些里最老的"
        );
    }
}
