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
//!
//! 跑完这件事本身也要有人说出来。一个人把一条长命令放到后台是为了继续说话，
//! 不是为了记得回来轮询；命令结束的那一刻，这一段对话应当收到一条消息，而不是
//! 等模型下一次碰巧调用 `command_output` 才发现。所以这里除了那本句柄簿，还有
//! 一条「跑完了」的广播：只投一次（`finish` 先到的那次算数），带上是哪一段对话
//! 起的这条命令，由装配层转成那一段对话上的一帧——这里够不着网关，网关也够不着
//! 这本书，两样都只在装配层手上。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use cog_core::CommandEvent;
use tokio::sync::broadcast;

/// How many finished commands the book keeps, most recent last.
///
/// The output of a finished command is held until it is dropped, so an
/// unbounded book is an unbounded leak in a process that never restarts. Only
/// finished commands are dropped, and only the oldest; a handle that has aged
/// out is refused by name rather than answered with someone else's output.
// cap: fixed — 这本书只服务「刚发出去、还在被轮询」的那几条；几十条之外的读者要的已经不是「刚才那条」而是审计，而审计不在这本书里
// keep: 删掉后这本书没有上界，没人再读的输出一直占内存；由「只服务刚发出、还在被轮询的那几条」兜住
const RETAINED: usize = 64;

/// How much of a finished command's output the notice carries.
///
/// A notice is one sentence plus a card, not a second copy of the output: the
/// full text stays readable through `command_output` for as long as the handle
/// lives. The cut is by characters, not bytes, so a tail never splits a
/// multi-byte character.
// cap: fixed — 通知是「一句话 + 一张卡片」，不是第二份输出；全文由 command_output 读，这里的长度只决定卡片上能看见多少
// keep: 删掉后通知卡片上要放全文，卡片变成第二份输出；由「通知是一句话＋一张卡片」兜住
const NOTICE_TAIL_CHARS: usize = 400;

/// How many finished notices are held for a subscriber that is momentarily
/// behind. The assembly is the only subscriber and forwards each one as a chat
/// frame, which costs nothing to await — but a subscriber that stopped reading
/// (a panic, a blocked runtime) must not grow this queue without bound.
// cap: fixed — 这条环只吸收订阅者短暂的落后；装配层的转发不阻塞，落后意味着它已经不在了
// keep: 删掉后通知环无界，订阅者落后时事件一直占内存；由「只吸收短暂的落后」兜住
const NOTICE_RING: usize = 64;

/// 一条跑完的后台命令：它属于哪一段对话，和它跑完的样子。
#[derive(Debug, Clone)]
pub struct Finished {
    /// 起这条命令的那一段对话。`None` 是「没人说过这是哪一段」——嵌进别的进程
    /// 里的运行、或运行时不带会话号的那一轮。装配层不替它猜一段对话。
    pub session: Option<String>,
    /// 这条命令的结局，见 [`Command::completion_notice`]。
    pub notice: serde_json::Value,
}

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
    /// 起这条命令的那一段对话，由运行时的任务局部变量递进来（见
    /// [`crate::tools::caller_session`]）。工具本身不知道自己在哪段对话里被点了名。
    session: Option<String>,
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
    fn with_session(handle: String, session: Option<String>) -> Arc<Self> {
        let (done, done_rx) = tokio::sync::watch::channel(false);
        Arc::new(Self {
            handle,
            session,
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
            CommandEvent::Workdir { path, .. } => {
                *self.workdir.lock().unwrap() = Some(path);
                None
            }
            CommandEvent::Exit { code } => Some(code),
        }
    }

    /// 收尾，并且**只**说一次「跑完了」。
    ///
    /// 先到的那次算数：把结局换成第二次的，屏幕上就会多出第二条完成消息，而
    /// 这条命令只结束过一次。所以写入只在空槽上发生，广播跟着写入走。
    pub fn finish(&self, ending: Ending) {
        {
            let mut slot = self.ending.lock().unwrap();
            if slot.is_some() {
                return;
            }
            *slot = Some(ending);
        }
        cog_core::ignore_lost!(self.done.send(true));
        if let Some(notice) = self.completion_notice() {
            // 没有订阅者时 `send` 报错，那不是错：这个进程里没人接这条通知
            // （嵌入运行、或装配层没有起转发），命令本身仍然跑完并留在簿上。
            let _ = notices().send(Finished {
                session: self.session.clone(),
                notice,
            });
        }
    }

    /// 这条命令跑完的样子，一句话说得清；还在跑时是 `None`。
    ///
    /// 卡片要的是「结局 + 多大的动静 + 去哪儿读全文」：结局只有这里知道（流结束
    /// 那一步把退出码或失败原因交进来），动静是这条命令印了多少，全文仍由
    /// [`Command::snapshot`] 分页读回。把全文塞进通知，等于把一份内容发两遍。
    pub fn completion_notice(&self) -> Option<serde_json::Value> {
        let ending = self.ending()?;
        let stdout = self.stdout.lock().unwrap().clone();
        let stderr = self.stderr.lock().unwrap().clone();
        let elapsed_ms = self.started.elapsed().as_millis() as u64;
        let (tail, tail_truncated) = tail_of(&stdout, NOTICE_TAIL_CHARS);
        let (stderr_tail, _) = tail_of(&stderr, NOTICE_TAIL_CHARS);
        let (state, exit_code, error, head) = match &ending {
            Ending::Exited(0) => (
                "exited",
                serde_json::json!(0),
                serde_json::Value::Null,
                "结束".to_string(),
            ),
            Ending::Exited(code) => (
                "exited",
                serde_json::json!(code),
                serde_json::Value::Null,
                format!("结束，退出码 {code}"),
            ),
            Ending::Failed(why) => (
                "failed",
                serde_json::Value::Null,
                serde_json::json!(why),
                format!("没能跑完：{why}"),
            ),
        };
        // 结尾的空行不算「说了什么」：一次正常的编译输出末尾也是它们。
        let printed = stdout.trim_end();
        let summary = if printed.is_empty() {
            format!(
                "后台命令 {} {head}（用时 {:.1} 秒），没有输出。",
                self.handle,
                elapsed_ms as f64 / 1000.0
            )
        } else {
            let lines = printed.lines().count();
            format!(
                "后台命令 {} {head}（用时 {:.1} 秒，输出 {lines} 行）。用 command_output 读全文。",
                self.handle,
                elapsed_ms as f64 / 1000.0
            )
        };
        Some(serde_json::json!({
            "handle": self.handle,
            "state": state,
            "exit_code": exit_code,
            "error": error,
            "elapsed_ms": elapsed_ms,
            "stdout_bytes": stdout.len(),
            "tail": tail,
            "tail_truncated": tail_truncated,
            "stderr_tail": stderr_tail,
            "summary": summary,
        }))
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
        // wall: 等=后台命令自己结束 / 等不到=答复「还在跑」，命令仍归它自己写完 / 续跑=能，下一次轮询再看
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

fn notices() -> &'static broadcast::Sender<Finished> {
    static NOTICES: OnceLock<broadcast::Sender<Finished>> = OnceLock::new();
    NOTICES.get_or_init(|| broadcast::channel(NOTICE_RING).0)
}

/// 订阅「后台命令跑完」。装配层是唯一的订阅者：它同时握着这本书与那一段对话
/// 的帧，而这两样分别在这一层和网关里。
pub fn subscribe() -> broadcast::Receiver<Finished> {
    notices().subscribe()
}

/// The last `limit` characters of `text`, and whether anything was cut.
fn tail_of(text: &str, limit: usize) -> (String, bool) {
    let count = text.chars().count();
    if count <= limit {
        (text.to_string(), false)
    } else {
        (text.chars().skip(count - limit).collect(), true)
    }
}

/// Hand out a handle and file it. The caller runs the command and folds its
/// events in; the handle is readable from that moment, which is why the book
/// entry exists before the command has produced anything.
///
/// 这一段对话的号在这一刻记下来：跑完时它已经不在工具调用的现场了，而通知要
/// 找得到当初把它放到后台的那一段对话。
pub fn open() -> Arc<Command> {
    let handle = format!("cmd-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let command = Command::with_session(handle, crate::tools::caller_session());
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

    /// 一条不属于任何对话的命令：书只按句柄记账，会话号与淘汰无关。
    fn unnamed(handle: &str) -> Arc<Command> {
        Command::with_session(handle.to_string(), None)
    }

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
            let command = unnamed(&format!("cmd-{}", kept.len()));
            command.finish(Ending::Exited(0));
            kept.push(command.handle().to_string());
            book.insert(command);
        }
        assert!(
            book.get(&kept[0]).is_none(),
            "超过上限后最老的已完成命令应当被丢掉"
        );
        assert!(book.get(&kept[kept.len() - 1]).is_some());

        let running = unnamed("cmd-running");
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
        book.insert(unnamed("cmd-head"));

        for i in 0..RETAINED + 5 {
            let command = unnamed(&format!("cmd-{i}"));
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
