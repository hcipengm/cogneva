//! E25：对话里看得见检查点，能把文件状态退回到某一轮之前；退回是显式动作，
//! 不会静默改树。
//!
//! 判的不是「有没有一个叫检查点的接口」，是三件看得见的事：
//!
//! 1. **看得见**：这一段对话记下了哪几轮结束时的树，清单读得回来；
//! 2. **先算后改**：只问不退的那一次，一个字节都不碰，清单说的是会改什么；
//! 3. **退了就是退了**：人点下确认之后，文件字节回到那一轮之前，那一轮之后才
//!    出现的文件被删掉，之后的检查点记录也不再指着一段已经不存在的历史。
//!
//! 树用一棵真的 git 仓库：检查点拍的是整棵树，替身回话只能证明「消息被发出
//! 去了」，证明不了「退回来的是那份内容」。执行器那一跳由一个只认三种检查点
//! 载荷的后端接住，落到 `cog_extension::checkpoints`——与生产上远程执行器那一
//! 层做的是同一件事。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use cog_core::{
    KeyValueStore, SFError, SFResult, SandboxBackend, SandboxPayload, SandboxRequest, SandboxResult,
};
use cog_gateway::chat::ConversationTurn;
use cogneva::checkpoints::{self, Row};
use cogneva::conversation::{Ledgers, PlatformConversation};

/// 这一段测试用的键值存储：字符串、哈希、集合三种形状都有。
///
/// 不用 `common::kv::MemoryStore`：那个替身只实现额度层级要的字符串形状，哈希
/// 字段一律 `unreachable!`；而这一段对话的「谁在说话」正是按属主记在哈希里的。
/// 用一个只实现一半的替身，红的时候分不清是替身没实现还是检查点错了。
#[derive(Default)]
struct Store {
    rows: Mutex<HashMap<String, String>>,
    hashes: Mutex<HashMap<String, HashMap<String, String>>>,
    sets: Mutex<HashMap<String, std::collections::HashSet<String>>>,
}

#[async_trait::async_trait]
impl KeyValueStore for Store {
    async fn get(&self, key: &str) -> SFResult<Option<String>> {
        Ok(self.rows.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> SFResult<()> {
        self.rows
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, _ttl_secs: u64) -> SFResult<()> {
        self.set(key, value).await
    }

    async fn del(&self, key: &str) -> SFResult<()> {
        self.rows.lock().unwrap().remove(key);
        Ok(())
    }

    async fn expire(&self, _key: &str, _ttl_secs: i64) -> SFResult<()> {
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> SFResult<i64> {
        Ok(-1)
    }

    async fn incr(&self, key: &str, by: i64) -> SFResult<i64> {
        let mut rows = self.rows.lock().unwrap();
        let next = rows
            .get(key)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
            + by;
        rows.insert(key.to_string(), next.to_string());
        Ok(next)
    }

    async fn incr_ex(&self, key: &str, by: i64, _ttl_secs: u64) -> SFResult<i64> {
        self.incr(key, by).await
    }

    async fn decr(&self, key: &str, by: i64) -> SFResult<i64> {
        self.incr(key, -by).await
    }

    async fn sadd(&self, key: &str, member: &str) -> SFResult<()> {
        self.sets
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_default()
            .insert(member.to_string());
        Ok(())
    }

    async fn srem(&self, key: &str, member: &str) -> SFResult<()> {
        if let Some(set) = self.sets.lock().unwrap().get_mut(key) {
            set.remove(member);
        }
        Ok(())
    }

    async fn smembers(&self, key: &str) -> SFResult<Vec<String>> {
        Ok(self
            .sets
            .lock()
            .unwrap()
            .get(key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default())
    }

    async fn hset(&self, key: &str, field: &str, value: &str) -> SFResult<()> {
        self.hashes
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_default()
            .insert(field.to_string(), value.to_string());
        Ok(())
    }

    async fn hget(&self, key: &str, field: &str) -> SFResult<Option<String>> {
        Ok(self
            .hashes
            .lock()
            .unwrap()
            .get(key)
            .and_then(|hash| hash.get(field).cloned()))
    }

    async fn hdel(&self, key: &str, field: &str) -> SFResult<()> {
        if let Some(hash) = self.hashes.lock().unwrap().get_mut(key) {
            hash.remove(field);
        }
        Ok(())
    }

    async fn hvals(&self, key: &str) -> SFResult<Vec<String>> {
        Ok(self
            .hashes
            .lock()
            .unwrap()
            .get(key)
            .map(|hash| hash.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn hgetall(&self, key: &str) -> SFResult<Vec<(String, String)>> {
        Ok(self
            .hashes
            .lock()
            .unwrap()
            .get(key)
            .map(|hash| hash.iter().map(|(f, v)| (f.clone(), v.clone())).collect())
            .unwrap_or_default())
    }

    async fn hlen(&self, key: &str) -> SFResult<u64> {
        Ok(self
            .hashes
            .lock()
            .unwrap()
            .get(key)
            .map(|hash| hash.len() as u64)
            .unwrap_or(0))
    }
}

fn store() -> Arc<dyn KeyValueStore> {
    Arc::new(Store::default())
}

/// 一棵真树上的执行器，只接检查点这三种载荷。
struct Tree {
    root: PathBuf,
}

#[async_trait::async_trait]
impl SandboxBackend for Tree {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        let outcome = match &req.payload {
            SandboxPayload::CaptureCheckpoint { key } => {
                cog_extension::checkpoints::capture(&self.root, key).await
            }
            SandboxPayload::PlanCheckpoint { key } => {
                cog_extension::checkpoints::plan(&self.root, key).await
            }
            SandboxPayload::RestoreCheckpoint { key } => {
                cog_extension::checkpoints::restore(&self.root, key).await
            }
            other => {
                return Err(SFError::Agent(format!(
                    "这条测试只发检查点载荷，手上这份是 {other:?}"
                )))
            }
        };
        match outcome {
            Ok(plan) => Ok(SandboxResult {
                stdout: plan.to_json().to_string(),
                exit_code: 0,
                workdir: Some(self.root.display().to_string()),
                ..Default::default()
            }),
            Err(error) => Ok(SandboxResult {
                stderr: error,
                exit_code: 1,
                workdir: Some(self.root.display().to_string()),
                ..Default::default()
            }),
        }
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(SFError::Agent("检查点不走 wasm".into()))
    }
}

fn conversation(
    sandbox: Option<Arc<dyn SandboxBackend>>,
    people: Option<Arc<dyn KeyValueStore>>,
) -> PlatformConversation {
    use crate::common::orchestrator::control;

    PlatformConversation::new(
        control("test-workspace"),
        None,
        None,
        None,
        None,
        None,
        people,
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

async fn git(root: &std::path::Path, args: &[&str]) {
    let status = tokio::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .stdin(std::process::Stdio::null())
        .status()
        .await
        .expect("git runs");
    assert!(status.success(), "git {args:?} failed");
}

/// 一棵提交过一次的 git 仓库，`a.txt` 里是 `one`。
async fn repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    git(dir.path(), &["init", "-q"]).await;
    git(dir.path(), &["config", "user.email", "t@example.com"]).await;
    git(dir.path(), &["config", "user.name", "t"]).await;
    std::fs::write(dir.path().join("a.txt"), "one\n").expect("a");
    git(dir.path(), &["add", "-A", "."]).await;
    git(dir.path(), &["commit", "-qm", "base"]).await;
    dir
}

async fn capture(tree: &Tree, kv: &Arc<dyn KeyValueStore>, session: &str, turn: usize) {
    let key = checkpoints::key(session, turn);
    let request = SandboxRequest {
        task_id: session.to_string(),
        agent_id: "t".into(),
        payload: SandboxPayload::CaptureCheckpoint { key: key.clone() },
        input: serde_json::Value::Null,
        limits: Default::default(),
    };
    tree.execute(&request).await.expect("capture runs");
    checkpoints::remember(
        kv,
        "alice",
        session,
        Row {
            key,
            turn,
            at: chrono::Utc::now().to_rfc3339(),
        },
    )
    .await
    .expect("row saved");
}

/// 只问不退的那一次：清单说得出会改什么，而树一个字节都不动。
///
/// 这一条是「不会静默改树」这句话的判据：把 `apply` 那一步提前到预览里，
/// 它当场红。
#[tokio::test]
async fn asking_what_a_rewind_would_do_changes_no_bytes() {
    let dir = repo().await;
    let tree = Tree {
        root: dir.path().to_path_buf(),
    };
    let kv = store();
    capture(&tree, &kv, "s-1", 0).await;

    std::fs::write(dir.path().join("a.txt"), "two\n").expect("edit");
    capture(&tree, &kv, "s-1", 1).await;
    std::fs::write(dir.path().join("a.txt"), "three\n").expect("edit");
    std::fs::write(dir.path().join("later.txt"), "later\n").expect("new");

    let turns = conversation(Some(Arc::new(tree)), Some(Arc::clone(&kv)));
    let report = turns
        .rewind("alice", "s-1", 2, false)
        .await
        .expect("预览读得回来");
    assert_eq!(report.target, 1, "退回第 2 轮之前用的是第 1 轮结束时的树");
    assert!(!report.applied, "预览那一步报的是「没改」");
    assert!(
        report
            .files
            .iter()
            .any(|file| file.path == "a.txt" && file.change == "modified"),
        "预览要列出 a.txt 会被改回来：{report:?}"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("a.txt")).expect("read"),
        "three\n",
        "只问不退时树被改了：退回就不再是一个显式动作"
    );
    assert!(
        dir.path().join("later.txt").exists(),
        "只问不退时文件被删了"
    );
}

/// 人点下确认之后：字节回到那一轮之前，之后才出现的文件删掉，之后的记录也去掉。
///
/// 三条一起判，因为它们回答的是同一个问题——「退回去」是不是真的把这棵树带回了
/// 那一刻。只判前两条，记录里会留着指向一段已经不存在的历史的快照，下一次退回
/// 时有人会挑到一份「退到这里也一样」。
#[tokio::test]
async fn confirming_puts_the_tree_back_to_before_that_turn() {
    let dir = repo().await;
    let tree = Tree {
        root: dir.path().to_path_buf(),
    };
    let kv = store();
    capture(&tree, &kv, "s-1", 0).await;

    std::fs::write(dir.path().join("a.txt"), "two\n").expect("edit");
    capture(&tree, &kv, "s-1", 1).await;
    capture(&tree, &kv, "s-1", 2).await;

    std::fs::write(dir.path().join("a.txt"), "three\n").expect("edit");
    std::fs::write(dir.path().join("later.txt"), "later\n").expect("new");

    let turns = conversation(Some(Arc::new(tree)), Some(Arc::clone(&kv)));
    let report = turns
        .rewind("alice", "s-1", 2, true)
        .await
        .expect("退回跑得下去");
    assert!(report.applied, "这一报说树已经改了");

    assert_eq!(
        std::fs::read_to_string(dir.path().join("a.txt")).expect("read"),
        "two\n",
        "退回到第 2 轮之前，a.txt 应当是第 1 轮结束时的样子"
    );
    assert!(
        !dir.path().join("later.txt").exists(),
        "那一轮之后才出现的文件要删掉"
    );

    let rows = checkpoints::load(&kv, "alice", "s-1").await.expect("rows");
    assert!(
        rows.rows.iter().all(|row| row.turn <= 1),
        "退回去之后，后面那几轮的记录还留着：{rows:?}"
    );
}

/// 清单读得回来：清单里是哪几轮，读到的就是哪几轮。
#[tokio::test]
async fn the_conversation_lists_the_snapshots_it_kept() {
    let dir = repo().await;
    let tree = Tree {
        root: dir.path().to_path_buf(),
    };
    let kv = store();
    capture(&tree, &kv, "s-1", 0).await;
    capture(&tree, &kv, "s-1", 1).await;
    capture(&tree, &kv, "s-2", 0).await;

    let turns = conversation(Some(Arc::new(tree)), Some(Arc::clone(&kv)));
    let list = turns.checkpoints("alice", "s-1").await.expect("list");
    let turns_listed: Vec<usize> = list.rows.iter().map(|row| row.turn).collect();
    assert_eq!(turns_listed, vec![0, 1], "清单里是这一段自己的那几轮");
    assert_eq!(list.hidden, 0);

    let other = turns.checkpoints("alice", "s-2").await.expect("list");
    assert_eq!(
        other.rows.iter().map(|row| row.turn).collect::<Vec<_>>(),
        vec![0],
        "另一段的检查点不该出现在这一段的清单里"
    );
}

/// 没接执行器的部署说得出来；没接存储的部署也说得出来。两种「做不了」各自
/// 点名缺的是哪一件，而不是都给一张空清单。
#[tokio::test]
async fn a_deployment_without_an_executor_says_so() {
    let kv = store();
    let turns = conversation(None, Some(Arc::clone(&kv)));
    let error = turns
        .rewind("alice", "s-1", 1, false)
        .await
        .expect_err("没有执行器时不该回一句成功");
    assert!(error.contains("执行器"), "{error}");
    assert_eq!(
        turns.checkpoints("alice", "s-1").await,
        Ok(Default::default()),
        "有存储、还没跑过一轮：这是一张空清单，不是「做不了」"
    );

    let unwired = conversation(None, None);
    let error = unwired
        .checkpoints("alice", "s-1")
        .await
        .expect_err("没有存储时不该回一张空清单");
    assert!(error.contains("检查点"), "{error}");
}

/// 没有那一份快照时点名说清楚，而不是退到一个随便挑的位置。
#[tokio::test]
async fn a_turn_with_no_snapshot_behind_it_is_named() {
    let dir = repo().await;
    let tree = Tree {
        root: dir.path().to_path_buf(),
    };
    let kv = store();
    capture(&tree, &kv, "s-1", 0).await;

    let turns = conversation(Some(Arc::new(tree)), Some(Arc::clone(&kv)));
    let error = turns
        .rewind("alice", "s-1", 5, false)
        .await
        .expect_err("第 5 轮之前没有快照");
    assert!(error.contains('4'), "拒的那句话要点名缺的是哪一轮：{error}");
}

/// 界面那一侧：退回分两步，先问不改、再点确认才改。
///
/// 判的是两件事而不是「有没有一个按钮」：没有第一步，点一下树就变了；没有第二步
/// 的确认，第一步的清单就没有用处。两条缺一条，这段门当场红。
#[test]
fn the_page_asks_before_it_changes_the_tree() {
    let pane = crate::common::routes::read_repo("web/src/conversation/ConversationPane.tsx");
    let preview = pane
        .find("apply: false")
        .expect("界面没有「只问不改」那一步：点一下树就变了");
    let confirm = pane
        .find("apply: true")
        .expect("界面没有真的改树那一步：清单给了也没人能点");
    assert!(
        preview < confirm,
        "确认那一步排在了预览之前：清单还没看到，树已经被改了"
    );
    assert!(
        pane.contains("确认退回"),
        "确认按钮上没写它要做什么：人点的是一个不知道后果的按钮"
    );
    let protocol = crate::common::routes::read_repo("crates/cog-gateway/src/websocket_protocol.rs");
    assert!(
        protocol.contains("ChatCheckpoint"),
        "协议里没有检查点这一条：界面发出去的那句话没有收件人"
    );
}
