//! 检查点：把一棵工作树在某一刻的样子记下来，之后能显式退回去。
//!
//! 记的状态是**整棵树**，不是「这一轮工具写过的那几个文件」。差别不在实现难易，
//! 而在覆盖：写盘的工具不止 `write_file` / `edit_file` / `apply_patch` 三只——
//! `run_command` 里一条 `sed -i`、`cargo fmt`、`git checkout` 都会改文件，而它们不经过
//! 任何一只文件工具。只记文件工具碰过的路径，检查点就会在「这一轮其实还跑过命令」
//! 的那种轮次上退不干净，而读数上它和退干净的那一轮长得一模一样。
//!
//! 存储借的是工作树自己的 git：快照走一个临时索引（`GIT_INDEX_FILE`），把当前工作树
//! 连同未跟踪文件写成一棵树、包成一个提交，挂到 `refs/cogneva/checkpoints/<key>`。
//! 真正的索引、工作区与 HEAD 一个都不动——拍快照是「看一眼并记住」，不是一次提交。
//! 共用同一份仓库对象，所以重复的内容不占第二份空间。
//!
//! 退回同样是「只动工作区」：把快照里有的内容写回去、把快照里没有的文件删掉，
//! 不碰索引。于是退完之后 `git status` 会如实显示这棵树被改了——那正是要的：
//! 退回是一次**显式**的改树，它应当在人平时看树的地方看得见。

use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;

/// 预览里最多列几条。
///
/// 这是钉住的值（`cap: bound`）：预览是给人做决定用的，一条看不完的清单和没有
/// 清单一样；退回本身不受这个数限制（少列不等于少退），所以超过时清单只列前面
/// 这些条并在 `truncated` 上说明「还有，没列完」。
// cap: fixed — 给人看的预览长度；退回不按它砍，只砍清单
// keep: 删掉后一次退回要在弹窗里列出全部文件（可能上千），人读不完也点不到底；由「清单只列前面几条、其余由 truncated 说明」兜住
const MAX_LISTED_FILES: usize = 500;

/// 一次退回会改动的一行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// 仓库相对路径。`git diff` 只给这个形状，所以不会有路径逃出这棵树。
    pub path: String,
    /// `added`（现在有、检查点里没有，退回就是删掉）、`removed`（检查点里有、
    /// 现在没有，退回就是写回来）、`modified`（两边都有，内容或可执行位不同）。
    pub change: &'static str,
}

/// 一个检查点，或者一次退回会做的事。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub key: String,
    /// 快照落在哪个提交上。
    pub commit: String,
    pub files: Vec<Change>,
    /// 清单被上面那个上限截断了。
    pub truncated: bool,
    /// 退回去会丢掉的那几条**未提交**改动：它们在快照之后改过，且不在 HEAD 里。
    /// 退回去是显式改树，丢掉是它的本意；但丢掉哪些要读得出来，不能是静默覆盖。
    pub discarded: Vec<String>,
}

impl Plan {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "key": self.key,
            "commit": self.commit,
            "truncated": self.truncated,
            "discarded": self.discarded,
            "files": self.files.iter().map(|c| serde_json::json!({
                "path": c.path,
                "change": c.change,
            })).collect::<Vec<_>>(),
        })
    }
}

/// 检查点名字最长多少。
///
/// 这是**固定**的：名字只由「哪一段对话、第几轮」拼出来，长度由那两个数决定；
/// 更长的名字不比短的有用，而 git 的 ref 名本身有长度上限。
// cap: fixed — ref 名的一部分，不随读数变
// keep: 删掉后点名会把任意长的名字拼进 ref，git 拒收发生在更晚也更深的地方；由「git ref 名的长度上限」兜住
const MAX_KEY_LEN: usize = 200;

/// `key` 只允许这些字符。
///
/// 它会被拼进一条 ref 名与一条命令行：`../` 或者 `refs/heads/x` 这种形状进了 ref
/// 就不再是「这一段对话的检查点」，而是一条能改别人引用的写入。
pub fn key_is_well_formed(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_KEY_LEN
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
}

/// 名字落到 ref 上的形状。
///
/// 不直接拼 key：git 的 ref 名不允许 `:`（而「哪一段会话、第几轮」这个 key 里
/// 就有它），也不喜欢会话号里可能出现的别的字符。折成一个定长串，ref 名就只由
/// 本模块的这些字符构成，而同一个 key 每次都落到同一个 ref 上。
pub(crate) fn refname(key: &str) -> String {
    // FNV-1a：只求稳定与分布，不求密码学性质——它算的是一个本地名字，不是
    // 一道校验。两段不同的 key 撞在一起的代价是「同一棵树上少了一个检查点」，
    // 而每段会话的轮次本来就少。
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("refs/cogneva/checkpoints/{hash:016x}")
}

async fn git(root: &Path, args: &[&str], index: Option<&Path>) -> Result<String, String> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(root).args(args);
    cmd.stdin(Stdio::null());
    if let Some(index) = index {
        cmd.env("GIT_INDEX_FILE", index);
    }
    let out = cmd
        .output()
        .await
        .map_err(|e| format!("跑不了 git（{e}）：检查点做不了"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        return Err(format!(
            "git {} 失败（{}）：{stderr}",
            args.join(" "),
            out.status
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// 这棵树的 HEAD，没有提交时是 `None`（空仓库也要能拍快照：第一轮写进去的文件
/// 同样值得退回去）。
async fn head(root: &Path) -> Result<Option<String>, String> {
    match git(root, &["rev-parse", "--verify", "HEAD"], None).await {
        Ok(head) => Ok(Some(head)),
        Err(_) => Ok(None),
    }
}

fn scratch_index() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "cogneva-checkpoint-index-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

/// 当前工作树（跟一个临时索引走）的树对象。
///
/// 临时索引是关键：`git add -A` 会重写它，写在真正的索引上就是替人暂存了一堆
/// 他根本没打算提交的改动——拍快照这个动作不该在 `git status` 里留下痕迹。
async fn worktree_tree(root: &Path) -> Result<String, String> {
    let index = scratch_index();
    cog_core::ignore_lost!(std::fs::remove_file(&index));
    let result = async {
        match head(root).await? {
            Some(_) => git(root, &["read-tree", "HEAD"], Some(&index)).await?,
            None => git(root, &["read-tree", "--empty"], Some(&index)).await?,
        };
        git(root, &["add", "-A", "."], Some(&index)).await?;
        git(root, &["write-tree"], Some(&index)).await
    }
    .await;
    cog_core::ignore_lost!(std::fs::remove_file(&index));
    result
}

/// 记住这棵树现在这个样子。
pub async fn capture(root: &Path, key: &str) -> Result<Plan, String> {
    if !key_is_well_formed(key) {
        return Err(format!("检查点的名字「{key}」不能用"));
    }
    let tree = worktree_tree(root).await?;
    let mut tree_args = vec!["commit-tree", tree.as_str(), "-m"];
    let message = format!("cogneva checkpoint {key}");
    tree_args.push(message.as_str());
    let head_commit = head(root).await?;
    if let Some(head) = head_commit.as_deref() {
        tree_args.extend(["-p", head]);
    }
    let commit = git(root, &tree_args, None).await?;
    git(root, &["update-ref", &refname(key), &commit], None).await?;
    Ok(Plan {
        key: key.to_string(),
        commit,
        files: Vec::new(),
        truncated: false,
        discarded: Vec::new(),
    })
}

/// 这个检查点现在退回会改动什么。
pub async fn plan(root: &Path, key: &str) -> Result<Plan, String> {
    if !key_is_well_formed(key) {
        return Err(format!("检查点的名字「{key}」不能用"));
    }
    let commit = git(root, &["rev-parse", "--verify", &refname(key)], None)
        .await
        .map_err(|_| format!("没有名字叫「{key}」的检查点"))?;
    let current = worktree_tree(root).await?;
    let raw = git(
        root,
        &["diff", "--name-status", "-z", &commit, &current],
        None,
    )
    .await?;
    let mut files = Vec::new();
    let mut fields = raw.split('\0').filter(|f| !f.is_empty());
    while let Some(status) = fields.next() {
        let Some(path) = fields.next() else { break };
        let change = match status.chars().next() {
            // A：现在有、快照里没有 ⇒ 退回就是删掉。
            Some('A') => "added",
            // D：快照里有、现在没有 ⇒ 退回就是写回来。
            Some('D') => "removed",
            // M/T 都是「两边都有但不一样」；T 是类型变了（文件变符号链接之类），
            // 对退回来说和内容不同是同一件事。
            _ => "modified",
        };
        files.push(Change {
            path: path.to_string(),
            change,
        });
    }
    let truncated = files.len() > MAX_LISTED_FILES;
    files.truncate(MAX_LISTED_FILES);
    Ok(Plan {
        key: key.to_string(),
        commit,
        files,
        truncated,
        discarded: Vec::new(),
    })
}

/// 这棵树里每个路径的（模式，对象）。
async fn snapshot_entries(
    root: &Path,
    commit: &str,
) -> Result<Vec<(String, String, String)>, String> {
    let raw = git(root, &["ls-tree", "-r", "-z", "--full-tree", commit], None).await?;
    let mut out = Vec::new();
    for line in raw.split('\0').filter(|l| !l.is_empty()) {
        // 形如 `100644 blob <sha>\t<path>`。
        let (meta, path) = match line.split_once('\t') {
            Some(parts) => parts,
            None => continue,
        };
        let mut fields = meta.split_whitespace();
        let (Some(mode), Some(_kind), Some(sha)) = (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        out.push((path.to_string(), mode.to_string(), sha.to_string()));
    }
    Ok(out)
}

/// 把工作树放回 `key` 那一份快照的样子。
///
/// 只写工作区与文件系统，不碰索引、HEAD 与分支：退完之后 `git status` 会把差别
/// 如实报出来——退回是一次显式的改树，不该同时替人把改动暂存好。
pub async fn restore(root: &Path, key: &str) -> Result<Plan, String> {
    let mut plan = plan(root, key).await?;
    // 先判一次这一步会永久失去什么：受影响路径里，当前内容既不在快照里、也不在
    // HEAD 里的那些，退回去就再也取不回。退回去仍然是本意，但丢掉的是谁必须点名。
    if !plan.files.is_empty() {
        let mut args: Vec<String> = vec![
            "status".into(),
            "--porcelain".into(),
            "-z".into(),
            "--".into(),
        ];
        args.extend(plan.files.iter().map(|c| c.path.clone()));
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        if let Ok(status) = git(root, &refs, None).await {
            for entry in status.split('\0') {
                if entry.len() > 3 {
                    let path = entry[3..].trim().to_string();
                    if !path.is_empty() {
                        tracing::warn!(
                            path = %path,
                            "restore discards uncommitted content that neither the snapshot nor HEAD holds"
                        );
                        plan.discarded.push(path);
                    }
                }
            }
        }
    }
    let plan = plan;
    let entries = snapshot_entries(root, &plan.commit).await?;
    let wanted: std::collections::HashMap<&str, (&str, &str)> = entries
        .iter()
        .map(|(path, mode, sha)| (path.as_str(), (mode.as_str(), sha.as_str())))
        .collect();
    for change in &plan.files {
        let target = root.join(&change.path);
        match change.change {
            "added" => {
                if let Err(e) = std::fs::remove_file(&target) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(format!("{} 删不掉：{e}", change.path));
                    }
                }
                remove_empty_parents(root, &target);
            }
            _ => {
                let Some((mode, sha)) = wanted.get(change.path.as_str()).copied() else {
                    return Err(format!("{} 的快照对象缺失", change.path));
                };
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("{} 的目录建不出来：{e}", change.path))?;
                }
                // 走字节而不是字符串：快照里可能是二进制，按行收尾会把内容改坏。
                let bytes = raw_blob(root, sha).await?;
                if std::fs::symlink_metadata(&target).is_ok() {
                    cog_core::ignore_lost!(std::fs::remove_file(&target));
                }
                if mode == "120000" {
                    #[cfg(unix)]
                    {
                        use std::os::unix::ffi::OsStrExt;
                        let link = std::ffi::OsStr::from_bytes(&bytes);
                        std::os::unix::fs::symlink(link, &target)
                            .map_err(|e| format!("{} 的符号链接写不回去：{e}", change.path))?;
                    }
                } else {
                    std::fs::write(&target, &bytes)
                        .map_err(|e| format!("{} 写不回去：{e}", change.path))?;
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let executable = mode == "100755";
                        let mut perms = std::fs::metadata(&target)
                            .map_err(|e| format!("{} 读不回来：{e}", change.path))?
                            .permissions();
                        perms.set_mode(if executable { 0o755 } else { 0o644 });
                        cog_core::ignore_lost!(std::fs::set_permissions(&target, perms));
                    }
                }
            }
        }
    }
    Ok(plan)
}

/// 一个 blob 的原始字节。
///
/// 不复用上面的 [`git`]：它把 stdout 当字符串并按行收尾，还原回去的二进制文件
/// 会被悄悄改掉几个字节，而「退回去了」与「退回去但内容不对」在屏幕上同形。
async fn raw_blob(root: &Path, sha: &str) -> Result<Vec<u8>, String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["cat-file", "blob", sha])
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| format!("读不了快照对象 {sha}：{e}"))?;
    if !out.status.success() {
        return Err(format!("读不了快照对象 {sha}：{}", out.status));
    }
    Ok(out.stdout)
}

fn remove_empty_parents(root: &Path, path: &Path) {
    let mut current = path.parent();
    while let Some(dir) = current {
        if dir == root || !dir.starts_with(root) {
            break;
        }
        if std::fs::remove_dir(dir).is_err() {
            break;
        }
        current = dir.parent();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        git(dir.path(), &["init", "-q"], None).await.expect("init");
        git(dir.path(), &["config", "user.email", "t@example.com"], None)
            .await
            .expect("email");
        git(dir.path(), &["config", "user.name", "t"], None)
            .await
            .expect("name");
        std::fs::write(dir.path().join(".gitignore"), "target/\n").expect("ignore");
        std::fs::write(dir.path().join("a.txt"), "one\n").expect("a");
        git(dir.path(), &["add", "-A", "."], None)
            .await
            .expect("add");
        git(dir.path(), &["commit", "-qm", "base"], None)
            .await
            .expect("commit");
        dir
    }

    #[tokio::test]
    async fn a_capture_does_not_touch_the_index_or_head() {
        let dir = repo().await;
        std::fs::write(dir.path().join("a.txt"), "two\n").expect("edit");
        let head = git(dir.path(), &["rev-parse", "HEAD"], None)
            .await
            .expect("head");
        capture(dir.path(), "s1:0").await.expect("capture");
        let after = git(dir.path(), &["rev-parse", "HEAD"], None)
            .await
            .expect("head");
        assert_eq!(head, after, "拍快照不该动 HEAD");
        let staged = git(dir.path(), &["diff", "--cached", "--name-only"], None)
            .await
            .expect("diff");
        assert!(staged.is_empty(), "拍快照不该替人暂存：{staged}");
    }

    #[tokio::test]
    async fn a_restore_puts_back_the_old_content_and_removes_what_appeared() {
        let dir = repo().await;
        std::fs::write(dir.path().join("a.txt"), "two\n").expect("edit");
        std::fs::write(dir.path().join("new.txt"), "new\n").expect("new");
        capture(dir.path(), "s1:0").await.expect("capture");
        std::fs::write(dir.path().join("a.txt"), "three\n").expect("edit");
        std::fs::write(dir.path().join("later.txt"), "later\n").expect("later");

        let plan = super::plan(dir.path(), "s1:0").await.expect("plan");
        assert!(
            plan.files
                .iter()
                .any(|c| c.path == "a.txt" && c.change == "modified"),
            "a.txt 应当被列为改动：{plan:?}"
        );
        assert!(
            plan.files
                .iter()
                .any(|c| c.path == "later.txt" && c.change == "added"),
            "后来才出现的文件应当列为要删：{plan:?}"
        );

        restore(dir.path(), "s1:0").await.expect("restore");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("a.txt")).expect("read"),
            "two\n"
        );
        assert!(
            !dir.path().join("later.txt").exists(),
            "退回要删掉快照里没有的文件"
        );
        assert!(
            dir.path().join("new.txt").exists(),
            "快照里有的新文件要留着"
        );
    }

    #[tokio::test]
    async fn a_path_that_is_not_a_checkout_says_so() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = capture(dir.path(), "s1:0").await.expect_err("不该成功");
        assert!(error.contains("git"), "{error}");
    }

    #[tokio::test]
    async fn a_key_that_could_reach_another_ref_is_refused() {
        let dir = repo().await;
        assert!(capture(dir.path(), "refs/heads/main").await.is_err());
        assert!(capture(dir.path(), "../x").await.is_err());
        assert!(plan(dir.path(), "a/b").await.is_err());
    }

    #[tokio::test]
    async fn an_unknown_key_is_named_in_the_refusal() {
        let dir = repo().await;
        let error = plan(dir.path(), "s9:0").await.expect_err("不该成功");
        assert!(error.contains("s9:0"), "{error}");
    }

    #[tokio::test]
    async fn ignored_files_are_not_part_of_the_snapshot() {
        let dir = repo().await;
        std::fs::create_dir_all(dir.path().join("target")).expect("dir");
        std::fs::write(dir.path().join("target/x"), "built\n").expect("write");
        capture(dir.path(), "s1:0").await.expect("capture");
        let plan = super::plan(dir.path(), "s1:0").await.expect("plan");
        assert!(
            !plan.files.iter().any(|c| c.path.starts_with("target/")),
            "忽略目录不进快照：{plan:?}"
        );
    }
    /// 退回去会丢掉的未提交改动必须点名，不能静默覆盖。
    ///
    /// 没有这次改动这条会红：快照之后改的那一行会被退回直接覆盖，`Plan` 里没有
    /// 任何一格说出「刚才那份内容再也不在了」。
    #[tokio::test]
    async fn a_restore_names_the_uncommitted_content_it_discards() {
        let dir = repo().await;
        capture(dir.path(), "cp1").await.expect("capture");
        std::fs::write(dir.path().join("a.txt"), "two\n").expect("edit");
        let plan = restore(dir.path(), "cp1").await.expect("restore");
        assert!(
            plan.discarded.iter().any(|p| p == "a.txt"),
            "退回去丢掉的未提交内容没有点名：{:?}",
            plan.discarded
        );
        let clean = restore(dir.path(), "cp1").await.expect("restore again");
        assert!(
            clean.discarded.is_empty(),
            "干净的树上退回不该报丢弃：{:?}",
            clean.discarded
        );
    }
}
