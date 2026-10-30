//! 源代码管理面：用户工作区里那本 git 历史。
//!
//! 这一面只碰**本地**的 git：没有远端、没有 fetch/push。工作区属于一个用户，
//! 它的历史就该留在它自己的卷上。给一个没有凭证的容器开一条推到任意远端的通道，
//! 换来的不是能力，是把别人的仓库当投递箱。
//!
//! 每个动作都是 `git -C <用户根> …`，路径参数先过 `ide::resolve_under`：出根的
//! 路径在起进程之前就被拒，而不是让 git 自己去判断。子进程走
//! `git_mirror::spawn_git_group`（自成进程组，future 被丢掉就整组收尾），并且
//! 每个动作都有硬超时——一个坏掉的仓库不该把网关的请求一起拖住。
//!
//! 读写的边界与编辑器一致：`.git` 目录在 `ide::resolve_under` 里就被拒，所以
//! 钩子、配置这些能变成命令执行的东西写不进去；这一面再在每条命令上把它们关掉。

use axum::{
    extract::{Extension, Query},
    Json,
};
use cog_core::Claims;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use crate::error::ApiError;
use crate::ide::{ensure_user_root, resolve_under};

/// 一个 git 动作的硬上限。工作区是本机小仓库，慢过一次轮询周期就说明它坏了。
// cap: bound
const GIT_TIMEOUT: Duration = Duration::from_secs(20);
/// 一次点名多少条路径。
// cap: bound
const MAX_PATHS: usize = 500;
/// 状态里最多回多少条变更。
// cap: bound
const MAX_CHANGES: usize = 500;
/// 提交说明的上限。留得比一条 changelog 宽，比一篇文章窄。
// cap: bound
const MAX_MESSAGE_CHARS: usize = 4000;
/// 一次 diff 最多回多少字节、多少行。
// cap: bound
const MAX_DIFF_BYTES: usize = 2 * 1024 * 1024;
// cap: bound — keeps a pathological line count from filling the byte cap with tiny lines; the byte cap is the primary bound
const MAX_DIFF_LINES: usize = 20_000;
/// 日志默认与最多回多少条。
// cap: bound
const LOG_DEFAULT: usize = 20;
// cap: bound — most log rows returned
const LOG_MAX: usize = 200;

#[derive(Debug, Deserialize)]
pub struct DiffQuery {
    pub path: String,
    /// 看暂存区那一份（`--cached`）而不是工作区那一份。
    #[serde(default)]
    pub staged: bool,
}

#[derive(Debug, Deserialize)]
pub struct LogQuery {
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct PathsBody {
    pub paths: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CommitBody {
    pub message: String,
}

/// 一个文件的一次变更。两个字母列被拆成两条记录：同一个文件可以既在暂存区
/// 又在工作区各改过一次，那就该在两边都出现——合成一条时「点了暂存，它还留在
/// 未暂存里」会被读成失败。
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ScmChange {
    pub path: String,
    /// `M` 改、`A` 加、`D` 删、`R` 改名、`C` 复制、`U` 冲突、`?` 未跟踪。
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct ScmStatus {
    /// 这个工作区是不是一个 git 仓库。不是时下面各格都空，前端据此给「初始化」。
    pub repo: bool,
    pub branch: Option<String>,
    /// 当前提交的短 id；还没有任何提交时为空。
    pub head: Option<String>,
    pub staged: Vec<ScmChange>,
    pub unstaged: Vec<ScmChange>,
    /// 变更条数到了上限，这里只是前一部分。
    pub truncated: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DiffLine {
    /// `context` / `add` / `del`
    pub kind: String,
    pub old_no: Option<usize>,
    pub new_no: Option<usize>,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct DiffHunk {
    pub header: String,
    pub old_start: usize,
    pub old_lines: usize,
    pub new_start: usize,
    pub new_lines: usize,
    pub lines: Vec<DiffLine>,
}

#[derive(Debug, Serialize)]
pub struct FileDiff {
    pub path: String,
    pub staged: bool,
    /// 两边都没有可比的正文（二进制文件，或只是模式变了）。
    pub binary: bool,
    pub hunks: Vec<DiffHunk>,
    pub added: usize,
    pub removed: usize,
    /// 输出被上限截断，后面还有。
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct CommitInfo {
    pub id: String,
    pub short: String,
    pub author: String,
    /// 提交时刻（Unix 秒）。
    pub at: i64,
    pub subject: String,
}

#[derive(Debug, Serialize)]
pub struct CommitOut {
    pub commit: CommitInfo,
    pub status: ScmStatus,
}

#[derive(Debug, Serialize)]
pub struct LogOut {
    pub repo: bool,
    pub commits: Vec<CommitInfo>,
}

struct GitRun {
    code: i32,
    stdout: String,
    stderr: String,
}

/// 起一个 git 子进程并等它结束。
///
/// 全局选项逐条关掉那些「配置能把 git 变成别的东西」的入口：钩子目录指向
/// `/dev/null`、文件系统监视与提交签名一律关。这些配置项就写在仓库自己的
/// `.git/config` 里，而编辑器又写不进 `.git`——两边都关上，才不必假设那边
/// 一定没人去动它。
async fn run_git(root: &Path, args: &[&str], accept: &[i32]) -> Result<GitRun, ApiError> {
    let root_str = root
        .to_str()
        .ok_or_else(|| ApiError::internal("workspace path is not valid utf-8"))?;
    let mut full: Vec<&str> = vec![
        "-C",
        root_str,
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "commit.gpgsign=false",
    ];
    full.extend_from_slice(args);

    let (child, mut guard) = crate::git_mirror::spawn_git_group(
        Path::new("git"),
        &full,
        None,
        Stdio::piped(),
        Stdio::piped(),
    )
    .map_err(|e| ApiError::internal(format!("cannot start git: {e}")))?;

    match tokio::time::timeout(GIT_TIMEOUT, child.wait_with_output()).await {
        Ok(Ok(out)) => {
            // 进程已经收尾：pid 可能被复用，再对那个组发信号就会误伤别人。
            guard.disarm();
            let run = GitRun {
                code: out.status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&out.stdout).to_string(),
                stderr: String::from_utf8_lossy(&out.stderr).to_string(),
            };
            if accept.contains(&run.code) {
                Ok(run)
            } else {
                Err(ApiError::bad_request(format!(
                    "git {} failed ({}): {}",
                    args.first().copied().unwrap_or(""),
                    run.code,
                    run.stderr.trim()
                )))
            }
        }
        Ok(Err(e)) => Err(ApiError::internal(format!("git did not finish: {e}"))),
        // 超时：守卫在离开作用域时对整组发 SIGKILL，git 自己起的孙子进程
        // （打包、分页器）不会在网关里留一条后台链。
        Err(_) => Err(ApiError::internal(format!(
            "git timed out after {}s",
            GIT_TIMEOUT.as_secs()
        ))),
    }
}

async fn is_repo(root: &Path) -> Result<bool, ApiError> {
    let run = run_git(root, &["rev-parse", "--git-dir"], &[0, 128]).await?;
    Ok(run.code == 0)
}

/// `git status` 的两列。纯函数：一行 porcelain 记录进、两条记录出。
///
/// X 列是暂存区相对 HEAD 的差异，Y 列是工作区相对暂存区的差异。`??` 是未跟踪，
/// 它只属于工作区那一侧——把它也塞进暂存区会凭空多出一条「可以提交」的假账。
fn split_changes(rows: &[String]) -> (Vec<ScmChange>, Vec<ScmChange>, bool) {
    let mut staged = Vec::new();
    let mut unstaged = Vec::new();
    let mut truncated = false;
    let mut i = 0;
    while i < rows.len() {
        let row = &rows[i];
        i += 1;
        if row.len() < 4 {
            continue;
        }
        let mut chars = row.chars();
        let x = chars.next().unwrap_or(' ');
        let y = chars.next().unwrap_or(' ');
        // 前三个字节是 `XY `，都是 ASCII，从第 4 个字节切不会切坏多字节字符。
        let path = row[3..].to_string();
        // -z 下改名/复制是两条记录：先是新名字，紧跟一条旧名字，旧名字不再带状态列。
        if matches!(x, 'R' | 'C') || matches!(y, 'R' | 'C') {
            i += 1;
        }
        if staged.len() + unstaged.len() >= MAX_CHANGES {
            truncated = true;
            break;
        }
        if x == '?' {
            unstaged.push(ScmChange {
                path,
                code: "?".into(),
            });
            continue;
        }
        if x != ' ' {
            staged.push(ScmChange {
                path: path.clone(),
                code: x.to_string(),
            });
        }
        if y != ' ' {
            unstaged.push(ScmChange {
                path,
                code: y.to_string(),
            });
        }
    }
    (staged, unstaged, truncated)
}

/// `## main...origin/main [ahead 1]` 这类头一行里的分支名。
fn parse_branch(line: &str) -> Option<String> {
    let rest = line.strip_prefix("## ")?;
    if rest.starts_with("HEAD (no branch)") {
        return None;
    }
    // 还没有任何提交时 git 会写 `No commits yet on main`（更老的版本写
    // `Initial commit on main`）：摘掉这一层前缀，剩下的才是分支名。
    let rest = rest
        .strip_prefix("No commits yet on ")
        .or_else(|| rest.strip_prefix("Initial commit on "))
        .unwrap_or(rest);
    let name = rest
        .split("...")
        .next()
        .unwrap_or(rest)
        .split(" [")
        .next()
        .unwrap_or(rest)
        .trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

async fn git_status(root: &Path) -> Result<ScmStatus, ApiError> {
    if !is_repo(root).await? {
        return Ok(ScmStatus {
            repo: false,
            branch: None,
            head: None,
            staged: Vec::new(),
            unstaged: Vec::new(),
            truncated: false,
        });
    }

    let run = run_git(
        root,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--branch",
            "--untracked-files=all",
        ],
        &[0],
    )
    .await?;
    let mut rows: Vec<String> = run
        .stdout
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let branch = if rows.first().map(|r| r.starts_with("## ")).unwrap_or(false) {
        parse_branch(&rows.remove(0))
    } else {
        None
    };

    let head = run_git(root, &["rev-parse", "--short", "HEAD"], &[0, 128]).await?;
    let head = if head.code == 0 {
        let value = head.stdout.trim().to_string();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    } else {
        None
    };

    let (staged, unstaged, truncated) = split_changes(&rows);
    Ok(ScmStatus {
        repo: true,
        branch,
        head,
        staged,
        unstaged,
        truncated,
    })
}

/// 把一段 unified diff 切成 hunk 与逐行记录。
///
/// 行号由这里算：`@@ -a,b +c,d @@` 给的起点之后，每个上下文行两边一起加一，
/// 删除行只动旧行号、新增行只动新行号。让前端自己数的话，前端就有了第二份
/// 行号算法，而它一旦错位，整份 diff 的左右两列就对不上。
pub(crate) fn parse_unified_diff(text: &str) -> Vec<DiffHunk> {
    let mut hunks: Vec<DiffHunk> = Vec::new();
    let mut total = 0usize;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("@@ ") {
            if let Some((old_start, old_lines, new_start, new_lines)) = parse_hunk_header(rest) {
                hunks.push(DiffHunk {
                    header: line.to_string(),
                    old_start,
                    old_lines,
                    new_start,
                    new_lines,
                    lines: Vec::new(),
                });
            }
            continue;
        }
        let Some(hunk) = hunks.last_mut() else {
            // hunk 之前是文件头（`diff --git`、`index`、`---`、`+++`）：不是正文。
            continue;
        };
        if total >= MAX_DIFF_LINES {
            break;
        }
        // 这一 hunk 现在走到哪一行，由已写入的行重算：hunk 结构里只有起点与
        // 行数，没有游标位，硬塞一个字段会让序列化多出一个读者不认识的格。
        let (mut old_no, mut new_no) = hunk_cursor(hunk);
        let (kind, body) = if let Some(t) = line.strip_prefix('+') {
            ("add", t)
        } else if let Some(t) = line.strip_prefix('-') {
            ("del", t)
        } else if let Some(t) = line.strip_prefix(' ') {
            ("context", t)
        } else {
            // `\ No newline at end of file` 与空行：不占行号。
            continue;
        };
        let (old_field, new_field) = match kind {
            "add" => {
                new_no += 1;
                (None, Some(new_no - 1))
            }
            "del" => {
                old_no += 1;
                (Some(old_no - 1), None)
            }
            _ => {
                old_no += 1;
                new_no += 1;
                (Some(old_no - 1), Some(new_no - 1))
            }
        };
        let hunk = hunks.last_mut().expect("hunk was pushed above");
        hunk.lines.push(DiffLine {
            kind: kind.to_string(),
            old_no: old_field,
            new_no: new_field,
            text: body.to_string(),
        });
        total += 1;
    }
    hunks
}

fn hunk_cursor(hunk: &DiffHunk) -> (usize, usize) {
    let mut old_no = hunk.old_start;
    let mut new_no = hunk.new_start;
    for line in &hunk.lines {
        match line.kind.as_str() {
            "add" => new_no += 1,
            "del" => old_no += 1,
            _ => {
                old_no += 1;
                new_no += 1;
            }
        }
    }
    (old_no, new_no)
}

/// `-a,b +c,d @@` 里那四个数。省略 `,b` 表示 1 行。
fn parse_hunk_header(rest: &str) -> Option<(usize, usize, usize, usize)> {
    let (old, rest) = rest.split_once(' ')?;
    let rest = rest.trim_start();
    let (new, _) = rest.split_once(' ').unwrap_or((rest, ""));
    let old = old.strip_prefix('-')?;
    let new = new.strip_prefix('+')?;
    let pair = |s: &str| -> Option<(usize, usize)> {
        let (start, count) = match s.split_once(',') {
            Some((a, b)) => (a, b),
            None => (s, "1"),
        };
        Some((start.parse().ok()?, count.parse().ok()?))
    };
    let (old_start, old_lines) = pair(old)?;
    let (new_start, new_lines) = pair(new)?;
    Some((old_start, old_lines, new_start, new_lines))
}

async fn file_diff(root: &Path, rel: &str, staged: bool) -> Result<FileDiff, ApiError> {
    let tracked = run_git(
        root,
        &["ls-files", "--error-unmatch", "--", rel],
        &[0, 1, 128],
    )
    .await?;
    let in_index = tracked.code == 0;

    let run = if !in_index && !staged {
        // 未跟踪的文件不参与 `git diff`：对着 /dev/null 现算一份，语义与
        // 「提交时会新增它」一致（退出码 1 = 有差异，不是失败）。
        run_git(
            root,
            &[
                "diff",
                "--no-color",
                "--no-ext-diff",
                "--no-textconv",
                "-U3",
                "--no-index",
                "--",
                "/dev/null",
                rel,
            ],
            &[0, 1],
        )
        .await?
    } else {
        let mut args = vec![
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "-U3",
        ];
        if staged {
            args.push("--cached");
        }
        args.push("--");
        args.push(rel);
        run_git(root, &args, &[0]).await?
    };

    let mut text = run.stdout;
    let mut truncated = false;
    if text.len() > MAX_DIFF_BYTES {
        let mut cut = MAX_DIFF_BYTES;
        while cut > 0 && !text.is_char_boundary(cut) {
            cut -= 1;
        }
        text.truncate(cut);
        truncated = true;
    }

    let binary = !text.is_empty() && text.contains("Binary files");
    let hunks = if binary {
        Vec::new()
    } else {
        parse_unified_diff(&text)
    };
    let added = hunks
        .iter()
        .flat_map(|h| h.lines.iter())
        .filter(|l| l.kind == "add")
        .count();
    let removed = hunks
        .iter()
        .flat_map(|h| h.lines.iter())
        .filter(|l| l.kind == "del")
        .count();
    Ok(FileDiff {
        path: rel.to_string(),
        staged,
        binary,
        hunks,
        added,
        removed,
        truncated,
    })
}

async fn git_log(root: &Path, limit: usize) -> Result<Vec<CommitInfo>, ApiError> {
    if !is_repo(root).await? {
        return Ok(Vec::new());
    }
    let has_head = run_git(root, &["rev-parse", "--verify", "-q", "HEAD"], &[0, 1]).await?;
    if has_head.code != 0 {
        return Ok(Vec::new());
    }
    let limit = format!("-n{limit}");
    let run = run_git(
        root,
        &[
            "log",
            "--no-color",
            &limit,
            "--format=%H%x1f%h%x1f%an%x1f%at%x1f%s%x1e",
        ],
        &[0],
    )
    .await?;
    let mut commits = Vec::new();
    for record in run.stdout.split('\u{1e}') {
        let record = record.trim_matches('\n');
        if record.is_empty() {
            continue;
        }
        let mut parts = record.split('\u{1f}');
        let id = parts.next().unwrap_or("").to_string();
        let short = parts.next().unwrap_or("").to_string();
        let author = parts.next().unwrap_or("").to_string();
        let at = parts.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
        let subject = parts.next().unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        commits.push(CommitInfo {
            id,
            short,
            author,
            at,
            subject,
        });
    }
    Ok(commits)
}

fn check_paths(root: &Path, paths: &[String]) -> Result<(), ApiError> {
    if paths.is_empty() {
        return Err(ApiError::bad_request("no paths given"));
    }
    if paths.len() > MAX_PATHS {
        return Err(ApiError::bad_request(format!(
            "at most {MAX_PATHS} paths per request"
        )));
    }
    for rel in paths {
        if rel.trim().is_empty() {
            return Err(ApiError::bad_request("every path must be non-empty"));
        }
        resolve_under(root, rel)?;
    }
    Ok(())
}

/// 提交者身份取自令牌：这本历史属于谁，提交里就写着谁。
///
/// 名字里的换行与控制字符会破坏 git 的 header 结构，所以先洗掉；洗空的回落到
/// 用户 id——一条没有作者的提交在历史里是不可归因的。
fn author_of(claims: &Claims) -> (String, String) {
    let cleaned: String = claims
        .preferred_username
        .chars()
        .filter(|c| !c.is_control())
        .collect();
    let cleaned = cleaned.trim();
    let name = if cleaned.is_empty() {
        claims.sub.clone()
    } else {
        cleaned.to_string()
    };
    (name, format!("{}@users.cogneva.invalid", claims.sub))
}

pub async fn status_handler(
    Extension(claims): Extension<Claims>,
) -> Result<Json<ScmStatus>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    Ok(Json(git_status(&root).await?))
}

pub async fn log_handler(
    Extension(claims): Extension<Claims>,
    Query(query): Query<LogQuery>,
) -> Result<Json<LogOut>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let limit = query.limit.unwrap_or(LOG_DEFAULT).clamp(1, LOG_MAX);
    let repo = is_repo(&root).await?;
    let commits = if repo {
        git_log(&root, limit).await?
    } else {
        Vec::new()
    };
    Ok(Json(LogOut { repo, commits }))
}

pub async fn diff_handler(
    Extension(claims): Extension<Claims>,
    Query(query): Query<DiffQuery>,
) -> Result<Json<FileDiff>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    if query.path.trim().is_empty() {
        return Err(ApiError::bad_request("diff needs a path"));
    }
    resolve_under(&root, &query.path)?;
    Ok(Json(file_diff(&root, &query.path, query.staged).await?))
}

pub async fn init_handler(
    Extension(claims): Extension<Claims>,
) -> Result<Json<ScmStatus>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    if !is_repo(&root).await? {
        run_git(&root, &["init", "-q", "-b", "main"], &[0]).await?;
        tracing::info!(user = %claims.sub, "workspace git repository initialised");
    }
    Ok(Json(git_status(&root).await?))
}

pub async fn stage_handler(
    Extension(claims): Extension<Claims>,
    Json(body): Json<PathsBody>,
) -> Result<Json<ScmStatus>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    check_paths(&root, &body.paths)?;
    let mut args = vec!["add", "--"];
    args.extend(body.paths.iter().map(String::as_str));
    run_git(&root, &args, &[0]).await?;
    Ok(Json(git_status(&root).await?))
}

pub async fn unstage_handler(
    Extension(claims): Extension<Claims>,
    Json(body): Json<PathsBody>,
) -> Result<Json<ScmStatus>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    check_paths(&root, &body.paths)?;
    let has_head = run_git(&root, &["rev-parse", "--verify", "-q", "HEAD"], &[0, 1]).await?;
    if has_head.code == 0 {
        let mut args = vec!["restore", "--staged", "--"];
        args.extend(body.paths.iter().map(String::as_str));
        run_git(&root, &args, &[0]).await?;
    } else {
        // 还没有第一条提交时 `restore --staged` 没有可比的对象，把这几条从
        // 索引里摘掉就是「取消暂存」在这里唯一可做的事。
        let mut args = vec!["rm", "--cached", "--quiet", "--"];
        args.extend(body.paths.iter().map(String::as_str));
        run_git(&root, &args, &[0, 1]).await?;
    }
    Ok(Json(git_status(&root).await?))
}

pub async fn commit_handler(
    Extension(claims): Extension<Claims>,
    Json(body): Json<CommitBody>,
) -> Result<Json<CommitOut>, ApiError> {
    let root = ensure_user_root(&claims).await?;
    let message = body.message.trim().to_string();
    if message.is_empty() {
        return Err(ApiError::bad_request(
            "a commit needs a message; an empty one records nothing about why",
        ));
    }
    if message.chars().count() > MAX_MESSAGE_CHARS {
        return Err(ApiError::bad_request(format!(
            "commit message is longer than {MAX_MESSAGE_CHARS} characters"
        )));
    }
    let (name, email) = author_of(&claims);
    // `-c` 选项必须排在子命令之前，`run_git` 已经铺好了那些全局选项。
    let identity_name = format!("user.name={name}");
    let identity_email = format!("user.email={email}");
    let started = std::time::Instant::now();
    run_git(
        &root,
        &[
            "-c",
            &identity_name,
            "-c",
            &identity_email,
            "commit",
            "--no-verify",
            "--quiet",
            "-m",
            &message,
        ],
        &[0],
    )
    .await?;

    let head = run_git(&root, &["rev-parse", "HEAD"], &[0]).await?;
    let id = head.stdout.trim().to_string();
    let short: String = id.chars().take(7).collect();
    let commit = CommitInfo {
        id,
        short,
        author: name,
        at: chrono::Utc::now().timestamp(),
        subject: message.lines().next().unwrap_or("").to_string(),
    };
    tracing::info!(
        user = %claims.sub,
        commit = %commit.short,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "workspace commit recorded"
    );
    Ok(Json(CommitOut {
        commit,
        status: git_status(&root).await?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(s: &str) -> String {
        s.to_string()
    }

    #[test]
    fn the_two_columns_become_two_records_each_with_its_own_side() {
        let rows = vec![row("M  src/a.rs"), row(" M src/b.rs"), row("MM src/c.rs")];
        let (staged, unstaged, truncated) = split_changes(&rows);
        assert!(!truncated);
        assert_eq!(
            staged,
            vec![
                ScmChange {
                    path: "src/a.rs".into(),
                    code: "M".into()
                },
                ScmChange {
                    path: "src/c.rs".into(),
                    code: "M".into()
                }
            ]
        );
        assert_eq!(
            unstaged,
            vec![
                ScmChange {
                    path: "src/b.rs".into(),
                    code: "M".into()
                },
                ScmChange {
                    path: "src/c.rs".into(),
                    code: "M".into()
                }
            ]
        );
    }

    #[test]
    fn an_untracked_file_is_never_counted_as_staged() {
        let (staged, unstaged, _) = split_changes(&[row("?? notes/todo.md")]);
        assert!(staged.is_empty(), "未跟踪的文件不该出现在暂存区");
        assert_eq!(
            unstaged,
            vec![ScmChange {
                path: "notes/todo.md".into(),
                code: "?".into()
            }]
        );
    }

    #[test]
    fn a_rename_consumes_its_pair_and_reports_the_new_path() {
        // `-z` 下改名是两条记录：先新名字，紧跟一条不带状态列的旧名字。
        let rows = vec![row("R  new/name.rs"), row("old/name.rs"), row(" M keep.rs")];
        let (staged, unstaged, _) = split_changes(&rows);
        assert_eq!(
            staged,
            vec![ScmChange {
                path: "new/name.rs".into(),
                code: "R".into()
            }]
        );
        assert_eq!(
            unstaged,
            vec![ScmChange {
                path: "keep.rs".into(),
                code: "M".into()
            }]
        );
    }

    #[test]
    fn a_branch_line_loses_its_upstream_and_ahead_count() {
        assert_eq!(
            parse_branch("## main...origin/main [ahead 1]"),
            Some("main".to_string())
        );
        assert_eq!(parse_branch("## feature/x"), Some("feature/x".to_string()));
        assert_eq!(
            parse_branch("## No commits yet on main"),
            Some("main".to_string())
        );
        assert_eq!(parse_branch("## HEAD (no branch)"), None);
        assert_eq!(
            parse_branch("## Initial commit on dev"),
            Some("dev".to_string())
        );
    }

    #[test]
    fn a_hunk_carries_both_line_numbers_so_the_two_columns_align() {
        let diff = "\
diff --git a/a.rs b/a.rs
index 111..222 100644
--- a/a.rs
+++ b/a.rs
@@ -10,4 +10,5 @@ fn main() {
 let x = 1;
-let y = 2;
+let y = 3;
+let z = 4;
 let w = 5;
";
        let hunks = parse_unified_diff(diff);
        assert_eq!(hunks.len(), 1);
        let h = &hunks[0];
        assert_eq!(
            (h.old_start, h.old_lines, h.new_start, h.new_lines),
            (10, 4, 10, 5)
        );
        let seen: Vec<_> = h
            .lines
            .iter()
            .map(|l| (l.kind.as_str(), l.old_no, l.new_no, l.text.as_str()))
            .collect();
        assert_eq!(
            seen,
            vec![
                ("context", Some(10), Some(10), "let x = 1;"),
                ("del", Some(11), None, "let y = 2;"),
                ("add", None, Some(11), "let y = 3;"),
                ("add", None, Some(12), "let z = 4;"),
                ("context", Some(12), Some(13), "let w = 5;"),
            ]
        );
    }

    #[test]
    fn a_header_line_before_any_hunk_is_not_body_text() {
        let diff = "diff --git a/a b/a\n--- a/a\n+++ b/a\n";
        assert!(parse_unified_diff(diff).is_empty());
    }

    #[test]
    fn an_empty_hunk_header_still_parses() {
        // `-0,0` 是「旧文件在第 0 行之后没有内容」，纯新增的第一个 hunk 会长这样。
        assert_eq!(parse_hunk_header("-0,0 +1,3 @@"), Some((0, 0, 1, 3)));
        assert_eq!(parse_hunk_header("-5 +5 @@"), Some((5, 1, 5, 1)));
    }

    #[test]
    fn staging_refuses_paths_that_leave_the_root_or_reach_into_git() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        for bad in [".git/config", "../outside", "/etc/passwd"] {
            let err = check_paths(root, &[bad.to_string()]).expect_err(bad);
            assert_eq!(err.error, "Forbidden", "{bad} 必须被拒绝");
        }
        assert!(check_paths(root, &["notes.md".to_string()]).is_ok());
        assert_eq!(
            check_paths(root, &[]).expect_err("空清单").error,
            "BadRequest"
        );
    }

    /// 真起 git：这一面的判据全是「git 说了什么」，拿构造出来的字符串测不到
    /// `-z` 的配对、索引与 HEAD 的实际形状。
    async fn git_available() -> bool {
        tokio::process::Command::new("git")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[tokio::test]
    async fn a_workspace_walks_from_untracked_to_committed_history() {
        if !git_available().await {
            panic!("this test needs git on PATH; the gateway image ships it");
        }
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();

        let before = git_status(root).await.expect("status before init");
        assert!(!before.repo, "空目录还不是仓库");

        run_git(root, &["init", "-q", "-b", "main"], &[0])
            .await
            .expect("init");

        tokio::fs::write(root.join("notes.md"), "hello\n")
            .await
            .expect("write");
        let st = git_status(root).await.expect("status");
        assert!(st.repo);
        assert_eq!(st.branch.as_deref(), Some("main"));
        assert!(st.head.is_none(), "还没有提交时不该有 head");
        assert!(st.staged.is_empty());
        assert_eq!(
            st.unstaged,
            vec![ScmChange {
                path: "notes.md".into(),
                code: "?".into()
            }]
        );

        // 未跟踪文件的 diff 由 `--no-index` 现算：整份都是新增行。
        let untracked = file_diff(root, "notes.md", false).await.expect("diff");
        assert_eq!(untracked.added, 1);
        assert_eq!(untracked.removed, 0);

        run_git(root, &["add", "--", "notes.md"], &[0])
            .await
            .expect("add");
        let staged = git_status(root).await.expect("status");
        assert_eq!(
            staged.staged,
            vec![ScmChange {
                path: "notes.md".into(),
                code: "A".into()
            }]
        );
        assert!(staged.unstaged.is_empty());

        run_git(
            root,
            &[
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--no-verify",
                "--quiet",
                "-m",
                "first",
            ],
            &[0],
        )
        .await
        .expect("commit");

        let after = git_status(root).await.expect("status");
        assert!(after.head.is_some());
        assert!(after.staged.is_empty() && after.unstaged.is_empty());

        let log = git_log(root, 10).await.expect("log");
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].subject, "first");

        // 改一行：工作区那一份有一条删除、一条新增。
        tokio::fs::write(root.join("notes.md"), "goodbye\n")
            .await
            .expect("write");
        let changed = file_diff(root, "notes.md", false).await.expect("diff");
        assert_eq!(changed.added, 1);
        assert_eq!(changed.removed, 1);
        assert_eq!(changed.hunks.len(), 1);
    }
}
