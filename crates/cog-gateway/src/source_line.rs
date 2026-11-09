//! `GET /api/v1/delivery/source-line` —— 共享裸仓库里主线走到了哪一版，哪些分支在
//! 它之上、各自差几笔。
//!
//! 交付链路的两格没有别的数据源：
//!
//! - **编辑**：一次改动从某个工作树出发，工作树的名字就是它那条分支的名字。宿主上的
//!   工作树目录不在网关的挂载里，但每条分支的尖端在裸仓库里，且带着提交时间与标题——
//!   「谁最近在这条线上动了」这句话只从引用里就能读出来，不必进那棵工作树。
//! - **推送**：部署器只跟 `main`。`refs/heads/main` 走到哪个字节、多久没动过，就是
//!   「推上去了没有」这一问的读数。
//!
//! 说清楚这一面**不**是什么：分支尖端是**提交**，不是工作树里的磁盘状态。一棵工作树
//! 里改了十个文件一个都没提交，在这里读出来是「它的分支没有动」——那是对的，因为链路上
//! 下一个节点（构建）也确实不会看见那些字节。要把「有人在改」说成一句读数，需要另一份
//! 挂载，而那一格现在就该报「读不到」，而不是拿分支尖端冒充它。
//!
//! 三种结局沿用构建槽那一面的闭集：`measured`（读到了）、`unwired`（这个部署没把裸
//! 仓库挂进来）、`unread`（挂在、读不动）。「没有 main 引用」不是前三种的任何一个：
//! 仓库读得到、但它里面没有主线，那是第四种事实，所以它落在 `measured` 里，由
//! `main: null` 与一句理由说出来。

use std::path::{Path, PathBuf};
use std::process::Command;

use axum::Json;
use serde::Serialize;

/// 共享裸仓库的路径。部署里只有一处设置它——`COGNEVA_GITOPS_REPO_URL`，GitOps
/// 拉取端也在读同一个键。不新开一个名字：两个键指向同一个目录时，改了一处没改另一处
/// 就会让两个读的人看到两条不同的源线，而且从读数上分辨不出是哪一处没改。
pub const REPO_ENV: &str = cog_core::env::GITOPS_REPO_URL;

/// 一次列出多少条分支。
///
/// 覆盖（override）：这是一张给人看的列表，不是被谁消费的界，没有「换输入 ⇒ 值跟着变」
/// 的闭环可走。实测 2026-11-09 裸仓库里有六条工作树分支与若干 `evol/<id>`，取 40 条足以
/// 把活跃的全列上；超出时读数自己会说还有几条没列（`more_branches`），所以截断是可见的，
/// 不会读成「就这么多」。
// cap: bound — 给人看的列表长度，不是被谁消费的界；截断由 more_branches 自己说出来
pub const MAX_BRANCHES: usize = 40;

/// 字段分隔符：`for-each-ref` 的 `%09`。提交标题里出现制表符是可能的，所以解析时按
/// 前三个分隔符切，剩下的整段都是标题，标题被切坏也不会污染前面的字段。
const FIELD_SEP: char = '\t';

/// 一格分支读数。链路页面上「编辑」与「推送」两格都从这里取依据。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BranchReading {
    /// 分支名，就是宿主上那棵工作树的名字（`main` 除外）。
    pub name: String,
    pub sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<String>,
    /// 尖端那笔提交是多久以前落的。页面画「从何时起」用的是这个数，不是让它自己减。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_secs_ago: Option<u64>,
    pub subject: String,
    /// 这条分支是否已经包含当前 `main`。成立才能快进；不成立就得先变基。
    /// `None` 是「问不出来」——仓库读得到，但这两个引用之间没有关系可算。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contains_main: Option<bool>,
    /// 比 `main` 多几笔。`None` 是算不出来，不是零。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ahead_of_main: Option<u32>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SourceLineReport {
    /// `measured` / `unwired` / `unread`。
    pub state: String,
    /// 前两种结局之外还要说的话：「仓库在但没有 main」「列表被截断」都落在这里。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    pub repo: String,
    /// 主线这一格。仓库读得到而它缺席时是 `None`，不是空对象。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub main: Option<BranchReading>,
    pub branches: Vec<BranchReading>,
    /// 被截断时这里说还有几条没列。`None` 是「全列上了」。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub more_branches: Option<u32>,
    pub generated_at: String,
}

pub async fn source_line_handler() -> Json<SourceLineReport> {
    let repo = repo_path();
    // 读一次是若干条 git 子进程：放在阻塞线程里跑，别占住异步执行器——裸仓库大起来时
    // `rev-list --count` 不是零成本的。
    let report = tokio::task::spawn_blocking(move || read_source_line(&repo))
        .await
        .unwrap_or_else(|e| {
            SourceLineReport::plain(
                "unread",
                Some(format!("读源线的那次调用没能跑完（{e}）")),
                String::new(),
            )
        });
    Json(report)
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub fn repo_path() -> PathBuf {
    non_empty_env(REPO_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(cog_core::paths::TREE_REPO))
}

impl SourceLineReport {
    fn plain(state: &str, why: Option<String>, repo: String) -> Self {
        SourceLineReport {
            state: state.to_string(),
            why,
            repo,
            main: None,
            branches: Vec::new(),
            more_branches: None,
            generated_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 拼这一拍。`repo` 是裸仓库的根，不是它里面某个`.git`。
pub fn read_source_line(repo: &Path) -> SourceLineReport {
    let shown = repo.display().to_string();

    // 裸仓库的判据是它自己那个 `HEAD` 文件：一个不存在的目录与一个不是仓库的目录，
    // 在这里都是「这个部署没有把它挂进来」——两种情况处置相同（去改挂载）。
    if !repo.join("HEAD").is_file() {
        return SourceLineReport::plain(
            "unwired",
            Some(format!(
                "这个部署没有把共享裸仓库挂进来（{REPO_ENV} = {shown}）：这里读不到主线与分支，不是没有主线"
            )),
            shown,
        );
    }

    let listing = match git(repo, &["for-each-ref", "--sort=-committerdate", "--format=%(refname:short)%09%(objectname)%09%(committerdate:unix)%09%(contents:subject)", "refs/heads"]) {
        Ok(text) => text,
        Err(e) => {
            return SourceLineReport::plain(
                "unread",
                Some(format!(
                    "裸仓库在 {shown}，但列不出它的分支（{e}）：这是读不动，不是没有分支"
                )),
                shown,
            )
        }
    };

    let mut all: Vec<BranchReading> = Vec::new();
    for line in listing.lines().filter(|line| !line.trim().is_empty()) {
        // 前三个字段固定，第四个是提交标题（里面可以有制表符）。
        let mut parts = line.splitn(4, FIELD_SEP);
        let name = parts.next().unwrap_or("").trim().to_string();
        let sha = parts.next().unwrap_or("").trim().to_string();
        let committed = parts.next().and_then(|s| s.trim().parse::<u64>().ok());
        let subject = parts.next().unwrap_or("").trim().to_string();
        if name.is_empty() || sha.is_empty() {
            continue;
        }
        all.push(BranchReading {
            name,
            sha,
            committed_at: committed.and_then(rfc3339),
            committed_secs_ago: committed.map(|s| now_secs().saturating_sub(s)),
            subject,
            contains_main: None,
            ahead_of_main: None,
        });
    }

    let mut main = all.iter().find(|b| b.name == "main").cloned();
    let main_sha = main.as_ref().map(|b| b.sha.clone());

    // 分支按提交时间倒序（git 已经排好），截断只影响「列出来哪些」，不影响 main 有没有
    // 找到——main 是从全表里找的。
    let total = all.len();
    let mut shown_branches: Vec<BranchReading> = all.into_iter().take(MAX_BRANCHES).collect();

    for branch in &mut shown_branches {
        if let Some(main_sha) = main_sha.as_deref() {
            let (contains, ahead) = relation(repo, main_sha, &branch.sha);
            branch.contains_main = contains;
            branch.ahead_of_main = ahead;
        }
    }

    // `main` 是 `all` 的一份副本，而关系算在 `shown_branches` 上；主线被截断挡在列表外时，
    // 它那两格就还是 None，页面会把主线读成「问不出来」。主线相对自己的答案是确定的，
    // 不依赖任何查询：包含，且不多不少。
    if let Some(main) = main.as_mut() {
        main.contains_main = Some(true);
        main.ahead_of_main = Some(0);
    }

    let more = total.saturating_sub(shown_branches.len()) as u32;
    let why = match (&main, more) {
        (None, _) => Some(format!(
            "裸仓库在 {shown}，但里面没有 main 引用：读得到仓库，读不到主线——部署器跟的是 main，没有它就没有「推进去了哪一版」可读"
        )),
        (Some(_), 0) => None,
        (Some(_), n) => Some(format!(
            "还有 {n} 条分支没列（按提交时间只列了最近 {MAX_BRANCHES} 条）"
        )),
    };

    SourceLineReport {
        state: "measured".to_string(),
        why,
        repo: shown,
        main,
        branches: shown_branches,
        more_branches: (more > 0).then_some(more),
        generated_at: chrono::Utc::now().to_rfc3339(),
    }
}

/// 两个提交之间的关系：分支是否包含 main，以及比它多几笔。
///
/// 「不包含」与「问不出来」是两件事：前者是退出码 1，后者是命令本身失败。把后者读成
/// 前者会让一条读不动的分支在页面上写着「要先变基」——那是一个不存在的结论。
fn relation(repo: &Path, main_sha: &str, branch_sha: &str) -> (Option<bool>, Option<u32>) {
    // 同一个提交相对自己是确定的：包含，且不多不少。省掉两次子进程，也让「名字叫 main」
    // 不再是一种判据——判据是这两个 sha 相等。
    if main_sha == branch_sha {
        return (Some(true), Some(0));
    }
    let contains = match git_status(repo, &["merge-base", "--is-ancestor", main_sha, branch_sha]) {
        Ok(0) => Some(true),
        Ok(1) => Some(false),
        _ => None,
    };
    let ahead = git(
        repo,
        &["rev-list", "--count", &format!("{main_sha}..{branch_sha}")],
    )
    .ok()
    .and_then(|t| t.trim().parse::<u32>().ok());
    (contains, ahead)
}

fn rfc3339(secs: u64) -> Option<String> {
    chrono::DateTime::from_timestamp(secs as i64, 0).map(|t| t.to_rfc3339())
}

/// 跑一条 git 命令，成功时交出标准输出。
fn git(repo: &Path, args: &[&str]) -> Result<String, String> {
    let out = run_git(repo, args).map_err(|e| format!("调不动 git（{e}）"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            format!("git {} 退 {}", args.join(" "), out.status)
        } else {
            stderr
        });
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// 同 [`git`]，但把退出码交回给调用方——`merge-base --is-ancestor` 的 1 是一个答案。
fn git_status(repo: &Path, args: &[&str]) -> Result<i32, String> {
    let out = run_git(repo, args).map_err(|e| format!("调不动 git（{e}）"))?;
    Ok(out.status.code().unwrap_or(-1))
}

fn run_git(repo: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new("git")
        .arg("--git-dir")
        .arg(repo)
        .args(args)
        // 钩子与提交落地器会把定位变量指到**调用方那棵树**；这里要读的是 `--git-dir`
        // 点名的那一个，继承来的变量会让它改读另一棵树。
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_COMMON_DIR")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn scratch(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "cogneva-source-line-{name}-{}-{}-{}",
            std::process::id(),
            now_secs(),
            seq
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn git_in(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .current_dir(dir)
            .args(args)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t.invalid")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t.invalid")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// 一棵工作树 + 一个裸仓库：往工作树里提交，再把它推进裸仓库的某条分支。
    /// 链路要读的正是这种裸仓库，所以夹具也必须长成那个样子。
    fn repo_with_commits(root: &Path, branch: &str, subjects: &[&str]) -> PathBuf {
        let bare = root.join("tree.git");
        git_in(root, &["init", "-q", "--bare", bare.to_str().unwrap()]);
        let work = root.join(format!("work-{branch}"));
        std::fs::create_dir_all(&work).unwrap();
        git_in(&work, &["init", "-q"]);
        for subject in subjects {
            std::fs::write(work.join("f.txt"), subject).unwrap();
            git_in(&work, &["add", "f.txt"]);
            git_in(&work, &["commit", "-qm", subject]);
        }
        git_in(
            &work,
            &[
                "push",
                "-q",
                bare.to_str().unwrap(),
                &format!("HEAD:refs/heads/{branch}"),
            ],
        );
        bare
    }

    #[test]
    fn a_repo_that_is_not_mounted_says_so_rather_than_no_main() {
        let root = scratch("unwired");
        let report = read_source_line(&root.join("not-mounted"));
        assert_eq!(report.state, "unwired");
        assert!(
            report.why.as_deref().unwrap_or("").contains("不是没有主线"),
            "理由要分开「读不到」与「没有」：{:?}",
            report.why
        );
        assert!(report.main.is_none());
        assert!(report.branches.is_empty());
    }

    #[test]
    fn a_repo_without_main_is_read_but_states_that_the_main_line_is_absent() {
        let root = scratch("no-main");
        let bare = repo_with_commits(&root, "topic", &["one"]);
        let report = read_source_line(&bare);
        assert_eq!(report.state, "measured", "仓库读得到，就不是读不动");
        assert!(report.main.is_none(), "没有 main 就不许造一个出来");
        assert!(
            report
                .why
                .as_deref()
                .unwrap_or("")
                .contains("没有 main 引用"),
            "缺席要说出来：{:?}",
            report.why
        );
        assert_eq!(report.branches.len(), 1);
    }

    #[test]
    fn the_main_line_carries_its_sha_and_time_and_is_its_own_ancestor() {
        let root = scratch("main");
        let bare = repo_with_commits(&root, "main", &["first", "second"]);
        let report = read_source_line(&bare);
        assert_eq!(report.state, "measured");
        let main = report.main.expect("main 要读出来");
        assert_eq!(main.name, "main");
        assert_eq!(main.sha.len(), 40);
        assert_eq!(main.subject, "second");
        assert!(main.committed_secs_ago.is_some());
        assert!(main.committed_at.is_some());
        assert_eq!(main.contains_main, Some(true));
        assert_eq!(main.ahead_of_main, Some(0));
    }

    #[test]
    fn a_branch_that_fell_behind_main_reads_as_needing_a_rebase() {
        let root = scratch("behind");
        let bare = repo_with_commits(&root, "main", &["base"]);
        // 从同一个 base 分出去的一条分支：它不含 main 后来的那一笔。`-b main` 不能省——
        // 裸仓库的 HEAD 还指着默认分支名，不点名检出的就是一个空分支，之后那笔提交
        // 与 main 没有共同祖先，测的就不是「落后」而是「无关」。
        git_in(
            &root,
            &[
                "clone",
                "-q",
                "-b",
                "main",
                bare.to_str().unwrap(),
                "work-topic",
            ],
        );
        let work = root.join("work-topic");
        std::fs::write(work.join("t.txt"), "topic").unwrap();
        git_in(&work, &["add", "t.txt"]);
        git_in(&work, &["commit", "-qm", "topic work"]);
        git_in(&work, &["push", "-q", "origin", "HEAD:refs/heads/topic"]);
        // 再让 main 前进一笔，这时 topic 就不含 main 了。名字不能叫 `work-main`——
        // `repo_with_commits` 已经用这个名字建过那棵树。
        git_in(
            &root,
            &[
                "clone",
                "-q",
                "-b",
                "main",
                bare.to_str().unwrap(),
                "main-advance",
            ],
        );
        let main_work = root.join("main-advance");
        std::fs::write(main_work.join("m.txt"), "more").unwrap();
        git_in(&main_work, &["add", "m.txt"]);
        git_in(&main_work, &["commit", "-qm", "main moves"]);
        git_in(
            &main_work,
            &["push", "-q", "origin", "HEAD:refs/heads/main"],
        );

        let report = read_source_line(&bare);
        let topic = report
            .branches
            .iter()
            .find(|b| b.name == "topic")
            .expect("topic 要列出来");
        assert_eq!(
            topic.contains_main,
            Some(false),
            "分出去之后 main 又动了：这条分支不含 main"
        );
        assert!(
            topic.ahead_of_main.unwrap_or(0) >= 1,
            "它自己那一笔要算在里面：{:?}",
            topic.ahead_of_main
        );
    }

    #[test]
    fn a_branch_built_on_top_of_main_reads_as_fast_forwardable() {
        let root = scratch("ahead");
        let bare = repo_with_commits(&root, "main", &["base"]);
        // clone 的目标必须还不存在，所以它不能是 `current_dir`——那是先有目录再起进程。
        // `-b main` 同样不能省：裸仓库的 HEAD 指着默认分支名，不点名就检出空分支，
        // 之后两笔提交与 main 没有共同祖先，「建在 main 之上」就无从谈起。
        git_in(
            &root,
            &[
                "clone",
                "-q",
                "-b",
                "main",
                bare.to_str().unwrap(),
                "work-wt",
            ],
        );
        let work = root.join("work-wt");
        for subject in ["one", "two"] {
            std::fs::write(work.join("w.txt"), subject).unwrap();
            git_in(&work, &["add", "w.txt"]);
            git_in(&work, &["commit", "-qm", subject]);
        }
        git_in(&work, &["push", "-q", "origin", "HEAD:refs/heads/wt-emp2"]);

        let report = read_source_line(&bare);
        let branch = report
            .branches
            .iter()
            .find(|b| b.name == "wt-emp2")
            .expect("工作树的分支要列出来");
        assert_eq!(branch.contains_main, Some(true), "建在 main 之上，可以快进");
        assert_eq!(branch.ahead_of_main, Some(2));
    }

    /// 列不下的分支必须留下一个读数。一条被截断的列表与一条真的只有这么长的列表，
    /// 在页面上长得一样，而前者会让「最近没人动过第三条以后的树」这句话凭空成立。
    #[test]
    fn a_long_branch_list_says_how_many_it_left_out() {
        let root = scratch("truncated");
        let bare = repo_with_commits(&root, "main", &["base"]);
        let sha = git_in(&bare, &["rev-parse", "refs/heads/main"]);
        // 一共 MAX_BRANCHES + 3 条引用：main 一条，b0..b(MAX_BRANCHES+1) 共
        // MAX_BRANCHES + 2 条。截到 MAX_BRANCHES 之后正好三条没列上，这条等式比
        // 循环边界好数。
        for i in 0..(MAX_BRANCHES + 2) {
            git_in(&bare, &["update-ref", &format!("refs/heads/b{i}"), &sha]);
        }

        let report = read_source_line(&bare);
        assert_eq!(report.state, "measured");
        assert_eq!(
            report.branches.len(),
            MAX_BRANCHES,
            "列表按上限截断：{}",
            report.branches.len()
        );
        assert_eq!(report.more_branches, Some(3), "截掉了几条要说出来");
        assert!(
            report
                .why
                .as_deref()
                .unwrap_or("")
                .contains("还有 3 条分支没列"),
            "理由里要点名截掉了多少：{:?}",
            report.why
        );
        assert!(
            report.main.is_some(),
            "截断只影响列出来的那些，main 是从全表里找的"
        );
    }
}
