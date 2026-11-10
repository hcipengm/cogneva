//! principle: flat-agent-society
//!
//! A18：工具面够用——搜索真的搜，git 三件套在，补丁落得下去。
//!
//! 「注册了但什么都不回」比「没有这个工具」更坏。前者看起来成功：`search_code`
//! 回一个空列表，模型得到的结论是「这棵树里没有这段代码」，于是它去别处找，
//! 或者自己编一个文件路径。这条错误路径上没有一步会报错，直到有人读那份改错
//! 了的代码。
//!
//! 所以本门用一棵真的树：命令真的跑，读的是那份文件，git 真的建仓库、真的提
//! 交。替身回话只能证明「工具会被调用」，证明不了「它读的是这棵树」。
//!
//! 它不判的：执行器 Pod 那一侧的路径解析（门在
//! `an_employee_works_in_its_own_checkout_test.rs`），以及命令被谁的身份选中
//! 的那条路由（单元测试在 `cog-agent` 的 `tree_tool_tests`）。

use std::process::Stdio;
use std::sync::Arc;

use cog_agent::tools::builtins::{
    apply_patch, git_commit, git_diff, git_status, glob, grep, search_code, todo, SandboxTools,
};
use cog_agent::ToolRegistry;
use cog_core::ToolRegistry as _;
use cog_core::{SFError, SFResult, SandboxBackend, SandboxPayload, SandboxRequest, SandboxResult};

/// 一棵真树上的执行器：命令在给定的检出目录里用 `sh -c` 跑。
///
/// 生产上这一段在执行器 Pod 里；这里要的是同一件事——命令真的跑，读的是那棵
/// 树——而不是又一个回话的替身。
struct Checkout {
    root: std::path::PathBuf,
}

#[async_trait::async_trait]
impl SandboxBackend for Checkout {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        let SandboxPayload::Command { command } = &req.payload else {
            return Err(SFError::Agent(
                "the tree tools run commands; this backend runs nothing else".into(),
            ));
        };
        let output = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .current_dir(&self.root)
            // 跑测试的人家里那份 git 配置与这里无关：署名由工具自己给，签名
            // 开关也不该由它决定。
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        Ok(SandboxResult {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
            workdir: Some(self.root.display().to_string()),
            ..Default::default()
        })
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(SFError::Agent("this backend runs commands, not wasm".into()))
    }
}

fn registry_for(root: &std::path::Path) -> ToolRegistry {
    let registry = ToolRegistry::new();
    let backend: Arc<dyn SandboxBackend> = Arc::new(Checkout {
        root: root.to_path_buf(),
    });
    let tools = SandboxTools::new(Some(backend), std::time::Duration::from_secs(60));
    registry.register(search_code(tools.clone()));
    registry.register(grep(tools.clone()));
    registry.register(glob(tools.clone()));
    registry.register(git_status(tools.clone()));
    registry.register(git_diff(tools.clone()));
    registry.register(git_commit(tools.clone(), None));
    registry.register(apply_patch(tools));
    registry.register(todo());
    registry
}

fn write(root: &std::path::Path, path: &str, text: &str) {
    let full = root.join(path);
    if let Some(parent) = full.parent() {
        std::fs::create_dir_all(parent).expect("the directory is creatable");
    }
    std::fs::write(full, text).expect("the file is writable");
}

fn temp_tree() -> tempfile::TempDir {
    tempfile::tempdir().expect("a temporary tree")
}

/// 在临时树里直接跑一条命令：建仓库这种「装置」也是命令，不另开入口。
async fn sh(root: &std::path::Path, command: &str) -> SandboxResult {
    Checkout {
        root: root.to_path_buf(),
    }
    .execute(&SandboxRequest {
        task_id: "setup".into(),
        agent_id: "setup".into(),
        payload: SandboxPayload::Command {
            command: command.to_string(),
        },
        input: serde_json::Value::Null,
        timeout: std::time::Duration::from_secs(30),
        limits: Default::default(),
    })
    .await
    .unwrap_or_else(|e| panic!("{command}: {e}"))
}

async fn init_repo(root: &std::path::Path) {
    let done = sh(root, "git init -q -b main").await;
    assert_eq!(done.exit_code, 0, "{}", done.stderr);
}

/// 搜索回的是这棵树里真的有的东西：文件、行号、那一行。
#[tokio::test]
async fn a_search_really_searches() {
    let tree = temp_tree();
    write(
        tree.path(),
        "src/main.rs",
        "fn helper() {}\n\nfn main() {\n    helper();\n}\n",
    );
    write(tree.path(), "src/other.rs", "fn helper() {}\n");
    let registry = registry_for(tree.path());

    let found = registry
        .execute("search_code", serde_json::json!({ "query": "fn main" }))
        .await
        .expect("a search over a real tree answers");
    let matches = found["matches"].as_array().expect("matches is a list");
    assert_eq!(matches.len(), 1, "只该命中一处：{found}");
    assert_eq!(matches[0]["path"], "src/main.rs");
    assert_eq!(matches[0]["line"], 3);
    assert_eq!(matches[0]["text"], "fn main() {");
    assert_eq!(
        found["truncated"],
        serde_json::json!(false),
        "两处命中还不到顶，不许说被截断"
    );

    // 同一个词在两处出现时，两处都回，而不是只回第一处。
    let helpers = registry
        .execute("search_code", serde_json::json!({ "query": "fn helper" }))
        .await
        .expect("a second search answers");
    let helpers = helpers["matches"].as_array().expect("matches is a list");
    assert_eq!(helpers.len(), 2, "两份文件各有一处：{helpers:?}");

    // 真的不存在的词回空列表，且不报错——这是空集，不是失败。
    let none = registry
        .execute("search_code", serde_json::json!({ "query": "no_such_symbol_anywhere" }))
        .await
        .expect("an empty result set is not an error");
    assert_eq!(none["matches"], serde_json::json!([]));
}

/// `grep` 与 `search_code` 搜的是同一批东西，只是一个回结构化、一个回原文。
#[tokio::test]
async fn grep_answers_with_the_lines_themselves() {
    let tree = temp_tree();
    write(tree.path(), "a.txt", "alpha\nbeta\ngamma\n");
    let registry = registry_for(tree.path());
    let found = registry
        .execute("grep", serde_json::json!({ "pattern": "beta" }))
        .await
        .expect("grep answers");
    let lines = found["matches"].as_array().expect("matches is a list");
    assert_eq!(lines.len(), 1, "{found}");
    assert_eq!(lines[0], "./a.txt:2:beta");
}

/// glob 认的是检出根算起的整条路径。
#[tokio::test]
async fn glob_lists_what_the_pattern_names() {
    let tree = temp_tree();
    write(tree.path(), "src/main.rs", "");
    write(tree.path(), "src/deep/lib.rs", "");
    write(tree.path(), "README.md", "");
    // 列文件问的是 git：它分得出「该看见的」与「被忽略的」，而 `find` 会把
    // `target/` 里几万个文件也列出来。生产上的检出本来就是一棵 git 树。
    init_repo(tree.path()).await;
    let registry = registry_for(tree.path());

    let top = registry
        .execute("glob", serde_json::json!({ "pattern": "*.md" }))
        .await
        .expect("a listing answers");
    assert_eq!(top["paths"], serde_json::json!(["README.md"]));

    let all = registry
        .execute("glob", serde_json::json!({ "pattern": "**/*.rs" }))
        .await
        .expect("a listing answers");
    assert_eq!(
        all["paths"],
        serde_json::json!(["src/deep/lib.rs", "src/main.rs"])
    );
}

/// git 三件套跑在同一棵树上：看见改动、提交、再看见 diff。
#[tokio::test]
async fn git_sees_the_change_and_commits_it() {
    let tree = temp_tree();
    write(tree.path(), "a.txt", "one\n");
    init_repo(tree.path()).await;
    let registry = registry_for(tree.path());

    let status = registry
        .execute("git_status", serde_json::json!({}))
        .await
        .expect("status answers");
    assert!(
        status["status"].as_str().unwrap_or_default().contains("?? a.txt"),
        "新文件该是未跟踪：{status}"
    );

    let committed = registry
        .execute(
            "git_commit",
            serde_json::json!({ "message": "add a.txt", "paths": ["a.txt"] }),
        )
        .await
        .expect("a commit happens");
    let commit = committed["commit"].as_str().unwrap_or_default();
    assert_eq!(commit.len(), 40, "回的是一个提交 id，不是一句话：{committed}");

    let after = registry
        .execute("git_status", serde_json::json!({}))
        .await
        .expect("status answers");
    assert!(
        after["status"].as_str().unwrap_or_default().contains("## main"),
        "提交之后该站在 main 上：{after}"
    );
    assert!(
        !after["status"].as_str().unwrap_or_default().contains("?? a.txt"),
        "提交过的文件不该再是未跟踪：{after}"
    );

    let diff = registry
        .execute("git_diff", serde_json::json!({}))
        .await
        .expect("diff answers");
    assert_eq!(diff["diff"], "", "刚提交完，工作区与提交一致");
}

/// 一份补丁落进树里，`git_diff` 随后看得见它加的那一行。
#[tokio::test]
async fn a_patch_lands_and_the_diff_shows_it() {
    let tree = temp_tree();
    write(tree.path(), "notes.txt", "one\n");
    init_repo(tree.path()).await;
    let registry = registry_for(tree.path());
    let patch = "--- a/notes.txt\n+++ b/notes.txt\n@@ -1 +1,2 @@\n one\n+two\n";

    registry
        .execute("apply_patch", serde_json::json!({ "patch": patch }))
        .await
        .expect("a clean patch applies");
    let text = std::fs::read_to_string(tree.path().join("notes.txt")).expect("the file is there");
    assert_eq!(text, "one\ntwo\n", "补丁的正文就是该落进文件的那份");

    // 打不上的补丁不许留一半：报错，文件保持原样。
    let stale = "--- a/notes.txt\n+++ b/notes.txt\n@@ -1 +1 @@\n seven\n+eight\n";
    let err = registry
        .execute("apply_patch", serde_json::json!({ "patch": stale }))
        .await
        .expect_err("a patch against text that is not there is refused");
    assert!(err.to_string().contains("did not apply"), "{err}");
    assert_eq!(
        std::fs::read_to_string(tree.path().join("notes.txt")).expect("the file is there"),
        "one\ntwo\n",
        "打不上的补丁不许改动文件"
    );
}

/// 清单是这一轮的事，不在一棵树里：没有执行器也记得住。
#[tokio::test]
async fn the_todo_list_needs_no_checkout() {
    let registry = ToolRegistry::new();
    registry.register(todo());
    let reply = registry
        .execute(
            "todo",
            serde_json::json!({"items": [
                {"content": "find where the failure comes from", "status": "in_progress"},
                {"content": "fix it", "status": "pending"},
            ]}),
        )
        .await
        .expect("a list is accepted with no executor in the process");
    assert_eq!(reply["in_progress"], 1);
    assert_eq!(reply["done"], 0);
}
