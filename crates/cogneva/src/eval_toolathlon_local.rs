//! toolathlon 题面声明的**本地工具**：不属任何 MCP server，由外壳这一侧提供。
//!
//! 上游 `_build_local_tools` 按 `needed_local_tools` 现造这几件，而它们的实现全都落在
//! **这道题的 agent_workspace** 里（写一个脚本再跑、把长输出存成文件再翻页看）。所以这里
//! 的每一件都经同一条 `kubectl exec` 落到那道题的 Pod 里，没有一个在宿主上跑——在宿主上
//! 跑出来的 `.python_tmp/` 不是判分脚本会去读的那一个。
//!
//! 口径逐条对着上游那一份，连返回**文本**的形状都对：模型看到的是这段字，换一种写法就是
//! 换了一个工具面，哪怕名字一样。
//!
//! 两处**已知**的差别，不当成已对齐：
//!
//! - `claim_done` 在壳里只是回一句「已标记完成」。上游那一侧它置一个停止旗；本平台的
//!   外壳没有那个旗，所以它不会让这一轮立刻收工。工具在、答复一致，**停止语义没有**。
//! - `handle_overlong_tool_outputs` 是一个**声明名**，上游由它造出**两件**工具
//!   （`save_overlong_output` / `view_overlong_output`）。这里同样两件，名字一致。

use std::sync::Arc;
use std::time::Instant;

use base64::Engine;
use cog_core::ToolDefinition;

use crate::eval_mcp_in_pod::sh_quote as shq;
use crate::eval_toolathlon::{ToolathlonPod, CONTAINER_AGENT_WORKSPACE};

/// 上游 `overlong_tool_manager.PAGE_SIZE`：一页约一万字符。
const PAGE_SIZE: usize = 10_000;
/// 上游 `python_execute` 自带上界：`timeout = min(int(timeout), 120)`。
const PYTHON_TIMEOUT_CEILING: u64 = 120;

/// 这道题要的本地工具。`wanted` 是 `needed_local_tools` 里的那几个**声明名**。
pub(crate) struct LocalTools {
    pod: Arc<ToolathlonPod>,
    declared: Vec<String>,
}

impl LocalTools {
    pub(crate) fn new(pod: Arc<ToolathlonPod>, declared: &[String]) -> Self {
        Self {
            pod,
            declared: declared.to_vec(),
        }
    }

    /// 这道题的工具规格。声明名与工具名不是一一对应的：`handle_overlong_tool_outputs`
    /// 出两件，别的声明名出一件。
    pub(crate) fn definitions(&self) -> Vec<ToolDefinition> {
        let mut out = Vec::new();
        for name in &self.declared {
            match name.as_str() {
                "claim_done" => out.push(def(
                    "claim_done",
                    "Call this tool when the task is fully completed.",
                    serde_json::json!({"type": "object", "properties": {}}),
                )),
                "python_execute" => out.push(def(
                    "python_execute",
                    "Execute Python code in the agent workspace and return stdout/stderr.\n\n\
                     Args:\n  code: Python source code to execute.\n  filename: Optional \
                     filename (with .py). A random UUID name is used if omitted.\n  timeout: Max \
                     execution time in seconds (capped at 120).",
                    serde_json::json!({
                        "type": "object",
                        "properties": {
                            "code": {"type": "string"},
                            "filename": {"type": "string"},
                            "timeout": {"type": "integer"}
                        },
                        "required": ["code"]
                    }),
                )),
                "handle_overlong_tool_outputs" => {
                    out.push(def(
                        "save_overlong_output",
                        "Save a large text to disk and return a reference ID.\n\nArgs:\n  \
                         content: The large text content to store.\n  label: Optional \
                         human-readable label for this output.",
                        serde_json::json!({
                            "type": "object",
                            "properties": {
                                "content": {"type": "string"},
                                "label": {"type": "string"}
                            },
                            "required": ["content"]
                        }),
                    ));
                    out.push(def(
                        "view_overlong_output",
                        "View a saved overlong output by ID, paginated.\n\nArgs:\n  id: The \
                         reference ID returned by save_overlong_output.\n  page: Page number \
                         (0-indexed, each page ~10000 chars).",
                        serde_json::json!({
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "page": {"type": "integer"}
                            },
                            "required": ["id"]
                        }),
                    ));
                }
                "manage_context" | "history" => out.push(def(
                    name,
                    &format!("Stub for {name}."),
                    serde_json::json!({
                        "type": "object",
                        "properties": {"action": {"type": "string"}}
                    }),
                )),
                // `sleep` 上游也认，但 503 道题一个都没声明它；声明了也不给一件假的。
                _ => {}
            }
        }
        out
    }

    /// 这个名字是不是本地工具（不是就落回 MCP 那一侧）。
    pub(crate) fn handles(&self, name: &str) -> bool {
        self.definitions().iter().any(|d| d.name == name)
    }

    /// 跑一件本地工具。返回的**文本**与上游逐字同形——模型看到的就是它。
    pub(crate) async fn call(
        &self,
        name: &str,
        arguments: &serde_json::Value,
    ) -> anyhow::Result<String> {
        match name {
            "claim_done" => Ok("Task marked as done.".to_string()),
            "python_execute" => self.python_execute(arguments).await,
            "save_overlong_output" => self.save_overlong(arguments).await,
            "view_overlong_output" => self.view_overlong(arguments).await,
            "manage_context" | "history" => Ok("OK".to_string()),
            other => anyhow::bail!("`{other}` is not a local tool of this benchmark"),
        }
    }

    async fn python_execute(&self, arguments: &serde_json::Value) -> anyhow::Result<String> {
        let code = arguments
            .get("code")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("`python_execute` needs `code` as a string"))?;
        let mut filename = arguments
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if filename.is_empty() {
            filename = format!("{}.py", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        }
        if !filename.ends_with(".py") {
            filename.push_str(".py");
        }
        // 上游把 `filename` 直接 join 进 workspace 下的路径；带 `/` 的名字会落到别的目录
        // 里，甚至跳出 workspace。判据在**入口**：名字只能是一个文件名。
        if filename.contains('/') || filename == "." || filename == ".." {
            return Ok(format!(
                "=== STDERR ===\nfilename must be a plain file name, got `{filename}`"
            ));
        }
        let timeout = arguments
            .get("timeout")
            .and_then(|v| v.as_u64())
            .unwrap_or(30)
            .min(PYTHON_TIMEOUT_CEILING);

        let dir = format!("{CONTAINER_AGENT_WORKSPACE}/.python_tmp");
        let path = format!("{dir}/{filename}");
        self.pod.mkdir(&dir).await?;
        self.pod.write_bytes(&path, code.as_bytes()).await?;

        let start = Instant::now();
        let out = self
            .pod
            .exec(&format!(
                "timeout {timeout}s uv run --directory {ws} ./.python_tmp/{filename}",
                ws = shq(CONTAINER_AGENT_WORKSPACE),
                filename = shq(&filename),
            ))
            .await?;
        let elapsed = start.elapsed().as_secs_f64();

        // `timeout` 杀掉的形状是 124。上游那一侧是 `subprocess.TimeoutExpired`，
        // 答复也只有那一句——不带上被截断的半截输出。
        if out.code == Some(124) {
            return Ok(format!("=== TIMEOUT ===\nExceeded {timeout}s limit."));
        }
        let mut parts: Vec<String> = Vec::new();
        if !out.stdout.is_empty() {
            parts.push("=== STDOUT ===".into());
            parts.push(out.stdout.trim_end().to_string());
        }
        if !out.stderr.is_empty() {
            parts.push("=== STDERR ===".into());
            parts.push(out.stderr.trim_end().to_string());
        }
        parts.push("=== INFO ===".into());
        parts.push(format!(
            "Return code: {}",
            out.code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-1".into())
        ));
        parts.push(format!("Time: {elapsed:.2}s / {timeout}s limit"));
        Ok(parts.join("\n"))
    }

    async fn save_overlong(&self, arguments: &serde_json::Value) -> anyhow::Result<String> {
        let content = arguments
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("`save_overlong_output` needs `content` as a string"))?;
        let label = arguments
            .get("label")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
        let dir = format!("{CONTAINER_AGENT_WORKSPACE}/.overlong_tool_outputs");
        self.pod.mkdir(&dir).await?;
        let body = serde_json::json!({
            "id": id,
            "label": label,
            "content": content,
            "saved_at": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
        });
        self.pod
            .write_bytes(&format!("{dir}/{id}.json"), body.to_string().as_bytes())
            .await?;
        let preview: String = content.chars().take(200).collect();
        let ellipsis = if content.chars().count() > 200 {
            "..."
        } else {
            ""
        };
        Ok(format!(
            "Saved {} chars as [{id}] label='{label}'.\nPreview: {preview}{ellipsis}\n\
             Use view_overlong_output(id='{id}') to read.",
            content.chars().count()
        ))
    }

    async fn view_overlong(&self, arguments: &serde_json::Value) -> anyhow::Result<String> {
        let id = arguments
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("`view_overlong_output` needs `id` as a string"))?;
        let page = arguments
            .get("page")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            .max(0) as usize;
        // 上游拿 `id` 直接拼路径；这里只认它自己发的那种形状。不认的一律按「没有这件」
        // 答复——与上游对不存在 id 的答复同一句，且不给路径穿越留口子。
        let looks_like_id = id.len() == 8 && id.chars().all(|c| c.is_ascii_hexdigit());
        if !looks_like_id {
            return Ok(format!("No saved output with id '{id}'."));
        }
        let path = format!("{CONTAINER_AGENT_WORKSPACE}/.overlong_tool_outputs/{id}.json");
        let Some(raw) = self.pod.read_text(&path).await? else {
            return Ok(format!("No saved output with id '{id}'."));
        };
        let data: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("the saved output {id} is not readable JSON: {e}"))?;
        let content = data
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("the saved output {id} carries no `content`"))?;
        let label = data.get("label").and_then(|v| v.as_str()).unwrap_or("");
        let chars: Vec<char> = content.chars().collect();
        let total = chars.len();
        let total_pages = total.div_ceil(PAGE_SIZE).max(1);
        let start = page * PAGE_SIZE;
        let chunk: String = chars.iter().skip(start).take(PAGE_SIZE).collect();
        Ok(format!(
            "[{id}] label='{label}' | page {}/{} | chars {start}–{} of {total}\n\n{chunk}",
            page + 1,
            total_pages,
            start + chunk.chars().count()
        ))
    }
}

fn def(name: &str, description: &str, parameters: serde_json::Value) -> ToolDefinition {
    ToolDefinition {
        name: name.to_string(),
        description: description.to_string(),
        parameters,
    }
}

/// 把一段字节送进 Pod 里的一个文件。
///
/// 走 base64 而不是 heredoc：代码里有引号、`$`、反引号，任何一层 shell 展开都会就地
/// 改写它。base64 的字母表里没有 shell 元字符。
pub(crate) fn b64_write_script(path: &str, bytes: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    format!("printf '%s' '{encoded}' | base64 -d > {}", shq(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_local_definitions_are_what_upstream_builds() {
        let declared = vec![
            "claim_done".to_string(),
            "handle_overlong_tool_outputs".to_string(),
            "manage_context".to_string(),
            "history".to_string(),
            "python_execute".to_string(),
        ];
        let pod = Arc::new(crate::eval_toolathlon::ToolathlonPod::for_test("p"));
        let tools = LocalTools::new(pod, &declared);
        let defs = tools.definitions();
        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        // 五个声明名 → 六件工具：`handle_overlong_tool_outputs` 出两件。
        assert_eq!(
            names,
            vec![
                "claim_done",
                "save_overlong_output",
                "view_overlong_output",
                "manage_context",
                "history",
                "python_execute",
            ]
        );
        // 没有声明的名字不出工具（`sleep` 上游认，但这 503 道题一件都没声明）。
        let none = LocalTools::new(
            Arc::new(crate::eval_toolathlon::ToolathlonPod::for_test("p")),
            &[],
        );
        assert!(none.definitions().is_empty());
        assert!(!none.handles("claim_done"));
    }

    /// 送进 Pod 的那段脚本要能存活任意字节：载荷只在 base64 里出现。
    #[test]
    fn a_payload_reaches_the_pod_without_being_read_by_the_shell() {
        let script = b64_write_script("/w/x.py", b"print('a $HOME `id` \\n')");
        assert!(script.contains("base64 -d > '/w/x.py'"));
        // 原文一个字节都不在脚本里——在就是被 shell 看过一遍了。
        assert!(!script.contains("$HOME"));
        assert!(!script.contains('`'));
    }

    /// 对着**一台真 Pod** 跑一遍本地工具：代码真的进了容器、真的在题的工作区里执行、
    /// 长输出真的能存下来再翻页读回来。
    ///
    /// 同样不随 `cargo test` 跑。跑法（活着的 pack Pod 一台）：
    ///
    /// ```text
    /// COGNEVA_EVAL_TOOLATHLON_PROBE_POD=<pod> cargo test -p cogneva --lib \
    ///   the_local_tools_really_run_in_the_pod -- --ignored --nocapture
    /// ```
    ///
    /// 单元测试测不到的是这三处：`python_execute` 那条 `uv run` 在真容器里能不能起、
    /// `save/view` 的目录与分页在真文件上对不对、以及送进去的字节有没有被 shell 改写。
    #[tokio::test]
    #[ignore = "needs a live pack pod: set COGNEVA_EVAL_TOOLATHLON_PROBE_POD (see the doc comment)"]
    async fn the_local_tools_really_run_in_the_pod() {
        let Ok(name) = std::env::var("COGNEVA_EVAL_TOOLATHLON_PROBE_POD") else {
            panic!("set COGNEVA_EVAL_TOOLATHLON_PROBE_POD to a running pack pod");
        };
        let pod = Arc::new(crate::eval_toolathlon::ToolathlonPod::for_test_in(
            &name,
            vec!["sudo".into(), "-n".into(), "kubectl".into()],
            "cogneva",
        ));
        let tools = LocalTools::new(
            pod,
            &[
                "claim_done".to_string(),
                "python_execute".to_string(),
                "handle_overlong_tool_outputs".to_string(),
            ],
        );

        let done = tools
            .call("claim_done", &serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(done, "Task marked as done.");

        // 引号与 `$` 都在这段代码里：它们必须原样到 python，而不是被某一层 shell 吃掉。
        let code = "import os\nprint('a $HOME \\'q\\' `id`')\nprint(os.getcwd())";
        let out = tools
            .call("python_execute", &serde_json::json!({"code": code}))
            .await
            .unwrap();
        assert!(out.contains("=== STDOUT ==="), "{out}");
        assert!(out.contains("a $HOME 'q' `id`"), "{out}");
        assert!(
            out.contains(crate::eval_toolathlon::CONTAINER_AGENT_WORKSPACE),
            "the script must run in this task's agent workspace: {out}"
        );
        assert!(out.contains("Return code: 0"), "{out}");

        // 25000 字符 → 三页（每段 4 字符，所以要 6250 段；写 2500 段只有 10000 字符，
        // 正好一页，翻页那三条断言会对着「page 3/1」红）。
        let long: String = (0..6250).map(|i| format!("{i:04}")).collect();
        let saved = tools
            .call(
                "save_overlong_output",
                &serde_json::json!({"content": long, "label": "probe"}),
            )
            .await
            .unwrap();
        let id = saved
            .split('[')
            .nth(1)
            .and_then(|s| s.split(']').next())
            .expect("the reply must carry the id")
            .to_string();
        let last = tools
            .call(
                "view_overlong_output",
                &serde_json::json!({"id": id, "page": 2}),
            )
            .await
            .unwrap();
        assert!(last.contains("page 3/3"), "{last}");
        assert!(last.contains("chars 20000–25000 of 25000"), "{last}");
        assert!(
            last.ends_with(&long[20000..]),
            "the last page must carry the tail"
        );
        let missing = tools
            .call(
                "view_overlong_output",
                &serde_json::json!({"id": "../etc/passwd"}),
            )
            .await
            .unwrap();
        assert_eq!(missing, "No saved output with id '../etc/passwd'.");
    }

    #[test]
    fn a_name_that_is_not_one_of_ours_is_refused() {
        let pod = Arc::new(crate::eval_toolathlon::ToolathlonPod::for_test("p"));
        let tools = LocalTools::new(pod, &["claim_done".to_string()]);
        assert!(tools.handles("claim_done"));
        assert!(!tools.handles("save_overlong_output"));
    }
}
