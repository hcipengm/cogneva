//! 把一份来源读成 wiki 页——这一步交给名册上的员工，不在这里叫模型。
//!
//! wiki 自己的活（存字节、建索引、检索）都是程序能干的，所以它们留在本 crate
//! 里；「这份来源讲的是什么、留下哪几条、页面该叫什么」是判断，只有通用智能体
//! 能干。这个模块做的就是那一次交接：把来源、要交回的形状写成一件活，投给名册
//! 上的任意一位员工，拿回答案后由这里写进 wiki。花的钱记在接活的
//! `agent:<id>` 上，本 crate 不持有 actor、不持有提示词之外的东西。
//!
//! 与 [`cog_core::WikiBackend::ingest_document`] 的区别就是「读没读」：那个方法
//! 只存字节，这个方法要先读一遍才能写，所以它花一次模型调用。

use std::sync::Arc;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use cog_core::{DistilledPage, SFError, SFResult, Task, WikiBackend, WikiDistiller};

/// 一次蒸馏最多把多少字的来源发出去。
///
/// 来源没有大小界（谁都可以投一份几百 KB 的文档），而任务预算是有界的：不截的话
/// 一件活会因为塞不进上下文而失败，读起来像「员工答不出来」。截掉的尾巴在这里
/// 写明，读的人能看出这份来源比发出去的长。
// cap: bound — 一份来源交给员工的上限；超出部分记在提示里而不是静默丢掉
pub const DISTILL_SOURCE_CHARS: usize = 8_000;

// cap: bound — 一件活声明自己会花多久；没有东西按这个数结束行，回收读的是租约与实测步间隔
const DISTILL_TASK_DECLARED_SECS: u64 = 180;
// cap: bound — 这里等答案的上界，比行声明的时长短，让等待的一方先说出自己为什么不再等
const DISTILL_WAIT_TIMEOUT_SECS: u64 = 170;

/// 把一件活交给名册、把答复拿回来的那一格。
///
/// 单独成一个 trait 是为了让这个模块能在没有编排器、没有名册的测试里跑：接到
/// 活的那一侧只认「一件活进、一份答复出」，不认它背后是谁。
#[async_trait]
pub trait WorkItemHandoff: Send + Sync {
    async fn answer(&self, task: Task) -> SFResult<serde_json::Value>;
}

/// 经编排器把活交给名册上的任意一位员工。
///
/// 编排器在装配完成之后才解析：wiki 插件与编排器插件之间没有排序边，`init` 期间
/// 读它拿到的只是「另一个插件恰好先跑完」。缺席是合法状态（这个进程没有任务图），
/// 那时这一条路报错，而不是静默写出一份没有读过的页面。
pub struct OrchestratorHandoff {
    orchestrator: cog_core::LateService<dyn cog_core::OrchestratorControl>,
}

impl OrchestratorHandoff {
    pub fn new(ctx: cog_core::PluginContext) -> Self {
        Self {
            orchestrator: cog_core::LateService::new(ctx),
        }
    }
}

#[async_trait]
impl WorkItemHandoff for OrchestratorHandoff {
    async fn answer(&self, task: Task) -> SFResult<serde_json::Value> {
        let Some(orchestrator) = self.orchestrator.get() else {
            return Err(SFError::Agent(
                "wiki distillation has no task graph in this process, so no employee could take \
                 the source"
                    .into(),
            ));
        };
        let source = task.input["task"]["source_path"]
            .as_str()
            .unwrap_or("(unnamed source)")
            .to_string();
        let goal = format!("Read the wiki source {source} and write one page from it");
        cog_core::contract::work_item::run_work_item(
            orchestrator.as_ref(),
            &goal,
            task,
            std::time::Duration::from_secs(DISTILL_WAIT_TIMEOUT_SECS),
        )
        .await
    }
}

/// 员工按这件活交回来的形状。
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct DistilledAnswer {
    /// 页面的标题：读者在目录与检索结果里看到的就是它。
    title: String,
    /// 页面正文，markdown。
    body: String,
    /// 主题标签，供按标签检索。
    #[serde(default)]
    tags: Vec<String>,
}

/// 用一件活把来源蒸馏成 wiki 页。
pub struct WorkItemDistiller {
    backend: Arc<dyn WikiBackend>,
    handoff: Arc<dyn WorkItemHandoff>,
    source_chars: usize,
}

impl WorkItemDistiller {
    pub fn new(backend: Arc<dyn WikiBackend>, handoff: Arc<dyn WorkItemHandoff>) -> Self {
        Self {
            backend,
            handoff,
            source_chars: DISTILL_SOURCE_CHARS,
        }
    }

    pub fn with_source_chars(mut self, chars: usize) -> Self {
        self.source_chars = chars;
        self
    }

    /// 交给员工的那句话。做法只写一次，材料与形状各在它们该在的字段里。
    const CONTRACT: &'static str = "Read the source in `task` and write one wiki page from it. \
         Say what the page is about, keep what a later reader would need, and drop the rest. \
         Answer with one JSON object conforming to this JSON Schema and nothing else -- no \
         markdown fences and no commentary.";

    fn bounded_source(&self, content: &str) -> String {
        let mut taken: String = content.chars().take(self.source_chars).collect();
        if taken.len() < content.len() {
            taken.push_str(&format!(
                "\n\n<source truncated: {} characters were sent of {}>",
                taken.chars().count(),
                content.chars().count()
            ));
        }
        taken
    }
}

#[async_trait]
impl WikiDistiller for WorkItemDistiller {
    async fn distill_source(&self, source_path: &str, content: &str) -> SFResult<String> {
        let schema = schemars::gen::SchemaGenerator::default().root_schema_for::<DistilledAnswer>();
        let schema_json = serde_json::to_string_pretty(&schema).map_err(SFError::Serialization)?;
        let contract = format!("{}\n\n{schema_json}", Self::CONTRACT);
        let mut input = serde_json::json!({
            cog_core::contract::prompt::PROMPT_CONTRACT_KEY: contract,
            "source_path": source_path,
        });
        input[cog_core::contract::prompt::REQUEST_KEYS[0]] = serde_json::json!({
            "source_path": source_path,
            "content": self.bounded_source(content),
        });

        // id 由路径与内容共同决定：路径决定落在哪一页，内容决定这一页是不是
        // 已经读过的那一版。只拿路径做键的话，来源改过之后重投会撞上平台上
        // 「同一件活只跑一次」的旧记录，页面上留下的就是改动之前那一版。
        let task_id = format!(
            "wiki-distill-{:08x}-{:08x}",
            fnv1a(source_path.as_bytes()),
            fnv1a(content.as_bytes())
        );
        let mut task = Task::new(
            task_id,
            cog_core::TaskType::Custom(cog_core::contract::work_item::PLATFORM_AGENT_TASK.into()),
            input,
        );
        task.action_planner_meta = Some(cog_core::ActionPlannerMeta {
            verified: true,
            version: Some("1.0.0".into()),
            note: Some("Wiki distillation; route to a standing employee".into()),
            source: Some(cog_core::ActionPlannerSource::UserProvided),
            confidence: None,
            timestamp: Some(chrono::Utc::now()),
        });
        task.declared_duration_seconds = DISTILL_TASK_DECLARED_SECS;
        // 场地标签跟着活走，不跟着接活的人走：读「wiki 这一栏今天收了多少」
        // 的人读的是这一列，而不是谁在这个场地上。
        task.venue = Some("wiki".into());
        task.acceptance.push(
            "The reply is one JSON object with a non-empty title and body, and the body is a \
             page about this source rather than a copy of it"
                .to_string(),
        );

        let answer = self.handoff.answer(task).await?;
        let value = answer_body(&answer)?;
        let distilled: DistilledAnswer =
            serde_json::from_value(value).map_err(SFError::Serialization)?;
        if distilled.title.trim().is_empty() || distilled.body.trim().is_empty() {
            return Err(SFError::Validation(
                "the answer carried an empty title or body, so this source was not distilled"
                    .into(),
            ));
        }

        let path = page_path(source_path);
        let page = DistilledPage {
            title: distilled.title.trim().to_string(),
            body: distilled.body,
            tags: distilled.tags,
        };
        let rendered = render_page(&path, &page);
        self.backend.update_document(&path, &rendered).await?;
        tracing::info!(
            source = source_path,
            page = %path,
            "wiki source distilled"
        );
        Ok(path)
    }
}

/// 员工的答复是 `{"result": "<text>"}` 这个信封；正文里再包一层 JSON。
fn answer_body(answer: &serde_json::Value) -> SFResult<serde_json::Value> {
    cog_core::contract::work_item::answer_object(answer).ok_or_else(|| {
        SFError::Validation(
            "the answer carried no JSON object, so this source was not distilled".into(),
        )
    })
}

/// 页面落点：来源路径的稳定函数。
///
/// 同一份来源再次蒸馏覆盖同一页——另起一页会让新旧两版同时在检索里出现，而读的
/// 人分不清哪一版是现在的。哈希那一段只为分开「不同路径被压成同一个 slug」的
/// 情况。
///
/// 命名空间跟着来源走：HTTP 写入按 `{workspace}/` 隔离，页面若一律落进全局的
/// `pages/`，写它的工作区在自己的前缀下反而看不到它，而未认证的全局视图看得到
/// ——隔离被这一步悄悄绕过去了。
fn page_path(source_path: &str) -> String {
    let (namespace, rest) = match source_path.split_once('/') {
        Some((namespace, rest)) => (format!("{namespace}/"), rest),
        None => (String::new(), source_path),
    };
    let slug = slug_of(rest);
    format!(
        "{namespace}pages/{slug}-{:08x}.md",
        fnv1a(source_path.as_bytes())
    )
}

/// 路径压成文件名可用的短串。
fn slug_of(path: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in path.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !slug.is_empty() {
            slug.push('-');
            last_dash = true;
        }
        if slug.len() >= 60 {
            break;
        }
    }
    slug.trim_matches('-').to_string()
}

/// FNV-1a，32 位。只用来把路径压成一个稳定短串，不是安全边界。
fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// 页面正文：标题、标签与内容。
///
/// 落盘的形状由这里定，不由员工定：员工写的是页面讲什么，格式是 wiki 的事。
fn render_page(path: &str, page: &DistilledPage) -> String {
    let tags = if page.tags.is_empty() {
        String::new()
    } else {
        let list = page
            .tags
            .iter()
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join(", ");
        format!("tags: [{list}]\n")
    };
    format!(
        "---\nsource_page: {path}\n{tags}---\n\n# {}\n\n{}\n",
        page.title,
        page.body.trim()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// 记下收到的活，回一份固定的答复。
    struct ScriptedHandoff {
        answer: String,
        tasks: Mutex<Vec<Task>>,
    }

    #[async_trait]
    impl WorkItemHandoff for ScriptedHandoff {
        async fn answer(&self, task: Task) -> SFResult<serde_json::Value> {
            self.tasks.lock().unwrap().push(task);
            Ok(serde_json::json!({ "result": self.answer }))
        }
    }

    /// 只把文档收进内存的 wiki。
    #[derive(Default)]
    struct MemoryWiki {
        docs: Mutex<BTreeMap<String, String>>,
    }

    #[async_trait]
    impl WikiBackend for MemoryWiki {
        async fn health_check(&self) -> bool {
            true
        }

        fn provider_name(&self) -> &str {
            "memory"
        }

        async fn ingest_document(&self, path: &str, content: &str) -> SFResult<()> {
            self.docs
                .lock()
                .unwrap()
                .insert(path.to_string(), content.to_string());
            Ok(())
        }

        async fn update_document(&self, path: &str, content: &str) -> SFResult<()> {
            self.ingest_document(path, content).await
        }

        async fn list_documents(&self) -> SFResult<Vec<cog_core::WikiDocument>> {
            Ok(self
                .docs
                .lock()
                .unwrap()
                .iter()
                .map(|(path, content)| cog_core::WikiDocument {
                    id: path.clone(),
                    path: path.clone(),
                    title: path.clone(),
                    content: content.clone(),
                    tags: None,
                    created_at: None,
                    updated_at: None,
                })
                .collect())
        }
    }

    fn distiller(answer: &str) -> (WorkItemDistiller, Arc<ScriptedHandoff>, Arc<MemoryWiki>) {
        let handoff = Arc::new(ScriptedHandoff {
            answer: answer.to_string(),
            tasks: Mutex::new(Vec::new()),
        });
        let wiki = Arc::new(MemoryWiki::default());
        (
            WorkItemDistiller::new(wiki.clone(), handoff.clone()),
            handoff,
            wiki,
        )
    }

    fn page_json() -> String {
        serde_json::json!({
            "title": "Auth tokens",
            "body": "Tokens are signed with the service key and expire after an hour.",
            "tags": ["auth", "tokens"],
        })
        .to_string()
    }

    /// 蒸馏是一件活：种类、自报的 verified、判据与材料都在行上。
    ///
    /// 少了任何一个，平台要么把它拆开去规划，要么派发不出去，而这两种在投递方
    /// 看来都是 202。
    #[tokio::test]
    async fn a_source_travels_as_a_work_item() {
        let (distiller, handoff, _) = distiller(&page_json());
        distiller
            .distill_source("notes/auth.md", "The source body")
            .await
            .expect("a working handoff answers");

        let tasks = handoff.tasks.lock().unwrap();
        let task = tasks.first().expect("the source was handed over");
        assert_eq!(
            task.task_type,
            cog_core::TaskType::Custom(cog_core::contract::work_item::PLATFORM_AGENT_TASK.into())
        );
        assert_eq!(
            task.action_planner_meta.as_ref().map(|m| m.verified),
            Some(true)
        );
        assert!(task.declared_duration_seconds > 0);
        assert_eq!(
            task.venue.as_deref(),
            Some("wiki"),
            "这一栏的读数按场地分组，漏了标签就落进「没有场地」"
        );
        assert!(
            !task.acceptance.is_empty(),
            "a verified row needs acceptance"
        );
        let contract = task.input[cog_core::contract::prompt::PROMPT_CONTRACT_KEY]
            .as_str()
            .expect("the item carries how to answer");
        assert!(contract.contains("JSON Schema"));
        assert_eq!(task.input["task"]["source_path"], "notes/auth.md");
        assert_eq!(task.input["task"]["content"], "The source body");
    }

    /// 答复是 `{"result": "<text>"}`，正文里再包一层 JSON（还可能带围栏）。
    #[tokio::test]
    async fn an_enveloped_answer_is_read_through() {
        let (distiller, _, wiki) = distiller(&format!("```json\n{}\n```", page_json()));
        let path = distiller
            .distill_source("notes/auth.md", "The source body")
            .await
            .expect("a fenced answer still names the page");
        let docs = wiki.docs.lock().unwrap();
        let page = docs.get(&path).expect("the page was written");
        assert!(page.contains("# Auth tokens"));
        assert!(page.contains("Tokens are signed with the service key"));
        assert!(page.contains("tags: [auth, tokens]"));
    }

    /// 没有对象就没有判词：不能据此写出一个空页。
    #[tokio::test]
    async fn an_answer_without_a_page_is_an_error_not_an_empty_page() {
        let (distiller, _, wiki) = distiller("I could not read this.");
        let err = distiller
            .distill_source("notes/auth.md", "The source body")
            .await
            .expect_err("no JSON object means nothing was distilled");
        assert!(matches!(err, SFError::Validation(_)));
        assert!(wiki.docs.lock().unwrap().is_empty());
    }

    /// 空标题同样是没蒸馏：写出去是一页没有名字的东西。
    #[tokio::test]
    async fn an_empty_title_is_an_error() {
        let (distiller, _, wiki) = distiller(
            &serde_json::json!({
                "title": "  ",
                "body": "something",
                "tags": [],
            })
            .to_string(),
        );
        assert!(distiller
            .distill_source("notes/auth.md", "The source body")
            .await
            .is_err());
        assert!(wiki.docs.lock().unwrap().is_empty());
    }

    /// 同一份来源重来一次落在同一页上：另起一页会让新旧两版同时被检索到。
    #[tokio::test]
    async fn the_same_source_lands_on_the_same_page() {
        let (distiller, _, wiki) = distiller(&page_json());
        let first = distiller
            .distill_source("notes/auth.md", "v1")
            .await
            .unwrap();
        let second = distiller
            .distill_source("notes/auth.md", "v2")
            .await
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(wiki.docs.lock().unwrap().len(), 1);
    }

    /// 页面留在来源自己的命名空间里。落到全局 `pages/` 的话，写它的工作区在
    /// 自己的前缀下看不到它，而未认证的全局视图看得到——隔离被这一步绕过去了。
    #[tokio::test]
    async fn a_page_stays_in_the_namespace_of_its_source() {
        let (distiller, _, _) = distiller(&page_json());
        let workspace = distiller
            .distill_source("ws-alpha/notes/auth.md", "The source body")
            .await
            .unwrap();
        let global = distiller
            .distill_source("notes/auth.md", "The source body")
            .await
            .unwrap();
        assert!(workspace.starts_with("ws-alpha/pages/"), "{workspace}");
        // 未认证的来源没有工作区前缀，第一段只是一个普通目录：页面留在那一
        // 段下面，仍然不出这个来源自己的范围。
        assert!(global.starts_with("notes/pages/"), "{global}");
        assert_ne!(workspace, global);
    }

    /// 来源改过之后重投是一次新活：任务 id 只拿路径做键的话，平台上「同一件活
    /// 只跑一次」会留下改动前那一版。
    #[tokio::test]
    async fn a_changed_source_is_a_new_item() {
        let (distiller, handoff, _) = distiller(&page_json());
        distiller
            .distill_source("notes/auth.md", "v1")
            .await
            .unwrap();
        distiller
            .distill_source("notes/auth.md", "v2")
            .await
            .unwrap();
        let tasks = handoff.tasks.lock().unwrap();
        assert_ne!(tasks[0].id, tasks[1].id);
    }

    /// 超出界的来源被截断时要写明：读的人看得出这份来源比发出去的长。
    #[tokio::test]
    async fn a_source_past_the_bound_says_so() {
        let handoff = Arc::new(ScriptedHandoff {
            answer: page_json(),
            tasks: Mutex::new(Vec::new()),
        });
        let wiki = Arc::new(MemoryWiki::default());
        let distiller = WorkItemDistiller::new(wiki, handoff.clone()).with_source_chars(10);
        distiller
            .distill_source("notes/long.md", "0123456789abcdef")
            .await
            .unwrap();
        let tasks = handoff.tasks.lock().unwrap();
        let sent = tasks[0].input["task"]["content"].as_str().unwrap();
        assert!(sent.starts_with("0123456789"));
        assert!(sent.contains("source truncated"));
        assert!(!sent.contains("abcdef"));
    }
}
