//! principle: flat-agent-society
//!
//! 验收是**差分**，不是清点。
//!
//! 「角色不再决定行为」这句话没法靠读代码确认：读点分散在运行配置、评判标尺、输出
//! schema、任务类型与席位五处，每一处单看都像是对的，合起来才会露出「同一件活换个
//! 标签接手，待遇变了」。所以这里换一种判法：**同一份任务输入，两个不同的标签各走
//! 一遍**，把这条路上每一处决定待遇的读数收进一份记录，逐字段比。
//!
//! 记录分两半，边界写在类型上：
//!
//! - **身份**（`identity`）与**归因行**（`attribution_row`）**允许**不同：那是标签
//!   该回答的问题——这一次花销记在谁头上、这次运行以谁的名义留下痕迹。
//! - **待遇**（`ceiling` / `deliverable` / `rubric` / `schema` / `task_type` / `tools`）
//!   **必须逐字相同**。任一字段随标签变，这份记录就红，并点名是哪一个——那一个就是
//!   「还有哪一处按标签决定待遇」的剩余清单，不需要再靠人去读。
//!
//! 两个方向都判：只判「待遇相同」会被一对都取不到值的字段骗过去（注册表没认领任何
//! 标签时，两边都是 `None`，记录逐字相同而什么都没证明）。所以先断言两个标签在身份
//! 与归因上**确实不同**，待遇那一半的相同才有意义。
//!
//! 可点工具这一栏读的是进程装的那一份清单（`register_builtins` 一处注册），不由标签
//! 分发。把它记进来是为了让「有一天它开始按标签分发」这件事落在判据里，不是因为今天
//! 它可能不同。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::RwLock;

use cog_agent::builtins::{register_builtins, BuiltinInputs};
use cog_agent::runtime::manager::runtime_config_for;
use cog_agent::{self_review::self_review_actor, SelfReviewLoop, ToolRegistry};
use cog_collaboration::config::PgeSettings;
use cog_core::{
    AssistantMessageEvent, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions,
    ContentBlock, LlmClient, Message, RuntimeConfig, SFResult, SelfReviewConfig, SkillConfig,
    SkillRegistry, StopReason, Task, TaskType, Usage,
};

/// 这一份任务：两个标签接手的是**同一件**活。
const SPEC: &str = "t1 and t2 must be done";
const DELIVERED: &str = "t1: read the file, then t2: deploy";
/// 协议上的席位名（`schemas` 表的键）。席位是寻址，不是谁的标签。
const SEAT: &str = "generator";

/// 一条决策记录：这条路上每一处会决定「这件活怎么被对待」的读数。
#[derive(Debug, Clone)]
struct DecisionRecord {
    /// 谁在跑。**允许**不同——这正是换标签的意思。
    identity: String,
    /// 归因行（技能行的 id）。**允许**不同：它只把花销记到某只手上。
    attribution_row: Option<String>,
    /// 以下全部**必须**相同：它们决定这件活被怎么对待。
    ceiling: u32,
    deliverable: String,
    rubric: String,
    schema: Option<serde_json::Value>,
    task_type: String,
    tools: Vec<String>,
}

impl DecisionRecord {
    /// 待遇那一半，摊成 JSON：差异项因此可以逐键点出来，而不是只得到一句「不相等」。
    fn treatment(&self) -> serde_json::Value {
        serde_json::json!({
            "ceiling": self.ceiling,
            "deliverable": self.deliverable,
            "rubric": self.rubric,
            "schema": self.schema,
            "task_type": self.task_type,
            "tools": self.tools,
        })
    }
}

/// 判一次，只把请求正文记下来：标尺是不是同一份，从外面只看得到这一段。
struct Recorder {
    payloads: Mutex<Vec<String>>,
    actors: Mutex<Vec<String>>,
}

impl Recorder {
    fn new() -> Self {
        Self {
            payloads: Mutex::new(Vec::new()),
            actors: Mutex::new(Vec::new()),
        }
    }

    fn last(&self) -> (String, String) {
        let payload = self
            .payloads
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("一次评判一次请求");
        let actor = self
            .actors
            .lock()
            .unwrap()
            .last()
            .cloned()
            .unwrap_or_else(|| "none".into());
        (payload, actor)
    }

    fn response(&self) -> ChatResponse {
        ChatResponse {
            content: vec![ContentBlock::text(
                r#"{"issues":[],"missing":[],"strengths":[],"score":1.0,"gaps":[],"aligned":[]}"#,
            )],
            api: "recorder".into(),
            provider: "recorder".into(),
            model: "recorder".into(),
            response_id: None,
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        }
    }
}

#[async_trait]
impl LlmClient for Recorder {
    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        let text = messages
            .iter()
            .map(|m| m.content())
            .collect::<Vec<_>>()
            .join("\n");
        self.payloads.lock().unwrap().push(text);
        self.actors.lock().unwrap().push(
            options
                .headers
                .get(cog_core::LLM_ACTOR_HEADER)
                .cloned()
                .unwrap_or_else(|| "none".into()),
        );
        Ok(self.response())
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let response = self.chat(messages, options).await?;
        let text = response
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect::<String>();
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(cog_core::DEFAULT_STREAM_CAPACITY);
        let message = response.clone();
        tokio::spawn(async move {
            let _ = producer
                .push(AssistantMessageEvent::Done {
                    reason: StopReason::Stop,
                    message: Message::assistant_text(text),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            producer.end(message);
        });
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, _) = AssistantMessageEventStream::with_capacity(1);
        Ok(stream)
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// 执行器表里的两行：两个标签各认领一行，行本身只有身份与种类。
fn two_hands() -> SkillRegistry {
    let mut registry = SkillRegistry::new();
    for (id, name) in [("planner", "Planner"), ("generator", "Generator")] {
        registry.insert_skill_config(SkillConfig {
            skill_id: id.into(),
            name: name.into(),
            role_type: id.into(),
        });
    }
    registry
}

/// 进程装的那一份工具清单，两个标签拿到的是同一份。
fn installed_tools() -> Vec<String> {
    let registry = ToolRegistry::new();
    register_builtins(&registry, BuiltinInputs::default());
    registry.names()
}

/// 让 `label` 接手同一份活，把这一路的待遇收进一条记录。
async fn record_for(label: &str, tools: &[String]) -> DecisionRecord {
    // 这件活本身：任务类型与「交什么」都由任务说，两个标签读到的是同一个任务对象。
    let task = Task::new(
        "t-1",
        TaskType::Custom("change".into()),
        serde_json::json!({"delivers": "change"}),
    );

    // 运行配置：生产上创建 agent 的那一处（角色行只挂上去做归因）。
    let registry = Arc::new(RwLock::new(two_hands()));
    let config =
        runtime_config_for(&RuntimeConfig::default(), Some(&registry), "agent-1", label).await;
    let attribution_row = config.skill_config.as_ref().map(|row| row.skill_id.clone());

    // 评判标尺：与运行时入口同一处构造（`Agent::self_review_loop`），只把 actor 换成标签。
    let recorder = Recorder::new();
    let loop_ = SelfReviewLoop::new(SelfReviewConfig {
        spec: Some(SPEC.into()),
        ..Default::default()
    })
    .with_actor(self_review_actor(label))
    .with_deliverable(task.deliverable());
    loop_
        .review(DELIVERED, &recorder)
        .await
        .expect("评判跑得完");
    let (rubric, _actor) = recorder.last();

    // 结构化输出：按席位查，不按标签查。两个标签坐的**同一个**席位。
    let settings = PgeSettings {
        schemas: HashMap::from([(
            SEAT.to_string(),
            serde_json::json!({"type": "object", "required": ["change"]}),
        )]),
        ..Default::default()
    };

    DecisionRecord {
        identity: label.to_string(),
        attribution_row,
        ceiling: config.max_iterations,
        deliverable: format!("{:?}", task.deliverable()),
        rubric,
        schema: settings.schema_for(SEAT),
        task_type: format!("{:?}", task.task_type),
        tools: tools.to_vec(),
    }
}

/// 同一份任务输入，两个标签各走一遍：待遇逐字段相等，身份与归因确有其差别。
#[tokio::test]
async fn decision_record() {
    let tools = installed_tools();
    assert!(
        !tools.is_empty(),
        "进程装出来的工具清单是空的：这一栏什么都没量到，不是「两边一样」"
    );

    let planner = record_for("planner", &tools).await;
    let generator = record_for("generator", &tools).await;

    // 先证两条记录确实来自两个不同的标签：否则下面那一半的「相等」可能只是两边都空。
    assert_ne!(
        planner.identity, generator.identity,
        "两个标签没被区分开，这份差分什么都没证明"
    );
    assert_eq!(
        planner.attribution_row.as_deref(),
        Some("planner"),
        "planner 的归因行没挂上：注册表没认领这个标签，待遇那半的相等也就不作数"
    );
    assert_eq!(
        generator.attribution_row.as_deref(),
        Some("generator"),
        "generator 的归因行没挂上：注册表没认领这个标签，待遇那半的相等也就不作数"
    );

    // 再逐字段比待遇：差异项就是「还有哪一处按标签决定」的剩余清单。
    let a = planner.treatment();
    let b = generator.treatment();
    let mut differing: Vec<String> = Vec::new();
    if let (Some(a), Some(b)) = (a.as_object(), b.as_object()) {
        for (field, value) in a {
            if b.get(field) != Some(value) {
                differing.push(field.clone());
            }
        }
    }
    assert!(
        differing.is_empty(),
        "这些待遇字段随标签变了：{:?}\nplanner:   {}\ngenerator: {}",
        differing,
        a,
        b
    );

    // 任务说的是什么，记录就该读到什么——不然上面那一行「相同」可能两处都错得一样。
    assert_eq!(planner.deliverable, "Change");
    assert_eq!(planner.ceiling, u32::MAX, "没人钉上限时不该凭空出现一个");
}
