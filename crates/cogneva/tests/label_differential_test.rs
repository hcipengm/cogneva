//! principle: flat-agent-society
//!
//! 验收是**差分**，不是清点。
//!
//! 「角色不再决定行为」这句话没法靠读代码确认：读点分散在运行配置、评判标尺、输出
//! schema、任务类型与席位五处，每一处单看都像是对的，合起来才会露出「同一件活换个
//! 标签接手，待遇变了」。所以这里换一种判法：**同一份任务输入，两个不同的标签各走
//! 一遍**，把这条路上每一处决定待遇的读数收进一份记录，逐字段比。
//!
//! 记录分两半，边界不是手挑的：
//!
//! - **`who`**（运行身份、归因行、路由）**允许**不同：那是标签该回答的问题——这一次
//!   花销记在谁头上、这次运行以谁的名义留下痕迹。
//! - **`treatment`**（整份运行配置里决定待遇的每一格，加上判官收到的标尺、席位读到
//!   的 schema、任务类型与工具清单）**必须逐字相同**。任一格随标签变，记录就红，并
//!   点名是哪一个——那一个就是「还有哪一处按标签决定待遇」的剩余清单。
//!
//! 两半的划分由 `split_config` 对 `RuntimeConfig` 做**无 `..` 的解构**得出：配置多
//! 一个字段，解构先编译不过，作者必须回答它属于哪一半。这样，「新旋钮悄悄落在待遇
//! 外面」和「新待遇格没说由什么决定」都过不了编译或测试，不必靠人去读。
//!
//! 两个方向都判：只判「待遇相同」会被一对都取不到值的字段骗过去（注册表没认领任何
//! 标签时，两边都是 `None`，记录逐字相同而什么都没证明）。所以先断言两个标签在 `who`
//! 上**确实不同**，待遇那一半的相同才有意义。
//!
//! 判不了什么：决定待遇、却一个字节都不落进上面这些读数的地方——比如某个按标签切换
//! 的沙箱权限。它不改上限、标尺、schema、任务类型、工具清单或运行配置的任何一格，
//! 这份记录就看不见。这类地方要自己带一条同形状的读数进来，否则不在本门的覆盖里。

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
///
/// 两半的边界不是这里手挑的：`who` 与 `treatment` 由 [`split_config`] 从整份运行
/// 配置里按字段分出来，再加上判官实际收到的标尺、席位读到的那份 schema、任务自己
/// 的类型与进程装的那份工具清单。
#[derive(Debug, Clone)]
struct DecisionRecord {
    /// 标签**该**回答的那一半：谁在跑、花销记在哪一行、这次运行路由到哪。
    /// **允许**不同——这正是「换一个标签接手」的意思。
    who: serde_json::Value,
    /// 待遇那一半。**必须**逐字相同；摊成 JSON 是为了差异项能逐键点出来，
    /// 而不是只得到一句「不相等」。
    treatment: serde_json::Value,
}

/// 待遇的每一格由什么决定。终态只认三种来源：活自己声明了什么（`declared`）、
/// 接手这个人的实测交卷数据（`measured`）、操作者的显式覆盖（`override`）。
///
/// 这张表必须与记录里的待遇键逐个对上：新加一格却说不出它由这三者里的哪一个
/// 决定，测试当场红。`who` 那一半不在这张表里——名字与花销行本来就不决定待遇。
const TREATMENT_SOURCES: &[(&str, &str)] = &[
    ("deliverable", "declared"),
    ("deliverable_state", "declared"),
    ("deliverable_source", "declared"),
    ("stated_directions", "declared"),
    ("rubric", "declared"),
    ("schema", "declared"),
    ("task_type", "declared"),
    ("tools", "override"),
    // 操作者钉的数与本进程交卷运行的轮次读数；样本按标签共享，见上限那一处的门。
    ("max_iterations", "override+measured"),
    ("context_window_size", "override"),
    ("skill_cache_ttl_secs", "override"),
    ("think_stall_timeout_secs", "override"),
];

/// 把整份运行配置按字段分成两半：谁在跑，与这件活怎么被对待。
///
/// 这里**故意不写 `..`**。`RuntimeConfig` 多出一个字段时，这段解构编译不过，
/// 作者必须先回答它是 `who` 还是待遇，才能让测试重新编出来——一个悄悄落在待遇
/// 那一半外面的旋钮，是这份差分唯一看不见的坏法。
fn split_config(config: RuntimeConfig) -> (serde_json::Value, serde_json::Value) {
    let RuntimeConfig {
        agent_id,
        role,
        max_iterations,
        context_window_size,
        skill_cache_ttl_secs,
        think_stall_timeout_secs,
        skill_config,
        crew_id,
        squad_id,
        owner_task_id,
        deliverable,
        deliverable_state,
        stated_directions,
    } = config;
    let who = serde_json::json!({
        "agent_id": agent_id,
        "role": role,
        "attribution_row": skill_config.map(|row| row.skill_id),
        "crew_id": crew_id,
        "squad_id": squad_id,
        "owner_task_id": owner_task_id,
    });
    let treatment = serde_json::json!({
        "max_iterations": max_iterations,
        "context_window_size": context_window_size,
        "skill_cache_ttl_secs": skill_cache_ttl_secs,
        "think_stall_timeout_secs": think_stall_timeout_secs,
        "deliverable": format!("{deliverable:?}"),
        "deliverable_state": deliverable_state.as_str(),
        "stated_directions": stated_directions,
    });
    (who, treatment)
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
        // 判表要的是「一个要求一行」：引规格里的一句、正文里的一句，状态 met。
        // 行数是 1 而不是 0——空表在判官那里是「一条都没判」，会直接报错。
        ChatResponse {
            content: vec![ContentBlock::text(
                r#"{"rows":[{"requirement":"the text does t1 and t2","spec_quote":"t1 and t2 must be done","status":"met","evidence":"t1: read the file, then t2: deploy"}]}"#,
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

/// 这件活本身：交什么由任务说，有人就「往哪儿算好」说过的原话也留在任务上。
/// 两个标签接手的是**同一个任务对象**，不是各拼一份。
fn declared_work() -> serde_json::Value {
    serde_json::json!({
        cog_core::types::task::INPUT_DELIVERS: "change",
        cog_core::INPUT_SIGNAL: {
            "kind": cog_core::TASTE_SIGNAL_KIND,
            "taste_kind": "direction_preference",
            "payload": {
                "kind": "direction_preference",
                "preferred": "quote the failing line",
                "alternative": "summarise the change",
                "rationale": "a summary cannot be checked",
            },
        },
    })
}

/// 让 `label` 接手同一份活，把这一路的待遇收进一条记录。
async fn record_for(label: &str, tools: &[String]) -> DecisionRecord {
    let task = Task::new("t-1", TaskType::Custom("change".into()), declared_work());
    let declared = task.deliverable();
    let directions = task.stated_directions();

    // 运行配置：生产上创建 agent 的那一处。任务声明在拿任务的那一层抄进配置，
    // 这里照做，免得这条记录量的是「测试自己拼的配置」而不是运行时会拿到的那一份。
    let base = RuntimeConfig {
        deliverable: declared,
        deliverable_state: task.deliverable_declaration().state(),
        stated_directions: directions.clone(),
        ..Default::default()
    };
    let registry = Arc::new(RwLock::new(two_hands()));
    let config = runtime_config_for(&base, Some(&registry), "agent-1", label).await;

    // 评判标尺：与运行时入口同一处构造（`Agent::self_review_loop`），只把 actor 换成标签。
    //
    // 走判表（`revise: false`）：标尺只在这条路上进请求。整轮改写走的是批评与
    // 比较两步，那两份提示里没有标尺——从那里量到的「rubric」两边当然相同，
    // 因为它根本不是标尺。
    let recorder = Recorder::new();
    let loop_ = SelfReviewLoop::new(SelfReviewConfig {
        spec: Some(SPEC.into()),
        revise: false,
        ..Default::default()
    })
    .with_actor(self_review_actor(label))
    .with_deliverable(declared)
    .with_stated_directions(directions);
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

    let (who, mut treatment) = split_config(config);
    let fields = treatment
        .as_object_mut()
        .expect("split_config 的待遇那一半是一张表");
    fields.insert("rubric".into(), serde_json::json!(rubric));
    fields.insert(
        "schema".into(),
        serde_json::json!(settings.schema_for(SEAT)),
    );
    fields.insert(
        "task_type".into(),
        serde_json::json!(format!("{:?}", task.task_type)),
    );
    fields.insert("tools".into(), serde_json::json!(tools));
    // 声明连同来源一起收进来：这道差分不只比结论，也比「这个结论是谁说的」。
    // 两个标签读的是同一个任务对象，来源那一栏相同才说明待遇跟声明走、不跟标签走。
    fields.insert(
        "deliverable_source".into(),
        serde_json::to_value(task.deliverable_declaration()).expect("声明总是可序列化"),
    );
    DecisionRecord { who, treatment }
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
        planner.who, generator.who,
        "两个标签没被区分开，这份差分什么都没证明"
    );
    assert_eq!(planner.who["role"], "planner");
    assert_eq!(generator.who["role"], "generator");
    assert_eq!(
        planner.who["attribution_row"], "planner",
        "planner 的归因行没挂上：注册表没认领这个标签，待遇那半的相等也就不作数"
    );
    assert_eq!(
        generator.who["attribution_row"], "generator",
        "generator 的归因行没挂上：注册表没认领这个标签，待遇那半的相等也就不作数"
    );

    // 再逐字段比待遇：差异项就是「还有哪一处按标签决定」的剩余清单。
    let a = &planner.treatment;
    let b = &generator.treatment;
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

    // 记录里的每一格都要说出它由什么决定；表与记录对不上，这扇门就漏了一格。
    let mut recorded: Vec<&str> = a
        .as_object()
        .expect("待遇是一张表")
        .keys()
        .map(String::as_str)
        .collect();
    recorded.sort_unstable();
    let mut registered: Vec<&str> = TREATMENT_SOURCES.iter().map(|(field, _)| *field).collect();
    registered.sort_unstable();
    assert_eq!(
        recorded, registered,
        "待遇表与记录对不上：多出来的一格没登记它由什么决定，少一格是表过期了"
    );
    for (field, source) in TREATMENT_SOURCES {
        for part in source.split('+') {
            assert!(
                matches!(part, "declared" | "measured" | "override"),
                "`{field}` 的来源写成 `{part}`：决定待遇的只许是活自己的声明、\
                 接手人的实测交卷、操作者的显式覆盖"
            );
        }
    }

    // 任务说的是什么，记录就该读到什么——不然上面那一行「相同」可能两处都错得一样。
    assert_eq!(planner.treatment["deliverable"], "Change");
    assert_eq!(
        planner.treatment["max_iterations"],
        serde_json::json!(u32::MAX),
        "没人钉上限时不该凭空出现一个"
    );
    // 人的原话确实进了标尺：否则「两边都空」也会让待遇那半相等。
    assert!(
        planner.treatment["rubric"]
            .as_str()
            .is_some_and(|rubric| rubric.contains("quote the failing line")),
        "任务上写明的方向没有走到判尺上：这条差分量到的 rubric 是空的"
    );
}
