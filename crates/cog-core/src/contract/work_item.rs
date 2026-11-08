//! 一件交给名册上任意员工的活——场景与员工之间的那一层。
//!
//! 平台上有两类东西，形状很像、位置不同：**场景**知道要做什么（这一份材料
//! 该按什么判据看、答案写给谁），**员工**是会干活的通用智能体。场景不持有
//! 身份、不持有提示词、不占一个 actor；它只交出一件活，材料、做法、验收判据
//! 都在活里，谁接谁干。把这两者粘在一起的那种做法——给每个场景在平台里写
//! 一个专用分支、一段专用提示词、一次一次性模型调用——会让每个场景都变成
//! 一个写进平台的角色。
//!
//! 这件活就是一个 [`crate::Task`]，种类是 [`PLATFORM_AGENT_TASK`]，其余全部
//! 用任务行上已经会被人读到的字段：
//!
//! - **材料与做法**：`input` 文档。[`super::prompt::PROMPT_CONTRACT_KEY`] 是
//!   做法（渲染成开头的 system 消息），[`super::prompt::REQUEST_KEYS`] 是这一次
//!   的请求（渲染在用户消息最前），[`super::prompt::MEDIA_KEY`] 是附件（渲染成
//!   内容块，不是 base64 文本）。
//! - **验收判据**：`acceptance`。可证伪的一句一句写，答案对不对由它判，不由
//!   接活的人的角色判。
//! - **预算**：`timeout_seconds`。平台不拿它砍任何一条正在跑的活，所以它只是
//!   交活那一侧自己愿意等多久的声明；另造一个没人读的预算字段只会让两处数字
//!   各说各话。
//! - **写回**：`TaskResult`，答案原样交给交活的那一方；`metadata` 记 `agent:<id>`，
//!   所以事后能说清是谁干的，而不是「平台干的」。
//!
//! 常数放在这里而不是各自 crate：交活的一方（场景）与接活的一方（执行器）
//! 是两个 crate，一个种类名两边各拼一次，就会有一边改了另一边不认。

/// 由名册上任意员工接的活。
///
/// 与那些绑在某个角色上的任务不同：这件活不挑人，材料与判据挑人。
pub const PLATFORM_AGENT_TASK: &str = "platform_agent_task";

use std::time::Duration;

use async_trait::async_trait;

/// 交接读回任务行的间隔。
// cap: bound — 答案被读到的延迟就是这一格；再密一点，安静的那一方也成了负载
pub const WORK_ITEM_POLL: Duration = Duration::from_millis(500);

/// 交接只向任务图问两个问题。
///
/// 单独成一个窄口，是因为来交活的一方只该知道「投进去」「读回来」两件事：
/// 把整个编排器控制面交给它，等于让每个场景都能碰图上的别的东西。窄口也让
/// 交接本身能在没有编排器的测试里跑。
#[async_trait]
pub trait WorkItemGraph: Send + Sync {
    /// 把活投进图里，返回真正落下的行 id（已存在时可能只有返回、没有插入）。
    async fn submit(&self, goal: &str, tasks: Vec<crate::Task>) -> crate::SFResult<Vec<String>>;
    /// 按 id 读回一行；图里没有这一行时是 `None`，不是错误。
    async fn row(&self, id: &str) -> Option<crate::Task>;
}

/// 编排器就是这样一个任务图：窄口的两问都已经是它公开的两问。
#[async_trait]
impl<T: crate::OrchestratorControl + ?Sized> WorkItemGraph for T {
    async fn submit(&self, goal: &str, tasks: Vec<crate::Task>) -> crate::SFResult<Vec<String>> {
        self.submit_goal_auto(goal, tasks).await
    }

    async fn row(&self, id: &str) -> Option<crate::Task> {
        self.get_task(id).await
    }
}

/// 投一件活进去，等它到终态，把答案交回来。
///
/// `wait` 是**这一侧**的耐心，与任务行上的 `timeout_seconds` 不是一回事：平台
/// 不按已用时长收任何正在跑的活，所以「我们不等了」与「这件活失败了」本来就是
/// 两句不同的话——前者返回时任务还在跑，它的结果仍然会落到行上。两处取同一个
/// 数只会让这两种结局在时间上撞在一起，让读的人以为它们有因果。
///
/// 等待期间只读这一行，不重投：投递是幂等的，重投只会让「已存在的行」与
/// 「刚插入的行」这两种情况在本函数的返回值里长得一样。
pub async fn run_work_item<G: WorkItemGraph + ?Sized>(
    graph: &G,
    goal: &str,
    task: crate::Task,
    wait: Duration,
) -> crate::SFResult<serde_json::Value> {
    let id = task.id.clone();
    let submitted = graph.submit(goal, vec![task]).await?;
    let id = submitted.into_iter().next().unwrap_or(id);
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        if let Some(row) = graph.row(&id).await {
            match row.status {
                crate::TaskStatus::Completed => {
                    return row.result.ok_or_else(|| {
                        crate::SFError::Agent(format!(
                            "the work item {id} completed and stored no answer"
                        ))
                    })
                }
                crate::TaskStatus::Failed | crate::TaskStatus::Cancelled => {
                    return Err(crate::SFError::Agent(format!(
                        "the work item {id} ended {}: {}",
                        row.status.as_str(),
                        row.error.as_deref().unwrap_or("it recorded no reason")
                    )))
                }
                _ => {}
            }
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(crate::SFError::Agent(format!(
                "the work item {id} did not reach a terminal state within {}s",
                wait.as_secs()
            )));
        }
        tokio::time::sleep(WORK_ITEM_POLL.min(deadline - now)).await;
    }
}

/// 员工答复里的那个 JSON 对象。
///
/// 答复的信封是 `{"result": "<text>"}`，正文里还可能再包一层围栏或一句解释；
/// 两种都要读穿。取不到对象时返回 `None` —— 交活的一方各自知道这一次没读到
/// 意味着什么，所以「没读到」的措辞留给它，不在这里写成一句通用的话。
pub fn answer_object(answer: &serde_json::Value) -> Option<serde_json::Value> {
    let text = match answer {
        serde_json::Value::Object(map) => match map.get("result") {
            Some(serde_json::Value::String(text)) => text,
            Some(other) => return Some(other.clone()),
            None => return Some(answer.clone()),
        },
        _ => return None,
    };
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        return Some(value);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if start >= end {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(&trimmed[start..=end]).ok()
}

/// 员工答复的正文：信封里那个字符串，别的原样转成文本。
///
/// 判分、写页这些地方要的是答案本身；把信封原样交出去，被判的是一段 JSON
/// 包装，而分数或页面掉下来时读的人只会以为答错了。
pub fn answer_text(answer: &serde_json::Value) -> String {
    match answer {
        serde_json::Value::Object(map) => match map.get("result") {
            Some(serde_json::Value::String(text)) => text.clone(),
            Some(other) => other.to_string(),
            None => answer.to_string(),
        },
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 一个只会把行从 `Running` 翻到 `Completed` 的任务图。
    struct ScriptedGraph {
        row: Mutex<crate::Task>,
        polls: Mutex<usize>,
        complete_on_poll: usize,
        answer: serde_json::Value,
    }

    impl ScriptedGraph {
        fn new(complete_on_poll: usize, answer: serde_json::Value) -> Self {
            let mut row = crate::Task::new(
                "item-1",
                crate::TaskType::Custom(PLATFORM_AGENT_TASK.into()),
                serde_json::json!({}),
            );
            row.status = crate::TaskStatus::Running;
            Self {
                row: Mutex::new(row),
                polls: Mutex::new(0),
                complete_on_poll,
                answer,
            }
        }
    }

    #[async_trait]
    impl WorkItemGraph for ScriptedGraph {
        async fn submit(
            &self,
            _goal: &str,
            _tasks: Vec<crate::Task>,
        ) -> crate::SFResult<Vec<String>> {
            Ok(vec!["item-1".into()])
        }

        async fn row(&self, id: &str) -> Option<crate::Task> {
            let mut polls = self.polls.lock().unwrap();
            *polls += 1;
            let poll = *polls;
            let mut row = self.row.lock().unwrap().clone();
            if poll >= self.complete_on_poll {
                row.status = crate::TaskStatus::Completed;
                row.result = Some(self.answer.clone());
            }
            (row.id == id).then_some(row)
        }
    }

    /// 一件活投进去，等它完成，答案原样交回来。
    #[tokio::test]
    async fn an_answer_comes_back_once_the_row_completes() {
        let graph = ScriptedGraph::new(2, serde_json::json!({"result": "ok"}));
        let task = crate::Task::new(
            "item-1",
            crate::TaskType::Custom(PLATFORM_AGENT_TASK.into()),
            serde_json::json!({}),
        );
        let answer = run_work_item(&graph, "judge it", task, Duration::from_secs(5))
            .await
            .expect("the row completes");
        assert_eq!(answer["result"], "ok");
    }

    /// 一等再等不到终态时，「我们不等了」是一句和「活超时了」不同的话。
    #[tokio::test]
    async fn a_row_that_never_settles_ends_the_wait_with_its_own_reason() {
        let graph = ScriptedGraph::new(usize::MAX, serde_json::json!({}));
        let task = crate::Task::new(
            "item-1",
            crate::TaskType::Custom(PLATFORM_AGENT_TASK.into()),
            serde_json::json!({}),
        );
        let err = run_work_item(&graph, "judge it", task, Duration::from_millis(50))
            .await
            .expect_err("nothing settles");
        assert!(err.to_string().contains("did not reach a terminal state"));
    }

    /// 员工交回 `{"result": "<text>"}`，正文里可能带围栏或一句话。
    #[test]
    fn the_answer_envelope_is_read_through() {
        let fenced = serde_json::json!({"result": "here you go:\n```json\n{\"a\": 1}\n```"});
        assert_eq!(answer_object(&fenced), Some(serde_json::json!({"a": 1})));

        let plain = serde_json::json!({"result": "{\"a\": 2}"});
        assert_eq!(answer_object(&plain), Some(serde_json::json!({"a": 2})));

        let direct = serde_json::json!({"a": 3});
        assert_eq!(answer_object(&direct), Some(serde_json::json!({"a": 3})));
    }

    /// 要正文的地方拿到的是答案本身，不是包着它的那层信封。
    #[test]
    fn the_answer_text_is_the_text_inside_the_envelope() {
        assert_eq!(answer_text(&serde_json::json!({"result": "4"})), "4");
        assert_eq!(answer_text(&serde_json::json!("4")), "4");
        assert_eq!(
            answer_text(&serde_json::json!({"result": {"n": 4}})),
            "{\"n\":4}"
        );
    }

    /// 读不出对象就是没读到，不是一个空对象。
    #[test]
    fn an_answer_without_an_object_reads_as_nothing() {
        assert_eq!(
            answer_object(&serde_json::json!({"result": "I cannot tell."})),
            None
        );
        assert_eq!(answer_object(&serde_json::json!("just text")), None);
    }
}
