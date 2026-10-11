//! HLE（Humanity's Last Exam）的四件适配器：数据、环境、工具包、判分。
//!
//! 这一列与另两列的关键分野：它**没有环境**（纯 API），工具面是「web search + Python」，
//! 而且是三个基准里**唯一要生成式裁判**的一列——另两列跑确定性脚本，上游模型死活与否
//! 都能出分。所以这里的判分器在裁判上游缺席时的行为必须清楚：**报错，不是判错**。
//! 报错让运行器记成 `errored`（这一格不是这一行的数，是上游的），判错会让它读成
//! 「模型答错了」——两者在表里同形，正是要分开的那两件事。

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{execute_structured, ChatOptions, LlmClient, Message, Tool};
use serde::Deserialize;

use crate::bench::{CaseJudge, CaseSource, ToolFactory, Toolkit, Verdict};
use crate::dataset::EvalCase;
use crate::scaffold::{AgentOutput, CaseEnv, ToolSet};

/// 取数脚本给 HLE 产出的文本形式在数据根里的相对路径。
///
/// 与 parquet 同目录、同名，只把扩展名换成 `.jsonl`。名字写在这里一处，
/// 取数脚本那一端也照同一个约定落文件——两处各写一份拼写就会有两份对不上的说法。
pub const HLE_JSONL: &str = "hle/test-00000-of-00001.jsonl";

/// 官方判分模板的版本。判分器**固定**用它、并把它钉进 `Verdict.readings`：
/// 换模板就是换判据，不同的格子用了不同的判据，表就不可比了。
pub const HLE_JUDGE_TEMPLATE_VERSION: &str = "hle-model-graded-fact-v1";

/// HLE 官方（`hle_eval/run_judge_results.py`）的 `model_graded_fact` 判分模板，
/// 逐字照抄。占位符 `{question}` / `{correct_answer}` / `{response}`。
///
/// 借的是**判据的形状**，不是它的判分器：上游裁判模型由调用方经 [`LlmClient`] 注入、
/// 与它无关，这样「换了裁判模型」是配置而不是改判据。
const HLE_JUDGE_TEMPLATE: &str = "Judge whether the following [response] to [question] is correct or not based on the precise and unambiguous [correct_answer] below.

[question]: {question}

[response]: {response}

Your judgement must be in the format and criteria specified below:

extracted_final_answer: The final exact answer extracted from the [response]. Put the extracted answer as 'None' if there is no exact, final answer to extract from the response.

[correct_answer]: {correct_answer}

reasoning: Explain why the extracted_final_answer is correct or incorrect based on [correct_answer], focusing only on if there are meaningful differences between [correct_answer] and the extracted_final_answer. Do not comment on any background to the problem, do not attempt to solve the problem, do not argue for any answer different than [correct_answer], focus only on whether the answers match.

correct: Answer 'yes' if extracted_final_answer matches the [correct_answer] given above, or is within a small margin of error for numerical problems. Answer 'no' otherwise, i.e. if there if there is any inconsistency, ambiguity, non-equivalency, or if the extracted answer is incorrect.

confidence: The extracted confidence score between 0 and 100 from [response]. Put 100 if there is no confidence score available.";

/// HLE 数据适配器：读文本形式的 parquet 行。
///
/// 读文本而不是读 parquet，是因为 parquet 解码器（pyarrow 那样的 ~50 MiB wheel）
/// 刻意不进仓库；由取数脚本在取数时把同一批行、同一顺序落成一行一条。
/// 题面的图像以 parquet 本就带的 data URL 原样带在 `input.image` 上，
/// 所以多模态外壳不需要旁挂文件；纯文本外壳读 `metadata.has_image` 自行取舍。
pub struct HleCaseSource;

#[derive(Debug, Deserialize)]
struct HleRow {
    id: String,
    question: String,
    #[serde(default)]
    image: Option<String>,
    answer: String,
    answer_type: String,
    #[serde(default)]
    rationale: Option<String>,
    #[serde(default)]
    raw_subject: Option<String>,
    #[serde(default)]
    category: Option<String>,
}

impl HleCaseSource {
    fn row_to_case(row: HleRow) -> EvalCase {
        let has_image = row
            .image
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("answer_type".into(), row.answer_type.clone());
        metadata.insert("has_image".into(), has_image.to_string());
        if let Some(v) = &row.category {
            metadata.insert("category".into(), v.clone());
        }
        if let Some(v) = &row.raw_subject {
            metadata.insert("raw_subject".into(), v.clone());
        }
        if let Some(v) = &row.rationale {
            metadata.insert("rationale".into(), v.clone());
        }
        let mut tags = vec![
            "hle".to_string(),
            format!("answer_type:{}", row.answer_type),
        ];
        if let Some(v) = &row.category {
            tags.push(format!("category:{v}"));
        }
        EvalCase {
            id: format!("hle-{}", row.id),
            name: format!("hle-{}", row.id),
            input: serde_json::json!({
                "question": row.question,
                "image": if has_image { row.image } else { None },
            }),
            expected_output: Some(serde_json::Value::String(row.answer)),
            expected_tools: None,
            tags,
            // 判分是裁判给的 0/1，不是字符串完全相等，所以这里不挂一个会误导的指标名。
            metrics: vec![],
            metadata,
        }
    }
}

#[async_trait]
impl CaseSource for HleCaseSource {
    fn cases(&self, root: &Path) -> anyhow::Result<Vec<EvalCase>> {
        let path = root.join(HLE_JSONL);
        let content = std::fs::read_to_string(&path).map_err(|e| {
            anyhow::anyhow!(
                "cannot read {}: {e} (HLE ships as a parquet; run the fetch script with --convert \
                 against the same --dest to write the text form this reads)",
                path.display()
            )
        })?;
        let mut cases = Vec::new();
        for (lineno, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let row: HleRow = serde_json::from_str(line)
                .map_err(|e| anyhow::anyhow!("{} line {}: {e}", path.display(), lineno + 1))?;
            cases.push(Self::row_to_case(row));
        }
        Ok(cases)
    }
}

/// HLE 工具名的钉死值。工具集由**基准**定、不由方法定：四个外壳在同一列上拿到的
/// 是同一束工具，多一件少一件都会让表里的差掺进「谁的工具多」。
pub const HLE_TOOL_WEB_SEARCH: &str = "web_search";
pub const HLE_TOOL_PYTHON: &str = "python";

/// HLE 的工具包。
///
/// 工具的**实现**（搜索 API 供应商、Python 沙盒）从外面注入——这一层只负责协议：
/// 恰好这两件、名字钉死。实现没接时构造就失败，而不是给出一束空工具让分数悄悄掉下去：
/// 「没有工具」与「工具没接上」在分数上同形，必须在这一层就分开。
pub struct HleToolkit {
    /// `None` ＝ 关工具的那一次跑。
    factory: Option<Arc<ToolFactory>>,
}

impl HleToolkit {
    /// 工具开的那一套，清单**按题现造**。
    ///
    /// HLE 没有环境，所以今天的工厂忽略 `case`/`env` 交回同一束；留成工厂是与另两列
    /// 同一形状：将来某题的工具要绑到那道题的环境上时，不必再改这一层的协议。
    pub fn with_factory(factory: Arc<ToolFactory>) -> Self {
        Self {
            factory: Some(factory),
        }
    }

    /// 清单不随题变的那一套（HLE 今天就是这一种）。构造时先核一遍，早失败。
    pub fn with_tools(tools: Vec<Tool>) -> anyhow::Result<Self> {
        check_face(&tools)?;
        Ok(Self::with_factory(Arc::new(move |_, _| Ok(tools.clone()))))
    }

    /// 关工具的那一套（同一 harness 只关工具的那一次跑）。
    pub fn without_tools() -> Self {
        Self { factory: None }
    }
}

/// HLE 的工具面**恰好**是这两件，名字按钉死的值来。
///
/// 造出来的清单每次都要过这一关，不只是构造期核一次——工厂是能按题变的，判据因此
/// 要落在**真正交出去的**那一束上。
fn check_face(tools: &[Tool]) -> anyhow::Result<()> {
    let mut names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
    names.sort_unstable();
    let mut wanted = [HLE_TOOL_WEB_SEARCH, HLE_TOOL_PYTHON];
    wanted.sort_unstable();
    if names != wanted {
        anyhow::bail!(
            "HLE's tool face is exactly [{HLE_TOOL_WEB_SEARCH}, {HLE_TOOL_PYTHON}]; got {names:?}"
        );
    }
    Ok(())
}

impl Toolkit for HleToolkit {
    fn toolset(&self, case: &EvalCase, env: &Arc<dyn CaseEnv>) -> anyhow::Result<ToolSet> {
        match &self.factory {
            Some(factory) => {
                let tools = factory(case, env)?;
                check_face(&tools)?;
                ToolSet::new(tools)
            }
            None => Ok(ToolSet::empty()),
        }
    }
}

/// 裁判给的判词。字段名照官方结构化输出的形状。
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct HleJudgeVerdict {
    extracted_final_answer: String,
    reasoning: String,
    /// 官方口径是 `"yes"` / `"no"` 两个字面量。
    correct: String,
    #[serde(default)]
    confidence: i64,
}

/// HLE 判分器：官方 `model_graded_fact`。
///
/// 每题一次裁判调用；`multipleChoice` 可选一条字符串直判的省法。省法**默认关**：
/// 它只在回答是被规范成单个选项字母时才等价于裁判，否则会把「答对了但没按格式写」判成错，
/// 那是拿分数换 token——省法开着时每条的 `Verdict.readings.path` 会写明走的是哪条。
pub struct HleJudge {
    judge: Option<Arc<dyn LlmClient>>,
    shortcut_multiple_choice: bool,
}

impl HleJudge {
    /// 只给裁判上游；省法关。
    pub fn new(judge: Option<Arc<dyn LlmClient>>) -> Self {
        Self {
            judge,
            shortcut_multiple_choice: false,
        }
    }

    /// 开 `multipleChoice` 的字符串直判省法。列进表之前要想清楚：这一列的判据
    /// 与「一路走裁判」的列已经不同了。
    pub fn with_multiple_choice_shortcut(mut self, on: bool) -> Self {
        self.shortcut_multiple_choice = on;
        self
    }
}

/// 只有当回答被规范成**单个字母**时才给判词；否则返回 `None` 让调用方走裁判。
/// 宽松地「从一段话里挑一个字母」会在模型没按格式写时误判，所以这里只认干净的那一种。
fn multiple_choice_verdict(response: &str, correct_answer: &str) -> Option<Verdict> {
    let got = response.trim();
    let correct = correct_answer.trim();
    if got.chars().count() != 1 || correct.chars().count() != 1 {
        return None;
    }
    let got = got.chars().next()?.to_ascii_uppercase();
    let want = correct.chars().next()?.to_ascii_uppercase();
    if !got.is_ascii_uppercase() || !want.is_ascii_uppercase() {
        return None;
    }
    Some(if got == want {
        Verdict::pass(format!("multiple choice {got} == {want}"))
    } else {
        Verdict::fail(format!("multiple choice {got} != {want}"))
    })
}

#[async_trait]
impl CaseJudge for HleJudge {
    async fn judge(
        &self,
        case: &EvalCase,
        _env: &Arc<dyn CaseEnv>,
        output: &AgentOutput,
    ) -> anyhow::Result<Verdict> {
        let answer_type = case
            .metadata
            .get("answer_type")
            .map(String::as_str)
            .unwrap_or("");
        let correct_answer = case
            .expected_output
            .as_ref()
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if self.shortcut_multiple_choice && answer_type == "multipleChoice" {
            if let Some(mut verdict) = multiple_choice_verdict(&output.final_answer, correct_answer)
            {
                verdict
                    .readings
                    .insert("path".into(), "multiple_choice_shortcut".into());
                return Ok(verdict);
            }
        }

        let Some(judge) = &self.judge else {
            // 不是判错：上游没接。判错会让这一格读成「模型不行」，而真相是这一列没跑成。
            anyhow::bail!(
                "HLE judges with a model and none is wired (case {} answer_type={answer_type}); \
                 a case judged without one would be scored as wrong when it was never judged",
                case.id
            );
        };

        let question = case
            .input
            .get("question")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let prompt = HLE_JUDGE_TEMPLATE
            .replace("{question}", question)
            .replace("{correct_answer}", correct_answer)
            .replace("{response}", &output.final_answer);

        let options = ChatOptions::default().with_actor("eval");
        let judged: HleJudgeVerdict =
            execute_structured(&**judge, &[Message::user(prompt)], &options).await?;

        let resolved = judged.correct.trim().eq_ignore_ascii_case("yes");
        let mut verdict = if resolved {
            Verdict::pass(format!("judge: {}", judged.reasoning))
        } else {
            Verdict::fail(format!("judge: {}", judged.reasoning))
        };
        verdict
            .readings
            .insert("path".into(), "model_graded_fact".into());
        verdict
            .readings
            .insert("template_version".into(), HLE_JUDGE_TEMPLATE_VERSION.into());
        verdict.readings.insert(
            "extracted_final_answer".into(),
            judged.extracted_final_answer,
        );
        verdict
            .readings
            .insert("confidence".into(), judged.confidence.to_string());
        Ok(verdict)
    }
}

/// 把四件拼成一个 HLE 基准，交给运行器。
///
/// `judge` 是生成式裁判的上游：可达时给一个 `LlmClient`，缺席时给 `None`——那时
/// 字符串直判之外的路会报错，这一列读起来是「没跑成」而不是「分数低」。
/// `tools` 决定这一列是带工具还是关工具的那一次。
pub fn hle_benchmark(
    judge: Option<Arc<dyn LlmClient>>,
    tools: HleToolkit,
) -> crate::bench::Benchmark {
    crate::bench::Benchmark {
        name: "hle".into(),
        metric: "pass@1".into(),
        cases: Arc::new(HleCaseSource),
        env: Arc::new(crate::bench::NoEnvProvider),
        tools: Arc::new(tools),
        judge: Arc::new(HleJudge::new(judge)),
        budget: crate::scaffold::Budget::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{Budget, FinishReason, NoEnv};
    use cog_core::{ChatOptions, ChatResponse, ContentBlock, SFResult, ToolImplementation, Usage};

    fn tempdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "hle-test-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(d.join("hle")).unwrap();
        d
    }

    fn write_jsonl(root: &Path, body: &str) {
        std::fs::write(root.join(HLE_JSONL), body).unwrap();
    }

    #[test]
    fn rows_become_cases_with_the_image_and_the_metadata_carried() {
        let root = tempdir();
        write_jsonl(
            &root,
            concat!(
                r#"{"id":"a1","question":"Q1","image":"data:image/jpeg;base64,AAAA","answer":"4","answer_type":"exactMatch","rationale":"r","raw_subject":"Math","category":"Math"}"#,
                "\n",
                r#"{"id":"a2","question":"Q2","image":"","answer":"B","answer_type":"multipleChoice","category":"Other"}"#,
                "\n",
            ),
        );
        let cases = HleCaseSource.cases(&root).unwrap();
        assert_eq!(cases.len(), 2);
        assert_eq!(cases[0].id, "hle-a1");
        assert_eq!(cases[0].input["question"], "Q1");
        assert_eq!(cases[0].input["image"], "data:image/jpeg;base64,AAAA");
        assert_eq!(cases[0].metadata["has_image"], "true");
        assert_eq!(cases[0].metadata["answer_type"], "exactMatch");
        assert!(cases[0].tags.contains(&"category:Math".to_string()));
        // 空串的 image 是「没有图」，不是「有一条空 data URL」。
        assert_eq!(cases[1].input["image"], serde_json::Value::Null);
        assert_eq!(cases[1].metadata["has_image"], "false");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_missing_text_form_names_what_to_run() {
        let root = tempdir();
        let err = HleCaseSource.cases(&root).unwrap_err().to_string();
        assert!(err.contains("--convert"), "{err}");
        std::fs::remove_dir_all(&root).ok();
    }

    fn tool(name: &str) -> Tool {
        Tool {
            name: name.into(),
            description: name.into(),
            parameters: serde_json::json!({"type": "object"}),
            implementation: ToolImplementation::Native(Arc::new(|_| {
                Box::pin(async move { Ok(serde_json::json!(null)) })
            })),
        }
    }

    #[test]
    fn the_tool_face_is_exactly_two_pinned_tools() {
        assert!(
            HleToolkit::with_tools(vec![tool(HLE_TOOL_PYTHON), tool(HLE_TOOL_WEB_SEARCH)]).is_ok()
        );
        // 多一件、少一件、换个名字，都不是 HLE 的那一束。
        assert!(HleToolkit::with_tools(vec![tool(HLE_TOOL_WEB_SEARCH)]).is_err());
        assert!(HleToolkit::with_tools(vec![
            tool(HLE_TOOL_WEB_SEARCH),
            tool(HLE_TOOL_PYTHON),
            tool("bash")
        ])
        .is_err());
        assert!(HleToolkit::with_tools(vec![tool("search"), tool(HLE_TOOL_PYTHON)]).is_err());
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        let empty_case = case("hle-x", "exactMatch", "q", "a");
        assert!(HleToolkit::without_tools()
            .toolset(&empty_case, &env)
            .unwrap()
            .is_empty());
    }

    /// 工具是按题现造的，判据因此落在**真正交出去的**那一束上。
    ///
    /// 构造期核不到工厂按题才交出来的错——那一类必须在这一层拦住，否则一束少了一件
    /// 的工具会静默地把分数拉低。
    #[test]
    fn a_factory_built_tool_face_is_checked_at_call_time() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        let c = case("hle-y", "exactMatch", "q", "a");

        let good: Arc<ToolFactory> =
            Arc::new(|_, _| Ok(vec![tool(HLE_TOOL_WEB_SEARCH), tool(HLE_TOOL_PYTHON)]));
        assert_eq!(
            HleToolkit::with_factory(good)
                .toolset(&c, &env)
                .unwrap()
                .definitions()
                .len(),
            2
        );

        let short: Arc<ToolFactory> = Arc::new(|_, _| Ok(vec![tool(HLE_TOOL_WEB_SEARCH)]));
        let err = HleToolkit::with_factory(short)
            .toolset(&c, &env)
            .unwrap_err()
            .to_string();
        assert!(err.contains(HLE_TOOL_PYTHON), "{err}");
    }

    fn case(id: &str, answer_type: &str, question: &str, answer: &str) -> EvalCase {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("answer_type".into(), answer_type.into());
        EvalCase {
            id: id.into(),
            name: id.into(),
            input: serde_json::json!({"question": question, "image": null}),
            expected_output: Some(serde_json::Value::String(answer.into())),
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata,
        }
    }

    fn output(answer: &str) -> AgentOutput {
        AgentOutput {
            final_answer: answer.into(),
            trace: vec![],
            tokens: Usage::default(),
            finish: FinishReason::Answered,
        }
    }

    /// 一个按脚本回话的裁判上游：`chat` 把预置的 JSON 当正文回。
    struct ScriptedJudge(String);
    #[async_trait]
    impl LlmClient for ScriptedJudge {
        async fn chat_stream(
            &self,
            _m: &[Message],
            _o: &ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!("the HLE judge never streams")
        }
        async fn complete_stream(
            &self,
            _p: &str,
            _o: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!("the HLE judge never streams")
        }
        async fn chat(&self, _m: &[Message], _o: &ChatOptions) -> SFResult<ChatResponse> {
            Ok(ChatResponse {
                content: vec![ContentBlock::text(self.0.clone())],
                ..Default::default()
            })
        }
        async fn health_check(&self) -> bool {
            true
        }
    }

    fn envelope(json: &str) -> Arc<dyn LlmClient> {
        Arc::new(ScriptedJudge(json.into()))
    }

    #[tokio::test]
    async fn the_judge_reads_the_official_verdict_shape() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        let judge = HleJudge::new(Some(envelope(
            r#"{"extracted_final_answer":"42","reasoning":"same","correct":"yes","confidence":90}"#,
        )));
        let v = judge
            .judge(&case("hle-1", "exactMatch", "q", "42"), &env, &output("42"))
            .await
            .unwrap();
        assert!(v.resolved, "{}", v.detail);
        assert_eq!(v.readings["path"], "model_graded_fact");
        assert_eq!(v.readings["template_version"], HLE_JUDGE_TEMPLATE_VERSION);
        assert_eq!(v.readings["extracted_final_answer"], "42");
        assert_eq!(v.readings["confidence"], "90");

        let judge = HleJudge::new(Some(envelope(
            r#"{"extracted_final_answer":"41","reasoning":"off by one","correct":"no","confidence":80}"#,
        )));
        let v = judge
            .judge(&case("hle-2", "exactMatch", "q", "42"), &env, &output("41"))
            .await
            .unwrap();
        assert!(!v.resolved);
        assert!(v.detail.contains("off by one"), "{}", v.detail);
    }

    #[tokio::test]
    async fn a_case_that_needs_the_judge_errors_when_no_judge_is_wired() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        let judge = HleJudge::new(None);
        let err = judge
            .judge(&case("hle-3", "exactMatch", "q", "42"), &env, &output("42"))
            .await
            .unwrap_err()
            .to_string();
        // 「判不了」不许被读成「答错了」。
        assert!(err.contains("judged without one"), "{err}");
    }

    #[tokio::test]
    async fn the_multiple_choice_shortcut_is_opt_in_and_only_for_clean_letters() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        // 省法关：同样的输入要裁判，没裁判就报错。
        let strict = HleJudge::new(None);
        assert!(strict
            .judge(&case("m1", "multipleChoice", "q", "B"), &env, &output("B"))
            .await
            .is_err());

        // 省法开、且回答是干净的单字母：直接判，不叫裁判。
        let shortcut = HleJudge::new(None).with_multiple_choice_shortcut(true);
        let v = shortcut
            .judge(&case("m2", "multipleChoice", "q", "B"), &env, &output("b"))
            .await
            .unwrap();
        assert!(v.resolved);
        assert_eq!(v.readings["path"], "multiple_choice_shortcut");

        // 回答不是单字母（模型没按格式写）⇒ 省法不认，仍要裁判。
        assert!(shortcut
            .judge(
                &case("m3", "multipleChoice", "q", "B"),
                &env,
                &output("The answer is B")
            )
            .await
            .is_err());

        // 省法只对 multipleChoice 开，exactMatch 一律走裁判。
        assert!(shortcut
            .judge(&case("m4", "exactMatch", "q", "B"), &env, &output("B"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn the_four_adapters_compose_into_a_benchmark() {
        let root = tempdir();
        write_jsonl(
            &root,
            r#"{"id":"z","question":"Q","image":"","answer":"B","answer_type":"multipleChoice","category":"Other"}"#,
        );
        let bench = hle_benchmark(None, HleToolkit::without_tools());
        assert_eq!(bench.name, "hle");
        assert_eq!(bench.metric, "pass@1");
        let cases = bench.cases.cases(&root).unwrap();
        assert_eq!(cases.len(), 1);
        let env = bench.env.acquire(&cases[0]).await.unwrap();
        assert_eq!(env.id(), "none");
        let tools = bench.tools.toolset(&cases[0], &env).unwrap();
        assert!(tools.is_empty());
        assert_eq!(bench.budget, Budget::default());
        // 无裁判上游、且省法关 ⇒ 判分这一步报错，不是判错。
        assert!(bench
            .judge
            .judge(&cases[0], &env, &output("B"))
            .await
            .is_err());
        std::fs::remove_dir_all(&root).ok();
    }
}
