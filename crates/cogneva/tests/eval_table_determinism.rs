//! 主表驱动器的验收：同一条命令、同一个数据根跑两次，输出逐字节相同。
//!
//! 这里的骨干与裁判是**替身**，所以这条测试量的是**管道**在拿到同样答案之后的确定性，
//! 不是上游端到端可复现——温度不为零的采样本来就不会两次给出同样的 token。替身每次
//! 交回同样的话，两次跑因此拿到同一组答案；此时表若不逐字节相同，就是管道自己在抖动
//! （墙上时钟、容器遍历序、并发完成序），与上游无关。
//!
//! 第二条测试量的是另一件事：平台缺席必须表现成「跑不起来」，不能表现成一个低分。

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, ContentBlock,
    LlmClient, Message, SFError, SFResult, StopReason, Usage,
};
use cog_eval::scaffolds::nql::PlatformReply;
use cog_eval::scaffolds::{PlatformRequest, PlatformRunner};
use cogneva::eval_table::{run_table, EvalTableArgs, Pins, Wiring};

/// 每次都交回同一句话的骨干替身。
///
/// 它不按请求内容作答：主表这条管道要能被单独测，就不能让被测的那部分取决于上游的
/// 采样。答复固定，两跑之间唯一的变量才是管道本身。
struct FixedAnswer(String);

#[async_trait]
impl LlmClient for FixedAnswer {
    async fn chat_stream(
        &self,
        _m: &[Message],
        _o: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(SFError::Config(
            "the eval-table doubles do not stream".into(),
        ))
    }

    async fn complete_stream(
        &self,
        _p: &str,
        _o: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(SFError::Config(
            "the eval-table doubles do not stream".into(),
        ))
    }

    async fn chat(&self, _m: &[Message], _o: &ChatOptions) -> SFResult<ChatResponse> {
        Ok(ChatResponse {
            content: vec![ContentBlock::text(self.0.clone())],
            api: "double".into(),
            provider: "double".into(),
            model: "double".into(),
            response_id: None,
            usage: Usage {
                total_tokens: 7,
                ..Default::default()
            },
            stop_reason: StopReason::Stop,
            error_message: None,
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// 固定判「对」的裁判替身。判词的形状照 HLE 官方结构化输出的字段来。
struct AlwaysAgrees;

#[async_trait]
impl LlmClient for AlwaysAgrees {
    async fn chat_stream(
        &self,
        _m: &[Message],
        _o: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(SFError::Config(
            "the eval-table doubles do not stream".into(),
        ))
    }

    async fn complete_stream(
        &self,
        _p: &str,
        _o: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(SFError::Config(
            "the eval-table doubles do not stream".into(),
        ))
    }

    async fn chat(&self, _m: &[Message], _o: &ChatOptions) -> SFResult<ChatResponse> {
        Ok(ChatResponse {
            content: vec![ContentBlock::text(
                r#"{"extracted_final_answer":"42","reasoning":"the double always agrees","correct":"yes","confidence":100}"#,
            )],
            api: "double".into(),
            provider: "double".into(),
            model: "double".into(),
            response_id: None,
            usage: Usage {
                total_tokens: 5,
                ..Default::default()
            },
            stop_reason: StopReason::Stop,
            error_message: None,
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// 平台替身：把收到的题面原样作为答案交回来，并带上一个任务 id。
///
/// 这个替身存在是为了让组合根那条真端口（`PlatformRunner`）在驱动里被走到一次——
/// 它自己不证明平台接上了，证明的是驱动把平台的答复原样放进表里。
struct EchoPlatform;

#[async_trait]
impl PlatformRunner for EchoPlatform {
    async fn run(&self, request: PlatformRequest) -> anyhow::Result<PlatformReply> {
        Ok(PlatformReply {
            task_id: format!("double-{}", request.seed),
            output: cog_eval::scaffold::AgentOutput {
                final_answer: "42".into(),
                trace: vec![],
                tokens: Usage::default(),
                finish: cog_eval::scaffold::FinishReason::Answered,
            },
        })
    }
}

/// 两个题的 HLE 文本形式，外加一份把这两个字节钉住的 pins 文件。
fn fixture(root: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let jsonl = root.join("hle/test-00000-of-00001.jsonl");
    std::fs::create_dir_all(jsonl.parent().unwrap()).unwrap();
    let body = concat!(
        r#"{"id":"q1","question":"What is 6*7?","answer":"42","answer_type":"exactMatch"}"#,
        "\n",
        r#"{"id":"q2","question":"Give the answer.","answer":"42","answer_type":"exactMatch"}"#,
        "\n"
    );
    std::fs::write(&jsonl, body).unwrap();

    use sha2::{Digest, Sha256};
    let sha = hex::encode(Sha256::digest(body.as_bytes()));
    let pins = root.join("pins.tsv");
    // 来源那行钉的是镜像上的原件：这里盘上没有它，报表里要说出「只有读的那份被核过」。
    std::fs::write(
        &pins,
        format!(
            "source\thle\tf00dcafe\thle/test-00000-of-00001.parquet\t{}\t4096\tfetched from the mirror\n\
             read-file\thle\tf00dcafe\thle/test-00000-of-00001.jsonl\t{sha}\t{}\tderived by --convert\n",
            "f".repeat(64),
            body.len()
        ),
    )
    .unwrap();
    (jsonl, pins)
}

fn args(root: &Path, pins: &Path, scaffolds: &str) -> EvalTableArgs {
    let raw: Vec<String> = [
        "--pins".to_string(),
        pins.display().to_string(),
        "--tools".to_string(),
        "none".to_string(),
        "--backbone".to_string(),
        "double@1".to_string(),
        "--judge".to_string(),
        "double@1".to_string(),
        "--data-root".to_string(),
        root.display().to_string(),
        "--benchmarks".to_string(),
        "hle".to_string(),
        "--scaffolds".to_string(),
        scaffolds.to_string(),
        "--seeds".to_string(),
        "0,1,2".to_string(),
    ]
    .to_vec();
    EvalTableArgs::parse(&raw).expect("the fixture's arguments are well formed")
}

fn wired(platform: bool) -> Wiring {
    Wiring {
        backbone: Some(Arc::new(FixedAnswer("42".into()))),
        judge: Some(Arc::new(AlwaysAgrees)),
        platform: if platform {
            Some(Arc::new(EchoPlatform))
        } else {
            None
        },
        // 平台的身份跟平台一起递：报表要说 nql 那一行长在哪个端点上，而不只是「接上了」。
        platform_base: if platform {
            Some("http://platform.test".into())
        } else {
            None
        },
        ..Default::default()
    }
}

/// 跑两次，两次的文本必须逐字节相同，而且这次跑是有数的。
///
/// GEPA 与 AgentFlow 不需要任何工具就能作答，所以这条测试跑出的是真的分数，不是
/// 「两跑都失败得一样」——后者在任何管道上都会相等，量不出确定性。
#[tokio::test]
async fn two_runs_over_one_data_root_produce_the_same_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let args = args(dir.path(), &pins_path, "gepa,agentflow");

    let first = run_table(&args, &wired(false), &pins).await.unwrap();
    let second = run_table(&args, &wired(false), &pins).await.unwrap();

    assert_eq!(
        first.text, second.text,
        "同一个数据根两次跑必须逐字节相同，差异在各段之间比对"
    );
    assert!(first.missing.is_empty(), "{:?}", first.missing);
    assert_eq!(first.errored, 0, "{}", first.text);

    // 表里有数：两题都判过、都过。
    assert!(
        first.text.contains("| **gepa** | 100.0±0.0 |"),
        "{}",
        first.text
    );
    assert!(
        first
            .text
            .contains("cell\thle\tgepa\tseed=0\tresolved=2\ttotal=2\terrored=0\t"),
        "{}",
        first.text
    );
    // pins 段写的是评测台真正打开的那份文件，以及它被怎么核的。
    assert!(
        first
            .text
            .contains("read-file=hle/test-00000-of-00001.jsonl"),
        "{}",
        first.text
    );
    assert!(first.text.contains("rev=f00dcafe"), "{}", first.text);
    assert!(first.text.contains("sha256("), "{}", first.text);
    // 镜像上的原件不在盘上：报表要说出来，不能沉默地少核一份。
    assert!(first.text.contains("not on disk"), "{}", first.text);
}

/// 平台接上时，NQL 那一行走的是组合根的端口，不是任何替身里的假答复。
#[tokio::test]
async fn the_platform_port_is_what_the_nql_row_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let args = args(dir.path(), &pins_path, "nql");

    let out = run_table(&args, &wired(true), &pins).await.unwrap();
    assert_eq!(out.errored, 0, "{}", out.text);
    assert!(out.text.contains("| **nql** | 100.0±0.0 |"), "{}", out.text);
    // 平台的读数出于哪个端点，随表出去：接上了却不说接在哪，这张表就没法被别人复跑。
    assert!(
        out.text.contains("platform\thttp://platform.test"),
        "{}",
        out.text
    );
}

/// 平台缺席：NQL 那一行是「跑不起来」，不是「0 分」。
#[tokio::test]
async fn a_missing_platform_is_an_error_not_a_zero_score() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let args = args(dir.path(), &pins_path, "nql");

    let out = run_table(&args, &wired(false), &pins).await.unwrap();
    let again = run_table(&args, &wired(false), &pins).await.unwrap();

    // 两题 × 三个种子都记成跑不动。
    assert_eq!(out.errored, 6, "{}", out.text);
    assert!(
        out.missing.iter().any(|m| m.contains("platform")),
        "{:?}",
        out.missing
    );
    assert!(
        out.text.contains("no platform endpoint is wired"),
        "判词要留下缺席的原因：{}",
        out.text
    );
    // 有判词的那张表也要逐字节可 diff：失败明细按题 id 排，不按并发完成的先后。
    assert_eq!(out.text, again.text, "失败明细的顺序不能由调度决定");
}
