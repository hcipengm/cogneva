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
    args_on(root, pins, scaffolds, None)
}

/// 同一条命令行，外加可选的 `--subset`。
fn args_on(root: &Path, pins: &Path, scaffolds: &str, subset: Option<&Path>) -> EvalTableArgs {
    let mut raw: Vec<String> = [
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
    if let Some(subset) = subset {
        raw.extend(["--subset".to_string(), subset.display().to_string()]);
    }
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
    // 这条测试走的是诊断臂（工具面与主表不同的那一臂）：它必须被记成「不是主表那一行」，
    // 而且除此之外没有别的缺席——逐格读数仍是真跑出来的。
    assert_eq!(first.missing.len(), 1, "{:?}", first.missing);
    assert!(
        first.missing[0].contains("diagnostic arm"),
        "{:?}",
        first.missing
    );
    assert!(first.text.contains("diagnostic\t"), "{}", first.text);
    assert_eq!(first.errored, 0, "{}", first.text);

    // 表体被拒收为表行，但逐格读数在：两题都判过、都过。
    assert!(
        first.text.contains("## table\n(withheld:"),
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
    // 诊断臂不发表体，但逐格读数在：两题都经平台端口跑通、都判过。
    assert!(
        out.text
            .contains("cell\thle\tnql\tseed=0\tresolved=2\ttotal=2\terrored=0\t"),
        "{}",
        out.text
    );
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

/// 跑一份存档点名的子集：分母是存档里的题数，表头要说清跑的是哪一批题。
#[tokio::test]
async fn a_subset_run_scores_the_archived_cases_and_names_the_archive() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let archive = dir.path().join("seed7_n1.tsv");
    std::fs::write(&archive, "# drawn for this test\n# seed\t7\nhle\thle-q1\n").unwrap();

    let args = args_on(dir.path(), &pins_path, "gepa,agentflow", Some(&archive));
    let out = run_table(&args, &wired(false), &pins).await.unwrap();

    // 两题的夹具里只跑一道：分母跟着存档走，不是基准的题数。
    assert!(
        out.text
            .contains("cell\thle\tgepa\tseed=0\tresolved=1\ttotal=1\terrored=0\t"),
        "{}",
        out.text
    );
    // 跑的是哪一批题随表出去，而且钉到文件：路径会变，内容哈希不会。
    assert!(
        out.text
            .contains(&format!("subset\t{}\tsha256:", archive.display())),
        "{}",
        out.text
    );
    assert!(out.text.contains("subset-cases\thle\t1"), "{}", out.text);
    assert_eq!(out.missing.len(), 1, "{:?}", out.missing);
}

/// 存档点名的题这一版数据里没有：报出来的是**因**（哪道题没了），不是一行 0 分。
#[tokio::test]
async fn an_archive_naming_a_case_this_data_lacks_says_which_case() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let archive = dir.path().join("stale.tsv");
    std::fs::write(&archive, "hle\thle-q1\nhle\thle-q9-gone\n").unwrap();

    let args = args_on(dir.path(), &pins_path, "gepa", Some(&archive));
    let out = run_table(&args, &wired(false), &pins).await.unwrap();

    let named = out
        .missing
        .iter()
        .find(|m| m.starts_with("hle:"))
        .unwrap_or_else(|| panic!("{:?}", out.missing));
    assert!(named.contains("hle-q9-gone"), "{named}");
    assert!(!named.contains("hle-q1"), "存在的题不许一起报：{named}");
    assert_eq!(out.errored, 0, "这一列是没读数，不是跑不动");
    assert!(
        out.text
            .contains("cell\thle\tgepa\tseed=0\tresolved=0\ttotal=0\t"),
        "{}",
        out.text
    );
}

/// 存档没覆盖这个基准：开跑之前就报错，不许拿「全量」顶上。
#[tokio::test]
async fn an_archive_that_does_not_cover_a_benchmark_is_refused_before_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let archive = dir.path().join("hle-only.tsv");
    std::fs::write(&archive, "hle\thle-q1\n").unwrap();

    // 夹具的数据根里只有 hle 的题，所以这里只验「覆盖不到」这一条：把 toolathlon 也选上。
    let raw: Vec<String> = [
        "--pins",
        &pins_path.display().to_string(),
        "--tools",
        "none",
        "--backbone",
        "double@1",
        "--judge",
        "double@1",
        "--data-root",
        &dir.path().display().to_string(),
        "--benchmarks",
        "hle,toolathlon",
        "--scaffolds",
        "gepa",
        "--seeds",
        "0",
        "--subset",
        &archive.display().to_string(),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let args = EvalTableArgs::parse(&raw).unwrap();

    let err = match run_table(&args, &wired(false), &pins).await {
        Ok(_) => panic!("覆盖不到就不该开跑"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("toolathlon"), "{err}");
    assert!(err.contains("hle"), "要点出存档里有什么：{err}");
}
