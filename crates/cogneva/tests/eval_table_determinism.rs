//! 主表驱动器的验收：同一条命令、同一个数据根跑两次，输出逐字节相同。
//!
//! 这里的骨干与裁判是**替身**，所以这条测试量的是**管道**在拿到同样答案之后的确定性，
//! 不是上游端到端可复现——温度不为零的采样本来就不会两次给出同样的 token。替身每次
//! 交回同样的话，两次跑因此拿到同一组答案；此时表若不逐字节相同，就是管道自己在抖动
//! （墙上时钟、容器遍历序、并发完成序），与上游无关。
//!
//! 第二条测试量的是另一件事：平台缺席必须表现成「跑不起来」，不能表现成一个低分。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, ContentBlock,
    LlmClient, Message, SFError, SFResult, StopReason, Usage,
};
use cog_eval::scaffolds::nql::PlatformReply;
use cog_eval::scaffolds::{PlatformRequest, PlatformRunner};
use cogneva::eval_platform::PLATFORM_BASE_ENV;
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

/// 接上了、但每次跑都失败的平台替身。
///
/// 它造的是「没有声明过的缺陷」：`Wiring.platform` 有值，所以 `## wiring` 里没有平台
/// 那一条声明——那么这一格的跑不动就只能是它自己的问题，必须照旧进 `errored`。
struct BrokenPlatform;

#[async_trait]
impl PlatformRunner for BrokenPlatform {
    async fn run(&self, _request: PlatformRequest) -> anyhow::Result<PlatformReply> {
        anyhow::bail!("the platform is wired but every run fails")
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
        // 静态名额没有默认值，两个界至少要给一个；这条链不带闸门，所以在这里给。
        "--concurrency-slot".to_string(),
        "2".to_string(),
    ]
    .to_vec();
    if let Some(subset) = subset {
        raw.extend(["--subset".to_string(), subset.display().to_string()]);
    }
    // 报表写到这一格自己的临时目录里，检查点跟着它走。不给 `--out` 的话检查点落在进程的
    // 当前目录：同一份参数的两条测试会指向**同一份记录**（身份里没有数据根的路径），并行
    // 跑时一个把另一个锁在门外，而跑完还会在仓库里留下文件。
    raw.extend([
        "--out".to_string(),
        root.join("table.md").display().to_string(),
    ]);
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
    // 第二次跑之前把检查点移开：这两次要读的是**两次独立的跑**。留着它的话，第二次会把
    // 第一次跑完的例接回来（那正是检查点该干的事），两次的报表于是差在 `resumed` 上——
    // 这条测试量的就不再是「同一个输入两次跑出同样的字节」了。
    let record = PathBuf::from(
        first
            .text
            .lines()
            .find_map(|line| line.strip_prefix("checkpoint\t"))
            .and_then(|line| line.split('\t').next())
            .expect("报表要说出检查点落在哪，这里照它的话去移"),
    );
    std::fs::remove_file(&record).expect("第一次跑必须留下过一份记录");
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

    // 两题 × 三个种子。这些题**不是跑错了**：平台这一行是被提前声明过的缺席挡住的，
    // 这一格本来就没轮到跑。两个数分开印，是为了让「这一行少配了一段」与「这一格有
    // 缺陷」在被读的那一刻就不是一回事——合在一起时读的人只能自己猜该去修哪一半。
    assert_eq!(out.errored, 0, "{}", out.text);
    assert_eq!(out.blocked, 6, "{}", out.text);
    // 逐格读数上要能看见是被谁挡的，名字能回到 `## wiring` 那一行。
    assert!(
        out.text.contains(
            "cell\thle\tnql\tseed=0\tresolved=0\ttotal=2\terrored=0\tblocked=2\tblocked-by=nql\t"
        ),
        "{}",
        out.text
    );
    assert!(
        out.text.contains(&format!(
            "missing\tnql\trow:nql\tnot-wired\t{PLATFORM_BASE_ENV} is unset in this run's environment"
        )),
        "管到哪与哪一类都要写出来：{}",
        out.text
    );
    assert!(
        out.missing.iter().any(|m| m.contains(PLATFORM_BASE_ENV)),
        "人读那一份要点名缺的是哪个值：{:?}",
        out.missing
    );
    // 判词要留下缺席的原因，而不是「没接上」这句不含指引的兜底：这一行缺的是**这一次跑的
    // 环境里一个值**，不点名那个变量，读的人会去翻组合根——那里三处都在，翻不出东西来。
    assert!(
        out.text.contains(PLATFORM_BASE_ENV),
        "判词要留下缺席的原因：{}",
        out.text
    );
    // 有判词的那张表也要逐字节可 diff：失败明细按题 id 排，不按并发完成的先后。
    assert_eq!(out.text, again.text, "失败明细的顺序不能由调度决定");
}

/// 提前声明过的缺席只盖它点名的那些格子：同一次跑里别的行照出真读数。
///
/// 少了这一条，「凡有声明就把整表算成没轮到跑」的实现也会过掉——而那会把真缺陷一起洗掉。
#[tokio::test]
async fn a_declared_absence_blocks_only_the_row_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let args = args(dir.path(), &pins_path, "gepa,nql");

    let out = run_table(&args, &wired(false), &pins).await.unwrap();

    // nql 那一行：平台没接上，两题 × 三种子都算没轮到跑。
    assert_eq!(out.blocked, 6, "{}", out.text);
    assert_eq!(out.errored, 0, "{}", out.text);
    assert!(
        out.text.contains(
            "cell\thle\tnql\tseed=0\tresolved=0\ttotal=2\terrored=0\tblocked=2\tblocked-by=nql\t"
        ),
        "{}",
        out.text
    );
    // gepa 那一行不靠平台，一格一题都不许被这条声明碰到。
    assert!(
        out.text.contains(
            "cell\thle\tgepa\tseed=0\tresolved=2\ttotal=2\terrored=0\tblocked=0\tblocked-by=-\t"
        ),
        "{}",
        out.text
    );
    assert!(
        !out.text
            .contains("cell\thle\tgepa\tseed=0\tresolved=2\ttotal=2\terrored=0\tblocked=2"),
        "声明盖到了没被点名的行：{}",
        out.text
    );
}

/// 平台接上了、但每次都失败：这是**没有声明过**的缺陷，必须照旧印 `errored`。
///
/// 同时钉住另一半：整次跑那一类的声明（这里只有诊断臂那一条）一个格子都不许盖——
/// 它说的是「这次跑不构成一行表」，而那些格子是真跑过的读数。
#[tokio::test]
async fn an_undeclared_failure_is_errored_and_the_run_wide_absence_covers_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let args = args(dir.path(), &pins_path, "nql");

    let mut wiring = wired(false);
    wiring.platform = Some(Arc::new(BrokenPlatform));
    wiring.platform_base = Some("http://platform.test".into());

    let out = run_table(&args, &wiring, &pins).await.unwrap();

    // 只有诊断臂那一条声明，平台这一侧没有任何声明。
    assert_eq!(out.missing.len(), 1, "{:?}", out.missing);
    assert!(!out.text.contains("missing\tnql\t"), "{}", out.text);
    assert_eq!(out.blocked, 0, "{}", out.text);
    assert_eq!(out.errored, 6, "{}", out.text);
    assert!(
        out.text.contains(
            "cell\thle\tnql\tseed=0\tresolved=0\ttotal=2\terrored=2\tblocked=0\tblocked-by=-\t"
        ),
        "{}",
        out.text
    );
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
        // 两个界至少要给一个，这条链不带闸门。
        "--concurrency-slot",
        "2",
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

/// 闸门从命令行一路接到运行器上——报表里那一段读数就是这一路接通的证据。
///
/// 少了这条，`--admission` 可以解析得很好、渲染得很好，而中间那一跳（命令行 → 运行器的
/// 配置 → 每一格要票的那一处）断着：报表照样打出一段 `## gate`，因为那一段是拿配置里
/// 那个对象渲染的，而每格拿到的仍然是固定名额的信号量。
#[tokio::test]
async fn the_gate_reaches_the_rig_and_the_report_says_which_directory_it_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let (_, pins_path) = fixture(dir.path());
    let pins = Pins::load(&pins_path).unwrap();
    let gate_dir = dir.path().join("gate");
    std::fs::create_dir(&gate_dir).unwrap();

    let mut args = args(dir.path(), &pins_path, "gepa,agentflow");
    args.admission_dir = Some(gate_dir.clone());
    // 两界都钉在 1：每一格里两题，所以闸门**必须**挡下一题。界若只是一个被渲染出来的
    // 配置，这一轮会两题并排跑完，`blocked` 就还是 0。
    args.admission_floor_unmeasured = Some(1);
    args.admission_ceiling_unmeasured = Some(1);

    let out = run_table(&args, &wired(false), &pins).await.unwrap();
    assert_eq!(out.errored, 0, "{}", out.text);
    assert!(out.text.contains("## gate\n"), "{}", out.text);
    assert!(!out.text.contains("gate\tnot in force"), "{}", out.text);
    assert!(
        out.text
            .contains(&format!("gate-dir\t{}\t", gate_dir.display())),
        "读数要说出界的是哪个目录：{}",
        out.text
    );
    assert!(out.text.contains("floor=1\tceiling=1"), "{}", out.text);
    assert!(out.text.contains("controller=yes"), "{}", out.text);
    // 界是 1 而每格两题：挡下来的例数必须是真的被挡过的那些。
    let gate = out
        .text
        .lines()
        .find_map(|line| line.strip_prefix("gate\t"))
        .expect("gate 那一段要有一行读数");
    let blocked: usize = gate
        .split('\t')
        .find_map(|f| f.strip_prefix("blocked="))
        .and_then(|n| n.parse().ok())
        .expect("这一行要说挡下了几例");
    assert!(blocked >= 1, "界是 1 而每格两题，至少挡下一题：{gate}");
    // 界是拍的，这一点必须与量出来的压力分开写，且写在表里。
    assert!(
        out.text.contains("unmeasured: both come from"),
        "{}",
        out.text
    );
}
