//! principle: feedback_the_unit_of_work_is_the_batch_not_the_turn
//!
//! **2.1–2.4 的入口**：把 2.5 的假清单变成一次 generator 调用。
//!
//! 这一版只做固定的一半——读判词、组请求、打出来；模型那一半要接 `cog-agent` 的
//! 调用通道（S79），在那之前它明确报「没有模型可取用」，不假装生成过 diff。
//!
//! 为什么先有这一半：驱动「整批做完」的循环是 `判 → 假清单 → 生成 → 再判`，而
//! 起点和终点都由判词给出。没有这一步，2.5 只是一个读数，不是一个能把活推下去的闸。

use std::process::Command;
use std::sync::Arc;

/// 一次 2.5 判词的两半：通过的 id，未通过的「id + 理由」。
type EditVerdict = (Vec<String>, Vec<(String, String)>);

/// 读一次 2.5 的判词，返回（通过的 id，未通过的「id + 理由」）。
///
/// 判词向产品自己问（`cogneva ledger editing`），不再 `python3` 调一份带外脚本：
/// 同一问在两处实现，改一处另一处不会红，而两边的答案在读数上同形。这里只做转交与
/// 解析——「哪几条改完了」的判据在产品里。
fn edit_verdict(root: &std::path::Path) -> Result<EditVerdict, String> {
    let exe = std::env::current_exe()
        .map_err(|e| format!("找不到自己的可执行文件，问不了这一批：{e}"))?;
    let out = Command::new(&exe)
        .current_dir(root)
        .arg("ledger")
        .arg("editing")
        .output()
        .map_err(|e| format!("run {}: {e}", exe.display()))?;
    // 契约：0 全真、1 有假、2 判不了。别的退出码都不是判词（崩了、被信号杀死）。
    let code = out.status.code();
    if !matches!(code, Some(0) | Some(1) | Some(2)) {
        return Err(format!(
            "{} 没有给出判词（退出码 {code:?}）：{}",
            exe.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    parse_editing(
        code,
        &String::from_utf8_lossy(&out.stdout),
        &String::from_utf8_lossy(&out.stderr),
        &exe.display().to_string(),
    )
}

/// 把 `ledger editing` 那一问的判词读成两半。拆出来是因为它是这一段里唯一有形状的
/// 地方：退出码、两种行、以及「一却没有 TODO 行 ⇒ 崩了」那条。
fn parse_editing(
    code: Option<i32>,
    stdout: &str,
    stderr: &str,
    who: &str,
) -> Result<EditVerdict, String> {
    if code == Some(2) {
        return Err(stderr.trim().to_string());
    }
    let text = stdout;
    let mut ok = Vec::new();
    let mut missing = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(id) = line.strip_prefix("可以 ") {
            ok.push(id.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("TODO ") {
            let (id, why) = rest.split_once('（').unwrap_or((rest, ""));
            missing.push((
                id.trim().to_string(),
                why.trim_end_matches('）').trim().to_string(),
            ));
        }
    }
    // 退出码 1 必须带着判词回来：一行 `TODO` 都没有的一，是崩了，不是判词。
    if code == Some(1) && missing.is_empty() {
        return Err(format!(
            "{who} 退出码是 1，却没有一行 `TODO`——那不是判词：{}",
            stderr.trim()
        ));
    }
    Ok((ok, missing))
}

/// 组出这一轮的生成请求：整批一起进（2.1），工具结果与假理由一并带上（2.2–2.4 的输入）。
fn compose(root: &std::path::Path, missing: &[(String, String)]) -> String {
    let mut out = String::new();
    out.push_str("# 2.1–2.4 的输入\n\n");
    out.push_str("这一批里还没编辑完的条目，逐条给出它缺什么：\n\n");
    for (id, why) in missing {
        out.push_str(&format!("## {id}\n{why}\n\n"));
    }
    out.push_str(&format!(
        "# 树：{}\n\
         # 要求：\n\
         1. 先一次给出你需要的**全部**工具调用（读文件、搜索、跑只读命令），不要一条一条试；\n\
         2. 工具结果不完备之前不许出改动；工具结果装不下时会以 `artifact://` 指针交回，按指针取全文，不要凭猜测补。\n\
         3. 整批一次出 diff：这一批的每一条都在同一份改动里，不要分批交。\n",
        root.display()
    ));
    out
}

/// **2.1–2.4 的 generator 侧**：把请求交给 agent，读回它在那次运行里留下的改动。
///
/// 走 `cog_core::Agent` 而不是裸 LLM：预算、重试、事件、工具都在 agent 那一层，
/// 裸调等于把记账抄一份（S78/S79）。diff 也从**树**里读（`Agent` 有读 checkout 的
/// 入口），不采信模型自己敲出来的补丁文本——那份是「关于文件的散文」。
/// 这次生成要的那一个动作。`Agent` 的整套生命周期（start/snapshot/steer…）在这一步
/// 用不到，而把一个只有 `prompt` 的调用点钉在整套接口上，等于让每个用例去实现十四个
/// 不相关的方法——那时测的就不是这一步了。
/// 这一批的身份：整批一次生成写下的每一笔调用、每一次工具调用都挂在它上面。
///
/// 名字由**这一批的 id 排序拼出**，不由谁发起、不由时刻决定：同一批重放，名字相同，
/// 于是「这一批花了几次调用」是一次按名字的查询；换一批就是另一个名字，不会把两批
/// 的账混成一本。理由不进名字——同一批 id 理由变了是同一个工作集，那是进展不是新批。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchId(String);

impl BatchId {
    pub fn from_ids(ids: impl IntoIterator<Item = String>) -> Self {
        let mut ids: Vec<String> = ids.into_iter().collect();
        ids.sort();
        ids.dedup();
        Self(format!("batch-{}", ids.join("-")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[async_trait::async_trait]
pub trait GeneratorCall: Send + Sync {
    async fn call(&self, batch: &BatchId, request: &str) -> cog_core::SFResult<serde_json::Value>;
}

/// 生产实现：交给 agent。
pub struct AgentCall {
    pub agent: Arc<dyn cog_core::Agent>,
}

#[async_trait::async_trait]
impl GeneratorCall for AgentCall {
    async fn call(&self, batch: &BatchId, request: &str) -> cog_core::SFResult<serde_json::Value> {
        self.agent
            // 身份不是可选的一层：`prompt` 不带任务，运行时写的每一行调用只有模型，
            // 按批号查「这一批花了几次」永远查不到。`prompt_for_task` 把批号挂进这次
            // 运行的作用域，运行时的 stamp 再把它写成每一笔调用上的任务头。
            // 角色名从声明读，不在这里再写一遍：改名时改一处，这一侧跟着走。
            .prompt_for_task(
                batch.as_str(),
                serde_json::json!({ "batch": request, "role": cog_core::roles::GENERATOR }),
            )
            .await
    }
}

pub struct BatchGenerator {
    call: Arc<dyn GeneratorCall>,
    /// 原则语料：只在启动时装进来一份索引；批次请求里按**要改的路径**取命中的那几条。
    corpus: Option<Arc<cog_core::PrincipleBundle>>,
}

/// 把命中的原则与它们的门附在请求后面。一条都不命中就原样返回。
fn with_principles(request: &str, corpus: Option<&cog_core::PrincipleBundle>) -> String {
    let Some(corpus) = corpus else {
        return request.to_string();
    };
    let paths = cog_core::paths_in(request);
    let picked = cog_core::select_for_paths(corpus, &paths);
    if picked.is_empty() {
        return request.to_string();
    }
    let mut out = String::from(request);
    out.push_str("\n\n## 适用原则（只列这一批路径命中的；每条附检查它的门）\n");
    for item in picked {
        out.push_str(&format!("- {}\n", item.lead));
        out.push_str(&format!("  {}\n", item.body.trim()));
        if let Some(gate) = item.host_gate.as_deref().or(item.where_red.as_deref()) {
            out.push_str(&format!("  门：{gate}\n"));
        }
    }
    out
}

impl BatchGenerator {
    pub fn new(call: Arc<dyn GeneratorCall>) -> Self {
        Self { call, corpus: None }
    }

    /// 接上原则语料：装载在入口做，这里只读。
    pub fn with_corpus(mut self, corpus: Arc<cog_core::PrincipleBundle>) -> Self {
        self.corpus = Some(corpus);
        self
    }

    /// 生产构造：把一个 agent 包成这次生成要的那一个调用。
    pub fn from_agent(agent: Arc<dyn cog_core::Agent>) -> Self {
        Self {
            call: Arc::new(AgentCall { agent }),
            corpus: None,
        }
    }

    /// 整批一次进、拿回这次运行的结果。工具轮次由 agent 自己决定，判据是信息完备
    /// （它够了才出 diff），这里不设上限。
    pub async fn generate(
        &self,
        batch: &BatchId,
        request: &str,
    ) -> cog_core::SFResult<serde_json::Value> {
        let request = with_principles(request, self.corpus.as_deref());
        self.call.call(batch, &request).await
    }
}

/// `cogneva batch-edit`。`--dry-run` 只打请求；没有 agent 时明确报错，不假装生成。
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?;
    let dry = std::env::args().any(|a| a == "--dry-run");
    let (ok, missing) = edit_verdict(&root).map_err(|e| format!("2.5 判不了：{e}"))?;
    if missing.is_empty() {
        println!(
            "batch-edit: 这一批 2.5 全真（{} 条），没有要生成的。",
            ok.len()
        );
        return Ok(());
    }
    let request = compose(&root, &missing);
    if dry {
        print!("{request}");
        println!("batch-edit: 只组了请求（--dry-run），没有调用模型。");
        return Ok(());
    }
    // 循环：判 → 假清单 → 生成 → 再判。没有「跑几轮」这个旋钮：一直跑到判词为空，
    // 或者某一轮什么都没变（那时再跑也只是把同一份输入再问一遍）。
    let _ = request;
    Err(format!(
        "batch-edit: {} 条还没生成；生成器要由调用方注入（`BatchGenerator::from_agent`），\
         命令行里没有 agent。",
        missing.len()
    )
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一个只回显的调用：判的是「整批原样进、答案原样出」，与模型无关。它同时记下
    /// 这一问带的批号——批号没传到调用这一层的话，写下的调用事件就归不了批。
    struct Echo(std::sync::Mutex<Option<(String, String)>>);

    #[async_trait::async_trait]
    impl GeneratorCall for Echo {
        async fn call(
            &self,
            batch: &BatchId,
            request: &str,
        ) -> cog_core::SFResult<serde_json::Value> {
            *self.0.lock().unwrap() = Some((batch.as_str().to_string(), request.to_string()));
            Ok(serde_json::json!({ "diff": format!("for {request}") }))
        }
    }

    /// 判词的两种行各读一半；一却没有 `TODO` 行是崩了，不是「全真」。
    #[test]
    fn the_edit_verdict_reads_both_lines_and_refuses_a_bare_one() {
        let (ok, missing) = parse_editing(
            Some(1),
            "可以 N01\nTODO N02（触点 crates/a 一处都没动）\n",
            "",
            "x",
        )
        .unwrap();
        assert_eq!(ok, vec!["N01".to_string()]);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].0, "N02");
        assert!(missing[0].1.contains("crates/a"));
        assert!(parse_editing(Some(1), "", "boom", "x").is_err());
        assert!(parse_editing(Some(2), "", "台账读不到", "x").is_err());
    }

    /// 整批一次进：请求里带的是整批，不是一条；同一问带着这一批的名字。
    #[tokio::test]
    async fn the_whole_batch_goes_in_as_one_request() {
        let agent = Arc::new(Echo(std::sync::Mutex::new(None)));
        let generator = BatchGenerator::new(agent.clone());
        let batch = BatchId::from_ids(["S1".to_string(), "S2".to_string(), "S3".to_string()]);
        let out = generator
            .generate(&batch, "## S1\n## S2\n## S3\n")
            .await
            .expect("the agent answers");
        let seen = agent.0.lock().unwrap().clone();
        assert_eq!(
            seen.as_ref().map(|(_, request)| request.as_str()),
            Some("## S1\n## S2\n## S3\n"),
            "进 agent 的不是整批"
        );
        assert_eq!(
            seen.as_ref().map(|(id, _)| id.as_str()),
            Some("batch-S1-S2-S3"),
            "生成这一问没有带着这一批的名字：写下的调用事件归不到批"
        );
        assert_eq!(out["diff"], "for ## S1\n## S2\n## S3\n");
    }

    /// 请求里钉住整批编辑的三条：一次要全工具、零截断（指针取全文）、一次交齐。
    #[test]
    fn the_request_states_the_three_batch_edit_rules() {
        let text = compose(
            std::path::Path::new("/w"),
            &[("S1".to_string(), "缺 x".to_string())],
        );
        for needle in ["**全部**工具调用", "artifact://", "整批一次出 diff"] {
            assert!(text.contains(needle), "请求缺 {needle:?}：{text}");
        }
        assert!(text.contains("## S1"), "整批的每一条都要进请求：{text}");
    }

    /// 批号由 id 现算，与顺序无关，重复的 id 不产生第二个名字。
    ///
    /// 与顺序无关是必需的：台账里同一批的次序会随编辑变化，名字跟着变的话，「这一批
    /// 花了几次调用」每次都是新的一本账，重放也就对不上上一次。
    #[test]
    fn the_batch_name_is_the_same_whatever_order_the_rows_came_in() {
        let a = BatchId::from_ids(["S2".to_string(), "S1".to_string(), "S2".to_string()]);
        let b = BatchId::from_ids(["S1".to_string(), "S2".to_string()]);
        assert_eq!(a, b);
        assert_eq!(a.as_str(), "batch-S1-S2");
    }
}

/// 这一轮的假清单**以前见过**（同一批 id、同一批理由）⇒ 再问一次只会得到同一份输入，停。
///
/// 判「见过」而不是「与上一轮相同」：只比相邻两轮时，`S1 → S2 → S1 → S2` 这种来回
/// 每一对相邻项都不同，循环永远不停（2026-11-22 复核发现）。比「见过的集合」把这一族
/// 都收进来，而理由也进指纹——同一批 id 的理由变了，说明喂给 generator 的输入变了，
/// 那是进展，不该被当成停滞。
///
/// 它不是「最多跑几轮」：轮数不是判据，判词为空才是；这一条只是无进展时的出口。
pub fn no_progress(seen: &std::collections::HashSet<String>, current: &[(String, String)]) -> bool {
    seen.contains(&fingerprint(current))
}

/// 一轮假清单的身份：id ＋ 理由，排序后拼成一个串。
fn fingerprint(missing: &[(String, String)]) -> String {
    let mut parts: Vec<String> = missing
        .iter()
        .map(|(id, why)| format!("{id}\u{0}{why}"))
        .collect();
    parts.sort();
    parts.join("\u{1e}")
}

pub async fn run_loop(root: &std::path::Path, generator: &BatchGenerator) -> Result<usize, String> {
    let mut seen: std::collections::HashSet<String> = Default::default();
    let mut rounds = 0usize;
    loop {
        let (_, missing) = edit_verdict(root)?;
        if missing.is_empty() {
            return Ok(rounds);
        }
        if !seen.insert(fingerprint(&missing)) {
            let ids: Vec<String> = missing.iter().map(|(id, _)| id.clone()).collect();
            return Err(format!(
                "batch-edit: 这一批的假清单又回到了见过的状态（{}）；再问一次只会是同一份输入",
                ids.join("、")
            ));
        }
        let request = compose(root, &missing);
        // 批号按这一批的 id 现算：同一批重放得到同一个名字，调用事件才按批查得到；
        // 名字里不含理由，理由变了是同一个工作集在动，不是另一批。
        let batch = BatchId::from_ids(missing.iter().map(|(id, _)| id.clone()));
        generator
            .generate(&batch, &request)
            .await
            .map_err(|e| format!("generator 失败：{e}"))?;
        rounds += 1;
    }
}

#[cfg(test)]
mod loop_tests {
    use super::*;

    fn pair(id: &str, why: &str) -> (String, String) {
        (id.to_string(), why.to_string())
    }

    /// 第一次见到的那份清单不是停滞。
    #[test]
    fn the_first_round_is_never_stalled() {
        let seen = std::collections::HashSet::new();
        assert!(!no_progress(&seen, &[pair("S1", "缺测试")]));
    }

    /// 同一份清单第二次出现：无进展。
    #[test]
    fn the_same_list_twice_is_a_stall() {
        let mut seen = std::collections::HashSet::new();
        let missing = vec![pair("S1", "缺测试")];
        assert!(!no_progress(&seen, &missing));
        seen.insert(fingerprint(&missing));
        assert!(no_progress(&seen, &missing));
    }

    /// 来回的清单也是停滞：只比相邻两轮的话，`S1 → S2 → S1` 每一对都不同、
    /// 循环永远不停。
    #[test]
    fn a_list_that_comes_back_after_another_is_still_a_stall() {
        let mut seen = std::collections::HashSet::new();
        let first = vec![pair("S1", "缺测试")];
        let second = vec![pair("S2", "缺测试")];
        seen.insert(fingerprint(&first));
        seen.insert(fingerprint(&second));
        assert!(no_progress(&seen, &first));
    }

    /// 同一批 id、理由变了 = 喂进去的输入变了，那是进展。
    #[test]
    fn the_same_ids_with_new_reasons_are_progress() {
        let mut seen = std::collections::HashSet::new();
        seen.insert(fingerprint(&[pair("S1", "缺测试")]));
        assert!(!no_progress(
            &seen,
            &[pair("S1", "测试在树里但点名的是别的用例")]
        ));
    }
}

#[cfg(test)]
mod principle_brief_tests {
    use super::*;
    use cog_core::{BundledPrinciple, PrincipleBundle};

    fn principle(slug: &str, theme: &str, lead: &str, gate: &str) -> BundledPrinciple {
        BundledPrinciple {
            slug: slug.into(),
            source: "list-principles.sh".into(),
            lead: lead.into(),
            carrier_path: "memory/x.md".into(),
            carrier_sha256: "0".repeat(64),
            host_gate: Some(gate.into()),
            where_red: None,
            check_kind: None,
            priority: None,
            themes: vec![theme.into()],
            body: format!("{lead}的正文"),
        }
    }

    #[test]
    fn only_the_principles_matching_the_request_paths_are_appended() {
        let corpus = PrincipleBundle {
            version: cog_core::BUNDLE_VERSION,
            built_at: "2026-11-25T00:00:00Z".into(),
            principles: vec![
                principle("hit", "crates/cog-agent/src", "上下文规则", "gate-a"),
                principle("miss", "crates/cog-eval/src", "评测规则", "gate-b"),
            ],
        };
        let request = "改 crates/cog-agent/src/context.rs 的裁剪";
        let out = with_principles(request, Some(&corpus));
        assert!(out.contains("上下文规则"), "{out}");
        assert!(out.contains("gate-a"), "门要跟着原则一起给：{out}");
        assert!(!out.contains("评测规则"), "没命中的原则不该出现：{out}");
        assert_eq!(with_principles(request, None), request);
    }
}
