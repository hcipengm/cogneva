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
fn edit_verdict(root: &std::path::Path) -> Result<EditVerdict, String> {
    let reader = root.join("scripts/hooks/edit-reading.py");
    let out = Command::new("python3")
        .arg(&reader)
        .arg("--root")
        .arg(root)
        .arg("--stage")
        .arg("edit")
        .output()
        .map_err(|e| format!("run {}: {e}", reader.display()))?;
    // 判词脚本的契约是 0=全真、1=有假、2=读不到。**其余任何退出码都不是判词**：
    // 未捕获异常是 1、被信号杀死是 `None`——把这两种读成「有假」都还好，读成「全真」
    // 就是把一次崩溃报成了绿。所以只认 0 与 1，别的都当「判不了」。
    match out.status.code() {
        Some(0) | Some(1) => {}
        _ => {
            return Err(format!(
                "{} 没有给出判词（退出码 {:?}）：{}",
                reader.display(),
                out.status.code(),
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        }
    }
    // 退出码 1 有两种来路：判词说「有假」，或者脚本自己崩了（Python 未捕获异常也是
    // 1）。前者一定有 `代码=false` 行，后者一行判词都没有——把后者读成「全真」正是
    // 这一族最贵的形状，所以这里要求 1 必须带着判词回来。
    let text = String::from_utf8_lossy(&out.stdout);
    let mut ok = Vec::new();
    let mut missing = Vec::new();
    for line in text.lines() {
        let line = line.trim_start();
        if let Some(rest) = line.strip_prefix("代码=true") {
            ok.push(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("代码=false") {
            let (id, why) = rest.split_once('：').unwrap_or((rest, ""));
            missing.push((id.trim().to_string(), why.trim().to_string()));
        }
    }
    if out.status.code() == Some(1) && missing.is_empty() {
        return Err(format!(
            "{} 退出码是 1，却没有一行 `代码=false`——那是脚本崩了，不是判词：{}",
            reader.display(),
            String::from_utf8_lossy(&out.stderr).trim()
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
        "# 树：{}\n# 要求：整批一次出 diff；工具结果不完备之前不许出。\n",
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
#[async_trait::async_trait]
pub trait GeneratorCall: Send + Sync {
    async fn call(&self, batch: &str) -> cog_core::SFResult<serde_json::Value>;
}

/// 生产实现：交给 agent。
pub struct AgentCall {
    pub agent: Arc<dyn cog_core::Agent>,
}

#[async_trait::async_trait]
impl GeneratorCall for AgentCall {
    async fn call(&self, batch: &str) -> cog_core::SFResult<serde_json::Value> {
        self.agent
            // 角色名从声明读，不在这里再写一遍：改名时改一处，这一侧跟着走。
            .prompt(serde_json::json!({ "batch": batch, "role": cog_core::roles::GENERATOR }))
            .await
    }
}

pub struct BatchGenerator {
    call: Arc<dyn GeneratorCall>,
}

impl BatchGenerator {
    pub fn new(call: Arc<dyn GeneratorCall>) -> Self {
        Self { call }
    }

    /// 生产构造：把一个 agent 包成这次生成要的那一个调用。
    pub fn from_agent(agent: Arc<dyn cog_core::Agent>) -> Self {
        Self {
            call: Arc::new(AgentCall { agent }),
        }
    }

    /// 整批一次进、拿回这次运行的结果。工具轮次由 agent 自己决定，判据是信息完备
    /// （它够了才出 diff），这里不设上限。
    pub async fn generate(&self, batch: &str) -> cog_core::SFResult<serde_json::Value> {
        self.call.call(batch).await
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

    /// 一个只回显的调用：判的是「整批原样进、答案原样出」，与模型无关。
    struct Echo(std::sync::Mutex<Option<String>>);

    #[async_trait::async_trait]
    impl GeneratorCall for Echo {
        async fn call(&self, batch: &str) -> cog_core::SFResult<serde_json::Value> {
            *self.0.lock().unwrap() = Some(batch.to_string());
            Ok(serde_json::json!({ "diff": format!("for {batch}") }))
        }
    }

    /// 整批一次进：请求里带的是整批，不是一条。
    #[tokio::test]
    async fn the_whole_batch_goes_in_as_one_request() {
        let agent = Arc::new(Echo(std::sync::Mutex::new(None)));
        let generator = BatchGenerator::new(agent.clone());
        let out = generator
            .generate("## S1\n## S2\n## S3\n")
            .await
            .expect("the agent answers");
        assert_eq!(
            agent.0.lock().unwrap().as_deref(),
            Some("## S1\n## S2\n## S3\n"),
            "进 agent 的不是整批"
        );
        assert_eq!(out["diff"], "for ## S1\n## S2\n## S3\n");
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
        generator
            .generate(&request)
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
