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

/// 读一次 2.5 的判词，返回（通过的 id，未通过的「id + 理由」）。
fn edit_verdict(root: &std::path::Path) -> Result<(Vec<String>, Vec<(String, String)>), String> {
    let reader = root.join("scripts/hooks/edit-reading.py");
    let out = Command::new("python3")
        .arg(&reader)
        .arg("--root")
        .arg(root)
        .arg("--stage")
        .arg("edit")
        .output()
        .map_err(|e| format!("run {}: {e}", reader.display()))?;
    if out.status.code() == Some(2) {
        return Err(String::from_utf8_lossy(&out.stderr).into_owned());
    }
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
            .prompt(serde_json::json!({ "batch": batch, "role": "generator" }))
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
    Err(format!(
        "batch-edit: {} 条还没生成，但模型通道还没接（S79）。先看 --dry-run 的请求。",
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
