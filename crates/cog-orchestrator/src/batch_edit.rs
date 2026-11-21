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

/// `cogneva batch-edit`。`--dry-run` 只打请求；没有模型通道时报错退出。
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
