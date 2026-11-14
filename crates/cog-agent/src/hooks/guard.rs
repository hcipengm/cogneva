//! 工具调用前的判定：阻断与改写。
//!
//! 通知型钩子（webhook / redis / log / notify）可以由发布器异步发出去——它只
//! 描述已经发生的事。**判定型**钩子不行：它必须在工具动手之前回答，答案决定
//! 这次调用跑不跑、拿什么参数跑。所以这里不是另起一套配置，而是同一份
//! [`HookDef`] 表里的另一类动作：
//!
//! * `on_tool_before` + `block`：这次调用不跑，理由作为工具结果交回模型；
//! * `on_tool_before` + `rewrite`：换掉参数再跑，模型提的那份不落地。
//!
//! 发布引擎跳过这两类动作（[`cog_core::HookAction::is_guard`]）——一个动作只有
//! 一个执行者：判定由这里做，通知由引擎做。两边都做会让一次调用被拒两次、读数
//! 里出现两条只实际发生了一条的记录。
//!
//! 「配了不等于默默生效」靠 [`Runs`]：每一条被咨询过的规则都留一条记录，随这一
//! 轮的交付物回到网关；同一次交付里还带着这份配置表本身，于是页面上能同时看见
//! 「配了几条」与「哪几条真的跑过」。

use std::sync::{Arc, Mutex};

use cog_core::{HookAction, HookDef, HookTrigger, SFError, SFResult};

/// 工具调用前钩子的签名（与 `AgentHooks::before_tool_call` 同形）。
///
/// 参数是 `&mut`：改写要能换掉模型递进来的那份，否则「改写」只能靠再加一层
/// 包装，而包装与被包装之间会开始分叉。
pub type BeforeToolCall = Arc<dyn Fn(&str, &mut serde_json::Value) -> SFResult<()> + Send + Sync>;

/// 一条会做判定的钩子。
#[derive(Debug, Clone)]
pub struct GuardRule {
    pub id: String,
    /// 这条规则管哪些工具。`None` 表示配置里没写 `tool:`——一条无差别的判定
    /// 会盖住平台上的每一只工具（含读写文件），所以它不参与判定：没写范围不
    /// 等于全都要。
    pub tool: Option<String>,
    pub action: GuardAction,
}

/// 判定动作。
#[derive(Debug, Clone)]
pub enum GuardAction {
    Block { reason: Option<String> },
    Rewrite { arguments: serde_json::Value },
}

/// 一条钩子对一次调用做过什么。
#[derive(Debug, Clone, serde::Serialize)]
pub struct HookRun {
    pub hook_id: String,
    pub tool: String,
    /// `block` 或 `rewrite`。
    pub decision: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// 这一轮里被咨询过的钩子记录。
///
/// 每次交付取走一批（[`Runs::take`]）：读数是「这一轮跑过什么」，不是从进程
/// 启动起的累计——累计在第一次大事件之后就分不出「刚才那条钩子有没有生效」。
#[derive(Debug, Default)]
pub struct Runs {
    taken: Mutex<Vec<HookRun>>,
}

impl Runs {
    fn record(&self, run: HookRun) {
        if let Ok(mut runs) = self.taken.lock() {
            runs.push(run);
        }
    }

    /// 取走并清空这一轮的记录。
    pub fn take(&self) -> Vec<HookRun> {
        self.taken
            .lock()
            .map(|mut runs| std::mem::take(&mut *runs))
            .unwrap_or_default()
    }
}

/// 从配置表里挑出判定规则。通知型动作留在这里不动，它们由发布引擎执行。
pub fn rules(defs: &[HookDef]) -> Vec<GuardRule> {
    defs.iter()
        .filter(|def| def.trigger == HookTrigger::OnToolBefore)
        .filter_map(|def| match &def.action {
            HookAction::Block { reason } => Some(GuardRule {
                id: def.id.clone(),
                tool: def.tool.clone(),
                action: GuardAction::Block {
                    reason: reason.clone(),
                },
            }),
            HookAction::Rewrite { arguments } => Some(GuardRule {
                id: def.id.clone(),
                tool: def.tool.clone(),
                action: GuardAction::Rewrite {
                    arguments: arguments.clone(),
                },
            }),
            _ => None,
        })
        .collect()
}

/// 把规则包成一个 `before_tool_call`：按配置顺序咨询，第一条做判定的说了算。
///
/// 顺序即优先级：阻断优先于改写，因为配置里的顺序就是属主写下的顺序，换一个
/// 隐含的优先级会让「我把它排在前面」失效。
pub fn before_tool_call(rules: Vec<GuardRule>, runs: Arc<Runs>) -> BeforeToolCall {
    Arc::new(move |tool: &str, arguments: &mut serde_json::Value| {
        for rule in &rules {
            let Some(pattern) = rule.tool.as_deref() else {
                continue;
            };
            if !matches_tool(pattern, tool) {
                continue;
            }
            match &rule.action {
                GuardAction::Block { reason } => {
                    let reason = reason
                        .clone()
                        .unwrap_or_else(|| format!("钩子 {} 不许调用 {}", rule.id, tool));
                    runs.record(HookRun {
                        hook_id: rule.id.clone(),
                        tool: tool.to_string(),
                        decision: "block",
                        reason: Some(reason.clone()),
                    });
                    return Err(SFError::Agent(reason));
                }
                GuardAction::Rewrite {
                    arguments: replacement,
                } => {
                    *arguments = replacement.clone();
                    runs.record(HookRun {
                        hook_id: rule.id.clone(),
                        tool: tool.to_string(),
                        decision: "rewrite",
                        reason: None,
                    });
                }
            }
        }
        Ok(())
    })
}

/// 工具的规则模式：`*` 匹配任意片段，其余按字面比较。
///
/// 不用正则：这是属主在 YAML 里手写的模式，一个错字符的正则要么编译不过、要么
/// 匹配到意料之外的名字，而 `*` 的行为在名字表上读得出来。
pub fn matches_tool(pattern: &str, tool: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == tool;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    if !tool.starts_with(parts[0]) {
        return false;
    }
    let mut rest = &tool[parts[0].len()..];
    let last = parts.len() - 1;
    for (index, part) in parts.iter().enumerate().skip(1) {
        if part.is_empty() {
            continue;
        }
        // 最后一段且模式不以 `*` 收尾：它必须落在名字末尾，而不只是出现在名字里。
        if index == last && !pattern.ends_with('*') {
            return rest.ends_with(part);
        }
        match rest.find(part) {
            Some(at) => rest = &rest[at + part.len()..],
            None => return false,
        }
    }
    true
}

/// 这一轮的读数：配了哪些钩子、这一轮哪几条跑过。
pub fn report(defs: &[HookDef], runs: &Runs) -> serde_json::Value {
    let configured: Vec<serde_json::Value> = defs
        .iter()
        .map(|def| {
            serde_json::json!({
                "id": def.id,
                "trigger": trigger_name(def.trigger),
                "tool": def.tool,
                "action": action_name(&def.action),
            })
        })
        .collect();
    serde_json::json!({
        "configured": configured,
        "ran": runs.take(),
    })
}

/// 触发器的线上名字，与配置文件里写的是同一个词。
fn trigger_name(trigger: HookTrigger) -> String {
    match serde_json::to_value(trigger) {
        Ok(serde_json::Value::String(name)) => name,
        _ => format!("{trigger:?}"),
    }
}

fn action_name(action: &HookAction) -> &'static str {
    match action {
        HookAction::Webhook { .. } => "webhook",
        HookAction::RedisStream { .. } => "redis_stream",
        HookAction::Log { .. } => "log",
        HookAction::Notify { .. } => "notify",
        HookAction::Block { .. } => "block",
        HookAction::Rewrite { .. } => "rewrite",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(id: &str, tool: Option<&str>, action: HookAction) -> HookDef {
        HookDef {
            id: id.to_string(),
            trigger: HookTrigger::OnToolBefore,
            scope: cog_core::HookScope::Global,
            crew_id_filter: None,
            squad_id_filter: None,
            tool: tool.map(str::to_string),
            action,
            rate_limit: None,
            timeout_ms: None,
        }
    }

    /// 阻断：调用不跑，理由里点得出是哪条钩子拒的。
    #[test]
    fn a_blocking_hook_refuses_the_call_and_names_itself() {
        let defs = vec![def(
            "no-bash",
            Some("bash"),
            HookAction::Block {
                reason: Some("这台机器上不跑 shell".into()),
            },
        )];
        let runs = Arc::new(Runs::default());
        let before = before_tool_call(rules(&defs), Arc::clone(&runs));

        let mut arguments = serde_json::json!({ "command": "rm -rf /" });
        let error = before("bash", &mut arguments).expect_err("这次调用必须被拒");
        assert!(error.to_string().contains("不跑 shell"), "{error}");
        assert_eq!(
            arguments,
            serde_json::json!({ "command": "rm -rf /" }),
            "被拒的调用不该顺手改掉参数：它根本没跑"
        );
        let ran = runs.take();
        assert_eq!(ran.len(), 1);
        assert_eq!(ran[0].hook_id, "no-bash");
        assert_eq!(ran[0].decision, "block");
    }

    /// 改写：参数真的换了，且记下换过。
    #[test]
    fn a_rewriting_hook_replaces_the_arguments_the_tool_will_see() {
        let defs = vec![def(
            "read-only",
            Some("git_*"),
            HookAction::Rewrite {
                arguments: serde_json::json!({ "args": ["status"] }),
            },
        )];
        let runs = Arc::new(Runs::default());
        let before = before_tool_call(rules(&defs), Arc::clone(&runs));

        let mut arguments = serde_json::json!({ "args": ["push", "--force"] });
        before("git_run", &mut arguments).expect("改写不是拒绝");
        assert_eq!(arguments, serde_json::json!({ "args": ["status"] }));
        let ran = runs.take();
        assert_eq!(ran[0].decision, "rewrite");
        assert_eq!(ran[0].tool, "git_run");
    }

    /// 模式决定管哪些工具：没被管到的那只原样通过，也不留记录。
    #[test]
    fn a_pattern_decides_which_tools_are_governed() {
        let defs = vec![def(
            "no-write",
            Some("*_write"),
            HookAction::Block { reason: None },
        )];
        let runs = Arc::new(Runs::default());
        let before = before_tool_call(rules(&defs), Arc::clone(&runs));

        let mut kept = serde_json::json!({ "path": "a" });
        assert!(before("read_file", &mut kept).is_ok());
        assert_eq!(kept, serde_json::json!({ "path": "a" }));
        assert!(runs.take().is_empty(), "没被管到的调用不该留下记录");

        assert!(before("file_write", &mut serde_json::json!({})).is_err());
        assert!(matches_tool("*_write", "file_write"));
        assert!(matches_tool("mcp__*", "mcp__search"));
        assert!(!matches_tool("bash", "bashful"));
        assert!(matches_tool("bash", "bash"));
        assert!(!matches_tool("mcp__*", "read_file"));
    }

    /// 没写 `tool:` 的判定钩子不咨询：无差别拒绝会盖住每一只工具。
    #[test]
    fn a_decision_without_a_tool_is_not_consulted() {
        let defs = vec![def("too-broad", None, HookAction::Block { reason: None })];
        let runs = Arc::new(Runs::default());
        let before = before_tool_call(rules(&defs), Arc::clone(&runs));
        assert!(before("read_file", &mut serde_json::json!({})).is_ok());
        assert!(runs.take().is_empty());
        assert_eq!(rules(&defs).len(), 1, "规则还在表里，只是不参与判定");
    }

    /// 读数：配置表与「跑过什么」在同一份报告里，取一次就清空。
    #[test]
    fn the_report_holds_the_configuration_and_what_ran_once() {
        let defs = vec![
            def("no-bash", Some("bash"), HookAction::Block { reason: None }),
            HookDef {
                trigger: HookTrigger::OnAgentEnd,
                ..def(
                    "notify-end",
                    None,
                    HookAction::Notify {
                        user_id: "u-1".into(),
                    },
                )
            },
        ];
        let runs = Arc::new(Runs::default());
        let before = before_tool_call(rules(&defs), Arc::clone(&runs));
        let _ = before("bash", &mut serde_json::json!({}));

        let reading = report(&defs, &runs);
        assert_eq!(reading["configured"].as_array().unwrap().len(), 2);
        assert_eq!(reading["configured"][0]["id"], "no-bash");
        assert_eq!(reading["configured"][0]["trigger"], "on_tool_before");
        assert_eq!(reading["configured"][1]["action"], "notify");
        assert_eq!(reading["ran"].as_array().unwrap().len(), 1);
        assert_eq!(reading["ran"][0]["tool"], "bash");
        // 第二次读是空的：读数是「这一轮」，不是从启动起的累计。
        assert!(report(&defs, &runs)["ran"].as_array().unwrap().is_empty());
    }

    /// 观测型钩子不应被当成判定：`log` on `on_tool_before` 不阻断任何调用。
    #[test]
    fn an_observational_hook_on_the_same_trigger_decides_nothing() {
        let defs = vec![def(
            "just-log",
            Some("bash"),
            HookAction::Log {
                level: cog_core::LogLevel::Info,
            },
        )];
        assert!(rules(&defs).is_empty());
        let runs = Arc::new(Runs::default());
        let before = before_tool_call(rules(&defs), Arc::clone(&runs));
        assert!(before("bash", &mut serde_json::json!({})).is_ok());
        assert!(runs.take().is_empty());
    }
}
