//! 员工申请改自己：把一条自改放进变更账，等它自己的判据落地。
//!
//! 这一层只有「申请」，没有「执行」。员工说得出「我要把这一行改成那样，因为
//! 这样」，说得出要改成什么，但它写不了「改完了」——那由看树的那一侧写。于是
//! 「自己改自己」与「自己宣布自己改好了」被拆成两件事，前者放得越宽越好，后者
//! 一次都不许。
//!
//! 落地走的是这个系统已有的那条路：声明排队 → 账的常驻写者折进账 → 看树的
//! 测量判它落没落地 → 没落地的行被交给目标路径去做。这里不新开一条捷径，因为
//! 新开的捷径就是「谁申请谁完成」。
//!
//! 唯一不许改的是量自己的那把尺子。判据载体（评测套件、门脚本、测试）由
//! [`cog_reflection::criteria_face::is_carrier_path`] 判定，用的是整个系统一直在用
//! 的那一份名单——不在这里另抄一份，抄出来的第二份迟早与第一份说的不是同一件事。
//! 理由写在那个模块里：改一道题是这个世界上最直接的移动判据的办法，而它恰好
//! 是唯一一个有动机不想让评测发现的人能干的事。

use std::sync::Arc;

use cog_core::contract::tool::Tool;
use cog_core::{ChangePriority, ChangeReading, ChangeSource, ToolHandler, ToolImplementation};

/// 员工用来申请改自己的那只工具。
pub const PROPOSE_SELF_CHANGE: &str = "propose_self_change";

/// 一次自改申请：改哪个文件、改完之后那里该有什么、为什么。
///
/// `literal` 是**逐字节**要被 grep 到的那段文字，不是一段描述。账的判据是
/// 「这个字面量在这个文件里出现过」，所以申请就得说清它长什么样；说「把上下文
/// 窗口调大」是一句愿望，说 `"context_window_size": 96000` 才是一条能被量的变更。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    /// 相对这棵树根的一个路径。
    pub path: String,
    /// 事成之后这个文件里一定存在的那段文字，逐字节。
    pub literal: String,
    /// 为什么要改。写给后来读账的人，不是写给模型自己看的。
    pub why: String,
}

/// 一条申请被拒的原因。每一种都说得出拒绝的是哪一条线，而不只是「不行」。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// 目标是一条判据载体：改它就是移动量自己的那把尺子。
    Carrier(String),
    /// 目标不在树里（绝对路径或带 `..`）：账查的是树，树外的东西没人量得到。
    OutsideTree(String),
    /// 某个必填项是空的。空话进不了账。
    Blank(&'static str),
    /// `literal` 是一句判词而不是树上的一段原文：它带着判词的分隔符，判据面读不成
    /// 一个片段，这条申请永远量不出来。
    Rendering(String),
}

impl Refusal {
    /// 回给模型的那一句。
    ///
    /// 拒绝也要说清「这是谁定的线、线在哪」，否则下一个人读到的只是一次失败，
    /// 会绕路再试一次。
    pub fn answer(&self) -> serde_json::Value {
        match self {
            Refusal::Carrier(path) => serde_json::json!({
                "queued": false,
                "refused": "criteria_carrier",
                "path": path,
                "note": format!(
                    "没有进账：{path} 是判据载体——评测套件、门脚本或测试，它们量的是\
                     这个系统做得怎么样。让它改这些，等于让它出自己那张卷子，卷子就不再是尺子。\
                     这不是能力边界，是唯一一条不许动的边界：别的一切（它自己的运行配置、\
                     源码、技能）都可以申请，只有量它的那部分留给外面。"
                ),
            }),
            Refusal::OutsideTree(path) => serde_json::json!({
                "queued": false,
                "refused": "outside_tree",
                "path": path,
                "note": format!(
                    "没有进账：{path} 不在这棵树里。账的判据是看树，「落地了没有」只有\
                     这棵树上的字节能回答；树外的路径进去就是一条永远量不出来的行。\
                     写相对树根的路径。"
                ),
            }),
            Refusal::Rendering(literal) => serde_json::json!({
                "queued": false,
                "refused": "literal_is_a_rendering",
                "literal": literal,
                "note": format!(
                    "没有进账：{literal:?} 是一句判词，不是树上的片段——它带着判词的分隔符。\
                     账的判据是「这段原文在这个文件里出现过」，所以 literal 要写成你要在\
                     文件里找到的那段原文本身。"
                ),
            }),
            Refusal::Blank(field) => serde_json::json!({
                "queued": false,
                "refused": "blank",
                "field": field,
                "note": format!(
                    "没有进账：{field} 是空的。一条申请要说出改哪里、改成什么、为什么，\
                     三项缺一项都读不成变更——后一个读账的人只能靠猜，而猜出来的东西\
                     会被当成申请本身。"
                ),
            }),
        }
    }
}

impl Proposal {
    /// 这条申请写成一条账上的读入。
    ///
    /// 来源写的是申请者自己（`self-change:<谁>`），所以账上永远看得出来这条是
    /// 谁提的：同一个人再提同一条不会变成两条，换一个人提就是两条各自的账——
    /// 「谁要的」不是可以从别处推出来的，它是这一行的一个字段。
    ///
    /// 优先级是 `Unstated`：申请者没有资格给自己的活定档，档是读账的人排的。
    pub fn reading(&self, asker: &str) -> Result<ChangeReading, Refusal> {
        let path = self.path.trim();
        let literal = self.literal.trim_end_matches(['\r', '\n']);
        let why = self.why.trim();
        if path.is_empty() {
            return Err(Refusal::Blank("path"));
        }
        if literal.trim().is_empty() {
            return Err(Refusal::Blank("literal"));
        }
        if why.is_empty() {
            return Err(Refusal::Blank("why"));
        }
        let path = path.trim_start_matches("./");
        // 形状的校验只有一份：判据面自己说得出「这条变更量得出来吗」，这里不另抄
        // 一份绝对路径／`..` 的判据——抄出来的第二份迟早与判据面说的不是一件事。
        let identity = cog_orchestrator::change_measure::measurable_identity("grep", path, literal)
            .map_err(|refused| match refused {
                cog_orchestrator::change_measure::DeclarationRefusal::ScopeNotInTree(scope) => {
                    Refusal::OutsideTree(scope)
                }
                cog_orchestrator::change_measure::DeclarationRefusal::LiteralIsARendering(
                    literal,
                ) => Refusal::Rendering(literal),
                // 这段身份写死是 `grep`，空 literal 上面已经拒过：这两种拒法在这条路上
                // 够不到，够到了就是这段代码被改坏了。
                other => unreachable!("grep 是判据面答得了的一种，且 literal 非空：{other:?}"),
            })?;
        if cog_reflection::criteria_face::is_carrier_path(path) {
            return Err(Refusal::Carrier(path.to_string()));
        }
        Ok(ChangeReading {
            identity,
            title: Some(format!("self-change: {}", one_line(why))),
            assertion: Some(format!(
                "{asker} 申请：{} 里出现 {}",
                self.path.trim(),
                literal
            )),
            source: ChangeSource {
                doc: format!("self-change:{asker}"),
                verdict: None,
                priority: ChangePriority::Unstated,
                block: Default::default(),
            },
        })
    }
}

/// 把一段理由压成一行：账的 `title` 是一行字，换行进去会把账的视图撑坏。
fn one_line(text: &str) -> String {
    let joined = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = joined.chars().take(200).collect();
    if out.chars().count() < joined.chars().count() {
        out.push('…');
    }
    out
}

/// 那只工具：账在就进账，账不在就如实说读不到。
///
/// 账缺席时不假装「已经记下了」——一个没有账的进程记不住任何申请，回一句
/// 「已受理」会让申请者以为它在账上等着，而它哪儿也没去。
pub fn tool(changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>, asker: String) -> Tool {
    let fixed = Arc::new(asker);
    build(changes, Arc::new(move || (*fixed).clone()))
}

/// 派发方没有声明身份时，这条申请在账上读作谁提的。
///
/// 编一个名字会让「没有人说」读成「就是这个人要的」，而账的那一列是申请人。
/// 说「没有人说」至少读得出这一点：这一行是真的，只是提它的人没被记下。
pub const UNATTRIBUTED_ASKER: &str = "unattributed";

/// 同一个工具，申请人由**调用时**的派发方给出。
///
/// 工具在进程里只注册一次、每个 agent 共用一张表，所以处理函数自己的状态说不
/// 出是谁在调用；派活的那一侧知道，它在 await 期间把 agent id 放在任务局部
/// 里（`cog_agent::tools::caller_agent_id`）。写死一个名字会让所有同事的申请
/// 都记成同一个人提的——「谁要的」不是能从别处推出来的，它是账上那一行的字段。
pub fn tool_for_caller(changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>) -> Tool {
    build(
        changes,
        Arc::new(|| {
            cog_agent::tools::caller_agent_id().unwrap_or_else(|| UNATTRIBUTED_ASKER.to_string())
        }),
    )
}

fn build(
    changes: Option<Arc<dyn cog_core::ChangeLedgerStore>>,
    asker: Arc<dyn Fn() -> String + Send + Sync>,
) -> Tool {
    let handler: ToolHandler = Arc::new(move |args: serde_json::Value| {
        let changes = changes.clone();
        let asker = asker.clone();
        Box::pin(async move {
            let proposal = Proposal {
                path: args
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                literal: args
                    .get("literal")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                why: args
                    .get("why")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
            };
            let reading = match proposal.reading(&asker()) {
                Ok(reading) => reading,
                Err(refusal) => return Ok(refusal.answer()),
            };
            let Some(store) = changes else {
                return Ok(serde_json::json!({
                    "queued": false,
                    "available": false,
                    "note": "这个进程没有接到变更账：申请没有进账，也没有被记住。\
                             这是「没记下」，不是「被拒了」——换一个有账的进程再提一次。",
                }));
            };
            let declared = store.declare(std::slice::from_ref(&reading)).await?;
            Ok(serde_json::json!({
                "queued": declared > 0,
                "already_queued": declared == 0,
                "identity": reading.identity.as_key(),
                "note": "进了变更账的入口队列。它不是「改好了」，也不是「答应改」：\
                         账的常驻写者会把它折进账，看树的那一侧按这个字面量量它，\
                         量到了才算落地。要看它到哪一步，用 get_change 读这一条。",
            }))
        })
    });
    Tool {
        name: PROPOSE_SELF_CHANGE.to_string(),
        description: "申请改这个系统自己：给出一个路径、改完之后那里该有的那段文字、以及为什么。\
                      进的是变更账，不是直接改文件——落地由看树的那一侧量出来。\
                      判据载体（评测套件、门脚本、测试）不受理：量自己的尺子不归自己改。"
            .to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "相对树根的路径，例如 deploy/helm/cogneva/files/cogneva.json"
                },
                "literal": {
                    "type": "string",
                    "description": "事成之后这个文件里一定存在的那段文字，逐字节照抄——账就是拿它做 grep 的判据"
                },
                "why": {
                    "type": "string",
                    "description": "为什么要改。写给后来读账的人看。"
                }
            },
            "required": ["path", "literal", "why"]
        }),
        implementation: ToolImplementation::Native(handler),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal(path: &str, literal: &str) -> Proposal {
        Proposal {
            path: path.into(),
            literal: literal.into(),
            why: "上一轮它撞了轮数天花板，读数上说不出是界在裁剪还是工作在迷路".into(),
        }
    }

    /// 申请写成的是账上的一条 `grep`：路径是范围，字面量是判据。
    ///
    /// 这一条钉住的是「申请」与「完成」之间的那道缝：账上没有别的法子把一行
    /// 变成落地，只有拿这个字面量去看树。要是这里写成了别的 kind，那条行就
    /// 再也量不动——提的人说完了，谁也不知道它到底改没改。
    #[test]
    fn a_proposal_becomes_one_grep_row_and_claims_no_state() {
        let reading = proposal(
            "deploy/helm/cogneva/files/cogneva.json",
            "\"context_window_size\": 96000",
        )
        .reading("employee-7")
        .expect("一条正当的申请");

        assert_eq!(reading.identity.kind, "grep");
        assert_eq!(
            reading.identity.scope,
            "deploy/helm/cogneva/files/cogneva.json"
        );
        assert_eq!(reading.identity.literal, "\"context_window_size\": 96000");
        assert_eq!(reading.source.doc, "self-change:employee-7");
        assert_eq!(reading.source.priority, ChangePriority::Unstated);
        // 读入里根本没有状态这一栏：这是「提的人没法宣布做完」在类型上就成立，
        // 不是靠谁记着别那么干。
        let json = serde_json::to_value(&reading).expect("读入要能序列化");
        for absent in ["state", "landed", "measured_at"] {
            assert!(json.get(absent).is_none(), "读入里出现了 {absent}");
        }
    }

    /// 量自己的那把尺子不受理，而且拒绝要说出这是哪条线。
    ///
    /// 只回一句「不行」的话，下一个人读到的是「这条路暂时不通」，会绕路再试；
    /// 说清「只有量你的那部分留给外面」，它才知道这条边界不需要绕。
    #[test]
    fn the_measuring_stick_is_refused_with_a_reason_and_a_way_round_it() {
        for path in [
            "eval_datasets/humaneval.jsonl",
            "crates/cogneva/tests/chat_turn_is_an_agent_with_control_plane_tools_test.rs",
            "deploy/scripts/check-deploy-parity.sh",
            ".github/workflows/ci.yml",
            "crates/cog-reflection/src/criteria_face.rs",
        ] {
            let refusal = proposal(path, "x")
                .reading("employee-7")
                .expect_err(&format!("{path} 是判据载体，不该受理"));
            assert_eq!(refusal, Refusal::Carrier(path.to_string()));
            let answer = refusal.answer();
            assert_eq!(answer["queued"], false);
            assert_eq!(answer["refused"], "criteria_carrier");
            assert!(
                answer["note"].as_str().unwrap_or_default().contains("尺子"),
                "{path} 的拒绝没说清拒绝的是什么"
            );
        }
    }

    /// 这棵树以外的东西不受理：账看的是树，树外没有字节可以量。
    #[test]
    fn a_path_outside_the_tree_is_refused() {
        for path in [
            "/etc/passwd",
            "../../etc/cogneva.json",
            "deploy/../../x.json",
        ] {
            let refusal = proposal(path, "x")
                .reading("employee-7")
                .expect_err(&format!("{path} 不在树里"));
            assert!(matches!(refusal, Refusal::OutsideTree(_)), "{path}");
        }
    }

    /// 它自己的运行配置是受理的——这条不变量与上一条是同一件事的两面。
    ///
    /// 尺子之所以能留给外面，正是因为除了尺子以外的东西都还开在它手上；要是
    /// 这条也拒了，「自己改自己」就只剩一句宣传语。
    #[test]
    fn its_own_runtime_configuration_is_accepted() {
        for path in [
            "deploy/helm/cogneva/files/cogneva.json",
            "./cogneva.example.json",
            "crates/cog-agent/src/config.rs",
        ] {
            let reading = proposal(path, "max_iterations")
                .reading("employee-7")
                .unwrap_or_else(|e| panic!("{path} 是它自己的配置，不该被拒：{e:?}"));
            assert_eq!(reading.identity.kind, "grep");
        }
    }

    /// 缺项的、说空话的不进账。
    #[test]
    fn a_blank_field_does_not_enter_the_ledger() {
        let mut p = proposal("cogneva.example.json", "\"a\": 1");
        p.why = "   ".into();
        assert_eq!(p.reading("employee-7"), Err(Refusal::Blank("why")));

        let mut p = proposal("cogneva.example.json", "");
        p.why = "因为".into();
        assert_eq!(p.reading("employee-7"), Err(Refusal::Blank("literal")));

        let mut p = proposal("", "x");
        p.why = "因为".into();
        assert_eq!(p.reading("employee-7"), Err(Refusal::Blank("path")));
    }

    /// 一句判词不是树上的片段：带判词分隔符的 literal 量不出来，当场拒。
    ///
    /// 这条以前会进账，然后永远停在 `undecided`——判据面看得出它读不成片段，
    /// 而那一答发生在行已经进了账之后。写点拒它的代价是提申请的人改一句，不拒
    /// 的代价是一条谁也还不了的欠债。
    #[test]
    fn a_literal_that_is_a_finding_written_out_is_refused() {
        let refusal = proposal("cogneva.example.json", "grep crates/ :: fn a()")
            .reading("employee-7")
            .expect_err("判词不是片段");
        assert!(matches!(refusal, Refusal::Rendering(_)), "{refusal:?}");
        let answer = refusal.answer();
        assert_eq!(answer["queued"], false);
        assert_eq!(answer["refused"], "literal_is_a_rendering");
        assert!(
            answer["note"].as_str().unwrap_or_default().contains("原文"),
            "拒绝没说清该写什么：{answer}"
        );
    }

    /// 账缺席时说的是「没记下」，不是「被拒了」，更不是「已受理」。
    #[tokio::test]
    async fn an_absent_ledger_says_it_was_not_recorded() {
        let tool = tool(None, "employee-7".into());
        let ToolImplementation::Native(handler) = tool.implementation else {
            panic!("不是原生工具");
        };
        let answer = handler(serde_json::json!({
            "path": "deploy/helm/cogneva/files/cogneva.json",
            "literal": "\"a\": 1",
            "why": "因为"
        }))
        .await
        .expect("缺席也要答得出来");

        assert_eq!(answer["queued"], false);
        assert_eq!(answer["available"], false);
        assert!(
            answer["note"]
                .as_str()
                .unwrap_or_default()
                .contains("没记下"),
            "账缺席被说成了别的什么：{}",
            answer["note"]
        );
    }
}
