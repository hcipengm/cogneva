//! 工具执行之前的那一问：谁在问、问什么、答案怎么回到模型。
//!
//! 这一层只做三件事：把「这一轮处在哪种模式」写成闭集；把「哪些工具不问」写成
//! 一张显式的只读名单；在工具真正跑之前给出一个结论——允许、拒绝并带理由，或者
//! 交给人去问。它不关心问的人是谁：网页背后是一个连接，命令行背后是一个人，
//! 只要实现了 [`PermissionDecider`]，内核看到的都是同一次询问。
//!
//! 默认是问。名单只列「读了也不会改变任何东西」的工具，因为漏列的代价是多问一句
//! （可见、可补），而多列一个的代价是一次没人看见的写盘。

use std::sync::Arc;

/// 这一轮处在哪种模式下。闭集：不认识的模式不猜一个默认值，交回给调用方拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PermissionMode {
    /// 只读：任何会改变工作树或外部世界的工具都不执行，连问都不问。
    Plan,
    /// 默认：只读工具直接跑，其余每一样都先问人。
    #[default]
    Default,
    /// 文件编辑直接跑，其余（跑命令、提交、发消息）仍然问。
    AcceptEdits,
    /// 全不问。显式选择，且这个选择本身是读得出来的（见 [`PermissionMode::as_str`]）。
    Bypass,
}

impl PermissionMode {
    /// 模式名。闭集的每一格都要有一个名字，因为会话上可读的是这个名字，不是枚举。
    pub const NAMES: [&'static str; 4] = ["plan", "default", "acceptEdits", "bypass"];

    /// 从会话上读到的名字解析回来。不认识就是 `None`：一个拼错的模式名不该
    /// 静默落进「默认」，那会让一次手误看起来像一次没有发生的询问。
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim() {
            "plan" => Some(Self::Plan),
            "default" => Some(Self::Default),
            "acceptEdits" => Some(Self::AcceptEdits),
            "bypass" => Some(Self::Bypass),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Default => "default",
            Self::AcceptEdits => "acceptEdits",
            Self::Bypass => "bypass",
        }
    }
}

/// 一次询问的全部内容。
///
/// 参数一起带上，因为「写哪里」正是人要看的那一格：只报工具名的话，屏幕上出现的
/// 是一百次一模一样的「write_file」。
#[derive(Debug, Clone)]
pub struct PermissionAsk {
    pub call_id: String,
    pub agent_id: String,
    pub tool: String,
    pub arguments: serde_json::Value,
    pub mode: PermissionMode,
}

/// 问回来的结论。拒绝必须带理由：理由要交给模型，它据此换一条路，而不是把同一次
/// 调用再发一遍。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionVerdict {
    Allow,
    Deny { reason: String },
}

impl PermissionVerdict {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// 谁回答那一问。实现在持有「人」的那一侧：网页连接、命令行、或者一个明确
/// 声明「这里没人可问、按某个词直接判」的调用方。
#[async_trait::async_trait]
pub trait PermissionDecider: Send + Sync {
    async fn decide(&self, ask: &PermissionAsk) -> PermissionVerdict;
}

/// 读了也不会改变任何东西的工具。
///
/// 名单是显式的、按名字列的，不是按前缀或按「看起来像读」猜的：`raw_fetch` 与
/// `read_file` 只差一个词，而 `apply_patch` 与 `git_diff` 也一样。名单外的一律要问。
pub const READ_ONLY_TOOLS: &[&str] = &[
    "read_file",
    "grep",
    "search_code",
    "glob",
    "git_status",
    "git_diff",
    "todo",
    "list_agents",
    "read_task_ledger",
    "command_output",
    "raw_list",
    "raw_fetch",
];

/// 只改文件、不碰外部世界的工具。`acceptEdits` 模式放行的就是这一组。
pub const EDIT_TOOLS: &[&str] = &["write_file", "edit_file", "apply_patch"];

pub fn is_read_only(tool: &str) -> bool {
    READ_ONLY_TOOLS.contains(&tool)
}

pub fn is_edit(tool: &str) -> bool {
    EDIT_TOOLS.contains(&tool)
}

/// 不用问就能定的结论；`None` 表示这一问要交给人。
///
/// 四个模式各有一条独立的路，而不是「默认放行、例外再拦」：后者每加一个模式都要
/// 回头检查前一个模式漏了什么。
pub fn decided_without_asking(mode: PermissionMode, tool: &str) -> Option<PermissionVerdict> {
    match mode {
        PermissionMode::Bypass => Some(PermissionVerdict::Allow),
        PermissionMode::Plan => Some(if is_read_only(tool) {
            PermissionVerdict::Allow
        } else {
            PermissionVerdict::Deny {
                reason: format!(
                    "plan 模式只执行只读工具，`{tool}` 会改变工作树或外部世界，没有执行。\
                     要动手请先切到 default 模式。"
                ),
            }
        }),
        PermissionMode::AcceptEdits => {
            if is_read_only(tool) || is_edit(tool) {
                Some(PermissionVerdict::Allow)
            } else {
                None
            }
        }
        PermissionMode::Default => is_read_only(tool).then_some(PermissionVerdict::Allow),
    }
}

/// 一条留下来的规则管多大范围。
///
/// 只有两格，因为这两格回答的是两个不同的问题：`Session` 是「这一段对话里别再问我」，
/// `Owner` 是「这个人往后都别再问」。再细分（按项目、按工具组）没有第二个消费者，
/// 而多一格就要多一条「它什么时候失效」的说明。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionScope {
    /// 管一段对话：会话号变了就失效。
    Session { session_id: String },
    /// 管一个人：他在哪一段对话里都算数，重启后还在。
    Owner,
}

impl PermissionScope {
    /// 这个范围属于谁：`Some(属主名)` 或 `Some(会话号)`。存的时候按它分键，
    /// 一格里放一个范围的全部规则。
    pub fn key(&self) -> String {
        match self {
            Self::Session { session_id } => format!("session:{session_id}"),
            Self::Owner => "owner".to_string(),
        }
    }
}

/// 人答过一次「总是这样」之后留下来的一条规则。
///
/// 规则的身份就是它自己的内容——工具、对象、放还是拦、管多大范围。没有另外生一个
/// 随机 id：同一个意思点两次只该有一条规则，撤销时按内容撤销也不会撤错那一条。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRule {
    pub tool: String,
    /// 只看这一个工具碰的东西：命令行的第一段、路径。`None` 表示这个工具不带对象。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// `true` = 直接放行，`false` = 直接拒绝。拒绝优先于放行，见
    /// [`PermissionRuleSet::evaluate`]。
    pub allow: bool,
    pub scope: PermissionScope,
}

impl PermissionRule {
    /// 规则的可读身份。撤销、去重、界面上那一行都用它；同一个意思只对应一个名字。
    pub fn id(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            if self.allow { "allow" } else { "deny" },
            self.tool,
            self.subject.as_deref().unwrap_or("*"),
            self.scope.key()
        )
    }

    fn covers(&self, ask: &PermissionAsk) -> bool {
        if self.tool != ask.tool {
            return false;
        }
        let subject = ask_subject(&ask.arguments);
        match (&self.subject, subject) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(rule), Some(subject)) => subject_covers(rule, subject),
        }
    }
}

/// 一次调用的参数里「它要动什么」的那一格。与问句摘要读的是同一组键，但只用第一个：
/// 摘要给人看，规则要一个稳定的比较对象。
pub fn ask_subject(arguments: &serde_json::Value) -> Option<&str> {
    const LEAD: [&str; 4] = ["command", "path", "file_path", "pattern"];
    let map = arguments.as_object()?;
    LEAD.iter()
        .find_map(|key| map.get(*key).and_then(|value| value.as_str()))
        .map(str::trim)
        .filter(|subject| !subject.is_empty())
}

/// 规则里的对象能盖住这次调用的对象吗。
///
/// 按「整段前缀」算，且前缀必须断在分隔符上：规则 `git push` 盖住 `git push --force`，
/// 但规则 `git` 不盖 `github`——否则一条看着很窄的规则会悄悄放到一串别的命令。
fn subject_covers(rule: &str, subject: &str) -> bool {
    if rule == subject {
        return true;
    }
    let Some(rest) = subject.strip_prefix(rule) else {
        return false;
    };
    matches!(rest.chars().next(), Some(' ' | '/' | ':' | '\t'))
}

/// 一组规则，按人拿在手上的那一份整体读、整体写。
///
/// 评估只有一条路：命中的规则里只要有拒绝，结论就是拒绝。放行是「没有人反对」，
/// 拒绝是「有人反对」——反过来（先看放行）会让一条后来加的拒绝永远轮不到它说话。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PermissionRuleSet {
    rules: Vec<PermissionRule>,
}

impl PermissionRuleSet {
    pub fn new(rules: Vec<PermissionRule>) -> Self {
        let mut set = Self::default();
        for rule in rules {
            set.insert(rule);
        }
        set
    }

    /// 加一条规则。同一条（内容相同）只留一份；返回是否真的加进去了。
    pub fn insert(&mut self, rule: PermissionRule) -> bool {
        if self.rules.iter().any(|existing| *existing == rule) {
            return false;
        }
        self.rules.push(rule);
        true
    }

    /// 按身份撤掉一条。返回是否真的撤掉了。
    pub fn remove(&mut self, id: &str) -> bool {
        let before = self.rules.len();
        self.rules.retain(|rule| rule.id() != id);
        before != self.rules.len()
    }

    pub fn rules(&self) -> &[PermissionRule] {
        &self.rules
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// 这次调用能不能由留下来的规则定下来；`None` = 没有规则管它，照常问人。
    ///
    /// `session_id` 用来判断会话范围的规则在这一段对话里算不算数。
    pub fn evaluate(&self, ask: &PermissionAsk, session_id: &str) -> Option<PermissionVerdict> {
        let applies = |rule: &&PermissionRule| {
            let in_scope = match &rule.scope {
                PermissionScope::Owner => true,
                PermissionScope::Session { session_id: ruled } => ruled == session_id,
            };
            in_scope && rule.covers(ask)
        };
        let matched: Vec<&PermissionRule> = self.rules.iter().filter(applies).collect();
        if let Some(deny) = matched.iter().find(|rule| !rule.allow) {
            return Some(PermissionVerdict::Deny {
                reason: format!(
                    "`{}` 命中一条留下来的人定规则（{}），没有执行。要改主意，把这条规则撤掉。",
                    ask.tool,
                    deny.subject.as_deref().unwrap_or("全部")
                ),
            });
        }
        matched
            .iter()
            .any(|rule| rule.allow)
            .then_some(PermissionVerdict::Allow)
    }
}

/// 把一次询问落成结论：先看模式能不能自己定，不能就问人；没有人可问时拒绝，
/// 而不是放行。放行等于把一个没人做过的决定记成一个做过的决定。
pub async fn decide(
    mode: PermissionMode,
    decider: Option<&Arc<dyn PermissionDecider>>,
    ask: &PermissionAsk,
) -> PermissionVerdict {
    if let Some(verdict) = decided_without_asking(mode, &ask.tool) {
        return verdict;
    }
    match decider {
        Some(decider) => decider.decide(ask).await,
        None => PermissionVerdict::Deny {
            reason: format!(
                "`{}` 在 {} 模式下需要人确认，但这一轮没有可以问的人（没有配置确认通道），\
                 所以没有执行。",
                ask.tool,
                mode.as_str()
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 那张只读名单必须真的只装只读工具：名单里的每一个都该在工具注册表里，
    /// 且不属于会写盘的那一组。
    #[test]
    fn the_read_only_list_stays_read_only() {
        for tool in READ_ONLY_TOOLS {
            assert!(!EDIT_TOOLS.contains(tool), "{tool} 同时出现在两张名单里");
        }
        assert!(is_read_only("read_file"));
        assert!(!is_read_only("write_file"));
        assert!(!is_read_only("run_command"));
    }

    /// 换模式 ⇒ 换结论。这条是模式存在的理由：同一个工具在四种模式下得到四种待遇，
    /// 而不是四种名字指向同一个行为。
    #[test]
    fn the_mode_decides_what_the_tool_name_cannot() {
        let denied = |mode, tool: &str| {
            matches!(
                decided_without_asking(mode, tool),
                Some(PermissionVerdict::Deny { .. })
            )
        };
        assert!(denied(PermissionMode::Plan, "write_file"));
        assert!(!denied(PermissionMode::Plan, "read_file"));
        assert_eq!(
            decided_without_asking(PermissionMode::Bypass, "write_file"),
            Some(PermissionVerdict::Allow)
        );
        assert_eq!(
            decided_without_asking(PermissionMode::AcceptEdits, "write_file"),
            Some(PermissionVerdict::Allow)
        );
        assert_eq!(
            decided_without_asking(PermissionMode::AcceptEdits, "run_command"),
            None,
            "acceptEdits 只放行编辑，跑命令仍然要问"
        );
        assert_eq!(
            decided_without_asking(PermissionMode::Default, "run_command"),
            None
        );
        assert_eq!(
            decided_without_asking(PermissionMode::Default, "read_file"),
            Some(PermissionVerdict::Allow)
        );
        assert_eq!(
            decided_without_asking(PermissionMode::Plan, "read_file"),
            Some(PermissionVerdict::Allow)
        );
    }

    /// 模式名是可读的一面：解析与打印必须互逆，拼错的名字不许落进默认。
    #[test]
    fn the_mode_names_round_trip_and_a_typo_is_not_default() {
        for name in PermissionMode::NAMES {
            let mode = PermissionMode::parse(name).expect("闭集里的每个名字都该解析得回来");
            assert_eq!(mode.as_str(), name);
        }
        assert_eq!(PermissionMode::parse("defaultt"), None);
        assert_eq!(PermissionMode::parse(""), None);
    }

    fn ask_for(tool: &str, arguments: serde_json::Value) -> PermissionAsk {
        PermissionAsk {
            call_id: "call-1".into(),
            agent_id: "worker".into(),
            tool: tool.into(),
            arguments,
            mode: PermissionMode::Default,
        }
    }

    fn rule(
        allow: bool,
        tool: &str,
        subject: Option<&str>,
        scope: PermissionScope,
    ) -> PermissionRule {
        PermissionRule {
            tool: tool.into(),
            subject: subject.map(str::to_string),
            allow,
            scope,
        }
    }

    /// 两条规则同时命中时，拒绝那条说了算——不管谁先写下来。
    ///
    /// 反过来（先看放行）的代价不对称：一条后来加的拒绝会永远轮不到它说话，
    /// 而界面上它明明躺着。
    #[test]
    fn a_deny_rule_wins_over_an_allow_rule_that_also_matches() {
        let ask = ask_for(
            "run_command",
            serde_json::json!({"command": "git push --force"}),
        );
        let scope = PermissionScope::Owner;
        for order in [
            vec![
                rule(true, "run_command", Some("git push"), scope.clone()),
                rule(false, "run_command", Some("git push"), scope.clone()),
            ],
            vec![
                rule(false, "run_command", Some("git push"), scope.clone()),
                rule(true, "run_command", Some("git push"), scope.clone()),
            ],
        ] {
            let set = PermissionRuleSet::new(order);
            match set.evaluate(&ask, "s1") {
                Some(PermissionVerdict::Deny { reason }) => {
                    assert!(reason.contains("git push"), "{reason}")
                }
                other => panic!("两条规则都命中时必须是拒绝，得到 {other:?}"),
            }
        }
    }

    /// 撤销之后重新开始问：撤掉的规则不再给出任何结论，而不是给一个相反的结论。
    #[test]
    fn revoking_the_rule_makes_the_ask_come_back() {
        let ask = ask_for("write_file", serde_json::json!({"path": "src/a.rs"}));
        let allowed = rule(true, "write_file", Some("src/a.rs"), PermissionScope::Owner);
        let mut set = PermissionRuleSet::new(vec![allowed.clone()]);
        assert_eq!(set.evaluate(&ask, "s1"), Some(PermissionVerdict::Allow));
        assert!(set.remove(&allowed.id()));
        assert_eq!(set.evaluate(&ask, "s1"), None, "撤掉之后要重新问人");
        assert!(!set.remove(&allowed.id()), "同一条撤第二次是没撤到");
    }

    /// 同一个意思点两次只留一条：规则的身份是内容，不是生成的号。
    #[test]
    fn the_same_rule_written_twice_is_one_rule() {
        let mut set = PermissionRuleSet::default();
        let owned = rule(
            true,
            "run_command",
            Some("git push"),
            PermissionScope::Owner,
        );
        assert!(set.insert(owned.clone()));
        assert!(!set.insert(owned.clone()));
        assert_eq!(set.rules().len(), 1);
    }

    /// 会话范围的规则只在它那一格里算数；人范围的规则在他每一段对话里都算数。
    #[test]
    fn a_session_rule_stops_at_its_session_and_an_owner_rule_does_not() {
        let ask = ask_for("run_command", serde_json::json!({"command": "cargo test"}));
        let here = PermissionRuleSet::new(vec![rule(
            true,
            "run_command",
            Some("cargo test"),
            PermissionScope::Session {
                session_id: "s1".into(),
            },
        )]);
        assert_eq!(here.evaluate(&ask, "s1"), Some(PermissionVerdict::Allow));
        assert_eq!(here.evaluate(&ask, "s2"), None, "换一段对话就不再算数");

        let everywhere = PermissionRuleSet::new(vec![rule(
            true,
            "run_command",
            Some("cargo test"),
            PermissionScope::Owner,
        )]);
        assert_eq!(
            everywhere.evaluate(&ask, "s2"),
            Some(PermissionVerdict::Allow)
        );
    }

    /// 对象按整段前缀比，且断在分隔符上：`git push` 盖 `git push --force`，
    /// 不盖 `git pushx`，`git` 也不盖 `github`。窄的规则不许悄悄放宽。
    #[test]
    fn a_subject_covers_a_prefix_that_breaks_on_a_separator() {
        let push = ask_for(
            "run_command",
            serde_json::json!({"command": "git push --force"}),
        );
        let allow_push = PermissionRuleSet::new(vec![rule(
            true,
            "run_command",
            Some("git push"),
            PermissionScope::Owner,
        )]);
        assert_eq!(
            allow_push.evaluate(&push, "s1"),
            Some(PermissionVerdict::Allow)
        );

        for (subject, ask) in [
            (
                "git push",
                ask_for("run_command", serde_json::json!({"command": "git pushx"})),
            ),
            (
                "git",
                ask_for("run_command", serde_json::json!({"command": "github"})).clone(),
            ),
        ] {
            let set = PermissionRuleSet::new(vec![rule(
                true,
                "run_command",
                Some(subject),
                PermissionScope::Owner,
            )]);
            assert_eq!(set.evaluate(&ask, "s1"), None, "{subject} 不该盖住这一段");
        }

        // 带对象的规则不认没有对象的调用：那个工具要动的东西规则里没写。
        let bare = ask_for("run_command", serde_json::json!({}));
        assert_eq!(allow_push.evaluate(&bare, "s1"), None);
        // 反过来，不带对象的规则盖住这个工具的全部调用。
        let any_tool_call = PermissionRuleSet::new(vec![rule(
            true,
            "run_command",
            None,
            PermissionScope::Owner,
        )]);
        assert_eq!(
            any_tool_call.evaluate(&bare, "s1"),
            Some(PermissionVerdict::Allow)
        );
    }

    /// 规则要能原样写成一段字再读回来：它存的是 KV 里的一个字符串，编码丢了
    /// 字段就等于撤销了一条人写的规则。
    #[test]
    fn a_rule_set_survives_a_round_trip_through_json() {
        let set = PermissionRuleSet::new(vec![
            rule(
                true,
                "run_command",
                Some("cargo test"),
                PermissionScope::Owner,
            ),
            rule(
                false,
                "write_file",
                None,
                PermissionScope::Session {
                    session_id: "s1".into(),
                },
            ),
        ]);
        let text = serde_json::to_string(&set).expect("规则要能序列化");
        let back: PermissionRuleSet = serde_json::from_str(&text).expect("规则要能读回来");
        assert_eq!(back, set);
        let ids: Vec<String> = back.rules().iter().map(PermissionRule::id).collect();
        assert_eq!(ids[0], "allow:run_command:cargo test:owner");
        assert_eq!(ids[1], "deny:write_file:*:session:s1");
    }

    /// 没有人可问时，需要问的那一步是拒绝而不是放行；且拒绝里带着「为什么没人」。
    #[tokio::test]
    async fn a_question_with_nobody_to_ask_is_refused_not_allowed() {
        let ask = PermissionAsk {
            call_id: "call-1".into(),
            agent_id: "worker".into(),
            tool: "write_file".into(),
            arguments: serde_json::json!({"path": "a.txt"}),
            mode: PermissionMode::Default,
        };
        let verdict = decide(PermissionMode::Default, None, &ask).await;
        match verdict {
            PermissionVerdict::Deny { reason } => {
                assert!(reason.contains("没有可以问的人"), "{reason}");
            }
            PermissionVerdict::Allow => panic!("没有人可问时必须拒绝"),
        }
    }
}
