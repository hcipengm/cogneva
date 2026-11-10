//! 平台上此刻生效的规则，随每一轮开头的基础指令一起交给员工。
//!
//! 一条原则是三元组：**什么情况下 → 看什么 → 做什么**。它不是角色设定：三角
//! 段里没有「你是…」这种话的位置，这里也不按人、不按角色挑规则——同一份账，
//! 同一批规则，交给每一轮运行的每一个人。改一条原则是改账上的一行，不改代码，
//! 也不重编这个人。
//!
//! 规则从账上现取，不是编进二进制里的常量：和基础指令一样，进程起来后把发布
//! 出来的账交进来，每轮开头读一次。没接上账的进程（库的单元测试、独立跑的
//! 运行时）就是没有额外规则——空着，不在这里编一份兜底：兜底会让「改账」看
//! 起来生效、实际没有。
//!
//! 读不到账不算这一轮失败：拿不到规则的运行仍然有基础指令、仍然能干这件活，
//! 为一个附加段落让整轮停下来是拿产出换整洁。读失败会记一条 warn，读到的每
//! 一轮也会把「这份规则交付过」记回账上（`note_used`），让「一条生效但从未被
//! 交出去的规则」在读数面上分得开。

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;

use cog_core::{SFError, SFResult};

/// 一条原则的三段。
///
/// 三段都是文本：判它的是读到它的那个人，不是这里的代码。这里只保证它非空
/// ——一段空着的判据读起来像「这一条今天没有内容」，会被当成没有约束。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principle {
    /// 什么情况下适用。
    pub trigger: String,
    /// 看什么，看到什么算符合。
    pub criterion: String,
    /// 符合与不符合各自做什么。
    pub action: String,
}

impl Principle {
    pub fn new(
        trigger: impl Into<String>,
        criterion: impl Into<String>,
        action: impl Into<String>,
    ) -> SFResult<Self> {
        fn part(name: &str, text: String) -> SFResult<String> {
            let text = text.trim();
            if text.is_empty() {
                return Err(SFError::Validation(format!(
                    "a runtime principle needs its {name}: a triple with an empty part \
                     cannot be judged, and reads as a rule that says nothing"
                )));
            }
            Ok(text.to_string())
        }
        Ok(Self {
            trigger: part("trigger", trigger.into())?,
            criterion: part("criterion", criterion.into())?,
            action: part("action", action.into())?,
        })
    }

    /// 交给模型的三行。键名就是三元组本身：读的人在文本里找得到「什么情况」
    /// 「看什么」「做什么」这三格，而不是自己去猜一段散文的哪半句在说什么。
    fn lines(&self, index: usize, slug: &str) -> String {
        format!(
            "{index}. {slug}\n   - 什么时候适用：{}\n   - 看什么：{}\n   - 做什么：{}",
            self.trigger, self.criterion, self.action
        )
    }
}

/// 账上一条生效的规则：名字加上它的三段。
///
/// 名字跟着走，是因为读面与账要对得上：模型回报「我按第几条做的」时，人拿着
/// 这个 slug 能在账上找到那一行；只有序号的话，规则集一变序号就指到别处。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InForce {
    pub slug: String,
    pub principle: Principle,
}

impl InForce {
    pub fn new(
        slug: impl Into<String>,
        trigger: impl Into<String>,
        criterion: impl Into<String>,
        action: impl Into<String>,
    ) -> SFResult<Self> {
        let slug = slug.into().trim().to_string();
        if slug.is_empty() {
            return Err(SFError::Validation(
                "a runtime principle needs its slug: it is the name people cite".into(),
            ));
        }
        Ok(Self {
            slug,
            principle: Principle::new(trigger, criterion, action)?,
        })
    }
}

/// 账上此刻生效的那一集，按账给的次序。
///
/// 次序由账定（通过的先后），这里不重排：同一批规则每轮换一种排法，读的人会
/// 以为规则变了。
pub fn section(rules: &[InForce]) -> Option<String> {
    if rules.is_empty() {
        return None;
    }
    let mut text = String::from(
        "平台此刻生效的规则（对每个人都一样，不按人或角色分）。每条都写成「什么时候适用 → \
         看什么 → 做什么」；照做，某一条在这一轮用不上就不用提。",
    );
    for (index, rule) in rules.iter().enumerate() {
        text.push('\n');
        text.push_str(&rule.principle.lines(index + 1, &rule.slug));
    }
    Some(text)
}

/// 规则账的读面：这一轮该拿哪几条，交出去之后记一笔。
#[async_trait]
pub trait PrincipleSource: Send + Sync {
    /// 此刻生效的那一集。
    async fn in_force(&self) -> SFResult<Vec<InForce>>;

    /// 这一轮拿走了这几条：引用次数加一（或等价的一次记账）。
    async fn note_used(&self, slugs: &[String]) -> SFResult<()>;
}

static SOURCE: OnceLock<Arc<dyn PrincipleSource>> = OnceLock::new();

/// 把进程的规则账交给运行时；装配根在插件起来之后调一次。
pub fn install(source: Arc<dyn PrincipleSource>) {
    let _ = SOURCE.set(source);
}

/// 这一轮要并进系统前缀的那一段；没有账、没有生效的规则时是 `None`。
///
/// 读失败只记一条 warn：这一轮按基础指令跑完，比为一个附加段落失败更有用。
pub(crate) async fn for_this_run() -> Option<String> {
    match SOURCE.get() {
        Some(source) => section_from(&**source).await,
        None => None,
    }
}

/// 同一件事，账号由调用方给：一条循环可以带着自己的账跑（见
/// `AgentRuntime::with_principle_source`），不必先装进进程。
pub(crate) async fn section_from(source: &dyn PrincipleSource) -> Option<String> {
    let rules = match source.in_force().await {
        Ok(rules) => rules,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "runtime principles could not be read; this run carries only its base instruction"
            );
            return None;
        }
    };
    let text = section(&rules)?;
    let slugs: Vec<String> = rules.iter().map(|rule| rule.slug.clone()).collect();
    if let Err(e) = source.note_used(&slugs).await {
        // 规则已经在这份前缀里了，这一轮照跑；记账丢了只是读数少了这一轮。
        tracing::warn!(error = %e, "this run's principles were delivered but not counted");
    }
    Some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(slug: &str, trigger: &str, criterion: &str, action: &str) -> InForce {
        InForce::new(slug, trigger, criterion, action).expect("a full triple is a rule")
    }

    /// 三段齐了才成一条原则，名字（slug）也得在。
    #[test]
    fn a_rule_needs_all_three_parts_and_a_name() {
        assert!(Principle::new("触发", "判据", "动作").is_ok());
        for (trigger, criterion, action) in [
            ("", "判据", "动作"),
            ("触发", "   ", "动作"),
            ("触发", "判据", ""),
        ] {
            assert!(
                Principle::new(trigger, criterion, action).is_err(),
                "an empty part cannot be judged: {trigger:?} {criterion:?} {action:?}"
            );
        }
        assert!(InForce::new("  ", "触发", "判据", "动作").is_err());
    }

    /// 交给模型的那一段里，每条规则的三格都在，名字也在：序号会随规则集变，
    /// 名字不会。
    #[test]
    fn the_section_carries_every_part_of_every_rule_and_its_name() {
        let text = section(&[
            rule(
                "build-before-merge",
                "在合并之前",
                "构建是不是绿的",
                "红了就停下来修",
            ),
            rule(
                "say-who-you-ask",
                "要给同事派活时",
                "对方是不是名册上的人",
                "用它的 agent id",
            ),
        ])
        .expect("two rules are a section");

        for needle in [
            "什么时候适用",
            "看什么",
            "做什么",
            "build-before-merge",
            "在合并之前",
            "构建是不是绿的",
            "红了就停下来修",
            "say-who-you-ask",
            "用它的 agent id",
        ] {
            assert!(text.contains(needle), "missing {needle:?}:\n{text}");
        }
        // 次序就是账给的次序：第一条第 1、第二条第 2。
        let first = text.find("build-before-merge").unwrap();
        let second = text.find("say-who-you-ask").unwrap();
        assert!(first < second, "the account's order is kept:\n{text}");
    }

    /// 一条生效的规则都没有时没有这一段：空标题会让模型以为规则被撤了。
    #[test]
    fn no_rules_means_no_section() {
        assert!(section(&[]).is_none());
    }
}
