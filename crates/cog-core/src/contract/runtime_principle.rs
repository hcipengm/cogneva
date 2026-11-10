//! 运行时原则：这个社会写给自己的规则，一条一行，带状态与引用次数。
//!
//! 与工程侧的原则账不是同一本账。那本记的是仓库里每条经验有没有一条会让它变红
//! 的门，读者是改代码的人；这一本记的是平台上的员工干活时照着做的规则，读者是
//! 每一轮开头拿到基础指令的员工。两本混在一起，一边是「这条原则有没有门」，
//! 一边是「这条规则此刻生不生效」，同一个 slug 会在两处给出两个答案。
//!
//! 一条原则是三元组，不是一段提示词：**触发条件 → 判据 → 动作**。「什么情况下、
//! 看什么、做什么」三段齐了才是一条能被判的规则；只有一段的话，读的人不知道
//! 它在什么时候适用，也不知道它做完了该是什么样——那属于人格设定，不属于规则。
//!
//! 状态只有四个，转换是闭集：提案能变成生效或否决，生效的能被撤回，被否决的可以
//! 按新的文本重新提案。没有「草稿」「待复核」这类中间态：一个不为任何转换服务的
//! 状态，存在的唯一效果是让人问「它什么时候会走」。已生效的规则在它的替代者被
//! 批准之前一直生效，所以改一条原则是「先提新的、批了再撤旧的」，不是把旧的那行
//! 就地改掉——就地改会让一条没人批准过的句子立刻成为规则。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// 一条原则在账上的状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePrincipleState {
    /// 有人提了，属主还没拍板。不在生效集里。
    Proposed,
    /// 属主批准了。在生效集里，每一轮开头交给员工。
    Approved,
    /// 属主否决了。留在账上不是待办，是「这条提过、当时没同意」的记录。
    Rejected,
    /// 曾经生效、后来被撤回。同样留在账上，读得到它什么时候失效。
    Retired,
}

impl RuntimePrincipleState {
    /// 账上的拼法。存进表里、写在读面上、按它排序，都是这一个。
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimePrincipleState::Proposed => "proposed",
            RuntimePrincipleState::Approved => "approved",
            RuntimePrincipleState::Rejected => "rejected",
            RuntimePrincipleState::Retired => "retired",
        }
    }

    /// 从账上的拼法读回来。认不出的拼法不猜成任何一态。
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "proposed" => Some(RuntimePrincipleState::Proposed),
            "approved" => Some(RuntimePrincipleState::Approved),
            "rejected" => Some(RuntimePrincipleState::Rejected),
            "retired" => Some(RuntimePrincipleState::Retired),
            _ => None,
        }
    }

    /// 这一态是不是「此刻生效」。只有一处回答这个问题。
    pub fn in_force(self) -> bool {
        matches!(self, RuntimePrincipleState::Approved)
    }
}

/// 一条运行时原则的一行。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimePrinciple {
    /// 身份。提案人给的名字，全账唯一：同一句话被提两次会是两行，而一行被改两次
    /// 只有一段历史，读者分不出哪一版是谁批的。
    pub slug: String,
    /// 触发条件：什么情况下适用。
    pub trigger: String,
    /// 判据：看什么，看到什么算符合。
    pub criterion: String,
    /// 动作：符合与不符合各自做什么。
    pub action: String,
    pub state: RuntimePrincipleState,
    /// 谁提的：员工 id 或属主通道名。空着不成——一条没人提的规则不知该找谁问。
    pub proposed_by: String,
    pub proposed_at: DateTime<Utc>,
    /// 属主拍板的时刻。`Proposed` 时为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<DateTime<Utc>>,
    /// 被交给过多少轮。它是这条原则被真正用到的读数：一条生效但从未被引用的
    /// 原则，和一条被引用上千次的原则，在「还该不该留」这个问题上不是同一件事。
    pub used: u64,
}

impl RuntimePrinciple {
    /// 提案。三段中任何一段是空的都不是规则，这里拒绝而不是存一条读起来像
    /// 规则的空句——空句在生效集里会被每一轮读成「这一条今天没有内容」。
    pub fn proposed(
        slug: impl Into<String>,
        trigger: impl Into<String>,
        criterion: impl Into<String>,
        action: impl Into<String>,
        proposed_by: impl Into<String>,
        at: DateTime<Utc>,
    ) -> SFResult<Self> {
        let slug = slug.into().trim().to_string();
        if slug.is_empty() {
            return Err(crate::SFError::Validation(
                "a runtime principle needs a slug: it is the row's identity".into(),
            ));
        }
        let part = |name: &str, text: String| -> SFResult<String> {
            let text = text.trim().to_string();
            if text.is_empty() {
                return Err(crate::SFError::Validation(format!(
                    "a runtime principle needs its {name}: a triple with an empty part \
                     cannot be judged"
                )));
            }
            Ok(text)
        };
        let proposed_by = proposed_by.into().trim().to_string();
        if proposed_by.is_empty() {
            return Err(crate::SFError::Validation(
                "a runtime principle names who proposed it".into(),
            ));
        }
        Ok(Self {
            slug,
            trigger: part("trigger", trigger.into())?,
            criterion: part("criterion", criterion.into())?,
            action: part("action", action.into())?,
            state: RuntimePrincipleState::Proposed,
            proposed_by,
            proposed_at: at,
            decided_at: None,
            used: 0,
        })
    }

    /// 这一行此刻是不是生效的。
    pub fn in_force(&self) -> bool {
        self.state.in_force()
    }
}

/// 运行时的原则账。
///
/// 写入只有两个动作：提案、拍板。没有「修改一行」——改一条原则是提一条新的、
/// 批了再撤旧的（见模块文档），所以账上每一行的文本都是某个时刻被某个人批准过
/// 的那一版，不会出现「同一行被改过但没人知道改前是什么」。
#[async_trait]
pub trait RuntimePrincipleStore: Send + Sync {
    /// 记一条提案。
    ///
    /// 同一个 slug 已经在账上、且不是被否决的那一行时拒绝：生效中的规则不接受
    /// 就地改写（见 [`RuntimePrinciple::proposed`] 的模块文档），而一条还在等
    /// 拍板的提案被静默覆盖，等于两个人的提案有一个人白写了。被否决的行可以按
    /// 新文本重新提案，它回到「提案」态等下一次拍板。
    async fn propose(&self, principle: &RuntimePrinciple) -> SFResult<()>;

    /// 属主拍板：`Approved` 或 `Rejected` 拍一条提案，`Retired` 撤一条生效的。
    ///
    /// 答「这一行有没有动」。同一件事再拍一次不是错误，但也不是又发生了一次，
    /// 所以第二个返回值必须分得开：重复调用读成「又通过一次」，通过时刻就会被
    /// 一路往后推，而那条原则是哪天开始生效的再也读不出来。
    async fn decide(
        &self,
        slug: &str,
        decision: RuntimePrincipleState,
        at: DateTime<Utc>,
    ) -> SFResult<bool>;

    /// 此刻生效的那一集，按通过时刻、slug 排。
    ///
    /// 排序是给读的人一份稳定的次序：同一批原则每轮换一种排法，员工会以为
    /// 规则变了。
    async fn in_force(&self) -> SFResult<Vec<RuntimePrinciple>>;

    /// 一次运行拿走了哪几条：引用次数加一。
    ///
    /// 不在账上的 slug 静默跳过：这一轮已经按读到的那一集跑完了，为一个计数
    /// 让整轮失败是拿读数换产出。
    async fn note_used(&self, slugs: &[String]) -> SFResult<()>;

    /// 全部行，可按状态筛，给读面用。
    async fn list(&self, state: Option<RuntimePrincipleState>) -> SFResult<Vec<RuntimePrinciple>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-11T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    /// 三段齐了才是规则，身份与提案人也得在。
    #[test]
    fn a_rule_needs_all_three_parts_and_a_name() {
        let row = RuntimePrinciple::proposed(
            "build-before-merge",
            " 在合并之前 ",
            " 构建是不是绿的 ",
            " 红了就停下来修，不要先合再修 ",
            "E0412",
            at(),
        )
        .expect("a full triple is a rule");
        assert_eq!(row.trigger, "在合并之前");
        assert_eq!(row.state, RuntimePrincipleState::Proposed);
        assert!(!row.in_force(), "提了不等于是规则");

        for (trigger, criterion, action) in [
            ("", "判据", "动作"),
            ("触发", "  ", "动作"),
            ("触发", "判据", ""),
        ] {
            assert!(
                RuntimePrinciple::proposed("slug", trigger, criterion, action, "E0412", at())
                    .is_err(),
                "an empty part cannot be judged: {trigger:?} {criterion:?} {action:?}"
            );
        }
        assert!(RuntimePrinciple::proposed(" ", "t", "c", "a", "E0412", at()).is_err());
        assert!(RuntimePrinciple::proposed("slug", "t", "c", "a", "  ", at()).is_err());
    }

    /// 状态的拼法是账上的字节，多一个写法就多一态。
    #[test]
    fn the_state_spellings_round_trip() {
        for state in [
            RuntimePrincipleState::Proposed,
            RuntimePrincipleState::Approved,
            RuntimePrincipleState::Rejected,
            RuntimePrincipleState::Retired,
        ] {
            assert_eq!(RuntimePrincipleState::parse(state.as_str()), Some(state));
        }
        assert_eq!(RuntimePrincipleState::parse("draft"), None);
        assert!(RuntimePrincipleState::Approved.in_force());
        for other in [
            RuntimePrincipleState::Proposed,
            RuntimePrincipleState::Rejected,
            RuntimePrincipleState::Retired,
        ] {
            assert!(!other.in_force(), "{other:?} is not in force");
        }
    }
}
