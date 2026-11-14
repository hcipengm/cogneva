//! 收件箱里的一条怎么变成收件人读到的一段话。
//!
//! 收件箱有两条投递路：常驻员工（[`crate::agent::Agent::handle_inbox`]）与对话
//! 员工（装配层每轮开跑前读一次收件箱）。两条路的收件人是同一个东西——一个只有
//! 这条消息、没有第二处能查发信人的智能体——所以「谁在说话」这件事必须由消息
//! 本身带进提示词，而且两条路必须说成同一句话：同一条消息在两处读起来不一样时，
//! 读的人分不清差别来自发信人还是来自他落在哪条路上。
//!
//! 渲染放在这里而不是放在各自的投递点上，是因为投递点有两处、而判据只有一条：
//! 谁改措辞都改这一处，两条路一起变。

use cog_core::InboxMessage;

/// 一条收件箱消息在这一轮历史里的样子；不是「要说的话」（撤停、重置）时没有。
///
/// 带发信人就写出是谁——收件人接下来要做的事（回一句、往下追、把话转给第三
/// 个人）全看这句话是谁说的。没留名（平台命令、加这一列之前写下的老消息）就
/// 只说「同事发来的」，不编一个名字：编出来的名字会让收件人以为可以回给一个
/// 不存在的人。
pub fn as_turn_text(msg: &InboxMessage) -> Option<String> {
    spoken_text(msg).map(|text| with_sender(msg.from_agent(), &text))
}

/// 这条消息说出来的话本身，不带出处；不是要说的话时没有。
///
/// 两条投递路拿到的字段不同（常驻员工那侧是拆开的 `goal` / `instruction`），
/// 所以出处那一行单独给：[`with_sender`] 接一个已知的话、一个已知的署名，
/// 投递点不必先把消息重新拼成一条再拆开。
pub fn spoken_text(msg: &InboxMessage) -> Option<String> {
    match msg {
        InboxMessage::Prompt { goal, .. } => Some(match goal {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        }),
        InboxMessage::Continue { instruction, .. } | InboxMessage::Steering { instruction, .. } => {
            Some(instruction.clone())
        }
        // 撤停与重置是给正在跑的那一轮的指令，不是一条要说的话。
        InboxMessage::Abort | InboxMessage::Reset => None,
    }
}

/// 一段话加上「这是谁说的」那一行。没留名就说没留名，不编一个名字。
pub fn with_sender(sender: Option<&str>, text: &str) -> String {
    match sender {
        Some(sender) => format!("[同事 {sender} 发来的消息]\n{text}"),
        None => format!("[没留名的同事发来的消息]\n{text}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(instruction: &str) -> InboxMessage {
        InboxMessage::Prompt {
            goal: serde_json::Value::String(instruction.into()),
            reply_stream: None,
            from_agent: Some("squad:task-1-reviewer".into()),
        }
    }

    #[test]
    fn a_message_that_names_its_sender_says_who_spoke() {
        assert_eq!(
            as_turn_text(&named("等下再合，构建还红着")).as_deref(),
            Some("[同事 squad:task-1-reviewer 发来的消息]\n等下再合，构建还红着")
        );
    }

    #[test]
    fn a_message_without_a_sender_does_not_invent_one() {
        let older: InboxMessage =
            serde_json::from_str(r#"{"type":"prompt","goal":"把首页配色改一下"}"#)
                .expect("老消息没有这一列，照旧解析");
        assert_eq!(
            as_turn_text(&older).as_deref(),
            Some("[没留名的同事发来的消息]\n把首页配色改一下")
        );
    }

    #[test]
    fn a_structured_goal_is_carried_as_it_was_written() {
        let msg = InboxMessage::Prompt {
            goal: serde_json::json!({ "step": 2 }),
            reply_stream: None,
            from_agent: None,
        };
        assert_eq!(
            as_turn_text(&msg).as_deref(),
            Some("[没留名的同事发来的消息]\n{\"step\":2}")
        );
    }

    #[test]
    fn commands_that_are_not_words_are_not_history() {
        assert_eq!(as_turn_text(&InboxMessage::Abort), None);
        assert_eq!(as_turn_text(&InboxMessage::Reset), None);
    }

    #[test]
    fn continue_and_steering_carry_the_sender_too() {
        for msg in [
            InboxMessage::Continue {
                instruction: "接着做".into(),
                from_agent: Some("mira".into()),
            },
            InboxMessage::Steering {
                instruction: "接着做".into(),
                from_agent: Some("mira".into()),
            },
        ] {
            assert_eq!(
                as_turn_text(&msg).as_deref(),
                Some("[同事 mira 发来的消息]\n接着做")
            );
        }
    }
}
