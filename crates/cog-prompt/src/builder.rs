//! 统一 PromptBuilder（guardrail 决策 阶段二 / 审计 3.9）。
//!
//! 所有消息经统一入口封装：
//! - 系统规则始终位于 messages 最前，并自动附加不可覆盖的
//!   [`cog_core::NON_OVERRIDABLE_META_INSTRUCTION`]；
//! - 不可信外部材料（RAG 检索、网页、文件、工具返回）统一包装为
//!   `<external-data source="...">` 标记，与系统指令不平级；
//! - `build_validated` 用 [`cog_core::validate_prompt_structure`]
//!   校验最终 prompt 结构完整性。

use cog_core::{wrap_external_data, Message, NON_OVERRIDABLE_META_INSTRUCTION};

/// 统一 prompt 构建入口。
#[derive(Debug, Default)]
pub struct PromptBuilder {
    system_rules: Vec<String>,
    messages: Vec<Message>,
}

impl PromptBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// 追加一条系统规则；build 时合并为单个最前的 system 消息，
    /// 并附加不可覆盖的 meta-instruction。
    pub fn system(mut self, rule: impl Into<String>) -> Self {
        self.system_rules.push(rule.into());
        self
    }

    pub fn user(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message::user(content));
        self
    }

    pub fn assistant_text(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message::assistant_text(content));
        self
    }

    /// 追加一条任意角色的原始消息。注意：system 消息请用
    /// [`PromptBuilder::system`]，否则 `build_validated` 会报
    /// `SystemNotFirst` 结构违规。
    pub fn message(mut self, message: Message) -> Self {
        self.messages.push(message);
        self
    }

    /// 通用外部材料入口：包装为 `<external-data>` 用户消息。
    pub fn external(mut self, content: &str, source: impl Into<String>) -> Self {
        self.messages
            .push(Message::user(wrap_external_data(content, &source.into())));
        self
    }

    /// RAG 检索结果，source 例：`rag:knowledge_base`。
    pub fn rag(self, content: &str, source: impl Into<String>) -> Self {
        self.external(content, source)
    }

    /// 网页内容，source 为 URL。
    pub fn web(self, content: &str, url: impl Into<String>) -> Self {
        self.external(content, url)
    }

    /// 文件内容，source 为文件路径。
    pub fn file(self, content: &str, path: impl Into<String>) -> Self {
        self.external(content, path)
    }

    /// 工具返回，source 自动标记为 `tool:<name>`。
    pub fn tool_result(self, content: &str, tool_name: &str) -> Self {
        self.external(content, format!("tool:{tool_name}"))
    }

    /// 构建消息列表：system 块（规则 + meta-instruction）在最前，
    /// 其余消息按追加顺序排列。
    pub fn build(self) -> Vec<Message> {
        let mut out = Vec::with_capacity(self.messages.len() + 1);
        if !self.system_rules.is_empty() {
            out.push(Message::system(format!(
                "{}\n\n{}",
                self.system_rules.join("\n"),
                NON_OVERRIDABLE_META_INSTRUCTION
            )));
        }
        out.extend(self.messages);
        out
    }

    /// 构建并校验结构完整性；存在违规时返回错误描述列表。
    pub fn build_validated(self) -> Result<Vec<Message>, Vec<String>> {
        let messages = self.build();
        let violations = cog_core::validate_prompt_structure(&messages);
        if violations.is_empty() {
            Ok(messages)
        } else {
            Err(violations.iter().map(|v| format!("{v:?}")).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::PromptStructureViolation;

    #[test]
    fn system_rules_come_first_with_meta_instruction() {
        let msgs = PromptBuilder::new()
            .system("Rule A")
            .system("Rule B")
            .user("hello")
            .build();
        assert_eq!(msgs[0].role(), "system");
        assert!(msgs[0].content().contains("Rule A"));
        assert!(msgs[0].content().contains("Rule B"));
        assert!(msgs[0].content().contains(NON_OVERRIDABLE_META_INSTRUCTION));
        assert_eq!(msgs[1].role(), "user");
    }

    #[test]
    fn external_sources_are_wrapped() {
        let msgs = PromptBuilder::new()
            .system("rules")
            .rag("kb hit", "rag:kb")
            .web("page", "https://example.com")
            .file("content", "/tmp/a.txt")
            .tool_result("tool out", "search")
            .build();
        assert!(msgs[1].content().contains("source=\"rag:kb\""));
        assert!(msgs[2].content().contains("source=\"https://example.com\""));
        assert!(msgs[3].content().contains("source=\"/tmp/a.txt\""));
        assert!(msgs[4].content().contains("source=\"tool:search\""));
        for m in &msgs[1..] {
            assert!(cog_core::validate_external_markers(&m.content()).is_ok());
        }
    }

    #[test]
    fn builder_output_passes_structure_validation() {
        let msgs = PromptBuilder::new()
            .system("rules")
            .user("q")
            .web("page", "https://x")
            .build_validated()
            .expect("valid structure");
        assert!(cog_core::validate_prompt_structure(&msgs).is_empty());
    }

    #[test]
    fn raw_system_message_after_user_is_flagged() {
        let result = PromptBuilder::new()
            .user("hi")
            .message(Message::system("late rules"))
            .build_validated();
        let errs = result.expect_err("must flag ordering violation");
        assert!(errs
            .iter()
            .any(|e| e.contains(&format!("{:?}", PromptStructureViolation::SystemNotFirst))));
    }

    #[test]
    fn no_system_rules_means_no_system_message() {
        let msgs = PromptBuilder::new().user("hi").build();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].role(), "user");
    }
}
