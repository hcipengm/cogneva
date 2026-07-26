//! PII / 敏感信息检测。

use cog_core::GuardResult;
use regex::Regex;

/// PII 检测器配置。
#[derive(Debug, Clone)]
pub struct PiiDetectorConfig {
    pub detect_email: bool,
    pub detect_phone: bool,
    pub detect_ssn: bool,
    pub detect_credit_card: bool,
    pub detect_api_key: bool,
    pub redact: bool, // true = 脱敏，false = 阻断
}

impl Default for PiiDetectorConfig {
    fn default() -> Self {
        Self {
            detect_email: true,
            detect_phone: true,
            detect_ssn: true,
            detect_credit_card: true,
            detect_api_key: true,
            redact: true,
        }
    }
}

/// PII 检测器。
pub struct PiiDetector {
    config: PiiDetectorConfig,
    email_re: Regex,
    phone_re: Regex,
    ssn_re: Regex,
    cc_re: Regex,
    api_key_re: Regex,
}

impl PiiDetector {
    pub fn new(config: PiiDetectorConfig) -> Self {
        Self {
            config,
            email_re: Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap(),
            phone_re: Regex::new(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b").unwrap(),
            ssn_re: Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap(),
            cc_re: Regex::new(r"\b(?:\d[ -]*?){13,16}\b").unwrap(),
            api_key_re: Regex::new(
                r#"(?i)(api[_-]?key|token|secret)\s*[:=]\s*['\"]?[a-zA-Z0-9_-]{16,}['\"]?""#,
            )
            .unwrap(),
        }
    }

    pub fn detect(&self, text: &str) -> Vec<(&'static str, String)> {
        let mut findings = vec![];
        if self.config.detect_email {
            for m in self.email_re.find_iter(text) {
                findings.push(("email", m.as_str().to_string()));
            }
        }
        if self.config.detect_phone {
            for m in self.phone_re.find_iter(text) {
                findings.push(("phone", m.as_str().to_string()));
            }
        }
        if self.config.detect_ssn {
            for m in self.ssn_re.find_iter(text) {
                findings.push(("ssn", m.as_str().to_string()));
            }
        }
        if self.config.detect_credit_card {
            for m in self.cc_re.find_iter(text) {
                findings.push(("credit_card", m.as_str().to_string()));
            }
        }
        if self.config.detect_api_key {
            for m in self.api_key_re.find_iter(text) {
                findings.push(("api_key", m.as_str().to_string()));
            }
        }
        findings
    }

    pub fn redact(&self, text: &str) -> String {
        let mut result = text.to_string();
        if self.config.detect_email {
            result = self
                .email_re
                .replace_all(&result, "[REDACTED_EMAIL]")
                .to_string();
        }
        if self.config.detect_phone {
            result = self
                .phone_re
                .replace_all(&result, "[REDACTED_PHONE]")
                .to_string();
        }
        if self.config.detect_ssn {
            result = self
                .ssn_re
                .replace_all(&result, "[REDACTED_SSN]")
                .to_string();
        }
        if self.config.detect_credit_card {
            result = self.cc_re.replace_all(&result, "[REDACTED_CC]").to_string();
        }
        if self.config.detect_api_key {
            result = self
                .api_key_re
                .replace_all(&result, "[REDACTED_API_KEY]")
                .to_string();
        }
        result
    }

    pub fn check(&self, text: &str) -> GuardResult {
        let findings = self.detect(text);
        if findings.is_empty() {
            return GuardResult::Pass;
        }
        let types: Vec<&str> = findings.iter().map(|(t, _)| *t).collect();
        let reason = format!("PII detected: {}", types.join(", "));
        if self.config.redact {
            GuardResult::Warn {
                reason,
                rule: "pii_detection".into(),
            }
        } else {
            GuardResult::Block {
                reason,
                rule: "pii_detection".into(),
            }
        }
    }
}

#[async_trait::async_trait]
impl cog_core::Guardrail for PiiDetector {
    async fn check_input(&self, messages: &[cog_core::Message]) -> GuardResult {
        let text: String = messages
            .iter()
            .map(|m| m.content())
            .collect::<Vec<_>>()
            .join("\n");
        self.check(&text)
    }

    async fn check_output(&self, response: &str) -> GuardResult {
        self.check(response)
    }

    async fn check_tool_call(&self, _tool: &cog_core::ToolCall) -> GuardResult {
        GuardResult::Pass
    }
}
