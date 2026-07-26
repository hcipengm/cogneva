/// Parse partial/incomplete JSON from a streaming response.
/// Best-effort: returns parsed Value if possible, otherwise returns the raw string as Value::String.
/// This is useful for streaming tool call arguments where the JSON arrives in chunks
/// and may be incomplete at any given point.
pub fn parse_streaming_json(input: &str) -> serde_json::Value {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return serde_json::Value::String(input.to_string());
    }

    // Try full parse first
    if let Ok(value) = serde_json::from_str(trimmed) {
        return value;
    }

    // Try common completion strategies for incomplete JSON
    let completions = build_completions(trimmed);
    for candidate in completions {
        if let Ok(value) = serde_json::from_str(&candidate) {
            return value;
        }
    }

    // Try to extract the largest valid prefix as a value
    if let Some(value) = try_extract_prefix(trimmed) {
        return value;
    }

    // Fall back to raw string
    serde_json::Value::String(input.to_string())
}

/// Build candidate completions by appending common closing tokens.
fn build_completions(input: &str) -> Vec<String> {
    let mut candidates = Vec::new();

    // Track whether we're inside a string
    let mut in_string = false;
    let mut escaped = false;
    let mut brace_depth = 0i32;
    let mut bracket_depth = 0i32;

    for ch in input.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
        } else {
            match ch {
                '"' => in_string = true,
                '{' => brace_depth += 1,
                '}' => brace_depth -= 1,
                '[' => bracket_depth += 1,
                ']' => bracket_depth -= 1,
                _ => {}
            }
        }
    }

    // If we're inside a string, try closing it
    if in_string {
        let closed_string = format!("{}\"", input);
        candidates.push(closed_string.clone());

        // Closing the string doesn't change brace/bracket counts
        let suffix = build_suffix(brace_depth, bracket_depth);
        if !suffix.is_empty() {
            candidates.push(format!("{}{}", closed_string, suffix));
        }
    } else {
        let suffix = build_suffix(brace_depth, bracket_depth);
        if !suffix.is_empty() {
            candidates.push(format!("{}{}", input, suffix));
        }
    }

    candidates
}

fn build_suffix(brace_depth: i32, bracket_depth: i32) -> String {
    let mut suffix = String::new();
    for _ in 0..bracket_depth.max(0) {
        suffix.push(']');
    }
    for _ in 0..brace_depth.max(0) {
        suffix.push('}');
    }
    suffix
}

/// Try to extract the largest valid prefix from the input.
/// This handles cases where trailing garbage prevents parsing.
fn try_extract_prefix(input: &str) -> Option<serde_json::Value> {
    // Try progressively shorter prefixes
    let mut end = input.len();
    while end > 0 {
        let prefix = &input[..end];
        if let Ok(value) = serde_json::from_str(prefix) {
            return Some(value);
        }
        end -= 1;
        // Skip trailing whitespace quickly
        while end > 0 && input.as_bytes()[end - 1].is_ascii_whitespace() {
            end -= 1;
        }
    }
    None
}

/// Parse a JSON string that may be split across multiple chunks.
/// Accumulates chunks and returns the parsed value once complete,
/// or the best-effort partial parse if `is_final` is true.
pub struct StreamingJsonParser {
    buffer: String,
}

impl StreamingJsonParser {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    /// Append a new chunk to the buffer.
    pub fn push(&mut self, chunk: &str) {
        self.buffer.push_str(chunk);
    }

    /// Try to parse the accumulated buffer.
    pub fn try_parse(&self) -> Option<serde_json::Value> {
        let trimmed = self.buffer.trim();
        if trimmed.is_empty() {
            return None;
        }
        serde_json::from_str(trimmed).ok()
    }

    /// Force a best-effort parse of whatever we have.
    pub fn finalize(&self) -> serde_json::Value {
        parse_streaming_json(&self.buffer)
    }

    /// Return the accumulated buffer content.
    pub fn buffer(&self) -> &str {
        &self.buffer
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for StreamingJsonParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_complete_json() {
        assert_eq!(
            parse_streaming_json(r#"{"key": "value"}"#),
            serde_json::json!({"key": "value"})
        );
    }

    #[test]
    fn test_parse_incomplete_object() {
        let result = parse_streaming_json(r#"{"key": "value""#);
        assert_eq!(result, serde_json::json!({"key": "value"}));
    }

    #[test]
    fn test_parse_incomplete_array() {
        let result = parse_streaming_json(r#"[1, 2, 3"#);
        assert_eq!(result, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_parse_incomplete_string_value() {
        let result = parse_streaming_json(r#"{"name": "test""#);
        assert_eq!(result, serde_json::json!({"name": "test"}));
    }

    #[test]
    fn test_parse_nested_incomplete() {
        let result = parse_streaming_json(r#"{"outer": {"inner": 1"#);
        assert_eq!(result, serde_json::json!({"outer": {"inner": 1}}));
    }

    #[test]
    fn test_parse_unrecoverable_returns_string() {
        let result = parse_streaming_json("not json at all");
        assert_eq!(result, serde_json::Value::String("not json at all".into()));
    }

    #[test]
    fn test_streaming_parser() {
        let mut parser = StreamingJsonParser::new();
        parser.push("{\"a\": ");
        assert!(parser.try_parse().is_none());

        parser.push("1}");
        assert_eq!(parser.try_parse(), Some(serde_json::json!({"a": 1})));
    }

    #[test]
    fn test_streaming_parser_finalizes() {
        let mut parser = StreamingJsonParser::new();
        parser.push("{\"partial\": \"val");
        assert!(parser.try_parse().is_none());

        let result = parser.finalize();
        assert_eq!(result, serde_json::json!({"partial": "val"}));
    }
}
