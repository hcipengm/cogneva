//! Reading object literals and argument lists out of the JavaScript the two
//! shipped clients are written in.
//!
//! One reader rather than one per gate: the request-body gate asks what keys a
//! client sends, the chat-frame gate asks what fields it reads, and both cut
//! the same literals the same way. Two copies would drift, and a difference
//! between them would live exactly where neither gate looks.

#![allow(dead_code)]

use std::collections::BTreeSet;

/// A comma-separated list cut at the top level only: commas inside parentheses,
/// brackets, braces and string or template literals stay in their piece.
pub fn split_top_level(list: &str) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let chars: Vec<char> = list.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' || c == '\'' || c == '`' {
            let quote = c;
            current.push(c);
            i += 1;
            while i < chars.len() {
                current.push(chars[i]);
                if chars[i] == '\\' {
                    i += 1;
                    if i < chars.len() {
                        current.push(chars[i]);
                    }
                } else if chars[i] == quote {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(std::mem::take(&mut current));
                i += 1;
                continue;
            }
            _ => {}
        }
        current.push(c);
        i += 1;
    }
    parts.push(current);
    parts
}

/// The word inside a quoted piece, whatever the quotes are: `'POST'`, `"POST"`.
pub fn quoted_word(piece: &str) -> Option<String> {
    let piece = piece.trim();
    let quote = piece.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let end = piece[1..].find(quote)?;
    Some(piece[1..1 + end].to_string())
}

pub fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_alphabetic() || c == '_' || c == '$')
        && chars.all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

/// The top-level keys of an object literal body, or `None` when the object
/// cannot be enumerated: a spread (`...rest`) and a computed key (`[k]:`) both
/// stand for keys this reader never sees, and guessing the rest is how a ghost
/// key sails through.
pub fn top_level_keys(body: &str) -> Option<BTreeSet<String>> {
    let parts = split_top_level(body);
    let mut keys = BTreeSet::new();
    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if part.starts_with("...") || part.starts_with('[') {
            return None;
        }
        if let Some(rest) = part.strip_prefix('"').or_else(|| part.strip_prefix('\'')) {
            let quote = &part[..1];
            if let Some(end) = rest.find(quote) {
                let after = rest[end + 1..].trim_start();
                if after.starts_with(':') {
                    let key = &rest[..end];
                    if is_identifier(key) {
                        keys.insert(key.to_string());
                    }
                }
            }
            continue;
        }
        let name: String = part
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
            .collect();
        if name.is_empty() || !is_identifier(&name) {
            continue;
        }
        let after = part[name.len()..].trim_start();
        // A shorthand (`{ path }`) and a spelled pair (`{ path: p }`) name the
        // same key; a bare expression (`fn()` here) names none.
        if after.is_empty() || after.starts_with(':') {
            keys.insert(name);
        }
    }
    Some(keys)
}
