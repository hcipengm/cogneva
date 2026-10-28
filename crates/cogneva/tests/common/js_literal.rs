//! Reading object literals and argument lists out of the JavaScript the two
//! shipped clients are written in.
//!
//! One reader rather than one per gate: the request-body gate asks what keys a
//! client sends, the chat-frame gate asks what fields it reads, and both cut
//! the same literals the same way. Two copies would drift, and a difference
//! between them would live exactly where neither gate looks.
//!
//! Cutting a list at its top-level commas is not here: where a string starts
//! and stops is the one dialect table in [`super::lex`], and a reader that
//! decided that for itself would be a second opinion nobody compares.

#![allow(dead_code)]

use std::collections::BTreeSet;

/// The word inside a quoted piece, whatever the quotes are: `'POST'`, `"POST"`,
/// and the template literal `` `POST` ``, which is a string in JavaScript too and
/// was the one form a second reader elsewhere accepted when this one did not.
pub fn quoted_word(piece: &str) -> Option<String> {
    let piece = piece.trim();
    let quote = piece.chars().next()?;
    if quote != '"' && quote != '\'' && quote != '`' {
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

/// Which words a caller is willing to hear as a key name.
///
/// The two vocabularies are not the same vocabulary, and that is the whole
/// point of saying so at the call site: a struct-shaped reader compares keys
/// against field names, and `security-gateway` cannot be one; a map keyed by
/// component ids is exactly where those dashed words live. Written as a flag
/// rather than as two readers, because two readers of the same object literal
/// disagreeing on one key is not a visible difference — each gate reads its own
/// answer and both stay green.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum KeyWords {
    /// Only what a JavaScript identifier can spell.
    Identifiers,
    /// Any word the literal quotes, dashed or not.
    Any,
}

/// The top-level keys of an object literal body, or `None` when the object
/// cannot be enumerated: a spread (`...rest`), a computed key (`[k]:`) and an
/// unterminated quote all stand for keys this reader never sees, and guessing
/// the rest is how a ghost key sails through.
pub fn top_level_keys(body: &str, words: KeyWords) -> Option<BTreeSet<String>> {
    // 切法不在这份文件里：认引号的那台状态机在 `lex`，「哪里是一段字符串」与别处读
    // 同一段 JavaScript 的读者是同一个答案。
    let parts = super::lex::split_top_level(body, super::lex::Dialect::JavaScript);
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
            // 收不了尾的引号：这个对象里有这个读取器看不见的键，整条不判，而不是
            // 把剩下那些当成全部。
            let end = rest.find(quote)?;
            if rest[end + 1..].trim_start().starts_with(':')
                && (words == KeyWords::Any || is_identifier(&rest[..end]))
            {
                keys.insert(rest[..end].to_string());
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
