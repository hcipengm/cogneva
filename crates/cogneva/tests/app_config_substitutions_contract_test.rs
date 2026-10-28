//! principle: feedback_no_hardcoding_config_surface
//! principle: feedback_a_whole_tree_census_reads_shapes_so_new_artifacts_must_declare_themselves
//! `deploy/helm/cogneva/files/cogneva.json` is the document the app, the evolution
//! worker and the sandbox executor actually run from: it is mounted at
//! `/etc/cogneva`, and the environment layer only overrides the keys somebody
//! bothered to map. The chart therefore does not render it -- it substitutes
//! literals in it, one row per address, in `templates/app-config.yaml`.
//!
//! A row whose pattern stops matching fails the render loudly. The other
//! direction is silent, and it is the one this test exists for: an address
//! written into the JSON that no row names is rendered as-is, and the deployment
//! then runs from a hardcoded cluster fact. Nothing errors -- a pod pointed at a
//! Loki nobody updates simply stops shipping logs, and a log pipeline with one
//! fewer process in it reads exactly like a quiet week.
//!
//! So the census reads both sides: every `host:port` in the JSON has to be
//! covered by a substitution row, and the row may carry the `%s`/`%d` holes the
//! namespace- and values-shaped patterns need. Coverage is the only judgement
//! here -- what the address should be is the row's business, not this test's.

mod common;

use common::routes::read_repo;

use std::collections::BTreeSet;

/// The document whose addresses must all be deployment-owned.
const JSON: &str = "deploy/helm/cogneva/files/cogneva.json";

/// The template holding the substitution table.
const TEMPLATE: &str = "deploy/helm/cogneva/templates/app-config.yaml";

/// The line that opens the table, and the line that closes it.
const TABLE_OPEN: &str = "$portage := dict";
const TABLE_CLOSE: &str = "-}}";

/// One piece of a substitution pattern: a literal, or a hole a deployment fills.
#[derive(Debug, PartialEq, Eq)]
enum Token {
    Literal(String),
    /// `%s` -- a namespace or a whole address.
    Free,
    /// `%d` -- a port.
    Digits,
}

/// The Go string literals of the substitution table, unescaped.
///
/// Only the region between the table's opening and closing lines is read, so a
/// comment outside it cannot contribute a pattern that covers nothing.
fn table_literals(template: &str) -> Vec<String> {
    let mut region = String::new();
    let mut inside = false;
    for line in template.lines() {
        if line.contains(TABLE_OPEN) {
            inside = true;
        }
        if inside {
            region.push_str(line);
            region.push('\n');
            if line.trim_end() == TABLE_CLOSE {
                break;
            }
        }
    }
    assert!(
        !region.is_empty(),
        "{TEMPLATE} no longer opens the substitution table with `{TABLE_OPEN}`, so this \
         test cannot see which addresses the chart rewrites -- update the marker here \
         before the table's shape changes underneath it"
    );

    let chars: Vec<char> = region.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '"' {
            i += 1;
            continue;
        }
        i += 1;
        let mut literal = String::new();
        while i < chars.len() && chars[i] != '"' {
            if chars[i] == '\\' && i + 1 < chars.len() {
                // The table writes JSON fragments, so `\"` is how a quote inside a
                // pattern is spelled; anything else escapes to itself.
                i += 1;
            }
            literal.push(chars[i]);
            i += 1;
        }
        i += 1;
        out.push(literal);
    }
    out
}

/// A pattern as tokens: `%s` and `%d` are holes, everything else is literal.
fn tokenise(pattern: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut literal = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.peek() {
                Some('s') => {
                    chars.next();
                    tokens.push(Token::Literal(std::mem::take(&mut literal)));
                    tokens.push(Token::Free);
                }
                Some('d') => {
                    chars.next();
                    tokens.push(Token::Literal(std::mem::take(&mut literal)));
                    tokens.push(Token::Digits);
                }
                _ => literal.push(c),
            }
            continue;
        }
        literal.push(c);
    }
    tokens.push(Token::Literal(literal));
    tokens
}

/// Whether `pattern` matches `address`, holes consuming at least one character.
fn covers(pattern: &str, address: &str) -> bool {
    match_from(&tokenise(pattern), &address.chars().collect::<Vec<_>>(), 0)
}

/// Matches the remaining tokens against `chars` from `at`.
///
/// Holes try every length they are allowed to have, shortest first: `%s` in
/// `postgres.%s.svc.cluster.local` must be free to stop at the dot before
/// `svc`, which a greedy scan would eat and then fail on the literal behind it.
fn match_from(tokens: &[Token], chars: &[char], at: usize) -> bool {
    let Some((token, rest)) = tokens.split_first() else {
        return at == chars.len();
    };
    match token {
        Token::Literal(text) => {
            let literal: Vec<char> = text.chars().collect();
            chars.len() >= at + literal.len()
                && chars[at..at + literal.len()] == literal[..]
                && match_from(rest, chars, at + literal.len())
        }
        Token::Free => {
            let mut end = at;
            while end < chars.len()
                && (chars[end].is_ascii_alphanumeric()
                    || chars[end] == '.'
                    || chars[end] == '-'
                    || chars[end] == '_')
            {
                end += 1;
                if match_from(rest, chars, end) {
                    return true;
                }
            }
            false
        }
        Token::Digits => {
            let mut end = at;
            while end < chars.len() && chars[end].is_ascii_digit() {
                end += 1;
                if match_from(rest, chars, end) {
                    return true;
                }
            }
            false
        }
    }
}

/// Whether `host` is a date prefix rather than a service name: `2026-10-04T23:41`
/// is a timestamp in a comment, and its `:41` is not a port.
fn is_timestamp_host(host: &str) -> bool {
    let bytes = host.as_bytes();
    bytes.len() >= 11
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[..4].iter().all(u8::is_ascii_digit)
}

/// Every `host:port` in `text`, as written.
fn addresses(text: &str) -> BTreeSet<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = BTreeSet::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != ':' {
            i += 1;
            continue;
        }
        let host_start = {
            let mut start = i;
            while start > 0
                && (chars[start - 1].is_ascii_alphanumeric()
                    || chars[start - 1] == '.'
                    || chars[start - 1] == '-'
                    || chars[start - 1] == '_')
            {
                start -= 1;
            }
            start
        };
        let mut end = i + 1;
        while end < chars.len() && chars[end].is_ascii_digit() {
            end += 1;
        }
        let port_len = end - (i + 1);
        let port_boundary = chars.get(end).is_none_or(|c| !c.is_ascii_alphanumeric());
        if (2..=5).contains(&port_len) && port_boundary && host_start < i {
            let host: String = chars[host_start..i].iter().collect();
            if host.chars().any(|c| c.is_ascii_alphabetic()) && !is_timestamp_host(&host) {
                out.insert(chars[host_start..end].iter().collect());
            }
            i = end;
            continue;
        }
        i += 1;
    }
    out
}

#[test]
fn every_address_in_the_app_config_is_rewritten_by_the_chart() {
    let template = read_repo(TEMPLATE);
    let patterns = table_literals(&template);
    let found = addresses(&read_repo(JSON));

    assert!(
        !found.is_empty(),
        "{JSON} no longer holds a single host:port, so this test reads nothing -- \
         either the addresses moved (point this test at where they went) or the \
         scanner stopped matching"
    );
    assert!(
        patterns.iter().any(|p| p.contains(':')),
        "{TEMPLATE} has no address-shaped row in its substitution table, so nothing \
         in the JSON can be covered"
    );

    let mut uncovered = Vec::new();
    for address in &found {
        // A row may name the address on its own (`nats-service:4222`) or inside a
        // longer literal (`http://loki.monitoring.svc.cluster.local:3100`): both
        // are replacements of the same fact, and the file keeps only the address.
        let matched = patterns
            .iter()
            .any(|pattern| pattern.contains(address.as_str()) || covers(pattern, address));
        if !matched {
            uncovered.push(address.clone());
        }
    }
    assert!(
        uncovered.is_empty(),
        "these addresses are written into {JSON} and no row in {TEMPLATE}'s table \
         rewrites them, so every deployment runs from whatever the file happens to \
         say: {uncovered:#?}\n\
         add a row reading the values key the rest of the chart uses, or -- if the \
         address is genuinely not a deployment fact -- say so where the row would be"
    );
}

#[test]
fn the_pattern_matcher_reads_holes_and_literals() {
    // A `%d` covers any port, a `%s` any host-shaped word, and neither may match
    // nothing -- an empty hole would let two unrelated addresses collide.
    assert!(covers(
        "postgres.%s.svc.cluster.local:5432",
        "postgres.cogneva.svc.cluster.local:5432"
    ));
    assert!(covers(
        "postgres.%s.svc.cluster.local:%d",
        "postgres.other.svc.cluster.local:5433"
    ));
    assert!(!covers(
        "postgres.%s.svc.cluster.local:5432",
        "postgres.cogneva.svc.cluster.local:5433"
    ));
    assert!(!covers("nats-service:%d", "nats-service:"));
    assert!(!covers("nats-service:%d", "nats-service-x:4222"));

    let found = addresses(
        "url: http://loki.monitoring.svc.cluster.local:3100/x
         seen: 2026-10-04T23:41 in a comment
         clock: 03:17
         bare: cogneva-security-gateway:8081",
    );
    assert_eq!(
        found,
        BTreeSet::from([
            "cogneva-security-gateway:8081".to_string(),
            "loki.monitoring.svc.cluster.local:3100".to_string(),
        ]),
        "a timestamp's `:41` and a bare clock's `:17` are not ports, and a URL's \
         host is the token before the colon"
    );
}
