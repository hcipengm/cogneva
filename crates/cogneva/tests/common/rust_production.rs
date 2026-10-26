//! The production half of a source file, as both censuses and the deployer
//! gates in this directory read it.
//!
//! Every one of them has to answer the same question -- "is this line shipped
//! code or a fixture?" -- and every one of them used to answer it with
//! "anything before the first `#[cfg(test)]`". That answer is wrong wherever a
//! file carries a gated item before its production tail, and it fails in the
//! direction a gate cannot afford: unread lines and clean lines look identical.
//!
//! Shared rather than copied so the rule has one shape. A gate that stops
//! reading is a bug in the gate, and a bug fixed in one copy stays in the rest.

#![allow(dead_code)]

/// Whether this line opens a test-gated item: `#[cfg(test)]` or an inline
/// `mod tests {`. The item that follows is a specimen, not a deployment fact.
pub fn opens_test_module(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("#[cfg(test)]")
        || trimmed.starts_with("mod tests")
        || trimmed.starts_with("pub mod tests")
}

/// One line's brace shape: how the nesting changes, and whether a `{` opened.
#[derive(Clone, Copy, Default)]
pub struct BraceShape {
    pub delta: i32,
    pub opens: bool,
}

/// Per-line brace shapes for a whole file, carrying string, character-literal
/// and comment state across the newlines.
///
/// Counting each line on its own is not enough. A fixture string spans lines --
/// `"suggestions":[]}"#,` closes a raw string opened four lines above -- and the
/// `}` inside that text would end a skipped item early, after which the rest of
/// the test module reads as production. Over-reading is noisy; under-reading is
/// silent, and only one of those is a gate.
///
/// Three things that look like nesting and are not: braces inside string or
/// character literals, and a `'` that is a lifetime (`&'a str`) rather than the
/// opening of `'{'`. A character literal's closing quote is the next quote
/// within a few bytes; a lifetime's is not.
pub fn brace_shapes(text: &str) -> Vec<BraceShape> {
    let b = text.as_bytes();
    let mut out: Vec<BraceShape> = Vec::new();
    let mut shape = BraceShape::default();
    let mut i = 0;
    let mut block_comment = 0usize;
    let mut string = false;
    let mut raw_hashes: Option<usize> = None;
    while i < b.len() {
        let c = b[i];
        if c == b'\n' {
            out.push(shape);
            shape = BraceShape::default();
            i += 1;
            continue;
        }
        if block_comment > 0 {
            if c == b'/' && b.get(i + 1) == Some(&b'*') {
                block_comment += 1;
                i += 2;
                continue;
            }
            if c == b'*' && b.get(i + 1) == Some(&b'/') {
                block_comment -= 1;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if let Some(hashes) = raw_hashes {
            if c == b'"' {
                let mut j = i + 1;
                let mut seen = 0;
                while seen < hashes && b.get(j) == Some(&b'#') {
                    seen += 1;
                    j += 1;
                }
                if seen == hashes {
                    raw_hashes = None;
                    i = j;
                    continue;
                }
            }
            i += 1;
            continue;
        }
        if string {
            if c == b'\\' {
                // A backslash that ends the line continues the string: let the
                // newline below emit the line boundary, or every later line's
                // shape is reported one line early.
                if b.get(i + 1) == Some(&b'\n') || b.get(i + 2) == Some(&b'\n') {
                    i += 1;
                } else {
                    i += 2;
                }
                continue;
            }
            if c == b'"' {
                string = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                block_comment = 1;
                i += 2;
                continue;
            }
            b'"' => {
                string = true;
                i += 1;
                continue;
            }
            b'\'' => {
                let mut j = i + 1;
                if b.get(j) == Some(&b'\\') {
                    j += 1;
                }
                let mut close = None;
                while j < b.len() && j <= i + 5 {
                    if b[j] == b'\'' {
                        close = Some(j);
                        break;
                    }
                    j += 1;
                }
                i = match close {
                    Some(at) => at + 1,
                    None => i + 1,
                };
                continue;
            }
            b'r' | b'b' | b'c' => {
                let mut j = i + 1;
                if b.get(j) == Some(&b'"') {
                    string = true;
                    i = j + 1;
                    continue;
                }
                if c == b'b' || c == b'c' {
                    if b.get(j) != Some(&b'r') {
                        i += 1;
                        continue;
                    }
                    j += 1;
                }
                let mut hashes = 0;
                while b.get(j) == Some(&b'#') {
                    hashes += 1;
                    j += 1;
                }
                if b.get(j) == Some(&b'"') {
                    raw_hashes = Some(hashes);
                    i = j + 1;
                    continue;
                }
                i += 1;
                continue;
            }
            b'{' => {
                shape.delta += 1;
                shape.opens = true;
            }
            b'}' => shape.delta -= 1,
            _ => {}
        }
        i += 1;
    }
    out.push(shape);
    out
}

/// One line's brace delta, for the scanner's own tests.
pub fn brace_delta(line: &str) -> i32 {
    brace_shapes(line)
        .first()
        .copied()
        .unwrap_or_default()
        .delta
}

/// The production half of one source file, in order.
///
/// The boundary is per item, not "everything after the first test module":
/// `mainline_deployer.rs` carries a `#[cfg(test)] fn` about a third of the way
/// in and roughly two thousand lines of production after it. Cutting at the
/// first marker leaves that tail unread while the census still reads clean --
/// which is the one failure a gate cannot have, because "I never looked there"
/// and "I looked and found nothing" are the same reading.
pub fn production_lines(text: &str) -> Vec<&str> {
    let lines: Vec<&str> = text.lines().collect();
    let shapes = brace_shapes(text);
    let mut out = Vec::new();
    let mut gated = false;
    let mut skipping: Option<(i32, bool)> = None;
    let mut waiting = 0usize;
    for (index, line) in lines.iter().copied().enumerate() {
        let shape = shapes.get(index).copied().unwrap_or_default();
        let trimmed = line.trim_start();
        if skipping.is_none() {
            if opens_test_module(line) {
                // `#[cfg(test)]` decorates the next item; `mod tests {` *is* the
                // item and opens a body on this very line.
                gated = !shape.opens;
                if shape.opens {
                    skipping = Some((shape.delta, true));
                }
                waiting = 0;
                continue;
            }
            if gated {
                // Blanks, comments and further attributes still belong to the
                // item the gate decorates.
                if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[") {
                    continue;
                }
                gated = false;
                skipping = Some((0, false));
                waiting = 0;
            } else {
                if trimmed.starts_with("//") {
                    continue;
                }
                out.push(line);
                continue;
            }
        }
        // Inside a skipped item, or starting one on this line.
        let (depth, opened) = skipping.take().unwrap_or((0, false));
        let next = depth + shape.delta;
        let opened = opened || shape.opens;
        let ended = (opened && next <= 0) || (!opened && trimmed.ends_with(';'));
        if ended {
            waiting = 0;
        } else if !opened {
            // An item whose body never opens is a parse this scanner does not
            // understand; after a while it gives up and reads the lines as
            // production. Reading too much shows up as a red gate; reading too
            // little shows up as nothing at all.
            waiting += 1;
            if waiting > 50 {
                waiting = 0;
            } else {
                skipping = Some((next.max(0), opened));
            }
        } else {
            skipping = Some((next.max(0), opened));
        }
    }
    out
}

/// The production lines of one file, joined back into text.
///
/// The deployer gates search for whole fragments (`self.dispatch_job(`, an
/// `async fn roll_out(`), so they want text rather than a line list. Joining
/// keeps a construct that spans lines intact and drops the fixture bodies in
/// between, which is the point.
pub fn production_text(text: &str) -> String {
    production_lines(text).join("\n")
}
