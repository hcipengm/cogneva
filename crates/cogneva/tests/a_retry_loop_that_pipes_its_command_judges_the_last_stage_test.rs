//! principle: feedback_a_retry_loop_that_pipes_its_command_judges_the_last_stage
//! Gate: a retry loop that pipes its command judges the last stage.
//!
//! `if cmd | tail -4; then break; fi` asks a pipeline for a status, and a
//! pipeline's status is the status of its **last stage**. `tail` succeeds
//! whenever it had a file to read, so the condition holds whatever `cmd` did:
//! the loop breaks on its first pass and a failed command is recorded as a
//! success. The gate is written before the tree is read, from the shape of the
//! act it rules out:
//!
//! - Only a script that does not turn on `pipefail` is exposed. With
//!   `set -o pipefail` (or `set -euo pipefail`) a pipeline's status is its
//!   rightmost non-zero stage, so the condition really does judge `cmd`, and
//!   the whole file is exempt -- for a reason, not by being unread.
//! - Only a pipeline that is the condition of `if` / `elif` / `while` /
//!   `until` is read: that is where a status is turned into a decision. A
//!   pipeline whose output is only consumed, as in `x=$(cmd | tail -1)`,
//!   decides nothing by itself.
//! - Only a single top-level `|` is a pipe: not `||`, not `|&`, and not one
//!   inside quotes.
//! - Only when the last stage is one of `STAGES_THAT_REPORT_NOTHING` -- a
//!   filter that reports that it filtered. `cmd | grep -q pattern` is not this
//!   shape: `grep` reports whether it matched, which is a statement about the
//!   command's output.
//! - The same bytes inside a `#` comment or a quoted literal are not a pipe:
//!   the reader is the shape of the bytes, not a substring search.
//!
//! Judging the command by its own status -- `if out="$(cmd 2>&1)"; then`, or
//! `if cmd >"$log"; then ...; tail -4 "$log"` -- is the repair, and is not red
//! here.
//!
//! The census is every `*.sh` and `*.bash` in the tree, with `.git`, `target`
//! and `node_modules` skipped: the set shellcheck reads, though shellcheck at
//! its default level does not report this. A census that reads nothing reports
//! no violations, so it is asserted non-empty and every entry is a shell
//! script.
//!
//! Tier: tree -- every answer comes from the shape of the bytes.
//!
//! principle: feedback_a_retry_loop_that_pipes_its_command_judges_the_last_stage

use std::ffi::OsStr;
use std::fs::ReadDir;
use std::path::{Path, PathBuf};

use crate::common::routes::repo_root;

/// The principle this gate answers for: the id is anchored in the body, not
/// only in the header.
const PRINCIPLE: &str = "feedback_a_retry_loop_that_pipes_its_command_judges_the_last_stage";

/// A stage that reports that it ran, not that its input was well formed: each
/// of these succeeds whenever it had a stream to read, whatever the command
/// upstream did.
const STAGES_THAT_REPORT_NOTHING: [&str; 15] = [
    "tail", "head", "tee", "cat", "cut", "tr", "sort", "uniq", "wc", "awk", "sed", "nl", "rev",
    "column", "paste",
];

/// The directories the census does not descend into.
const SKIP_DIRS: [&str; 3] = [".git", "target", "node_modules"];

/// The extensions the census reads.
const SHELL_EXTENSIONS: [&str; 2] = ["sh", "bash"];

/// One judged pipeline: the line it starts on, the stage that answered for it,
/// and that line as written.
#[derive(Debug)]
struct Violation {
    line: usize,
    last_stage: String,
    text: String,
}

/// Every `*.sh` and `*.bash` in the tree, as a repository-relative name and its
/// text.
fn shell_scripts() -> Vec<(String, String)> {
    let root = repo_root();
    let mut paths = Vec::new();
    collect_scripts(&root, &mut paths);
    paths
        .into_iter()
        .map(|path| {
            let name = path.strip_prefix(&root).unwrap_or(path.as_path());
            (name.display().to_string(), read_text(&path))
        })
        .collect()
}

/// The text of one file, with the read error carried into the panic: a script
/// the census cannot read is a script whose violations would be reported as
/// none.
fn read_text(path: &Path) -> String {
    match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => panic!("{}: unreadable ({error})", path.display()),
    }
}

/// The entries of one directory, or a panic: a census that silently skipped an
/// unreadable directory would report zero violations from the part it read.
fn directory_entries(dir: &Path) -> ReadDir {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => panic!("{}: unreadable ({error})", dir.display()),
    }
}

/// Every shell script under `dir`, appended to `out`.
fn collect_scripts(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in directory_entries(dir).flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(is_skipped_directory) {
                continue;
            }
            collect_scripts(&path, out);
        } else if path.extension().is_some_and(is_shell) {
            out.push(path);
        }
    }
}

/// Whether a directory with this name is one the census does not enter.
fn is_skipped_directory(name: &OsStr) -> bool {
    SKIP_DIRS.iter().any(|skip| name == *skip)
}

/// Whether a file with this extension is a shell script.
fn is_shell(extension: &OsStr) -> bool {
    SHELL_EXTENSIONS.iter().any(|known| extension == *known)
}

/// Whether `script` turns on `pipefail` anywhere, which exempts the whole file:
/// then a pipeline's status is its rightmost non-zero stage, and the condition
/// really does judge the command it ran.
fn pipefail_enabled(script: &str) -> bool {
    script.lines().any(|line| {
        let mut tokens = line.split_whitespace();
        if tokens.next() != Some("set") {
            return false;
        }
        tokens.any(|token| token == "pipefail")
    })
}

/// The script as logical lines: a trailing `\` joins the next physical line, and
/// the number reported is the first physical line's.
fn logical_lines(script: &str) -> Vec<(usize, String)> {
    let mut out: Vec<(usize, String)> = Vec::new();
    let mut buffer = String::new();
    let mut start = 1usize;
    for (index, line) in script.lines().enumerate() {
        if buffer.is_empty() {
            start = index + 1;
        }
        match line.strip_suffix('\\') {
            Some(rest) => {
                buffer.push_str(rest);
                buffer.push(' ');
            }
            None => {
                buffer.push_str(line);
                out.push((start, std::mem::take(&mut buffer)));
            }
        }
    }
    if !buffer.is_empty() {
        out.push((start, buffer));
    }
    out
}

/// The quoting a shell script carries from one logical line to the next, so a
/// `#` or a `|` inside a quoted string is not read as one.
#[derive(Default)]
struct Quoting {
    single: bool,
    double: bool,
}

impl Quoting {
    /// `line` with an unquoted `#` at the start of a word, and everything after
    /// it, removed.
    fn strip(&mut self, line: &str) -> String {
        let chars: Vec<char> = line.chars().collect();
        let mut out = String::with_capacity(line.len());
        let mut index = 0;
        while index < chars.len() {
            let c = chars[index];
            if self.single {
                out.push(c);
                self.single = c != '\'';
                index += 1;
                continue;
            }
            if self.double {
                out.push(c);
                if c == '\\' && index + 1 < chars.len() {
                    out.push(chars[index + 1]);
                    index += 2;
                    continue;
                }
                self.double = c != '"';
                index += 1;
                continue;
            }
            if c == '#' && starts_word(&chars, index) {
                break;
            }
            if c == '\\' && index + 1 < chars.len() {
                out.push(c);
                out.push(chars[index + 1]);
                index += 2;
                continue;
            }
            self.single = c == '\'';
            self.double = c == '"';
            out.push(c);
            index += 1;
        }
        out
    }
}

/// Whether the `#` at `index` starts a word, which is the only place a shell
/// comment begins.
fn starts_word(chars: &[char], index: usize) -> bool {
    match index.checked_sub(1).map(|before| chars[before]) {
        None => true,
        Some(before) => matches!(before, ' ' | '\t' | ';' | '(' | '&' | '|'),
    }
}

/// The character index of every top-level single `|` in `chars`: outside
/// quotes, and neither `||` nor `|&`.
fn top_level_pipes(chars: &[char]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut single = false;
    let mut double = false;
    let mut index = 0;
    while index < chars.len() {
        let c = chars[index];
        if single {
            single = c != '\'';
            index += 1;
            continue;
        }
        if double {
            if c == '\\' {
                index += 2;
                continue;
            }
            double = c != '"';
            index += 1;
            continue;
        }
        match c {
            '\'' => single = true,
            '"' => double = true,
            '\\' => {
                index += 2;
                continue;
            }
            '|' => {
                match chars.get(index + 1).copied() {
                    Some('|') | Some('&') => index += 2,
                    _ => {
                        out.push(index);
                        index += 1;
                    }
                }
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    out
}

/// The command word of the last stage of a pipeline, with its directory taken
/// off and its quotes dropped.
fn last_stage(chars: &[char]) -> String {
    let mut at = 0;
    while at < chars.len() && is_noise(chars[at]) {
        at += 1;
    }
    let mut word = String::new();
    while at < chars.len() {
        let c = chars[at];
        if c.is_whitespace() || matches!(c, ';' | '|' | ')') {
            break;
        }
        if c != '"' && c != '\'' {
            word.push(c);
        }
        at += 1;
    }
    word.rsplit('/').next().unwrap_or_default().to_string()
}

/// Whether a character says nothing about which command is being run: the
/// shell's noise words and blanks.
fn is_noise(c: char) -> bool {
    c.is_whitespace() || matches!(c, '!' | '{' | '(' | ';')
}

/// The byte offset just past `if` / `elif` / `while` / `until` when the line
/// opens with one of them, which is where a loop's condition begins.
fn condition_keyword(code: &str) -> Option<usize> {
    let trimmed = code.trim_start();
    let offset = code.len() - trimmed.len();
    for keyword in ["if", "elif", "while", "until"] {
        let Some(rest) = trimmed.strip_prefix(keyword) else {
            continue;
        };
        if rest.is_empty() || rest.starts_with(char::is_whitespace) {
            return Some(offset + keyword.len());
        }
    }
    None
}

/// Whether the joined condition already reached the `then` or `do` that ends it.
fn ends_condition(text: &str) -> bool {
    text.split(|c: char| c.is_whitespace() || c == ';')
        .any(|token| token == "then" || token == "do")
}

/// Every pipeline in `script` whose condition is answered by a stage that
/// reports nothing.
///
/// A script that enables `pipefail` is exempt as a whole: its condition really
/// does judge the command, which is the repair this gate asks for and not the
/// act it rules out.
fn violations(script: &str) -> Vec<Violation> {
    if pipefail_enabled(script) {
        return Vec::new();
    }
    let lines = logical_lines(script);
    let mut quoting = Quoting::default();
    let mut out = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        let (number, raw) = &lines[index];
        let code = quoting.strip(raw);
        let Some(start) = condition_keyword(&code) else {
            index += 1;
            continue;
        };
        let mut joined = code[start..].to_string();
        let mut last = index;
        while !ends_condition(&joined) && last + 1 < lines.len() {
            last += 1;
            joined.push(' ');
            joined.push_str(&quoting.strip(&lines[last].1));
        }
        let chars: Vec<char> = joined.chars().collect();
        let pipes = top_level_pipes(&chars);
        if let Some(pipe) = pipes.last().copied() {
            let stage = last_stage(&chars[pipe + 1..]);
            if STAGES_THAT_REPORT_NOTHING.contains(&stage.as_str()) {
                out.push(Violation {
                    line: *number,
                    last_stage: stage,
                    text: raw.trim().to_string(),
                });
            }
        }
        index = last + 1;
    }
    out
}

#[test]
fn a_retry_loop_that_pipes_through_a_silent_stage_is_judged_by_it() {
    let script = "\
while ! healthy; do
  if run_check | tail -4; then
    break
  fi
done
";
    let found = violations(script);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].line, 2);
    assert_eq!(found[0].last_stage, "tail");
}

#[test]
fn pipefail_is_the_repair_and_exempts_the_file() {
    let script = "\
set -euo pipefail
while ! healthy; do
  if run_check | tail -4; then
    break
  fi
done
";
    assert!(violations(script).is_empty());
}

#[test]
fn the_last_stage_is_read_from_the_end_of_the_pipe() {
    let script = "\
while run_check | tee log | tail -1; do
  count=$((count + 1))
done
";
    let found = violations(script);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].last_stage, "tail");
}

#[test]
fn until_a_service_is_up_is_judged_by_the_stage_that_answers() {
    let script = "\
until systemctl is-active svc | cat; do
  sleep 1
done
";
    let found = violations(script);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].last_stage, "cat");
}

#[test]
fn judging_the_command_by_its_own_status_is_not_this_shape() {
    let captured = "\
if out=\"$(run_check 2>&1)\"; then
  echo \"$out\"
fi
";
    assert!(violations(captured).is_empty());
    let redirected = "\
if run_check >\"$log\"; then
  tail -4 \"$log\"
fi
";
    assert!(violations(redirected).is_empty());
}

#[test]
fn the_same_bytes_in_prose_or_a_literal_are_not_a_pipe() {
    let commented = "\
# if run_check | tail -4; then break; fi
";
    assert!(violations(commented).is_empty());
    let printed = "\
printf 'if run_check | tail -4; then break; fi\\n'
";
    assert!(violations(printed).is_empty());
    let quoted_argument = "\
if [ -n \"$(echo x | tail -1)\" ]; then
  echo ok
fi
";
    assert!(violations(quoted_argument).is_empty());
}

#[test]
fn a_stage_that_reports_its_own_verdict_judges_the_command() {
    let script = "\
if run_check | grep -q ok; then
  echo ok
fi
";
    assert!(violations(script).is_empty());
}

#[test]
fn the_census_is_the_shell_scripts_the_tree_carries() {
    let scripts = shell_scripts();
    assert!(!scripts.is_empty(), "no shell script was read at all");
    for (name, _) in &scripts {
        assert!(
            name.ends_with(".sh") || name.ends_with(".bash"),
            "{name}: not a shell script"
        );
    }
}

#[test]
fn no_script_judges_a_pipe_by_a_stage_that_reports_nothing() {
    let mut found = Vec::new();
    for (name, text) in shell_scripts() {
        for violation in violations(&text) {
            let at = format!("{name}:{}", violation.line);
            found.push(format!("{at}: {} {}", violation.last_stage, violation.text));
        }
    }
    assert!(
        found.is_empty(),
        "{PRINCIPLE}: {} script(s) judge a pipeline by a stage that always \
         succeeds:\n{}",
        found.len(),
        found.join("\n")
    );
}
