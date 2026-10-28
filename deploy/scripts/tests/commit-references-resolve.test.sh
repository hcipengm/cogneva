#!/usr/bin/env bash
# A push may not add a reference or a module declaration its own tree cannot answer.
#
# The failure this pins is invisible to every local reading: on 2026-10-29 a
# commit carried `use crate::test_support::scrape;` while the file that defines
# it sat untracked in the working tree. `cargo check` was green (the file was on
# disk), the push went out, CI failed on the unresolved import, and the
# deployment driver stopped at the last green revision. Nothing in the pre-push
# suite compiled, and even a compile would have read the working tree.
#
# Both directions are load-bearing. A gate that never fires leaves the rule
# unenforced; a gate that fires on a legitimate commit blocks every push until
# someone deletes it. So the fixtures pin the red cases and the shapes that must
# stay green: a module file added alongside its reference, a path quoted in prose
# and in a string literal, a root item, a re-exported module reached through
# `pub use`, a `#[path]` declaration pointing elsewhere, and a `mod x;` written
# beside a file that sits in a subdirectory.
#
# The declarations are the second half, and the first draft of the gate got them
# exactly backwards: it read `mod x;` as the proof that `crate::x` resolves, so a
# push that added `mod orphan;` and no `orphan.rs` was green, and so was a push
# that added `crate::orphan` together with the declaration that was supposed to
# answer it. Both compile nowhere. They are pinned below (found 2026-10-29 by the
# session watching the rollout, with fixtures).
#
# An unasked question is never green: a range that does not resolve must exit 2,
# not 0.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-commit-references.py"
fail() { echo "FAIL: $*"; exit 1; }

command -v git >/dev/null || { echo "missing dependency: git" >&2; exit 2; }
command -v python3 >/dev/null || { echo "missing dependency: python3" >&2; exit 2; }
[ -f "${checker}" ] || fail "not found: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
export GIT_AUTHOR_NAME=fixture GIT_AUTHOR_EMAIL=fixture@example.invalid
export GIT_COMMITTER_NAME=fixture GIT_COMMITTER_EMAIL=fixture@example.invalid
git init -q -b main "${work}/hist"
cd "${work}/hist"

commit() { # commit <message> — commit what is staged and print the sha
  git commit -q -m "$1"
  git rev-parse HEAD
}

mkdir -p crates/demo/src
cat >crates/demo/src/lib.rs <<'RS'
pub mod alpha;
pub fn f() {
    crate::alpha::g();
}
RS
echo 'pub fn g() {}' >crates/demo/src/alpha.rs
git add crates
base="$(commit 'base: a crate whose tree answers its own references')"

# 1. The break: a reference with no definition anywhere in the committed tree.
cat >crates/demo/src/lib.rs <<'RS'
pub mod alpha;
pub fn f() {
    crate::alpha::g();
}
pub fn h() {
    crate::beta::g();
}
RS
git add crates
bad="$(commit 'add a call into a module that is not in this commit')"

out="$("${checker}" --base "${base}" --head "${bad}" 2>&1)" && status=0 || status=$?
[ "${status}" -eq 1 ] || fail "unresolvable reference exited ${status}, wanted 1"$'\n'"${out}"
grep -q 'crate::beta' <<<"${out}" || fail "the red did not name crate::beta"$'\n'"${out}"
grep -q '判了' <<<"${out}" || fail "no denominator in the summary"$'\n'"${out}"

# 2. Green: the module lands in the same push.
echo 'pub fn g() {}' >crates/demo/src/beta.rs
sed -i 's/^pub mod alpha;$/pub mod alpha;\npub mod beta;/' crates/demo/src/lib.rs
git add crates
good="$(commit 'land the module the reference needs')"
out="$("${checker}" --base "${base}" --head "${good}" 2>&1)" || fail "a landed module stayed red"$'\n'"${out}"

# 3. Green: prose and literals quote paths; a quote is not a reference.
cat >crates/demo/src/lib.rs <<'RS'
pub mod alpha;
pub mod beta;
pub fn f() {
    crate::alpha::g();
}
/// Compare with `crate::gamma::g()`, which this crate does not have.
pub fn documented() -> &'static str {
    "crate::delta::g()"
}
RS
git add crates
prose="$(commit 'quote two modules that do not exist')"
out="$("${checker}" --base "${good}" --head "${prose}" 2>&1)" || fail "prose was read as a reference"$'\n'"${out}"

# 4. Green: a root item and a root re-export both answer `crate::<name>`.
cat >crates/demo/src/lib.rs <<'RS'
pub mod alpha;
pub mod beta;
pub use some_other_crate::widget;
pub struct Config;
pub fn f() -> Config {
    crate::widget::build();
    crate::Config {}
}
RS
git add crates
reexport="$(commit 'reach a re-export and a root item')"
out="$("${checker}" --base "${prose}" --head "${reexport}" 2>&1)" || fail "root item / re-export stayed red"$'\n'"${out}"

# 5. Red: a declaration alone is a promise the tree has to keep. Taking it as
#    proof of itself is what made the first draft blind to this shape.
cat >crates/demo/src/lib.rs <<'RS'
pub mod alpha;
pub mod beta;
pub mod orphan;
pub fn f() {
    crate::alpha::g();
}
RS
git add crates
orphan="$(commit 'declare a module whose file is still in the working tree')"
out="$("${checker}" --base "${reexport}" --head "${orphan}" 2>&1)" && status=0 || status=$?
[ "${status}" -eq 1 ] || fail "a declaration with no file exited ${status}, wanted 1"$'\n'"${out}"
grep -q 'mod orphan' <<<"${out}" || fail "the red did not name mod orphan"$'\n'"${out}"

# 6. Red: the reference and the declaration that is supposed to answer it, both
#    added, neither backed by a file. The declaration must not discharge itself.
cat >crates/demo/src/lib.rs <<'RS'
pub mod alpha;
pub mod beta;
pub mod ghost;
pub fn f() {
    crate::alpha::g();
    crate::ghost::g();
}
RS
git add crates
ghost="$(commit 'declare and call a module, ship no file')"
out="$("${checker}" --base "${orphan}" --head "${ghost}" 2>&1)" && status=0 || status=$?
[ "${status}" -eq 1 ] || fail "a self-answering declaration exited ${status}, wanted 1"$'\n'"${out}"
grep -q 'mod ghost' <<<"${out}" || fail "the red did not name mod ghost"$'\n'"${out}"

# 7. Green: `#[path]` decides where the file is, even outside the module tree.
mkdir -p crates/demo/src/elsewhere
cat >crates/demo/src/lib.rs <<'RS'
pub mod alpha;
pub mod beta;
pub mod ghost;
#[path = "elsewhere/relocated.rs"]
pub mod relocated;
pub fn f() {
    crate::alpha::g();
    crate::ghost::g();
}
RS
echo 'pub fn g() {}' >crates/demo/src/elsewhere/relocated.rs
git add crates
relocated="$(commit 'point #[path] at a file outside the module tree')"
out="$("${checker}" --base "${ghost}" --head "${relocated}" 2>&1)" || fail "#[path] was read as a missing file"$'\n'"${out}"

# 8. Green: a `mod x;` written beside a file in a subdirectory. Rust looks in a
#    directory of the module's name, so a check that insisted on the declaring
#    file's own directory would red this legitimate shape.
mkdir -p crates/demo/src/nested
cat >crates/demo/src/nested/inline.rs <<'RS'
pub mod leaf;
RS
echo 'pub fn g() {}' >crates/demo/src/nested/leaf.rs
cat >crates/demo/src/lib.rs <<'RS'
pub mod alpha;
pub mod beta;
pub mod ghost;
#[path = "elsewhere/relocated.rs"]
pub mod relocated;
pub mod nested;
pub fn f() {
    crate::alpha::g();
    crate::ghost::g();
}
RS
echo 'pub mod inline;' >crates/demo/src/nested/mod.rs
git add crates
nested="$(commit 'declare a module under a directory of its own name')"
out="$("${checker}" --base "${relocated}" --head "${nested}" 2>&1)" || fail "a nested mod was read as missing"$'\n'"${out}"

# 9. An unasked question is never green.
if "${checker}" --base does-not-exist --head "${nested}" >/dev/null 2>&1; then
  fail "an unresolvable base exited 0"
fi
status=0
"${checker}" --base does-not-exist --head "${nested}" >/dev/null 2>&1 || status=$?
[ "${status}" -eq 2 ] || fail "an unresolvable base exited ${status}, wanted 2"

echo "PASS commit-references-resolve"
