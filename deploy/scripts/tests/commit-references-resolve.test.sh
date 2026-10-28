#!/usr/bin/env bash
# A push may not add a `crate::<name>` reference its own tree cannot answer.
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
# someone deletes it. So the fixtures pin the red case and the four shapes that
# must stay green: a module file added alongside its reference, a path quoted in
# prose and in a string literal, a root item, and a re-exported module reached
# through `pub use`. The census that preceded this gate (200 commits, one hit --
# the break above) is what says the resolver knows those shapes; this test is
# what says it keeps knowing them.
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

# 5. An unasked question is never green.
if "${checker}" --base does-not-exist --head "${reexport}" >/dev/null 2>&1; then
  fail "an unresolvable base exited 0"
fi
status=0
"${checker}" --base does-not-exist --head "${reexport}" >/dev/null 2>&1 || status=$?
[ "${status}" -eq 2 ] || fail "an unresolvable base exited ${status}, wanted 2"

echo "PASS commit-references-resolve"
