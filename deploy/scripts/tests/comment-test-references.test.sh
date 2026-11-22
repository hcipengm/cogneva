#!/usr/bin/env bash
# Deterministic gate for "a comment points the reader at a test file that is gone".
#
# The failure is silent by construction: the reader follows the name, finds no
# such file, and nothing errored anywhere. Which is why the citation needs a
# reader rather than a rule. The fixtures below are one small git repo each
# spelling the same name three legal ways plus one dead way:
#   - root-relative, citing-file-relative, and bare basename all resolve;
#   - a name resolved by none is red and printed with its file:line;
#   - the same dead name with a marker on its line is green (a retirement note
#     is a claim about the tree, not a pointer into it);
#   - a shell comment is read with the same judgement as a Rust comment;
#   - a defaulted run that reads almost no comments exits 3, because "read
#     nothing" and "nothing is missing" print the same thing;
#   - a defaulted run on a tree full of comments and empty of citations exits 3
#     too, because "the token pattern stopped matching" and "no comment names a
#     test" print the same thing -- that one trips the citation floor with the
#     comment floor satisfied, which is the only shape that tells them apart;
#   - outside a git work tree it exits 3, because there is no population.
# The real tree is judged last: green, and the reading it prints is non-zero.
set -euo pipefail
# Fixture repositories are written through `git -C`, which reads GIT_DIR /
# GIT_WORK_TREE / GIT_INDEX_FILE from the environment when they are exported by
# a commit lander, a hook, or a snapshot -- and then a fixture's commit lands in
# the caller's repository. Clear it once, at file scope.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-comment-test-references.py"
fail() { echo "FAIL: $*" >&2; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# new_repo <name> => prints its path, with one tracked test in it
new_repo() {
  local d="${work}/$1"
  mkdir -p "${d}/src" "${d}/tests"
  git -C "${d}" init -q
  git -C "${d}" config user.email t@example.invalid
  git -C "${d}" config user.name t
  printf '%s\n' '//! principle: feedback_fixture' 'fn keep() {}' >"${d}/tests/keep_test.rs"
  printf '%s\n' '#!/usr/bin/env bash' 'true' >"${d}/tests/keep.test.sh"
  git -C "${d}" add -A
  git -C "${d}" commit -q -m base
  printf '%s' "${d}"
}

# run <repo> [args…] -- the checker resolves the tree from the working directory
run() {
  local d="$1"; shift
  ( cd "${d}" && "${checker}" "$@" 2>&1 )
}

# ---- 1. all three legal spellings resolve
d="$(new_repo resolve)"
printf '%s\n' '// tests/keep_test.rs covers the keep half' \
              '// ../tests/keep_test.rs covers it again' \
              '// keep_test.rs is the same file named short' \
              '// keep.test.sh is its shell side' 'fn body() {}' >"${d}/src/a.rs"
rc=0; out="$(run "${d}" src/a.rs)" || rc=$?
[ "${rc}" -eq 0 ] || fail "1. legal spellings should be green, got rc=${rc}: ${out}"
case "${out}" in *"4 处测试引用（4 处判了、0 处标了原因不判）"*) : ;; *) fail "1. expected 4 citations in the reading: ${out}" ;; esac

# ---- 2. a dead name is red and printed with file:line
printf '%s\n' '// tests/gone_test.rs is where it used to live' 'fn body() {}' >"${d}/src/b.rs"
rc=0; out="$(run "${d}" src/b.rs)" || rc=$?
[ "${rc}" -eq 1 ] || fail "2. dead name should be red, got rc=${rc}: ${out}"
case "${out}" in *"src/b.rs:1: tests/gone_test.rs"*) : ;; *) fail "2. expected the hit named: ${out}" ;; esac

# ---- 3. the same dead name, marked on its line, is green
printf '%s\n' '// tests/gone_test.rs（已退休）不再是这一层的门' 'fn body() {}' >"${d}/src/c.rs"
rc=0; out="$(run "${d}" src/c.rs)" || rc=$?
[ "${rc}" -eq 0 ] || fail "3. marked dead citation should be green, got rc=${rc}: ${out}"
case "${out}" in *"1 处测试引用（0 处判了、1 处标了原因不判）"*) : ;; *) fail "3. a marked citation must still be counted: ${out}" ;; esac

# ---- 4. a shell comment is judged the same way
printf '%s\n' '#!/usr/bin/env bash' '# tests/gone.test.sh used to run here' 'true' >"${d}/src/d.sh"
rc=0; out="$(run "${d}" src/d.sh)" || rc=$?
[ "${rc}" -eq 1 ] || fail "4. dead shell citation should be red, got rc=${rc}: ${out}"
case "${out}" in *"src/d.sh:2: tests/gone.test.sh"*) : ;; *) fail "4. expected the hit named: ${out}" ;; esac

# ---- 5. the shebang is not read as a comment
printf '%s\n' '#!/usr/bin/env gone_test.rs' 'true' >"${d}/src/e.sh"
rc=0; out="$(run "${d}" src/e.sh)" || rc=$?
[ "${rc}" -eq 0 ] || fail "5. shebang should not be read, got rc=${rc}: ${out}"

# ---- 6. a defaulted run that reads almost no comments exits 3
comment_floor="$(sed -n 's/^COMMENT_FLOOR = \([0-9][0-9]*\)$/\1/p' "${checker}")"
[ -n "${comment_floor}" ] || fail "6. could not read COMMENT_FLOOR from ${checker}"
rc=0; out="$(run "${d}")" || rc=$?
[ "${rc}" -eq 3 ] || fail "6. tiny population should exit 3, got rc=${rc}: ${out}"
case "${out}" in *"下限 ${comment_floor}"*) : ;; *) fail "6. expected the comment floor named: ${out}" ;; esac

# ---- 6b. comments aplenty, citations none: only the citation floor can catch it
# Read the floor out of the gate rather than restating it: a test that pins its
# own copy of the knob stays green when the knob moves.
cite_floor="$(sed -n 's/^CITE_FLOOR = \([0-9][0-9]*\)$/\1/p' "${checker}")"
[ -n "${cite_floor}" ] || fail "6b. could not read CITE_FLOOR from ${checker}"
awk 'BEGIN { for (i = 0; i < 10001; i++) print "// padding so the comment floor is met" }' >"${d}/src/f.rs"
git -C "${d}" add src/f.rs
git -C "${d}" commit -q -m padding -- src/f.rs
rc=0; out="$(run "${d}")" || rc=$?
[ "${rc}" -eq 3 ] || fail "6b. comments without citations should exit 3, got rc=${rc}: ${out}"
case "${out}" in *"处测试引用（下限 ${cite_floor}）"*) : ;; *) fail "6b. expected the citation floor named: ${out}" ;; esac

# ---- 7. outside a git work tree there is no population
nogit="${work}/nogit"
mkdir -p "${nogit}"
printf '%s\n' '// tests/keep_test.rs' 'fn body() {}' >"${nogit}/a.rs"
rc=0; out="$(run "${nogit}" a.rs)" || rc=$?
[ "${rc}" -eq 3 ] || fail "7. no git tree should exit 3, got rc=${rc}: ${out}"

# ---- 8. the real tree is green, and the reading is not zero
rc=0; out="$(run "${repo}")" || rc=$?
[ "${rc}" -eq 0 ] || fail "8. real tree should be green, got rc=${rc}:\n${out}"
case "${out}" in *"OK:"*) : ;; *) fail "8. expected the OK reading: ${out}" ;; esac
count="$(printf '%s\n' "${out}" | sed -n 's/.*注释里 \([0-9][0-9]*\) 处测试引用.*/\1/p')"
[ -n "${count}" ] || fail "8. could not read the citation count: ${out}"
[ "${count}" -ge "${cite_floor}" ] ||
  fail "8. real tree reading is ${count}, below the gate's own floor ${cite_floor}"

echo "OK: comment-test-references fixtures green; real tree ${count} citations"
