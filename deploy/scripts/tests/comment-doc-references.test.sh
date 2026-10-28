#!/usr/bin/env bash
# Deterministic gate for "a code comment points the reader at a document".
#
# The failure this pins is silent by construction. The private design, audit and
# record documents live outside this repository, so a comment naming one reads as
# a complete explanation to whoever wrote it and as a dead pointer to everyone
# else -- nothing fails, nothing warns, the citation just sits there being
# unusable. The rule the checker enforces is prose ("a code comment does not
# reference any document"), which is why it drifted: only a reading makes it stay
# true. Measured before landing: one comment cited the private directory and
# twelve carried a section sign, one of which was a legitimate external standard.
#
# The judgement is the shape of the citation, not the file it lives in, so the
# fixtures below are one file per shape:
#   - the three shapes that count (private directory, dated file name, section
#     sign) each get a fixture that must be read as a hit;
#   - two fixtures must stay green because they are what the narrow reading is
#     for: a `.md` path inside code (a string the program means, not a citation)
#     and an external standard (`RFC 7230 §4.1` -- a document the reader can
#     obtain, and the only way to cite the grammar a wire format follows);
#   - one clean file to keep the fixtures from being green for the wrong reason;
#   - one path the checker cannot read at all, which must be red: a failed read
#     hands back zero hits, the same output a clean file gives.
# Every fixture is a real file with a real extension handed to the checker by
# path, so what is exercised is the reading, not a stub.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-comment-doc-references.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# expects <name> <exit-code> <body>
expect() {
  local name="$1" want="$2" body="$3"
  local path="${work}/${name}.rs"
  printf '%s\n' "${body}" >"${path}"
  local got=0
  "${checker}" "${path}" >/dev/null 2>&1 || got=$?
  [ "${got}" -eq "${want}" ] || fail "${name}: exit ${got}, wanted ${want}"
}

expect clean 0 'fn main() {
    // The counter is read before the write so a torn pair cannot be observed.
    let x = 1;
}'

expect private_dir 1 'fn main() {
    // 见 internal-docs/README.md 的说明
}'

expect dated_file 1 'fn main() {
    /// 设计见 2026-10-11_把散文规则落成门.md
}'

expect section_sign 1 'fn main() {
    // 产物级进化（§14.3）在这里接入
}'

# A path in code is a value the program means. Reading it would flag every test
# that loads a markdown fixture, which is why the read is comment-only.
expect md_path_in_code 0 'fn main() {
    let p = "internal-docs/2026-10-11_foo.md";
    assert!(p.contains("§"));
}'

# Narrowing the strict side: a published standard is citable.
expect external_standard 0 'fn main() {
    /// 分块传输的解码（RFC 7230 §4.1）
}'

# A path the checker cannot read is not a clean file. The read discards its own
# failures unless they are asked for, and the discarded failure hands back zero
# hits -- the same output a clean file gives. A directory is unreadable for
# every caller, so this case does not depend on who runs the test.
mkdir -p "${work}/not-a-file"
if out="$("${checker}" "${work}/not-a-file" 2>&1)"; then
  fail "读不动的路径应当红，实际绿：${out}"
fi
printf '%s' "${out}" | grep -q '读不出这些文件' \
  || fail "红的时候要说清是读不出来，不是有引用：${out}"

# The default path -- the one CI runs -- must be green on the tree itself, and it
# has to read something: an empty file set would make the check pass by reading
# nothing.
out="$("${checker}" 2>&1)" || fail "tree is not green: ${out}"
case "${out}" in
  OK:*) ;;
  *) fail "unexpected output: ${out}" ;;
esac

echo "OK: comment-doc-references"
