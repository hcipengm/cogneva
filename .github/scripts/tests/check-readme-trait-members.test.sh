#!/usr/bin/env bash
# check-readme-trait-members.sh 的判据：README 里标成 Trait「定义」的成员表，必须与源一致。
#
# 这条门的分档是它的全部：`definition` 差一个成员就要红，`example` 少列只作读数。所以
# 夹具必须**同时给两边**——只测 definition 那一半，门会在别人精简一个示例时挡住他，
# 而只测 example 那一半，门等于没有。两个标签各自正反两面都钉住，四个方向。
#
# 另外两边的「定不到」也要钉：源文件不存在、源里没有这个 trait。这两种如果静默通过，
# 门会在文件被改名之后**对着一个不存在的目标报绿**——那正是它要防的漂移，却是它自己
# 制造出来的。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${here}/../check-readme-trait-members.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "gate not found: ${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture="${work}/fixture"
mkdir -p "${fixture}/crates/x/src"
cat >"${fixture}/crates/x/src/lib.rs" <<'RS'
pub trait Alpha {
    /// 注释里出现的方法名不是成员。
    fn a(&self);
    fn b(&self);
}
RS

write_readme() { # <label> <member list>
    cat >"${fixture}/README.md" <<EOF
\`Alpha\` Trait ${1} (\`crates/x/src/lib.rs\`):
- ${2}
EOF
}

# 跑门：<期望退出码> <期望输出里必须出现的串>
run_gate() {
    local want="$1" needle="$2" out rc=0
    out="$(bash "${gate}" "${fixture}" 2>&1)" || rc=$?
    if [ "${rc}" -ne "${want}" ]; then
        printf '%s\n' "${out}" >&2
        fail "exit ${rc}, wanted ${want}"
    fi
    if [ -n "${needle}" ]; then
        case "${out}" in
            *"${needle}"*) ;;
            *) printf '%s\n' "${out}" >&2; fail "输出里没有「${needle}」" ;;
        esac
    fi
}

# --- definition：齐则绿，缺则红且点名، 多则红
write_readme definition '`a` / `b`'
run_gate 0 '与源一致'

write_readme definition '`a`'
run_gate 1 '却少了 1 个成员'
out="$(bash "${gate}" "${fixture}" 2>&1 || true)"
case "${out}" in
    *'    b'*) ;;
    *) fail "判词没有点出缺的那个成员 b —— ${out}" ;;
esac

write_readme definition '`a` / `b` / `zzz_not_a_member`'
run_gate 1 '源里没有的名字'

# --- example：不齐是允许的，且不许因此变红
write_readme example '`a`'
run_gate 0 '是示例，少列 1 个成员'
write_readme example '`a` / `b` / `zzz_not_a_member`'
run_gate 1 '源里没有的名字'   # 多列在两边都是缺陷：那不是「不全」，是列了不是成员的东西

# --- 定不到目标：两种都要红，不能静默绿
cat >"${fixture}/README.md" <<'EOF'
`Alpha` Trait definition (`crates/x/src/gone.rs`):
- `a` / `b`
EOF
run_gate 1 '源路径定不到'

cat >"${fixture}/README.md" <<'EOF'
`Beta` Trait definition (`crates/x/src/lib.rs`):
- `a` / `b`
EOF
run_gate 1 '名字或路径已经漂了'

# --- 真文件：这棵仓库今天必须绿
# 夹具全绿而真树红，说明这条门在测自己；两边都绿，才说明它今天在守东西。
out="$(bash "${gate}" "${repo}" 2>&1)" || fail "真树红了 —— 判为 Trait 定义的成员表与源不一致：
${out}"
case "${out}" in
    *'与源一致'*) ;;
    *) fail "真树没有给出通过判词：${out}" ;;
esac

echo "check-readme-trait-members.test: all checks passed"
