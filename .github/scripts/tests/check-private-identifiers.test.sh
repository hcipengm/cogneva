#!/usr/bin/env bash
# check-private-identifiers.sh 的判据：公开树里只允许指到公开的那个仓。
#
# 这条门坏起来是**静默**的：判据锚错地方，它就对着一棵合规的树报红（于是被删掉），
# 或者对着一个真泄露报绿（于是等于没有）。所以夹具必须同时给三边：
#   - 允许的那一个（含 `.git` 这种同仓的另一种写法）——必须绿；
#   - 另一个仓——必须红，且判词点到行；
#   - **第三方 owner**（别人的仓）——必须完全不被这条判据看到。
# 少了第三边，「所有者是开放集合、只有 hcipengm 归我管」这条口径就没被钉住，门会
# 长成一张全网仓名清单。
#
# 裸标识那一边的输入来自 CI secret，所以它在本地有四种状态：注入且命中（红）、
# 注入且无命中（绿）、没注入（绿但要明说没跑）、没注入且本级要求必须有（红）。
# 四种都要钉——只测「命中即红」的话，一个永远没注入的 secret 会让这层静默作废。
#
# 两层的夹具分开：测裸标识那一边时树上不留任何 `owner/repo` 泄露，否则红可能来自
# 第一层，判不出究竟是哪一层在说话。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${here}/../check-private-identifiers.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "gate not found: ${gate}"

# 测试自己建的 throwaway 仓会继承调用者的 `GIT_*`（钩子会设 `GIT_INDEX_FILE`），
# 清掉，别把夹具写进别人的索引。名单类变量同理：调用者环境里若有一份，会盖住这里
# 设的那份，测的就不是这段代码了。
unset GIT_INDEX_FILE GIT_DIR GIT_WORK_TREE GIT_OBJECT_DIRECTORY \
      GIT_ALTERNATE_OBJECT_DIRECTORIES PRIVATE_IDENTIFIERS REQUIRE_PRIVATE_IDENTIFIERS

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture="${work}/fixture"
git -C "${work}" init -q fixture
mkdir -p "${fixture}/src"
printf '%s\n' 'clone: https://github.com/hcipengm/cogneva.git' >"${fixture}/README.md"
printf '%s\n' 'https://gitee.com/hcipengm/cogneva.git' >"${fixture}/src/notes.txt"
git -C "${fixture}" add -A

# 跑门：<期望退出码> <期望输出里必须出现的串> [args...]
# 名单类变量用 export 传（`VAR=x bash script` 里的赋值只到函数为止，不落到子进程）。
run_gate() {
  local want="$1" needle="$2" out rc=0
  shift 2
  out="$(bash "${gate}" "$@" 2>&1)" || rc=$?
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

# --- 1：所有者/仓那一层 ---

# 允许的那两个写法：绿。`.git` 后缀被算成同一个仓，不是第二个。
run_gate 0 '只有 hcipengm/cogneva' "${fixture}"

# 第三方 owner 完全不在这条判据的视野里：加进去仍然绿。
printf '%s\n' 'upstream: https://github.com/rust-lang/rust.git' >"${fixture}/src/third.txt"
git -C "${fixture}" add -A
run_gate 0 '只有 hcipengm/cogneva' "${fixture}"

# 另一个仓：红，且判词指到行（文件:行号）。
printf '%s\n' 'see https://github.com/hcipengm/secret-thing' >"${fixture}/src/leak.txt"
git -C "${fixture}" add -A
run_gate 1 'secret-thing' "${fixture}"
out="$(bash "${gate}" "${fixture}" 2>&1 || true)"
case "${out}" in
  *leak.txt:*) ;;
  *) fail "判词没有指到行 —— ${out}" ;;
esac

# --- 2：裸标识那一层（树上此时不留 owner/repo 泄露）---
rm "${fixture}/src/leak.txt"
printf '%s\n' 'the face name zzfacequux appears here' >"${fixture}/src/bare.txt"
git -C "${fixture}" add -A
run_gate 0 '只有 hcipengm/cogneva' "${fixture}"   # 第一层干净，红只能来自第二层

# 注入且命中 ⇒ 红。
export PRIVATE_IDENTIFIERS='zzfacequux'
run_gate 1 '命中「zzfacequux」' "${fixture}"
unset PRIVATE_IDENTIFIERS

# 注入但无命中 ⇒ 绿。
export PRIVATE_IDENTIFIERS='no-such-token-anywhere'
run_gate 0 '无命中' "${fixture}"
unset PRIVATE_IDENTIFIERS

# 没注入 ⇒ 绿，但必须明说这一遍没跑（不静默当通过）。
run_gate 0 '这一遍没跑' "${fixture}"

# 没注入而本级要求必须在 ⇒ 红。
export REQUIRE_PRIVATE_IDENTIFIERS=true
run_gate 1 '要求它必须在' "${fixture}"
unset REQUIRE_PRIVATE_IDENTIFIERS

# --- 3：真文件 —— 这棵仓库今天必须绿 ---
# 夹具全绿而真树红，说明这条门在测自己；两边都绿，才说明它今天在守东西。
run_gate 0 '通过' "${repo}"

echo "check-private-identifiers.test: all checks passed"
