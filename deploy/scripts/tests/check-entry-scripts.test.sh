#!/usr/bin/env bash
# 「入口脚本」这一格的检查只有一份正文，两处调用者共享：上游 CI 的 `entry-scripts`
# 那一格，以及树自己的变更验证（名字以 `check-` 开头，被当成一条门禁问）。
#
# 这条测试判的是「共享」与「跑不了要说明白」这两件事，不是那 136 条脚本测试各自
# 对不对——那些要 helm、kubectl、promtool、pwsh 和一张真库，而这里要能在没有它们
# 的机器上跑。判六件事：
#
#   1. 缺工具时 exit 2，并逐个点名缺的是什么。全树的约定是「2 = 我跑不了」；把缺
#      工具记成通过，就是这条脚本存在要消灭的那种读数。
#   2. 缺工具时**不跑**脚本测试那一格。那些测试故意在缺工具时红（告警规则那一族
#      拿不到 promtool 就把「看不了」记成红），跑下去会把缺工具报成一条与改动无关
#      的红——一次真实的缺陷就藏在那片假红后面。
#   3. 清单是从树里数出来的（`git ls-files '*.sh'`）与 glob 出来的（三处测试目录），
#      不是手抄的名单。
#   4. 工作流里不许再出现那三段正文，而且必须调这条脚本。两份正文会漂，漂了之后
#      本地绿、上游红的差就回来了——那正是这次改动的起因。
#   5. 镜像里装着 shellcheck。这一条是这一格的「有东西可判」：没有它，脚本在沙箱里
#      恒为 exit 2，一段脚本都没扫过，而契约那一栏读起来像「跑了」——这正是这次
#      改动要消灭的那种读数，所以钉在镜像的 Dockerfile 上。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${HERE}/../check-entry-scripts.sh"
ROOT="$(cd "${HERE}/../../.." && pwd)"
WORKFLOW="${ROOT}/.github/workflows/ci.yml"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# 一个最小的工具台：shellcheck 与 git 换成假的，好让这条测试不扫 253 个文件、也不
# 依赖真仓库的索引。其余工具（helm / kubectl / promtool / pwsh）**故意不给**，好让
# 「缺什么就点名什么」这一格有东西可缺。
mkdir -p "${WORK}/bin"
cat >"${WORK}/bin/git" <<'EOF'
#!/usr/bin/env bash
# 只被问 `ls-files '*.sh'`：给一个文件就够，判的是「清单从树里数」这件事。
printf 'deploy/scripts/check-entry-scripts.sh\n'
EOF
cat >"${WORK}/bin/shellcheck" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "${WORK}/bin/git" "${WORK}/bin/shellcheck"

# 1 与 2：缺工具、缺库。
#
# 「缺哪些工具」必须是这条路**自己造出来**的，不能靠跑它的机器上恰好没有：CI 的
# runner 用 apt 装了 promtool，也自带 /usr/bin/pwsh，于是 `PATH=…:/usr/bin:/bin`
# 这一句在那里仍然找得到它们；脚本如实地不点名，测试却在断言「必须点名」。2026-11-06
# 实测：这一条在上游 Entry Scripts Lint 上一直是 3 条不满足，而本机（两样都没有）
# 一直是绿的——读数跟着机器变，红的原因与提交内容无关，整整一天没有一版滚上去。
# 所以这里给一个只有 bash 与那两个假工具的 PATH：缺的那几样是构造出来的，不是捡来的。
mkdir -p "${WORK}/only"
for kept in bash dirname; do
  ln -s "$(command -v "${kept}")" "${WORK}/only/${kept}"
done
env -u COGNEVA_TEST_DATABASE_URL \
  PATH="${WORK}/bin:${WORK}/only" \
  bash "${SCRIPT}" >"${WORK}/out" 2>"${WORK}/err"
status=$?
if [ "${status}" -ne 2 ]; then
  fail "缺工具时退出码是 ${status}，不是 2（「我跑不了」）；这一格报成别的，就是把它读成了一个判词"
fi
for tool in helm kubectl promtool pwsh COGNEVA_TEST_DATABASE_URL; do
  grep -q "${tool}" "${WORK}/err" ||
    fail "没跑成的清单里没有点名 ${tool}：$(cat "${WORK}/err")"
done
grep -q '没跑：缺' "${WORK}/err" ||
  fail "缺工具时脚本测试那一格没有说明「没跑」：$(cat "${WORK}/err")"
# 「没跑」要连「跑了什么」一起说：只印缺什么，读的人不知道这一格判了多少。
grep -q '跑了且绿：shellcheck' "${WORK}/err" ||
  fail "缺工具时没有说明已经跑成了哪一段：$(cat "${WORK}/err")"
if grep -q '^== script tests' "${WORK}/out"; then
  fail "缺工具时仍然起了脚本测试；那些测试会把「缺 promtool」报成一条红，而红与缺工具是两种要分别处置的事"
fi
# 造出来的 PATH 要够这条脚本**自己**跑起来（`dirname` 这种不算「被测的工具」）。
# 不够时它带着半截正文跑完，而上面几条断言只看「点了谁的名」——半截的 Gate 与完整
# 的 Gate 在那些断言上长得一模一样，于是这条测试会绿在一段根本没走完的正文上。
if grep -q 'command not found' "${WORK}/err"; then
  fail "缺工具那一跑里有命令找不到：造的 PATH 不够脚本自己跑起来——$(cat "${WORK}/err")"
fi

# 3：清单是数出来的，不是抄的。
grep -q "git ls-files '\*\.sh'" "${SCRIPT}" ||
  fail "shellcheck 的清单不是从树里数出来的（找不到 git ls-files）"
for glob in 'deploy/scripts/tests/*.test.sh' '.github/scripts/tests/*.test.sh' 'scripts/eval/*.test.sh'; do
  grep -qF "${glob}" "${SCRIPT}" ||
    fail "脚本测试的清单少了 ${glob}"
done

# 4：工作流调这条脚本，且不再自带那三段正文。
grep -q 'bash deploy/scripts/check-entry-scripts.sh' "${WORKFLOW}" ||
  fail "工作流没有调这条脚本，两边的正文就会各自漂"
if grep -q 'mapfile -t scripts' "${WORKFLOW}"; then
  fail "工作流里还留着一份 shellcheck 正文；两份正文会漂，而漂出来的差就是本地绿、上游红"
fi
if grep -q 'PSParser' "${WORKFLOW}"; then
  fail "工作流里还留着一份 bootstrap.ps1 解析正文"
fi

# 5：镜像里装着 shellcheck，这一格在沙箱里才有东西判。
# 判的是运行段 apt 清单里的那一行；少一个字母就是恒 exit 2。
if ! grep -qE '^[[:space:]]+shellcheck[[:space:]]*\\?$' "${ROOT}/Dockerfile"; then
  fail "Dockerfile 的运行段没装 shellcheck：沙箱里的变更验证会在这一格恒为 exit 2，一段脚本都没扫过"
fi

# 6：一条脚本测试自己退 4（「这一格只在平台宿主上判」）时，门禁点名它、接着跑完其
#    余的，最后不拦；退 1 仍然红。判的是「绿」与「没在这台机器上判」在读数面分得开：
#    只印一句「三段都通过」，读的人会把那一格读成判过了——而它恰恰是没人判过的那一格。
#
#    树是造的：这条测试要的是门禁对退出码的处置，不是那些脚本测试各自的断言。工具台
#    也全是假的——真 helm/kubectl 在这里只决定「这一格跑不跑得起来」，与判词无关。
mkdir -p "${WORK}/tools" "${WORK}/trees"
for tool in git shellcheck helm kubectl promtool psql pwsh python3; do
  printf '#!/usr/bin/env bash\nexit 0\n' >"${WORK}/tools/${tool}"
  chmod +x "${WORK}/tools/${tool}"
done
# git 只被问 `ls-files '*.sh'`：给一个文件就够，shellcheck 那一格是假的。
cat >"${WORK}/tools/git" <<'EOF'
#!/usr/bin/env bash
printf 'deploy/scripts/check-entry-scripts.sh\n'
EOF
chmod +x "${WORK}/tools/git"

# mk_tree <目录> <平台测试的正文>：一份最小的树，三处测试目录各一条，平台那一条的
# 正文由调用方给——退 4 与退 1 的差只在它身上。
mk_tree() {
  local dir="$1" platform="$2"
  mkdir -p "${dir}/deploy/scripts/tests" "${dir}/.github/scripts/tests" "${dir}/scripts/eval"
  cp "${SCRIPT}" "${dir}/deploy/scripts/check-entry-scripts.sh"
  printf '#!/usr/bin/env bash\necho "ok - a"\nexit 0\n' \
    >"${dir}/deploy/scripts/tests/a-pass.test.sh"
  printf '%s\n' "${platform}" >"${dir}/deploy/scripts/tests/b-platform.test.sh"
  # 跑过就留一个记号：退 4 被跳过之后，后面那一条要真的还在跑。
  printf '#!/usr/bin/env bash\nprintf "ran\\n" >"${MARKER}"\nexit 0\n' \
    >"${dir}/.github/scripts/tests/c-runs-after.test.sh"
  printf '#!/usr/bin/env bash\nexit 0\n' >"${dir}/scripts/eval/d-pass.test.sh"
}

# 6a：退 4 —— 点名跳过、其余照跑、整段仍是 0。
MARKER="${WORK}/c-ran"
export MARKER
mk_tree "${WORK}/trees/platform" '#!/usr/bin/env bash
echo "quota: 本机连不上集群，这一格只在平台宿主上判。" >&2
exit 4'
COGNEVA_TEST_DATABASE_URL=postgres://x/y \
  PATH="${WORK}/tools:${PATH}" \
  bash "${WORK}/trees/platform/deploy/scripts/check-entry-scripts.sh" \
  >"${WORK}/skip-out" 2>"${WORK}/skip-err"
skip_status=$?
if [ "${skip_status}" -ne 0 ]; then
  fail "退 4 的脚本测试把门禁拦红了（退出码 ${skip_status}）：换台机器就有答案的格子，不该让每次推送红在一条与改动无关的读数上"
fi
grep -q 'SKIP  deploy/scripts/tests/b-platform.test.sh' "${WORK}/skip-out" ||
  fail "退 4 的脚本测试没有被点名跳过：$(cat "${WORK}/skip-out")"
grep -q '没在这台机器上判' "${WORK}/skip-err" ||
  fail "退 4 的脚本测试没有进「没在这台机器上判」那一栏：$(cat "${WORK}/skip-err")"
if ! grep -q 'b-platform.test.sh' "${WORK}/skip-err"; then
  fail "「没在这台机器上判」那一栏没有点名是哪一格：$(cat "${WORK}/skip-err")"
fi
[ -f "${MARKER}" ] ||
  fail "退 4 之后剩下的脚本测试没有接着跑：跳过一格变成了跳过这一整段"
grep -q '三段都通过；上面点名的格子没在这台机器上判' "${WORK}/skip-out" ||
  fail "收尾没有把「没判过」与「绿」分开说：$(tail -n 3 "${WORK}/skip-out")"

# 6b：同一条退 1 仍是红——这一条是 6a 的反例，防「一律当跳过」。
mk_tree "${WORK}/trees/red" '#!/usr/bin/env bash
echo "FAIL: 判了，不合" >&2
exit 1'
COGNEVA_TEST_DATABASE_URL=postgres://x/y \
  PATH="${WORK}/tools:${PATH}" \
  bash "${WORK}/trees/red/deploy/scripts/check-entry-scripts.sh" \
  >"${WORK}/red-out" 2>"${WORK}/red-err"
red_status=$?
if [ "${red_status}" -ne 1 ]; then
  fail "退 1 的脚本测试没有把门禁判红（退出码 ${red_status}）：跳过与红混成一格，真红了也会被当没判"
fi
grep -q 'b-platform.test.sh:1:1: failed' "${WORK}/red-err" "${WORK}/red-out" ||
  fail "退 1 的那一格没有被点名为红：$(tail -n 3 "${WORK}/red-err")"

if [ "${failures}" -gt 0 ]; then
  printf '%d 条不满足\n' "${failures}" >&2
  exit 1
fi
echo "ok: check-entry-scripts 是一份正文、两处调用，缺失点名退 2，镜像里有 shellcheck"
