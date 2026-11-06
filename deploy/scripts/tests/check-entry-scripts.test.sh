#!/usr/bin/env bash
# 「入口脚本」这一格的检查只有一份正文，两处调用者共享：上游 CI 的 `entry-scripts`
# 那一格，以及树自己的变更验证（名字以 `check-` 开头，被当成一条门禁问）。
#
# 这条测试判的是「共享」与「跑不了要说明白」这两件事，不是那 136 条脚本测试各自
# 对不对——那些要 helm、kubectl、promtool、pwsh 和一张真库，而这里要能在没有它们
# 的机器上跑。判四件事：
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

# 1 与 2：缺工具、缺库。PATH 里只有假工具台和系统基础目录，/usr/local/bin 不在其中
# （helm 与 kubectl 装在那里），promtool 与 pwsh 这台机器上本来就没有。
env -u COGNEVA_TEST_DATABASE_URL \
  PATH="${WORK}/bin:/usr/bin:/bin" \
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

if [ "${failures}" -gt 0 ]; then
  printf '%d 条不满足\n' "${failures}" >&2
  exit 1
fi
echo "ok: check-entry-scripts 是一份正文、两处调用，缺失点名退 2，镜像里有 shellcheck"
