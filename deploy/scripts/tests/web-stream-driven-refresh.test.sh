#!/usr/bin/env bash
# check-web-stream-driven-refresh.sh 的判据里，与本轮改动相关的那一条：令牌面把
# 池子的读数（额度窗）和花销几读一起交给 `usage` 帧。
#
# 坏起来是静默的：额度窗只跟 30 秒兜底，账已经往前走了一笔而额度窗还停在上一笔，
# 同一屏两块各说一个时刻——没有任何东西报错，屏幕只是慢一拍而且自相矛盾。
#
# 夹具用**真树的 web/src**（这道门只读文本，不解析 import）：抄一份骨架就等于门
# 在测自己，而这块恰恰是要跟着产品文件走的。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-stream-driven-refresh.sh"
fail() { echo "FAIL: $*"; exit 1; }

# 这道门不入可执行位（CI 用 `bash <脚本>` 调它），所以判的是文件在不在，不是 -x。
[ -f "${checker}" ] || fail "missing: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture() {
  rm -rf "${work}/web"
  mkdir -p "${work}/web"
  cp -r "${repo}/web/src" "${work}/web/src"
}

expect_red() { # name, substring
  local out rc
  rc=0
  out="$(bash "${checker}" "${work}" 2>&1)" || rc=$?
  [ "${rc}" -ne 0 ] || fail "「$1」应该红，实际绿了（这条判据抓不住它）"
  case "${out}" in
    *"$2"*) ;;
    *) fail "「$1」红了但判词不对：期望含「$2」，实际是「${out}」" ;;
  esac
  echo "ok(red): $1"
}

# 1. 额度窗不再跟着 `usage` 帧重读：账往前走了一笔，额度窗还停在上一笔。
fixture
python3 - "${work}/web/src/views/TokenView.tsx" <<'PY'
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
before = "account.refresh, overview.refresh]"
after = "account.refresh]"
if before not in src:
    sys.exit("fixture: TokenView.tsx 里找不到「account.refresh, overview.refresh]」")
open(path, "w", encoding="utf-8").write(src.replace(before, after))
PY
grep -qF "account.refresh, overview.refresh]" "${work}/web/src/views/TokenView.tsx" \
  && fail "改动没落到 TokenView.tsx：overview.refresh 还挂在订阅上"
expect_red "额度窗不跟帧" "没把池子的那份读数跟花销几读一起交给"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
#
# 这道门要 `web/node_modules/.bin/tsc`（它把 `frameTick` 编译出来在 node 里实跑），
# 而跑这份测试的作业（Entry Scripts Lint）有意不装 web 依赖——那份额外的工具链只
# 活在 Web Build 作业里。于是：
#   - tsc 在位（本机、以及装了依赖的作业）：照旧跑真树，红就是红。
#   - tsc 不在位：明写一行「谁在替它看」，并且**把那个前提也判一次**——真树由
#     ci.yml 里 Web Build 那一跑同一条门的步骤覆盖。那一步被删掉时这里立刻红，
#     跳过就不会悄悄变成「没人看」，而「跳过」在作业摘要上与「通过」同形。
if [ -x "${repo}/web/node_modules/.bin/tsc" ]; then
  got=0
  bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
  [ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }
  echo "ok   - 真树（本作业有 tsc）"
else
  grep -qF 'bash deploy/scripts/check-web-stream-driven-refresh.sh' \
    "${repo}/.github/workflows/ci.yml" \
    || fail "本作业没有 tsc，而 ci.yml 也不再跑这条门：真树没人看了"
  echo "skip - 真树：本作业没有 web/node_modules/.bin/tsc；真树由 ci.yml 的 Web Build 作业跑同一条门"
fi

echo "web-stream-driven-refresh.test: all checks passed"
