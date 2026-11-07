#!/usr/bin/env bash
# 工作台顶上「人」那一格数的是**此刻在岗**，不是名册行数。
#
# 名册回答「存不存在」，这一格问「在不在岗」：`/api/v1/agents` 上既有心跳上的
# 人（`live: true`），也有说过话、招进来、这一刻池里没有的人（`live: false`）。
# 直接在整份名单上取 `.length` 不会报错、不会空屏，只是把休息中的人算成在岗——
# 2026-11-07 那一格就是这么读的，读数与员工页上的「在岗 N · 休息中 M」对不上。
#
# 这条测试防的坏法同样是静默的：改回整份名单，界面照常渲染。所以夹具是三段
# 剪断——不筛、筛了不用、用了但名字仍念整份名单——每剪一段都必须红。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
band="${repo}/web/src/views/WorkbenchBand.tsx"

check() {
  local f="$1"
  [ -f "${f}" ] || { echo "missing: ${f}"; return 1; }
  # 1. 从名单里筛出在岗的那些。
  grep -qF 'roster.filter((a) => a.live !== false)' "${f}" \
    || { echo "「人」这一格没有从名册里筛出在岗的人"; return 1; }
  # 2. 数字取的是筛完的那一份。
  grep -qF 'count(agents.data !== null, onDuty.length)' "${f}" \
    || { echo "「人」这一格数的还是整份名册"; return 1; }
  # 3. 名字念的也是筛完的那一份：数对了、念错的名单还是错的。
  grep -qF 'onDuty.map(' "${f}" \
    || { echo "「人」这一格念的是整份名册"; return 1; }
}

# 真树先判：夹具全绿而真树红，说明这条测试只在测自己。
check "${band}" || { echo "FAIL: 真树上的工作台带子" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 剪断一：不筛。
sed '/roster.filter((a) => a.live !== false)/d' "${band}" > "${work}/no-filter.tsx"
if check "${work}/no-filter.tsx" >/dev/null 2>&1; then
  echo "FAIL: 去掉筛选之后仍然绿" >&2
  exit 1
fi

# 剪断二：筛了不用，数字仍取整份名册。
sed 's/count(agents\.data !== null, onDuty\.length)/count(agents.data !== null, roster.length)/' \
  "${band}" > "${work}/counts-roster.tsx"
if cmp -s "${band}" "${work}/counts-roster.tsx"; then
  echo "FAIL: 第二段的改动没有落到文件上" >&2
  exit 1
fi
if check "${work}/counts-roster.tsx" >/dev/null 2>&1; then
  echo "FAIL: 数字改回整份名册之后仍然绿" >&2
  exit 1
fi

# 剪断三：数对了，念的仍是整份名册。
sed 's/onDuty\.map(/roster.map(/' "${band}" > "${work}/names-roster.tsx"
if cmp -s "${band}" "${work}/names-roster.tsx"; then
  echo "FAIL: 第三段的改动没有落到文件上" >&2
  exit 1
fi
if check "${work}/names-roster.tsx" >/dev/null 2>&1; then
  echo "FAIL: 名字改回整份名册之后仍然绿" >&2
  exit 1
fi

echo "web-workbench-band.test: all checks passed"
