#!/usr/bin/env bash
# 工作台的版式与可拖拽：四格按数据排，左栏与页签能换序、两栏能拖宽。
#
# 这一面答四问（投入 产出 能力 项目），版式意图由后端在数据里声明——`priority`
# 决定先后，三档 `span` 决定在 12 列网格里横跨几列。前端只做「格 → 网格位置」的
# 确定性映射。这一层一旦自己发明版式（按数组下标排、按断点反推跨度），后端说要
# 放大的那一格就放不大，而没人看得出是谁改的。
#
# 拖拽那几条防的坏法也都是静默的：拖完不存，刷新回到原样；存下的名单盖掉声明
# 名单，这一版新加的一面于是永远不出现；只剩中间一条竖线能拖，右抽屉与左栏各自
# 定死。所以夹具逐段剪断，每剪一刀都必须红。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
view="${repo}/web/src/views/WorkbenchView.tsx"
hook="${repo}/web/src/hooks/useBreakpoint.ts"
store="${repo}/web/src/shell/store.ts"
bar="${repo}/web/src/shell/ActivityBar.tsx"
tabs="${repo}/web/src/shell/Tabs.tsx"
drawer="${repo}/web/src/shell/ExplainDrawer.tsx"

check_view() {
  local f="$1"
  [ -f "${f}" ] || { echo "missing: ${f}"; return 1; }
  # 1. 四格来自一次读数：这一层不自己拼几份。
  grep -qF 'getWorkbench' "${f}" \
    || { echo "工作台没有读 /api/v1/workbench：四格回到各自取数，四份响应各有各的时刻"; return 1; }
  for stale in listAgents getVenues getTaskSummary ConversationPane; do
    if grep -qF "${stale}" "${f}"; then
      echo "工作台里还有 ${stale}：四格之外的东西（员工/场地/任务/会话）都搬走了"; return 1
    fi
  done
  # 2. 先后读数据，不读数组下标。
  grep -qF 'priority' "${f}" \
    || { echo "工作台没有按 priority 排序：后端说哪一格该在前，界面不认"; return 1; }
  # 3. 三档跨度都接到网格上：跨度按当前断点取，断点由 hook 回答。
  grep -qF 'span[tier]' "${f}" \
    || { echo "工作台没有按断点取跨度：三档紧窄是各格自己的版式意图"; return 1; }
  grep -qF 'gridColumn' "${f}" \
    || { echo "工作台没有把跨度写成 grid-column：版式没有落到网格上"; return 1; }
  grep -qF 'useBreakpoint' "${f}" \
    || { echo "工作台没有回答「现在在哪一档」：三档跨度没有一处可以被选中"; return 1; }
  # 4. 没量到的读数印破折号，不印 0。
  grep -qF "'—'" "${f}" \
    || { echo "没量到的读数没有印成破折号：读者会把「还没接上」读成「量出来是零」"; return 1; }
}

check_breakpoint() {
  local f="$1"
  [ -f "${f}" ] || { echo "missing: ${f}"; return 1; }
  # 三档都要回答得出来：窄是默认，中与宽各有一条媒体查询，窗口改了要跟着变。
  for tier in "'narrow'" "'mid'" "'wide'"; do
    grep -qF -- "return ${tier};" "${f}" \
      || { echo "断点没有 ${tier} 这一档：窄屏或宽屏上格子挤成一列"; return 1; }
  done
  for query in MID_QUERY WIDE_QUERY; do
    grep -qF -- "${query}" "${f}" \
      || { echo "断点没有声明 ${query}：三条线的界会散落在别处"; return 1; }
  done
  grep -qF "addEventListener('change'" "${f}" \
    || { echo "断点只在初次渲染时量一次：拖动窗口之后排法不会跟着变"; return 1; }
}

check_reorder() {
  local f="$1"
  [ -f "${f}" ] || { echo "missing: ${f}"; return 1; }
  # 拖得动：元素本身可拖，落到另一格上就换序。
  grep -qF 'draggable' "${f}" || { echo "${f##*/} 里的格子拖不动"; return 1; }
  grep -qF 'onDrop' "${f}" || { echo "${f##*/} 收到落点也不换序"; return 1; }
}

check_store() {
  local f="$1"
  [ -f "${f}" ] || { echo "missing: ${f}"; return 1; }
  # 存下来的先后叠在声明名单上，不是盖掉它：盖掉之后这一版新加的面永远不出现。
  grep -qF 'export function mergeActivityOrder' "${f}" \
    || { echo "store 里没有 mergeActivityOrder：拖出来的先后与声明名单之间没有合并规则"; return 1; }
  grep -qF 'const ACTIVITY_ORDER_STORAGE_KEY' "${f}" \
    || { echo "活动栏先后没有具名存储键：写入与读出各写一遍字面量，漂了就都不报错"; return 1; }
  grep -qF 'const DRAWER_STORAGE_KEY' "${f}" \
    || { echo "抽屉宽度没有具名存储键：拖过的宽度刷新就没了"; return 1; }
}

check_drawer() {
  local f="$1"
  [ -f "${f}" ] || { echo "missing: ${f}"; return 1; }
  grep -qF 'drawerWidth' "${f}" \
    || { echo "右抽屉的宽度没有读 store：拖过之后刷新就回到写死的那个数"; return 1; }
  grep -qF 'style={{ width }}' "${f}" \
    || { echo "右抽屉的宽度写死在类名里：只有中间一条竖线能拖"; return 1; }
  grep -qF 'onPointerDown={startDrag}' "${f}" \
    || { echo "右抽屉左沿不是抓带：拖不动"; return 1; }
}

# 真树先判：夹具全绿而真树红，说明这条测试只在测自己。
check_view "${view}" || { echo "FAIL: 真树上的工作台" >&2; exit 1; }
check_breakpoint "${hook}" || { echo "FAIL: 真树上的断点" >&2; exit 1; }
check_store "${store}" || { echo "FAIL: 真树上的 store" >&2; exit 1; }
check_reorder "${bar}" || { echo "FAIL: 真树上的活动栏" >&2; exit 1; }
check_reorder "${tabs}" || { echo "FAIL: 真树上的页签" >&2; exit 1; }
check_drawer "${drawer}" || { echo "FAIL: 真树上的右抽屉" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 剪断一：工作台把会话搬回来。
sed 's/^export function WorkbenchView/import { ConversationPane } from "@\/conversation\/ConversationPane";\nexport function WorkbenchView/' \
  "${view}" > "${work}/with-chat.tsx"
if cmp -s "${view}" "${work}/with-chat.tsx"; then
  echo "FAIL: 第一段的改动没有落到文件上" >&2
  exit 1
fi
if check_view "${work}/with-chat.tsx" >/dev/null 2>&1; then
  echo "FAIL: 工作台上重新画会话之后仍然绿" >&2
  exit 1
fi

# 剪断二：三档跨度塌成一档（不论断点，一律用宽屏那一档）。
sed 's/span\[tier\]/span.wide/' "${view}" > "${work}/one-tier.tsx"
if cmp -s "${view}" "${work}/one-tier.tsx"; then
  echo "FAIL: 第二段的改动没有落到文件上" >&2
  exit 1
fi
if check_view "${work}/one-tier.tsx" >/dev/null 2>&1; then
  echo "FAIL: 三档跨度塌成一档之后仍然绿" >&2
  exit 1
fi

# 剪断二之二：断点少一档（只剩窄与宽，中屏落到窄的那一档）。
python3 - "$hook" "$work/two-tier.ts" <<'PY'
import sys
src = open(sys.argv[1]).read()
src = src.replace("  if (window.matchMedia(MID_QUERY).matches) return 'mid';\n", "")
open(sys.argv[2], 'w').write(src)
PY
if cmp -s "${hook}" "${work}/two-tier.ts"; then
  echo "FAIL: 第二段之二 的改动没有落到文件上" >&2
  exit 1
fi
if check_breakpoint "${work}/two-tier.ts" >/dev/null 2>&1; then
  echo "FAIL: 中屏那一档没了之后仍然绿" >&2
  exit 1
fi

# 剪断三：没量到的读数印成 0。
sed "s/'—'/'0'/" "${view}" > "${work}/zero.tsx"
if cmp -s "${view}" "${work}/zero.tsx"; then
  echo "FAIL: 第三段的改动没有落到文件上" >&2
  exit 1
fi
if check_view "${work}/zero.tsx" >/dev/null 2>&1; then
  echo "FAIL: 没量到的读数印成 0 之后仍然绿" >&2
  exit 1
fi

# 剪断四：存下来的先后盖掉声明名单（新加的面永远不出现）。
python3 - "$store" "$work/no-merge.ts" <<'PY'
import sys
src = open(sys.argv[1]).read()
start = src.index('export function mergeActivityOrder(')
end = src.index('\n}\n', start) + 3
src = src[:start] + 'export function mergeOrder(saved, declared) {\n  return [...(saved ?? declared)];\n}\n' + src[end:]
open(sys.argv[2], 'w').write(src)
PY
if cmp -s "${store}" "${work}/no-merge.ts"; then
  echo "FAIL: 第四段的改动没有落到文件上" >&2
  exit 1
fi
if check_store "${work}/no-merge.ts" >/dev/null 2>&1; then
  echo "FAIL: 合并规则没了之后仍然绿" >&2
  exit 1
fi

# 剪断五：右抽屉的宽度回到写死的类名上。
sed 's/style={{ width }}/className="w-[340px]"/' "${drawer}" > "${work}/fixed-drawer.tsx"
if cmp -s "${drawer}" "${work}/fixed-drawer.tsx"; then
  echo "FAIL: 第五段的改动没有落到文件上" >&2
  exit 1
fi
if check_drawer "${work}/fixed-drawer.tsx" >/dev/null 2>&1; then
  echo "FAIL: 抽屉宽度写死之后仍然绿" >&2
  exit 1
fi

# 剪断六：活动栏的格子拖不动。
sed 's/^            draggable$//' "${bar}" > "${work}/no-drag.tsx"
if cmp -s "${bar}" "${work}/no-drag.tsx"; then
  echo "FAIL: 第六段的改动没有落到文件上" >&2
  exit 1
fi
if check_reorder "${work}/no-drag.tsx" >/dev/null 2>&1; then
  echo "FAIL: 活动栏拖不动之后仍然绿" >&2
  exit 1
fi

echo "web-workbench-layout.test: all checks passed"
