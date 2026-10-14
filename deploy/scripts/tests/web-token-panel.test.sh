#!/usr/bin/env bash
# check-web-token-panel.sh 的判据：令牌花销那条路由必须有一格**从入口可达、
# 且被渲染出来**的消费者，而那一屏必须读响应的覆盖端点。
#
# 这条规则要防的坏法是**静默**的：路由在、契约测试绿、账上那行读 landed，
# 而整块面板可以消失而不让任何东西失败（那一行的字面量就是路由自己的路径串）。
# 所以夹具的形状是「一条完整的链」，然后**逐段剪断**，每剪一段必须红：
#   - 剪掉路径声明      → 前端这一侧没有声明这条读面
#   - 剪掉调用点        → 没有活的外部消费者
#   - 只 import 不渲染  → 没有一格渲染出来的组件用到它
#   - 剪掉覆盖端点      → 会把「这段没折叠」显示成「这期间没花销」
#   - 把她挪到不可达文件 → 死文件里的调用点不算消费者
# 每段改动都先确认**落到了文件上**（否则「改动没生效」与「门没抓住」不可区分）。
#
# 最后一条断言是在**真树**上跑：夹具全绿而真树红，说明这条门只是在测自己。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-token-panel.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ── 夹具：一条从入口到面板的链 ──
fixture() {
  rm -rf "${work}/web"
  mkdir -p "${work}/web/src/api" "${work}/web/src/hooks" "${work}/web/src/overlay"
  cat >"${work}/web/src/main.tsx" <<'EOF'
import App from './App';
render(<App />);
EOF
  cat >"${work}/web/src/App.tsx" <<'EOF'
import { TokenPanel } from './overlay/TokenPanel';
export default function App() {
  return <TokenPanel />;
}
EOF
  cat >"${work}/web/src/overlay/TokenPanel.tsx" <<'EOF'
import { useUsage } from '@/hooks/useUsage';
export function TokenPanel() {
  const { usage } = useUsage();
  return <div>{usage?.covered_since}</div>;
}
EOF
  cat >"${work}/web/src/hooks/useUsage.ts" <<'EOF'
import { getTokenUsage } from '@/api/client';
export function useUsage() {
  return getTokenUsage(24);
}
EOF
  cat >"${work}/web/src/api/client.ts" <<'EOF'
export const PATH = '/api/v1/observability/token-usage';
export async function getTokenUsage(hours: number) {
  return fetch(`${PATH}?hours=${hours}`);
}
EOF
}

# 跑门禁，要求 rc==1 且判词里出现 <substr>
expect_red() {
  local name="$1" substr="$2" got=0
  bash "${checker}" "${work}" >"${work}/out" 2>&1 || got=$?
  [ "${got}" -eq 1 ] || { cat "${work}/out" >&2; fail "${name}: exit ${got}, wanted 1"; }
  grep -qF "${substr}" "${work}/out" || {
    cat "${work}/out" >&2
    fail "${name}: 判词里没有「${substr}」"
  }
}

expect_green() {
  local name="$1" got=0
  bash "${checker}" "${work}" >"${work}/out" 2>&1 || got=$?
  [ "${got}" -eq 0 ] || { cat "${work}/out" >&2; fail "${name}: exit ${got}, wanted 0"; }
}

# 改完先确认改动真的落到了文件上，再判红。
confirm() {
  grep -qF "$2" "$1" || fail "改动没落到 ${1#${work}/}：找不到「$2」"
}

# 未改动的夹具：整条链在，必须绿。
fixture
expect_green "完整链路"

# 1. 剪掉路径声明。
fixture
sed -i "s#'/api/v1/observability/token-usage'#'/api/v1/observability/usage'#" "${work}/web/src/api/client.ts"
confirm "${work}/web/src/api/client.ts" "/usage'"
expect_red "没有路径声明" "没有声明这条读面的路径"

# 2. 剪掉调用点（函数还在，没人调）。
fixture
cat >"${work}/web/src/hooks/useUsage.ts" <<'EOF'
import { getTokenUsage } from '@/api/client';
export function useUsage() {
  return { usage: null, refresh: () => {} };
}
export const unused = getTokenUsage;
EOF
confirm "${work}/web/src/hooks/useUsage.ts" "export const unused"
expect_red "没有调用点" "没有活的外部消费者"

# 3. 只 import、不渲染。
fixture
cat >"${work}/web/src/App.tsx" <<'EOF'
import { TokenPanel } from './overlay/TokenPanel';
export default function App() {
  return <div />;
}
EOF
confirm "${work}/web/src/App.tsx" "return <div />"
expect_red "只 import 不渲染" "没有一格渲染出来的组件用到它"

# 4. 剪掉覆盖端点（取数与显示两处都没有）。
fixture
sed -i 's/{usage?.covered_since}/花销/' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "花销"
expect_red "没读覆盖端点" "没有读覆盖端点"

# 5. 调用点挪到一个没人 import 的文件里：不可达的调用点不算消费者。
fixture
cat >"${work}/web/src/hooks/useUsage.ts" <<'EOF'
export function useUsage() {
  return null;
}
EOF
cat >"${work}/web/src/orphan.tsx" <<'EOF'
import { getTokenUsage } from '@/api/client';
export function Orphan() {
  getTokenUsage(24);
  return <div />;
}
EOF
confirm "${work}/web/src/orphan.tsx" "getTokenUsage(24)"
expect_red "不可达的调用点" "没有活的外部消费者"

# 6. 入口什么也不 import：可达集里只剩入口自己，消费者自然不在里面。
fixture
printf 'export const nothing = 1;\n' >"${work}/web/src/main.tsx"
confirm "${work}/web/src/main.tsx" 'export const nothing'
expect_red "入口不接任何东西" "没有活的外部消费者"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-token-panel.test: all checks passed"
