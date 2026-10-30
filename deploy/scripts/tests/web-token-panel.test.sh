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
# 后三段判的是另外三件事，坏起来同样是静默的：额度窗整段消失（花销面答得出「花了
# 多少」却答不出「还够不够」）、时区名消失（本地时刻被当成 UTC 读）、执行者的名字
# 与网关的闭集各写各的（新名字被报成笔误，旧名字的读法永远不上屏）。
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
  rm -rf "${work}/web" "${work}/crates"
  mkdir -p "${work}/web/src/api" "${work}/web/src/hooks" "${work}/web/src/overlay" \
    "${work}/web/src/topology" "${work}/crates/cog-gateway/src"
  # 执行者的闭集：第 7 段跨这条边界比名单，所以夹具两侧都要有。
  cat >"${work}/crates/cog-gateway/src/actor_labels.rs" <<'EOF'
pub const ACTOR_LABELS: &[&str] = &[
    "eval",
    "memory",
    "web_chat",
];

pub const ACTOR_LABEL_PREFIXES: &[&str] = &["agent:", "self_review:"];

/// Where a caller that declared no usable actor lands.
pub const UNKNOWN_ACTOR: &str = "unknown";
EOF
  cat >"${work}/web/src/topology/format.ts" <<'EOF'
export function localZoneLabel(): string {
  return 'UTC+8';
}
EOF
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
import { useUsage, useSeries, useOutcomes, useAccount, usePool } from '@/hooks/useUsage';
import { localZoneLabel } from '@/topology/format';
const ACTOR_MEANINGS: Record<string, string> = {
  eval: '评测跑批',
  memory: '记忆读写',
  web_chat: 'WebUI 对话',
  unknown: '没声明自己是谁',
};
export function TokenPanel() {
  const { usage } = useUsage();
  const series = useSeries();
  const outcomes = useOutcomes();
  const account = useAccount();
  const pace = usePool().llm_pool?.pace ?? null;
  return <div>{usage?.covered_since}{series?.covered_until}{outcomes?.since}{outcomes?.until}{account?.since}{account?.until}{pace ?? "?"} {localZoneLabel()} {ACTOR_MEANINGS.eval}</div>;
}
EOF
  cat >"${work}/web/src/hooks/useUsage.ts" <<'EOF'
import { getTokenUsage, getTokenUsageSeries, getTokenOutcomes, getTokenAccount, getClusterOverview } from '@/api/client';
export function useUsage() {
  return getTokenUsage(24);
}
export function useSeries() {
  return getTokenUsageSeries(24);
}
export function useOutcomes() {
  return getTokenOutcomes(24);
}
export function useAccount() {
  return getTokenAccount(24);
}
export function usePool() {
  return getClusterOverview(24);
}
EOF
  cat >"${work}/web/src/api/client.ts" <<'EOF'
export const PATH = '/api/v1/observability/token-usage';
export const SERIES = '/api/v1/observability/token-usage/series';
export const OUTCOMES = '/api/v1/observability/token-usage/outcomes';
export const ACCOUNT = '/api/v1/observability/token-usage/account';
export async function getTokenUsage(hours: number) {
  return fetch(`${PATH}?hours=${hours}`);
}
export async function getTokenUsageSeries(hours: number) {
  return fetch(`${SERIES}?hours=${hours}`);
}
export async function getTokenOutcomes(hours: number) {
  return fetch(`${OUTCOMES}?hours=${hours}`);
}
export async function getTokenAccount(hours: number) {
  return fetch(`${ACCOUNT}?hours=${hours}`);
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

# 4. 剪掉区间字段（三条读面那几格都拿掉：任何一处读到它都会让这一门绿）。
fixture
sed -i 's/{usage?.covered_since}{series?.covered_until}{outcomes?.since}{outcomes?.until}{account?.since}{account?.until}/花销/' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "花销"
expect_red "没读区间字段" "没有读它实际答出的区间"

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

# 7. 第二条读面（按窗口分行）被剪掉，第一条还在：整块时间轴可以不留痕迹地消失，
#    而第一条读面一律绿。这一条判的是「两条各判一遍」这件事本身。
fixture
cat >"${work}/web/src/hooks/useUsage.ts" <<'EOF'
import { getTokenUsage, getTokenOutcomes, getTokenAccount } from '@/api/client';
export function useUsage() {
  return getTokenUsage(24);
}
export function useSeries() {
  return null;
}
export function useOutcomes() {
  return getTokenOutcomes(24);
}
export function useAccount() {
  return getTokenAccount(24);
}
EOF
confirm "${work}/web/src/hooks/useUsage.ts" "return null"
expect_red "只剩第一条读面" "getTokenUsageSeries"

# 8. 结局那一条读面被剪掉，另外两条还在：花销面会一直显示「没花多少」，
#    而「为什么没花」那一块可以整块消失。这一条判的是第三条读面各判一遍。
fixture
cat >"${work}/web/src/hooks/useUsage.ts" <<'EOF'
import { getTokenUsage, getTokenUsageSeries, getTokenAccount } from '@/api/client';
export function useUsage() {
  return getTokenUsage(24);
}
export function useSeries() {
  return getTokenUsageSeries(24);
}
export function useOutcomes() {
  return null;
}
export function useAccount() {
  return getTokenAccount(24);
}
EOF
confirm "${work}/web/src/hooks/useUsage.ts" "return null"
expect_red "只剩前两条读面" "getTokenOutcomes"

# 9. 省下账与判定账被剪掉，另外三条还在：「根本没发起」与「本来在花、现在停了」
#    这两句话会整块消失，而那三种零花销从此只有一种读得出来。这一条判的是
#    第四条读面各判一遍——它和前三条答的不是同一件事，所以不能由它们代判。
fixture
cat >"${work}/web/src/hooks/useUsage.ts" <<'EOF'
import { getTokenUsage, getTokenUsageSeries, getTokenOutcomes } from '@/api/client';
export function useUsage() {
  return getTokenUsage(24);
}
export function useSeries() {
  return getTokenUsageSeries(24);
}
export function useOutcomes() {
  return getTokenOutcomes(24);
}
export function useAccount() {
  return null;
}
EOF
confirm "${work}/web/src/hooks/useUsage.ts" "return null"
expect_red "只剩前三条读面" "getTokenAccount"

# 10. 额度窗整段剪掉：花销面仍答得出「花了多少」，却答不出「这一窗还剩多少、
#     什么时候换窗」——那两格在订阅制上游没有单价可乘，它们就是成本读数本身。
#     上面九条此时全绿：路由在、调用点在、区间字段也在。
fixture
sed -i 's/usePool().llm_pool?.pace ?? null/null/' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "const pace = null;"
expect_red "没读额度窗" "没有读上游额度窗"

# 11. 只留池子、剪掉 `.pace`：快照照样读、池子照样显示「可用」，而「这一窗还剩
#     多少」无声地没了。这一条判的是那两半各判一遍，不是「读了 llm_pool 就算数」。
#     判词固定报两半（`缺 llm_pool, .pace`，不论实际缺哪一个），所以这里只认那句
#     「没有读上游额度窗」，认不到具体字段名。
fixture
sed -i 's/usePool().llm_pool?.pace ?? null/usePool().llm_pool ?? null/' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "llm_pool ?? null"
expect_red "读了池子没读窗" "没有读上游额度窗"

# 12. 时区名剪掉：面板照样画柱、照样给读数，而每一个时刻都是浏览器按本地时区
#     渲染的，账本与日志是 UTC。差八小时的时候同一件事在两处写成两个时刻，读的
#     人只能得出「其中一个错了」——现场已经这样误判过一次。
fixture
sed -i 's/ {localZoneLabel()}//' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" 'pace ?? "?"} {ACTOR_MEANINGS.eval}'
expect_red "没有时区调用点" "有定义但没有调用点"

# 13. 时区定义剪掉、调用点还在：名字从别的文件里来不了，那个调用点读的是空。
fixture
printf 'export const nothing = 1;\n' >"${work}/web/src/topology/format.ts"
confirm "${work}/web/src/topology/format.ts" 'export const nothing'
expect_red "没有时区定义" "没有地方定义 localZoneLabel()"

# 14. 闭集添了一个执行者、这一屏没有它的读法：那个名字会显示成「不在网关登记的
#     闭集里，可能是新调用方或写错了名字」——一次正常的扩词被报成一次事故。
fixture
sed -i 's/    "web_chat",/    "web_chat",\n    "wiki",/' "${work}/crates/cog-gateway/src/actor_labels.rs"
confirm "${work}/crates/cog-gateway/src/actor_labels.rs" '"wiki"'
expect_red "闭集多一个名字" "这一屏没有它的读法"

# 15. 反向：这一屏留着一个闭集里没有的名字。那段读法再也不会出现在任何一行上，
#     而它看起来仍然是对的——两份名单各写各的，谁也不会红第二遍。
fixture
sed -i "s/  unknown: '没声明自己是谁',/  unknown: '没声明自己是谁',\n  planner: '计划者',/" "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" 'planner:'
expect_red "闭集少一个名字" "里没有这个名字"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-token-panel.test: all checks passed"
