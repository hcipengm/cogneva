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
    "${work}/web/src/topology" "${work}/web/src/ui" "${work}/web/src/styles" \
    "${work}/web/src/observability" "${work}/crates/cog-gateway/src"
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
  # 这一屏的骨架与真树同形：额度窗与累计在页顶（不随窗口变），下面一整组按窗口
  # ——花销 / 零花销（结局＋两本旁账）/ 谁在花（一张两级表）。夹具要是只有一条扁平
  # 的 div，第 9~12 段就没有可判的形状。
  cat >"${work}/web/src/overlay/TokenPanel.tsx" <<'EOF'
import { useUsage, useSeries, useOutcomes, useAccount, usePool } from '@/hooks/useUsage';
import { localZoneLabel } from '@/topology/format';
import { Note } from '@/ui/Note';
import { ActorTable } from './ActorTable';

const WINDOWS = [{ hours: 6, label: '6 小时' }];
const ACTOR_MEANINGS: Record<string, string> = {
  eval: '评测跑批',
  memory: '记忆读写',
  web_chat: 'WebUI 对话',
  unknown: '没声明自己是谁',
};
const UNKNOWN_ORIGIN = 'unknown';
const UNKNOWN_ACTOR_NAME = 'unknown';
const ACTOR: Dimension = {
  isPlaceholder: (value) => value === UNKNOWN_ACTOR_NAME,
};
const UPSTREAM: Dimension = {
  isPlaceholder: (value) => value === '-',
};
const API_STYLE: Dimension = {
  isPlaceholder: (value) => value === '-',
};
const ORIGIN: Dimension = {
  isPlaceholder: (value) => value === UNKNOWN_ORIGIN,
};
const DIMENSIONS = [ACTOR, UPSTREAM, API_STYLE, ORIGIN];
function registeredButAbsent(list: Member[], dim: Dimension): string[] {
  return Object.keys(dim.registry).filter(
    (value) => !seen.has(value) && !(dim.isPlaceholder?.(value) ?? false)
  );
}
function AllowanceWindow({ pool }: { pool: unknown }) {
  return <div>{String(pool)}</div>;
}
function OutcomeBreakdown() {
  return <div />;
}
function SpendAccount() {
  return <div />;
}
export function TokenPanel() {
  const { usage } = useUsage();
  const series = useSeries();
  const outcomes = useOutcomes();
  const account = useAccount();
  const pace = usePool().llm_pool?.pace ?? null;
  return (
    <div>
      <div>{counted.length}</div>
      <div>{usage?.covered_since}{series?.covered_until}{outcomes?.since}{outcomes?.until}{account?.since}{account?.until}</div>
      <AllowanceWindow pool={pace} />
      <h2>累计</h2>
      <section>
        <div>
          {WINDOWS.map((option) => (
            <button key={option.hours}>{option.label}</button>
          ))}
          <span>下面三块（花销 / 零花销 / 谁在花）都跟着它变，上面两节不跟</span>
        </div>
        <h3>花销</h3>
        <div>
          <h3>零花销</h3>
          <p>三段的单位不同，不能相加。</p>
          <OutcomeBreakdown />
          <SpendAccount />
        </div>
        <div>
          <h3>谁在花</h3>
          <ActorTable actors={[]} />
          <Note text="四格合起来才是唯一的一行">?</Note>
        </div>
      </section>
      <span>{pace ?? "?"} {localZoneLabel()} {ACTOR_MEANINGS.eval}</span>
    </div>
  );
}
EOF
  cat >"${work}/web/src/ui/Note.tsx" <<'EOF'
export function Note({ text, children }: { text: string; children?: unknown }) {
  return (
    <span role="note" onMouseEnter={() => {}} onFocus={() => {}}>
      {children}
      <span className="note-panel">{text}</span>
    </span>
  );
}
EOF
  cat >"${work}/web/src/styles/tokens.css" <<'EOF'
.note-panel {
  position: fixed;
  user-select: text;
}
EOF
  cat >"${work}/web/src/observability/usageRows.ts" <<'EOF'
export function actorTotals(actors: unknown[]): unknown[] {
  return actors;
}
export function cacheFormula(style: string, input: number, cached: number): string {
  return `${style} ${input} ${cached}`;
}
EOF
  # 两级表：一张表、两级行、列宽一处定义、失败格无条件出现。
  cat >"${work}/web/src/overlay/ActorTable.tsx" <<'EOF'
import { Fragment } from 'react';
import { actorTotals, cacheFormula } from '@/observability/usageRows';
import { Note } from '@/ui/Note';

const W = {
  actor: '17rem',
  share: '6.5rem',
  calls: '5.5rem',
  failed: '5rem',
  input: '7rem',
  output: '7rem',
  cached: '7rem',
  rate: '7.5rem',
  per: '7rem',
  latency: '5.5rem',
};

function registeredActorNames(): string[] {
  return ['eval', 'memory', 'web_chat'];
}

export function ActorTable({ actors }: { actors: any[] }) {
  const groups = actorTotals(actors) as any[];
  const grand = groups.reduce((sum: number, group: any) => sum + group.input + group.output, 0);
  const spenders = groups.filter((group: any) => group.input + group.output > 0);
  const absent = registeredActorNames();
  return (
    <div>
      <div aria-hidden>
        {spenders.map((group: any) => (
          <span key={group.actor} style={{ width: `${((group.input + group.output) / grand) * 100}%` }} />
        ))}
      </div>
      <table className="grid-table">
        <thead>
          <tr>
            <th style={{ width: W.actor }}>执行者</th>
            <th style={{ width: W.share }}>占比</th>
            <th style={{ width: W.calls }}>调用</th>
            <th style={{ width: W.failed }}>失败</th>
            <th style={{ width: W.input }}>输入</th>
            <th style={{ width: W.output }}>输出</th>
            <th style={{ width: W.cached }}>缓存</th>
            <th style={{ width: W.rate }}>缓存占比</th>
            <th style={{ width: W.per }}>每调用输入</th>
            <th style={{ width: W.latency }}>均耗</th>
          </tr>
        </thead>
        <tbody>
          {groups.map((group: any) => (
            <Fragment key={group.actor}>
              <tr>
                <td>{group.actor}</td>
                <td>{group.calls}</td>
                <td>{group.failed}</td>
                <td>
                  {group.styles.length === 1 ? (
                    <Note text={cacheFormula(group.styles[0], group.input, group.cached)}>{group.cached}</Note>
                  ) : (
                    <Note text="两种协议的 input 含义不同">分协议见下</Note>
                  )}
                </td>
                <td>
                  <Note text={perCallNote(group.input, group.calls)}>{group.calls}</Note>
                </td>
                <td>
                  <Note text={`Σ(每行均耗 × 该行调用) = ${group.latencyWeighted}`}>ms</Note>
                </td>
              </tr>
              {group.rows.map((row: any) => (
                <tr key={row.upstream}>
                  <td>{row.upstream}</td>
                </tr>
              ))}
            </Fragment>
          ))}
          {absent.map((name) => (
            <tr key={name}>
              <td>{name}</td>
              <td colSpan={9}>本窗口没有调用</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function perCallNote(tokens: number, calls: number): string {
  return `${tokens} / ${calls} 次调用`;
}
EOF
  # 环境那一维的闭集住在这份契约里：第 8 段要跨这条边界比占位符的字面量。
  mkdir -p "${work}/crates/cog-core/src/contract"
  cat >"${work}/crates/cog-core/src/contract/usage_ledger.rs" <<'EOF'
pub enum LlmOrigin {
    Unknown,
}

impl LlmOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            LlmOrigin::Unknown => "unknown",
        }
    }
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

# 16. 环境那一维不再声明谁是占位符：`unknown` 会重新被数成一个环境，屏幕上
#     「环境有 N 个」里就有一个指向不存在的地方。
fixture
sed -i '/isPlaceholder: (value) => value === UNKNOWN_ORIGIN,/d' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "const ORIGIN: Dimension = {"
expect_red "环境维没标占位符" "ORIGIN 没有声明谁是占位符"

# 17. 屏上抄的那个字面量与代码里那一臂漂开：屏幕不再认得出占位符，个数多出一个，
#     而两边各自看起来都对。
fixture
sed -i "s/const UNKNOWN_ORIGIN = 'unknown';/const UNKNOWN_ORIGIN = 'unattributed';/" \
  "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "unattributed"
expect_red "占位符字面量漂开" "两边漂开时屏幕不再认得出这个占位符"

# 18. 「登记了但本窗口没出现」那一段不剔占位符：`unknown` 会被列成「本窗口没有
#     调用」的环境，而它根本不是环境——正好把要分开的两件事合回去。
fixture
sed -i 's/(value) => !seen.has(value) \&\& !(dim.isPlaceholder?.(value) ?? false)/(value) => !seen.has(value)/' \
  "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "(value) => !seen.has(value)"
expect_red "缺席清单不剔占位符" "没有剔除占位符"

# 19. 那个数退回折叠表的每一行：占位符又被数成成员。这一条与上一条判的是两件事
#     ——一条判「清单剔了没有」，一条判「数是从哪一列来的」。
fixture
sed -i 's|<div>{counted.length}</div>|<div>{list.length}</div>|' \
  "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "<div>{list.length}</div>"
expect_red "计数退回全量" "又退回成 {list.length} 了"

# 20. DOM 元素上又出现原生 title=：提示框选不中、复制不了，而这一屏的每一句解释
#     都是要贴进工单的。组件自己的 title 参数（可见标题）不算，iframe 的无障碍
#     title 也不算——这一条判的是「提示框退回去了没有」。
fixture
sed -i 's|<span>{pace ??|<span title="读数来自折叠账">{pace ??|' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" 'title="读数来自折叠账"'
expect_red "原生 title 回来了" "DOM 元素上又出现了原生 title="

# 21. 注释组件不再画成页面内的面板：它退回成一个只有文字的壳，选不中、复制不了。
fixture
sed -i 's/role="note"/role="tip"/' "${work}/web/src/ui/Note.tsx"
confirm "${work}/web/src/ui/Note.tsx" 'role="tip"'
expect_red "注释不再是面板" "注释组件缺"

# 22. 面板的规则丢了「能选中」：面板照样浮出来，而里面的字拖不动。
fixture
sed -i '/user-select: text/d' "${work}/web/src/styles/tokens.css"
confirm "${work}/web/src/styles/tokens.css" "position: fixed"
expect_red "面板选不中" "缺 user-select: text"

# 23. 注释组件整个删掉：下一句要解释的话只能退回成 title=。
fixture
rm "${work}/web/src/ui/Note.tsx"
[ -f "${work}/web/src/ui/Note.tsx" ] && fail "删不掉 ${work}/web/src/ui/Note.tsx"
expect_red "没有注释组件" "可达图里没有导出 Note 的组件"

# 24. 窗口按钮挪回页头（额度窗与累计之上）：那两节一个是当刻快照、一个是整本
#     折叠账，都不随窗口变；按钮罩住它们等于说它们也会变。
fixture
sed -i '/<AllowanceWindow pool={pace} \/>/d' "${work}/web/src/overlay/TokenPanel.tsx"
sed -i 's|</section>|</section><AllowanceWindow pool={pace} />|' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" '</section><AllowanceWindow pool={pace} />'
expect_red "窗口按钮挪回页头" "窗口按钮排在了"

# 25. 组头那句话删掉：按钮还在组头上，但「下面哪几块跟、上面哪几节不跟」没有了，
#     读的人按下 6 小时只能自己猜哪些数字变了。
fixture
sed -i '/都跟着它变，上面两节不跟/d' "${work}/web/src/overlay/TokenPanel.tsx"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "<h3>花销</h3>"
expect_red "组头没写哪几块跟" "窗口那一排没有那句话"

# 26. 两级行拆成两张表：列宽与对齐只能靠两处手抄，迟早对不上。
fixture
sed -i 's|<table className="grid-table">|<table className="grid-table"></table><table className="grid-table">|' \
  "${work}/web/src/overlay/ActorTable.tsx"
confirm "${work}/web/src/overlay/ActorTable.tsx" "</table><table"
expect_red "两张表" "两级行不在同一张表里"

# 27. 一级行点不开到四格行：同一个执行者换过几条线路，这一屏上就没有地方说了。
fixture
sed -i 's/group.rows.map(/group.rows.forEach(/' "${work}/web/src/overlay/ActorTable.tsx"
confirm "${work}/web/src/overlay/ActorTable.tsx" "group.rows.forEach("
expect_red "点不开到四格行" "一级行点不开到四格行"

# 28. 失败那一格退回成条件渲染：零失败的行走少一格，它后面的数字整体左移一列。
fixture
sed -i 's|<td>{group.failed}</td>|<td>{group.failed > 0 \&\& group.failed}</td>|' \
  "${work}/web/src/overlay/ActorTable.tsx"
confirm "${work}/web/src/overlay/ActorTable.tsx" "group.failed > 0 && group.failed"
expect_red "失败格条件渲染" "失败那一格又变成条件渲染"

# 29. 一级行的缓存占比不再分协议：两种协议的 input 含义不同，先加起来再除，是一个
#     协议的分母除以另一个的分子。
fixture
sed -i 's/group.styles.length === 1/group.styles.length >= 1/' "${work}/web/src/overlay/ActorTable.tsx"
confirm "${work}/web/src/overlay/ActorTable.tsx" "group.styles.length >= 1"
expect_red "缓存占比混协议" "缓存占比没有分协议的限制"

# 30. 合计再算一遍：分摊条、一级行、占比的分母各算各的，迟早各说各的。
fixture
sed -i 's|  const grand = groups.reduce|  const again = actorTotals(actors);\n  const grand = groups.reduce|' \
  "${work}/web/src/overlay/ActorTable.tsx"
confirm "${work}/web/src/overlay/ActorTable.tsx" "const again = actorTotals(actors);"
expect_red "合计算两遍" "合计算了"

# 31. 判定账被挪到「谁在花」后面：同一件事的三条腿被拆成两节，少了哪一块，剩下的
#     两种零花销就会读成同一件事。
fixture
sed -i '/^          <SpendAccount \/>$/d' "${work}/web/src/overlay/TokenPanel.tsx"
sed -i 's|          <ActorTable actors={\[\]} />|          <ActorTable actors={[]} />\n          <SpendAccount />|' \
  "${work}/web/src/overlay/TokenPanel.tsx"
awk '/<SpendAccount \/>/{a=NR} /<ActorTable/{t=NR} END{exit !(a>t)}' \
  "${work}/web/src/overlay/TokenPanel.tsx" || fail "改动没落到 TokenPanel.tsx：SpendAccount 没挪到谁在花之后"
expect_red "判定账被隔开" "被「谁在花」隔到了后面"

# 32. 那句「单位不同、不能相加」删掉：尝试次数、决定次数、窗口个数并排在一起，
#     读者会把它当成一份花销去加。
fixture
sed -i '/不能相加/d' "${work}/web/src/overlay/TokenPanel.tsx"
grep -qF "不能相加" "${work}/web/src/overlay/TokenPanel.tsx" && fail "改动没落到 TokenPanel.tsx：不能相加 还在"
confirm "${work}/web/src/overlay/TokenPanel.tsx" "<h3>零花销</h3>"
expect_red "没写不能相加" "没有写清三段的单位不同"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-token-panel.test: all checks passed"
