#!/usr/bin/env bash
# check-web-observability-boards.sh 的判据：观测面必须是一排有读者的工具板。
#
# 这条门守的不是「页面长得对不对」，是**每块板有没有真读者、有没有把话说全**：
# 板条上写着「事件回放」而没人渲染、历史事件接口没有消费者、读满 1000 条被当成
# 总数、日志分页数被当成总量、轨迹少读了帧却按全量展示、模型花销在两个面各算一遍
# ——这些坏法全都不会报错，只会让人读到一句假话。
#
# 夹具是一条完整的链（入口 → 观测面 → 七块板 → client），然后逐段剪断，每剪一段
# 必须红，且判词要对得上。每段改动先确认**落到了文件上**，否则「改动没生效」与
# 「门没抓住」不可区分。最后在**真树**上跑一遍：夹具全绿而真树红，说明这条门只是
# 在测自己。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-observability-boards.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture() {
  rm -rf "${work}/web"
  mkdir -p "${work}/web/src/api" "${work}/web/src/views" "${work}/web/src/observability" \
    "${work}/web/src/topology"
  cat >"${work}/web/src/main.tsx" <<'EOF'
import App from './App';
render(<App />);
EOF
  cat >"${work}/web/src/App.tsx" <<'EOF'
import { ObservabilityView } from './views/ObservabilityView';
export default function App() {
  return <ObservabilityView />;
}
EOF
  cat >"${work}/web/src/views/ObservabilityView.tsx" <<'EOF'
import { getClusterOverview } from '@/api/client';
import { EventsBoard } from '@/observability/EventsBoard';
import { LogsBoard } from '@/observability/LogsBoard';
import { MetricsBoard } from '@/observability/MetricsBoard';
import { ProbesBoard } from '@/observability/ProbesBoard';
import { ReadingsBoard } from '@/observability/ReadingsBoard';
import { TracesBoard } from '@/observability/TracesBoard';
import { ClusterTopology } from '@/topology/ClusterTopology';

const BOARDS = [
  { id: 'overview' },
  { id: 'readings' },
  { id: 'topology' },
  { id: 'metrics' },
  { id: 'logs' },
  { id: 'traces' },
  { id: 'events' },
  { id: 'probes' },
];

export function ObservabilityView() {
  const board = 'overview';
  return (
    <div>
      {board === 'overview' && <OverviewBoard />}
      {board === 'readings' && <ReadingsBoard />}
      {board === 'topology' && <ClusterTopology />}
      {board === 'metrics' && <MetricsBoard />}
      {board === 'logs' && <LogsBoard />}
      {board === 'traces' && <TracesBoard />}
      {board === 'events' && <EventsBoard />}
      {board === 'probes' && <ProbesBoard />}
    </div>
  );
}

function OverviewBoard() {
  const overview = getClusterOverview();
  return <span>{overview.cluster_health}</span>;
}
EOF
  cat >"${work}/web/src/topology/ClusterTopology.tsx" <<'EOF'
import { getClusterTopology } from '@/api/client';
export function ClusterTopology() {
  return <span>{getClusterTopology.name}</span>;
}
EOF
  cat >"${work}/web/src/observability/MetricsBoard.tsx" <<'EOF'
import { getEvolutionMetrics } from '@/api/client';
export function MetricsBoard() {
  return <span>{getEvolutionMetrics.name}</span>;
}
EOF
  cat >"${work}/web/src/observability/LogsBoard.tsx" <<'EOF'
import { listRawLogs } from '@/api/client';
export function LogsBoard() {
  const page = listRawLogs();
  return <span>这一页 {page.count} 份归档</span>;
}
EOF
  cat >"${work}/web/src/observability/TracesBoard.tsx" <<'EOF'
import { listTraces } from '@/api/client';
export function TracesBoard() {
  const trace = listTraces();
  return (
    <span>
      {trace.events.length < trace.event_count && '读回的帧比轨迹自报的少'}
    </span>
  );
}
EOF
  cat >"${work}/web/src/observability/EventsBoard.tsx" <<'EOF'
import { listEvents } from '@/api/client';
import { normalizeAgentEvent } from '@/transport/websocket';
export function EventsBoard() {
  const raw = listEvents();
  const events = raw.map((item, index) => normalizeAgentEvent(item, String(index)));
  const truncated = raw.length >= 1000;
  return <span>窗口最近 24 小时，最多 1000 条{truncated ? '（被截断）' : ''} {events.length}</span>;
}
EOF
  cat >"${work}/web/src/observability/ProbesBoard.tsx" <<'EOF'
import { getBackendHealth } from '@/api/client';
export function ProbesBoard() {
  return <span>{getBackendHealth.name}</span>;
}
EOF
  cat >"${work}/web/src/observability/ReadingsBoard.tsx" <<'EOF'
import { getObservabilityReadings } from '@/api/client';
import { deepLink } from '@/observability/deepLink';
export function ReadingsBoard() {
  const readings = getObservabilityReadings();
  return (
    <ul>
      {readings.map((r) => (
        <li key={r.id}>
          {r.title} {r.value ?? '无读数'} 产出方：{r.produced_by}
          {r.unavailable_because && <span>{r.unavailable_because}</span>}
          {deepLink(r.dashboard_uid).href ?? deepLink(r.dashboard_uid).why}
        </li>
      ))}
    </ul>
  );
}
EOF
  cat >"${work}/web/src/observability/deepLink.ts" <<'EOF'
export function deepLink(uid: string | null) {
  if (!uid) return { href: null, why: '这条读数没有对应的面板' };
  return { href: `/d/${uid}`, why: null };
}
EOF
  cat >"${work}/web/src/api/client.ts" <<'EOF'
export async function getClusterOverview() { return fetch('/api/v1/cluster/overview'); }
export async function getClusterTopology() { return fetch('/api/v1/cluster/topology'); }
export async function getEvolutionMetrics() { return fetch('/api/v1/evolution/metrics'); }
export async function listRawLogs() { return fetch('/api/v1/raw_logs'); }
export async function listTraces() { return fetch('/api/v1/traces'); }
export async function listEvents() { return fetch('/api/v1/events'); }
export async function getBackendHealth() { return fetch('/api/v1/health/backends'); }
export async function getObservabilityReadings() { return fetch('/api/v1/observability/readings'); }
EOF
}

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

confirm() {
  grep -qF "$2" "$1" || fail "改动没落到 ${1#"${work}"/}：找不到「$2」"
}

fixture
expect_green "完整链路"

# 1. 板条上少了事件回放。
fixture
sed -i "/{ id: 'events' },/d" "${work}/web/src/views/ObservabilityView.tsx"
if grep -q "{ id: 'events' }" "${work}/web/src/views/ObservabilityView.tsx"; then fail "板条没被剪掉"; fi
expect_red "板条里没有事件回放" "板条里没有 events 这块板"

# 2. 板条里写着，屏上没人渲染。
fixture
sed -i "/board === 'events' && <EventsBoard \/>/d" "${work}/web/src/views/ObservabilityView.tsx"
if grep -q "board === 'events'" "${work}/web/src/views/ObservabilityView.tsx"; then fail "渲染没被剪掉"; fi
expect_red "没有渲染事件回放" "events 这块板没有渲染"

# 3. 前端没有声明那条路径。
fixture
sed -i "s#'/api/v1/events'#'/api/v1/evt'#" "${work}/web/src/api/client.ts"
confirm "${work}/web/src/api/client.ts" "/api/v1/evt"
expect_red "没有声明路径" "没有声明 /api/v1/events"

# 4. 没有取它的函数。
fixture
sed -i 's/export async function listEvents/export async function readEvents/' \
  "${work}/web/src/api/client.ts"
sed -i 's/listEvents/readEvents/' "${work}/web/src/observability/EventsBoard.tsx"
confirm "${work}/web/src/api/client.ts" "readEvents"
expect_red "没有取数函数" "没有导出 listEvents"

# 5. 事件回放不写窗口边界。
fixture
python3 - "${work}/web/src/observability/EventsBoard.tsx" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("窗口最近 24 小时，最多 1000 条", "最近的事件")
open(path, "w", encoding="utf-8").write(src)
PY
if grep -q "1000" "${work}/web/src/observability/EventsBoard.tsx"; then
  # 上限那处还在（截断判定），把判词里的边界文字换成不带数字的。
  python3 - "${work}/web/src/observability/EventsBoard.tsx" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("raw.length >= 1000", "raw.length >= LIMIT")
open(path, "w", encoding="utf-8").write(src)
PY
fi
if grep -qE "1000|24 小时" "${work}/web/src/observability/EventsBoard.tsx"; then
  fail "窗口边界没被剪掉"
fi
expect_red "不写窗口边界" "没有写出窗口边界"

# 6. 事件回放自己再写一份帧映射。
fixture
python3 - "${work}/web/src/observability/EventsBoard.tsx" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("import { normalizeAgentEvent } from '@/transport/websocket';\n", "")
src = src.replace("normalizeAgentEvent(item, String(index))", "{ type: item.type, payload: item }")
open(path, "w", encoding="utf-8").write(src)
PY
if grep -q "normalizeAgentEvent" "${work}/web/src/observability/EventsBoard.tsx"; then fail "归一函数没被剪掉"; fi
expect_red "自己写一份帧映射" "没有走 normalizeAgentEvent"

# 7. 读满上限却不判截断。
fixture
sed -i 's/raw.length >= 1000/raw.length >= LIMIT/' "${work}/web/src/observability/EventsBoard.tsx"
confirm "${work}/web/src/observability/EventsBoard.tsx" ">= LIMIT"
expect_red "不判截断" "没有判截断"

# 8. 日志的分页数被说成总量。
fixture
sed -i 's/这一页 {page.count} 份归档/共 {page.count} 份归档/' \
  "${work}/web/src/observability/LogsBoard.tsx"
confirm "${work}/web/src/observability/LogsBoard.tsx" "共 {page.count}"
expect_red "分页数当总量" "没有把 count 说成「这一页」"

# 9. 轨迹少读了帧却按全量展示。
fixture
python3 - "${work}/web/src/observability/TracesBoard.tsx" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("{trace.events.length < trace.event_count && '读回的帧比轨迹自报的少'}",
                  "{trace.events.length}")
open(path, "w", encoding="utf-8").write(src)
PY
if grep -q "trace.events.length < trace.event_count" "${work}/web/src/observability/TracesBoard.tsx"; then
  fail "比较没被剪掉"
fi
expect_red "不比较帧数" "没有比较读回的帧数与轨迹自报的帧数"

# 10. 模型花销在观测面又算了一遍。
fixture
python3 - "${work}/web/src/views/ObservabilityView.tsx" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("import { getClusterOverview } from '@/api/client';",
                  "import { getClusterOverview, getTokenUsage } from '@/api/client';")
src = src.replace("const overview = getClusterOverview();",
                  "const overview = getClusterOverview();\n  const usage = getTokenUsage();")
open(path, "w", encoding="utf-8").write(src)
PY
confirm "${work}/web/src/views/ObservabilityView.tsx" "getTokenUsage"
expect_red "模型花销算两遍" "又在观测面里读了一次模型用量"

# 11. 一块板从入口不可达（import 指向别的模块，渲染行还在）。
fixture
sed -i "s#from '@/observability/ProbesBoard'#from '@/observability/LogsBoard'#" \
  "${work}/web/src/views/ObservabilityView.tsx"
confirm "${work}/web/src/views/ObservabilityView.tsx" "import { ProbesBoard } from '@/observability/LogsBoard'"
expect_red "板不可达" "ProbesBoard.tsx: 从 main.tsx 不可达"

# 12. 读数板不注明来源。
fixture
sed -i 's/产出方：{r.produced_by}//' "${work}/web/src/observability/ReadingsBoard.tsx"
if grep -q "produced_by" "${work}/web/src/observability/ReadingsBoard.tsx"; then fail "来源没被剪掉"; fi
expect_red "读数不注明来源" "没有把 series/produced_by 显示出来"

# 13. 深链自己拼地址，不经 deepLink。
fixture
python3 - "${work}/web/src/observability/ReadingsBoard.tsx" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("{deepLink(r.dashboard_uid).href ?? deepLink(r.dashboard_uid).why}",
                  "{`/d/${r.dashboard_uid}`}")
src = src.replace("import { deepLink } from '@/observability/deepLink';\n", "")
open(path, "w", encoding="utf-8").write(src)
PY
if grep -q "deepLink" "${work}/web/src/observability/ReadingsBoard.tsx"; then fail "deepLink 没被剪掉"; fi
expect_red "深链自己拼" "深链不是经 deepLink 拼的"

# 14. 拼不出链接时不给原因。
fixture
sed -i "s/why:/reason:/g" "${work}/web/src/observability/deepLink.ts"
if grep -q "why" "${work}/web/src/observability/deepLink.ts"; then fail "why 没被剪掉"; fi
expect_red "深链不说原因" "拼不出链接时没有给出原因"

# 真树：今天必须绿。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-observability-boards.test: all checks passed"
