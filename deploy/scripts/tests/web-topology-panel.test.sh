#!/usr/bin/env bash
# check-web-topology-panel.sh 的判据：拓扑那一屏必须说真话。
#
# 这条门守的不是「有没有数据」，是**数据有没有被说错**。它防的坏法全都是静默的：
# 路由在、JSON 里每个字段都对、组件也在渲染，只是那一屏把「没读到节点」说成
# 「没被调度」、把跑完的 Job 画成失败、或者干脆不说它只读一个命名空间。
#
# 夹具的形状是「一条完整的链 + 服务端那段分流」，然后逐段剪断，每剪一段必须红：
#   - 剪掉路径声明 / 取它的函数     → 前端这一侧没有这条读面
#   - 没有活的消费者 / 只 import    → 屏上不会有这一面
#   - 不渲染 unplaced               → 节点读失败时那些 Pod 从屏上消失
#   - 不显示 pod.namespace          → 一屏只读一个命名空间，却被读成整台集群
#   - 判色函数不单独判 Succeeded    → 完成的 Job 被画成红点
#   - 完成与失败不分开数            → 「Pod N」把两类混成一个数
#   - 服务端把「节点没匹配上」并入 unscheduled → 整屏 Pod 变成「没被调度」
#   - 服务端那条测试被删掉          → 上面这条分流可以被人改回去而不让任何东西红
#   - 把 clusters 折成 clusters[0]  → 接上第二个集群那天，它在屏上根本不存在
#   - 把卷与事件排进容器下面         → 屏上说的包含关系是 Kubernetes 里没有的
#   - 服务端量到进程数而不上屏       → 读到的数活在 JSON 里，没有人看得见
# 每段改动都先确认**落到了文件上**（否则「改动没生效」与「门没抓住」不可区分）。
#
# 最后一条断言是在**真树**上跑：夹具全绿而真树红，说明这条门只是在测自己。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-topology-panel.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ── 夹具：一条从入口到拓扑屏的链，加服务端那段分流 ──
fixture() {
  rm -rf "${work}/web" "${work}/crates"
  mkdir -p "${work}/web/src/api" "${work}/web/src/hooks" "${work}/web/src/topology" \
    "${work}/crates/cog-gateway/src"
  cat >"${work}/web/src/main.tsx" <<'EOF'
import App from './App';
render(<App />);
EOF
  cat >"${work}/web/src/App.tsx" <<'EOF'
import { TopologyPanel } from './topology/TopologyPanel';
export default function App() {
  return <TopologyPanel />;
}
EOF
  cat >"${work}/web/src/topology/TopologyPanel.tsx" <<'EOF'
import { useTopology } from '@/hooks/useTopology';
export function TopologyPanel() {
  const clusters = useTopology();
  return (
    <div>
      {clusters.map((cluster) => (
        <div key={cluster.id}>
          <span>{cluster.namespace}</span>
          {cluster.unplaced.map((pod) => (
            <span key={pod.name}>{pod.namespace}/{pod.name}</span>
          ))}
        </div>
      ))}
      {counts.finished} / {counts.failed}
    </div>
  );
}
export function phaseTone(pod: { phase: string; ready: boolean }) {
  if (pod.phase === 'Succeeded') return 'idle';
  if (pod.phase === 'Failed') return 'danger';
  return pod.ready ? 'ok' : 'warn';
}
const counts = { finished: 0, failed: 0 };
const dot = phaseTone({ phase: 'Running', ready: true });
EOF
  cat >"${work}/web/src/hooks/useTopology.ts" <<'EOF'
import { getClusterTopology } from '@/api/client';
export function useTopology() {
  const topology = useResource('集群拓扑', getClusterTopology, 10_000);
  return topology.data?.clusters ?? [];
}
EOF
  cat >"${work}/web/src/api/client.ts" <<'EOF'
export async function getClusterTopology() {
  return fetch('/api/v1/cluster/topology');
}
EOF
  cat >"${work}/crates/cog-gateway/src/cluster.rs" <<'EOF'
pub struct ClusterView {
    pub id: String,
    pub namespace: String,
    pub unscheduled: Vec<PodView>,
    pub unplaced: Vec<PodView>,
}

fn place_pods(cluster: &mut ClusterView, pods: Vec<PodView>) {
    for pod in pods {
        let Some(node_name) = pod.node.clone() else {
            cluster.unscheduled.push(pod);
            continue;
        };
        match cluster.nodes.iter_mut().find(|node| node.name == node_name) {
            Some(node) => node.pods.push(pod),
            None => cluster.unplaced.push(pod),
        }
    }
}

#[test]
fn a_pod_on_a_node_that_was_not_read_is_not_called_unscheduled() {
    assert!(true);
}
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

# 未改动的夹具：整条链在，必须绿。
fixture
expect_green "完整链路"

# 1. 剪掉路径声明。
fixture
sed -i "s#'/api/v1/cluster/topology'#'/api/v1/cluster/graph'#" "${work}/web/src/api/client.ts"
confirm "${work}/web/src/api/client.ts" "/cluster/graph"
expect_red "没有路径声明" "没有声明拓扑读取"

# 2. 剪掉取它的函数。
fixture
sed -i 's/export async function getClusterTopology/export async function getTopology/' \
  "${work}/web/src/api/client.ts"
sed -i 's/getClusterTopology/getTopology/' "${work}/web/src/hooks/useTopology.ts"
confirm "${work}/web/src/api/client.ts" "getTopology"
expect_red "没有取它的函数" "没有导出 getClusterTopology"

# 3. 没有活的消费者。
fixture
cat >"${work}/web/src/hooks/useTopology.ts" <<'EOF'
export function useTopology() {
  return null;
}
EOF
confirm "${work}/web/src/hooks/useTopology.ts" "return null"
expect_red "没有消费者" "没有活的消费者"

# 4. 只 import、不渲染。
fixture
cat >"${work}/web/src/App.tsx" <<'EOF'
import { TopologyPanel } from './topology/TopologyPanel';
export default function App() {
  return <div />;
}
EOF
confirm "${work}/web/src/App.tsx" "return <div />"
expect_red "只 import 不渲染" "没有一格渲染出来的组件用到它"

# 5. 不渲染 unplaced：节点读失败时那些 Pod 从屏上消失。
fixture
sed -i 's/{cluster.unplaced.map((pod) => (/{([]).map((pod) => (/' \
  "${work}/web/src/topology/TopologyPanel.tsx"
confirm "${work}/web/src/topology/TopologyPanel.tsx" "{([]).map"
expect_red "不渲染 unplaced" "没有渲染 unplaced"

# 6. 不显示命名空间。
fixture
sed -i 's#{pod.namespace}/{pod.name}#{pod.name}#' "${work}/web/src/topology/TopologyPanel.tsx"
confirm "${work}/web/src/topology/TopologyPanel.tsx" "{pod.name}"
expect_red "不显示命名空间" "没有显示 pod.namespace"

# 7. 判色退回「不是 Running 就红」。
fixture
cat >"${work}/web/src/topology/TopologyPanel.tsx" <<'EOF'
import { useTopology } from '@/hooks/useTopology';
export function TopologyPanel() {
  const cluster = useTopology();
  return (
    <div>
      <span>{cluster.namespace}</span>
      {cluster.unplaced.map((pod) => (
        <span key={pod.name}>{pod.namespace}/{pod.name}</span>
      ))}
      {counts.finished} / {counts.failed}
    </div>
  );
}
export function phaseTone(pod: { phase: string; ready: boolean }) {
  return pod.phase === 'Running' && pod.ready ? 'ok' : pod.phase === 'Running' ? 'warn' : 'danger';
}
const counts = { finished: 0, failed: 0 };
const dot = phaseTone({ phase: 'Running', ready: true });
EOF
confirm "${work}/web/src/topology/TopologyPanel.tsx" "? 'warn' : 'danger'"
expect_red "判色不看 Succeeded" "没有单独判 Succeeded"

# 8. 完成与失败不分开数。
fixture
sed -i 's/{counts.finished} \/ {counts.failed}/共 {counts.total} 个/' \
  "${work}/web/src/topology/TopologyPanel.tsx"
sed -i 's/const counts = { finished: 0, failed: 0 };/const counts = { total: 0 };/' \
  "${work}/web/src/topology/TopologyPanel.tsx"
confirm "${work}/web/src/topology/TopologyPanel.tsx" "counts.total"
expect_red "完成与失败混成一个数" "没有把 finished 单独数出来"

# 9. 服务端把「节点没匹配上」并回 unscheduled：整屏 Pod 变成「没被调度」。
fixture
python3 - "${work}/crates/cog-gateway/src/cluster.rs" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("None => cluster.unplaced.push(pod),", "None => cluster.unscheduled.push(pod),")
open(path, "w", encoding="utf-8").write(src)
PY
confirm "${work}/crates/cog-gateway/src/cluster.rs" "None => cluster.unscheduled.push(pod),"
expect_red "服务端并回一栏" "没有把「写着在节点上、但没匹配到节点」的 Pod 放进 unplaced"

# 10. 服务端那条测试被删掉。
fixture
sed -i '/fn a_pod_on_a_node_that_was_not_read_is_not_called_unscheduled/d' \
  "${work}/crates/cog-gateway/src/cluster.rs"
if grep -q "a_pod_on_a_node_that_was_not_read" "${work}/crates/cog-gateway/src/cluster.rs"; then
  fail "测试名没被删掉"
fi
expect_red "没有测试守着" "没有测试 a_pod_on_a_node_that_was_not_read_is_not_called_unscheduled"

# 11. 把集群列表折成第一个：屏上照样有节点、有 Pod，只是少了别的集群。
fixture
sed -i 's/return topology.data?.clusters ?? \[\];/return topology.data?.clusters[0];/' \
  "${work}/web/src/hooks/useTopology.ts"
confirm "${work}/web/src/hooks/useTopology.ts" "clusters[0]"
expect_red "只读第一个集群" "把集群列表折成了第一个"

# 12. 把卷与事件画成容器的下一级：箭头链在断言一条并不存在的包含关系。
fixture
printf '// 集群 → 节点 → Pod → 容器 → 卷与事件\n' >>"${work}/web/src/topology/TopologyPanel.tsx"
confirm "${work}/web/src/topology/TopologyPanel.tsx" "容器 → 卷与事件"
expect_red "把卷与事件排进容器下面" "写成了容器的下一级"

# 13. 服务端量到进程数，读面却不显示。
fixture
python3 - "${work}/crates/cog-gateway/src/cluster.rs" <<'FIXPATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace(
    "    pub namespace: String,",
    "    pub namespace: String,\n    pub processes: Option<u64>,",
    1,
)
src = src.replace(
    "fn place_pods(",
    "fn processes_of(summary: &Value) -> Option<u64> {\n"
    '    summary.pointer("/process_stats/process_count").and_then(|v| v.as_u64())\n'
    "}\n\nfn place_pods(",
    1,
)
open(path, "w", encoding="utf-8").write(src)
FIXPATCH
confirm "${work}/crates/cog-gateway/src/cluster.rs" "process_count"
expect_red "量到进程数不上屏" "读面没有一处显示它"

# 14. 服务端收下了容器自报的进程表，读面只显示 Pod 级的计数。
fixture
cat >"${work}/crates/cog-gateway/src/process_reports.rs" <<'EOF'
pub async fn process_report_handler() {}
EOF
python3 - "${work}/crates/cog-gateway/src/cluster.rs" <<'FIXPATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace(
    "pub struct ClusterView {",
    "pub struct ContainerView {\n    pub processes: Option<Vec<ReportedProcess>>,\n}\n\npub struct ClusterView {",
    1,
)
open(path, "w", encoding="utf-8").write(src)
FIXPATCH
confirm "${work}/crates/cog-gateway/src/cluster.rs" "pub processes: Option<Vec<ReportedProcess>>"
expect_red "容器进程表不上屏" "没有一处遍历它"

# 14b. 同一份夹具，读面开始遍历容器上的进程表 → 必须绿。
python3 - "${work}/web/src/topology/TopologyPanel.tsx" <<'FIXPATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace(
    "{counts.finished} / {counts.failed}",
    "{counts.finished} / {counts.failed}\n"
    "      {container.processes?.map((process) => (\n"
    "        <span key={process.pid}>{process.command}</span>\n"
    "      ))}",
    1,
)
open(path, "w", encoding="utf-8").write(src)
FIXPATCH
confirm "${work}/web/src/topology/TopologyPanel.tsx" "container.processes"
expect_green "容器进程表上屏"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-topology-panel.test: all checks passed"
