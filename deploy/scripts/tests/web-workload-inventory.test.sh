#!/usr/bin/env bash
# check-web-workload-inventory.sh 的判据：构建与部署那一屏必须列全它看得见的东西。
#
# 这条门守的不是「有没有数据」，是**看得见的工作负载有没有被列全**。它防的坏法全都是
# 静默的：接口在、Deployment 那几行一个字都没错，只是其余四种工种根本不在屏上——
# postgres（StatefulSet）、节点级常驻组件（DaemonSet）、备份（CronJob）、每次主线部署
# （Job）都在集群里跑着，而空白读起来像「本来就没有」。
#
# 夹具的形状是「五种工种 × 各自的接口与状态字段 + 一屏带工种列的表」，然后逐段剪断，
# 每剪一段必须红：
#   - 少一种工种的接口              → 那一种读不到，屏上只少几行
#   - 用 Deployment 的字段顶替      → DaemonSet / Job 读成「零个」
#   - 跨工种字段给 0 而不是 null    → 「不适用」被读成「零个就绪」
#   - 屏上没有工种列                → 五种混在一张表里，分不出谁是谁
#   - 不适用的格子留空              → 空白读起来像「这一行没读到」
#   - 旧的单一 Deployment 读法复活  → 其余四种又静默消失
#   - 判坏消息退回 `!ok`            → Failed=True 的 Job 被画成绿点
# 每段改动都先确认**落到了文件上**（否则「改动没生效」与「门没抓住」不可区分）。
#
# 最后一条断言是在**真树**上跑：夹具全绿而真树红，说明这条门只是在测自己。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-workload-inventory.sh"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ── 夹具：服务端五种工种的读法 + 一屏带工种列的表 ──
fixture() {
  rm -rf "${work}/web" "${work}/crates"
  mkdir -p "${work}/web/src/api" "${work}/web/src/views" "${work}/crates/cog-gateway/src"

  cat >"${work}/crates/cog-gateway/src/cluster.rs" <<'EOF'
pub enum WorkloadKind { Deployment, StatefulSet, DaemonSet, CronJob, Job }

impl WorkloadKind {
    fn list_path(self, namespace: &str) -> String {
        match self {
            WorkloadKind::Deployment => format!("/apis/apps/v1/namespaces/{namespace}/deployments"),
            WorkloadKind::StatefulSet => format!("/apis/apps/v1/namespaces/{namespace}/statefulsets"),
            WorkloadKind::DaemonSet => format!("/apis/apps/v1/namespaces/{namespace}/daemonsets"),
            WorkloadKind::CronJob => format!("/apis/batch/v1/namespaces/{namespace}/cronjobs"),
            WorkloadKind::Job => format!("/apis/batch/v1/namespaces/{namespace}/jobs"),
        }
    }
    fn containers(self) -> &'static str {
        match self {
            WorkloadKind::CronJob => "/spec/jobTemplate/spec/template/spec/containers",
            _ => "/spec/template/spec/containers",
        }
    }
}

pub struct WorkloadView {
    pub ready: Option<u32>,
    pub desired: Option<u32>,
}

fn count(path: &str) -> Option<u32> {
    let _ = "/status/desiredNumberScheduled";
    let _ = "/spec/completions";
    None
}

fn workloads_from(kind: WorkloadKind) -> Vec<WorkloadView> {
    let _ = kind;
    Vec::new()
}

#[test]
fn the_other_four_kinds_are_counted_by_their_own_status_fields() {}
EOF

  cat >"${work}/web/src/api/client.ts" <<'EOF'
export interface WorkloadView {
  kind: string;
  name: string;
  ready: number | null;
}
EOF

  cat >"${work}/web/src/views/DeliveryView.tsx" <<'EOF'
import { getWorkloads } from '@/api/client';
export function DeliveryView() {
  const workloads = useResource('工作负载', getWorkloads, 15_000);
  const stuck = workloads.data?.workloads.filter((d) => d.conditions.some(conditionIsTrouble)) ?? [];
  return (
    <Table
      columns={[
        { key: 'kind', title: '类型', render: (d) => d.kind },
        { key: 'revision', title: '版本', render: (d) => d.revision ?? <span>不适用</span> },
        { key: 'conditions', title: '条件', render: (d) => conditionIsTrouble(d) && d.reason },
      ]}
      rows={stuck}
    />
  );
}
function conditionIsTrouble(condition: { kind: string; ok: boolean }) {
  if (condition.kind === 'ReplicaFailure' || condition.kind.startsWith('Failed')) {
    return condition.ok;
  }
  return !condition.ok;
}
EOF
}

confirm() {
  grep -qF "$2" "$1" || fail "改动没落到 ${1#"${work}"/}：找不到「$2」"
}

expect_green() {
  local what="$1"
  bash "${checker}" "${work}" >"${work}/green.out" 2>&1 \
    || { cat "${work}/green.out" >&2; fail "$*：应当绿却红了"; }
}

expect_red() {
  local what="$1" needle="$2"
  local got=0
  bash "${checker}" "${work}" >"${work}/red.out" 2>&1 || got=$?
  [ "${got}" -ne 0 ] || { fail "${what}：应当红却绿了"; }
  grep -qF "${needle}" "${work}/red.out" \
    || { cat "${work}/red.out" >&2; fail "${what}：红了，但没点到「${needle}」"; }
}

fixture
expect_green "完整夹具"

# 1. 少一种工种的接口：那一种读不到，屏上只少几行。
fixture
python3 - "${work}/crates/cog-gateway/src/cluster.rs" <<'PATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace('"/apis/apps/v1/namespaces/{namespace}/statefulsets"', '""', 1)
open(path, "w", encoding="utf-8").write(src)
PATCH
confirm "${work}/crates/cog-gateway/src/cluster.rs" 'WorkloadKind::StatefulSet => format!("")'
expect_red "少一种工种的接口" "statefulsets"

# 2. 用 Deployment 的字段顶替 DaemonSet 的「要几个」。
fixture
sed -i 's#/status/desiredNumberScheduled#/spec/replicas#' "${work}/crates/cog-gateway/src/cluster.rs"
confirm "${work}/crates/cog-gateway/src/cluster.rs" "/spec/replicas"
expect_red "DaemonSet 用 spec.replicas" "desiredNumberScheduled"

# 3. 跨工种不通用的字段给 0 而不是 null。
fixture
sed -i 's/pub ready: Option<u32>/pub ready: u32/' "${work}/crates/cog-gateway/src/cluster.rs"
confirm "${work}/crates/cog-gateway/src/cluster.rs" "pub ready: u32"
expect_red "ready 不是 Option" "不是 Option"

# 3b. 同上，desired 那一栏。
fixture
sed -i 's/pub desired: Option<u32>/pub desired: u32/' "${work}/crates/cog-gateway/src/cluster.rs"
confirm "${work}/crates/cog-gateway/src/cluster.rs" "pub desired: u32"
expect_red "desired 不是 Option" "desired 不是 Option"

# 4. CronJob 的容器声明读错位置。
fixture
sed -i 's#/spec/jobTemplate/spec/template/spec/containers#/spec/template/spec/containers#' \
  "${work}/crates/cog-gateway/src/cluster.rs"
confirm "${work}/crates/cog-gateway/src/cluster.rs" 'WorkloadKind::CronJob => "/spec/template'
expect_red "CronJob 容器读错位置" "jobTemplate"

# 5. 屏上没有工种列。
fixture
sed -i "s/{ key: 'kind', title: '类型', render: (d) => d.kind },//" \
  "${work}/web/src/views/DeliveryView.tsx"
confirm "${work}/web/src/views/DeliveryView.tsx" "columns={["
expect_red "屏上没有工种列" "「类型」这一列"

# 6. 不适用的格子留空。
fixture
sed -i "s/<span>不适用<\/span>/<span>—<\/span>/" "${work}/web/src/views/DeliveryView.tsx"
confirm "${work}/web/src/views/DeliveryView.tsx" "<span>—</span>"
expect_red "不适用写成空白" "不适用"

# 7. 旧的单一 Deployment 读法复活。
fixture
cat >>"${work}/crates/cog-gateway/src/cluster.rs" <<'EOF'

fn deployments_from(list: &str) -> Vec<WorkloadView> {
    let _ = list;
    Vec::new()
}
EOF
confirm "${work}/crates/cog-gateway/src/cluster.rs" "fn deployments_from"
expect_red "旧读法复活" "deployments_from 还在"

# 8. 判坏消息退回 `!ok`：Failed=True 的 Job 被画成绿点。
fixture
python3 - "${work}/web/src/views/DeliveryView.tsx" <<'PATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
start = src.index("function conditionIsTrouble")
src = src[:start] + "function conditionIsTrouble(condition: { ok: boolean }) {\n  return !condition.ok;\n}\n"
open(path, "w", encoding="utf-8").write(src)
PATCH
confirm "${work}/web/src/views/DeliveryView.tsx" "return !condition.ok;"
expect_red "判坏消息退回 !ok" "ReplicaFailure"

# 8b. 屏上又按 !ok 直接判（函数还在，但没人用它）。
fixture
sed -i 's/d.conditions.some(conditionIsTrouble)/d.conditions.some((c) => !c.ok)/' \
  "${work}/web/src/views/DeliveryView.tsx"
confirm "${work}/web/src/views/DeliveryView.tsx" "some((c) => !c.ok)"
expect_red "屏上绕过判据函数" "conditionIsTrouble"

# 9. 没有这次改动就会红的那条测试被删掉。
fixture
sed -i '/the_other_four_kinds_are_counted_by_their_own_status_fields/d' \
  "${work}/crates/cog-gateway/src/cluster.rs"
confirm "${work}/crates/cog-gateway/src/cluster.rs" "workloads_from"
expect_red "服务端那条测试被删掉" "没有测试 the_other_four_kinds"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-workload-inventory.test: all checks passed"
