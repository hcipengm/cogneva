#!/usr/bin/env bash
# 部署面读权限门禁的夹具测试。
#
# 「构建与部署」那一屏读五种工作负载，而读不读得到由 RBAC 决定：接口改完了、镜像
# 发布了、屏上仍然只有 11 行，因为 ClusterRole 只授了 `deployments`。这不是假想的
# 坏法，是实测的现场读数（2026-10-27，见下面这条门的注释）。
#
# 判据：`crates/cog-gateway/src/cluster.rs` 里每种工作负载问的那条 (group, resource)，
# 在两个投递路径的 ClusterRole `cogneva-topology-reader` 里都必须带 `list`——
# 渲染产物（chart）与静态清单（集群内 GitOps 拉取端）各一份，少哪一份都在某一种
# 安装形态下读不到。
#   - 静态清单少授一种    → apply 安装的那套集群上，那一种读不到
#   - 渲染产物少授一种    → helm 安装的那套集群上，那一种读不到
#   - 动词从 list 变 get  → 集合级读取被拒（按名读的规则替代不了它）
#
# 夹具的形状是「真的两份清单各剪一刀」，每剪一刀必须红；最后在真树上跑一遍。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-workload-reader-rbac.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 一份最小但形状相同的树：只留这条门读的三个文件。
fixture() {
  rm -rf "${work}/crates" "${work}/deploy"
  mkdir -p "${work}/crates/cog-gateway/src" \
    "${work}/deploy/rendered/k3s-single" "${work}/deploy/k3s"
  cat >"${work}/crates/cog-gateway/src/cluster.rs" <<'EOF'
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
}
EOF
  for path in "${work}/deploy/rendered/k3s-single/16-clusterrole-cogneva-topology-reader.yaml" \
    "${work}/deploy/k3s/gitops-puller-rbac.yaml"; do
    cat >"${path}" <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: cogneva-topology-reader
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list"]
  - apiGroups: ["apps"]
    resources: ["deployments", "statefulsets", "daemonsets"]
    verbs: ["get", "list"]
  - apiGroups: ["batch"]
    resources: ["jobs", "cronjobs"]
    verbs: ["get", "list"]
EOF
  done
}

confirm() {
  grep -qF "$2" "$1" || fail "改动没落到 ${1#"${work}"/}：找不到「$2」"
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
bash "${checker}" "${work}" >"${work}/green.out" 2>&1 \
  || { cat "${work}/green.out" >&2; fail "完整夹具：应当绿却红了"; }

# 1. 静态清单少授 StatefulSet。
fixture
sed -i 's/"deployments", "statefulsets", "daemonsets"/"deployments", "daemonsets"/' \
  "${work}/deploy/k3s/gitops-puller-rbac.yaml"
confirm "${work}/deploy/k3s/gitops-puller-rbac.yaml" '"deployments", "daemonsets"'
expect_red "静态清单少授 StatefulSet" "statefulsets"

# 2. 渲染产物少授 Job。
fixture
sed -i 's/\["jobs", "cronjobs"\]/["cronjobs"]/' \
  "${work}/deploy/rendered/k3s-single/16-clusterrole-cogneva-topology-reader.yaml"
confirm "${work}/deploy/rendered/k3s-single/16-clusterrole-cogneva-topology-reader.yaml" '["cronjobs"]'
expect_red "渲染产物少授 Job" "jobs"

# 3. 动词从 list 退到 get：集合级读取被拒。
fixture
python3 - "${work}/deploy/k3s/gitops-puller-rbac.yaml" <<'PATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace('resources: ["deployments", "statefulsets", "daemonsets"]\n    verbs: ["get", "list"]',
                  'resources: ["deployments", "statefulsets", "daemonsets"]\n    verbs: ["get"]', 1)
open(path, "w", encoding="utf-8").write(src)
PATCH
confirm "${work}/deploy/k3s/gitops-puller-rbac.yaml" 'verbs: ["get"]'
expect_red "动词缺 list" "list"

# 4. 服务端多读一种，两份清单都没跟上：新增工种那天必须红。
fixture
python3 - "${work}/crates/cog-gateway/src/cluster.rs" <<'PATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace(
    'WorkloadKind::Job => format!("/apis/batch/v1/namespaces/{namespace}/jobs"),',
    'WorkloadKind::Job => format!("/apis/batch/v1/namespaces/{namespace}/jobs"),\n'
    '            WorkloadKind::ReplicaSet => format!("/apis/apps/v1/namespaces/{namespace}/replicasets"),',
    1,
)
open(path, "w", encoding="utf-8").write(src)
PATCH
confirm "${work}/crates/cog-gateway/src/cluster.rs" "replicasets"
expect_red "新增工种没授权限" "replicasets"

# 5. 集群范围的读只写在命名空间 Role 里：必须红。这是 2026-10-27 第二次实测的形状——
#    `pods` 授过，而 `/api/v1/events`、`/api/v1/persistentvolumeclaims` 也「授过」，
#    只是授在了一条命名空间 Role 上；集合读不带 `namespaces/<ns>`，那条规则一次都没生效。
#    同一份规则搬到 ClusterRole 里，同一条门必须绿——红与绿只差作用域这一个变量。
add_cluster_read() {
    python3 - "${work}/crates/cog-gateway/src/cluster.rs" <<'PATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src += '''
async fn read_events(client: &Client) {
    client.get_json("/api/v1/events").await;
}
'''
open(path, "w", encoding="utf-8").write(src)
PATCH
    confirm "${work}/crates/cog-gateway/src/cluster.rs" '"/api/v1/events"'
}

fixture
add_cluster_read
for path in "${work}/deploy/rendered/k3s-single/16-clusterrole-cogneva-topology-reader.yaml" \
    "${work}/deploy/k3s/gitops-puller-rbac.yaml"; do
    cat >>"${path}" <<'EOF'
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: cogneva-topology-reader
  namespace: cogneva
rules:
  - apiGroups: [""]
    resources: ["events", "persistentvolumeclaims"]
    verbs: ["get", "list"]
EOF
done
expect_red "集群范围的读只授在命名空间 Role 上" "集群范围的读只有 ClusterRole 算数"

# 6. 同一份规则搬进 ClusterRole：作用域对了就绿。
fixture
add_cluster_read
for path in "${work}/deploy/rendered/k3s-single/16-clusterrole-cogneva-topology-reader.yaml" \
    "${work}/deploy/k3s/gitops-puller-rbac.yaml"; do
    python3 - "${path}" <<'PATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace('''  - apiGroups: ["batch"]
    resources: ["jobs", "cronjobs"]
    verbs: ["get", "list"]''', '''  - apiGroups: ["batch"]
    resources: ["jobs", "cronjobs"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["events", "persistentvolumeclaims"]
    verbs: ["get", "list"]''', 1)
open(path, "w", encoding="utf-8").write(src)
PATCH
    confirm "${path}" '["events", "persistentvolumeclaims"]'
done
bash "${checker}" "${work}" >"${work}/scope-ok.out" 2>&1 \
    || { cat "${work}/scope-ok.out" >&2; fail "规则搬进 ClusterRole：应当绿却红了"; }

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "workload-reader-rbac.test: all checks passed"
