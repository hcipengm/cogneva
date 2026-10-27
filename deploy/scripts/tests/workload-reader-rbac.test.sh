#!/usr/bin/env bash
# 部署面读权限门禁的夹具测试。
#
# 「构建与部署」那一屏读五种工作负载，而读不读得到由 RBAC 决定：接口改完了、镜像
# 发布了、屏上仍然只有 11 行，因为 ClusterRole 只授了 `deployments`。这不是假想的
# 坏法，是实测的现场读数（2026-10-27，见下面这条门的注释）。
#
# 判据：`crates/cog-gateway/src/cluster.rs` 里每种工作负载问的那条 (group, resource)，
# 在**每一条**投递路径的 ClusterRole `cogneva-topology-reader` 里都必须带 `list`——
# 每个 profile 的渲染产物（chart）各一份、静态清单（集群内 GitOps 拉取端）一份，
# 少哪一份都在某一种安装形态下读不到。投递路径的集合是 `deploy/rendered/` 下的目录，
# 不是脚本里的一份名单：名单会漏掉新增的那一个 profile。
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
  # 「谁端着这一面」那一半要读 Deployment 与绑定，夹具里也必须有这两个形状。
  # 网关与进化：同一个镜像、都不覆盖 command，只是服务账号不同。
  for dir in "${work}/deploy/rendered/k3s-single" "${work}/deploy/k3s"; do
    for pair in "cogneva:cogneva" "cogneva-evolution:cogneva-evolution"; do
      workload="${pair%%:*}"
      account="${pair##*:}"
      cat >"${dir}/41-deployment-${workload}.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${workload}
spec:
  template:
    spec:
      serviceAccountName: ${account}
      containers:
        - name: cogneva
          image: localhost:30500/cogneva:local
EOF
    done
    cat >"${dir}/17-clusterrolebinding-cogneva-topology-reader.yaml" <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: cogneva-topology-reader
subjects:
  - kind: ServiceAccount
    name: cogneva
  - kind: ServiceAccount
    name: cogneva-evolution
roleRef:
  kind: ClusterRole
  name: cogneva-topology-reader
  apiGroup: rbac.authorization.k8s.io
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
python3 - "${work}/deploy/k3s/17-clusterrolebinding-cogneva-topology-reader.yaml" <<'PATCH'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("""  - kind: ServiceAccount
    name: cogneva-evolution
""", "", 1)
open(path, "w", encoding="utf-8").write(src)
PATCH
! grep -q "name: cogneva-evolution" \
  "${work}/deploy/k3s/17-clusterrolebinding-cogneva-topology-reader.yaml" \
  || fail "夹具没改到：绑定里还留着 cogneva-evolution"
expect_red "绑定漏了进化进程" "cogneva-evolution"

# 10. 新起一个跑同一份镜像、不覆盖 command 的 Deployment：它是本应用，就必须绑定。
#     判据从清单读「谁跑这份镜像」，不点名名单——点名的话，新加的那个工作负载
#     在绑定里缺一条，而这半边门一个字节都不变。
fixture
cat >"${work}/deploy/rendered/k3s-single/41-deployment-cogneva-worker.yaml" <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cogneva-worker
spec:
  template:
    spec:
      serviceAccountName: cogneva-worker
      containers:
        - name: cogneva
          image: localhost:30500/cogneva:local
EOF
confirm "${work}/deploy/rendered/k3s-single/41-deployment-cogneva-worker.yaml" "cogneva-worker"
expect_red "新工作负载跑应用却没绑定" "cogneva-worker"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "workload-reader-rbac.test: all checks passed"
