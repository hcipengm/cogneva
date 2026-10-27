#!/usr/bin/env bash
# `check-process-census-coverage.sh` 的夹具：先证明它在缺东西时红，再证明完整树是绿的。
#
# 一份门禁自己的「红」也要被证明过：只跑真树的那一版在真树修好之后永远绿，
# 它到底还看不看得见「采集器没了」，没人知道。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-process-census-coverage.sh"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture() {
  rm -rf "${work}/crates" "${work}/deploy"
  mkdir -p "${work}/crates/cogneva/src" \
    "${work}/deploy/helm/cogneva/templates" \
    "${work}/deploy/k3s"
  # 形态的集合从真树读。夹具自称「完整树」，而写死的三种在新增 profile 之后就不再
  # 完整——负例 3（某一种没渲染出这份 DaemonSet）也就只覆盖得到老的那三种，新增的
  # 那一种缺了它，门照样绿。
  for real_profile in "${repo}"/deploy/rendered/*/; do
    [ -d "${real_profile}" ] || continue
    mkdir -p "${work}/deploy/rendered/$(basename "${real_profile}")"
  done

  cat >"${work}/crates/cogneva/src/cli.rs" <<'EOF'
const SUBCOMMANDS: [(&str, Command); 20] = [
    ("host-ledger", Command::HostLedger),
    ("process-census", Command::ProcessCensus),
];
EOF
  cat >"${work}/crates/cogneva/src/main.rs" <<'EOF'
Command::ProcessCensus => cogneva::process_census::run_from_env().await,
EOF
  cat >"${work}/crates/cogneva/src/lib.rs" <<'EOF'
pub mod process_census;
EOF
  cat >"${work}/crates/cogneva/src/process_census.rs" <<'EOF'
pub const DEFAULT_URL: &str = "http://cogneva:8085/api/v1/cluster/process-report";
EOF
  cat >"${work}/crates/cogneva/src/process_report.rs" <<'EOF'
pub const DEFAULT_REPORT_URL: &str = "http://cogneva:8085/api/v1/cluster/process-report";
EOF

  cat >"${work}/deploy/helm/cogneva/templates/process-census.yaml" <<'EOF'
spec:
  hostPID: true
  automountServiceAccountToken: false
  volumeMounts:
    - mountPath: /hostcgroup
      readOnly: true
EOF

  for profile_dir in "${work}"/deploy/rendered/*/; do
    [ -d "${profile_dir}" ] || continue
    profile="$(basename "${profile_dir}")"
    cat >"${work}/deploy/rendered/${profile}/40-daemonset-cogneva-process-census.yaml" <<'EOF'
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: cogneva-process-census
spec:
  template:
    spec:
      hostPID: true
      automountServiceAccountToken: false
      containers:
        - name: process-census
          volumeMounts:
            - name: host-cgroup
              mountPath: /hostcgroup
              readOnly: true
EOF
  done
  cp "${work}/deploy/rendered/k3s-single/40-daemonset-cogneva-process-census.yaml" \
    "${work}/deploy/k3s/process-census-daemonset.yaml"
}

run() { (cd "${work}" && bash "${checker}" "${work}" 2>&1); }

fixture
if ! out="$(run)"; then fail "完整夹具应当绿却红了：${out}"; fi

# 负例 1：没有 hostPID。`/proc` 里只有自己，第三方容器一个都读不到。
fixture
python3 - "${work}" <<'PY'
import sys, pathlib
root = pathlib.Path(sys.argv[1])
for path in root.glob("deploy/rendered/*/40-daemonset-cogneva-process-census.yaml"):
    text = path.read_text().replace("      hostPID: true\n", "")
    path.write_text(text)
pathlib.Path(root / "deploy/k3s/process-census-daemonset.yaml").write_text(
    (root / "deploy/rendered/k3s-single/40-daemonset-cogneva-process-census.yaml").read_text()
)
PY
if out="$(run)"; then fail "去掉 hostPID 之后应当红却绿了"; fi
grep -q "hostPID" <<<"${out}" || fail "报错没有点名 hostPID：${out}"

# 负例 2：静态清单里没有这份 DaemonSet（集群内 GitOps 拉取端就不会建它）。
fixture
rm -f "${work}/deploy/k3s/process-census-daemonset.yaml"
if out="$(run)"; then fail "静态清单缺这一份时应当红却绿了"; fi
grep -q "process-census-daemonset.yaml" <<<"${out}" || fail "报错没有点名静态清单：${out}"

# 负例 3：一个 profile 没有渲染出这份 DaemonSet。
fixture
rm -f "${work}/deploy/rendered/k3s-multi/40-daemonset-cogneva-process-census.yaml"
if out="$(run)"; then fail "某个 profile 缺这份 DaemonSet 时应当红却绿了"; fi
grep -q "k3s-multi" <<<"${out}" || fail "报错没有点名那个 profile：${out}"

# 负例 4：子命令没有接上。DaemonSet 里写的那条命令会落到完整应用启动上。
fixture
sed -i 's/Command::ProcessCensus/Command::HostLedger/' "${work}/crates/cogneva/src/main.rs"
if out="$(run)"; then fail "子命令没人分派时应当红却绿了"; fi

# 负例 5：cgroup 挂载不是只读。
fixture
sed -i 's/readOnly: true/readOnly: false/' "${work}/deploy/k3s/process-census-daemonset.yaml"
if out="$(run)"; then fail "cgroup 挂载可写时应当红却绿了"; fi
grep -q "只读" <<<"${out}" || fail "报错没有点名只读：${out}"

# 负例 6：多出一个 profile 目录，而它没有渲染出这份 DaemonSet。
# 判据的 profile 集合若是在脚本里写死的，这个目录根本不会进判据——新增的拓扑装上就是
# 没有采集器，而判词与「本来就没有这个 profile」一模一样。
fixture
mkdir -p "${work}/deploy/rendered/k3s-airgap"
if out="$(run)"; then fail "新增的 profile 目录没有采集器时应当红却绿了"; fi
grep -q "k3s-airgap" <<<"${out}" || fail "报错没有点名那个新 profile：${out}"

# 负例 7：一个 profile 目录都没有。这读作「判不了」，不是「全过」。
fixture
rm -rf "${work}/deploy/rendered"
if out="$(run)"; then fail "没有渲染产物时应当红却绿了"; fi
grep -q "判不了" <<<"${out}" || fail "报错没有说明判不了：${out}"

# 最后：真树必须绿。
if ! out="$(cd "${repo}" && bash "${checker}")"; then
  fail "真树应当绿却红了：${out}"
fi

echo "process-census-coverage.test: all checks passed"
