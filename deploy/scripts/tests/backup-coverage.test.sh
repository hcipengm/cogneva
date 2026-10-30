#!/usr/bin/env bash
# principle: feedback_a_census_needs_a_source_outside_itself
# check-backup-coverage.sh 判的是「每个挂到数据目录的 claim 都进了备份面」。它曾经
# 还判第二件事（数据根一级子目录逐个分类），那一条已经搬进代码：
#   - 判词在 `crates/cogneva/src/backup.rs` 的 `DATA_ROOT_SUBDIRS`；
#   - 判据在 `crates/cogneva/tests/data_root_subdirs.rs`（普查面从代码、配置、渲染物
#     数出来，不是登记表自己当普查面——那样的判据恒真）。
# 这个测试只剩卷那一半，用例都是「漏一个 claim 或挂一块没人用的卷」的变体。
#
# 用例：
#   1. 真树 -> 绿；
#   2. 工作负载挂了 claim，备份 Job 没挂 -> 红，点名那个 claim；
#   3. 备份 Job 挂了一块没有任何工作负载使用的 claim -> 红；
#   4. 备份 CronJob 一个数据根都没挂 -> 红（空不是绿）；
#   5. 没有任何工作负载挂到数据目录 -> 红（空不是绿）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 门禁会读渲染物与配置两处；副本里两处都要有，但可以很小——判据只有「工作负载
# 挂在数据目录上的 claim」与「备份 CronJob 的卷声明」两张集合。
mirror() {
  rm -rf "${work}/root"
  mkdir -p "${work}/root/deploy/rendered/k3s-single" \
           "${work}/root/deploy/helm/cogneva/files" \
           "${work}/root/deploy/scripts"
  cp "${repo}/deploy/scripts/check-backup-coverage.sh" "${work}/root/deploy/scripts/"
  cp "${repo}/deploy/helm/cogneva/files/cogneva.json" "${work}/root/deploy/helm/cogneva/files/"
  cat > "${work}/root/deploy/rendered/k3s-single/app.yaml" <<'YAML'
kind: Deployment
metadata: {name: demo}
spec:
  template:
    spec:
      containers: [{name: app, volumeMounts: [{name: v, mountPath: /var/lib/cogneva-data}]}]
      volumes: [{name: v, persistentVolumeClaim: {claimName: cogneva-data-pvc}}]
YAML
  cat > "${work}/root/deploy/rendered/k3s-single/backup.yaml" <<'YAML'
kind: CronJob
metadata: {name: cogneva-backup}
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers: [{name: backup, volumeMounts: [{name: b, mountPath: /var/lib/cogneva-data}]}]
          volumes: [{name: b, persistentVolumeClaim: {claimName: cogneva-data-pvc}}]
YAML
}

run_gate() { bash "${work}/root/deploy/scripts/check-backup-coverage.sh" "${work}/root" 2>&1; }

# 1) 真树绿
mirror
out="$(run_gate)" || fail "真树应当绿，实际红：${out}"
grep -q "备份挂载" <<<"${out}" || fail "绿时没有报出备份挂载面：${out}"

# 2) 工作负载挂了 claim，备份 Job 没挂
mirror
cat > "${work}/root/deploy/rendered/k3s-single/backup.yaml" <<'YAML'
kind: CronJob
metadata: {name: cogneva-backup}
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers: [{name: backup, volumeMounts: [{name: b, mountPath: /var/lib/cogneva-data}]}]
          volumes: [{name: b, persistentVolumeClaim: {claimName: some-other-pvc}}]
YAML
out="$(run_gate)" && fail "漏挂的 claim 应当红，实际绿：${out}"
grep -q "cogneva-data-pvc" <<<"${out}" || fail "红时没有点名漏挂的 claim：${out}"

# 3) 备份 Job 挂了一块没人用的卷
mirror
cat > "${work}/root/deploy/rendered/k3s-single/backup.yaml" <<'YAML'
kind: CronJob
metadata: {name: cogneva-backup}
spec:
  jobTemplate:
    spec:
      template:
        spec:
          # 两个容器各把一块卷挂到数据目录：一块是工作负载用的，一块没有任何工作
          # 负载用过。判据按 claim 判，所以第二块必须真的挂到数据目录才进面——
          # 只是声明在 volumes 里不算，那不是「备份了它」。
          containers:
            - {name: backup, volumeMounts: [{name: b, mountPath: /var/lib/cogneva-data}]}
            - {name: stray, volumeMounts: [{name: u, mountPath: /var/lib/cogneva-data}]}
          volumes:
            - {name: b, persistentVolumeClaim: {claimName: cogneva-data-pvc}}
            - {name: u, persistentVolumeClaim: {claimName: nobody-uses-me}}
YAML
out="$(run_gate)" && fail "没人用的备份卷应当红，实际绿：${out}"
grep -q "nobody-uses-me" <<<"${out}" || fail "红时没有点名那块卷：${out}"

# 4) 备份 CronJob 一个数据根都没挂
mirror
cat > "${work}/root/deploy/rendered/k3s-single/backup.yaml" <<'YAML'
kind: CronJob
metadata: {name: cogneva-backup}
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers: [{name: backup}]
          volumes: []
YAML
out="$(run_gate)" && fail "备份 Job 没有挂到数据目录应当红，实际绿：${out}"
grep -q "没有挂到数据目录" <<<"${out}" || fail "红时没有说是备份没挂：${out}"

# 5) 没有任何工作负载挂到数据目录
mirror
cat > "${work}/root/deploy/rendered/k3s-single/app.yaml" <<'YAML'
kind: Deployment
metadata: {name: demo}
spec:
  template:
    spec:
      containers: [{name: app}]
      volumes: []
YAML
out="$(run_gate)" && fail "没有任何工作负载挂数据目录应当红，实际绿：${out}"
grep -q "没有任何工作负载挂到数据目录" <<<"${out}" || fail "红时没有说是空数据根：${out}"

echo "backup-coverage: 5 个用例全绿"
