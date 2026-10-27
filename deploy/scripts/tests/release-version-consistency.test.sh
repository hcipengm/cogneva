#!/usr/bin/env bash
# 发布版本一致性门禁的夹具测试。
#
# 判据是「交付物里写着发布版本的每一处都等于声明版本」，集合从树里读出来：
# chart 的 version/appVersion、values 的 image.tag、deploy/k3s 下每个本平台工作负载
# 的 Pod 模板标签、configmap 的 COGNEVA_APP_VERSION。这条门原先是一份写死的四文件
# 名单，树里另外五个跑本平台镜像的工作负载不在名单上——它们的标签停在上一版，
# 而门读作绿。所以这里每个负例都是「树上多出/改坏一处」，而不是「名单少一项」：
#   - 第 5 个工作负载没有版本标签   → 红（名单时代它是绿的）
#   - 某个工作负载的标签是上一版     → 红
#   - 新工作负载的标签是上一版       → 红（它从来不进名单）
#   - 第三方工作负载的标签是自己的版本 → 不判（拿发布版本去比是错的）
#   - values 的 tag 与声明版本不同    → 红
#   - 一个本平台工作负载都没有        → 判不了（退出码 2），不是全过
# 最后在真树上跑一遍。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-release-version-consistency.sh"
pass=0

fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 一份最小但形状相同的树：声明版本、chart、values、主应用 configmap，两个本平台的
# 工作负载（一个 Deployment、一个 DaemonSet），一个第三方的 StatefulSet。
fixture() {
    rm -rf "${work}/Cargo.toml" "${work}/deploy"
    mkdir -p "${work}/deploy/helm/cogneva" "${work}/deploy/k3s" "${work}/deploy/scripts"
    cp "${repo}/deploy/scripts/declared-version.sh" "${work}/deploy/scripts/"
    cat >"${work}/Cargo.toml" <<'EOF'
[workspace]
members = []

[workspace.package]
version = "9.9.9"
EOF
    cat >"${work}/deploy/helm/cogneva/Chart.yaml" <<'EOF'
apiVersion: v2
name: cogneva
version: 9.9.9
appVersion: "9.9.9"
EOF
    cat >"${work}/deploy/helm/cogneva/values.yaml" <<'EOF'
image:
  repository: localhost:30500/cogneva
  tag: "9.9.9"
EOF
    cat >"${work}/deploy/k3s/configmap.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: cogneva-config
data:
  COGNEVA_APP_VERSION: "9.9.9"
EOF
    cat >"${work}/deploy/k3s/deployment.yaml" <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cogneva
  labels:
    app.kubernetes.io/name: cogneva
    app.kubernetes.io/version: "9.9.9"
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: cogneva
  template:
    metadata:
      labels:
        app.kubernetes.io/name: cogneva
        app.kubernetes.io/component: gateway
        app.kubernetes.io/version: "9.9.9"
    spec:
      containers:
        - name: cogneva
          image: localhost:30500/cogneva:local
EOF
    cat >"${work}/deploy/k3s/host-ledger-daemonset.yaml" <<'EOF'
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: cogneva-host-ledger
  labels:
    app.kubernetes.io/name: cogneva
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: cogneva
  template:
    metadata:
      labels:
        app.kubernetes.io/name: cogneva
        app.kubernetes.io/component: host-ledger
        app.kubernetes.io/version: "9.9.9"
    spec:
      containers:
        - name: host-ledger
          image: localhost:30500/cogneva:local
EOF
    cat >"${work}/deploy/k3s/postgres-deployment.yaml" <<'EOF'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: postgres
        app.kubernetes.io/version: "16"
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
EOF
}

run() { bash "${checker}" "${work}" 2>&1; }

# 1. 形状齐的树要绿：这是后面每条负例的对照。
fixture
if ! out="$(run)"; then fail "形状齐的最小树应当绿却红了：${out}"; fi
grep -q "9.9.9" <<<"${out}" || fail "成功时没有报出声明版本：${out}"
pass=$((pass + 1))

# 2. 树里第五个工作负载没有版本标签。名单时代它不在名单上，门是绿的。
fixture
cat >"${work}/deploy/k3s/claim-store-daemonset.yaml" <<'EOF'
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: cogneva-claim-store
  labels:
    app.kubernetes.io/name: cogneva
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: cogneva
  template:
    metadata:
      labels:
        app.kubernetes.io/name: cogneva
        app.kubernetes.io/component: claim-store
    spec:
      containers:
        - name: claim-store
          image: localhost:30500/cogneva:local
EOF
if out="$(run)"; then fail "没有版本标签的工作负载应当红却绿了：${out}"; fi
grep -q "claim-store" <<<"${out}" || fail "报错没有点名那个工作负载：${out}"
pass=$((pass + 1))

# 3. 某个工作负载的 Pod 模板写着上一版。
fixture
sed -i 's/        app.kubernetes.io\/version: "9.9.9"/        app.kubernetes.io\/version: "9.9.8"/' \
    "${work}/deploy/k3s/host-ledger-daemonset.yaml"
if out="$(run)"; then fail "Pod 模板停在上一版应当红却绿了：${out}"; fi
grep -q "9.9.8" <<<"${out}" || fail "报错没有点出那个旧版本：${out}"
pass=$((pass + 1))

# 4. 新增的工作负载（从来没在任何名单里）带着上一版的标签。
fixture
cat >"${work}/deploy/k3s/cluster-registry.yaml" <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cogneva-registry
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: cogneva
  template:
    metadata:
      labels:
        app.kubernetes.io/name: cogneva
        app.kubernetes.io/component: cluster-registry
        app.kubernetes.io/version: "9.9.8"
    spec:
      containers:
        - name: registry
          image: registry:2
EOF
if out="$(run)"; then fail "新工作负载停在上一版应当红却绿了：${out}"; fi
grep -q "cogneva-registry" <<<"${out}" || fail "报错没有点名这个新工作负载：${out}"
pass=$((pass + 1))

# 5. 第三方工作负载写的是它自己的版本（postgres 16）：不归这条判，不能红。
fixture
if ! out="$(run)"; then fail "第三方工作负载的版本标签被误判：${out}"; fi
pass=$((pass + 1))

# 6. values.yaml 的 image.tag 与声明版本不同。
fixture
sed -i 's/tag: "9.9.9"/tag: "9.9.8"/' "${work}/deploy/helm/cogneva/values.yaml"
if out="$(run)"; then fail "values 的 tag 漂移应当红却绿了：${out}"; fi
grep -q "image.tag" <<<"${out}" || fail "报错没有点名 image.tag：${out}"
pass=$((pass + 1))

# 7. 树上一个本平台工作负载都没有：判不了，退出码 2，不是全过。
fixture
rm -f "${work}/deploy/k3s/deployment.yaml" "${work}/deploy/k3s/host-ledger-daemonset.yaml"
set +e
out="$(run)"
status=$?
set -e
[ "${status}" -ne 0 ] || fail "一个工作负载都没有时应当红却绿了：${out}"
[ "${status}" -eq 2 ] || fail "判不了要用退出码 2，实际是 ${status}：${out}"
grep -q "判不了" <<<"${out}" || fail "报错没有说明判不了：${out}"
pass=$((pass + 1))

# 8. 真树：门必须绿，且要点出读到的工作负载数。
if ! out="$(bash "${checker}" "${repo}" 2>&1)"; then
    fail "真树应当绿却红了：${out}"
fi
grep -q "本平台" <<<"${out}" || fail "真树的成功判词没有说读到的是什么：${out}"
pass=$((pass + 1))

echo "release-version-consistency.test.sh: ${pass} 用例全过"
