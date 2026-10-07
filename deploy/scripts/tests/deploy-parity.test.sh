#!/usr/bin/env bash
# check-deploy-parity.sh 的定向测试：比对面是**声明**出来的，而声明比实际做得宽不会
# 报错——比对落到名字上时，一类资源改哪个字段它都绿，判词照样写着「全对齐」。
#
# 为什么要有这份测试：实测（2026-10-07，59 个资源）这条门禁把其中 10 个按
# （kind, name）配对之后就再没读过内容——四条 NetworkPolicy（零凭证红线落地的位置：
# 进化 Pod 的 egress 白名单、沙盒执行器放行的端口）、两个 ServiceAccount、CronJob、
# Ingress、Namespace、StorageClass。同一形状还在往里一层：Pod 模板的 labels
# （NetworkPolicy 的 podSelector 与 Service 的 selector 都按它匹配，改名不报错、
# 只换受众）与 Service 的 selector/type/clusterIP 也没进过比对。
#
# 每个用例都在**真树的一份副本**上只改一处，然后断言它红的正是那一处、并且只报那一处：
# 「改坏了会绿」的判据不能靠它自己说自己绿（见 alert-rule-subjects.test.sh 同理）。
#
# 用例：
#   1. 真树 -> 绿，并报出读到多少资源、多少类（不是空跑：两侧都非空、每类都有决定）；
#   2. NetworkPolicy 放行端口只改一侧 -> 红，点名策略与字段；
#   3. ServiceAccount 上多一个 automountServiceAccountToken -> 红；
#   4. Pod 模板标签只改一侧 -> 红；
#   5. Service 的 selector 只改一侧 -> 红；
#   6. 两侧都多出一类没人决定怎么比的资源 -> 红；
#   7. 同名资源渲染出两份 -> 红（后一份顶掉前一份，被顶掉的那份没人比）；
#   8. 一侧渲染成空 -> 红（没有题目时全绿是句空话）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-deploy-parity.sh"
fail() { echo "FAIL: $*"; exit 1; }

for bin in helm kubectl python3; do
  command -v "${bin}" >/dev/null || { echo "缺少依赖：${bin}" >&2; exit 2; }
done

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 基线是**真树**的那两份渲染输入：门禁只读 deploy/k3s（kustomize）与 deploy/helm
# （chart），复制别的目录只会让「只改了一处」这件事变得难以相信。
mkdir -p "${work}/base/deploy"
cp -a "${repo}/deploy/k3s" "${work}/base/deploy/k3s"
cp -a "${repo}/deploy/helm" "${work}/base/deploy/helm"

case_dir() { # case_dir <名字> -> 打印一份可以随便改的树根
  local d="${work}/$1"
  mkdir -p "${d}"
  cp -a "${work}/base/deploy" "${d}/deploy"
  printf '%s' "${d}"
}

run() { # run <树根> -> 打印合并输出，回传退出码
  local out
  if out="$(bash "${gate}" "$1" 2>&1)"; then
    printf '%s\n' "${out}"; return 0
  else
    printf '%s\n' "${out}"; return 1
  fi
}

# 一处改动最多只该报一条差异。多于一条，说明这份副本上还有别的东西动了（改法本身
# 动了第二处，或者基线已经红了），那时「红的正是我改的那处」就不成立。
one_error() { # one_error <输出> <用例名>
  local n
  n="$(grep -c '^  - ' <<<"$1" || true)"
  [ "${n}" = "1" ] || fail "$2：期望只报一条差异，实际 ${n} 条：$1"
}

# 在一份清单里**按文档**定位一处字面量再改掉它。锚点串必须正好出现一次——锚点搬家
# 之后替换会变成空操作，而空操作的用例是绿的（变异要先断言它生效了）。
edit_doc() { # edit_doc <文件> <文档里必有的串> <旧串> <新串>
  python3 - "$@" <<'PY'
import re, sys
path, marker, old, new = sys.argv[1:5]
text = open(path, encoding='utf-8').read()
parts = re.split(r'(?m)^---$', text)
hits = [i for i, p in enumerate(parts) if marker in p]
assert len(hits) == 1, f"{path}: 标记 {marker!r} 命中 {len(hits)} 份文档，要正好一份"
i = hits[0]
n = parts[i].count(old)
assert n == 1, f"{path}: 锚点 {old!r} 在那份文档里出现 {n} 次，要正好一次"
parts[i] = parts[i].replace(old, new, 1)
open(path, 'w', encoding='utf-8').write('---'.join(parts))
PY
}

# 1. 真树必须绿，并且说出它读到了什么——报不出分母的绿与空跑分不开。
out="$(run "${work}/base")" || fail "真树被判红：${out}"
grep -q 'PARITY OK' <<<"${out}" || fail "绿的时候也要报判词：${out}"
grep -qE 'PARITY OK：[0-9]+ 个资源 / [0-9]+ 类' <<<"${out}" \
  || fail "判词没有报出读到多少资源、多少类：${out}"
read -r n_res n_kinds < <(sed -nE 's/.*PARITY OK：([0-9]+) 个资源 \/ ([0-9]+) 类.*/\1 \2/p' <<<"${out}")
[ "${n_res:-0}" -ge 40 ] || fail "读到的资源数 ${n_res:-0} 少于 40，这个数不像整棵树：${out}"
[ "${n_kinds:-0}" -ge 10 ] || fail "读到的资源类型数 ${n_kinds:-0} 少于 10：${out}"

# 2. NetworkPolicy 的放行端口：改的是进化 Pod 白名单里安全网关那一条。
d="$(case_dir np-port)"
edit_doc "${d}/deploy/k3s/network-policy.yaml" \
  'name: cogneva-evolution-deny-egress' \
  '          port: 8081' '          port: 8082'
if out="$(run "${d}")"; then fail "NetworkPolicy 放行端口只改了一侧却判绿：${out}"; fi
one_error "${out}" "NetworkPolicy 端口"
grep -q 'NetworkPolicy/cogneva-evolution-deny-egress' <<<"${out}" \
  || fail "判红却没点名那条策略：${out}"
grep -q 'object.spec.egress\[0\].ports\[1\].port' <<<"${out}" \
  || fail "判红却没点名那个字段：${out}"

# 3. ServiceAccount 自己身上的 automount：它是 Pod 之外的另一把开关。
d="$(case_dir sa-automount)"
edit_doc "${d}/deploy/k3s/evolution-rbac.yaml" \
  'kind: ServiceAccount
metadata:' \
  'kind: ServiceAccount' 'kind: ServiceAccount
automountServiceAccountToken: true'
if out="$(run "${d}")"; then fail "ServiceAccount 的 automount 只改了一侧却判绿：${out}"; fi
one_error "${out}" "ServiceAccount automount"
grep -q 'ServiceAccount/cogneva-evolution/object.automountServiceAccountToken' <<<"${out}" \
  || fail "判红却没点名那个 ServiceAccount 与字段：${out}"

# 4. Pod 模板标签：NetworkPolicy 的 podSelector 与 Service 的 selector 都是按它匹配的。
d="$(case_dir pod-labels)"
edit_doc "${d}/deploy/k3s/deployment.yaml" \
  'kind: Deployment' \
  '      labels:
        app.kubernetes.io/name: cogneva
        app.kubernetes.io/component: gateway' \
  '      labels:
        app.kubernetes.io/name: cogneva
        app.kubernetes.io/component: gateway-canary'
if out="$(run "${d}")"; then fail "Pod 模板标签只改了一侧却判绿：${out}"; fi
one_error "${out}" "Pod 模板标签"
grep -q 'Deployment/cogneva templateLabels' <<<"${out}" \
  || fail "判红却没点名那处模板标签：${out}"

# 5. Service 的 selector：换一个值就是换掉它指向的 Pod（Pod 照样 Running）。
d="$(case_dir svc-selector)"
edit_doc "${d}/deploy/k3s/service.yaml" \
  'kind: Service' \
  '  selector:
    app.kubernetes.io/name: cogneva
    app.kubernetes.io/component: gateway' \
  '  selector:
    app.kubernetes.io/name: cogneva
    app.kubernetes.io/component: gateway-canary'
if out="$(run "${d}")"; then fail "Service 的 selector 只改了一侧却判绿：${out}"; fi
one_error "${out}" "Service selector"
grep -q 'Service/cogneva/spec.selector.app.kubernetes.io/component' <<<"${out}" \
  || fail "判红却没点名那个 selector 键：${out}"

# 6. 两侧都多出一类没人决定怎么比的资源。名字都对着、集合也一致——这一格原来没人看。
d="$(case_dir uncovered-kind)"
cat >> "${d}/deploy/k3s/service.yaml" <<'YAML'
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cogneva-test-pdb
  namespace: cogneva
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/component: gateway
YAML
cat > "${d}/deploy/helm/cogneva/templates/pdb-test.yaml" <<'YAML'
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cogneva-test-pdb
  namespace: cogneva
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/component: gateway
YAML
if out="$(run "${d}")"; then fail "两侧都多出一类没人决定怎么比的资源却判绿：${out}"; fi
one_error "${out}" "未决定的资源类型"
grep -q '资源类型 PodDisruptionBudget 两侧都有' <<<"${out}" \
  || fail "判红却没点名那一类：${out}"

# 7. 同名资源渲染出两份：后一份顶掉前一份，被顶掉的那份从没进过比对（kustomize 自己
# 会拒重复 id，chart 不会——两个模板文件写出了同一个名字就是这种形状）。
d="$(case_dir duplicate-name)"
cat > "${d}/deploy/helm/cogneva/templates/duplicate-namespace.yaml" <<'YAML'
apiVersion: v1
kind: Namespace
metadata:
  name: cogneva
YAML
if out="$(run "${d}")"; then fail "同名资源渲染出两份却判绿：${out}"; fi
grep -q '同名资源：Namespace/cogneva' <<<"${out}" \
  || fail "判红却没点名那份被顶掉的资源：${out}"

# 8. 一侧渲染成空：资源集合、字段、覆盖决定全部没有题目，那时候的「全绿」是句空话。
d="$(case_dir empty-render)"
printf 'apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: []\n' \
  > "${d}/deploy/k3s/kustomization.yaml"
if out="$(run "${d}")"; then fail "一侧渲染成空却判绿：${out}"; fi
grep -q '两侧都渲染出东西才谈得上对齐：k3s=0 个资源' <<<"${out}" \
  || fail "判红却没说出两侧各读到多少：${out}"

echo "DEPLOY PARITY GATE TESTS OK"
