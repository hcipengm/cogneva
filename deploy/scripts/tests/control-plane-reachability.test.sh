#!/usr/bin/env bash
# principle: feedback_agreement_between_copies_says_nothing_about_the_relations_inside_them
# check-control-plane-reachability.sh 的定向测试：门禁只读退出码，所以「它判得出来」
# 这句话只能由反例证明——在一份真渲染产物的副本上只改一处，断言它红的正是那一处。
#
# 每个用例都从**真树**复制一份 deploy（`deploy/rendered` 与 `deploy/k3s` 两条投递
# 路径都在，缺一条门禁会报「判不了」），改一处，跑门禁，然后断言：
#   - 退出码是 1（不是 0：改了还绿就是没判；也不是 2：2 是「判不了」，会把缺陷读成缺依赖）；
#   - 判词点名了被改的对象（不是只报一个总数）；
#   - 真树自己跑是 0，并且报出了读过的 profile 数与交付树（空跑全绿是句空话）。
#
# 用例：
#   1. 真树副本不改 -> 0，并印出 profile 数与交付树；
#   2. Service selector 改到一个没人带的标签 -> 红，点名 Service 与 selector；
#   3. Service targetPort 改成没声明的端口名 -> 红，点名端口名；
#   4. Deployment replicas 改成 0 -> 红，点名工作负载；
#   5. 探针按名字指向一个没声明的端口 -> 红，点名容器与探针；
#   6. 卷指向一个不存在的 PVC -> 红，点名 PVC；
#   7. Ingress 后端指向不存在的 Service -> 红，点名 Ingress；
#   8. rendered 目录为空 -> 2（判不了，不是 0）；
#   9. **交付树**（`deploy/k3s`）里的 Service selector 改到没人带的标签 -> 红，判词
#      点名的是交付树那条路径（两条路径各自被判，一条绿不掩盖另一条）；
#   10. `kustomization.yaml` 列了一个不存在的文件 -> 2（读不出交付对象集，不是全过）；
#   11. NetworkPolicy 的同命名空间对端选不中任何工作负载 -> 红，点名那条放行；
#   12. 跨命名空间对端按名字指了一个 deploy/ 没声明过的 namespace -> 红，点名那个名字；
#   13. 出站放行端口拼错（没有 Pod 声明、配置里也没拼过） -> 红，点名端口；
#   14. 入站放行端口拼错（端口落在被选中的 Pod 上，与来源无关） -> 红；
#   15. 整条策略一个 Pod 都不管（subject 与同命名空间对端一起选不中） -> 红；
#   16. 3306 这条放行只有配置里的 endpoint 端口保着：改掉配置 -> 红（证明那个第二来源
#       真的在判，不是摆设）；
#   17. 定时任务的 Pod 模板也在判（以前 Job/CronJob 的 Pod 一条都不判）；
#   18. 写了某方向的规则而 policyTypes 里没有那个方向 -> 红（规则一条都不生效）；
#   19. Pod 的 serviceAccountName 在树里没声明 -> 红（Pod 会拿到 default 身份）；
#   20. RoleBinding 的 roleRef 指向不存在的角色 -> 红（授予落在空集上）；
#   21. 绑定的主体是一个没人拥有的 ServiceAccount -> 红；
#   22. Ingress 注解里的中间件名在树里没有同名对象 -> 红（traefik 只是不套这层中间件）；
#   23. 中间件注解少了 provider 后缀 -> 红（这条引用同样不生效）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-control-plane-reachability.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 -c 'import yaml' 2>/dev/null || { echo "缺少依赖：python3-yaml" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

base="${work}/base"
mkdir -p "${base}/deploy"
cp -a "${repo}/deploy/rendered" "${base}/deploy/rendered"
cp -a "${repo}/deploy/k3s" "${base}/deploy/k3s"

case_tree() { # case_tree <名字> -> 打印一份可以随便改的树根
  local d="${work}/$1"
  mkdir -p "${d}"
  cp -a "${base}/deploy" "${d}/deploy"
  printf '%s' "${d}"
}

expect_red() { # expect_red <名字> <期望出现在判词里的片段> <python 改动片段> [改哪棵树]
  local case_name="$1" needle="$2" mutation="$3" at="${4:-deploy/rendered/k3s-single}"
  local tree; tree="$(case_tree "${case_name}")"
  CASE_DIR="${tree}/${at}/" python3 - "${mutation}" <<'PY'
import os, sys, yaml

mutation = sys.argv[1]
path = os.environ["CASE_DIR"]

# 改动用 exec 执行，脚本只给三样东西：读一个文件、写回一个文件、遍历文档。
def load(fname):
    p = os.path.join(path, fname)
    with open(p, encoding="utf-8") as fh:
        return [d for d in yaml.safe_load_all(fh) if d]

def save(fname, docs):
    p = os.path.join(path, fname)
    with open(p, "w", encoding="utf-8") as fh:
        yaml.safe_dump_all(docs, fh, allow_unicode=True)

def edit(fname, fn):
    docs = load(fname)
    for d in docs:
        fn(d)
    save(fname, docs)

namespace = {"load": load, "save": save, "edit": edit}
exec(mutation, namespace)
PY
  local out rc
  set +e
  out="$(bash "${gate}" "${tree}" 2>&1)"
  rc=$?
  set -e
  [ "${rc}" -eq 1 ] || fail "${case_name}: 期望退出码 1，实得 ${rc}；输出：${out}"
  printf '%s' "${out}" | grep -Fq -- "${needle}" \
    || fail "${case_name}: 判词里没有 ${needle}；输出：${out}"
  printf '%s' "${out}" | grep -Fq "控制面可达性" \
    || fail "${case_name}: 输出里没有总数行；输出：${out}"
  echo "ok   ${case_name}：${needle}"
}

# 1. 真树副本不改。
clean_tree="$(case_tree clean)"
out="$(bash "${gate}" "${clean_tree}" 2>&1)" || fail "clean: 干净副本不应为红；输出：${out}"
printf '%s' "${out}" | grep -Eq '控制面可达性：[0-9]+ 个 profile 与交付树 .* 全过' \
  || fail "clean: 没有报出读过的 profile 数与交付树；输出：${out}"
printf '%s' "${out}" | grep -Fq '3 个 profile' \
  || fail "clean: 期望读到 3 个 profile；输出：${out}"
printf '%s' "${out}" | grep -Fq '交付树 deploy/k3s' \
  || fail "clean: 没有报出交付树；输出：${out}"
echo "ok   clean：干净副本全过并报出 profile 数与交付树"

# 2. Service selector 选不中任何工作负载。
expect_red "selector-miss" "Service cogneva 的 selector" '
edit("30-service-cogneva.yaml", lambda d: d["spec"].__setitem__("selector", {"app.kubernetes.io/name": "cogneva", "app.kubernetes.io/component": "gateway-typo"}))
'

# 3. Service targetPort 名在选中工作负载里没声明。
expect_red "targetport-miss" "targetPort 名 'http-typo'" '
edit("30-service-cogneva.yaml", lambda d: d["spec"]["ports"][0].__setitem__("targetPort", "http-typo"))
'

# 4. 工作负载 replicas 改成 0。
expect_red "replicas-zero" "replicas 是 0" '
edit("41-deployment-cogneva.yaml", lambda d: d["spec"].__setitem__("replicas", 0))
'

# 5. 探针按名字指向没声明的端口。
expect_red "probe-port-miss" "readinessProbe" '
def mutate(d):
    d["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["httpGet"]["port"] = "api-typo"
edit("41-deployment-cogneva.yaml", mutate)
'

# 6. 卷指向不存在的 PVC。
expect_red "pvc-miss" "不存在的 PVC" '
def mutate(d):
    d["spec"]["template"]["spec"]["volumes"].append({"name": "extra", "persistentVolumeClaim": {"claimName": "no-such-pvc"}})
edit("41-deployment-cogneva.yaml", mutate)
'

# 7. Ingress 后端指向不存在的 Service。
expect_red "ingress-backend-miss" "Ingress cogneva 的后端指向不存在的 Service" '
def mutate(d):
    d["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"] = "cogneva-typo"
edit("60-ingress-cogneva.yaml", mutate)
'

# 8. 判不了必须不是 0。
empty="${work}/empty"
mkdir -p "${empty}/deploy/rendered"
set +e
out="$(bash "${gate}" "${empty}" 2>&1)"
rc=$?
set -e
[ "${rc}" -eq 2 ] || fail "empty: 期望退出码 2，实得 ${rc}；输出：${out}"
printf '%s' "${out}" | grep -Fq "判不了" || fail "empty: 输出里没有「判不了」；输出：${out}"
echo "ok   empty：没有产物时退出码 2（不是 0）"

# 9. 交付树自己的形状也要判：另一条投递路径绿，不掩盖这一条红。
expect_red "apply-tree-selector-miss" "deploy/k3s：Service cogneva 的 selector" '
edit("service.yaml", lambda d: d["spec"]["selector"].__setitem__("app.kubernetes.io/component", "gateway-typo"))
' "deploy/k3s"

# 10. 读不出交付对象集必须不是 0。
ghost="$(case_tree kustomization-ghost)"
printf 'resources:\n  - no-such-file.yaml\n' > "${ghost}/deploy/k3s/kustomization.yaml"
set +e
out="$(bash "${gate}" "${ghost}" 2>&1)"
rc=$?
set -e
[ "${rc}" -eq 2 ] || fail "kustomization-ghost: 期望退出码 2，实得 ${rc}；输出：${out}"
printf '%s' "${out}" | grep -Fq "判不了" || fail "kustomization-ghost: 输出里没有「判不了」；输出：${out}"
echo "ok   kustomization-ghost：交付对象集读不出来时退出码 2（不是 0）"

# 11. NetworkPolicy 的同命名空间对端选不中任何 Pod。
expect_red "peer-selector-miss" "egress 目的对端 podSelector" '
def mutate(d):
    d["spec"]["egress"][0]["to"][0]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] = "security-gatewy"
edit("50-networkpolicy-cogneva-evolution-deny-egress.yaml", mutate)
'

# 12. 跨命名空间对端按名字指了一个不存在的 namespace。
expect_red "namespace-name-miss" "按名字指 namespace 'monitering'" '
def mutate(d):
    for rule in d["spec"]["egress"]:
        for peer in rule.get("to") or []:
            ns = (peer.get("namespaceSelector") or {}).get("matchLabels") or {}
            if "kubernetes.io/metadata.name" in ns:
                ns["kubernetes.io/metadata.name"] = "monitering"
edit("50-networkpolicy-cogneva-evolution-deny-egress.yaml", mutate)
'

# 13. 出站放行端口拼错：没有 Pod 声明过它，配置里也没有 endpoint 用它。
expect_red "egress-port-miss" "放行端口 6739" '
def mutate(d):
    for rule in d["spec"]["egress"]:
        for port in rule.get("ports") or []:
            if port.get("port") == 6379:
                port["port"] = 6739
edit("50-networkpolicy-cogneva-evolution-deny-egress.yaml", mutate)
'

# 14. 入站放行端口拼错：端口落在被策略选中的 Pod 上，与来源无关，照样判。
expect_red "ingress-port-miss" "放行端口 8086" '
def mutate(d):
    d["spec"]["ingress"][1]["ports"][0]["port"] = 8086
edit("50-networkpolicy-cogneva-internal-ingress.yaml", mutate)
'

# 15. 整条策略一个 Pod 都不管：subject 与所有同命名空间对端一起选不中。
expect_red "dead-policy" "这份策略谁都不管" '
def mutate(d):
    d["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] = "gatewy"
    for rule in d["spec"]["ingress"]:
        for peer in rule.get("from") or []:
            labels = (peer.get("podSelector") or {}).get("matchLabels") or {}
            if "app.kubernetes.io/component" in labels:
                labels["app.kubernetes.io/component"] = "gatewy"
            if labels.get("app.kubernetes.io/name") == "cogneva":
                labels["app.kubernetes.io/name"] = "cogneva-typo"
edit("50-networkpolicy-cogneva-internal-ingress.yaml", mutate)
'

# 16. 3306 这条放行今天只有配置保着：把 `providers.db.options.port` 改掉，它就得红。
expect_red "config-endpoint-gone" "放行端口 3306" '
def mutate(d):
    d["data"] = {k: v.replace("\"port\": 3306", "\"port\": 3307")
                  for k, v in (d.get("data") or {}).items()}
edit("10-configmap-cogneva-json.yaml", mutate)
'

# 17. 定时任务的 Pod 模板也吃第 3～5 族（以前 Job/CronJob 的 Pod 一条都不判）。
expect_red "cronjob-template-judged" "CronJob cogneva-backup 的容器 backup" '
def mutate(d):
    container = d["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
    container.setdefault("envFrom", []).append({"configMapRef": {"name": "no-such-cm"}})
edit("90-cronjob-cogneva-backup.yaml", mutate)
'

# 18. 写了某个方向的规则，policyTypes 里却没有那个方向：规则一条都不生效。
expect_red "policy-type-omits-rules" "policyTypes 里却没有 Egress" '
def mutate(d):
    d["spec"]["policyTypes"] = ["Ingress"]
edit("50-networkpolicy-cogneva-security-gateway.yaml", mutate)
'

# 19. Pod 指名的工作负载身份在树里没有声明：Pod 会拿到 default 身份。
expect_red "service-account-miss" "serviceAccountName 'cogneva-typo'" '
def mutate(d):
    d["spec"]["template"]["spec"]["serviceAccountName"] = "cogneva-typo"
edit("41-deployment-cogneva.yaml", mutate)
'

# 20. RoleBinding 的 roleRef 指向不存在的角色：绑定照建，授予的东西落在空集上。
expect_red "role-ref-miss" "roleRef 指向 Role 'cogneva-evolution-typo'" '
def mutate(d):
    d["roleRef"]["name"] = "cogneva-evolution-typo"
edit("17-rolebinding-cogneva-evolution.yaml", mutate)
'

# 21. 绑定的主体是一个没人拥有的身份。
expect_red "binding-subject-miss" "主体 ServiceAccount 'cogneva-evolution-typo'" '
def mutate(d):
    d["subjects"][0]["name"] = "cogneva-evolution-typo"
edit("17-rolebinding-cogneva-evolution.yaml", mutate)
'

# 22. Ingress 注解按名字引用的 Middleware 在树里没有同名对象：注解是字符串、对象是
#     另一份清单，指不到时 traefik 不报错、只是不套这层中间件。
expect_red "middleware-name-miss" "里没有这个 Middleware" '
def mutate(d):
    if d.get("kind") != "Ingress":
        return
    d["metadata"]["annotations"]["traefik.ingress.kubernetes.io/router.middlewares"] = \
        "websocket-headers-typo@kubernetescrd"
edit("60-ingress-cogneva.yaml", mutate)
'

# 23. 中间件注解少了 provider 后缀：traefik 解析不出 provider，这条引用同样不生效。
expect_red "middleware-no-provider" "没有 provider 后缀" '
def mutate(d):
    if d.get("kind") != "Ingress":
        return
    d["metadata"]["annotations"]["traefik.ingress.kubernetes.io/router.middlewares"] = \
        "websocket-headers"
edit("60-ingress-cogneva.yaml", mutate)
'

echo "control-plane-reachability.test.sh: 23 用例全过"
