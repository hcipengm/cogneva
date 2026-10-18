#!/usr/bin/env bash
# principle: feedback_agreement_between_copies_says_nothing_about_the_relations_inside_them
# check-control-plane-reachability.sh 的定向测试：门禁只读退出码，所以「它判得出来」
# 这句话只能由反例证明——在一份真渲染产物的副本上只改一处，断言它红的正是那一处。
#
# 每个用例都从**真树**复制一份 deploy/rendered，改一处，跑门禁，然后断言：
#   - 退出码是 1（不是 0：改了还绿就是没判；也不是 2：2 是「判不了」，会把缺陷读成缺依赖）；
#   - 判词点名了被改的对象（不是只报一个总数）；
#   - 真树自己跑是 0，并且报出了读过的 profile 数（空跑全绿是句空话）。
#
# 用例：
#   1. 真树副本不改 -> 0，并印出 profile 数；
#   2. Service selector 改到一个没人带的标签 -> 红，点名 Service 与 selector；
#   3. Service targetPort 改成没声明的端口名 -> 红，点名端口名；
#   4. Deployment replicas 改成 0 -> 红，点名工作负载；
#   5. 探针按名字指向一个没声明的端口 -> 红，点名容器与探针；
#   6. 卷指向一个不存在的 PVC -> 红，点名 PVC；
#   7. Ingress 后端指向不存在的 Service -> 红，点名 Ingress；
#   8. rendered 目录为空 -> 2（判不了，不是 0）。
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

case_tree() { # case_tree <名字> -> 打印一份可以随便改的树根
  local d="${work}/$1"
  mkdir -p "${d}"
  cp -a "${base}/deploy" "${d}/deploy"
  printf '%s' "${d}"
}

expect_red() { # expect_red <名字> <期望出现在判词里的片段> <python 改动片段>
  local case_name="$1" needle="$2" mutation="$3"
  local tree; tree="$(case_tree "${case_name}")"
  CASE_TREE="${tree}" python3 - "${mutation}" <<'PY'
import os, sys, yaml

tree = os.environ["CASE_TREE"]
mutation = sys.argv[1]
path = os.path.join(tree, "deploy/rendered/k3s-single/")

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
printf '%s' "${out}" | grep -Eq '控制面可达性：[0-9]+ 个 profile 全过' \
  || fail "clean: 没有报出读过的 profile 数；输出：${out}"
printf '%s' "${out}" | grep -Fq '3 个 profile' \
  || fail "clean: 期望读到 3 个 profile；输出：${out}"
echo "ok   clean：干净副本全过并报出 profile 数"

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

echo "control-plane-reachability.test.sh: 8 用例全过"
