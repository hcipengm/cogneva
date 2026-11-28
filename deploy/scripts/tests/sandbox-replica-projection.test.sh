#!/usr/bin/env bash
# 沙箱执行器的副本数是**部署里的一个数**，不是模板里的一处硬编码：加一副副本，
# 就多一块卷，客户端下一次解析名字时就多一条能钉上去的地址。这两件事的形状不同，
# 判据也不同：
#
#   1. 卷：StatefulSet 的 `volumeClaimTemplates`——一副副本一块，卷名是
#      `<模板>-<StatefulSet>-<序号>`，容器按 `$(POD_NAME)` 拼出自己那块。换副本数
#      ⇒ 这个模板跟着变；
#   2. 地址：`cogneva.json` 的 `system.sandbox_executor_url` 是**无头服务的名字**，
#      不是一个逗号表。换副本数 ⇒ 这个叶子一个字节都不变——客户端在运行时解析名字，
#      副本增减只让它下一次解析看到不同的表。名字与副本数脱钩，才叫「加副本不需要
#      改任何配置」。
#
# 判据因此是两条相反的：卷随输入变，地址不随输入变。只测前者，一份写死的地址表
# 会把副本数锁回渲染时的那一个；只测后者，卷可能悄悄退回共享的一块。
#
# 需要的工具与 check-deploy-parity.sh 同一套：helm、python3-yaml。
# 用法：bash deploy/scripts/tests/sandbox-replica-projection.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*" >&2; exit 1; }

for bin in helm python3; do
    command -v "${bin}" >/dev/null || { echo "缺少依赖：${bin}" >&2; exit 2; }
done
python3 -c 'import yaml' 2>/dev/null || { echo "缺少依赖：python3-yaml" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

render() { # render <输出文件> [--set …]
    local out="$1"; shift
    bash "${repo}/deploy/scripts/render-chart.sh" k3s-single -- "$@" >"${out}" \
        || fail "渲染失败（k3s-single $*）"
    [ -s "${out}" ] || fail "渲染出来是空的（k3s-single $*）"
}

render "${work}/one.yaml"
render "${work}/three.yaml" --set sandboxExecutor.replicas=3

python3 - "${work}/one.yaml" "${work}/three.yaml" <<'PY'
import json
import sys

import yaml


def objects(path):
    with open(path) as f:
        return [d for d in yaml.safe_load_all(f) if isinstance(d, dict)]


def one(docs, kind, name, path):
    hits = [
        d
        for d in docs
        if d.get("kind") == kind and (d.get("metadata") or {}).get("name") == name
    ]
    assert len(hits) == 1, f"{path}: {kind}/{name} 渲染出 {len(hits)} 份"
    return hits[0]


def executor_url(path):
    docs = objects(path)
    config = one(docs, "ConfigMap", "cogneva-json", path)
    return json.loads(config["data"]["cogneva.json"])["system"]["sandbox_executor_url"]


def check_volumes(path, replicas):
    docs = objects(path)
    sts = one(docs, "StatefulSet", "cogneva-sandbox-executor", path)

    # 1. 副本数就是传进去的那一个数。
    assert sts["spec"]["replicas"] == replicas, (
        f"{path}: replicas={sts['spec']['replicas']}，期望 {replicas}"
    )
    # 2. 一副副本一块卷：卷模板必须存在，且模板名与容器挂载的那个卷名对得上。
    templates = (sts["spec"].get("volumeClaimTemplates") or [])
    assert templates, f"{path}: StatefulSet 没有 volumeClaimTemplates，卷不是每副副本自己的"
    names = []
    for t in templates:
        names.append(t["metadata"]["name"])
        assert not t.get("spec", {}).get("volumeName"), (
            f"{path}: 卷模板绑定了 volumeName，那就不是一副副本一块卷了"
        )
    # 3. Pod 模板里不能有同名的 volumes 条目：卷由控制器注入，写两处就是两份事实。
    pod = sts["spec"]["template"]["spec"]
    pod_volumes = [v.get("name") for v in (pod.get("volumes") or [])]
    for name in names:
        assert name not in pod_volumes, (
            f"{path}: 卷 {name} 既有 volumeClaimTemplates 又有 Pod 级 volumes 条目"
        )
    mounted = {
        m.get("name")
        for c in (pod.get("containers") or []) + (pod.get("initContainers") or [])
        for m in (c.get("volumeMounts") or [])
    }
    for name in names:
        assert name in mounted, f"{path}: 卷模板 {name} 没有被任何容器挂载"


def check_name_form(path):
    docs = objects(path)
    sts = one(docs, "StatefulSet", "cogneva-sandbox-executor", path)
    headless = one(docs, "Service", sts["spec"]["serviceName"], path)
    assert headless["spec"].get("clusterIP") == "None", (
        f"{path}: {sts['spec']['serviceName']} 不是无头 Service，解析名字拿不到逐副本地址"
    )
    port = headless["spec"]["ports"][0]["port"]
    url = executor_url(path)
    expected = f"dns://{sts['spec']['serviceName']}:{port}"
    assert url == expected, (
        f"{path}: sandbox_executor_url 是 {url!r}，期望一个名字 {expected!r}——"
        "写成逗号表就把副本数锁回渲染时的那一个数了"
    )


check_volumes(sys.argv[1], 1)
check_volumes(sys.argv[2], 3)
check_name_form(sys.argv[1])
check_name_form(sys.argv[2])
one_url = executor_url(sys.argv[1])
three_url = executor_url(sys.argv[2])
assert one_url == three_url, (
    f"副本数 1→3 时 sandbox_executor_url 变了（{one_url!r} → {three_url!r}）："
    "加副本又变成一次配置改动了"
)
print("SANDBOX REPLICA PROJECTION OK：副本数 1→3 时卷模板跟着变，地址是一个不随它变的名字")
PY
