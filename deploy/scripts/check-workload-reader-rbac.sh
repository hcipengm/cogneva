#!/usr/bin/env bash
# 部署面读权限门禁：接口问了哪几条路径，RBAC 就得授哪几条。
#
# 这条门来自两次实测（2026-10-27）：
#
# 1. `/api/v1/cluster/deployments` 改成读五种工作负载之后，镜像发了、屏也换了，端点
#    自己的回答是
#
#      读不到 StatefulSet：apiserver 返回 403 Forbidden；读不到 DaemonSet：…；
#      读不到 CronJob：…；读不到 Job：…
#
#    而工作负载那一栏仍然是一张看起来完整的表——11 行 Deployment。postgres、redis、
#    三个 DaemonSet、两个 CronJob 都不在，空白读起来像「本来就没有」。代码、测试、CI
#    全绿：缺的是那条只授了 `deployments` 的 ClusterRole。
#
# 2. 拓扑面把事件与卷声明改成集群范围的集合读（`/api/v1/events`、
#    `/api/v1/persistentvolumeclaims`）之后，屏幕上是
#
#      读不到 Pod：apiserver 返回 403 Forbidden
#
#    而 `pods` 明明授过。真正缺的是那两条集合读：命名空间里的 Role 授不了不带
#    `namespaces/<ns>` 的路径，规则写得再对也一条都不生效。这就是第一版的这条门漏掉的
#    形状——它只认 `/…/namespaces/{namespace}/…`，而集群范围的读根本不长这样。
#
# 判据：`crates/cog-gateway/src/cluster.rs` 里每一条 apiserver 路径的 (作用域, group,
# resource, 动词)，在两个投递路径的读权限里都满足：
#   1. `deploy/rendered/k3s-single/`（chart 渲染产物，helm 安装的那套集群读它）；
#   2. `deploy/k3s/`（静态清单，集群内 GitOps 拉取端 apply 它）。
#
# 作用域决定谁能满足它：
#   - 命名空间里的集合读（`…/namespaces/{ns}/<resource>`）：ClusterRole 与命名空间
#     Role 任一即可，动词要有 `list`；
#   - 集群范围的读（`/api/v1/<resource>`、`/apis/<group>/<version>/<resource>`）：**只有
#     ClusterRole 算数**——命名空间的 Role 在这一层不参与授权，写了也是假的；
#   - 按名读与子资源读（`…/<resource>/<name>`、`nodes/<n>/proxy/stats/*`）：动词要有 `get`。
#
# 不认识的路径形状直接报错，不跳过：这条门一旦对某个形状沉默，那个形状下一次缺权限
# 时它就什么都不说，而这一面缺权限的样子**没有报错，只有空白**。
#
# 用法：bash deploy/scripts/check-workload-reader-rbac.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

try:
    import yaml
except ImportError:
    sys.exit("缺少依赖：PyYAML（python3 -c 'import yaml' 要能过）")

ROOT = sys.argv[1]
SERVER = os.path.join(ROOT, "crates/cog-gateway/src/cluster.rs")
ROLE = "cogneva-topology-reader"
# 命名空间里的那几个 Role 都可能替拓扑面授权（拉取端与它自己的那条）。
NAMESPACED_ROLES = {"cogneva-topology-reader", "cogneva-gitops-puller"}
CARRIERS = [
    "deploy/rendered/k3s-single",
    "deploy/k3s",
]

if not os.path.isfile(SERVER):
    sys.exit(f"读不到 {os.path.relpath(SERVER, ROOT)}——这一面要问哪几条路径无从知道")
server = open(SERVER, encoding="utf-8").read()


def classify(path):
    """(作用域, group, resource, 动词)；不认识的形状返回 None。"""
    path = path.split("?")[0]
    # 命名空间里的集合读。
    m = re.match(r"^/apis/([a-z0-9.\-]+)/v[0-9a-z]+/namespaces/[^/]+/([a-z]+)$", path)
    if m:
        return ("namespaced", m.group(1), m.group(2), "list")
    m = re.match(r"^/api/v1/namespaces/[^/]+/([a-z]+)$", path)
    if m:
        return ("namespaced", "", m.group(1), "list")
    # 按名读。
    m = re.match(r"^/apis/([a-z0-9.\-]+)/v[0-9a-z]+/namespaces/[^/]+/([a-z]+)/[^/]+$", path)
    if m:
        return ("namespaced", m.group(1), m.group(2), "get")
    m = re.match(r"^/api/v1/namespaces/[^/]+/([a-z]+)/[^/]+$", path)
    if m:
        return ("namespaced", "", m.group(1), "get")
    # kubelet summary 经 apiserver 代理：apiserver 把 `/stats/*` 映射到节点子资源。
    if re.match(r"^/api/v1/nodes/[^/]+/proxy/stats/", path):
        return ("cluster", "", "nodes/stats", "get")
    # 集群范围的集合读。
    m = re.match(r"^/apis/([a-z0-9.\-]+)/v[0-9a-z]+/([a-z]+)$", path)
    if m:
        return ("cluster", m.group(1), m.group(2), "list")
    m = re.match(r"^/api/v1/([a-z]+)$", path)
    if m:
        return ("cluster", "", m.group(1), "list")
    return None


wanted = {}
unknown = []
for path in re.findall(r'"(/api[^"\s?]*)"', server):
    parsed = classify(path)
    if parsed is None:
        unknown.append(path)
        continue
    scope, group, resource, verb = parsed
    wanted.setdefault((scope, group, resource), set()).add(verb)

if not wanted:
    sys.exit(
        f"{os.path.relpath(SERVER, ROOT)}: 一条 apiserver 路径都没读到——"
        "正则读错了形状，不是这一面真的不读集群"
    )

problems = []
for path in sorted(set(unknown)):
    problems.append(
        f"{os.path.relpath(SERVER, ROOT)}: 这条门不认识的路径形状 {path}"
        "——新形状要在这里登记，否则它缺权限时这条门什么都不说"
    )

for rel in CARRIERS:
    dirpath = os.path.join(ROOT, rel)
    if not os.path.isdir(dirpath):
        problems.append(f"读不到 {rel}——这条投递路径的读权限无从检查")
        continue
    cluster_rules = {}  # (group, resource) -> verbs，只来自 ClusterRole
    role_rules = {}  # (group, resource) -> verbs，只来自命名空间 Role
    for filename in sorted(os.listdir(dirpath)):
        if not filename.endswith((".yaml", ".yml")):
            continue
        with open(os.path.join(dirpath, filename), encoding="utf-8") as handle:
            docs = list(yaml.safe_load_all(handle))
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            kind = doc.get("kind")
            name = (doc.get("metadata") or {}).get("name")
            if kind == "ClusterRole" and name == ROLE:
                bucket = cluster_rules
            elif kind == "Role" and name in NAMESPACED_ROLES:
                bucket = role_rules
            else:
                continue
            for rule in doc.get("rules") or []:
                verbs = rule.get("verbs") or []
                for group in rule.get("apiGroups") or []:
                    for resource in rule.get("resources") or []:
                        bucket.setdefault((group, resource), set()).update(verbs)
    if not cluster_rules:
        problems.append(
            f"{rel}: 没有 ClusterRole {ROLE}——这条投递路径上这份读权限不存在"
        )
        continue
    for (scope, group, resource), verbs_wanted in sorted(wanted.items()):
        # 集群范围的读只有 ClusterRole 能满足；命名空间 Role 在这一层不参与授权。
        granted = cluster_rules
        if scope == "namespaced":
            granted = {
                key: cluster_rules.get(key, set()) | role_rules.get(key, set())
                for key in set(cluster_rules) | set(role_rules)
            }
        verbs = granted.get((group, resource), set())
        missing = sorted(verbs_wanted - verbs)
        if missing:
            where = f"ClusterRole {ROLE}" if scope == "cluster" else f"{ROLE} 与命名空间 Role"
            problems.append(
                f"{rel}: {where} 对 {group or 'core'}/{resource} 缺 {missing}"
                + (f"（只有 {sorted(verbs)}）" if verbs else "")
                + ("——集群范围的读只有 ClusterRole 算数" if scope == "cluster" else "")
            )

if problems:
    sys.exit(
        "读权限没跟上面向集群的接口：\n  "
        + "\n  ".join(problems)
        + "\n读不到权限时这一面不报错：屏幕上只会少几行，而空白读起来像「本来就没有」。"
    )

print(
    "ok："
    + "、".join(
        f"{'集群' if scope == 'cluster' else '命名空间'}:{group or 'core'}/{resource}"
        f"({','.join(sorted(verbs))})"
        for (scope, group, resource), verbs in sorted(wanted.items())
    )
    + " 在两个投递路径上都授了"
)
PYEOF
