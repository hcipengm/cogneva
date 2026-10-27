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
# resource, 动词)，在**每一条**投递路径的读权限里都满足：
#   1. `deploy/rendered/<profile>/`（chart 渲染产物，helm 安装的那套集群读它）——profile
#      的集合从目录读，不在这里写名单：写死一份名单，新增的那一个 profile 的 RBAC 就
#      安静地不判，而「它缺权限」与「它本来就不在这条判据里」在判词上一样；
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
RENDERED = os.path.join(ROOT, "deploy/rendered")
RENDERED_TREES = sorted(
    name for name in os.listdir(RENDERED) if os.path.isdir(os.path.join(RENDERED, name))
) if os.path.isdir(RENDERED) else []
CARRIERS = [f"deploy/rendered/{name}" for name in RENDERED_TREES] + ["deploy/k3s"]
if not RENDERED_TREES:
    sys.exit(
        "读不到 deploy/rendered/ 下任何一个 profile 目录——判不了每条投递路径的读权限，"
        "这不等于全过"
    )

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
checked_workloads = []  # 判过「谁端着这一面」的工作负载，读在成功判词的末尾
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
    docs = []  # 这一条投递路径上的全部对象，第二轮判「谁跑这份镜像」
    for filename in sorted(os.listdir(dirpath)):
        if not filename.endswith((".yaml", ".yml")):
            continue
        with open(os.path.join(dirpath, filename), encoding="utf-8") as handle:
            docs.extend(list(yaml.safe_load_all(handle)))
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

    # 授了权限还不够：**谁**跑这份镜像，谁就是端这一面的进程。同一个二进制、同一套
    # 路由，换一个 Deployment 起就是另一个 ServiceAccount；绑定只点名了网关，进化
    # 进程就拿着默认身份去读它同样会答的那几面。2026-10-28 实测：探针上线第一轮就
    # 报出进化进程有 nodes、pods、events、persistentvolumeclaims 四条读是 403——而
    # 在此之前，两个进程的差别只藏在这一个绑定里。判据从清单本身读，不点名名单：
    # 容器用镜像的默认入口（没有 `command`）就是本应用。
    # 两遍：先把本应用的镜像认出来（网关那个容器用的就是它），再按它去找谁在跑。
    # 一遍走完的话，排在前面的 Deployment 判的时候手里还没有这个镜像。
    app_image = None
    for doc in docs:
        if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
            continue
        spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
        for container in spec.get("containers") or []:
            if container.get("name") == "cogneva":
                app_image = container.get("image")
    app_workloads = []  # (workload, serviceAccountName)
    for doc in docs:
        if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
            continue
        meta = doc.get("metadata") or {}
        spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
        sa = spec.get("serviceAccountName") or "default"
        for container in spec.get("containers") or []:
            if container.get("image") == app_image and not container.get("command"):
                app_workloads.append((meta.get("name"), sa))
    if app_image is None:
        problems.append(
            f"{rel}: 找不到网关那个容器（名为 cogneva）——判不出哪份镜像是本应用，"
            "「谁端着这一面」这一半等于没判"
        )
    elif not app_workloads:
        problems.append(
            f"{rel}: 一个跑应用默认入口的 Deployment 都没找到——这一半读的是空集，"
            "不是「人人都授了」"
        )
    else:
        bound = set()
        for doc in docs:
            if not isinstance(doc, dict) or doc.get("kind") != "ClusterRoleBinding":
                continue
            if (doc.get("metadata") or {}).get("name") != ROLE:
                continue
            for subject in doc.get("subjects") or []:
                bound.add(subject.get("name"))
        for workload, sa in app_workloads:
            if sa not in bound:
                problems.append(
                    f"{rel}: Deployment {workload} 跑的是应用默认入口（服务账号 {sa}），"
                    f"却不在 ClusterRoleBinding {ROLE} 的 subjects 里——它同样答拓扑、"
                    "工作负载、配额那几面，读不到就是空白"
                )
        checked_workloads.extend(f"{rel}:{workload}={sa}" for workload, sa in app_workloads)

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
    + " 在每一条投递路径上都授了（读了 %d 个：%s）；跑应用默认入口的工作负载也都在绑定的"
    " subjects 里：%s"
    % (len(CARRIERS), "、".join(CARRIERS), "、".join(checked_workloads))
)
PYEOF
