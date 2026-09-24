#!/usr/bin/env bash
# git 身份的接线门禁：谁写 cogneva-secrets，谁就必须知道自己该登记的基线仓库。
#
# 为什么需要这条门禁（一次实测踩出来的）：git 身份自举的循环只跑在能读写
# cogneva-secrets 的进程里，而它自动登记部署密钥需要三样东西同时到位——
# Secret 读写、到平台的 egress、以及基线仓库名（COGNEVA_GATEWAY_GIT_IDENTITY_REPO）。
# 基线仓库名曾经只挂给了安全网关（那边的用途是传输选路的探测目标），跑循环的
# 主应用进程反而没有：于是自举每轮都只走"生成公钥挂到 WebUI 等人"那一半，
# 有 token 也不登记，状态永远停在 pending——配置面两个消费者，只接好了一个，
# 而**这类缺陷在渲染期完全没有症状**（清单合法、parity 也绿）。
#
# 判据面按「能推导的就不手写」取：
#   1. 从渲染产物反查 Secret 写权的持有者——凡有 Role 授予 secrets 的
#      patch/update/create、且有 RoleBinding 把它绑到某个 ServiceAccount，
#      那么**使用该 SA 的长时工作负载就是自举循环的宿主**，必须带基线仓库 env。
#      （手写清单会静默吃掉新出现的宿主：新加一个能写 Secret 的部署时，这里
#      会自动要求它带上，而不是无声放行。）
#   2. 全集群这份基线仓库只能有一个取值：自举登记的仓库与选路探测的仓库一旦
#      不同，SSH 自证永远握手不过，而症状只是"停在 pending"。
#   3. 两侧都必须非空：空值等于关掉自动登记，属于要显式说明的部署决定。
#
# 用法：bash deploy/scripts/check-git-identity-wiring.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - deploy/rendered <<'PYEOF'
import glob, os, sys, yaml

IDENTITY_REPO_ENV = "COGNEVA_GATEWAY_GIT_IDENTITY_REPO"
WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet")

profiles = sorted(d for d in os.listdir(sys.argv[1])
                  if os.path.isdir(os.path.join(sys.argv[1], d)))
if not profiles:
    sys.exit(f"没有可校验的渲染产物：{sys.argv[1]}")

def docs(profile):
    for path in sorted(glob.glob(os.path.join(sys.argv[1], profile, "*.yaml"))):
        with open(path) as fh:
            for doc in yaml.safe_load_all(fh):
                if isinstance(doc, dict) and doc.get("kind"):
                    yield path, doc

def pod_template(doc):
    spec = doc["spec"]
    if doc["kind"] == "CronJob":
        return spec["jobTemplate"]["spec"]["template"]
    return spec["template"]

def containers(tpl):
    return (tpl["spec"].get("containers") or []) + (tpl["spec"].get("initContainers") or [])

def env_of(tpl):
    found = {}
    for c in containers(tpl):
        for e in c.get("env") or []:
            if e.get("name") == IDENTITY_REPO_ENV:
                found[c["name"]] = (e.get("value"), bool(e.get("valueFrom")))
    return found

failures = []
seen_values = {}
for profile in profiles:
    all_docs = list(docs(profile))

    # 1) Secret 写权的持有者：有写 verbs 的 Role/ClusterRole → RoleBinding → SA
    write_roles = set()
    for _, d in all_docs:
        if d["kind"] not in ("Role", "ClusterRole"):
            continue
        for rule in d.get("rules") or []:
            verbs = set(rule.get("verbs") or [])
            if "secrets" in (rule.get("resources") or []) and (verbs & {"patch", "update", "create"}):
                # 名字相同的两类角色用 kind/name 一起标识，避免跨 kind 撞名。
                write_roles.add((d["kind"], d["metadata"]["name"]))
    writer_sas = set()
    for _, d in all_docs:
        if d["kind"] != "RoleBinding" or not write_roles & {("Role", d["roleRef"]["name"]),
                                                           ("ClusterRole", d["roleRef"]["name"])}:
            continue
        for s in d.get("subjects") or []:
            if s.get("kind") == "ServiceAccount":
                writer_sas.add(s["name"])
    if not writer_sas:
        failures.append(f"[{profile}] 没有找到任何持有 Secret 写权的 ServiceAccount："
                        "自举循环将无处落脚（判据失效，先核 RBAC 是否还在渲染集合里）")
        continue

    # 2) 这些 SA 用在哪、带没带基线仓库
    hosts = []
    for path, d in all_docs:
        if d["kind"] not in WORKLOAD_KINDS:
            continue
        tpl = pod_template(d)
        sa = tpl["spec"].get("serviceAccountName", "default")
        env = env_of(tpl)
        name = f"{d['kind']}/{d['metadata']['name']}"
        if sa in writer_sas:
            hosts.append(name)
            if not env:
                failures.append(
                    f"[{profile}] {name} 是 Secret 写权的宿主（SA {sa}）却没带 {IDENTITY_REPO_ENV}："
                    "git 身份自举在它这里只能走到'生成公钥挂出去等人'，自动登记那支永远不触发")
                continue
            for container, (value, from_secret) in env.items():
                if from_secret:
                    failures.append(
                        f"[{profile}] {name} 的 {IDENTITY_REPO_ENV} 取自 Secret：渲染期无法比对两侧取值，"
                        "而两侧必须同值（值本身不是凭证，应走 chart values）")
                elif not (value or "").strip():
                    failures.append(f"[{profile}] {name} 的 {IDENTITY_REPO_ENV} 为空：等于关掉自动登记")
                else:
                    seen_values.setdefault(value.strip(), []).append(f"{profile}:{name}")
        elif env:
            # 非宿主也带：只可能是选路探测那一侧（安全网关）。记下取值参与下面的同值判据。
            for container, (value, from_secret) in env.items():
                if not from_secret and (value or "").strip():
                    seen_values.setdefault(value.strip(), []).append(f"{profile}:{name}")
    if not hosts:
        failures.append(f"[{profile}] 持有 Secret 写权的 SA {sorted(writer_sas)} 没有任何长时工作负载在用")

# 3) 同值：登记侧与探测侧只能是同一个仓库
if len(seen_values) > 1:
    detail = "；".join(f"{v!r} ← {', '.join(where)}" for v, where in sorted(seen_values.items()))
    failures.append(f"{IDENTITY_REPO_ENV} 出现了多个取值：{detail}。"
                    "两侧不同值时 SSH 自证永远握手不过，症状只是停在 pending")

if failures:
    print("GIT IDENTITY WIRING FAIL：", file=sys.stderr)
    for f in failures:
        print(f"  - {f}", file=sys.stderr)
    sys.exit(1)

value = next(iter(seen_values))
where = ", ".join(sorted(seen_values[value]))
print(f"GIT IDENTITY WIRING OK：基线仓库 {value!r}，宿主与探测侧同值（{where}）")
PYEOF
