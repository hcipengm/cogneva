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
#   4. 归属声明恰好落在一个工作负载上（`COGNEVA_CREDENTIAL_LOOP_OWNER`）。判据若
#      只有"碰不碰得到那两个 Secret 对象"这一半，在两个身份都持授权的迁移窗口里
#      两边都会答"是"：同一对密钥被登记两遍，续期那边的 refresh token 被兑换两次
#      （它是一次性的，第二次会把第一次换出来的那对作废）。能力面分不出该谁做，
#      部署面的这一行才分得出。
#   5. 声明为属主的那个工作负载必须真的持有 Secret 写权：声明说真而授权没跟上时，
#      循环每轮对着 403 空转，而清单本身完全合法——这类缺陷在渲染期没有症状。
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
LOOP_OWNER_ENV = "COGNEVA_CREDENTIAL_LOOP_OWNER"
# 判据面与 Rust 侧逐字对应：那边只认这四个字面量（见
# contribution_admin::declared_credential_loop_owner），别处都读成 false。清单里写
# 了别的字面量就等于写了 false 而读起来像 true，所以这里单独拦。
OWNER_TRUE = {"1", "true", "True", "TRUE"}
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

def env_of(tpl, var=IDENTITY_REPO_ENV):
    found = {}
    for c in containers(tpl):
        for e in c.get("env") or []:
            if e.get("name") == var:
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

    # 2) 这些 SA 用在哪、带没带基线仓库；以及谁声明了自己是凭证循环的宿主
    hosts = []
    owner_hosts = []
    for path, d in all_docs:
        if d["kind"] not in WORKLOAD_KINDS:
            continue
        tpl = pod_template(d)
        sa = tpl["spec"].get("serviceAccountName", "default")
        env = env_of(tpl)
        owner_env = env_of(tpl, LOOP_OWNER_ENV)
        name = f"{d['kind']}/{d['metadata']['name']}"
        if owner_env:
            owner_hosts.append((name, sa, owner_env))
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

    # 3) 归属声明：取值本身合法、恰好一处置真、置真的那个真的写得动 Secret
    for name, sa, decl in owner_hosts:
        for container, (value, from_secret) in decl.items():
            if from_secret:
                failures.append(
                    f"[{profile}] {name}/{container} 的 {LOOP_OWNER_ENV} 取自 Secret：渲染期"
                    "无法比对取值，而这个变量的取值就是归属本身")
            elif not (value or "").strip():
                failures.append(
                    f"[{profile}] {name}/{container} 的 {LOOP_OWNER_ENV} 是空串：Rust 侧读成"
                    " false，而清单读起来像已经置真")
            elif (value or "").strip() not in OWNER_TRUE:
                failures.append(
                    f"[{profile}] {name}/{container} 的 {LOOP_OWNER_ENV}={value!r} 不是 Rust 侧"
                    f"认的真值（{'/'.join(sorted(OWNER_TRUE))}）：它会被读成 false")
    owners = sorted(
        (name, sa) for name, sa, decl in owner_hosts
        if any(not fs and (v or "").strip() in OWNER_TRUE for v, fs in decl.values())
    )
    if len(owners) != 1:
        detail = ", ".join(f"{n}(SA {sa})" for n, sa in owners) or "无"
        failures.append(
            f"[{profile}] {LOOP_OWNER_ENV} 置真的长时工作负载有 {len(owners)} 个（{detail}）："
            "这一行声明的全部内容就是「恰好一个」。两个属主会把同一对 git 密钥登记两遍，"
            "并把一次性的 refresh token 兑成互相作废的两对")
    elif owners[0][1] not in writer_sas:
        failures.append(
            f"[{profile}] {owners[0][0]} 声明自己是凭证循环的宿主，但它的 SA "
            f"{owners[0][1]!r} 不在持有 Secret 写权的 SA 集合 {sorted(writer_sas)} 里：循环会"
            "每轮对着 403 空转，而清单本身完全合法")

# 4) 同值：登记侧与探测侧只能是同一个仓库
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
