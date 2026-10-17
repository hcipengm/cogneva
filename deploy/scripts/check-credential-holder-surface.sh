#!/usr/bin/env bash
# 凭证持有面门禁：谁碰得到 cogneva-secrets / cogneva-secrets-oob 里的哪个键，必须
# 与登记表逐条对齐。
#
# 为什么需要这条门禁：这两个对象的读者面就是"零带外凭证"这条约束的实际边界，而
# 它是**纯粹声明出来**的——多一个工作负载挂上带外那一片，渲染期没有任何症状：清单
# 合法、parity 绿、Pod 照常起来，只是那个 Pod 从此握着平台 token 与 LLM 上游密钥。
# 反过来，写者少声明一个键、而读者按名去读它，症状是 Pod 卡在 ContainerCreating
# （非 optional 的 secretKeyRef 缺键即起不来），或者更安静——读者拿到空值继续跑。
#
# 判据面「能推导的就不手写」：
#   1. 写者声明的键集合从 chart 的两份 Secret 模板解析（`stringData` 的键），不手抄；
#      解析出空集合直接判判据失效——空集合会让"每个声明键都有登记"这条恒真。
#   2. 持有者从渲染产物反查，三条路径：
#        env     键级注入（哪个工作负载读哪个键）；
#        volume  卷 items 键级，或整卷挂载（key 记 `*`）；
#        role    Role/ClusterRole 里的 secrets 规则 → RoleBinding 解析到
#                ServiceAccount。**动词含 patch/update/create/delete 记 write**：
#                能改就能把值换成自己写的，等价于持有；只有 get/list/watch 记 read。
#   3. 两侧互判：树上的持有者 ⊆ 登记表（多出来的点名），登记表 ⊆ 树上的持有者
#      （陈旧的登记点名）。
#   4. 登记表里的键必须是写者声明过的键（`*` 除外）：运维手写的可选键得同时满足
#      「树上有读者」且「读它是 optional」——否则读者按名读一个没人建的键就起不来。
#   5. 三种部署形态的持有面逐条相同（含权限面）：登记表不带形态维，而"只在某一个
#      形态里多挂了一个凭证"是最容易漏看的一种——用另一个形态的清单去看永远看不见。
#
# 本门**核不到**的一条路，写在登记表里而不是假装核过：进程内经 Kubernetes API 读
# 某个键（`path = api` 的行）。渲染产物里看不到它，evidence 列给出读它的代码文件，
# 门禁只核那个文件还在树上（指针防锈）。它是否真读那个键，要另一条判据。
#
# 用法：bash deploy/scripts/check-credential-holder-surface.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - <<'PYEOF'
import glob, os, sys, yaml

RENDERED = "deploy/rendered"
REGISTRY = "deploy/credential-holders.tsv"
TEMPLATES = {
    "cogneva-secrets": "deploy/helm/cogneva/templates/secret.yaml",
    "cogneva-secrets-oob": "deploy/helm/cogneva/templates/secret-oob.yaml",
}
OBJS = tuple(TEMPLATES)
PATHS = ("env", "volume", "role", "api", "none")
ACCESS = ("read", "write", "read,write", "none")
READ_VERBS = {"get", "list", "watch"}
WRITE_VERBS = {"patch", "update", "create", "delete"}

failures = []


def declared_keys(path):
    """chart 模板里 stringData 块声明的键。空集合是判据失效，不是"没问题"。"""
    keys, inside = [], False
    with open(path) as fh:
        for line in fh:
            body = line.rstrip("\n")
            stripped = body.strip()
            if not inside:
                if stripped == "stringData:":
                    inside = True
                continue
            # 键之间夹着 helm 指令（`{{- if $x }}` / `{{- end }}`，写在行首），
            # 它们不是块结束；按"行首不是空格"停会在这里静默截断键表，
            # 而截断的后果是后面那些键被当成"写者没声明"。
            if not stripped or stripped.startswith("#") or stripped.startswith("{{"):
                continue
            if not body.startswith(" "):         # 顶层键 = 块结束
                break
            if body.startswith("   "):            # 更深缩进属于上一个键的值
                continue
            name = stripped.split(":", 1)[0].strip()
            if name and all(ch.isalnum() or ch in "-._" for ch in name):
                keys.append(name)
    return set(keys)


declared = {obj: declared_keys(tpl) for obj, tpl in TEMPLATES.items()}
for obj, keys in declared.items():
    if not keys:
        failures.append(f"{TEMPLATES[obj]} 里没解析出任何 stringData 键：判据失效"
                        "（空集合会让「每个声明的键都有登记」恒真）")


def docs(profile):
    for path in sorted(glob.glob(os.path.join(RENDERED, profile, "*.yaml"))):
        with open(path) as fh:
            for doc in yaml.safe_load_all(fh):
                if isinstance(doc, dict) and doc.get("kind"):
                    yield doc


def pod_templates(doc):
    kind = doc["kind"]
    if kind in ("Deployment", "StatefulSet", "DaemonSet", "Job"):
        yield doc["spec"]["template"]
    elif kind == "CronJob":
        yield doc["spec"]["jobTemplate"]["spec"]["template"]
    elif kind == "Pod":
        yield doc["spec"]


def containers(tpl):
    spec = tpl.get("spec") or {}
    return (spec.get("containers") or []) + (spec.get("initContainers") or [])


profiles = sorted(d for d in os.listdir(RENDERED)
                  if os.path.isdir(os.path.join(RENDERED, d)))
if not profiles:
    sys.exit(f"没有可校验的渲染产物：{RENDERED}")

optional_undeclared = set()   # (obj, key) 树上有读者、写者没声明、且读它是 optional


def surface(profile):
    """一种部署形态下的全部持有者：(obj, key, path, holder) -> access。"""
    obs = {}
    binds = {}
    role_access = {}
    for doc in docs(profile):
        name = f"{doc['kind']}/{doc['metadata']['name']}"
        for tpl in pod_templates(doc):
            for c in containers(tpl):
                for e in c.get("env") or []:
                    ref = (e.get("valueFrom") or {}).get("secretKeyRef")
                    if not ref or ref.get("name") not in OBJS:
                        continue
                    key = ref.get("key")
                    obs[(ref["name"], key, "env", name)] = "read"
                    if key not in declared[ref["name"]]:
                        if ref.get("optional"):
                            optional_undeclared.add((ref["name"], key))
                        else:
                            failures.append(
                                f"[{profile}] {name}/{c.get('name')} 按名读 "
                                f"{ref['name']}/{key}，而写者的键表里没有这个键，读它也不是 "
                                "optional：安装器建的 Secret 里不会有它，这个 Pod 起不来")
            for v in tpl["spec"].get("volumes") or []:
                sec = v.get("secret")
                if not sec or sec.get("secretName") not in OBJS:
                    continue
                items = sec.get("items") or []
                if items:
                    for it in items:
                        obs[(sec["secretName"], it["key"], "volume", name)] = "read"
                        if it["key"] not in declared[sec["secretName"]]:
                            if sec.get("optional"):
                                optional_undeclared.add((sec["secretName"], it["key"]))
                            else:
                                failures.append(
                                    f"[{profile}] {name} 的卷 {v.get('name')} 取未声明键 "
                                    f"{sec['secretName']}/{it['key']}，且整卷不是 optional："
                                    "键不存在时 Pod 卡在 ContainerCreating")
                else:
                    obs[(sec["secretName"], "*", "volume", name)] = "read"
        if doc["kind"] in ("Role", "ClusterRole"):
            for rule in doc.get("rules") or []:
                if "secrets" not in (rule.get("resources") or []):
                    continue
                verbs = set(rule.get("verbs") or [])
                if not (verbs & (READ_VERBS | WRITE_VERBS)):
                    continue
                acc = set()
                if verbs & READ_VERBS:
                    acc.add("read")
                if verbs & WRITE_VERBS:
                    acc.add("write")
                if acc:
                    role_access[(doc["kind"], doc["metadata"]["name"])] = ",".join(sorted(acc))
        if doc["kind"] == "RoleBinding":
            binds[(doc["roleRef"]["kind"], doc["roleRef"]["name"])] = [
                s["name"] for s in doc.get("subjects") or []
                if s.get("kind") == "ServiceAccount"
            ]
    for role, acc in role_access.items():
        sas = binds.get(role)
        if not sas:
            failures.append(
                f"[{profile}] {role[0]}/{role[1]} 授了 Secret 的 {acc}，却没有 RoleBinding 绑到"
                "任何 ServiceAccount：这是一条谁也用不到的授权，只让人以为有人持有着")
            continue
        for sa in sas:
            for obj in OBJS:
                obs[(obj, "*", "role", f"ServiceAccount/{sa}")] = acc
    return obs


surfaces = {profile: surface(profile) for profile in profiles}
base = profiles[0]
# 三种形态的持有面必须逐条相同：登记表不带形态维，而"哪一份清单里多挂了一个
# 凭证"恰恰是最容易漏看的一种——只在 k8s-standard 里多挂的那一个，用 k3s 形态
# 看清单永远看不见。
for profile in profiles[1:]:
    for t in sorted(set(surfaces[base]) - set(surfaces[profile])):
        failures.append(
            f"[{profile}] 比 [{base}] 少一个持有者：{t[3]} 持有 {t[0]}/{t[1]}（{t[2]}）。"
            "持有面按形态各不相同，就没人能从一份清单读出边界")
    for t in sorted(set(surfaces[profile]) - set(surfaces[base])):
        failures.append(
            f"[{profile}] 比 [{base}] 多一个持有者：{t[3]} 持有 {t[0]}/{t[1]}（{t[2]}）")
    for t in sorted(set(surfaces[profile]) & set(surfaces[base])):
        if surfaces[profile][t] != surfaces[base][t]:
            failures.append(
                f"[{profile}] 的 {t[3]} 对 {t[0]}/{t[1]} 是 {surfaces[profile][t]}，"
                f"[{base}] 是 {surfaces[base][t]}：权限面在形态之间不同")
observed = surfaces[base]

rows = []
seen = set()
with open(REGISTRY) as fh:
    for lineno, line in enumerate(fh, 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        fields = (line.rstrip("\n").split("\t") + [""] * 7)[:7]
        obj, key, path, holder, access, evidence, note = fields
        where = f"{REGISTRY}:{lineno}"
        if not obj or not key or not path or not holder:
            failures.append(f"{where} 列不全（object/key/path/holder 都必填）：{line.rstrip()!r}")
            continue
        if obj not in OBJS:
            failures.append(f"{where} 的对象 {obj!r} 不是那两个 Secret 对象之一")
        if path not in PATHS:
            failures.append(f"{where} 的路径 {path!r} 不在 {PATHS} 里")
        if access not in ACCESS:
            failures.append(f"{where} 的权限 {access!r} 不在 {ACCESS} 里")
        row = (obj, key, path, holder, access)
        if row in seen:
            failures.append(f"{where} 与上面某行逐字重复：{obj}/{key}/{path}/{holder}")
        seen.add(row)
        rows.append(row + (evidence, note))

# 1) 树上的持有者 ⊆ 登记表
reg_keys = {(r[0], r[1], r[2], r[3]) for r in rows}
for obs in sorted(observed):
    if obs not in reg_keys:
        failures.append(
            f"渲染产物里 {obs[3]} 持有 {obs[0]}/{obs[1]}（路径 {obs[2]}，{observed[obs]}），"
            "登记表里没有这一条：新出现一个握凭证的持有者，或者读法变了")

# 2) 登记表 ⊆ 树上的持有者（api / none 两路径核不到，不参与）
for obj, key, path, holder, access, evidence, _ in rows:
    if path in ("api", "none"):
        continue
    if (obj, key, path, holder) not in observed:
        failures.append(
            f"登记表里的 {holder} 持有 {obj}/{key}（{path}）在渲染产物里已不存在："
            "这条登记陈旧了，删掉它或改回真实持有者")
    elif observed[(obj, key, path, holder)] != access:
        failures.append(
            f"登记表说 {holder} 对 {obj}/{key} 是 {access}，清单里是 "
            f"{observed[(obj, key, path, holder)]}：权限面变了")

# 3) 写者声明的每个键都得有登记（真持有者或写明的"无人读"）
for obj, keys in declared.items():
    for key in sorted(keys):
        if not any(r[0] == obj and r[1] == key for r in rows):
            failures.append(
                f"{obj}/{key} 是写者声明的键，登记表里一条都没有：它要么真无人读"
                "（补一条 path=none、access=none、holder=- 的行说明），要么有个读者没登记")

# 4) 登记表里的键必须在写者的键表里，除非是"树上有 optional 读者"的运维手写键
for obj, key, path, holder, access, evidence, _ in rows:
    if key == "*" or key in declared.get(obj, ()):
        continue
    if (obj, key) not in optional_undeclared:
        failures.append(
            f"登记表里的 {obj}/{key} 不在写者声明的键表里，树上也读不到（或读它不是 "
            "optional）：键名漂移或手误")

# 5) api 路径的指针防锈
for obj, key, path, holder, access, evidence, _ in rows:
    if path != "api":
        continue
    if not evidence:
        failures.append(f"{obj}/{key}（api）没写 evidence：这条路核不到，只剩代码指针能指认")
    elif not os.path.exists(evidence):
        failures.append(f"{obj}/{key}（api）的 evidence {evidence!r} 在树上不存在：指针锈了")

if failures:
    print("CREDENTIAL HOLDER SURFACE FAIL：", file=sys.stderr)
    for f in failures:
        print(f"  - {f}", file=sys.stderr)
    sys.exit(1)

per_obj = {obj: sum(1 for r in rows if r[0] == obj) for obj in OBJS}
api_rows = sum(1 for r in rows if r[2] == "api")
print(
    f"CREDENTIAL HOLDER SURFACE OK：{len(rows)} 条登记与渲染产物对齐"
    f"（{OBJS[0]} {per_obj[OBJS[0]]} 条 / {OBJS[1]} {per_obj[OBJS[1]]} 条）；"
    f"写者声明键 {sum(len(k) for k in declared.values())} 个，无一漏登记；"
    f"{len(profiles)} 种形态的持有面逐条相同（{len(observed)} 个持有者）。"
    f"其中 {api_rows} 条走 API 路径，本门核不到（只核代码指针），见登记表"
)
PYEOF
