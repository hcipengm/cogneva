#!/usr/bin/env bash
# 凭证持有面门禁：谁碰得到 cogneva-secrets / cogneva-secrets-oob 里的哪个键，必须
# 与每个键自己的读者声明逐条对齐。
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
#   3. 两侧互判：树上的持有者 ⊆ 该键上方 `# readers:` 行声明的读者（多出来的点名），
#      声明的读者 ⊆ 树上的持有者（陈旧的声明点名）。**读者面只能声明、不能推导**：
#      机器推得出「谁在读」，推不出「谁该读」；那份判断写在键自己头上，跟着键走，
#      不另立一张表。`deploy/credential-holders.tsv` 2026-10-29 删掉正是因为它把
#      渲染产物已经算得出的 object/key/path/access 又手抄了一遍——49 行里 47 行
#      与机器读数逐字相同，抄错的那一行不会有任何一拍红。
#   4. 每个声明键都要有读者声明，且声明过的读者必须真在树上读它；运维手写的可选键
#      （安装器不建，`smtp-password` / `task-face-sources`）没有 `stringData` 行可挂
#      声明，门禁只保留「按名读一个没人建的键、且不是 optional 即红」这一条。
#   5. 三种部署形态的持有面逐条相同（含权限面）：读者声明不带形态维，而"只在某一个
#      形态里多挂了一个凭证"是最容易漏看的一种——用另一个形态的清单去看永远看不见。
#   6. 带外片的 role 写权限只能落在安全网关那个主体上：多一个能 patch 的人，等于
#      把「代持带外凭证」复制一份，而清单上只多了一个对象名。
#   7. 迁移窗必须写 `closes-when=<文件>:<符号>`，且那些读点全部搬走（符号都不在树
#      上了）就红：「迁移窗」三个字是状态描述，不是判据，它不会自己到期。
#
# 本门**核不到**的一条路，写在声明里而不是假装核过：进程内经 Kubernetes API 读
# 某个键。渲染产物里看不到它，所以那类读者写成 `# readers: Deployment/x (api: <文件>)`，
# 门禁只核两件事：那个文件还在树上，且文件里出现这个键名（指针防锈）。它是否真在
# 读那个键，要另一条判据。
#
# 用法：bash deploy/scripts/check-credential-holder-surface.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - <<'PYEOF'
import glob, os, re, sys, yaml

RENDERED = "deploy/rendered"
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


READERS_RE = re.compile(r"^\s*#\s*readers:\s*(.*)$")
API_RE = re.compile(r"^(?P<holder>\w+/\S+)\s*\(api:\s*(?P<file>[^)]+)\)$")


def parse_template(path):
    """stringData 的键、键所在行、以及键正上方那条 `# readers:` 声明。

    空键集合是判据失效，不是"没问题"。键之间夹着 helm 指令（`{{- if $x }}` /
    `{{- end }}`，写在行首），它们不是块结束；按"行首不是空格"停会在这里静默截断
    键表，而截断的后果是后面那些键被当成"写者没声明"。
    """
    keys, inside, pending = {}, False, None
    with open(path) as fh:
        for lineno, line in enumerate(fh, 1):
            body = line.rstrip("\n")
            stripped = body.strip()
            if not inside:
                if stripped == "stringData:":
                    inside = True
                continue
            if not stripped or stripped.startswith("{{"):
                continue
            if not body.startswith(" "):         # 顶层键 = 块结束
                break
            if stripped.startswith("#"):
                m = READERS_RE.match(body)
                if m:
                    pending = (lineno, m.group(1).strip())
                continue
            if body.startswith("   "):            # 更深缩进属于上一个键的值
                continue
            name = stripped.split(":", 1)[0].strip()
            if name and all(ch.isalnum() or ch in "-._" for ch in name):
                keys[name] = (lineno, pending)
                pending = None
    return keys, pending


# (obj, key) -> [(holder, api_file 或 None)]；读者面是声明出来的那一份。
declared = {}
for obj, tpl in TEMPLATES.items():
    keys, leftover = parse_template(tpl)
    if not keys:
        failures.append(f"{tpl} 里没解析出任何 stringData 键：判据失效"
                        "（空集合会让「每个声明的键都有读者」恒真）")
    if leftover is not None:
        failures.append(f"{tpl}:{leftover[0]} 的 `# readers:` 没有挂到任何键上："
                        "它要么被挪到了别的键头上，要么那个键已被删掉")
    for key, (lineno, pending) in keys.items():
        where = f"{tpl}:{lineno} 的 {key}"
        if pending is None:
            failures.append(f"{where} 上方没有 `# readers:` 行：这个键谁读、凭什么读，"
                            "是声明出来的判断，写不出来就该把这个键删掉")
            continue
        raw = pending[1]
        note = ""
        if " — " in raw:
            raw, note = raw.split(" — ", 1)
        if "迁移窗" in raw + note:
            if not re.search(r"closes-when=[^\s:]+:\S+", note):
                failures.append(f"{where} 的读者声明是迁移窗，却没有 "
                                "closes-when=<文件>:<符号>：没有关闭条件的窗口等于永久授权")
        entries, line_fail = [], []
        for part in [p.strip() for p in raw.split(",") if p.strip()]:
            m = API_RE.match(part)
            if m:
                entries.append((m.group("holder").strip(), m.group("file").strip()))
            elif re.match(r"^\w+/\S+$", part):
                entries.append((part, None))
            else:
                line_fail.append(f"{where} 的读者 {part!r} 不是 `<kind>/<名字>`"
                                 "（API 路径写成 `<kind>/<名字> (api: <文件>)`）")
        if line_fail:
            failures.extend(line_fail)
            continue
        if not entries:
            failures.append(f"{where} 的 `# readers:` 是空的：没有读者就删掉这个键，"
                            "别用空声明把它留在那里")
            continue
        declared[(obj, key)] = entries

declared_keys = {obj: {k for (o, k) in declared if o == obj} for obj in TEMPLATES}


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
                    if key not in declared_keys[ref["name"]]:
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
                        if it["key"] not in declared_keys[sec["secretName"]]:
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

# 1) 树上的持有者 ⊆ 声明的读者。role 授予的是整个对象（key 记 `*`），它不按键声明，
#    由下面第 6 条单独约束。
undeclared_optional = {}   # (obj, key) -> [holder]，运维手写键的读者面
for obs in sorted(observed):
    obj, key, path, holder = obs
    if key == "*":
        continue
    if key not in declared_keys[obj]:
        if (obj, key) in optional_undeclared:
            undeclared_optional.setdefault((obj, key), []).append(holder)
        continue
    if holder not in {h for h, api in declared[(obj, key)] if api is None}:
        failures.append(
            f"渲染产物里 {holder} 持有 {obj}/{key}（路径 {path}，{observed[obs]}），"
            f"而 {TEMPLATES[obj]} 里这个键的 `# readers:` 没写它：新出现一个握凭证的"
            "读者，或者读法变了")

# 2) 声明的读者 ⊆ 树上的持有者（API 路径核不到，单独核指针）
for (obj, key), entries in sorted(declared.items()):
    for holder, api_file in entries:
        if api_file is not None:
            if not os.path.exists(api_file):
                failures.append(
                    f"{obj}/{key} 的 API 读点 {api_file!r} 在树上不存在：指针锈了")
                continue
            text = open(api_file, encoding="utf-8", errors="replace").read()
            if key not in text:
                failures.append(
                    f"{obj}/{key} 的 API 读点 {api_file!r} 里没有出现这个键名："
                    "指针指向的文件已经不再读它了")
            continue
        if not any(o == obj and k == key and h == holder and p in ("env", "volume")
                   for (o, k, p, h) in observed):
            failures.append(
                f"{TEMPLATES[obj]} 声明 {holder} 读 {obj}/{key}，渲染产物里找不到这个"
                "读者：声明陈旧了，删掉它或改回真实读者")

# 3) 整对象访问（Role 授权、整卷挂载、envFrom 整片）只能落在安全网关这一个主体上：
#    带外片里每一把都是能对外发号的身份，多一个能 patch 的人等于把「代持」复制一份；
#    而清单上看起来只是多了一个对象名。内部片同理——整片挂上就不必再逐键判断。
GW_SA = "ServiceAccount/cogneva-security-gateway"
for (obj, key, path, holder), access in sorted(observed.items()):
    if key != "*" or holder == GW_SA:
        continue
    failures.append(
        f"{holder} 对 {obj} 有整对象访问（路径 {path}，{access}）：整对象授权只该给 "
        f"{GW_SA}，其余主体的读写应当经 secret_write::forward 交给网关")

if failures:
    print("CREDENTIAL HOLDER SURFACE FAIL：", file=sys.stderr)
    for f in failures:
        print(f"  - {f}", file=sys.stderr)
    sys.exit(1)

per_obj = {obj: sum(1 for (o, k, p, h) in observed if o == obj) for obj in OBJS}
api_rows = sum(len([1 for _, f in e if f]) for e in declared.values())
print(
    f"CREDENTIAL HOLDER SURFACE OK：{len(observed)} 个持有者与键头上的 `# readers:` "
    f"逐条对齐（{OBJS[0]} {per_obj[OBJS[0]]} / {OBJS[1]} {per_obj[OBJS[1]]}）；"
    f"写者声明键 {sum(len(k) for k in declared_keys.values())} 个，读者面两侧都对上；"
    f"{len(profiles)} 种形态的持有面逐条相同。"
    f"其中 {api_rows} 条走 API 路径，本门只核代码指针"
)
if undeclared_optional:
    print(
        "运维手写键（写者不建，读者面不受本门约束）："
        + "；".join(f"{o}/{k} ← {'、'.join(sorted(set(h)))}"
                   for (o, k), h in sorted(undeclared_optional.items()))
    )
PYEOF
