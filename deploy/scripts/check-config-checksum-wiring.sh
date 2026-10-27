#!/usr/bin/env bash
# 配置内容与 Pod 身份的接线门禁：`checksum/config`。
#
# 读 chart 配置的 Pod 只会在**启动时**读一次配置：卷里的文件由 kubelet 就地更新，
# env 更是只在容器创建时注入。只改配置（或只改 values 里那些进 ConfigMap 的键）时，
# 工作负载的 spec 可以一个字节都不变，滚动更新因此不会发生——新配置在旧 Pod 里
# 永远读不到，而从任何一侧看都是绿的：渲染成功、parity 对齐、Pod 全 Running。
# 2026-10-20 的演练里实测到的是反面：升级后配置已换、Pod 未换。
#
# 唯一的办法是把这个 Pod 模板绑到配置内容上：Pod 模板多一条
# `checksum/config: <配置的 sha256>`，配置一变注解就变，模板哈希随之变，滚动才发生。
#
# 这条注解**此前没有任何门禁看着**：chart 里删掉它，渲染照常成功；deploy/k3s/
# 静态清单同步删掉，parity 也照常绿（parity 比的是两侧是否一致，不是该不该有）。
# 于是它能被无声删掉，直到下一次只改配置的升级不出 Pod 才被发现。
#
# 覆盖面分两段，第二段补的是第一段看不见的那个坏法：
#
#   第一段（产物）：判据**读同一份配置的工作负载，必须带着这份配置的指纹**。
#     1. 凡 Pod 模板引用了 chart 渲染的任一 ConfigMap（envFrom 或卷）的工作负载，
#        必须带 `checksum/config`，值是 64 位小写十六进制的 sha256。
#     2. 同一份产物里，读配置的几条工作负载指纹必须相同——它们哈希的是同一组
#        配置文档；不等说明有人把某一条钉成了别的值（或改成了写死的字面量）。
#     3. 每种形态至少四条消费者（主应用、evolution、安全网关、沙箱执行器）。这是
#        **声明**不是推导：谁读这份配置是代码里的事，清单里读不出来。
#     4. 三种形态与静态清单之间，指纹不能全等：三份 values 渲染出的 config 不同，
#        全等意味着有人把 `sha256sum` 表达式换成了写死的字面量——那样配置变了注解
#        也不变，这道门禁本身就成了摆设。
#
#   第二段（chart 源码）：指纹**必须跟着 env 那份 ConfigMap 变**。第一段只看已渲染
#     产物里的 hex 值，看不出它哈希了哪几个文件：把表达式改回只哈希卷里那份
#     （`app-config.yaml`），上面四条全绿——直到运维在 values 里配好通知地址、
#     `helm upgrade` 成功、Pod 却一个都没换，出口永远挂不上。所以这一段真渲染两次：
#     一次原样，一次只加一个只影响 `cogneva-config` 的 `--set`，要求每个读配置的
#     Pod 模板指纹都变。判的是「注解与那份配置的依赖关系」，不是表达式长什么样。
#
# 用法：bash deploy/scripts/check-config-checksum-wiring.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - deploy <<'PYEOF'
import glob
import os
import re
import shutil
import subprocess
import sys

import yaml

DEPLOY = sys.argv[1]
# 哪些 ConfigMap 算「配置」不在这里列名字，从产物里读：同一份产物交付的 ConfigMap
# 当中，被 Pod 模板引用到的就是配置面。写死一份四个名字的表时，第五份配置
# ——searxng-settings 就是这样——的消费者不在判据里，删掉它的指纹也不会红。
ANNOTATION = "checksum/config"
HEX64 = re.compile(r"^[0-9a-f]{64}$")
WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet", "CronJob")
MIN_CONSUMERS = 4
MIN_DISTINCT_FINGERPRINTS = 2


def workload_docs(path):
    with open(path) as fh:
        for doc in yaml.safe_load_all(fh):
            if isinstance(doc, dict) and doc.get("kind") in WORKLOAD_KINDS:
                yield doc


def shipped_configmaps(paths):
    """→ 这些产物里交付的 ConfigMap 名字集合（配置面的底数）。"""
    shipped = set()
    for path in paths:
        with open(path) as fh:
            for doc in yaml.safe_load_all(fh):
                if isinstance(doc, dict) and doc.get("kind") == "ConfigMap":
                    name = (doc.get("metadata") or {}).get("name")
                    if name:
                        shipped.add(name)
    return shipped


def pod_template(doc):
    spec = doc.get("spec") or {}
    if doc["kind"] == "CronJob":
        spec = (spec.get("jobTemplate") or {}).get("spec") or {}
    template = spec.get("template") or {}
    return template.get("spec") or {}, (template.get("metadata") or {}).get("annotations") or {}


def consumed_config(podspec, shipped):
    """→ Pod 模板引用到的交付 ConfigMap 名字集合（envFrom 与卷都算）。"""
    found = set()
    for container in (podspec.get("containers") or []) + (podspec.get("initContainers") or []):
        for env_from in container.get("envFrom") or []:
            name = (env_from.get("configMapRef") or {}).get("name")
            if name in shipped:
                found.add(name)
    for volume in podspec.get("volumes") or []:
        name = (volume.get("configMap") or {}).get("name")
        if name in shipped:
            found.add(name)
    return found


def config_groups(consumers):
    """按「读到的配置有交集」把消费者并成一族 → {配置名集合: [消费者]}。"""
    parent = list(range(len(consumers)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i, (_, _, names) in enumerate(consumers):
        for j in range(i):
            if consumers[j][2] & names:
                parent[find(i)] = find(j)
    families = {}
    for i, member in enumerate(consumers):
        families.setdefault(find(i), []).append(member)
    return {frozenset().union(*[m[2] for m in members]): members
            for members in families.values()}


def scan(label, pattern):
    """→ (consumers, problems)；consumers 是 [(name, fingerprint, 读到的配置名)]。"""
    consumers = []
    problems = []
    files = sorted(glob.glob(pattern))
    shipped = shipped_configmaps(files)
    for path in files:
        for doc in workload_docs(path):
            podspec, annotations = pod_template(doc)
            names = consumed_config(podspec, shipped)
            if not names:
                continue
            name = doc["metadata"]["name"]
            value = annotations.get(ANNOTATION)
            if value is None:
                problems.append(
                    f"{label}: {name} 读 {'/'.join(sorted(names))} 却没有 {ANNOTATION}——"
                    "只改配置的那次升级不会换 Pod")
                continue
            value = str(value).strip()
            if not HEX64.match(value):
                problems.append(
                    f"{label}: {name} 的 {ANNOTATION} 不是 64 位小写十六进制（{value!r}）")
                continue
            consumers.append((name, value, names))
    return consumers, problems


def render(chart, profile, extra):
    cmd = [
        "helm", "template", "cogneva", chart,
        "--namespace", "cogneva",
        "-f", profile,
    ]
    # 额度不是 chart values，是按 profile 从渲染集投影出来的产物（见
    # project-resource-governance.py）。模板缺了它会直接报错——那是故意的，
    # 免得渲染出一条空额度的 ResourceQuota。这一段判的是注解跟随，不是额度，
    # 但渲染得过去才判得了。
    governed = os.path.join(
        chart,
        f"values.resource-governance.{os.path.splitext(os.path.basename(profile))[0]}.yaml",
    )
    if os.path.isfile(governed):
        cmd += ["-f", governed]
    cmd += extra
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"渲染失败（{' '.join(cmd)}）：\n{proc.stderr}")
    return [doc for doc in yaml.safe_load_all(proc.stdout) if isinstance(doc, dict)]


def config_contents(docs):
    """→ {(kind, 名字): 内容}，只收 ConfigMap；用来判断哪个键落在了哪一份里。"""
    return {
        (doc["kind"], doc["metadata"]["name"]): (doc.get("data"), doc.get("stringData"))
        for doc in docs
        if doc.get("kind") == "ConfigMap"
    }


# ---- 第二段：注解必须跟着它读的那份 ConfigMap 变 ----
chart = os.path.join(DEPLOY, "helm", "cogneva")
profile = os.path.join(chart, "profiles", "k3s-single.yaml")
if not os.path.isfile(os.path.join(chart, "Chart.yaml")):
    sys.exit(f"没有 chart：{chart}（这一段要真渲染，缺 chart 等于没判）")
if not os.path.isfile(profile):
    sys.exit(f"没有 profile：{profile}")
if shutil.which("helm") is None:
    sys.exit("缺少依赖：helm（这一段要真渲染两次，没有它这道判据等于没跑）")

# 这个键只出现在 env 那份配置里：变它就不会顺带改到卷里那几份。
PROBE_SET = "notification.dingtalkUrl=https://example.invalid/checksum-probe"
base = render(chart, profile, [])
probed = render(chart, profile, ["--set", PROBE_SET])

probe_problems = []
# 哪几份 ConfigMap 真的变了，由两次渲染比出来——不是在这里断言它落在
# cogneva-config 上。把名字写进判据时，一份新的配置只要不在那张表上，
# 它「变了却不滚 Pod」这件事就没人看着。
base_cms = config_contents(base)
probed_cms = config_contents(probed)
changed = {key[1] for key in set(base_cms) | set(probed_cms)
           if base_cms.get(key) != probed_cms.get(key)}
if not changed:
    probe_problems.append(
        f"--set {PROBE_SET} 没有改动任何 ConfigMap——这段判据的探针是瞎的")

base_by_key = {(d["kind"], d["metadata"]["name"]): d for d in base
               if d.get("kind") in WORKLOAD_KINDS}
probed_by_key = {(d["kind"], d["metadata"]["name"]): d for d in probed
                 if d.get("kind") in WORKLOAD_KINDS}
if set(base_by_key) != set(probed_by_key):
    probe_problems.append(
        "只为 env 配置加了一个 --set，工作负载集合就变了——这段判据的两次渲染不可比")
shipped_in_render = {name for _, name in base_cms}
for key, doc in base_by_key.items():
    podspec, annotations = pod_template(doc)
    names = consumed_config(podspec, shipped_in_render)
    if not names & changed:
        continue
    before = annotations.get(ANNOTATION)
    after = pod_template(probed_by_key[key])[1].get(ANNOTATION)
    if before is None:
        # 第一段会点名；这里不重复报。
        continue
    if str(before).strip() == str(after).strip():
        probe_problems.append(
            f"{key[0]}/{key[1]}：只改 {'/'.join(sorted(names & changed))} 的内容"
            f"（{PROBE_SET}）指纹没变（{str(before).strip()[:12]}…）——"
            "注解没哈希那份 ConfigMap，配置变了这个 Pod 不会滚动")

# ---- 第一段：已提交产物 ----
roots = []
for profile_name in sorted(os.listdir(f"{DEPLOY}/rendered")):
    if os.path.isdir(f"{DEPLOY}/rendered/{profile_name}"):
        roots.append((f"rendered/{profile_name}", f"{DEPLOY}/rendered/{profile_name}/*.yaml"))
roots.append(("k3s", f"{DEPLOY}/k3s/*.yaml"))

if not roots:
    sys.exit(f"没有可校验的产物：{DEPLOY}/rendered 与 {DEPLOY}/k3s")

problems = list(probe_problems)
distinct = set()
surveyed = 0
for label, pattern in roots:
    consumers, found = scan(label, pattern)
    problems.extend(found)
    surveyed += len(consumers)
    if len(consumers) < MIN_CONSUMERS and not any(p.endswith("却没有 " + ANNOTATION) for p in found):
        problems.append(
            f"{label}: 只找到 {len(consumers)} 条读 chart 配置的工作负载，"
            f"下界是 {MIN_CONSUMERS}（主应用、evolution、安全网关、沙箱执行器）")
    # 指纹只在「读同一组配置」的消费者之间必须相同：不同组本来就哈希不同的文档
    # （只读 searxng-settings 的那条与读主配置那几条不是同一份），把它们硬拉到
    # 一个值上，判的就成了「所有 Pod 共用一份配置」。同组里有人把值钉成别的
    # 字面量，这里仍然红。
    for group, members in config_groups(consumers).items():
        fingerprints = {value for _, value, _ in members}
        if len(fingerprints) > 1:
            detail = "、".join(f"{name}={value[:8]}…" for name, value, _ in sorted(members))
            problems.append(
                f"{label}: 读同一组配置（{'/'.join(sorted(group))}）的工作负载"
                f"指纹不一致（{detail}）")
    distinct |= {value for _, value, _ in consumers}

if len(distinct) < MIN_DISTINCT_FINGERPRINTS:
    problems.append(
        f"所有产物只渲染出 {len(distinct)} 个不同的指纹——"
        "读同一份配置的形态本该各不相同，全等说明注解被换成了写死的字面量")

if problems:
    print(f"CONFIG CHECKSUM WIRING FAILED（{len(problems)} 处）：", file=sys.stderr)
    for problem in problems:
        print(f"  - {problem}", file=sys.stderr)
    sys.exit(1)

print(
    f"ok：{len(roots)} 份产物里 {surveyed} 条读 chart 配置的工作负载都带着配置指纹，"
    f"{len(distinct)} 个互不相同的值；只改 env 配置时指纹确实会变")
PYEOF
