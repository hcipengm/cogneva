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
# chart 渲染出的全部 ConfigMap。它们由两份模板产出：app-config.yaml 出前三份，
# configmap.yaml 出 env 那份 cogneva-config。
CONFIG_CMS = {
    "cogneva-json",
    "cogneva-prompts",
    "cogneva-evolution-config",
    "cogneva-config",
}
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


def pod_template(doc):
    spec = doc.get("spec") or {}
    if doc["kind"] == "CronJob":
        spec = (spec.get("jobTemplate") or {}).get("spec") or {}
    template = spec.get("template") or {}
    return template.get("spec") or {}, (template.get("metadata") or {}).get("annotations") or {}


def consumed_config(podspec):
    """→ Pod 模板引用到的 chart ConfigMap 名字集合（envFrom 与卷都算）。"""
    found = set()
    for container in (podspec.get("containers") or []) + (podspec.get("initContainers") or []):
        for env_from in container.get("envFrom") or []:
            name = (env_from.get("configMapRef") or {}).get("name")
            if name in CONFIG_CMS:
                found.add(name)
    for volume in podspec.get("volumes") or []:
        name = (volume.get("configMap") or {}).get("name")
        if name in CONFIG_CMS:
            found.add(name)
    return found


def scan(label, pattern):
    """→ (consumers, problems)；consumers 是 [(name, fingerprint)]。"""
    consumers = []
    problems = []
    for path in sorted(glob.glob(pattern)):
        for doc in workload_docs(path):
            podspec, annotations = pod_template(doc)
            names = consumed_config(podspec)
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
            consumers.append((name, value))
    return consumers, problems


def render(chart, profile, extra):
    cmd = [
        "helm", "template", "cogneva", chart,
        "--namespace", "cogneva",
        "-f", profile,
    ] + extra
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"渲染失败（{' '.join(cmd)}）：\n{proc.stderr}")
    return [doc for doc in yaml.safe_load_all(proc.stdout)
            if isinstance(doc, dict) and doc.get("kind") in WORKLOAD_KINDS]


# ---- 第二段：注解必须跟着 env 那份 ConfigMap 变 ----
chart = os.path.join(DEPLOY, "helm", "cogneva")
profile = os.path.join(chart, "profiles", "k3s-single.yaml")
if not os.path.isfile(os.path.join(chart, "Chart.yaml")):
    sys.exit(f"没有 chart：{chart}（这一段要真渲染，缺 chart 等于没判）")
if not os.path.isfile(profile):
    sys.exit(f"没有 profile：{profile}")
if shutil.which("helm") is None:
    sys.exit("缺少依赖：helm（这一段要真渲染两次，没有它这道判据等于没跑）")

# 这个键只出现在 cogneva-config 里：变它就不会顺带改到卷里那份配置。
PROBE_SET = "notification.dingtalkUrl=https://example.invalid/checksum-probe"
base = render(chart, profile, [])
probed = render(chart, profile, ["--set", PROBE_SET])

probe_problems = []
base_by_key = {(d["kind"], d["metadata"]["name"]): d for d in base}
probed_by_key = {(d["kind"], d["metadata"]["name"]): d for d in probed}
if set(base_by_key) != set(probed_by_key):
    probe_problems.append(
        "只为 env 配置加了一个 --set，工作负载集合就变了——这段判据的两次渲染不可比")
for key, doc in base_by_key.items():
    podspec, annotations = pod_template(doc)
    if not consumed_config(podspec):
        continue
    before = annotations.get(ANNOTATION)
    after = pod_template(probed_by_key[key])[1].get(ANNOTATION)
    if before is None:
        # 第一段会点名；这里不重复报。
        continue
    if str(before).strip() == str(after).strip():
        probe_problems.append(
            f"{key[0]}/{key[1]}：只改 env 那份配置（{PROBE_SET}）指纹没变"
            f"（{str(before).strip()[:12]}…）——注解没哈希 configmap.yaml，"
            "配置变了这个 Pod 不会滚动")

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
    fingerprints = {value for _, value in consumers}
    if len(fingerprints) > 1:
        detail = "、".join(f"{name}={value[:8]}…" for name, value in sorted(consumers))
        problems.append(f"{label}: 读同一组配置的工作负载指纹不一致（{detail}）")
    distinct |= fingerprints

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
