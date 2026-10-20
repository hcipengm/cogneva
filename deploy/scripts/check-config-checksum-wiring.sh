#!/usr/bin/env bash
# 配置内容与 Pod 身份的接线门禁：`checksum/config`。
#
# 主应用与 evolution 从 ConfigMap `cogneva-json` 读同一份 cogneva.json。只改这份
# 配置时，工作负载的 spec 一个字节都不变，滚动更新不会发生——新配置在旧 Pod 里
# 永远读不到，而从任何一侧看都是绿的：渲染成功、parity 对齐、Pod 全 Running。
# 2026-10-20 的演练里实测到的是反面：升级后配置已换、Pod 未换。
#
# 唯一的办法是把这个 Pod 模板绑到配置内容上：Pod 模板多一条
# `checksum/config: <配置的 sha256>`，配置一变注解就变，模板哈希随之变，滚动才发生。
#
# 这条注解**此前没有任何门禁看着**：chart 里删掉它，渲染照常成功；deploy/k3s/
# 静态清单同步删掉，parity 也照常绿（parity 比的是两侧是否一致，不是该不该有）。
# 于是它能被无声删掉，直到下一次只改配置的升级不出 Pod 才被发现。这条门禁补的就是
# 这个判据：**读同一份配置的工作负载，必须带着这份配置的指纹**。
#
# 判据（全部从渲染产物与静态清单推导，不重复 chart 里的常量）：
#   1. 凡 Pod 模板引用了 ConfigMap `cogneva-json`（envFrom 或卷）的工作负载，
#      必须带 `checksum/config`，值是 64 位小写十六进制的 sha256。
#   2. 同一份产物里，读同一份配置的几条工作负载，指纹必须相同——它们哈希的是
#      同一个文件；不等说明有人把某一条钉成了别的值（或改成了写死的字面量）。
#   3. 每种形态至少两条消费者（今天就是主应用与 evolution）。这是**声明**不是推导：
#      谁读这份配置是代码里的事，清单里读不出来。并成一条、删掉一条都会撞它。
#   4. 三种形态与静态清单之间，指纹不能全等：三份 values 渲染出的 config 不同
#      （实测 k3s-single a44c995c…、k3s-multi 8aa308f1…、k8s-standard dffa29ad…），
#      全等意味着有人把 `sha256sum` 表达式换成了写死的字面量——那样配置变了注解
#      也不变，这道门禁本身就成了摆设。
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
import sys

import yaml

DEPLOY = sys.argv[1]
CONFIG_CM = "cogneva-json"
ANNOTATION = "checksum/config"
HEX64 = re.compile(r"^[0-9a-f]{64}$")
WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet", "CronJob")
MIN_CONSUMERS = 2
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


def reads_config(podspec):
    for container in podspec.get("containers") or []:
        for env_from in container.get("envFrom") or []:
            ref = env_from.get("configMapRef") or {}
            if ref.get("name") == CONFIG_CM:
                return True
    for volume in podspec.get("volumes") or []:
        if (volume.get("configMap") or {}).get("name") == CONFIG_CM:
            return True
    return False


def scan(label, pattern):
    """→ (consumers, problems)；consumers 是 [(name, fingerprint)]。"""
    consumers = []
    problems = []
    for path in sorted(glob.glob(pattern)):
        for doc in workload_docs(path):
            podspec, annotations = pod_template(doc)
            if not reads_config(podspec):
                continue
            name = doc["metadata"]["name"]
            value = annotations.get(ANNOTATION)
            if value is None:
                problems.append(
                    f"{label}: {name} 读 {CONFIG_CM} 却没有 {ANNOTATION}——"
                    "只改配置的那次升级不会换 Pod")
                continue
            value = str(value).strip()
            if not HEX64.match(value):
                problems.append(
                    f"{label}: {name} 的 {ANNOTATION} 不是 64 位小写十六进制（{value!r}）")
                continue
            consumers.append((name, value))
    return consumers, problems


roots = []
for profile in sorted(os.listdir(f"{DEPLOY}/rendered")):
    if os.path.isdir(f"{DEPLOY}/rendered/{profile}"):
        roots.append((f"rendered/{profile}", f"{DEPLOY}/rendered/{profile}/*.yaml"))
roots.append(("k3s", f"{DEPLOY}/k3s/*.yaml"))

if not roots:
    sys.exit(f"没有可校验的产物：{DEPLOY}/rendered 与 {DEPLOY}/k3s")

problems = []
distinct = set()
surveyed = 0
for label, pattern in roots:
    consumers, found = scan(label, pattern)
    problems.extend(found)
    surveyed += len(consumers)
    if len(consumers) < MIN_CONSUMERS and not any(p.endswith("却没有 " + ANNOTATION) for p in found):
        problems.append(
            f"{label}: 只找到 {len(consumers)} 条读 {CONFIG_CM} 的工作负载，"
            f"下界是 {MIN_CONSUMERS}（主应用与 evolution）")
    fingerprints = {value for _, value in consumers}
    if len(fingerprints) > 1:
        detail = "、".join(f"{name}={value[:8]}…" for name, value in sorted(consumers))
        problems.append(f"{label}: 读同一份配置的工作负载指纹不一致（{detail}）")
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
    f"ok：{len(roots)} 份产物里 {surveyed} 条读 {CONFIG_CM} 的工作负载都带着配置指纹，"
    f"{len(distinct)} 个互不相同的值")
PYEOF
