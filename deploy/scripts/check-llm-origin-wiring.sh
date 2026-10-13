#!/usr/bin/env bash
# 环境轴（COGNEVA_LLM_ORIGIN）的接线门禁。
#
# token 账按两条轴记每一笔花费：actor 说「哪个组件烧的」，origin 说「在哪个环境
# 烧的」。同一个组件在多个环境里跑（生成器既在控制面的任务池里跑，也在沙盒里跑），
# 所以两条轴互不蕴含——**只记 actor 时，"哪个环境烧的"这个问题在读数面上无解**。
#
# 这条轴的值由**进程自己声明**，而进程是部署出来的，所以声明落在清单上。它坏起来
# 没有症状：值是自由字符串，写错一个字母、或者干脆没写，读数照常发布，只是那一格
# 落进 `unknown`，而 `unknown` 不是环境、不计进任何一家的总数（见 usage_store.rs
# 里 `LlmOrigin::Unknown` 的注释）。渲染期合法、parity 也绿、面板上少一格，没人会
# 去看。这与 check-deployment-name-wiring.sh 是同一个形状：**写者身份类的东西在
# 清单里错了，症状要到读数面才出现，所以判据必须提前到渲染产物上。**
#
# 判据（闭集从代码读，不在这里抄一份）：
#   1. 凡设了这条 env 的工作负载，值必须是**字面量**（不是 valueFrom），且在闭集里。
#      闭集即 `LlmOrigin::declared` 认的那几个值——写错、写了个想当然的新词，
#      在这里红，而不是在读数面上静默变成 unknown。`unknown` 本身不是可声明的值，
#      它在闭集之外，声明它等于声明「我没说」。
#   2. 一个 profile 里至少要出现两个不同的值。只声明一个值时这条轴不区分任何东西，
#      等于没做——这正是它存在的理由。
#   3. 沙盒执行器必须声明 `sandbox`，且**别的**工作负载不许声明它。沙盒是实验性
#      代码跑的地方，它那一格存在的意义就是能与控制面分开读；两处共用一个值就把
#      要分开的东西合回去了。
#   4. 闭集里除 `resident_worker` 之外的每个值都要有生产者：清单里有人声明它，
#      或者代码里有 `with_origin` 调用点指名它。**`resident_worker` 豁免**——它是
#      常驻 worker 会话，生产者在本仓之外（会话侧直连上游），本仓里找不到不是缺陷，
#      去树里找一个反而是错的。豁免是写死的一个名字，因为「生产者在树外」这件事
#      树里读不出来。
#
# 用法：bash deploy/scripts/check-llm-origin-wiring.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import glob, os, re, sys

import yaml

ROOT = sys.argv[1]
RENDERED = os.path.join(ROOT, "deploy/rendered")
CLOSED_SET_SOURCE = os.path.join(ROOT, "crates/cog-observability/src/usage_store.rs")
CRATES = os.path.join(ROOT, "crates")
ENV_NAME = "COGNEVA_LLM_ORIGIN"
# 生产者在树外的那一个值。见脚本头注释第 4 条。
PRODUCER_OUTSIDE_THE_TREE = {"resident_worker"}
MIN_DISTINCT = 2
WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet", "CronJob")

# 闭集从代码读。抄一份在这里就多了一处会漂的值，而这条门的全部意义就是
# 「清单里的值必须与代码认的值一致」——自己抄的那份一旦漂，门会替错的那边说话。
src = open(CLOSED_SET_SOURCE).read()
arms = re.findall(r'Some\("([a-z_]+)"\)\s*=>\s*(?:LlmOrigin|Self)::(\w+)', src)
if not arms:
    sys.exit(f"闭集没读出来：{CLOSED_SET_SOURCE} 里找不到 LlmOrigin::declared 的匹配臂")
declarable = {value: variant for value, variant in arms}

profiles = sorted(d for d in os.listdir(RENDERED)
                  if os.path.isdir(os.path.join(RENDERED, d)))
if not profiles:
    sys.exit(f"没有可校验的渲染产物：{RENDERED}")


def docs(profile):
    for path in sorted(glob.glob(os.path.join(RENDERED, profile, "*.yaml"))):
        with open(path) as fh:
            for doc in yaml.safe_load_all(fh):
                if isinstance(doc, dict) and doc.get("kind"):
                    yield path, doc


def pod_template(doc):
    spec = doc["spec"]
    if doc["kind"] == "CronJob":
        return spec["jobTemplate"]["spec"]["template"]
    return spec["template"]


def env_entries(tpl):
    for c in (tpl["spec"].get("containers") or []) + (tpl["spec"].get("initContainers") or []):
        for e in c.get("env") or []:
            yield c.get("name"), e


# 树里的生产者：`with_origin(...)` 的实参指名了闭集里的哪一项。按调用形式认，
# 不按裸子串认——一个值名出现在别的地方（注释、别的 API）不构成有人声明它。
source_producers = set()
for path in glob.glob(os.path.join(CRATES, "**", "*.rs"), recursive=True):
    try:
        text = open(path).read()
    except (OSError, UnicodeDecodeError):
        continue
    for arg in re.findall(r"with_origin\(\s*([^)]*)\)", text):
        for value, variant in declarable.items():
            if variant in arg:
                source_producers.add(value)

failures = []
declared_by_profile = {}
for profile in profiles:
    before = len(failures)
    declared = []  # (workload, container, value)
    for path, doc in docs(profile):
        if doc["kind"] not in WORKLOAD_KINDS:
            continue
        name = doc["metadata"]["name"]
        workload = f'{doc["kind"]}/{name}'
        for container, env in env_entries(pod_template(doc)):
            if env.get("name") != ENV_NAME:
                continue
            where = f"{profile}:{workload}:{container}:{os.path.basename(path)}"
            value = env.get("value")
            if env.get("valueFrom") or value is None:
                failures.append(
                    f"{where}：{ENV_NAME} 不是字面量。这一格要的是**这个进程自己**在"
                    f"哪个环境里跑，任何 valueFrom 都是从别处借一个值——借来的名字里"
                    f"没有「本进程在哪」这条信息")
                continue
            if value not in declarable:
                failures.append(
                    f"{where}：{ENV_NAME}={value!r} 不在闭集里（代码认的是 "
                    f"{sorted(declarable)}）。读侧认不出的值读成 unknown，"
                    f"那一格就从「哪个环境」变成「没人说」，且没有任何读数会报错")
                continue
            declared.append((workload, container, value))

    values = {value for _, _, value in declared}
    if len(values) < MIN_DISTINCT:
        failures.append(
            f"{profile}：只声明了 {len(values)} 个不同的 {ENV_NAME} 值 {sorted(values)}，"
            f"下界是 {MIN_DISTINCT}——只声明一个值时这条轴不区分任何环境")

    for workload, container, value in declared:
        if "sandbox" in workload and value != "sandbox":
            failures.append(
                f"{profile}:{workload}:{container}：沙盒执行器声明了 {value!r}。"
                f"它在的环境就是沙盒，混进别的值得不到「沙盒烧掉多少」这个读数")
        if value == "sandbox" and "sandbox" not in workload:
            failures.append(
                f"{profile}:{workload}:{container}：声明了 {value!r}，但它不是沙盒"
                f"执行器。两处共用一个值＝把要分开读的花费合回去")
    if len(failures) == before:
        print(f"{profile}: " + ", ".join(
            f"{w}[{c}]={v}" for w, c, v in sorted(declared)), flush=True)
    declared_by_profile[profile] = declared

# 闭集里的值都要有生产者，除了生产者在树外的那个。
produced = {value for profile in profiles for _, _, value in declared_by_profile[profile]}
produced |= source_producers
for value in declarable:
    if value in PRODUCER_OUTSIDE_THE_TREE:
        continue
    if value not in produced:
        failures.append(
            f"闭集里的 {value!r} 没有任何生产者：清单里没人声明它，代码里也没有 "
            f"with_origin 调用点指名它。闭集里留着一条永远为空的路，"
            f"读到的人无法区分「这个环境今天没花钱」与「这个环境没有写者」")

if failures:
    for f in failures:
        print("FAIL: " + f, file=sys.stderr)
    sys.exit(1)
print(f"LLM ORIGIN WIRING OK：闭集 {sorted(declarable)} 全部有生产者"
      f"（{sorted(PRODUCER_OUTSIDE_THE_TREE)} 的生产者在树外，豁免）；"
      f"清单里每个值都是闭集内的字面量，沙盒单独一格")
PYEOF
