#!/usr/bin/env bash
# 「构建与部署」那半页的清单门禁。
#
# 这一面的成品不是 `/api/v1/cluster/deployments` 那份 JSON，是**打开就能看见命名空间里
# 每种工作负载各几个、谁在跑第几版**的那张表。它曾经只读 Deployment 一种：11 行，全是
# Deployment，而 postgres / redis（StatefulSet）、claim-store / host-ledger / buildah
# （DaemonSet）、备份（CronJob）、每次主线部署（Job）都在集群里跑着，页面上没有。空白
# 读起来像「本来就没有」——这是这一面最坏的一种错法，因为它不报错、不缺数据，只是把
# 存在的东西说成不存在。
#
# 判据：
#   1. 服务端认五种工种，且每个工种问的是它自己那条接口（apps/v1 与 batch/v1 分组不同）；
#   2. 每个工种读的是它自己那组状态字段——DaemonSet 的「要几个」在 status 里，Job 的
#      「到位」是成功次数，CronJob 没有副本；
#   3. 工种之间不能通用的字段给 null 而不是 0（0 在屏上是「零个就绪」，不是「没这回事」）；
#   4. 有一条没有这次改动就会红的测试守着上面三件事；
#   5. 前端那一栏有工种列，那一行读的是后端给的 `kind`；
#   6. 不适用的格子写「不适用」，不是空白；
#   7. 旧的单一 Deployment 读法没有回来（`deployments_from` 这种按 Deployment 形状读
#      全表的函数不能复活）；
#   9. 没有控制器拥有的裸 Pod 也在清单里。评测环境（toolathlon 一道题一台 Pod）与宿主
#      脚本直接 apply 的探针既不是 Deployment 也不是 Job，只读那五种时它们一条都不
#      出现在屏上——评测正在跑，而工作负载那一栏看不出这件事在发生。
#
#   8. 判坏消息按条件的名字判。服务端给的是原样读数（`ok = 状态是 True`），方向和名字
#      有关：Job 的 `Failed=True` 是失败、`Failed=False` 才是它该有的样子。一律按
#      `!ok` 当坏消息，跑挂的 Job 会被画成绿点——和把 Succeeded 画成红点是同一个错误的
#      两个方向。
#
# 用法：bash deploy/scripts/check-web-workload-inventory.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
CLIENT = os.path.join(WEB, "api/client.ts")
SERVER = os.path.join(ROOT, "crates/cog-gateway/src/cluster.rs")
VIEW = os.path.join(WEB, "views/DeliveryView.tsx")
TEST = "the_other_four_kinds_are_counted_by_their_own_status_fields"

for path in (CLIENT, SERVER, VIEW):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {os.path.relpath(path, ROOT)}")

problems = []

server = open(SERVER, encoding="utf-8").read()
client = open(CLIENT, encoding="utf-8").read()
view = open(VIEW, encoding="utf-8").read()

# ── 1. 五种工种各有自己的接口 ──
kinds = ["Deployment", "StatefulSet", "DaemonSet", "CronJob", "Job", "Pod"]
for kind in kinds:
    if not re.search(rf"WorkloadKind::{kind}\b", server):
        problems.append(f"服务端没有 WorkloadKind::{kind}——这一种工作负载进不了清单")
groups = {
    "deployments": "apps",
    "statefulsets": "apps",
    "daemonsets": "apps",
    "cronjobs": "batch",
    "jobs": "batch",
}
# 核心组没有 `/apis` 前缀：Pod 那条路径是 `/api/v1/namespaces/…/pods`。
CORE = {"pods": ""}
for resource, group in groups.items():
    if not re.search(rf"apis/{group}/v1/namespaces/.*{resource}", server):
        problems.append(
            f"服务端没有问 /apis/{group}/v1/…/{resource}——这一种工作负载读不到，"
            "而屏上只会少一行，不会有任何东西变红"
        )

for resource in CORE:
    if not re.search(rf'"/api/v1/namespaces/\{{namespace\}}/{resource}"', server):
        problems.append(
            f"服务端没有问 /api/v1/namespaces/…/{resource}——裸 Pod（评测环境、一次性探针）"
            "不在清单里，而评测跑起来的时候屏上看不出这件事在发生"
        )
if not re.search(r"WorkloadKind::Pod", server):
    problems.append("服务端没有 WorkloadKind::Pod——没有控制器拥有的 Pod 进不了清单")

# ── 2. 每个工种读自己那组状态字段 ──
own_fields = {
    "DaemonSet": "desiredNumberScheduled",
    "Job": "completions",
    "CronJob": None,
}
for kind, field in own_fields.items():
    if field is None:
        continue
    if field not in server:
        problems.append(
            f"服务端没有读 {field}——{kind} 的副本/完成数只能从这一格读，"
            "用 Deployment 的 spec.replicas 顶替会读成「零个」"
        )
if "jobTemplate" not in server:
    problems.append(
        "服务端没有读 spec.jobTemplate.spec.template.spec.containers——CronJob 的"
        "容器声明比别的工种深一层，读错位置只能读到空镜像列表，"
        "那读起来像「这个 CronJob 没有容器」"
    )

# ── 3. 工种之间不能通用的字段给 null，不给 0 ──
if not re.search(r"pub\s+ready:\s*Option<u32>", server):
    problems.append(
        "WorkloadView 的 ready 不是 Option——用 0 表示「这一种没有这个概念」，"
        "屏上读起来是「零个就绪」"
    )
if not re.search(r"pub\s+desired:\s*Option<u32>", server):
    problems.append("WorkloadView 的 desired 不是 Option——CronJob 会被读成「要 0 个」")

# ── 4. 一条没有这次改动就会红的测试 ──
if TEST not in server:
    problems.append(f"{os.path.relpath(SERVER, ROOT)}: 没有测试 {TEST}")

# ── 5. 前端那一栏有工种列，读的是后端的 kind ──
if not re.search(r"kind:\s*string", client):
    problems.append("client.ts: WorkloadView 没有 kind 字段——屏上分不出这一行是什么工种")
if not re.search(r"key:\s*'kind',\s*title:\s*'类型'", view):
    problems.append("DeliveryView: 表里没有「类型」这一列——同一张表里五种工作负载混在一起，分不出谁是谁")
if not re.search(r"\bd\.kind\b", view):
    problems.append("DeliveryView: 类型列没有读 d.kind——那一格是别的来源，不是服务端报的工种")
if not re.search(r"getWorkloads", view):
    problems.append("DeliveryView: 没有用 getWorkloads——清单可能还停在只读 Deployment 的那一版")

# ── 6. 不适用的格子写「不适用」 ──
if "不适用" not in view:
    problems.append(
        "DeliveryView: 没有一处写「不适用」——非 Deployment 的版本/滚动列只能留空白，"
        "而空白读起来像「这一行没读到」"
    )

# ── 7. 旧的单一 Deployment 读法没有回来 ──
if re.search(r"fn\s+deployments_from\b", server):
    problems.append(
        f"{os.path.relpath(SERVER, ROOT)}: deployments_from 还在——按 Deployment 的形状读"
        "整张表的读法一旦复活，其余四种又会静默消失"
    )

# ── 8. 坏消息按条件的名字判 ──
if not re.search(r"function\s+conditionIsTrouble\s*\(", view):
    problems.append(
        "DeliveryView: 没有按名字判条件的函数——一律按「不是 True 就是坏消息」，"
        "Failed=True 的 Job 会被画成绿点"
    )
elif "'ReplicaFailure'" not in view or "Failed" not in view:
    problems.append(
        "DeliveryView: 判条件的函数里没有 ReplicaFailure / Failed 这两个名字——"
        "这两种 True 是坏消息，反过来才是"
    )
# 「定义了却没人用」是最常见的半落地：函数在、名字对，而屏上那一行还是原来的写法。
uses = len(re.findall(r"\bconditionIsTrouble\b", view))
if "d.conditions.some(conditionIsTrouble)" not in view:
    problems.append(
        "DeliveryView: 「有几个有坏条件」那一格没有用 conditionIsTrouble——"
        "定义了的判据没有人用，屏上数的还是原来的读法"
    )
elif uses < 3:
    problems.append(
        f"DeliveryView: conditionIsTrouble 只出现 {uses} 次（定义 + 至少两处使用），"
        "判色或明细仍有一处按原样读数判"
    )

if problems:
    sys.exit("构建与部署的清单不合格：\n  " + "\n  ".join(problems))

print(
    "ok：构建与部署列出全部六种工作负载（"
    + "、".join(kinds)
    + "），每种读自己的状态字段，不适用的格子写「不适用」"
)
PYEOF
