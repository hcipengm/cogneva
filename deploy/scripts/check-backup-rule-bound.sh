#!/usr/bin/env bash
# 备份族的「一天」只有一个数，且那个数必须高过从 chart 自己推出来的最坏健康年龄。
#
# 为什么需要这条门禁：三份规则摘要都写着「93600 不是新数……三条不会在『一天』上漂开」，
# 而 `93600` 只活在配置载体里——`grep -rn 93600` 除 `deploy/rendered/` 外只命中
# `deploy/helm/cogneva/files/cogneva.json` 与 `deploy/k3s/cogneva-json-configmap.yaml`，
# **没有任何测试或脚本读过它**。于是那句承诺靠人记得。它推自的两个操作数都在别处，
# 而且两个都改得动：
#   - 排班周期：`values.yaml` 的 `backup.schedule`（cron 串，`17 3 * * *`）；
#   - 迟到宽限：`templates/backup.yaml` 的 `startingDeadlineSeconds`（**模板里的字面量，
#     连 values 口都没有**，`values.yaml` 的 `backup:` 块里没有这一项）；
#   - 运行时长：`values.yaml` 的 `backup.runDeadlineSeconds`——健康的一次运行最长能跑
#     这么久才被 kubelet 杀掉，所以它进「最坏健康年龄」。**摘要里那句「实测 41 秒」
#     不是这个操作数**：41 是某一次测量的结果（最新一次实测 72 秒，见下），会随包变大
#     而漂；能进推演的只有「一次运行被允许跑多久」这个上界。上一版这条门禁只算了
#     「一天 ＋ 宽限」，漏了这一项——于是有人把 `runDeadlineSeconds` 抬到 5400 以上时
#     （最坏健康年龄 = 86400 ＋ 1800 ＋ 5400 = 93600，正好顶到界上），一次跑得慢但
#     **成功** 的运行会在成功之前把灯点亮，而门禁照样绿。
# 任何一个动了，界都该跟着动。这个脚本把两件事钉住：
#   1. 四条出现（三条日尺度规则的 `condition.greater_than` ＋ `backup_success_not_recorded`
#      的 promql 里那一个 `<(数字)`）必须是同一个数；
#   2. 那个数必须落在一条**从 chart 自己推出来的带子**里：下端是「一天 ＋ 迟到宽限
#      ＋ 允许的运行时长」（健康部署的年龄上界，界掉到它以下正常部署会常年亮灯），
#      上端是「两天」（`backup_no_recent_success` 的摘要写明它要「一次漏跑就亮、
#      下次成功就灭」，并点名「界定在 48 小时就整天不响」）。
# 两个端点的操作数都是读出来的，所以改排班 / 改宽限 / 改运行时长的人都会在这里被拦；
# 至于界在带子里落在哪一点是运维取舍，本脚本不替它拍。
#
# 判据取的是**出现形式**，不是全文件搜数字：日尺度那三条按规则名取
# `condition.greater_than`，成功那条取 promql 里第一个 `<(数字)`。同族的
# `backup_never_succeeded` / `backup_run_abandoned` / `backup_run_over_its_own_deadline`
# 用的是 `> 0` 那一档，不在这条判据里（那份测试的最后一格就是拿它们当负对照）。
#
# 用法：bash deploy/scripts/check-backup-rule-bound.sh [cogneva.json] [values.yaml] [backup.yaml]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="${ROOT}/deploy/helm/cogneva"
CONFIG="${1:-${CHART}/files/cogneva.json}"
VALUES="${2:-${CHART}/values.yaml}"
TEMPLATE="${3:-${CHART}/templates/backup.yaml}"

for f in "${CONFIG}" "${VALUES}" "${TEMPLATE}"; do
    [ -f "${f}" ] || { echo "找不到文件：${f}" >&2; exit 2; }
done

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "${CONFIG}" "${VALUES}" "${TEMPLATE}" <<'PYEOF'
import json
import re
import sys

config_path, values_path, template_path = sys.argv[1:4]

# 日尺度那三条：界写在 condition 上。
BY_CONDITION = [
    "backup_no_recent_success",
    "backup_never_succeeded_by_the_cronjob",
    "backup_nothing_ever_recorded",
]
# 成功那条：界写在 promql 里（`(time() - …) < 93600`），不在 condition 上。
BY_PROMQL = "backup_success_not_recorded"

doc = json.load(open(config_path, encoding="utf-8"))
rules = {r["name"]: r for r in doc["observability"]["infra_watch"]["rules"]}


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


occurrences = {}
for name in BY_CONDITION:
    rule = rules.get(name)
    if rule is None:
        fail(f"规则 {name} 不在配置里——改名的同时也把界带走了，本门禁按名字取它的界")
    gt = rule.get("condition", {}).get("greater_than")
    if gt is None:
        fail(f"规则 {name} 的 condition 上没有 greater_than，本门禁读不到它的界")
    occurrences[name] = float(gt)

rule = rules.get(BY_PROMQL)
if rule is None:
    fail(f"规则 {BY_PROMQL} 不在配置里——界就在它的 promql 里，规则没了界也就没了")
m = re.search(r"<\s*(\d+(?:\.\d+)?)", rule["promql"])
if m is None:
    fail(f"规则 {BY_PROMQL} 的 promql 里没有 `<(数字)`，本门禁读不到它的界：{rule['promql']}")
occurrences[BY_PROMQL] = float(m.group(1))

bounds = set(occurrences.values())
if len(bounds) != 1:
    detail = "、".join(f"{n}={v:g}" for n, v in sorted(occurrences.items()))
    fail(f"备份族的界不止一个数（{detail}）——摘要承诺它们「不会在一天上漂开」")
bound = bounds.pop()

# 从 chart 自己读那两个操作数，不抄。
values_text = open(values_path, encoding="utf-8").read()
block = re.search(r"^backup:\n((?:[ \t].*\n|\n)*)", values_text, re.MULTILINE)
if block is None:
    fail(f"{values_path} 里没有顶层 `backup:` 块，读不到排班")
sched = re.search(r"^\s+schedule:\s*[\"']?([^\"'\n#]+?)[\"']?\s*(?:#.*)?$", block.group(1), re.MULTILINE)
if sched is None:
    fail(f"{values_path} 的 `backup:` 块里没有 `schedule`，读不到排班周期")
schedule = sched.group(1).strip()

fields = schedule.split()
if len(fields) != 5:
    fail(f"排班 `{schedule}` 不是五段的 cron 串，推不出周期")
minute, hour, dom, month, dow = fields
# 分与时必须是**单个数字**：`*` / `*/15` / `3,15` / `3-5` 都是「一天不止一次」，
# 周期就不是 86400 了（`3,15` 这一格踩过：只挡 `*` 和 `/` 会把每天两次读成每天一次）。
if dom != "*" or dow != "*" or not minute.isdigit() or not hour.isdigit():
    fail(
        f"排班 `{schedule}` 不是「每天一次」，本门禁只会从每日排班推周期——"
        f"排班变了，备份族的界必须跟着重推"
    )
period = 24 * 60 * 60

template_text = open(template_path, encoding="utf-8").read()
deadline = re.search(r"^\s*startingDeadlineSeconds:\s*(\d+)\s*$", template_text, re.MULTILINE)
if deadline is None:
    fail(f"{template_path} 里没有 `startingDeadlineSeconds: <数字>`，读不到迟到宽限")
grace = int(deadline.group(1))

# 第三个操作数：一次运行被允许跑多久。它也在同一个 `backup:` 块里，读得到。
run_deadline = re.search(r"^\s+runDeadlineSeconds:\s*(\d+)\s*(?:#.*)?$", block.group(1), re.MULTILINE)
if run_deadline is None:
    fail(f"{values_path} 的 `backup:` 块里没有 `runDeadlineSeconds`，读不到允许的运行时长")
run_seconds = int(run_deadline.group(1))

floor = period + grace + run_seconds
ceiling = 2 * period
if not bound > floor:
    fail(
        f"备份族的界 {bound:g} 没有高过「一天 {period} ＋ startingDeadlineSeconds {grace} "
        f"＋ runDeadlineSeconds {run_seconds}」= {floor}：一次跑满允许时长的**成功**运行"
        f"会在成功之前把灯点亮（健康部署被照到）"
    )
if not bound < ceiling:
    fail(
        f"备份族的界 {bound:g} 到了「两天 {ceiling}」以上："
        f"漏掉的那一天它不响，要等第二天也漏掉才亮——而摘要写的是漏一次就亮"
    )

print(
    f"OK: 四条出现一致（{bound:g} = {bound / 3600:.2f}h）；"
    f"带的端点由 chart 推得：下 {floor}（排班 `{schedule}` → {period} ＋ "
    f"startingDeadlineSeconds {grace} ＋ runDeadlineSeconds {run_seconds}）、"
    f"上 {ceiling}（两天）"
)
PYEOF
