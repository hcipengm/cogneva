#!/usr/bin/env bash
# 备份族的「一天」只有一个数，且那个数必须高过从 chart 自己推出来的最坏健康年龄。
#
# 为什么需要这条门禁：三份规则摘要都写着「93600 不是新数……三条不会在『一天』上漂开」，
# 而 `93600` 只活在配置载体里——`grep -rn 93600` 除 `deploy/rendered/` 外只命中
# `deploy/helm/cogneva/files/cogneva.json` 与 `deploy/k3s/cogneva-json-configmap.yaml`，
# **没有任何测试或脚本读过它**。于是那句承诺靠人记得。它推自的两个操作数都在别处，
# 而且两个都改得动：
#   - 排班周期：`values.yaml` 的 `backup.schedule`（cron 串，`17 3 * * *`）；
#   - 迟到宽限：`values.yaml` 的 `backup.startingDeadlineSeconds`。它曾经是
#     `templates/backup.yaml` 里的字面量、连 values 口都没有，门禁那时去扒模板；现在
#     模板只引用这个键，数在 values 里。两个地方都读得到时必须选对：模板里若又写了
#     字面量，生效的是它而 values 里那个成了摆设，所以先断言模板引用这个键、再回
#     values 取数——核的数必须就是生效的那个；
#   - 运行时长：`values.yaml` 的 `backup.runDeadlineSeconds`——健康的一次运行最长能跑
#     这么久才被 kubelet 杀掉，所以它进「最坏健康年龄」。**摘要里那句「实测 41 秒」
#     不是这个操作数**：41 是某一次测量的结果（最新一次实测 72 秒，见下），会随包变大
#     而漂；能进推演的只有「一次运行被允许跑多久」这个上界。上一版这条门禁只算了
#     「一天 ＋ 宽限」，漏了这一项——于是有人把 `runDeadlineSeconds` 抬到 5400 以上时
#     （最坏健康年龄 = 86400 ＋ 1800 ＋ 5400 = 93600，正好顶到界上），一次跑得慢但
#     **成功** 的运行会在成功之前把灯点亮，而门禁照样绿。
# 任何一个动了，界都该跟着动。这个脚本把四件事钉住：
#   1. 四条出现（三条日尺度规则的 `condition.greater_than` ＋ `backup_success_not_recorded`
#      的 promql 里那一个 `<(数字)`）必须是同一个数；
#   2. 那个数必须落在一条**从 chart 自己推出来的带子**里：下端是「一天 ＋ 迟到宽限
#      ＋ 允许的运行时长」（健康部署的年龄上界，界掉到它以下正常部署会常年亮灯），
#      上端是「两天」（`backup_no_recent_success` 的摘要写明它要「一次漏跑就亮、
#      下次成功就灭」，并点名「界定在 48 小时就整天不响」）；
#   3. **摘要说的操作数就是上面相加的那三个**——摘要要么自己点名这组，要么点名它从
#      哪条规则复用这个界（复述会造出第二份算式，而第二份算式正是漂移的成因）；
#   4. **摘要把派生数写进散文时，那些数必须与门算出的相等**——整点小时数 == 界、
#      余量秒数 == 界减最坏健康年龄。前三条核的是摘要点了哪些**名字**，一条都不核
#      它说的**数**；摘要正是告警唯一的散文面（人读的是它），只核名字就等于允许
#      散文里的数无声地变假：改 chart 换了界，门算出的数动了，「the 26 hours」与
#      「headroom is 1800 seconds」一字不动地留着，门照样绿。
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

# 日尺度的那几条：界写在 condition 上。
#
# 演练与卷外副本那四条是同一族的第二个读者面：它们各自也在判「一天」以内有没有
# 东西落地，用的必须是同一个数。把它们留在名单外，界就在这里漂——一条规则改了
# 排班或运行时长，另外两条照旧写着昨天的 26 小时，而谁都没在核它们。
BY_CONDITION = [
    "backup_no_recent_success",
    "backup_never_succeeded_by_the_cronjob",
    "backup_nothing_ever_recorded",
    "backup_restore_drill_never_ran",
    "backup_restore_drill_is_stale",
    "backup_offsite_copy_never_written",
    "backup_offsite_copy_is_stale",
    # 仓库侧的两个读者：异地副本陈旧、以及「从来没人观察过仓库」。它们判的
    # 同样是「一天以内有没有东西落地」，界必须与上面几条同源。钥匙年龄那条
    # （`backup_offsite_repository_key_is_old`）不在名单里是故意的：它的界是
    # 轮换周期（90 天），与排班、宽限、运行时长都不是同一个量。
    "backup_offsite_remote_copy_is_stale",
    "backup_offsite_repository_is_never_observed",
    "backup_cronjob_stopped_scheduling",
    "backup_restore_drill_cronjob_stopped_scheduling",
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

# 覆盖：名单是手写的，而手写的名单只覆盖写它那天想到的规则。后来加进来的规则
# 若也把族界写进 condition 或 promql，名单不会自己长出这一条，界就只在它身上
# 无声地漂——所以这里反向扫一遍：带着这个数的规则必须**已经在名单上**。
# 判据取的是「数是这个界」，不是「文本里出现过」：摘要里复述这个数是被允许的，
# 真正会被拿来判超期的是 condition 与 promql。
carriers = []
# 只扫本族的规则：族是按名字划的（`backup_`），别的族碰巧用同一个数不是
# 漂移，把它们也收进来等于让这条门禁去管别人的界。
for name, rule in rules.items():
    if name in occurrences or not name.startswith("backup_"):
        continue
    numbers = [
        float(v)
        for k, v in rule.get("condition", {}).items()
        if k in ("greater_than", "less_than") and isinstance(v, (int, float))
    ]
    numbers += [float(x) for x in re.findall(r"[<>]\s*(\d+(?:\.\d+)?)", rule.get("promql", ""))]
    if any(n == bound for n in numbers):
        carriers.append(name)
if carriers:
    fail(
        f"这些规则带着备份族的界 {bound:g} 却不在名单上：{'、'.join(sorted(carriers))}。"
        f"把它们加进 BY_CONDITION（或 BY_PROMQL）——不在名单上的规则，界随排班 / "
        f"宽限 / 运行时长改动时不会被这里核到"
    )

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
# 迟到宽限与另外两个操作数一样住在 values 的 `backup:` 块里。读它之前先确认模板
# 要的是**这一个**键：模板里若还写着字面量，装进集群的是那个数，而门禁算的是
# values 里这个——两边各走各的，界被核的是一份没人用的数。
ref = re.compile(r"startingDeadlineSeconds:\s*\{\{\s*\.Values\.backup\.startingDeadlineSeconds\s*\}\}")
stated = [line.strip() for line in template_text.splitlines() if line.strip().startswith("startingDeadlineSeconds:")]
if not stated:
    fail(f"{template_path} 里没有 startingDeadlineSeconds，读不到迟到宽限")
wrong = [line for line in stated if not ref.fullmatch(line)]
if wrong:
    fail(
        f"{template_path} 有 {len(wrong)} 处 startingDeadlineSeconds 不是从 "
        f"`backup.startingDeadlineSeconds` 取的（{wrong[0]}）：模板写死一个字面量时，"
        f"生效的是它，而本门禁核的是 values 里那个数——两个数就此各走各的"
    )
grace_field = re.search(r"^\s+startingDeadlineSeconds:\s*(\d+)\s*(?:#.*)?$", block.group(1), re.MULTILINE)
if grace_field is None:
    fail(f"{values_path} 的 `backup:` 块里没有 startingDeadlineSeconds，读不到迟到宽限")
grace = int(grace_field.group(1))

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

# 第三个前提：**摘要点名的操作数，就是本门禁相加的那三个**。
#
# 为什么它必须在这里：这一族此前的摘要写着自己的算式——「一天 ＋ 宽限 ＋ 41 秒实测 ＋
# 一个读数周期」。那段算式里既有本门禁**没有相加**的项（实测时长是按包大小漂的结果，
# 不是一个上界），又缺了本门禁**真在相加**的那一项（`runDeadlineSeconds`）。于是摘要
# 里的算式与门禁里的算式是分开的两份，谁也没在看另一份——`93600` 与两份算式都不相等
# 这件事，就这样同时躲过了散文和人。
#
# 判据取**名字**，并且**认两种正当形状**：
#   a. 摘要自己点名那三个操作数（`backup_no_recent_success` 就是这么写的）；
#   b. 摘要**不复述算式**、而是点名它从哪条规则那里复用这个界——复述会造出第二份算式，
#      而第二份算式正是这次漂移的成因；`backup_never_succeeded_by_the_cronjob` 走这条，
#      它写的是「93600 不是新数：它就是 backup_no_recent_success 带的那个界」。
#      走这条时，被点名的那条规则必须自己满足 (a)，否则这条链子在空处落脚——所以是
#      递归地查，不是「提了别人的名字就算数」。
DEFERRAL = re.compile(r"the bound ([a-z0-9_]+) carries")
OPERAND_TOKENS = [
    # 同一个操作数的几种正当写法；`once-a-day` 是既有摘要用的那一种，
    # 认不出来就等于逼人为了过门禁改句子，而不是把话写清楚。
    ("每天一次", ("once a day", "once-a-day", "daily run", "run a day", "once per day")),
    ("startingDeadlineSeconds", ("startingDeadlineSeconds",)),
    ("runDeadlineSeconds", ("runDeadlineSeconds",)),
]


def unstated_operands(summary):
    return [label for label, forms in OPERAND_TOKENS if not any(f in summary for f in forms)]


def operands_unaccounted_for(name, seen=()):
    """返回 None 表示这条规则的摘要（或它点名的那条）把三个操作数说清了；
    否则返回没被说到的那几项。"""
    summary = rules[name].get("summary", "")
    missing = unstated_operands(summary)
    if not missing:
        return None
    src = DEFERRAL.search(summary)
    if src is not None and src.group(1) in rules and src.group(1) not in seen:
        return operands_unaccounted_for(src.group(1), seen + (name,))
    return missing


for name in list(BY_CONDITION) + [BY_PROMQL]:
    missing = operands_unaccounted_for(name)
    if missing is not None:
        fail(
            f"规则 {name} 的摘要没有点名本门禁相加的操作数：{'、'.join(missing)}。"
            f"门禁相加的是「排班周期 ＋ startingDeadlineSeconds ＋ runDeadlineSeconds」，"
            f"摘要要么自己写这一组，要么点名它从哪条规则那里复用这个界（被点名的那条"
            f"必须自己写清楚）——两边各写一份算式，就没人会发现另一份算错了"
        )

# 第四个前提：**摘要把派生数写进散文时，那些数必须与门自己算出的相等**。
#
# 为什么它必须在这里：前三个前提核的是「摘要点名了哪几个操作数」（名字），一个都
# **不核它说的数**。摘要可以把界写成「the 26 hours」、把余量写成「headroom is 1800
# seconds」，而这两句都是**从操作数算出来的派生数**——改了排班 / 宽限 / 运行时长，
# 门算出的数变了，散文里的数**一字不动**地留着，门照样绿。摘要正是告警唯一的散文
# 面（人读的是它），所以这句话无声地变假＝人按着一句错的话判断。名字由第三前提核，
# 数由这一个核。
#
# 判据能**从环境读出来**，不是待决策项：门已经算出 bound（四个出现那个数）与 floor
# （三操作数之和），摘要点名的两个派生数分别等于它们：
#   - 整点小时数（`the 26 hours` / `a round 26 hours` / `the same 26 hours`）== bound / 3600；
#   - 余量秒数（`headroom is 1800 seconds`）== bound - floor（「最坏健康年龄之上还留多少」）。
# **只核出现形式，不做全文件搜数字**：摘要里还有别的 `<N> hours`——`backup_no_recent_success`
# 写的「一次漏跑约 48 小时」那个 **48**，说的是**被否掉的那个界的替代方案**、不是本族
# 的界，拿它比 26 会误红。所以整点小时数只认「(the|a round|the same) <N> hours」这种
# **指界**的形状，与第三个前提认操作数名字同一路数。
HOURS_CLAIM = re.compile(r"\b(?:round|the|same)\s+(\d+)\s+hours?\b")
HEADROOM_CLAIM = re.compile(r"\bheadroom\s+(?:is\s+|of\s+)?(\d+)\s+seconds?\b")


def mismatched_derived_numbers(name):
    """返回摘要里「说的数」与门算出的数对不上的那几处（人类可读）。
    没写这类数、或写了且对得上，返回空表。"""
    summary = rules[name].get("summary", "")
    bad = []
    for token in HOURS_CLAIM.findall(summary):
        stated = int(token) * 3600
        if stated != bound:
            bad.append(
                f"整点小时数「{token} hours」= {stated} 秒，而界是 {bound:g} 秒"
                f"（{bound / 3600:.2f} 小时）"
            )
    headroom = bound - floor
    for token in HEADROOM_CLAIM.findall(summary):
        if int(token) != headroom:
            bad.append(
                f"余量「headroom is {token} seconds」，而界比最坏健康年龄 {floor} "
                f"高 {headroom:g} 秒"
            )
    return bad


for name in list(BY_CONDITION) + [BY_PROMQL]:
    bad = mismatched_derived_numbers(name)
    if bad:
        fail(
            f"规则 {name} 的摘要把派生数写进了散文，而它与门算出的对不上："
            f"{'；'.join(bad)}。这两个数是从操作数算出来的——改了 chart 就必须跟着改；"
            f"摘要正是告警唯一的散文面，人读的是它，只核名字不核数等于允许它无声地变假"
        )

print(
    # 条数由名单自己数出来，不写死：名单加一条而这句话没加，读的人会以为
    # 「四条都在核」——这正是本脚本要防的那种散文漂移。
    f"OK: {len(occurrences)} 条出现一致（{bound:g} = {bound / 3600:.2f}h）；"
    f"这 {len(occurrences)} 条摘要都交代了门禁相加的三个操作数（排班周期、"
    f"startingDeadlineSeconds、"
    f"runDeadlineSeconds——自己点名，或点名它复用界的那条）；"
    f"摘要里点名的派生数（整点小时数 == {bound:g}、余量 == {bound - floor:g}）"
    f"与门算出的相符；"
    f"带的端点由 chart 推得：下 {floor}（排班 `{schedule}` → {period} ＋ "
    f"startingDeadlineSeconds {grace} ＋ runDeadlineSeconds {run_seconds}）、"
    f"上 {ceiling}（两天）"
)
PYEOF
