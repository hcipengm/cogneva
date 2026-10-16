#!/usr/bin/env bash
# check-backup-rule-bound.sh 的定向测试：备份族那个「一天」的数，两个前提各自
# 都要红得起来、而且要红到点名那一条。
#
# 为什么要有这份测试：这条门禁的正身是「四条出现同一个数」＋「那个数落在由 chart
# 自己推出来的带子里」。两个前提都**改坏了不报错**——界漂了、排班改了、迟到宽限
# 抬了，配置照样是合法 JSON、parity 照样绿、三条规则照样求值，只是什么时候亮灯
# 悄悄变了。所以每一格都单独造一份夹具，断言**点名的那一句**，不只断言退出码。
#
# 下面每一格只隔离一件事。**这里刻意不写格数**：写在散文里的计数会与用例漂开
# （本文件第一版就写着「六格」而下面列着七条），要几格看末尾那句自报的数。
#   1. 真文件 -> 绿，且读数里印出界与推得的两个端点；
#   2. 一条日尺度规则的界与同族不同 -> 红，点名那条规则与两个数；
#   3. 排班改成不是「每天一次」 -> 红（周期推不出来，界必须跟着重推）；
#   4. `startingDeadlineSeconds` 抬高到界之下 -> 红（健康部署会被照到）；
#   5. 界抬到两天以上 -> 红（漏一天不响，而摘要写的是漏一次就亮）；
#   6. 负对照：把**别的**规则的 condition 改成**同一个数** -> 仍绿
#      （判据取的是出现形式：按规则名取界；若滑成「在整份配置里搜这个数」，
#       这一格就会因为多出一处而红——写别的数（如 999）则滑了也不红，白搭）；
#   7. 负对照：`backup_success_not_recorded` 被改名 -> 红并点名它
#      （界在它的 promql 里，规则没了界也就没了，不能静默少一条）；
#   8. `runDeadlineSeconds` 抬到「一天 ＋ 宽限」之上 -> 红，点名它
#      （一次跑满允许时长的**成功**运行会在成功之前把灯点亮）；
#   9. 摘要不再点名 `runDeadlineSeconds` -> 红，并点名缺的那一项与那条规则
#      （摘要里的算式必须就是门禁里的那一份：此前写的是「41 秒实测」，
#       那是某次测量的结果、门禁从没相加过，而真在相加的 `runDeadlineSeconds`
#       摘要里没有——两份算式各写各的）；
#  10. 负对照：摘要改写措辞但仍点名那三个操作数 -> 仍绿（判据是「点没点名」，
#      不是逐字比对，否则每一次正当改写都会被判红）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-backup-rule-bound.sh"
chart="${repo}/deploy/helm/cogneva"
config="${chart}/files/cogneva.json"
values="${chart}/values.yaml"
template="${chart}/templates/backup.yaml"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

cp "${values}" "${work}/values.yaml"
cp "${template}" "${work}/backup.yaml"

# 夹具：按名字改一条规则的 condition.greater_than / 改名 / 改别的规则。
python3 - "${config}" "${work}" <<'PYEOF'
import json
import re
import sys

src, out = sys.argv[1], sys.argv[2]
doc = json.load(open(src, encoding="utf-8"))


def dump(name, mutate):
    copy = json.loads(json.dumps(doc))
    mutate(copy["observability"]["infra_watch"]["rules"])
    json.dump(copy, open(f"{out}/{name}.json", "w", encoding="utf-8"))


def rule(rules, name):
    for r in rules:
        if r["name"] == name:
            return r
    raise SystemExit(f"夹具要改的规则不在配置里：{name}")


def widen(rules, value):
    """把**四条出现**一起抬上去：这一格的判据是「带子」，不是「一致」，
    所以四者必须仍然相等，否则红的是上一格的一致性检查。"""
    for name in ("backup_no_recent_success", "backup_never_succeeded_by_the_cronjob",
                 "backup_nothing_ever_recorded"):
        rule(rules, name)["condition"]["greater_than"] = float(value)
    r = rule(rules, "backup_success_not_recorded")
    # 按**出现形式**改（`<(数字)`），不绑字面量：上一版写 `replace("< 93600", …)`，
    # 源配置里只要空格变一下（`<93600`）这个替换就静默不动，夹具少了第四条、门在
    # 「不一致」上红，于是这一格红得和它宣称的理由无关——门自己还是绿的、也没改名。
    new_promql, n = re.subn(r"<\s*\d+(?:\.\d+)?", f"< {value}", r["promql"], count=1)
    if n != 1:
        raise SystemExit(
            f"夹具改不动 {r['name']} 的 promql：里面没有 `<(数字)`，这一格量不到东西"
        )
    r["promql"] = new_promql


dump("drift", lambda rules: rule(rules, "backup_nothing_ever_recorded")["condition"].__setitem__("greater_than", 43200.0))
dump("too-wide", lambda rules: widen(rules, 259200))
dump("unrelated", lambda rules: rule(rules, "container_memory_near_limit")["condition"].__setitem__("greater_than", 93600.0))
dump("renamed", lambda rules: rule(rules, "backup_success_not_recorded").__setitem__("name", "backup_success_unrecorded"))


def drop_operand(rules):
    """把摘要里 `runDeadlineSeconds` 这个名字换掉——门禁相加的那一项还在配置里，
    只是摘要不再点它的名。这正是本族此前的形状：摘要写的是「41 秒实测」，一个门禁
    从没相加过的项，而真在相加的那项没被写出来。"""
    r = rule(rules, "backup_no_recent_success")
    before = "".join(r.get("summary", "").split())
    after = before.replace("runDeadlineSeconds", "a-run-allowance")
    if after == before:
        raise SystemExit("夹具改不动摘要：真摘要里没有 `runDeadlineSeconds`，这一格量不到东西")
    r["summary"] = after


def reword_but_keep(rules):
    """正当改写：换措辞、换语序，三个操作数的名字一个不少（负对照）。"""
    rule(rules, "backup_no_recent_success")["summary"] = (
        "The run is scheduled once a day, so a day plus one startingDeadlineSeconds "
        "of slack plus one runDeadlineSeconds of allowance bounds a healthy age; "
        "an age above the bound means a run has been missed."
    )


dump("summary-drops-operand", drop_operand)
dump("summary-reworded", reword_but_keep)
PYEOF

# 排班改成每天两次（每 12 小时一次）：周期不再是 86400，界必须重推。
sed 's/^  schedule: .*/  schedule: "17 3,15 * * *"/' "${values}" >"${work}/values-notdaily.yaml"
# 迟到宽限抬到 3 小时：一天 ＋ 宽限 = 97200，已高过 93600。
sed 's/^  startingDeadlineSeconds: .*/  startingDeadlineSeconds: 10800/' "${template}" >"${work}/backup-wide-grace.yaml"

run_gate() {
    local out
    if out="$(bash "${gate}" "$1" "$2" "$3" 2>&1)"; then rc=0; else rc=$?; fi
    printf '%s\n' "${out}"
    return "${rc}"
}

fail() { echo "FAIL: $*"; exit 1; }

# 1. 真文件：绿，且读数印出界与两个端点。
if ! out="$(run_gate "${config}" "${values}" "${template}")"; then
    printf '%s\n' "${out}" >&2
    fail "真 chart 上这条门禁就该是绿的"
fi
grep -q '93600' <<<"${out}" || fail "绿色读数里没有印出界本身：${out}"
grep -q '91800' <<<"${out}" || fail "绿色读数里没有印出一＋宽限＋运行时长推得的端点：${out}"
grep -q '172800' <<<"${out}" || fail "绿色读数里没有印出两天的端点：${out}"

# 2. 界漂了：红，并点名那条规则与两个数。
if out="$(run_gate "${work}/drift.json" "${values}" "${template}")"; then
    fail "同族的界都不一样了，这条门禁还是绿的"
fi
grep -q 'backup_nothing_ever_recorded' <<<"${out}" || fail "红的时候没点名漂开的那条规则：${out}"
grep -q '93600' <<<"${out}" || fail "红的时候没印出同族的那个数：${out}"
grep -q '43200' <<<"${out}" || fail "红的时候没印出漂开的那个数：${out}"

# 3. 排班不是每日：红，且把排班串念出来。
if out="$(run_gate "${config}" "${work}/values-notdaily.yaml" "${template}")"; then
    fail "排班改成每天两次了，这条门禁还是绿的"
fi
grep -q '17 3,15 \* \* \*' <<<"${out}" || fail "红的时候没念出那条排班：${out}"

# 4. 迟到宽限抬高到界之下：红，并点名 startingDeadlineSeconds。
if out="$(run_gate "${config}" "${values}" "${work}/backup-wide-grace.yaml")"; then
    fail "一天 + 宽限已经高过界了，这条门禁还是绿的"
fi
grep -q 'startingDeadlineSeconds' <<<"${out}" || fail "红的时候没点名宽限这一项：${out}"
grep -q '100800' <<<"${out}" || fail "红的时候没印出推得的端点：${out}"

# 5. 界抬到两天以上：红，念的是「漏一天不响」。
if out="$(run_gate "${work}/too-wide.json" "${values}" "${template}")"; then
    fail "界抬到三天了，这条门禁还是绿的"
fi
grep -q '172800' <<<"${out}" || fail "红的时候没印出两天的端点：${out}"

# 6. 负对照：改的是不属这一族的规则，仍绿。
if ! out="$(run_gate "${work}/unrelated.json" "${values}" "${template}")"; then
    printf '%s\n' "${out}" >&2
    fail "改了一条不相干的规则的 condition，这条门禁红了——判据滑成了全文件搜数字"
fi

# 7. 负对照：规则被改名，红并点名它。
if out="$(run_gate "${work}/renamed.json" "${values}" "${template}")"; then
    fail "界所在的规则被改名了，这条门禁还是绿的"
fi
grep -q 'backup_success_not_recorded' <<<"${out}" || fail "红的时候没点名那条找不到的规则：${out}"

# 8. 允许的运行时长抬到「一天 ＋ 宽限」之上：红，并点名 runDeadlineSeconds。
#    最坏健康年龄 = 86400 ＋ 1800 ＋ 7200 = 95400，已高过界 93600——一次跑得慢但**成功**
#    的运行会在成功之前把灯点亮，而上一版门禁只算「一天 ＋ 宽限」，照样绿。
sed 's/^  runDeadlineSeconds: .*/  runDeadlineSeconds: 7200/' "${values}" >"${work}/values-longrun.yaml"
if out="$(run_gate "${config}" "${work}/values-longrun.yaml" "${template}")"; then
    fail "允许的运行时长抬高到最坏健康年龄高过界了，这条门禁还是绿的"
fi
grep -q 'runDeadlineSeconds' <<<"${out}" || fail "红的时候没点名运行时长这一项：${out}"
grep -q '95400' <<<"${out}" || fail "红的时候没印出推得的端点：${out}"

# 9. 摘要不再点 `runDeadlineSeconds` 的名：红，并点名缺的是哪一项。
#    这一格测的是**第三个前提**：摘要说的操作数必须就是门禁相加的那三个。此前这一族
#    的摘要写的是「一天 ＋ 宽限 ＋ 41 秒实测 ＋ 一个读数周期」——41 秒是某次测量的
#    结果，门禁从没相加过它；而门禁真在相加的 runDeadlineSeconds 摘要里没有。两份
#    算式谁也没在看另一份，所以前两格全绿而散文是错的。
if out="$(run_gate "${work}/summary-drops-operand.json" "${values}" "${template}")"; then
    fail "摘要不再点名门禁相加的操作数，这条门禁还是绿的——散文里的算式与门禁里的算式又成了两份"
fi
grep -q 'runDeadlineSeconds' <<<"${out}" || fail "红的时候没点名摘要缺的那个操作数：${out}"
grep -q 'backup_no_recent_success' <<<"${out}" || fail "红的时候没点名是哪条规则的摘要：${out}"

# 10. 负对照：摘要**改写**但仍点名那三个操作数 -> 仍绿。
#     判据取的是「点没点名」，不是「原文一字不差」；写成逐字比对会把每一次正当改写
#     都判红，而正当改写正是这条门禁希望发生的事。
if ! out="$(run_gate "${work}/summary-reworded.json" "${values}" "${template}")"; then
    printf '%s\n' "${out}" >&2
    fail "摘要改写了措辞但仍点名那三个操作数，这条门禁红了——判据滑成了逐字比对"
fi

# 自报的格数**从本文件里数出来**，不手写：手写的数会在加了一格之后仍旧印旧值，
# 而「少了一格」正是这个数唯一要提醒的事。标记＝正文里 `# <数字>. ` 开头的注释行
# （开头那三行是 `#   ` 缩进，不在此列）。
#
# 单数一个选择子不够——它数的是**标记的形状**，把一格缩进一下就少一个、加一行别的
# 注释就多一个，两种漂都不经过真的删格。所以再数一个**定义上每格恰好一个**的真值
# （调用点＝以 `if ` 开头且跑一次 run_gate 的那些行）与它对账，对不上就红：这样
# 「标记与用例不一致」这件事本身有了判据，而不是印个数了事。
cases="$(grep -c '^# [0-9]' "${BASH_SOURCE[0]}")" ||
    { echo "FAIL: 数不出用例数（正文里的 `# <数字>. ` 标记不在了）" >&2; exit 1; }
calls="$(grep -cE '^if .*\$\(run_gate ' "${BASH_SOURCE[0]}")" ||
    { echo "FAIL: 数不出 run_gate 的调用点" >&2; exit 1; }
if [ "${cases}" != "${calls}" ]; then
    echo "FAIL: 正文标记数（${cases}）与真正跑过的格数（${calls}，按 run_gate 调用点数的）对不上——要么有标记没有格，要么有格没有标记" >&2
    exit 1
fi

echo "backup-rule-bound: ${cases} passed, 0 failed"

