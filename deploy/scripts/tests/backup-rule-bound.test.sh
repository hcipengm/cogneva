#!/usr/bin/env bash
# check-backup-rule-bound.sh 的定向测试：备份族那个「一天」的数，两个前提各自
# 都要红得起来、而且要红到点名那一条。
#
# 为什么要有这份测试：这条门禁的正身是「四条出现同一个数」＋「那个数落在由 chart
# 自己推出来的带子里」。两个前提都**改坏了不报错**——界漂了、排班改了、迟到宽限
# 抬了，配置照样是合法 JSON、parity 照样绿、三条规则照样求值，只是什么时候亮灯
# 悄悄变了。所以每一格都单独造一份夹具，断言**点名的那一句**，不只断言退出码。
#
# 六格，每格只隔离一件事：
#   1. 真文件 -> 绿，且读数里印出界与推得的两个端点；
#   2. 一条日尺度规则的界与同族不同 -> 红，点名那条规则与两个数；
#   3. 排班改成不是「每天一次」 -> 红（周期推不出来，界必须跟着重推）；
#   4. `startingDeadlineSeconds` 抬高到界之下 -> 红（健康部署会被照到）；
#   5. 界抬到两天以上 -> 红（漏一天不响，而摘要写的是漏一次就亮）；
#   6. 负对照：改的是一条**不属于**这一族的规则的 condition -> 仍绿
#      （判据取的是出现形式：按规则名取界，不是全文件搜数字）；
#   7. 负对照：`backup_success_not_recorded` 被改名 -> 红并点名它
#      （界在它的 promql 里，规则没了界也就没了，不能静默少一条）。
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
    r["promql"] = r["promql"].replace("< 93600", f"< {value}")


dump("drift", lambda rules: rule(rules, "backup_nothing_ever_recorded")["condition"].__setitem__("greater_than", 43200.0))
dump("too-wide", lambda rules: widen(rules, 259200))
dump("unrelated", lambda rules: rule(rules, "container_memory_near_limit")["condition"].__setitem__("greater_than", 999.0))
dump("renamed", lambda rules: rule(rules, "backup_success_not_recorded").__setitem__("name", "backup_success_unrecorded"))
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
grep -q '88200' <<<"${out}" || fail "绿色读数里没有印出一＋宽限推得的端点：${out}"
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
grep -q '97200' <<<"${out}" || fail "红的时候没印出推得的端点：${out}"

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

echo "backup-rule-bound: 7 passed, 0 failed"
