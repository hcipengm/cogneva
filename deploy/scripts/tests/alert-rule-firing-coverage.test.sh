#!/usr/bin/env bash
# 「哪几条规则真的被证明过会响」这条门**自己**会不会红。
#
# 为什么要有这一层：这条门判的是「新增规则必须自带一格证明」。它自己错成「永远绿」时，
# 唯一的症状就是再也没有人被拦——而那正是它存在的原因。这里把它的三个输入各改坏一次：
#   1. 库里多一条没人跑过的规则（新债）：必须红，并点出名字；
#   2. 基线里多一条**已经证明**的规则（基线没划掉）：必须红——否则基线是永久免检；
#   3. 夹具点名一条库里没有的规则（死指针）：必须红；
#   4. 基线写不回去时（想把它写大）：--update-baseline 必须拒绝，且基线文件一个字节都不动；
#   5. 配置读不到：退 3，不许把「看不了」读成「没有新债」。
# 另有两格证明它不会白红：真树必须绿；--stock 在有欠账时也退 0（它是读数不是判据）。
#
# 跑法：bash deploy/scripts/tests/alert-rule-firing-coverage.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-firing-coverage.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
fixtures="${repo}/deploy/k3s/observability/tests"
baseline="${repo}/deploy/scripts/alert-rule-firing-coverage.baseline"
proven="boot_phase_footprint_grew"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { COGNEVA_ALERT_CONFIG="${1:-$config}" COGNEVA_ALERT_FIXTURES="${2:-$fixtures}" \
    COGNEVA_ALERT_COVERAGE_BASELINE="${3:-$baseline}" bash "$gate" "${@:4}" \
    >"${work}/out" 2>&1; rc=$?; }

# 0) 真树必须绿。
run
case "$rc" in
    0) ok "真树：绿" ;;
    *) bad "真树应绿" "0" "$rc；$(tail -3 "${work}/out" | tr '\n' ' ')" ;;
esac

# 1) 库里多一条没人跑过的规则：必须红，且点名。
python3 -I - "$config" "${work}/new-rule.json" <<'PY'
import json
import sys
src, out = sys.argv[1:3]
config = json.load(open(src, encoding="utf-8"))
config["observability"]["infra_watch"]["rules"].append(
    {"name": "a_rule_nobody_ever_ran", "promql": "vector(0)", "for_seconds": 0,
     "severity": "warning", "summary": "test", "description": "test"})
json.dump(config, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
PY
run "${work}/new-rule.json"
if [ "$rc" -eq 1 ] && grep -q "a_rule_nobody_ever_ran" "${work}/out"; then
    ok "库里新增一条没人跑过的规则：红并点名（新债不许悄悄进树）"
else
    bad "新增未证明规则应红并点名" "rc=1 且输出含规则名" "$rc；$(head -2 "${work}/out" | tr '\n' ' ')"
fi

# 2) 基线里留着一条**已经证明**的规则：必须红——基线不是永久免检名单。
{ cat "$baseline"; echo "$proven"; } >"${work}/stale.baseline"
run "$config" "$fixtures" "${work}/stale.baseline"
if [ "$rc" -eq 1 ] && grep -q "$proven" "${work}/out" && grep -q "已证明" "${work}/out"; then
    ok "基线里留着已证明的规则：红（基线要跟着划掉）"
else
    bad "基线里留已证明规则应红" "rc=1 且点到 $proven" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 3) 基线里留着一条**库里已经没有**的规则：同样必须红（同一条规矩的另一面）。
{ cat "$baseline"; echo "a_rule_that_no_longer_exists"; } >"${work}/dead.baseline"
run "$config" "$fixtures" "${work}/dead.baseline"
if [ "$rc" -eq 1 ] && grep -q "a_rule_that_no_longer_exists" "${work}/out" && grep -q "不在库里" "${work}/out"; then
    ok "基线里留着已删除的规则：红（改名/删除没同步基线）"
else
    bad "基线里留已删规则应红" "rc=1 且点到已删名" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 4) 夹具点名一条库里没有的规则：必须红（否则那一格永远不执行，而跑者那头只会报取不到）。
mkdir -p "${work}/fx"
cp "${fixtures}"/*.test.yaml "${work}/fx/" 2>/dev/null
printf '# rule: %s_typo\n\n' "$proven" >"${work}/fx/stray.test.yaml"
run "$config" "${work}/fx"
if [ "$rc" -eq 1 ] && grep -q "${proven}_typo" "${work}/out" && grep -q "不在这一库" "${work}/out"; then
    ok "夹具点名不存在的规则：红（死指针先在这里被拦）"
else
    bad "夹具点名不存在规则应红" "rc=1 且点到 ${proven}_typo" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 4b) 夹具点了名、但只断言它安静：不算证明。把该规则从基线里拿掉后必须红——
# 「静格」证明的是这份数据上它没响，恒假的规则同样满足，而那正是这一族要抓的形状。
mkdir -p "${work}/fx-silent"
cp "${fixtures}"/*.test.yaml "${work}/fx-silent/" 2>/dev/null
# 挑一条**目录里没有任何夹具**的规则：否则真夹具里的响格会让它照样算已证明，
# 这一格就测不到「静格不算证明」。
# 规则名从读数里现取，不写死：写死的那一天有人给它写了夹具，这一格就会静默地测不到
# 东西（静格与响格并存 ⇒ 它照样算已证明）。取当前第一条**没有任何证明**的规则。
run "$config" "$fixtures" "$baseline" --stock
silent_rule=$(awk -F'\t' '$1 == "unproven" { print $2; exit }' "${work}/out")
[ -n "$silent_rule" ] && ok "为无证明的规则建夹具：取到 ${silent_rule}" \
    || bad "找不到一条没有证明的规则，这一格无从构造" "至少一条 unproven" "0"
printf '# rule: %s\n\nevaluation_interval: 1m\ntests:\n  - interval: 1m\n    input_series:\n      - series: %s\n        values: "1x5"\n    alert_rule_test:\n      - eval_time: 4m\n        alertname: %s\n        exp_alerts: []\n' \
    "$silent_rule" "$silent_rule" "$silent_rule" >"${work}/fx-silent/silent.test.yaml"
grep -v "^${silent_rule}\$" "$baseline" >"${work}/no-silent.baseline"
run "$config" "${work}/fx-silent" "${work}/no-silent.baseline"
if [ "$rc" -eq 1 ] && grep -q "$silent_rule" "${work}/out"; then
    ok "只断言安静的夹具：不算证明（恒假规则同样满足它）"
else
    bad "只断言安静应记成未证明" "rc=1 且点到 $silent_rule" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi
run "$config" "${work}/fx-silent" "${work}/no-silent.baseline" --stock
grep -q "^named-but-never-fires	${silent_rule}" "${work}/out" \
    && ok "读数里单列 named-but-never-fires（点过名≠证明会响）" \
    || bad "读数应把只断言安静的规则单列" "named-but-never-fires 行" "$(grep "$silent_rule" "${work}/out" | head -1)"

# 5) --update-baseline 想把基线写大：必须拒绝，且基线文件不被改动。
cp "$baseline" "${work}/grow.baseline"
before=$(sha256sum "${work}/grow.baseline" | cut -d' ' -f1)
run "${work}/new-rule.json" "$fixtures" "${work}/grow.baseline" --update-baseline
after=$(sha256sum "${work}/grow.baseline" | cut -d' ' -f1)
if [ "$rc" -eq 1 ] && [ "$before" = "$after" ]; then
    ok "--update-baseline 拒绝把基线写大，且一个字节没动"
else
    bad "--update-baseline 应拒绝变大" "rc=1 且基线未变" "rc=$rc，基线$( [ "$before" = "$after" ] && echo 未变 || echo 已被改写 )"
fi

# 6) 配置读不到：退 3，不许读成「没有新债」。
run "${work}/does-not-exist.json"
[ "$rc" -eq 3 ] && ok "配置读不到：退 3（看不了 ≠ 没有新债）" \
    || bad "配置读不到应退 3" "3" "$rc"

# 7) --stock 是读数不是判据：有欠账也退 0。
run "$config" "$fixtures" "$baseline" --stock
if [ "$rc" -eq 0 ] && grep -q "^proven	${proven}" "${work}/out"; then
    ok "--stock 印出覆盖情况并退 0（读数不判）"
else
    bad "--stock 应退 0 并标出已证明的那条" "rc=0 且含 proven 行" "$rc"
fi

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
