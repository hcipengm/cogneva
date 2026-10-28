#!/usr/bin/env bash
# 「哪几条规则真的被证明过会响」这条门**自己**会不会红。
#
# 为什么要有这一层：这条门判的是「新增规则必须自带一格证明」。它自己错成「永远绿」时，
# 唯一的症状就是再也没有人被拦——而那正是它存在的原因。这里把它的三个输入各改坏一次：
#   1. 库里多一条没人跑过的规则（新债）：必须红，并点出名字；
#   2. 基线里多一条**已经证明**的规则（基线没划掉）：必须红——否则基线是永久免检；
#   3. 夹具点名一条库里没有的规则（死指针）：必须红；
#   4. 基线写不回去时（想把它写大）：--update-baseline 必须拒绝，且基线文件一个字节都不动；
#   5. 配置读不到：退 3，不许把「看不了」读成「没有新债」；
#   6. 夹具目录在、但一条 `*.test.yaml` 都没读到：同样退 3。零份证据与「整库都还没被
#      证明」在判词上同形，而基线恰好覆盖全库时**旧版会退 0 说全覆盖**——那是这条门
#      最不能犯的错：它绿了，却一份证据都没读。
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
# 挑一条**只有一份夹具点名**的规则，把那份从副本里拿掉：它就从「已证明」退回「未知」。
# 曾经是「从读数里现取第一条 unproven」，那写法在库里还欠着账时能借到现成的标本；
# 欠账清零那天，它自己变成了「找不到标本」——一条只在有债时才测得了门的门。
# 现造标本不依赖欠账：从夹具头部的 `# rule:` 点名声里数出现次数为 1 的规则即可。
# 不清空反例而只取现成的一条，是因为「静格与响格并存 ⇒ 照样算已证明」：
# 真正在测的是「没有任何响格时它算不算数」，所以标本必须先把响格拿掉。
declare -A named_in=()
for f in "${fixtures}"/*.test.yaml; do
    while IFS= read -r name; do
        named_in[$name]="${named_in[$name]:-} ${f}"
    done < <(sed -n 's/^# rule: //p' "$f")
done
silent_rule=""; silent_file=""
for name in "${!named_in[@]}"; do
    set -- ${named_in[$name]}
    if [ "$#" -eq 1 ]; then silent_rule="$name"; silent_file="$1"; break; fi
done
if [ -n "$silent_rule" ]; then
    rm -f "${work}/fx-silent/$(basename "${silent_file}")"
    run "$config" "${work}/fx-silent" "$baseline" --stock
    awk -F'\t' -v r="$silent_rule" '$1 == "unproven" && $2 == r { found = 1 } END { exit found ? 0 : 1 }' "${work}/out" \
        && ok "拿掉唯一一份夹具后，${silent_rule} 退回未证明（标本是现造的，不靠欠账）" \
        || bad "拿掉唯一一份夹具后应退回未证明" "${silent_rule} 出现在 unproven 行" "$(grep -c . "${work}/out") 行"
else
    bad "找不到一条只被一份夹具点名的规则，这一格无从构造" "至少一条 count==1" "0"
fi
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

# 6b) 夹具目录在、但一条 *.test.yaml 都没有：退 3。
# 反例按**最坏**的那种摆：基线写成整库全量，于是「一份证据都没读到」在旧版判词里
# 恰好等于「零条新债、零条要划掉」——退 0 并印 OK。零份证据与「全覆盖」同形。
mkdir -p "${work}/fx-empty"
python3 -I - "$config" "${work}/all.baseline" <<'PY'
import json
import sys
config = json.load(open(sys.argv[1], encoding="utf-8"))
names = sorted(r["name"] for r in config["observability"]["infra_watch"]["rules"] if r.get("name"))
open(sys.argv[2], "w", encoding="utf-8").write("".join(n + "\n" for n in names))
PY
run "$config" "${work}/fx-empty" "${work}/all.baseline"
if [ "$rc" -eq 3 ] && grep -q "一条 \*.test.yaml 都没读到" "${work}/out"; then
    ok "夹具目录空的：退 3（零份证据 ≠ 整库都还没证明）"
else
    bad "空夹具目录应退 3" "rc=3 且判词点名零份证据" "$rc；$(head -2 "${work}/out" | tr '\n' ' ')"
fi

# 7) --stock 是读数不是判据：有欠账也退 0。
run "$config" "$fixtures" "$baseline" --stock
if [ "$rc" -eq 0 ] && grep -q "^proven	${proven}" "${work}/out"; then
    ok "--stock 印出覆盖情况并退 0（读数不判）"
else
    bad "--stock 应退 0 并标出已证明的那条" "rc=0 且含 proven 行" "$rc"
fi

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
