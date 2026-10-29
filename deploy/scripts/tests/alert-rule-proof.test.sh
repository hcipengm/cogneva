#!/usr/bin/env bash
# 「每条托管告警规则都得有一格会响、一格不响的证明」这条门**自己**会不会红。
#
# 为什么要有这一层：这条门判的是「新加的规则必须自带证明」。它自己错成「永远绿」时，
# 唯一的症状就是再也没有人被拦——而那正是它存在的原因（2026-10-28 两条规则就是这么
# 进来的，直到第二天才在另一棵树的 must-green 里炸开）。这里把它的每个输入各改坏一次，
# 每一格都必须红在它该红的那一句上：
#   1. 清单里多一条规则而没有任何夹具点到名：必须红并点出名字；
#   2. 只有「不响」的格：必须红——恒假规则同样满足静格；
#   3. 只有「会响」的格：必须红——永远在响与写对了同形；
#   4. 夹具点到一条清单里没有的规则：必须红（改名／拼错＝那一格是死指针）；
#   5. 基线里留着一条已经有本仓证明的规则：必须红——基线是欠账，不是永久免检；
#   6. `--update-baseline` 想把基线变大：必须拒绝，且基线文件一个字节都不动；
#   7. `--update-baseline` 划掉一条已证明的规则：基线变小，之后判绿。
# 另有三格证明它不会白红、也不会把「看不了」读成「没问题」：真树必须绿；
# `--stock` 在有大片欠账时也退 0（它是读数不是判据）；清单目录或夹具目录空时退 3。
#
# 夹具不碰真清单：判据只吃「清单目录／夹具目录／基线文件」三个入口，这里各造一份最小
# 的，真树只用来跑第一格。
#
# 跑法：bash deploy/scripts/tests/alert-rule-proof.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-proof.sh"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { # <清单目录> <夹具目录> <基线> [其余参数]
    local m="$1" f="$2" b="$3"; shift 3
    COGNEVA_ALERT_MANIFEST_DIR="$m" COGNEVA_ALERT_FIXTURES="$f" \
        COGNEVA_ALERT_PROOF_BASELINE="$b" bash "$gate" "$@" >"${work}/out" 2>&1
    rc=$?
    return 0
}

write_manifest() { # <路径> <规则名…>
    local path="$1"; shift
    {
        printf 'apiVersion: monitoring.coreos.com/v1\nkind: PrometheusRule\n'
        printf 'metadata:\n  name: fixture\nspec:\n  groups:\n    - name: fixture\n      rules:\n'
        local n
        for n in "$@"; do
            printf '        - alert: %s\n          expr: fixture_flag == 1\n          for: 5m\n' "$n"
        done
    } >"$path"
}

fixture_case() { # <规则名> <firing|quiet|both>：一个 tests 项的 YAML 片段
    local name="$1" kind="$2"
    printf '  - interval: 1m\n    input_series:\n'
    printf "      - series: 'fixture_flag'\n        values: '1x20'\n"
    printf '    alert_rule_test:\n'
    case "$kind" in
        firing|both)
            printf '      - eval_time: 10m\n        alertname: %s\n        exp_alerts:\n' "$name"
            printf '          - exp_labels:\n              severity: warning\n' ;;
    esac
    case "$kind" in
        quiet|both)
            printf '      - eval_time: 10m\n        alertname: %s\n        exp_alerts: []\n' "$name" ;;
    esac
}

write_fixture() { # <路径> <规则名:kind>…：一份完整的夹具（只有一个 tests 头）
    local path="$1"; shift
    {
        printf 'rule_files:\n  - rules.yaml\nevaluation_interval: 1m\ntests:\n'
        local spec
        for spec in "$@"; do
            fixture_case "${spec%%:*}" "${spec##*:}"
        done
    } >"$path"
}

# 0) 真树必须绿。
run "${repo}/deploy/k3s/observability/manifests" "${repo}/deploy/k3s/observability/tests" \
    "${repo}/deploy/scripts/alert-rule-proof.baseline"
[ "$rc" -eq 0 ] && ok "真树：判绿" || bad "真树应为绿" "0" "$rc；$(head -2 "${work}/out" | tr '\n' ' ')"

# 下面每一格都从一份「两规则、都有两格证明」的最小树出发。
base() { # <工作目录名>
    local d="${work}/$1"
    mkdir -p "$d/manifests" "$d/tests"
    write_manifest "$d/manifests/99-prometheusrule-fixture.yaml" RuleOne RuleTwo
    write_fixture "$d/tests/99-prometheusrule-fixture.test.yaml" RuleOne:both RuleTwo:both
    : >"$d/baseline.txt"
    printf '%s' "$d"
}

d="$(base clean)"
run "$d/manifests" "$d/tests" "$d/baseline.txt"
[ "$rc" -eq 0 ] && ok "两条规则各有两格证明：判绿" || bad "干净树应为绿" "0" "$rc；$(head -2 "${work}/out" | tr '\n' ' ')"

# 1) 新加一条规则，没有任何夹具点到名。
d="$(base unnamed)"
write_manifest "$d/manifests/99-prometheusrule-fixture.yaml" RuleOne RuleTwo RuleThree
run "$d/manifests" "$d/tests" "$d/baseline.txt"
if [ "$rc" -eq 1 ] && grep -q "RuleThree" "${work}/out"; then
    ok "新规则没有夹具点名：判红并点出名字"
else
    bad "新规则没有夹具点名应红并点名" "1 且含 RuleThree" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 2) 只有「不响」的格。
d="$(base quietonly)"
write_fixture "$d/tests/99-prometheusrule-fixture.test.yaml" RuleOne:both RuleTwo:quiet
run "$d/manifests" "$d/tests" "$d/baseline.txt"
if [ "$rc" -eq 1 ] && grep -q "没有一格断言它会响" "${work}/out" && grep -q "RuleTwo" "${work}/out"; then
    ok "只有静格：判红（恒假的规则不许过关）"
else
    bad "只有静格应红" "1 且含「没有一格断言它会响」" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 3) 只有「会响」的格。
d="$(base firingonly)"
write_fixture "$d/tests/99-prometheusrule-fixture.test.yaml" RuleOne:both RuleTwo:firing
run "$d/manifests" "$d/tests" "$d/baseline.txt"
if [ "$rc" -eq 1 ] && grep -q "没有一格断言它不响" "${work}/out" && grep -q "RuleTwo" "${work}/out"; then
    ok "只有响格：判红（永远在响与写对了同形）"
else
    bad "只有响格应红" "1 且含「没有一格断言它不响」" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 4) 夹具点到一条清单里没有的规则。
d="$(base stray)"
write_fixture "$d/tests/99-prometheusrule-fixture.test.yaml" RuleOne:both RuleTwo:both RuleGhost:both
run "$d/manifests" "$d/tests" "$d/baseline.txt"
if [ "$rc" -eq 1 ] && grep -q "RuleGhost" "${work}/out"; then
    ok "夹具点到清单里没有的规则：判红（死指针）"
else
    bad "死指针应红" "1 且含 RuleGhost" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 5) 基线里留着一条已经有本仓证明的规则。
d="$(base retired)"
printf 'RuleOne\n' >"$d/baseline.txt"
run "$d/manifests" "$d/tests" "$d/baseline.txt"
if [ "$rc" -eq 1 ] && grep -q "要从基线划掉" "${work}/out" && grep -q "RuleOne" "${work}/out"; then
    ok "基线里留着已证明的规则：判红（基线不是永久免检）"
else
    bad "基线未划掉应红" "1 且含 RuleOne" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 6) --update-baseline 想把基线变大：拒绝，且文件不动。
d="$(base nogrow)"
printf 'RuleOne\n' >"$d/baseline.txt"
write_manifest "$d/manifests/99-prometheusrule-fixture.yaml" RuleOne RuleTwo RuleFour
before="$(md5sum <"$d/baseline.txt")"
run "$d/manifests" "$d/tests" "$d/baseline.txt" --update-baseline
after="$(md5sum <"$d/baseline.txt")"
if [ "$rc" -eq 1 ] && [ "$before" = "$after" ]; then
    ok "--update-baseline 想让基线变大：拒绝且文件不动"
else
    bad "--update-baseline 应拒绝且不动文件" "1 且 md5 不变" "$rc；$(cat "$d/baseline.txt" | tr '\n' ' ')"
fi

# 7) --update-baseline 划掉一条已证明的规则：基线变小，之后判绿。
# 同一条基线里还留着一个**已经不存在的规则名**（RuleGhost）：那一条永远不算欠账、
# 于是永远落在「该划掉却没划」那一支，重写基线时必须一起清掉，否则基线里会留一行
# 谁也证明不了、谁也删不掉的名字。
d="$(base shrink)"
printf 'RuleOne\nRuleGhost\n' >"$d/baseline.txt"
run "$d/manifests" "$d/tests" "$d/baseline.txt" --update-baseline
if [ "$rc" -eq 0 ] && ! grep -qx 'RuleOne' "$d/baseline.txt" && ! grep -qx 'RuleGhost' "$d/baseline.txt"; then
    run "$d/manifests" "$d/tests" "$d/baseline.txt"
    [ "$rc" -eq 0 ] && ok "--update-baseline 划掉已证明的、清掉已不存在的：之后判绿" \
        || bad "划掉之后应判绿" "0" "$rc；$(head -2 "${work}/out" | tr '\n' ' ')"
else
    bad "--update-baseline 应成功并清掉 RuleOne 与 RuleGhost" "0 且基线里两个名字都不在" "$rc；$(cat "$d/baseline.txt" | tr '\n' ' ')"
fi

# 8) --stock 在有大片欠账时也退 0：它是读数不是判据。
d="$(base stock)"
write_manifest "$d/manifests/99-prometheusrule-fixture.yaml" RuleOne RuleTwo RuleFive
run "$d/manifests" "$d/tests" "$d/baseline.txt" --stock
if [ "$rc" -eq 0 ] && grep -q 'NEW-DEBT' "${work}/out" && grep -q 'RuleFive' "${work}/out"; then
    ok "--stock 有欠账也退 0，并把新债标成 NEW-DEBT"
else
    bad "--stock 应退 0 并标出 NEW-DEBT" "0 且含 NEW-DEBT" "$rc；$(head -3 "${work}/out" | tr '\n' ' ')"
fi

# 9) 夹具目录空：退 3（零份证据与「都还没证明」同形）。
d="$(base nofix)"
rm -f "$d/tests/"*.yaml
run "$d/manifests" "$d/tests" "$d/baseline.txt"
[ "$rc" -eq 3 ] && ok "夹具目录里一条 *.test.yaml 都没有：退 3（看不了 ≠ 一致）" \
    || bad "空夹具目录应退 3" "3" "$rc"

# 10) 清单目录空：退 3（人口为空时「全覆盖」是假的）。
d="$(base nomani)"
rm -f "$d/manifests/"*.yaml
run "$d/manifests" "$d/tests" "$d/baseline.txt"
[ "$rc" -eq 3 ] && ok "清单目录里一份清单都没有：退 3（人口为空时全覆盖是假的）" \
    || bad "空清单目录应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
