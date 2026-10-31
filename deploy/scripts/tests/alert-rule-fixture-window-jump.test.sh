#!/usr/bin/env bash
# principle: feedback_a_jump_on_the_window_start_is_outside_the_increase
# check-alert-rule-fixture-window-jump.py 的定向测试。
#
# 被测的是「夹具里造出来的跳变必须落在窗口内部」。治的病是这一格**看起来通过了**：
# 范围选择器左开，跳变正好压在被减窗口的左端（或紧挨着它的前一个采样）时，
# `increase`/`rate` 读到的是平序列，答案是 0，于是那一格静悄悄地什么都不测——promtool
# 也说它静（静格本来就期望静），规则装了、告警不响，没有任何读数说这件事。
#
# 格子分两半，一半钉「什么写法必须红」，一半钉「不该红的别红」——后者更重要：
#   - 跳变压在左端（`0x15 1x15`，窗口 15m、eval 30m）⇒ 红，判词点名告警与序列；
#   - 跳变在左端前一个采样（`0x14 1x16`）⇒ 也红：这是把一格从宽窗口的夹具抄到窄窗口
#     规则上时的形状，2026-10-23 的 loki 那一格就是这么来的；
#   - 跳变在窗口内部（`0x17 1x13`）⇒ 绿；
#   - 跳变既压在左端又铺满整个窗口（`0+1x31`，每步都涨）⇒ 绿：边界那一个采样只是
#     其中一次，表达式读得到窗口里那些；
#   - 没有窗口的规则（`max(gauge) > 0`）即使跳变压在左端也不判：瞬时值没有窗口；
#   - 树里没有一格带窗口的规则 ⇒ 3，不是 0：零格与「每格都合格」在汇总行上同形；
#   - 读不动 yaml ⇒ 3，不是 0；
#   - 真树：退出码 0，且 `cases=` 至少 20 格——这一格防的是扫描面缩到零而没人发现。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-fixture-window-jump.py"
[ -f "$gate" ] || { echo "FAIL: 被测脚本不在：$gate" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1（期望退出码 $2，实得 $3）"; fi; }
ck_has() { if printf '%s\n' "$2" | grep -qF -- "$3"; then ok "$1"; else
  bad "$1（期望含 $3）"; printf '%s\n' "$2" | sed 's/^/        实得: /' | head -12 >&2; fi; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

tree() { mkdir -p "$1/manifests" "$1/tests"; }

rule() { # $1=树 $2=告警名 $3=表达式
  cat >"$1/manifests/10-$2.yaml" <<YAML
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: fixture
spec:
  groups:
    - name: fixture
      rules:
        - alert: $2
          expr: $3
          for: 5m
          labels:
            severity: warning
YAML
}

fixture() { # $1=树 $2=测试文件名 $3=告警名 $4=序列名 $5=values $6=eval_time
  cat >"$1/tests/$2.test.yaml" <<YAML
rule_files:
  - ../manifests/10-$3.yaml
evaluation_interval: 1m
tests:
  - interval: 1m
    input_series:
      - series: '$4'
        values: '$5'
    alert_rule_test:
      - eval_time: $6
        alertname: $3
        exp_alerts: []
YAML
}

RATE_EXPR='increase(cogneva_fixture_events_total[15m]) > 0'
GAUGE_EXPR='max(cogneva_fixture_gauge) > 0'

# ---- case 1: 跳变压在窗口左端 ----
t="$work/on-the-edge"; tree "$t"
rule "$t" CognevaFixtureRate "$RATE_EXPR"
fixture "$t" edge CognevaFixtureRate cogneva_fixture_events_total '0x15 1x15' 30m
out="$(python3 "$gate" "$t/tests" "$t/manifests" 2>&1)"; rc=$?
ck_rc "跳变正好压在窗口左端 ⇒ 红" 1 "$rc"
ck_has "判词点名那一格的告警" "$out" "CognevaFixtureRate"
ck_has "判词说清左开" "$out" "left-open"

# ---- case 2: 跳变在左端前一个采样（抄格子到窄窗口的形状）----
t="$work/before-the-edge"; tree "$t"
rule "$t" CognevaFixtureRate "$RATE_EXPR"
fixture "$t" before CognevaFixtureRate cogneva_fixture_events_total '0x14 1x16' 30m
out="$(python3 "$gate" "$t/tests" "$t/manifests" 2>&1)"; rc=$?
ck_rc "跳变在左端前一个采样 ⇒ 也红" 1 "$rc"
ck_has "判词点名那一格的告警" "$out" "CognevaFixtureRate"

# ---- case 3: 跳变在窗口内部 ----
t="$work/inside"; tree "$t"
rule "$t" CognevaFixtureRate "$RATE_EXPR"
fixture "$t" inside CognevaFixtureRate cogneva_fixture_events_total '0x17 1x13' 30m
out="$(python3 "$gate" "$t/tests" "$t/manifests" 2>&1)"; rc=$?
ck_rc "跳变在窗口内部 ⇒ 绿" 0 "$rc"
ck_has "汇总行报出被判的格数" "$out" "cases=1 violations=0"

# ---- case 4: 边界那一个采样只是其中一次 ----
t="$work/every-step"; tree "$t"
rule "$t" CognevaFixtureRate "$RATE_EXPR"
fixture "$t" ramp CognevaFixtureRate cogneva_fixture_events_total '0+1x31' 30m
out="$(python3 "$gate" "$t/tests" "$t/manifests" 2>&1)"; rc=$?
ck_rc "每步都涨、边界也在涨 ⇒ 绿" 0 "$rc"

# ---- case 5: 没有窗口的规则不判 ----
t="$work/gauge"; tree "$t"
rule "$t" CognevaFixtureRate "$RATE_EXPR"
fixture "$t" ok CognevaFixtureRate cogneva_fixture_events_total '0x17 1x13' 30m
rule "$t" CognevaFixtureGauge "$GAUGE_EXPR"
fixture "$t" gauge CognevaFixtureGauge cogneva_fixture_gauge '0x15 1x15' 30m
out="$(python3 "$gate" "$t/tests" "$t/manifests" 2>&1)"; rc=$?
ck_rc "瞬时判据即使跳变压在左端也不判" 0 "$rc"
ck_has "只有带窗口的那一格进了判据" "$out" "cases=1 violations=0"

# ---- case 6: 树里没有带窗口的规则 ----
t="$work/no-window"; tree "$t"
rule "$t" CognevaFixtureGauge "$GAUGE_EXPR"
fixture "$t" gauge CognevaFixtureGauge cogneva_fixture_gauge '0+1x31' 30m
out="$(python3 "$gate" "$t/tests" "$t/manifests" 2>&1)"; rc=$?
ck_rc "零格被判 ⇒ 3，不是 0" 3 "$rc"
ck_has "零格与干净分开说" "$out" "judged nothing"

# ---- case 7: 读不动夹具 ----
t="$work/broken"; tree "$t"
rule "$t" CognevaFixtureRate "$RATE_EXPR"
printf 'tests: [ this is not yaml\n  : :\n' >"$t/tests/broken.test.yaml"
out="$(python3 "$gate" "$t/tests" "$t/manifests" 2>&1)"; rc=$?
ck_rc "读不动 yaml ⇒ 3，不是 0" 3 "$rc"

# ---- case 8: 真树 ----
out="$(cd "$repo" && python3 "$gate" 2>&1)"; rc=$?
ck_rc "真树的夹具全部合格" 0 "$rc"
judged="$(printf '%s\n' "$out" | sed -n 's/.*cases=\([0-9]*\).*/\1/p' | tail -1)"
if [ "${judged:-0}" -ge 20 ]; then
  ok "真树里判到 ${judged} 格（扫描面没有缩到零）"
else
  bad "真树里只判到 ${judged:-0} 格——扫描面缩了，或夹具改名了"
fi

printf '\nalert-rule-fixture-window-jump.test: %d passed / %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
