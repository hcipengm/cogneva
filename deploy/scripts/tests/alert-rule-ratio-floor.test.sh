#!/usr/bin/env bash
# principle: feedback_a_rate_reading_and_a_rate_rule_are_two_different_numbers
# check-alert-rule-ratio-floor.py 的定向测试。
#
# 被测的是「拿自己平均值的两倍当界的规则，必须另配一个绝对值下限」。治的病是
# **这个形状可以被合法地写回来**：一条 `deriv(...) > 2 * 终生平均` 的规则在低基数
# 序列上会被写成常红（噪声翻倍还是噪声），而在高基数上又可能太晚；两种错在读数面上
# 都是「规则没响」或「规则一直响」，看不出界本身少了半边。
#
# 格子分两半，一半钉「什么样的写法必须红」，一半钉「不该红的别红」——后者更重要：
# 一扇把正常写法判红的门会被绕过，绕过之后连它本来守的那条也一起没了。
#   - 速率 + 跟另一个序列的倍数比 + 另有一个绝对下限 ⇒ 绿；
#   - 同样的形状但没有下限 ⇒ 红，判词点名规则；
#   - `>= 0` 也算下限（它挡的是负数与零，不是数值大小）；
#   - 年龄跟自己的周期倍数比（`time() - ts > 6 * period`）⇒ **不判**：两边同为时长，
#     周期本身就是下限，再加一个界只是重复；
#   - 速率跟常数比（`deriv(x[6h]) > 0.5`）⇒ 不判：没有「比值」这回事；
#   - 普通阈值规则（`x > 90`）⇒ 不判，但计入 unjudged，因为「没判」与「判过且通过」
#     在汇总行上必须分开；
#   - 两套库都在判：只在清单里判、漏掉进程内规则，等于一半的门；
#   - 读不到规则库 ⇒ 3，不是 0；**读到了但零条规则** ⇒ 也是 3：零条与「全都合规」
#     在汇总行上同形（`rules=0 ... violations=0`）。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-ratio-floor.py"
[ -f "$gate" ] || { echo "FAIL: 被测脚本不在：$gate" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1（期望退出码 $2，实得 $3）"; fi; }
ck_has() { if printf '%s\n' "$2" | grep -qF -- "$3"; then ok "$1"; else
  bad "$1（期望含 $3）"; printf '%s\n' "$2" | sed 's/^/        实得: /' | head -10 >&2; fi; }
ck_hasnt() { if printf '%s\n' "$2" | grep -qF -- "$3"; then bad "$1（不该含 $3）"; else ok "$1"; fi; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# 两套库都在，且各自至少一条规则：门按「每套库都必须读到至少一条」判（读到零条与
# 「全都合规」在汇总行上同形），所以只用清单的格子也要往进程内库放一条不判的占位规则，
# 只用进程内库的格子也要往清单里放。占位规则是普通阈值，不进 ratio 家族。
seed() {
  mkdir -p "$1/deploy/k3s/observability/manifests" "$1/deploy/helm/cogneva/files"
  printf 'PlaceholderJSON|max(node_inodes_pct) > 85\n' | json_rules "$1"
  yaml_rule "$1" PlaceholderYAML 'max(node_inodes_pct) > 85'
}

json_rules() { # $1=树，stdin 每行 name|promql
  cat >"$work/.json-maker.py" <<'PY'
import json, sys
rules = []
for line in sys.stdin.read().splitlines():
    if not line.strip():
        continue
    name, promql = line.split("|", 1)
    rules.append({"name": name, "promql": promql, "severity": "warning", "summary": name})
json.dump({"observability": {"infra_watch": {"rules": rules}}},
          open(sys.argv[1], "w", encoding="utf-8"), indent=2)
PY
  python3 "$work/.json-maker.py" "$1/deploy/helm/cogneva/files/cogneva.json"
}

yaml_rule() { # $1=树 $2=告警名 $3=表达式
  cat >"$1/deploy/k3s/observability/manifests/10-$2.yaml" <<YAML
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
          for: 30m
          labels:
            severity: warning
YAML
}

run() { # $1=树 [args...]
  local tree="$1"; shift
  python3 "$gate" "$tree/deploy/helm/cogneva/files/cogneva.json" \
                  "$tree/deploy/k3s/observability/manifests" "$@" 2>&1
}

empty_tree() { seed "$1"; printf '' | json_rules "$1"; rm -f "$1"/deploy/k3s/observability/manifests/*.yaml; }

# 1. 有规则但没有一条是比值规则：绿，且汇总行说得出「一条比值规则都没判」
t="$work/empty"; seed "$t"
yaml_rule "$t" Plain 'max(node_inodes_pct) > 85'
out="$(run "$t")"; ck_rc "1. 没有比值规则 ⇒ 0" 0 "$?"
ck_has "1b. 汇总行把 0 条判过的也印出来" "$out" "ratio_rules=0"

# 1c. 两套库各自读到零条 ⇒ 3：零条与「全都合规」在汇总行上同形（rules=0 ... violations=0）
t="$work/nothing"; empty_tree "$t"
out="$(run "$t")"; ck_rc "1c. 库读到零条 ⇒ 3" 3 "$?"
ck_has "1d. 说清是没读到规则" "$out" "一条规则都没有读到"

# 2. 速率 + 倍数 + 下限 ⇒ 绿
t="$work/ok"; seed "$t"
yaml_rule "$t" RateAccel 'deriv(x_pct[6h]) * 86400 > 2 * y_rate and deriv(x_pct[6h]) * 86400 > 0.1'
out="$(run "$t")"; ck_rc "2. 有下限 ⇒ 0" 0 "$?"
ck_has "2b. 它确实被判了，不是被略过" "$out" "ratio_rules=1 with_floor=1"

# 3. 同样的形状，没有下限 ⇒ 红并点名
t="$work/bad"; seed "$t"
yaml_rule "$t" RateAccelNoFloor 'deriv(x_pct[6h]) * 86400 > 2 * y_rate'
out="$(run "$t")"; ck_rc "3. 没有下限 ⇒ 1" 1 "$?"
ck_has "3b. 判词点名是哪条规则" "$out" "RateAccelNoFloor"
ck_has "3c. 判词说清缺的是什么" "$out" "no floor"
ck_has "3d. 汇总行计到 1 条违规" "$out" "violations=1"

# 4. `>= 0` 也算下限
t="$work/zerofloor"; seed "$t"
yaml_rule "$t" RateAccelZero 'deriv(x_pct[6h]) * 86400 > 2 * y_rate and deriv(x_pct[6h]) * 86400 >= 0'
out="$(run "$t")"; ck_rc "4. 下限写成 >= 0 也算 ⇒ 0" 0 "$?"

# 5. 年龄跟自己的周期倍数比 ⇒ 不判（周期本身就是下限）
t="$work/age"; seed "$t"
yaml_rule "$t" StaleAge 'time() - last_seen_seconds > 6 * scan_period_seconds'
out="$(run "$t")"; ck_rc "5. 年龄 vs 周期 ⇒ 0（不判）" 0 "$?"
ck_hasnt "5b. 它不该被算进比值规则" "$out" "ratio_rules=1"

# 6. 速率跟常数比 ⇒ 不判：没有比值这回事
t="$work/constonly"; seed "$t"
yaml_rule "$t" RateVsConstant 'deriv(x_pct[6h]) * 86400 > 0.5'
out="$(run "$t")"; ck_rc "6. 速率 vs 常数 ⇒ 0（不判）" 0 "$?"
ck_has "6b. 它在 unjudged 里被数出来" "$out" "CENSUS unjudged"

# 7. 进程内规则库也在判（只在清单里判等于一半的门）
t="$work/jsonlib"; seed "$t"
printf 'JsonRateNoFloor|deriv(y_pct[6h]) * 86400 > 3 * z_rate\n' | json_rules "$t"
out="$(run "$t")"; ck_rc "7. 进程内规则库同样判 ⇒ 1" 1 "$?"
ck_has "7b. 点名的是 json 库那条规则" "$out" "JsonRateNoFloor"

# 8. `rate()` 与 `irate()` 也算速率形态
t="$work/rateshape"; seed "$t"
yaml_rule "$t" IrateNoFloor 'irate(x_pct[5m]) > 2 * y_rate'
out="$(run "$t")"; ck_rc "8. irate 同属速率形态 ⇒ 1" 1 "$?"

# 9. 报而不判：--report 印出违规仍退 0
t="$work/report"; seed "$t"
yaml_rule "$t" RateAccelNoFloor 'deriv(x_pct[6h]) * 86400 > 2 * y_rate'
out="$(run "$t" --report)"; ck_rc "9. --report ⇒ 0" 0 "$?"
ck_has "9b. 但违规照样印出来" "$out" "RateAccelNoFloor"

# 10. 读不到规则库 ⇒ 3，不是 0
t="$work/unreadable"; seed "$t"; printf '' | json_rules "$t"
rm -rf "$t/deploy/k3s/observability/manifests"
out="$(run "$t")"; ck_rc "10. 清单目录不在 ⇒ 3（不是 0）" 3 "$?"
ck_has "10b. 说清「什么都没判」" "$out" "judged nothing"

# 11. 真树：这一族规则今天带着下限，门在真数据上是绿的
out="$(cd "$repo" && python3 deploy/scripts/check-alert-rule-ratio-floor.py 2>&1)"; ck_rc "11. 真树 ⇒ 0" 0 "$?"
ck_has "11b. 真树至少判到一条（否则这扇门是空转的）" "$out" "with_floor=1"

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
