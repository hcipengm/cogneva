#!/usr/bin/env bash
# check-alert-rule-duplicate-bounds.py 的定向测试。
#
# 被测的是一个**只报不拦**的普查：两套规则库（集群内 PrometheusRule 清单、应用进程内
# `infra_watch` 规则）里，有没有同一个界被两条规则各守一遍。
#
# 这份测试的重点不是「能不能找出重复」，是「**不把正常写法报成重复**」——普查表一旦
# 噪声压过信号就没人读了，而没人读的普查表和不存在是同一种东西。所以大部分格子钉的是
# **不该报**的形状，每格给一份最小夹具（两套库各一份文件），改一个字节就能验证：
#   - 同一条表达式 + 同一个阈值 ⇒ 报（这是真副本）；
#   - 同一条表达式 + 不同阈值 ⇒ 不报（分级告警是正常写法，warning 80 / critical 90）；
#   - 同一个受试序列 + 同一个数 + 不同表达式 ⇒ 报（同一界两种问法，最会漂的一对）；
#   - 同一个受试序列 + 不同数 / 不同比较符 ⇒ 不报；
#   - 同一个序列名 + 不同标签值 ⇒ 不报（`{outcome="failed"}` 与 `{outcome="succeeded"}`
#     是两个对象；这一格挡的是把标签抹平后「一家子全配对」的写法）；
#   - 一条表达式读两个序列 ⇒ 不进subject 层（组合不是对象，组合之间撞上不说明任何事）。
# 另外三条读不到的路各自一格：目录不在、YAML 解析不了、规则表为空——它们必须退 3，
# 因为「没读到」与「没有重复」在报告上长得一模一样，而后者是最坏的假答案。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-duplicate-bounds.py"
[ -f "$gate" ] || { echo "FAIL: 被測脚本不在：$gate" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1（期望退出码 $2，实得 $3）"; fi; }
ck_has() { if printf '%s\n' "$2" | grep -qF -- "$3"; then ok "$1"; else
  bad "$1（期望含 $3）"; printf '%s\n' "$2" | sed 's/^/        实得: /' | head -10 >&2; fi; }
ck_hasnt() { if printf '%s\n' "$2" | grep -qF -- "$3"; then
  bad "$1（不该含 $3）"; else ok "$1"; fi; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# 一份最小树：manifest 目录 + 进程内规则文件，两边都换得动。
seed() { # $1=用例目录
  mkdir -p "$1/deploy/k3s/observability/manifests" "$1/deploy/helm/cogneva/files"
}
# 清单里的一条规则，表达式原样给。
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
          expr: |
            $(printf '%s' "$3" | sed '2,$s/^/            /')
YAML
}
# 进程内规则：表达式与 condition 分开写。
json_rules() { # $1=树，之后从 stdin 读 name|promql|op|num
  cat >"$work/.json-maker.py" <<'PY'
import json, sys
ops = {">": "greater_than", ">=": "greater_than_or_equal", "<": "less_than",
       "<=": "less_than_or_equal", "==": "equal_to"}
rules = []
for line in sys.stdin.read().splitlines():
    if not line.strip():
        continue
    name, promql, op, num = line.split("|")
    rules.append({"name": name, "promql": promql,
                  "condition": {ops[op]: float(num)},
                  "severity": "warning", "summary": name})
json.dump({"observability": {"infra_watch": {"rules": rules}}},
          open(sys.argv[1], "w", encoding="utf-8"))
PY
  python3 "$work/.json-maker.py" "$1/deploy/helm/cogneva/files/cogneva.json"
}
run() { # $1=树 [args...]
  local tree="$1"; shift
  COGNEVA_DUPLICATE_BOUNDS_REPO="$tree" python3 "$gate" "$@" 2>&1
}

# 1. 什么都不重复：报「没有」，且 --strict 也退 0（普查是常态绿）
t="$work/clean"; seed "$t"
yaml_rule "$t" CleanA 'alpha_state > 0'
printf 'clean_b|beta_state|>|0\n' | json_rules "$t"
out="$(run "$t")"; ck_rc "1. 没有重复 ⇒ 0" 0 "$?"
ck_has "1. 说清没有" "$out" "no bound is carried by two rules"
out="$(run "$t" --strict)"; ck_rc "1. --strict 在没重复时也退 0" 0 "$?"

# 2. 同一条表达式、同一个阈值，两套库各写一遍 ⇒ 真副本，报出来
t="$work/copy"; seed "$t"
yaml_rule "$t" CopyA 'gamma_state > 3'
printf 'copy_b|gamma_state|>|3\n' | json_rules "$t"
out="$(run "$t")"; ck_rc "2. 真副本 ⇒ 报告模式仍退 0" 0 "$?"
ck_has "2. 报成 identical_bound" "$out" "identical_bound"
ck_has "2. 两条规则都点名" "$out" "CopyA@manifests/10-CopyA.yaml, copy_b@chart/cogneva.json"
out="$(run "$t" --strict)"; ck_rc "2. --strict 下有副本 ⇒ 1" 1 "$?"

# 3. 同一条表达式、不同阈值：分级告警是**正常写法**，不许报
t="$work/tiers"; seed "$t"
printf 'disk_high|node_disk_state|>|80\ndisk_critical|node_disk_state|>|90\n' | json_rules "$t"
yaml_rule "$t" Unrelated 'other_state > 0'
out="$(run "$t")"
ck_hasnt "3. 同表达式不同阈值（分级告警）不报" "$out" "identical_bound"
ck_hasnt "3. 分级告警也不该进受试层（阈值不同）" "$out" "node_disk_state"

# 4. 同一受试序列、同一个数、同一段时间、同一组比较，只是包法不同 ⇒ 同一界写了两遍，报
t="$work/same-subject"; seed "$t"
yaml_rule "$t" SeenMax 'max by (node) (same_state) > 0'
printf 'seen_zero|1 - max by (node) (same_state)|>|0\n' | json_rules "$t"
out="$(run "$t")"
ck_has "4. 同一受试序列同一个数 ⇒ 报" "$out" "same_subject_same_threshold"
ck_has "4. 点名受试序列" "$out" "object=same_state"
ck_has "4. 两条规则都点名" "$out" "SeenMax@manifests/10-SeenMax.yaml"

# 5. 同一受试序列、不同数 ⇒ 不报（两个界不是同一个界）
t="$work/diff-num"; seed "$t"
yaml_rule "$t" N1 'num_state > 1'
printf 'n2|num_state|>|2\n' | json_rules "$t"
out="$(run "$t")"
ck_hasnt "5. 同一个序列不同阈值不报" "$out" "num_state"

# 6. 同一受试序列、不同比较符 ⇒ 不报（两个方向各自要人看）
t="$work/diff-op"; seed "$t"
yaml_rule "$t" OpA 'op_state < 1'
printf 'op_b|op_state|>|1\n' | json_rules "$t"
out="$(run "$t")"
ck_hasnt "6. 不同比较符不报" "$out" "op_state"

# 7. 同一个序列名、不同标签值 ⇒ 不报。抹平标签的写法会让一家子互配，报告立刻变噪声。
t="$work/labels"; seed "$t"
yaml_rule "$t" LabFailed 'readings_state{outcome="failed"} > 0'
printf 'label_ok|readings_state{outcome="succeeded"}|>|0\n' | json_rules "$t"
out="$(run "$t")"
ck_hasnt "7. 标签值不同的两个对象不报" "$out" "readings_state"

# 8. 进程内规则的条件盖住两个序列 ⇒ 不进受试层：它的条件不是关于某一个对象的
t="$work/two-subjects"; seed "$t"
yaml_rule "$t" TwoA 'left_state > 0 or right_state > 0'
printf 'two_b|left_state > 0 or other_state > 0\n' | sed 's/$/|>|0/' | json_rules "$t"
out="$(run "$t")"
ck_hasnt "8. 条件盖住两个对象的规则不进受试层" "$out" "same_subject_same_threshold"

# 9. 守卫子句：两条规则的**整个表达式**不同，但都拿同一个序列跟同一个数比。
#    这属于受试层要看的形状（可能只是各守各的，也可能真是同一个界写了两遍），
#    所以报出来给人判；但它**不是**整条表达式相同，不该进副本层。
t="$work/guard"; seed "$t"
yaml_rule "$t" GuardA 'surface_measured == 1
  unless on(source) surface_period_seconds'
yaml_rule "$t" GuardB 'surface_aligned == 0
  and on(source) surface_measured == 1'
printf 'guard_c|unrelated_state|>|0\n' | json_rules "$t"
out="$(run "$t")"
ck_hasnt "9. 守卫子句相同不等于整条表达式相同（不报副本）" "$out" "identical_bound"
# 也不进受试层：`surface_measured == 1` 只是两条规则的**守卫**，各自要比的是
# 「没有周期」与「aligned 掉了」。判据是**比较的集合**——两条规则的比较集不同，
# 就不是同一个界；只看受试序列会把「共用一句守卫」误报成「同一个界两个信使」。
ck_hasnt "9. 共用一句守卫不算同一个界（比较集不同）" "$out" "same_subject_same_threshold"

# 9b. **窗口属于界**：同一个序列、同一个数、同一组比较，但一个是「连续 15 分钟」
#     另一个是「30 分钟里每一拍都大于 0」。这是早期预警与确认两档，不是副本——
#     与「80 / 90 两级阈值」同一种正常写法。判据落在 `for:` 与表达式里的区间选择器上。
t="$work/windows"; seed "$t"
yaml_rule "$t" EarlyWarn 'stale_state > 0'
printf 'confirmed|min_over_time(stale_state[30m])|>|0\n' | json_rules "$t"
# 给清单那条补上 for: 15m
python3 - "$t/deploy/k3s/observability/manifests/10-EarlyWarn.yaml" <<'PY'
import sys
p = sys.argv[1]
s = open(p, encoding="utf-8").read().replace("          expr:", "          for: 15m\n          expr:")
open(p, "w", encoding="utf-8").write(s)
PY
out="$(run "$t")"
ck_hasnt "9b. 窗口不同的同一序列不报（早期预警 vs 确认）" "$out" "same_subject_same_threshold"

# 9c. 比较集不同 ⇒ 不是同一个界：`> 0` 与 `> 0` 之外还多比了一个数
t="$work/cmp-set"; seed "$t"
yaml_rule "$t" CmpA 'alpha_state > 0'
printf 'cmp_b|alpha_state and (beta_state > 0)|>|0\n' | json_rules "$t"
out="$(run "$t")"
ck_hasnt "9c. 比较集不同的同一受试序列不报" "$out" "same_subject_same_threshold"

# 10. 清单目录不在 ⇒ 读不到（3），不是「没有重复」
t="$work/no-manifests"; mkdir -p "$t/deploy/helm/cogneva/files"
out="$(run "$t")"; ck_rc "10. 清单目录不在 ⇒ 3" 3 "$?"
ck_has "10. 说清是哪一边读不到" "$out" "manifest directory is not there"

# 11. YAML 解析不了 ⇒ 读不到（3）
t="$work/bad-yaml"; seed "$t"
printf 'kind: PrometheusRule\nspec: [unclosed\n' >"$t/deploy/k3s/observability/manifests/10-bad.yaml"
printf 'x|y|>|0\n' | json_rules "$t"
out="$(run "$t")"; ck_rc "11. YAML 解析不了 ⇒ 3" 3 "$?"
ck_has "11. 点名是哪份文件" "$out" "cannot parse 10-bad.yaml"

# 12. 进程内规则表为空 ⇒ 读不到（3）：「一张空表」与「一张没读到的表」必须分开
t="$work/empty-json"; seed "$t"
yaml_rule "$t" AnyA 'any_state > 0'
printf '{"observability": {"infra_watch": {"rules": []}}}' >"$t/deploy/helm/cogneva/files/cogneva.json"
out="$(run "$t")"; ck_rc "12. 规则表为空 ⇒ 3" 3 "$?"

# 13. --json 给人之外的东西读：字段要能直接取
t="$work/copy"
out="$(run "$t" --json)"
printf '%s' "$out" | python3 -c '
import json, sys
d = json.load(sys.stdin)
assert d["rules"] == 2, d["rules"]
assert len(d["pairs"]) == 1, d["pairs"]
p = d["pairs"][0]
assert p["tier"] == "identical_bound", p["tier"]
assert [r["name"] for r in p["rules"]] == ["CopyA", "copy_b"], p["rules"]
' 2>/dev/null && ok "13. --json 印出条数与配对" || bad "13. --json 结构不对：$out"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" = 0 ]
