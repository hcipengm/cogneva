#!/usr/bin/env bash
# principle: feedback_a_threshold_must_be_derived_from_the_thing_it_measures
# check-alert-rule-derived-bounds.py 的定向测试。
#
# 被测的是「读余量的规则只能拿余量跟 0 比」这条判据。治的病不是某一条规则写错了，
# 是**这个形状可以被合法地写回来**：一条规则的界是一个写死的提前期（「七天」），
# 而提前期是策略输入——盘换快了它就该变，写在规则里没人看得见它该变。
#
# 所以格子分两半：一半钉「什么样的写法必须红」，一半钉「不该红的别红」——
# 后者更重要，一扇把正常写法判红的门会被绕过，绕过之后连它本来守的那条也一起没了。
#   - 规则读窗口内的**速率**、跟一个常数比（`disk_usage_pct > 85`、`... > 0`）⇒ 绿；
#     速率不是提前期，它跟常数比不产生「界藏在规则里」这件事。
#   - 读 runway 家族（`_days_to_` / `_headroom_` / `_days_left`）跟 0 比 ⇒ 绿，0 的各种写法都算；
#   - 读 runway 家族跟别的数比 ⇒ 红，判词要点名是哪条规则、读了哪个序列、比了几；
#   - runway 家族没有任何数值比较 ⇒ 不判（守卫子句），但要**计数上报**，
#     因为「没判」与「判过且通过」在汇总行上必须分开；
#   - 读不到规则库 ⇒ 3，不是 0（同上：没读到 ≠ 干净）。
# 另有一格确认两套库**都**在判：只读进程内规则、漏掉清单，等于一半的门。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-derived-bounds.py"
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

seed() { # $1=用例目录：两套库都在
  mkdir -p "$1/deploy/k3s/observability/manifests" "$1/deploy/helm/cogneva/files"
}
# 进程内规则：stdin 每行 name|promql|condkey|num
json_rules() { # $1=树，stdin 每行一条。生成器单独落盘：`python3 - <<PY` 会吃掉调用方的管道
  cat >"$work/.json-maker.py" <<'PY'
import json, sys
rules = []
for line in sys.stdin.read().splitlines():
    if not line.strip():
        continue
    name, promql, key, num = line.split("|")
    rules.append({"name": name, "promql": promql, "condition": {key: float(num)},
                  "severity": "warning", "summary": name})
json.dump({"observability": {"infra_watch": {"rules": rules}}},
          open(sys.argv[1], "w", encoding="utf-8"), indent=2)
PY
  python3 "$work/.json-maker.py" "$1/deploy/helm/cogneva/files/cogneva.json"
}
yaml_rule() { # $1=树 $2=告警名 $3=表达式（原样写进 expr:）
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

# 1. 没有 runway 规则：绿，且汇总行说得出「判了 0 条 runway」
t="$work/clean"; seed "$t"
printf 'disk_high|node_disk_usage_pct|greater_than|85\n' | json_rules "$t"
yaml_rule "$t" Inodes 'max(node_inodes_pct) > 85'
out="$(run "$t")"; ck_rc "1. 没有 runway 规则 ⇒ 0" 0 "$?"
ck_has "1. 汇总行点出 runway_rules=0" "$out" "runway_rules=0 judged=0 unjudged=0 violations=0"

# 2. 两套库都在读：汇总行里的规则数是两边之和
ck_has "2. 进程内规则与清单规则都数进来了" "$out" "rules=2 "

# 3. 进程内规则拿「到满还剩几天」跟写死的 7 比 ⇒ 红，点名规则、序列、那个数
t="$work/registry-lead"; seed "$t"
printf 'approaching|min(disk_trend_days_to_full_recent)|less_than|7\n' | json_rules "$t"
yaml_rule "$t" Unrelated 'max(node_inodes_pct) > 85'
out="$(run "$t")"; ck_rc "3. 拿提前期当界 ⇒ 1" 1 "$?"
ck_has "3. 点名是哪条规则" "$out" "approaching"
ck_has "3. 点名读的是哪个序列" "$out" "disk_trend_days_to_full_recent"
ck_has "3. 点名比的是几" "$out" "compares against 7"
ck_has "3. 判词给出修法方向（比余量）" "$out" "margin"

# 4. 同一条规则改成比余量 ⇒ 绿（修法确实修得上）
t="$work/registry-margin"; seed "$t"
printf 'approaching|min(disk_trend_days_to_full_recent_margin)|less_than|0\n' | json_rules "$t"
yaml_rule "$t" Unrelated 'max(node_inodes_pct) > 85'
out="$(run "$t")"; ck_rc "4. 比余量与 0 ⇒ 0" 0 "$?"
ck_has "4. 这一条被判过（不是被跳过）" "$out" "judged=1 unjudged=0"

# 5. 清单里拿「到触发线还剩几天」跟 1 比 ⇒ 红
t="$work/manifest-lead"; seed "$t"
printf 'disk_high|node_disk_usage_pct|greater_than|85\n' | json_rules "$t"
yaml_rule "$t" RunwayShort 'min by (mountpoint) (disk_trend_days_to_trigger) < 1'
out="$(run "$t")"; ck_rc "5. 清单里的写死提前期 ⇒ 1" 1 "$?"
ck_has "5. 点名清单里那条规则" "$out" "RunwayShort"
ck_has "5. 点名比的是 1" "$out" "compares against 1"

# 6. 清单里的余量写法 ⇒ 绿
t="$work/manifest-margin"; seed "$t"
printf 'disk_high|node_disk_usage_pct|greater_than|85\n' | json_rules "$t"
yaml_rule "$t" RunwayShort 'min by (mountpoint) (disk_trend_days_to_trigger_margin) < 0'
out="$(run "$t")"; ck_rc "6. 清单里比余量与 0 ⇒ 0" 0 "$?"

# 7. 同一族里的另一种名字（磨损余量 headroom，与整窗余量 days_left）也在判
t="$work/family"; seed "$t"
printf 'wear|min(cogneva_host_nvme_wear_headroom_pct)|less_than|5\n' | json_rules "$t"
yaml_rule "$t" LeftFew 'min by (mountpoint) (disk_trend_days_left) < 3'
out="$(run "$t")"; ck_rc "7. headroom / days_left 也在 runway 家族里 ⇒ 1" 1 "$?"
ck_has "7. 两条都点名" "$out" "wear"
ck_has "7. 另一条也点名" "$out" "LeftFew"

# 8. 0 的各种写法都算「比余量」：`> 0`、`>= 0.0`、`== 0`、`!= 0`
t="$work/zero-forms"; seed "$t"
printf 'z1|min(disk_trend_days_to_full_margin)|greater_than|0\n' | json_rules "$t"
yaml_rule "$t" Z2 'min by (mountpoint) (disk_trend_headroom_days) >= 0.0'
out="$(run "$t")"; ck_rc "8. 0 的各种写法都算 ⇒ 0" 0 "$?"
ck_has "8. 两条都被判过" "$out" "judged=2 unjudged=0"

# 9. runway 规则没有数值比较（守卫子句）⇒ 不判，但计数上报并点名
t="$work/unjudged"; seed "$t"
printf 'unknown|disk_trend_days_to_left|greater_than|0\n' | json_rules "$t"
yaml_rule "$t" Guard 'disk_trend_days_left unless on(source) disk_trend_window_seconds'
out="$(run "$t")"; ck_rc "9. 判不动的不拦（0），但要计数" 0 "$?"
ck_has "9. 汇总行说得出有一条没判" "$out" "unjudged=1"
ck_has "9. 点名那条没判的规则" "$out" "CENSUS unjudged"
ck_has "9. 点得出它读的是哪个序列" "$out" "disk_trend_days_left"

# 10. --report：一样印红，但不拦（普查模式）
t="$work/registry-lead"
out="$(run "$t" --report)"; ck_rc "10. --report 下有违规 ⇒ 0" 0 "$?"
ck_has "10. --report 仍然印红" "$out" "RED"

# 11. 清单目录不在 ⇒ 3，不是 0
t="$work/no-manifests"; mkdir -p "$t/deploy/helm/cogneva/files"
printf 'disk_high|node_disk_usage_pct|greater_than|85\n' | json_rules "$t"
out="$(run "$t")"; ck_rc "11. 清单目录不在 ⇒ 3" 3 "$?"
ck_has "11. 说清是哪一边读不到" "$out" "manifest directory is not there"

# 12. 清单解析不了 ⇒ 3，并点名那份文件
t="$work/bad-yaml"; seed "$t"
printf 'kind: PrometheusRule\nspec: [unclosed\n' >"$t/deploy/k3s/observability/manifests/10-bad.yaml"
printf 'disk_high|node_disk_usage_pct|greater_than|85\n' | json_rules "$t"
out="$(run "$t")"; ck_rc "12. 解析不了 ⇒ 3" 3 "$?"
ck_has "12. 点名是哪份文件" "$out" "cannot parse 10-bad.yaml"

# 13. 进程内规则文件不在 ⇒ 3
t="$work/no-json"; seed "$t"
yaml_rule "$t" Unrelated 'max(node_inodes_pct) > 85'
out="$(run "$t")"; ck_rc "13. 进程内规则文件不在 ⇒ 3" 3 "$?"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" = 0 ]
