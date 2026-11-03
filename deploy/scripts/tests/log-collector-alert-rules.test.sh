#!/usr/bin/env bash
# 节点日志采集器四条告警的**夹具自己**能不能红。
#
# 判据与证明是同两件事：`28-prometheusrule-log-collector.test.yaml` 判「规则会不会响」，
# 这里判那份夹具是不是一枚橡皮图章。把清单改坏五种方式，每一种都必须让单测红，而且
# 红在它该红的那一格：
#   1. 就绪数与应调度数的比较 `<` 改成 `<=`：两者相等（干净的负对照）必须开始响；
#   2. 删掉 `Absent` 挂的 `count(kube_pod_info{loki}) > 0`：`BACKENDS=0` 那一格必须有告警；
#   3. 跟踪文件数 `== 0` 改成 `>= 0`：只要有这条序列就响，跟着 42 个文件的那格必须红；
#   4. `Blind` 的 `for: 30m` 改成 `for: 30y`：永远不亮，够不着那一格必须红；
#   5. 丢行的 `> 0` 改成 `>= 0`：计数器平着走的那格必须开始响。
#
# 跑法：bash deploy/scripts/tests/log-collector-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/28-prometheusrule-log-collector.yaml"
case_file="${repo}/deploy/k3s/observability/tests/28-prometheusrule-log-collector.test.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 夹具改的是**清单的副本**，用例文件不动：规则只有一份，测试读的就是它。
run() { RULES_MANIFEST="$1" RULES_CASE="$case_file" bash "$runner" >"${work}/out" 2>&1; rc=$?; }

run "$manifest"
case "$rc" in
    0) ok "真文件：单测绿" ;;
    3) bad "真文件：拿不到 promtool（看不了，不是一致）" "0" "3；$(tail -1 "${work}/out")" ;;
    *) bad "真文件：单测应为绿" "0" "$rc；$(grep -m1 -E 'FAILED|error' "${work}/out")" ;;
esac
if [ "$rc" -eq 3 ]; then
    printf '（promtool 拿不到，后面几格改坏清单的用例都无从判——总数按 1 格算）\n'
    printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
    exit "$fail"
fi

# 1) `<` -> `<=`：相等时也开始响，干净的负对照那一格必须红。
python3 -I - "$manifest" "${work}/m1.yaml" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
text = open(src, encoding="utf-8").read()
old = "            <\n"
assert old in text, "清单里没有独立成行的 `<`"
open(dst, "w", encoding="utf-8").write(text.replace(old, "            <=\n", 1))
PY
run "${work}/m1.yaml"
[ "$rc" -eq 1 ] && ok "比较写成 <=：单测红（相等不再算「有节点没起来」）" \
    || bad "比较写成 <= 应红" "1" "$rc"

# 2) 删掉 Absent 挂的 Loki 条件：`BACKENDS=0` 那一格必须红。
python3 -I - "$manifest" "${work}/m2.yaml" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
text = open(src, encoding="utf-8").read()
block = ("            and on()\n"
         "            count(kube_pod_info{namespace=\"monitoring\",pod=~\"loki-.*\"}) > 0\n")
assert block in text, "清单里找不到 Absent 的 Loki 条件"
open(dst, "w", encoding="utf-8").write(text.replace(block, "", 1))
PY
run "${work}/m2.yaml"
[ "$rc" -eq 1 ] && ok "删掉 Loki 条件：单测红（没有 Loki 的安装不许报缺席）" \
    || bad "删掉 Loki 条件应红" "1" "$rc"

# 3) `== 0` -> `>= 0`：跟着 42 个文件的那格必须红。
python3 -I - "$manifest" "${work}/m3.yaml" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
text = open(src, encoding="utf-8").read()
old = 'loki_source_file_files_active_total{component_id="loki.source.file.pods"}) == 0'
assert old in text, "清单里找不到跟踪文件数的判据"
open(dst, "w", encoding="utf-8").write(text.replace(old, old.replace("== 0", ">= 0"), 1))
PY
run "${work}/m3.yaml"
[ "$rc" -eq 1 ] && ok "跟踪文件数写成 >= 0：单测红（有序列就算有东西可跟）" \
    || bad "跟踪文件数写成 >= 0 应红" "1" "$rc"

# 4) `for: 30m` -> `for: 30y`：够不着，必须红。
sed 's/^          for: 30m$/          for: 30y/' "$manifest" >"${work}/m4.yaml"
grep -q 'for: 30y' "${work}/m4.yaml" || bad "改坏没落地" "含 for: 30y 的清单" "原样"
run "${work}/m4.yaml"
[ "$rc" -eq 1 ] && ok "for 写成 30y：单测红（抓得住「永远不会亮」）" \
    || bad "for 写成 30y 应红" "1" "$rc"

# 5) 丢行 `> 0` -> `>= 0`：平着走的计数器那格必须红。
python3 -I - "$manifest" "${work}/m5.yaml" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
text = open(src, encoding="utf-8").read()
old = 'loki_write_dropped_entries_total{component_id="loki.write.default"}[30m])) > 0'
assert old in text, "清单里找不到丢行的判据"
open(dst, "w", encoding="utf-8").write(text.replace(old, old.replace("> 0", ">= 0"), 1))
PY
run "${work}/m5.yaml"
[ "$rc" -eq 1 ] && ok "丢行写成 >= 0：单测红（没有增量不许亮）" \
    || bad "丢行写成 >= 0 应红" "1" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
