#!/usr/bin/env bash
# 进程内那一库规则（部署配置里的 `observability.infra_watch.rules`）的「会不会响」夹具
# **自己**能不能红。
#
# 为什么要有这一层：出生占用那份 promtool 单测判的是规则会不会响，而它自己也可能是一枚
# 橡皮图章。这里把那一库的副本改坏五次，每次都必须让它红在**该红的那一格**：
#   1. 界 1.05 抬到 1.5：firing 那一格必须红——否则一条永远不响的规则也绿；
#   2. `for_seconds` 改成 900000000：firing 那一格必须红——否则「等一亿秒」也绿；
#   3. 删掉铺满腿：序列比窗口年轻那一格必须红——否则「刚起的 Pod 凭一条采样证明三天」也绿；
#   4. 删掉 1 GiB 地板：小阶段那一格必须红——否则 300→400 MiB 的噪声也会响；
#   5. 夹具点名一条库里没有的规则：必须非零，而不是「没取到规则所以没判」。
# 另有两格证明它**不会**白红：真文件必须绿；拿不到 promtool 必须退 3（看不了 ≠ 一致）。
#
# 跑法：bash deploy/scripts/tests/cogneva-json-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
case_file="${repo}/deploy/k3s/observability/tests/cogneva-json-boot-phase-footprint.test.yaml"
rule="boot_phase_footprint_grew"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { RULES_MANIFEST="$1" RULES_CASE="${2:-$case_file}" bash "$runner" >"${work}/out" 2>&1; rc=$?; }

# 把那一库的副本按一段 python 改坏。改的是**部署配置的副本**，规则名不变。
mutate() { # $1=输出文件  $2=python 片段（拿到 rules 里那条规则的 dict，改它）
    python3 - "$config" "$1" "$rule" "$2" <<'PY'
import json
import sys

src, out, name, snippet = sys.argv[1:5]
config = json.load(open(src, encoding="utf-8"))
rules = config["observability"]["infra_watch"]["rules"]
rule = next(r for r in rules if r["name"] == name)
exec(snippet)
json.dump(config, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
PY
}

# 0) 真文件必须绿。拿不到 promtool 时退 3，按「看不了」记红，不冒充通过。
run "$config"
case "$rc" in
    0) ok "真文件：单测绿" ;;
    3) bad "真文件：拿不到 promtool（看不了，不是一致）" "0" "3；$(tail -1 "${work}/out")" ;;
    *) bad "真文件：单测应为绿" "0" "$rc；$(tail -3 "${work}/out" | tr '\n' ' ')" ;;
esac
if [ "$rc" -eq 3 ]; then
    printf '（promtool 拿不到，后面几格改坏配置的用例都无从判——总数按 1 格算）\n'
    printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
    exit "$fail"
fi

# 1) 界抬到 1.5：永远不响，必须红。
mutate "${work}/c1.json" 'rule["promql"] = rule["promql"].replace("> 1.05)", "> 1.5)")'
run "${work}/c1.json"
[ "$rc" -eq 1 ] && ok "界抬到 1.5：单测红（抓得住「永远不会响」）" \
    || bad "界抬到 1.5 应红" "1" "$rc"

# 2) for_seconds 改成一亿秒：永远停在 pending，必须红。
mutate "${work}/c2.json" 'rule["for_seconds"] = 900000000'
run "${work}/c2.json"
[ "$rc" -eq 1 ] && ok "for_seconds 改成一亿秒：单测红（抓得住「永远 pending」）" \
    || bad "for_seconds 改成一亿秒应红" "1" "$rc"

# 3) 删掉铺满腿：年轻序列那一格必须红。
mutate "${work}/c3.json" 'rule["promql"] = rule["promql"].split(" and (count_over_time")[0]'
run "${work}/c3.json"
[ "$rc" -eq 1 ] && ok "删掉铺满腿：单测红（年轻序列凭一条采样不再是三天）" \
    || bad "删掉铺满腿应红" "1" "$rc"

# 4) 删掉 1 GiB 地板：小阶段那一格必须红。
mutate "${work}/c4.json" 'rule["promql"] = rule["promql"].replace(" and (max by (phase) (cogneva_boot_rss_bytes) > 1073741824)", "")'
run "${work}/c4.json"
[ "$rc" -eq 1 ] && ok "删掉 1 GiB 地板：单测红（小阶段的比值重新算数）" \
    || bad "删掉 1 GiB 地板应红" "1" "$rc"

# 5) 夹具点名一条库里没有的规则：必须非零——「取不到规则」不许读成「没判出问题」。
sed "s/^# rule: ${rule}\$/# rule: ${rule}_not_in_the_library/" "$case_file" >"${work}/missing.test.yaml"
run "$config" "${work}/missing.test.yaml"
[ "$rc" -ne 0 ] && ok "夹具点名的规则不在库里：非零（没取到 ≠ 没问题）" \
    || bad "夹具点名库里没有的规则应非零" "非零" "0"

# 6) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool RULES_MANIFEST="$config" RULES_CASE="$case_file" \
    bash "$runner" >"${work}/out6" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
