#!/usr/bin/env bash
# principle: feedback_a_join_keeps_every_label_nobody_named
# principle: feedback_absent_over_time_synthesizes_the_selector_equality_matchers
# 进程内那一库规则（部署配置里的 `observability.infra_watch.rules`）的「会不会响」夹具
# **自己**能不能红。
#
# 为什么要有这一层：出生占用那份 promtool 单测判的是规则会不会响，而它自己也可能是一枚
# 橡皮图章。这里把那一库的副本改坏五次，每次都必须让它红在**该红的那一格**：
#   1. 界 1.05 抬到 1.5：firing 那一格必须红——否则一条永远不响的规则也绿；
#   2. `for_seconds` 改成 900000000：firing 那一格必须红——否则「等一亿秒」也绿；
#   3. 删掉铺满腿：序列比窗口年轻那一格必须红——否则「刚起的 Pod 凭一条采样证明三天」也绿；
#   4. 删掉默认的 1 GiB 地板：小阶段那一格必须红——否则 300→400 MiB 的噪声也会响；
#   4d. 把折叠键从 `(phase, job)` 退回 `(phase)`：跨工作负载那一格必须红——否则应用会被
#       拿去和同一批阶段名的进化 Pod 比，报出 47 倍而那是两个进程；
#   5. 夹具点名一条库里没有的规则：必须非零，而不是「没取到规则所以没判」；
#   5b. 夹具目录里每一份 `cogneva-json-*.test.yaml` 都必须真跑绿——覆盖门只按夹具头
#       记「已证明」，一份红着的或从没被执行过的夹具在它眼里与绿的完全一样；
#   5c. 删掉告警出口那条规则的 `unless` 腿：那一格必须红；
#   5d. 把一条长窗口规则退回 `increase()`：值夹具必须红——它判的是发货的那份表达式，
#       不是抄进夹具的一份。
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
mutate() { # $1=输出文件  $2=python 片段（拿到 rules 里那条规则的 dict，改它）  ${3:-}=改哪条（默认 $rule）
    python3 - "$config" "$1" "${3:-$rule}" "$2" <<'PY'
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

# 4) 删掉默认的 1 GiB 地板：小阶段那一格必须红。
mutate "${work}/c4.json" 'rule["promql"] = rule["promql"].replace(" and (max by (phase, job) (cogneva_boot_rss_bytes) > 1073741824)", "")'
run "${work}/c4.json"
[ "$rc" -eq 1 ] && ok "删掉 1 GiB 地板：单测红（小阶段的比值重新算数）" \
    || bad "删掉 1 GiB 地板应红" "1" "$rc"

# 4d) 把折叠键从 `(phase, job)` 退回 `(phase)`：跨工作负载那一格必须红。
# 同一个序列名有两个发布者——应用与进化 Pod 都打这七个阶段名，量级差着四十多倍。只按
# `phase` 折叠时，三天窗口里的最小值会落到当天恰好在线的那一个进程身上，规则就报「应用涨了
# 47 倍」，而它比的是两个不同进程。这一格证明那个夹具真的在为这件事站岗：退回旧折叠键，
# 应用那两代一样大的形状必须当场亮起来。
mutate "${work}/c4d.json" 'rule["promql"] = rule["promql"].replace("max by (phase, job)", "max by (phase)")'
run "${work}/c4d.json"
[ "$rc" -eq 1 ] && ok "退回只按 phase 折叠：单测红（跨工作负载的比值重新算数）" \
    || bad "退回只按 phase 折叠应红" "1" "$rc"

# 4b) 只改 `condition` 的阈值（表达式一个字节不动）：单测必须红。
# 这一格的存在本身是那条判据：不把 condition 折进表达式，Prometheus 看到的是「表达式返回
# 元素就点火」，阈值改成十亿也照样亮，于是「值到没到界」这一半没有任何一格在证明。
mutate "${work}/c5.json" 'rule["condition"] = {"greater_than": 1e9}'
run "${work}/c5.json"
[ "$rc" -eq 1 ] && ok "只把阈值抬到十亿：单测红（条件真的被跑到了）" \
    || bad "只改阈值应红" "1" "$rc"

# 4c) condition 换成一个不认识的算子：必须非零，不许被当作「没条件」跳过。
mutate "${work}/c6.json" 'rule["condition"] = {"greater_than_or_whatever": 0.0}'
run "${work}/c6.json"
[ "$rc" -ne 0 ] && ok "不认识的 condition 算子：非零（不认识的算子不许当成没条件）" \
    || bad "不认识的算子应非零" "非零" "0"

# 5) 夹具点名一条库里没有的规则：必须非零——「取不到规则」不许读成「没判出问题」。
sed "s/^# rule: ${rule}\$/# rule: ${rule}_not_in_the_library/" "$case_file" >"${work}/missing.test.yaml"
run "$config" "${work}/missing.test.yaml"
[ "$rc" -ne 0 ] && ok "夹具点名的规则不在库里：非零（没取到 ≠ 没问题）" \
    || bad "夹具点名库里没有的规则应非零" "非零" "0"

# 5b) 夹具目录里每一份 `cogneva-json-*.test.yaml` 都必须**真跑绿**。
# 覆盖门（check-alert-rule-firing-coverage.sh）按夹具头把规则记成「已证明」，但它不跑夹具：
# 一份红着的、或从来没被任何地方执行过的夹具，在它眼里与一份绿的完全一样。这一格把
# 「点过名」补成「跑过且绿」，否则覆盖数会以夹具的名义涨上去而证明是空的。
for f in "${repo}"/deploy/k3s/observability/tests/cogneva-json-*.test.yaml; do
    [ -e "$f" ] || { bad "夹具目录里一份 cogneva-json-*.test.yaml 都没有" "至少一份" "0"; continue; }
    run "$config" "$f"
    [ "$rc" -eq 0 ] && ok "夹具真跑绿：$(basename "$f")" \
        || bad "夹具应跑绿：$(basename "$f")" "0" "$rc；$(tail -3 "${work}/out" | tr '\n' ' ')"
done

# 5c) 告警出口那条规则的 `unless` 腿是承重的：删掉它，「watcher 自己在跑」那格必须红。
# 这一格证明上面那份新夹具不是橡皮图章——它真的会在规则被改坏时说话。
mutate "${work}/c7.json" 'rule["promql"] = rule["promql"].split(" unless")[0]' "alert_batch_reached_no_outlet"
run "${work}/c7.json" "${repo}/deploy/k3s/observability/tests/cogneva-json-alert-pipeline.test.yaml"
[ "$rc" -eq 1 ] && ok "删掉 unless 腿：夹具红（watcher 在跑时不该报「没有出口」）" \
    || bad "删掉 unless 腿应红" "1" "$rc"

# 5d) 把一条长窗口规则退回 `increase()`：值夹具必须红。
# 这一格证明 `cogneva-json-store-counter-window.test.yaml` 判的是**发货的那份表达式**，
# 不是夹具里抄的一份：占位 `'__rule_promql:<名字>'` 由跑者换掉，抄进夹具的那份在规则被
# 改回去时一声不响。判据是数——同一份序列（41、40、41）在包络读法下是 1，在旧读法下是
# 71.75，而 `> 0` 的判词两种读法都亮，只有值断言分得开。
mutate "${work}/c9.json" 'rule["promql"] = "max(increase(cogneva_change_tier_total[24h])) unless max(increase(cogneva_change_tier_total{tier=\"real_gate\"}[24h])) > 0"' "change_tier_routing_inert"
run "${work}/c9.json" "${repo}/deploy/k3s/observability/tests/cogneva-json-store-counter-window.test.yaml"
[ "$rc" -eq 1 ] && ok "长窗口规则退回 increase：值夹具红" \
    || bad "退回 increase 应红" "1" "$rc"

# 6) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool RULES_MANIFEST="$config" RULES_CASE="$case_file" \
    bash "$runner" >"${work}/out6" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
