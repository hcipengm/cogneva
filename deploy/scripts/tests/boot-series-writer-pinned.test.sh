#!/usr/bin/env bash
# `check-boot-series-writer-pinned.sh` 的定向测试：这条门自己会不会红。
#
# 为什么要有这一层：门读的是表达式结构，而「结构被改坏」与「结构从来就没对过」在真树上
# 长得一样——真树现在绿。这里把树上的副本改坏五次，每次都必须红在**该红的那一格**：
#   1. 规则退回 `max by (phase)`：必须红（2026-10-29 的现网形状）；
#   2. 规则改成「选择器点名 job、折叠键不带 job」：必须绿（另一种合法写法，别把合法写法
#      判成违规，否则下一个人只会去改判据）；
#   3. 面板的 target 改成按 `phase` 折叠：必须红（面板那一侧也在射程里）；
#   4. manifest 目录不存在：必须退 3（看不了 ≠ 一致）；
#   5. 规则文件不存在：必须退 2。
# 另有一格自检：真树必须绿。
#
# 跑法：bash deploy/scripts/tests/boot-series-writer-pinned.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-boot-series-writer-pinned.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
manifests="${repo}/deploy/k3s/observability/manifests"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { bash "$gate" "$1" "${2:-$manifests}" >"${work}/out" 2>&1; rc=$?; }

# 变异没落上，这一格测的就还是原树，会假绿。落不上就直接终止，别继续记分。
need() { [ "$1" -eq 0 ] || { bad "$2" "变异落上" "rc=$1"; exit 1; }; }

# 改 chart 里那条规则的 promql。改的是副本。
mutate_rule() { # $1=输出文件  $2=python 片段（拿到 rule 这个 dict）
    python3 - "$config" "$1" "$2" <<'PY'
import json
import sys

src, out, snippet = sys.argv[1:4]
config = json.load(open(src, encoding="utf-8"))
rule = next(r for r in config["observability"]["infra_watch"]["rules"]
            if r["name"] == "boot_phase_footprint_grew")
exec(snippet)
json.dump(config, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
PY
}

# 0) 真树必须绿。
run "$config"
[ "$rc" -eq 0 ] && ok "真树：绿" || bad "真树应为绿" "0" "$rc；$(tail -3 "${work}/out" | tr '\n' ' ')"

# 1) 退回只按 phase 折叠：必须红，而且判词要点到那条规则。
mutate_rule "${work}/c1.json" 'rule["promql"] = rule["promql"].replace("max by (phase, job)", "max by (phase)")'; need $? "规则变异（只按 phase 折叠）"
run "${work}/c1.json"
if [ "$rc" -eq 1 ] && grep -q "boot_phase_footprint_grew" "${work}/out"; then
    ok "退回只按 phase 折叠：红，且判词点名那条规则"
else
    bad "退回只按 phase 折叠应红并点名规则" "1 且含规则名" "$rc；$(tail -3 "${work}/out" | tr '\n' ' ')"
fi

# 2) 选择器点名写者、折叠键不带 job：另一种合法写法，必须仍然绿。
mutate_rule "${work}/c2.json" 'rule["promql"] = rule["promql"].replace("max by (phase, job)", "max by (phase)").replace("cogneva_boot_rss_bytes)", "cogneva_boot_rss_bytes{job=\"cogneva\"})")'; need $? "规则变异（选择器钉住写者）"
run "${work}/c2.json"
[ "$rc" -eq 0 ] && ok "选择器钉住写者：绿（合法写法不被误判）" \
    || bad "选择器钉住写者应为绿" "0" "$rc；$(tail -3 "${work}/out" | tr '\n' ' ')"

# 3) 面板 target 改成按 phase 折叠：必须红。复制整份 manifest 目录后只改那一行。
#    注意不是「去掉 {job=...}」就算红——裸选择器画出来的是两条按 job 分开的曲线，没有东西
#    被折回去（与 CONTROLS 里 `{job=~".*"}` 那条对照同理）；面板这一侧的危险形状是它也开始折叠。
cp -r "$manifests" "${work}/manifests"
python3 - "${work}/manifests/06-grafana-dashboard-configmap.yaml" <<'PY'
import sys
p = sys.argv[1]
t = open(p, encoding="utf-8").read()
old = '"expr": "cogneva_boot_phase_cpu_seconds{job=\\"cogneva\\"}"'
new = '"expr": "max by (phase) (cogneva_boot_phase_cpu_seconds)"'
assert old in t, "面板里没找到那一行"
open(p, "w", encoding="utf-8").write(t.replace(old, new, 1))
PY
need $? "面板变异（按 phase 折叠）"
run "$config" "${work}/manifests"
[ "$rc" -eq 1 ] && ok "面板按 phase 折叠：红（面板也在射程里）" \
    || bad "面板按 phase 折叠应红" "1" "$rc；$(tail -3 "${work}/out" | tr '\n' ' ')"

# 4) manifest 目录不存在：退 3，不许「没看」读成「合规」。
run "$config" "${work}/no-such-dir"
[ "$rc" -eq 3 ] && ok "manifest 目录不存在：退 3（看不了 ≠ 一致）" \
    || bad "manifest 目录不存在应退 3" "3" "$rc"

# 5) 规则文件不存在：退 2。
run "${work}/no-such-config.json"
[ "$rc" -eq 2 ] && ok "规则文件不存在：退 2" || bad "规则文件不存在应退 2" "2" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
