#!/usr/bin/env bash
# principle: feedback_a_join_keeps_every_label_nobody_named
# principle: feedback_absent_over_time_synthesizes_the_selector_equality_matchers
# 让每一份告警证明夹具**真的跑一遍**。
#
# 为什么要有这一层：`check-alert-rule-proof.sh` 判的是「每条规则都被一格会响、一格不响
# 点到名」，那是**在场**的判据，不是**通过**的判据——它读的是 YAML 里的 `alertname:` 与
# `exp_alerts`，从不执行 promtool。于是一份写坏的夹具（断言了错的标签、写错了时间点、
# 规则改名后还指着旧名字）照样能让那条门判绿，而它声称证明的东西一个字节也没被验证过。
# 在场与通过是两件事：前者挡的是「没写」，后者挡的是「写了但写错」。
#
# 配对按文件名前缀：`<N>-prometheusrule-*.test.yaml` 跑 `<N>-prometheusrule-*.yaml`。
# 不手写一张对照表——表会在加新清单时悄悄漏掉一份（漏掉的那份永远不跑，且没有任何读数
# 会变色）。配对失败（找不到同名清单）判红，不判跳过。
#
# `cogneva-json-*` 的夹具不在这里：它们对的是 `cogneva.json` 里的进程内规则库，由
# `cogneva-json-alert-rules.test.sh` 跑，那一份要六分钟，不该塞进这条。
#
# 另有两格证明这条门自己会红：改坏一份夹具的断言必须跑红；指一份不存在的清单必须退 3
# （看不了不是通过）。
#
# 跑法：bash deploy/scripts/tests/alert-rule-fixtures-run.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
tests="${repo}/deploy/k3s/observability/tests"
manifests="${repo}/deploy/k3s/observability/manifests"

pass=0; fail=0; skip=0
ok()   { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad()  { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

# 一份夹具配哪份清单，只看文件名前缀——配对是从人口推出来的，不是抄的。
manifest_for() { # <夹具路径> ⇒ 清单路径；配不上就空
    local base n
    base="$(basename "$1")"
    n="${base%%-*}"
    [ "$n" != "$base" ] || return 0
    local hits=( "${manifests}/${n}-prometheusrule-"*.yaml )
    [ -f "${hits[0]}" ] && printf '%s' "${hits[0]}"
}

# 0) 真树：每一份夹具都跑过。
ran=0; failed=""
for f in "${tests}"/*prometheusrule*.test.yaml; do
    case "$(basename "$f")" in cogneva-json-*) skip=$((skip + 1)); continue ;; esac
    m="$(manifest_for "$f")"
    if [ -z "$m" ]; then
        failed="${failed} $(basename "$f")(无配对清单)"
        continue
    fi
    if out="$(bash "$runner" "$m" "$f" 2>&1)" && grep -q 'SUCCESS' <<<"$out"; then
        ran=$((ran + 1))
    else
        failed="${failed} $(basename "$f")"
        printf '%s\n' "$out" | tail -12 | sed 's/^/        /'
    fi
done
if [ -n "$failed" ]; then
    bad "真树：每份夹具都要跑过 promtool" "全部 SUCCESS" "没跑过的：$failed"
elif [ "$ran" -eq 0 ]; then
    bad "真树：一份夹具都没跑到" ">0 份" "0 份——人口为空时「全过」是假的"
else
    ok "真树：${ran} 份夹具全部跑过 promtool（每份都断言了一响一不响）"
fi

# 1) 自己会红：把一格的 `exp_alerts: []` 换成一个必须响的断言，这一份必须跑红。
work="$(mktemp -d)"; trap 'rm -rf "${work}"' EXIT
src="${tests}/15-prometheusrule-host-target-gc.test.yaml"
if [ -f "$src" ] && [ -n "$(manifest_for "$src")" ]; then
    python3 -I - "$src" "${work}/broken.yaml" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
lines = open(src, encoding="utf-8").read().splitlines(keepends=True)
out, flipped = [], False
for i, line in enumerate(lines):
    if not flipped and line.strip() == "exp_alerts: []":
        # 把它换成一个「必须响」的断言：静格所在的样本喂的是健康数据，规则不该响。
        out.append(line.replace("exp_alerts: []", "exp_alerts:\n          - exp_labels:\n              severity: warning"))
        flipped = True
        continue
    out.append(line)
assert flipped, "夹具里没有 exp_alerts: []"
open(dst, "w", encoding="utf-8").write("".join(out))
PY
    if out="$(bash "$runner" "$(manifest_for "$src")" "${work}/broken.yaml" 2>&1)"; then
        bad "改坏一格静格后应当跑红" "非 0" "退出码 0：这份门在断言写错时也照绿"
    else
        ok "改坏一格静格后跑红（静格不是摆设）"
    fi
else
    bad "找不到可用于自检的夹具" "${src}" "文件或配对清单不存在"
fi

# 2) 看不了不是通过：清单不存在必须退 3，不能退 0。
bash "$runner" "${work}/no-such-manifest.yaml" "$src" >/dev/null 2>&1
rc=$?
[ "$rc" -eq 3 ] && ok "清单不存在时退 3（看不了 ≠ 通过）" \
    || bad "清单不存在应退 3" "3" "$rc"

# 跳过的份数要印出来：一份既没跑也没跳才是「安静」，印了 0 才说明这一轮确实没有该跳的。
printf '%s: %d passed, %d failed, %d skipped\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail" "$skip"
[ "$fail" -eq 0 ]
