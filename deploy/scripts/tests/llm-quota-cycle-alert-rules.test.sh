#!/usr/bin/env bash
# 「池掉了，而上游自己说额度周期用完了」那两条规则的夹具**自己**能不能红。
#
# 为什么要有这一层：`cogneva-json-llm-pool.test.yaml` 里那三格判的是「谁响谁静」，
# 而它自己也可能是一枚橡皮图章——把排除腿删掉、把界抬高、把新鲜度腿放开，只要夹具
# 没跟着红，那三格就什么都没测。这里把部署配置的副本改坏五次，每次都必须红在**该红
# 的那一格**：
#   1. 把 `llm_pool_open_but_idle` 的排除腿删掉：它那一格（额度周期现场）必须红——
#      那正是 2026-10-24 现场真实发生的事：它响了，而它的摘要说「不是为预算拒的」；
#   2. 把新规则的界抬到 1e9：它那条 firing 断言必须红——否则一条永远不响的规则也绿；
#   3. 把新规则的「池不可用」腿删掉：池可用那一格（其余成员还在供）必须红——那一格
#      判的是第二条腿，只判第一条腿的夹具留不住它；
#   4. 把新鲜度界从一小时放开到十亿秒：八小时前写的那一格必须红——写者停了之后，
#      存储永远服务最后一行，没有这条腿，一条旧拒绝会被读成「此刻还在拒」；
#   5. 把新规则整条删掉：跑者必须非零，而不是「夹具点名的规则不在库里所以没判」。
#
# 每一处改坏前先断言副本真的变了：改不动的夹具是「什么都没测」的另一种写法。
#
# 跑法：bash deploy/scripts/tests/llm-quota-cycle-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
case_file="${repo}/deploy/k3s/observability/tests/cogneva-json-llm-pool.test.yaml"

pass=0
fail=0
ok() { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() {
    fail=$((fail + 1))
    printf 'FAIL- %s\n' "$1"
    [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"
    [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# stdin 关掉再跑：跑者与 promtool 在本机从终端跑时会去读标准输入，于是这一格
# 不是红也不是绿，是挂在那里——本机测一次要等到外层超时，而「挂住」与「通过」
# 在 `set -uo pipefail`（没有 `-e`）下面读起来一样安静。
run() { RULES_MANIFEST="$1" RULES_CASE="${2:-$case_file}" bash "$runner" >"${work}/out" 2>&1 </dev/null; rc=$?; }

# 把那一库的副本按一段 python 改坏。$1=输出文件 $2=规则名 $3=拿到 rule 后要执行的片段
mutate() {
    python3 - "$config" "$1" "$2" "$3" <<'PY'
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

# 改坏这一步本身就是一条判据：片段没命中时 python 抛异常、副本根本没写出来，
# 而 `cmp -s` 把「文件不存在」读成「与真文件不同」，于是后面那一格拿一个不存在的
# 清单去跑，跑者非零退出，这一格就被记成「夹具红了」——红的是缺文件，不是断言。
# 2026-10-27 实测就踩到了：规则的活文本从 1800 改到 3600，第 3 处的片段再也命中不了，
# 四格照样全绿。
mutated() { # $1=副本 $2=规则名 $3=片段；写成了且与真文件不同才算改坏
    mutate "$1" "$2" "$3" || return 1
    [ -f "$1" ] && ! cmp -s "$config" "$1"
}

# 0) 真文件必须绿。
run "$config"
if [ "$rc" -eq 0 ]; then
    ok "真配置：三格断言全绿"
else
    bad "真配置：这一族规则此刻是红的" "rc=0" "rc=$rc"
    sed 's/^/        /' "${work}/out" | tail -20 >&2
fi

# 1) 删掉 idle 那条的排除腿：额度周期现场那一格必须红。
if mutated "${work}/no-unless.json" llm_pool_open_but_idle \
    'rule["promql"] = rule["promql"].split(" unless on () ")[0]'; then
    run "${work}/no-unless.json"
    [ "$rc" -ne 0 ] && ok "idle 那条丢了排除腿：额度周期那一格红了" \
        || bad "idle 那条丢了排除腿：仍然绿——夹具不判「同一件事不许两条规则抢着答」"
else
    bad "删排除腿：改坏当前规则文本的那一段没命中" "副本已写出且与真文件不同" "没写出来或逐字相同"
fi

# 2) 抬高新规则的界：firing 那一格必须红。
if mutated "${work}/threshold.json" llm_pool_down_on_a_quota_cycle \
    'rule["condition"]={"greater_than": 1e9}'; then
    run "${work}/threshold.json"
    [ "$rc" -ne 0 ] && ok "新规则界抬到十亿：firing 那一格红了" || bad "界抬到十亿仍然绿——这条断言不判界"
else
    bad "界抬高：改坏当前规则文本的那一段没命中" "副本已写出且与真文件不同" "没写出来或逐字相同"
fi

# 3) 删掉「池不可用」那条腿：池可用那一格必须红。
if mutated "${work}/no-pool-leg.json" llm_pool_down_on_a_quota_cycle '
leg = " and on () (max(llm_pool_available and ((time() - llm_pool_available_observed_timestamp_seconds) < 3600)) == 0)"
assert leg in rule["promql"], "那条腿的文本变了，这条改坏不再有效"
rule["promql"] = rule["promql"].replace(leg, "")'
then
    run "${work}/no-pool-leg.json"
    [ "$rc" -ne 0 ] && ok "新规则丢了池不可用腿：池可用那一格红了" || bad "删掉池不可用腿仍然绿——夹具不判第二条腿"
else
    bad "删池不可用腿：改坏当前规则文本的那一段没命中" "副本已写出且与真文件不同" "没写出来或逐字相同"
fi

# 4) 把新鲜度界放开：八小时前那一格必须红。
if mutated "${work}/no-freshness.json" llm_pool_down_on_a_quota_cycle \
    'rule["promql"] = rule["promql"].replace("< 3600", "< 1000000000")'; then
    run "${work}/no-freshness.json"
    [ "$rc" -ne 0 ] && ok "新鲜度界放开到十亿秒：八小时前那一格红了" || bad "新鲜度界放开仍然绿——夹具不判写者是否还在写"
else
    bad "放开新鲜度界：改坏当前规则文本的那一段没命中" "副本已写出且与真文件不同" "没写出来或逐字相同"
fi

# 5) 删掉整条规则：夹具点名了库里没有的规则，跑者必须非零。
if mutated "${work}/dropped.json" llm_pool_down_on_a_quota_cycle 'rules.remove(rule)'; then
    run "${work}/dropped.json"
    [ "$rc" -ne 0 ] && ok "夹具点名的规则被删：跑者非零" || bad "规则被删仍然零——夹具的死指针没被抓住"
else
    bad "删规则：改坏当前规则文本的那一段没命中" "副本已写出且与真文件不同" "没写出来或逐字相同"
fi

printf '\n通过 %d，失败 %d\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
