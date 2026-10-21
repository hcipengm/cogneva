#!/usr/bin/env bash
# check-alert-rule-join-shape.sh 的定向测试：判词必须由**表达式结构**推出来，
# 而且「绿 / 红 / 判不动」三个方向各自都要点名到自己那一条。
#
# 为什么要有这份测试：这条门禁是文本推理——从 by(...) 集与 on(...) 键集比出谁是多的一侧。
# 这类推理**改坏了不会报错**：它只会让判词系统性偏松（把该红的读成绿）或者偏严
# （把无辜的规则读成红），而条数照出。所以每个用例都断言**具体那句判词**，不只断言退出码。
#
# 用例全部靠改动真规则文件的一份副本造出来（真文件本身就是控制集，规则名与条数都还在）：
#   1. 真文件 -> 绿，且汇总行自报「判得动 N 个带 on() 的比较、违规 0 处」；
#      同一个文件里那条规则此刻是带 group_left 的——它绿是因为声明了，不是因为门禁没看它。
#   2. 把 build_gate_slot_held_too_long 的 group_left () 摘掉 -> 红，点名它，并把两侧的
#      label 集印出来（['dir', 'role', 'slot', 'what'] 对 ['dir', 'role']）。这是 2026-10-20
#      现网 422 的原形状：单槽时一对一、不报错，两槽同时被占才炸。
#   3. 两侧 by 集都等于连接键 -> 绿（一对一，不需要 group_*）。
#   4. 带 ignoring(...) 的比较 -> 绿，但计入「判不动」：键集是「现有标签减去这些」，不读
#      现网补不齐，说不清不许当绿。
#   5. 同一条规则里两个 on() 比较、只有一个缺 group_* -> 正好报 1 处，不多不少。
#   6. 文件不存在 / JSON 读不了 -> rc=3，且判词说「看不了，不是一致」。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-join-shape.sh"
config="${repo}/deploy/helm/cogneva/files/cogneva.json"
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { printf 'ok    %s\n' "$*"; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
[ -f "${gate}" ] || fail "判据脚本不在：${gate}"
[ -f "${config}" ] || fail "规则文件不在：${config}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 用一个 key -> promql 的改动表造夹具；其余规则原样保留。改动表走实参而不是 stdin：
# 下面每个用例自己也要给门禁喂参数，共用 stdin 会互相吃掉。
mkvariant() { # $1 = 输出名，$2 = JSON 改动表
    python3 -c '
import json, sys
src, out, patch = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
doc = json.load(open(src, encoding="utf-8"))
for rule in doc["observability"]["infra_watch"]["rules"]:
    if rule["name"] in patch:
        rule["promql"] = patch[rule["name"]]
json.dump(doc, open(out, "w", encoding="utf-8"))
' "${config}" "${work}/$1" "$2"
}

# 真文件：绿，且汇总行把射程说清楚（判得动几条、违规几处）。
out="$(bash "${gate}" "${config}")"
rc=$?
[ "${rc}" -eq 0 ] || fail "真文件应绿，实际 rc=${rc}：${out}"
grep -q '违规 0 处' <<<"${out}" || fail "真文件的汇总行没有自报 0 处违规：${out}"
grep -q '判不动' <<<"${out}" || fail "汇总行没有印判不动的条数：${out}"
# 判得动的条数由这份文件自己算出来，钉死一个字面量的话，每加一条规则这份测试就红一次——
# 红的是它的抄本，不是门禁坏了。
judged="$(sed -n 's/.*判得动 \([0-9]*\) 个带 on().*/\1/p' <<<"${out}")"
[ -n "${judged}" ] || fail "汇总行里读不出判得动的条数：${out}"
ok "真文件绿，汇总行自报射程（判得动 ${judged} 条）"

# 真文件里那条规则的当前形状必须真的带 group_left——否则用例 2 摘掉的东西不存在。
grep -q 'group_left' "${config}" || fail "真文件里已经没有 group_left，用例 2 失去了对照"

# 2. 复现 2026-10-20 的形状：摘掉 group_left ()。
mkvariant prefix '{"build_gate_slot_held_too_long": "max by (dir, role, slot, what) (cogneva_build_gate_holder_seconds) > on(dir, role) (2 * max by (dir, role) (cogneva_build_gate_wait_budget_ms) / 1000)"}'
set +e
out="$(bash "${gate}" "${work}/prefix")"; rc=$?
set -e
[ "${rc}" -eq 1 ] || fail "摘掉 group_left 后应红（rc=1），实际 rc=${rc}：${out}"
grep -q 'build_gate_slot_held_too_long' <<<"${out}" || fail "红的那条没点名规则：${out}"
grep -q "slot', 'what'\]" <<<"${out}" || fail "判词没把多的一侧的 label 集印出来：${out}"
grep -q '违规 1 处' <<<"${out}" || fail "应正好 1 处违规：${out}"
ok "原形状红，点名规则与两侧 label 集"

# 3. 两侧 by 集都等于连接键：一对一，绿。
mkvariant balanced '{"build_gate_slot_held_too_long": "max by (dir, role) (cogneva_build_gate_holder_seconds) > on(dir, role) max by (dir, role) (cogneva_build_gate_wait_budget_ms)"}'
set +e
out="$(bash "${gate}" "${work}/balanced")"; rc=$?
set -e
[ "${rc}" -eq 0 ] || fail "两侧 by 集相等应绿，实际 rc=${rc}：${out}"
grep -q '违规 0 处' <<<"${out}" || fail "相等的 by 集被读成了违规：${out}"
ok "两侧 by 集相等 -> 绿"

# 4. ignoring(...)：绿，但计入判不动——不许把「不读现网补不齐键集」当绿。
mkvariant ignoring '{"build_gate_slot_held_too_long": "max by (dir, role, slot, what) (cogneva_build_gate_holder_seconds) > ignoring(slot, what) (2 * max by (dir, role) (cogneva_build_gate_wait_budget_ms) / 1000)"}'
set +e
out="$(bash "${gate}" "${work}/ignoring")"; rc=$?
set -e
[ "${rc}" -eq 0 ] || fail "ignoring 不在判据内，应绿，实际 rc=${rc}：${out}"
grep -q '判不动 1 个' <<<"${out}" || fail "ignoring 的比较没有被记进判不动：${out}"
ok "ignoring -> 绿但记判不动"

# 5. 同一条规则两个 on() 比较、只缺一个 group_*：正好 1 处。
#    两段之间用 or 而不是 and：这里要考的是「两条比较逐条判、只报缺的那一条」，而
#    and 会顺带考到第二段判据（两条比较的输出标签集 {dir,role} 与 {dir,role,slot} 不
#    相等，那个 and 自己就永远连不上，是另一件事）。用例只该问一件事。
mkvariant twojoins '{"build_gate_slot_held_too_long": "(max by (dir, role) (cogneva_build_gate_holder_seconds) > on(dir, role) max by (dir, role) (cogneva_build_gate_wait_budget_ms)) or (max by (dir, role, slot) (cogneva_build_gate_wait_seconds) > on(dir, role) max by (dir, role) (cogneva_build_gate_wait_budget_ms))"}'
set +e
out="$(bash "${gate}" "${work}/twojoins")"; rc=$?
set -e
[ "${rc}" -eq 1 ] || fail "两条比较里缺一条 group_* 应红，实际 rc=${rc}：${out}"
grep -q '违规 1 处' <<<"${out}" || fail "应正好 1 处违规（不是把整条规则算一处或两处）：${out}"
grep -q "判得动 $((judged + 1)) 个带 on() 的比较" <<<"${out}" \
    || fail "这条规则的两条比较都应被判到（它在真文件里的那条被换成了两条）：${out}"
ok "多条比较逐条判，违规数准确"

# 6. 读不了：文件不存在 / JSON 坏掉 -> rc=3 且不许说成一致。
set +e
out="$(bash "${gate}" "${work}/nope.json" 2>&1)"; rc=$?
set -e
[ "${rc}" -eq 3 ] || fail "文件不存在应 rc=3，实际 rc=${rc}：${out}"
grep -q '看不了，不是一致' <<<"${out}" || fail "读不了的判词没说清「看不了不是一致」：${out}"
printf 'not json' > "${work}/broken.json"
set +e
out="$(bash "${gate}" "${work}/broken.json" 2>&1)"; rc=$?
set -e
[ "${rc}" -eq 3 ] || fail "JSON 坏掉应 rc=3，实际 rc=${rc}：${out}"
ok "读不了 -> rc=3，判词不留成「一致」"

# 7. 射程守卫：清单里的规则一条都不被这条门读过。真清单里没有带 on() 的比较 -> 绿；
#    放一份进去 -> 红并点名文件与行号。那是 subject 门的前身走过的弯路：规则改落在
#    manifest 里，chart 那份 JSON 一条不少，门却再也没看过新规则。
set +e
out="$(bash "${gate}" "${config}" "${repo}/deploy/k3s/observability/manifests")"; rc=$?
set -e
[ "${rc}" -eq 0 ] || fail "真清单不该让这条门红，实际 rc=${rc}：${out}"
grep -q '射程守卫' <<<"${out}" || fail "汇总里没有射程守卫那一行：${out}"
mkdir -p "${work}/manifests"
cat > "${work}/manifests/99-fake-rule.yaml" <<'YAML'
spec:
  groups:
    - name: fake
      rules:
        - alert: someone_else_joins
          expr: max by (dir, role, slot) (a_metric) > on(dir, role) max by (dir, role) (b_metric)
YAML
set +e
out="$(bash "${gate}" "${config}" "${work}/manifests")"; rc=$?
set -e
[ "${rc}" -eq 1 ] || fail "清单里有带 on() 的比较应红，实际 rc=${rc}：${out}"
grep -q '99-fake-rule.yaml:' <<<"${out}" || fail "射程守卫没点名文件与行号：${out}"
set +e
out="$(bash "${gate}" "${config}" "${work}/no-such-dir" 2>&1)"; rc=$?
set -e
[ "${rc}" -eq 3 ] || fail "清单目录不在应 rc=3（射程有没有缺口不知道），实际 rc=${rc}：${out}"
grep -q '判词记 3' <<<"${out}" || fail "清单目录读不了时没说「判词记 3」：${out}"
ok "射程守卫：清单里的连接红并点名，真清单绿，清单目录读不了记 3"

# 8. 集合运算（and / unless）不带 on()/ignoring()：默认按全标签集匹配。
#    8a 两侧 by 集不等 -> 连不上，红：and 恒空（规则一生不响）、unless 恒亮（永远在报）。
mkvariant setjoin_bad '{"build_gate_slot_held_too_long": "max by (dir, role) (cogneva_build_gate_holder_seconds) unless max by (dir, role, slot) (cogneva_build_gate_wait_budget_ms)"}'
set +e
out="$(bash "${gate}" "${work}/setjoin_bad")"; rc=$?
set -e
[ "${rc}" -eq 1 ] || fail "两侧 by 集不等的 unless 应红，实际 rc=${rc}：${out}"
grep -q '不可能相等' <<<"${out}" || fail "集合运算的判词没说「不可能相等」：${out}"
ok "集合运算两侧 by 集不等 -> 红（连不上）"

#    8b 两侧 without(...)：命中标签集取决于指标自己带了什么，判不了 -> 点名但判词不改，
#       并且不许因为「两侧形状一样」就当成连得上（2026-10-22 的恒亮规则正是这个形状）。
mkvariant setjoin_without '{"build_gate_slot_held_too_long": "max without (pod, container, instance) (cogneva_build_gate_holder_seconds) unless max without (pod, container, instance) (cogneva_build_gate_wait_budget_ms)"}'
set +e
out="$(bash "${gate}" "${work}/setjoin_without")"; rc=$?
set -e
[ "${rc}" -eq 0 ] || fail "without 判不了，不应改判词（rc=0），实际 rc=${rc}：${out}"
grep -q 'build_gate_slot_held_too_long' <<<"${out}" || fail "判不了的集合运算没有点名规则：${out}"
grep -q '两侧用了 without' <<<"${out}" || fail "判词没说清为什么判不了：${out}"
ok "集合运算两侧 without(...) -> 点名判不了，不当成绿"

echo "alert-rule-join-shape: 8 组用例全绿"
