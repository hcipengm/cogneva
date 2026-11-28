#!/usr/bin/env bash
# principle: feedback_meta_autonomy_decide_from_principles
# principle: feedback_a_census_witness_must_be_the_structure_that_makes_the_claim_true
#
# 判的是 `scripts/gates/check-ownership-deferral.sh` 本尊。这门此前**没有测试**：
# 它在真树上印过 0，而没有人能说清那是「树干净」还是「谓词死了」。这一份就是那个见证。
#
# 五格，每格都有一条**挪一格就翻面**的对照：
#   1. 一句把决定交出去的话          ⇒ DEFERS、exit 1（这是它存在的理由）
#   2. 引用那两本冻结台账的名字      ⇒ 不算命中（excluded，不是静默丢掉）
#   3. 用反引号**提到**这句话        ⇒ 不算命中（引号里的形状是被叙述的，不是被使用的）
#   4. 生成物（台账投影、两份名册）  ⇒ 读都不读，但**印出来**读过几份被跳过
#   5. 形状表读不到                  ⇒ exit 2，「读不到」不是「没发现」
#
# 覆盖位：无。只读仓内的形状表，其余全在自建的临时目录里。

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="$HERE/check-ownership-deferral.sh"

pass=0
fail=0
report() {
    if [ "$1" = "$2" ]; then
        printf 'ok    %s\n' "$3"
        pass=$((pass + 1))
    else
        printf 'FAIL  %s\n      实际=%s\n      期望=%s\n' "$3" "$1" "$2" >&2
        fail=$((fail + 1))
    fi
}

[ -f "$GATE" ] || { printf 'FAIL  门不在这台机器上：%s\n' "$GATE" >&2; exit 1; }

T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT

run() { # <文件…>：显式点名，跑门，收 stdout+rc
    OUT="$(bash "$GATE" "$@" 2>&1)"; RC=$?
}

# --- 1. 一句把决定交出去的话 ------------------------------------------------
printf '下一步等用户确认之后再说。\n' >"$T/defers.md"
run "$T/defers.md"
report "$RC" "1" "把决定交给人：exit 1（不是警告）"
report "$(printf '%s\n' "$OUT" | grep -c '^DEFERS')" "1" "并点名那一行"

# --- 2. 引用冻结台账的名字 ---------------------------------------------------
printf '这一条记在待拍板清单里，是台账的名字，不是又一次把决定交出去。\n' >"$T/reference.md"
run "$T/reference.md"
report "$RC" "0" "引用台账名：不算命中"
report "$(printf '%s\n' "$OUT" | grep -c 'excluded')" "1" "但仍印出来（豁免不许静默）"

# --- 3. 用反引号提到这句话 ---------------------------------------------------
printf '报告里写了 `下一步等用户确认` 这一种形状。\n' >"$T/quoted.md"
run "$T/quoted.md"
report "$RC" "0" "引号里提到这句话：不算命中"

# --- 4. 生成物：默认窗里读都不读，但要说出来 --------------------------------
mkdir -p "$T/gen/specs/ledger"
printf '## [landed] 按用户拍板的范围分批次修复\n' >"$T/gen/specs/ledger/2026-11-03.md"
printf '下一步等用户确认之后再说。\n' >"$T/gen/plan.md"
OUT="$(cd "$T/gen" && bash "$GATE" 2>&1)"; RC=$?
report "$RC" "1" "默认窗：只有那份计划命中（生成物不读）"
report "$(printf '%s\n' "$OUT" | grep -c '^DEFERS')" "1" "默认窗：命中数正好是那一份计划"
report "$(printf '%s\n' "$OUT" | grep -c '1 generated file(s) not read')" "1" \
       "默认窗：印出跳过了几份生成物（豁免可见）"

# 反向对照：同一句话放进生成物，就**不该**再命中——否则上面那格证明不了是「跳过」在起作用。
mkdir -p "$T/gen2/specs/ledger"
printf '下一步等用户确认之后再说。\n' >"$T/gen2/specs/ledger/2026-11-03.md"
OUT="$(cd "$T/gen2" && bash "$GATE" 2>&1)"; RC=$?
report "$RC" "0" "同一个形状只出现在生成物里：不命中（跳过是承重的）"
report "$(printf '%s\n' "$OUT" | grep -c 'generated file(s) not read')" "1" \
       "也没有文件可读时，仍然交代跳过了几份"

# --- 5. 形状表读不到：退出 2，不是「没发现」 --------------------------------
mkdir -p "$T/lonely"
cp "$GATE" "$T/lonely/gate.sh"
printf '下一步等用户确认之后再说。\n' >"$T/plain.md"
OUT="$(env -u _COGNEVA_SHAPES -u _COGNEVA_SHAPES_REPO bash "$T/lonely/gate.sh" "$T/plain.md" 2>&1)"; RC=$?
report "$RC" "2" "形状表读不到：exit 2（读不到不是没发现）"

# --- 6. `--staged`：判这一笔的索引，不判盘 ----------------------------------
# 提交路径接的就是这一档，所以它自己也要有见证。关键的对照是 6a：那一份 `.md`
# **盘上根本不存在**，只有索引里有。读数盘的实现那里什么也找不到 ⇒ 印 0、退 0；
# 它红，才证明判的是索引里的字节。
#
# 夹具不往工作树里写任何东西（共享树，写了就是别人的 `git status` 里多一份），
# 而是把 blob 直接塞进一份私有索引——与提交者自己做的事同一种。
REPO="$(cd "$HERE/../../.." && pwd)"
IDX="$T/probe.index"
blob_of() { printf '%s\n' "$2" | git -C "$REPO" hash-object -w --stdin 2>/dev/null; }
fresh_index() { rm -f "$IDX"; GIT_INDEX_FILE="$IDX" git -C "$REPO" read-tree HEAD; }
add_md() { # <路径> <正文>
    local b
    b="$(blob_of "$1" "$2")" || return 1
    GIT_INDEX_FILE="$IDX" git -C "$REPO" update-index --add --cacheinfo 100644,"$b","$1"
}

fresh_index || printf 'note  建不出私有索引（%s 不是 git 仓？）\n' "$REPO"
add_md "zz-deferral-probe-not-on-disk.md" '下一步等用户确认之后再说。'
OUT="$(GIT_INDEX_FILE="$IDX" bash "$GATE" --staged 2>&1)"; RC=$?
report "$RC" "1" "--staged：盘上不存在的 .md 也能从索引里判红"
report "$(printf '%s\n' "$OUT" | grep -c 'zz-deferral-probe-not-on-disk.md')" "1" "并点名那一份"
report "$(printf '%s\n' "$OUT" | grep -c '判这一笔的 1 份')" "1" "并交代判了几份、取自哪份索引"

# 6b 生成物的跳过不随取法翻面：同一句话在台账投影里，按索引算也不算命中。
fresh_index
add_md "specs/ledger/zz-probe.md" '## [landed] 按用户拍板的范围分批次修复'
add_md "zz-clean-probe.md" '今天天气好。'
OUT="$(GIT_INDEX_FILE="$IDX" bash "$GATE" --staged 2>&1)"; RC=$?
report "$RC" "0" "--staged：台账投影里的历史叙述不算命中"
report "$(printf '%s\n' "$OUT" | grep -c '1 generated file(s) not read')" "1" "并印出跳过了几份"

# 6c 索引不在 ⇒ 2。少读一份而印 0，与「这一笔干净」逐字同形。
OUT="$(GIT_INDEX_FILE="$T/no-such.index" bash "$GATE" --staged 2>&1)"; RC=$?
report "$RC" "2" "--staged：索引不在 ⇒ exit 2（读不到不是没发现）"

# 6d 一笔没动 .md：不判，但要说出来。
fresh_index
OUT="$(GIT_INDEX_FILE="$IDX" bash "$GATE" --staged 2>&1)"; RC=$?
report "$RC" "0" "--staged：这一笔没动 .md 时不判"
report "$(printf '%s\n' "$OUT" | grep -c 'no files to read')" "1" "并说清没有文件可读"

# 6e 它是**显式开关**，不是「环境里有 GIT_INDEX_FILE 就不一样」。测试夹具会继承环境，
#    一个按环境变量自转的模式会让同一份套件在提交门里判另一批文件。
mkdir -p "$T/envswitch"
printf '等用户确认。\n' >"$T/envswitch/plan.md"
fresh_index
add_md "zz-other-probe.md" '等用户确认。'
OUT="$(cd "$T/envswitch" && GIT_INDEX_FILE="$IDX" bash "$GATE" 2>&1)"; RC=$?
report "$RC" "1" "环境里有 GIT_INDEX_FILE、但没给 --staged：仍按默认窗判盘"
report "$(printf '%s\n' "$OUT" | grep -c 'plan.md')" "1" "判的是盘上那份"
report "$(printf '%s\n' "$OUT" | grep -c 'zz-other-probe')" "0" "索引里那份没被读（模式没自己切过去）"

printf '\ncheck-ownership-deferral.test: %s passed / %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ] || exit 1
