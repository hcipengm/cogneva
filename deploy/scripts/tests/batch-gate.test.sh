#!/usr/bin/env bash
# `scripts/hooks/batch-gate.py` 的离线测试：夹具是一棵小仓 + 一份小台账。
#
# 判的是「默认做完整批，没做的要逐条点名」：
#   1. 有未完成待办、提交里没有 Split ⇒ 红，并把它列出来。
#   2. Split 点名了别的待办、没点名它 ⇒ 红（没点名的排除不算排除）。
#   3. Split 点名了它、理由在这一批里成立 ⇒ 绿。
#   4. 本轮的待办都动到了 ⇒ 绿，不看 Split。
#   5. 台账读不到 ⇒ 退 2（没判），不是绿。
set -uo pipefail
# 夹具仓通过 `git -C <dir>` 写，外层带进的绝对 GIT_DIR 会让它写进调用方的仓。
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
gate="$root/scripts/hooks/batch-gate.py"
[ -f "$gate" ] || { echo "FAIL: 门脚本不在：$gate" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/scripts/hooks" "$tree/crates/a/src" "$tree/crates/b/src" "$tree/deploy"
cp "$gate" "$tree/scripts/hooks/batch-gate.py"
cp "$(dirname "$gate")/todo_source.py" "$tree/scripts/hooks/todo_source.py"
cp "$(dirname "$gate")/round.py" "$tree/scripts/hooks/round.py"
echo a >"$tree/crates/a/src/x.rs"; echo b >"$tree/crates/b/src/y.rs"; echo d >"$tree/deploy/manifest.yaml"
git -C "$tree" init -q; git -C "$tree" config user.email t@t; git -C "$tree" config user.name t
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base; git -C "$tree" branch -f origin/main HEAD

ledger="$tree/ledger.md"
row() { printf '| %s | 内容 | 未落 | 判据 |｜round: current｜touches: %s\n' "$1" "$2"; }
{ row S1 "crates/a/src/x.rs"; row S2 "crates/b/src/y.rs"; row S3 "deploy/manifest.yaml"; } >"$ledger"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm ledger
git -C "$tree" branch -f origin/main HEAD
LEDGER_REV="$(git -C "$tree" rev-parse HEAD)"
CODE_REV="$LEDGER_REV"

# 事件流的替身：round 现在住在流里，夹具给一条「开着一轮」的回答就够。
cat >"$tmp/psql.sh" <<STUB
#!/bin/sh
case "\$*" in
  *batch_planned*) printf '1\n' ;;
  *round_opened*) printf '%s\t%s\t%s\n' "round-test" "$LEDGER_REV" "$CODE_REV" ;;
  *) : ;;
esac
STUB
chmod +x "$tmp/psql.sh"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
reset() { git -C "$tree" reset -q --hard origin/main; }
run() { COGNEVA_EVENT_PSQL="$tmp/psql.sh" COGNEVA_TODO_LEDGER="$ledger" python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main 2>&1; }
commit_with() { git -C "$tree" commit -q --allow-empty -F - < <(printf '%s\n' "$1"); }

echo a2 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "touch one

nothing else"; out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "S2"; } && ok "没做完又没有 Split：红，并点名没做的" || bad "没做完也没 Split 却没红（rc=$rc）：$out"
reset

echo a3 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "split, wrong todo

Split: 3 — S1 — 点名了已经做了的那条"; out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "既没做也没被"; } && ok "Split 没点名真正没做的：红" || bad "点名错对象没红（rc=$rc）：$out"
reset

echo a5 >>"$tree/crates/a/src/x.rs"; echo d2 >>"$tree/deploy/manifest.yaml"
git -C "$tree" add -A >/dev/null
commit_with "split by irreversibility

Split: 3 — S2 — 这一批要动 deploy 清单，S2 是纯代码，留着下一批"; out=$(run); rc=$?
[ "$rc" -eq 0 ] && ok "Split 点名了真正没做的 S2，且这一批碰了 deploy/：绿" || bad "合格的 Split 被拒（rc=$rc）：$out"
reset

echo a4 >>"$tree/crates/a/src/x.rs"; echo b4 >>"$tree/crates/b/src/y.rs"; echo d3 >>"$tree/deploy/manifest.yaml"
git -C "$tree" add -A >/dev/null; commit_with "touch both"
out=$(run); rc=$?
[ "$rc" -eq 0 ] && ok "本轮待办都动到了：绿，不看 Split" || bad "全做完仍被拒（rc=$rc）：$out"

out=$(COGNEVA_EVENT_PSQL="$tmp/psql.sh" COGNEVA_TODO_LEDGER="$tmp/missing.md" python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main 2>&1); rc=$?
[ "$rc" -eq 2 ] && ok "台账读不到：退 2（没判）" || bad "台账读不到时的行为不对（rc=$rc）：$out"

# ---- 第 3 条理由判的是两侧的不对称，不是「这一批有没有碰 deploy/」-----------
#
# 这三组各用一本自己的台账：主台账要做到「这一批全动到了」，多出来的不可回退条目
# 会把它变成红。两本都在基准提交里，于是它们不进 `git diff`。
ledger_code="$tree/ledger-code.md"
{ row S1 "crates/a/src/x.rs"; row S2 "crates/b/src/y.rs"; } >"$ledger_code"
ledger_irr="$tree/ledger-irr.md"
{ row S1 "crates/a/src/x.rs"; row S2 "crates/b/src/y.rs"; row S3 "deploy/manifest.yaml";
  row S4 "crates/a/migrations/001_x.up.sql"; } >"$ledger_irr"
long=$(printf '这一条要读的正文很长%.0s' $(seq 1 120))
ledger_long="$tree/ledger-long.md"
{ row S1 "crates/a/src/x.rs"; printf '| S2 | %s | 未落 | 判据 |｜round: current｜touches: crates/b/src/y.rs\n' "$long"; } >"$ledger_long"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "两本窄台账"; git -C "$tree" branch -f origin/main HEAD
LEDGER_REV="$(git -C "$tree" rev-parse HEAD)"
# 这一轮的代码基准也前移到这一笔：留着旧的那一条，diff 会把上面几个用例动过的
# 文件一起算进来（`deploy/manifest.yaml` 就是那样冒出来的）。
CODE_REV="$LEDGER_REV"
cat >"$tmp/psql.sh" <<STUB
#!/bin/sh
case "\$*" in
  *batch_planned*) printf '1\n' ;;
  *round_opened*) printf '%s\t%s\t%s\n' "round-test" "$LEDGER_REV" "$CODE_REV" ;;
  *) : ;;
esac
STUB
chmod +x "$tmp/psql.sh"
reset
run_on() { COGNEVA_EVENT_PSQL="$tmp/psql.sh" COGNEVA_TODO_LEDGER="$2" python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main "${@:3}" 2>&1; }

# 不可回退的那条在**被排除**的一侧：这一批是纯代码，deploy/ 与迁移留到下一批。
echo a6 >>"$tree/crates/a/src/x.rs"; echo b6 >>"$tree/crates/b/src/y.rs"
git -C "$tree" add -A >/dev/null
commit_with "irreversible side is the excluded one

Split: 3 — S3, S4 — 这一批是纯代码；deploy 与迁移要单独落地，回滚一次提交回不到原状"
out=$(run_on "$tree" "$ledger_irr"); rc=$?
[ "$rc" -eq 0 ] && ok "不可回退的那一侧被排除、这一批是纯代码：绿" || bad "正确的不对称被拒（rc=$rc）：$out"
reset

# 两侧都碰：拆开并没有把不可回退的那一侧单独拎出来。
echo a7 >>"$tree/crates/a/src/x.rs"; echo d7 >>"$tree/deploy/manifest.yaml"
git -C "$tree" add -A >/dev/null
commit_with "both sides irreversible

Split: 3 — S2, S4 — 两侧都有不可回退的东西"
out=$(run_on "$tree" "$ledger_irr"); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "两侧都碰"; } && ok "两侧都碰不可回退的面：红" || bad "两侧都碰却没红（rc=$rc）：$out"
reset

# 两侧都不碰：没有「回滚一次提交回不到原状」的东西，理由不成立。
echo a8 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "neither side irreversible

Split: 3 — S2 — 两侧都是纯代码"
out=$(run_on "$tree" "$ledger_code"); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "两侧都没碰"; } && ok "两侧都不碰不可回退的面：红" || bad "两侧都不碰却没红（rc=$rc）：$out"
reset

# 第 4 条理由要拿**整行正文**去比窗口份额：标题截断到 80 字时，最长的几条会被
# 估成同一个数，于是这条理由永远不成立。
echo a9 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "too much to read in one window

Split: 4 — S2 — 这一条读不完"
out=$(run_on "$tree" "$ledger_long" --window-tokens 400); rc=$?
[ "$rc" -eq 0 ] && ok "第 4 条：正文超过窗口份额的一半，成立" || bad "第 4 条合格却仍被拒（rc=$rc）：$out"
out=$(run_on "$tree" "$ledger_long" --window-tokens 8000); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "窗口份额"; } && ok "第 4 条：正文远小于窗口份额，红" || bad "第 4 条不合格却没红（rc=$rc）：$out"
reset

printf '\nbatch-gate.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
