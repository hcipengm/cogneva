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

# 绕过编译前的整批检查要在这道门被看见：记账里有一条 BYPASS、提交里没有 Split ⇒ 红。
bypass_ledger="$tmp/compile-ledger.tsv"
printf '%s\t%s\tcheck --workspace\tBYPASS\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$tree" >"$bypass_ledger"
echo a9 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "touch one

nothing else"
out=$(COGNEVA_COMPILE_LEDGER="$bypass_ledger" COGNEVA_EVENT_PSQL="$tmp/psql.sh" \
      COGNEVA_TODO_LEDGER="$ledger" python3 "$tree/scripts/hooks/batch-gate.py" \
      --root "$tree" --base origin/main 2>&1); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "跳过整批检查"; } \
    && ok "有绕过记录、又没有 Split：红" \
    || bad "绕过记录没有被这道门看见（rc=$rc）：$out"
reset

# 没有计划的一轮：判不了「整批有没有动到」，退 2。
cat >"$tmp/psql-noplan.sh" <<STUB
#!/bin/sh
case "\$*" in
  *batch_planned*) printf '0\n' ;;
  *round_opened*) printf '%s\t%s\t%s\n' "round-test" "$LEDGER_REV" "$CODE_REV" ;;
  *) : ;;
esac
STUB
chmod +x "$tmp/psql-noplan.sh"
out=$(COGNEVA_EVENT_PSQL="$tmp/psql-noplan.sh" COGNEVA_TODO_LEDGER="$ledger" \
      python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main 2>&1); rc=$?
{ [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "没有计划"; } \
    && ok "这一轮没有计划：退 2（判不了）" \
    || bad "没有计划时没退 2（rc=$rc）：$out"
reset

printf '\nbatch-gate.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
