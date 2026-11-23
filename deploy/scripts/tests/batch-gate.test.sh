#!/usr/bin/env bash
# `scripts/hooks/batch-gate.py` 的离线测试：夹具是一棵小仓 + 一份小台账。
#
# 判的是「默认做完整批，没做的要逐条点名」：
#   1. 有未完成待办、提交里没有 Split ⇒ 红，并把它列出来。
#   2. Split 点名了别的待办、没点名它 ⇒ 红（没点名的排除不算排除）。
#   3. Split 点名了它、理由在这一批里成立 ⇒ 绿。
#   4. 本轮的待办**都填了 `代码` 格** ⇒ 绿，不看 Split。
#   5. 台账读不到 ⇒ 退 2（没判），不是绿。
#
# 「做完了」只认台账那一行的 `代码` 格，**不认触点**（属主 2026-11-21 定）。夹具因此
# 每个场景自带一本台账：写死了哪些条做完了，就是哪些条做完了；改了文件不等于做完，
# 这正是第 6 条要钉的形状。
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
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base
BASE="$(git -C "$tree" rev-parse HEAD)"
git -C "$tree" branch -f origin/main "$BASE"

ledger="$tree/ledger.md"
# 一条待办的一行：`$1` 是 id，`$2` 是触点，`$3` 是 `done` 时带上 `代码` 格。
row() {
  if [ "${3:-}" = "done" ]; then
    printf '| %s | 内容 | 未落 | 判据 代码: %s |｜round: current｜touches: %s\n' "$1" "$2" "$2"
  else
    printf '| %s | 内容 | 未落 | 判据 |｜round: current｜touches: %s\n' "$1" "$2"
  fi
}

# 写一本台账并提交，返回那一笔的 rev（回合事件钉住它）。
# `SPECS` 是这一本台账有哪几条（`S1=crates/a/src/x.rs` 这样写）；`$*` 是做完的那些。
SPECS="S1=crates/a/src/x.rs S2=crates/b/src/y.rs S3=deploy/manifest.yaml"
write_ledger() { # $1 = path, rest = 已完成（带 `代码` 格）的 id
  local path="$1"; shift
  local done_ids=" $* "
  : >"$path"
  local spec
  for spec in $SPECS; do
    local id="${spec%%=*}" touch="${spec#*=}"
    case "$done_ids" in
      *" $id "*) row "$id" "$touch" "done" ;;
      *) row "$id" "$touch" ;;
    esac
  done >>"$path"
  git -C "$tree" add -A >/dev/null
  git -C "$tree" commit -qm "ledger: done=$*" >/dev/null
  git -C "$tree" rev-parse HEAD
}

# 事件流的替身：这一面的来源是 md 台账，轮的书由**台账来源导出**——台账在这个 git
# 仓里，书就是 `refs/cogneva/round`（见 `round-git-store.test.sh`）。夹具每个场景
# 重钉这一轮：钉住的台账 rev 不同，代码基准固定为 `BASE`——两个基准分开，改了哪些
# 文件才读得出来。
: >"$tmp/ledger_rev"
cat >"$tmp/psql.sh" <<STUB
#!/bin/sh
case "\$*" in
  *batch_planned*) printf '1\n' ;;
  *) : ;;
esac
STUB
chmod +x "$tmp/psql.sh"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
reset() { git -C "$tree" reset -q --hard "$BASE"; }
run() { COGNEVA_EVENT_PSQL="$tmp/psql.sh" COGNEVA_TODO_LEDGER="${2:-$ledger}" python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main "${@:3}" 2>&1; }
commit_with() { git -C "$tree" commit -q --allow-empty -F - < <(printf '%s\n' "$1"); }
pin() { # $1 = 钉住的台账 rev：写进 git 书的载荷
  printf '%s' "$1" >"$tmp/ledger_rev"
  payload=$(printf '{"event_id":"round-test","ledger_rev":"%s","code_rev":"%s","batch":[],"first_rev":"%s"}' "$1" "$BASE" "$1")
  sha=$(printf '%s' "$payload" | git -C "$tree" hash-object -w --stdin)
  git -C "$tree" update-ref refs/cogneva/round "$sha"
}

# 场景 0：一条都没做完。台账判「全没做」，与改了哪些文件无关。
reset; rev=$(write_ledger "$ledger"); pin "$rev"
echo a2 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "touch one

nothing else"; out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "S2"; } && ok "没做完又没有 Split：红，并点名没做的" || bad "没做完也没 Split 却没红（rc=$rc）：$out"

# 场景 1：S1、S3 做完，S2 没做。提交里 Split 点名的是 S1——没点中真正没做的。
reset; rev=$(write_ledger "$ledger" S1 S3); pin "$rev"
commit_with "split, wrong todo

Split: 3 — S1 — 点名了已经做了的那条"; out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "既没做也没被"; } && ok "Split 没点名真正没做的：红" || bad "点名错对象没红（rc=$rc）：$out"

# 场景 2：S1、S3 做完，这一批碰了 deploy/，Split 把纯代码的 S2 留到下一批。
reset; rev=$(write_ledger "$ledger" S1 S3); pin "$rev"
echo d2 >>"$tree/deploy/manifest.yaml"; git -C "$tree" add -A >/dev/null
commit_with "split by irreversibility

Split: 3 — S2 — 这一批要动 deploy 清单，S2 是纯代码，留着下一批"; out=$(run); rc=$?
[ "$rc" -eq 0 ] && ok "Split 点名了真正没做的 S2，且这一批碰了 deploy/：绿" || bad "合格的 Split 被拒（rc=$rc）：$out"

# 场景 3：三条都填了 `代码` 格 ⇒ 绿，不看 Split、也不看改了哪些文件。
reset; rev=$(write_ledger "$ledger" S1 S2 S3); pin "$rev"
commit_with "all declared"
out=$(run); rc=$?
[ "$rc" -eq 0 ] && ok "本轮待办都填了代码格：绿，不看 Split" || bad "全做完仍被拒（rc=$rc）：$out"

# 场景 3b：**触点全动到了、`代码` 格没填** ⇒ 仍然是红。这是这条判据换掉的那个旧行为：
# 从前「文件改过」就算做完，于是改了一半、改错了地方都读成做了。
reset; rev=$(write_ledger "$ledger"); pin "$rev"
echo a3b >>"$tree/crates/a/src/x.rs"; echo b3b >>"$tree/crates/b/src/y.rs"; echo d3b >>"$tree/deploy/manifest.yaml"
git -C "$tree" add -A >/dev/null
commit_with "touched every path but declared nothing"; out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "S1"; } && ok "触点全动到、代码格空着：仍红（改了文件不等于做完）" || bad "触点判据还在：没填代码格却绿了（rc=$rc）：$out"

# 场景 3c：`代码` 格点了一个树里不存在的路径 ⇒ 红。填了不等于填得对：那一格
# 必须是「树里有这个证据」，凭记忆写的路径与真的改动在读者眼里同形，除非有门去查。
reset; rev=$(write_ledger "$ledger" S1 S2 S3); pin "$rev"
sed -i 's|代码: crates/b/src/y.rs|代码: crates/b/tests/never_written.rs|' "$tree/ledger.md"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "a phantom path in the code cell" >/dev/null
pin "$(git -C "$tree" rev-parse HEAD)"
out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "不存在"; } \
  && ok "代码格点名了树里不存在的路径：红" \
  || bad "凭空的代码格被放过了（rc=$rc）：$out"

# 台账读不到：退 2。
out=$(COGNEVA_EVENT_PSQL="$tmp/psql.sh" COGNEVA_TODO_LEDGER="$tmp/missing.md" python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main 2>&1); rc=$?
[ "$rc" -eq 2 ] && ok "台账读不到：退 2（没判）" || bad "台账读不到时的行为不对（rc=$rc）：$out"

# ---- 第 3 条理由判的是两侧的不对称，不是「这一批有没有碰 deploy/」-----------
#
# 这几组各用一本自己的台账：主台账要做到「这一批全做完」，多出来的不可回退条目
# 会把它变成红。
ledger_code="$tree/ledger-code.md"
ledger_irr="$tree/ledger-irr.md"
SPECS_IRR="S1=crates/a/src/x.rs S2=crates/b/src/y.rs S3=deploy/manifest.yaml S4=crates/a/migrations/001_x.up.sql"
long=$(printf '这一条要读的正文很长%.0s' $(seq 1 120))
ledger_long="$tree/ledger-long.md"

# 不可回退的那几条在**被排除**的一侧：这一批是纯代码，deploy/ 与迁移留到下一批。
reset; SPECS="$SPECS_IRR" rev_irr=$(write_ledger "$ledger_irr" S1 S2); pin "$rev_irr"
echo a6 >>"$tree/crates/a/src/x.rs"; echo b6 >>"$tree/crates/b/src/y.rs"
git -C "$tree" add -A >/dev/null
commit_with "irreversible side is the excluded one

Split: 3 — S3, S4 — 这一批是纯代码；deploy 与迁移要单独落地，回滚一次提交回不到原状"
out=$(run "$tree" "$ledger_irr"); rc=$?
[ "$rc" -eq 0 ] && ok "不可回退的那一侧被排除、这一批是纯代码：绿" || bad "正确的不对称被拒（rc=$rc）：$out"

# 两侧都碰：拆开并没有把不可回退的那一侧单独拎出来。
reset; SPECS="$SPECS_IRR" rev_irr=$(write_ledger "$ledger_irr" S1 S2); pin "$rev_irr"
echo a7 >>"$tree/crates/a/src/x.rs"; echo d7 >>"$tree/deploy/manifest.yaml"
git -C "$tree" add -A >/dev/null
commit_with "both sides irreversible

Split: 3 — S3, S4 — 两侧都有不可回退的东西"
out=$(run "$tree" "$ledger_irr"); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "两侧都碰"; } && ok "两侧都碰不可回退的面：红" || bad "两侧都碰却没红（rc=$rc）：$out"

# 改的是 deploy/ 下的一个**测试夹具**：它不改任何已部署的东西，回滚一次提交就回到
# 原状。按目录前缀一刀切会把它读成一次滚动，于是这条理由又能被借去排除别人。
reset; SPECS="S1=crates/a/src/x.rs S2=crates/b/src/y.rs" rev_code=$(write_ledger "$ledger_code" S1); pin "$rev_code"
echo a75 >>"$tree/crates/a/src/x.rs"; mkdir -p "$tree/deploy/scripts/tests"
printf '#!/bin/sh\n' >"$tree/deploy/scripts/tests/x.test.sh"
git -C "$tree" add -A >/dev/null
commit_with "a test fixture under deploy is not a rollout

Split: 3 — S2 — 这一批只是改了 deploy/ 下的夹具"
out=$(run "$tree" "$ledger_code"); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "两侧都没碰"; } && ok "deploy/ 下的夹具不算不可逆：理由不成立" || bad "夹具被当成滚动（rc=$rc）：$out"

# 两侧都不碰：没有「回滚一次提交回不到原状」的东西，理由不成立。
reset; SPECS="S1=crates/a/src/x.rs S2=crates/b/src/y.rs" rev_code=$(write_ledger "$ledger_code" S1); pin "$rev_code"
echo a8 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "neither side irreversible

Split: 3 — S2 — 两侧都是纯代码"
out=$(run "$tree" "$ledger_code"); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "两侧都没碰"; } && ok "两侧都不碰不可回退的面：红" || bad "两侧都不碰却没红（rc=$rc）：$out"

# 第 4 条理由要拿**整行正文**去比窗口份额：标题截断到 80 字时，最长的几条会被
# 估成同一个数，于是这条理由永远不成立。
reset
{ row S1 "crates/a/src/x.rs" "done"; printf '| S2 | %s | 未落 | 判据 |｜round: current｜touches: crates/b/src/y.rs\n' "$long"; } >"$ledger_long"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "long ledger" >/dev/null
pin "$(git -C "$tree" rev-parse HEAD)"
commit_with "too much to read in one window

Split: 4 — S2 — 这一条读不完"
out=$(run "$tree" "$ledger_long" --window-tokens 400); rc=$?
[ "$rc" -eq 0 ] && ok "第 4 条：正文超过窗口份额的一半，成立" || bad "第 4 条合格却仍被拒（rc=$rc）：$out"
out=$(run "$tree" "$ledger_long" --window-tokens 8000); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "窗口份额"; } && ok "第 4 条：正文远小于窗口份额，红" || bad "第 4 条不合格却没红（rc=$rc）：$out"

# ---- 提交那一刻的门（`pre-commit.d/55-batch-edited-before-commit.sh`） ----
#
# 判的是另一件事：**这一遍编辑走完了没有**。planner／generator／evaluator 是同一个
# 模型在扮，流程对它唯一的约束就落在它能自己敲的 `git add` / `git commit` 上——
# 「改一条提交一条」与「整批改完再编译一次」在读数上完全同形，这条门把那句话变成事实。
# 每一格都先 reset 再放钩子：`write_ledger` 会 `git add -A` 把钩子一起提交进去，
# 下一次 `reset --hard` 就把它删了——那样「没拒」读起来像门坏了，其实是夹具没了。
arm_hook() {
  hook="$tree/scripts/git-hooks/pre-commit.d/55-batch-edited-before-commit.sh"
  mkdir -p "$(dirname "$hook")"
  cp "$root/scripts/git-hooks/pre-commit.d/55-batch-edited-before-commit.sh" "$hook"
  # 钩子跑的是 `family.py`；它在夹具里没有就退 0，而「没拒」与「都改完了」同形。
  cp "$root/scripts/hooks/family.py" "$tree/scripts/hooks/family.py"
}

run_hook() { ( cd "$tree" && COGNEVA_TODO_LEDGER="$ledger" bash "$hook" ) 2>&1; }

# 一条都没动：拒，并把两条都点名。
reset; write_ledger "$ledger" >/dev/null; arm_hook
out=$(run_hook); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "S1" && printf '%s' "$out" | grep -q "S2"; } \
    && ok "批没动：提交被拒，并逐条点名" \
    || bad "批没动却放行（rc=$rc）：$out"

# 只改了一半：仍然拒，点名没动的那一条——改了一半不是做完。
reset; write_ledger "$ledger" >/dev/null; arm_hook
echo half >>"$tree/crates/a/src/x.rs"
out=$(run_hook); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "S2"; } \
    && ok "改了一半：仍拒，点名没动的那一条" \
    || bad "改了一半却放行（rc=$rc）：$out"

# 触点全动过：放行。这条与编译垫片问的是同一个问题、走的是同一段代码。
reset; write_ledger "$ledger" >/dev/null; arm_hook
echo half >>"$tree/crates/a/src/x.rs"; echo half >>"$tree/crates/b/src/y.rs"; echo half >>"$tree/deploy/manifest.yaml"
out=$(run_hook); rc=$?
{ [ "$rc" -eq 0 ] && ! printf '%s' "$out" | grep -q "^TODO "; } \
    && ok "整批都动过：放行" \
    || bad "整批都动过却仍被拒（rc=$rc）：$out"

printf '\nbatch-gate.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
