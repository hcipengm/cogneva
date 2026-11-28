#!/usr/bin/env bash
# `check-claims.sh` 的离线测试：夹具是一棵小仓，提交信息是唯一的输入。
#
# 判的是三句话有没有出处：
#   1. 说「修好了」没有 `Reading:` ⇒ 红；有 ⇒ 绿。
#   2. 引了「第 N 条 / 原则」没有 `Rule:` ⇒ 红；有 ⇒ 绿。
#   3. 声称「普查完备」没有 `Census:` ⇒ 红；有 ⇒ 绿。
#   4. 什么都没说 ⇒ 绿（门只判声明，不判沉默）。
set -uo pipefail
# 夹具仓通过 `git -C <dir>` 或子 shell 的 `cd` 写，两者都不改调用方的仓：可是外层
# 带进来的绝对 `GIT_DIR` / `GIT_WORK_TREE` / `GIT_INDEX_FILE` 会让每一次调用写进**调用方**
# 的仓。一次清干净，文件作用域，后面的用例都覆盖。
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
gate="$root/deploy/scripts/check-claims.sh"
[ -f "$gate" ] || { echo "FAIL: 门脚本不在：$gate" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/deploy/scripts"
cp "$gate" "$tree/deploy/scripts/check-claims.sh"
git -C "$tree" init -q
git -C "$tree" config user.email t@t; git -C "$tree" config user.name t
echo base >"$tree/f"; git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base
git -C "$tree" branch -f origin/main HEAD

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

check() { # $1=提交信息文本 $2=期望退出码 $3=说明
  echo x >>"$tree/f"
  git -C "$tree" commit -q --allow-empty -F - < <(printf '%s\n' "$1")
  COGNEVA_PUSH_BASE=origin/main bash "$tree/deploy/scripts/check-claims.sh" >/dev/null 2>&1
  rc=$?
  if [ "$rc" -eq "$2" ]; then ok "$3"; else bad "$3（期望 $2，实得 $rc）"; fi
  git -C "$tree" reset -q --hard HEAD^
}

check "Fix the writer

修好了" 1 "说修好了却没给读数：红"
check "Fix the writer

修好了
Reading: cogneva_task_events_rows 41 → 41" 0 "说修好了并给出读数：绿"
check "Justify the split

按第 4 条拆" 1 "引规则不给出处：红"
# 形容词不是声明。2026-11-20 实测：五笔功能提交因为 `a fixed batch`、`no fixed subject`
# 这类用法被拒，而它们一笔也没在说「修好了」。判据要按出现形式判，不按裸子串判。
check "Pin the batch at a fixed point

the batch had no fixed subject to ask about" 0 "fixed 作形容词：不是声明，绿"
check "Hand the loop a fixed batch" 0 "标题里的 fixed batch：不是声明，绿"
check "Fixed the resume point

steps are read in order" 1 "行首的动词 fixed：是声明，没读数就红"

check "Justify the split

按第 4 条拆
Rule: specs/work-ledger-b6f42e.md S52" 0 "引规则并给出处：绿"
check "Census is complete

普查已经完备" 1 "声称完备却没跑：红"
check "Census is complete

普查已经完备
Census: bounds-census.py 791/592/569/334" 0 "声称完备并给出四面计数：绿"
check "Add a test" 0 "什么都没说：绿（门只判声明）"
check "Describe the rule

提交信息用「」引用那个词时不算声明" 0 "引号里的词是提到，不是声明"

printf '\nclaims.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
