#!/usr/bin/env bash
# principle: feedback_a_gate_whose_subject_moved_keeps_grading_the_old_path
#
# 「有没有一条续跑循环在跑」这个问题，三份脚本各写了一份 `pgrep -f` 模式：值守用它决定
# 起不起一轮，探针与定点起跑用它决定要不要让位。模式里带一段目录路径，而这三个脚本随
# 评测宿主从私有仓的 `scripts/host/` 搬到了公开仓的 `scripts/eval/`——搬完之后，还写着
# 旧目录的那两份匹配不到任何在跑的循环，判词从「有一条在跑」变成「一条也没有」。
#
# 那条判据失效时不报错、不告警，只是让位不再发生：窗口到了起第二条循环，两条抢同一份
# checkpoint 的单写者锁，整轮 bail。所以这里不判「模式长得对不对」，判**它认不认这一族
# 脚本真正会出现的命令行**——拿当前树上这个脚本自己的路径造一行。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

pass=0
fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 一份文件里那条「有没有循环在跑」的模式，原样取出来。
#
# 只看**代码行**：正文里也引着这条模式的旧写法（解释为什么锚在 bash 上），先取到注释
# 那一份就会把判据判成注释本身——它连 `git commit` 都算在跑。
pattern_of() { # $1 = 文件
  local p
  p=$(sed -n "s/^[[:space:]]*\(if[[:space:]]*\)\{0,1\}pgrep -f '\([^']*\)'.*/\2/p" "$1" | head -1)
  # 值守不把模式写在调用处，而是先赋给 `pattern`，再 `pgrep -f "$pattern"`。
  [ -n "$p" ] || p=$(sed -n "s/^pattern='\([^']*\)'.*/\1/p" "$1" | head -1)
  printf '%s' "$p"
}

FILES=(
  "$HERE/supervise-eval-loop.sh"
  "$HERE/eval-probe-window.sh"
  "$HERE/eval-rerun-at-window.sh"
)

first=""
for f in "${FILES[@]}"; do
  name=$(basename "$f")
  pat=$(pattern_of "$f")
  if [ -z "$pat" ]; then
    bad "$name：读不出那条模式（这一行的形状变了？）——读不出来本身就是让位失效"
    continue
  fi
  if [ -z "$first" ]; then
    first=$pat
  elif [ "$pat" != "$first" ]; then
    bad "$name 的模式与 supervise-eval-loop.sh 不同：两处答复同一个问题，就会有一处在搬家后失明"
  else
    ok "$name 与守在同一个问题上用同一个模式"
  fi

  # 这一族真正会出现的命令行：值守用绝对路径起它、带参数；手工起也一样。
  for cmd in \
    "bash $HERE/eval-registered-rerun.sh --target 198 --tools platform" \
    "bash /home/runner/work/cogneva/cogneva/scripts/eval/eval-registered-rerun.sh --target 198"; do
    if printf '%s\n' "$cmd" | grep -Eq "$pat"; then
      ok "$name 认得这一行：「$cmd」"
    else
      bad "$name 认不出这一行：「$cmd」——它会把在跑的循环读成没在跑，窗口到了再起一条"
    fi
  done

  # 反方向：排队中的提交把路径写在参数里，不能当成循环在跑（2026-10-19 实测误报）。
  for cmd in \
    "git commit -m moved scripts/eval/eval-registered-rerun.sh" \
    "bash scripts/other.sh eval-registered-rerun.sh"; do
    if printf '%s\n' "$cmd" | grep -Eq "$pat"; then
      bad "$name 把这一行当成循环在跑：「$cmd」"
    else
      ok "$name 不把这一行当成循环在跑"
    fi
  done
done

printf '%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
