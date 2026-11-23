#!/usr/bin/env bash
# 五格台账（`序号 | 终态 | 现在 | 差距 | 状态`）也要能被**收工那一问**读懂。
#
# 形如 `| CC01 | 终态 | 现在 | 差距 | 未落 |` 的行，判终态的格子是它自己写的「差距」：
# 差距非空 ⇒ 未落，差距空且状态 已落 ⇒ 终态。但 `load()` 只给「未完成」那一侧，做完的行
# 从那一侧消失；不查「它还在不在台账里」的话，读完的行读成「未完成」，于是**每一轮都
# 关不掉**——这一族最贵的形状：收尾的动作被读成没做完。
#
# 判六件事：
#   1. 未落的五格行：status 退 1，缺口写的是「差距」，不是三格那套；
#   2. 改成 `已落`＋空差距：status 退 0、点名「已落 CC01」，`close` 关得掉；
#   3. 被钉住的行从台账里删掉：status 退 1 并点名「台账里也没有这一行」（删件 ≠ 收尾）；
#   4. 写着 `touches: unknown` 的条目不进批次（`open` 明写本轮不排，不钉它）；
#   5. 状态词不在闭集里的五格行：点名拒读，不静默跳过；
#   6. 列数不足的五格行：同样点名拒读。
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
round="$root/scripts/hooks/round.py"
todo="$root/scripts/hooks/todo_source.py"
[ -f "$round" ] || { echo "FAIL: round.py 不在：$round" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/scripts/hooks"
cp "$round" "$tree/scripts/hooks/round.py"
cp "$todo" "$tree/scripts/hooks/todo_source.py"
git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
# 事件流替身：走 git 书时一次都不该被调用。
cat >"$tmp/psql.sh" <<STUB
#!/bin/sh
: >"$tmp/psql-was-called"
exit 2
STUB
chmod +x "$tmp/psql.sh"
export COGNEVA_TODO_SOURCE="md:$tree/ledger.md"
export COGNEVA_EVENT_PSQL="$tmp/psql.sh"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
run() { python3 "$tree/scripts/hooks/round.py" "$@" 2>&1; }

# 一行五格台账：`touches:` 写在某一格**里面**（这里是「现在」那一格），它不是一个
# 独立的列——把触点当第六列加在行尾，读出来的状态词就会是「｜ touches: …」。
row() { # $1 = 状态, $2 = 差距
  printf '| CC01 | 把读数面的失败留下 | 两处已收口 ｜ touches: crates/a/src/x.rs | %s | %s |\n' "$2" "$1"
}

# 1. 未落 ⇒ 退 1，缺口是「差距」。
row 未落 "余下一处" >"$tree/ledger.md"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base
out=$(run open); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "CC01"; } && ok "open 钉住了这条五格行" || bad "open 没钉住（rc=$rc）：$out"
out=$(run status); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "缺 差距" && ! printf '%s' "$out" | grep -q "缺 代码"; } \
  && ok "未落的五格行：退 1，缺的是它自己那一格「差距」" \
  || bad "未落五格行的判词不对（rc=$rc）：$out"

# 2. 已落＋空差距 ⇒ 终态，关得掉。
row 已落 "—" >"$tree/ledger.md"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "close CC01" >/dev/null
out=$(run status); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "已落 CC01"; } \
  && ok "已落＋差距空：status 退 0，点名已落" \
  || bad "做完的条目没读成终态（rc=$rc）：$out"
out=$(run close); rc=$?
if [ "$rc" -eq 0 ]; then
  out=$(run status); rc=$?
  { [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "没有开着的一轮"; } \
    && ok "关掉之后：读作「没有开着的一轮」" \
    || bad "关掉之后的 status 判词不对（rc=$rc）：$out"
else
  bad "close 没关掉这一轮（rc=$rc）：$out"
fi
[ ! -e "$tmp/psql-was-called" ] && ok "全程没有碰事件流" || bad "走 git 书时调用了事件流"

# 3. 钉住的行被从台账里删掉 ⇒ 退 1 并点名，不读成收尾。
row 未落 "余下一处" >"$tree/ledger.md"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "reopen row" >/dev/null
run open >/dev/null
git -C "$tree" rm -q ledger.md >/dev/null; git -C "$tree" commit -qm "drop the row" >/dev/null
printf '| S1 | 另一条 | 未落 | touches: crates/a/src/x.rs\n' >"$tree/ledger.md"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "a different ledger" >/dev/null
out=$(run status); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "台账里也没有这一行"; } \
  && ok "行被删掉：退 1 并点名，不读成收尾" \
  || bad "删掉的行被判成收尾了（rc=$rc）：$out"
# 这一轮关不掉（正确行为），下一个场景要干净的书：直接把 ref 删掉。
git -C "$tree" update-ref -d refs/cogneva/round

# 4. `touches: unknown` 不排进批次。
printf '| CC01 | 长期条目 | 已收口两处 ｜ touches: unknown — 长期：按面开新行 | 余下逐面 | 未落 |\n| CC02 | 这一批要做的 | 还没动 ｜ touches: crates/b/src/y.rs | 改三处 | 未落 |\n' >"$tree/ledger.md"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "one deferred, one scheduled" >/dev/null
out=$(run open); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "本轮不排" && printf '%s' "$out" | grep -q "CC01" \
   && printf '%s' "$out" | grep -q "CC02"; } \
  && ok "unknown 的那条不排进批次，明写本轮不排" \
  || bad "deferred 处理不对（rc=$rc）：$out"
python3 - "$tree" <<'PY' && ok "批次载荷里只有 CC02" || bad "批次载荷里混进了不排的条目"
import json, subprocess, sys
tree = sys.argv[1]
payload = subprocess.run(["git", "-C", tree, "cat-file", "-p", "refs/cogneva/round"],
                         capture_output=True, text=True).stdout
batch = json.loads(payload).get("batch")
sys.exit(0 if batch == ["CC02"] else 1)
PY

# 5. 状态词不在闭集里：按旧格式略过，但**点名印出来**，不静默。
#    真台账里还留着几代以自由文字作状态的表格（「部分已落；剩余移交」），所以这一支
#    不能整本拒读；但一个打错字的 `已落` 也不能就这么消失。
printf '| CC01 | 内容 | 现在 ｜ touches: crates/a/src/x.rs | 差距 | 进行中 |\n' >"$tree/bad-status.md"
out=$(python3 - "$tree" <<'PY' 2>&1
import sys
sys.path.insert(0, sys.argv[1] + "/scripts/hooks")
import todo_source
try:
    items = todo_source.load("md:" + sys.argv[1] + "/bad-status.md")
except todo_source.Unreadable as e:
    print("REFUSED", e); sys.exit(1)
print("IDS", [i.id for i in items])
PY
) && printf '%s' "$out" | grep -q "按旧格式略过" && printf '%s' "$out" | grep -q "CC01" \
  && printf '%s' "$out" | grep -q 'IDS \[\]' \
  && ok "状态词不在闭集：略过但点名，不进批次" || bad "坏状态词被静默跳过：$out"

# 6. 末格是状态词、却只有四格：五格行少了一列，同样点名拒读。
#
# 同一本台账里还留着更早的 `| id | 一句话 | 状态 | 证据 |` 四列表格（历史现场）：
# 它们的末格是路径，不是状态词，不能被当成「坏掉的五格行」——那样整本台账都会拒读。
printf '| CC01 | 内容 | 现在 | 未落 |\n' >"$tree/short.md"
out=$(python3 - "$tree" <<'PY'
import sys
sys.path.insert(0, sys.argv[1] + "/scripts/hooks")
import todo_source
try:
    todo_source.load("md:" + sys.argv[1] + "/short.md")
except todo_source.Unreadable as e:
    print(e); sys.exit(0)
print("LOADED")
sys.exit(1)
PY
) && printf '%s' "$out" | grep -q "CC01" \
  && ok "列数不足：点名拒读" || bad "短行被静默跳过：$out"

printf '\nround-close-five-field.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
