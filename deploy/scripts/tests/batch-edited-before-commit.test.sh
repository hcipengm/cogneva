#!/usr/bin/env bash
# 提交时那道批次门的失败标本：它自己也得会红。
#
# 判的是「改一条提交一条」在这道门上长什么样：这一轮钉着的批里还有待办没被触到，
# 任何提交都停下；批改完了才放行。三个方向各一格，缺一格这道门就可能是一枚橡皮
# 图章——一个「永远放行」的实现也能让「批改完了就放行」那一格绿。
#
#   - 判词是「还没改完」（退 1）⇒ 提交必须被拒（退 1），并且把没改的那几条印出来；
#   - 判词是「改完了」（退 0）⇒ 放行（退 0）；
#   - 判不了（退 2）或没有平台二进制 ⇒ 不阻，但要印一行说它没判——
#     「判不了」不许表现成「通过」，也不许把一个不做这件事的人拦下。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
hook="$root/scripts/git-hooks/pre-commit.d/55-batch-edited-before-commit.sh"
[ -f "$hook" ] || { echo "FAIL: 批次门不在：$hook" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/target/debug"
git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
echo hi >"$tree/a.txt"
git -C "$tree" add -A >/dev/null
git -C "$tree" commit -qm base

bin="$tree/target/debug/cogneva"
cat >"$bin" <<'FAKE'
#!/usr/bin/env bash
# 假平台：只答 `ledger editing`，退出码与正文由环境给。
if [ "$*" = "ledger editing" ]; then
    printf '%s\n' "${FAKE_OUT:-}"
    exit "${FAKE_RC:-0}"
fi
exit 2
FAKE
chmod +x "$bin"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

run() { ( cd "$tree" && bash "$hook" 2>&1 ); }

# 1. 批没改完：必须拒，并且点名没改的那几条（不是只印一句「不行」）。
out=$(FAKE_RC=1 FAKE_OUT='TODO S1（触点 crates/a 一处都没动）' run)
rc=$?
if [ "$rc" -eq 1 ]; then ok "批没改完时提交被拒"; else bad "批没改完却放行了（exit $rc）"; fi
case "$out" in
    *"S1"*) ok "拒绝时点名了没改的那一条" ;;
    *) bad "拒绝时没有点名是哪一条没改：$out" ;;
esac

# 2. 批改完了：放行，且不再印拒绝的正文。
out=$(FAKE_RC=0 run)
rc=$?
if [ "$rc" -eq 0 ]; then ok "批改完了时放行"; else bad "批改完了仍被拒（exit $rc）：$out"; fi

# 3. 判不了（退 2）：不阻，但要说它没判。
out=$(FAKE_RC=2 FAKE_OUT='台账读不到' run)
rc=$?
if [ "$rc" -eq 0 ]; then ok "判不了时没有把提交拦下"; else bad "判不了时拦了提交（exit $rc）"; fi
case "$out" in
    *判不了*) ok "判不了时说明了这一问没判" ;;
    *) bad "判不了时没有说话，读的人会当成通过：$out" ;;
esac

# 4. 没有平台二进制：不阻，但要说这一问没判。
mv "$bin" "$bin.away"
out=$(run)
rc=$?
if [ "$rc" -eq 0 ]; then ok "没有二进制时没有把提交拦下"; else bad "没有二进制时拦了提交（exit $rc）"; fi
case "$out" in
    *没有平台二进制*|*没判*) ok "没有二进制时说明了这一问没判" ;;
    *) bad "没有二进制时静默放行：$out" ;;
esac
mv "$bin.away" "$bin"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
