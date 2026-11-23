#!/usr/bin/env bash
# `scripts/hooks/check-cargo-path.sh`：装完之后，PATH 上第一个 cargo 到底是不是转发器。
#
# 这一面要判的正是「装好了」与「装了但从不被调用」同形的那一格：
#   1. 首位是转发器 ⇒ 退 0；
#   2. 首位是真 cargo（rustup 那种）⇒ 退 1，并点名它排在哪；
#   3. PATH 上没有 cargo ⇒ 退 3（没量到，不是判过）。
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
chk="$root/scripts/hooks/check-cargo-path.sh"
[ -f "$chk" ] || { echo "FAIL: 检查脚本不在：$chk" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 转发器的形状：与 `scripts/hooks/install.sh` 写出来的那一段同构。
mkdir -p "$tmp/fwd" "$tmp/real" "$tmp/none"
cat >"$tmp/fwd/cargo" <<'SHIM'
#!/usr/bin/env bash
real_cargo="/usr/bin/true"
shim="${toplevel:+$toplevel/scripts/hooks/cargo}"
if [ -n "$shim" ]; then
  REAL_CARGO="$real_cargo" exec bash "$shim" "$@"
fi
exec "$real_cargo" "$@"
SHIM
cat >"$tmp/real/cargo" <<'REAL'
#!/bin/sh
exec /usr/bin/true "$@"
REAL
chmod +x "$tmp/fwd/cargo" "$tmp/real/cargo"

out=$(PATH="$tmp/fwd:$tmp/real:/usr/bin:/bin" bash "$chk" 2>&1); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "会被调用"; } \
  && ok "首位是转发器：退 0" || bad "首位是转发器却没认出来（rc=$rc）：$out"

out=$(PATH="$tmp/real:$tmp/fwd:/usr/bin:/bin" bash "$chk" 2>&1); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "$tmp/real"; } \
  && ok "首位是真 cargo：退 1，并点名它" || bad "首位是真 cargo 时的判词不对（rc=$rc）：$out"

out=$(PATH="$tmp/none" /usr/bin/env -i PATH="$tmp/none" /bin/bash "$chk" 2>&1); rc=$?
[ "$rc" -eq 3 ] && ok "PATH 上没有 cargo：退 3（没量到）" \
  || bad "没有 cargo 时的判词不对（rc=$rc）：$out"

printf 'check-cargo-path.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
