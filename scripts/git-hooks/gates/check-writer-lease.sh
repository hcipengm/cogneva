#!/usr/bin/env bash
# 写者租约的门：租约有效时，只有持有者能往这棵树写。
#
# 判据只有三条，都来自同一份状态（writer-lease.sh show 的那份）：
#   1. 没有租约、或租约已过期  → 放行（没人接管，正常协作）
#   2. 本进程声明的写者身份 == 持有者 → 放行
#   3. 其余（含**没声明身份**的会话）→ 拒绝，拒词里带持有者、到期时间、理由
#
# 「没声明身份」为什么算拒绝而不是放行：带外会话恰恰是没设 COGNEVA_WRITER 的那些进程；
# 放行它们等于租约只挡自己人。写者身份由宿主单元声明（Environment=COGNEVA_WRITER=<名字>），
# 本会话是哪一个写者是**声明出来的**，不是从 pid 猜的。
#
# 出口码：0 放行；1 拒绝（租约在别人手上）；2 用法错；3 **读不回来**（文件不是合法 JSON）。
# 3 不许当 0：状态读不回来却放行，等于把「门瞎了」记成「没人持有」。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 本脚本在 `scripts/gates/`，租约工具在 `scripts/`：按仓根取，不数层数。
ROOT="$(env -u GIT_DIR -u GIT_WORK_TREE git -C "$HERE" rev-parse --show-toplevel 2>/dev/null)" || ROOT="$(cd "$HERE/../.." && pwd)"
LEASE="$ROOT/scripts/writer-lease.sh"
actor="${COGNEVA_WRITER:-}"

[ -f "$LEASE" ] || { echo "写者租约：找不到 $LEASE（判据缺席）" >&2; exit 3; }

state="$(bash "$LEASE" show 2>&1)"; rc=$?
[ "$rc" -eq 3 ] && { echo "写者租约：状态读不回来——$state" >&2; exit 3; }

case "$state" in
  *"空闲"*) exit 0 ;;
  *"已过期"*) exit 0 ;;
esac

holder="$(printf '%s\n' "$state" | sed -n 's/^持有者 \([^（]*\)（.*/\1/p' | head -1)"
expiry="$(printf '%s\n' "$state" | sed -n 's/^到期 \([^（]*\)（.*/\1/p' | head -1)"
why="$(printf '%s\n' "$state" | sed -n 's/^理由 //p' | head -1)"

if [ -n "$actor" ] && [ "$actor" = "$holder" ]; then
  exit 0
fi
if [ -z "$actor" ]; then
  printf '写者租约拒绝：本会话没有声明写者身份（COGNEVA_WRITER），租约在 %s 手上直到 %s（%s）。\n' \
    "$holder" "$expiry" "$why" >&2
else
  printf '写者租约拒绝：%s 不持有租约，租约在 %s 手上直到 %s（%s）。\n' \
    "$actor" "$holder" "$expiry" "$why" >&2
fi
exit 1
