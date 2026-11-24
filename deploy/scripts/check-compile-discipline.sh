#!/usr/bin/env bash
# 编译纪律：这一推的内容，是不是在最后一次编辑之后编译过；这一批编译了几次。
#
# 判据只有两条，都读同一个账本（宿主上的 cargo 垫片每调用一次记一行）：
#
#   1. **编辑之后必须编译**。这一推涉及的文件里，最新的一次修改时间晚于这棵树上
#      最后一次编译 ⇒ 没编译过就推，拒。这条就是「等全部编辑做完再编译」的机械形态：
#      改早了、改晚了都一样，编辑在编译之后就是没编译。
#   2. **一批只编译一次**。自这一批最早的提交以来编译了不止一次 ⇒ 提交信息里要有
#      `Recompiled: <为什么第一次不够>`。没有就拒。
#
# 账本读不到时**不判**，也不假绿：这台机器没装垫片，量不到不等于零次。要装：
# `bash scripts/hooks/install.sh`。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="${1:-$(cd "$here/../.." && pwd)}"
ledger="${COGNEVA_COMPILE_LEDGER:-$HOME/.local/state/cogneva/compile-ledger.tsv}"
bin="${COGNEVA_BIN:-$root/target/debug/cogneva}"

if [ ! -x "$bin" ]; then
  echo "compile-discipline: 平台二进制不在：$bin —— 没判。" >&2
  exit 3
fi

if [ ! -f "$ledger" ]; then
  echo "compile-discipline: 这台机器没有编译账本（未装垫片），这一推没判。" >&2
  echo "                  安装：bash scripts/hooks/install.sh" >&2
  exit 3
fi

base="${COGNEVA_PUSH_BASE:-origin/main}"
if ! git -C "$root" rev-parse --verify "$base" >/dev/null 2>&1; then
  echo "compile-discipline: 找不到基线 $base，这一推没判。" >&2
  exit 3
fi

"$bin" ledger compiled --root "$root" --base "$base" --ledger "$ledger"
