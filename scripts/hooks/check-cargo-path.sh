#!/usr/bin/env bash
# 装完之后问一句：PATH 上第一个 `cargo`，是不是那个转发器？
#
# 为什么需要它：转发器装在 `~/.local/bin` 与 `/usr/local/bin`，而 rustup 把真的 cargo
# 放在 `~/.cargo/bin`——它在 PATH 上常常排在前面。于是「装好了」与「装了但从不被调用」
# 在键盘上完全是同一个样子，而后者意味着记账永远是空的、编译纪律门永远读不到数。
#
# 判据只看一件事：`command -v cargo` 指到的那个文件里，有没有转发器的两处标记
# （`scripts/hooks/cargo` 与 `REAL_CARGO`）。
#   退 0 = 首位就是转发器，装了会被调用；
#   退 1 = 首位不是转发器——装了也不会被调用；
#   退 3 = 量不出（PATH 上没有 cargo，或那个文件读不了）：没量到不是判过。
set -uo pipefail

first="$(command -v cargo 2>/dev/null || true)"
if [ -z "$first" ]; then
  echo "check-cargo-path: PATH 上没有 cargo —— 这一条没量（不是判过）" >&2
  exit 3
fi
if [ ! -r "$first" ]; then
  echo "check-cargo-path: 读不了 $first —— 这一条没量（不是判过）" >&2
  exit 3
fi

has_shim=0
has_real=0
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    *scripts/hooks/cargo*) has_shim=1 ;;
    *REAL_CARGO*) has_real=1 ;;
  esac
done <"$first"

if [ "$has_shim" -eq 1 ] && [ "$has_real" -eq 1 ]; then
  echo "check-cargo-path: PATH 首位 cargo = $first（转发器，会被调用）"
  exit 0
fi

printf 'check-cargo-path: PATH 首位 cargo 是 %s，不是转发器。\n' "$first" >&2
printf '  垫片装了也不会被调用，compile-ledger 会一直是空的（编译纪律门因此永远判不了）。\n' >&2
printf '  让它被调用：把转发器所在目录排在 %s 之前，或把 cargo 垫片装到 PATH 首位那一处。\n' \
  "$(dirname "$first")" >&2
exit 1
