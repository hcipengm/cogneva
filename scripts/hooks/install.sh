#!/usr/bin/env bash
# 把这台机器接进「默认全量编辑」的机制：装一个 cargo 垫片。
#
# 机制全在仓库里，只有一件事必须在机器上落地——**编译要被看见**。垫片做两件事：
#
#   1. 记一行：时间、工作树、参数、HEAD、树指纹（`cogneva ledger compiled` 读它）；
#   2. 在脏树上编译前问 `cogneva ledger editing`：这一轮的默认批次还有哪些文件没改。
#      有缺口就不编译，并把缺口打出来。
#
# 不需要装 git 钩子：判据在推之前那道门上（`deploy/scripts/check-compile-discipline.sh`），
# 它由 `check-before-push.sh` 调用，而推之前跑那道门本来就是规矩。
#
# 幂等；不覆盖非本脚本写过的东西。卸载：删掉两个 cargo 垫片即可。
set -euo pipefail

real_cargo="${REAL_CARGO:-$HOME/.cargo/bin/cargo}"

[ -x "$real_cargo" ] || { echo "install-hooks: 找不到 cargo：$real_cargo（可用 REAL_CARGO= 指定）" >&2; exit 1; }
mkdir -p "$HOME/.local/state/cogneva"

# 装的是**转发器**，不是副本。
#
# 副本是第二个正本：仓库里那份改了，机器上那份不会动，于是「门改了」与「门在跑」在
# 读数上同形——2026-11-20 实测，机器上跑的是 11-19 14:33 的副本，一整天改的门一次都
# 没执行过。转发器只回答一个问题：「当前工作树里有没有这份垫片」，有就 exec 它。
install_shim() {
  local target="$1"
  local real="$2"
  mkdir -p "$(dirname "$target")"
  cat >"$target" <<SHIM
#!/usr/bin/env bash
# 转发器：真正的垫片在仓库里（\`scripts/hooks/cargo\`），这里只找它并 exec。
# 不在任何工作树里时直接交给真的 cargo。机器上不留第二份逻辑。
real_cargo="$real"
toplevel="\$(git rev-parse --show-toplevel 2>/dev/null || true)"
shim="\${toplevel:+\$toplevel/scripts/hooks/cargo}"
if [ -n "\$shim" ] && [ -f "\$shim" ] && [ "\$shim" != "\$0" ]; then
  REAL_CARGO="\$real_cargo" exec bash "\$shim" "\$@"
fi
exec "\$real_cargo" "\$@"
SHIM
  chmod 755 "$target"
  echo "install-hooks: $target（转发到工作树里的 scripts/hooks/cargo）"
}

install_shim "$HOME/.local/bin/cargo" "$real_cargo"
if [ -w /usr/local/bin ]; then
  install_shim /usr/local/bin/cargo "$real_cargo"
elif sudo -n true 2>/dev/null; then
  tmp="$(mktemp)"
  install_shim "$tmp" "$real_cargo" >/dev/null
  sudo -n install -m 755 "$tmp" /usr/local/bin/cargo && echo "install-hooks: /usr/local/bin/cargo（转发）"
  rm -f "$tmp"
else
  echo "install-hooks: 跳过 /usr/local/bin（没有写权限）；此时 ~/.local/bin 必须在 PATH 上" >&2
fi

# 装完再问一句：PATH 上第一个 cargo 是不是刚才装的那个转发器？
#
# 不装这一问，本脚本只有一个结论——「装好了」；而 rustup 的 `~/.cargo/bin` 常常排在
# 前面，垫片一次也不会被调用，compile-ledger 永远是空的。两种情况的后果相反，
# 读数却同形，所以判据要从「装没装」换成「PATH 首位认不认它」。
#
# 退 3 = 量不出（PATH 上没有 cargo）；退 1 = 首位不是转发器。两种都不算装成。
if ! check_out="$(bash "$(dirname "$0")/check-cargo-path.sh" 2>&1)"; then
  printf '%s\n' "$check_out" >&2
  echo "install-hooks: 没装成——这一问的答案不是「会被调用」。修好 PATH 顺序后重跑。" >&2
  exit 3
fi
printf '%s\n' "$check_out"

echo "install-hooks: 完成。账本：${COGNEVA_COMPILE_LEDGER:-$HOME/.local/state/cogneva/compile-ledger.tsv}"
echo "install-hooks: 待办台账路径由 COGNEVA_TODO_LEDGER 指定（默认 $HOME/.local/state/cogneva/todo-ledger.md）"
