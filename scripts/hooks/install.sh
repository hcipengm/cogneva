#!/usr/bin/env bash
# 把这台机器接进「默认全量编辑」的机制：装一个 cargo 垫片。
#
# 机制全在仓库里，只有一件事必须在机器上落地——**编译要被看见**。垫片做两件事：
#
#   1. 记一行：时间、工作树、参数、HEAD、树指纹（`compile-ledger.py` 读它）；
#   2. 在脏树上编译前问 `family.py --all-todos`：这一轮的默认批次还有哪些文件没改。
#      有缺口就不编译，并把缺口打出来。
#
# 不需要装 git 钩子：判据在推之前那道门上（`deploy/scripts/check-compile-discipline.sh`），
# 它由 `check-before-push.sh` 调用，而推之前跑那道门本来就是规矩。
#
# 幂等；不覆盖非本脚本写过的东西。卸载：删掉两个 cargo 垫片即可。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
real_cargo="${REAL_CARGO:-$HOME/.cargo/bin/cargo}"

[ -x "$real_cargo" ] || { echo "install-hooks: 找不到 cargo：$real_cargo（可用 REAL_CARGO= 指定）" >&2; exit 1; }
mkdir -p "$HOME/.local/state/cogneva"

install_shim() {
  local target="$1"
  mkdir -p "$(dirname "$target")"
  sed "s#^real_cargo=\${REAL_CARGO:-\$HOME/.cargo/bin/cargo}#real_cargo=\"$real_cargo\"#" \
    "$here/cargo" >"$target"
  chmod 755 "$target"
  echo "install-hooks: $target"
}

install_shim "$HOME/.local/bin/cargo"
if [ -w /usr/local/bin ]; then
  install_shim /usr/local/bin/cargo
elif sudo -n true 2>/dev/null; then
  tmp="$(mktemp)"
  sed "s#^real_cargo=\${REAL_CARGO:-\$HOME/.cargo/bin/cargo}#real_cargo=\"$real_cargo\"#" "$here/cargo" >"$tmp"
  sudo -n install -m 755 "$tmp" /usr/local/bin/cargo && echo "install-hooks: /usr/local/bin/cargo"
  rm -f "$tmp"
else
  echo "install-hooks: 跳过 /usr/local/bin（没有写权限）；此时 ~/.local/bin 必须在 PATH 上" >&2
fi

echo "install-hooks: 完成。账本：${COGNEVA_COMPILE_LEDGER:-$HOME/.local/state/cogneva/compile-ledger.tsv}"
echo "install-hooks: 待办台账路径由 COGNEVA_TODO_LEDGER 指定（默认 $HOME/.local/state/cogneva/todo-ledger.md）"
