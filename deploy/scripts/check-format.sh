#!/usr/bin/env bash
# CI 的 Format 作业逐字同一条命令，搬到这里判一次。
#
# 为什么这条要单独成门：Format 是 main 上的必过检查之一，而它在本地原本没有任何调用点。
# 一笔 rustfmt 没排过的改动推上去，整条 CI 要跑二十分钟才红，红的还是「空格与换行」——
# 十秒钟就能在本地判出来的一件事，代价却是一轮 CI，而且 CI 红期间主线部署器读到
# verdict=fail 就把整班滚动扣住，部署停在那里等这一笔重推。
#
# 判的是**工作树**，不是 HEAD：这条门最常被问的一刻是「我正要提交」那一问，而那一刻
# 要提交的字节还在工作树里、没进 HEAD。共享树里别人的在飞改动会让这条红一次，那是
# 多判，不是假绿——与 Test 那一格同一个取舍。
#
# 用法：bash deploy/scripts/check-format.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

# Cargo 的找法与 `check-before-push.sh` 同一套：PATH 上找不到就退到 `$HOME/.cargo/bin`。
#
# 少了这一条回退，本机会出现一种最难读的红：`cargo clippy` 那一格（用绝对路径）刚跑过，
# 紧接着这一格印「缺少依赖：cargo」——同一台机器上一步说 cargo 在、下一步说不在，因为两种
# 找法不同。而这一步的退出码 2 在门里记成 RED，于是「问不了」被读成「没排过格式」，推的
# 人被指去改一份空格根本没问题的文件。PATH 不是判据的一部分。
cargo_bin="$(command -v cargo || true)"
[ -n "${cargo_bin}" ] || [ ! -x "${HOME}/.cargo/bin/cargo" ] || cargo_bin="${HOME}/.cargo/bin/cargo"
[ -n "${cargo_bin}" ] || { echo "缺少依赖：cargo" >&2; exit 2; }
"${cargo_bin}" fmt --version >/dev/null 2>&1 || { echo "缺少依赖：这个工具链没装 rustfmt 组件" >&2; exit 2; }

# rustfmt 把每一处偏差打成一段 diff 印在 stdout 上，退出码 1。这里只补一句「这意味着
# 什么」：那一行是给人看的结论，上面那些 diff 是证据。
if ! "${cargo_bin}" fmt --all -- --check; then
    echo "上面这些文件没有按 rustfmt 排过：CI 的 Format 作业会因此红。" >&2
    echo "修法是 \`cargo fmt --all\`（会改写文件）；只想看会改哪里就再跑一次这条门。" >&2
    exit 1
fi
