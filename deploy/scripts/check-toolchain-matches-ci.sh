#!/usr/bin/env bash
# 本地编译器是不是将会审判你这笔改动的那个。
#
# 判据不是「装了什么」而是「两边是不是同一个」：CI 与本机都从 `rust-toolchain.toml`
# 取通道（stable），所以通道一致由那份文件保证；剩下唯一会分叉的地方是本机停在哪
# 一次 stable 上。分叉的代价不是编译慢——是本地全绿、推上去红，而红的原因和提交内容
# 毫无关系：2026-10-27 一天之内，`assert!(常量 >= 12)`、常量登记表、循环注册三处红
# 各自都是「新 stable 的 lint/约定」而不是写错了什么，每处都要等 30 分钟 CI 才发现。
#
# 为什么要在推之前问、而不是在 CI 里问：CI 上的答案永远是「一致」（它就是那个编译器），
# 所以这条门的读者是本机，CI 里跑它只是为了保证 yml 与 toml 没各说一套。
#
# 判据：
#   1. `rust-toolchain.toml` 在，通道是 stable——版本号钉死会让「能不能过」变成
#      「某个编译器能不能过」，本机装的是另一个；
#   2. `.github/workflows/` 里每一处 `dtolnay/rust-toolchain@X` 的 X 都是 stable，
#      没有一处写死版本号（写死的那一处就是下一个分叉点）；
#   3. 本机 stable 不落后于已发布的 stable（`rustup check`）。离线时打一行 SKIP
#      然后放行：问不到不等于问过了红，也不等于没问过绿。
#
# 用法：bash deploy/scripts/check-toolchain-matches-ci.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

problems=()
TOML="rust-toolchain.toml"

if [ ! -f "$TOML" ]; then
    problems+=("$TOML 不存在——CI 与本机就没有共同的工具链来源，两边各装一个版本")
else
    channel="$(python3 - "$TOML" <<'PY'
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
m = re.search(r"^\s*channel\s*=\s*\"([^\"]+)\"", text, re.M)
print(m.group(1) if m else "")
PY
)"
    if [ "$channel" != "stable" ]; then
        problems+=("$TOML 的 channel 是「${channel:-（没写）}」而不是 stable——钉死版本号之后，「这笔改动能不能过」取决于 CI 上那个编译器，本机装的是另一个")
    fi
fi

# 工作流里不许出现带版本号的 rust-toolchain：那一处就是下一个分叉点。
while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    problems+=("$hit 写死了工具链版本——它绕过了 $TOML，stable 发布时这一处不会跟着动")
done < <(grep -rEo 'dtolnay/rust-toolchain@[^[:space:]]+' .github/workflows/ 2>/dev/null \
         | grep -v 'dtolnay/rust-toolchain@stable' | sed 's/^/形如 /')

# 本机 stable 是否落后。离线放行，但要留下那一行。
#
# rustup 不一定在 PATH 上：这个仓库的构建机把它装在 ~/.cargo/bin 而 PATH 里只有
# 系统目录。找不到就当「问不到」，不当「一致」——「没问」与「问过了绿」是两件事。
rustup_bin="$(command -v rustup 2>/dev/null || true)"
[ -n "$rustup_bin" ] || rustup_bin="$HOME/.cargo/bin/rustup"
[ -x "$rustup_bin" ] || rustup_bin=""

# 这一格只在作者机器上有读者。CI 的 runner 自己预装了一个 stable，它落后于上游
# 是那张镜像的属性，与在跑的这笔改动无关——在这里问，红的是 runner 的镜像历史，
# 而修法（`rustup update`）在 runner 上不成立，也没人能做。上面的两条结构判据
# 在 CI 里照跑：那两条判的是 yml 与 toml 有没有各说一套，与装了什么无关。
#
# 2026-10-27 实测：这条门在 CI 上红了整整一步（`Repo Conventions` 的最后一格），
# 部署器因此每轮持单，而同一棵树在本机全绿——两条读数差在一件与提交无关的事上。
CI_SKIP_LOCAL_VERSION=0
if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    CI_SKIP_LOCAL_VERSION=1
    echo "SKIP 在 CI 上：这一格核对的是作者机器上的 stable，runner 预装的版本不是它"
fi
# 判据读的是输出，不是退出码：`rustup check` 发现「有更新可用」时退的是 100，
# 按退出码判会把「有更新」读成「问不到」，于是这条门在它唯一该红的时候静默放行。
checked=0
if [ "$CI_SKIP_LOCAL_VERSION" -eq 1 ]; then
    : # 原因上面那一行已经印过；这里再印一句「没有 rustup」会把跳过说成缺工具
elif [ -n "$rustup_bin" ]; then
    check_out="$(timeout 60 "$rustup_bin" check 2>&1 || true)"
    behind="$(printf '%s\n' "$check_out" | grep -E '^stable[^ ]*( |$).*update available' || true)"
    current="$(printf '%s\n' "$check_out" | grep -E '^stable[^ ]*( |$).*(up to date|unchanged)' || true)"
    if [ -n "$behind" ]; then
        checked=1
        problems+=("本机 stable 落后于已发布的 stable：$behind——先跑 rustup update stable，否则 CI 会用一台你看不见的编译器判这笔改动")
    elif [ -n "$current" ]; then
        checked=1
    else
        echo "SKIP 问不到 rustup 的更新状态（多半是离线）：没核对本机 stable 是否落后"
    fi
else
    echo "SKIP 本机没有 rustup：无从核对编译器版本"
fi

if [ "${#problems[@]}" -gt 0 ]; then
    printf '工具链与 CI 不一致：\n' >&2
    printf '  %s\n' "${problems[@]}" >&2
    exit 1
fi

if [ "$checked" -eq 1 ]; then
    echo "ok：$TOML 声明 stable，工作流里没有写死的版本号，本机 stable 不落后"
else
    echo "ok（部分）：$TOML 声明 stable，工作流里没有写死的版本号；本机版本没核对"
fi
