#!/usr/bin/env bash
# `check-comment-capacity-figures.sh` 的离线测试：夹具是一棵小树，不是本仓。
#
# 判的是**「一台机器的容量」与「软件的属性」能不能在注释里分开**：
#   1. 注释里一个没有归属的容量数字 ⇒ 红，并点名那一行。
#   2. 同一行带「实测」「本机」「默认」「例如」 ⇒ 绿（它说清了是谁的数）。
#   3. 基线里的老账 ⇒ 绿，且只许缩：新加一条不在基线里的 ⇒ 照样红。
#   4. 代码里的容量常量（非注释）不读——那不是散文，是配置。
#   5. `--stock` 只报不判，退 0。
set -uo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
gate="$(cd "$here/.." && pwd)/check-comment-capacity-figures.sh"
[ -f "$gate" ] || { echo "FAIL: 门脚本不在：$gate" >&2; exit 1; }

tmp="$(mktemp -d)" || exit 1
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else
  printf 'FAIL- %s（期望退出码 %s，实得 %s）\n' "$1" "$2" "$3" >&2; fail=$((fail + 1)); fi; }
ck_has() { if printf '%s\n' "$3" | grep -qF -- "$2"; then ok "$1"; else
  printf 'FAIL- %s\n        期望含：%s\n' "$1" "$2" >&2
  printf '%s\n' "$3" | sed 's/^/        实得: /' | head -20 >&2; fail=$((fail + 1)); fi; }

# 门按 `$(dirname $0)/..` 找基线，所以夹具必须放在一棵形如 <root>/deploy/scripts 的树里。
tree="$tmp/tree"
mkdir -p "$tree/deploy/scripts" "$tree/deploy/k3s" "$tree/crates/cogneva/src"
cp "$gate" "$tree/deploy/scripts/check-comment-capacity-figures.sh"
: >"$tree/deploy/scripts/comment-capacity-figures.baseline"
run() { bash "$tree/deploy/scripts/check-comment-capacity-figures.sh" "$@"; }

yaml="$tree/deploy/k3s/x.yaml"

# 1. 没有归属的容量数字。
cat >"$yaml" <<'YAML'
spec:
  # overlay 层占了 47 GB，读不出来
YAML
out=$(run "$yaml" 2>&1); rc=$?
ck_rc "无来源标注的容量 ⇒ 红" 1 "$rc"
ck_has "点名那一行" "47 GB" "$out"

# 2. 说清了是谁的数的四种写法各绿一次。
for marker in '实测 2026-10-22：47 GB' '本机 47 GB' '默认 5 GiB' '例如 47 GB'; do
    printf 'spec:\n  # %s\n' "$marker" >"$yaml"
    out=$(run "$yaml" 2>&1); rc=$?
    ck_rc "标了归属（$marker）⇒ 绿" 0 "$rc"
done

# 3. 基线只许缩：老账绿，新账红。
printf 'spec:\n  # 47 GB 的老账\n' >"$yaml"
run --update-baseline "$yaml" >/dev/null 2>&1
out=$(run "$yaml" 2>&1); rc=$?
ck_rc "基线里的老账 ⇒ 绿" 0 "$rc"
printf 'spec:\n  # 47 GB 的老账\n  # 新写的 13 GB\n' >"$yaml"
out=$(run "$yaml" 2>&1); rc=$?
ck_rc "新加一条不在基线的 ⇒ 红" 1 "$rc"
ck_has "只点名新的那条" "13 GB" "$out"
if printf '%s\n' "$out" | grep -q '47 GB'; then
    bad "把基线里的老账也点名了（老账会被逼着当场改，门就成了噪音）"
else
    ok "基线里的老账不再点名"
fi

# 4. 非注释的容量常量不读：那是配置，不是散文。
cat >"$yaml" <<'YAML'
spec:
  resources:
    limits:
      memory: 512Mi
YAML
out=$(run "$yaml" 2>&1); rc=$?
ck_rc "配置值不是注释 ⇒ 绿" 0 "$rc"

# 5. 行首没有 `#` 的中文续行、以及 Rust 的 `//` 两种注释形态都读得到。
rs="$tree/crates/cogneva/src/x.rs"
printf '// 这块缓存 8 GiB\n' >"$rs"
out=$(run "$rs" 2>&1); rc=$?
ck_rc "Rust 行注释里的容量 ⇒ 红" 1 "$rc"
printf '//! 实测 8 GiB\n' >"$rs"
out=$(run "$rs" 2>&1); rc=$?
ck_rc "Rust 文档注释标了归属 ⇒ 绿" 0 "$rc"

# 6. --stock 只报不判。
printf 'spec:\n  # 47 GB\n' >"$yaml"
out=$(run --stock "$yaml" 2>&1); rc=$?
ck_rc "--stock 退 0" 0 "$rc"
ck_has "--stock 仍然印出来" "47 GB" "$out"

# 7. 未知参数是用法错，不是「没有违规」。
out=$(run --nope 2>&1); rc=$?
ck_rc "未知参数退 2" 2 "$rc"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
