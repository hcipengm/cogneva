#!/usr/bin/env bash
# `check-comment-capacity-figures.sh` 的离线测试：夹具是一棵小树，不是本仓。
#
# 判的是**「一台机器的容量」与「软件的属性」能不能在注释里分开**：
#   1. 注释里一个没有归属的容量数字 ⇒ 红，并点名那一行。
#   2. 同一行带「实测」「本机」「默认」「例如」 ⇒ 绿（它说清了是谁的数）。
#   3. 基线里的老账 ⇒ 绿，且只许缩：新加一条不在基线里的 ⇒ 照样红。
#   4. 代码里的容量常量（非注释）不读——那不是散文，是配置。
#   5. `--stock` 只报不判，退 0。
#   6. 读不动的路径 ⇒ 红：读失败交回来零命中，与「这行干净」同形。
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

# 1. 没有归属的容量数字。（夹具用 printf 写，免得这段中文本身进了门的扫描面）
printf 'spec:\n  # overlay 层占了 47 GB，读不出来\n' >"$yaml"
out=$(run "$yaml" 2>&1); rc=$?
ck_rc "无来源标注的容量 ⇒ 红" 1 "$rc"
ck_has "点名那一行" "47 GB" "$out"

# 1b. 告警注解的值不是注释，但运维在任何部署上都读得到，判据与注释同一档。
printf 'annotations:\n  summary: "盘只剩 13 GiB"\n' >"$yaml"
out=$(run "$yaml" 2>&1); rc=$?
ck_rc "告警 summary 里的无主容量 ⇒ 红" 1 "$rc"
ck_has "点名那一条注解" "13 GiB" "$out"
printf 'annotations:\n  summary: "盘只剩 13 GiB（实测 2026-10-22）"\n' >"$yaml"
out=$(run "$yaml" 2>&1); rc=$?
ck_rc "告警 summary 标了实测日期 ⇒ 绿" 0 "$rc"

# 1c. 部署脚本的注释同样在扫：这一族写现场最随意。
printf '#!/usr/bin/env bash\n# 缓存长到 47 GB 就该清\n' >"$tree/deploy/scripts/x.sh"
out=$(run "$tree/deploy/scripts/x.sh" 2>&1); rc=$?
ck_rc "deploy 脚本注释里的无主容量 ⇒ 红" 1 "$rc"
printf '#!/usr/bin/env bash\n# 缓存长到 47 GB 就该清（2026-10-22 实测）\n' >"$tree/deploy/scripts/x.sh"
out=$(run "$tree/deploy/scripts/x.sh" 2>&1); rc=$?
ck_rc "标了日期的现场 ⇒ 绿（日期也是一种归属）" 0 "$rc"
rm -f "$tree/deploy/scripts/x.sh"

# 2. 说清了是谁的数的四种写法各绿一次。
for marker in '实测 2026-10-22：47 GB' '本机 47 GB' '默认 5 GiB' '例如 47 GB'; do
    printf 'spec:\n  # %s\n' "$marker" >"$yaml"
    out=$(run "$yaml" 2>&1); rc=$?
    ck_rc "标了归属（$marker）⇒ 绿" 0 "$rc"
done

# 3. 基线只许缩：老账绿，新账红。
printf 'spec:\n  # 47 GB 的老账\n' >"$yaml"
run --update-baseline "$yaml" >/dev/null 2>&1; rc=$?
ck_rc "空基线要长出一条也得显式放行（新账不该被记下来）" 1 "$rc"
run --update-baseline --allow-grow "$yaml" >/dev/null 2>&1
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
# 基线不许被悄悄放宽：第二条新账出现时，--update-baseline 必须拒绝，且基线保持原样。
run --update-baseline "$yaml" >/dev/null 2>&1; rc=$?
ck_rc "基线要变长 ⇒ --update-baseline 拒绝" 1 "$rc"
ck_rc "拒绝之后基线没变" 1 "$(grep -c '' "$tree/deploy/scripts/comment-capacity-figures.baseline")"

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

# 8. 生成物不读：`deploy/rendered/` 是 render-deploy.sh 从 chart 生成的，同一句注释由
#    拥有它的模板负责；读生成物会把一次正当的 chart 改动记到没人手写的文件头上。
gen="$tmp/gen"
mkdir -p "$gen/deploy/scripts" "$gen/deploy/rendered/k3s-single" "$gen/deploy/k3s"
cp "$gate" "$gen/deploy/scripts/check-comment-capacity-figures.sh"
printf 'spec:\n  # 生成出来的 47 GB\n' >"$gen/deploy/rendered/k3s-single/x.yaml"
printf 'spec:\n  # 手写的 47 GB\n' >"$gen/deploy/k3s/y.yaml"
( cd "$gen" && git init -q . && git add -A ) >/dev/null 2>&1
out=$(bash "$gen/deploy/scripts/check-comment-capacity-figures.sh" --stock 2>&1); rc=$?
ck_rc "不带参数时自己沿 git 找树上的文件" 0 "$rc"
ck_has "手写清单里的容量被点到" "deploy/k3s/y.yaml" "$out"
if printf '%s\n' "$out" | grep -q 'deploy/rendered/'; then
    bad "读了生成物（chart 一改就会红在没人手写的文件上）"
else
    ok "生成物不在扫描面里"
fi
# 从别的目录跑（排队作业、钩子常见）必须照样判得出：git 给的是相对仓库根的路径，
# 若拿它去按调用者的 cwd 读，一个文件都不存在——那是退出码 0 的假绿。
out=$(cd / && bash "$gen/deploy/scripts/check-comment-capacity-figures.sh" 2>&1); rc=$?
ck_rc "从别的目录跑（不带参数）照样判得出红" 1 "$rc"
ck_has "点名手写清单那一行" "deploy/k3s/y.yaml" "$out"

# 9. 读不动的路径不是「这一行干净」：读失败交回来零命中，而这条门的绿正是零命中。
#    目录对任何调用者都读不动，所以这一条不取决于谁在跑。
mkdir -p "$tmp/not-a-file"
out=$(run "$tmp/not-a-file" 2>&1); rc=$?
ck_rc "读不动的路径 ⇒ 红" 1 "$rc"
ck_has "说清是读不出来，不是没有容量数字" "读不出这些文件" "$out"

# 10. 自选人口时一处容量数字都没读到，与「每一处都标明了归属」同形：匹配式或单位表
#     被改窄就是这个症状。造一棵只有一处无关注释的树，默认路径必须退 3。
quiet="$tmp/quiet"
mkdir -p "$quiet/deploy/scripts" "$quiet/deploy/k3s"
cp "$gate" "$quiet/deploy/scripts/check-comment-capacity-figures.sh"
: >"$quiet/deploy/scripts/comment-capacity-figures.baseline"
printf 'spec:\n  # 这里没有任何容量数字\n' >"$quiet/deploy/k3s/x.yaml"
( cd "$quiet" && git init -q . && git add -A ) >/dev/null 2>&1
out=$(cd "$quiet" && bash deploy/scripts/check-comment-capacity-figures.sh 2>&1); rc=$?
ck_rc "零处容量数字 ⇒ 退 3（没判成）" 3 "$rc"
ck_has "说清是一处都没读到" "只读到 0 处容量数字" "$out"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
