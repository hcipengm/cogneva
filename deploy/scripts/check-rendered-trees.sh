#!/usr/bin/env bash
# reads: deploy/helm/cogneva/profiles/*.yaml subject
#
# 渲染产物只有一种存在方式：读者在读的那一刻从 chart 生成它。
#
# 这条门判两件事，它们是一条判据的两面：
#   1. 仓里没有 `deploy/rendered/` 的任何字节。提交了的那天，读者就有了一份可以
#      读到旧值的副本——副本不会报错，只会过期；而「过期」与「就是这一版」在字节
#      上一样，没有任何东西能分辨。
#   2. 这条生成链此刻跑得通：每个 profile 都能从 `deploy/helm/cogneva/profiles/`
#      渲染出一棵有清单的目录。跑不通时读者会拿到空目录，而空目录在读它的每一条
#      判据里都读作「本来就没有东西」。
#
# 所以它不是新鲜度门：没有一份「已提交的产物」可以比。它判的是「源决定产物」这条
# 关系还在，以及没有任何人把产物又搬回树里。
#
# 用法：bash deploy/scripts/check-rendered-trees.sh [仓库根]
# 退出码：0 产物不入仓且每个 profile 都渲染得出来；1 有产物进了仓或某个 profile
#         渲染不出东西；2 判不了（不是 git 工作树、渲染链跑不起来）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${1:-$(cd "${here}/../.." && pwd)}"
ROOT="$(cd "$ROOT" && pwd)"

git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1 || {
    echo "RENDERED-TREES: 判不了：$ROOT 不是 git 工作树，问不了「产物进没进仓」" >&2
    exit 2
}

tracked="$(git -C "$ROOT" ls-files -- deploy/rendered)"
if [ -n "$tracked" ]; then
    echo "FAIL: deploy/rendered/ 下有文件进了仓：它是 chart 的产物，不是源。" >&2
    echo "$tracked" | head -20 | sed 's/^/  /' >&2
    echo "      删掉它们（git rm），需要这份产物的读者用 deploy/scripts/lib/rendered-cache.sh 现场生成。" >&2
    exit 1
fi

# 现场生成一次：这就是每个读者的第一读。夹具树（没有 chart）由缓存脚本原样放过，
# 但这条门跑在真树上，没有 chart 时下面按「一个 profile 都没读到」判红。
# shellcheck source=lib/rendered-cache.sh
source "${here}/lib/rendered-cache.sh"
ensure_rendered "$ROOT" || exit 2

profiles=0
missing=()
for profile_file in "$ROOT"/deploy/helm/cogneva/profiles/*.yaml; do
    [ -e "$profile_file" ] || continue
    name="$(basename "$profile_file" .yaml)"
    profiles=$((profiles + 1))
    dir="$ROOT/deploy/rendered/$name"
    count="$(find "$dir" -maxdepth 1 -name '*.yaml' -type f 2>/dev/null | wc -l || true)"
    if [ "$count" -eq 0 ]; then
        missing+=("$name")
    fi
done

if [ "$profiles" -eq 0 ]; then
    echo "RENDERED-TREES: 判不了：deploy/helm/cogneva/profiles/ 下一个 profile 都没有" >&2
    exit 2
fi
if [ "${#missing[@]}" -gt 0 ]; then
    echo "FAIL: 这些 profile 渲染不出任何清单：${missing[*]}（目录在，里面是空的）" >&2
    exit 1
fi

echo "RENDERED-TREES OK: 仓里没有渲染产物，$profiles 个 profile 都从 chart 现场生成得出来"
