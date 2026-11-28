#!/usr/bin/env bash
# 渲染产物的消费点生成器。
#
# `deploy/rendered/<profile>/` 不是源：chart、values、profile 与这一族脚本完全决定它。
# 所以仓里不再提交它，任何读者（引导器构建、契约测试、部署门、集群内落地端）在
# **读的那一刻**把它生成出来。输入摘要没变就不重新渲染；变了就整棵换掉。
#
# 为什么生成在 `deploy/rendered` 而不是别处：仓里每个读者都用这个路径认这份产物
# （`deploy/rendered/<profile>/…` 是它写进日志、判词与配置里的名字）。生成到别处
# 就等于把同一个名字再抄一遍，而抄出来的那份迟早与读者写的分叉。
#
# 这份脚本不打开 `-e`：它被别人的脚本 source，开关属于调用方。函数失败时返回
# 非零并说明原因；调用方要么 `|| exit`，要么自己判。
#
# 用法：
#   source deploy/scripts/lib/rendered-cache.sh && ensure_rendered [<树根>]
#   bash deploy/scripts/lib/rendered-cache.sh --ensure [<树根>]

# 摘要覆盖「决定产物字节」的每一份输入：chart 整棵树（含已提交的治理投影，
# 它们是渲染入参）、渲染链的三个脚本、以及 k3s-single 的额度基线。
rendered_digest() {
    local root="$1"
    local inputs=(
        deploy/scripts/render-deploy.sh
        deploy/scripts/render-chart.sh
        deploy/scripts/project-resource-governance.py
        deploy/scripts/check-governance-consistency.sh
        deploy/k3s/resource-quota.yaml
    )
    local f
    for f in "${inputs[@]}"; do
        [ -f "$root/$f" ] || {
            echo "rendered-tree: 缺少渲染输入：$root/$f" >&2
            return 1
        }
    done
    # 摘要按**树内相对路径**算：同一棵树的两份副本（测试的复制、别处的检出）必须
    # 得到同一个摘要，否则每份副本都会重渲一次，而重渲会把测试摆出来的夹具覆盖掉。
    (
        cd "$root" || exit 1
        {
            find deploy/helm/cogneva -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum
            sha256sum "${inputs[@]}"
        } | sha256sum | awk '{print $1}'
    )
}

# 这棵树是不是一份「渲染产物已经就位」的真树：有 chart、有渲染器。
rendered_tree_is_renderable() {
    local root="$1"
    [ -d "$root/deploy/helm/cogneva" ] && [ -f "$root/deploy/scripts/render-deploy.sh" ]
}

# 产物树真的在：每个 profile 都有一整棵。摘要对上还不够——`rm -rf dir/*` 或
# `git rm` 会留下隐藏的 `.stamp` 而挖空可见文件，这时摘要是新的、树是空的，
# 读者读到的却是「本来就没有东西」。所以完整性也要判。
rendered_tree_is_complete() {
    local root="$1" dir="$2" p name
    for p in "$root/deploy/helm/cogneva/profiles"/*.yaml; do
        [ -e "$p" ] || continue
        name="$(basename "$p" .yaml)"
        [ -n "$(find "$dir/$name" -maxdepth 1 -name '*.yaml' -type f -print -quit 2>/dev/null)" ] \
            || return 1
    done
    return 0
}

# 让 `<树根>/deploy/rendered` 等于当前 chart/profile 的渲染结果。
#
# 夹具树（没有 chart 或渲染器）原样放过：它的 `deploy/rendered` 是它自己的题目，
# 由夹具摆出来，不是这里有资格改写的东西。
ensure_rendered() {
    local root="${1:-}"
    local dir digest lock
    [ -n "$root" ] || {
        echo "rendered-tree: 没给树根" >&2
        return 2
    }
    root="$(cd "$root" && pwd)" || return 1
    rendered_tree_is_renderable "$root" || return 0
    dir="$root/deploy/rendered"
    digest="$(rendered_digest "$root")" || return 1
    if rendered_tree_is_complete "$root" "$dir" \
        && [ -f "$dir/.stamp" ] && [ "$(cat "$dir/.stamp")" = "$digest" ]; then
        return 0
    fi
    if ! command -v helm >/dev/null 2>&1; then
        echo "rendered-tree: 产物不在或已过期，而本机没有 helm——它由 chart 现场渲染，" >&2
        echo "              装一个 helm 再读（产物不进仓，没有可回落的提交物）。" >&2
        return 2
    fi
    lock="$root/deploy/.rendered.lock"
    # 一把锁包住「判摘要 → 渲染 → 换树」：多个测试二进制与门会同时醒来。
    # 整段放进 if 的条件里：调用方的 `set -e` 不该把一次可读的失败变成整份脚本消失。
    if (
        if command -v flock >/dev/null 2>&1; then
            flock -x 9
        fi
        digest="$(rendered_digest "$root")" || exit 1
        if rendered_tree_is_complete "$root" "$dir" \
            && [ -f "$dir/.stamp" ] && [ "$(cat "$dir/.stamp")" = "$digest" ]; then
            exit 0
        fi
        tmp="$root/deploy/.rendered.tmp.$$"
        old="$root/deploy/.rendered.old.$$"
        rm -rf "$tmp" "$old"
        if ! bash "$root/deploy/scripts/render-deploy.sh" --out "$tmp" >&2; then
            rm -rf "$tmp"
            exit 1
        fi
        printf '%s\n' "$digest" >"$tmp/.stamp"
        # 换名而不是先删后拷：读的人要么看见旧的一整棵，要么看见新的一整棵。
        if [ -d "$dir" ]; then
            mv "$dir" "$old"
        fi
        mv "$tmp" "$dir"
        rm -rf "$old"
        echo "rendered-tree: 已从 chart/profile 现场生成 $dir（$digest）" >&2
    ) 9>"$lock"; then
        return 0
    else
        return $?
    fi
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    case "${1:-}" in
        --ensure)
            shift
            ensure_rendered "${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
            ;;
        *)
            echo "usage: rendered-cache.sh --ensure [<树根>]" >&2
            exit 2
            ;;
    esac
fi
