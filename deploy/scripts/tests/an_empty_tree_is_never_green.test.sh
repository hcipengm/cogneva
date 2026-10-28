#!/usr/bin/env bash
# 一族的**总判据**：任何一条门，在一棵空树上都不许退 0。
#
# 为什么要有这一条：这一族（「门读到的人口是零 / 读失败，却印出与『全都合规』
# 一字不差的那句话」）的每一处都长着同一张脸——它自己的判据没错，错的是判据
# 上一个**集合为空**时没人拦。而「集合为空」最容易出现的场合不是某个具体夹具，
# 是**走错了树**：仓库根指错、渲染物没生成、配置换了个键名、普查面换了个写法。
# 这时每一条门都还在，都绿。
#
# 所以这里不看任何一条门判的是什么，只看它**能不能在一棵什么都没有的树上
# 退 0**：能，就说明它的绿不依赖它自称读的那份东西。
#
# 退出码怎么分不归这里管（1 = 有违规、2/3 = 没判成），两种都算「没说绿」——
# 这一格唯一的失败态是「它说这棵树没问题」。
#
# 例外只有一个，写在下面那行过滤里，理由也写在那一行：check-before-push.sh 的
# 工作是把 CI 的 shell 步原样再跑一遍，在空树上它没有可跑的东西，而它的零步守卫
# 会红；但把它放进这个循环意味着每分钟一次全量重放，代价与这条判据无关。
#
# 这条判据要求门认第一个参数当仓库根（`bash 门 <root>`）。不认参数、永远判自己
# 所在那棵树的门，在这里会以「真树的结论」出现——它绿就说明它对自己的树是绿的，
# 与空树无关，那不是这一族要抓的东西，但它会被这里判红。今天 55 条门都认。
#
# 实测（2026-10-28）：deploy/scripts 下 55 条 check-*.sh 在空根上全部退非 0
# （两条在此之后补的零人口守卫：alert-rule-firing-coverage 的「夹具目录里一条
# *.test.yaml 都没读到」、backup-coverage 的「普查面一条都没读到」）。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 空根的形状：门的常见前置（渲染物目录、crates 目录、工作流目录）都在，只是**没有内容**。
# 「目录不存在」与「目录是空的」对这条判据是同一件事，所以这里只要有目录就够——
# 少给几个目录只会让更多的门在更早的地方退 2，判据照样成立。
empty_root() {
    local root="$1"
    mkdir -p "${root}/deploy/scripts" "${root}/deploy/rendered" \
             "${root}/deploy/k3s" "${root}/deploy/helm/cogneva/files" \
             "${root}/crates" "${root}/.github/workflows" "${root}/web/src"
}

# 「这条门没说这棵树是绿的」。每个门一棵新树：某条门往树里写了东西，不许让
# 下一条门因为看见了别人的垃圾而变红——那会把「它自己判绿了」藏起来。
judge() {
    local gate="$1" root rc=0
    root="$(mktemp -d "${work}/root.XXXXXX")"
    empty_root "$root"
    timeout 60 bash "$gate" "$root" >"${work}/out" 2>&1 || rc=$?
    rm -rf "$root"
    [ "$rc" -ne 0 ]
}

# 0) 断言逻辑自己先过一格：一条在空树上退 0 的「门」必须被判成违规。
# 没有这一格，下面 55 格全绿也可能只是因为 judge 恒真。
printf '#!/usr/bin/env bash\nexit 0\n' >"${work}/green.sh"
if judge "${work}/green.sh"; then
    fail "断言自己坏了：一条恒退 0 的脚本被判成「没说绿」"
fi

green=()
ran=0
for gate in "${repo}"/deploy/scripts/check-*.sh; do
    case "${gate}" in
        # 代价与判据无关：它重放 CI 的每一步，在空树上要跑满整条流水线才算完。
        */check-before-push.sh) continue ;;
    esac
    ran=$((ran + 1))
    judge "${gate}" || green+=("$(basename "${gate}")")
done

[ "${ran}" -ge 40 ] || fail "只跑了 ${ran} 条门（下限 40）：普查面本身没读到，这时「全绿」是假的"
if [ "${#green[@]}" -ne 0 ]; then
    printf 'FAIL: %d 条门在一棵空树上退 0（它的绿不依赖它自称读的那份东西）：\n' "${#green[@]}" >&2
    printf '  %s\n' "${green[@]}" >&2
    exit 1
fi

echo "an-empty-tree-is-never-green: ${ran} 条门在空根上都退非 0"
