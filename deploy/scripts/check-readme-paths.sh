#!/usr/bin/env bash
# 文档里点名的路径必须是这棵树里真有的路径。
#
# 要防的失败是静默的：一句「`MessageBackend` Trait 定义（`cog-core/src/stream/message_bus.rs`）」
# 对写它的那一刻是真的（那时文件在那儿），对读它的人是假的——路径搬了家，正文没跟。
# 没有任何东西会因此报警：编译器读不到 markdown，缺文件的那一行照常渲染成一句看着
# 完整的说明，而读者照着去开文件时才发现是个死指针。这类指针只会在**有人重读**
# 时才被发现，所以它得有一条判据替人去读。
#
# 判的是**被反引号括起来、长得像文件名**的串（含 `/`、无空白、末段带扩展名），
# 在仓库根或 `crates/` 下至少有一处存在。三条口径是量出来的，不是拍的：
#   - **带扩展名**才算路径：`evolution-changes/` 这类是运行期目录的散文提及（那份
#     目录由程序在运行时创建，不在仓库里），拿它判红就是判了一件文档没主张的事。
#     代价是**目录路径不判**——这是有意让出的，本门只管「点了一个文件」。
#   - **`crates/` 兜底**：README 里 crate 有时带前缀（`crates/cog-core/src/...`）有时
#     不带（`cogneva/src/bootstrap.rs`），两种都指同一处；按存在的那个算。
#   - **只看被反引号括起来的**：反引号是「这是一个路径」的标记本身。正文里的散装
#     词不做路径解析（`publish / subscribe` 这种斜杠不是路径分隔符）。
#   - **占位符不算路径**：`<file>`、`*.rs`、`{name}/{version:06}.json` 说的是「这儿的
#     文件长这样」，不是「这棵树下有这一份」。缺了这一条，文档一描述产物命名就得改
#     说法去迁就门——门把一句正确的说明判成死指针，红的是门不是文档。
#
# 用法：bash deploy/scripts/check-readme-paths.sh [文件...]
#       不给文件时判仓库根的两份 README（本门服务的正是这份对外承诺）。
#       路径按**仓库根**解析，所以本门适用于放在根部的文档。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"

files=("$@")
defaulted=0
if [ "${#files[@]}" -eq 0 ]; then
  files=("${repo}/README.md" "${repo}/README.zh-CN.md")
  defaulted=1
fi

# 判过多少处「像路径」的串。自己选人口时（两份 README），零处与「每一处都还在」
# 在输出上同形：正则写坏了、README 被换成一段没有路径的简介，都印「绿」。
path_floor=5
judged=0

bad=0
for f in "${files[@]}"; do
  [ -f "${f}" ] || { echo "check-readme-paths: 没有这个文件：${f}" >&2; exit 2; }
  # 读失败（grep 退出 >1）不是「这个文件里没有路径」：文件在而读不出来时，
  # 这一份就是零命中，零命中在下面与「全都还在」走同一条路。
  hits=""
  grep_status=0
  hits="$(grep -noE '`[^`]+`' "${f}")" || grep_status=$?
  if [ "${grep_status}" -gt 1 ]; then
    echo "check-readme-paths: 读不出 ${f}（grep 退出 ${grep_status}）——没读到就判不了里面的路径" >&2
    exit 2
  fi
  # grep -o 会丢掉行号，判词要指到行才可修，所以用 -n 并在这里拆。
  while IFS= read -r hit; do
    [ -n "${hit}" ] || continue
    lineno="${hit%%:*}"
    token="${hit#*:}"
    token="${token//\`/}"
    case "${token}" in
      "" | *[[:space:]]* | /* | *"://"* | *'<'* | *'>'* | *'*'* | *'{'* | *'}'*) continue ;;
    esac
    case "${token}" in */*) ;; *) continue ;; esac
    case "${token##*/}" in *.*) ;; *) continue ;; esac
    judged=$((judged + 1))
    [ -e "${repo}/${token}" ] && continue
    [ -e "${repo}/crates/${token}" ] && continue
    printf '%s:%s: 点名的路径不在这棵树里：%s\n' "${f}" "${lineno}" "${token}" >&2
    bad=1
  done <<<"${hits}"
done

if [ "${bad}" -ne 0 ]; then
  echo "改法：把路径改成它今天真实的位置（或把文件放回被点名的那一处）。" >&2
  exit 1
fi

# 只有门自己挑人口时才要求下限：调用方点名一份文档时，那份文档里没有路径就是它
# 今天的实情；而两份 README 是这个门服务的对外承诺，里面一处路径都认不出来，说明
# 认路径的那一步坏了，不是文档变干净了。
if [ "${defaulted}" -eq 1 ] && [ "${judged}" -lt "${path_floor}" ]; then
  echo "check-readme-paths: 两份 README 里只认出 ${judged} 处像路径的串（下限 ${path_floor}）——" >&2
  echo "零处与「每一处都还在」印出来一模一样，这一跑什么都没证明。" >&2
  exit 3
fi
