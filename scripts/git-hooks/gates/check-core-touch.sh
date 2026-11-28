#!/usr/bin/env bash
# 这一笔动了 `crates/cog-core` 时，把「core 非必要不碰」这件事说出来——**只提示，不拦**。
#
# 为什么是提示而不是门：判据是「动它的正当理由只有跨 crate 的类型／trait 对不上」，
# 而一句提交里的理由，机器读不出来。能机检的只有形状的一半——**这一笔碰没碰 core、
# 净增还是净减、是不是只动了那两张只增登记表**。把判不了的那一半伪装成门的判决，
# 只会得到一条永远红、判词里却没有「谁该修」的灯。
#
# 这条形状的由来（`memory/feedback_prefer_not_to_touch_core.md`，属主 2026-10-12 定）：
# 近 300 个 commit 里 94 个碰 core，其中 40 个只改 `contract/metric_names.rs` 与
# `metric_help.rs` 两本只增不减的登记表——加一个指标就得改 core、就得重编 26/27 个
# crate。所以「只碰这两张表」不是普通的 core 改动，它是被实测点名过的那个形状。
#
# 读数全部从这一笔读出来，不写死：`git diff --cached --numstat` 给每个文件的增删行数，
# 判词里印的就是这一笔自己的数。路径与状态也取自索引，不读工作树——判的宾语是提交。
#
# 出口码：0 判完了（碰没碰 core 都是 0：它是提示不是门）；3 问不出来（不在仓里）。
# 调用方（`hooks/cogneva-tree/pre-commit`）按「0 或 3 都继续」处理。
#
# Usage: bash scripts/gates/check-core-touch.sh    # 判当前仓的暂存区
set -uo pipefail

top="$(env -u GIT_DIR -u GIT_WORK_TREE git rev-parse --show-toplevel 2>/dev/null)" || top=""
if [ -z "$top" ]; then
    echo "core 提示：读不到仓库根（不在 git 仓里），这一笔没有被判定。"
    exit 3
fi

numstat="$(git -C "$top" diff --cached --numstat -- crates/cog-core 2>/dev/null)" || numstat=""
[ -n "$numstat" ] || exit 0     # 没碰 core：这条提示不归这一笔

added=0
removed=0
files=0
only_registries=1
while IFS=$'\t' read -r a d path; do
    [ -n "${path:-}" ] || continue
    files=$((files + 1))
    # 二进制文件在 numstat 里是 `-`：没有行数可加，但它仍是一次 core 改动。
    [[ $a =~ ^[0-9]+$ ]] && added=$((added + a))
    [[ $d =~ ^[0-9]+$ ]] && removed=$((removed + d))
    case "$path" in
        crates/cog-core/src/contract/metric_names.rs) ;;
        crates/cog-core/src/contract/metric_help.rs) ;;
        *) only_registries=0 ;;
    esac
done <<<"$numstat"

printf 'core 提示：这一笔动了 crates/cog-core（%s 个文件，+%s/−%s；`git diff --cached --numstat` 量的）。\n' \
    "$files" "$added" "$removed"
if ((only_registries)); then
    echo "  只动了 metric_names.rs / metric_help.rs 两张只增登记表——实测这两张表是 94 笔 core 改动里 40 笔的全部内容；新增指标名不是动 core 的理由。"
fi
if ((added > removed)); then
    echo "  净增 $((added - removed)) 行：core 的净方向是变小，往它加新内容要比改它更该被拒。"
fi
echo "  理由只认一条：跨 crate 的类型／trait 对不上；能在一个 crate 完成的就在一个 crate 完成。"
echo "  判据全文：memory/feedback_prefer_not_to_touch_core.md"
exit 0
