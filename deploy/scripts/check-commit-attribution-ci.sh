#!/usr/bin/env bash
#
# 一条命令算出「这次推新增的提交」范围，交给唯一的判据
# （`check-commit-attribution.sh`）。
#
# 为什么范围计算要从 ci.yml 挪到这里：推前门只**回放单行** `run: <解释器> <路径>` 的
# CI 步，多行脚本块它跳过，而跳过的那一面会在判词里写成「本机没验，不算验过」。同一件
# 事若在门里再写一条专用步骤，判词就会自相矛盾——上一行印着「本机没验」，下一行又判了
# 一遍。收进脚本之后 CI 那一步是单行，本机回放的是**同一条命令**，名单由证据算出。
#
# 三种调用方式，范围各自从哪里来：
#   1. CI（`GITHUB_ACTIONS=true`）：按事件算。PR 读 base/head，push 读 before/sha；
#      before 全零（新建分支）时退回「HEAD 可达的全部」。范围算不出来时判据自己拒绝
#      （退 2），不静默放行。
#   2. 全树普查（`bash 这条脚本 <root>`）：判那棵树，读不出一段可判的范围就退 3
#      ——「看不了」不是绿（`an_empty_tree_is_never_green.test.sh` 判的就是这条）。
#   3. 推前门回放 / 人手工跑（不带参数）：退回 `origin/main..HEAD`。没有输入时印
#      `SKIP … 没判` 交 0：门在回放 CI 那一步，而 CI 把非零当失败——这里说绿的是
#      「没有要判的字节」，判词里那行 SKIP 就是它不比「判过且干净」的地方。
#
# 事件变量只在 CI 里可信：本机的 shell 里可能有别的作业留下的 BASE_SHA/HEAD_SHA（回放、
# 调试），拿它们算范围会判一段与这次推无关的历史。所以按「在不在 CI 里」分，而不是按
# 「变量在不在」分。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
checker="${here}/check-commit-attribution.sh"
root="${1:-}"

# 「这一段没判」只在一处写，两种调用者各得其所：给了根的是普查在问那棵树，看不了
# 就退 3；没给根的是门在回放，CI 的退出码语义把非零读成红，所以说出没判并交 0。
no_input() { # no_input <为什么>
    if [ -n "${root}" ]; then
        echo "UNJUDGED 提交信息署名：$1（给了根就不算绿：读不到 ≠ 读到零）" >&2
        exit 3
    fi
    echo "SKIP 提交信息署名：$1（这一面没判，不是判过且干净）"
    exit 0
}

if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    base="${BASE_SHA:-}"
    head="${HEAD_SHA:-}"
    case "${base}" in
        "" | 0000000000000000000000000000000000000000) base="" ;;
    esac
    if [ -n "${base}" ]; then range="${base}..${head}"; else range="${head}"; fi
    echo "judging ${range}"
    exec bash "${checker}" "${range}"
fi

if [ -n "${root}" ]; then
    [ -d "${root}" ] || no_input "${root} 不是一个目录"
    if ! git -C "${root}" rev-parse --verify --quiet 'origin/main^{commit}' >/dev/null; then
        no_input "${root} 里读不到 origin/main"
    fi
    if [ "$(git -C "${root}" rev-list --count 'origin/main..HEAD')" -eq 0 ]; then
        no_input "${root} 的 origin/main..HEAD 没有提交"
    fi
    cd "${root}" || no_input "进不去 ${root}"
    exec bash "${checker}" 'origin/main..HEAD'
fi

range='origin/main..HEAD'
if ! git rev-parse --verify --quiet 'origin/main^{commit}' >/dev/null; then
    no_input "本机没有 origin/main，推的范围判不出来（CI 的 Check 会跑）"
fi
if [ "$(git rev-list --count "${range}")" -eq 0 ]; then
    no_input "${range} 没有提交（这次推的一笔都没有）"
fi
exec bash "${checker}" "${range}"
