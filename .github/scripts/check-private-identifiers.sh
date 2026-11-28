#!/usr/bin/env bash
# 私有标识门：推到公开树里的东西，不许把私有仓指出来。
#
# 为什么判据锚在**所有者句柄**而不是一张私有仓名清单：私有仓名是开放集合（以后会有
# 不带 `chronicle` 的私有代码仓），清单永远补不全；更糟的是，把私有名写进公开的 CI
# 文件本身就是泄露——清单放进被它守的那棵树里，它就成了它要防的那件事。
# 锚在所有者上则自动覆盖未来所有私有仓：凡 `hcipengm/<repo>` 形式的引用，`<repo>`
# 只允许公开的那一个，多一个就红，不必逐个点名。
#
# 克隆 URL 的 `.git` 后缀是同一个仓的另一种写法（`hcipengm/cogneva.git`），按仓名
# 比较而不是按字面比较，免得它被当成第二个仓。这条不是洁癖：真实测量过，公开树里
# `.git` 形态比不带后缀的还多，判据认字面的话第一天就红在一个不是问题的东西上。
#
# 第二遍（裸标识）是针对结构判据够不着的那一类：不带所有者的裸名字（注释里、
# fixture 里的一个词）。它只能靠名单，而名单进公开文件就泄露 ⇒ 名单从 CI secret
# 注入，有就跑、没有就明说没跑。**这层是补漏网，不是完整证明**——真正的护栏是
# 「代码里零面字面量、值全走配置注入」这条设计规则，靠结构判据 + 代码评审守它。
#
# 用法：bash check-private-identifiers.sh [仓库根目录]
#
# Environment:
#   PRIVATE_IDENTIFIERS          换行分隔的裸标识名单（CI secret 注入；空＝这一遍没跑）
#   REQUIRE_PRIVATE_IDENTIFIERS  `true` 时名单缺席算失败（可信事件上设，别在 fork PR 上设）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"

# 所有者句柄与它上面唯一允许被公开引用的仓。
#
# 2026-11-20 之前所有者是**写死的**一个账号名。那是把「这个仓属于谁」钉成了常量：
# 换所有者、或者有人 fork 过去跑同一套门，这一行就成了错的判据，而它错的方式是
# 「每一处引用都按一个不属于这个仓的所有者去判」。
#
# 现在从**这个仓自己的 origin** 读（fork 之后自然跟着变），环境变量可覆盖；
# 读不出来就是判不了——3，不是 0。
ALLOWED_REPO="cogneva"
origin_url="${COGNEVA_ORIGIN_URL:-$(git -C "$ROOT" remote get-url origin 2>/dev/null || true)}"
OWNER="${COGNEVA_OWNER:-}"
if [ -z "$OWNER" ] && [ -n "$origin_url" ]; then
    OWNER="$(printf '%s' "$origin_url" | sed -n 's#.*[/:]\([^/]*\)/[^/]*$#\1#p')"
fi
if [ -z "$OWNER" ] && git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1; then
    # 是一个 git 仓却没有远端：所有者读不出来，这一遍判不了。
    #
    # 不是 git 仓时**不在这里报**：那是另一句话（「读不出这棵树」），由 main 去说。
    # 两件事都在这里退 3 的话，判词会把「没有远端」与「根本不是仓」混成一句。
    printf '私有标识门：读不出这个仓的所有者（origin 没有远端地址，也没给 COGNEVA_OWNER）——这一遍没判\n' >&2
    exit 3
fi

log() { printf '%s\n' "$*"; }

# stdin: 一行一个 `owner/repo`。stdout: 其中不在允许表里的那些。
# 剥 `.git` 后缀后比较，保留原样输出（判词要能照着 grep 回去）。
offenders() {
  awk -v ok="${OWNER}/${ALLOWED_REPO}" '
    NF {
      repo = $0
      sub(/\.git$/, "", repo)
      if (repo != ok) print $0
    }'
}

# 树里所有 `owner/repo` 形态的匹配，去重。`git grep` 读的是**被跟踪**的文件，
# 所以构建残渣、未跟踪的草稿不会把门弄红。
#
# 调用方在 main 里直接取它的输出并**自己看退出码**：这个函数若在命令替换里 `exit`，
# 只结束那个子壳，父壳会接着走到「通过」。`git grep` 的 1 是「一处也没中」，
# >1 才是「没扫成」——两种都在输出上表现为空。
scan_owner_repo() {
  git -C "$ROOT" grep -ohE -- "${OWNER}/[A-Za-z0-9._-]+" | sed '/^$/d' | sort -u
}

locate() {  # 判词要指到行，不然拿到一个仓名还得自己找它在哪
  git -C "$ROOT" grep -nF -- "$1" || true
}

# 去掉首尾空白（名单里常常是换行 + 缩进）。
trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

# 裸标识那一遍。命中即红，并把命中行打出来。
scan_bare_identifiers() {
  local rc=0 ident hits
  if [ -z "${PRIVATE_IDENTIFIERS:-}" ]; then
    if [ "${REQUIRE_PRIVATE_IDENTIFIERS:-}" = "true" ]; then
      log "私有标识门（裸标识）：PRIVATE_IDENTIFIERS 没有注入，而本级要求它必须在。"
      return 1
    fi
    log "私有标识门（裸标识）：PRIVATE_IDENTIFIERS 没有注入 ⇒ 这一遍没跑（补漏网层，不是完整证明）。"
    # 3 而不是 0：这一层没跑成，与「跑了且干净」在退出码上不能同形——同形的话，
    # 这一个作业在 CI 上永远是绿的，而它有一整层从没执行过。3 是「没验」，由调用方
    # 决定它算不算失败（今天不算：名单是属主手上的那一格，见 ci.yml 的注释）。
    return 3
  fi
  while IFS= read -r ident; do
    ident="$(trim "${ident}")"
    [ -n "${ident}" ] || continue
    if hits="$(git -C "$ROOT" grep -nF -- "${ident}")"; then
      rc=1
      log "私有标识门（裸标识）：命中「${ident}」："
      printf '%s\n' "${hits}"
    fi
  done <<< "${PRIVATE_IDENTIFIERS}"
  if [ "${rc}" -eq 0 ]; then
    log "私有标识门（裸标识）：名单里 ${#PRIVATE_IDENTIFIERS} 个字节逐个比对，无命中。"
  fi
  return "${rc}"
}

main() {
  local matches bad rc=0 member status=0
  matches="$(scan_owner_repo)" || status=$?
  if [ "${status}" -gt 1 ]; then
    # 读不出来与「树里没有指向任何私有仓的引用」印的是同一句「通过」。后者是结论，
    # 前者是没做成这件事，两者不能共用一个退出码。
    log "私有标识门（所有者/仓）：读不出这棵树（git grep 退出 ${status}）——没读到就判不了引用指向谁"
    return 3
  fi
  if [ -z "${matches}" ]; then
    log "私有标识门（所有者/仓）：树里一处 ${OWNER}/… 的引用都没有读到——零处与"
    log "「只有 ${OWNER}/${ALLOWED_REPO}」在输出上同形，这一跑什么都没证明"
    return 3
  fi
  bad="$(printf '%s\n' "${matches}" | offenders)"
  if [ -n "${bad}" ]; then
    rc=1
    log "私有标识门（所有者/仓）：以下引用不在允许表里，${OWNER} 下只允许 ${ALLOWED_REPO}："
    while IFS= read -r member; do
      [ -n "${member}" ] || continue
      locate "${member}"
    done <<< "${bad}"
  else
    log "私有标识门（所有者/仓）：${OWNER} 下的引用只有 ${OWNER}/${ALLOWED_REPO}，通过。"
  fi
  # `|| bare=$?` 而不是分两句：这一份脚本跑在 `set -e` 下，裸调用一个会返回非零的函数
  # 会让整份脚本当场以那个码退出——第二层还没轮到判词，3 就先出去了。
  local bare=0
  scan_bare_identifiers || bare=$?
  case "${bare}" in
    0) : ;;
    # 3 只在还没有红的时候往外传：已经红了的一推，红比「有一层没验」更要紧。
    3) if [ "${rc}" -eq 0 ]; then rc=3; fi ;;
    *) rc=1 ;;
  esac
  return "${rc}"
}

main
