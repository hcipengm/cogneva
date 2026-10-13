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

# 所有者句柄与它上面唯一允许被公开引用的仓。两个都是**结构**，不是名单：换所有者
# 或换公开仓都是这一行的改动，而不是往清单一类的东西里再加一条。
OWNER="hcipengm"
ALLOWED_REPO="cogneva"

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
scan_owner_repo() {
  git -C "$ROOT" grep -ohE -- "${OWNER}/[A-Za-z0-9._-]+" | sort -u
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
    return 0
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
  local matches bad rc=0 member
  matches="$(scan_owner_repo)"
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
  scan_bare_identifiers || rc=1
  return "${rc}"
}

main
