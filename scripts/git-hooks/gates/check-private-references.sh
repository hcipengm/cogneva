#!/usr/bin/env bash
# 私有标识门（本地那一半）：公开树里不许出现**私有仓库的名字**。
#
# 为什么要有这一半：公开仓的 CI 里已经有一道 `.github/scripts/check-private-identifiers.sh`，
# 但它只有两层——所有者句柄（结构判据）与一份**从 CI secret 注入**的裸标识名单。裸标识那层
# 缺席时它明说没跑（CI 里也确实没打开 fail-closed：secret 在不在，从树里读不出来）。所以
# 「文档里写着私有仓的名字」这件事，在推送之前没有任何东西问过。2026-10-22 实测：公开树里
# 有 24 处私有仓名（整名与它的段名），全是「这个读数不在本仓、它在哪」这类说明里的顺手指路，
# 而它们一次都没被拦下——门在推之后，而且那一遍没跑。
#
# 名单为什么不写进公开树：写进去就等于把它要防的那件事放进被它守的那棵树里（CI 那份脚本
# 的头部写着同一条理由）。所以名单**从私有仓自己身上推导**：本脚本住在私有仓，仓目录名与
# `origin` 地址里的仓名就是答案；连字符／下划线切出来的每一段，只要不出现在**被守那棵树
# 自己的名字**里，也算一个标识（`私有仓名` 对 `公开仓名` ⇒ 那一段）。换机器改名不用
# 改这里，加一个私有仓也不用——它守的是「所有私有仓」，不是这一份清单。
#
# 这一份**只判这一笔提交新增的字节**（暂存区，`git diff --cached`）：已落地的存量不归
# 提交者管，拿它判红的门第一天就会被 `--no-verify` 学会。存量由 `--tree` 那一档量
# （`principle-tests.sh` 的目录派生扫描会跑它的一份自检）。
#
# 用法：
#   bash scripts/gates/check-private-references.sh [--index|--tree] [被守的仓]
# 退出码：0 干净 / 1 命中（拒提交）/ 2 用法不对 / 3 判不了（不是 git 仓、推不出标识）。
#   3 与 1 必须分开：判不了是这台机器答不上来，不该拦下这一笔。
set -uo pipefail

# 这个门问的是**两个具名的仓**，所以它必须先把自己从「我正在被哪一棵树调用」里摘出来。
#
# git 跑钩子时把 `GIT_DIR` 指向正在提交的那棵树，而 `git -C <别的仓>` 只换工作目录、
# 不换 `GIT_DIR`：于是每一条 `git -C "$PUBLIC_ROOT"` / `git -C "$PRIVATE_ROOT"` 读的都
# 还是那一棵，而不是它名字里那棵。两边各自的失败方式相反——私有那一侧把名单推成被守
# 树的名字（合法行逐行假红、真泄漏放行），公开那一侧扫的是别人的 index（真泄漏报成
# 干净，而报告里点的是被守树的名字）。一次摘干净比在每个调用点补 `env -u` 可靠：漏一
# 处就退回同一类错，而漏的那处从输出上看不出来。
#
# **`GIT_INDEX_FILE` 不在这张表里，这是故意的。** 它不定位仓，定位的是「这一笔要判哪份
# 字节」：`git commit -- <路径>`（partial commit）与 `principle-commit.sh` 都会把提交器
# 自己造的那份索引塞进来，钩子就是在那个环境里跑起来的。摘掉它，门退回读仓里的真索引——
# 共享树里那份索引躺着别的会话暂存的路径，于是「判这一笔」变成「判这棵树里所有人暂存的
# 东西」：2026-10-26 实测，真索引里一处别人暂存的私有仓名字，让一笔只落了干净文件的提交
# 判红并点名 `b.md`，而那一笔根本没碰它。它给的是绝对路径，`-C` 换不换目录都指同一份字节。
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY

mode=--index
case "${1:-}" in
  --index | --tree) mode="$1"; shift ;;
  "") ;;
  *) case "${1:-}" in -*) echo "usage: $0 [--index|--tree] [repo]" >&2; exit 2 ;; esac ;;
esac

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# `COGNEVA_PRIVATE_ROOT` 只给自检用：它要造一份「带随机后缀的临时工作树」，证明名单推的是
# 仓而不是这个工作目录。生产路径永远从脚本自己的位置推。
# 私有仓在哪：本脚本随公开仓走，推不出那个路径，只能问本机的配置。
#
# 早先它住在私有仓里，路径从**脚本自己的位置**推出来。搬到公开仓之后那条推法失效，
# 而把路径写死在这里等于把要守的那个词放进被守的树里——所以它从 git config 读
# （`cogneva.privateRoot`，本地配置，不随 clone 走），环境变量仍可覆盖（自检用）。
# 没配就是「判不了」：3，不是 0。
PRIVATE_ROOT="${COGNEVA_PRIVATE_ROOT:-$(git config --get cogneva.privateRoot 2>/dev/null || true)}"
# 没配就是**判不了**，不是干净：名单是从私有仓推出来的，推不出来时「一处也没有」
# 与「手里根本没有名单」在输出上一模一样，而后者必须报 3。这一格不能省——省掉之后
# 这道门在没有私有仓的机器上会安静地通过每一次提交，而那正是它存在要防的事。
if [ -z "${PRIVATE_ROOT:-}" ]; then
  printf '私有标识门（本地）：没有配置私有仓（`git config cogneva.privateRoot`）——这一次没有被判定\n' >&2
  exit 3
fi

not_judged() {
  printf '私有标识门（本地）：这次没有判定 —— %s\n' "$*" >&2
  exit 3
}

PUBLIC_ROOT="${1:-$(env -u GIT_DIR -u GIT_WORK_TREE git rev-parse --show-toplevel 2>/dev/null)}"
[ -n "${PUBLIC_ROOT:-}" ] || not_judged "不在 git 工作树里，也没给被守的仓"
PUBLIC_ROOT="$(cd "$PUBLIC_ROOT" 2>/dev/null && pwd)" || not_judged "被守的仓不存在：${1:-}"
git -C "$PUBLIC_ROOT" rev-parse --git-dir >/dev/null 2>&1 || not_judged "$PUBLIC_ROOT 不是 git 仓"

# 守的是**别的树**：私有仓自己的提交里当然会出现它自己的名字（那就是它自己）。
if [ "$PUBLIC_ROOT" = "$PRIVATE_ROOT" ]; then
  printf '私有标识门（本地）：被守的树就是私有仓自己 —— 不判。\n'
  exit 0
fi

# ── 名单：从私有仓自己的名字推导 ────────────────────────────────────────────
# 名字要从**仓**推，不能从这个工作目录推：这套门自己是在一份临时工作树里被自检跑起来的
# （`.cog-judged.<随机>`），拿它的目录名当仓名会切出 `judged` 这类词——英文散文里到处
# 都是，整棵树假红。工作树的 common-dir 指回真仓，那里才是名字。
#
# 这两条 `-C` 能问到私有仓，靠的是文件头那次 `unset`：钩子环境里的 `GIT_DIR` 会把它们
# 答成被守树的名字与地址，判据就此反向（详见文件头）。
private_repo_dir="$PRIVATE_ROOT"
if common="$(git -C "$PRIVATE_ROOT" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" &&
  [ -n "$common" ]; then
  common="${common%/}"
  [ "$(basename "$common")" = ".git" ] && private_repo_dir="$(dirname "$common")"
fi
private_base="$(basename "$private_repo_dir")"
# 点开头是临时工作树的形状，不是仓名：宁可少一个词，也不要一个到处误命中的词。
case "$private_base" in .*) private_base="" ;; esac

private_names=()
[ -n "$private_base" ] && private_names+=("$private_base")
origin_url="$(git -C "$PRIVATE_ROOT" remote get-url origin 2>/dev/null || true)"
if [ -n "$origin_url" ]; then
  repo="${origin_url##*/}"
  repo="${repo%.git}"
  [ -n "$repo" ] && private_names+=("$repo")
fi

# 被守那棵树自己的名字（目录名与 origin 仓名）：那几个词出现在它自己的说明里是合法的
# （`cogneva` 在公开仓里到处都有）。目录名可能带后缀（`cogneva-work`），所以按**包含**
# 比而不是按相等比——否则换个克隆目录名，`cogneva` 就成了私有标识，每一笔都红。
# 同一个理由：问被守树「你的 origin 是谁」时也不许让 `GIT_DIR` 代答——它答的可能是别的
# 树，而这一格是私有名字的豁免名单，答错一次就等于把私有仓的地址记成公开仓的。
public_names="$(basename "$PUBLIC_ROOT")"
public_origin="$(git -C "$PUBLIC_ROOT" remote get-url origin 2>/dev/null || true)"
if [ -n "$public_origin" ]; then
  public_origin="${public_origin##*/}"
  public_names="$public_names ${public_origin%.git}"
fi

tokens=()
for name in "${private_names[@]}"; do
  [ -n "$name" ] || continue
  tokens+=("$name")
  # 整名之外还要判它的每一段：说明里最省事的写法是只写段名，不写整名。
  for seg in $(printf '%s' "$name" | tr -- '-_.' '   '); do
    skip=0
    for pub in $public_names; do
      case "$pub" in
        *"$seg"*) skip=1 ;;
      esac
    done
    [ "$skip" = 0 ] || continue
    # 太短的段（1-2 个字符）到处误命中，判了也没人看得懂它为什么红。
    [ "${#seg}" -ge 3 ] || continue
    tokens+=("$seg")
  done
done

# 去重，顺带丢掉空串。
mapfile -t tokens < <(printf '%s\n' "${tokens[@]}" | awk 'NF && !seen[$0]++')
[ "${#tokens[@]}" -gt 0 ] || not_judged "推不出任何私有标识（私有仓目录名与 origin 都没给出可切分的名字）"

# 词界：前后都得是非标识字符，否则段名会在一个更长的普通词里被误判。
pattern=""
for t in "${tokens[@]}"; do
  esc="$(printf '%s' "$t" | sed 's/[][\\.^$*+?(){}|]/\\&/g')"
  pattern="${pattern:+$pattern|}(^|[^A-Za-z0-9_-])${esc}([^A-Za-z0-9_-]|\$)"
done

# ── 判 ──────────────────────────────────────────────────────────────────────
hits=""
if [ "$mode" = --tree ]; then
  hits="$(git -C "$PUBLIC_ROOT" grep -inE -- "$pattern" 2>/dev/null || true)"
else
  # 只取新增行：`+++ b/…` 是文件头，不是内容；删掉的行读的就是存量，不归这一笔。
  # 行号跟着 hunk 头走：`@@ -a,b +c,d @@` 里的 c 才是这一行在文件里的位置，拿 diff 的
  # 行序当行号会让读到判词的人去翻一个不存在的行。
  hits="$(git -C "$PUBLIC_ROOT" diff --cached -U0 --no-color --no-ext-diff 2>/dev/null |
    awk '
      /^\+\+\+ / {
        file = $2
        sub(/^[ab]\//, "", file)
        next
      }
      /^@@ / {
        if (match($0, /\+[0-9]+/)) line = substr($0, RSTART + 1, RLENGTH - 1) + 0
        next
      }
      /^\+/ { print file ":" line ":" substr($0, 2); line++; next }
    ' | grep -inE -- "$pattern" || true)"
  # 文件名本身也是会漏出去的字面量（`<私有仓>.md` 这种）。
  named="$(git -C "$PUBLIC_ROOT" diff --cached --name-only 2>/dev/null | grep -iE -- "$pattern" || true)"
  [ -z "$named" ] || hits="${hits}${hits:+
}${named}"
  [ -n "$hits" ] || { printf '私有标识门（本地）：这一笔新增的字节里没有私有仓的名字。\n'; exit 0; }
fi

if [ -z "$hits" ]; then
  printf '私有标识门（本地）：被守的树（%s 个文件）里没有私有仓的名字。\n' \
    "$(git -C "$PUBLIC_ROOT" ls-files | wc -l | tr -d ' ')"
  exit 0
fi

{
  printf '私有标识门（本地）：公开树里出现了私有仓的名字（名单从私有仓自身推导：%s）：\n' \
    "${tokens[*]}"
  printf '%s\n' "$hits" | sed -n '1,40p'
  [ "$(printf '%s\n' "$hits" | wc -l | tr -d ' ')" -le 40 ] ||
    printf '……（共 %s 行，只印前 40）\n' "$(printf '%s\n' "$hits" | wc -l | tr -d ' ')"
  cat <<'EOF'

读到这行的人多半在写「这条读数不在本仓」这类说明。*不在本仓* 是公开读者需要的全部；
点名私有仓与它的路径，公开读者跟不过去，而它本身就是要防的那件事。

改法：把「私有仓 X」「X/scripts/…」换成「不在本仓」或「另一棵树」，脚本名可以留（它不会
泄露仓的名字）。名单不写进公开树——写进去等于把它要防的东西放进被它守的那棵树里。

要照常提交（例如这一条判据本身在修）：`git commit --no-verify`，并在信息里写清为什么。
EOF
} >&2
exit 1
