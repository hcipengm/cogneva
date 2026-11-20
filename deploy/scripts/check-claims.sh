#!/usr/bin/env bash
# 说出口的每一句结论，都要带上它是怎么被量到的。
#
# 这不是文风检查，是把这一轮反复犯的三类错变成判据：
#
#   1. **说「修好了」，但没给读数。** 2026-11-19 现场：watchdog 我说修好了，
#      实际只改了单元文件、没重启，提示词照旧每两分钟一次。判据：提交信息里出现
#      「修好 / 修复 / fixed」就必须有一行
#          Reading: <哪条读数> <改前> → <改后>
#      没有就拒。
#   2. **引一条规则来支持一个结论，但那条规则不适用。** 2026-11-19 现场：我拿
#      「第 4 条：工作集超出一次能读完」去解释把 1709 条拆成 4 批，而读得完、真正的
#      限制是编辑量——规则里没有这一条。判据：提交信息里出现 `第 N 条` 或 `原则`
#      的引用，就必须有一行
#          Rule: <规则所在的文件或台账行>
#      点不出出处就拒。
#   3. **声称普查完备，但没有跑过普查。** 判据：出现「普查 + 完备」就必须有一行
#          Census: <跑了哪个工具> <四面各多少条>
#
# 只判这一推新增的提交（origin/main..HEAD），不翻历史：历史里的旧句子改不动，
# 一条永久红的门最后会被删掉。
set -uo pipefail

# 第一个参数是仓库根（这一族门的约定）；不给时才按脚本自己的位置推。
root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
base="${COGNEVA_PUSH_BASE:-origin/main}"

# 读不到基线 ⇒ 退 3（「没判」），不是 0：一条在空树上也绿的判据，它的绿不依赖它
# 自称读的那份东西（`an_empty_tree_is_never_green` 就是这个意思）。
git -C "$root" rev-parse --verify "$base" >/dev/null 2>&1 || {
    echo "claims: 找不到基线 $base，这一推没判。" >&2
    exit 3
}

# 判的是**声明**，不是提到。提交信息里写「说修好了要带 Reading:」是在描述规则，
# 不是在声称一次修复——把引号或反引号里的字先抹掉再判，否则这条门会去咬每一个
# 解释它自己的人（2026-11-19 实测：机制那一笔的说明里写了那个词，门当场把那一笔拒了）。
strip_mentions() {
    sed -E 's/`[^`]*`//g; s/"[^"]*"//g; s/“[^”]*”//g; s/「[^」]*」//g' <<<"$1"
}

shas="$(git -C "$root" rev-list "$base"..HEAD)"
if [ -z "$shas" ]; then
    echo "claims: 这一推没有提交，没判。" >&2
    exit 3
fi

fail=0
for sha in $shas; do
    raw="$(git -C "$root" show -s --format=%B "$sha")"
    msg="$(strip_mentions "$raw")"

    # 英文那一条只认**动词位置的声明**：行首（或句号／冒号之后）的 `fix(ed|es)`。
    # 原来写的是 `\bfixed\b`，那是**裸单词**，于是把这五类形容词用法全算成了声明：
    # `a fixed batch`、`no fixed subject`、`the fixed validator`、`rules are fixed here`、
    # `the shape the gate was fixed for`——2026-11-20 实测，五笔功能提交因此被拒。
    # 判据要按**出现形式**判，不是按裸子串判：`fixed` 作形容词时前面总有一个限定词或系动词。
    if printf '%s' "$msg" | grep -qE '修好|已修复|修掉了|(^|[.!?;:][[:space:]]+)[Ff]ix(ed|es)\b' &&
        ! printf '%s' "$msg" | grep -qE '^Reading:'; then
        printf 'FAIL: %s 说「修好了」，但没有一行 `Reading: <哪条读数> <改前> → <改后>`.\n' "${sha:0:9}" >&2
        fail=1
    fi

    if printf '%s' "$msg" | grep -qE '第[[:space:]]*[0-9一二三四]+[[:space:]]*条|原则' &&
        ! printf '%s' "$msg" | grep -qE '^Rule:'; then
        printf 'FAIL: %s 引了规则，但没有一行 `Rule: <规则出处>`——引错规则的代价比不引更大。\n' "${sha:0:9}" >&2
        fail=1
    fi

    if printf '%s' "$msg" | grep -qE '普查' && printf '%s' "$msg" | grep -qE '完备' &&
        ! printf '%s' "$msg" | grep -qE '^Census:'; then
        printf 'FAIL: %s 声称普查完备，但没有一行 `Census: <工具> <四面各多少条>`.\n' "${sha:0:9}" >&2
        fail=1
    fi
done

if [ "$fail" -ne 0 ]; then
    cat >&2 <<'EOF'

这三行不是格式要求，是把「说出口就成立」改成「说出口要先量过」：
  Reading: cogneva_task_events_rows 4,102,331 → 4,102,331（未删一行）
  Rule: specs/work-ledger-b6f42e.md S52 第 4 条
  Census: bounds-census.py markers 791 / consts 592 / config 569 / destroys 334
EOF
    exit 1
fi

echo "claims: 这一推的结论都带上了出处。"
