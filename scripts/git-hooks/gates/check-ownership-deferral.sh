#!/usr/bin/env bash
# Find the places where a plan hands a decision to a person instead of naming
# the condition and the predicate that decide it.
#
# Why this exists rather than another memory line: the rule has been in
# `memory/feedback_meta_autonomy_decide_from_principles.md` since 2026-09-21 and
# has been corrected eight times since, including once on 2026-10-12 with 26
# occurrences inside a single document written that same day. A memory is a
# document, and a document does not stop anything at the moment it is written.
# What stops it is a reading taken on the artefact itself.
#
# What counts as a hit is a *claim of ownership over a decision*, not the bare
# word: the two frozen ledgers are literally named "待拍板清单" and "任务树跟踪表",
# so quoting them is a reference, not a deferral. Those lines are reported
# separately as excluded, never silently dropped -- a check whose exemptions are
# invisible cannot be told from a check that stopped looking.
#
# Usage:
#   check-ownership-deferral.sh FILE...      # or: check-ownership-deferral.sh < list
#   check-ownership-deferral.sh -            # read paths from stdin
#   check-ownership-deferral.sh --staged     # this commit's own .md, from the index
#
# `--staged` is what the commit path uses. It is an explicit flag rather than an
# automatic switch on `GIT_INDEX_FILE`: the test fixtures inherit the environment,
# so a mode that turns itself on when a variable is present would quietly judge a
# different population inside the very suite that proves the other mode works.
#
# Exit 1 when any line defers a decision, so it can gate a commit; exit 2 when the
# input could not be read (which is not the same as "nothing found"); exit 0 with a
# count either way, because "how many" is the reading that matters over time.

set -uo pipefail

# 仓根在几级之上：这份脚本在私有仓里住 `scripts/gates/`（上两级），搬进公开仓的
# `scripts/git-hooks/gates/` 之后是三级。按固定深度写死，搬一次就指错一次——
# 而指错的后果是「读不到索引」，与「没有发现」在输出上不同，但都退非零。
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# The shapes live in one file because two readers share this judgement: this
# script, and the hook that gates a tool call before it lands.
#
# Located by trying the places it can be, not by a fixed depth. Until the
# 2026-10-17 move this script sat beside the table; afterwards the `source`
# still pointed at its own directory, so it failed *after* the script had
# already started judging -- and the unbound shape variables turned that into
# exit 1, which is this script's "a line defers a decision". A broken table
# therefore read as a finding. Unreadable is not a verdict.
for _patterns in "$(dirname "${BASH_SOURCE[0]}")" \
                 "$(dirname "${BASH_SOURCE[0]}")/../principles"; do
    if [ -f "$_patterns/principle-patterns.sh" ]; then
        source "$_patterns/principle-patterns.sh"
        break
    fi
done
if [ "${PRINCIPLE_SHAPES_OK:-0}" != "1" ]; then
    echo "check-ownership-deferral: 形状表读不到，门不能假装「没发现」。" >&2
    exit 2
fi

SHAPES=("${DEFERRAL_SHAPES[@]}")
REFERENCE="$DEFERRAL_EXEMPT"
STRIKING="$DEFERRAL_STRIKING"

paths=()
skipped_generated=0
src=disk
TMP=""
if [ "${1:-}" = "--staged" ]; then
    src=index
    shift
elif [ "${1:-}" = "-" ]; then
    src=stdin
    shift
fi

# 生成物整份跳过，三种取法共用。`specs/ledger/` 是变更台账的投影：那里面每一行都是
# **已经发生过的**记录，「用户拍板选项 B」在叙述一次决定，不是又一次把决定交出去。
# 判生成物等于判生成器，而它给出的改法只能是「改历史」。实测 2026-11-03：默认窗 5 条
# 命中里 4 条是投影里的历史叙述，只剩 1 条是真的——一条判据九成命中在历史记录上，
# 读者就会把它整个忽略（脚本自己的注释里已经写过这个病）。
#
# 跳过不随取法变：那是**文件的性质**，不是这一跑怎么找到它的。一笔重新生成的台账投影
# 里必然带着历史叙述，按盘算会红、按索引算不红，就成了一条随取法翻面的判据。
skip_generated() { # <路径> → 0 = 跳过（并计数）
    case "$1" in
        ./specs/ledger/* | specs/ledger/*)
            skipped_generated=$((skipped_generated + 1))
            return 0 ;;
    esac
    return 1
}

raw=()
if [ "$src" = index ]; then
    # 判**这一笔将被提交的字节**，不是盘。提交门给的是私有索引（HEAD ∪ 本笔路径）；
    # 判盘会让判词变成「隔壁会话此刻写到了哪」的函数，那正是这一族要治的病。
    # 与 `check-readings-table.sh` 同一处置：`GIT_INDEX_FILE` 认两种形态，索引不在就退 2，
    # 不悄悄换一份去判。
    TMP="$(mktemp -d)" || exit 2
    trap 'rm -rf "$TMP"' EXIT
    if [ -n "${GIT_INDEX_FILE-}" ]; then
        case "$GIT_INDEX_FILE" in
            /*) idxpath="$GIT_INDEX_FILE" ;;
            *) idxpath="$ROOT/$GIT_INDEX_FILE" ;;
        esac
    else
        idxpath="$(git -C "$ROOT" rev-parse --absolute-git-dir 2>/dev/null)/index"
    fi
    if [ ! -f "$idxpath" ]; then
        echo "check-ownership-deferral: 要判的那份索引不在（$idxpath）——" >&2
        echo "  这不是「没有把决定交给人的句子」，是这一跑没看成。" >&2
        exit 2
    fi
    git -C "$ROOT" diff --cached -M --name-status --diff-filter=ACMR -- '*.md' \
        >"$TMP/changed" 2>"$TMP/err"
    drc=$?
    if [ "$drc" -gt 1 ]; then
        echo "check-ownership-deferral: 这一笔的改动读不出来（git diff --cached 退出码 ${drc}）——" >&2
        sed 's/^/  /' "$TMP/err" >&2
        echo "  这不是「这一笔没有 .md」，是这一跑没看成。" >&2
        exit 2
    fi
    while IFS=$'\t' read -r st a b; do
        [ -n "$a" ] || continue
        case "$st" in
            R* | C*) raw+=("$b") ;;
            *) raw+=("$a") ;;
        esac
    done <"$TMP/changed"
elif [ "$src" = stdin ]; then
    while IFS= read -r p; do [ -n "$p" ] && raw+=("$p"); done
elif [ "$#" -gt 0 ]; then
    raw=("$@")
else
    # Default: today's and recent documents, which is where a new plan lands.
    while IFS= read -r p; do [ -n "$p" ] && raw+=("$p"); done \
        < <(find . -name '*.md' -newermt '-2 days' -not -path './.git/*' | sort)
fi

if [ "${#raw[@]}" -gt 0 ]; then
    for p in "${raw[@]}"; do
        skip_generated "$p" && continue
        paths+=("$p")
    done
fi

if [ "$src" = index ]; then
    printf 'check-ownership-deferral: 判这一笔的 %d 份 .md（取自索引 %s，不是盘上此刻的字节）。\n' \
        "${#paths[@]}" "$idxpath"
fi

[ "${#paths[@]}" -gt 0 ] || { echo "check-ownership-deferral: no files to read; $skipped_generated generated file(s) not read."; exit 0; }

pattern="$(IFS='|'; echo "${SHAPES[*]}")"
hits=0
excluded=0

emit_lines() { # <路径> → `行号:正文`，来源按取法
    if [ "$src" = index ]; then
        git -C "$ROOT" cat-file -p ":0:$1" 2>/dev/null | grep -n ''
    else
        grep -n '' "$1" 2>/dev/null
    fi
}

for f in "${paths[@]}"; do
    if [ "$src" = index ]; then
        # 索引里读不出这一份就得说，不能当它干净：少读一份而印 0，与「这一份没有那句话」
        # 逐字同形，而读的人只会看见 0。
        if ! git -C "$ROOT" cat-file -e ":0:$f" 2>/dev/null; then
            echo "check-ownership-deferral: 索引里读不出这一份：$f ——" >&2
            echo "  这不是「没有把决定交给人的句子」，是这一跑没看成。" >&2
            exit 2
        fi
    else
        [ -f "$f" ] || continue
    fi
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        text="${line#*:}"
        printf '%s' "$text" | grep -qE "$pattern" || continue
        if printf '%s' "$text" | grep -qE "$REFERENCE" || printf '%s' "$text" | grep -qE "$STRIKING"; then
            excluded=$((excluded + 1))
            printf '  (excluded) %s:%s\n' "$f" "$line"
        elif ! printf '%s' "$(strip_named_spans "$text")" | grep -qE "$pattern"; then
            # The phrase was quoted, so the line names it rather than uses it.
            # The document most likely to quote a deferral is the one reporting
            # that deferrals leak, and a check whose hits are mostly its own
            # subject gets ignored -- measured 2026-10-12, when nine quoted
            # phrases in one file read as nine plans. Reported, not dropped, and
            # the stripping itself lives in the shared shapes file so this
            # reader and the tool-call gate cannot drift apart on it.
            excluded=$((excluded + 1))
            printf '  (excluded: quoted) %s:%s\n' "$f" "$line"
        else
            hits=$((hits + 1))
            printf 'DEFERS  %s:%s\n' "$f" "$line"
        fi
    done < <(emit_lines "$f")
done

echo
echo "check-ownership-deferral: $hits line(s) hand a decision to a person; $excluded excluded as references or as the rule being struck; $skipped_generated generated file(s) not read."

# A hit is not a style complaint: the step it appears on has no condition and no
# predicate, which means nobody can tell later whether it was ever decided. That
# is why this exits non-zero rather than printing a warning.
[ "$hits" -eq 0 ] || exit 1
