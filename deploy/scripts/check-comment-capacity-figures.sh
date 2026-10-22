#!/usr/bin/env bash
#
# A comment states a capacity — the prose rule "this repository describes the
# software, not the machine it happened to run on" turned into a reading.
#
# Why it is a rule at all: a comment outlives the machine it was written on, and
# a figure like "the overlay layer is 47 GB" or "13 GiB of headroom left" is true
# of exactly one host at exactly one moment. A reader on another deployment does
# not see a stale note; they see a specification. They will size something
# against it, and nothing downstream contradicts them: comments take no part in
# rendering, in the parity comparison, or in any alert — a wrong figure there has
# no reader that can go red.
#
# What may be stated, and in which form:
#   - a bound the software itself enforces, where the number is the code's own
#     constant rather than an observation of a run (chart defaults, a limit the
#     template writes). Those are read from the configuration, not from prose, so
#     this check does not look at them — it reads comments only.
#   - a figure that says whose figure it is: an observation carries `实测` (and
#     the date, by the tree's habit), a local run says `本机` / `这台` / `该机`,
#     a software default says `默认`, an illustrative number says `例如` / `如 `.
#     The marker is the whole difference between "this was true here" and "this
#     is true".
#   - the mechanism, which is what the reader on another host actually needs:
#     a ratio and its source ("the reserved-block share, 5% by default on ext4,
#     tunable with `tune2fs -m`") travels; a byte count does not.
#
# The read is deliberately narrow. It judges comment lines in the deployment
# manifests and in the Rust sources, and a capacity figure is a number followed
# by a binary or decimal unit. A bare number with no unit is not judged: "512"
# in a comment may be anything, and a rule that guessed would be refused the
# first time it guessed wrong.
#
# Existing debt is baselined by line content rather than waived by file, so a
# violation that moves to another line is still the same violation and the
# baseline cannot be grown by reformatting. A hit that is not in the baseline
# fails the check; the baseline may only shrink.
#
# Usage: check-comment-capacity-figures.sh [--stock] [--update-baseline] [file ...]
#        (default: every manifest and Rust source the tree tracks)
#   --stock            print every violation, baselined or not, and exit 0
#   --update-baseline  rewrite the baseline from what is in the tree now
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/../.." && pwd)
baseline_file="$here/comment-capacity-figures.baseline"

stock=0
update=0
files=()
for arg in "$@"; do
    case "$arg" in
        --stock)           stock=1 ;;
        --update-baseline) update=1 ;;
        -*)                echo "usage: $0 [--stock] [--update-baseline] [file ...]" >&2; exit 2 ;;
        *)                 files+=("$arg") ;;
    esac
done

if [ "${#files[@]}" -eq 0 ]; then
    command -v git >/dev/null || { echo "FAIL: git is needed to find the tree's files" >&2; exit 1; }
    mapfile -t files < <(cd "$root" && git ls-files 'deploy/**/*.yaml' 'deploy/**/*.yml' 'crates/**/*.rs')
fi
[ "${#files[@]}" -gt 0 ] || { echo "FAIL: no file to read" >&2; exit 1; }

# 一条注释行：YAML 的 `#`、Rust 的 `//` 家族与块注释的 `*` 续行。
# 一条容量数字：数字（可带小数）紧跟二进制或十进制单位，允许中间有空格。
unit='(KiB|MiB|GiB|TiB|PiB|KB|MB|GB|TB|PB|Ki|Mi|Gi|Ti)'
hits="$(
    grep -nH -E "^[[:space:]]*(#|//|/\*|\*)" "${files[@]}" 2>/dev/null \
        | grep -E "[0-9]+(\.[0-9]+)?[[:space:]]*${unit}([^A-Za-z0-9]|$)" \
        | grep -vE '实测|本机|这台|该机|本地|默认|例如|如 ' || true
)"

if [ "$update" -eq 1 ]; then
    printf '%s\n' "$hits" | grep -v '^$' | while IFS= read -r line; do
        printf '%s\n' "${line#*:}" | sed -E 's/^[^:]*://' | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//' \
            | sha1sum | cut -d' ' -f1
    done | sort >"$baseline_file"
    echo "baseline rewritten: $(wc -l <"$baseline_file") 行（只许缩）"
    exit 0
fi

total=0
new=0
report=""
while IFS= read -r line; do
    [ -n "$line" ] || continue
    total=$((total + 1))
    text="$(printf '%s\n' "$line" | sed -E 's/^[^:]+:[0-9]+://' | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
    hash="$(printf '%s\n' "$text" | sha1sum | cut -d' ' -f1)"
    known=0
    if [ -f "$baseline_file" ]; then
        grep -qxF "$hash" "$baseline_file" && known=1
    fi
    if [ "$stock" -eq 1 ] || [ "$known" -eq 0 ]; then
        report+="$line"$'\n'
        [ "$known" -eq 0 ] && new=$((new + 1))
    fi
done <<<"$hits"

if [ "$stock" -eq 1 ]; then
    printf '%s' "$report"
    echo "COMMENT CAPACITY STOCK：$total 处（基线 $( [ -f "$baseline_file" ] && wc -l <"$baseline_file" || echo 0) 处；只报不判）"
    exit 0
fi

if [ "$new" -gt 0 ]; then
    printf '%s' "$report" | sed 's/^/  /' >&2
    echo "FAIL: $new 条注释把某一台机器的容量写成了软件的属性（基线 $((total - new)) 处是老账）。" >&2
    echo "      改法：写机制与比例（例如「保留块占 size 的比例」），或标明这是谁的数——" >&2
    echo "      「实测 YYYY-MM-DD」「本机」「默认」「例如」。数字本身不是问题，冒充规格才是。" >&2
    exit 1
fi
echo "COMMENT CAPACITY OK：$total 处容量数字全部标明了归属（基线内）"
exit 0
