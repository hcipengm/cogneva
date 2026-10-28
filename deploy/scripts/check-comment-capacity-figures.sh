#!/usr/bin/env bash
#
# A comment states a capacity — the prose rule "this repository describes the
# software, not the machine it happened to run on" turned into a reading.
#
# Why it is a rule at all: a comment outlives the machine it was written on, and
# a figure like "the overlay layer is N GB" or "N GiB of headroom left" is true
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
# manifests, the deployment scripts and the Rust sources, plus the alert
# annotation values (`summary:` / `description:`), which are the same mistake in
# a louder place: an operator on any deployment reads those. A capacity figure is
# a number followed by a binary or decimal unit. A bare number with no unit is
# not judged: "512" in a comment may be anything, and a rule that guessed would
# be refused the first time it guessed wrong.
#
# Two scope decisions, written down so they are not silent:
#   - `deploy/rendered/` is not read. It is generated from the chart by
#     `render-deploy.sh`; the same comment is judged in the template that owns
#     it. Reading the generated copy would turn an honest chart edit into a
#     finding against a file nobody hand-writes.
#   - Markdown is not read. Changelogs, READMEs and design notes are records of
#     what was true when they were written, and the wide ones quote sizes of
#     outside artifacts (a pinned dataset, a registry limit) that are properties
#     of that artifact. If one of those starts quoting this host, that is a
#     documentation review, not something a line-wise check can judge.
#
# Existing debt is baselined by line content rather than waived by file, so a
# violation that moves to another line is still the same violation and the
# baseline cannot be grown by reformatting. A hit that is not in the baseline
# fails the check, and `--update-baseline` itself refuses to grow the baseline:
# new debt is fixed, never recorded. (Re-scoping the check is the one case that
# legitimately moves the count, and it says so with `--allow-grow`.)
#
# Usage: check-comment-capacity-figures.sh [--stock] [--update-baseline [--allow-grow]] [file ...]
#        (default: every manifest, deploy script and Rust source the tree tracks)
#   --stock            print every violation, baselined or not, and exit 0
#   --update-baseline  rewrite the baseline from what is in the tree now
#   --allow-grow       let --update-baseline write more lines than it had before
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/../.." && pwd)
baseline_file="$here/comment-capacity-figures.baseline"

stock=0
update=0
grow=0
files=()
for arg in "$@"; do
    case "$arg" in
        --stock)           stock=1 ;;
        --update-baseline) update=1 ;;
        --allow-grow)      grow=1 ;;
        -*)                echo "usage: $0 [--stock] [--update-baseline [--allow-grow]] [file ...]" >&2; exit 2 ;;
        *)                 files+=("$arg") ;;
    esac
done

if [ "${#files[@]}" -eq 0 ]; then
    command -v git >/dev/null || { echo "FAIL: git is needed to find the tree's files" >&2; exit 1; }
    # `deploy/rendered/` 是 render-deploy.sh 从 chart 生成的：同一句注释由拥有它的
    # 模板负责，读生成物只会把一次正当的 chart 改动记到没人手写的文件头上。
    # 路径一律补成绝对路径：git 给的是相对仓库根的路径，而下面按调用者的 cwd 去读；
    # 从别的目录跑（排队作业、钩子）时相对路径一个都不存在，那就是退出码 0 的假绿。
    mapfile -t files < <(git -C "$root" ls-files 'deploy/**/*.yaml' 'deploy/**/*.yml' \
        'deploy/**/*.sh' 'crates/**/*.rs' | grep -v '^deploy/rendered/' | sed "s#^#$root/#")
fi
[ "${#files[@]}" -gt 0 ] || { echo "FAIL: no file to read" >&2; exit 1; }

yaml_files=()
for f in "${files[@]}"; do
    case "$f" in *.yaml|*.yml) yaml_files+=("$f") ;; esac
done

# 一条注释行：YAML 的 `#`、Rust 的 `//` 家族与块注释的 `*` 续行。
# 一条告警注解：`summary:` / `description:` 这类值，运维在任何部署上都读得到。
# 一条容量数字：数字（可带小数）紧跟二进制或十进制单位，允许中间有空格。
# 归属标记：说清这个数是谁的——实测/本机/默认/例如，或一个日期。
unit='(KiB|MiB|GiB|TiB|PiB|KB|MB|GB|TB|PB|Ki|Mi|Gi|Ti)'
provenance='实测|本机|这台|该机|本地|默认|例如|如 |[0-9]{4}-[0-9]{2}-[0-9]{2}|measured|on this host|by default|e\.g\.'
# 读不出来的文件不是「这行干净」：一条读失败交回来零命中，与一份没有容量数字的
# 文件在输出上完全同形，而这条门的绿正是「零命中」。所以 grep 自己的退出码要收下：
# 大于 1 是它读不动（权限、路径是目录、名字不存在），按红算；退出码 1 才是没有命中。
comment_lines=""
grep_status=0
comment_lines="$(grep -nH -E "^[[:space:]]*(#|//|/\*|\*)" "${files[@]}" 2>&1)" || grep_status=$?
if [ "$grep_status" -gt 1 ]; then
    echo "FAIL: 读不出这些文件（grep 退出 ${grep_status}）：${comment_lines}——没读到就判不了里面有没有容量数字" >&2
    exit 1
fi
annotation_lines=""
if [ "${#yaml_files[@]}" -gt 0 ]; then
    annotation_lines="$(grep -nH -E "^[[:space:]]*(summary|description|message|title):" "${yaml_files[@]}" 2>&1)" || grep_status=$?
    if [ "$grep_status" -gt 1 ]; then
        echo "FAIL: 读不出这些 YAML（grep 退出 ${grep_status}）：${annotation_lines}——没读到就判不了告警注解里的容量数字" >&2
        exit 1
    fi
fi

hits="$(
    {
        printf '%s\n' "$comment_lines"
        printf '%s\n' "$annotation_lines"
    } | grep -E "[0-9]+(\.[0-9]+)?[[:space:]]*${unit}([^A-Za-z0-9]|$)" \
      | grep -vE "$provenance" || true
)"

if [ "$update" -eq 1 ]; then
    new_file="$(mktemp)" || exit 1
    printf '%s\n' "$hits" | grep -v '^$' | while IFS= read -r line; do
        printf '%s\n' "${line#*:}" | sed -E 's/^[^:]*://' | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//' \
            | sha1sum | cut -d' ' -f1
    done | sort >"$new_file"
    old_count=0
    [ -f "$baseline_file" ] && old_count=$(grep -c '' "$baseline_file")
    new_count=$(grep -c '' "$new_file")
    if [ "$new_count" -gt "$old_count" ] && [ "$grow" -ne 1 ]; then
        # 基线只许缩：新账要改掉，不是记下来。放开基线就等于把这道门变成一道记录。
        rm -f "$new_file"
        echo "REFUSED: 基线要长到 $new_count 行（原 $old_count 行）。" >&2
        echo "         新出现的命中要改：写机制与比例，或标明这是谁的数（实测 YYYY-MM-DD／本机／默认／例如）。" >&2
        echo "         只有「这次扩了扫描范围」这类口径变更才配 --allow-grow。" >&2
        exit 1
    fi
    mv "$new_file" "$baseline_file"
    echo "baseline rewritten: $new_count 行（原 $old_count 行，只许缩）"
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
        report+="${line#"$root"/}"$'\n'
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
