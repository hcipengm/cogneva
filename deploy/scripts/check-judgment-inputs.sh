#!/usr/bin/env bash
# principle: feedback_a_projection_needs_a_named_projector_or_it_is_just_a_copy
# 判「判词脚本读的表，树里有没有东西写它」。
#
# 病：一张表被门读成判据，却没人写它。改一格就改判词，而树里没有一句话说那一格
# 该不该在。2026-11 把登记表所在的目录逐份普查过一遍，19 份各自有了去向；这张门
# 把那一次普查变成常设判据——新长出来的手写表不能悄悄成为判据。
#
# 同一个文件跑在两种布局的树上：判词在 `scripts/` 底下的树，与判词在
# `deploy/scripts/`、`crates/*/tests/` 的树。规则、登记格式、退出码一致，只有登记表
# 的位置按布局不同（按下面的顺序找，找不到退 3）——所以两处跑的是同一份实现，不是
# 各留一份会分家的复制。判据要落在每种布局自己的提交路径上：一棵树只有别的树的
# 提交车道替它判，那它自己的推送就永远等不到红。
#
# 判的是**读法**，不是扩展名：tsv 是表，yaml 也是表。一个 `rows:` 底下几百行的
# registry 改一格就改判词，它是什么后缀都一样。所以数据面两种读法分开：
#   *.tsv *.pin *.csv —— 按**文件名**认。脚本本来就是按名字找表的。
#   *.yaml *.yml      —— 按**相对路径**认。同名 yaml 太多（`deployment.yaml` 在
#                        渲染产物、静态清单、模板里各有一份），按名字认会把一份
#                        被读、其余跟着过关；路径才是读者真正写下的那一份。
#
# 判词脚本＝这一族里能让提交变红的东西：`scripts/` 下的 `*.test.sh`、`gates/` 与
# `principles/` 下的 shell／python、公开仓的 `deploy/scripts/*.sh` 与
# `deploy/scripts/*/*.test.sh`，以及公开仓 `crates/*/tests/*.rs`（那边判词是 Rust
# 测试，不是 shell）。被读＝名字／路径在某个判词脚本里出现过。
#
# 登记表每行一份被读的表，三分类：
#   generated  生产者路径  —— 机器写的。生产者必须在，且正文里提到这张表（整行表就
#                            是全路径；通配行提到静态前缀就够）。只验这条链，不重跑生产：
#                            那一步是每张表自己的新鲜度门的事。
#   subject    判词脚本    —— 读它是为了**验它**，不是为了从里面取判据：chart 模板、
#                            values、渲染产物、静态清单、告警规则、promtool 夹具。
#                            改它那些点名的门会红，所以它不是暗中决定判词的表。
#                            判词脚本必须在判词面里，且提到这张表（整行表提到路径；
#                            通配行提到静态前缀——按目录整读的门正文里只有目录）。
#   debt       理由   到期日 —— 已知违规，带理由与到期，到期那天这门就红。它不是豁免，
#                            是欠账记录；只许减，不许再添新的（添一笔是树上看得见的
#                            一行，不添就落不了）。
# 登记表自己也在数据面里：它是一张手写的、决定别的文件判词的表，所以它给自己写一行
# debt 并每次印出来——门不给自己的账开后门。
# 第一列可以是通配：`*` 只在一段里匹配（不跨 `/`），`**` 不支持（写出来是让人以为
# 跨目录、实际不跨，宁可拒掉）；`debt` 不许通配（一份文件一行，理由与到期要对得上
# 那一份）。同一份表被两行匹配＝红：一份表两个身份，是有人没想清。
# 没有行＝红。登记了但文件没了／已经没人读＝印 `stale`，不红——清账是表主人的事，
# 而 stale 每跑一次印一次，忘不掉。
#
# 登记表从哪来，两代：终态是**图里的声明**（平台动作导入、`governance project` 写出
# `<layout>/judgment-input.tsv`，头两行是生成戳与图的版本），门读那份机器投影；导入
# 之前，同一套行写在手写的 `judgment-inputs.pin` 里，它自己登记成欠账。两种落点按
# 布局发现；**同时存在就是红**——一份分类两个答案，读哪份都是在猜。手写表没了的那
# 一天，这份门一行都不用改：投影那条路它一直在读。
#
# 为什么「有没有生产者」要人登记而不是自动识破：写文件的方式有几十种，判「手写」没有
# 可靠判法；判「登记的生产者存不存在、提没提到这张表」是二值的。于是登记是声明，链是
# 被验的声明——要骗过它得写一个真的提到这张表的生产者，那是看得见的一笔。
#
# 退出码：0 全部登记；1 有未登记／链断／欠账过期／登记表本身不合规的表；3 量不出来
# （根不在、没有登记表、没有判词脚本、没有数据文件）——「量不出来」不是绿。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PIN=""
while [ $# -gt 0 ]; do
    case "$1" in
        --root) ROOT="${2:-}"; shift 2 ;;
        --pin)  PIN="${2:-}"; shift 2 ;;
        -h|--help) printf 'usage: %s [--root PATH] [--pin PATH]\n' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) printf 'exit 2：未知参数 %s\n' "$1" >&2; exit 2 ;;
    esac
done
[ -n "$ROOT" ] && [ -d "$ROOT" ] || { printf 'exit 3：根不在：%s\n' "${ROOT:-<空>}" >&2; exit 3; }

fail=0
red() { printf 'FAIL  %s\n' "$1" >&2; fail=$((fail + 1)); }

# 登记表按布局发现，不按一台机器写死：机器投影与手写表各两处候选。发现的是**唯一
# 一份**才算发现：两份并排＝红（分类有两个答案，读哪份都是猜），一份都没有＝量不出来。
PROJECTION=0
if [ -z "$PIN" ]; then
    found=()
    for cand in scripts/data/judgment-input.tsv deploy/scripts/judgment-input.tsv \
                scripts/data/judgment-input-host.tsv deploy/scripts/judgment-input-host.tsv \
                scripts/data/judgment-inputs.pin deploy/scripts/judgment-inputs.pin; do
        [ -f "$ROOT/$cand" ] && found+=("$cand")
    done
    if [ "${#found[@]}" -gt 1 ]; then
        red "登记表有两份：$(printf '%s ' "${found[@]}")——一份分类只能有一个答案；导入那一刻删掉手写表，别让两份并排"
        exit 1
    elif [ "${#found[@]}" -eq 1 ]; then
        PIN="$ROOT/${found[0]}"
    fi
fi
[ -n "$PIN" ] || { printf 'exit 3：%s 下找不到登记表（找过 scripts/data/ 与 deploy/scripts/ 下的 judgment-input.tsv 与 judgment-inputs.pin）——这不是绿，是量法失效\n' "$ROOT" >&2; exit 3; }
case "$PIN" in
    *.pin) PROJECTION=0 ;;
    *.tsv) PROJECTION=1 ;;
    *) PROJECTION=0 ;;
esac
# 投影是机器写的，但「机器写的」要有据可查：没有生成戳的投影与手写表同形——分不出它
# 是哪一版图写的，也就分不出它是否还记得昨天那次改判。
if [ "$PROJECTION" = 1 ]; then
    if ! grep -q '^# generated_at: ' "$PIN" || ! grep -q '^# source_revision: ' "$PIN"; then
        red "投影没有生成戳：${PIN#"$ROOT/"}——头两行要 # generated_at: 与 # source_revision:；没有戳的投影与手写表同形"
    fi
fi


tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# 名单从哪来：提交车道给的是私有索引（`GIT_INDEX_FILE` 指向这一笔的字节），那时只能判
# **这一笔**——工作树里此刻躺着隔壁会话还没落的表，判它等于替别人判旧账。没有索引时
# （夹具、tarball、手跑）才扫盘，那里文件本身就是答案。
list_paths() {
    if [ -n "${GIT_INDEX_FILE:-}" ] && git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1; then
        git -C "$ROOT" ls-files -z
    else
        ( cd "$ROOT" && find . \
            \( -name .git -o -name target -o -name node_modules -o -name __pycache__ -o -name vendor \) -prune -o \
            -type f -print ) | sed 's|^\./||' | tr '\n' '\0'
    fi
}

# 数据文件（相对根的路径）、判词脚本、以及两种读法的键各一张名单。
: > "$tmp/data"
: > "$tmp/verdict"
: > "$tmp/keys"
while IFS= read -r -d '' p; do
    case "$p" in
        *.tsv|*.pin|*.csv)
            printf '%s\n' "$p" >> "$tmp/data"
            basename "$p" >> "$tmp/keys" ;;
        *.yaml|*.yml)
            printf '%s\n' "$p" >> "$tmp/data"
            printf '%s\n' "$p" >> "$tmp/keys" ;;
    esac
    case "$p" in
        scripts/*.test.sh|scripts/*/*.test.sh) printf '%s\n' "$p" >> "$tmp/verdict" ;;
        scripts/gates/*.sh|scripts/gates/*.py) printf '%s\n' "$p" >> "$tmp/verdict" ;;
        scripts/principles/*.sh|scripts/principles/*.py) printf '%s\n' "$p" >> "$tmp/verdict" ;;
        deploy/scripts/*.sh|deploy/scripts/*/*.test.sh) printf '%s\n' "$p" >> "$tmp/verdict" ;;
        crates/*/tests/*.rs) printf '%s\n' "$p" >> "$tmp/verdict" ;;
    esac
done < <(list_paths)
LC_ALL=C sort -u -o "$tmp/data" "$tmp/data"
LC_ALL=C sort -u -o "$tmp/verdict" "$tmp/verdict"
LC_ALL=C sort -u -o "$tmp/keys" "$tmp/keys"
if [ ! -s "$tmp/data" ]; then
    printf 'exit 3：%s 下一张数据文件都没扫到——这不是绿，是量法失效\n' "$ROOT" >&2
    exit 3
fi
if [ ! -s "$tmp/verdict" ]; then
    printf 'exit 3：%s 下一份判词脚本都没找到——这不是绿，是量法失效\n' "$ROOT" >&2
    exit 3
fi

verdict_files=()
while IFS= read -r p; do verdict_files+=("$ROOT/$p"); done < "$tmp/verdict"
# 一次扫完：命中的就是「有判词脚本按名字／路径找它」的表。`-o` 印出的是模式本身，
# 也就是这张表被认出来的那把键。
grep -hoF -f "$tmp/keys" -- "${verdict_files[@]}" | LC_ALL=C sort -u > "$tmp/read"
declare -A READ=()
while IFS= read -r k; do READ["$k"]=1; done < "$tmp/read"
declare -A VERDICT=()
while IFS= read -r p; do VERDICT["$p"]=1; done < "$tmp/verdict"

# 通配只在一段里匹配：`*` 不跨 `/`。写出来是让人以为跨目录、实际不跨的 `**` 直接拒。
glob_to_ere() { # <pattern> → ERE（调用方自己加 ^$）
    local p="$1" out='' c i
    for ((i = 0; i < ${#p}; i++)); do
        c="${p:i:1}"
        case "$c" in
            '*') out+='[^/]*' ;;
            '?') out+='[^/]' ;;
            '.'|'+'|'('|')'|'['|']'|'{'|'}'|'^'|'$'|'|'|'\') out+="\\$c" ;;
            *) out+="$c" ;;
        esac
    done
    printf '%s' "$out"
}

declare -A P_CLASS=() P_DETAIL=() P_EXP=() P_RE=() P_PREFIX=() P_HIT=()
if [ -f "$PIN" ]; then
    while IFS=$'\t' read -r p c d e; do
        case "$p" in ''|\#*) continue ;; esac
        case "$p" in
            *'**'*) red "登记行的通配写成了 **：$p——只支持一段里的 *" ; continue ;;
            /*) red "登记行是绝对路径：$p——登记相对扫描根写" ; continue ;;
        esac
        pre="${p%%\**}"
        if [ -z "$pre" ]; then
            red "登记行没有静态前缀：$p——通配要从一个具体的名字开始，否则整棵树一个身份"
            continue
        fi
        # 欠账是一份文件的事：一行欠账盖住一个目录，等于把一个目录列成「有理由的违规」，
        # 而理由是对着哪一份写的、到期该修哪一份，都读不出来了。
        case "$p:$c" in
            *'*'*:debt)
                red "欠账行不能通配：$p——一份文件一行，理由和到期要对得上那一份"
                continue ;;
        esac
        P_CLASS["$p"]="$c"
        P_DETAIL["$p"]="${d:-}"
        P_EXP["$p"]="${e:-}"
        P_RE["$p"]="^$(glob_to_ere "$p")$"
        P_PREFIX["$p"]="${p%%\**}"
        P_HIT["$p"]=0
    done < "$PIN"
fi

total=0 read_n=0 unread_n=0 generated_n=0 subject_n=0 debt_n=0 stale_n=0
while IFS= read -r rel; do
    total=$((total + 1))
    key="$rel"
    case "$rel" in
        *.tsv|*.pin|*.csv) key="$(basename "$rel")" ;;
    esac
    if [ -z "${READ[$key]:-}" ]; then
        unread_n=$((unread_n + 1))
        continue
    fi
    read_n=$((read_n + 1))
    matched=()
    for pat in "${!P_CLASS[@]}"; do
        if [[ "$rel" =~ ${P_RE[$pat]} ]]; then matched+=("$pat"); fi
    done
    if [ "${#matched[@]}" -eq 0 ]; then
        red "没有登记：$rel 被判词脚本读，而树里没有一行说它由谁写"
        continue
    fi
    if [ "${#matched[@]}" -gt 1 ]; then
        red "两份登记都匹配：$rel（$(printf '%s ' "${matched[@]}")）——一份表只该有一个身份"
        continue
    fi
    pat="${matched[0]}"
    P_HIT["$pat"]=$((${P_HIT[$pat]} + 1))
    cls="${P_CLASS[$pat]}"
    detail="${P_DETAIL[$pat]}"
    # 链要验的字符串就是这张表被认出来的那把键：通配行验静态前缀（读者按目录整读时
    # 正文里只有目录，没有这一份的名字——要求逐份点名等于逼它把目录抄一遍），整行表验
    # tsv 的文件名／yaml 的路径——读者就是那样写它的。
    case "$pat" in
        *'*'*) needle="${P_PREFIX[$pat]}" ;;
        *) case "$rel" in *.tsv|*.pin|*.csv) needle="$(basename "$rel")" ;; *) needle="$rel" ;; esac ;;
    esac
    case "$cls" in
        generated|subject)
            what=生产者
            [ "$cls" = subject ] && what=判词脚本
            if [ -z "$detail" ] || [ ! -f "$ROOT/$detail" ]; then
                red "$cls 没点名${what}或它不在：$rel -> ${detail:-<空>}"
            elif ! grep -qF -- "$needle" "$ROOT/$detail"; then
                red "$cls 的 ${detail} 正文里没有 ${needle}：$rel"
            elif [ "$cls" = subject ] && [ -z "${VERDICT[$detail]:-}" ]; then
                red "subject 点名的 ${detail} 不在判词面里：$rel——验它的必须是一份能变红的脚本"
            elif [ "$cls" = generated ]; then
                generated_n=$((generated_n + 1))
            else
                subject_n=$((subject_n + 1))
            fi ;;
        debt)
            exp="${P_EXP[$pat]:-}"
            case "$exp" in
                [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) ;;
                *) red "debt 没有到期日或格式不对：$rel（${exp:-<空>}）——欠账要写到期的，没过期日就是豁免"; continue ;;
            esac
            if [ "$exp" \< "$(date +%F)" ]; then
                red "欠账到期：$rel 的到期日是 $exp，已经过了——补生产者，或者把这行删掉"
            else
                debt_n=$((debt_n + 1))
                printf 'debt  %s（%s；到期 %s）\n' "$rel" "${detail:-<没写理由>}" "$exp"
            fi ;;
        *)
            red "未知的类：$rel 登记成 $cls（只有 generated / subject / debt）" ;;
    esac
done < "$tmp/data"

# stale 的行要按路径排一次序再印；计数不能放在管道里（那一段是子 shell，加的不是这个数）。
: > "$tmp/stale"
for pat in "${!P_CLASS[@]}"; do
    if [ "${P_HIT[$pat]}" -eq 0 ]; then
        printf 'stale %s（登记了，但没有被读的文件匹配它）\n' "$pat" >> "$tmp/stale"
    fi
done
LC_ALL=C sort "$tmp/stale"
stale_n="$(grep -c . "$tmp/stale" 2>/dev/null || true)"
[ -n "$stale_n" ] || stale_n=0

rel_pin="${PIN#"$ROOT/"}"; [ "$rel_pin" = "$PIN" ] && rel_pin="$PIN"
if [ "$PROJECTION" = 1 ]; then src="图投影 ${rel_pin}"; else src="手写登记表 ${rel_pin}（导入前是欠账）"; fi
printf 'judgment-inputs: %d 张数据文件，%d 张被判词脚本读（generated %d，subject %d，debt %d），%d 张没人读，stale %d；登记表：%s\n' \
    "$total" "$read_n" "$generated_n" "$subject_n" "$debt_n" "$unread_n" "$stale_n" "$src"
if [ "$fail" -gt 0 ]; then
    printf 'judgment-inputs: %d 条红——被门读的表必须有生产者或验它的门；欠账要带理由与到期，且只能减\n' "$fail" >&2
    exit 1
fi
exit 0
