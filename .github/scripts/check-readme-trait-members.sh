#!/usr/bin/env bash
# README 里标成 Trait「定义」的那一块，列出的成员必须就是该 Trait 的全部成员。
#
# 为什么这值得一条门：README 的这两块是**接口的对外说明**，读者拿它当"这个 Trait 能做什么"
# 的唯一来源。`crates/cog-core/src/contract/stream.rs` 的 `MessageBackend` 实有 11 个方法，
# README 只列了 5 个——缺的 `payload_limit`（编码期字节上限，总线帧封顶那条链依赖它）与
# `claim_pending`/`pending_stats`（至少一次投递的认领与积压）恰好是读者最先要知道的两个界。
# 少列不比列错好：它让"这个 Trait 没有那个能力"成为默认读法，而字面上一句话都没错，
# 所以没人会去改它。`delay_publish` 更直接——它在紧跟的一段正文里被承诺（"delayed
# delivery"），却不在成员表里，而表头写着「定义」。
#
# 标签决定后果，所以门要**分档**：`definition`/`定义` 是一份定义，差集必须为空；
# `example`/`示例` 是一份举例，允许不全，只报数不当红。把 example 也判红的门会在
# 下一个作者精简示例时挡住他，那时他会去删门，而不是去改文档。
#
# 与 check-readme-paths.sh 的分工：那条判的是反引号里像路径的串，它自己的注释里就写明
# `publish / subscribe` 这种斜杠不是路径分隔符——也就是说，正是这一行被它排除在外。
# 两条门各管一种形状，不是同一条的两份实现。
#
# 用法：bash check-readme-trait-members.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"

fail=0

# 从 Rust 源里取出 `<trait>` 的成员名。只认 `pub trait X ... {` 到顶格 `}` 之间、且不以
# `//` 开头的行——注释里写到一个方法名不算它是成员。
members_of() { # <源文件> <Trait>
    [ -f "$1" ] || return 0
    awk -v t="$2" '
        $0 ~ "^pub trait " t "[ :{]" { inside = 1; next }
        inside && /^}/ { inside = 0; exit }
        inside && $0 !~ /^[[:space:]]*\/\// {
            if (match($0, /(async )?fn [a-z_0-9]+/)) {
                s = substr($0, RSTART, RLENGTH)
                sub(/^(async )?fn /, "", s)
                print s
            }
        }
    ' "$1"
}

# 表头行之后——成员表是紧接着的那几行反引号标识符，遇到第一行不带这种记号的就停。
listed_members() { # <文件> <表头行号>
    awk -v start="$2" '
        NR > start {
            if ($0 !~ /`[A-Za-z_][A-Za-z_0-9]*`/) { exit }
            line = $0
            while (match(line, /`[A-Za-z_][A-Za-z_0-9]*`/)) {
                print substr(line, RSTART + 1, RLENGTH - 2)
                line = substr(line, RSTART + RLENGTH)
            }
        }
    ' "$1"
}

check_file() { # <README 文件名，相对 ROOT>
    local doc="$1" ln rest trait path want got missing extra
    while IFS=: read -r ln rest; do
        [ -n "${ln}" ] || continue
        trait="$(printf '%s' "${rest}" | grep -oE '`[^`]+`' | head -1 | tr -d '`')"
        path="$(printf '%s' "${rest}" | grep -oE '`[^`]+`' | tail -1 | tr -d '`')"
        case "${rest}" in
            *definition*|*定义*) label=definition ;;
            *example*|*示例*)    label=example ;;
            *) continue ;;
        esac
        if [ "${path}" = "${trait}" ] || [ ! -f "${ROOT}/${path}" ]; then
            echo "READMEGATE ${doc}:${ln} 标为 Trait ${label}，但括号里的源路径定不到：${path}"
            fail=1
            continue
        fi
        want="$(members_of "${ROOT}/${path}" "${trait}" | sort -u)"
        if [ -z "${want}" ]; then
            echo "READMEGATE ${doc}:${ln} 在 ${path} 里找不到 \`pub trait ${trait}\` —— 名字或路径已经漂了"
            fail=1
            continue
        fi
        got="$(listed_members "${ROOT}/${doc}" "${ln}" | sort -u)"
        missing="$(comm -23 <(printf '%s\n' "${want}") <(printf '%s\n' "${got}"))"
        # `grep -v` 在「一条都没滤掉」时退出 1；在 set -e 下那会直接终止脚本，
        # 而这里"没有多余的"正是好消息，不是错误。
        extra="$(comm -13 <(printf '%s\n' "${want}") <(printf '%s\n' "${got}") | grep -vxF "${trait}" || true)"
        if [ -n "${extra}" ]; then
            echo "READMEGATE ${doc}:${ln} \`${trait}\` 列了源里没有的名字（那是别的东西，不是成员）："
            printf '%s\n' "${extra}" | sed 's/^/    /'
            fail=1
        fi
        if [ -n "${missing}" ]; then
            if [ "${label}" = definition ]; then
                echo "READMEGATE ${doc}:${ln} \`${trait}\` 标为定义，却少了 $(printf '%s\n' "${missing}" | wc -l) 个成员："
                printf '%s\n' "${missing}" | sed 's/^/    /'
                fail=1
            else
                echo "readme-trait-members: ${doc}:${ln} \`${trait}\` 是示例，少列 $(printf '%s\n' "${missing}" | wc -l) 个成员 —— 示例允许，只作读数"
            fi
        fi
    done < <(grep -nE '^`[A-Za-z_][A-Za-z_0-9]*` Trait (definition|定义|example|示例)' "${ROOT}/${doc}")
}

for doc in README.md README.zh-CN.md; do
    [ -f "${ROOT}/${doc}" ] || continue
    check_file "${doc}"
done

if [ "${fail}" -ne 0 ]; then
    echo "readme-trait-members: 标为「定义」的成员表必须与源一致（示例可以不齐）。" >&2
    exit 1
fi
echo "readme-trait-members: README 里标为 Trait 定义的成员表与源一致；示例块的缺项只作读数。"
