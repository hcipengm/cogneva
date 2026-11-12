#!/usr/bin/env bash
# 按格记账：一格判过、而它读的东西没变，下一次就不必再判一遍。
#
# 为什么要有这一份：门的戳是整份的。`patch_key` 记的是「这批改动」，改一个测试文件，
# 全部 199 格一起作废重跑。2026-11-12 实测整门 8107 秒（前一次 13571 秒），其中脚本测试
# 段约 2500 秒、Rust 几条约 5300 秒——而绝大多数格子与这次改动毫无关系。代价因此不是
# 「判得仔细」，是「每改一行付一遍两小时」。
#
# 这一份记的是**每一格自己的账**：这一格真的读过哪些路径（量出来的，不是手写的），
# 那些路径此刻的字节 + 判据自己的输入（门脚本、`lib/*.sh`、`ci.yml`、工具版本）算一个键。
# 键没变 ⇒ 沿用上次的判词；变了 ⇒ 重判并重新量一次作用域。
#
# 作用域是**量**出来的：把这一格跑一遍，`strace -f -e trace=openat` 记下它打开过仓里
# 哪些路径。手写一张作用域表是「两份名单、没有任何东西比较」——漏写一条的失败形态是
# **这一格从此永不重判**，而永不重判与一直绿在读数上同形，正是这道门要防的那种假绿。
# 量出来的那份不会漏：漏掉一次读取的唯一方式是这次没走到那条分支，而走到没走到由
# 作用域里的路径决定，那一变键就变、键变就重判、重判就重新量。
#
# 父目录的条目集也进作用域：`[ -f x ]` 只看不打开，新出现的 `x` 不会出现在 openat 里，
# 但它会进父目录的条目，于是同一目录下新增一个文件同样让这一格重判。
#
# 量不出来就不记账：没有 `strace` 的机器上，这一格照旧每次都跑。少赚一点，不会把没判过
# 的字节说成判过。
#
# 用法（在门里 source 之后）：
#   cell_run <说明> <外部命令…>
#
# 环境：
#   COGNEVA_CELL_STAMP_DIR   每格的账记在哪（门设成 <stamp_dir>/cells）
#   COGNEVA_CELL_STAMPS=off  关掉按格记账（调试用；关掉时每格都跑，读数照样对）
#
# 仓根与脚本目录自算，不从调用方继承：被 source 的脚本拿别人的变量，换一个调用方就错。

cell_here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || exit 1
cell_root="$(cd "${cell_here}/../.." && pwd)" || exit 1

# 账本目录在**调用时**读，不在 source 时读：调用方（门）是在 source 之后才算得出
# `stamp_dir` 的，source 时读一次就把空串记死了——每一格都以为自己在「不记账」模式，
# 跑得对，却一格也存不下。
cell_stamp_root() { printf '%s' "${COGNEVA_CELL_STAMP_DIR:-}"; }

cell_id() { # cell_id <说明> <命令…>
    local what="$1"
    shift
    {
        printf 'what=%s\n' "${what}"
        printf '%s\0' "$@"
    } | sha256sum | cut -d' ' -f1
}

cell_dir() { printf '%s/%s' "$(cell_stamp_root)" "$1"; }

declare -A _cell_path_hash=()

cell_path_hash() { # <path> —— 文件取内容，目录取条目集，不在取 absent
    local p="$1" h
    if [ -n "${_cell_path_hash["${p}"]+x}" ]; then
        printf '%s' "${_cell_path_hash["${p}"]}"
        return 0
    fi
    if [ -f "${p}" ]; then
        h="$(sha256sum -- "${p}" 2>/dev/null | cut -d' ' -f1)"
    elif [ -d "${p}" ]; then
        h="$(find "${p}" -mindepth 1 -maxdepth 1 -printf '%f\n' 2>/dev/null |
            LC_ALL=C sort | sha256sum | cut -d' ' -f1)"
    else
        h="absent"
    fi
    _cell_path_hash["${p}"]="${h}"
    printf '%s' "${h}"
}

# 这一格此刻的键：作用域里每条路径的字节，加判据自己的输入。
cell_key() { # cell_key <id>
    local id="$1" scope line rel
    scope="$(cell_dir "${id}")/scope"
    [ -f "${scope}" ] || return 1
    {
        while IFS= read -r line; do
            case "${line}" in
                'f '*)
                    rel="${line#f }"
                    printf 'f %s %s\n' "${rel}" "$(cell_path_hash "${cell_root}/${rel}")"
                    ;;
                'd '*)
                    rel="${line#d }"
                    printf 'd %s %s\n' "${rel}" "$(cell_path_hash "${cell_root}/${rel}")"
                    ;;
            esac
        done <"${scope}"
        gate_inputs
        # 门自己的其余字节：`lib/*.sh` 也是判据，改了它的判词就不作数。
        sha256sum "${cell_here}/lib/"*.sh 2>/dev/null
        printf 'id=%s\n' "${id}"
    } | LC_ALL=C sort | sha256sum | cut -d' ' -f1
}

# 从一份 `strace -f -e trace=openat` 的轨迹里量出作用域。
cell_scope_from_trace() { # <trace 文件> <写出的作用域文件>
    local trace="$1" out="$2"
    CELL_ROOT="${cell_root}" CELL_TRACE="${trace}" CELL_OUT="${out}" python3 -I - <<'PY'
import os, re

root = os.path.realpath(os.environ["CELL_ROOT"]).rstrip("/")
trace = os.environ["CELL_TRACE"]
out_path = os.environ["CELL_OUT"]

# `-y` 把 openat 的 dirfd 解成绝对路径：`openat(AT_FDCWD</cwd>, "rel", ...)`、
# `openat(3</dir>, "rel", ...)`。没有它时，一行 `cd 子目录 && cat f.txt` 里的
# `f.txt` 会被错拼到仓根上——要么落到一个不存在的路径、要么落到同名的另一份文件，
# 两种都让这一格**永不重判**，而「永不重判」与「一直绿」在读数上同形。
pat = re.compile(r'openat\((?:AT_FDCWD|(\d+))<([^>]*)>,\s*"([^"]+)"')
# 没有 fd 注记时的退路（老版本 strace）：第二个引号参数就是路径，按仓根解。
plain = re.compile(r'openat\((?:[^,]*,\s*)?"([^"]+)"')

files, dirs = set(), set()
try:
    handle = open(trace, errors="replace")
except OSError:
    handle = []

for line in handle:
    rel_dir = ""
    match = pat.search(line)
    if match:
        path = match.group(3)
        if not path.startswith("/"):
            path = os.path.join(match.group(2) or root, path)
    else:
        match = plain.search(line)
        if not match:
            continue
        path = match.group(1)
        if not path.startswith("/"):
            path = os.path.join(root, path)
    path = os.path.realpath(path)
    if path == root:
        dirs.add(".")
        continue
    if not path.startswith(root + "/"):
        continue
    rel = os.path.relpath(path, root)
    if os.path.isdir(path):
        dirs.add(rel)
    elif os.path.isfile(path):
        files.add(rel)

# 父目录的条目集进作用域：只看不打开的文件（`[ -f x ]`）不会出现在轨迹里。
for name in list(files):
    parent = os.path.dirname(name)
    dirs.add(parent if parent else ".")

with open(out_path, "w") as out:
    for name in sorted(files):
        out.write("f %s\n" % name)
    for name in sorted(dirs):
        out.write("d %s\n" % name)
PY
}

cell_record() { # cell_record <id> <key> <verdict> <seconds>
    local dir
    dir="$(cell_dir "$1")"
    mkdir -p "${dir}" 2>/dev/null || return 0
    {
        printf 'key=%s\n' "$2"
        printf 'verdict=%s\n' "$3"
        printf 'seconds=%s\n' "$4"
        printf 'when=%s\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    } >"${dir}/verdict" 2>/dev/null || true
}

cell_stored() { # cell_stored <id> <字段> —— 读上次记下的那一行，读不到返回 1
    local file
    file="$(cell_dir "$1")/verdict"
    [ -f "${file}" ] || return 1
    sed -n "s/^$2=//p" "${file}" | head -n 1
}

# 跑一格：键命中就沿用上次的判词，不命中就（在 strace 下）真跑一遍并记账。
# 退出码与直接跑这条命令一样；判为红由调用方计入，这里只交出状态。
cell_run() { # cell_run <说明> <命令…>
    local what="$1"
    shift
    local id key stored verdict t0 status trace=""

    id="$(cell_id "${what}" "$@")"
    if [ "${COGNEVA_CELL_STAMPS:-on}" = on ] && [ -n "$(cell_stamp_root)" ]; then
        if key="$(cell_key "${id}" 2>/dev/null)"; then
            stored="$(cell_stored "${id}" key)"
            if [ -n "${stored}" ] && [ "${stored}" = "${key}" ]; then
                verdict="$(cell_stored "${id}" verdict)"
                case "${verdict}" in
                    ok)
                        printf '== %s（沿用 %s 的判词：这一格读的东西没变）\n' \
                            "${what}" "$(cell_stored "${id}" when)"
                        return 0
                        ;;
                    RED)
                        printf '== %s（沿用 %s 的判词：这一格读的东西没变，上次是红的）\n' \
                            "${what}" "$(cell_stored "${id}" when)"
                        return 1
                        ;;
                esac
            fi
        fi
    fi

    t0=${SECONDS}
    printf '== %s\n' "${what}"
    local traced=0
    if command -v strace >/dev/null 2>&1; then
        # 作用域写在账本目录里，所以这个目录要在**跑之前**就有：写在跑之后（`cell_record`
        # 里那次 mkdir）时，python 的 `open(..., "w")` 会抛 FileNotFoundError，而那一句
        # 被 `2>/dev/null || true` 吞掉——于是每一格都「量不出作用域」，也就一格账都存不下，
        # 读数上却与「这台机器没有 strace」同形。
        mkdir -p "$(cell_dir "${id}")" 2>/dev/null || true
        trace="${TMPDIR:-/tmp}/cogneva-cell.$$.trace"
        rm -f "${trace}"
        strace -f -qq -y -e trace=openat -o "${trace}" "$@"
        status=$?
        traced=1
        if [ "${status}" -ne 75 ] && [ -n "$(cell_stamp_root)" ]; then
            cell_scope_from_trace "${trace}" "$(cell_dir "${id}")/scope" 2>/dev/null || true
            # 量到的东西为空 = 这一次没能量（strace 没起得来、或轨迹解析失败），**不是**
            # 「这一格什么都不读」。两者在账上必须分得开：空作用域也是一个合法的键，记下来
            # 就等于宣称「树怎么变它都不需要重判」——而它可能根本没跑起来。所以空作用域
            # 一律不记账，并且当场说一句，免得这一格退回每次重判时没人看得出是为什么。
            if [ ! -s "$(cell_dir "${id}")/scope" ]; then
                rm -f "$(cell_dir "${id}")/scope" 2>/dev/null || true
                printf '%s\n' "（量不出作用域：这一格不记账，下次照旧重判）"
            fi
        fi
        rm -f "${trace}"
    else
        "$@"
        status=$?
    fi

    # 轨迹只用来**量作用域**，判词以不带追踪的那一次为准。
    #
    # 追踪不是透明的：被追踪者每一次系统调用都要停下来交给追踪者，计时的判据因此改判
    # （2026-11-12 实测：`bootstrap-entry.test.sh` 里「开放网络实测 HTTPS 更快 → HTTPS
    # 优先」那一格在追踪下反了向），而**跑门的格子根本跑不起来**——里层 strace 的
    # PTRACE_TRACEME 被拒（一个进程只能有一个追踪者），命令一次都没执行，红在读数的
    # 外壳上而不是被它判的那份字节上（`before-push*.test.sh`、`cell-stamps.test.sh` 四格
    # 同时红，全是这个形状）。
    #
    # 所以：追踪下不是绿，就在不带追踪的条件下重跑一次，记的是这一次。代价只有红格付
    # 两遍；绿格一遍。
    if [ "${traced}" -eq 1 ] && [ "${status}" -ne 0 ] && [ "${status}" -ne 75 ]; then
        printf '%s\n' "（追踪下的判词不是绿：按不带追踪重跑一次，轨迹只用来量作用域）"
        status=0
        "$@" || status=$?
    fi

    # 75 是「没进构建槽、一行判据都没跑」——那是没判，不是判过，不记账。
    if [ "${status}" -ne 75 ] && [ -n "$(cell_stamp_root)" ] && [ -f "$(cell_dir "${id}")/scope" ]; then
        local now_key
        now_key="$(cell_key "${id}" 2>/dev/null || true)"
        if [ -n "${now_key}" ]; then
            if [ "${status}" -eq 0 ]; then
                cell_record "${id}" "${now_key}" ok "$((SECONDS - t0))"
            else
                cell_record "${id}" "${now_key}" RED "$((SECONDS - t0))"
            fi
        fi
    fi
    return "${status}"
}
