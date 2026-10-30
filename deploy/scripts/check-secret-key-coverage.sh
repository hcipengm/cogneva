#!/usr/bin/env bash
# 清单要读的 Secret 键，必须有东西保证它会存在——树里与现网两面都判。
#
# 为什么需要它：谁需要某个键（清单里的 `secretKeyRef`）与谁生成这个键
# （`init-secrets.sh` 的键表）是两份各写一遍的名单，中间没有比较。新装一台机器时两边
# 一起写、一起跑，看不出问题；而在**已装好的**实例上新增一个必需键，生成侧不会被再跑
# 一次——安装流程里只有那一回。2026-10-30 就是这样断的：`6b99311b` 给业务进程换成
# 独立数据库角色，清单开始读 `app-password`，而现网 Secret 是 37 天前建的，没有这个键。
# 症状是 `CreateContainerConfigError`：安全网关一个可用 Pod 都不剩，部署器把这一版读成
# 「坏的」并回滚，而树里的契约测试全绿——它只看树，看不到现网。
#
# 所以这条读数分两面：
#   树里（总是跑）：渲染产物里每个非 optional 的 `secretKeyRef`，其键必须在
#     `init-secrets.sh --list-keys` 的声明里。两边读的是同一份声明，不各写一份。
#   现网（--live）：那些键必须在现网 Secret 里真的存在。缺一个就红，不等它把一个 Pod
#     卡在 CreateContainerConfigError 上。
#
# 为什么判 optional 与否：manifest 已经把「缺了会不会卡住」写成 `optional: true|false`，
# 再判一次就该读这个字段而不是自己猜。optional 的键缺了是功能降级，不是这件事。
#
# 认不出来的形状（单行 flow 写法、块里没有 name 或 key）直接判红：静默跳过就是这条
# 读数失效的方式——表里少一行，读起来和「没有引用」一模一样。
#
# 用法：bash deploy/scripts/check-secret-key-coverage.sh [--live] [--record] [<仓库根>]
# 退出码：0 绿；1 有缺口（逐条点名）；2 判不了（读不到声明或读不到现网）。
#
# `--record` 把这一拍追加成序列文件的一行（默认
# `${XDG_STATE_HOME:-$HOME/.local/state}/cogneva/secret-key-coverage.tsv`）：
#   时刻 <TAB> 结局 <TAB> 必需键数 <TAB> 缺失数 <TAB> 判了几个渲染面
# 判红与读不到也各写一行：只在绿的时候写，序列上的洞就分不开「没跑」与「跑了但红了」。
#
# 环境：
#   COGNEVA_NS                 命名空间（默认 cogneva）
#   COGNEVA_KUBECTL            读现网用的命令（默认 kubectl；宿主上是 `sudo -n k3s kubectl`）
#   COGNEVA_SECRET_KEY_SERIES  `--record` 的序列文件路径
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NS="${COGNEVA_NS:-cogneva}"
SERIES="${COGNEVA_SECRET_KEY_SERIES:-${XDG_STATE_HOME:-$HOME/.local/state}/cogneva/secret-key-coverage.tsv}"
live=0
record=0
for arg in "$@"; do
    case "$arg" in
    --live) live=1 ;;
    --record) record=1 ;;
    -*) printf 'SECRET-KEYS: 不认识的参数：%s（只有 --live / --record）\n' "$arg" >&2; exit 2 ;;
    *) ROOT="$(cd "$arg" && pwd)" ;;
    esac
done

record_line() { # <结局> <必需键数> <缺失数> <面数>
    [ "${record}" = 1 ] || return 0
    mkdir -p -- "$(dirname -- "${SERIES}")" 2>/dev/null \
        || { printf 'SECRET-KEYS: 建不出序列目录，这一拍没有留下读数：%s\n' "${SERIES}" >&2; return 0; }
    printf '%s\t%s\t%s\t%s\t%s\n' "$(date +%s)" "$@" >>"${SERIES}" 2>/dev/null \
        || printf 'SECRET-KEYS: 序列写不进去，这一拍没有留下读数：%s\n' "${SERIES}" >&2
}

kubectl_cmd=(kubectl)
if [ -n "${COGNEVA_KUBECTL:-}" ]; then
    read -ra kubectl_cmd <<<"${COGNEVA_KUBECTL}"
fi

init_secrets="${ROOT}/deploy/scripts/init-secrets.sh"
[ -f "${init_secrets}" ] || { printf 'SECRET-KEYS: 判不了：%s 不存在\n' "${init_secrets}" >&2; exit 2; }

# 生成侧的声明。由脚本自己回答，不在这里再抄一份键名——抄的那份会在下一次新增键时
# 悄悄落后，而落后的表现是「门绿着，现网缺键」。
declared="$(bash "${init_secrets}" --list-keys 2>/dev/null)" \
    || { printf 'SECRET-KEYS: 判不了：%s --list-keys 跑不通\n' "${init_secrets}" >&2; exit 2; }
[ -n "${declared}" ] || { printf 'SECRET-KEYS: 判不了：生成侧一条键都没声明\n' >&2; exit 2; }

# 渲染产物里引用到的键：每条一行「Secret<TAB>键<TAB>required|optional<TAB>文件」。
# 单行 flow 写法与块里缺 name/key 的，一律以 CONFUSED 开头，交给下面判红。
refs="$(
    shopt -s nullglob
    files=("${ROOT}"/deploy/rendered/*/*.yaml)
    [ "${#files[@]}" -gt 0 ] || exit 3
    for f in "${files[@]}"; do
        awk '
            function emit() {
                if (inblk) {
                    if (n == "" || k == "") printf "CONFUSED\t%s\t%s\t%s\n", FILENAME, n, k
                    else printf "%s\t%s\t%s\t%s\n", n, k, opt, FILENAME
                }
                inblk = 0
            }
            /^[[:space:]]*secretKeyRef:/ {
                emit()
                if ($0 ~ /\{/) { printf "CONFUSED\t%s\tflow\t\n", FILENAME; next }
                match($0, /^[[:space:]]*/); ind = RLENGTH
                inblk = 1; n = ""; k = ""; opt = "required"
                next
            }
            {
                if (!inblk) next
                if ($0 ~ /^[[:space:]]*$/) next
                match($0, /^[[:space:]]*/)
                if (RLENGTH <= ind) { emit(); next }
                line = $0
                if (line ~ /^[[:space:]]*name:/) {
                    sub(/^[[:space:]]*name:[[:space:]]*/, "", line)
                    gsub(/["\047]/, "", line); n = line; next
                }
                if (line ~ /^[[:space:]]*key:/) {
                    sub(/^[[:space:]]*key:[[:space:]]*/, "", line)
                    gsub(/["\047]/, "", line); k = line; next
                }
                if (line ~ /^[[:space:]]*optional:[[:space:]]*true/) { opt = "optional"; next }
            }
            END { emit() }
        ' "$f"
    done
)" || {
    printf 'SECRET-KEYS: 判不了：%s 下没有渲染产物，一条引用都读不到\n' "${ROOT}/deploy/rendered" >&2
    record_line unreadable - - 0
    exit 2
}

problems=""
required=0
faces=0
declare -A ref_secrets=()
declare -A required_by_secret=()

while IFS=$'\t' read -r secret key opt file; do
    [ -n "${secret}" ] || continue
    faces=$((faces + 1))
    if [ "${secret}" = CONFUSED ]; then
        problems+="${file}: 认不出这个 secretKeyRef 的形状（单行 flow 或缺少 name/key）：${key}"$'\n'
        continue
    fi
    ref_secrets["${secret}"]=1
    if [ "${opt}" = required ]; then
        required=$((required + 1))
        required_by_secret["${secret}"]+="${key}"$'\n'
        grep -qxF "$(printf '%s\t%s' "${secret}" "${key}")" <<<"${declared}" \
            || problems+="${file}: 要读 ${secret}/${key}，而 init-secrets.sh 不声明它"$'\n'
    fi
done <<<"${refs}"

if [ "${live}" = 1 ]; then
    for secret in "${!ref_secrets[@]}"; do
        keys="$("${kubectl_cmd[@]}" -n "${NS}" get secret "${secret}" \
            -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}' 2>/dev/null)" || {
            problems+="现网读不到 ${NS}/${secret}：它可能根本没被建出来"$'\n'
            continue
        }
        while IFS= read -r key; do
            [ -n "${key}" ] || continue
            grep -qxF "${key}" <<<"${keys}" \
                || problems+="现网 ${secret} 缺少必需键 ${key}：读它的 Pod 会卡在 CreateContainerConfigError"$'\n'
        done <<<"${required_by_secret[${secret}]:-}"
    done
fi

if [ -n "${problems}" ]; then
    printf 'SECRET-KEYS: 有缺口（%d 条必需的引用，判了 %d 条）\n%s' \
        "${required}" "${faces}" "${problems}" >&2
    record_line red "${required}" "$(grep -c . <<<"${problems}")" "${faces}"
    exit 1
fi

printf 'SECRET-KEYS: 绿（%d 条必需的引用，判了 %d 条%s）\n' \
    "${required}" "${faces}" "$([ "${live}" = 1 ] && printf '，现网 Secret 已核对' || printf '，只判了树里')"
record_line green "${required}" 0 "${faces}"
exit 0
