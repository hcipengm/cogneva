#!/usr/bin/env bash
# 量 postgres-exporter 的产出面：这个镜像实际发哪些 `pg_*` 序列、认哪些
# `--[no-]collector.x` 旗标，然后把两张名单写回
# `deploy/images/postgres-exporter.series.yaml`。
#
# 那份文件以前靠一段贴在它自己文件头里的配方手工跑：kubectl run 一个带
# `DATA_SOURCE_PASS=<口令>` 的 Pod、port-forward、curl、再跑一次 `--help`，然后把输出
# 一条条抄进 YAML。换 tag 时人照着抄一遍，抄错一格不会红——只会让「读数指向的序列必须
# 有生产者」那条门去担保一张过期的名单，而那正是它要防的事。
#
# 用法：
#   bash deploy/scripts/measure-postgres-exporter-surface.sh          # 量完写回
#   bash deploy/scripts/measure-postgres-exporter-surface.sh --check  # 只判不写
#
# 序列从**已经在跑的边车**上量：它与数据库同 Pod，口令已经在它的 env 里，本脚本因此
# 一个凭证都不需要碰。旗标从同一个镜像的一次性 Pod 的 `--help` 里取——`--help` 不连库，
# 也不带凭证。
#
# `--check` 判的是两张名单与镜像此刻发出的东西是否一致，不一致退出 1。要紧的是
# 「名单里有、镜像不发」那一格：过期的条目让门担保一个不存在的序列。反过来
# 「镜像发了、名单没有」不会让谁读错——它只是说明名单落后了。
#
# 离线复现（CI 没有集群）：`--metrics-file` / `--help-file` 喂两份抓好的输出，跳过测量，
# 只跑折叠与写回。这条路径就是 deploy/scripts/tests/ 里那条测试判的东西。
#
# 退出码：0 一致；1 名单过期（只在 --check 下）；2 用法或配置错；3 量不出来——
# 读不了集群、找不到边车、盯着的镜像与实际在跑的不是同一个。3 与 1 必须分开：
# 「量不到」写成「过期」，下一个人会去改一份其实没错的名单。
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 两张名单的落点与那个钉住镜像的 values 文件。都从脚本位置算出来，不写死绝对路径。
INVENTORY="$REPO_ROOT/deploy/images/postgres-exporter.series.yaml"
VALUES="$REPO_ROOT/deploy/helm/cogneva/values.yaml"

# 探针用的本地端口。可覆盖：同一台机器上两条测量并排跑时，第二个得换一个。
PROBE_PORT="${COGNEVA_EXPORTER_PROBE_PORT:-19190}"

# 边车在这个 namespace 里的选择器。chart 给 postgres Pod 打的就是这两个标签，换
# release 名不会变；真变了用 --selector 覆盖，不去猜第二个默认。
SELECTOR="app.kubernetes.io/name=postgres,app.kubernetes.io/component=database"
EXPORTER_CONTAINER="postgres-exporter"

usage() {
    awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "${BASH_SOURCE[0]}"
}

die() { # die <exit-code> <message…>
    local rc="$1"
    shift
    printf '%s\n' "$*" >&2
    exit "$rc"
}

CHECK=0
NAMESPACE="${COGNEVA_NAMESPACE:-cogneva}"
METRICS_FILE=""
HELP_FILE=""
IMAGE=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --check) CHECK=1 ;;
        --file)
            [ "$#" -ge 2 ] || die 2 "--file 需要一个路径"
            INVENTORY="$2"
            shift
            ;;
        --namespace)
            [ "$#" -ge 2 ] || die 2 "--namespace 需要一个名字"
            NAMESPACE="$2"
            shift
            ;;
        --selector)
            [ "$#" -ge 2 ] || die 2 "--selector 需要一个标签选择器"
            SELECTOR="$2"
            shift
            ;;
        --image)
            [ "$#" -ge 2 ] || die 2 "--image 需要一个镜像引用"
            IMAGE="$2"
            shift
            ;;
        --metrics-file)
            [ "$#" -ge 2 ] || die 2 "--metrics-file 需要一个路径"
            METRICS_FILE="$2"
            shift
            ;;
        --help-file)
            [ "$#" -ge 2 ] || die 2 "--help-file 需要一个路径"
            HELP_FILE="$2"
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            die 2 "不认识的参数：$1（--help 看用法）"
            ;;
    esac
    shift
done

[ -f "$INVENTORY" ] || die 2 "量不回一份不存在的名单：$INVENTORY"
grep -q '^image:' "$INVENTORY" ||
    die 2 "$INVENTORY 里没有顶格的 image: 行——分不出哪一段是表头，改写会把表头吃掉"

# values.yaml 里钉住的镜像。改成从文件里读而不是在这里再写一份：写两份的那一天，
# 量的是其中一份、写回的是另一份，而两边都是合法镜像名。
pinned_image() {
    awk '/^[[:space:]]*exporterImage:/{
             sub(/^[[:space:]]*exporterImage:[[:space:]]*/, "")
             sub(/[[:space:]]+$/, "")
             print
             exit
         }' "$VALUES"
}

if [ -z "$IMAGE" ]; then
    IMAGE="$(pinned_image)"
    [ -n "$IMAGE" ] || die 2 "$VALUES 里找不到 exporterImage:，也就没有要量的镜像"
fi

# 决定用哪个 kubectl。k3s 上 `kubectl` 常在 PATH 里而权限在 sudo 后面，所以这条路
# 也认；都不在就退 3——「没有集群可以量」不是「名单是对的」。
resolve_kubectl() {
    if [ -n "${KUBECTL:-}" ]; then
        return 0
    fi
    if command -v kubectl >/dev/null 2>&1; then
        KUBECTL=kubectl
        return 0
    fi
    if command -v k3s >/dev/null 2>&1; then
        KUBECTL="sudo -n k3s kubectl"
        return 0
    fi
    return 1
}

work="$(mktemp -d)"
pf_pid=""
cleanup() {
    if [ -n "$pf_pid" ]; then
        kill "$pf_pid" >/dev/null 2>&1 || true
        wait "$pf_pid" >/dev/null 2>&1 || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT

# ---- 量序列 ----
measure_metrics() { # <out-file>
    local out="$1"

    resolve_kubectl || die 3 "这台机器上没有 kubectl 也没有 k3s——量不了，别把「量不了」当成「名单是对的」"

    local pod
    pod="$($KUBECTL -n "$NAMESPACE" get pods -l "$SELECTOR" \
        -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' 2>"$work/kubectl.err" |
        head -n1)"
    if [ -z "$pod" ]; then
        die 3 "namespace $NAMESPACE 里没有跑着的 postgres Pod（选择器 $SELECTOR）：$(tr '\n' ' ' <"$work/kubectl.err")"
    fi

    # 量的必须是钉住的那个镜像。边车跑着上一个 tag 时量出来的名单照样完整、照样能写回，
    # 而它担保的是一个这一版根本没发过的序列——静默，且没有任何读数会说话。
    local live_image
    live_image="$($KUBECTL -n "$NAMESPACE" get pod "$pod" \
        -o jsonpath="{.spec.containers[?(@.name==\"$EXPORTER_CONTAINER\")].image}" 2>"$work/kubectl.err")"
    [ -n "$live_image" ] ||
        die 3 "pod/$pod 里没有名为 $EXPORTER_CONTAINER 的容器：$(tr '\n' ' ' <"$work/kubectl.err")"
    [ "$live_image" = "$IMAGE" ] ||
        die 3 "pod/$pod 跑的边车是 $live_image，而要量的是 $IMAGE——边车还没滚到新 tag。\
把新 tag 部署上去再量，或者用 --image 明确说要量正在跑的那一个"

    local container_port
    container_port="$($KUBECTL -n "$NAMESPACE" get pod "$pod" \
        -o jsonpath="{.spec.containers[?(@.name==\"$EXPORTER_CONTAINER\")].ports[0].containerPort}" 2>"$work/kubectl.err")"
    [ -n "$container_port" ] ||
        die 3 "pod/$pod 的 $EXPORTER_CONTAINER 没有声明端口，没法 port-forward"

    $KUBECTL -n "$NAMESPACE" port-forward "pod/$pod" "$PROBE_PORT:$container_port" \
        >"$work/port-forward.log" 2>&1 &
    pf_pid=$!

    # 转发建起来之前 curl 会拒连。等，但要有尽头的等：卡在这里读起来会像集群挂了。
    local i=0
    until curl -sf --max-time 5 "http://127.0.0.1:$PROBE_PORT/metrics" -o "$out" 2>/dev/null; do
        i=$((i + 1))
        if [ "$i" -ge 30 ]; then
            die 3 "port-forward 到 pod/$pod 的 $container_port 端口一直没通：$(tr '\n' ' ' <"$work/port-forward.log")"
        fi
        sleep 1
    done
}

# ---- 量旗标 ----
measure_help() { # <out-file>
    local out="$1"
    local name="cogneva-exporter-help-$RANDOM"

    # 与量序列那一半各自解析一次：`--metrics-file` 配着不喂 `--help` 的混用是允许的，
    # 那种跑法下量序列那一半根本没被调用过，解析也就没发生过。
    resolve_kubectl || die 3 "这台机器上没有 kubectl 也没有 k3s——取不到 $IMAGE 的 --help"

    # `--help` 不连库，因此这个 Pod 不带任何 DATA_SOURCE_*：要口令的那种做法在这里
    # 一次都不需要。名字带随机后缀是因为同名 Pod 正在删除时重建会直接失败。
    $KUBECTL -n "$NAMESPACE" run "$name" --rm -i --restart=Never \
        --image="$IMAGE" -- --help >"$out" 2>"$work/help.log" ||
        die 3 "取不到 $IMAGE 的 --help：$(tr '\n' ' ' <"$work/help.log")"
    [ -s "$out" ] || die 3 "$IMAGE 的 --help 是空的——没有旗标名单可写"
}

if [ -n "$METRICS_FILE" ]; then
    [ -f "$METRICS_FILE" ] || die 2 "读不到 --metrics-file：$METRICS_FILE"
    cp "$METRICS_FILE" "$work/metrics.raw"
else
    measure_metrics "$work/metrics.raw"
fi

if [ -n "$HELP_FILE" ]; then
    [ -f "$HELP_FILE" ] || die 2 "读不到 --help-file：$HELP_FILE"
    cp "$HELP_FILE" "$work/help.raw"
else
    measure_help "$work/help.raw"
fi

# ---- 从原始输出里取两张名单 ----

# 抓取输出的每一行是 `<名字> <值>`，`^pg_` 开头到第一个非名字字符为止就是序列名。
series_names() {
    grep -oE '^pg_[a-zA-Z0-9_]+' "$1" | LC_ALL=C sort -u
}

# `--help` 把布尔旗标写成 `--[no-]collector.x`；命令行上要的是 `--collector.x`。
# 只认这一种写法：它正是当初量出下面那张名单的那条正则，换成更宽的模式会把帮助文本
# 里的举例一起收进来。
collector_names() {
    grep -oE '\-\-\[no-\]collector\.[a-z_]+' "$1" | sed 's/^--\[no-\]/--/' | LC_ALL=C sort -u
}

# 带 `*` 的条目是**声明**，不是测量：那一族的成员跟着宿主的库内容走（`pg_settings_`
# 一条 GUC 一个名字，约 300 个），逐条列会让这份名单按宿主漂移。脚本不猜哪一族该折叠——
# 「条目多到这个数就折」这种阈值两头都不对：它会把 `pg_stat_*`（六十来条、名字却是固定的）
# 一起折进去，而读数的名字是固定的还是随行数变的，只有人知道。所以声明从这份文件里读，
# 新冒出来的族逐条列出来——看得见才有人判它该不该折。
declared_prefixes() {
    awk '/^series:/{f=1;next} /^[a-z_]+:/{f=0} f && /^  - .*\*$/{sub(/^  - /,""); sub(/\*$/,""); print}' "$INVENTORY"
}

# 把量到的名字按声明折叠，折叠后的名单整份排序：`pg_settings_*` 因此落在 `pg_settings_`
# 本该在的位置，而不是人手补在末尾的那一行。
collapse_families() { # stdin: 排好序的名字
    local prefixes
    prefixes="$(declared_prefixes)"
    awk -v declared="$prefixes" '
        BEGIN {
            n = split(declared, p, "\n")
            for (i = 1; i <= n; i++) if (p[i] != "") prefix[++np] = p[i]
        }
        {
            for (i = 1; i <= np; i++) {
                if (index($0, prefix[i]) == 1) { covered[prefix[i]] = 1; next }
            }
            print
        }
        END {
            for (i = 1; i <= np; i++) if (prefix[i] in covered) print prefix[i] "*"
        }' | LC_ALL=C sort -u
}

series_names "$work/metrics.raw" >"$work/series.flat"
[ -s "$work/series.flat" ] ||
    die 3 "抓回来的 /metrics 里一条 pg_ 序列都没有——边车大概率连不上库，这份输出量不出名单"
collapse_families <"$work/series.flat" >"$work/series.list"

collector_names "$work/help.raw" >"$work/flags.list"
[ -s "$work/flags.list" ] || die 3 "$IMAGE 的 --help 里没解析出任何 --[no-]collector.x 旗标"

# ---- 写回 ----

# 表头（顶格 image: 之前的一切）原样保留，它是人写的、讲为什么的；下面的三节是量的产物。
render() { # <out-file>
    local out="$1"
    awk '/^image:/{ exit } { print }' "$INVENTORY" >"$out"
    printf 'image: %s\n\ncollector_flags:\n' "$IMAGE" >>"$out"
    sed 's/^/  - /' "$work/flags.list" >>"$out"
    printf '\nseries:\n' >>"$out"
    sed 's/^/  - /' "$work/series.list" >>"$out"
}

# 两张名单现在的样子，用来只报「多了什么、少了什么」而不是整份 diff。
current_list() { # <section>
    awk -v want="$1" '
        /^[a-z_]+:/ { inlist = ($0 == want ":") ; next }
        inlist && /^  - / { sub(/^  - /, ""); print }
    ' "$INVENTORY" | LC_ALL=C sort
}

report_delta() { # <label> <old-list> <new-list>
    local label="$1" old="$2" new="$3" added removed
    added="$(LC_ALL=C comm -13 "$old" "$new" | tr '\n' ' ')"
    removed="$(LC_ALL=C comm -23 "$old" "$new" | tr '\n' ' ')"
    [ -n "$added" ] && printf '  %s 新增：%s\n' "$label" "$added"
    [ -n "$removed" ] && printf '  %s 过期：%s\n' "$label" "$removed"
    return 0
}

current_list collector_flags >"$work/flags.before"
current_list series >"$work/series.before"

render "$work/rendered.yaml"

if cmp -s "$work/rendered.yaml" "$INVENTORY"; then
    printf 'ok  %s 与 %s 此刻发出的东西一致\n' "$INVENTORY" "$IMAGE"
    exit 0
fi

report_delta '旗标' "$work/flags.before" "$work/flags.list"
report_delta '序列' "$work/series.before" "$work/series.list"

if [ "$CHECK" = 1 ]; then
    printf 'FAIL  %s 与 %s 此刻发出的东西不一致（--check 只判不写）\n' "$INVENTORY" "$IMAGE" >&2
    diff -u "$INVENTORY" "$work/rendered.yaml" | sed -n '1,80p' >&2
    exit 1
fi

cp "$work/rendered.yaml" "$INVENTORY.tmp"
mv "$INVENTORY.tmp" "$INVENTORY"
printf 'wrote %s (%s)\n' "$INVENTORY" "$IMAGE"
