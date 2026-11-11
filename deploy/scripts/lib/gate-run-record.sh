#!/usr/bin/env bash
# 一跑门「花在哪」的记录落在哪、长什么样。
#
# 为什么要落盘：一跑两小时，跑完终端上那张表随窗口一起没了，而「刚才那一推慢在哪」
# 恰恰是回头才问的问题。眼下要看它，只能在宿主上翻 /tmp 里那份段日志——一份要人
# 手动去捞的读数，在真正要它的时候永远不在。
#
# 为什么落在这里：这道门跑在宿主上，屏幕上的面跑在集群里。宿主进不了网关的 ingest
# （NetworkPolicy 只放集群内的采集器），但构建槽目录本来就已只读挂进网关 Pod；写进
# 同一个目录下的 runs/，网关就能在它已有的那一面
# （`GET /api/v1/delivery/build-gate`，「编译与测」那一格）里读出来。于是这件事只剩
# 一个写入点、一个读取点，不必新开推送通道，也不必新开一面界面——宿主上再落一份
# 自成一体的 JSON 台账，就是把同一件事记第二遍。
#
# 形状就是这道门印成本表用的那三列（秒、结局、说明）：写的地方与读的地方共用一份
# 格式，成本表、页面、指标读到的才是同一件事。
#
# 三列之外还有一个身份：**阶段**。同一条交付链上不止一个钩子要知道自己花了多少——
# 提交前的钩子、整门、推之前的钩子各是一段。它们共用这一份实现，靠 `<阶段>` 区分
# 记录（`precommit` / `gate` / `prepush`），而不是各写一套格式、各起一个目录。
#
# 这三列是**可以公开**的：读者是另一个 uid（Pod 里 1001，宿主上是跑门的那个人），
# 所以文件必须对得上别人可读——权限因此钉死成目录 0755、文件 0644，不看跑门的 shell
# 把 umask 设成什么。由此也定下这条记录的边界：只许装这三列，命令输出、环境变量值、
# 凭证一个都不许进来。跑门的 shell 里有多少秘密，这一份里就有多少会变成世界可读。
#
# 寿命：一跑写完就从 `.running` 改名成 `.segments`。改名前那一份是「这一跑还没收尾」，
# 它就摆在那儿，读者自己看得见「在跑」与「没收尾」，不必由这里替它猜一个时限。每种
# 树、每个阶段只留最近一跑（收尾的、没收尾的各一份）：读者永远只读最新的那一份，
# 留更多只是让宿主上多出没人读的文件。

# 记录目录：构建槽目录下的 runs/。与槽位文件分开，位子那一面按 `slot-*.lock` 读，
# 两边互不干扰。
gate_run_dir() { # <构建槽目录>
    printf '%s/runs' "$1"
}

# 树的名字：路径本身，只把 `/` 换成 `_`。
#
# 用路径不用哈希：读者要的是「这一跑在哪棵树上」，一串十六进制说不出来；而路径就是
# 工作树的身份，两台机器上的同名路径本来也不可能落在同一个目录里。
gate_run_tag() { # <仓库根>
    printf '%s' "$1" | sed 's|^/||; s|/|_|g'
}

# 阶段是闭集：读的那一侧按这三个词切文件名，写出第四个词等于把自己记成另一棵树。
# 认不出就不记——一份记到错名字下的记录比没有记录更坏。
gate_run_stage_known() { # <阶段>
    case "$1" in
    precommit | gate | prepush) return 0 ;;
    *) return 1 ;;
    esac
}

# 每种树、每个阶段只留最近的一跑。收尾的与没收尾的各自一份：一份是「上一跑怎么样」，
# 一份是「有一跑没走完」——后者是证据，不能因为下一跑开始了就抹掉。
#
# 两处调用，答的是两个不同的问题：开跑时收掉的是**更早**那些没收尾的（被杀掉的跑
# 会一直留在盘上），收尾时收掉的是**被这一跑取代**的那份收尾记录。所以开跑的那一刻，
# 盘上同时有「上一跑的结果」与「这一跑到哪儿了」，读者两边都读得到。
gate_run_prune() { # <runs 目录> <树-阶段>
    local dir="$1" base="$2" keep_old
    local -a drop=()
    keep_old="$(ls -1t "${dir}/${base}-"*.segments 2>/dev/null | tail -n +2 || true)"
    if [ -n "${keep_old}" ]; then
        mapfile -t drop <<<"${keep_old}"
        if [ "${#drop[@]}" -gt 0 ]; then
            rm -f -- "${drop[@]}" 2>/dev/null || true
        fi
    fi
    keep_old="$(ls -1t "${dir}/${base}-"*.running 2>/dev/null | tail -n +2 || true)"
    if [ -n "${keep_old}" ]; then
        mapfile -t drop <<<"${keep_old}"
        if [ "${#drop[@]}" -gt 0 ]; then
            rm -f -- "${drop[@]}" 2>/dev/null || true
        fi
    fi
}

# 开一跑。打出来的路径给 record_segment 用；打不出来（目录建不出、名字是空的）就是
# 这一跑不留读数——**不能因此拦下这一跑**：读数是给人看的，判据归判据。
gate_run_begin() { # <构建槽目录> <仓库根> <阶段> <开跑时刻> <pid>
    local gate_dir="$1" root="$2" stage="$3" start="$4" pid="$5"
    local dir tag base file
    gate_run_stage_known "${stage}" || return 0
    dir="$(gate_run_dir "${gate_dir}")"
    mkdir -p "${dir}" 2>/dev/null || return 0
    [ -w "${dir}" ] || return 0
    tag="$(gate_run_tag "${root}")"
    [ -n "${tag}" ] || return 0
    # 读者是另一个 uid，权限不能跟着跑门的 shell 走：umask 077 下建出来的
    # 0700/0600 只有本用户读得到，Pod 那边看到的是「目录在、文件进不去」。
    chmod 0755 "${dir}" 2>/dev/null || true
    base="${tag}-${stage}"
    gate_run_prune "${dir}" "${base}"
    file="${dir}/${base}-${start}-${pid}.running"
    : >"${file}" 2>/dev/null || return 0
    chmod 0644 "${file}" 2>/dev/null || true
    printf '%s' "${file}"
}

# 记一格。写不进去就算了：这一跑照常判，只是屏上少一格读数。
gate_run_append() { # <记录文件> <秒> <结局> <说明>
    local file="$1"
    [ -n "${file}" ] || return 0
    printf '%s\t%s\t%s\n' "$2" "$3" "$4" >>"${file}" 2>/dev/null || true
}

# 收尾：改名成 `.segments`。改名就是这一跑的收尾记号，不另写一行终止记录——
# 多一种行，读的时候就要多认一种；而「文件还在不在 .running 上」本来就是它的结局。
gate_run_finish() { # <记录文件>
    local file="$1" dir base
    [ -n "${file}" ] || return 0
    [ -f "${file}" ] || return 0
    dir="$(dirname "${file}")"
    base="$(basename "${file}" | sed 's/-[0-9][0-9]*-[0-9][0-9]*\.running$//')"
    mv -f "${file}" "${file%.running}.segments" 2>/dev/null || return 0
    gate_run_prune "${dir}" "${base}"
}

# 开一跑并在退出时收尾。调用方只用这一个函数，就不会漏掉收尾——一段被 `exit 1`
# 拦下的提交/推送正是最该留下一行读数的那一种。
gate_run_wrap() { # <构建槽目录> <仓库根> <阶段> — 打出文件路径
    local file
    file="$(gate_run_begin "$1" "$2" "$3" "$(date +%s)" "$$")"
    [ -n "${file}" ] || return 0
    # shellcheck disable=SC2064  # 捕获此刻的 ${file}：退出时它不会再变
    trap "gate_run_finish '${file}'" EXIT
    printf '%s' "${file}"
}
