#!/usr/bin/env bash
# 推之前跑一遍 CI 里那些**只靠 shell 就能跑**的作业。
#
# 为什么要有这个调用点：2026-10-18 的一串提交里，连着十几个 commit 推上去之后红在
# 与代码无关的判据上——README 里那张「标为定义」的 Trait 成员表少了一个新方法、
# 一个没人读的变量触发 shellcheck、以及一个跑真树的脚本测试。每一条在本机都是秒级，
# 而发现它们要等 30~40 分钟的 CI；在这段时间里部署器按设计停摆（CI 红它就不滚），
# 于是七八个已经落地的修复全都卡在「没生效」。
#
# 缺的不是判据，是**推之前的那个调用点**。所以这里不新增任何判据，只是把 CI 里已经
# 有的那些原样跑一遍。
#
# 但「秒级」只对当初那三条成立，不是对整条脚本：2026-10-29 实测整条跑完逾 13 分钟，
# 其中 `deploy/scripts/tests/cogneva-json-alert-rules.test.sh` 单跑 488 秒占掉大半
# （54 格，每格起一次 promtool，脚本里没有一处 `sleep`）。把它当便宜的外层逻辑排预算
# 会直接排错——「跑一次推之前自检要多久」的答案是分钟级，不是秒级。
#
# 分钟是几，要看这一跑跟谁挤同一台机器。2026-10-30 在干净检出上跑（detached、无脏文件），
# 宿主上另有两个会话同时在跑同一批 promtool 格：40 分钟到点时它还在
# `test-acceptance-census-alert-rules.sh` 里面，被外层 timeout 杀掉，**没跑完**。所以
# 拿一个固定的超时去包它，包出来的是「超时」而不是「绿/红」——要么给足时间，要么别把它
# 当一次能等到结果的动作。也正因为被杀在中间，这一次没有给出任何判据读数：杀掉不等于通过。
#
# carrier: 人 —— 推之前手动跑。它没有自动调用点也不该有：跑的是 CI 自己的作业，
#   再挂到 CI 上就是同一件事跑两遍。`check-gate-carriers.py` 认这一行。
#
# 命令是从 `.github/workflows/ci.yml` 里**取**的，不是在这里抄一份：抄一份的门会在
# 有人加作业时静默变窄，而这条门唯一不能犯的错就是「它绿了，CI 却红了」。
#
# 不覆盖：Check / Code Coverage / Version Contract /
# Web Build / Bootstrap Cross-Platform —— 它们要编译或要 Node，本机跑一遍可能比 CI
# 还慢。末尾会把这份清单印出来：**这条门绿不等于 CI 绿**，它只保证那几类「改动碰了
# 别的面的读者」的判据不是推上去才发现的。
#
# Test 是个折中：整棵工作区在本机是十几分钟起，所以只判**这次动过的包加装配件**。
# 2026-11-06 20:26 那笔直接推到 main 的改动正是死在「改了包却没跑包自己的测试」上，
# 那一笔之后部署器停了九版。容量登记表那条秒级的纯文本扫描（`rust_capacity_constants_registry_test.rs`）
# 仍然单独在这里跑，理由是它可以不编译就判。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "${here}/../.." && pwd)"
cd "${root}" || exit 1

# 这一跑会开 `cargo test` 与 `cargo clippy`，也是这台机器上的一个构建方。宿主上一共有
# 四个：集群里的构建器、宿主自己的包装器、别的 worktree 的全量门、这一跑。前两个进构建
# 槽，后两个以前不进——于是「同时最多几条构建」这条界对它要保护的两个家伙失效，带内构建
# 反被宿主上的门饿着。所以每一格编译命令都从槽里拿：拿不到就让这一格**说它没判**，
# 不假装判过。
# 缺了它这一跑判不了「有没有槽」，而那一步的失败会长成 `command not found`：读的人
# 只会看见某一步红了，看不见红在「这份脚本没有随身带它的依赖」。夹具也一样，所以那句
# 话要在这里说清楚，而不是留给第一条被包起来的命令。
if [ ! -r "${here}/lib/build-slot.sh" ]; then
    echo "RED  构建槽客户端不在 ${here}/lib/build-slot.sh：这一跑没法和宿主的构建槽对齐，不判" >&2
    echo "     只复制 check-before-push.sh 的树要连 deploy/scripts/lib/ 一起带（deploy/scripts/tests/fixtures/stage-gate.sh）" >&2
    exit 1
fi
# shellcheck source=lib/build-slot.sh
. "${here}/lib/build-slot.sh"
# 等槽的秒数记在这里，由 lib/build-slot.sh 每次退出槽时追加一行。不这么做，表上的一格
# 就是「这一格花了多少秒」，而其中可能有一半是排队——两件事要相反的应对（一格真慢要
# 拆判据，排队久要加槽或让路），混在一行里读的人只会去优化编译。
# 写不进去只是少了这一面拆分：这一跑照常判，格子按整段记，不拿一个没量到的数去减。
slot_report="$(mktemp "${TMPDIR:-/tmp}/cogneva-gate-slot-waits.XXXXXX")" 2>/dev/null || slot_report=""
[ -n "${slot_report}" ] && export COGNEVA_BUILD_SLOT_REPORT="${slot_report}"
# 这一跑每一格的秒数与结局也写进构建槽目录下的 runs/：网关在它已有的「编译与测」那一面
# 读，页面因此不必另开一条通道，也不必在宿主上再落一份自成一体的台账。缺了它只是屏上
# 少这一面读数，判据不受影响。
# shellcheck source=lib/gate-run-record.sh
. "${here}/lib/gate-run-record.sh"

# ---- 验过没有：按字节记的戳 ----
#
# 推之前那次判定是分钟级到小时级的（整棵 `cargo test` 加全部脚本测试），把它挂在一次
# 推送前面，推的人只能干等；等的时候远端还会往前走，一被拒这一段就白等了。所以判定
# 与推送拆成两步：整条跑绿时按**工作树的字节**记一笔，推送那一腿只问「这份字节验过没有」
# （`--status`），一问是一次 sha256。
#
# 判词必须与「现在替我验一遍」分得开，否则读的人会把「没验过」当成「验过了」。
#
# 键是「这份字节」指的是**要推上去的那一版**：HEAD 加这次推的范围的底（`origin/main`，
# 它决定骨架那一格的触发判据）。工作树上的未提交改动**不进键**，因为它们不随这一推走；
# 反过来，判据读的是工作树，所以只有干净的工作树上「判了工作树」才等于「判了 HEAD」——
# 因此**脏的树上跑完不记账**，并在结尾说明为什么。共享工作树上别的会话在飞的那半成品
# 因此不会让一笔已经验过的提交反复失效，也不会被当成验过。
stamp_dir="$(git rev-parse --git-common-dir 2>/dev/null || echo .git)/check-before-push"
case "${stamp_dir}" in /*) ;; *) stamp_dir="${root}/${stamp_dir}" ;; esac

tree_key() {
    {
        git rev-parse HEAD 2>/dev/null
        git rev-parse --verify --quiet 'origin/main^{commit}' 2>/dev/null
    } | sha256sum | cut -d' ' -f1
}

# ---- 第二把键：这棵工作树此刻的字节 ----
#
# 上面那把键回答的是**推送**那一问：推上去的就是 HEAD，别人的在飞改动不跟着走。可同一些
# 钩子在**提交之前**问的是另一句话——「我现在要把这棵树提交掉，这份字节判过没有」。那时
# 改动还没进 HEAD，按 HEAD 记的账永远命不中；更糟的是反过来，跑过一次干净树之后，HEAD
# 与 origin/main 都不动，戳就一直有效，此后改多少都能提交，判过的字节与提交的字节从此
# 没关系。2026-11-08 实测：钩子按这把键问，分享这棵树的每个会话要么提交不了，要么拿一笔
# 旧账放行新字节。
#
# 所以另记一笔按**工作树字节**的账：临时索引上 `read-tree HEAD` + `add -A` + `write-tree`，
# 得到「现在就把这棵树提交掉」的那棵树的 oid。判据自己的输入也进指纹——这道门与
# `ci.yml` 的字节、工具的版本、测试库设没设：判据变了，上一次的结论就不再作数。
#
# 脏树也记这一笔：判的就是这份脏，提交之后字节不变，同一份指纹照样命中。
tool_sig() { # 名字 命令…
    local name="$1"
    shift
    if ! command -v "$1" >/dev/null 2>&1; then
        printf '%s=absent\n' "${name}"
        return 0
    fi
    printf '%s=%s\n' "${name}" "$("$@" 2>/dev/null | head -n 1)"
}

work_tree_oid() {
    local idx rc=0
    idx="$(mktemp "${TMPDIR:-/tmp}/cogneva-prepush-index.XXXXXX")" || return 1
    rm -f "${idx}"
    # 定位变量要摘掉：从钩子里被拉起来时，git 会把 `GIT_DIR`/`GIT_WORK_TREE` 指到**调用方
    # 那棵树**，而 `GIT_WORK_TREE` 一在手上，`add -A` 收的就是另一棵树的内容——指纹会指
    # 着一棵与这道门、与这次提交都无关的树。索引逐条显式给，不靠继承。
    #
    # 任何一步不成都不出 oid：`add -A` 失败时索引里还剩着 read-tree 的 HEAD 树，那会给
    # 出一棵与工作树无关的指纹，两次不同的改动就共用同一个戳。
    env -u GIT_DIR -u GIT_WORK_TREE -u GIT_COMMON_DIR -u GIT_OBJECT_DIRECTORY \
        -u GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_INDEX_FILE="${idx}" \
        git read-tree HEAD >/dev/null 2>&1 || rc=1
    env -u GIT_DIR -u GIT_WORK_TREE -u GIT_COMMON_DIR -u GIT_OBJECT_DIRECTORY \
        -u GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_INDEX_FILE="${idx}" \
        git add -A >/dev/null 2>&1 || rc=1
    if [ "${rc}" -eq 0 ]; then
        env -u GIT_DIR -u GIT_WORK_TREE -u GIT_COMMON_DIR -u GIT_OBJECT_DIRECTORY \
            -u GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_INDEX_FILE="${idx}" \
            git write-tree 2>/dev/null
    fi
    rm -f "${idx}"
    return 0
}

# 判据自己的输入：门脚本的字节、`ci.yml` 的字节、工具的版本、测试库设没设。
# 三把键都要带上它——判据变了，上一次的结论就不再作数。
gate_inputs() {
    [ -f "${here}/check-before-push.sh" ] && sha256sum "${here}/check-before-push.sh"
    [ -f "$root/.github/workflows/ci.yml" ] && sha256sum "$root/.github/workflows/ci.yml"
    tool_sig cargo cargo --version
    tool_sig rustc rustc --version
    tool_sig shellcheck shellcheck --version
    tool_sig node node --version
    printf 'test-db=%s\n' "${COGNEVA_TEST_DATABASE_URL:+set}"
}

content_key() {
    local tree
    tree="$(work_tree_oid)"
    [ -n "${tree}" ] || return 1
    {
        printf 'tree=%s\n' "${tree}"
        gate_inputs
    } | sha256sum | cut -d' ' -f1
}

# ---- 第三把键：这一批改动本身，而不是它踩在哪棵树上 ----
#
# 前两把键都绑在一棵具体的树上（HEAD 组成的树、或工作树字节组成的树）。一棵树只要
# 动了一格，哪怕动的那一格与这次改动毫无关系，键就换了，验过的结论就作废。2026-11-08
# 实测的形状：整门跑两小时，期间 cogneva 自己的进化流水线每 66～123 分钟往 origin/main
# 落一笔；要推上去就得先变基，一变基树里就多了别人的文件，两小时白跑。谁的整门都快不
# 过这个间隔，于是每个会话都在重复这两小时——不是红，是永远差一步。
#
# 所以第三把键记的是**改动本身**：从共同祖先到工作树的那一份 diff 的 patch-id，加判据
# 自己的输入。`git patch-id --stable` 按补丁正文算——提交 id、blob id、`@@` 里的行号都
# 不进哈希（2026-11-08 实测：只改这三样，id 一字不变），所以改动落在文件的哪一处不影响；
# **改动前后那几行在场文字进哈希**，同一份改动换个上下文就是另一个 id。
#
# 这两面都实测过（同一台机器，同一天）：
#   - 今天这两笔变基到 `845c45a5`（别人只动了另外两个文件），整批的 id 一分没变
#     （24a757a4…），这也是这条链能成立的原因；
#   - 基底在紧邻处改过一行、把我的改动挤进另一片上下文，id 就换了。落空是保守的方向：
#     宁可重判一次，不把「没判过的组合」放行。
# 解冲突改了解法的变基同样换 id，这正是要拦的那种。
#
# 它证明的是另一句话，必须说准：**这批改动在当时那棵树上判过**，不是**这批改动与今天这棵
# 树合起来判过**。合起来那一问由便宜的腿答——推之前那道钩子按推上去的树解引用、对改动的
# 脚本跑 shellcheck，CI 再整棵判一遍。本机整门的职责是「别把明显坏的东西推上去」，不是
# 替代 CI；把这两件事混成一件，门就会长到把自己卡死。
patch_key() { # [共同祖先] —— 工作树那一份（这一跑判的就是它）
    local base idx p
    base="${1:-}"
    if [ -z "${base}" ]; then
        base="$(git merge-base HEAD origin/main 2>/dev/null)" || return 1
    fi
    [ -n "${base}" ] || return 1
    idx="$(mktemp "${TMPDIR:-/tmp}/cogneva-prepush-index.XXXXXX")" || return 1
    rm -f "${idx}"
    # 与 work_tree_oid 同样的定位变量摘除：钩子拉起来的 git 会把 GIT_DIR/GIT_WORK_TREE
    # 指到调用方那棵树，索引逐条显式给，不靠继承。
    if ! env -u GIT_DIR -u GIT_WORK_TREE -u GIT_COMMON_DIR -u GIT_OBJECT_DIRECTORY \
             -u GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_INDEX_FILE="${idx}" \
             git read-tree "${base}" >/dev/null 2>&1 ||
       ! env -u GIT_DIR -u GIT_WORK_TREE -u GIT_COMMON_DIR -u GIT_OBJECT_DIRECTORY \
             -u GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_INDEX_FILE="${idx}" \
             git add -A >/dev/null 2>&1; then
        rm -f "${idx}"
        return 1
    fi
    p="$(env -u GIT_DIR -u GIT_WORK_TREE -u GIT_COMMON_DIR -u GIT_OBJECT_DIRECTORY \
             -u GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_INDEX_FILE="${idx}" \
             git diff --cached --no-color --no-ext-diff "${base}" 2>/dev/null |
         git patch-id --stable 2>/dev/null | cut -d' ' -f1)"
    rm -f "${idx}"
    [ -n "${p}" ] || return 1
    {
        printf 'patch=%s\n' "${p}"
        gate_inputs
    } | sha256sum | cut -d' ' -f1
}

# 同一批改动，问的人不一样，读的地方也不一样：
#   - 判的那一头（这一跑）读**工作树**：判据读的就是它，脏的那几处也在判过的字节里；
#   - 推的那一头（钩子）读**HEAD**：推上去的只有提交里的字节，共享树上别人此刻在飞的
#     改动不进这一推，也不该让这一推的账失效。
# 两边算出来的是同一个数，条件是「判过的那份字节已经被提交掉」。这不是巧合，是这条链的
# 全部：判完 → 提交（字节不变）→ 变基（diff 不变）→ 推。字节只要变过，两头就对不上，
# 而这正是这一问要的结局。
patch_key_committed() { # [共同祖先]
    local base p
    base="${1:-}"
    if [ -z "${base}" ]; then
        base="$(git merge-base HEAD origin/main 2>/dev/null)" || base=""
    fi
    [ -n "${base}" ] || return 1
    # 树对树，不碰索引与工作树：别人的在飞改动不在这份 diff 里。
    p="$(git diff --no-color --no-ext-diff "${base}" HEAD 2>/dev/null |
         git patch-id --stable 2>/dev/null | cut -d' ' -f1)"
    [ -n "${p}" ] || return 1
    {
        printf 'patch=%s\n' "${p}"
        gate_inputs
    } | sha256sum | cut -d' ' -f1
}

# 念一段段日志。单独一个入口是给测试用的：一整跑要两小时，而这张表本身的判据（每格都在、
# 秒数按长的在前、红的也在表上）不该跟着两小时一起付。
print_cost_table() { # [段日志]
    local log="${1:-${segment_log}}"
    [ -s "${log}" ] || return 0
    echo "== 这一跑花在哪（秒；长的在前）"
    sort -rn -t "$(printf '\t')" -k1,1 "${log}" | while IFS="$(printf '\t')" read -r secs outcome what; do
        printf '%7s  %-3s  %s\n' "${secs}" "${outcome}" "${what}"
    done
    printf '  合计 %s 秒\n' "$(awk -F'\t' '{s += $1} END {printf "%d", s}' "${log}")"
}

# ---- 工具来源：一条门只有一个 ----
#
# 这件事必须发生在子命令分派**之前**。三把键里的 `gate_inputs` 记着工具的版本，而它问的
# 是 `command -v`；整条判定跑起来之后才按 `~/.cargo/bin` 兜底找 cargo。两处不是一个来源，
# 同一份树字节能算出两个键：钩子那边（PATH 里没有 cargo）算出 `cargo=absent`，跑绿的那一
# 跑（兜底找到了 cargo）算出版本号，于是刚验过的这份字节在提交前仍然报「没验过」，整条
# 记账的链在接缝处断掉。2026-11-09 实测到的就是这个形状。
#
# 所以先解析一次，再把解析到的目录交给孩子：本机的 cargo 只在 `~/.cargo/bin`，PATH 里没
# 有；父进程用兜底找到了它，孩子（`check-format.sh` 的 `command -v cargo`、各条夹具自己的
# 自检）却按 PATH 找、找不到，于是同一台机器上「这一格判得了」而「这一格里那条用例说这
# 机器没有 cargo」。找到的那一份交给孩子，两处就只有一个来源。
gate_rustc="$(command -v rustc || true)"
[ -n "${gate_rustc}" ] || [ ! -x "${HOME}/.cargo/bin/rustc" ] || gate_rustc="${HOME}/.cargo/bin/rustc"
gate_cargo="$(command -v cargo || true)"
[ -n "${gate_cargo}" ] || [ ! -x "${HOME}/.cargo/bin/cargo" ] || gate_cargo="${HOME}/.cargo/bin/cargo"
for gate_tool in "${gate_rustc}" "${gate_cargo}"; do
    [ -n "${gate_tool}" ] || continue
    gate_tool_dir="$(dirname "${gate_tool}")"
    case ":${PATH}:" in
    *":${gate_tool_dir}:"*) ;;
    *) PATH="${gate_tool_dir}:${PATH}"; export PATH ;;
    esac
done

case "${1:-}" in
--cost-table)
    [ -n "${2:-}" ] || { echo "--cost-table 要一段段日志的路径" >&2; exit 2; }
    print_cost_table "$2"
    exit 0
    ;;
--key)
    tree_key
    exit 0
    ;;
--key-content)
    content_key
    exit $?
    ;;
--status)
    key="$(tree_key)"
    if [ -f "${stamp_dir}/${key}" ]; then
        echo "这份字节验过：$(head -n 1 "${stamp_dir}/${key}")"
        exit 0
    fi
    echo "这份字节没验过（$(git rev-parse --short HEAD 2>/dev/null)，键 ${key:0:12}）；"
    echo "先跑 \`bash deploy/scripts/check-before-push.sh\`，它跑绿了自己记账，之后这一推秒过。"
    exit 1
    ;;
--key-patch)
    patch_key_committed "${2:-}"
    exit $?
    ;;
--key-patch-tree)
    # 绿的那一跑记的是这一把：判的是工作树，键也得按工作树算。两份在「判过的那份字节已被
    # 提交掉」时相等，那正是这条链成立的条件。
    patch_key "${2:-}"
    exit $?
    ;;
--status-patch)
    # 推之前问的是这一句：**我要推上去的这几笔，判过而且绿吗**。与 `--status-content`
    # 问的不是同一件事：内容键问的是「这棵树」，这里问的是「这批改动」——树换了基底，
    # 改动还是这批，这一问照样命中。
    #
    # 基底由调用方给（`merge-base <远端那个提交> <本地那个提交>`），因为这正是这次推
    # 要补上的那一段。自己去猜 `origin/main` 会在钩子里猜错：钩子说的远端提交就是它
    # 传入的那一个，不是本地缓存的引用。
    pbase="${2:-}"
    if [ -z "${pbase}" ]; then
        pbase="$(git merge-base HEAD origin/main 2>/dev/null)" || pbase=""
    fi
    pkey="$(patch_key_committed "${pbase}")"
    if [ -z "${pkey}" ]; then
        echo "这批改动读不出指纹（没有共同祖先，或临时索引写不了），问不了（不是红）"
        exit 2
    fi
    if [ -f "${stamp_dir}/patch.${pkey}" ]; then
        # 判词必须说清验的是什么。这一问验的是**这几笔改动本身**，不是它们落到这次推的
        # 基底上的合并结果：干净的变基与「套得上但语义变了」的变基在 patch-id 上同形
        # （上游改了同一个函数的签名，这几笔照样干净套上、却编不过），本地分不出，只有
        # CI 整棵判得出。一句笼统的「已验证」会把这两件事说成一件。
        echo "这批改动验过（验的是改动本身，不是它与基底 ${pbase:0:12} 的合并结果）：$(head -n 1 "${stamp_dir}/patch.${pkey}")"
        exit 0
    fi
    echo "这批改动（改动键 ${pkey:0:12}，基底 ${pbase:0:12}）没验过：这批改动本身改过、或从没跑过验证；"
    echo "先跑 \`bash deploy/scripts/check-before-push.sh\`，它跑绿了自己记账，之后变基到新基底也不用重判。"
    exit 1
    ;;
--status-content)
    # 提交之前问的是这一句：**我此刻要提交的这份字节，判过而且绿吗**。
    ckey="$(content_key)" || { echo "这棵树读不出字节指纹，问不了（不是红）"; exit 2; }
    if [ -z "${ckey}" ]; then
        echo "这棵树读不出字节指纹（临时索引写不了，或还没有 HEAD），问不了（不是红）"
        exit 2
    fi
    if [ -f "${stamp_dir}/content.${ckey}" ]; then
        echo "这份字节验过：$(head -n 1 "${stamp_dir}/content.${ckey}")"
        exit 0
    fi
    echo "这份字节（内容键 ${ckey:0:12}）没验过：改过、或者从没跑过验证；"
    echo "先跑 \`bash deploy/scripts/check-before-push.sh\`，它跑绿了自己记账，之后这份字节的提交秒过。"
    exit 1
    ;;
esac

ci=".github/workflows/ci.yml"
[ -f "${ci}" ] || { echo "找不到 ${ci}（要从仓库根跑）"; exit 1; }

# 记账记的是**开跑那一刻**的提交：跑的过程中别人又提交了，那一笔没有被这一跑判过。
run_key="$(tree_key)"
# 判据读的是工作树；只有干净的树上「判了工作树」才等于「判了 HEAD」。脏树不记账，
# 但整条判定照常跑、照常出读数——它仍然回答了「现在这棵树绿不绿」。
run_dirty="$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')"

# 一份判词只能记在**它判过的那份字节**上，而这一跑要一两小时。结尾才取指纹的话，
# 期间树上多出来的任何一格都会被记成「验过」，而它一秒都没被判过——2026-11-09 实测：
# 17:24 起跑的那一跑在改完文件之后才收尾，内容键记下的是 18:xx 的字节。所以开跑时先
# 记一把，结尾对不上就不记账：这一跑仍然回答了「开跑那棵树绿不绿」，但它不是现在这份
# 字节的判词。取不到（读不出指纹）时不参与比对，交给结尾那一处自己说。
start_ckey="$(content_key 2>/dev/null || true)"
# 改动键的基底也记一把：远端在这一跑的期间往前走了，收尾那把键就会变，而这**不是**
# 这批改动被改过——判过的字节一个位没动，变的只是「从哪儿算起」这个标签。两种漂移
# 要分得开（2026-11-10 实测：同事在我这跑期间推进了 main，我这一跑判的字节没变，
# 改动键却按「改动变过」作废，两小时白跑）。
start_base="$(git merge-base HEAD origin/main 2>/dev/null || true)"
start_pkey="$(patch_key 2>/dev/null || true)"

failures=0
# 「没判」与「判红」是两件事，读数上也得是两个：一格没进构建槽就跑，判词写成红是把
# 机器的忙碌栽给这一笔改动；写成绿更糟，那是一份没人问过的字节被记成验过。所以它自己
# 一格，挡住推送，但说的是它自己的话。
unjudged=0
# 本机没验的面。这一步必须自己报出来，因为「这一格没验」在判词上原来是隐形的：一格
# `command -v <tool> || SKIP` 让同一份字节在不同机器上得出同一个「绿」，而绿的覆盖面
# 随机器装了哪几个可选工具而变，这个变化在最后那行判词里一个字节都不显示。2026-11-10
# 就是这一格：本机四轮全绿，CI 的 Entry Scripts Lint 一直红——那一步在没装 pwsh 的
# 机器上印 SKIP，而「印了 SKIP」与「验过」在判词上同形，本地没有任何读数说得出来。
#
# 记名字不记数字：读的人要的是「哪一面没验」，一个数说不出来，还得回去翻日志。
skipped_faces=()
skip_step() { # <面> <为什么没验>
    skipped_faces+=("$1")
    printf 'SKIP  %s：%s\n' "$1" "$2"
}

# CI 里有、本地回放不了的那一步：它没验的原因在写法（多行块 / 装依赖），不在本机装了
# 什么，所以印的形状与上面那一格不同（理由放在括号里）。但**记账必须是同一个名单**：
# 少记一格，判词里的面数就少一个，而名单自己不会告诉你名单不全。
skip_ci_step() { # <这一步的名字> <为什么回放不了>
    skipped_faces+=("$1")
    printf 'SKIP  %s（%s）\n' "$1" "$2"
}

# ---- 这一跑花在哪 ----
#
# 一道两小时的门，说不清时间花在哪一步，就只能整体喊「太慢」；而整体压缩等于猜——砍掉的
# 可能正是唯一会红的那一步，也可能砍掉的是零成本的一步，白担风险。所以每一格自己报秒数，
# 结尾按长的在前印一遍，红的绿的都印：红的那一跑也要知道钱花在哪。
#
# 「等了多久、等谁」这一面**这里没有**，要说准：这些格子直接调 cargo，构建槽的排队在
# cargo 里面，这道门看不见。别把「本格 900 秒」读成「编译了 900 秒」——它可能是 100 秒
# 编译加 800 秒排队。要分开这两者，得给排队那一层加上自己的读数，那是下一件事。
segment_log="$(mktemp "${TMPDIR:-/tmp}/cogneva-gate-segments.XXXXXX")" || segment_log=/dev/null
# 同一份格子另写一份到构建槽目录：那份活得过这一跑的终端，网关从它已有的挂载里读出来，
# 页面上的「编译与测」那一格就答得了「刚才那一推慢在哪」。写不进去只是少这一面读数，
# 这一跑照常判——所以这里不问、也不拦。
# 目录取构建槽客户端算好的那一个：同一个键、同一个默认值，两处各写一遍会在改过路径的
# 机器上分叉——记录落在一个没人读的目录里，屏上永远是「没有这一跑」。
run_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
run_file=""
case "${run_root}" in
# 夹具树是 `mktemp -d` 造的。把一次夹具跑记成「最近一跑」，页面上就会出现一棵早已删掉
# 的树、一段几秒的「编译与测」——测试的产物穿着交付读数的外衣。判据只有一处，所以这条
# 守卫写在这里，不散在各个夹具里。
"${TMPDIR:-/tmp}"/*) ;;
*) run_file="$(gate_run_begin "$(build_slot_dir_path)" "${run_root}" gate "$(date +%s)" "$$")" ;;
esac
finish_segments() {
    rm -f "${segment_log}"
    [ -z "${slot_report}" ] || rm -f "${slot_report}"
    gate_run_finish "${run_file}"
}
trap finish_segments EXIT

segments_written=0
record_segment() { # <秒> <结局> <说明>
    printf '%s\t%s\t%s\n' "$1" "$2" "$3" >>"${segment_log}" 2>/dev/null
    gate_run_append "${run_file}" "$1" "$2" "$3"
    segments_written=$((segments_written + 1))
}

# 一格只记一行，排队的秒数从这一格里拆出来单独记一行「等槽」。
#
# 拆的依据是构建槽客户端写的那份报告（`build_slot_report`），量不到就不拆：宁可把整段
# 记在这格上，也不拿一个没量到的数去减——减错的那一格会让人去拆一条并不慢的判据。
# 这一行同时是「这一跑有多少时间不是在判」的唯一读数：门慢可以是判据贵，也可以是机器
# 满，而这两个答案要求完全相反的动作。
record_cell() { # <起始秒> <起始等槽行数> <结局> <说明>
    local t0="$1" wait0="$2" outcome="$3" what="$4" row secs out txt
    local waited
    local -a rows=()
    waited="$(build_slot_reported_waits_since "${wait0}")"
    mapfile -t rows < <(cell_rows "$((SECONDS - t0))" "${waited}" "${outcome}" "${what}")
    for row in "${rows[@]}"; do
        IFS="$(printf '\t')" read -r secs out txt <<<"${row}"
        record_segment "${secs}" "${out}" "${txt}"
    done
}

run_suite_file() { # run_suite_file <说明> <模块名>
    # 合并成一个测试二进制之后，「跑哪一份文件」只能靠模块名当 filter 来选，而 libtest 在
    # filter 一条都命中不了时的结局是成功：`0 passed; 0 filtered out`，退出码 0。模块改名、
    # 或者某次重构把文件挪了层级，这一格就会**照常印绿而一条判据都没跑**——绿的形状与真跑
    # 过的绿逐字相同，这正是这份脚本别处（取不到 CI 步骤即红）治的那件事。所以判据不只读
    # 退出码，还要求它真的报出至少一条通过的用例。
    local what="$1"; shift
    local filter="$1"; shift
    local out status t0=${SECONDS} wait0
    wait0="$(build_slot_reported_lines)"
    out="$("$@" "${filter}" 2>&1)"
    status=$?
    printf '%s\n' "${out}" | tail -6
    if [ "${status}" -eq 75 ]; then
        printf 'UNJUDGED  %s：这一格没进构建槽，一行判据都没跑（不是红，也不是绿）。\n' "${what}" >&2
        unjudged=$((unjudged + 1))
        record_cell "${t0}" "${wait0}" UNJUDGED "${what}"
        return 0
    fi
    if [ "${status}" -ne 0 ]; then
        printf 'RED  %s\n' "${what}" >&2
        failures=$((failures + 1))
        record_cell "${t0}" "${wait0}" RED "${what}"
        return 0
    fi
    if ! printf '%s\n' "${out}" | grep -Eq 'test result: ok\. [1-9][0-9]* passed'; then
        printf 'RED  %s：filter“%s”一条用例都没命中，这条判据没跑\n' "${what}" "${filter}" >&2
        failures=$((failures + 1))
        record_cell "${t0}" "${wait0}" RED "${what}"
        return 0
    fi
    record_cell "${t0}" "${wait0}" ok "${what}"
    return 0
}

run_step() { # run_step <说明> <命令…>
    local what="$1"; shift
    local t0=${SECONDS} status wait0 written0
    wait0="$(build_slot_reported_lines)"
    written0=${segments_written}
    printf '== %s\n' "${what}"
    "$@"
    status=$?
    # 命令自己记过一格的，这一格不再记第二行：表上的两行会把同一段时间算两遍，
    # 而「合计」是拿来排期的。外层只负责在里层什么都没记时兜底。
    if [ "${segments_written}" -ne "${written0}" ]; then
        return 0
    fi
    if [ "${status}" -eq 0 ]; then
        record_cell "${t0}" "${wait0}" ok "${what}"
        return 0
    fi
    if [ "${status}" -eq 75 ]; then
        printf 'UNJUDGED  %s：这一格没进构建槽，一行判据都没跑（不是红，也不是绿）。\n' "${what}" >&2
        unjudged=$((unjudged + 1))
        record_cell "${t0}" "${wait0}" UNJUDGED "${what}"
        return 0
    fi
    printf 'RED  %s\n' "${what}" >&2
    failures=$((failures + 1))
    record_cell "${t0}" "${wait0}" RED "${what}"
    return 0
}

# CI 里这两个作业的每一步都是单行 `run: <解释器> <路径> [参数]`，按作业块取出来原样跑。
# 作业名以 ci.yml 里那两个块的键为准；取不到就红——静静地跑零步是另一种假绿。
#
# 认得的是**解释器**，不是字面的 `bash`：这里曾经只匹配 `run: bash <路径>`，于是同一
# 块里 `run: python3 deploy/scripts/check-shared-test-db-counts.py` 从来没在本机跑过，
# 推上去才发现它红。取法比 CI 窄的后果不是漏一条判据，是这条门**自称**跑过 CI 的那一段。
# 所以现在三种结局各有各的说法：认得就跑；多行脚本块与装依赖这两类本地跑不了的印出来
# （跳过的绿与跑过的绿在读数上一样，所以必须说）；剩下的写法一律红，逼下一次要么写成
# 解释器加路径，要么把新形式在这里说清楚——不再有「安静地不跑」这一种。
mapfile -t ci_steps < <(python3 - "${ci}" repo-conventions deploy-parity <<'PY'
import re, sys
# 分隔符不能用制表符：IFS 把连续的制表符当一个分隔展开，空的步骤名会越位，
# 名字与理由两列就会对到别的位置上。
SEP = "\x1f"
path, wanted = sys.argv[1], set(sys.argv[2:])
job = None
step_name = ""
for raw in open(path, encoding="utf-8"):
    line = raw.rstrip("\n")
    header = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
    if header:
        job = header.group(1)
        continue
    if job not in wanted:
        continue
    # 每一步的名字先记下，后面那一行 `run:` 才说得清跳过的到底是哪一步：一份
    # 不点名的跳过与另一份不点名的跳过在读数上一样，而这次要看见的恰恰是
    # 「跑整棵工作区测试的那一步被跳过了」。
    named = re.match(r"^\s+- name:\s*(.*?)\s*$", line)
    if named:
        step_name = named.group(1)
        continue
    # `- run: …` 是同一个 `run:` 写在了列表项那一行上：把前缀摘掉再往下判，否则
    # 一条列表项的复位会把这一步整个吞掉——跳过不印、跑也不印，读数上什么都不会发生。
    item = re.match(r"^\s+- (.*)$", line)
    if item:
        step_name = ""
        line = f"      {item.group(1)}"
    step = re.match(r"^\s+run: (bash|python3|sh) (\S.*)$", line)
    if step:
        print(SEP.join(["RUN", step.group(1), step.group(2).strip()]))
        step_name = ""
        continue
    if re.match(r"^\s+run: \|\s*$", line):
        print(SEP.join(["SKIP", step_name, "多行脚本块"]))
        step_name = ""
        continue
    if re.match(r"^\s+run: sudo ", line):
        print(SEP.join(["SKIP", step_name, "装依赖"]))
        step_name = ""
        continue
    other = re.match(r"^\s+run: (.*)$", line)
    if other:
        print(SEP.join(["OTHER", other.group(1).strip(), step_name]))
        step_name = ""
PY
)
if [ "${#ci_steps[@]}" -eq 0 ]; then
    echo "RED  没能从 ${ci} 取到任何一步：作业块改名了，这条门正静静地什么都不跑" >&2
    failures=$((failures + 1))
fi
ran=0; skipped=0; unrecognised=0
for step in "${ci_steps[@]}"; do
    [ -n "${step}" ] || continue
    IFS=$'\x1f' read -r kind a b <<<"${step}"
    case "${kind}" in
        RUN)
            ran=$((ran + 1))
            read -r -a argv <<<"${b}"
            run_step "${b}" "${a}" "${argv[@]}"
            ;;
        SKIP)
            skipped=$((skipped + 1))
            skip_ci_step "${a:-这一步没有名字}" "${b}"
            ;;
        OTHER)
            unrecognised=$((unrecognised + 1))
            printf 'RED   CI 里这一步本地认不出，推之前不会跑它：%s%s\n' \
                "${a}" "${b:+（${b}）}" >&2
            failures=$((failures + 1))
            ;;
    esac
done
printf '== 从 %s 取到 %d 步，跳过 %d 步（多行脚本块 / 装依赖），认不出 %d 步\n' \
    "${ci}" "${ran}" "${skipped}" "${unrecognised}"
# 一步都没跑与几步都过了印的是同一句「全绿」。取到的东西全是跳过的，说明这两个作业
# 的写法变了，而这条门正自称跑过 CI 的那一段——它没跑，只是没说。
if [ "${ran}" -eq 0 ]; then
    echo "RED  从 ${ci} 取到的 ${#ci_steps[@]} 步里一步都没跑起来（可跑的 0 步，跳过 ${skipped} 步）：" >&2
    echo "     这条门唯一不能犯的错就是「它绿了，CI 却红了」——现在是它绿了，它自己什么都没跑" >&2
    failures=$((failures + 1))
fi

# 脚本测试：CI 用同一个 glob 跑，这里照抄它的形状而不是列名字。
#
# 跑它们之前先把 **git 的定位变量**从环境里摘掉。这条门本身由 pre-push 钩子拉起，
# 钩子里的 git 会把 `GIT_DIR`/`GIT_WORK_TREE` 指向**调用方那棵树**；而下面这些测试
# 各自 `git init` 一个夹具仓库，定位变量在手上时 `git init --bare <夹具>` 落的是
# 那棵共享树——它被重新初始化成裸库，`core.bare` 翻成 true，之后每一个工作树里的
# git 命令都报 "must be run in a work tree"，钩子返回时提交写不进去。
# 2026-11-07 实测：这笔提交卡在钩子里两次，真凶是 `branch-convergence.test.sh`
# 的 `git init --bare`。夹具测试要的是**自己的**仓库，摘掉定位变量就是它们本来的
# 意思；CI 里没有这两个变量，摘掉等于不摘。
git_locators=(-u GIT_DIR -u GIT_WORK_TREE -u GIT_INDEX_FILE -u GIT_OBJECT_DIRECTORY
              -u GIT_ALTERNATE_OBJECT_DIRECTORIES -u GIT_COMMON_DIR -u GIT_NAMESPACE
              -u GIT_PREFIX)
mapfile -t script_tests < <(printf '%s\n' deploy/scripts/tests/*.test.sh .github/scripts/tests/*.test.sh)
for t in "${script_tests[@]}"; do
    run_step "${t}" env "${git_locators[@]}" bash "${t}"
done

# 容量登记表：`crates/cogneva/tests/rust_capacity_constants_registry_test.rs` 是**唯一**
# 会因为「新写了一个 Rust 常量」而红的门，而它只住在 CI 的 Test 里。一夜三次实测
# （`9e6aebd1`、`60b33d98`，每次约 20 分钟流水线停摆）：有人加了个常量、没补登记表、
# 推上去 CI 红、部署器按设计停住，补登记表的那一步却是在本机几秒钟的事。
#
# 走 `rustc` 而不是 `cargo test`：这条判据是纯文本扫描，不依赖任何 crate，而 `cargo`
# 要先把整个 `cogneva` 连同它的测试 harness 链出来——同一棵树上实测 55 秒（判定本身
# 只占 2.7 秒）。直接把这个**同一个文件**编成测试 harness 跑，1 秒编译 + 3 秒判定，
# 判据仍只有一份，两条路喂给同一条判据。
#
# 快路有前提：这个文件自己不带外来的依赖。它现在从 `tests/common` 取仓库根与产出面
# 清单，那条路要用 `cog_core`、`serde_json` 这些 crate，裸 `rustc` 没有 `--extern`
# 解析不了，于是快路编不出来——那不等于判据红，只是这条路走不通。所以编不出来时退回
# `cargo test` 跑**同一个文件**：判据仍只有一份，代价是多花一分钟，比让这一格从此
# 永远红着、把真正的红淹掉便宜。
capreg_src="crates/cogneva/tests/rust_capacity_constants_registry_test.rs"
capreg_bin="${TMPDIR:-/tmp}/cogneva-capreg.$$"
capreg_compiled=no
# 这一格也是一次编译，所以要进槽：它原先绕开槽直调 rustc 的那一秒里，与带内构建、
# 别的会话的构建一起抢同一台机器的 CPU 与 IO，而界是同一个数。不在槽里，读数归不到因，
# 界也管不住它——「只差一秒」不是理由，界的单位是并发构建数，不是秒。
if [ -n "${gate_rustc}" ] && \
        build_slot_run "gate: 容量登记表（rustc 直编）" \
            env CARGO_MANIFEST_DIR="${root}/crates/cogneva" \
            "${gate_rustc}" --test --edition 2021 -o "${capreg_bin}" "${capreg_src}"; then
    capreg_compiled=yes
fi
if [ "${capreg_compiled}" = yes ]; then
    run_step "容量登记表（${capreg_src}，经 rustc 直接编）" "${capreg_bin}"
    rm -f "${capreg_bin}"
elif [ -n "${gate_cargo}" ]; then
    run_step "容量登记表（${capreg_src}，经 cargo test：这份测试带 tests/common 的依赖，rustc 直接编找不到）" \
        run_suite_file "容量登记表（经 cargo test）" rust_capacity_constants_registry_test \
        build_slot_run "容量登记表" "${gate_cargo}" test -p cogneva --test suite
else
    # 与 pwsh 那一格同一条规矩：跑不了要说出来，不能安静跳过。
    skip_step "容量登记表" "本机既没有可用的 rustc 也没有 cargo（CI 的 Test 会跑这一步）"
fi

# 两份「名字」契约：`observability_alert_rule_contract_test.rs` 与
# `observability_scrape_census_contract_test.rs`。它们判的是同一笔账的两面——一个数字
# 没有来历、一条序列没有读者、写了个名字没人认领——与上面那张登记表是同一族，都只在
# CI 里才被问。2026-10-29 一天之内两面各红了一次：`cogneva_change_ledger_remeasure_pending`
# 发布出去了却没有规则读它；另一个标本在解析器测试里写成 `"cogneva_x{...`，被普查的
# 「双引号紧跟 cogneva_」读成「这棵树发布了这个名字」。两次都是 `Code Coverage` 红、
# 部署器按设计停在最后一个绿 rev 上、整条主线跟着停。
#
# 走 `cargo test` 而不是裸 `rustc`：两份测试都从 `tests/common` 取仓库根与词法分析，
# 裸 `rustc` 没有 `--extern` 解析不了。价格是两个数，别混成一个：判定 0.11 秒与 0.28 秒，
# 冷编译 7 分 55 秒（后一个是编译的价，不是这两条判据的价）。跑**整份文件**，不加 filter
# ——挑着跑一条是在挑判据，而这条门唯一不能犯的错就是「它绿了，CI 却红了」。
#
# 第二条为什么也在这一格里：它们编的是同一个 `cogneva` test target，第二个二进制多花的
# 只是链接与那 0.28 秒判定，接进来几乎不要钱；而漏掉它，今天上午那一次红就仍然要等
# 一整轮 CI 才被发现。
#
# `--no-fail-fast` 不是顺手加的：不带它，`cargo test` 在第一个二进制红就停，第二份判据
# 那一跑根本没发生——改完第一条再推，才发现第二条也红，同一轮 CI 的代价付两次。实测
# 2026-10-29：不带它只印出一行 `Running`，带上它两行都印、两个 verdict 都在。
#
# 这一格没把整条脚本推到二十分钟，靠的是**它排在容量登记表之后**：那张表的快路（裸
# `rustc`）今天编不出来、实测 1.2 秒即失败，于是走 `cargo test` 回退，而回退编的是
# 同一个 `cogneva` test target —— 轮到这一格时编译已经热了，多花的只有判定那零点几秒。
# 这个顺序是它便宜的**唯一**理由。哪天快路修好了、容量登记表真的只花几秒，这一格就会
# 退回冷编译的 8 分钟；那时要么把它挪进 cargo 回退那一支，要么给它加触发条件。
#
# 还有一处它必然要付出的代价，写在读它的人最容易读错的地方：**这条门判的是工作树，而推
# 上去的是提交**。共享工作树里别的会话飞到一半的改动也在这一份读数里，所以它红的时候先
# `git status` 看一眼红的是谁的文件——别人的半成品不进你的提交，这一格红了也说明不了你要
# 推的那一笔红了。反过来也别把它当噪声：那一格绿的时候它确实判过了你树上的东西。
cargo_bin="${gate_cargo}"

if [ -n "${cargo_bin}" ]; then
    # 合并成一个测试二进制之后，选「一份文件」靠的是模块名当 filter：过滤词是模块路径的
    # 前缀，整份文件的所有用例都在它下面，挑一条用例仍然要再写一层名字——判据还是整份文件
    # 的，只是它现在住在 `suite` 里。两步分开跑而不是一次给两个 filter：`run_step` 会记下
    # 失败继续走，两个 verdict 照样都印出来。
    run_suite_file "名字契约（告警规则契约）" observability_alert_rule_contract_test \
        build_slot_run "名字契约（告警规则契约）" "${cargo_bin}" test -p cogneva --test suite
    run_suite_file "名字契约（观测面普查）" observability_scrape_census_contract_test \
        build_slot_run "名字契约（观测面普查）" "${cargo_bin}" test -p cogneva --test suite
else
    skip_step "名字契约" "本机没有可用的 cargo（CI 的 Code Coverage 会跑这两步）"
fi

# 工作区的骨架，两面都由树自己推、没有手工表：依赖方向、仅测试 crate 的豁免、crate 有没有
# 人用（`crate_dependency_contract_test`，题目取自 `cargo metadata` 与各个包的 `src/main.rs`）；
# 基底 crate 的每一条依赖必须在自己的源码里真被写到、动了机器的模块必须被 base 之外的
# 两个以上 crate 用到（`core_boundary_contract_test`，题目取自 `cog-core` 的源码）。这两份只在
# `cargo test --workspace`（CI 的 Test）里被问，所以「加一个 crate」「加一条依赖」「base 里多一个
# 会开文件的模块」这三种最该被拦下的编辑，本机推之前是看不见的——它们编译得过、单测全绿。
#
# 这一格是**带触发条件**的：只有这次要推的提交或当前工作树里动过某个 `Cargo.toml`，
# 或动过 `crates/cog-core/src`，才跑。判据本身是整份文件的（filter 只写到模块名，挑到单条用例
# 就是在挑判据），
# 但它的题目只有 manifest 与 base 源码能出——两处都没动的时候它必然绿，而一次冷编译的代价
# 是分钟级，不该摊到每一笔上。base 源码那一半是新加的：依赖是否「被写到」与「模块是否动机器」
# 这两条判的是源码，只盯 manifest 会让一次新增的机制模块等一整轮 CI 才被看见。
#
# 触发判据读的是**工作树**（`git status`）加**要推的范围**（`origin/main...HEAD`）两份：
# 门判的是工作树，推上去的是提交，两者任一动了这两处都可能让这一格红在下一次 CI 上。
if [ -n "${cargo_bin}" ]; then
    if ! git rev-parse --verify --quiet 'origin/main^{commit}' >/dev/null; then
        skip_step "crate 骨架" "本机没有 origin/main，推的范围判不出来（CI 的 Test 会跑）"
    else
        skeleton_changed="$(
            { git diff --name-only 'origin/main...HEAD' -- '*Cargo.toml' 'crates/cog-core/src'
              git status --porcelain -- '*Cargo.toml' 'crates/cog-core/src' | cut -c4-
            } | sed '/^$/d'
        )"
        if [ -n "${skeleton_changed}" ]; then
            echo "== crate 骨架：这次动过 $(printf '%s\n' "${skeleton_changed}" | wc -l) 个 manifest / base 源文件，两份门整份跑"
            run_suite_file "crate 骨架（依赖方向 / 仅测试 crate / crate 有没有人用）" crate_dependency_contract_test \
                "${cargo_bin}" test -p cogneva --test suite
            run_suite_file "crate 骨架（base 依赖与机制模块）" core_boundary_contract_test \
                "${cargo_bin}" test -p cogneva --test suite
        else
            echo "== crate 骨架：这次没动 Cargo.toml，也没动 crates/cog-core/src，跳过（CI 的 Test 会整份跑）"
        fi
    fi
else
    skip_step "crate 骨架" "本机没有可用的 cargo（CI 的 Test 会跑这两步）"
fi

# Format 那一格在 CI 里是 `cargo fmt --all -- --check`：整棵树、十秒、一次编译都不用，
# 而它是 main 上的必过检查之一，本地此前一个调用点都没有。代价不对称得厉害——一笔没排过
# 格式的改动推上去，二十分钟后红的是空格，而 CI 红期间主线部署器读到 verdict=fail 就把
# 整班滚动扣住，部署停在那里等这一笔重推。
#
# 判的是**工作树**：这条门最常被问的一刻是「我正要提交」，那一笔的字节还在工作树里、没进
# HEAD。共享树里别人的在飞改动会让这一格替它红一次——那是多判，不是假绿（与 Test 同一取舍）。
if [ -n "${cargo_bin}" ] && "${cargo_bin}" fmt --version >/dev/null 2>&1; then
    run_step "cargo fmt --all -- --check（CI 的 Format 作业同一条）" bash "${here}/check-format.sh"
else
    skip_step "Format" "本机 cargo 没有 rustfmt 组件（CI 的 Format 会整份跑）"
fi

# 这一跑判的 `crates/*.rs`：要推的范围 ∪ 工作树 ∪ 未跟踪，三段并集。
#
# 只取第一段时这里有一个假绿的洞。`origin/main...HEAD` 列的是**提交**，而这一跑末尾盖的
# 戳记的是**盘上**的字节：一棵带着未提交 `crates/*.rs`、而这些改动一笔都没提交的树，两格
# 都印「这次要推的提交没动 crates/ 下的 .rs」跳过了，这份字节照样拿到「验过」。2026-11-09
# 实测：15 个这样的文件，0 个在任何一笔提交里。
#
# 未跟踪的也要算：新模块正是那种「还不在任何提交里」的改动，`git diff HEAD` 看不见它。
touched_crate_sources() {
    {
        git diff --name-only 'origin/main...HEAD' -- 'crates/*.rs' 2>/dev/null
        git diff --name-only HEAD -- 'crates/*.rs' 2>/dev/null
        git ls-files --others --exclude-standard -- 'crates/*.rs' 2>/dev/null
    } | sed '/^[[:space:]]*$/d' | sort -u
}

# 这些字节里还有没有没提交的（已改／未跟踪都算）。Clippy 那一格问它：那一格原先判的是
# HEAD 展开出来的临时树，而临时树里放不下脏树的改动——脏树上的绿会变成一份对
# 「没人要推的字节」的判词，而末尾的戳记的却是盘上这份。
crate_sources_uncommitted() {
    git status --porcelain -- 'crates/*.rs' 2>/dev/null | sed '/^[[:space:]]*$/d'
}

# 整跑读一次，Clippy 与 Test 两格共用这一份。两格各读一次会在中间树变了的时候印出
# 两个不一致的数——而收尾那把内容键本来就会让这一跑不记账，那时这两个数谁也不对。
uncommitted_crate_sources="$(crate_sources_uncommitted)"

# Clippy 那一格在 CI 里是整份 workspace 的 `--all-targets -- -D warnings`，红一格就判整轮
# 四十分钟为红。它的红只值一行：2026-10-30 一次 `let mut` 的 `unused_mut` 就这样烧掉了一轮，
# 而本机此前一条读数都看不见它——脚本末尾那句「未覆盖」里写着 Clippy。
#
# 本机不能整份跑：实测一次冷跑 214 秒。所以这一格的**题目**由 diff 推出来——这次要推的
# 提交动过哪些 `crates/<dir>` 下的 `.rs`，就把那个计件包名（读 `crates/<dir>/Cargo.toml`
# 的 `name`，不点名，`crates/bootstrap` 这种目录名与包名不一致的也不用另写一条）交给 cargo；
# **每一个题目里跑的是 CI 的同一条命令与同一组 flag**，不挑 lint、不挑 target。CI 仍然整份跑。
#
# 判的是**提交**不是工作树：CI 编译的是推上去的那棵树，而共享工作树里别人飞到一半的字节
# 会给这次推的判词投票——红的那个人往往不是推的人。所以 HEAD 展开到一个临时工作树里判，
# 目标目录指向主树的 `target`：registry 依赖的指纹不随路径变，实测 `-p cog-quota` 在展开里
# 1m27s，其中大部分是在等别人的构建锁，自己只花 22s CPU。
if [ -n "${cargo_bin}" ]; then
    if ! git rev-parse --verify --quiet 'origin/main^{commit}' >/dev/null; then
        skip_step "Clippy" "本机没有 origin/main，推的范围判不出来（CI 的 Clippy 会整份跑）"
    elif ! "${cargo_bin}" clippy --version >/dev/null 2>&1; then
        skip_step "Clippy" "这个工具链没装 clippy 组件（CI 的 Clippy 会整份跑）"
    else
        mapfile -t touched_crates < <(
            touched_crate_sources |
                awk -F/ '$1 == "crates" && NF >= 3 { print $2 }' | sort -u
        )
        clippy_args=()
        crate_count=0
        for crate_dir in ${touched_crates[@]+"${touched_crates[@]}"}; do
            package="$(sed -n 's/^name *= *"\([^"]*\)".*/\1/p' "crates/${crate_dir}/Cargo.toml" 2>/dev/null | head -1)"
            if [ -n "${package}" ]; then
                clippy_args+=(-p "${package}")
                crate_count=$((crate_count + 1))
            else
                # 有 `.rs` 却读不到包名：那个目录不是工作区成员。另一条门
                # （`crate_source_membership_contract_test`）判这件事，这里只说没判到。
                skip_step "Clippy" "crates/${crate_dir} 下这次动过源码但读不到包名，这一份没判（CI 的 Clippy 会整份跑）"
            fi
        done
        if [ "${crate_count}" -gt 0 ] && [ -n "${uncommitted_crate_sources}" ]; then
            # 树脏时就地判：临时树里放不下脏树的改动，而末尾盖的戳是盘上这份字节。
            # 判一份没人要推的字节、再把结论记到这份字节头上，等于用一个别的树的
            # 结论替这棵树背书。与 Test 那一格同一取舍：多判别人的在飞改动是不绿得
            # 过头，不是假绿。
            echo "== Clippy：这次动过 ${crate_count} 个 crate 的源码（其中 $(printf '%s\n' "${uncommitted_crate_sources}" | wc -l | tr -d ' ') 份还没提交），判的是工作树"
            judge_worktree_clippy() {
                build_slot_run "clippy ${clippy_args[*]}" \
                    "${cargo_bin}" clippy "${clippy_args[@]}" --all-targets -- -D warnings
            }
            run_step "cargo clippy ${clippy_args[*]} --all-targets -- -D warnings" judge_worktree_clippy
        elif [ "${crate_count}" -gt 0 ]; then
            repo_toplevel="$(git rev-parse --show-toplevel)"
            head_tree="$(mktemp -d "${TMPDIR:-/tmp}/cogneva-push-clippy.XXXXXX")"
            git worktree prune 2>/dev/null
            if git worktree add --detach --quiet "${head_tree}" HEAD 2>/dev/null; then
                echo "== Clippy：这次要推的提交动过 ${crate_count} 个 crate 的源码，判的是 HEAD 展开出来的那棵树"
                judge_head_tree_inner() {
                    ( cd "${head_tree}" &&
                        CARGO_TARGET_DIR="${repo_toplevel}/target" \
                            "${cargo_bin}" clippy "${clippy_args[@]}" --all-targets -- -D warnings )
                }
                judge_head_tree() {
                    build_slot_run "clippy ${clippy_args[*]}" judge_head_tree_inner
                }
                run_step "cargo clippy ${clippy_args[*]} --all-targets -- -D warnings" judge_head_tree
                git worktree remove --force "${head_tree}" >/dev/null 2>&1 || rm -rf "${head_tree}"
            else
                rm -rf "${head_tree}"
                skip_step "Clippy" "展开不出 HEAD 的临时工作树（${head_tree}），这一份没判（CI 的 Clippy 会整份跑）"
            fi
        else
            echo "== Clippy：这次要推的提交与工作树都没动 crates/ 下的 .rs，跳过（CI 的 Clippy 会整份跑）"
        fi
    fi
else
    skip_step "Clippy" "本机没有可用的 cargo 或 python3（CI 的 Clippy 会整份跑）"
fi

# Test：CI 的 Test 作业跑 `cargo test --workspace --no-fail-fast`。整棵在本机是十几
# 分钟起，而这一步要站在每一次推送前面，所以收窄到**这次要推的提交动过的包，加上
# 装配件 `cogneva`**。
#
# 装配件不是「多跑一点更保险」：读全树源码的普查门住在它的测试里，按改动包收窄
# 正好把它们排除在外，而它们恰恰最容易在落地之后才第一次被执行。
#
# 为什么必须有这一条：2026-11-06 20:26 有一笔改动直接推到了 main——在滚动构建器里
# 给持久状态的工作负载加了一条豁免，没有同步改同一文件里的那道门。那道门就在它改
# 的那个包里、跑起来是秒级，可推之前没人跑它；此后每一版 Test 与 Code Coverage 都
# 是红的，部署器停在 5dad1f95 上整整九版，前面已经落地的修复一条都没生效。这一条
# 腿判的就是这一类：改动落在哪个包，那个包自己的测试先在本机绿一次。
#
# 它不等于 CI 的 Test：没被改动、也不被它们依赖的 crate 仍然只有 CI 会跑。末尾的
# 清单会说出来，因为「跑过的绿」与「没跑的绿」在读数上同形。
if [ -n "${cargo_bin}" ]; then
    if ! git rev-parse --verify --quiet 'origin/main^{commit}' >/dev/null; then
        skip_step "Test" "本机没有 origin/main，推的范围判不出来（CI 的 Test 会整棵跑）"
    else
        mapfile -t test_crates < <(
            touched_crate_sources |
                awk -F/ '$1 == "crates" && NF >= 3 { print $2 }' | sort -u
        )
        test_args=(-p cogneva)
        test_count=0
        for crate_dir in ${test_crates[@]+"${test_crates[@]}"}; do
            package="$(sed -n 's/^name *= *"\([^"]*\)".*/\1/p' "crates/${crate_dir}/Cargo.toml" 2>/dev/null | head -1)"
            if [ -n "${package}" ] && [ "${package}" != "cogneva" ]; then
                test_args+=(-p "${package}")
                test_count=$((test_count + 1))
            elif [ -z "${package}" ]; then
                # 有 `.rs` 却读不到包名：那个目录不是工作区成员。另一条门
                # （`crate_source_membership_contract_test`）判这件事，这里只说没判到。
                skip_step "Test" "crates/${crate_dir} 下这次动过源码但读不到包名，这一份没判（CI 的 Test 会整棵跑）"
            fi
        done
        if [ "${test_count}" -gt 0 ]; then
            # 就地判，不展开 HEAD 的临时工作树：临时树里每个包根都是新路径，而 cargo
            # 的指纹按包根算，于是整棵工作区要重编一遍——实测在一个临时树里
            # `cargo build -p cog-core` 用了 55 秒，同一个包在本树里是命中缓存。
            # 这条腿要站在每一次推送前面，代价必须留在分钟级（本机实测整条 Test 腿
            # 4m41s，其中测试本身 133s）。判的因而是**工作树**：树上有未提交改动时，
            # 它测的比这一推多。多测不会假绿，只是可能替别人的改动红一次。
            echo "== Test：这次动过 ${test_count} 个 crate 的源码（其中 $(printf '%s\n' "${uncommitted_crate_sources}" | wc -l | tr -d ' ') 份还没提交），判的是工作树，外加装配件 cogneva"
            judge_head_tests() {
                build_slot_run "cargo test ${test_args[*]}" \
                    "${cargo_bin}" test "${test_args[@]}" --no-fail-fast
            }
            run_step "cargo test ${test_args[*]} --no-fail-fast" judge_head_tests
        else
            echo "== Test：这次要推的提交与工作树都没动 crates/ 下的 .rs，跳过（CI 的 Test 会整棵跑）"
        fi
    fi
else
    skip_step "Test" "本机没有可用的 cargo（CI 的 Test 会整棵跑）"
fi

# Check 作业本机跑不动（要编译整个工作区，比 CI 还慢），可它红得最多的一种形状本机
# 几秒钟就能判：**推上去的这几笔，新增了对一个这棵树里不存在的模块的引用**。
# 2026-10-29 实测过一次代价：一笔带 `use crate::test_support::scrape;` 而定义它的文件
# 还停在工作树里没提交的提交推了上去，工作树编译是绿的（文件在盘上），CI 红在
# unresolved import，部署器按设计停在最后一个绿 rev 上不滚，主线红了一小时。
# 本地没有任何读数看得见它——这条门连同工作树都不编译，就算编译也是读工作树。
# 判据只有一份（`check-commit-references.py`），这里是它的调用点。
if command -v python3 >/dev/null 2>&1 && git rev-parse --verify --quiet 'origin/main^{commit}' >/dev/null; then
    run_step "推上去的提交引用的 crate:: 路径在树上能解答" \
        python3 "${here}/check-commit-references.py" --base origin/main --head HEAD
else
    # 跑不了要说出来，不能安静跳过：跳过的绿与跑过的绿在读数上一模一样。
    skip_step "提交引用可达性" "本机没有 python3 或没有 origin/main（CI 的 Check 会跑编译）"
fi

# ShellCheck：清单同样由 git 推出来，与 CI 同一条命令。
mapfile -t shell_scripts < <(git ls-files '*.sh')
# 零个脚本时 shellcheck 没有参数，它读 stdin、读到空、退 0——「一个脚本都没读到」
# 与「每个脚本都过了」在退出码上同形，而前者意味着 `git ls-files` 没在仓库里跑或
# 索引是空的，那正是最该红的一刻。
if [ "${#shell_scripts[@]}" -eq 0 ]; then
    echo "RED  git ls-files '*.sh' 一个脚本都没读到：shellcheck 会读 stdin 退 0，" >&2
    echo "     那句话与「每个脚本都过了」同形——多半是没在仓库根跑，或索引是空的" >&2
    failures=$((failures + 1))
else
    run_step "shellcheck -S warning（${#shell_scripts[@]} 个脚本）" \
        shellcheck -S warning "${shell_scripts[@]}"
fi

# PowerShell 那一步只在装了 pwsh 的机器上可跑。跑不了要**说出来**，不能安静跳过——
# 跳过的绿与通过过的绿在读数上一模一样，而这正是这条门要防的那类假绿。
if command -v pwsh >/dev/null 2>&1; then
    run_step "bootstrap.ps1 语法" bash -c \
        'pwsh -NoProfile -Command "\$errs = \$null; [void][System.Management.Automation.PSParser]::Tokenize((Get-Content -Raw bootstrap.ps1), [ref]\$errs); if (\$errs.Count -gt 0) { \$errs | ForEach-Object { Write-Error \$_ }; exit 1 }"'
else
    skip_step "bootstrap.ps1 语法检查" "本机没有 pwsh（CI 会跑这一步）"
fi

echo
print_cost_table
echo
if [ "${unjudged}" -ne 0 ]; then
    echo "PRE-PUSH: ${unjudged} 步没进构建槽、没判；${failures} 步红。没判过的字节不算验过。"
    exit 1
fi
if [ "${failures}" -ne 0 ]; then
    echo "PRE-PUSH: ${failures} 步红，推上去 CI 也会红。"
    exit 1
fi
# 「判了的全绿」与「每一面都判过」是两句话。上一版只说前一句，于是缺工具的格子（印了
# SKIP 的那些）在判词上与验过同形——同一份字节在一台装了 pwsh 的机器上有 8 面都验过，
# 在这台只有 7 面，两次都说「全绿」。要推的是字节，不是机器，所以没验的那几面得点名。
# 名字与条数从同一个地方数出来。上一版用 `paste -d'、' -` 拼名字、再用 `tr '、' '\n'`
# 数条数：`paste -d` 的分隔符按**字节**取（C locale 下 `、` 只取了首字节 0xe3），印出来是
# 一个替换符、几个面名黏成一句；而 `tr` 也按字节切，正好把那一个字节切成换行，条数歪打正着
# 是对的。两处都不能留：名字是给人读的，读不出来等于没点名；条数若哪天跟着改了实现，就没
# 人发现它其实一直在数错。现在 awk 一次拼出名字（它写的是 UTF-8 字节，不经过任何 locale
# 规则），条数直接数数组。
skipped_unique=()
if [ "${#skipped_faces[@]}" -gt 0 ]; then
    mapfile -t skipped_unique < <(printf '%s\n' "${skipped_faces[@]}" | awk 'NF && !seen[$0]++')
fi
skipped_count="${#skipped_unique[@]}"
skipped_list=""
if [ "${skipped_count}" -gt 0 ]; then
    skipped_list="$(printf '%s\n' "${skipped_unique[@]}" |
        awk 'NR == 1 { out = $0; next } { out = out "、" $0 } END { printf "%s", out }')"
fi
if [ "${skipped_count}" -gt 0 ]; then
    echo "PRE-PUSH: 判过的 $(wc -l <"${segment_log}" 2>/dev/null | tr -d ' ') 步全绿；另有 ${skipped_count} 面本机没验，不算验过：${skipped_list}。"
    echo "         这几面推上去由 CI 判，它们红了这一跑看不出来——所以别把这一行读成整轮验过。"
else
    echo "PRE-PUSH: 上列的判据全绿。"
fi
echo "Clippy 只判这次要推的提交动过的 crate；其余 crate 的 lint 仍由 CI 整份跑。"
echo "Test 只判这次动过的包加装配件 cogneva；其余 crate 的测试仍由 CI 整棵跑。"
echo "未覆盖（要编译 / 要 Node，CI 才会跑）：Check / Code Coverage /"
echo "Version Contract / Web Build / Bootstrap Cross-Platform。"

# 绿了才记账。红的那一跑什么也不写：一份没验过的字节不能因为「上次跑过」就推得出去。
#
# 三笔分开记，因为它们回答三个不同的问题：
#   - 内容键（`content.<键>`）：这一跑判的就是工作树的字节，脏不脏无关——提交之后字节不变，
#     所以提交前后同一份指纹，提交那一问（`--status-content`）命中的正是判过的那一份；
#   - 改动键（`patch.<键>`）：从共同祖先到工作树的那份 diff 的 patch-id，再加上整门的
#     输入（`gate_inputs`：门脚本、`ci.yml`、工具版本）。远端往前走了、变基到新基底上，
#     改动还是这一批，这一笔照样命中——**前提是那几笔没动整门自己的输入**：输入变了，
#     那一跑判的是旧输入下的结论，这一批要按新输入重判。改动本身被改过（解冲突改了解法）
#     同样作废。推送那一问（`--status-patch`）问的是它；
#   - 推送键（`<HEAD+origin/main>`）：只在干净的树上成立，因为推上去的是 HEAD，而这跑判的
#     是工作树；脏的时候两者不是同一份字节，只在结尾说明。
mkdir -p "${stamp_dir}" 2>/dev/null
ckey="$(content_key)"
pkey="$(patch_key)"
# 「算不出改动键」与「没有改动可判」是两句话，读它们的人会去查不同的东西。共同祖先涨到了
# HEAD（远端已经含了这一支）、工作树又干净时，从基底到工作树本来就没有 diff，patch-id 是
# 空的——这不是读不出来，是没东西可读。2026-11-10 实测：这一形状记不上改动键，印的却是
# 「没有共同祖先，或临时索引写不了」，于是下一个人去查那把根本不缺的东西。
# 判据是**两个树对象相等**，用的就是这一跑自己那套定位工作树的办法（`add -A` 进临时索引），
# 不是另起一套更松的「脏不脏」：脏不脏问的是相对 HEAD，而这里问的是相对基底，两者在
# 「远端已经含了这一支」时不是一回事。
patch_why=""
if [ -z "${pkey}" ]; then
    base_probe="$(git merge-base HEAD origin/main 2>/dev/null || true)"
    if [ -n "${base_probe}" ]; then
        wtree="$(work_tree_oid)"
        btree_obj="$(git rev-parse --verify "${base_probe}^{tree}" 2>/dev/null || true)"
        if [ -n "${wtree}" ] && [ -n "${btree_obj}" ] && [ "${wtree}" = "${btree_obj}" ]; then
            patch_why="no-diff"
        else
            patch_why="unreadable"
        fi
    else
        patch_why="unreadable"
    fi
fi
# 开跑时那把指纹与现在这把对不上，说明中间有人动过树：现在这份字节没有被这一跑判过，
# 两把键都不记。判词本身照常留在输出里——它说的是开跑那棵树。
moved=0
moved_why="这棵树在跑的过程中变过"
tree_moved=0
if [ -n "${start_ckey}" ] && [ -z "${ckey}" ]; then
    # 「算不出」与「变过」都让这一跑不能记账，但叫人去做的事相反：前者是收尾这一问
    # 读不到（树忽然读不出来），后者是有人动了字节。落进同一条消息里，读的人会去找
    # 一个不存在的改动者。
    #
    # 这一支只丢内容键：改动键判的是「这一跑判过的是不是这批改动」，收尾那一问读不到
    # 与这批改动是不是同一批无关，作废它没有依据。说成「两把都不记」会让读的人把一份
    # 已经判过的改动重新跑一遍。
    moved=1
    moved_why="收尾算不出内容键"
    echo "记账：收尾算不出内容键（开跑 ${start_ckey:0:12}），内容键不记；改动键照记——这一跑判过这批改动，缺的是收尾那一问。" >&2
elif [ -n "${start_ckey}" ] && [ "${ckey}" != "${start_ckey}" ]; then
    moved=1
    tree_moved=1
    echo "记账：${moved_why}（开跑 ${start_ckey:0:12}，收尾 ${ckey:0:12}），这一跑的判词不属于现在这份字节，两把键都不记。" >&2
    ckey=""
fi
voided_patch=0
base_now="$(git merge-base HEAD origin/main 2>/dev/null || true)"
base_moved=0
if [ -n "${start_base}" ] && [ -n "${base_now}" ] && [ "${base_now}" != "${start_base}" ]; then
    base_moved=1
fi
if [ "${tree_moved}" -ne 0 ]; then
    # 树自己变过：这批改动与判过的那份不是一回事，两把键都不记（上面已经说过）。
    # 判据是 `tree_moved` 而不是 `moved`：收尾算不出内容键时 `moved` 也是 1，但那与
    # 「判过的是不是这批改动」无关，那只该丢内容键。
    voided_patch=1
    pkey=""
elif [ "${base_moved}" -ne 0 ]; then
    # 远端在这一跑期间往前走了，而树一个位没动。共同祖先只会在一种情况下变深：远端涨到了
    # 这个 HEAD 的后代（别人把这一支推上去了）。于是「这一推要补上的那一段」缩水了，收尾
    # 按**新基底**重算的这把才是推前钩子要问的那把——记它，判过的字节没变，不必重判。
    #
    # 旁支型前进（远端落了与 HEAD 无关的一笔）不会走到这里：那种情况下共同祖先一个位没动，
    # 改动键也一个位没动。这条边界值得写下来，否则下一个人会把「远端动了」当成触发器。
    moved=1
    if [ -z "${pkey}" ] && [ "${patch_why}" = "no-diff" ]; then
        echo "记账：远端在这跑期间已经含了这一支（基底 ${start_base:0:12} → ${base_now:0:12}），从新基底到工作树没有 diff，没有改动键可记；这一推没有要补的那一段。" >&2
    else
        echo "记账：远端在这跑期间往前走了（基底 ${start_base:0:12} → ${base_now:0:12}），判过的字节没变，改动键按新基底重记（${pkey:0:12}）。" >&2
    fi
elif [ -n "${start_pkey}" ] && [ -n "${pkey}" ] && [ "${pkey}" != "${start_pkey}" ]; then
    # 基底没动、改动键却变了：变的是这批改动本身（判过的那批不是你手上这批）。
    voided_patch=1
    pkey=""
    echo "记账：这批改动在跑的过程中变过（开跑 ${start_pkey:0:12}，收尾 ${pkey:0:12}），改动键不记。" >&2
fi
if [ -z "${ckey}" ]; then
    # 上面已经说过为什么不记（树变过），这里不再补一句读起来像「写盘失败」的话。
    :
elif printf '%s %s（工作树，%s 处未提交改动%s）\n' "$(date -Is)" "$(git rev-parse --short HEAD 2>/dev/null)" \
       "${run_dirty}" \
       "${skipped_list:+；本机没验 ${skipped_list}}" > "${stamp_dir}/content.${ckey}" 2>/dev/null; then
    echo "记账：内容键 ${ckey:0:12} 这份字节判过，提交前后都读得到这一笔。"
else
    echo "记账：写不进 ${stamp_dir}，这一跑照常算数，只是下一次提交／推送还得再判一遍。" >&2
fi
# 第三笔：这批改动本身。它与脏不脏无关——判的是从共同祖先到「现在这棵树」的那一份 diff，
# 提交只是把同样的字节换个名字（工作树 → 提交），patch-id 不变。所以这一笔在脏树上也记。
if [ "${voided_patch}" -ne 0 ]; then
    :
elif [ -z "${pkey}" ] && [ "${patch_why}" = "no-diff" ]; then
    echo "记账：没有改动键可记——远端已含这一支、工作树干净，从基底到工作树没有 diff，这批改动没有要推的那一段。"
elif [ -z "${pkey}" ]; then
    echo "记账：算不出改动键（没有共同祖先，或临时索引写不了），这一跑照常算数，只是变基之后还得重判。" >&2
elif printf '%s %s（改动，基底 %s）\n' "$(date -Is)" "$(git rev-parse --short HEAD 2>/dev/null)" \
       "$(git merge-base HEAD origin/main 2>/dev/null | cut -c1-7)" \
       > "${stamp_dir}/patch.${pkey}" 2>/dev/null; then
    echo "记账：改动键 ${pkey:0:12} 这批改动判过；远端往前走了、变基到新基底上照样命中（前提是那几笔没动整门的输入：门脚本、ci.yml、工具版本）。"
else
    echo "记账：算不出改动键（没有共同祖先，或临时索引写不了），这一跑照常算数，只是变基之后还得重判。" >&2
fi
if [ "${moved}" -ne 0 ]; then
    if [ "${tree_moved}" -eq 0 ] && [ "${base_moved}" -ne 0 ]; then
        echo "推进账不记：origin/main 在这跑期间往前走了，这一跑判的（HEAD, origin/main）这一对已经不是现在那一对；内容键与改动键另记，照常能推。" >&2
    else
        echo "推进账不记：${moved_why}，这一跑判的不是现在这份字节。" >&2
    fi
elif [ "${run_dirty}" -ne 0 ]; then
    echo "推进账另记：工作树上有 ${run_dirty} 处未提交改动，这一跑判的不是 HEAD 的字节，暂不记。"
    echo "      提交之后这棵树的字节没变，内容键照样命中，提交与推送两个钩子都不用再判。"
elif mkdir -p "${stamp_dir}" 2>/dev/null &&
   printf '%s %s\n' "$(date -Is)" "$(git rev-parse --short HEAD 2>/dev/null)" \
       > "${stamp_dir}/${run_key}" 2>/dev/null; then
    echo "记账：${run_key:0:12} 这份字节在推之前不用再判一遍，钩子会读到这一笔。"
else
    echo "记账：写不进 ${stamp_dir}，这一跑照常算数，只是下一推还得再判一遍。" >&2
fi
