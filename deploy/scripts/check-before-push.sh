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
# 不覆盖：Check / Test / Code Coverage / Format / Version Contract /
# Web Build / Bootstrap Cross-Platform —— 它们要编译或要 Node，本机跑一遍可能比 CI
# 还慢。末尾会把这份清单印出来：**这条门绿不等于 CI 绿**，它只保证那几类「改动碰了
# 别的面的读者」的判据不是推上去才发现的。
#
# Test 里有一个例外：容量登记表那条测试（`rust_capacity_constants_registry_test.rs`）
# 在本机是秒级的纯文本扫描，所以它单独在这里跑（见下面那段为什么走 `rustc`）。Test 作业
# 整体仍然不覆盖——这一格补的是「加常量不补登记表」这一个具体的红，不是那个作业。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "${here}/../.." && pwd)"
cd "${root}" || exit 1

ci=".github/workflows/ci.yml"
[ -f "${ci}" ] || { echo "找不到 ${ci}（要从仓库根跑）"; exit 1; }

failures=0
run_suite_file() { # run_suite_file <说明> <模块名>
    # 合并成一个测试二进制之后，「跑哪一份文件」只能靠模块名当 filter 来选，而 libtest 在
    # filter 一条都命中不了时的结局是成功：`0 passed; 0 filtered out`，退出码 0。模块改名、
    # 或者某次重构把文件挪了层级，这一格就会**照常印绿而一条判据都没跑**——绿的形状与真跑
    # 过的绿逐字相同，这正是这份脚本别处（取不到 CI 步骤即红）治的那件事。所以判据不只读
    # 退出码，还要求它真的报出至少一条通过的用例。
    local what="$1"; shift
    local filter="$1"; shift
    local out status
    out="$("$@" "${filter}" 2>&1)"
    status=$?
    printf '%s\n' "${out}" | tail -6
    if [ "${status}" -ne 0 ]; then
        printf 'RED  %s\n' "${what}" >&2
        failures=$((failures + 1))
        return 0
    fi
    if ! printf '%s\n' "${out}" | grep -Eq 'test result: ok\. [1-9][0-9]* passed'; then
        printf 'RED  %s：filter“%s”一条用例都没命中，这条判据没跑\n' "${what}" "${filter}" >&2
        failures=$((failures + 1))
    fi
    return 0
}

run_step() { # run_step <说明> <命令…>
    local what="$1"; shift
    printf '== %s\n' "${what}"
    if "$@"; then
        return 0
    fi
    printf 'RED  %s\n' "${what}" >&2
    failures=$((failures + 1))
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
            printf 'SKIP  %s（%s）\n' "${a:-这一步没有名字}" "${b}"
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
mapfile -t script_tests < <(printf '%s\n' deploy/scripts/tests/*.test.sh .github/scripts/tests/*.test.sh)
for t in "${script_tests[@]}"; do
    run_step "${t}" bash "${t}"
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
capreg_rustc="$(command -v rustc || true)"
[ -n "${capreg_rustc}" ] || [ ! -x "${HOME}/.cargo/bin/rustc" ] || capreg_rustc="${HOME}/.cargo/bin/rustc"
capreg_cargo="$(command -v cargo || true)"
[ -n "${capreg_cargo}" ] || [ ! -x "${HOME}/.cargo/bin/cargo" ] || capreg_cargo="${HOME}/.cargo/bin/cargo"
capreg_bin="${TMPDIR:-/tmp}/cogneva-capreg.$$"
capreg_compiled=no
if [ -n "${capreg_rustc}" ] && CARGO_MANIFEST_DIR="${root}/crates/cogneva" \
        "${capreg_rustc}" --test --edition 2021 -o "${capreg_bin}" "${capreg_src}"; then
    capreg_compiled=yes
fi
if [ "${capreg_compiled}" = yes ]; then
    run_step "容量登记表（${capreg_src}，经 rustc 直接编）" "${capreg_bin}"
    rm -f "${capreg_bin}"
elif [ -n "${capreg_cargo}" ]; then
    run_step "容量登记表（${capreg_src}，经 cargo test：这份测试带 tests/common 的依赖，rustc 直接编找不到）" \
        run_suite_file "容量登记表（经 cargo test）" rust_capacity_constants_registry_test \
        "${capreg_cargo}" test -p cogneva --test suite
else
    # 与 pwsh 那一格同一条规矩：跑不了要说出来，不能安静跳过。
    echo "SKIP  容量登记表：本机既没有可用的 rustc 也没有 cargo（CI 的 Test 会跑这一步）"
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
cargo_bin="${capreg_cargo}"
if [ -n "${cargo_bin}" ]; then
    # 合并成一个测试二进制之后，选「一份文件」靠的是模块名当 filter：过滤词是模块路径的
    # 前缀，整份文件的所有用例都在它下面，挑一条用例仍然要再写一层名字——判据还是整份文件
    # 的，只是它现在住在 `suite` 里。两步分开跑而不是一次给两个 filter：`run_step` 会记下
    # 失败继续走，两个 verdict 照样都印出来。
    run_suite_file "名字契约（告警规则契约）" observability_alert_rule_contract_test \
        "${cargo_bin}" test -p cogneva --test suite
    run_suite_file "名字契约（观测面普查）" observability_scrape_census_contract_test \
        "${cargo_bin}" test -p cogneva --test suite
else
    echo "SKIP  名字契约：本机没有可用的 cargo（CI 的 Code Coverage 会跑这两步）"
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
        echo "SKIP  crate 骨架：本机没有 origin/main，推的范围判不出来（CI 的 Test 会跑）"
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
    echo "SKIP  crate 骨架：本机没有可用的 cargo（CI 的 Test 会跑这两步）"
fi

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
        echo "SKIP  Clippy：本机没有 origin/main，推的范围判不出来（CI 的 Clippy 会整份跑）"
    elif ! "${cargo_bin}" clippy --version >/dev/null 2>&1; then
        echo "SKIP  Clippy：这个工具链没装 clippy 组件（CI 的 Clippy 会整份跑）"
    else
        mapfile -t touched_crates < <(
            git diff --name-only 'origin/main...HEAD' -- 'crates/*.rs' |
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
                echo "SKIP  Clippy：crates/${crate_dir} 下这次动过源码但读不到包名，这一份没判（CI 的 Clippy 会整份跑）"
            fi
        done
        if [ "${crate_count}" -gt 0 ]; then
            repo_toplevel="$(git rev-parse --show-toplevel)"
            head_tree="$(mktemp -d "${TMPDIR:-/tmp}/cogneva-push-clippy.XXXXXX")"
            git worktree prune 2>/dev/null
            if git worktree add --detach --quiet "${head_tree}" HEAD 2>/dev/null; then
                echo "== Clippy：这次要推的提交动过 ${crate_count} 个 crate 的源码，判的是 HEAD 展开出来的那棵树"
                judge_head_tree() {
                    ( cd "${head_tree}" &&
                        CARGO_TARGET_DIR="${repo_toplevel}/target" \
                            "${cargo_bin}" clippy "${clippy_args[@]}" --all-targets -- -D warnings )
                }
                run_step "cargo clippy ${clippy_args[*]} --all-targets -- -D warnings" judge_head_tree
                git worktree remove --force "${head_tree}" >/dev/null 2>&1 || rm -rf "${head_tree}"
            else
                rm -rf "${head_tree}"
                echo "SKIP  Clippy：展开不出 HEAD 的临时工作树（${head_tree}），这一份没判（CI 的 Clippy 会整份跑）"
            fi
        else
            echo "== Clippy：这次要推的提交没动 crates/ 下的 .rs，跳过（CI 的 Clippy 会整份跑）"
        fi
    fi
else
    echo "SKIP  Clippy：本机没有可用的 cargo 或 python3（CI 的 Clippy 会整份跑）"
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
    echo "SKIP  提交引用可达性：本机没有 python3 或没有 origin/main（CI 的 Check 会跑编译）"
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
    echo "SKIP  bootstrap.ps1 语法检查：本机没有 pwsh（CI 会跑这一步）"
fi

echo
if [ "${failures}" -ne 0 ]; then
    echo "PRE-PUSH: ${failures} 步红，推上去 CI 也会红。"
    exit 1
fi
echo "PRE-PUSH: 上列的 shell 面判据全绿。"
echo "Clippy 只判这次要推的提交动过的 crate；其余 crate 的 lint 仍由 CI 整份跑。"
echo "未覆盖（要编译 / 要 Node，CI 才会跑）：Check / Test / Code Coverage /"
echo "Format / Version Contract / Web Build / Bootstrap Cross-Platform。"
