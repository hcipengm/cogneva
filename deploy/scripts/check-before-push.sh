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
# carrier: 人 —— 推之前手动跑。它没有自动调用点也不该有：跑的是 CI 自己的作业，
#   再挂到 CI 上就是同一件事跑两遍。`check-gate-carriers.py` 认这一行。
#
# 命令是从 `.github/workflows/ci.yml` 里**取**的，不是在这里抄一份：抄一份的门会在
# 有人加作业时静默变窄，而这条门唯一不能犯的错就是「它绿了，CI 却红了」。
#
# 不覆盖：Check / Test / Clippy / Code Coverage / Format / Version Contract /
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
        "${capreg_cargo}" test -p cogneva --test rust_capacity_constants_registry_test
else
    # 与 pwsh 那一格同一条规矩：跑不了要说出来，不能安静跳过。
    echo "SKIP  容量登记表：本机既没有可用的 rustc 也没有 cargo（CI 的 Test 会跑这一步）"
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
echo "未覆盖（要编译 / 要 Node，CI 才会跑）：Check / Test / Clippy / Code Coverage /"
echo "Format / Version Contract / Web Build / Bootstrap Cross-Platform。"
