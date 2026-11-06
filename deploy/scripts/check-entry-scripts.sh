#!/usr/bin/env bash
# 「入口脚本」这一格的检查：全树 *.sh 的 shellcheck、三处脚本测试、bootstrap.ps1
# 的解析。CI 与沙箱跑的是这一份脚本，不是两份各自抄了同一段步骤的作业。
#
# 为什么要有这一份：这一格在工作流里是六步，前三步（装工具）之外的三步是一段段
# 带 `mapfile`／`for`／`pwsh` 的正文，而变更管线从工作流里读门禁时只认单行、且
# 路径带 `scripts/check-` 的命令。于是这一整格在本地一条都没跑过，读数上却与
# 「跑过且绿」同形——脚本改错了，第一次知情的时刻永远是推送之后。
#
# 现在三处正文搬到这里，工作流里那三步合成为一行 `bash deploy/scripts/check-entry-scripts.sh`。
# 两边是同一批字节：改一处，两边同时改。
#
# 三段的判据各自不同，所以各自报，最后合成一个退出码：
#   0  三段都跑了且都绿
#   1  有东西红了（先报红，因为红是可以动手的那一种）
#   2  有东西没跑成（缺工具／缺库），红绿都还不知道——这是全树的约定：跑不了
#      不是跳过，也不是通过
#
# 依赖：shellcheck、git、bash；脚本测试还要 psql、helm、kubectl、python3+PyYAML、
# promtool；PowerShell 那一步还要 pwsh。缺哪一个就点名哪一个，不静默少跑。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
cd "${ROOT}" || exit 2

unavailable=()
# 跑成了哪几段。exit 2 时这份清单要与缺的那份一起印出来：只报「缺什么」，读的人
# 不知道这一格到底判了多少；而「一段没跑」与「一段都没跑」在处置上是两件事。
ran=()
# 脚本测试那一格的依赖单独收：缺任何一样，这一格整个不跑。原因是这些测试**故意**
# 在缺工具时红（告警规则那一族拿不到 promtool 就把「看不了」记成红，见它们自己的头
# 注释「看不了 ≠ 一致」）。跑下去报出来的会是一条与这次改动无关的红，而这条读数比
# 「没跑」更糟——它会把一次真实的缺陷藏在一片假红后面。
tests_unavailable=()

need() {
    command -v "$1" >/dev/null 2>&1 || unavailable+=("$1")
}

need shellcheck
need pwsh

for t in psql helm kubectl promtool; do
    command -v "$t" >/dev/null 2>&1 || tests_unavailable+=("$t")
done
# PyYAML 不是一个命令，缺了它那几条读清单的脚本测试会以 ImportError 的样子红，
# 而 ImportError 在读的人眼里是一条规则错了——与 CI 里那步 Ensure 同一个理由。
if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import yaml' >/dev/null 2>&1 || tests_unavailable+=("python3-yaml")
else
    tests_unavailable+=("python3")
fi
# 脚本测试里有一批要连真库（引导脚本那条门判的是「这段 SQL 在 Postgres 上跑不跑得
# 通」）。CI 给的是它自己那个用完即弃的实例，沙箱给的是变更验证那条测试库。
if [ -z "${COGNEVA_TEST_DATABASE_URL:-}" ]; then
    tests_unavailable+=("COGNEVA_TEST_DATABASE_URL")
fi
if [ "${#tests_unavailable[@]}" -gt 0 ]; then
    unavailable+=("${tests_unavailable[@]}")
fi

fail() {
    printf '\n== %s\n' "$1" >&2
    shift
    printf '%s\n' "$@" >&2
}

# 1. shellcheck：清单从树里数，不手抄。手抄的名单在加了一个脚本之后不会响，
#    而「漏掉一个」与「那个脚本本来就干净」在读数上是同一格。
if command -v shellcheck >/dev/null 2>&1; then
    printf '\n== shellcheck\n'
    mapfile -t scripts < <(git ls-files '*.sh')
    printf 'checking %d scripts\n' "${#scripts[@]}"
    if ! shellcheck -S warning "${scripts[@]}"; then
        fail shellcheck "shellcheck -S warning 报错了（上面点名的文件）"
        exit 1
    fi
    ran+=("shellcheck（${#scripts[@]} 个脚本）")
fi

# 2. 三处脚本测试。目录是 glob 出来的，不是列出来的：守卫自己的测试在 .github/
#    下面，评测主机的在 scripts/eval/ 下面，而手抄的清单正是「加了一个就安静」的
#    那一种载体。
if [ "${#tests_unavailable[@]}" -gt 0 ]; then
    printf '\n== script tests\n' >&2
    printf '没跑：缺 %s——这些测试在缺工具时会故意红，跑下去会把缺工具报成一条与改动无关的红\n' "${tests_unavailable[*]}" >&2
elif command -v bash >/dev/null 2>&1; then
    printf '\n== script tests\n'
    for t in deploy/scripts/tests/*.test.sh .github/scripts/tests/*.test.sh scripts/eval/*.test.sh; do
        echo "== ${t}"
        # 红在剧本自己的断言里，它的输出里往往没有一行带 `path:line`——而「红的一跑
        # 点名哪条原则」正是靠这样一行把日志接回文件头部的声明。所以红的这一份由
        # 跑它的人自己报出来，格式与编译器指错同一形状。行号恒为 1：判据是「这个
        # 文件红过」，不是「它的第几行红的」。
        #
        # 红的那一份的**正文**也带上来（`::error::` 注解）：日志只有对仓库有读权限
        # 的人拿得到，注解是任何人都能通过 API 取的。不带正文时，外面看到的只是一句
        # exit 1，判不了这条红是「规则写错了」还是「这台机器没有 promtool」。
        log="$(mktemp)"
        if ! bash "${t}" >"${log}" 2>&1; then
            cat "${log}"
            echo "--> ${t}:1:1: failed"
            while IFS= read -r line; do
                echo "::error file=${t},line=1::${line//%/%25}"
            done < <(tail -n 12 "${log}")
            rm -f "${log}"
            fail "script tests" "${t} 红了（上面是它的输出）"
            exit 1
        fi
        tail -n 2 "${log}"
        rm -f "${log}"
        ran+=("${t}")
    done
fi

# 3. bootstrap.ps1 的解析。这一条要 pwsh，而 pwsh 在沙箱宿主上通常没有——没有就
#    记进「没跑成」，而不是当作通过。
if command -v pwsh >/dev/null 2>&1; then
    printf '\n== bootstrap.ps1 parse\n'
    if ! pwsh -NoProfile -Command "\$errs = \$null; [void][System.Management.Automation.PSParser]::Tokenize((Get-Content -Raw bootstrap.ps1), [ref]\$errs); if (\$errs.Count -gt 0) { \$errs | ForEach-Object { Write-Error \$_ }; exit 1 }; 'bootstrap.ps1 parse OK'"; then
        fail "bootstrap.ps1 parse" "bootstrap.ps1 解析失败（上面是 pwsh 的报错）"
        exit 1
    fi
    ran+=("bootstrap.ps1 parse")
fi

if [ "${#unavailable[@]}" -gt 0 ]; then
    if [ "${#ran[@]}" -gt 0 ]; then
        printf '\ncheck-entry-scripts: 跑了且绿：%s\n' "${ran[*]}" >&2
    else
        printf '\ncheck-entry-scripts: 一段都没跑成\n' >&2
    fi
    printf 'check-entry-scripts: 没跑成：缺 %s（exit 2）\n' "${unavailable[*]}" >&2
    exit 2
fi

printf '\ncheck-entry-scripts: shellcheck、脚本测试、bootstrap.ps1 三段都通过\n'
