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
path, wanted = sys.argv[1], set(sys.argv[2:])
job = None
for raw in open(path, encoding="utf-8"):
    line = raw.rstrip("\n")
    header = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
    if header:
        job = header.group(1)
        continue
    if job not in wanted:
        continue
    step = re.match(r"^\s+run: (bash|python3|sh) (\S.*)$", line)
    if step:
        print(f"RUN\t{step.group(1)}\t{step.group(2).strip()}")
        continue
    if re.match(r"^\s+run: \|\s*$", line):
        print("SKIP\t多行脚本块")
        continue
    if re.match(r"^\s+run: sudo ", line):
        print("SKIP\t装依赖")
        continue
    other = re.match(r"^\s+run: (.*)$", line)
    if other:
        print(f"OTHER\t{other.group(1).strip()}")
PY
)
if [ "${#ci_steps[@]}" -eq 0 ]; then
    echo "RED  没能从 ${ci} 取到任何一步：作业块改名了，这条门正静静地什么都不跑" >&2
    failures=$((failures + 1))
fi
ran=0; skipped=0; unrecognised=0
for step in "${ci_steps[@]}"; do
    [ -n "${step}" ] || continue
    IFS=$'\t' read -r kind a b <<<"${step}"
    case "${kind}" in
        RUN)
            ran=$((ran + 1))
            read -r -a argv <<<"${b}"
            run_step "${b}" "${a}" "${argv[@]}"
            ;;
        SKIP)
            skipped=$((skipped + 1))
            printf 'SKIP  %s（%s）\n' "${b}" "${a}"
            ;;
        OTHER)
            unrecognised=$((unrecognised + 1))
            printf 'RED   CI 里这一步本地认不出，推之前不会跑它：%s\n' "${b}" >&2
            failures=$((failures + 1))
            ;;
    esac
done
printf '== 从 %s 取到 %d 步，跳过 %d 步（多行脚本块 / 装依赖），认不出 %d 步\n' \
    "${ci}" "${ran}" "${skipped}" "${unrecognised}"

# 脚本测试：CI 用同一个 glob 跑，这里照抄它的形状而不是列名字。
mapfile -t script_tests < <(printf '%s\n' deploy/scripts/tests/*.test.sh .github/scripts/tests/*.test.sh)
for t in "${script_tests[@]}"; do
    run_step "${t}" bash "${t}"
done

# ShellCheck：清单同样由 git 推出来，与 CI 同一条命令。
mapfile -t shell_scripts < <(git ls-files '*.sh')
run_step "shellcheck -S warning（${#shell_scripts[@]} 个脚本）" \
    shellcheck -S warning "${shell_scripts[@]}"

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
