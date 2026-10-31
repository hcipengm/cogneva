#!/usr/bin/env bash
# principle: feedback_a_role_shift_is_not_a_change_of_rank
# check-handover-verb.py 的定向测试。
#
# 被测的是「写人类与系统分工的变化时，动词只许表达转换，不许表达升降」。治的病是这句读起来
# 顺、也没人反对的话：公开 README 里「人类从操作者退化为设定者」把职责环节的转移写成人被
# 降级，「上移」又把同一条轴倒过来写成升格——两个方向都在给两个角色排高低，各被属主否了
# 一次。它不挡任何门、不报任何错，只是把产品主张读成一句口号。
#
# 判据钉句形不钉裸子串，所以格子的另一半钉「不该红的别红」：
#   - 中文段里「退化」与「操作者」同句 ⇒ 红，判词点名行号与那个词；
#   - 英文段里 `demote` 与 `operator` 同一行 ⇒ 红；
#   - 中性转换（「转为」／`shift from … to …`）⇒ 绿；
#   - 升降词与操作者**不同行** ⇒ 绿：正文里「技能效果退化」「entries demoted」都是正当用法；
#   - 段落被改名 ⇒ 3，不是 0：扫描面缩到零与干净在汇总行上同形；
#   - 段落里一处都没提到操作者 ⇒ 3，同上；
#   - 真树：0，且两段都判到了。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-handover-verb.py"
[ -f "$gate" ] || { echo "FAIL: 被测脚本不在：$gate" >&2; exit 1; }

pass=0
fail=0
ok() { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1（期望退出码 $2，实得 $3）"; fi; }
ck_has() {
    if printf '%s\n' "$2" | grep -qF -- "$3"; then ok "$1"; else
        bad "$1（期望含 $3）"
        printf '%s\n' "$2" | sed 's/^/        实得: /' | head -12 >&2
    fi
}

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

zh() { # $1=目录 $2=标题 $3=段内正文
    cat >"$1/README.zh-CN.md" <<MD
# 项目

## 三大支柱

### $2

$3

### 全进化

这一段不在判据里：操作者把它升为了一等公民。
MD
}

en() { # $1=目录 $2=标题 $3=段内正文
    cat >"$1/README.md" <<MD
# Project

### $2

$3

### Omni-evolution

This section is not judged: the operator may promote anything here.
MD
}

# ---- case 1: 中文段里「退化」与「操作者」同句 ----
d="$work/degrade"; mkdir -p "$d"
zh "$d" '真自治' '人类从操作者退化为设定者，系统从执行者进化为主角。'
en "$d" 'Real-autonomous' 'Humans shift from operators to goal-setters.'
out="$(python3 "$gate" "$d" 2>&1)"; rc=$?
ck_rc "「退化」与操作者同句 ⇒ 红" 1 "$rc"
ck_has "判词点名文件与行号" "$out" "README.zh-CN.md:7"
ck_has "判词点名那个词" "$out" "「退化」"
ck_has "汇总数出违例" "$out" "violations=1"

# ---- case 2: 英文段里 demote 与 operator 同一行 ----
d="$work/demote"; mkdir -p "$d"
zh "$d" '真自治' '人类从操作者转为设定者。'
en "$d" 'Real-autonomous' 'Humans are demoted from operators to goal-setters.'
out="$(python3 "$gate" "$d" 2>&1)"; rc=$?
ck_rc "demote 与 operator 同一行 ⇒ 红" 1 "$rc"
ck_has "判词点名英文那一份" "$out" "README.md:5"

# ---- case 3: 中性转换 ----
d="$work/neutral"; mkdir -p "$d"
zh "$d" '真自治' '人类从操作者转为设定者，系统从执行者变为主角。'
en "$d" 'Real-autonomous' 'Humans shift from operators to goal-setters.'
out="$(python3 "$gate" "$d" 2>&1)"; rc=$?
ck_rc "中性转换 ⇒ 绿" 0 "$rc"
ck_has "两段都判到了" "$out" "sections=2"
ck_has "汇总里没有违例" "$out" "violations=0"

# ---- case 4: 升降词与操作者不同行 ----
d="$work/other-lines"; mkdir -p "$d"
zh "$d" '真自治' '系统在某些情况下会退化。
人类从操作者转为设定者。'
en "$d" 'Real-autonomous' 'Some skills degrade over time.
Humans shift from operators to goal-setters.'
out="$(python3 "$gate" "$d" 2>&1)"; rc=$?
ck_rc "升降词不在操作者那句上 ⇒ 绿（钉句形不钉裸子串）" 0 "$rc"

# ---- case 5: 段落被改名 ----
d="$work/renamed"; mkdir -p "$d"
zh "$d" '自治能力' '人类从操作者退化为设定者。'
en "$d" 'Real-autonomous' 'Humans shift from operators to goal-setters.'
out="$(python3 "$gate" "$d" 2>&1)"; rc=$?
ck_rc "段找不到 ⇒ 3，不是 0" 3 "$rc"
ck_has "说清扫的是哪一段" "$out" "扫描面缩到零"

# ---- case 6: 段落里零处提到操作者 ----
d="$work/silent"; mkdir -p "$d"
zh "$d" '真自治' '这一节改成了纯能力描述，没有主语。'
en "$d" 'Real-autonomous' 'Humans shift from operators to goal-setters.'
out="$(python3 "$gate" "$d" 2>&1)"; rc=$?
ck_rc "段内零处提到操作者 ⇒ 3，不是 0" 3 "$rc"
ck_has "说清判的是零句" "$out" "一处都没提到操作者"

# ---- case 7: 真树 ----
out="$(cd "$repo" && python3 "$gate" . 2>&1)"; rc=$?
ck_rc "真树的两段都干净" 0 "$rc"
ck_has "真树里两段都进了判据" "$out" "sections=2"

printf '\nhandover-verb.test: %d passed / %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
