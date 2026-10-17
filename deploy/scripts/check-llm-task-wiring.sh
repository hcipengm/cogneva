#!/usr/bin/env bash
# 任务轴（x-cogneva-task）的接线门禁。
#
# token 账按三条轴记每一笔花费：actor 说「哪个组件烧的」，origin 说「在哪个环境
# 烧的」，task 说「为哪一件事烧的」。前两条都已落地；第三条是判「这次改动值不值
# 这些 token」「这个任务一共花了多少」以及「在任务边界配速」唯一可能的落点——
# 缺了它，前两个问题在账上无处落脚，第三个只能按秒摊（而按秒摊必然在任务中途拒
# 调用，等于把浪费从尾部挪到中间）。
#
# 这条轴坏起来同样没有症状：头名写错一个字母、网关不再读它、或运行时新加一条
# LLM 调用路径忘了带上它，账照常落行，只是那一列空着；而空着读作「没声明」，
# 与「这件事没花钱」在读数面上不同形、却与「这条轴没人接」同形。所以判据要
# 提前到接线形状上，而不是等读数面少一格。
#
# 判据（全部按**形式**认，不按裸子串认）：
#   1. 头名只在一处定义：`crates/cog-core` 里的 `LLM_TASK_HEADER`，且整棵树里
#      没有第二处写出这个字面量——第二个拼法就是第二个值。
#   2. 网关读它：`security_gateway.rs` 里以 `LLM_TASK_HEADER` 取名。
#   3. 台账写得下它：`usage_store.rs` 里既有 `ADD COLUMN IF NOT EXISTS task_id`，
#      又有把 `task_id` 列进 INSERT 的写法。
#   4. 有人声明它：树里存在 `with_task(` 调用点，且 agent 运行时里每一条
#      `ChatOptions {` 都经过 `stamp_task(`——新加一条 LLM 调用路径而不盖任务身份，
#      在这里红。运行时在没有任务身份时调用 `stamp_task` 是无害的（它原样返回）。
#
# 用法：bash deploy/scripts/check-llm-task-wiring.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import glob, os, re, sys

ROOT = sys.argv[1]
CRATES = os.path.join(ROOT, "crates")
CORE_LLM = os.path.join(CRATES, "cog-core/src/contract/llm.rs")
GATEWAY = os.path.join(CRATES, "cog-gateway/src/security_gateway.rs")
LEDGER = os.path.join(CRATES, "cog-observability/src/usage_store.rs")
RUNTIME = os.path.join(CRATES, "cog-agent/src/agent_kernel/runtime.rs")
HEADER_NAME = "x-cogneva-task"
HEADER_CONST = "LLM_TASK_HEADER"

def read(path):
    try:
        return open(path, encoding="utf-8").read()
    except OSError as e:
        sys.exit(f"读不到 {path}：{e}")

def members():
    for path in glob.glob(os.path.join(CRATES, "**", "*.rs"), recursive=True):
        yield path, read(path)

failures = []

def need(condition, message):
    if not condition:
        failures.append(message)

core = read(CORE_LLM)
# 1. 唯一的一处定义。
need(
    re.search(r'pub\s+const\s+' + HEADER_CONST + r'\s*:\s*&str\s*=\s*"' + HEADER_NAME + r'"', core) is not None,
    f"{CORE_LLM}：找不到 `pub const {HEADER_CONST}: &str = \"{HEADER_NAME}\"`。"
    f"头名是跨 crate 的契约，定义不在契约层，写者就只能自己拼字符串，第二个拼法就是第二个值",
)
# 同一个字面量不许在别处再写一遍（注释里提到它不算写者，所以按字符串字面量认）。
literal = re.compile(r'"' + re.escape(HEADER_NAME) + r'"')
seen = []
for path, text in members():
    if literal.search(text):
        seen.append(os.path.relpath(path, ROOT))
need(
    seen == [os.path.relpath(CORE_LLM, ROOT)],
    f"头名字面量 {HEADER_NAME!r} 出现在这些文件里：{seen}。它只该出现在契约层的定义上，"
    f"其余地方一律引用常量——直接写字面量的那一处，改常量时不会跟着改",
)

# 2. 网关真的读它。
gateway = read(GATEWAY)
need(
    re.search(r'\.get\(\s*(?:cog_core::)?' + HEADER_CONST + r'\s*\)', gateway) is not None,
    f"{GATEWAY}：没有以 {HEADER_CONST} 取名。台账列存在、写者也填，但网关不读请求头，"
    f"这一列就永远是空——而空读作「没声明」，与「这条轴没接」同形",
)

# 3. 台账写得下它：列与 INSERT 都要有。
ledger = read(LEDGER)
need(
    "ADD COLUMN IF NOT EXISTS task_id" in ledger,
    f"{LEDGER}：没有 task_id 的加列迁移。新装库会从 CREATE 建表拿到它，"
    f"已装库不会——升级路径丢一列，旧库的账就永远答不了按任务的问题",
)
need(
    re.search(r'INSERT\s+INTO\s+gateway_llm_usage[\s\S]{0,400}?task_id', ledger) is not None,
    f"{LEDGER}：INSERT 的列里没有 task_id。列建了而写者不填，读者读到的全是 NULL，"
    f"与「没有人声明过任务」在读数面上完全一样",
)

# 4. 有人声明它，且运行时每条 LLM 调用路径都盖上。
producers = [os.path.relpath(p, ROOT) for p, t in members() if re.search(r'\bwith_task\(', t)]
need(
    producers != [],
    "树里没有任何 with_task( 调用点：头常量与读侧都在，但没有写者——这条轴是空的",
)
runtime = read(RUNTIME)
# 按**赋值形式**认构造点：函数签名里的 `-> cog_core::ChatOptions {` 也是同一个
# 括号形状，按裸子串数会把它算成第四条调用路径（本门第一版就误报了这一条）。
call_sites = len(re.findall(r'=\s*cog_core::ChatOptions\s*\{', runtime))
stamped = len(re.findall(r'self\.stamp_task\(', runtime))
need(
    call_sites > 0 and stamped >= call_sites,
    f"{RUNTIME}：有 {call_sites} 处 ChatOptions 构造，只盖了 {stamped} 处任务身份。"
    f"运行时的每一条 LLM 调用都属于某一次 run，漏盖的那条路径花的钱归不到任何任务上；"
    f"没有任务身份时 stamp_task 原样返回，所以每条路径都该经过它",
)

if failures:
    for f in failures:
        print("FAIL: " + f, file=sys.stderr)
    sys.exit(1)
print(
    f"LLM TASK WIRING OK：{HEADER_NAME} 只在契约层定义一次；网关读它、台账建有 task_id 列"
    f"并写它；声明方在 {producers}；运行时 {stamped}/{call_sites} 处调用都盖了任务身份"
)
PYEOF
