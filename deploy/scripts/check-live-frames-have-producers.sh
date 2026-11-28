#!/usr/bin/env bash
# 「折叠进来的每一种帧，总得有一端会发它」——实时面的生产者门禁。
#
# 五个面的门禁（折叠写了、分派调了、画面读了、频道订了）可以全绿，屏幕上却仍然
# 一帧不动：那一族的帧在整棵树里**没有一个生产者**。消费端齐全是查得出来的，
# 生产端的缺失查不出来——一个接了从没发过的帧的面，不是「慢半拍的面」，是永远
# 不动的面，而且屏幕上没有任何一格会说这件事。2026-10-26 实测过一次：告警面折
# `resource_alert`，而 `AgentEvent::ResourceAlert` 在生产代码里一次都没被构造过，
# 只出现在 match 臂、变体声明和测试里。
#
# 判据（两段，缺一段就退化成查字符串）：
#   1. 消费端从折叠模块自己读：`store/live*.ts` 里 `event.type` 比的字面量、以及
#      它 switch 里 `case '…'` 的分支。这一份名单不手抄——手抄的名单会与模块分叉，
#      而分叉的那天这里照样绿。
#   2. 每个帧类型都要在 `crates/` 的生产代码里找到一次构造（`AgentEvent::变体 {`
#      或 `(…`，同一行带 `=>` 的是 match 臂不算），位置按文件:行印出来给读的人核。
#
# 局限写在这里，读的人不必猜：测试区按**条目**认（`#[cfg(test)]` 抹掉紧跟它的那一个
# 条目）；一条构造若藏在 `#[cfg(feature = "…")]` 后面，这里仍算它是生产代码。判的是
# 「有没有一端构造它」，不判它构造得对不对——后者归那个面自己的门禁。
#
# 用法：bash deploy/scripts/check-live-frames-have-producers.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
STORE = os.path.join(WEB, "store")
CRATES = os.path.join(ROOT, "crates")

if not os.path.isdir(STORE):
    sys.exit(f"读不到折叠模块目录：{STORE}")
if not os.path.isdir(CRATES):
    sys.exit(f"读不到源码目录：{CRATES}")

# snake_case → 线上变体名。规则的按规则推，不规则的写在这里（与网关的序列化名同一把尺子）。
IRREGULAR = {
    "re_act_step_start": "ReActStepStart",
    "re_act_step_end": "ReActStepEnd",
}

def variant_of(event_type):
    if event_type in IRREGULAR:
        return IRREGULAR[event_type]
    return "".join(part[:1].upper() + part[1:] for part in event_type.split("_") if part)

# ---- 1. 消费端：每个折叠模块吃哪几种帧（从模块自己读，不手抄） --------------------
consumed = {}   # face -> sorted list of event types
wildcard = []   # 只数到达、不认类型的模块
for name in sorted(os.listdir(STORE)):
    if re.fullmatch(r"live[A-Za-z]*Store\.ts", name):
        continue  # store 是 zustand 外壳，折叠在它旁边那个文件里
    if not re.fullmatch(r"live[A-Za-z]*\.ts", name):
        continue
    source = open(os.path.join(STORE, name), encoding="utf-8").read()
    types = set(re.findall(r"case\s+'([a-z0-9_]+)'\s*:", source))
    types |= set(re.findall(r"event\.type\s*[!=]==\s*'([a-z0-9_]+)'", source))
    # `switch (event.type)` 之后的 case 已经收进来了；`event.type` 没出现过比较
    # 却又不认类型的模块，就是只数到达的那种。
    if not types and "event.type" not in source:
        wildcard.append(name)
        continue
    if not types:
        sys.exit(
            f"web/src/store/{name} 读了 event.type 却没认出任何一种帧："
            "要么把类型写成变量（这里读不出来），要么它其实什么都不折"
        )
    consumed[name] = sorted(types)

if not consumed:
    sys.exit("web/src/store 里一个折叠模块都没读到：这门禁判的对象不见了")

# ---- 2. 生产端：每种帧在 crates/ 的生产代码里被构造过吗 --------------------------
#
# 「出现」不够：`match event { AgentEvent::X { agent_id: id, .. } => … }` 里也有这串
# 字，那是**认帧**不是**发帧**。两种形状各有一条机械判据：
#   - 花括号里带 `..`（余下字段不写）＝ match 模式，构造要把每个字段都写上；
#   - 花括号闭合后紧跟（可带 `|` 分支或换行）`=>` ＝ match 臂。
# 两条都不命中的才算一次构造。多行臂靠数括号配对，不靠看同一行有没有 `=>`。
def brace_body(source, open_at):
    """从 `{` 起取配对的内容与结束位置；括号不配对时返回 (None, open_at)。"""
    depth = 0
    for i in range(open_at, len(source)):
        ch = source[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return source[open_at + 1:i], i
    return None, open_at

ARM_AFTER = re.compile(r"\s*=>")

CFG_TEST = "#[cfg(test)]"

def blank_at(source, start, end):
    """把 [start, end) 抹成空白，换行留下——行号还是这个文件的行号。"""
    out = list(source)
    for i in range(start, end):
        if out[i] != "\n":
            out[i] = " "
    return "".join(out)


def production_of(source):
    """文件里不属于 `#[cfg(test)]` 条目的那部分。

    这一格曾经读成「第一处 `#[cfg(test)]` 到文件尾」，那条判据在**测试总在文件末尾**
    的写法下成立，在别的写法下会让门悄悄失明：2026-11-11 一笔把测试专用的构造器放在
    `new()` 旁边，于是 runtime.rs 里每一个事件的构造都落进了「测试区」，四个面被判成
    「没有一个生产者」——生产代码一行没动，红的是判据。属性管的是**紧跟它的那一个条目**，
    所以这里按条目抹，抹到它的右花括号（无花括号的条目抹到分号）为止。
    """
    out = source
    pos = 0
    while True:
        at = out.find(CFG_TEST, pos)
        if at < 0:
            return out
        item_at = at + len(CFG_TEST)
        open_at = out.find("{", item_at)
        semi_at = out.find(";", item_at)
        if open_at < 0 or (0 <= semi_at < open_at):
            end = semi_at + 1 if semi_at >= 0 else len(out)
        else:
            _, close = brace_body(out, open_at)
            end = close + 1 if close > open_at else len(out)
        out = blank_at(out, at, end)
        pos = end


constructor_cache = {}
def producers(variant):
    if variant in constructor_cache:
        return constructor_cache[variant]
    hits = []
    pattern = re.compile(r"AgentEvent::" + variant + r"\s*\{")
    for dirpath, dirnames, filenames in os.walk(CRATES):
        dirnames[:] = [d for d in dirnames if d != "target"]
        for fname in filenames:
            if not fname.endswith(".rs"):
                continue
            path = os.path.join(dirpath, fname)
            rel = os.path.relpath(path, ROOT)
            # `tests/` 下的集成测试不是生产代码；`benches`/`examples` 同理。
            if rel.split(os.sep)[1:2] and rel.split(os.sep)[2] in ("tests", "benches", "examples"):
                continue
            source = open(path, encoding="utf-8", errors="ignore").read()
            # 测试区按**条目**认：`#[cfg(test)]` 管的是紧跟它的那一个条目，不是文件余下
            # 全部。按「到文件尾」认时，一个测试专用的小构造器放在文件前部就足以让这门
            # 对下面所有事件的构造失明（`production_of` 的 doc 里写着那一次）。
            production = production_of(source)
            for m in pattern.finditer(production):
                body, close = brace_body(production, m.end() - 1)
                if body is None:
                    continue
                if ".." in body:
                    continue  # 余下字段不写：那是模式，不是构造
                if ARM_AFTER.match(production, close + 1):
                    continue  # 花括号后面是 `=>`：match 臂
                lineno = production.count("\n", 0, m.start()) + 1
                hits.append(f"{rel}:{lineno}")
    constructor_cache[variant] = hits
    return hits

missing = []
print("面                   帧类型                 生产者")
for face in sorted(consumed):
    for event_type in consumed[face]:
        variant = variant_of(event_type)
        hits = producers(variant)
        if hits:
            shown = hits[0] + (f"（共 {len(hits)} 处）" if len(hits) > 1 else "")
            print(f"{face:<20} {event_type:<22} {shown}")
        else:
            print(f"{face:<20} {event_type:<22} 没有")
            missing.append((face, event_type, variant))

for name in wildcard:
    print(f"{name:<20} {'(每一帧)':<22} 不适用：只数到达，不认类型")

if missing:
    print("", file=sys.stderr)
    for face, event_type, variant in missing:
        print(
            f"web/src/store/{face} 折 `{event_type}`，而 `AgentEvent::{variant}` 在 crates/ 的"
            f"生产代码里一次都没被构造过：这一面不是在等下一次轮询，是永远不会动——"
            f"要么给它一个生产者，要么把这个面从这一帧上摘下来。",
            file=sys.stderr,
        )
    sys.exit(1)

print(f"ok   - {len(consumed)} 个折叠模块吃的 {sum(len(v) for v in consumed.values())} 种帧，"
      f"每一种在 crates/ 里都有生产者")
PYEOF
