#!/usr/bin/env python3
"""判两份 README 里「人机分工变化」那句话用的动词。

写人类与系统分工的变化时，动词只许表达**转换**（转为／变为／`shift from … to …`），
不许表达**升降**（退化／上移／升为／降为／demote／promote／move up）。理由不是文风：
「退化」把职责环节的转移写成人被降级，「上移」又把同一条轴倒过来写成升格，两个方向都在
给两个角色排高低——而操作与设定不是同一架梯子的上下层，是两个环节。这句写歪了，公开
README 的「真自治」就被人读成一句口号；2026-10-23 前后两版各被属主否了一次。

判据钉的是**句形**，不是裸子串：升降词只有在**同一行还提到操作者**时才算数。
「技能效果退化」「entries demoted」这一类正当用法在正文里到处都是，按裸词判会打到它们。

扫描面只有两段：`README.md` 的 `Real-autonomous` 一节、`README.zh-CN.md` 的「真自治」一节。
一段都没找到、或某一段里一次都没提到操作者，都退 3——「没读到」与「读到了且干净」是两句话，
合起来写会让这道门在有人改标题那天安静地什么都不判。

退出码：0 两段都干净；1 至少一处升降词与操作者同句（逐处点名行号）；
        3 判不了（文件不在、段找不到、段里零处提到操作者）。

用法：check-handover-verb.py [仓库根]
"""

import re
import sys

# 段：文件里出现这个标记的那一级标题开始，到下一条同级或更高级标题为止。
SECTIONS = [
    ("README.md", re.compile(r"real-autonomous", re.I)),
    ("README.zh-CN.md", re.compile("真自治")),
]

# 这一族是记忆正文点名的那些，加词是一次有意的动作，不是随手放宽。
RANK_VERB = re.compile(
    r"退化|上移|升为|降为|\b(?:de|pro)mote[sd]?\b|\bmov(?:e|ed|ing) up\b",
    re.I,
)
ACTOR = re.compile(r"operator|操作者", re.I)
HEADING = re.compile(r"^(#{1,6})\s+(.*)$")


def section(lines, marker):
    """返回 (起始行号, 结束行号, 标题)，行号 0 起；找不到返回 None。"""
    for i, line in enumerate(lines):
        m = HEADING.match(line)
        if not m or not marker.search(m.group(2)):
            continue
        level = len(m.group(1))
        end = len(lines)
        for j in range(i + 1, len(lines)):
            n = HEADING.match(lines[j])
            if n and len(n.group(1)) <= level:
                end = j
                break
        return i, end, m.group(2)
    return None


def main(argv):
    root = argv[1] if len(argv) > 1 else "."
    violations = []
    judged = 0
    actor_lines = 0

    for name, marker in SECTIONS:
        path = f"{root}/{name}"
        try:
            with open(path, encoding="utf-8") as fh:
                lines = fh.read().splitlines()
        except OSError as exc:
            print(f"check-handover-verb: 读不到 {name}：{exc}", file=sys.stderr)
            return 3

        found = section(lines, marker)
        if found is None:
            print(
                f"check-handover-verb: {name} 里找不到标题带 {marker.pattern!r} 的一节——"
                "扫描面缩到零，不是干净",
                file=sys.stderr,
            )
            return 3
        start, end, title = found

        here = 0
        for i in range(start, end):
            line = lines[i]
            if not ACTOR.search(line):
                continue
            here += 1
            m = RANK_VERB.search(line)
            if m:
                violations.append((name, i + 1, m.group(0), line.strip()))

        judged += 1
        actor_lines += here
        if here == 0:
            print(
                f"check-handover-verb: {name} 的「{title}」一节里一处都没提到操作者/operator——"
                "这一节能判的句子是零，不是干净",
                file=sys.stderr,
            )
            return 3
        print(f"ok    - {name}：{title}，{here} 行提到操作者")

    for name, lineno, verb, text in violations:
        print(f"RED   - {name}:{lineno}「{verb}」与操作者同句：{text}")
        print("        改法：动词取中性转换义（转为／变为／shift from … to …）；")
        print("              被写的一方是环节，不是人的等级。")

    print(
        f"check-handover-verb: sections={judged} operator_lines={actor_lines} "
        f"violations={len(violations)}"
    )
    if violations:
        return 1
    if judged == 0:
        print(
            "check-handover-verb: judged nothing (not the same as clean)", file=sys.stderr
        )
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
