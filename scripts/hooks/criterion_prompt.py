#!/usr/bin/env python3
"""把「写不出读数」的待办变成一次语义分析调用。

属主 2026-11-21：想不清楚的不许降级，可以调大模型把它想清楚。这个脚本就是那次
调用的**输入构造器**——它不判任何事，只把每一条缺读数的待办连同它需要的信息，拼成
一段可以直接发给模型的文本。

模型要回答的只有三选一，且必须能被机器接着判：

    A  判据读数: <普查面> → <目标数>      # 集合由扫描定义，目标是不剩
    B  判据读数: test = <文件>::<函数>     # 一条断言终态的测试
    C  信息不足：<缺哪一份输入，去问谁>    # 它把「想不到」变成「还差什么」

C 不是降级：它必须点名缺的那份输入（哪个文档、哪次现场、哪张表），那一份拿到之后
再问一次。所以这条循环永远不会停在「想不出」。

用法：
    criterion_prompt.py --root . [--source md:<台账>] > /tmp/ask.txt
"""

from __future__ import annotations

import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import todo_source  # noqa: E402

HEADER = """你在分析一批待办条目，目标是把每一条变成**机器判得出真假**的读数。

对每一条，只输出三选一：

A  判据读数: <普查面名字> → <目标数>
   —— 当终态是「某种东西在某棵树上不再存在」时用。集合必须由一条可执行的扫描定义
      （grep / 遍历路径 / 枚举闭集），目标数是扫描到 0 处。

B  判据读数: test = <文件>::<测试函数名>
   —— 当终态是「某段行为在某种输入下如此」时用。函数名要写得出现在测试里，断言要
      直接断言终态，不许断言替身（返回 Ok、字段非空）。

C  信息不足：<缺哪一份输入> <去问谁 / 在哪找>
   —— 当你答不出 A 或 B。必须点名缺的那一份具体输入，不许写「需要更多上下文」。

不要解释、不要寒暄，一条一行。

"""


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--source", default=None)
    args = parser.parse_args(argv[1:])

    source = args.source or todo_source.default_spec()
    try:
        items = todo_source.load(source)
    except todo_source.Unreadable as e:
        print(f"criterion_prompt: 台账读不到：{e}", file=sys.stderr)
        return 2

    # 判「有没有读数」看的是**行原文**：`判据读数` 不是三格之一，`cells` 里没有它。
    # 用 `cells` 判断会得到「全都没读数」——一个与事实无关的数。
    need = [
        i for i in items
        if "判据读数" not in i.text and "代码" not in i.cells
    ]
    print(HEADER)
    print(f"# 树：{pathlib.Path(args.root).resolve()}")
    print(f"# 缺读数的条目：{len(need)} 条")
    for item in need:
        print(f"\n## {item.id}")
        print(item.text[:1200])
        print(f"[触点] {', '.join(item.touches)}")
    print(f"\n共 {len(need)} 条，请逐条给 A / B / C。")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
