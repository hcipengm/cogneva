#!/usr/bin/env python3
"""把分析回来的答案写回台账，再用**固定程序**复判一遍形状。

接在 `criterion_prompt.py` 之后：那个只发请求，这个负责把答案落账并让固定程序再看一次
——模型不能自称写对了，格式不符照样不过。

答案文件每行一条：

    S20.1	判据读数: state-files → 0
    S24	判据读数: test = crates/x/tests/y.rs::proves_it
    S30	信息不足：2026-11-18 的现场记录 在 brain 的 internal-docs 里

写回之后跑的形状检查与前一步同一套：A 型要求面名非空且目标是整数；B 型要求文件在树里、
函数带测试属性。C 型不写格，原样打印出来——它是「还差哪份输入」，不是读数。

退出码：0 全部合格；1 有不合格（逐条点名）；2 读不到。
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import todo_source  # noqa: E402

A = re.compile(r"判据读数\s*:\s*([a-z0-9-]+)\s*(?:→|->)\s*(\d+)\s*$")
B = re.compile(r"判据读数\s*:\s*test\s*=\s*([^\s:]+)::([A-Za-z0-9_]+)")


def shape_problem(root: pathlib.Path, text: str) -> str | None:
    """固定程序对一行读数的形状判词；`None` 表示合格。"""
    if m := A.search(text):
        if not m.group(1).strip():
            return "A 型缺面名"
        return None
    if m := B.search(text):
        path, name = m.group(1), m.group(2)
        file = root / path
        if not file.is_file():
            return f"B 型的文件不在树里：{path}"
        body = file.read_text(errors="ignore")
        for hit in re.finditer(rf"fn\s+{re.escape(name)}\s*\(", body):
            head = body[max(0, hit.start() - 200) : hit.start()]
            if "#[test]" in head or "#[tokio::test]" in head:
                return None
        return f"B 型的函数 {name} 在 {path} 里没有测试属性"
    return "既不是 A 也不是 B"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("answers", help="答案文件")
    parser.add_argument("--root", default=".")
    parser.add_argument("--ledger", default=None)
    args = parser.parse_args(argv[1:])

    root = pathlib.Path(args.root).resolve()
    ledger = pathlib.Path(args.ledger or todo_source.default_spec()[3:]).expanduser()
    if not ledger.is_file():
        print(f"criterion_apply: 台账不在：{ledger}", file=sys.stderr)
        return 2

    answers: dict[str, str] = {}
    for line in pathlib.Path(args.answers).read_text(errors="ignore").splitlines():
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        ident, text = parts[0].strip(), parts[1].strip()
        if ident and text:
            answers[ident] = text

    lines = ledger.read_text(encoding="utf-8").splitlines(keepends=True)
    bad: list[str] = []
    pending_info: list[str] = []
    written: list[str] = []
    for ident, text in answers.items():
        if text.startswith("信息不足"):
            pending_info.append(f"{ident}: {text}")
            continue
        problem = shape_problem(root, text)
        if problem:
            bad.append(f"{ident}: {problem}（{text[:60]}）")
            continue
        for i, line in enumerate(lines):
            if not line.startswith(f"| {ident} |"):
                continue
            j = line.rfind("|", 0, line.rfind("touches:"))
            # 已有读数就替换，没有就插入
            if "判据读数" in line:
                line = re.sub(r"判据读数\s*:[^｜|]*", text, line, count=1)
                lines[i] = line
            else:
                lines[i] = line[:j] + " " + text + " " + line[j:]
            written.append(ident)
            break
        else:
            bad.append(f"{ident}: 台账里没有这一行")

    ledger.write_text("".join(lines), encoding="utf-8")
    for ident in written:
        print(f"  ok   {ident}")
    for line in bad:
        print(f"  坏   {line}")
    for line in pending_info:
        print(f"  待补 {line}")
    print(f"criterion_apply: 写入 {len(written)} 条，不合格 {len(bad)} 条，待补输入 {len(pending_info)} 条")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
