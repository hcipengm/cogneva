#!/usr/bin/env python3
"""同一份清单里，一个键只能出现一次。

2026-11-18 现场：`COGNEVA_MEMORY_API_BASE` 在网关的环境变量列表里出现了两次——
一笔为了让 parity 能逐字段比对而加，另一笔加在 git-mirror 注释块中间。两笔各自
都对，合起来是一份 server-side apply 拒收的清单：

    duplicate entries for key [name="COGNEVA_MEMORY_API_BASE"]

整轮滚动在 `security-gateway` 上失败并回滚到旧镜像。parity 比的是「有没有这个
字段」，不比「这个字段出现了几次」，所以这一族门一条都拦不住。

判的是清单里按名字区分的列表项：`env`、`envFrom`、`volumeMounts`、`ports`、
`containers`、`initContainers`、`volumes`、`imagePullSecrets`。判法按行读，不做
YAML 解析——chart 的模板里有 Go 模板语法，解析器进不去，而这些列表的形状在缩进上
写得清清楚楚：块的第一条列表项的缩进就是这一层的缩进，只有同缩进的条目属于同一层
（更深的是它们的子列表，例如 containers 里的 env）。

读的文件：`deploy/k3s/` 下的静态清单与 `deploy/helm/cogneva/templates/` 下的模板。
`deploy/rendered/` 按既有约定不读——它是渲染产物，同一句话在拥有它的模板里判。
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(os.environ.get("COGNEVA_ROOT") or Path(__file__).resolve().parents[2])

LIST_KEYS = (
    "env",
    "envFrom",
    "volumeMounts",
    "ports",
    "containers",
    "initContainers",
    "volumes",
    "imagePullSecrets",
)

SOURCES = (
    "deploy/k3s/*.yaml",
    "deploy/k3s/observability/manifests/*.yaml",
    "deploy/helm/cogneva/templates/*.yaml",
)


def files() -> list[Path]:
    found: list[Path] = []
    for pattern in SOURCES:
        found.extend(sorted(ROOT.glob(pattern)))
    return found


def duplicates_in(path: Path, lines: list[str]) -> list[str]:
    found: list[str] = []
    for index, line in enumerate(lines):
        for key in LIST_KEYS:
            match = re.match(r"^(\s*)" + key + r":\s*$", line)
            if not match:
                continue
            base = len(match.group(1))
            entries: list[tuple[str, int]] = []
            entry_indent: int | None = None
            cursor = index + 1
            while cursor < len(lines):
                body = lines[cursor]
                if not body.strip():
                    cursor += 1
                    continue
                indent = len(body) - len(body.lstrip())
                stripped = body.lstrip()
                if indent <= base and not stripped.startswith("#"):
                    break
                item = re.match(r"^(\s*)-\s+(.*)$", body)
                if item:
                    if entry_indent is None:
                        entry_indent = len(item.group(1))
                    if len(item.group(1)) == entry_indent:
                        name = re.search(r"\bname:\s*([^\s{]+)", item.group(2))
                        if name:
                            entries.append((name.group(1).strip("'\""), cursor + 1))
                cursor += 1
            seen: dict[str, int] = {}
            for name, line_no in entries:
                if name in seen:
                    where = path.relative_to(ROOT) if ROOT in path.parents else path
                    found.append(
                        f"{where}:{line_no}: `{key}` 里的 `{name}` "
                        f"与第 {seen[name]} 行重复"
                    )
                else:
                    seen[name] = line_no
    return found


def main(argv: list[str]) -> int:
    targets = [Path(a) for a in argv[1:]] or files()
    findings: list[str] = []
    for path in targets:
        if not path.is_absolute():
            path = ROOT / path
        if not path.exists():
            continue
        findings.extend(duplicates_in(path, path.read_text().splitlines()))
    if findings:
        for finding in findings:
            print(f"FAIL: {finding}", file=sys.stderr)
        print(
            "同一份清单里一个键出现两次：apply 会以 duplicate entries 拒绝整份，"
            "整轮滚动失败并回滚。删掉多余的一条（两份内容的差异要先合并）。",
            file=sys.stderr,
        )
        return 1
    print(f"MANIFEST DUPLICATE KEYS OK：{len(targets)} 份清单，没有重复键")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
