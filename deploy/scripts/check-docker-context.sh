#!/usr/bin/env bash
# 镜像构建上下文里必须有每一个编译期内嵌的文件。
#
# 这条失败是静默的，而且只在 tag 发版那条路上现形：`.dockerignore` 把 `deploy/*`
# 排除在上下文之外，而 `crates/` 里有几处 `include_str!` 指向 `deploy/` 下的文件。
# 住在被排除目录里的文件在开发树里永远存在，本机发布（`build-release-image.sh`
# 用整棵树）也永远编得过；只有从构建上下文里编才会失败——而那条路
# （`release.yml` 的镜像 job）只在 tag 上跑。2026-10-19 实测：
# `deploy/helm/cogneva/files/cogneva.json` 自 2026-09-25 起被 cog-observability
# 用 `include_str!` 编进二进制，镜像路径因此红了一个多月，期间没有一次普通推送
# 会碰到它——错的不是那一行 COPY，是「谁该在上下文里」这件事没有判据。
#
# 判据两条，缺一不可：
#   1. 文件在上下文里：不匹配 `.dockerignore` 里任何一条排除（否定的那条也一样，
#      而且 Docker **不允许**在父目录被排除之后重新包含子文件，所以逐层目录都要判）；
#   2. `Dockerfile` 里有一条 `COPY` 把它取进构建阶段（整目录 COPY 也算）。
#
# 对象是 `crates/*/src/**.rs` 与 `crates/*/build.rs` 里的 `include_str!` /
# `include_bytes!`，只判**外部**路径（落在该 crate 目录之外的）：落回 crate 内的
# 由 `COPY crates/` 覆盖，重复判只会多一条会漂的口径。测试模块里的内嵌也算——
# 它们不随 `--bin cogneva` 编译，所以 `ALLOWED` 用一条带理由的名单把那几个豁免掉；
# 名单外的写法都会红，这正是要的：新加一个内嵌就要想一次「它在不在镜像路上」。
#
# 为什么不用 `#[cfg(test)]` 自动分类：这里只做文本扫描，而「哪个内嵌在测试模块里」
# 要靠条件编译的真实归属来判——属性可以写在模块上、可以走 `cfg_attr`、可以套在
# `mod tests { ... }` 之外，文本层面的括号配对认不全。自动分类会静默误判成
# 「是测试所以跳过」，那正是这条门最该红的那一格；所以宁可漏判成红、由人写进
# `ALLOWED` 并给理由，也不猜。
#
# 用法：bash deploy/scripts/check-docker-context.sh [仓库根]
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="${1:-$(cd "${here}/../.." && pwd)}"

exec python3 - "${repo}" <<'PY'
import fnmatch
import re
import sys
from pathlib import Path

repo = Path(sys.argv[1]).resolve()

# 不随 `--bin cogneva` 编译的内嵌，逐条写理由。加一条等于声明「这个文件不需要
# 进镜像上下文」——它不会被编译器在这个方向上验证，所以理由必须自己站得住。
ALLOWED = {
    # bootstrap 是**另一个二进制**（Dockerfile 只 `cargo build --bin cogneva`），
    # 它内嵌的引导资产不随镜像编译。
    "deploy/k8s/image-distributor.yaml": "bootstrap 二进制内嵌，镜像只构建 --bin cogneva",
}


def read_dockerignore(path: Path) -> list[str]:
    patterns = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return patterns


def matches(pattern: str, path: str) -> bool:
    p = pattern.lstrip("/").rstrip("/")
    return fnmatch.fnmatchcase(path, p)


def in_context(patterns: list[str], path: str) -> bool:
    """Docker 的忽略语义：逐层目录判，某层目录被排除 ⇒ 整棵子树都不在上下文里，
    后面的否定条目也救不回来（这正是 `.dockerignore` 不能简单写 `deploy/*` +
    `!deploy/helm/.../cogneva.json` 的原因）。"""
    parts = path.split("/")
    for i in range(1, len(parts) + 1):
        prefix = "/".join(parts[:i])
        decision = None
        for pattern in patterns:
            negated = pattern.startswith("!")
            body = pattern[1:] if negated else pattern
            if matches(body, prefix):
                decision = negated
        if decision is False:
            return False
    return True


def copied_sources(dockerfile: Path) -> list[str]:
    """Dockerfile 里从构建上下文取的 COPY 源（`COPY --from=...` 是阶段间拷贝，
    与上下文无关）。"""
    sources = []
    for raw in dockerfile.read_text().splitlines():
        line = raw.strip()
        if not line.upper().startswith("COPY "):
            continue
        tokens = line.split()[1:]
        if any(t.startswith("--from=") for t in tokens):
            continue
        # 去掉 flag（--chown=... 等）；JSON 形式（["src","dest"]）本仓库没用，跳过。
        tokens = [t for t in tokens if not t.startswith("--")]
        if len(tokens) < 2 or tokens[0].startswith("["):
            continue
        sources.extend(tokens[:-1])
    return sources


def covers(sources: list[str], path: str) -> bool:
    for source in sources:
        s = source.lstrip("./").rstrip("/")
        if not s:
            continue
        if path == s or path.startswith(s + "/"):
            return True
    return False


INCLUDE = re.compile(r'include_(?:str|bytes)!\("([^"]+)"\)')


def include_targets() -> tuple[list[tuple[Path, str, str]], int, int]:
    """返回 (外部内嵌路径, 扫过的文件数, 读到的内嵌条数)。

    树外的解析结果**不跳过**：符号链接或 `../../` 越过仓库根的内嵌必然不在构建
    上下文里，而它在这里正是最该红的一格——跳过它，门就在一个确实会坏的文件上
    变绿，且没有任何别的门在看 include_str!。
    """
    targets = []
    scanned = 0
    found = 0
    files = list(repo.glob("crates/*/src/**/*.rs")) + list(repo.glob("crates/*/build.rs"))
    for rs in files:
        scanned += 1
        try:
            text = rs.read_text()
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            for rel in INCLUDE.findall(line):
                found += 1
                absolute = (rs.parent / rel).resolve()
                located = f"{rs.relative_to(repo).as_posix()}:{lineno}"
                try:
                    repo_rel = absolute.relative_to(repo).as_posix()
                except ValueError:
                    targets.append((rs, located, f"<树外> {absolute}"))
                    continue
                targets.append((rs, located, repo_rel))
    return targets, scanned, found


def main() -> int:
    patterns = read_dockerignore(repo / ".dockerignore")
    sources = copied_sources(repo / "Dockerfile")
    targets, scanned, found = include_targets()
    # 扫描面是空的不是绿的：这一门的全部意义就是替人去看编译期依赖，而它自己
    # 什么都看不到的时候，与「看了、都合格」在退出码上同形。`crates/*/src/**/*.rs`
    # 的布局一旦变了，这里必须红一次，让人来改门的扫法。
    print(f"check-docker-context: scanned_files={scanned} includes={found}")
    if scanned == 0 or found == 0:
        print(
            "check-docker-context: 扫描面是空的（一个文件或一条内嵌都没读到），"
            "这不是通过——门的扫法要跟着树改：检查 crates/*/src 下是否还有源码。",
            file=sys.stderr,
        )
        return 1

    bad = []
    for rs, where, target in targets:
        crate = rs.relative_to(repo).parts[1]  # crates/<crate>/...
        if target.startswith(f"crates/{crate}/") or target in ALLOWED:
            continue
        if target.startswith("<树外> "):
            bad.append(f"{where}: {target[5:]} 解析到仓库树之外，进不了构建上下文")
        elif not in_context(patterns, target):
            bad.append(f"{where}: {target} 被 .dockerignore 排除在构建上下文之外")
        elif not covers(sources, target):
            bad.append(f"{where}: {target} 在上下文里，但 Dockerfile 没有 COPY 它")

    if bad:
        print("check-docker-context: 镜像构建上下文缺编译期内嵌的文件：", file=sys.stderr)
        for line in bad:
            print(f"  {line}", file=sys.stderr)
        print(
            "改法：在 .dockerignore 里逐层放行目录与文件（父目录被排除时否定子文件无效），"
            "并在 Dockerfile 里补一条 COPY；若是测试模块或另一个二进制，写进本脚本的 ALLOWED 并给理由。",
            file=sys.stderr,
        )
        return 1
    return 0


sys.exit(main())
PY
