#!/usr/bin/env python3
# principle: feedback_a_projection_needs_a_named_projector_or_it_is_just_a_copy
"""判「判词脚本读的表，树里有没有东西写它」。

病：一张表被门读成判据，却没人写它。改一格就改判词，而树里没有一句话说那一格该
不该在。2026-11 把这张表所在的目录逐份普查过一遍，19 份各自有了去向；这张门把那一次
普查变成常设判据——新长出来的手写表不能悄悄成为判据。

身份不住在一张表里，住在代码动作的旁边。一张被读的表只有两种身份：

  写者       写这份文件的代码在自己的注释里写一行 `writes: <模式> generated`
             （Rust 用 `//`），正文里还要真的点到那份文件——通配行点到静态前缀就够，
             因为按目录整写的代码正文里只有目录。验的是「代码碰过它」，不是「有人
             说过它」：注解行本身不算正文。
  读者       只把文件读来**验**它的判词脚本写一行 `reads: <模式> subject`，正文里
             同样要点到它。chart 模板、values、渲染产物、静态清单、告警规则、夹具
             属于这一类：改它们会红在点了名的门上，所以它们不暗中决定判词。

手写的表不是第三种身份。一张手写表被判词读，在终态里它**不存在**：判据要么有写它的
代码，要么只被读来验它，要么它不该在树里。这里没有 `debt:`／`carrier:`／到期／基线
棘轮这种中间身份——中间身份就是「允许继续手写」的许可，许可可以续期，于是目标永远
到不了。要留的判据换成机器写或从源码推得出来，留不下的删掉；删不掉时门就是红的，
红在那里是终态契约在说「这张表不该存在」，不是一道可以用一行头注释换掉的关。

为什么不再要一张登记表：登记表是第二处必须与代码同说的东西，而它自己就是一张手写
判据。身份写在动作旁边，文件挪走、读者改行、写者换名时，注解跟着那一笔一起动或一起
变 stale；没有一处需要「记得同步」。

判的是**读法**，不是扩展名：tsv 是表，yaml 是表，交付配置与清单也是。所以数据面两种
读法分开：
  * `*.tsv *.pin *.csv` —— 按**文件名**认。脚本本来就是按名字找表的。
  * `*.yaml *.yml *.json` —— 按**相对路径**认。同名的那几类太多（`package.json`、
                          `cogneva.json` 各树都有），按名字认会把一份被读、其余跟着
                          过关；路径才是读者真正写下的那一份。

判词脚本＝这一族里能让提交变红的东西：`scripts/` 下的 `*.test.sh`、`gates/`、
`principles/` 与任意一层目录的测试，公开仓的 `deploy/scripts/*.sh|*.py` 与
`deploy/scripts/*/*.test.sh`，以及公开仓 `crates/*/tests/*.rs`。写者面是树里任何
shell／python／make 文件——写文件的代码就在那一面上。

写者优先于读者：一份机器写的表可以同时被门读来验（配额基线就是——渲染写它，parity
读它），那仍是 produced，读者的注解只说明它那次读是验不是取。两个写者匹配同一份表
也是红：一个生产者，不是一个先到先得。通配只在一段里
匹配（`*` 不跨 `/`），`**` 不支持（写出来是让人以为跨目录、实际不跨），绝对路径与
没有静态前缀的模式直接拒。注解匹配不到任何**被读**的数据文件就印 `stale`，不红——
清账是写者的事，而 stale 每跑一次印一次。

退出码：0 每张被读的表都有身份；1 有没身份／两个身份／链断；3 根不在、
一张数据文件都没有、一份判词脚本都没有——「量不出来」不是绿。

用法：check-judgment-inputs.py [--root PATH]
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

DATA_SUFFIXES = (".tsv", ".pin", ".csv", ".yaml", ".yml", ".json")
CODE_SUFFIXES = (".sh", ".py", ".bash", ".mk")
CODE_NAMES = {"Makefile", "GNUmakefile"}
PRUNE = {".git", "target", "node_modules", "__pycache__", "vendor"}

VERDICT = tuple(re.compile(p) for p in (
    r"^scripts/[^/]*\.test\.sh$",
    r"^scripts/[^/]+/[^/]*\.test\.sh$",
    r"^scripts/gates/[^/]*\.(?:sh|py)$",
    r"^scripts/principles/[^/]*\.(?:sh|py)$",
    r"^deploy/scripts/[^/]*\.sh$",
    r"^deploy/scripts/[^/]*\.py$",
    r"^deploy/scripts/[^/]+/[^/]*\.test\.sh$",
    r"^crates/[^/]+/tests/[^/]*\.rs$",
))

# 注解只认行首（列 0）：散文里缩进提到的同一串字不是声明，夹具里用 printf 拼出来的
# 也不是——它没有那一列。
ANNOT = re.compile(r"^(#|//)\s+(reads|writes):\s+(\S+)\s+(\S+)\s*$")
ANNOT_LOOSE = re.compile(r"^(#|//)\s+(reads|writes):")


class Decl:
    __slots__ = ("kind", "pattern", "rel", "rx", "needles", "body")

    def __init__(self, kind, pattern, rel, rx, needles, body):
        self.kind = kind          # writes | reads
        self.pattern = pattern
        self.rel = rel            # 声明它的代码文件
        self.rx = rx
        self.needles = needles
        self.body = body          # 摘掉注解行之后的正文


def is_data(rel):
    return rel.endswith(DATA_SUFFIXES)


def is_code(rel):
    return rel.endswith(CODE_SUFFIXES) or os.path.basename(rel) in CODE_NAMES


def is_verdict(rel):
    return any(rx.match(rel) for rx in VERDICT)


def key_of(rel):
    return os.path.basename(rel) if rel.endswith((".tsv", ".pin", ".csv")) else rel


def compile_glob(pattern):
    """`*` 只在一段里匹配；`?` 一个字符；其余按字面。返回 (regex, 错误)。"""
    if "**" in pattern:
        return None, "通配写成了 **（只支持一段里的 *）"
    if pattern.startswith("/"):
        return None, "是绝对路径（模式相对扫描根写）"
    if not pattern:
        return None, "是空的"
    if not pattern.split("*")[0]:
        return None, "没有静态前缀（通配要从一个具体的名字开始，否则整棵树一个身份）"
    out = []
    for ch in pattern:
        if ch == "*":
            out.append("[^/]*")
        elif ch == "?":
            out.append("[^/]")
        elif ch in ".+()[]{}^$|\\":
            out.append("\\" + ch)
        else:
            out.append(ch)
    return re.compile("^" + "".join(out) + "$"), None


def needles(pattern):
    """正文要点到的字符串：通配行点静态前缀（前缀停在文件名中间时，文件名那一段也算）。"""
    if "*" not in pattern:
        if pattern.endswith((".tsv", ".pin", ".csv")):
            return [os.path.basename(pattern)]
        return [pattern]
    prefix = pattern.split("*")[0]
    out = [prefix]
    last = prefix.rsplit("/", 1)[-1]
    if last and last != prefix and len(last) >= 3:
        out.append(last)
    return out


def body_without_annotations(text):
    return "\n".join(line for line in text.splitlines() if not ANNOT_LOOSE.match(line))


def parse_annotations(rel, text, fail):
    marker = "//" if rel.endswith(".rs") else "#"
    body = body_without_annotations(text)
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        m = ANNOT_LOOSE.match(line)
        if not m or m.group(1) != marker:
            continue
        m2 = ANNOT.match(line)
        if not m2:
            fail(f"注解写不成：{rel}:{n}——要 writes: <模式> generated 或 reads: <模式> subject")
            continue
        kind, pattern, cls = m2.group(2), m2.group(3), m2.group(4)
        want = "generated" if kind == "writes" else "subject"
        if cls != want:
            fail(f"注解的类不对：{rel}:{n}——{kind} 只有 {want}（读到的是 {cls}）")
            continue
        if kind == "reads" and not is_verdict(rel):
            fail(f"reads 注解只写在判词脚本里：{rel}:{n}——取判据的读者不能自己认领")
            continue
        rx, why = compile_glob(pattern)
        if rx is None:
            fail(f"注解的模式不合法：{rel}:{n} 的 {pattern} {why}")
            continue
        out.append(Decl(kind, pattern, rel, rx, needles(pattern), body))
    return out


def git(root, *args, data=None):
    return subprocess.run(["git", "-C", root, *args], input=data, capture_output=True)


def read_blobs(root, shas):
    if not shas:
        return {}
    proc = git(root, "cat-file", "--batch", data=("\n".join(shas) + "\n").encode())
    out = proc.stdout
    blobs = {}
    i = 0
    for _ in shas:
        nl = out.find(b"\n", i)
        if nl < 0:
            break
        header = out[i:nl].split()
        i = nl + 1
        if len(header) != 3:
            break
        sha, size = header[0].decode(), int(header[2])
        blobs[sha] = out[i:i + size]
        i += size + 1
    return blobs


def load_tree(root):
    """这一笔要判的是哪些路径、各自的字节。

    提交车道给的是私有索引（`GIT_INDEX_FILE` 指向这一笔的字节），那时名单和内容都
    从索引读——工作树里此刻躺着隔壁会话还没落的编辑，判它等于替别人判旧账。没有索引
    时（夹具、tarball、手跑）才扫盘，那里文件本身就是答案。
    """
    if os.environ.get("GIT_INDEX_FILE") and git(root, "rev-parse", "--git-dir").returncode == 0:
        listing = git(root, "ls-files", "-s", "-z").stdout
        entries = []
        for rec in listing.split(b"\0"):
            if not rec:
                continue
            meta, _, path = rec.partition(b"\t")
            parts = meta.split()
            if not path or len(parts) < 3 or parts[2] != b"0":
                continue
            entries.append((parts[1].decode(), path.decode("utf-8", "replace")))
        paths = sorted(rel for _, rel in entries)
        need = sorted({sha for sha, rel in entries
                       if is_data(rel) or is_code(rel) or is_verdict(rel)})
        blobs = read_blobs(root, need)
        contents = {rel: blobs[sha] for sha, rel in entries if sha in blobs}
        return paths, contents
    paths = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in PRUNE)
        for fn in filenames:
            paths.append(os.path.relpath(os.path.join(dirpath, fn), root).replace(os.sep, "/"))
    paths.sort()
    contents = {}
    for rel in paths:
        if is_data(rel) or is_code(rel) or is_verdict(rel):
            try:
                with open(os.path.join(root, rel), "rb") as fh:
                    contents[rel] = fh.read()
            except OSError:
                contents[rel] = b""
    return paths, contents


def parse_args(argv):
    root = None
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--root":
            if i + 1 >= len(argv):
                print("exit 2：--root 要给一个路径", file=sys.stderr)
                raise SystemExit(2)
            root = argv[i + 1]
            i += 2
        elif arg in ("-h", "--help"):
            print("usage: check-judgment-inputs.py [--root PATH]")
            raise SystemExit(0)
        else:
            print(f"exit 2：未知参数 {arg}", file=sys.stderr)
            raise SystemExit(2)
    return root


def main(argv):
    root = os.path.abspath(
        parse_args(argv)
        or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    if not os.path.isdir(root):
        print(f"exit 3：根不在：{root}", file=sys.stderr)
        return 3

    paths, contents = load_tree(root)
    data = [p for p in paths if is_data(p)]
    verdicts = [p for p in paths if is_verdict(p)]
    if not data:
        print(f"exit 3：{root} 下一张数据文件都没扫到——这不是绿，是量法失效", file=sys.stderr)
        return 3
    if not verdicts:
        print(f"exit 3：{root} 下一份判词脚本都没找到——这不是绿，是量法失效", file=sys.stderr)
        return 3

    fails = []

    def fail(msg):
        fails.append(msg)

    def text_of(rel):
        return contents.get(rel, b"").decode("utf-8", "replace")

    verdict_text = {p: text_of(p) for p in verdicts}
    readers = {}
    for rel in data:
        key = key_of(rel)
        readers[rel] = [v for v in verdicts if key in verdict_text[v]]

    decls = []
    for rel in sorted({p for p in paths if is_code(p) or is_verdict(p)}):
        decls.extend(parse_annotations(rel, text_of(rel), fail))
    seen = set()
    unique = []
    for d in decls:
        k = (d.kind, d.pattern, d.rel)
        if k in seen:
            continue
        seen.add(k)
        unique.append(d)
    decls = unique

    # 注解只证明说过，不证明碰过：写它的那份代码的正文（注解行之外）必须点到模式。
    for d in decls:
        if not any(n in d.body for n in d.needles):
            fail(f"{d.kind} 的注解正文没有点到 {d.pattern}：{d.rel}——"
                 f"注解要落在真的碰这份文件的代码旁边")

    writes = [d for d in decls if d.kind == "writes"]
    reads = [d for d in decls if d.kind == "reads"]

    total = read_n = unread_n = 0
    generated_n = subject_n = 0

    for rel in data:
        total += 1
        if not readers[rel]:
            unread_n += 1
            continue
        read_n += 1

        matched_w = [d for d in writes if d.rx.match(rel)]
        matched_r = [d for d in reads if d.rx.match(rel)]

        # 写者优先：机器写的表被门读来验是常事，产出的身份由写者认领。
        kind = "generated" if matched_w else "subject" if matched_r else ""
        if not kind:
            fail(f"没有身份：{rel} 被判词脚本读，而树里没有写它的代码、也没有只把它读来验的"
                 f"判词脚本——手写的表不该成为判据")
            continue

        if kind == "generated":
            if len(matched_w) > 1:
                fail(f"两个写者匹配同一份表：{rel}"
                     f"（{', '.join(d.rel for d in matched_w)}）——一个生产者")
                continue
            generated_n += 1
        else:
            subject_n += 1

    stale = []
    for d in decls:
        hit = [rel for rel in data if d.rx.match(rel)]
        if not any(readers[rel] for rel in hit):
            stale.append(f"stale {d.kind} {d.pattern}（{d.rel}；没有一张被读的表匹配它）")
    for line in sorted(stale):
        print(line)

    print(f"judgment-inputs: {total} 张数据文件，{read_n} 张被判词脚本读"
          f"（generated {generated_n}，subject {subject_n}），"
          f"{unread_n} 张没人读，stale {len(stale)}；声明：writes {len(writes)}、reads {len(reads)}")
    if fails:
        for msg in fails:
            print(f"FAIL  {msg}", file=sys.stderr)
        print(f"judgment-inputs: {len(fails)} 条红——被门读的表要有写它的代码，"
              f"或只被读来验它的判词脚本；手写的表没有第三种身份", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
