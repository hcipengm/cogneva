#!/usr/bin/env bash
# 贡献榜与生产端之间那份「标记格式」的判据。
#
# bot_board.py 不调用 Rust：它用正则去读交叉验证评论里的机器可读标记。这些标记
# 的生产端在 `crates/cog-github/` —— `<!-- cogneva-bot-meta -->`、`<!-- cogneva-cv:
# pr=… head=… bot=… -->`、`<!-- cogneva-eval: {json} -->`、`**Verdict: PASS**`，以及
# eval 载荷里那几个缩写字段名（rb/ra/nb/na/lb/la）。同一份词汇写两遍，中间没有任何
# 东西比较它们：生产端改一个空格、换一个字段名，Python 侧一条都匹配不上——而空榜
# 与「今天还没有 bot PR」在读数上一模一样，谁都不会红。
#
# 所以这里不比对字符串，而是**让生产端的字面量真的走一遍消费端**：从 Rust 源码里
# 取出它写评论用的格式串，按格式串填好样例值，再交给 bot_board.py 的正则与解析函数
# 读。要么读得出人，要么这条测试红。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
board="${repo}/scripts/bot_board.py"
producer_dir="${repo}/crates/cog-github/src"

[ -f "$board" ] || { echo "FAIL: 没有 $board" >&2; exit 1; }
[ -d "$producer_dir" ] || { echo "FAIL: 没有生产端源码目录 $producer_dir" >&2; exit 1; }

python3 -B - "$board" "$producer_dir" <<'PY'
import importlib.util
import json
import pathlib
import re
import sys

board_path, producer_dir = sys.argv[1], pathlib.Path(sys.argv[2])

spec = importlib.util.spec_from_file_location("bot_board", board_path)
board = importlib.util.module_from_spec(spec)
spec.loader.exec_module(board)

sources = {p.name: p.read_text() for p in sorted(producer_dir.glob("*.rs"))}
corpus = "\n".join(sources.values())

failures = []


def check(name, ok, detail=""):
    if ok:
        print(f"ok    - {name}")
    else:
        failures.append(name)
        print(f"FAIL  - {name}{('：' + detail) if detail else ''}")


def literal_containing(fragment, where=""):
    """→ 生产端源码里包含 fragment 的那个字符串字面量（去引号）。"""
    for name, text in sources.items():
        for match in re.finditer(r'"((?:[^"\\]|\\[\s\S])*)"', text):
            if fragment in match.group(1):
                return match.group(1), name
    raise SystemExit(f"在 {producer_dir} 里找不到含 {fragment!r} 的字面量{where}")


# 1. bot-meta 块：PR 正文里的这个标记两端各有一个解析者——Rust 认它来认领自己的
#    PR，榜认它来取 `bot:` 那一行。两边认的若不是同一个名字，一边读得到、另一边
#    读不到，而「读不到」在榜上就是「今天没有 bot PR」。
meta_token = (
    board.META_MARKER.strip().removeprefix("<!--").removesuffix("-->").strip()
)
meta = board.parse_meta(f"{board.META_MARKER}\nbot: alice\nenv: prod\n")
check(
    "bot-meta 标记：榜认的名字在 Rust 侧也认，且 parse_meta 读得出 bot",
    meta_token in corpus and isinstance(meta, dict) and meta.get("bot") == "alice",
    f"榜认 {board.META_MARKER!r}（关键字 {meta_token!r} 在 Rust 侧"
    f"{'有' if meta_token in corpus else '没有'}），parse_meta 读回 {meta!r}",
)

# 2. 交叉验证标记：pr / head / bot 三个捕获组必须都读得到。
cv_literal, src = literal_containing("<!-- cogneva-cv:")
cv_body = cv_literal
for placeholder, value in (("{pr}", "42"), ("{sha8}", "deadbeef"), ("{handle}", "bob")):
    cv_body = cv_body.replace(placeholder, value)
cv_match = board.CV_MARKER_RE.search(cv_body)
check(
    "cv 标记：生产端的格式串能被 CV_MARKER_RE 读出 pr / head / bot",
    cv_match is not None and cv_match.groups() == ("42", "deadbeef", "bob"),
    f"生产端 {src} 写 {cv_literal!r}，读回 "
    f"{cv_match.groups() if cv_match else None}",
)

# 3. 判词：榜按 `**Verdict: PASS**` 计数，生产端写的是 `**Verdict: {}**`。
verdict_literal, src = literal_containing("**Verdict:")
verdict_body = verdict_literal.replace("{}", "PASS")
passes, fails, _ = board.tally_verdicts(
    [{"body": (_ := cv_body) + "\n\n" + verdict_body}]
)
check(
    "判词：生产端的写法能被 tally_verdicts 记成一次 PASS",
    (passes, fails) == (1, 0),
    f"生产端 {src} 写 {verdict_literal!r}，榜记成 {passes} pass / {fails} fail",
)

# 3b. 标签：榜在建跟踪 issue 与建标签时用的是同一个名字，而这个名字也是 Rust
#     侧标 bot PR 用的那个。两边分家时，榜给 issue 打的标签不是 bot PR 的那个，
#     而「按标签找 bot PR」的地方（榜之外的面）从此一条都找不到。
label_match = re.search(
    r'CROSS_VALIDATION_LABEL[^=]*=\s*"([^"]+)"', corpus
)
rust_label = label_match.group(1) if label_match else None
check(
    "标签：榜的 bot 标签与 Rust 侧标 bot PR 的那个同名",
    rust_label is not None and board.BOT_LABEL == rust_label,
    f"榜用 {board.BOT_LABEL!r}，生产端 {rust_label!r}",
)

# 4. eval 载荷：字段名是生产端 serde 的缩写，榜直接按这些名字取数。
#    名字从 Rust 结构体里读：`#[serde(rename = "rb")]` 用缩写的就用缩写，
#    没写的用字段名。榜单侧用到哪几个，就判哪几个真的在里面。
renames = set(re.findall(r'rename\s*=\s*"([a-z_]+)"', corpus))
plain = set(re.findall(r"pub\s+(\w+)\s*:\s*[A-Za-z]", corpus))
declared = renames | plain
used_by_board = {"applicable", "rb", "ra", "lb", "la", "nb", "na", "significant"}
missing = used_by_board - declared
check(
    "eval 载荷：榜读的字段名都在生产端结构体里声明过",
    not missing,
    f"榜读 {sorted(missing)}，生产端只声明了 {sorted(declared & used_by_board)}",
)

# 走一遍真实链路：用生产端声明的缩写名拼一份载荷，榜要能算出 +20 个百分点。
payload = {"applicable": True, "rb": 0.4, "ra": 0.6, "nb": 10, "na": 10}
marker_prefix, src = literal_containing("<!-- cogneva-eval:")
eval_body = f"{marker_prefix} {json.dumps(payload)} -->"
evals = board.collect_evals([{"body": (_ := cv_body) + "\n" + eval_body, "user": {"login": "bob"}}])
gain = board.improvement_pp(evals[0]["metrics"]) if evals else None
check(
    "eval 载荷：生产端的标记形状能让榜读出改进量（+20pp）",
    len(evals) == 1 and gain is not None and abs(gain - 20.0) < 1e-9,
    f"读回 {evals!r}，算出 {gain!r}",
)

if failures:
    print(f"\n失败 {len(failures)} 项：{', '.join(failures)}", file=sys.stderr)
    sys.exit(1)
print("\nbot-board-markers.test: 全部通过")
PY
