#!/usr/bin/env bash
# 文件内替换的语义门禁。
#
# 替换这件事看着简单，坏起来全是**静默改错**：多改一处、少改一处、把大小写也
# 一起改写掉、从前往后替换导致第二处起错位。这些错误都不会让任何东西变红，只
# 会让人在保存之后发现文件被改坏了——而那时代码已经进了工作区。
#
# 所以这一条不是问「按钮在不在」，是把 `replaceMatches` 当成一个函数来验：把它
# 单文件编译出来，在 node 里按判据跑。函数没有依赖，能单文件编译，这本身就是
# 它保持纯净的证明——哪天它开始 import 别的东西，这条门会先红。
#
# 判据：
#   1. 全部替换：三处命中全换掉，其余字节一字不动；
#   2. 只替换一处：只动下标指定的那一处，别的命中留在原地；
#   3. 大小写：搜索不敏感、替换照原文区间写，原文里的大小写不许被替换文本的
#      大小写带跑（`Foo` 命中 `foo` 的查询，替换成 `bar` 后是 `bar`，而不是把
#      别处的 `FOO` 一起改成 `bar`）；
#   4. 从后往前拼：连续命中的第二处不许错位（`aa` 里把 `a` 换成 `bb`）；
#   5. 没有命中：原样返回，`replaced = 0`——调用方靠这个数说「一处都没替换」。
#
# 用法：bash deploy/scripts/check-web-replace-semantics.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v node >/dev/null || { echo "缺少依赖：node" >&2; exit 2; }
SRC="web/src/files/highlight.ts"
[ -f "$SRC" ] || { echo "读不到 $SRC" >&2; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# 单文件编译：这一步同时验证了「这个函数不依赖别的东西」。
#
# 用仓库里装好的那一份 tsc，不用 `npx tsc`：npm 上另有一个同名包，装不上时
# npx 会去下载它，拿到的是另一个东西，报错还长得像「编译不过」。
TSC="$ROOT/web/node_modules/.bin/tsc"
[ -x "$TSC" ] || {
  echo "缺少 web/node_modules/.bin/tsc——先在 web/ 里装依赖（npm ci）" >&2
  exit 2
}
"$TSC" "$SRC" --target es2020 --module es2020 --outDir "$TMP" >/dev/null 2>&1 || {
  echo "$SRC 编译不过：替换的判据无从检查" >&2
  exit 1
}
mv "$TMP/highlight.js" "$TMP/highlight.mjs"

node --input-type=module - "$TMP/highlight.mjs" <<'JS'
const mod = await import(process.argv[2]);
const { replaceMatches } = mod;
const fail = (msg) => {
  console.error(`替换语义不对：${msg}`);
  process.exit(1);
};

// 1. 全部替换
let out = replaceMatches('a b a b a', 'a', 'X');
if (out.text !== 'X b X b X') fail(`全部替换得到 ${JSON.stringify(out.text)}`);
if (out.replaced !== 3) fail(`全部替换应报 3 处，实际 ${out.replaced}`);

// 2. 只替换一处
out = replaceMatches('a b a', 'a', 'X', 1);
if (out.text !== 'a b X') fail(`只替换下标 1 得到 ${JSON.stringify(out.text)}`);
if (out.replaced !== 1) fail('只替换一处应报 1 处');

// 3. 大小写：搜索不敏感，但只动真正命中的区间
out = replaceMatches('Foo foo FOO', 'foo', 'bar');
if (out.text !== 'bar bar bar') fail(`大小写不敏感替换得到 ${JSON.stringify(out.text)}`);

// 4. 连续命中不许错位
out = replaceMatches('aa', 'a', 'bb');
if (out.text !== 'bbbb') fail(`连续命中得到 ${JSON.stringify(out.text)}（从前往后拼就会错位成 bba）`);

// 5. 没有命中
out = replaceMatches('abc', 'zzz', 'x');
if (out.text !== 'abc' || out.replaced !== 0) fail('没有命中时必须原样返回且报 0 处');

// 6. 替换文本比原命中更长/更短，其余字节不受影响
out = replaceMatches('key = 1; key = 2;', 'key', 'longer_name');
if (out.text !== 'longer_name = 1; longer_name = 2;') fail(`长度变化后正文被改坏：${JSON.stringify(out.text)}`);

console.log('replace semantics ok');
JS

# ── 视图这一侧：替换必须落在草稿上，而不是直接写盘 ──
python3 - "$ROOT" <<'PYEOF'
import os, re, sys
ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
view = "views/FilesView.tsx"
path = os.path.join(WEB, view)
src = open(path, encoding="utf-8").read()
if "replaceMatches(" not in src:
    sys.exit(f"{view} 没有调用 replaceMatches：替换框在，替换不在")
start = src.find("replaceMatches(")
end = src.find("\n  };", start)
body = src[start : end if end > 0 else len(src)]
if "setDraft(" not in body:
    sys.exit(f"{view}: 替换的结果没有写进草稿——替换完既看不见也存不上")
if re.search(r"saveWorkspaceFile\s*\(", body):
    sys.exit(f"{view}: 替换直接写了盘——一次全部替换能改掉几十处，读者没有一次整份放弃的机会")
if "replace(true)" not in src or "replace(false)" not in src:
    sys.exit(f"{view}: 替换一处与全部替换没有各接一个入口")
print("view wiring ok")
PYEOF

echo "REPLACE OK：语义（全部/一处/大小写/连续命中/无命中）与落地（写草稿不写盘、两个入口）都在"
