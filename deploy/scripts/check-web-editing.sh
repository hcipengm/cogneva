#!/usr/bin/env bash
# 编辑器「打字行为」的门禁：回车之后的缩进、括号配对、跳到第几行。
#
# 这三件事编得过、跑起来也「成功」，错的只是结果：缩进少一格、多出半个括号、
# 光标跳完又弹回末尾。屏上看到的永远是「编辑器有点怪」，看不出是哪一层——所以
# 判据只能落在纯函数与接线上，而且必须真跑一遍。
#
# 判据：
#   1. `editing.ts` 的三个函数按形状给出对齐与缩进（编译成 mjs 在 node 里断言）；
#   2. `CodeEditor` 接管回车与括号，并把光标落回去（`pendingCaret`）；
#   3. `CodeEditor` 消费 `goto`，`FilesView` 用 ⌘G 开出输入框并把它传进去。
#
# 用法：bash deploy/scripts/check-web-editing.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
EDIT = os.path.join(WEB, "files/editing.ts")
EDITOR = os.path.join(WEB, "files/CodeEditor.tsx")
VIEW = os.path.join(WEB, "views/FilesView.tsx")
for path in (EDIT, EDITOR, VIEW):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {path}")

editor = open(EDITOR, encoding="utf-8").read()
view = open(VIEW, encoding="utf-8").read()

# ── 2. 编辑器接管了按键，并把光标落回去 ──
if not re.search(r"from\s+['\"]@/files/editing['\"]", editor):
    sys.exit("CodeEditor.tsx 没有引用 editing.ts：缩进与配对没有任何调用点")
for name in ("onEnter", "onBracket"):
    if not re.search(rf"\b{name}\(text,", editor):
        sys.exit(f"CodeEditor.tsx 没有调用 {name}(text, …)：这一层仍然由浏览器默认行为接管")
if not re.search(r"onEnter\(text,\s*area\.selectionStart", editor):
    sys.exit("CodeEditor.tsx 调 onEnter 时没有把光标位置传进去：缩进会按第 0 列算")
# 按形状判「光标真的被落回去了」：手里记着位置、却在写回之前就返回，屏上看到的
# 与「完全没接管」一模一样——只查这个名字出现过没有，那种改动照样绿。
if not re.search(r"pendingCaret\.current\s*=\s*edit\.caret;", editor):
    sys.exit("CodeEditor.tsx 接管按键后没有记下光标位置")
if not re.search(r"if\s*\(!area\s*\|\|\s*pendingCaret\.current === null\)\s*return;", editor):
    sys.exit("CodeEditor.tsx 落光标的那个 effect 少了「没有待落位置就返回」这一支")
if not re.search(r"area\.selectionStart\s*=\s*at;", editor):
    sys.exit(
        "CodeEditor.tsx 没有把光标落回去：受控 textarea 重渲染会把光标送回末尾，"
        "连打两个括号就会插在同一个位置"
    )

# ── 3. 跳到第几行：编辑器消费 goto，文件页用 ⌘G 开出来 ──
if not re.search(r"offsetOfLine\(text,\s*goto\.line\)", editor):
    sys.exit("CodeEditor.tsx 没有用 offsetOfLine 把行号换成下标")
if not re.search(r"\{\s*goto\s*\}", view):
    sys.exit("FilesView.tsx 没有把 goto 传给 CodeEditor：输入了行号也不会有人跳")
if not re.search(r"key\.toLowerCase\(\)\s*===\s*['\"]g['\"]", view):
    sys.exit("FilesView.tsx 没有 ⌘G 的按键处理：这一格只能靠点按钮打开")
print("ok：编辑器接管回车与括号并落光标，⌘G 的行号跳转接到了编辑器")
PYEOF

TSC="$ROOT/web/node_modules/.bin/tsc"
if [ ! -x "$TSC" ]; then
  echo "缺少依赖：$TSC（先装 web 依赖）" >&2
  exit 2
fi
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
"$TSC" "$ROOT/web/src/files/editing.ts" --target es2020 --module es2020 \
  --outDir "$TMP" --skipLibCheck >/dev/null
mv "$TMP/editing.js" "$TMP/editing.mjs"

NODE="$(command -v node || true)"
if [ -z "$NODE" ]; then
  echo "缺少依赖：node" >&2
  exit 2
fi
"$NODE" --input-type=module - "$TMP/editing.mjs" <<'JSEOF'
const { onEnter, onBracket, offsetOfLine } = await import('file://' + process.argv[2]);

let failures = 0;
const check = (name, got, want) => {
  const a = JSON.stringify(got);
  const b = JSON.stringify(want);
  if (a !== b) {
    console.error(`${name}: 得到 ${a}，期望 ${b}`);
    failures += 1;
  }
};

// 回车：沿用上一行的缩进。
check(
  '沿用缩进',
  onEnter('  foo', 5),
  { text: '  foo\n  ', caret: 8 }
);

// 回车：上一行以 `{` 收尾，新行多缩进一格。
check(
  '开括号后缩进',
  onEnter('  if (x) {', 10),
  { text: '  if (x) {\n    ', caret: 15 }
);

// 回车：光标正好在一对花括号中间，先开一行、再补一行收尾。
check(
  '括号之间回车',
  onEnter('  {}', 3),
  { text: '  {\n    \n  }', caret: 8 }
);

// 回车：YAML / Python 那种以冒号收尾的行也要缩进。
check(
  '冒号后缩进',
  onEnter('key:', 4),
  { text: 'key:\n  ', caret: 7 }
);

// 回车：顶格的行不该凭空多出缩进。
check('顶格回车', onEnter('foo', 3), { text: 'foo\n', caret: 4 });

// 左括号：补上另一半，光标停在中间。
check('补右括号', onBracket('a', 1, 1, '('), { text: 'a()', caret: 2 });

// 右括号：右边已经有一个，跨过去而不是再补一个。
check('跨过右括号', onBracket('a()', 2, 2, ')'), { text: 'a()', caret: 3 });

// 引号：同一种身份，右边已有一个时也跨过去。
check('跨过引号', onBracket('""', 1, 1, '"'), { text: '""', caret: 2 });

// 引号：右边是别的字符时不补——补了会把引号成对塞进不该成对的地方。
check('引号后接字词不补', onBracket('"a"', 1, 1, '"'), null);

// 右括号紧挨字词：不在字词前面补括号。
check('字词前不补', onBracket('(x', 1, 1, '('), null);

// 有选区：交给默认行为（整段替换），不接管。
check('有选区不接管', onBracket('hello', 0, 5, '('), null);

// 行号换下标：三行里第二行从下标 2 开始。
check('第二行行首', offsetOfLine('a\nbb\nccc', 2), 2);
check('第三行行首', offsetOfLine('a\nbb\nccc', 3), 5);
// 越界夹到首行 / 末行。
check('行号 0 落在第一行', offsetOfLine('a\nbb', 0), 0);
check('超出总行数落在最后一行', offsetOfLine('a\nbb\nccc', 99), 5);

if (failures > 0) process.exit(1);
console.log('EDITING OK');
JSEOF

echo "WEB EDITING OK"
