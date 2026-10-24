#!/usr/bin/env bash
# 「源代码管理」这一面的消费者门禁。
#
# 成品不是 `/api/v1/ide/git/status` 这个路由，是**有人打开那一屏、看见改了哪些
# 文件、点开一份并排的差异、把它提交掉**。这里挡的是几种不出声的坏形状：
#   1. 视图文件在树上，但活动栏 / 页标题 / 注册表里没有它——从界面上打不开；
#   2. 取数函数没人调，或调了却不带筛选：差异页永远画同一个文件；
#   3. 差异页把两栏换成一条流水账，或者对齐算法把删除与新增错位；
#   4. 网关收下了路径却不当成路径用：出根的东西交给 git 自己判断；
#   5. git 起来的时候钩子还开着——仓库里的 `.git` 一旦可写，「保存一个文件」
#      就能变成一条命令。
#
# 判据：
#   1. `client.ts` 声明了七条路由并导出对应的七个取数函数；
#   2. 从 `main.tsx` 可达的源码里调用 `getScmStatus`/`getScmDiff`/`stagePaths`/
#      `unstagePaths`/`commitStaged`/`initWorkspaceRepo`，且 diff 那一次把路径与
#      「看哪一侧」都传了进去；
#   3. 这一面在四个登记处都齐（ActivityId / VIEW_TITLES / 活动栏 ITEMS / registry），
#      `diff` 这个页也在注册表里，且源码管理页真的开出 `diff:` 页签；
#   4. 网关登记了七条路由，`scm.rs` 每条命令都关掉钩子与签名，路径先过
#      `resolve_under`，`ide.rs` 拒绝 `.git` 路径段；
#   5. `pairHunk` 的对齐行为当场跑一遍（编译成 mjs 再跑断言）。
#
# 用法：bash deploy/scripts/check-web-source-control.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
CLIENT = "api/client.ts"
ENTRY = "main.tsx"
GATEWAY = "crates/cog-gateway/src/lib.rs"
SCM = "crates/cog-gateway/src/scm.rs"
IDE = "crates/cog-gateway/src/ide.rs"

ROUTES = {
    "getScmStatus": "/api/v1/ide/git/status",
    "getScmLog": "/api/v1/ide/git/log",
    "getScmDiff": "/api/v1/ide/git/diff",
    "initWorkspaceRepo": "/api/v1/ide/git/init",
    "stagePaths": "/api/v1/ide/git/stage",
    "unstagePaths": "/api/v1/ide/git/unstage",
    "commitStaged": "/api/v1/ide/git/commit",
}

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")
client = os.path.join(WEB, CLIENT)
if not os.path.isfile(client):
    sys.exit(f"读不到 {CLIENT}")
client_src = open(client, encoding="utf-8").read()

# ── 1. 七条路由与取它们的函数 ──
missing = [route for route in ROUTES.values() if route not in client_src]
if missing:
    sys.exit(f"{CLIENT} 里没有这些路由：{', '.join(missing)}")
for name in ROUTES:
    if not re.search(rf"export\s+(?:async\s+)?function\s+{name}\b", client_src):
        sys.exit(f"{CLIENT} 没有导出 {name}：路径在了，但没有人能取它")

# ── import 图 ──
IMPORT = re.compile(r"""^\s*(?:import|export)\b[^'"]*?from\s+['"]([^'"]+)['"]""", re.M)
EXT = ("", ".ts", ".tsx", "/index.ts", "/index.tsx")


def read(rel):
    return open(os.path.join(WEB, rel), encoding="utf-8").read()


def resolve(rel, spec):
    base = spec[2:] if spec.startswith("@/") else os.path.normpath(os.path.join(os.path.dirname(rel), spec))
    for ext in EXT:
        if os.path.isfile(os.path.join(WEB, base + ext)):
            return base + ext
    return None


def reachable():
    if not os.path.isfile(os.path.join(WEB, ENTRY)):
        sys.exit(f"读不到入口 {ENTRY}——可达性无从算起")
    seen, queue = set(), [ENTRY]
    while queue:
        rel = queue.pop()
        if rel in seen:
            continue
        seen.add(rel)
        for spec in IMPORT.findall(read(rel)):
            target = resolve(rel, spec)
            if target and target not in seen:
                queue.append(target)
    return seen


graph = reachable()


def call_args(src, name):
    """调用点括号里那一段（按括号配平，能跨行）。"""
    out = []
    for match in re.finditer(rf"\b{name}\s*\(", src):
        depth, start = 0, match.end() - 1
        for index in range(start, len(src)):
            if src[index] == "(":
                depth += 1
            elif src[index] == ")":
                depth -= 1
                if depth == 0:
                    out.append(src[start : index + 1])
                    break
    return out


# ── 2. 每个取数函数都有一个活的调用点 ──
# 只要求「有一个可达的消费者」：取数函数常被当作值传进去（`useResource(..., getScmStatus)`），
# 那一次不带括号。未使用的 import 由 tsc 的 noUnusedLocals 挡，两边合起来才是完整的判据。
dead = []
for name in ROUTES:
    if not any(re.search(rf"\b{name}\b", read(rel)) for rel in graph if rel != CLIENT):
        dead.append(name)
if dead:
    sys.exit(f"从 {ENTRY} 可达的源码里没有人调用 {', '.join(dead)}：路由在了，界面上没有消费者")

# diff 那一次必须把「哪个文件」和「看哪一侧」都传进去：少了后一个，暂存区与
# 工作区的差异会画成同一份，读者会以为已经暂存过。
def split_top_args(arg_text):
    """把 `(a, b, c)` 里的实参按顶层逗号切开。"""
    inner = arg_text.strip()
    if inner.startswith("(") and inner.endswith(")"):
        inner = inner[1:-1]
    parts, depth, current = [], 0, ""
    for ch in inner:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(current)
            current = ""
            continue
        current += ch
    if current.strip():
        parts.append(current)
    return [part.strip() for part in parts]


diff_args = []
for rel in sorted(graph):
    if rel != CLIENT:
        diff_args.extend(call_args(read(rel), "getScmDiff"))
if not diff_args:
    sys.exit("没有 getScmDiff 的调用点")
for arg in diff_args:
    parts = split_top_args(arg)
    if len(parts) < 2 or "path" not in parts[0]:
        sys.exit(f"getScmDiff{arg}：第一个实参不是要看的那个文件")
    # 第二格写死成 true/false 时，两条路径里必有一条永远打不开——而屏幕上
    # 照样画出一份「差异」，读者不会知道那是另一侧的。
    if parts[1] in ("true", "false"):
        sys.exit(
            f"getScmDiff{arg}：看哪一侧被写成了一个常量——"
            "另一侧的差异永远打不开，画出来的却是另一份"
        )

# ── 3. 四个登记处 + diff 页 ──
store = os.path.join(WEB, "shell/store.ts")
bar = os.path.join(WEB, "shell/ActivityBar.tsx")
registry = os.path.join(WEB, "views/registry.tsx")
for path in (store, bar, registry):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {path}")
store_src, bar_src, registry_src = (open(p, encoding="utf-8").read() for p in (store, bar, registry))
if not re.search(r"ActivityId\s*=\s*[^;]*['\"]scm['\"]", store_src, re.S):
    sys.exit("shell/store.ts: ActivityId 里没有 'scm'：这一面没有身份")
if not re.search(r"VIEW_TITLES[^{]*\{[^}]*scm\s*:", store_src, re.S):
    sys.exit("shell/store.ts: VIEW_TITLES 里没有 scm")
if not re.search(r"\{\s*id:\s*['\"]scm['\"]", bar_src):
    sys.exit("shell/ActivityBar.tsx: 活动栏里没有 scm 这一格")
if not re.search(r"case\s+['\"]scm['\"]\s*:\s*return\s*<SourceControlView", registry_src):
    sys.exit("views/registry.tsx: 没有 case 'scm' → <SourceControlView")
if not re.search(r"case\s+['\"]diff['\"]\s*:\s*return\s*<DiffView", registry_src):
    sys.exit("views/registry.tsx: 没有 case 'diff' → <DiffView：点开一份改动也没有东西画")

scm_view = [rel for rel in graph if rel.endswith(".tsx") and re.search(r"<SourceControlView", read(rel))]
if not scm_view:
    sys.exit("可达的源码里没有一处渲染 <SourceControlView")
# 开页签的那段代码在**定义这一面的文件**里，不在渲染它的 registry 里——
# 两处都要找，否则这条判据问的是一份不存在的连接。
impl = [
    rel for rel in graph if re.search(r"export\s+function\s+SourceControlView\b", read(rel))
]
if not impl:
    sys.exit("可达的源码里没有 export function SourceControlView")
opened = [
    rel
    for rel in impl
    if re.search(r"view:\s*['\"]diff['\"]", read(rel)) and re.search(r"`diff:\$\{", read(rel))
]
if not opened:
    sys.exit("源代码管理页没有开出 `diff:` 页签：改动列在那里，点不开差异")

# ── 4. 网关：七条路由 + 每条命令关掉钩子 + 路径先出根判据 ──
for path in (os.path.join(ROOT, GATEWAY), os.path.join(ROOT, SCM), os.path.join(ROOT, IDE)):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {path}")
gw_src = open(os.path.join(ROOT, GATEWAY), encoding="utf-8").read()
scm_src = open(os.path.join(ROOT, SCM), encoding="utf-8").read()
ide_src = open(os.path.join(ROOT, IDE), encoding="utf-8").read()

for route in ROUTES.values():
    if f'"{route}"' not in gw_src:
        sys.exit(f"{GATEWAY} 没有登记 {route}")
for handler in ("status_handler", "log_handler", "diff_handler", "init_handler",
                "stage_handler", "unstage_handler", "commit_handler"):
    if not re.search(rf"pub async fn {handler}\b", scm_src):
        sys.exit(f"{SCM}: 没有 {handler}——路由登记了也没有人接")
    if f"scm::{handler}" not in gw_src:
        sys.exit(f"{GATEWAY}: 路由没有接到 scm::{handler}")

if "core.hooksPath=/dev/null" not in scm_src:
    sys.exit(
        f"{SCM}: git 起来的时候钩子没关——仓库里的 .git 一旦可写，"
        "「保存一个文件」就能变成在网关进程里执行一条命令"
    )
if "commit.gpgsign=false" not in scm_src:
    sys.exit(f"{SCM}: 没有关掉提交签名——没有密钥的 Pod 上它会以「签名失败」收场")
if "--no-ext-diff" not in scm_src or "--no-textconv" not in scm_src:
    sys.exit(f"{SCM}: 看差异时没关掉外部 diff / textconv，那些是仓库配置里能指定的命令")
if "resolve_under(&root" not in scm_src:
    sys.exit(f"{SCM}: 路径没有先过 resolve_under——出根的路径交给了 git 自己判断")
# 按形状判，不按裸子串：改名成 `is_git_dir_unused` 时子串还在，判据就假绿了。
if not re.search(r"Component::Normal\(name\)\s+if\s+is_git_dir\(name\)", ide_src):
    sys.exit(f"{IDE}: 路径判据里没有「路径段是 .git 就拒」这一支——钩子与配置重新变成可写的")
if not re.search(r"fn is_git_dir\(name: &std::ffi::OsStr\) -> bool \{\s*name == std::ffi::OsStr::new\(\"\.git\"\)\s*\}", ide_src):
    sys.exit(f"{IDE}: is_git_dir 比较的不是 .git 这一个路径段")

print("ok：源代码管理七条路由都有活的消费者，四处登记齐，diff 页能开，"
      "git 关了钩子与外部 diff，.git 不可写")
PYEOF

# ── 5. 对齐算法当场跑一遍 ──
# 静态读代码看不出「两删一新会错位」这类形状，所以把纯函数编译出来真跑。
TSC="$ROOT/web/node_modules/.bin/tsc"
if [ ! -x "$TSC" ]; then
  echo "缺少依赖：$TSC（先装 web 依赖）" >&2
  exit 2
fi
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
"$TSC" "$ROOT/web/src/files/diff.ts" --target es2020 --module es2020 \
  --outDir "$TMP" --skipLibCheck >/dev/null
mv "$TMP/diff.js" "$TMP/diff.mjs"

NODE="$(command -v node || true)"
if [ -z "$NODE" ]; then
  echo "缺少依赖：node" >&2
  exit 2
fi
"$NODE" --input-type=module - "$TMP/diff.mjs" <<'JSEOF'
import { readFileSync } from 'node:fs';
const path = process.argv[2];
const { pairHunk } = await import('file://' + path);

const line = (kind, old_no, new_no, text) => ({ kind, old_no, new_no, text });
const check = (name, got, want) => {
  const a = JSON.stringify(got);
  const b = JSON.stringify(want);
  if (a !== b) {
    console.error(`${name}: 得到 ${a}，期望 ${b}`);
    process.exit(1);
  }
};
const shape = (rows) =>
  rows.map((r) => [
    r.left ? `${r.left.kind}:${r.left.old_no ?? '-'}` : 'null',
    r.right ? `${r.right.kind}:${r.right.new_no ?? '-'}` : 'null',
  ]);

// 一处改一行：删除与新增并排。
check(
  '一处改动',
  shape(
    pairHunk({
      lines: [
        line('context', 1, 1, 'keep'),
        line('del', 2, null, 'old'),
        line('add', null, 2, 'new'),
        line('context', 3, 3, 'keep'),
      ],
    })
  ),
  [
    ['context:1', 'context:1'],
    ['del:2', 'add:2'],
    ['context:3', 'context:3'],
  ]
);

// 两删一新：多出来的那条删除自己占一行，右侧留空。
check(
  '删除多于新增',
  shape(
    pairHunk({
      lines: [line('del', 1, null, 'a'), line('del', 2, null, 'b'), line('add', null, 1, 'c')],
    })
  ),
  [
    ['del:1', 'add:1'],
    ['del:2', 'null'],
  ]
);

// 一删两新：多出来的那条新增自己占一行，左侧留空。
check(
  '新增多于删除',
  shape(pairHunk({ lines: [line('del', 1, null, 'a'), line('add', null, 1, 'b'), line('add', null, 2, 'c')] })),
  [
    ['del:1', 'add:1'],
    ['null', 'add:2'],
  ]
);

// 一整块替换：两侧行数不同也要各就各位。
check(
  '整块替换',
  shape(
    pairHunk({
      lines: [
        line('del', 5, null, 'x'),
        line('del', 6, null, 'y'),
        line('del', 7, null, 'z'),
        line('add', null, 5, 'q'),
        line('add', null, 6, 'r'),
      ],
    })
  ),
  [
    ['del:5', 'add:5'],
    ['del:6', 'add:6'],
    ['del:7', 'null'],
  ]
);

check('空 hunk', pairHunk({ lines: [] }), []);

console.log('PAIR OK');
JSEOF

echo "SOURCE CONTROL OK"
