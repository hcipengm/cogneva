#!/usr/bin/env bash
# 访问令牌续期的消费者门禁。
#
# 网关签的访问令牌只活 15 分钟（`DEFAULT_ACCESS_TOKEN_TTL_MINUTES`）。界面的
# 成品不是「登录成功」，是**登录一个小时之后那一屏还在动**。坏起来的形状特别安静：
#
#   1. 登录响应里的 `refresh_token` 被丢掉。前端没有任何一处会打到
#      `/api/v1/auth/refresh`，于是没有任何东西报错——响应里多一个字段，谁注意？
#   2. 令牌到期之后，WebSocket 按写死的那枚旧令牌每 30 秒重连一次，每次都被
#      网关判 `Token expired`。界面只把状态条写成 closed，不登出、不提示，而所有
#      REST 读数都是 401。2026-10-26 在现网量到：一个浏览器标签在 20 分钟里
#      重连被拒 34 次，闸口日志里就是同一句 `WebSocket JWT verification failed:
#      Auth error: Token expired` 刷屏。
#   3. 从 OAuth 回调落地页进来的会话只拿到 `access_token`——落地页是网关渲染的
#      HTML，前端那套续期逻辑碰不到它，只能由落地页自己把续期凭据一起写下去。
#
# 这三种都编得过、跑得起来、测试全绿，只有人坐在前面等回复时会发现屏幕不动。
#
# 判据（每条都问「接线通不通」，不是问「文件里有没有某个串」）：
#   1. `api/client.ts` 声明 `/api/v1/auth/refresh` 路由并导出取它的函数。
#   2. `login()` 把响应里的 `refresh_token` 带出来（类型里有、返回值里有）。
#   3. `useAuth` 调用续期函数；续期失败时必须登出——否则界面停在一个看着还在、
#      其实一格都读不到的壳上。
#   4. 续上的令牌必须喂给事件流：`App.tsx` 从 `auth.token` 拼 `/ws?token=`，而
#      `useEventStream` 的副作用依赖 `options.url`（地址变了才重连）。
#   5. 令牌寿命的算术按判据实跑：编译 `auth/tokenLifecycle.ts`，在 node 里问它
#      「15 分钟的令牌还剩多少才该续」「过期的」「读不出 exp 的」三个答案。
#   6. OAuth 落地页也写续期凭据：同一个 localStorage 键在前端与网关渲染的那段
#      HTML 里都要出现，否则两条登录入口一条能续、一条不能。
#
# 用法：bash deploy/scripts/check-web-token-refresh.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
command -v node >/dev/null || { echo "缺少依赖：node" >&2; exit 2; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
CLIENT = "api/client.ts"
HOOK = "hooks/useAuth.ts"
APP = "App.tsx"
TRANSPORT_HOOK = "hooks/useEventStream.ts"
LIFECYCLE = "auth/tokenLifecycle.ts"
LANDING = "crates/cog-gateway/src/auth/platform.rs"
REFRESH_KEY = "cogneva_refresh_token"

def read(rel, base=WEB):
    path = os.path.join(base, rel)
    if not os.path.isfile(path):
        sys.exit(f"读不到 {rel}——续期这一面无从问起")
    return open(path, encoding="utf-8").read()

client = read(CLIENT)

# ── 1. 续期路由与取它的函数 ──
if "/api/v1/auth/refresh" not in client:
    sys.exit(f"{CLIENT} 里没有 /api/v1/auth/refresh：前端这一侧没有声明续期的路由")
if not re.search(r"export\s+(?:async\s+)?function\s+refreshSession\b", client):
    sys.exit(f"{CLIENT} 没有导出 refreshSession：路径在了，但没有人能取它")

# ── 2. 登录响应里的续期凭据不许被丢掉 ──
login = re.search(r"export\s+async\s+function\s+login\b[\s\S]*?\n}\n", client)
if not login:
    sys.exit(f"{CLIENT} 里找不到 login()")
if "refresh_token" not in login.group(0):
    sys.exit(
        "login() 的响应类型里没有 refresh_token：服务端给了，前端当它不存在，"
        "15 分钟之后没有任何东西可以拿来续期"
    )
if not re.search(r"refreshToken\s*:\s*[\w.]*refresh_token", login.group(0)):
    sys.exit(
        "login() 的返回值不是从响应的 refresh_token 来的（写成 `refreshToken: null` 也编得过，"
        "但那与丢掉它没有区别）：类型认了它，值仍然被扔掉了"
    )

# ── 3. 到期前续期；续不上就登出 ──
hook = read(HOOK)
if "refreshSession" not in hook:
    sys.exit(f"{HOOK} 没有调用 refreshSession：登录拿到的续期凭据没有任何东西在用")
renew = re.search(r"const\s+renew\s*=\s*useCallback\([\s\S]*?\n  \}, \[", hook)
if not renew:
    sys.exit(f"{HOOK} 里找不到续期的那段（renew）——无法确认失败时会发生什么")
body = renew.group(0)
if not re.search(r"catch\s*\{[\s\S]*?signOut\(\)", body):
    sys.exit(
        f"{HOOK} 的续期失败分支没有登出：令牌到期后界面会停在一个状态条写着 "
        "closed、每一格 REST 都 401 的壳上，看起来像实时流根本没实现"
    )
if not re.search(r"if\s*\(!refreshToken\)\s*\{[\s\S]*?signOut\(\)", body):
    sys.exit(
        f"{HOOK} 在拿不到续期凭据时没有登出：那种会话到期只会静默死掉，"
        "而不是把人送回登录页"
    )
if "tokenExpiryMs" not in hook or "nextRefreshDelayMs" not in hook:
    sys.exit(f"{HOOK} 没有按令牌自己的到期时刻排续期：什么时候续成了拍脑袋")

# ── 4. 续上的令牌必须真的喂给事件流 ──
app = read(APP)
if not re.search(r"/ws\?token=\$\{auth\.token\}", app):
    sys.exit(f"{APP} 里的实时流地址不再从 auth.token 拼：续了新令牌也重连不上")
stream = read(TRANSPORT_HOOK)
deps = re.search(r"\}, \[\s*([\s\S]*?)\n  \]\);", stream)
if not deps or "options.url" not in deps.group(1):
    sys.exit(
        f"{TRANSPORT_HOOK} 的副作用依赖里没有 options.url：令牌换了地址变了，"
        "连接却不会重建，新令牌用不上"
    )

# ── 5. 寿命算术：编译成单文件，在 node 里按判据跑 ──
lifecycle = os.path.join(WEB, LIFECYCLE)
if not os.path.isfile(lifecycle):
    sys.exit(f"读不到 {LIFECYCLE}：到期时刻没有任何一处算得出来")
if re.search(r"^\s*import\b", open(lifecycle, encoding="utf-8").read(), re.M):
    sys.exit(f"{LIFECYCLE} 有 import：它必须能单文件编译，否则下面这几条判据跑不起来")

# ── 6. 两条登录入口都要交出续期凭据 ──
if REFRESH_KEY not in hook:
    sys.exit(f"{HOOK} 里没有 {REFRESH_KEY}：前端存的键与落地页写的键对不上就是两条路各走各的")
landing_path = os.path.join(ROOT, LANDING)
if not os.path.isfile(landing_path):
    sys.exit(f"读不到 {LANDING}")
landing = open(landing_path, encoding="utf-8").read()
# 只在渲染那个页面的函数里找，不在整个文件里找：同一份文件里还有一条断言同一句话
# 的测试，按整份文件搜会让这个判据被它自己要检查的那句话满足——那样删掉写入行也绿。
render = re.search(r"fn login_landing_page\b[\s\S]*?\n}\n", landing)
if not render:
    sys.exit(f"{LANDING} 里找不到 login_landing_page：落地页写了什么无从问起")
if f"localStorage.setItem('{REFRESH_KEY}', d.auth.refresh_token)" not in render.group(0):
    sys.exit(
        f"{LANDING} 的登录落地页只写访问令牌：从 OAuth 回调进来的会话没有续期凭据，"
        "15 分钟后只能被登出"
    )

print("ok   - 续期路由、登录响应、钩子、事件流、落地页四处接线齐全")
PYEOF

# 单文件编译：这一步同时验证了「这个模块不依赖别的东西」。
TSC="$ROOT/web/node_modules/.bin/tsc"
[ -x "$TSC" ] || {
    echo "缺少 web/node_modules/.bin/tsc——先在 web/ 里装依赖（npm ci）" >&2
    exit 2
}
"$TSC" "$ROOT/web/src/auth/tokenLifecycle.ts" --target es2020 --module es2020 \
    --outDir "$TMP" >/dev/null 2>&1 || {
    echo "web/src/auth/tokenLifecycle.ts 编译不过：续期的判据无从检查" >&2
    exit 1
}
mv "$TMP/tokenLifecycle.js" "$TMP/tokenLifecycle.mjs"

node --input-type=module - "$TMP/tokenLifecycle.mjs" <<'JS'
const mod = await import(process.argv[2]);
const { tokenExpiryMs, nextRefreshDelayMs, REFRESH_LEAD_MS, FALLBACK_REFRESH_MS } = mod;
const fail = (msg) => {
  console.error(`续期算术不对：${msg}`);
  process.exit(1);
};

// 签一枚形状像真的 jwt：header.payload.signature，载荷里带 exp。
const b64url = (obj) =>
  Buffer.from(JSON.stringify(obj)).toString('base64url');
const jwt = (claims) => `${b64url({ alg: 'HS256' })}.${b64url(claims)}.sig`;

// 1. 15 分钟的令牌：当前时刻读出来必须在提前量之内。
const exp15 = 1_700_000_000;
if (tokenExpiryMs(jwt({ exp: exp15 })) !== exp15 * 1000) fail('读不出 exp 的秒数');

// 2. 现在续，还是过 14 分钟续：只有提前量那一分钟之差。
const now = exp15 * 1000 - 15 * 60 * 1000;
const delay = nextRefreshDelayMs(tokenExpiryMs(jwt({ exp: exp15 })), now);
if (delay !== 15 * 60 * 1000 - REFRESH_LEAD_MS) {
  fail(`刚登录时应过 ${15 * 60 * 1000 - REFRESH_LEAD_MS}ms 才续，实得 ${delay}`);
}

// 3. 已在提前量之内、以及已经过期：都要当场续（0，不是负数）。
if (nextRefreshDelayMs(exp15 * 1000, exp15 * 1000 - REFRESH_LEAD_MS + 1) !== 0) {
  fail('进入提前量后必须当场续期');
}
if (nextRefreshDelayMs(exp15 * 1000, exp15 * 1000 + 5_000) !== 0) {
  fail('已经过期的令牌必须当场续期，而不是返回一个负数');
}

// 4. 读不出到期时刻（不是 jwt、没有 exp）：走兜底间隔，别当场登出也别装没事。
if (tokenExpiryMs('not-a-jwt') !== null) fail('非 jwt 形状不该读出一个到期时刻');
if (tokenExpiryMs(jwt({ sub: 'x' })) !== null) fail('没有 exp 的载荷不该编一个到期时刻');
if (nextRefreshDelayMs(null, now) !== FALLBACK_REFRESH_MS) {
  fail('读不出到期时刻时应走兜底间隔');
}

// 5. base64url 里出现 - 与 _ 的令牌也要解得出（标准 base64 的字母表解不了）。
const weird = `${b64url({ alg: 'HS256' })}.${Buffer.from('{"exp":1700000000}').toString('base64url').replace(/=+$/, '')}.sig`;
if (tokenExpiryMs(weird) !== exp15 * 1000) fail(`带 -/_ 的载荷读不出来：${tokenExpiryMs(weird)}`);

console.log('ok   - 到期时刻、提前量、过期、兜底四条判据都成立');
JS

echo "check-web-token-refresh: 通过"
