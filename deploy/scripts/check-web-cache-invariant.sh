#!/usr/bin/env bash
# 缓存命中率的分母门禁：读面与记账面必须是同一条不变式。
#
# 命中率是这个系统里唯一能验收「把稳定前缀排到前面」这类改动的读数，而它由两半拼成：
# 网关落账时把两个协议面归一（OpenAI 形状的 `prompt_tokens` 先减去命中量再记），
# 前端再用 `cached / (input + cached)` 把它算出来。两半各自都对、拼起来错，是这条
# 读数最常见的坏法——屏幕上一个像模像样的百分比，谁也不会去验。
#
# 2026-11-01 之前就是这样：前端按 `api_style` 挑分母（OpenAI 面取 `input`，Anthropic
# 面取两者之和）。而 `api_style` 记的是请求走哪个面，不是上游回哪种形状——实测过一行
# `api_style=anthropic` 却带回 OpenAI 形状的尾帧，88% 的命中被读成 1.1%。归一口径
# 之后那一支更把 OpenAI 面的命中率压到一成多。所以判据只有一条：分母只有一个，
# 而且与协议无关。
#
# 判据（都是可跑的断言，不是读源码猜）：
#   1. 实测的那一行：input=175、cached=1280 时，分母必须是 1455、命中率 88%。
#      按旧写法（openai 面取 input）这里是 13.7%，按 anthropic 面取两者之和才对
#      ——同一个数在两种协议名下给出两个答案，本身就是错的形状。
#   2. 分母函数只收两个数：`cacheBase.length === 2`。它没有地方接协议名，也就没有
#      按协议改分母的余地。哪天有人把协议加回参数里，这一条先红。
#   3. 分母为零印 `—`，不印 `0%`：没输入与命中为零是两件事。
#   4. 悬停里必须同时印出分子与分母（交接时定的口径：比值前端算，算的两端都看得见）。
#
# 这个文件之所以能这样验，是靠它自己：`usageRows.ts` 只 import 类型，值层面没有依赖。
# 哪天它开始 import 别的东西，这一步会先红在「加载不起来」上——那也是它不再纯净的证据。
#
# 用法：bash deploy/scripts/check-web-cache-invariant.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v node >/dev/null || { echo "缺少依赖：node" >&2; exit 2; }
SRC="web/src/observability/usageRows.ts"
[ -f "$SRC" ] || { echo "读不到 $SRC" >&2; exit 1; }

node --no-warnings --experimental-strip-types --input-type=module - "$ROOT/$SRC" <<'JS'
const { cacheBase, cacheFormula, hitRate } = await import(process.argv[2]);
const fail = (msg) => {
  console.error(`缓存分母不对：${msg}`);
  process.exit(1);
};

// 0. 先判形状后面再判数：分母函数一旦重新收协议名，下面那些按数调用会抛栈，读的人
//    看到的是「startsWith is not a function」而不是「分母不许由协议决定」。
if (cacheBase.length !== 2) {
  fail(`cacheBase 收了 ${cacheBase.length} 个参数——分母不许由协议名决定`);
}

// 1. 实测的那一行（k3，2026-11-01）：prompt 1455，其中 1280 由缓存服务。
//    归一之后账上记 input=175、cached=1280。
if (cacheBase(175, 1280) !== 1455) {
  fail(`input=175、cached=1280 的分母应是整段 prompt 1455，实际 ${cacheBase(175, 1280)}`);
}
if (hitRate(175, 1280) !== '88%') {
  fail(`同一行命中率应是 88%，实际 ${hitRate(175, 1280)}`);
}
// 反过来：命中高于一半时 cached 大过 input 是健康的形状，分母仍然只能是两者之和。
if (cacheBase(100000, 900000) !== 1000000) {
  fail(`命中九成的行分母应是 1000000，实际 ${cacheBase(100000, 900000)}`);
}

// 3. 没有输入时没有比例可言，写「—」；写 0% 是把「算不出来」说成一个读数。
if (hitRate(0, 0) !== '—') fail(`分母为零时应印「—」，实际 ${hitRate(0, 0)}`);
if (hitRate(0, 5000) !== '100%') fail(`没命中输入、缓存服务了 5000 时是 100%，实际 ${hitRate(0, 5000)}`);

// 4. 悬停里分子与分母都在，读者能自己复算。
const formula = cacheFormula('anthropic', 175, 1280);
if (!formula.includes('1,280') || !formula.includes('1,455')) {
  fail(`悬停没同时印出分子与分母：${formula}`);
}

console.log('cache invariant ok');
JS
