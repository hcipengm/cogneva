#!/usr/bin/env bash
# principle: feedback_a_proxy_that_rewrites_a_header_makes_a_synthetic_header_test_blind
# check-ingress-forwarded-proto.py 的定向测试。
#
# 被测的是「改写 X-Forwarded-Proto 的 middleware，必须把这条 ingress 实际在服务的
# scheme 写进这个头」。治的病是**合成头测试对此完全失明**：测试自己塞
# `X-Forwarded-Proto: https`，应用照头拼出 `https://…`，断言全绿——而现网这条路由
# 只开明文 `web`，对端拿到的 `https://<ip>/…` 拨过去是 404。头是反代编的，应用没错。
#
# 格子分两半，一半钉「什么必须红」，一半钉「不该红的别红」——后者更重要：一扇把正常
# 布局判红的门会被绕过，绕过之后连它本来守的那条也一起没了。
#   - 明文路由、middleware 不碰这个头 ⇒ 绿（traefik 自己按实际连接写的才是真值）；
#   - 明文路由、middleware 写死 https ⇒ 红，判词点名 ingress、middleware、两个 scheme；
#   - websecure 路由写死 https ⇒ 绿；写死 http ⇒ 红；
#   - 没有 entrypoint 注解但有 `spec.tls` ⇒ 按 https 读，写死 http 仍红；
#   - middleware 是 `namespace-name@kubernetescrd` 引用、对象上名字与 namespace 分开写
#     ⇒ 必须能解析：判成「找不到」会让真树的每一格都读作 unresolved，门就成了摆设；
#   - 引用了本目录里没有的 middleware ⇒ 计入 unresolved 并点名，但不判红（那不是
#     这个门守的事，看错的是另一条）；
#   - 一个目录的 ingress 引用另一个目录才有的 middleware ⇒ 不许借：每套清单单独判，
#     跨目录借会把 profile 之间的分叉读成一致；
#   - `customResponseHeaders` 里的同名头 ⇒ 不判：它发回客户端，到不了处理器；
#   - 解析不了清单 ⇒ 3；一条 ingress 都没读到 ⇒ 3（「没判过」不许读作「干净」）；
#   - 真树 ⇒ 绿且 routes ≥ 1（防空转：判据写得再对，读到 0 条路由也是没判）。
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-ingress-forwarded-proto.py"
[ -f "$gate" ] || { echo "FAIL: 被测脚本不在：$gate" >&2; exit 1; }

pass=0; fail=0
ok()    { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad()   { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
ck_rc() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1（期望退出码 $2，实得 $3）"; fi; }
ck_has() { if printf '%s\n' "$2" | grep -qF -- "$3"; then ok "$1"; else
  bad "$1（期望含 $3）"; printf '%s\n' "$2" | sed 's/^/        实得: /' | head -12 >&2; fi; }
ck_hasnt() { if printf '%s\n' "$2" | grep -qF -- "$3"; then bad "$1（不该含 $3）"; else ok "$1"; fi; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# ingress <目录> <entrypoints 注解值或 - 表示不写> <有无 tls 段 yes/no> <middlewares 注解值或 ->
ingress() {
  local dir="$1" entry="$2" tls="$3" mws="$4"
  mkdir -p "$dir"
  {
    printf 'apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: cogneva\n  namespace: cogneva\n'
    if [ "$entry" != "-" ] || [ "$mws" != "-" ]; then
      printf '  annotations:\n'
      [ "$entry" = "-" ] || printf '    traefik.ingress.kubernetes.io/router.entrypoints: %s\n' "$entry"
      [ "$mws" = "-" ]   || printf '    traefik.ingress.kubernetes.io/router.middlewares: "%s"\n' "$mws"
    fi
    printf 'spec:\n  ingressClassName: traefik\n'
    [ "$tls" = "no" ] || printf '  tls:\n    - hosts: [cogneva.example]\n'
    printf '  rules:\n    - http:\n        paths:\n          - path: /\n            pathType: Prefix\n'
    printf '            backend:\n              service:\n                name: cogneva\n                port:\n                  number: 8080\n'
  } >"$dir/ingress.yaml"
}

# middleware <目录> <name> <namespace> <标量头或 - 表示不设> <放在哪个 section>
middleware() {
  local dir="$1" name="$2" ns="$3" value="$4" section="${5:-customRequestHeaders}"
  mkdir -p "$dir"
  {
    printf 'apiVersion: traefik.io/v1alpha1\nkind: Middleware\nmetadata:\n  name: %s\n  namespace: %s\n' "$name" "$ns"
    printf 'spec:\n  headers:\n    %s:\n      X-Frame-Options: "SAMEORIGIN"\n' "$section"
    [ "$value" = "-" ] || printf '      X-Forwarded-Proto: "%s"\n' "$value"
  } >"$dir/middleware.yaml"
}

run() { python3 "$gate" "$@" 2>&1; }

# 1) 明文路由，middleware 不碰这个头 ⇒ 绿
t="$work/plain-clean"; ingress "$t" web no "cogneva-websocket-headers@kubernetescrd"
middleware "$t" websocket-headers cogneva -
out="$(run "$t")"; rc=$?
ck_rc "明文路由 + 不改写 ⇒ 绿" 0 "$rc"
ck_has "汇总印出判过的路由数" "$out" "routes=1"
ck_has "汇总印出改写的条数（这里是 0）" "$out" "rewrites=0"

# 2) 明文路由写死 https ⇒ 红，且点名两处与两个 scheme
t="$work/plain-pinned-https"; ingress "$t" web no "cogneva-websocket-headers@kubernetescrd"
middleware "$t" websocket-headers cogneva https
out="$(run "$t")"; rc=$?
ck_rc "明文路由写死 https ⇒ 红" 1 "$rc"
ck_has "判词点名 ingress" "$out" "ingress/cogneva"
ck_has "判词点名 middleware" "$out" "cogneva-websocket-headers"
ck_has "判词说清路由实际的 scheme" "$out" "serves http"
ck_has "判词说清头被写成了什么" "$out" "rewrites X-Forwarded-Proto to https"

# 3) websecure 路由写死 https ⇒ 绿（这才是那条 middleware 该有的写法）
t="$work/tls-https"; ingress "$t" websecure no "cogneva-websocket-headers@kubernetescrd"
middleware "$t" websocket-headers cogneva https
out="$(run "$t")"; rc=$?
ck_rc "websecure 路由写死 https ⇒ 绿" 0 "$rc"
ck_has "确实判到了那一条改写（不是空转）" "$out" "rewrites=1"

# 4) websecure 路由写死 http ⇒ 红（反方向同样是撒谎）
t="$work/tls-pinned-http"; ingress "$t" websecure no "cogneva-websocket-headers@kubernetescrd"
middleware "$t" websocket-headers cogneva http
out="$(run "$t")"; rc=$?
ck_rc "websecure 路由写死 http ⇒ 红" 1 "$rc"
ck_has "判词说清路由实际的 scheme" "$out" "serves https"

# 5) 没有 entrypoint 注解但带 spec.tls ⇒ 按 https 读
t="$work/tls-only"; ingress "$t" - yes "cogneva-websocket-headers@kubernetescrd"
middleware "$t" websocket-headers cogneva http
out="$(run "$t")"; rc=$?
ck_rc "只有 tls 段、写死 http ⇒ 红" 1 "$rc"
ck_has "tls 段读作 https" "$out" "serves https"

# 6) web,websecure 同时开 ⇒ 有 TLS，按 https 读
t="$work/both"; ingress "$t" "web,websecure" no "cogneva-websocket-headers@kubernetescrd"
middleware "$t" websocket-headers cogneva https
out="$(run "$t")"; rc=$?
ck_rc "web,websecure 写死 https ⇒ 绿" 0 "$rc"

# 7) 引用的 middleware 在本目录里没有 ⇒ 计入 unresolved 并点名，不判红
t="$work/dangling"; ingress "$t" web no "cogneva-absent@kubernetescrd"
out="$(run "$t")"; rc=$?
ck_rc "引用不存在的 middleware 不判红" 0 "$rc"
ck_has "但要点名 unresolved" "$out" "CENSUS unresolved"
ck_has "点名缺的是哪一个" "$out" "cogneva-absent"
ck_has "汇总裁出 unresolved 条数" "$out" "unresolved=1"

# 8) 跨目录不许借：A 的 ingress 引用只存在于 B 的 middleware
t="$work/xdir"
ingress "$t/a" web no "cogneva-websocket-headers@kubernetescrd"
middleware "$t/b" websocket-headers cogneva https
out="$(run "$t/a" "$t/b")"; rc=$?
ck_rc "跨目录借 middleware 不算解析成功" 0 "$rc"
ck_has "A 引用的那个在 A 里没有 ⇒ unresolved" "$out" "unresolved=1"

# 9) customResponseHeaders 里的同名头 ⇒ 不判（它到不了处理器）
t="$work/response-only"; ingress "$t" web no "cogneva-websocket-headers@kubernetescrd"
middleware "$t" websocket-headers cogneva https customResponseHeaders
out="$(run "$t")"; rc=$?
ck_rc "响应头里的同名头不判" 0 "$rc"
ck_has "没有把它算成改写" "$out" "rewrites=0"

# 10) 解析不了清单 ⇒ 3
t="$work/broken"; mkdir -p "$t"
printf 'apiVersion: v1\nkind: [unclosed\n' >"$t/broken.yaml"
out="$(run "$t")"; rc=$?
ck_rc "清单解析失败 ⇒ 3" 3 "$rc"
ck_has "并说清那一段没被任何东西判过" "$out" "not the same as clean"

# 11) 一条 ingress 都没有 ⇒ 3，不是 0
t="$work/empty"; mkdir -p "$t"
middleware "$t" websocket-headers cogneva https
out="$(run "$t")"; rc=$?
ck_rc "没有路由可判 ⇒ 3" 3 "$rc"
ck_has "并说「没判过」" "$out" "nothing was judged"

# 12) 真树 ⇒ 绿，且确实读到了路由
out="$(cd "$repo" && python3 "$gate" 2>&1)"; rc=$?
ck_rc "真树 ⇒ 绿" 0 "$rc"
ck_has "真树的每条 middleware 引用都解析到了" "$out" "unresolved=0"
echo "$out" | tail -1 | grep -qE 'routes=[1-9][0-9]*' \
  && ok "真树至少判到一条路由（防空转）" \
  || bad "真树一条路由都没判到（门是空转的）"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
