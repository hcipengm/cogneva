#!/usr/bin/env bash
# `eval-switch.sh` 的夹具：只判盘上能判的结局——关的时候标记写成什么、在跑的那一轮被不
# 被叫停、开的时候清掉哪几个文件、`status` 读不读得到「谁在看着开关」。
#
# 真进程表一概不碰：`pkill` 在 PATH 前面塞一个桩，把「它被要求停谁」记下来。判据要的是
# 「关这个动作叫停了三类进程」，不是「宿主上恰好有这些进程」。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/eval-switch.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: 脚本不在：$SCRIPT" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin"
cat >"$TMP/bin/pkill" <<'EOF'
#!/bin/sh
# 只记参数，不去动任何真进程。
printf '%s\n' "$*" >>"$PKILL_LOG"
EOF
cat >"$TMP/bin/systemctl" <<'EOF'
#!/bin/sh
# `status` 会问定时器；答案本身不是这条夹具要判的，它只不许把整条命令弄失败。
echo enabled
EOF
# 集群那一半：`off` 要删掉这一族 pod。桩只记参数，并且可以按需判失败——
# 「删没删」与「删不掉时敢不敢说」是两条判据，缺一条就会再造出「关着还在花钱」。
cat >"$TMP/bin/kubectl" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$KUBECTL_LOG"
[ -f "$KUBECTL_FAIL" ] && exit 1
exit 0
EOF
chmod +x "$TMP/bin/pkill" "$TMP/bin/systemctl" "$TMP/bin/kubectl"

FLAG="$TMP/paused"
PKILL_LOG="$TMP/pkill.log"
KUBECTL_LOG="$TMP/kubectl.log"
KUBECTL_FAIL="$TMP/kubectl.fail"
export PKILL_LOG KUBECTL_LOG KUBECTL_FAIL
run() { env PATH="$TMP/bin:$PATH" PAUSE_FLAG="$FLAG" KUBECTL="$TMP/bin/kubectl" \
            bash "$SCRIPT" "$@" 2>&1; }

# 1. 关：先写标记再叫停。标记里要有人写的理由——读的人下一次只会看到这个文件。
: >"$PKILL_LOG"
out=$(run off "各方面没有理顺，先不做测评")
if [ -f "$FLAG" ] && grep -q '^reason: 各方面没有理顺，先不做测评$' "$FLAG"; then
  ok "off 写下了开关与理由"
else
  bad "off 没写下开关或理由，实得：$( [ -f "$FLAG" ] && cat "$FLAG" || echo 无文件 )"
fi
for p in 'supervise-eval-loop[.]sh' 'eval-registered-rerun[.]sh' 'eval-pool-watch[.]sh'; do
  if grep -qxF -- "-f $p" "$PKILL_LOG"; then
    ok "off 叫停了 $p"
  else
    bad "off 没有叫停 $p（关不住已经在跑的那一轮，标记挡不住它）"
  fi
done
case "$out" in *switch=off*) ok "off 的判词说了现在是关着的";; *) bad "off 的判词读不出 off：$out";; esac

# 1b. 关的第二半：集群里那一轮。宿主上杀掉的只是驱动，花钱的调用来自它起的 pod
# （`app=eval-table`），而那一族只在**下一轮开始时**才被回收——开关恰好挡住了下一轮。
# 实测 2026-11-08 22:51:08 关掉之后它又跑了 2 小时 33 分、1,422 次调用、¥14.75。
if grep -q -- '-l app=eval-table' "$KUBECTL_LOG"; then
  ok "off 删掉了集群里的 eval pod 一族"
else
  bad "off 没删 eval pod：关着的时候那一轮仍然在花钱（实得：$(tr '\n' ';' <"$KUBECTL_LOG")）"
fi

# 1c. 删不掉时必须说出来，且与「关好了」退出码不同：写一个假绿比不写更贵。
touch "$KUBECTL_FAIL"
set +e
out=$(run off "各方面没有理顺，先不做测评")
rc=$?
set -e
rm -f "$KUBECTL_FAIL"
[ "$rc" -eq 3 ] \
  && ok "删除失败时 off 退 3（标记写了、宿主叫停了、集群停不下来）" \
  || bad "删除失败时 off 退 ${rc}，不是 3：读的人会以为整条腿都停了"
case "$out" in
  *"may still be spending"*) ok "删除失败时判词点名了还在花钱这件事";;
  *) bad "删除失败时没说还在花钱：$out";;
esac

# 2. 关着时 status：判词、理由、以及「ensure 看过它没有」。
: >"$PKILL_LOG"
out=$(run status)
case "$out" in
  *switch=off*) ok "status 读出 off";;
  *) bad "status 没读出 off：$out";;
esac
case "$out" in
  *"reason: 各方面没有理顺，先不做测评"*) ok "status 把理由一并印出来";;
  *) bad "status 没印理由：$out";;
esac
case "$out" in
  *"ensure last looked: never"*) ok "没人摸过 .seen 时说「关掉之后还没人看过」，不冒充有人看着";;
  *) bad "status 对 .seen 的读法不对：$out";;
esac
touch "$FLAG.seen"
out=$(run status)
case "$out" in
  *"ensure last looked: never"*) bad "有 .seen 时仍说 never，读的人会以为那条腿死了";;
  *"ensure last looked: "*) ok "有 .seen 时印出它最后一次看的时间";;
  *) bad "status 读不到 .seen：$out";;
esac

# 3. `off` 清掉 `.logged`：不清的话下一段关闸一行日志都不写，而那一段正是要读的。
touch "$FLAG.logged"
run off >/dev/null
[ ! -e "$FLAG.logged" ] && ok "off 清掉 .logged" || bad "off 没清 .logged：下一段关闸将静默"

# 4. 开：三个文件都清掉，且不在此刻自己去拉值守——拉起是定时器那条腿的事。
touch "$FLAG.seen" "$FLAG.logged"
out=$(run on)
[ ! -e "$FLAG" ] && [ ! -e "$FLAG.seen" ] && [ ! -e "$FLAG.logged" ] \
  && ok "on 清掉开关、.seen 与 .logged" \
  || bad "on 没清干净：$(ls -1 "$FLAG"* 2>/dev/null | tr '\n' ' ')"
case "$out" in
  *switch=on*) ok "on 的判词说了现在是开着的";;
  *) bad "on 的判词读不出 on：$out";;
esac
out=$(run status)
case "$out" in
  *switch=on*) ok "开闸后 status 读出 on";;
  *) bad "开闸后 status 仍读出别的：$out";;
esac

# 5. 判据只有「文件在不在」，不看内容：写坏的标记（空文件、半行）不许被读成开着。
: >"$FLAG"
out=$(run status)
case "$out" in
  *switch=off*) ok "空标记仍判为关着（写坏的机器不该把评测打开）";;
  *) bad "空标记被读成开着：$out";;
esac

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
