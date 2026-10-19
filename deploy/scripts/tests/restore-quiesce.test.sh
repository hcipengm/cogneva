#!/usr/bin/env bash
# 恢复前停写者：缩到 0、等 Pod 真没了、恢复完按原副本数放行。
#
# 变异测试要拦的三种写法都长得像对的：
#   1. 只发 `scale --replicas=0` 不等 Pod 退出——命令返回了，Pod 还在写；
#   2. 放行时写死一个副本数——扩容过的部署被缩回默认值，恢复后少了一半容量；
#   3. 停与放行各读一次集群——两次之间有人改过副本数时，两边对不上而没人发现。
# 被测代码从脚本里按标记抽出来，不在这里抄一份：抄的那份会在脚本改过之后继续绿。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
script="${repo}/deploy/scripts/restore-from-package.sh"
fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
mkdir -p "${work}/bin"

sed -n '/^# quiesce: start$/,/^# quiesce: end$/p' "${script}" >"${work}/quiesce.sh"
[ -s "${work}/quiesce.sh" ] || fail "no quiesce block found in ${script}"

# 假的 kubectl：只回答这一段用到的调用，并逐条记账。每台部署的原始副本数由
# REPLICAS_<名字> 给出；Pod 列表在缩容后返回空（真实集群里 wait --for=delete
# 就是这个结局）。
cat >"${work}/bin/kubectl" <<'STUB'
#!/usr/bin/env bash
cmd="$*"
printf '%s\n' "$cmd" >>"${CALLS}"
case "$cmd" in
    "-n cogneva get deploy "*"jsonpath={.spec.replicas}")
        d="${cmd#-n cogneva get deploy }"
        d="${d%% *}"
        var="REPLICAS_${d//-/_}"
        printf '%s' "${!var:-1}"
        ;;
    "-n cogneva scale --replicas=0 deployment/"*) : ;;
    "-n cogneva scale --replicas="*" deployment/"*) : ;;
    "-n cogneva get deploy "*"matchLabels"*)
        d="${cmd#-n cogneva get deploy }"
        d="${d%% *}"
        printf 'app.kubernetes.io/name=%s,' "$d"
        ;;
    "-n cogneva get pods -l "*" -o name")
        # 缩过容的部署没有 Pod；没缩的一律有——用来证明「不等就过」会被拦。
        case "$cmd" in
            *"app.kubernetes.io/name=cogneva-slow"*) echo "pod/leftover" ;;
            *) : ;;
        esac
        ;;
    "-n cogneva wait --for=delete pod"*) ;;
    "-n cogneva rollout status deployment/"*) : ;;
    *) echo "stub kubectl: unexpected call: $cmd" >&2; exit 1 ;;
esac
STUB
chmod +x "${work}/bin/kubectl"
export PATH="${work}/bin:${PATH}"
export CALLS="${work}/calls"
: >"${CALLS}"

run() {  # run <原始副本数……>，部署名由 REPLICAS_* 变量给出
    (
        export NS=cogneva
        # shellcheck disable=SC1090
        source "${work}/quiesce.sh"
        # 被 source 进来的 quiesce_writers 读它。
        # shellcheck disable=SC2034
        APP_DEPLOYMENTS=(cogneva cogneva-evolution cogneva-slow)
        quiesce_writers
        resume_writers
    )
}

REPLICAS_cogneva=3 REPLICAS_cogneva_evolution=1 REPLICAS_cogneva_slow=2 run >/dev/null \
    || fail "停写者那一段跑不通"

# 1. 每个部署都缩到 0，且缩容发生在放行之前。
zero_lines="$(grep -n 'scale --replicas=0' "$CALLS" | cut -d: -f1 | tail -1)"
first_restore="$(grep -n 'scale --replicas=[1-9]' "$CALLS" | head -1 | cut -d: -f1)"
[ -n "$first_restore" ] || fail "没有任何放行动作"
[ "$zero_lines" -lt "$first_restore" ] || fail "先放行后缩容"
for d in cogneva cogneva-evolution cogneva-slow; do
    grep -q "scale --replicas=0 deployment/$d" "$CALLS" || fail "$d 没有缩到 0"
done

# 2. 放行用的是停之前读到的数，不是写死的 1。
grep -q "scale --replicas=3 deployment/cogneva$" "$CALLS" || fail "cogneva 没按原副本数 3 放行"
grep -q "scale --replicas=1 deployment/cogneva-evolution$" "$CALLS" || fail "evolution 没按原副本数放行"
grep -q "scale --replicas=2 deployment/cogneva-slow$" "$CALLS" || fail "slow 没按原副本数 2 放行"

# 3. 还有一个部署的 Pod 没退干净时必须等，不能只发缩容命令就往下走。
grep -q "wait --for=delete pod -l app.kubernetes.io/name=cogneva-slow" "$CALLS" \
    || fail "Pod 还在就往下走了"

# 4. 每个部署只读一次副本数：停与放行共用同一份读数。
reads="$(grep -c 'jsonpath={.spec.replicas}' "$CALLS")"
[ "$reads" = "3" ] || fail "副本数读了 ${reads} 次（3 个部署应各一次；多读说明两处各取一遍）"

echo "restore quiesce OK"
