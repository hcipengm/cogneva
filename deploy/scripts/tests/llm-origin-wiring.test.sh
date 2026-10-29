#!/usr/bin/env bash
# check-llm-origin-wiring.sh 的判据。
#
# 这条门的价值全在「清单里写错了会不会红」，所以夹具必须**每个失败臂各有一条**：
# 值不在闭集、只声明一个值、沙盒那格写成别的、别处冒用沙盒、不是字面量、闭集里
# 某个值没有任何生产者。六条里少一条，那一条坏了就没人知道。
#
# 有几个夹具同时踩到两条判据（放宽「至少两个值」的那条，往往也顺手动了别的格子），
# 所以断言除了退出码还要**点名该条判据的判词**——只看退出码的话，一个红在别处的
# 夹具会替它想验的那条判据签字。
#
# 闭集那份源码从**真文件**拷进来，不是夹具里另写一份：这条门的一半判据是「清单
# 必须与代码认的值一致」，夹具自己编一份闭集就只证明了门认得出它自己写的那份。
# 夹具里那棵 crates 树也是真树的切片，所以它跟着真代码漂——真代码改了闭集，这里
# 会先红。
#
# 最后一条断言是**在真仓库根上跑**：夹具全绿而真树红，说明这条门只在测自己。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-llm-origin-wiring.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# workload <kind> <name> <container> <env-name> <value>|--valueFrom
# 每份都自带 `---` 分隔：同一个 profile 目录里几条负载要能拼进同一个文件，
# 少这个分隔符时 PyYAML 把它们并成一份映射、重复键取最后一个——负载会静默少一条，
# 而少掉的那条正是夹具想验的那条。
workload() {
  local kind="$1" name="$2" container="$3" envname="$4" value="$5"
  cat <<YAML
---
apiVersion: apps/v1
kind: ${kind}
metadata:
  name: ${name}
spec:
  template:
    spec:
      containers:
        - name: ${container}
          env:
            - name: ${envname}
$(if [ "${value}" = "--valueFrom" ]; then
  printf '              valueFrom:\n                fieldRef:\n                  fieldPath: metadata.name\n'
else
  printf '              value: "%s"\n' "${value}"
fi)
YAML
}

# 把一棵完整的假树铺出来：真闭集源码 + 一个 with_origin 调用点 + 该用例的清单。
# 用法：make_root <根目录> <用例名>
make_root() {
  local root="$1" case="$2" p
  p="${root}/deploy/rendered/p"
  mkdir -p "${p}" "${root}/crates/cog-core/src/contract" \
           "${root}/crates/cog-observability/src" "${root}/crates/cog-gateway/src"
  cp "${repo}/crates/cog-core/src/contract/usage_ledger.rs" \
     "${root}/crates/cog-core/src/contract/usage_ledger.rs"
  cp "${repo}/crates/cog-observability/src/usage_store.rs" \
     "${root}/crates/cog-observability/src/usage_store.rs"
  # 树里那半生产者要每个闭集成员都点到名：夹具只写一个值的话，真闭集一加成员，
  # 正向用例就会以「新成员没有生产者」红——那是夹具的缺口，不是门的判据。
  cat >"${root}/crates/cog-gateway/src/chat.rs" <<'RS'
fn opts() -> ChatOptions {
    ChatOptions::default()
        .with_actor("web_chat")
        .with_origin(LlmOrigin::Interactive.as_str())
}

fn eval_opts() -> ChatOptions {
    ChatOptions::default()
        .with_actor("eval")
        .with_origin(LlmOrigin::Eval.as_str())
}
RS
  case "${case}" in
    # 到位的一组：两条 gateway_pool、一条沙盒。
    good|typo|valuefrom|no_producer|moved)
      workload Deployment cogneva cogneva COGNEVA_LLM_ORIGIN gateway_pool >"${p}/10.yaml"
      workload Deployment cogneva-evolution cogneva COGNEVA_LLM_ORIGIN gateway_pool >>"${p}/10.yaml"
      workload Deployment cogneva-sandbox-executor sandbox-executor \
        COGNEVA_LLM_ORIGIN sandbox >"${p}/20.yaml"
      ;;
    # 没有沙盒那条负载：域里只剩一个值。
    one_value)
      workload Deployment cogneva cogneva COGNEVA_LLM_ORIGIN gateway_pool >"${p}/10.yaml"
      workload Deployment cogneva-evolution cogneva COGNEVA_LLM_ORIGIN gateway_pool >>"${p}/10.yaml"
      ;;
    # 沙盒那格写成别的：两条不同的值仍在，坏的是沙盒那一格。
    sandbox_wrong)
      workload Deployment cogneva cogneva COGNEVA_LLM_ORIGIN gateway_pool >"${p}/10.yaml"
      workload Deployment cogneva-evolution cogneva COGNEVA_LLM_ORIGIN resident_worker >>"${p}/10.yaml"
      workload Deployment cogneva-sandbox-executor sandbox-executor \
        COGNEVA_LLM_ORIGIN resident_worker >"${p}/20.yaml"
      ;;
    # 不是沙盒的负载冒用沙盒那一格。
    sandbox_stolen)
      workload Deployment cogneva cogneva COGNEVA_LLM_ORIGIN sandbox >"${p}/10.yaml"
      workload Deployment cogneva-evolution cogneva COGNEVA_LLM_ORIGIN gateway_pool >>"${p}/10.yaml"
      workload Deployment cogneva-sandbox-executor sandbox-executor \
        COGNEVA_LLM_ORIGIN sandbox >"${p}/20.yaml"
      ;;
  esac
  case "${case}" in
    typo) sed -i 's/"gateway_pool"/"gateway-pool"/' "${p}/10.yaml" ;;
    valuefrom)
      sed -i 's/^              value: "gateway_pool"$/              valueFrom:\n                fieldRef:\n                  fieldPath: metadata.name/' \
        "${p}/10.yaml"
      ;;
    no_producer) : >"${root}/crates/cog-gateway/src/chat.rs" ;;
    # 闭集所在的文件搬走了：只留 core 里那一份，旧位置不存在。门必须照样读出闭集
    # ——它写死过一个路径，契约搬去 core 之后它自己红成「闭集没读出来」，而那条判
    # 词指向的是门记的旧行，不是任何一条接线。
    moved) rm -f "${root}/crates/cog-observability/src/usage_store.rs" ;;
  esac
}

# expects <case> <exit-code> [<判词里必须出现的串>]
expect() {
  local case="$1" want="$2" mention="${3:-}"
  local root="${work}/${case}" got=0
  make_root "${root}" "${case}"
  bash "${checker}" "${root}" >"${work}/${case}.out" 2>&1 || got=$?
  [ "${got}" -eq "${want}" ] || {
    cat "${work}/${case}.out" >&2
    fail "${case}: exit ${got}, wanted ${want}"
  }
  if [ -n "${mention}" ]; then
    grep -qF "${mention}" "${work}/${case}.out" || {
      cat "${work}/${case}.out" >&2
      fail "${case}: 判词里没有「${mention}」——它红在别处，不是它想验的那条判据"
    }
  fi
}

expect good 0
expect moved 0

# 正向用例必须先证明它把该有的三条负载都装进去了。夹具铺错时（写这条时真踩过：
# 几份 YAML 追加进同一个文件却没带 `---`，解析器把它们并成一份映射、重复键取最后
# 一个）会静默少一条，而剩下那条照样让门绿——**那样的绿是为错的理由绿的**，
# 后面六条失败臂全部建在它上面。所以正向的断言不能只看退出码。
for w in cogneva cogneva-evolution cogneva-sandbox-executor; do
  grep -qF "Deployment/${w}[" "${work}/good.out" ||
    fail "good: 判词里没有 Deployment/${w} —— 正向夹具没把该有的负载装齐"
done

expect typo 1 "不在闭集里"
expect one_value 1 "下界是 2"
expect sandbox_wrong 1 "沙盒执行器声明了"
expect sandbox_stolen 1 "但它不是沙盒执行器"
expect valuefrom 1 "不是字面量"
expect no_producer 1 "'interactive' 没有任何生产者"

# 真仓库：今天必须绿。
got=0
out="$(bash "${checker}" "${repo}" 2>&1)" || got=$?
[ "${got}" -eq 0 ] || fail "真仓库里这条轴没接好：${out}"

echo "llm-origin-wiring.test: all checks passed"
