#!/usr/bin/env bash
# principle: feedback_a_guard_that_nothing_asserts_can_be_deleted_in_silence
# check-config-checksum-wiring.sh 的定向测试。
#
# 这道门禁守的是一条**只在真升级里才现形**的接线：读同一份配置的工作负载必须带着
# 配置的 sha256 指纹，否则只改配置的那次升级不会换 Pod——配置换了，Pod 还是旧的，
# 而渲染、parity、Pod 状态三面全绿。它的失败方式不是报错，是**没有变化**，所以
# 门禁自己必须能在树被改坏时红，否则它只是一段自我介绍。
#
# 每个用例都在真树的副本上只改一处，然后断言它红的正是那一处：
#   1. 真树 → 绿，并报出消费者条数（不是空跑）；
#   2. 删掉一条消费者的注解 → 红，点名那条工作负载；
#   3. 注解不是 64 位小写十六进制 → 红；
#   4. 同一形态里两条消费者的指纹不等 → 红；
#   5. 一种形态里只剩一条消费者 → 红（下界是声明）；
#   6. 所有产物共用一个写死的指纹 → 红（sha256 表达式被换成字面量）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-config-checksum-wiring.sh"
evolution_single="deploy/rendered/k3s-single/41-deployment-cogneva-evolution.yaml"
main_single="deploy/rendered/k3s-single/41-deployment-cogneva.yaml"
pass=0
fail=0
ck() { # $1 名称 $2 期望 0/1 $3 命令输出 $4 退出码
  if [ "$4" -eq "$2" ]; then pass=$((pass + 1)); echo "ok    - $1"; else
    fail=$((fail + 1)); echo "FAIL  - $1（rc=$4，期望 $2）"; echo "$3" | sed 's/^/        /'; fi
}
ck_has() { # $1 名称 $2 子串 $3 输出
  if printf '%s' "$3" | grep -qF -- "$2"; then pass=$((pass + 1)); echo "ok    - $1"; else
    fail=$((fail + 1)); echo "FAIL  - $1（输出里没有「$2」）"; echo "$3" | sed 's/^/        /'; fi
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

mirror() {
  rm -rf "${work}/root"
  mkdir -p "${work}/root/deploy"
  cp -r "${repo}/deploy/rendered" "${work}/root/deploy/rendered"
  cp -r "${repo}/deploy/k3s" "${work}/root/deploy/k3s"
}
run() { ROOT="${work}/root" bash "${gate}" "${work}/root" 2>&1 || true; }
rc() { ROOT="${work}/root" bash "${gate}" "${work}/root" >/dev/null 2>&1; echo $?; }

# 1. 真树 → 绿，而且不是空跑
mirror
out="$(run)"
ck "1. 真树：绿" 0 "$out" "$(rc)"
ck_has "1. 报出消费者条数（不是空跑）" "条读 cogneva-json 的工作负载" "$out"

# 2. 删掉 evolution 的注解 → 红，点名它
mirror
sed -i '/checksum\/config:/d' "${work}/root/${evolution_single}"
out="$(run)"
ck "2. 删掉注解：红" 1 "$out" "$(rc)"
ck_has "2. 点名那条工作负载" "cogneva-evolution" "$out"

# 3. 注解不是 64 位小写十六进制 → 红
mirror
sed -i 's|^\( *\)checksum/config: .*|\1checksum/config: latest|' "${work}/root/${main_single}"
out="$(run)"
ck "3. 指纹不是 sha256：红" 1 "$out" "$(rc)"
ck_has "3. 说清是形状不对" "64 位小写十六进制" "$out"

# 4. 同一形态里两条消费者的指纹不等 → 红
mirror
# 用带字母的值：全数字的十六进制会被 YAML 读成整数（64 个 0 ⇒ 0），那不是被测的形状。
other="$(printf 'a%.0s' $(seq 1 64))"
sed -i "s|^\( *\)checksum/config: .*|\1checksum/config: ${other}|" "${work}/root/${evolution_single}"
out="$(run)"
ck "4. 同一份配置两个指纹：红" 1 "$out" "$(rc)"
ck_has "4. 说清是同一份配置" "指纹不一致" "$out"

# 5. 一种形态里只剩一条消费者 → 红（下界是声明）
mirror
rm -f "${work}/root/${evolution_single}"
out="$(run)"
ck "5. 消费者只剩一条：红" 1 "$out" "$(rc)"
ck_has "5. 报出下界" "下界是 2" "$out"

# 6. 所有产物共用一个写死的指纹 → 红
mirror
frozen="$(printf 'f%.0s' $(seq 1 64))"
find "${work}/root/deploy/rendered" "${work}/root/deploy/k3s" -name '*.yaml' -print0 |
  while IFS= read -r -d '' f; do
    sed -i "s|^\( *\)checksum/config: .*|\1checksum/config: ${frozen}|" "$f"
  done
out="$(run)"
ck "6. 写死的字面量：红" 1 "$out" "$(rc)"
ck_has "6. 说清是全等" "写死的字面量" "$out"

printf '\n%s\n' "通过 ${pass}，失败 ${fail}"
[ "${fail}" -eq 0 ]
