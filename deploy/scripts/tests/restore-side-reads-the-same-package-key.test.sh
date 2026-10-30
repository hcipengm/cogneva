#!/usr/bin/env bash
# 包级口令的三个读者必须由同一个 values 键同时点亮，且指向同一个 Secret 的同一个键。
#
# 一个包被打开的地方有三处：写包（CronJob/cogneva-backup）、演练读包
# （CronJob/cogneva-backup-drill）、恢复读包（主 Deployment/cogneva —— `cogneva
# restore` 就在这个 Pod 里跑，它读的正是前两者用过的那块数据卷上的包）。
#
# 只点亮其中两处不会有任何症状：渲染通过、parity 通过、Pod 起得来、当晚备份照写。
# 症状只在「真的要拿一份密文包恢复」时才出现，而那一刻已经不能回头重写那份包了。
# 所以这条判据不是「有没有给口令」，而是**三处给的是不是同一份口令**——写成三个
# 各自可配的键，就等于允许它们各自漂走。
#
#   1. 默认值（未配 backup.packageKeySecret）：三处都不注入。留一个指不存在的
#      Secret 的引用会让 Pod 卡在 CreateContainerConfigError，而默认安装根本
#      不加密，那个引用没有任何读者。
#   2. 配了口令：三处都注入，且 secretKeyRef 的 name/key 三处一致。
#
# 这一条红的时候，症状在运行时：恢复命令报「是加密包，但没有 COGNEVA_BACKUP_PACKAGE_KEY」，
# 而包是昨晚刚写出来的，没有可替代的旧包。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
profile="${chart}/profiles/k3s-single.yaml"

command -v helm >/dev/null || { echo "缺少依赖：helm" >&2; exit 2; }

fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

render() {
  local out="$1"
  shift
  helm template cogneva "${chart}" \
    -f "${profile}" \
    --set resourceGovernance.enabled=false \
    --namespace cogneva \
    "$@" > "${out}"
}

# 印出「谁拿了这个口令、从哪儿拿的」：每行 `kind/name container=<c> secret=<s> key=<k>`。
# 按整份渲染扫而不是按名字找三个对象：漏掉一个读者正是这条判据要拦的形状。
package_key_refs() {
  python3 - "$1" <<'PY'
import sys, yaml

ENV = "COGNEVA_BACKUP_PACKAGE_KEY"
with open(sys.argv[1]) as fh:
    for doc in yaml.safe_load_all(fh):
        if not isinstance(doc, dict):
            continue
        kind = doc.get("kind")
        if kind not in ("Deployment", "CronJob", "Job", "StatefulSet", "DaemonSet"):
            continue
        name = (doc.get("metadata") or {}).get("name", "?")
        if kind == "CronJob":
            pod = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
        else:
            pod = doc["spec"]["template"]["spec"]
        for container in pod.get("containers", []):
            for entry in container.get("env", []) or []:
                if entry.get("name") != ENV:
                    continue
                ref = (entry.get("valueFrom") or {}).get("secretKeyRef") or {}
                print("%s/%s container=%s secret=%s key=%s" % (
                    kind, name, container.get("name", "?"),
                    ref.get("name", "-"), ref.get("key", "-")))
PY
}

default_render="${work}/default.yaml"
keyed_render="${work}/keyed.yaml"
render "${default_render}"
render "${keyed_render}" --set backup.packageKeySecret.name=cogneva-backup-package-key

# 1) 默认不加密 ⇒ 三处都不该带口令
refs="$(package_key_refs "${default_render}")" || fail "解析默认渲染件失败"
if [[ -n "${refs}" ]]; then
  fail "默认安装（backup.packageKeySecret.name 为空）仍注入了 COGNEVA_BACKUP_PACKAGE_KEY：$(tr '\n' ';' <<<"${refs}")——Secret 不存在时 Pod 起不来，而默认安装不加密，这个引用没有读者"
fi

# 2) 配了口令 ⇒ 三处读者都在，且是同一份
refs="$(package_key_refs "${keyed_render}")" || fail "解析配了口令的渲染件失败"
[[ -n "${refs}" ]] || fail "配了 backup.packageKeySecret 但整份渲染里没有任何工作负载拿到 COGNEVA_BACKUP_PACKAGE_KEY"

# 三个读者：写包、演练、恢复。恢复那一处是这条判据的重点——它离「包被写出来」
# 最远，最容易被漏掉。
for want in "CronJob/cogneva-backup" "CronJob/cogneva-backup-drill" "Deployment/cogneva"; do
  grep -q "^${want} " <<<"${refs}" \
    || fail "配了口令但 ${want} 没拿到 COGNEVA_BACKUP_PACKAGE_KEY（拿到的是：$(tr '\n' ';' <<<"${refs}")）：三处读者缺一处，缺的那一处会在最不该失败的时刻读不开包"
done

# 同一份口令：不是「都有口令」，而是「是同一把」
refs_body="$(sed 's/.* container=[^ ]* //' <<<"${refs}" | sort -u)"
if [[ "$(wc -l <<<"${refs_body}")" -ne 1 ]]; then
  fail "三个读者指向了不同的 Secret/键：$(tr '\n' ';' <<<"${refs}")——写包与读包各拿一把钥匙时，昨晚那份包今晚就打不开了"
fi
if grep -q 'secret=-\|key=-' <<<"${refs_body}"; then
  fail "注入了一个解析不出 secret 名字或键名的引用：${refs_body}——空名字渲染成空字符串，Pod 会卡在 CreateContainerConfigError"
fi

echo "OK：默认不注入；配了口令三处读者同时注入且指向同一 Secret 的同一键"
