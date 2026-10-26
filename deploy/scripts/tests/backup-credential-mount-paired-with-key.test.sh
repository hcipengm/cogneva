#!/usr/bin/env bash
# 备份 Pod 持有凭证这件事，只允许出现在「包是密文」那一形态里。
#
# 凭证持有面门禁（check-credential-holder-surface.sh）判的是默认渲染产物，而备份
# Job 的整对象挂载现在挂在一个 values 开关后面：默认安装不挂，配了包级口令才挂。
# 那条门禁看不见后者——它的输入是已提交的默认渲染件。这个盲区不能只写在注释里，
# 所以在这里用同一次渲染的两种取值把它量出来：
#
#   1. 默认值：CronJob/cogneva-backup 不挂 cogneva-secrets / cogneva-secrets-oob。
#      凭证只在安全网关里，是从「这个 Pod 碰不碰得到」这一层就开始算的；挂着一个
#      用不到的凭证卷，等于给每一条被注入的可能多备一份可读的原材料。
#   2. 配了 backup.packageKeySecret：挂载回来，而且同一个容器上必须有
#      COGNEVA_BACKUP_PACKAGE_KEY —— 否则「挂了就是密文」只是把两个开关分别打开，
#      它们之间没有判据连着，配错一半的情形（挂载有、口令没注入）会写出明文凭证包。
#   3. 演练 Job 两种取值下都不挂：它读的是包，不是凭证。
#
# 这一条红的时候，症状在运行时：备份卷与卷外副本里各躺一份能读文件就拿走全部平台
# token 的明文包，而渲染、parity、Pod 启动全都没有症状。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
profile="${chart}/profiles/k3s-single.yaml"

command -v helm >/dev/null || { echo "缺少依赖：helm" >&2; exit 2; }

fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 渲染件按文件走：一整份清单几百 KB，塞进 argv 会撞 ARG_MAX。
render() {
  local out="$1"
  shift
  helm template cogneva "${chart}" \
    -f "${profile}" \
    --set resourceGovernance.enabled=false \
    --namespace cogneva \
    "$@" > "${out}"
}

# 渲染件里某个 CronJob 的 Pod 模板：卷名列表 + 容器 env 名列表。
cronjob_pod() {
  local rendered="$1" name="$2"
  python3 - "$rendered" "$name" <<'PY'
import sys, yaml

path, want = sys.argv[1], sys.argv[2]
with open(path) as fh:
    for doc in yaml.safe_load_all(fh):
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") != "CronJob" or doc.get("metadata", {}).get("name") != want:
            continue
        pod = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
        volumes = sorted(v["name"] for v in pod.get("volumes", []))
        env = sorted(e["name"] for c in pod["containers"] for e in c.get("env", []))
        print("volumes=" + ",".join(volumes))
        print("env=" + ",".join(env))
        break
    else:
        sys.exit(f"渲染件里没有 CronJob/{want}")
PY
}

credential_volumes_present() {
  local volumes="$1"
  [[ ",${volumes}," == *",secrets,"* || ",${volumes}," == *",secrets-oob,"* ]]
}

default_render="${work}/default.yaml"
keyed_render="${work}/keyed.yaml"
render "${default_render}"
render "${keyed_render}" --set backup.packageKeySecret.name=cogneva-backup-package-key

# 1) 默认：备份 Job 不碰两片凭证
out="$(cronjob_pod "${default_render}" cogneva-backup)" || fail "${out}"
volumes="$(sed -n 's/^volumes=//p' <<<"${out}")"
if credential_volumes_present "${volumes}"; then
  fail "默认安装里 CronJob/cogneva-backup 挂着凭证卷（volumes=${volumes}）：没有包级口令时它根本不会读这两片，挂着只是给这条路径备了原材料"
fi

# 2) 配了口令：挂载回来，且口令注入在同一个容器上
out="$(cronjob_pod "${keyed_render}" cogneva-backup)" || fail "${out}"
volumes="$(sed -n 's/^volumes=//p' <<<"${out}")"
env="$(sed -n 's/^env=//p' <<<"${out}")"
credential_volumes_present "${volumes}" \
  || fail "配了 backup.packageKeySecret 之后备份 Job 仍不挂凭证卷（volumes=${volumes}）：导出端会把两片都记成有损面，换机后要回平台后台重新签发一遍"
for needed in secrets secrets-oob; do
  [[ ",${volumes}," == *",${needed},"* ]] \
    || fail "配了口令但少了 ${needed} 卷（volumes=${volumes}）：两片要一起导出，少一片就等于那一半凭证换机后丢失"
done
[[ ",${env}," == *",COGNEVA_BACKUP_PACKAGE_KEY,"* ]] \
  || fail "备份 Job 挂着凭证卷却没拿到 COGNEVA_BACKUP_PACKAGE_KEY（env=${env}）：写端会照写明文包，而那两片清单正躺在包里"

# 3) 演练 Job 两种取值下都不碰凭证：它读的是包
for pair in "默认:${default_render}" "配了口令:${keyed_render}"; do
  label="${pair%%:*}"
  rendered="${pair#*:}"
  out="$(cronjob_pod "${rendered}" cogneva-backup-drill)" || fail "${label}：${out}"
  volumes="$(sed -n 's/^volumes=//p' <<<"${out}")"
  if credential_volumes_present "${volumes}"; then
    fail "${label}：CronJob/cogneva-backup-drill 挂了凭证卷（volumes=${volumes}）：演练只需要读包，不需要平台 token 与 git 私钥"
  fi
done

echo "OK：默认不挂、配了口令才挂且同容器带口令、演练始终不挂"
