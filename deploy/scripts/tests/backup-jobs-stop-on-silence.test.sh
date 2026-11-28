#!/usr/bin/env bash
# 备份与演练的两个 Job 判停读的是**还在不在推进**，不是跑了多久。
#
# 这两个 Job 以前各带一个 `activeDeadlineSeconds`：到点由 kubelet 把容器杀掉。
# 一个钟分不出「跑了很久但每一步都在写」与「一步卡住」——2026-10-27 的现场就是
# 后者被判成前者的反例：第一趟 2271 s 跑完，第二趟在 3600 s 的界上被杀，
# `backup_drills` 里一行都没有，于是那份包从此永远证明不了。判停现在是：
#
#   1. 容器每推进一步就写 `COGNEVA_PROGRESS_FILE`（内容是窗口秒数），
#   2. 容器上的 exec 存活探针读 `now - mtime` 与那个窗口比，
#   3. Job 上不再有 `activeDeadlineSeconds`。
#
# 这条门量四件事，缺一条就是旧的形状会重新长回来：
#
#   1. chart 渲染件里两个 CronJob 的容器都带存活探针与 COGNEVA_PROGRESS_FILE；
#   2. 静态清单 deploy/k3s/backup-cronjob.yaml 与 backup-drill-cronjob.yaml 同样有；
#   3. 两个 Job 上都没有 activeDeadlineSeconds（它杀的是时长，不是停滞）；
#   4. 进度文件落在挂进来的那一卷上（不落在容器的可写层：根文件系统只读时写不进去，
#      而探针会读到「没有文件」并一路判死）。
#
# 演练另有一条：单步静默界不在清单里。它是一个作业类量出来的极值（`progress_bounds`），
# 不是部署可调的数；部署里再出现那一格，就等于把「谁先挑的数」请回来判一件还在干的活。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
profile="${chart}/profiles/k3s-single.yaml"

command -v helm >/dev/null || { echo "缺少依赖：helm" >&2; exit 2; }

fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

rendered="${work}/jobs.yaml"
helm template cogneva "${chart}" \
  -f "${profile}" \
  --set resourceGovernance.enabled=false \
  --namespace cogneva > "${rendered}"

# 从一份清单里取某一个 CronJob 的三件事。渲染件是多文档、静态清单是单文档，
# 同一个读法都能吃。
inspect() {
  python3 - "$1" "$2" <<'PY'
import sys, yaml

path, want = sys.argv[1], sys.argv[2]
with open(path) as fh:
    for doc in yaml.safe_load_all(fh):
        if not isinstance(doc, dict) or doc.get("kind") != "CronJob":
            continue
        if doc.get("metadata", {}).get("name") != want:
            continue
        job = doc["spec"]["jobTemplate"]["spec"]
        deadline = job.get("activeDeadlineSeconds")
        container = job["template"]["spec"]["containers"][0]
        env = {e.get("name"): e.get("value") for e in container.get("env", [])}
        mounts = {m.get("name"): m.get("mountPath") for m in container.get("volumeMounts", [])}
        volumes = {v.get("name") for v in job["template"]["spec"].get("volumes", [])}
        probe = container.get("livenessProbe", {}).get("exec", {}).get("command", [])
        print(f"deadline={deadline}")
        print(f"progress_file={env.get('COGNEVA_PROGRESS_FILE', '')}")
        print(f"probe={probe[-1] if probe else ''}")
        print(f"mounts={','.join(f'{k}:{v}' for k, v in mounts.items())}")
        print(f"volumes={','.join(sorted(volumes))}")
        break
    else:
        sys.exit(f"{path} 里没有 CronJob/{want}")
PY
}

check() {
  local label="$1" path="$2" want="$3"
  local out deadline progress_file probe mounts volumes
  out="$(inspect "${path}" "${want}")" || fail "${label}：${out}"
  deadline="$(sed -n 's/^deadline=//p' <<<"${out}")"
  progress_file="$(sed -n 's/^progress_file=//p' <<<"${out}")"
  probe="$(sed -n 's/^probe=//p' <<<"${out}")"
  mounts="$(sed -n 's/^mounts=//p' <<<"${out}")"
  volumes="$(sed -n 's/^volumes=//p' <<<"${out}")"

  [[ "${deadline}" == "None" ]] \
    || fail "${label}：Job 上还有 activeDeadlineSeconds=${deadline}。它到点杀容器，不问那容器还在不在推进——判停要落在存活探针上"
  [[ -n "${progress_file}" ]] \
    || fail "${label}：容器没有 COGNEVA_PROGRESS_FILE。没有它，进程哪里也不写，探针读的是一条空路径"
  [[ "${probe}" == *COGNEVA_PROGRESS_FILE* ]] \
    || fail "${label}：容器没有读进度文件的存活探针（probe=${probe}）"

  # 进度文件必须落在挂进来的那一卷上：根文件系统只读时容器的可写层写不进去，
  # 而探针会把「写不进去」读成「一直没推进」，把每一趟都判死。
  local dir name
  dir="$(dirname "${progress_file}")"
  name="$(basename "${progress_file}")"
  [[ -n "${name}" && "${name}" != "." ]] || fail "${label}：进度文件路径没有文件名（${progress_file}）"
  [[ "${mounts}" == *":${dir}"* ]] \
    || fail "${label}：进度文件目录 ${dir} 没有挂进容器（mounts=${mounts}）"
  local mount_name
  mount_name="$(tr ',' '\n' <<<"${mounts}" | sed -n "s#^\([^:]*\):${dir}\$#\1#p")"
  [[ -n "${mount_name}" ]] || fail "${label}：${dir} 不是任何一卷的挂载点（mounts=${mounts}）"
  [[ ",${volumes}," == *",${mount_name},"* ]] \
    || fail "${label}：${dir} 挂在卷 ${mount_name} 上，但 Pod 没有声明它（volumes=${volumes}）"
}

check "chart 渲染件/备份" "${rendered}" "cogneva-backup"
check "chart 渲染件/演练" "${rendered}" "cogneva-backup-drill"
check "deploy/k3s 静态清单/备份" "${repo}/deploy/k3s/backup-cronjob.yaml" "cogneva-backup"
check "deploy/k3s 静态清单/演练" "${repo}/deploy/k3s/backup-drill-cronjob.yaml" "cogneva-backup-drill"

# 静默界不在部署里：它是一行实测（`progress_bounds`），不是一个部署可调的数。
#
# 这里曾经量的是「两份清单与代码默认值同值」。那个界 3600 s 是一颗挑出来的数：部署里
# 调它，等于让人用一个事先定的值决定一件可能还在干的活的死活。现在没有这一格可调——
# 作业类自己量出来的极值在库里，量不到就不判停。所以这条门改成量**它没有回来**：
# 两份清单里都不许再出现那个 env，`values.yaml` 里不许再有那一格。
for manifest in "${rendered}" "${repo}/deploy/k3s/backup-drill-cronjob.yaml"; do
  if grep -q 'COGNEVA_BACKUP_DRILL_STEP_SILENCE_SECS' "${manifest}"; then
    fail "${manifest}：单步静默界又长回清单里了。界是 progress_bounds 里的实测值，不是部署可调的数"
  fi
done
if grep -qE '^[[:space:]]*drillStepSilenceSeconds:' "${repo}/deploy/helm/cogneva/values.yaml"; then
  fail "values.yaml：drillStepSilenceSeconds 又长回来了。一个事先挑好的数不该决定一件还在干的活的死活"
fi

echo "OK：两个 Job 都没有 activeDeadlineSeconds，判停读进度文件；静默界是 progress_bounds 里的实测值，部署里没有这一格可调"
