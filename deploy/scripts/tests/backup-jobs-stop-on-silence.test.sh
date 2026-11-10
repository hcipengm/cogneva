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
# 演练另有一条：单步静默界在清单与代码默认值里是同一个数。容器没拿到 env 时进程
# 拿默认值跑，那一趟的判词会写出另一个界。
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
        print(f"step_silence={env.get('COGNEVA_BACKUP_DRILL_STEP_SILENCE_SECS', '')}")
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

# 单步静默界：两份清单与代码默认值必须同源。容器没拿到 env 时进程拿默认值跑，
# 那一趟的判词会写出一个与部署清单不同的界，读的人只能去翻代码。
chart_silence="$(inspect "${rendered}" cogneva-backup-drill | sed -n 's/^step_silence=//p')"
static_silence="$(inspect "${repo}/deploy/k3s/backup-drill-cronjob.yaml" cogneva-backup-drill | sed -n 's/^step_silence=//p')"
[[ -n "${chart_silence}" ]] \
  || fail "chart 渲染件：演练容器没有 COGNEVA_BACKUP_DRILL_STEP_SILENCE_SECS"
[[ "${chart_silence}" == "${static_silence}" ]] \
  || fail "两份清单的单步静默界不同（chart=${chart_silence} static=${static_silence}）"

code_default="$(sed -n 's/^const DRILL_STEP_SILENCE_DEFAULT_SECS: u64 = \([0-9]*\);$/\1/p' \
  "${repo}/crates/cogneva/src/backup.rs")"
[[ -n "${code_default}" ]] \
  || fail "crates/cogneva/src/backup.rs 里找不到 DRILL_STEP_SILENCE_DEFAULT_SECS"
[[ "${code_default}" == "${chart_silence}" ]] \
  || fail "代码默认单步静默界 ${code_default}s 与清单 ${chart_silence}s 不同：没注入 env 的那一趟会写出另一个界"

echo "OK：两个 Job 都没有 activeDeadlineSeconds，判停读进度文件；单步静默界 ${chart_silence}s 在两份清单与代码里同值"
