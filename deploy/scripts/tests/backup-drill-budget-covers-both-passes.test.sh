#!/usr/bin/env bash
# 演练的两个界是一对：单趟预算（进程自己的）与 Job 的 activeDeadlineSeconds。
#
# 一次演练开两份包——最新那份，加上窗口里最久没被证明的那份。账户上能读出
# 「这一趟跑了多久」的只有进程自己：Job 的界到点是把 Pod 杀掉，杀掉的进程写不出
# 那一行，`backup_drills` 里于是停在上一晚的记录上，「这一趟被杀」与「这一趟没跑过」
# 同形。2026-10-27 就是这个形状：第一趟 2271 s 跑完，第二趟在 3600 s 的 Job 界上被
# 杀，没有任何一行——于是那份包永远证明不了，而报警规则只能一直报「未证明」，
# 说不出为什么。
#
# 这条门量三件事，缺一条就是上面那个形状会重新长出来：
#
#   1. 渲染件里演练容器带着 COGNEVA_BACKUP_DRILL_PASS_BUDGET_SECS（没有它，进程
#      拿的是代码里的默认值，部署清单与账上的判词就不再是同一个数）。
#   2. 静态清单 deploy/k3s/backup-drill-cronjob.yaml 同样带它，且两份的界同值
#      （静态清单是集群内 GitOps 拉取端的消费物，parity 之外这里再钉一次关系）。
#   3. activeDeadlineSeconds ≥ 2 × 单趟预算 ＋ 余量。只改一个数会红——这正是当初
#      失配的原因：库在长，单趟从 497 s 长到 2271 s，而界一直停在 3600。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
profile="${chart}/profiles/k3s-single.yaml"
static="${repo}/deploy/k3s/backup-drill-cronjob.yaml"

# 两趟之外的余量：解包、目录遍历、写账行这些不进单趟预算的开销。
slack=600

command -v helm >/dev/null || { echo "缺少依赖：helm" >&2; exit 2; }

fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

rendered="${work}/drill.yaml"
helm template cogneva "${chart}" \
  -f "${profile}" \
  --set resourceGovernance.enabled=false \
  --namespace cogneva > "${rendered}"

# 从一份清单里取演练 CronJob 的两个数。静态清单是单文档 YAML，渲染件是多文档，
# 同一个读法都能吃。输出两行：deadline=… / budget=…
drill_pair() {
  python3 - "$1" <<'PY'
import sys, yaml

with open(sys.argv[1]) as fh:
    for doc in yaml.safe_load_all(fh):
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") != "CronJob":
            continue
        if doc.get("metadata", {}).get("name") != "cogneva-backup-drill":
            continue
        job = doc["spec"]["jobTemplate"]["spec"]
        deadline = job.get("activeDeadlineSeconds")
        budgets = [
            e.get("value")
            for c in job["template"]["spec"]["containers"]
            for e in c.get("env", [])
            if e.get("name") == "COGNEVA_BACKUP_DRILL_PASS_BUDGET_SECS"
        ]
        print(f"deadline={deadline}")
        print(f"budget={budgets[0] if budgets else ''}")
        break
    else:
        sys.exit("清单里没有 CronJob/cogneva-backup-drill")
PY
}

check_pair() {
  local label="$1" path="$2"
  local out deadline budget
  out="$(drill_pair "${path}")" || fail "${label}：${out}"
  deadline="$(sed -n 's/^deadline=//p' <<<"${out}")"
  budget="$(sed -n 's/^budget=//p' <<<"${out}")"

  [[ -n "${budget}" ]] \
    || fail "${label}：演练容器没有 COGNEVA_BACKUP_DRILL_PASS_BUDGET_SECS。界只有 Job 那一头时，超时的那一趟是被杀掉的，账上一行都不留"
  [[ "${budget}" =~ ^[0-9]+$ && "${budget}" -gt 0 ]] \
    || fail "${label}：单趟预算不是正整数（budget=${budget}）"
  [[ "${deadline}" =~ ^[0-9]+$ ]] \
    || fail "${label}：activeDeadlineSeconds 不是数（deadline=${deadline}）"

  local need=$(( budget * 2 + slack ))
  [[ "${deadline}" -ge "${need}" ]] \
    || fail "${label}：Job 的界 ${deadline}s 盖不住两趟（单趟预算 ${budget}s × 2 ＋ 余量 ${slack}s ＝ ${need}s）。第二趟会在界上被杀，账上一行都不留，那份包从此永远证明不了"

  echo "${deadline} ${budget}"
}

read -r chart_deadline chart_budget <<<"$(check_pair "chart 渲染件" "${rendered}")"
read -r static_deadline static_budget <<<"$(check_pair "deploy/k3s 静态清单" "${static}")"

# 两份清单是同一次部署的两个载体：一份给 helm 投递，一份给 GitOps 拉取端。
# 数不同意味着「用 helm 装」与「用 apply 装」的演练有两种命运。
[[ "${chart_deadline}" == "${static_deadline}" ]] \
  || fail "两份清单的 activeDeadlineSeconds 不同（chart=${chart_deadline} static=${static_deadline}）"
[[ "${chart_budget}" == "${static_budget}" ]] \
  || fail "两份清单的单趟预算不同（chart=${chart_budget} static=${static_budget}）"

# 代码里的默认值也必须与清单同值：容器没拿到 env 时（老清单、手工 create job）
# 进程拿默认值跑，那一趟的判词会写出另一个界，与部署清单对不上。
code_default="$(sed -n 's/^const DRILL_PASS_BUDGET_DEFAULT_SECS: u64 = \([0-9]*\);$/\1/p' \
  "${repo}/crates/cogneva/src/backup.rs")"
[[ -n "${code_default}" ]] \
  || fail "crates/cogneva/src/backup.rs 里找不到 DRILL_PASS_BUDGET_DEFAULT_SECS"
[[ "${code_default}" == "${chart_budget}" ]] \
  || fail "代码默认单趟预算 ${code_default}s 与清单 ${chart_budget}s 不同：没注入 env 的那一趟会写出另一个界"

echo "OK：界 ${chart_deadline}s ≥ 2 × ${chart_budget}s ＋ ${slack}s，两份清单与代码默认值同源"
