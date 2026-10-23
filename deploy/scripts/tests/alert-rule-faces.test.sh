#!/usr/bin/env bash
# check-alert-rule-faces.sh 的定向测试：四件事各造一份真清单的副本，只隔离一件事。
#
# 为什么要有这份测试：这条门只有两种判词，而两种都可能**静默失效**——把 `labels:` 的
# 解析缩进写错，它会一条标签都读不到，于是「都申报了面」照样印得出来（读不到不是通过，
# 是没看）；把 `- alert:` 的识别写窄，清单里改个写法它就漏数。所以每个方向都断言**具体
# 那句判词**与**点到的名字**，不只断言退出码。
#
#   1. 真清单 -> 绿，且条数由清单自己算（不钉字面量）；
#   2. 一条托管规则摘掉面标签 -> 红，并点名那条规则；
#   3. 一条不投递的规则挂着面标签 -> 红（面标签没有读者）；
#   4. 一份清单里一条告警都没有 -> 退 2（没看到不等于看到了）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-alert-rule-faces.sh"
manifests="${repo}/deploy/k3s/observability/manifests"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { bash "${gate}" "$1" 2>&1; }

# 1. 真清单：绿，而且条数由清单自己数出来。
python3 - "${manifests}" "${work}/expected" <<'PYEOF'
import os, sys
count = 0
for name in sorted(os.listdir(sys.argv[1])):
    if name.endswith(('.yaml', '.yml')):
        text = open(os.path.join(sys.argv[1], name), encoding='utf-8').read()
        for doc in text.split('\n---\n'):
            if 'kind: PrometheusRule' in doc:
                count += sum(1 for l in doc.split('\n') if l.strip().startswith('- alert:'))
open(sys.argv[2], 'w', encoding='utf-8').write(str(count))
PYEOF
expected="$(cat "${work}/expected")"
[ "${expected}" -gt 0 ] || fail "真清单里一条告警都没数到，这份测试自己没在看东西"
out="$(run "${manifests}")" || fail "真清单判红了：${out}"
grep -q "${expected} 条托管告警规则都申报了面" <<<"${out}" \
  || fail "真清单的判词条数不对（应为 ${expected}）：${out}"

# 2. 一条托管规则摘掉面标签 -> 红，点名它。
cp -r "${manifests}" "${work}/m-missing"
victim="CognevaAlertMirrorMissing"
python3 - "${work}/m-missing/15-prometheusrule-host-readings.yaml" "${victim}" <<'PYEOF'
import sys
path, alert = sys.argv[1], sys.argv[2]
lines = open(path, encoding='utf-8').read().split('\n')
out, inside = [], False
for line in lines:
    stripped = line.strip()
    if stripped.startswith('- alert:'):
        inside = stripped[len('- alert:'):].strip() == alert
    if inside and stripped.startswith('cogneva_io_face:'):
        continue
    out.append(line)
open(path, 'w', encoding='utf-8').write('\n'.join(out))
PYEOF
if out="$(run "${work}/m-missing")"; then
  fail "摘掉面标签之后仍判绿：${out}"; fi
grep -q "${victim}" <<<"${out}" || fail "红的时候没点名具体规则：${out}"
grep -q '没有任何面的读法答得出该谁动' <<<"${out}" || fail "缺面的判词不对：${out}"

# 3. 一条不投递的规则挂面标签 -> 红（面标签没有读者）。
cp -r "${manifests}" "${work}/m-orphan"
cat >>"${work}/m-orphan/15-prometheusrule-host-readings.yaml" <<'YAML'

---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: cogneva-unmanaged-specimen
  namespace: monitoring
spec:
  groups:
    - name: cogneva-unmanaged-specimen
      rules:
        - alert: CognevaUnmanagedSpecimen
          expr: vector(1)
          labels:
            cogneva_io_face: "资源管理"
            severity: warning
YAML
if out="$(run "${work}/m-orphan")"; then
  fail "不投递却挂面标签仍判绿：${out}"; fi
grep -q 'CognevaUnmanagedSpecimen' <<<"${out}" || fail "没点名那条孤儿面标签：${out}"
grep -q '没有任何读者' <<<"${out}" || fail "孤儿面标签的判词不对：${out}"

# 4. 一份清单里一条告警都没有 -> 退 2（没看到不是通过）。
mkdir -p "${work}/m-empty"
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n' \
  >"${work}/m-empty/empty.yaml"
set +e
bash "${gate}" "${work}/m-empty" >/dev/null 2>&1
rc=$?
set -e
[ "${rc}" -eq 2 ] || fail "空清单的退出码是 ${rc}，应为 2"

# 目录不存在也是依赖错误，不是判词。
set +e
bash "${gate}" "${work}/does-not-exist" >/dev/null 2>&1
rc=$?
set -e
[ "${rc}" -eq 2 ] || fail "目录不存在时的退出码是 ${rc}，应为 2"

echo "ALERT RULE FACE GATE TESTS OK"
