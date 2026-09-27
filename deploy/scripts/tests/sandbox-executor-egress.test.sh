#!/usr/bin/env bash
# Deterministic gate for the sandbox executor's egress face.
#
# The property this pins is not "a policy exists" but "the passthrough LLM port is
# not on this pod's reachable set". That property is why the host-document caller
# pipeline can be trusted to send bodies only through the audited channel: if the
# passthrough port were reachable, the choice would be a line of code that someone
# can get wrong, and nothing would say so.
#
# So the gate reads the rendered policies that select the executor, unions the
# ports they allow, and asserts the union is exactly the declared set. It is a
# closed-set assertion on purpose: adding a port has to change this file too,
# which is the moment a reviewer sees the widening.
#
# It reads the static bootstrap baseline as well, because that is the manifest a
# from-scratch cluster actually applies -- a chart-only fix would leave bootstrap
# with the old, open policy.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
static_policy="${repo}/deploy/k3s/network-policy.yaml"
fail() { echo "FAIL: $*"; exit 1; }

command -v helm >/dev/null || { echo "missing dependency: helm" >&2; exit 2; }
command -v python3 >/dev/null || { echo "missing dependency: python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# The audited port is rendered, not hardcoded: the caller side and the policy side
# read the same value, so it is taken from the render here too -- change the port
# and the policy follows (getting it wrong is what fails).
audited_port=8083
helm template cogneva "${chart}" --set secrets.create=false \
  --set securityGateway.auditedLlmPort="${audited_port}" > "${work}/rendered.yaml"

check_file() {
  python3 - "${1}" "${audited_port}" <<'PYEOF'
import sys, yaml

path, audited = sys.argv[1], int(sys.argv[2])
docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]

# This pod can be selected by several policies at once. NetworkPolicy is a union,
# so checking "that one executor policy" would be quietly bypassed by a second one:
# the judgement has to land on the pod, not on a policy name.
selected = []
for d in docs:
    if d.get("kind") != "NetworkPolicy":
        continue
    sel = (d["spec"].get("podSelector") or {}).get("matchLabels") or {}
    if sel.get("app.kubernetes.io/component") == "sandbox-executor":
        selected.append(d)

if not selected:
    sys.exit("no NetworkPolicy selects sandbox-executor")

ports, protos = set(), set()
for d in selected:
    for rule in d["spec"].get("egress") or []:
        for p in rule.get("ports") or []:
            ports.add(int(p["port"]))
            protos.add(p.get("protocol", "TCP"))

expected = {audited, 53, 443}
if ports != expected:
    extra = sorted(ports - expected)
    missing = sorted(expected - ports)
    sys.exit(f"executor egress port set differs from the declaration: extra {extra}, missing {missing}")

# The passthrough ports (8080/8081) and the webhook (8082) have to stay outside the
# set. The union assertion above already implies that, but this one says it by name:
# on the day one of them is put back, the failure should name the port instead of
# leaving it to be inferred from a port diff.
for forbidden in (8080, 8081, 8082):
    if forbidden in ports:
        sys.exit(f"the executor can reach {forbidden}: a body passthrough port is back on the reachable set")

if "UDP" not in protos:
    sys.exit("no UDP 53: DNS does not resolve, so the seed repositories cannot be fetched")

print(f"OK: executor egress = {sorted(ports)} (UDP+TCP), audited={audited}")
PYEOF
}

echo "-- chart render"
check_file "${work}/rendered.yaml"
echo "-- static bootstrap baseline"
check_file "${static_policy}"

# On this point the static baseline and the chart render have to yield the same port
# set, otherwise a bootstrap-installed cluster and the chart describe different bounds.
grep -q "port: ${audited_port}" "${static_policy}" \
  || fail "the static baseline carries no audited channel port ${audited_port}"
echo "OK: sandbox executor egress gate"
