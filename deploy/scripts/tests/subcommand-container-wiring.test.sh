#!/usr/bin/env bash
# Deterministic gate for "a container that is supposed to run one subcommand runs
# the whole application instead".
#
# The failure this pins is silent by construction: the image entry point with no
# arguments starts the full application, and a sidecar that was meant to serve one
# narrow mode then dies during assembly -- no Redis, no config map in that pod --
# and CrashLoops. Nothing about the manifest looks wrong; the container has the
# right image, the right env, the right probes. It was measured live on
# 2026-09-27: the registry's volume-walker sidecar had been CrashLooping for hours
# with `Redis URL did not parse`, so the volume-occupancy series that the reclaim
# logic reads never appeared at all.
#
# The judgement is name-based on purpose. A container named after a subcommand
# (`volume-walker`, `sandbox-executor`, `security-gateway`) exists in this
# topology for exactly one reason, so its argv has to say so. Checking "every
# cogneva-image container has a command" would be wrong -- the four main workloads
# are the full application and legitimately carry none -- and checking a hand
# written list of container names would go stale the moment a sidecar is added.
#
# Both surfaces are read: the static bootstrap manifests (what a from-scratch
# cluster applies) and the chart render, plus every committed rendered profile,
# because the GitOps path applies the rendered copies without ever reading either
# of the first two.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
fail() { echo "FAIL: $*"; exit 1; }

command -v helm >/dev/null || { echo "missing dependency: helm" >&2; exit 2; }
command -v python3 >/dev/null || { echo "missing dependency: python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

helm template cogneva "${chart}" --set secrets.create=false > "${work}/chart.yaml"

# The subcommand names are the ones the binary actually dispatches on
# (crates/cogneva/src/cli.rs SUBCOMMANDS); reading them from there keeps this gate
# from carrying a second copy of the vocabulary.
mapfile -t declared < <(python3 - "${repo}" <<'PYEOF'
import re, sys
src = open(f"{sys.argv[1]}/crates/cogneva/src/cli.rs").read()
block = re.search(r"const SUBCOMMANDS: \[\(&str, Command\); \d+\] = \[(.*?)\];", src, re.S).group(1)
for name in re.findall(r'\("([a-z0-9-]+)"', block):
    print(name)
PYEOF
)
[ "${#declared[@]}" -gt 0 ] || fail "no subcommands parsed out of cli.rs (the parser stopped matching)"
echo "subcommands: ${declared[*]}"

check_file() {
  python3 - "${1}" "${declared[@]}" <<'PYEOF'
import sys, yaml

path, names = sys.argv[1], sys.argv[2:]
docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]

def specs(doc):
    if doc["kind"] == "CronJob":
        yield doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    elif "template" in doc.get("spec", {}):
        yield doc["spec"]["template"]["spec"]

bad = []
seen = 0
for d in docs:
    for spec in specs(d):
        for c in (spec.get("containers") or []) + (spec.get("initContainers") or []):
            if c["name"] not in names:
                continue
            seen += 1
            argv = list(c.get("command") or []) + list(c.get("args") or [])
            if c["name"] not in argv:
                bad.append(f"{d['metadata']['name']}/{c['name']}: argv={argv} "
                           f"does not carry the '{c['name']}' subcommand")

# A container that matches by name but never appears would mean this gate stopped
# looking (renamed container, renamed file); that is the same silent pass as the
# bug it exists to catch.
if seen == 0:
    sys.exit(f"no container named after a subcommand found in {path}: the gate looked at nothing")

if bad:
    sys.exit("containers that would boot the full application:\n  " + "\n  ".join(bad))
print(f"OK: {path.split('/')[-1]} ({seen} subcommand container(s))")
PYEOF
}

check_file "${repo}/deploy/k3s/cluster-registry.yaml"
check_file "${repo}/deploy/k3s/gateway-deployment.yaml"
check_file "${repo}/deploy/k3s/sandbox-executor-deployment.yaml"
check_file "${work}/chart.yaml"
for profile in k3s-single k3s-multi k8s-standard; do
  check_file "${repo}/deploy/rendered/${profile}/41-deployment-cogneva-registry.yaml"
  check_file "${repo}/deploy/rendered/${profile}/41-deployment-cogneva-security-gateway.yaml"
  check_file "${repo}/deploy/rendered/${profile}/41-deployment-cogneva-sandbox-executor.yaml"
done

# Reverse control: the same reading has to fail on a manifest where the argv was
# taken away, otherwise "OK" only means "the file parsed".
python3 - "${repo}/deploy/k3s/cluster-registry.yaml" "${work}/broken.yaml" <<'PYEOF'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d and d.get("kind")]
for d in docs:
    if "template" not in d.get("spec", {}):
        continue
    for c in d["spec"]["template"]["spec"].get("containers", []):
        if c["name"] == "volume-walker":
            c.pop("command", None)
            c.pop("args", None)
yaml.safe_dump_all(docs, open(sys.argv[2], "w"))
PYEOF
if check_file "${work}/broken.yaml" >/dev/null 2>&1; then
  fail "the gate passes a manifest whose walker argv was removed: it is not reading the argv"
fi
echo "OK: subcommand container wiring gate"
