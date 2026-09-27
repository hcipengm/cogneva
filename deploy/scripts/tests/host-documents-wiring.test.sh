#!/usr/bin/env bash
# Deterministic gate for host document access.
#
# The capability mounts a host directory into the sandbox executor, so two
# failures matter more than the happy path:
#   1. a scope that is configured but never mounted (or mounted but never
#      exported to the executor) — the operator believes document access is on
#      while every request answers "not configured";
#   2. a mount that appears with no scope behind it — an executor that can
#      reach a host directory nobody declared.
# Neither is visible from the values file alone, so the gate renders the chart
# in both states and reads the manifest it produces.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
fail() { echo "FAIL: $*"; exit 1; }

render() {
  helm template cogneva "${chart}" --set secrets.create=false "$@"
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# --- 1) default: the capability is off, and provably so --------------------
default_out="$(render)"
if grep -q "HOST_DOCS_SCOPES" <<<"${default_out}"; then
  fail "the default values still hand HOST_DOCS_SCOPES to the executor; the capability must be off"
fi
if grep -q "host-docs-" <<<"${default_out}"; then
  fail "the default render mounts a host directory; nothing from the host may be mounted by default"
fi

# --- 2) configured scope: env, mount and volume agree ----------------------
enabled_out="$(render --set hostDocuments.scopes.alice=/srv/alice/Documents)"
mount_expected="/opt/cogneva/host-docs/alice"
grep -q "value: \"alice=${mount_expected}\"" <<<"${enabled_out}" \
  || fail "HOST_DOCS_SCOPES does not map the scope name to its mount point"
grep -q "mountPath: ${mount_expected}" <<<"${enabled_out}" \
  || fail "a scope is declared with no matching volumeMount"
grep -q "path: \"/srv/alice/Documents\"" <<<"${enabled_out}" \
  || fail "the host path does not appear in volumes"
# DirectoryOrCreate would let a typo create an empty directory on the host.
# Read the type from the host-docs volume itself: the chart uses
# DirectoryOrCreate elsewhere (buildah storage) and a whole-file grep would
# report that one instead.
host_volume_type() {
  awk '/name: host-docs-/{ hit = NR } hit && NR <= hit + 5 && /type:/ { print $2; exit }' "$1"
}
render --set hostDocuments.scopes.alice=/srv/alice/Documents > "${work}/render.yaml"
got_type="$(host_volume_type "${work}/render.yaml")"
[ "${got_type}" = "Directory" ] \
  || fail "the host-docs volume type is ${got_type:-missing}; it must be Directory (DirectoryOrCreate silently creates an empty directory on the host)"
# Negative control: the extractor has to answer "DirectoryOrCreate" when that
# is what the file says, otherwise the assertion above proves nothing.
sed 's/type: Directory$/type: DirectoryOrCreate/' "${work}/render.yaml" > "${work}/mutated.yaml"
if [ "$(host_volume_type "${work}/mutated.yaml")" != "DirectoryOrCreate" ]; then
  fail "self-check failed: the extractor cannot read the manifest that was changed to DirectoryOrCreate"
fi

# --- 3) a scope entry with no path is a render error, not a silent skip ----
set +e
out="$(render --set hostDocuments.scopes.alice= 2>&1)"
rc=$?
set -e
[ "${rc}" -ne 0 ] || fail "a scope entry with an empty path still rendered; it must fail rather than skip that scope"
grep -q "hostDocuments.scopes.alice has no host path" <<<"${out}" \
  || fail "rejected, but not for the empty path (wrong reason): ${out}"

# --- 4) the executor reads every key the template sets --------------------
executor_rs="${repo}/crates/cog-extension/src/hostdocs.rs"
[ -f "${executor_rs}" ] || fail "cannot find ${executor_rs}"
for env_name in HOST_DOCS_SCOPES HOST_DOCS_JOURNAL_DIR HOST_DOCS_MAX_WRITE_BYTES \
  HOST_DOCS_MAX_READ_BYTES HOST_DOCS_APPROVAL_TTL_SECS HOST_DOCS_MAX_PLAN_BYTES; do
  grep -q "\"${env_name}\"" "${executor_rs}" \
    || fail "${env_name} is carried by the template but the executor never reads it (dead knob)"
done
# The journal has to outlive the process that wrote it, otherwise a rollback
# after a restart restores nothing: the default path must be on the executor
# volume, which the pod mounts at /opt/cogneva/sandbox.
grep -q 'const DEFAULT_JOURNAL_DIR: &str = "/opt/cogneva/sandbox' "${executor_rs}" \
  || fail "the rollback journal defaults off the executor volume; a restart would lose it"

# --- 5) the egress switch: off by default, carried to the side that can refuse --
# The switch has a second half that no single side can check: the manifest decides
# whether it is on, and the gateway -- the only process that can refuse a request
# -- is what has to read it. Either half alone reads as "the capability is off".
#
# Everything below reads the **rendered YAML as objects**, not as text: the
# template carries comments that quote the very shapes being asserted (the float
# form of the byte bound), and a text match would find the warning instead of the
# value.
gateway_rs="${repo}/crates/cog-gateway/src/security_gateway.rs"
egress_rs="${repo}/crates/cog-gateway/src/document_egress.rs"
[ -f "${egress_rs}" ] || fail "cannot find ${egress_rs} (the audited channel's judgement module)"

# The env value a named container of a named workload carries; empty when absent.
env_value() {
  python3 - "$1" "$2" "$3" "$4" <<'PYEOF'
import sys, yaml
path, workload, container, env = sys.argv[1:5]
for doc in yaml.safe_load_all(open(path)):
    if not doc or doc.get('kind') != 'Deployment' or doc['metadata']['name'] != workload:
        continue
    for c in doc['spec']['template']['spec']['containers']:
        if c['name'] != container:
            continue
        for e in c.get('env', []):
            if e['name'] == env:
                print(e.get('value', ''))
                sys.exit(0)
PYEOF
}

render > "${work}/default.yaml"
grep -q 'name: HOST_DOCS_BODY_EGRESS_ENABLED' "${work}/default.yaml" \
  || fail "the default render has no HOST_DOCS_BODY_EGRESS_ENABLED; the switch has no carrier at all"
got="$(env_value "${work}/default.yaml" cogneva-security-gateway security-gateway HOST_DOCS_BODY_EGRESS_ENABLED)"
[ "${got}" = "false" ] || fail "the switch reads ${got:-missing} in the default render; it must default to off"
# Negative control: turn it on and the same reading must flip. Without this a
# template hardcoding "false" passes the line above.
render --set hostDocuments.bodyEgress=true > "${work}/on.yaml"
got="$(env_value "${work}/on.yaml" cogneva-security-gateway security-gateway HOST_DOCS_BODY_EGRESS_ENABLED)"
[ "${got}" = "true" ] || fail "self-check failed: hostDocuments.bodyEgress=true still reads ${got:-missing}"

# The switch's name and reading live once, in the shared contract module: the
# gateway refuses on it and the executor gates its body reads on it, so a second
# definition would drift into "one side open, the other closed" — a state each
# side's own reading reports as "the capability is off".
core_rs="${repo}/crates/cog-core/src/contract/host_documents.rs"
[ -f "${core_rs}" ] || fail "cannot find ${core_rs} (the switch's shared definition)"
for env_name in HOST_DOCS_BODY_EGRESS_ENABLED COGNEVA_SG_AUDITED_LLM_PORT COGNEVA_SG_AUDITED_MAX_BODY_BYTES; do
  grep -qh "\"${env_name}\"" "${gateway_rs}" "${egress_rs}" "${core_rs}" \
    || fail "${env_name} is carried by the manifest but the gateway never reads it (dead knob)"
done
grep -q "cog_core::host_documents" "${egress_rs}" \
  || fail "the gateway does not reference the shared contract's switch definition; a second copy drifts into one side open, the other closed"
grep -q "cog_core::host_documents" "${executor_rs}" \
  || fail "the executor does not reference the shared contract's switch definition; its read path would then judge separately from the audited channel"
# And neither side may carry a second declaration of that name.
for side in "${egress_rs}" "${executor_rs}"; do
  if grep -q "const BODY_EGRESS_ENV" "${side}"; then
    fail "${side} declares a second switch constant; the switch may have exactly one definition"
  fi
done
# The body bound is a byte count. Values numbers arrive as float64, so a missing
# `| int64` renders 8388608 as "8.388608e+06": unparseable, and the process then
# silently falls back to its default -- the declaration looks set and is not.
got="$(env_value "${work}/default.yaml" cogneva-security-gateway security-gateway COGNEVA_SG_AUDITED_MAX_BODY_BYTES)"
case "${got}" in
  ''|*[!0-9]*) fail "COGNEVA_SG_AUDITED_MAX_BODY_BYTES reads '${got}'; it must be a decimal integer byte count" ;;
esac
# Same trap on the executor's side, in the same capability: it is only reachable
# once scopes are configured, which is exactly when the value matters. The
# approval window is a count too, in its own unit -- a float-form window would
# fall back to the default and every plan would look stale (or never stale).
render --set hostDocuments.scopes.alice=/srv/alice/Documents > "${work}/scopes.yaml"
for byte_env in HOST_DOCS_MAX_WRITE_BYTES HOST_DOCS_MAX_READ_BYTES HOST_DOCS_MAX_PLAN_BYTES; do
  got="$(env_value "${work}/scopes.yaml" cogneva-sandbox-executor sandbox-executor "${byte_env}")"
  case "${got}" in
    ''|*[!0-9]*) fail "${byte_env} reads '${got}'; it must be a decimal integer byte count" ;;
  esac
done
got="$(env_value "${work}/scopes.yaml" cogneva-sandbox-executor sandbox-executor HOST_DOCS_APPROVAL_TTL_SECS)"
case "${got}" in
  ''|*[!0-9]*) fail "HOST_DOCS_APPROVAL_TTL_SECS reads '${got}'; it must be a decimal integer second count" ;;
esac

# --- 6) reachability: the audited port reaches exactly one workload ----------
# The whole reason this channel is a separate port is that its reachable surface
# is small. A port that ends up on the shared Service, or on the gateway's
# `podSelector: {}` ingress rule, is back to being callable by every pod in the
# namespace -- and it would still pass every other check in this file.
python3 - "${work}/default.yaml" <<'PYEOF' || exit 1
import sys, yaml

docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d and d.get('kind')]
by_name = {(d['kind'], d['metadata']['name']): d for d in docs}
fail = []

gw = by_name.get(('Deployment', 'cogneva-security-gateway'))
if gw is None:
    fail.append("the render carries no gateway Deployment")
else:
    ports = {
        p['name']: p['containerPort']
        for p in gw['spec']['template']['spec']['containers'][0].get('ports', [])
    }
    if ports.get('audited-llm') != 8083:
        fail.append(f"the gateway container does not expose audited-llm:8083 (read {ports.get('audited-llm')})")

shared = by_name.get(('Service', 'cogneva-security-gateway'))
if shared is None:
    fail.append("no shared gateway Service")
elif any(p['name'] == 'audited-llm' for p in shared['spec']['ports']):
    fail.append("the audited port was merged into the shared Service; it must have exactly one narrow cluster-internal entry")

audited = by_name.get(('Service', 'cogneva-security-gateway-audited'))
if audited is None:
    fail.append("the audited channel has no Service of its own")
elif [p['port'] for p in audited['spec']['ports']] != [8083]:
    fail.append(f"the audited Service's ports are not the single 8083: {audited['spec']['ports']}")

policy = by_name.get(('NetworkPolicy', 'cogneva-security-gateway'))
if policy is None:
    fail.append("no gateway NetworkPolicy")
else:
    matches = [
        rule
        for rule in policy['spec']['ingress']
        for p in rule.get('ports', [])
        if p.get('port') == 8083
    ]
    if len(matches) != 1:
        fail.append(f"the gateway policy has {len(matches)} source rules on the audited port 8083; there must be exactly one")
    else:
        sources = matches[0].get('from')
        if sources != [
            {'podSelector': {'matchLabels': {'app.kubernetes.io/component': 'sandbox-executor'}}}
        ]:
            fail.append(f"the audited port's source is not the sandbox executor only: {sources}")
        if policy['spec']['ingress'][0].get('port') is None:
            for rule in policy['spec']['ingress']:
                if 8083 in [p.get('port') for p in rule.get('ports', [])]:
                    continue
                if rule.get('from') == []:
                    fail.append("the audited port may have landed in a rule with no source restriction")

print("\n".join(fail))
sys.exit(1 if fail else 0)
PYEOF

echo "PASS: document scopes mount by identity, default off, every template-set key has a reader, and the audited channel admits the executor only"
