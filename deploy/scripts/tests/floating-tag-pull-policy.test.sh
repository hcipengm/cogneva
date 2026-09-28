#!/usr/bin/env bash
# Deterministic gate for "a container that consumes this cluster's own image
# distribution runs whatever the node happened to cache under that name".
#
# The failure this pins is silent by construction. Our own images are consumed by
# tag, and that tag is re-pointed by the rollout (the cluster's floating tag is
# advanced only after a rev has converged). `IfNotPresent` turns the node's
# content store into an authority that nobody updates: the manifest is correct,
# the pod is Running, and the process inside is an older build. Measured live on
# 2026-09-29: the registry's volume-walker sidecar asked for the floating tag with
# `IfNotPresent`, so it kept running the binary of a rev that predated the
# `volume-walker` subcommand, CrashLooped on `unrecognized argument`, and the
# volume-occupancy series the reclaim logic reads never appeared. Nothing in the
# pod's own status said "stale binary" -- the node cache was the only holder of
# that reading, and it is in no manifest.
#
# The judgement is structural: a container whose image this project publishes,
# under a tag the pipeline re-points, has to be pulled at every start. It is not a
# container-name list (a new sidecar would go uncovered) and not a tag-name list
# (`local` is not the only floating tag that can appear). Two exemptions, both
# narrowing the strict side:
#   - digest-pinned references: the content is fixed by the reference itself, so
#     there is no name under which anything stale can hide;
#   - version-shaped tags: release tags are immutable under the single-source
#     release policy, so a cached copy is the right copy.
# Everything else -- `latest`, `local`, `main-<rev>`, no tag at all -- is treated
# as re-pointable, which is the safe direction: over-requiring `Always` costs a
# manifest fetch, under-requiring it costs a silent stale binary.
#
# Three surfaces are read: the chart rendered with each profile values file (the
# authority), the static bootstrap manifests (what a from-scratch cluster
# applies), and every committed rendered profile -- the GitOps path applies those
# without ever reading either of the first two.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
fail() { echo "FAIL: $*"; exit 1; }

command -v helm >/dev/null || { echo "missing dependency: helm" >&2; exit 2; }
command -v python3 >/dev/null || { echo "missing dependency: python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# Which references this project publishes, read from the same value the image
# helper renders (`{{ .Values.image.repository }}`) plus any profile override. A
# repository string that a manifest uses but this set does not carry would be
# silently out of scope, so the set is printed and an empty one is fatal.
own_csv="$(python3 - "${chart}" <<'PYEOF'
import glob, os, sys, yaml

chart = sys.argv[1]
repos = []

def pull(path):
    try:
        doc = yaml.safe_load(open(path))
    except Exception:
        return
    repo = ((doc or {}).get("image") or {}).get("repository")
    if repo:
        repos.append(repo)

pull(os.path.join(chart, "values.yaml"))
for profile in sorted(glob.glob(os.path.join(chart, "profiles", "*.yaml"))):
    pull(profile)
repos = sorted(set(repos))
if not repos:
    sys.exit("no image.repository declared in the chart: cannot tell which "
             "references this project publishes, so nothing would be in scope")
print(",".join(repos))
PYEOF
)"
echo "own image repository: ${own_csv}"

for profile in k3s-single k3s-multi k8s-standard; do
  helm template cogneva "${chart}" -f "${chart}/profiles/${profile}.yaml" \
    --namespace cogneva > "${work}/chart-${profile}.yaml"
done

# require_floating is asserted per real surface: a surface that carries no
# floating reference at all would pass while checking nothing about the policy.
check() {
  local label="$1" require_floating="$2"; shift 2
  local out
  if ! out="$(SURFACE_LABEL="${label}" python3 - "${own_csv}" "${require_floating}" "$@" <<'PYEOF' 2>&1
import os, re, sys, yaml

own = set(sys.argv[1].split(","))
require_floating = sys.argv[2] == "1"
paths = sys.argv[3:]
label = os.environ.get("SURFACE_LABEL", "surface")

VERSION_TAG = re.compile(r"^v?\d+(\.\d+)*$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def split_image(image):
    """(repository, tag, digest_pinned); tag is None when the reference has none."""
    if "@" in image:
        name, _, ref = image.partition("@")
        return name, None, bool(DIGEST.match(ref))
    head, sep, last = image.rpartition("/")
    if ":" in last:
        base, _, tag = last.partition(":")
        return (f"{head}/{base}" if sep else base), tag, False
    return image, None, False


def specs(doc):
    if doc.get("kind") == "CronJob":
        yield doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    elif "template" in doc.get("spec", {}):
        yield doc["spec"]["template"]["spec"]


own_seen = floating_seen = pinned_seen = 0
bad = []
for path in paths:
    for doc in yaml.safe_load_all(open(path)):
        if not doc or not doc.get("kind"):
            continue
        for spec in specs(doc):
            containers = (spec.get("containers") or []) + (spec.get("initContainers") or [])
            for c in containers:
                image = c.get("image") or ""
                name, tag, digest_pinned = split_image(image)
                if name not in own:
                    continue
                own_seen += 1
                where = f"{os.path.basename(path)}/{c['name']}"
                if digest_pinned or (tag and VERSION_TAG.match(tag)):
                    pinned_seen += 1
                    continue
                floating_seen += 1
                policy = c.get("imagePullPolicy") or "<unset>"
                if policy != "Always":
                    bad.append(
                        f"{where}: image {image} has imagePullPolicy {policy}, but the "
                        "tag is re-pointed by the rollout -- a cached copy under this "
                        "name is a different build"
                    )

if own_seen == 0:
    sys.exit(f"no container referencing {sorted(own)} in {len(paths)} file(s): "
             "the gate looked at nothing")
if require_floating and floating_seen == 0:
    sys.exit(f"all {own_seen} own-registry reference(s) in {len(paths)} file(s) are "
             "pinned: this surface no longer exercises the floating-tag rule")
if bad:
    sys.exit("containers that can be pinned to a stale node cache:\n  " + "\n  ".join(bad))
print(f"OK: {label}: own-registry={own_seen} floating={floating_seen} pinned={pinned_seen}")
PYEOF
)"; then
    echo "${out}"
    fail "${label}"
  fi
  echo "${out}"
}

check "chart k3s-single" 1 "${work}/chart-k3s-single.yaml"
check "chart k3s-multi" 1 "${work}/chart-k3s-multi.yaml"
check "chart k8s-standard" 1 "${work}/chart-k8s-standard.yaml"
check "static deploy/k3s" 1 "${repo}"/deploy/k3s/*.yaml
for profile in k3s-single k3s-multi k8s-standard; do
  check "rendered ${profile}" 1 "${repo}/deploy/rendered/${profile}"/*.yaml
done

# Reverse control, all three sides. The same file with the policy set to Always
# has to pass, with IfNotPresent has to fail, and with a version-shaped tag has to
# pass -- otherwise "OK" only means "the file parsed", and an exemption that is
# dead text would swallow a real reference exactly like the bug above.
python3 - "${repo}/deploy/k3s/cluster-registry.yaml" "${work}" <<'PYEOF'
import copy, sys, yaml

src, out_dir = sys.argv[1], sys.argv[2]
docs = [d for d in yaml.safe_load_all(open(src)) if d and d.get("kind")]


def write(variant, policy, image):
    docs2 = copy.deepcopy(docs)
    for d in docs2:
        if "template" not in d.get("spec", {}):
            continue
        for c in d["spec"]["template"]["spec"].get("containers", []):
            if not c.get("imagePullPolicy"):
                continue
            c["imagePullPolicy"] = policy
            if image and c["image"].endswith(":local"):
                c["image"] = image
    with open(f"{out_dir}/control-{variant}.yaml", "w") as fh:
        yaml.safe_dump_all(docs2, fh)


write("always", "Always", None)
write("ifnotpresent", "IfNotPresent", None)
write("versioned", "IfNotPresent", "localhost:30500/cogneva:0.5.8")
PYEOF

check "control, the same container with policy Always" 0 "${work}/control-always.yaml"
if ( check "control, the same container with policy IfNotPresent" 0 \
      "${work}/control-ifnotpresent.yaml" >/dev/null ); then
  fail "the gate passes a floating own-registry reference with IfNotPresent: it is not reading the tag"
fi
check "control, the same container with a version-shaped tag" 0 "${work}/control-versioned.yaml"

echo "OK: floating tag pull policy gate"
