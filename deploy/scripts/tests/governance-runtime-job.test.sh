#!/usr/bin/env bash
# Deterministic gate for the headroom reading in check-governance-consistency.sh.
#
# The reading that went wrong: the quota headroom was computed over the rendered
# workload set alone, and the one pod the deployer creates at *runtime* -- the
# rollout judgement Job -- is not in any rendered set. It is declared, though: its
# resource face lives in the evolution ConfigMap, which the same render carries.
# Measured live on 2026-09-29: the note printed "headroom 600m" while the namespace
# was at 17/17 during a rollout (steady pods 15000m + judgement Job 2000m), and the
# admission failure landed as `FailedCreate ... exceeded quota: cogneva-quota` which
# the rollout reports as a *timeout*, not as a resource verdict. Two carriers held
# the same figure and disagreed: the ConfigMap said 2000m, the quota comment's
# calibration (and therefore the note) still counted 500m.
#
# The reading is widened, not the verdict: numbers here are policy, so the gate
# prints the runtime-inclusive headroom and leaves pass/fail on the rendered-set
# lower bound. What this test pins is that the printed number is *read from the
# declaration* rather than being a constant that happens to look right today.
#
# Three directions:
#   - the real rendered profiles print the line, and the runtime-inclusive headroom
#     equals the rendered-set sum plus the Job's declared limit;
#   - a manifest-declared Job is counted in the lower bound (the note has always
#     claimed "concurrent Jobs count as 1" while the enumeration listed four kinds
#     without Job -- a claim wider than the code);
#   - mutating the declared limit moves the printed number by exactly that much,
#     and a disabled deployer removes the line instead of printing a stale one.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-governance-consistency.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "missing dependency: python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# Reads the two cpu numbers the gate prints for the limits face: the rendered-set
# lower bound and the runtime-inclusive one. The `requests` face prints a line of
# the same shape, so the together-line is taken from the block that follows the
# `limits.CPU` line and is required to name the *limit* key -- matching the first
# "把它一起算进来" in the file would silently read the request face instead.
read_cpu() {
  python3 - "$1" <<'PYEOF'
import re, sys

lines = open(sys.argv[1]).read().splitlines()
alone = together = None
for i, line in enumerate(lines):
    m = re.search(r"limits\.CPU 单副本下限和 (\d+)m", line)
    if not m:
        continue
    alone = m.group(1)
    for follow in lines[i + 1:]:
        if "└" not in follow:
            break
        if "ROLLOUT_JOB_CPU_LIMIT" not in follow:
            break
        t = re.search(r"把它一起算进来 (\d+)m vs", follow)
        if t:
            together = t.group(1)
        break
    break
if alone is None:
    sys.exit("the gate printed no limits.CPU lower-bound reading")
print(alone, together or "-")
PYEOF
}

# The Job's declared cpu limit, read from the same ConfigMap the deployer reads.
declared_cpu_m() {
  python3 - "$1" <<'PYEOF'
import glob, os, sys, yaml

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.yaml"))):
    for doc in yaml.safe_load_all(open(path)):
        data = (doc or {}).get("data") or {}
        if str(data.get("COGNEVA_MAINLINE_DEPLOYER_ENABLED", "")).lower() != "true":
            continue
        raw = data.get("COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT")
        if raw is None:
            continue
        text = str(raw).strip()
        if text.endswith("m"):
            print(int(float(text[:-1])))
        else:
            print(int(float(text) * 1000))
        raise SystemExit
sys.exit("no enabled deployer ConfigMap declaring a rollout job cpu limit")
PYEOF
}

for profile in k3s-single k3s-multi; do
  dir="${repo}/deploy/rendered/${profile}"
  bash "${gate}" "${dir}" > "${work}/${profile}.out" 2>&1 \
    || fail "the gate is red on ${profile}: $(tail -3 "${work}/${profile}.out")"

  job_m="$(declared_cpu_m "${dir}")"
  read -r alone together < <(read_cpu "${work}/${profile}.out")
  [ "${together}" != "-" ] \
    || fail "${profile}: the gate printed no runtime-inclusive headroom line"
  [ $(( together - alone )) -eq "${job_m}" ] \
    || fail "${profile}: runtime-inclusive ${together}m minus rendered-set ${alone}m is not the declared ${job_m}m"

  # Mutation control: a limit that cannot coincide with anything else in the tree.
  # If the number were a constant, or read from the wrong carrier, this would not move.
  mutated="${work}/mutated-${profile}"
  cp -r "${dir}" "${mutated}"
  python3 - "${mutated}" <<'PYEOF'
import glob, os, sys, yaml

target = os.path.join(sys.argv[1])
for path in sorted(glob.glob(os.path.join(target, "*.yaml"))):
    docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]
    hit = False
    for d in docs:
        data = d.get("data") or {}
        if "COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT" in data:
            data["COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT"] = "9"
            hit = True
    if hit:
        with open(path, "w") as fh:
            yaml.safe_dump_all(docs, fh)
PYEOF
  bash "${gate}" "${mutated}" > "${work}/mutated-${profile}.out" 2>&1 \
    || fail "the gate is red on the mutated ${profile}"
  read -r _ mutated_together < <(read_cpu "${work}/mutated-${profile}.out")
  [ $(( mutated_together - alone )) -eq 9000 ] \
    || fail "${profile}: raising the declared limit to 9 cores moved the reading to ${mutated_together}m from ${alone}m, not by 9000m"

  # Reverse control: with the deployer disabled the Job never exists, so the line
  # must disappear rather than keep printing a number nothing declares.
  off="${work}/off-${profile}"
  cp -r "${dir}" "${off}"
  python3 - "${off}" <<'PYEOF'
import glob, os, sys, yaml

for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.yaml"))):
    docs = [d for d in yaml.safe_load_all(open(path)) if d and d.get("kind")]
    hit = False
    for d in docs:
        data = d.get("data") or {}
        if "COGNEVA_MAINLINE_DEPLOYER_ENABLED" in data:
            data["COGNEVA_MAINLINE_DEPLOYER_ENABLED"] = "false"
            hit = True
    if hit:
        with open(path, "w") as fh:
            yaml.safe_dump_all(docs, fh)
PYEOF
  bash "${gate}" "${off}" > "${work}/off-${profile}.out" 2>&1 \
    || fail "the gate is red with the deployer disabled on ${profile}"
  read -r _ off_together < <(read_cpu "${work}/off-${profile}.out")
  [ "${off_together}" = "-" ] \
    || fail "${profile}: the runtime-inclusive line is still printed with the deployer disabled"
  echo "OK: ${profile}: rendered-set ${alone}m + judgement Job ${job_m}m = ${together}m, and the reading follows the declaration"
done

# A manifest-declared Job has to move the lower bound: the note has always claimed
# concurrent Jobs count as 1, and until now the enumeration could not see any Job.
synth="${work}/synth"
mkdir -p "${synth}"
job_limit="700m"
cat > "${synth}/10-quota.yaml" <<YAML
apiVersion: v1
kind: ResourceQuota
metadata:
  name: cogneva-quota
spec:
  hard:
    limits.cpu: "8"
YAML
cat > "${synth}/11-limits.yaml" <<'YAML'
apiVersion: v1
kind: LimitRange
metadata:
  name: cogneva-limits
spec:
  limits:
    - type: Container
      default:
        cpu: 500m
      defaultRequest:
        cpu: 50m
      max:
        cpu: "4"
      min:
        cpu: 10m
YAML
cat > "${synth}/40-job.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: a-rolling-job
spec:
  template:
    spec:
      containers:
        - name: c
          image: example.invalid/x:1
          resources:
            limits:
              cpu: ${job_limit}
            requests:
              cpu: 100m
YAML

echo "--- with the Job ---"
bash "${gate}" "${synth}" > "${work}/synth-with.out" 2>&1 || fail "the gate is red on the synthetic tree with a Job"
read -r synth_alone _ < <(read_cpu "${work}/synth-with.out")
[ "${synth_alone}" = "700" ] \
  || fail "a manifest-declared Job with 700m did not enter the limits.cpu lower bound (read ${synth_alone}m)"

echo "--- without the Job ---"
rm "${synth}/40-job.yaml"
bash "${gate}" "${synth}" > "${work}/synth-without.out" 2>&1 || fail "the gate is red on the synthetic tree without a Job"
read -r synth_alone _ < <(read_cpu "${work}/synth-without.out")
[ "${synth_alone}" = "0" ] \
  || fail "the lower bound did not fall back to 0m without the Job (read ${synth_alone}m)"

echo "OK: governance runtime-job headroom gate"
