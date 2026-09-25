#!/usr/bin/env bash
# Wiring gate for the Redis AOF tail repair.
#
# The repair only works if three things line up, and none of them is visible in
# the other's output: the directory the repair is pointed at must be the one redis
# writes to (`--dir <base>/appendonlydir`), that directory must be on a volume the
# init container actually mounts, and the image must be resolved at pod start
# (the deployment pins a floating tag, so a cached older image would run a binary
# that predates the subcommand and hold the pod in Init forever — hence the guard
# in the command and `pullPolicy: Always`).
#
# Deleting the init container keeps every existing gate green: the manifests stay
# self-consistent, parity stays aligned, and the repair silently stops existing.
# That is the regression this gate is for.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"

python3 - "${repo}" <<'PY'
import pathlib, posixpath, re, sys

try:
    import yaml
except ImportError:
    print("needs python3 with the yaml module (apt install python3-yaml)", file=sys.stderr)
    sys.exit(2)

repo = pathlib.Path(sys.argv[1])
static_path = repo / "deploy" / "k3s" / "redis-deployment.yaml"
template_path = repo / "deploy" / "helm" / "cogneva" / "templates" / "redis.yaml"
values_path = repo / "deploy" / "helm" / "cogneva" / "values.yaml"

INIT_NAME = "aof-repair"
REDIS_CONTAINER = "redis"
STATIC_IMAGE = "localhost:30500/cogneva:local"


def command_of(container):
    cmd = container.get("command") or []
    return " ".join(str(c) for c in cmd)


def check_workload(doc, origin):
    """Structural checks on a rendered redis workload."""
    bad = []
    spec = doc.get("spec", {}).get("template", {}).get("spec", {})
    inits = {c.get("name"): c for c in spec.get("initContainers", [])}
    containers = {c.get("name"): c for c in spec.get("containers", [])}

    repair = inits.get(INIT_NAME)
    redis = containers.get(REDIS_CONTAINER)
    if repair is None:
        return [f"{origin}: no init container named {INIT_NAME}: the repair is gone"]
    if redis is None:
        return [f"{origin}: no container named {REDIS_CONTAINER}"]

    cmd = command_of(repair)
    # The absolute operand, not the bare word: the command also mentions the
    # subcommand inside its capability probe, where nothing follows it.
    walked = re.search(r"repair-aof\s+(/\S+)", cmd)
    if not walked:
        bad.append(f"{origin}: {INIT_NAME} does not run `repair-aof <absolute-dir>`")
    else:
        target = walked.group(1)
        # The operand has to be the directory redis itself writes: --dir <base>
        # puts the multi-part AOF in <base>/appendonlydir.
        redis_dir = re.search(r"--dir\s+(\S+)", command_of(redis))
        if not redis_dir:
            bad.append(f"{origin}: {REDIS_CONTAINER} declares no --dir to compare against")
        else:
            expected = posixpath.join(redis_dir.group(1), "appendonlydir")
            if target != expected:
                bad.append(f"{origin}: repair target {target} is not redis' own AOF directory {expected}")
        mounts = {m.get("mountPath") for m in repair.get("volumeMounts", [])}
        if posixpath.dirname(target) not in mounts:
            bad.append(
                f"{origin}: {INIT_NAME} repairs {target} but does not mount its parent "
                f"(mounts: {sorted(m for m in mounts if m)})"
            )

    # The floating tag must be re-resolved at pod start, and the command must
    # stand aside when the image is older than the subcommand instead of holding
    # the pod in Init forever.
    if repair.get("imagePullPolicy") != "Always":
        bad.append(f"{origin}: {INIT_NAME} imagePullPolicy is {repair.get('imagePullPolicy')!r}, not Always")
    for token, why in (("--help", "no capability probe"), ("grep", "no capability probe"), ("exec", "no exec")):
        if token not in cmd:
            bad.append(f"{origin}: {INIT_NAME} command has {why} ({token!r} missing)")
    return bad


problems = []

doc = next(
    (d for d in yaml.safe_load_all(static_path.read_text()) if d and d.get("kind") == "StatefulSet"),
    None,
)
if doc is None:
    problems.append(f"{static_path.name}: no StatefulSet")
else:
    problems += check_workload(doc, static_path.name)
    spec = doc["spec"]["template"]["spec"]
    init = next((c for c in spec.get("initContainers", []) if c.get("name") == INIT_NAME), None)
    if init is not None and init.get("image") != STATIC_IMAGE:
        problems.append(f"{static_path.name}: {INIT_NAME} image {init.get('image')!r} != {STATIC_IMAGE}")

# The chart is checked textually: this gate runs in the job that has no helm, and
# the deploy-parity job already holds the chart to the static manifest field by
# field. What this half adds is that the wiring exists in the source of truth at
# all, so deleting it cannot ride into a release unnoticed.
template = template_path.read_text()
for needle, why in (
    (f"name: {INIT_NAME}", "the init container is gone from the chart"),
    ("repair-aof /data/appendonlydir", "the chart no longer runs the repair against redis' AOF directory"),
    ("mountPath: /data", "the chart no longer mounts the AOF directory into the repair container"),
    ("--dir /data", "the chart's redis no longer declares the --dir the repair target is derived from"),
    ("image: {{ .Values.image.repository }}:{{ .Values.image.tag }}", "the repair no longer runs the deployed image"),
    ("imagePullPolicy: {{ .Values.image.pullPolicy }}", "the repair no longer follows the image pull policy"),
    ("--help", "the chart lost the capability probe in front of the subcommand"),
):
    if needle not in template:
        problems.append(f"{template_path.name}: {why}")

values = yaml.safe_load(values_path.read_text()) or {}
if (values.get("image") or {}).get("pullPolicy") != "Always":
    problems.append("values.yaml: image.pullPolicy is not Always, so a cached older image can predate the repair command")

# Control: the same check must flag a workload whose repair target is off the
# mounted volume, otherwise this gate is measuring nothing.
control = list(
    yaml.safe_load_all(
        """
kind: StatefulSet
metadata: {name: control}
spec:
  template:
    spec:
      initContainers:
        - name: aof-repair
          image: localhost:30500/cogneva:local
          imagePullPolicy: Always
          command: [sh, -c, "exec /opt/cogneva/cogneva repair-aof /data/appendonlydir"]
          volumeMounts: [{name: data, mountPath: /var/lib/elsewhere}]
      containers:
        - name: redis
          command: [sh, -c, "redis-server --appendonly yes --dir /data"]
"""
    )
)
if not check_workload(control[0], "control"):
    problems.append("control case not caught: the gate is blind to a repair target outside the mounted volume")

if problems:
    print("AOF repair wiring gate failed:")
    for p in problems:
        print("  - " + p)
    sys.exit(1)
print("PASS: the redis pod repairs the AOF directory redis itself writes, from the deployed image")
PY
