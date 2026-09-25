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
# A fourth link carries the outcome: what the repair did is only knowable if it
# publishes a reading, a container beside it serves that reading, and a monitor
# actually scrapes it. Each link can be removed without breaking the others, and
# the pod then starts with nothing anywhere saying what it gave up — the state
# that made the repair's own findings unobservable in the first place.
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
monitor_path = repo / "deploy" / "k3s" / "observability" / "manifests" / "11-podmonitor-redis.yaml"

INIT_NAME = "aof-repair"
REDIS_CONTAINER = "redis"
STATIC_IMAGE = "localhost:30500/cogneva:local"
VERDICT_METRIC = "cogneva_aof_repair_verdict_published"

# Port names the containers that serve the verdict declare. Collected while
# checking the workload so the monitor below can be held to selecting one of
# them: a monitor pointing at a port nobody serves scrapes nothing, and the
# safest way to be wrong about a name is to write it twice.
served_ports = set()


def command_of(container):
    cmd = container.get("command") or []
    return " ".join(str(c) for c in cmd)


def expand(cmd, value):
    """Resolve a shell variable in an operand from the command's own assignments.

    The wrapper names the report path once and passes it to both the repair and
    the guard that states an empty report, so the operand arrives here as
    `$report`. Following the assignment keeps the path single-sourced in the
    command instead of duplicated in the gate's idea of where it should be.
    """
    assigned = dict(re.findall(r"([A-Za-z_]\w*)=(/\S+)", cmd))
    return re.sub(r"\$\{?(\w+)\}?", lambda m: assigned.get(m.group(1), m.group(0)), value)


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
    for token, why in (("--help", "no capability probe"), ("grep", "no capability probe")):
        if token not in cmd:
            bad.append(f"{origin}: {INIT_NAME} command has {why} ({token!r} missing)")
    # Publishing the verdict means the command now has work to do after the
    # repair returns, so the status can be exec'd away or re-exported — but not
    # dropped: a repair that could not read the directory must fail the pod.
    if "exec" not in cmd and not (re.search(r"rc=\$\?", cmd) and re.search(r"exit\s+\$\{?rc", cmd)):
        bad.append(
            f"{origin}: {INIT_NAME} command neither exec's the repair nor re-exports its status, "
            "so a failed repair would look like a successful start"
        )
    if VERDICT_METRIC not in cmd or "[ ! -s" not in cmd:
        bad.append(
            f"{origin}: {INIT_NAME} cannot state that it published nothing "
            f"({VERDICT_METRIC} and the empty-file guard are what make a blind pass visible)"
        )

    # Link two: the reading has to land where a scraper can reach it.
    published = re.search(r"--metrics-file\s+(\S+)", cmd)
    if not published:
        bad.append(f"{origin}: {INIT_NAME} publishes no verdict (--metrics-file missing)")
        return bad
    report = expand(cmd, published.group(1).strip('"'))
    report_dir = posixpath.dirname(report)
    by_path = {
        m.get("mountPath"): m.get("name")
        for m in repair.get("volumeMounts", [])
        if m.get("mountPath")
    }
    if report_dir not in by_path:
        bad.append(
            f"{origin}: {INIT_NAME} publishes to {report} but does not mount {report_dir} "
            f"(mounts: {sorted(by_path)})"
        )
        return bad

    # Link three: something serves that directory as Prometheus textfiles.
    servers = [
        (name, c)
        for name, c in containers.items()
        if name != REDIS_CONTAINER
        and any(m.get("mountPath") == report_dir for m in c.get("volumeMounts", []))
    ]
    if not servers:
        bad.append(
            f"{origin}: nothing beside {INIT_NAME} mounts {report_dir}, so the verdict is "
            "written where no scraper can read it"
        )
    for name, c in servers:
        served = " ".join(str(a) for a in (c.get("args") or [])) + " " + command_of(c)
        if f"--collector.textfile.directory={report_dir}" not in served:
            bad.append(
                f"{origin}: {name} mounts {report_dir} but does not serve it as a textfile directory"
            )
        ports = {p.get("name") for p in c.get("ports", []) if p.get("name")}
        if not ports:
            bad.append(f"{origin}: {name} declares no named port, so there is nothing to scrape")
        served_ports.update(ports)
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

# Link four: the served port has to be selected by a monitor. Held against the
# port names the workload itself declared, so the two halves cannot agree on a
# name that no container serves.
def check_monitor(monitor, origin):
    """Structural checks on a PodMonitor that is supposed to scrape the verdict."""
    bad = []
    spec = monitor.get("spec", {})
    selected = (spec.get("selector") or {}).get("matchLabels") or {}
    if selected.get("app.kubernetes.io/name") != REDIS_CONTAINER:
        bad.append(
            f"{origin}: {monitor.get('metadata', {}).get('name')} does not select the "
            f"{REDIS_CONTAINER} pod (matchLabels: {selected})"
        )
    namespaces = ((spec.get("namespaceSelector") or {}).get("matchNames")) or []
    if "cogneva" not in namespaces:
        bad.append(f"{origin}: does not look in the cogneva namespace ({namespaces})")
    endpoints = spec.get("podMetricsEndpoints") or []
    if not endpoints:
        bad.append(f"{origin}: no podMetricsEndpoints")
    for endpoint in endpoints:
        port = endpoint.get("port")
        if port not in served_ports:
            bad.append(
                f"{origin}: scrapes port {port!r}, which no container serving the verdict "
                f"declares ({sorted(served_ports)})"
            )
    return bad


monitors = (
    [d for d in yaml.safe_load_all(monitor_path.read_text()) if d and d.get("kind") == "PodMonitor"]
    if monitor_path.exists()
    else []
)
if not monitors:
    problems.append(
        f"{monitor_path.name}: no PodMonitor, so the published verdict is served and never scraped "
        "(the reading is then absent from every query, which reads as a clean start)"
    )
for monitor in monitors:
    problems += check_monitor(monitor, monitor_path.name)

# The chart is checked textually: this gate runs in the job that has no helm, and
# the deploy-parity job already holds the chart to the static manifest field by
# field. What this half adds is that the wiring exists in the source of truth at
# all, so deleting it cannot ride into a release unnoticed.
template = template_path.read_text()
for needle, why in (
    (f"name: {INIT_NAME}", "the init container is gone from the chart"),
    ("repair-aof /data/appendonlydir --metrics-file",
     "the chart no longer runs the repair against redis' AOF directory with a verdict to publish"),
    ("report=/report/aof-repair.prom", "the chart no longer names the file the verdict is published to"),
    ("mountPath: /data", "the chart no longer mounts the AOF directory into the repair container"),
    ("mountPath: /report", "the chart lost the volume the verdict is published into"),
    (f"{VERDICT_METRIC}", "the chart cannot state that the repair published nothing"),
    ("--collector.textfile.directory=/report",
     "the chart lost the container that serves the verdict as Prometheus textfiles"),
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

# Control: a fully wired workload that must pass, then the same one with one
# defect at a time. A control that trips a check for some other reason proves
# nothing about the check it is meant to exercise, so each mutation asserts the
# message it is supposed to produce.
GOOD = """
kind: StatefulSet
metadata: {name: control}
spec:
  template:
    spec:
      initContainers:
        - name: aof-repair
          image: localhost:30500/cogneva:local
          imagePullPolicy: Always
          command:
            - sh
            - -c
            - |
              report=/report/aof-repair.prom
              if ! /opt/cogneva/cogneva --help 2>&1 | grep -q 'repair-aof'; then
                printf '%s 0\\n' cogneva_aof_repair_verdict_published > "$report"
                exit 0
              fi
              /opt/cogneva/cogneva repair-aof /data/appendonlydir --metrics-file "$report"
              rc=$?
              if [ ! -s "$report" ]; then
                printf '%s 0\\n' cogneva_aof_repair_verdict_published > "$report"
              fi
              exit $rc
          volumeMounts:
            - {name: data, mountPath: /data}
            - {name: report, mountPath: /report}
      containers:
        - name: redis
          command: [sh, -c, "redis-server --appendonly yes --dir /data"]
        - name: verdict-exporter
          args: ["--collector.textfile.directory=/report"]
          ports: [{name: textfile, containerPort: 9100}]
          volumeMounts: [{name: report, mountPath: /report}]
"""
clean = check_workload(yaml.safe_load(GOOD), "control")
if clean:
    problems.append(f"control case not clean: a fully wired workload was flagged ({clean})")

# Each entry replaces one substring; the replacement either breaks the link or
# leaves it intact with a different name, and the message it must produce is
# asserted rather than "something was flagged".
MUTATIONS = [
    ("- {name: report, mountPath: /report}\n", "- {name: report, mountPath: /elsewhere}\n",
     "publishes to /report/aof-repair.prom but does not mount /report"),
    ("--metrics-file \"$report\"", "", "publishes no verdict"),
    ("exit $rc", "true", "neither exec's the repair nor re-exports its status"),
    ("if [ ! -s \"$report\" ]; then", "if false; then",
     "cannot state that it published nothing"),
    ("          volumeMounts: [{name: report, mountPath: /report}]\n", "",
     "nothing beside aof-repair mounts /report"),
    ("args: [\"--collector.textfile.directory=/report\"]", "args: []",
     "does not serve it as a textfile directory"),
    ("ports: [{name: textfile, containerPort: 9100}]", "",
     "declares no named port"),
]
for needle, replacement, expected in MUTATIONS:
    if needle not in GOOD:
        problems.append(f"control case is stale: {needle!r} is no longer in the sample")
        continue
    caught = check_workload(yaml.safe_load(GOOD.replace(needle, replacement)), "control")
    if not any(expected in c for c in caught):
        problems.append(
            f"control case not caught ({expected!r}): "
            f"{caught or 'the check passed a defective workload'}"
        )

# Control for the monitor half: the same monitor with a port no container
# declares must be flagged, and with the selector emptied must be flagged too.
if monitors:
    control_monitor = yaml.safe_load(yaml.safe_dump(monitors[0]))
    control_monitor["spec"]["podMetricsEndpoints"][0]["port"] = "no-such-port"
    if not any("no-such-port" in c for c in check_monitor(control_monitor, "control")):
        problems.append("control case not caught: a monitor scraping a port nothing serves passed")
    control_monitor = yaml.safe_load(yaml.safe_dump(monitors[0]))
    control_monitor["spec"]["selector"] = {"matchLabels": {"app.kubernetes.io/name": "not-redis"}}
    if not any("does not select" in c for c in check_monitor(control_monitor, "control")):
        problems.append("control case not caught: a monitor selecting another workload passed")

if problems:
    print("AOF repair wiring gate failed:")
    for p in problems:
        print("  - " + p)
    sys.exit(1)
print("PASS: the redis pod repairs the AOF directory redis itself writes, from the deployed image, and publishes what it found to a monitor that scrapes it")
PY
