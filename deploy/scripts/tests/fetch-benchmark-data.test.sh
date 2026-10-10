#!/usr/bin/env bash
# Deterministic gate for the benchmark data fetcher.
#
# The fetcher writes a directory nobody inspects by hand, and four of its failure modes are
# silent: a mirror that serves different bytes than the catalog pins installs a benchmark the
# gate was not calibrated against; a second run that re-downloads 108 MiB on every start; an
# installed file whose bytes were changed underneath is overwritten instead of refused, which
# erases the evidence of what it used to be; and a tarball that unpacks to something other
# than what its hash described. So the real script runs here against a fixture mirror over a
# local HTTP server, and what is asserted is what an operator cannot see.
#
# Two things are checked in two ways, because they fail differently:
#
#   * The catalog's *identity* -- which benchmarks and which paths -- is read straight from
#     the committed script with --print-files, no fixture involved. A path that quietly
#     changed shape is caught here.
#   * The fetch *mechanics* -- install layout, idempotency, resume, refusal, extraction,
#     and the --verify verdict -- run against a fixture catalog (BENCHMARK_CATALOG_FILE) with
#     small bodies whose hashes the fixture computes. The real artifacts are 7.8 MiB and
#     108 MiB, so they cannot be served here; the fixture is what makes a wrong byte and a
#     truncated body expressible at all. The committed hashes themselves are pinned by being
#     read at fetch time, not by this test -- see the note on --verify below.
#
# The parquet -> JSONL conversion needs pyarrow, which the gate environment does not carry,
# so that path is exercised only when pyarrow is importable; otherwise it prints a SKIP line
# rather than pretending to have covered it.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
# Overridable so a mutation proof can point this at a mutated copy.
fetcher="${FETCHER_UNDER_TEST:-${repo}/deploy/scripts/fetch-benchmark-data.sh}"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
server_pid=""
cleanup() {
  [ -n "${server_pid}" ] && kill "${server_pid}" 2>/dev/null || true
  rm -rf "${work}"
}
trap cleanup EXIT

[ -x "${fetcher}" ] || fail "${fetcher} is not executable"

# --- the committed catalog's identity ----------------------------------------
# The revision is not printed (it lives in the source), but the paths a provision produces
# must be exactly these, whether or not the fetch ever runs.
mapfile -t committed < <("${fetcher}" --print-files)
expected_committed=(
  "swe-bench-pro/test-00000-of-00001.parquet"
  "toolathlon-gym.tar.gz"
  "toolathlon-gym/db/init.sql.gz"
  "toolathlon-gym/docker-compose.yml"
)
[ "${#committed[@]}" -eq "${#expected_committed[@]}" ] ||
  fail "--print-files listed ${#committed[@]} paths, the catalog is expected to hold ${#expected_committed[@]}"
for i in "${!expected_committed[@]}"; do
  [ "${committed[$i]}" = "${expected_committed[$i]}" ] ||
    fail "--print-files line $((i + 1)) is '${committed[$i]}', expected '${expected_committed[$i]}'"
done
mapfile -t committed_conv < <("${fetcher}" --print-files --convert)
printf '%s\n' "${committed_conv[@]}" | grep -qx 'swe-bench-pro/test-00000-of-00001.jsonl' ||
  fail "--print-files --convert does not list the derived JSONL"

# --- the fixture mirror ------------------------------------------------------
hub="${work}/hub"
log="${work}/requests.log"
port_file="${work}/port"
bogus_file="${work}/bogus"
tarball="${work}/fixture-tree.tar.gz"
: >"${log}"

# A small tree with the two member paths the catalog names, packed the way codeload packs one
# (a single top directory, stripped on extraction).
mkdir -p "${work}/tree/toolathlon_gym-fixture/db"
printf 'fixture sql dump\n' >"${work}/tree/toolathlon_gym-fixture/db/init.sql.gz"
printf 'services: {}\n' >"${work}/tree/toolathlon_gym-fixture/docker-compose.yml"
tar -czf "${tarball}" -C "${work}/tree" toolathlon_gym-fixture

cat >"${work}/serve.py" <<'PY'
import http.server, os, sys, urllib.parse

root, port_file, log, tarball, bogus_file = sys.argv[1:6]

def bogus_target():
    if os.path.isfile(bogus_file):
        with open(bogus_file) as fh:
            return fh.read().strip()
    return ""

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def route(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        with open(log, "a") as fh:
            fh.write(parsed.path + "?" + parsed.query + "\n")

        path = parsed.path
        # ModelScope dataset raw bytes: /api/v1/datasets/<repo>/repo?Revision=&FilePath=
        if path.endswith("/repo") and "FilePath" in query:
            repo_rel = path[len("/api/v1/datasets/"):-len("/repo")]
            src = query["FilePath"][0]
            target = os.path.join(root, repo_rel, "resolve", query["Revision"][0], src)
            served = os.path.basename(src)
        elif "/tar.gz/" in path:
            target, served = tarball, os.path.basename(tarball)
        else:
            target, served = os.path.join(root, path.lstrip("/")), os.path.basename(path)

        if not os.path.isfile(target):
            self.send_error(404)
            return
        with open(target, "rb") as fh:
            data = fh.read()
        # A mirror told to corrupt one body hands out the wrong bytes at the right length,
        # which is the case nothing but the hash can catch.
        if served == bogus_target():
            data = data[:-1] + b"!"

        # Range support, because the fetcher resumes with `curl -C -`: a server that answers
        # 200 to a ranged request would make the resume assertion test curl, not the script.
        start = 0
        range_hdr = self.headers.get("Range")
        if range_hdr and range_hdr.startswith("bytes="):
            start = int(range_hdr[len("bytes="):].split("-")[0])
            if start >= len(data):
                self.send_error(416)
                return
        body = data[start:]
        if start:
            self.send_response(206)
            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, len(data) - 1, len(data)))
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(body)

    def do_GET(self):
        self.route()

    def do_HEAD(self):
        self.route()

server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
with open(port_file, "w") as fh:
    fh.write(str(server.server_address[1]))
server.serve_forever()
PY

python3 "${work}/serve.py" "${hub}" "${port_file}" "${log}" "${tarball}" "${bogus_file}" &
server_pid=$!
for _ in $(seq 1 50); do [ -s "${port_file}" ] && break; sleep 0.1; done
[ -s "${port_file}" ] || fail "fixture mirror did not start"
endpoint="http://127.0.0.1:$(cat "${port_file}")"

sha_of() { sha256sum "$1" | cut -d' ' -f1; }
size_of() { stat -c%s "$1"; }

# pyarrow is not part of the gate environment, so anything that needs a real parquet is
# guarded on it. BENCHMARK_PYLIBS is honoured exactly as the fetcher honours it.
PY_PREAMBLE='import os, sys
sys.path[:0] = [p for p in os.environ.get("BENCHMARK_PYLIBS", "").split(os.pathsep) if p]'
have_pyarrow=""
python3 -c "${PY_PREAMBLE}
import pyarrow.parquet" 2>/dev/null && have_pyarrow=1

parquet_body="${hub}/ScaleAI/SWE-bench_Pro/resolve/fixture-rev/data/test.parquet"
mkdir -p "$(dirname "${parquet_body}")"
if [ -n "${have_pyarrow}" ]; then
  python3 -c "${PY_PREAMBLE}
import sys, pyarrow as pa, pyarrow.parquet as pq
pq.write_table(pa.table({'id': [1, 2], 'text': ['a', 'b']}), sys.argv[1])" "${parquet_body}"
  # The JSONL the fetcher must produce, written here by the same rule so the assertion is
  # about the fetcher's output, not about pyarrow round-tripping itself.
  python3 -c "${PY_PREAMBLE}
import json, sys, pyarrow.parquet as pq
with open(sys.argv[2], 'w', encoding='utf-8') as fh:
    for row in pq.read_table(sys.argv[1]).to_pylist():
        fh.write(json.dumps(row, ensure_ascii=False) + '\n')" "${parquet_body}" "${work}/expected.jsonl"
else
  # Nothing here reads a parquet: only its bytes and hash matter to the fetch.
  head -c 4096 /dev/zero | tr '\0' 'p' >"${parquet_body}"
fi

tar_sha="$(sha_of "${tarball}")"
tar_size="$(size_of "${tarball}")"
m1_sha="$(sha_of "${work}/tree/toolathlon_gym-fixture/db/init.sql.gz")"
m1_size="$(size_of "${work}/tree/toolathlon_gym-fixture/db/init.sql.gz")"
m2_sha="$(sha_of "${work}/tree/toolathlon_gym-fixture/docker-compose.yml")"
m2_size="$(size_of "${work}/tree/toolathlon_gym-fixture/docker-compose.yml")"

# write_catalog <parquet-sha> <parquet-size> <tar-sha> <tar-size>
# The derived entry is present only where a parquet can be built; where it is not, --convert
# has nothing to produce and the fixture says so.
write_catalog() {
  if [ -n "${have_pyarrow}" ]; then
    derived='d_dest=("swe-bench-pro/test.jsonl"); d_src=("swe-bench-pro/test.parquet")
      d_sha=("'"$(sha_of "${work}/expected.jsonl")"'"); d_size=("'"$(size_of "${work}/expected.jsonl")"'")'
  else
    derived='d_dest=(); d_src=(); d_sha=(); d_size=()'
  fi
  cat >"${work}/catalog.sh" <<EOF
catalog() {
  source=""; repo=""; revision=""; extract_dir=""
  a_dest=(); a_src=(); a_sha=(); a_size=()
  d_dest=(); d_src=(); d_sha=(); d_size=()
  m_path=(); m_sha=(); m_size=()
  case "\$1" in
    swe-bench-pro)
      source="modelscope-dataset"; repo="ScaleAI/SWE-bench_Pro"; revision="fixture-rev"
      a_dest=("swe-bench-pro/test.parquet"); a_src=("data/test.parquet")
      a_sha=("$1"); a_size=("$2")
      ${derived}
      ;;
    toolathlon-gym)
      source="github-tarball"; repo="eigent-ai/toolathlon_gym"; revision="fixture-rev"
      extract_dir="toolathlon-gym"
      a_dest=("toolathlon-gym.tar.gz"); a_src=("fixture-rev")
      a_sha=("$3"); a_size=("$4")
      m_path=("db/init.sql.gz" "docker-compose.yml")
      m_sha=("${m1_sha}" "${m2_sha}"); m_size=("${m1_size}" "${m2_size}")
      ;;
    *) return 1 ;;
  esac
}
EOF
}

write_catalog "$(sha_of "${parquet_body}")" "$(size_of "${parquet_body}")" "${tar_sha}" "${tar_size}"
export BENCHMARK_CATALOG_FILE="${work}/catalog.sh"

run() { "${fetcher}" --benchmark "$1" --endpoint "${endpoint}" --dest "$2" "${@:3}"; }
count_downloads() { grep -cE '/repo\?|/tar\.gz/' "${log}" || true; }

# --- 1) one real provision ---------------------------------------------------
dest="${work}/dest"
run swe-bench-pro "${dest}" >"${work}/r1.log" 2>&1 || {
  cat "${work}/r1.log" >&2; fail "provisioning swe-bench-pro failed against a complete fixture"
}
cmp -s "${dest}/swe-bench-pro/test.parquet" "${parquet_body}" ||
  fail "the installed parquet is not the body the mirror served"
[ ! -e "${dest}/swe-bench-pro/test.parquet.part" ] ||
  fail "a .part survived a successful install"

run toolathlon-gym "${dest}" >"${work}/r2.log" 2>&1 || {
  cat "${work}/r2.log" >&2; fail "provisioning toolathlon-gym failed against a complete fixture"
}
cmp -s "${dest}/toolathlon-gym/db/init.sql.gz" "${work}/tree/toolathlon_gym-fixture/db/init.sql.gz" ||
  fail "extraction did not reproduce the member the tarball holds"
echo "fixture sql dump" | cmp -s - "${dest}/toolathlon-gym/db/init.sql.gz" ||
  fail "the extracted member is not the tarball's content (strip-components may have cut a file)"

# --- 2) --verify is green on the provisioned set -----------------------------
run swe-bench-pro "${dest}" --verify >"${work}/v1.log" 2>&1 ||
  { cat "${work}/v1.log" >&2; fail "--verify went red on a freshly provisioned tree"; }
run toolathlon-gym "${dest}" --verify >"${work}/v2.log" 2>&1 ||
  { cat "${work}/v2.log" >&2; fail "--verify went red on a freshly provisioned tarball tree"; }

# --- 3) a second run downloads nothing ---------------------------------------
before="$(count_downloads)"
run swe-bench-pro "${dest}" >"${work}/r3.log" 2>&1 || fail "a second swe-bench-pro run failed"
run toolathlon-gym "${dest}" >"${work}/r4.log" 2>&1 || fail "a second toolathlon-gym run failed"
after="$(count_downloads)"
[ "${after}" -eq "${before}" ] ||
  fail "a second run fetched $((after - before)) more times: installed data was not recognized"
grep -q "present" "${work}/r3.log" || fail "the second run reported nothing as already present"

# --- 4) tampering with an installed file is refused, not overwritten ---------
printf 'x' >>"${dest}/swe-bench-pro/test.parquet"
size_before="$(size_of "${dest}/swe-bench-pro/test.parquet")"
if run swe-bench-pro "${dest}" >"${work}/r5.log" 2>&1; then
  fail "an installed file whose bytes were changed was accepted"
fi
grep -q "refusing to overwrite" "${work}/r5.log" ||
  fail "the refusal did not name why: $(cat "${work}/r5.log")"
[ "$(size_of "${dest}/swe-bench-pro/test.parquet")" -eq "${size_before}" ] ||
  fail "the tampered file was overwritten: a refusal must leave it as it was"

# --- 5) --verify goes red on the same tampered tree --------------------------
if run swe-bench-pro "${dest}" --verify >"${work}/v3.log" 2>&1; then
  fail "--verify stayed green after the parquet bytes changed"
fi
grep -q "MISSING" "${work}/v3.log" || fail "--verify went red without naming the missing file"

# --- 6) a body that does not match its catalog hash is refused ---------------
# The mirror hands out the wrong byte at the right length for one file; only the hash can
# tell, and the check must happen before it is installed.
printf '%s' 'test.parquet' >"${bogus_file}"
bad_dest="${work}/bad-dest"
if run swe-bench-pro "${bad_dest}" >"${work}/r6.log" 2>&1; then
  fail "a body whose hash does not match the catalog was installed"
fi
grep -q "does not match the catalog" "${work}/r6.log" ||
  fail "the refusal did not name a hash mismatch: $(cat "${work}/r6.log")"
[ ! -e "${bad_dest}/swe-bench-pro/test.parquet" ] ||
  fail "a mismatched body was installed under its destination name"
rm -f "${bogus_file}"

# --- 7) a tarball whose hash does not match is refused -----------------------
write_catalog "$(sha_of "${parquet_body}")" "$(size_of "${parquet_body}")" "$(printf '0%.0s' $(seq 1 64))" "${tar_size}"
bad_tar="${work}/bad-tar"
if run toolathlon-gym "${bad_tar}" >"${work}/r7.log" 2>&1; then
  fail "a tarball whose hash does not match the catalog was extracted"
fi
[ ! -d "${bad_tar}/toolathlon-gym" ] || fail "a mismatched tarball was extracted anyway"
write_catalog "$(sha_of "${parquet_body}")" "$(size_of "${parquet_body}")" "${tar_sha}" "${tar_size}"

# --- 8) a truncated download resumes -----------------------------------------
# The mirror's body is emulated by the fixture, so resume is exercised by pre-placing part of
# the file the script would have written and asserting it is completed, not restarted.
resume_dest="${work}/resume-dest"
mkdir -p "${resume_dest}/swe-bench-pro"
head -c 1000 "${parquet_body}" >"${resume_dest}/swe-bench-pro/test.parquet.part"
run swe-bench-pro "${resume_dest}" >"${work}/r8.log" 2>&1 || {
  cat "${work}/r8.log" >&2; fail "resuming from a .part failed"
}
grep -q "resume" "${work}/r8.log" || fail "the run did not report resuming from a .part"
cmp -s "${resume_dest}/swe-bench-pro/test.parquet" "${parquet_body}" ||
  fail "the resumed download did not end up as the full body"

# --- 9) the derived JSONL, only where pyarrow exists -------------------------
if [ -n "${have_pyarrow}" ]; then
  conv_dest="${work}/conv-dest"
  run swe-bench-pro "${conv_dest}" --convert >"${work}/r9.log" 2>&1 || {
    cat "${work}/r9.log" >&2; fail "--convert failed with pyarrow available"
  }
  cmp -s "${conv_dest}/swe-bench-pro/test.jsonl" "${work}/expected.jsonl" ||
    fail "the derived JSONL is not what the converter rule produces"
  run swe-bench-pro "${conv_dest}" --verify --convert >/dev/null 2>&1 ||
    fail "--verify --convert went red on a tree with the derived file in place"
  rm -f "${conv_dest}/swe-bench-pro/test.jsonl"
  if run swe-bench-pro "${conv_dest}" --verify --convert >"${work}/v9.log" 2>&1; then
    fail "--verify --convert stayed green with the derived file removed"
  fi
  run swe-bench-pro "${conv_dest}" --verify >/dev/null 2>&1 ||
    fail "--verify without --convert should not require the derived file"
else
  echo "SKIP: pyarrow not importable, the parquet -> JSONL path was not exercised"
fi

"${fetcher}" --help >/dev/null || fail "--help did not exit 0"
"${fetcher}" --benchmark nope --print-files >/dev/null 2>&1 &&
  fail "an unknown benchmark was accepted"

echo "PASS: the provisioned set matches the catalog, a second run fetches nothing, a truncated part resumes, and a mirror or a tree whose bytes do not match the pin is refused before it is installed"
