#!/usr/bin/env bash
# Deterministic gate for the model weight fetcher.
#
# The fetcher is the only producer of the directory the model loaders read, and it writes a
# layout nobody inspects by hand, so three of its failure modes are silent: a wrong layout
# downloads everything and installs nothing findable (the loader then answers by going to the
# network, which is the thing the fetcher exists to avoid); a body installed without being
# hashed is a 2.1 GiB file under a name that claims to be its hash; and a run that ignores
# what is already in place re-fetches 2.1 GiB on every process start.
#
# So the real script runs here against a fixture hub over a local HTTP server, and what is
# asserted is what an operator cannot see: the layout the loader looks in (for a top-level
# file and for one under `onnx/`, whose symlink needs one more `..`), the hash installed
# under each name, that a second run asks the hub for nothing, that a name whose bytes do not
# match its hash is refused rather than overwritten, that a hub whose listing does not
# describe the whole weight set is refused before anything is written, and that a body which
# arrives whole but hashes to something else is not installed under the advertised name.
#
# The whole battery runs once per model, because the two models are the two hub shapes and
# the two hash sources: `reranker` is read from HuggingFace (the listing carries sizes, the
# hash comes from a per-file `x-linked-etag` on `/resolve/`) and `bge-m3` from ModelScope
# (the listing carries the `Sha256` itself, and its files live under `onnx/`). A change that
# works for one shape and not the other has to fail here.
#
# The fixture serves the two etag algorithms HuggingFace uses: files below a kilobyte are git
# blobs (etag = git blob sha1), the rest are LFS objects (etag = content sha256). Which branch
# runs is decided by the etag length, so both halves of the check are exercised.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
# Overridable so that the mutation proof for this suite can point it at a mutated copy: the
# checkout's own file may be mid-download by the operator, and a script being read while it
# is being written is exercised by accident, not on purpose.
fetcher="${FETCHER_UNDER_TEST:-${repo}/deploy/scripts/fetch-model-weights.sh}"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
server_pid=""
cleanup() {
  [ -n "${server_pid}" ] && kill "${server_pid}" 2>/dev/null || true
  rm -rf "${work}"
}
trap cleanup EXIT

[ -x "${fetcher}" ] || fail "${fetcher} 不可执行（invocation 会以 Permission denied 失败，而输出看起来像脚本自己报错）"

# --- the fixture hub ---------------------------------------------------------
hub="${work}/hub"
log="${work}/requests.log"
port_file="${work}/port"
bogus_file="${work}/bogus"
: >"${log}"

cat >"${work}/serve.py" <<'PY'
import hashlib, http.server, os, sys, urllib.parse

root, port_file, log, bogus_file = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

def git_blob_sha1(data):
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()

def bogus_target():
    """A file name this hub is told to advertise a false HuggingFace etag for."""
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
        etag = None

        # ModelScope raw bytes: /api/v1/models/<repo>/repo?Revision=<rev>&FilePath=<path>
        if path.endswith("/repo") and "FilePath" in query:
            repo_rel = path[len("/api/v1/models/"):-len("/repo")]
            target = os.path.join(root, repo_rel, "resolve", query["Revision"][0], query["FilePath"][0])
        else:
            # Everything else is a path on disk: the two listings, and HuggingFace's
            # /<repo>/resolve/<commit>/<file> bodies.
            target = os.path.join(root, path.lstrip("/"))
            if "/resolve/" in path:
                etag = True

        if not os.path.isfile(target):
            self.send_error(404)
            return
        with open(target, "rb") as fh:
            data = fh.read()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        if etag:
            value = git_blob_sha1(data) if len(data) < 1024 else hashlib.sha256(data).hexdigest()
            # A mirror that hands out bytes different from the ones the hash it advertises
            # belongs to: the length is right, the content is not.
            if os.path.basename(path) == bogus_target():
                value = "a" * 64
            self.send_header("x-linked-etag", '"%s"' % value)
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(data)

    def do_GET(self):
        self.route()

    def do_HEAD(self):
        self.route()

server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
with open(port_file, "w") as fh:
    fh.write(str(server.server_address[1]))
server.serve_forever()
PY

python3 "${work}/serve.py" "${hub}" "${port_file}" "${log}" "${bogus_file}" &
server_pid=$!
for _ in $(seq 1 50); do [ -s "${port_file}" ] && break; sleep 0.1; done
[ -s "${port_file}" ] || fail "fixture hub 没起来"
endpoint="http://127.0.0.1:$(cat "${port_file}")"

sha256_of() { sha256sum "$1" | cut -d' ' -f1; }

hf_etag_of() { # the name the hub's own etag gives a body
  python3 - "$1" <<'PY'
import hashlib, sys
data = open(sys.argv[1], "rb").read()
if len(data) < 1024:
    print(hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest())
else:
    print(hashlib.sha256(data).hexdigest())
PY
}

# write_listing <hub> <source> <repo> <commit> <bodies> <files...> -- <mode> [file] [value]
#   mode: normal | size <file> <bytes> | sha <file> <hex> | drop <file>
write_listing() {
  python3 - "$@" <<'PY'
import hashlib, json, os, sys
hub, source, repo, commit, bodies = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
sep = sys.argv.index("--")
files = sys.argv[6:sep]
args = sys.argv[sep + 1:]
mode = args[0] if args else "normal"
target = args[1] if len(args) > 1 else ""
value = args[2] if len(args) > 2 else ""

sha, size = {}, {}
for name in files:
    data = open(os.path.join(bodies, name), "rb").read()
    sha[name] = hashlib.sha256(data).hexdigest()
    size[name] = len(data)
if mode == "size":
    size[target] = int(value)
elif mode == "sha":
    sha[target] = value
elif mode == "drop":
    files = [f for f in files if f != target]

if source == "hf":
    siblings = []
    for name in files:
        entry = {"rfilename": name}
        if name.endswith(".data"):
            entry["lfs"] = {"size": size[name]}
        else:
            entry["size"] = size[name]
        siblings.append(entry)
    body = {"sha": commit, "siblings": siblings}
    path = os.path.join(hub, "api", "models", repo)
else:
    entries = [{"Path": n, "Name": os.path.basename(n), "Size": size[n], "Sha256": sha[n],
                "Revision": commit, "Type": "blob", "IsLFS": n.endswith(".data")}
               for n in files]
    # The directories the real hub lists beside the blobs; the script has to skip them.
    entries.append({"Path": "onnx", "Name": "onnx", "Size": 0, "Sha256": "",
                    "Revision": commit, "Type": "tree", "IsLFS": False})
    body = {"Code": 200, "Success": True, "Data": {"Files": entries}}
    path = os.path.join(hub, "api", "v1", "models", repo, "repo", "files")
os.makedirs(os.path.dirname(path), exist_ok=True)
with open(path, "w") as fh:
    json.dump(body, fh)
PY
}

# --- the battery, run once per model ----------------------------------------
battery() {
  local model="$1" source="$2" model_repo="$3" commit="$4"

  local label="[${model}/${source}]"
  mapfile -t needed < <("${fetcher}" --model "${model}" --print-files)
  [ "${#needed[@]}" -ge 6 ] \
    || fail "${label} --print-files 只给了 ${#needed[@]} 项，catalog 或该选项出了问题"
  # The reranker's files all sit at the repository root; bge-m3's graph does not, and a
  # catalog that lost the `onnx/` prefix has to be caught here rather than by a loader that
  # hangs looking for a file it will never find.
  if [ "${model}" = "bge-m3" ]; then
    printf '%s\n' "${needed[@]}" | grep -q '/' \
      || fail "${label} 权重清单里没有嵌套路径，快照软链深度的断言就没被覆盖"
  fi

  local bodies="${hub}/${model_repo}/resolve/${commit}"
  local f
  for f in "${needed[@]}"; do
    mkdir -p "$(dirname "${bodies}/${f}")"
    case "${f}" in
      *.data) head -c 4096 /dev/zero | tr '\0' 'x' >"${bodies}/${f}" ;;
      *) printf 'fixture body for %s\n' "${f}" >"${bodies}/${f}" ;;
    esac
  done
  write_listing "${hub}" "${source}" "${model_repo}" "${commit}" "${bodies}" "${needed[@]}" -- normal

  local count_pattern downloads_before downloads_after
  if [ "${source}" = "hf" ]; then count_pattern='/resolve/'; else count_pattern='/repo?'; fi

  run_fetch() { "${fetcher}" --model "${model}" --endpoint "${endpoint}" --repo "${model_repo}" --dest "${1}"; }

  # --- 1) one real run against the fixture hub ------------------------------
  local cache="${work}/cache-${model}"
  run_fetch "${cache}" >"${work}/run1-${model}.log" 2>&1 || {
    cat "${work}/run1-${model}.log" >&2
    fail "${label} 第一次抓取失败（fixture hub 齐备，不该失败）"
  }

  local cache_root="${cache}/models--${model_repo//\//--}"
  [ "$(cat "${cache_root}/refs/main")" = "${commit}" ] \
    || fail "${label} refs/main 不是 ${commit}"
  # The reader does not trim this file, so a trailing newline would name a snapshot
  # directory that does not exist.
  [ "$(wc -c <"${cache_root}/refs/main")" -eq "${#commit}" ] \
    || fail "${label} refs/main 不是精确的 ${#commit} 字节（多出的字节会进快照路径）"

  for f in "${needed[@]}"; do
    local link="${cache_root}/snapshots/${commit}/${f}"
    [ -L "${link}" ] || fail "${label} snapshots/${commit}/${f} 不是软链（加载器读的就是这个路径）"
    local target expected_prefix
    target="$(readlink "${link}")"
    case "${f}" in
      */*) expected_prefix="../../../blobs/" ;;
      *) expected_prefix="../../blobs/" ;;
    esac
    case "${target}" in
      "${expected_prefix}"*) ;;
      *) fail "${label} 快照软链的 .. 层数与 ${f} 的深度对不上（期望 ${expected_prefix}…）：${target}" ;;
    esac
    # Resolving the link from the snapshot is what proves the `..` count.
    [ -f "${link}" ] || fail "${label} 快照软链 ${f} 解不开（相对层级不对：${target}）"
    cmp -s "${link}" "${bodies}/${f}" || fail "${label} ${f} 装进库里的内容与 hub 上的不一致"
    # The blob is named by the hub's own hash, and the check that it deserves that name
    # happens at install time. Recomputing it here is what proves the install hashed
    # anything at all.
    local expected_hash
    if [ "${source}" = "hf" ]; then expected_hash="$(hf_etag_of "${bodies}/${f}")"; else expected_hash="$(sha256_of "${bodies}/${f}")"; fi
    [ -f "${cache_root}/blobs/${expected_hash}" ] \
      || fail "${label} ${f} 没有装在以它的哈希命名的名字下（期望 ${expected_hash}）"
  done

  # --- 2) a second run asks the hub for nothing -----------------------------
  downloads_before="$(grep -c "${count_pattern}" "${log}" || true)"
  run_fetch "${cache}" >"${work}/run2-${model}.log" 2>&1 || {
    cat "${work}/run2-${model}.log" >&2
    fail "${label} 第二次抓取失败（应当是一次纯校验）"
  }
  downloads_after="$(grep -c "${count_pattern}" "${log}" || true)"
  [ "${downloads_after}" -eq "${downloads_before}" ] \
    || fail "${label} 第二次抓取又取了 $((downloads_after - downloads_before)) 次：已就位的权重没有被认出来"
  grep -q "已就位" "${work}/run2-${model}.log" || fail "${label} 第二次抓取没有报告任何文件已就位"

  # --- 3) bytes that do not match their name are refused, not replaced ------
  local victim="${needed[0]}" victim_blob victim_path size_before
  victim_blob="$(readlink "${cache_root}/snapshots/${commit}/${victim}")"
  victim_path="${cache_root}/snapshots/${commit}/${victim_blob}"
  printf 'tampered' >>"${victim_path}"
  size_before="$(stat -c%s "${victim_path}")"

  if run_fetch "${cache}" >"${work}/run3-${model}.log" 2>&1; then
    fail "${label} 库里有一份哈希对不上的权重，抓取却成功了"
  fi
  grep -q "校验失败" "${work}/run3-${model}.log" \
    || fail "${label} 拒绝的原因不是校验失败：$(cat "${work}/run3-${model}.log")"
  [ "$(stat -c%s "${victim_path}")" -eq "${size_before}" ] \
    || fail "${label} 被拒的那份被覆盖了：拒绝就应当原样留下，运维删掉它再跑才是重下"

  # --- 4) a body that is not the size it claims installs nothing ------------
  local bad_cache="${work}/bad-cache-${model}"
  local size_victim="tokenizer.json"
  printf '%s\n' "${needed[@]}" | grep -qx "${size_victim}" || size_victim="${needed[0]}"
  write_listing "${hub}" "${source}" "${model_repo}" "${commit}" "${bodies}" "${needed[@]}" -- size "${size_victim}" 99999
  if run_fetch "${bad_cache}" >"${work}/run4-${model}.log" 2>&1; then
    fail "${label} 清单里的字节数与实际内容对不上，抓取却成功了"
  fi
  grep -q "字节数不对" "${work}/run4-${model}.log" \
    || fail "${label} 拒绝的原因不是字节数不符：$(cat "${work}/run4-${model}.log")"
  local bad_root="${bad_cache}/models--${model_repo//\//--}"
  [ ! -e "${bad_root}/snapshots/${commit}/${size_victim}" ] \
    || fail "${label} 字节数不符的 ${size_victim} 仍然装进了快照目录"
  write_listing "${hub}" "${source}" "${model_repo}" "${commit}" "${bodies}" "${needed[@]}" -- normal

  # --- 5) a hub that does not carry the whole weight set is refused ---------
  # The largest file, which is the model body in both catalogs: the completeness check has
  # to cover the file the loader cannot do without, not just a tokenizer file.
  local drop_victim="${needed[${#needed[@]} - 1]}"
  write_listing "${hub}" "${source}" "${model_repo}" "${commit}" "${bodies}" "${needed[@]}" -- drop "${drop_victim}"
  if run_fetch "${work}/short-cache-${model}" >"${work}/run5-${model}.log" 2>&1; then
    fail "${label} hub 的清单里没有 ${drop_victim}，抓取却成功了"
  fi
  grep -q "权重集与仓库对不上" "${work}/run5-${model}.log" \
    || fail "${label} 拒绝的原因不是权重集不全：$(cat "${work}/run5-${model}.log")"
  write_listing "${hub}" "${source}" "${model_repo}" "${commit}" "${bodies}" "${needed[@]}" -- normal

  # --- 6) a body whose bytes are not what the hub's own hash describes ------
  # The hash the hub reports is the only thing that says which bytes belong under a name, so
  # a body that arrives whole, at the right length, and hashes to something else must not be
  # installed: this is the case where nothing but the hash can tell.
  local bogus_hash
  bogus_hash="$(printf 'a%.0s' $(seq 1 64))"
  if [ "${source}" = "hf" ]; then
    printf '%s' "${victim}" >"${bogus_file}"
  else
    write_listing "${hub}" "${source}" "${model_repo}" "${commit}" "${bodies}" "${needed[@]}" -- sha "${victim}" "${bogus_hash}"
  fi
  if run_fetch "${work}/bogus-cache-${model}" >"${work}/run6-${model}.log" 2>&1; then
    fail "${label} hub 声明的哈希与它给的字节对不上，抓取却成功了"
  fi
  grep -q "校验失败" "${work}/run6-${model}.log" \
    || fail "${label} 拒绝的原因不是校验失败：$(cat "${work}/run6-${model}.log")"
  [ ! -e "${work}/bogus-cache-${model}/models--${model_repo//\//--}/blobs/${bogus_hash}" ] \
    || fail "${label} 对不上哈希的字节仍然装在 hub 声明的那个名字下"
  rm -f "${bogus_file}"
  write_listing "${hub}" "${source}" "${model_repo}" "${commit}" "${bodies}" "${needed[@]}" -- normal
}

battery reranker hf "rozgo/bge-reranker-v2-m3" "fbd57b17b4db111a9d16813bb08b4c804fac18e9"
battery bge-m3 modelscope "BAAI/bge-m3" "e44369c5623cc146f016da906583db4ee0e3488d"

"${fetcher}" --help >/dev/null || fail "--help 退出码不是 0"

echo "PASS: for both hub shapes the weights land in the layout the loader reads (nested files included), each blob under the hash of its own bytes, a second run fetches nothing, and a hub or a cache that does not match what it claims is refused before anything is installed"
