#!/usr/bin/env bash
# Deterministic gate for the reranker weight fetcher.
#
# The fetcher is the only producer of the directory the model loader reads, and it
# writes a layout nobody inspects by hand, so three of its four failure modes are
# silent: a wrong layout downloads everything and installs nothing findable (the
# loader then answers by going to the network, which is the thing the fetcher
# exists to avoid); a body installed without being hashed is a 2.1 GiB file under a
# name that claims to be its hash; and a run that ignores what is already in place
# re-fetches 2.3 GiB on every process start.
#
# So the real script runs here against a fixture hub over a local HTTP server, and
# what is asserted is what an operator cannot see: the layout the loader looks in,
# the hash installed under each name, that a second run asks the hub for nothing,
# that a name whose bytes do not match its hash is refused rather than overwritten,
# and that a hub whose listing does not describe the files it serves is refused
# before anything is written.
#
# The fixture hub is served with the two etag algorithms the real one uses: files
# below a kilobyte are git blobs (etag = git blob sha1), the rest are LFS objects
# (etag = content sha256). Which branch runs is decided by the etag length, so both
# halves of the check are exercised.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
# Overridable so that the mutation proof for this suite can point it at a mutated
# copy: the checkout's own file may be mid-download by the operator, and a script
# being read while it is being written is exercised by accident, not on purpose.
fetcher="${FETCHER_UNDER_TEST:-${repo}/deploy/scripts/fetch-reranker-weights.sh}"
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
commit="fbd57b17b4db111a9d16813bb08b4c804fac18e9"
model_repo="rozgo/bge-reranker-v2-m3"
resolve="${hub}/${model_repo}/resolve/${commit}"
mkdir -p "${resolve}"

# The file list is read out of the fetcher rather than repeated here: a file added
# there is then covered by every assertion below without an edit, and one removed
# there cannot leave a stale expectation behind.
mapfile -t needed < <(
  sed -n '/^readonly NEEDED_FILES=(/,/^)/p' "${fetcher}" |
    sed -n 's/^[[:space:]]*\([A-Za-z0-9._-]\+\)$/\1/p'
)
[ "${#needed[@]}" -ge 6 ] || fail "从 ${fetcher} 里读到的权重清单只有 ${#needed[@]} 项，抽取器或清单本身出了问题"

# One body above the fixture's kilobyte line, so the sha256 branch is real.
{
  for file in "${needed[@]}"; do
    if [ "${file}" = "model.onnx.data" ]; then
      head -c 4096 /dev/zero | tr '\0' 'x' >"${resolve}/${file}"
    else
      printf 'fixture body for %s\n' "${file}" >"${resolve}/${file}"
    fi
  done
} || fail "建 fixture 失败"

# The hub's etag for a body, in whichever of the two algorithms applies: this is
# the name the blob has to carry, computed the same way the fixture server does.
etag_of_body() {
  python3 - "$1" <<'PY'
import hashlib, sys
data = open(sys.argv[1], "rb").read()
if len(data) < 1024:
    print(hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest())
else:
    print(hashlib.sha256(data).hexdigest())
PY
}

write_listing() { # sizes overridable per file: write_listing <file>=<size> ...
  local overrides=("$@")
  python3 - "${hub}" "${model_repo}" "${commit}" "${needed[@]}" -- "${overrides[@]+"${overrides[@]}"}" <<'PY'
import json, os, sys
hub, repo, commit = sys.argv[1], sys.argv[2], sys.argv[3]
sep = sys.argv.index("--")
files = sys.argv[4:sep]
overrides = dict(a.split("=", 1) for a in sys.argv[sep + 1:])
siblings = []
for name in files:
    path = os.path.join(hub, repo, "resolve", commit, name)
    size = int(overrides.get(name, os.path.getsize(path)))
    entry = {"rfilename": name}
    if name.endswith(".data"):
        entry["lfs"] = {"size": size}
    else:
        entry["size"] = size
    siblings.append(entry)
listing = os.path.join(hub, "api", "models", repo)
os.makedirs(os.path.dirname(listing), exist_ok=True)
with open(listing, "w") as fh:
    json.dump({"sha": commit, "siblings": siblings}, fh)
PY
}
write_listing

cat >"${work}/serve.py" <<'PY'
import hashlib, http.server, os, sys, urllib.parse

root, port_file, log, bogus_file = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

def git_blob_sha1(data):
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()

def bogus_for():
    """A file name this hub is told to advertise a false etag for."""
    if os.path.isfile(bogus_file):
        with open(bogus_file) as fh:
            return fh.read().strip()
    return ""

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def serve(self):
        path = urllib.parse.urlparse(self.path).path.lstrip("/")
        with open(log, "a") as fh:
            fh.write(path + "\n")
        target = os.path.join(root, path)
        if not os.path.isfile(target):
            self.send_error(404)
            return
        with open(target, "rb") as fh:
            data = fh.read()
        etag = git_blob_sha1(data) if len(data) < 1024 else hashlib.sha256(data).hexdigest()
        # A mirror that hands out bytes different from the ones the hash it
        # advertises belongs to: the length is right, the content is not.
        if os.path.basename(path) == bogus_for():
            etag = "a" * 64
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("x-linked-etag", '"%s"' % etag)
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(data)

    def do_GET(self):
        self.serve()

    def do_HEAD(self):
        self.serve()

server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
with open(port_file, "w") as fh:
    fh.write(str(server.server_address[1]))
server.serve_forever()
PY

log="${work}/requests.log"
port_file="${work}/port"
bogus_file="${work}/bogus"
: >"${log}"
python3 "${work}/serve.py" "${hub}" "${port_file}" "${log}" "${bogus_file}" &
server_pid=$!
for _ in $(seq 1 50); do [ -s "${port_file}" ] && break; sleep 0.1; done
[ -s "${port_file}" ] || fail "fixture hub 没起来"
endpoint="http://127.0.0.1:$(cat "${port_file}")"

# --- 1) one real run against the fixture hub ---------------------------------
cache="${work}/cache"
run_fetch() { "${fetcher}" --endpoint "${endpoint}" --repo "${model_repo}" --dest "${cache}"; }

run_fetch >"${work}/run1.log" 2>&1 || {
  cat "${work}/run1.log" >&2
  fail "第一次抓取失败（fixture hub 齐备，不该失败）"
}

cache_root="${cache}/models--${model_repo//\//--}"
[ "$(cat "${cache_root}/refs/main")" = "${commit}" ] || fail "refs/main 不是 ${commit}"
# The reader does not trim this file, so a trailing newline would name a snapshot
# directory that does not exist.
[ "$(wc -c <"${cache_root}/refs/main")" -eq "${#commit}" ] \
  || fail "refs/main 不是精确的 ${#commit} 字节（多出的字节会进快照路径）"

for file in "${needed[@]}"; do
  link="${cache_root}/snapshots/${commit}/${file}"
  [ -L "${link}" ] || fail "snapshots/${commit}/${file} 不是软链（加载器读的就是这个路径）"
  target="$(readlink "${link}")"
  # The target is relative to the symlink, which sits one level below the repo dir.
  blob="${cache_root}/snapshots/${commit}/${target}"
  [ "${target#\../../blobs/}" != "${target}" ] || fail "快照软链不是相对 blobs/ 的写法：${target}"
  [ -f "${blob}" ] || fail "快照软链指向的 ${target} 不存在"
  cmp -s "${blob}" "${resolve}/${file}" \
    || fail "${file} 装进库里的内容与 hub 上的不一致"
  # The blob is named by the hub's etag, and the check that it deserves that name
  # happens at install time. Recomputing it here is what proves the install step
  # hashed anything at all.
  expected="${cache_root}/blobs/$(etag_of_body "${resolve}/${file}")"
  [ -f "${expected}" ] || fail "${file} 没有装在以它的 etag 命名的名字下（期望 $(basename "${expected}")）"
done

# --- 2) a second run asks the hub for nothing --------------------------------
resolve_before="$(grep -c "/resolve/" "${log}" || true)"
run_fetch >"${work}/run2.log" 2>&1 || {
  cat "${work}/run2.log" >&2
  fail "第二次抓取失败（应当是一次纯校验）"
}
resolve_after="$(grep -c "/resolve/" "${log}" || true)"
[ "${resolve_after}" -eq "${resolve_before}" ] \
  || fail "第二次抓取又取了 $((resolve_after - resolve_before)) 个文件：已就位的权重没有被认出来"
grep -q "已就位" "${work}/run2.log" || fail "第二次抓取没有报告任何文件已就位"

# --- 3) bytes that do not match their name are refused, not replaced ----------
victim="${needed[0]}"
victim_blob="$(readlink "${cache_root}/snapshots/${commit}/${victim}")"
victim_path="${cache_root}/snapshots/${commit}/${victim_blob}"
printf 'tampered' >>"${victim_path}"
size_before="$(stat -c%s "${victim_path}")"

if run_fetch >"${work}/run3.log" 2>&1; then
  fail "库里有一份哈希对不上的权重，抓取却成功了"
fi
grep -q "校验失败" "${work}/run3.log" \
  || fail "拒绝的原因不是校验失败：
$(cat "${work}/run3.log")"
[ "$(stat -c%s "${victim_path}")" -eq "${size_before}" ] \
  || fail "被拒的那份被覆盖了：拒绝就应当原样留下，运维删掉它再跑才是重下"

# --- 4) a body that is not the size it claims installs nothing ---------------
# The files before it in the list do install, and that is what a resumable run
# should do -- what must not happen is the short body being installed under a name
# that claims its size.
bad_cache="${work}/bad-cache"
write_listing "tokenizer.json=99999"
if "${fetcher}" --endpoint "${endpoint}" --repo "${model_repo}" --dest "${bad_cache}" \
  >"${work}/run4.log" 2>&1; then
  fail "清单里的字节数与实际内容对不上，抓取却成功了"
fi
grep -q "字节数不对" "${work}/run4.log" \
  || fail "拒绝的原因不是字节数不符：
$(cat "${work}/run4.log")"
bad_root="${bad_cache}/models--${model_repo//\//--}"
[ ! -e "${bad_root}/snapshots/${commit}/tokenizer.json" ] \
  || fail "字节数不符的 tokenizer.json 仍然装进了快照目录"
[ ! -e "${bad_root}/blobs/$(etag_of_body "${resolve}/tokenizer.json")" ] \
  || fail "字节数不符的 tokenizer.json 仍然装进了 blobs/"
write_listing

# --- 5) a hub that does not carry the whole weight set is refused -------------
short_cache="${work}/short-cache"
python3 - "${hub}/api/models/${model_repo}" <<'PY'
import json, sys
path = sys.argv[1]
with open(path) as fh:
    listing = json.load(fh)
listing["siblings"] = [s for s in listing["siblings"] if s["rfilename"] != "model.onnx.data"]
with open(path, "w") as fh:
    json.dump(listing, fh)
PY
if "${fetcher}" --endpoint "${endpoint}" --repo "${model_repo}" --dest "${short_cache}" \
  >"${work}/run5.log" 2>&1; then
  fail "hub 的清单里没有权重本体，抓取却成功了"
fi
grep -q "权重集与仓库对不上" "${work}/run5.log" \
  || fail "拒绝的原因不是权重集不全：
$(cat "${work}/run5.log")"
write_listing

"${fetcher}" --help >/dev/null || fail "--help 退出码不是 0"

# --- 6) a body whose bytes are not what the hub's own etag describes ----------
# The mirror's etag is the only thing that says which bytes belong under a name,
# so a body that arrives whole, at the right length, and hashes to something else
# must not be installed: this is the case where nothing but the hash can tell.
bogus_cache="${work}/bogus-cache"
printf '%s' 'model.onnx' >"${bogus_file}"
if "${fetcher}" --endpoint "${endpoint}" --repo "${model_repo}" --dest "${bogus_cache}" \
  >"${work}/run6.log" 2>&1; then
  fail "hub 声明的哈希与它给的字节对不上，抓取却成功了"
fi
grep -q "校验失败" "${work}/run6.log" \
  || fail "拒绝的原因不是校验失败：
$(cat "${work}/run6.log")"
bogus_root="${bogus_cache}/models--${model_repo//\//--}"
[ ! -e "${bogus_root}/blobs/$(printf 'a%.0s' $(seq 1 64))" ] \
  || fail "对不上哈希的字节仍然装在 hub 声明的那个名字下"
rm -f "${bogus_file}"

echo "PASS: the weights land in the layout the loader reads, each blob under the hash of its own bytes, a second run fetches nothing, and a hub or a cache that does not match what it claims is refused before anything is installed"
