#!/usr/bin/env bash
#
# Fetch the benchmark data the evolution loop scores against.
#
# Why this is a producer and not a one-off download: the bytes live on third-party
# mirrors (a ModelScope dataset, a GitHub repository behind a proxy) that move under us --
# the same dataset repo has already republished its default config once. A run that only
# downloads leaves two things unverifiable: which revision the bytes came from, and whether
# they are the bytes the gate was calibrated against. So the pin is split in two -- the
# catalog records the revision (that names the listing) and a sha256 per file (that names
# the bytes) -- and every file is checked against its own hash before it is installed,
# never after.
#
# The catalog below is the single source of truth for what "this benchmark is provisioned"
# means: --print-files reads it, --verify reads it, and the fetch reads it. Nothing lands
# under <dest> that is not in it.
#
# Re-runnable: a file already in place under its own name is re-hashed and left alone; a
# download in flight sits in a `.part` and resumes. A file whose bytes do not match its
# declared hash is refused, never overwritten -- the name came from the hash, so mismatched
# bytes mean the mirror is not serving what it claims, and that is an operator decision,
# not a silent re-fetch.
#
# One artifact is derived rather than downloaded: SWE-bench Pro's JSONL is written from its
# parquet. That step needs pyarrow, so it is opt-in (--convert) -- the base fetch must not
# depend on a Python package the gate's environment may not carry. When it runs, its output
# is checked against the catalog hash like any other file, so a pyarrow that serializes a
# row differently turns into a red run rather than a quietly different benchmark.
#
# Usage:
#   deploy/scripts/fetch-benchmark-data.sh --dest /srv/cogneva/benchmarks
#   deploy/scripts/fetch-benchmark-data.sh --benchmark swe-bench-pro --convert
#   deploy/scripts/fetch-benchmark-data.sh --verify [--convert]      # re-check, no network
#   deploy/scripts/fetch-benchmark-data.sh --print-files

set -euo pipefail

readonly DEST_DEFAULT="/srv/cogneva/benchmarks"

die() { echo "error: $*" >&2; exit 1; }

usage() {
  cat <<'EOF'
usage: fetch-benchmark-data.sh [--benchmark <swe-bench-pro|toolathlon-gym|all>]
                               [--dest <dir>] [--endpoint <url>] [--convert]
                               [--verify] [--print-files]

  --benchmark  which benchmark to provision, default all
  --dest       where the data is written, default /srv/cogneva/benchmarks
  --endpoint   mirror base URL, overriding the per-source default
  --convert    also build (provision) or require (verify) the derived JSONL, needs pyarrow
  --verify     check what is installed against the catalog, no network, non-zero on mismatch
  --print-files  print every path this would install under --dest, one per line, no network
  -h, --help   this text

benchmarks and their pins:
  swe-bench-pro    ModelScope dataset ScaleAI/SWE-bench_Pro, public test split (731 rows)
  toolathlon-gym   GitHub eigent-ai/toolathlon_gym, 503 tasks, fetched as a tarball
EOF
}

# The catalog: per benchmark, the source shape, the pinned revision, and the files. The
# arrays are indexed in step instead of packing fields into one delimited string, so no
# field can be split by a character that happens to occur inside it.
#
#   a_*  artifacts fetched from the mirror (.part + hash checked before install)
#   d_*  artifacts derived locally from another artifact (only built under --convert)
#   m_*  files checked inside an extracted tarball, a spot check that extraction happened
catalog() {
  source=""
  repo=""
  revision=""
  extract_dir=""
  a_dest=(); a_src=(); a_sha=(); a_size=()
  d_dest=(); d_src=(); d_sha=(); d_size=()
  m_path=(); m_sha=(); m_size=()

  case "$1" in
    swe-bench-pro)
      source="modelscope-dataset"
      repo="ScaleAI/SWE-bench_Pro"
      revision="0c470e9cebb295f66caccb625dc7f40169771f79"
      # The public test split. The parquet's sha256 is the LFS object id the mirror
      # publishes for it, so the download is checked against the mirror's own claim and
      # not only against a number remembered here.
      a_dest=("swe-bench-pro/test-00000-of-00001.parquet")
      a_src=("data/test-00000-of-00001.parquet")
      a_sha=("c8cd7115496ad4e9a8b21d088cef576a65bf821bb542b24336f13f714cef13f8")
      a_size=("7816820")
      # The JSONL form, one row per line, in the parquet's row order.
      d_dest=("swe-bench-pro/test-00000-of-00001.jsonl")
      d_src=("swe-bench-pro/test-00000-of-00001.parquet")
      d_sha=("a1a67075e95009f25709b64792df2fcebef59884473f83c792e25886bd07e1bd")
      d_size=("24882378")
      ;;
    toolathlon-gym)
      source="github-tarball"
      repo="eigent-ai/toolathlon_gym"
      revision="ed735ba0af71d82688a55f952a6ea11479ae7206"
      extract_dir="toolathlon-gym"
      a_dest=("toolathlon-gym.tar.gz")
      a_src=("$revision") # the URL is built from the revision, not from a path
      # The archive for a commit keeps that commit in the name of every entry, so this hash
      # is the one for /tar.gz/<revision> and not for the branch tarball, which packs the
      # same tree under a differently named top directory.
      a_sha=("fbe8af77414fad3d2a2741dccd2e3a73d86716aebdc4e878c3878e5957bad766")
      a_size=("108238239")
      # Two files the tree cannot run without, hashed after extraction.
      m_path=("db/init.sql.gz" "docker-compose.yml")
      m_sha=("9d48204b20dd0b474d2766e40ce2d53f0d68a5d69706709cdbbacc33d8fd8511" \
             "59beee37084adb84f84d1462a5d05f451c2dc5a5b4a5f95fa278a847949087e7")
      m_size=("8236215" "1392")
      ;;
    *) die "unknown --benchmark: $1 (available: swe-bench-pro, toolathlon-gym)" ;;
  esac
}

benchmark="all"
dest="$DEST_DEFAULT"
endpoint=""
convert=""
mode="provision"
print_files=""

# The catalog above is the committed one. A test substitutes another by pointing
# BENCHMARK_CATALOG_FILE at a file that defines catalog(); nothing else changes.
if [ -n "${BENCHMARK_CATALOG_FILE:-}" ]; then
  # shellcheck source=/dev/null
  . "$BENCHMARK_CATALOG_FILE"
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --benchmark) benchmark="${2:?--benchmark needs a value}"; shift 2 ;;
    --dest) dest="${2:?--dest needs a value}"; shift 2 ;;
    --endpoint) endpoint="${2:?--endpoint needs a value}"; shift 2 ;;
    --convert) convert="1"; shift ;;
    --verify) mode="verify"; shift ;;
    --print-files) print_files="1"; shift ;;
    -h | --help) usage; exit 0 ;;
    *) die "unknown argument: $1 (see --help)" ;;
  esac
done

case "$benchmark" in
  all) benchmarks=("swe-bench-pro" "toolathlon-gym") ;;
  swe-bench-pro | toolathlon-gym) benchmarks=("$benchmark") ;;
  *) die "unknown --benchmark: $benchmark (available: swe-bench-pro, toolathlon-gym, all)" ;;
esac

if [ -n "$print_files" ]; then
  for b in "${benchmarks[@]}"; do
    catalog "$b"
    printf '%s\n' "${a_dest[@]}"
    if [ -n "$extract_dir" ]; then printf '%s\n' "${m_path[@]/#/$extract_dir/}"; fi
    if [ -n "$convert" ]; then printf '%s\n' "${d_dest[@]}"; fi
  done
  exit 0
fi

command -v curl >/dev/null || die "missing dependency: curl"
command -v python3 >/dev/null || die "missing dependency: python3"
command -v sha256sum >/dev/null || die "missing dependency: sha256sum"
command -v tar >/dev/null || die "missing dependency: tar"
command -v numfmt >/dev/null || die "missing dependency: numfmt"

urlencode() {
  python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

base_url() {
  if [ -n "$endpoint" ]; then
    printf '%s' "${endpoint%/}"
  elif [ "$source" = "modelscope-dataset" ]; then
    printf '%s' "https://www.modelscope.cn"
  else
    printf '%s' "https://gh-proxy.com"
  fi
}

url_of() {
  local base; base="$(base_url)"
  if [ "$source" = "modelscope-dataset" ]; then
    printf '%s/api/v1/datasets/%s/repo?Revision=%s&FilePath=%s' \
      "$base" "$repo" "$revision" "$(urlencode "$1")"
  else
    printf '%s/https://codeload.github.com/%s/tar.gz/%s' "$base" "$repo" "$revision"
  fi
}

# The size first (a truncated download is caught without reading the whole body), then the
# hash the catalog declares. Both, because a name that came from a hash proves nothing
# about the bytes actually on disk.
check_file() {
  local f="$1" want_sha="$2" want_size="$3" got_sha got_size
  [ -f "$f" ] || return 1
  got_size="$(stat -c%s "$f")"
  [ "$got_size" = "$want_size" ] || return 1
  got_sha="$(sha256sum "$f" | cut -d' ' -f1)"
  [ "$got_sha" = "$want_sha" ] || return 1
  return 0
}

why_mismatch() {
  local f="$1" want_sha="$2" want_size="$3"
  [ -f "$f" ] || { printf 'missing'; return; }
  local got_size; got_size="$(stat -c%s "$f")"
  if [ "$got_size" != "$want_size" ]; then
    printf 'size %s, catalog says %s' "$got_size" "$want_size"; return
  fi
  printf 'hash %s, catalog says %s' "$(sha256sum "$f" | cut -d' ' -f1)" "$want_sha"
}

fetch_file() {
  local rel="$1" url="$2" want_sha="$3" want_size="$4"
  local out="$dest/$rel" part="$dest/$rel.part"
  mkdir -p "$(dirname "$out")"

  if [ -e "$out" ]; then
    if check_file "$out" "$want_sha" "$want_size"; then
      echo "  present  $rel"
      return 0
    fi
    die "$rel is already installed but does not match the catalog ($(why_mismatch "$out" "$want_sha" "$want_size")); refusing to overwrite -- remove it and re-run to re-fetch"
  fi

  [ -f "$part" ] && echo "  resume   $rel" || echo "  fetch    $rel  ($(numfmt --to=iec "$want_size"))"
  curl -fSL --retry 5 --retry-delay 3 -C - --no-progress-meter "$url" -o "$part" ||
    die "$rel failed to download (what arrived is in $part, re-running resumes it)"

  if ! check_file "$part" "$want_sha" "$want_size"; then
    die "$rel does not match the catalog after download ($(why_mismatch "$part" "$want_sha" "$want_size"))"
  fi
  mv -f "$part" "$out"
  echo "  ok       $rel"
}

# The parquet reader lives outside the repo on purpose (a ~50 MiB wheel is not something to
# commit); BENCHMARK_PYLIBS names where to load it from, as a PATH-style list. The row order
# and the non-ASCII handling match what the catalog hash was taken from, so a different
# pyarrow that serializes a row differently fails the hash check instead of going unnoticed.
convert_jsonl() {
  python3 - "$1" "$2" <<'PY'
import json, os, sys
libs = [p for p in os.environ.get("BENCHMARK_PYLIBS", "").split(os.pathsep) if p]
sys.path[:0] = libs
import pyarrow.parquet as pq
src, dst = sys.argv[1], sys.argv[2]
rows = pq.read_table(src).to_pylist()
with open(dst, "w", encoding="utf-8") as fh:
    for row in rows:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
print("{} rows={} bytes={}".format(dst, len(rows), os.path.getsize(dst)))
PY
}

verify_benchmark() {
  local b="$1" i ok=0
  catalog "$b"
  echo "verify $b ($source @ $revision)"

  for i in "${!a_dest[@]}"; do
    if check_file "$dest/${a_dest[$i]}" "${a_sha[$i]}" "${a_size[$i]}"; then
      echo "  ok       ${a_dest[$i]}"
    else
      echo "  MISSING  ${a_dest[$i]} -- $(why_mismatch "$dest/${a_dest[$i]}" "${a_sha[$i]}" "${a_size[$i]}")"
      ok=1
    fi
  done

  for i in "${!m_path[@]}"; do
    local rel="$extract_dir/${m_path[$i]}"
    if check_file "$dest/$rel" "${m_sha[$i]}" "${m_size[$i]}"; then
      echo "  ok       $rel"
    else
      echo "  MISSING  $rel -- $(why_mismatch "$dest/$rel" "${m_sha[$i]}" "${m_size[$i]}")"
      ok=1
    fi
  done

  if [ -n "$convert" ]; then
    for i in "${!d_dest[@]}"; do
      if check_file "$dest/${d_dest[$i]}" "${d_sha[$i]}" "${d_size[$i]}"; then
        echo "  ok       ${d_dest[$i]}"
      else
        echo "  MISSING  ${d_dest[$i]} -- $(why_mismatch "$dest/${d_dest[$i]}" "${d_sha[$i]}" "${d_size[$i]}")"
        ok=1
      fi
    done
  fi
  return "$ok"
}

provision_benchmark() {
  local b="$1" i
  catalog "$b"
  echo "provision $b ($source @ $revision) -> $dest"
  mkdir -p "$dest"

  if [ "$source" = "github-tarball" ]; then
    # The tarball is kept next to the tree it produces: it is the pinned artifact, so
    # --verify hashes it again instead of trusting an unpacked tree nothing hashed.
    fetch_file "${a_dest[0]}" "$(url_of "${a_src[0]}")" "${a_sha[0]}" "${a_size[0]}"
    if [ -d "$dest/$extract_dir" ] && members_ok; then
      echo "  present  $extract_dir/"
    else
      echo "  extract  $extract_dir/"
      local tmp="$dest/.extract.$$"
      rm -rf "$tmp"; mkdir -p "$tmp"
      tar -xzf "$dest/${a_dest[0]}" -C "$tmp" --strip-components=1
      rm -rf "${dest:?}/${extract_dir:?}"
      mv "$tmp" "$dest/$extract_dir"
      members_ok ||
        die "$extract_dir after extraction does not match the catalog (the tarball hash passed, so extraction itself is what went wrong)"
    fi
  else
    for i in "${!a_dest[@]}"; do
      fetch_file "${a_dest[$i]}" "$(url_of "${a_src[$i]}")" "${a_sha[$i]}" "${a_size[$i]}"
    done
  fi

  if [ -n "$convert" ]; then
    for i in "${!d_dest[@]}"; do
      local out="$dest/${d_dest[$i]}" src="$dest/${d_src[$i]}"
      if check_file "$out" "${d_sha[$i]}" "${d_size[$i]}"; then
        echo "  present  ${d_dest[$i]}"
        continue
      fi
      mkdir -p "$(dirname "$out")"
      echo "  convert  ${d_dest[$i]}"
      convert_jsonl "$src" "$out" ||
        die "converting ${d_src[$i]} needs pyarrow; set BENCHMARK_PYLIBS to a directory holding it, or install pyarrow"
      check_file "$out" "${d_sha[$i]}" "${d_size[$i]}" ||
        die "${d_dest[$i]} was written but does not match the catalog ($(why_mismatch "$out" "${d_sha[$i]}" "${d_size[$i]}"))"
    done
  fi
}

members_ok() {
  local i
  for i in "${!m_path[@]}"; do
    check_file "$dest/$extract_dir/${m_path[$i]}" "${m_sha[$i]}" "${m_size[$i]}" || return 1
  done
  return 0
}

rc=0
for b in "${benchmarks[@]}"; do
  if [ "$mode" = "verify" ]; then
    verify_benchmark "$b" || rc=1
  else
    provision_benchmark "$b"
  fi
done

if [ "$mode" = "verify" ]; then
  [ "$rc" -eq 0 ] && echo "verified: all catalog files under $dest match" ||
    die "verification failed: $dest is not the provisioned set the catalog describes"
else
  echo "done: $dest"
fi
