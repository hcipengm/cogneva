#!/usr/bin/env bash
# Release the scratch build trees under the temporary roots when keeping them is
# what would make the next build fail.
#
# The rule is derived from two readings, not picked: the scratch trees are
# released when the free space on their filesystem is smaller than the space
# they occupy. At that point the machine is keeping more throwaway build output
# than it has room for, and the next build is the one that runs out -- which is
# exactly what happened on 2026-11-23 (实测, 本机): a scratch worktree's target
# grew until the filesystem had no free space left, and the half-hour cache
# reclaimer had run before the build started. Below that reading nothing is
# released: a small leftover costs
# nothing to keep, and deleting trees another session may still be using is not
# a thing to do for tidiness.
#
# What it refuses to touch, each refusal a reading:
#   - only directories carrying cargo's own cache tag, with cargo's layout file
#     next to it. Same evidence the host cache reclaimer requires: a directory
#     merely named `target` is never a candidate;
#   - only trees no process of ours holds -- cwd, or an open file descriptor
#     inside -- and a process of ours whose /proc entries cannot be read stops
#     the whole run (an attribution failure is not evidence of absence);
#   - nothing at all when a candidate's filesystem cannot be read.
#
# COGNEVA_SCRATCH_GC_DRY_RUN=1 prints the plan and releases nothing; it reports
# `planned` rather than `released`, because a reading that cannot tell those
# apart is worse than none.
set -euo pipefail

readonly CARGO_CACHE_TAG_SIGNATURE='Signature: 8a477f597d28d172789f06886806bc55'
readonly CARGO_LAYOUT_FILE='.rustc_info.json'

roots="${COGNEVA_SCRATCH_GC_ROOTS:-/tmp}"
state_file="${COGNEVA_SCRATCH_GC_STATE:-${HOME:?}/.local/state/cogneva/scratch-gc.json}"
dry_run="${COGNEVA_SCRATCH_GC_DRY_RUN:-0}"
# An injected free-space reading, for the tests: they cannot fill a disk to
# check a rule that fires on a full one. Unset in every real run.
injected_avail_kb="${COGNEVA_SCRATCH_GC_AVAIL_KB:-}"

log() { printf 'scratch-gc: %s\n' "$*" >&2; }

now_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }
dry_json() { [ "$dry_run" = 1 ] && printf 'true' || printf 'false'; }

write_state() { # <json body minus the trailing brace>
    mkdir -p "$(dirname "$state_file")" 2>/dev/null || true
    printf '{%s,"dry_run":%s,"script":"%s"}\n' "$1" "$(dry_json)" \
        "$(sha256sum "${BASH_SOURCE[0]}" | cut -c1-12)" >"$state_file" 2>/dev/null || true
}

is_cargo_cache() { # dir
    local tag="$1/CACHEDIR.TAG"
    [ -f "$tag" ] && [ -f "$1/${CARGO_LAYOUT_FILE}" ] &&
        [ "$(head -c "${#CARGO_CACHE_TAG_SIGNATURE}" "$tag" 2>/dev/null || true)" = "${CARGO_CACHE_TAG_SIGNATURE}" ]
}

# Every path a live process of ours sits in or holds open, one per line.
process_paths() {
    # `find` over /proc always reports a race -- a process ends between the
    # listing and the read -- so its status is not a reading about the trees.
    {
        find /proc/[0-9]* -user "$(id -u)" -maxdepth 2 \
            \( -path '*/cwd' -o -path '*/fd/*' \) -type l \
            -printf '%l\n' 2>/dev/null |
            sed 's/ (deleted)$//'
    } || true
}

# A process of ours that cannot be read: a failure to attribute, not an absence.
unattributable_processes() {
    find /proc/[0-9]* -user "$(id -u)" -maxdepth 0 2>/dev/null |
        while IFS= read -r p; do
            [ -d "$p" ] || continue
            if ! readlink "$p/cwd" >/dev/null 2>&1; then
                state="$(sed -n 's/^[^)]*) \(.\).*/\1/p' "$p/stat" 2>/dev/null || true)"
                if [ -n "$state" ] && [ "$state" != Z ]; then
                    printf '%s\n' "${p#/proc/}"
                fi
            fi
        done
}

size_kb() { du -sk -- "$1" 2>/dev/null | cut -f1; }

avail_kb_of() { # mount point
    df -Pk -- "$1" 2>/dev/null | awk 'NR == 2 { print $4 }'
}

declare -a cands=() cand_sizes=() cand_mounts=()
while IFS= read -r dir; do
    [ -n "$dir" ] || continue
    case "$dir/" in
    "${PWD%/}/"*) log "leaving ${dir} alone: this run is inside it"; continue ;;
    esac
    is_cargo_cache "$dir" || { log "not cargo's cache, skipping: ${dir}"; continue; }
    mount="$(stat -c '%m' "$dir" 2>/dev/null || true)"
    [ -n "$mount" ] || { log "cannot read the filesystem of ${dir}: skipping"; continue; }
    cands+=("$dir")
    cand_sizes+=("$(size_kb "$dir" || true)")
    cand_mounts+=("$mount")
done < <(
    IFS=:
    for root in $roots; do
        [ -d "$root" ] || continue
        find "$root" -xdev -type d -name target -prune -print 2>/dev/null || true
    done
)

declare -a unattrib=()
mapfile -t unattrib < <(unattributable_processes)

if [ "${#cands[@]}" -eq 0 ]; then
    log "nothing scratch under ${roots}"
    write_state "\"last_run\":\"$(now_iso)\",\"outcome\":\"nothing\",\"roots\":\"${roots}\""
    exit 0
fi

if [ "${#unattrib[@]}" -gt 0 ]; then
    log "a process of ours could not be read (pids: ${unattrib[*]}): releasing nothing this run"
    write_state "\"last_run\":\"$(now_iso)\",\"outcome\":\"unattributable\",\"roots\":\"${roots}\",\"unreadable\":${#unattrib[@]}"
    exit 0
fi

scratch_kb=0
for kb in "${cand_sizes[@]}"; do scratch_kb=$((scratch_kb + ${kb:-0})); done

if [ -n "$injected_avail_kb" ]; then
    avail_kb="$injected_avail_kb"
else
    avail_kb="$(avail_kb_of "${cand_mounts[0]}" || true)"
fi
case "${avail_kb:-}" in
'' | *[!0-9]*)
    log "cannot read the free space on the filesystem holding ${cands[0]}: releasing nothing"
    exit 0
    ;;
esac

held_paths="$(process_paths)"

released_bytes=0
released_trees=0
held_trees=0
planned_trees=0

is_held() { # tree
    awk -v t="$1" 'index($0, t) == 1 && (length($0) == length(t) || substr($0, length(t) + 1, 1) == "/") { found = 1 } END { exit !found }' <<<"${held_paths}"
}

reading="the free space (${avail_kb} KiB) is at least the scratch space (${scratch_kb} KiB): keeping it"
if [ "$avail_kb" -lt "$scratch_kb" ]; then
    reading="the free space (${avail_kb} KiB) is smaller than the scratch space (${scratch_kb} KiB): releasing it"
    i=0
    for dir in "${cands[@]}"; do
        i=$((i + 1))
        if is_held "$dir"; then
            held_trees=$((held_trees + 1))
            log "leaving ${dir} alone: a live process holds it"
            continue
        fi
        if [ "$dry_run" = 1 ]; then
            planned_trees=$((planned_trees + 1))
            log "would release ${dir} (${cand_sizes[$((i - 1))]:-0} KiB)"
            continue
        fi
        before="$(avail_kb_of "${cand_mounts[$((i - 1))]}" || true)"
        rm -rf -- "$dir"
        after="$(avail_kb_of "${cand_mounts[$((i - 1))]}" || true)"
        case "${before:-}:${after:-}" in
        *[!0-9:]* | :) freed=0 ;;
        *) freed=$(((${after} - ${before}) * 1024)); [ "$freed" -ge 0 ] || freed=0 ;;
        esac
        released_bytes=$((released_bytes + freed))
        released_trees=$((released_trees + 1))
        log "released ${dir}"
    done
fi

outcome=kept
[ "${planned_trees:-0}" -gt 0 ] && outcome=planned
[ "${released_trees:-0}" -gt 0 ] && outcome=released

log "${reading}; released ${released_trees} tree(s), ${released_bytes} bytes"
write_state "\"last_run\":\"$(now_iso)\",\"outcome\":\"${outcome}\",\"roots\":\"${roots}\",\"scratch_kb\":${scratch_kb},\"avail_kb\":${avail_kb},\"candidates\":${#cands[@]},\"held\":${held_trees},\"released_trees\":${released_trees},\"released_bytes\":${released_bytes},\"planned\":${planned_trees},\"unreadable\":0"
